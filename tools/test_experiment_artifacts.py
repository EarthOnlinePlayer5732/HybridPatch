from __future__ import annotations

import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path

import build_experiment_records as records
import experiment_artifacts as artifacts


class ExperimentArtifactTests(unittest.TestCase):
    def fixture(self, root: Path) -> tuple[Path, Path]:
        (root / ".env.test").write_text(
            "FIXTURE_API_KEY=fixture-secret-value-12345\n",
            encoding="utf-8",
        )
        source = root / "HP_V8" / "exp_fixture"
        (source / "api_raw").mkdir(parents=True)
        (source / "api_raw" / "response.json").write_text(
            '{"content":"safe"}\n', encoding="utf-8"
        )
        (source / "result.jsonl").write_text(
            '{"score":1}\n', encoding="utf-8"
        )
        return source, root / "private" / "exp_fixture.tgz"

    def test_parallel_seal_matches_tree_digest_and_is_cached(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)

            seal, cached = artifacts.seal_experiment(
                source, archive, root=root
            )

            self.assertFalse(cached)
            self.assertEqual(
                seal["tree"]["tree_sha256"],
                records.tree_digest(source)["tree_sha256"],
            )
            self.assertEqual(
                seal["tree"]["credential_scan"][
                    "exact_local_secret_match_count"
                ],
                0,
            )
            with tarfile.open(archive, "r:gz") as handle:
                names = set(handle.getnames())
            self.assertIn("exp_fixture/api_raw/response.json", names)
            self.assertIsNone(seal["archive"]["scan_report_member"])
            self.assertEqual(
                seal["archive"]["scan_execution"],
                "parallel_with_native_tar",
            )

            second, cached = artifacts.seal_experiment(
                source, archive, root=root
            )
            self.assertTrue(cached)
            self.assertEqual(second["archive"]["sha256"], seal["archive"]["sha256"])

    def test_source_change_invalidates_seal_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)
            artifacts.seal_experiment(source, archive, root=root)
            (source / "result.jsonl").write_text(
                '{"score":0}\n', encoding="utf-8"
            )

            with self.assertRaisesRegex(RuntimeError, "cache key changed"):
                artifacts.seal_experiment(source, archive, root=root)

    def test_same_size_same_mtime_source_change_invalidates_seal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)
            artifacts.seal_experiment(source, archive, root=root)
            result = source / "result.jsonl"
            before = result.stat()
            result.write_text('{"score":0}\n', encoding="utf-8")
            os.utime(
                result,
                ns=(before.st_atime_ns, before.st_mtime_ns),
            )

            with self.assertRaisesRegex(RuntimeError, "cache key changed"):
                artifacts.seal_experiment(source, archive, root=root)

    def test_exact_secret_match_blocks_archive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)
            (source / "api_raw" / "leak.txt").write_text(
                "fixture-secret-value-12345", encoding="utf-8"
            )

            with self.assertRaisesRegex(RuntimeError, "exact local secret"):
                artifacts.seal_experiment(source, archive, root=root)
            self.assertFalse(archive.exists())

    def test_load_valid_seal_rejects_changed_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)
            artifacts.seal_experiment(source, archive, root=root)
            seal_path = archive.with_name(archive.name + ".seal.json")
            loaded = artifacts.load_valid_seal(
                seal_path,
                experiment_id="exp_fixture",
                source=source,
                root=root,
            )
            self.assertEqual(loaded["schema"], artifacts.SEAL_SCHEMA)

            (source / "new.txt").write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "changed after"):
                artifacts.load_valid_seal(
                    seal_path,
                    experiment_id="exp_fixture",
                    source=source,
                    root=root,
                )

    def test_load_valid_seal_rejects_changed_archive_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)
            artifacts.seal_experiment(source, archive, root=root)
            seal_path = archive.with_name(archive.name + ".seal.json")
            before = archive.stat()
            payload = bytearray(archive.read_bytes())
            payload[len(payload) // 2] ^= 1
            archive.write_bytes(payload)
            os.utime(
                archive,
                ns=(before.st_atime_ns, before.st_mtime_ns),
            )

            with self.assertRaisesRegex(
                    RuntimeError, "archive content is missing or changed"):
                artifacts.load_valid_seal(
                    seal_path,
                    experiment_id="exp_fixture",
                    source=source,
                    root=root,
                )

    def test_seal_contains_no_secret_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive = self.fixture(root)
            seal, _ = artifacts.seal_experiment(source, archive, root=root)
            serialized = json.dumps(seal, sort_keys=True)
            self.assertNotIn("fixture-secret-value-12345", serialized)


if __name__ == "__main__":
    unittest.main()
