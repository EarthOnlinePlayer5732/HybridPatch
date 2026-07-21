from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import finalize_experiment as finalize


class FinalizeExperimentTests(unittest.TestCase):
    def test_contract_validation_precedes_raw_tree_build(self) -> None:
        with tempfile.TemporaryDirectory(dir=finalize.ROOT) as directory:
            archive = Path(directory) / "exp_fixture"
            archive.mkdir()
            relative = archive.relative_to(finalize.ROOT).as_posix()
            catalog = {
                "record_sets": [
                    {
                        "owner": "HP_V8",
                        "experiments": [
                            {
                                "experiment_id": "exp_fixture",
                                "archive_path": relative,
                                "lifecycle_status": "incomplete",
                                "evidence_role": "diagnostic",
                                "verification": {"status": "pass"},
                            }
                        ],
                    }
                ]
            }
            calls: list[tuple[str, ...]] = []

            with (
                mock.patch.object(
                    finalize,
                    "parse_args",
                    return_value=SimpleNamespace(
                        experiment_id="exp_fixture",
                        skip_tree_hash=False,
                        sealed_manifest=None,
                    ),
                ),
                mock.patch.object(finalize, "load_catalog", return_value=catalog),
                mock.patch.object(
                    finalize,
                    "run",
                    side_effect=lambda *args: calls.append(tuple(args)),
                ),
            ):
                self.assertEqual(finalize.main(), 0)

        self.assertIn("--validate-only", calls[0])
        self.assertIn("--skip-tree-hash", calls[0])
        self.assertNotIn("--skip-tree-hash", calls[1])
        self.assertEqual(calls[2][-1], "--skip-tree-hash")

    def test_sealed_manifest_is_used_for_expensive_target_build(self) -> None:
        with tempfile.TemporaryDirectory(dir=finalize.ROOT) as directory:
            root = Path(directory)
            archive = root / "exp_fixture"
            archive.mkdir()
            seal = root / "fixture.seal.json"
            seal.write_text("{}", encoding="utf-8")
            catalog = {
                "record_sets": [
                    {
                        "owner": "HP_V8",
                        "experiments": [
                            {
                                "experiment_id": "exp_fixture",
                                "archive_path": archive.relative_to(
                                    finalize.ROOT
                                ).as_posix(),
                                "verification": {"status": "pass"},
                            }
                        ],
                    }
                ]
            }
            calls: list[tuple[str, ...]] = []
            with (
                mock.patch.object(
                    finalize,
                    "parse_args",
                    return_value=SimpleNamespace(
                        experiment_id="exp_fixture",
                        skip_tree_hash=False,
                        sealed_manifest=seal,
                    ),
                ),
                mock.patch.object(finalize, "load_catalog", return_value=catalog),
                mock.patch.object(
                    finalize,
                    "run",
                    side_effect=lambda *args: calls.append(tuple(args)),
                ),
            ):
                self.assertEqual(finalize.main(), 0)

        self.assertIn("--sealed-manifest", calls[1])
        self.assertIn(str(seal.resolve()), calls[1])


if __name__ == "__main__":
    unittest.main()
