#!/usr/bin/env python3
"""Zero-API provenance tests for build_experiment_records.py."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("build_experiment_records.py")
SPEC = importlib.util.spec_from_file_location("build_experiment_records", SCRIPT)
assert SPEC and SPEC.loader
records = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(records)


class BuildExperimentRecordProvenanceTests(unittest.TestCase):
    def test_builder_rejects_non_quiescent_event_metadata_before_scan(self) -> None:
        with tempfile.TemporaryDirectory(
            dir=records.ROOT, prefix="tmp_event_builder_"
        ) as directory:
            archive = Path(directory)
            entry = {
                "archive_path": archive.relative_to(records.ROOT).as_posix(),
            }
            with mock.patch.object(
                records,
                "read_quiescent_run_metadata",
                side_effect=RuntimeError("stale event metadata"),
            ) as reader:
                with self.assertRaisesRegex(RuntimeError, "stale event metadata"):
                    records.build_experiment(
                        {"owner": "HP_V8"},
                        entry,
                        catalog_digest="a" * 64,
                        skip_tree_hash=True,
                        check=True,
                    )
            reader.assert_called_once_with(
                archive,
                repository_root=records.ROOT,
                owner="HP_V8",
                required=False,
            )

    def test_single_identity_keeps_legacy_semantics(self) -> None:
        fingerprint = {
            "hybrid_prompt.py": "prompt-a",
            "hybrid_executor.py": "executor-a",
        }

        result = records.resolve_metadata_code_provenance(
            Path("unused"),
            [{
                "run_git_commit": "a" * 40,
                "code_fingerprint": fingerprint,
            }],
        )

        self.assertEqual(result["run_git_commit"], "a" * 40)
        self.assertEqual(result["code_fingerprints"], [fingerprint])
        self.assertIsNone(result["campaign_recovery_authorization"])

    def test_single_commit_keeps_legacy_multiple_fingerprints(self) -> None:
        result = records.resolve_metadata_code_provenance(
            Path("unused"),
            [
                {
                    "run_git_commit": "a" * 40,
                    "code_fingerprint": {"runner.py": "one"},
                },
                {
                    "run_git_commit": "a" * 40,
                    "code_fingerprint": {"runner.py": "two"},
                },
            ],
        )

        self.assertEqual(result["run_git_commits"], ["a" * 40])
        self.assertEqual(len(result["code_fingerprints"]), 2)
        self.assertIsNone(result["campaign_recovery_authorization"])

    def test_dual_identity_requires_and_records_exact_authorization(self) -> None:
        with tempfile.TemporaryDirectory(
            dir=SCRIPT.parents[1], prefix="tmp_record_provenance_"
        ) as directory:
            archive = Path(directory)
            prior_commit = "a" * 40
            recovery_commit = "b" * 40
            prior = {
                "hybrid_prompt.py": "prompt-a",
                "run_meta.py": "run-a",
            }
            recovery = {
                "hybrid_prompt.py": "prompt-a",
                "run_meta.py": "run-b",
            }
            authorization = {
                "schema": "anchorpatch.campaign_recovery_authorization/1",
                "authorization_id": "fixture-recovery",
                "prior_git_commit": prior_commit,
                "recovery_git_commit": recovery_commit,
                "prior_code_fingerprint": prior,
                "recovery_code_fingerprint": recovery,
                "changed_code_fingerprint_keys": ["run_meta.py"],
                "recovery_git_tree_state": "clean",
            }
            authorization_path = archive / "campaign_recovery_authorization.json"
            authorization_path.write_text(
                json.dumps(authorization, sort_keys=True), encoding="utf-8"
            )
            authorization_sha256 = hashlib.sha256(
                authorization_path.read_bytes()
            ).hexdigest()
            (archive / "dispatch_log.jsonl").write_text(
                json.dumps({
                    "campaign_recovery_authorization_id": "fixture-recovery",
                    "campaign_recovery_authorization_sha256": authorization_sha256,
                    "prior_git_commit": prior_commit,
                    "recovery_git_commit": recovery_commit,
                }) + "\n",
                encoding="utf-8",
            )
            metadata = [
                {
                    "run_git_commit": prior_commit,
                    "code_fingerprint": prior,
                },
                {
                    "run_git_commit": recovery_commit,
                    "code_fingerprint": recovery,
                },
            ]

            result = records.resolve_metadata_code_provenance(
                archive, metadata
            )

            self.assertEqual(result["run_git_commit"], recovery_commit)
            self.assertEqual(
                result["run_git_commits"], [prior_commit, recovery_commit]
            )
            self.assertEqual(
                result["campaign_recovery_authorization"]["sha256"],
                authorization_sha256,
            )
            self.assertEqual(
                records.invariant_fingerprint_value(
                    result["code_fingerprints"], "hybrid_prompt.py"
                ),
                "prompt-a",
            )
            self.assertIsNone(
                records.invariant_fingerprint_value(
                    result["code_fingerprints"], "run_meta.py"
                )
            )

    def test_dual_identity_without_authorization_is_rejected(self) -> None:
        metadata = [
            {
                "run_git_commit": "a" * 40,
                "code_fingerprint": {"run_meta.py": "run-a"},
            },
            {
                "run_git_commit": "b" * 40,
                "code_fingerprint": {"run_meta.py": "run-b"},
            },
        ]
        with tempfile.TemporaryDirectory(
            dir=SCRIPT.parents[1], prefix="tmp_record_provenance_"
        ) as directory:
            with self.assertRaisesRegex(
                RuntimeError, "campaign_recovery_authorization.json is missing"
            ):
                records.resolve_metadata_code_provenance(
                    Path(directory), metadata
                )

    def test_raw_manifest_reuses_sealed_tree_and_compression_digest(self) -> None:
        sealed = {
            "tree": {
                "algorithm": "sha256-tree-v1",
                "tree_sha256": "a" * 64,
                "file_count": 3,
                "size_bytes": 123,
                "skipped_sensitive_files": [],
                "skipped_symlinks": [],
                "credential_scan": {
                    "status": "pass_zero_matches",
                    "exact_local_secret_match_count": 0,
                },
            },
            "archive": {
                "format": "tar+gzip",
                "sha256": "b" * 64,
                "size_bytes": 45,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            manifest = records.raw_manifest_document(
                {
                    "experiment_id": "exp_fixture",
                    "archive_path": "HP_V8/exp_fixture",
                    "raw_retention": "private",
                },
                path,
                "artifact",
                skip_tree_hash=False,
                existing_path=path / "absent.json",
                source_experiments=[],
                sealed_manifest=sealed,
            )

        artifact = manifest["artifacts"][0]
        self.assertEqual(artifact["tree_sha256"], "a" * 64)
        self.assertEqual(artifact["archive_status"], "compressed_private")
        self.assertEqual(artifact["compression"]["sha256"], "b" * 64)

    def test_missing_evaluator_sample_is_not_labeled_infrastructure(self) -> None:
        classified = records.classify_row(
            None,
            missing_reason=(
                "evaluator_incomplete: domain evaluator raised before commit"),
        )

        self.assertEqual(classified["failure_label"], "evaluator_incomplete")
        self.assertEqual(classified["failure_stage"], "evaluator")
        self.assertEqual(classified["commit_outcome"], "missing")


if __name__ == "__main__":
    unittest.main()
