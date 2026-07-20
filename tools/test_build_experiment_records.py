#!/usr/bin/env python3
"""Zero-API provenance tests for build_experiment_records.py."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("build_experiment_records.py")
SPEC = importlib.util.spec_from_file_location("build_experiment_records", SCRIPT)
assert SPEC and SPEC.loader
records = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(records)


class BuildExperimentRecordProvenanceTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
