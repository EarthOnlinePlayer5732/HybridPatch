from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import process_experiment as process


class ProcessExperimentTests(unittest.TestCase):
    def facts(self) -> dict:
        return {
            "owner": "HP_V8",
            "experiment_id": "exp_test",
            "archive_ref": "HP_V8/exp_test",
            "experiment_plan_ref": "docs/experiment_plans/exp_test.md",
            "experiment_plan_sha256": "b" * 64,
            "input_sha256": "a" * 64,
            "planned_sample_ids": ["sample1", "sample2"],
            "planned_sample_count": 2,
            "complete_all_methods_sample_ids": ["sample1"],
            "methods": ["hybridpatch", "fullrewrite"],
            "round_trips": 2,
            "committed_backward_rows": 5,
            "protocol_revision": "hybridpatch/8",
            "per_method_sample_counts": {
                "hybridpatch": {
                    "sample1": {"forward": 2, "backward": 2, "total": 4},
                    "sample2": {"forward": 1, "backward": 1, "total": 2},
                },
                "fullrewrite": {
                    "sample1": {"forward": 2, "backward": 2, "total": 4},
                },
            },
        }

    def review(self) -> dict:
        return {
            "schema": "hybridpatch.experiment_record_review/1",
            "owner": "HP_V8",
            "experiment_id": "exp_test",
            "prepared_input_sha256": "a" * 64,
            "review_confirmations": {
                "canonical_scope_reviewed": True,
                "failure_and_exclusions_reviewed": True,
                "source_reports_reviewed": True,
                "claim_boundary_reviewed": True,
                "complete_sample_exclusions_pre_registered": False,
            },
            "review_provenance": {
                "reviewed_by": "main-agent",
                "reviewed_at": "2026-07-17T12:00:00+08:00",
                "review_method": "main synthesis after independent read-only review",
                "independent_reviews": [
                    {
                        "role": "result_integrity_audit",
                        "reviewer": "subagent-a",
                        "verdict": "PASS",
                        "summary": "Fixture evidence is internally consistent.",
                    }
                ],
                "independent_review_exception": None,
            },
            "catalog_entry": {
                "experiment_id": "exp_test",
                "scope_id": "complete1",
                "archive_path": "HP_V8/exp_test",
                "status": "diagnostic",
                "lifecycle_status": "incomplete",
                "evidence_role": "diagnostic",
                "research_stage": "val",
                "purpose": "fixture",
                "round_trips": 2,
                "expected_sample_count": 2,
                "sample_policy": "complete_all_methods",
                "excluded_samples": {},
                "protocol_revision": "hybridpatch/8",
                "dataset": "DELEGATE-52",
                "split": "fixture",
                "task_plan_source": "fixture plans",
                "arms": {
                    "hybridpatch": "live",
                    "fullrewrite": "live",
                },
                "code_reference": "HP_V8/VERSION.md",
                "comparison_policy": "diagnostic complete pairs",
                "canonical_source_report": "HP_V8/exp_test/analysis/comparison.md",
                "source_reports": [
                    {
                        "path": "docs/experiment_plans/exp_test.md",
                        "role": "pre_registered_experiment_plan",
                    },
                    {
                        "path": "HP_V8/exp_test/analysis/comparison.md",
                        "role": "source_analysis",
                    }
                ],
                "verification": {
                    "status": "pass",
                    "replayed_backward_rows": 5,
                    "canonical_backward_rows": "AUTO",
                    "source": "HP_V8/VERSION.md",
                    "log": "HP_V8/exp_test/analysis/verification.log",
                    "exceptions": [],
                },
                "known_exceptions": [],
                "raw_retention": "formal",
                "raw_credential_scan": "not_rechecked",
                "paper_reporting": None,
            },
        }

    def test_frozen_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "frozen"):
            process.resolve_experiment(
                process.ROOT
                / "HP_V7"
                / "exp_20260712_hybridv7val40_transportv3"
            )

    def test_outside_path_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "inside this repository"):
            process.resolve_experiment(Path("C:/not-this-repository/exp_test"))

    def test_placeholder_detection_is_recursive(self) -> None:
        self.assertTrue(process.contains_placeholder({"x": ["review_required"]}))
        self.assertFalse(process.contains_placeholder({"x": ["reviewed"]}))

    def test_result_protocols_ignores_unparsed_kept_context_default(self) -> None:
        rows = [
            {
                "bdpatch": {
                    "hybrid": {
                        "protocol_rev": "hybridpatch/8",
                        "protocol_version": "hybridpatch/8",
                        "route": "local_patch",
                    }
                }
            },
            {
                "bdpatch": {
                    "hybrid": {
                        "protocol_rev": None,
                        "protocol_version": "hybridpatch/1",
                        "invalid_json": True,
                        "route": None,
                        "failed_step_kept_context": True,
                    }
                }
            },
        ]

        self.assertEqual(process.result_protocols(rows), ["hybridpatch/8"])

    def test_result_protocols_keeps_legacy_parsed_fallback(self) -> None:
        rows = [
            {
                "bdpatch": {
                    "hybrid": {
                        "protocol_version": "hybridpatch/7",
                        "invalid_json": False,
                        "route": "bulk_patch",
                        "failed_step_kept_context": False,
                    }
                }
            }
        ]

        self.assertEqual(process.result_protocols(rows), ["hybridpatch/7"])

    def test_result_protocols_accepts_legacy_route_share_key(self) -> None:
        rows = [
            {
                "bdpatch": {
                    "hybrid": {
                        "protocol_version": "hybridpatch/6",
                        "route": None,
                        "route_share_key": "local_patch",
                    }
                }
            }
        ]

        self.assertEqual(process.result_protocols(rows), ["hybridpatch/6"])

    def test_result_protocols_prefers_direct_revision_without_route(self) -> None:
        rows = [
            {
                "bdpatch": {
                    "hybrid": {
                        "protocol_rev": "hybridpatch/8",
                        "protocol_version": "hybridpatch/1",
                        "route": None,
                        "route_share_key": None,
                    }
                }
            }
        ]

        self.assertEqual(process.result_protocols(rows), ["hybridpatch/8"])

    def test_result_protocols_prefers_direct_revision_over_fallback(self) -> None:
        rows = [
            {
                "bdpatch": {
                    "hybrid": {
                        "protocol_rev": "hybridpatch/8",
                        "protocol_version": "hybridpatch/1",
                        "route": "local_patch",
                    }
                }
            }
        ]

        self.assertEqual(process.result_protocols(rows), ["hybridpatch/8"])

    def test_review_computes_canonical_counts(self) -> None:
        entry = process.validate_review(self.review(), self.facts())
        self.assertEqual(entry["verification"]["canonical_backward_rows"], 4)
        self.assertEqual(
            entry["verification"]["canonical_paired_backward_rows"],
            2,
        )

    def test_complete_sample_exclusion_requires_confirmation(self) -> None:
        review = self.review()
        review["catalog_entry"]["sample_policy"] = "exclude"
        review["catalog_entry"]["excluded_samples"] = {
            "sample1": "fixture exclusion",
            "sample2": "incomplete",
        }
        with self.assertRaisesRegex(RuntimeError, "pre-registration"):
            process.validate_review(review, self.facts())

    def test_review_placeholders_are_rejected(self) -> None:
        review = self.review()
        review["catalog_entry"]["scope_id"] = process.PLACEHOLDER
        with self.assertRaisesRegex(RuntimeError, "REVIEW_REQUIRED"):
            process.validate_review(review, self.facts())

    def test_review_requires_pre_registered_plan_source(self) -> None:
        review = self.review()
        review["catalog_entry"]["source_reports"] = [
            review["catalog_entry"]["source_reports"][1]
        ]
        with self.assertRaisesRegex(RuntimeError, "pre-registered experiment plan"):
            process.validate_review(review, self.facts())

    def test_review_requires_review_provenance(self) -> None:
        review = self.review()
        review["review_provenance"]["reviewed_by"] = process.PLACEHOLDER
        with self.assertRaisesRegex(RuntimeError, "reviewed_by"):
            process.validate_review(review, self.facts())

    def test_experiment_plan_is_required_and_hashed(self) -> None:
        with tempfile.TemporaryDirectory(
            dir=process.ROOT / "docs" / "experiment_plans"
        ) as directory:
            plan_root = Path(directory)
            with mock.patch.object(process, "EXPERIMENT_PLANS", plan_root):
                with self.assertRaisesRegex(RuntimeError, "pre-experiment plan"):
                    process.experiment_plan("HP_V8", "exp_test")
                plan_path = plan_root / "exp_test.md"
                plan_path.write_text(
                    "# 实验计划：`exp_test`\n\n- owner：`HP_V8`\n",
                    encoding="utf-8",
                )
                plan = process.experiment_plan("HP_V8", "exp_test")
                plan_path.write_text(
                    "# 实验计划：`exp_test`\n\n"
                    "- owner：`HP_V8 | Baseline | transport`\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(RuntimeError, "owner HP_V8"):
                    process.experiment_plan("HP_V8", "exp_test")
        self.assertTrue(plan["ref"].endswith("/exp_test.md"))
        self.assertRegex(plan["sha256"], r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
