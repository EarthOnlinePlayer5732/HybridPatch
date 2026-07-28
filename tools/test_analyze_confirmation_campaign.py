#!/usr/bin/env python3
"""Synthetic zero-API tests for analyze_confirmation_campaign.py."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("analyze_confirmation_campaign.py")
SPEC = importlib.util.spec_from_file_location("analyze_confirmation_campaign", SCRIPT)
assert SPEC and SPEC.loader
analysis = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(analysis)


class ConfirmationCampaignAnalysisTests(unittest.TestCase):
    samples = ["alpha1", "beta2"]
    round_trips = 2

    def test_analysis_rejects_non_quiescent_event_metadata_before_results(
            self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch.object(
                analysis,
                "read_quiescent_run_metadata",
                side_effect=RuntimeError("stale event metadata"),
            ) as reader:
                with self.assertRaisesRegex(
                        analysis.AnalysisError, "stale event metadata"):
                    analysis.analyze_campaign(root)
            reader.assert_called_once_with(
                root.resolve(),
                repository_root=analysis.ROOT.resolve(),
                required=False,
            )

    def _write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )

    def _row(
        self,
        method: str,
        sample: str,
        rt: int,
        direction: str,
        score: float,
        call_id: str,
    ) -> dict:
        bdpatch = {
            "actual_method": "hybridpatch" if method == "hybridpatch" else "fullrewrite",
            "preservation_violations": 0,
            "preservation_rate": 1.0,
        }
        if method == "hybridpatch":
            bdpatch["hybrid"] = {
                "route": "local_patch" if sample == "alpha1" else "bulk_patch",
                "prompt_profile": "default",
                "failed_step_kept_context": False,
                "invalid_json": False,
                "schema_error_count": 0,
                "repair": {
                    "attempted": rt == 2 and direction == "forward",
                    "used": rt == 2 and direction == "forward",
                    "success": rt == 2 and direction == "forward",
                },
            }
        return {
            "sample_id": sample,
            "method": method,
            "round_trip_num": rt,
            "round_trip_direction": direction,
            "evaluation": {"score": score},
            "api_call_ids": [call_id],
            "input_tokens": 10,
            "cache_read_input_tokens": 2,
            "cache_creation_input_tokens": 0,
            "output_tokens": 5,
            "total_tokens": 17,
            "total_usd": 0.001,
            "bdpatch": bdpatch,
        }

    def _fixture(self, root: Path) -> None:
        assignments = [
            {
                "sample": sample,
                "key_label": f"KEY_{index + 1:02d}",
                "methods": list(analysis.METHODS),
            }
            for index, sample in enumerate(self.samples)
        ]
        manifest = {
            "schema": "anchorpatch.paired_campaign_manifest/1",
            "experiment_id": "exp_synthetic_confirmation",
            "config": {
                "samples": self.samples,
                "method_set": list(analysis.METHODS),
                "num_round_trips": self.round_trips,
                "seed": 42,
                "model": "minimax-m3",
                "transport_revision": "opencode_anthropic_sdk/4",
            },
            "assignments": assignments,
            "task_plans": {sample: {} for sample in self.samples},
        }
        (root / "dispatch_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )

        scores = {
            "hybridpatch": {
                "alpha1": {1: 0.8, 2: 0.6},
                "beta2": {1: 0.6, 2: 0.8},
            },
            "fullrewrite": {
                "alpha1": {1: 0.5, 2: 0.7},
                "beta2": {1: 0.7, 2: 0.6},
            },
        }
        api_calls: list[dict] = []
        ledger: list[dict] = []
        outcomes: list[dict] = []
        call_index = 0
        for method in analysis.METHODS:
            for sample in self.samples:
                rows = []
                for rt in range(1, self.round_trips + 1):
                    for direction in ("forward", "backward"):
                        call_index += 1
                        call_id = f"call{call_index:04d}"
                        rows.append(
                            self._row(
                                method,
                                sample,
                                rt,
                                direction,
                                scores[method][sample][rt],
                                call_id,
                            )
                        )
                        kind = f"{method}_primary"
                        semantic_id = (
                            f"{method}/{sample}/rt{rt:02d}/{direction}/{kind}/g000"
                        )
                        api_calls.append(
                            {
                                "request_id": call_id,
                                "sample": sample,
                                "method": method,
                                "rt_index": rt,
                                "direction": direction,
                                "call_kind": kind,
                                "semantic_call_id": semantic_id,
                            }
                        )
                        journal = {
                            "call_id": call_id,
                            "semantic_call_id": semantic_id,
                            "result": {
                                "call_kind": kind,
                                "input_tokens": 10,
                                "cache_read_input_tokens": 2,
                                "cache_creation_input_tokens": 0,
                                "output_tokens": 5,
                                "total_tokens": 17,
                                "total_usd": 0.001,
                            },
                        }
                        journal_path = root / "api_journal" / f"{call_id}.response.json"
                        journal_path.parent.mkdir(parents=True, exist_ok=True)
                        journal_path.write_text(json.dumps(journal), encoding="utf-8")
                        ledger.append(
                            {
                                "event": "attempt_end",
                                "call_id": call_id,
                                "semantic_call_id": semantic_id,
                                "final_usage_seen": True,
                                "status": "success",
                            }
                        )
                self._write_jsonl(root / method / f"{sample}.jsonl", rows)

        # A failed generation attempt has no final usage and must be disclosed,
        # but it does not make the committed result grid incomplete.
        ledger.append(
            {
                "event": "attempt_end",
                "call_id": "failed0001",
                "semantic_call_id": (
                    "hybridpatch/alpha1/rt02/forward/hybridpatch_primary/g000"
                ),
                "final_usage_seen": False,
                "generation_delta_seen": True,
                "status": "retryable_error",
                "error_type": "incomplete_stream",
            }
        )
        for sample in self.samples:
            outcomes.append({"sample": sample, "status": "finished"})
        self._write_jsonl(root / "api_calls.jsonl", api_calls)
        self._write_jsonl(root / "api_attempt_ledger.jsonl", ledger)
        self._write_jsonl(root / "sample_outcomes.jsonl", outcomes)

    def test_complete_report_preserves_raw_and_adds_percent_scale(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            report = analysis.analyze_campaign(
                root, sensitivity_exclude=["beta2"]
            )

            self.assertTrue(report["integrity"]["structural_complete"])
            self.assertFalse(report["integrity"]["formal_reporting_ready"])
            self.assertFalse(
                report["integrity"]["confirmation_identity"]["applicable"]
            )
            self.assertEqual(report["integrity"]["incomplete_samples"], [])
            self.assertEqual(
                report["integrity"]["committed_evaluation_errors"]["count"], 0
            )
            trajectory = report["round_trip_scores"]
            self.assertEqual([row["fixed_n"] for row in trajectory], [2, 2])
            self.assertEqual([row["paired_n"] for row in trajectory], [2, 2])
            self.assertAlmostEqual(trajectory[0]["hybridpatch_mean_raw_0_1"], 0.7)
            self.assertAlmostEqual(trajectory[0]["fullrewrite_mean_raw_0_1"], 0.6)
            self.assertAlmostEqual(trajectory[0]["delta_percentage_points"], 10.0)
            self.assertAlmostEqual(trajectory[1]["delta_percentage_points"], 5.0)

            final = report["rt_final_summary"]
            self.assertEqual(final["win_loss_tie"], {
                "hybridpatch_wins": 1,
                "hybridpatch_losses": 1,
                "ties": 0,
                "tie_tolerance_raw": analysis.TIE_TOLERANCE,
            })
            self.assertAlmostEqual(
                final["display_percent_and_percentage_points"]["delta_mean"],
                5.0,
            )
            self.assertAlmostEqual(
                final["display_percent_and_percentage_points"]["delta_iqr"],
                30.0,
            )
            self.assertEqual(
                final["paired_inference"]["bootstrap_resamples"], 10_000
            )
            self.assertEqual(
                final["paired_inference"]["sign_test_non_ties"], 2
            )
            self.assertAlmostEqual(
                final["paired_inference"]["sign_test_two_sided_p"], 1.0
            )
            self.assertEqual(
                report["critical_failure_at_0_10"]["hybridpatch"]["count"], 1
            )
            self.assertEqual(
                report["usage"]["api_semantic_calls"]
                ["unknown_final_usage_attempts"]["count"],
                1,
            )
            self.assertEqual(
                report["hybridpatch_telemetry"]["preservation"]["violations"], 0
            )
            sensitivity = report["pre_registered_sensitivity"]
            self.assertFalse(sensitivity["headline_replacement"])
            self.assertEqual(sensitivity["excluded_samples"], ["beta2"])
            self.assertEqual(sensitivity["fixed_n"], 1)
            self.assertAlmostEqual(
                sensitivity["rt_final_summary"]
                ["display_percent_and_percentage_points"]["delta_mean"],
                -10.0,
            )

            prefix = root / "analysis" / "confirmation"
            json_path, markdown_path = analysis.write_report(report, prefix)
            saved = json.loads(json_path.read_text(encoding="utf-8"))
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertEqual(saved["score_storage_scale"], "raw_0_1_unchanged")
            self.assertIn("| RT1 | 2 | 2 | 70.000 | 60.000 | +10.000 |", markdown)
            self.assertIn("RT2 by sample", markdown)
            self.assertIn("Pre-registered evaluator-validity sensitivity", markdown)

    def test_committed_evaluation_error_is_scored_zero_and_disclosed(self) -> None:
        rows = {
            ("fullrewrite", "alpha1", 1, "backward"): {
                "evaluation": {"error": "runtime evaluator failure"}
            }
        }
        summary = analysis._committed_evaluation_errors(rows)
        self.assertEqual(summary["count"], 1)
        self.assertEqual(summary["by_method"], {"fullrewrite": 1})
        self.assertEqual(
            analysis._formal_score(
                rows[("fullrewrite", "alpha1", 1, "backward")], "unit"
            ),
            0.0,
        )

    def test_incomplete_scope_rejected_by_default_and_explicitly_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            path = root / "hybridpatch" / "beta2.jsonl"
            rows = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            rows = [
                row
                for row in rows
                if not (
                    row["round_trip_num"] == 2
                    and row["round_trip_direction"] == "backward"
                )
            ]
            self._write_jsonl(path, rows)

            with self.assertRaises(analysis.CampaignIncompleteError):
                analysis.analyze_campaign(root)

            report = analysis.analyze_campaign(root, allow_incomplete=True)
            self.assertFalse(report["integrity"]["formal_reporting_ready"])
            self.assertEqual(report["integrity"]["incomplete_samples"], ["beta2"])
            self.assertEqual(report["integrity"]["complete_pair_sample_count"], 1)
            self.assertEqual(report["round_trip_scores"][1]["fixed_n"], 2)
            self.assertEqual(report["round_trip_scores"][1]["paired_n"], 1)
            self.assertEqual(
                report["critical_failure_at_0_10"]["fullrewrite"]["count"], 0
            )
            self.assertEqual(
                report["campaign_observed_critical_failure_at_0_10"]
                ["fullrewrite"]["count"],
                1,
            )
            bounds = report["post_hoc_missing_endpoint_sensitivity"]
            self.assertFalse(bounds["is_imputation"])
            self.assertAlmostEqual(
                bounds["planned_n_mean_delta_bounds_percentage_points"][0],
                -55.0,
            )
            self.assertAlmostEqual(
                bounds["planned_n_mean_delta_bounds_percentage_points"][1],
                45.0,
            )
            beta = next(
                row
                for row in report["rt_final_by_sample"]
                if row["sample_id"] == "beta2"
            )
            self.assertIsNone(beta["delta_percentage_points"])

    def test_failure_backed_committed_cell_is_not_structurally_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            journal = root / "api_journal" / "call0001.response.json"
            journal.unlink()

            with self.assertRaises(analysis.CampaignIncompleteError):
                analysis.analyze_campaign(root)

            report = analysis.analyze_campaign(root, allow_incomplete=True)
            self.assertFalse(report["integrity"]["structural_complete"])
            failure_backed = report["usage"]["api_semantic_calls"][
                "failure_backed_committed_cells"
            ]
            self.assertEqual(len(failure_backed), 1)
            self.assertEqual(failure_backed[0]["sample_id"], "alpha1")

    def test_response_replay_is_backed_by_source_journal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            result_path = root / "fullrewrite" / "alpha1.jsonl"
            result_rows = [
                json.loads(line)
                for line in result_path.read_text(encoding="utf-8").splitlines()
            ]
            source_id = result_rows[0]["api_call_ids"][0]
            replay_id = "replay0001"
            result_rows[0]["api_call_ids"] = [replay_id]
            self._write_jsonl(result_path, result_rows)

            calls_path = root / "api_calls.jsonl"
            calls = [
                json.loads(line)
                for line in calls_path.read_text(encoding="utf-8").splitlines()
            ]
            source = next(
                row for row in calls if row["request_id"] == source_id
            )
            source.update({
                "provider_called": True,
                "provider_request_id": "provider-1",
                "content_sha256": "c" * 64,
            })
            replay = dict(source)
            replay.update({
                "request_id": replay_id,
                "provider_called": False,
                "response_replayed": True,
                "replayed_from_call_id": source_id,
            })
            calls.append(replay)
            self._write_jsonl(calls_path, calls)

            report = analysis.analyze_campaign(root)

            api = report["usage"]["api_semantic_calls"]
            self.assertTrue(report["integrity"]["structural_complete"])
            self.assertEqual(api["response_replay_calls"], 1)
            self.assertEqual(api["indirectly_referenced_source_calls"], 1)
            self.assertEqual(api["failure_backed_committed_cells"], [])

    def test_invalid_response_replay_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            calls_path = root / "api_calls.jsonl"
            calls = [
                json.loads(line)
                for line in calls_path.read_text(encoding="utf-8").splitlines()
            ]
            replay = dict(calls[0])
            replay.update({
                "request_id": "replay0001",
                "provider_called": True,
                "response_replayed": True,
                "replayed_from_call_id": calls[0]["request_id"],
            })
            calls.append(replay)
            self._write_jsonl(calls_path, calls)

            report = analysis.analyze_campaign(root, allow_incomplete=True)

            self.assertIn(
                "invalid API response replay chain (1 calls)",
                report["integrity"]["api_integrity_problems"],
            )

    def test_valid_unreferenced_replay_is_audit_evidence_not_api_gap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            calls_path = root / "api_calls.jsonl"
            calls = [
                json.loads(line)
                for line in calls_path.read_text(encoding="utf-8").splitlines()
            ]
            source = calls[0]
            source.update({
                "provider_called": True,
                "provider_request_id": "provider-1",
                "content_sha256": "c" * 64,
            })
            replay = dict(source)
            replay.update({
                "request_id": "replay0001",
                "provider_called": False,
                "response_replayed": True,
                "replayed_from_call_id": source["request_id"],
            })
            calls.append(replay)
            self._write_jsonl(calls_path, calls)

            report = analysis.analyze_campaign(root)

            api = report["usage"]["api_semantic_calls"]
            self.assertTrue(report["integrity"]["structural_complete"])
            self.assertEqual(api["unreferenced_replay_calls"], 1)

    def test_complete_thinking_only_response_keeps_its_journal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            calls_path = root / "api_calls.jsonl"
            calls = [
                json.loads(line)
                for line in calls_path.read_text(encoding="utf-8").splitlines()
            ]
            calls[0].update({
                "stream_complete": True,
                "error_type": "thinking_budget_exhausted",
                "response_classification": "thinking_budget_exhausted",
                "stop_reason": "max_tokens",
            })
            self._write_jsonl(calls_path, calls)

            report = analysis.analyze_campaign(root)

            self.assertTrue(report["integrity"]["structural_complete"])
            self.assertEqual(
                report["integrity"]["api_integrity_problems"], []
            )

    def test_frozen_confirmation_identity_binds_selection_and_policy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository_root = Path(directory)
            samples = [f"sample{index:02d}" for index in range(68)]
            selection_path = repository_root / analysis.CONFIRMATION_SELECTION_PATH
            selection_path.parent.mkdir(parents=True, exist_ok=True)
            selection_path.write_text(
                json.dumps({
                    "experiment_id": analysis.CONFIRMATION_EXPERIMENT_ID,
                    "selected_sample_ids": samples,
                }),
                encoding="utf-8",
            )
            assignments = [
                {
                    "sample": sample,
                    "methods": (
                        ["hybridpatch", "fullrewrite"]
                        if index < 34
                        else ["fullrewrite", "hybridpatch"]
                    ),
                }
                for index, sample in enumerate(samples)
            ]
            manifest = {
                "experiment_id": analysis.CONFIRMATION_EXPERIMENT_ID,
                "run_git_commit": "a" * 40,
                "git_tree_state": "clean",
                "config": {
                    "campaign_role": "confirmation",
                    "seed": 42,
                    "model": "minimax-m3",
                    "max_tokens": 131072,
                    "distractor": True,
                    "transport_revision": "opencode_anthropic_sdk/4",
                },
                "assignments": assignments,
                "assignment_waves": [
                    {"worker_count": 52},
                    {"worker_count": 16},
                ],
                "selection_manifest": {
                    "path": analysis.CONFIRMATION_SELECTION_PATH,
                    "sha256": analysis._sha256(selection_path),
                    **analysis.CONFIRMATION_SELECTION_COUNTS,
                },
                "analysis_policy": {
                    "schema": "anchorpatch.confirmation_analysis_policy/1",
                    "sensitivity_sets": [
                        ["python4"],
                        ["audiosyn1"],
                        ["python4", "audiosyn1"],
                    ],
                    "bootstrap": {"seed": 42, "resamples": 10000},
                    "critical_failure": {
                        "metric": "CriticalFailure",
                        "threshold": 0.10,
                    },
                    "evaluator_error_policy": {
                        "backward_rows_require": (
                            "finite_score_0_1_or_nonempty_error"
                        ),
                        "error_row_score": 0.0,
                        "error_rows_reported_separately": True,
                    },
                    "known_committed_usage_wave_boundary_stop": {
                        "scope": "wave_boundary_committed_result_rows",
                        "usd_threshold": 130.0,
                    },
                },
            }
            identity = analysis._confirmation_identity(
                manifest,
                samples,
                10,
                repository_root=repository_root,
            )
            self.assertTrue(identity["applicable"])
            self.assertTrue(identity["valid"], identity["problems"])

    def test_postrun_verification_requires_both_proofs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            analysis_dir = root / "analysis"
            analysis_dir.mkdir()
            (analysis_dir / "verification.log").write_text(
                "exit_code=0\n"
                "HONESTY GATE: PASS — 1360 backward RS independently "
                "reproduced from raw responses\n",
                encoding="utf-8",
            )
            (analysis_dir / "strict_inspection_postrun.json").write_text(
                json.dumps({"errors": [], "preservation_violations": 0}),
                encoding="utf-8",
            )
            proof = analysis._postrun_verification(
                root, expected_backward_rows=1360
            )
            self.assertTrue(proof["valid"], proof["problems"])

    def test_postrun_accepts_registered_sample_level_evaluator_incomplete(
            self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            analysis_dir = root / "analysis"
            analysis_dir.mkdir()
            (analysis_dir / "verification.log").write_text(
                "exit_code=0\n"
                "HONESTY GATE: PASS — 8 backward RS independently "
                "reproduced from raw responses\n",
                encoding="utf-8",
            )
            source_log = root / "dispatch_logs" / "calendar5.log"
            source_log.parent.mkdir()
            source_log.write_text("BrokenCalendarProperty\n", encoding="utf-8")
            self._write_jsonl(
                root / "evaluator_incomplete_samples.jsonl",
                [{
                    "schema": "anchorpatch.evaluator_incomplete/1",
                    "sample": "calendar5",
                    "status": "evaluator_incomplete",
                    "disposition": "cancel_sample_continue_campaign",
                    "failure_stage": "evaluator",
                    "result_committed_for_failed_step": False,
                    "score_imputed": False,
                    "source_log": "dispatch_logs/calendar5.log",
                    "source_log_sha256": analysis._sha256(source_log),
                }],
            )
            raw_error = "latest run_metadata invocation failed: calendar5"
            (analysis_dir / "strict_inspection_postrun.json").write_text(
                json.dumps({
                    "errors": [raw_error],
                    "preservation_violations": 0,
                }),
                encoding="utf-8",
            )

            proof = analysis._postrun_verification(
                root, expected_backward_rows=8
            )

            self.assertTrue(proof["valid"], proof["problems"])
            self.assertEqual(
                proof["accepted_sample_level_incomplete_errors"], [raw_error]
            )
            self.assertEqual(proof["unaccepted_inspection_errors"], [])

    def test_postrun_accepts_formal_v2_evaluator_incomplete_evidence(
            self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            analysis_dir = root / "analysis"
            analysis_dir.mkdir()
            (analysis_dir / "verification.log").write_text(
                "exit_code=0\n"
                "HONESTY GATE: PASS — 8 backward RS independently "
                "reproduced from raw responses\n",
                encoding="utf-8",
            )
            shared = {
                "sample": "calendar5",
                "invocation_id": "invocation-calendar5",
                "status": "evaluator_incomplete",
                "failure_stage": "evaluator",
                "result_committed_for_failed_step": False,
                "score_imputed": False,
            }
            self._write_jsonl(
                root / "evaluator_incomplete_samples.jsonl",
                [{
                    "schema": "anchorpatch.evaluator_incomplete/2",
                    "disposition": "cancel_sample_continue_campaign",
                    **shared,
                }],
            )
            self._write_jsonl(
                root / "sample_outcomes.jsonl",
                [{"schema": "anchorpatch.sample_outcome/1", **shared}],
            )
            self._write_jsonl(
                root / "run_metadata.jsonl",
                [{
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": shared["invocation_id"],
                    "samples": [shared["sample"]],
                    "status": "evaluator_incomplete",
                }],
            )
            raw_error = "latest run_metadata invocation failed: calendar5"
            (analysis_dir / "strict_inspection_postrun.json").write_text(
                json.dumps({
                    "errors": [raw_error],
                    "preservation_violations": 0,
                }),
                encoding="utf-8",
            )

            proof = analysis._postrun_verification(
                root, expected_backward_rows=8
            )

            self.assertTrue(proof["valid"], proof["problems"])
            self.assertEqual(
                proof["accepted_sample_level_incomplete_errors"], [raw_error]
            )

    def test_critical_failure_threshold_uses_float_tolerance(self) -> None:
        scores = {
            ("fullrewrite", "chess4", 1): 0.8978427515498331,
            ("fullrewrite", "chess4", 2): 0.7978427515498331,
        }

        result = analysis._critical_failures(
            scores, ["chess4"], 2, "fullrewrite"
        )

        self.assertEqual(result["count"], 1)

    def test_mixed_confirmation_identity_and_cohort_balance(self) -> None:
        repository_root = SCRIPT.parents[1]
        selection_path = (
            repository_root / analysis.MIXED_CONFIRMATION_SELECTION_PATH
        )
        selection = json.loads(selection_path.read_text(encoding="utf-8"))
        samples = selection["selected_sample_ids"]
        unseen = selection["cohorts"]["method_unseen"]["selected_sample_ids"]
        historical = selection["cohorts"][
            "historical_hp_method_exposed"]["selected_sample_ids"]
        assignments = []
        for index, sample in enumerate(unseen):
            assignments.append({
                "sample": sample,
                "methods": (
                    ["hybridpatch", "fullrewrite"]
                    if index < 30 else ["fullrewrite", "hybridpatch"]
                ),
            })
        for index, sample in enumerate(historical):
            assignments.append({
                "sample": sample,
                "methods": (
                    ["hybridpatch", "fullrewrite"]
                    if index < 20 else ["fullrewrite", "hybridpatch"]
                ),
            })
        manifest = {
            "experiment_id": analysis.MIXED_CONFIRMATION_EXPERIMENT_ID,
            "run_git_commit": "b" * 40,
            "git_tree_state": "clean",
            "config": {
                "campaign_role": "confirmation",
                "seed": 42,
                "model": "minimax-m3",
                "max_tokens": 131072,
                "distractor": True,
                "transport_revision": "opencode_anthropic_sdk/4",
            },
            "assignments": assignments,
            "assignment_waves": [
                {"worker_count": 52},
                {"worker_count": 48},
            ],
            "selection_manifest": {
                "path": analysis.MIXED_CONFIRMATION_SELECTION_PATH,
                "sha256": analysis._sha256(selection_path),
                **analysis.MIXED_CONFIRMATION_SELECTION_COUNTS,
            },
            "analysis_policy": {
                "schema": "anchorpatch.mixed_confirmation_analysis_policy/1",
                "headline_scope": "selected100",
                "pre_registered_cohorts": [
                    "method_unseen", "historical_hp_method_exposed"],
                "bootstrap": {"seed": 42, "resamples": 10000},
                "critical_failure": {
                    "metric": "CriticalFailure", "threshold": 0.10},
                "evaluator_error_policy": {
                    "backward_rows_require": (
                        "finite_score_0_1_or_nonempty_error"),
                    "error_row_score": 0.0,
                    "error_rows_reported_separately": True,
                },
                "known_committed_usage_wave_boundary_stop": {
                    "scope": "wave_boundary_committed_result_rows",
                    "usd_threshold": 130.0,
                },
            },
        }
        identity = analysis._confirmation_identity(
            manifest, samples, 10, repository_root=repository_root)
        self.assertTrue(identity["valid"], identity["problems"])
        self.assertEqual(len(identity["cohort_samples"]["method_unseen"]), 60)
        self.assertEqual(
            len(identity["cohort_samples"]["historical_hp_method_exposed"]),
            40,
        )

    def test_rt_final_rows_include_pre_registered_cohort(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            identity = {
                "applicable": False,
                "valid": False,
                "problems": [],
                "cohort_samples": {
                    "method_unseen": ["alpha1"],
                    "historical_hp_method_exposed": ["beta2"],
                },
            }
            with mock.patch.object(
                analysis, "_confirmation_identity", return_value=identity
            ):
                report = analysis.analyze_campaign(root)
            cohorts = {
                row["sample_id"]: row["cohort"]
                for row in report["rt_final_by_sample"]
            }
            self.assertEqual(cohorts, {
                "alpha1": "method_unseen",
                "beta2": "historical_hp_method_exposed",
            })

    def test_confirmation_rejects_unregistered_sensitivity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            identity = {
                "applicable": True,
                "valid": True,
                "problems": [],
            }
            with mock.patch.object(
                analysis, "_confirmation_identity", return_value=identity
            ):
                with self.assertRaisesRegex(
                    analysis.AnalysisError, "not one of the pre-registered sets"
                ):
                    analysis.analyze_campaign(
                        root,
                        allow_incomplete=True,
                        sensitivity_exclude=["beta2"],
                    )


if __name__ == "__main__":
    unittest.main()
