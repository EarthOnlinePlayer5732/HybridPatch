from __future__ import annotations

import hashlib
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import process_experiment as process
import versioned_run_metadata as versioned_metadata


class ProcessExperimentTests(unittest.TestCase):
    def test_event_metadata_uses_owner_quiescent_reader(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)
            for filename in (
                versioned_metadata.EVENTS_FILENAME,
                versioned_metadata.SNAPSHOT_FILENAME,
                versioned_metadata.RECEIPT_FILENAME,
            ):
                (archive / filename).write_text("{}\n", encoding="utf-8")
            expected = [{"schema": "anchorpatch.run_metadata/3"}]
            reader = mock.Mock(return_value=expected)
            owner_module = SimpleNamespace(
                read_quiescent_run_metadata_snapshot=reader
            )
            with mock.patch.object(
                versioned_metadata,
                "_load_version_run_meta",
                return_value=owner_module,
            ) as load_module:
                rows = versioned_metadata.read_quiescent_run_metadata(
                    archive,
                    repository_root=process.ROOT,
                    owner="HP_V8",
                )
            self.assertEqual(rows, expected)
            load_module.assert_called_once_with(str(
                (process.ROOT / "HP_V8" / "src" / "run_meta.py").resolve()
            ))
            reader.assert_called_once_with(str(archive))
            self.assertEqual(
                {path.name for path in
                 versioned_metadata.run_metadata_artifact_paths(archive)},
                {
                    versioned_metadata.EVENTS_FILENAME,
                    versioned_metadata.SNAPSHOT_FILENAME,
                    versioned_metadata.RECEIPT_FILENAME,
                },
            )

    def test_legacy_metadata_bypasses_owner_module(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)
            expected = {"schema": "anchorpatch.run_metadata/3"}
            (archive / versioned_metadata.SNAPSHOT_FILENAME).write_text(
                json.dumps(expected) + "\n", encoding="utf-8"
            )
            with mock.patch.object(
                versioned_metadata, "_load_version_run_meta"
            ) as load_module:
                rows = versioned_metadata.read_quiescent_run_metadata(
                    archive,
                    repository_root=process.ROOT,
                    owner="HP_V8",
                )
            self.assertEqual(rows, [expected])
            load_module.assert_not_called()

    def test_declared_event_metadata_cannot_downgrade_to_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)
            (archive / versioned_metadata.SNAPSHOT_FILENAME).write_text(
                json.dumps({"schema": "anchorpatch.run_metadata/3"}) + "\n",
                encoding="utf-8",
            )
            (archive / "dispatch_manifest.json").write_text(
                json.dumps({
                    "config": {
                        "run_metadata_storage": versioned_metadata.EVENT_STORAGE_V1,
                    },
                }),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "requires a missing"):
                versioned_metadata.read_quiescent_run_metadata(
                    archive,
                    repository_root=process.ROOT,
                    owner="HP_V8",
                )

    def test_pending_only_event_metadata_is_not_treated_as_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)
            pending_path = archive / versioned_metadata.PENDING_FILENAME
            pending_path.write_text("{}", encoding="utf-8")
            reader = mock.Mock(
                side_effect=RuntimeError("pending event intent")
            )
            owner_module = SimpleNamespace(
                read_quiescent_run_metadata_snapshot=reader
            )
            with mock.patch.object(
                versioned_metadata,
                "_load_version_run_meta",
                return_value=owner_module,
            ) as load_module:
                with self.assertRaisesRegex(RuntimeError, "pending event intent"):
                    versioned_metadata.read_quiescent_run_metadata(
                        archive,
                        repository_root=process.ROOT,
                        owner="HP_V8",
                    )
            load_module.assert_called_once()
            reader.assert_called_once_with(str(archive))
            self.assertEqual(
                versioned_metadata.run_metadata_artifact_paths(archive),
                [pending_path],
            )

    def test_collect_facts_rejects_non_quiescent_event_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)
            with mock.patch.object(
                process, "experiment_plan", return_value={}
            ), mock.patch.object(
                process,
                "read_quiescent_run_metadata",
                side_effect=RuntimeError("stale event metadata"),
            ) as reader:
                with self.assertRaisesRegex(RuntimeError, "stale event metadata"):
                    process.collect_facts("HP_V8", archive)
            reader.assert_called_once_with(
                archive,
                repository_root=process.ROOT,
                owner="HP_V8",
            )

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

    def test_review_rejects_catalog_vocab_before_record_build(self) -> None:
        review = self.review()
        review["catalog_entry"]["status"] = "failed_informative"
        with self.assertRaisesRegex(RuntimeError, "catalog_entry.status"):
            process.validate_review(review, self.facts())

        review = self.review()
        review["catalog_entry"]["evidence_role"] = "supporting_confirmation"
        with self.assertRaisesRegex(RuntimeError, "catalog_entry.evidence_role"):
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

    def test_two_fingerprints_require_recovery_authorization(self) -> None:
        metadata = [
            {"run_git_commit": "a" * 40, "code_fingerprint": {"x.py": "1"}},
            {"run_git_commit": "b" * 40, "code_fingerprint": {"x.py": "2"}},
        ]
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            with self.assertRaisesRegex(RuntimeError, "authorization.*missing"):
                process.code_provenance(Path(directory), metadata)

    def test_exact_recovery_authorization_allows_two_fingerprints(self) -> None:
        prior_fingerprint = {"run_meta.py": "old", "runner.py": "same"}
        recovery_fingerprint = {"run_meta.py": "new", "runner.py": "same"}
        prior_commit = "a" * 40
        recovery_commit = "b" * 40
        metadata = [
            {
                "run_git_commit": prior_commit,
                "code_fingerprint": prior_fingerprint,
            },
            {
                "run_git_commit": recovery_commit,
                "code_fingerprint": recovery_fingerprint,
            },
        ]
        authorization = {
            "schema": "anchorpatch.campaign_recovery_authorization/1",
            "authorization_id": "fixture-recovery",
            "prior_git_commit": prior_commit,
            "prior_code_fingerprint": prior_fingerprint,
            "recovery_git_commit": recovery_commit,
            "recovery_git_tree_state": "clean",
            "recovery_code_fingerprint": recovery_fingerprint,
            "changed_code_fingerprint_keys": ["run_meta.py"],
        }
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            archive = Path(directory)
            authorization_path = archive / "campaign_recovery_authorization.json"
            authorization_path.write_text(
                json.dumps(authorization), encoding="utf-8"
            )
            authorization_sha256 = hashlib.sha256(
                authorization_path.read_bytes()
            ).hexdigest()
            (archive / "dispatch_log.jsonl").write_text(
                json.dumps({
                    "event": "user_authorized_direct_wave2_recovery",
                    "campaign_recovery_authorization_id": "fixture-recovery",
                    "campaign_recovery_authorization_sha256": authorization_sha256,
                    "prior_git_commit": prior_commit,
                    "recovery_git_commit": recovery_commit,
                }) + "\n",
                encoding="utf-8",
            )

            provenance = process.code_provenance(archive, metadata)

        self.assertEqual(provenance["run_git_commit"], recovery_commit)
        self.assertEqual(provenance["code_fingerprint"], recovery_fingerprint)
        self.assertEqual(
            provenance["campaign_recovery_authorization"]["sha256"],
            authorization_sha256,
        )

    def test_recovery_authorization_requires_matching_dispatch_witness(self) -> None:
        prior_fingerprint = {"run_meta.py": "old"}
        recovery_fingerprint = {"run_meta.py": "new"}
        metadata = [
            {"run_git_commit": "a" * 40, "code_fingerprint": prior_fingerprint},
            {"run_git_commit": "b" * 40, "code_fingerprint": recovery_fingerprint},
        ]
        authorization = {
            "schema": "anchorpatch.campaign_recovery_authorization/1",
            "authorization_id": "fixture-recovery",
            "prior_git_commit": "a" * 40,
            "prior_code_fingerprint": prior_fingerprint,
            "recovery_git_commit": "b" * 40,
            "recovery_git_tree_state": "clean",
            "recovery_code_fingerprint": recovery_fingerprint,
            "changed_code_fingerprint_keys": ["run_meta.py"],
        }
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            archive = Path(directory)
            (archive / "campaign_recovery_authorization.json").write_text(
                json.dumps(authorization), encoding="utf-8"
            )
            (archive / "dispatch_log.jsonl").write_text(
                json.dumps({"event": "unrelated"}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "dispatch witness"):
                process.code_provenance(archive, metadata)

    def test_complete_pair_view_excludes_incomplete_samples(self) -> None:
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            root = Path(directory)
            archive = root / "archive"
            view = root / "view"
            for method in ("hybridpatch", "fullrewrite"):
                method_dir = archive / method
                method_dir.mkdir(parents=True)
                (method_dir / "complete.jsonl").write_text(
                    '{"sample_id":"complete"}\n', encoding="utf-8"
                )
                (method_dir / "incomplete.jsonl").write_text(
                    '{"sample_id":"incomplete"}\n', encoding="utf-8"
                )

            process.populate_complete_pair_view(
                archive,
                view,
                ["hybridpatch", "fullrewrite"],
                ["complete"],
            )

            self.assertEqual(
                sorted(path.name for path in (view / "hybridpatch").iterdir()),
                ["complete.jsonl"],
            )
            self.assertEqual(
                sorted(path.name for path in (view / "fullrewrite").iterdir()),
                ["complete.jsonl"],
            )

    def test_incomplete_analysis_annotation_forbids_zero_imputation(self) -> None:
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            analysis = Path(directory)
            (analysis / "comparison.md").write_text(
                "# comparison\n", encoding="utf-8"
            )
            (analysis / "sample_level_final_endpoint.json").write_text(
                json.dumps({"complete": True, "n": 1}), encoding="utf-8"
            )
            scope = {
                "planned_sample_count": 2,
                "analysis_sample_count": 1,
                "incomplete_sample_ids": ["broken"],
                "incomplete_sample_outcomes": {
                    "broken": "evaluator_incomplete"},
                "sample_policy": "complete_all_methods",
            }

            process.annotate_incomplete_analysis(analysis, scope)

            comparison = (analysis / "comparison.md").read_text(encoding="utf-8")
            endpoint = process.load_json(
                analysis / "sample_level_final_endpoint.json"
            )
            self.assertIn("1/2", comparison)
            self.assertIn("no zero score was imputed", comparison)
            self.assertFalse(endpoint["campaign_complete"])
            self.assertFalse(endpoint["score_imputed_for_incomplete_samples"])
            self.assertEqual(
                endpoint["campaign_incomplete_sample_outcomes"],
                {"broken": "evaluator_incomplete"},
            )

    def test_review_check_runs_validate_only_before_heavy_hashing(self) -> None:
        catalog = {"schema": "fixture", "record_sets": []}
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            archive = Path(directory) / "exp_test"
            archive.mkdir()
            with (
                mock.patch.object(
                    process,
                    "reviewed_finalization_inputs",
                    return_value=(
                        "HP_V8",
                        archive,
                        self.facts(),
                        self.review()["catalog_entry"],
                        catalog,
                    ),
                ),
                mock.patch.object(
                    process.subprocess,
                    "run",
                    return_value=SimpleNamespace(returncode=0),
                ) as run,
            ):
                process.review_check_experiment(archive, None)

        command = run.call_args.args[0]
        self.assertIn("--validate-only", command)
        self.assertIn("--skip-tree-hash", command)

    def test_review_check_propagates_contract_failure_before_hashing(self) -> None:
        with tempfile.TemporaryDirectory(dir=process.ROOT) as directory:
            archive = Path(directory) / "exp_test"
            archive.mkdir()
            with (
                mock.patch.object(
                    process,
                    "reviewed_finalization_inputs",
                    return_value=(
                        "HP_V8",
                        archive,
                        self.facts(),
                        self.review()["catalog_entry"],
                        {"schema": "fixture", "record_sets": []},
                    ),
                ),
                mock.patch.object(
                    process.subprocess,
                    "run",
                    return_value=SimpleNamespace(returncode=1),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "before raw-tree"):
                    process.review_check_experiment(archive, None)

    def test_private_record_helpers_preserve_orphan_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog_path = root / "tools" / "experiment_records_catalog.json"
            catalog_path.parent.mkdir(parents=True)
            catalog = {
                "record_sets": [
                    {
                        "owner": "HP_V8",
                        "experiments": [{"experiment_id": "published"}],
                    }
                ]
            }
            catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
            (root / "docs").mkdir()
            (root / "docs" / "EXPERIMENT_INDEX.md").write_text(
                "old index", encoding="utf-8"
            )
            records = root / "HP_V8" / "records"
            (records / "published").mkdir(parents=True)
            (records / "published" / "report.md").write_text(
                "published", encoding="utf-8"
            )
            (records / "orphan").mkdir()
            (records / "orphan" / "report.md").write_text(
                "private", encoding="utf-8"
            )
            (root / "HP_V8" / "EXPERIMENTS.md").write_text(
                "old owner index", encoding="utf-8"
            )

            with (
                mock.patch.object(process, "ROOT", root),
                mock.patch.object(process, "CATALOG", catalog_path),
                tempfile.TemporaryDirectory(dir=root) as backup_dir,
            ):
                orphans = process.orphan_record_directories(catalog)
                self.assertEqual(orphans, [records / "orphan"])
                snapshots = process.snapshot_generated(
                    catalog, Path(backup_dir)
                )
                (records / "orphan" / "report.md").write_text(
                    "changed", encoding="utf-8"
                )
                process.restore_generated(snapshots)
                self.assertEqual(
                    (records / "orphan" / "report.md").read_text(
                        encoding="utf-8"
                    ),
                    "private",
                )

                bundle = root / "private" / "record.tgz"
                digest = process.write_private_record_bundle(
                    bundle,
                    owner="HP_V8",
                    experiment_id="published",
                )
                self.assertRegex(digest, r"^[0-9a-f]{64}$")
                with tarfile.open(bundle, "r:gz") as handle:
                    self.assertIn(
                        "HP_V8/records/published/report.md",
                        handle.getnames(),
                    )

    def test_private_finalize_bundles_generated_state_then_restores_orphan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog_path = root / "tools" / "experiment_records_catalog.json"
            catalog_path.parent.mkdir(parents=True)
            original_catalog = {
                "record_sets": [
                    {
                        "owner": "HP_V8",
                        "experiments": [{"experiment_id": "published"}],
                    }
                ]
            }
            prospective_catalog = json.loads(json.dumps(original_catalog))
            prospective_catalog["record_sets"][0]["experiments"].append(
                {"experiment_id": "exp_test"}
            )
            catalog_path.write_text(
                process.stable_json(original_catalog), encoding="utf-8"
            )
            (root / "docs").mkdir()
            (root / "docs" / "EXPERIMENT_INDEX.md").write_text(
                "original index", encoding="utf-8"
            )
            owner = root / "HP_V8"
            records = owner / "records"
            (records / "published").mkdir(parents=True)
            (records / "published" / "report.md").write_text(
                "published", encoding="utf-8"
            )
            orphan = records / "exp_test"
            orphan.mkdir()
            (orphan / "report.md").write_text(
                "old private record", encoding="utf-8"
            )
            (owner / "EXPERIMENTS.md").write_text(
                "original owner index", encoding="utf-8"
            )
            archive = owner / "exp_test"
            (archive / "analysis").mkdir(parents=True)
            bundle = root / "private" / "record.tgz"

            def generate_state(*_args, **_kwargs):
                target = records / "exp_test"
                target.mkdir(parents=True)
                (target / "report.md").write_text(
                    "new validated record", encoding="utf-8"
                )
                (root / "docs" / "EXPERIMENT_INDEX.md").write_text(
                    "prospective index", encoding="utf-8"
                )
                (owner / "EXPERIMENTS.md").write_text(
                    "prospective owner index", encoding="utf-8"
                )
                return SimpleNamespace(returncode=0)

            with (
                mock.patch.object(process, "ROOT", root),
                mock.patch.object(process, "CATALOG", catalog_path),
                mock.patch.object(
                    process,
                    "reviewed_finalization_inputs",
                    return_value=(
                        "HP_V8",
                        archive,
                        {"input_sha256": "a" * 64},
                        {"experiment_id": "exp_test"},
                        prospective_catalog,
                    ),
                ),
                mock.patch.object(
                    process.subprocess,
                    "run",
                    side_effect=generate_state,
                ),
            ):
                process.finalize_experiment(
                    archive,
                    None,
                    private_record_bundle=bundle,
                )

            self.assertEqual(
                json.loads(catalog_path.read_text(encoding="utf-8")),
                original_catalog,
            )
            self.assertEqual(
                (orphan / "report.md").read_text(encoding="utf-8"),
                "old private record",
            )
            state = json.loads(
                (archive / "analysis" / "process_state.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(state["stage"], "finalized_private")
            with tarfile.open(bundle, "r:gz") as handle:
                report = handle.extractfile(
                    "HP_V8/records/exp_test/report.md"
                )
                assert report is not None
                self.assertEqual(report.read().decode(), "new validated record")

    def test_public_finalize_records_canonical_report_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog_path = root / "tools" / "experiment_records_catalog.json"
            catalog_path.parent.mkdir(parents=True)
            original_catalog = {
                "record_sets": [{
                    "owner": "HP_V8",
                    "experiments": [{"experiment_id": "published"}],
                }]
            }
            prospective_catalog = json.loads(json.dumps(original_catalog))
            prospective_catalog["record_sets"][0]["experiments"].append(
                {"experiment_id": "exp_test"}
            )
            catalog_path.write_text(
                process.stable_json(original_catalog), encoding="utf-8"
            )
            (root / "docs").mkdir()
            (root / "docs" / "EXPERIMENT_INDEX.md").write_text(
                "original index", encoding="utf-8"
            )
            owner = root / "HP_V8"
            records = owner / "records"
            (records / "published").mkdir(parents=True)
            (records / "published" / "report.md").write_text(
                "published", encoding="utf-8"
            )
            (owner / "EXPERIMENTS.md").write_text(
                "original owner index", encoding="utf-8"
            )
            archive = owner / "exp_test"
            (archive / "analysis").mkdir(parents=True)

            def generate_state(*_args, **_kwargs):
                target = records / "exp_test"
                target.mkdir(parents=True)
                (target / "report.md").write_text(
                    "new validated record", encoding="utf-8"
                )
                return SimpleNamespace(returncode=0)

            with (
                mock.patch.object(process, "ROOT", root),
                mock.patch.object(process, "CATALOG", catalog_path),
                mock.patch.object(
                    process,
                    "reviewed_finalization_inputs",
                    return_value=(
                        "HP_V8",
                        archive,
                        {"input_sha256": "a" * 64},
                        {"experiment_id": "exp_test"},
                        prospective_catalog,
                    ),
                ),
                mock.patch.object(
                    process.subprocess,
                    "run",
                    side_effect=generate_state,
                ),
            ):
                process.finalize_experiment(archive, None)

            report = records / "exp_test" / "report.md"
            state = json.loads(
                (archive / "analysis" / "process_state.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(state["stage"], "finalized")
            self.assertEqual(
                state["public_record_sha256"], process.sha256_file(report)
            )
            self.assertIsNone(state["private_record_bundle_sha256"])


if __name__ == "__main__":
    unittest.main()
