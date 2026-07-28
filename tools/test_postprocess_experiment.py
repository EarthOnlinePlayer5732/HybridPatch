from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import postprocess_experiment as postprocess


class PostprocessExperimentTests(unittest.TestCase):
    @staticmethod
    def _write_dispatcher(owner_path: Path) -> None:
        source_dir = owner_path / "src"
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "paired_campaign_dispatch.py").write_text(
            "# inspection fixture\n", encoding="utf-8")
        (source_dir / "run_meta.py").write_text(
            "# run-meta inspection fixture\n", encoding="utf-8")

    def test_strict_inspection_is_cached_by_quick_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            postprocess.process.write_atomic(
                archive / "dispatch_manifest.json",
                postprocess.process.stable_json(
                    {"schema": "fixture", "config": {}}
                ),
            )
            dispatcher = SimpleNamespace(
                inspect_campaign=mock.Mock(
                    return_value={
                        "errors": [],
                        "preservation_violations": 0,
                        "latched_preservation_violations": 0,
                        "preservation_not_applicable": 0,
                        "stop_conditions": [],
                        "api_calls": 2,
                        "semantic_calls": 2,
                        "provider_call_rows": 2,
                    }
                )
            )
            facts = {
                "complete_all_methods_sample_ids": ["sample1"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                first, cached = postprocess.inspect_experiment(archive)
                second, cached_second = postprocess.inspect_experiment(archive)
                (owner_path / "src" / "run_meta.py").write_text(
                    "# changed inspection dependency\n", encoding="utf-8")
                third, cached_third = postprocess.inspect_experiment(archive)

        self.assertFalse(cached)
        self.assertTrue(cached_second)
        self.assertFalse(cached_third)
        self.assertEqual(first, second)
        self.assertNotEqual(
            first["inspection_source"]["inspector_sha256"],
            third["inspection_source"]["inspector_sha256"],
        )
        self.assertEqual(dispatcher.inspect_campaign.call_count, 2)
        self.assertTrue(
            dispatcher.inspect_campaign.call_args.kwargs["require_complete"]
        )
        self.assertTrue(
            dispatcher.inspect_campaign.call_args.kwargs[
                "require_terminal_provenance"
            ]
        )

    def test_tool_source_hash_changes_invalidate_inspection_cache(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8")
            wrapper_source = root / "postprocess_experiment.py"
            process_source = root / "process_experiment.py"
            artifacts_source = root / "experiment_artifacts.py"
            wrapper_source.write_text("# wrapper v1\n", encoding="utf-8")
            process_source.write_text("# process v1\n", encoding="utf-8")
            artifacts_source.write_text("# artifacts v1\n", encoding="utf-8")
            dispatcher = SimpleNamespace(inspect_campaign=mock.Mock(
                return_value={
                    "errors": [],
                    "preservation_violations": 0,
                    "latched_preservation_violations": 0,
                    "stop_conditions": [],
                }
            ))
            facts = {
                "complete_all_methods_sample_ids": ["sample"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
                mock.patch.object(
                    postprocess, "__file__", str(wrapper_source)
                ),
                mock.patch.object(
                    postprocess.process, "__file__", str(process_source)
                ),
                mock.patch.object(
                    postprocess.artifacts, "__file__", str(artifacts_source)
                ),
            ):
                _, first_cached = postprocess.inspect_experiment(archive)
                _, second_cached = postprocess.inspect_experiment(archive)
                wrapper_source.write_text("# wrapper v2\n", encoding="utf-8")
                _, wrapper_changed_cached = postprocess.inspect_experiment(archive)
                process_source.write_text("# process v2\n", encoding="utf-8")
                _, process_changed_cached = postprocess.inspect_experiment(archive)

        self.assertFalse(first_cached)
        self.assertTrue(second_cached)
        self.assertFalse(wrapper_changed_cached)
        self.assertFalse(process_changed_cached)
        self.assertEqual(dispatcher.inspect_campaign.call_count, 3)

    def test_preservation_violation_stops_inspection(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8"
            )
            dispatcher = SimpleNamespace(
                inspect_campaign=lambda *_args, **_kwargs: {
                    "errors": ["preservation_violations=1"],
                    "preservation_violations": 1,
                }
            )
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process,
                    "collect_facts",
                    return_value={
                        "complete_all_methods_sample_ids": [],
                        "incomplete_samples": {},
                        "incomplete_sample_outcomes": {},
                        "input_sha256": "1" * 64,
                    },
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "preservation"):
                    postprocess.inspect_experiment(archive)

    def test_non_preservation_integrity_error_is_cached_but_never_accepted(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8"
            )
            dispatcher = SimpleNamespace(
                inspect_campaign=mock.Mock(
                    return_value={
                        "errors": ["API linkage is invalid"],
                        "preservation_violations": 0,
                    }
                )
            )
            facts = {
                "complete_all_methods_sample_ids": ["sample1"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "API linkage"):
                    postprocess.inspect_experiment(archive)
                with self.assertRaisesRegex(RuntimeError, "API linkage"):
                    postprocess.inspect_experiment(archive)

        self.assertEqual(dispatcher.inspect_campaign.call_count, 2)

    def test_incomplete_campaign_uses_sample_scoped_strict_inspection(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8"
            )
            dispatcher = SimpleNamespace(
                inspect_campaign=mock.Mock(
                    return_value={"errors": [], "preservation_violations": 0}
                )
            )
            facts = {
                "complete_all_methods_sample_ids": ["sample1"],
                "incomplete_samples": {"sample2": "evaluator_incomplete"},
                "incomplete_sample_outcomes": {
                    "sample2": "evaluator_incomplete"},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                postprocess.inspect_experiment(archive)

        self.assertFalse(
            dispatcher.inspect_campaign.call_args.kwargs["require_complete"]
        )
        self.assertTrue(
            dispatcher.inspect_campaign.call_args.kwargs[
                "require_terminal_provenance"
            ]
        )
        self.assertEqual(
            dispatcher.inspect_campaign.call_args.kwargs[
                "required_complete_samples"
            ],
            {"sample1"},
        )

    def test_incomplete_campaign_requires_supported_terminal_outcome(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8")
            facts = {
                "complete_all_methods_sample_ids": [],
                "incomplete_samples": {"sample": {}},
                "incomplete_sample_outcomes": {
                    "sample": "missing_outcome"},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module") as load,
            ):
                with self.assertRaisesRegex(
                        RuntimeError, "supported terminal sample outcome"):
                    postprocess.inspect_experiment(archive)
            load.assert_not_called()

    def test_finalized_sha_linked_inspection_can_be_adopted_without_dispatch(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            analysis = archive / "analysis"
            analysis.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8"
            )
            quick = postprocess.artifacts.quick_tree_fingerprint(
                archive, excluded_prefixes=("analysis",))
            inspector_sources, inspector_sha256 = (
                postprocess._inspection_dependency_identity(owner_path)
            )
            evidence_identity = postprocess._inspection_evidence_identity(archive)
            input_sha256 = "1" * 64
            inspection = analysis / "strict_inspection_postrun.json"
            inspection.write_text(
                json.dumps(
                    {
                        "schema": postprocess.INSPECTION_SCHEMA,
                        "errors": [],
                        "preservation_violations": 0,
                        "latched_preservation_violations": 0,
                        "stop_conditions": [],
                        "inspection_source": {
                            "zero_api": True,
                            "experiment_id": archive.name,
                            "required_complete_sample_count": 1,
                            "manifest_sha256": (
                                postprocess.artifacts.sha256_file(
                                    archive / "dispatch_manifest.json")),
                            "input_sha256": input_sha256,
                            "incomplete_sample_ids": [],
                            "incomplete_sample_outcomes": {},
                            "campaign_complete": True,
                            "require_complete": True,
                            "require_terminal_provenance": True,
                            "inspector_sha256": inspector_sha256,
                            "inspector_sources": inspector_sources,
                            "input_quick_fingerprint": quick,
                            "input_evidence_identity": evidence_identity,
                        },
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            digest = hashlib.sha256(inspection.read_bytes()).hexdigest()
            inspection_value = json.loads(inspection.read_text(encoding="utf-8"))
            (analysis / "confirmation.json").write_text(
                json.dumps(
                    {
                        "schema": "hybridpatch.confirmation_campaign_analysis/1",
                        "experiment_id": archive.name,
                        "integrity": {
                            "postrun_verification": {
                                "valid": True,
                                "problems": [],
                                "accepted_sample_level_incomplete_errors": [],
                                "unaccepted_inspection_errors": [],
                                "strict_inspection_sha256": digest,
                                "strict_inspection": inspection_value,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            (analysis / "process_state.json").write_text(
                json.dumps({
                    "schema": "hybridpatch.experiment_process_state/1",
                    "experiment_id": archive.name,
                    "stage": "finalized",
                    "input_sha256": input_sha256,
                }),
                encoding="utf-8",
            )
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process,
                    "collect_facts",
                    return_value={
                        "complete_all_methods_sample_ids": ["sample1"],
                        "incomplete_samples": {},
                        "incomplete_sample_outcomes": {},
                        "input_sha256": input_sha256,
                    },
                ) as collect,
                mock.patch.object(postprocess, "_load_dispatch_module") as load,
            ):
                _, cached = postprocess.inspect_experiment(
                    archive,
                    adopt_existing=True,
                )
                _, cached_again = postprocess.inspect_experiment(archive)

        self.assertTrue(cached)
        self.assertTrue(cached_again)
        self.assertEqual(collect.call_count, 2)
        load.assert_not_called()

    def test_cache_rejects_current_input_drift_hidden_by_quick_fingerprint(
            self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8")
            dispatcher = SimpleNamespace(inspect_campaign=mock.Mock(
                return_value={
                    "errors": [],
                    "preservation_violations": 0,
                    "latched_preservation_violations": 0,
                    "stop_conditions": [],
                }
            ))
            initial = {
                "complete_all_methods_sample_ids": ["sample"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": "1" * 64,
            }
            drifted = {**initial, "input_sha256": "2" * 64}
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process,
                    "collect_facts",
                    side_effect=[initial, initial, drifted],
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
                mock.patch.object(
                    postprocess.artifacts,
                    "quick_tree_fingerprint",
                    return_value={"fixture": "unchanged"},
                ),
            ):
                postprocess.inspect_experiment(archive)
                with self.assertRaisesRegex(
                        RuntimeError, "source binding is invalid or stale"):
                    postprocess.inspect_experiment(archive)

        dispatcher.inspect_campaign.assert_called_once()

    def test_content_identity_catches_same_size_same_mtime_evidence_drift(
            self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            transport_dir = archive / "critical"
            transport_dir.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8")
            api_calls = archive / "api_calls.jsonl"
            sidecar = transport_dir / "call.transport.jsonl"
            api_calls.write_bytes(b"AAAA\n")
            sidecar.write_bytes(b"1111\n")
            dispatcher = SimpleNamespace(inspect_campaign=mock.Mock(
                return_value={
                    "errors": [],
                    "preservation_violations": 0,
                    "latched_preservation_violations": 0,
                    "stop_conditions": [],
                }
            ))
            facts = {
                "complete_all_methods_sample_ids": ["sample"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                _, first_cached = postprocess.inspect_experiment(archive)
                _, second_cached = postprocess.inspect_experiment(archive)

                api_stat = api_calls.stat()
                quick_before_api = postprocess.artifacts.quick_tree_fingerprint(
                    archive, excluded_prefixes=("analysis",))
                api_calls.write_bytes(b"BBBB\n")
                os.utime(
                    api_calls,
                    ns=(api_stat.st_atime_ns, api_stat.st_mtime_ns),
                )
                quick_after_api = postprocess.artifacts.quick_tree_fingerprint(
                    archive, excluded_prefixes=("analysis",))
                _, api_changed_cached = postprocess.inspect_experiment(archive)

                sidecar_stat = sidecar.stat()
                quick_before_sidecar = (
                    postprocess.artifacts.quick_tree_fingerprint(
                        archive, excluded_prefixes=("analysis",)))
                sidecar.write_bytes(b"2222\n")
                os.utime(
                    sidecar,
                    ns=(sidecar_stat.st_atime_ns, sidecar_stat.st_mtime_ns),
                )
                quick_after_sidecar = (
                    postprocess.artifacts.quick_tree_fingerprint(
                        archive, excluded_prefixes=("analysis",)))
                _, sidecar_changed_cached = postprocess.inspect_experiment(archive)

        self.assertFalse(first_cached)
        self.assertTrue(second_cached)
        self.assertEqual(quick_before_api, quick_after_api)
        self.assertEqual(quick_before_sidecar, quick_after_sidecar)
        self.assertFalse(api_changed_cached)
        self.assertFalse(sidecar_changed_cached)
        self.assertEqual(dispatcher.inspect_campaign.call_count, 3)

    def test_authoritative_report_with_non_object_integrity_is_rejected(
            self) -> None:
        self.assertFalse(postprocess._authoritative_inspection_report_matches(
            {
                "schema": "hybridpatch.confirmation_campaign_analysis/1",
                "experiment_id": "exp_fixture",
                "integrity": "invalid",
            },
            experiment_id="exp_fixture",
            inspection={"errors": []},
            digest="1" * 64,
        ))

    def test_finalized_closeout_requires_existing_canonical_record(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            analysis = archive / "analysis"
            analysis.mkdir(parents=True)
            manifest = archive / "dispatch_manifest.json"
            manifest.write_text('{"config":{}}', encoding="utf-8")
            quick = postprocess.artifacts.quick_tree_fingerprint(
                archive, excluded_prefixes=("analysis",))
            inspector_sources, inspector_sha256 = (
                postprocess._inspection_dependency_identity(owner_path)
            )
            evidence_identity = postprocess._inspection_evidence_identity(archive)
            input_sha256 = "1" * 64
            (analysis / "strict_inspection_postrun.json").write_text(
                json.dumps({
                    "schema": postprocess.INSPECTION_SCHEMA,
                    "errors": [],
                    "preservation_violations": 0,
                    "latched_preservation_violations": 0,
                    "stop_conditions": [],
                    "inspection_source": {
                        "zero_api": True,
                        "experiment_id": archive.name,
                        "required_complete_sample_count": 1,
                        "manifest_sha256": (
                            postprocess.artifacts.sha256_file(manifest)),
                        "input_sha256": input_sha256,
                        "incomplete_sample_ids": [],
                        "incomplete_sample_outcomes": {},
                        "campaign_complete": True,
                        "require_complete": True,
                        "require_terminal_provenance": True,
                        "inspector_sha256": inspector_sha256,
                        "inspector_sources": inspector_sources,
                        "input_quick_fingerprint": quick,
                        "input_evidence_identity": evidence_identity,
                    },
                }),
                encoding="utf-8",
            )
            (analysis / "process_state.json").write_text(json.dumps({
                "schema": "hybridpatch.experiment_process_state/1",
                "experiment_id": archive.name,
                "stage": "finalized",
                "input_sha256": input_sha256,
                "record_ref": f"HP_V8/records/{archive.name}/report.md",
                "public_record_sha256": "1" * 64,
            }), encoding="utf-8")
            facts = {
                "complete_all_methods_sample_ids": ["sample"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": input_sha256,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess,
                    "status_document",
                    return_value={"finalized": True, "prepared_current": True},
                ),
                mock.patch.object(
                    postprocess, "_inspection_cache_matches", return_value=True
                ),
            ):
                with self.assertRaisesRegex(
                        RuntimeError, "record_ref does not exist"):
                    postprocess.closeout(SimpleNamespace(
                        confirm_stopped=True,
                        experiment=archive,
                    ))

            unexpected = root / "unexpected-report.md"
            unexpected.write_text("unexpected\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "canonical report"):
                postprocess._require_final_record(
                    "HP_V8",
                    archive,
                    {
                        "stage": "finalized",
                        "record_ref": str(unexpected),
                        "public_record_sha256": (
                            postprocess.artifacts.sha256_file(unexpected)),
                    },
                )

    def test_finalized_public_record_content_must_match_state(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            owner = owner_path.relative_to(postprocess.ROOT).as_posix()
            report = owner_path / "records" / archive.name / "report.md"
            report.parent.mkdir(parents=True)
            report.write_text("original report\n", encoding="utf-8")
            process_state = {
                "stage": "finalized",
                "record_ref": f"{owner}/records/{archive.name}/report.md",
                "public_record_sha256": (
                    postprocess.artifacts.sha256_file(report)),
            }

            self.assertEqual(
                postprocess._require_final_record(
                    owner, archive, process_state),
                report.resolve(),
            )
            report.write_text("tampered report\n", encoding="utf-8")
            with self.assertRaisesRegex(
                    RuntimeError, "public record does not match"):
                postprocess._require_final_record(
                    owner, archive, process_state)

    def test_finalized_closeout_rejects_raw_tree_drift(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            self._write_dispatcher(owner_path)
            archive = owner_path / "exp_fixture"
            archive.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8")
            dispatcher = SimpleNamespace(inspect_campaign=mock.Mock(
                return_value={
                    "errors": [],
                    "preservation_violations": 0,
                    "latched_preservation_violations": 0,
                    "stop_conditions": [],
                }
            ))
            facts = {
                "complete_all_methods_sample_ids": ["sample"],
                "incomplete_samples": {},
                "incomplete_sample_outcomes": {},
                "input_sha256": "1" * 64,
            }
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts", return_value=facts
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                postprocess.inspect_experiment(archive)
                (archive / "raw-drift.txt").write_text(
                    "changed\n", encoding="utf-8")
                with mock.patch.object(
                    postprocess,
                    "status_document",
                    return_value={"finalized": True, "prepared_current": True},
                ):
                    with self.assertRaisesRegex(
                            RuntimeError, "no longer matches"):
                        postprocess.closeout(SimpleNamespace(
                            confirm_stopped=True,
                            experiment=archive,
                        ))


    def test_strict_evidence_identity_rejects_archive_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "exp_fixture"
            archive.mkdir()
            target = root / "external-api-calls.jsonl"
            target.write_text('{"request_id":"outside"}\n', encoding="utf-8")
            link = archive / "api_calls.jsonl"
            try:
                os.symlink(target, link)
            except OSError as exc:
                self.skipTest(f"symlink creation unavailable: {exc}")

            with self.assertRaisesRegex(RuntimeError, "contains a symlink"):
                postprocess._inspection_evidence_identity(archive)


if __name__ == "__main__":
    unittest.main()
