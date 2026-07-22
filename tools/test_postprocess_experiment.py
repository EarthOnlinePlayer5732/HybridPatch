from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import postprocess_experiment as postprocess


class PostprocessExperimentTests(unittest.TestCase):
    def test_strict_inspection_is_cached_by_quick_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
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

        self.assertFalse(cached)
        self.assertTrue(cached_second)
        self.assertEqual(first, second)
        dispatcher.inspect_campaign.assert_called_once()

    def test_preservation_violation_stops_inspection(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
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
                    },
                ),
                mock.patch.object(
                    postprocess, "_load_dispatch_module", return_value=dispatcher
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "preservation"):
                    postprocess.inspect_experiment(archive)

    def test_finalized_sha_linked_inspection_can_be_adopted_without_dispatch(self) -> None:
        with tempfile.TemporaryDirectory(dir=postprocess.ROOT) as directory:
            root = Path(directory)
            owner_path = root / "HP_V8"
            archive = owner_path / "exp_fixture"
            analysis = archive / "analysis"
            analysis.mkdir(parents=True)
            (archive / "dispatch_manifest.json").write_text(
                '{"config":{}}', encoding="utf-8"
            )
            inspection = analysis / "strict_inspection_postrun.json"
            inspection.write_text(
                json.dumps(
                    {
                        "schema": postprocess.INSPECTION_SCHEMA,
                        "errors": [],
                        "preservation_violations": 0,
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            digest = hashlib.sha256(inspection.read_bytes()).hexdigest()
            (analysis / "confirmation.json").write_text(
                json.dumps(
                    {
                        "postrun_verification": {
                            "strict_inspection_sha256": digest
                        }
                    }
                ),
                encoding="utf-8",
            )
            (analysis / "process_state.json").write_text(
                '{"stage":"finalized"}', encoding="utf-8"
            )
            with (
                mock.patch.object(
                    postprocess.process,
                    "resolve_experiment",
                    return_value=("HP_V8", owner_path, archive),
                ),
                mock.patch.object(
                    postprocess.process, "collect_facts"
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
        collect.assert_not_called()
        load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
