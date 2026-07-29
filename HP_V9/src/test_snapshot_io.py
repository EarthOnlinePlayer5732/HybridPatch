"""Snapshot and auxiliary I/O failure-domain tests.

Run from HP_V8 root:
  PYTHONUTF8=1 python -B ./src/test_snapshot_io.py
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import experiment_runner
import paired_campaign_dispatch
import run_meta


def _all_files(root):
    paths = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            paths.append(os.path.join(dirpath, filename))
    return sorted(paths)


def _minimal_manifest(snapshot_mode_marker):
    config = {
        "campaign_role": "smoke",
        "samples": ["s1"],
        "method_set": ["fullrewrite", "hybridpatch"],
        "num_round_trips": 1,
    }
    if snapshot_mode_marker is not None:
        config["snapshot_mode"] = snapshot_mode_marker
    return {
        "schema": paired_campaign_dispatch.SCHEMA,
        "experiment_id": "exp",
        "run_git_commit": "0" * 40,
        "git_tree_state": "clean",
        "code_fingerprint": {},
        "config": config,
        "assignments": [{
            "sample": "s1",
            "methods": ["hybridpatch", "fullrewrite"],
            "key_label": "k1",
            "console_log": "dispatch_logs/s1.log",
        }],
        "task_plans": {},
        "upstream_smoke_gate": None,
    }


class SnapshotIoTests(unittest.TestCase):
    def test_normal_snapshot_keeps_legacy_paths_and_reserves_step_json(self):
        with tempfile.TemporaryDirectory() as out_dir:
            step_dir = run_meta.dump_step_docs(
                out_dir,
                "hybridpatch",
                "treebank4",
                1,
                "fwd",
                "target",
                {
                    "answer.txt": "ok",
                    "_step.json": "user payload",
                    "a:b.txt": "one",
                    "a?b.txt": "two",
                    "A.txt": "upper",
                    "a.txt": "lower",
                },
                step_info={"score": 1.0},
            )

            self.assertEqual(
                step_dir,
                os.path.join(
                    out_dir, "docs", "hybridpatch", "treebank4",
                    "rt01_fwd_target"),
            )
            self.assertTrue(os.path.isfile(os.path.join(step_dir, "answer.txt")))
            self.assertFalse(
                os.path.isfile(os.path.join(step_dir, "doc_answer.txt")))

            with open(os.path.join(step_dir, "_step.json"), encoding="utf-8") as handle:
                metadata = json.load(handle)
            self.assertEqual(metadata["score"], 1.0)
            self.assertEqual(metadata["snapshot_path_mode"], "legacy")
            stored = {item["original"]: item["stored"] for item in metadata["snapshot_files"]}
            self.assertEqual(stored["answer.txt"], "answer.txt")
            self.assertNotEqual(stored["_step.json"], "_step.json")
            self.assertEqual(len({stored["a:b.txt"], stored["a?b.txt"]}), 2)
            self.assertEqual(len({
                stored["A.txt"].casefold(), stored["a.txt"].casefold(),
            }), 2)
            with open(
                    os.path.join(step_dir, stored["A.txt"]),
                    encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "upper")
            with open(
                    os.path.join(step_dir, stored["a.txt"]),
                    encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "lower")
            self.assertTrue(os.path.isfile(os.path.join(step_dir, stored["_step.json"])))

            self.assertNotEqual(
                run_meta.snapshot_docs_sample_dir(out_dir, "method", "Sample").casefold(),
                run_meta.snapshot_docs_sample_dir(out_dir, "method", "sample").casefold(),
            )
            self.assertNotEqual(
                run_meta._snapshot_step_dir(
                    out_dir, "method", "sample", 1, "fwd", "State").casefold(),
                run_meta._snapshot_step_dir(
                    out_dir, "method", "sample", 1, "fwd", "state").casefold(),
            )
            self.assertNotEqual(
                run_meta._hashed_snapshot_component("CON").casefold(), "con")
            self.assertNotEqual(
                run_meta._hashed_snapshot_component("state."), "state.")

    def test_long_snapshot_switches_to_compact_paths_within_budget(self):
        with tempfile.TemporaryDirectory() as base:
            # These generated paths are ASCII, so len(path) is a conservative
            # proxy for Windows UTF-16 code units under the 240-char budget.
            target_len = 120
            segment_len = max(40, target_len - len(os.path.abspath(base)) - 1)
            out_dir = os.path.join(base, "o" * segment_len)
            os.makedirs(out_dir)
            method = "hybridpatch/" + ("m" * 160)
            sample = "sample:" + ("s" * 160)
            state = "state/" + ("t" * 160)
            filename = "report:" + ("r" * 160) + ".txt"

            step_dir = run_meta.dump_step_docs(
                out_dir, method, sample, 1, "forward", state,
                {filename: "payload"}, step_info={"score": 0.0})

            self.assertIsNotNone(step_dir)
            self.assertIn("_compact", os.path.normpath(step_dir).split(os.sep))
            for path in _all_files(step_dir):
                self.assertLessEqual(
                    len(os.path.abspath(path)),
                    run_meta._SNAPSHOT_FULL_PATH_BUDGET,
                    path,
                )
            with open(os.path.join(step_dir, "_step.json"), encoding="utf-8") as handle:
                metadata = json.load(handle)
            self.assertEqual(metadata["snapshot_path_mode"], "compact")
            self.assertEqual(metadata["snapshot_files"][0]["original"], filename)
            stored = metadata["snapshot_files"][0]["stored"]
            self.assertTrue(os.path.isfile(os.path.join(step_dir, stored)))

    def test_snapshot_path_budget_exhaustion_is_best_effort(self):
        with tempfile.TemporaryDirectory() as out_dir:
            stderr = io.StringIO()
            with mock.patch.object(run_meta, "_SNAPSHOT_FULL_PATH_BUDGET", 20), \
                    contextlib.redirect_stderr(stderr):
                result = run_meta.dump_step_docs(
                    out_dir, "hybridpatch", "treebank4", 1, "fwd", "target",
                    {"answer.txt": "ok"}, step_info={"score": 1.0})

            self.assertIsNone(result)
            self.assertIn("WARNING: snapshot best-effort write failed", stderr.getvalue())

    def test_runner_snapshot_boundary_catches_monkeypatched_dump_failure(self):
        with tempfile.TemporaryDirectory() as out_dir:
            stderr = io.StringIO()
            with mock.patch.object(
                    experiment_runner, "dump_step_docs",
                    side_effect=OSError("blocked")), \
                    contextlib.redirect_stderr(stderr):
                result = experiment_runner._maybe_dump_step_docs(
                    run_meta.SNAPSHOT_MODE_ALL,
                    out_dir,
                    "hybridpatch",
                    "treebank4",
                    1,
                    "fwd",
                    "target",
                    {"answer.txt": "ok"},
                    step_info={"score": 1.0},
                )

            self.assertIsNone(result)
            self.assertIn("WARNING: snapshot best-effort write failed", stderr.getvalue())

            class BrokenStderr:
                def write(self, _value):
                    raise OSError("stderr unavailable")

                def flush(self):
                    raise OSError("stderr unavailable")

            with mock.patch.object(
                    experiment_runner, "dump_step_docs",
                    side_effect=OSError("blocked")), mock.patch.object(
                        sys, "stderr", BrokenStderr()):
                self.assertIsNone(experiment_runner._maybe_dump_step_docs(
                    run_meta.SNAPSHOT_MODE_ALL,
                    out_dir,
                    "hybridpatch",
                    "treebank4",
                    1,
                    "fwd",
                    "target",
                    {"answer.txt": "ok"},
                    step_info={"score": 1.0},
                ))

    def test_failure_snapshot_mode_uses_deterministic_failure_signals(self):
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.object(
                    experiment_runner, "dump_step_docs",
                    return_value="snapshot") as dump:
                result = experiment_runner._maybe_dump_step_docs(
                    run_meta.SNAPSHOT_MODE_OFF,
                    out_dir,
                    "hybridpatch",
                    "treebank4",
                    1,
                    "fwd",
                    "target",
                    {},
                    evaluation={"score": 0.0},
                )
                self.assertIsNone(result)
                dump.assert_not_called()

            with mock.patch.object(
                    experiment_runner, "dump_step_docs",
                    return_value="snapshot") as dump:
                result = experiment_runner._maybe_dump_step_docs(
                    run_meta.SNAPSHOT_MODE_FAILURES,
                    out_dir,
                    "hybridpatch",
                    "treebank4",
                    1,
                    "fwd",
                    "target",
                    {},
                    evaluation={"score": 0.49},
                )
                self.assertIsNone(result)
                dump.assert_not_called()

            with mock.patch.object(
                    experiment_runner, "dump_step_docs",
                    return_value="snapshot") as dump:
                result = experiment_runner._maybe_dump_step_docs(
                    run_meta.SNAPSHOT_MODE_FAILURES,
                    out_dir,
                    "hybridpatch",
                    "treebank4",
                    1,
                    "fwd",
                    "target",
                    {},
                    evaluation={"score": 0.0},
                )
                self.assertEqual(result, "snapshot")
                dump.assert_called_once()

            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                evaluation={"error": "context_mismatch"}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                v2_info={"invalid_json": True}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                v2_info={"schema_error_count": 1}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                v2_info={"validation_gate_errors": ["bad"]}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                v2_info={"failed_step_kept_context": True}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                v2_info={"partial_extraction": True}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                v2_info={"failure_reason": "op_rejected"}))
            self.assertTrue(experiment_runner._step_has_snapshot_failure(
                row={"bdpatch": {"preservation_violations": 1}}))
            self.assertFalse(experiment_runner._step_has_snapshot_failure(
                evaluation={"score": 0.01}))

    def test_api_anomalies_are_best_effort_but_api_calls_are_not(self):
        row = {
            "sample_id": "treebank4",
            "round_trip_num": 1,
            "round_trip_direction": "forward",
            "method": "hybridpatch",
            "bdpatch": {"hybrid": {"invalid_json": True}},
            "evaluation": {},
            "raw_llm_response": "{",
        }
        with tempfile.TemporaryDirectory() as out_dir:
            stderr = io.StringIO()
            with mock.patch.object(
                    run_meta, "append_jsonl_locked",
                    side_effect=OSError("locked")), \
                    contextlib.redirect_stderr(stderr):
                record = run_meta.record_model_content_anomaly(out_dir, row)
            self.assertIsNotNone(record)
            self.assertIn("WARNING: api_anomalies best-effort write failed",
                          stderr.getvalue())

            calls = []

            def anomaly_fails(path, record):
                calls.append(os.path.basename(path))
                if os.path.basename(path) == "api_anomalies.jsonl":
                    raise OSError("anomaly locked")

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "treebank4", "v8",
                "offline-model", lambda *args, **kwargs: "ok")
            stderr = io.StringIO()
            with mock.patch.object(
                    run_meta, "append_jsonl_locked",
                    side_effect=anomaly_fails), \
                    contextlib.redirect_stderr(stderr):
                recorder._write_record({
                    "request_id": "req-1",
                    "classification": "model-content failure",
                })
            self.assertEqual(calls, ["api_calls.jsonl", "api_anomalies.jsonl"])
            self.assertIn("req-1", recorder.records_by_id)
            self.assertIn("WARNING: api_anomalies best-effort write failed",
                          stderr.getvalue())

            calls = []

            def api_calls_fail(path, record):
                calls.append(os.path.basename(path))
                if os.path.basename(path) == "api_calls.jsonl":
                    raise OSError("api locked")

            with mock.patch.object(
                    run_meta, "append_jsonl_locked",
                    side_effect=api_calls_fail):
                with self.assertRaises(OSError):
                    recorder._write_record({
                        "request_id": "req-2",
                        "classification": "model-content failure",
                    })
            self.assertEqual(calls, ["api_calls.jsonl"])

    def test_paired_snapshot_mode_defaults_and_legacy_manifest(self):
        with tempfile.TemporaryDirectory() as out_dir:
            args = types.SimpleNamespace(snapshot_mode=None, resume=False)
            self.assertEqual(
                paired_campaign_dispatch._resolve_paired_snapshot_mode(
                    out_dir, args),
                run_meta.SNAPSHOT_MODE_FAILURES,
            )

            paired_campaign_dispatch.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"),
                _minimal_manifest(None),
            )
            args = types.SimpleNamespace(snapshot_mode=None, resume=True)
            self.assertEqual(
                paired_campaign_dispatch._resolve_paired_snapshot_mode(
                    out_dir, args),
                run_meta.SNAPSHOT_MODE_ALL,
            )
            args = types.SimpleNamespace(
                snapshot_mode=run_meta.SNAPSHOT_MODE_FAILURES, resume=True)
            self.assertEqual(
                paired_campaign_dispatch._resolve_paired_snapshot_mode(
                    out_dir, args),
                run_meta.SNAPSHOT_MODE_FAILURES,
            )

            paired_campaign_dispatch.write_or_verify_manifest(
                out_dir, _minimal_manifest(run_meta.SNAPSHOT_MODE_ALL),
                resume=True,
            )
            with self.assertRaisesRegex(RuntimeError, "dispatch manifest differs"):
                paired_campaign_dispatch.write_or_verify_manifest(
                    out_dir,
                    _minimal_manifest(run_meta.SNAPSHOT_MODE_FAILURES),
                    resume=True,
                )

    def test_paired_manifest_records_resolved_snapshot_mode(self):
        with tempfile.TemporaryDirectory() as out_dir:
            args = types.SimpleNamespace(
                campaign_role="smoke",
                num_round_trips=1,
                seed=42,
                snapshot_mode=run_meta.SNAPSHOT_MODE_FAILURES,
            )
            assignments = [{
                "sample": "s1",
                "methods": ["hybridpatch", "fullrewrite"],
                "key_label": "k1",
                "console_log": "dispatch_logs/s1.log",
            }]
            with mock.patch.object(
                    paired_campaign_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                manifest = paired_campaign_dispatch.build_manifest(
                    out_dir, ["s1"], assignments, {}, args)

            self.assertEqual(
                manifest["config"]["snapshot_mode"],
                run_meta.SNAPSHOT_MODE_FAILURES,
            )


if __name__ == "__main__":
    unittest.main()
