from __future__ import annotations

import contextlib
import io
import json
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import evaluator_runtime_preflight as preflight


class EvaluatorRuntimePreflightTests(unittest.TestCase):
    @staticmethod
    def _worker_result(sample_id: str) -> dict[str, object]:
        return {
            "schema": preflight.WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": "test",
            "status": "runtime_runnable",
            "score": 1.0,
            "self_score_one": True,
            "sample_tree_unchanged": True,
            "new_tmp_eval_artifacts": [],
            "api_guard_active": True,
        }

    def test_nonunit_self_score_is_runtime_runnable(self) -> None:
        result = preflight.classify_evaluation({"score": 0.95})
        self.assertEqual(result["status"], "runtime_runnable")
        self.assertFalse(result["self_score_one"])

    def test_error_result_is_not_runtime_runnable(self) -> None:
        result = preflight.classify_evaluation(
            {"score": 0.0, "error": "missing dependency"}
        )
        self.assertEqual(result["status"], "invalid_result")

    def test_selection_deduplicates_and_rejects_missing_samples(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            version = Path(directory)
            root = version / "data" / "samples_delegate52"
            for sample in ("sample1", "sample2"):
                folder = root / sample
                folder.mkdir(parents=True)
                (folder / "sample.json").write_text("{}", encoding="utf-8")
            self.assertEqual(
                preflight.select_samples(
                    version,
                    run_all=False,
                    requested=["sample2", "sample1", "sample2"],
                ),
                ["sample2", "sample1"],
            )
            self.assertEqual(
                preflight.select_samples(version, run_all=True, requested=None),
                ["sample1", "sample2"],
            )
            with self.assertRaisesRegex(RuntimeError, "not found"):
                preflight.select_samples(
                    version,
                    run_all=False,
                    requested=["missing"],
                )

    def test_output_writes_full_report_and_stdout_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            version = Path(directory) / "HP_V8"
            sample_dir = version / "data" / "samples_delegate52" / "sample1"
            sample_dir.mkdir(parents=True)
            (sample_dir / "sample.json").write_text("{}", encoding="utf-8")
            output = Path(directory) / "artifacts" / "preflight.json"
            with self.assertRaisesRegex(RuntimeError, "must not write"):
                preflight.resolve_output_path(sample_dir / "report.json", version)
            worker_result = {
                "schema": preflight.WORKER_SCHEMA,
                "sample_id": "sample1",
                "sample_type": "test",
                "status": "runtime_runnable",
                "score": 1.0,
                "self_score_one": True,
            }
            stdout = io.StringIO()
            with (
                mock.patch.object(
                    preflight, "active_version_root", return_value=version
                ),
                mock.patch.object(
                    preflight, "run_sample", return_value=worker_result
                ),
                contextlib.redirect_stdout(stdout),
            ):
                exit_code = preflight.main(
                    [
                        "--samples",
                        "sample1",
                        "--output",
                        str(output),
                        "--quiet",
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads(stdout.getvalue())
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(summary["schema"], preflight.SUMMARY_SCHEMA)
            self.assertEqual(report["schema"], preflight.SCHEMA)
            self.assertTrue(report["summary"]["passed"])
            self.assertIn("caveats", report)
            self.assertEqual(
                {risk["id"] for risk in report["known_risks"]},
                {
                    "api_guard_is_process_level_not_network_sandbox",
                    "python1_uses_locally_rebuilt_evaluator_dependency",
                    "python2_7_negative_control_nondiscriminative",
                },
            )
            self.assertEqual(list(output.parent.glob(".*.tmp")), [])

    def test_parallel_execution_is_bounded_and_keeps_all_results(self) -> None:
        active = 0
        maximum = 0
        lock = threading.Lock()

        def fake_run(_version, sample_id, _timeout, *, concurrent=False):
            nonlocal active, maximum
            self.assertTrue(concurrent)
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.03)
            with lock:
                active -= 1
            return self._worker_result(sample_id)

        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(preflight, "run_sample", side_effect=fake_run),
        ):
            selected = [f"sample{index}" for index in range(6)]
            results = preflight.run_selected_samples(
                Path(directory),
                selected,
                timeout=10.0,
                jobs=3,
                quiet=True,
            )

        self.assertEqual(set(results), set(selected))
        self.assertGreater(maximum, 1)
        self.assertLessEqual(maximum, 3)

    def test_reuse_output_skips_matching_successes_and_reruns_changed_sample(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            version = Path(directory) / "HP_V8"
            sample_root = version / "data" / "samples_delegate52"
            for sample_id in ("sample1", "sample2"):
                folder = sample_root / sample_id
                folder.mkdir(parents=True)
                (folder / "sample.json").write_text(
                    '{"sample_type":"test"}\n', encoding="utf-8"
                )
            output = Path(directory) / "cache" / "preflight.json"

            def fake_run(_version, sample_id, _timeout, *, concurrent=False):
                return self._worker_result(sample_id)

            with (
                mock.patch.object(
                    preflight, "active_version_root", return_value=version
                ),
                mock.patch.object(preflight, "run_sample", side_effect=fake_run),
            ):
                self.assertEqual(
                    preflight.main(
                        [
                            "--samples",
                            "sample1",
                            "sample2",
                            "--output",
                            str(output),
                            "--jobs",
                            "2",
                            "--quiet",
                        ]
                    ),
                    0,
                )

            with (
                mock.patch.object(
                    preflight, "active_version_root", return_value=version
                ),
                mock.patch.object(
                    preflight,
                    "run_sample",
                    side_effect=AssertionError("matching cache must skip workers"),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(
                    preflight.main(
                        [
                            "--samples",
                            "sample1",
                            "sample2",
                            "--output",
                            str(output),
                            "--reuse-output",
                            "--quiet",
                        ]
                    ),
                    0,
                )
            cached = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(cached["summary"]["reused"], 2)
            self.assertEqual(cached["summary"]["executed"], 0)

            (sample_root / "sample2" / "sample.json").write_text(
                '{"sample_type":"test","changed":true}\n', encoding="utf-8"
            )
            calls: list[str] = []

            def rerun_changed(_version, sample_id, _timeout, *, concurrent=False):
                calls.append(sample_id)
                return self._worker_result(sample_id)

            with (
                mock.patch.object(
                    preflight, "active_version_root", return_value=version
                ),
                mock.patch.object(
                    preflight, "run_sample", side_effect=rerun_changed
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(
                    preflight.main(
                        [
                            "--samples",
                            "sample1",
                            "sample2",
                            "--output",
                            str(output),
                            "--reuse-output",
                            "--quiet",
                        ]
                    ),
                    0,
                )
            refreshed = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(calls, ["sample2"])
            self.assertEqual(refreshed["summary"]["reused"], 1)
            self.assertEqual(refreshed["summary"]["executed"], 1)

    def test_worker_timeout_is_reported(self) -> None:
        version = preflight.active_version_root()
        expired = subprocess.TimeoutExpired(cmd=["python"], timeout=0.01)
        with mock.patch.object(preflight.subprocess, "run", side_effect=expired):
            result = preflight.run_sample(version, "audiosyn1", timeout=0.01)
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(result["sample_type"], "audiosyn")

    def test_real_runner_path_accepts_current_audiosyn_evaluator(self) -> None:
        version = preflight.active_version_root()
        result = preflight.run_sample(version, "audiosyn1", timeout=120.0)
        self.assertEqual(result["status"], "runtime_runnable", result)
        self.assertEqual(result["sample_type"], "audiosyn")
        self.assertEqual(result["score"], 1.0)
        self.assertTrue(result["sample_tree_unchanged"])
        self.assertEqual(result["new_tmp_eval_artifacts"], [])


if __name__ == "__main__":
    unittest.main()
