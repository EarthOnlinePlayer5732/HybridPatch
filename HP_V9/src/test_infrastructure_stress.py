"""Zero-API concurrency stress for the HP_V9 full234 control plane."""

import json
import multiprocessing
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

import control_telemetry
import run_meta
from model_openai_test_cases.fixture_builders import run_metadata_kwargs


WORKER_COUNT = 30


def _append_shared_rows_process(path, worker_index, row_count):
    for row_index in range(row_count):
        run_meta.append_jsonl_locked(path, {
            "worker_index": worker_index,
            "row_index": row_index,
        })


class InfrastructureStressTests(unittest.TestCase):
    def test_control_timing_is_thresholded_and_per_process(self):
        with tempfile.TemporaryDirectory() as out_dir:
            self.assertIsNone(control_telemetry.record_slow_control_operation(
                out_dir, "below_threshold", 0.001, threshold_ms=10.0))
            record = control_telemetry.record_slow_control_operation(
                out_dir, "forced_record", 0.001, threshold_ms=0.0,
                sample_count=30)
            self.assertEqual(record["operation"], "forced_record")
            self.assertEqual(record["sample_count"], 30)
            target = os.path.join(
                out_dir, "control_telemetry", f"pid-{os.getpid()}.jsonl")
            rows = run_meta._read_jsonl_records_with_retry(target)
            self.assertEqual([row["operation"] for row in rows], [
                "forced_record",
            ])

    def test_control_timing_write_failure_is_nonfatal(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch(
                "builtins.open", side_effect=OSError("telemetry unavailable")):
            self.assertIsNone(control_telemetry.record_slow_control_operation(
                out_dir, "write_failure", 1.0, threshold_ms=0.0))

    def test_thirty_processes_append_one_shared_jsonl_without_loss(self):
        rows_per_worker = 3
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "api_calls.jsonl")
            context = multiprocessing.get_context("spawn")
            workers = [
                context.Process(
                    target=_append_shared_rows_process,
                    args=(path, index, rows_per_worker),
                )
                for index in range(WORKER_COUNT)
            ]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(30)
            failures = [
                f"pid={worker.pid} exit={worker.exitcode}"
                for worker in workers if worker.exitcode != 0
            ]
            self.assertEqual(failures, [])
            rows = run_meta._read_jsonl_records_with_retry(path)
            self.assertEqual(len(rows), WORKER_COUNT * rows_per_worker)
            self.assertEqual(
                {(row["worker_index"], row["row_index"]) for row in rows},
                {
                    (worker_index, row_index)
                    for worker_index in range(WORKER_COUNT)
                    for row_index in range(rows_per_worker)
                },
            )

    def test_thirty_metadata_registrations_and_finishes_are_consistent(self):
        kwargs = run_metadata_kwargs(
            command="python hp-v9-metadata-stress",
            num_round_trips=10,
            distractor=False,
        )
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"hp_v9_stress": "v1"}):
            invocations = []
            errors = []
            guard = threading.Lock()
            start = threading.Barrier(WORKER_COUNT)

            def register():
                try:
                    start.wait(10)
                    record = run_meta.append_run_metadata(out_dir, **kwargs)
                    with guard:
                        invocations.append(record)
                except BaseException as exc:  # pragma: no cover - asserted below
                    with guard:
                        errors.append(exc)

            workers = [threading.Thread(target=register) for _ in range(WORKER_COUNT)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(30)
            self.assertTrue(all(not worker.is_alive() for worker in workers))
            self.assertEqual(errors, [])
            self.assertEqual(len(invocations), WORKER_COUNT)

            finish_start = threading.Barrier(WORKER_COUNT)

            def finish(invocation_id):
                try:
                    finish_start.wait(10)
                    run_meta.finish_run_metadata(out_dir, invocation_id)
                except BaseException as exc:  # pragma: no cover - asserted below
                    with guard:
                        errors.append(exc)

            workers = [
                threading.Thread(target=finish, args=(row["invocation_id"],))
                for row in invocations
            ]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(30)
            self.assertTrue(all(not worker.is_alive() for worker in workers))
            self.assertEqual(errors, [])
            projection = run_meta.read_quiescent_run_metadata_snapshot(out_dir)
            self.assertEqual(len(projection), WORKER_COUNT)
            self.assertTrue(all(row["status"] == "finished" for row in projection))

    def test_thirty_committers_do_not_starve_exclusive_stop(self):
        with tempfile.TemporaryDirectory() as out_dir:
            start = threading.Barrier(WORKER_COUNT + 1)
            errors = []
            stopped = []
            guard = threading.Lock()
            original_write = run_meta.write_json_atomic

            def slightly_slow_checkpoint(path, payload):
                if path.endswith(".ckpt.json"):
                    time.sleep(0.01)
                return original_write(path, payload)

            def commit_worker(worker_index):
                sample = f"sample-{worker_index:02d}"
                try:
                    start.wait(10)
                    for rt in range(1, 4):
                        run_meta.append_relay_rows_and_checkpoint(
                            os.path.join(
                                out_dir, "hybridpatch", f"{sample}.jsonl"),
                            os.path.join(
                                out_dir, "hybridpatch", f"{sample}.ckpt.json"),
                            [
                                {"round_trip_num": rt,
                                 "round_trip_direction": "forward"},
                                {"round_trip_num": rt,
                                 "round_trip_direction": "backward"},
                            ],
                            {"completed_round_trips": rt},
                            campaign_out_dir=out_dir,
                        )
                except run_meta.CampaignStoppedError:
                    with guard:
                        stopped.append(sample)
                except BaseException as exc:  # pragma: no cover - asserted below
                    with guard:
                        errors.append(exc)

            workers = [
                threading.Thread(target=commit_worker, args=(index,))
                for index in range(WORKER_COUNT)
            ]
            with mock.patch.object(
                    run_meta, "write_json_atomic",
                    side_effect=slightly_slow_checkpoint):
                for worker in workers:
                    worker.start()
                start.wait(10)
                time.sleep(0.005)
                stop_started = time.monotonic()
                run_meta.record_campaign_stop_condition(
                    out_dir, "zero_api_stress_stop")
                stop_elapsed = time.monotonic() - stop_started
                for worker in workers:
                    worker.join(30)

            self.assertTrue(all(not worker.is_alive() for worker in workers))
            self.assertEqual(errors, [])
            self.assertLess(stop_elapsed, 10.0)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0]["condition"],
                "zero_api_stress_stop",
            )
            with self.assertRaises(run_meta.CampaignStoppedError):
                run_meta.append_relay_rows_and_checkpoint(
                    os.path.join(out_dir, "hybridpatch", "post-stop.jsonl"),
                    os.path.join(out_dir, "hybridpatch", "post-stop.ckpt.json"),
                    [
                        {"round_trip_num": 1,
                         "round_trip_direction": "forward"},
                        {"round_trip_num": 1,
                         "round_trip_direction": "backward"},
                    ],
                    {"completed_round_trips": 1},
                    campaign_out_dir=out_dir,
                )
            for worker_index in range(WORKER_COUNT):
                sample = f"sample-{worker_index:02d}"
                result_path = os.path.join(
                    out_dir, "hybridpatch", f"{sample}.jsonl")
                checkpoint_path = os.path.join(
                    out_dir, "hybridpatch", f"{sample}.ckpt.json")
                if not os.path.isfile(result_path):
                    self.assertFalse(os.path.exists(checkpoint_path))
                    continue
                rows = run_meta._read_jsonl_records_with_retry(result_path)
                with open(checkpoint_path, encoding="utf-8") as handle:
                    checkpoint = json.load(handle)
                self.assertEqual(len(rows) % 2, 0)
                self.assertEqual(
                    checkpoint["completed_round_trips"], len(rows) // 2)


if __name__ == "__main__":
    unittest.main()
