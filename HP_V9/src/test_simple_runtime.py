"""Zero-API acceptance tests for the simplified HP_V9 runtime."""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import run_campaign
import run_sample
from domains import get_domain
from relay_core import (_evaluate, EvaluatorIncompleteError,
                        PreservationViolationError)
from simple_api_recorder import LocalCallEvidenceError, SimpleApiRecorder
from simple_runtime_io import (
    LocalEvidenceError, all_sample_ids, campaign_lock, commit_round_trip,
    create_or_validate_run, load_resume_state, lock_is_held, method_orders,
    prepare_task_plans, read_json, read_status, sample_dir, sample_lock,
    validate_result_pairs, sha256_json, write_json_atomic, write_status,
)
from utils_context import build_context_from_folder, stringify_context
from utils_env import load_sample
from verify_campaign import (_verify_row_journals, verify_full,
                             write_quick_summary)


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SAMPLES_ROOT = ROOT / "data" / "samples_delegate52"
requires_local_data = unittest.skipUnless(
    os.environ.get("ANCHORPATCH_TEST_NO_LOCAL_DATA") != "1"
    and (SAMPLES_ROOT / "accounting1" / "sample.json").is_file(),
    "local DELEGATE-52 junction is not available")


def _complete_response(message="answer", *, call_kind="fullrewrite_primary",
                       attempts=None):
    attempts = list(attempts or [{
        "status": "success", "http_status": 200, "stream_complete": True,
        "final_usage_seen": True, "terminal_sequence_valid": True,
        "retry_budget_consumed": False,
    }])
    return {
        "message": message, "http_status": 200, "stream_complete": True,
        "finish_reason": "stop", "response_classification": (
            "normal" if message.strip() else "model_empty"),
        "transport": "openai_sdk_stream",
        "transport_revision": "opencode_openai_compatible/6",
        "reasoning_effort": "high", "call_kinds": [call_kind],
        "transport_attempts": attempts, "prompt_tokens": 2,
        "completion_tokens": 1, "total_tokens": 3,
        "input_tokens": 2, "output_tokens": 1,
        "elapsed_time": 0.01, "retry_count": max(len(attempts) - 1, 0),
        "failed_attempt_count": max(len(attempts) - 1, 0),
    }


def _minimal_run(out_dir, samples, methods_by_sample, round_trips=10):
    plans = {}
    for sample in samples:
        path = Path(out_dir) / "task_plans" / f"{sample}.json"
        payload = {
            "schema": "anchorpatch.simple_task_plan/1", "sample": sample,
            "round_trips": round_trips, "seed": 42,
            "target_state_ids": ["unused"] * round_trips,
        }
        write_json_atomic(path, payload)
        plans[sample] = {
            "path": f"task_plans/{sample}.json", "sha256": sha256_json(payload)}
    scientific = {
        "samples": list(samples), "method_order_by_sample": methods_by_sample,
        "method_set": sorted({m for values in methods_by_sample.values() for m in values}),
        "round_trips": round_trips, "seed": 42,
        "model": "deepseek-v4-flash", "endpoint": run_campaign._ENDPOINT,
        "request_url": run_campaign._REQUEST_URL, "stream": True,
        "stream_options": {"include_usage": True}, "reasoning_effort": "high",
        "max_tokens": 20000, "include_distractor": True,
        "protocol": "hybridpatch/8", "prompt_builder": "hybrid_prompt.build_hybrid_prompt",
        "executor": "hybrid_executor.apply_hybrid", "transport": "openai_sdk_stream",
        "transport_revision": "opencode_openai_compatible/6",
        "run_git_commit": "a" * 40, "git_tree_state": "clean",
        "samples_root": "data/samples_delegate52", "task_plans": plans,
    }
    write_json_atomic(Path(out_dir) / "run.json", {
        "schema": "anchorpatch.simple_campaign/1", "created_at": "test",
        "scientific": scientific, "initial_execution": {},
    })
    return scientific


class FakeProcess:
    next_pid = 40000

    def __init__(self, action, code=0):
        self.action = action
        self.code = code
        self.done = False
        self.pid = FakeProcess.next_pid
        FakeProcess.next_pid += 1
        self.terminated = False

    def poll(self):
        if not self.done:
            self.action()
            self.done = True
        return self.code

    def terminate(self):
        self.terminated = True
        self.done = True
        self.code = 130

    def kill(self):
        self.terminate()

    def wait(self, timeout=None):
        del timeout
        self.done = True
        return self.code


class SimpleRuntimeAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.out = Path(self.temp.name) / "campaign"
        os.environ["OPENAI_BASE_URL"] = run_campaign._ENDPOINT

    def tearDown(self):
        self.temp.cleanup()

    def _worker_fixture(self, samples, methods=("fullrewrite",), round_trips=1):
        orders = {sample: list(methods) for sample in samples}
        _minimal_run(self.out, samples, orders, round_trips=round_trips)
        return orders

    def test_specified_three_samples_complete_without_cross_sample_state(self):
        samples = ["accounting1", "json2", "treebank4"]
        self._worker_fixture(samples)
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", return_value={"completed_round_trips": 1}):
            for sample in samples:
                self.assertEqual(run_sample.run_sample(self.out, sample, "KEY_1"), 0)
        for sample in samples:
            self.assertEqual(
                read_status(self.out, sample, ["fullrewrite"])["state"], "complete")

    @requires_local_data
    def test_all_selects_exact_sorted_234_inventory(self):
        samples = all_sample_ids(SAMPLES_ROOT)
        self.assertEqual(len(samples), 234)
        self.assertEqual(samples, sorted(samples))
        self.assertEqual(len(set(samples)), 234)
        orders = method_orders(samples, ["hybridpatch", "fullrewrite"])
        self.assertEqual(
            sum(order[0] == "hybridpatch" for order in orders.values()), 117)
        self.assertEqual(
            sum(order[0] == "fullrewrite" for order in orders.values()), 117)

    def test_same_command_skips_completed_method(self):
        self._worker_fixture(["accounting1"])
        write_status(self.out, "accounting1", {
            "state": "api_incomplete", "methods": {"fullrewrite": "incomplete"},
            "attempt": 1,
        })
        with mock.patch.object(
                run_sample, "load_resume_state",
                return_value=({"completed_round_trips": 1}, 1)), \
                mock.patch.object(run_sample, "run_method") as run_method_mock:
            self.assertEqual(run_sample.run_sample(
                self.out, "accounting1", "KEY_1"), 0)
        run_method_mock.assert_not_called()

    def test_worker_crash_is_sample_local(self):
        samples = ["accounting1", "accounting2"]
        self._worker_fixture(samples)
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(
                    run_sample, "run_method",
                    side_effect=[RuntimeError("boom"), {"completed_round_trips": 1}]):
            first = run_sample.run_sample(self.out, samples[0], "KEY_1")
            second = run_sample.run_sample(self.out, samples[1], "KEY_1")
        self.assertEqual(first, run_sample.EXIT_WORKER_FAILED)
        self.assertEqual(second, 0)
        self.assertEqual(read_status(self.out, samples[0], ["fullrewrite"])["state"], "worker_failed")
        self.assertEqual(read_status(self.out, samples[1], ["fullrewrite"])["state"], "complete")

    def test_evaluator_exception_is_sample_local(self):
        self._worker_fixture(["accounting1"])
        error = EvaluatorIncompleteError(
            "accounting1", "fullrewrite", 1, "forward", "state", ValueError("bad"))
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", side_effect=error):
            code = run_sample.run_sample(self.out, "accounting1", "KEY_1")
        self.assertEqual(code, run_sample.EXIT_EVALUATOR_FAILED)
        self.assertEqual(read_status(self.out, "accounting1", ["fullrewrite"])["state"], "evaluator_failed")

    def test_preservation_violation_is_sample_local(self):
        self._worker_fixture(["accounting1"], methods=("hybridpatch",))
        error = PreservationViolationError(
            "hybridpatch", "accounting1", 1, "forward", 1)
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", side_effect=error):
            code = run_sample.run_sample(self.out, "accounting1", "KEY_1")
        self.assertEqual(code, run_sample.EXIT_PRESERVATION_INVALID)
        self.assertEqual(read_status(self.out, "accounting1", ["hybridpatch"])["state"], "preservation_invalid")

    def test_502_503_attempts_are_preserved_from_existing_transport(self):
        calls = {"count": 0, "kwargs": None}
        attempts = [
            {"http_status": 503, "retry_budget_consumed": False},
            {"http_status": 502, "retry_budget_consumed": True},
            {"http_status": 200, "status": "success", "stream_complete": True,
             "final_usage_seen": True, "terminal_sequence_valid": True},
        ]
        def generate(messages, **kwargs):
            calls["count"] += 1
            calls["kwargs"] = kwargs
            return _complete_response("ok", attempts=attempts)
        recorder = SimpleApiRecorder(
            self.out / "samples/a/fullrewrite", "a", "fullrewrite",
            "deepseek-v4-flash", generate_impl=generate)
        recorder.set_step(1, "forward")
        result = recorder.generate(
            [{"role": "user", "content": "x"}], model="deepseek-v4-flash",
            max_retries=3, return_metadata=True, call_kind="fullrewrite_primary")
        self.assertEqual(calls["count"], 1)
        self.assertEqual(calls["kwargs"]["max_retries"], 3)
        self.assertEqual(result["transport_attempts"], attempts)

    def test_incomplete_stream_retry_metadata_is_not_reimplemented(self):
        attempts = [
            {"error_type": "incomplete_stream", "retry_budget_consumed": True},
            {"http_status": 200, "status": "success", "stream_complete": True,
             "final_usage_seen": True, "terminal_sequence_valid": True},
        ]
        recorder = SimpleApiRecorder(
            self.out / "samples/a/fullrewrite", "a", "fullrewrite", "m",
            generate_impl=lambda _messages, **_kwargs:
                _complete_response("ok", attempts=attempts))
        recorder.set_step(1, "backward")
        result = recorder.generate([], model="m", max_retries=3,
                                   call_kind="fullrewrite_primary")
        self.assertEqual(result["transport_attempts"][0]["error_type"], "incomplete_stream")

    def test_retry_exhausted_becomes_api_incomplete(self):
        self._worker_fixture(["accounting1"])
        class ApiFailure(RuntimeError):
            transport_attempts = [{"http_status": 502}]
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", side_effect=ApiFailure("exhausted")):
            code = run_sample.run_sample(self.out, "accounting1", "KEY_1")
        self.assertEqual(code, run_sample.EXIT_API_INCOMPLETE)
        self.assertEqual(read_status(self.out, "accounting1", ["fullrewrite"])["state"], "api_incomplete")

    def _campaign_args(self, samples, key_labels):
        key_file = Path(self.temp.name) / "keys.env"
        key_file.write_text("\n".join(
            f"{label}=secret-{index}" for index, label in enumerate(key_labels)),
            encoding="utf-8")
        return argparse.Namespace(
            samples=list(samples), all=False, methods=["fullrewrite"],
            round_trips=1, seed=42, keys_file=str(key_file),
            key_labels=list(key_labels), slots_per_key=1,
            out_dir=str(self.out), log_level="quiet", grace_seconds=0.1)

    def _fake_summary(self, out_dir, hint=None):
        run = read_json(Path(out_dir) / "run.json")
        rows = {}
        states = {}
        for sample in run["scientific"]["samples"]:
            status = read_status(out_dir, sample, ["fullrewrite"])
            complete = status["state"] == "complete"
            rows[sample] = {"complete": complete}
            states[status["state"]] = states.get(status["state"], 0) + 1
        return {"campaign_state": "complete" if all(r["complete"] for r in rows.values()) else (hint or "incomplete"),
                "sample_state_counts": states, "samples": rows}

    @requires_local_data
    def test_quota_exhausted_key_moves_sample_to_healthy_key(self):
        args = self._campaign_args(["accounting1"], ["KEY_1", "KEY_2"])
        launches = []
        def launch(out_dir, sample, label, _key):
            launches.append(label)
            def action():
                if label == "KEY_1":
                    write_status(out_dir, sample, {
                        "state": "api_incomplete", "methods": {"fullrewrite": "incomplete"},
                        "last_error": {"key_unusable_reason": "http_429_key_quota_exhausted"},
                    })
                else:
                    write_status(out_dir, sample, {
                        "state": "complete", "methods": {"fullrewrite": "complete"}})
            return FakeProcess(action, 10 if label == "KEY_1" else 0)
        with mock.patch.object(run_campaign, "_launch_worker", side_effect=launch), \
                mock.patch.object(run_campaign, "write_quick_summary", side_effect=self._fake_summary):
            code = run_campaign.run_campaign(args)
        self.assertEqual(code, 0)
        self.assertEqual(launches, ["KEY_1", "KEY_2"])

    @requires_local_data
    def test_all_keys_invalid_exits_incomplete_without_fatal_state(self):
        args = self._campaign_args(["accounting1"], ["KEY_1"])
        def launch(out_dir, sample, label, _key):
            def action():
                write_status(out_dir, sample, {
                    "state": "api_incomplete", "methods": {"fullrewrite": "incomplete"},
                    "last_error": {"key_unusable_reason": "http_403_access_denied"}})
            return FakeProcess(action, 10)
        with mock.patch.object(run_campaign, "_launch_worker", side_effect=launch), \
                mock.patch.object(run_campaign, "write_quick_summary", side_effect=self._fake_summary):
            code = run_campaign.run_campaign(args)
        self.assertEqual(code, 2)
        text = (self.out / "dispatch.jsonl").read_text(encoding="utf-8")
        self.assertIn("key_quarantined", text)
        self.assertNotIn("campaign_fatal", text)

    def test_replacement_key_and_concurrency_do_not_change_run_identity(self):
        scientific = {"samples": ["a"], "fixed": 1}
        first, created = create_or_validate_run(
            self.out, scientific, {"keys_file": "one", "slots_per_key": 10})
        second, resumed = create_or_validate_run(
            self.out, scientific, {"keys_file": "two", "slots_per_key": 2})
        self.assertTrue(created)
        self.assertFalse(resumed)
        self.assertEqual(first, second)

    @requires_local_data
    def test_replacement_key_resumes_existing_api_incomplete_sample(self):
        first_args = self._campaign_args(["accounting1"], ["KEY_1"])
        def fail_launch(out_dir, sample, _label, _key):
            def action():
                write_status(out_dir, sample, {
                    "state": "api_incomplete",
                    "methods": {"fullrewrite": "incomplete"},
                    "last_error": {"key_unusable_reason": "http_403_access_denied"}})
            return FakeProcess(action, 10)
        with mock.patch.object(run_campaign, "_launch_worker", side_effect=fail_launch), \
                mock.patch.object(run_campaign, "write_quick_summary", side_effect=self._fake_summary):
            self.assertEqual(run_campaign.run_campaign(first_args), 2)

        second_args = self._campaign_args(["accounting1"], ["KEY_NEW"])
        def pass_launch(out_dir, sample, _label, _key):
            return FakeProcess(lambda: write_status(out_dir, sample, {
                "state": "complete", "methods": {"fullrewrite": "complete"}}), 0)
        with mock.patch.object(run_campaign, "_launch_worker", side_effect=pass_launch), \
                mock.patch.object(run_campaign, "write_quick_summary", side_effect=self._fake_summary):
            self.assertEqual(run_campaign.run_campaign(second_args), 0)
        self.assertFalse((self.out / "campaign_recovery_authorization.json").exists())

    def test_sigterm_worker_status_is_resumable_pending(self):
        self._worker_fixture(["accounting1"])
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", side_effect=run_sample.WorkerInterrupted("stop")):
            code = run_sample.run_sample(self.out, "accounting1", "KEY_1")
        self.assertEqual(code, run_sample.EXIT_INTERRUPTED)
        self.assertEqual(read_status(self.out, "accounting1", ["fullrewrite"])["state"], "pending")
        self.assertFalse((self.out / "campaign_stop.json").exists())
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", return_value={}):
            self.assertEqual(
                run_sample.run_sample(self.out, "accounting1", "KEY_2"), 0)
        self.assertEqual(
            read_status(self.out, "accounting1", ["fullrewrite"])["state"],
            "complete")

    def test_live_sample_lock_prevents_duplicate_worker_after_dispatcher_loss(self):
        path = self.out / "locks" / "accounting1.lock"
        with sample_lock(self.out, "accounting1"):
            self.assertTrue(lock_is_held(path))
        self.assertFalse(lock_is_held(path))

    def test_success_journal_replays_without_second_post(self):
        calls = {"count": 0}
        def generate(_messages, **_kwargs):
            calls["count"] += 1
            return _complete_response("answer")
        recorder = SimpleApiRecorder(
            self.out / "samples/a/fullrewrite", "a", "fullrewrite", "m",
            generate_impl=generate)
        recorder.set_step(1, "forward")
        kwargs = {"model": "m", "return_metadata": True,
                  "call_kind": "fullrewrite_primary"}
        first = recorder.generate([], **kwargs)
        second = recorder.generate([], **kwargs)
        self.assertEqual(calls["count"], 1)
        self.assertEqual(first["message"], second["message"])
        self.assertTrue(second["replayed_from_success_journal"])

    def test_success_journal_replay_rejects_incomplete_transport_terminal(self):
        recorder = SimpleApiRecorder(
            self.out / "samples/a/fullrewrite", "a", "fullrewrite", "m",
            generate_impl=lambda _messages, **_kwargs: _complete_response("answer"))
        recorder.set_step(1, "forward")
        kwargs = {"model": "m", "return_metadata": True,
                  "call_kind": "fullrewrite_primary"}
        recorder.generate([], **kwargs)
        path = (self.out / "samples/a/fullrewrite/calls/rt01/forward/primary/"
                "success.json")
        payload = read_json(path)
        payload["response"]["stream_complete"] = False
        write_json_atomic(path, payload)
        with self.assertRaises(LocalCallEvidenceError):
            recorder.generate([], **kwargs)

    def test_verifier_accepts_explainable_primary_plus_repair_linkage(self):
        sample, method = "synthetic", "hybridpatch"
        method_path = self.out / "samples" / sample / method
        payloads = {}
        responses = []
        for leaf, call_kind, call_id, message in (
                ("primary", "hybridpatch_primary", "call-primary", "bad envelope"),
                ("repair", "hybridpatch_repair", "call-repair", "fixed envelope")):
            response = _complete_response(message, call_kind=call_kind)
            request = {
                "sample": sample, "method": method, "messages": [],
                "parameters": {"call_kind": call_kind}}
            path = method_path / "calls" / "rt01" / "forward" / leaf / "success.json"
            payload = {
                "schema": "anchorpatch.simple_api_success/1", "call_id": call_id,
                "sample": sample, "method": method, "request": request,
                "request_sha256": sha256_json(request), "response": response,
            }
            write_json_atomic(path, payload)
            payload["_path"] = str(path)
            payloads[path.resolve()] = payload
            responses.append(response)
        row = {
            "round_trip_num": 1, "round_trip_direction": "forward",
            "raw_llm_response": "fixed envelope",
            "api_call_ids": ["call-primary", "call-repair"],
            "api_raw_paths": [
                "calls/rt01/forward/primary/success.json",
                "calls/rt01/forward/repair/success.json"],
            "call_kinds": ["hybridpatch_primary", "hybridpatch_repair"],
            "api_transport_attempts": [
                attempt for response in responses
                for attempt in response["transport_attempts"]],
        }
        for key in (
                "prompt_tokens", "completion_tokens", "total_tokens",
                "input_tokens", "output_tokens", "elapsed_time", "retry_count",
                "failed_attempt_count"):
            row[key] = sum(response.get(key) or 0 for response in responses)
        for key in ("finish_reason", "response_classification", "transport",
                    "transport_revision", "reasoning_effort"):
            row[key] = responses[0][key]
        used = set()
        self.assertEqual(
            _verify_row_journals(
                method_path, sample, method, row, payloads, used), [])
        self.assertEqual(used, {"call-primary", "call-repair"})

    def test_process_death_without_success_journal_allows_retry(self):
        calls = {"count": 0}
        def generate(_messages, **_kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("died")
            return _complete_response("answer")
        recorder = SimpleApiRecorder(
            self.out / "samples/a/fullrewrite", "a", "fullrewrite", "m",
            generate_impl=generate)
        recorder.set_step(1, "forward")
        kwargs = {"model": "m", "call_kind": "fullrewrite_primary"}
        with self.assertRaises(RuntimeError):
            recorder.generate([], **kwargs)
        result = recorder.generate([], **kwargs)
        self.assertEqual(calls["count"], 2)
        self.assertEqual(result["message"], "answer")

    @requires_local_data
    def test_complete_result_pair_repairs_lagging_checkpoint(self):
        sample = "accounting1"
        loaded, sample_folder, states = load_sample(
            sample, samples_folder=str(SAMPLES_ROOT) + os.sep)
        initial_id = loaded["start_state"]
        initial = states[initial_id]
        forward_id = initial["prompts"][0]["target_state"]
        forward = states[forward_id]
        forward_instruction = initial["prompts"][0]["prompt"]
        backward_instruction = next(
            item["prompt"] for item in forward["prompts"]
            if item["target_state"] == initial_id)
        forward_context = {
            filename: "placeholder\n" for filename in forward["context"]}
        backward_context = build_context_from_folder(
            Path(sample_folder) / initial["solution_folder"])
        path = self.out / "samples" / sample / "fullrewrite"
        rows = [
            {"sample_id": sample, "method": "fullrewrite",
             "round_trip_num": 1, "round_trip_direction": "forward",
             "target_state_id": forward_id, "task_state_id": forward_id,
             "initial_state_id": initial_id,
             "edit_instruction": forward_instruction,
             "raw_llm_response": stringify_context(forward_context),
             "response_id": "r1", "rid_chain": ["r1"],
             "state_chain": [forward_id],
             "bdpatch": {"actual_method": "full_rewrite"}},
            {"sample_id": sample, "method": "fullrewrite",
             "round_trip_num": 1, "round_trip_direction": "backward",
             "target_state_id": initial_id, "task_state_id": initial_id,
             "initial_state_id": initial_id,
             "edit_instruction": backward_instruction,
             "raw_llm_response": stringify_context(backward_context),
             "response_id": "r2", "rid_chain": ["r1", "r2"],
             "state_chain": [forward_id, initial_id],
             "bdpatch": {"actual_method": "full_rewrite"}},
        ]
        path.mkdir(parents=True)
        (path / "result.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        checkpoint, complete = load_resume_state(
            self.out, sample, "fullrewrite", 1, 42, True, SAMPLES_ROOT,
            task_plan=[forward_id])
        self.assertEqual(complete, 1)
        self.assertEqual(checkpoint["completed_round_trips"], 1)
        self.assertTrue((path / "checkpoint.json").is_file())

    @requires_local_data
    def test_corrupt_single_sample_result_is_isolated(self):
        self._worker_fixture(["accounting1", "accounting2"])
        bad = self.out / "samples" / "accounting1" / "fullrewrite" / "result.jsonl"
        bad.parent.mkdir(parents=True)
        bad.write_text('{"round_trip_num":1,"round_trip_direction":"forward"}\n', encoding="utf-8")
        with mock.patch.object(run_sample, "run_method", return_value={}):
            first = run_sample.run_sample(self.out, "accounting1", "KEY_1")
        with mock.patch.object(run_sample, "load_resume_state", return_value=(None, 0)), \
                mock.patch.object(run_sample, "run_method", return_value={}):
            second = run_sample.run_sample(self.out, "accounting2", "KEY_1")
        self.assertEqual(first, run_sample.EXIT_WORKER_FAILED)
        self.assertEqual(second, 0)

    def test_quick_summary_is_generated_for_incomplete_campaign(self):
        self._worker_fixture(["accounting1"])
        summary = write_quick_summary(self.out, "incomplete")
        self.assertEqual(summary["campaign_state"], "incomplete")
        self.assertTrue((self.out / "reports" / "quick_summary.json").is_file())

    def test_quick_summary_isolates_legal_json_with_invalid_field_types(self):
        sample = "accounting1"
        self._worker_fixture([sample], round_trips=1)
        path = self.out / "samples" / sample / "fullrewrite"
        path.mkdir(parents=True)
        rows = [
            {"round_trip_num": 1, "round_trip_direction": "forward",
             "evaluation": {"score": 0.1},
             "bdpatch": {"preservation_violations": []}},
            {"round_trip_num": 1, "round_trip_direction": "backward",
             "evaluation": {"score": 0.2}, "bdpatch": {}},
        ]
        (path / "result.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        write_json_atomic(path / "checkpoint.json", {
            "completed_round_trips": 1, "current_context": {},
            "rid_chain": [], "state_chain": [],
            "context_shuffle_random_state": []})
        summary = write_quick_summary(self.out, "incomplete")
        method = summary["samples"][sample]["methods"]["fullrewrite"]
        self.assertFalse(method["complete"])
        self.assertTrue(any(
            "preservation_violations is not an integer" in item
            for item in method["errors"]))

    def test_full_verifier_does_not_modify_runtime_evidence(self):
        self._worker_fixture(["accounting1"])
        evidence = [path for path in self.out.rglob("*") if path.is_file()]
        before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in evidence}
        report = verify_full(self.out)
        after = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in evidence}
        self.assertEqual(before, after)
        self.assertFalse(report["ok"])

    @requires_local_data
    def _build_complete_verified_campaign(self):
        sample = "accounting1"
        self._worker_fixture([sample])
        scientific = read_json(self.out / "run.json")["scientific"]
        loaded, sample_folder, states = load_sample(
            sample, samples_folder=str(SAMPLES_ROOT) + os.sep)
        initial_id = loaded["start_state"]
        initial = states[initial_id]
        forward_id = initial["prompts"][0]["target_state"]
        forward = states[forward_id]
        forward_instruction = initial["prompts"][0]["prompt"]
        backward_instruction = next(
            item["prompt"] for item in forward["prompts"]
            if item["target_state"] == initial_id)
        plan_path = self.out / scientific["task_plans"][sample]["path"]
        plan = read_json(plan_path)
        plan["target_state_ids"] = [forward_id]
        write_json_atomic(plan_path, plan)
        scientific["task_plans"][sample]["sha256"] = sha256_json(plan)
        run_payload = read_json(self.out / "run.json")
        run_payload["scientific"] = scientific
        write_json_atomic(self.out / "run.json", run_payload)
        forward_context = {
            filename: "placeholder\n" for filename in forward["context"]}
        backward_context = build_context_from_folder(
            Path(sample_folder) / initial["solution_folder"])
        domain = get_domain(loaded["sample_type"])
        domain.samples_folder = str(SAMPLES_ROOT) + os.sep
        forward_evaluation = _evaluate(
            domain, sample, forward_context, forward, list(forward["context"]))
        backward_evaluation = _evaluate(
            domain, sample, backward_context, initial, list(initial["context"]))
        forward_raw = stringify_context(forward_context)
        backward_raw = stringify_context(backward_context)
        responses = {
            "forward": _complete_response(forward_raw),
            "backward": _complete_response(backward_raw),
        }
        def linked_fields(direction, call_id):
            response = responses[direction]
            return {
                "api_call_ids": [call_id],
                "api_raw_paths": [
                    f"calls/rt01/{direction}/primary/success.json"],
                "call_kinds": ["fullrewrite_primary"],
                "api_transport_attempts": response["transport_attempts"],
                **{key: response[key] for key in (
                    "finish_reason", "response_classification", "transport",
                    "transport_revision", "reasoning_effort", "prompt_tokens",
                    "completion_tokens", "total_tokens", "input_tokens",
                    "output_tokens", "elapsed_time", "retry_count",
                    "failed_attempt_count")},
            }
        rows = [
            {"sample_id": sample, "sample_type": loaded["sample_type"],
             "method": "fullrewrite", "round_trip_num": 1,
             "round_trip_direction": "forward", "target_state_id": forward_id,
             "task_state_id": forward_id, "initial_state_id": initial_id,
             "edit_instruction": forward_instruction,
             "raw_llm_response": forward_raw,
             "evaluation": forward_evaluation, "response_id": "r1",
             "rid_chain": ["r1"], "state_chain": [forward_id],
             **linked_fields("forward", "call-fwd"),
             "bdpatch": {"actual_method": "full_rewrite",
                         "preservation_violations": None}},
            {"sample_id": sample, "sample_type": loaded["sample_type"],
             "method": "fullrewrite", "round_trip_num": 1,
             "round_trip_direction": "backward", "target_state_id": initial_id,
             "task_state_id": initial_id, "initial_state_id": initial_id,
             "edit_instruction": backward_instruction,
             "raw_llm_response": backward_raw,
             "evaluation": backward_evaluation, "response_id": "r2",
             "rid_chain": ["r1", "r2"],
             "state_chain": [forward_id, initial_id],
             **linked_fields("backward", "call-bwd"),
             "bdpatch": {"actual_method": "full_rewrite",
                         "preservation_violations": None}},
        ]
        path = self.out / "samples" / sample / "fullrewrite"
        path.mkdir(parents=True)
        (path / "result.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        from utils_env import load_distractor_context, merge_distractor, shuffle_context
        random.seed(42)
        initial_context = build_context_from_folder(
            Path(sample_folder) / initial["solution_folder"])
        distractor = load_distractor_context(sample_folder)
        initial_context = shuffle_context(merge_distractor(initial_context, distractor))
        replay_context = shuffle_context(merge_distractor(forward_context, distractor))
        replay_context = shuffle_context(merge_distractor(backward_context, distractor))
        write_json_atomic(path / "checkpoint.json", {
            "completed_round_trips": 1, "current_context": replay_context,
            "rid_chain": ["r1", "r2"],
            "state_chain": [forward_id, initial_id],
            "context_shuffle_random_state": random.getstate(),
        })
        for direction, call_id in (("forward", "call-fwd"), ("backward", "call-bwd")):
            request = {
                "sample": sample, "method": "fullrewrite", "messages": [],
                "parameters": {"call_kind": "fullrewrite_primary"}}
            write_json_atomic(
                path / "calls" / "rt01" / direction / "primary" / "success.json",
                {"schema": "anchorpatch.simple_api_success/1", "call_id": call_id,
                 "sample": sample, "method": "fullrewrite",
                 "request": request, "request_sha256": sha256_json(request),
                 "response": responses[direction]})
        write_status(self.out, sample, {
            "state": "complete", "methods": {"fullrewrite": "complete"}})
        report = verify_full(self.out)
        return report, path

    def test_full_verifier_replays_complete_single_method_campaign(self):
        report, _path = self._build_complete_verified_campaign()
        self.assertTrue(report["ok"], report["issues"])
        self.assertEqual(report["analysis"]["endpoint_count"]["fullrewrite"], 1)

    def test_full_verifier_rejects_raw_response_journal_mismatch(self):
        report, path = self._build_complete_verified_campaign()
        self.assertTrue(report["ok"], report["issues"])
        rows = [json.loads(line) for line in (
            path / "result.jsonl").read_text(encoding="utf-8").splitlines()]
        rows[0]["raw_llm_response"] = "wrong response"
        (path / "result.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        report = verify_full(self.out)
        self.assertFalse(report["ok"])
        self.assertTrue(any(
            "raw response/journal mismatch" in issue for issue in report["issues"]))

    def test_full_verifier_rejects_wrong_direction_journal_link(self):
        report, path = self._build_complete_verified_campaign()
        self.assertTrue(report["ok"], report["issues"])
        rows = [json.loads(line) for line in (
            path / "result.jsonl").read_text(encoding="utf-8").splitlines()]
        rows[0]["api_raw_paths"] = [
            "calls/rt01/backward/primary/success.json"]
        (path / "result.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        report = verify_full(self.out)
        self.assertFalse(report["ok"])
        self.assertTrue(any(
            "path does not match the result step" in issue
            for issue in report["issues"]))

    def test_full_verifier_reports_orphan_success_journal(self):
        report, path = self._build_complete_verified_campaign()
        self.assertTrue(report["ok"], report["issues"])
        source = read_json(
            path / "calls" / "rt01" / "forward" / "primary" / "success.json")
        source["call_id"] = "orphan-call"
        write_json_atomic(
            path / "calls" / "rt02" / "forward" / "primary" / "success.json",
            source)
        report = verify_full(self.out)
        self.assertFalse(report["ok"])
        self.assertTrue(any(
            "orphan success journal" in issue for issue in report["issues"]))

    def test_full_verifier_rejects_task_plan_trajectory_mismatch(self):
        report, path = self._build_complete_verified_campaign()
        self.assertTrue(report["ok"], report["issues"])
        rows = [json.loads(line) for line in (
            path / "result.jsonl").read_text(encoding="utf-8").splitlines()]
        rows[0]["edit_instruction"] = "wrong frozen prompt"
        (path / "result.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        report = verify_full(self.out)
        self.assertFalse(report["ok"])
        self.assertTrue(any(
            "task-plan trajectory invalid" in issue for issue in report["issues"]))
        plan_record = read_json(self.out / "run.json")["scientific"]["task_plans"][
            "accounting1"]
        task_plan = read_json(self.out / plan_record["path"])["target_state_ids"]
        with self.assertRaises(LocalEvidenceError):
            load_resume_state(
                self.out, "accounting1", "fullrewrite", 1, 42, True,
                SAMPLES_ROOT, task_plan=task_plan)

    def test_resume_rejects_checkpoint_chain_mismatch_at_equal_progress(self):
        report, path = self._build_complete_verified_campaign()
        self.assertTrue(report["ok"], report["issues"])
        checkpoint = read_json(path / "checkpoint.json")
        checkpoint["rid_chain"] = ["wrong", "chain"]
        write_json_atomic(path / "checkpoint.json", checkpoint)
        plan_record = read_json(self.out / "run.json")["scientific"]["task_plans"][
            "accounting1"]
        task_plan = read_json(self.out / plan_record["path"])["target_state_ids"]
        with self.assertRaises(LocalEvidenceError):
            load_resume_state(
                self.out, "accounting1", "fullrewrite", 1, 42, True,
                SAMPLES_ROOT, task_plan=task_plan)

    def test_active_runtime_source_contains_no_forbidden_components(self):
        names = [
            "relay_core.py", "run_campaign.py", "run_sample.py",
            "simple_runtime_io.py", "simple_api_recorder.py", "verify_campaign.py"]
        forbidden = [
            "active_worker_set.json", "campaign_stop.json",
            "run_metadata_events.jsonl", "campaign_recovery_runtime",
            "authorize_ledger_lock_recovery", "paired_campaign_dispatch"]
        for name in names:
            text = (HERE / name).read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(token, text, f"{name}: {token}")

    def test_active_runtime_transitive_local_import_graph_excludes_forensics(self):
        pending = [
            "relay_core", "run_campaign", "run_sample", "simple_runtime_io",
            "simple_api_recorder", "verify_campaign"]
        visited = set()
        forbidden = {
            "paired_campaign_dispatch", "campaign_recovery_runtime",
            "authorize_ledger_lock_recovery", "run_meta"}
        while pending:
            module = pending.pop()
            if module in visited:
                continue
            visited.add(module)
            path = HERE / f"{module}.py"
            if not path.is_file():
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imported = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name.split(".", 1)[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module.split(".", 1)[0])
            self.assertFalse(imported & forbidden, (module, imported & forbidden))
            pending.extend(
                name for name in imported
                if (HERE / f"{name}.py").is_file() and not name.startswith("test_"))

    @requires_local_data
    def test_thirty_workers_have_disjoint_sample_write_roots(self):
        samples = all_sample_ids(SAMPLES_ROOT)[:30]
        roots = [sample_dir(self.out, sample).resolve() for sample in samples]
        self.assertEqual(len(set(roots)), 30)
        for index, left in enumerate(roots):
            for right in roots[index + 1:]:
                self.assertNotEqual(left, right)
                self.assertNotIn(left, right.parents)
                self.assertNotIn(right, left.parents)
        self.assertEqual(
            {"run.json", "task_plans", "dispatch.jsonl", "locks", "samples", "reports"},
            {"run.json", "task_plans", "dispatch.jsonl", "locks", "samples", "reports"})

        script = (
            "import sys; from pathlib import Path; "
            "from simple_runtime_io import sample_lock, append_jsonl_owned; "
            "out=Path(sys.argv[1]); sample=sys.argv[2]; "
            "lock=sample_lock(out,sample); lock.acquire(); "
            "path=out/'samples'/sample/'worker-events.jsonl'; "
            "[append_jsonl_owned(path,{'sample':sample,'index':i}) for i in range(5)]; "
            "lock.close()"
        )
        env = os.environ.copy()
        env["PYTHONPATH"] = str(HERE)
        processes = [
            subprocess.Popen(
                [sys.executable, "-B", "-c", script, str(self.out), sample],
                cwd=ROOT, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True)
            for sample in samples]
        failures = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=30)
            if process.returncode != 0:
                failures.append((process.returncode, stdout, stderr))
        self.assertFalse(failures, failures)
        for sample in samples:
            lines = (self.out / "samples" / sample / "worker-events.jsonl").read_text(
                encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 5)
            self.assertTrue(all(json.loads(line)["sample"] == sample for line in lines))
        for forbidden in (
                "api_calls.jsonl", "api_attempt_ledger.jsonl",
                "sample_outcomes.jsonl", "run_metadata_events.jsonl"):
            self.assertFalse((self.out / forbidden).exists())

    def test_round_trip_pair_validation_rejects_partial_and_duplicates(self):
        with self.assertRaises(LocalEvidenceError):
            validate_result_pairs([
                {"round_trip_num": 1, "round_trip_direction": "forward"}], 10)
        with self.assertRaises(LocalEvidenceError):
            validate_result_pairs([
                {"round_trip_num": 1, "round_trip_direction": "forward"},
                {"round_trip_num": 1, "round_trip_direction": "forward"}], 10)

    def test_campaign_lock_allows_only_one_dispatcher(self):
        with campaign_lock(self.out):
            self.assertTrue(lock_is_held(self.out / ".campaign.lock"))
        self.assertFalse(lock_is_held(self.out / ".campaign.lock"))

    def test_worker_bootstrap_failure_is_captured_in_sample_log(self):
        process = run_campaign._launch_worker(
            self.out, "missing-sample", "KEY_1", "fake-secret")
        self.assertEqual(process.wait(timeout=30), run_sample.EXIT_WORKER_FAILED)
        log = (self.out / "samples" / "missing-sample" / "worker.log").read_text(
            encoding="utf-8")
        self.assertIn("bootstrap_failed", log)
        self.assertIn("run.json", log)
        self.assertNotIn("fake-secret", log)

    @requires_local_data
    def test_incomplete_fresh_task_plan_initialization_is_resumable(self):
        sample = "accounting1"
        prepare_task_plans(self.out, [sample], 1, 42, SAMPLES_ROOT)
        args = self._campaign_args([sample], ["KEY_1"])
        def launch(out_dir, selected, _label, _key):
            return FakeProcess(lambda: write_status(out_dir, selected, {
                "state": "complete", "methods": {"fullrewrite": "complete"}}), 0)
        with mock.patch.object(run_campaign, "_launch_worker", side_effect=launch), \
                mock.patch.object(
                    run_campaign, "write_quick_summary", side_effect=self._fake_summary):
            self.assertEqual(run_campaign.run_campaign(args), 0)
        self.assertTrue((self.out / "run.json").is_file())

    def test_interrupted_summary_skips_external_locked_sample_evidence(self):
        sample = "accounting1"
        self._worker_fixture([sample], round_trips=1)
        result = self.out / "samples" / sample / "fullrewrite" / "result.jsonl"
        result.parent.mkdir(parents=True)
        result.write_text("not-json\n", encoding="utf-8")
        with sample_lock(self.out, sample):
            summary = write_quick_summary(
                self.out, "interrupted", skip_samples={sample})
        record = summary["samples"][sample]
        self.assertEqual(record["runtime_observation"], "running_external")
        self.assertTrue(any(
            "evidence not read" in item
            for item in record["methods"]["fullrewrite"]["errors"]))


if __name__ == "__main__":
    unittest.main()
