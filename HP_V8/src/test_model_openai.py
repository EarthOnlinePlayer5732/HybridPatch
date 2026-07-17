"""Zero-API regression tests for the OpenCode Go Anthropic transport."""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest import mock

import portalocker
import hashlib

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ARCHIVED_FIXTURE = os.path.join(
    HERE, "test_fixtures", "opencode_transport_v2_anomalies.json"
)
for path in (ROOT, HERE):
    if path not in sys.path:
        sys.path.insert(0, path)

import experiment_runner
import model_openai
import paired_campaign_dispatch as paired_dispatch
import probe_fr_keys
import run_meta
import utils_relay_plan
import portalocker


def _events(include_delta=True, include_stop=True, content=None):
    events = [{"type": "message_start"}]
    for index, block in enumerate(content or []):
        block_type = block.get("type")
        delta_type = "thinking_delta" if block_type == "thinking" else "text_delta"
        events.extend([
            {"type": "content_block_start", "index": index, "content_block": block},
            {"type": "content_block_delta", "index": index,
             "delta": {"type": delta_type}},
            {"type": "content_block_stop", "index": index},
        ])
    if include_delta:
        events.append({"type": "message_delta", "delta": {"stop_reason": "end_turn"}})
    if include_stop:
        events.append({"type": "message_stop"})
    return events


def _message(content, stop_reason="end_turn", usage=None):
    return {
        "id": "msg_test",
        "model": "minimax-m3",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": usage or {
            "input_tokens": 5,
            "cache_read_input_tokens": 2,
            "cache_creation_input_tokens": 3,
            "output_tokens": 4,
        },
    }


def _archived_events(case):
    counts = case["events"]
    events = [{"type": "message_start"}]
    index = 0
    if counts.get("content_block_start_thinking"):
        events.append({
            "type": "content_block_start", "index": index,
            "content_block": {"type": "thinking", "thinking": ""},
        })
        events.extend({
            "type": "content_block_delta", "index": index,
            "delta": {"type": "thinking_delta", "thinking": "x"},
        } for _ in range(counts.get("thinking_delta", 0)))
        events.extend({
            "type": "content_block_delta", "index": index,
            "delta": {"type": "signature_delta", "signature": "x"},
        } for _ in range(counts.get("signature_delta", 0)))
        if counts.get("content_block_stop", 0) >= 1:
            events.append({"type": "content_block_stop", "index": index})
        index += 1
    if counts.get("content_block_start_text"):
        events.append({
            "type": "content_block_start", "index": index,
            "content_block": {"type": "text", "text": ""},
        })
        events.extend({
            "type": "content_block_delta", "index": index,
            "delta": {"type": "text_delta", "text": "x"},
        } for _ in range(counts.get("text_delta", 0)))
        if counts.get("content_block_stop", 0) >= 2:
            events.append({"type": "content_block_stop", "index": index})
    if counts.get("message_delta"):
        events.append({
            "type": "message_delta",
            "delta": {"stop_reason": case.get("stop_reason")},
        })
    if counts.get("message_stop"):
        events.append({"type": "message_stop"})
    return events


def _archived_message(case):
    content = []
    for block in case.get("content") or []:
        if block["type"] == "text":
            content.append({"type": "text", "text": "archived-text-placeholder"})
        else:
            content.append({"type": "thinking", "thinking": "archived-thinking-placeholder"})
    usage = case.get("usage") or {"input_tokens": None, "output_tokens": None}
    return _message(content, stop_reason=case.get("stop_reason"), usage=usage)


class _FakeStream:
    def __init__(self, events, final_message):
        self.events = list(events)
        self.final_message = final_message

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def __iter__(self):
        return iter(self.events)

    def get_final_message(self):
        return self.final_message


class _FakeMessages:
    def __init__(self, owner):
        self.owner = owner

    def stream(self, **kwargs):
        self.owner.request_body = kwargs
        return _FakeStream(self.owner.events, self.owner.final_message)


def _client_factory(events, final_message, captures):
    class FakeClient:
        def __init__(self, **kwargs):
            self.events = events
            self.final_message = final_message
            self.messages = _FakeMessages(self)
            captures["client_init"] = kwargs
            captures["client"] = self

        def close(self):
            captures["closed"] = True

    return FakeClient


def _successful_normalized(text="Hello", stop_reason="end_turn"):
    response = model_openai._normalize_anthropic_response(
        _message([{"type": "text", "text": text}], stop_reason=stop_reason)
    )
    response.update({
        "_transport_attempt": {
            "attempt_index": 1,
            "status": "success",
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_delta_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "content_blocks_balanced": True,
        },
        "transport": "anthropic_sdk_v2",
        "transport_revision": "opencode_anthropic_sdk/3",
        "_raw_request_body": {},
        "_raw_stream_events": [],
    })
    return response


class OpenCodeTransportTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(
            os.environ,
            {
                "OPENCODE_API_KEY": "unit-test-key",
                "OPENCODE_TRANSPORT": "anthropic_sdk_v2",
            },
            clear=False,
        )
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def _sdk_call(self, content, stop_reason="end_turn", events=None, sink=None):
        captures = {}
        factory = _client_factory(
            events if events is not None else _events(content=content),
            _message(content, stop_reason), captures
        )
        result = model_openai._call_opencode_anthropic_sdk(
            [{"role": "user", "content": "Hello"}],
            "minimax-m3",
            None,
            0.2,
            30,
            False,
            thinking_mode="adaptive",
            call_kind="key_probe",
            raw_event_sink=sink,
            client_factory=factory,
        )
        return result, captures

    def test_runtime_config_max_tokens_and_adaptive_thinking(self):
        config = model_openai.minimax_runtime_config()
        self.assertEqual(config["base_url"], "https://opencode.ai/zen/go")
        self.assertEqual(config["request_url"], "https://opencode.ai/zen/go/v1/messages")
        self.assertEqual(config["effective_max_tokens"], 131072)
        self.assertEqual(config["thinking_mode"], "adaptive")
        self.assertEqual(model_openai._effective_minimax_max_tokens(16), 16)
        self.assertEqual(model_openai._effective_minimax_max_tokens(131072), 131072)
        with self.assertRaises(ValueError):
            model_openai._effective_minimax_max_tokens(131073)

    def test_complete_stream_cache_usage_and_request_shape(self):
        transport_log = []
        out, captures = self._sdk_call(
            [
                {"type": "thinking", "thinking": "brief"},
                {"type": "text", "text": "Hello"},
            ],
            sink=transport_log.append,
        )
        self.assertEqual(out["choices"][0]["message"]["content"], "Hello")
        self.assertEqual(out["usage"]["prompt_tokens"], 10)
        self.assertEqual(out["usage"]["completion_tokens"], 4)
        self.assertEqual(out["usage"]["total_tokens"], 14)
        self.assertTrue(out["stream_complete"])
        self.assertEqual(out["content_block_counts"], {"thinking": 1, "text": 1})
        body = captures["client"].request_body
        self.assertEqual(body["thinking"], {"type": "adaptive"})
        self.assertEqual(body["temperature"], 1.0)
        self.assertEqual(body["max_tokens"], 131072)
        self.assertTrue(captures["closed"])
        self.assertTrue(any(row["record_type"] == "attempt_start" for row in transport_log))
        self.assertTrue(any(row["record_type"] == "attempt_end" for row in transport_log))

    def test_eof_partial_text_is_incomplete_and_not_returned(self):
        captures = {}
        factory = _client_factory(
            _events(include_delta=True, include_stop=False,
                    content=[{"type": "text", "text": "partial must be discarded"}]),
            _message([{"type": "text", "text": "partial must be discarded"}]),
            captures,
        )
        with self.assertRaises(model_openai._IncompleteStreamError) as caught:
            model_openai._call_opencode_anthropic_sdk(
                [{"role": "user", "content": "Hello"}], "minimax-m3",
                16, 1.0, 30, False, client_factory=factory,
            )
        attempt = caught.exception._opencode_attempt
        self.assertEqual(attempt["error_type"], "incomplete_stream")
        self.assertFalse(attempt["stream_complete"])
        self.assertFalse(attempt["message_stop_seen"])

    def test_eof_thinking_and_missing_final_usage_are_incomplete(self):
        cases = [
            (
                _events(include_delta=False, include_stop=False,
                        content=[{"type": "thinking", "thinking": "unfinished"}]),
                _message([{"type": "thinking", "thinking": "unfinished"}]),
            ),
            (
                _events(content=[{"type": "text", "text": "looks complete"}]),
                _message(
                    [{"type": "text", "text": "looks complete"}],
                    usage={"input_tokens": 5, "output_tokens": None},
                ),
            ),
        ]
        for events, final_message in cases:
            with self.subTest(events=[event["type"] for event in events]):
                factory = _client_factory(events, final_message, {})
                with self.assertRaises(model_openai._IncompleteStreamError):
                    model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "Hello"}], "minimax-m3",
                        16, 1.0, 30, False, client_factory=factory,
                    )

    def test_complete_response_classification_matrix(self):
        cases = [
            ([{"type": "thinking", "thinking": "x"}], "max_tokens", "", "thinking_budget_exhausted"),
            ([], "end_turn", "", "model_empty"),
            ([{"type": "text", "text": "partial"}], "max_tokens", "partial", "text_truncated"),
            ([{"type": "text", "text": "done"}], "end_turn", "done", "normal"),
        ]
        for content, stop_reason, expected_text, expected_classification in cases:
            with self.subTest(stop_reason=stop_reason, classification=expected_classification):
                out, _captures = self._sdk_call(content, stop_reason=stop_reason)
                self.assertEqual(out["choices"][0]["message"]["content"], expected_text)
                self.assertEqual(out["response_classification"], expected_classification)

    def test_complete_empty_response_is_not_transport_retried(self):
        response = model_openai._normalize_anthropic_response(
            _message([{"type": "thinking", "thinking": "x"}], stop_reason="max_tokens")
        )
        response.update({
            "_transport_attempt": {
                "attempt_index": 1, "status": "success", "http_status": 200,
                "stream_complete": True, "generation_delta_seen": True,
            },
            "transport": "anthropic_sdk_v2",
            "transport_revision": "opencode_anthropic_sdk/3",
        })
        with mock.patch.object(
            model_openai, "_call_opencode_messages", return_value=response
        ) as call:
            out = model_openai.OpenAI_Model().generate(
                [{"role": "user", "content": "Hello"}], model="minimax-m3",
                return_metadata=True,
            )
        self.assertEqual(call.call_count, 1)
        self.assertEqual(out["response_classification"], "thinking_budget_exhausted")
        self.assertEqual(out["message"], "")

    def test_missing_message_start_or_unbalanced_block_is_incomplete(self):
        complete_content = [{"type": "text", "text": "done"}]
        cases = [
            [event for event in _events(content=complete_content)
             if event["type"] != "message_start"],
            [event for event in _events(content=complete_content)
             if event["type"] != "content_block_stop"],
        ]
        for events in cases:
            with self.subTest(types=[event["type"] for event in events]):
                factory = _client_factory(events, _message(complete_content), {})
                with self.assertRaises(model_openai._IncompleteStreamError):
                    model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "Hello"}], "minimax-m3",
                        16, 1.0, 30, False, client_factory=factory,
                    )

    def test_archived_v2_anomaly_event_sequences_under_v3_rules(self):
        with open(ARCHIVED_FIXTURE, encoding="utf-8") as handle:
            fixture = json.load(handle)
        observed = {}
        for case in fixture["cases"]:
            events = _archived_events(case)
            factory = _client_factory(events, _archived_message(case), {})
            if case["expected_v3"] == "incomplete_stream":
                with self.subTest(case=case["name"]):
                    with self.assertRaises(model_openai._IncompleteStreamError) as caught:
                        model_openai._call_opencode_anthropic_sdk(
                            [{"role": "user", "content": "archived replay"}],
                            "minimax-m3", 131072, 1.0, 30, False,
                            client_factory=factory,
                        )
                    attempt = caught.exception._opencode_attempt
                    self.assertTrue(attempt["generation_delta_seen"])
                    self.assertFalse(attempt["stream_complete"])
                    observed[case["name"]] = caught.exception
            else:
                with self.subTest(case=case["name"]):
                    out = model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "archived replay"}],
                        "minimax-m3", 131072, 1.0, 30, False,
                        client_factory=factory,
                    )
                    self.assertEqual(out["response_classification"], case["expected_v3"])
                    self.assertEqual(out["stop_reason"], "max_tokens")
                    self.assertTrue(out["stream_complete"])
                    observed[case["name"]] = out

        # The archived FR sequence was: thinking EOF, then a fresh POST that
        # completed thinking-only at max_tokens. Under v3 this consumes both
        # response slots and returns a scored model-empty result, not a third POST.
        first = observed["translation4_fr_rt01_forward_attempt1_eof_thinking"]
        second = observed["translation4_fr_rt01_forward_attempt2_thinking_max_tokens"]
        with mock.patch.object(
            model_openai, "_call_opencode_messages", side_effect=[first, second]
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            result = model_openai.OpenAI_Model().generate(
                [{"role": "user", "content": "archived replay"}],
                model="minimax-m3", max_tokens=131072, return_metadata=True,
            )
        self.assertEqual(call.call_count, 2)
        self.assertEqual(result["response_slots_used"], 2)
        self.assertEqual(result["response_classification"], "thinking_budget_exhausted")

    def test_pre_generation_transients_do_not_consume_response_retry(self):
        failures = [model_openai._HTTPStatusError(code, "temporary") for code in (429, 504)]
        side_effects = failures + [_successful_normalized()]
        with mock.patch.object(
            model_openai, "_call_opencode_messages", side_effect=side_effects
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            out = model_openai.OpenAI_Model().generate(
                [{"role": "user", "content": "Hello"}], model="minimax-m3",
                max_retries=99, return_metadata=True,
            )
        self.assertEqual(call.call_count, 3)
        self.assertEqual(out["message"], "Hello")
        self.assertEqual(out["response_slots_used"], 1)
        self.assertFalse(out["response_retry_used"])
        self.assertEqual(out["transient_failure_count"], 2)
        self.assertEqual(len(out["transport_attempts"]), 3)

    def test_partial_generation_consumes_the_one_response_retry(self):
        exc = model_openai._IncompleteStreamError("incomplete_stream")
        exc._opencode_attempt = {
            "attempt_index": 1, "status": "retryable_error",
            "http_status": 200, "error_type": "incomplete_stream",
            "stream_complete": False, "generation_delta_seen": True,
        }
        success = _successful_normalized()
        success["_transport_attempt"]["attempt_index"] = 2
        with mock.patch.object(
            model_openai, "_call_opencode_messages", side_effect=[exc, success]
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            out = model_openai.OpenAI_Model().generate(
                [{"role": "user", "content": "Hello"}], model="minimax-m3",
                return_metadata=True,
            )
        self.assertEqual(call.call_count, 2)
        self.assertEqual(out["response_slots_used"], 2)
        self.assertTrue(out["response_retry_used"])
        self.assertEqual(out["retry_count"], 1)

    def test_two_partial_generation_streams_exhaust_response_slots(self):
        failures = []
        for index in (1, 2):
            exc = model_openai._IncompleteStreamError("incomplete_stream")
            exc._opencode_attempt = {
                "attempt_index": index, "status": "retryable_error",
                "http_status": 200, "error_type": "incomplete_stream",
                "stream_complete": False, "generation_delta_seen": True,
            }
            failures.append(exc)
        with mock.patch.object(
            model_openai, "_call_opencode_messages", side_effect=failures
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            with self.assertRaises(model_openai.OpenCodeTransportError):
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}], model="minimax-m3",
                    return_metadata=True,
                )
        self.assertEqual(call.call_count, 2)

    def test_three_pre_generation_transients_exhaust_infrastructure_budget(self):
        with mock.patch.object(
            model_openai, "_call_opencode_messages",
            side_effect=[model_openai._HTTPStatusError(504, "temporary")] * 3,
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            with self.assertRaises(model_openai.OpenCodeTransportError):
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}], model="minimax-m3",
                    return_metadata=True,
                )
        self.assertEqual(call.call_count, 3)

    def test_incomplete_chunked_read_is_retryable(self):
        exc = RuntimeError(
            "peer closed connection without sending complete message body "
            "(incomplete chunked read)"
        )
        self.assertTrue(model_openai._is_retryable_opencode_error(exc))

    def test_convenience_snapshots_are_not_written_to_transport_log(self):
        content = [{"type": "thinking", "thinking": "x"},
                   {"type": "text", "text": "Hello"}]
        events = _events(content=content)
        events.insert(3, {
            "type": "thinking",
            "thinking": "delta",
            "snapshot": "x" * 100_000,
        })
        sink = []
        self._sdk_call(
            content,
            events=events,
            sink=sink.append,
        )
        logged_types = [
            (row.get("event") or {}).get("type")
            for row in sink if row.get("record_type") == "sdk_stream_event"
        ]
        self.assertNotIn("thinking", logged_types)
        self.assertIn("message_delta", logged_types)

    def test_auth_error_is_not_retried(self):
        with mock.patch.object(
            model_openai,
            "_call_opencode_messages",
            side_effect=model_openai._HTTPStatusError(401, "invalid key"),
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            with self.assertRaises(model_openai.OpenCodeTransportError) as caught:
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", return_metadata=True,
                )
        self.assertEqual(call.call_count, 1)
        self.assertEqual(caught.exception.transport_attempts[-1]["http_status"], 401)


class IntegrationContractTests(unittest.TestCase):
    def test_paired_dispatch_counterbalances_and_checks_campaign_integrity(self):
        orders = [paired_dispatch.method_order(index) for index in range(10)]
        self.assertEqual(
            sum(order[0] == "hybridpatch" for order in orders), 5)
        self.assertEqual(
            sum(order[0] == "fullrewrite" for order in orders), 5)

        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
            plan_sha = paired_dispatch._sha256(plan_path)
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": ["sample"],
                    "method_set": ["fullrewrite", "hybridpatch"],
                    "num_round_trips": 1,
                },
                "task_plans": {
                    "sample": {
                        "path": "sample.task_plan.json",
                        "sha256": plan_sha,
                        "forward_state_sequence": ["state_a"],
                    },
                },
            }
            api_rows = []
            for method in manifest["config"]["method_set"]:
                os.makedirs(os.path.join(out_dir, method), exist_ok=True)
                result_rows = []
                for direction in ("forward", "backward"):
                    call_kind = (
                        "hybridpatch_primary"
                        if method == "hybridpatch"
                        else "fullrewrite_primary"
                    )
                    semantic_call_id = (
                        f"{method}/sample/rt01/{direction}/{call_kind}"
                    )
                    api_rows.append({
                        "sample": "sample", "method": method,
                        "rt_index": 1, "direction": direction,
                        "call_kind": call_kind,
                        "semantic_call_id": semantic_call_id,
                        "request_id": f"original-{method}-{direction}",
                        "provider_called": True,
                        "worker_launch_id": "worker-a",
                        "worker_pid": 101,
                        "response_replayed": False,
                        "replayed_from_call_id": None,
                        "transport_revision": "opencode_anthropic_sdk/3",
                        "max_response_slots": 2,
                        "response_slots_used": 1,
                        "max_transient_failures": 3,
                        "transient_failure_count": 0,
                    })
                    result_rows.append({
                        "sample_id": "sample", "method": method,
                        "round_trip_num": 1,
                        "round_trip_direction": direction,
                        "evaluation": (
                            {"score": 1.0}
                            if direction == "backward" else {}
                        ),
                        "bdpatch": (
                            {
                                "preservation_violations": 0,
                                "exec_log": {
                                    "preservation_violations": 0,
                                },
                            }
                            if method == "hybridpatch" else {}
                        ),
                    })
                with open(
                    os.path.join(out_dir, method, "sample.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in result_rows:
                        handle.write(json.dumps(row) + "\n")
                run_meta.write_json_atomic(
                    os.path.join(out_dir, method, "sample.ckpt.json"),
                    {"completed_round_trips": 1},
                )
            with open(
                os.path.join(out_dir, "api_calls.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in api_rows:
                    handle.write(json.dumps(row) + "\n")
            with open(
                os.path.join(out_dir, "run_metadata.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({
                    "invocation_id": "invocation-a",
                    "worker_launch_id": "worker-a",
                    "worker_pid": 101,
                    "samples": ["sample"], "status": "finished",
                    "finished_at": "2026-07-17T00:00:00+08:00",
                    "task_plans": {
                        "sample": {
                            "sha256": plan_sha,
                            "round_trips": 1,
                        },
                    },
                }) + "\n")
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            for dispatch_row in (
                {
                    "event": "launch", "worker_launch_id": "worker-a",
                    "sample": "sample", "pid": 101,
                },
                {
                    "event": "worker_authorized",
                    "worker_launch_id": "worker-a", "sample": "sample",
                    "worker_pid": 101,
                    "invocation_id": "invocation-a",
                    "task_plan_sha256": plan_sha,
                },
                {
                    "event": "worker_exit", "worker_launch_id": "worker-a",
                    "sample": "sample", "pid": 101, "returncode": 0,
                    "created_at": "2026-07-17T00:00:00+08:00",
                },
            ):
                run_meta.append_jsonl_locked(dispatch_path, dispatch_row)

            with mock.patch.object(
                paired_dispatch, "_git_identity", return_value=("1" * 40, "clean")
            ):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])
                self.assertEqual(inspection["preservation_violations"], 0)

                hybrid_result_path = os.path.join(
                    out_dir, "hybridpatch", "sample.jsonl"
                )
                with open(hybrid_result_path, encoding="utf-8") as handle:
                    hybrid_result_rows = [
                        json.loads(line) for line in handle if line.strip()
                    ]

                def write_hybrid_rows():
                    with open(
                        hybrid_result_path, "w", encoding="utf-8"
                    ) as result_handle:
                        for result_row in hybrid_result_rows:
                            result_handle.write(json.dumps(result_row) + "\n")

                hybrid_result_rows[0]["sample_id"] = "wrong"
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "committed row identity mismatch" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["sample_id"] = "sample"
                hybrid_result_rows[0]["method"] = "fullrewrite"
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "committed row identity mismatch" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["method"] = "hybridpatch"

                backward_row = next(
                    row for row in hybrid_result_rows
                    if row["round_trip_direction"] == "backward"
                )
                backward_row["evaluation"] = {}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unscoreable exact backward RT1" in error
                    for error in inspection["errors"]
                ))
                backward_row["evaluation"] = {"error": "context_mismatch"}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])
                backward_row["evaluation"] = {"score": 1.0}

                original_bdpatch = dict(hybrid_result_rows[0]["bdpatch"])
                original_bdpatch["exec_log"] = dict(
                    original_bdpatch["exec_log"])
                for invalid in (True, "0", 0.5, -1):
                    hybrid_result_rows[0]["bdpatch"][
                        "preservation_violations"] = invalid
                    write_hybrid_rows()
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest)
                    self.assertTrue(any(
                        "invalid/missing preservation telemetry" in error
                        for error in inspection["errors"]
                    ))
                hybrid_result_rows[0]["bdpatch"] = dict(original_bdpatch)
                hybrid_result_rows[0]["bdpatch"]["exec_log"] = dict(
                    original_bdpatch["exec_log"])
                for invalid_nested in (False, 0.0):
                    hybrid_result_rows[0]["bdpatch"]["exec_log"][
                        "preservation_violations"
                    ] = invalid_nested
                    write_hybrid_rows()
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest)
                    self.assertTrue(any(
                        "invalid/missing preservation telemetry" in error
                        for error in inspection["errors"]
                    ))
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"
                ] = 0
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"] = 1
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "invalid/missing preservation telemetry" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["bdpatch"] = {
                    "actual_method": (
                        "hybridpatch_protocol_failure_kept_context"
                    ),
                    "preservation_violations": None,
                    "exec_log": None,
                    "hybrid": {
                        "failed_step_kept_context": True,
                        "effective_modification": False,
                    },
                }
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])
                self.assertEqual(inspection["preservation_not_applicable"], 1)
                hybrid_result_rows[0]["bdpatch"] = original_bdpatch

                hybrid_result_rows[0]["bdpatch"]["preservation_violations"] = 1
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"] = 1
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest)
                self.assertEqual(inspection["preservation_violations"], 1)
                self.assertIn(
                    "preservation_violations=1", inspection["errors"]
                )
                hybrid_result_rows[0]["bdpatch"]["preservation_violations"] = 0
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"] = 0
                write_hybrid_rows()

                original = api_rows[0]
                digest = hashlib.sha256(
                    original["semantic_call_id"].encode("utf-8")
                ).hexdigest()[:24]
                journal_dir = os.path.join(out_dir, "api_journal")
                os.makedirs(journal_dir, exist_ok=True)
                run_meta.write_json_atomic(
                    os.path.join(journal_dir, f"{digest}.response.json"),
                    {
                        "schema": "anchorpatch.api_response_journal/3",
                        "semantic_call_id": original["semantic_call_id"],
                        "call_id": original["request_id"],
                        "result": {},
                    },
                )
                replay = dict(original)
                replay.update({
                    "request_id": "replay-request",
                    "provider_called": False,
                    "response_replayed": True,
                    "replayed_from_call_id": original["request_id"],
                })
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "a", encoding="utf-8",
                ) as handle:
                    handle.write(json.dumps(replay) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])

                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "a", encoding="utf-8",
                ) as handle:
                    handle.write(json.dumps(original) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "duplicate provider POST" in error
                    for error in inspection["errors"]
                ))

                api_rows[0]["sample"] = "unknown"
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unmappable API ledger" in error
                    for error in inspection["errors"]
                ))

                api_rows[0]["sample"] = "sample"
                api_rows[0]["rt_index"] = True
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unmappable API ledger" in error
                    for error in inspection["errors"]
                ))
                api_rows[0]["rt_index"] = 1

                api_rows[0]["response_slots_used"] = True
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "response-slot overrun" in error
                    for error in inspection["errors"]
                ))
                api_rows[0]["response_slots_used"] = 1

                api_rows[0]["transient_failure_count"] = False
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "transient budget overrun" in error
                    for error in inspection["errors"]
                ))
                api_rows[0]["transient_failure_count"] = 0

                hybrid_result_rows[0]["round_trip_num"] = True
                write_hybrid_rows()
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "invalid committed row key" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["round_trip_num"] = 1
                write_hybrid_rows()

                checkpoint_path = os.path.join(
                    out_dir, "hybridpatch", "sample.ckpt.json"
                )
                for invalid_completed in (True, "1", 1.0):
                    run_meta.write_json_atomic(
                        checkpoint_path,
                        {"completed_round_trips": invalid_completed},
                    )
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, require_complete=True)
                    self.assertTrue(any(
                        "invalid checkpoint completed_round_trips" in error
                        for error in inspection["errors"]
                    ))
                run_meta.write_json_atomic(
                    checkpoint_path, {"completed_round_trips": 1}
                )

                hybrid_primary = next(
                    row for row in api_rows
                    if row["method"] == "hybridpatch"
                    and row["call_kind"] == "hybridpatch_primary"
                )
                hybrid_primary["call_kind"] = "hybridpatch_repair"
                hybrid_primary["semantic_call_id"] = (
                    hybrid_primary["semantic_call_id"].replace(
                        "hybridpatch_primary", "hybridpatch_repair"
                    )
                )
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "requires exactly one primary semantic call" in error
                    for error in inspection["errors"]
                ))

    def test_paired_dispatch_uses_exclusive_out_dir_lease(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaises(RuntimeError):
                        paired_dispatch.launch(mock.Mock(out_dir=out_dir))
                finally:
                    portalocker.unlock(lease)

    def test_paired_dispatch_resume_allows_only_audited_key_rotation(self):
        prior = {
            "schema": paired_dispatch.SCHEMA,
            "experiment_id": "exp_test",
            "run_git_commit": "1" * 40,
            "git_tree_state": "clean",
            "code_fingerprint": {"x": "y"},
            "config": {"samples": ["sample"]},
            "assignments": [{
                "sample": "sample", "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
                "console_log": "dispatch_logs/sample__KEY_01.console.log",
            }],
            "task_plans": {"sample": {"sha256": "a" * 64}},
        }
        rotated = json.loads(json.dumps(prior))
        rotated["assignments"][0]["key_label"] = "KEY_11"
        rotated["assignments"][0]["console_log"] = (
            "dispatch_logs/sample__KEY_11.console.log"
        )
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(path, prior)
            with self.assertRaises(RuntimeError):
                paired_dispatch.write_or_verify_manifest(
                    out_dir, rotated, resume=False)
            found_path, found_manifest = (
                paired_dispatch.write_or_verify_manifest(
                    out_dir, rotated, resume=True)
            )
            self.assertEqual(found_path, path)
            self.assertEqual(found_manifest, prior)

    def test_dispatch_worker_lease_and_stale_metadata_closure(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lease_path = paired_dispatch._worker_lease_path(out_dir, "sample")
            os.makedirs(os.path.dirname(lease_path), exist_ok=True)
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaises(RuntimeError):
                        paired_dispatch._assert_worker_leases_free(
                            out_dir, ["sample"])
                finally:
                    portalocker.unlock(lease)
            paired_dispatch._assert_worker_leases_free(out_dir, ["sample"])

            metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
            run_meta._write_jsonl_atomic(metadata_path, [
                {
                    "invocation_id": "invocation-a",
                    "worker_launch_id": "worker-a",
                    "worker_pid": 101,
                    "samples": ["sample"],
                    "status": "running",
                    "invocation_finished_at": None,
                    "finished_at": None,
                },
                {
                    "invocation_id": "invocation-b",
                    "worker_launch_id": "worker-b",
                    "worker_pid": 202,
                    "samples": ["other"],
                    "status": "running",
                    "invocation_finished_at": None,
                    "finished_at": None,
                },
            ])
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            for worker, sample, pid in (
                ("worker-a", "sample", 101),
                ("worker-b", "other", 202),
            ):
                run_meta.append_jsonl_locked(dispatch_path, {
                    "event": "launch_intent",
                    "worker_launch_id": worker,
                    "sample": sample,
                })
                run_meta.append_jsonl_locked(dispatch_path, {
                    "event": "launch",
                    "worker_launch_id": worker,
                    "sample": sample,
                    "pid": pid,
                })
            audited = paired_dispatch._audit_running_invocation_provenance(
                out_dir)
            self.assertEqual(
                {item["worker_launch_id"] for item in audited},
                {"worker-a", "worker-b"},
            )
            self.assertTrue(all(item["launch_recorded"] for item in audited))

            closed = run_meta.interrupt_running_invocations(
                out_dir,
                status="interrupted_by_dispatcher",
                worker_launch_ids={"worker-a"},
            )
            self.assertEqual(
                [item["invocation_id"] for item in closed],
                ["invocation-a"],
            )
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(snapshot[0]["status"], "interrupted_by_dispatcher")
            self.assertEqual(snapshot[1]["status"], "running")
            self.assertTrue(all(row["finished_at"] is None for row in snapshot))

            run_meta.interrupt_running_invocations(
                out_dir,
                status="interrupted_before_audited_resume",
            )
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(row["status"] != "running" for row in snapshot))
            self.assertTrue(all(row["finished_at"] for row in snapshot))

        with tempfile.TemporaryDirectory() as out_dir:
            running = {
                "sample": {
                    "worker_launch_id": "worker-a",
                    "key_label": "KEY_01",
                    "log": mock.Mock(),
                    "process": mock.Mock(pid=101),
                },
            }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with self.assertRaisesRegex(RuntimeError, "exited with 7"):
                paired_dispatch._record_worker_exit(
                    running, "sample", running["sample"], 7, dispatch_log
                )
            self.assertIn("sample", running)
            exit_rows = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(exit_rows[-1]["returncode"], 7)
            self.assertEqual(exit_rows[-1]["worker_launch_id"], "worker-a")
            self.assertEqual(exit_rows[-1]["pid"], 101)
            running["sample"]["process"].poll.return_value = 7
            running["sample"]["process"].wait.return_value = 7
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                [{
                    "invocation_id": "invocation-a",
                    "worker_launch_id": "worker-a",
                    "worker_pid": 101,
                    "samples": ["sample"],
                    "status": "running",
                    "invocation_finished_at": None,
                    "finished_at": None,
                }],
            )
            reconciled = paired_dispatch._stop_and_reconcile_workers(
                out_dir, running)
            self.assertEqual(
                reconciled["closed_invocations"][0]["invocation_id"],
                "invocation-a",
            )
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(
                snapshot[0]["status"], "interrupted_by_dispatcher"
            )
            self.assertIsNotNone(snapshot[0]["finished_at"])

    def test_dispatch_reconciles_metadata_even_if_termination_reports_error(self):
        running = {
            "sample": {
                "worker_launch_id": "worker-a",
                "key_label": "KEY_01",
                "log": mock.Mock(),
                "process": mock.Mock(pid=101),
            },
        }
        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with self.assertRaisesRegex(RuntimeError, "exited with 7"):
                paired_dispatch._record_worker_exit(
                    running, "sample", running["sample"], 7, dispatch_log
                )
            self.assertIn("sample", running)
            exit_rows = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(exit_rows[-1]["returncode"], 7)
            self.assertEqual(exit_rows[-1]["worker_launch_id"], "worker-a")
            self.assertEqual(exit_rows[-1]["pid"], 101)

        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_terminate_workers",
                    side_effect=RuntimeError("terminate failed"),
                ), \
                mock.patch.object(
                    paired_dispatch, "_assert_worker_leases_free"
                ) as leases_free, \
                mock.patch.object(
                    paired_dispatch, "interrupt_running_invocations",
                    return_value=[{"invocation_id": "invocation-a"}],
                ) as interrupt:
            result = paired_dispatch._stop_and_reconcile_workers(
                out_dir, running)
        self.assertEqual(result["termination_error"], "terminate failed")
        self.assertIsNone(result["lease_error"])
        self.assertEqual(
            result["closed_invocations"],
            [{"invocation_id": "invocation-a"}],
        )
        leases_free.assert_called_once_with(out_dir, running)
        interrupt.assert_called_once_with(
            out_dir,
            status="interrupted_by_dispatcher",
            worker_launch_ids={"worker-a"},
        )

    def test_audited_resume_cas_rejects_toctou_and_malformed_identity(self):
        def running(invocation, worker, pid, sample):
            return {
                "invocation_id": invocation,
                "worker_launch_id": worker,
                "worker_pid": pid,
                "samples": [sample],
                "status": "running",
                "invocation_finished_at": None,
                "finished_at": None,
            }

        audited_a = [{
            "invocation_id": "invocation-a",
            "worker_launch_id": "worker-a",
            "worker_pid": 101,
            "sample": "sample-a",
        }]
        with tempfile.TemporaryDirectory() as out_dir:
            metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
            rows = [
                running("invocation-a", "worker-a", 101, "sample-a"),
                running("invocation-b", "worker-b", 202, "sample-b"),
            ]
            run_meta._write_jsonl_atomic(metadata_path, rows)
            with self.assertRaisesRegex(RuntimeError, "set changed"):
                run_meta.interrupt_audited_running_invocations(
                    out_dir, status="interrupted", audited=audited_a)
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(row["status"] == "running" for row in snapshot))

            duplicate = [
                running("invocation-a", "worker-a", 101, "sample-a"),
                running("invocation-a", "worker-c", 303, "sample-c"),
            ]
            run_meta._write_jsonl_atomic(metadata_path, duplicate)
            with self.assertRaisesRegex(RuntimeError, "identities are invalid"):
                run_meta.interrupt_audited_running_invocations(
                    out_dir, status="interrupted", audited=audited_a)
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(row["status"] == "running" for row in snapshot))

            run_meta._write_jsonl_atomic(metadata_path, [rows[0]])
            closed = run_meta.interrupt_audited_running_invocations(
                out_dir, status="interrupted", audited=audited_a)
            self.assertEqual(closed[0]["invocation_id"], "invocation-a")
            with self.assertRaisesRegex(RuntimeError, "non-running"):
                run_meta.finish_run_metadata(
                    out_dir, "invocation-a", status="finished")

        with self.assertRaisesRegex(RuntimeError, "identities are invalid"):
            run_meta.interrupt_audited_running_invocations(
                tempfile.gettempdir(), status="interrupted",
                audited=[dict(audited_a[0], invocation_id="")],
            )

    def test_campaign_stop_latch_is_first_writer_wins_and_blocks_work(self):
        with tempfile.TemporaryDirectory() as out_dir:
            first = run_meta.record_campaign_stop_condition(
                out_dir, "preservation_violation",
                sample="sample", result_committed=False,
            )
            second = run_meta.record_campaign_stop_condition(
                out_dir, "git_identity_drift", sample="other")
            self.assertEqual(second, first)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [first])
            with self.assertRaises(ValueError):
                run_meta.record_campaign_stop_condition(
                    out_dir, "x", schema="override")

            provider_calls = []
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "offline-test-model",
                lambda *_args, **_kwargs: provider_calls.append(1),
            )
            recorder.set_step(1, "forward", "target")
            for call_kind in ("hybridpatch_primary", "hybridpatch_repair"):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    recorder.generate(
                        [], model="offline-test-model",
                        call_kind=call_kind,
                    )
            self.assertEqual(provider_calls, [])

            kwargs = {
                "command": "python test", "samples": ["sample"],
                "methods": ["hybridpatch", "fullrewrite"],
                "num_round_trips": 1, "seed": 42,
                "model": "offline-test-model", "distractor": True,
                "max_tokens": 16, "printing": False,
            }
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint", return_value={"x": "y"}):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            self.assertEqual(
                run_meta.read_run_metadata_snapshot(out_dir), [])

        with tempfile.TemporaryDirectory() as out_dir:
            with open(
                os.path.join(out_dir, "campaign_stop.json"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write("{broken")
            with self.assertRaisesRegex(RuntimeError, "invalid campaign stop"):
                run_meta.read_campaign_stop_conditions(out_dir)
            with self.assertRaisesRegex(RuntimeError, "invalid campaign stop"):
                run_meta.record_campaign_stop_condition(out_dir, "new")

    def test_dispatch_worker_barrier_closes_lease_metadata_pid_and_plan(self):
        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
            plan_sha = paired_dispatch._sha256(plan_path)
            task_plans = {
                "sample": {
                    "path": "sample.task_plan.json",
                    "sha256": plan_sha,
                    "forward_state_sequence": ["target"],
                },
            }
            worker_id = "worker-a"
            ready_path, ack_path = paired_dispatch._worker_barrier_paths(
                out_dir, worker_id)
            invocation_id = "invocation-a"
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                [{
                    "invocation_id": invocation_id,
                    "status": "running",
                    "worker_launch_id": worker_id,
                    "worker_pid": 101,
                    "samples": ["sample"],
                    "task_plans": {
                        "sample": {
                            "sha256": plan_sha,
                            "round_trips": 1,
                        },
                    },
                }],
            )
            run_meta.write_json_atomic(ready_path, {
                "schema": "anchorpatch.worker_ready/1",
                "worker_launch_id": worker_id,
                "worker_pid": 101,
                "invocation_id": invocation_id,
                "sample": "sample",
                "task_plan_path": os.path.abspath(plan_path),
                "task_plan_sha256": plan_sha,
            })
            process = mock.Mock(pid=101)
            process.poll.return_value = None
            running = {
                "sample": {
                    "process": process,
                    "worker_launch_id": worker_id,
                    "ready_path": ready_path,
                    "ack_path": ack_path,
                },
            }
            lease_path = paired_dispatch._worker_lease_path(
                out_dir, "sample")
            os.makedirs(os.path.dirname(lease_path), exist_ok=True)
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    paired_dispatch._authorize_workers(
                        out_dir, running, task_plans, dispatch_log, 1.0)
                finally:
                    portalocker.unlock(lease)
            with open(ack_path, encoding="utf-8") as handle:
                ack = json.load(handle)
            self.assertEqual(ack["invocation_id"], invocation_id)
            self.assertEqual(ack["worker_pid"], 101)
            events = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(events[-1]["event"], "worker_authorized")
            os.remove(ack_path)
            with open(lease_path, "a+", encoding="utf-8") as lease, \
                    mock.patch.object(
                        paired_dispatch, "append_jsonl_locked",
                        side_effect=OSError("fsync failed")):
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaisesRegex(OSError, "fsync failed"):
                        paired_dispatch._authorize_workers(
                            out_dir, running, task_plans,
                            dispatch_log, 1.0)
                finally:
                    portalocker.unlock(lease)
            self.assertFalse(os.path.exists(ack_path))

    def test_dispatch_cohort_authorization_is_durable_before_any_ack(self):
        with tempfile.TemporaryDirectory() as out_dir:
            task_plans = {}
            running = {}
            metadata = []
            ack_paths = []
            for index, sample in enumerate(("sample-a", "sample-b"), 1):
                plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
                plan_sha = paired_dispatch._sha256(plan_path)
                task_plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": plan_sha,
                    "forward_state_sequence": ["target"],
                }
                worker_id = f"worker-{index}"
                invocation_id = f"invocation-{index}"
                pid = 100 + index
                ready_path, ack_path = paired_dispatch._worker_barrier_paths(
                    out_dir, worker_id)
                run_meta.write_json_atomic(ready_path, {
                    "schema": "anchorpatch.worker_ready/1",
                    "worker_launch_id": worker_id,
                    "worker_pid": pid,
                    "invocation_id": invocation_id,
                    "sample": sample,
                    "task_plan_path": os.path.abspath(plan_path),
                    "task_plan_sha256": plan_sha,
                })
                process = mock.Mock(pid=pid)
                process.poll.return_value = None
                running[sample] = {
                    "process": process,
                    "worker_launch_id": worker_id,
                    "ready_path": ready_path,
                    "ack_path": ack_path,
                }
                ack_paths.append(ack_path)
                metadata.append({
                    "invocation_id": invocation_id,
                    "status": "running",
                    "worker_launch_id": worker_id,
                    "worker_pid": pid,
                    "samples": [sample],
                    "task_plans": {
                        sample: {
                            "sha256": plan_sha,
                            "round_trips": 1,
                        },
                    },
                })
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"), metadata)

            leases = []
            try:
                for sample in running:
                    lease_path = paired_dispatch._worker_lease_path(
                        out_dir, sample)
                    os.makedirs(os.path.dirname(lease_path), exist_ok=True)
                    lease = open(lease_path, "a+", encoding="utf-8")
                    portalocker.lock(
                        lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                    leases.append(lease)
                with mock.patch.object(
                        paired_dispatch, "append_jsonl_locked",
                        side_effect=[None, OSError("second fsync failed")]
                ) as append:
                    with self.assertRaisesRegex(
                            OSError, "second fsync failed"):
                        paired_dispatch._authorize_workers(
                            out_dir, running, task_plans,
                            os.path.join(out_dir, "dispatch_log.jsonl"),
                            1.0,
                        )
                    self.assertEqual(append.call_count, 2)
            finally:
                for lease in leases:
                    portalocker.unlock(lease)
                    lease.close()
            self.assertTrue(all(
                not os.path.exists(path) for path in ack_paths
            ))

    def test_runner_waits_for_exact_dispatch_authorization_before_api(self):
        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
            plan_sha = paired_dispatch._sha256(plan_path)
            invocation_id = "invocation-a"
            worker_id = "worker-a"
            active_path = paired_dispatch._active_worker_set_path(out_dir)
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "run_git_commit": "1" * 40,
                "workers": {worker_id: {"sample": "sample"}},
            })
            ready_path, ack_path = paired_dispatch._worker_barrier_paths(
                out_dir, worker_id)
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                [{
                    "invocation_id": invocation_id,
                    "status": "running",
                    "worker_launch_id": worker_id,
                    "worker_pid": os.getpid(),
                    "samples": ["sample"],
                    "task_plans": {},
                }],
            )
            run_meta.write_json_atomic(ack_path, {
                "schema": "anchorpatch.worker_start/1",
                "worker_launch_id": worker_id,
                "worker_pid": os.getpid(),
                "invocation_id": invocation_id,
                "sample": "sample",
                "task_plan_sha256": plan_sha,
            })
            environment = {
                "ANCHORPATCH_WORKER_LAUNCH_ID": worker_id,
                "ANCHORPATCH_WORKER_READY_PATH": ready_path,
                "ANCHORPATCH_WORKER_ACK_PATH": ack_path,
                "ANCHORPATCH_ACTIVE_WORKER_SET_PATH": active_path,
                "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256": plan_sha,
                "ANCHORPATCH_START_BARRIER_TIMEOUT": "1",
            }
            with mock.patch.dict(os.environ, environment, clear=False):
                ready = experiment_runner._dispatch_worker_start_barrier(
                    out_dir, ["sample"], 1, invocation_id)
            self.assertEqual(ready["task_plan_sha256"], plan_sha)
            with open(ready_path, encoding="utf-8") as handle:
                saved_ready = json.load(handle)
            self.assertEqual(saved_ready["worker_pid"], os.getpid())
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(
                snapshot[0]["task_plans"]["sample"]["sha256"], plan_sha)

    def test_standalone_runner_cannot_race_paired_dispatcher_lock(self):
        with tempfile.TemporaryDirectory() as out_dir:
            argv = [
                "experiment_runner.py", "--out_dir", out_dir,
            ]
            lock_path = os.path.join(out_dir, ".paired_dispatch.lock")
            with open(lock_path, "a+", encoding="utf-8") as lease, \
                    mock.patch.object(sys, "argv", argv), \
                    mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(experiment_runner, "main") as main:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaisesRegex(
                            RuntimeError, "dispatcher already owns"):
                        experiment_runner._run_cli_with_worker_lease()
                finally:
                    portalocker.unlock(lease)
                main.assert_not_called()
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(
                        experiment_runner, "main", return_value=17) as main:
                self.assertEqual(
                    experiment_runner._run_cli_with_worker_lease(), 17)
                main.assert_called_once_with()

    def test_smoke_cost_gate_enforces_complete_usage_and_fifty_usd_limit(self):
        commit = "1" * 40

        def make_smoke(smoke_dir, total_usd):
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": commit,
                "git_tree_state": "clean",
                "code_fingerprint": {"x": "y"},
                "config": {
                    "campaign_role": "smoke",
                    "samples": paired_dispatch.SMOKE_SAMPLES,
                    "method_set": ["fullrewrite", "hybridpatch"],
                    "num_round_trips": 2,
                    "seed": 42,
                    "model": "minimax-m3",
                    "max_tokens": 131072,
                    "distractor": True,
                    "opencode_transport": "anthropic_sdk_v2",
                    "minimax_transport": "opencode",
                    "transport_revision": "opencode_anthropic_sdk/3",
                    "stop_on_preservation_violation": True,
                },
                "task_plans": {},
            }
            run_meta.write_json_atomic(
                os.path.join(smoke_dir, "dispatch_manifest.json"), manifest)
            per_row = total_usd / paired_dispatch.SMOKE_GRID_STEPS
            for sample in paired_dispatch.SMOKE_SAMPLES:
                for method in ("fullrewrite", "hybridpatch"):
                    method_dir = os.path.join(smoke_dir, method)
                    os.makedirs(method_dir, exist_ok=True)
                    with open(
                        os.path.join(method_dir, f"{sample}.jsonl"),
                        "w", encoding="utf-8",
                    ) as handle:
                        for rt in (1, 2):
                            for direction in ("forward", "backward"):
                                handle.write(json.dumps({
                                    "sample_id": sample,
                                    "method": method,
                                    "round_trip_num": rt,
                                    "round_trip_direction": direction,
                                    "total_usd": per_row,
                                }) + "\n")

        with tempfile.TemporaryDirectory() as smoke_dir, \
                tempfile.TemporaryDirectory() as main_dir, \
                mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(commit, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"x": "y"}), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={
                        "errors": [], "preservation_violations": 0,
                    }):
            make_smoke(smoke_dir, 2.0)
            gate = paired_dispatch.evaluate_smoke_cost_gate(
                smoke_dir, main_dir)
            self.assertEqual(gate["decision"], "GO")
            self.assertEqual(gate["projected_main_usd"], 50.0)
            with open(
                os.path.join(main_dir, "smoke_cost_gate.json"),
                encoding="utf-8",
            ) as handle:
                report = json.load(handle)
            self.assertEqual(report["usage_rows"], 16)
            missing_path = os.path.join(
                smoke_dir, "hybridpatch", "treebank4.jsonl")
            with open(missing_path, encoding="utf-8") as handle:
                missing_rows = [json.loads(line) for line in handle]
            missing_rows[0].pop("total_usd")
            with open(missing_path, "w", encoding="utf-8") as handle:
                for row in missing_rows:
                    handle.write(json.dumps(row) + "\n")
            with tempfile.TemporaryDirectory() as missing_main:
                with self.assertRaisesRegex(RuntimeError, "NO_GO"):
                    paired_dispatch.evaluate_smoke_cost_gate(
                        smoke_dir, missing_main)
                with open(
                    os.path.join(missing_main, "smoke_cost_gate.json"),
                    encoding="utf-8",
                ) as handle:
                    missing_report = json.load(handle)
                self.assertIn(
                    "smoke_usage_incomplete",
                    missing_report["failure_codes"],
                )

        with tempfile.TemporaryDirectory() as smoke_dir, \
                tempfile.TemporaryDirectory() as main_dir, \
                mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(commit, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"x": "y"}), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={
                        "errors": [], "preservation_violations": 0,
                    }):
            make_smoke(smoke_dir, 2.01)
            with self.assertRaisesRegex(RuntimeError, "NO_GO"):
                paired_dispatch.evaluate_smoke_cost_gate(
                    smoke_dir, main_dir)
            with open(
                os.path.join(main_dir, "smoke_cost_gate.json"),
                encoding="utf-8",
            ) as handle:
                report = json.load(handle)
            self.assertIn(
                "projected_cost_limit_exceeded", report["failure_codes"])

        args = mock.Mock(
            campaign_role="main", smoke_dir="missing",
            samples=paired_dispatch.MAIN_SAMPLES,
            num_round_trips=10, seed=42,
        )
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    paired_dispatch, "evaluate_smoke_cost_gate",
                    side_effect=RuntimeError("gate stopped")), \
                mock.patch.object(paired_dispatch, "read_keys") as read_keys, \
                mock.patch.object(
                    paired_dispatch.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "gate stopped"):
                paired_dispatch._launch_under_lease(args, out_dir)
            read_keys.assert_not_called()
            popen.assert_not_called()

        for role, samples, round_trips, smoke_dir in (
            ("smoke", paired_dispatch.SMOKE_SAMPLES, 2, None),
            ("main", paired_dispatch.MAIN_SAMPLES, 10, "smoke"),
        ):
            with self.subTest(role=role), tempfile.TemporaryDirectory() as out_dir:
                bad_seed_args = mock.Mock(
                    campaign_role=role,
                    smoke_dir=smoke_dir,
                    samples=samples,
                    num_round_trips=round_trips,
                    seed=99,
                )
                with mock.patch.object(
                        paired_dispatch,
                        "_require_formal_opencode_transport") as transport, \
                        mock.patch.object(
                            paired_dispatch, "evaluate_smoke_cost_gate"
                        ) as gate, \
                        mock.patch.object(
                            paired_dispatch, "read_keys") as read_keys, \
                        mock.patch.object(
                            paired_dispatch.subprocess, "Popen") as popen:
                    with self.assertRaisesRegex(RuntimeError, "seed=42"):
                        paired_dispatch._launch_under_lease(
                            bad_seed_args, out_dir)
                    transport.assert_not_called()
                    gate.assert_not_called()
                    read_keys.assert_not_called()
                    popen.assert_not_called()

    def test_runner_call_kinds_are_all_adaptive(self):
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs)
            return {"message": "", "completion_tokens": 0}

        experiment_runner._attempt_hybrid_repair(
            "bad", ["invalid"], "minimax-m3", 16, fake_generate,
            editable_context={"a.txt": "x"}, edit_instruction="edit",
        )
        self.assertEqual(calls[-1]["call_kind"], "hybridpatch_repair")
        self.assertEqual(calls[-1]["thinking_mode"], "adaptive")

        class DummyDomain:
            @staticmethod
            def prepare_prompt(*_args):
                return "rewrite"

        experiment_runner._edit_step(
            "fullrewrite", DummyDomain(), "sample", "minimax-m3",
            {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
            fake_generate, step_direction="forward",
        )
        self.assertEqual(calls[-1]["call_kind"], "fullrewrite_primary")
        self.assertEqual(calls[-1]["thinking_mode"], "adaptive")

        clean_attempt = {
            "raw": "{}", "gen": {"a.txt": "x"}, "exec_log": None,
            "gate_pass": True, "need_repair": False, "partial_ok": False,
            "invalid_json": False, "partial_extraction": False,
            "fence_complete": True, "schema_errors": [], "schema_warnings": [],
            "gate_errors": [], "trigger": None, "forward_audit": None,
            "errors": [], "key": (1, 1, 1, 1.0),
        }
        with mock.patch.object(experiment_runner, "build_hybrid_prompt", return_value="patch"), \
             mock.patch.object(experiment_runner, "_run_attempt_hybrid", return_value=clean_attempt):
            experiment_runner._edit_step(
                "hybridpatch", DummyDomain(), "sample", "minimax-m3",
                {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                fake_generate, step_direction="forward",
            )
        self.assertEqual(calls[-1]["call_kind"], "hybridpatch_primary")
        self.assertEqual(calls[-1]["thinking_mode"], "adaptive")
        self.assertIn("thinking_mode='adaptive'", probe_fr_keys._PROBE_SOURCE)
        self.assertIn("call_kind='key_probe'", probe_fr_keys._PROBE_SOURCE)

    def test_run_relay_preservation_gate_stops_before_next_api_or_commit(self):
        class DummyDomain:
            samples_folder = None

        class DummyExecLog:
            def to_dict(self):
                return {"ops_accepted": 1, "ops_total": 1}

        states = {
            "initial": {
                "context": ["a.txt"],
                "solution_folder": "solution",
                "prompts": [{"target_state": "target", "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"],
                "solution_folder": "solution",
                "prompts": [{"target_state": "initial", "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}
        edit_result = (
            "raw", {"a.txt": "new"}, {}, DummyExecLog(), "hybridpatch",
            {"a.txt": "old"}, {},
        )
        with tempfile.TemporaryDirectory() as forward_dir, \
                tempfile.TemporaryDirectory() as backward_dir, \
                mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"
                ), \
                mock.patch.object(
                    experiment_runner, "load_sample",
                    return_value=(sample, forward_dir, states),
                ), \
                mock.patch.object(
                    experiment_runner, "get_domain", return_value=DummyDomain()
                ), \
                mock.patch.object(
                    experiment_runner, "load_distractor_context",
                    return_value={},
                ), \
                mock.patch.object(
                    experiment_runner, "build_context_from_folder",
                    return_value={"a.txt": "old"},
                ), \
                mock.patch.object(
                    experiment_runner, "build_relay_task_plan",
                    return_value=["target"],
                ), \
                mock.patch.object(
                    experiment_runner, "register_task_plan",
                    return_value={"sha256": "a" * 64, "round_trips": 1},
                ), \
                mock.patch.object(
                    experiment_runner, "shuffle_context",
                    side_effect=lambda value: value,
                ), \
                mock.patch.object(
                    experiment_runner, "merge_distractor",
                    side_effect=lambda value, _distractor: value,
                ), \
                mock.patch.object(
                    experiment_runner, "_evaluate",
                    return_value={"score": 1.0},
                ), \
                mock.patch.object(
                    experiment_runner, "is_context_complete",
                    return_value=True,
                ), \
                mock.patch.object(
                    experiment_runner, "dump_step_docs"
                ), \
                mock.patch.object(
                    experiment_runner, "generate_response_id",
                    side_effect=["rid-fwd", "rid-fwd-2", "rid-bwd"],
                ), \
                mock.patch.object(
                    experiment_runner, "_edit_step",
                    return_value=edit_result,
                ) as edit_step, \
                mock.patch.object(
                    experiment_runner, "append_relay_rows_and_checkpoint"
                ) as commit, \
                mock.patch.object(experiment_runner, "_row") as make_row:
            make_row.side_effect = [
                {"bdpatch": {"preservation_violations": 1}},
            ]
            with self.assertRaisesRegex(RuntimeError, "/forward"):
                experiment_runner.run_relay(
                    "hybridpatch", "sample", num_round_trips=1,
                    include_distractor=True, out_dir=forward_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=mock.Mock(), printing=False,
                    stop_on_preservation_violation=True,
                )
            self.assertEqual(edit_step.call_count, 1)
            commit.assert_not_called()
            forward_stop = run_meta.read_campaign_stop_conditions(
                forward_dir)[0]
            self.assertEqual(
                forward_stop["condition"], "preservation_violation")
            self.assertEqual(forward_stop["direction"], "forward")
            self.assertFalse(forward_stop["result_committed"])

            edit_step.reset_mock()
            commit.reset_mock()
            make_row.side_effect = [
                {
                    "bdpatch": {"preservation_violations": 0},
                    "evaluation": {"score": 1.0},
                },
                {"bdpatch": {"preservation_violations": 1}},
            ]
            with self.assertRaisesRegex(RuntimeError, "/backward"):
                experiment_runner.run_relay(
                    "hybridpatch", "sample", num_round_trips=1,
                    include_distractor=True, out_dir=backward_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=mock.Mock(), printing=False,
                    stop_on_preservation_violation=True,
                )
            self.assertEqual(edit_step.call_count, 2)
            commit.assert_not_called()
            backward_stop = run_meta.read_campaign_stop_conditions(
                backward_dir)[0]
            self.assertEqual(backward_stop["direction"], "backward")
            self.assertFalse(backward_stop["result_committed"])

    def test_formal_runner_rejects_legacy_transport(self):
        with mock.patch.dict(
            os.environ, {
                "OPENCODE_TRANSPORT": "urllib_v1",
                "MINIMAX_TRANSPORT": "opencode",
            }, clear=False
        ):
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport("minimax-m3")
        with mock.patch.dict(
            os.environ, {
                "OPENCODE_TRANSPORT": "anthropic_sdk_v2",
                "MINIMAX_TRANSPORT": "official_nonstream",
            }, clear=False
        ):
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport("minimax-m3")

    def test_empty_classification_uses_protocol_state_not_token_heuristic(self):
        classification, error_type = run_meta._empty_classification(
            {
                "stream_complete": True,
                "completion_tokens": 262144,
                "response_classification": "thinking_budget_exhausted",
            },
            "",
        )
        self.assertEqual(classification, "transport-valid but model-empty")
        self.assertEqual(error_type, "thinking_budget_exhausted")
        self.assertEqual(run_meta._empty_classification({}, "nonempty"), (None, None))

    def test_thinking_budget_empty_is_not_mislabeled_truncated_json(self):
        with tempfile.TemporaryDirectory() as out_dir:
            record = run_meta.record_model_content_anomaly(out_dir, {
                "sample_id": "translation4", "round_trip_num": 1,
                "round_trip_direction": "forward", "method": "hybridpatch",
                "raw_llm_response": "", "finish_reasons": ["max_tokens"],
                "response_classification": "thinking_budget_exhausted",
                "bdpatch": {"hybrid": {
                    "invalid_json": True,
                    "failed_step_kept_context": True,
                }},
            })
        self.assertIsNotNone(record)
        self.assertNotIn("finish_reason_max_tokens_truncated_json", record["error_type"])

    def test_hybrid_empty_and_non_protocol_outputs_do_not_trigger_repair(self):
        class DummyDomain:
            pass

        for raw, classification in (
            ("", "model_empty"),
            ("I cannot perform this task.", "normal"),
        ):
            calls = []

            def fake_generate(*_args, **kwargs):
                calls.append(kwargs.get("call_kind"))
                return {
                    "message": raw,
                    "stream_complete": True,
                    "response_classification": classification,
                    "finish_reason": "end_turn",
                }

            with mock.patch.object(experiment_runner, "build_hybrid_prompt", return_value="patch"):
                _raw, _gen, _meta, _log, _tag, _input, info = experiment_runner._edit_step(
                    "hybridpatch", DummyDomain(), "sample", "minimax-m3",
                    {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                    fake_generate, step_direction="forward",
                )
            self.assertEqual(calls, ["hybridpatch_primary"])
            self.assertFalse(info["repair"]["attempted"])

    def test_malformed_protocol_attempt_gets_one_semantic_repair(self):
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            return {
                "message": '{"protocol":"hybridpatch/2","ops":',
                "stream_complete": True,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        with mock.patch.object(experiment_runner, "build_hybrid_prompt", return_value="patch"):
            experiment_runner._edit_step(
                "hybridpatch", object(), "sample", "minimax-m3",
                {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                fake_generate, step_direction="forward",
            )
        self.assertEqual(calls, ["hybridpatch_primary", "hybridpatch_repair"])

    def test_v8_soft_burden_executes_without_repair_and_records_telemetry(self):
        def envelope(ops):
            return {
                "protocol": "hybridpatch/8",
                "plan": {
                    "task_family": "precise replacement",
                    "edit_footprint": "few_precise_edits",
                },
                "action": {"route": "local_patch", "ops": ops},
            }

        tokens = [f"item_{i:02d}_token" for i in range(32)]
        primary_ops = [
            {"op": "replace", "file": "a.txt", "old_text": token,
             "new_text": token.upper()}
            for token in tokens
        ]
        primary = (
            "```json\n"
            + json.dumps(envelope(primary_ops), ensure_ascii=False)
            + "\n```"
        )
        calls = []

        def fake_generate(_messages, *_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            return {
                "message": primary,
                "completion_tokens": 1,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        current = {
            "a.txt": " ".join(tokens) + "\n",
            "unrelated.txt": "UNRELATED_EDITABLE_CONTENT\n",
            "reference.txt": "READONLY_CONTENT\n",
        }
        distractor = {"reference.txt": current["reference.txt"]}
        raw, generated, _meta, log, tag, input_real, info = experiment_runner._edit_step(
            "hybridpatch", object(), "sample", "minimax-m3",
            current, distractor, {"context": ["a.txt"]},
            "Replace old with new in a.txt.", 16, fake_generate,
            step_direction="backward",
        )
        self.assertEqual(calls, ["hybridpatch_primary"])
        self.assertEqual(generated, {"a.txt": " ".join(
            token.upper() for token in tokens) + "\n"})
        self.assertEqual(tag, "hybridpatch")
        self.assertEqual(log.preservation_violations, 0)
        self.assertEqual(raw, primary)
        self.assertEqual(log.ops_accepted, 32)
        self.assertIsNone(log.error)
        self.assertTrue(info["protocol_burden_exceeded"])
        self.assertEqual(info["local_op_count"], 32)
        self.assertEqual(info["bulk_op_count"], 0)
        self.assertEqual(info["explicit_op_count"], 32)
        self.assertGreater(info["anchor_bytes"], 0)
        self.assertGreater(info["envelope_bytes"], 0)
        self.assertEqual(info["explicit_block_id_count"], 0)
        self.assertEqual(info["touched_file_count"], 1)
        self.assertEqual(info["prompt_profile"], "default")
        self.assertEqual(info["prompt_classifier"], "operation_family_lexical/1")
        self.assertGreater(info["prompt_chars"], 0)
        self.assertEqual(info["repair_prompt_chars"], 0)
        self.assertEqual(info["call_budget"], {"primary_calls": 1, "repair_calls": 0})
        self.assertFalse(info["repair"]["attempted"])
        self.assertEqual(info["schema_error_count"], 0)
        self.assertIsNone(info["failure_reason"])
        self.assertEqual(
            info["protocol_burden_overages"]["local_op_count"],
            {"actual": 32, "threshold": 31},
        )
        self.assertEqual(
            info["protocol_burden_attempt_overages"]["primary"],
            info["protocol_burden_overages"],
        )

    def test_v8_below_burden_threshold_records_false(self):
        envelope = {
            "protocol": "hybridpatch/8",
            "plan": {
                "task_family": "precise replacement",
                "edit_footprint": "few_precise_edits",
            },
            "action": {"route": "local_patch", "ops": [{
                "op": "replace", "file": "a.txt", "old_text": "old",
                "new_text": "new",
            }]},
        }
        primary = "```json\n" + json.dumps(envelope) + "\n```"
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            return {
                "message": primary,
                "completion_tokens": 1,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        raw, generated, _meta, log, tag, _input, info = experiment_runner._edit_step(
            "hybridpatch", object(), "sample", "minimax-m3",
            {"a.txt": "old\n"}, {}, {"context": ["a.txt"]},
            "Replace old with new in a.txt.", 16, fake_generate,
            step_direction="backward",
        )
        self.assertEqual(calls, ["hybridpatch_primary"])
        self.assertEqual(raw, primary)
        self.assertEqual(generated, {"a.txt": "new\n"})
        self.assertEqual(tag, "hybridpatch")
        self.assertFalse(info["protocol_burden_exceeded"])
        self.assertEqual(info["protocol_burden_overages"], {})
        self.assertEqual(
            info["protocol_burden_attempt_overages"], {"primary": {}},
        )
        self.assertFalse(info["repair"]["attempted"])
        self.assertEqual(log.ops_accepted, 1)
        self.assertEqual(log.ops_rejected, 0)
        self.assertEqual(log.preservation_violations, 0)

    def test_run_metadata_v3_shares_campaign_times_and_rejects_identity_mix(self):
        kwargs = {
            "command": "python test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        commit = "1" * 40
        fingerprint = {"hybrid_schema.py": "abc123"}
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(run_meta, "_git_identity", return_value=(commit, "clean")), \
                mock.patch.object(run_meta, "code_fingerprint", return_value=fingerprint):
            first = run_meta.append_run_metadata(out_dir, **kwargs)
            reordered = dict(kwargs, methods=["fullrewrite", "hybridpatch"])
            second = run_meta.append_run_metadata(out_dir, **reordered)
            self.assertEqual(first["schema"], "anchorpatch.run_metadata/3")
            self.assertEqual(first["run_git_commit"], commit)
            self.assertEqual(first["git_tree_state"], "clean")
            self.assertEqual(first["started_at"], second["started_at"])
            self.assertIsNotNone(datetime.fromisoformat(first["started_at"]).tzinfo)

            run_meta.finish_run_metadata(out_dir, first["invocation_id"])
            records = run_meta._read_run_metadata_strict(
                os.path.join(out_dir, "run_metadata.jsonl"))
            self.assertTrue(all(record["finished_at"] is None for record in records))
            run_meta.finish_run_metadata(out_dir, second["invocation_id"])
            records = run_meta._read_run_metadata_strict(
                os.path.join(out_dir, "run_metadata.jsonl"))
            finished = {record["finished_at"] for record in records}
            self.assertEqual(len(finished), 1)
            self.assertNotIn(None, finished)
            self.assertTrue(all(record["status"] == "finished" for record in records))

            with mock.patch.object(
                    run_meta, "_git_identity", return_value=("2" * 40, "clean")):
                with self.assertRaises(RuntimeError):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_git_identity", return_value=(commit, "dirty")):
                with self.assertRaises(RuntimeError):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            with self.assertRaises(RuntimeError):
                run_meta.append_run_metadata(out_dir, **dict(kwargs, seed=43))

    def test_run_metadata_locks_task_plan_hash_before_api(self):
        kwargs = {
            "command": "python test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 2,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": True,
            "max_tokens": 16,
            "printing": False,
            "context_shuffle_seeded": True,
            "context_shuffle_seed_version": "global_random_seed_v1",
            "stop_on_preservation_violation": True,
        }
        commit = "1" * 40
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(run_meta, "_git_identity", return_value=(commit, "clean")), \
                mock.patch.object(run_meta, "code_fingerprint", return_value={"x": "y"}):
            run_meta.append_run_metadata(out_dir, **kwargs)
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a", "state_b"])
            with mock.patch.dict(
                os.environ,
                {"ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256": "0" * 64},
                clear=False,
            ):
                with self.assertRaises(RuntimeError):
                    run_meta.register_task_plan(
                        out_dir, "sample", plan_path, num_round_trips=2)
            records = run_meta._read_run_metadata_strict(
                os.path.join(out_dir, "run_metadata.jsonl"))
            self.assertEqual(records[0]["task_plans"], {})

            entry = run_meta.register_task_plan(
                out_dir, "sample", plan_path, num_round_trips=2)
            self.assertEqual(len(entry["sha256"]), 64)
            records = run_meta._read_run_metadata_strict(
                os.path.join(out_dir, "run_metadata.jsonl"))
            self.assertEqual(records[0]["task_plans"]["sample"], entry)
            self.assertFalse(any(".tmp-" in name for name in os.listdir(out_dir)))

            utils_relay_plan.save_relay_task_plan(plan_path, ["state_b", "state_a"])
            with self.assertRaises(RuntimeError):
                run_meta.register_task_plan(
                    out_dir, "sample", plan_path, num_round_trips=2)

    def test_response_journal_replays_without_second_provider_post(self):
        calls = []
        result = {
            "message": "Hello", "http_status": 200, "stream_complete": True,
            "finish_reason": "end_turn", "stop_reason": "end_turn",
            "response_classification": "normal", "prompt_tokens": 5,
            "completion_tokens": 1, "total_tokens": 6, "input_tokens": 5,
            "output_tokens": 1, "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0, "transport_attempts": [],
            "call_kind": "fullrewrite_primary", "thinking_mode": "adaptive",
            "transport": "anthropic_sdk_v2",
            "transport_revision": "opencode_anthropic_sdk/3",
            "max_response_slots": 2, "response_slots_used": 1,
            "max_transient_failures": 3, "transient_failure_count": 0,
            "http_attempts_used": 1,
        }

        def fake_generate(*_args, **kwargs):
            calls.append(1)
            kwargs["_raw_event_sink"]({
                "record_type": "attempt_start", "attempt_index": 1,
            })
            kwargs["_raw_event_sink"]({
                "record_type": "attempt_end", "attempt": {
                    "attempt_index": 1, "status": "success", "stream_complete": True,
                },
            })
            kwargs["_response_commit_sink"](result)
            return dict(result)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            first = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3", fake_generate
            )
            first.set_step(1, "forward", "target")
            first.generate([{"role": "user", "content": "Hello"}],
                           model="minimax-m3", call_kind="fullrewrite_primary")

            second = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3",
                mock.Mock(side_effect=AssertionError("provider must not be called")),
            )
            second.set_step(1, "forward", "target")
            replay = second.generate([{"role": "user", "content": "Hello"}],
                                     model="minimax-m3", call_kind="fullrewrite_primary")
            self.assertEqual(len(calls), 1)
            self.assertTrue(replay["response_replayed"])
            records = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl")
            )
            self.assertEqual([r["provider_called"] for r in records], [True, False])

    def test_dangling_generation_attempt_budget_survives_worker_restart(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            first = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3", mock.Mock()
            )
            first.set_step(2, "backward", "target")
            _step, semantic = first._semantic_ids("hybridpatch_primary")
            first._append_ledger(semantic, "attempt_start", attempt_index=1)
            first._append_ledger(
                semantic, "generation_progress", attempt_index=1,
                delta_type="thinking_delta",
            )

            observed = {}

            def resumed_generate(*_args, **kwargs):
                observed.update(kwargs["_retry_state"])
                raise RuntimeError("test stop after state observation")

            second = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3", resumed_generate
            )
            second.set_step(2, "backward", "target")
            with self.assertRaises(RuntimeError):
                second.generate([{"role": "user", "content": "Hello"}],
                                model="minimax-m3", call_kind="hybridpatch_primary")
            self.assertEqual(observed["response_slots_used"], 1)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 1)

    def test_api_log_schema_v3_and_secret_redaction(self):
        secret = "unit-test-secret-key"
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {"OPENCODE_API_KEY": secret, "OPENCODE_TRANSPORT": "anthropic_sdk_v2"},
            clear=False,
        ):
            def fake_generate(*_args, **kwargs):
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "error": f"must redact {secret}",
                })
                return {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn", "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
                    "input_tokens": 5, "output_tokens": 1,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                    "transport_attempts": [{"attempt_index": 1, "status": "success"}],
                    "call_kind": "key_probe", "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/3",
                }

            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3", fake_generate
            )
            out = recorder.generate(
                [{"role": "user", "content": "Hello"}], model="minimax-m3",
                call_kind="key_probe", thinking_mode="adaptive",
            )
            with open(os.path.join(out_dir, "api_calls.jsonl"), encoding="utf-8") as handle:
                record = json.loads(handle.readline())
            self.assertEqual(record["schema"], "anchorpatch.api_call/3")
            self.assertEqual(record["call_kind"], "key_probe")
            self.assertTrue(record["stream_complete"])
            self.assertEqual(len(record["transport_attempts"]), 1)
            self.assertIn("api_call_id", out)
            for root, _dirs, files in os.walk(out_dir):
                for name in files:
                    with open(os.path.join(root, name), encoding="utf-8") as handle:
                        text = handle.read()
                    self.assertNotIn(secret, text)

    def test_failed_api_log_keeps_effective_runtime_config(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {"OPENCODE_API_KEY": "unit-test-key", "OPENCODE_TRANSPORT": "anthropic_sdk_v2"},
            clear=False,
        ):
            last_error = RuntimeError("peer closed connection (incomplete chunked read)")
            terminal = model_openai.OpenCodeTransportError(
                "failed transport",
                attempts=[{
                    "attempt_index": 1, "status": "retryable_error",
                    "error_type": "transport_disconnect", "stream_complete": False,
                }],
                last_error=last_error,
            )

            def fail_generate(*_args, **_kwargs):
                raise terminal

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3", fail_generate
            )
            with self.assertRaises(model_openai.OpenCodeTransportError):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}], model="minimax-m3",
                    max_tokens=None, thinking_mode="adaptive",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["schema"], "anchorpatch.api_call/3")
            self.assertEqual(record["transport_revision"], "opencode_anthropic_sdk/3")
            self.assertEqual(record["max_tokens"], 131072)
            self.assertEqual(record["thinking_mode"], "adaptive")

    def test_run_metadata_rejects_unversioned_resume(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_TRANSPORT": "anthropic_sdk_v2"}, clear=False
        ):
            path = os.path.join(out_dir, "run_metadata.jsonl")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"schema": "anchorpatch.run_metadata/1"}) + "\n")
            with self.assertRaises(RuntimeError):
                run_meta.append_run_metadata(
                    out_dir,
                    command="test",
                    samples=["sample"],
                    methods=["hybridpatch"],
                    num_round_trips=1,
                    seed=42,
                    model="minimax-m3",
                    distractor=False,
                    max_tokens=None,
                    printing=False,
                )


class _FakeOfficialResponse:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self):
        return self._payload


def _official_payload(content="Hello", finish_reason="stop"):
    return {
        "id": "chatcmpl-test",
        "choices": [{
            "finish_reason": finish_reason,
            "message": {
                "content": content,
                "reasoning_details": [{"type": "text", "text": "thinking..."}],
            },
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
    }


def _official_client_factory(outcomes, captures):
    """outcomes: list of payload dicts or Exceptions, consumed per call."""

    class _Completions:
        def create(self, **kwargs):
            captures.append(kwargs)
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return _FakeOfficialResponse(outcome)

    class _Chat:
        completions = _Completions()

    class _FakeOpenAI:
        def __init__(self, **kwargs):
            captures.append({"_ctor": kwargs})
            self.chat = _Chat()

    return _FakeOpenAI


class MinimaxOfficialTransportTests(unittest.TestCase):
    """MINIMAX_TRANSPORT=official_nonstream: baseline-aligned non-streaming path."""

    def _generate(self, outcomes, captures, **kwargs):
        env = {"MINIMAX_TRANSPORT": "official_nonstream",
               "MINIMAX_API_KEY": "test-key"}
        fake_cls = _official_client_factory(outcomes, captures)
        with mock.patch.dict(os.environ, env), \
                mock.patch.object(model_openai, "OpenAI", fake_cls), \
                mock.patch.object(model_openai.time, "sleep", lambda *_: None):
            wrapper = model_openai.OpenAI_Model()
            return wrapper.generate(
                [{"role": "user", "content": "Hi"}],
                model="minimax-m3", return_metadata=True,
                timeout=60, max_retries=3, **kwargs)

    def test_official_routing_request_shape_and_metadata(self):
        captures = []
        result = self._generate([_official_payload()], captures)
        ctor = captures[0]["_ctor"]
        self.assertEqual(ctor["base_url"], "https://api.minimaxi.com/v1")
        self.assertEqual(ctor["max_retries"], 0)
        request = captures[1]
        self.assertEqual(request["model"], "MiniMax-M3")
        self.assertEqual(request["max_completion_tokens"], 131072)
        self.assertEqual(request["temperature"], 1.0)
        self.assertEqual(request["extra_body"],
                         {"thinking": {"type": "adaptive"}, "reasoning_split": True})
        self.assertEqual(result["message"], "Hello")
        self.assertEqual(result["provider"], "minimax_official")
        self.assertEqual(result["transport_revision"], "minimax_official_nonstream/1")
        self.assertIs(result["stream_complete"], True)
        self.assertEqual(result["response_classification"], "normal")
        self.assertEqual(result["input_tokens"], 10)
        self.assertEqual(result["output_tokens"], 4)
        self.assertEqual(result["retry_count"], 0)
        self.assertEqual(result["http_attempts_used"], 1)

    def test_official_empty_length_is_thinking_budget_exhausted_no_retry(self):
        captures = []
        result = self._generate(
            [_official_payload(content="", finish_reason="length")], captures)
        self.assertEqual(result["message"], "")
        self.assertEqual(result["response_classification"], "thinking_budget_exhausted")
        # Complete HTTP-200 response: accepted as-is, exactly one create() call.
        self.assertEqual(len([c for c in captures if "_ctor" not in c]), 1)

    def test_official_blanket_exception_retry(self):
        captures = []
        result = self._generate(
            [RuntimeError("connection reset"), _official_payload()], captures)
        self.assertEqual(result["message"], "Hello")
        self.assertEqual(result["retry_count"], 1)
        self.assertEqual(result["http_attempts_used"], 2)
        self.assertEqual(result["failed_attempt_count"], 1)

    def test_official_abort_without_content_key_is_empty_not_crash(self):
        # Server-side generation abort: thinking-only response omits the
        # content key entirely (observed live: filesystem3 RT3 fwd,
        # protein1 RT1 bwd on 2026-07-13). Must be an accepted empty
        # response, not a malformed_provider_response crash.
        payload = {
            "id": "chatcmpl-abort",
            "choices": [{
                "finish_reason": "abort",
                "message": {"role": "assistant",
                            "reasoning_details": [{"type": "text", "text": "t"}]},
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 18641,
                      "total_tokens": 18651},
        }
        captures = []
        result = self._generate([payload], captures)
        self.assertEqual(result["message"], "")
        self.assertEqual(result["response_classification"], "model_empty")
        self.assertEqual(len([c for c in captures if "_ctor" not in c]), 1)

    def test_official_abort_with_partial_text_is_labeled(self):
        captures = []
        result = self._generate(
            [_official_payload(content="partial doc...", finish_reason="abort")],
            captures)
        self.assertEqual(result["message"], "partial doc...")
        self.assertEqual(result["response_classification"], "aborted_partial_text")

    def test_official_rate_limit_waits_without_burning_attempts(self):
        class RateLimitError(Exception):
            pass

        sleeps = []
        captures = []
        env = {"MINIMAX_TRANSPORT": "official_nonstream",
               "MINIMAX_API_KEY": "test-key"}
        fake_cls = _official_client_factory(
            [RateLimitError("5h window"), RateLimitError("5h window"),
             _official_payload()], captures)
        with mock.patch.dict(os.environ, env), \
                mock.patch.object(model_openai, "OpenAI", fake_cls), \
                mock.patch.object(model_openai.time, "sleep", sleeps.append):
            wrapper = model_openai.OpenAI_Model()
            result = wrapper.generate(
                [{"role": "user", "content": "Hi"}],
                model="minimax-m3", return_metadata=True,
                timeout=60, max_retries=3)
        self.assertEqual(result["message"], "Hello")
        # Quota waits pause and resume; baseline blanket-retry budget untouched.
        self.assertEqual(result["quota_wait_count"], 2)
        self.assertEqual(result["retry_count"], 0)
        self.assertEqual(result["http_attempts_used"], 1)
        self.assertEqual(sleeps,
                         [model_openai._MINIMAX_OFFICIAL_QUOTA_WAIT_SECONDS] * 2)

    def test_official_runtime_config_and_opencode_default(self):
        with mock.patch.dict(os.environ, {"MINIMAX_TRANSPORT": "official_nonstream"}):
            cfg = model_openai.minimax_runtime_config(max_tokens=None)
            self.assertEqual(cfg["transport_revision"], "minimax_official_nonstream/1")
            self.assertEqual(cfg["provider"], "minimax_official")
            self.assertEqual(cfg["effective_max_tokens"], 131072)
        clean_env = {k: v for k, v in os.environ.items() if k != "MINIMAX_TRANSPORT"}
        with mock.patch.dict(os.environ, clean_env, clear=True):
            cfg = model_openai.minimax_runtime_config(max_tokens=None)
            self.assertEqual(cfg["transport_revision"], "opencode_anthropic_sdk/3")


if __name__ == "__main__":
    unittest.main(verbosity=2)
