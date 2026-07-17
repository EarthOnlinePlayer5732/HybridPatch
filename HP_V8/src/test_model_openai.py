"""Zero-API regression tests for the OpenCode Go Anthropic transport."""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest import mock

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
import probe_fr_keys
import run_meta


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

    def test_formal_runner_rejects_legacy_transport(self):
        with mock.patch.dict(
            os.environ, {"OPENCODE_TRANSPORT": "urllib_v1"}, clear=False
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

    def test_v8_burden_repair_prompt_and_telemetry(self):
        def envelope(ops):
            return {
                "protocol": "hybridpatch/8",
                "plan": {
                    "task_family": "precise replacement",
                    "edit_footprint": "few_precise_edits",
                },
                "action": {"route": "local_patch", "ops": ops},
            }

        primary_ops = [
            {"op": "replace", "file": "a.txt", "old_text": "old", "new_text": "new"}
            for _ in range(32)
        ]
        primary = (
            "RAW_SENTINEL_DO_NOT_REPEAT\n```json\n"
            + json.dumps(envelope(primary_ops), ensure_ascii=False)
            + "\n```"
        )
        repaired = "```json\n" + json.dumps(envelope([{
            "op": "replace", "file": "a.txt", "old_text": "old", "new_text": "new",
        }]), ensure_ascii=False) + "\n```"
        responses = [primary, repaired]
        calls = []
        repair_prompt = []

        def fake_generate(messages, *_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            if kwargs.get("call_kind") == "hybridpatch_repair":
                repair_prompt.append(messages[0]["content"])
            return {
                "message": responses.pop(0),
                "completion_tokens": 1,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        current = {
            "a.txt": "old\n",
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
        self.assertEqual(calls, ["hybridpatch_primary", "hybridpatch_repair"])
        self.assertEqual(generated, {"a.txt": "new\n"})
        self.assertEqual(tag, "hybridpatch")
        self.assertEqual(log.preservation_violations, 0)
        self.assertEqual(raw, repaired)
        self.assertTrue(info["protocol_burden_exceeded"])
        self.assertEqual(info["local_op_count"], 1)
        self.assertEqual(info["bulk_op_count"], 0)
        self.assertEqual(info["explicit_op_count"], 1)
        self.assertEqual(info["anchor_bytes"], 3)
        self.assertEqual(info["explicit_block_id_count"], 0)
        self.assertEqual(info["touched_file_count"], 1)
        expected_ratio = len("new\n".encode("utf-8")) / sum(
            len(value.encode("utf-8")) for value in input_real.values()
        )
        self.assertAlmostEqual(info["input_output_size_ratio"], expected_ratio)
        self.assertEqual(info["prompt_profile"], "default")
        self.assertEqual(info["prompt_classifier"], "operation_family_lexical/1")
        self.assertGreater(info["prompt_chars"], 0)
        self.assertGreater(info["repair_prompt_chars"], 0)
        self.assertEqual(info["call_budget"], {"primary_calls": 1, "repair_calls": 1})
        self.assertTrue(info["repair"]["attempted"])
        self.assertTrue(info["repair"]["used"])

        self.assertEqual(len(repair_prompt), 1)
        compact = repair_prompt[0]
        self.assertIn("protocol_burden_exceeded", compact)
        self.assertIn("a.txt", compact)
        self.assertIn("reference.txt", compact)
        self.assertNotIn("READONLY_CONTENT", compact)
        self.assertNotIn("UNRELATED_EDITABLE_CONTENT", compact)
        self.assertNotIn("RAW_SENTINEL_DO_NOT_REPEAT", compact)
        self.assertNotIn("bulk_patch:", compact)
        self.assertNotIn("dsl_rules:", compact)
        self.assertEqual(compact.count("Protocol burden fix:"), 1)

    def test_v8_burden_primary_cannot_outrank_partial_repair(self):
        def envelope(ops):
            return {
                "protocol": "hybridpatch/8",
                "plan": {
                    "task_family": "precise replacement",
                    "edit_footprint": "few_precise_edits",
                },
                "action": {"route": "local_patch", "ops": ops},
            }

        primary = "```json\n" + json.dumps(envelope([
            {"op": "replace", "file": "a.txt", "old_text": "old", "new_text": "new"}
            for _ in range(32)
        ])) + "\n```"
        repair = "```json\n" + json.dumps(envelope([
            {"op": "replace", "file": "a.txt", "old_text": "old", "new_text": "new"},
            {"op": "replace", "file": "a.txt", "old_text": "missing", "new_text": "x"},
        ])) + "\n```"
        responses = [primary, repair]

        def fake_generate(*_args, **_kwargs):
            return {
                "message": responses.pop(0),
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
        self.assertEqual(raw, repair)
        self.assertEqual(generated, {"a.txt": "new\n"})
        self.assertEqual(tag, "hybridpatch")
        self.assertTrue(info["repair"]["used"])
        self.assertTrue(info["partial_acceptance"])
        self.assertEqual(log.ops_accepted, 1)
        self.assertEqual(log.ops_rejected, 1)
        self.assertEqual(log.preservation_violations, 0)

    def test_run_metadata_v3_shares_campaign_times_and_rejects_identity_mix(self):
        kwargs = {
            "command": "python test",
            "samples": ["sample"],
            "methods": ["hybridpatch"],
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
            second = run_meta.append_run_metadata(out_dir, **kwargs)
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
