"""Zero-API regression tests for the OpenCode Go Anthropic transport."""

import json
import os
import sys
import tempfile
import unittest
from unittest import mock

import httpx

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
        events.append({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 4},
        })
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
        usage = case.get("usage") or {}
        events.append({
            "type": "message_delta",
            "delta": {"stop_reason": case.get("stop_reason")},
            "usage": (
                {"output_tokens": usage.get("output_tokens")}
                if usage.get("output_tokens") is not None else {}
            ),
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
        for event in self.events:
            if isinstance(event, BaseException):
                raise event
            yield event

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
        "transport_revision": "opencode_anthropic_sdk/4",
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
        self.assertEqual(
            config["transport_resume_policy"],
            "exact_payload_new_semantic_call/1")
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
        start = next(
            row for row in transport_log if row["record_type"] == "attempt_start"
        )
        self.assertEqual(start["call_kind"], "key_probe")
        self.assertEqual(start["semantic_call_kind"], "key_probe")
        self.assertEqual(start["attempt_kind"], "transport_initial")
        self.assertTrue(any(row["record_type"] == "attempt_end" for row in transport_log))

        repair_log = []
        factory = _client_factory(
            _events(content=[{"type": "text", "text": "repair"}]),
            _message([{"type": "text", "text": "repair"}]), {},
        )
        model_openai._call_opencode_anthropic_sdk(
            [{"role": "user", "content": "Hello"}], "minimax-m3",
            16, 1.0, 30, False, call_kind="hybridpatch_repair",
            raw_event_sink=repair_log.append, attempt_index=1,
            client_factory=factory,
        )
        repair_start = next(
            row for row in repair_log if row["record_type"] == "attempt_start"
        )
        self.assertEqual(repair_start["call_kind"], "hybridpatch_repair")
        self.assertEqual(repair_start["attempt_kind"], "transport_initial")

        retry_log = []
        retry_factory = _client_factory(
            _events(content=[{"type": "text", "text": "retry"}]),
            _message([{"type": "text", "text": "retry"}]), {},
        )
        model_openai._call_opencode_anthropic_sdk(
            [{"role": "user", "content": "Hello"}], "minimax-m3",
            16, 1.0, 30, False, call_kind="key_probe",
            raw_event_sink=retry_log.append, attempt_index=2,
            client_factory=retry_factory,
        )
        retry_start = next(
            row for row in retry_log if row["record_type"] == "attempt_start"
        )
        self.assertEqual(retry_start["call_kind"], "key_probe")
        self.assertEqual(retry_start["attempt_kind"], "transport_retry")

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
                "message_stop_seen",
            ),
            (
                _events(content=[{"type": "text", "text": "looks complete"}]),
                _message(
                    [{"type": "text", "text": "looks complete"}],
                    usage={"input_tokens": 5, "output_tokens": None},
                ),
                "final_usage_seen",
            ),
        ]
        for events, final_message, missing_field in cases:
            with self.subTest(events=[event["type"] for event in events]):
                factory = _client_factory(events, final_message, {})
                with self.assertRaises(
                        model_openai._IncompleteStreamError) as caught:
                    model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "Hello"}], "minimax-m3",
                        16, 1.0, 30, False, client_factory=factory,
                    )
                attempt = caught.exception._opencode_attempt
                self.assertEqual(attempt["error_type"], "incomplete_stream")
                self.assertEqual(attempt["status"], "retryable_error")
                self.assertFalse(attempt[missing_field])
                self.assertTrue(
                    model_openai._is_retryable_opencode_error(
                        caught.exception))

    def test_blank_final_stop_reason_is_incomplete_stream(self):
        content = [{"type": "text", "text": "looks complete"}]
        for stop_reason in ("", "   \t"):
            with self.subTest(stop_reason=repr(stop_reason)):
                factory = _client_factory(
                    _events(content=content),
                    _message(content, stop_reason=stop_reason),
                    {},
                )
                with self.assertRaises(
                        model_openai._IncompleteStreamError) as caught:
                    model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "Hello"}],
                        "minimax-m3", 16, 1.0, 30, False,
                        client_factory=factory,
                    )
                attempt = caught.exception._opencode_attempt
                self.assertEqual(attempt["error_type"], "incomplete_stream")
                self.assertEqual(attempt["status"], "retryable_error")
                self.assertFalse(attempt["stream_complete"])
                self.assertEqual(attempt["stop_reason"], stop_reason)

    def test_terminal_event_usage_and_block_sequence_are_required(self):
        content = [{"type": "text", "text": "looks complete"}]
        missing_usage_events = _events(content=content)
        next(
            event for event in missing_usage_events
            if event["type"] == "message_delta"
        ).pop("usage")
        duplicate_start_events = _events(content=content)
        duplicate_start_events.insert(2, {
            "type": "content_block_start", "index": 0,
            "content_block": content[0],
        })
        duplicate_message_stop = _events(content=content)
        duplicate_message_stop.append({"type": "message_stop"})
        out_of_order_terminal = _events(content=content)
        out_of_order_terminal[-2:] = list(
            reversed(out_of_order_terminal[-2:]))
        for events, expected_field in (
                (missing_usage_events, "final_usage_seen"),
                (duplicate_start_events, "content_blocks_balanced"),
                (duplicate_message_stop, "terminal_sequence_valid"),
                (out_of_order_terminal, "terminal_sequence_valid")):
            with self.subTest(expected_field=expected_field):
                factory = _client_factory(events, _message(content), {})
                with self.assertRaises(
                        model_openai._IncompleteStreamError) as caught:
                    model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "Hello"}],
                        "minimax-m3", 16, 1.0, 30, False,
                        client_factory=factory,
                    )
                attempt = caught.exception._opencode_attempt
                self.assertFalse(attempt[expected_field])
                self.assertEqual(attempt["error_type"], "incomplete_stream")
                self.assertEqual(attempt["status"], "retryable_error")

    def test_watchdog_without_delta_consumes_transient_and_never_overlaps(self):
        class TimedOutFuture:
            def __init__(self):
                self.cancelled = False

            def result(self, timeout=None):
                raise model_openai.concurrent.futures.TimeoutError()

            def cancel(self):
                self.cancelled = True
                return True

        future = TimedOutFuture()
        pool = mock.Mock()
        pool.submit.return_value = future
        with mock.patch.object(model_openai, "_WATCHDOG_POOL", pool), \
                mock.patch.object(
                    model_openai, "_call_opencode_messages") as provider:
            with self.assertRaises(
                    model_openai.OpenCodeTransportError) as caught:
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", return_metadata=True,
                )
        attempts = caught.exception.transport_attempts
        self.assertEqual(len(attempts), 1)
        self.assertFalse(attempts[0]["generation_delta_seen"])
        self.assertEqual(attempts[0]["budget_class"], "transient_failure")
        self.assertEqual(attempts[0]["transient_failure_count"], 1)
        self.assertEqual(pool.submit.call_count, 1)
        provider.assert_not_called()
        self.assertTrue(future.cancelled)
        self.assertTrue(getattr(
            caught.exception,
            "_anchorpatch_transport_observability_failure", False))

    def test_nested_attempt_sink_propagates_critical_start_failure(self):
        class InlineFuture:
            def __init__(self, function):
                try:
                    self.result_value = function()
                    self.error = None
                except BaseException as exc:
                    self.result_value = None
                    self.error = exc

            def result(self, timeout=None):
                if self.error is not None:
                    raise self.error
                return self.result_value

            def cancel(self):
                return False

        class InlinePool:
            def __init__(self):
                self.submit_count = 0

            def submit(self, function):
                self.submit_count += 1
                return InlineFuture(function)

        raw_events = []

        def critical_sink(payload):
            raw_events.append(payload)
            raise OSError("ledger fsync failed before provider stream")

        critical_sink._anchorpatch_critical = True
        provider_calls = []
        stream_started = []

        def fake_call(*_args, **kwargs):
            provider_calls.append(kwargs.get("attempt_index"))
            kwargs["raw_event_sink"]({
                "record_type": "attempt_start",
                "attempt_index": kwargs.get("attempt_index"),
                "attempt_kind": "transport_initial",
            })
            stream_started.append(True)
            raise AssertionError("stream must not start after critical sink failure")

        pool = InlinePool()
        with mock.patch.object(model_openai, "_WATCHDOG_POOL", pool), \
                mock.patch.object(
                    model_openai, "_call_opencode_messages",
                    side_effect=fake_call) as provider:
            with self.assertRaises(
                    model_openai.OpenCodeTransportError) as caught:
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", return_metadata=True,
                    _raw_event_sink=critical_sink,
                )

        self.assertEqual(pool.submit_count, 1)
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(provider_calls, [1])
        self.assertEqual(stream_started, [])
        self.assertEqual(len(raw_events), 1)
        self.assertEqual(raw_events[0]["record_type"], "attempt_start")
        self.assertTrue(getattr(
            caught.exception.last_error,
            "_anchorpatch_transport_observability_failure", False))

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
            "transport_revision": "opencode_anthropic_sdk/4",
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

    def test_api_connection_failure_before_delta_consumes_transient_only(self):
        exc = model_openai.anthropic.APIConnectionError(
            message="connection reset",
            request=httpx.Request(
                "POST", "https://opencode.invalid/v1/messages"),
        )
        success = _successful_normalized()
        success["_transport_attempt"]["attempt_index"] = 2
        with mock.patch.object(
            model_openai, "_call_opencode_messages", side_effect=[exc, success]
        ) as call, mock.patch.object(model_openai.time, "sleep"):
            out = model_openai.OpenAI_Model().generate(
                [{"role": "user", "content": "Hello"}],
                model="minimax-m3", return_metadata=True,
            )
        self.assertEqual(call.call_count, 2)
        self.assertEqual(out["response_slots_used"], 1)
        self.assertEqual(out["transient_failure_count"], 1)
        self.assertFalse(
            out["transport_attempts"][0]["generation_delta_seen"])

    def test_incomplete_chunked_read_is_retryable(self):
        exc = RuntimeError(
            "peer closed connection without sending complete message body "
            "(incomplete chunked read)"
        )
        self.assertTrue(model_openai._is_retryable_opencode_error(exc))

    def test_anthropic_status_200_streaming_failure_is_incomplete_and_retryable(self):
        response = httpx.Response(
            200, request=httpx.Request("POST", "https://opencode.invalid/v1/messages")
        )
        exc = model_openai.anthropic.APIStatusError(
            "Streaming response failed", response=response,
            body={"type": "api_error", "message": "Streaming response failed"},
        )
        self.assertEqual(model_openai._transport_status_code(exc), 200)
        self.assertTrue(model_openai._is_incomplete_stream_exception(exc))
        self.assertEqual(model_openai._transport_error_type(exc), "incomplete_stream")
        self.assertTrue(model_openai._is_retryable_opencode_error(exc))
        exc._opencode_attempt = {
            "attempt_index": 1, "status": "retryable_error",
            "http_status": 200, "error_type": "incomplete_stream",
            "stream_complete": False, "message_start_seen": True,
            "message_stop_seen": False, "final_usage_seen": False,
            "generation_delta_seen": True, "thinking_delta_seen": True,
            "content_blocks_started": 1, "content_blocks_stopped": 0,
            "content_blocks_balanced": False,
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
        self.assertEqual(out["transport_attempts"][0]["error_type"], "incomplete_stream")

        stream_exc = model_openai.anthropic.APIStatusError(
            "Streaming response failed", response=response,
            body={"type": "api_error", "message": "Streaming response failed"},
        )
        events = [
            {"type": "message_start"},
            {"type": "content_block_start", "index": 0,
             "content_block": {"type": "thinking", "thinking": ""}},
            {"type": "content_block_delta", "index": 0,
             "delta": {"type": "thinking_delta", "thinking": "partial"}},
            stream_exc,
        ]
        factory = _client_factory(events, _message([]), {})
        with self.assertRaises(model_openai.anthropic.APIStatusError) as caught:
            model_openai._call_opencode_anthropic_sdk(
                [{"role": "user", "content": "Hello"}], "minimax-m3",
                16, 1.0, 30, False, client_factory=factory,
            )
        attempt = caught.exception._opencode_attempt
        self.assertEqual(attempt["error_type"], "incomplete_stream")
        self.assertEqual(attempt["status"], "retryable_error")
        self.assertTrue(attempt["generation_delta_seen"])
        self.assertFalse(attempt["content_blocks_balanced"])

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
    @staticmethod
    def _failed_attempt_end_fields(*, generation_delta_seen=True,
                                   error_type="incomplete_stream"):
        return {
            "status": "retryable_error",
            "error_type": error_type,
            "stream_complete": False,
            "message_stop_seen": False,
            "final_usage_seen": False,
            "generation_delta_seen": generation_delta_seen,
            "content_blocks_started": 1 if generation_delta_seen else 0,
            "content_blocks_stopped": 0,
            "content_blocks_balanced": not generation_delta_seen,
            "terminal_sequence_valid": False,
        }

    @staticmethod
    def _successful_attempt_end_fields():
        return {
            "status": "success",
            "stream_complete": True,
            "stop_reason": "end_turn",
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "content_blocks_started": 1,
            "content_blocks_stopped": 1,
            "content_blocks_balanced": True,
            "terminal_sequence_valid": True,
        }

    def _append_exhausted_response_generation(
            self, recorder, semantic_call_id, call_id,
            request_fingerprint, attempt_indices):
        generation = int(semantic_call_id.rsplit("/g", 1)[1])
        recorder._append_ledger(
            semantic_call_id, "semantic_request", call_id=call_id,
            call_kind="hybridpatch_primary",
            request_fingerprint=request_fingerprint)
        for local_index, attempt_index in enumerate(attempt_indices, 1):
            recorder._append_ledger(
                semantic_call_id, "attempt_start",
                attempt_index=attempt_index, call_id=call_id,
                call_kind="hybridpatch_primary",
                attempt_kind=(
                    "transport_initial"
                    if generation == 0 and local_index == 1
                    else "transport_retry"
                    if generation == 0
                    else "transport_recovery_initial"
                    if local_index == 1
                    else "transport_recovery_retry"),
                request_fingerprint=request_fingerprint)
            recorder._append_ledger(
                semantic_call_id, "generation_progress",
                attempt_index=attempt_index, call_id=call_id,
                delta_type="text_delta")
            recorder._append_ledger(
                semantic_call_id, "attempt_end",
                attempt_index=attempt_index, call_id=call_id,
                **self._failed_attempt_end_fields())
            recorder._append_ledger(
                semantic_call_id, "attempt_budget",
                attempt_index=attempt_index, call_id=call_id,
                budget_class="response_slot",
                response_slots_used=local_index,
                transient_failure_count=0)
        recorder._append_ledger(
            semantic_call_id, "call_failed", call_id=call_id,
            status="provider_failure", error_type="incomplete_stream",
            response_slots_used=len(attempt_indices),
            transient_failure_count=0,
            http_attempts_used=attempt_indices[-1],
            attempt_index=attempt_indices[-1],
            request_fingerprint=request_fingerprint)

    def test_runner_call_kinds_are_all_adaptive(self):
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs)
            return {"message": "", "completion_tokens": 0}

        with mock.patch.object(
                experiment_runner, "build_hybrid_repair_prompt",
                return_value="repair"):
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

        with mock.patch.object(
                experiment_runner, "build_hybrid_prompt",
                return_value="patch"), mock.patch.object(
                    experiment_runner, "build_hybrid_repair_prompt",
                    return_value="repair"):
            experiment_runner._edit_step(
                "hybridpatch", object(), "sample", "minimax-m3",
                {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                fake_generate, step_direction="forward",
            )
        self.assertEqual(calls, ["hybridpatch_primary", "hybridpatch_repair"])

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
            "transport_revision": "opencode_anthropic_sdk/4",
            "transport_resume_policy": (
                "exact_payload_new_semantic_call/1"),
            "max_response_slots": 2, "response_slots_used": 1,
            "max_transient_failures": 3, "transient_failure_count": 0,
            "http_attempts_used": 1,
        }

        def fake_generate(*_args, **kwargs):
            calls.append(1)
            kwargs["_raw_event_sink"]({
                "record_type": "attempt_start", "attempt_index": 1,
                "attempt_kind": "transport_initial",
            })
            kwargs["_raw_event_sink"]({
                "record_type": "sdk_stream_event", "attempt_index": 1,
                "event": {"type": "content_block_delta",
                          "delta": {"type": "text_delta"}},
            })
            kwargs["_raw_event_sink"]({
                "record_type": "attempt_end", "attempt": {
                    "attempt_index": 1,
                    **self._successful_attempt_end_fields(),
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

            ledger_path = os.path.join(
                out_dir, "api_attempt_ledger.jsonl")
            ledger = run_meta._read_jsonl_records_with_retry(ledger_path)
            self.assertEqual(
                [row.get("event") for row in ledger].count(
                    "response_committed"), 1)
            with open(ledger_path, "w", encoding="utf-8") as handle:
                for row in ledger:
                    if row.get("event") != "response_committed":
                        handle.write(json.dumps(row) + "\n")

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
            reconciled = run_meta._read_jsonl_records_with_retry(ledger_path)
            self.assertEqual(
                [row.get("event") for row in reconciled].count(
                    "response_committed"), 1)

            journal_path = second._journal_path(
                second._semantic_ids("fullrewrite_primary")[1]
            )
            with open(journal_path, encoding="utf-8") as handle:
                valid = json.load(handle)
            broken = dict(valid)
            broken["request_fingerprint"] = "wrong-fingerprint"
            run_meta.write_json_atomic(journal_path, broken)
            fingerprint_forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called")
            )
            third = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3",
                fingerprint_forbidden,
            )
            third.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "fingerprint differs within"):
                third.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary",
                )
            fingerprint_forbidden.assert_not_called()

            broken = dict(valid)
            broken["schema"] = "anchorpatch.api_response_journal/3"
            run_meta.write_json_atomic(journal_path, broken)
            forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called")
            )
            fourth = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3", forbidden
            )
            fourth.set_step(1, "forward", "target")
            with self.assertRaisesRegex(RuntimeError, "journal schema"):
                fourth.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary",
                )
            forbidden.assert_not_called()

    def test_dangling_generation_attempt_fails_closed_before_provider(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            first = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                provider,
            )
            first.set_step(2, "backward", "target")
            messages = [{"role": "user", "content": "Hello"}]
            generate_kwargs = {
                "model": "minimax-m3",
                "call_kind": "hybridpatch_primary",
            }
            fingerprint = run_meta._semantic_request_fingerprint(
                (messages,), generate_kwargs, "minimax-m3",
                "hybridpatch_primary")
            _step, semantic = first._semantic_ids("hybridpatch_primary")
            first._append_ledger(
                semantic, "semantic_request", call_id="crashed-call",
                call_kind="hybridpatch_primary",
                request_fingerprint=fingerprint)
            first._append_ledger(
                semantic, "attempt_start", attempt_index=1,
                call_id="crashed-call", call_kind="hybridpatch_primary",
                attempt_kind="transport_initial",
                request_fingerprint=fingerprint)
            second = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                provider,
            )
            second.set_step(2, "backward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "unclosed HTTP attempt"):
                second.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
            provider.assert_not_called()

    def test_malformed_or_v3_attempt_ledger_fails_closed_before_provider(self):
        for payload, expected in (
            ('{"schema":', "invalid JSONL"),
            (json.dumps({
                "schema": "anchorpatch.api_attempt/3",
                "semantic_call_id": (
                    "hybridpatch/sample/rt01/forward/hybridpatch_primary"),
                "event": "semantic_request",
            }), "schema differs"),
            (json.dumps({
                "schema": "anchorpatch.api_attempt/4",
                "step_id": "hybridpatch/sample/rt01/forward",
                "semantic_root_id": (
                    "hybridpatch/sample/rt01/forward/hybridpatch_primary"),
                "semantic_call_id": (
                    "hybridpatch/sample/rt01/forward/"
                    "hybridpatch_primary/g000"),
                "generation_index": 0,
                "parent_semantic_call_id": None,
                "event": "transport_resume",
            }), "unknown event|retired same-ID"),
        ):
            with self.subTest(expected=expected), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ,
                        {"OPENCODE_API_KEY": "unit-test-key"}, clear=False):
                with open(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    handle.write(payload + "\n")
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                with self.assertRaisesRegex(RuntimeError, expected):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3",
                        call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_lineage_gap_and_fingerprint_mismatch_fail_before_provider(self):
        cases = (
            ("gap", 2, "same", "semantic lineage generation order"),
            ("fingerprint", 1, "different",
             "request fingerprint is inconsistent"),
        )
        for label, next_generation, next_fingerprint, expected in cases:
            with self.subTest(label=label), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ,
                        {"OPENCODE_API_KEY": "unit-test-key"}, clear=False):
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                step_id, root = recorder._semantic_root(
                    "hybridpatch_primary")
                generation_zero = f"{root}/g000"
                recorder._append_ledger(
                    generation_zero, "semantic_request", call_id="call-0",
                    call_kind="hybridpatch_primary",
                    request_fingerprint="same")
                for attempt_index in (1, 2):
                    recorder._append_ledger(
                        generation_zero, "attempt_start",
                        attempt_index=attempt_index, call_id="call-0",
                        call_kind="hybridpatch_primary",
                        attempt_kind=(
                            "transport_initial" if attempt_index == 1
                            else "transport_retry"),
                        request_fingerprint="same")
                    recorder._append_ledger(
                        generation_zero, "generation_progress",
                        attempt_index=attempt_index, call_id="call-0",
                        delta_type="text_delta")
                    recorder._append_ledger(
                        generation_zero, "attempt_end",
                        attempt_index=attempt_index, call_id="call-0",
                        **self._failed_attempt_end_fields())
                    recorder._append_ledger(
                        generation_zero, "attempt_budget",
                        attempt_index=attempt_index, call_id="call-0",
                        budget_class="response_slot",
                        response_slots_used=attempt_index,
                        transient_failure_count=0)
                recorder._append_ledger(
                    generation_zero, "call_failed", call_id="call-0",
                    attempt_index=2, status="provider_failure",
                    error_type="incomplete_stream", response_slots_used=2,
                    transient_failure_count=0, http_attempts_used=2,
                    request_fingerprint="same")
                exact = f"{root}/g{next_generation:03d}"
                parent = f"{root}/g{next_generation - 1:03d}"
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"), {
                        "schema": "anchorpatch.api_attempt/4",
                        "step_id": step_id,
                        "semantic_root_id": root,
                        "semantic_call_id": exact,
                        "generation_index": next_generation,
                        "parent_semantic_call_id": parent,
                        "worker_launch_id": recorder.worker_launch_id,
                        "event": "semantic_request",
                        "call_id": f"call-{next_generation}",
                        "call_kind": "hybridpatch_primary",
                        "request_fingerprint": next_fingerprint,
                    })
                with self.assertRaisesRegex(RuntimeError, expected):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3",
                        call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_forged_transport_budget_counters_fail_before_provider(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", provider)
            recorder.set_step(1, "forward", "target")
            _step, semantic = recorder._semantic_ids(
                "hybridpatch_primary")
            fingerprint = "fingerprint"
            recorder._append_ledger(
                semantic, "semantic_request", call_id="call-0",
                call_kind="hybridpatch_primary",
                request_fingerprint=fingerprint)
            recorder._append_ledger(
                semantic, "attempt_start", attempt_index=1,
                call_id="call-0", call_kind="hybridpatch_primary",
                attempt_kind="transport_initial",
                request_fingerprint=fingerprint)
            recorder._append_ledger(
                semantic, "attempt_end", attempt_index=1,
                call_id="call-0",
                **self._failed_attempt_end_fields(
                    generation_delta_seen=False,
                    error_type="connection_error"))
            recorder._append_ledger(
                semantic, "attempt_budget", attempt_index=1,
                call_id="call-0", budget_class="response_slot",
                response_slots_used=1, transient_failure_count=0)
            with self.assertRaisesRegex(
                    RuntimeError, "budget counters disagree"):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3",
                    call_kind="hybridpatch_primary")
            provider.assert_not_called()

    def test_request_fingerprint_includes_positional_generate_arguments(self):
        base = ([{"role": "user", "content": "one"}],)
        changed = ([{"role": "user", "content": "two"}],)
        kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        self.assertNotEqual(
            run_meta._semantic_request_fingerprint(
                base, kwargs, "minimax-m3", "hybridpatch_primary"),
            run_meta._semantic_request_fingerprint(
                changed, kwargs, "minimax-m3", "hybridpatch_primary"),
        )

    def test_future_generation_delta_and_signature_delta_restore_distinct_budgets(self):
        def state_after(delta_type):
            with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
            ):
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", mock.Mock(),
                )
                recorder.set_step(3, "forward", "target")
                messages = [{"role": "user", "content": "Hello"}]
                kwargs = {
                    "model": "minimax-m3",
                    "call_kind": "hybridpatch_primary",
                }
                fingerprint = run_meta._semantic_request_fingerprint(
                    (messages,), kwargs, "minimax-m3",
                    "hybridpatch_primary")
                _step, semantic = recorder._semantic_ids(
                    "hybridpatch_primary")
                recorder._append_ledger(
                    semantic, "semantic_request", call_id="failed-call",
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                recorder._append_ledger(
                    semantic, "attempt_start", attempt_index=1,
                    call_id="failed-call", call_kind="hybridpatch_primary",
                    attempt_kind="transport_initial",
                    request_fingerprint=fingerprint)
                generation_delta_seen = run_meta._generation_delta_type(
                    delta_type)
                if generation_delta_seen:
                    recorder._append_ledger(
                        semantic, "generation_progress", attempt_index=1,
                        call_id="failed-call", delta_type=delta_type)
                recorder._append_ledger(
                    semantic, "attempt_end", attempt_index=1,
                    call_id="failed-call",
                    **self._failed_attempt_end_fields(
                        generation_delta_seen=generation_delta_seen))
                recorder._append_ledger(
                    semantic, "attempt_budget", attempt_index=1,
                    call_id="failed-call",
                    budget_class=(
                        "response_slot" if generation_delta_seen
                        else "transient_failure"),
                    response_slots_used=int(generation_delta_seen),
                    transient_failure_count=int(not generation_delta_seen))
                return recorder._ledger_state(semantic)

        future = state_after("audio_delta")
        self.assertEqual(future["response_slots_used"], 1)
        self.assertEqual(future["transient_failure_count"], 0)
        signature = state_after("signature_delta")
        self.assertEqual(signature["response_slots_used"], 0)
        self.assertEqual(signature["transient_failure_count"], 1)

    def test_infrastructure_resume_keeps_attempts_and_uses_next_index(self):
        messages = [{"role": "user", "content": "Hello"}]
        kwargs = {"model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), kwargs, "minimax-m3", "hybridpatch_primary"
        )
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            prior = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3", mock.Mock()
            )
            prior.set_step(2, "backward", "target")
            _step, semantic = prior._semantic_ids("hybridpatch_primary")
            prior._append_ledger(
                semantic, "semantic_request", call_id="failed-call",
                call_kind="hybridpatch_primary", request_fingerprint=fingerprint,
            )
            for attempt_index in (1, 2):
                prior._append_ledger(
                    semantic, "attempt_start", attempt_index=attempt_index,
                    call_id="failed-call", call_kind="hybridpatch_primary",
                    attempt_kind=(
                        "transport_initial" if attempt_index == 1
                        else "transport_retry"),
                    request_fingerprint=fingerprint)
                prior._append_ledger(
                    semantic, "generation_progress",
                    attempt_index=attempt_index,
                    call_id="failed-call", delta_type="thinking_delta")
                prior._append_ledger(
                    semantic, "attempt_end", attempt_index=attempt_index,
                    call_id="failed-call",
                    **self._failed_attempt_end_fields())
                prior._append_ledger(
                    semantic, "attempt_budget", attempt_index=attempt_index,
                    call_id="failed-call", budget_class="response_slot",
                    response_slots_used=attempt_index,
                    transient_failure_count=0)
            prior._append_ledger(
                semantic, "call_failed", call_id="failed-call",
                status="provider_failure", error_type="incomplete_stream",
                response_slots_used=2, transient_failure_count=0,
                http_attempts_used=2, attempt_index=2,
                recovery_index=0, request_fingerprint=fingerprint,
            )
            observed = {}

            def resumed_generate(*_args, **inner):
                observed.update(inner["_retry_state"])
                next_index = observed["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start", "attempt_index": next_index,
                    "attempt_kind": "transport_recovery_initial",
                })
                inner["_raw_event_sink"]({
                    "record_type": "sdk_stream_event",
                    "attempt_index": next_index,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn", "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
                    "input_tokens": 5, "output_tokens": 1,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                    "transport_attempts": [{"attempt_index": next_index,
                                            "status": "success"}],
                    "call_kind": "hybridpatch_primary", "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3, "transient_failure_count": 0,
                    "http_attempts_used": next_index,
                }
                inner["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": next_index,
                        **self._successful_attempt_end_fields(),
                    },
                })
                inner["_response_commit_sink"](result)
                return result

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": semantic,
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "1",
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                    fingerprint),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": "3",
            }, clear=False):
                resumed = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", resumed_generate,
                )
                resumed.set_step(2, "backward", "target")
                out = resumed.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary",
                )
            self.assertEqual(observed["response_slots_used"], 0)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 2)
            self.assertEqual(observed["generation_index"], 1)
            self.assertEqual(observed["parent_semantic_call_id"], semantic)
            self.assertEqual(out["message"], "Hello")
            self.assertEqual(out["generation_index"], 1)
            self.assertEqual(out["parent_semantic_call_id"], semantic)
            root = semantic.rsplit("/", 1)[0]
            recovered_semantic = f"{root}/g001"
            self.assertEqual(out["semantic_call_id"], recovered_semantic)
            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl")
            )
            attempt_indices = [
                row.get("attempt_index") for row in ledger
                if row.get("event") == "attempt_start"
            ]
            self.assertEqual(attempt_indices, [1, 2, 3])
            self.assertNotIn(
                "transport_resume", [row.get("event") for row in ledger])
            exact_ids = {
                row["semantic_call_id"] for row in ledger
                if row.get("semantic_root_id") == root
            }
            self.assertEqual(exact_ids, {semantic, recovered_semantic})
            parent_state = resumed._ledger_state(semantic)
            recovered_state = resumed._ledger_state(recovered_semantic)
            self.assertEqual(parent_state["response_slots_used"], 2)
            self.assertEqual(parent_state["http_attempts_used"], 2)
            self.assertEqual(recovered_state["response_slots_used"], 1)
            self.assertEqual(recovered_state["http_attempts_used"], 3)
            self.assertEqual(
                recovered_state["parent_semantic_call_id"], semantic)

    def test_resume_authorization_mismatch_fails_before_provider(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")
        cases = (
            ("wrong-fingerprint", "3", "request fingerprint changed"),
            (fingerprint, "99", "next HTTP attempt"),
        )
        for authorized_fingerprint, next_attempt, expected in cases:
            with self.subTest(expected=expected), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ, {"OPENCODE_API_KEY": "unit-test-key"},
                        clear=False):
                seed = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", mock.Mock())
                seed.set_step(1, "forward", "target")
                _step, generation_zero = seed._semantic_ids(
                    "hybridpatch_primary")
                self._append_exhausted_response_generation(
                    seed, generation_zero, "failed-g000", fingerprint,
                    (1, 2))
                provider = mock.Mock(side_effect=AssertionError(
                    "provider must not be called"))
                with mock.patch.dict(os.environ, {
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                        generation_zero),
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "1",
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                        authorized_fingerprint),
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": (
                        next_attempt),
                }, clear=False):
                    recorder = run_meta.ApiCallRecorder(
                        out_dir, "hybridpatch", "sample", None,
                        "minimax-m3", provider)
                    recorder.set_step(1, "forward", "target")
                    with self.assertRaisesRegex(RuntimeError, expected):
                        recorder.generate(
                            messages, model="minimax-m3",
                            call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_exhausted_exact_call_refuses_duplicate_provider_post(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            seed = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", mock.Mock())
            seed.set_step(1, "forward", "target")
            _step, generation_zero = seed._semantic_ids(
                "hybridpatch_primary")
            self._append_exhausted_response_generation(
                seed, generation_zero, "failed-g000", fingerprint, (1, 2))

            provider = mock.Mock(side_effect=AssertionError(
                "provider must not be called"))
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", provider)
            recorder.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "previously exhausted|duplicate provider POST"):
                recorder.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
            provider.assert_not_called()

    def test_api_log_schema_v4_and_secret_redaction(self):
        secret = "unit-test-secret-key"
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {"OPENCODE_API_KEY": secret, "OPENCODE_TRANSPORT": "anthropic_sdk_v2"},
            clear=False,
        ):
            def fake_generate(*_args, **kwargs):
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_start", "attempt_index": 1,
                    "attempt_kind": "transport_initial",
                    "error": f"must redact {secret}",
                })
                kwargs["_raw_event_sink"]({
                    "record_type": "sdk_stream_event", "attempt_index": 1,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": 1,
                        **self._successful_attempt_end_fields(),
                    },
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn", "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
                    "input_tokens": 5, "output_tokens": 1,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                    "transport_attempts": [{"attempt_index": 1, "status": "success"}],
                    "call_kind": "key_probe", "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3", fake_generate
            )
            recorder.set_step(1, "forward", "target")
            out = recorder.generate(
                [{"role": "user", "content": "Hello"}], model="minimax-m3",
                call_kind="key_probe", thinking_mode="adaptive",
            )
            with open(os.path.join(out_dir, "api_calls.jsonl"), encoding="utf-8") as handle:
                record = json.loads(handle.readline())
            self.assertEqual(record["schema"], "anchorpatch.api_call/4")
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
            recorder.set_step(1, "forward", "target")
            with self.assertRaises(model_openai.OpenCodeTransportError):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}], model="minimax-m3",
                    max_tokens=None, thinking_mode="adaptive",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["schema"], "anchorpatch.api_call/4")
            self.assertEqual(record["transport_revision"], "opencode_anthropic_sdk/4")
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


class OpenCodeZenDeepSeekTests(unittest.TestCase):
    def _generate(self, outcomes, captures, **kwargs):
        env = {
            "OPENAI_API_KEY": "unit-test-key",
            "OPENAI_BASE_URL": "https://opencode.ai/zen/v1",
        }
        fake_cls = _official_client_factory(list(outcomes), captures)
        events = []
        with mock.patch.dict(os.environ, env, clear=False), \
                mock.patch.object(model_openai, "OpenAI", fake_cls), \
                mock.patch.object(model_openai.time, "sleep"):
            wrapper = model_openai.OpenAI_Model()
            result = wrapper.generate(
                [{"role": "user", "content": "Hello"}],
                model="deepseek-v4-flash",
                max_tokens=20000,
                reasoning_effort="high",
                return_metadata=True,
                _raw_event_sink=events.append,
                **kwargs,
            )
        return result, events

    def test_high_reasoning_is_sent_and_audited(self):
        captures = []
        result, events = self._generate(
            [_official_payload(content="Hello", finish_reason="stop")],
            captures,
        )
        constructor = captures[0]["_ctor"]
        request = captures[1]
        self.assertEqual(constructor["base_url"],
                         "https://opencode.ai/zen/v1")
        self.assertEqual(constructor["max_retries"], 0)
        self.assertEqual(request["model"], "deepseek-v4-flash")
        self.assertEqual(request["reasoning_effort"], "high")
        self.assertEqual(request["max_completion_tokens"], 20000)
        self.assertEqual(
            result["_raw_request_body"]["reasoning_effort"], "high")
        self.assertEqual(result["provider"], "opencode_zen")
        self.assertEqual(result["transport"], "openai_sdk_nonstream")
        self.assertEqual(
            result["transport_revision"], "opencode_openai_compatible/1")
        self.assertEqual(
            result["request_url"],
            "https://opencode.ai/zen/v1/chat/completions")
        self.assertEqual(result["reasoning_effort"], "high")
        self.assertEqual(result["http_attempts_used"], 1)
        self.assertEqual(result["retry_count"], 0)
        self.assertEqual(result["cost_currency"], "USD")
        self.assertAlmostEqual(result["total_usd"], 0.00000252)
        self.assertEqual(result["total_cny"], 0.0)
        self.assertEqual(
            [event["record_type"] for event in events],
            ["attempt_start", "attempt_end"],
        )

    def test_retryable_failure_is_bounded_and_recorded(self):
        captures = []
        result, events = self._generate(
            [
                model_openai._HTTPStatusError(429, "busy"),
                _official_payload(content="Hello", finish_reason="stop"),
            ],
            captures,
        )
        self.assertEqual(result["http_attempts_used"], 2)
        self.assertEqual(result["retry_count"], 1)
        self.assertEqual(result["rate_limit_wait_count"], 1)
        self.assertEqual(
            [item["status"] for item in result["transport_attempts"]],
            ["retryable_error", "success"],
        )
        self.assertEqual(
            [event["record_type"] for event in events],
            ["attempt_start", "attempt_end", "attempt_start", "attempt_end"],
        )

    def test_runtime_config_and_reasoning_validation(self):
        with mock.patch.dict(
            os.environ,
            {"OPENAI_BASE_URL": "https://opencode.ai/zen/v1"},
            clear=False,
        ):
            config = model_openai.model_runtime_config(
                "deepseek-v4-flash",
                max_tokens=20000,
                reasoning_effort="high",
            )
        self.assertEqual(config["provider"], "opencode_zen")
        self.assertEqual(
            config["transport_revision"], "opencode_openai_compatible/1")
        self.assertEqual(config["reasoning_effort"], "high")
        with self.assertRaises(ValueError):
            model_openai._effective_reasoning_effort("ultra")


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
            self.assertEqual(cfg["transport_revision"], "opencode_anthropic_sdk/4")


if __name__ == "__main__":
    unittest.main(verbosity=2)
