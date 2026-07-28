"""Zero-API regression tests for the OpenCode Go Anthropic transport."""

import argparse
import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import threading
import traceback
import unittest
from datetime import datetime
from unittest import mock

import httpx
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
import authorize_ledger_lock_recovery as ledger_recovery
import fr_baseline_dispatch
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
            row for row in transport_log
            if row["record_type"] == "attempt_start")
        self.assertEqual(start["call_kind"], "key_probe")
        self.assertEqual(start["semantic_call_kind"], "key_probe")
        self.assertEqual(start["attempt_kind"], "transport_initial")
        self.assertTrue(any(row["record_type"] == "attempt_end" for row in transport_log))

        repair_log = []
        repair_factory = _client_factory(
            _events(content=[{"type": "text", "text": "repair"}]),
            _message([{"type": "text", "text": "repair"}]), {})
        model_openai._call_opencode_anthropic_sdk(
            [{"role": "user", "content": "Repair"}], "minimax-m3",
            16, 1.0, 30, False, call_kind="hybridpatch_repair",
            raw_event_sink=repair_log.append, attempt_index=1,
            client_factory=repair_factory)
        repair_start = next(
            row for row in repair_log
            if row["record_type"] == "attempt_start")
        self.assertEqual(repair_start["call_kind"], "hybridpatch_repair")
        self.assertEqual(repair_start["attempt_kind"], "transport_initial")

        retry_log = []
        retry_factory = _client_factory(
            _events(content=[{"type": "text", "text": "retry"}]),
            _message([{"type": "text", "text": "retry"}]), {})
        model_openai._call_opencode_anthropic_sdk(
            [{"role": "user", "content": "Hello"}], "minimax-m3",
            16, 1.0, 30, False, call_kind="key_probe",
            raw_event_sink=retry_log.append, attempt_index=2,
            client_factory=retry_factory)
        retry_start = next(
            row for row in retry_log
            if row["record_type"] == "attempt_start")
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

    def test_blank_final_stop_reason_is_incomplete_stream(self):
        content = [{"type": "text", "text": "looks complete"}]
        for stop_reason in ("", " \t "):
            with self.subTest(stop_reason=repr(stop_reason)):
                factory = _client_factory(
                    _events(content=content),
                    _message(content, stop_reason=stop_reason), {})
                with self.assertRaises(
                        model_openai._IncompleteStreamError) as caught:
                    model_openai._call_opencode_anthropic_sdk(
                        [{"role": "user", "content": "Hello"}],
                        "minimax-m3", 16, 1.0, 30, False,
                        client_factory=factory)
                attempt = caught.exception._opencode_attempt
                self.assertEqual(attempt["stop_reason"], stop_reason)
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
            200, request=httpx.Request(
                "POST", "https://opencode.invalid/v1/messages")
        )
        exc = model_openai.anthropic.APIStatusError(
            "Streaming response failed", response=response,
            body={"type": "api_error",
                  "message": "Streaming response failed"},
        )
        self.assertEqual(model_openai._transport_status_code(exc), 200)
        self.assertTrue(model_openai._is_incomplete_stream_exception(exc))
        self.assertEqual(
            model_openai._transport_error_type(exc), "incomplete_stream")
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
                [{"role": "user", "content": "Hello"}],
                model="minimax-m3", return_metadata=True,
            )
        self.assertEqual(call.call_count, 2)
        self.assertEqual(out["response_slots_used"], 2)
        self.assertEqual(
            out["transport_attempts"][0]["error_type"],
            "incomplete_stream")

        stream_exc = model_openai.anthropic.APIStatusError(
            "Streaming response failed", response=response,
            body={"type": "api_error",
                  "message": "Streaming response failed"},
        )
        events = [
            {"type": "message_start"},
            {"type": "content_block_start", "index": 0,
             "content_block": {"type": "thinking", "thinking": ""}},
            {"type": "content_block_delta", "index": 0,
             "delta": {"type": "thinking_delta",
                       "thinking": "partial"}},
            stream_exc,
        ]
        factory = _client_factory(events, _message([]), {})
        with self.assertRaises(
                model_openai.anthropic.APIStatusError) as caught:
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
            request_fingerprint, attempt_indices, *,
            parent_semantic_call_id=None,
            call_kind="hybridpatch_primary"):
        generation = int(semantic_call_id.rsplit("/g", 1)[1])
        parent_field = (
            {"parent_semantic_call_id": parent_semantic_call_id}
            if parent_semantic_call_id is not None else {}
        )
        recorder._append_ledger(
            semantic_call_id, "semantic_request", call_id=call_id,
            call_kind=call_kind, request_fingerprint=request_fingerprint,
            **parent_field)

        for local_index, attempt_index in enumerate(attempt_indices, 1):
            recorder._append_ledger(
                semantic_call_id, "attempt_start",
                attempt_index=attempt_index, call_id=call_id,
                call_kind=call_kind,
                attempt_kind=(
                    "transport_initial"
                    if generation == 0 and local_index == 1
                    else "transport_retry"
                    if generation == 0
                    else "transport_recovery_initial"
                    if local_index == 1
                    else "transport_recovery_retry"),
                request_fingerprint=request_fingerprint, **parent_field)
            recorder._append_ledger(
                semantic_call_id, "generation_progress",
                attempt_index=attempt_index, call_id=call_id,
                delta_type="text_delta", **parent_field)
            recorder._append_ledger(
                semantic_call_id, "attempt_end",
                attempt_index=attempt_index, call_id=call_id,
                **self._failed_attempt_end_fields(), **parent_field)
            recorder._append_ledger(
                semantic_call_id, "attempt_budget",
                attempt_index=attempt_index, call_id=call_id,
                budget_class="response_slot",
                response_slots_used=local_index,
                transient_failure_count=0, **parent_field)
        recorder._append_ledger(
            semantic_call_id, "call_failed", call_id=call_id,
            status="provider_failure", error_type="incomplete_stream",
            response_slots_used=len(attempt_indices),
            transient_failure_count=0,
            http_attempts_used=attempt_indices[-1],
            attempt_index=attempt_indices[-1],
            request_fingerprint=request_fingerprint, **parent_field)

    def _append_successful_response_generation(
            self, recorder, semantic_call_id, call_id,
            request_fingerprint, attempt_index, *,
            parent_semantic_call_id=None,
            call_kind="hybridpatch_primary"):
        generation = int(semantic_call_id.rsplit("/g", 1)[1])
        parent_field = (
            {"parent_semantic_call_id": parent_semantic_call_id}
            if parent_semantic_call_id is not None else {}
        )
        recorder._append_ledger(
            semantic_call_id, "semantic_request", call_id=call_id,
            call_kind=call_kind, request_fingerprint=request_fingerprint,
            **parent_field)
        recorder._append_ledger(
            semantic_call_id, "attempt_start", call_id=call_id,
            attempt_index=attempt_index, call_kind=call_kind,
            attempt_kind=(
                "transport_initial" if generation == 0
                else "transport_recovery_initial"),
            request_fingerprint=request_fingerprint, **parent_field)
        recorder._append_ledger(
            semantic_call_id, "generation_progress", call_id=call_id,
            attempt_index=attempt_index, delta_type="text_delta",
            **parent_field)
        recorder._append_ledger(
            semantic_call_id, "attempt_end", call_id=call_id,
            attempt_index=attempt_index,
            **self._successful_attempt_end_fields(), **parent_field)
        recorder._append_ledger(
            semantic_call_id, "response_committed", call_id=call_id,
            attempt_index=attempt_index, response_slots_used=1,
            transient_failure_count=0, http_attempts_used=attempt_index,
            request_fingerprint=request_fingerprint, **parent_field)

    @staticmethod
    def _write_active_inspection_fixture(
            out_dir, *, sample="sample", method="hybridpatch",
            worker_id="worker-a"):
        plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
        utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": "1" * 40,
            "config": {
                "samples": [sample],
                "method_set": [method],
                "num_round_trips": 1,
            },
            "task_plans": {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                },
            },
        }
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "dispatch_log.jsonl"), {
                "event": "launch", "worker_launch_id": worker_id,
                "sample": sample, "pid": os.getpid(),
            })
        paired_dispatch._write_active_worker_set(
            out_dir, manifest, [{
                "worker_launch_id": worker_id, "sample": sample,
            }])
        return manifest

    def _write_infrastructure_fixture(
            self, out_dir, sample="sample-a", worker_id="worker-a",
            worker_pid=101, methods=None):
        methods = list(methods or ["hybridpatch", "fullrewrite"])
        step_id = f"hybridpatch/{sample}/rt01/forward"
        semantic_root = f"{step_id}/hybridpatch_primary"
        semantic = f"{semantic_root}/g000"
        request_id = f"failed-{sample}"
        fingerprint = f"fingerprint-{sample}"
        invocation = f"invocation-{sample}"
        api_row = {
            "schema": "anchorpatch.api_call/4",
            "sample": sample, "method": "hybridpatch",
            "rt_index": 1, "direction": "forward",
            "call_kind": "hybridpatch_primary",
            "step_id": step_id,
            "semantic_root_id": semantic_root,
            "semantic_call_id": semantic, "request_id": request_id,
            "generation_index": 0,
            "parent_semantic_call_id": None,
            "worker_launch_id": worker_id, "worker_pid": worker_pid,
            "provider_called": True, "response_replayed": False,
            "replayed_from_call_id": None,
            "transport_revision": "opencode_anthropic_sdk/4",
            "transport_resume_policy": (
                "exact_payload_new_semantic_call/1"),
            "transport_recovery_index": 0,
            "request_fingerprint": fingerprint,
            "classification": "provider/API failure",
            "count_as_method_failure": False,
            "error_type": "incomplete_stream",
            "max_response_slots": 2, "response_slots_used": 2,
            "max_transient_failures": 3,
            "transient_failure_count": 0,
            "http_attempts_used": 2,
        }
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "api_calls.jsonl"), api_row)
        for event, fields in (
            ("semantic_request", {
                "call_id": request_id, "call_kind": "hybridpatch_primary",
                "request_fingerprint": fingerprint,
            }),
            ("attempt_start", {
                "call_id": request_id, "attempt_index": 1,
                "call_kind": "hybridpatch_primary",
                "attempt_kind": "transport_initial",
                "request_fingerprint": fingerprint,
            }),
            ("generation_progress", {
                "call_id": request_id, "attempt_index": 1,
                "delta_type": "text_delta",
            }),
            ("attempt_end", {
                "call_id": request_id, "attempt_index": 1,
                **self._failed_attempt_end_fields(),
            }),
            ("attempt_budget", {
                "call_id": request_id, "attempt_index": 1,
                "budget_class": "response_slot",
                "response_slots_used": 1,
                "transient_failure_count": 0,
            }),
            ("attempt_start", {
                "call_id": request_id, "attempt_index": 2,
                "call_kind": "hybridpatch_primary",
                "attempt_kind": "transport_retry",
                "request_fingerprint": fingerprint,
            }),
            ("generation_progress", {
                "call_id": request_id, "attempt_index": 2,
                "delta_type": "text_delta",
            }),
            ("attempt_end", {
                "call_id": request_id, "attempt_index": 2,
                **self._failed_attempt_end_fields(),
            }),
            ("attempt_budget", {
                "call_id": request_id, "attempt_index": 2,
                "budget_class": "response_slot",
                "response_slots_used": 2,
                "transient_failure_count": 0,
            }),
            ("call_failed", {
                "call_id": request_id, "attempt_index": 2,
                "status": "provider_failure",
                "error_type": "incomplete_stream",
                "response_slots_used": 2,
                "transient_failure_count": 0,
                "http_attempts_used": 2,
                "request_fingerprint": fingerprint,
            }),
        ):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"), {
                    "schema": "anchorpatch.api_attempt/4",
                    "step_id": step_id,
                    "semantic_root_id": semantic_root,
                    "semantic_call_id": semantic,
                    "generation_index": 0,
                    "parent_semantic_call_id": None,
                    "worker_launch_id": worker_id,
                    "event": event, **fields,
                })
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "run_metadata.jsonl"), {
                "schema": "anchorpatch.run_metadata/3",
                "invocation_id": invocation,
                "worker_launch_id": worker_id, "worker_pid": worker_pid,
                "samples": [sample], "status": "infrastructure_incomplete",
                "transport_resume_authorization": None,
            })
        progress = {
            method: {"completed_round_trips": 0, "committed_rows": 0}
            for method in methods
        }
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "sample_outcomes.jsonl"), {
                "schema": "anchorpatch.sample_outcome/1",
                "created_at": "2026-07-18T00:00:00+08:00",
                "sample": sample, "status": "infrastructure_incomplete",
                "worker_launch_id": worker_id, "worker_pid": worker_pid,
                "invocation_id": invocation, "methods": methods,
                "method": "hybridpatch", "rt_index": 1,
                "direction": "forward",
                "call_kind": "hybridpatch_primary",
                "semantic_root_id": semantic_root,
                "semantic_call_id": semantic, "request_id": request_id,
                "generation_index": 0,
                "parent_semantic_call_id": None,
                "request_fingerprint": fingerprint,
                "error_type": "incomplete_stream",
                "classification": "provider/API failure",
                "response_slots_used": 2,
                "transient_failure_count": 0,
                "http_attempts_used": 2,
                "next_attempt_index": 3,
                "transport_recovery_index": 0,
                "checkpoint_progress": progress,
            })
        return {
            "semantic_root_id": semantic_root,
            "semantic_call_id": semantic,
            "generation_index": 0,
            "request_fingerprint": fingerprint,
            "next_attempt_index": 3,
            "progress": progress,
        }

    def _write_finished_fixture(self, out_dir, sample, methods):
        progress = {}
        for method in methods:
            method_dir = os.path.join(out_dir, method)
            os.makedirs(method_dir, exist_ok=True)
            rows = []
            for direction in ("forward", "backward"):
                rows.append({
                    "sample_id": sample, "method": method,
                    "round_trip_num": 1,
                    "round_trip_direction": direction,
                })
            with open(
                os.path.join(method_dir, f"{sample}.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")
            run_meta.write_json_atomic(
                os.path.join(method_dir, f"{sample}.ckpt.json"),
                {"completed_round_trips": 1})
            progress[method] = {
                "completed_round_trips": 1, "committed_rows": 2}
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "sample_outcomes.jsonl"), {
                "schema": "anchorpatch.sample_outcome/1",
                "created_at": "2026-07-18T00:00:01+08:00",
                "sample": sample, "status": "finished",
                "worker_launch_id": f"worker-{sample}",
                "worker_pid": 202,
                "invocation_id": f"invocation-{sample}",
                "methods": list(methods),
                "checkpoint_progress": progress,
            })

    @staticmethod
    def _api_call_fixture_row(*, root, exact_id, request_id, fingerprint,
                              worker_id, worker_pid, parent=None,
                              failure=False, http_attempts=1):
        method, sample, rt_segment, direction, call_kind = root.split("/")
        generation = int(exact_id.rsplit("/g", 1)[1])
        return {
            "schema": "anchorpatch.api_call/4",
            "sample": sample, "method": method,
            "rt_index": int(rt_segment[2:]), "direction": direction,
            "call_kind": call_kind,
            "step_id": "/".join(root.split("/")[:4]),
            "semantic_root_id": root,
            "semantic_call_id": exact_id,
            "generation_index": generation,
            "parent_semantic_call_id": parent,
            "request_id": request_id,
            "worker_launch_id": worker_id, "worker_pid": worker_pid,
            "provider_called": True, "response_replayed": False,
            "replayed_from_call_id": None,
            "transport_revision": "opencode_anthropic_sdk/4",
            "transport_resume_policy": "exact_payload_new_semantic_call/1",
            "transport_recovery_index": generation,
            "request_fingerprint": fingerprint,
            "classification": "provider/API failure" if failure else None,
            "count_as_method_failure": False,
            "error_type": "incomplete_stream" if failure else None,
            "max_response_slots": 2,
            "response_slots_used": 2 if failure else 1,
            "max_transient_failures": 3,
            "transient_failure_count": 0,
            "http_attempts_used": http_attempts,
        }

    @staticmethod
    def _write_success_journal(out_dir, *, root, exact_id, request_id,
                               fingerprint, parent=None, http_attempts=1):
        generation = int(exact_id.rsplit("/g", 1)[1])
        digest = hashlib.sha256(exact_id.encode("utf-8")).hexdigest()[:24]
        os.makedirs(os.path.join(out_dir, "api_journal"), exist_ok=True)
        run_meta.write_json_atomic(
            os.path.join(out_dir, "api_journal", f"{digest}.response.json"), {
                "schema": "anchorpatch.api_response_journal/4",
                "semantic_root_id": root,
                "semantic_call_id": exact_id,
                "generation_index": generation,
                "parent_semantic_call_id": parent,
                "call_id": request_id,
                "request_fingerprint": fingerprint,
                "result": {
                    "message": "complete",
                    "stream_complete": True,
                    "stop_reason": "end_turn",
                    "input_tokens": 5, "output_tokens": 1,
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "call_kind": root.split("/")[-1],
                    "semantic_root_id": root,
                    "semantic_call_id": exact_id,
                    "generation_index": generation,
                    "parent_semantic_call_id": parent,
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": http_attempts,
                },
            })

    @staticmethod
    def _write_completed_method_prefix(out_dir, sample, method,
                                       completed_round_trips):
        if completed_round_trips <= 0:
            return
        method_dir = os.path.join(out_dir, method)
        os.makedirs(method_dir, exist_ok=True)
        rows = []
        for rt_index in range(1, completed_round_trips + 1):
            for direction in ("forward", "backward"):
                rows.append({
                    "sample_id": sample, "method": method,
                    "round_trip_num": rt_index,
                    "round_trip_direction": direction,
                })
        with open(
            os.path.join(method_dir, f"{sample}.jsonl"),
            "w", encoding="utf-8",
        ) as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        run_meta.write_json_atomic(
            os.path.join(method_dir, f"{sample}.ckpt.json"),
            {"completed_round_trips": completed_round_trips},
        )

    def test_transport_exhaustion_isolates_one_worker_and_sibling_completes(self):
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_infrastructure_fixture(out_dir)
            failed_process = mock.Mock(pid=101)
            failed_process.poll.return_value = 7
            sibling_process = mock.Mock(pid=202)
            sibling_process.poll.return_value = None
            running = {
                "sample-a": {
                    "sample": "sample-a", "process": failed_process,
                    "log": mock.Mock(), "key_label": "KEY_01",
                    "worker_launch_id": "worker-a",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "target_round_trips": 1, "exit_recorded": False,
                },
                "sample-b": {
                    "sample": "sample-b", "process": sibling_process,
                    "log": mock.Mock(), "key_label": "KEY_02",
                    "worker_launch_id": "worker-b",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "target_round_trips": 1, "exit_recorded": False,
                },
            }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, "sample-a", running["sample-a"], 7,
                dispatch_log)
            self.assertEqual(disposition, "infrastructure_incomplete")
            self.assertNotIn("sample-a", running)
            self.assertIn("sample-b", running)
            self.assertIsNone(sibling_process.poll())

            sibling_process.poll.return_value = 0
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, "sample-b", running["sample-b"], 0,
                dispatch_log)
            self.assertEqual(disposition, "finished")
            self.assertEqual(running, {})
            exit_rows = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(
                [row["disposition"] for row in exit_rows],
                ["infrastructure_incomplete", "finished"])
            self.assertEqual(
                paired_dispatch.read_campaign_stop_conditions(out_dir), [])

    def test_evaluator_exception_is_sample_local_and_resume_never_reposts(self):
        class BrokenDomain:
            @staticmethod
            def evaluate_context(*_args):
                raise ValueError("broken sample evaluator")

        with self.assertRaises(
                experiment_runner.EvaluatorIncompleteError) as caught:
            experiment_runner._evaluate(
                BrokenDomain(), "sample-eval", {"a.txt": "generated"},
                {"context": ["a.txt"]}, ["a.txt"],
                method="hybridpatch", rt_index=1, direction="backward",
                target_state_id="basic_state",
            )
        self.assertEqual(caught.exception.error_type, "builtins.ValueError")

        sample = "sample-eval"
        worker_id = "worker-eval"
        invocation_id = "invocation-eval"
        methods = ["hybridpatch", "fullrewrite"]
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {"ANCHORPATCH_WORKER_LAUNCH_ID": worker_id},
                clear=False):
            sidecar = experiment_runner._record_evaluator_incomplete(
                out_dir, sample, methods, 1, invocation_id,
                caught.exception,
            )
            self.assertFalse(sidecar["result_committed_for_failed_step"])
            self.assertFalse(sidecar["score_imputed"])
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "run_metadata.jsonl"), {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": invocation_id,
                    "worker_launch_id": worker_id,
                    "worker_pid": os.getpid(),
                    "samples": [sample],
                    "status": "evaluator_incomplete",
                })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), {
                    "schema": "anchorpatch.api_call/4",
                    "sample": sample,
                    "method": "hybridpatch",
                    "rt_index": 1,
                    "direction": "backward",
                    "call_kind": "hybridpatch_primary",
                    "worker_launch_id": "prior-recovery-worker",
                    "classification": "provider/API failure",
                })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), {
                    "schema": "anchorpatch.api_call/4",
                    "sample": sample,
                    "method": "hybridpatch",
                    "rt_index": 1,
                    "direction": "backward",
                    "call_kind": "hybridpatch_primary",
                    "worker_launch_id": worker_id,
                    "classification": "success",
                })
            process = mock.Mock(pid=os.getpid())
            running = {
                sample: {
                    "sample": sample,
                    "process": process,
                    "log": mock.Mock(),
                    "key_label": "KEY_01",
                    "worker_launch_id": worker_id,
                    "methods": methods,
                    "target_round_trips": 1,
                    "exit_recorded": False,
                },
            }
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, sample, running[sample], 7,
                os.path.join(out_dir, "dispatch_log.jsonl"),
            )
            self.assertEqual(disposition, "evaluator_incomplete")
            self.assertEqual(running, {})

            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir,
                    [{"sample": sample, "methods": methods}],
                    resume=True, target_round_trips=1,
                ))
            self.assertEqual(selected, [])
            self.assertEqual(authorizations, {})
            provider_post = mock.Mock(
                side_effect=AssertionError(
                    "evaluator-incomplete sample must not be reposted"))
            for _item in selected:
                provider_post()
            provider_post.assert_not_called()
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "hybridpatch", f"{sample}.jsonl")))
            self.assertEqual(
                paired_dispatch.read_campaign_stop_conditions(out_dir), [])

    def test_runner_main_records_evaluator_terminal_status(self):
        failure = experiment_runner.EvaluatorIncompleteError(
            "sample", "hybridpatch", 1, "forward", "target",
            ValueError("broken evaluator"),
        )
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    sys, "argv", [
                        "experiment_runner.py", "--sample", "sample",
                        "--methods", "hybridpatch", "fullrewrite",
                        "--num_round_trips", "1", "--out_dir", out_dir,
                        "--model", "offline-test-model",
                    ]), \
                mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    experiment_runner, "_require_formal_dispatch_environment"), \
                mock.patch.object(
                    experiment_runner, "append_run_metadata",
                    return_value={"invocation_id": "invocation"}), \
                mock.patch.object(
                    experiment_runner, "_dispatch_worker_start_barrier"), \
                mock.patch.object(
                    experiment_runner, "run_relay", side_effect=failure), \
                mock.patch.object(
                    experiment_runner, "_record_evaluator_incomplete") as record, \
                mock.patch.object(
                    experiment_runner, "finish_run_metadata") as finish:
            with self.assertRaises(
                    experiment_runner.EvaluatorIncompleteError):
                experiment_runner.main()

        record.assert_called_once_with(
            out_dir, "sample", ["hybridpatch", "fullrewrite"], 1,
            "invocation", failure,
        )
        finish.assert_called_once_with(
            out_dir, "invocation", status="evaluator_incomplete")

    def test_launch_poll_isolates_exhausted_worker_and_keeps_sibling_running(self):
        class FakeProcess:
            def __init__(self, pid, poll_sequence):
                self.pid = pid
                self._poll_sequence = list(poll_sequence)
                self.returncode = None

            def poll(self):
                if self._poll_sequence:
                    self.returncode = self._poll_sequence.pop(0)
                return self.returncode

        def make_task_plans(out_dir, samples, *_args):
            plans = {}
            for sample in samples:
                plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="smoke",
                smoke_dir=None,
                samples=["sample-a", "sample-b"],
                key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env",
                num_round_trips=1,
                seed=42,
                dry_run=False,
                resume=False,
                start_timeout=0.1,
                poll_interval=0,
                progress_interval=9999,
                notes="unit",
            )
            active_snapshots = []
            real_write_active = paired_dispatch._write_active_worker_set

            def capture_active(active_out_dir, manifest, workers):
                record = real_write_active(active_out_dir, manifest, workers)
                active_snapshots.append(sorted(
                    item["sample"] for item in record["workers"].values()))
                return record

            processes = {}

            def fake_popen(command, **_kwargs):
                sample = command[command.index("--sample") + 1]
                process = (
                    FakeProcess(101, [7])
                    if sample == "sample-a"
                    else FakeProcess(202, [None, 0])
                )
                processes[sample] = process
                return process

            def fake_latest_sample_outcomes(
                    active_out_dir, *_args, **_kwargs):
                if "sample-a" not in processes:
                    return {}
                active = paired_dispatch._read_json(
                    paired_dispatch._active_worker_set_path(active_out_dir))
                worker_id = next(
                    (
                        candidate for candidate, payload
                        in (active.get("workers") or {}).items()
                        if payload.get("sample") == "sample-a"
                    ),
                    None,
                )
                return {
                    "sample-a": {
                        "status": "infrastructure_incomplete",
                        "worker_launch_id": worker_id,
                        "worker_pid": processes["sample-a"].pid,
                    }
                }

            with mock.patch.object(
                    paired_dispatch, "_validate_campaign_grid"), \
                    mock.patch.object(
                        paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={"errors": [], "api_calls": 2,
                                      "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_authorize_workers"), \
                    mock.patch.object(
                        paired_dispatch, "_verified_infrastructure_incomplete",
                        return_value={"semantic_call_id": "failed-call",
                                      "transport_recovery_index": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_latest_sample_outcomes",
                        side_effect=fake_latest_sample_outcomes), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen",
                        side_effect=fake_popen), \
                    mock.patch.object(
                        paired_dispatch.time, "sleep"), \
                    mock.patch.object(
                        paired_dispatch, "_write_active_worker_set",
                        side_effect=capture_active):
                result = paired_dispatch._launch_under_lease(args, out_dir)

            self.assertEqual(result, 2)
            self.assertEqual(set(processes), {"sample-a", "sample-b"})
            self.assertIn(["sample-a", "sample-b"], active_snapshots)
            self.assertIn(["sample-b"], active_snapshots)
            self.assertEqual(active_snapshots[-1], [])
            self.assertEqual(
                paired_dispatch.read_campaign_stop_conditions(out_dir), [])
            dispatch_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            self.assertEqual(
                [row.get("disposition") for row in dispatch_rows
                 if row.get("event") == "worker_exit"],
                ["infrastructure_incomplete", "finished"])
            self.assertEqual(
                dispatch_rows[-1]["event"], "campaign_incomplete")
            self.assertEqual(
                dispatch_rows[-1]["infrastructure_incomplete_samples"],
                ["sample-a"])

    def test_confirmation_queue_isolates_incomplete_and_preservation_stops(self):
        samples = ["sample-a", "sample-b", "sample-c"]
        lifecycle = []

        def make_task_plans(out_dir, sample_ids, *_args):
            lifecycle.append("all_plans")
            plans = {}
            for sample in sample_ids:
                path = os.path.join(out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(path),
                    "sha256": paired_dispatch._sha256(path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="confirmation", smoke_dir=None,
                samples=list(samples), key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env", num_round_trips=10, seed=42,
                slots_per_key=1, dry_run=False, resume=False,
                start_timeout=0.1, poll_interval=0,
                progress_interval=9999, notes="unit",
                selection_manifest="selection.json",
            )
            selection_record = {
                "experiment_id": os.path.basename(out_dir),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "HP_V8/analysis/selection.json",
                "sha256": "1" * 64,
                "artifact_sha256_preview": "2" * 64,
                "candidate_count": 85, "selected_count": 68,
                "reserve_count": 17, "seed": 42,
            }
            launched_queues = []

            def fake_queue(
                    _args, _out_dir, _manifest, _plans, _keys, assignments,
                    _authorizations, _dispatch_log, _running, incomplete,
                    _evaluator_incomplete, completed, _total, _slots):
                self.assertEqual(lifecycle, ["all_plans"])
                manifest = paired_dispatch._read_json(os.path.join(
                    out_dir, "dispatch_manifest.json"))
                self.assertEqual(set(manifest["task_plans"]), set(samples))
                queue_samples = [item["sample"] for item in assignments]
                launched_queues.append(queue_samples)
                incomplete.add("sample-a")
                completed.update({"sample-b", "sample-c"})

            with mock.patch.object(
                    paired_dispatch, "CONFIRMATION_SAMPLE_COUNT", 3), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_KEY_COUNT", 2), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_SLOTS_PER_KEY", 1), \
                    mock.patch.object(
                        paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={"errors": [], "api_calls": 3,
                                      "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(list(samples), selection_record)) as load_selection, \
                    mock.patch.object(
                        paired_dispatch, "_run_worker_queue",
                        side_effect=fake_queue):
                result = paired_dispatch._launch_under_lease(args, out_dir)
                load_selection.assert_called_once_with(
                    "selection.json", current_experiment_dir=out_dir)

            self.assertEqual(result, 2)
            self.assertEqual(
                launched_queues, [["sample-a", "sample-b", "sample-c"]])
            dispatch_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            self.assertFalse(any(
                row["event"].startswith("wave_") for row in dispatch_rows))

        lifecycle.clear()
        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="confirmation", smoke_dir=None,
                samples=list(samples), key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env", num_round_trips=10, seed=42,
                slots_per_key=1, dry_run=False, resume=False,
                start_timeout=0.1, poll_interval=0,
                progress_interval=9999, notes="unit",
                selection_manifest="selection.json",
            )
            selection_record = {
                "experiment_id": os.path.basename(out_dir),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "HP_V8/analysis/selection.json",
                "sha256": "1" * 64,
                "artifact_sha256_preview": "2" * 64,
                "candidate_count": 85, "selected_count": 68,
                "reserve_count": 17, "seed": 42,
            }
            stopped_queues = []

            def preservation_stop(
                    _args, wave_out_dir, _manifest, _plans, _keys,
                    assignments, *_rest):
                stopped_queues.append(
                    [item["sample"] for item in assignments])
                run_meta.record_campaign_stop_condition(
                    wave_out_dir, "preservation_violation",
                    preservation_violations=1)
                raise RuntimeError("preservation_violations=1")

            with mock.patch.object(
                    paired_dispatch, "CONFIRMATION_SAMPLE_COUNT", 3), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_KEY_COUNT", 2), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_SLOTS_PER_KEY", 1), \
                    mock.patch.object(
                        paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={"errors": [], "api_calls": 0,
                                      "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(list(samples), selection_record)) as load_selection, \
                    mock.patch.object(
                        paired_dispatch, "_run_worker_queue",
                        side_effect=preservation_stop):
                result = paired_dispatch._launch_under_lease(args, out_dir)
                load_selection.assert_called_once_with(
                    "selection.json", current_experiment_dir=out_dir)

            self.assertEqual(result, 1)
            self.assertEqual(
                stopped_queues, [["sample-a", "sample-b", "sample-c"]])
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0][
                    "condition"], "preservation_violation")

    def test_work_conserving_queue_refills_key_after_evaluator_incomplete(self):
        class FakeProcess:
            def __init__(self, pid, polls):
                self.pid = pid
                self.polls = list(polls)
                self.returncode = None

            def poll(self):
                if self.polls:
                    self.returncode = self.polls.pop(0)
                return self.returncode

        assignments = [
            {"sample": "sample-a", "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"]},
            {"sample": "sample-b", "key_label": "KEY_01",
             "methods": ["fullrewrite", "hybridpatch"]},
            {"sample": "sample-c", "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"]},
            {"sample": "sample-x", "key_label": "KEY_02",
             "methods": ["fullrewrite", "hybridpatch"]},
        ]
        polls = {
            "sample-a": [7],
            "sample-b": [None, 0],
            "sample-c": [0],
            "sample-x": [None, None, 0],
        }
        lifecycle = []

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            lifecycle.append({
                "event": "launch",
                "samples": [item["sample"] for item in batch],
                "already_running": sorted(running),
            })
            for index, item in enumerate(batch, 1):
                sample = item["sample"]
                running[sample] = {
                    **item,
                    "process": FakeProcess(index, polls[sample]),
                    "log": mock.Mock(),
                    "worker_launch_id": f"worker-{sample}",
                    "exit_recorded": False,
                }
            by_key = {}
            for item in running.values():
                by_key[item["key_label"]] = (
                    by_key.get(item["key_label"], 0) + 1)
            self.assertTrue(all(count <= 2 for count in by_key.values()))
            return [item["sample"] for item in batch]

        def fake_exit(_out_dir, running, sample, _item, returncode,
                      _dispatch_log):
            del running[sample]
            disposition = (
                "evaluator_incomplete"
                if sample == "sample-a" else "finished")
            self.assertEqual(returncode != 0, sample == "sample-a")
            lifecycle.append({"event": "exit", "sample": sample,
                              "disposition": disposition})
            return disposition

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        infrastructure_incomplete = set()
        evaluator_incomplete = set()
        completed = set()
        inspection_calls = []

        def fake_inspect(*_args, **kwargs):
            inspection_calls.append({
                "active_samples": set(kwargs.get("active_samples") or []),
                "required_complete_samples": set(
                    kwargs.get("required_complete_samples") or []),
            })
            return {"errors": [], "api_calls": 0,
                    "preservation_violations": 0}

        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "_record_worker_exit",
                    side_effect=fake_exit), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    side_effect=fake_inspect), \
                mock.patch.object(
                    paired_dispatch,
                    "_write_active_worker_set") as write_active, \
                mock.patch.object(paired_dispatch.time, "sleep"):
            paired_dispatch._run_worker_queue(
                args, out_dir, {"run_git_commit": "1" * 40}, {}, {},
                assignments, {}, os.path.join(out_dir, "dispatch_log.jsonl"),
                {}, infrastructure_incomplete, evaluator_incomplete,
                completed, len(assignments), 2,
            )

        launches = [row for row in lifecycle if row["event"] == "launch"]
        self.assertEqual(
            [row["samples"] for row in launches],
            [["sample-a", "sample-b", "sample-x"], ["sample-c"]],
        )
        self.assertIn("sample-b", launches[1]["already_running"])
        self.assertEqual(infrastructure_incomplete, set())
        self.assertEqual(evaluator_incomplete, {"sample-a"})
        self.assertEqual(completed, {"sample-b", "sample-c", "sample-x"})
        self.assertEqual(len(inspection_calls), 3)
        self.assertEqual(
            [call["required_complete_samples"]
             for call in inspection_calls],
            [set(), {"sample-b", "sample-c"}, {"sample-x"}],
        )
        self.assertEqual(write_active.call_count, 4)

    def test_operator_pause_reconciles_terminal_active_workers_by_truth(self):
        expected_progress = {
            "fullrewrite": {
                "completed_round_trips": 10,
                "committed_rows": 20,
            }
        }
        workers = {
            "worker-finished": {"sample": "sample-finished"},
            "worker-incomplete": {"sample": "sample-incomplete"},
        }
        launches = [
            {
                "event": "launch", "sample": "sample-finished",
                "key_label": "KEY_01", "worker_launch_id": "worker-finished",
                "pid": 101,
            },
            {
                "event": "launch", "sample": "sample-incomplete",
                "key_label": "KEY_02", "worker_launch_id": "worker-incomplete",
                "pid": 202,
            },
        ]
        metadata = [
            {
                "worker_launch_id": "worker-finished", "worker_pid": 101,
                "samples": ["sample-finished"], "methods": ["fullrewrite"],
                "method_phase": "fullrewrite", "status": "finished",
            },
            {
                "worker_launch_id": "worker-incomplete", "worker_pid": 202,
                "samples": ["sample-incomplete"], "methods": ["fullrewrite"],
                "method_phase": "fullrewrite",
                "status": "infrastructure_incomplete",
            },
        ]
        outcomes = [
            {
                "worker_launch_id": "worker-finished", "worker_pid": 101,
                "sample": "sample-finished", "status": "finished",
                "checkpoint_progress": expected_progress,
            },
            {
                "worker_launch_id": "worker-incomplete", "worker_pid": 202,
                "sample": "sample-incomplete",
                "status": "infrastructure_incomplete",
            },
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"num_round_trips": 10},
        }
        with tempfile.TemporaryDirectory() as out_dir:
            pathlib.Path(out_dir, "active_worker_set.json").write_text(
                json.dumps({"workers": workers}), encoding="utf-8")
            dispatch_path = pathlib.Path(out_dir, "dispatch_log.jsonl")
            dispatch_path.write_text(
                "".join(json.dumps(row) + "\n" for row in launches),
                encoding="utf-8",
            )
            with mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery, "read_run_metadata_snapshot",
                        return_value=metadata), \
                    mock.patch.object(
                        ledger_recovery, "read_sample_outcomes",
                        return_value=outcomes), \
                    mock.patch.object(
                        ledger_recovery, "_actual_sample_progress",
                        return_value=expected_progress), \
                    mock.patch.object(
                        ledger_recovery, "_verified_infrastructure_incomplete",
                        return_value={"semantic_call_id": "call"}), \
                    mock.patch.object(
                        ledger_recovery, "_write_active_worker_set") as write_active:
                reconciled = ledger_recovery._reconcile_terminal_active_workers(
                    out_dir, manifest)
            rows = ledger_recovery._read_jsonl(str(dispatch_path))

        exits = [row for row in rows if row.get("event") == "worker_exit"]
        self.assertEqual(len(exits), 2)
        by_sample = {row["sample"]: row for row in exits}
        self.assertEqual(by_sample["sample-finished"]["returncode"], 0)
        self.assertEqual(
            by_sample["sample-finished"]["disposition"], "finished")
        self.assertEqual(by_sample["sample-incomplete"]["returncode"], 1)
        self.assertEqual(
            by_sample["sample-incomplete"]["disposition"],
            "infrastructure_incomplete")
        self.assertTrue(all(
            row["exit_code_observed"] is False for row in exits))
        self.assertEqual(
            [item["sample"] for item in reconciled],
            ["sample-finished", "sample-incomplete"])
        write_active.assert_called_once_with(out_dir, manifest, [])

    def test_operator_pause_reconciles_only_explicit_interrupted_worker(self):
        worker_id = "worker-interrupted"
        sample = "sample-interrupted"
        launch = {
            "event": "launch", "sample": sample,
            "key_label": "KEY_01", "worker_launch_id": worker_id,
            "pid": 303,
        }
        metadata = [{
            "invocation_id": "invocation-interrupted",
            "worker_launch_id": worker_id, "worker_pid": 303,
            "samples": [sample], "methods": ["fullrewrite"],
            "method_phase": "fullrewrite", "status": "running",
        }]
        audit = [{
            "invocation_id": "invocation-interrupted",
            "worker_launch_id": worker_id, "worker_pid": 303,
            "sample": sample, "method_phase": "fullrewrite",
            "launch_recorded": True,
        }]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"num_round_trips": 10},
        }
        with tempfile.TemporaryDirectory() as out_dir:
            pathlib.Path(out_dir, "active_worker_set.json").write_text(
                json.dumps({"workers": {
                    worker_id: {"sample": sample}
                }}), encoding="utf-8")
            dispatch_path = pathlib.Path(out_dir, "dispatch_log.jsonl")
            dispatch_path.write_text(
                json.dumps(launch) + "\n", encoding="utf-8")
            with mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_audit_running_invocation_provenance",
                        return_value=audit), \
                    mock.patch.object(
                        ledger_recovery, "read_run_metadata_snapshot",
                        return_value=metadata), \
                    mock.patch.object(
                        ledger_recovery, "read_sample_outcomes",
                        return_value=[]), \
                    mock.patch.object(
                        ledger_recovery,
                        "interrupt_audited_running_invocations",
                        return_value=[{
                            "invocation_id": "invocation-interrupted",
                            "worker_launch_id": worker_id,
                            "worker_pid": 303, "samples": [sample],
                        }]) as interrupt, \
                    mock.patch.object(
                        ledger_recovery,
                        "_write_active_worker_set") as write_active:
                reconciled = (
                    ledger_recovery._reconcile_terminal_active_workers(
                        out_dir, manifest,
                        operator_interrupted_samples=[sample],
                        operator_pause_reason="stalled stream",
                    )
                )
            rows = ledger_recovery._read_jsonl(str(dispatch_path))

        interrupt.assert_called_once_with(
            out_dir, status="interrupted_before_audited_resume",
            audited=audit)
        self.assertEqual(reconciled, [{
            "sample": sample, "worker_launch_id": worker_id,
            "worker_pid": 303,
            "status": "interrupted_before_audited_resume",
        }])
        stale = [
            row for row in rows
            if row.get("event") == "stale_worker_reconciled"
        ]
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["sample"], sample)
        self.assertEqual(stale[0]["reason"], "stalled stream")
        self.assertFalse(any(
            row.get("event") == "worker_exit" for row in rows))
        write_active.assert_called_once_with(out_dir, manifest, [])

    def test_operator_pause_hash_binds_interrupted_open_attempt(self):
        worker_id = "worker-interrupted"
        semantic_call_id = (
            "fullrewrite/sample-interrupted/rt09/backward/"
            "fullrewrite_primary")
        rows = [
            {
                "event": "semantic_request",
                "worker_launch_id": worker_id,
                "semantic_call_id": semantic_call_id,
            },
            {
                "event": "attempt_start", "attempt_index": 1,
                "worker_launch_id": worker_id,
                "semantic_call_id": semantic_call_id,
            },
            {
                "event": "generation_progress",
                "delta_type": "thinking_delta", "attempt_index": 1,
                "worker_launch_id": worker_id,
                "semantic_call_id": semantic_call_id,
            },
        ]
        prior = {
            "recovered_worker_launch_ids": ["worker-prior"],
            "incident_attempt_rows": [],
            "operator_pause_reconciled_workers": [{
                "sample": "sample-interrupted",
                "worker_launch_id": worker_id,
                "worker_pid": 303,
                "status": "interrupted_before_audited_resume",
            }],
        }
        with tempfile.TemporaryDirectory() as out_dir:
            attempt_path = pathlib.Path(
                out_dir, "api_attempt_ledger.jsonl")
            attempt_path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            result = (
                ledger_recovery
                ._extend_operator_interrupted_attempt_evidence(
                    out_dir, prior, []))

        self.assertEqual(
            result["recovered_worker_launch_ids"],
            ["worker-prior", worker_id])
        self.assertEqual(
            result["operator_interrupted_attempt_row_count"], 3)
        self.assertEqual(
            [entry["row_number"]
             for entry in result["incident_attempt_rows"]],
            [1, 2, 3])
        self.assertTrue(all(
            entry["incident_kind"]
            == "dispatcher_interrupted_open_attempt"
            for entry in result["incident_attempt_rows"]))

    def test_confirmation_duplicate_key_value_fails_before_provider(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        key_values = {
            label: f"secret-value-{index}"
            for index, label in enumerate(labels)
        }
        key_values["KEY_02"] = key_values["KEY_01"]
        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="confirmation", smoke_dir=None,
                samples=[], key_labels=labels, keys_file="unused.env",
                num_round_trips=10, seed=42, slots_per_key=4,
                dry_run=False, resume=False, start_timeout=0.1,
                poll_interval=0, progress_interval=9999, notes="unit",
                selection_manifest="selection.json",
            )
            selection_record = {
                "experiment_id": os.path.basename(out_dir),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "HP_V8/analysis/selection.json",
                "sha256": "1" * 64,
                "artifact_sha256_preview": "2" * 64,
                "candidate_count": 85, "selected_count": 68,
                "reserve_count": 17, "seed": 42,
            }
            with mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(samples, selection_record)), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value=key_values), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=AssertionError(
                            "duplicate keys must fail before task plans")), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen",
                        side_effect=AssertionError(
                            "duplicate keys must fail before provider")):
                with self.assertRaisesRegex(RuntimeError, "physically unique"):
                    paired_dispatch._launch_under_lease(args, out_dir)

    def test_fresh_campaign_dry_run_writes_empty_active_set_and_passes(self):
        def make_task_plans(out_dir, samples, *_args):
            plans = {}
            for sample in samples:
                plan_path = os.path.join(
                    out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(
                    plan_path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="smoke", smoke_dir=None,
                samples=["sample-a", "sample-b"],
                key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env", num_round_trips=1, seed=42,
                dry_run=True, resume=False, notes="unit",
            )
            with mock.patch.object(
                    paired_dispatch, "_validate_campaign_grid"), \
                    mock.patch.object(
                        paired_dispatch,
                        "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}):
                result = paired_dispatch._launch_under_lease(args, out_dir)

            self.assertEqual(result, 0)
            active = paired_dispatch._read_json(
                paired_dispatch._active_worker_set_path(out_dir))
            self.assertEqual(active, {
                "schema": "anchorpatch.active_worker_set/1",
                "run_git_commit": "1" * 40,
                "dispatcher_pid": None,
                "dispatcher_instance_id": None,
                "workers": {},
            })
            manifest = paired_dispatch._read_json(os.path.join(
                out_dir, "dispatch_manifest.json"))
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest,
                    active_samples={"sample-a", "sample-b"})
            self.assertEqual(inspection["errors"], [])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "api_calls.jsonl")))

    def test_preservation_latch_keeps_failure_global_for_all_workers(self):
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_infrastructure_fixture(out_dir)
            run_meta.record_campaign_stop_condition(
                out_dir, "preservation_violation",
                preservation_violations=1)
            running = {}
            for sample, worker, pid in (
                    ("sample-a", "worker-a", 101),
                    ("sample-b", "worker-b", 202)):
                process = mock.Mock(pid=pid)
                process.poll.return_value = 7 if sample == "sample-a" else None
                running[sample] = {
                    "sample": sample, "process": process,
                    "log": mock.Mock(), "key_label": f"KEY_{pid}",
                    "worker_launch_id": worker,
                    "methods": ["hybridpatch", "fullrewrite"],
                    "target_round_trips": 1, "exit_recorded": False,
                }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with self.assertRaisesRegex(
                    RuntimeError, "campaign-wide stop latch"):
                paired_dispatch._record_worker_exit(
                    out_dir, running, "sample-a", running["sample-a"], 7,
                    dispatch_log)
            self.assertEqual(set(running), {"sample-a", "sample-b"})
            with mock.patch.object(
                    paired_dispatch, "_terminate_workers") as terminate, \
                    mock.patch.object(
                        paired_dispatch, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        paired_dispatch,
                        "_audit_running_invocation_provenance",
                        return_value=[]), \
                    mock.patch.object(
                        paired_dispatch,
                        "interrupt_audited_running_invocations",
                        return_value=[]):
                paired_dispatch._stop_and_reconcile_workers(out_dir, running)
            terminate.assert_called_once_with(running)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0][
                    "condition"], "preservation_violation")

    def test_run_relay_resume_skips_committed_round_trip_without_provider_post(self):
        class DummyDomain:
            samples_folder = None

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

        with tempfile.TemporaryDirectory() as out_dir:
            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            utils_relay_plan.save_relay_task_plan(
                os.path.join(out_dir, "sample.task_plan.json"),
                ["target"],
            )
            with open(
                os.path.join(method_dir, "sample.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for direction in ("forward", "backward"):
                    handle.write(json.dumps({
                        "sample_id": "sample",
                        "method": "hybridpatch",
                        "round_trip_num": 1,
                        "round_trip_direction": direction,
                    }) + "\n")
            run_meta.write_json_atomic(
                os.path.join(method_dir, "sample.ckpt.json"),
                {
                    "completed_round_trips": 1,
                    "current_context": {"a.txt": "already committed"},
                    "rid_chain": ["rid-forward", "rid-backward"],
                    "state_chain": ["initial", "target", "initial"],
                    "context_shuffle_random_state": None,
                },
            )
            provider_post = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            with mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        experiment_runner, "load_sample",
                        return_value=(sample, out_dir, states)), \
                    mock.patch.object(
                        experiment_runner, "get_domain",
                        return_value=DummyDomain()), \
                    mock.patch.object(
                        experiment_runner, "load_distractor_context",
                        return_value={}), \
                    mock.patch.object(
                        experiment_runner, "register_task_plan",
                        return_value={"sha256": "a" * 64,
                                      "round_trips": 1}), \
                    mock.patch.object(
                        experiment_runner, "_edit_step",
                        side_effect=AssertionError(
                            "committed RT must not be regenerated")) as edit_step:
                result_path = experiment_runner.run_relay(
                    "hybridpatch", "sample", num_round_trips=1,
                    include_distractor=True, out_dir=out_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=provider_post, printing=False,
                )

            self.assertEqual(
                result_path,
                os.path.join(method_dir, "sample.jsonl"))
            provider_post.assert_not_called()
            edit_step.assert_not_called()

    def test_interrupted_phase_prefix_resumes_without_reposting_committed_rt(self):
        class DummyDomain:
            samples_folder = None

        sample_id = "sample"
        method = "hybridpatch"
        plan_hash = "a" * 64
        states = {
            "initial": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{"target_state": "target",
                             "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{"target_state": "initial",
                             "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}
        with tempfile.TemporaryDirectory() as out_dir:
            method_dir = os.path.join(out_dir, method)
            os.makedirs(method_dir, exist_ok=True)
            utils_relay_plan.save_relay_task_plan(
                os.path.join(out_dir, "sample.task_plan.json"), ["target"])
            with open(
                    os.path.join(method_dir, "sample.jsonl"),
                    "w", encoding="utf-8") as handle:
                for direction in ("forward", "backward"):
                    handle.write(json.dumps({
                        "sample_id": sample_id,
                        "method": method,
                        "round_trip_num": 1,
                        "round_trip_direction": direction,
                    }) + "\n")
            run_meta.write_json_atomic(
                os.path.join(method_dir, "sample.ckpt.json"), {
                    "completed_round_trips": 1,
                    "current_context": {"a.txt": "already committed"},
                    "rid_chain": ["rid-forward", "rid-backward"],
                    "state_chain": ["initial", "target", "initial"],
                    "context_shuffle_random_state": None,
                })
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"), [{
                    "invocation_id": "inv-old",
                    "worker_launch_id": "worker-old",
                    "worker_pid": 101,
                    "samples": [sample_id],
                    "methods": [method],
                    "method_phase": method,
                    "status": "interrupted_before_audited_resume",
                    "task_plans": {sample_id: {
                        "sha256": plan_hash, "round_trips": 1}},
                }])
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            for row in (
                {"event": "launch_intent", "sample": sample_id,
                 "methods": [method], "method_phase": method,
                 "worker_launch_id": "worker-old"},
                {"event": "launch", "sample": sample_id,
                 "method_phase": method, "worker_launch_id": "worker-old",
                 "pid": 101},
                {"event": "stale_worker_reconciled", "sample": sample_id,
                 "worker_launch_id": "worker-old", "pid": 101,
                 "invocation_id": "inv-old"},
            ):
                run_meta.append_jsonl_locked(dispatch_log, row)
            assignment = {
                "sample": sample_id, "key_label": "KEY_01",
                "methods": [method], "method_phase": method,
            }
            selected, transport_authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, [assignment], resume=True,
                    target_round_trips=1,
                    allow_pristine_pending=True,
                    method_phase=method,
                    allow_audited_interrupted=True,
                    task_plans={sample_id: {"sha256": plan_hash}}))
            self.assertEqual(len(selected), 1)
            self.assertEqual(transport_authorizations, {})
            self.assertEqual(
                selected[0]["interrupted_resume_evidence"][
                    "checkpoint_progress"][method][
                        "completed_round_trips"],
                1)

            provider_post = mock.Mock(
                side_effect=AssertionError("committed RT was reposted"))
            with mock.patch.object(
                    experiment_runner,
                    "_require_formal_opencode_transport"), mock.patch.object(
                        experiment_runner, "load_sample",
                        return_value=(sample, out_dir, states)), mock.patch.object(
                            experiment_runner, "get_domain",
                            return_value=DummyDomain()), mock.patch.object(
                                experiment_runner,
                                "load_distractor_context",
                                return_value={}), mock.patch.object(
                                    experiment_runner,
                                    "register_task_plan",
                                    return_value={"sha256": plan_hash,
                                                  "round_trips": 1}), \
                    mock.patch.object(
                        experiment_runner, "_edit_step",
                        side_effect=AssertionError(
                            "committed RT must not be regenerated")):
                experiment_runner.run_relay(
                    method, sample_id, num_round_trips=1,
                    include_distractor=True, out_dir=out_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=provider_post, printing=False)
            provider_post.assert_not_called()

    def test_resume_selects_only_incomplete_sample_and_skips_committed_sample(self):
        methods_a = ["hybridpatch", "fullrewrite"]
        methods_b = ["fullrewrite", "hybridpatch"]
        with tempfile.TemporaryDirectory() as out_dir:
            evidence = self._write_infrastructure_fixture(
                out_dir, methods=methods_a)
            self._write_finished_fixture(out_dir, "sample-b", methods_b)
            assignments = [
                {"sample": "sample-a", "methods": methods_a},
                {"sample": "sample-b", "methods": methods_b},
            ]
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=1))
            self.assertEqual(
                [item["sample"] for item in selected], ["sample-a"])
            self.assertEqual(
                authorizations["sample-a"]["parent_semantic_call_id"],
                evidence["semantic_call_id"])
            self.assertEqual(
                authorizations["sample-a"]["semantic_root_id"],
                evidence["semantic_root_id"])
            self.assertEqual(
                authorizations["sample-a"]["generation_index"], 1)
            self.assertEqual(
                authorizations["sample-a"]["semantic_call_id"],
                f"{evidence['semantic_root_id']}/g001")
            self.assertEqual(
                authorizations["sample-a"]["next_attempt_index"], 3)
            provider_post = mock.Mock()
            for item in selected:
                if item["sample"] == "sample-b":
                    provider_post(item["sample"])
            provider_post.assert_not_called()

    def test_resume_real_g001_exhaustion_is_verified_then_g002_forbidden(self):
        sample = "sample-a"
        methods = ["hybridpatch", "fullrewrite"]
        worker_old, worker_new = "worker-old", "worker-new"
        old_pid, new_pid = 101, 202
        old_invocation, new_invocation = "inv-old", "inv-new"
        root = f"hybridpatch/{sample}/rt01/forward/hybridpatch_primary"
        g000, g001 = f"{root}/g000", f"{root}/g001"
        fingerprint = "fingerprint-recovery"
        progress = {
            method: {"completed_round_trips": 0, "committed_rows": 0}
            for method in methods
        }
        with tempfile.TemporaryDirectory() as out_dir:
            old_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            old_recorder.worker_launch_id = worker_old
            old_recorder.set_step(1, "forward", "target")
            self._append_exhausted_response_generation(
                old_recorder, g000, "failed-g000", fingerprint, (1, 2))
            new_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            new_recorder.worker_launch_id = worker_new
            new_recorder.set_step(1, "forward", "target")
            self._append_exhausted_response_generation(
                new_recorder, g001, "failed-g001", fingerprint, (3, 4),
                parent_semantic_call_id=g000)
            for row in (
                self._api_call_fixture_row(
                    root=root, exact_id=g000, request_id="failed-g000",
                    fingerprint=fingerprint, worker_id=worker_old,
                    worker_pid=old_pid, failure=True, http_attempts=2),
                self._api_call_fixture_row(
                    root=root, exact_id=g001, request_id="failed-g001",
                    fingerprint=fingerprint, worker_id=worker_new,
                    worker_pid=new_pid, parent=g000, failure=True,
                    http_attempts=4),
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            resume_authorization = {
                "parent_semantic_call_id": g000,
                "semantic_root_id": root,
                "semantic_call_id": g001,
                "generation_index": 1,
                "request_fingerprint": fingerprint,
                "next_attempt_index": 3,
            }
            for row in (
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": old_invocation,
                    "worker_launch_id": worker_old,
                    "worker_pid": old_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": None,
                },
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": new_invocation,
                    "worker_launch_id": worker_new,
                    "worker_pid": new_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": (
                        paired_dispatch._canonical_resume_authorization(
                            resume_authorization)),
                },
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), row)
            for created_at, worker, pid, invocation, exact_id, parent, \
                    request_id, generation, next_attempt in (
                        (
                            "2026-07-18T00:00:00+08:00",
                            worker_old, old_pid, old_invocation, g000, None,
                            "failed-g000", 0, 3,
                        ),
                        (
                            "2026-07-18T00:00:01+08:00",
                            worker_new, new_pid, new_invocation, g001, g000,
                            "failed-g001", 1, 5,
                        ),
                    ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "sample_outcomes.jsonl"), {
                        "schema": "anchorpatch.sample_outcome/1",
                        "created_at": created_at,
                        "sample": sample,
                        "status": "infrastructure_incomplete",
                        "worker_launch_id": worker,
                        "worker_pid": pid,
                        "invocation_id": invocation,
                        "methods": methods,
                        "method": "hybridpatch",
                        "rt_index": 1,
                        "direction": "forward",
                        "call_kind": "hybridpatch_primary",
                        "semantic_root_id": root,
                        "semantic_call_id": exact_id,
                        "request_id": request_id,
                        "generation_index": generation,
                        "parent_semantic_call_id": parent,
                        "request_fingerprint": fingerprint,
                        "error_type": "incomplete_stream",
                        "classification": "provider/API failure",
                        "response_slots_used": 2,
                        "transient_failure_count": 0,
                        "http_attempts_used": next_attempt - 1,
                        "next_attempt_index": next_attempt,
                        "transport_recovery_index": generation,
                        "checkpoint_progress": progress,
                    })

            evidence = paired_dispatch._verified_infrastructure_incomplete(
                out_dir, sample, {
                    "worker_launch_id": worker_new,
                    "worker_pid": new_pid,
                    "methods": methods,
                    "target_round_trips": 1,
                })
            self.assertEqual(evidence["generation_index"], 1)
            self.assertEqual(evidence["next_attempt_index"], 5)
            with self.assertRaisesRegex(RuntimeError, "g002\\+ is forbidden"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, [{"sample": sample, "methods": methods}],
                    resume=True, target_round_trips=1)

    def test_later_g000_exhaustion_allows_consumed_resume_metadata(self):
        sample = "sample-a"
        methods = ["hybridpatch", "fullrewrite"]
        worker_old, worker_new = "worker-old", "worker-new"
        old_pid, new_pid = 101, 202
        old_invocation, new_invocation = "inv-old", "inv-new"
        recovered_root = (
            f"hybridpatch/{sample}/rt01/forward/hybridpatch_primary")
        recovered_g000 = f"{recovered_root}/g000"
        recovered_g001 = f"{recovered_root}/g001"
        backward_root = (
            f"hybridpatch/{sample}/rt01/backward/hybridpatch_primary")
        backward_g000 = f"{backward_root}/g000"
        later_root = f"hybridpatch/{sample}/rt02/forward/hybridpatch_primary"
        later_g000 = f"{later_root}/g000"
        recovered_fp = "fingerprint-recovered"
        backward_fp = "fingerprint-backward"
        later_fp = "fingerprint-later"
        with tempfile.TemporaryDirectory() as out_dir:
            old_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            old_recorder.worker_launch_id = worker_old
            old_recorder.set_step(1, "forward", "target")
            self._append_exhausted_response_generation(
                old_recorder, recovered_g000, "failed-g000",
                recovered_fp, (1, 2))
            new_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            new_recorder.worker_launch_id = worker_new
            new_recorder.set_step(1, "forward", "target")
            self._append_successful_response_generation(
                new_recorder, recovered_g001, "recovered-g001",
                recovered_fp, 3, parent_semantic_call_id=recovered_g000)
            self._write_success_journal(
                out_dir, root=recovered_root, exact_id=recovered_g001,
                request_id="recovered-g001", fingerprint=recovered_fp,
                parent=recovered_g000, http_attempts=3)
            new_recorder.set_step(1, "backward", "target")
            self._append_successful_response_generation(
                new_recorder, backward_g000, "backward-g000",
                backward_fp, 1)
            self._write_success_journal(
                out_dir, root=backward_root, exact_id=backward_g000,
                request_id="backward-g000", fingerprint=backward_fp,
                http_attempts=1)
            new_recorder.set_step(2, "forward", "target-2")
            self._append_exhausted_response_generation(
                new_recorder, later_g000, "failed-later-g000",
                later_fp, (1, 2))
            for row in (
                self._api_call_fixture_row(
                    root=recovered_root, exact_id=recovered_g000,
                    request_id="failed-g000", fingerprint=recovered_fp,
                    worker_id=worker_old, worker_pid=old_pid,
                    failure=True, http_attempts=2),
                self._api_call_fixture_row(
                    root=recovered_root, exact_id=recovered_g001,
                    request_id="recovered-g001", fingerprint=recovered_fp,
                    worker_id=worker_new, worker_pid=new_pid,
                    parent=recovered_g000, http_attempts=3),
                self._api_call_fixture_row(
                    root=backward_root, exact_id=backward_g000,
                    request_id="backward-g000", fingerprint=backward_fp,
                    worker_id=worker_new, worker_pid=new_pid,
                    http_attempts=1),
                self._api_call_fixture_row(
                    root=later_root, exact_id=later_g000,
                    request_id="failed-later-g000", fingerprint=later_fp,
                    worker_id=worker_new, worker_pid=new_pid,
                    failure=True, http_attempts=2),
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            resume_authorization = {
                "parent_semantic_call_id": recovered_g000,
                "semantic_root_id": recovered_root,
                "semantic_call_id": recovered_g001,
                "generation_index": 1,
                "request_fingerprint": recovered_fp,
                "next_attempt_index": 3,
            }
            for row in (
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": old_invocation,
                    "worker_launch_id": worker_old,
                    "worker_pid": old_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": None,
                },
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": new_invocation,
                    "worker_launch_id": worker_new,
                    "worker_pid": new_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": (
                        paired_dispatch._canonical_resume_authorization(
                            resume_authorization)),
                },
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), row)
            self._write_completed_method_prefix(
                out_dir, sample, "hybridpatch", 1)
            progress = {
                "hybridpatch": {
                    "completed_round_trips": 1,
                    "committed_rows": 2,
                },
                "fullrewrite": {
                    "completed_round_trips": 0,
                    "committed_rows": 0,
                },
            }
            for created_at, worker, pid, invocation, root, exact_id, \
                    request_id, fingerprint, rt, next_attempt in (
                        (
                            "2026-07-18T00:00:00+08:00",
                            worker_old, old_pid, old_invocation,
                            recovered_root, recovered_g000, "failed-g000",
                            recovered_fp, 1, 3,
                        ),
                        (
                            "2026-07-18T00:00:02+08:00",
                            worker_new, new_pid, new_invocation,
                            later_root, later_g000, "failed-later-g000",
                            later_fp, 2, 3,
                        ),
                    ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "sample_outcomes.jsonl"), {
                        "schema": "anchorpatch.sample_outcome/1",
                        "created_at": created_at,
                        "sample": sample,
                        "status": "infrastructure_incomplete",
                        "worker_launch_id": worker,
                        "worker_pid": pid,
                        "invocation_id": invocation,
                        "methods": methods,
                        "method": "hybridpatch",
                        "rt_index": rt,
                        "direction": "forward",
                        "call_kind": "hybridpatch_primary",
                        "semantic_root_id": root,
                        "semantic_call_id": exact_id,
                        "request_id": request_id,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "request_fingerprint": fingerprint,
                        "error_type": "incomplete_stream",
                        "classification": "provider/API failure",
                        "response_slots_used": 2,
                        "transient_failure_count": 0,
                        "http_attempts_used": next_attempt - 1,
                        "next_attempt_index": next_attempt,
                        "transport_recovery_index": 0,
                        "checkpoint_progress": (
                            {
                                method: {
                                    "completed_round_trips": 0,
                                    "committed_rows": 0,
                                }
                                for method in methods
                            }
                            if worker == worker_old else progress
                        ),
                    })

            process = mock.Mock(pid=new_pid)
            process.poll.return_value = 7
            sibling_process = mock.Mock(pid=303)
            sibling_process.poll.return_value = None
            running = {
                sample: {
                    "sample": sample,
                    "process": process,
                    "log": mock.Mock(),
                    "key_label": "KEY_01",
                    "worker_launch_id": worker_new,
                    "methods": methods,
                    "target_round_trips": 2,
                    "exit_recorded": False,
                },
                "sample-b": {
                    "sample": "sample-b",
                    "process": sibling_process,
                    "log": mock.Mock(),
                    "key_label": "KEY_02",
                    "worker_launch_id": "worker-sibling",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "target_round_trips": 2,
                    "exit_recorded": False,
                },
            }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, sample, running[sample], 7, dispatch_log)
            self.assertEqual(disposition, "infrastructure_incomplete")
            self.assertNotIn(sample, running)
            self.assertIn("sample-b", running)
            provider_post = mock.Mock(
                side_effect=AssertionError(
                    "sibling/committed sample must not be posted here"))
            for item in running.values():
                if item["sample"] == sample:
                    provider_post(item["sample"])
            provider_post.assert_not_called()

            sibling_methods = ["fullrewrite", "hybridpatch"]
            sibling_progress = {}
            for method in sibling_methods:
                self._write_completed_method_prefix(
                    out_dir, "sample-b", method, 2)
                sibling_progress[method] = {
                    "completed_round_trips": 2,
                    "committed_rows": 4,
                }
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "sample_outcomes.jsonl"), {
                    "schema": "anchorpatch.sample_outcome/1",
                    "created_at": "2026-07-18T00:00:03+08:00",
                    "sample": "sample-b",
                    "status": "finished",
                    "worker_launch_id": "worker-sibling",
                    "worker_pid": 303,
                    "invocation_id": "inv-sibling",
                    "methods": sibling_methods,
                    "checkpoint_progress": sibling_progress,
                })
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir,
                    [
                        {"sample": sample, "methods": methods},
                        {
                            "sample": "sample-b",
                            "methods": sibling_methods,
                        },
                    ],
                    resume=True, target_round_trips=2))
            self.assertEqual([item["sample"] for item in selected], [sample])
            self.assertEqual(
                authorizations[sample]["parent_semantic_call_id"],
                later_g000)

    def test_confirmation_resume_selects_pristine_unstarted_wave_only(self):
        finished_methods = ["hybridpatch", "fullrewrite"]
        pending_methods = ["fullrewrite", "hybridpatch"]
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_finished_fixture(
                out_dir, "sample-finished", finished_methods)
            utils_relay_plan.save_relay_task_plan(
                os.path.join(out_dir, "sample-pending.task_plan.json"),
                ["target"],
            )
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"), {
                    "event": "wave_start", "wave_index": 2,
                    "samples": ["sample-pending"],
                })
            lease_path = paired_dispatch._worker_lease_path(
                out_dir, "sample-pending")
            os.makedirs(os.path.dirname(lease_path), exist_ok=True)
            open(lease_path, "a", encoding="utf-8").close()
            assignments = [
                {"sample": "sample-finished", "methods": finished_methods},
                {"sample": "sample-pending", "methods": pending_methods},
            ]
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=1,
                    allow_pristine_pending=True,
                ))
            self.assertEqual(
                [item["sample"] for item in selected], ["sample-pending"])
            self.assertEqual(authorizations, {})

            provider_post = mock.Mock(
                side_effect=AssertionError(
                    "a committed sample must never produce a provider POST"))
            for item in selected:
                if item["sample"] == "sample-finished":
                    provider_post(item["sample"])
            provider_post.assert_not_called()

            with self.assertRaisesRegex(RuntimeError, "missing outcomes"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=1,
                )

    def test_confirmation_resume_rejects_pending_with_execution_evidence(self):
        assignments = [{
            "sample": "sample-pending",
            "methods": ["hybridpatch", "fullrewrite"],
        }]

        def write_result(out_dir):
            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            open(
                os.path.join(method_dir, "sample-pending.jsonl"),
                "w", encoding="utf-8",
            ).close()

        def write_metadata(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "run_metadata.jsonl"),
                {"samples": ["sample-pending"], "status": "running"},
            )

        def write_api_call(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"),
                {"sample": "sample-pending", "provider_called": True},
            )

        def write_attempt(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                {
                    "semantic_call_id": (
                        "hybridpatch/sample-pending/rt01/forward/"
                        "hybridpatch_primary/g000"
                    ),
                    "event": "attempt_start",
                },
            )

        def write_dispatch_launch(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"),
                {"event": "launch", "sample": "sample-pending"},
            )

        def write_journal(out_dir):
            semantic = (
                "hybridpatch/sample-pending/rt01/forward/"
                "hybridpatch_primary/g000"
            )
            digest = hashlib.sha256(semantic.encode("utf-8")).hexdigest()[:24]
            journal_dir = os.path.join(out_dir, "api_journal")
            os.makedirs(journal_dir, exist_ok=True)
            run_meta.write_json_atomic(
                os.path.join(journal_dir, f"{digest}.response.json"), {
                    "schema": paired_dispatch.API_RESPONSE_JOURNAL_SCHEMA,
                    "semantic_call_id": semantic,
                })

        writers = {
            "result": write_result,
            "metadata": write_metadata,
            "api_call": write_api_call,
            "attempt": write_attempt,
            "dispatch_launch": write_dispatch_launch,
            "journal": write_journal,
        }
        for label, writer in writers.items():
            with self.subTest(evidence=label), \
                    tempfile.TemporaryDirectory() as out_dir:
                writer(out_dir)
                with self.assertRaisesRegex(RuntimeError, "never started"):
                    paired_dispatch._select_invocation_assignments(
                        out_dir, assignments, resume=True,
                        target_round_trips=10,
                        allow_pristine_pending=True,
                    )

    def test_resume_rejects_g002_after_g001_exhaustion(self):
        assignments = [{
            "sample": "sample-a",
            "methods": ["hybridpatch", "fullrewrite"],
        }]
        evidence = {
            "generation_index": 1,
            "semantic_call_id": (
                "hybridpatch/sample-a/rt01/forward/"
                "hybridpatch_primary/g001"
            ),
            "semantic_root_id": (
                "hybridpatch/sample-a/rt01/forward/"
                "hybridpatch_primary"
            ),
            "request_fingerprint": "fingerprint",
            "next_attempt_index": 4,
            "invocation_id": "invocation",
        }
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_latest_sample_outcomes",
                    return_value={
                        "sample-a": {"status": "infrastructure_incomplete"}
                    }), \
                mock.patch.object(
                    paired_dispatch, "_verified_infrastructure_incomplete",
                    return_value=evidence):
            with self.assertRaisesRegex(
                    RuntimeError, "g002\\+ is forbidden"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=10)

    def test_paired_dispatch_preflight_allows_missing_new_checkpoints(self):
        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
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
                        "sha256": paired_dispatch._sha256(plan_path),
                        "forward_state_sequence": ["state_a"],
                    },
                },
            }
            with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean"),
            ):
                paired_dispatch._write_active_worker_set(
                    out_dir, manifest, [])
                preflight = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                completion = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)

            self.assertEqual(preflight["errors"], [])
            self.assertTrue(any(
                "incomplete task" in error
                for error in completion["errors"]
            ))

    def test_dispatch_inspection_allows_only_proven_active_open_attempt(self):
        def build_fixture(out_dir, *, ledger_worker="worker-a",
                          active_worker="worker-a", include_launch=True):
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": ["sample"],
                    "method_set": ["hybridpatch"],
                    "num_round_trips": 1,
                },
                "task_plans": {
                    "sample": {
                        "path": "sample.task_plan.json",
                        "sha256": paired_dispatch._sha256(plan_path),
                        "forward_state_sequence": ["state_a"],
                    },
                },
            }
            paired_dispatch.write_json_atomic(
                paired_dispatch._active_worker_set_path(out_dir), {
                    "schema": "anchorpatch.active_worker_set/1",
                    "run_git_commit": "1" * 40,
                    "workers": {
                        active_worker: {"sample": "sample"},
                    },
                })
            if include_launch:
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "dispatch_log.jsonl"), {
                        "event": "launch",
                        "worker_launch_id": "worker-a",
                        "sample": "sample", "pid": 101,
                    })
            semantic_root = (
                "hybridpatch/sample/rt01/forward/hybridpatch_primary")
            semantic = f"{semantic_root}/g000"
            for event, fields in (
                ("semantic_request", {
                    "call_id": "live-call",
                    "call_kind": "hybridpatch_primary",
                    "request_fingerprint": "live-fingerprint",
                }),
                ("attempt_start", {
                    "call_id": "live-call", "attempt_index": 1,
                    "call_kind": "hybridpatch_primary",
                    "attempt_kind": "transport_initial",
                    "request_fingerprint": "live-fingerprint",
                }),
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"), {
                        "schema": "anchorpatch.api_attempt/4",
                        "step_id": "hybridpatch/sample/rt01/forward",
                        "semantic_root_id": semantic_root,
                        "semantic_call_id": semantic,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "worker_launch_id": ledger_worker,
                        "event": event,
                        **fields,
                    })
            return manifest

        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")):
            with tempfile.TemporaryDirectory() as out_dir:
                manifest = build_fixture(out_dir)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                self.assertEqual(inspection["errors"], [])

            with tempfile.TemporaryDirectory() as out_dir:
                manifest = build_fixture(out_dir)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples=set())
                self.assertTrue(any(
                    "unclosed HTTP attempt" in error
                    for error in inspection["errors"]), inspection["errors"])

            with tempfile.TemporaryDirectory() as out_dir:
                manifest = build_fixture(
                    out_dir, ledger_worker="worker-b",
                    active_worker="worker-b")
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                self.assertTrue(any(
                    "attempt worker provenance mismatch" in error
                    for error in inspection["errors"]), inspection["errors"])

    def test_failure_terminal_ledger_precedes_api_row_during_active_poll(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {
                "OPENCODE_API_KEY": "unit-test-key",
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
            }, clear=False,
        ):
            manifest = self._write_active_inspection_fixture(out_dir)

            def exhausted_generate(*_args, **kwargs):
                attempts = []
                for attempt_index in (1, 2):
                    kwargs["_raw_event_sink"]({
                        "record_type": "attempt_start",
                        "attempt_index": attempt_index,
                        "attempt_kind": (
                            "transport_initial" if attempt_index == 1
                            else "transport_retry"),
                    })
                    kwargs["_raw_event_sink"]({
                        "record_type": "sdk_stream_event",
                        "attempt_index": attempt_index,
                        "event": {"type": "content_block_delta",
                                  "delta": {"type": "text_delta"}},
                    })
                    attempt = {
                        "attempt_index": attempt_index,
                        **self._failed_attempt_end_fields(),
                    }
                    kwargs["_raw_event_sink"]({
                        "record_type": "attempt_end",
                        "attempt": attempt,
                    })
                    kwargs["_raw_event_sink"]({
                        "record_type": "attempt_budget",
                        "attempt_index": attempt_index,
                        "budget_class": "response_slot",
                        "response_slots_used": attempt_index,
                        "transient_failure_count": 0,
                    })
                    attempts.append(attempt)
                raise model_openai.OpenCodeTransportError(
                    "response slots exhausted", attempts=attempts,
                    last_error=model_openai._IncompleteStreamError(
                        "incomplete_stream"))

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", exhausted_generate)
            recorder.set_step(1, "forward", "target")
            original_write = recorder._write_record
            write_entered = threading.Event()
            allow_write = threading.Event()
            worker_errors = []

            def blocked_write(record):
                write_entered.set()
                if not allow_write.wait(5):
                    raise RuntimeError("unit-test write barrier timed out")
                original_write(record)

            recorder._write_record = blocked_write

            def run_failure():
                try:
                    with mock.patch.object(
                            run_meta, "_enforce_pre_call_campaign_guards"):
                        recorder.generate(
                            [{"role": "user", "content": "Hello"}],
                            model="minimax-m3",
                            call_kind="hybridpatch_primary")
                except BaseException as exc:
                    worker_errors.append(exc)

            worker = threading.Thread(target=run_failure, daemon=True)
            worker.start()
            self.assertTrue(write_entered.wait(5), worker_errors)
            try:
                ledger = run_meta._read_jsonl_records_with_retry(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"))
                self.assertEqual(ledger[-1]["event"], "call_failed")
                self.assertFalse(os.path.exists(
                    os.path.join(out_dir, "api_calls.jsonl")))
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")):
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, active_samples={"sample"})
                self.assertEqual(inspection["errors"], [])
            finally:
                allow_write.set()
                worker.join(5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(worker_errors), 1)
            self.assertIsInstance(
                worker_errors[0], model_openai.OpenCodeTransportError)
            api_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl"))
            self.assertEqual(len(api_rows), 1)
            self.assertEqual(
                api_rows[0]["classification"], "provider/API failure")

    def test_success_terminal_holds_root_lock_through_api_row_and_active_poll(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {
                "OPENCODE_API_KEY": "unit-test-key",
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
            }, clear=False,
        ):
            manifest = self._write_active_inspection_fixture(out_dir)

            def successful_generate(*_args, **kwargs):
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
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": 1, "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", successful_generate)
            recorder.set_step(1, "forward", "target")
            _step, semantic_root = recorder._semantic_root(
                "hybridpatch_primary")
            lock_path = os.path.join(
                out_dir, "api_semantic_locks",
                f"{recorder._semantic_digest(semantic_root)}.lock")
            original_write = recorder._write_record
            write_entered = threading.Event()
            allow_write = threading.Event()
            worker_results = []
            worker_errors = []

            def blocked_write(record):
                write_entered.set()
                if not allow_write.wait(5):
                    raise RuntimeError("unit-test write barrier timed out")
                original_write(record)

            recorder._write_record = blocked_write

            def run_success():
                try:
                    with mock.patch.object(
                            run_meta, "_enforce_pre_call_campaign_guards"):
                        worker_results.append(recorder.generate(
                            [{"role": "user", "content": "Hello"}],
                            model="minimax-m3",
                            call_kind="hybridpatch_primary"))
                except BaseException as exc:
                    worker_errors.append(exc)

            worker = threading.Thread(target=run_success, daemon=True)
            worker.start()
            self.assertTrue(write_entered.wait(5), worker_errors)
            contender = open(lock_path, "a+", encoding="utf-8")
            try:
                with self.assertRaises(portalocker.exceptions.LockException):
                    portalocker.lock(
                        contender,
                        portalocker.LOCK_EX | portalocker.LOCK_NB)
                ledger = run_meta._read_jsonl_records_with_retry(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"))
                self.assertEqual(ledger[-1]["event"], "response_committed")
                self.assertFalse(os.path.exists(
                    os.path.join(out_dir, "api_calls.jsonl")))
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")):
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, active_samples={"sample"})
                self.assertEqual(inspection["errors"], [])
            finally:
                allow_write.set()
                worker.join(5)
                contender.close()
            self.assertFalse(worker.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertEqual(len(worker_results), 1)
            with open(lock_path, "a+", encoding="utf-8") as post_commit:
                portalocker.lock(
                    post_commit,
                    portalocker.LOCK_EX | portalocker.LOCK_NB)
                portalocker.unlock(post_commit)
            api_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl"))
            self.assertEqual(len(api_rows), 1)
            self.assertIsNone(api_rows[0]["classification"])

    def test_live_inspection_avoids_cross_file_torn_snapshot(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {
                "OPENCODE_API_KEY": "unit-test-key",
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
            }, clear=False,
        ):
            manifest = self._write_active_inspection_fixture(
                out_dir, method="fullrewrite")

            def successful_generate(*_args, **kwargs):
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
                result = {
                    "message": "complete", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": 1, "status": "success"}],
                    "call_kind": "fullrewrite_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", successful_generate)
            api_snapshot_taken = threading.Event()
            writer_done = threading.Event()
            inspection_results = []
            inspection_errors = []
            real_read_jsonl = paired_dispatch._read_jsonl

            def blocked_read_jsonl(path):
                rows = real_read_jsonl(path)
                if (os.path.basename(path) == "api_calls.jsonl"
                        and not api_snapshot_taken.is_set()):
                    api_snapshot_taken.set()
                    if not writer_done.wait(5):
                        raise RuntimeError(
                            "unit-test publication barrier timed out")
                return rows

            def inspect_live_campaign():
                try:
                    inspection_results.append(
                        paired_dispatch.inspect_campaign(
                            out_dir, manifest,
                            active_samples={"sample"}))
                except BaseException as exc:
                    inspection_errors.append(exc)

            with mock.patch.object(
                    run_meta, "_enforce_pre_call_campaign_guards"), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=blocked_read_jsonl):
                inspector = threading.Thread(
                    target=inspect_live_campaign, daemon=True)
                inspector.start()
                self.assertTrue(api_snapshot_taken.wait(5))
                try:
                    for direction in ("forward", "backward"):
                        recorder.set_step(1, direction, "target")
                        recorder.generate(
                            [{"role": "user", "content": "Hello"}],
                            model="minimax-m3",
                            call_kind="fullrewrite_primary")
                    rows = [
                        {
                            "sample_id": "sample",
                            "method": "fullrewrite",
                            "round_trip_num": 1,
                            "round_trip_direction": "forward",
                            "evaluation": {},
                        },
                        {
                            "sample_id": "sample",
                            "method": "fullrewrite",
                            "round_trip_num": 1,
                            "round_trip_direction": "backward",
                            "evaluation": {"score": 1.0},
                        },
                    ]
                    commit = run_meta.append_relay_rows_and_checkpoint(
                        os.path.join(
                            out_dir, "fullrewrite", "sample.jsonl"),
                        os.path.join(
                            out_dir, "fullrewrite", "sample.ckpt.json"),
                        rows, {"completed_round_trips": 1},
                        campaign_out_dir=out_dir)
                    self.assertEqual(commit["status"], "appended")
                finally:
                    writer_done.set()
                inspector.join(5)

            self.assertFalse(inspector.is_alive())
            self.assertEqual(inspection_errors, [])
            self.assertEqual(len(inspection_results), 1)
            self.assertEqual(inspection_results[0]["errors"], [])
            self.assertEqual(len(real_read_jsonl(os.path.join(
                out_dir, "api_calls.jsonl"))), 2)
            self.assertEqual(len(real_read_jsonl(os.path.join(
                out_dir, "fullrewrite", "sample.jsonl"))), 2)

    def test_formal_recovery_authorization_state_machine(self):
        def build_fixture(out_dir, variant):
            sample = "sample"
            method = "hybridpatch"
            old_worker, new_worker = "worker-old", "worker-new"
            old_pid, new_pid = 101, 202
            old_invocation, new_invocation = "inv-old", "inv-new"
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(
                plan_path, ["target", "next"])
            plan_sha = paired_dispatch._sha256(plan_path)
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": [sample], "method_set": [method],
                    "num_round_trips": 2,
                },
                "task_plans": {
                    sample: {
                        "path": os.path.basename(plan_path),
                        "sha256": plan_sha,
                        "forward_state_sequence": ["target", "next"],
                    },
                },
            }
            forward_root = (
                "hybridpatch/sample/rt01/forward/hybridpatch_primary")
            backward_root = (
                "hybridpatch/sample/rt01/backward/hybridpatch_primary")
            next_root = (
                "hybridpatch/sample/rt02/forward/hybridpatch_primary")
            forward_g000 = f"{forward_root}/g000"
            backward_g000 = f"{backward_root}/g000"
            backward_g001 = f"{backward_root}/g001"
            next_g000 = f"{next_root}/g000"
            forward_fp = "fingerprint-forward"
            backward_fp = "fingerprint-backward"
            next_fp = "fingerprint-next"
            resume = {
                "parent_semantic_call_id": backward_g000,
                "semantic_root_id": backward_root,
                "semantic_call_id": backward_g001,
                "generation_index": 1,
                "request_fingerprint": backward_fp,
                "next_attempt_index": 3,
                "prior_worker_launch_id": old_worker,
                "prior_invocation_id": old_invocation,
            }

            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            for row in (
                {"event": "launch", "worker_launch_id": old_worker,
                 "sample": sample, "pid": old_pid},
                {"event": "worker_authorized",
                 "worker_launch_id": old_worker, "sample": sample,
                 "worker_pid": old_pid, "invocation_id": old_invocation,
                 "task_plan_sha256": plan_sha,
                 "transport_resume_authorization": None},
                {"event": "launch", "worker_launch_id": new_worker,
                 "sample": sample, "pid": new_pid},
                {"event": "worker_authorized",
                 "worker_launch_id": new_worker, "sample": sample,
                 "worker_pid": new_pid, "invocation_id": new_invocation,
                 "task_plan_sha256": plan_sha,
                 "transport_resume_authorization": resume},
            ):
                run_meta.append_jsonl_locked(dispatch_path, row)
            for row in (
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": old_invocation,
                    "worker_launch_id": old_worker,
                    "worker_pid": old_pid, "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "task_plans": {sample: {
                        "sha256": plan_sha, "round_trips": 2}},
                    "transport_resume_authorization": None,
                },
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": new_invocation,
                    "worker_launch_id": new_worker,
                    "worker_pid": new_pid, "samples": [sample],
                    "status": "started",
                    "task_plans": {sample: {
                        "sha256": plan_sha, "round_trips": 2}},
                    "transport_resume_authorization": (
                        paired_dispatch._canonical_resume_authorization(
                            resume)),
                },
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), row)
            paired_dispatch._write_active_worker_set(
                out_dir, manifest,
                [] if variant == "unconsumed" else [{
                    "worker_launch_id": new_worker, "sample": sample,
                }])

            attempt_rows = []

            def append_event(exact_id, root, generation, parent, worker,
                             event, **fields):
                attempt_rows.append({
                    "schema": "anchorpatch.api_attempt/4",
                    "step_id": "/".join(root.split("/")[:4]),
                    "semantic_root_id": root,
                    "semantic_call_id": exact_id,
                    "generation_index": generation,
                    "parent_semantic_call_id": parent,
                    "worker_launch_id": worker,
                    "event": event,
                    **fields,
                })

            def append_success(exact_id, root, generation, parent, worker,
                               call_id, fingerprint, attempt_index):
                append_event(
                    exact_id, root, generation, parent, worker,
                    "semantic_request", call_id=call_id,
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                append_event(
                    exact_id, root, generation, parent, worker,
                    "attempt_start", call_id=call_id,
                    attempt_index=attempt_index,
                    call_kind="hybridpatch_primary",
                    attempt_kind=(
                        "transport_initial" if generation == 0
                        else "transport_recovery_initial"),
                    request_fingerprint=fingerprint)
                append_event(
                    exact_id, root, generation, parent, worker,
                    "generation_progress", call_id=call_id,
                    attempt_index=attempt_index, delta_type="text_delta")
                append_event(
                    exact_id, root, generation, parent, worker,
                    "attempt_end", call_id=call_id,
                    attempt_index=attempt_index,
                    **self._successful_attempt_end_fields())
                append_event(
                    exact_id, root, generation, parent, worker,
                    "response_committed", call_id=call_id,
                    attempt_index=attempt_index,
                    response_slots_used=1,
                    transient_failure_count=0,
                    http_attempts_used=attempt_index,
                    request_fingerprint=fingerprint)

            def append_failure():
                append_event(
                    backward_g000, backward_root, 0, None, old_worker,
                    "semantic_request", call_id="old-backward",
                    call_kind="hybridpatch_primary",
                    request_fingerprint=backward_fp)
                for attempt_index in (1, 2):
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "attempt_start", call_id="old-backward",
                        attempt_index=attempt_index,
                        call_kind="hybridpatch_primary",
                        attempt_kind=(
                            "transport_initial" if attempt_index == 1
                            else "transport_retry"),
                        request_fingerprint=backward_fp)
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "generation_progress", call_id="old-backward",
                        attempt_index=attempt_index,
                        delta_type="text_delta")
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "attempt_end", call_id="old-backward",
                        attempt_index=attempt_index,
                        **self._failed_attempt_end_fields())
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "attempt_budget", call_id="old-backward",
                        attempt_index=attempt_index,
                        budget_class="response_slot",
                        response_slots_used=attempt_index,
                        transient_failure_count=0)
                append_event(
                    backward_g000, backward_root, 0, None, old_worker,
                    "call_failed", call_id="old-backward",
                    attempt_index=2, status="provider_failure",
                    error_type="incomplete_stream",
                    response_slots_used=2,
                    transient_failure_count=0,
                    http_attempts_used=2,
                    request_fingerprint=backward_fp)

            append_success(
                forward_g000, forward_root, 0, None, old_worker,
                "old-forward", forward_fp, 1)
            append_failure()
            if variant != "unconsumed":
                append_success(
                    backward_g001, backward_root, 1, backward_g000,
                    new_worker, "new-backward", backward_fp, 3)
            if variant == "accepted":
                append_success(
                    next_g000, next_root, 0, None, new_worker,
                    "new-next", next_fp, 1)
            with open(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in attempt_rows:
                    handle.write(json.dumps(row) + "\n")

            def api_row(*, exact_id, root, rt, direction, generation,
                        parent, request_id, fingerprint, worker, pid,
                        provider_called=True, response_replayed=False,
                        replayed_from=None, failure=False,
                        http_attempts=1):
                return {
                    "schema": "anchorpatch.api_call/4",
                    "sample": sample, "method": method,
                    "rt_index": rt, "direction": direction,
                    "call_kind": "hybridpatch_primary",
                    "step_id": "/".join(root.split("/")[:4]),
                    "semantic_root_id": root,
                    "semantic_call_id": exact_id,
                    "generation_index": generation,
                    "parent_semantic_call_id": parent,
                    "request_id": request_id,
                    "worker_launch_id": worker, "worker_pid": pid,
                    "provider_called": provider_called,
                    "response_replayed": response_replayed,
                    "replayed_from_call_id": replayed_from,
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "transport_recovery_index": generation,
                    "request_fingerprint": fingerprint,
                    "classification": (
                        "provider/API failure" if failure else None),
                    "count_as_method_failure": False,
                    "error_type": (
                        "incomplete_stream" if failure else None),
                    "max_response_slots": 2,
                    "response_slots_used": 2 if failure else 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": http_attempts,
                }

            rows = [
                api_row(
                    exact_id=forward_g000, root=forward_root, rt=1,
                    direction="forward", generation=0, parent=None,
                    request_id="old-forward", fingerprint=forward_fp,
                    worker=old_worker, pid=old_pid),
                api_row(
                    exact_id=backward_g000, root=backward_root, rt=1,
                    direction="backward", generation=0, parent=None,
                    request_id="old-backward", fingerprint=backward_fp,
                    worker=old_worker, pid=old_pid, failure=True,
                    http_attempts=2),
                api_row(
                    exact_id=forward_g000, root=forward_root, rt=1,
                    direction="forward", generation=0, parent=None,
                    request_id="replay-forward", fingerprint=forward_fp,
                    worker=new_worker, pid=new_pid,
                    provider_called=False, response_replayed=True,
                    replayed_from="old-forward"),
            ]
            if variant == "provider-before":
                rows[-1].update({
                    "provider_called": True,
                    "response_replayed": False,
                    "replayed_from_call_id": None,
                })
            if variant != "unconsumed":
                recovery_row = api_row(
                    exact_id=backward_g001, root=backward_root, rt=1,
                    direction="backward", generation=1,
                    parent=backward_g000, request_id="new-backward",
                    fingerprint=backward_fp, worker=new_worker,
                    pid=new_pid, http_attempts=3)
                rows.append(recovery_row)
                if variant == "duplicate":
                    rows.append(dict(recovery_row))
            if variant == "accepted":
                rows.append(api_row(
                    exact_id=next_g000, root=next_root, rt=2,
                    direction="forward", generation=0, parent=None,
                    request_id="new-next", fingerprint=next_fp,
                    worker=new_worker, pid=new_pid))
            with open(
                os.path.join(out_dir, "api_calls.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")

            os.makedirs(os.path.join(out_dir, "api_journal"), exist_ok=True)

            def write_journal(exact_id, root, generation, parent, call_id,
                              fingerprint, attempt_index):
                digest = hashlib.sha256(
                    exact_id.encode("utf-8")).hexdigest()[:24]
                run_meta.write_json_atomic(
                    os.path.join(
                        out_dir, "api_journal",
                        f"{digest}.response.json"), {
                            "schema": "anchorpatch.api_response_journal/4",
                            "semantic_root_id": root,
                            "semantic_call_id": exact_id,
                            "generation_index": generation,
                            "parent_semantic_call_id": parent,
                            "call_id": call_id,
                            "request_fingerprint": fingerprint,
                            "result": {
                                "message": "complete",
                                "stream_complete": True,
                                "stop_reason": "end_turn",
                                "input_tokens": 5, "output_tokens": 1,
                                "transport_revision": (
                                    "opencode_anthropic_sdk/4"),
                                "transport_resume_policy": (
                                    "exact_payload_new_semantic_call/1"),
                                "call_kind": "hybridpatch_primary",
                                "semantic_root_id": root,
                                "semantic_call_id": exact_id,
                                "generation_index": generation,
                                "parent_semantic_call_id": parent,
                                "max_response_slots": 2,
                                "response_slots_used": 1,
                                "max_transient_failures": 3,
                                "transient_failure_count": 0,
                                "http_attempts_used": attempt_index,
                            },
                        })

            write_journal(
                forward_g000, forward_root, 0, None,
                "old-forward", forward_fp, 1)
            if variant != "unconsumed":
                write_journal(
                    backward_g001, backward_root, 1, backward_g000,
                    "new-backward", backward_fp, 3)
            if variant == "accepted":
                write_journal(
                    next_g000, next_root, 0, None,
                    "new-next", next_fp, 1)
            return manifest, rows

        cases = (
            ("accepted", None),
            ("provider-before",
             "provider call preceded recovery authorization use"),
            ("unconsumed", "recovery authorization was not consumed"),
            ("duplicate", "recovery authorization used more than once"),
        )
        for variant, expected_error in cases:
            with self.subTest(variant=variant), \
                    tempfile.TemporaryDirectory() as out_dir:
                manifest, rows = build_fixture(out_dir, variant)
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")):
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, active_samples={"sample"})
                if expected_error is None:
                    self.assertEqual(inspection["errors"], [])
                    new_worker_ids = [
                        row["semantic_call_id"] for row in rows
                        if row["worker_launch_id"] == "worker-new"
                    ]
                    self.assertEqual(new_worker_ids, [
                        "hybridpatch/sample/rt01/forward/"
                        "hybridpatch_primary/g000",
                        "hybridpatch/sample/rt01/backward/"
                        "hybridpatch_primary/g001",
                        "hybridpatch/sample/rt02/forward/"
                        "hybridpatch_primary/g000",
                    ])
                else:
                    self.assertTrue(any(
                        expected_error in error
                        for error in inspection["errors"]),
                        inspection["errors"])

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
            attempt_rows = []
            journal_rows = []
            for method in manifest["config"]["method_set"]:
                os.makedirs(os.path.join(out_dir, method), exist_ok=True)
                result_rows = []
                for direction in ("forward", "backward"):
                    call_kind = (
                        "hybridpatch_primary"
                        if method == "hybridpatch"
                        else "fullrewrite_primary"
                    )
                    step_id = f"{method}/sample/rt01/{direction}"
                    semantic_root_id = f"{step_id}/{call_kind}"
                    semantic_call_id = f"{semantic_root_id}/g000"
                    request_id = f"original-{method}-{direction}"
                    request_fingerprint = f"fingerprint-{method}-{direction}"
                    api_rows.append({
                        "schema": "anchorpatch.api_call/4",
                        "sample": "sample", "method": method,
                        "rt_index": 1, "direction": direction,
                        "call_kind": call_kind,
                        "step_id": step_id,
                        "semantic_root_id": semantic_root_id,
                        "semantic_call_id": semantic_call_id,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "request_id": request_id,
                        "provider_called": True,
                        "worker_launch_id": "worker-a",
                        "worker_pid": 101,
                        "response_replayed": False,
                        "replayed_from_call_id": None,
                        "transport_revision": "opencode_anthropic_sdk/4",
                        "transport_resume_policy": (
                            "exact_payload_new_semantic_call/1"),
                        "transport_recovery_index": 0,
                        "request_fingerprint": request_fingerprint,
                        "classification": None,
                        "count_as_method_failure": False,
                        "max_response_slots": 2,
                        "response_slots_used": 1,
                        "max_transient_failures": 3,
                        "transient_failure_count": 0,
                        "http_attempts_used": 1,
                    })
                    journal_rows.append((semantic_call_id, {
                        "schema": "anchorpatch.api_response_journal/4",
                        "semantic_root_id": semantic_root_id,
                        "semantic_call_id": semantic_call_id,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "call_id": request_id,
                        "request_fingerprint": request_fingerprint,
                        "result": {
                            "message": "complete",
                            "stream_complete": True,
                            "stop_reason": "end_turn",
                            "input_tokens": 5,
                            "output_tokens": 1,
                            "transport_revision": (
                                "opencode_anthropic_sdk/4"),
                            "transport_resume_policy": (
                                "exact_payload_new_semantic_call/1"),
                            "call_kind": call_kind,
                            "semantic_root_id": semantic_root_id,
                            "semantic_call_id": semantic_call_id,
                            "generation_index": 0,
                            "parent_semantic_call_id": None,
                            "max_response_slots": 2,
                            "response_slots_used": 1,
                            "max_transient_failures": 3,
                            "transient_failure_count": 0,
                            "http_attempts_used": 1,
                        },
                    }))
                    for event, fields in (
                        ("semantic_request", {
                            "call_id": request_id,
                            "call_kind": call_kind,
                            "request_fingerprint": request_fingerprint,
                        }),
                        ("attempt_start", {
                            "call_id": request_id, "attempt_index": 1,
                            "call_kind": call_kind,
                            "attempt_kind": "transport_initial",
                            "request_fingerprint": request_fingerprint,
                        }),
                        ("generation_progress", {
                            "call_id": request_id, "attempt_index": 1,
                            "delta_type": "text_delta",
                        }),
                        ("attempt_end", {
                            "call_id": request_id, "attempt_index": 1,
                            **self._successful_attempt_end_fields(),
                        }),
                        ("response_committed", {
                            "call_id": request_id, "attempt_index": 1,
                            "response_slots_used": 1,
                            "transient_failure_count": 0,
                            "http_attempts_used": 1,
                            "request_fingerprint": request_fingerprint,
                        }),
                    ):
                        attempt_rows.append({
                            "schema": "anchorpatch.api_attempt/4",
                            "step_id": step_id,
                            "semantic_root_id": semantic_root_id,
                            "semantic_call_id": semantic_call_id,
                            "generation_index": 0,
                            "parent_semantic_call_id": None,
                            "worker_launch_id": "worker-a",
                            "event": event,
                            **fields,
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
                os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in attempt_rows:
                    handle.write(json.dumps(row) + "\n")
            os.makedirs(os.path.join(out_dir, "api_journal"), exist_ok=True)
            for semantic_call_id, journal in journal_rows:
                digest = hashlib.sha256(
                    semantic_call_id.encode("utf-8")).hexdigest()[:24]
                run_meta.write_json_atomic(
                    os.path.join(
                        out_dir, "api_journal",
                        f"{digest}.response.json"),
                    journal)
            with open(
                os.path.join(out_dir, "run_metadata.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({
                    "schema": "anchorpatch.run_metadata/3",
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
                    "transport_resume_authorization": None,
                }) + "\n")
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "sample_outcomes.jsonl"), {
                    "schema": "anchorpatch.sample_outcome/1",
                    "created_at": "2026-07-17T00:00:00+08:00",
                    "sample": "sample", "status": "finished",
                    "worker_launch_id": "worker-a", "worker_pid": 101,
                    "invocation_id": "invocation-a",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "checkpoint_progress": {
                        "fullrewrite": {
                            "completed_round_trips": 1,
                            "committed_rows": 2,
                        },
                        "hybridpatch": {
                            "completed_round_trips": 1,
                            "committed_rows": 2,
                        },
                    },
                })
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
                    "disposition": "finished",
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

                outcome_path = os.path.join(out_dir, "sample_outcomes.jsonl")
                with open(outcome_path, encoding="utf-8") as handle:
                    outcome_rows = [
                        json.loads(line) for line in handle if line.strip()
                    ]
                original_outcome_rows = copy.deepcopy(outcome_rows)

                def write_outcome_rows():
                    run_meta._write_jsonl_atomic(outcome_path, outcome_rows)

                outcome_rows[0]["checkpoint_progress"] = None
                write_outcome_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertTrue(any(
                    "finished sample checkpoint evidence drift" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                outcome_rows[:] = copy.deepcopy(original_outcome_rows)
                outcome_rows[0]["worker_pid"] = 202
                write_outcome_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertTrue(any(
                    "finished sample worker provenance mismatch" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                outcome_rows[:] = copy.deepcopy(original_outcome_rows)
                write_outcome_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])

                with open(dispatch_path, encoding="utf-8") as handle:
                    dispatch_rows = [
                        json.loads(line) for line in handle if line.strip()
                    ]
                original_dispatch_rows = copy.deepcopy(dispatch_rows)
                worker_exit = next(
                    row for row in dispatch_rows
                    if row.get("event") == "worker_exit"
                )
                worker_exit["returncode"] = 7
                worker_exit["disposition"] = "campaign_fatal"
                run_meta._write_jsonl_atomic(dispatch_path, dispatch_rows)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertTrue(any(
                    "worker exit provenance incomplete" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                dispatch_rows[:] = copy.deepcopy(original_dispatch_rows)
                run_meta._write_jsonl_atomic(dispatch_path, dispatch_rows)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])

                fullrewrite_checkpoint = os.path.join(
                    out_dir, "fullrewrite", "sample.ckpt.json")
                os.remove(fullrewrite_checkpoint)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                self.assertTrue(any(
                    "missing checkpoint: fullrewrite/sample" in error
                    for error in inspection["errors"]
                ))
                run_meta.write_json_atomic(
                    fullrewrite_checkpoint, {"completed_round_trips": 1})
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
                    "unscoreable backward RT1" in error
                    for error in inspection["errors"]
                ))
                backward_row["evaluation"] = {"error": "context_mismatch"}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])
                backward_row["evaluation"] = {"score": 1.5}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unscoreable backward RT1" in error
                    for error in inspection["errors"]
                ))
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

                failed_semantic = api_rows[0]["semantic_call_id"]
                failed_api_rows = [dict(row) for row in api_rows]
                failed_api_rows[0].update({
                    "classification": "provider/API failure",
                    "error_type": "incomplete_stream",
                    "response_slots_used": 2,
                    "http_attempts_used": 2,
                })

                def failed_attempt_records(api_row):
                    base = {
                        "schema": "anchorpatch.api_attempt/4",
                        "step_id": (
                            f"{api_row['method']}/{api_row['sample']}/"
                            f"rt{api_row['rt_index']:02d}/"
                            f"{api_row['direction']}"
                        ),
                        "semantic_root_id": api_row["semantic_root_id"],
                        "semantic_call_id": api_row["semantic_call_id"],
                        "generation_index": api_row["generation_index"],
                        "parent_semantic_call_id": (
                            api_row["parent_semantic_call_id"]
                        ),
                        "worker_launch_id": api_row["worker_launch_id"],
                    }
                    rows = [{
                        **base,
                        "event": "semantic_request",
                        "call_id": api_row["request_id"],
                        "call_kind": api_row["call_kind"],
                        "request_fingerprint": (
                            api_row["request_fingerprint"]
                        ),
                    }]
                    for attempt_index in (1, 2):
                        rows.extend([
                            {
                                **base,
                                "event": "attempt_start",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                "call_kind": api_row["call_kind"],
                                "attempt_kind": (
                                    "transport_initial"
                                    if attempt_index == 1
                                    else "transport_retry"
                                ),
                                "request_fingerprint": (
                                    api_row["request_fingerprint"]
                                ),
                            },
                            {
                                **base,
                                "event": "generation_progress",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                "delta_type": "text_delta",
                            },
                            {
                                **base,
                                "event": "attempt_end",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                **self._failed_attempt_end_fields(),
                            },
                            {
                                **base,
                                "event": "attempt_budget",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                "budget_class": "response_slot",
                                "response_slots_used": attempt_index,
                                "transient_failure_count": 0,
                            },
                        ])
                    rows.append({
                        **base,
                        "event": "call_failed",
                        "call_id": api_row["request_id"],
                        "attempt_index": 2,
                        "status": "provider_failure",
                        "error_type": "incomplete_stream",
                        "response_slots_used": 2,
                        "transient_failure_count": 0,
                        "http_attempts_used": 2,
                        "request_fingerprint": (
                            api_row["request_fingerprint"]
                        ),
                    })
                    return rows

                failed_attempt_rows = []
                for row in attempt_rows:
                    if row["semantic_call_id"] == failed_semantic:
                        continue
                    failed_attempt_rows.append(row)
                failed_attempt_rows.extend(
                    failed_attempt_records(failed_api_rows[0]))
                failed_journal_rows = [
                    item for item in journal_rows
                    if item[0] != failed_semantic
                ]
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    failed_api_rows)
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                    failed_attempt_rows)
                journal_dir = os.path.join(out_dir, "api_journal")
                failed_digest = hashlib.sha256(
                    failed_semantic.encode("utf-8")).hexdigest()[:24]
                os.remove(os.path.join(
                    journal_dir, f"{failed_digest}.response.json"))
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest)
                self.assertTrue(any(
                    "committed row requires a response-journal-backed "
                    "successful semantic call" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    api_rows)
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                    attempt_rows)
                for semantic_call_id, journal in failed_journal_rows:
                    digest = hashlib.sha256(
                        semantic_call_id.encode("utf-8")).hexdigest()[:24]
                    run_meta.write_json_atomic(
                        os.path.join(
                            journal_dir, f"{digest}.response.json"),
                        journal)
                for semantic_call_id, journal in journal_rows:
                    digest = hashlib.sha256(
                        semantic_call_id.encode("utf-8")).hexdigest()[:24]
                    run_meta.write_json_atomic(
                        os.path.join(
                            journal_dir, f"{digest}.response.json"),
                        journal)

                original = api_rows[0]
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
                    "duplicate provider semantic generation" in error
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

    def test_supplemental_key_pool_caps_four_and_mixes_method_starts(self):
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        assignments = paired_dispatch.build_key_assignments(
            paired_dispatch.SUPPLEMENTAL_SAMPLES, labels, 4,
            alternate_within_key=True)
        self.assertEqual(len(assignments), 40)
        by_key = {label: [] for label in labels}
        for item in assignments:
            by_key[item["key_label"]].append(item)
        self.assertEqual(
            sorted(len(items) for items in by_key.values()),
            [3] * 12 + [4],
        )
        for items in by_key.values():
            self.assertLessEqual(len(items), 4)
            self.assertEqual(
                {item["methods"][0] for item in items},
                {"hybridpatch", "fullrewrite"},
            )
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments), 20)
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments), 20)

        args = mock.Mock(
            campaign_role="supplemental", smoke_dir=None,
            samples=paired_dispatch.SUPPLEMENTAL_SAMPLES,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        paired_dispatch._validate_campaign_grid(args)
        args.slots_per_key = 3
        with self.assertRaisesRegex(RuntimeError, "slots_per_key 4"):
            paired_dispatch._validate_campaign_grid(args)

        legacy = paired_dispatch.build_key_assignments(
            paired_dispatch.MAIN_SAMPLES, labels[:10], 1)
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch" for item in legacy), 5)
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite" for item in legacy), 5)

    def test_confirmation_selection_digest_and_partition_are_strict(self):
        candidates = [f"sample-{index:03d}" for index in range(85)]
        selected = candidates[:68]
        reserve = candidates[68:]
        features = {
            sample: {"domain": f"domain-{index % 7}"}
            for index, sample in enumerate(candidates)
        }
        payload = {
            "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
            "experiment_id": paired_dispatch.CONFIRMATION_EXPERIMENT_ID,
            "candidate_rule": {
                "seed": 42, "candidate_count": 85,
                "selected_count": 68, "reserve_count": 17,
                "rule": "candidate_count_below100_select_about80_percent",
                "strict_unseen_candidate_count": 0,
            },
            "candidate_sample_ids": candidates,
            "selected_sample_ids": selected,
            "reserve_sample_ids": reserve,
            "reserve_selection_order": list(reversed(reserve)),
            "features": features,
            "exposure_policy": {
                "strict_any_provider_call": {"sample_count": 234},
                "method_developer_unseen": {
                    "excluded_sample_count": 149,
                    "registry_clean_candidate_count": 86,
                    "split_test_count": 20,
                    "clean_candidate_with_method_exposure_count": 1,
                    "clean_candidate_with_method_exposure_ids": ["python1"],
                    "documented_developer_content_exposure": {
                        "python1": "docs/FINDINGS.md:370-399",
                    },
                },
            },
            "runtime_evaluator_smoke": {
                "checked_count": 234, "runnable_count": 234,
                "failed_count": 0,
            },
            "inputs": {
                "selector_script": {
                    "path": "tools/build_unseen_confirmation_split.py",
                    "sha256": "1" * 64,
                },
                "registry": {
                    "path": "data/CONTAMINATION_REGISTRY.json",
                    "sha256": "2" * 64,
                },
                "hybrid_split": {
                    "path": "data/hybrid_split.json",
                    "sha256": "3" * 64,
                },
                "samples_root": "data/samples_delegate52",
                "sample_count": 234,
                "sample_json_manifest_sha256": "4" * 64,
            },
        }
        payload["artifact_sha256_preview"] = hashlib.sha256(
            paired_dispatch._canonical_json_bytes({
                "candidate_sample_ids": candidates,
                "selected_sample_ids": selected,
                "reserve_sample_ids": reserve,
                "features": features,
            })
        ).hexdigest()
        validated = paired_dispatch._validate_confirmation_selection_payload(
            payload, available_samples=candidates)
        self.assertEqual(validated["selected_sample_ids"], selected)
        self.assertEqual(validated["reserve_sample_ids"], reserve)
        registry = {
            "entries": [
                {"sample_id": sample, "status": "clean_candidate"}
                for sample in [*candidates, "python1"]
            ]
        }
        sealed_split = {
            "splits": {
                "dev": [], "val": [], "test": [], "unused_reserve": []
            }
        }
        paired_dispatch._validate_confirmation_candidate_semantics(
            validated, registry, sealed_split)
        bad_split = json.loads(json.dumps(sealed_split))
        bad_split["splits"]["test"] = [candidates[0]]
        with self.assertRaisesRegex(RuntimeError, "sealed"):
            paired_dispatch._validate_confirmation_candidate_semantics(
                validated, registry, bad_split)
        bad_registry = json.loads(json.dumps(registry))
        bad_registry["entries"].pop(0)
        with self.assertRaisesRegex(RuntimeError, "clean_candidate"):
            paired_dispatch._validate_confirmation_candidate_semantics(
                validated, bad_registry, sealed_split)
        args = mock.Mock(
            campaign_role="confirmation", samples=selected,
            selection_manifest="selection.json")
        with self.assertRaisesRegex(RuntimeError, "match --out_dir"):
            with mock.patch.object(
                    paired_dispatch, "_load_confirmation_selection",
                    return_value=(
                        selected, {"experiment_id": "exp_confirmation"})):
                paired_dispatch._resolve_confirmation_selection(
                    args, out_dir="different_experiment")

        mutations = []
        bad = json.loads(json.dumps(payload))
        bad["schema"] = "anchorpatch.method_unseen_confirmation_split/0"
        mutations.append((bad, "schema"))
        bad = json.loads(json.dumps(payload))
        bad["candidate_rule"]["seed"] = 7
        mutations.append((bad, "seed42"))
        bad = json.loads(json.dumps(payload))
        bad["reserve_sample_ids"][0] = bad["selected_sample_ids"][0]
        mutations.append((bad, "partition"))
        bad = json.loads(json.dumps(payload))
        bad["artifact_sha256_preview"] = "0" * 64
        mutations.append((bad, "digest"))
        for value, message in mutations:
            with self.subTest(message=message), self.assertRaisesRegex(
                    RuntimeError, message):
                paired_dispatch._validate_confirmation_selection_payload(
                    value, available_samples=candidates)

    def test_confirmation_selection_prefill_does_not_bypass_loader(self):
        selected = ["sample-a", "sample-b"]
        verified_record = {
            "experiment_id": "exp_verified",
            "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
            "path": "HP_V8/analysis/selection.json",
            "sha256": "1" * 64,
        }
        args = mock.Mock(
            campaign_role="confirmation", samples=[],
            selection_manifest="selection.json")
        args._selection_samples = ["forged"]
        args._selection_manifest_record = {"experiment_id": "forged"}
        with mock.patch.object(
                paired_dispatch, "_load_confirmation_selection",
                return_value=(selected, verified_record)) as load_selection:
            resolved = paired_dispatch._resolve_confirmation_selection(
                args, out_dir="exp_verified")
            load_selection.assert_called_once_with(
                "selection.json", current_experiment_dir="exp_verified")
        self.assertEqual(resolved, verified_record)
        self.assertEqual(args.samples, selected)

        with mock.patch.object(
                paired_dispatch, "_load_confirmation_selection",
                side_effect=AssertionError("verified cache should be reused")):
            self.assertEqual(
                paired_dispatch._resolve_confirmation_selection(
                    args, out_dir="exp_verified"),
                verified_record)

    def test_confirmation_selection_recompute_detects_swap_and_evidence_drift(self):
        candidates = [f"sample-{index}" for index in range(5)]
        selected = candidates[:4]
        reserve = candidates[4:]
        features = {
            sample: {
                "sample_id": sample,
                "domain": "unit",
                "formats": [".txt"],
                "format_group": ".txt",
                "task_types": ["local_edit"],
                "file_count": 1,
                "file_count_bin": "1",
                "doc_bytes": 100 + index,
                "context_files": ["a.txt"],
                "doc_length_bin": "q1",
            }
            for index, sample in enumerate(candidates)
        }

        class FakeSelector:
            @staticmethod
            def sample_ids(_samples_root):
                return list(candidates)

            @staticmethod
            def scan_any_provider_exposure(_repo, _all_samples):
                return {
                    "sample_ids": list(candidates),
                    "sample_count": len(candidates),
                    "evidence_manifest_sha256": "any-evidence",
                    "api_call_files": 1,
                    "api_raw_request_files": 0,
                }

            @staticmethod
            def scan_method_exposure(_repo, _all_samples):
                return {
                    "sample_ids": [],
                    "sample_count": 0,
                    "path_manifest_sha256": "method-evidence",
                }

            @staticmethod
            def load_registry_sets(_registry):
                return {
                    "contaminated": set(),
                    "unsupported_generation_domain": set(),
                    "clean_candidate": set(candidates),
                    "reserved_holdout_candidate": set(),
                }

            @staticmethod
            def split_sets(_hybrid_split):
                return {}

            @staticmethod
            def method_holdout_candidates(
                    _all_samples, registry_sets, _split, _method_exposure):
                return set(registry_sets["clean_candidate"]), set(), set()

            @staticmethod
            def sample_feature(_repo, sample_id):
                return copy.deepcopy(features[sample_id])

            @staticmethod
            def add_length_bins(_features):
                return None

            @staticmethod
            def selection_counts(candidate_count):
                self.assertEqual(candidate_count, 5)
                return 4, 1, "candidate_count_below100_select_about80_percent"

            @staticmethod
            def stratified_reserve(
                    _candidate_ids, _features, reserve_n, seed):
                self.assertEqual((reserve_n, seed), (1, 42))
                return list(reserve), list(reserve), {}

        payload = {
            "exposure_policy": {
                "strict_any_provider_call": {
                    "sample_ids": list(candidates),
                    "sample_count": len(candidates),
                    "evidence_manifest_sha256": "any-evidence",
                    "api_call_files": 1,
                    "api_raw_request_files": 0,
                },
                "method_developer_unseen": {
                    "actual_method_scan_count": 0,
                    "actual_method_scan_sample_ids": [],
                    "actual_method_path_manifest_sha256": "method-evidence",
                    "clean_candidate_with_method_exposure_count": 0,
                    "clean_candidate_with_method_exposure_ids": [],
                    "excluded_sample_count": 0,
                    "excluded_sample_ids": [],
                },
            },
            "runtime_evaluator_smoke": {
                "runnable_sample_ids": list(candidates),
            },
            "candidate_rule": {
                "seed": 42,
                "rule": "candidate_count_below100_select_about80_percent",
                "candidate_count": 5,
                "selected_count": 4,
                "reserve_count": 1,
                "strict_unseen_candidate_count": 0,
            },
            "candidate_sample_ids": list(candidates),
            "selected_sample_ids": list(selected),
            "reserve_sample_ids": list(reserve),
            "reserve_selection_order": list(reserve),
            "features": copy.deepcopy(features),
        }
        validated = {
            "selected_sample_ids": list(selected),
            "reserve_sample_ids": list(reserve),
            "reserve_selection_order": list(reserve),
        }
        paired_dispatch._validate_confirmation_recomputed_selection(
            payload, validated, pathlib.Path("."), FakeSelector, {}, {})

        swapped_payload = copy.deepcopy(payload)
        swapped_payload["selected_sample_ids"] = [
            *candidates[:3], candidates[4]]
        swapped_payload["reserve_sample_ids"] = [candidates[3]]
        swapped_validated = {
            "selected_sample_ids": swapped_payload["selected_sample_ids"],
            "reserve_sample_ids": swapped_payload["reserve_sample_ids"],
            "reserve_selection_order": swapped_payload["reserve_sample_ids"],
        }
        with self.assertRaisesRegex(RuntimeError, "selected_sample_ids"):
            paired_dispatch._validate_confirmation_recomputed_selection(
                swapped_payload, swapped_validated, pathlib.Path("."),
                FakeSelector, {}, {})

        drifted_payload = copy.deepcopy(payload)
        drifted_payload["exposure_policy"]["strict_any_provider_call"][
            "evidence_manifest_sha256"] = "changed"
        with self.assertRaisesRegex(RuntimeError, "evidence_manifest_sha256"):
            paired_dispatch._validate_confirmation_recomputed_selection(
                drifted_payload, validated, pathlib.Path("."),
                FakeSelector, {}, {})

    def test_confirmation_selection_loader_excludes_current_experiment_only(self):
        all_samples = [f"sample-{index:03d}" for index in range(234)]
        candidates = all_samples[:85]
        selected = candidates[:68]
        reserve = candidates[68:]
        excluded = all_samples[85:]

        def feature(sample_id):
            return {
                "sample_id": sample_id,
                "domain": "unit",
                "formats": [".txt"],
                "format_group": ".txt",
                "task_types": ["local_edit"],
                "file_count": 1,
                "file_count_bin": "1",
                "doc_bytes": 100,
                "context_files": ["a.txt"],
                "doc_length_bin": "q1",
            }

        selector_source = textwrap.dedent("""
            from collections import defaultdict
            import hashlib
            import json

            def compact_json_bytes(value):
                return json.dumps(
                    value, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")).encode("utf-8")

            def sha256_file(path):
                h = hashlib.sha256()
                with path.open("rb") as handle:
                    h.update(handle.read())
                return h.hexdigest()

            def sample_ids(samples_root):
                return sorted(
                    path.parent.name
                    for path in samples_root.glob("*/sample.json")
                    if path.parent.is_dir())

            def _walk_experiment_paths(repo):
                root = repo / "HP_V8"
                if root.exists():
                    yield from root.rglob("*")

            def scan_any_provider_exposure(repo, all_samples):
                samples = set()
                evidence = defaultdict(list)
                api_call_files = 0
                api_call_sources = []
                for path in _walk_experiment_paths(repo):
                    if not path.is_file() or path.name != "api_calls.jsonl":
                        continue
                    api_call_files += 1
                    rel = path.relative_to(repo).as_posix()
                    api_call_sources.append({
                        "path": rel, "sha256": sha256_file(path)})
                    with path.open(encoding="utf-8") as handle:
                        for line in handle:
                            try:
                                row = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            sample = row.get("sample") or row.get("sample_id")
                            if sample in all_samples:
                                samples.add(sample)
                                if len(evidence[sample]) < 3:
                                    evidence[sample].append(rel)
                return {
                    "sample_ids": sorted(samples),
                    "sample_count": len(samples),
                    "evidence_examples": dict(sorted(evidence.items())),
                    "api_call_files": api_call_files,
                    "api_raw_request_files": 0,
                    "evidence_manifest_sha256": hashlib.sha256(
                        compact_json_bytes({
                            "api_call_sources": sorted(
                                api_call_sources,
                                key=lambda item: item["path"]),
                            "api_raw_request_paths": [],
                        })).hexdigest(),
                }

            def scan_method_exposure(_repo, _all_samples):
                return {
                    "sample_ids": [],
                    "sample_count": 0,
                    "evidence_examples": {},
                    "path_manifest_sha256": hashlib.sha256(
                        compact_json_bytes({
                            "matched_paths": [],
                            "matched_api_call_sources": [],
                        })).hexdigest(),
                }

            def load_registry_sets(registry):
                clean = {
                    item["sample_id"] for item in registry["entries"]
                    if item.get("status") == "clean_candidate"}
                return {
                    "contaminated": set(),
                    "unsupported_generation_domain": set(),
                    "clean_candidate": clean,
                    "reserved_holdout_candidate": set(),
                }

            def split_sets(_hybrid_split):
                return {}

            def method_holdout_candidates(
                    all_samples, registry_sets, _split, _method_exposure):
                pool = set(registry_sets["clean_candidate"]) - {"python1"}
                exposed = set(all_samples) - pool
                return pool, exposed, {"python1"}

            def sample_feature(_repo, sample_id):
                return {
                    "sample_id": sample_id,
                    "domain": "unit",
                    "formats": [".txt"],
                    "format_group": ".txt",
                    "task_types": ["local_edit"],
                    "file_count": 1,
                    "file_count_bin": "1",
                    "doc_bytes": 100,
                    "context_files": ["a.txt"],
                    "doc_length_bin": "q1",
                }

            def add_length_bins(_features):
                return None

            def selection_counts(candidate_count):
                assert candidate_count == 85
                return 68, 17, "candidate_count_below100_select_about80_percent"

            def stratified_reserve(_candidate_ids, _features, reserve_n, seed):
                assert (reserve_n, seed) == (17, 42)
                reserve = [f"sample-{index:03d}" for index in range(68, 85)]
                return reserve, reserve, {}
        """)

        with tempfile.TemporaryDirectory() as root_dir:
            repo_root = pathlib.Path(root_dir)
            samples_root = repo_root / "data" / "samples_delegate52"
            for sample in all_samples:
                sample_dir = samples_root / sample
                sample_dir.mkdir(parents=True, exist_ok=True)
                (sample_dir / "sample.json").write_text(
                    "{}", encoding="utf-8")
            sample_files = {
                sample: paired_dispatch._sha256(
                    samples_root / sample / "sample.json")
                for sample in all_samples
            }
            sample_digest = hashlib.sha256(
                paired_dispatch._canonical_json_bytes(sample_files)
            ).hexdigest()

            tools_dir = repo_root / "tools"
            tools_dir.mkdir()
            selector_path = tools_dir / "build_unseen_confirmation_split.py"
            selector_path.write_text(selector_source, encoding="utf-8")
            registry_path = repo_root / "data" / "CONTAMINATION_REGISTRY.json"
            registry = {
                "entries": [
                    {"sample_id": sample, "status": "clean_candidate"}
                    for sample in [*candidates, "python1"]
                ]
            }
            paired_dispatch.write_json_atomic(registry_path, registry)
            split_path = repo_root / "data" / "hybrid_split.json"
            paired_dispatch.write_json_atomic(split_path, {
                "splits": {
                    "dev": [], "val": [], "test": [],
                    "unused_reserve": [],
                }
            })

            history_dir = repo_root / "HP_V8" / "exp_history"
            history_dir.mkdir(parents=True)
            history_api = history_dir / "api_calls.jsonl"
            with history_api.open("w", encoding="utf-8") as handle:
                for sample in all_samples:
                    handle.write(json.dumps({
                        "sample": sample,
                        "method": "fullrewrite",
                    }) + "\n")
            strict_digest = hashlib.sha256(
                paired_dispatch._canonical_json_bytes({
                    "api_call_sources": [{
                        "path": history_api.relative_to(repo_root).as_posix(),
                        "sha256": paired_dispatch._sha256(history_api),
                    }],
                    "api_raw_request_paths": [],
                })
            ).hexdigest()
            method_digest = hashlib.sha256(
                paired_dispatch._canonical_json_bytes({
                    "matched_paths": [],
                    "matched_api_call_sources": [],
                })
            ).hexdigest()
            features = {sample: feature(sample) for sample in candidates}
            payload = {
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "experiment_id": paired_dispatch.CONFIRMATION_EXPERIMENT_ID,
                "candidate_rule": {
                    "seed": 42,
                    "candidate_count": 85,
                    "selected_count": 68,
                    "reserve_count": 17,
                    "rule": (
                        "candidate_count_below100_select_about80_percent"),
                    "strict_unseen_candidate_count": 0,
                },
                "candidate_sample_ids": candidates,
                "selected_sample_ids": selected,
                "reserve_sample_ids": reserve,
                "reserve_selection_order": reserve,
                "features": features,
                "exposure_policy": {
                    "strict_any_provider_call": {
                        "sample_ids": all_samples,
                        "sample_count": 234,
                        "evidence_manifest_sha256": strict_digest,
                        "api_call_files": 1,
                        "api_raw_request_files": 0,
                    },
                    "method_developer_unseen": {
                        "actual_method_scan_count": 0,
                        "actual_method_scan_sample_ids": [],
                        "actual_method_path_manifest_sha256": method_digest,
                        "excluded_sample_count": 149,
                        "excluded_sample_ids": excluded,
                        "registry_clean_candidate_count": 86,
                        "split_test_count": 20,
                        "clean_candidate_with_method_exposure_count": 1,
                        "clean_candidate_with_method_exposure_ids": ["python1"],
                        "documented_developer_content_exposure": {
                            "python1": "docs/FINDINGS.md:370-399",
                        },
                    },
                },
                "runtime_evaluator_smoke": {
                    "checked_count": 234,
                    "runnable_count": 234,
                    "failed_count": 0,
                    "runnable_sample_ids": all_samples,
                },
                "inputs": {
                    "selector_script": {
                        "path": "tools/build_unseen_confirmation_split.py",
                        "sha256": paired_dispatch._sha256(selector_path),
                    },
                    "registry": {
                        "path": "data/CONTAMINATION_REGISTRY.json",
                        "sha256": paired_dispatch._sha256(registry_path),
                    },
                    "hybrid_split": {
                        "path": "data/hybrid_split.json",
                        "sha256": paired_dispatch._sha256(split_path),
                    },
                    "samples_root": "data/samples_delegate52",
                    "sample_count": 234,
                    "sample_json_manifest_sha256": sample_digest,
                },
            }
            payload["artifact_sha256_preview"] = hashlib.sha256(
                paired_dispatch._canonical_json_bytes({
                    "candidate_sample_ids": candidates,
                    "selected_sample_ids": selected,
                    "reserve_sample_ids": reserve,
                    "features": features,
                })
            ).hexdigest()
            selection_path = (
                repo_root / "HP_V8" / "analysis"
                / "selection.json")
            selection_path.parent.mkdir(parents=True)
            paired_dispatch.write_json_atomic(selection_path, payload)
            current_dir = (
                repo_root / "HP_V8"
                / paired_dispatch.CONFIRMATION_EXPERIMENT_ID)
            current_dir.mkdir()
            with (current_dir / "api_calls.jsonl").open(
                    "w", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "sample": all_samples[0],
                    "method": "fullrewrite",
                }) + "\n")

            class Completed:
                returncode = 0
                stdout = b""
                stderr = b""

            def fake_git(command, **_kwargs):
                result = Completed()
                if command[:2] == ["git", "show"]:
                    result.stdout = selection_path.read_bytes()
                return result

            with mock.patch.object(
                    paired_dispatch, "_resolve_selection_path",
                    return_value=(selection_path, repo_root)), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "run",
                        side_effect=fake_git), \
                    mock.patch.object(
                        paired_dispatch, "SAMPLES_ROOT", str(samples_root)):
                selected_ids, record = paired_dispatch._load_confirmation_selection(
                    str(selection_path), current_experiment_dir=current_dir)
                self.assertEqual(selected_ids, selected)
                self.assertEqual(
                    record["recompute"]["excluded_current_experiment_dir"],
                    current_dir.relative_to(repo_root).as_posix())

                other_dir = repo_root / "HP_V8" / "exp_new_history"
                other_dir.mkdir()
                with (other_dir / "api_calls.jsonl").open(
                        "w", encoding="utf-8") as handle:
                    handle.write(json.dumps({
                        "sample": all_samples[1],
                        "method": "fullrewrite",
                    }) + "\n")
                with self.assertRaisesRegex(
                        RuntimeError,
                        "strict_any_provider_call.evidence_manifest_sha256"):
                    paired_dispatch._load_confirmation_selection(
                        str(selection_path),
                        current_experiment_dir=current_dir)

    def test_confirmation_manifest_uses_balanced_work_conserving_key_queues(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 4, alternate_within_key=True,
            allow_queue=True)
        for label in labels:
            initial = [
                item for item in assignments if item["key_label"] == label
            ][:4]
            self.assertEqual(
                sum(item["methods"][0] == "hybridpatch" for item in initial),
                2,
            )
            self.assertEqual(
                sum(item["methods"][0] == "fullrewrite" for item in initial),
                2,
            )
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments), 34)
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments), 34)
        queues = paired_dispatch._assignment_key_queues(assignments)
        self.assertEqual(len(queues), 13)
        self.assertEqual(
            sorted(queue["worker_count"] for queue in queues),
            [5] * 10 + [6] * 3,
        )
        with self.assertRaisesRegex(RuntimeError, "concurrency capacity"):
            paired_dispatch.build_key_assignments(samples, labels, 4)

        args = mock.Mock(
            campaign_role="confirmation", smoke_dir=None, samples=samples,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        args._selection_manifest_record = {
            "experiment_id": "out",
            "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
            "path": "HP_V8/analysis/selection.json",
            "sha256": "1" * 64,
            "artifact_sha256_preview": "2" * 64,
            "candidate_count": 85, "selected_count": 68,
            "reserve_count": 17, "seed": 42,
            "inputs": {
                "selector_script": {"path": "tools/selector.py",
                                    "sha256": "3" * 64},
                "registry": {"path": "data/registry.json",
                             "sha256": "4" * 64},
                "hybrid_split": {"path": "data/split.json",
                                 "sha256": "5" * 64},
                "sample_json_manifest": {
                    "path": "data/samples_delegate52",
                    "sample_count": 234, "sha256": "6" * 64,
                },
            },
        }
        paired_dispatch._validate_campaign_grid(args)
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "out", samples, assignments, {}, args)
        self.assertNotIn("assignment_waves", manifest)
        self.assertEqual(
            sum(queue["worker_count"]
                for queue in manifest["assignment_queues"]),
            68)
        self.assertEqual(
            manifest["config"]["dispatch_policy"],
            paired_dispatch.WORK_CONSERVING_DISPATCH_POLICY)
        self.assertEqual(manifest["config"]["max_worker_count"], 52)
        self.assertEqual(manifest["config"]["queued_worker_count"], 16)
        self.assertEqual(
            manifest["selection_manifest"]["sha256"], "1" * 64)
        self.assertEqual(
            manifest["analysis_policy"],
            paired_dispatch.CONFIRMATION_ANALYSIS_POLICY)
        self.assertEqual(
            manifest["analysis_policy"]["evaluator_error_policy"][
                "error_row_score"],
            0.0)
        self.assertEqual(
            set(manifest["selection_manifest"]["inputs"]),
            {"selector_script", "registry", "hybrid_split",
             "sample_json_manifest"})
        self.assertEqual(manifest["config"]["slots_per_key"], 4)

    def test_mixed_confirmation_selection_and_balanced_100_grid(self):
        repo_root = pathlib.Path(paired_dispatch._ROOT).parent
        selection_path = (
            repo_root / "HP_V8" / "analysis"
            / "20260719_hybridv8_transportv4_mixed_confirmation100"
            / "selection.json"
        )
        payload = json.loads(selection_path.read_text(encoding="utf-8"))
        validated = paired_dispatch._validate_confirmation_selection_payload(
            payload,
            available_samples=set(payload["candidate_sample_ids"]),
        )
        self.assertEqual(validated["selection_kind"], "mixed100")
        self.assertEqual(len(validated["selected_sample_ids"]), 100)
        self.assertEqual(
            len(payload["cohorts"]["method_unseen"]["selected_sample_ids"]),
            60,
        )
        self.assertEqual(
            len(payload["cohorts"]["historical_hp_method_exposed"]
                ["selected_sample_ids"]),
            40,
        )
        samples = validated["selected_sample_ids"]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        selection_record = {
            "schema": paired_dispatch.MIXED_CONFIRMATION_SELECTION_SCHEMA,
            "cohorts": validated["cohorts"],
        }
        assignment_samples = paired_dispatch._mixed_confirmation_sample_order(
            samples, labels, 4, selection_record)
        assignments = paired_dispatch.build_key_assignments(
            assignment_samples, labels, 4, alternate_within_key=True,
            allow_queue=True)
        queues = paired_dispatch._assignment_key_queues(assignments)
        self.assertEqual(len(queues), 13)
        self.assertEqual(
            sorted(queue["worker_count"] for queue in queues),
            [7] * 4 + [8] * 9,
        )
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments),
            50,
        )
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments),
            50,
        )
        for cohort, expected in (
                (set(validated["cohorts"]["method_unseen"]), 30),
                (set(validated["cohorts"]
                     ["historical_hp_method_exposed"]), 20)):
            cohort_assignments = [
                item for item in assignments if item["sample"] in cohort]
            self.assertEqual(
                sum(item["methods"][0] == "hybridpatch"
                    for item in cohort_assignments),
                expected,
            )
            self.assertEqual(
                sum(item["methods"][0] == "fullrewrite"
                    for item in cohort_assignments),
                expected,
            )
        args = mock.Mock(
            campaign_role="confirmation", smoke_dir=None, samples=samples,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        args._selection_manifest_record = {
            "selected_count": 100,
            "schema": paired_dispatch.MIXED_CONFIRMATION_SELECTION_SCHEMA,
            "experiment_id": paired_dispatch.MIXED_CONFIRMATION_EXPERIMENT_ID,
        }
        paired_dispatch._validate_campaign_grid(args)

    def test_full234_scope_is_exact_and_uses_thirteen_rolling_key_queues(self):
        samples, scope = paired_dispatch._load_full234_scope()
        self.assertEqual(len(samples), 234)
        self.assertEqual(samples, sorted(samples))
        self.assertEqual(scope["sample_count"], 234)
        self.assertEqual(len(scope["sample_json_sha256"]), 64)

        args = mock.Mock(
            campaign_role="full234", smoke_dir=None, samples=None,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        resolved = paired_dispatch._resolve_full234_scope(args)
        self.assertEqual(resolved, scope)
        self.assertEqual(args.samples, samples)
        paired_dispatch._validate_campaign_grid(args)

        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 4, alternate_within_key=True,
            allow_queue=True)
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments),
            117,
        )
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments),
            117,
        )
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "out", samples, assignments, {}, args)
        self.assertEqual(manifest["full234_scope"], scope)
        self.assertEqual(len(manifest["assignment_queues"]), 13)
        self.assertTrue(all(
            queue["worker_count"] == 18
            for queue in manifest["assignment_queues"]))
        self.assertEqual(manifest["config"]["max_worker_count"], 52)
        self.assertEqual(manifest["config"]["queued_worker_count"], 182)
        self.assertNotIn("assignment_waves", manifest)

    def test_remaining134_scope_excludes_planned100_and_uses_fourteen_keys(self):
        all_samples, _full_scope = paired_dispatch._load_full234_scope()
        samples, scope = paired_dispatch._load_remaining134_scope()
        selection = json.loads(pathlib.Path(
            paired_dispatch.REMAINING134_SELECTION_PATH
        ).read_text(encoding="utf-8"))
        excluded = set(selection["selected_sample_ids"])
        self.assertEqual(len(all_samples), 234)
        self.assertEqual(len(excluded), 100)
        self.assertEqual(len(samples), 134)
        self.assertFalse(excluded & set(samples))
        self.assertEqual(excluded | set(samples), set(all_samples))
        self.assertEqual(scope["sample_count"], 134)
        self.assertEqual(
            scope["exclusion_source"]["sha256"],
            paired_dispatch.REMAINING134_SELECTION_SHA256)

        args = mock.Mock(
            campaign_role="remaining134", smoke_dir=None, samples=None,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        resolved = paired_dispatch._resolve_remaining134_scope(args)
        self.assertEqual(resolved, scope)
        self.assertEqual(args.samples, samples)
        paired_dispatch._validate_campaign_grid(args)

        labels = [f"KEY_{index:02d}" for index in range(1, 15)]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 4, allow_queue=True)
        for item in assignments:
            item["methods"] = list(paired_dispatch.REMAINING134_METHOD_PHASES)
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "out", samples, assignments, {}, args)
        self.assertEqual(manifest["remaining134_scope"], scope)
        self.assertEqual(len(manifest["assignment_queues"]), 14)
        self.assertEqual(
            sorted(queue["worker_count"]
                   for queue in manifest["assignment_queues"]),
            [9] * 6 + [10] * 8,
        )
        self.assertTrue(all(
            queue["hybridpatch_first"] == queue["worker_count"]
            and queue["fullrewrite_first"] == 0
            for queue in manifest["assignment_queues"]
        ))
        self.assertEqual(manifest["config"]["key_count"], 14)
        self.assertEqual(manifest["config"]["max_worker_count"], 56)
        self.assertEqual(manifest["config"]["queued_worker_count"], 78)
        self.assertEqual(manifest["config"]["total_worker_invocations"], 268)
        self.assertEqual(
            manifest["config"]["method_phases"],
            ["hybridpatch", "fullrewrite"])

    def test_remaining134_runs_all_hp_before_any_fr_and_persists_barrier(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": f"KEY_0{index}",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for index, sample in enumerate(samples, 1)
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=False, resume_reason=None,
            confirm_workers_stopped=False,
        )
        phases = []

        def fake_queue(
                _args, out_dir, _manifest, _plans, _keys, phase_items,
                _authorizations, _dispatch_log, _running, _infra, _eval,
                completed, _total, _slots):
            phase = phase_items[0]["method_phase"]
            phases.append(phase)
            if phase == "fullrewrite":
                paired_dispatch._require_hybridpatch_phase_barrier(
                    out_dir, manifest)
                hp = paired_dispatch._latest_sample_outcomes(
                    out_dir, samples, method_phase="hybridpatch")
                self.assertEqual(set(hp), set(samples))
            for item in phase_items:
                run_meta.record_sample_outcome(
                    out_dir, item["sample"], "finished",
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})
                completed.add(item["sample"])

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value={"errors": [], "api_calls": 0,
                              "preservation_violations": 0}), mock.patch.object(
                    paired_dispatch, "_run_worker_queue",
                    side_effect=fake_queue):
            result = paired_dispatch._run_remaining134_campaign(
                args, out_dir, manifest, {}, {}, assignments,
                os.path.join(out_dir, "dispatch_log.jsonl"), {})
            rows = paired_dispatch._read_jsonl(
                os.path.join(out_dir, "dispatch_log.jsonl"))
        self.assertEqual(result, 0)
        self.assertEqual(phases, ["hybridpatch", "fullrewrite"])
        hp_barrier_index = next(
            index for index, row in enumerate(rows)
            if row.get("event") == "method_phase_complete"
            and row.get("method_phase") == "hybridpatch")
        fr_start_index = next(
            index for index, row in enumerate(rows)
            if row.get("event") == "method_phase_start"
            and row.get("method_phase") == "fullrewrite")
        self.assertLess(hp_barrier_index, fr_start_index)

    def test_remaining134_resume_reuses_hp_barrier_and_runs_only_fr(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": f"KEY_0{index}",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for index, sample in enumerate(samples, 1)
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=True,
            resume_reason="continue unfinished FR only",
            confirm_workers_stopped=True,
        )
        phases = []

        def fake_phase(
                _args, _out_dir, _manifest, _plans, _keys,
                _assignments, method_phase, **_kwargs):
            phases.append(method_phase)
            self.assertEqual(method_phase, "fullrewrite")
            return ({
                "finished": set(samples),
                "evaluator_incomplete": set(),
                "infrastructure_incomplete": set(),
                "missing": set(),
            }, True)

        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            run_meta.append_jsonl_locked(dispatch_log, {
                "schema": paired_dispatch.METHOD_PHASE_COMPLETE_SCHEMA,
                "event": "method_phase_complete",
                "method_phase": "hybridpatch",
                "next_method_phase": "fullrewrite",
                "eligible_sample_count": len(samples),
                "eligible_sample_ids_sha256": (
                    paired_dispatch._sample_ids_sha256(samples)),
                "finished_samples": samples,
                "evaluator_incomplete_samples": [],
                "phase_api_calls": 20,
                "preservation_violations": 0,
                "run_git_commit": "1" * 40,
            })
            with mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={"errors": [], "api_calls": 0,
                                  "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_run_remaining134_phase",
                        side_effect=fake_phase):
                result = paired_dispatch._run_remaining134_campaign(
                    args, out_dir, manifest, {}, {}, assignments,
                    dispatch_log, {})
            rows = paired_dispatch._read_jsonl(dispatch_log)

        self.assertEqual(result, 0)
        self.assertEqual(phases, ["fullrewrite"])
        reused = [
            row for row in rows
            if row.get("event") == "method_phase_resume_reused"
        ]
        self.assertEqual(len(reused), 1)
        self.assertEqual(reused[0]["method_phase"], "hybridpatch")

    def test_remaining134_hp_infrastructure_failure_never_launches_fr(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for sample in samples
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=False, resume_reason=None,
            confirm_workers_stopped=False,
        )
        phases = []

        def fake_queue(
                _args, out_dir, _manifest, _plans, _keys, phase_items,
                _authorizations, _dispatch_log, _running, _infra, _eval,
                _completed, _total, _slots):
            phase = phase_items[0]["method_phase"]
            phases.append(phase)
            for item in phase_items:
                status = (
                    "infrastructure_incomplete"
                    if item["sample"] == "sample-a" else "finished")
                run_meta.record_sample_outcome(
                    out_dir, item["sample"], status,
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value={"errors": [], "api_calls": 0,
                              "preservation_violations": 0}), mock.patch.object(
                    paired_dispatch, "_run_worker_queue",
                    side_effect=fake_queue):
            result = paired_dispatch._run_remaining134_campaign(
                args, out_dir, manifest, {}, {}, assignments,
                os.path.join(out_dir, "dispatch_log.jsonl"), {})
            barriers = paired_dispatch._method_phase_complete_events(
                out_dir, "hybridpatch")
        self.assertEqual(result, 2)
        self.assertEqual(phases, ["hybridpatch"])
        self.assertEqual(barriers, [])

    def test_remaining134_hp_evaluator_incomplete_skips_only_its_fr(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for sample in samples
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=False, resume_reason=None,
            confirm_workers_stopped=False,
        )
        phase_samples = []

        def fake_queue(
                _args, out_dir, _manifest, _plans, _keys, phase_items,
                _authorizations, _dispatch_log, _running, _infra, _eval,
                _completed, _total, _slots):
            phase = phase_items[0]["method_phase"]
            launched = [item["sample"] for item in phase_items]
            phase_samples.append((phase, launched))
            for item in phase_items:
                status = (
                    "evaluator_incomplete"
                    if phase == "hybridpatch"
                    and item["sample"] == "sample-a" else "finished")
                run_meta.record_sample_outcome(
                    out_dir, item["sample"], status,
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value={"errors": [], "api_calls": 0,
                              "preservation_violations": 0}), mock.patch.object(
                    paired_dispatch, "_run_worker_queue",
                    side_effect=fake_queue):
            result = paired_dispatch._run_remaining134_campaign(
                args, out_dir, manifest, {}, {}, assignments,
                os.path.join(out_dir, "dispatch_log.jsonl"), {})
            fr_outcomes = paired_dispatch._latest_sample_outcomes(
                out_dir, method_phase="fullrewrite")
        self.assertEqual(result, 2)
        self.assertEqual(
            phase_samples,
            [("hybridpatch", samples), ("fullrewrite", ["sample-b"])])
        self.assertNotIn("sample-a", fr_outcomes)
        self.assertEqual(fr_outcomes["sample-b"]["status"], "finished")

    def test_phase_resume_skips_committed_sample_and_keeps_pending_sample(self):
        sample_done = "sample-done"
        sample_pending = "sample-pending"
        assignments = [
            {"sample": sample_done, "key_label": "KEY_01",
             "methods": ["fullrewrite"], "method_phase": "fullrewrite"},
            {"sample": sample_pending, "key_label": "KEY_02",
             "methods": ["fullrewrite"], "method_phase": "fullrewrite"},
        ]
        expected_progress = {
            "fullrewrite": {
                "completed_round_trips": 10,
                "committed_rows": 20,
            }
        }
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_completed_method_prefix(
                out_dir, sample_done, "fullrewrite", 10)
            run_meta.record_sample_outcome(
                out_dir, sample_done, "finished",
                methods=["fullrewrite"], method_phase="fullrewrite",
                checkpoint_progress=expected_progress)
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=10, allow_pristine_pending=True,
                    method_phase="fullrewrite"))
            root = (
                "fullrewrite/sample-pending/rt01/forward/"
                "fullrewrite_primary")
            self._write_success_journal(
                out_dir, root=root, exact_id=f"{root}/g000",
                request_id="unexpected-fr", fingerprint="fingerprint")
            with self.assertRaisesRegex(
                    RuntimeError, "method phase was never started"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=10, allow_pristine_pending=True,
                    method_phase="fullrewrite")
        self.assertEqual(
            [item["sample"] for item in selected], [sample_pending])
        self.assertEqual(authorizations, {})

    def test_fullrewrite_worker_launch_requires_hp_phase_barrier(self):
        sample = "sample-a"
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": [sample]},
        }
        assignment = {
            "sample": sample, "key_label": "KEY_01",
            "methods": ["fullrewrite"], "method_phase": "fullrewrite",
            "console_log": (
                "dispatch_logs/sample-a__KEY_01__fullrewrite.console.log"),
        }
        args = mock.Mock(
            num_round_trips=10, seed=42, notes="unit", start_timeout=1)
        with tempfile.TemporaryDirectory() as out_dir:
            os.makedirs(os.path.join(out_dir, "dispatch_logs"))
            task_plans = {
                sample: {"path": "sample-a.task_plan.json",
                         "sha256": "a" * 64}
            }
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen") as popen:
                with self.assertRaisesRegex(RuntimeError, "phase barrier"):
                    paired_dispatch._launch_worker_batch(
                        args, out_dir, manifest, task_plans,
                        {"KEY_01": "redacted"}, [assignment], {},
                        os.path.join(out_dir, "dispatch_log.jsonl"), {})
            popen.assert_not_called()

            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"), {
                    "schema": paired_dispatch.METHOD_PHASE_COMPLETE_SCHEMA,
                    "event": "method_phase_complete",
                    "method_phase": "hybridpatch",
                    "next_method_phase": "fullrewrite",
                    "eligible_sample_count": 1,
                    "eligible_sample_ids_sha256": (
                        paired_dispatch._sample_ids_sha256([sample])),
                    "finished_samples": [sample],
                    "evaluator_incomplete_samples": [],
                    "preservation_violations": 0,
                    "run_git_commit": "1" * 40,
                })

            class FakeProcess:
                pid = 12345

                @staticmethod
                def poll():
                    return None

            running = {}
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen",
                    return_value=FakeProcess()) as popen, mock.patch.object(
                        paired_dispatch, "_authorize_workers"):
                launched = paired_dispatch._launch_worker_batch(
                    args, out_dir, manifest, task_plans,
                    {"KEY_01": "redacted"}, [assignment], {},
                    os.path.join(out_dir, "dispatch_log.jsonl"), running)
            self.assertEqual(launched, [sample])
            popen.assert_called_once()
            self.assertEqual(
                popen.call_args.kwargs["env"]["ANCHORPATCH_METHOD_PHASE"],
                "fullrewrite")
            running[sample]["log"].close()

    def test_phase_outcomes_allow_hp_then_fr_but_reject_duplicate_terminal(self):
        with tempfile.TemporaryDirectory() as out_dir:
            for phase in ("hybridpatch", "fullrewrite"):
                run_meta.record_sample_outcome(
                    out_dir, "sample-a", "finished",
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})
            self.assertEqual(
                paired_dispatch._latest_sample_outcomes(
                    out_dir, ["sample-a"],
                    method_phase="hybridpatch")["sample-a"]["status"],
                "finished")
            self.assertEqual(
                paired_dispatch._latest_sample_outcomes(
                    out_dir, ["sample-a"],
                    method_phase="fullrewrite")["sample-a"]["status"],
                "finished")
            run_meta.record_sample_outcome(
                out_dir, "sample-a", "infrastructure_incomplete",
                methods=["fullrewrite"], method_phase="fullrewrite",
                checkpoint_progress={})
            with self.assertRaisesRegex(
                    RuntimeError, "after finished state"):
                paired_dispatch._latest_sample_outcomes(
                    out_dir, ["sample-a"],
                    method_phase="fullrewrite")

    def test_strict_inspector_auto_aggregates_declared_method_phases(self):
        sample = "sample-a"
        args = mock.Mock(
            campaign_role="remaining134", num_round_trips=10, seed=42,
            slots_per_key=4,
        )
        args._remaining134_scope_record = {
            "schema": "anchorpatch.remaining134_scope/1",
            "sample_count": 1,
            "sample_ids": [sample],
        }
        assignments = [{
            "sample": sample, "key_label": "KEY_01",
            "methods": ["hybridpatch", "fullrewrite"],
            "console_log": "dispatch_logs/sample-a.log",
        }]
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                out_dir, [sample], assignments, {}, args)
            paired_dispatch._write_active_worker_set(out_dir, manifest, [])
            inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
            self.assertEqual(inspection["errors"], [])
            manifest["config"]["method_phases"] = ["fullrewrite"]
            with self.assertRaisesRegex(RuntimeError, "phases are invalid"):
                paired_dispatch.inspect_campaign(out_dir, manifest)

    def test_preflight_require_plans_fails_on_missing_frozen_plan(self):
        with tempfile.TemporaryDirectory() as out_dir, \
                tempfile.TemporaryDirectory() as plans_from, \
                mock.patch.object(
                    fr_baseline_dispatch.subprocess, "Popen") as popen:
            passed = fr_baseline_dispatch.preflight(
                ["treebank4"], [("KEY_01", "redacted")],
                out_dir, plans_from, skip_probe=False,
                require_plans=True,
            )
        self.assertFalse(passed)
        popen.assert_not_called()

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

    def test_campaign_stop_read_does_not_contend_on_metadata_writer_lock(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lock_path = os.path.join(out_dir, ".run_metadata.lock")
            with open(lock_path, "a+", encoding="utf-8") as writer:
                portalocker.lock(
                    writer, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    errors = []
                    results = []

                    def read_latch():
                        try:
                            results.append(
                                run_meta.read_campaign_stop_conditions(out_dir))
                        except BaseException as exc:
                            errors.append(exc)

                    readers = [
                        threading.Thread(target=read_latch, daemon=True)
                        for _ in range(40)
                    ]
                    for reader in readers:
                        reader.start()
                    for reader in readers:
                        reader.join(2)
                finally:
                    portalocker.unlock(writer)
            self.assertTrue(all(not reader.is_alive() for reader in readers))
            self.assertEqual(errors, [])
            self.assertEqual(results, [[]] * 40)

    def test_campaign_metadata_writer_waits_for_existing_writer(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lock_path = os.path.join(out_dir, ".run_metadata.lock")
            entered = threading.Event()
            errors = []
            with open(lock_path, "a+", encoding="utf-8") as first_writer:
                portalocker.lock(
                    first_writer, portalocker.LOCK_EX | portalocker.LOCK_NB)

                def acquire_after_contention():
                    try:
                        with run_meta._campaign_metadata_lock(out_dir):
                            entered.set()
                    except BaseException as exc:
                        errors.append(exc)

                contender = threading.Thread(
                    target=acquire_after_contention, daemon=True)
                contender.start()
                self.assertFalse(entered.wait(0.15))
                portalocker.unlock(first_writer)
                contender.join(3)
            self.assertFalse(contender.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(entered.is_set())

    def test_shared_jsonl_writer_waits_for_existing_writer(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
            appended = threading.Event()
            errors = []
            with open(path, "a+", encoding="utf-8") as first_writer:
                portalocker.lock(
                    first_writer,
                    portalocker.LOCK_EX | portalocker.LOCK_NB)

                def append_after_contention():
                    try:
                        run_meta.append_jsonl_locked(path, {"value": 1})
                        appended.set()
                    except BaseException as exc:
                        errors.append(exc)

                contender = threading.Thread(
                    target=append_after_contention, daemon=True)
                contender.start()
                self.assertFalse(appended.wait(0.15))
                portalocker.unlock(first_writer)
                contender.join(3)
            self.assertFalse(contender.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(appended.is_set())
            self.assertEqual(
                run_meta._read_jsonl_records_with_retry(path),
                [{"value": 1}],
            )

    def test_run_metadata_atomic_replace_retries_windows_sharing_violation(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "run_metadata.jsonl")
            real_replace = run_meta.os.replace
            attempts = []

            def flaky_replace(source, target):
                attempts.append((source, target))
                if len(attempts) == 1:
                    error = PermissionError(13, "transient sharing violation")
                    error.winerror = 5
                    raise error
                return real_replace(source, target)

            with mock.patch.object(
                    run_meta.os, "replace",
                    side_effect=flaky_replace), mock.patch.object(
                    run_meta.time, "sleep") as sleep:
                run_meta._write_jsonl_atomic(path, [{"value": 1}])

            self.assertEqual(len(attempts), 2)
            sleep.assert_called_once_with(0.05)
            self.assertEqual(
                run_meta._read_jsonl_records_with_retry(path),
                [{"value": 1}],
            )

            json_path = os.path.join(out_dir, "active_worker_set.json")
            attempts.clear()
            with mock.patch.object(
                    run_meta.os, "replace",
                    side_effect=flaky_replace), mock.patch.object(
                    run_meta.time, "sleep") as sleep:
                run_meta.write_json_atomic(json_path, {"workers": {}})
            self.assertEqual(len(attempts), 2)
            sleep.assert_called_once_with(0.05)
            with open(json_path, encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), {"workers": {}})

    def test_api_recorder_filters_only_authorized_lock_incident_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            semantic = (
                "fullrewrite/sample/rt01/forward/fullrewrite_primary/g000"
            )
            context = run_meta._semantic_lineage_fields(semantic)
            incident = {
                "schema": run_meta.API_ATTEMPT_SCHEMA,
                "created_local": "2026-07-21T18:00:00",
                "step_id": context["step_id"],
                "semantic_root_id": context["semantic_root_id"],
                "semantic_call_id": semantic,
                "generation_index": 0,
                "parent_semantic_call_id": None,
                "worker_launch_id": "old-worker",
                "event": "semantic_request",
                "call_id": "old-call",
                "call_kind": "fullrewrite_primary",
                "request_fingerprint": "old-fingerprint",
            }
            path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
            run_meta.append_jsonl_locked(path, incident)
            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", mock.Mock())
            self.assertEqual(
                recorder._ledger_state(semantic)["request_fingerprints"],
                ["old-fingerprint"],
            )
            recorder._authorized_attempt_incident_hashes = {
                run_meta._canonical_record_sha256(incident)
            }
            state = recorder._ledger_state(semantic)
            self.assertEqual(state["request_fingerprints"], [])
            self.assertEqual(state["http_attempts_used"], 0)

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

    def test_paired_dispatch_resume_accepts_only_authorized_git_transition(self):
        prior = {
            "schema": paired_dispatch.SCHEMA,
            "experiment_id": "exp_test",
            "run_git_commit": "1" * 40,
            "git_tree_state": "clean",
            "code_fingerprint": {"run_meta.py": "old"},
            "config": {"samples": ["sample"]},
            "assignments": [{
                "sample": "sample", "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
                "console_log": "dispatch_logs/sample__KEY_01.console.log",
            }],
            "task_plans": {"sample": {"sha256": "a" * 64}},
        }
        recovered = copy.deepcopy(prior)
        recovered["run_git_commit"] = "2" * 40
        recovered["code_fingerprint"] = {"run_meta.py": "new"}
        authorization = {
            "prior_git_commit": prior["run_git_commit"],
            "recovery_git_commit": recovered["run_git_commit"],
        }
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(path, prior)
            with mock.patch.object(
                    paired_dispatch, "read_campaign_recovery_authorization",
                    return_value=authorization):
                found_path, found_manifest = (
                    paired_dispatch.write_or_verify_manifest(
                        out_dir, recovered, resume=True)
                )
                self.assertEqual(found_path, path)
                self.assertEqual(found_manifest, prior)

                changed_config = copy.deepcopy(recovered)
                changed_config["config"]["samples"] = ["other"]
                with self.assertRaises(RuntimeError):
                    paired_dispatch.write_or_verify_manifest(
                        out_dir, changed_config, resume=True)

    def test_confirmation_resume_identity_ignores_wave_key_label_names(self):
        prior = {
            "schema": paired_dispatch.SCHEMA,
            "experiment_id": "exp_test",
            "run_git_commit": "1" * 40,
            "git_tree_state": "clean",
            "code_fingerprint": {"x": "y"},
            "config": {
                "campaign_role": "confirmation",
                "samples": ["sample-a", "sample-b"],
            },
            "assignments": [
                {
                    "sample": "sample-a", "key_label": "KEY_01",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "console_log": (
                        "dispatch_logs/sample-a__KEY_01.console.log"),
                },
                {
                    "sample": "sample-b", "key_label": "KEY_02",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "console_log": (
                        "dispatch_logs/sample-b__KEY_02.console.log"),
                },
            ],
            "assignment_waves": [{
                "wave_index": 1,
                "sample_ids": ["sample-a", "sample-b"],
                "worker_count": 2,
                "key_worker_counts": {"KEY_01": 1, "KEY_02": 1},
                "hybridpatch_first": 1,
                "fullrewrite_first": 1,
            }],
            "assignment_queues": [
                {
                    "key_label": "KEY_01", "sample_ids": ["sample-a"],
                    "worker_count": 1, "hybridpatch_first": 1,
                    "fullrewrite_first": 0,
                },
                {
                    "key_label": "KEY_02", "sample_ids": ["sample-b"],
                    "worker_count": 1, "hybridpatch_first": 0,
                    "fullrewrite_first": 1,
                },
            ],
            "task_plans": {
                "sample-a": {"sha256": "a" * 64},
                "sample-b": {"sha256": "b" * 64},
            },
        }
        rotated = json.loads(json.dumps(prior))
        for index, label in enumerate(("KEY_11", "KEY_12")):
            rotated["assignments"][index]["key_label"] = label
            rotated["assignments"][index]["console_log"] = (
                f"dispatch_logs/sample-{chr(ord('a') + index)}__"
                f"{label}.console.log"
            )
        rotated["assignment_waves"][0]["key_worker_counts"] = {
            "KEY_11": 1, "KEY_12": 1}
        rotated["assignment_queues"][0]["key_label"] = "KEY_11"
        rotated["assignment_queues"][1]["key_label"] = "KEY_12"
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(path, prior)
            found_path, found_manifest = (
                paired_dispatch.write_or_verify_manifest(
                    out_dir, rotated, resume=True)
            )
            self.assertEqual(found_path, path)
            self.assertEqual(found_manifest, prior)

            bad_rotation = json.loads(json.dumps(rotated))
            bad_rotation["assignment_waves"][0]["key_worker_counts"] = {
                "KEY_11": 2}
            with self.assertRaises(RuntimeError):
                paired_dispatch.write_or_verify_manifest(
                    out_dir, bad_rotation, resume=True)

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
                    out_dir, running, "sample", running["sample"], 7,
                    dispatch_log
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
            run_meta.append_jsonl_locked(dispatch_log, {
                "event": "launch_intent",
                "worker_launch_id": "worker-a",
                "sample": "sample",
            })
            run_meta.append_jsonl_locked(dispatch_log, {
                "event": "launch",
                "worker_launch_id": "worker-a",
                "sample": "sample",
                "pid": 101,
            })
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
                    out_dir, running, "sample", running["sample"], 7,
                    dispatch_log
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
                    paired_dispatch,
                    "_audit_running_invocation_provenance",
                    return_value=[{
                        "invocation_id": "invocation-a",
                        "worker_launch_id": "worker-a",
                        "worker_pid": 101,
                        "sample": "sample",
                    }],
                ) as audit, \
                mock.patch.object(
                    paired_dispatch,
                    "interrupt_audited_running_invocations",
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
        leases_free.assert_called_once_with(out_dir, ["sample"])
        audit.assert_called_once_with(out_dir)
        interrupt.assert_called_once_with(
            out_dir,
            status="interrupted_by_dispatcher",
            audited=[{
                "invocation_id": "invocation-a",
                "worker_launch_id": "worker-a",
                "worker_pid": 101,
                "sample": "sample",
            }],
        )

    def test_reconcile_full_lease_scope_preserves_live_orphan_metadata(self):
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_terminate_workers") as terminate, \
                mock.patch.object(
                    paired_dispatch, "_assert_worker_leases_free",
                    side_effect=RuntimeError("worker lease still held: orphan"),
                ) as leases_free, \
                mock.patch.object(
                    paired_dispatch,
                    "_audit_running_invocation_provenance",
                ) as audit, \
                mock.patch.object(
                    paired_dispatch,
                    "interrupt_audited_running_invocations",
                ) as interrupt:
            result = paired_dispatch._stop_and_reconcile_workers(
                out_dir, {}, lease_scope=["orphan"])
        terminate.assert_called_once_with({})
        leases_free.assert_called_once_with(out_dir, ["orphan"])
        self.assertIn("still held", result["lease_error"])
        self.assertEqual(result["lease_scope"], ["orphan"])
        self.assertEqual(result["closed_invocations"], [])
        audit.assert_not_called()
        interrupt.assert_not_called()

    def test_dispatch_retains_active_worker_until_lease_and_metadata_close(self):
        class FakeProcess:
            pid = 101

            @staticmethod
            def poll():
                return None

        def make_task_plans(out_dir, samples, *_args):
            plans = {}
            for sample in samples:
                plan_path = os.path.join(
                    out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(
                    plan_path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        for hold_lease in (True, False):
            with self.subTest(hold_lease=hold_lease), \
                    tempfile.TemporaryDirectory() as out_dir:
                args = mock.Mock(
                    campaign_role="smoke", smoke_dir=None,
                    samples=["sample"], key_labels=["KEY_01"],
                    keys_file="unused.env", num_round_trips=1, seed=42,
                    dry_run=False, resume=False, start_timeout=0.1,
                    poll_interval=0, progress_interval=9999,
                    notes="unit",
                )
                held_lease = []
                latch_seen_before_termination = []

                def fake_popen(_command, **kwargs):
                    worker_id = kwargs["env"][
                        "ANCHORPATCH_WORKER_LAUNCH_ID"]
                    run_meta._write_jsonl_atomic(
                        os.path.join(out_dir, "run_metadata.jsonl"),
                        [{
                            "invocation_id": f"invocation-{worker_id}",
                            "worker_launch_id": worker_id,
                            "worker_pid": 101,
                            "samples": ["sample"],
                            "status": "running",
                            "invocation_finished_at": None,
                            "finished_at": None,
                        }],
                    )
                    if hold_lease:
                        lease = open(
                            kwargs["env"]["ANCHORPATCH_WORKER_LOCK_PATH"],
                            "a+", encoding="utf-8")
                        portalocker.lock(
                            lease,
                            portalocker.LOCK_EX | portalocker.LOCK_NB)
                        held_lease.append(lease)
                    return FakeProcess()

                def fail_termination(_running):
                    latch_seen_before_termination.extend(
                        run_meta.read_campaign_stop_conditions(out_dir))
                    raise RuntimeError("terminate/kill failed")

                inspections = iter([
                    {"errors": [], "api_calls": 0,
                     "preservation_violations": 0},
                    {"errors": ["forced global integrity error"],
                     "api_calls": 0, "preservation_violations": 0},
                ])
                try:
                    with mock.patch.object(
                            paired_dispatch,
                            "_validate_campaign_grid"), \
                            mock.patch.object(
                                paired_dispatch,
                                "_require_formal_opencode_transport"), \
                            mock.patch.object(
                                paired_dispatch, "read_keys",
                                return_value={"KEY_01": "redacted"}), \
                            mock.patch.object(
                                paired_dispatch, "prepare_task_plans",
                                side_effect=make_task_plans), \
                            mock.patch.object(
                                paired_dispatch, "_git_identity",
                                return_value=("1" * 40, "clean")), \
                            mock.patch.object(
                                paired_dispatch, "code_fingerprint",
                                return_value={"unit": "test"}), \
                            mock.patch.object(
                                paired_dispatch, "inspect_campaign",
                                side_effect=lambda *_args, **_kwargs: next(
                                    inspections)), \
                            mock.patch.object(
                                paired_dispatch, "_authorize_workers"), \
                            mock.patch.object(
                                paired_dispatch.subprocess, "Popen",
                                side_effect=fake_popen), \
                            mock.patch.object(
                                paired_dispatch, "_terminate_workers",
                                side_effect=fail_termination), \
                            mock.patch.object(
                                paired_dispatch.time, "sleep"):
                        result = paired_dispatch._launch_under_lease(
                            args, out_dir)
                finally:
                    for lease in held_lease:
                        portalocker.unlock(lease)
                        lease.close()

                self.assertEqual(result, 1)
                self.assertEqual(len(latch_seen_before_termination), 1)
                self.assertEqual(
                    latch_seen_before_termination[0]["condition"],
                    "dispatcher_integrity_failure")
                self.assertEqual(
                    latch_seen_before_termination[0]["error_type"],
                    "RuntimeError")
                self.assertIn(
                    "forced global integrity error",
                    latch_seen_before_termination[0]["error"])
                self.assertEqual(
                    run_meta.read_campaign_stop_conditions(out_dir),
                    latch_seen_before_termination)
                with open(
                    paired_dispatch._active_worker_set_path(out_dir),
                    encoding="utf-8",
                ) as handle:
                    active = json.load(handle)
                dispatch_rows = run_meta._read_jsonl_records_with_retry(
                    os.path.join(out_dir, "dispatch_log.jsonl"))
                stop = next(
                    row for row in reversed(dispatch_rows)
                    if row.get("event") == "campaign_stop")
                reconciliation = stop["worker_reconciliation"]
                self.assertEqual(
                    reconciliation["termination_error"],
                    "terminate/kill failed")
                metadata = run_meta.read_run_metadata_snapshot(out_dir)

                if hold_lease:
                    self.assertEqual(
                        [item["sample"]
                         for item in active["workers"].values()],
                        ["sample"])
                    self.assertIn(
                        "sample", reconciliation["lease_error"])
                    self.assertEqual(
                        reconciliation["active_set_retained"],
                        ["sample"])
                    self.assertNotIn(
                        "audited_invocations", reconciliation)
                    self.assertEqual(metadata[0]["status"], "running")
                else:
                    self.assertEqual(active["workers"], {})
                    self.assertIsNone(reconciliation["lease_error"])
                    self.assertEqual(
                        len(reconciliation["closed_invocations"]), 1)
                    self.assertEqual(
                        reconciliation["audited_invocations"][0]["sample"],
                        "sample")
                    self.assertEqual(
                        reconciliation["audited_invocations"][0][
                            "worker_pid"],
                        101)
                    self.assertNotIn("active_set_retained", reconciliation)
                    self.assertEqual(
                        metadata[0]["status"],
                        "interrupted_by_dispatcher")

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

    def test_stop_latched_during_transport_preflight_blocks_provider_post(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", provider)
            recorder.set_step(1, "forward", "target")

            def latch_after_ledger_preflight(*_args, **_kwargs):
                run_meta.record_campaign_stop_condition(
                    out_dir, "sibling_integrity_failure",
                    sample="sibling")
                return None, None

            with mock.patch.object(
                    recorder, "_find_lineage_journal",
                    side_effect=latch_after_ledger_preflight):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3",
                        call_kind="hybridpatch_primary")

            provider.assert_not_called()
            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            self.assertFalse(any(
                row.get("event") == "attempt_start" for row in ledger))

    def test_stop_latched_during_backoff_blocks_retry_provider_post(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider_posts = []
            first_failure = model_openai._HTTPStatusError(
                503, "temporary service failure")

            def fake_transport_call(*_args, **kwargs):
                attempt_index = kwargs["attempt_index"]
                sink = kwargs["raw_event_sink"]
                sink({
                    "record_type": "attempt_start",
                    "attempt_index": attempt_index,
                    "attempt_kind": (
                        "transport_initial" if attempt_index == 1
                        else "transport_retry"),
                })
                # The SDK emits attempt_start immediately before opening the
                # stream; reaching here represents an actual provider POST.
                provider_posts.append(attempt_index)
                if attempt_index == 1:
                    attempt = {
                        "attempt_index": 1,
                        **self._failed_attempt_end_fields(
                            generation_delta_seen=False,
                            error_type="server_error"),
                        "http_status": 503,
                    }
                    first_failure._opencode_attempt = attempt
                    sink({"record_type": "attempt_end", "attempt": attempt})
                    raise first_failure
                raise AssertionError("retry POST must be stopped by latch")

            def latch_during_backoff(_delay):
                run_meta.record_campaign_stop_condition(
                    out_dir, "sibling_integrity_failure",
                    sample="sibling")

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                model_openai.OpenAI_Model().generate)
            recorder.set_step(1, "forward", "target")
            with mock.patch.object(
                    model_openai, "_call_opencode_messages",
                    side_effect=fake_transport_call) as transport_call, \
                    mock.patch.object(
                        model_openai.time, "sleep",
                        side_effect=latch_during_backoff):
                with self.assertRaises(model_openai.OpenCodeTransportError):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3", return_metadata=True,
                        call_kind="hybridpatch_primary")

            self.assertEqual(transport_call.call_count, 2)
            self.assertEqual(provider_posts, [1])
            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            self.assertEqual(
                [row.get("event") for row in ledger].count(
                    "attempt_start"),
                1)
            self.assertFalse(any(
                row.get("event") == "response_committed" for row in ledger))

    def test_stop_latch_wins_commit_ordering_lock_with_zero_relay_commit(self):
        with tempfile.TemporaryDirectory() as out_dir:
            result_path = os.path.join(out_dir, "hybridpatch", "sample.jsonl")
            checkpoint_path = os.path.join(
                out_dir, "hybridpatch", "sample.ckpt.json")
            rows = [
                {"round_trip_num": 1,
                 "round_trip_direction": "forward"},
                {"round_trip_num": 1,
                 "round_trip_direction": "backward"},
            ]
            checkpoint = {"completed_round_trips": 1}
            stop_has_ordering_lock = threading.Event()
            allow_stop_write = threading.Event()
            stop_errors = []
            commit_errors = []
            original_write = run_meta.write_json_atomic

            def blocked_stop_write(path, payload):
                if os.path.basename(path) == "campaign_stop.json":
                    stop_has_ordering_lock.set()
                    if not allow_stop_write.wait(5):
                        raise RuntimeError("unit-test stop barrier timed out")
                return original_write(path, payload)

            def stop_worker():
                try:
                    run_meta.record_campaign_stop_condition(
                        out_dir, "sibling_integrity_failure")
                except BaseException as exc:
                    stop_errors.append(exc)

            def commit_worker():
                try:
                    run_meta.append_relay_rows_and_checkpoint(
                        result_path, checkpoint_path, rows, checkpoint,
                        campaign_out_dir=out_dir)
                except BaseException as exc:
                    commit_errors.append(exc)

            with mock.patch.object(
                    run_meta, "write_json_atomic",
                    side_effect=blocked_stop_write):
                stopper = threading.Thread(target=stop_worker, daemon=True)
                stopper.start()
                self.assertTrue(stop_has_ordering_lock.wait(5))
                committer = threading.Thread(target=commit_worker, daemon=True)
                committer.start()
                self.assertTrue(committer.is_alive())
                allow_stop_write.set()
                stopper.join(5)
                committer.join(5)

            self.assertFalse(stopper.is_alive())
            self.assertFalse(committer.is_alive())
            self.assertEqual(stop_errors, [])
            self.assertEqual(len(commit_errors), 1)
            self.assertIsInstance(
                commit_errors[0], run_meta.CampaignStoppedError)
            self.assertFalse(os.path.exists(result_path))
            self.assertFalse(os.path.exists(checkpoint_path))
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0][
                    "condition"],
                "sibling_integrity_failure")

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

    def test_worker_authorization_retries_transient_active_set_read(self):
        with tempfile.TemporaryDirectory() as out_dir:
            worker_id = "worker-a"
            active_path = os.path.join(out_dir, "active_worker_set.json")
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "workers": {worker_id: {"sample": "sample"}},
            })
            real_open = open
            reads = []

            def flaky_open(path, *args, **kwargs):
                if os.path.abspath(path) == os.path.abspath(active_path):
                    reads.append(path)
                    if len(reads) == 1:
                        error = PermissionError(
                            13, "transient sharing violation")
                        error.winerror = 5
                        raise error
                return real_open(path, *args, **kwargs)

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_WORKER_LAUNCH_ID": worker_id,
                "ANCHORPATCH_ACTIVE_WORKER_SET_PATH": active_path,
            }, clear=False), mock.patch(
                "builtins.open", side_effect=flaky_open
            ), mock.patch.object(run_meta.time, "sleep") as sleep:
                run_meta.enforce_active_worker_authorization(
                    out_dir, "sample")
            self.assertEqual(len(reads), 2)
            sleep.assert_called_once_with(0.05)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

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
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
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

    def test_confirmation_known_usage_gate_enforces_limit_and_known_rows(self):
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "config": {
                "campaign_role": "confirmation",
                "samples": ["sample"],
                "method_set": ["hybridpatch"],
            },
            "analysis_policy": copy.deepcopy(
                paired_dispatch.CONFIRMATION_ANALYSIS_POLICY),
        }

        def write_result_rows(out_dir, rows):
            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            with open(
                os.path.join(method_dir, "sample.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")

        with tempfile.TemporaryDirectory() as out_dir:
            write_result_rows(out_dir, [{"total_usd": 129.5}])
            report = paired_dispatch.evaluate_confirmation_known_usage_gate(
                out_dir, manifest, wave_index=1)
            self.assertEqual(report["decision"], "GO")
            self.assertEqual(report["rows_observed"], 1)
            self.assertEqual(report["usage_rows"], 1)
            self.assertEqual(report["known_committed_usage_usd"], 129.5)
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertEqual(reports[-1]["decision"], "GO")

        with tempfile.TemporaryDirectory() as out_dir:
            write_result_rows(out_dir, [{}])
            with self.assertRaisesRegex(
                    RuntimeError, "committed_usage_unknown"):
                paired_dispatch.evaluate_confirmation_known_usage_gate(
                    out_dir, manifest, wave_index=1)
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertIn(
                "committed_usage_unknown", reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

        with tempfile.TemporaryDirectory() as out_dir:
            write_result_rows(out_dir, [{"total_usd": 131.0}])
            with self.assertRaisesRegex(
                    RuntimeError,
                    "known_committed_usage_threshold_exceeded"):
                paired_dispatch.evaluate_confirmation_known_usage_gate(
                    out_dir, manifest, wave_index=2)
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertIn(
                "known_committed_usage_threshold_exceeded",
                reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

    def test_confirmation_known_usage_no_go_blocks_resume_before_popen(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock()
            args.campaign_role = "confirmation"
            args.samples = []
            args.selection_manifest = "selection.json"
            args.smoke_dir = None
            args.num_round_trips = 10
            args.seed = 42
            args.slots_per_key = 4
            args.keys_file = "keys.env"
            args.key_labels = labels
            args.resume = True
            args.resume_reason = "retry after known usage stop"
            args.confirm_workers_stopped = True
            args.dry_run = False
            args.start_timeout = 1
            args.poll_interval = 0.01
            args.progress_interval = 999
            args.notes = "unit"
            args.skip_distractor = False

            method_dir = os.path.join(out_dir, "fullrewrite")
            os.makedirs(method_dir, exist_ok=True)
            with open(
                os.path.join(method_dir, f"{samples[0]}.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({"total_usd": 131.0}) + "\n")

            selection_record = {
                "experiment_id": os.path.basename(os.path.abspath(out_dir)),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "selection.json",
                "sha256": "1" * 64,
            }
            task_plans = {
                sample: {
                    "path": f"{sample}.task_plan.json",
                    "sha256": "2" * 64,
                    "forward_state_sequence": ["state"],
                }
                for sample in samples
            }

            def use_current_manifest(_out_dir, manifest, *, resume):
                self.assertTrue(resume)
                return os.path.join(out_dir, "dispatch_manifest.json"), manifest

            def select_all(_out_dir, assignments, **_kwargs):
                return list(assignments), {}

            with mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(samples, selection_record)), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={
                            label: f"unit-key-value-{index}"
                            for index, label in enumerate(labels)
                        }), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        return_value=task_plans), \
                    mock.patch.object(
                        paired_dispatch, "write_or_verify_manifest",
                        side_effect=use_current_manifest), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={
                            "errors": [], "api_calls": 0,
                            "preservation_violations": 0,
                        }), \
                    mock.patch.object(
                        paired_dispatch,
                        "_select_invocation_assignments",
                        side_effect=select_all), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen") as popen:
                self.assertEqual(
                    paired_dispatch._launch_under_lease(args, out_dir), 1)
                popen.assert_not_called()
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertEqual(reports[-1]["phase"], "pre_refill")
            self.assertIn(
                "known_committed_usage_threshold_exceeded",
                reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

    def test_confirmation_known_usage_blocks_all_finished_empty_resume(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock()
            args.campaign_role = "confirmation"
            args.samples = []
            args.selection_manifest = "selection.json"
            args.smoke_dir = None
            args.num_round_trips = 10
            args.seed = 42
            args.slots_per_key = 4
            args.keys_file = "keys.env"
            args.key_labels = labels
            args.resume = True
            args.resume_reason = "finish gate after crash"
            args.confirm_workers_stopped = True
            args.dry_run = False
            args.start_timeout = 1
            args.poll_interval = 0.01
            args.progress_interval = 999
            args.notes = "unit"
            args.skip_distractor = False

            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            with open(
                os.path.join(method_dir, f"{samples[0]}.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({"total_usd": 131.0}) + "\n")

            selection_record = {
                "experiment_id": os.path.basename(os.path.abspath(out_dir)),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "selection.json",
                "sha256": "1" * 64,
            }
            task_plans = {
                sample: {
                    "path": f"{sample}.task_plan.json",
                    "sha256": "2" * 64,
                    "forward_state_sequence": ["state"],
                }
                for sample in samples
            }

            def use_current_manifest(_out_dir, manifest, *, resume):
                self.assertTrue(resume)
                return os.path.join(out_dir, "dispatch_manifest.json"), manifest

            def select_none(_out_dir, _assignments, **_kwargs):
                return [], {}

            with mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(samples, selection_record)), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={
                            label: f"unit-key-value-{index}"
                            for index, label in enumerate(labels)
                        }), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        return_value=task_plans), \
                    mock.patch.object(
                        paired_dispatch, "write_or_verify_manifest",
                        side_effect=use_current_manifest), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={
                            "errors": [], "api_calls": 0,
                            "preservation_violations": 0,
                        }), \
                    mock.patch.object(
                        paired_dispatch,
                        "_select_invocation_assignments",
                        side_effect=select_none), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen") as popen:
                self.assertEqual(
                    paired_dispatch._launch_under_lease(args, out_dir), 1)
                popen.assert_not_called()
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertEqual(reports[-1]["phase"], "pre_final")
            self.assertIn(
                "known_committed_usage_threshold_exceeded",
                reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

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
            def __init__(self, preservation_violations):
                self.preservation_violations = preservation_violations

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
        def edit_result(preservation_violations):
            return (
                "raw", {"a.txt": "new"}, {},
                DummyExecLog(preservation_violations), "hybridpatch",
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
                ) as evaluate, \
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
                    return_value=edit_result(1),
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
            evaluate.assert_not_called()
            commit.assert_not_called()
            forward_stop = run_meta.read_campaign_stop_conditions(
                forward_dir)[0]
            self.assertEqual(
                forward_stop["condition"], "preservation_violation")
            self.assertEqual(forward_stop["direction"], "forward")
            self.assertFalse(forward_stop["result_committed"])

            edit_step.reset_mock()
            evaluate.reset_mock()
            commit.reset_mock()
            edit_step.side_effect = [
                edit_result(0), edit_result(1),
            ]
            make_row.side_effect = [
                {
                    "bdpatch": {"preservation_violations": 0},
                    "evaluation": {"score": 1.0},
                },
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
            self.assertEqual(evaluate.call_count, 1)
            commit.assert_not_called()
            backward_stop = run_meta.read_campaign_stop_conditions(
                backward_dir)[0]
            self.assertEqual(backward_stop["direction"], "backward")
            self.assertFalse(backward_stop["result_committed"])

    def test_sibling_stop_during_backward_generation_blocks_eval_and_commit(self):
        class DummyDomain:
            samples_folder = None

        class DummyExecLog:
            def to_dict(self):
                return {"ops_accepted": 1, "ops_total": 1}

        states = {
            "initial": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{
                    "target_state": "target", "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{
                    "target_state": "initial", "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}
        edit_result = (
            "raw", {"a.txt": "new"}, {}, DummyExecLog(), "hybridpatch",
            {"a.txt": "old"}, {},
        )
        backward_entered = threading.Event()
        release_backward = threading.Event()
        calls = []
        runner_errors = []

        def blocking_edit(*_args, **kwargs):
            direction = kwargs.get("step_direction")
            calls.append(direction)
            if direction == "backward":
                backward_entered.set()
                if not release_backward.wait(5):
                    raise RuntimeError("unit-test backward barrier timed out")
            return edit_result

        with tempfile.TemporaryDirectory() as out_dir, \
                tempfile.TemporaryDirectory() as sample_dir, \
                mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    experiment_runner, "load_sample",
                    return_value=(sample, sample_dir, states)), \
                mock.patch.object(
                    experiment_runner, "get_domain",
                    return_value=DummyDomain()), \
                mock.patch.object(
                    experiment_runner, "load_distractor_context",
                    return_value={}), \
                mock.patch.object(
                    experiment_runner, "build_context_from_folder",
                    return_value={"a.txt": "old"}), \
                mock.patch.object(
                    experiment_runner, "build_relay_task_plan",
                    return_value=["target"]), \
                mock.patch.object(
                    experiment_runner, "register_task_plan",
                    return_value={"sha256": "a" * 64,
                                  "round_trips": 1}), \
                mock.patch.object(
                    experiment_runner, "shuffle_context",
                    side_effect=lambda value: value), \
                mock.patch.object(
                    experiment_runner, "merge_distractor",
                    side_effect=lambda value, _distractor: value), \
                mock.patch.object(
                    experiment_runner, "_edit_step",
                    side_effect=blocking_edit), \
                mock.patch.object(
                    experiment_runner, "_evaluate",
                    return_value={"score": 1.0}) as evaluate, \
                mock.patch.object(
                    experiment_runner, "is_context_complete",
                    return_value=True), \
                mock.patch.object(experiment_runner, "dump_step_docs"), \
                mock.patch.object(
                    experiment_runner, "generate_response_id",
                    return_value="rid-fwd"), \
                mock.patch.object(
                    experiment_runner, "_row",
                    return_value={"evaluation": {"score": 1.0},
                                  "bdpatch": {}}), \
                mock.patch.object(
                    experiment_runner,
                    "append_relay_rows_and_checkpoint") as commit:

            def run_worker():
                try:
                    experiment_runner.run_relay(
                        "hybridpatch", "sample", num_round_trips=1,
                        include_distractor=True, out_dir=out_dir,
                        model="offline-test-model", max_tokens=16,
                        generate_fn=mock.Mock(), printing=False)
                except BaseException as exc:
                    runner_errors.append(exc)

            worker = threading.Thread(target=run_worker, daemon=True)
            worker.start()
            self.assertTrue(backward_entered.wait(5), runner_errors)
            run_meta.record_campaign_stop_condition(
                out_dir, "sibling_integrity_failure",
                sample="sibling")
            release_backward.set()
            worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(calls, ["forward", "backward"])
            self.assertEqual(evaluate.call_count, 1)
            commit.assert_not_called()
            self.assertEqual(len(runner_errors), 1)
            self.assertIsInstance(
                runner_errors[0], run_meta.CampaignStoppedError)
            self.assertRegex(
                str(runner_errors[0]),
                "campaign stop")
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "hybridpatch", "sample.jsonl")))
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "hybridpatch", "sample.ckpt.json")))

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
            ("Use {ordinary braces} in prose.", "normal"),
            ('```json\n{"answer":"ordinary data"}\n```', "normal"),
            ("I cannot perform this task; {no protocol was emitted}.",
             "normal"),
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
            self.assertNotIn("method_phase", first)
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

    def test_run_metadata_accepts_hp_then_fr_phases_without_changing_legacy(self):
        kwargs = {
            "command": "python phased-test",
            "samples": ["sample"],
            "num_round_trips": 10,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": True,
            "max_tokens": 16,
            "printing": False,
        }
        phase_env = {
            "ANCHORPATCH_CAMPAIGN_METHOD_SET": "hybridpatch,fullrewrite",
            "ANCHORPATCH_METHOD_PHASE": "hybridpatch",
            "ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE": json.dumps({
                "schema": "anchorpatch.interrupted_phase_resume/1",
                "sample": "sample",
                "method_phase": "hybridpatch",
                "prior_invocation_id": "prior-invocation",
                "task_plan_sha256": "a" * 64,
                "checkpoint_progress": {
                    "hybridpatch": {
                        "completed_round_trips": 1,
                        "committed_rows": 2,
                    }
                },
            }),
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"unit": "test"}), mock.patch.dict(
                        os.environ, phase_env, clear=False):
            hp = run_meta.append_run_metadata(
                out_dir, methods=["hybridpatch"], **kwargs)
            run_meta.finish_run_metadata(out_dir, hp["invocation_id"])
            os.environ["ANCHORPATCH_METHOD_PHASE"] = "fullrewrite"
            os.environ.pop("ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE", None)
            fr = run_meta.append_run_metadata(
                out_dir, methods=["fullrewrite"], **kwargs)
            run_meta.finish_run_metadata(out_dir, fr["invocation_id"])
        self.assertEqual(hp["method_phase"], "hybridpatch")
        self.assertEqual(fr["method_phase"], "fullrewrite")
        self.assertEqual(
            hp["interrupted_resume_authorization"]["prior_invocation_id"],
            "prior-invocation")
        self.assertNotIn("interrupted_resume_authorization", fr)
        self.assertEqual(
            hp["campaign_config"]["method_set"],
            ["fullrewrite", "hybridpatch"])
        self.assertEqual(hp["campaign_config"], fr["campaign_config"])

    def test_run_metadata_records_exact_authorized_git_recovery_boundary(self):
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
        prior_commit = "1" * 40
        recovery_commit = "2" * 40
        prior_fingerprint = {"run_meta.py": "old", "hybrid_schema.py": "same"}
        recovery_fingerprint = {
            "run_meta.py": "new", "hybrid_schema.py": "same"}
        authorization = {
            "authorization_id": "test-recovery",
            "authorization_sha256": "a" * 64,
            "prior_git_commit": prior_commit,
            "recovery_git_commit": recovery_commit,
            "prior_code_fingerprint": prior_fingerprint,
            "recovery_code_fingerprint": recovery_fingerprint,
            "archived_stop_path": "recovery_history/stop.json",
            "archived_stop_sha256": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(prior_commit, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=prior_fingerprint), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=None):
                first = run_meta.append_run_metadata(out_dir, **kwargs)
                run_meta.finish_run_metadata(out_dir, first["invocation_id"])
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(recovery_commit, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=recovery_fingerprint), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=authorization):
                recovered = run_meta.append_run_metadata(out_dir, **kwargs)
                second_recovered = run_meta.append_run_metadata(
                    out_dir, **kwargs)
            boundary = recovered["campaign_recovery_authorization"]
            self.assertEqual(boundary["authorization_id"], "test-recovery")
            self.assertEqual(boundary["prior_git_commit"], prior_commit)
            self.assertEqual(boundary["recovery_git_commit"], recovery_commit)
            self.assertEqual(recovered["run_git_commit"], recovery_commit)
            self.assertEqual(
                second_recovered["run_git_commit"], recovery_commit)

    def test_run_metadata_accepts_sha_pinned_chained_recovery_identities(self):
        kwargs = {
            "command": "python chained-recovery-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        commits = [character * 40 for character in "123"]
        fingerprints = [
            {"run_meta.py": f"revision-{index}"}
            for index in range(3)
        ]

        def authorization(index, history):
            return {
                "authorization_id": f"recovery-{index}",
                "authorization_sha256": str(index) * 64,
                "prior_git_commit": commits[0],
                "recovery_git_commit": commits[index],
                "prior_code_fingerprint": fingerprints[0],
                "recovery_code_fingerprint": fingerprints[index],
                "archived_stop_path": f"recovery-{index}/stop.json",
                "archived_stop_sha256": "f" * 64,
                "recovery_identity_history": history,
            }

        base_identity = {
            "authorization_id": None,
            "authorization_sha256": None,
            "run_git_commit": commits[0],
            "git_tree_state": "clean",
            "code_fingerprint": fingerprints[0],
        }
        first_recovery_identity = {
            "authorization_id": "recovery-1",
            "authorization_sha256": "1" * 64,
            "run_git_commit": commits[1],
            "git_tree_state": "clean",
            "code_fingerprint": fingerprints[1],
        }
        current_recovery_identity = {
            "authorization_id": "recovery-2",
            "authorization_sha256": "2" * 64,
            "run_git_commit": commits[2],
            "git_tree_state": "clean",
            "code_fingerprint": fingerprints[2],
        }
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(commits[0], "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=fingerprints[0]), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=None):
                original = run_meta.append_run_metadata(out_dir, **kwargs)
                run_meta.finish_run_metadata(
                    out_dir, original["invocation_id"])

            first_authorization = authorization(
                1, [base_identity, first_recovery_identity])
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(commits[1], "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=fingerprints[1]), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=first_authorization):
                first_recovery = run_meta.append_run_metadata(
                    out_dir, **kwargs)
                run_meta.finish_run_metadata(
                    out_dir, first_recovery["invocation_id"])

            current_authorization = authorization(
                2, [
                    base_identity,
                    first_recovery_identity,
                    current_recovery_identity,
                ])
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(commits[2], "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=fingerprints[2]), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=current_authorization):
                latest = run_meta.append_run_metadata(out_dir, **kwargs)

        self.assertEqual(latest["run_git_commit"], commits[2])
        self.assertEqual(
            latest["campaign_recovery_authorization"]["authorization_id"],
            "recovery-2",
        )

    def test_recovery_incident_evidence_marks_preauthorization_workers(self):
        authorization = {
            "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
            "authorization_id": "recovery-test",
            "incident_api_rows": [],
            "incident_attempt_rows": [],
            "recovered_worker_launch_ids": ["worker-old", "worker-preauth"],
            "preauthorization_worker_launch_ids": ["worker-preauth"],
        }
        run_meta.campaign_recovery_incident_evidence.cache_clear()
        with mock.patch.object(
                run_meta, "read_campaign_recovery_authorization",
                return_value=authorization):
            evidence = run_meta.campaign_recovery_incident_evidence(
                "unit-test-out-dir")
        run_meta.campaign_recovery_incident_evidence.cache_clear()
        self.assertEqual(
            evidence["worker_launch_ids"],
            frozenset({"worker-old", "worker-preauth"}),
        )
        self.assertEqual(
            evidence["preauthorization_worker_launch_ids"],
            frozenset({"worker-preauth"}),
        )

    def test_operator_pause_preserves_prior_preauthorization_evidence(self):
        workers = {"worker-preauth"}
        expected_errors = {
            "worker sample exited before authorization with 1"}
        self.assertTrue(run_meta._preauthorization_stop_evidence_matches(
            {"condition": "operator_directed_dispatcher_pause"},
            workers, expected_errors))
        self.assertTrue(run_meta._preauthorization_stop_evidence_matches(
            {
                "condition": "dispatcher_integrity_failure",
                "error": "worker sample exited before authorization with 1",
            },
            workers, expected_errors))
        self.assertFalse(run_meta._preauthorization_stop_evidence_matches(
            {
                "condition": "dispatcher_integrity_failure",
                "error": "unrelated",
            },
            workers, expected_errors))

    def test_git_identity_ignores_evaluator_tmp_but_not_other_untracked_files(self):
        root_ignore = pathlib.Path(ROOT).parent / ".gitignore"
        ignore_text = root_ignore.read_text(encoding="utf-8")
        self.assertIn("HP_V**/tmp_eval_**/", ignore_text.splitlines())

        with tempfile.TemporaryDirectory() as repo_dir:
            repo = pathlib.Path(repo_dir)
            source_dir = repo / "HP_V8" / "src"
            source_dir.mkdir(parents=True)
            (repo / ".gitignore").write_text(ignore_text, encoding="utf-8")
            (source_dir / "tracked.py").write_text("TRACKED = True\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=repo, check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "HybridPatch Test"],
                cwd=repo, check=True,
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "fixture"],
                cwd=repo, check=True,
            )

            evaluator_tmp = repo / "HP_V8" / "tmp_eval_unit_test" / "work"
            evaluator_tmp.mkdir(parents=True)
            (evaluator_tmp / "artifact.py").write_text("temporary\n", encoding="utf-8")
            with mock.patch.object(run_meta, "_HERE", str(source_dir)):
                _commit, tree_state, porcelain = run_meta._git_identity_details()
            self.assertEqual(tree_state, "clean")
            self.assertEqual(porcelain, "")

            untracked_source = source_dir / "untracked_source.py"
            untracked_source.write_text("untracked\n", encoding="utf-8")
            with mock.patch.object(run_meta, "_HERE", str(source_dir)):
                _commit, tree_state, porcelain = run_meta._git_identity_details()
            self.assertEqual(tree_state, "dirty")
            self.assertEqual(porcelain, "?? HP_V8/src/untracked_source.py\n")

    def test_git_identity_drift_stop_records_full_porcelain(self):
        with tempfile.TemporaryDirectory() as repo_dir, \
                tempfile.TemporaryDirectory() as out_dir:
            repo = pathlib.Path(repo_dir)
            source_dir = repo / "HP_V8" / "src"
            source_dir.mkdir(parents=True)
            (repo / ".gitignore").write_text(
                "HP_V**/tmp_eval_**/\n", encoding="utf-8")
            (source_dir / "tracked.py").write_text("TRACKED = True\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=repo, check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "HybridPatch Test"],
                cwd=repo, check=True,
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "fixture"],
                cwd=repo, check=True,
            )
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
                capture_output=True, text=True, encoding="utf-8",
            ).stdout.strip()
            untracked_source = source_dir / "unexpected.py"
            untracked_source.write_text("unexpected\n", encoding="utf-8")
            expected_porcelain = "?? HP_V8/src/unexpected.py\n"
            env = {
                "ANCHORPATCH_EXPECTED_GIT_COMMIT": commit,
                "ANCHORPATCH_EXPECTED_GIT_TREE_STATE": "clean",
            }
            with mock.patch.object(run_meta, "_HERE", str(source_dir)), \
                    mock.patch.dict(os.environ, env, clear=False):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    run_meta.enforce_campaign_runtime_guards(out_dir, "sample")
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(stop["condition"], "git_identity_drift")
            self.assertEqual(stop["actual_tree_state"], "dirty")
            self.assertEqual(stop["git_status_porcelain"], expected_porcelain)

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
                second._semantic_ids("fullrewrite_primary")[1])
            with open(journal_path, encoding="utf-8") as handle:
                valid = json.load(handle)
            broken = dict(valid)
            broken["request_fingerprint"] = "wrong-fingerprint"
            run_meta.write_json_atomic(journal_path, broken)
            fingerprint_forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            third = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", fingerprint_forbidden)
            third.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "fingerprint differs within"):
                third.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary")
            fingerprint_forbidden.assert_not_called()

            broken = dict(valid)
            broken["schema"] = "anchorpatch.api_response_journal/3"
            run_meta.write_json_atomic(journal_path, broken)
            forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            fourth = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", forbidden)
            fourth.set_step(1, "forward", "target")
            with self.assertRaisesRegex(RuntimeError, "journal schema"):
                fourth.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary")
            forbidden.assert_not_called()

    def test_blank_stop_reason_fails_closed_in_ledger_and_journal(self):
        for stop_reason in ("", " \t "):
            with self.subTest(
                    artifact="ledger", stop_reason=repr(stop_reason)), \
                    tempfile.TemporaryDirectory() as out_dir:
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                _step, semantic = recorder._semantic_ids(
                    "hybridpatch_primary")
                call_id = "call-ledger"
                fingerprint = "fingerprint-ledger"
                recorder._append_ledger(
                    semantic, "semantic_request", call_id=call_id,
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                recorder._append_ledger(
                    semantic, "attempt_start", attempt_index=1,
                    call_id=call_id, call_kind="hybridpatch_primary",
                    attempt_kind="transport_initial",
                    request_fingerprint=fingerprint)
                recorder._append_ledger(
                    semantic, "generation_progress", attempt_index=1,
                    call_id=call_id, delta_type="text_delta")
                terminal = self._successful_attempt_end_fields()
                terminal["stop_reason"] = stop_reason
                recorder._append_ledger(
                    semantic, "attempt_end", attempt_index=1,
                    call_id=call_id, **terminal)
                with self.assertRaisesRegex(
                        RuntimeError, "incomplete successful stream"):
                    recorder._ledger_state(semantic)
                provider.assert_not_called()

            with self.subTest(
                    artifact="journal", stop_reason=repr(stop_reason)), \
                    tempfile.TemporaryDirectory() as out_dir:
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                _step, semantic = recorder._semantic_ids(
                    "hybridpatch_primary")
                lineage = run_meta._semantic_lineage_fields(semantic)
                fingerprint = "fingerprint-journal"
                result = {
                    "semantic_root_id": lineage["semantic_root_id"],
                    "semantic_call_id": semantic,
                    "generation_index": 0,
                    "parent_semantic_call_id": None,
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "call_kind": "hybridpatch_primary",
                    "stream_complete": True,
                    "stop_reason": stop_reason,
                    "message": "complete text",
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "max_response_slots": 2,
                    "max_transient_failures": 3,
                    "response_slots_used": 1,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                run_meta.write_json_atomic(
                    recorder._journal_path(semantic), {
                        "schema": run_meta.API_RESPONSE_JOURNAL_SCHEMA,
                        "semantic_root_id": lineage["semantic_root_id"],
                        "semantic_call_id": semantic,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "call_id": "call-journal",
                        "request_fingerprint": fingerprint,
                        "result": result,
                    })
                with self.assertRaisesRegex(
                        RuntimeError, "journal result is incomplete"):
                    recorder._find_lineage_journal(
                        lineage["semantic_root_id"], fingerprint)
                provider.assert_not_called()

    def test_committed_ledger_without_journal_fails_before_provider(self):
        result = {
            "message": "Hello", "http_status": 200,
            "stream_complete": True, "finish_reason": "end_turn",
            "stop_reason": "end_turn", "response_classification": "normal",
            "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
            "input_tokens": 5, "output_tokens": 1,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0, "transport_attempts": [],
            "call_kind": "fullrewrite_primary", "thinking_mode": "adaptive",
            "transport": "anthropic_sdk_v2",
            "transport_revision": "opencode_anthropic_sdk/4",
            "max_response_slots": 2, "response_slots_used": 1,
            "max_transient_failures": 3, "transient_failure_count": 0,
            "http_attempts_used": 1,
        }

        def fake_generate(*_args, **kwargs):
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
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", fake_generate)
            first.set_step(1, "forward", "target")
            first.generate(
                [{"role": "user", "content": "Hello"}],
                model="minimax-m3", call_kind="fullrewrite_primary")
            journal_path = first._journal_path(
                first._semantic_ids("fullrewrite_primary")[1])
            os.remove(journal_path)

            forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            second = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", forbidden)
            second.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "committed response.*journal is missing"):
                second.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary")
            forbidden.assert_not_called()

    def test_dangling_generation_attempt_fails_closed_before_provider(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            first = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                provider)
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
                provider)
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
            with self.subTest(label=label), tempfile.TemporaryDirectory() as out_dir, \
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
                    "minimax-m3", mock.Mock())
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
        kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), kwargs, "minimax-m3", "hybridpatch_primary")
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            prior = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", mock.Mock())
            prior.set_step(2, "backward", "target")
            _step, semantic = prior._semantic_ids("hybridpatch_primary")
            prior._append_ledger(
                semantic, "semantic_request", call_id="failed-call",
                call_kind="hybridpatch_primary",
                request_fingerprint=fingerprint)
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
                recovery_index=0, request_fingerprint=fingerprint)
            observed = {}

            def resumed_generate(*_args, **inner):
                observed.update(inner["_retry_state"])
                next_index = observed["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "attempt_index": next_index,
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
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": next_index, "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
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
                    "minimax-m3", resumed_generate)
                resumed.set_step(2, "backward", "target")
                out = resumed.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
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
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            attempt_indices = [
                row.get("attempt_index") for row in ledger
                if row.get("event") == "attempt_start"]
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

    def test_multiple_exact_recoveries_keep_per_generation_budgets(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", mock.Mock())
            recorder.set_step(4, "forward", "target")
            _step, generation_zero = recorder._semantic_ids(
                "hybridpatch_primary")
            generation_one_context = recorder._semantic_context(
                "hybridpatch_primary", 1,
                parent_semantic_call_id=generation_zero)
            generation_one = generation_one_context["semantic_call_id"]

            def append_exhausted_generation(
                    semantic_call_id, call_id, attempt_indices, generation):
                recorder._append_ledger(
                    semantic_call_id, "semantic_request", call_id=call_id,
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                for local_index, attempt_index in enumerate(
                        attempt_indices, 1):
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
                        request_fingerprint=fingerprint)
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
                    status="provider_failure",
                    error_type="incomplete_stream",
                    response_slots_used=2, transient_failure_count=0,
                    http_attempts_used=attempt_indices[-1],
                    attempt_index=attempt_indices[-1],
                    request_fingerprint=fingerprint)

            append_exhausted_generation(
                generation_zero, "failed-g000", (1, 2), 0)
            append_exhausted_generation(
                generation_one, "failed-g001", (3, 4), 1)

            observed = {}

            def generation_two_generate(*_args, **inner):
                observed.update(inner["_retry_state"])
                attempt_index = observed["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "attempt_index": attempt_index,
                    "attempt_kind": "transport_recovery_initial",
                })
                inner["_raw_event_sink"]({
                    "record_type": "sdk_stream_event",
                    "attempt_index": attempt_index,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                inner["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": attempt_index,
                        **self._successful_attempt_end_fields(),
                    },
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": attempt_index, "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": attempt_index,
                }
                inner["_response_commit_sink"](result)
                return result

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                    generation_one),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "2",
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                    fingerprint),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": "5",
            }, clear=False):
                resumed = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", generation_two_generate)
                resumed.set_step(4, "forward", "target")
                out = resumed.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")

            root = generation_zero.rsplit("/", 1)[0]
            generation_two = f"{root}/g002"
            self.assertEqual(observed["response_slots_used"], 0)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 4)
            self.assertEqual(observed["generation_index"], 2)
            self.assertEqual(
                observed["parent_semantic_call_id"], generation_one)
            self.assertEqual(out["semantic_call_id"], generation_two)
            self.assertEqual(out["generation_index"], 2)

            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            self.assertEqual([
                row["attempt_index"] for row in ledger
                if row.get("event") == "attempt_start"
            ], [1, 2, 3, 4, 5])
            self.assertNotIn(
                "transport_resume", [row.get("event") for row in ledger])
            for semantic_call_id, expected_slots in (
                    (generation_zero, 2), (generation_one, 2),
                    (generation_two, 1)):
                state = resumed._ledger_state(semantic_call_id)
                self.assertEqual(
                    state["response_slots_used"], expected_slots)
                self.assertEqual(state["transient_failure_count"], 0)

    def test_committed_recovery_does_not_block_next_semantic_root(self):
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

            observed = []

            def successful_generate(*_args, **inner):
                state = dict(inner["_retry_state"])
                observed.append(state)
                attempt_index = state["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "attempt_index": attempt_index,
                    "attempt_kind": (
                        "transport_recovery_initial"
                        if state["generation_index"] else
                        "transport_initial"),
                })
                inner["_raw_event_sink"]({
                    "record_type": "sdk_stream_event",
                    "attempt_index": attempt_index,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                inner["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": attempt_index,
                        **self._successful_attempt_end_fields(),
                    },
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": attempt_index,
                        "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": attempt_index,
                }
                inner["_response_commit_sink"](result)
                return result

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                    generation_zero),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "1",
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                    fingerprint),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": "3",
            }, clear=False):
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", successful_generate)
                recorder.set_step(1, "forward", "target")
                recovered = recorder.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
                recorder.set_step(2, "backward", "initial")
                next_root = recorder.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")

            self.assertEqual(recovered["generation_index"], 1)
            self.assertEqual(next_root["generation_index"], 0)
            self.assertNotEqual(
                recovered["semantic_root_id"], next_root["semantic_root_id"])
            self.assertEqual(
                [state["http_attempts_used"] for state in observed], [2, 0])

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

    def test_budget_overrun_and_cross_generation_attempt_order_fail_closed(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")

        for label in ("extra-attempt", "cross-generation-order"):
            with self.subTest(label=label), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ, {"OPENCODE_API_KEY": "unit-test-key"},
                        clear=False):
                provider = mock.Mock(side_effect=AssertionError(
                    "provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                _step, generation_zero = recorder._semantic_ids(
                    "hybridpatch_primary")
                if label == "extra-attempt":
                    recorder._append_ledger(
                        generation_zero, "semantic_request",
                        call_id="over-budget",
                        call_kind="hybridpatch_primary",
                        request_fingerprint=fingerprint)
                    for attempt_index in (1, 2, 3):
                        recorder._append_ledger(
                            generation_zero, "attempt_start",
                            attempt_index=attempt_index,
                            call_id="over-budget",
                            call_kind="hybridpatch_primary",
                            attempt_kind=(
                                "transport_initial" if attempt_index == 1
                                else "transport_retry"),
                            request_fingerprint=fingerprint)
                        recorder._append_ledger(
                            generation_zero, "generation_progress",
                            attempt_index=attempt_index,
                            call_id="over-budget", delta_type="text_delta")
                        recorder._append_ledger(
                            generation_zero, "attempt_end",
                            attempt_index=attempt_index,
                            call_id="over-budget",
                            **self._failed_attempt_end_fields())
                        recorder._append_ledger(
                            generation_zero, "attempt_budget",
                            attempt_index=attempt_index,
                            call_id="over-budget",
                            budget_class="response_slot",
                            response_slots_used=attempt_index,
                            transient_failure_count=0)
                    expected = "exceeds the frozen R2/I3 budget"
                else:
                    self._append_exhausted_response_generation(
                        recorder, generation_zero, "failed-g000",
                        fingerprint, (1, 2))
                    generation_one = recorder._semantic_context(
                        "hybridpatch_primary", 1,
                        parent_semantic_call_id=generation_zero
                    )["semantic_call_id"]
                    recorder._append_ledger(
                        generation_one, "semantic_request",
                        call_id="out-of-order",
                        call_kind="hybridpatch_primary",
                        request_fingerprint=fingerprint)
                    recorder._append_ledger(
                        generation_one, "attempt_start", attempt_index=1,
                        call_id="out-of-order",
                        call_kind="hybridpatch_primary",
                        attempt_kind="transport_recovery_initial",
                        request_fingerprint=fingerprint)
                    recorder._append_ledger(
                        generation_one, "attempt_end", attempt_index=1,
                        call_id="out-of-order",
                        **self._failed_attempt_end_fields(
                            generation_delta_seen=False,
                            error_type="connection_error"))
                    recorder._append_ledger(
                        generation_one, "attempt_budget", attempt_index=1,
                        call_id="out-of-order",
                        budget_class="transient_failure",
                        response_slots_used=0,
                        transient_failure_count=1)
                    expected = "attempt indexes are duplicated/out of order"
                with self.assertRaisesRegex(RuntimeError, expected):
                    recorder.generate(
                        messages, model="minimax-m3",
                        call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_hybrid_repair_has_independent_transport_budget(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            observed = {}

            def repair_generate(*_args, **kwargs):
                observed.update(kwargs["_retry_state"])
                result = {
                    "message": "{}", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 1, "completion_tokens": 1,
                    "total_tokens": 2, "input_tokens": 1,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [],
                    "call_kind": "hybridpatch_repair",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_start", "attempt_index": 1,
                    "attempt_kind": "transport_initial"})
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
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", repair_generate)
            recorder.set_step(1, "forward", "target")
            _step, primary = recorder._semantic_ids(
                "hybridpatch_primary")
            self._append_exhausted_response_generation(
                recorder, primary, "primary-failed",
                "primary-fingerprint", (1, 2))
            recorder.generate(
                [{"role": "user", "content": "repair"}],
                model="minimax-m3", call_kind="hybridpatch_repair")
            self.assertEqual(observed["response_slots_used"], 0)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 0)

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

    def test_deepseek_failed_attempt_body_persists_in_api_row_and_sidecar(self):
        secret = "unit-test-key"
        sanitized_attempt = {
            "attempt_index": 1,
            "status": "retryable_error",
            "error_type": "server_error",
            "http_status": 502,
            "stream_complete": False,
            "retry_budget_consumed": True,
            "retry_budget_attempt_index": 3,
            "error_body": {
                "error_name": "origin_bad_gateway",
                "detail": "<redacted-key>",
            },
        }
        raw_attempt = copy.deepcopy(sanitized_attempt)
        raw_attempt["error_body"]["detail"] = secret
        last_error = model_openai._HTTPStatusError(
            502, json.dumps(raw_attempt["error_body"]))
        terminal = model_openai.OpenAICompatibleTransportError(
            "failed transport",
            attempts=[sanitized_attempt],
            last_error=last_error,
        )

        def fail_generate(*_args, **kwargs):
            sink = kwargs["_raw_event_sink"]
            sink({
                "record_type": "attempt_start",
                "attempt_index": 1,
            })
            sink({
                "record_type": "attempt_end",
                "attempt_index": 1,
                "attempt": raw_attempt,
            })
            raise terminal

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": secret,
                "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
            },
            clear=False,
        ):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fail_generate)
            recorder.set_step(1, "forward", "target")
            with self.assertRaises(
                    model_openai.OpenAICompatibleTransportError):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model=paired_dispatch.DEEPSEEK_MODEL,
                    max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    reasoning_effort="high",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(
                record["transport_revision"],
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION)
            self.assertEqual(
                record["transport_attempts"][0]["error_body"],
                sanitized_attempt["error_body"])
            sidecar = record["raw_sse_saved_path"]
            self.assertTrue(os.path.isfile(sidecar))
            with open(sidecar, encoding="utf-8") as handle:
                sidecar_text = handle.read()
            self.assertIn("origin_bad_gateway", sidecar_text)
            self.assertIn("<redacted-key>", sidecar_text)
            self.assertNotIn(secret, sidecar_text)
            with open(
                os.path.join(out_dir, "api_calls.jsonl"),
                encoding="utf-8",
            ) as handle:
                self.assertNotIn(secret, handle.read())

    def test_deepseek_sidecar_write_failure_aborts_before_success_commit(self):
        real_open = open

        class FailingTransportFile:
            def __init__(self, inner):
                self._inner = inner

            @property
            def closed(self):
                return self._inner.closed

            def write(self, _value):
                raise OSError("transport sidecar write failed")

            def flush(self):
                self._inner.flush()

            def close(self):
                self._inner.close()

        def selective_open(path, *args, **kwargs):
            inner = real_open(path, *args, **kwargs)
            if str(path).endswith(".transport.jsonl"):
                return FailingTransportFile(inner)
            return inner

        def fake_generate(*_args, **kwargs):
            model_openai._emit_transport_event(
                kwargs["_raw_event_sink"],
                {
                    "record_type": "attempt_start",
                    "attempt_index": 1,
                },
            )
            self.fail("provider call continued after sidecar write failure")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "unit-test-key",
                    "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
                },
                clear=False), mock.patch.object(
                    run_meta, "open", side_effect=selective_open,
                    create=True):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fake_generate)
            recorder.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    OSError, "transport sidecar write failed"):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model=paired_dispatch.DEEPSEEK_MODEL,
                    max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    reasoning_effort="high",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["classification"], "runner_exception")
            self.assertFalse(record["provider_called"])
            self.assertFalse(record["stream_complete"])
            self.assertIsNone(record["raw_sse_saved_path"])

    def test_deepseek_sidecar_fsync_failure_aborts_before_success_commit(self):
        real_open = open
        real_fsync = run_meta.os.fsync
        transport_fd = 987654321

        class FailingFsyncTransportFile:
            def __init__(self, inner):
                self._inner = inner

            @property
            def closed(self):
                return self._inner.closed

            def write(self, value):
                return self._inner.write(value)

            def flush(self):
                return self._inner.flush()

            def fileno(self):
                return transport_fd

            def close(self):
                self._inner.close()

        def selective_open(path, *args, **kwargs):
            inner = real_open(path, *args, **kwargs)
            if str(path).endswith(".transport.jsonl"):
                return FailingFsyncTransportFile(inner)
            return inner

        def selective_fsync(fd):
            if fd == transport_fd:
                raise OSError("transport sidecar fsync failed")
            return real_fsync(fd)

        def fake_generate(*_args, **kwargs):
            sink = kwargs["_raw_event_sink"]
            model_openai._emit_transport_event(sink, {
                "record_type": "attempt_start",
                "attempt_index": 1,
            })
            model_openai._emit_transport_event(sink, {
                "record_type": "attempt_end",
                "attempt_index": 1,
                "attempt": {
                    "attempt_index": 1,
                    "status": "success",
                },
            })
            self.fail("provider result continued after sidecar fsync failure")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "unit-test-key",
                    "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
                },
                clear=False), mock.patch.object(
                    run_meta, "open", side_effect=selective_open,
                    create=True), mock.patch.object(
                        run_meta.os, "fsync", side_effect=selective_fsync):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fake_generate)
            recorder.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    OSError, "transport sidecar fsync failed"):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model=paired_dispatch.DEEPSEEK_MODEL,
                    max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    reasoning_effort="high",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["classification"], "runner_exception")
            self.assertTrue(record["provider_called"])
            self.assertFalse(record["stream_complete"])

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


def _stream_chunks_from_payload(payload):
    choice = (payload.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    common = {
        "id": payload.get("id"),
        "model": "deepseek-v4-flash",
    }
    chunks = [{
        **common,
        "choices": [{
            "index": 0,
            "delta": {
                "role": "assistant",
                "reasoning_details": message.get("reasoning_details") or [],
            },
            "finish_reason": None,
        }],
        "usage": None,
    }]
    if message.get("content") is not None:
        chunks.append({
            **common,
            "choices": [{
                "index": 0,
                "delta": {"content": message.get("content")},
                "finish_reason": None,
            }],
            "usage": None,
        })
    chunks.append({
        **common,
        "choices": [{
            "index": 0,
            "delta": {},
            "finish_reason": choice.get("finish_reason"),
        }],
        "usage": None,
    })
    chunks.append({
        **common,
        "choices": [],
        "usage": payload.get("usage"),
    })
    return chunks


def _official_client_factory(outcomes, captures):
    """outcomes: list of payload dicts or Exceptions, consumed per call."""

    class _Completions:
        def create(self, **kwargs):
            captures.append(kwargs)
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if kwargs.get("stream"):
                chunks = (
                    outcome if isinstance(outcome, list)
                    else _stream_chunks_from_payload(outcome)
                )

                def _iter_chunks():
                    for chunk in chunks:
                        if isinstance(chunk, Exception):
                            raise chunk
                        yield chunk

                return _iter_chunks()
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
            "OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1",
            "AZURE_OPENAI_API_KEY": "",
            "AZURE_OPENAI_ENDPOINT": "",
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
        self.assertEqual(
            constructor["base_url"], "https://opencode.ai/zen/go/v1")
        self.assertEqual(constructor["max_retries"], 0)
        self.assertEqual(request["model"], "deepseek-v4-flash")
        self.assertEqual(request["reasoning_effort"], "high")
        self.assertEqual(request["max_completion_tokens"], 20000)
        self.assertIs(request["stream"], True)
        self.assertEqual(
            request["stream_options"], {"include_usage": True})
        self.assertEqual(
            result["_raw_request_body"]["reasoning_effort"], "high")
        self.assertEqual(result["provider"], "opencode_zen")
        self.assertEqual(result["transport"], "openai_sdk_stream")
        self.assertEqual(
            result["transport_revision"], "opencode_openai_compatible/5")
        self.assertEqual(
            result["request_url"],
            "https://opencode.ai/zen/go/v1/chat/completions")
        self.assertEqual(result["reasoning_effort"], "high")
        self.assertIs(result["stream_complete"], True)
        self.assertEqual(len(result["_raw_stream_events"]), 4)
        self.assertEqual(result["http_attempts_used"], 1)
        self.assertEqual(result["retry_count"], 0)
        self.assertEqual(result["cost_currency"], "USD")
        self.assertAlmostEqual(result["total_usd"], 0.00000252)
        self.assertEqual(result["total_cny"], 0.0)
        self.assertEqual(events[0]["record_type"], "attempt_start")
        self.assertEqual(events[-1]["record_type"], "attempt_end")
        self.assertEqual(
            sum(event["record_type"] == "sdk_stream_event"
                for event in events),
            4,
        )

    def test_finish_chunk_usage_and_empty_trailer_are_complete(self):
        captures = []
        payload = _official_payload(content="Hello", finish_reason="stop")
        chunks = _stream_chunks_from_payload(payload)
        chunks[-2]["usage"] = payload["usage"]
        chunks[-1]["usage"] = None
        result, _events = self._generate([chunks], captures)
        self.assertEqual(result["message"], "Hello")
        self.assertTrue(result["stream_complete"])
        self.assertEqual(result["input_tokens"], 10)
        self.assertEqual(result["output_tokens"], 4)
        attempt = result["transport_attempts"][-1]
        self.assertTrue(attempt["message_stop_seen"])
        self.assertTrue(attempt["final_usage_seen"])
        self.assertTrue(attempt["terminal_sequence_valid"])

    def test_duplicate_usage_after_finish_chunk_is_incomplete(self):
        captures = []
        payload = _official_payload(content="bad", finish_reason="stop")
        malformed = _stream_chunks_from_payload(payload)
        malformed[-2]["usage"] = payload["usage"]
        result, _events = self._generate(
            [malformed, _official_payload(content="complete")],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertTrue(first["final_usage_seen"])
        self.assertFalse(first["terminal_sequence_valid"])
        self.assertEqual(result["message"], "complete")

    def test_blank_finish_reason_is_incomplete(self):
        captures = []
        result, _events = self._generate(
            [
                _official_payload(content="bad", finish_reason=" \t"),
                _official_payload(content="complete"),
            ],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertFalse(first["message_stop_seen"])
        self.assertFalse(first["terminal_sequence_valid"])
        self.assertEqual(result["message"], "complete")

    def test_terminal_usage_counts_are_strict(self):
        self.assertTrue(model_openai._valid_openai_final_usage({
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 14,
        }))
        for usage in (
                {},
                {"prompt_tokens": -1, "completion_tokens": 4,
                 "total_tokens": 3},
                {"prompt_tokens": True, "completion_tokens": 4,
                 "total_tokens": 5},
                {"prompt_tokens": 10, "completion_tokens": 4,
                 "total_tokens": 15}):
            with self.subTest(usage=usage):
                self.assertFalse(
                    model_openai._valid_openai_final_usage(usage))

    def test_finish_without_usage_is_incomplete(self):
        captures = []
        malformed = _stream_chunks_from_payload(
            _official_payload(content="bad", finish_reason="stop"))
        malformed[-1]["usage"] = None
        result, _events = self._generate(
            [malformed, _official_payload(content="complete")],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertTrue(first["message_stop_seen"])
        self.assertFalse(first["final_usage_seen"])
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertEqual(result["message"], "complete")

    def test_usage_without_finish_is_incomplete(self):
        captures = []
        result, _events = self._generate(
            [
                _official_payload(content="bad", finish_reason=None),
                _official_payload(content="complete"),
            ],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertFalse(first["message_stop_seen"])
        self.assertFalse(first["final_usage_seen"])
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertEqual(result["message"], "complete")

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
            [event["record_type"] for event in events
             if event["record_type"] != "sdk_stream_event"],
            ["attempt_start", "attempt_end", "attempt_start", "attempt_end"],
        )

    def test_openai_connection_error_is_retried_and_recorded(self):
        captures = []
        result, _events = self._generate(
            [
                model_openai.APIConnectionError(
                    request=httpx.Request(
                        "POST",
                        "https://opencode.ai/zen/go/v1/chat/completions",
                    ),
                ),
                _official_payload(content="complete", finish_reason="stop"),
            ],
            captures,
        )
        self.assertEqual(result["message"], "complete")
        self.assertEqual(result["http_attempts_used"], 2)
        self.assertEqual(result["retry_count"], 1)
        first = result["transport_attempts"][0]
        self.assertEqual(first["status"], "retryable_error")
        self.assertEqual(first["error_type"], "transport_disconnect")
        self.assertEqual(first["error_message"], "Connection error.")
        self.assertIsNone(first["http_status"])
        self.assertIs(first["retry_budget_consumed"], True)
        self.assertEqual(first["retry_budget_attempt_index"], 1)

    def test_openai_connection_error_exhaustion_is_bounded(self):
        captures = []
        failures = [
            model_openai.APIConnectionError(
                request=httpx.Request(
                    "POST",
                    "https://opencode.ai/zen/go/v1/chat/completions",
                ),
            )
            for _index in range(3)
        ]
        with self.assertRaises(
                model_openai.OpenAICompatibleTransportError) as caught:
            self._generate(failures, captures, max_retries=3)
        attempts = caught.exception.transport_attempts
        self.assertEqual(len(attempts), 3)
        self.assertTrue(all(
            item["status"] == "retryable_error"
            and item["error_type"] == "transport_disconnect"
            and item["retry_budget_consumed"] is True
            for item in attempts
        ))
        self.assertEqual(
            [item["retry_budget_attempt_index"] for item in attempts],
            [1, 2, 3],
        )

    def test_transport_error_before_first_chunk_is_incomplete_and_retried(self):
        captures = []
        result, _events = self._generate(
            [
                [httpx.ReadTimeout("timed out before first chunk")],
                _official_payload(content="complete", finish_reason="stop"),
            ],
            captures,
        )
        self.assertEqual(result["message"], "complete")
        first = result["transport_attempts"][0]
        self.assertEqual(first["status"], "retryable_error")
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertEqual(first["http_status"], 200)
        self.assertEqual(first["response_started_http_status"], 200)
        self.assertFalse(first["generation_delta_seen"])
        self.assertTrue(first["retry_budget_consumed"])

    def test_sdk_error_before_first_chunk_is_incomplete_and_retried(self):
        captures = []
        result, _events = self._generate(
            [
                [model_openai.APIError(
                    "SSE error event",
                    request=httpx.Request(
                        "POST",
                        "https://opencode.ai/zen/go/v1/chat/completions",
                    ),
                    body={"error": "temporary upstream failure"},
                )],
                _official_payload(content="complete", finish_reason="stop"),
            ],
            captures,
        )
        self.assertEqual(result["message"], "complete")
        first = result["transport_attempts"][0]
        self.assertEqual(first["status"], "retryable_error")
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertEqual(first["response_started_http_status"], 200)
        self.assertFalse(first["generation_delta_seen"])

    def test_malformed_first_sse_is_incomplete_and_retried(self):
        captures = []
        result, _events = self._generate(
            [
                [json.JSONDecodeError("malformed SSE", "not-json", 0)],
                _official_payload(content="complete", finish_reason="stop"),
            ],
            captures,
        )
        self.assertEqual(result["message"], "complete")
        first = result["transport_attempts"][0]
        self.assertEqual(first["status"], "retryable_error")
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertEqual(first["response_started_http_status"], 200)
        self.assertFalse(first["generation_delta_seen"])

    def test_transport_observability_failure_is_not_stream_retryable(self):
        failure = httpx.ReadError("connection reset in local evidence sink")
        failure._anchorpatch_transport_observability_failure = True
        failure._opencode_attempt = {
            "response_started_http_status": 200,
            "generation_delta_seen": False,
        }
        self.assertFalse(
            model_openai._is_incomplete_stream_exception(failure)
        )
        self.assertFalse(
            model_openai._is_retryable_opencode_error(failure)
        )

    def test_503_retries_do_not_consume_retry_budget(self):
        captures = []
        body = {
            "error_name": "failover_exhausted",
            "detail": "Inference is temporarily unavailable",
            "retry_after": 12,
        }
        result, _events = self._generate(
            [
                model_openai._HTTPStatusError(
                    503, json.dumps(body, ensure_ascii=False)),
                model_openai._HTTPStatusError(503, "unavailable"),
                model_openai._HTTPStatusError(503, "unavailable"),
                _official_payload(content="Hello", finish_reason="stop"),
            ],
            captures,
            max_retries=1,
        )
        self.assertEqual(result["http_attempts_used"], 4)
        self.assertEqual(result["retry_count"], 3)
        failed = result["transport_attempts"][:-1]
        self.assertEqual([item["http_status"] for item in failed], [503, 503, 503])
        self.assertTrue(all(
            item["retry_budget_consumed"] is False for item in failed
        ))
        self.assertTrue(all(
            item["retry_budget_attempt_index"] == 0 for item in failed
        ))
        self.assertEqual(failed[0]["error_body"], body)
        self.assertEqual(failed[0]["retry_after_seconds"], 12)
        self.assertGreaterEqual(failed[1]["retry_after_seconds"], 10)
        self.assertGreater(
            failed[2]["retry_after_seconds"],
            failed[1]["retry_after_seconds"],
        )
        self.assertLessEqual(failed[2]["retry_after_seconds"], 300)

    def test_partial_stream_is_discarded_and_retried(self):
        captures = []
        partial = [{
            "id": "partial",
            "model": "deepseek-v4-flash",
            "choices": [{
                "index": 0,
                "delta": {"content": "must-not-leak"},
                "finish_reason": None,
            }],
            "usage": None,
        }]
        result, _events = self._generate(
            [partial, _official_payload(content="complete")],
            captures,
        )
        self.assertEqual(result["message"], "complete")
        self.assertEqual(result["failed_attempt_count"], 1)
        first = result["transport_attempts"][0]
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertEqual(first["http_status"], 200)
        self.assertEqual(first["response_started_http_status"], 200)
        self.assertTrue(first["generation_delta_seen"])
        self.assertTrue(first["retry_budget_consumed"])
        self.assertNotIn("must-not-leak", result["message"])

    def test_503_after_generation_delta_consumes_stream_retry_budget(self):
        captures = []
        partial = [
            {
                "id": "partial-503",
                "model": "deepseek-v4-flash",
                "choices": [{
                    "index": 0,
                    "delta": {"content": "must-not-leak"},
                    "finish_reason": None,
                }],
                "usage": None,
            },
            model_openai._HTTPStatusError(
                503,
                json.dumps({
                    "error_name": "failover_exhausted",
                    "detail": "Inference is temporarily unavailable",
                })),
        ]
        result, _events = self._generate(
            [partial, _official_payload(content="complete")],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertEqual(first["http_status"], 503)
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertTrue(first["generation_delta_seen"])
        self.assertTrue(first["retry_budget_consumed"])
        self.assertEqual(first["retry_budget_attempt_index"], 1)
        self.assertEqual(result["message"], "complete")

    def test_usage_before_finish_is_not_terminal_usage(self):
        captures = []
        malformed = [{
            "id": "early-usage",
            "model": "deepseek-v4-flash",
            "choices": [],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }, *_stream_chunks_from_payload(_official_payload(content="bad"))]
        result, _events = self._generate(
            [malformed, _official_payload(content="complete")],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertFalse(first["terminal_sequence_valid"])
        self.assertEqual(first["http_status"], 200)
        self.assertEqual(result["message"], "complete")

    def test_empty_terminal_usage_is_incomplete(self):
        captures = []
        malformed = _stream_chunks_from_payload(
            _official_payload(content="bad"))
        malformed[-1] = {**malformed[-1], "usage": {}}
        result, _events = self._generate(
            [malformed, _official_payload(content="complete")],
            captures,
        )
        first = result["transport_attempts"][0]
        self.assertEqual(first["error_type"], "incomplete_stream")
        self.assertFalse(first["final_usage_seen"])
        self.assertEqual(first["http_status"], 200)
        self.assertEqual(result["message"], "complete")

    def test_reasoning_only_stream_is_complete_model_empty(self):
        captures = []
        payload = _official_payload(content="", finish_reason="stop")
        result, _events = self._generate([payload], captures)
        self.assertEqual(result["message"], "")
        self.assertTrue(result["stream_complete"])
        self.assertEqual(result["response_classification"], "model_empty")
        attempt = result["transport_attempts"][-1]
        self.assertTrue(attempt["thinking_delta_seen"])
        self.assertTrue(attempt["final_usage_seen"])
        self.assertTrue(attempt["terminal_sequence_valid"])

    def test_502_body_is_preserved_redacted_and_retry_after_is_honored(self):
        captures = []
        body = {
            "error_name": "origin_bad_gateway",
            "retry_after": 60,
            "detail": "unit-test-key",
        }
        result, _events = self._generate(
            [
                model_openai._HTTPStatusError(
                    502, json.dumps(body, ensure_ascii=False)),
                _official_payload(),
            ],
            captures,
        )
        attempt = result["transport_attempts"][0]
        self.assertEqual(attempt["http_status"], 502)
        self.assertEqual(attempt["error_body"], {
            "error_name": "origin_bad_gateway",
            "retry_after": 60,
            "detail": "<redacted-key>",
        })
        self.assertEqual(attempt["retry_after_seconds"], 60)
        self.assertTrue(attempt["retry_budget_consumed"])

    def test_terminal_5xx_traceback_does_not_retain_provider_secret(self):
        captures = []
        secret = "unit-test-key"
        body = json.dumps({
            "error_name": "origin_bad_gateway",
            "detail": secret,
        })
        fake_cls = _official_client_factory(
            [model_openai._HTTPStatusError(502, body)], captures)
        events = []
        with mock.patch.dict(os.environ, {
                "OPENAI_API_KEY": secret,
                "OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1",
                "AZURE_OPENAI_API_KEY": "",
                "AZURE_OPENAI_ENDPOINT": "",
        }, clear=False), mock.patch.object(
                model_openai, "OpenAI", fake_cls), mock.patch.object(
                    model_openai.time, "sleep"):
            with self.assertRaises(
                    model_openai.OpenAICompatibleTransportError) as caught:
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}],
                    model="deepseek-v4-flash",
                    max_tokens=20000,
                    max_retries=1,
                    reasoning_effort="high",
                    return_metadata=True,
                    _raw_event_sink=events.append,
                )

        rendered = "".join(traceback.format_exception(
            caught.exception))
        attempts = json.dumps(
            caught.exception.transport_attempts, ensure_ascii=False)
        self.assertNotIn(secret, rendered)
        self.assertNotIn(secret, str(caught.exception.last_error))
        self.assertNotIn(secret, attempts)
        self.assertIn("<redacted-key>", attempts)
        self.assertIsNone(caught.exception.__context__)
        self.assertIsNone(caught.exception.__cause__)
        frame_last_errors = []
        trace = caught.exception.__traceback__
        while trace is not None:
            if trace.tb_frame.f_code.co_filename == model_openai.__file__:
                retained = trace.tb_frame.f_locals.get("last_err")
                if retained is not None:
                    frame_last_errors.append(retained)
            trace = trace.tb_next
        self.assertNotIn(secret, repr(frame_last_errors))

    def test_runtime_config_and_reasoning_validation(self):
        with mock.patch.dict(
            os.environ,
            {"OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1"},
            clear=False,
        ):
            config = model_openai.model_runtime_config(
                "deepseek-v4-flash",
                max_tokens=20000,
                reasoning_effort="high",
            )
        self.assertEqual(config["provider"], "opencode_zen")
        self.assertEqual(
            config["transport_revision"], "opencode_openai_compatible/5")
        self.assertEqual(config["transport"], "openai_sdk_stream")
        self.assertEqual(config["reasoning_effort"], "high")
        with self.assertRaises(ValueError):
            model_openai._effective_reasoning_effort("ultra")

    def test_opencode_zen_rejects_inherited_azure_client_environment(self):
        with mock.patch.dict(os.environ, {
            "OPENAI_API_KEY": "unit-test-key",
            "OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1",
            "AZURE_OPENAI_API_KEY": "azure-key",
            "AZURE_OPENAI_ENDPOINT": "https://azure.invalid",
        }, clear=False), mock.patch.object(
                model_openai, "AzureOpenAI") as azure_client:
            with self.assertRaisesRegex(RuntimeError, "forbids inherited"):
                model_openai.model_runtime_config(
                    "deepseek-v4-flash",
                    max_tokens=20000,
                    reasoning_effort="high",
                )
            with self.assertRaisesRegex(RuntimeError, "forbids inherited"):
                model_openai.OpenAI_Model()._default_client()
        azure_client.assert_not_called()


class DeepSeekOpenCodeCampaignTests(unittest.TestCase):
    @staticmethod
    def _write_inspection_fixture(out_dir):
        sample = "sample"
        worker_id = "worker-a"
        worker_pid = os.getpid()
        methods = ["hybridpatch", "fullrewrite"]
        plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
        states = [
            f"state-{index}"
            for index in range(
                1, paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS + 1)
        ]
        utils_relay_plan.save_relay_task_plan(plan_path, states)
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": "1" * 40,
            "config": {
                "campaign_role": "deepseek_full234",
                "samples": [sample],
                "method_set": methods,
                "num_round_trips": (
                    paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "provider": "opencode_zen",
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "transport_resume_policy": None,
                "openai_base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "slots_per_key": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY),
                "key_count": paired_dispatch.DEEPSEEK_FULL_KEY_COUNT,
                "max_worker_count": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY
                    * paired_dispatch.DEEPSEEK_FULL_KEY_COUNT),
            },
            "task_plans": {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": states,
                },
            },
        }
        run_meta.write_json_atomic(
            os.path.join(out_dir, "dispatch_manifest.json"), manifest)
        paired_dispatch._write_active_worker_set(
            out_dir, manifest, [{
                "worker_launch_id": worker_id,
                "sample": sample,
            }])
        dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
        run_meta.append_jsonl_locked(dispatch_log, {
            "event": "launch",
            "worker_launch_id": worker_id,
            "sample": sample,
            "pid": worker_pid,
        })
        run_meta.append_jsonl_locked(dispatch_log, {
            "event": "worker_authorized",
            "worker_launch_id": worker_id,
            "sample": sample,
            "worker_pid": worker_pid,
        })
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "run_metadata.jsonl"), {
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": "invocation-a",
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "samples": [sample],
                "methods": methods,
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "provider": "opencode_zen",
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "transport_resume_policy": None,
                "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "campaign_config": {
                    "reasoning_effort": (
                        paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                },
                "status": "running",
                "finished_at": None,
            })
        for method in methods:
            os.makedirs(os.path.join(out_dir, method), exist_ok=True)
        return manifest, sample, worker_id, worker_pid

    @staticmethod
    def _write_dispatcher_parent_loss_fixture(
            out_dir, stages, *, record_stop=True):
        commit = "1" * 40
        fingerprint = {
            "model_openai.py": "2" * 64,
            "paired_campaign_dispatch.py": "3" * 64,
            "run_meta.py": "4" * 64,
        }
        methods = ["hybridpatch", "fullrewrite"]
        dispatcher_pid = 43210
        dispatcher_instance_id = "dispatcher-instance-a"
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": commit,
            "git_tree_state": "clean",
            "code_fingerprint": fingerprint,
            "config": {
                "campaign_role": "deepseek_full234",
                "samples": [
                    f"parent-loss-{index}"
                    for index in range(len(stages))
                ],
                "method_set": methods,
                "num_round_trips": (
                    paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "provider": "opencode_zen",
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "transport_resume_policy": None,
                "openai_base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "slots_per_key": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY),
                "key_count": paired_dispatch.DEEPSEEK_FULL_KEY_COUNT,
                "max_worker_count": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY
                    * paired_dispatch.DEEPSEEK_FULL_KEY_COUNT),
            },
            "task_plans": {},
        }
        run_meta.write_json_atomic(
            os.path.join(out_dir, "dispatch_manifest.json"), manifest)
        workers = []
        details = []
        for index, stage in enumerate(stages):
            sample = f"parent-loss-{index}"
            worker_id = f"worker-parent-loss-{index}"
            worker_pid = (
                None
                if stage == "registered_prelaunch"
                else os.getpid() + index
            )
            item = {
                "sample": sample,
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "invocation_id": (
                    f"invocation-parent-loss-{index}"
                    if stage == "running" else None
                ),
                "stage": stage,
            }
            details.append(item)
            workers.append({
                "sample": sample,
                "worker_launch_id": worker_id,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            })
        paired_dispatch._write_active_worker_set(
            out_dir, manifest, workers)

        dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
        metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
        for index, item in enumerate(details):
            intent = {
                "event": "launch_intent",
                "sample": item["sample"],
                "key_label": f"KEY_{index + 1}",
                "methods": methods,
                "worker_launch_id": item["worker_launch_id"],
                "console_log": (
                    f"dispatch_logs/{item['sample']}.console.log"),
                "method_phase": None,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            }
            run_meta.append_jsonl_locked(dispatch_path, intent)
            if item["stage"] == "registered_prelaunch":
                continue
            run_meta.append_jsonl_locked(dispatch_path, {
                **intent,
                "event": "launch",
                "pid": item["worker_pid"],
            })
            if item["stage"] != "running":
                continue
            run_meta.append_jsonl_locked(dispatch_path, {
                "event": "worker_authorized",
                "worker_launch_id": item["worker_launch_id"],
                "sample": item["sample"],
                "worker_pid": item["worker_pid"],
                "invocation_id": item["invocation_id"],
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            })
            run_meta.append_jsonl_locked(metadata_path, {
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": item["invocation_id"],
                "worker_launch_id": item["worker_launch_id"],
                "worker_pid": item["worker_pid"],
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "samples": [item["sample"]],
                "methods": methods,
                "method_phase": None,
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "provider": "opencode_zen",
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "transport_resume_policy": None,
                "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "campaign_config": {
                    "reasoning_effort": (
                        paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                },
                "run_git_commit": commit,
                "git_tree_state": "clean",
                "code_fingerprint": fingerprint,
                "status": "running",
                "finished_at": None,
            })

        if record_stop:
            stop_worker = next(
                item for item in details
                if item["stage"] != "registered_prelaunch"
            )
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": stop_worker[
                            "worker_launch_id"],
                    },
                    clear=False):
                run_meta.record_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=dispatcher_pid,
                    dispatcher_instance_id=dispatcher_instance_id,
                )
        return {
            "commit": commit,
            "fingerprint": fingerprint,
            "manifest": manifest,
            "dispatcher_pid": dispatcher_pid,
            "dispatcher_instance_id": dispatcher_instance_id,
            "workers": details,
        }

    @staticmethod
    def _deepseek_api_row(
            out_dir, sample, worker_id, worker_pid, direction, *,
            method="fullrewrite", call_kind=None, request_id=None):
        request_id = request_id or f"call-{direction}"
        call_kind = call_kind or (
            "hybridpatch_primary"
            if method == "hybridpatch" else "fullrewrite_primary")
        raw_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.request.json")
        sidecar_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.transport.jsonl")
        run_meta.write_json_atomic(raw_path, {
            "request_body": {
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_completion_tokens": (
                    paired_dispatch.DEEPSEEK_MAX_TOKENS),
                "stream": True,
                "stream_options": {"include_usage": True},
            },
        })
        attempt = {
            "attempt_index": 1,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "response_started_http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_delta_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "thinking_delta_seen": False,
            "text_delta_seen": True,
            "tool_delta_seen": False,
            "content_blocks_started": 0,
            "content_blocks_stopped": 0,
            "content_blocks_balanced": True,
            "terminal_sequence_valid": True,
        }
        for event in (
                {
                    "record_type": "attempt_start",
                    "attempt_index": 1,
                },
                {
                    "record_type": "sdk_stream_event",
                    "attempt_index": 1,
                    "event": {
                        "choices": [{
                            "delta": {"content": "fixture"},
                            "finish_reason": None,
                        }],
                    },
                },
                {
                    "record_type": "attempt_end",
                    "attempt_index": 1,
                    "attempt": attempt,
                }):
            run_meta.append_jsonl_locked(sidecar_path, event)
        return {
            "schema": paired_dispatch.API_CALL_SCHEMA,
            "request_id": request_id,
            "semantic_call_id": f"semantic-{direction}",
            "sample": sample,
            "method": method,
            "rt_index": 1,
            "direction": direction,
            "call_kind": call_kind,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
            "request_url": (
                f"{paired_dispatch.DEEPSEEK_BASE_URL}/chat/completions"),
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_resume_policy": None,
            "reasoning_effort": (
                paired_dispatch.DEEPSEEK_REASONING_EFFORT),
            "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
            "provider_called": True,
            "response_replayed": False,
            "generation_index": 0,
            "classification": None,
            "response_classification": "normal",
            "stream_complete": True,
            "http_status": 200,
            "input_tokens": 1,
            "output_tokens": 1,
            "transport_attempts": [attempt],
            "http_attempts_used": 1,
            "retry_count": 0,
            "failed_attempt_count": 0,
            "max_retries": 3,
            "raw_request_saved_path": raw_path,
            "raw_sse_saved_path": sidecar_path,
        }

    @staticmethod
    def _deepseek_failed_api_row(
            out_dir, sample, worker_id, worker_pid, *, method,
            direction, call_kind, request_id):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 502,
                "response_started_http_status": None,
                "stream_complete": False,
                "message_start_seen": False,
                "message_delta_seen": False,
                "message_stop_seen": False,
                "final_usage_seen": False,
                "generation_delta_seen": False,
                "thinking_delta_seen": False,
                "text_delta_seen": False,
                "tool_delta_seen": False,
                "content_blocks_started": 0,
                "content_blocks_stopped": 0,
                "content_blocks_balanced": False,
                "terminal_sequence_valid": False,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": index,
            }
            for index in range(1, 4)
        ]
        sidecar_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.transport.jsonl")
        for attempt in attempts:
            run_meta.append_jsonl_locked(sidecar_path, {
                "record_type": "attempt_start",
                "attempt_index": attempt["attempt_index"],
            })
            run_meta.append_jsonl_locked(sidecar_path, {
                "record_type": "attempt_end",
                "attempt_index": attempt["attempt_index"],
                "attempt": attempt,
            })
        return {
            "schema": paired_dispatch.API_CALL_SCHEMA,
            "request_id": request_id,
            "semantic_call_id": f"semantic-{request_id}",
            "sample": sample,
            "method": method,
            "rt_index": 1,
            "direction": direction,
            "call_kind": call_kind,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
            "request_url": (
                f"{paired_dispatch.DEEPSEEK_BASE_URL}/chat/completions"),
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_resume_policy": None,
            "reasoning_effort": (
                paired_dispatch.DEEPSEEK_REASONING_EFFORT),
            "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
            "provider_called": True,
            "response_replayed": False,
            "generation_index": 0,
            "classification": "provider/API failure",
            "response_classification": None,
            "error_type": "server_error",
            "http_status": 502,
            "raw_content_length": 0,
            "stream_complete": False,
            "count_as_method_failure": False,
            "input_tokens": None,
            "output_tokens": None,
            "transport_attempts": attempts,
            "http_attempts_used": 3,
            "retry_count": 2,
            "failed_attempt_count": 3,
            "max_retries": 3,
            "raw_request_saved_path": None,
            "raw_sse_saved_path": sidecar_path,
        }

    @staticmethod
    def _deepseek_infrastructure_outcome(
            sample, worker_id, worker_pid, api_row):
        methods = ["hybridpatch", "fullrewrite"]
        return {
            "schema": run_meta.SAMPLE_OUTCOME_SCHEMA,
            "created_at": datetime.now().astimezone().isoformat(),
            "sample": sample,
            "status": "infrastructure_incomplete",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": "invocation-a",
            "methods": methods,
            "method": api_row["method"],
            "rt_index": api_row["rt_index"],
            "direction": api_row["direction"],
            "call_kind": api_row["call_kind"],
            "semantic_root_id": api_row.get("semantic_root_id"),
            "semantic_call_id": api_row["semantic_call_id"],
            "generation_index": api_row["generation_index"],
            "parent_semantic_call_id": api_row.get(
                "parent_semantic_call_id"),
            "request_fingerprint": api_row.get("request_fingerprint"),
            "request_id": api_row["request_id"],
            "error_type": api_row["error_type"],
            "classification": api_row["classification"],
            "response_slots_used": api_row.get("response_slots_used"),
            "transient_failure_count": api_row.get(
                "transient_failure_count"),
            "http_attempts_used": api_row["http_attempts_used"],
            "next_attempt_index": api_row["http_attempts_used"] + 1,
            "transport_recovery_index": api_row.get(
                "transport_recovery_index"),
            "checkpoint_progress": {
                method: {
                    "completed_round_trips": 0,
                    "committed_rows": 0,
                }
                for method in methods
            },
            "evidence": {
                "api_calls": "api_calls.jsonl",
                "attempt_ledger": "api_attempt_ledger.jsonl",
                "run_metadata": "run_metadata.jsonl",
            },
        }

    @staticmethod
    def _fullrewrite_result_rows(sample, call_ids):
        return [
            {
                "sample_id": sample,
                "method": "fullrewrite",
                "round_trip_num": 1,
                "round_trip_direction": direction,
                "api_call_ids": [call_id],
                "model_name": paired_dispatch.DEEPSEEK_MODEL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "evaluation": (
                    {"score": 1.0} if direction == "backward" else {}),
            }
            for direction, call_id in zip(
                ("forward", "backward"), call_ids)
        ]

    def test_live_inspection_avoids_deepseek_api_linkage_torn_snapshot(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_rows = [
                self._deepseek_api_row(
                    out_dir, sample, worker_id, worker_pid, direction)
                for direction in ("forward", "backward")
            ]
            result_rows = self._fullrewrite_result_rows(
                sample, [row["request_id"] for row in api_rows])
            api_snapshot_taken = threading.Event()
            writer_done = threading.Event()
            inspection_results = []
            inspection_errors = []
            real_read_jsonl = paired_dispatch._read_jsonl

            def blocked_read_jsonl(path):
                rows = real_read_jsonl(path)
                if (os.path.basename(path) == "api_calls.jsonl"
                        and not api_snapshot_taken.is_set()):
                    api_snapshot_taken.set()
                    if not writer_done.wait(5):
                        raise RuntimeError(
                            "unit-test publication barrier timed out")
                return rows

            def inspect_live_campaign():
                try:
                    inspection_results.append(
                        paired_dispatch.inspect_campaign(
                            out_dir, manifest,
                            active_samples={sample}))
                except BaseException as exc:
                    inspection_errors.append(exc)

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=blocked_read_jsonl):
                inspector = threading.Thread(
                    target=inspect_live_campaign, daemon=True)
                inspector.start()
                self.assertTrue(api_snapshot_taken.wait(5))
                try:
                    for row in api_rows:
                        run_meta.append_jsonl_locked(
                            os.path.join(out_dir, "api_calls.jsonl"), row)
                    commit = run_meta.append_relay_rows_and_checkpoint(
                        os.path.join(
                            out_dir, "fullrewrite", f"{sample}.jsonl"),
                        os.path.join(
                            out_dir, "fullrewrite",
                            f"{sample}.ckpt.json"),
                        result_rows, {"completed_round_trips": 1},
                        campaign_out_dir=out_dir)
                    self.assertEqual(commit["status"], "appended")
                finally:
                    writer_done.set()
                inspector.join(5)

            self.assertFalse(inspector.is_alive())
            self.assertEqual(inspection_errors, [])
            self.assertEqual(len(inspection_results), 1)
            self.assertEqual(inspection_results[0]["errors"], [])
            self.assertEqual(len(real_read_jsonl(os.path.join(
                out_dir, "api_calls.jsonl"))), 2)
            self.assertEqual(len(real_read_jsonl(os.path.join(
                out_dir, "fullrewrite", f"{sample}.jsonl"))), 2)

    def test_active_retry_exhaustion_waits_for_sample_outcome_publication(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            attempts = [
                {
                    "attempt_index": index,
                    "status": "retryable_error",
                    "error_type": "server_error",
                    "http_status": 502,
                    "response_started_http_status": None,
                    "stream_complete": False,
                    "message_start_seen": False,
                    "message_delta_seen": False,
                    "message_stop_seen": False,
                    "final_usage_seen": False,
                    "generation_delta_seen": False,
                    "thinking_delta_seen": False,
                    "text_delta_seen": False,
                    "tool_delta_seen": False,
                    "content_blocks_started": 0,
                    "content_blocks_stopped": 0,
                    "content_blocks_balanced": False,
                    "terminal_sequence_valid": False,
                    "retry_budget_consumed": True,
                    "retry_budget_attempt_index": index,
                }
                for index in range(1, 4)
            ]
            request_id = "call-provisional-failure"
            sidecar_path = os.path.join(
                out_dir, "api_raw",
                f"{request_id}.transport.jsonl")
            for attempt in attempts:
                run_meta.append_jsonl_locked(sidecar_path, {
                    "record_type": "attempt_start",
                    "attempt_index": attempt["attempt_index"],
                })
                run_meta.append_jsonl_locked(sidecar_path, {
                    "record_type": "attempt_end",
                    "attempt_index": attempt["attempt_index"],
                    "attempt": attempt,
                })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), {
                    "schema": paired_dispatch.API_CALL_SCHEMA,
                    "request_id": request_id,
                    "semantic_call_id": "semantic-provisional",
                    "sample": sample,
                    "method": "fullrewrite",
                    "rt_index": 1,
                    "direction": "forward",
                    "call_kind": "fullrewrite_primary",
                    "worker_launch_id": worker_id,
                    "worker_pid": worker_pid,
                    "model": paired_dispatch.DEEPSEEK_MODEL,
                    "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                    "request_url": (
                        f"{paired_dispatch.DEEPSEEK_BASE_URL}"
                        "/chat/completions"),
                    "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                    "transport_revision": (
                        paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                    "transport_resume_policy": None,
                    "reasoning_effort": (
                        paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                    "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    "provider_called": True,
                    "response_replayed": False,
                    "generation_index": 0,
                    "classification": "provider/API failure",
                    "response_classification": None,
                    "error_type": "server_error",
                    "http_status": 502,
                    "raw_content_length": 0,
                    "stream_complete": False,
                    "count_as_method_failure": False,
                    "input_tokens": None,
                    "output_tokens": None,
                    "transport_attempts": attempts,
                    "http_attempts_used": 3,
                    "retry_count": 2,
                    "failed_attempt_count": 3,
                    "max_retries": 3,
                    "raw_request_saved_path": None,
                    "raw_sse_saved_path": sidecar_path,
                })
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                active = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples={sample})
                inactive = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples=set())
            self.assertEqual(active["errors"], [])
            self.assertIn(
                "DeepSeek API response incomplete at row 1",
                inactive["errors"])

    def test_active_infrastructure_outcome_waits_for_metadata_finish(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_row = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="hybridpatch", direction="forward",
                call_kind="hybridpatch_primary",
                request_id="call-outcome-before-metadata")
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), api_row)
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "sample_outcomes.jsonl"),
                self._deepseek_infrastructure_outcome(
                    sample, worker_id, worker_pid, api_row))

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                active = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples={sample})
                with self.assertRaisesRegex(
                        RuntimeError, "run metadata mismatch"):
                    paired_dispatch._inspect_deepseek_campaign(
                        out_dir, manifest, active_samples=set())

            self.assertEqual(active["errors"], [])

    def test_infrastructure_outcome_snapshot_precedes_api_snapshot(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_row = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="hybridpatch", direction="forward",
                call_kind="hybridpatch_primary",
                request_id="call-after-api-snapshot")
            outcome = self._deepseek_infrastructure_outcome(
                sample, worker_id, worker_pid, api_row)
            api_snapshot_taken = threading.Event()
            writer_done = threading.Event()
            inspection_results = []
            inspection_errors = []
            real_read_jsonl = paired_dispatch._read_jsonl

            def blocked_read_jsonl(path):
                rows = real_read_jsonl(path)
                if (os.path.basename(path) == "api_calls.jsonl"
                        and not api_snapshot_taken.is_set()):
                    api_snapshot_taken.set()
                    if not writer_done.wait(5):
                        raise RuntimeError(
                            "unit-test publication barrier timed out")
                return rows

            def inspect_live_campaign():
                try:
                    inspection_results.append(
                        paired_dispatch._inspect_deepseek_campaign(
                            out_dir, manifest, active_samples={sample}))
                except BaseException as exc:
                    inspection_errors.append(exc)

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=blocked_read_jsonl):
                inspector = threading.Thread(
                    target=inspect_live_campaign, daemon=True)
                inspector.start()
                self.assertTrue(api_snapshot_taken.wait(5))
                try:
                    run_meta.append_jsonl_locked(
                        os.path.join(out_dir, "api_calls.jsonl"), api_row)
                    run_meta.append_jsonl_locked(
                        os.path.join(out_dir, "sample_outcomes.jsonl"),
                        outcome)
                finally:
                    writer_done.set()
                inspector.join(5)

            self.assertFalse(inspector.is_alive())
            self.assertEqual(inspection_errors, [])
            self.assertEqual(len(inspection_results), 1)
            self.assertEqual(inspection_results[0]["errors"], [])

    def test_retry_incident_cohort_covers_uncommitted_primary_calls(self):
        with tempfile.TemporaryDirectory() as out_dir:
            _manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            forward = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                request_id="call-fr-forward")
            backward_failure = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="fullrewrite", direction="backward",
                call_kind="fullrewrite_primary",
                request_id="call-fr-backward-failure")
            fullrewrite_outcome = {
                "sample": sample,
                "method": "fullrewrite",
                "rt_index": 1,
                "direction": "backward",
                "call_kind": "fullrewrite_primary",
            }
            fullrewrite_evidence = {
                "request_id": backward_failure["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
            }
            self.assertEqual(
                paired_dispatch
                ._verified_deepseek_uncommitted_incident_request_ids(
                    out_dir, fullrewrite_outcome,
                    fullrewrite_evidence,
                    [forward, backward_failure], set()),
                {"call-fr-forward", "call-fr-backward-failure"},
            )

        with tempfile.TemporaryDirectory() as out_dir:
            _manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            primary = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                method="hybridpatch",
                call_kind="hybridpatch_primary",
                request_id="call-hp-primary")
            repair_failure = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="hybridpatch", direction="forward",
                call_kind="hybridpatch_repair",
                request_id="call-hp-repair-failure")
            hybrid_outcome = {
                "sample": sample,
                "method": "hybridpatch",
                "rt_index": 1,
                "direction": "forward",
                "call_kind": "hybridpatch_repair",
            }
            hybrid_evidence = {
                "request_id": repair_failure["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
            }
            self.assertEqual(
                paired_dispatch
                ._verified_deepseek_uncommitted_incident_request_ids(
                    out_dir, hybrid_outcome, hybrid_evidence,
                    [primary, repair_failure], set()),
                {"call-hp-primary", "call-hp-repair-failure"},
            )

    def test_deepseek_inspection_rejects_persistent_missing_api_linkage(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            for direction in ("forward", "backward"):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    self._deepseek_api_row(
                        out_dir, sample, worker_id, worker_pid, direction))
            result_rows = self._fullrewrite_result_rows(
                sample, ["missing-forward", "missing-backward"])
            commit = run_meta.append_relay_rows_and_checkpoint(
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.jsonl"),
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.ckpt.json"),
                result_rows, {"completed_round_trips": 1},
                campaign_out_dir=out_dir)
            self.assertEqual(commit["status"], "appended")
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertEqual(set(inspection["errors"]), {
                "committed row API linkage mismatch: "
                "fullrewrite/sample/(1, 'forward')",
                "committed row API linkage mismatch: "
                "fullrewrite/sample/(1, 'backward')",
            })

    def test_current_stream_inspection_requires_matching_transport_sidecar(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_rows = [
                self._deepseek_api_row(
                    out_dir, sample, worker_id, worker_pid, direction)
                for direction in ("forward", "backward")
            ]
            api_rows[0]["raw_sse_saved_path"] = os.path.join(
                out_dir, "api_raw", "missing.transport.jsonl")
            for row in api_rows:
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            commit = run_meta.append_relay_rows_and_checkpoint(
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.jsonl"),
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.ckpt.json"),
                self._fullrewrite_result_rows(
                    sample, [row["request_id"] for row in api_rows]),
                {"completed_round_trips": 1},
                campaign_out_dir=out_dir,
            )
            self.assertEqual(commit["status"], "appended")
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "DeepSeek transport sidecar invalid at row 1",
                inspection["errors"],
            )

    def test_inspection_allows_only_hash_bound_historical_disconnect_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            disconnect = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                request_id="call-disconnect")
            disconnect_attempt = dict(
                disconnect["transport_attempts"][0],
                status="fatal_error",
                error_type="transport_disconnect",
                http_status=None,
                response_started_http_status=None,
                stream_complete=False,
                message_start_seen=False,
                message_delta_seen=False,
                message_stop_seen=False,
                final_usage_seen=False,
                generation_delta_seen=False,
                text_delta_seen=False,
                content_blocks_balanced=False,
                terminal_sequence_valid=False,
                retry_budget_consumed=True,
                retry_budget_attempt_index=1,
                error_message="Connection error.",
            )
            disconnect_sidecar = os.path.join(
                out_dir, "api_raw",
                "call-disconnect.failure.transport.jsonl")
            for event in ({
                    "record_type": "attempt_start",
                    "attempt_index": 1,
            }, {
                    "record_type": "attempt_end",
                    "attempt_index": 1,
                    "attempt": disconnect_attempt,
            }):
                run_meta.append_jsonl_locked(disconnect_sidecar, event)
            disconnect.update({
                "classification": "runner_exception",
                "response_classification": None,
                "error_type": "transport_disconnect",
                "http_status": None,
                "stream_complete": False,
                "count_as_method_failure": True,
                "input_tokens": None,
                "output_tokens": None,
                "transport_attempts": [disconnect_attempt],
                "http_attempts_used": 1,
                "retry_count": 0,
                "failed_attempt_count": 1,
                "raw_request_saved_path": None,
                "raw_sse_saved_path": disconnect_sidecar,
                "runner_exception": (
                    "OpenAICompatibleTransportError: OpenAI-compatible "
                    "provider failed after 1 attempt(s)"
                ),
            })

            stopped = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "backward",
                request_id="call-dispatcher-stopped")
            stopped.update({
                "classification": "runner_exception",
                "response_classification": None,
                "error_type": "CampaignStoppedError",
                "http_status": None,
                "stream_complete": False,
                "count_as_method_failure": True,
                "input_tokens": None,
                "output_tokens": None,
                "transport_attempts": [],
                "http_attempts_used": None,
                "retry_count": None,
                "failed_attempt_count": None,
                "raw_request_saved_path": None,
                "runner_exception": (
                    "CampaignStoppedError: campaign stop latch is set: "
                    "worker_fatal_error"
                ),
            })
            partial_attempt = {
                "attempt_index": 1,
                "status": "retryable_error",
                "http_status": 200,
                "error_type": "incomplete_stream",
                "error_message": (
                    "OpenAI-compatible stream ended without finish_reason "
                    "and final_usage"
                ),
                "response_started_http_status": 200,
                "stream_complete": False,
                "message_start_seen": True,
                "message_stop_seen": False,
                "final_usage_seen": False,
                "generation_delta_seen": True,
                "terminal_sequence_valid": False,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": 1,
            }
            partial_sidecar = os.path.join(
                out_dir, "api_raw",
                "call-dispatcher-stopped.partial.transport.jsonl")
            linkage = {
                "transport_event_schema": (
                    "anchorpatch.transport_event/1"),
                "call_id": stopped["request_id"],
                "worker_launch_id": worker_id,
                "sample": sample,
                "method": stopped["method"],
                "rt_index": stopped["rt_index"],
                "direction": stopped["direction"],
                "attempt_index": 1,
            }
            for event in ({
                    **linkage,
                    "record_type": "attempt_start",
            }, {
                    **linkage,
                    "record_type": "sdk_stream_event",
                    "event": {"choices": [{"delta": {"content": "x"}}]},
            }, {
                    **linkage,
                    "record_type": "attempt_end",
                    "attempt": partial_attempt,
            }):
                run_meta.append_jsonl_locked(partial_sidecar, event)
            stopped["raw_sse_saved_path"] = partial_sidecar
            rows = [disconnect, stopped]
            for row in rows:
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            incident_kinds = {
                run_meta._canonical_record_sha256(disconnect): (
                    "deepseek_transport_disconnect_misclassification"),
                run_meta._canonical_record_sha256(stopped): (
                    "deepseek_dispatcher_stopped_uncommitted_api"),
            }
            recovery = {
                "api_row_hashes": frozenset(incident_kinds),
                "api_incident_kinds": incident_kinds,
                "attempt_row_hashes": frozenset(),
                "transport_sidecar_hashes": frozenset(),
                "transport_sidecars_by_api_row_hash": {},
                "worker_launch_ids": frozenset({worker_id}),
                "preauthorization_worker_launch_ids": frozenset(),
                "provider_access_retry_authorizations": {},
                "provider_access_resume_samples": frozenset(),
                "dispatcher_parent_loss_workers": {},
                "authorization_id": "recovery-a",
            }
            partial_evidence = (
                run_meta._deepseek_dispatcher_stopped_sidecar_evidence(
                    out_dir, stopped)
            )
            recovery["transport_sidecar_hashes"] = frozenset({
                partial_evidence["sha256"],
            })
            recovery["transport_sidecars_by_api_row_hash"] = {
                run_meta._canonical_record_sha256(stopped):
                partial_evidence,
            }
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch,
                        "campaign_recovery_incident_evidence",
                        return_value=recovery):
                accepted = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertEqual(accepted["errors"], [])

            run_meta.append_jsonl_locked(partial_sidecar, {
                **linkage,
                "record_type": "sdk_stream_event",
                "event": {"choices": [{"delta": {"content": "tamper"}}]},
            })
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch,
                        "campaign_recovery_incident_evidence",
                        return_value=recovery):
                tampered = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "DeepSeek transport sidecar invalid at row 2",
                tampered["errors"],
            )

            forged = {
                **recovery,
                "api_row_hashes": frozenset({"0" * 64}),
                "api_incident_kinds": {
                    "0" * 64: (
                        "deepseek_transport_disconnect_misclassification"),
                },
            }
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch,
                        "campaign_recovery_incident_evidence",
                        return_value=forged):
                rejected = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "DeepSeek API response incomplete at row 1",
                rejected["errors"],
            )
            self.assertIn(
                "DeepSeek retry evidence invalid at row 2",
                rejected["errors"],
            )
            self.assertIn(
                "DeepSeek transport sidecar invalid at row 2",
                rejected["errors"],
            )

    def test_recovery_file_prefix_allows_append_but_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "ledger.jsonl")
            with open(path, "wb") as handle:
                handle.write(b'{"row":1}\n')
            evidence = ledger_recovery._file_prefix_evidence(path)
            with open(path, "ab") as handle:
                handle.write(b'{"row":2}\n')
            self.assertTrue(run_meta._file_prefix_matches(path, evidence))
            with open(path, "r+b") as handle:
                handle.write(b"X")
            self.assertFalse(run_meta._file_prefix_matches(path, evidence))

    def test_recovery_metadata_allows_finish_and_manifest_plan_extension(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "run_metadata.jsonl")
            old_plan = {"sha256": "1" * 64, "round_trips": 10}
            new_plan_path = os.path.join(out_dir, "new.task_plan.json")
            run_meta.write_json_atomic(new_plan_path, {
                "forward_state_sequence": [
                    f"state-{index}" for index in range(10)
                ],
            })
            new_plan = {
                "sha256": run_meta._sha256_file(new_plan_path),
                "round_trips": 10,
            }
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"),
                {
                    "config": {"num_round_trips": 10},
                    "task_plans": {
                        "old": {"sha256": old_plan["sha256"]},
                        "new": {"sha256": new_plan["sha256"]},
                    },
                },
            )
            rows = [{
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": "old-a",
                "worker_launch_id": "worker-a",
                "status": "failed",
                "finished_at": "2026-07-28T10:26:09+08:00",
                "invocation_finished_at": (
                    "2026-07-28T10:26:09+08:00"),
                "task_plans": {"old": old_plan},
            }]
            run_meta.append_jsonl_locked(path, rows[0])
            identities = run_meta._run_metadata_recovery_identities(path)

            rows[0]["finished_at"] = None
            rows.append({
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": "new-b",
                "worker_launch_id": "worker-b",
                "status": "running",
                "finished_at": None,
                "task_plans": {"old": old_plan},
            })
            run_meta._write_jsonl_atomic(path, rows)
            run_meta.register_task_plan(
                out_dir, "new", new_plan_path, num_round_trips=10)
            self.assertTrue(
                run_meta._run_metadata_recovery_prefix_matches(
                    path, identities))

            run_meta.finish_run_metadata(
                out_dir, "new-b", status="finished")
            self.assertTrue(
                run_meta._run_metadata_recovery_prefix_matches(
                    path, identities))
            rows = run_meta.read_run_metadata_snapshot(out_dir)
            rows[0]["status"] = "forged"
            run_meta._write_jsonl_atomic(path, rows)
            self.assertFalse(
                run_meta._run_metadata_recovery_prefix_matches(
                    path, identities))

    def test_deepseek_transport_recovery_requires_free_dispatcher_lease(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaisesRegex(
                            RuntimeError,
                            "dispatcher lease to be free"):
                        ledger_recovery.authorize(
                            out_dir,
                            deepseek_transport_disconnect_retry=True,
                        )
                finally:
                    portalocker.unlock(lease)

    def test_resume_classifier_pending_requires_explicit_recovery_mode(self):
        with tempfile.TemporaryDirectory() as out_dir:
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"),
                {"schema": paired_dispatch.SCHEMA},
            )
            run_meta.write_json_atomic(
                os.path.join(
                    out_dir,
                    run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
                ),
                {"followup_kind": "resume_classifier"},
            )
            with self.assertRaisesRegex(
                    RuntimeError, "requires its explicit recovery mode"):
                ledger_recovery.authorize(
                    out_dir,
                    deepseek_transport_disconnect_retry=True,
                )
            pending_path = os.path.join(
                out_dir,
                run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            )
            run_meta.write_json_atomic(pending_path, {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "authorization_record": {
                    "deepseek_resume_classifier_followup_recovery": True,
                    "authorization_basis": (
                        ledger_recovery
                        ._DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS),
                },
            })
            with self.assertRaisesRegex(
                    RuntimeError, "pending mode binding is invalid"):
                ledger_recovery.authorize(
                    out_dir,
                    deepseek_transport_disconnect_retry=True,
                )
            run_meta.write_json_atomic(pending_path, {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "followup_kind": "resume_classifier",
                "authorization_record": {},
            })
            with self.assertRaisesRegex(
                    RuntimeError, "pending mode binding is invalid"):
                ledger_recovery.authorize(
                    out_dir,
                    deepseek_resume_classifier_retry=True,
                )

    def _make_deepseek_initial_recovery_fixture(self, out_dir):
        manifest_path = os.path.join(
            out_dir, "dispatch_manifest.json")
        auth_path = os.path.join(
            out_dir,
            run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
        )
        stop_path = os.path.join(out_dir, "campaign_stop.json")
        dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
        api_path = os.path.join(out_dir, "api_calls.jsonl")
        manifest = {"schema": paired_dispatch.SCHEMA}
        stop = {
            "schema": run_meta.STOP_CONDITION_SCHEMA,
            "created_at": "2026-07-28T13:00:00+08:00",
            "condition": "worker_fatal_error",
            "error_type": "OpenAICompatibleTransportError",
            "error": "OpenAI-compatible provider failed after 1 attempt(s)",
            "sample": "sample-a",
            "worker_launch_id": "worker-a",
        }
        api_row = {
            "request_id": "request-a",
            "sample": "sample-a",
            "worker_launch_id": "worker-a",
        }
        run_meta.write_json_atomic(manifest_path, manifest)
        run_meta.write_json_atomic(stop_path, stop)
        run_meta.write_json_atomic(
            os.path.join(out_dir, "active_worker_set.json"),
            {"workers": {}},
        )
        run_meta.append_jsonl_locked(dispatch_path, {"event": "prior"})
        run_meta.append_jsonl_locked(api_path, api_row)
        api_row_sha256 = run_meta._canonical_record_sha256(api_row)
        sidecar = {
            "api_row_sha256": api_row_sha256,
            "sha256": "c" * 64,
        }
        fingerprint = {"run_meta.py": "fingerprint"}
        history_relative = (
            "recovery_history/deepseek-initial-transaction-test")
        archived_stop = os.path.join(
            out_dir, history_relative, "campaign_stop.json")
        record = {
            "schema": (
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
            "authorization_id": "deepseek-initial-auth",
            "created_at": "2026-07-28T13:01:00+08:00",
            "deepseek_transport_disconnect_retry_recovery": True,
            "deepseek_transport_disconnect_initial_transaction": True,
            "deepseek_transport_disconnect_initial_prefixes": {
                "dispatch_log.jsonl": (
                    ledger_recovery._file_prefix_evidence(dispatch_path)),
            },
            "deepseek_transport_disconnect_retry_samples": ["sample-a"],
            "deepseek_resume_samples": ["sample-a"],
            "deepseek_pending_samples": [],
            "dispatch_manifest_sha256": (
                run_meta._sha256_file(manifest_path)),
            "prior_git_commit": "b" * 40,
            "recovery_git_commit": "a" * 40,
            "recovery_code_fingerprint": fingerprint,
            "archived_stop_path": os.path.relpath(
                archived_stop, out_dir).replace("\\", "/"),
            "archived_stop_sha256": run_meta._sha256_file(stop_path),
            "archived_emergency_stop_records": [],
            "incident_api_rows": [{"row_number": 1}],
            "incident_transport_sidecars": [sidecar],
            "recovered_worker_launch_ids": ["worker-a"],
        }
        return {
            "manifest": manifest,
            "record": record,
            "sidecar": sidecar,
            "fingerprint": fingerprint,
            "auth_path": auth_path,
            "stop_path": stop_path,
            "dispatch_path": dispatch_path,
            "archived_stop": archived_stop,
        }

    def test_deepseek_initial_recovery_validates_sidecar_before_stop_archive(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._make_deepseek_initial_recovery_fixture(out_dir)
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fixture["fingerprint"]), \
                    mock.patch.object(
                        ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_dispatcher_stopped_sidecar_evidence",
                        return_value={
                            **fixture["sidecar"],
                            "sha256": "d" * 64,
                        }):
                with self.assertRaisesRegex(
                        RuntimeError, "sidecar has drifted"):
                    (
                        ledger_recovery
                        ._commit_deepseek_transport_disconnect_initial(
                            out_dir, fixture["manifest"],
                            fixture["stop_path"], fixture["auth_path"],
                            fixture["record"],
                        )
                    )
            self.assertTrue(os.path.isfile(fixture["stop_path"]))
            self.assertFalse(os.path.exists(fixture["auth_path"]))
            self.assertFalse(os.path.exists(fixture["archived_stop"]))

    def test_deepseek_initial_recovery_repairs_partial_witness(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._make_deepseek_initial_recovery_fixture(out_dir)
            record = fixture["record"]
            run_meta.write_json_atomic(fixture["auth_path"], record)
            event = {
                "event": (
                    "user_authorized_deepseek_transport_disconnect_"
                    "retry_recovery"),
                "created_at": record["created_at"],
                "campaign_recovery_authorization_id": (
                    record["authorization_id"]),
                "campaign_recovery_authorization_sha256": (
                    run_meta._sha256_file(fixture["auth_path"])),
                "deepseek_transport_disconnect_retry_samples": [
                    "sample-a"],
                "incident_api_rows": 1,
                "prior_git_commit": "b" * 40,
                "recovery_git_commit": "a" * 40,
            }
            encoded = (
                json.dumps(event, ensure_ascii=False).encode("utf-8")
                + b"\n"
            )
            with open(fixture["dispatch_path"], "ab") as handle:
                handle.write(encoded[:len(encoded) // 2])
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fixture["fingerprint"]), \
                    mock.patch.object(
                        ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_dispatcher_stopped_sidecar_evidence",
                        return_value=fixture["sidecar"]), \
                    mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        return_value={"authorization_sha256": "e" * 64}):
                result = (
                    ledger_recovery
                    ._commit_deepseek_transport_disconnect_initial(
                        out_dir, fixture["manifest"],
                        fixture["stop_path"], fixture["auth_path"],
                        record,
                    )
                )
            self.assertEqual(
                result["authorization_id"], "deepseek-initial-auth")
            self.assertFalse(os.path.exists(fixture["stop_path"]))
            self.assertTrue(os.path.isfile(fixture["archived_stop"]))
            witnesses = [
                row for row in ledger_recovery._read_jsonl(
                    fixture["dispatch_path"])
                if row.get("event") == event["event"]
            ]
            self.assertEqual(witnesses, [event])

    def test_deepseek_initial_recovery_cli_is_idempotent_after_stop_move(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._make_deepseek_initial_recovery_fixture(out_dir)
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fixture["fingerprint"]), \
                    mock.patch.object(
                        ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_dispatcher_stopped_sidecar_evidence",
                        return_value=fixture["sidecar"]), \
                    mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        side_effect=RuntimeError("simulated crash")):
                with self.assertRaisesRegex(
                        RuntimeError, "simulated crash"):
                    (
                        ledger_recovery
                        ._commit_deepseek_transport_disconnect_initial(
                            out_dir, fixture["manifest"],
                            fixture["stop_path"], fixture["auth_path"],
                            fixture["record"],
                        )
                    )
            self.assertFalse(os.path.exists(fixture["stop_path"]))
            self.assertTrue(os.path.isfile(fixture["auth_path"]))
            self.assertTrue(os.path.isfile(fixture["archived_stop"]))

            verified = {
                "authorization_id": "deepseek-initial-auth",
                "authorization_sha256": "e" * 64,
                "deepseek_transport_disconnect_retry_samples": [
                    "sample-a"],
                "incident_api_rows": [{"row_number": 1}],
                "recovered_worker_launch_ids": ["worker-a"],
                "deepseek_pending_samples": [],
                "committed_results_modified": False,
                "checkpoint_rows_modified": False,
                "provider_post_replay_scope": "uncommitted_steps_only",
            }
            with mock.patch.object(
                    ledger_recovery,
                    "read_campaign_recovery_authorization",
                    return_value=verified):
                result = ledger_recovery.authorize(
                    out_dir,
                    deepseek_transport_disconnect_retry=True,
                )
            self.assertTrue(result["already_authorized"])
            self.assertFalse(result["inspector_followup"])
            self.assertEqual(
                result["authorization_id"], "deepseek-initial-auth")

    def test_deepseek_inspector_pending_commit_is_idempotent(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest_path = os.path.join(
                out_dir, "dispatch_manifest.json")
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            pending_path = os.path.join(
                out_dir,
                run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            )
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            history_relative = "recovery_history/followup-auth"
            history_dir = os.path.join(out_dir, history_relative)
            archived_auth = os.path.join(
                history_dir,
                "superseded_campaign_recovery_authorization.json",
            )
            archived_stop = os.path.join(
                history_dir, "campaign_stop.json")
            manifest = {"schema": paired_dispatch.SCHEMA}
            prior_auth = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "prior-auth",
            }
            stop = {
                "schema": run_meta.STOP_CONDITION_SCHEMA,
                "condition": "dispatcher_integrity_failure",
                "error": "fixture",
            }
            run_meta.write_json_atomic(manifest_path, manifest)
            run_meta.write_json_atomic(auth_path, prior_auth)
            run_meta.write_json_atomic(stop_path, stop)
            run_meta.write_json_atomic(
                os.path.join(out_dir, "active_worker_set.json"),
                {"workers": {}},
            )
            run_meta.append_jsonl_locked(
                dispatch_path, {"event": "prior"})
            dispatch_prefix = (
                ledger_recovery._file_prefix_evidence(dispatch_path)
            )
            fingerprint = {"run_meta.py": "fingerprint"}
            prior_auth_sha256 = run_meta._sha256_file(auth_path)
            record = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "followup-auth",
                "created_at": "2026-07-28T14:00:00+08:00",
                "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
                "authorization_basis": (
                    "explicit_user_resume_after_deepseek_recovery_"
                    "inspector_fix"
                ),
                "deepseek_transport_disconnect_retry_recovery": True,
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery": True,
                "dispatch_manifest_sha256": (
                    run_meta._sha256_file(manifest_path)),
                "recovery_git_commit": "a" * 40,
                "recovery_git_tree_state": "clean",
                "recovery_code_fingerprint": fingerprint,
                "deepseek_resume_samples": ["sample-a"],
                "superseded_authorization_path": (
                    os.path.relpath(
                        archived_auth, out_dir).replace("\\", "/")),
                "superseded_authorization_sha256": (
                    prior_auth_sha256),
                "archived_stop_path": (
                    os.path.relpath(
                        archived_stop, out_dir).replace("\\", "/")),
                "archived_stop_sha256": (
                    run_meta._sha256_file(stop_path)),
                "archived_emergency_stop_records": [],
                "deepseek_transport_disconnect_inspector_"
                "prior_authorization_id": "prior-auth",
                "deepseek_transport_disconnect_inspector_"
                "prior_authorization_sha256": prior_auth_sha256,
                "deepseek_transport_disconnect_inspector_"
                "prior_recovery_git_commit": "b" * 40,
                "deepseek_transport_disconnect_inspector_prefixes": {
                    "dispatch_log.jsonl": dispatch_prefix,
                },
                "deepseek_transport_disconnect_retry_samples": [
                    "sample-a"],
                "incident_api_rows": [{"row_number": 1}],
                "recovered_worker_launch_ids": ["worker-a"],
                "deepseek_pending_samples": [],
                "committed_results_modified": False,
                "checkpoint_rows_modified": False,
                "provider_post_replay_scope": "uncommitted_steps_only",
            }
            pending = {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "created_at": "2026-07-28T14:00:00+08:00",
                "history_dir": history_relative,
                "authorization_record": record,
            }
            run_meta.write_json_atomic(pending_path, pending)
            verified = {
                "authorization_sha256": "c" * 64,
            }
            common_patches = (
                mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")),
                mock.patch.object(
                    ledger_recovery, "code_fingerprint",
                    return_value=fingerprint),
                mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"),
                mock.patch.object(
                    ledger_recovery,
                    "read_campaign_recovery_authorization",
                    return_value=verified),
            )
            malicious = json.loads(json.dumps(pending))
            malicious["authorization_record"][
                "superseded_authorization_path"
            ] = os.path.basename(auth_path)
            run_meta.write_json_atomic(pending_path, malicious)
            prior_auth_sha256 = run_meta._sha256_file(auth_path)
            prior_stop_sha256 = run_meta._sha256_file(stop_path)
            with common_patches[0], common_patches[1], \
                    common_patches[2], common_patches[3]:
                with self.assertRaisesRegex(
                        RuntimeError, "archive path has drifted"):
                    (
                        ledger_recovery
                        ._commit_deepseek_transport_inspector_followup(
                            out_dir, manifest, stop_path, auth_path,
                            pending_path, malicious
                        )
                    )
            self.assertEqual(
                run_meta._sha256_file(auth_path), prior_auth_sha256)
            self.assertEqual(
                run_meta._sha256_file(stop_path), prior_stop_sha256)
            self.assertFalse(os.path.exists(history_dir))
            run_meta.write_json_atomic(pending_path, pending)

            with common_patches[0], common_patches[1], \
                    common_patches[2], common_patches[3], \
                    mock.patch.object(
                        ledger_recovery,
                        "_unlink_with_sharing_retry"):
                first = (
                    ledger_recovery
                    ._commit_deepseek_transport_inspector_followup(
                        out_dir, manifest, stop_path, auth_path,
                        pending_path, pending
                    )
                )
            self.assertEqual(first["authorization_id"], "followup-auth")
            self.assertTrue(os.path.isfile(pending_path))
            self.assertFalse(os.path.isfile(stop_path))
            self.assertEqual(
                ledger_recovery._read_json(auth_path), record)

            with common_patches[0], common_patches[1], \
                    common_patches[2], common_patches[3]:
                second = (
                    ledger_recovery
                    ._commit_deepseek_transport_inspector_followup(
                        out_dir, manifest, stop_path, auth_path,
                        pending_path, pending
                    )
                )
            self.assertEqual(second["authorization_id"], "followup-auth")
            self.assertFalse(os.path.exists(pending_path))
            witness = [
                row for row in ledger_recovery._read_jsonl(dispatch_path)
                if row.get("event")
                == "user_authorized_deepseek_transport_inspector_followup"
            ]
            self.assertEqual(len(witness), 1)

    def test_deepseek_pristine_pending_reprepare_shortens_history_path(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            pending_path = os.path.join(
                out_dir,
                run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            )
            manifest_path = os.path.join(
                out_dir, "dispatch_manifest.json")
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            api_path = os.path.join(out_dir, "api_calls.jsonl")
            old_authorization_id = (
                "dsv4f-inspector-20260728T144037+0800-16a9a52c")
            old_history_relative = (
                "recovery_history/" + old_authorization_id)
            old_history_dir = os.path.join(
                out_dir, old_history_relative)
            os.makedirs(old_history_dir)
            prior_auth = {"authorization_id": "prior-auth"}
            stop = {"condition": "dispatcher_integrity_failure"}
            run_meta.write_json_atomic(
                manifest_path, {"schema": paired_dispatch.SCHEMA})
            run_meta.write_json_atomic(auth_path, prior_auth)
            run_meta.write_json_atomic(stop_path, stop)
            run_meta.write_json_atomic(
                os.path.join(out_dir, "active_worker_set.json"),
                {"workers": {}},
            )
            run_meta.append_jsonl_locked(
                dispatch_path, {"event": "prior"})
            run_meta.append_jsonl_locked(
                api_path, {"request_id": "request-a"})
            prepared_fingerprint = {
                "model_openai.py": "same",
                "run_meta.py": "old",
            }
            current_fingerprint = {
                "model_openai.py": "same",
                "run_meta.py": "new",
            }
            prepared_commit = "a" * 40
            current_commit = "c" * 40
            prior_recovery_commit = "b" * 40
            prior_git_commit = "0" * 40
            record = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": old_authorization_id,
                "created_at": "2026-07-28T14:40:37+08:00",
                "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
                "authorization_basis": (
                    "explicit_user_resume_after_deepseek_recovery_"
                    "inspector_fix"
                ),
                "deepseek_transport_disconnect_retry_recovery": True,
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery": True,
                "dispatch_manifest_sha256": (
                    run_meta._sha256_file(manifest_path)),
                "recovery_git_commit": prepared_commit,
                "recovery_git_tree_state": "clean",
                "recovery_code_fingerprint": prepared_fingerprint,
                "prior_git_commit": prior_git_commit,
                "deepseek_resume_samples": ["sample-a"],
                "incident_api_rows": [{"row_number": 1}],
                "recovered_worker_launch_ids": ["worker-a"],
                "deepseek_pending_samples": [],
                "archived_emergency_stop_records": [],
                "deepseek_transport_disconnect_inspector_"
                "prior_recovery_git_commit": prior_recovery_commit,
                "deepseek_transport_disconnect_inspector_"
                "delta_changed_paths": ["delta.py"],
                "changed_tracked_paths": ["cumulative.py"],
                "superseded_authorization_path": (
                    old_history_relative
                    + "/superseded_campaign_recovery_authorization.json"
                ),
                "superseded_authorization_sha256": (
                    run_meta._sha256_file(auth_path)),
                "archived_stop_path": (
                    old_history_relative + "/campaign_stop.json"),
                "archived_stop_sha256": (
                    run_meta._sha256_file(stop_path)),
                "deepseek_transport_disconnect_inspector_prefixes": {
                    "api_calls.jsonl": (
                        ledger_recovery._file_prefix_evidence(api_path)),
                    "dispatch_log.jsonl": (
                        ledger_recovery._file_prefix_evidence(
                            dispatch_path)),
                },
            }
            pending = {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "created_at": "2026-07-28T14:40:37+08:00",
                "history_dir": old_history_relative,
                "authorization_record": record,
            }
            run_meta.write_json_atomic(pending_path, pending)
            old_pending_sha256 = run_meta._sha256_file(pending_path)

            tooling_paths = sorted({
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/run_meta.py",
                "HP_V8/src/test_model_openai.py",
            })

            def changed_paths(prior, current):
                self.assertEqual(current, current_commit)
                if prior == prepared_commit:
                    return tooling_paths
                if prior == prior_recovery_commit:
                    return ["delta.py"]
                if prior == prior_git_commit:
                    return ["cumulative.py"]
                self.fail(f"unexpected prior commit: {prior}")

            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(current_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=current_fingerprint), \
                    mock.patch.object(
                        ledger_recovery, "_git_changed_paths",
                        side_effect=changed_paths), \
                    mock.patch.object(
                        ledger_recovery.subprocess, "run",
                        return_value=mock.Mock(
                            stdout=(
                                current_commit + " "
                                + prepared_commit + "\n"
                            ))):
                updated = (
                    ledger_recovery
                    ._reprepare_pristine_deepseek_inspector_pending(
                        out_dir, pending_path, pending
                    )
                )
                (
                    ledger_recovery
                    ._validate_deepseek_inspector_reprepare_evidence(
                        out_dir, updated,
                        updated["authorization_record"],
                    )
                )
                tampered_pending = json.loads(json.dumps(updated))
                tampered_pending["authorization_record"][
                    "deepseek_transport_inspector_pending_reprepared_"
                    "from_sha256"
                ] = "f" * 64
                with self.assertRaisesRegex(
                        RuntimeError, "prior pending archive mismatch"):
                    (
                        ledger_recovery
                        ._validate_deepseek_inspector_reprepare_evidence(
                            out_dir, tampered_pending,
                            tampered_pending["authorization_record"],
                        )
                    )
                stripped_pending = json.loads(json.dumps(updated))
                stripped_record = stripped_pending[
                    "authorization_record"]
                for key in list(stripped_record):
                    if key.startswith(
                            "deepseek_transport_inspector_pending_"
                            "reprepared_"):
                        stripped_record.pop(key)
                stripped_pending.pop("reprepared_at")
                with self.assertRaisesRegex(
                        RuntimeError, "provenance was removed"):
                    (
                        ledger_recovery
                        ._validate_deepseek_inspector_reprepare_evidence(
                            out_dir, stripped_pending, stripped_record,
                        )
                    )
            updated_record = updated["authorization_record"]
            self.assertTrue(
                updated_record["authorization_id"].startswith("dsi-"))
            self.assertLess(
                len(updated_record["authorization_id"]),
                len(old_authorization_id),
            )
            self.assertEqual(
                updated_record["recovery_git_commit"], current_commit)
            self.assertEqual(
                updated_record["recovery_code_fingerprint"],
                current_fingerprint,
            )
            self.assertEqual(
                updated_record[
                    "deepseek_transport_inspector_pending_"
                    "reprepared_from_sha256"
                ],
                old_pending_sha256,
            )
            self.assertEqual(
                updated_record[
                    "deepseek_transport_inspector_pending_reprepared_"
                    "from_authorization_id"
                ],
                old_authorization_id,
            )
            self.assertEqual(os.listdir(old_history_dir), ["pending.json"])
            archived_pending = os.path.join(
                old_history_dir, "pending.json")
            self.assertEqual(
                run_meta._sha256_file(archived_pending),
                old_pending_sha256,
            )
            self.assertTrue(os.path.isfile(auth_path))
            self.assertTrue(os.path.isfile(stop_path))

            def run_meta_git(args, **_kwargs):
                if "rev-list" in args:
                    return mock.Mock(stdout=(
                        current_commit + " " + prepared_commit + "\n"))
                self.assertEqual(args[3:5], ["diff", "--name-only"])
                return mock.Mock(stdout=(
                    "HP_V8/src/authorize_ledger_lock_recovery.py\n"
                    "HP_V8/src/run_meta.py\n"
                    "HP_V8/src/test_model_openai.py\n"
                ))

            with mock.patch.object(
                    run_meta.subprocess, "run",
                    side_effect=run_meta_git), mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=current_fingerprint):
                self.assertTrue(
                    run_meta
                    ._deepseek_inspector_reprepare_evidence_matches(
                        out_dir, updated_record
                    )
                )
                tampered_record = json.loads(json.dumps(updated_record))
                tampered_record[
                    "deepseek_transport_inspector_pending_reprepared_"
                    "from_authorization_id"
                ] = "forged"
                self.assertFalse(
                    run_meta
                    ._deepseek_inspector_reprepare_evidence_matches(
                        out_dir, tampered_record
                    )
                )
                stripped_record = json.loads(json.dumps(updated_record))
                for key in list(stripped_record):
                    if key.startswith(
                            "deepseek_transport_inspector_pending_"
                            "reprepared_"):
                        stripped_record.pop(key)
                self.assertFalse(
                    run_meta
                    ._deepseek_inspector_reprepare_evidence_matches(
                        out_dir, stripped_record
                    )
                )

    def test_deepseek_recovery_rebuilds_partial_archive_temp(self):
        with tempfile.TemporaryDirectory() as out_dir:
            source = os.path.join(out_dir, "source.json")
            destination = os.path.join(out_dir, "archive.json")
            with open(source, "wb") as handle:
                handle.write(b'{"authorization":"prior"}')
            short_temp = os.path.join(
                out_dir, ".recovery-copy.pending")
            with open(short_temp, "wb") as handle:
                handle.write(b'{"authorization":')
            expected = run_meta._sha256_file(source)
            ledger_recovery._ensure_bound_file_copy(
                source, destination, expected)
            self.assertEqual(
                run_meta._sha256_file(destination), expected)
            self.assertFalse(os.path.exists(short_temp))

    def test_deepseek_recovery_repairs_only_expected_partial_witness(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_log.jsonl")
            run_meta.append_jsonl_locked(path, {"event": "prior"})
            prefix = ledger_recovery._file_prefix_evidence(path)
            witness = {
                "event": (
                    "user_authorized_deepseek_transport_"
                    "inspector_followup"),
                "created_at": "2026-07-28T14:00:00+08:00",
                "campaign_recovery_authorization_id": "auth-a",
            }
            encoded = (
                json.dumps(
                    witness, ensure_ascii=False).encode("utf-8")
                + b"\n"
            )
            with open(path, "ab") as handle:
                handle.write(encoded[:len(encoded) // 2])
            ledger_recovery._append_recoverable_jsonl_tail(
                path, prefix, witness)
            self.assertEqual(
                ledger_recovery._read_jsonl(path),
                [{"event": "prior"}, witness],
            )

            with open(path, "r+b") as handle:
                handle.truncate(prefix["byte_count"])
                handle.seek(prefix["byte_count"])
                handle.write(b"conflict")
            with self.assertRaisesRegex(
                    RuntimeError, "JSONL tail conflicts"):
                ledger_recovery._append_recoverable_jsonl_tail(
                    path, prefix, witness)

    def test_retry_audit_allows_unbounded_free_503_attempts(self):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "stream_complete": False,
                "generation_delta_seen": False,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            }
            for index in range(1, 6)
        ]
        attempts.append({
            "attempt_index": 6,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
        })
        self.assertTrue(paired_dispatch._valid_deepseek_retry_evidence({
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_attempts": attempts,
            "http_attempts_used": 6,
            "retry_count": 5,
            "failed_attempt_count": 5,
            "max_retries": 3,
        }))

    def test_retry_audit_rejects_exhausted_non_503_budget(self):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "rate_limit",
                "http_status": 429,
                "stream_complete": False,
                "generation_delta_seen": False,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": index,
            }
            for index in range(1, 4)
        ]
        attempts.append({
            "attempt_index": 4,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
        })
        self.assertFalse(paired_dispatch._valid_deepseek_retry_evidence({
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_attempts": attempts,
            "http_attempts_used": 4,
            "retry_count": 3,
            "failed_attempt_count": 3,
            "max_retries": 3,
        }))

    def test_retry_audit_requires_explicit_current_stream_terminal_evidence(self):
        success = {
            "attempt_index": 2,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
        }
        row = {
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "stream_complete": False,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            }, success],
            "http_attempts_used": 2,
            "retry_count": 1,
            "failed_attempt_count": 1,
            "max_retries": 3,
        }
        self.assertFalse(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        row["transport_attempts"][0]["generation_delta_seen"] = False
        self.assertTrue(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        del success["terminal_sequence_valid"]
        self.assertFalse(
            paired_dispatch._valid_deepseek_retry_evidence(row))

    def test_retry_audit_keeps_frozen_legacy_nonstream_rows_readable(self):
        row = {
            "transport": paired_dispatch.DEEPSEEK_LEGACY_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_LEGACY_TRANSPORT_REVISION),
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            }, {
                "attempt_index": 2,
                "status": "success",
                "http_status": 200,
                "stream_complete": True,
            }],
            "http_attempts_used": 2,
            "retry_count": 1,
            "failed_attempt_count": 1,
            "max_retries": 3,
        }
        self.assertTrue(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        self.assertTrue(
            paired_dispatch._valid_deepseek_transport_sidecar(
                "unused", row))

    def test_previous_stream_revision_remains_readable(self):
        row = {
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch
                .DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION),
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "success",
                "http_status": 200,
                "stream_complete": True,
                "message_start_seen": True,
                "message_stop_seen": True,
                "final_usage_seen": True,
                "terminal_sequence_valid": True,
            }],
            "http_attempts_used": 1,
            "retry_count": 0,
            "failed_attempt_count": 0,
            "max_retries": 3,
        }
        self.assertTrue(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        with tempfile.TemporaryDirectory() as out_dir:
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"), {
                    "config": {
                        "model": paired_dispatch.DEEPSEEK_MODEL,
                        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                        "transport_revision": (
                            paired_dispatch
                            .DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION),
                    },
                })
            self.assertEqual(
                paired_dispatch._deepseek_campaign_runtime_identity(out_dir),
                (
                    paired_dispatch.DEEPSEEK_TRANSPORT,
                    paired_dispatch
                    .DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
                ),
            )

    def test_partial_stream_exhaustion_routes_to_deepseek_sample_isolation(self):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "incomplete_stream",
                "http_status": 503,
                "stream_complete": False,
                "generation_delta_seen": True,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": index,
            }
            for index in range(1, 4)
        ]
        api_row = {
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "classification": "provider/API failure",
            "error_type": "incomplete_stream",
            "http_status": 503,
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "provider_called": True,
            "stream_complete": False,
            "response_replayed": False,
            "count_as_method_failure": False,
            "transport_attempts": attempts,
            "http_attempts_used": len(attempts),
            "retry_count": len(attempts) - 1,
            "failed_attempt_count": len(attempts),
            "max_retries": 3,
        }
        self.assertTrue(
            paired_dispatch._deepseek_failed_retry_row(api_row))

        outcome = {
            "status": "infrastructure_incomplete",
            "worker_launch_id": "worker-a",
            "worker_pid": 123,
            "methods": ["hybridpatch", "fullrewrite"],
            "classification": "provider/API failure",
            "error_type": "incomplete_stream",
            "method_phase": None,
        }
        expected = {"request_id": "call-a"}
        with mock.patch.object(
                paired_dispatch, "read_campaign_stop_conditions",
                return_value=[]), mock.patch.object(
                    paired_dispatch, "_latest_sample_outcomes",
                    return_value={"sample": outcome}), mock.patch.object(
                        paired_dispatch, "_worker_lease_is_held",
                        return_value=False), mock.patch.object(
                            paired_dispatch,
                            "_deepseek_campaign_runtime_identity",
                            return_value=(
                                paired_dispatch.DEEPSEEK_TRANSPORT,
                                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION,
                            )), mock.patch.object(
                                paired_dispatch,
                                "_verified_deepseek_infrastructure_incomplete",
                                return_value=expected) as verify:
            actual = paired_dispatch._verified_infrastructure_incomplete(
                "unused", "sample", {
                    "worker_launch_id": "worker-a",
                    "worker_pid": 123,
                    "methods": ["hybridpatch", "fullrewrite"],
                    "method_phase": None,
                })
        self.assertEqual(actual, expected)
        verify.assert_called_once_with(
            "unused", "sample",
            {
                "worker_launch_id": "worker-a",
                "worker_pid": 123,
                "methods": ["hybridpatch", "fullrewrite"],
                "method_phase": None,
            },
            outcome,
        )

    def test_resume_accepts_only_hash_bound_disconnect_misclassification(self):
        sample = "sample"
        worker_id = "worker-a"
        worker_pid = 123
        api_row = {
            "sample": sample,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "classification": "runner_exception",
            "error_type": "transport_disconnect",
            "http_status": None,
            "provider_called": True,
            "stream_complete": False,
            "response_replayed": False,
            "count_as_method_failure": True,
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "fatal_error",
                "error_type": "transport_disconnect",
            }],
        }
        api_digest = run_meta._canonical_record_sha256(api_row)
        assignments = [{
            "sample": sample,
            "methods": ["hybridpatch", "fullrewrite"],
        }]
        metadata = [{
            "samples": [sample],
            "status": "failed",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
        }]
        dispatch_rows = [{
            "event": "worker_exit",
            "sample": sample,
            "worker_launch_id": worker_id,
            "pid": worker_pid,
            "returncode": 1,
            "disposition": "campaign_fatal",
        }]

        def read_jsonl(path):
            if os.path.basename(path) == "dispatch_log.jsonl":
                return dispatch_rows
            if os.path.basename(path) == "api_calls.jsonl":
                return [api_row]
            raise AssertionError(path)

        recovery = {
            "worker_launch_ids": frozenset({worker_id}),
            "dispatcher_parent_loss_workers": {},
            "api_incident_kinds": {
                api_digest: (
                    "deepseek_transport_disconnect_misclassification"),
            },
        }
        patches = (
            mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=recovery),
            mock.patch.object(
                paired_dispatch, "read_run_metadata_snapshot",
                return_value=metadata),
            mock.patch.object(
                paired_dispatch, "_read_jsonl", side_effect=read_jsonl),
            mock.patch.object(
                paired_dispatch, "_queued_pending_evidence",
                return_value=["run_metadata.jsonl"]),
            mock.patch.object(
                paired_dispatch, "_worker_lease_is_held",
                return_value=False),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            self.assertEqual(
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments),
                {sample},
            )

        forged_recovery = {
            **recovery,
            "api_incident_kinds": {
                "0" * 64: (
                    "deepseek_transport_disconnect_misclassification"),
            },
        }
        with mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=forged_recovery), mock.patch.object(
                    paired_dispatch, "read_run_metadata_snapshot",
                    return_value=metadata), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=read_jsonl), mock.patch.object(
                            paired_dispatch, "_queued_pending_evidence",
                            return_value=["run_metadata.jsonl"]), \
                mock.patch.object(
                    paired_dispatch, "_worker_lease_is_held",
                    return_value=False):
            with self.assertRaisesRegex(
                    RuntimeError, "cannot prove pending sample"):
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments)

    def test_resume_accepts_exact_authorized_failed_recovered_worker(self):
        sample = "sample"
        worker_id = "worker-a"
        worker_pid = 123
        invocation_id = "invocation-a"
        assignments = [{
            "sample": sample,
            "methods": ["hybridpatch", "fullrewrite"],
        }]
        metadata = [{
            "samples": [sample],
            "status": "failed",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": invocation_id,
        }]
        dispatch_rows = [{
            "event": "worker_exit",
            "sample": sample,
            "worker_launch_id": worker_id,
            "pid": worker_pid,
            "returncode": 1,
            "disposition": "campaign_fatal",
        }]
        recovered_worker = {
            "sample": sample,
            "status": "failed",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": invocation_id,
        }
        recovery = {
            "worker_launch_ids": frozenset({worker_id}),
            "deepseek_recovered_workers": {
                sample: recovered_worker,
            },
            "dispatcher_parent_loss_workers": {},
            "api_incident_kinds": {},
        }

        def read_jsonl(path):
            if os.path.basename(path) == "dispatch_log.jsonl":
                return dispatch_rows
            if os.path.basename(path) == "api_calls.jsonl":
                return []
            raise AssertionError(path)

        patches = (
            mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=recovery),
            mock.patch.object(
                paired_dispatch, "read_run_metadata_snapshot",
                return_value=metadata),
            mock.patch.object(
                paired_dispatch, "_read_jsonl", side_effect=read_jsonl),
            mock.patch.object(
                paired_dispatch, "_queued_pending_evidence",
                return_value=["run_metadata.jsonl"]),
            mock.patch.object(
                paired_dispatch, "_worker_lease_is_held",
                return_value=False),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            self.assertEqual(
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments),
                {sample},
            )

        forged = {
            **recovery,
            "deepseek_recovered_workers": {
                sample: {
                    **recovered_worker,
                    "invocation_id": "forged",
                },
            },
        }
        with mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=forged), mock.patch.object(
                    paired_dispatch, "read_run_metadata_snapshot",
                    return_value=metadata), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=read_jsonl), mock.patch.object(
                            paired_dispatch, "_queued_pending_evidence",
                            return_value=["run_metadata.jsonl"]), \
                mock.patch.object(
                    paired_dispatch, "_worker_lease_is_held",
                    return_value=False):
            with self.assertRaisesRegex(
                    RuntimeError, "recovered worker identity is invalid"):
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments)

    def test_deepseek_resume_consumes_recovered_before_pristine_audit(self):
        assignment = {
            "sample": "sample",
            "methods": ["hybridpatch", "fullrewrite"],
        }
        recovery = {
            "provider_access_retry_authorizations": {},
            "provider_access_resume_samples": frozenset(),
            "worker_launch_ids": frozenset({"worker-a"}),
        }
        with mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=recovery), mock.patch.object(
                    paired_dispatch, "_latest_sample_outcomes",
                    return_value={}), mock.patch.object(
                        paired_dispatch,
                        "_verified_deepseek_resume_missing_samples",
                        return_value={"sample"}), mock.patch.object(
                            paired_dispatch,
                            "_verify_queued_pending_samples") as pristine:
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    "unused",
                    [assignment],
                    resume=True,
                    target_round_trips=10,
                    allow_pristine_pending=True,
                    allow_deepseek_resume=True,
                )
            )
        self.assertEqual(selected, [assignment])
        self.assertEqual(authorizations, {})
        pristine.assert_not_called()

    def test_deepseek_recovered_worker_scope_is_exact_and_isolated(self):
        worker = {
            "sample": "sample",
            "status": "failed",
            "worker_launch_id": "worker-a",
            "worker_pid": 123,
            "invocation_id": "invocation-a",
        }
        self.assertTrue(
            run_meta._deepseek_recovered_worker_scope_is_valid(
                [worker], ["worker-a"], ["sample"], []))
        self.assertFalse(
            run_meta._deepseek_recovered_worker_scope_is_valid(
                [{**worker, "worker_pid": True}],
                ["worker-a"],
                ["sample"],
                [],
            ))
        self.assertFalse(
            run_meta._deepseek_recovered_worker_scope_is_valid(
                [
                    worker,
                    {
                        **worker,
                        "worker_launch_id": "worker-b",
                        "invocation_id": "invocation-b",
                    },
                ],
                ["worker-a", "worker-b"],
                ["sample", "sample-b"],
                [],
            ))

        authorization = {
            "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
            "incident_api_rows": [],
            "incident_attempt_rows": [],
            "incident_transport_sidecars": [],
            "recovered_worker_launch_ids": ["worker-a"],
            "deepseek_recovered_workers": [worker],
            "preauthorization_worker_launch_ids": [],
            "provider_access_retry_authorizations": [],
            "provider_access_resume_samples": [],
            "authorization_id": "authorization-a",
        }
        run_meta.campaign_recovery_incident_evidence.cache_clear()
        try:
            with mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=authorization):
                evidence = run_meta.campaign_recovery_incident_evidence(
                    "unused")
        finally:
            run_meta.campaign_recovery_incident_evidence.cache_clear()
        self.assertEqual(evidence["deepseek_recovered_workers"], {})

    def test_failed_retry_audit_validates_every_attempt_and_budget_step(self):
        attempts = [
            {
                "attempt_index": 1,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "stream_complete": False,
                "generation_delta_seen": False,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            },
            *[
                {
                    "attempt_index": index,
                    "status": "retryable_error",
                    "error_type": "incomplete_stream",
                    "http_status": 200,
                    "stream_complete": False,
                    "generation_delta_seen": True,
                    "retry_budget_consumed": True,
                    "retry_budget_attempt_index": index - 1,
                }
                for index in range(2, 5)
            ],
        ]
        row = {
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "classification": "provider/API failure",
            "error_type": "incomplete_stream",
            "http_status": 200,
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "provider_called": True,
            "stream_complete": False,
            "response_replayed": False,
            "count_as_method_failure": False,
            "transport_attempts": attempts,
            "http_attempts_used": 4,
            "retry_count": 3,
            "failed_attempt_count": 4,
            "max_retries": 3,
        }
        self.assertTrue(paired_dispatch._deepseek_failed_retry_row(row))

        malformed = copy.deepcopy(row)
        malformed["transport_attempts"][1][
            "retry_budget_attempt_index"] = 0
        self.assertFalse(
            paired_dispatch._deepseek_failed_retry_row(malformed))
        malformed = copy.deepcopy(row)
        malformed["transport_attempts"][2]["attempt_index"] = 99
        self.assertFalse(
            paired_dispatch._deepseek_failed_retry_row(malformed))

    def test_historical_infrastructure_outcomes_remain_auditable(self):
        def outcome(sample, request_id, worker_id):
            return {
                "sample": sample,
                "status": "infrastructure_incomplete",
                "worker_launch_id": worker_id,
                "worker_pid": 123,
                "methods": ["hybridpatch", "fullrewrite"],
                "classification": "provider/API failure",
                "method_phase": None,
                "request_id": request_id,
            }

        first = outcome("sample-a", "call-a1", "worker-a1")
        second = outcome("sample-b", "call-b1", "worker-b1")
        third = outcome("sample-b", "call-b2", "worker-b2")
        rows = [
            first,
            {
                "sample": "sample-a",
                "status": "finished",
            },
            second,
            third,
        ]
        config = {
            "samples": ["sample-a", "sample-b"],
            "method_set": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 10,
        }

        def verified(_out_dir, _sample, _item, row, **kwargs):
            self.assertFalse(kwargs["require_current_progress"])
            return {
                "request_id": row["request_id"],
                "worker_launch_id": row["worker_launch_id"],
                "worker_pid": row["worker_pid"],
            }

        def incident(_out_dir, row, _evidence, _api, _committed):
            return {row["request_id"]}

        with mock.patch.object(
                paired_dispatch, "_latest_sample_outcomes"), \
                mock.patch.object(
                    paired_dispatch, "read_sample_outcomes",
                    return_value=rows), \
                mock.patch.object(
                    paired_dispatch,
                    "_verified_deepseek_infrastructure_incomplete",
                    side_effect=verified) as verify, mock.patch.object(
                        paired_dispatch,
                        "_verified_deepseek_uncommitted_incident_request_ids",
                        side_effect=incident) as verify_incident:
            request_ids = (
                paired_dispatch
                ._verified_deepseek_infrastructure_request_ids(
                    "unused", config, api_snapshot=[],
                    committed_call_ids=set())
            )
        self.assertEqual(
            request_ids, {"call-a1", "call-b1", "call-b2"})
        self.assertEqual(verify.call_count, 3)
        self.assertEqual(verify_incident.call_count, 3)

        forged = copy.deepcopy(first)
        forged["classification"] = "runner_exception"
        with mock.patch.object(
                paired_dispatch, "_latest_sample_outcomes"), \
                mock.patch.object(
                    paired_dispatch, "read_sample_outcomes",
                    return_value=[forged]):
            with self.assertRaisesRegex(
                    RuntimeError, "invalid DeepSeek infrastructure outcome"):
                paired_dispatch._verified_deepseek_infrastructure_request_ids(
                    "unused", config)

    def test_formal_runner_requires_opencode_zen_and_high(self):
        with mock.patch.dict(
                os.environ,
                {
                    "OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1",
                    "AZURE_OPENAI_API_KEY": "",
                    "AZURE_OPENAI_ENDPOINT": "",
                },
                clear=False):
            experiment_runner._require_formal_opencode_transport(
                "deepseek-v4-flash", "high")
            experiment_runner._require_formal_opencode_transport(
                "t-deepseek-v4-flash", "high")
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport(
                    "deepseek-v4-flash", "medium")
        with mock.patch.dict(
                os.environ,
                {"OPENAI_BASE_URL": "https://api.deepseek.com"},
                clear=False):
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport(
                    "deepseek-v4-flash", "high")

    def test_multisample_deepseek_runner_requires_dispatch_manifest(self):
        with tempfile.TemporaryDirectory() as out_dir:
            experiment_runner._require_formal_dispatch_environment(
                out_dir, ["sample"], "deepseek-v4-flash")
            with self.assertRaisesRegex(
                    RuntimeError, "require.*paired dispatcher manifest"):
                experiment_runner._require_formal_dispatch_environment(
                    out_dir, ["sample-a", "sample-b"],
                    "t-deepseek-v4-flash")

    def test_capacity_manifest_is_one_key_fifteen_slot_rt2(self):
        args = argparse.Namespace(
            campaign_role="deepseek_capacity15",
            num_round_trips=2,
            seed=42,
            slots_per_key=paired_dispatch.DEEPSEEK_CAPACITY_SLOTS_PER_KEY,
            smoke_dir=None,
        )
        samples = list(paired_dispatch.DEEPSEEK_CAPACITY_SAMPLES)
        args.samples = samples
        paired_dispatch._validate_campaign_grid(args)
        assignments = paired_dispatch.build_key_assignments(
            samples, ["KEY_1"], 15,
            alternate_within_key=True, allow_queue=True)
        task_plans = {
            sample: {
                "path": f"{sample}.task_plan.json",
                "sha256": "a" * 64,
                "forward_state_sequence": ["state"],
            }
            for sample in samples
        }
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "unused", samples, assignments, task_plans, args)
        config = manifest["config"]
        self.assertEqual(config["model"], "deepseek-v4-flash")
        self.assertEqual(config["reasoning_effort"], "high")
        self.assertEqual(config["num_round_trips"], 2)
        self.assertEqual(config["slots_per_key"], 15)
        self.assertEqual(config["key_count"], 1)
        self.assertEqual(config["max_worker_count"], 15)
        self.assertEqual(config["queued_worker_count"], 0)
        self.assertEqual(
            config["dispatch_policy"], "per_key_work_conserving_v1")

    def test_full_manifest_is_three_key_work_conserving_queue(self):
        args = argparse.Namespace(
            campaign_role="deepseek_full234",
            num_round_trips=(
                paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
            seed=42,
            slots_per_key=paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY,
            smoke_dir=None,
            _full234_scope_record={
                "schema": "anchorpatch.full234_scope/1",
                "sample_count": 234,
                "sample_ids": [f"sample-{index:03d}" for index in range(234)],
                "sample_json_sha256": "c" * 64,
            },
        )
        samples = list(args._full234_scope_record["sample_ids"])
        args.samples = samples
        paired_dispatch._validate_campaign_grid(args)
        labels = ["KEY_1", "KEY_2", "KEY_3"]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 10,
            alternate_within_key=True, allow_queue=True)
        task_plans = {
            sample: {
                "path": f"{sample}.task_plan.json",
                "sha256": "a" * 64,
                "forward_state_sequence": ["state"],
            }
            for sample in samples
        }
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "unused", samples, assignments, task_plans, args)
        config = manifest["config"]
        self.assertEqual(
            config["num_round_trips"],
            paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS)
        self.assertEqual(config["key_count"], 3)
        self.assertEqual(config["slots_per_key"], 10)
        self.assertEqual(config["max_worker_count"], 30)
        self.assertEqual(config["queued_worker_count"], 204)
        self.assertEqual(
            config["dispatch_policy"], "per_key_work_conserving_v1")
        queues = manifest["assignment_queues"]
        self.assertEqual(len(queues), 3)
        self.assertEqual(
            sorted(queue["worker_count"] for queue in queues),
            [78, 78, 78])

        two_key_assignments = paired_dispatch.build_key_assignments(
            samples, labels[:2], 10,
            alternate_within_key=True, allow_queue=True)
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            with self.assertRaisesRegex(
                    RuntimeError, "requires exactly three keys"):
                paired_dispatch.build_manifest(
                    "unused", samples, two_key_assignments,
                    task_plans, args)

    def test_formal_deepseek_full234_rejects_rt2(self):
        samples = [f"sample-{index:03d}" for index in range(234)]
        args = argparse.Namespace(
            campaign_role="deepseek_full234",
            num_round_trips=2,
            seed=42,
            slots_per_key=paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY,
            smoke_dir=None,
            samples=samples,
            _full234_scope_record={
                "schema": "anchorpatch.full234_scope/1",
                "sample_count": 234,
                "sample_ids": samples,
                "sample_json_sha256": "c" * 64,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "at 10 RT"):
            paired_dispatch._validate_campaign_grid(args)

    def test_worker_launch_uses_per_key_openai_env_and_high(self):
        captured = {}

        class FakeProcess:
            pid = 12345

            @staticmethod
            def poll():
                return None

        def fake_popen(command, **kwargs):
            captured["command"] = list(command)
            captured["env"] = dict(kwargs["env"])
            return FakeProcess()

        args = argparse.Namespace(
            campaign_role="deepseek_capacity15",
            num_round_trips=2,
            seed=42,
            notes="unit",
            start_timeout=10,
        )
        sample = "earncall1"
        assignment = {
            "sample": sample,
            "key_label": "KEY_1",
            "methods": ["hybridpatch", "fullrewrite"],
            "console_log": "dispatch_logs/earncall1.log",
        }
        with tempfile.TemporaryDirectory() as out_dir:
            os.makedirs(os.path.join(out_dir, "dispatch_logs"))
            plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
            with open(plan_path, "w", encoding="utf-8") as handle:
                json.dump({"targets": []}, handle)
            task_plans = {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": "b" * 64,
                }
            }
            manifest = {"run_git_commit": "1" * 40}
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen",
                    side_effect=fake_popen), mock.patch.object(
                        paired_dispatch, "_authorize_workers"), mock.patch.object(
                            paired_dispatch, "_write_active_worker_set"), \
                    mock.patch.dict(
                        os.environ,
                        {
                            "OPENCODE_API_KEY": "wrong",
                            "MINIMAX_API_KEY": "wrong",
                            "MINIMAX_TRANSPORT": "wrong",
                            "AZURE_OPENAI_API_KEY": "wrong",
                            "AZURE_OPENAI_ENDPOINT": "https://wrong.invalid",
                        },
                        clear=False):
                running = {}
                paired_dispatch._launch_worker_batch(
                    args, out_dir, manifest, task_plans,
                    {"KEY_1": "secret-key"}, [assignment], {},
                    os.path.join(out_dir, "dispatch_log.jsonl"), running)
                running[sample]["log"].close()
        command = captured["command"]
        environment = captured["env"]
        self.assertIn("deepseek-v4-flash", command)
        self.assertIn("20000", command)
        self.assertEqual(
            command[command.index("--reasoning_effort") + 1], "high")
        self.assertEqual(environment["OPENAI_API_KEY"], "secret-key")
        self.assertEqual(
            environment["OPENAI_BASE_URL"],
            "https://opencode.ai/zen/go/v1")
        self.assertEqual(
            environment["ANCHORPATCH_DISPATCHER_PID"], str(os.getpid()))
        self.assertTrue(
            environment["ANCHORPATCH_DISPATCHER_INSTANCE_ID"].startswith(
                "dispatcher-"))
        self.assertNotIn("OPENCODE_API_KEY", environment)
        self.assertNotIn("MINIMAX_API_KEY", environment)
        self.assertNotIn("MINIMAX_TRANSPORT", environment)
        self.assertNotIn("AZURE_OPENAI_API_KEY", environment)
        self.assertNotIn("AZURE_OPENAI_ENDPOINT", environment)

    def test_dispatcher_parent_watchdog_requires_complete_identity(self):
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANCHORPATCH_DISPATCHER_PID", None)
            os.environ.pop("ANCHORPATCH_DISPATCHER_INSTANCE_ID", None)
            self.assertIsNone(
                experiment_runner._start_dispatcher_parent_watchdog(out_dir))

            os.environ["ANCHORPATCH_DISPATCHER_PID"] = str(os.getpid())
            with self.assertRaisesRegex(
                    RuntimeError, "watchdog identity is invalid"):
                experiment_runner._start_dispatcher_parent_watchdog(out_dir)

    def test_dispatcher_parent_loss_records_campaign_stop_latch(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a"},
                clear=False), mock.patch.object(
                    run_meta, "_campaign_metadata_lock",
                    side_effect=AssertionError(
                        "emergency writer must not take metadata lock")):
            experiment_runner._record_dispatcher_parent_loss(
                out_dir, 12345, "dispatcher-instance-a")
            records = run_meta.read_campaign_stop_conditions(out_dir)
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, "campaign_stop.json")))
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            self.assertEqual(
                [
                    name for name in os.listdir(emergency_dir)
                    if name.endswith(".json")
                ],
                [],
            )
        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["condition"], "dispatcher_process_lost")
        self.assertEqual(records[0]["dispatcher_pid"], 12345)
        self.assertEqual(
            records[0]["dispatcher_instance_id"],
            "dispatcher-instance-a")
        self.assertEqual(records[0]["worker_pid"], os.getpid())

    def test_dispatcher_parent_loss_recovery_closes_and_resumes_worker(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            worker = fixture["workers"][0]
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                result = ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                allowed = (
                    paired_dispatch
                    ._verified_deepseek_resume_missing_samples(
                        out_dir, [{
                            "sample": worker["sample"],
                            "methods": ["hybridpatch", "fullrewrite"],
                        }]))

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertEqual(result["resume_samples"], [worker["sample"]])
            self.assertEqual(
                authorization["recovery_kind"],
                run_meta.DISPATCHER_PROCESS_LOST_RECOVERY_KIND)
            self.assertEqual(
                authorization["dispatcher_parent_loss_workers"], [{
                    "sample": worker["sample"],
                    "worker_launch_id": worker["worker_launch_id"],
                    "worker_pid": worker["worker_pid"],
                    "invocation_id": worker["invocation_id"],
                    "status": "interrupted_by_dispatcher",
                }])
            self.assertEqual(allowed, {worker["sample"]})
            self.assertEqual(run_meta.read_campaign_stop_conditions(out_dir), [])
            active = ledger_recovery._read_json(os.path.join(
                out_dir, "active_worker_set.json"))
            self.assertEqual(active["workers"], {})
            metadata = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(
                [row["status"] for row in metadata],
                ["interrupted_by_dispatcher"])
            events = [
                row["event"] for row in ledger_recovery._read_jsonl(
                    os.path.join(out_dir, "dispatch_log.jsonl"))
            ]
            self.assertEqual(events.count("worker_exit"), 1)
            self.assertEqual(events.count("stale_worker_reconciled"), 1)
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))

    def test_dispatcher_parent_loss_recovery_covers_startup_windows(self):
        stages = ["preauthorization", "registered_prelaunch"]
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, stages)
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                result = ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                allowed = (
                    paired_dispatch
                    ._verified_deepseek_resume_missing_samples(
                        out_dir, [{
                            "sample": item["sample"],
                            "methods": ["hybridpatch", "fullrewrite"],
                        } for item in fixture["workers"]]))

            expected_samples = {
                item["sample"] for item in fixture["workers"]
            }
            self.assertEqual(result["reconciled_workers"], 2)
            self.assertEqual(set(result["resume_samples"]), expected_samples)
            self.assertEqual(allowed, expected_samples)
            workers = {
                item["sample"]: item
                for item in authorization[
                    "dispatcher_parent_loss_workers"]
            }
            self.assertEqual(
                workers["parent-loss-0"]["status"], "preauthorization")
            self.assertEqual(
                workers["parent-loss-1"]["status"],
                "registered_prelaunch")
            self.assertEqual(
                authorization["preauthorization_worker_launch_ids"],
                ["worker-parent-loss-0"])
            self.assertEqual(
                authorization[
                    "dispatcher_parent_loss_registered_prelaunch_worker_launch_ids"],
                ["worker-parent-loss-1"])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "run_metadata.jsonl")))
            events = ledger_recovery._read_jsonl(os.path.join(
                out_dir, "dispatch_log.jsonl"))
            self.assertEqual(
                [
                    row["worker_launch_id"] for row in events
                    if row.get("event") == "worker_exit"
                ],
                ["worker-parent-loss-0"])

    def test_stopless_registered_prelaunch_has_recovery_entry(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            worker = fixture["workers"][0]
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                result = ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                allowed = (
                    paired_dispatch
                    ._verified_deepseek_resume_missing_samples(
                        out_dir, [{
                            "sample": worker["sample"],
                            "methods": ["hybridpatch", "fullrewrite"],
                        }]))

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertEqual(result["resume_samples"], [worker["sample"]])
            self.assertEqual(allowed, {worker["sample"]})
            self.assertEqual(
                authorization["dispatcher_parent_loss_workers"], [{
                    "sample": worker["sample"],
                    "worker_launch_id": worker["worker_launch_id"],
                    "worker_pid": None,
                    "invocation_id": None,
                    "status": "registered_prelaunch",
                }])
            archived_stop = ledger_recovery._read_json(os.path.join(
                out_dir,
                authorization["archived_stop_path"],
            ))
            self.assertIs(archived_stop["registered_prelaunch_only"], True)
            self.assertEqual(
                archived_stop["registered_worker_launch_ids"],
                [worker["worker_launch_id"]],
            )
            self.assertIsNone(archived_stop["worker_launch_id"])
            self.assertIsNone(archived_stop["worker_pid"])
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_stopless_parent_loss_rejects_launched_worker(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["preauthorization"], record_stop=False)
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint):
                with self.assertRaisesRegex(
                        RuntimeError,
                        "limited to workers registered before process launch"):
                    ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)

            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))

    def test_synthetic_prelaunch_stop_absorbs_late_watchdog_record(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            worker = fixture["workers"][0]
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            synthetic = (
                ledger_recovery
                ._record_stopless_registered_prelaunch_parent_loss(
                    out_dir, fixture["manifest"], stop_path))
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": worker[
                            "worker_launch_id"],
                    },
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=os.getpid() + 1000):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=fixture["dispatcher_pid"],
                    dispatcher_instance_id=fixture[
                        "dispatcher_instance_id"],
                )

            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir),
                [synthetic],
            )
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            self.assertFalse(
                os.path.isdir(emergency_dir)
                and any(
                    name.endswith(".json")
                    for name in os.listdir(emergency_dir)
                )
            )

    def test_recovered_active_set_blocks_late_watchdog_relatched_stop(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            worker = fixture["workers"][0]
            paired_dispatch._write_active_worker_set(
                out_dir, fixture["manifest"], [])
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": worker[
                            "worker_launch_id"],
                    },
                    clear=False):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=fixture["dispatcher_pid"],
                    dispatcher_instance_id=fixture[
                        "dispatcher_instance_id"],
                )

            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_parent_loss_recovery_serializes_late_watchdog_publication(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            worker = fixture["workers"][0]
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            ledger_recovery._record_stopless_registered_prelaunch_parent_loss(
                out_dir, fixture["manifest"], stop_path)
            publication_entered = threading.Event()
            allow_publication = threading.Event()
            original_link = os.link
            watchdog_errors = []
            recovery_results = []
            recovery_errors = []

            def blocked_link(source, destination):
                publication_entered.set()
                if not allow_publication.wait(timeout=5):
                    raise RuntimeError("test publication barrier timed out")
                return original_link(source, destination)

            def publish_watchdog():
                try:
                    run_meta.record_emergency_campaign_stop_condition(
                        out_dir,
                        "dispatcher_process_lost",
                        dispatcher_pid=fixture["dispatcher_pid"],
                        dispatcher_instance_id=fixture[
                            "dispatcher_instance_id"],
                    )
                except Exception as exc:
                    watchdog_errors.append(exc)

            def recover_parent():
                try:
                    recovery_results.append(ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True))
                except Exception as exc:
                    recovery_errors.append(exc)

            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": worker[
                            "worker_launch_id"],
                    },
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=os.getpid() + 1000), mock.patch.object(
                            run_meta.os, "link",
                            side_effect=blocked_link), mock.patch.object(
                                ledger_recovery, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                watchdog = threading.Thread(
                    target=publish_watchdog, daemon=True)
                watchdog.start()
                self.assertTrue(publication_entered.wait(timeout=5))
                recovery = threading.Thread(
                    target=recover_parent, daemon=True)
                recovery.start()
                recovery.join(timeout=0.1)
                self.assertTrue(recovery.is_alive())
                allow_publication.set()
                watchdog.join(timeout=5)
                recovery.join(timeout=5)

            self.assertFalse(watchdog.is_alive())
            self.assertFalse(recovery.is_alive())
            self.assertEqual(watchdog_errors, [])
            self.assertEqual(recovery_errors, [])
            self.assertEqual(len(recovery_results), 1)
            self.assertEqual(
                recovery_results[0]["resume_samples"], [worker["sample"]])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_parent_loss_stop_recovers_unrecorded_worker_launch(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running", "registered_prelaunch"])
            unrecorded = fixture["workers"][1]
            unrecorded_pid = os.getpid() + 1000
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": unrecorded[
                            "worker_launch_id"],
                    },
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=unrecorded_pid):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=fixture["dispatcher_pid"],
                    dispatcher_instance_id=fixture[
                        "dispatcher_instance_id"],
                )
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))

            workers = {
                item["worker_launch_id"]: item
                for item in authorization[
                    "dispatcher_parent_loss_workers"]
            }
            self.assertEqual(
                workers[unrecorded["worker_launch_id"]]["status"],
                "preauthorization")
            self.assertEqual(
                workers[unrecorded["worker_launch_id"]]["worker_pid"],
                unrecorded_pid)
            launches = [
                row for row in ledger_recovery._read_jsonl(os.path.join(
                    out_dir, "dispatch_log.jsonl"))
                if row.get("event") == "launch"
                and row.get("worker_launch_id")
                == unrecorded["worker_launch_id"]
            ]
            self.assertEqual(len(launches), 1)
            self.assertIs(launches[0]["launch_observed"], False)
            self.assertTrue(launches[0]["reconciled_after_parent_loss"])
            self.assertEqual(
                len(authorization["archived_emergency_stop_records"]), 1)

    def test_dispatcher_parent_loss_recovery_retries_transaction_once(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            real_archive = ledger_recovery._archive_campaign_stop_cohort
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                with mock.patch.object(
                        ledger_recovery,
                        "_archive_campaign_stop_cohort",
                        side_effect=RuntimeError(
                            "injected archive interruption")):
                    with self.assertRaisesRegex(
                            RuntimeError, "injected archive interruption"):
                        ledger_recovery.authorize(
                            out_dir, dispatcher_process_lost=True)
                pending_path = os.path.join(
                    out_dir,
                    ledger_recovery
                    ._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
                )
                self.assertTrue(os.path.isfile(pending_path))
                self.assertTrue(os.path.isfile(os.path.join(
                    out_dir,
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
                )))
                self.assertTrue(
                    run_meta.read_campaign_stop_conditions(out_dir))
                with mock.patch.object(
                        ledger_recovery,
                        "_archive_campaign_stop_cohort",
                        wraps=real_archive):
                    result = ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertFalse(os.path.exists(pending_path))
            self.assertEqual(run_meta.read_campaign_stop_conditions(out_dir), [])
            events = ledger_recovery._read_jsonl(os.path.join(
                out_dir, "dispatch_log.jsonl"))
            self.assertEqual(sum(
                row.get("event") == "worker_exit" for row in events), 1)
            self.assertEqual(sum(
                row.get("event") == "stale_worker_reconciled"
                for row in events), 1)
            self.assertEqual(sum(
                row.get("event")
                == "user_authorized_dispatcher_parent_loss_recovery"
                for row in events), 1)

    def test_parent_loss_pending_after_archive_blocks_resume(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            real_read = (
                ledger_recovery.read_campaign_recovery_authorization)
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                with mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        side_effect=RuntimeError(
                            "injected post-archive interruption")):
                    with self.assertRaisesRegex(
                            RuntimeError, "post-archive interruption"):
                        ledger_recovery.authorize(
                            out_dir, dispatcher_process_lost=True)
                self.assertEqual(
                    run_meta.read_campaign_stop_conditions(out_dir), [])
                with self.assertRaisesRegex(
                        RuntimeError, "transaction is pending"):
                    run_meta.read_campaign_recovery_authorization(out_dir)
                with self.assertRaisesRegex(
                        RuntimeError, "transaction is pending"):
                    paired_dispatch._launch_under_lease(
                        mock.Mock(), out_dir)
                with mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        wraps=real_read):
                    result = ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))

    def test_parent_loss_pending_after_event_unlinks_idempotently(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            real_unlink = ledger_recovery._unlink_with_sharing_retry
            pending_name = (
                run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME)

            def interrupt_pending_unlink(path):
                if os.path.basename(path) == pending_name:
                    raise RuntimeError("injected pending unlink interruption")
                return real_unlink(path)

            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                with mock.patch.object(
                        ledger_recovery, "_unlink_with_sharing_retry",
                        side_effect=interrupt_pending_unlink):
                    with self.assertRaisesRegex(
                            RuntimeError, "pending unlink interruption"):
                        ledger_recovery.authorize(
                            out_dir, dispatcher_process_lost=True)
                with self.assertRaisesRegex(
                        RuntimeError, "transaction is pending"):
                    run_meta.read_campaign_recovery_authorization(out_dir)
                result = ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)

            self.assertEqual(result["reconciled_workers"], 1)
            events = ledger_recovery._read_jsonl(os.path.join(
                out_dir, "dispatch_log.jsonl"))
            self.assertEqual(sum(
                row.get("event")
                == "user_authorized_dispatcher_parent_loss_recovery"
                for row in events), 1)

    def test_emergency_only_archive_retry_rebuilds_canonical_stop(self):
        stop = {
            "schema": run_meta.STOP_CONDITION_SCHEMA,
            "created_at": "2026-07-28T00:00:00+08:00",
            "condition": "dispatcher_process_lost",
            "worker_launch_id": "worker-a",
            "worker_pid": 123,
            "dispatcher_pid": 456,
            "dispatcher_instance_id": "dispatcher-instance-a",
        }
        with tempfile.TemporaryDirectory() as out_dir:
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            history_dir = os.path.join(
                out_dir, "recovery_history", "authorization-a")
            os.makedirs(emergency_dir)
            os.makedirs(history_dir)
            run_meta.write_json_atomic(
                os.path.join(emergency_dir, "worker-a.json"), stop)
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            with mock.patch.object(
                    ledger_recovery, "_copy_file_durable",
                    side_effect=RuntimeError(
                        "injected canonical reconstruction interruption")):
                with self.assertRaisesRegex(
                        RuntimeError, "canonical reconstruction"):
                    ledger_recovery._archive_campaign_stop_cohort(
                        out_dir, stop_path, history_dir)
            archived_stop, archived_emergency = (
                ledger_recovery._archive_campaign_stop_cohort(
                    out_dir, stop_path, history_dir))

            self.assertTrue(os.path.isfile(archived_stop))
            self.assertEqual(len(archived_emergency), 1)
            self.assertEqual(
                ledger_recovery._read_json(archived_stop), stop)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_repeated_dispatcher_parent_loss_keeps_prior_pending_worker(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["preauthorization", "registered_prelaunch"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            manifest = fixture["manifest"]
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)

                sample = fixture["workers"][0]["sample"]
                worker_id = "worker-parent-loss-repeat"
                worker_pid = os.getpid()
                invocation_id = "invocation-parent-loss-repeat"
                dispatcher_pid = 54321
                dispatcher_instance_id = "dispatcher-instance-b"
                paired_dispatch._write_active_worker_set(
                    out_dir, manifest, [{
                        "sample": sample,
                        "worker_launch_id": worker_id,
                        "dispatcher_pid": dispatcher_pid,
                        "dispatcher_instance_id": dispatcher_instance_id,
                    }])
                dispatch_path = os.path.join(
                    out_dir, "dispatch_log.jsonl")
                intent = {
                    "event": "launch_intent",
                    "sample": sample,
                    "key_label": "KEY_1",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "worker_launch_id": worker_id,
                    "console_log": (
                        f"dispatch_logs/{sample}.repeat.console.log"),
                    "method_phase": None,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                }
                run_meta.append_jsonl_locked(dispatch_path, intent)
                run_meta.append_jsonl_locked(dispatch_path, {
                    **intent,
                    "event": "launch",
                    "pid": worker_pid,
                })
                run_meta.append_jsonl_locked(dispatch_path, {
                    "event": "worker_authorized",
                    "worker_launch_id": worker_id,
                    "sample": sample,
                    "worker_pid": worker_pid,
                    "invocation_id": invocation_id,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                })
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), {
                        "schema": run_meta.METADATA_SCHEMA,
                        "invocation_id": invocation_id,
                        "worker_launch_id": worker_id,
                        "worker_pid": worker_pid,
                        "dispatcher_pid": dispatcher_pid,
                        "dispatcher_instance_id": dispatcher_instance_id,
                        "samples": [sample],
                        "methods": ["hybridpatch", "fullrewrite"],
                        "method_phase": None,
                        "model": paired_dispatch.DEEPSEEK_MODEL,
                        "provider": "opencode_zen",
                        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                        "transport_revision": (
                            paired_dispatch
                            .DEEPSEEK_TRANSPORT_REVISION),
                        "transport_resume_policy": None,
                        "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                        "reasoning_effort": (
                            paired_dispatch
                            .DEEPSEEK_REASONING_EFFORT),
                        "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                        "campaign_config": {
                            "reasoning_effort": (
                                paired_dispatch
                                .DEEPSEEK_REASONING_EFFORT),
                        },
                        "run_git_commit": commit,
                        "git_tree_state": "clean",
                        "code_fingerprint": fingerprint,
                        "status": "running",
                        "finished_at": None,
                    })
                with mock.patch.dict(
                        os.environ,
                        {"ANCHORPATCH_WORKER_LAUNCH_ID": worker_id},
                        clear=False):
                    run_meta.record_campaign_stop_condition(
                        out_dir,
                        "dispatcher_process_lost",
                        dispatcher_pid=dispatcher_pid,
                        dispatcher_instance_id=dispatcher_instance_id,
                    )
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                incidents = (
                    run_meta.campaign_recovery_incident_evidence(out_dir))
                prior_pending_sample = fixture["workers"][1]["sample"]
                allowed = (
                    paired_dispatch
                    ._verified_deepseek_resume_missing_samples(
                        out_dir, [{
                            "sample": prior_pending_sample,
                            "methods": ["hybridpatch", "fullrewrite"],
                        }]))

            resume_workers = {
                item["sample"]: item
                for item in authorization[
                    "dispatcher_parent_loss_resume_workers"]
            }
            self.assertEqual(
                resume_workers[sample]["worker_launch_id"], worker_id)
            self.assertEqual(
                resume_workers[sample]["status"],
                "interrupted_by_dispatcher")
            self.assertEqual(
                resume_workers[prior_pending_sample]["status"],
                "registered_prelaunch")
            self.assertEqual(
                incidents["dispatcher_parent_loss_workers"][
                    prior_pending_sample]["status"],
                "registered_prelaunch")
            self.assertEqual(allowed, {prior_pending_sample})

    def test_repeated_parent_loss_removes_now_finished_resume_sample(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["preauthorization", "registered_prelaunch"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            manifest = fixture["manifest"]
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)

                sample = fixture["workers"][0]["sample"]
                prior_pending_sample = fixture["workers"][1]["sample"]
                worker_id = "worker-parent-loss-finished"
                worker_pid = os.getpid() + 1000
                invocation_id = "invocation-parent-loss-finished"
                dispatcher_pid = 54321
                dispatcher_instance_id = "dispatcher-instance-finished"
                paired_dispatch._write_active_worker_set(
                    out_dir, manifest, [{
                        "sample": sample,
                        "worker_launch_id": worker_id,
                        "dispatcher_pid": dispatcher_pid,
                        "dispatcher_instance_id": dispatcher_instance_id,
                    }])
                dispatch_path = os.path.join(
                    out_dir, "dispatch_log.jsonl")
                intent = {
                    "event": "launch_intent",
                    "sample": sample,
                    "key_label": "KEY_1",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "worker_launch_id": worker_id,
                    "console_log": (
                        f"dispatch_logs/{sample}.finished.console.log"),
                    "method_phase": None,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                }
                run_meta.append_jsonl_locked(dispatch_path, intent)
                run_meta.append_jsonl_locked(dispatch_path, {
                    **intent,
                    "event": "launch",
                    "pid": worker_pid,
                })
                run_meta.append_jsonl_locked(dispatch_path, {
                    "event": "worker_authorized",
                    "worker_launch_id": worker_id,
                    "sample": sample,
                    "worker_pid": worker_pid,
                    "invocation_id": invocation_id,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                })
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), {
                        "schema": run_meta.METADATA_SCHEMA,
                        "invocation_id": invocation_id,
                        "worker_launch_id": worker_id,
                        "worker_pid": worker_pid,
                        "dispatcher_pid": dispatcher_pid,
                        "dispatcher_instance_id": dispatcher_instance_id,
                        "samples": [sample],
                        "methods": ["hybridpatch", "fullrewrite"],
                        "method_phase": None,
                        "model": paired_dispatch.DEEPSEEK_MODEL,
                        "provider": "opencode_zen",
                        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                        "transport_revision": (
                            paired_dispatch
                            .DEEPSEEK_TRANSPORT_REVISION),
                        "transport_resume_policy": None,
                        "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                        "reasoning_effort": (
                            paired_dispatch
                            .DEEPSEEK_REASONING_EFFORT),
                        "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                        "campaign_config": {
                            "reasoning_effort": (
                                paired_dispatch
                                .DEEPSEEK_REASONING_EFFORT),
                        },
                        "run_git_commit": commit,
                        "git_tree_state": "clean",
                        "code_fingerprint": fingerprint,
                        "status": "finished",
                        "finished_at": "2026-07-28T00:00:00+08:00",
                    })
                expected_progress = {}
                for method in ("hybridpatch", "fullrewrite"):
                    method_dir = os.path.join(out_dir, method)
                    os.makedirs(method_dir, exist_ok=True)
                    rows = [
                        {
                            "sample_id": sample,
                            "method": method,
                            "round_trip_num": rt_index,
                            "round_trip_direction": direction,
                        }
                        for rt_index in range(1, 11)
                        for direction in ("forward", "backward")
                    ]
                    with open(
                            os.path.join(
                                method_dir, f"{sample}.jsonl"),
                            "w", encoding="utf-8") as handle:
                        for row in rows:
                            handle.write(json.dumps(row) + "\n")
                    run_meta.write_json_atomic(
                        os.path.join(
                            method_dir, f"{sample}.ckpt.json"),
                        {"completed_round_trips": 10},
                    )
                    expected_progress[method] = {
                        "completed_round_trips": 10,
                        "committed_rows": 20,
                    }
                with mock.patch.dict(
                        os.environ,
                        {"ANCHORPATCH_WORKER_LAUNCH_ID": worker_id},
                        clear=False), mock.patch.object(
                            run_meta.os, "getpid",
                            return_value=worker_pid):
                    run_meta.record_sample_outcome(
                        out_dir,
                        sample,
                        "finished",
                        invocation_id=invocation_id,
                        methods=["hybridpatch", "fullrewrite"],
                        checkpoint_progress=expected_progress,
                    )
                    run_meta.record_campaign_stop_condition(
                        out_dir,
                        "dispatcher_process_lost",
                        dispatcher_pid=dispatcher_pid,
                        dispatcher_instance_id=dispatcher_instance_id,
                    )
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                selected, _authorizations = (
                    paired_dispatch._select_invocation_assignments(
                        out_dir,
                        [
                            {
                                "sample": sample,
                                "methods": [
                                    "hybridpatch", "fullrewrite"],
                            },
                            {
                                "sample": prior_pending_sample,
                                "methods": [
                                    "hybridpatch", "fullrewrite"],
                            },
                        ],
                        resume=True,
                        target_round_trips=10,
                        allow_deepseek_resume=True,
                    ))

            self.assertNotIn(
                sample,
                authorization["provider_access_resume_samples"],
            )
            self.assertNotIn(
                sample,
                {
                    item["sample"] for item in authorization[
                        "dispatcher_parent_loss_resume_workers"]
                },
            )
            self.assertEqual(
                [item["sample"] for item in selected],
                [prior_pending_sample],
            )

    def test_emergency_stop_retries_transient_secondary_name_cleanup(self):
        real_unlink = os.unlink
        transient_failures = []

        def flaky_unlink(path):
            if (path.endswith(".json")
                    and run_meta.EMERGENCY_STOP_DIRECTORY in path
                    and not transient_failures):
                transient_failures.append(path)
                exc = OSError("simulated Windows sharing violation")
                exc.winerror = 32
                raise exc
            return real_unlink(path)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta.os, "unlink", side_effect=flaky_unlink):
            run_meta.record_emergency_campaign_stop_condition(
                out_dir, "dispatcher_process_lost",
                dispatcher_pid=12345,
                dispatcher_instance_id="dispatcher-instance-a",
            )
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, "campaign_stop.json")))
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            self.assertEqual(
                [
                    name for name in os.listdir(emergency_dir)
                    if name.endswith(".json")
                ],
                [],
            )
        self.assertEqual(len(transient_failures), 1)

    def test_emergency_stop_keeps_each_distinct_worker_record(self):
        real_pid = os.getpid()
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a"},
                    clear=False):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=12345,
                    dispatcher_instance_id="dispatcher-instance-a",
                )
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-b"},
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=real_pid + 1000):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=12345,
                    dispatcher_instance_id="dispatcher-instance-a",
                )
            records = run_meta.read_campaign_stop_conditions(out_dir)
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            emergency_names = [
                name for name in os.listdir(emergency_dir)
                if name.endswith(".json")
            ]

        self.assertEqual(len(records), 2)
        self.assertEqual(
            {row["worker_launch_id"] for row in records},
            {"worker-a", "worker-b"})
        self.assertEqual(
            {row["worker_pid"] for row in records},
            {real_pid, real_pid + 1000})
        self.assertEqual(len(emergency_names), 1)


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
