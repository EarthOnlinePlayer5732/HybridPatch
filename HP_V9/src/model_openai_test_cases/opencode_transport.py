"""Mixin implementation for test_model_openai.OpenCodeTransportTests."""

from .support import *


class OpenCodeTransportTestsMixin:
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
