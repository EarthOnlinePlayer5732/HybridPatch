"""Mixin implementation for test_model_openai.OpenCodeZenDeepSeekTests."""

from .support import *


class OpenCodeZenDeepSeekTestsMixin:
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
            result["transport_revision"], "opencode_openai_compatible/6")
        self.assertEqual(
            result["request_url"],
            "https://opencode.ai/zen/go/v1/chat/completions")
        self.assertEqual(result["reasoning_effort"], "high")
        self.assertIs(result["stream_complete"], True)
        self.assertNotIn("_raw_stream_events", result)
        self.assertEqual(result["http_attempts_used"], 1)
        self.assertEqual(result["retry_count"], 0)
        self.assertEqual(result["cost_currency"], "USD")
        self.assertAlmostEqual(result["total_usd"], 0.00000252)
        self.assertEqual(result["total_cny"], 0.0)
        self.assertEqual(events[0]["record_type"], "attempt_start")
        self.assertEqual(events[-1]["record_type"], "attempt_end")
        self.assertEqual(
            [event["record_type"] for event in events],
            [
                "attempt_start", "stream_checkpoint", "stream_checkpoint",
                "stream_checkpoint", "stream_checkpoint", "stream_summary",
                "attempt_end",
            ],
        )
        self.assertEqual(
            result["transport_attempts"][0]["stream_event_count"], 4)

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

    def test_fifty_thousand_chunks_emit_bounded_compact_evidence(self):
        captures = []
        payload = _official_payload(content="x", finish_reason="stop")
        template = _stream_chunks_from_payload(payload)
        chunks = [template[0], *([template[1]] * 50_000), *template[2:]]

        result, events = self._generate([chunks], captures)

        self.assertEqual(result["message"], "x" * 50_000)
        self.assertNotIn("_raw_stream_events", result)
        self.assertLessEqual(len(events), 8)
        self.assertNotIn(
            "sdk_stream_event",
            [event["record_type"] for event in events],
        )
        attempt = result["transport_attempts"][0]
        self.assertEqual(attempt["stream_event_count"], 50_003)
        self.assertEqual(attempt["text_delta_count"], 50_000)
        self.assertRegex(attempt["stream_event_sha256"], r"^[0-9a-f]{64}$")

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
        event_types = [event["record_type"] for event in events]
        self.assertEqual(event_types.count("attempt_start"), 2)
        self.assertEqual(event_types.count("stream_summary"), 2)
        self.assertEqual(event_types.count("attempt_end"), 2)
        self.assertNotIn("sdk_stream_event", event_types)

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
            config["transport_revision"], "opencode_openai_compatible/6")
        self.assertEqual(config["transport"], "openai_sdk_stream")
        self.assertEqual(config["reasoning_effort"], "high")
        with self.assertRaises(ValueError):
            model_openai._effective_reasoning_effort("ultra")
        captures = []
        fake_cls = _official_client_factory(
            [_official_payload(content="unused")], captures)
        with mock.patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "unit-test-key",
                    "OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1",
                },
                clear=False), mock.patch.object(
                    model_openai, "OpenAI", fake_cls):
            with self.assertRaisesRegex(ValueError, "requires reasoning_effort"):
                model_openai.OpenAI_Model().generate(
                    [{"role": "user", "content": "Hello"}],
                    model="deepseek-v4-flash",
                    max_tokens=20000,
                    return_metadata=True,
                )
        self.assertFalse(any("model" in item for item in captures))

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
