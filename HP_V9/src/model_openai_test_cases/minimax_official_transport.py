"""Mixin implementation for test_model_openai.MinimaxOfficialTransportTests."""

from .support import *


class MinimaxOfficialTransportTestsMixin:
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
