"""
Standalone OpenAI / Azure OpenAI wrapper providing generate() and generate_json().

Set OPENAI_API_KEY (or AZURE_OPENAI_API_KEY + AZURE_OPENAI_ENDPOINT) in your
environment before running.
"""

from openai import OpenAI, AzureOpenAI
import os, time, json, re
import concurrent.futures
import urllib.error
import urllib.request

try:
    import anthropic
    from anthropic import Anthropic
except ImportError:  # Keep the OpenAI-compatible path importable with a clear error.
    anthropic = None
    Anthropic = None

# MiniMax M3 is routed through OpenCode Go's Anthropic-compatible /messages
# endpoint. Guard MiniMax calls with a hard WALL-CLOCK watchdog: a stalled
# attempt is abandoned and retried on a fresh request. This only bounds stalls —
# it never changes the behavior of a successful call (so DeepSeek
# reproducibility is untouched).
# Wall-clock watchdog bound (seconds). Extended-thinking calls generate far more
# output (observed 20k-47k completion tokens) and legitimately run for many
# minutes, so the default is raised and made env-configurable via
# MINIMAX_HARD_TIMEOUT. This must exceed the provider/gateway timeout, otherwise
# the watchdog abandons a call before a real 524 can surface and be retried.
_MINIMAX_HARD_TIMEOUT = int(os.environ.get("MINIMAX_HARD_TIMEOUT", "1800"))
# Research-policy ceiling for MiniMax-M3. None/0 maps to this value; callers may
# request a smaller positive value, but no environment variable can raise it.
_MINIMAX_MAX_TOKENS = 131_072
_MINIMAX_MAX_RESPONSE_SLOTS = 2
_MINIMAX_MAX_TRANSIENT_FAILURES = 3
_OPENCODE_TRANSPORT_SDK = "anthropic_sdk_v2"
_OPENCODE_TRANSPORT_LEGACY = "urllib_v1"
_OPENCODE_TRANSPORT_REVISION = "opencode_anthropic_sdk/4"
_OPENCODE_TRANSPORT_RESUME_POLICY = "exact_payload_new_semantic_call/1"
_OPENCODE_ZEN_BASE_URL = "https://opencode.ai/zen/go/v1"
_OPENCODE_ZEN_CHAT_COMPLETIONS_URL = (
    _OPENCODE_ZEN_BASE_URL + "/chat/completions"
)
_OPENCODE_OPENAI_COMPATIBLE_REVISION = "opencode_openai_compatible/5"
_OPENCODE_OPENAI_COMPATIBLE_TRANSPORT = "openai_sdk_stream"
_OPENCODE_MAX_RETRY_AFTER_SECONDS = 300
_REASONING_EFFORTS = {"low", "medium", "high"}
# MiniMax official OpenAI-compatible endpoint (docs/Minimax_OPENAI.md), selected
# ONLY via MINIMAX_TRANSPORT=official_nonstream. Own transport revision with
# baseline-aligned semantics: blocking non-streaming create(), blanket-exception
# retry, complete HTTP-200 responses (empty/truncated included) accepted as-is.
# Frozen transport-v3 (OpenCode) experiments are unaffected; run_meta's revision
# gate refuses to mix the two in one out_dir.
_MINIMAX_TRANSPORT_OPENCODE = "opencode"
_MINIMAX_TRANSPORT_OFFICIAL = "official_nonstream"
_MINIMAX_OFFICIAL_BASE_URL = "https://api.minimaxi.com/v1"
_MINIMAX_OFFICIAL_MODEL = "MiniMax-M3"
_MINIMAX_OFFICIAL_REVISION = "minimax_official_nonstream/1"
# Official-key quota windows (5h / weekly). Hitting 429 pauses and resumes —
# scheduling only, so baseline retry semantics are untouched: quota waits never
# consume blanket-retry attempts. 36 x 600s = 6h covers a full 5h window.
_MINIMAX_OFFICIAL_QUOTA_WAIT_SECONDS = int(
    os.environ.get("MINIMAX_QUOTA_WAIT_SECONDS", "600"))
_MINIMAX_OFFICIAL_MAX_QUOTA_WAITS = int(
    os.environ.get("MINIMAX_QUOTA_MAX_WAITS", "36"))


def _is_official_rate_limit(exc):
    if type(exc).__name__ == "RateLimitError":
        return True
    return getattr(exc, "status_code", None) == 429
_WATCHDOG_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=8,
                                                       thread_name_prefix="llm-watchdog")

# Load project-local .env so OPENAI_API_KEY / OPENAI_BASE_URL can live in the
# project dir instead of system-wide env. Checked repo-root-first so the same
# file works in both layouts: module at repo root (old workspace) and module
# under src/ (hybridpatch_clean, whose README puts .env at the repo root).
# Real environment variables take precedence over .env (override=False default).
try:
    from dotenv import load_dotenv
    _MOD_DIR = os.path.dirname(os.path.abspath(__file__))
    for _envp in (os.path.join(os.path.dirname(_MOD_DIR), ".env"),
                  os.path.join(_MOD_DIR, ".env")):
        load_dotenv(_envp)
except ImportError:
    pass

# ── Prompt variable substitution ─────────────────────────────────────────

def _format_messages(messages, variables={}):
    """Replace [[KEY]] placeholders in the last user message."""
    if not variables:
        return messages
    last_user_msg = [msg for msg in messages if msg["role"] == "user"][-1]
    for k, v in variables.items():
        key_string = f"[[{k}]]"
        assert isinstance(v, str), f"Variable {k} is not a string"
        last_user_msg["content"] = last_user_msg["content"].replace(key_string, v)
    return messages


# ── Pricing ──────────────────────────────────────────────────────────────

# Per-1M-token USD costs: (input, output)
_PRICING_USD = {
    "gpt-4o-mini":      (0.15,  0.60),
    "gpt-4o":           (2.50,  10.00),
    "gpt-4.1":          (2.00,  8.00),
    "gpt-4.1-mini":     (0.40,  1.60),
    "gpt-4.1-nano":     (0.10,  0.40),
    "gpt-4.5-preview":  (75.00, 150.00),
    "minimax-m3":       (0.30,  1.20),
    "o1-mini":          (3.00,  12.00),
    "o1":               (15.00, 60.00),
    "o3":               (10.00, 40.00),
    "o3-mini":          (1.10,  4.40),
    "o4-mini":          (1.10,  4.40),
}

_OPENCODE_PRICING_USD = {
    "deepseek-v4-flash": (0.14, 0.28),
    "deepseek-v4-pro": (1.74, 3.48),
}

# Per-1M-token CNY costs: (input cache hit, input cache miss, output).
# Source: DeepSeek official Chinese pricing page, checked 2026-06-06.
_DEEPSEEK_PRICING_CNY = {
    "deepseek-v4-flash": (0.02, 1.0, 2.0),
    "deepseek-v4-pro":   (0.025, 3.0, 6.0),
}

_OPENCODE_GO_BASE_URL = "https://opencode.ai/zen/go"
_OPENCODE_GO_MESSAGES_URL = _OPENCODE_GO_BASE_URL + "/v1/messages"


class _HTTPStatusError(RuntimeError):
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code}: {body[:1000]}")


class _IncompleteStreamError(RuntimeError):
    """A HTTP-success stream that ended without its required terminal evidence."""

    def __init__(self, message, *, status_code=None):
        self.status_code = status_code
        super().__init__(message)


class OpenCodeTransportError(RuntimeError):
    """Terminal OpenCode failure with sanitized per-attempt audit metadata."""

    def __init__(self, message, *, attempts, last_error):
        super().__init__(message)
        self.transport_attempts = list(attempts)
        self.last_error = _sanitized_transport_cause(last_error)
        self.status_code = getattr(self.last_error, "status_code", None)


class OpenAICompatibleTransportError(RuntimeError):
    """Terminal OpenAI-compatible failure with per-attempt audit metadata."""

    def __init__(self, message, *, attempts, last_error):
        super().__init__(message)
        self.transport_attempts = list(attempts)
        self.last_error = _sanitized_transport_cause(last_error)
        self.status_code = getattr(self.last_error, "status_code", None)


def _match_pricing(model, pricing):
    model_l = model.lower()
    matched = None
    for prefix, costs in pricing.items():
        if model_l.startswith(prefix.lower()):
            if matched is None or len(prefix) > len(matched[0]):
                matched = (prefix, costs)
    return matched[1] if matched else None


def _is_minimax_model(model):
    return model.lower().startswith("minimax-m3")


def _normalized_base_url(value):
    return str(value or "").strip().rstrip("/")


def _is_opencode_zen_runtime(model, base_url=None):
    return (
        str(model or "").lower().startswith("deepseek-v4-")
        and _normalized_base_url(
            base_url if base_url is not None else os.environ.get(
                "OPENAI_BASE_URL")
        ) == _OPENCODE_ZEN_BASE_URL
    )


def _effective_reasoning_effort(value):
    if value is None:
        return None
    effort = str(value).strip().lower()
    if effort not in _REASONING_EFFORTS:
        raise ValueError(
            "reasoning_effort must be one of low, medium, high, or omitted"
        )
    return effort


def _effective_minimax_max_tokens(max_tokens):
    if max_tokens is None or max_tokens == 0:
        return _MINIMAX_MAX_TOKENS
    if isinstance(max_tokens, bool) or not isinstance(max_tokens, int):
        raise ValueError("MiniMax-M3 max_tokens must be an integer")
    if max_tokens < 1:
        raise ValueError("MiniMax-M3 max_tokens must be >= 1")
    if max_tokens > _MINIMAX_MAX_TOKENS:
        raise ValueError(
            f"MiniMax-M3 max_tokens={max_tokens} exceeds the research-policy "
            f"ceiling {_MINIMAX_MAX_TOKENS}"
        )
    return max_tokens


def _opencode_transport():
    transport = (os.environ.get("OPENCODE_TRANSPORT") or _OPENCODE_TRANSPORT_SDK).strip()
    if transport not in (_OPENCODE_TRANSPORT_SDK, _OPENCODE_TRANSPORT_LEGACY):
        raise ValueError(
            "OPENCODE_TRANSPORT must be anthropic_sdk_v2 or urllib_v1, "
            f"got {transport!r}"
        )
    return transport


def _minimax_transport():
    value = (os.environ.get("MINIMAX_TRANSPORT") or _MINIMAX_TRANSPORT_OPENCODE).strip()
    if value not in (_MINIMAX_TRANSPORT_OPENCODE, _MINIMAX_TRANSPORT_OFFICIAL):
        raise ValueError(
            "MINIMAX_TRANSPORT must be opencode or official_nonstream, "
            f"got {value!r}"
        )
    return value


def _opencode_base_url():
    # Deliberately fixed to OpenCode Go. The MiniMax official endpoint is
    # reachable ONLY through the explicit MINIMAX_TRANSPORT=official_nonstream
    # route (its own transport revision); it must never become a silent
    # fallback here.
    return _OPENCODE_GO_BASE_URL


def _opencode_messages_url():
    return _OPENCODE_GO_MESSAGES_URL


def _minimax_thinking_config(thinking_mode="adaptive"):
    mode = (thinking_mode or "adaptive").strip().lower()
    if mode not in ("adaptive", "disabled"):
        raise ValueError("thinking_mode must be 'adaptive' or 'disabled'")
    return {"type": mode}


def minimax_runtime_config(max_tokens=None, thinking_mode="adaptive"):
    """Public, side-effect-free runtime description used by run metadata."""
    if _minimax_transport() == _MINIMAX_TRANSPORT_OFFICIAL:
        return {
            "provider": "minimax_official",
            "transport": "openai_sdk_nonstream",
            "transport_revision": _MINIMAX_OFFICIAL_REVISION,
            "base_url": _MINIMAX_OFFICIAL_BASE_URL,
            "request_url": _MINIMAX_OFFICIAL_BASE_URL + "/chat/completions",
            "anthropic_sdk_version": None,
            "effective_max_tokens": _effective_minimax_max_tokens(max_tokens),
            "thinking_mode": (thinking_mode or "adaptive").strip().lower(),
            "reasoning_split": True,
            "max_response_slots": None,
            "max_response_retries": None,
            "max_transient_failures": None,
            "transport_resume_policy": None,
        }
    sdk_version = getattr(anthropic, "__version__", None) if anthropic else None
    transport = _opencode_transport()
    return {
        "provider": "opencode_go",
        "transport": transport,
        "transport_revision": (
            _OPENCODE_TRANSPORT_REVISION
            if transport == _OPENCODE_TRANSPORT_SDK else "opencode_urllib/1"
        ),
        "base_url": _opencode_base_url(),
        "request_url": _opencode_messages_url(),
        "anthropic_sdk_version": sdk_version,
        "effective_max_tokens": _effective_minimax_max_tokens(max_tokens),
        "thinking_mode": (thinking_mode or "adaptive").strip().lower(),
        "max_response_slots": _MINIMAX_MAX_RESPONSE_SLOTS,
        "max_response_retries": _MINIMAX_MAX_RESPONSE_SLOTS - 1,
        "max_transient_failures": _MINIMAX_MAX_TRANSIENT_FAILURES,
        "transport_resume_policy": _OPENCODE_TRANSPORT_RESUME_POLICY,
    }


def openai_compatible_runtime_config(
        model, max_tokens=None, reasoning_effort=None):
    """Side-effect-free runtime identity for an OpenAI-compatible model."""
    resolved = resolve_model_name(model)
    base_url = _normalized_base_url(os.environ.get("OPENAI_BASE_URL")) or None
    opencode_zen = _is_opencode_zen_runtime(resolved, base_url)
    _reject_azure_opencode_zen_conflict(base_url)
    return {
        "provider": (
            "opencode_zen" if opencode_zen else "openai_chat_completions"
        ),
        "transport": (
            _OPENCODE_OPENAI_COMPATIBLE_TRANSPORT
            if opencode_zen else "openai_sdk_nonstream"
        ),
        "transport_revision": (
            _OPENCODE_OPENAI_COMPATIBLE_REVISION if opencode_zen else None
        ),
        "base_url": base_url,
        "request_url": (
            _OPENCODE_ZEN_CHAT_COMPLETIONS_URL if opencode_zen else None
        ),
        "anthropic_sdk_version": None,
        "effective_max_tokens": max_tokens,
        "thinking_mode": None,
        "reasoning_effort": _effective_reasoning_effort(reasoning_effort),
        "max_response_slots": None,
        "max_response_retries": None,
        "max_transient_failures": None,
        "transport_resume_policy": None,
    }


def model_runtime_config(
        model, max_tokens=None, thinking_mode="adaptive",
        reasoning_effort=None):
    if _is_minimax_model(resolve_model_name(model)):
        return minimax_runtime_config(
            max_tokens=max_tokens, thinking_mode=thinking_mode
        )
    return openai_compatible_runtime_config(
        model, max_tokens=max_tokens, reasoning_effort=reasoning_effort
    )


def _reject_azure_opencode_zen_conflict(base_url):
    """Do not let Azure credentials silently override the audited Zen route."""
    if (
            _normalized_base_url(base_url) == _OPENCODE_ZEN_BASE_URL
            and (
                os.environ.get("AZURE_OPENAI_API_KEY")
                or os.environ.get("AZURE_OPENAI_ENDPOINT")
            )):
        raise RuntimeError(
            "OpenCode Zen runtime forbids inherited AZURE_OPENAI_API_KEY/"
            "AZURE_OPENAI_ENDPOINT"
        )


def _as_plain_dict(value):
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json", warnings=False)
        except TypeError:
            return value.model_dump()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"expected a mapping-like SDK object, got {type(value).__name__}")


def _safe_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _content_to_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("content") or ""))
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p)
    return str(content)


def _messages_to_anthropic(messages):
    system_parts = []
    out = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content", "")
        if role == "system":
            system_parts.append(_content_to_text(content))
        elif role in ("user", "assistant"):
            out.append({"role": role, "content": content})
        else:
            out.append({"role": "user", "content": f"{role}: {_content_to_text(content)}"})
    if not out:
        out = [{"role": "user", "content": ""}]
    system = "\n\n".join(p for p in system_parts if p) or None
    return system, out


def _anthropic_text(resp):
    content = resp.get("content") or []
    if isinstance(content, str):
        return content
    parts = []
    for block in content:
        if isinstance(block, dict):
            if block.get("type") == "text" or "text" in block:
                parts.append(str(block.get("text") or ""))
        else:
            parts.append(str(block))
    return "\n".join(p for p in parts if p)


def _normalize_anthropic_response(resp):
    resp = _as_plain_dict(resp)
    usage0 = resp.get("usage") or {}
    cache_read = _safe_int(usage0.get("cache_read_input_tokens"))
    cache_create = _safe_int(usage0.get("cache_creation_input_tokens"))
    raw_input_tokens = _safe_int(usage0.get("input_tokens"))
    input_tokens = raw_input_tokens + cache_read + cache_create
    output_tokens = _safe_int(usage0.get("output_tokens"))
    usage = {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "prompt_cache_hit_tokens": cache_read,
        "prompt_cache_miss_tokens": max(input_tokens - cache_read, 0),
    }
    content = resp.get("content") or []
    block_counts = {}
    for block in content if isinstance(content, list) else []:
        plain = block if isinstance(block, dict) else _as_plain_dict(block)
        block_type = str(plain.get("type") or "unknown")
        block_counts[block_type] = block_counts.get(block_type, 0) + 1
    text = _anthropic_text(resp)
    stop_reason = resp.get("stop_reason")
    truncated_reasons = {"max_tokens", "model_context_window_exceeded"}
    if stop_reason == "refusal":
        response_classification = "model_refusal"
    elif text:
        response_classification = (
            "text_truncated" if stop_reason in truncated_reasons else "normal"
        )
    elif stop_reason in truncated_reasons:
        response_classification = "thinking_budget_exhausted"
    else:
        response_classification = "model_empty"
    return {
        "id": resp.get("id"),
        "model": resp.get("model"),
        "choices": [{"message": {"content": text}}],
        "usage": usage,
        "raw_usage": {
            "input_tokens": raw_input_tokens,
            "output_tokens": output_tokens,
            "cache_read_input_tokens": cache_read,
            "cache_creation_input_tokens": cache_create,
        },
        "finish_reason": stop_reason,
        "stop_reason": stop_reason,
        "stop_sequence": resp.get("stop_sequence"),
        "http_status": 200,
        "stream_complete": True,
        "response_classification": response_classification,
        "content_block_counts": block_counts,
        "content_block_count": sum(block_counts.values()),
        "provider_response": resp,
    }

def _emit_transport_event(sink, payload):
    if sink is None:
        return
    try:
        sink(payload)
    except Exception as exc:
        if getattr(sink, "_anchorpatch_critical", False):
            try:
                exc._anchorpatch_transport_observability_failure = True
            except Exception:
                pass
            raise
        # Observability must not turn a valid model call into a method failure.
        pass


def _read_sse_stream(resp, raw_events=None, raw_event_sink=None, attempt_index=1):
    """Reassemble an Anthropic-style SSE stream into the full message dict.

    Streaming keeps bytes flowing through Cloudflare's 120s Proxy Read Timeout,
    which a non-streaming extended-thinking call structurally cannot survive.

    If a `raw_events` list is passed, every parsed SSE event is appended to it
    verbatim (thinking deltas, signatures, error events and all) so the raw API
    log can preserve the complete stream even though the returned message keeps
    only text blocks. The reassembly here is unchanged; capture is a side channel.
    """
    message = {}
    blocks = []
    state = {
        "message_delta_seen": False,
        "message_stop_seen": False,
        "final_usage_seen": False,
    }
    for raw_line in resp:
        line = raw_line.decode("utf-8", errors="replace").strip()
        if not line.startswith("data:"):
            continue
        try:
            ev = json.loads(line[5:].strip())
        except Exception:
            continue
        if raw_events is not None:
            raw_events.append(ev)
        _emit_transport_event(raw_event_sink, {
            "record_type": "sdk_stream_event",
            "attempt_index": attempt_index,
            "event": ev,
        })
        t = ev.get("type")
        if t == "message_start":
            message = dict(ev.get("message") or {})
            blocks = list(message.get("content") or [])
        elif t == "content_block_start":
            idx = ev.get("index", len(blocks))
            while len(blocks) <= idx:
                blocks.append({})
            blocks[idx] = dict(ev.get("content_block") or {})
        elif t == "content_block_delta":
            idx = ev.get("index", 0)
            while len(blocks) <= idx:
                blocks.append({})
            d = ev.get("delta") or {}
            blk = blocks[idx]
            if d.get("type") == "text_delta":
                blk["text"] = (blk.get("text") or "") + (d.get("text") or "")
            elif d.get("type") == "thinking_delta":
                blk["thinking"] = (blk.get("thinking") or "") + (d.get("thinking") or "")
            elif d.get("type") == "signature_delta":
                blk["signature"] = (blk.get("signature") or "") + (d.get("signature") or "")
        elif t == "message_delta":
            state["message_delta_seen"] = True
            for k, v in (ev.get("delta") or {}).items():
                message[k] = v
            message.setdefault("usage", {}).update(ev.get("usage") or {})
        elif t == "error":
            # surface stream-level errors as a transient-classifiable HTTP error
            raise _HTTPStatusError(503, json.dumps(ev.get("error") or {}, ensure_ascii=False))
        elif t == "message_stop":
            state["message_stop_seen"] = True
            break
    message["content"] = blocks
    usage = message.get("usage") or {}
    state["final_usage_seen"] = (
        state["message_delta_seen"]
        and usage.get("input_tokens") is not None
        and usage.get("output_tokens") is not None
    )
    return message, state


def _transport_status_code(exc):
    code = getattr(exc, "status_code", None)
    if code is not None:
        try:
            return int(code)
        except (TypeError, ValueError):
            return None
    match = re.search(r"\bHTTP\s+(\d{3})\b", str(exc))
    return int(match.group(1)) if match else None


def _attempt_http_status(exc, prior=None):
    """Keep the HTTP status that opened a stream when its EOF has no status."""
    code = _transport_status_code(exc)
    if code is None and isinstance(prior, dict):
        code = prior.get("response_started_http_status")
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


def _is_generation_delta_type(delta_type):
    """Whether an Anthropic content delta proves generation has started."""
    value = str(delta_type or "")
    if value in {"thinking_delta", "text_delta", "input_json_delta"}:
        return True
    # Future content-producing Anthropic deltas consume a response slot too.
    # A signature authenticates thinking already emitted; it is not generation.
    return value.endswith("_delta") and value != "signature_delta"


def _stream_event_seen(attempt):
    attempt = attempt or {}
    return bool(
        attempt.get("message_start_seen")
        or attempt.get("message_delta_seen")
        or attempt.get("message_stop_seen")
        or attempt.get("generation_delta_seen")
        or int(attempt.get("content_blocks_started") or 0)
        or int(attempt.get("content_blocks_stopped") or 0)
    )


def _is_incomplete_stream_exception(exc, attempt=None):
    """Classify every HTTP-200/observed-stream terminal-chain failure first."""
    if getattr(exc, "_anchorpatch_transport_observability_failure", False):
        return False
    if isinstance(exc, _IncompleteStreamError):
        return True

    prior = getattr(exc, "_opencode_attempt", None)
    state = attempt if attempt is not None else prior
    code = _transport_status_code(exc)
    message = str(exc).lower()
    is_sdk_status_error = (
        type(exc).__name__ == "APIStatusError"
        or (
            anthropic is not None
            and getattr(anthropic, "APIStatusError", None) is not None
            and isinstance(exc, anthropic.APIStatusError)
        )
    )
    if (is_sdk_status_error and code == 200
            and "streaming response failed" in message):
        return True

    if _stream_event_seen(state):
        state = state or {}
        if not state.get("message_stop_seen"):
            return True
        if not state.get("final_usage_seen"):
            return True
        if state.get("terminal_sequence_valid") is False:
            return True
        started = int(state.get("content_blocks_started") or 0)
        stopped = int(state.get("content_blocks_stopped") or 0)
        if started != stopped or state.get("content_blocks_balanced") is False:
            return True
    return False


def _transport_error_type(exc, attempt=None):
    if _is_incomplete_stream_exception(exc, attempt):
        return "incomplete_stream"
    code = _transport_status_code(exc)
    if code == 429:
        return "rate_limit"
    if code in (401, 403):
        return "provider_access_denied"
    if code == 402:
        return "balance_or_payment_required"
    if code and code >= 500:
        return "server_error"
    name = type(exc).__name__
    msg = str(exc).lower()
    if "timeout" in name.lower() or "timeout" in msg:
        return "timeout"
    if any(term in msg for term in ("connection", "disconnect", "remote end closed", "reset")):
        return "transport_disconnect"
    return name


def _is_retryable_opencode_error(exc, attempt=None):
    if _is_incomplete_stream_exception(exc, attempt):
        return True
    code = _transport_status_code(exc)
    if code is not None:
        return code in (408, 409, 429) or code >= 500
    if anthropic is not None and isinstance(
        exc, (anthropic.APIConnectionError, anthropic.APITimeoutError)
    ):
        return True
    msg = str(exc).lower()
    return any(term in msg for term in (
        "timeout", "connection reset", "remote end closed", "disconnect",
        "peer closed connection", "incomplete chunked read", "cloudflare", "tunnel",
    ))


def _retry_after_seconds(
        exc, attempt_index, *, exponential_503=False):
    if exponential_503 and _transport_status_code(exc) == 503:
        exponent = min(max(int(attempt_index) - 1, 0), 6)
        base = min(5 * (2 ** exponent), _OPENCODE_MAX_RETRY_AFTER_SECONDS)
        # Spread worker retries without touching the experiment PRNG.
        jitter = (
            ((os.getpid() * 1103515245 + int(attempt_index) * 12345) & 0xFF)
            / 1280.0
        )
        backoff = min(
            base * (1.0 + jitter),
            _OPENCODE_MAX_RETRY_AFTER_SECONDS,
        )
    else:
        backoff = min(5 + 2 * attempt_index, 30)
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if headers:
        value = headers.get("retry-after") or headers.get("retry_after")
        try:
            backoff = max(
                backoff, min(float(value), _OPENCODE_MAX_RETRY_AFTER_SECONDS)
            )
        except (TypeError, ValueError):
            pass
    body = _transport_error_body(exc)
    body_text = json.dumps(body, ensure_ascii=False, default=str)
    match = re.search(
        r"retry[_-]after['\"]?\s*[:=]\s*['\"]?(\d+)",
        f"{str(exc)}\n{body_text}",
        flags=re.IGNORECASE,
    )
    if match:
        backoff = max(
            backoff,
            min(int(match.group(1)), _OPENCODE_MAX_RETRY_AFTER_SECONDS),
        )
    return backoff


def _redact_transport_value(value):
    try:
        serialized = json.dumps(
            value, ensure_ascii=False, default=str, separators=(",", ":")
        )
    except Exception:
        serialized = str(value)
    for secret in (
            os.environ.get("OPENAI_API_KEY"),
            os.environ.get("OPENCODE_API_KEY"),
            os.environ.get("OPENCODE_GO_API_KEY")):
        if secret:
            serialized = serialized.replace(secret, "<redacted-key>")
    try:
        return json.loads(serialized)
    except (TypeError, ValueError):
        return serialized


def _transport_error_body(exc):
    body = getattr(exc, "body", None)
    response = getattr(exc, "response", None)
    if body is None and response is not None:
        try:
            body = response.json()
        except Exception:
            body = getattr(response, "text", None)
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except (TypeError, ValueError):
            pass
    return _redact_transport_value(body) if body is not None else None


def _transport_error_message(exc):
    return _redact_transport_value(str(exc))


def _sanitized_transport_cause(exc):
    """Keep classification fields without retaining a secret-bearing cause."""
    sanitized = RuntimeError(str(_transport_error_message(exc)))
    sanitized.status_code = _transport_status_code(exc)
    if getattr(
            exc, "_anchorpatch_transport_observability_failure", False):
        sanitized._anchorpatch_transport_observability_failure = True
    return sanitized


def _attempt_from_exception(exc, attempt_index, elapsed_ms):
    prior = getattr(exc, "_opencode_attempt", None)
    record = {
        "attempt_index": attempt_index,
        "status": "retryable_error" if _is_retryable_opencode_error(exc) else "fatal_error",
        "http_status": _attempt_http_status(exc, prior),
        "error_type": _transport_error_type(exc),
        "error_message": _transport_error_message(exc),
        "error_body": _transport_error_body(exc),
        "elapsed_ms": elapsed_ms,
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
    }
    if prior:
        record.update(dict(prior))
        record["status"] = (
            "retryable_error"
            if _is_retryable_opencode_error(exc, record)
            else "fatal_error"
        )
        record["http_status"] = _attempt_http_status(exc, prior)
        record["error_type"] = _transport_error_type(exc, record)
        record["error_message"] = _transport_error_message(exc)
        record["error_body"] = _transport_error_body(exc)
        record["elapsed_ms"] = elapsed_ms
    return record


def _openai_stream_delta_seen(delta, key):
    value = delta.get(key)
    if isinstance(value, str):
        return bool(value)
    if isinstance(value, list):
        return bool(value)
    return value is not None


def _valid_openai_final_usage(value):
    """Require the complete non-negative Chat Completions token accounting."""
    usage = value if isinstance(value, dict) else _as_plain_dict(value)
    if not isinstance(usage, dict):
        return False
    counts = [
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
        usage.get("total_tokens"),
    ]
    if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0
            for count in counts):
        return False
    return counts[2] == counts[0] + counts[1]


def _call_openai_compatible_stream(
        client, request_kwargs, *, raw_event_sink=None, attempt_index=1):
    """Collect a complete OpenAI ChatCompletion stream with terminal usage."""
    raw_chunks = []
    content_parts = []
    response_id = None
    response_model = None
    finish_reason = None
    usage = None
    state = {
        "stream_complete": False,
        "message_start_seen": False,
        "message_delta_seen": False,
        "message_stop_seen": False,
        "final_usage_seen": False,
        "response_started_http_status": None,
        "generation_delta_seen": False,
        "thinking_delta_seen": False,
        "text_delta_seen": False,
        "tool_delta_seen": False,
        "content_blocks_started": 0,
        "content_blocks_stopped": 0,
        "content_blocks_balanced": True,
        "terminal_sequence_valid": False,
    }
    terminal_sequence_valid = True

    try:
        stream = client.chat.completions.create(
            **request_kwargs,
            stream=True,
            stream_options={"include_usage": True},
        )
        # The SDK returns the stream iterator only after the HTTP response has
        # opened successfully. Preserve that 200 if a later EOF/connection
        # exception carries no status of its own.
        state["response_started_http_status"] = 200
        for chunk in stream:
            plain = _as_plain_dict(chunk)
            raw_chunks.append(plain)
            state["message_start_seen"] = True
            _emit_transport_event(raw_event_sink, {
                "record_type": "sdk_stream_event",
                "attempt_index": attempt_index,
                "event": plain,
            })
            response_id = plain.get("id") or response_id
            response_model = plain.get("model") or response_model
            choices = plain.get("choices") or []
            current_finish = None
            if choices:
                if len(choices) != 1 or state["message_stop_seen"]:
                    terminal_sequence_valid = False
                choice0 = choices[0] if isinstance(choices[0], dict) else {}
                delta = choice0.get("delta") or {}
                if isinstance(delta, dict):
                    content = delta.get("content")
                    if content is not None:
                        text = _content_to_text(content)
                        if text:
                            content_parts.append(text)
                            state["generation_delta_seen"] = True
                            state["text_delta_seen"] = True
                    if any(_openai_stream_delta_seen(delta, key) for key in (
                            "reasoning", "reasoning_content",
                            "reasoning_details")):
                        state["generation_delta_seen"] = True
                        state["thinking_delta_seen"] = True
                    if _openai_stream_delta_seen(delta, "tool_calls"):
                        state["generation_delta_seen"] = True
                        state["tool_delta_seen"] = True
                current_finish = choice0.get("finish_reason")
                if current_finish is not None and (
                        not isinstance(current_finish, str)
                        or not current_finish.strip()):
                    terminal_sequence_valid = False
                elif current_finish:
                    if state["message_stop_seen"]:
                        terminal_sequence_valid = False
                    finish_reason = current_finish
                    state["message_delta_seen"] = True
                    state["message_stop_seen"] = True
            usage_value = plain.get("usage")
            if usage_value is not None:
                if (
                        state["message_stop_seen"]
                        and (not choices or bool(current_finish))
                        and not state["final_usage_seen"]
                        and _valid_openai_final_usage(usage_value)):
                    usage = usage_value
                    state["final_usage_seen"] = True
                else:
                    terminal_sequence_valid = False
    except Exception as exc:
        state["terminal_sequence_valid"] = False
        exc._opencode_attempt = dict(state)
        raise

    state["terminal_sequence_valid"] = bool(
        terminal_sequence_valid
        and state["message_stop_seen"]
        and state["final_usage_seen"]
    )
    if not state["terminal_sequence_valid"]:
        missing = []
        if not state["message_stop_seen"]:
            missing.append("finish_reason")
        if not state["final_usage_seen"]:
            missing.append("final_usage")
        if not terminal_sequence_valid:
            missing.append("valid_terminal_sequence")
        exc = _IncompleteStreamError(
            "OpenAI-compatible stream ended without " + " and ".join(missing),
            status_code=state["response_started_http_status"],
        )
        exc._opencode_attempt = dict(state)
        raise exc

    state["stream_complete"] = True
    return {
        "id": response_id,
        "model": response_model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "".join(content_parts),
            },
            "finish_reason": finish_reason,
        }],
        "usage": usage or {},
        "http_status": 200,
        "stream_complete": True,
        "_raw_stream_events": raw_chunks,
        "_stream_attempt_state": state,
    }


def _call_opencode_messages_urllib_v1(
        messages, model, max_tokens, temperature, timeout, is_json,
        thinking_mode="adaptive", call_kind="primary", raw_event_sink=None,
        attempt_index=1):
    key = os.environ.get("OPENCODE_API_KEY") or os.environ.get("OPENCODE_GO_API_KEY")
    assert key, "Set OPENCODE_API_KEY for minimax-m3 via OpenCode Go"
    system, anthropic_messages = _messages_to_anthropic(messages)
    effective_max_tokens = _effective_minimax_max_tokens(max_tokens)
    thinking = _minimax_thinking_config(thinking_mode)
    body = {
        "model": model,
        "messages": anthropic_messages,
        "max_tokens": effective_max_tokens,
        "temperature": 1.0 if thinking["type"] == "adaptive" else temperature,
        "stream": True,
        "thinking": thinking,
    }
    if system:
        body["system"] = system
    if is_json:
        body["system"] = (body.get("system", "") + "\n\nOutput valid JSON only.").strip()

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "User-Agent": os.environ.get(
            "OPENCODE_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AnchorPatch/1.0",
        ),
        "x-api-key": key,
        "Authorization": f"Bearer {key}",
        "anthropic-version": "2023-06-01",
    }
    req = urllib.request.Request(_opencode_messages_url(), data=data,
                                 headers=headers, method="POST")
    raw_events = []
    started = time.time()
    _emit_transport_event(raw_event_sink, {
        "record_type": "attempt_start", "attempt_index": attempt_index,
        "transport": _OPENCODE_TRANSPORT_LEGACY, "call_kind": call_kind,
        "request_url": _opencode_messages_url(), "request_body": body,
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            msg, stream_state = _read_sse_stream(
                r, raw_events=raw_events, raw_event_sink=raw_event_sink,
                attempt_index=attempt_index)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise _HTTPStatusError(e.code, body_text) from e
    if not all(stream_state.values()):
        exc = _IncompleteStreamError(
            "OpenCode urllib_v1 stream ended before message_delta/message_stop/final usage"
        )
        exc._opencode_attempt = {
            "attempt_index": attempt_index, "status": "retryable_error",
            "http_status": 200, "error_type": "incomplete_stream",
            "elapsed_ms": int((time.time() - started) * 1000),
            "stream_complete": False, **stream_state,
        }
        _emit_transport_event(raw_event_sink, {
            "record_type": "attempt_end", "attempt": exc._opencode_attempt,
        })
        raise exc
    out = _normalize_anthropic_response(msg)
    attempt = {
        "attempt_index": attempt_index, "status": "success", "http_status": 200,
        "error_type": None, "elapsed_ms": int((time.time() - started) * 1000),
        "stream_complete": True, **stream_state,
        "provider_request_id": out.get("id"),
        "stop_reason": out.get("stop_reason"),
        "usage": out.get("raw_usage"),
    }
    _emit_transport_event(raw_event_sink, {
        "record_type": "attempt_end", "attempt": attempt,
    })
    # Raw API-log side channel: the exact request body sent to the provider and
    # every SSE event received (incl. thinking). generate() lifts these into the
    # returned dict; the recorder writes them to files and strips them, so they
    # never reach committed rows or telemetry.
    out["_raw_request_body"] = body
    out["_raw_stream_events"] = raw_events
    out["_transport_attempt"] = attempt
    out["transport"] = _OPENCODE_TRANSPORT_LEGACY
    out["transport_revision"] = "opencode_urllib/1"
    return out


def _call_opencode_anthropic_sdk(
        messages, model, max_tokens, temperature, timeout, is_json,
        thinking_mode="adaptive", call_kind="primary", raw_event_sink=None,
        attempt_index=1, semantic_attempt_index=None,
        transport_generation_index=0, client_factory=None,
        transport_control=None):
    key = os.environ.get("OPENCODE_API_KEY") or os.environ.get("OPENCODE_GO_API_KEY")
    assert key, "Set OPENCODE_API_KEY for minimax-m3 via OpenCode Go"
    if Anthropic is None and client_factory is None:
        raise RuntimeError(
            "anthropic==0.104.1 is required for OpenCode MiniMax-M3 transport"
        )
    system, anthropic_messages = _messages_to_anthropic(messages)
    effective_max_tokens = _effective_minimax_max_tokens(max_tokens)
    thinking = _minimax_thinking_config(thinking_mode)
    effective_temperature = 1.0 if thinking["type"] == "adaptive" else temperature
    body = {
        "model": model,
        "messages": anthropic_messages,
        "max_tokens": effective_max_tokens,
        "temperature": effective_temperature,
        "thinking": thinking,
        "stream": True,
    }
    if system:
        body["system"] = system
    if is_json:
        body["system"] = (body.get("system", "") + "\n\nOutput valid JSON only.").strip()

    capture_events = [] if raw_event_sink is None else None
    started = time.time()
    attempt = {
        "attempt_index": attempt_index,
        "semantic_attempt_index": (
            semantic_attempt_index
            if semantic_attempt_index is not None else attempt_index
        ),
        "status": "in_progress",
        "http_status": None,
        "error_type": None,
        "elapsed_ms": None,
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
        "provider_request_id": None,
        "stop_reason": None,
        "usage": None,
    }
    _emit_transport_event(raw_event_sink, {
        "record_type": "attempt_start", "attempt_index": attempt_index,
        "transport": _OPENCODE_TRANSPORT_SDK, "call_kind": call_kind,
        "semantic_call_kind": call_kind,
        "attempt_kind": (
            ("transport_recovery_initial"
             if (semantic_attempt_index or attempt_index) == 1
             else "transport_recovery_retry")
            if transport_generation_index > 0 else
            ("transport_initial"
             if (semantic_attempt_index or attempt_index) == 1
             else "transport_retry")
        ),
        "generation_index": transport_generation_index,
        "transport_revision": _OPENCODE_TRANSPORT_REVISION,
        "request_url": _opencode_messages_url(), "request_body": body,
    })
    client = None
    started_blocks = set()
    stopped_blocks = set()
    open_blocks = set()
    block_sequence_valid = True
    terminal_phase = "before_message_start"
    terminal_sequence_valid = True
    try:
        factory = client_factory or Anthropic
        client = factory(
            api_key=key,
            base_url=_opencode_base_url(),
            timeout=timeout,
            max_retries=0,
            default_headers={"User-Agent": os.environ.get(
                "OPENCODE_USER_AGENT", "HybridPatch/2 anthropic-python"
            )},
        )
        if transport_control is not None:
            transport_control["client"] = client
        sdk_body = {k: v for k, v in body.items() if k != "stream"}
        with client.messages.stream(**sdk_body, timeout=timeout) as stream:
            for event in stream:
                event_dict = _as_plain_dict(event)
                event_type = event_dict.get("type")
                # MessageStream emits both protocol events and convenience
                # events (text/thinking/signature) whose `snapshot` is the full
                # accumulated block. Logging every convenience snapshot is
                # quadratic in output length. Keep only the protocol-shaped
                # events; they retain every delta plus terminal snapshots.
                if event_type in {
                    "message_start", "message_delta", "message_stop",
                    "content_block_start", "content_block_delta",
                    "content_block_stop",
                }:
                    if capture_events is not None:
                        capture_events.append(event_dict)
                    _emit_transport_event(raw_event_sink, {
                        "record_type": "sdk_stream_event",
                        "attempt_index": attempt_index,
                        "event": event_dict,
                    })
                if event_type == "message_start":
                    if terminal_phase != "before_message_start":
                        terminal_sequence_valid = False
                    else:
                        terminal_phase = "content"
                        attempt["message_start_seen"] = True
                elif event_type == "content_block_start":
                    if terminal_phase != "content":
                        terminal_sequence_valid = False
                    index = event_dict.get("index")
                    if (not isinstance(index, int) or isinstance(index, bool)
                            or index in started_blocks or index in open_blocks):
                        block_sequence_valid = False
                    else:
                        started_blocks.add(index)
                        open_blocks.add(index)
                elif event_type == "content_block_stop":
                    if terminal_phase != "content":
                        terminal_sequence_valid = False
                    index = event_dict.get("index")
                    if (not isinstance(index, int) or isinstance(index, bool)
                            or index not in open_blocks
                            or index in stopped_blocks):
                        block_sequence_valid = False
                    else:
                        open_blocks.remove(index)
                        stopped_blocks.add(index)
                elif event_type == "content_block_delta":
                    if terminal_phase != "content":
                        terminal_sequence_valid = False
                    index = event_dict.get("index")
                    if (not isinstance(index, int) or isinstance(index, bool)
                            or index not in open_blocks):
                        block_sequence_valid = False
                    delta_type = str((event_dict.get("delta") or {}).get("type") or "")
                    if _is_generation_delta_type(delta_type):
                        attempt["generation_delta_seen"] = True
                    if delta_type == "thinking_delta":
                        attempt["thinking_delta_seen"] = True
                    elif delta_type == "text_delta":
                        attempt["text_delta_seen"] = True
                    elif delta_type == "input_json_delta":
                        attempt["tool_delta_seen"] = True
                elif event_type == "message_delta":
                    if (terminal_phase != "content" or open_blocks
                            or attempt["message_delta_seen"]):
                        terminal_sequence_valid = False
                    else:
                        terminal_phase = "message_delta"
                        attempt["message_delta_seen"] = True
                    terminal_usage = event_dict.get("usage")
                    attempt["final_usage_seen"] = (
                        isinstance(terminal_usage, dict)
                        and terminal_usage.get("output_tokens") is not None
                    )
                elif event_type == "message_stop":
                    if (terminal_phase != "message_delta"
                            or attempt["message_stop_seen"]):
                        terminal_sequence_valid = False
                    else:
                        terminal_phase = "stopped"
                        attempt["message_stop_seen"] = True
            final_message = _as_plain_dict(stream.get_final_message())

        usage = final_message.get("usage") or {}
        attempt["content_blocks_started"] = len(started_blocks)
        attempt["content_blocks_stopped"] = len(stopped_blocks)
        # "Every started block was stopped" is vacuously true for a valid
        # complete empty message, whose content array can contain no blocks.
        attempt["content_blocks_balanced"] = (
            block_sequence_valid
            and not open_blocks
            and started_blocks == stopped_blocks
        )
        attempt["terminal_sequence_valid"] = (
            terminal_sequence_valid and terminal_phase == "stopped"
        )
        attempt["stop_reason"] = final_message.get("stop_reason")
        final_message_usage_complete = (
            usage.get("input_tokens") is not None
            and usage.get("output_tokens") is not None
        )
        attempt["final_usage_seen"] = (
            attempt["final_usage_seen"] and final_message_usage_complete
        )
        if not (
            attempt["message_start_seen"]
            and attempt["terminal_sequence_valid"]
            and attempt["content_blocks_balanced"]
            and attempt["message_delta_seen"]
            and attempt["message_stop_seen"]
            and attempt["final_usage_seen"]
            and isinstance(attempt["stop_reason"], str)
            and bool(attempt["stop_reason"].strip())
        ):
            raise _IncompleteStreamError(
                "OpenCode SDK stream ended before the complete Anthropic terminal chain"
            )

        out = _normalize_anthropic_response(final_message)
        attempt.update({
            "status": "success",
            "http_status": 200,
            "elapsed_ms": int((time.time() - started) * 1000),
            "stream_complete": True,
            "provider_request_id": out.get("id"),
            "stop_reason": out.get("stop_reason"),
            "usage": out.get("raw_usage"),
            "content_block_counts": out.get("content_block_counts"),
        })
        _emit_transport_event(raw_event_sink, {
            "record_type": "attempt_end", "attempt": attempt,
        })
        out["_raw_request_body"] = body
        out["_raw_stream_events"] = capture_events
        out["_transport_attempt"] = dict(attempt)
        out["transport"] = _OPENCODE_TRANSPORT_SDK
        out["transport_revision"] = _OPENCODE_TRANSPORT_REVISION
        return out
    except Exception as exc:
        attempt["content_blocks_started"] = len(started_blocks)
        attempt["content_blocks_stopped"] = len(stopped_blocks)
        attempt["content_blocks_balanced"] = (
            block_sequence_valid
            and not open_blocks
            and started_blocks == stopped_blocks
        )
        attempt["terminal_sequence_valid"] = (
            terminal_sequence_valid and terminal_phase == "stopped"
        )
        attempt.update({
            "status": (
                "retryable_error"
                if _is_retryable_opencode_error(exc, attempt) else "fatal_error"
            ),
            "http_status": _transport_status_code(exc),
            "error_type": _transport_error_type(exc, attempt),
            "error_message": str(exc)[:1000],
            "elapsed_ms": int((time.time() - started) * 1000),
            "stream_complete": False,
        })
        try:
            exc._opencode_attempt = dict(attempt)
        except Exception:
            pass
        _emit_transport_event(raw_event_sink, {
            "record_type": "attempt_end", "attempt": attempt,
        })
        raise
    finally:
        if (transport_control is not None
                and transport_control.get("client") is client):
            transport_control.pop("client", None)
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass


def _call_opencode_messages(
        messages, model, max_tokens, temperature, timeout, is_json,
        thinking_mode="adaptive", call_kind="primary", raw_event_sink=None,
        attempt_index=1, semantic_attempt_index=None,
        transport_generation_index=0, transport_control=None):
    if _opencode_transport() == _OPENCODE_TRANSPORT_LEGACY:
        return _call_opencode_messages_urllib_v1(
            messages, model, max_tokens, temperature, timeout, is_json,
            thinking_mode=thinking_mode, call_kind=call_kind,
            raw_event_sink=raw_event_sink, attempt_index=attempt_index)
    return _call_opencode_anthropic_sdk(
        messages, model, max_tokens, temperature, timeout, is_json,
        thinking_mode=thinking_mode, call_kind=call_kind,
        raw_event_sink=raw_event_sink, attempt_index=attempt_index,
        semantic_attempt_index=semantic_attempt_index,
        transport_generation_index=transport_generation_index,
        transport_control=transport_control)



def _prompt_cache_usage(usage):
    """Return (cache_hit_tokens, cache_miss_tokens, provider_reported_cache)."""
    prompt_tokens = usage.get("prompt_tokens", 0) or 0

    hit = usage.get("prompt_cache_hit_tokens")
    miss = usage.get("prompt_cache_miss_tokens")
    if hit is not None or miss is not None:
        hit = hit or 0
        miss = miss if miss is not None else max(prompt_tokens - hit, 0)
        return hit, miss, True

    ptd = usage.get("prompt_tokens_details")
    if ptd and isinstance(ptd, dict) and ptd.get("cached_tokens") is not None:
        hit = ptd.get("cached_tokens", 0) or 0
        return hit, max(prompt_tokens - hit, 0), True

    return 0, prompt_tokens, False


def _token_rates(completion_tokens, total_tokens, elapsed):
    if not elapsed or elapsed <= 0:
        return None, None
    return completion_tokens / elapsed, total_tokens / elapsed


def _estimate_usd_cost(model, usage):
    """Best-effort USD cost estimate from usage dict. Returns 0 if model unknown."""
    prompt_tokens = usage.get("prompt_tokens", 0) or 0
    completion_tokens = usage.get("completion_tokens", 0) or 0
    pricing = _match_pricing(model, _PRICING_USD)
    if not pricing:
        return 0.0
    inp_cost, out_cost = pricing

    cached, non_cached, cache_available = _prompt_cache_usage(usage)
    if not cache_available:
        cached, non_cached = 0, prompt_tokens
    return (
        ((non_cached + cached * 0.5) / 1_000_000) * inp_cost
        + (completion_tokens / 1_000_000) * out_cost
    )


def _estimate_cny_cost(model, usage):
    """Best-effort CNY cost estimate for DeepSeek V4 models."""
    pricing = _match_pricing(model, _DEEPSEEK_PRICING_CNY)
    if not pricing:
        return 0.0

    hit_price, miss_price, output_price = pricing
    cache_hit, cache_miss, cache_available = _prompt_cache_usage(usage)
    if not cache_available:
        cache_hit = 0
        cache_miss = usage.get("prompt_tokens", 0) or 0
    completion_tokens = usage.get("completion_tokens", 0) or 0
    return (
        (cache_hit / 1_000_000) * hit_price
        + (cache_miss / 1_000_000) * miss_price
        + (completion_tokens / 1_000_000) * output_price
    )


def _estimate_costs(model, usage):
    total_usd = _estimate_usd_cost(model, usage)
    total_cny = _estimate_cny_cost(model, usage)
    currency = None
    if _match_pricing(model, _DEEPSEEK_PRICING_CNY):
        currency = "CNY"
    elif _match_pricing(model, _PRICING_USD):
        currency = "USD"
    return {"total_usd": total_usd, "total_cny": total_cny, "cost_currency": currency}


def _estimate_runtime_costs(model, usage, *, opencode_zen=False):
    if opencode_zen:
        pricing = _match_pricing(model, _OPENCODE_PRICING_USD)
        prompt_tokens = usage.get("prompt_tokens", 0) or 0
        completion_tokens = usage.get("completion_tokens", 0) or 0
        cached, non_cached, cache_available = _prompt_cache_usage(usage)
        if not cache_available:
            cached, non_cached = 0, prompt_tokens
        total_usd = 0.0
        if pricing:
            input_price, output_price = pricing
            total_usd = (
                ((non_cached + cached * 0.2) / 1_000_000) * input_price
                + (completion_tokens / 1_000_000) * output_price
            )
        return {
            "total_usd": total_usd,
            "total_cny": 0.0,
            "cost_currency": "USD",
        }
    return _estimate_costs(model, usage)


# ── Model maps (alias → deployment name) ────────────────────────────────

model_maps = {
    # Add your own aliases here, e.g.:
    # "t-gpt-4o": "gpt-4o-2024-11-20",
}


def resolve_model_name(model_name):
    """Strip t- prefix and resolve aliases."""
    name = model_maps.get(model_name, model_name)
    if name.startswith("t-"):
        name = name[2:]
    return name


# ── Main class ───────────────────────────────────────────────────────────

class OpenAI_Model:
    def __init__(self, instance=None):
        """Create a wrapper that selects the right OpenAI-compatible client.

        Args:
            instance: Ignored (for API compatibility with internal TRAPI).
        """
        self._client_cache = {}
        self.client = None

    def _default_client(self):
        base_url = os.environ.get("OPENAI_BASE_URL") or None
        _reject_azure_opencode_zen_conflict(base_url)
        if self.client is not None:
            return self.client
        azure_key = os.environ.get("AZURE_OPENAI_API_KEY")
        azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
        if azure_key and azure_endpoint:
            self.client = AzureOpenAI(
                api_key=azure_key,
                azure_endpoint=azure_endpoint,
                api_version="2024-10-01-preview",
            )
        else:
            openai_key = os.environ.get("OPENAI_API_KEY")
            assert openai_key, (
                "Set OPENAI_API_KEY (or AZURE_OPENAI_API_KEY + AZURE_OPENAI_ENDPOINT)"
            )
            client_args = {"api_key": openai_key, "base_url": base_url}
            if _normalized_base_url(base_url) == _OPENCODE_ZEN_BASE_URL:
                # generate() owns retries so every OpenCode attempt is visible.
                client_args["max_retries"] = 0
            self.client = OpenAI(**client_args)
        return self.client

    def _client_for_model(self, model):
        return self._default_client()

    def _minimax_official_client(self):
        cached = self._client_cache.get("minimax_official")
        if cached is None:
            key = os.environ.get("MINIMAX_API_KEY")
            assert key, "Set MINIMAX_API_KEY for MINIMAX_TRANSPORT=official_nonstream"
            # SDK retries stay 0: the baseline-aligned blanket-exception loop in
            # generate() owns every retry so attempt counts remain honest.
            cached = OpenAI(api_key=key, base_url=_MINIMAX_OFFICIAL_BASE_URL,
                            max_retries=0)
            self._client_cache["minimax_official"] = cached
        return cached

    def generate(
        self,
        messages,
        model="gpt-4o-mini",
        timeout=30,
        max_retries=3,
        temperature=1.0,
        is_json=False,
        return_metadata=False,
        max_tokens=None,
        variables={},
        instance=None,
        thinking_mode="adaptive",
        reasoning_effort=None,
        call_kind="primary",
        _raw_event_sink=None,
        max_response_retries=1,
        max_transient_failures=3,
        _retry_state=None,
        _response_commit_sink=None,
    ):
        """Call the chat completions API.

        Args:
            messages: List of {"role": ..., "content": ...} dicts.
            model: Model name (aliases in model_maps are resolved automatically).
            timeout: Per-request timeout in seconds.
            max_retries: Legacy retry option for non-MiniMax providers only.
            temperature: Sampling temperature.
            is_json: If True, request JSON output mode.
            return_metadata: If True, return dict with message + usage stats.
            max_tokens: Max completion tokens.
            variables: Dict of [[KEY]] → value replacements for the prompt.
            instance: Ignored (API compat).
            thinking_mode: MiniMax Anthropic thinking mode (adaptive by default).
            reasoning_effort: OpenAI-compatible reasoning effort. Formal
                OpenCode Zen DeepSeek runs use ``high``.
            call_kind: Audit label such as hybridpatch_primary or repair.
            max_response_retries: MiniMax retries after a response began but the
                Anthropic terminal chain was incomplete. Frozen maximum is one.
            max_transient_failures: MiniMax pre-generation infrastructure
                failures tolerated without consuming a response slot.

        Returns:
            str if return_metadata=False, else dict with keys:
                message, elapsed_time, prompt_tokens, completion_tokens,
                reasoning_tokens, total_tokens, total_usd, total_cny,
                output_tokens_per_second, total_tokens_per_second
        """
        resolved = resolve_model_name(model)
        kwargs = {}
        if is_json:
            kwargs["response_format"] = {"type": "json_object"}

        messages = _format_messages(messages, variables)

        # o1/o3 models don't support system messages — fold into first user msg
        if resolved.startswith(("o1", "o3", "o4")) and len(messages) > 1 and messages[0]["role"] == "system" and messages[1]["role"] == "user":
            system_message = messages[0]["content"]
            messages[1]["content"] = f"System Message: {system_message}\n{messages[1]['content']}"
            messages = messages[1:]

        is_minimax = _is_minimax_model(resolved)
        is_minimax_official = (
            is_minimax and _minimax_transport() == _MINIMAX_TRANSPORT_OFFICIAL
        )
        effective_max_tokens = (
            _effective_minimax_max_tokens(max_tokens) if is_minimax else max_tokens
        )
        effective_thinking_mode = (
            _minimax_thinking_config(thinking_mode)["type"] if is_minimax else None
        )
        effective_reasoning_effort = _effective_reasoning_effort(reasoning_effort)
        if is_minimax and effective_reasoning_effort is not None:
            raise ValueError(
                "reasoning_effort is only valid for OpenAI-compatible models"
            )
        is_opencode_zen = (
            not is_minimax and _is_opencode_zen_runtime(resolved)
        )
        effective_temperature = (
            1.0 if is_minimax and effective_thinking_mode == "adaptive" else temperature
        )
        t0 = time.time()
        last_err = None
        # Watchdog is at least the env-configurable bound and honors a larger
        # per-call timeout, so raising MINIMAX_HARD_TIMEOUT actually extends it
        # (min() would have pinned it to the runner's shorter value).
        hard_to = max(timeout or 0, _MINIMAX_HARD_TIMEOUT) if is_minimax else None
        # Socket timeout matches the watchdog so a slow extended-thinking call is
        # not cut off before the gateway responds.
        # Let the SDK's socket timeout fire before the outer watchdog.  This
        # leaves a grace window in which the POST is known closed and can be
        # classified as an ordinary pre-generation transient instead of an
        # ambiguous in-flight request.
        eff_timeout = (
            max(1, hard_to - min(30, max(1, hard_to * 0.05)))
            if is_minimax and not is_minimax_official
            else (hard_to if is_minimax else timeout)
        )
        if is_minimax_official:
            client = self._minimax_official_client()
        else:
            client = None if is_minimax else self._client_for_model(resolved)
        response = None
        attempt = 0
        quota_waits = 0
        transient_waits = 0
        timeout_hit = False
        last_error_type = None
        transport_attempts = []
        raw_request_body = None
        if is_minimax and not is_minimax_official:
            max_response_slots = min(
                _MINIMAX_MAX_RESPONSE_SLOTS,
                max(1, 1 + int(max_response_retries or 0)),
            )
            effective_max_transient_failures = min(
                _MINIMAX_MAX_TRANSIENT_FAILURES,
                max(0, int(max_transient_failures or 0)),
            )
            retry_state = dict(_retry_state or {})
            response_slots_used = int(retry_state.get("response_slots_used") or 0)
            transient_failure_count = int(retry_state.get("transient_failure_count") or 0)
            http_attempt_index = int(retry_state.get("http_attempts_used") or 0)

            while True:
                if response_slots_used >= max_response_slots:
                    last_err = RuntimeError("response retry budget already exhausted")
                    raise OpenCodeTransportError(
                        "OpenCode MiniMax-M3 response slots exhausted before a complete response",
                        attempts=transport_attempts,
                        last_error=last_err,
                    )
                if transient_failure_count >= effective_max_transient_failures:
                    last_err = RuntimeError("transient infrastructure budget already exhausted")
                    raise OpenCodeTransportError(
                        "OpenCode MiniMax-M3 transient infrastructure budget exhausted",
                        attempts=transport_attempts,
                        last_error=last_err,
                    )

                http_attempt_index += 1
                attempt_index = http_attempt_index
                attempt_started = time.time()
                live_progress = {"generation_delta_seen": False}
                transport_control = {}

                def _attempt_event_sink(payload):
                    if payload.get("record_type") == "sdk_stream_event":
                        event = payload.get("event") or {}
                        if event.get("type") == "content_block_delta":
                            delta_type = str((event.get("delta") or {}).get("type") or "")
                            if _is_generation_delta_type(delta_type):
                                live_progress["generation_delta_seen"] = True
                    _emit_transport_event(_raw_event_sink, payload)

                # Preserve the recorder's fail-closed contract through this
                # nested progress wrapper.  Otherwise a ledger/fsync error or
                # per-attempt campaign guard exception is silently swallowed
                # by the outer best-effort event emitter and a POST proceeds.
                _attempt_event_sink._anchorpatch_critical = bool(
                    getattr(_raw_event_sink, "_anchorpatch_critical", False)
                )

                def _do_minimax_call(_attempt_index=attempt_index):
                    semantic_attempt_index = (
                        response_slots_used + transient_failure_count + 1
                    )
                    return _call_opencode_messages(
                        messages, resolved, effective_max_tokens,
                        effective_temperature, eff_timeout, is_json,
                        thinking_mode=effective_thinking_mode,
                        call_kind=call_kind,
                        raw_event_sink=_attempt_event_sink,
                        attempt_index=_attempt_index,
                        semantic_attempt_index=semantic_attempt_index,
                        transport_generation_index=int(
                            retry_state.get("generation_index") or 0
                        ),
                        transport_control=transport_control,
                    )

                try:
                    future = _WATCHDOG_POOL.submit(_do_minimax_call)
                    response = future.result(timeout=hard_to)
                    successful_attempt = response.pop("_transport_attempt", None)
                    if successful_attempt:
                        response_slots_used += 1
                        successful_attempt["budget_class"] = "response_slot"
                        successful_attempt["response_slot_index"] = response_slots_used
                        successful_attempt["transient_failure_count"] = transient_failure_count
                        transport_attempts.append(successful_attempt)
                    response["_transport_attempts"] = list(transport_attempts)
                    response["_retry_budget_state"] = {
                        "max_response_slots": max_response_slots,
                        "response_slots_used": response_slots_used,
                        "response_retry_used": response_slots_used > 1,
                        "max_transient_failures": effective_max_transient_failures,
                        "transient_failure_count": transient_failure_count,
                        "http_attempts_used": http_attempt_index,
                    }
                    break
                except concurrent.futures.TimeoutError as exc:
                    last_err = RuntimeError(f"hard wall-clock timeout after {hard_to}s")
                    last_error_type = "TimeoutError"
                    timeout_hit = True
                    client_to_close = transport_control.get("client")
                    if client_to_close is not None and hasattr(client_to_close, "close"):
                        try:
                            client_to_close.close()
                        except Exception:
                            pass
                    future.cancel()
                    rec = _attempt_from_exception(
                        last_err, attempt_index,
                        int((time.time() - attempt_started) * 1000),
                    )
                    rec["generation_delta_seen"] = bool(live_progress["generation_delta_seen"])
                    rec["status"] = "retryable_error"
                    rec["error_type"] = "watchdog_ambiguous_inflight"
                    if rec["generation_delta_seen"]:
                        response_slots_used += 1
                        rec["budget_class"] = "response_slot"
                        rec["response_slot_index"] = response_slots_used
                    else:
                        transient_failure_count += 1
                        rec["budget_class"] = "transient_failure"
                        rec["transient_failure_index"] = transient_failure_count
                    rec["transient_failure_count"] = transient_failure_count
                    transport_attempts.append(rec)
                    _emit_transport_event(_raw_event_sink, {
                        "record_type": "attempt_end", "attempt": rec,
                    })
                    _emit_transport_event(_raw_event_sink, {
                        "record_type": "attempt_budget",
                        "attempt_index": attempt_index,
                        "budget_class": rec["budget_class"],
                        "response_slots_used": response_slots_used,
                        "transient_failure_count": transient_failure_count,
                    })
                    import sys as _sys
                    print(
                        f"[model_openai] watchdog: abandoned stalled {resolved} call "
                        f"after {hard_to}s (HTTP attempt {attempt_index})",
                        file=_sys.stderr, flush=True,
                    )
                    ambiguous = OpenCodeTransportError(
                        "OpenCode MiniMax-M3 watchdog expired while the prior POST "
                        "could still be in flight; refusing an overlapping retry",
                        attempts=transport_attempts,
                        last_error=last_err,
                    )
                    ambiguous._anchorpatch_transport_observability_failure = True
                    raise ambiguous from exc
                except Exception as exc:
                    last_err = exc
                    last_error_type = _transport_error_type(exc)
                    rec = _attempt_from_exception(
                        exc, attempt_index,
                        int((time.time() - attempt_started) * 1000),
                    )
                    timeout_hit = timeout_hit or rec.get("error_type") == "timeout"

                retryable = _is_retryable_opencode_error(last_err)
                if not retryable:
                    rec["budget_class"] = "fatal"
                    transport_attempts.append(rec)
                    raise OpenCodeTransportError(
                        "OpenCode MiniMax-M3 failed with a non-retryable "
                        f"error: {_transport_error_message(last_err)}",
                        attempts=transport_attempts,
                        last_error=last_err,
                    ) from None

                if rec.get("generation_delta_seen"):
                    response_slots_used += 1
                    rec["budget_class"] = "response_slot"
                    rec["response_slot_index"] = response_slots_used
                else:
                    transient_failure_count += 1
                    rec["budget_class"] = "transient_failure"
                    rec["transient_failure_index"] = transient_failure_count
                    if rec.get("error_type") == "rate_limit":
                        quota_waits += 1
                    else:
                        transient_waits += 1
                rec["transient_failure_count"] = transient_failure_count
                transport_attempts.append(rec)
                _emit_transport_event(_raw_event_sink, {
                    "record_type": "attempt_budget",
                    "attempt_index": attempt_index,
                    "budget_class": rec.get("budget_class"),
                    "response_slots_used": response_slots_used,
                    "transient_failure_count": transient_failure_count,
                })

                if response_slots_used >= max_response_slots:
                    raise OpenCodeTransportError(
                        "OpenCode MiniMax-M3 response retry exhausted after an incomplete stream",
                        attempts=transport_attempts,
                        last_error=last_err,
                    ) from None
                if transient_failure_count >= effective_max_transient_failures:
                    raise OpenCodeTransportError(
                        "OpenCode MiniMax-M3 transient infrastructure failures exhausted",
                        attempts=transport_attempts,
                        last_error=last_err,
                    ) from None
                time.sleep(_retry_after_seconds(last_err, attempt_index))
            attempt = max(response_slots_used - 1, 0)
        else:
            max_attempts = max(1, int(max_retries) if max_retries is not None else 1)
            attempt_index = 0
            retry_budget_attempt_index = 0
            while True:
                http_attempt_index = attempt_index + 1
                attempt_started = time.time()
                terminal_error = None
                retry_delay = None
                if not is_minimax_official:
                    _emit_transport_event(_raw_event_sink, {
                        "record_type": "attempt_start",
                        "attempt_index": http_attempt_index,
                        "attempt_kind": (
                            "openai_compatible_initial"
                            if http_attempt_index == 1
                            else "openai_compatible_retry"
                        ),
                    })
                try:
                    extra = dict(kwargs)
                    request_model = resolved
                    if is_minimax_official:
                        # Baseline-aligned MiniMax official call: blocking
                        # non-streaming create(); reasoning_split keeps <think>
                        # content out of message.content.
                        request_model = _MINIMAX_OFFICIAL_MODEL
                        extra["max_completion_tokens"] = effective_max_tokens
                        extra["extra_body"] = {
                            "thinking": {"type": effective_thinking_mode or "adaptive"},
                            "reasoning_split": True,
                        }
                    elif max_tokens is not None:
                        extra["max_completion_tokens"] = max_tokens
                    if not is_minimax_official and effective_reasoning_effort:
                        extra["reasoning_effort"] = effective_reasoning_effort
                    raw_request_body = {
                        "model": request_model,
                        "messages": messages,
                        "temperature": effective_temperature,
                        **extra,
                    }
                    request_kwargs = {
                        "model": request_model,
                        "messages": messages,
                        "timeout": eff_timeout,
                        "temperature": effective_temperature,
                        **extra,
                    }
                    if is_opencode_zen:
                        raw_request_body.update({
                            "stream": True,
                            "stream_options": {"include_usage": True},
                        })
                        response = _call_openai_compatible_stream(
                            client,
                            request_kwargs,
                            raw_event_sink=_raw_event_sink,
                            attempt_index=http_attempt_index,
                        )
                    else:
                        response = client.chat.completions.create(
                            **request_kwargs
                        )
                    if not is_minimax_official:
                        success_record = {
                            "attempt_index": http_attempt_index,
                            "status": "success",
                            "error_type": None,
                            "http_status": 200,
                            "stream_complete": True,
                        }
                        if is_opencode_zen:
                            success_record.update(
                                response.pop("_stream_attempt_state", {})
                            )
                        transport_attempts.append(success_record)
                        _emit_transport_event(_raw_event_sink, {
                            "record_type": "attempt_end",
                            "attempt_index": http_attempt_index,
                            "attempt": success_record,
                        })
                    break
                except Exception as exc:
                    if (is_minimax_official and _is_official_rate_limit(exc)
                            and quota_waits < _MINIMAX_OFFICIAL_MAX_QUOTA_WAITS):
                        quota_waits += 1
                        import sys as _sys
                        print(
                            f"[model_openai] MiniMax official rate limit; waiting "
                            f"{_MINIMAX_OFFICIAL_QUOTA_WAIT_SECONDS}s before resume "
                            f"(quota wait {quota_waits}/{_MINIMAX_OFFICIAL_MAX_QUOTA_WAITS})",
                            file=_sys.stderr, flush=True,
                        )
                        time.sleep(_MINIMAX_OFFICIAL_QUOTA_WAIT_SECONDS)
                        continue
                    attempt_index += 1
                    last_err = exc
                    last_error_type = _transport_error_type(exc)
                    attempt = attempt_index
                    if is_minimax_official:
                        if attempt_index >= max_attempts:
                            terminal_error = RuntimeError(
                                f"Failed after {max_attempts} attempt(s): "
                                f"{_transport_error_message(last_err)}"
                        )
                        else:
                            retry_delay = 4
                    else:
                        attempt_record = _attempt_from_exception(
                            exc, http_attempt_index,
                            int((time.time() - attempt_started) * 1000),
                        )
                        retryable = attempt_record["status"] == "retryable_error"
                        free_opencode_503 = (
                            is_opencode_zen
                            and _transport_status_code(exc) == 503
                            and not attempt_record.get("generation_delta_seen")
                        )
                        if not free_opencode_503:
                            retry_budget_attempt_index += 1
                        attempt_record["retry_budget_consumed"] = (
                            not free_opencode_503
                        )
                        attempt_record["retry_budget_attempt_index"] = (
                            retry_budget_attempt_index
                        )
                        attempt_record["retry_after_seconds"] = (
                            _retry_after_seconds(
                                exc, attempt_index,
                                exponential_503=is_opencode_zen,
                            )
                        )
                        transport_attempts.append(attempt_record)
                        _emit_transport_event(_raw_event_sink, {
                            "record_type": "attempt_end",
                            "attempt_index": http_attempt_index,
                            "attempt": attempt_record,
                        })
                        if not retryable or (
                            not free_opencode_503
                            and retry_budget_attempt_index >= max_attempts
                        ):
                            terminal_error = OpenAICompatibleTransportError(
                                "OpenAI-compatible provider failed after "
                                f"{attempt_index} attempt(s)",
                                attempts=transport_attempts,
                                last_error=last_err,
                            )
                        else:
                            if attempt_record["error_type"] == "rate_limit":
                                quota_waits += 1
                            else:
                                transient_waits += 1
                            retry_delay = attempt_record["retry_after_seconds"]
                    if terminal_error is not None:
                        # The wrapper already owns a sanitized copy. Replace the
                        # raw SDK exception retained by this generator frame so
                        # crash reporters that capture frame locals cannot
                        # recover a provider body or credential.
                        last_err = _sanitized_transport_cause(last_err)
                # Raise only after leaving the provider exception handler.  A
                # ``raise ... from None`` inside ``except`` suppresses display
                # of the raw SDK exception but still retains it in
                # ``__context__`` for crash-reporting/introspection code.
                if terminal_error is not None:
                    raise terminal_error
                if retry_delay is not None:
                    time.sleep(retry_delay)
            if not is_minimax_official:
                attempt = max(len(transport_attempts) - 1, 0)

        elapsed = time.time() - t0
        if isinstance(response, dict):
            resp = response
        else:
            resp = response.to_dict() if hasattr(response, "to_dict") else response.model_dump()
        if raw_request_body is not None:
            resp["_raw_request_body"] = raw_request_body
        usage = resp.get("usage", {})
        raw_usage = resp.get("raw_usage") or {}
        choices = resp.get("choices") or []
        if not choices or not isinstance(choices[0], dict):
            raise RuntimeError("provider response missing choices[0]")
        choice0 = choices[0]
        message0 = choice0.get("message") or {}
        if not isinstance(message0, dict):
            raise RuntimeError("provider response missing choices[0].message")
        if "content" not in message0 and not is_minimax_official:
            raise RuntimeError("provider response missing choices[0].message.content")
        # MiniMax official omits the content key entirely on thinking-only
        # server aborts (finish_reason="abort"; to_dict(exclude_unset) then
        # drops the unset field). Baseline semantics: a complete HTTP-200 is
        # accepted as-is, so missing content is an empty response, not a crash.
        response_text = message0.get("content")
        if response_text is None and is_minimax_official:
            response_text = ""
        finish_reason = (choice0.get("finish_reason") or resp.get("finish_reason")
                         or resp.get("stop_reason"))
        official_classification = None
        if is_minimax_official:
            # Same taxonomy as transport-v3 so telemetry stays comparable; a
            # complete non-streaming HTTP response is accepted as-is (baseline
            # semantics), classification is disclosure only.
            has_text = bool((response_text or "").strip())
            if finish_reason == "length":
                official_classification = (
                    "text_truncated" if has_text else "thinking_budget_exhausted")
            elif finish_reason == "abort":
                # Server-side generation abort — the official-endpoint costume of
                # the mid-thinking truncation pathology (FINDINGS §227).
                official_classification = (
                    "aborted_partial_text" if has_text else "model_empty")
            else:
                official_classification = "normal" if has_text else "model_empty"
        costs = _estimate_runtime_costs(
            resolved, usage, opencode_zen=is_opencode_zen
        )
        prompt_cache_hit, prompt_cache_miss, cache_available = _prompt_cache_usage(usage)

        # Extract reasoning tokens if present (o1/o3 models)
        reasoning_tokens = usage.get("reasoning_tokens", 0) or 0
        ctd = usage.get("completion_tokens_details")
        if ctd and isinstance(ctd, dict):
            reasoning_tokens = ctd.get("reasoning_tokens", reasoning_tokens) or 0

        completion_tokens = usage.get("completion_tokens", 0) or 0
        total_tokens = usage.get("total_tokens", 0) or 0
        output_tps, total_tps = _token_rates(completion_tokens, total_tokens, elapsed)

        if not return_metadata:
            return response_text

        retry_budget_state = resp.get("_retry_budget_state") or {}
        if is_minimax_official:
            provider_label = "minimax_official"
            transport_label = "openai_sdk_nonstream"
            transport_revision_label = _MINIMAX_OFFICIAL_REVISION
            base_url_label = _MINIMAX_OFFICIAL_BASE_URL
            request_url_label = _MINIMAX_OFFICIAL_BASE_URL + "/chat/completions"
        elif is_minimax:
            provider_label = "opencode_go"
            transport_label = resp.get("transport")
            transport_revision_label = resp.get("transport_revision")
            base_url_label = _opencode_base_url()
            request_url_label = _opencode_messages_url()
        else:
            runtime = openai_compatible_runtime_config(
                resolved,
                max_tokens=effective_max_tokens,
                reasoning_effort=effective_reasoning_effort,
            )
            provider_label = runtime["provider"]
            transport_label = runtime["transport"]
            transport_revision_label = runtime["transport_revision"]
            base_url_label = runtime["base_url"]
            request_url_label = runtime["request_url"]
        result = {
            "message": response_text,
            # Raw API-log side channel (recorder writes then strips these):
            # the exact request sent and the full raw response incl. thinking.
            "_raw_request_messages": messages,
            "_raw_request_body": resp.get("_raw_request_body"),
            "_raw_stream_events": resp.get("_raw_stream_events"),
            "_raw_response_full": {k: v for k, v in resp.items()
                                   if k not in ("_raw_request_body", "_raw_stream_events")},
            "response_id": resp.get("id"),
            "provider_request_id": resp.get("id"),
            "resolved_model": resolved,
            "provider": provider_label,
            "transport": transport_label,
            "transport_revision": transport_revision_label,
            "transport_resume_policy": (
                _OPENCODE_TRANSPORT_RESUME_POLICY
                if is_minimax and not is_minimax_official else None
            ),
            "base_url": base_url_label,
            "request_url": request_url_label,
            "anthropic_sdk_version": (
                getattr(anthropic, "__version__", None)
                if is_minimax and not is_minimax_official and anthropic else None
            ),
            "temperature": effective_temperature,
            "requested_temperature": temperature,
            "max_tokens": effective_max_tokens,
            "requested_max_tokens": max_tokens,
            "thinking_mode": effective_thinking_mode,
            "reasoning_effort": effective_reasoning_effort,
            "call_kind": call_kind,
            "call_kinds": [call_kind],
            "timeout": eff_timeout,
            "max_retries": max_retries if (is_minimax_official or not is_minimax) else None,
            "max_response_slots": retry_budget_state.get("max_response_slots"),
            "response_slots_used": retry_budget_state.get("response_slots_used"),
            "max_response_retries": (
                retry_budget_state.get("max_response_slots", 1) - 1
                if is_minimax and not is_minimax_official else None
            ),
            "response_retry_used": bool(retry_budget_state.get("response_retry_used")),
            "max_transient_failures": retry_budget_state.get("max_transient_failures"),
            "transient_failure_count": retry_budget_state.get("transient_failure_count"),
            "http_attempts_used": (
                attempt + 1 if (is_minimax_official or not is_minimax)
                else retry_budget_state.get("http_attempts_used")
            ),
            "retry_count": (
                attempt if (is_minimax_official or not is_minimax)
                else max((retry_budget_state.get("response_slots_used") or 1) - 1, 0)
            ),
            "failed_attempt_count": (
                attempt if is_minimax_official else sum(
                    a.get("status") != "success"
                    for a in (resp.get("_transport_attempts") or transport_attempts)
                )
            ),
            "quota_wait_count": quota_waits,
            "rate_limit_wait_count": quota_waits,
            "transient_wait_count": transient_waits,
            "timeout_hit": timeout_hit,
            "last_error_type": last_error_type,
            "http_status": resp.get("http_status", 200),
            "finish_reason": finish_reason,
            "stop_reason": resp.get("stop_reason") or finish_reason,
            "stream_complete": (
                True if is_minimax_official
                else resp.get("stream_complete")
                if (is_minimax or is_opencode_zen)
                else True
            ),
            "response_classification": (
                official_classification if is_minimax_official
                else resp.get("response_classification")
                if is_minimax
                else (
                    "normal" if str(response_text or "").strip()
                    else "model_empty"
                )
            ),
            "response_classifications": (
                [official_classification] if is_minimax_official
                else (
                    [resp.get("response_classification")]
                    if is_minimax and resp.get("response_classification")
                    else [] if is_minimax
                    else ["normal" if str(response_text or "").strip()
                          else "model_empty"]
                )
            ),
            "transport_attempts": resp.get("_transport_attempts") or transport_attempts,
            "content_block_counts": resp.get("content_block_counts") or {},
            "content_block_count": resp.get("content_block_count") or 0,
            "elapsed_time": elapsed,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": completion_tokens,
            "reasoning_tokens": reasoning_tokens,
            "total_tokens": total_tokens,
            "total_usd": costs["total_usd"],
            "total_cny": costs["total_cny"],
            "cost_currency": costs["cost_currency"],
            "prompt_cache_hit_tokens": prompt_cache_hit,
            "prompt_cache_miss_tokens": prompt_cache_miss,
            "prompt_cache_usage_available": cache_available,
            "input_tokens": (
                usage.get("prompt_tokens", 0)
                if (is_minimax_official or not is_minimax)
                else raw_usage.get("input_tokens")
            ),
            "output_tokens": (
                completion_tokens if (is_minimax_official or not is_minimax)
                else raw_usage.get("output_tokens")
            ),
            "cache_read_input_tokens": raw_usage.get("cache_read_input_tokens"),
            "cache_creation_input_tokens": raw_usage.get("cache_creation_input_tokens"),
            "output_tokens_per_second": output_tps,
            "total_tokens_per_second": total_tps,
        }
        if _response_commit_sink is not None:
            _response_commit_sink(result)
        return result

    def generate_json(self, messages, model="gpt-4o-mini", **kwargs):
        """Generate a JSON response and return the parsed dict."""
        response = self.generate(messages, model, is_json=True, return_metadata=True, **kwargs)
        return json.loads(response["message"])

    def cost_calculator(self, model, usage):
        """Compute cost from a usage dict (for model_agentic.py compat)."""
        resolved = resolve_model_name(model)
        return _estimate_usd_cost(resolved, usage)

    def cost_calculator_cny(self, model, usage):
        """Compute DeepSeek RMB cost from a usage dict."""
        resolved = resolve_model_name(model)
        return _estimate_cny_cost(resolved, usage)


# ── Module-level convenience functions ───────────────────────────────────
_model = OpenAI_Model()
generate = _model.generate
generate_json = _model.generate_json


if __name__ == "__main__":
    response = generate(
        [{"role": "user", "content": "Tell me a one-line joke."}],
        model="t-gpt-4o-mini",
        return_metadata=True,
    )
    print(response)
