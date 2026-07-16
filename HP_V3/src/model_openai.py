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
# Provider hard ceiling for max_tokens, probed live 2026-07-07: the MiniMax
# endpoint rejects max_tokens > 524288 ("does not support max tokens > 524288").
# Passing max_tokens=None to generate() means "uncapped": the minimax body uses
# this ceiling (thinking budget scales with it), i.e. no artificial output cap.
_MINIMAX_MAX_TOKENS_CEILING = int(os.environ.get("MINIMAX_MAX_TOKENS_CEILING", "524288"))
_MINIMAX_QUOTA_WAIT_LIMIT = 3
_MINIMAX_TRANSIENT_WAIT_LIMIT = 10
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

# Per-1M-token CNY costs: (input cache hit, input cache miss, output).
# Source: DeepSeek official Chinese pricing page, checked 2026-06-06.
_DEEPSEEK_PRICING_CNY = {
    "deepseek-v4-flash": (0.02, 1.0, 2.0),
    "deepseek-v4-pro":   (0.025, 3.0, 6.0),
}

_OPENCODE_GO_MESSAGES_URL = "https://opencode.ai/zen/go/v1/messages"


class _HTTPStatusError(RuntimeError):
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code}: {body[:1000]}")


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


def _opencode_messages_url():
    url = (os.environ.get("OPENCODE_GO_BASE_URL")
           or os.environ.get("OPENCODE_BASE_URL")
           or _OPENCODE_GO_MESSAGES_URL)
    url = url.rstrip("/")
    if url.endswith("/messages"):
        return url
    return url + "/messages"


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
    usage0 = resp.get("usage") or {}
    cache_read = usage0.get("cache_read_input_tokens", 0) or 0
    cache_create = usage0.get("cache_creation_input_tokens", 0) or 0
    input_tokens = (usage0.get("input_tokens", 0) or 0) + cache_read + cache_create
    output_tokens = usage0.get("output_tokens", 0) or 0
    usage = {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "prompt_cache_hit_tokens": cache_read,
        "prompt_cache_miss_tokens": max(input_tokens - cache_read, 0),
    }
    return {
        "id": resp.get("id"),
        "model": resp.get("model"),
        "choices": [{"message": {"content": _anthropic_text(resp)}}],
        "usage": usage,
        "finish_reason": resp.get("stop_reason"),
        "stop_sequence": resp.get("stop_sequence"),
        "http_status": 200,
        "provider_response": resp,
    }


def _minimax_thinking_config(max_tokens):
    """Extended-thinking config for MiniMax-M3 via the Anthropic-compatible API.

    Enabled by env MINIMAX_THINKING (truthy). Budget comes from
    MINIMAX_THINKING_BUDGET when set; otherwise it is left large and uncapped
    relative to max_tokens (Anthropic requires 1024 <= budget < max_tokens, so we
    only reserve a small answer margin). Returns None when thinking is off."""
    flag = (os.environ.get("MINIMAX_THINKING", "") or "").strip().lower()
    if flag in ("", "0", "false", "off", "no"):
        return None
    # max_tokens=None (uncapped) -> budget scales with the provider ceiling.
    cap = max_tokens or _MINIMAX_MAX_TOKENS_CEILING
    env_budget = (os.environ.get("MINIMAX_THINKING_BUDGET", "") or "").strip()
    if env_budget.isdigit():
        budget = int(env_budget)
    else:
        budget = cap - 4096  # uncapped: give thinking almost the whole window
    budget = max(1024, min(budget, cap - 1))
    return {"type": "enabled", "budget_tokens": budget}

def _read_sse_stream(resp, raw_events=None):
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
            for k, v in (ev.get("delta") or {}).items():
                message[k] = v
            message.setdefault("usage", {}).update(ev.get("usage") or {})
        elif t == "error":
            # surface stream-level errors as a transient-classifiable HTTP error
            raise _HTTPStatusError(503, json.dumps(ev.get("error") or {}, ensure_ascii=False))
        elif t == "message_stop":
            break
    message["content"] = blocks
    return message


def _call_opencode_messages(messages, model, max_tokens, temperature, timeout, is_json):
    key = os.environ.get("OPENCODE_API_KEY") or os.environ.get("OPENCODE_GO_API_KEY")
    assert key, "Set OPENCODE_API_KEY for minimax-m3 via OpenCode Go"
    system, anthropic_messages = _messages_to_anthropic(messages)
    thinking = _minimax_thinking_config(max_tokens)
    body = {
        "model": model,
        "messages": anthropic_messages,
        # None/0 = uncapped: use the probed provider ceiling instead of an
        # artificial limit, so finish_reason=max_tokens truncation cannot fire.
        "max_tokens": max_tokens or _MINIMAX_MAX_TOKENS_CEILING,
        # Anthropic extended thinking requires temperature == 1; honor that when on.
        "temperature": 1.0 if thinking else temperature,
        "stream": True,
    }
    if thinking:
        body["thinking"] = thinking
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
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            msg = _read_sse_stream(r, raw_events=raw_events)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise _HTTPStatusError(e.code, body_text) from e
    out = _normalize_anthropic_response(msg)
    # Raw API-log side channel: the exact request body sent to the provider and
    # every SSE event received (incl. thinking). generate() lifts these into the
    # returned dict; the recorder writes them to files and strips them, so they
    # never reach committed rows or telemetry.
    out["_raw_request_body"] = body
    out["_raw_stream_events"] = raw_events
    return out



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
            self.client = OpenAI(
                api_key=openai_key,
                base_url=os.environ.get("OPENAI_BASE_URL") or None,  # 未设则自动回落 OpenAI 官方
            )
        return self.client

    def _client_for_model(self, model):
        return self._default_client()

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
    ):
        """Call the chat completions API.

        Args:
            messages: List of {"role": ..., "content": ...} dicts.
            model: Model name (aliases in model_maps are resolved automatically).
            timeout: Per-request timeout in seconds.
            max_retries: Number of retries on transient failures.
            temperature: Sampling temperature.
            is_json: If True, request JSON output mode.
            return_metadata: If True, return dict with message + usage stats.
            max_tokens: Max completion tokens.
            variables: Dict of [[KEY]] → value replacements for the prompt.
            instance: Ignored (API compat).

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

        t0 = time.time()
        last_err = None
        is_minimax = _is_minimax_model(resolved)
        # Watchdog is at least the env-configurable bound and honors a larger
        # per-call timeout, so raising MINIMAX_HARD_TIMEOUT actually extends it
        # (min() would have pinned it to the runner's shorter value).
        hard_to = max(timeout or 0, _MINIMAX_HARD_TIMEOUT) if is_minimax else None
        # Socket timeout matches the watchdog so a slow extended-thinking call is
        # not cut off before the gateway responds.
        eff_timeout = hard_to if is_minimax else timeout
        client = None if is_minimax else self._client_for_model(resolved)
        response = None
        attempt = 0
        quota_waits = 0
        transient_waits = 0
        timeout_hit = False
        last_error_type = None
        while True:
            def _do_call(_c=client):
                if is_minimax:
                    return _call_opencode_messages(
                        messages, resolved, max_tokens, temperature, eff_timeout, is_json)
                extra = dict(kwargs)
                if max_tokens is not None:
                    extra["max_completion_tokens"] = max_tokens
                return _c.chat.completions.create(
                    model=resolved,
                    messages=messages,
                    timeout=eff_timeout,
                    temperature=temperature,
                    **extra,
                )
            try:
                if hard_to is not None:
                    # Wall-clock watchdog: abandon a stalled MiniMax attempt.
                    response = _WATCHDOG_POOL.submit(_do_call).result(timeout=hard_to)
                else:
                    response = _do_call()
                break
            except concurrent.futures.TimeoutError:
                last_err = RuntimeError(f"hard wall-clock timeout after {hard_to}s")
                last_error_type = "TimeoutError"
                timeout_hit = True
                import sys as _sys
                print(f"[model_openai] watchdog: abandoned stalled {resolved} call "
                      f"after {hard_to}s (attempt {attempt + 1}/{max_retries})",
                      file=_sys.stderr, flush=True)
                # Drop the possibly-poisoned client so the retry uses a fresh
                # connection pool (the zombie read thread dies on its own).
                if is_minimax:
                    self._client_cache.pop("minimax", None)
                attempt += 1
            except Exception as e:
                last_err = e
                last_error_type = type(e).__name__
                # Endpoint availability errors are not model failures. Keep two
                # separate wait budgets so quota contention cannot hide tunnel flaps.
                msg = str(e)
                code = getattr(e, "status_code", None)
                is_quota = (code == 429 or "concurrency_limit" in msg or "429" in msg)
                is_access_denied = (code in (401, 402, 403)
                                    or "browser_signature_banned" in msg
                                    or "retryable\":false" in msg
                                    or "retryable': false" in msg)
                # 524 is a Cloudflare gateway timeout (origin did not respond in
                # time); with extended thinking a slow call can trip it. Treat it
                # (and the 520-525 origin-error family) as retryable transient,
                # not a method failure.
                is_transient = (not is_access_denied and (
                                code in (502, 503, 504, 520, 521, 522, 523, 524, 525, 530)
                                or any(s in msg for s in ("530", "502", "503", "504",
                                                          "520", "521", "522", "523", "524", "525",
                                                          "tunnel", "Cloudflare", "retryable"))))
                if is_quota and quota_waits < _MINIMAX_QUOTA_WAIT_LIMIT:
                    quota_waits += 1
                    backoff = 5 + 2 * quota_waits
                    m = re.search(r"retry_after['\"]?\s*[:=]\s*(\d+)", msg)
                    if m:
                        backoff = max(backoff, min(int(m.group(1)), 30))
                    time.sleep(min(backoff, 30))
                    continue
                if is_transient and transient_waits < _MINIMAX_TRANSIENT_WAIT_LIMIT:
                    transient_waits += 1
                    backoff = 5 + 2 * transient_waits
                    m = re.search(r"retry_after['\"]?\s*[:=]\s*(\d+)", msg)
                    if m:
                        backoff = max(backoff, min(int(m.group(1)), 30))
                    time.sleep(min(backoff, 30))
                    continue
                attempt += 1
            if attempt >= max_retries:
                raise RuntimeError(f"Failed after {max_retries} retries "
                                   f"({quota_waits} quota waits, "
                                   f"{transient_waits} transient waits): {last_err}")
            time.sleep(4)

        elapsed = time.time() - t0
        if isinstance(response, dict):
            resp = response
        else:
            resp = response.to_dict() if hasattr(response, "to_dict") else response.model_dump()
        usage = resp.get("usage", {})
        choices = resp.get("choices") or []
        if not choices or not isinstance(choices[0], dict):
            raise RuntimeError("provider response missing choices[0]")
        choice0 = choices[0]
        message0 = choice0.get("message") or {}
        if not isinstance(message0, dict) or "content" not in message0:
            raise RuntimeError("provider response missing choices[0].message.content")
        response_text = message0["content"]
        finish_reason = (choice0.get("finish_reason") or resp.get("finish_reason")
                         or resp.get("stop_reason"))
        costs = _estimate_costs(resolved, usage)
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

        return {
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
            "provider": "opencode_go_messages" if is_minimax else "openai_chat_completions",
            "base_url": _opencode_messages_url() if is_minimax else (os.environ.get("OPENAI_BASE_URL") or None),
            "temperature": temperature,
            "max_tokens": max_tokens,
            "timeout": eff_timeout,
            "max_retries": max_retries,
            "retry_count": attempt + quota_waits + transient_waits,
            "failed_attempt_count": attempt,
            "quota_wait_count": quota_waits,
            "rate_limit_wait_count": quota_waits,
            "transient_wait_count": transient_waits,
            "timeout_hit": timeout_hit,
            "last_error_type": last_error_type,
            "http_status": resp.get("http_status", 200 if is_minimax else None),
            "finish_reason": finish_reason,
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
            "output_tokens_per_second": output_tps,
            "total_tokens_per_second": total_tps,
        }

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
