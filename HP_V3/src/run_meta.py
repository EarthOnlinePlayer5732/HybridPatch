"""
Experiment-output convention enforcement (run metadata, structured logs,
per-step document snapshots).

Provides the four mechanisms the runner uses to keep a run dir self-describing:

  code_fingerprint()         12-char sha1 per key source file — distinguishes
                             code versions without git (a run dir may outlive
                             any checkout). The "which code produced this?" fix.
  append_run_metadata(...)   append one record per process invocation to
                             <out_dir>/run_metadata.jsonl (portalocker-locked),
                             capturing command / params / model / fingerprint.
                             Warns when the fingerprint differs from the dir's
                             prior runs (the "one dir, many code versions" smell).
  RunLogger                  per-(method,sample) structured log under logs/:
                             tees progress to stdout + an ANSI-free file, and
                             captures noisy domain-evaluator stdout separately.
  dump_step_docs(...)        always-on per-step output-document snapshots under
                             docs/<method>/<sample>/rt<NN>_<dir>_<state>/.

All four are ADDITIVE: they never touch the JSONL rows, checkpoints, or the
honesty-gate replay contract.
"""
import os
import io
import re
import sys
import json
import time
import hashlib
import contextlib
import uuid

import portalocker

_HERE = os.path.dirname(os.path.abspath(__file__))
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

# source files whose content identifies the executor/protocol version
_FINGERPRINT_FILES = [
    "patch_schema.py", "splitters.py", "experiment_runner.py",
    "hybrid_schema.py", "hybrid_index.py", "hybrid_prompt.py",
    "hybrid_executor.py", "hybrid_gate.py",
]

METADATA_SCHEMA = "anchorpatch.run_metadata/1"


def code_fingerprint():
    out = {}
    for f in _FINGERPRINT_FILES:
        p = os.path.join(_HERE, f)
        try:
            out[f] = hashlib.sha1(open(p, "rb").read()).hexdigest()[:12]
        except OSError:
            out[f] = None
    return out


def _strip(s):
    return _ANSI.sub("", s)


def append_jsonl_locked(path, record):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        finally:
            portalocker.unlock(f)


def write_json_atomic(path, record):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except OSError:
            pass


def _relay_row_key(row):
    return (row.get("round_trip_num"), row.get("round_trip_direction"))


def append_relay_rows_and_checkpoint(jsonl_path, ckpt_path, rows, ckpt):
    """Append one committed round trip and atomically advance its checkpoint.

    This prevents future resume duplicates: if the target JSONL already contains
    either pending row key, the caller must stop/reconcile rather than append a
    second copy. Raw rows are never edited or deduped here.
    """
    rows = list(rows or [])
    pending_keys = [_relay_row_key(r) for r in rows]
    if any(k[0] is None or k[1] not in ("forward", "backward") for k in pending_keys):
        raise RuntimeError(f"refusing relay commit with malformed row keys: {pending_keys}")
    if len(set(pending_keys)) != len(pending_keys):
        raise RuntimeError(f"refusing relay commit with duplicate pending keys: {pending_keys}")

    os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)
    with open(jsonl_path, "a+", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            existing = set()
            f.seek(0)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                key = _relay_row_key(row)
                if key[0] is not None and key[1] in ("forward", "backward"):
                    existing.add(key)

            overlap = [k for k in pending_keys if k in existing]
            if overlap:
                return {
                    "status": "already_committed",
                    "overlap_keys": overlap,
                    "pending_keys": pending_keys,
                }

            f.seek(0, os.SEEK_END)
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
            write_json_atomic(ckpt_path, ckpt)
            return {
                "status": "appended",
                "rows_appended": len(rows),
                "pending_keys": pending_keys,
            }
        finally:
            portalocker.unlock(f)


def _read_jsonl_records_with_retry(path, attempts=30, sleep_s=0.1):
    records = []
    for attempt in range(attempts):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
            return records
        except PermissionError:
            if attempt == attempts - 1:
                return []
            time.sleep(sleep_s)
    return records


def _sha256_text(text):
    if text is None:
        text = ""
    if not isinstance(text, str):
        text = str(text)
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def _content_len(text):
    if text is None:
        return 0
    if not isinstance(text, str):
        text = str(text)
    return len(text.encode("utf-8", errors="replace"))


def _exception_http_status(exc):
    code = getattr(exc, "status_code", None)
    if code is not None:
        return code
    m = re.search(r"\bHTTP\s+(\d{3})\b", str(exc))
    return int(m.group(1)) if m else None


def _is_provider_exception(exc):
    msg = str(exc)
    code = _exception_http_status(exc)
    provider_terms = (
        "timeout", "connection reset", "429", "concurrency_limit", "Cloudflare",
        "tunnel", "HTTP ", "quota", "rate", "502", "503", "504", "530",
        "missing choices", "missing choices[0].message.content",
    )
    return bool(code or any(t.lower() in msg.lower() for t in provider_terms))


def _provider_error_type(exc):
    code = _exception_http_status(exc)
    msg_l = str(exc).lower()
    if code == 429 or "concurrency_limit" in str(exc) or "429" in str(exc):
        return "rate_limit"
    if code in (401, 403) or "browser_signature_banned" in msg_l or "error 1010" in msg_l:
        return "provider_access_denied"
    if code == 402:
        return "balance_or_payment_required"
    if code and code >= 500:
        return "server_error"
    if "remote end closed" in msg_l or "connection reset" in msg_l or "disconnect" in msg_l:
        return "transport_disconnect"
    if "timeout" in msg_l:
        return "timeout"
    if "Cloudflare" in str(exc) or "tunnel" in str(exc):
        return "provider_tunnel"
    if "missing choices" in str(exc):
        return "malformed_provider_response"
    return type(exc).__name__


def _empty_classification(meta, raw):
    text = "" if raw is None else str(raw)
    if text.strip():
        return None, None
    completion_tokens = (meta or {}).get("completion_tokens")
    if completion_tokens and completion_tokens > 0:
        return "transport-valid but model-empty", "empty_message_completion_tokens_gt_0"
    return "transport-valid but model-empty", "empty_message_completion_tokens_0"


def _audit_required(record):
    classification = record.get("classification")
    error_type = record.get("error_type") or ""
    finish_reason = record.get("finish_reason")
    if classification in ("transport-valid but model-empty", "provider/API failure"):
        return True
    if finish_reason and finish_reason not in ("stop", "end_turn", "stop_sequence", "length", None):
        return True
    return error_type in {
        "empty_message_completion_tokens_gt_0",
        "empty_message_completion_tokens_0",
        "malformed_provider_response",
        "invalid_json",
        "schema_error",
        "context_mismatch",
        "validation_gate",
        "failed_step_kept_context",
        "finish_reason_length_truncated_json",
    }


class ApiCallRecorder:
    """Transparent generate() wrapper that writes provider-call telemetry.

    It does not alter prompts, generation kwargs, return values, or exceptions.
    Successful raw response text is saved under api_raw/ so repair calls are
    inspectable even when the selected row stores a different response.
    """

    def __init__(self, out_dir, method, sample_id, strategy_variant, model, generate_fn):
        self.out_dir = out_dir
        self.method = method
        self.sample_id = sample_id
        self.strategy_variant = strategy_variant
        self.model = model
        self.generate_fn = generate_fn
        self.rt_index = None
        self.direction = None
        self.target_state_id = None
        self.call_index = 0
        self.records_by_id = {}

    def set_step(self, rt_index, direction, target_state_id=None):
        self.rt_index = rt_index
        self.direction = direction
        self.target_state_id = target_state_id

    def _raw_path(self, call_id, ext="txt"):
        rt = "rt??" if self.rt_index is None else f"rt{int(self.rt_index):02d}"
        direction = self.direction or "unknown"
        d = os.path.join(self.out_dir, "api_raw", _safe(self.method), _safe(self.sample_id))
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{rt}_{direction}_{call_id}.{ext}")

    def _dump_raw_io(self, call_id, meta):
        """Write the complete raw API log for one call: the request that was
        sent and the full raw response (including thinking blocks and every SSE
        event). Returns a dict of the paths written. Best-effort — a logging
        failure never breaks the run."""
        paths = {}
        try:
            req = {
                "request_messages": meta.get("_raw_request_messages"),
                "request_body": meta.get("_raw_request_body"),
                "model": meta.get("resolved_model") or self.model,
                "provider": meta.get("provider"),
                "base_url": meta.get("base_url"),
            }
            rp = self._raw_path(call_id, "request.json")
            with open(rp, "w", encoding="utf-8", newline="") as f:
                json.dump(req, f, ensure_ascii=False, indent=1)
            paths["request"] = os.path.abspath(rp)

            events = meta.get("_raw_stream_events")
            if events:
                ep = self._raw_path(call_id, "sse.jsonl")
                with open(ep, "w", encoding="utf-8", newline="") as f:
                    for ev in events:
                        f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                paths["sse"] = os.path.abspath(ep)

            full = meta.get("_raw_response_full")
            if full is not None:
                fp = self._raw_path(call_id, "response.json")
                with open(fp, "w", encoding="utf-8", newline="") as f:
                    json.dump(full, f, ensure_ascii=False, indent=1)
                paths["response_full"] = os.path.abspath(fp)
        except Exception:
            pass
        return paths

    def _base_record(self, call_id):
        return {
            "schema": "anchorpatch.api_call/1",
            "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "sample": self.sample_id,
            "rt_index": self.rt_index,
            "direction": self.direction,
            "method": self.method,
            "strategy_variant": self.strategy_variant,
            "request_id": call_id,
            "model": self.model,
            "target_state_id": self.target_state_id,
        }

    def _write_record(self, record):
        self.records_by_id[record["request_id"]] = record
        append_jsonl_locked(os.path.join(self.out_dir, "api_calls.jsonl"), record)
        if record.get("classification"):
            append_jsonl_locked(os.path.join(self.out_dir, "api_anomalies.jsonl"), record)

    def generate(self, *args, **kwargs):
        self.call_index += 1
        call_id = f"call{self.call_index:06d}_{uuid.uuid4().hex[:8]}"
        t0 = time.time()
        try:
            out = self.generate_fn(*args, **kwargs)
        except Exception as exc:
            latency_ms = int((time.time() - t0) * 1000)
            classification = "provider/API failure" if _is_provider_exception(exc) else "runner_exception"
            record = self._base_record(call_id)
            record.update({
                "provider_request_id": None,
                "http_status": _exception_http_status(exc),
                "error_type": _provider_error_type(exc),
                "finish_reason": None,
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "raw_content_length": 0,
                "content_sha256": _sha256_text(""),
                "latency_ms": latency_ms,
                "retry_index": None,
                "retry_count": None,
                "timeout_hit": "timeout" in str(exc).lower(),
                "rate_limit_wait_count": None,
                "quota_wait_count": None,
                "transient_wait_count": None,
                "raw_response_saved_path": None,
                "runner_exception": f"{type(exc).__name__}: {exc}",
                "classification": classification,
                "subagent_audit_required": True,
                "subagent_audit_result": None,
                "rerun_recommended": classification == "provider/API failure",
                "count_as_method_failure": classification != "provider/API failure",
            })
            self._write_record(record)
            try:
                setattr(exc, "_anchorpatch_api_recorded", True)
            except Exception:
                pass
            raise

        meta = out if isinstance(out, dict) else {}
        raw = meta.get("message") if isinstance(out, dict) else str(out)
        raw_path = self._raw_path(call_id)
        with open(raw_path, "w", encoding="utf-8", newline="") as f:
            f.write(raw if isinstance(raw, str) else str(raw))
        # Complete raw API log (request + full response incl. thinking + SSE).
        raw_io_paths = self._dump_raw_io(call_id, meta)

        classification, error_type = _empty_classification(meta, raw)
        latency_ms = int((meta.get("elapsed_time") or (time.time() - t0)) * 1000)
        record = self._base_record(call_id)
        record.update({
            "provider_request_id": meta.get("provider_request_id") or meta.get("response_id"),
            "http_status": meta.get("http_status"),
            "error_type": error_type,
            "finish_reason": meta.get("finish_reason"),
            "prompt_tokens": meta.get("prompt_tokens"),
            "completion_tokens": meta.get("completion_tokens"),
            "total_tokens": meta.get("total_tokens"),
            "raw_content_length": _content_len(raw),
            "content_sha256": _sha256_text(raw),
            "latency_ms": latency_ms,
            "retry_index": None,
            "retry_count": meta.get("retry_count"),
            "timeout_hit": bool(meta.get("timeout_hit")),
            "rate_limit_wait_count": meta.get("rate_limit_wait_count"),
            "quota_wait_count": meta.get("quota_wait_count"),
            "transient_wait_count": meta.get("transient_wait_count"),
            "raw_response_saved_path": os.path.abspath(raw_path),
            "raw_request_saved_path": raw_io_paths.get("request"),
            "raw_response_full_saved_path": raw_io_paths.get("response_full"),
            "raw_sse_saved_path": raw_io_paths.get("sse"),
            "runner_exception": None,
            "classification": classification,
            "subagent_audit_required": False,
            "subagent_audit_result": None,
            "rerun_recommended": False,
            "count_as_method_failure": False,
            "base_url": meta.get("base_url"),
            "temperature": meta.get("temperature"),
            "max_tokens": meta.get("max_tokens"),
            "timeout": meta.get("timeout"),
            "max_retries": meta.get("max_retries"),
            "output_tokens_per_second": meta.get("output_tokens_per_second"),
            "total_tokens_per_second": meta.get("total_tokens_per_second"),
        })
        if classification:
            record["subagent_audit_required"] = _audit_required(record)
            record["rerun_recommended"] = True
        self._write_record(record)

        if isinstance(out, dict):
            out = dict(out)
            # The raw-io side channel has served its purpose (files written);
            # drop the heavy fields so they never propagate into committed rows
            # or downstream meta merges.
            for k in ("_raw_request_messages", "_raw_request_body",
                      "_raw_stream_events", "_raw_response_full"):
                out.pop(k, None)
            out["api_call_id"] = call_id
            out["api_call_ids"] = [call_id]
            out["api_raw_paths"] = [os.path.abspath(raw_path)]
            out["finish_reasons"] = [meta.get("finish_reason")]
        return out

    def record_runner_exception(self, exc):
        self.call_index += 1
        call_id = f"runner{self.call_index:06d}_{uuid.uuid4().hex[:8]}"
        record = self._base_record(call_id)
        record.update({
            "provider_request_id": None,
            "http_status": None,
            "error_type": type(exc).__name__,
            "finish_reason": None,
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
            "raw_content_length": 0,
            "content_sha256": _sha256_text(""),
            "latency_ms": None,
            "retry_index": None,
            "retry_count": None,
            "timeout_hit": False,
            "rate_limit_wait_count": None,
            "quota_wait_count": None,
            "transient_wait_count": None,
            "raw_response_saved_path": None,
            "runner_exception": f"{type(exc).__name__}: {exc}",
            "classification": "runner_exception",
            "subagent_audit_required": True,
            "subagent_audit_result": None,
            "rerun_recommended": False,
            "count_as_method_failure": True,
        })
        self._write_record(record)


def record_model_content_anomaly(out_dir, row):
    bd = row.get("bdpatch") or {}
    v2 = bd.get("v2") or {}
    hybrid = bd.get("hybrid") or {}
    diag = hybrid if hybrid else v2
    ev = row.get("evaluation") or {}
    reasons = []
    finish_reasons = row.get("finish_reasons") or []

    if diag.get("invalid_json"):
        reasons.append("invalid_json")
    if diag.get("schema_error_count"):
        reasons.append("schema_error")
    if diag.get("validation_gate_errors"):
        reasons.append("validation_gate")
    if diag.get("failed_step_kept_context"):
        reasons.append("failed_step_kept_context")
    if ev.get("error") == "context_mismatch":
        reasons.append("context_mismatch")
    if "length" in finish_reasons and diag.get("invalid_json"):
        reasons.append("finish_reason_length_truncated_json")
    if not reasons:
        return None

    raw = row.get("raw_llm_response") or ""
    call_ids = row.get("api_call_ids") or []
    raw_paths = row.get("api_raw_paths") or []
    record = {
        "schema": "anchorpatch.api_anomaly/1",
        "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "sample": row.get("sample_id"),
        "rt_index": row.get("round_trip_num"),
        "direction": row.get("round_trip_direction"),
        "method": row.get("method"),
        "strategy_variant": (
            v2.get("prompt_variant")
            or hybrid.get("protocol_version")
            or hybrid.get("route")
        ),
        "request_id": call_ids[-1] if call_ids else row.get("response_id"),
        "provider_request_id": row.get("provider_request_id"),
        "api_call_ids": call_ids,
        "http_status": row.get("http_status"),
        "error_type": ",".join(sorted(set(reasons))),
        "finish_reason": finish_reasons[-1] if finish_reasons else row.get("finish_reason"),
        "prompt_tokens": row.get("prompt_tokens"),
        "completion_tokens": row.get("completion_tokens"),
        "total_tokens": row.get("total_tokens"),
        "raw_content_length": _content_len(raw),
        "content_sha256": _sha256_text(raw),
        "latency_ms": int(row.get("latency") * 1000) if row.get("latency") is not None else None,
        "retry_index": None,
        "retry_count": row.get("api_retry_count"),
        "timeout_hit": row.get("api_timeout_hit"),
        "rate_limit_wait_count": row.get("api_rate_limit_wait_count"),
        "quota_wait_count": row.get("api_quota_wait_count"),
        "transient_wait_count": row.get("api_transient_wait_count"),
        "raw_response_saved_path": raw_paths[-1] if raw_paths else None,
        "runner_exception": None,
        "classification": "model-content failure",
        "subagent_audit_required": any(r in ("invalid_json", "finish_reason_length_truncated_json")
                                       for r in reasons),
        "subagent_audit_result": None,
        "rerun_recommended": False,
        "count_as_method_failure": True,
    }
    append_jsonl_locked(os.path.join(out_dir, "api_anomalies.jsonl"), record)
    return record


def append_run_metadata(out_dir, *, command, samples, methods, num_round_trips,
                        seed, model, distractor, max_tokens, notes="", printing=True,
                        context_shuffle_seeded=False,
                        context_shuffle_seed_version=None):
    """Append one invocation record to <out_dir>/run_metadata.jsonl. Returns the
    record. Warns to stderr if this dir already holds runs with a different code
    fingerprint (mixing code versions into one campaign dir)."""
    os.makedirs(out_dir, exist_ok=True)
    fp = code_fingerprint()
    rec = {
        "schema": METADATA_SCHEMA,
        "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "command": command,
        "out_dir": os.path.abspath(out_dir),
        "samples": list(samples), "methods": list(methods),
        "num_round_trips": num_round_trips, "seed": seed, "model": model,
        "distractor": bool(distractor), "max_tokens": max_tokens,
        "code_fingerprint": fp, "notes": notes,
    }
    if context_shuffle_seeded:
        rec["context_shuffle_seeded"] = True
        rec["context_shuffle_seed_version"] = (
            context_shuffle_seed_version or "global_random_seed_v1"
        )
    path = os.path.join(out_dir, "run_metadata.jsonl")
    prior = _read_jsonl_records_with_retry(path) if os.path.exists(path) else []
    if printing and prior:
        prev_fp = prior[-1].get("code_fingerprint")
        if prev_fp and prev_fp != fp:
            diff = sorted(k for k in fp if fp.get(k) != (prev_fp or {}).get(k))
            sys.stderr.write(
                f"[run_meta] WARNING: {out_dir} already holds runs under a DIFFERENT "
                f"code fingerprint (changed: {', '.join(diff)}). Mixing code versions "
                f"in one campaign dir is exactly the ambiguity this convention prevents; "
                f"prefer a new --out_dir.\n")
    with open(path, "a", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        finally:
            portalocker.unlock(f)
    return rec


def canonical_log_name(method, sample_id):
    """logs/<method>__<sample>.log — canonical, no phase/alias soup."""
    return f"{method}__{sample_id}.log"


class RunLogger:
    """Structured per-(method,sample) log. line() tees to stdout + an ANSI-free
    file; capture() redirects noisy evaluator stdout into the same file under an
    [eval] prefix so it never pollutes the progress stream."""

    def __init__(self, out_dir, method, sample_id, header=None, to_console=True):
        self.dir = os.path.join(out_dir, "logs")
        os.makedirs(self.dir, exist_ok=True)
        self.path = os.path.join(self.dir, canonical_log_name(method, sample_id))
        self.to_console = to_console
        self._fh = open(self.path, "a", encoding="utf-8")
        if header:
            self._raw(f"# {header}")

    def _raw(self, text):
        self._fh.write(_strip(text) + "\n")
        self._fh.flush()

    def line(self, text):
        if self.to_console:
            print(text, flush=True)
        self._raw(text)

    @contextlib.contextmanager
    def capture(self, tag="eval"):
        """Redirect stdout (the domain evaluator's debug prints) into the log."""
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            yield
        for ln in buf.getvalue().splitlines():
            if ln.strip():
                self._raw(f"  [{tag}] {_strip(ln)}")

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass


def _safe(name):
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(name))


_MAX_DOC_FILENAME = 80


def _safe_doc_filename(name):
    safe = _safe(name).strip("._") or "unnamed"
    if len(safe) <= _MAX_DOC_FILENAME:
        return safe
    digest = hashlib.sha256(str(name).encode("utf-8", errors="replace")).hexdigest()[:12]
    root, ext = os.path.splitext(safe)
    if len(ext) > 16:
        root, ext = safe, ""
    keep = max(12, _MAX_DOC_FILENAME - len(digest) - len(ext) - 2)
    return f"{root[:keep]}__{digest}{ext}"


def dump_step_docs(out_dir, method, sample_id, rt_num, direction, state_id,
                   gen_docs, step_info=None):
    """Always-on snapshot of the documents a step produced.

    docs/<method>/<sample>/rt<NN>_<fwd|bwd>_<state>/<filename>   (+ _step.json)
    gen_docs is the editable output {filename: content}. Additive; independent
    of JSONL / replay."""
    d = os.path.join(out_dir, "docs", _safe(method), _safe(sample_id),
                     f"rt{int(rt_num):02d}_{direction}_{_safe(state_id)}")
    os.makedirs(d, exist_ok=True)
    for fname, content in (gen_docs or {}).items():
        with open(os.path.join(d, _safe_doc_filename(fname)), "w", encoding="utf-8", newline="") as f:
            f.write(content if isinstance(content, str) else str(content))
    if step_info is not None:
        json.dump(step_info, open(os.path.join(d, "_step.json"), "w", encoding="utf-8"),
                  ensure_ascii=False, indent=2)
    return d
