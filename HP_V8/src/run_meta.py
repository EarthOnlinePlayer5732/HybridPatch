"""
Experiment-output convention enforcement (run metadata, structured logs,
per-step document snapshots).

Provides the four mechanisms the runner uses to keep a run dir self-describing:

  code_fingerprint()         12-char sha1 per key source file — distinguishes
                             code versions without git (a run dir may outlive
                             any checkout). The "which code produced this?" fix.
  append_run_metadata(...)   register one process invocation in the locked
                             <out_dir>/run_metadata.jsonl campaign ledger,
                             capturing Git identity, shared timezone-aware
                             timestamps, params, model, and code fingerprint;
                             incompatible resumes are rejected.
  finish_run_metadata(...)   close an invocation and publish one shared campaign
                             finish time after every concurrent worker stops.
  RunLogger                  per-(method,sample) structured log under logs/:
                             tees progress to stdout + an ANSI-free file, and
                             captures noisy domain-evaluator stdout separately.
  dump_step_docs(...)        always-on per-step output-document snapshots under
                             docs/<method>/<sample>/rt<NN>_<dir>_<state>/.

All mechanisms are ADDITIVE: they never touch result JSONL rows, checkpoints, or the
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
import threading
import subprocess
from contextlib import contextmanager
from datetime import datetime

import portalocker

_HERE = os.path.dirname(os.path.abspath(__file__))
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

# source files whose content identifies the executor/protocol version
_FINGERPRINT_FILES = [
    "patch_schema.py", "splitters.py", "experiment_runner.py",
    "hybrid_schema.py", "hybrid_index.py", "hybrid_prompt.py",
    "hybrid_executor.py", "hybrid_gate.py", "model_openai.py", "run_meta.py",
    "../requirements.txt",
]

METADATA_SCHEMA = "anchorpatch.run_metadata/3"
STOP_CONDITION_SCHEMA = "anchorpatch.campaign_stop_condition/1"


class CampaignStoppedError(RuntimeError):
    """Raised before a semantic call when a formal campaign is latched stopped."""

    _anchorpatch_api_recorded = True


def code_fingerprint():
    out = {}
    for f in _FINGERPRINT_FILES:
        p = os.path.join(_HERE, f)
        key = f[3:] if f.startswith("../") else f
        try:
            with open(p, "rb") as handle:
                out[key] = hashlib.sha1(handle.read()).hexdigest()[:12]
        except OSError:
            out[key] = None
    return out


def _strip(s):
    return _ANSI.sub("", s)


def append_jsonl_locked(path, record):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        finally:
            portalocker.unlock(f)


def _read_campaign_stop_unlocked(out_dir):
    path = os.path.join(out_dir, "campaign_stop.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as handle:
            record = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"invalid campaign stop latch: {path}"
        ) from exc
    if (not isinstance(record, dict)
            or record.get("schema") != STOP_CONDITION_SCHEMA
            or not isinstance(record.get("condition"), str)
            or not record.get("condition")):
        raise RuntimeError(f"invalid campaign stop latch: {path}")
    return [record]


def read_campaign_stop_conditions(out_dir):
    """Return the durable campaign-wide stop latch, failing closed on damage."""
    with _campaign_metadata_lock(out_dir):
        return _read_campaign_stop_unlocked(out_dir)


def record_campaign_stop_condition(out_dir, condition, **details):
    """Durably set the first-writer-wins campaign stop latch."""
    if not isinstance(condition, str) or not condition:
        raise ValueError("campaign stop condition must be a non-empty string")
    reserved = {
        "schema", "created_at", "condition", "worker_launch_id",
        "worker_pid",
    }
    overlap = reserved & set(details)
    if overlap:
        raise ValueError(
            f"campaign stop details override reserved fields: {sorted(overlap)}"
        )
    record = {
        "schema": STOP_CONDITION_SCHEMA,
        "created_at": _iso_with_timezone(_aware_now()),
        "condition": condition,
        "worker_launch_id": os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID"),
        "worker_pid": os.getpid(),
    }
    record.update(details)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "campaign_stop.json")
    with _campaign_metadata_lock(out_dir):
        existing = _read_campaign_stop_unlocked(out_dir)
        if existing:
            return existing[0]
        write_json_atomic(path, record)
        return record


def _raise_if_campaign_stopped(out_dir):
    records = read_campaign_stop_conditions(out_dir)
    if records:
        conditions = sorted({str(row.get("condition")) for row in records})
        raise CampaignStoppedError(
            "campaign stop latch is set: " + ", ".join(conditions)
        )


def enforce_active_worker_authorization(out_dir, sample_id):
    """Fail closed if this process is not in the dispatcher's active set."""
    worker_id = os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID")
    active_path = os.environ.get("ANCHORPATCH_ACTIVE_WORKER_SET_PATH")
    if not worker_id and not active_path:
        return
    authorized = False
    try:
        if not worker_id or not active_path:
            raise RuntimeError("active worker authorization is incomplete")
        with open(active_path, encoding="utf-8") as handle:
            active = json.load(handle)
        authorized = (
            isinstance(active, dict)
            and active.get("schema") == "anchorpatch.active_worker_set/1"
            and (active.get("workers") or {}).get(worker_id)
            == {"sample": sample_id}
        )
    except (OSError, ValueError, RuntimeError):
        authorized = False
    if not authorized:
        record_campaign_stop_condition(
            out_dir,
            "worker_authorization_drift",
            sample=sample_id,
            attempted_worker_launch_id=worker_id,
        )
        _raise_if_campaign_stopped(out_dir)


def _enforce_pre_call_campaign_guards(out_dir, sample_id):
    """Check the global latch and immutable campaign identity before a call."""
    _raise_if_campaign_stopped(out_dir)
    enforce_active_worker_authorization(out_dir, sample_id)
    expected_commit = os.environ.get("ANCHORPATCH_EXPECTED_GIT_COMMIT")
    expected_tree = os.environ.get("ANCHORPATCH_EXPECTED_GIT_TREE_STATE")
    if expected_commit or expected_tree:
        current_commit, current_tree = _git_identity()
        if ((expected_commit and current_commit != expected_commit)
                or (expected_tree and current_tree != expected_tree)):
            record_campaign_stop_condition(
                out_dir,
                "git_identity_drift",
                sample=sample_id,
                expected_commit=expected_commit,
                actual_commit=current_commit,
                expected_tree_state=expected_tree,
                actual_tree_state=current_tree,
            )
            _raise_if_campaign_stopped(out_dir)
    expected_plan = os.environ.get(
        "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256")
    plan_path = os.environ.get("ANCHORPATCH_EXPECTED_TASK_PLAN_PATH")
    if expected_plan or plan_path:
        actual_plan = None
        if plan_path and os.path.isfile(plan_path):
            with open(plan_path, "rb") as handle:
                actual_plan = hashlib.sha256(handle.read()).hexdigest()
        if (not expected_plan or not plan_path or actual_plan != expected_plan):
            record_campaign_stop_condition(
                out_dir,
                "task_plan_drift",
                sample=sample_id,
                expected_sha256=expected_plan,
                actual_sha256=actual_plan,
            )
            _raise_if_campaign_stopped(out_dir)
    # Close the identity-check/latch-check window as far as a file-based latch
    # permits. Calls already in flight may finish, but no later semantic call
    # proceeds after another worker durably sets the latch.
    _raise_if_campaign_stopped(out_dir)


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


def _write_jsonl_atomic(path, records):
    """Atomically replace a JSONL file while its separate campaign lock is held."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
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
        "missing choices", "missing choices[0].message.content", "incomplete_stream",
        "stream ended before", "OpenCode MiniMax-M3 failed",
        "peer closed connection", "incomplete chunked read",
    )
    return bool(code or any(t.lower() in msg.lower() for t in provider_terms))


def _provider_error_type(exc):
    attempts = getattr(exc, "transport_attempts", None) or []
    if attempts and attempts[-1].get("error_type"):
        return attempts[-1]["error_type"]
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
    meta = meta or {}
    if meta.get("stream_complete") is False:
        return "provider/API failure", "incomplete_stream"
    text = "" if raw is None else str(raw)
    if text.strip():
        return None, None
    response_classification = meta.get("response_classification")
    if response_classification == "thinking_budget_exhausted":
        return "transport-valid but model-empty", "thinking_budget_exhausted"
    if response_classification == "model_empty":
        return "transport-valid but model-empty", "model_empty"
    return "transport-valid but model-empty", "model_empty_unknown"


def _audit_required(record):
    classification = record.get("classification")
    error_type = record.get("error_type") or ""
    finish_reason = record.get("finish_reason")
    if classification in ("transport-valid but model-empty", "provider/API failure"):
        return True
    if finish_reason and finish_reason not in ("stop", "end_turn", "stop_sequence", None):
        return True
    return error_type in {
        "thinking_budget_exhausted",
        "model_empty",
        "model_empty_unknown",
        "incomplete_stream",
        "malformed_provider_response",
        "invalid_json",
        "schema_error",
        "context_mismatch",
        "validation_gate",
        "failed_step_kept_context",
        "finish_reason_max_tokens_truncated_json",
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
        self.worker_launch_id = (
            os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID")
            or f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        )

    def set_step(self, rt_index, direction, target_state_id=None):
        self.rt_index = rt_index
        self.direction = direction
        self.target_state_id = target_state_id

    def _raw_path(self, call_id, ext="txt"):
        rt = "rtNA" if self.rt_index is None else f"rt{int(self.rt_index):02d}"
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

    def _base_record(self, call_id, call_kind=None):
        step_id, semantic_call_id = self._semantic_ids(call_kind or "primary")
        return {
            "schema": "anchorpatch.api_call/3",
            "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "sample": self.sample_id,
            "rt_index": self.rt_index,
            "direction": self.direction,
            "method": self.method,
            "strategy_variant": self.strategy_variant,
            "request_id": call_id,
            "model": self.model,
            "call_kind": call_kind or "primary",
            "target_state_id": self.target_state_id,
            "step_id": step_id,
            "semantic_call_id": semantic_call_id,
            "worker_launch_id": self.worker_launch_id,
            "worker_pid": os.getpid(),
        }

    def _semantic_ids(self, call_kind):
        rt = "rtNA" if self.rt_index is None else f"rt{int(self.rt_index):02d}"
        direction = self.direction or "unknown"
        step_id = "/".join((
            _safe(self.method), _safe(self.sample_id), rt, _safe(direction),
        ))
        return step_id, f"{step_id}/{_safe(call_kind or 'primary')}"

    def _semantic_digest(self, semantic_call_id):
        return hashlib.sha256(semantic_call_id.encode("utf-8")).hexdigest()[:24]

    def _ledger_path(self):
        return os.path.join(self.out_dir, "api_attempt_ledger.jsonl")

    def _journal_path(self, semantic_call_id, kind="response"):
        digest = self._semantic_digest(semantic_call_id)
        return os.path.join(self.out_dir, "api_journal", f"{digest}.{kind}.json")

    def _append_ledger(self, semantic_call_id, event, **fields):
        step_id = semantic_call_id.rsplit("/", 1)[0]
        record = {
            "schema": "anchorpatch.api_attempt/3",
            "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "step_id": step_id,
            "semantic_call_id": semantic_call_id,
            "worker_launch_id": self.worker_launch_id,
            "event": event,
        }
        record.update(fields)
        append_jsonl_locked(self._ledger_path(), record)

    def _ledger_state(self, semantic_call_id):
        path = self._ledger_path()
        records = _read_jsonl_records_with_retry(path) if os.path.exists(path) else []
        records = [r for r in records if r.get("semantic_call_id") == semantic_call_id]
        response_slots = 0
        transient_failures = 0
        http_attempts = 0
        starts = set()
        budgeted = set()
        progress = set()
        ends = {}
        terminal_failure = None
        response_committed = False
        for record in records:
            idx = int(record.get("attempt_index") or 0)
            http_attempts = max(http_attempts, idx, int(record.get("http_attempts_used") or 0))
            response_slots = max(response_slots, int(record.get("response_slots_used") or 0))
            transient_failures = max(
                transient_failures, int(record.get("transient_failure_count") or 0)
            )
            event = record.get("event")
            if event == "attempt_start" and idx:
                starts.add(idx)
            elif event == "generation_progress" and idx:
                progress.add(idx)
            elif event == "attempt_end" and idx:
                ends[idx] = record
            elif event == "attempt_budget" and idx:
                budgeted.add(idx)
            elif event == "call_failed":
                terminal_failure = record
            elif event == "response_committed":
                response_committed = True

        # A process can die between the protocol event and the outer budget
        # update. Account for every such dangling POST conservatively. Once a
        # generation delta was seen it is a response slot; before that it is a
        # transient infrastructure failure.
        for idx in sorted(starts - budgeted):
            end_status = (ends.get(idx) or {}).get("status")
            if end_status == "success" or idx in progress:
                response_slots += 1
                if end_status == "success" and not response_committed:
                    terminal_failure = terminal_failure or dict(
                        ends[idx], error_type="complete_response_not_journaled"
                    )
            elif end_status == "fatal_error":
                terminal_failure = terminal_failure or ends[idx]
            else:
                transient_failures += 1

        return {
            "response_slots_used": response_slots,
            "transient_failure_count": transient_failures,
            "http_attempts_used": http_attempts,
            "terminal_failure": terminal_failure,
        }

    def _load_journal(self, semantic_call_id):
        path = self._journal_path(semantic_call_id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("semantic_call_id") != semantic_call_id:
            raise RuntimeError(f"API response journal identity mismatch: {path}")
        result = dict(payload.get("result") or {})
        result["provider_called"] = False
        result["response_replayed"] = True
        result["replayed_from_call_id"] = payload.get("call_id")
        return result

    def _save_journal(self, semantic_call_id, call_id, result):
        compact = dict(result)
        for key in ("_raw_request_messages", "_raw_request_body",
                    "_raw_stream_events", "_raw_response_full"):
            compact.pop(key, None)
        compact["provider_called"] = True
        compact["response_replayed"] = False
        payload = {
            "schema": "anchorpatch.api_response_journal/3",
            "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "semantic_call_id": semantic_call_id,
            "call_id": call_id,
            "result": compact,
        }
        write_json_atomic(self._journal_path(semantic_call_id), payload)
        self._append_ledger(
            semantic_call_id, "response_committed", call_id=call_id,
            response_slots_used=compact.get("response_slots_used"),
            transient_failure_count=compact.get("transient_failure_count"),
            http_attempts_used=compact.get("http_attempts_used"),
        )

    def _write_record(self, record):
        self.records_by_id[record["request_id"]] = record
        append_jsonl_locked(os.path.join(self.out_dir, "api_calls.jsonl"), record)
        if record.get("classification"):
            append_jsonl_locked(os.path.join(self.out_dir, "api_anomalies.jsonl"), record)

    def generate(self, *args, **kwargs):
        _enforce_pre_call_campaign_guards(self.out_dir, self.sample_id)
        self.call_index += 1
        call_id = f"call{self.call_index:06d}_{uuid.uuid4().hex[:8]}"
        kwargs = dict(kwargs)
        requested_model = (
            kwargs.get("model")
            or (args[1] if len(args) > 1 and isinstance(args[1], str) else None)
            or self.model
        )
        call_kind = kwargs.get("call_kind") or "primary"
        _step_id, semantic_call_id = self._semantic_ids(call_kind)
        provider_runtime = {}
        if str(requested_model).lower().startswith("minimax-m3"):
            try:
                from model_openai import minimax_runtime_config
                provider_runtime = minimax_runtime_config(
                    max_tokens=kwargs.get("max_tokens"),
                    thinking_mode=kwargs.get("thinking_mode") or "adaptive",
                )
            except Exception:
                provider_runtime = {}
        transport_path = None
        transport_fh = None
        transport_lock = threading.Lock()
        progress_logged = set()
        secrets = [
            value for value in (
                os.environ.get("OPENCODE_API_KEY"),
                os.environ.get("OPENCODE_GO_API_KEY"),
            ) if value
        ]
        if str(requested_model).lower().startswith("minimax-m3"):
            transport_path = self._raw_path(call_id, "transport.jsonl")
            transport_fh = open(transport_path, "w", encoding="utf-8", newline="")
            prior_sink = kwargs.get("_raw_event_sink")

            def _transport_sink(payload):
                if prior_sink is not None:
                    try:
                        prior_sink(payload)
                    except Exception:
                        pass
                record_type = payload.get("record_type")
                attempt_index = int(
                    payload.get("attempt_index")
                    or (payload.get("attempt") or {}).get("attempt_index")
                    or 0
                )
                if record_type == "attempt_start":
                    self._append_ledger(
                        semantic_call_id, "attempt_start",
                        attempt_index=attempt_index, call_id=call_id,
                    )
                elif record_type == "sdk_stream_event":
                    event = payload.get("event") or {}
                    delta_type = str((event.get("delta") or {}).get("type") or "")
                    if (delta_type in {"thinking_delta", "text_delta", "input_json_delta"}
                            and attempt_index not in progress_logged):
                        progress_logged.add(attempt_index)
                        self._append_ledger(
                            semantic_call_id, "generation_progress",
                            attempt_index=attempt_index, delta_type=delta_type,
                            call_id=call_id,
                        )
                elif record_type == "attempt_end":
                    attempt = payload.get("attempt") or {}
                    self._append_ledger(
                        semantic_call_id, "attempt_end",
                        attempt_index=attempt_index, call_id=call_id,
                        status=attempt.get("status"),
                        error_type=attempt.get("error_type"),
                        http_status=attempt.get("http_status"),
                        stream_complete=attempt.get("stream_complete"),
                        stop_reason=attempt.get("stop_reason"),
                    )
                elif record_type == "attempt_budget":
                    self._append_ledger(
                        semantic_call_id, "attempt_budget",
                        attempt_index=attempt_index, call_id=call_id,
                        budget_class=payload.get("budget_class"),
                        response_slots_used=payload.get("response_slots_used"),
                        transient_failure_count=payload.get("transient_failure_count"),
                    )
                line = json.dumps(payload, ensure_ascii=False, default=str)
                for secret in secrets:
                    line = line.replace(secret, "<redacted-key>")
                try:
                    with transport_lock:
                        transport_fh.write(line + "\n")
                        transport_fh.flush()
                except (OSError, ValueError):
                    # The ledger above is the fairness-critical state. Raw
                    # transport logging remains best-effort observability.
                    pass

            _transport_sink._anchorpatch_critical = True

            kwargs["_raw_event_sink"] = _transport_sink

            replay = self._load_journal(semantic_call_id)
            retry_state = self._ledger_state(semantic_call_id)
            prior_commit_sink = kwargs.get("_response_commit_sink")

            def _commit_response(result):
                self._save_journal(semantic_call_id, call_id, result)
                if prior_commit_sink is not None:
                    prior_commit_sink(result)

            kwargs["_retry_state"] = retry_state
            kwargs["_response_commit_sink"] = _commit_response
        else:
            replay = None
            retry_state = {}

        def _close_transport():
            if transport_fh is not None and not transport_fh.closed:
                transport_fh.close()

        t0 = time.time()
        try:
            if replay is not None:
                out = replay
            elif retry_state.get("terminal_failure"):
                prior = retry_state["terminal_failure"]
                raise RuntimeError(
                    "OpenCode MiniMax-M3 semantic call previously exhausted or "
                    f"failed fatally ({prior.get('error_type') or prior.get('status')}); "
                    "refusing a duplicate provider POST"
                )
            else:
                out = self.generate_fn(*args, **kwargs)
        except Exception as exc:
            _close_transport()
            latency_ms = int((time.time() - t0) * 1000)
            classification = "provider/API failure" if _is_provider_exception(exc) else "runner_exception"
            attempts = list(getattr(exc, "transport_attempts", None) or [])
            failure_state = (
                self._ledger_state(semantic_call_id)
                if str(requested_model).lower().startswith("minimax-m3") else retry_state
            )
            error_message = f"{type(exc).__name__}: {exc}"
            for secret in secrets:
                error_message = error_message.replace(secret, "<redacted-key>")
            record = self._base_record(call_id, call_kind)
            record.update({
                "provider_request_id": None,
                "http_status": _exception_http_status(exc),
                "error_type": _provider_error_type(exc),
                "finish_reason": None,
                "stop_reason": None,
                "stream_complete": False,
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "input_tokens": None,
                "output_tokens": None,
                "cache_read_input_tokens": None,
                "cache_creation_input_tokens": None,
                "raw_content_length": 0,
                "content_sha256": _sha256_text(""),
                "latency_ms": latency_ms,
                "retry_index": None,
                "retry_count": max(len(attempts) - 1, 0) if attempts else None,
                "failed_attempt_count": len(attempts) or None,
                "transport_attempts": attempts,
                "timeout_hit": any(a.get("error_type") == "timeout" for a in attempts)
                               or "timeout" in str(exc).lower(),
                "rate_limit_wait_count": sum(
                    a.get("error_type") == "rate_limit" for a in attempts
                ) if attempts else None,
                "quota_wait_count": sum(
                    a.get("error_type") == "rate_limit" for a in attempts
                ) if attempts else None,
                "transient_wait_count": sum(
                    a.get("status") == "retryable_error"
                    and a.get("error_type") != "rate_limit" for a in attempts
                ) if attempts else None,
                "raw_response_saved_path": None,
                "raw_sse_saved_path": (
                    os.path.abspath(transport_path)
                    if transport_path and os.path.getsize(transport_path) else None
                ),
                "runner_exception": error_message,
                "classification": classification,
                "subagent_audit_required": True,
                "subagent_audit_result": None,
                "rerun_recommended": classification == "provider/API failure",
                "count_as_method_failure": classification != "provider/API failure",
                "base_url": provider_runtime.get("base_url"),
                "request_url": provider_runtime.get("request_url"),
                "transport": provider_runtime.get("transport"),
                "transport_revision": provider_runtime.get("transport_revision"),
                "anthropic_sdk_version": provider_runtime.get("anthropic_sdk_version"),
                "max_tokens": provider_runtime.get("effective_max_tokens"),
                "thinking_mode": provider_runtime.get("thinking_mode"),
                "provider_called": False if retry_state.get("terminal_failure") else True,
                "response_replayed": False,
                "replayed_from_call_id": None,
                "max_response_slots": provider_runtime.get("max_response_slots"),
                "response_slots_used": failure_state.get("response_slots_used"),
                "max_transient_failures": provider_runtime.get("max_transient_failures"),
                "transient_failure_count": failure_state.get("transient_failure_count"),
                "http_attempts_used": failure_state.get("http_attempts_used"),
            })
            self._write_record(record)
            if (str(requested_model).lower().startswith("minimax-m3")
                    and classification == "provider/API failure"):
                persisted_state = failure_state
                self._append_ledger(
                    semantic_call_id, "call_failed", call_id=call_id,
                    status="provider_failure",
                    error_type=record.get("error_type"),
                    response_slots_used=persisted_state.get("response_slots_used"),
                    transient_failure_count=persisted_state.get("transient_failure_count"),
                    http_attempts_used=persisted_state.get("http_attempts_used"),
                )
            try:
                setattr(exc, "_anchorpatch_api_recorded", True)
            except Exception:
                pass
            raise
        _close_transport()

        meta = out if isinstance(out, dict) else {}
        raw = meta.get("message") if isinstance(out, dict) else str(out)
        raw_path = self._raw_path(call_id)
        with open(raw_path, "w", encoding="utf-8", newline="") as f:
            f.write(raw if isinstance(raw, str) else str(raw))
        # Complete raw API log (request + full response incl. thinking + SSE).
        raw_io_paths = self._dump_raw_io(call_id, meta)

        classification, error_type = _empty_classification(meta, raw)
        latency_ms = int((meta.get("elapsed_time") or (time.time() - t0)) * 1000)
        record = self._base_record(call_id, meta.get("call_kind") or call_kind)
        record.update({
            "provider_request_id": meta.get("provider_request_id") or meta.get("response_id"),
            "http_status": meta.get("http_status"),
            "error_type": error_type,
            "finish_reason": meta.get("finish_reason"),
            "stop_reason": meta.get("stop_reason") or meta.get("finish_reason"),
            "stream_complete": meta.get("stream_complete"),
            "response_classification": meta.get("response_classification"),
            "prompt_tokens": meta.get("prompt_tokens"),
            "completion_tokens": meta.get("completion_tokens"),
            "total_tokens": meta.get("total_tokens"),
            "input_tokens": meta.get("input_tokens"),
            "output_tokens": meta.get("output_tokens"),
            "cache_read_input_tokens": meta.get("cache_read_input_tokens"),
            "cache_creation_input_tokens": meta.get("cache_creation_input_tokens"),
            "raw_content_length": _content_len(raw),
            "content_sha256": _sha256_text(raw),
            "latency_ms": latency_ms,
            "retry_index": None,
            "retry_count": meta.get("retry_count"),
            "failed_attempt_count": meta.get("failed_attempt_count"),
            "transport_attempts": meta.get("transport_attempts") or [],
            "timeout_hit": bool(meta.get("timeout_hit")),
            "rate_limit_wait_count": meta.get("rate_limit_wait_count"),
            "quota_wait_count": meta.get("quota_wait_count"),
            "transient_wait_count": meta.get("transient_wait_count"),
            "raw_response_saved_path": os.path.abspath(raw_path),
            "raw_request_saved_path": raw_io_paths.get("request"),
            "raw_response_full_saved_path": raw_io_paths.get("response_full"),
            "raw_sse_saved_path": (
                os.path.abspath(transport_path)
                if transport_path and os.path.getsize(transport_path)
                else raw_io_paths.get("sse")
            ),
            "runner_exception": None,
            "classification": classification,
            "subagent_audit_required": False,
            "subagent_audit_result": None,
            "rerun_recommended": False,
            "count_as_method_failure": False,
            "base_url": meta.get("base_url"),
            "request_url": meta.get("request_url"),
            "transport": meta.get("transport"),
            "transport_revision": meta.get("transport_revision"),
            "anthropic_sdk_version": meta.get("anthropic_sdk_version"),
            "temperature": meta.get("temperature"),
            "max_tokens": meta.get("max_tokens"),
            "thinking_mode": meta.get("thinking_mode"),
            "content_block_counts": meta.get("content_block_counts") or {},
            "content_block_count": meta.get("content_block_count") or 0,
            "timeout": meta.get("timeout"),
            "max_retries": meta.get("max_retries"),
            "output_tokens_per_second": meta.get("output_tokens_per_second"),
            "total_tokens_per_second": meta.get("total_tokens_per_second"),
            "provider_called": meta.get("provider_called", True),
            "response_replayed": bool(meta.get("response_replayed")),
            "replayed_from_call_id": meta.get("replayed_from_call_id"),
            "max_response_slots": meta.get("max_response_slots"),
            "response_slots_used": meta.get("response_slots_used"),
            "max_response_retries": meta.get("max_response_retries"),
            "response_retry_used": meta.get("response_retry_used"),
            "max_transient_failures": meta.get("max_transient_failures"),
            "transient_failure_count": meta.get("transient_failure_count"),
            "http_attempts_used": meta.get("http_attempts_used"),
        })
        if classification:
            record["subagent_audit_required"] = _audit_required(record)
            record["rerun_recommended"] = False
            record["count_as_method_failure"] = True
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
    raw = row.get("raw_llm_response") or ""

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
    if ("max_tokens" in finish_reasons and diag.get("invalid_json")
            and str(raw).strip()
            and row.get("response_classification") != "thinking_budget_exhausted"):
        reasons.append("finish_reason_max_tokens_truncated_json")
    if not reasons:
        return None

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
        "subagent_audit_required": any(r in ("invalid_json", "finish_reason_max_tokens_truncated_json")
                                       for r in reasons),
        "subagent_audit_result": None,
        "rerun_recommended": False,
        "count_as_method_failure": True,
    }
    append_jsonl_locked(os.path.join(out_dir, "api_anomalies.jsonl"), record)
    return record


def _aware_now():
    return datetime.now().astimezone()


def _iso_with_timezone(value):
    return value.isoformat(timespec="seconds")


def _timezone_name(value):
    key = getattr(value.tzinfo, "key", None)
    if key:
        return key
    return value.tzname() or value.strftime("%z") or "local"


def _git_identity():
    """Return the immutable Git identity required for every V8+ campaign."""
    try:
        commit = subprocess.run(
            ["git", "-C", _HERE, "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True, encoding="utf-8",
        ).stdout.strip().lower()
        status = subprocess.run(
            ["git", "-C", _HERE, "status", "--porcelain", "--untracked-files=normal"],
            check=True, capture_output=True, text=True, encoding="utf-8",
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("cannot determine run Git identity") from exc
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RuntimeError(f"invalid Git commit identity: {commit!r}")
    return commit, ("dirty" if status.strip() else "clean")


def _read_run_metadata_strict(path):
    if not os.path.exists(path):
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except Exception as exc:
                raise RuntimeError(
                    f"invalid run metadata JSON at {path}:{lineno}"
                ) from exc
            if not isinstance(record, dict):
                raise RuntimeError(f"run metadata record at {path}:{lineno} is not an object")
            records.append(record)
    return records


@contextmanager
def _campaign_metadata_lock(out_dir):
    """Serialize campaign identity, shared timestamps, and invocation status."""
    os.makedirs(out_dir, exist_ok=True)
    lock_path = os.path.join(out_dir, ".run_metadata.lock")
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        portalocker.lock(lock_file, portalocker.LOCK_EX)
        try:
            yield os.path.join(out_dir, "run_metadata.jsonl")
        finally:
            portalocker.unlock(lock_file)


def _existing_experiment_payload(out_dir):
    for _root, _dirs, files in os.walk(out_dir):
        if any(name.endswith(".ckpt.json") for name in files):
            return True
        if "api_calls.jsonl" in files:
            return True
    return False


def _one_prior_value(records, key):
    values = {json.dumps(r.get(key), ensure_ascii=False, sort_keys=True)
              for r in records if r.get(key) is not None}
    if len(values) > 1:
        raise RuntimeError(f"run_metadata has conflicting {key} values")
    if not values:
        return None
    return json.loads(next(iter(values)))


def register_task_plan(out_dir, sample_id, plan_path, *, num_round_trips):
    """Lock one sample's exact task-plan bytes into the campaign ledger.

    Every invocation sees the same shared mapping.  A changed plan is refused
    before the next method can issue an API call, while different samples may
    register concurrently under the existing campaign metadata lock.
    """
    if not isinstance(sample_id, str) or not sample_id:
        raise ValueError("sample_id must be a non-empty string")
    with open(plan_path, "rb") as handle:
        payload = handle.read()
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"invalid task plan: {plan_path}") from exc
    sequence = decoded.get("forward_state_sequence")
    if not isinstance(sequence, list) or len(sequence) != num_round_trips:
        raise RuntimeError(
            f"task plan for {sample_id} must contain exactly "
            f"{num_round_trips} forward states"
        )
    entry = {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "round_trips": num_round_trips,
    }
    expected_sha256 = os.environ.get(
        "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256"
    )
    if expected_sha256 and entry["sha256"] != expected_sha256:
        raise RuntimeError(
            f"task-plan hash differs from dispatch manifest for {sample_id}"
        )
    with _campaign_metadata_lock(out_dir) as metadata_path:
        records = _read_run_metadata_strict(metadata_path)
        if not records:
            raise RuntimeError("task plan cannot be registered before run metadata")
        prior_manifest = _one_prior_value(records, "task_plans") or {}
        previous = prior_manifest.get(sample_id)
        if previous is not None and previous != entry:
            raise RuntimeError(
                f"refusing task-plan drift for {sample_id}: "
                f"registered {previous}, current {entry}"
            )
        manifest = dict(prior_manifest)
        manifest[sample_id] = entry
        for record in records:
            record["task_plans"] = manifest
        _write_jsonl_atomic(metadata_path, records)
    return dict(entry)


def read_run_metadata_snapshot(out_dir):
    """Read one strict metadata snapshot under its separate campaign lock.

    The writer atomically replaces ``run_metadata.jsonl`` while holding
    ``.run_metadata.lock``.  Locking the metadata file itself would keep an
    open Windows handle across ``os.replace`` and can make a live worker fail.
    """
    with _campaign_metadata_lock(out_dir) as metadata_path:
        return [dict(record) for record in _read_run_metadata_strict(metadata_path)]


def interrupt_running_invocations(out_dir, *, status, worker_launch_ids=None):
    """Close stale runner invocations after their process leases are released.

    Records are retained in place with an explicit non-success status.  The
    caller must first establish that the corresponding worker processes have
    stopped; this helper only performs the atomic ledger transition.
    """
    if status == "running" or not isinstance(status, str) or not status:
        raise ValueError("interrupt status must be a non-running string")
    selected = None if worker_launch_ids is None else set(worker_launch_ids)
    finished_now = _aware_now()
    changed = []
    with _campaign_metadata_lock(out_dir) as metadata_path:
        records = _read_run_metadata_strict(metadata_path)
        for record in records:
            if record.get("status") != "running":
                continue
            worker_launch_id = record.get("worker_launch_id")
            if selected is not None and worker_launch_id not in selected:
                continue
            record["status"] = status
            record["invocation_finished_at"] = _iso_with_timezone(finished_now)
            changed.append({
                "invocation_id": record.get("invocation_id"),
                "worker_launch_id": worker_launch_id,
                "worker_pid": record.get("worker_pid"),
                "samples": list(record.get("samples") or []),
            })
        if changed:
            active = [
                record for record in records
                if record.get("status") == "running"
            ]
            campaign_finished_at = (
                None if active else _iso_with_timezone(finished_now)
            )
            for record in records:
                record["finished_at"] = campaign_finished_at
            _write_jsonl_atomic(metadata_path, records)
    return changed


def interrupt_audited_running_invocations(out_dir, *, status, audited):
    """Atomically close exactly the stale invocations audited by the caller.

    Any new or identity-changed ``running`` record makes the transition fail
    without editing metadata. This closes the audit-to-interrupt TOCTOU window.
    """
    if status == "running" or not isinstance(status, str) or not status:
        raise ValueError("interrupt status must be a non-running string")
    audited = list(audited or [])
    expected = {}
    for item in audited:
        invocation_id = item.get("invocation_id")
        worker_id = item.get("worker_launch_id")
        worker_pid = item.get("worker_pid")
        sample = item.get("sample")
        if (not isinstance(invocation_id, str) or not invocation_id
                or invocation_id in expected
                or not isinstance(worker_id, str) or not worker_id
                or not isinstance(worker_pid, int)
                or isinstance(worker_pid, bool) or worker_pid <= 0
                or not isinstance(sample, str) or not sample):
            raise RuntimeError(
                "audited running invocation identities are invalid"
            )
        expected[invocation_id] = (worker_id, worker_pid, sample)
    if len(expected) != len(audited):
        raise RuntimeError("audited running invocation identities are invalid")
    changed = []
    with _campaign_metadata_lock(out_dir) as metadata_path:
        records = _read_run_metadata_strict(metadata_path)
        running = [record for record in records
                   if record.get("status") == "running"]
        actual_id_list = [record.get("invocation_id") for record in running]
        if (any(not isinstance(value, str) or not value
                for value in actual_id_list)
                or len(actual_id_list) != len(set(actual_id_list))):
            raise RuntimeError("running invocation identities are invalid")
        actual_ids = set(actual_id_list)
        if actual_ids != set(expected):
            raise RuntimeError(
                "running invocation set changed after provenance audit"
            )
        for record in running:
            identity = (
                record.get("worker_launch_id"), record.get("worker_pid"),
                (record.get("samples") or [None])[0]
                if len(record.get("samples") or []) == 1 else None,
            )
            if identity != expected[record.get("invocation_id")]:
                raise RuntimeError(
                    "running invocation identity changed after provenance audit"
                )
        finished_now = _aware_now()
        for record in running:
            record["status"] = status
            record["invocation_finished_at"] = _iso_with_timezone(finished_now)
            changed.append({
                "invocation_id": record.get("invocation_id"),
                "worker_launch_id": record.get("worker_launch_id"),
                "worker_pid": record.get("worker_pid"),
                "samples": list(record.get("samples") or []),
            })
        if changed:
            for record in records:
                record["finished_at"] = _iso_with_timezone(finished_now)
            _write_jsonl_atomic(metadata_path, records)
    return changed


def append_run_metadata(out_dir, *, command, samples, methods, num_round_trips,
                        seed, model, distractor, max_tokens, notes="", printing=True,
                        context_shuffle_seeded=False,
                        context_shuffle_seed_version=None,
                        stop_on_collapse=False,
                        stop_on_preservation_violation=False):
    """Register one invocation in a locked V8 campaign metadata ledger.

    The first invocation establishes the campaign Git identity and timezone-aware
    ``started_at``. Concurrent or resumed invocations reuse those exact values.
    Any commit, clean/dirty state, fingerprint, or transport mixture is rejected
    before a record is added. ``finish_run_metadata`` closes the invocation and
    gives every record one shared campaign ``finished_at`` once no invocation is
    running.
    """
    del printing  # V3 turns the old fingerprint warning into a hard refusal.
    os.makedirs(out_dir, exist_ok=True)
    fp = code_fingerprint()
    run_git_commit, git_tree_state = _git_identity()
    expected_commit = os.environ.get("ANCHORPATCH_EXPECTED_GIT_COMMIT")
    expected_tree_state = os.environ.get(
        "ANCHORPATCH_EXPECTED_GIT_TREE_STATE")
    if ((expected_commit and run_git_commit != expected_commit)
            or (expected_tree_state
                and git_tree_state != expected_tree_state)):
        raise RuntimeError(
            "runner Git identity differs from dispatch manifest"
        )
    provider_runtime = {}
    if str(model).lower().startswith("minimax-m3"):
        from model_openai import minimax_runtime_config
        provider_runtime = minimax_runtime_config(
            max_tokens=max_tokens, thinking_mode="adaptive"
        )
    invocation_now = _aware_now()
    invocation_id = uuid.uuid4().hex

    with _campaign_metadata_lock(out_dir) as path:
        if _read_campaign_stop_unlocked(out_dir):
            raise CampaignStoppedError(
                "cannot append runner metadata after campaign stop latch"
            )
        prior = _read_run_metadata_strict(path)
        if prior and any(record.get("schema") != METADATA_SCHEMA for record in prior):
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: existing run metadata is not "
                f"{METADATA_SCHEMA}; use a new --out_dir"
            )
        if not prior and _existing_experiment_payload(out_dir):
            raise RuntimeError(
                f"refusing to run in {out_dir!r}: existing experiment payload has no "
                "compatible V8 run metadata; use a new --out_dir"
            )

        identity_fields = {
            "run_git_commit": run_git_commit,
            "git_tree_state": git_tree_state,
            "code_fingerprint": fp,
        }
        for key, current in identity_fields.items():
            previous = _one_prior_value(prior, key)
            if prior and previous is None:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior {key} is unrecorded"
                )
            if previous is not None and previous != current:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior {key} differs from "
                    "the current invocation; use a new --out_dir"
                )

        if provider_runtime and prior:
            current_revision = provider_runtime["transport_revision"]
            previous_revision = _one_prior_value(prior, "transport_revision")
            if previous_revision != current_revision:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior transport revision "
                    f"is {previous_revision or 'unrecorded'}, current revision is "
                    f"{current_revision}; use a new --out_dir"
                )

        campaign_config = {
            "method_set": sorted(set(methods)),
            "num_round_trips": num_round_trips,
            "seed": seed,
            "model": model,
            "distractor": bool(distractor),
            "max_tokens": max_tokens,
            "context_shuffle_seeded": bool(context_shuffle_seeded),
            "context_shuffle_seed_version": (
                context_shuffle_seed_version if context_shuffle_seeded else None
            ),
            "stop_on_collapse": bool(stop_on_collapse),
            "stop_on_preservation_violation": bool(
                stop_on_preservation_violation
            ),
        }
        previous_config = _one_prior_value(prior, "campaign_config")
        if prior and previous_config is None:
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: prior campaign_config "
                "is unrecorded"
            )
        if previous_config is not None and previous_config != campaign_config:
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: prior campaign_config "
                "differs from the current invocation; use a new --out_dir"
            )

        campaign_started_at = _one_prior_value(prior, "started_at")
        campaign_timezone = _one_prior_value(prior, "timezone")
        if prior and (campaign_started_at is None or campaign_timezone is None):
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: prior campaign time is incomplete"
            )
        if campaign_started_at is None:
            campaign_started_at = _iso_with_timezone(invocation_now)
            campaign_timezone = _timezone_name(invocation_now)

        # Reopening or extending a campaign makes its final time unknown until
        # every currently registered invocation has closed again.
        for record in prior:
            record["finished_at"] = None

        rec = {
            "schema": METADATA_SCHEMA,
            "invocation_id": invocation_id,
            "status": "running",
            "created_local": _iso_with_timezone(invocation_now),
            "invocation_started_at": _iso_with_timezone(invocation_now),
            "invocation_finished_at": None,
            "started_at": campaign_started_at,
            "finished_at": None,
            "timezone": campaign_timezone,
            "run_git_commit": run_git_commit,
            "git_tree_state": git_tree_state,
            "command": command,
            "out_dir": os.path.abspath(out_dir),
            "samples": list(samples), "methods": list(methods),
            "num_round_trips": num_round_trips, "seed": seed, "model": model,
            "distractor": bool(distractor), "max_tokens": max_tokens,
            "code_fingerprint": fp, "notes": notes,
            "campaign_config": campaign_config,
            "task_plans": _one_prior_value(prior, "task_plans") or {},
            "worker_launch_id": os.environ.get(
                "ANCHORPATCH_WORKER_LAUNCH_ID"
            ),
            "worker_pid": os.getpid(),
        }
        rec.update(provider_runtime)
        rec["context_shuffle_seeded"] = bool(context_shuffle_seeded)
        rec["context_shuffle_seed_version"] = (
            context_shuffle_seed_version if context_shuffle_seeded else None
        )
        rec["stop_on_collapse"] = bool(stop_on_collapse)
        rec["stop_on_preservation_violation"] = bool(
            stop_on_preservation_violation
        )
        records = prior + [rec]
        _write_jsonl_atomic(path, records)
        return dict(rec)


def finish_run_metadata(out_dir, invocation_id, *, status="finished"):
    """Close one invocation and atomically set the shared campaign finish time."""
    if status == "running" or not isinstance(status, str) or not status:
        raise ValueError("finish status must be a non-running string")
    finished_now = _aware_now()
    with _campaign_metadata_lock(out_dir) as path:
        records = _read_run_metadata_strict(path)
        matches = [record for record in records
                   if record.get("invocation_id") == invocation_id]
        if len(matches) != 1:
            raise RuntimeError(
                f"cannot finish unknown or duplicate invocation {invocation_id!r}"
            )
        target = matches[0]
        if target.get("status") != "running":
            raise RuntimeError(
                f"cannot finish non-running invocation {invocation_id!r}: "
                f"status={target.get('status')!r}"
            )
        target["status"] = status
        target["invocation_finished_at"] = _iso_with_timezone(finished_now)
        active = [record for record in records if record.get("status") == "running"]
        campaign_finished_at = None if active else _iso_with_timezone(finished_now)
        for record in records:
            record["finished_at"] = campaign_finished_at
        _write_jsonl_atomic(path, records)
        return dict(target)


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
