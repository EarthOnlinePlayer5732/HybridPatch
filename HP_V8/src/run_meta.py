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
import functools
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
API_CALL_SCHEMA = "anchorpatch.api_call/4"
API_ATTEMPT_SCHEMA = "anchorpatch.api_attempt/4"
API_RESPONSE_JOURNAL_SCHEMA = "anchorpatch.api_response_journal/4"
SAMPLE_OUTCOME_SCHEMA = "anchorpatch.sample_outcome/1"
CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA = (
    "anchorpatch.campaign_recovery_authorization/1"
)
CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2 = (
    "anchorpatch.campaign_recovery_authorization/2"
)
CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME = (
    "campaign_recovery_authorization.json"
)
LEDGER_LOCK_RECOVERY_KIND = "ledger_lock_contention"
_GIT_IDENTITY_RECOVERY_CHANGED_PATHS = {
    ".gitignore",
    "HP_V8/VERSION.md",
    "HP_V8/src/paired_campaign_dispatch.py",
    "HP_V8/src/run_meta.py",
    "HP_V8/src/test_model_openai.py",
}
_LEDGER_LOCK_RECOVERY_ALLOWED_CHANGED_PATHS = {
    "HP_V8/VERSION.md",
    "HP_V8/src/authorize_ledger_lock_recovery.py",
    "HP_V8/src/paired_campaign_dispatch.py",
    "HP_V8/src/run_meta.py",
    "HP_V8/src/test_model_openai.py",
    "docs/active_log.md",
}


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
    """Append one durable JSONL row, waiting through ordinary contention.

    ``portalocker.lock`` may raise ``AlreadyLocked`` immediately on Windows.
    A shared campaign ledger is expected to have many short-lived writers, so
    that condition is contention rather than a failed model/transport call.
    ``portalocker.Lock`` retains fail-closed behavior after a bounded wait.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with portalocker.Lock(
        path,
        mode="a",
        timeout=60,
        check_interval=0.05,
        flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        encoding="utf-8",
    ) as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _canonical_record_sha256(record):
    payload = json.dumps(
        record, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
    """Return the durable campaign-wide stop latch, failing closed on damage.

    The latch writer publishes with ``os.replace`` while holding the campaign
    metadata lock.  Readers therefore see either the old complete file or the
    new complete file and do not need to join the exclusive writer lock.  This
    matters because every provider call checks the latch twice; taking the
    exclusive metadata lock for those reads can turn ordinary multi-worker
    contention into ``portalocker.AlreadyLocked`` on Windows.
    """
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


def record_sample_outcome(out_dir, sample, status, **details):
    """Append one worker/sample terminal outcome without replacing prior attempts."""
    if not isinstance(sample, str) or not sample:
        raise ValueError("sample outcome requires a non-empty sample")
    if status not in {
            "finished", "infrastructure_incomplete", "evaluator_incomplete"}:
        raise ValueError(f"unsupported sample outcome status: {status!r}")
    reserved = {
        "schema", "created_at", "sample", "status", "worker_launch_id",
        "worker_pid",
    }
    overlap = reserved & set(details)
    if overlap:
        raise ValueError(
            f"sample outcome details override reserved fields: {sorted(overlap)}"
        )
    record = {
        "schema": SAMPLE_OUTCOME_SCHEMA,
        "created_at": _iso_with_timezone(_aware_now()),
        "sample": sample,
        "status": status,
        "worker_launch_id": os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID"),
        "worker_pid": os.getpid(),
    }
    record.update(details)
    append_jsonl_locked(os.path.join(out_dir, "sample_outcomes.jsonl"), record)
    return record


def read_sample_outcomes(out_dir):
    path = os.path.join(out_dir, "sample_outcomes.jsonl")
    if not os.path.exists(path):
        return []
    records = _read_jsonl_records_with_retry(path)
    if any(record.get("schema") != SAMPLE_OUTCOME_SCHEMA for record in records):
        raise RuntimeError("invalid sample outcome ledger")
    return records


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
        current_commit, current_tree, current_status = _git_identity_details()
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
                git_status_porcelain=current_status,
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


def enforce_campaign_runtime_guards(out_dir, sample_id):
    """Public pre/post-call guard for latch, worker, Git and task-plan identity."""
    _enforce_pre_call_campaign_guards(out_dir, sample_id)


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
        deadline = time.monotonic() + 60
        while True:
            try:
                os.replace(tmp, path)
                break
            except OSError as exc:
                # Windows can briefly deny replace while a just-closed reader,
                # indexer, or scanner still owns a non-delete-sharing handle.
                # The metadata lock already serializes project writers; retry
                # only these transient sharing/access violations and retain the
                # same atomic replacement semantics.
                if (getattr(exc, "winerror", None) not in {5, 32, 33}
                        or time.monotonic() >= deadline):
                    raise
                time.sleep(0.05)
    finally:
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except OSError:
            pass


def _relay_row_key(row):
    return (row.get("round_trip_num"), row.get("round_trip_direction"))


def append_relay_rows_and_checkpoint(
        jsonl_path, ckpt_path, rows, ckpt, *, campaign_out_dir=None):
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

    # The campaign stop latch and relay commit share one ordering lock.  A
    # stop that wins the lock prevents both result rows and checkpoint; a
    # commit that wins is fully durable before the stop can be published.
    # This closes the post-provider race between sibling workers.
    guard = (
        _campaign_metadata_lock(campaign_out_dir)
        if campaign_out_dir else contextlib.nullcontext()
    )
    with guard:
        if (campaign_out_dir
                and _read_campaign_stop_unlocked(campaign_out_dir)):
            raise CampaignStoppedError(
                "campaign stop latch was set before relay commit")
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
                    if (key[0] is not None
                            and key[1] in ("forward", "backward")):
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
    for attempt in range(attempts):
        try:
            with open(path, encoding="utf-8") as f:
                portalocker.lock(f, portalocker.LOCK_SH)
                try:
                    records = []
                    for line_number, line in enumerate(
                            f.read().splitlines(), 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except ValueError as exc:
                            raise RuntimeError(
                                f"invalid JSONL record at {path}:{line_number}"
                            ) from exc
                        if not isinstance(record, dict):
                            raise RuntimeError(
                                f"non-object JSONL record at {path}:{line_number}"
                            )
                        records.append(record)
                    return records
                finally:
                    portalocker.unlock(f)
        except PermissionError:
            if attempt == attempts - 1:
                raise RuntimeError(f"cannot read locked JSONL ledger: {path}")
            time.sleep(sleep_s)
    raise RuntimeError(f"cannot read JSONL ledger: {path}")


def _sha256_text(text):
    if text is None:
        text = ""
    if not isinstance(text, str):
        text = str(text)
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


_GENERATE_POSITIONAL_PARAMETERS = (
    "messages", "model", "timeout", "max_retries", "temperature", "is_json",
    "return_metadata", "max_tokens", "variables", "instance",
    "thinking_mode", "call_kind", "_raw_event_sink",
    "max_response_retries", "max_transient_failures", "_retry_state",
    "_response_commit_sink",
)

_GENERATE_PARAMETER_DEFAULTS = {
    "model": "gpt-4o-mini",
    "timeout": 30,
    "max_retries": 3,
    "temperature": 1.0,
    "is_json": False,
    "return_metadata": False,
    "max_tokens": None,
    "variables": {},
    "instance": None,
    "thinking_mode": "adaptive",
    "call_kind": "primary",
    "max_response_retries": 1,
    "max_transient_failures": 3,
}


def _normalized_generate_arguments(args, kwargs):
    """Bind positional/keyword generate arguments without logging callbacks."""
    values = dict(_GENERATE_PARAMETER_DEFAULTS)
    for name, value in zip(_GENERATE_POSITIONAL_PARAMETERS, args):
        values[name] = value
    for name in _GENERATE_POSITIONAL_PARAMETERS:
        if name in kwargs:
            values[name] = kwargs[name]
    return values


def _semantic_request_fingerprint(args, kwargs, requested_model, call_kind):
    """Hash every request-affecting argument, excluding credentials/log sinks."""
    bound = _normalized_generate_arguments(args, kwargs)
    payload = {
        "messages": bound.get("messages"),
        "model": requested_model,
        "call_kind": call_kind,
        "timeout": bound["timeout"],
        "max_retries": bound["max_retries"],
        "temperature": bound["temperature"],
        "is_json": bool(bound["is_json"]),
        "return_metadata": bool(bound["return_metadata"]),
        "max_tokens": bound["max_tokens"],
        "variables": bound["variables"] or {},
        "instance": bound["instance"],
        "thinking_mode": bound["thinking_mode"],
        "max_response_retries": bound["max_response_retries"],
        "max_transient_failures": bound["max_transient_failures"],
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str,
    )
    return _sha256_text(canonical)


def _generation_delta_type(delta_type):
    value = str(delta_type or "")
    if value in {"thinking_delta", "text_delta", "input_json_delta"}:
        return True
    return value.endswith("_delta") and value != "signature_delta"


_ATTEMPT_EVENTS = {
    "semantic_request", "attempt_start", "generation_progress",
    "attempt_end", "attempt_budget", "call_failed", "response_committed",
}
_FINGERPRINT_EVENTS = {
    "semantic_request", "attempt_start", "call_failed",
    "response_committed",
}
_GENERATION_SEGMENT = re.compile(r"^g([0-9]{3,})$")
_TRANSPORT_MAX_RESPONSE_SLOTS = 2
_TRANSPORT_MAX_TRANSIENT_FAILURES = 3
_ATTEMPT_END_STATUSES = {"success", "retryable_error", "fatal_error"}


def _exact_nonnegative_int(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _generation_segment(generation_index):
    if not _exact_nonnegative_int(generation_index):
        raise RuntimeError("semantic generation index is invalid")
    return f"g{generation_index:03d}"


def _parse_semantic_exact_id(semantic_call_id):
    if not isinstance(semantic_call_id, str) or not semantic_call_id:
        raise RuntimeError("semantic call ID is missing")
    parts = semantic_call_id.split("/")
    if len(parts) != 6:
        raise RuntimeError("semantic call ID is not a transport-v4 exact ID")
    if (not parts[0] or not parts[1]
            or not re.fullmatch(r"rt[0-9]+", parts[2])
            or parts[3] not in {"forward", "backward"}
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", parts[4])):
        raise RuntimeError("semantic call ID cannot map to a formal relay step")
    rt_index = int(parts[2][2:])
    if parts[2] != f"rt{rt_index:02d}":
        raise RuntimeError("semantic call ID round-trip suffix is not canonical")
    match = _GENERATION_SEGMENT.match(parts[-1])
    if not match:
        raise RuntimeError("semantic call ID lacks a generation suffix")
    generation_index = int(match.group(1))
    if parts[-1] != _generation_segment(generation_index):
        raise RuntimeError("semantic call ID generation suffix is not canonical")
    semantic_root_id = "/".join(parts[:-1])
    step_id = "/".join(parts[:-2])
    return {
        "step_id": step_id,
        "rt_index": rt_index,
        "semantic_root_id": semantic_root_id,
        "semantic_call_id": semantic_call_id,
        "generation_index": generation_index,
        "call_kind": parts[-2],
    }


def _semantic_lineage_fields(semantic_call_id, parent_semantic_call_id=None):
    fields = _parse_semantic_exact_id(semantic_call_id)
    if (parent_semantic_call_id is not None
            and (not isinstance(parent_semantic_call_id, str)
                 or not parent_semantic_call_id)):
        raise RuntimeError("parent semantic call ID is invalid")
    fields["parent_semantic_call_id"] = parent_semantic_call_id
    return fields


def _validate_record_lineage(record, lineage, *, require_parent=False):
    for key in ("semantic_root_id", "generation_index"):
        if record.get(key) != lineage.get(key):
            raise RuntimeError("API attempt ledger semantic lineage is invalid")
    parent = record.get("parent_semantic_call_id")
    if parent != lineage.get("parent_semantic_call_id"):
        raise RuntimeError("API attempt ledger parent lineage is invalid")
    if require_parent and lineage.get("generation_index") > 0 and not parent:
        raise RuntimeError("API attempt ledger recovered generation lacks parent")


def _validated_transport_ledger_state(
        records, semantic_call_id=None, *, allow_open_attempt=False):
    """Validate one semantic-call event chain and derive R2/I3 from events."""
    empty_lineage = (
        _semantic_lineage_fields(semantic_call_id)
        if semantic_call_id else {
            "semantic_root_id": None,
            "semantic_call_id": semantic_call_id,
            "generation_index": 0,
            "parent_semantic_call_id": None,
        }
    )
    if not records:
        return {
            "response_slots_used": 0,
            "transient_failure_count": 0,
            "http_attempts_used": 0,
            "terminal_failure": None,
            "response_committed": False,
            "call_failed": False,
            "retry_budget_exhausted": False,
            "last_attempt_status": None,
            "last_call_id": None,
            "first_attempt_index": None,
            "open_attempt_index": None,
            "request_fingerprints": [],
            "generation_index": empty_lineage["generation_index"],
            "semantic_root_id": empty_lineage["semantic_root_id"],
            "parent_semantic_call_id": empty_lineage["parent_semantic_call_id"],
        }
    if any(record.get("event") not in _ATTEMPT_EVENTS for record in records):
        raise RuntimeError("API attempt ledger contains an unknown event")
    if any(record.get("event") == "transport_resume" for record in records):
        raise RuntimeError(
            "API attempt ledger uses retired same-ID transport_resume semantics"
        )
    semantic_requests = [
        record for record in records
        if record.get("event") == "semantic_request"
    ]
    if len(semantic_requests) != 1 or records[0] is not semantic_requests[0]:
        raise RuntimeError(
            "API attempt ledger requires one leading semantic_request"
        )
    fingerprint = semantic_requests[0].get("request_fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint:
        raise RuntimeError("API attempt ledger request fingerprint is missing")
    lineage = _semantic_lineage_fields(
        semantic_requests[0].get("semantic_call_id"),
        semantic_requests[0].get("parent_semantic_call_id"),
    )
    if semantic_call_id is not None and lineage["semantic_call_id"] != semantic_call_id:
        raise RuntimeError("API attempt ledger semantic identity mismatch")
    if semantic_requests[0].get("call_kind") != lineage["call_kind"]:
        raise RuntimeError("API attempt ledger semantic call kind is invalid")
    for record in records:
        if record.get("semantic_call_id") != lineage["semantic_call_id"]:
            raise RuntimeError("API attempt ledger mixes semantic generations")
        _validate_record_lineage(
            record, lineage,
            require_parent=(record.get("event") == "semantic_request"),
        )
        if (record.get("event") in _FINGERPRINT_EVENTS
                and record.get("request_fingerprint") != fingerprint):
            raise RuntimeError(
                "API attempt ledger request fingerprint is inconsistent"
            )

    starts = {}
    ends = {}
    budgets = {}
    progress = set()
    active_terminal = None
    response_committed = False
    last_start_index = 0
    last_call_id = None
    for position, record in enumerate(records):
        event = record["event"]
        if event == "semantic_request":
            continue
        if response_committed:
            raise RuntimeError(
                "API attempt ledger contains events after response_committed"
            )
        if active_terminal is not None:
            raise RuntimeError(
                "API attempt ledger contains events after call_failed"
            )
        if event == "attempt_start":
            attempt_index = record.get("attempt_index")
            if (not _exact_nonnegative_int(attempt_index)
                    or attempt_index <= 0
                    or attempt_index <= last_start_index
                    or attempt_index in starts
                    or not isinstance(record.get("call_id"), str)
                    or not record.get("call_id")):
                raise RuntimeError(
                    "API attempt ledger attempt_start identity is invalid"
                )
            if last_start_index:
                prior_end = (ends.get(last_start_index) or (None, {}))[1]
                if (prior_end.get("status") != "retryable_error"
                        or last_start_index not in budgets):
                    raise RuntimeError(
                        "API attempt ledger starts a new HTTP attempt before "
                        "the prior retryable attempt is fully closed"
                    )
            expected_attempt_kind = (
                ("transport_recovery_initial" if not starts
                 else "transport_recovery_retry")
                if lineage["generation_index"] > 0 else
                ("transport_initial" if not starts else "transport_retry")
            )
            if (record.get("call_kind") != lineage["call_kind"]
                    or record.get("attempt_kind") != expected_attempt_kind):
                raise RuntimeError(
                    "API attempt ledger transport attempt kind is invalid"
                )
            starts[attempt_index] = (position, record)
            last_start_index = attempt_index
            if last_call_id is None:
                last_call_id = record["call_id"]
            elif last_call_id != record["call_id"]:
                raise RuntimeError(
                    "API attempt ledger mixes call IDs within a recovery epoch"
                )
            continue
        if event in {"generation_progress", "attempt_end", "attempt_budget"}:
            attempt_index = record.get("attempt_index")
            if (not _exact_nonnegative_int(attempt_index)
                    or attempt_index <= 0 or attempt_index not in starts
                    or record.get("call_id")
                    != starts[attempt_index][1].get("call_id")):
                raise RuntimeError(
                    f"API attempt ledger {event} identity is invalid"
                )
            start_position = starts[attempt_index][0]
            if position <= start_position:
                raise RuntimeError(
                    f"API attempt ledger {event} precedes attempt_start"
                )
            if event == "generation_progress":
                if attempt_index in progress or attempt_index in ends:
                    raise RuntimeError(
                        "API attempt ledger generation_progress is duplicated/out of order"
                    )
                progress.add(attempt_index)
            elif event == "attempt_end":
                if attempt_index in ends:
                    raise RuntimeError(
                        "API attempt ledger attempt_end is duplicated"
                    )
                status = record.get("status")
                if status not in _ATTEMPT_END_STATUSES:
                    raise RuntimeError(
                        "API attempt ledger attempt_end status is invalid"
                    )
                progress_seen = attempt_index in progress
                if (not isinstance(record.get("generation_delta_seen"), bool)
                        or record.get("generation_delta_seen") != progress_seen):
                    raise RuntimeError(
                        "API attempt ledger generation progress disagrees with "
                        "attempt_end"
                    )
                if not isinstance(record.get("stream_complete"), bool):
                    raise RuntimeError(
                        "API attempt ledger stream completion state is invalid"
                    )
                if not isinstance(
                        record.get("terminal_sequence_valid"), bool):
                    raise RuntimeError(
                        "API attempt ledger terminal stream sequence is invalid"
                    )
                if status == "success":
                    if (record.get("stream_complete") is not True
                            or record.get("terminal_sequence_valid") is not True
                            or record.get("message_stop_seen") is not True
                            or record.get("final_usage_seen") is not True
                            or record.get("content_blocks_balanced") is not True
                            or not isinstance(record.get("stop_reason"), str)
                            or not record.get("stop_reason").strip()):
                        raise RuntimeError(
                            "API attempt ledger commits an incomplete successful "
                            "stream"
                        )
                elif record.get("stream_complete") is not False:
                    raise RuntimeError(
                        "API attempt ledger failed attempt is marked complete"
                    )
                ends[attempt_index] = (position, record)
            else:
                if (attempt_index in budgets or attempt_index not in ends
                        or position <= ends[attempt_index][0]):
                    raise RuntimeError(
                        "API attempt ledger attempt_budget is duplicated/out of order"
                    )
                if ends[attempt_index][1].get("status") != "retryable_error":
                    raise RuntimeError(
                        "API attempt ledger budgets a non-retryable attempt"
                    )
                budgets[attempt_index] = (position, record)
            continue
        if event in {"call_failed", "response_committed"}:
            attempt_index = record.get("attempt_index")
            if (not _exact_nonnegative_int(attempt_index)
                    or attempt_index <= 0 or attempt_index not in starts
                    or attempt_index != last_start_index
                    or attempt_index not in ends
                    or record.get("call_id") != last_call_id):
                raise RuntimeError(
                    f"API attempt ledger {event} identity is invalid"
                )
            if event == "call_failed":
                if record.get("status") != "provider_failure":
                    raise RuntimeError(
                        "API attempt ledger call_failed status is invalid"
                    )
                if (ends[attempt_index][1].get("status") == "retryable_error"
                        and attempt_index not in budgets):
                    raise RuntimeError(
                        "API attempt ledger retry exhaustion lacks a budget event"
                    )
                active_terminal = record
            else:
                if ((ends.get(attempt_index) or (None, {}))[1].get("status")
                        != "success"):
                    raise RuntimeError(
                        "API attempt ledger commits a non-successful response"
                    )
                response_committed = True

    open_attempts = set(starts) - set(ends)
    if open_attempts:
        if (not allow_open_attempt or len(open_attempts) != 1
                or open_attempts != {last_start_index}
                or records[-1].get("event") not in {
                    "attempt_start", "generation_progress"}):
            raise RuntimeError(
                "API attempt ledger contains an unclosed HTTP attempt"
            )
    for attempt_index, (_position, end) in ends.items():
        has_budget = attempt_index in budgets
        active_budget_window = bool(
            allow_open_attempt
            and attempt_index == last_start_index
            and records[-1].get("event") == "attempt_end"
            and end.get("status") == "retryable_error"
            and not has_budget
        )
        if ((end.get("status") == "retryable_error") != has_budget
                and not active_budget_window):
            raise RuntimeError(
                "API attempt ledger retryable attempt budget event is missing "
                "or misplaced"
            )

    response_slots = 0
    transient_failures = 0
    cumulative = {}
    synthetic_terminal = None
    for attempt_index in sorted(ends):
        end = (ends.get(attempt_index) or (None, {}))[1]
        status = end.get("status")
        if status == "success" or attempt_index in progress:
            response_slots += 1
        elif status == "fatal_error":
            synthetic_terminal = synthetic_terminal or end
        else:
            transient_failures += 1
        cumulative[attempt_index] = (response_slots, transient_failures)
        budget = (budgets.get(attempt_index) or (None, None))[1]
        if budget is not None:
            expected_class = (
                "response_slot"
                if status == "success" or attempt_index in progress
                else "transient_failure"
            )
            if (budget.get("budget_class") != expected_class
                    or budget.get("response_slots_used") != response_slots
                    or budget.get("transient_failure_count")
                    != transient_failures):
                raise RuntimeError(
                    "API attempt ledger budget counters disagree with events"
                )
    if (response_slots > _TRANSPORT_MAX_RESPONSE_SLOTS
            or transient_failures > _TRANSPORT_MAX_TRANSIENT_FAILURES):
        raise RuntimeError("API attempt ledger exceeds the frozen R2/I3 budget")
    for attempt_index, (used_response, used_transient) in cumulative.items():
        if (attempt_index != last_start_index
                and (used_response >= _TRANSPORT_MAX_RESPONSE_SLOTS
                     or used_transient >= _TRANSPORT_MAX_TRANSIENT_FAILURES)):
            raise RuntimeError(
                "API attempt ledger continues after the frozen R2/I3 budget "
                "was exhausted"
            )

    terminal_records = [
        record for record in records
        if record.get("event") in {"call_failed", "response_committed"}
    ]
    for record in terminal_records:
        attempt_index = record["attempt_index"]
        expected_response, expected_transient = cumulative[attempt_index]
        if (record.get("response_slots_used") != expected_response
                or record.get("transient_failure_count")
                != expected_transient
                or record.get("http_attempts_used") != attempt_index):
            raise RuntimeError(
                "API attempt ledger terminal counters disagree with events"
            )
    last_closed_index = max(ends, default=0)
    last_end = (ends.get(last_closed_index) or (None, {}))[1]
    retry_budget_exhausted = bool(
        last_end.get("status") == "retryable_error"
        and (
            response_slots == _TRANSPORT_MAX_RESPONSE_SLOTS
            or transient_failures == _TRANSPORT_MAX_TRANSIENT_FAILURES
        )
    )
    if (active_terminal is not None
            and last_end.get("status") == "retryable_error"
            and not retry_budget_exhausted):
        raise RuntimeError(
            "API attempt ledger marks provider exhaustion before R2/I3 is spent"
        )
    for attempt_index, (_position, end) in ends.items():
        if end.get("status") == "success" and not any(
                record.get("event") == "response_committed"
                and record.get("attempt_index") == attempt_index
                for record in records):
            synthetic_terminal = synthetic_terminal or dict(
                end, error_type="complete_response_not_journaled"
            )
    terminal_failure = active_terminal or synthetic_terminal
    return {
        "response_slots_used": response_slots,
        "transient_failure_count": transient_failures,
        "http_attempts_used": max(starts, default=0),
        "terminal_failure": terminal_failure,
        "response_committed": response_committed,
        "call_failed": active_terminal is not None,
        "retry_budget_exhausted": retry_budget_exhausted,
        "last_attempt_status": (
            "in_progress" if open_attempts else last_end.get("status")
        ),
        "last_call_id": last_call_id,
        "first_attempt_index": min(starts, default=None),
        "open_attempt_index": (
            next(iter(open_attempts)) if open_attempts else None
        ),
        "request_fingerprints": [fingerprint],
        "generation_index": lineage["generation_index"],
        "semantic_root_id": lineage["semantic_root_id"],
        "parent_semantic_call_id": lineage["parent_semantic_call_id"],
    }


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
    last_error = getattr(exc, "last_error", None)
    if (getattr(exc, "_anchorpatch_transport_observability_failure", False)
            or getattr(
                last_error, "_anchorpatch_transport_observability_failure",
                False)):
        return False
    attempts = list(getattr(exc, "transport_attempts", None) or [])
    if attempts and attempts[-1].get("status") == "retryable_error":
        return True
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


def _is_transport_retry_exhaustion(record, exc):
    """Whether a provider error exhausted an R2/I3 automatic retry budget."""
    last_error = getattr(exc, "last_error", None)
    if (getattr(exc, "_anchorpatch_transport_observability_failure", False)
            or getattr(
                last_error, "_anchorpatch_transport_observability_failure",
                False)):
        return False
    attempts = list(getattr(exc, "transport_attempts", None) or [])
    if not attempts or attempts[-1].get("status") != "retryable_error":
        return False
    return bool(
        record.get("response_slots_used")
        == record.get("max_response_slots") == 2
        or record.get("transient_failure_count")
        == record.get("max_transient_failures") == 3
    )


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
        self._semantic_parent_by_id = {}
        self._pending_resume_semantic_call_id = os.environ.get(
            "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID"
        )
        self._pending_resume_generation = os.environ.get(
            "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX"
        )
        self._pending_resume_fingerprint = os.environ.get(
            "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT"
        )
        self._pending_resume_next_attempt = os.environ.get(
            "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX"
        )
        self.worker_launch_id = (
            os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID")
            or f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        )
        self._authorized_attempt_incident_hashes = (
            campaign_recovery_incident_evidence(out_dir)[
                "attempt_row_hashes"
            ]
        )
        self._provider_access_retry_authorizations = (
            campaign_recovery_incident_evidence(out_dir)[
                "provider_access_retry_authorizations"
            ]
        )

    def _consume_resume_authorization(self):
        parent = self._pending_resume_semantic_call_id
        generation = self._pending_resume_generation
        fingerprint = self._pending_resume_fingerprint
        next_attempt = self._pending_resume_next_attempt
        self._pending_resume_semantic_call_id = None
        self._pending_resume_generation = None
        self._pending_resume_fingerprint = None
        self._pending_resume_next_attempt = None
        if os.environ.get(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID") == parent:
            os.environ.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID", None)
        if os.environ.get(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX") == generation:
            os.environ.pop("ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX", None)
        if os.environ.get(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT") == fingerprint:
            os.environ.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT", None)
        if os.environ.get(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX") == next_attempt:
            os.environ.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX", None)

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

    def _base_record(self, call_id, call_kind=None, semantic_context=None):
        context = semantic_context or self._semantic_context(
            call_kind or "primary"
        )
        return {
            "schema": API_CALL_SCHEMA,
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
            "step_id": context["step_id"],
            "semantic_root_id": context["semantic_root_id"],
            "semantic_call_id": context["semantic_call_id"],
            "generation_index": context["generation_index"],
            "parent_semantic_call_id": context["parent_semantic_call_id"],
            "worker_launch_id": self.worker_launch_id,
            "worker_pid": os.getpid(),
        }

    def _semantic_root(self, call_kind):
        rt = "rtNA" if self.rt_index is None else f"rt{int(self.rt_index):02d}"
        direction = self.direction or "unknown"
        step_id = "/".join((
            _safe(self.method), _safe(self.sample_id), rt, _safe(direction),
        ))
        return step_id, f"{step_id}/{_safe(call_kind or 'primary')}"

    def _semantic_context(self, call_kind, generation_index=0,
                          parent_semantic_call_id=None):
        step_id, semantic_root_id = self._semantic_root(call_kind)
        semantic_call_id = (
            f"{semantic_root_id}/{_generation_segment(generation_index)}"
        )
        context = _semantic_lineage_fields(
            semantic_call_id, parent_semantic_call_id
        )
        context["step_id"] = step_id
        self._semantic_parent_by_id[semantic_call_id] = parent_semantic_call_id
        return context

    def _semantic_ids(self, call_kind):
        context = self._semantic_context(call_kind)
        return context["step_id"], context["semantic_call_id"]

    def _semantic_digest(self, semantic_call_id):
        return hashlib.sha256(semantic_call_id.encode("utf-8")).hexdigest()[:24]

    def _ledger_path(self):
        return os.path.join(self.out_dir, "api_attempt_ledger.jsonl")

    def _journal_path(self, semantic_call_id, kind="response"):
        digest = self._semantic_digest(semantic_call_id)
        return os.path.join(self.out_dir, "api_journal", f"{digest}.{kind}.json")

    def _append_ledger(self, semantic_call_id, event, **fields):
        parent = fields.pop(
            "parent_semantic_call_id",
            self._semantic_parent_by_id.get(semantic_call_id),
        )
        lineage = _semantic_lineage_fields(
            semantic_call_id, parent_semantic_call_id=parent
        )
        record = {
            "schema": API_ATTEMPT_SCHEMA,
            "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "step_id": lineage["step_id"],
            "semantic_root_id": lineage["semantic_root_id"],
            "semantic_call_id": semantic_call_id,
            "generation_index": lineage["generation_index"],
            "parent_semantic_call_id": lineage["parent_semantic_call_id"],
            "worker_launch_id": self.worker_launch_id,
            "event": event,
        }
        record.update(fields)
        append_jsonl_locked(self._ledger_path(), record)

    def _ledger_state(self, semantic_call_id):
        records = [
            record for record in self._all_ledger_records()
            if record.get("semantic_call_id") == semantic_call_id
        ]
        return _validated_transport_ledger_state(
            records, semantic_call_id=semantic_call_id
        )

    def _all_ledger_records(self):
        path = self._ledger_path()
        records = (
            _read_jsonl_records_with_retry(path)
            if os.path.exists(path) else []
        )
        if self._authorized_attempt_incident_hashes:
            records = [
                record for record in records
                if _canonical_record_sha256(record)
                not in self._authorized_attempt_incident_hashes
            ]
        if any(record.get("schema") != API_ATTEMPT_SCHEMA
               for record in records):
            raise RuntimeError(
                "API attempt ledger schema differs from transport-v4; "
                "use a new experiment directory"
            )
        for record in records:
            if (not isinstance(record.get("semantic_call_id"), str)
                    or not record.get("semantic_call_id")
                    or record.get("event") not in _ATTEMPT_EVENTS):
                raise RuntimeError(
                    "API attempt ledger contains an unknown event or retired "
                    "same-ID transport record"
                )
            lineage = _semantic_lineage_fields(
                record["semantic_call_id"],
                record.get("parent_semantic_call_id"),
            )
            if (record.get("semantic_root_id") != lineage["semantic_root_id"]
                    or record.get("generation_index")
                    != lineage["generation_index"]
                    or record.get("step_id") != lineage["step_id"]):
                raise RuntimeError(
                    "API attempt ledger semantic lineage is invalid"
                )
        return records

    def _lineage_global_http_attempts_used(self, semantic_root_id):
        attempts = []
        for record in self._all_ledger_records():
            if record.get("semantic_root_id") != semantic_root_id:
                continue
            attempt_index = record.get("attempt_index")
            if _exact_nonnegative_int(attempt_index):
                attempts.append(attempt_index)
        return max(attempts, default=0)

    def _validated_semantic_lineage(self, semantic_root_id):
        """Validate contiguous generations, parents, fingerprints and attempts."""
        groups = {}
        root_records = [
            record for record in self._all_ledger_records()
            if record.get("semantic_root_id") == semantic_root_id
        ]
        prior_generation = None
        prior_record = None
        for record in root_records:
            context = _semantic_lineage_fields(
                record["semantic_call_id"],
                record.get("parent_semantic_call_id"),
            )
            generation = context["generation_index"]
            if prior_generation is None:
                if generation != 0:
                    raise RuntimeError(
                        "semantic lineage does not begin at generation zero"
                    )
            elif generation < prior_generation or generation > prior_generation + 1:
                raise RuntimeError(
                    "semantic lineage generation order is invalid"
                )
            elif generation == prior_generation + 1:
                if (record.get("event") != "semantic_request"
                        or (prior_record or {}).get("event") != "call_failed"):
                    raise RuntimeError(
                        "semantic lineage advances before the exhausted parent "
                        "is terminally recorded"
                    )
            groups.setdefault(record["semantic_call_id"], []).append(record)
            prior_generation = generation
            prior_record = record
        if not groups:
            return []
        generations = {}
        all_attempt_indexes = []
        lineage_fingerprint = None
        committed_seen = False
        for semantic_call_id, records in groups.items():
            context = _semantic_lineage_fields(
                semantic_call_id, records[0].get("parent_semantic_call_id")
            )
            generation = context["generation_index"]
            if generation in generations:
                raise RuntimeError(
                    "semantic lineage contains a duplicate generation"
                )
            state = _validated_transport_ledger_state(
                records, semantic_call_id=semantic_call_id
            )
            fingerprint = (state.get("request_fingerprints") or [None])[0]
            if lineage_fingerprint is None:
                lineage_fingerprint = fingerprint
            elif fingerprint != lineage_fingerprint:
                raise RuntimeError(
                    "semantic lineage request fingerprint is inconsistent"
                )
            attempt_indexes = [
                record["attempt_index"] for record in records
                if record.get("event") == "attempt_start"
            ]
            all_attempt_indexes.extend(attempt_indexes)
            committed = any(
                record.get("event") == "response_committed"
                for record in records
            )
            generations[generation] = {
                "context": context,
                "state": state,
                "committed": committed,
            }
        ordered_indexes = sorted(generations)
        if ordered_indexes != list(range(len(ordered_indexes))):
            raise RuntimeError("semantic lineage generation sequence has a gap")
        if (len(all_attempt_indexes) != len(set(all_attempt_indexes))
                or all_attempt_indexes != sorted(all_attempt_indexes)):
            raise RuntimeError(
                "semantic lineage attempt indexes are duplicated/out of order"
            )
        ordered = []
        for generation in ordered_indexes:
            item = generations[generation]
            expected_parent = (
                None if generation == 0
                else generations[generation - 1]["context"]["semantic_call_id"]
            )
            if item["context"]["parent_semantic_call_id"] != expected_parent:
                raise RuntimeError("semantic lineage parent chain is invalid")
            if committed_seen:
                raise RuntimeError(
                    "semantic lineage continues after a committed response"
                )
            if item["committed"]:
                committed_seen = True
            elif generation < ordered_indexes[-1]:
                state = item["state"]
                terminal = state.get("terminal_failure") or {}
                semantic_call_id = item["context"]["semantic_call_id"]
                provider_access_authorized = any(
                    authorization.get("parent_semantic_call_id")
                    == semantic_call_id
                    for authorization in
                    self._provider_access_retry_authorizations.values()
                )
                ordinary_exhaustion = (
                    terminal.get("status") == "provider_failure"
                    and state.get("call_failed")
                    and state.get("last_attempt_status")
                    == "retryable_error"
                    and state.get("retry_budget_exhausted")
                )
                authorized_access_denial = (
                    provider_access_authorized
                    and terminal.get("status") == "provider_failure"
                    and terminal.get("error_type")
                    == "provider_access_denied"
                    and state.get("call_failed")
                    and state.get("last_attempt_status") == "fatal_error"
                    and isinstance(state.get("response_slots_used"), int)
                    and 0 <= state.get("response_slots_used") < 2
                    and state.get("transient_failure_count") == 0
                )
                if not (ordinary_exhaustion or authorized_access_denial):
                    raise RuntimeError(
                        "semantic lineage advances from a non-exhausted parent"
                    )
            ordered.append(item)
        return ordered

    def _find_lineage_journal(self, semantic_root_id, request_fingerprint):
        journal_dir = os.path.join(self.out_dir, "api_journal")
        if not os.path.isdir(journal_dir):
            return None, None
        matches = []
        for name in sorted(os.listdir(journal_dir)):
            if not name.endswith(".response.json"):
                continue
            path = os.path.join(journal_dir, name)
            with open(path, encoding="utf-8") as handle:
                payload = json.load(handle)
            if payload.get("schema") != API_RESPONSE_JOURNAL_SCHEMA:
                raise RuntimeError(
                    f"API response journal schema differs from transport-v4: {path}"
                )
            payload_semantic_id = payload.get("semantic_call_id")
            lineage = _semantic_lineage_fields(
                payload_semantic_id,
                payload.get("parent_semantic_call_id"),
            )
            expected_name = (
                f"{self._semantic_digest(payload_semantic_id)}.response.json"
            )
            if name != expected_name:
                raise RuntimeError(
                    f"API response journal filename digest mismatch: {path}"
                )
            if payload.get("semantic_root_id") != lineage["semantic_root_id"]:
                raise RuntimeError(
                    f"API response journal semantic root mismatch: {path}"
                )
            if payload.get("generation_index") != lineage["generation_index"]:
                raise RuntimeError(
                    f"API response journal generation mismatch: {path}"
                )
            result = payload.get("result")
            if (not isinstance(payload.get("call_id"), str)
                    or not payload.get("call_id")
                    or not isinstance(result, dict)
                    or result.get("semantic_root_id")
                    != lineage["semantic_root_id"]
                    or result.get("semantic_call_id")
                    != lineage["semantic_call_id"]
                    or result.get("generation_index")
                    != lineage["generation_index"]
                    or result.get("parent_semantic_call_id")
                    != lineage["parent_semantic_call_id"]
                    or result.get("transport_revision")
                    != "opencode_anthropic_sdk/4"
                    or result.get("transport_resume_policy")
                    != "exact_payload_new_semantic_call/1"
                    or result.get("call_kind") != lineage["call_kind"]
                    or result.get("stream_complete") is not True
                    or not isinstance(result.get("stop_reason"), str)
                    or not result.get("stop_reason").strip()
                    or not isinstance(result.get("message"), str)
                    or result.get("input_tokens") is None
                    or result.get("output_tokens") is None
                    or result.get("max_response_slots")
                    != _TRANSPORT_MAX_RESPONSE_SLOTS
                    or result.get("max_transient_failures")
                    != _TRANSPORT_MAX_TRANSIENT_FAILURES
                    or not isinstance(result.get("response_slots_used"), int)
                    or not (1 <= result.get("response_slots_used")
                            <= _TRANSPORT_MAX_RESPONSE_SLOTS)
                    or not isinstance(
                        result.get("transient_failure_count"), int)
                    or not (0 <= result.get("transient_failure_count")
                            <= _TRANSPORT_MAX_TRANSIENT_FAILURES)
                    or not isinstance(result.get("http_attempts_used"), int)
                    or result.get("http_attempts_used") <= 0):
                raise RuntimeError(
                    f"API response journal result is incomplete or invalid: {path}"
                )
            if payload.get("semantic_root_id") != semantic_root_id:
                continue
            if payload.get("request_fingerprint") != request_fingerprint:
                raise RuntimeError(
                    "API response journal request fingerprint differs within "
                    "one semantic lineage"
                )
            matches.append((path, payload, lineage))
        if len(matches) > 1:
            raise RuntimeError(
                "API response journal contains multiple committed generations "
                "for one semantic call"
            )
        if not matches:
            return None, None
        path, payload, lineage = matches[0]
        result = dict(payload.get("result") or {})
        result["provider_called"] = False
        result["response_replayed"] = True
        result["replayed_from_call_id"] = payload.get("call_id")
        result["semantic_call_id"] = lineage["semantic_call_id"]
        result["semantic_root_id"] = lineage["semantic_root_id"]
        result["generation_index"] = lineage["generation_index"]
        result["parent_semantic_call_id"] = lineage["parent_semantic_call_id"]
        return result, lineage

    def _load_journal(self, semantic_call_id, request_fingerprint=None):
        path = self._journal_path(semantic_call_id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("schema") != API_RESPONSE_JOURNAL_SCHEMA:
            raise RuntimeError(
                f"API response journal schema differs from transport-v4: {path}"
            )
        if payload.get("semantic_call_id") != semantic_call_id:
            raise RuntimeError(f"API response journal identity mismatch: {path}")
        lineage = _semantic_lineage_fields(
            semantic_call_id, payload.get("parent_semantic_call_id")
        )
        if (payload.get("semantic_root_id") != lineage["semantic_root_id"]
                or payload.get("generation_index")
                != lineage["generation_index"]):
            raise RuntimeError(f"API response journal lineage mismatch: {path}")
        stored_fingerprint = payload.get("request_fingerprint")
        if (not isinstance(stored_fingerprint, str) or not stored_fingerprint
                or request_fingerprint is None
                or stored_fingerprint != request_fingerprint):
            raise RuntimeError(
                f"API response journal request fingerprint mismatch: {path}"
            )
        result = dict(payload.get("result") or {})
        result["provider_called"] = False
        result["response_replayed"] = True
        result["replayed_from_call_id"] = payload.get("call_id")
        result["semantic_call_id"] = lineage["semantic_call_id"]
        result["semantic_root_id"] = lineage["semantic_root_id"]
        result["generation_index"] = lineage["generation_index"]
        result["parent_semantic_call_id"] = lineage["parent_semantic_call_id"]
        return result

    def _save_journal(self, semantic_call_id, call_id, result,
                      request_fingerprint=None):
        parent_semantic_call_id = self._semantic_parent_by_id.get(
            semantic_call_id
        )
        lineage = _semantic_lineage_fields(
            semantic_call_id, parent_semantic_call_id
        )
        compact = dict(result)
        for key in ("_raw_request_messages", "_raw_request_body",
                    "_raw_stream_events", "_raw_response_full"):
            compact.pop(key, None)
        compact["provider_called"] = True
        compact["response_replayed"] = False
        compact["semantic_root_id"] = lineage["semantic_root_id"]
        compact["semantic_call_id"] = lineage["semantic_call_id"]
        compact["generation_index"] = lineage["generation_index"]
        compact["parent_semantic_call_id"] = lineage["parent_semantic_call_id"]
        payload = {
            "schema": API_RESPONSE_JOURNAL_SCHEMA,
            "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "semantic_root_id": lineage["semantic_root_id"],
            "semantic_call_id": semantic_call_id,
            "generation_index": lineage["generation_index"],
            "parent_semantic_call_id": lineage["parent_semantic_call_id"],
            "call_id": call_id,
            "request_fingerprint": request_fingerprint,
            "result": compact,
        }
        write_json_atomic(self._journal_path(semantic_call_id), payload)
        self._append_ledger(
            semantic_call_id, "response_committed", call_id=call_id,
            response_slots_used=compact.get("response_slots_used"),
            transient_failure_count=compact.get("transient_failure_count"),
            http_attempts_used=compact.get("http_attempts_used"),
            attempt_index=compact.get("http_attempts_used"),
            request_fingerprint=request_fingerprint,
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
        bound_arguments = _normalized_generate_arguments(args, kwargs)
        requested_model = bound_arguments.get("model") or self.model
        call_kind = bound_arguments.get("call_kind") or "primary"
        semantic_context = self._semantic_context(call_kind)
        semantic_call_id = semantic_context["semantic_call_id"]
        semantic_root_id = semantic_context["semantic_root_id"]
        request_fingerprint = _semantic_request_fingerprint(
            args, kwargs, requested_model, call_kind
        )
        provider_runtime = {}
        if str(requested_model).lower().startswith("minimax-m3"):
            try:
                from model_openai import minimax_runtime_config
                provider_runtime = minimax_runtime_config(
                    max_tokens=kwargs.get("max_tokens"),
                    thinking_mode=kwargs.get("thinking_mode") or "adaptive",
                )
            except Exception as exc:
                raise RuntimeError(
                    "failed to resolve the frozen MiniMax transport runtime "
                    "before provider POST"
                ) from exc
        transport_path = None
        transport_fh = None
        semantic_lock_fh = None
        transport_lock = threading.Lock()
        progress_logged = set()
        provider_post_started = False
        transport_preflight_error = None
        secrets = [
            value for value in (
                os.environ.get("OPENCODE_API_KEY"),
                os.environ.get("OPENCODE_GO_API_KEY"),
            ) if value
        ]

        def _release_semantic_lock():
            nonlocal semantic_lock_fh
            if semantic_lock_fh is None:
                return
            try:
                portalocker.unlock(semantic_lock_fh)
            finally:
                semantic_lock_fh.close()
                semantic_lock_fh = None

        if str(requested_model).lower().startswith("minimax-m3"):
            semantic_lock_dir = os.path.join(
                self.out_dir, "api_semantic_locks")
            os.makedirs(semantic_lock_dir, exist_ok=True)
            semantic_lock_path = os.path.join(
                semantic_lock_dir,
                f"{self._semantic_digest(semantic_root_id)}.lock",
            )
            semantic_lock_fh = open(
                semantic_lock_path, "a+", encoding="utf-8")
            try:
                portalocker.lock(
                    semantic_lock_fh,
                    portalocker.LOCK_EX | portalocker.LOCK_NB,
                )
            except Exception as exc:
                semantic_lock_fh.close()
                semantic_lock_fh = None
                raise RuntimeError(
                    "semantic call already has an active provider owner; "
                    "refusing a duplicate POST"
                ) from exc
            transport_path = self._raw_path(call_id, "transport.jsonl")
            try:
                transport_fh = open(
                    transport_path, "w", encoding="utf-8", newline="")
            except Exception:
                _release_semantic_lock()
                raise
            prior_sink = kwargs.get("_raw_event_sink")

            def _transport_sink(payload):
                nonlocal provider_post_started
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
                    # ``attempt_start`` is emitted immediately before each
                    # Anthropic SDK stream open.  Recheck the durable global
                    # guards for every HTTP attempt, including automatic
                    # retries inside one exact semantic call.
                    enforce_campaign_runtime_guards(
                        self.out_dir, self.sample_id)
                    provider_post_started = True
                    self._append_ledger(
                        semantic_call_id, "attempt_start",
                        attempt_index=attempt_index, call_id=call_id,
                        call_kind=call_kind,
                        attempt_kind=payload.get("attempt_kind"),
                        request_fingerprint=request_fingerprint,
                    )
                elif record_type == "sdk_stream_event":
                    event = payload.get("event") or {}
                    delta_type = str((event.get("delta") or {}).get("type") or "")
                    if (_generation_delta_type(delta_type)
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
                        message_stop_seen=attempt.get("message_stop_seen"),
                        final_usage_seen=attempt.get("final_usage_seen"),
                        generation_delta_seen=attempt.get("generation_delta_seen"),
                        content_blocks_started=attempt.get("content_blocks_started"),
                        content_blocks_stopped=attempt.get("content_blocks_stopped"),
                        content_blocks_balanced=attempt.get("content_blocks_balanced"),
                        terminal_sequence_valid=attempt.get(
                            "terminal_sequence_valid"),
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

            def _preflight_guard(function, *call_args, **call_kwargs):
                try:
                    return function(*call_args, **call_kwargs)
                except BaseException:
                    if transport_fh is not None and not transport_fh.closed:
                        transport_fh.close()
                    _release_semantic_lock()
                    raise

            resume_semantic_id = self._pending_resume_semantic_call_id
            resume_index_raw = self._pending_resume_generation
            resume_fingerprint = self._pending_resume_fingerprint
            resume_next_attempt_raw = self._pending_resume_next_attempt
            resume_values = (
                resume_semantic_id, resume_index_raw, resume_fingerprint,
                resume_next_attempt_raw,
            )
            if any(value is None for value in resume_values) and any(
                    value is not None for value in resume_values):
                transport_preflight_error = RuntimeError(
                    "infrastructure resume authorization is incomplete"
                )
            lineage_state = []
            try:
                lineage_state = _preflight_guard(
                    self._validated_semantic_lineage, semantic_root_id
                )
                replay, replay_lineage = _preflight_guard(
                    self._find_lineage_journal, semantic_root_id,
                    request_fingerprint,
                )
            except Exception as exc:
                replay = None
                replay_lineage = None
                transport_preflight_error = transport_preflight_error or exc
            if replay_lineage is not None:
                replay_state = {}
                lineage_match = next((
                    item for item in lineage_state
                    if item["context"]["semantic_call_id"]
                    == replay_lineage["semantic_call_id"]
                ), None)
                if lineage_match is None:
                    transport_preflight_error = (
                        transport_preflight_error or RuntimeError(
                            "API response journal has no matching attempt-ledger "
                            "generation"
                        )
                    )
                else:
                    replay_state = lineage_match["state"]
                    if (replay_state.get("last_attempt_status") != "success"
                            or replay_state.get("last_call_id")
                            != replay.get("replayed_from_call_id")
                            or replay_state.get("response_slots_used")
                            != replay.get("response_slots_used")
                            or replay_state.get("transient_failure_count")
                            != replay.get("transient_failure_count")
                            or replay_state.get("http_attempts_used")
                            != replay.get("http_attempts_used")):
                        transport_preflight_error = (
                            transport_preflight_error or RuntimeError(
                                "API response journal disagrees with its "
                                "successful attempt-ledger generation"
                            )
                        )
                semantic_context = dict(replay_lineage)
                semantic_call_id = semantic_context["semantic_call_id"]
                self._semantic_parent_by_id[semantic_call_id] = (
                    semantic_context.get("parent_semantic_call_id")
                )
                if resume_semantic_id is not None:
                    try:
                        resume_parent = _semantic_lineage_fields(
                            resume_semantic_id
                        )
                        resume_generation = int(resume_index_raw or "")
                        resume_next_attempt = int(
                            resume_next_attempt_raw or "")
                    except (RuntimeError, TypeError, ValueError):
                        transport_preflight_error = (
                            transport_preflight_error or RuntimeError(
                                "infrastructure resume authorization is invalid"
                            )
                        )
                    else:
                        if (resume_parent["semantic_root_id"]
                                == replay_lineage["semantic_root_id"]):
                            if (replay_lineage["generation_index"]
                                    != resume_generation
                                    or replay_lineage[
                                        "parent_semantic_call_id"]
                                    != resume_semantic_id):
                                transport_preflight_error = (
                                    transport_preflight_error or RuntimeError(
                                        "response journal conflicts with the "
                                        "authorized recovery generation"
                                    )
                                )
                            elif (resume_fingerprint != request_fingerprint
                                  or resume_next_attempt
                                  != replay_state.get("first_attempt_index")):
                                transport_preflight_error = (
                                    transport_preflight_error or RuntimeError(
                                        "response journal conflicts with the "
                                        "authorized recovery fingerprint or "
                                        "attempt index"
                                    )
                                )
                            else:
                                self._consume_resume_authorization()
            if replay is None:
                if (resume_semantic_id is None) != (resume_index_raw is None):
                    transport_preflight_error = transport_preflight_error or RuntimeError(
                        "infrastructure resume authorization is incomplete"
                    )
                elif resume_semantic_id is not None:
                    try:
                        parent_lineage = _semantic_lineage_fields(
                            resume_semantic_id
                        )
                        authorized_generation = int(resume_index_raw or "")
                        authorized_next_attempt = int(
                            resume_next_attempt_raw or "")
                    except (RuntimeError, TypeError, ValueError):
                        transport_preflight_error = transport_preflight_error or RuntimeError(
                            "infrastructure resume authorization is invalid"
                        )
                    else:
                        if parent_lineage["semantic_root_id"] != semantic_root_id:
                            transport_preflight_error = transport_preflight_error or RuntimeError(
                                "infrastructure resume parent does not belong "
                                "to the current semantic root"
                            )
                        elif authorized_generation != (
                                parent_lineage["generation_index"] + 1):
                            transport_preflight_error = transport_preflight_error or RuntimeError(
                                "infrastructure resume generation index must "
                                "be exactly one after the parent generation"
                            )
                        elif resume_fingerprint != request_fingerprint:
                            transport_preflight_error = transport_preflight_error or RuntimeError(
                                "infrastructure resume request fingerprint "
                                "changed before provider POST"
                            )
                        elif (not lineage_state
                              or lineage_state[-1]["context"][
                                  "semantic_call_id"] != resume_semantic_id):
                            transport_preflight_error = transport_preflight_error or RuntimeError(
                                "infrastructure resume parent is not the latest "
                                "semantic generation"
                            )
                        else:
                            parent_state = _preflight_guard(
                                self._ledger_state, resume_semantic_id)
                            parent_fingerprints = (
                                parent_state.get("request_fingerprints") or []
                            )
                            parent_failure = parent_state.get(
                                "terminal_failure")
                            provider_access_authorization = next((
                                item for item in
                                self._provider_access_retry_authorizations.values()
                                if item.get("parent_semantic_call_id")
                                == resume_semantic_id
                            ), None)
                            provider_access_resume = bool(
                                provider_access_authorization
                                and provider_access_authorization.get(
                                    "semantic_call_id")
                                == f"{semantic_root_id}/g{authorized_generation:03d}"
                                and provider_access_authorization.get(
                                    "request_fingerprint")
                                == request_fingerprint
                                and provider_access_authorization.get(
                                    "next_attempt_index")
                                == authorized_next_attempt
                                and parent_failure
                                and parent_failure.get("status")
                                == "provider_failure"
                                and parent_failure.get("error_type")
                                == "provider_access_denied"
                                and parent_state.get("call_failed")
                                and parent_state.get("last_attempt_status")
                                == "fatal_error"
                                and isinstance(parent_state.get(
                                    "response_slots_used"), int)
                                and 0 <= parent_state.get(
                                    "response_slots_used") < 2
                                and parent_state.get(
                                    "transient_failure_count") == 0
                            )
                            if parent_fingerprints != [request_fingerprint]:
                                transport_preflight_error = transport_preflight_error or RuntimeError(
                                    "infrastructure resume parent prompt or "
                                    "generation parameters changed"
                                )
                            elif not parent_failure:
                                transport_preflight_error = transport_preflight_error or RuntimeError(
                                    "infrastructure resume parent has not failed"
                                )
                            elif parent_failure.get("status") != "provider_failure":
                                transport_preflight_error = transport_preflight_error or RuntimeError(
                                    "only a provider/API infrastructure failure "
                                    "is resumable"
                                )
                            elif (not provider_access_resume
                                  and (not parent_state.get("call_failed")
                                       or parent_state.get(
                                           "last_attempt_status")
                                       != "retryable_error"
                                       or not parent_state.get(
                                           "retry_budget_exhausted"))):
                                transport_preflight_error = transport_preflight_error or RuntimeError(
                                    "infrastructure resume requires an exhausted "
                                    "transport budget"
                                )
                            elif authorized_next_attempt != (
                                    _preflight_guard(
                                        self._lineage_global_http_attempts_used,
                                        semantic_root_id) + 1):
                                transport_preflight_error = transport_preflight_error or RuntimeError(
                                    "infrastructure resume next HTTP attempt "
                                    "does not match the audited lineage"
                                )
                            else:
                                semantic_context = self._semantic_context(
                                    call_kind, authorized_generation,
                                    parent_semantic_call_id=resume_semantic_id,
                                )
                                semantic_call_id = (
                                    semantic_context["semantic_call_id"]
                                )
                                semantic_root_id = (
                                    semantic_context["semantic_root_id"]
                                )
                                self._consume_resume_authorization()
                retry_state = _preflight_guard(
                    self._ledger_state, semantic_call_id)
                fingerprints = retry_state.get("request_fingerprints") or []
                if len(fingerprints) > 1:
                    transport_preflight_error = transport_preflight_error or RuntimeError(
                        "semantic-call request fingerprint history is inconsistent"
                    )
                elif fingerprints and fingerprints[0] != request_fingerprint:
                    transport_preflight_error = transport_preflight_error or RuntimeError(
                        "semantic-call prompt or generation parameters changed; "
                        "refusing replay/retry"
                    )
                elif not fingerprints and transport_preflight_error is None:
                    _preflight_guard(
                        self._append_ledger,
                        semantic_call_id, "semantic_request", call_id=call_id,
                        call_kind=call_kind,
                        request_fingerprint=request_fingerprint,
                    )
                    retry_state = _preflight_guard(
                        self._ledger_state, semantic_call_id)
                global_attempts = _preflight_guard(
                    self._lineage_global_http_attempts_used, semantic_root_id)
                if global_attempts > int(retry_state.get("http_attempts_used") or 0):
                    retry_state = dict(retry_state)
                    retry_state["http_attempts_used"] = global_attempts
            else:
                retry_state = _preflight_guard(
                    self._ledger_state, semantic_call_id)
                if (not retry_state.get("response_committed")
                        and transport_preflight_error is None):
                    _preflight_guard(
                        self._append_ledger,
                        semantic_call_id, "response_committed",
                        call_id=replay.get("replayed_from_call_id"),
                        response_slots_used=replay.get("response_slots_used"),
                        transient_failure_count=replay.get(
                            "transient_failure_count"),
                        http_attempts_used=replay.get("http_attempts_used"),
                        attempt_index=replay.get("http_attempts_used"),
                        request_fingerprint=request_fingerprint,
                    )
                    retry_state = _preflight_guard(
                        self._ledger_state, semantic_call_id)
            if (replay is None and transport_preflight_error is None
                    and any(item.get("committed") for item in lineage_state)):
                transport_preflight_error = RuntimeError(
                    "API attempt ledger records a committed response but the "
                    "response journal is missing"
                )
            prior_commit_sink = kwargs.get("_response_commit_sink")

            def _commit_response(result):
                result["semantic_root_id"] = semantic_context["semantic_root_id"]
                result["semantic_call_id"] = semantic_call_id
                result["generation_index"] = semantic_context["generation_index"]
                result["parent_semantic_call_id"] = (
                    semantic_context["parent_semantic_call_id"]
                )
                result["transport_recovery_index"] = (
                    semantic_context["generation_index"]
                )
                self._save_journal(
                    semantic_call_id, call_id, result,
                    request_fingerprint=request_fingerprint,
                )
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
            if transport_preflight_error is not None:
                try:
                    transport_preflight_error._anchorpatch_transport_observability_failure = True
                except Exception:
                    pass
                raise transport_preflight_error
            if replay is not None:
                out = replay
            elif retry_state.get("terminal_failure"):
                prior = retry_state["terminal_failure"]
                duplicate_refusal = RuntimeError(
                    "OpenCode MiniMax-M3 semantic call previously exhausted or "
                    f"failed fatally ({prior.get('error_type') or prior.get('status')}); "
                    "refusing a duplicate provider POST"
                )
                duplicate_refusal._anchorpatch_transport_observability_failure = True
                raise duplicate_refusal
            else:
                # Preflight and journal/ledger inspection can take long enough
                # for a sibling to latch a global stop or for immutable
                # campaign identity to drift.  Recheck immediately before the
                # provider function so no paid POST begins after that event.
                enforce_campaign_runtime_guards(
                    self.out_dir, self.sample_id)
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
            record = self._base_record(
                call_id, call_kind, semantic_context=semantic_context
            )
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
                "transport_resume_policy": provider_runtime.get(
                    "transport_resume_policy"),
                "anthropic_sdk_version": provider_runtime.get("anthropic_sdk_version"),
                "max_tokens": provider_runtime.get("effective_max_tokens"),
                "thinking_mode": provider_runtime.get("thinking_mode"),
                "provider_called": provider_post_started,
                "response_replayed": False,
                "replayed_from_call_id": None,
                "max_response_slots": provider_runtime.get("max_response_slots"),
                "response_slots_used": failure_state.get("response_slots_used"),
                "max_transient_failures": provider_runtime.get("max_transient_failures"),
                "transient_failure_count": failure_state.get("transient_failure_count"),
                "http_attempts_used": failure_state.get("http_attempts_used"),
                "transport_recovery_index": semantic_context["generation_index"],
                "generation_index": semantic_context["generation_index"],
                "semantic_root_id": semantic_context["semantic_root_id"],
                "parent_semantic_call_id": (
                    semantic_context["parent_semantic_call_id"]
                ),
                "request_fingerprint": request_fingerprint,
            })
            if (str(requested_model).lower().startswith("minimax-m3")
                    and classification == "provider/API failure"
                    and provider_post_started
                    and not failure_state.get("call_failed")):
                persisted_state = failure_state
                self._append_ledger(
                    semantic_call_id, "call_failed", call_id=call_id,
                    status="provider_failure",
                    error_type=record.get("error_type"),
                    response_slots_used=persisted_state.get("response_slots_used"),
                    transient_failure_count=persisted_state.get("transient_failure_count"),
                    http_attempts_used=persisted_state.get("http_attempts_used"),
                    attempt_index=persisted_state.get("http_attempts_used"),
                    request_fingerprint=request_fingerprint,
                )
            # The semantic-root lock covers both sides of the terminal
            # evidence pair.  Publish the ledger terminal before the API row
            # so a live inspector either sees no mapped API row yet or sees a
            # complete pair; it must never observe an API failure row whose
            # matching call_failed event is still pending.
            self._write_record(record)
            infrastructure_incomplete = (
                classification == "provider/API failure"
                and (
                    _is_transport_retry_exhaustion(record, exc)
                    or (
                        record.get("error_type")
                        == "provider_access_denied"
                        and semantic_context.get("parent_semantic_call_id")
                        in {
                            item.get("parent_semantic_call_id")
                            for item in self._provider_access_retry_authorizations.values()
                        }
                    )
                )
            )
            try:
                setattr(exc, "_anchorpatch_api_recorded", True)
                setattr(
                    exc, "_anchorpatch_failure_class",
                    "infrastructure_incomplete"
                    if infrastructure_incomplete else "local_failure",
                )
                setattr(exc, "_anchorpatch_api_record", dict(record))
            except Exception:
                pass
            _release_semantic_lock()
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
        record_context = semantic_context
        if isinstance(meta, dict) and meta.get("semantic_call_id"):
            parent_semantic_call_id = meta.get("parent_semantic_call_id")
            record_context = _semantic_lineage_fields(
                meta.get("semantic_call_id"), parent_semantic_call_id
            )
            self._semantic_parent_by_id[record_context["semantic_call_id"]] = (
                parent_semantic_call_id
            )
        record = self._base_record(
            call_id, meta.get("call_kind") or call_kind,
            semantic_context=record_context,
        )
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
            "transport_resume_policy": meta.get("transport_resume_policy"),
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
            "transport_recovery_index": record_context["generation_index"],
            "generation_index": record_context["generation_index"],
            "semantic_root_id": record_context["semantic_root_id"],
            "parent_semantic_call_id": record_context["parent_semantic_call_id"],
            "request_fingerprint": request_fingerprint,
        })
        if classification:
            record["subagent_audit_required"] = _audit_required(record)
            record["rerun_recommended"] = False
            record["count_as_method_failure"] = True
        self._write_record(record)
        # Keep exclusive ownership through raw capture and the terminal API
        # row.  Releasing after the transport journal alone would permit a
        # duplicate worker to acquire this root during cross-file commit.
        _release_semantic_lock()

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

    def record_runner_exception(self, exc, call_kind="runner_exception"):
        self.call_index += 1
        call_id = f"runner{self.call_index:06d}_{uuid.uuid4().hex[:8]}"
        record = self._base_record(call_id, call_kind=call_kind)
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


def _git_identity_details():
    """Return commit, tree state, and the exact porcelain used for the state."""
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
    return commit, ("dirty" if status.strip() else "clean"), status


def _git_identity():
    """Return the immutable Git identity required for every V8+ campaign."""
    commit, tree_state, _status = _git_identity_details()
    return commit, tree_state


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_recovery_authorization_chain(out_dir, path, record):
    """Load the exact V2 authorization chain pinned by nested SHA-256 links."""
    history = []
    seen_paths = set()
    seen_ids = set()
    current_path = os.path.realpath(path)
    current = record
    prior_commit = record.get("prior_git_commit")
    prior_fingerprint = record.get("prior_code_fingerprint")
    manifest_digest = record.get("dispatch_manifest_sha256")
    while True:
        authorization_id = current.get("authorization_id")
        if (current_path in seen_paths
                or not isinstance(authorization_id, str)
                or not authorization_id
                or authorization_id in seen_ids
                or current.get("schema")
                != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
                or current.get("recovery_kind")
                != LEDGER_LOCK_RECOVERY_KIND
                or current.get("prior_git_commit") != prior_commit
                or current.get("prior_git_tree_state") != "clean"
                or current.get("prior_code_fingerprint")
                != prior_fingerprint
                or current.get("dispatch_manifest_sha256")
                != manifest_digest
                or not re.fullmatch(
                    r"[0-9a-f]{40}",
                    str(current.get("recovery_git_commit") or ""),
                )
                or current.get("recovery_git_tree_state") != "clean"
                or not isinstance(
                    current.get("recovery_code_fingerprint"), dict)):
            raise RuntimeError(
                "campaign recovery superseded authorization identity mismatch")
        seen_paths.add(current_path)
        seen_ids.add(authorization_id)
        history.append({
            "record": current,
            "path": current_path,
            "sha256": _sha256_file(current_path),
        })
        relative = current.get("superseded_authorization_path")
        if relative is None:
            break
        expected_digest = current.get("superseded_authorization_sha256")
        if (not isinstance(relative, str) or not relative
                or not isinstance(expected_digest, str)
                or not expected_digest):
            raise RuntimeError(
                "campaign recovery superseded authorization link is invalid")
        superseded_path = os.path.realpath(os.path.join(out_dir, relative))
        if (os.path.commonpath([out_dir, superseded_path]) != out_dir
                or not os.path.isfile(superseded_path)
                or _sha256_file(superseded_path) != expected_digest):
            raise RuntimeError(
                "campaign recovery superseded authorization digest mismatch")
        try:
            with open(superseded_path, encoding="utf-8") as handle:
                superseded = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "campaign recovery superseded authorization is invalid") from exc
        if not isinstance(superseded, dict):
            raise RuntimeError(
                "campaign recovery superseded authorization is invalid")
        current_path = superseded_path
        current = superseded
    return history


def _preauthorization_stop_evidence_matches(
        archived_stop, preauthorization_worker_ids, expected_stop_errors):
    """Keep old worker evidence valid across an operator pause supersession."""
    if (archived_stop.get("condition")
            == "operator_directed_dispatcher_pause"
            or str(archived_stop.get("error") or "").endswith((
                "infrastructure outcome provenance mismatch",
                "infrastructure attempt lineage mismatch",
                "API attempt ledger contains an unclosed HTTP attempt",
            ))):
        return True
    return bool(preauthorization_worker_ids) == (
        archived_stop.get("error") in expected_stop_errors)


def read_campaign_recovery_authorization(out_dir):
    """Validate a narrow, append-only campaign recovery boundary."""
    out_dir = os.path.abspath(out_dir)
    path = os.path.join(out_dir, CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            record = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("invalid campaign recovery authorization") from exc
    if (not isinstance(record, dict)
            or record.get("schema") not in {
                CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA,
                CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
            }
            or not isinstance(record.get("authorization_id"), str)
            or not record.get("authorization_id")):
        raise RuntimeError("invalid campaign recovery authorization")
    recovery_kind = record.get("recovery_kind")
    legacy_git_recovery = (
        record.get("schema") == CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA
        and recovery_kind is None
    )
    ledger_lock_recovery = (
        record.get("schema") == CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
        and recovery_kind == LEDGER_LOCK_RECOVERY_KIND
    )
    if not (legacy_git_recovery or ledger_lock_recovery):
        raise RuntimeError("unknown campaign recovery kind")

    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if (not os.path.isfile(manifest_path)
            or _sha256_file(manifest_path)
            != record.get("dispatch_manifest_sha256")):
        raise RuntimeError("campaign recovery manifest digest mismatch")
    try:
        with open(manifest_path, encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("campaign recovery manifest is invalid") from exc
    if (manifest.get("run_git_commit") != record.get("prior_git_commit")
            or manifest.get("git_tree_state") != "clean"
            or manifest.get("code_fingerprint")
            != record.get("prior_code_fingerprint")):
        raise RuntimeError("campaign recovery prior identity mismatch")

    archived_relative = record.get("archived_stop_path")
    if not isinstance(archived_relative, str) or not archived_relative:
        raise RuntimeError("campaign recovery archived stop path is invalid")
    archived_path = os.path.realpath(os.path.join(out_dir, archived_relative))
    if (os.path.commonpath([out_dir, archived_path]) != out_dir
            or not os.path.isfile(archived_path)
            or _sha256_file(archived_path)
            != record.get("archived_stop_sha256")):
        raise RuntimeError("campaign recovery archived stop digest mismatch")
    try:
        with open(archived_path, encoding="utf-8") as handle:
            archived_stop = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("campaign recovery archived stop is invalid") from exc
    prior_commit = record.get("prior_git_commit")
    if legacy_git_recovery:
        stop_valid = (
            archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition") == "git_identity_drift"
            and archived_stop.get("expected_commit") == prior_commit
            and archived_stop.get("actual_commit") == prior_commit
            and archived_stop.get("expected_tree_state") == "clean"
            and archived_stop.get("actual_tree_state") == "dirty"
        )
    else:
        recovery_error = archived_stop.get("error")
        superseding_lock_recovery = (
            (
                recovery_error
                == "campaign recovery interrupted attempt evidence is incomplete"
                or str(recovery_error).startswith(
                    "hybridpatch phase preflight failed before worker launch: "
                    "attempt ledger has no mapped API call: fullrewrite/"
                )
                or re.fullmatch(
                    r"worker [A-Za-z0-9_.-]+ exited before authorization "
                    r"with -?[0-9]+",
                    str(recovery_error),
                ) is not None
            )
            and isinstance(record.get("superseded_authorization_path"), str)
            and bool(record.get("superseded_authorization_path"))
        )
        operator_pause_recovery = (
            archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "operator_directed_dispatcher_pause"
            and archived_stop.get("stopped_git_commit")
            == record.get("recovery_git_commit")
            and archived_stop.get("stopped_git_tree_state") == "clean"
            and archived_stop.get("git_status_porcelain") == ""
            and isinstance(record.get("superseded_authorization_path"), str)
            and bool(record.get("superseded_authorization_path"))
            and isinstance(
                record.get("operator_pause_reconciled_workers"), list)
        )
        provider_access_retry_recovery = (
            archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "dispatcher_integrity_failure"
            and str(archived_stop.get("error") or "").endswith((
                "infrastructure outcome provenance mismatch",
                "infrastructure attempt lineage mismatch",
                "API attempt ledger contains an unclosed HTTP attempt",
            ))
            and isinstance(record.get("superseded_authorization_path"), str)
            and bool(record.get("superseded_authorization_path"))
            and isinstance(
                record.get("provider_access_retry_authorizations"), list)
            and bool(record.get("provider_access_retry_authorizations"))
        )
        stop_valid = (
            (
                archived_stop.get("schema") == STOP_CONDITION_SCHEMA
                and archived_stop.get("condition")
                == "dispatcher_integrity_failure"
                and isinstance(recovery_error, str)
                and ("API row" in recovery_error
                     or superseding_lock_recovery)
            )
            or operator_pause_recovery
            or provider_access_retry_recovery
        )
    if not stop_valid:
        raise RuntimeError("campaign recovery stop is not authorized")
    if os.path.exists(os.path.join(out_dir, "campaign_stop.json")):
        raise RuntimeError(
            "campaign recovery requires the stop latch to be archived first")
    recovery_chain = (
        _load_recovery_authorization_chain(out_dir, path, record)
        if ledger_lock_recovery else [{
            "record": record,
            "path": path,
            "sha256": _sha256_file(path),
        }]
    )

    current_commit, current_tree = _git_identity()
    current_fingerprint = code_fingerprint()
    if (current_commit != record.get("recovery_git_commit")
            or current_tree != "clean"
            or record.get("recovery_git_tree_state") != "clean"
            or current_fingerprint
            != record.get("recovery_code_fingerprint")):
        raise RuntimeError("campaign recovery current identity mismatch")
    prior_fingerprint = record.get("prior_code_fingerprint")
    if not isinstance(prior_fingerprint, dict):
        raise RuntimeError("campaign recovery prior fingerprint is invalid")
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(current_fingerprint)
        if prior_fingerprint.get(key) != current_fingerprint.get(key)
    )
    expected_fingerprint_changes = ["run_meta.py"]
    if (fingerprint_changes != expected_fingerprint_changes
            or record.get("changed_code_fingerprint_keys")
            != expected_fingerprint_changes):
        raise RuntimeError("campaign recovery runtime fingerprint scope changed")

    try:
        changed = subprocess.run(
            [
                "git", "-C", _HERE, "diff", "--name-only",
                prior_commit, current_commit, "--",
            ],
            check=True, capture_output=True, text=True, encoding="utf-8",
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("cannot audit campaign recovery Git diff") from exc
    changed = sorted(item.replace("\\", "/") for item in changed if item)
    recorded_changes = record.get("changed_tracked_paths")
    changed_set = set(changed)
    if legacy_git_recovery:
        changed_scope_valid = changed_set == _GIT_IDENTITY_RECOVERY_CHANGED_PATHS
    else:
        changed_scope_valid = (
            changed_set <= _LEDGER_LOCK_RECOVERY_ALLOWED_CHANGED_PATHS
            and {
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/paired_campaign_dispatch.py",
                "HP_V8/src/run_meta.py",
                "HP_V8/src/test_model_openai.py",
            } <= changed_set
        )
    if changed != recorded_changes or not changed_scope_valid:
        raise RuntimeError("campaign recovery changed-file scope mismatch")
    if ledger_lock_recovery:
        worker_ids = record.get("recovered_worker_launch_ids")
        preauthorization_worker_ids = record.get(
            "preauthorization_worker_launch_ids", [])
        api_incidents = record.get("incident_api_rows")
        attempt_incidents = record.get("incident_attempt_rows")
        if (not isinstance(worker_ids, list) or not worker_ids
                or len(worker_ids) != len(set(worker_ids))
                or any(not isinstance(item, str) or not item
                       for item in worker_ids)
                or not isinstance(preauthorization_worker_ids, list)
                or len(preauthorization_worker_ids)
                != len(set(preauthorization_worker_ids))
                or any(not isinstance(item, str) or not item
                       for item in preauthorization_worker_ids)
                or not set(preauthorization_worker_ids) <= set(worker_ids)
                or not isinstance(api_incidents, list) or not api_incidents
                or not isinstance(attempt_incidents, list)):
            raise RuntimeError("campaign ledger-lock recovery evidence is invalid")

        def _validate_rows(filename, entries):
            rows = _read_jsonl_records_with_retry(
                os.path.join(out_dir, filename))
            seen_numbers = set()
            validated = []
            for entry in entries:
                number = entry.get("row_number") if isinstance(entry, dict) else None
                digest = entry.get("canonical_sha256") if isinstance(entry, dict) else None
                if (not isinstance(number, int) or isinstance(number, bool)
                        or not 1 <= number <= len(rows)
                        or number in seen_numbers
                        or not isinstance(digest, str)
                        or digest != _canonical_record_sha256(rows[number - 1])):
                    raise RuntimeError(
                        f"campaign recovery {filename} evidence mismatch")
                seen_numbers.add(number)
                validated.append((entry, rows[number - 1]))
            return validated

        validated_api = _validate_rows("api_calls.jsonl", api_incidents)
        validated_attempts = _validate_rows(
            "api_attempt_ledger.jsonl", attempt_incidents)
        if any(
                row.get("method") != "fullrewrite"
                or row.get("classification") != "runner_exception"
                or row.get("error_type") != "AlreadyLocked"
                or row.get("worker_launch_id") not in worker_ids
                or row.get("provider_request_id") is not None
                or row.get("total_tokens") is not None
                or entry.get("incident_kind") != "ledger_lock"
                for entry, row in validated_api):
            raise RuntimeError("campaign recovery API incidents are not lock failures")
        incident_roots = {
            row.get("semantic_root_id") for _entry, row in validated_api
        }
        if any(
                row.get("worker_launch_id") not in worker_ids
                or row.get("event") == "response_committed"
                or entry.get("incident_kind") not in {
                    "ledger_lock", "dispatcher_interrupted_open_attempt"
                }
                or (entry.get("incident_kind") == "ledger_lock"
                    and (row.get("semantic_root_id") not in incident_roots
                         or row.get("event") == "generation_progress"))
                for entry, row in validated_attempts):
            raise RuntimeError(
                "campaign recovery attempt incidents are not pre-commit lock failures")
        interrupted_ids = {
            row.get("semantic_call_id")
            for entry, row in validated_attempts
            if entry.get("incident_kind")
            == "dispatcher_interrupted_open_attempt"
        }
        for semantic_call_id in interrupted_ids:
            authorized_group = [
                row for entry, row in validated_attempts
                if (entry.get("incident_kind")
                    == "dispatcher_interrupted_open_attempt"
                    and row.get("semantic_call_id") == semantic_call_id)
            ]
            start_count = sum(
                row.get("event") == "attempt_start"
                for row in authorized_group
            )
            end_count = sum(
                row.get("event") == "attempt_end"
                for row in authorized_group
            )
            pre_provider_interruption = (
                start_count == 0
                and end_count == 0
                and sum(row.get("event") == "semantic_request"
                        for row in authorized_group) == 1
            )
            if (not (start_count > end_count or pre_provider_interruption)
                    or any(row.get("event") == "response_committed"
                           for row in authorized_group)):
                raise RuntimeError(
                    "campaign recovery interrupted attempt evidence is incomplete")
        provider_access_retries = record.get(
            "provider_access_retry_authorizations", [])
        provider_access_resume_samples = record.get(
            "provider_access_resume_samples")
        if (not isinstance(provider_access_resume_samples, list)
                or not provider_access_resume_samples
                or len(provider_access_resume_samples)
                != len(set(provider_access_resume_samples))
                or any(not isinstance(item, str) or not item
                       for item in provider_access_resume_samples)):
            raise RuntimeError(
                "campaign provider-access resume sample scope is invalid")
        retry_samples = set()
        retry_workers = set()
        retry_parents = set()
        api_rows_all = _read_jsonl_records_with_retry(
            os.path.join(out_dir, "api_calls.jsonl"))
        attempt_rows_all = _read_jsonl_records_with_retry(
            os.path.join(out_dir, "api_attempt_ledger.jsonl"))
        for retry in provider_access_retries:
            if not isinstance(retry, dict):
                raise RuntimeError(
                    "campaign provider-access retry authorization is invalid")
            sample = retry.get("sample")
            worker_id = retry.get("prior_worker_launch_id")
            parent_id = retry.get("parent_semantic_call_id")
            semantic_root_id = retry.get("semantic_root_id")
            generation_index = retry.get("generation_index")
            api_entry = retry.get("api_row")
            attempt_entries = retry.get("attempt_rows")
            if (not isinstance(sample, str) or not sample
                    or sample in retry_samples
                    or not isinstance(worker_id, str) or not worker_id
                    or worker_id in retry_workers
                    or worker_id not in worker_ids
                    or not isinstance(parent_id, str) or not parent_id
                    or parent_id in retry_parents
                    or not isinstance(semantic_root_id, str)
                    or not semantic_root_id
                    or not isinstance(generation_index, int)
                    or isinstance(generation_index, bool)
                    or generation_index < 1
                    or retry.get("semantic_call_id") != (
                        f"{semantic_root_id}/g{generation_index:03d}")
                    or not isinstance(retry.get("request_fingerprint"), str)
                    or not retry.get("request_fingerprint")
                    or not isinstance(retry.get("next_attempt_index"), int)
                    or isinstance(retry.get("next_attempt_index"), bool)
                    or retry.get("next_attempt_index") < 2
                    or not isinstance(api_entry, dict)
                    or not isinstance(attempt_entries, list)
                    or not attempt_entries):
                raise RuntimeError(
                    "campaign provider-access retry authorization is invalid")
            parent_match = re.fullmatch(r"(.+)/g([0-9]{3,})", parent_id)
            if (parent_match is None
                    or parent_match.group(1) != semantic_root_id
                    or int(parent_match.group(2)) + 1 != generation_index):
                raise RuntimeError(
                    "campaign provider-access retry lineage is invalid")

            def _bound_row(rows, entry, label):
                number = entry.get("row_number")
                digest = entry.get("canonical_sha256")
                if (not isinstance(number, int)
                        or isinstance(number, bool)
                        or not 1 <= number <= len(rows)
                        or not isinstance(digest, str)
                        or digest != _canonical_record_sha256(
                            rows[number - 1])):
                    raise RuntimeError(
                        f"campaign provider-access {label} evidence mismatch")
                return rows[number - 1]

            api_row = _bound_row(api_rows_all, api_entry, "API")
            bound_attempts = [
                _bound_row(attempt_rows_all, entry, "attempt")
                for entry in attempt_entries
            ]
            if (api_row.get("sample") != sample
                    or api_row.get("worker_launch_id") != worker_id
                    or api_row.get("method") != "fullrewrite"
                    or api_row.get("semantic_call_id") != parent_id
                    or api_row.get("semantic_root_id") != semantic_root_id
                    or api_row.get("generation_index")
                    != generation_index - 1
                    or api_row.get("request_fingerprint")
                    != retry.get("request_fingerprint")
                    or api_row.get("classification")
                    != "provider/API failure"
                    or api_row.get("error_type")
                    != "provider_access_denied"
                    or api_row.get("http_status") not in {401, 403}
                    or api_row.get("provider_called") is not True
                    or not isinstance(
                        api_row.get("response_slots_used"), int)
                    or not 0 <= api_row.get("response_slots_used") < 2
                    or api_row.get("transient_failure_count") != 0):
                raise RuntimeError(
                    "campaign provider-access API evidence is invalid")
            if any(
                    row.get("worker_launch_id") != worker_id
                    or row.get("semantic_call_id") != parent_id
                    or row.get("semantic_root_id") != semantic_root_id
                    for row in bound_attempts):
                raise RuntimeError(
                    "campaign provider-access attempt scope is invalid")
            events = [row.get("event") for row in bound_attempts]
            attempt_end = [
                row for row in bound_attempts
                if row.get("event") == "attempt_end"
            ]
            call_failed = [
                row for row in bound_attempts
                if row.get("event") == "call_failed"
            ]
            if (events.count("semantic_request") != 1
                    or events.count("attempt_start") < 1
                    or len(attempt_end) < 1
                    or events.count("attempt_end")
                    != events.count("attempt_start")
                    or len(call_failed) != 1
                    or "response_committed" in events
                    or attempt_end[-1].get("status") != "fatal_error"
                    or attempt_end[-1].get("error_type")
                    != "provider_access_denied"
                    or attempt_end[-1].get(
                        "generation_delta_seen") is not False
                    or call_failed[0].get("status") != "provider_failure"
                    or call_failed[0].get("error_type")
                    != "provider_access_denied"):
                raise RuntimeError(
                    "campaign provider-access terminal evidence is invalid")
            root_attempt_indexes = [
                row.get("attempt_index") for row in attempt_rows_all
                if row.get("semantic_root_id") == semantic_root_id
                and isinstance(row.get("attempt_index"), int)
                and not isinstance(row.get("attempt_index"), bool)
            ]
            if retry.get("next_attempt_index") != (
                    max(root_attempt_indexes, default=0) + 1):
                raise RuntimeError(
                    "campaign provider-access next attempt is invalid")
            retry_samples.add(sample)
            retry_workers.add(worker_id)
            retry_parents.add(parent_id)
        dispatch_rows = _read_jsonl_records_with_retry(
            os.path.join(out_dir, "dispatch_log.jsonl"))
        launches = {
            row.get("worker_launch_id"): row for row in dispatch_rows
            if row.get("event") == "launch"
            and row.get("method_phase") == "fullrewrite"
        }
        if not set(worker_ids) <= set(launches):
            raise RuntimeError("campaign recovery worker cohort is not dispatched")
        preauthorization_set = set(preauthorization_worker_ids)
        metadata_rows = _read_jsonl_records_with_retry(
            os.path.join(out_dir, "run_metadata.jsonl"))
        metadata_workers = {
            row.get("worker_launch_id") for row in metadata_rows
            if row.get("worker_launch_id") in set(worker_ids)
        }
        authorized_workers = {
            row.get("worker_launch_id") for row in dispatch_rows
            if row.get("event") == "worker_authorized"
        }
        exits = {
            row.get("worker_launch_id"): row for row in dispatch_rows
            if row.get("event") == "worker_exit"
        }
        api_workers = {
            row.get("worker_launch_id")
            for row in _read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl"))
        }
        attempt_workers = {
            row.get("worker_launch_id")
            for row in _read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
        }
        expected_stop_errors = set()
        for worker_id in preauthorization_set:
            launch = launches.get(worker_id) or {}
            exit_row = exits.get(worker_id) or {}
            if (worker_id in metadata_workers
                    or worker_id in authorized_workers
                    or worker_id in api_workers
                    or worker_id in attempt_workers
                    or exit_row.get("sample") != launch.get("sample")
                    or exit_row.get("pid") != launch.get("pid")
                    or not isinstance(exit_row.get("returncode"), int)
                    or isinstance(exit_row.get("returncode"), bool)
                    or exit_row.get("returncode") == 0
                    or exit_row.get("disposition") != "campaign_fatal"):
                raise RuntimeError(
                    "campaign recovery preauthorization worker evidence mismatch")
            expected_stop_errors.add(
                f"worker {launch.get('sample')} exited before authorization "
                f"with {exit_row.get('returncode')}"
            )
        if (metadata_workers != set(worker_ids) - preauthorization_set
                or not _preauthorization_stop_evidence_matches(
                    archived_stop, preauthorization_set,
                    expected_stop_errors)):
            raise RuntimeError(
                "campaign recovery preauthorization stop evidence mismatch")

    identity_history = [{
        "authorization_id": None,
        "authorization_sha256": None,
        "run_git_commit": record.get("prior_git_commit"),
        "git_tree_state": "clean",
        "code_fingerprint": record.get("prior_code_fingerprint"),
    }]
    for item in reversed(recovery_chain):
        historical = item["record"]
        identity_history.append({
            "authorization_id": historical.get("authorization_id"),
            "authorization_sha256": item["sha256"],
            "run_git_commit": historical.get("recovery_git_commit"),
            "git_tree_state": "clean",
            "code_fingerprint": historical.get(
                "recovery_code_fingerprint"),
        })
    return dict(
        record,
        authorization_path=path,
        authorization_sha256=_sha256_file(path),
        recovery_identity_history=identity_history,
    )


@functools.lru_cache(maxsize=16)
def campaign_recovery_incident_evidence(out_dir):
    """Return exact row hashes and workers covered by a V2 lock recovery."""
    authorization = read_campaign_recovery_authorization(out_dir)
    if (not authorization
            or authorization.get("recovery_kind")
            != LEDGER_LOCK_RECOVERY_KIND):
        return {
            "api_row_hashes": frozenset(),
            "attempt_row_hashes": frozenset(),
            "worker_launch_ids": frozenset(),
            "preauthorization_worker_launch_ids": frozenset(),
            "provider_access_retry_authorizations": {},
            "provider_access_resume_samples": frozenset(),
            "authorization_id": None,
        }
    provider_access_retries = {
        item["sample"]: dict(item)
        for item in authorization.get(
            "provider_access_retry_authorizations", [])
    }
    return {
        "api_row_hashes": frozenset({
            item["canonical_sha256"]
            for item in authorization["incident_api_rows"]
        }),
        "attempt_row_hashes": frozenset({
            item["canonical_sha256"]
            for item in authorization["incident_attempt_rows"]
        }),
        "worker_launch_ids": frozenset(
            authorization["recovered_worker_launch_ids"]),
        "preauthorization_worker_launch_ids": frozenset(
            authorization.get("preauthorization_worker_launch_ids", [])),
        "provider_access_retry_authorizations": provider_access_retries,
        "provider_access_resume_samples": frozenset(
            authorization.get("provider_access_resume_samples") or []),
        "authorization_id": authorization["authorization_id"],
    }


def filter_authorized_attempt_incidents(out_dir, records):
    incident_hashes = campaign_recovery_incident_evidence(
        out_dir)["attempt_row_hashes"]
    if not incident_hashes:
        return list(records)
    return [
        record for record in records
        if _canonical_record_sha256(record) not in incident_hashes
    ]


def is_authorized_api_incident(out_dir, record):
    incident_hashes = campaign_recovery_incident_evidence(
        out_dir)["api_row_hashes"]
    return _canonical_record_sha256(record) in incident_hashes


def _recovery_identity_transition_matches(
        authorization, *, prior_commit, prior_tree_state, prior_fingerprint,
        current_commit, current_tree_state, current_fingerprint):
    return bool(
        authorization
        and prior_commit == authorization.get("prior_git_commit")
        and prior_tree_state == "clean"
        and prior_fingerprint == authorization.get("prior_code_fingerprint")
        and current_commit == authorization.get("recovery_git_commit")
        and current_tree_state == "clean"
        and current_fingerprint
        == authorization.get("recovery_code_fingerprint")
    )


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
    # ``portalocker.lock(..., LOCK_EX)`` can fail immediately with
    # ``AlreadyLocked`` under normal cross-process contention on Windows.
    # ``portalocker.Lock`` uses non-blocking attempts plus a bounded retry
    # interval, so genuine metadata writers serialize instead of surfacing a
    # local infrastructure error to the model-call recorder.
    with portalocker.Lock(
        lock_path,
        mode="a+",
        timeout=60,
        check_interval=0.05,
        flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        encoding="utf-8",
    ):
        yield os.path.join(out_dir, "run_metadata.jsonl")


def _existing_experiment_payload(out_dir):
    for _root, _dirs, files in os.walk(out_dir):
        if any(name.endswith(".ckpt.json") for name in files):
            return True
        if "api_calls.jsonl" in files:
            return True
    return False


def _one_prior_value(records, key):
    if records and any(key not in record or record.get(key) is None
                       for record in records):
        raise RuntimeError(f"run_metadata has unrecorded {key} values")
    values = {json.dumps(r.get(key), ensure_ascii=False, sort_keys=True)
              for r in records}
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
    method_phase = os.environ.get("ANCHORPATCH_METHOD_PHASE")
    declared_method_set = os.environ.get("ANCHORPATCH_CAMPAIGN_METHOD_SET")
    campaign_methods = sorted(set(methods))
    if declared_method_set:
        declared = sorted(set(
            item.strip() for item in declared_method_set.split(",")
            if item.strip()
        ))
        if (method_phase not in declared
                or list(methods) != [method_phase]
                or declared != ["fullrewrite", "hybridpatch"]):
            raise RuntimeError(
                "phased campaign method declaration is inconsistent"
            )
        campaign_methods = declared
    elif method_phase:
        raise RuntimeError(
            "ANCHORPATCH_METHOD_PHASE requires a campaign method set"
        )
    interrupted_resume_raw = os.environ.get(
        "ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE")
    interrupted_resume_evidence = None
    if interrupted_resume_raw:
        try:
            interrupted_resume_evidence = json.loads(interrupted_resume_raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "interrupted phase resume evidence is invalid JSON") from exc
        if (not isinstance(interrupted_resume_evidence, dict)
                or interrupted_resume_evidence.get("schema")
                != "anchorpatch.interrupted_phase_resume/1"
                or interrupted_resume_evidence.get("sample")
                not in set(samples)
                or interrupted_resume_evidence.get("method_phase")
                != method_phase
                or list(methods) != [method_phase]
                or not isinstance(interrupted_resume_evidence.get(
                    "prior_invocation_id"), str)
                or not isinstance(interrupted_resume_evidence.get(
                    "task_plan_sha256"), str)
                or not isinstance(interrupted_resume_evidence.get(
                    "checkpoint_progress"), dict)):
            raise RuntimeError(
                "interrupted phase resume evidence is inconsistent")
    os.makedirs(out_dir, exist_ok=True)
    fp = code_fingerprint()
    run_git_commit, git_tree_state = _git_identity()
    recovery_authorization = read_campaign_recovery_authorization(out_dir)
    expected_commit = os.environ.get("ANCHORPATCH_EXPECTED_GIT_COMMIT")
    expected_tree_state = os.environ.get(
        "ANCHORPATCH_EXPECTED_GIT_TREE_STATE")
    expected_identity_matches = (
        (not expected_commit or run_git_commit == expected_commit)
        and (not expected_tree_state
             or git_tree_state == expected_tree_state)
    )
    recovery_expected_identity_matches = bool(
        recovery_authorization
        and expected_commit
        in {
            recovery_authorization.get("prior_git_commit"),
            recovery_authorization.get("recovery_git_commit"),
        }
        and (not expected_tree_state or expected_tree_state == "clean")
        and run_git_commit
        == recovery_authorization.get("recovery_git_commit")
        and git_tree_state == "clean"
    )
    if not (expected_identity_matches or recovery_expected_identity_matches):
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
    resume_semantic_call_id = os.environ.get(
        "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID")
    resume_index_raw = os.environ.get(
        "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX")
    resume_fingerprint = os.environ.get(
        "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT")
    resume_next_attempt_raw = os.environ.get(
        "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX")
    resume_fields = (
        resume_semantic_call_id, resume_index_raw, resume_fingerprint,
        resume_next_attempt_raw,
    )
    if any(value is None for value in resume_fields) and any(
            value is not None for value in resume_fields):
        raise RuntimeError(
            "infrastructure resume authorization is incomplete")
    transport_resume_authorization = None
    if resume_semantic_call_id is not None:
        if not resume_semantic_call_id:
            raise RuntimeError(
                "infrastructure resume semantic call ID is empty")
        parent_lineage = _semantic_lineage_fields(resume_semantic_call_id)
        try:
            resume_index = int(resume_index_raw)
            resume_next_attempt = int(resume_next_attempt_raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "infrastructure resume index or next attempt is not an integer"
            ) from exc
        if resume_index != parent_lineage["generation_index"] + 1:
            raise RuntimeError(
                "infrastructure resume generation index must be exactly one "
                "after the parent generation")
        if not resume_fingerprint or resume_next_attempt < 1:
            raise RuntimeError(
                "infrastructure resume fingerprint or next attempt is invalid"
            )
        transport_resume_authorization = {
            "parent_semantic_call_id": resume_semantic_call_id,
            "semantic_root_id": parent_lineage["semantic_root_id"],
            "semantic_call_id": (
                f"{parent_lineage['semantic_root_id']}/"
                f"{_generation_segment(resume_index)}"
            ),
            "generation_index": resume_index,
            "request_fingerprint": resume_fingerprint,
            "next_attempt_index": resume_next_attempt,
        }

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
        authorized_identity_history = (
            recovery_authorization.get("recovery_identity_history")
            if recovery_authorization else None
        )
        if recovery_authorization and not authorized_identity_history:
            authorized_identity_history = [
                {
                    "authorization_id": None,
                    "authorization_sha256": None,
                    "run_git_commit": recovery_authorization.get(
                        "prior_git_commit"),
                    "git_tree_state": "clean",
                    "code_fingerprint": recovery_authorization.get(
                        "prior_code_fingerprint"),
                },
                {
                    "authorization_id": recovery_authorization.get(
                        "authorization_id"),
                    "authorization_sha256": recovery_authorization.get(
                        "authorization_sha256"),
                    "run_git_commit": recovery_authorization.get(
                        "recovery_git_commit"),
                    "git_tree_state": "clean",
                    "code_fingerprint": recovery_authorization.get(
                        "recovery_code_fingerprint"),
                },
            ]

        def _matches_authorized_identity(record):
            boundary = record.get("campaign_recovery_authorization")
            for identity in authorized_identity_history or []:
                if (record.get("run_git_commit")
                        != identity.get("run_git_commit")
                        or record.get("git_tree_state")
                        != identity.get("git_tree_state")
                        or record.get("code_fingerprint")
                        != identity.get("code_fingerprint")):
                    continue
                authorization_id = identity.get("authorization_id")
                if authorization_id is None:
                    return boundary is None
                return bool(
                    isinstance(boundary, dict)
                    and boundary.get("authorization_id") == authorization_id
                    and boundary.get("authorization_sha256")
                    == identity.get("authorization_sha256")
                )
            return False

        recovery_identity_history_valid = bool(
            recovery_authorization
            and prior
            and all(_matches_authorized_identity(record) for record in prior)
        )
        for key, current in identity_fields.items():
            if recovery_identity_history_valid:
                continue
            previous = _one_prior_value(prior, key)
            if prior and previous is None:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior {key} is unrecorded"
                )
            transition_matches = _recovery_identity_transition_matches(
                recovery_authorization,
                prior_commit=_one_prior_value(prior, "run_git_commit"),
                prior_tree_state=_one_prior_value(prior, "git_tree_state"),
                prior_fingerprint=_one_prior_value(prior, "code_fingerprint"),
                current_commit=run_git_commit,
                current_tree_state=git_tree_state,
                current_fingerprint=fp,
            )
            if (previous is not None and previous != current
                    and not transition_matches):
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
            current_policy = provider_runtime["transport_resume_policy"]
            previous_policy = _one_prior_value(
                prior, "transport_resume_policy")
            if previous_policy != current_policy:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior transport "
                    f"resume policy is {previous_policy or 'unrecorded'}, "
                    f"current policy is {current_policy}; use a new --out_dir"
                )

        campaign_config = {
            "method_set": campaign_methods,
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
            "transport_resume_authorization": transport_resume_authorization,
            "campaign_recovery_authorization": (
                {
                    "authorization_id": recovery_authorization[
                        "authorization_id"],
                    "authorization_sha256": recovery_authorization[
                        "authorization_sha256"],
                    "prior_git_commit": recovery_authorization[
                        "prior_git_commit"],
                    "recovery_git_commit": recovery_authorization[
                        "recovery_git_commit"],
                    "archived_stop_path": recovery_authorization[
                        "archived_stop_path"],
                    "archived_stop_sha256": recovery_authorization[
                        "archived_stop_sha256"],
                }
                if recovery_authorization else None
            ),
        }
        if method_phase:
            rec["method_phase"] = method_phase
        if interrupted_resume_evidence is not None:
            rec["interrupted_resume_authorization"] = (
                interrupted_resume_evidence)
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
