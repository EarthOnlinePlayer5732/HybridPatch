"""
Experiment-output convention enforcement (run metadata, structured logs,
per-step document snapshots).

Provides the four mechanisms the runner uses to keep a run dir self-describing:

  code_fingerprint()         12-char sha1 per key source file — distinguishes
                             code versions without git (a run dir may outlive
                             any checkout). The "which code produced this?" fix.
  append_run_metadata(...)   register one process invocation. Fresh campaigns
                             append canonical events under one metadata lock;
                             existing run_metadata/3 directories retain their
                             historical in-place contract.
  finish_run_metadata(...)   append the terminal transition and, only after all
                             invocations stop, publish a receipt-bound
                             run_metadata.jsonl compatibility projection.
  RunLogger                  per-(method,sample) structured log under logs/:
                             tees progress to stdout + an ANSI-free file, and
                             captures noisy domain-evaluator stdout separately.
  dump_step_docs(...)        best-effort per-step output-document snapshots under
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
METADATA_EVENT_SCHEMA = "anchorpatch.run_metadata_event/1"
METADATA_EVENTS_FILENAME = "run_metadata_events.jsonl"
METADATA_PROJECTION_RECEIPT_SCHEMA = (
    "anchorpatch.run_metadata_projection_receipt/2"
)
METADATA_PROJECTION_RECEIPT_FILENAME = "run_metadata_projection_receipt.json"
METADATA_EVENT_PENDING_SCHEMA = "anchorpatch.run_metadata_event_pending/1"
METADATA_EVENT_PENDING_FILENAME = "run_metadata_event_pending.json"
METADATA_EVENT_RECOVERY_SCHEMA = "anchorpatch.run_metadata_event_recovery/1"
METADATA_EVENT_RECOVERY_DIRECTORY = "run_metadata_event_recoveries"
METADATA_EVENT_RECOVERY_REGISTRY_SCHEMA = (
    "anchorpatch.run_metadata_event_recovery_registry/1"
)
METADATA_EVENT_RECOVERY_REGISTRY_FILENAME = "_registry.json"
RUN_METADATA_STORAGE_EVENT_V1 = "event_v1"
RUN_METADATA_TERMINAL_STATUSES = frozenset({
    "finished", "failed", "evaluator_incomplete",
    "infrastructure_incomplete", "interrupted",
    "interrupted_by_dispatcher", "interrupted_before_audited_resume",
})
METADATA_RECOVERY_EVIDENCE_SCHEMA = (
    "anchorpatch.run_metadata_recovery_evidence/1"
)
STOP_CONDITION_SCHEMA = "anchorpatch.campaign_stop_condition/1"
EMERGENCY_STOP_DIRECTORY = "campaign_stop_emergency"
API_CALL_SCHEMA = "anchorpatch.api_call/4"
API_ATTEMPT_SCHEMA = "anchorpatch.api_attempt/4"
API_RESPONSE_JOURNAL_SCHEMA = "anchorpatch.api_response_journal/4"
DEEPSEEK_COMPACT_TRANSPORT_REVISION = "opencode_openai_compatible/6"
DEEPSEEK_COMPACT_EVENT_SCHEMA = "anchorpatch.transport_event/2"
SAMPLE_OUTCOME_SCHEMA = "anchorpatch.sample_outcome/1"
SNAPSHOT_MODE_ALL = "all"
SNAPSHOT_MODE_FAILURES = "failures"
SNAPSHOT_MODE_OFF = "off"
SNAPSHOT_MODES = frozenset({
    SNAPSHOT_MODE_ALL, SNAPSHOT_MODE_FAILURES, SNAPSHOT_MODE_OFF,
})
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
DISPATCHER_PROCESS_LOST_RECOVERY_KIND = "dispatcher_process_lost"
DISPATCHER_PARENT_LOSS_PENDING_FILENAME = (
    "dispatcher_parent_loss_recovery_pending.json"
)
DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME = (
    "deepseek_transport_inspector_recovery_pending.json"
)
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
_DEEPSEEK_TRANSPORT_DISCONNECT_RECOVERY_CHANGED_PATHS = {
    "HP_V8/src/authorize_ledger_lock_recovery.py",
    "HP_V8/src/model_openai.py",
    "HP_V8/src/paired_campaign_dispatch.py",
    "HP_V8/src/run_meta.py",
    "HP_V8/src/test_model_openai.py",
    "transport/src/model_openai.py",
    "transport/src/test_model_openai.py",
}
_DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS = {
    "HP_V8/VERSION.md",
    "HP_V8/src/authorize_ledger_lock_recovery.py",
    "HP_V8/src/run_meta.py",
    "HP_V8/src/test_model_openai.py",
    "docs/active_log.md",
}
_DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS = [
    "run_meta.py",
]
_DEEPSEEK_TRANSPORT_INSPECTOR_STOP_ERROR = (
    "campaign preflight failed before worker launch: "
    "DeepSeek API response incomplete at row 760; "
    "DeepSeek API response incomplete at row 778; "
    "DeepSeek raw request transport audit failed at row 760; "
    "DeepSeek raw request transport audit failed at row 778; "
    "DeepSeek retry evidence invalid at row 760; "
    "DeepSeek retry evidence invalid at row 778; "
    "DeepSeek transport sidecar invalid at row 778"
)
_DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX = (
    "queued resume cannot prove pending sample was never started: "
)
_DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS = (
    "explicit_user_resume_after_deepseek_resume_classifier_fix"
)


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


def warn_best_effort_io(kind, target, exc):
    """Emit a diagnostic without ever becoming a second failure source."""
    try:
        print(
            f"WARNING: {kind} best-effort write failed: {target}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
    except Exception:
        pass


def normalize_snapshot_mode(value, *, default=SNAPSHOT_MODE_ALL):
    if value is None:
        value = default
    if value not in SNAPSHOT_MODES:
        raise ValueError(f"unsupported snapshot_mode: {value!r}")
    return value


def _campaign_config_for_snapshot_compare(config):
    if not isinstance(config, dict):
        return config
    normalized = dict(config)
    normalized["snapshot_mode"] = normalize_snapshot_mode(
        normalized.get("snapshot_mode"), default=SNAPSHOT_MODE_ALL)
    return normalized


def _campaign_configs_match(prior, current):
    return (
        _campaign_config_for_snapshot_compare(prior)
        == _campaign_config_for_snapshot_compare(current)
    )


def _campaign_config_for_storage(current, prior):
    """Keep legacy all-snapshot campaigns byte-compatible on resume."""
    if (isinstance(prior, dict)
            and "snapshot_mode" not in prior
            and _campaign_configs_match(prior, current)):
        stored = dict(current)
        stored.pop("snapshot_mode", None)
        return stored
    return current


def append_jsonl_locked(path, record):
    """Append one durable JSONL row, waiting through ordinary contention.

    ``portalocker.lock`` may raise ``AlreadyLocked`` immediately on Windows.
    A shared campaign ledger is expected to have many short-lived writers, so
    that condition is contention rather than a failed model/transport call.
    ``portalocker.Lock`` retains fail-closed behavior after a bounded wait.
    """
    append_jsonl_records_locked(path, [record])


def append_jsonl_records_locked(path, records):
    """Durably append one ordered cohort behind a single file lock/fsync.

    Callers may use this only when the complete cohort is one durability
    barrier.  API/attempt/outcome events whose individual publication order is
    semantically observable continue to use ``append_jsonl_locked``.
    """
    lines = [
        json.dumps(record, ensure_ascii=False) + "\n"
        for record in records
    ]
    if not lines:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with portalocker.Lock(
        path,
        mode="a",
        timeout=60,
        check_interval=0.05,
        flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        encoding="utf-8",
    ) as f:
        f.write("".join(lines))
        f.flush()
        os.fsync(f.fileno())


def _canonical_record_sha256(record):
    payload = json.dumps(
        record, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _read_stop_record(path):
    deadline = time.monotonic() + 1.0
    while True:
        try:
            with open(path, encoding="utf-8") as handle:
                record = json.load(handle)
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"invalid campaign stop latch: {path}"
                )
            time.sleep(0.05)
        except ValueError as exc:
            raise RuntimeError(
                f"invalid campaign stop latch: {path}"
            ) from exc
    if record is None:
        raise RuntimeError(
            f"invalid campaign stop latch: {path}"
        )
    if (not isinstance(record, dict)
            or record.get("schema") != STOP_CONDITION_SCHEMA
            or not isinstance(record.get("condition"), str)
            or not record.get("condition")):
        raise RuntimeError(f"invalid campaign stop latch: {path}")
    return record


def _read_campaign_stop_unlocked(out_dir):
    records = []
    record_hashes = set()
    path = os.path.join(out_dir, "campaign_stop.json")
    if os.path.exists(path):
        record = _read_stop_record(path)
        records.append(record)
        record_hashes.add(_canonical_record_sha256(record))
    emergency_dir = os.path.join(out_dir, EMERGENCY_STOP_DIRECTORY)
    if os.path.isdir(emergency_dir):
        try:
            names = sorted(
                name for name in os.listdir(emergency_dir)
                if name.endswith(".json"))
        except OSError as exc:
            raise RuntimeError(
                f"invalid emergency campaign stop directory: "
                f"{emergency_dir}"
            ) from exc
        for name in names:
            record = _read_stop_record(os.path.join(
                emergency_dir, name))
            digest = _canonical_record_sha256(record)
            if digest not in record_hashes:
                records.append(record)
                record_hashes.add(digest)
    return records


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


@contextmanager
def _campaign_stop_publication_lock(out_dir):
    """Serialize emergency stop publication with parent-loss recovery."""
    os.makedirs(out_dir, exist_ok=True)
    with portalocker.Lock(
        os.path.join(out_dir, ".campaign_stop_publication.lock"),
        mode="a+",
        timeout=60,
        check_interval=0.05,
        flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        encoding="utf-8",
    ):
        yield


def _serialize_campaign_stop_publication(function):
    @functools.wraps(function)
    def wrapped(out_dir, *args, **kwargs):
        with _campaign_stop_publication_lock(os.path.abspath(out_dir)):
            return function(out_dir, *args, **kwargs)
    return wrapped


@_serialize_campaign_stop_publication
def record_emergency_campaign_stop_condition(
        out_dir, condition, **details):
    """Publish parent-loss evidence without the shared metadata lock."""
    if not isinstance(condition, str) or not condition:
        raise ValueError("campaign stop condition must be a non-empty string")
    reserved = {
        "schema", "created_at", "condition", "worker_launch_id",
        "worker_pid",
    }
    overlap = reserved & set(details)
    if overlap:
        raise ValueError(
            f"campaign stop details override reserved fields: "
            f"{sorted(overlap)}"
        )
    record = {
        "schema": STOP_CONDITION_SCHEMA,
        "created_at": _iso_with_timezone(_aware_now()),
        "condition": condition,
        "worker_launch_id": os.environ.get(
            "ANCHORPATCH_WORKER_LAUNCH_ID"),
        "worker_pid": os.getpid(),
    }
    record.update(details)
    active_path = os.path.join(
        os.path.abspath(out_dir), "active_worker_set.json")

    def _worker_no_longer_active():
        if condition != "dispatcher_process_lost":
            return False
        try:
            with open(active_path, encoding="utf-8") as handle:
                active = json.load(handle)
        except (OSError, ValueError):
            return False
        if not isinstance(active, dict):
            return False
        workers = active.get("workers")
        if (active.get("schema") != "anchorpatch.active_worker_set/1"
                or not isinstance(workers, dict)):
            return False
        return not (
            active.get("dispatcher_pid") == record.get("dispatcher_pid")
            and active.get("dispatcher_instance_id")
            == record.get("dispatcher_instance_id")
            and record.get("worker_launch_id") in workers
        )

    if _worker_no_longer_active():
        return record
    directory = os.path.join(
        os.path.abspath(out_dir), EMERGENCY_STOP_DIRECTORY)
    os.makedirs(directory, exist_ok=True)
    stem = (
        f"{condition}.{os.getpid()}."
        f"{uuid.uuid4().hex}"
    )
    final_path = os.path.join(directory, stem + ".json")
    temp_path = os.path.join(directory, stem + ".tmp")
    try:
        with open(
                temp_path, "x", encoding="utf-8", newline="") as handle:
            json.dump(record, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        deadline = time.monotonic() + 0.4
        while True:
            try:
                os.replace(temp_path, final_path)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)
        # Make the ordinary monitor/recovery path visible without taking or
        # waiting on the shared metadata lock. A hard link is atomic and never
        # overwrites a prior first-writer stop condition.
        canonical_path = os.path.join(
            os.path.abspath(out_dir), "campaign_stop.json")
        canonical_published = False

        def _canonical_covers_this_record():
            try:
                return (
                    _canonical_record_sha256(
                        _read_stop_record(canonical_path))
                    == _canonical_record_sha256(record)
                )
            except (OSError, RuntimeError):
                return False

        def _canonical_prelaunch_stop_covers_this_worker():
            try:
                canonical = _read_stop_record(canonical_path)
            except (OSError, RuntimeError):
                return False
            return (
                canonical.get("condition") == "dispatcher_process_lost"
                and canonical.get("registered_prelaunch_only") is True
                and canonical.get("dispatcher_pid")
                == record.get("dispatcher_pid")
                and canonical.get("dispatcher_instance_id")
                == record.get("dispatcher_instance_id")
                and record.get("worker_launch_id")
                in (
                    canonical.get("registered_worker_launch_ids") or [])
            )

        if _worker_no_longer_active():
            try:
                os.unlink(final_path)
            except FileNotFoundError:
                pass
            return record
        try:
            os.link(final_path, canonical_path)
            canonical_published = True
        except FileExistsError:
            canonical_published = (
                _canonical_covers_this_record()
                or _canonical_prelaunch_stop_covers_this_worker()
            )
        except OSError:
            # Fallback for filesystems without hard links. ``x`` preserves the
            # first-writer rule; a concurrent writer either owns the canonical
            # path or observes FileExistsError.
            try:
                with open(
                        canonical_path, "x",
                        encoding="utf-8", newline="") as handle:
                    json.dump(record, handle, ensure_ascii=False)
                    handle.flush()
                    os.fsync(handle.fileno())
                canonical_published = True
            except FileExistsError:
                canonical_published = (
                    _canonical_covers_this_record()
                    or _canonical_prelaunch_stop_covers_this_worker()
                )
        if (_worker_no_longer_active()
                and _canonical_covers_this_record()):
            deadline = time.monotonic() + 0.4
            while True:
                try:
                    os.unlink(canonical_path)
                    break
                except FileNotFoundError:
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(0.02)
        if canonical_published:
            # Existing recovery tools archive campaign_stop.json. Do not leave
            # a second active name that would re-latch the authorized resume.
            deadline = time.monotonic() + 1.0
            while True:
                try:
                    os.unlink(final_path)
                    break
                except FileNotFoundError:
                    break
                except OSError as exc:
                    if (getattr(exc, "winerror", None) not in {5, 32, 33}
                            or time.monotonic() >= deadline):
                        break
                    time.sleep(0.02)
    finally:
        try:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        except OSError:
            pass
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
        deadline = time.monotonic() + 1.0
        while True:
            try:
                with open(active_path, encoding="utf-8") as handle:
                    active = json.load(handle)
                break
            except OSError:
                # Windows scanners/readers can briefly deny access even
                # though writers publish by atomic replace.  A single sharing
                # failure is not authorization drift; retry briefly, then
                # retain fail-closed behavior.
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
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
    """Check the hot pre-call campaign guards.

    This runs directly in the provider-call path, so it stays limited to the
    durable stop latch and the dispatcher's active-worker authorization.  Git
    identity and task-plan immutability remain enforced at startup,
    dispatcher authorization, and final inspection.
    """
    _raise_if_campaign_stopped(out_dir)
    enforce_active_worker_authorization(out_dir, sample_id)
    # Close the latch-check window as far as a file-based latch permits. Calls
    # already in flight may finish, but no later semantic call proceeds after
    # another worker durably sets the latch.
    _raise_if_campaign_stopped(out_dir)


def enforce_campaign_runtime_guards(out_dir, sample_id):
    """Public hot guard for stop-latch and active-worker authorization."""
    _enforce_pre_call_campaign_guards(out_dir, sample_id)


def write_json_atomic(path, record):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        deadline = time.monotonic() + 60
        while True:
            try:
                os.replace(tmp, path)
                break
            except OSError as exc:
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
        except (OSError, portalocker.exceptions.LockException):
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


def _transport_sidecar_facts(path):
    if not path or not os.path.isfile(path) or os.path.getsize(path) == 0:
        return {
            "transport_sidecar_sha256": None,
            "transport_sidecar_size_bytes": None,
            "transport_sidecar_record_count": None,
        }
    digest = hashlib.sha256()
    record_count = 0
    with io.open(path, "rb") as handle:
        for line in handle:
            digest.update(line)
            if line.strip():
                record_count += 1
    return {
        "transport_sidecar_sha256": digest.hexdigest(),
        "transport_sidecar_size_bytes": os.path.getsize(path),
        "transport_sidecar_record_count": record_count,
    }


def _read_deepseek_transport_sidecar(path):
    rows = _read_jsonl_records_with_retry(path)
    compact = bool(
        rows
        and rows[0].get("record_type") == "transport_header"
        and rows[0].get("transport_event_schema")
        == DEEPSEEK_COMPACT_EVENT_SCHEMA
    )
    header = rows[0] if compact else None
    body = rows[1:] if compact else rows
    starts = [
        row for row in body if row.get("record_type") == "attempt_start"
    ]
    ends = [
        row for row in body if row.get("record_type") == "attempt_end"
    ]
    summaries = [
        row for row in body if row.get("record_type") == "stream_summary"
    ]
    if compact:
        stream_attempt_indexes = {
            row.get("attempt_index")
            for row in body
            if (
                row.get("record_type") == "stream_checkpoint"
                and row.get("checkpoint") == "first_chunk"
            ) or (
                row.get("record_type") == "stream_summary"
                and _exact_nonnegative_int(row.get("stream_event_count"))
                and row.get("stream_event_count") > 0
            )
        }
        generation_attempt_indexes = {
            row.get("attempt_index")
            for row in body
            if (
                row.get("record_type") == "stream_checkpoint"
                and row.get("checkpoint") == "generation_started"
            ) or (
                row.get("record_type") == "stream_summary"
                and row.get("generation_delta_seen") is True
            )
        }
    else:
        stream_attempt_indexes = {
            row.get("attempt_index")
            for row in body
            if row.get("record_type") == "sdk_stream_event"
        }
        generation_attempt_indexes = set(stream_attempt_indexes)
    return {
        "rows": rows,
        "body": body,
        "compact": compact,
        "header": header,
        "identity_rows": [header] if compact else rows,
        "starts": starts,
        "ends": ends,
        "summaries": summaries,
        "stream_attempt_indexes": stream_attempt_indexes,
        "generation_attempt_indexes": generation_attempt_indexes,
    }


def _validate_deepseek_compact_records(
        rows, *, expected_linkage=None, allow_open_final=False):
    """Validate the one canonical /6 grammar for inspector and recovery."""
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("compact DeepSeek sidecar is empty")
    header = rows[0]
    header_keys = {
        "transport_event_schema", "record_type", "transport_revision",
        "worker_launch_id", "worker_pid", "sample", "method", "rt_index",
        "direction", "call_id",
    }
    if (not isinstance(header, dict)
            or set(header) != header_keys
            or header.get("transport_event_schema")
            != DEEPSEEK_COMPACT_EVENT_SCHEMA
            or header.get("record_type") != "transport_header"
            or header.get("transport_revision")
            != DEEPSEEK_COMPACT_TRANSPORT_REVISION
            or not isinstance(header.get("worker_launch_id"), str)
            or not header.get("worker_launch_id")
            or not isinstance(header.get("worker_pid"), int)
            or isinstance(header.get("worker_pid"), bool)
            or header.get("worker_pid") <= 0
            or not isinstance(header.get("sample"), str)
            or not header.get("sample")
            or header.get("method") not in {"hybridpatch", "fullrewrite"}
            or not isinstance(header.get("rt_index"), int)
            or isinstance(header.get("rt_index"), bool)
            or header.get("rt_index") <= 0
            or header.get("direction") not in {"forward", "backward"}
            or not isinstance(header.get("call_id"), str)
            or not header.get("call_id")):
        raise RuntimeError("compact DeepSeek header is invalid")
    if expected_linkage is not None and any(
            header.get(key) != value
            for key, value in expected_linkage.items()):
        raise RuntimeError("compact DeepSeek header linkage mismatch")

    summary_keys = {
        "record_type", "attempt_index", "stream_event_count",
        "stream_event_canonical_bytes", "stream_event_sha256",
        "text_delta_count", "text_delta_utf8_bytes",
        "reasoning_delta_count", "reasoning_delta_utf8_bytes",
        "tool_delta_count", "tool_delta_utf8_bytes", "message_start_seen",
        "message_stop_seen", "final_usage_seen", "generation_delta_seen",
        "terminal_sequence_valid", "finish_reason", "usage",
    }
    checkpoint_order = {
        "first_chunk": 1,
        "generation_started": 2,
        "finish_seen": 3,
        "usage_seen": 4,
    }

    def _valid_usage(value):
        return (
            isinstance(value, dict)
            and all(
                _exact_nonnegative_int(value.get(key))
                for key in (
                    "prompt_tokens", "completion_tokens", "total_tokens")
            )
            and value["total_tokens"]
            == value["prompt_tokens"] + value["completion_tokens"]
        )

    def _successful_summary_is_complete(
            summary, event_count, checkpoint_sequence):
        finish_reason = summary.get("finish_reason")
        return (
            event_count > 0
            and summary.get("message_start_seen") is True
            and summary.get("message_stop_seen") is True
            and summary.get("final_usage_seen") is True
            and summary.get("terminal_sequence_valid") is True
            and isinstance(finish_reason, str)
            and bool(finish_reason.strip())
            and _valid_usage(summary.get("usage"))
            and checkpoint_sequence == sorted(checkpoint_sequence)
        )

    def _validate_checkpoint_payload(name, checkpoint):
        if (name == "first_chunk"
                and checkpoint["stream_event_count"] != 1):
            raise RuntimeError(
                "compact DeepSeek first-chunk checkpoint is invalid")
        if name == "generation_started":
            flags = [
                checkpoint.get("text_delta_seen"),
                checkpoint.get("thinking_delta_seen"),
                checkpoint.get("tool_delta_seen"),
            ]
            if (any(not isinstance(value, bool) for value in flags)
                    or not any(flags)):
                raise RuntimeError(
                    "compact DeepSeek generation checkpoint is invalid")
        if name == "finish_seen":
            finish_reason = checkpoint.get("finish_reason")
            if (not isinstance(finish_reason, str)
                    or not finish_reason.strip()):
                raise RuntimeError(
                    "compact DeepSeek finish checkpoint is invalid")
        if name == "usage_seen" and not _valid_usage(
                checkpoint.get("usage")):
            raise RuntimeError(
                "compact DeepSeek usage checkpoint is invalid")

    closed_attempts = []
    retry_budget_attempt_index = 0
    cursor = 1
    expected_index = 1
    open_attempt = (
        {
            "attempt_index": 1,
            "start": None,
            "checkpoints": {},
            "summary": None,
        }
        if allow_open_final and len(rows) == 1 else None
    )
    while cursor < len(rows):
        if (closed_attempts
                and closed_attempts[-1]["attempt"].get("status")
                != "retryable_error"):
            raise RuntimeError(
                "compact DeepSeek terminal attempt has trailing records")
        start = rows[cursor]
        cursor += 1
        if (not isinstance(start, dict)
                or set(start) != {
                    "record_type", "attempt_index", "attempt_kind"}
                or start.get("record_type") != "attempt_start"
                or start.get("attempt_index") != expected_index
                or start.get("attempt_kind") != (
                    "openai_compatible_initial"
                    if expected_index == 1
                    else "openai_compatible_retry")):
            raise RuntimeError("compact DeepSeek attempt_start is invalid")

        checkpoints = {}
        checkpoint_sequence = []
        prior_event_count = 0
        while (cursor < len(rows)
               and rows[cursor].get("record_type") == "stream_checkpoint"):
            checkpoint = rows[cursor]
            cursor += 1
            name = checkpoint.get("checkpoint")
            order = checkpoint_order.get(name)
            allowed_keys = {
                "record_type", "attempt_index", "checkpoint",
                "stream_event_count",
            }
            if name == "generation_started":
                allowed_keys.update({
                    "text_delta_seen", "thinking_delta_seen",
                    "tool_delta_seen",
                })
            elif name == "finish_seen":
                allowed_keys.add("finish_reason")
            elif name == "usage_seen":
                allowed_keys.add("usage")
            event_count = checkpoint.get("stream_event_count")
            if (order is None or name in checkpoints
                    or set(checkpoint) != allowed_keys
                    or checkpoint.get("attempt_index") != expected_index
                    or not _exact_nonnegative_int(event_count)
                    or event_count < prior_event_count):
                raise RuntimeError("compact DeepSeek checkpoint is invalid")
            checkpoints[name] = checkpoint
            checkpoint_sequence.append(order)
            prior_event_count = event_count
            _validate_checkpoint_payload(name, checkpoint)

        if checkpoint_sequence != sorted(checkpoint_sequence):
            raise RuntimeError(
                "compact DeepSeek checkpoint order is invalid")

        if (cursor >= len(rows)
                or rows[cursor].get("record_type") != "stream_summary"):
            if allow_open_final and cursor == len(rows):
                open_attempt = {
                    "attempt_index": expected_index,
                    "start": start,
                    "checkpoints": checkpoints,
                    "summary": None,
                }
                break
            raise RuntimeError("compact DeepSeek stream_summary is missing")
        summary = rows[cursor]
        cursor += 1
        if (set(summary) != summary_keys
                or summary.get("attempt_index") != expected_index):
            raise RuntimeError("compact DeepSeek stream_summary is invalid")
        for key in (
                "stream_event_count", "stream_event_canonical_bytes",
                "text_delta_count", "text_delta_utf8_bytes",
                "reasoning_delta_count", "reasoning_delta_utf8_bytes",
                "tool_delta_count", "tool_delta_utf8_bytes"):
            if not _exact_nonnegative_int(summary.get(key)):
                raise RuntimeError("compact DeepSeek summary counter is invalid")
        for key in (
                "message_start_seen", "message_stop_seen", "final_usage_seen",
                "generation_delta_seen", "terminal_sequence_valid"):
            if not isinstance(summary.get(key), bool):
                raise RuntimeError("compact DeepSeek summary state is invalid")
        stream_sha = summary.get("stream_event_sha256")
        finish_reason = summary.get("finish_reason")
        event_count = summary["stream_event_count"]
        canonical_bytes = summary["stream_event_canonical_bytes"]
        delta_counts = [
            summary["text_delta_count"], summary["reasoning_delta_count"],
            summary["tool_delta_count"],
        ]
        delta_bytes = [
            summary["text_delta_utf8_bytes"],
            summary["reasoning_delta_utf8_bytes"],
            summary["tool_delta_utf8_bytes"],
        ]
        if (not isinstance(stream_sha, str)
                or not re.fullmatch(r"[0-9a-f]{64}", stream_sha)
                or summary["message_start_seen"] is not (event_count > 0)
                or summary["message_stop_seen"] is not (
                    isinstance(finish_reason, str)
                    and bool(finish_reason.strip()))
                or summary["generation_delta_seen"] is not any(
                    count > 0 for count in delta_counts)
                or (
                    summary["terminal_sequence_valid"]
                    and (
                        summary["message_stop_seen"] is not True
                        or summary["final_usage_seen"] is not True
                    )
                )
                or any(
                    (count == 0) is not (byte_count == 0)
                    for count, byte_count in zip(delta_counts, delta_bytes)
                )
                or (event_count == 0) is not (canonical_bytes == 0)
                or (event_count == 0) is not (
                    stream_sha == hashlib.sha256(b"").hexdigest())):
            raise RuntimeError("compact DeepSeek summary invariants are invalid")
        expected_checkpoints = {
            "first_chunk": event_count > 0,
            "generation_started": summary["generation_delta_seen"],
            "finish_seen": summary["message_stop_seen"],
            "usage_seen": summary["final_usage_seen"],
        }
        if (any(
                (name in checkpoints) is not expected
                for name, expected in expected_checkpoints.items())
                or (checkpoints and any(
                    checkpoint["stream_event_count"] > event_count
                    for checkpoint in checkpoints.values()
                ))
                or checkpoints.get("first_chunk", {}).get(
                    "stream_event_count") != (1 if event_count else None)):
            raise RuntimeError("compact DeepSeek checkpoint coverage is invalid")
        generation = checkpoints.get("generation_started")
        if generation is not None:
            flags = [
                generation.get("text_delta_seen"),
                generation.get("thinking_delta_seen"),
                generation.get("tool_delta_seen"),
            ]
            if (any(not isinstance(value, bool) for value in flags)
                    or not any(flags)
                    or (flags[0] and summary["text_delta_count"] == 0)
                    or (flags[1] and summary["reasoning_delta_count"] == 0)
                    or (flags[2] and summary["tool_delta_count"] == 0)):
                raise RuntimeError(
                    "compact DeepSeek generation checkpoint is invalid")
        if ("finish_seen" in checkpoints
                and checkpoints["finish_seen"].get("finish_reason")
                != finish_reason):
            raise RuntimeError("compact DeepSeek finish checkpoint is invalid")
        usage = summary.get("usage")
        if ("usage_seen" in checkpoints
                and checkpoints["usage_seen"].get("usage") != usage):
            raise RuntimeError("compact DeepSeek usage checkpoint is invalid")
        if summary["final_usage_seen"]:
            if not _valid_usage(usage):
                raise RuntimeError("compact DeepSeek terminal usage is invalid")
        elif usage is not None:
            raise RuntimeError("compact DeepSeek nonterminal usage is invalid")

        if (cursor >= len(rows)
                or rows[cursor].get("record_type") != "attempt_end"):
            if allow_open_final and cursor == len(rows):
                open_attempt = {
                    "attempt_index": expected_index,
                    "start": start,
                    "checkpoints": checkpoints,
                    "summary": summary,
                }
                break
            raise RuntimeError("compact DeepSeek attempt_end is missing")
        end_row = rows[cursor]
        cursor += 1
        attempt = end_row.get("attempt")
        if (set(end_row) != {"record_type", "attempt_index", "attempt"}
                or end_row.get("attempt_index") != expected_index
                or not isinstance(attempt, dict)
                or attempt.get("attempt_index") != expected_index):
            raise RuntimeError("compact DeepSeek attempt_end is invalid")
        mirrored_keys = (
                "stream_event_count", "stream_event_canonical_bytes",
                "stream_event_sha256", "text_delta_count",
                "text_delta_utf8_bytes", "reasoning_delta_count",
                "reasoning_delta_utf8_bytes", "tool_delta_count",
                "tool_delta_utf8_bytes", "message_start_seen",
                "message_stop_seen", "final_usage_seen",
                "generation_delta_seen", "terminal_sequence_valid",
                "finish_reason")
        for key in mirrored_keys:
            if key not in attempt or attempt.get(key) != summary.get(key):
                raise RuntimeError(
                    "compact DeepSeek summary/attempt mismatch")
        status = attempt.get("status")
        if status == "success":
            if (attempt.get("http_status") != 200
                    or attempt.get("error_type") is not None
                    or attempt.get("stream_complete") is not True
                    or not _successful_summary_is_complete(
                        summary, event_count, checkpoint_sequence)):
                raise RuntimeError(
                    "compact DeepSeek successful attempt is invalid")
        elif status in {"retryable_error", "fatal_error"}:
            free_503 = (
                attempt.get("http_status") == 503
                and summary["generation_delta_seen"] is False
            )
            if not free_503:
                retry_budget_attempt_index += 1
            if (attempt.get("stream_complete") is not False
                    or summary["terminal_sequence_valid"] is not False
                    or not isinstance(attempt.get("error_type"), str)
                    or not attempt.get("error_type")
                    or not isinstance(
                        attempt.get("retry_budget_consumed"), bool)
                    or attempt.get("retry_budget_consumed")
                    is not (not free_503)
                    or not _exact_nonnegative_int(
                        attempt.get("retry_budget_attempt_index"))
                    or attempt.get("retry_budget_attempt_index")
                    != retry_budget_attempt_index):
                raise RuntimeError(
                    "compact DeepSeek failed attempt is invalid")
        else:
            raise RuntimeError("compact DeepSeek attempt status is invalid")
        closed_attempts.append({
            "start": start,
            "checkpoints": checkpoints,
            "summary": summary,
            "end": end_row,
            "attempt": attempt,
        })
        expected_index += 1

    if cursor != len(rows):
        raise RuntimeError("compact DeepSeek sidecar has trailing records")
    if not allow_open_final and (not closed_attempts or open_attempt is not None):
        raise RuntimeError("compact DeepSeek sidecar is not closed")
    if open_attempt is not None:
        if (open_attempt["summary"] is not None
                and open_attempt["summary"][
                    "terminal_sequence_valid"] is True):
            open_summary = open_attempt["summary"]
            open_checkpoint_sequence = [
                checkpoint_order[name]
                for name in open_attempt["checkpoints"]
            ]
            if not _successful_summary_is_complete(
                    open_summary,
                    open_summary["stream_event_count"],
                    open_checkpoint_sequence):
                raise RuntimeError(
                    "compact DeepSeek complete summary is invalid")
            recovery_state = "complete_unpublished_response"
        else:
            recovery_state = (
                "pre_attempt_no_post"
                if open_attempt["start"] is None else "open_attempt"
            )
    elif closed_attempts[-1]["attempt"].get("status") == "success":
        recovery_state = "complete_unpublished_response"
    else:
        recovery_state = "retry_or_error_unpublished"
    return {
        "header": header,
        "closed_attempts": closed_attempts,
        "open_attempt": open_attempt,
        "attempt_start_count": len(closed_attempts) + int(
            open_attempt is not None and open_attempt["start"] is not None),
        "attempt_end_count": len(closed_attempts),
        "recovery_state": recovery_state,
    }


def _validate_deepseek_linear_records(
        rows, *, expected_linkage, expected_attempts=None,
        allow_open_final=False):
    """Validate frozen /4-/5 linear event/1 sidecars without upgrading them."""
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("linear DeepSeek sidecar is empty")
    if (not isinstance(expected_linkage, dict)
            or any(
                not isinstance(row, dict)
                or row.get("transport_event_schema")
                != "anchorpatch.transport_event/1"
                or any(
                    row.get(key) != value
                    for key, value in expected_linkage.items()
                )
                for row in rows
            )):
        raise RuntimeError("linear DeepSeek sidecar linkage mismatch")

    closed_attempts = []
    cursor = 0
    expected_index = 1
    open_attempt = None
    while cursor < len(rows):
        if (closed_attempts
                and closed_attempts[-1]["attempt"].get("status")
                != "retryable_error"):
            raise RuntimeError(
                "linear DeepSeek terminal attempt has trailing records")
        start = rows[cursor]
        cursor += 1
        if (start.get("record_type") != "attempt_start"
                or start.get("attempt_index") != expected_index):
            raise RuntimeError("linear DeepSeek attempt_start is invalid")
        stream_events = []
        while (cursor < len(rows)
               and rows[cursor].get("record_type") == "sdk_stream_event"):
            event = rows[cursor]
            cursor += 1
            if event.get("attempt_index") != expected_index:
                raise RuntimeError(
                    "linear DeepSeek stream attempt index is invalid")
            stream_events.append(event)
        if (cursor >= len(rows)
                or rows[cursor].get("record_type") != "attempt_end"):
            if allow_open_final and cursor == len(rows):
                open_attempt = {
                    "attempt_index": expected_index,
                    "start": start,
                    "stream_events": stream_events,
                }
                break
            raise RuntimeError("linear DeepSeek attempt_end is missing")
        end = rows[cursor]
        cursor += 1
        attempt = end.get("attempt")
        if (end.get("attempt_index") != expected_index
                or not isinstance(attempt, dict)
                or attempt.get("attempt_index") != expected_index):
            raise RuntimeError("linear DeepSeek attempt_end is invalid")
        status = attempt.get("status")
        if status == "success":
            if (attempt.get("http_status") != 200
                    or attempt.get("stream_complete") is not True
                    or not stream_events):
                raise RuntimeError(
                    "linear DeepSeek successful attempt is invalid")
        elif status in {"retryable_error", "fatal_error"}:
            if (not isinstance(attempt.get("error_type"), str)
                    or not attempt.get("error_type")):
                raise RuntimeError("linear DeepSeek failed attempt is invalid")
            if attempt.get("message_start_seen") is True and not stream_events:
                raise RuntimeError(
                    "linear DeepSeek failed stream evidence is missing")
        else:
            raise RuntimeError("linear DeepSeek attempt status is invalid")
        closed_attempts.append({
            "start": start,
            "stream_events": stream_events,
            "end": end,
            "attempt": attempt,
        })
        expected_index += 1

    if cursor != len(rows):
        raise RuntimeError("linear DeepSeek sidecar has trailing records")
    if not allow_open_final and (not closed_attempts or open_attempt is not None):
        raise RuntimeError("linear DeepSeek sidecar is not closed")
    attempts = [item["attempt"] for item in closed_attempts]
    if expected_attempts is not None and attempts != expected_attempts:
        raise RuntimeError("linear DeepSeek attempts mismatch")
    recovery_state = (
        "open_attempt" if open_attempt is not None
        else (
            "complete_unpublished_response"
            if closed_attempts[-1]["attempt"].get("status") == "success"
            else "retry_or_error_unpublished"
        )
    )
    return {
        "closed_attempts": closed_attempts,
        "open_attempt": open_attempt,
        "attempt_start_count": len(closed_attempts) + int(
            open_attempt is not None),
        "attempt_end_count": len(closed_attempts),
        "recovery_state": recovery_state,
    }


_GENERATE_POSITIONAL_PARAMETERS = (
    "messages", "model", "timeout", "max_retries", "temperature", "is_json",
    "return_metadata", "max_tokens", "variables", "instance",
    "thinking_mode", "reasoning_effort", "call_kind", "_raw_event_sink",
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
    "reasoning_effort": None,
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
        "reasoning_effort": bound["reasoning_effort"],
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
    retry_budget_index = attempts[-1].get("retry_budget_attempt_index")
    if (record.get("transport") in {
            "openai_sdk_nonstream", "openai_sdk_stream"}
            and attempts[-1].get("retry_budget_consumed") is True
            and isinstance(retry_budget_index, int)
            and not isinstance(retry_budget_index, bool)
            and retry_budget_index >= 3):
        return True
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

    def _dump_raw_io(
            self, call_id, meta, *, stream_events_already_saved=False):
        """Write the complete raw API log for one call: the request that was
        sent and the full raw response. Raw stream events are only duplicated
        when no critical transport sidecar already owns them. Returns a dict of
        the paths written. Best-effort — a logging failure never breaks the
        run."""
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
            if events and not stream_events_already_saved:
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
            try:
                append_jsonl_locked(
                    os.path.join(self.out_dir, "api_anomalies.jsonl"),
                    record,
                )
            except Exception as exc:
                warn_best_effort_io(
                    "api_anomalies",
                    os.path.join(self.out_dir, "api_anomalies.jsonl"),
                    exc,
                )

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
        try:
            from model_openai import model_runtime_config
            provider_runtime = model_runtime_config(
                requested_model,
                max_tokens=kwargs.get("max_tokens"),
                thinking_mode=kwargs.get("thinking_mode") or "adaptive",
                reasoning_effort=kwargs.get("reasoning_effort"),
            )
        except Exception as exc:
            raise RuntimeError(
                "failed to resolve provider transport runtime before provider "
                "POST"
            ) from exc
        transport_path = None
        transport_fh = None
        transport_header_written = False
        semantic_lock_fh = None
        transport_lock = threading.Lock()
        progress_logged = set()
        provider_post_started = False
        transport_preflight_error = None
        secrets = [
            value for value in (
                os.environ.get("OPENCODE_API_KEY"),
                os.environ.get("OPENCODE_GO_API_KEY"),
                os.environ.get("OPENAI_API_KEY"),
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
            prior_sink = kwargs.get("_raw_event_sink")
            if provider_runtime.get("provider") == "opencode_zen":
                transport_path = self._raw_path(
                    call_id, "transport.jsonl")
                transport_fh = open(
                    transport_path, "w", encoding="utf-8", newline="")

            def _openai_compatible_sink(payload):
                nonlocal provider_post_started, transport_header_written
                if prior_sink is not None:
                    prior_sink(payload)
                if payload.get("record_type") == "attempt_start":
                    enforce_campaign_runtime_guards(
                        self.out_dir, self.sample_id)
                if transport_fh is not None:
                    payload = dict(payload)
                    if (provider_runtime.get("transport_revision")
                            == DEEPSEEK_COMPACT_TRANSPORT_REVISION):
                        if payload.get("record_type") not in {
                                "attempt_start", "stream_checkpoint",
                                "stream_summary", "attempt_end"}:
                            raise RuntimeError(
                                "compact DeepSeek transport received an "
                                "unsupported event")
                        if not transport_header_written:
                            header = {
                                "transport_event_schema":
                                DEEPSEEK_COMPACT_EVENT_SCHEMA,
                                "record_type": "transport_header",
                                "transport_revision":
                                DEEPSEEK_COMPACT_TRANSPORT_REVISION,
                                "worker_launch_id": self.worker_launch_id,
                                "worker_pid": os.getpid(),
                                "sample": self.sample_id,
                                "method": self.method,
                                "rt_index": self.rt_index,
                                "direction": self.direction,
                                "call_id": call_id,
                            }
                            transport_fh.write(
                                json.dumps(header, ensure_ascii=False) + "\n")
                            transport_fh.flush()
                            transport_header_written = True
                    else:
                        payload.update({
                            "transport_event_schema":
                            "anchorpatch.transport_event/1",
                            "worker_launch_id": self.worker_launch_id,
                            "worker_pid": os.getpid(),
                            "sample": self.sample_id,
                            "method": self.method,
                            "rt_index": self.rt_index,
                            "direction": self.direction,
                            "call_id": call_id,
                        })
                    line = json.dumps(
                        payload, ensure_ascii=False, default=str)
                    for secret in secrets:
                        line = line.replace(secret, "<redacted-key>")
                    with transport_lock:
                        transport_fh.write(line + "\n")
                        transport_fh.flush()
                        if payload.get("record_type") == "attempt_end":
                            os.fsync(transport_fh.fileno())
                if payload.get("record_type") == "attempt_start":
                    provider_post_started = True

            _openai_compatible_sink._anchorpatch_critical = True
            kwargs["_raw_event_sink"] = _openai_compatible_sink

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
            transport_facts = _transport_sidecar_facts(transport_path)
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
                **transport_facts,
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
                "reasoning_effort": provider_runtime.get(
                    "reasoning_effort"),
                "provider_called": provider_post_started,
                "response_replayed": False,
                "replayed_from_call_id": None,
                "max_response_slots": provider_runtime.get("max_response_slots"),
                "response_slots_used": failure_state.get("response_slots_used"),
                "max_transient_failures": provider_runtime.get("max_transient_failures"),
                "transient_failure_count": failure_state.get("transient_failure_count"),
                "http_attempts_used": (
                    failure_state.get("http_attempts_used")
                    if str(requested_model).lower().startswith("minimax-m3")
                    else (len(attempts) or None)
                ),
                "max_retries": bound_arguments.get("max_retries"),
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
        transport_facts = _transport_sidecar_facts(transport_path)

        meta = out if isinstance(out, dict) else {}
        raw = meta.get("message") if isinstance(out, dict) else str(out)
        raw_path = self._raw_path(call_id)
        with open(raw_path, "w", encoding="utf-8", newline="") as f:
            f.write(raw if isinstance(raw, str) else str(raw))
        transport_saved_path = (
            os.path.abspath(transport_path)
            if transport_path and os.path.getsize(transport_path) else None
        )
        # Complete raw API log. A critical DeepSeek sidecar already owns the
        # stream evidence (compact summaries in /6, linear events in /4-/5), so
        # never write a second .sse.jsonl copy of the same transport evidence.
        raw_io_paths = self._dump_raw_io(
            call_id,
            meta,
            stream_events_already_saved=bool(transport_saved_path),
        )

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
                transport_saved_path or raw_io_paths.get("sse")
            ),
            **transport_facts,
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
            "reasoning_effort": meta.get("reasoning_effort"),
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
    try:
        append_jsonl_locked(os.path.join(out_dir, "api_anomalies.jsonl"), record)
    except Exception as exc:
        warn_best_effort_io(
            "api_anomalies", os.path.join(out_dir, "api_anomalies.jsonl"),
            exc,
        )
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


def _file_prefix_matches(path, evidence):
    if (not isinstance(evidence, dict)
            or not isinstance(evidence.get("byte_count"), int)
            or isinstance(evidence.get("byte_count"), bool)
            or evidence["byte_count"] <= 0
            or not isinstance(evidence.get("sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", evidence["sha256"])):
        return False
    remaining = evidence["byte_count"]
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as handle:
            while remaining:
                chunk = handle.read(min(1024 * 1024, remaining))
                if not chunk:
                    return False
                digest.update(chunk)
                remaining -= len(chunk)
    except OSError:
        return False
    return digest.hexdigest() == evidence["sha256"]


def _jsonl_first_suffix_record_matches(path, evidence, expected_record):
    if not _file_prefix_matches(path, evidence):
        return False
    expected = (
        json.dumps(
            expected_record, ensure_ascii=False).encode("utf-8")
        + b"\n"
    )
    try:
        with open(path, "rb") as handle:
            handle.seek(evidence["byte_count"])
            tail = handle.read()
    except OSError:
        return False
    return tail.startswith(expected)


def _deepseek_initial_recovery_witness_matches(
        out_dir, record, authorization_sha256):
    prefixes = record.get(
        "deepseek_transport_disconnect_initial_prefixes")
    if (not isinstance(prefixes, dict)
            or set(prefixes) != {"dispatch_log.jsonl"}):
        return False
    expected = {
        "event": (
            "user_authorized_deepseek_transport_disconnect_retry_recovery"),
        "created_at": record.get("created_at"),
        "campaign_recovery_authorization_id": record.get(
            "authorization_id"),
        "campaign_recovery_authorization_sha256": authorization_sha256,
        "deepseek_transport_disconnect_retry_samples": record.get(
            "deepseek_transport_disconnect_retry_samples"),
        "incident_api_rows": len(record.get("incident_api_rows") or []),
        "prior_git_commit": record.get("prior_git_commit"),
        "recovery_git_commit": record.get("recovery_git_commit"),
    }
    rows = [
        row for row in _read_jsonl_records_with_retry(
            os.path.join(out_dir, "dispatch_log.jsonl"))
        if row.get("event") == expected["event"]
        and row.get("campaign_recovery_authorization_id")
        == record.get("authorization_id")
    ]
    return (
        len(rows) == 1
        and rows[0] == expected
        and _jsonl_first_suffix_record_matches(
            os.path.join(out_dir, "dispatch_log.jsonl"),
            prefixes["dispatch_log.jsonl"],
            expected,
        )
    )


def _run_metadata_recovery_identity(record):
    """Hash immutable metadata fields across a controlled campaign resume."""
    if not isinstance(record, dict):
        raise RuntimeError("run metadata recovery row is invalid")
    identity = dict(record)
    identity.pop("finished_at", None)
    identity.pop("task_plans", None)
    return _canonical_record_sha256(identity)


def _capture_run_metadata_recovery_snapshot(path):
    """Atomically bind a recoverable prefix identity to its folded projection."""
    out_dir = os.path.dirname(os.path.abspath(path))
    events_path = _run_metadata_events_path(out_dir)
    with _campaign_metadata_lock(out_dir) as metadata_path:
        _reconcile_run_metadata_event_pending_unlocked(out_dir)
        mode = _run_metadata_storage_mode_unlocked(out_dir, metadata_path)
        if mode == "event" and os.path.isfile(events_path):
            events = _read_run_metadata_events_strict(events_path)
            if not events:
                raise RuntimeError("run metadata recovery evidence is empty")
            projection = _read_run_metadata_snapshot_unlocked(
                out_dir, metadata_path)
            identities = {
                "schema": METADATA_RECOVERY_EVIDENCE_SCHEMA,
                "mode": "event_prefix",
                "event_schema": METADATA_EVENT_SCHEMA,
                "events_file": METADATA_EVENTS_FILENAME,
                "event_prefix_size_bytes": os.path.getsize(events_path),
                "event_prefix_sha256": _sha256_file(events_path),
                "event_count": len(events),
                "projection_schema": METADATA_SCHEMA,
                "projection_record_count": len(projection),
                "projection_sha256": _run_metadata_projection_sha256(
                    projection),
            }
            return identities, [dict(record) for record in projection]
        rows = _read_jsonl_records_with_retry(path)
        if not rows:
            raise RuntimeError("run metadata recovery evidence is empty")
        identities = [
            {
                "row_number": number,
                "canonical_sha256": _run_metadata_recovery_identity(row),
                "task_plans": json.loads(json.dumps(
                    row.get("task_plans") or {})),
            }
            for number, row in enumerate(rows, 1)
        ]
        return identities, [dict(record) for record in rows]


def _run_metadata_recovery_identities(path):
    identities, _projection = _capture_run_metadata_recovery_snapshot(path)
    return identities


def _run_metadata_task_plans_match_manifest(out_dir, rows):
    try:
        with open(
                os.path.join(out_dir, "dispatch_manifest.json"),
                encoding="utf-8",
        ) as handle:
            dispatch_manifest = json.load(handle)
    except (OSError, ValueError):
        return False
    target_round_trips = (
        (dispatch_manifest.get("config") or {}).get("num_round_trips")
        if isinstance(dispatch_manifest, dict) else None
    )
    manifest_task_plans = (
        dispatch_manifest.get("task_plans") or {}
        if isinstance(dispatch_manifest, dict) else {}
    )
    expected_task_plans = {
        sample: {
            "sha256": plan.get("sha256"),
            "round_trips": target_round_trips,
        }
        for sample, plan in manifest_task_plans.items()
        if isinstance(sample, str) and isinstance(plan, dict)
    }
    if not rows:
        return False
    shared_task_plans = rows[0].get("task_plans") or {}
    return bool(
        isinstance(shared_task_plans, dict)
        and all((row.get("task_plans") or {}) == shared_task_plans
                for row in rows)
        and all(expected_task_plans.get(sample) == plan
                for sample, plan in shared_task_plans.items())
    )


def _run_metadata_recovery_prefix_matches(path, identities):
    out_dir = os.path.dirname(os.path.abspath(path))
    events_path = _run_metadata_events_path(out_dir)
    if os.path.exists(_run_metadata_event_pending_path(out_dir)):
        return False
    if isinstance(identities, dict):
        expected_keys = {
            "schema", "mode", "event_schema", "events_file",
            "event_prefix_size_bytes", "event_prefix_sha256", "event_count",
            "projection_schema", "projection_record_count",
            "projection_sha256",
        }
        if (set(identities) != expected_keys
                or identities.get("schema")
                != METADATA_RECOVERY_EVIDENCE_SCHEMA
                or identities.get("mode") != "event_prefix"
                or identities.get("event_schema") != METADATA_EVENT_SCHEMA
                or identities.get("events_file") != METADATA_EVENTS_FILENAME
                or identities.get("projection_schema") != METADATA_SCHEMA
                or not os.path.isfile(events_path)):
            return False
        receipt = {
            "event_prefix_size_bytes": identities.get(
                "event_prefix_size_bytes"),
            "event_prefix_sha256": identities.get("event_prefix_sha256"),
            "event_count": identities.get("event_count"),
        }
        try:
            with _campaign_metadata_lock(out_dir) as metadata_path:
                prefix_events, _current_size = _read_run_metadata_event_prefix(
                    events_path, receipt)
                prefix_projection = _fold_run_metadata_events(prefix_events)
                current_projection = _read_run_metadata_snapshot_unlocked(
                    out_dir, metadata_path)
        except (OSError, ValueError, RuntimeError):
            return False
        return (
            identities.get("projection_record_count")
            == len(prefix_projection)
            and identities.get("projection_sha256")
            == _run_metadata_projection_sha256(prefix_projection)
            and bool(current_projection)
            and _run_metadata_task_plans_match_manifest(
                out_dir, current_projection)
        )
    if os.path.isfile(events_path):
        return False
    if not isinstance(identities, list) or not identities:
        return False
    try:
        rows = _read_jsonl_records_with_retry(path)
    except (OSError, ValueError, RuntimeError):
        return False
    if len(rows) < len(identities):
        return False
    if not _run_metadata_task_plans_match_manifest(out_dir, rows):
        return False
    shared_task_plans = rows[0].get("task_plans") or {}
    statuses = [row.get("status") for row in rows]
    finished_values = [row.get("finished_at") for row in rows]
    if "running" in statuses:
        if any(value is not None for value in finished_values):
            return False
    else:
        if (not finished_values
                or not isinstance(finished_values[0], str)
                or not finished_values[0]
                or any(
                    value != finished_values[0]
                    for value in finished_values
                )):
            return False
        try:
            shared_finished = datetime.fromisoformat(finished_values[0])
            invocation_finished = [
                datetime.fromisoformat(row["invocation_finished_at"])
                for row in rows
            ]
        except (KeyError, TypeError, ValueError):
            return False
        if (shared_finished.tzinfo is None
                or shared_finished.utcoffset() is None
                or any(
                    value.tzinfo is None or value.utcoffset() is None
                    for value in invocation_finished
                )
                or shared_finished != max(invocation_finished)):
            return False
    for number, (entry, row) in enumerate(
            zip(identities, rows), 1):
        prior_task_plans = (
            entry.get("task_plans")
            if isinstance(entry, dict) else None
        )
        if (not isinstance(entry, dict)
                or entry.get("row_number") != number
                or not isinstance(entry.get("canonical_sha256"), str)
                or not re.fullmatch(
                    r"[0-9a-f]{64}", entry["canonical_sha256"])
                or _run_metadata_recovery_identity(row)
                != entry["canonical_sha256"]
                or not isinstance(prior_task_plans, dict)
                or any(
                    shared_task_plans.get(sample) != plan
                    for sample, plan in prior_task_plans.items()
                )):
            return False
    return True


def _deepseek_dispatcher_stopped_sidecar_evidence(out_dir, api_row):
    """Bind the exact partial stream hidden by a dispatcher-stop API row."""
    out_dir = os.path.realpath(os.path.abspath(out_dir))
    if not isinstance(api_row, dict):
        raise RuntimeError(
            "DeepSeek dispatcher-stop transport evidence is invalid")
    raw_path = api_row.get("raw_sse_saved_path")
    if not isinstance(raw_path, str) or not raw_path:
        raise RuntimeError(
            "DeepSeek dispatcher-stop transport sidecar is missing")
    path = os.path.realpath(raw_path)
    try:
        inside = os.path.commonpath([out_dir, path]) == out_dir
    except ValueError:
        inside = False
    if not inside or not os.path.isfile(path):
        raise RuntimeError(
            "DeepSeek dispatcher-stop transport sidecar is invalid")
    evidence = _read_deepseek_transport_sidecar(path)
    rows = evidence["rows"]
    starts = evidence["starts"]
    end_rows = evidence["ends"]
    expected_linkage = {
        "call_id": api_row.get("request_id"),
        "worker_launch_id": api_row.get("worker_launch_id"),
        "worker_pid": api_row.get("worker_pid"),
        "sample": api_row.get("sample"),
        "method": api_row.get("method"),
        "rt_index": api_row.get("rt_index"),
        "direction": api_row.get("direction"),
    }
    compact_validation = None
    legacy_validation = None
    transport_revision = api_row.get("transport_revision")
    if transport_revision == DEEPSEEK_COMPACT_TRANSPORT_REVISION:
        if not evidence["compact"]:
            raise RuntimeError(
                "DeepSeek dispatcher-stop compact sidecar was downgraded")
        try:
            compact_validation = _validate_deepseek_compact_records(
                rows, expected_linkage=expected_linkage)
        except RuntimeError as exc:
            raise RuntimeError(
                "DeepSeek dispatcher-stop compact sidecar is invalid") from exc
    elif transport_revision in {
            "opencode_openai_compatible/4",
            "opencode_openai_compatible/5",
    }:
        if evidence["compact"]:
            raise RuntimeError(
                "DeepSeek dispatcher-stop legacy sidecar is compact")
    else:
        raise RuntimeError(
            "DeepSeek dispatcher-stop transport revision is invalid")
    attempt = (
        end_rows[0].get("attempt")
        if len(end_rows) == 1 else None
    )
    if (transport_revision in {
            "opencode_openai_compatible/4",
            "opencode_openai_compatible/5",
    } and isinstance(attempt, dict)):
        try:
            legacy_validation = _validate_deepseek_linear_records(
                rows,
                expected_linkage=expected_linkage,
                expected_attempts=[attempt],
            )
        except RuntimeError:
            legacy_validation = None
    compact_valid = (
        compact_validation is not None
        and len(compact_validation["closed_attempts"]) == 1
    )
    legacy_valid = (
        legacy_validation is not None
        and len(legacy_validation["closed_attempts"]) == 1
    )
    if (len(rows) < 3
            or len(starts) != 1
            or 1 not in evidence["generation_attempt_indexes"]
            or len(end_rows) != 1
            or (
                evidence["body"][0] is not starts[0]
                if evidence["body"] else True
            )
            or rows[-1] is not end_rows[0]
            or not (compact_valid or legacy_valid)
            or not isinstance(attempt, dict)
            or starts[0].get("attempt_index") != 1
            or end_rows[0].get("attempt_index") != 1
            or any(
                attempt.get(key) != value
                for key, value in {
                    "attempt_index": 1,
                    "status": "retryable_error",
                    "http_status": 200,
                    "error_type": "incomplete_stream",
                    "error_message": (
                        "OpenAI-compatible stream ended without "
                        "finish_reason and final_usage"
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
                }.items()
            )):
        raise RuntimeError(
            "DeepSeek dispatcher-stop partial stream shape is invalid")
    return {
        "api_row_sha256": _canonical_record_sha256(api_row),
        "request_id": api_row.get("request_id"),
        "incident_kind": (
            "deepseek_dispatcher_stopped_uncommitted_api"),
        "path": os.path.relpath(path, out_dir).replace("\\", "/"),
        "sha256": _sha256_file(path),
        "event_count": len(rows),
        "attempt_start_count": 1,
        "sdk_stream_event_count": (
            evidence["summaries"][0].get("stream_event_count")
            if evidence["compact"] else len([
                row for row in rows
                if row.get("record_type") == "sdk_stream_event"
            ])
        ),
        "attempt_end_count": 1,
        "attempt_end_sha256": _canonical_record_sha256(end_rows[0]),
    }


def _deepseek_inspector_reprepare_evidence_matches(
        out_dir, record, *, require_live_identity=True):
    prefix = "deepseek_transport_inspector_pending_reprepared_"
    suffixes = {
        "from_commit",
        "from_sha256",
        "from_path",
        "from_authorization_id",
        "from_history_dir",
        "at",
    }
    present = {suffix for suffix in suffixes if prefix + suffix in record}
    if not present:
        history_root = os.path.join(out_dir, "recovery_history")
        if (os.path.isdir(history_root)
                and any(
                    os.path.isfile(os.path.join(
                        history_root, name, "pending.json"))
                    for name in os.listdir(history_root)
                )):
            return False
        return True
    if present != suffixes:
        return False
    prepared_commit = record.get(prefix + "from_commit")
    old_pending_sha256 = record.get(prefix + "from_sha256")
    old_authorization_id = record.get(prefix + "from_authorization_id")
    old_history_relative = record.get(prefix + "from_history_dir")
    old_pending_relative = record.get(prefix + "from_path")
    reprepared_at = record.get(prefix + "at")
    if (not isinstance(prepared_commit, str)
            or not re.fullmatch(r"[0-9a-f]{40}", prepared_commit)
            or not isinstance(old_pending_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", old_pending_sha256)
            or not isinstance(old_authorization_id, str)
            or not old_authorization_id
            or "/" in old_authorization_id
            or "\\" in old_authorization_id
            or old_history_relative
            != "recovery_history/" + old_authorization_id
            or old_pending_relative
            != old_history_relative + "/pending.json"
            or not isinstance(reprepared_at, str)
            or not reprepared_at):
        return False
    old_pending_path = os.path.realpath(os.path.join(
        out_dir, old_pending_relative))
    try:
        old_pending_inside = (
            os.path.commonpath([out_dir, old_pending_path]) == out_dir)
    except ValueError:
        old_pending_inside = False
    if (not old_pending_inside
            or not os.path.isfile(old_pending_path)
            or _sha256_file(old_pending_path) != old_pending_sha256):
        return False
    try:
        with open(old_pending_path, encoding="utf-8") as handle:
            old_pending = json.load(handle)
    except (OSError, ValueError):
        return False
    old_record = (
        old_pending.get("authorization_record")
        if isinstance(old_pending, dict) else None
    )
    if (not isinstance(old_record, dict)
            or old_pending.get("schema")
            != "anchorpatch.deepseek_transport_inspector_recovery/1"
            or old_pending.get("history_dir") != old_history_relative
            or old_pending.get("created_at") != record.get("created_at")
            or old_record.get("authorization_id")
            != old_authorization_id
            or old_record.get("recovery_git_commit") != prepared_commit):
        return False

    current_commit = record.get("recovery_git_commit")
    old_fingerprint = old_record.get("recovery_code_fingerprint")
    current_fingerprint = record.get("recovery_code_fingerprint")
    if (not isinstance(old_fingerprint, dict)
            or not isinstance(current_fingerprint, dict)
            or sorted(
                key for key in set(old_fingerprint) | set(
                    current_fingerprint)
                if old_fingerprint.get(key)
                != current_fingerprint.get(key)
            ) != ["run_meta.py"]
            or (
                require_live_identity
                and current_fingerprint != code_fingerprint()
            )):
        return False
    try:
        parent_line = subprocess.run(
            [
                "git", "-C", _HERE, "rev-list", "--parents",
                "-n", "1", current_commit,
            ],
            check=True, capture_output=True, text=True,
            encoding="utf-8",
        ).stdout.split()
        changed_paths = {
            item.replace("\\", "/")
            for item in subprocess.run(
                [
                    "git", "-C", _HERE, "diff", "--name-only",
                    prepared_commit, current_commit, "--",
                ],
                check=True, capture_output=True, text=True,
                encoding="utf-8",
            ).stdout.splitlines()
            if item
        }
    except (OSError, subprocess.CalledProcessError, TypeError):
        return False
    if (parent_line != [current_commit, prepared_commit]
            or changed_paths != {
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/run_meta.py",
                "HP_V8/src/test_model_openai.py",
            }):
        return False

    expected_record = json.loads(json.dumps(old_record))
    current_authorization_id = record.get("authorization_id")
    if (not isinstance(current_authorization_id, str)
            or re.fullmatch(
                r"dsi-[0-9a-f]{12}", current_authorization_id) is None):
        return False
    current_history_relative = (
        "recovery_history/" + current_authorization_id)
    expected_record["recovery_git_commit"] = current_commit
    expected_record["recovery_code_fingerprint"] = current_fingerprint
    expected_record["authorization_id"] = current_authorization_id
    expected_record["superseded_authorization_path"] = (
        current_history_relative
        + "/superseded_campaign_recovery_authorization.json"
    )
    expected_record["archived_stop_path"] = (
        current_history_relative + "/campaign_stop.json")
    for suffix in suffixes:
        expected_record[prefix + suffix] = record[prefix + suffix]
    return expected_record == record


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
                or current.get("recovery_kind") not in {
                    LEDGER_LOCK_RECOVERY_KIND,
                    DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
                }
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


def _dispatcher_parent_loss_code_transition_matches(
        record, superseded_item):
    """Validate one digest-bound direct-child recovery-tool transition."""
    transition = record.get("dispatcher_parent_loss_code_transition")
    superseded = (
        superseded_item.get("record")
        if isinstance(superseded_item, dict) else None
    )
    if (not isinstance(transition, dict)
            or set(transition) != {
                "prior_authorization_id",
                "prior_authorization_sha256",
                "prior_recovery_git_commit",
                "prior_recovery_code_fingerprint",
                "delta_changed_paths",
                "delta_changed_code_fingerprint_keys",
            }
            or not isinstance(superseded, dict)
            or record.get("authorization_basis") != (
                "explicit_user_resume_after_dispatcher_process_loss_and_"
                "recovery_tool_fix"
            )
            or transition.get("prior_authorization_id")
            != superseded.get("authorization_id")
            or transition.get("prior_authorization_sha256")
            != superseded_item.get("sha256")
            or transition.get("prior_recovery_git_commit")
            != superseded.get("recovery_git_commit")
            or transition.get("prior_recovery_code_fingerprint")
            != superseded.get("recovery_code_fingerprint")
            or transition.get("delta_changed_paths")
            != sorted(
                _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS)
            or transition.get("delta_changed_code_fingerprint_keys")
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS):
        return False

    prior_commit = superseded.get("recovery_git_commit")
    current_commit = record.get("recovery_git_commit")
    prior_fingerprint = superseded.get("recovery_code_fingerprint")
    current_fingerprint = record.get("recovery_code_fingerprint")
    if (not isinstance(prior_fingerprint, dict)
            or not isinstance(current_fingerprint, dict)):
        return False
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(current_fingerprint)
        if prior_fingerprint.get(key) != current_fingerprint.get(key)
    )
    try:
        parent_line = subprocess.run(
            [
                "git", "-C", _HERE, "rev-list", "--parents", "-n", "1",
                current_commit,
            ],
            check=True, capture_output=True, text=True, encoding="utf-8",
        ).stdout.split()
        changed_paths = {
            item.replace("\\", "/")
            for item in subprocess.run(
                [
                    "git", "-C", _HERE, "diff", "--name-only",
                    prior_commit, current_commit, "--",
                ],
                check=True, capture_output=True, text=True,
                encoding="utf-8",
            ).stdout.splitlines()
            if item
        }
    except (OSError, subprocess.CalledProcessError, TypeError):
        return False
    return (
        parent_line == [current_commit, prior_commit]
        and changed_paths
        == _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
        and fingerprint_changes
        == _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS
    )


def _load_dispatcher_parent_loss_pending_witness(
        out_dir, record, history_relative, witness_kind):
    """Load one digest-bound pending amendment witness."""
    if witness_kind == "validator":
        prefix = "dispatcher_parent_loss_pending_validator_reprepared_"
        filename = "pending.validator-before.json"
    elif witness_kind == "api_validator":
        prefix = (
            "dispatcher_parent_loss_pending_api_validator_reprepared_")
        filename = "pending.api-validator-before.json"
    else:
        raise RuntimeError(
            "dispatcher parent-loss pending witness kind is invalid")
    provenance_fields = {
        prefix + suffix
        for suffix in ("from_commit", "from_sha256", "from_path", "at")
    }
    relative = history_relative + "/" + filename
    if ({
            key for key in record if key.startswith(prefix)
        } != provenance_fields
            or not re.fullmatch(
                r"[0-9a-f]{40}",
                str(record.get(prefix + "from_commit") or ""),
            )
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(record.get(prefix + "from_sha256") or ""),
            )
            or record.get(prefix + "from_path") != relative
            or not isinstance(record.get(prefix + "at"), str)
            or not record.get(prefix + "at")):
        raise RuntimeError(
            "dispatcher parent-loss pending witness provenance is invalid")
    path = os.path.realpath(os.path.join(out_dir, relative))
    if (os.path.commonpath([out_dir, path]) != out_dir
            or not os.path.isfile(path)
            or _sha256_file(path) != record[prefix + "from_sha256"]):
        raise RuntimeError(
            "dispatcher parent-loss pending witness digest mismatch")
    try:
        with open(path, encoding="utf-8") as handle:
            pending = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "dispatcher parent-loss pending witness is invalid") from exc
    witness_record = (
        pending.get("authorization_record")
        if isinstance(pending, dict) else None
    )
    recovery_plan = (
        pending.get("recovery_plan")
        if isinstance(pending, dict) else None
    )
    if (not isinstance(pending, dict)
            or pending.get("schema")
            != "anchorpatch.dispatcher_parent_loss_recovery/1"
            or pending.get("history_dir") != history_relative
            or not isinstance(pending.get("created_at"), str)
            or not pending.get("created_at")
            or not isinstance(witness_record, dict)
            or witness_record.get("authorization_id")
            != record.get("authorization_id")
            or witness_record.get("recovery_git_commit")
            != record.get(prefix + "from_commit")
            or not isinstance(recovery_plan, dict)
            or recovery_plan.get("dispatcher_pid")
            != witness_record.get("dispatcher_pid")
            or recovery_plan.get("dispatcher_instance_id")
            != witness_record.get("dispatcher_instance_id")
            or recovery_plan.get("workers")
            != witness_record.get("dispatcher_parent_loss_workers")):
        raise RuntimeError(
            "dispatcher parent-loss pending witness identity mismatch")
    return pending


def _dispatcher_parent_loss_pending_record_transition_matches(
        current_record, witness_record, witness_kind):
    """Match the exact record-only mutation made by one reprepare step."""
    if witness_kind == "validator":
        prefix = "dispatcher_parent_loss_pending_validator_reprepared_"
        phase_flags = (
            "deepseek_transport_disconnect_"
            "inspector_followup_recovery",
            "deepseek_resume_classifier_followup_recovery",
        )
        if (any(witness_record.get(name) is not True
                for name in phase_flags)
                or any(name in current_record for name in phase_flags)):
            return False
    elif witness_kind == "api_validator":
        prefix = (
            "dispatcher_parent_loss_pending_api_validator_reprepared_")
        phase_flags = ()
    else:
        return False
    expected = json.loads(json.dumps(witness_record))
    for field in (
            "recovery_git_commit",
            "recovery_code_fingerprint",
            "changed_tracked_paths",
            "changed_code_fingerprint_keys",
            "dispatcher_parent_loss_code_transition"):
        expected[field] = json.loads(json.dumps(current_record.get(field)))
    for name in phase_flags:
        expected.pop(name, None)
    for suffix in ("from_commit", "from_sha256", "from_path", "at"):
        field = prefix + suffix
        expected[field] = current_record.get(field)
    return expected == current_record


def _validate_dispatcher_parent_loss_pending_reprepare_witnesses(
        out_dir, record, *, authorization_path=None):
    """Validate the optional completed/pending amendment witness chain."""
    archived_stop_relative = record.get("archived_stop_path")
    if (not isinstance(archived_stop_relative, str)
            or "/" not in archived_stop_relative):
        raise RuntimeError(
            "dispatcher parent-loss archived stop path is invalid")
    history_relative = archived_stop_relative.rsplit("/", 1)[0]
    reader_prefix = (
        "dispatcher_parent_loss_authorization_reader_sha_reprepared_")
    reader_fields = {
        reader_prefix + suffix
        for suffix in ("from_commit", "from_sha256", "from_path", "at")
    }
    reader_keys = {
        key for key in record if key.startswith(reader_prefix)
    }
    pending_record = record
    if reader_keys:
        reader_relative = (
            history_relative + "/authorization.reader-sha-before.json")
        reader_path = os.path.realpath(os.path.join(
            out_dir, reader_relative))
        if (reader_keys != reader_fields
                or not re.fullmatch(
                    r"[0-9a-f]{40}",
                    str(record.get(
                        reader_prefix + "from_commit") or ""),
                )
                or not re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(record.get(
                        reader_prefix + "from_sha256") or ""),
                )
                or record.get(reader_prefix + "from_path")
                != reader_relative
                or not isinstance(record.get(reader_prefix + "at"), str)
                or not record.get(reader_prefix + "at")
                or os.path.commonpath([out_dir, reader_path]) != out_dir
                or not os.path.isfile(reader_path)
                or _sha256_file(reader_path)
                != record[reader_prefix + "from_sha256"]):
            raise RuntimeError(
                "dispatcher parent-loss authorization reader witness "
                "is invalid")
        try:
            with open(reader_path, encoding="utf-8") as handle:
                pending_record = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "dispatcher parent-loss authorization reader witness "
                "is invalid") from exc
        expected = json.loads(json.dumps(pending_record))
        if (not isinstance(pending_record, dict)
                or pending_record.get("authorization_id")
                != record.get("authorization_id")
                or pending_record.get("recovery_git_commit")
                != record.get(reader_prefix + "from_commit")
                or any(
                    key.startswith(reader_prefix)
                    for key in pending_record
                )):
            raise RuntimeError(
                "dispatcher parent-loss authorization reader witness "
                "identity mismatch")
        for field in (
                "recovery_git_commit",
                "recovery_code_fingerprint",
                "changed_tracked_paths",
                "changed_code_fingerprint_keys",
                "dispatcher_parent_loss_code_transition"):
            expected[field] = json.loads(json.dumps(record.get(field)))
        for field in reader_fields:
            expected[field] = record[field]
        if expected != record:
            raise RuntimeError(
                "dispatcher parent-loss authorization reader witness "
                "transition is invalid")
        authorization_path = (
            os.path.join(
                out_dir,
                CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            if authorization_path is None else authorization_path
        )
        expected_event = {
            "event": (
                "user_authorized_dispatcher_parent_loss_reader_sha_fix"),
            "created_at": record[reader_prefix + "at"],
            "campaign_recovery_authorization_id": record[
                "authorization_id"],
            "campaign_recovery_authorization_sha256": _sha256_file(
                authorization_path),
            "superseded_authorization_sha256": record[
                reader_prefix + "from_sha256"],
            "prior_recovery_git_commit": record[
                reader_prefix + "from_commit"],
            "recovery_git_commit": record["recovery_git_commit"],
        }
        matching_events = [
            row for row in _read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            if row.get("event") == expected_event["event"]
            and row.get("campaign_recovery_authorization_id")
            == record["authorization_id"]
        ]
        if len(matching_events) != 1 or matching_events[0] != expected_event:
            raise RuntimeError(
                "dispatcher parent-loss authorization reader event "
                "is invalid")

    api_prefix = (
        "dispatcher_parent_loss_pending_api_validator_reprepared_")
    validator_prefix = (
        "dispatcher_parent_loss_pending_validator_reprepared_")
    has_api_witness = any(
        key.startswith(api_prefix) for key in pending_record)
    has_validator_witness = any(
        key.startswith(validator_prefix) for key in pending_record)
    if has_api_witness:
        api_pending = _load_dispatcher_parent_loss_pending_witness(
            out_dir, pending_record, history_relative, "api_validator")
        api_record = api_pending["authorization_record"]
        if (not _dispatcher_parent_loss_pending_record_transition_matches(
                    pending_record, api_record, "api_validator")
                or "api_validator_reprepared_at" in api_pending
                or any(key.startswith(api_prefix) for key in api_record)):
            raise RuntimeError(
                "dispatcher parent-loss API-validator pending witness "
                "transition is invalid")
        validator_pending = (
            _load_dispatcher_parent_loss_pending_witness(
                out_dir, api_record, history_relative, "validator")
        )
        validator_record = validator_pending["authorization_record"]
        expected_api_pending = json.loads(json.dumps(validator_pending))
        expected_api_pending["authorization_record"] = api_record
        expected_api_pending["validator_reprepared_at"] = api_record[
            validator_prefix + "at"]
        if (not _dispatcher_parent_loss_pending_record_transition_matches(
                    api_record, validator_record, "validator")
                or expected_api_pending != api_pending):
            raise RuntimeError(
                "dispatcher parent-loss validator pending witness "
                "transition is invalid")
    elif has_validator_witness:
        validator_pending = (
            _load_dispatcher_parent_loss_pending_witness(
                out_dir, pending_record, history_relative, "validator")
        )
        if not _dispatcher_parent_loss_pending_record_transition_matches(
                pending_record,
                validator_pending["authorization_record"],
                "validator",
        ):
            raise RuntimeError(
                "dispatcher parent-loss validator pending witness "
                "transition is invalid")


def _deepseek_recovered_worker_scope_is_valid(
        recovered_workers, worker_ids, resume_samples, pending_samples):
    fields = {
        "sample",
        "status",
        "worker_launch_id",
        "worker_pid",
        "invocation_id",
    }
    if (not isinstance(recovered_workers, list)
            or len(recovered_workers) != len(worker_ids)
            or any(
                not isinstance(item, dict)
                or set(item) != fields
                or not isinstance(item.get("sample"), str)
                or not item.get("sample")
                or item.get("status") not in {
                    "failed", "interrupted_by_dispatcher"}
                or not isinstance(item.get("worker_launch_id"), str)
                or not item.get("worker_launch_id")
                or not isinstance(item.get("worker_pid"), int)
                or isinstance(item.get("worker_pid"), bool)
                or item.get("worker_pid") <= 0
                or not isinstance(item.get("invocation_id"), str)
                or not item.get("invocation_id")
                for item in recovered_workers)):
        return False
    samples = [item["sample"] for item in recovered_workers]
    recovered_ids = [
        item["worker_launch_id"] for item in recovered_workers]
    return (
        len(samples) == len(set(samples))
        and len(recovered_ids) == len(set(recovered_ids))
        and set(recovered_ids) == set(worker_ids)
        and set(samples) == set(resume_samples) - set(pending_samples)
    )


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


def read_campaign_recovery_authorization(
        out_dir, *, allow_active_dispatcher_stop=False,
        allow_pending_transaction=False):
    """Validate a narrow, append-only campaign recovery boundary."""
    out_dir = os.path.abspath(out_dir)
    pending_paths = [
        os.path.join(out_dir, name)
        for name in (
            DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
        )
    ]
    if (any(os.path.isfile(path) for path in pending_paths)
            and not allow_pending_transaction):
        raise RuntimeError(
            "campaign recovery transaction is pending; rerun the matching "
            "authorize_ledger_lock_recovery.py mode before resume"
        )
    authorization_path = os.path.join(
        out_dir, CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
    if not os.path.exists(authorization_path):
        return None
    try:
        with open(authorization_path, encoding="utf-8") as handle:
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
    dispatcher_process_lost_recovery = (
        record.get("schema") == CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
        and recovery_kind == DISPATCHER_PROCESS_LOST_RECOVERY_KIND
    )
    if not (
            legacy_git_recovery
            or ledger_lock_recovery
            or dispatcher_process_lost_recovery):
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
    if record.get(
            "deepseek_transport_disconnect_inspector_followup_recovery"
            ) is True:
        authorization_id = record.get("authorization_id")
        if (not isinstance(authorization_id, str)
                or not authorization_id
                or authorization_id in {".", ".."}
                or "/" in authorization_id
                or "\\" in authorization_id
                or (
                    record.get(
                        "deepseek_resume_classifier_"
                        "followup_recovery") is True
                    and re.fullmatch(
                        r"dsr-[0-9a-f]{12}", authorization_id) is None
                )):
            raise RuntimeError(
                "DeepSeek inspector recovery authorization id is invalid")
        expected_history_relative = (
            "recovery_history/" + authorization_id)
        expected_superseded_relative = (
            expected_history_relative
            + "/superseded_campaign_recovery_authorization.json"
        )
        expected_stop_relative = (
            expected_history_relative + "/campaign_stop.json")
        if (record.get("superseded_authorization_path")
                != expected_superseded_relative
                or archived_relative != expected_stop_relative):
            raise RuntimeError(
                "DeepSeek inspector recovery archive path mismatch")
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
            record.get("dispatcher_operator_pause_recovery") is not True
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "operator_directed_dispatcher_pause"
            and archived_stop.get("stopped_git_commit")
            == record.get("prior_git_commit")
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
        deepseek_server_retry_recovery = (
            record.get("deepseek_server_retry_recovery") is True
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "dispatcher_integrity_failure"
            and str(archived_stop.get("error") or "").endswith(
                "failed without a supported sample-local outcome")
            and isinstance(record.get("deepseek_server_retry_samples"), list)
            and bool(record.get("deepseek_server_retry_samples"))
        )
        deepseek_transport_disconnect_initial_stop = (
            record.get("authorization_basis") == (
                "explicit_user_resume_after_deepseek_transport_disconnect_"
                "classifier_fix"
            )
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition") == "worker_fatal_error"
            and archived_stop.get("error_type")
            == "OpenAICompatibleTransportError"
            and archived_stop.get("error")
            == "OpenAI-compatible provider failed after 1 attempt(s)"
            and record.get(
                "deepseek_transport_disconnect_retry_samples")
            == [archived_stop.get("sample")]
            and record.get(
                "deepseek_transport_disconnect_retry_worker_launch_id")
            == archived_stop.get("worker_launch_id")
        )
        deepseek_transport_disconnect_inspector_followup_stop = (
            record.get(
                "deepseek_transport_disconnect_inspector_followup_recovery")
            is True
            and record.get("authorization_basis")
            == (
                "explicit_user_resume_after_deepseek_recovery_inspector_fix"
            )
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "dispatcher_integrity_failure"
            and archived_stop.get("worker_launch_id") is None
            and isinstance(archived_stop.get("worker_pid"), int)
            and not isinstance(archived_stop.get("worker_pid"), bool)
            and archived_stop.get("worker_pid") > 0
            and archived_stop.get("error_type") == "RuntimeError"
            and archived_stop.get("error")
            == _DEEPSEEK_TRANSPORT_INSPECTOR_STOP_ERROR
            and isinstance(record.get("superseded_authorization_path"), str)
            and bool(record.get("superseded_authorization_path"))
            and isinstance(
                record.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_authorization_id"),
                str,
            )
            and isinstance(
                record.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_authorization_sha256"),
                str,
            )
        )
        deepseek_resume_classifier_followup_stop = (
            record.get(
                "deepseek_resume_classifier_followup_recovery") is True
            and record.get("authorization_basis")
            == _DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "dispatcher_integrity_failure"
            and archived_stop.get("worker_launch_id") is None
            and isinstance(archived_stop.get("worker_pid"), int)
            and not isinstance(archived_stop.get("worker_pid"), bool)
            and archived_stop.get("worker_pid") > 0
            and archived_stop.get("error_type") == "RuntimeError"
            and archived_stop.get("error")
            == record.get("deepseek_resume_classifier_stop_error")
            and str(archived_stop.get("error") or "").startswith(
                _DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX)
            and isinstance(
                record.get(
                    "deepseek_resume_classifier_prior_authorization_id"),
                str)
            and isinstance(
                record.get(
                    "deepseek_resume_classifier_prior_authorization_sha256"),
                str)
        )
        deepseek_transport_disconnect_retry_recovery = (
            record.get(
                "deepseek_transport_disconnect_retry_recovery") is True
            and (
                deepseek_transport_disconnect_initial_stop
                or deepseek_transport_disconnect_inspector_followup_stop
                or deepseek_resume_classifier_followup_stop
            )
        )
        dispatcher_process_lost_stop = (
            dispatcher_process_lost_recovery
            and record.get("dispatcher_operator_pause_recovery") is not True
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition") == "dispatcher_process_lost"
            and archived_stop.get("dispatcher_pid")
            == record.get("dispatcher_pid")
            and archived_stop.get("dispatcher_instance_id")
            == record.get("dispatcher_instance_id")
        )
        dispatcher_operator_pause_stop = (
            dispatcher_process_lost_recovery
            and record.get("dispatcher_operator_pause_recovery") is True
            and record.get("dispatcher_stop_condition")
            == "operator_directed_dispatcher_pause"
            and archived_stop.get("schema") == STOP_CONDITION_SCHEMA
            and archived_stop.get("condition")
            == "operator_directed_dispatcher_pause"
            and archived_stop.get("dispatcher_pid")
            == record.get("dispatcher_pid")
            and archived_stop.get("dispatcher_instance_id")
            == record.get("dispatcher_instance_id")
            and archived_stop.get("stopped_git_commit")
            == record.get("prior_git_commit")
            and archived_stop.get("stopped_git_tree_state") == "clean"
            and archived_stop.get("git_status_porcelain") == ""
            and isinstance(archived_stop.get("active_worker_launch_ids"), list)
            and archived_stop.get("dispatch_manifest_sha256")
            == record.get("dispatch_manifest_sha256")
            and archived_stop.get("active_worker_set_sha256")
            == record.get("archived_active_worker_set_sha256")
            and archived_stop.get("publication_mode")
            in {"canonical", "emergency_fallback"}
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
            or deepseek_server_retry_recovery
            or deepseek_transport_disconnect_retry_recovery
            or dispatcher_process_lost_stop
            or dispatcher_operator_pause_stop
        )
    if not stop_valid:
        raise RuntimeError("campaign recovery stop is not authorized")
    active_stop_records = read_campaign_stop_conditions(out_dir)
    allowed_active_dispatcher_stop = (
        allow_active_dispatcher_stop
        and bool(active_stop_records)
        and all(
            item.get("condition") in {
                "dispatcher_process_lost",
                "operator_directed_dispatcher_pause",
            }
            for item in active_stop_records
        )
    )
    if active_stop_records and not allowed_active_dispatcher_stop:
        raise RuntimeError(
            "campaign recovery requires the stop latch to be archived first")
    recovery_chain = (
        _load_recovery_authorization_chain(
            out_dir, authorization_path, record)
        if (ledger_lock_recovery or dispatcher_process_lost_recovery) else [{
            "record": record,
            "path": authorization_path,
            "sha256": _sha256_file(authorization_path),
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
    if (record.get(
            "deepseek_transport_disconnect_inspector_followup_recovery"
            ) is True
            and record.get(
                "deepseek_resume_classifier_followup_recovery") is not True
            and not _deepseek_inspector_reprepare_evidence_matches(
                out_dir, record)):
        raise RuntimeError(
            "DeepSeek inspector pending reprepare evidence mismatch")
    if (record.get(
            "deepseek_transport_disconnect_initial_transaction") is True
            and record.get(
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery") is not True
            and not _deepseek_initial_recovery_witness_matches(
                out_dir, record, _sha256_file(authorization_path))):
        raise RuntimeError(
            "DeepSeek initial transport recovery dispatch witness "
            "mismatch"
        )
    if record.get(
            "deepseek_transport_disconnect_inspector_followup_recovery"
            ) is True and record.get(
                "deepseek_resume_classifier_followup_recovery") is not True:
        if len(recovery_chain) < 2:
            raise RuntimeError(
                "DeepSeek inspector follow-up authorization chain is "
                "incomplete"
            )
        superseded_item = recovery_chain[1]
        superseded = superseded_item["record"]
        prior_recovery_commit = superseded.get("recovery_git_commit")
        prior_recovery_fingerprint = superseded.get(
            "recovery_code_fingerprint")
        if (superseded.get(
                "deepseek_transport_disconnect_retry_recovery") is not True
                or superseded.get(
                    "deepseek_transport_disconnect_"
                    "inspector_followup_recovery") is not None
                or superseded.get("authorization_basis")
                != (
                    "explicit_user_resume_after_deepseek_transport_"
                    "disconnect_classifier_fix"
                )
                or superseded.get("authorization_id")
                != record.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_authorization_id")
                or superseded_item["sha256"]
                != record.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_authorization_sha256")
                or prior_recovery_commit
                != record.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_recovery_git_commit")
                or not isinstance(prior_recovery_fingerprint, dict)):
            raise RuntimeError(
                "DeepSeek inspector follow-up superseded authorization "
                "mismatch"
            )
        if (superseded.get(
                "deepseek_transport_disconnect_initial_transaction")
                is True
                and not _deepseek_initial_recovery_witness_matches(
                    out_dir, superseded, superseded_item["sha256"])):
            raise RuntimeError(
                "DeepSeek inspector superseded authorization witness "
                "mismatch"
            )
        superseded_stop_relative = superseded.get("archived_stop_path")
        if (not isinstance(superseded_stop_relative, str)
                or not superseded_stop_relative):
            raise RuntimeError(
                "DeepSeek inspector superseded stop evidence is invalid")
        superseded_stop_path = os.path.realpath(os.path.join(
            out_dir, superseded_stop_relative))
        try:
            superseded_stop_inside = (
                os.path.commonpath([out_dir, superseded_stop_path])
                == out_dir
            )
        except ValueError:
            superseded_stop_inside = False
        if (not superseded_stop_inside
                or not os.path.isfile(superseded_stop_path)
                or _sha256_file(superseded_stop_path)
                != superseded.get("archived_stop_sha256")):
            raise RuntimeError(
                "DeepSeek inspector superseded stop digest mismatch")
        try:
            with open(
                    superseded_stop_path, encoding="utf-8") as handle:
                superseded_stop = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "DeepSeek inspector superseded stop is invalid") from exc
        if (not isinstance(superseded_stop, dict)
                or superseded_stop.get("schema")
                != STOP_CONDITION_SCHEMA
                or superseded_stop.get("condition")
                != "worker_fatal_error"
                or superseded_stop.get("error_type")
                != "OpenAICompatibleTransportError"
                or superseded_stop.get("error")
                != "OpenAI-compatible provider failed after 1 attempt(s)"
                or superseded.get(
                    "deepseek_transport_disconnect_retry_samples")
                != [superseded_stop.get("sample")]
                or superseded.get(
                    "deepseek_transport_disconnect_retry_worker_launch_id")
                != superseded_stop.get("worker_launch_id")):
            raise RuntimeError(
                "DeepSeek inspector superseded stop scope mismatch")
        try:
            delta_changed = subprocess.run(
                [
                    "git", "-C", _HERE, "diff", "--name-only",
                    prior_recovery_commit, current_commit, "--",
                ],
                check=True, capture_output=True, text=True,
                encoding="utf-8",
            ).stdout.splitlines()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise RuntimeError(
                "cannot audit DeepSeek inspector follow-up Git diff"
            ) from exc
        delta_changed = sorted(
            item.replace("\\", "/") for item in delta_changed if item)
        expected_delta_paths = {
            "HP_V8/src/authorize_ledger_lock_recovery.py",
            "HP_V8/src/paired_campaign_dispatch.py",
            "HP_V8/src/run_meta.py",
            "HP_V8/src/test_model_openai.py",
        }
        delta_fingerprint_changes = sorted(
            key for key in set(prior_recovery_fingerprint) | set(
                current_fingerprint)
            if prior_recovery_fingerprint.get(key)
            != current_fingerprint.get(key)
        )
        if (set(delta_changed) != expected_delta_paths
                or delta_changed != record.get(
                    "deepseek_transport_disconnect_inspector_"
                    "delta_changed_paths")
                or delta_fingerprint_changes != ["run_meta.py"]):
            raise RuntimeError(
                "DeepSeek inspector follow-up code scope mismatch")
        prefixes = record.get(
            "deepseek_transport_disconnect_inspector_prefixes")
        expected_prefix_names = {
            "api_calls.jsonl",
            "dispatch_log.jsonl",
        }
        if (not isinstance(prefixes, dict)
                or set(prefixes) != expected_prefix_names
                or any(
                    not _file_prefix_matches(
                        os.path.join(out_dir, name), prefixes[name])
                    for name in expected_prefix_names
                )):
            raise RuntimeError(
                "DeepSeek inspector follow-up evidence prefix mismatch")
        metadata_identities = record.get(
            "deepseek_transport_disconnect_inspector_"
            "run_metadata_identities"
        )
        if not _run_metadata_recovery_prefix_matches(
                os.path.join(out_dir, "run_metadata.jsonl"),
                metadata_identities):
            raise RuntimeError(
                "DeepSeek inspector follow-up run metadata identity "
                "mismatch"
            )
        current_authorization_sha256 = _sha256_file(
            authorization_path)
        expected_witness = {
            "event": (
                "user_authorized_deepseek_transport_inspector_followup"),
            "created_at": record.get("created_at"),
            "campaign_recovery_authorization_id": record.get(
                "authorization_id"),
            "campaign_recovery_authorization_sha256": (
                current_authorization_sha256),
            "superseded_authorization_id": superseded.get(
                "authorization_id"),
            "incident_api_rows": len(record.get("incident_api_rows") or []),
            "prior_recovery_git_commit": prior_recovery_commit,
            "recovery_git_commit": current_commit,
        }
        witness_rows = [
            row for row in _read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            if row.get("event")
            == "user_authorized_deepseek_transport_inspector_followup"
            and row.get("campaign_recovery_authorization_id")
            == record.get("authorization_id")
        ]
        if (len(witness_rows) != 1
                or witness_rows[0] != expected_witness
                or not _jsonl_first_suffix_record_matches(
                    os.path.join(out_dir, "dispatch_log.jsonl"),
                    prefixes["dispatch_log.jsonl"],
                    expected_witness)):
            raise RuntimeError(
                "DeepSeek inspector follow-up dispatch witness mismatch")
    if record.get(
            "deepseek_resume_classifier_followup_recovery") is True:
        if len(recovery_chain) < 3:
            raise RuntimeError(
                "DeepSeek resume-classifier authorization chain is "
                "incomplete")
        inspector_item = recovery_chain[1]
        initial_item = recovery_chain[2]
        inspector = inspector_item["record"]
        initial = initial_item["record"]
        inspector_commit = inspector.get("recovery_git_commit")
        inspector_fingerprint = inspector.get(
            "recovery_code_fingerprint")
        initial_commit = initial.get("recovery_git_commit")
        initial_fingerprint = initial.get("recovery_code_fingerprint")
        if (record.get("authorization_basis")
                != _DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS
                or inspector.get(
                    "deepseek_transport_disconnect_"
                    "inspector_followup_recovery") is not True
                or inspector.get(
                    "deepseek_resume_classifier_"
                    "followup_recovery") is not None
                or inspector.get("authorization_basis")
                != (
                    "explicit_user_resume_after_deepseek_recovery_"
                    "inspector_fix"
                )
                or initial.get(
                    "deepseek_transport_disconnect_retry_recovery")
                is not True
                or initial.get(
                    "deepseek_transport_disconnect_"
                    "inspector_followup_recovery") is not None
                or initial.get("authorization_basis")
                != (
                    "explicit_user_resume_after_deepseek_transport_"
                    "disconnect_classifier_fix"
                )
                or inspector.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_authorization_id")
                != initial.get("authorization_id")
                or inspector.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_authorization_sha256")
                != initial_item["sha256"]
                or inspector.get(
                    "deepseek_transport_disconnect_inspector_"
                    "prior_recovery_git_commit") != initial_commit
                or record.get(
                    "deepseek_resume_classifier_prior_authorization_id")
                != inspector.get("authorization_id")
                or record.get(
                    "deepseek_resume_classifier_prior_authorization_sha256")
                != inspector_item["sha256"]
                or record.get(
                    "deepseek_resume_classifier_prior_recovery_git_commit")
                != inspector_commit
                or record.get("committed_results_modified") is not False
                or record.get("checkpoint_rows_modified") is not False
                or record.get("provider_post_replay_scope")
                != "uncommitted_steps_only"
                or not isinstance(inspector_fingerprint, dict)
                or not isinstance(initial_fingerprint, dict)):
            raise RuntimeError(
                "DeepSeek resume-classifier superseded authorization "
                "mismatch")
        if (initial.get(
                "deepseek_transport_disconnect_initial_transaction") is True
                and not _deepseek_initial_recovery_witness_matches(
                    out_dir, initial, initial_item["sha256"])):
            raise RuntimeError(
                "DeepSeek resume-classifier initial authorization witness "
                "mismatch")

        allowed_changed_keys = {
            "authorization_id",
            "created_at",
            "authorization_basis",
            "deepseek_resume_classifier_followup_recovery",
            "deepseek_resume_classifier_prior_authorization_id",
            "deepseek_resume_classifier_prior_authorization_sha256",
            "deepseek_resume_classifier_prior_recovery_git_commit",
            "deepseek_resume_classifier_delta_changed_paths",
            "deepseek_resume_classifier_prefixes",
            "deepseek_resume_classifier_run_metadata_identities",
            "deepseek_resume_classifier_sample",
            "deepseek_resume_classifier_stop_error",
            "deepseek_resume_classifier_evidence",
            "recovery_git_commit",
            "recovery_git_tree_state",
            "recovery_code_fingerprint",
            "changed_code_fingerprint_keys",
            "changed_tracked_paths",
            "archived_stop_path",
            "archived_stop_sha256",
            "archived_emergency_stop_records",
            "superseded_authorization_path",
            "superseded_authorization_sha256",
            "committed_results_modified",
            "checkpoint_rows_modified",
            "provider_post_replay_scope",
        }
        if any(
                record.get(key) != inspector.get(key)
                for key in set(record) | set(inspector)
                if key not in allowed_changed_keys):
            raise RuntimeError(
                "DeepSeek resume-classifier authorization scope changed")

        def _archived_stop_for(layer):
            relative = layer.get("archived_stop_path")
            if not isinstance(relative, str) or not relative:
                return None
            candidate = os.path.realpath(os.path.join(out_dir, relative))
            try:
                inside = (
                    os.path.commonpath([out_dir, candidate]) == out_dir)
            except ValueError:
                inside = False
            if (not inside or not os.path.isfile(candidate)
                    or _sha256_file(candidate)
                    != layer.get("archived_stop_sha256")):
                return None
            try:
                with open(candidate, encoding="utf-8") as handle:
                    value = json.load(handle)
            except (OSError, ValueError):
                return None
            return value if isinstance(value, dict) else None

        initial_stop = _archived_stop_for(initial)
        inspector_stop = _archived_stop_for(inspector)
        if (initial_stop is None
                or initial_stop.get("schema") != STOP_CONDITION_SCHEMA
                or initial_stop.get("condition") != "worker_fatal_error"
                or initial_stop.get("error_type")
                != "OpenAICompatibleTransportError"
                or initial_stop.get("error")
                != "OpenAI-compatible provider failed after 1 attempt(s)"
                or initial.get(
                    "deepseek_transport_disconnect_retry_samples")
                != [initial_stop.get("sample")]
                or initial.get(
                    "deepseek_transport_disconnect_retry_worker_launch_id")
                != initial_stop.get("worker_launch_id")
                or inspector_stop is None
                or inspector_stop.get("schema")
                != STOP_CONDITION_SCHEMA
                or inspector_stop.get("condition")
                != "dispatcher_integrity_failure"
                or inspector_stop.get("worker_launch_id") is not None
                or inspector_stop.get("error_type") != "RuntimeError"
                or inspector_stop.get("error")
                != _DEEPSEEK_TRANSPORT_INSPECTOR_STOP_ERROR):
            raise RuntimeError(
                "DeepSeek resume-classifier historical stop scope mismatch")

        def _git_delta(older, newer):
            return sorted(
                item.replace("\\", "/")
                for item in subprocess.run(
                    [
                        "git", "-C", _HERE, "diff", "--name-only",
                        older, newer, "--",
                    ],
                    check=True, capture_output=True, text=True,
                    encoding="utf-8",
                ).stdout.splitlines()
                if item
            )

        expected_delta_paths = {
            "HP_V8/src/authorize_ledger_lock_recovery.py",
            "HP_V8/src/paired_campaign_dispatch.py",
            "HP_V8/src/run_meta.py",
            "HP_V8/src/test_model_openai.py",
        }
        try:
            inspector_delta = _git_delta(
                initial_commit, inspector_commit)
            classifier_delta = _git_delta(
                inspector_commit, current_commit)
            parent_line = subprocess.run(
                [
                    "git", "-C", _HERE, "rev-list", "--parents",
                    "-n", "1", current_commit,
                ],
                check=True, capture_output=True, text=True,
                encoding="utf-8",
            ).stdout.split()
        except (OSError, subprocess.CalledProcessError, TypeError) as exc:
            raise RuntimeError(
                "cannot audit DeepSeek resume-classifier Git history"
            ) from exc
        inspector_fingerprint_delta = sorted(
            key for key in set(initial_fingerprint) | set(
                inspector_fingerprint)
            if initial_fingerprint.get(key)
            != inspector_fingerprint.get(key)
        )
        classifier_fingerprint_delta = sorted(
            key for key in set(inspector_fingerprint) | set(
                current_fingerprint)
            if inspector_fingerprint.get(key)
            != current_fingerprint.get(key)
        )
        if (set(inspector_delta) != expected_delta_paths
                or inspector_delta != inspector.get(
                    "deepseek_transport_disconnect_inspector_"
                    "delta_changed_paths")
                or inspector_fingerprint_delta != ["run_meta.py"]
                or set(classifier_delta) != expected_delta_paths
                or classifier_delta != record.get(
                    "deepseek_resume_classifier_delta_changed_paths")
                or classifier_fingerprint_delta != ["run_meta.py"]
                or parent_line != [current_commit, inspector_commit]):
            raise RuntimeError(
                "DeepSeek resume-classifier code scope mismatch")

        if not _deepseek_inspector_reprepare_evidence_matches(
                out_dir, inspector, require_live_identity=False):
            raise RuntimeError(
                "DeepSeek historical inspector reprepare evidence mismatch")

        def _prefixes_match(layer, field):
            prefixes = layer.get(field)
            expected_names = {"api_calls.jsonl", "dispatch_log.jsonl"}
            return (
                isinstance(prefixes, dict)
                and set(prefixes) == expected_names
                and all(
                    _file_prefix_matches(
                        os.path.join(out_dir, name), prefixes[name])
                    for name in expected_names
                )
            )

        if (not _prefixes_match(
                inspector,
                "deepseek_transport_disconnect_inspector_prefixes")
                or not _prefixes_match(
                    record, "deepseek_resume_classifier_prefixes")
                or not _run_metadata_recovery_prefix_matches(
                    os.path.join(out_dir, "run_metadata.jsonl"),
                    inspector.get(
                        "deepseek_transport_disconnect_inspector_"
                        "run_metadata_identities"))
                or not _run_metadata_recovery_prefix_matches(
                    os.path.join(out_dir, "run_metadata.jsonl"),
                    record.get(
                        "deepseek_resume_classifier_"
                        "run_metadata_identities"))):
            raise RuntimeError(
                "DeepSeek resume-classifier evidence prefix mismatch")

        sample = record.get("deepseek_resume_classifier_sample")
        evidence = record.get("deepseek_resume_classifier_evidence")
        expected_error = (
            _DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX
            + str(sample) + f"; evidence={evidence}"
        )
        recovered_by_sample = {
            item.get("sample"): item
            for item in record.get("deepseek_recovered_workers") or []
            if isinstance(item, dict)
        }
        if (not isinstance(sample, str) or not sample
                or not isinstance(evidence, list) or not evidence
                or record.get("deepseek_resume_classifier_stop_error")
                != expected_error
                or recovered_by_sample.get(sample, {}).get("status")
                != "failed"
                or sample not in (
                    record.get("deepseek_resume_samples") or [])
                or sample in (
                    record.get("deepseek_pending_samples") or [])):
            raise RuntimeError(
                "DeepSeek resume-classifier sample scope mismatch")

        inspector_expected_witness = {
            "event": (
                "user_authorized_deepseek_transport_inspector_followup"),
            "created_at": inspector.get("created_at"),
            "campaign_recovery_authorization_id": inspector.get(
                "authorization_id"),
            "campaign_recovery_authorization_sha256": (
                inspector_item["sha256"]),
            "superseded_authorization_id": initial.get(
                "authorization_id"),
            "incident_api_rows": len(
                inspector.get("incident_api_rows") or []),
            "prior_recovery_git_commit": initial_commit,
            "recovery_git_commit": inspector_commit,
        }
        classifier_expected_witness = {
            "event": (
                "user_authorized_deepseek_resume_classifier_followup"),
            "created_at": record.get("created_at"),
            "campaign_recovery_authorization_id": record.get(
                "authorization_id"),
            "campaign_recovery_authorization_sha256": _sha256_file(
                authorization_path),
            "superseded_authorization_id": inspector.get(
                "authorization_id"),
            "incident_api_rows": len(record.get("incident_api_rows") or []),
            "prior_recovery_git_commit": inspector_commit,
            "recovery_git_commit": current_commit,
            "resume_classifier_sample": sample,
        }
        dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
        dispatch_rows = _read_jsonl_records_with_retry(dispatch_path)
        for layer, prefix_field, expected_witness in (
                (
                    inspector,
                    "deepseek_transport_disconnect_inspector_prefixes",
                    inspector_expected_witness,
                ),
                (
                    record,
                    "deepseek_resume_classifier_prefixes",
                    classifier_expected_witness,
                )):
            witness_rows = [
                row for row in dispatch_rows
                if row.get("event") == expected_witness["event"]
                and row.get("campaign_recovery_authorization_id")
                == layer.get("authorization_id")
            ]
            if (len(witness_rows) != 1
                    or witness_rows[0] != expected_witness
                    or not _jsonl_first_suffix_record_matches(
                        dispatch_path,
                        layer[prefix_field]["dispatch_log.jsonl"],
                        expected_witness)):
                raise RuntimeError(
                    "DeepSeek resume-classifier dispatch witness mismatch")
    prior_fingerprint = record.get("prior_code_fingerprint")
    if not isinstance(prior_fingerprint, dict):
        raise RuntimeError("campaign recovery prior fingerprint is invalid")
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(current_fingerprint)
        if prior_fingerprint.get(key) != current_fingerprint.get(key)
    )
    if dispatcher_process_lost_recovery:
        expected_fingerprint_changes = fingerprint_changes
    elif record.get(
            "deepseek_transport_disconnect_retry_recovery") is True:
        expected_fingerprint_changes = ["model_openai.py", "run_meta.py"]
    else:
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
    elif dispatcher_process_lost_recovery:
        changed_scope_valid = True
    elif record.get(
            "deepseek_transport_disconnect_retry_recovery") is True:
        changed_scope_valid = (
            changed_set
            == _DEEPSEEK_TRANSPORT_DISCONNECT_RECOVERY_CHANGED_PATHS
        )
    elif record.get("deepseek_server_retry_recovery") is True:
        changed_scope_valid = (
            changed_set <= _LEDGER_LOCK_RECOVERY_ALLOWED_CHANGED_PATHS
            and {
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/paired_campaign_dispatch.py",
                "HP_V8/src/run_meta.py",
            } <= changed_set
        )
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

        deepseek_server_retry = (
            record.get("deepseek_server_retry_recovery") is True)
        deepseek_transport_disconnect_retry = (
            record.get(
                "deepseek_transport_disconnect_retry_recovery") is True
        )
        deepseek_retry = (
            deepseek_server_retry
            or deepseek_transport_disconnect_retry
        )
        deepseek_worker_scope = record.get("deepseek_recovered_workers")
        if (not deepseek_retry
                and deepseek_worker_scope is not None
                and deepseek_worker_scope != []):
            raise RuntimeError(
                "non-DeepSeek recovery carries DeepSeek worker scope")
        validated_api = _validate_rows("api_calls.jsonl", api_incidents)
        validated_attempts = (
            []
            if deepseek_retry and not attempt_incidents
            else _validate_rows(
                "api_attempt_ledger.jsonl", attempt_incidents)
        )
        if deepseek_retry:
            retry_samples = record.get(
                "deepseek_server_retry_samples"
                if deepseek_server_retry
                else "deepseek_transport_disconnect_retry_samples"
            )
            resume_samples = record.get("deepseek_resume_samples")
            pending_samples = record.get("deepseek_pending_samples")
            recovered_workers = record.get("deepseek_recovered_workers")
            if (not isinstance(retry_samples, list)
                    or not retry_samples
                    or len(retry_samples) != len(set(retry_samples))
                    or any(not isinstance(item, str) or not item
                           for item in retry_samples)
                    or not isinstance(resume_samples, list)
                    or not resume_samples
                    or len(resume_samples) != len(set(resume_samples))
                    or not set(retry_samples) <= set(resume_samples)
                    or not isinstance(pending_samples, list)
                    or len(pending_samples) != len(set(pending_samples))
                    or not set(pending_samples) <= set(resume_samples)
                    or not _deepseek_recovered_worker_scope_is_valid(
                        recovered_workers,
                        worker_ids,
                        resume_samples,
                        pending_samples,
                    )):
                raise RuntimeError(
                    "campaign DeepSeek retry sample scope is invalid")

            metadata_rows = read_run_metadata_snapshot(out_dir)
            dispatch_rows = _read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            for recovered_worker in recovered_workers:
                metadata_matches = [
                    row for row in metadata_rows
                    if recovered_worker["sample"] in (
                        row.get("samples") or [])
                    and all(
                        row.get(key) == recovered_worker[key]
                        for key in (
                            "status",
                            "worker_launch_id",
                            "worker_pid",
                            "invocation_id",
                        )
                    )
                ]
                exit_matches = [
                    row for row in dispatch_rows
                    if row.get("event") == "worker_exit"
                    and row.get("sample") == recovered_worker["sample"]
                    and row.get("worker_launch_id")
                    == recovered_worker["worker_launch_id"]
                    and row.get("pid") == recovered_worker["worker_pid"]
                    and row.get("returncode") != 0
                    and row.get("disposition") == "campaign_fatal"
                ]
                if len(metadata_matches) != 1 or len(exit_matches) != 1:
                    raise RuntimeError(
                        "campaign DeepSeek recovered worker evidence "
                        "mismatch")

            committed_call_ids = set()
            for method in ("hybridpatch", "fullrewrite"):
                for sample in resume_samples:
                    result_path = os.path.join(
                        out_dir, method, f"{sample}.jsonl")
                    if not os.path.isfile(result_path):
                        continue
                    for result_row in _read_jsonl_records_with_retry(
                            result_path):
                        committed_call_ids.update(
                            result_row.get("api_call_ids") or [])
            sidecar_entries = record.get(
                "incident_transport_sidecars", [])
            if deepseek_transport_disconnect_retry:
                dispatcher_stopped_rows = [
                    row for entry, row in validated_api
                    if entry.get("incident_kind")
                    == "deepseek_dispatcher_stopped_uncommitted_api"
                ]
                if (len(dispatcher_stopped_rows) != 1
                        or not isinstance(sidecar_entries, list)
                        or len(sidecar_entries) != 1
                        or sidecar_entries[0]
                        != _deepseek_dispatcher_stopped_sidecar_evidence(
                            out_dir, dispatcher_stopped_rows[0])):
                    raise RuntimeError(
                        "campaign DeepSeek dispatcher-stop transport "
                        "evidence is invalid"
                    )
            elif sidecar_entries:
                raise RuntimeError(
                    "campaign DeepSeek server retry transport evidence "
                    "is invalid"
                )
            observed_retry_samples = set()
            for entry, row in validated_api:
                attempts = row.get("transport_attempts")
                final_attempt = attempts[-1] if isinstance(
                    attempts, list) and attempts else {}
                final_budget = final_attempt.get("retry_budget_attempt_index")
                incident_kind = entry.get("incident_kind")
                if (row.get("model") != "deepseek-v4-flash"
                        or row.get("sample") not in set(resume_samples)
                        or row.get("worker_launch_id") not in worker_ids
                        or row.get("provider_called") is not True
                        or row.get("response_replayed") is not False
                        or row.get("request_id") in committed_call_ids
                        or incident_kind not in {
                            "deepseek_server_retry_exhaustion",
                            (
                                "deepseek_transport_disconnect_"
                                "misclassification"
                            ),
                            "deepseek_dispatcher_stopped_uncommitted_api",
                            "deepseek_interrupted_uncommitted_api",
                        }):
                    raise RuntimeError(
                        "campaign DeepSeek retry API evidence is invalid")
                if incident_kind == "deepseek_server_retry_exhaustion":
                    observed_retry_samples.add(row.get("sample"))
                    if (row.get("classification")
                            != "provider/API failure"
                            or row.get("error_type") != "server_error"
                            or row.get("http_status") not in {502, 503}
                            or row.get("stream_complete") is not False
                            or row.get("count_as_method_failure") is not False
                            or not isinstance(final_budget, int)
                            or isinstance(final_budget, bool)
                            or final_budget < 3
                            or final_attempt.get("status")
                            != "retryable_error"
                            or final_attempt.get(
                                "retry_budget_consumed") is not True):
                        raise RuntimeError(
                            "campaign DeepSeek retry API evidence is invalid")
                elif incident_kind == (
                        "deepseek_transport_disconnect_misclassification"):
                    observed_retry_samples.add(row.get("sample"))
                    if (not deepseek_transport_disconnect_retry
                            or row.get("classification")
                            != "runner_exception"
                            or row.get("error_type")
                            != "transport_disconnect"
                            or row.get("http_status") is not None
                            or row.get("stream_complete") is not False
                            or row.get("count_as_method_failure") is not True
                            or row.get("http_attempts_used") != 1
                            or row.get("retry_count") != 0
                            or row.get("failed_attempt_count") != 1
                            or row.get("max_retries") != 3
                            or len(attempts) != 1
                            or final_budget != 1
                            or final_attempt.get("status") != "fatal_error"
                            or final_attempt.get("http_status") is not None
                            or final_attempt.get("error_type")
                            != "transport_disconnect"
                            or final_attempt.get("error_message")
                            != "Connection error."
                            or final_attempt.get(
                                "response_started_http_status") is not None
                            or final_attempt.get(
                                "generation_delta_seen") is not False
                            or final_attempt.get(
                                "retry_budget_consumed") is not True
                            or row.get("runner_exception")
                            != (
                                "OpenAICompatibleTransportError: "
                                "OpenAI-compatible provider failed after "
                                "1 attempt(s)"
                            )):
                        raise RuntimeError(
                            "campaign DeepSeek transport-disconnect evidence "
                            "is invalid"
                        )
                elif incident_kind == (
                        "deepseek_dispatcher_stopped_uncommitted_api"):
                    if (not deepseek_transport_disconnect_retry
                            or row.get("classification")
                            != "runner_exception"
                            or row.get("error_type")
                            != "CampaignStoppedError"
                            or row.get("http_status") is not None
                            or row.get("stream_complete") is not False
                            or row.get("count_as_method_failure") is not True
                            or row.get("http_attempts_used") is not None
                            or attempts != []
                            or row.get("runner_exception")
                            != (
                                "CampaignStoppedError: campaign stop latch "
                                "is set: worker_fatal_error"
                            )):
                        raise RuntimeError(
                            "campaign DeepSeek dispatcher-stop evidence "
                            "is invalid"
                        )
                elif (incident_kind
                      != "deepseek_interrupted_uncommitted_api"
                      or row.get("classification") is not None
                      or row.get("http_status") != 200
                      or row.get("stream_complete") is not True
                      or row.get("input_tokens") is None
                      or row.get("output_tokens") is None
                      or not isinstance(attempts, list) or not attempts
                      or attempts[-1].get("status") != "success"):
                    raise RuntimeError(
                        "campaign DeepSeek interrupted API evidence is invalid")
            if observed_retry_samples != set(retry_samples):
                raise RuntimeError(
                    "campaign DeepSeek retry API scope is invalid")
        elif any(
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
        attempt_rows_all = (
            []
            if deepseek_retry
            else _read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
        )
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
            and (deepseek_retry
                 or row.get("method_phase") == "fullrewrite")
        }
        if not set(worker_ids) <= set(launches):
            raise RuntimeError("campaign recovery worker cohort is not dispatched")
        preauthorization_set = set(preauthorization_worker_ids)
        metadata_rows = read_run_metadata_snapshot(out_dir)
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
        attempt_workers = (
            set()
            if deepseek_retry
            else {
                row.get("worker_launch_id")
                for row in _read_jsonl_records_with_retry(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            }
        )
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
    elif dispatcher_process_lost_recovery:
        manifest_config = manifest.get("config") or {}
        if (manifest_config.get("campaign_role") != "deepseek_full234"
                or manifest_config.get("model") != "deepseek-v4-flash"
                or manifest_config.get("num_round_trips") != 10
                or set(manifest_config.get("method_set") or [])
                != {"hybridpatch", "fullrewrite"}
                or manifest_config.get("transport")
                != "openai_sdk_stream"
                or manifest_config.get("transport_revision") not in {
                    "opencode_openai_compatible/4",
                    "opencode_openai_compatible/5",
                    "opencode_openai_compatible/6",
                }):
            raise RuntimeError(
                "dispatcher parent-loss manifest contract is invalid")
        for item in recovery_chain:
            historical = item["record"]
            if (historical.get("recovery_kind")
                    == DISPATCHER_PROCESS_LOST_RECOVERY_KIND):
                _validate_dispatcher_parent_loss_pending_reprepare_witnesses(
                    out_dir,
                    historical,
                    authorization_path=item["path"],
                )
        assignments = manifest.get("assignments")
        methods_by_sample = {}
        if not isinstance(assignments, list):
            raise RuntimeError(
                "dispatcher parent-loss manifest assignments are invalid")
        for assignment in assignments:
            sample = (
                assignment.get("sample")
                if isinstance(assignment, dict) else None
            )
            methods = (
                assignment.get("methods")
                if isinstance(assignment, dict) else None
            )
            if (not isinstance(sample, str) or not sample
                    or sample in methods_by_sample
                    or methods not in (
                        ["hybridpatch", "fullrewrite"],
                        ["fullrewrite", "hybridpatch"],
                    )):
                raise RuntimeError(
                    "dispatcher parent-loss manifest assignments are invalid")
            methods_by_sample[sample] = methods
        dispatcher_pid = record.get("dispatcher_pid")
        dispatcher_instance_id = record.get("dispatcher_instance_id")
        workers = record.get("dispatcher_parent_loss_workers")
        cumulative_resume_workers = record.get(
            "dispatcher_parent_loss_resume_workers")
        resume_samples = record.get(
            "dispatcher_parent_loss_resume_samples")
        worker_ids = record.get("recovered_worker_launch_ids")
        preauthorization_worker_ids = record.get(
            "preauthorization_worker_launch_ids", [])
        if (not isinstance(dispatcher_pid, int)
                or isinstance(dispatcher_pid, bool) or dispatcher_pid <= 0
                or not isinstance(dispatcher_instance_id, str)
                or not dispatcher_instance_id
                or not isinstance(workers, list) or not workers
                or not isinstance(cumulative_resume_workers, list)
                or not isinstance(resume_samples, list)
                or len(resume_samples) != len(set(resume_samples))
                or any(not isinstance(item, str) or not item
                       for item in resume_samples)
                or not isinstance(worker_ids, list) or not worker_ids
                or len(worker_ids) != len(set(worker_ids))
                or any(not isinstance(item, str) or not item
                       for item in worker_ids)
                or not isinstance(preauthorization_worker_ids, list)
                or len(preauthorization_worker_ids)
                != len(set(preauthorization_worker_ids))
                or any(not isinstance(item, str) or not item
                       for item in preauthorization_worker_ids)
                or not set(preauthorization_worker_ids) <= set(worker_ids)):
            raise RuntimeError(
                "dispatcher parent-loss recovery scope is invalid")

        current_workers = {}
        current_samples = set()
        for item in workers:
            worker_id = (
                item.get("worker_launch_id")
                if isinstance(item, dict) else None
            )
            sample = item.get("sample") if isinstance(item, dict) else None
            status = item.get("status") if isinstance(item, dict) else None
            worker_pid = (
                item.get("worker_pid") if isinstance(item, dict) else None
            )
            invocation_id = (
                item.get("invocation_id") if isinstance(item, dict) else None
            )
            ordinary_status = status in {
                "finished", "infrastructure_incomplete",
                "evaluator_incomplete", "interrupted_by_dispatcher",
            }
            if (not isinstance(worker_id, str) or not worker_id
                    or worker_id in current_workers
                    or not isinstance(sample, str) or not sample
                    or sample in current_samples
                    or sample not in methods_by_sample
                    or status not in {
                        "finished", "infrastructure_incomplete",
                        "evaluator_incomplete",
                        "interrupted_by_dispatcher", "preauthorization",
                        "registered_prelaunch",
                    }
                    or (
                        ordinary_status
                        and (
                            not isinstance(worker_pid, int)
                            or isinstance(worker_pid, bool)
                            or worker_pid <= 0
                            or not isinstance(invocation_id, str)
                            or not invocation_id
                        )
                    )
                    or (
                        status == "preauthorization"
                        and (
                            not isinstance(worker_pid, int)
                            or isinstance(worker_pid, bool)
                            or worker_pid <= 0
                            or invocation_id is not None
                        )
                    )
                    or (
                        status == "registered_prelaunch"
                        and (worker_pid is not None
                             or invocation_id is not None)
                    )):
                raise RuntimeError(
                    "dispatcher parent-loss worker evidence is invalid")
            current_workers[worker_id] = item
            current_samples.add(sample)
        current_resume_samples = {
            item["sample"] for item in workers
            if item["status"] in {
                "interrupted_by_dispatcher", "preauthorization",
                "registered_prelaunch",
            }
        }
        current_preauthorization_ids = {
            item["worker_launch_id"] for item in workers
            if item["status"] == "preauthorization"
        }
        current_registered_ids = {
            item["worker_launch_id"] for item in workers
            if item["status"] == "registered_prelaunch"
        }
        if (set(resume_samples) != current_resume_samples
                or not set(current_workers) <= set(worker_ids)
                or not set(resume_samples) <= set(
                    record.get("provider_access_resume_samples") or [])):
            raise RuntimeError(
                "dispatcher parent-loss resume scope is invalid")
        cumulative_worker_ids = [
            item.get("worker_launch_id")
            if isinstance(item, dict) else None
            for item in cumulative_resume_workers
        ]
        cumulative_samples = [
            item.get("sample") if isinstance(item, dict) else None
            for item in cumulative_resume_workers
        ]
        if (len(cumulative_worker_ids) != len(set(cumulative_worker_ids))
                or len(cumulative_samples) != len(set(cumulative_samples))
                or any(
                    not isinstance(worker_id, str) or not worker_id
                    for worker_id in cumulative_worker_ids
                )
                or any(
                    not isinstance(sample, str) or not sample
                    for sample in cumulative_samples
                )
                or not set(cumulative_worker_ids) <= set(worker_ids)):
            raise RuntimeError(
                "dispatcher parent-loss cumulative resume worker scope "
                "is invalid")
        archived_stop_worker = current_workers.get(
            archived_stop.get("worker_launch_id"))
        registered_prelaunch_only_stop = (
            archived_stop.get("registered_prelaunch_only") is True
            and archived_stop.get("worker_launch_id") is None
            and archived_stop.get("worker_pid") is None
            and set(archived_stop.get(
                "registered_worker_launch_ids") or [])
            == set(current_workers)
            and current_registered_ids == set(current_workers)
            and len(archived_stop.get(
                "registered_worker_launch_ids") or [])
            == len(current_workers)
        )
        ordinary_worker_stop = (
            archived_stop.get("registered_prelaunch_only") is not True
            and isinstance(archived_stop_worker, dict)
            and archived_stop.get("worker_pid")
            == archived_stop_worker.get("worker_pid")
        )
        dispatcher_operator_pause_worker_stop = (
            record.get("dispatcher_operator_pause_recovery") is True
            and archived_stop.get("condition")
            == "operator_directed_dispatcher_pause"
            and archived_stop.get("worker_launch_id") is None
            and archived_stop.get("worker_pid") == dispatcher_pid
            and archived_stop.get("active_worker_launch_ids")
            == sorted(current_workers)
            and archived_stop.get("active_worker_count")
            == len(current_workers)
            and archived_stop.get("dispatch_manifest_sha256")
            == record.get("dispatch_manifest_sha256")
            and archived_stop.get("active_worker_set_sha256")
            == record.get("archived_active_worker_set_sha256")
            and archived_stop.get("publication_mode")
            in {"canonical", "emergency_fallback"}
        )
        if not (
                registered_prelaunch_only_stop
                or ordinary_worker_stop
                or dispatcher_operator_pause_worker_stop):
            raise RuntimeError(
                "dispatcher parent-loss canonical stop worker mismatch")

        if len(recovery_chain) > 1:
            superseded_item = recovery_chain[1]
            superseded = superseded_item["record"]
            identity_unchanged = (
                record.get("recovery_git_commit")
                == superseded.get("recovery_git_commit")
                and record.get("recovery_code_fingerprint")
                == superseded.get("recovery_code_fingerprint")
            )
            identity_transition_valid = (
                not identity_unchanged
                and _dispatcher_parent_loss_code_transition_matches(
                    record, superseded_item)
            )
            prior_worker_ids = set(
                superseded.get("recovered_worker_launch_ids") or [])
            prior_preauthorization_ids = set(
                superseded.get(
                    "preauthorization_worker_launch_ids") or [])
            prior_resume_samples = set(
                superseded.get("provider_access_resume_samples") or [])
            prior_provider_retries = list(
                superseded.get(
                    "provider_access_retry_authorizations") or [])
            prior_parent_resume_workers = list(
                superseded.get(
                    "dispatcher_parent_loss_resume_workers")
                or superseded.get("dispatcher_parent_loss_workers")
                or []
            )
        else:
            superseded = None
            identity_unchanged = (
                record.get("recovery_git_commit")
                == record.get("prior_git_commit")
                and record.get("recovery_code_fingerprint")
                == record.get("prior_code_fingerprint")
            )
            identity_transition_valid = False
            prior_worker_ids = set()
            prior_preauthorization_ids = set()
            prior_resume_samples = set()
            prior_provider_retries = []
            prior_parent_resume_workers = []
        resumable_parent_loss_statuses = {
            "interrupted_by_dispatcher",
            "preauthorization",
            "registered_prelaunch",
        }
        expected_parent_resume_by_sample = {}
        for item in prior_parent_resume_workers:
            sample = item.get("sample") if isinstance(item, dict) else None
            if not isinstance(sample, str) or not sample:
                raise RuntimeError(
                    "dispatcher parent-loss inherited resume worker scope "
                    "is invalid")
            expected_parent_resume_by_sample[sample] = dict(item)
        terminal_current_samples = set()
        for item in workers:
            sample = item.get("sample") if isinstance(item, dict) else None
            status = item.get("status") if isinstance(item, dict) else None
            if not isinstance(sample, str) or not sample:
                raise RuntimeError(
                    "dispatcher parent-loss current resume worker scope "
                    "is invalid")
            if status in resumable_parent_loss_statuses:
                expected_parent_resume_by_sample[sample] = dict(item)
            else:
                expected_parent_resume_by_sample.pop(sample, None)
                terminal_current_samples.add(sample)
        expected_parent_resume_workers = [
            expected_parent_resume_by_sample[sample]
            for sample in sorted(expected_parent_resume_by_sample)
        ]
        if (not (identity_unchanged or identity_transition_valid)
                or set(worker_ids)
                != prior_worker_ids | set(current_workers)
                or set(preauthorization_worker_ids)
                != prior_preauthorization_ids
                | current_preauthorization_ids
                or set(record.get(
                    "dispatcher_parent_loss_registered_prelaunch_worker_launch_ids"
                ) or []) != current_registered_ids
                or set(record.get("provider_access_resume_samples") or [])
                != (
                    prior_resume_samples | set(resume_samples)
                ) - terminal_current_samples
                or record.get("provider_access_retry_authorizations")
                != prior_provider_retries
                or cumulative_resume_workers
                != expected_parent_resume_workers
                or record.get("committed_results_modified") is not False
                or record.get("checkpoint_rows_modified") is not False
                or record.get("provider_post_replay_scope")
                != (
                    "none_required"
                    if record.get("dispatcher_operator_pause_recovery") is True
                    else "uncommitted_steps_only"
                )):
            raise RuntimeError(
                "dispatcher parent-loss recovery identity changed")

        def _bound_recovery_file(path_field, digest_field, label):
            relative = record.get(path_field)
            if not isinstance(relative, str) or not relative:
                raise RuntimeError(
                    f"dispatcher parent-loss {label} path is invalid")
            candidate = os.path.realpath(os.path.join(out_dir, relative))
            if (os.path.commonpath([out_dir, candidate]) != out_dir
                    or not os.path.isfile(candidate)
                    or _sha256_file(candidate) != record.get(digest_field)):
                raise RuntimeError(
                    f"dispatcher parent-loss {label} digest mismatch")
            return candidate

        archived_active_path = _bound_recovery_file(
            "archived_active_worker_set_path",
            "archived_active_worker_set_sha256",
            "active worker archive",
        )
        archived_metadata_path = _bound_recovery_file(
            "archived_run_metadata_path",
            "archived_run_metadata_sha256",
            "metadata archive",
        )
        try:
            with open(archived_active_path, encoding="utf-8") as handle:
                archived_active = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "dispatcher parent-loss active worker archive is invalid"
            ) from exc
        expected_archived_workers = {
            worker_id: {"sample": item["sample"]}
            for worker_id, item in current_workers.items()
        }
        if (not isinstance(archived_active, dict)
                or archived_active.get("schema")
                != "anchorpatch.active_worker_set/1"
                or archived_active.get("run_git_commit")
                != record.get("prior_git_commit")
                or archived_active.get("dispatcher_pid") != dispatcher_pid
                or archived_active.get("dispatcher_instance_id")
                != dispatcher_instance_id
                or archived_active.get("workers")
                != expected_archived_workers):
            raise RuntimeError(
                "dispatcher parent-loss active worker archive mismatch")
        if (record.get("dispatcher_operator_pause_recovery") is True
                and archived_stop.get(
                    "active_worker_set_canonical_sha256")
                != _canonical_record_sha256(archived_active)):
            raise RuntimeError(
                "operator pause active worker archive witness mismatch")
        try:
            archived_metadata = _read_run_metadata_strict(
                archived_metadata_path)
        except RuntimeError as exc:
            raise RuntimeError(
                "dispatcher parent-loss metadata archive is invalid") from exc
        metadata_identity_evidence = record.get(
            "dispatcher_parent_loss_run_metadata_identities")
        current_has_event_metadata = os.path.isfile(
            _run_metadata_events_path(out_dir))
        if current_has_event_metadata:
            if (not isinstance(metadata_identity_evidence, dict)
                    or not _run_metadata_recovery_prefix_matches(
                        os.path.join(out_dir, "run_metadata.jsonl"),
                        metadata_identity_evidence)
                    or metadata_identity_evidence.get(
                        "projection_record_count") != len(archived_metadata)
                    or metadata_identity_evidence.get("projection_sha256")
                    != _run_metadata_projection_sha256(archived_metadata)):
                raise RuntimeError(
                    "dispatcher parent-loss event metadata identity mismatch")
        elif metadata_identity_evidence is not None:
            raise RuntimeError(
                "dispatcher parent-loss metadata identity mode mismatch")
        metadata_entries = record.get(
            "dispatcher_parent_loss_metadata_rows")
        if not isinstance(metadata_entries, list):
            raise RuntimeError(
                "dispatcher parent-loss metadata evidence is invalid")
        metadata_worker_ids = {
            worker_id for worker_id, item in current_workers.items()
            if item["status"] not in {
                "preauthorization", "registered_prelaunch",
            }
        }
        bound_metadata = {}
        for entry in metadata_entries:
            number = (
                entry.get("row_number")
                if isinstance(entry, dict) else None
            )
            digest = (
                entry.get("canonical_sha256")
                if isinstance(entry, dict) else None
            )
            if (not isinstance(number, int) or isinstance(number, bool)
                    or not 1 <= number <= len(archived_metadata)
                    or not isinstance(digest, str)
                    or digest != _canonical_record_sha256(
                        archived_metadata[number - 1])):
                raise RuntimeError(
                    "dispatcher parent-loss metadata evidence mismatch")
            metadata_row = archived_metadata[number - 1]
            worker_id = metadata_row.get("worker_launch_id")
            worker_item = current_workers.get(worker_id) or {}
            if (worker_id not in metadata_worker_ids
                    or worker_id in bound_metadata
                    or entry.get("invocation_id")
                    != metadata_row.get("invocation_id")
                    or entry.get("prior_status")
                    != metadata_row.get("status")
                    or metadata_row.get("samples")
                    != [worker_item.get("sample")]
                    or metadata_row.get("worker_pid")
                    != worker_item.get("worker_pid")
                    or metadata_row.get("dispatcher_pid")
                    != dispatcher_pid
                    or metadata_row.get("dispatcher_instance_id")
                    != dispatcher_instance_id
                    or metadata_row.get("methods")
                    != methods_by_sample[worker_item.get("sample")]):
                raise RuntimeError(
                    "dispatcher parent-loss metadata evidence mismatch")
            bound_metadata[worker_id] = metadata_row
        if set(bound_metadata) != metadata_worker_ids:
            raise RuntimeError(
                "dispatcher parent-loss metadata scope is incomplete")

        active_path = os.path.join(out_dir, "active_worker_set.json")
        try:
            with open(active_path, encoding="utf-8") as handle:
                active_now = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "dispatcher parent-loss active worker set is invalid") from exc
        if (not isinstance(active_now, dict)
                or active_now.get("schema")
                != "anchorpatch.active_worker_set/1"
                or active_now.get("run_git_commit")
                != record.get("prior_git_commit")
                or not isinstance(active_now.get("workers"), dict)
                or set(active_now.get("workers")) & set(current_workers)):
            raise RuntimeError(
                "dispatcher parent-loss recovered workers remain active")

        metadata_now = read_run_metadata_snapshot(out_dir)
        current_metadata = {}
        for row in metadata_now:
            worker_id = row.get("worker_launch_id")
            if worker_id not in current_workers:
                continue
            if worker_id in current_metadata:
                raise RuntimeError(
                    "dispatcher parent-loss current metadata is duplicated")
            current_metadata[worker_id] = row
        if set(current_metadata) != metadata_worker_ids:
            raise RuntimeError(
                "dispatcher parent-loss current metadata scope is invalid")
        for worker_id in metadata_worker_ids:
            item = current_workers[worker_id]
            before = bound_metadata[worker_id]
            after = current_metadata.get(worker_id) or {}
            if (after.get("invocation_id") != item["invocation_id"]
                    or after.get("worker_pid") != item["worker_pid"]
                    or after.get("samples") != [item["sample"]]
                    or after.get("dispatcher_pid") != dispatcher_pid
                    or after.get("dispatcher_instance_id")
                    != dispatcher_instance_id
                    or after.get("status") != item["status"]
                    or before.get("status") not in {
                        item["status"], "running"}):
                raise RuntimeError(
                    "dispatcher parent-loss metadata transition mismatch")

        emergency_entries = record.get(
            "archived_emergency_stop_records")
        if not isinstance(emergency_entries, list):
            raise RuntimeError(
                "dispatcher parent-loss emergency stop evidence is invalid")
        archived_emergency_dir = os.path.join(
            os.path.dirname(archived_path), EMERGENCY_STOP_DIRECTORY)
        actual_emergency_paths = set()
        if os.path.isdir(archived_emergency_dir):
            actual_emergency_paths = {
                os.path.relpath(
                    os.path.join(archived_emergency_dir, name), out_dir
                ).replace("\\", "/")
                for name in os.listdir(archived_emergency_dir)
                if name.endswith(".json")
            }
        emergency_paths = set()
        for entry in emergency_entries:
            if not isinstance(entry, dict):
                raise RuntimeError(
                    "dispatcher parent-loss emergency stop evidence is invalid")
            relative = entry.get("path")
            if (not isinstance(relative, str) or not relative
                    or relative in emergency_paths):
                raise RuntimeError(
                    "dispatcher parent-loss emergency stop evidence is invalid")
            emergency_paths.add(relative)
            candidate = os.path.realpath(os.path.join(out_dir, relative))
            if (os.path.commonpath([out_dir, candidate]) != out_dir
                    or not os.path.isfile(candidate)
                    or _sha256_file(candidate) != entry.get("sha256")):
                raise RuntimeError(
                    "dispatcher parent-loss emergency stop digest mismatch")
            stop_record = _read_stop_record(candidate)
            stop_worker = current_workers.get(
                stop_record.get("worker_launch_id"))
            if (stop_record.get("condition")
                    != "dispatcher_process_lost"
                    or stop_record.get("dispatcher_pid") != dispatcher_pid
                    or stop_record.get("dispatcher_instance_id")
                    != dispatcher_instance_id
                    or not isinstance(stop_worker, dict)
                    or stop_record.get("worker_pid")
                    != stop_worker.get("worker_pid")):
                raise RuntimeError(
                    "dispatcher parent-loss emergency stop identity mismatch")
        if emergency_paths != actual_emergency_paths:
            raise RuntimeError(
                "dispatcher parent-loss emergency stop scope mismatch")

        dispatch_rows = _read_jsonl_records_with_retry(
            os.path.join(out_dir, "dispatch_log.jsonl"))
        intents = {}
        launches = {}
        authorizations = {}
        exits = {}
        reconciliations = {}
        for row in dispatch_rows:
            worker_id = row.get("worker_launch_id")
            if worker_id not in current_workers:
                continue
            target = None
            if row.get("event") == "launch_intent":
                target = intents
            elif row.get("event") == "launch":
                target = launches
            elif row.get("event") == "worker_authorized":
                target = authorizations
            elif row.get("event") == "worker_exit":
                target = exits
            elif row.get("event") == "stale_worker_reconciled":
                target = reconciliations
            if target is not None:
                if worker_id in target:
                    raise RuntimeError(
                        "dispatcher parent-loss dispatch evidence is duplicated")
                target[worker_id] = row
        expected_launched_ids = set(current_workers) - current_registered_ids
        if set(launches) != expected_launched_ids:
            raise RuntimeError(
                "dispatcher parent-loss launch scope is incomplete")
        for worker_id, item in current_workers.items():
            intent = intents.get(worker_id)
            if item["status"] == "registered_prelaunch":
                if (worker_id in launches
                        or worker_id in authorizations
                        or worker_id in exits
                        or (
                            intent is not None
                            and (
                                intent.get("sample") != item["sample"]
                                or intent.get("methods")
                                != methods_by_sample[item["sample"]]
                                or intent.get("dispatcher_pid")
                                != dispatcher_pid
                                or intent.get("dispatcher_instance_id")
                                != dispatcher_instance_id
                            )
                        )):
                    raise RuntimeError(
                        "dispatcher parent-loss registered-prelaunch "
                        "evidence mismatch")
                continue
            launch = launches[worker_id]
            exit_row = exits.get(worker_id)
            if (not isinstance(intent, dict)
                    or intent.get("sample") != item["sample"]
                    or intent.get("methods")
                    != methods_by_sample[item["sample"]]
                    or intent.get("dispatcher_pid") != dispatcher_pid
                    or intent.get("dispatcher_instance_id")
                    != dispatcher_instance_id
                    or launch.get("sample") != item["sample"]
                    or launch.get("methods")
                    != methods_by_sample[item["sample"]]
                    or launch.get("pid") != item["worker_pid"]
                    or launch.get("dispatcher_pid") != dispatcher_pid
                    or launch.get("dispatcher_instance_id")
                    != dispatcher_instance_id
                    or not isinstance(exit_row, dict)
                    or exit_row.get("sample") != item["sample"]
                    or exit_row.get("pid") != item["worker_pid"]):
                raise RuntimeError(
                    "dispatcher parent-loss dispatch provenance mismatch")
            authorization = authorizations.get(worker_id)
            if authorization is not None and (
                    authorization.get("sample") != item["sample"]
                    or authorization.get("worker_pid")
                    != item["worker_pid"]
                    or authorization.get("invocation_id")
                    != item["invocation_id"]
                    or authorization.get("dispatcher_pid") != dispatcher_pid
                    or authorization.get("dispatcher_instance_id")
                    != dispatcher_instance_id):
                raise RuntimeError(
                    "dispatcher parent-loss authorization provenance mismatch")
            if item["status"] == "preauthorization":
                if (authorization is not None
                        or exit_row.get("returncode") != 97
                        or exit_row.get("disposition") != "campaign_fatal"):
                    raise RuntimeError(
                        "dispatcher parent-loss preauthorization evidence "
                        "mismatch")
            elif item["status"] == "interrupted_by_dispatcher":
                reconciliation = reconciliations.get(worker_id)
                if (exit_row.get("returncode") != 97
                        or exit_row.get("disposition") != "campaign_fatal"
                        or not isinstance(reconciliation, dict)
                        or reconciliation.get("sample") != item["sample"]
                        or reconciliation.get("pid") != item["worker_pid"]
                        or reconciliation.get("invocation_id")
                        != item["invocation_id"]
                        or reconciliation.get("reason")
                        != "dispatcher_process_lost"):
                    raise RuntimeError(
                        "dispatcher parent-loss interruption evidence mismatch")
            elif (exit_row.get("disposition") != item["status"]
                  or (
                      item["status"] == "finished"
                      and exit_row.get("returncode") != 0
                  )
                  or (
                      item["status"] != "finished"
                      and exit_row.get("returncode") == 0
                  )):
                raise RuntimeError(
                    "dispatcher parent-loss terminal evidence mismatch")

        def _validate_incident_entries(filename, entries):
            if not isinstance(entries, list):
                raise RuntimeError(
                    f"dispatcher parent-loss {filename} evidence is invalid")
            file_path = os.path.join(out_dir, filename)
            rows = (
                _read_jsonl_records_with_retry(file_path)
                if os.path.isfile(file_path) else []
            )
            validated = {}
            for entry in entries:
                number = (
                    entry.get("row_number")
                    if isinstance(entry, dict) else None
                )
                digest = (
                    entry.get("canonical_sha256")
                    if isinstance(entry, dict) else None
                )
                if (not isinstance(number, int) or isinstance(number, bool)
                        or not 1 <= number <= len(rows)
                        or number in validated
                        or not isinstance(digest, str)
                        or digest != _canonical_record_sha256(
                            rows[number - 1])):
                    raise RuntimeError(
                        f"dispatcher parent-loss {filename} evidence mismatch")
                validated[number] = (entry, rows[number - 1])
            return validated

        current_api = _validate_incident_entries(
            "api_calls.jsonl", record.get("incident_api_rows"))
        current_attempts = _validate_incident_entries(
            "api_attempt_ledger.jsonl",
            record.get("incident_attempt_rows"))
        current_pre_provider_api = _validate_incident_entries(
            "api_calls.jsonl",
            record.get("operator_pre_provider_api_rows", []))
        if (record.get("dispatcher_operator_pause_recovery") is True
                and (current_api or current_attempts)):
            raise RuntimeError(
                "operator pause recovery cannot authorize provider replay "
                "incidents"
            )
        if (record.get("dispatcher_operator_pause_recovery") is not True
                and current_pre_provider_api):
            raise RuntimeError(
                "non-operator recovery cannot contain operator pre-provider "
                "API evidence")
        prior_api_entries = {
            (
                item.get("row_number"),
                item.get("canonical_sha256"),
            ): item
            for item in (
                (superseded or {}).get("incident_api_rows") or [])
        }
        prior_attempt_entries = {
            (
                item.get("row_number"),
                item.get("canonical_sha256"),
            ): item
            for item in (
                (superseded or {}).get("incident_attempt_rows") or [])
        }

        def _validate_mapped_transport_sidecar(entry, row):
            attempts = row.get("transport_attempts")
            sidecar_relative = entry.get("transport_sidecar_path")
            sidecar_digest = entry.get("transport_sidecar_sha256")
            if (not isinstance(attempts, list) or not attempts
                    or not isinstance(sidecar_relative, str)
                    or not sidecar_relative
                    or not isinstance(sidecar_digest, str)
                    or not sidecar_digest):
                raise RuntimeError(
                    "dispatcher parent-loss transport sidecar evidence "
                    "is invalid")
            sidecar_path = os.path.realpath(os.path.join(
                out_dir, sidecar_relative))
            raw_path = os.path.realpath(str(
                row.get("raw_sse_saved_path") or ""))
            if (os.path.commonpath([out_dir, sidecar_path]) != out_dir
                    or sidecar_path != raw_path
                    or not os.path.isfile(sidecar_path)
                    or _sha256_file(sidecar_path) != sidecar_digest):
                raise RuntimeError(
                    "dispatcher parent-loss transport sidecar digest mismatch")
            sidecar = _read_deepseek_transport_sidecar(sidecar_path)
            is_compact_revision = (
                row.get("transport_revision")
                == DEEPSEEK_COMPACT_TRANSPORT_REVISION
            )
            if is_compact_revision and not sidecar["compact"]:
                raise RuntimeError(
                    "dispatcher parent-loss API compact sidecar was downgraded")
            if not is_compact_revision and sidecar["compact"]:
                raise RuntimeError(
                    "dispatcher parent-loss legacy API sidecar is compact")
            expected_linkage = {
                "call_id": row.get("request_id"),
                "worker_launch_id": row.get("worker_launch_id"),
                "worker_pid": row.get("worker_pid"),
                "sample": row.get("sample"),
                "method": row.get("method"),
                "rt_index": row.get("rt_index"),
                "direction": row.get("direction"),
            }
            if is_compact_revision:
                compact = _validate_deepseek_compact_records(
                    sidecar["rows"], expected_linkage=expected_linkage)
                if ([item["attempt"] for item in compact["closed_attempts"]]
                        != attempts):
                    raise RuntimeError(
                        "dispatcher parent-loss compact sidecar mismatch")
            else:
                _validate_deepseek_linear_records(
                    sidecar["rows"],
                    expected_linkage=expected_linkage,
                    expected_attempts=attempts,
                )

        new_api = []
        for entry, row in current_api.values():
            key = (entry.get("row_number"), entry.get("canonical_sha256"))
            if key in prior_api_entries:
                if prior_api_entries[key] != entry:
                    raise RuntimeError(
                        "dispatcher parent-loss prior API evidence drifted")
            else:
                new_api.append((entry, row))
        new_attempts = []
        for entry, row in current_attempts.values():
            key = (entry.get("row_number"), entry.get("canonical_sha256"))
            if key in prior_attempt_entries:
                if prior_attempt_entries[key] != entry:
                    raise RuntimeError(
                        "dispatcher parent-loss prior attempt evidence drifted")
            else:
                new_attempts.append((entry, row))
        if (set(prior_api_entries) - {
                (entry.get("row_number"), entry.get("canonical_sha256"))
                for entry, _row in current_api.values()
        } or set(prior_attempt_entries) - {
                (entry.get("row_number"), entry.get("canonical_sha256"))
                for entry, _row in current_attempts.values()
        }):
            raise RuntimeError(
                "dispatcher parent-loss superseded incident evidence is missing")
        committed_call_ids = set()
        manifest_methods = set((manifest.get("config") or {}).get(
            "method_set") or [])
        for sample in (manifest.get("config") or {}).get("samples") or []:
            for method in manifest_methods:
                result_path = os.path.join(
                    out_dir, method, f"{sample}.jsonl")
                if not os.path.isfile(result_path):
                    continue
                for result_row in _read_jsonl_records_with_retry(result_path):
                    committed_call_ids.update(
                        result_row.get("api_call_ids") or [])
        expected_new_api_numbers = set()
        all_api_rows = (
            _read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl"))
            if os.path.isfile(os.path.join(
                out_dir, "api_calls.jsonl")) else []
        )
        interrupted_worker_ids = {
            worker_id for worker_id, item in current_workers.items()
            if item["status"] == "interrupted_by_dispatcher"
        }
        for number, row in enumerate(all_api_rows, 1):
            if (row.get("worker_launch_id") in interrupted_worker_ids
                    and row.get("request_id") not in committed_call_ids):
                expected_new_api_numbers.add(number)
        if ({
                entry["row_number"] for entry, _row in new_api
        } | {
                entry["row_number"]
                for entry, _row in current_pre_provider_api.values()
        }) != expected_new_api_numbers:
            raise RuntimeError(
                "dispatcher parent-loss uncommitted API scope mismatch")
        for entry, row in current_pre_provider_api.values():
            worker = current_workers.get(row.get("worker_launch_id")) or {}
            rt_index = row.get("rt_index")
            method = row.get("method")
            call_kind = row.get("call_kind")
            allowed_call_kinds = (
                {"hybridpatch_primary", "hybridpatch_repair"}
                if method == "hybridpatch"
                else {"fullrewrite_primary"}
            )
            if (entry.get("incident_kind")
                    != "operator_pause_pre_provider_api"
                    or row.get("schema") != API_CALL_SCHEMA
                    or row.get("sample") != worker.get("sample")
                    or row.get("worker_pid") != worker.get("worker_pid")
                    or row.get("model") != "deepseek-v4-flash"
                    or method not in manifest_methods
                    or not isinstance(rt_index, int)
                    or isinstance(rt_index, bool)
                    or not 1 <= rt_index <= 10
                    or row.get("direction") not in {"forward", "backward"}
                    or call_kind not in allowed_call_kinds
                    or row.get("transport") != "openai_sdk_stream"
                    or row.get("transport_revision")
                    != manifest_config.get("transport_revision")
                    or row.get("provider_called") is not False
                    or row.get("response_replayed") is not False
                    or row.get("request_id") in committed_call_ids
                    or row.get("classification") != "runner_exception"
                    or row.get("error_type") != "CampaignStoppedError"
                    or row.get("runner_exception") != (
                        "CampaignStoppedError: campaign stop latch is set: "
                        "operator_directed_dispatcher_pause")
                    or row.get("provider_request_id") is not None
                    or row.get("http_status") is not None
                    or row.get("stream_complete") is not False
                    or row.get("transport_attempts") != []
                    or row.get("http_attempts_used") is not None
                    or row.get("raw_response_saved_path") is not None
                    or row.get("raw_sse_saved_path") is not None
                    or row.get("raw_content_length") != 0
                    or row.get("content_sha256") != _sha256_text("")
                    or row.get("transport_sidecar_sha256") is not None
                    or row.get("transport_sidecar_size_bytes") is not None
                    or row.get("transport_sidecar_record_count") is not None
                    or row.get("count_as_method_failure") is not True):
                raise RuntimeError(
                    "operator pause pre-provider API evidence is invalid")
        for entry, row in new_api:
            worker = current_workers.get(row.get("worker_launch_id")) or {}
            rt_index = row.get("rt_index")
            method = row.get("method")
            call_kind = row.get("call_kind")
            allowed_call_kinds = (
                {"hybridpatch_primary", "hybridpatch_repair"}
                if method == "hybridpatch"
                else {"fullrewrite_primary"}
            )
            attempts = row.get("transport_attempts")
            if (entry.get("incident_kind")
                    != "dispatcher_parent_loss_uncommitted_api"
                    or row.get("schema") != API_CALL_SCHEMA
                    or row.get("sample") != worker.get("sample")
                    or row.get("worker_pid") != worker.get("worker_pid")
                    or row.get("model") != "deepseek-v4-flash"
                    or method not in manifest_methods
                    or not isinstance(rt_index, int)
                    or isinstance(rt_index, bool)
                    or not 1 <= rt_index <= 10
                    or row.get("direction") not in {"forward", "backward"}
                    or call_kind not in allowed_call_kinds
                    or row.get("transport") != "openai_sdk_stream"
                    or row.get("transport_revision")
                    != manifest_config.get("transport_revision")
                    or row.get("provider_called") is not True
                    or row.get("response_replayed") is not False
                    or row.get("request_id") in committed_call_ids
                    or row.get("classification") is not None
                    or row.get("http_status") != 200
                    or row.get("stream_complete") is not True
                    or row.get("response_classification") != "normal"
                    or not isinstance(row.get("input_tokens"), int)
                    or isinstance(row.get("input_tokens"), bool)
                    or row.get("input_tokens") < 0
                    or not isinstance(row.get("output_tokens"), int)
                    or isinstance(row.get("output_tokens"), bool)
                    or row.get("output_tokens") < 0
                    or not isinstance(attempts, list) or not attempts
                    or row.get("http_attempts_used") != len(attempts)
                    or row.get("retry_count") != len(attempts) - 1
                    or attempts[-1].get("status") != "success"
                    or attempts[-1].get("http_status") != 200
                    or attempts[-1].get("stream_complete") is not True
                    or attempts[-1].get("message_start_seen") is not True
                    or attempts[-1].get("message_stop_seen") is not True
                    or attempts[-1].get("final_usage_seen") is not True
                    or attempts[-1].get("terminal_sequence_valid") is not True):
                raise RuntimeError(
                    "dispatcher parent-loss API incident is invalid")
            _validate_mapped_transport_sidecar(entry, row)
            budget_index = 0
            for expected_index, attempt in enumerate(attempts, 1):
                if attempt.get("attempt_index") != expected_index:
                    raise RuntimeError(
                        "dispatcher parent-loss retry sequence is invalid")
                if expected_index == len(attempts):
                    continue
                free_503 = (
                    attempt.get("http_status") == 503
                    and attempt.get("generation_delta_seen") is False
                )
                if (attempt.get("status") != "retryable_error"
                        or attempt.get("retry_budget_consumed")
                        is not (not free_503)):
                    raise RuntimeError(
                        "dispatcher parent-loss retry sequence is invalid")
                if not free_503:
                    budget_index += 1
                if attempt.get(
                        "retry_budget_attempt_index") != budget_index:
                    raise RuntimeError(
                        "dispatcher parent-loss retry sequence is invalid")
        all_attempt_rows = (
            _read_jsonl_records_with_retry(os.path.join(
                out_dir, "api_attempt_ledger.jsonl"))
            if os.path.isfile(os.path.join(
                out_dir, "api_attempt_ledger.jsonl")) else []
        )
        open_groups = {}
        for number, row in enumerate(all_attempt_rows, 1):
            worker_id = row.get("worker_launch_id")
            semantic_call_id = row.get("semantic_call_id")
            if (worker_id in interrupted_worker_ids
                    and isinstance(semantic_call_id, str)
                    and semantic_call_id):
                open_groups.setdefault(
                    (worker_id, semantic_call_id), []).append((number, row))
        expected_open_attempt_numbers = set()
        for (_worker_id, semantic_call_id), group in open_groups.items():
            events = [row.get("event") for _number, row in group]
            starts = events.count("attempt_start")
            ends = events.count("attempt_end")
            pre_provider = (
                starts == 0 and ends == 0
                and events.count("semantic_request") == 1
                and len(events) == 1
            )
            is_open = (
                "response_committed" not in events
                and (starts > ends or pre_provider)
            )
            if not is_open:
                continue
            journal_digest = hashlib.sha256(
                semantic_call_id.encode("utf-8")).hexdigest()[:24]
            if os.path.exists(os.path.join(
                    out_dir, "api_journal",
                    f"{journal_digest}.response.json")):
                raise RuntimeError(
                    "dispatcher parent-loss open attempt has a response journal")
            expected_open_attempt_numbers.update(
                number for number, _row in group)
        if {
                entry["row_number"] for entry, _row in new_attempts
        } != expected_open_attempt_numbers:
            raise RuntimeError(
                "dispatcher parent-loss open attempt scope mismatch")
        for entry, row in new_attempts:
            if (entry.get("incident_kind")
                    != "dispatcher_parent_loss_open_attempt"
                    or row.get("worker_launch_id")
                    not in interrupted_worker_ids
                    or row.get("event") == "response_committed"):
                raise RuntimeError(
                    "dispatcher parent-loss attempt incident is invalid")

        provider_worker_ids = {
            row.get("worker_launch_id")
            for row in all_api_rows + all_attempt_rows
        }
        no_provider_worker_ids = (
            current_preauthorization_ids | current_registered_ids
        )
        if no_provider_worker_ids & provider_worker_ids:
            raise RuntimeError(
                "dispatcher parent-loss preauthorization provider evidence "
                "is not empty")
        for worker_id in interrupted_worker_ids:
            if (worker_id not in authorizations
                    and worker_id in provider_worker_ids):
                raise RuntimeError(
                    "dispatcher parent-loss unauthorized worker has provider "
                    "evidence")

        sidecar_entries = record.get("incident_transport_sidecars")
        if not isinstance(sidecar_entries, list):
            raise RuntimeError(
                "dispatcher parent-loss transport incident evidence is invalid")
        if (record.get("dispatcher_operator_pause_recovery") is True
                and sidecar_entries):
            raise RuntimeError(
                "operator pause recovery cannot authorize transport replay "
                "incidents"
            )
        prior_sidecars = {
            (item.get("path"), item.get("sha256")): item
            for item in (
                (superseded or {}).get(
                    "incident_transport_sidecars") or [])
        }
        new_sidecars = {}
        current_sidecar_keys = set()
        for entry in sidecar_entries:
            if not isinstance(entry, dict):
                raise RuntimeError(
                    "dispatcher parent-loss transport incident evidence "
                    "is invalid")
            key = (entry.get("path"), entry.get("sha256"))
            if (not isinstance(key[0], str) or not key[0]
                    or not isinstance(key[1], str) or not key[1]
                    or key in current_sidecar_keys):
                raise RuntimeError(
                    "dispatcher parent-loss transport incident evidence "
                    "is invalid")
            current_sidecar_keys.add(key)
            prior_entry = prior_sidecars.get(key)
            if prior_entry is not None:
                if prior_entry != entry:
                    raise RuntimeError(
                        "dispatcher parent-loss prior transport evidence "
                        "drifted")
            sidecar_path = os.path.realpath(os.path.join(out_dir, key[0]))
            if (os.path.commonpath([out_dir, sidecar_path]) != out_dir
                    or not os.path.isfile(sidecar_path)
                    or _sha256_file(sidecar_path) != key[1]):
                raise RuntimeError(
                    "dispatcher parent-loss transport incident digest mismatch")
            sidecar = _read_deepseek_transport_sidecar(sidecar_path)
            rows = sidecar["rows"]
            worker_id = entry.get("worker_launch_id")
            worker = (
                current_workers.get(worker_id) or {}
                if prior_entry is None else None
            )
            starts = sidecar["starts"]
            ends = sidecar["ends"]
            expected_linkage = {
                "worker_launch_id": worker_id,
                "worker_pid": (
                    worker.get("worker_pid")
                    if worker is not None else entry.get("worker_pid")
                ),
                "sample": (
                    worker.get("sample")
                    if worker is not None else entry.get("sample")
                ),
                "method": entry.get("method"),
                "rt_index": entry.get("rt_index"),
                "direction": entry.get("direction"),
                "call_id": entry.get("call_id"),
            }
            compact = None
            is_compact_revision = (
                manifest_config.get("transport_revision")
                == DEEPSEEK_COMPACT_TRANSPORT_REVISION
            )
            if is_compact_revision:
                if not sidecar["compact"]:
                    raise RuntimeError(
                        "dispatcher parent-loss compact transport schema "
                        "was downgraded")
                try:
                    compact = _validate_deepseek_compact_records(
                        rows,
                        expected_linkage=expected_linkage,
                        allow_open_final=True,
                    )
                except RuntimeError as exc:
                    raise RuntimeError(
                        "dispatcher parent-loss compact transport incident "
                        "is invalid") from exc
                attempt_start_count = compact["attempt_start_count"]
                attempt_end_count = compact["attempt_end_count"]
                expected_state = compact["recovery_state"]
                linkage_valid = True
            else:
                if sidecar["compact"]:
                    raise RuntimeError(
                        "dispatcher parent-loss legacy transport schema "
                        "was upgraded")
                try:
                    linear = _validate_deepseek_linear_records(
                        rows,
                        expected_linkage=expected_linkage,
                        allow_open_final=True,
                    )
                except RuntimeError as exc:
                    raise RuntimeError(
                        "dispatcher parent-loss linear transport incident "
                        "is invalid") from exc
                linkage_valid = True
                attempt_start_count = linear["attempt_start_count"]
                attempt_end_count = linear["attempt_end_count"]
                expected_state = linear["recovery_state"]
            if (not rows
                    or not linkage_valid
                    or not isinstance(worker_id, str) or not worker_id
                    or not isinstance(entry.get("worker_pid"), int)
                    or isinstance(entry.get("worker_pid"), bool)
                    or entry.get("worker_pid") <= 0
                    or not isinstance(entry.get("sample"), str)
                    or not entry.get("sample")
                    or entry.get("method") not in manifest_methods
                    or not isinstance(entry.get("rt_index"), int)
                    or isinstance(entry.get("rt_index"), bool)
                    or not 1 <= entry.get("rt_index") <= 10
                    or entry.get("direction") not in {
                        "forward", "backward"}
                    or entry.get("event_count") != len(rows)
                    or entry.get("attempt_start_count")
                    != attempt_start_count
                    or entry.get("attempt_end_count") != attempt_end_count
                    or (
                        not is_compact_revision
                        and attempt_start_count == 0
                    )
                    or (
                        prior_entry is None
                        and (
                            worker_id not in interrupted_worker_ids
                            or entry.get("sample") != worker.get("sample")
                            or entry.get("worker_pid")
                            != worker.get("worker_pid")
                        )
                    )):
                raise RuntimeError(
                    "dispatcher parent-loss transport incident scope mismatch")
            if entry.get("state") != expected_state:
                raise RuntimeError(
                    "dispatcher parent-loss transport incident state mismatch")
            if prior_entry is None:
                new_sidecars[sidecar_path] = entry
        if set(prior_sidecars) - current_sidecar_keys:
            raise RuntimeError(
                "dispatcher parent-loss superseded transport evidence "
                "is missing")

        mapped_sidecars = {
            os.path.realpath(row.get("raw_sse_saved_path"))
            for row in all_api_rows
            if isinstance(row.get("raw_sse_saved_path"), str)
            and row.get("raw_sse_saved_path")
        }
        expected_new_sidecars = set()
        raw_root = os.path.join(out_dir, "api_raw")
        if os.path.isdir(raw_root):
            for root, _dirs, files in os.walk(raw_root):
                for name in files:
                    if not name.endswith(".transport.jsonl"):
                        continue
                    sidecar_path = os.path.realpath(
                        os.path.join(root, name))
                    if sidecar_path in mapped_sidecars:
                        continue
                    rows = _read_jsonl_records_with_retry(sidecar_path)
                    if rows and {
                            row.get("worker_launch_id") for row in rows
                    } & interrupted_worker_ids:
                        expected_new_sidecars.add(sidecar_path)
        if set(new_sidecars) != expected_new_sidecars:
            raise RuntimeError(
                "dispatcher parent-loss transport incident set mismatch")

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
        authorization_path=authorization_path,
        authorization_sha256=_sha256_file(authorization_path),
        recovery_identity_history=identity_history,
    )


@functools.lru_cache(maxsize=16)
def campaign_recovery_incident_evidence(out_dir):
    """Return exact row hashes and workers covered by a V2 lock recovery."""
    authorization = read_campaign_recovery_authorization(out_dir)
    if (not authorization
            or authorization.get("recovery_kind") not in {
                LEDGER_LOCK_RECOVERY_KIND,
                DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
            }):
        return {
            "api_row_hashes": frozenset(),
            "api_incident_kinds": {},
            "attempt_row_hashes": frozenset(),
            "transport_sidecar_hashes": frozenset(),
            "transport_sidecars_by_api_row_hash": {},
            "worker_launch_ids": frozenset(),
            "deepseek_recovered_workers": {},
            "preauthorization_worker_launch_ids": frozenset(),
            "provider_access_retry_authorizations": {},
            "provider_access_resume_samples": frozenset(),
            "dispatcher_parent_loss_workers": {},
            "authorization_id": None,
        }
    provider_access_retries = {
        item["sample"]: dict(item)
        for item in authorization.get(
            "provider_access_retry_authorizations", [])
    }
    deepseek_recovered_workers = (
        authorization.get("deepseek_recovered_workers", [])
        if (
            authorization.get("deepseek_server_retry_recovery") is True
            or authorization.get(
                "deepseek_transport_disconnect_retry_recovery") is True
        )
        else []
    )
    return {
        "api_row_hashes": frozenset({
            item["canonical_sha256"]
            for item in authorization["incident_api_rows"]
        }),
        "api_incident_kinds": {
            item["canonical_sha256"]: item.get("incident_kind")
            for item in authorization["incident_api_rows"]
        },
        "attempt_row_hashes": frozenset({
            item["canonical_sha256"]
            for item in authorization["incident_attempt_rows"]
        }),
        "transport_sidecar_hashes": frozenset({
            item["sha256"]
            for item in authorization.get(
                "incident_transport_sidecars", [])
        }),
        "transport_sidecars_by_api_row_hash": {
            item["api_row_sha256"]: dict(item)
            for item in authorization.get(
                "incident_transport_sidecars", [])
            if isinstance(item.get("api_row_sha256"), str)
        },
        "worker_launch_ids": frozenset(
            authorization["recovered_worker_launch_ids"]),
        "deepseek_recovered_workers": {
            item["sample"]: dict(item)
            for item in deepseek_recovered_workers
            if isinstance(item, dict)
            and isinstance(item.get("sample"), str)
            and item.get("sample")
        },
        "preauthorization_worker_launch_ids": frozenset(
            authorization.get("preauthorization_worker_launch_ids", [])),
        "provider_access_retry_authorizations": provider_access_retries,
        "provider_access_resume_samples": frozenset(
            authorization.get("provider_access_resume_samples") or []),
        "dispatcher_parent_loss_workers": {
            item["sample"]: dict(item)
            for item in authorization.get(
                "dispatcher_parent_loss_resume_workers")
            or authorization.get(
                "dispatcher_parent_loss_workers", [])
        },
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


def _run_metadata_events_path(out_dir):
    return os.path.join(out_dir, METADATA_EVENTS_FILENAME)


def _run_metadata_event_pending_path(out_dir):
    return os.path.join(out_dir, METADATA_EVENT_PENDING_FILENAME)


def _decode_run_metadata_event_bytes(payload, *, label):
    if not payload or not payload.endswith(b"\n"):
        raise RuntimeError(
            f"{label} has an uncommitted tail (missing newline commit marker)")
    try:
        rows = [
            json.loads(line)
            for line in payload.decode("utf-8").splitlines()
            if line.strip()
        ]
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError(f"{label} is invalid") from exc
    if not rows or any(not isinstance(row, dict) for row in rows):
        raise RuntimeError(f"{label} is invalid")
    return rows


def _read_run_metadata_events_strict(events_path):
    """Read only newline-committed event records."""
    try:
        with open(events_path, "rb") as handle:
            payload = handle.read()
    except OSError as exc:
        raise RuntimeError("run metadata event ledger is empty") from exc
    return _decode_run_metadata_event_bytes(
        payload, label="run metadata event ledger")


def _run_metadata_projection_receipt_path(out_dir):
    return os.path.join(out_dir, METADATA_PROJECTION_RECEIPT_FILENAME)


def _run_metadata_storage_mode_unlocked(out_dir, metadata_path):
    """Classify the three-file metadata store without allowing downgrades."""
    events_exists = os.path.isfile(_run_metadata_events_path(out_dir))
    pending_exists = os.path.isfile(_run_metadata_event_pending_path(out_dir))
    snapshot_exists = os.path.exists(metadata_path)
    receipt_exists = os.path.exists(
        _run_metadata_projection_receipt_path(out_dir))
    recovery_dir = os.path.join(out_dir, METADATA_EVENT_RECOVERY_DIRECTORY)
    recovery_dir_exists = os.path.exists(recovery_dir)
    registry_exists = os.path.isfile(
        _run_metadata_event_recovery_registry_path(out_dir))
    declared_storage = None
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path, encoding="utf-8") as handle:
                manifest = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError("dispatch manifest is invalid") from exc
        config = manifest.get("config") if isinstance(manifest, dict) else None
        declared_storage = (
            config.get("run_metadata_storage")
            if isinstance(config, dict) else None
        )
        if declared_storage not in {None, RUN_METADATA_STORAGE_EVENT_V1}:
            raise RuntimeError("dispatch manifest run metadata storage is invalid")
    if events_exists or pending_exists:
        return "event"
    if recovery_dir_exists:
        if not registry_exists:
            fresh_empty_directory = (
                os.path.isdir(recovery_dir)
                and not os.listdir(recovery_dir)
                and not snapshot_exists
                and not receipt_exists
            )
            if not fresh_empty_directory:
                raise RuntimeError(
                    "run metadata event recovery registry is missing")
            return "event"
        registry = _read_run_metadata_event_recovery_registry(out_dir)
        if registry["receipts"] or snapshot_exists or receipt_exists:
            raise RuntimeError(
                "run metadata recovery registry requires a missing event ledger")
        return "event"
    if receipt_exists:
        raise RuntimeError(
            "run metadata projection receipt exists without event ledger")
    if declared_storage == RUN_METADATA_STORAGE_EVENT_V1 and snapshot_exists:
        raise RuntimeError(
            "dispatch manifest requires a missing run metadata event ledger")
    if snapshot_exists:
        return "legacy"
    return "event"


def _run_metadata_projection_sha256(records):
    payload = json.dumps(
        records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _metadata_timestamp_is_aware(value):
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _fold_run_metadata_events(events):
    """Strictly reduce one canonical append-only metadata event sequence."""
    if not isinstance(events, list):
        raise RuntimeError("run metadata events are invalid")
    records = []
    by_invocation = {}
    task_plans = {}
    for expected_index, event in enumerate(events, 1):
        if (not isinstance(event, dict)
                or event.get("schema") != METADATA_EVENT_SCHEMA
                or event.get("event_index") != expected_index
                or not isinstance(event.get("event_index"), int)
                or isinstance(event.get("event_index"), bool)):
            raise RuntimeError(
                f"run metadata event {expected_index} has invalid identity")
        event_kind = event.get("event")
        if event_kind == "invocation_registered":
            if set(event) != {"schema", "event", "event_index", "record"}:
                raise RuntimeError(
                    "run metadata invocation registration schema is invalid")
            source = event.get("record")
            required = {
                "schema", "invocation_id", "status", "created_local",
                "invocation_started_at", "invocation_finished_at",
                "started_at", "finished_at", "timezone",
                "run_git_commit", "git_tree_state", "command", "out_dir",
                "samples", "methods", "num_round_trips", "seed", "model",
                "distractor", "max_tokens", "reasoning_effort",
                "code_fingerprint", "campaign_config", "worker_pid",
                "campaign_recovery_authorization",
            }
            invocation_id = (
                source.get("invocation_id") if isinstance(source, dict) else None
            )
            campaign_config = (
                source.get("campaign_config")
                if isinstance(source, dict) else None
            )
            samples = source.get("samples") if isinstance(source, dict) else None
            methods = source.get("methods") if isinstance(source, dict) else None
            num_round_trips = (
                source.get("num_round_trips")
                if isinstance(source, dict) else None
            )
            worker_pid = (
                source.get("worker_pid") if isinstance(source, dict) else None
            )
            if (not isinstance(source, dict)
                    or not required <= set(source)
                    or "task_plans" in source
                    or source.get("schema") != METADATA_SCHEMA
                    or not isinstance(invocation_id, str) or not invocation_id
                    or invocation_id in by_invocation
                    or source.get("status") != "running"
                    or source.get("invocation_finished_at") is not None
                    or source.get("finished_at") is not None
                    or not _metadata_timestamp_is_aware(
                        source.get("created_local"))
                    or not _metadata_timestamp_is_aware(
                        source.get("invocation_started_at"))
                    or not _metadata_timestamp_is_aware(source.get("started_at"))
                    or not isinstance(source.get("timezone"), str)
                    or not source.get("timezone")
                    or not isinstance(source.get("run_git_commit"), str)
                    or not re.fullmatch(
                        r"[0-9a-f]{40}", source.get("run_git_commit", ""))
                    or source.get("git_tree_state") not in {"clean", "dirty"}
                    or not isinstance(source.get("command"), str)
                    or not source.get("command")
                    or not isinstance(source.get("out_dir"), str)
                    or not source.get("out_dir")
                    or not isinstance(samples, list) or not samples
                    or any(not isinstance(item, str) or not item
                           for item in samples)
                    or not isinstance(methods, list) or not methods
                    or any(not isinstance(item, str) or not item
                           for item in methods)
                    or not isinstance(num_round_trips, int)
                    or isinstance(num_round_trips, bool)
                    or num_round_trips <= 0
                    or not isinstance(source.get("seed"), int)
                    or isinstance(source.get("seed"), bool)
                    or not isinstance(source.get("model"), str)
                    or not source.get("model")
                    or not isinstance(source.get("distractor"), bool)
                    or not isinstance(source.get("max_tokens"), int)
                    or isinstance(source.get("max_tokens"), bool)
                    or source.get("max_tokens") <= 0
                    or not isinstance(source.get("code_fingerprint"), dict)
                    or not source.get("code_fingerprint")
                    or not isinstance(campaign_config, dict)
                    or campaign_config.get("num_round_trips")
                    != num_round_trips
                    or campaign_config.get("seed") != source.get("seed")
                    or campaign_config.get("model") != source.get("model")
                    or campaign_config.get("distractor")
                    != source.get("distractor")
                    or campaign_config.get("max_tokens")
                    != source.get("max_tokens")
                    or campaign_config.get("reasoning_effort")
                    != source.get("reasoning_effort")
                    or not isinstance(campaign_config.get("method_set"), list)
                    or not set(methods) <= set(campaign_config["method_set"])
                    or campaign_config.get(
                        "snapshot_mode", SNAPSHOT_MODE_ALL)
                    not in SNAPSHOT_MODES
                    or not isinstance(worker_pid, int)
                    or isinstance(worker_pid, bool) or worker_pid <= 0):
                raise RuntimeError(
                    "run metadata invocation registration is invalid")
            if records:
                first = records[0]
                invariant_fields = {
                    "started_at", "timezone", "out_dir", "transport",
                    "transport_revision", "transport_resume_policy",
                }
                if any(source.get(field) != first.get(field)
                       for field in invariant_fields) or not (
                           _campaign_configs_match(
                               first.get("campaign_config"),
                               source.get("campaign_config"))):
                    raise RuntimeError(
                        "run metadata campaign registration identity drifted")
                previous = records[-1]
                previous_identity = (
                    previous.get("run_git_commit"),
                    previous.get("git_tree_state"),
                    previous.get("code_fingerprint"),
                )
                current_identity = (
                    source.get("run_git_commit"),
                    source.get("git_tree_state"),
                    source.get("code_fingerprint"),
                )
                if current_identity != previous_identity:
                    boundary = source.get("campaign_recovery_authorization")
                    if (not isinstance(boundary, dict)
                            or not isinstance(
                                boundary.get("authorization_id"), str)
                            or not boundary.get("authorization_id")
                            or not isinstance(
                                boundary.get("authorization_sha256"), str)
                            or not re.fullmatch(
                                r"[0-9a-f]{64}",
                                boundary.get("authorization_sha256", ""))
                            or boundary.get("prior_git_commit") not in {
                                record.get("run_git_commit")
                                for record in records
                            }
                            or boundary.get("recovery_git_commit")
                            != source.get("run_git_commit")
                            or previous.get("git_tree_state") != "clean"
                            or source.get("git_tree_state") != "clean"):
                        raise RuntimeError(
                            "run metadata recovery identity transition is invalid")
            for record in records:
                record["finished_at"] = None
            record = dict(source)
            record["task_plans"] = dict(task_plans)
            records.append(record)
            by_invocation[invocation_id] = record
            continue

        if event_kind == "task_plan_registered":
            if set(event) != {
                    "schema", "event", "event_index", "sample_id",
                    "task_plan"}:
                raise RuntimeError(
                    "run metadata task-plan event schema is invalid")
            sample_id = event.get("sample_id")
            task_plan = event.get("task_plan")
            round_trips = (
                task_plan.get("round_trips")
                if isinstance(task_plan, dict) else None
            )
            registered_samples = {
                item
                for record in records
                for item in (record.get("samples") or [])
            }
            active_samples = {
                item
                for record in records
                if record.get("status") == "running"
                for item in (record.get("samples") or [])
            }
            expected_round_trips = records[0].get("num_round_trips") if records else None
            if (not records
                    or not isinstance(sample_id, str) or not sample_id
                    or sample_id not in registered_samples
                    or sample_id not in active_samples
                    or sample_id in task_plans
                    or not isinstance(task_plan, dict)
                    or set(task_plan) != {"sha256", "round_trips"}
                    or not isinstance(task_plan.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", task_plan["sha256"])
                    or not isinstance(round_trips, int)
                    or isinstance(round_trips, bool) or round_trips <= 0
                    or round_trips != expected_round_trips):
                raise RuntimeError("run metadata task-plan event is invalid")
            task_plans[sample_id] = dict(task_plan)
            for record in records:
                record["task_plans"] = dict(task_plans)
            continue

        if event_kind == "invocation_terminal":
            if set(event) != {
                    "schema", "event", "event_index", "invocation_id",
                    "status", "finished_at"}:
                raise RuntimeError(
                    "run metadata terminal event schema is invalid")
            invocation_id = event.get("invocation_id")
            status = event.get("status")
            finished_at = event.get("finished_at")
            target = by_invocation.get(invocation_id)
            if (target is None or target.get("status") != "running"
                    or status not in RUN_METADATA_TERMINAL_STATUSES
                    or not _metadata_timestamp_is_aware(finished_at)
                    or datetime.fromisoformat(finished_at)
                    < datetime.fromisoformat(
                        target["invocation_started_at"])):
                raise RuntimeError("run metadata terminal event is invalid")
            target["status"] = status
            target["invocation_finished_at"] = finished_at
            campaign_finished_at = (
                None
                if any(record.get("status") == "running" for record in records)
                else max(
                    (record["invocation_finished_at"] for record in records),
                    key=datetime.fromisoformat,
                )
            )
            for record in records:
                record["finished_at"] = campaign_finished_at
            continue

        if event_kind == "invocations_interrupted":
            if set(event) != {
                    "schema", "event", "event_index", "finished_at",
                    "invocations"}:
                raise RuntimeError(
                    "run metadata interruption event schema is invalid")
            finished_at = event.get("finished_at")
            transitions = event.get("invocations")
            if (not _metadata_timestamp_is_aware(finished_at)
                    or not isinstance(transitions, list) or not transitions):
                raise RuntimeError("run metadata interruption event is invalid")
            seen = set()
            resolved = []
            for transition in transitions:
                if (not isinstance(transition, dict)
                        or set(transition) != {
                            "invocation_id", "worker_launch_id", "worker_pid",
                            "samples", "status"}
                        or not isinstance(
                            transition.get("invocation_id"), str)
                        or not transition.get("invocation_id")
                        or transition["invocation_id"] in seen
                        or transition.get("status")
                        not in RUN_METADATA_TERMINAL_STATUSES):
                    raise RuntimeError(
                        "run metadata interruption transition is invalid")
                target = by_invocation.get(transition["invocation_id"])
                if (target is None or target.get("status") != "running"
                        or target.get("worker_launch_id")
                        != transition.get("worker_launch_id")
                        or target.get("worker_pid")
                        != transition.get("worker_pid")
                        or list(target.get("samples") or [])
                        != transition.get("samples")
                        or datetime.fromisoformat(finished_at)
                        < datetime.fromisoformat(
                            target["invocation_started_at"])):
                    raise RuntimeError(
                        "run metadata interruption identity is invalid")
                seen.add(transition["invocation_id"])
                resolved.append((target, transition["status"]))
            for target, status in resolved:
                target["status"] = status
                target["invocation_finished_at"] = finished_at
            campaign_finished_at = (
                None
                if any(record.get("status") == "running" for record in records)
                else max(
                    (record["invocation_finished_at"] for record in records),
                    key=datetime.fromisoformat,
                )
            )
            for record in records:
                record["finished_at"] = campaign_finished_at
            continue

        raise RuntimeError(
            f"run metadata event {expected_index} has unknown type {event_kind!r}")
    return [dict(record) for record in records]


def _append_run_metadata_event_bytes(events_path, event_bytes):
    with open(events_path, "ab") as handle:
        handle.write(event_bytes)
        handle.flush()
        os.fsync(handle.fileno())


def _clear_run_metadata_event_pending(out_dir):
    path = _run_metadata_event_pending_path(out_dir)
    if os.path.exists(path):
        os.unlink(path)


def _read_run_metadata_event_pending(out_dir):
    path = _run_metadata_event_pending_path(out_dir)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            pending = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("run metadata event pending intent is invalid") from exc
    required = {
        "schema", "events_file", "event_schema", "prior_prefix_size_bytes",
        "prior_prefix_sha256", "prior_event_count", "event_index",
        "event_line", "event_sha256",
    }
    event_line = pending.get("event_line") if isinstance(pending, dict) else None
    event_bytes = (
        event_line.encode("utf-8") if isinstance(event_line, str) else None
    )
    recovery_observation = (
        pending.get("recovery_observation")
        if isinstance(pending, dict) else None
    )
    allowed_keys = required | (
        {"recovery_observation"}
        if recovery_observation is not None else set()
    )
    if (not isinstance(pending, dict) or set(pending) != allowed_keys
            or pending.get("schema") != METADATA_EVENT_PENDING_SCHEMA
            or pending.get("events_file") != METADATA_EVENTS_FILENAME
            or pending.get("event_schema") != METADATA_EVENT_SCHEMA
            or not isinstance(pending.get("prior_prefix_size_bytes"), int)
            or isinstance(pending.get("prior_prefix_size_bytes"), bool)
            or pending["prior_prefix_size_bytes"] < 0
            or not isinstance(pending.get("prior_event_count"), int)
            or isinstance(pending.get("prior_event_count"), bool)
            or pending["prior_event_count"] < 0
            or not isinstance(pending.get("event_index"), int)
            or isinstance(pending.get("event_index"), bool)
            or pending["event_index"] != pending["prior_event_count"] + 1
            or not isinstance(pending.get("prior_prefix_sha256"), str)
            or not re.fullmatch(
                r"[0-9a-f]{64}", pending["prior_prefix_sha256"])
            or event_bytes is None or not event_bytes.endswith(b"\n")
            or not isinstance(pending.get("event_sha256"), str)
            or hashlib.sha256(event_bytes).hexdigest()
            != pending.get("event_sha256")):
        raise RuntimeError("run metadata event pending intent is invalid")
    event_rows = _decode_run_metadata_event_bytes(
        event_bytes, label="run metadata pending event")
    if len(event_rows) != 1 or event_rows[0].get("event_index") != pending["event_index"]:
        raise RuntimeError("run metadata event pending intent is invalid")
    if recovery_observation is not None:
        observed_size = (
            recovery_observation.get("observed_suffix_size_bytes")
            if isinstance(recovery_observation, dict) else None
        )
        if (not isinstance(recovery_observation, dict)
                or set(recovery_observation) != {
                    "prepared_at", "observed_suffix_size_bytes",
                    "observed_suffix_sha256",
                }
                or not _metadata_timestamp_is_aware(
                    recovery_observation.get("prepared_at"))
                or not isinstance(observed_size, int)
                or isinstance(observed_size, bool)
                or observed_size < 0 or observed_size > len(event_bytes)
                or recovery_observation.get("observed_suffix_sha256")
                != hashlib.sha256(event_bytes[:observed_size]).hexdigest()):
            raise RuntimeError("run metadata event pending intent is invalid")
    return pending, event_bytes, event_rows[0]


def _run_metadata_event_recovery_registry_path(out_dir):
    return os.path.join(
        out_dir, METADATA_EVENT_RECOVERY_DIRECTORY,
        METADATA_EVENT_RECOVERY_REGISTRY_FILENAME)


def _read_run_metadata_event_recovery_registry(out_dir, *, required=True):
    path = _run_metadata_event_recovery_registry_path(out_dir)
    if not os.path.isfile(path):
        if required:
            raise RuntimeError(
                "run metadata event recovery registry is missing")
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            registry = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "run metadata event recovery registry is invalid") from exc
    receipts = registry.get("receipts") if isinstance(registry, dict) else None
    required_keys = {
        "schema", "events_file", "event_schema", "receipt_schema", "receipts",
    }
    if (not isinstance(registry, dict) or set(registry) != required_keys
            or registry.get("schema")
            != METADATA_EVENT_RECOVERY_REGISTRY_SCHEMA
            or registry.get("events_file") != METADATA_EVENTS_FILENAME
            or registry.get("event_schema") != METADATA_EVENT_SCHEMA
            or registry.get("receipt_schema") != METADATA_EVENT_RECOVERY_SCHEMA
            or not isinstance(receipts, list)):
        raise RuntimeError("run metadata event recovery registry is invalid")
    validated = []
    prior_index = 0
    for descriptor in receipts:
        event_index = (
            descriptor.get("event_index")
            if isinstance(descriptor, dict) else None)
        filename = (
            descriptor.get("filename")
            if isinstance(descriptor, dict) else None)
        sha256 = (
            descriptor.get("sha256")
            if isinstance(descriptor, dict) else None)
        if (not isinstance(descriptor, dict)
                or set(descriptor) != {"event_index", "filename", "sha256"}
                or not isinstance(event_index, int)
                or isinstance(event_index, bool) or event_index <= prior_index
                or not isinstance(filename, str)
                or not re.fullmatch(r"[0-9]{8}_[0-9a-f]{16}\.json", filename)
                or int(filename[:8]) != event_index
                or not isinstance(sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", sha256)):
            raise RuntimeError(
                "run metadata event recovery registry is invalid")
        validated.append(dict(descriptor))
        prior_index = event_index
    return {**registry, "receipts": validated}


def _empty_run_metadata_event_recovery_registry():
    return {
        "schema": METADATA_EVENT_RECOVERY_REGISTRY_SCHEMA,
        "events_file": METADATA_EVENTS_FILENAME,
        "event_schema": METADATA_EVENT_SCHEMA,
        "receipt_schema": METADATA_EVENT_RECOVERY_SCHEMA,
        "receipts": [],
    }


def _ensure_run_metadata_event_recovery_registry_unlocked(out_dir):
    recovery_dir = os.path.join(out_dir, METADATA_EVENT_RECOVERY_DIRECTORY)
    created = False
    try:
        os.mkdir(recovery_dir)
        created = True
    except FileExistsError:
        if not os.path.isdir(recovery_dir):
            raise RuntimeError(
                "run metadata event recovery directory is invalid")
    registry = _read_run_metadata_event_recovery_registry(
        out_dir, required=False)
    if registry is not None:
        return registry
    fresh_unfinished_store = (
        not os.path.exists(_run_metadata_events_path(out_dir))
        and not os.path.exists(_run_metadata_event_pending_path(out_dir))
        and not os.path.exists(os.path.join(out_dir, "run_metadata.jsonl"))
        and not os.path.exists(_run_metadata_projection_receipt_path(out_dir))
        and not os.listdir(recovery_dir)
    )
    if not created and not fresh_unfinished_store:
        raise RuntimeError(
            "run metadata event recovery registry is missing")
    receipt_names = sorted(
        name for name in os.listdir(recovery_dir)
        if name.endswith(".json")
        and name != METADATA_EVENT_RECOVERY_REGISTRY_FILENAME
    )
    if receipt_names:
        # A projection receipt only binds the recovery files that happened to
        # be present when that cache was published.  Older code could publish
        # count=0 after a completed recovery receipt was lost, so it cannot
        # prove that an unregistered set is complete.  Only the still-present
        # pending WAL may initialize an empty registry; any receipt without a
        # registry requires explicit audited recovery.
        raise RuntimeError(
            "run metadata event recovery registry is missing")
    registry = _empty_run_metadata_event_recovery_registry()
    write_json_atomic(
        _run_metadata_event_recovery_registry_path(out_dir), registry)
    return registry


def _register_run_metadata_event_recovery_receipt_unlocked(
        out_dir, descriptor):
    registry = _read_run_metadata_event_recovery_registry(out_dir)
    receipts = list(registry["receipts"])
    event_index = descriptor.get("event_index")
    matches = [
        item for item in receipts if item["event_index"] == event_index
    ]
    if matches:
        if len(matches) == 1 and matches[0] == descriptor:
            return False
        raise RuntimeError(
            "run metadata event recovery registry conflicts")
    if receipts and event_index <= receipts[-1]["event_index"]:
        raise RuntimeError(
            "run metadata event recovery registry order is invalid")
    updated = dict(registry)
    updated["receipts"] = receipts + [dict(descriptor)]
    write_json_atomic(
        _run_metadata_event_recovery_registry_path(out_dir), updated)
    return True


def _reconcile_run_metadata_event_pending_unlocked(out_dir):
    loaded = _read_run_metadata_event_pending(out_dir)
    if loaded is None:
        return False
    pending, event_bytes, event = loaded
    events_path = _run_metadata_events_path(out_dir)
    try:
        with open(events_path, "rb") as handle:
            current = handle.read()
    except FileNotFoundError:
        current = b""
    prior_size = pending["prior_prefix_size_bytes"]
    if len(current) < prior_size:
        raise RuntimeError("run metadata event ledger was truncated before pending intent")
    prior = current[:prior_size]
    if (hashlib.sha256(prior).hexdigest()
            != pending["prior_prefix_sha256"]):
        raise RuntimeError("run metadata event pending prefix digest mismatch")
    prior_events = (
        _decode_run_metadata_event_bytes(
            prior, label="run metadata pending prefix")
        if prior else []
    )
    if len(prior_events) != pending["prior_event_count"]:
        raise RuntimeError("run metadata event pending prefix count mismatch")
    _fold_run_metadata_events(prior_events + [event])
    suffix = current[prior_size:]
    if (len(suffix) > len(event_bytes)
            or not event_bytes.startswith(suffix)):
        raise RuntimeError("run metadata event pending suffix diverged")
    observation = pending.get("recovery_observation")
    if observation is None:
        observation = {
            "prepared_at": _iso_with_timezone(_aware_now()),
            "observed_suffix_size_bytes": len(suffix),
            "observed_suffix_sha256": hashlib.sha256(suffix).hexdigest(),
        }
        pending = dict(pending)
        pending["recovery_observation"] = observation
        # The pending intent is the authoritative owner of the immutable crash
        # cut.  Persist it before the separate human-audit receipt or any ledger
        # completion so deleting/recreating that receipt cannot move the cut.
        write_json_atomic(_run_metadata_event_pending_path(out_dir), pending)
    observed_size = observation["observed_suffix_size_bytes"]
    if (len(suffix) < observed_size
            or event_bytes[:observed_size] != suffix[:observed_size]):
        raise RuntimeError(
            "run metadata event ledger regressed after recovery preparation")

    _ensure_run_metadata_event_recovery_registry_unlocked(out_dir)
    recovery_dir = os.path.join(out_dir, METADATA_EVENT_RECOVERY_DIRECTORY)
    receipt_path = os.path.join(
        recovery_dir,
        f"{pending['event_index']:08d}_{pending['event_sha256'][:16]}.json",
    )
    final_bytes = prior + event_bytes
    immutable_receipt = {
        "schema": METADATA_EVENT_RECOVERY_SCHEMA,
        "pending_sha256": _canonical_record_sha256(pending),
        "prior_prefix_size_bytes": prior_size,
        "prior_prefix_sha256": pending["prior_prefix_sha256"],
        "prior_event_count": pending["prior_event_count"],
        "event_index": pending["event_index"],
        "event_sha256": pending["event_sha256"],
        "final_event_count": pending["event_index"],
        "final_ledger_size_bytes": len(final_bytes),
        "final_ledger_sha256": hashlib.sha256(final_bytes).hexdigest(),
    }
    receipt = None
    if os.path.isfile(receipt_path):
        try:
            with open(receipt_path, encoding="utf-8") as handle:
                receipt = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "run metadata event recovery receipt is invalid") from exc
        required = set(immutable_receipt) | {
            "state", "prepared_at", "completed_at",
            "observed_suffix_size_bytes", "observed_suffix_sha256",
        }
        receipt_observed_size = (
            receipt.get("observed_suffix_size_bytes")
            if isinstance(receipt, dict) else None
        )
        if (not isinstance(receipt, dict) or set(receipt) != required
                or any(receipt.get(key) != value
                       for key, value in immutable_receipt.items())
                or receipt.get("state") not in {"prepared", "completed"}
                or receipt.get("prepared_at") != observation["prepared_at"]
                or (receipt.get("state") == "prepared"
                    and receipt.get("completed_at") is not None)
                or (receipt.get("state") == "completed"
                    and receipt.get("completed_at") != receipt.get("prepared_at"))
                or not isinstance(receipt_observed_size, int)
                or isinstance(receipt_observed_size, bool)
                or receipt_observed_size != observed_size
                or receipt.get("observed_suffix_sha256")
                != observation["observed_suffix_sha256"]):
            raise RuntimeError(
                "run metadata event recovery receipt is invalid")
        if (receipt.get("state") == "completed"
                and len(suffix) != len(event_bytes)):
            raise RuntimeError(
                "run metadata event ledger regressed after completed recovery")
    else:
        receipt = {
            **immutable_receipt,
            "state": "prepared",
            "prepared_at": observation["prepared_at"],
            "completed_at": None,
            "observed_suffix_size_bytes": observed_size,
            "observed_suffix_sha256": observation[
                "observed_suffix_sha256"],
        }
        # Persist the immutable crash observation before adding any remaining
        # event bytes.  A later recovery reuses this observation instead of
        # inferring a different cut point from the now-longer ledger.
        write_json_atomic(receipt_path, receipt)
    if len(suffix) < len(event_bytes):
        _append_run_metadata_event_bytes(
            events_path, event_bytes[len(suffix):])
    final_events = _read_run_metadata_events_strict(events_path)
    _fold_run_metadata_events(final_events)
    if (len(final_events) != pending["event_index"]
            or final_events[-1] != event):
        raise RuntimeError("run metadata pending event reconciliation failed")
    if (os.path.getsize(events_path) != immutable_receipt[
            "final_ledger_size_bytes"]
            or _sha256_file(events_path)
            != immutable_receipt["final_ledger_sha256"]):
        raise RuntimeError("run metadata pending event reconciliation failed")
    if receipt["state"] == "prepared":
        receipt = dict(receipt)
        receipt["state"] = "completed"
        receipt["completed_at"] = receipt["prepared_at"]
        write_json_atomic(receipt_path, receipt)
    _register_run_metadata_event_recovery_receipt_unlocked(out_dir, {
        "event_index": pending["event_index"],
        "filename": os.path.basename(receipt_path),
        "sha256": _sha256_file(receipt_path),
    })
    _clear_run_metadata_event_pending(out_dir)
    return True


def _read_run_metadata_event_recovery_receipts(out_dir, events_path):
    """Validate every durable WAL-recovery receipt against the event ledger."""
    recovery_dir = os.path.join(out_dir, METADATA_EVENT_RECOVERY_DIRECTORY)
    if not os.path.isdir(recovery_dir):
        return []
    try:
        names = sorted(
            name for name in os.listdir(recovery_dir)
            if name.endswith(".json")
            and name != METADATA_EVENT_RECOVERY_REGISTRY_FILENAME
        )
        with open(events_path, "rb") as handle:
            ledger = handle.read()
    except OSError as exc:
        raise RuntimeError(
            "run metadata event recovery evidence is unreadable") from exc
    ledger_events = _read_run_metadata_events_strict(events_path)
    required = {
        "schema", "state", "prepared_at", "completed_at",
        "pending_sha256", "prior_prefix_size_bytes", "prior_prefix_sha256",
        "prior_event_count", "event_index", "event_sha256",
        "observed_suffix_size_bytes", "observed_suffix_sha256",
        "final_event_count", "final_ledger_size_bytes",
        "final_ledger_sha256",
    }
    validated = []
    seen_indexes = set()
    for name in names:
        path = os.path.join(recovery_dir, name)
        try:
            with open(path, encoding="utf-8") as handle:
                receipt = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "run metadata event recovery receipt is invalid") from exc
        prior_size = (
            receipt.get("prior_prefix_size_bytes")
            if isinstance(receipt, dict) else None
        )
        prior_count = (
            receipt.get("prior_event_count")
            if isinstance(receipt, dict) else None
        )
        event_index = (
            receipt.get("event_index")
            if isinstance(receipt, dict) else None
        )
        final_size = (
            receipt.get("final_ledger_size_bytes")
            if isinstance(receipt, dict) else None
        )
        observed_size = (
            receipt.get("observed_suffix_size_bytes")
            if isinstance(receipt, dict) else None
        )
        if (not isinstance(receipt, dict) or set(receipt) != required
                or receipt.get("schema") != METADATA_EVENT_RECOVERY_SCHEMA
                or receipt.get("state") != "completed"
                or not _metadata_timestamp_is_aware(receipt.get("prepared_at"))
                or not _metadata_timestamp_is_aware(receipt.get("completed_at"))
                or receipt.get("completed_at") != receipt.get("prepared_at")
                or not isinstance(receipt.get("pending_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", receipt["pending_sha256"])
                or not isinstance(prior_size, int) or isinstance(prior_size, bool)
                or prior_size < 0
                or not isinstance(prior_count, int)
                or isinstance(prior_count, bool) or prior_count < 0
                or not isinstance(event_index, int)
                or isinstance(event_index, bool)
                or event_index != prior_count + 1
                or event_index in seen_indexes
                or not isinstance(final_size, int)
                or isinstance(final_size, bool) or final_size <= prior_size
                or final_size > len(ledger)
                or receipt.get("final_event_count") != event_index
                or not isinstance(observed_size, int)
                or isinstance(observed_size, bool) or observed_size < 0
                or not isinstance(receipt.get("prior_prefix_sha256"), str)
                or not re.fullmatch(
                    r"[0-9a-f]{64}", receipt["prior_prefix_sha256"])
                or not isinstance(receipt.get("event_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", receipt["event_sha256"])
                or not isinstance(receipt.get("observed_suffix_sha256"), str)
                or not re.fullmatch(
                    r"[0-9a-f]{64}", receipt["observed_suffix_sha256"])
                or not isinstance(receipt.get("final_ledger_sha256"), str)
                or not re.fullmatch(
                    r"[0-9a-f]{64}", receipt["final_ledger_sha256"])):
            raise RuntimeError(
                "run metadata event recovery receipt is invalid")
        event_bytes = ledger[prior_size:final_size]
        if (observed_size > len(event_bytes)
                or name != f"{event_index:08d}_{receipt['event_sha256'][:16]}.json"
                or hashlib.sha256(ledger[:prior_size]).hexdigest()
                != receipt["prior_prefix_sha256"]
                or hashlib.sha256(event_bytes).hexdigest()
                != receipt["event_sha256"]
                or hashlib.sha256(event_bytes[:observed_size]).hexdigest()
                != receipt["observed_suffix_sha256"]
                or hashlib.sha256(ledger[:final_size]).hexdigest()
                != receipt["final_ledger_sha256"]):
            raise RuntimeError(
                "run metadata event recovery receipt does not match the ledger")
        prior_bytes = ledger[:prior_size]
        prior_events = (
            _decode_run_metadata_event_bytes(
                prior_bytes, label="run metadata recovery prior prefix")
            if prior_bytes else []
        )
        event_rows = _decode_run_metadata_event_bytes(
            event_bytes, label="run metadata recovered event")
        if ((prior_count == 0 and prior_size != 0)
                or len(prior_events) != prior_count
                or prior_events != ledger_events[:prior_count]
                or len(event_rows) != 1
                or event_rows[0].get("event_index") != event_index
                or len(ledger_events) < event_index
                or event_rows[0] != ledger_events[event_index - 1]
                ):
            raise RuntimeError(
                "run metadata event recovery receipt prefix is invalid")
        reconstructed_pending = {
            "schema": METADATA_EVENT_PENDING_SCHEMA,
            "events_file": METADATA_EVENTS_FILENAME,
            "event_schema": METADATA_EVENT_SCHEMA,
            "prior_prefix_size_bytes": prior_size,
            "prior_prefix_sha256": receipt["prior_prefix_sha256"],
            "prior_event_count": prior_count,
            "event_index": event_index,
            "event_line": event_bytes.decode("utf-8"),
            "event_sha256": receipt["event_sha256"],
            "recovery_observation": {
                "prepared_at": receipt["prepared_at"],
                "observed_suffix_size_bytes": observed_size,
                "observed_suffix_sha256": receipt[
                    "observed_suffix_sha256"],
            },
        }
        if (_canonical_record_sha256(reconstructed_pending)
                != receipt["pending_sha256"]):
            raise RuntimeError(
                "run metadata event recovery receipt pending identity is invalid")
        seen_indexes.add(event_index)
        validated.append({
            "event_index": event_index,
            "filename": name,
            "sha256": _sha256_file(path),
        })
    registry = _read_run_metadata_event_recovery_registry(
        out_dir, required=False)
    if registry is None:
        raise RuntimeError(
            "run metadata event recovery registry is missing")
    if registry["receipts"] != validated:
        raise RuntimeError(
            "run metadata event recovery registry does not match receipts")
    return validated


def _run_metadata_event_recovery_manifest(receipts, *, event_count):
    selected = [
        dict(receipt) for receipt in receipts
        if receipt["event_index"] <= event_count
    ]
    payload = json.dumps(
        selected,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "event_recovery_receipt_count": len(selected),
        "event_recovery_receipts_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _append_run_metadata_event_unlocked(out_dir, event):
    _reconcile_run_metadata_event_pending_unlocked(out_dir)
    # The registry is also the durable event-store marker for standalone
    # runners that have no dispatch manifest.  Publishing it before the first
    # event prevents a later events+projection-receipt loss from silently
    # reclassifying the compatibility snapshot as legacy metadata.
    _ensure_run_metadata_event_recovery_registry_unlocked(out_dir)
    events_path = _run_metadata_events_path(out_dir)
    events = (
        _read_run_metadata_events_strict(events_path)
        if os.path.isfile(events_path) else []
    )
    payload = dict(event)
    payload.update({
        "schema": METADATA_EVENT_SCHEMA,
        "event_index": len(events) + 1,
    })
    _fold_run_metadata_events(events + [payload])
    os.makedirs(out_dir, exist_ok=True)
    prior = b""
    if os.path.isfile(events_path):
        with open(events_path, "rb") as handle:
            prior = handle.read()
    event_bytes = (
        json.dumps(payload, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    pending = {
        "schema": METADATA_EVENT_PENDING_SCHEMA,
        "events_file": METADATA_EVENTS_FILENAME,
        "event_schema": METADATA_EVENT_SCHEMA,
        "prior_prefix_size_bytes": len(prior),
        "prior_prefix_sha256": hashlib.sha256(prior).hexdigest(),
        "prior_event_count": len(events),
        "event_index": payload["event_index"],
        "event_line": event_bytes.decode("utf-8"),
        "event_sha256": hashlib.sha256(event_bytes).hexdigest(),
    }
    write_json_atomic(_run_metadata_event_pending_path(out_dir), pending)
    _append_run_metadata_event_bytes(events_path, event_bytes)
    _clear_run_metadata_event_pending(out_dir)
    return payload


def _read_run_metadata_event_prefix(events_path, receipt):
    size_bytes = receipt.get("event_prefix_size_bytes")
    event_count = receipt.get("event_count")
    if (not isinstance(size_bytes, int) or isinstance(size_bytes, bool)
            or size_bytes <= 0
            or not isinstance(event_count, int) or isinstance(event_count, bool)
            or event_count <= 0):
        raise RuntimeError("run metadata projection receipt prefix is invalid")
    with open(events_path, "rb") as handle:
        current = handle.read()
    if len(current) < size_bytes:
        raise RuntimeError("run metadata event prefix was truncated")
    prefix = current[:size_bytes]
    if (not prefix.endswith(b"\n")
            or hashlib.sha256(prefix).hexdigest()
            != receipt.get("event_prefix_sha256")):
        raise RuntimeError("run metadata event prefix digest mismatch")
    try:
        rows = [
            json.loads(line)
            for line in prefix.decode("utf-8").splitlines()
            if line.strip()
        ]
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError("run metadata event prefix is invalid") from exc
    if len(rows) != event_count or any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("run metadata event prefix count mismatch")
    return rows, len(current)


def _read_run_metadata_projection_receipt(receipt_path):
    try:
        with open(receipt_path, encoding="utf-8") as handle:
            receipt = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "run metadata projection receipt is invalid") from exc
    receipt_keys = {
        "schema", "events_file", "event_schema", "event_count",
        "event_prefix_size_bytes", "event_prefix_sha256", "projection_sha256",
        "projection_record_count", "snapshot_file", "snapshot_sha256",
        "event_recovery_receipt_count",
        "event_recovery_receipts_sha256",
    }
    if (not isinstance(receipt, dict) or set(receipt) != receipt_keys
            or receipt.get("schema") != METADATA_PROJECTION_RECEIPT_SCHEMA
            or receipt.get("events_file") != METADATA_EVENTS_FILENAME
            or receipt.get("event_schema") != METADATA_EVENT_SCHEMA
            or receipt.get("snapshot_file") != "run_metadata.jsonl"
            or not isinstance(receipt.get("event_prefix_sha256"), str)
            or not re.fullmatch(
                r"[0-9a-f]{64}", receipt["event_prefix_sha256"])
            or not isinstance(receipt.get("projection_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", receipt["projection_sha256"])
            or not isinstance(receipt.get("snapshot_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", receipt["snapshot_sha256"])
            or not isinstance(
                receipt.get("event_recovery_receipt_count"), int)
            or isinstance(receipt.get("event_recovery_receipt_count"), bool)
            or receipt["event_recovery_receipt_count"] < 0
            or not isinstance(
                receipt.get("event_recovery_receipts_sha256"), str)
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                receipt["event_recovery_receipts_sha256"]
            )):
        raise RuntimeError("run metadata projection receipt is invalid")
    return receipt


def _validate_run_metadata_projection_receipt_prefix(
        events_path, receipt, *, recovery_receipts=None):
    prefix_events, current_size = _read_run_metadata_event_prefix(
        events_path, receipt)
    prefix_projection = _fold_run_metadata_events(prefix_events)
    if recovery_receipts is None:
        recovery_receipts = _read_run_metadata_event_recovery_receipts(
            os.path.dirname(events_path), events_path)
    recovery_manifest = _run_metadata_event_recovery_manifest(
        recovery_receipts, event_count=receipt.get("event_count"))
    if (receipt.get("projection_record_count") != len(prefix_projection)
            or receipt.get("projection_sha256")
            != _run_metadata_projection_sha256(prefix_projection)
            or any(receipt.get(key) != value
                   for key, value in recovery_manifest.items())):
        raise RuntimeError("run metadata projection receipt is invalid")
    return prefix_projection, current_size


def _validate_run_metadata_projection_cache(
        out_dir, metadata_path, events_path, current_projection,
        *, recovery_receipts=None):
    receipt_path = _run_metadata_projection_receipt_path(out_dir)
    snapshot_exists = os.path.isfile(metadata_path)
    receipt_exists = os.path.isfile(receipt_path)
    if not snapshot_exists and not receipt_exists:
        return
    if snapshot_exists is not receipt_exists:
        raise RuntimeError(
            "run metadata compatibility snapshot/receipt is incomplete")
    receipt = _read_run_metadata_projection_receipt(receipt_path)
    prefix_projection, current_size = (
        _validate_run_metadata_projection_receipt_prefix(
            events_path, receipt, recovery_receipts=recovery_receipts)
    )
    snapshot = _read_run_metadata_strict(metadata_path)
    if (snapshot != prefix_projection
            or _sha256_file(metadata_path) != receipt.get("snapshot_sha256")):
        raise RuntimeError("run metadata compatibility snapshot drifted")
    if (receipt["event_prefix_size_bytes"] == current_size
            and snapshot != current_projection):
        raise RuntimeError("current run metadata projection is inconsistent")


def _read_run_metadata_snapshot_unlocked(out_dir, metadata_path):
    events_path = _run_metadata_events_path(out_dir)
    mode = _run_metadata_storage_mode_unlocked(out_dir, metadata_path)
    if os.path.isfile(events_path):
        events = _read_run_metadata_events_strict(events_path)
        projection = _fold_run_metadata_events(events)
        recovery_receipts = _read_run_metadata_event_recovery_receipts(
            out_dir, events_path)
        _validate_run_metadata_projection_cache(
            out_dir, metadata_path, events_path, projection,
            recovery_receipts=recovery_receipts)
        return projection
    if mode == "event":
        return []
    return _read_run_metadata_strict(metadata_path)


def _publish_run_metadata_projection_unlocked(
        out_dir, metadata_path, projection):
    if not projection or any(
            record.get("status") == "running" for record in projection):
        return False
    events_path = _run_metadata_events_path(out_dir)
    if not os.path.isfile(events_path):
        return False
    prior_snapshot = (
        _read_run_metadata_strict(metadata_path)
        if os.path.isfile(metadata_path) else None
    )
    try:
        _write_jsonl_atomic(metadata_path, projection)
    except OSError:
        return False
    events = _read_run_metadata_events_strict(events_path)
    recovery_receipts = _read_run_metadata_event_recovery_receipts(
        out_dir, events_path)
    recovery_manifest = _run_metadata_event_recovery_manifest(
        recovery_receipts, event_count=len(events))
    size_bytes = os.path.getsize(events_path)
    receipt = {
        "schema": METADATA_PROJECTION_RECEIPT_SCHEMA,
        "events_file": METADATA_EVENTS_FILENAME,
        "event_schema": METADATA_EVENT_SCHEMA,
        "event_count": len(events),
        "event_prefix_size_bytes": size_bytes,
        "event_prefix_sha256": _sha256_file(events_path),
        "projection_sha256": _run_metadata_projection_sha256(projection),
        "projection_record_count": len(projection),
        "snapshot_file": "run_metadata.jsonl",
        "snapshot_sha256": _sha256_file(metadata_path),
        **recovery_manifest,
    }
    try:
        write_json_atomic(_run_metadata_projection_receipt_path(out_dir), receipt)
    except OSError:
        try:
            if prior_snapshot is None:
                os.unlink(metadata_path)
            else:
                _write_jsonl_atomic(metadata_path, prior_snapshot)
        except OSError as rollback_exc:
            raise RuntimeError(
                "run metadata projection publish rollback failed") from rollback_exc
        return False
    return True


def _reconcile_run_metadata_projection_unlocked(
        out_dir, metadata_path, projection=None):
    """Idempotently finish a quiescent derived-cache publication.

    The append-only event ledger is authoritative.  This helper only repairs
    cache states that can be produced by a crash between the atomic snapshot
    and receipt replacements, or by a reported publication failure that rolled
    back to the previous valid prefix.  Other drift remains fail-closed.
    """
    events_path = _run_metadata_events_path(out_dir)
    if not os.path.isfile(events_path):
        raise RuntimeError("run metadata event ledger is missing")
    if projection is None:
        events = _read_run_metadata_events_strict(events_path)
        if not events:
            raise RuntimeError("run metadata event ledger is empty")
        projection = _fold_run_metadata_events(events)
    if not projection or any(
            record.get("status") == "running" for record in projection):
        return False
    recovery_receipts = _read_run_metadata_event_recovery_receipts(
        out_dir, events_path)

    receipt_path = _run_metadata_projection_receipt_path(out_dir)
    snapshot_exists = os.path.isfile(metadata_path)
    receipt_exists = os.path.isfile(receipt_path)
    if receipt_exists and not snapshot_exists:
        raise RuntimeError(
            "run metadata compatibility snapshot/receipt is incomplete")
    if snapshot_exists and not receipt_exists:
        if _read_run_metadata_strict(metadata_path) != projection:
            raise RuntimeError(
                "unreceipted run metadata compatibility snapshot drifted")
    elif snapshot_exists and receipt_exists:
        receipt = _read_run_metadata_projection_receipt(receipt_path)
        prefix_projection, current_size = (
            _validate_run_metadata_projection_receipt_prefix(
                events_path, receipt, recovery_receipts=recovery_receipts)
        )
        snapshot = _read_run_metadata_strict(metadata_path)
        snapshot_sha_matches = (
            _sha256_file(metadata_path) == receipt.get("snapshot_sha256")
        )
        valid_prefix_cache = (
            snapshot == prefix_projection and snapshot_sha_matches
        )
        interrupted_current_snapshot = (
            snapshot == projection
            and receipt.get("event_prefix_size_bytes") < current_size
        )
        if not valid_prefix_cache and not interrupted_current_snapshot:
            raise RuntimeError("run metadata compatibility snapshot drifted")
        if (valid_prefix_cache
                and receipt.get("event_prefix_size_bytes") == current_size):
            if snapshot != projection:
                raise RuntimeError(
                    "current run metadata projection is inconsistent")
            return True

    if not _publish_run_metadata_projection_unlocked(
            out_dir, metadata_path, projection):
        raise RuntimeError(
            "run metadata terminal event is durable but projection publication "
            "is incomplete; retry is safe")
    return True


def reconcile_run_metadata_projection(out_dir):
    """Explicitly republish a quiescent event projection after an interrupted write."""
    with _campaign_metadata_lock(out_dir) as metadata_path:
        _reconcile_run_metadata_event_pending_unlocked(out_dir)
        if (_run_metadata_storage_mode_unlocked(out_dir, metadata_path)
                != "event"
                or not os.path.isfile(_run_metadata_events_path(out_dir))):
            raise RuntimeError("run metadata event ledger is missing")
        return _reconcile_run_metadata_projection_unlocked(
            out_dir, metadata_path)


def _read_run_metadata_for_mutation_unlocked(out_dir, metadata_path):
    """Read one store for mutation, completing any prior terminal publication."""
    _reconcile_run_metadata_event_pending_unlocked(out_dir)
    event_mode = (
        _run_metadata_storage_mode_unlocked(out_dir, metadata_path) == "event"
    )
    events_path = _run_metadata_events_path(out_dir)
    if event_mode and os.path.isfile(events_path):
        projection = _fold_run_metadata_events(
            _read_run_metadata_events_strict(events_path))
        if projection and not any(
                record.get("status") == "running" for record in projection):
            _reconcile_run_metadata_projection_unlocked(
                out_dir, metadata_path, projection)
            return event_mode, projection
    records = (
        _read_run_metadata_snapshot_unlocked(out_dir, metadata_path)
        if event_mode else _read_run_metadata_strict(metadata_path)
    )
    return event_mode, records


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
        event_mode, records = (
            _read_run_metadata_for_mutation_unlocked(out_dir, metadata_path)
        )
        if not records:
            raise RuntimeError("task plan cannot be registered before run metadata")
        prior_manifest = _one_prior_value(records, "task_plans") or {}
        previous = prior_manifest.get(sample_id)
        if previous is not None and previous != entry:
            raise RuntimeError(
                f"refusing task-plan drift for {sample_id}: "
                f"registered {previous}, current {entry}"
            )
        if event_mode:
            if previous == entry:
                return dict(entry)
            _append_run_metadata_event_unlocked(out_dir, {
                "event": "task_plan_registered",
                "sample_id": sample_id,
                "task_plan": entry,
            })
            projection = _read_run_metadata_snapshot_unlocked(
                out_dir, metadata_path)
            _reconcile_run_metadata_projection_unlocked(
                out_dir, metadata_path, projection)
            return dict(entry)
        manifest = dict(prior_manifest)
        manifest[sample_id] = entry
        for record in records:
            record["task_plans"] = manifest
        _write_jsonl_atomic(metadata_path, records)
    return dict(entry)


def read_run_metadata_snapshot(out_dir, *, _locked_metadata_path=None):
    """Return the only canonical reduction of legacy or event metadata.

    Legacy directories without an event ledger retain the exact /3 JSONL
    behavior. New directories reduce the append-only event ledger; an optional
    compatibility snapshot is only a receipt-bound cache of an event prefix.
    """
    if _locked_metadata_path is not None:
        _reconcile_run_metadata_event_pending_unlocked(out_dir)
        return [dict(record) for record in _read_run_metadata_snapshot_unlocked(
            out_dir, _locked_metadata_path)]
    with _campaign_metadata_lock(out_dir) as metadata_path:
        _reconcile_run_metadata_event_pending_unlocked(out_dir)
        return [dict(record) for record in _read_run_metadata_snapshot_unlocked(
            out_dir, metadata_path)]


def read_quiescent_run_metadata_snapshot(out_dir):
    """Return metadata only when the final compatibility cache is current.

    Event-ledger campaigns may retain a valid snapshot of an older quiescent
    prefix while a later invocation is running.  Runtime readers must accept
    that cache as historical evidence and reduce the full event ledger.  An
    offline/finalization reader has a stricter boundary: every invocation is
    terminal and the compatibility snapshot plus receipt must cover the exact
    current event bytes.  Legacy ``run_metadata/3`` directories keep their
    historical read contract.
    """
    with _campaign_metadata_lock(out_dir) as metadata_path:
        if os.path.exists(_run_metadata_event_pending_path(out_dir)):
            raise RuntimeError(
                "run metadata event campaign has a pending event intent")
        records = _read_run_metadata_snapshot_unlocked(out_dir, metadata_path)
        events_path = _run_metadata_events_path(out_dir)
        if not os.path.isfile(events_path):
            return [dict(record) for record in records]
        if not records or any(
                record.get("status") == "running" for record in records):
            raise RuntimeError("run metadata event campaign is not quiescent")
        receipt_path = _run_metadata_projection_receipt_path(out_dir)
        if (not os.path.isfile(metadata_path)
                or not os.path.isfile(receipt_path)):
            raise RuntimeError(
                "quiescent run metadata compatibility snapshot is unpublished")
        try:
            with open(receipt_path, encoding="utf-8") as handle:
                receipt = json.load(handle)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                "run metadata projection receipt is invalid") from exc
        event_size = os.path.getsize(events_path)
        events = _read_run_metadata_events_strict(events_path)
        if (not isinstance(receipt, dict)
                or receipt.get("event_prefix_size_bytes") != event_size
                or receipt.get("event_prefix_sha256") != _sha256_file(events_path)
                or receipt.get("event_count") != len(events)
                or receipt.get("projection_record_count") != len(records)
                or receipt.get("projection_sha256")
                != _run_metadata_projection_sha256(records)):
            raise RuntimeError(
                "quiescent run metadata compatibility snapshot is stale")
        return [dict(record) for record in records]


def interrupt_running_invocations(out_dir, *, status, worker_launch_ids=None):
    """Close stale runner invocations after their process leases are released.

    Records are retained in place with an explicit non-success status.  The
    caller must first establish that the corresponding worker processes have
    stopped; this helper only performs the atomic ledger transition.
    """
    if status not in RUN_METADATA_TERMINAL_STATUSES:
        raise ValueError("interrupt status must be a non-running string")
    selected = None if worker_launch_ids is None else set(worker_launch_ids)
    finished_now = _aware_now()
    changed = []
    with _campaign_metadata_lock(out_dir) as metadata_path:
        event_mode, records = (
            _read_run_metadata_for_mutation_unlocked(out_dir, metadata_path)
        )
        transitions = []
        for record in records:
            if record.get("status") != "running":
                continue
            worker_launch_id = record.get("worker_launch_id")
            if selected is not None and worker_launch_id not in selected:
                continue
            transitions.append({
                "invocation_id": record.get("invocation_id"),
                "worker_launch_id": worker_launch_id,
                "worker_pid": record.get("worker_pid"),
                "samples": list(record.get("samples") or []),
                "status": status,
            })
            changed.append({
                "invocation_id": record.get("invocation_id"),
                "worker_launch_id": worker_launch_id,
                "worker_pid": record.get("worker_pid"),
                "samples": list(record.get("samples") or []),
            })
        if changed:
            finished_at = _iso_with_timezone(finished_now)
            if event_mode:
                _append_run_metadata_event_unlocked(out_dir, {
                    "event": "invocations_interrupted",
                    "finished_at": finished_at,
                    "invocations": transitions,
                })
                projection = _read_run_metadata_snapshot_unlocked(
                    out_dir, metadata_path)
                _reconcile_run_metadata_projection_unlocked(
                    out_dir, metadata_path, projection)
                return changed
            for record, transition in zip(
                    [
                        record for record in records
                        if record.get("invocation_id") in {
                            item["invocation_id"] for item in transitions
                        }
                    ],
                    transitions,
            ):
                record["status"] = transition["status"]
                record["invocation_finished_at"] = finished_at
            active = [
                record for record in records
                if record.get("status") == "running"
            ]
            campaign_finished_at = (
                None if active else finished_at
            )
            for record in records:
                record["finished_at"] = campaign_finished_at
            _write_jsonl_atomic(metadata_path, records)
    return changed


def interrupt_audited_running_invocations(
        out_dir, *, status=None, statuses=None, audited):
    """Atomically close exactly the stale invocations audited by the caller.

    Any new or identity-changed ``running`` record makes the transition fail
    without editing metadata. This closes the audit-to-interrupt TOCTOU window.
    """
    if (status is None) == (statuses is None):
        raise ValueError(
            "provide exactly one uniform status or per-invocation statuses")
    if status is not None and status not in RUN_METADATA_TERMINAL_STATUSES:
        raise ValueError("interrupt status must be a non-running string")
    if statuses is not None and not isinstance(statuses, dict):
        raise ValueError("per-invocation statuses must be a mapping")
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
    status_by_invocation = (
        {invocation_id: status for invocation_id in expected}
        if status is not None else dict(statuses)
    )
    if (set(status_by_invocation) != set(expected)
            or any(
                value not in RUN_METADATA_TERMINAL_STATUSES
                for value in status_by_invocation.values()
            )):
        raise RuntimeError(
            "per-invocation terminal statuses are invalid")
    if not expected:
        with _campaign_metadata_lock(out_dir) as metadata_path:
            _read_run_metadata_for_mutation_unlocked(
                out_dir, metadata_path)
        return []
    changed = []
    with _campaign_metadata_lock(out_dir) as metadata_path:
        event_mode, records = (
            _read_run_metadata_for_mutation_unlocked(out_dir, metadata_path)
        )
        running = [record for record in records
                   if record.get("status") == "running"]
        actual_id_list = [record.get("invocation_id") for record in running]
        if (any(not isinstance(value, str) or not value
                for value in actual_id_list)
                or len(actual_id_list) != len(set(actual_id_list))):
            raise RuntimeError("running invocation identities are invalid")
        actual_ids = set(actual_id_list)
        if event_mode and not actual_ids:
            by_invocation = {
                record.get("invocation_id"): record for record in records
            }
            already_applied = []
            for invocation_id, identity in expected.items():
                record = by_invocation.get(invocation_id)
                worker_id, worker_pid, sample = identity
                if (not isinstance(record, dict)
                        or record.get("status")
                        != status_by_invocation[invocation_id]
                        or record.get("worker_launch_id") != worker_id
                        or record.get("worker_pid") != worker_pid
                        or record.get("samples") != [sample]):
                    break
                already_applied.append({
                    "invocation_id": invocation_id,
                    "worker_launch_id": worker_id,
                    "worker_pid": worker_pid,
                    "samples": [sample],
                })
            else:
                _reconcile_run_metadata_projection_unlocked(
                    out_dir, metadata_path, records)
                return already_applied
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
        finished_at = _iso_with_timezone(finished_now)
        transitions = []
        for record in running:
            transition_status = status_by_invocation[record["invocation_id"]]
            transitions.append({
                "invocation_id": record.get("invocation_id"),
                "worker_launch_id": record.get("worker_launch_id"),
                "worker_pid": record.get("worker_pid"),
                "samples": list(record.get("samples") or []),
                "status": transition_status,
            })
            changed.append({
                "invocation_id": record.get("invocation_id"),
                "worker_launch_id": record.get("worker_launch_id"),
                "worker_pid": record.get("worker_pid"),
                "samples": list(record.get("samples") or []),
            })
        if changed:
            if event_mode:
                _append_run_metadata_event_unlocked(out_dir, {
                    "event": "invocations_interrupted",
                    "finished_at": finished_at,
                    "invocations": transitions,
                })
                projection = _read_run_metadata_snapshot_unlocked(
                    out_dir, metadata_path)
                _reconcile_run_metadata_projection_unlocked(
                    out_dir, metadata_path, projection)
                return changed
            for record, transition in zip(running, transitions):
                record["status"] = transition["status"]
                record["invocation_finished_at"] = finished_at
            for record in records:
                record["finished_at"] = finished_at
            _write_jsonl_atomic(metadata_path, records)
    return changed


def _dispatch_key_metadata_from_env():
    dispatch_key_label = os.environ.get("ANCHORPATCH_DISPATCH_KEY_LABEL")
    original_key_label = os.environ.get("ANCHORPATCH_ORIGINAL_KEY_LABEL")
    failover_count_raw = os.environ.get("ANCHORPATCH_KEY_FAILOVER_COUNT")
    prior_key_label = os.environ.get("ANCHORPATCH_PRIOR_KEY_LABEL")
    failover_reason = os.environ.get("ANCHORPATCH_KEY_FAILOVER_REASON")
    values = (
        dispatch_key_label, original_key_label, failover_count_raw,
        prior_key_label, failover_reason,
    )
    if all(value is None for value in values):
        return None
    if (not dispatch_key_label or not original_key_label
            or failover_count_raw is None):
        raise RuntimeError("dispatch key metadata is incomplete")
    try:
        failover_count = int(failover_count_raw)
    except ValueError as exc:
        raise RuntimeError(
            "dispatch key failover count is invalid") from exc
    if str(failover_count) != failover_count_raw or failover_count < 0:
        raise RuntimeError("dispatch key failover count is invalid")
    if failover_count == 0:
        if (dispatch_key_label != original_key_label
                or prior_key_label is not None
                or failover_reason is not None):
            raise RuntimeError("dispatch key failover provenance is invalid")
    elif (not prior_key_label or not failover_reason
            or dispatch_key_label == prior_key_label):
        raise RuntimeError("dispatch key failover provenance is invalid")
    return {
        "dispatch_key_label": dispatch_key_label,
        "original_key_label": original_key_label,
        "prior_key_label": prior_key_label,
        "failover_count": failover_count,
        "failover_reason": failover_reason,
    }


def append_run_metadata(out_dir, *, command, samples, methods, num_round_trips,
                        seed, model, distractor, max_tokens, notes="", printing=True,
                        context_shuffle_seeded=False,
                        context_shuffle_seed_version=None,
                        stop_on_collapse=False,
                        stop_on_preservation_violation=False,
                        reasoning_effort=None,
                        snapshot_mode=SNAPSHOT_MODE_ALL):
    """Register one invocation in a locked V8 campaign metadata ledger.

    The first invocation establishes the campaign Git identity and timezone-aware
    ``started_at``. Concurrent or resumed invocations reuse those exact values.
    Any commit, clean/dirty state, fingerprint, or transport mixture is rejected
    before a record is added. Fresh directories use an append-only event ledger;
    legacy ``run_metadata/3`` directories are never migrated. Once no invocation
    is running, ``finish_run_metadata`` publishes the shared ``finished_at`` as a
    receipt-bound compatibility projection.
    """
    del printing  # V3 turns the old fingerprint warning into a hard refusal.
    snapshot_mode = normalize_snapshot_mode(
        snapshot_mode, default=SNAPSHOT_MODE_ALL)
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
    from model_openai import model_runtime_config
    provider_runtime = model_runtime_config(
        model,
        max_tokens=max_tokens,
        thinking_mode="adaptive",
        reasoning_effort=reasoning_effort,
    )
    invocation_now = _aware_now()
    invocation_id = uuid.uuid4().hex
    dispatch_key_metadata = _dispatch_key_metadata_from_env()
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
        _reconcile_run_metadata_event_pending_unlocked(out_dir)
        events_path = _run_metadata_events_path(out_dir)
        event_mode = (
            _run_metadata_storage_mode_unlocked(out_dir, path) == "event"
        )
        if os.path.isfile(events_path):
            event_projection = _fold_run_metadata_events(
                _read_run_metadata_events_strict(events_path))
            if event_projection and not any(
                    record.get("status") == "running"
                    for record in event_projection):
                _reconcile_run_metadata_projection_unlocked(
                    out_dir, path, event_projection)
        prior = (
            _read_run_metadata_snapshot_unlocked(out_dir, path)
            if os.path.isfile(events_path) else _read_run_metadata_strict(path)
        )
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
                    if boundary is None:
                        return True
                    continue
                if (
                    isinstance(boundary, dict)
                    and boundary.get("authorization_id") == authorization_id
                    and boundary.get("authorization_sha256")
                    == identity.get("authorization_sha256")
                ):
                    return True
            return False

        recovery_identity_history_valid = bool(
            recovery_authorization
            and prior
            and all(_matches_authorized_identity(record) for record in prior)
        )
        if (recovery_authorization and prior
                and not recovery_identity_history_valid):
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: prior recovery "
                "authorization boundary differs from the validated chain"
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

        if provider_runtime.get("transport_revision") is not None and prior:
            current_revision = provider_runtime["transport_revision"]
            previous_revision = _one_prior_value(prior, "transport_revision")
            if previous_revision != current_revision:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior transport revision "
                    f"is {previous_revision or 'unrecorded'}, current revision is "
                    f"{current_revision}; use a new --out_dir"
                )
            current_policy = provider_runtime["transport_resume_policy"]
            previous_policies = {
                record.get("transport_resume_policy") for record in prior
            }
            if previous_policies != {current_policy}:
                raise RuntimeError(
                    f"refusing to resume/mix {out_dir!r}: prior transport "
                    f"resume policies are {sorted(map(str, previous_policies))}, "
                    f"current policy is {current_policy}; use a new --out_dir"
                )

        campaign_config = {
            "method_set": campaign_methods,
            "num_round_trips": num_round_trips,
            "seed": seed,
            "model": model,
            "distractor": bool(distractor),
            "max_tokens": max_tokens,
            "reasoning_effort": reasoning_effort,
            "context_shuffle_seeded": bool(context_shuffle_seeded),
            "context_shuffle_seed_version": (
                context_shuffle_seed_version if context_shuffle_seeded else None
            ),
            "stop_on_collapse": bool(stop_on_collapse),
            "stop_on_preservation_violation": bool(
                stop_on_preservation_violation
            ),
            "snapshot_mode": snapshot_mode,
        }
        previous_config = _one_prior_value(prior, "campaign_config")
        if prior and previous_config is None:
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: prior campaign_config "
                "is unrecorded"
            )
        if (previous_config is not None
                and not _campaign_configs_match(
                    previous_config, campaign_config)):
            raise RuntimeError(
                f"refusing to resume/mix {out_dir!r}: prior campaign_config "
                "differs from the current invocation; use a new --out_dir"
            )
        campaign_config = _campaign_config_for_storage(
            campaign_config, previous_config)

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
            "reasoning_effort": reasoning_effort,
            "code_fingerprint": fp, "notes": notes,
            "campaign_config": campaign_config,
            "task_plans": _one_prior_value(prior, "task_plans") or {},
            "worker_launch_id": os.environ.get(
                "ANCHORPATCH_WORKER_LAUNCH_ID"
            ),
            "worker_pid": os.getpid(),
            "dispatcher_pid": (
                int(os.environ["ANCHORPATCH_DISPATCHER_PID"])
                if os.environ.get("ANCHORPATCH_DISPATCHER_PID")
                else None
            ),
            "dispatcher_instance_id": os.environ.get(
                "ANCHORPATCH_DISPATCHER_INSTANCE_ID"),
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
        if dispatch_key_metadata is not None:
            rec.update(dispatch_key_metadata)
        rec.update(provider_runtime)
        rec["context_shuffle_seeded"] = bool(context_shuffle_seeded)
        rec["context_shuffle_seed_version"] = (
            context_shuffle_seed_version if context_shuffle_seeded else None
        )
        rec["stop_on_collapse"] = bool(stop_on_collapse)
        rec["stop_on_preservation_violation"] = bool(
            stop_on_preservation_violation
        )
        if event_mode:
            event_record = dict(rec)
            event_record.pop("task_plans", None)
            _append_run_metadata_event_unlocked(out_dir, {
                "event": "invocation_registered",
                "record": event_record,
            })
        else:
            records = prior + [rec]
            _write_jsonl_atomic(path, records)
        return dict(rec)


def finish_run_metadata(out_dir, invocation_id, *, status="finished"):
    """Close one invocation and atomically set the shared campaign finish time."""
    if status not in RUN_METADATA_TERMINAL_STATUSES:
        raise ValueError("finish status must be a non-running string")
    finished_now = _aware_now()
    with _campaign_metadata_lock(out_dir) as path:
        _reconcile_run_metadata_event_pending_unlocked(out_dir)
        event_mode = (
            _run_metadata_storage_mode_unlocked(out_dir, path) == "event"
        )
        if event_mode and os.path.isfile(_run_metadata_events_path(out_dir)):
            event_projection = _fold_run_metadata_events(
                _read_run_metadata_events_strict(
                    _run_metadata_events_path(out_dir)))
            if event_projection and not any(
                    record.get("status") == "running"
                    for record in event_projection):
                _reconcile_run_metadata_projection_unlocked(
                    out_dir, path, event_projection)
        records = (
            _read_run_metadata_snapshot_unlocked(out_dir, path)
            if event_mode else _read_run_metadata_strict(path)
        )
        matches = [record for record in records
                   if record.get("invocation_id") == invocation_id]
        if len(matches) != 1:
            raise RuntimeError(
                f"cannot finish unknown or duplicate invocation {invocation_id!r}"
            )
        target = matches[0]
        if target.get("status") != "running":
            if event_mode and target.get("status") == status:
                _reconcile_run_metadata_projection_unlocked(
                    out_dir, path, records)
                return dict(target)
            raise RuntimeError(
                f"cannot finish non-running invocation {invocation_id!r}: "
                f"status={target.get('status')!r}"
            )
        finished_at = _iso_with_timezone(finished_now)
        if event_mode:
            _append_run_metadata_event_unlocked(out_dir, {
                "event": "invocation_terminal",
                "invocation_id": invocation_id,
                "status": status,
                "finished_at": finished_at,
            })
            projection = _read_run_metadata_snapshot_unlocked(out_dir, path)
            _reconcile_run_metadata_projection_unlocked(
                out_dir, path, projection)
            return next(
                dict(record) for record in projection
                if record.get("invocation_id") == invocation_id
            )
        target["status"] = status
        target["invocation_finished_at"] = finished_at
        active = [record for record in records if record.get("status") == "running"]
        campaign_finished_at = None if active else finished_at
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


_MAX_SNAPSHOT_COMPONENT = 80
_MAX_DOC_FILENAME = 120
_SNAPSHOT_FULL_PATH_BUDGET = 240
_WINDOWS_RESERVED_SNAPSHOT_NAMES = frozenset({
    "CON", "PRN", "AUX", "NUL",
    *{f"COM{index}" for index in range(1, 10)},
    *{f"LPT{index}" for index in range(1, 10)},
})


def _snapshot_digest(value, size=16):
    return hashlib.sha256(
        str(value).encode("utf-8", errors="replace")).hexdigest()[:size]


def _portable_snapshot_name(text, safe, max_len, *, reserve_step=False):
    if (not safe or safe in {".", ".."} or safe != text
            or len(safe) > max_len or safe != safe.casefold()
            or safe.endswith((".", " "))):
        return False
    stem = safe.split(".", 1)[0].rstrip(". ").upper()
    if stem in _WINDOWS_RESERVED_SNAPSHOT_NAMES:
        return False
    if reserve_step and safe.casefold() == "_step.json":
        return False
    return True


def _hashed_snapshot_component(name, *, fallback="item",
                               max_len=_MAX_SNAPSHOT_COMPONENT):
    text = str(name)
    safe = _safe(text)
    if _portable_snapshot_name(text, safe, max_len):
        return safe
    safe = safe.strip("._") or fallback
    digest = _snapshot_digest(text)
    suffix = f"__{digest}"
    if max_len < len(suffix) + 8:
        return f"h_{_snapshot_digest(text, max(8, max_len - 2))}"[:max_len]
    keep = max(8, max_len - len(suffix))
    prefix = (safe[:keep].rstrip("._") or fallback)
    return f"{prefix}{suffix}"


def _hashed_doc_filename(name):
    text = str(name)
    safe = _safe(text)
    safe = safe.strip("._") or "unnamed"
    digest = _snapshot_digest(text)
    root, ext = os.path.splitext(safe)
    if len(ext) > 16:
        root, ext = safe, ""
    root = root.strip("._") or "unnamed"
    suffix = f"__{digest}{ext}"
    prefix = "doc_"
    keep = max(8, _MAX_DOC_FILENAME - len(prefix) - len(suffix))
    return f"{prefix}{root[:keep].rstrip('._') or 'unnamed'}{suffix}"


def _safe_doc_filename(name):
    text = str(name)
    safe = _safe(text)
    if _portable_snapshot_name(
            text, safe, _MAX_DOC_FILENAME, reserve_step=True):
        return safe
    return _hashed_doc_filename(text)


def _snapshot_filenames(doc_items):
    """Return portable, case-insensitive collision-free snapshot names."""
    initial = [_safe_doc_filename(name) for name, _content in doc_items]
    counts = {}
    for filename in initial:
        key = filename.casefold()
        counts[key] = counts.get(key, 0) + 1
    result = []
    used = set()
    for index, ((name, _content), filename) in enumerate(
            zip(doc_items, initial), 1):
        candidate = (
            _hashed_doc_filename(name)
            if counts[filename.casefold()] > 1 else filename
        )
        if candidate.casefold() in used:
            candidate = _compact_doc_filename(index, name)
        if candidate.casefold() in used:
            raise RuntimeError("snapshot filename collision could not be resolved")
        used.add(candidate.casefold())
        result.append(candidate)
    return result


def snapshot_docs_sample_dir(out_dir, method, sample_id):
    return os.path.join(
        out_dir,
        "docs",
        _hashed_snapshot_component(method, fallback="method"),
        _hashed_snapshot_component(sample_id, fallback="sample"),
    )


def _compact_snapshot_sample_dir(out_dir, method, sample_id):
    return os.path.join(
        out_dir,
        "docs",
        "_compact",
        f"m_{_snapshot_digest(method)}",
        f"s_{_snapshot_digest(sample_id)}",
    )


def snapshot_docs_sample_dirs(out_dir, method, sample_id):
    current = snapshot_docs_sample_dir(out_dir, method, sample_id)
    compact = _compact_snapshot_sample_dir(out_dir, method, sample_id)
    legacy = os.path.join(out_dir, "docs", _safe(method), _safe(sample_id))
    paths = []
    seen = set()
    for path in (current, compact, legacy):
        key = os.path.normcase(os.path.abspath(path))
        if key not in seen:
            seen.add(key)
            paths.append(path)
    return paths


def _snapshot_step_dir(out_dir, method, sample_id, rt_num, direction, state_id):
    sample_dir = snapshot_docs_sample_dir(out_dir, method, sample_id)
    step = "rt{:02d}_{}_{}".format(
        int(rt_num),
        _hashed_snapshot_component(direction, fallback="direction", max_len=40),
        _hashed_snapshot_component(state_id, fallback="state"),
    )
    return os.path.join(sample_dir, step)


def _compact_snapshot_step_dir(
        out_dir, method, sample_id, rt_num, direction, state_id):
    step = "rt{:02d}_{}_st_{}".format(
        int(rt_num),
        _hashed_snapshot_component(direction, fallback="direction", max_len=16),
        _snapshot_digest(state_id),
    )
    return os.path.join(
        _compact_snapshot_sample_dir(out_dir, method, sample_id), step)


def _compact_doc_filename(index, name):
    safe = _safe(name)
    _root, ext = os.path.splitext(safe)
    if len(ext) > 10:
        ext = ""
    return f"d{int(index):04d}_{_snapshot_digest(name)}{ext}"


def _require_contained_path(root, path):
    root = os.path.realpath(os.path.abspath(root))
    path = os.path.realpath(os.path.abspath(path))
    try:
        inside = os.path.commonpath([root, path]) == root
    except ValueError:
        inside = False
    if not inside:
        raise RuntimeError(f"snapshot path escaped output directory: {path}")
    return path


def _snapshot_path_within_budget(path):
    return len(os.path.abspath(path)) <= _SNAPSHOT_FULL_PATH_BUDGET


def _snapshot_file_plan(out_dir, step_dir, doc_items, filenames, step_info,
                        path_mode):
    docs_root = _require_contained_path(
        os.path.join(out_dir, "docs"), os.path.join(out_dir, "docs"))
    d = _require_contained_path(docs_root, step_dir)
    file_plan = []
    file_map = []
    for (original, content), stored in zip(doc_items, filenames):
        path = _require_contained_path(d, os.path.join(d, stored))
        file_plan.append((path, content))
        file_map.append({
            "original": str(original),
            "stored": stored,
            "relative_path": os.path.relpath(path, out_dir),
        })
    metadata = None
    meta_path = None
    if step_info is not None or file_map:
        metadata = dict(step_info) if isinstance(step_info, dict) else {
            "step_info": step_info,
        }
        metadata["snapshot_path_mode"] = path_mode
        metadata["snapshot_full_path_budget"] = _SNAPSHOT_FULL_PATH_BUDGET
        metadata["snapshot_files"] = file_map
        meta_path = _require_contained_path(d, os.path.join(d, "_step.json"))
    paths = [path for path, _content in file_plan]
    if meta_path is not None:
        paths.append(meta_path)
    return d, file_plan, meta_path, metadata, paths


def _snapshot_plan_within_budget(paths):
    return all(_snapshot_path_within_budget(path) for path in paths)


def dump_step_docs(out_dir, method, sample_id, rt_num, direction, state_id,
                   gen_docs, step_info=None):
    """Best-effort snapshot of the documents a step produced.

    docs/<method>/<sample>/rt<NN>_<dir>_<state>/<filename>   (+ _step.json)
    gen_docs is the editable output {filename: content}. Additive; independent
    of JSONL / replay."""
    try:
        doc_items = list((gen_docs or {}).items())
        filenames = _snapshot_filenames(doc_items)
        plan = _snapshot_file_plan(
            out_dir,
            _snapshot_step_dir(
                out_dir, method, sample_id, rt_num, direction, state_id),
            doc_items, filenames, step_info, "legacy",
        )
        if not _snapshot_plan_within_budget(plan[-1]):
            compact_filenames = [
                _compact_doc_filename(index, fname)
                for index, (fname, _content) in enumerate(doc_items, 1)
            ]
            plan = _snapshot_file_plan(
                out_dir,
                _compact_snapshot_step_dir(
                    out_dir, method, sample_id, rt_num, direction, state_id),
                doc_items, compact_filenames, step_info, "compact",
            )
        if not _snapshot_plan_within_budget(plan[-1]):
            raise RuntimeError(
                "snapshot absolute path budget exhausted "
                f"(limit={_SNAPSHOT_FULL_PATH_BUDGET})")
        d, file_plan, meta_path, metadata, _paths = plan
        os.makedirs(d, exist_ok=True)
        for path, content in file_plan:
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write(content if isinstance(content, str) else str(content))
        if meta_path is not None:
            with open(meta_path, "w", encoding="utf-8", newline="") as handle:
                json.dump(metadata, handle, ensure_ascii=False, indent=2)
        return d
    except Exception as exc:
        warn_best_effort_io("snapshot", out_dir, exc)
        return None
