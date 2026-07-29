"""Runtime helpers for campaign recovery transactions.

This module owns the current generic dispatcher_process_lost recovery
primitives. Historical authorization migrations and CLI compatibility remain in
``authorize_ledger_lock_recovery``.
"""

from datetime import datetime
import hashlib
import json
import os
import subprocess
import time
import uuid

import portalocker

from paired_campaign_dispatch import (
    DEEPSEEK_MODEL,
    DEEPSEEK_FULL234_ROUND_TRIPS,
    DEEPSEEK_TRANSPORT,
    DEEPSEEK_TRANSPORT_REVISION,
    _actual_sample_progress,
    _assert_worker_leases_free,
    _audit_running_invocation_provenance,
    _deepseek_campaign_runtime_identity,
    _valid_deepseek_transport_sidecar,
    _verified_deepseek_infrastructure_incomplete,
    _verified_evaluator_incomplete,
    _write_active_worker_set,
)
from run_meta import (
    CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
    DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
    EMERGENCY_STOP_DIRECTORY,
    METADATA_EVENTS_FILENAME,
    STOP_CONDITION_SCHEMA,
    _canonical_record_sha256,
    _capture_run_metadata_recovery_snapshot,
    _git_identity,
    _read_deepseek_transport_sidecar,
    _sha256_file,
    _validate_deepseek_compact_records,
    _validate_deepseek_linear_records,
    _write_jsonl_atomic,
    append_jsonl_locked,
    campaign_recovery_incident_evidence,
    code_fingerprint,
    interrupt_audited_running_invocations,
    read_campaign_recovery_authorization,
    read_campaign_stop_conditions,
    read_run_metadata_snapshot,
    read_sample_outcomes,
    write_json_atomic,
)

_DISPATCHER_PARENT_LOSS_PENDING_FILENAME = (
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME
)
_DISPATCHER_PARENT_LOSS_PENDING_SCHEMA = (
    "anchorpatch.dispatcher_parent_loss_recovery/1"
)
_DISPATCHER_PARENT_LOSS_HISTORY_ROOT = "r"


def _read_json(path):
    with open(path, encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


def _replace_with_sharing_retry(source, destination):
    """Move one recovery artifact despite transient Windows readers."""
    deadline = time.monotonic() + 60.0
    while True:
        try:
            os.replace(source, destination)
            return
        except OSError as exc:
            if (getattr(exc, "winerror", None) not in {5, 32, 33}
                    or time.monotonic() >= deadline):
                raise
            time.sleep(0.05)


def _copy_file_durable(source, destination, *, allow_missing=False):
    if not os.path.isfile(source) and not allow_missing:
        raise FileNotFoundError(source)
    with open(destination, "xb") as writer:
        if os.path.isfile(source):
            with open(source, "rb") as reader:
                for chunk in iter(
                        lambda: reader.read(1024 * 1024), b""):
                    writer.write(chunk)
        elif not allow_missing:
            raise FileNotFoundError(source)
        writer.flush()
        os.fsync(writer.fileno())


def _ensure_bound_file_copy(source, destination, expected_sha256):
    """Publish one byte-exact archive through a crash-recoverable temp file."""
    if (not isinstance(expected_sha256, str)
            or len(expected_sha256) != 64):
        raise RuntimeError("recovery archive digest is invalid")
    if os.path.isfile(destination):
        if _sha256_file(destination) != expected_sha256:
            raise RuntimeError("recovery archive destination digest mismatch")
        return
    if _sha256_file(source) != expected_sha256:
        raise RuntimeError("recovery archive source digest mismatch")
    temp_path = os.path.join(
        os.path.dirname(destination), ".recovery-copy.pending")
    legacy_temp_path = destination + ".pending-copy"
    _unlink_with_sharing_retry(legacy_temp_path)
    _unlink_with_sharing_retry(temp_path)
    try:
        with open(temp_path, "xb") as writer:
            with open(source, "rb") as reader:
                for chunk in iter(
                        lambda: reader.read(1024 * 1024), b""):
                    writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        if _sha256_file(temp_path) != expected_sha256:
            raise RuntimeError("recovery archive temp digest mismatch")
        _replace_with_sharing_retry(temp_path, destination)
    finally:
        _unlink_with_sharing_retry(temp_path)
        _unlink_with_sharing_retry(legacy_temp_path)


def _assert_dispatcher_parent_loss_path_budget(
        history_dir, emergency_names, *, extra_paths=()):
    """Fail before mutation when a Windows legacy archive path is too long."""
    if os.name != "nt":
        return
    candidates = [
        os.path.join(history_dir, name)
        for name in (
            "active_worker_set.before.json",
            "run_metadata.before.jsonl",
            "superseded_campaign_recovery_authorization.json",
            "campaign_stop.json",
            ".recovery-copy.pending",
        )
    ]
    candidates.extend(
        os.path.join(
            history_dir, EMERGENCY_STOP_DIRECTORY, name)
        for name in emergency_names
    )
    candidates.extend(extra_paths)
    longest = max((len(path) for path in candidates), default=0)
    if longest > 259:
        raise RuntimeError(
            "dispatcher parent-loss archive exceeds Windows legacy path "
            f"budget: {longest}")


def _recoverable_jsonl_tail(handle, prefix_evidence, record):
    if (not isinstance(prefix_evidence, dict)
            or not isinstance(prefix_evidence.get("byte_count"), int)
            or isinstance(prefix_evidence.get("byte_count"), bool)
            or prefix_evidence["byte_count"] <= 0
            or not isinstance(prefix_evidence.get("sha256"), str)
            or len(prefix_evidence["sha256"]) != 64):
        raise RuntimeError("recovery JSONL prefix evidence is invalid")
    expected = (
        json.dumps(record, ensure_ascii=False).encode("utf-8") + b"\n")
    handle.seek(0)
    prefix = handle.read(prefix_evidence["byte_count"])
    if (len(prefix) != prefix_evidence["byte_count"]
            or hashlib.sha256(prefix).hexdigest()
            != prefix_evidence["sha256"]):
        raise RuntimeError("recovery JSONL prefix has drifted")
    tail = handle.read()
    if not expected.startswith(tail):
        raise RuntimeError("recovery JSONL tail conflicts")
    return expected, tail


def _assert_recoverable_jsonl_tail(path, prefix_evidence, record):
    """Verify one pending-bound JSONL event before any recovery mutation."""
    with portalocker.Lock(
            path,
            mode="r+b",
            timeout=60,
            check_interval=0.05,
            flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
    ) as handle:
        _recoverable_jsonl_tail(handle, prefix_evidence, record)


def _append_recoverable_jsonl_tail(path, prefix_evidence, record):
    """Repair only an exact partial copy of one pending-bound JSONL event."""
    with portalocker.Lock(
            path,
            mode="r+b",
            timeout=60,
            check_interval=0.05,
            flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
    ) as handle:
        expected, tail = _recoverable_jsonl_tail(
            handle, prefix_evidence, record)
        if tail == expected:
            return
        handle.seek(prefix_evidence["byte_count"])
        handle.truncate()
        handle.write(expected)
        handle.flush()
        os.fsync(handle.fileno())


def _write_json_no_replace(path, record):
    """Publish one complete JSON object without replacing a concurrent stop."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
    temp_path = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    try:
        with open(temp_path, "xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path)
        except FileExistsError:
            raise RuntimeError(
                "campaign stop evidence appeared during parent-loss recovery")
        except OSError:
            try:
                descriptor = os.open(
                    path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                )
            except FileExistsError:
                raise RuntimeError(
                    "campaign stop evidence appeared during parent-loss "
                    "recovery")
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def _read_jsonl(path):
    rows = []
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError as exc:
                raise RuntimeError(f"invalid JSONL at {path}:{number}") from exc
            if not isinstance(row, dict):
                raise RuntimeError(f"non-object JSONL at {path}:{number}")
            rows.append(row)
    return rows


def _file_prefix_evidence(path):
    size = os.path.getsize(path)
    if size <= 0:
        raise RuntimeError(f"recovery evidence file is empty: {path}")
    with open(path, "rb") as handle:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) != b"\n":
            raise RuntimeError(
                f"recovery evidence file lacks a terminal newline: {path}")
    return {
        "byte_count": size,
        "sha256": _sha256_file(path),
    }


def _git_changed_paths(prior_commit, recovery_commit):
    result = subprocess.run(
        [
            "git", "diff", "--name-only", prior_commit, recovery_commit,
            "--",
        ],
        check=True, capture_output=True, text=True, encoding="utf-8",
    )
    return sorted(
        item.replace("\\", "/")
        for item in result.stdout.splitlines() if item
    )


def _incident_entry(number, row, incident_kind):
    return {
        "row_number": number,
        "canonical_sha256": _canonical_record_sha256(row),
        "incident_kind": incident_kind,
        "sample": row.get("sample"),
        "worker_launch_id": row.get("worker_launch_id"),
        "semantic_root_id": row.get("semantic_root_id"),
        "event": row.get("event"),
        "call_kind": row.get("call_kind"),
        "provider_called": row.get("provider_called"),
    }


def _latest_outcomes_by_worker(out_dir):
    latest = {}
    for row in read_sample_outcomes(out_dir):
        worker_id = row.get("worker_launch_id")
        if isinstance(worker_id, str) and worker_id:
            if worker_id in latest:
                raise RuntimeError(
                    f"duplicate sample outcome for worker {worker_id}")
            latest[worker_id] = row
    return latest

def _reconcile_deepseek_parent_loss_workers(
        out_dir, manifest, stop_records, *, apply=True,
        allow_stopless_registered_prelaunch=False,
        stop_condition="dispatcher_process_lost"):
    """Close exactly the worker cohort orphaned by one dispatcher instance."""
    config = manifest.get("config") or {}
    if (config.get("campaign_role") != "deepseek_full234"
            or config.get("model") != DEEPSEEK_MODEL
            or config.get("num_round_trips")
            != DEEPSEEK_FULL234_ROUND_TRIPS
            or config.get("transport") != DEEPSEEK_TRANSPORT
            or config.get("transport_revision")
            != DEEPSEEK_TRANSPORT_REVISION
            or _deepseek_campaign_runtime_identity(out_dir)
            != (DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION)):
        raise RuntimeError(
            "dispatcher parent-loss recovery requires current DeepSeek full234")
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

    active_path = os.path.join(out_dir, "active_worker_set.json")
    active = _read_json(active_path)
    active_workers = active.get("workers")
    dispatcher_pid = active.get("dispatcher_pid")
    dispatcher_instance_id = active.get("dispatcher_instance_id")
    if (active.get("schema") != "anchorpatch.active_worker_set/1"
            or active.get("run_git_commit") != manifest.get("run_git_commit")
            or not isinstance(active_workers, dict) or not active_workers
            or not isinstance(dispatcher_pid, int)
            or isinstance(dispatcher_pid, bool) or dispatcher_pid <= 0
            or not isinstance(dispatcher_instance_id, str)
            or not dispatcher_instance_id):
        raise RuntimeError(
            "dispatcher parent-loss active cohort identity is invalid")
    samples_by_worker = {
        worker_id: item.get("sample")
        for worker_id, item in active_workers.items()
        if isinstance(item, dict)
    }
    if (len(samples_by_worker) != len(active_workers)
            or any(
                not isinstance(worker_id, str) or not worker_id
                or not isinstance(sample, str) or not sample
                for worker_id, sample in samples_by_worker.items()
            )
            or len(set(samples_by_worker.values()))
            != len(samples_by_worker)):
        raise RuntimeError(
            "dispatcher parent-loss active worker identities are invalid")

    if not stop_records and not allow_stopless_registered_prelaunch:
        raise RuntimeError("dispatcher parent-loss stop evidence is missing")
    for record in stop_records:
        worker_id = record.get("worker_launch_id")
        dispatcher_level_stop = (
            record.get("worker_launch_id") is None
            and (
                record.get("worker_pid") is None
                or (
                    stop_condition
                    == "operator_directed_dispatcher_pause"
                    and record.get("worker_pid") == dispatcher_pid
                )
            )
            and record.get("active_worker_launch_ids") == sorted(
                active_workers)
        )
        registered_prelaunch_only = (
            record.get("registered_prelaunch_only") is True
            and record.get("worker_launch_id") is None
            and record.get("worker_pid") is None
            and record.get("registered_worker_launch_ids")
            == sorted(active_workers)
        )
        ordinary_worker_stop = (
            record.get("registered_prelaunch_only") is not True
            and worker_id in active_workers
        )
        if (record.get("condition") != stop_condition
                or record.get("dispatcher_pid") != dispatcher_pid
                or record.get("dispatcher_instance_id")
                != dispatcher_instance_id
                or not (
                    dispatcher_level_stop
                    or registered_prelaunch_only
                    or ordinary_worker_stop)):
            raise RuntimeError(
                "dispatcher parent-loss stop cohort identity mismatch")

    samples = sorted(samples_by_worker.values())
    if any(sample not in methods_by_sample for sample in samples):
        raise RuntimeError(
            "dispatcher parent-loss active sample assignment is missing")
    _assert_worker_leases_free(out_dir, samples)
    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    dispatch_rows = _read_jsonl(dispatch_path)
    intents = {}
    launches = {}
    authorizations = {}
    exits = {}
    for row in dispatch_rows:
        worker_id = row.get("worker_launch_id")
        if worker_id not in active_workers:
            continue
        event = row.get("event")
        target = {
            "launch_intent": intents,
            "launch": launches,
            "worker_authorized": authorizations,
            "worker_exit": exits,
        }.get(event)
        if target is not None:
            if worker_id in target:
                raise RuntimeError(
                    f"dispatcher parent-loss {event} is duplicated")
            target[worker_id] = row
    metadata_by_worker = {}
    metadata_records = read_run_metadata_snapshot(out_dir)
    for row in metadata_records:
        worker_id = row.get("worker_launch_id")
        if worker_id in active_workers:
            if worker_id in metadata_by_worker:
                raise RuntimeError(
                    "dispatcher parent-loss metadata is duplicated")
            metadata_by_worker[worker_id] = row
    outcomes = _latest_outcomes_by_worker(out_dir)
    api_path = os.path.join(out_dir, "api_calls.jsonl")
    api_rows = _read_jsonl(api_path) if os.path.isfile(api_path) else []
    api_workers = {
        row.get("worker_launch_id") for row in api_rows
        if row.get("worker_launch_id") in active_workers
    }
    attempt_path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
    attempt_rows = (
        _read_jsonl(attempt_path) if os.path.isfile(attempt_path) else [])
    attempt_workers = {
        row.get("worker_launch_id") for row in attempt_rows
        if row.get("worker_launch_id") in active_workers
    }
    audited_running = _audit_running_invocation_provenance(out_dir)
    audited_by_worker = {
        item["worker_launch_id"]: item for item in audited_running
    }
    if set(audited_by_worker) != {
            worker_id for worker_id, row in metadata_by_worker.items()
            if row.get("status") == "running"}:
        raise RuntimeError(
            "dispatcher parent-loss running invocation scope is invalid")

    running_statuses = {}
    pending_launches = []
    pending_exits = []
    reconciled = []
    interrupted = []
    preauthorization = []
    registered_prelaunch = []
    for worker_id, sample in samples_by_worker.items():
        intent = intents.get(worker_id)
        launch = launches.get(worker_id)
        metadata = metadata_by_worker.get(worker_id)
        assigned_methods = methods_by_sample[sample]
        worker_stops = [
            record for record in stop_records
            if record.get("worker_launch_id") == worker_id
        ]
        if intent is not None and (
                intent.get("sample") != sample
                or intent.get("methods") != assigned_methods
                or intent.get("dispatcher_pid") != dispatcher_pid
                or intent.get("dispatcher_instance_id")
                != dispatcher_instance_id):
            raise RuntimeError(
                f"dispatcher parent-loss launch intent is invalid: {sample}")
        if launch is None and (metadata is not None or worker_stops):
            if intent is None:
                raise RuntimeError(
                    "dispatcher parent-loss spawned worker has no launch intent: "
                    f"{sample}")
            synthetic_pid = (
                metadata.get("worker_pid")
                if isinstance(metadata, dict)
                else worker_stops[0].get("worker_pid")
            )
            if (not isinstance(synthetic_pid, int)
                    or isinstance(synthetic_pid, bool)
                    or synthetic_pid <= 0
                    or any(
                        record.get("worker_pid") != synthetic_pid
                        for record in worker_stops
                    )):
                raise RuntimeError(
                    "dispatcher parent-loss unrecorded launch PID is invalid: "
                    f"{sample}")
            launch = {
                "event": "launch",
                "created_at": datetime.now().astimezone().isoformat(
                    timespec="seconds"),
                "sample": sample,
                "key_label": intent.get("key_label"),
                "methods": intent.get("methods"),
                "pid": synthetic_pid,
                "worker_launch_id": worker_id,
                "console_log": intent.get("console_log"),
                "method_phase": intent.get("method_phase"),
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "launch_observed": False,
                "reconciled_after_parent_loss": True,
            }
            pending_launches.append(launch)
            launches[worker_id] = launch
        if launch is None:
            if (metadata is not None
                    or worker_id in authorizations
                    or worker_id in api_workers
                    or worker_id in attempt_workers
                    or worker_id in outcomes
                    or worker_id in exits
                    or worker_stops):
                raise RuntimeError(
                    "dispatcher parent-loss registered worker has execution "
                    f"evidence: {sample}")
            item = {
                "sample": sample,
                "worker_launch_id": worker_id,
                "worker_pid": None,
                "invocation_id": None,
                "status": "registered_prelaunch",
            }
            reconciled.append(item)
            registered_prelaunch.append(item)
            continue
        if (intent is None
                or launch.get("sample") != sample
                or launch.get("methods") != assigned_methods
                or launch.get("dispatcher_pid") != dispatcher_pid
                or launch.get("dispatcher_instance_id")
                != dispatcher_instance_id
                or not isinstance(launch.get("pid"), int)
                or isinstance(launch.get("pid"), bool)
                or launch.get("pid") <= 0
                ):
            raise RuntimeError(
                f"dispatcher parent-loss worker launch is invalid: {sample}")
        for record in worker_stops:
            if record.get("worker_pid") != launch.get("pid"):
                raise RuntimeError(
                    "dispatcher parent-loss stop worker PID mismatch")
        if metadata is None:
            if (worker_id in authorizations
                    or worker_id in api_workers
                    or worker_id in attempt_workers
                    or worker_id in outcomes):
                raise RuntimeError(
                    "dispatcher parent-loss preauthorization worker has "
                    f"execution evidence: {sample}")
            returncode = 97
            disposition = "campaign_fatal"
            evidence = {
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "exit_code_observed": False,
                "exit_code_source": "dispatcher_parent_watchdog_contract",
                "lifecycle_stage": "preauthorization",
            }
            existing_exit = exits.get(worker_id)
            expected_exit = {
                "sample": sample,
                "pid": launch["pid"],
                "returncode": returncode,
                "disposition": disposition,
            }
            if existing_exit is not None:
                if any(
                        existing_exit.get(key) != value
                        for key, value in expected_exit.items()):
                    raise RuntimeError(
                        f"dispatcher parent-loss exit drift: {sample}")
            else:
                pending_exits.append({
                    "event": "worker_exit",
                    "created_at": datetime.now().astimezone().isoformat(
                        timespec="seconds"),
                    "worker_launch_id": worker_id,
                    "key_label": launch.get("key_label"),
                    **expected_exit,
                    "evidence": evidence,
                    "exit_code_observed": False,
                })
            item = {
                "sample": sample,
                "worker_launch_id": worker_id,
                "worker_pid": launch["pid"],
                "invocation_id": None,
                "status": "preauthorization",
            }
            reconciled.append(item)
            preauthorization.append(item)
            continue
        if (not isinstance(metadata, dict)
                or metadata.get("samples") != [sample]
                or metadata.get("worker_pid") != launch.get("pid")
                or metadata.get("dispatcher_pid") != dispatcher_pid
                or metadata.get("dispatcher_instance_id")
                != dispatcher_instance_id
                or metadata.get("methods") != assigned_methods):
            raise RuntimeError(
                f"dispatcher parent-loss worker provenance is invalid: "
                f"{sample}")
        authorization = authorizations.get(worker_id)
        if authorization is not None:
            if (authorization.get("sample") != sample
                    or authorization.get("worker_pid") != launch.get("pid")
                    or authorization.get("invocation_id")
                    != metadata.get("invocation_id")
                    or authorization.get("dispatcher_pid")
                    != dispatcher_pid
                    or authorization.get("dispatcher_instance_id")
                    != dispatcher_instance_id):
                raise RuntimeError(
                    "dispatcher parent-loss worker authorization mismatch")
        elif worker_id in api_workers or worker_id in attempt_workers:
            raise RuntimeError(
                "dispatcher parent-loss unauthorized worker has provider "
                f"evidence: {sample}")

        metadata_status = metadata.get("status")
        outcome = outcomes.get(worker_id)
        terminal_status = metadata_status
        if metadata_status == "running":
            terminal_status = (
                outcome.get("status")
                if isinstance(outcome, dict)
                else "interrupted_by_dispatcher"
            )
            running_statuses[metadata["invocation_id"]] = terminal_status
        if terminal_status not in {
                "finished", "infrastructure_incomplete",
                "evaluator_incomplete", "interrupted_by_dispatcher"}:
            raise RuntimeError(
                f"dispatcher parent-loss worker status is invalid: {sample}")
        if terminal_status == "interrupted_by_dispatcher":
            if outcome is not None:
                raise RuntimeError(
                    "dispatcher-interrupted worker has a terminal outcome")
            returncode = 97
            disposition = "campaign_fatal"
            evidence = {
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "exit_code_observed": False,
                "exit_code_source": "dispatcher_parent_watchdog_contract",
            }
            interrupted.append({
                "sample": sample,
                "worker_launch_id": worker_id,
                "worker_pid": launch["pid"],
                "invocation_id": metadata["invocation_id"],
                "status": terminal_status,
            })
        else:
            if (not isinstance(outcome, dict)
                    or outcome.get("status") != terminal_status
                    or outcome.get("sample") != sample
                    or outcome.get("worker_launch_id") != worker_id
                    or outcome.get("worker_pid") != launch.get("pid")
                    or outcome.get("invocation_id")
                    != metadata.get("invocation_id")):
                raise RuntimeError(
                    f"dispatcher parent-loss terminal outcome mismatch: "
                    f"{sample}")
            item = {
                "worker_launch_id": worker_id,
                "worker_pid": launch["pid"],
                "methods": metadata["methods"],
                "target_round_trips": (
                    config["num_round_trips"]),
                "method_phase": None,
            }
            allow_running = metadata_status == "running"
            if terminal_status == "finished":
                expected_progress = {
                    method: {
                        "completed_round_trips": config[
                            "num_round_trips"],
                        "committed_rows": 2 * config[
                            "num_round_trips"],
                    }
                    for method in metadata["methods"]
                }
                if (_actual_sample_progress(
                        out_dir, sample, metadata["methods"])
                        != expected_progress
                        or outcome.get("checkpoint_progress")
                        != expected_progress):
                    raise RuntimeError(
                        f"dispatcher parent-loss finished evidence is "
                        f"incomplete: {sample}")
                returncode = 0
                disposition = "finished"
                evidence = {
                    "terminal_metadata_status": terminal_status,
                    "exit_code_observed": False,
                }
            elif terminal_status == "infrastructure_incomplete":
                evidence = _verified_deepseek_infrastructure_incomplete(
                    out_dir, sample, item, outcome,
                    allow_active_running_metadata=allow_running,
                )
                evidence["exit_code_observed"] = False
                returncode = 1
                disposition = terminal_status
            else:
                evidence = _verified_evaluator_incomplete(
                    out_dir, sample, item,
                    allow_campaign_stop=True,
                    allow_active_running_metadata=allow_running,
                )
                evidence["exit_code_observed"] = False
                returncode = 1
                disposition = terminal_status

        expected_exit = {
            "sample": sample,
            "pid": launch["pid"],
            "returncode": returncode,
            "disposition": disposition,
        }
        existing_exit = exits.get(worker_id)
        if existing_exit is not None:
            if any(
                    existing_exit.get(key) != value
                    for key, value in expected_exit.items()):
                raise RuntimeError(
                    f"dispatcher parent-loss exit drift: {sample}")
        else:
            pending_exits.append({
                "event": "worker_exit",
                "created_at": datetime.now().astimezone().isoformat(
                    timespec="seconds"),
                "worker_launch_id": worker_id,
                "key_label": launch.get("key_label"),
                **expected_exit,
                "evidence": evidence,
                "exit_code_observed": False,
            })
        reconciled.append({
            "sample": sample,
            "worker_launch_id": worker_id,
            "worker_pid": launch["pid"],
            "invocation_id": metadata["invocation_id"],
            "status": terminal_status,
        })

    pending_reconciliations = [
        {
            "event": "stale_worker_reconciled",
            "created_at": datetime.now().astimezone().isoformat(
                timespec="seconds"),
            "worker_launch_id": item["worker_launch_id"],
            "pid": item["worker_pid"],
            "sample": item["sample"],
            "invocation_id": item["invocation_id"],
            "exit_code_observed": False,
            "reason": stop_condition,
        }
        for item in interrupted
    ]
    recovery_plan = {
        "dispatcher_pid": dispatcher_pid,
        "dispatcher_instance_id": dispatcher_instance_id,
        "workers": sorted(
            reconciled, key=lambda item: item["sample"]),
        "interrupted_workers": sorted(
            interrupted, key=lambda item: item["sample"]),
        "preauthorization_workers": sorted(
            preauthorization, key=lambda item: item["sample"]),
        "registered_prelaunch_workers": sorted(
            registered_prelaunch, key=lambda item: item["sample"]),
        "running_statuses": dict(running_statuses),
        "audited_running": sorted(
            audited_by_worker.values(),
            key=lambda item: item["sample"]),
        "pending_exit_rows": list(pending_exits),
        "pending_launch_rows": list(pending_launches),
        "pending_reconciliation_rows": pending_reconciliations,
    }
    if not stop_records and allow_stopless_registered_prelaunch:
        if (len(registered_prelaunch) != len(active_workers)
                or interrupted or preauthorization
                or running_statuses or audited_by_worker
                or pending_launches or pending_exits
                or pending_reconciliations):
            raise RuntimeError(
                "stopless dispatcher parent-loss recovery is limited to "
                "workers registered before process launch")
    if not apply:
        return recovery_plan
    _apply_deepseek_parent_loss_recovery_plan(
        out_dir, manifest, recovery_plan)
    return recovery_plan


def _record_stopless_registered_prelaunch_parent_loss(
        out_dir, manifest, stop_path):
    """Create evidence for the only parent-loss window with no worker writer."""
    recovery_scope = _reconcile_deepseek_parent_loss_workers(
        out_dir,
        manifest,
        [],
        apply=False,
        allow_stopless_registered_prelaunch=True,
    )
    if read_campaign_stop_conditions(out_dir):
        raise RuntimeError(
            "campaign stop evidence appeared during parent-loss recovery")
    worker_ids = sorted(
        item["worker_launch_id"]
        for item in recovery_scope["registered_prelaunch_workers"]
    )
    record = {
        "schema": STOP_CONDITION_SCHEMA,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "condition": "dispatcher_process_lost",
        "worker_launch_id": None,
        "worker_pid": None,
        "dispatcher_pid": recovery_scope["dispatcher_pid"],
        "dispatcher_instance_id": recovery_scope[
            "dispatcher_instance_id"],
        "registered_prelaunch_only": True,
        "registered_worker_launch_ids": worker_ids,
        "evidence_basis": (
            "dispatcher_lease_free_worker_leases_free_and_no_execution_evidence"
        ),
    }
    _write_json_no_replace(stop_path, record)
    observed = read_campaign_stop_conditions(out_dir)
    if (len(observed) != 1
            or _canonical_record_sha256(observed[0])
            != _canonical_record_sha256(record)):
        raise RuntimeError(
            "registered-prelaunch parent-loss stop publication failed")
    return record


def _apply_deepseek_parent_loss_recovery_plan(
        out_dir, manifest, recovery_plan):
    """Idempotently apply one already validated parent-loss recovery plan."""
    workers = recovery_plan.get("workers")
    if not isinstance(workers, list) or not workers:
        raise RuntimeError("dispatcher parent-loss recovery plan is invalid")
    worker_ids = {
        item.get("worker_launch_id") for item in workers
        if isinstance(item, dict)
    }
    if (len(worker_ids) != len(workers)
            or any(not isinstance(item, str) or not item
                   for item in worker_ids)):
        raise RuntimeError("dispatcher parent-loss recovery plan is invalid")

    statuses = recovery_plan.get("running_statuses") or {}
    audits = recovery_plan.get("audited_running") or []
    audits_by_invocation = {
        item.get("invocation_id"): item
        for item in audits if isinstance(item, dict)
    }
    if (len(audits_by_invocation) != len(audits)
            or set(statuses) != set(audits_by_invocation)):
        raise RuntimeError(
            "dispatcher parent-loss metadata plan is invalid")
    metadata = read_run_metadata_snapshot(out_dir)
    unexpected_running = {
        row.get("invocation_id") for row in metadata
        if row.get("status") == "running"
        and row.get("invocation_id") not in audits_by_invocation
    }
    if unexpected_running:
        raise RuntimeError(
            "dispatcher parent-loss running metadata scope has drifted")
    metadata_by_invocation = {}
    for row in metadata:
        invocation_id = row.get("invocation_id")
        if invocation_id not in audits_by_invocation:
            continue
        if invocation_id in metadata_by_invocation:
            raise RuntimeError(
                "dispatcher parent-loss metadata plan is duplicated")
        metadata_by_invocation[invocation_id] = row
    if len(metadata_by_invocation) != len(audits_by_invocation):
        raise RuntimeError(
            "dispatcher parent-loss metadata plan has drifted")
    remaining_audits = []
    remaining_statuses = {}
    for invocation_id, audit in audits_by_invocation.items():
        row = metadata_by_invocation[invocation_id]
        expected_status = statuses[invocation_id]
        if (row.get("worker_launch_id") != audit.get("worker_launch_id")
                or row.get("worker_pid") != audit.get("worker_pid")
                or row.get("samples") != [audit.get("sample")]):
            raise RuntimeError(
                "dispatcher parent-loss metadata identity has drifted")
        if row.get("status") == "running":
            remaining_audits.append(audit)
            remaining_statuses[invocation_id] = expected_status
        elif row.get("status") != expected_status:
            raise RuntimeError(
                "dispatcher parent-loss metadata status has drifted")
    if remaining_audits:
        closed = interrupt_audited_running_invocations(
            out_dir, statuses=remaining_statuses,
            audited=remaining_audits)
        if {
                item.get("worker_launch_id") for item in closed
        } != {
                item.get("worker_launch_id") for item in remaining_audits
        }:
            raise RuntimeError(
                "dispatcher parent-loss metadata closure is incomplete")

    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    for row in (
            list(recovery_plan.get("pending_launch_rows") or [])
            + list(recovery_plan.get("pending_exit_rows") or [])
            + list(recovery_plan.get("pending_reconciliation_rows") or [])):
        if (not isinstance(row, dict)
                or row.get("event") not in {
                    "launch", "worker_exit", "stale_worker_reconciled"}
                or row.get("worker_launch_id") not in worker_ids):
            raise RuntimeError(
                "dispatcher parent-loss dispatch plan is invalid")
        matches = [
            existing for existing in _read_jsonl(dispatch_path)
            if existing.get("event") == row.get("event")
            and existing.get("worker_launch_id")
            == row.get("worker_launch_id")
        ]
        if not matches:
            append_jsonl_locked(dispatch_path, row)
        elif (len(matches) != 1
              or _canonical_record_sha256(matches[0])
              != _canonical_record_sha256(row)):
            raise RuntimeError(
                "dispatcher parent-loss dispatch plan has drifted")

    active_path = os.path.join(out_dir, "active_worker_set.json")
    active = _read_json(active_path)
    expected_active_workers = {
        item["worker_launch_id"]: {"sample": item["sample"]}
        for item in workers
    }
    if active.get("workers") == {}:
        return
    if (active.get("schema") != "anchorpatch.active_worker_set/1"
            or active.get("run_git_commit") != manifest.get("run_git_commit")
            or active.get("dispatcher_pid")
            != recovery_plan.get("dispatcher_pid")
            or active.get("dispatcher_instance_id")
            != recovery_plan.get("dispatcher_instance_id")
            or active.get("workers") != expected_active_workers):
        raise RuntimeError(
            "dispatcher parent-loss active worker plan has drifted")
    _write_active_worker_set(out_dir, manifest, [])


def _is_operator_pause_pre_provider_api_row(row):
    """Recognize the exact guard-stop row that proves no provider POST began."""
    return (
        row.get("provider_called") is False
        and row.get("response_replayed") is False
        and row.get("classification") == "runner_exception"
        and row.get("error_type") == "CampaignStoppedError"
        and row.get("runner_exception") == (
            "CampaignStoppedError: campaign stop latch is set: "
            "operator_directed_dispatcher_pause")
        and row.get("provider_request_id") is None
        and row.get("http_status") is None
        and row.get("stream_complete") is False
        and row.get("transport_attempts") == []
        and row.get("http_attempts_used") is None
        and row.get("raw_response_saved_path") is None
        and row.get("raw_sse_saved_path") is None
        and row.get("raw_content_length") == 0
        and row.get("content_sha256")
        == hashlib.sha256(b"").hexdigest()
        and row.get("transport_sidecar_sha256") is None
        and row.get("transport_sidecar_size_bytes") is None
        and row.get("transport_sidecar_record_count") is None
        and row.get("count_as_method_failure") is True
    )


def _deepseek_parent_loss_incidents(out_dir, manifest, recovery_scope):
    """Hash-bind calls that may be replayed after a parent-loss interruption."""
    interrupted = recovery_scope["interrupted_workers"]
    interrupted_by_id = {
        item["worker_launch_id"]: item for item in interrupted
    }
    workers_by_id = {
        item["worker_launch_id"]: item
        for item in recovery_scope["workers"]
    }
    config = manifest.get("config") or {}
    methods = set(config.get("method_set") or [])
    target_rt = config.get("num_round_trips")
    committed_call_ids = set()
    for sample in config.get("samples") or []:
        for method in methods:
            path = os.path.join(out_dir, method, f"{sample}.jsonl")
            if not os.path.isfile(path):
                continue
            for row in _read_jsonl(path):
                committed_call_ids.update(row.get("api_call_ids") or [])

    api_path = os.path.join(out_dir, "api_calls.jsonl")
    api_rows = _read_jsonl(api_path) if os.path.isfile(api_path) else []
    api_incidents = []
    pre_provider_api_rows = []
    for number, row in enumerate(api_rows, 1):
        worker_id = row.get("worker_launch_id")
        if worker_id not in interrupted_by_id:
            continue
        request_id = row.get("request_id")
        worker = interrupted_by_id[worker_id]
        if request_id in committed_call_ids:
            continue
        if (row.get("sample") != worker["sample"]
                or row.get("worker_pid") != worker["worker_pid"]
                or row.get("model") != DEEPSEEK_MODEL
                or row.get("method") not in methods
                or not isinstance(row.get("rt_index"), int)
                or isinstance(row.get("rt_index"), bool)
                or not 1 <= row.get("rt_index") <= target_rt
                or row.get("transport") != DEEPSEEK_TRANSPORT
                or row.get("transport_revision")
                != config.get("transport_revision")
                or row.get("response_replayed") is not False):
            raise RuntimeError(
                "dispatcher parent-loss uncommitted API evidence is invalid")
        if _is_operator_pause_pre_provider_api_row(row):
            pre_provider_api_rows.append(_incident_entry(
                number, row, "operator_pause_pre_provider_api"))
            continue
        if row.get("provider_called") is not True:
            raise RuntimeError(
                "dispatcher parent-loss uncommitted API evidence is invalid")
        if (row.get("classification") is not None
                or row.get("response_classification") != "normal"
                or row.get("http_status") != 200
                or row.get("stream_complete") is not True
                or not _valid_deepseek_transport_sidecar(out_dir, row)):
            raise RuntimeError(
                "dispatcher parent-loss uncommitted API response is not a "
                "complete successful stream")
        sidecar_path = os.path.realpath(row.get("raw_sse_saved_path"))
        entry = _incident_entry(
            number, row,
            "dispatcher_parent_loss_uncommitted_api")
        entry["transport_sidecar_path"] = os.path.relpath(
            sidecar_path, out_dir).replace("\\", "/")
        entry["transport_sidecar_sha256"] = _sha256_file(sidecar_path)
        api_incidents.append(entry)

    attempt_path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
    attempt_rows = (
        _read_jsonl(attempt_path) if os.path.isfile(attempt_path) else []
    )
    groups = {}
    for number, row in enumerate(attempt_rows, 1):
        worker_id = row.get("worker_launch_id")
        semantic_call_id = row.get("semantic_call_id")
        if (worker_id in interrupted_by_id
                and isinstance(semantic_call_id, str)
                and semantic_call_id):
            groups.setdefault(
                (worker_id, semantic_call_id), []).append((number, row))
    attempt_incidents = []
    for (worker_id, semantic_call_id), group in groups.items():
        events = [row.get("event") for _number, row in group]
        if "response_committed" in events:
            continue
        starts = events.count("attempt_start")
        ends = events.count("attempt_end")
        pre_provider = (
            starts == 0 and ends == 0
            and events.count("semantic_request") == 1
        )
        if not (starts > ends or pre_provider):
            continue
        digest = hashlib.sha256(
            semantic_call_id.encode("utf-8")).hexdigest()[:24]
        if os.path.exists(os.path.join(
                out_dir, "api_journal", f"{digest}.response.json")):
            raise RuntimeError(
                "dispatcher parent-loss open attempt has a response journal")
        worker = interrupted_by_id[worker_id]
        if any(
                row.get("worker_launch_id") != worker_id
                or row.get("sample") not in {None, worker["sample"]}
                for _number, row in group):
            raise RuntimeError(
                "dispatcher parent-loss attempt scope is invalid")
        attempt_incidents.extend(
            _incident_entry(
                number, row, "dispatcher_parent_loss_open_attempt")
            for number, row in group
        )

    mapped_sidecars = {
        os.path.realpath(row.get("raw_sse_saved_path"))
        for row in api_rows
        if isinstance(row.get("raw_sse_saved_path"), str)
        and row.get("raw_sse_saved_path")
    }
    sidecar_incidents = []
    raw_root = os.path.join(out_dir, "api_raw")
    if os.path.isdir(raw_root):
        for root, _dirs, files in os.walk(raw_root):
            for name in sorted(files):
                if not name.endswith(".transport.jsonl"):
                    continue
                path = os.path.realpath(os.path.join(root, name))
                if path in mapped_sidecars:
                    continue
                sidecar = _read_deepseek_transport_sidecar(path)
                rows = sidecar["rows"]
                if not rows:
                    continue
                identity_rows = sidecar["identity_rows"]
                worker_ids = {
                    row.get("worker_launch_id") for row in identity_rows
                }
                relevant = worker_ids & set(workers_by_id)
                if not relevant:
                    continue
                if len(relevant) != 1 or worker_ids != relevant:
                    raise RuntimeError(
                        "dispatcher parent-loss transport sidecar worker "
                        "identity is inconsistent")
                worker_id = next(iter(relevant))
                worker = workers_by_id[worker_id]
                if worker_id not in interrupted_by_id:
                    raise RuntimeError(
                        "dispatcher parent-loss preauthorization worker has "
                        "transport evidence")
                methods_seen = {row.get("method") for row in identity_rows}
                samples_seen = {row.get("sample") for row in identity_rows}
                pids_seen = {row.get("worker_pid") for row in identity_rows}
                call_ids = {row.get("call_id") for row in identity_rows}
                rt_indexes = {row.get("rt_index") for row in identity_rows}
                directions = {row.get("direction") for row in identity_rows}
                if (samples_seen != {worker["sample"]}
                        or pids_seen != {worker["worker_pid"]}
                        or len(call_ids) != 1
                        or None in call_ids
                        or not methods_seen <= methods
                        or len(methods_seen) != 1
                        or len(rt_indexes) != 1
                        or not isinstance(next(iter(rt_indexes)), int)
                        or isinstance(next(iter(rt_indexes)), bool)
                        or not 1 <= next(iter(rt_indexes)) <= target_rt
                        or len(directions) != 1
                        or not directions <= {"forward", "backward"}):
                    raise RuntimeError(
                        "dispatcher parent-loss transport sidecar scope "
                        "is invalid")
                expected_linkage = {
                    "worker_launch_id": worker_id,
                    "worker_pid": worker["worker_pid"],
                    "sample": worker["sample"],
                    "method": next(iter(methods_seen)),
                    "rt_index": next(iter(rt_indexes)),
                    "direction": next(iter(directions)),
                    "call_id": next(iter(call_ids)),
                }
                try:
                    if (config.get("transport_revision")
                            == DEEPSEEK_TRANSPORT_REVISION):
                        if not sidecar["compact"]:
                            raise RuntimeError(
                                "compact transport schema was downgraded")
                        parsed = _validate_deepseek_compact_records(
                            rows,
                            expected_linkage=expected_linkage,
                            allow_open_final=True,
                        )
                    elif config.get("transport_revision") in {
                            "opencode_openai_compatible/4",
                            "opencode_openai_compatible/5",
                    }:
                        if sidecar["compact"]:
                            raise RuntimeError(
                                "linear transport schema was upgraded")
                        parsed = _validate_deepseek_linear_records(
                            rows,
                            expected_linkage=expected_linkage,
                            allow_open_final=True,
                        )
                    else:
                        raise RuntimeError(
                            "unsupported DeepSeek transport revision")
                except RuntimeError as exc:
                    raise RuntimeError(
                        "dispatcher parent-loss transport sidecar sequence "
                        "is invalid") from exc
                sidecar_incidents.append({
                    "path": os.path.relpath(
                        path, out_dir).replace("\\", "/"),
                    "sha256": _sha256_file(path),
                    "worker_launch_id": worker_id,
                    "worker_pid": worker["worker_pid"],
                    "sample": worker["sample"],
                    "method": next(iter(methods_seen)),
                    "rt_index": next(iter(rt_indexes)),
                    "direction": next(iter(directions)),
                    "call_id": next(iter(call_ids)),
                    "event_count": len(rows),
                    "attempt_start_count": parsed["attempt_start_count"],
                    "attempt_end_count": parsed["attempt_end_count"],
                    "state": parsed["recovery_state"],
                })
    return {
        "api": api_incidents,
        "pre_provider_api": pre_provider_api_rows,
        "attempts": attempt_incidents,
        "transport_sidecars": sorted(
            sidecar_incidents, key=lambda item: item["path"]),
    }


def _merge_incident_entries(*groups):
    merged = {}
    for entry in (
            item for group in groups for item in (group or [])):
        if not isinstance(entry, dict):
            raise RuntimeError("recovery incident entry is invalid")
        key = (entry.get("row_number"), entry.get("canonical_sha256"))
        if (not isinstance(key[0], int) or isinstance(key[0], bool)
                or key[0] < 1 or not isinstance(key[1], str)
                or not key[1]):
            raise RuntimeError("recovery incident entry is invalid")
        prior = merged.get(key)
        if prior is not None and prior != entry:
            raise RuntimeError("recovery incident entry conflicts")
        merged[key] = dict(entry)
    return [merged[key] for key in sorted(merged)]


def _merge_transport_sidecar_entries(*groups):
    merged = {}
    for entry in (
            item for group in groups for item in (group or [])):
        if not isinstance(entry, dict):
            raise RuntimeError(
                "recovery transport sidecar entry is invalid")
        key = (entry.get("path"), entry.get("sha256"))
        if (not isinstance(key[0], str) or not key[0]
                or not isinstance(key[1], str) or not key[1]):
            raise RuntimeError(
                "recovery transport sidecar entry is invalid")
        prior = merged.get(key)
        if prior is not None and prior != entry:
            raise RuntimeError(
                "recovery transport sidecar entry conflicts")
        merged[key] = dict(entry)
    return [merged[key] for key in sorted(merged)]


def _unlink_with_sharing_retry(path):
    deadline = time.monotonic() + 60.0
    while True:
        try:
            os.unlink(path)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            if (getattr(exc, "winerror", None) not in {5, 32, 33}
                    or time.monotonic() >= deadline):
                raise
            time.sleep(0.05)
