"""Authorize one append-only recovery from shared JSONL lock contention.

This tool never edits result rows, checkpoints, API rows, attempt rows, raw
responses, task plans, or the dispatch manifest.  It archives the durable stop
latch byte-for-byte and binds exact incident-row hashes plus the interrupted
worker cohort to the current clean recovery commit.
"""

import argparse
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
    _verified_evaluator_incomplete,
    _verified_deepseek_infrastructure_incomplete,
    _verified_infrastructure_incomplete,
    _write_active_worker_set,
)
from run_meta import (
    CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
    CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
    DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
    EMERGENCY_STOP_DIRECTORY,
    LEDGER_LOCK_RECOVERY_KIND,
    STOP_CONDITION_SCHEMA,
    _canonical_record_sha256,
    _campaign_stop_publication_lock,
    _git_identity,
    _git_identity_details,
    _sha256_file,
    append_jsonl_locked,
    campaign_recovery_incident_evidence,
    code_fingerprint,
    interrupt_audited_running_invocations,
    read_campaign_stop_conditions,
    read_run_metadata_snapshot,
    read_sample_outcomes,
    record_campaign_stop_condition,
    read_campaign_recovery_authorization,
    write_json_atomic,
)

_DISPATCHER_PARENT_LOSS_PENDING_FILENAME = (
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME
)
_DISPATCHER_PARENT_LOSS_PENDING_SCHEMA = (
    "anchorpatch.dispatcher_parent_loss_recovery/1"
)


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


def _archive_campaign_stop_cohort(out_dir, stop_path, history_dir):
    """Archive canonical and emergency stop names before authorizing resume."""
    stop_records = read_campaign_stop_conditions(out_dir)
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    emergency_history = os.path.join(
        history_dir, EMERGENCY_STOP_DIRECTORY)
    archived_sources = []
    if os.path.isdir(emergency_history):
        archived_sources = sorted(
            os.path.join(emergency_history, name)
            for name in os.listdir(emergency_history)
            if name.endswith(".json")
        )
    if (not stop_records and not os.path.isfile(archived_stop)
            and not archived_sources):
        raise RuntimeError("campaign stop cohort is missing")
    emergency_dir = os.path.join(out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_names = []
    if os.path.isdir(emergency_dir):
        emergency_names = sorted(
            name for name in os.listdir(emergency_dir)
            if name.endswith(".json")
        )
    archived_emergency = []
    if emergency_names:
        os.makedirs(emergency_history, exist_ok=True)
        for name in emergency_names:
            source = os.path.join(emergency_dir, name)
            destination = os.path.join(emergency_history, name)
            if os.path.isfile(destination):
                if _sha256_file(source) != _sha256_file(destination):
                    raise RuntimeError(
                        "campaign emergency stop archive conflicts")
                _unlink_with_sharing_retry(source)
            else:
                _replace_with_sharing_retry(source, destination)
    archived_sources = []
    if os.path.isdir(emergency_history):
        archived_sources = sorted(
            os.path.join(emergency_history, name)
            for name in os.listdir(emergency_history)
            if name.endswith(".json")
        )
    if os.path.isfile(stop_path):
        if os.path.isfile(archived_stop):
            if _sha256_file(stop_path) != _sha256_file(archived_stop):
                raise RuntimeError("campaign canonical stop archive conflicts")
            _unlink_with_sharing_retry(stop_path)
        else:
            _replace_with_sharing_retry(stop_path, archived_stop)
    elif not os.path.isfile(archived_stop):
        if not archived_sources:
            raise RuntimeError("campaign canonical stop evidence is missing")
        _copy_file_durable(archived_sources[0], archived_stop)
    archived_emergency = [
        {
            "path": os.path.relpath(
                source, out_dir).replace("\\", "/"),
            "sha256": _sha256_file(source),
        }
        for source in archived_sources
    ]
    if read_campaign_stop_conditions(out_dir):
        raise RuntimeError(
            "campaign stop cohort remains active after archival")
    return archived_stop, archived_emergency


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
        allow_stopless_registered_prelaunch=False):
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
        if (record.get("condition") != "dispatcher_process_lost"
                or record.get("dispatcher_pid") != dispatcher_pid
                or record.get("dispatcher_instance_id")
                != dispatcher_instance_id
                or not (
                    registered_prelaunch_only or ordinary_worker_stop)):
            raise RuntimeError(
                "dispatcher parent-loss stop cohort identity mismatch")

    samples = sorted(samples_by_worker.values())
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
        worker_stops = [
            record for record in stop_records
            if record.get("worker_launch_id") == worker_id
        ]
        if intent is not None and (
                intent.get("sample") != sample
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
                or metadata.get("methods")
                != ["hybridpatch", "fullrewrite"]):
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
            "reason": "dispatcher_process_lost",
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
                != DEEPSEEK_TRANSPORT_REVISION
                or row.get("provider_called") is not True
                or row.get("response_replayed") is not False):
            raise RuntimeError(
                "dispatcher parent-loss uncommitted API evidence is invalid")
        if (row.get("classification") is not None
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
                rows = _read_jsonl(path)
                if not rows:
                    continue
                worker_ids = {row.get("worker_launch_id") for row in rows}
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
                methods_seen = {row.get("method") for row in rows}
                samples_seen = {row.get("sample") for row in rows}
                pids_seen = {row.get("worker_pid") for row in rows}
                call_ids = {row.get("call_id") for row in rows}
                rt_indexes = {row.get("rt_index") for row in rows}
                directions = {row.get("direction") for row in rows}
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
                        or not directions <= {"forward", "backward"}
                        or any(
                            row.get("transport_event_schema")
                            != "anchorpatch.transport_event/1"
                            for row in rows)):
                    raise RuntimeError(
                        "dispatcher parent-loss transport sidecar scope "
                        "is invalid")
                starts = [
                    row for row in rows
                    if row.get("record_type") == "attempt_start"
                ]
                ends = [
                    row for row in rows
                    if row.get("record_type") == "attempt_end"
                ]
                if (not starts or len(ends) > len(starts)
                        or [row.get("attempt_index") for row in starts]
                        != list(range(1, len(starts) + 1))
                        or any(
                            not isinstance(row.get("attempt"), dict)
                            or row["attempt"].get("attempt_index") != index
                            for index, row in enumerate(ends, 1)
                        )):
                    raise RuntimeError(
                        "dispatcher parent-loss transport sidecar sequence "
                        "is invalid")
                terminal_attempt = (
                    ends[-1].get("attempt") if ends else None)
                if len(ends) < len(starts):
                    state = "open_attempt"
                elif (terminal_attempt.get("status") == "success"
                      and terminal_attempt.get("stream_complete") is True):
                    state = "complete_unpublished_response"
                else:
                    state = "retry_or_error_unpublished"
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
                    "attempt_start_count": len(starts),
                    "attempt_end_count": len(ends),
                    "state": state,
                })
    return {
        "api": api_incidents,
        "attempts": attempt_incidents,
        "transport_sidecars": sorted(
            sidecar_incidents, key=lambda item: item["path"]),
    }


def _reconcile_terminal_active_workers(
        out_dir, manifest, *, operator_interrupted_samples=None,
        operator_pause_reason=None):
    """Close terminal registrations and explicitly named stopped workers."""
    interrupted_samples = list(operator_interrupted_samples or [])
    if (any(not isinstance(sample, str) or not sample
            for sample in interrupted_samples)
            or len(interrupted_samples) != len(set(interrupted_samples))):
        raise RuntimeError("operator-interrupted sample list is invalid")
    interrupted_samples = set(interrupted_samples)
    active_path = os.path.join(out_dir, "active_worker_set.json")
    active = _read_json(active_path)
    active_workers = active.get("workers")
    if not isinstance(active_workers, dict):
        raise RuntimeError("active worker set is invalid")
    if not active_workers:
        return []
    samples = [
        item.get("sample") for item in active_workers.values()
        if isinstance(item, dict)
    ]
    if (len(samples) != len(active_workers)
            or any(not isinstance(sample, str) or not sample
                   for sample in samples)
            or len(samples) != len(set(samples))):
        raise RuntimeError("active worker identities are invalid")
    if not interrupted_samples <= set(samples):
        raise RuntimeError(
            "operator-interrupted sample is not actively registered")
    _assert_worker_leases_free(out_dir, samples)

    interrupted_audits = []
    if interrupted_samples:
        interrupted_audits = _audit_running_invocation_provenance(out_dir)
        audited_samples = {
            item.get("sample") for item in interrupted_audits
        }
        if audited_samples != interrupted_samples:
            raise RuntimeError(
                "operator-interrupted running invocation scope is invalid")
        interrupted_audits_by_sample = {
            item["sample"]: item for item in interrupted_audits
        }
    else:
        interrupted_audits_by_sample = {}

    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    dispatch_rows = _read_jsonl(dispatch_path)
    launches = {}
    exits = {}
    for row in dispatch_rows:
        worker_id = row.get("worker_launch_id")
        if worker_id not in active_workers:
            continue
        if row.get("event") == "launch":
            if worker_id in launches:
                raise RuntimeError(
                    f"duplicate launch for active worker: {worker_id}")
            launches[worker_id] = row
        elif row.get("event") == "worker_exit":
            if worker_id in exits:
                raise RuntimeError(
                    f"duplicate exit for active worker: {worker_id}")
            exits[worker_id] = row
    metadata = {}
    for row in read_run_metadata_snapshot(out_dir):
        worker_id = row.get("worker_launch_id")
        if worker_id in active_workers:
            if worker_id in metadata:
                raise RuntimeError(
                    f"duplicate metadata for active worker: {worker_id}")
            metadata[worker_id] = row
    outcomes = _latest_outcomes_by_worker(out_dir)
    target_round_trips = (manifest.get("config") or {}).get(
        "num_round_trips")
    if not isinstance(target_round_trips, int) or target_round_trips < 1:
        raise RuntimeError("campaign round-trip target is invalid")

    reconciled = []
    pending_exit_rows = []
    for worker_id, active_item in active_workers.items():
        sample = active_item.get("sample")
        launch = launches.get(worker_id)
        meta = metadata.get(worker_id)
        outcome = outcomes.get(worker_id)
        if (not isinstance(launch, dict)
                or launch.get("sample") != sample
                or not isinstance(launch.get("pid"), int)
                or isinstance(launch.get("pid"), bool)
                or launch.get("pid") <= 0
                or not isinstance(meta, dict)
                or meta.get("samples") != [sample]
                or meta.get("worker_pid") != launch.get("pid")):
            raise RuntimeError(
                f"terminal active worker provenance is invalid: {sample}")
        status = meta.get("status")
        methods = meta.get("methods")
        phase = meta.get("method_phase")
        if sample in interrupted_samples:
            audit = interrupted_audits_by_sample.get(sample)
            if (status != "running"
                    or outcome is not None
                    or methods != [phase]
                    or phase not in {"hybridpatch", "fullrewrite"}
                    or not isinstance(audit, dict)
                    or audit.get("worker_launch_id") != worker_id
                    or audit.get("worker_pid") != launch.get("pid")
                    or exits.get(worker_id) is not None):
                raise RuntimeError(
                    "operator-interrupted worker provenance is invalid: "
                    f"{sample}")
            reconciled.append({
                "sample": sample,
                "worker_launch_id": worker_id,
                "worker_pid": launch["pid"],
                "status": "interrupted_before_audited_resume",
            })
            continue
        if (not isinstance(outcome, dict)
                or outcome.get("sample") != sample
                or outcome.get("worker_pid") != launch.get("pid")
                or outcome.get("status") != status):
            raise RuntimeError(
                f"terminal active worker provenance is invalid: {sample}")
        if (status not in {
                "finished", "infrastructure_incomplete",
                "evaluator_incomplete"}
                or methods != [phase]
                or phase not in {"hybridpatch", "fullrewrite"}):
            raise RuntimeError(
                f"active worker is not terminal: {sample}")
        item = {
            "worker_launch_id": worker_id,
            "worker_pid": launch["pid"],
            "methods": methods,
            "method_phase": phase,
            "target_round_trips": target_round_trips,
        }
        if status == "finished":
            expected = {
                phase: {
                    "completed_round_trips": target_round_trips,
                    "committed_rows": 2 * target_round_trips,
                }
            }
            if (_actual_sample_progress(out_dir, sample, methods) != expected
                    or outcome.get("checkpoint_progress") != expected):
                raise RuntimeError(
                    f"finished active worker evidence is incomplete: {sample}")
            returncode = 0
            evidence = {
                "terminal_metadata_status": status,
                "worker_lease_free": True,
                "exit_code_source": "deterministic_runner_terminal_status",
            }
        elif status == "infrastructure_incomplete":
            returncode = 1
            evidence = _verified_infrastructure_incomplete(
                out_dir, sample, item)
            evidence["exit_code_source"] = (
                "deterministic_runner_terminal_status")
        else:
            returncode = 1
            evidence = _verified_evaluator_incomplete(
                out_dir, sample, item)
            evidence["exit_code_source"] = (
                "deterministic_runner_terminal_status")
        expected_exit = {
            "sample": sample,
            "pid": launch["pid"],
            "returncode": returncode,
            "disposition": status,
        }
        existing_exit = exits.get(worker_id)
        if existing_exit is not None:
            if any(existing_exit.get(key) != value
                   for key, value in expected_exit.items()):
                raise RuntimeError(
                    f"terminal active worker exit drift: {sample}")
        else:
            pending_exit_rows.append({
                "event": "worker_exit",
                "sample": sample,
                "key_label": launch.get("key_label"),
                "worker_launch_id": worker_id,
                "pid": launch["pid"],
                "returncode": returncode,
                "disposition": status,
                "created_at": datetime.now().astimezone().isoformat(
                    timespec="seconds"),
                "evidence": evidence,
                "exit_code_observed": False,
            })
        reconciled.append({
            "sample": sample,
            "worker_launch_id": worker_id,
            "worker_pid": launch["pid"],
            "status": status,
        })
    if interrupted_audits:
        closed = interrupt_audited_running_invocations(
            out_dir,
            status="interrupted_before_audited_resume",
            audited=interrupted_audits,
        )
        if {
                item.get("worker_launch_id") for item in closed
        } != {
                item.get("worker_launch_id")
                for item in interrupted_audits
        }:
            raise RuntimeError(
                "operator-interrupted worker closure is incomplete")
        for item in interrupted_audits:
            append_jsonl_locked(dispatch_path, {
                "event": "stale_worker_reconciled",
                "created_at": datetime.now().astimezone().isoformat(
                    timespec="seconds"),
                "worker_launch_id": item["worker_launch_id"],
                "pid": item["worker_pid"],
                "sample": item["sample"],
                "invocation_id": item["invocation_id"],
                "exit_code_observed": False,
                "reason": operator_pause_reason,
            })
    for row in pending_exit_rows:
        append_jsonl_locked(dispatch_path, row)
    _write_active_worker_set(out_dir, manifest, [])
    return sorted(reconciled, key=lambda item: item["sample"])


def _extend_operator_interrupted_attempt_evidence(
        out_dir, prior_authorization, reconciled_workers):
    """Hash-bind open attempts for explicitly interrupted operator workers."""
    prior_interrupted = list(
        (prior_authorization or {}).get(
            "operator_pause_reconciled_workers") or [])
    if not reconciled_workers and not prior_interrupted:
        return {
            "recovered_worker_launch_ids": list(
                (prior_authorization or {}).get(
                    "recovered_worker_launch_ids") or []),
            "incident_attempt_rows": list(
                (prior_authorization or {}).get(
                    "incident_attempt_rows") or []),
            "operator_interrupted_attempt_row_count": 0,
        }
    interrupted = {}
    for item in prior_interrupted + list(reconciled_workers or []):
        if (isinstance(item, dict)
                and item.get("status")
                == "interrupted_before_audited_resume"):
            worker_id = item.get("worker_launch_id")
            sample = item.get("sample")
            if (not isinstance(worker_id, str) or not worker_id
                    or not isinstance(sample, str) or not sample
                    or (worker_id in interrupted
                        and interrupted[worker_id] != sample)):
                raise RuntimeError(
                    "operator-interrupted worker identity is invalid")
            interrupted[worker_id] = sample
    if not interrupted:
        return {
            "recovered_worker_launch_ids": list(
                (prior_authorization or {}).get(
                    "recovered_worker_launch_ids") or []),
            "incident_attempt_rows": list(
                (prior_authorization or {}).get(
                    "incident_attempt_rows") or []),
            "operator_interrupted_attempt_row_count": 0,
        }

    attempt_rows = _read_jsonl(
        os.path.join(out_dir, "api_attempt_ledger.jsonl"))
    incident_by_number = {}
    for entry in ((prior_authorization or {}).get(
            "incident_attempt_rows") or []):
        number = entry.get("row_number") if isinstance(entry, dict) else None
        if (not isinstance(number, int) or isinstance(number, bool)
                or not 1 <= number <= len(attempt_rows)
                or entry.get("canonical_sha256")
                != _canonical_record_sha256(attempt_rows[number - 1])):
            raise RuntimeError(
                "prior recovery attempt evidence has drifted")
        incident_by_number[number] = entry

    added_numbers = set()
    for worker_id, sample in interrupted.items():
        groups = {}
        for number, row in enumerate(attempt_rows, 1):
            semantic_call_id = row.get("semantic_call_id")
            if (row.get("worker_launch_id") == worker_id
                    and isinstance(semantic_call_id, str)
                    and semantic_call_id.startswith("fullrewrite/")):
                groups.setdefault(semantic_call_id, []).append((number, row))
        open_groups = []
        for semantic_call_id, group in groups.items():
            starts = sum(
                row.get("event") == "attempt_start"
                for _number, row in group)
            ends = sum(
                row.get("event") == "attempt_end"
                for _number, row in group)
            pre_provider = (
                starts == 0 and ends == 0
                and sum(row.get("event") == "semantic_request"
                        for _number, row in group) == 1
            )
            if ((starts > ends or pre_provider)
                    and not any(row.get("event") == "response_committed"
                                for _number, row in group)):
                open_groups.append((semantic_call_id, group))
        if not open_groups:
            raise RuntimeError(
                "operator-interrupted worker has no open attempt: "
                f"{sample}")
        for semantic_call_id, group in open_groups:
            digest = hashlib.sha256(
                semantic_call_id.encode("utf-8")).hexdigest()[:24]
            if os.path.exists(os.path.join(
                    out_dir, "api_journal", f"{digest}.response.json")):
                raise RuntimeError(
                    "operator-interrupted attempt has a response journal")
            for number, row in group:
                expected = _incident_entry(
                    number, row,
                    "dispatcher_interrupted_open_attempt")
                prior = incident_by_number.get(number)
                if prior is not None and prior != expected:
                    raise RuntimeError(
                        "operator-interrupted attempt evidence conflicts")
                incident_by_number[number] = expected
                added_numbers.add(number)

    recovered = list((prior_authorization or {}).get(
        "recovered_worker_launch_ids") or [])
    for worker_id in interrupted:
        if worker_id not in recovered:
            recovered.append(worker_id)
    return {
        "recovered_worker_launch_ids": recovered,
        "incident_attempt_rows": [
            incident_by_number[number]
            for number in sorted(incident_by_number)
        ],
        "operator_interrupted_attempt_row_count": len(added_numbers),
    }


def _authorize_operator_pause(
        out_dir, manifest, stop, stop_path, auth_path, prior_authorization,
        reconciled_workers):
    if (prior_authorization is None
            or prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or prior_authorization.get("recovery_kind")
            != LEDGER_LOCK_RECOVERY_KIND
            or stop.get("condition")
            != "operator_directed_dispatcher_pause"):
        raise RuntimeError("operator pause recovery boundary is invalid")
    current_commit, current_tree = _git_identity()
    if current_tree != "clean":
        raise RuntimeError("operator pause recovery requires a clean Git tree")
    if current_commit == prior_authorization.get("recovery_git_commit"):
        raise RuntimeError("operator pause recovery code commit has not changed")
    if (_sha256_file(os.path.join(out_dir, "dispatch_manifest.json"))
            != prior_authorization.get("dispatch_manifest_sha256")
            or manifest.get("run_git_commit")
            != prior_authorization.get("prior_git_commit")
            or manifest.get("code_fingerprint")
            != prior_authorization.get("prior_code_fingerprint")):
        raise RuntimeError("prior recovery authorization identity has drifted")

    prior_commit = manifest["run_git_commit"]
    prior_fingerprint = manifest["code_fingerprint"]
    recovery_fingerprint = code_fingerprint()
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    changed_paths = _git_changed_paths(prior_commit, current_commit)
    required_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    allowed_paths = required_paths | {
        "HP_V8/VERSION.md", "docs/active_log.md",
    }
    if (fingerprint_changes != ["run_meta.py"]
            or not required_paths <= set(changed_paths)
            or not set(changed_paths) <= allowed_paths):
        raise RuntimeError("operator pause recovery commit scope is invalid")

    interrupted_evidence = _extend_operator_interrupted_attempt_evidence(
        out_dir, prior_authorization, reconciled_workers)
    combined_reconciled = []
    seen_reconciled_workers = set()
    for item in list(prior_authorization.get(
            "operator_pause_reconciled_workers") or []) + list(
                reconciled_workers or []):
        worker_id = item.get("worker_launch_id") if isinstance(item, dict) else None
        if (not isinstance(worker_id, str) or not worker_id
                or worker_id in seen_reconciled_workers):
            if worker_id in seen_reconciled_workers:
                continue
            raise RuntimeError(
                "operator pause reconciled worker history is invalid")
        seen_reconciled_workers.add(worker_id)
        combined_reconciled.append(item)

    authorization_id = (
        "dispatcher-pause-" + datetime.now().astimezone().strftime(
            "%Y%m%dT%H%M%S%z") + "-" + uuid.uuid4().hex[:8]
    )
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    os.makedirs(history_dir, exist_ok=False)
    archived_authorization = os.path.join(
        history_dir, "superseded_campaign_recovery_authorization.json")
    os.replace(auth_path, archived_authorization)
    archived_stop, _archived_emergency = (
        _archive_campaign_stop_cohort(
            out_dir, stop_path, history_dir))

    record = json.loads(json.dumps(prior_authorization))
    record.update({
        "authorization_id": authorization_id,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "authorization_basis": (
            "explicit_user_resume_after_operator_directed_queue_optimization"
        ),
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": recovery_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": os.path.relpath(
            archived_stop, out_dir).replace("\\", "/"),
        "archived_stop_sha256": _sha256_file(archived_stop),
        "superseded_authorization_path": os.path.relpath(
            archived_authorization, out_dir).replace("\\", "/"),
        "superseded_authorization_sha256": _sha256_file(
            archived_authorization),
        "operator_pause_reconciled_workers": combined_reconciled,
        "recovered_worker_launch_ids": interrupted_evidence[
            "recovered_worker_launch_ids"],
        "incident_attempt_rows": interrupted_evidence[
            "incident_attempt_rows"],
        "operator_interrupted_attempt_row_count": (
            interrupted_evidence[
                "operator_interrupted_attempt_row_count"]),
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    })
    write_json_atomic(auth_path, record)
    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(out_dir)
    append_jsonl_locked(os.path.join(out_dir, "dispatch_log.jsonl"), {
        "event": "user_authorized_dispatcher_pause_recovery",
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "campaign_recovery_authorization_id": authorization_id,
        "campaign_recovery_authorization_sha256": verified[
            "authorization_sha256"],
        "reconciled_worker_count": len(reconciled_workers),
    })
    return {
        "authorization_id": authorization_id,
        "authorization_sha256": verified["authorization_sha256"],
        "reconciled_workers": len(reconciled_workers),
        "incident_api_rows": len(record.get("incident_api_rows") or []),
        "incident_attempt_rows": len(
            record.get("incident_attempt_rows") or []),
        "operator_interrupted_attempt_rows": record.get(
            "operator_interrupted_attempt_row_count", 0),
    }


def _authorize_provider_access_retry(
        out_dir, manifest, stop, stop_path, auth_path, prior_authorization):
    """Bind one stopped FR wave and authorize one exact 401 resend each."""
    if (prior_authorization is None
            or prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or prior_authorization.get("recovery_kind")
            != LEDGER_LOCK_RECOVERY_KIND
            or stop.get("condition") != "dispatcher_integrity_failure"
            or not str(stop.get("error") or "").endswith((
                "infrastructure outcome provenance mismatch",
                "infrastructure attempt lineage mismatch",
                "API attempt ledger contains an unclosed HTTP attempt",
            ))):
        raise RuntimeError("provider-access recovery boundary is invalid")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")

    current_commit, current_tree = _git_identity()
    if current_tree != "clean":
        raise RuntimeError("provider-access recovery requires a clean Git tree")
    if current_commit == prior_authorization.get("recovery_git_commit"):
        raise RuntimeError("provider-access recovery code commit has not changed")
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if (_sha256_file(manifest_path)
            != prior_authorization.get("dispatch_manifest_sha256")
            or manifest.get("run_git_commit")
            != prior_authorization.get("prior_git_commit")
            or manifest.get("code_fingerprint")
            != prior_authorization.get("prior_code_fingerprint")):
        raise RuntimeError("prior recovery authorization identity has drifted")

    prior_commit = manifest["run_git_commit"]
    prior_fingerprint = manifest["code_fingerprint"]
    recovery_fingerprint = code_fingerprint()
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    changed_paths = _git_changed_paths(prior_commit, current_commit)
    required_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    allowed_paths = required_paths | {
        "HP_V8/VERSION.md", "docs/active_log.md",
    }
    if (fingerprint_changes != ["run_meta.py"]
            or not required_paths <= set(changed_paths)
            or not set(changed_paths) <= allowed_paths):
        raise RuntimeError("provider-access recovery commit scope is invalid")

    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    dispatch_rows = _read_jsonl(dispatch_path)
    phase_starts = [
        index for index, row in enumerate(dispatch_rows)
        if row.get("event") == "method_phase_start"
        and row.get("method_phase") == "fullrewrite"
    ]
    if not phase_starts:
        raise RuntimeError("provider-access recovery lacks an FR phase start")
    cohort = dispatch_rows[phase_starts[-1] + 1:]
    launches = {
        row.get("worker_launch_id"): row for row in cohort
        if row.get("event") == "launch"
        and row.get("method_phase") == "fullrewrite"
    }
    exits = {
        row.get("worker_launch_id"): row for row in cohort
        if row.get("event") == "worker_exit"
    }
    if (not launches or len(launches) != sum(
            row.get("event") == "launch"
            and row.get("method_phase") == "fullrewrite"
            for row in cohort)):
        raise RuntimeError("provider-access launch cohort is invalid")
    if set(exits) != set(launches):
        raise RuntimeError("provider-access exit cohort is incomplete")
    incomplete_workers = {
        worker_id: launch for worker_id, launch in launches.items()
        if exits[worker_id].get("disposition") != "finished"
    }
    if not incomplete_workers:
        raise RuntimeError("provider-access recovery has no incomplete workers")
    samples = [row.get("sample") for row in incomplete_workers.values()]
    if (any(not isinstance(sample, str) or not sample for sample in samples)
            or len(samples) != len(set(samples))):
        raise RuntimeError("provider-access sample cohort is invalid")
    _assert_worker_leases_free(out_dir, samples)

    metadata_by_worker = {}
    for row in read_run_metadata_snapshot(out_dir):
        worker_id = row.get("worker_launch_id")
        if worker_id in incomplete_workers:
            if worker_id in metadata_by_worker:
                raise RuntimeError(
                    "provider-access worker metadata is duplicated")
            metadata_by_worker[worker_id] = row
    if set(metadata_by_worker) != set(incomplete_workers):
        raise RuntimeError("provider-access worker metadata is incomplete")
    for worker_id, launch in incomplete_workers.items():
        metadata = metadata_by_worker[worker_id]
        exit_row = exits[worker_id]
        if (metadata.get("samples") != [launch.get("sample")]
                or metadata.get("methods") != ["fullrewrite"]
                or metadata.get("method_phase") != "fullrewrite"
                or metadata.get("status") not in {
                    "failed", "interrupted_by_dispatcher"
                }
                or exit_row.get("sample") != launch.get("sample")
                or exit_row.get("pid") != launch.get("pid")
                or exit_row.get("returncode") == 0
                or exit_row.get("disposition") != "campaign_fatal"):
            raise RuntimeError(
                "provider-access worker terminal evidence is invalid")

    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    attempt_rows = _read_jsonl(os.path.join(
        out_dir, "api_attempt_ledger.jsonl"))
    provider_retries = []
    interrupted_attempt_entries = []
    existing_attempt_numbers = {
        entry.get("row_number") for entry in
        prior_authorization.get("incident_attempt_rows") or []
    }
    for worker_id, launch in incomplete_workers.items():
        sample = launch["sample"]
        metadata = metadata_by_worker[worker_id]
        access_rows = [
            (number, row) for number, row in enumerate(api_rows, 1)
            if row.get("worker_launch_id") == worker_id
            and row.get("sample") == sample
            and row.get("method") == "fullrewrite"
            and row.get("classification") == "provider/API failure"
            and row.get("error_type") == "provider_access_denied"
            and row.get("http_status") in {401, 403}
            and row.get("provider_called") is True
        ]
        if access_rows:
            if len(access_rows) != 1 or metadata.get("status") != "failed":
                raise RuntimeError(
                    "provider-access API failure is not unique")
            api_number, api_row = access_rows[0]
            parent_id = api_row.get("semantic_call_id")
            semantic_root_id = api_row.get("semantic_root_id")
            generation_index = api_row.get("generation_index")
            request_fingerprint = api_row.get("request_fingerprint")
            if (not isinstance(parent_id, str) or not parent_id
                    or not isinstance(semantic_root_id, str)
                    or not semantic_root_id
                    or not isinstance(generation_index, int)
                    or isinstance(generation_index, bool)
                    or generation_index < 0
                    or parent_id
                    != f"{semantic_root_id}/g{generation_index:03d}"
                    or not isinstance(request_fingerprint, str)
                    or not request_fingerprint
                    or not isinstance(
                        api_row.get("response_slots_used"), int)
                    or not 0 <= api_row.get("response_slots_used") < 2
                    or api_row.get("transient_failure_count") != 0):
                raise RuntimeError(
                    "provider-access API lineage is invalid")
            group = [
                (number, row) for number, row in enumerate(attempt_rows, 1)
                if row.get("worker_launch_id") == worker_id
                and row.get("semantic_call_id") == parent_id
            ]
            events = [row.get("event") for _number, row in group]
            attempt_end = [
                row for _number, row in group
                if row.get("event") == "attempt_end"
            ]
            call_failed = [
                row for _number, row in group
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
                    "provider-access attempt evidence is invalid")
            root_attempt_indexes = [
                row.get("attempt_index") for row in attempt_rows
                if row.get("semantic_root_id") == semantic_root_id
                and isinstance(row.get("attempt_index"), int)
                and not isinstance(row.get("attempt_index"), bool)
            ]
            next_generation = generation_index + 1
            provider_retries.append({
                "sample": sample,
                "prior_worker_launch_id": worker_id,
                "prior_invocation_id": metadata.get("invocation_id"),
                "parent_semantic_call_id": parent_id,
                "semantic_root_id": semantic_root_id,
                "semantic_call_id": (
                    f"{semantic_root_id}/g{next_generation:03d}"
                ),
                "generation_index": next_generation,
                "request_fingerprint": request_fingerprint,
                "next_attempt_index": max(
                    root_attempt_indexes, default=0) + 1,
                "api_row": _incident_entry(
                    api_number, api_row, "provider_access_denied"),
                "attempt_rows": [
                    _incident_entry(
                        number, row, "provider_access_denied_retry")
                    for number, row in group
                ],
            })
            continue

        if metadata.get("status") != "interrupted_by_dispatcher":
            raise RuntimeError(
                "non-401 worker is not a dispatcher interruption")
        groups = {}
        for number, row in enumerate(attempt_rows, 1):
            semantic_call_id = row.get("semantic_call_id")
            if (row.get("worker_launch_id") == worker_id
                    and isinstance(semantic_call_id, str)
                    and semantic_call_id.startswith("fullrewrite/")):
                groups.setdefault(semantic_call_id, []).append((number, row))
        open_groups = []
        for semantic_call_id, group in groups.items():
            starts = sum(
                row.get("event") == "attempt_start"
                for _number, row in group)
            ends = sum(
                row.get("event") == "attempt_end"
                for _number, row in group)
            pre_provider = (
                starts == 0 and ends == 0
                and sum(row.get("event") == "semantic_request"
                        for _number, row in group) == 1
            )
            if ((starts > ends or pre_provider)
                    and not any(row.get("event") == "response_committed"
                                for _number, row in group)):
                open_groups.append((semantic_call_id, group))
        if not open_groups:
            raise RuntimeError(
                "dispatcher-interrupted worker has no open attempt")
        for semantic_call_id, group in open_groups:
            digest = hashlib.sha256(
                semantic_call_id.encode("utf-8")).hexdigest()[:24]
            if os.path.exists(os.path.join(
                    out_dir, "api_journal", f"{digest}.response.json")):
                raise RuntimeError(
                    "dispatcher-interrupted attempt has a response journal")
            for number, row in group:
                if number in existing_attempt_numbers:
                    continue
                interrupted_attempt_entries.append(_incident_entry(
                    number, row, "dispatcher_interrupted_open_attempt"))
                existing_attempt_numbers.add(number)

    if not provider_retries:
        raise RuntimeError("provider-access recovery has no 401 failures")
    recovered_workers = list(
        prior_authorization.get("recovered_worker_launch_ids") or [])
    for worker_id in incomplete_workers:
        if worker_id not in recovered_workers:
            recovered_workers.append(worker_id)

    authorization_id = (
        "provider-access-" + datetime.now().astimezone().strftime(
            "%Y%m%dT%H%M%S%z") + "-" + uuid.uuid4().hex[:8]
    )
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    os.makedirs(history_dir, exist_ok=False)
    archived_authorization = os.path.join(
        history_dir, "superseded_campaign_recovery_authorization.json")
    os.replace(auth_path, archived_authorization)
    archived_stop, _archived_emergency = (
        _archive_campaign_stop_cohort(
            out_dir, stop_path, history_dir))

    record = json.loads(json.dumps(prior_authorization))
    record.update({
        "authorization_id": authorization_id,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "authorization_basis": (
            "explicit_user_retry_after_upstream_provider_access_denied"
        ),
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": recovery_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": os.path.relpath(
            archived_stop, out_dir).replace("\\", "/"),
        "archived_stop_sha256": _sha256_file(archived_stop),
        "superseded_authorization_path": os.path.relpath(
            archived_authorization, out_dir).replace("\\", "/"),
        "superseded_authorization_sha256": _sha256_file(
            archived_authorization),
        "recovered_worker_launch_ids": sorted(recovered_workers),
        "incident_attempt_rows": list(
            prior_authorization.get("incident_attempt_rows") or [])
            + interrupted_attempt_entries,
        "provider_access_retry_authorizations": sorted(
            provider_retries, key=lambda item: item["sample"]),
        "provider_access_resume_samples": sorted(samples),
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    })
    write_json_atomic(auth_path, record)
    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(out_dir)
    append_jsonl_locked(dispatch_path, {
        "event": "user_authorized_provider_access_retry",
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "campaign_recovery_authorization_id": authorization_id,
        "campaign_recovery_authorization_sha256": verified[
            "authorization_sha256"],
        "provider_access_retry_count": len(provider_retries),
        "dispatcher_interrupted_worker_count": (
            len(incomplete_workers) - len(provider_retries)),
    })
    return {
        "authorization_id": authorization_id,
        "authorization_sha256": verified["authorization_sha256"],
        "provider_access_retries": len(provider_retries),
        "dispatcher_interrupted_workers": (
            len(incomplete_workers) - len(provider_retries)),
    }


def _deepseek_server_retry_rows(out_dir):
    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    failures = []
    for number, row in enumerate(api_rows, 1):
        attempts = row.get("transport_attempts")
        final_attempt = attempts[-1] if isinstance(
            attempts, list) and attempts else {}
        final_budget = final_attempt.get("retry_budget_attempt_index")
        if (row.get("model") == "deepseek-v4-flash"
                and row.get("classification") == "provider/API failure"
                and row.get("error_type") == "server_error"
                and row.get("http_status") in {502, 503}
                and row.get("provider_called") is True
                and row.get("stream_complete") is False
                and row.get("response_replayed") is False
                and row.get("count_as_method_failure") is False
                and isinstance(final_budget, int)
                and not isinstance(final_budget, bool)
                and final_budget >= 3
                and final_attempt.get("status") == "retryable_error"
                and final_attempt.get("retry_budget_consumed") is True):
            failures.append((number, row))
    return failures


def _deepseek_checkpoint_resume_scope(out_dir, manifest):
    """Return the exact incomplete DeepSeek cohort and uncommitted API rows."""
    config = manifest.get("config") or {}
    target = config.get("num_round_trips")
    samples = config.get("samples") or []
    methods = config.get("method_set") or []
    if (not isinstance(target, int) or isinstance(target, bool) or target < 1
            or set(methods) != {"hybridpatch", "fullrewrite"}):
        raise RuntimeError("DeepSeek checkpoint scope is invalid")

    progress_by_sample = {}
    resume_samples = []
    for sample in samples:
        progress = _actual_sample_progress(out_dir, sample, methods)
        progress_by_sample[sample] = progress
        if any(
                progress.get(method) != {
                    "completed_round_trips": target,
                    "committed_rows": 2 * target,
                }
                for method in methods):
            resume_samples.append(sample)

    latest_metadata = {}
    for row in read_run_metadata_snapshot(out_dir):
        for sample in row.get("samples") or []:
            if sample in resume_samples:
                latest_metadata[sample] = row
    dispatch_rows = _read_jsonl(os.path.join(
        out_dir, "dispatch_log.jsonl"))
    exits_by_worker = {}
    for row in dispatch_rows:
        worker_id = row.get("worker_launch_id")
        if row.get("event") == "worker_exit" and isinstance(worker_id, str):
            exits_by_worker.setdefault(worker_id, []).append(row)

    recovered = []
    pending_samples = []
    for sample in resume_samples:
        metadata = latest_metadata.get(sample)
        if metadata is None:
            pending_samples.append(sample)
            continue
        worker_id = metadata.get("worker_launch_id")
        worker_pid = metadata.get("worker_pid")
        exits = exits_by_worker.get(worker_id) or []
        if (metadata.get("status") not in {
                "failed", "interrupted_by_dispatcher"}
                or not isinstance(worker_id, str) or not worker_id
                or not isinstance(worker_pid, int)
                or isinstance(worker_pid, bool) or worker_pid <= 0
                or len(exits) != 1
                or exits[0].get("sample") != sample
                or exits[0].get("pid") != worker_pid
                or exits[0].get("returncode") == 0
                or exits[0].get("disposition") != "campaign_fatal"):
            raise RuntimeError(
                f"DeepSeek interrupted worker evidence is invalid: {sample}")
        _assert_worker_leases_free(out_dir, [sample])
        recovered.append({
            "sample": sample,
            "status": metadata["status"],
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": metadata.get("invocation_id"),
        })

    recovered_workers = {
        item["worker_launch_id"] for item in recovered
    }
    resume_set = set(resume_samples)
    incident_rows = []
    for number, row in enumerate(_read_jsonl(os.path.join(
            out_dir, "api_calls.jsonl")), 1):
        sample = row.get("sample")
        method = row.get("method")
        rt_index = row.get("rt_index")
        if (sample not in resume_set
                or row.get("worker_launch_id") not in recovered_workers
                or method not in methods
                or not isinstance(rt_index, int)
                or isinstance(rt_index, bool)):
            continue
        committed_rt = progress_by_sample[sample][
            method]["completed_round_trips"]
        if rt_index > committed_rt:
            incident_rows.append((number, row))
    if not incident_rows:
        raise RuntimeError(
            "DeepSeek checkpoint recovery has no uncommitted API rows")
    return {
        "resume_samples": sorted(resume_samples),
        "pending_samples": sorted(pending_samples),
        "recovered_workers": sorted(
            recovered, key=lambda item: item["sample"]),
        "incident_api_rows": incident_rows,
    }


def _authorize_deepseek_server_retry(
        out_dir, manifest, stop, stop_path, auth_path, prior_authorization):
    config = manifest.get("config") or {}
    if (prior_authorization is not None
            or config.get("campaign_role") != "deepseek_full234"
            or config.get("num_round_trips")
            != DEEPSEEK_FULL234_ROUND_TRIPS
            or config.get("transport") != DEEPSEEK_TRANSPORT
            or config.get("transport_revision")
            != DEEPSEEK_TRANSPORT_REVISION
            or stop.get("condition") != "dispatcher_integrity_failure"
            or not str(stop.get("error") or "").endswith(
                "failed without a supported sample-local outcome")):
        raise RuntimeError("DeepSeek server-retry recovery boundary is invalid")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = (manifest.get("config") or {}).get("samples") or []
    _assert_worker_leases_free(out_dir, samples)

    current_commit, current_tree = _git_identity()
    if current_tree != "clean":
        raise RuntimeError("DeepSeek server-retry recovery requires a clean Git tree")
    prior_commit = manifest.get("run_git_commit")
    if current_commit == prior_commit:
        raise RuntimeError("DeepSeek server-retry recovery code commit has not changed")

    scope = _deepseek_checkpoint_resume_scope(out_dir, manifest)
    recovered_workers = scope["recovered_workers"]
    recovered_by_id = {
        item["worker_launch_id"]: item for item in recovered_workers
    }
    failures = _deepseek_server_retry_rows(out_dir)
    if not failures:
        raise RuntimeError("no DeepSeek server-retry failure rows found")
    worker_ids = sorted(recovered_by_id)
    retry_samples = [row.get("sample") for _number, row in failures]
    if (len(retry_samples) != len(set(retry_samples))
            or any(
                row.get("worker_launch_id") not in recovered_by_id
                or recovered_by_id[row.get("worker_launch_id")].get("status")
                != "failed"
                for _number, row in failures)
            or any(not isinstance(sample, str) or sample not in samples
                   for sample in retry_samples)):
        raise RuntimeError("DeepSeek server-retry worker scope is invalid")
    failure_hashes = {
        _canonical_record_sha256(row) for _number, row in failures
    }

    prior_fingerprint = manifest.get("code_fingerprint")
    recovery_fingerprint = code_fingerprint()
    if not isinstance(prior_fingerprint, dict):
        raise RuntimeError("dispatch manifest code fingerprint is invalid")
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    changed_paths = _git_changed_paths(prior_commit, current_commit)
    required_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
    }
    allowed_paths = required_paths | {
        "HP_V8/VERSION.md", "docs/active_log.md",
    }
    if (fingerprint_changes != ["run_meta.py"]
            or not required_paths <= set(changed_paths)
            or not set(changed_paths) <= allowed_paths):
        raise RuntimeError("DeepSeek server-retry recovery commit scope is invalid")

    authorization_id = (
        "deepseek-server-retry-" + datetime.now().astimezone().strftime(
            "%Y%m%dT%H%M%S%z") + "-" + uuid.uuid4().hex[:8]
    )
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    os.makedirs(history_dir, exist_ok=False)
    archived_stop, _archived_emergency = (
        _archive_campaign_stop_cohort(
            out_dir, stop_path, history_dir))

    record = {
        "schema": CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
        "authorization_id": authorization_id,
        "recovery_kind": LEDGER_LOCK_RECOVERY_KIND,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "authorization_basis": (
            "explicit_user_resume_after_deepseek_server_retry_exhaustion"
        ),
        "deepseek_server_retry_recovery": True,
        "deepseek_server_retry_samples": sorted(retry_samples),
        "deepseek_resume_samples": scope["resume_samples"],
        "deepseek_pending_samples": scope["pending_samples"],
        "deepseek_recovered_workers": recovered_workers,
        "provider_access_resume_samples": sorted(retry_samples),
        "provider_access_retry_authorizations": [],
        "dispatch_manifest_sha256": _sha256_file(os.path.join(
            out_dir, "dispatch_manifest.json")),
        "prior_git_commit": prior_commit,
        "prior_git_tree_state": "clean",
        "prior_code_fingerprint": prior_fingerprint,
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": recovery_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": os.path.relpath(
            archived_stop, out_dir).replace("\\", "/"),
        "archived_stop_sha256": _sha256_file(archived_stop),
        "incident_api_rows": [
            _incident_entry(
                number, row,
                "deepseek_server_retry_exhaustion"
                if _canonical_record_sha256(row) in failure_hashes
                else "deepseek_interrupted_uncommitted_api")
            for number, row in scope["incident_api_rows"]
        ],
        "incident_attempt_rows": [],
        "recovered_worker_launch_ids": sorted(worker_ids),
        "preauthorization_worker_launch_ids": [],
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    }
    write_json_atomic(auth_path, record)
    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(out_dir)
    append_jsonl_locked(os.path.join(out_dir, "dispatch_log.jsonl"), {
        "event": "user_authorized_deepseek_server_retry_recovery",
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "campaign_recovery_authorization_id": authorization_id,
        "campaign_recovery_authorization_sha256": verified[
            "authorization_sha256"],
        "deepseek_server_retry_samples": sorted(retry_samples),
    })
    return {
        "authorization_id": authorization_id,
        "authorization_sha256": verified["authorization_sha256"],
        "deepseek_server_retry_samples": sorted(retry_samples),
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


def _commit_dispatcher_parent_loss_recovery(
        out_dir, manifest, stop_path, auth_path, pending_path, pending):
    """Finish a prepared parent-loss recovery; every step is idempotent."""
    if (not isinstance(pending, dict)
            or pending.get("schema")
            != _DISPATCHER_PARENT_LOSS_PENDING_SCHEMA
            or not isinstance(pending.get("authorization_record"), dict)
            or not isinstance(pending.get("recovery_plan"), dict)):
        raise RuntimeError(
            "dispatcher parent-loss pending recovery is invalid")
    record = pending["authorization_record"]
    recovery_plan = pending["recovery_plan"]
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if (_sha256_file(manifest_path)
            != record.get("dispatch_manifest_sha256")):
        raise RuntimeError(
            "dispatcher parent-loss pending manifest has drifted")
    current_commit, current_tree = _git_identity()
    if (current_tree != "clean"
            or current_commit != record.get("recovery_git_commit")
            or code_fingerprint()
            != record.get("recovery_code_fingerprint")):
        raise RuntimeError(
            "dispatcher parent-loss pending identity has drifted")
    if (record.get("dispatcher_pid")
            != recovery_plan.get("dispatcher_pid")
            or record.get("dispatcher_instance_id")
            != recovery_plan.get("dispatcher_instance_id")
            or record.get("dispatcher_parent_loss_workers")
            != recovery_plan.get("workers")):
        raise RuntimeError(
            "dispatcher parent-loss pending worker plan has drifted")
    samples = [
        item.get("sample") for item in recovery_plan.get("workers") or []
        if isinstance(item, dict)
    ]
    _assert_worker_leases_free(out_dir, samples)

    _apply_deepseek_parent_loss_recovery_plan(
        out_dir, manifest, recovery_plan)
    existing_authorization = (
        _read_json(auth_path) if os.path.isfile(auth_path) else None
    )
    if existing_authorization != record:
        write_json_atomic(auth_path, record)

    history_relative = pending.get("history_dir")
    if not isinstance(history_relative, str) or not history_relative:
        raise RuntimeError(
            "dispatcher parent-loss pending history path is invalid")
    history_dir = os.path.realpath(os.path.join(
        out_dir, history_relative))
    if (os.path.commonpath([out_dir, history_dir]) != out_dir
            or not os.path.isdir(history_dir)):
        raise RuntimeError(
            "dispatcher parent-loss pending history path is invalid")
    archived_stop, archived_emergency = _archive_campaign_stop_cohort(
        out_dir, stop_path, history_dir)
    if (_sha256_file(archived_stop)
            != record.get("archived_stop_sha256")
            or os.path.relpath(
                archived_stop, out_dir).replace("\\", "/")
            != record.get("archived_stop_path")
            or archived_emergency
            != record.get("archived_emergency_stop_records")):
        raise RuntimeError(
            "dispatcher parent-loss archived stop cohort has drifted")

    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(
        out_dir, allow_pending_transaction=True)
    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    event = {
        "event": "user_authorized_dispatcher_parent_loss_recovery",
        "created_at": pending.get("created_at"),
        "campaign_recovery_authorization_id": record["authorization_id"],
        "campaign_recovery_authorization_sha256": verified[
            "authorization_sha256"],
        "dispatcher_pid": recovery_plan["dispatcher_pid"],
        "dispatcher_instance_id": recovery_plan[
            "dispatcher_instance_id"],
        "resume_samples": record[
            "dispatcher_parent_loss_resume_samples"],
    }
    matches = [
        row for row in _read_jsonl(dispatch_path)
        if row.get("event") == event["event"]
        and row.get("campaign_recovery_authorization_id")
        == record["authorization_id"]
    ]
    if not matches:
        append_jsonl_locked(dispatch_path, event)
    elif (len(matches) != 1
          or any(
              matches[0].get(key) != value
              for key, value in event.items()
              if key != "created_at"
          )):
        raise RuntimeError(
            "dispatcher parent-loss authorization event has drifted")
    _unlink_with_sharing_retry(pending_path)
    return {
        "authorization_id": record["authorization_id"],
        "authorization_sha256": verified["authorization_sha256"],
        "resume_samples": record[
            "dispatcher_parent_loss_resume_samples"],
        "reconciled_workers": len(recovery_plan["workers"]),
    }


def _authorize_dispatcher_process_lost(
        out_dir, manifest, stop_path, auth_path, prior_authorization):
    """Authorize an exact DeepSeek checkpoint resume after parent loss."""
    pending_path = os.path.join(
        out_dir, _DISPATCHER_PARENT_LOSS_PENDING_FILENAME)
    if os.path.isfile(pending_path):
        pending = _read_json(pending_path)
        return _commit_dispatcher_parent_loss_recovery(
            out_dir, manifest, stop_path, auth_path, pending_path,
            pending)
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    manifest_digest = _sha256_file(manifest_path)
    prior_commit = manifest.get("run_git_commit")
    prior_fingerprint = manifest.get("code_fingerprint")
    current_commit, current_tree = _git_identity()
    current_fingerprint = code_fingerprint()
    if (current_tree != "clean"
            or not isinstance(prior_commit, str)
            or not isinstance(prior_fingerprint, dict)):
        raise RuntimeError(
            "dispatcher parent-loss recovery requires a clean identity")
    if prior_authorization is None:
        if (current_commit != prior_commit
                or current_fingerprint != prior_fingerprint):
            raise RuntimeError(
                "dispatcher parent-loss recovery cannot change code identity")
    else:
        if (prior_authorization.get("schema")
                != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
                or prior_authorization.get("recovery_kind") not in {
                    LEDGER_LOCK_RECOVERY_KIND,
                    DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
                }
                or prior_authorization.get("dispatch_manifest_sha256")
                != manifest_digest
                or prior_authorization.get("prior_git_commit")
                != prior_commit
                or prior_authorization.get("prior_code_fingerprint")
                != prior_fingerprint
                or prior_authorization.get("recovery_git_commit")
                != current_commit
                or prior_authorization.get("recovery_code_fingerprint")
                != current_fingerprint):
            raise RuntimeError(
                "dispatcher parent-loss prior authorization identity drift")
        verified_prior = read_campaign_recovery_authorization(
            out_dir, allow_active_dispatcher_stop=True)
        if (not isinstance(verified_prior, dict)
                or verified_prior.get("authorization_id")
                != prior_authorization.get("authorization_id")
                or verified_prior.get("authorization_sha256")
                != _sha256_file(auth_path)):
            raise RuntimeError(
                "dispatcher parent-loss prior authorization is invalid")

    stop_records = read_campaign_stop_conditions(out_dir)
    if not stop_records:
        _record_stopless_registered_prelaunch_parent_loss(
            out_dir, manifest, stop_path)
        stop_records = read_campaign_stop_conditions(out_dir)
    if (not stop_records
            or any(
                row.get("condition") != "dispatcher_process_lost"
                for row in stop_records)):
        raise RuntimeError(
            "campaign is not stopped by one lost dispatcher parent")

    authorization_id = (
        "dispatcher-parent-loss-"
        + datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
        + "-" + uuid.uuid4().hex[:8]
    )
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    os.makedirs(history_dir, exist_ok=False)
    active_path = os.path.join(out_dir, "active_worker_set.json")
    metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
    archived_active = os.path.join(
        history_dir, "active_worker_set.before.json")
    archived_metadata = os.path.join(
        history_dir, "run_metadata.before.jsonl")
    _copy_file_durable(active_path, archived_active)
    _copy_file_durable(
        metadata_path, archived_metadata, allow_missing=True)
    metadata_before = _read_jsonl(archived_metadata)

    recovery_scope = _reconcile_deepseek_parent_loss_workers(
        out_dir, manifest, stop_records, apply=False)
    incidents = _deepseek_parent_loss_incidents(
        out_dir, manifest, recovery_scope)
    resume_workers = (
        recovery_scope["interrupted_workers"]
        + recovery_scope["preauthorization_workers"]
        + recovery_scope["registered_prelaunch_workers"]
    )
    resume_samples = [item["sample"] for item in resume_workers]
    recovered_worker_ids = sorted(set(
        list((prior_authorization or {}).get(
            "recovered_worker_launch_ids") or [])
        + [
            item["worker_launch_id"]
            for item in recovery_scope["workers"]
        ]
    ))
    preauthorization_worker_ids = sorted(set(
        list((prior_authorization or {}).get(
            "preauthorization_worker_launch_ids") or [])
        + [
            item["worker_launch_id"]
            for item in recovery_scope["preauthorization_workers"]
        ]
    ))
    registered_prelaunch_worker_ids = sorted(
        item["worker_launch_id"]
        for item in recovery_scope["registered_prelaunch_workers"]
    )
    api_incidents = _merge_incident_entries(
        (prior_authorization or {}).get("incident_api_rows"),
        incidents["api"],
    )
    attempt_incidents = _merge_incident_entries(
        (prior_authorization or {}).get("incident_attempt_rows"),
        incidents["attempts"],
    )
    transport_sidecar_incidents = _merge_transport_sidecar_entries(
        (prior_authorization or {}).get(
            "incident_transport_sidecars"),
        incidents["transport_sidecars"],
    )
    parent_metadata = []
    recovered_ids_current = {
        item["worker_launch_id"] for item in recovery_scope["workers"]
    }
    for number, row in enumerate(metadata_before, 1):
        if row.get("worker_launch_id") in recovered_ids_current:
            parent_metadata.append({
                "row_number": number,
                "canonical_sha256": _canonical_record_sha256(row),
                "invocation_id": row.get("invocation_id"),
                "worker_launch_id": row.get("worker_launch_id"),
                "prior_status": row.get("status"),
            })
    prior_resume_workers = (
        (prior_authorization or {}).get(
            "dispatcher_parent_loss_resume_workers")
        or (prior_authorization or {}).get(
            "dispatcher_parent_loss_workers")
        or []
    )
    resumable_parent_loss_statuses = {
        "interrupted_by_dispatcher",
        "preauthorization",
        "registered_prelaunch",
    }
    resume_workers_by_sample = {}
    for item in prior_resume_workers:
        sample = item.get("sample") if isinstance(item, dict) else None
        worker_id = (
            item.get("worker_launch_id")
            if isinstance(item, dict) else None
        )
        if (not isinstance(sample, str) or not sample
                or not isinstance(worker_id, str) or not worker_id):
            raise RuntimeError(
                "dispatcher parent-loss inherited worker scope is invalid")
        resume_workers_by_sample[sample] = dict(item)
    for item in recovery_scope["workers"]:
        sample = item.get("sample") if isinstance(item, dict) else None
        worker_id = (
            item.get("worker_launch_id")
            if isinstance(item, dict) else None
        )
        status = item.get("status") if isinstance(item, dict) else None
        if (not isinstance(sample, str) or not sample
                or not isinstance(worker_id, str) or not worker_id):
            raise RuntimeError(
                "dispatcher parent-loss current worker scope is invalid")
        if status in resumable_parent_loss_statuses:
            resume_workers_by_sample[sample] = dict(item)
        else:
            resume_workers_by_sample.pop(sample, None)
    cumulative_resume_workers = [
        resume_workers_by_sample[sample]
        for sample in sorted(resume_workers_by_sample)
    ]
    terminal_current_samples = {
        item["sample"] for item in recovery_scope["workers"]
        if item["status"] not in resumable_parent_loss_statuses
    }

    archived_authorization = None
    if prior_authorization is not None:
        archived_authorization = os.path.join(
            history_dir,
            "superseded_campaign_recovery_authorization.json")
        _copy_file_durable(auth_path, archived_authorization)
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    emergency_dir = os.path.join(out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_names = (
        sorted(
            name for name in os.listdir(emergency_dir)
            if name.endswith(".json"))
        if os.path.isdir(emergency_dir) else []
    )
    emergency_sources = [
        os.path.join(emergency_dir, name) for name in emergency_names
    ]
    canonical_source = (
        stop_path if os.path.isfile(stop_path)
        else (emergency_sources[0] if emergency_sources else None)
    )
    if canonical_source is None:
        raise RuntimeError("campaign stop cohort is missing")
    archived_emergency = [
        {
            "path": os.path.relpath(
                os.path.join(
                    history_dir, EMERGENCY_STOP_DIRECTORY, name),
                out_dir,
            ).replace("\\", "/"),
            "sha256": _sha256_file(source),
        }
        for name, source in zip(emergency_names, emergency_sources)
    ]

    changed_paths = _git_changed_paths(prior_commit, current_commit)
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(current_fingerprint)
        if prior_fingerprint.get(key) != current_fingerprint.get(key)
    )
    record = json.loads(json.dumps(prior_authorization or {}))
    record.pop("deepseek_server_retry_recovery", None)
    record.update({
        "schema": CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
        "authorization_id": authorization_id,
        "recovery_kind": DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "authorization_basis": (
            "explicit_user_resume_after_dispatcher_process_loss"),
        "dispatcher_process_lost_recovery": True,
        "dispatcher_pid": recovery_scope["dispatcher_pid"],
        "dispatcher_instance_id": (
            recovery_scope["dispatcher_instance_id"]),
        "dispatcher_parent_loss_workers": recovery_scope["workers"],
        "dispatcher_parent_loss_resume_workers": (
            cumulative_resume_workers),
        "dispatcher_parent_loss_resume_samples": sorted(
            resume_samples),
        "dispatcher_parent_loss_registered_prelaunch_worker_launch_ids": (
            registered_prelaunch_worker_ids),
        "dispatcher_parent_loss_metadata_rows": parent_metadata,
        "dispatch_manifest_sha256": manifest_digest,
        "prior_git_commit": prior_commit,
        "prior_git_tree_state": "clean",
        "prior_code_fingerprint": prior_fingerprint,
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": current_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": os.path.relpath(
            archived_stop, out_dir).replace("\\", "/"),
        "archived_stop_sha256": _sha256_file(canonical_source),
        "archived_emergency_stop_records": archived_emergency,
        "archived_active_worker_set_path": os.path.relpath(
            archived_active, out_dir).replace("\\", "/"),
        "archived_active_worker_set_sha256": _sha256_file(
            archived_active),
        "archived_run_metadata_path": os.path.relpath(
            archived_metadata, out_dir).replace("\\", "/"),
        "archived_run_metadata_sha256": _sha256_file(
            archived_metadata),
        "recovered_worker_launch_ids": recovered_worker_ids,
        "preauthorization_worker_launch_ids": list(
            preauthorization_worker_ids),
        "incident_api_rows": api_incidents,
        "incident_attempt_rows": attempt_incidents,
        "incident_transport_sidecars": transport_sidecar_incidents,
        "provider_access_retry_authorizations": list(
            (prior_authorization or {}).get(
                "provider_access_retry_authorizations") or []),
        "provider_access_resume_samples": sorted(set(
            list((prior_authorization or {}).get(
                "provider_access_resume_samples") or [])
            + resume_samples
        ) - terminal_current_samples),
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    })
    if archived_authorization is None:
        record.pop("superseded_authorization_path", None)
        record.pop("superseded_authorization_sha256", None)
    else:
        record["superseded_authorization_path"] = os.path.relpath(
            archived_authorization, out_dir).replace("\\", "/")
        record["superseded_authorization_sha256"] = _sha256_file(
            archived_authorization)

    pending = {
        "schema": _DISPATCHER_PARENT_LOSS_PENDING_SCHEMA,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "history_dir": os.path.relpath(
            history_dir, out_dir).replace("\\", "/"),
        "authorization_record": record,
        "recovery_plan": recovery_scope,
    }
    write_json_atomic(pending_path, pending)
    return _commit_dispatcher_parent_loss_recovery(
        out_dir, manifest, stop_path, auth_path, pending_path, pending)


def authorize(
        out_dir, *, operator_pause=False, operator_pause_reason=None,
        operator_interrupted_samples=None, provider_access_retry=False,
        deepseek_server_retry=False, dispatcher_process_lost=False):
    out_dir = os.path.abspath(out_dir)
    selected_modes = sum(bool(value) for value in (
        operator_pause, provider_access_retry, deepseek_server_retry,
        dispatcher_process_lost,
    ))
    if selected_modes > 1:
        raise RuntimeError("campaign recovery modes are mutually exclusive")
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    stop_path = os.path.join(out_dir, "campaign_stop.json")
    auth_path = os.path.join(
        out_dir, CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
    if dispatcher_process_lost:
        lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
        with open(lease_path, "a+", encoding="utf-8") as lease:
            try:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.LockException as exc:
                raise RuntimeError(
                    "dispatcher parent-loss recovery requires the dispatcher "
                    "lease to be free") from exc
            try:
                prior_authorization = (
                    _read_json(auth_path)
                    if os.path.exists(auth_path) else None
                )
                if (prior_authorization is not None
                        and prior_authorization.get("schema")
                        != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2):
                    raise RuntimeError(
                        "cannot supersede a non-V2 recovery authorization")
                manifest = _read_json(manifest_path)
                # Freeze emergency stop publication while the parent-loss
                # cohort is snapshotted, bound into pending evidence, and
                # archived. A late watchdog either publishes before this
                # transaction or waits, then observes the recovered active set.
                with _campaign_stop_publication_lock(out_dir):
                    return _authorize_dispatcher_process_lost(
                        out_dir, manifest, stop_path, auth_path,
                        prior_authorization)
            finally:
                portalocker.unlock(lease)
    prior_authorization = (
        _read_json(auth_path) if os.path.exists(auth_path) else None
    )
    if (prior_authorization is not None
            and prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2):
        raise RuntimeError("cannot supersede a non-V2 recovery authorization")
    manifest = _read_json(manifest_path)
    if deepseek_server_retry:
        if operator_pause or operator_interrupted_samples or provider_access_retry:
            raise RuntimeError(
                "DeepSeek server retry cannot be combined with other recovery modes")
        stop = _read_json(stop_path)
        return _authorize_deepseek_server_retry(
            out_dir, manifest, stop, stop_path, auth_path,
            prior_authorization)
    if provider_access_retry:
        if operator_pause or operator_interrupted_samples:
            raise RuntimeError(
                "provider-access retry cannot be combined with operator pause")
        stop = _read_json(stop_path)
        return _authorize_provider_access_retry(
            out_dir, manifest, stop, stop_path, auth_path,
            prior_authorization)
    if operator_pause:
        if os.path.exists(stop_path):
            raise RuntimeError(
                "operator pause recovery requires no pre-existing stop latch")
        reconciled_workers = _reconcile_terminal_active_workers(
            out_dir, manifest,
            operator_interrupted_samples=operator_interrupted_samples,
            operator_pause_reason=operator_pause_reason,
        )
        current_commit, current_tree, current_status = _git_identity_details()
        if current_tree != "clean":
            raise RuntimeError(
                "operator pause recovery requires a clean Git tree")
        stop = record_campaign_stop_condition(
            out_dir, "operator_directed_dispatcher_pause",
            reason=operator_pause_reason,
            stopped_git_commit=current_commit,
            stopped_git_tree_state=current_tree,
            git_status_porcelain=current_status,
            reconciled_worker_count=len(reconciled_workers),
            reconciled_worker_launch_ids=[
                item["worker_launch_id"] for item in reconciled_workers
            ],
            prior_recovery_authorization_id=(
                prior_authorization or {}).get("authorization_id"),
        )
        return _authorize_operator_pause(
            out_dir, manifest, stop, stop_path, auth_path,
            prior_authorization, reconciled_workers)
    stop = _read_json(stop_path)
    if ((manifest.get("config") or {}).get("method_phases")
            != ["hybridpatch", "fullrewrite"]
            or stop.get("condition") != "dispatcher_integrity_failure"):
        raise RuntimeError("campaign is not the stopped phased campaign")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = (manifest.get("config") or {}).get("samples") or []
    _assert_worker_leases_free(out_dir, samples)

    current_commit, current_tree = _git_identity()
    if current_tree != "clean":
        raise RuntimeError("recovery commit requires a clean Git tree")
    prior_commit = manifest.get("run_git_commit")
    if current_commit == prior_commit:
        raise RuntimeError("recovery code commit has not changed")

    dispatch_rows = _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
    phase_starts = [
        index for index, row in enumerate(dispatch_rows)
        if row.get("event") == "method_phase_start"
        and row.get("method_phase") == "fullrewrite"
    ]
    if not phase_starts:
        raise RuntimeError("expected an interrupted FullRewrite phase start")
    cohort_rows = dispatch_rows[phase_starts[0] + 1:]
    launches = [
        row for row in cohort_rows
        if row.get("event") == "launch"
        and row.get("method_phase") == "fullrewrite"
    ]
    worker_ids = [row.get("worker_launch_id") for row in launches]
    if (not worker_ids or len(worker_ids) != len(set(worker_ids))
            or any(not isinstance(item, str) or not item for item in worker_ids)):
        raise RuntimeError("interrupted FullRewrite worker cohort is invalid")
    worker_set = set(worker_ids)

    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    api_incidents = [
        (number, row) for number, row in enumerate(api_rows, 1)
        if row.get("worker_launch_id") in worker_set
        and row.get("method") == "fullrewrite"
        and row.get("classification") == "runner_exception"
        and row.get("error_type") == "AlreadyLocked"
        and row.get("provider_request_id") is None
        and row.get("total_tokens") is None
    ]
    if not api_incidents:
        raise RuntimeError("no exact ledger-lock API incidents found")
    api_incident_hashes = {
        _canonical_record_sha256(row) for _number, row in api_incidents
    }
    mapped_semantic_call_ids = {
        row.get("semantic_call_id") for row in api_rows
        if _canonical_record_sha256(row) not in api_incident_hashes
    }
    incident_roots = {row.get("semantic_root_id") for _n, row in api_incidents}
    attempt_rows = _read_jsonl(
        os.path.join(out_dir, "api_attempt_ledger.jsonl"))
    prior_attempt_entries = (
        prior_authorization.get("incident_attempt_rows") or []
        if prior_authorization else []
    )
    prior_attempt_by_number = {}
    for entry in prior_attempt_entries:
        number = entry.get("row_number") if isinstance(entry, dict) else None
        if (not isinstance(number, int) or isinstance(number, bool)
                or not 1 <= number <= len(attempt_rows)
                or entry.get("canonical_sha256")
                != _canonical_record_sha256(attempt_rows[number - 1])):
            raise RuntimeError("prior recovery attempt evidence has drifted")
        prior_attempt_by_number[number] = (
            attempt_rows[number - 1], entry.get("incident_kind"))
    lock_attempt_incidents = (
        [
            (number, row) for number, row in enumerate(attempt_rows, 1)
            if row.get("worker_launch_id") in worker_set
            and row.get("semantic_root_id") in incident_roots
        ]
        if not prior_authorization else [
            (number, row) for number, (row, kind)
            in prior_attempt_by_number.items()
            if kind == "ledger_lock"
        ]
    )
    if any(
            row.get("event") in {"generation_progress", "response_committed"}
            or (row.get("event") == "attempt_end"
                and (row.get("status") != "fatal_error"
                     or row.get("error_type") != "AlreadyLocked"
                     or row.get("generation_delta_seen") is not False))
            for _number, row in lock_attempt_incidents):
        raise RuntimeError("ledger-lock incident contains generated/committed output")

    prior_attempt_hashes = {
        entry.get("canonical_sha256") for entry in prior_attempt_entries
    }
    attempt_groups = {}
    for number, row in enumerate(attempt_rows, 1):
        if (row.get("worker_launch_id") in worker_set
                and _canonical_record_sha256(row) not in prior_attempt_hashes
                and str(row.get("semantic_call_id") or "").startswith(
                    "fullrewrite/")):
            attempt_groups.setdefault(
                row.get("semantic_call_id"), []).append((number, row))
    interrupted_attempt_groups = []
    for semantic_call_id, group in attempt_groups.items():
        starts = {
            row.get("attempt_index") for _number, row in group
            if row.get("event") == "attempt_start"
        }
        ends = {
            row.get("attempt_index") for _number, row in group
            if row.get("event") == "attempt_end"
        }
        pre_provider_interruption = (
            not starts and not ends
            and semantic_call_id not in mapped_semantic_call_ids
        )
        if starts - ends or pre_provider_interruption:
            if any(row.get("event") == "response_committed"
                   for _number, row in group):
                raise RuntimeError(
                    "interrupted attempt already committed a response")
            interrupted_attempt_groups.append((semantic_call_id, group))
    interrupted_attempt_incidents = [
        item for _semantic_call_id, group in interrupted_attempt_groups
        for item in group
    ]
    incident_attempt_by_number = dict(prior_attempt_by_number)
    if not prior_authorization:
        incident_attempt_by_number.update({
            number: (row, "ledger_lock")
            for number, row in lock_attempt_incidents
        })
    for number, row in interrupted_attempt_incidents:
        incident_attempt_by_number[number] = (
            row, "dispatcher_interrupted_open_attempt")
    attempt_incidents = [
        (number, *incident_attempt_by_number[number])
        for number in sorted(incident_attempt_by_number)
    ]
    if not prior_authorization:
        for root in incident_roots:
            digest = hashlib.sha256(root.encode("utf-8")).hexdigest()[:24]
            if os.path.exists(os.path.join(
                    out_dir, "api_journal", f"{digest}.response.json")):
                raise RuntimeError("ledger-lock incident has a response journal")
    for semantic_call_id, _group in interrupted_attempt_groups:
        digest = hashlib.sha256(
            semantic_call_id.encode("utf-8")).hexdigest()[:24]
        if os.path.exists(os.path.join(
                out_dir, "api_journal", f"{digest}.response.json")):
            raise RuntimeError("interrupted open attempt has a response journal")

    metadata = _read_jsonl(os.path.join(out_dir, "run_metadata.jsonl"))
    metadata_by_worker = {}
    for row in metadata:
        worker_id = row.get("worker_launch_id")
        if worker_id in worker_set:
            metadata_by_worker.setdefault(worker_id, []).append(row)
    exits_by_worker = {}
    authorizations_by_worker = {}
    launches_by_worker = {row.get("worker_launch_id"): row for row in launches}
    for row in cohort_rows:
        worker_id = row.get("worker_launch_id")
        if worker_id not in worker_set:
            continue
        if row.get("event") == "worker_exit":
            exits_by_worker.setdefault(worker_id, []).append(row)
        elif row.get("event") == "worker_authorized":
            authorizations_by_worker.setdefault(worker_id, []).append(row)
    preauthorization_worker_set = worker_set - set(metadata_by_worker)
    ordinary_worker_set = worker_set - preauthorization_worker_set
    api_worker_set = {
        row.get("worker_launch_id") for row in api_rows
        if row.get("worker_launch_id") in worker_set
    }
    attempt_worker_set = {
        row.get("worker_launch_id") for row in attempt_rows
        if row.get("worker_launch_id") in worker_set
    }
    ordinary_evidence_valid = all(
        len(metadata_by_worker.get(worker_id) or []) == 1
        and metadata_by_worker[worker_id][0].get("status") in {
            "failed", "interrupted_by_dispatcher"
        }
        and len(exits_by_worker.get(worker_id) or []) == 1
        and exits_by_worker[worker_id][0].get("disposition")
        == "campaign_fatal"
        for worker_id in ordinary_worker_set
    )
    preauthorization_evidence_valid = all(
        len(exits_by_worker.get(worker_id) or []) == 1
        and exits_by_worker[worker_id][0].get("sample")
        == launches_by_worker[worker_id].get("sample")
        and exits_by_worker[worker_id][0].get("pid")
        == launches_by_worker[worker_id].get("pid")
        and isinstance(
            exits_by_worker[worker_id][0].get("returncode"), int)
        and not isinstance(
            exits_by_worker[worker_id][0].get("returncode"), bool)
        and exits_by_worker[worker_id][0].get("returncode") != 0
        and exits_by_worker[worker_id][0].get("disposition")
        == "campaign_fatal"
        and worker_id not in authorizations_by_worker
        and worker_id not in api_worker_set
        and worker_id not in attempt_worker_set
        for worker_id in preauthorization_worker_set
    )
    expected_preauthorization_errors = {
        "worker " + str(launches_by_worker[worker_id].get("sample"))
        + " exited before authorization with "
        + str(exits_by_worker[worker_id][0].get("returncode"))
        for worker_id in preauthorization_worker_set
        if len(exits_by_worker.get(worker_id) or []) == 1
    }
    if (set(exits_by_worker) != worker_set
            or not ordinary_evidence_valid
            or not preauthorization_evidence_valid
            or bool(preauthorization_worker_set)
            != (stop.get("error") in expected_preauthorization_errors)):
        raise RuntimeError("interrupted worker terminal evidence is incomplete")

    authorization_id = (
        "ledger-lock-" + datetime.now().astimezone().strftime(
            "%Y%m%dT%H%M%S%z") + "-" + uuid.uuid4().hex[:8]
    )
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    prior_fingerprint = manifest.get("code_fingerprint")
    recovery_fingerprint = code_fingerprint()
    if not isinstance(prior_fingerprint, dict):
        raise RuntimeError("dispatch manifest code fingerprint is invalid")
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    changed_paths = _git_changed_paths(prior_commit, current_commit)
    required_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    allowed_paths = required_paths | {
        "HP_V8/VERSION.md", "docs/active_log.md",
    }
    if (fingerprint_changes != ["run_meta.py"]
            or not required_paths <= set(changed_paths)
            or not set(changed_paths) <= allowed_paths):
        raise RuntimeError("recovery commit scope is not the lock hotfix")

    os.makedirs(history_dir, exist_ok=False)
    archived_authorization = None
    if prior_authorization is not None:
        archived_authorization = os.path.join(
            history_dir, "superseded_campaign_recovery_authorization.json")
        os.replace(auth_path, archived_authorization)
    archived_stop, _archived_emergency = (
        _archive_campaign_stop_cohort(
            out_dir, stop_path, history_dir))
    relative_stop = os.path.relpath(archived_stop, out_dir).replace("\\", "/")

    record = {
        "schema": CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
        "authorization_id": authorization_id,
        "recovery_kind": LEDGER_LOCK_RECOVERY_KIND,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "authorization_basis": "explicit_user_resume_after_shared_ledger_lock_contention",
        "dispatch_manifest_sha256": _sha256_file(manifest_path),
        "prior_git_commit": prior_commit,
        "prior_git_tree_state": "clean",
        "prior_code_fingerprint": prior_fingerprint,
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": recovery_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": relative_stop,
        "archived_stop_sha256": _sha256_file(archived_stop),
        "incident_api_rows": [
            _incident_entry(number, row, "ledger_lock")
            for number, row in api_incidents
        ],
        "incident_attempt_rows": [
            _incident_entry(number, row, incident_kind)
            for number, row, incident_kind in attempt_incidents
        ],
        "interrupted_open_semantic_call_count": len(
            interrupted_attempt_groups),
        "interrupted_pre_provider_semantic_call_count": sum(
            not any(row.get("event") == "attempt_start"
                    for _number, row in group)
            for _semantic_call_id, group in interrupted_attempt_groups
        ),
        "interrupted_open_generation_delta_count": sum(
            any(row.get("event") == "generation_progress"
                for _number, row in group)
            for _semantic_call_id, group in interrupted_attempt_groups
        ),
        "recovered_worker_launch_ids": sorted(worker_set),
        "preauthorization_worker_launch_ids": sorted(
            preauthorization_worker_set),
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    }
    if archived_authorization is not None:
        record["superseded_authorization_path"] = os.path.relpath(
            archived_authorization, out_dir).replace("\\", "/")
        record["superseded_authorization_sha256"] = _sha256_file(
            archived_authorization)
    write_json_atomic(auth_path, record)
    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(out_dir)
    append_jsonl_locked(os.path.join(out_dir, "dispatch_log.jsonl"), {
        "event": "user_authorized_ledger_lock_recovery",
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "campaign_recovery_authorization_id": authorization_id,
        "campaign_recovery_authorization_sha256": verified[
            "authorization_sha256"],
        "incident_api_row_count": len(api_incidents),
        "incident_attempt_row_count": len(attempt_incidents),
        "recovered_worker_count": len(worker_set),
        "preauthorization_worker_count": len(
            preauthorization_worker_set),
    })
    return {
        "authorization_id": authorization_id,
        "authorization_sha256": verified["authorization_sha256"],
        "incident_api_rows": len(api_incidents),
        "incident_attempt_rows": len(attempt_incidents),
        "interrupted_open_semantic_calls": len(
            interrupted_attempt_groups),
        "recovered_workers": len(worker_set),
        "preauthorization_workers": len(
            preauthorization_worker_set),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out_dir", required=True)
    parser.add_argument("--confirm_workers_stopped", action="store_true")
    parser.add_argument("--operator_dispatcher_pause", action="store_true")
    parser.add_argument("--operator_pause_reason")
    parser.add_argument(
        "--operator_interrupted_sample", action="append", default=[])
    parser.add_argument("--provider_access_retry", action="store_true")
    parser.add_argument("--deepseek_server_retry", action="store_true")
    parser.add_argument("--dispatcher_process_lost", action="store_true")
    args = parser.parse_args()
    if not args.confirm_workers_stopped:
        parser.error("--confirm_workers_stopped is required")
    if args.operator_dispatcher_pause and not args.operator_pause_reason:
        parser.error(
            "--operator_dispatcher_pause requires --operator_pause_reason")
    if args.operator_interrupted_sample and not args.operator_dispatcher_pause:
        parser.error(
            "--operator_interrupted_sample requires "
            "--operator_dispatcher_pause")
    print(json.dumps(authorize(
        args.out_dir,
        operator_pause=args.operator_dispatcher_pause,
        operator_pause_reason=args.operator_pause_reason,
        operator_interrupted_samples=args.operator_interrupted_sample,
        provider_access_retry=args.provider_access_retry,
        deepseek_server_retry=args.deepseek_server_retry,
        dispatcher_process_lost=args.dispatcher_process_lost,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
