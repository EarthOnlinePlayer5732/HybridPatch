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
import uuid

from paired_campaign_dispatch import (
    _actual_sample_progress,
    _assert_worker_leases_free,
    _audit_running_invocation_provenance,
    _verified_evaluator_incomplete,
    _verified_infrastructure_incomplete,
    _write_active_worker_set,
)
from run_meta import (
    CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
    CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
    LEDGER_LOCK_RECOVERY_KIND,
    _canonical_record_sha256,
    _git_identity,
    _git_identity_details,
    _sha256_file,
    append_jsonl_locked,
    campaign_recovery_incident_evidence,
    code_fingerprint,
    interrupt_audited_running_invocations,
    read_run_metadata_snapshot,
    read_sample_outcomes,
    record_campaign_stop_condition,
    read_campaign_recovery_authorization,
    write_json_atomic,
)


def _read_json(path):
    with open(path, encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


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
            latest[worker_id] = row
    return latest


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
    if not reconciled_workers:
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
    for item in list(
            (prior_authorization or {}).get(
                "operator_pause_reconciled_workers") or []) + list(
                    reconciled_workers or []):
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
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    archived_authorization = os.path.join(
        history_dir, "superseded_campaign_recovery_authorization.json")
    os.replace(auth_path, archived_authorization)
    os.replace(stop_path, archived_stop)

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
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    archived_authorization = os.path.join(
        history_dir, "superseded_campaign_recovery_authorization.json")
    os.replace(auth_path, archived_authorization)
    os.replace(stop_path, archived_stop)

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


def authorize(
        out_dir, *, operator_pause=False, operator_pause_reason=None,
        operator_interrupted_samples=None, provider_access_retry=False):
    out_dir = os.path.abspath(out_dir)
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    stop_path = os.path.join(out_dir, "campaign_stop.json")
    auth_path = os.path.join(
        out_dir, CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
    prior_authorization = (
        _read_json(auth_path) if os.path.exists(auth_path) else None
    )
    if (prior_authorization is not None
            and prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2):
        raise RuntimeError("cannot supersede a non-V2 recovery authorization")
    manifest = _read_json(manifest_path)
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
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    archived_authorization = None
    if prior_authorization is not None:
        archived_authorization = os.path.join(
            history_dir, "superseded_campaign_recovery_authorization.json")
        os.replace(auth_path, archived_authorization)
    os.replace(stop_path, archived_stop)
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
    ), sort_keys=True))


if __name__ == "__main__":
    main()
