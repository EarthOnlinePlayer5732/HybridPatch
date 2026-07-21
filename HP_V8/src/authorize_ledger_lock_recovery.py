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

from paired_campaign_dispatch import _assert_worker_leases_free
from run_meta import (
    CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
    CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
    LEDGER_LOCK_RECOVERY_KIND,
    _canonical_record_sha256,
    _git_identity,
    _sha256_file,
    append_jsonl_locked,
    campaign_recovery_incident_evidence,
    code_fingerprint,
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


def authorize(out_dir):
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
    args = parser.parse_args()
    if not args.confirm_workers_stopped:
        parser.error("--confirm_workers_stopped is required")
    print(json.dumps(authorize(args.out_dir), sort_keys=True))


if __name__ == "__main__":
    main()
