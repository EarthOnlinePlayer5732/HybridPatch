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
import re
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
    _queued_pending_evidence,
    _valid_deepseek_transport_sidecar,
    _verified_evaluator_incomplete,
    _verified_deepseek_infrastructure_incomplete,
    _verified_infrastructure_incomplete,
    _write_active_worker_set,
)
from run_meta import (
    CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
    CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
    DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
    DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
    EMERGENCY_STOP_DIRECTORY,
    LEDGER_LOCK_RECOVERY_KIND,
    METADATA_EVENTS_FILENAME,
    STOP_CONDITION_SCHEMA,
    _canonical_record_sha256,
    _capture_run_metadata_recovery_snapshot,
    _campaign_stop_publication_lock,
    _deepseek_dispatcher_stopped_sidecar_evidence,
    _git_identity,
    _git_identity_details,
    _read_deepseek_transport_sidecar,
    _sha256_file,
    _run_metadata_recovery_identities,
    _write_jsonl_atomic,
    _validate_deepseek_compact_records,
    _validate_deepseek_linear_records,
    _validate_dispatcher_parent_loss_pending_reprepare_witnesses,
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
from campaign_recovery_runtime import (
    _append_recoverable_jsonl_tail,
    _apply_deepseek_parent_loss_recovery_plan,
    _assert_dispatcher_parent_loss_path_budget,
    _assert_recoverable_jsonl_tail,
    _copy_file_durable,
    _deepseek_parent_loss_incidents,
    _ensure_bound_file_copy,
    _file_prefix_evidence,
    _git_changed_paths,
    _incident_entry,
    _merge_incident_entries,
    _merge_transport_sidecar_entries,
    _read_json,
    _read_jsonl,
    _reconcile_deepseek_parent_loss_workers,
    _record_stopless_registered_prelaunch_parent_loss,
    _recoverable_jsonl_tail,
    _replace_with_sharing_retry,
    _unlink_with_sharing_retry,
    _write_json_no_replace,
)

_DISPATCHER_PARENT_LOSS_PENDING_FILENAME = (
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME
)
_DISPATCHER_PARENT_LOSS_PENDING_SCHEMA = (
    "anchorpatch.dispatcher_parent_loss_recovery/1"
)
_DISPATCHER_PARENT_LOSS_HISTORY_ROOT = "r"
_DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA = (
    "anchorpatch.deepseek_transport_inspector_recovery/1"
)
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
_DISPATCHER_PARENT_LOSS_PENDING_REPREPARE_CHANGED_PATHS = {
    "HP_V8/VERSION.md",
    "HP_V8/src/authorize_ledger_lock_recovery.py",
    "HP_V8/src/test_model_openai.py",
    "docs/active_log.md",
}


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

def _validated_dispatcher_parent_loss_code_transition(
        manifest, manifest_digest, auth_path, prior_authorization,
        validated_prior_authorization_sha256, current_commit, current_tree,
        current_fingerprint):
    """Bind one parent-loss recovery-tool fix to the exact prior authority."""
    prior_commit = manifest.get("run_git_commit")
    prior_fingerprint = manifest.get("code_fingerprint")
    prior_recovery_commit = prior_authorization.get(
        "recovery_git_commit")
    prior_recovery_fingerprint = prior_authorization.get(
        "recovery_code_fingerprint")
    actual_authorization_sha256 = _sha256_file(auth_path)
    if (not isinstance(validated_prior_authorization_sha256, str)
            or re.fullmatch(
                r"[0-9a-f]{64}",
                validated_prior_authorization_sha256,
            ) is None
            or actual_authorization_sha256
            != validated_prior_authorization_sha256
            or prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or not isinstance(
                prior_authorization.get("authorization_id"), str)
            or not prior_authorization.get("authorization_id")
            or prior_authorization.get("recovery_kind") not in {
                LEDGER_LOCK_RECOVERY_KIND,
                DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
            }
            or prior_authorization.get("dispatch_manifest_sha256")
            != manifest_digest
            or prior_authorization.get("prior_git_commit") != prior_commit
            or prior_authorization.get("prior_git_tree_state") != "clean"
            or prior_authorization.get("prior_code_fingerprint")
            != prior_fingerprint
            or re.fullmatch(
                r"[0-9a-f]{40}",
                str(prior_recovery_commit or ""),
            ) is None
            or prior_authorization.get("recovery_git_tree_state")
            != "clean"
            or not isinstance(prior_recovery_fingerprint, dict)
            or re.fullmatch(
                r"[0-9a-f]{40}", str(current_commit or "")) is None
            or current_tree != "clean"
            or not isinstance(current_fingerprint, dict)):
        raise RuntimeError(
            "dispatcher parent-loss prior authorization transition is "
            "invalid")

    parent_line = subprocess.run(
        ["git", "rev-list", "--parents", "-n", "1", current_commit],
        check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.split()
    changed_paths = _git_changed_paths(
        prior_recovery_commit, current_commit)
    fingerprint_changes = sorted(
        key for key in set(prior_recovery_fingerprint) | set(
            current_fingerprint)
        if prior_recovery_fingerprint.get(key)
        != current_fingerprint.get(key)
    )
    if (parent_line != [current_commit, prior_recovery_commit]
            or set(changed_paths)
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            or fingerprint_changes
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS):
        raise RuntimeError(
            "dispatcher parent-loss recovery-tool transition is invalid")
    return {
        "prior_authorization_id": prior_authorization["authorization_id"],
        "prior_authorization_sha256": actual_authorization_sha256,
        "prior_recovery_git_commit": prior_recovery_commit,
        "prior_recovery_code_fingerprint": prior_recovery_fingerprint,
        "delta_changed_paths": changed_paths,
        "delta_changed_code_fingerprint_keys": fingerprint_changes,
    }


def _validate_dispatcher_parent_loss_pending_witness(
        out_dir, pending, record, history_relative, witness_kind):
    """Validate one exact pending-journal amendment witness."""
    if witness_kind == "validator":
        prefix = "dispatcher_parent_loss_pending_validator_reprepared_"
        filename = "pending.validator-before.json"
        timestamp_field = "validator_reprepared_at"
    elif witness_kind == "api_validator":
        prefix = (
            "dispatcher_parent_loss_pending_api_validator_reprepared_")
        filename = "pending.api-validator-before.json"
        timestamp_field = "api_validator_reprepared_at"
    else:
        raise RuntimeError(
            "dispatcher parent-loss pending witness kind is invalid")
    provenance_fields = {
        prefix + suffix
        for suffix in ("from_commit", "from_sha256", "from_path", "at")
    }
    witness_relative = history_relative + "/" + filename
    witness_commit = record.get(prefix + "from_commit")
    witness_sha256 = record.get(prefix + "from_sha256")
    witness_timestamp = record.get(prefix + "at")
    if ({
            key for key in record if key.startswith(prefix)
        } != provenance_fields
            or re.fullmatch(
                r"[0-9a-f]{40}", str(witness_commit or "")) is None
            or re.fullmatch(
                r"[0-9a-f]{64}", str(witness_sha256 or "")) is None
            or record.get(prefix + "from_path") != witness_relative
            or not isinstance(witness_timestamp, str)
            or not witness_timestamp
            or pending.get(timestamp_field) != witness_timestamp):
        raise RuntimeError(
            "dispatcher parent-loss pending witness provenance is invalid")
    witness_path = os.path.realpath(os.path.join(
        out_dir, witness_relative))
    try:
        witness_inside = (
            os.path.commonpath([out_dir, witness_path]) == out_dir)
    except ValueError:
        witness_inside = False
    if (not witness_inside
            or not os.path.isfile(witness_path)
            or _sha256_file(witness_path) != witness_sha256):
        raise RuntimeError(
            "dispatcher parent-loss pending witness digest mismatch")
    witness = _read_json(witness_path)
    witness_record = witness.get("authorization_record")
    if (witness.get("schema") != pending.get("schema")
            or witness.get("history_dir") != history_relative
            or witness.get("created_at") != pending.get("created_at")
            or witness.get("recovery_plan") != pending.get("recovery_plan")
            or not isinstance(witness_record, dict)
            or witness_record.get("authorization_id")
            != record.get("authorization_id")
            or witness_record.get("recovery_git_commit")
            != witness_commit):
        raise RuntimeError(
            "dispatcher parent-loss pending witness identity mismatch")

    expected = json.loads(json.dumps(witness))
    expected_record = expected["authorization_record"]
    for field in (
            "recovery_git_commit",
            "recovery_code_fingerprint",
            "changed_tracked_paths",
            "changed_code_fingerprint_keys",
            "dispatcher_parent_loss_code_transition"):
        expected_record[field] = json.loads(json.dumps(record.get(field)))
    if witness_kind == "validator":
        phase_flags = (
            "deepseek_transport_disconnect_"
            "inspector_followup_recovery",
            "deepseek_resume_classifier_followup_recovery",
        )
        if any(witness_record.get(name) is not True for name in phase_flags):
            raise RuntimeError(
                "dispatcher parent-loss pending validator witness phase "
                "flags are invalid")
        for name in phase_flags:
            expected_record.pop(name, None)
    for field in provenance_fields:
        expected_record[field] = record[field]
    expected[timestamp_field] = witness_timestamp
    if expected != pending:
        raise RuntimeError(
            "dispatcher parent-loss pending witness transition is invalid")
    if witness_kind == "api_validator":
        _validate_dispatcher_parent_loss_pending_witness(
            out_dir,
            witness,
            witness_record,
            history_relative,
            "validator",
        )
    return witness


def _reprepare_dispatcher_parent_loss_pending_short_path(
        out_dir, manifest, pending_path, pending, auth_path, stop_path):
    """Rebind the one legacy long-path pending transaction without replay."""
    record = (
        pending.get("authorization_record")
        if isinstance(pending, dict) else None
    )
    recovery_plan = (
        pending.get("recovery_plan")
        if isinstance(pending, dict) else None
    )
    if (not isinstance(record, dict)
            or not isinstance(recovery_plan, dict)
            or pending.get("schema")
            != _DISPATCHER_PARENT_LOSS_PENDING_SCHEMA
            or record.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or record.get("recovery_kind")
            != DISPATCHER_PROCESS_LOST_RECOVERY_KIND
            or record.get("dispatcher_process_lost_recovery") is not True
            or record.get("recovery_git_tree_state") != "clean"
            or record.get("prior_git_commit")
            != manifest.get("run_git_commit")
            or record.get("prior_code_fingerprint")
            != manifest.get("code_fingerprint")
            or record.get("dispatcher_pid")
            != recovery_plan.get("dispatcher_pid")
            or record.get("dispatcher_instance_id")
            != recovery_plan.get("dispatcher_instance_id")
            or record.get("dispatcher_parent_loss_workers")
            != recovery_plan.get("workers")):
        raise RuntimeError(
            "dispatcher parent-loss pending recovery is invalid")

    authorization_id = record.get("authorization_id")
    history_relative = pending.get("history_dir")
    if (not isinstance(authorization_id, str)
            or not authorization_id
            or authorization_id in {".", ".."}
            or "/" in authorization_id
            or "\\" in authorization_id):
        raise RuntimeError(
            "dispatcher parent-loss pending history path is invalid")
    active_path = os.path.join(out_dir, "active_worker_set.json")
    active = (
        _read_json(active_path) if os.path.isfile(active_path) else None
    )
    expected_active_workers = {
        item.get("worker_launch_id"): {"sample": item.get("sample")}
        for item in recovery_plan.get("workers") or []
        if isinstance(item, dict)
    }
    active_is_reconciled = (
        active.get("dispatcher_pid") is None
        and active.get("dispatcher_instance_id") is None
        and active.get("workers") == {}
    ) if isinstance(active, dict) else False
    active_is_original_cohort = (
        active.get("dispatcher_pid") == recovery_plan.get("dispatcher_pid")
        and active.get("dispatcher_instance_id")
        == recovery_plan.get("dispatcher_instance_id")
        and active.get("workers") == expected_active_workers
        and len(expected_active_workers)
        == len(recovery_plan.get("workers") or [])
    ) if isinstance(active, dict) else False
    if (not isinstance(active, dict)
            or active.get("schema")
            != "anchorpatch.active_worker_set/1"
            or active.get("run_git_commit")
            != manifest.get("run_git_commit")
            or not (active_is_reconciled or active_is_original_cohort)):
        raise RuntimeError(
            "dispatcher parent-loss pending active set is invalid")
    short_pending = (
        re.fullmatch(r"dpl-[0-9a-f]{12}", authorization_id)
        is not None
    )
    if short_pending:
        if history_relative != (
                _DISPATCHER_PARENT_LOSS_HISTORY_ROOT
                + "/" + authorization_id):
            raise RuntimeError(
                "dispatcher parent-loss pending history path is invalid")
    elif (not authorization_id.startswith("dispatcher-parent-loss-")
          or history_relative
          != "recovery_history/" + authorization_id):
        raise RuntimeError(
            "dispatcher parent-loss pending authorization ID is invalid")

    current_commit, current_tree = _git_identity()
    current_fingerprint = code_fingerprint()
    prepared_commit = record.get("recovery_git_commit")
    prepared_fingerprint = record.get("recovery_code_fingerprint")
    inherited_inspector_flags = {
        name: record.get(name)
        for name in (
            "deepseek_transport_disconnect_inspector_followup_recovery",
            "deepseek_resume_classifier_followup_recovery",
        )
    }
    if (short_pending
            and current_commit == prepared_commit
            and current_fingerprint == prepared_fingerprint
            and all(
                name not in record
                for name in inherited_inspector_flags
            )):
        api_prefix = (
            "dispatcher_parent_loss_pending_api_validator_reprepared_")
        validator_prefix = (
            "dispatcher_parent_loss_pending_validator_reprepared_")
        if ("api_validator_reprepared_at" in pending
                or any(key.startswith(api_prefix) for key in record)):
            _validate_dispatcher_parent_loss_pending_witness(
                out_dir,
                pending,
                record,
                history_relative,
                "api_validator",
            )
        elif ("validator_reprepared_at" in pending
              or any(key.startswith(validator_prefix) for key in record)):
            _validate_dispatcher_parent_loss_pending_witness(
                out_dir,
                pending,
                record,
                history_relative,
                "validator",
            )
        return pending
    prior_fingerprint = record.get("prior_code_fingerprint")
    transition = record.get("dispatcher_parent_loss_code_transition")
    transition_keys = {
        "prior_authorization_id",
        "prior_authorization_sha256",
        "prior_recovery_git_commit",
        "prior_recovery_code_fingerprint",
        "delta_changed_paths",
        "delta_changed_code_fingerprint_keys",
    }
    prior_recovery_commit = (
        transition.get("prior_recovery_git_commit")
        if isinstance(transition, dict) else None
    )
    prior_recovery_fingerprint = (
        transition.get("prior_recovery_code_fingerprint")
        if isinstance(transition, dict) else None
    )
    if (current_tree != "clean"
            or re.fullmatch(
                r"[0-9a-f]{40}", str(current_commit or "")) is None
            or re.fullmatch(
                r"[0-9a-f]{40}", str(prepared_commit or "")) is None
            or current_commit == prepared_commit
            or not isinstance(prepared_fingerprint, dict)
            or not isinstance(current_fingerprint, dict)
            or not isinstance(prior_fingerprint, dict)
            or not isinstance(transition, dict)
            or set(transition) != transition_keys
            or re.fullmatch(
                r"[0-9a-f]{40}",
                str(prior_recovery_commit or ""),
            ) is None
            or not isinstance(prior_recovery_fingerprint, dict)
            or transition.get("delta_changed_paths")
            != sorted(
                _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS)
            or transition.get("delta_changed_code_fingerprint_keys")
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS):
        raise RuntimeError(
            "dispatcher parent-loss pending code transition is invalid")

    def _parent_line(commit):
        return subprocess.run(
            ["git", "rev-list", "--parents", "-n", "1", commit],
            check=True, capture_output=True, text=True, encoding="utf-8",
        ).stdout.split()

    prepared_delta_paths = _git_changed_paths(
        prior_recovery_commit, prepared_commit)
    current_delta_paths = _git_changed_paths(
        prior_recovery_commit, current_commit)
    amendment_paths = _git_changed_paths(
        prepared_commit, current_commit)
    cumulative_prepared_paths = _git_changed_paths(
        record.get("prior_git_commit"), prepared_commit)
    cumulative_current_paths = _git_changed_paths(
        record.get("prior_git_commit"), current_commit)
    prepared_to_current_fingerprint_changes = sorted(
        key for key in set(prepared_fingerprint) | set(
            current_fingerprint)
        if prepared_fingerprint.get(key)
        != current_fingerprint.get(key)
    )
    prior_to_current_fingerprint_changes = sorted(
        key for key in set(prior_recovery_fingerprint) | set(
            current_fingerprint)
        if prior_recovery_fingerprint.get(key)
        != current_fingerprint.get(key)
    )
    cumulative_fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(
            current_fingerprint)
        if prior_fingerprint.get(key)
        != current_fingerprint.get(key)
    )
    phase_flag_amendment = (
        set(amendment_paths)
        == _DISPATCHER_PARENT_LOSS_PENDING_REPREPARE_CHANGED_PATHS
        and not prepared_to_current_fingerprint_changes
    )
    api_validator_amendment = (
        set(amendment_paths)
        == _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
        and prepared_to_current_fingerprint_changes
        == _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS
    )
    if (_parent_line(prepared_commit)
            != [prepared_commit, prior_recovery_commit]
            or _parent_line(current_commit)
            != [current_commit, prior_recovery_commit]
            or set(prepared_delta_paths)
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            or set(current_delta_paths)
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            or not (phase_flag_amendment or api_validator_amendment)
            or prior_to_current_fingerprint_changes
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS
            or cumulative_prepared_paths
            != record.get("changed_tracked_paths")
            or cumulative_current_paths != cumulative_prepared_paths
            or cumulative_fingerprint_changes
            != record.get("changed_code_fingerprint_keys")):
        raise RuntimeError(
            "dispatcher parent-loss pending tooling amendment is invalid")

    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if short_pending:
        phase_flag_repair = (
            phase_flag_amendment
            and inherited_inspector_flags == {
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery": True,
                "deepseek_resume_classifier_followup_recovery": True,
            }
        )
        api_validator_repair = (
            api_validator_amendment
            and all(
                name not in record
                for name in inherited_inspector_flags
            )
            and "api_validator_reprepared_at" not in pending
            and not any(
                key.startswith(
                    "dispatcher_parent_loss_pending_api_validator_"
                    "reprepared_")
                for key in record
            )
        )
        if not (phase_flag_repair or api_validator_repair):
            raise RuntimeError(
                "dispatcher parent-loss pending tooling amendment profile "
                "is invalid")
        history_dir = os.path.realpath(os.path.join(
            out_dir, history_relative))
        try:
            history_inside = (
                os.path.commonpath([out_dir, history_dir]) == out_dir)
        except ValueError:
            history_inside = False
        archived_stop_relative = (
            history_relative + "/campaign_stop.json")
        archived_active_relative = (
            history_relative + "/active_worker_set.before.json")
        archived_metadata_relative = (
            history_relative + "/run_metadata.before.jsonl")
        superseded_relative = (
            history_relative
            + "/superseded_campaign_recovery_authorization.json")
        bound_files = {
            archived_stop_relative: record.get("archived_stop_sha256"),
            archived_active_relative: record.get(
                "archived_active_worker_set_sha256"),
            archived_metadata_relative: record.get(
                "archived_run_metadata_sha256"),
            superseded_relative: record.get(
                "superseded_authorization_sha256"),
        }
        if (not history_inside
                or not os.path.isdir(history_dir)
                or _sha256_file(manifest_path)
                != record.get("dispatch_manifest_sha256")
                or _read_json(auth_path) != record
                or record.get("archived_stop_path")
                != archived_stop_relative
                or record.get("archived_active_worker_set_path")
                != archived_active_relative
                or record.get("archived_run_metadata_path")
                != archived_metadata_relative
                or record.get("superseded_authorization_path")
                != superseded_relative
                or os.path.isfile(stop_path)):
            raise RuntimeError(
                "dispatcher parent-loss archived pending state is invalid")
        for relative, expected_sha256 in bound_files.items():
            path = os.path.join(out_dir, relative)
            if (not isinstance(expected_sha256, str)
                    or len(expected_sha256) != 64
                    or not os.path.isfile(path)
                    or _sha256_file(path) != expected_sha256):
                raise RuntimeError(
                    "dispatcher parent-loss archived pending digest "
                    "mismatch")
        superseded = _read_json(os.path.join(
            out_dir, superseded_relative))
        if (record.get("superseded_authorization_sha256")
                != transition.get("prior_authorization_sha256")
                or superseded.get("authorization_id")
                != transition.get("prior_authorization_id")
                or superseded.get("recovery_git_commit")
                != prior_recovery_commit
                or superseded.get("recovery_code_fingerprint")
                != prior_recovery_fingerprint):
            raise RuntimeError(
                "dispatcher parent-loss pending prior authorization "
                "mismatch")
        if api_validator_repair:
            _validate_dispatcher_parent_loss_pending_witness(
                out_dir,
                pending,
                record,
                history_relative,
                "validator",
            )

        emergency_source_dir = os.path.join(
            out_dir, EMERGENCY_STOP_DIRECTORY)
        source_names = (
            sorted(
                name for name in os.listdir(emergency_source_dir)
                if name.endswith(".json")
            )
            if os.path.isdir(emergency_source_dir) else []
        )
        archived_emergency_dir = os.path.join(
            history_dir, EMERGENCY_STOP_DIRECTORY)
        actual_archived_names = (
            sorted(
                name for name in os.listdir(archived_emergency_dir)
                if name.endswith(".json")
            )
            if os.path.isdir(archived_emergency_dir) else []
        )
        expected_archived_names = []
        emergency_entries = record.get(
            "archived_emergency_stop_records")
        if source_names or not isinstance(emergency_entries, list):
            raise RuntimeError(
                "dispatcher parent-loss archived emergency stop scope is "
                "invalid")
        for entry in emergency_entries:
            relative = (
                entry.get("path") if isinstance(entry, dict) else None
            )
            expected_sha256 = (
                entry.get("sha256") if isinstance(entry, dict) else None
            )
            if (not isinstance(relative, str)
                    or not relative.startswith(
                        history_relative + "/"
                        + EMERGENCY_STOP_DIRECTORY + "/")
                    or "/" in relative.rsplit("/", 1)[-1]
                    or "\\" in relative
                    or not isinstance(expected_sha256, str)
                    or len(expected_sha256) != 64):
                raise RuntimeError(
                    "dispatcher parent-loss archived emergency stop scope "
                    "is invalid")
            name = relative.rsplit("/", 1)[-1]
            path = os.path.join(out_dir, relative)
            if (not os.path.isfile(path)
                    or _sha256_file(path) != expected_sha256):
                raise RuntimeError(
                    "dispatcher parent-loss archived emergency stop digest "
                    "mismatch")
            expected_archived_names.append(name)
        if (len(expected_archived_names)
                != len(set(expected_archived_names))
                or sorted(expected_archived_names)
                != actual_archived_names):
            raise RuntimeError(
                "dispatcher parent-loss archived emergency stop scope is "
                "invalid")

        old_pending_sha256 = _sha256_file(pending_path)
        old_pending_archive_name = (
            "pending.validator-before.json"
            if phase_flag_repair
            else "pending.api-validator-before.json"
        )
        old_pending_archive_relative = (
            history_relative + "/" + old_pending_archive_name)
        _ensure_bound_file_copy(
            pending_path,
            os.path.join(out_dir, old_pending_archive_relative),
            old_pending_sha256,
        )
        updated = json.loads(json.dumps(pending))
        updated_record = updated["authorization_record"]
        updated_record["recovery_git_commit"] = current_commit
        updated_record["recovery_code_fingerprint"] = current_fingerprint
        updated_record["changed_tracked_paths"] = cumulative_current_paths
        updated_record["changed_code_fingerprint_keys"] = (
            cumulative_fingerprint_changes)
        if phase_flag_repair:
            for name in inherited_inspector_flags:
                updated_record.pop(name, None)
        updated_transition = updated_record[
            "dispatcher_parent_loss_code_transition"]
        updated_transition["delta_changed_paths"] = current_delta_paths
        updated_transition[
            "delta_changed_code_fingerprint_keys"
        ] = prior_to_current_fingerprint_changes
        reprepared_at = datetime.now().astimezone().isoformat(
            timespec="seconds")
        prefix = (
            "dispatcher_parent_loss_pending_validator_reprepared_"
            if phase_flag_repair
            else "dispatcher_parent_loss_pending_api_validator_reprepared_"
        )
        updated_record[prefix + "from_commit"] = prepared_commit
        updated_record[prefix + "from_sha256"] = old_pending_sha256
        updated_record[prefix + "from_path"] = (
            old_pending_archive_relative)
        updated_record[prefix + "at"] = reprepared_at
        updated[
            "validator_reprepared_at"
            if phase_flag_repair
            else "api_validator_reprepared_at"
        ] = reprepared_at
        write_json_atomic(pending_path, updated)
        return _read_json(pending_path)

    if not phase_flag_amendment:
        raise RuntimeError(
            "dispatcher parent-loss legacy pending tooling amendment is "
            "invalid")

    old_history_dir = os.path.realpath(os.path.join(
        out_dir, history_relative))
    try:
        old_history_inside = (
            os.path.commonpath([out_dir, old_history_dir]) == out_dir)
    except ValueError:
        old_history_inside = False
    archived_active_relative = (
        history_relative + "/active_worker_set.before.json")
    archived_metadata_relative = (
        history_relative + "/run_metadata.before.jsonl")
    superseded_relative = (
        history_relative
        + "/superseded_campaign_recovery_authorization.json")
    archived_stop_relative = history_relative + "/campaign_stop.json"
    old_emergency_relative = (
        history_relative + "/" + EMERGENCY_STOP_DIRECTORY)
    old_emergency_dir = os.path.join(
        old_history_dir, EMERGENCY_STOP_DIRECTORY)
    required_history_files = {
        "active_worker_set.before.json",
        "run_metadata.before.jsonl",
        "superseded_campaign_recovery_authorization.json",
    }
    allowed_history_entries = required_history_files | {
        EMERGENCY_STOP_DIRECTORY,
        "pending.json",
        ".recovery-copy.pending",
    }
    history_entries = (
        set(os.listdir(old_history_dir))
        if os.path.isdir(old_history_dir) else set()
    )
    auth_record = (
        _read_json(auth_path) if os.path.isfile(auth_path) else None
    )
    if (not old_history_inside
            or not os.path.isdir(old_history_dir)
            or not required_history_files <= history_entries
            or history_entries - allowed_history_entries
            or (os.path.isdir(old_emergency_dir)
                and os.listdir(old_emergency_dir))
            or (os.path.exists(old_emergency_dir)
                and not os.path.isdir(old_emergency_dir))
            or auth_record != record
            or _sha256_file(manifest_path)
            != record.get("dispatch_manifest_sha256")
            or record.get("archived_active_worker_set_path")
            != archived_active_relative
            or record.get("archived_run_metadata_path")
            != archived_metadata_relative
            or record.get("superseded_authorization_path")
            != superseded_relative
            or record.get("archived_stop_path")
            != archived_stop_relative):
        raise RuntimeError(
            "dispatcher parent-loss pending partial transaction is invalid")

    bound_history_files = {
        archived_active_relative: record.get(
            "archived_active_worker_set_sha256"),
        archived_metadata_relative: record.get(
            "archived_run_metadata_sha256"),
        superseded_relative: record.get(
            "superseded_authorization_sha256"),
    }
    for relative, expected_sha256 in bound_history_files.items():
        path = os.path.join(out_dir, relative)
        if (not isinstance(expected_sha256, str)
                or len(expected_sha256) != 64
                or not os.path.isfile(path)
                or _sha256_file(path) != expected_sha256):
            raise RuntimeError(
                "dispatcher parent-loss pending history digest mismatch")
    superseded = _read_json(os.path.join(
        out_dir, superseded_relative))
    if (record.get("superseded_authorization_sha256")
            != transition.get("prior_authorization_sha256")
            or superseded.get("authorization_id")
            != transition.get("prior_authorization_id")
            or superseded.get("recovery_git_commit")
            != prior_recovery_commit
            or superseded.get("recovery_code_fingerprint")
            != prior_recovery_fingerprint):
        raise RuntimeError(
            "dispatcher parent-loss pending prior authorization mismatch")
    if (not os.path.isfile(stop_path)
            or _sha256_file(stop_path)
            != record.get("archived_stop_sha256")):
        raise RuntimeError(
            "dispatcher parent-loss pending canonical stop mismatch")

    emergency_entries = record.get("archived_emergency_stop_records")
    emergency_source_dir = os.path.join(
        out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_names = (
        sorted(
            name for name in os.listdir(emergency_source_dir)
            if name.endswith(".json")
        )
        if os.path.isdir(emergency_source_dir) else []
    )
    expected_emergency_names = []
    if not isinstance(emergency_entries, list):
        raise RuntimeError(
            "dispatcher parent-loss pending emergency stop scope is invalid")
    for entry in emergency_entries:
        relative = (
            entry.get("path") if isinstance(entry, dict) else None
        )
        expected_sha256 = (
            entry.get("sha256") if isinstance(entry, dict) else None
        )
        if (not isinstance(relative, str)
                or not relative.startswith(old_emergency_relative + "/")
                or "/" in relative[len(old_emergency_relative) + 1:]
                or "\\" in relative
                or not isinstance(expected_sha256, str)
                or len(expected_sha256) != 64):
            raise RuntimeError(
                "dispatcher parent-loss pending emergency stop scope is "
                "invalid")
        name = relative.rsplit("/", 1)[-1]
        source = os.path.join(emergency_source_dir, name)
        if (not name.endswith(".json")
                or not os.path.isfile(source)
                or _sha256_file(source) != expected_sha256):
            raise RuntimeError(
                "dispatcher parent-loss pending emergency stop digest "
                "mismatch")
        expected_emergency_names.append(name)
    if (len(expected_emergency_names)
            != len(set(expected_emergency_names))
            or sorted(expected_emergency_names) != emergency_names):
        raise RuntimeError(
            "dispatcher parent-loss pending emergency stop scope is invalid")

    old_pending_sha256 = _sha256_file(pending_path)
    old_pending_archive_relative = history_relative + "/pending.json"
    old_pending_archive_path = os.path.join(
        out_dir, old_pending_archive_relative)
    replacement_authorization_id = (
        "dpl-" + hashlib.sha256(
            (old_pending_sha256 + current_commit).encode("ascii")
        ).hexdigest()[:12]
    )
    replacement_history_relative = (
        _DISPATCHER_PARENT_LOSS_HISTORY_ROOT
        + "/" + replacement_authorization_id)
    replacement_history_dir = os.path.realpath(os.path.join(
        out_dir, replacement_history_relative))
    try:
        replacement_history_inside = (
            os.path.commonpath(
                [out_dir, replacement_history_dir]) == out_dir)
    except ValueError:
        replacement_history_inside = False
    if not replacement_history_inside:
        raise RuntimeError(
            "dispatcher parent-loss replacement history path is invalid")
    _assert_dispatcher_parent_loss_path_budget(
        replacement_history_dir,
        emergency_names,
        extra_paths=(old_pending_archive_path,),
    )
    _ensure_bound_file_copy(
        pending_path, old_pending_archive_path, old_pending_sha256)
    os.makedirs(replacement_history_dir, exist_ok=True)
    replacement_entries = set(os.listdir(replacement_history_dir))
    if replacement_entries - (
            required_history_files | {".recovery-copy.pending"}):
        raise RuntimeError(
            "dispatcher parent-loss replacement history is not pristine")

    replacement_paths = {
        "archived_active_worker_set_path": (
            replacement_history_relative
            + "/active_worker_set.before.json"),
        "archived_run_metadata_path": (
            replacement_history_relative
            + "/run_metadata.before.jsonl"),
        "superseded_authorization_path": (
            replacement_history_relative
            + "/superseded_campaign_recovery_authorization.json"),
    }
    for field, replacement_relative in replacement_paths.items():
        source_relative = record[field]
        expected_sha256 = bound_history_files[source_relative]
        _ensure_bound_file_copy(
            os.path.join(out_dir, source_relative),
            os.path.join(out_dir, replacement_relative),
            expected_sha256,
        )

    updated = json.loads(json.dumps(pending))
    updated_record = updated["authorization_record"]
    updated_record["authorization_id"] = replacement_authorization_id
    updated_record["recovery_git_commit"] = current_commit
    updated_record["recovery_code_fingerprint"] = current_fingerprint
    updated_record["changed_tracked_paths"] = cumulative_current_paths
    updated_record["changed_code_fingerprint_keys"] = (
        cumulative_fingerprint_changes)
    for name in inherited_inspector_flags:
        updated_record.pop(name, None)
    updated_record.update(replacement_paths)
    updated_record["archived_stop_path"] = (
        replacement_history_relative + "/campaign_stop.json")
    updated_record["archived_emergency_stop_records"] = [
        {
            **entry,
            "path": (
                replacement_history_relative + "/"
                + EMERGENCY_STOP_DIRECTORY + "/"
                + entry["path"].rsplit("/", 1)[-1]
            ),
        }
        for entry in emergency_entries
    ]
    updated_transition = updated_record[
        "dispatcher_parent_loss_code_transition"]
    updated_transition["delta_changed_paths"] = current_delta_paths
    updated_transition["delta_changed_code_fingerprint_keys"] = (
        prior_to_current_fingerprint_changes)
    reprepared_at = datetime.now().astimezone().isoformat(
        timespec="seconds")
    provenance_prefix = "dispatcher_parent_loss_pending_reprepared_"
    updated_record[provenance_prefix + "from_commit"] = prepared_commit
    updated_record[provenance_prefix + "from_sha256"] = (
        old_pending_sha256)
    updated_record[provenance_prefix + "from_path"] = (
        old_pending_archive_relative)
    updated_record[provenance_prefix + "from_authorization_id"] = (
        authorization_id)
    updated_record[provenance_prefix + "from_history_dir"] = (
        history_relative)
    updated_record[provenance_prefix + "at"] = reprepared_at
    updated["history_dir"] = replacement_history_relative
    updated["reprepared_at"] = reprepared_at
    write_json_atomic(pending_path, updated)
    return _read_json(pending_path)


def _reprepare_pristine_deepseek_inspector_pending(
        out_dir, pending_path, pending):
    """Rebind a side-effect-free pending record to one tooling-only fix."""
    record = (
        pending.get("authorization_record")
        if isinstance(pending, dict) else None
    )
    classifier_followup = (
        isinstance(record, dict)
        and record.get(
            "deepseek_resume_classifier_followup_recovery") is True
        and record.get("authorization_basis")
        == _DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS
    )
    pending_kind = (
        pending.get("followup_kind")
        if isinstance(pending, dict) else None
    )
    if (pending_kind not in {None, "resume_classifier"}
            or classifier_followup
            != (pending_kind == "resume_classifier")):
        raise RuntimeError(
            "DeepSeek recovery pending mode binding is invalid")
    inspector_followup = (
        isinstance(record, dict)
        and record.get(
            "deepseek_resume_classifier_followup_recovery") is not True
        and record.get("authorization_basis")
        == (
            "explicit_user_resume_after_deepseek_recovery_"
            "inspector_fix"
        )
    )
    if (not isinstance(record, dict)
            or pending.get("schema")
            != _DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA
            or record.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or record.get("recovery_kind") != LEDGER_LOCK_RECOVERY_KIND
            or record.get(
                "deepseek_transport_disconnect_retry_recovery") is not True
            or record.get(
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery") is not True
            or not (inspector_followup or classifier_followup)
            or pending.get("created_at") != record.get("created_at")
            or not isinstance(record.get("created_at"), str)
            or not record.get("created_at")
            or record.get("recovery_git_tree_state") != "clean"
            or not isinstance(record.get("deepseek_resume_samples"), list)
            or not record.get("deepseek_resume_samples")
            or not isinstance(record.get("incident_api_rows"), list)
            or not isinstance(
                record.get("recovered_worker_launch_ids"), list)
            or not isinstance(
                record.get("deepseek_pending_samples"), list)
            or record.get("archived_emergency_stop_records") != []
            or _sha256_file(os.path.join(
                out_dir, "dispatch_manifest.json"))
            != record.get("dispatch_manifest_sha256")):
        raise RuntimeError(
            "DeepSeek transport inspector pending recovery is invalid")
    prepared_commit = record.get("recovery_git_commit")
    current_commit, current_tree = _git_identity()
    if current_commit == prepared_commit:
        return pending
    if classifier_followup:
        raise RuntimeError(
            "DeepSeek resume-classifier pending identity has drifted")
    prepared_fingerprint = record.get("recovery_code_fingerprint")
    current_fingerprint = code_fingerprint()
    prepared_fingerprint_map = (
        prepared_fingerprint
        if isinstance(prepared_fingerprint, dict) else {}
    )
    fingerprint_changes = sorted(
        key for key in set(prepared_fingerprint_map) | set(
            current_fingerprint)
        if prepared_fingerprint_map.get(key)
        != current_fingerprint.get(key)
    )
    if (current_tree != "clean"
            or not isinstance(prepared_commit, str)
            or not isinstance(prepared_fingerprint, dict)
            or fingerprint_changes != ["run_meta.py"]):
        raise RuntimeError(
            "DeepSeek transport inspector pending identity has drifted")
    parent_line = subprocess.run(
        ["git", "rev-list", "--parents", "-n", "1", current_commit],
        check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.split()
    tooling_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    prior_recovery_commit = record.get(
        "deepseek_transport_disconnect_inspector_"
        "prior_recovery_git_commit")
    if (parent_line != [current_commit, prepared_commit]
            or set(_git_changed_paths(prepared_commit, current_commit))
            != tooling_paths
            or sorted(_git_changed_paths(
                prior_recovery_commit, current_commit))
            != sorted(record.get(
                "deepseek_transport_disconnect_inspector_"
                "delta_changed_paths") or [])
            or sorted(_git_changed_paths(
                record.get("prior_git_commit"), current_commit))
            != sorted(record.get("changed_tracked_paths") or [])):
        raise RuntimeError(
            "DeepSeek transport inspector pending tooling delta is invalid")

    authorization_id = record.get("authorization_id")
    expected_history_relative = (
        f"recovery_history/{authorization_id}")
    history_dir = os.path.join(out_dir, expected_history_relative)
    auth_path = os.path.join(
        out_dir, CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
    stop_path = os.path.join(out_dir, "campaign_stop.json")
    active = _read_json(os.path.join(
        out_dir, "active_worker_set.json"))
    history_entries = (
        set(os.listdir(history_dir))
        if os.path.isdir(history_dir) else set()
    )
    if (not isinstance(authorization_id, str)
            or not authorization_id
            or authorization_id in {".", ".."}
            or "/" in authorization_id
            or "\\" in authorization_id
            or pending.get("history_dir") != expected_history_relative
            or record.get("superseded_authorization_path")
            != (
                expected_history_relative
                + "/superseded_campaign_recovery_authorization.json"
            )
            or record.get("archived_stop_path")
            != expected_history_relative + "/campaign_stop.json"
            or active.get("workers") != {}
            or not os.path.isfile(auth_path)
            or _sha256_file(auth_path)
            != record.get("superseded_authorization_sha256")
            or not os.path.isfile(stop_path)
            or _sha256_file(stop_path)
            != record.get("archived_stop_sha256")
            or history_entries - {
                "pending.json", ".recovery-copy.pending"}
            or (os.path.exists(history_dir)
                and not os.path.isdir(history_dir))):
        raise RuntimeError(
            "DeepSeek transport inspector pending is not pristine")
    _assert_worker_leases_free(
        out_dir, record["deepseek_resume_samples"])
    prefix_evidence = record.get(
        "deepseek_transport_disconnect_inspector_prefixes")
    if (not isinstance(prefix_evidence, dict)
            or set(prefix_evidence) != {
                "api_calls.jsonl", "dispatch_log.jsonl"}
            or any(
                _file_prefix_evidence(os.path.join(out_dir, name))
                != evidence
                for name, evidence in prefix_evidence.items()
            )):
        raise RuntimeError(
            "DeepSeek transport inspector pending prefix has drifted")

    old_pending_sha256 = _sha256_file(pending_path)
    old_pending_archive_relative = (
        expected_history_relative + "/pending.json")
    old_pending_archive_path = os.path.join(
        out_dir, old_pending_archive_relative)
    os.makedirs(history_dir, exist_ok=True)
    _ensure_bound_file_copy(
        pending_path, old_pending_archive_path, old_pending_sha256)
    updated = json.loads(json.dumps(pending))
    updated_record = updated["authorization_record"]
    replacement_authorization_id = "dsi-" + uuid.uuid4().hex[:12]
    replacement_history_relative = (
        "recovery_history/" + replacement_authorization_id)
    updated_record["recovery_git_commit"] = current_commit
    updated_record["recovery_code_fingerprint"] = current_fingerprint
    updated_record["authorization_id"] = replacement_authorization_id
    updated_record["superseded_authorization_path"] = (
        replacement_history_relative
        + "/superseded_campaign_recovery_authorization.json"
    )
    updated_record["archived_stop_path"] = (
        replacement_history_relative + "/campaign_stop.json")
    updated_record[
        "deepseek_transport_inspector_pending_reprepared_from_commit"
    ] = prepared_commit
    updated_record[
        "deepseek_transport_inspector_pending_reprepared_from_sha256"
    ] = old_pending_sha256
    updated_record[
        "deepseek_transport_inspector_pending_reprepared_from_path"
    ] = old_pending_archive_relative
    updated_record[
        "deepseek_transport_inspector_pending_reprepared_from_"
        "authorization_id"
    ] = authorization_id
    updated_record[
        "deepseek_transport_inspector_pending_reprepared_from_history_dir"
    ] = expected_history_relative
    reprepared_at = datetime.now().astimezone().isoformat(
        timespec="seconds")
    updated_record[
        "deepseek_transport_inspector_pending_reprepared_at"
    ] = reprepared_at
    updated["history_dir"] = replacement_history_relative
    updated["reprepared_at"] = reprepared_at
    write_json_atomic(pending_path, updated)
    return _read_json(pending_path)


def _validate_deepseek_inspector_reprepare_evidence(
        out_dir, pending, record):
    prefix = "deepseek_transport_inspector_pending_reprepared_"
    provenance_keys = {
        "from_commit",
        "from_sha256",
        "from_path",
        "from_authorization_id",
        "from_history_dir",
        "at",
    }
    present = {
        suffix for suffix in provenance_keys
        if prefix + suffix in record
    }
    history_root = os.path.join(out_dir, "recovery_history")
    pending_archives = (
        sorted(
            os.path.join(history_root, name, "pending.json")
            for name in os.listdir(history_root)
            if os.path.isfile(os.path.join(
                history_root, name, "pending.json"))
        )
        if os.path.isdir(history_root) else []
    )
    if not present and "reprepared_at" not in pending:
        if pending_archives:
            raise RuntimeError(
                "DeepSeek transport inspector reprepare provenance was "
                "removed")
        return
    if present != provenance_keys:
        raise RuntimeError(
            "DeepSeek transport inspector reprepare provenance is "
            "incomplete")
    prepared_commit = record[prefix + "from_commit"]
    old_pending_sha256 = record[prefix + "from_sha256"]
    old_authorization_id = record[prefix + "from_authorization_id"]
    old_history_relative = record[prefix + "from_history_dir"]
    old_pending_relative = record[prefix + "from_path"]
    reprepared_at = record[prefix + "at"]
    if (not isinstance(prepared_commit, str)
            or not isinstance(old_pending_sha256, str)
            or len(old_pending_sha256) != 64
            or not isinstance(old_authorization_id, str)
            or not old_authorization_id
            or "/" in old_authorization_id
            or "\\" in old_authorization_id
            or old_history_relative
            != "recovery_history/" + old_authorization_id
            or old_pending_relative
            != old_history_relative + "/pending.json"
            or pending.get("reprepared_at") != reprepared_at
            or not isinstance(reprepared_at, str)
            or not reprepared_at):
        raise RuntimeError(
            "DeepSeek transport inspector reprepare provenance is invalid")
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
        raise RuntimeError(
            "DeepSeek transport inspector prior pending archive mismatch")
    old_pending = _read_json(old_pending_path)
    old_record = old_pending.get("authorization_record")
    if (not isinstance(old_record, dict)
            or old_pending.get("schema")
            != _DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA
            or old_pending.get("history_dir") != old_history_relative
            or old_record.get("authorization_id")
            != old_authorization_id
            or old_record.get("recovery_git_commit") != prepared_commit):
        raise RuntimeError(
            "DeepSeek transport inspector prior pending identity mismatch")

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
            or current_fingerprint != code_fingerprint()):
        raise RuntimeError(
            "DeepSeek transport inspector reprepare fingerprint mismatch")
    parent_line = subprocess.run(
        ["git", "rev-list", "--parents", "-n", "1", current_commit],
        check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.split()
    tooling_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    if (parent_line != [current_commit, prepared_commit]
            or set(_git_changed_paths(prepared_commit, current_commit))
            != tooling_paths):
        raise RuntimeError(
            "DeepSeek transport inspector reprepare commit mismatch")

    expected = json.loads(json.dumps(old_pending))
    expected_record = expected["authorization_record"]
    current_authorization_id = record.get("authorization_id")
    current_history_relative = (
        "recovery_history/" + str(current_authorization_id))
    expected_record["recovery_git_commit"] = current_commit
    expected_record["recovery_code_fingerprint"] = current_fingerprint
    expected_record["authorization_id"] = current_authorization_id
    expected_record["superseded_authorization_path"] = (
        current_history_relative
        + "/superseded_campaign_recovery_authorization.json"
    )
    expected_record["archived_stop_path"] = (
        current_history_relative + "/campaign_stop.json")
    for suffix in provenance_keys:
        expected_record[prefix + suffix] = record[prefix + suffix]
    expected["history_dir"] = current_history_relative
    expected["reprepared_at"] = reprepared_at
    if (not isinstance(current_authorization_id, str)
            or re.fullmatch(
                r"dsi-[0-9a-f]{12}", current_authorization_id) is None
            or expected != pending
            or expected_record != record):
        raise RuntimeError(
            "DeepSeek transport inspector reprepared pending mismatch")


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


def _deepseek_transport_disconnect_retry_rows(out_dir, stop):
    """Return the one pre-generation OpenAI disconnect misclassified fatal."""
    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    failures = []
    for number, row in enumerate(api_rows, 1):
        attempts = row.get("transport_attempts")
        final_attempt = attempts[-1] if isinstance(
            attempts, list) and attempts else {}
        if (row.get("model") == "deepseek-v4-flash"
                and row.get("sample") == stop.get("sample")
                and row.get("worker_launch_id")
                == stop.get("worker_launch_id")
                and row.get("worker_pid") == stop.get("worker_pid")
                and row.get("classification") == "runner_exception"
                and row.get("error_type") == "transport_disconnect"
                and row.get("http_status") is None
                and row.get("provider_called") is True
                and row.get("stream_complete") is False
                and row.get("response_replayed") is False
                and row.get("count_as_method_failure") is True
                and row.get("http_attempts_used") == 1
                and row.get("retry_count") == 0
                and row.get("failed_attempt_count") == 1
                and row.get("max_retries") == 3
                and len(attempts) == 1
                and final_attempt.get("attempt_index") == 1
                and final_attempt.get("status") == "fatal_error"
                and final_attempt.get("http_status") is None
                and final_attempt.get("error_type")
                == "transport_disconnect"
                and final_attempt.get("error_message")
                == "Connection error."
                and final_attempt.get(
                    "response_started_http_status") is None
                and final_attempt.get("generation_delta_seen") is False
                and final_attempt.get("retry_budget_consumed") is True
                and final_attempt.get("retry_budget_attempt_index") == 1
                and row.get("runner_exception")
                == (
                    "OpenAICompatibleTransportError: OpenAI-compatible "
                    "provider failed after 1 attempt(s)"
                )):
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


def _commit_deepseek_transport_disconnect_initial(
        out_dir, manifest, stop_path, auth_path, record):
    """Complete the initial recovery while its stop remains fail-closed."""
    if (not isinstance(record, dict)
            or record.get(
                "deepseek_transport_disconnect_initial_transaction")
            is not True
            or record.get(
                "deepseek_transport_disconnect_retry_recovery") is not True
            or record.get(
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery") is not None):
        raise RuntimeError(
            "DeepSeek initial transport recovery record is invalid")
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    current_commit, current_tree = _git_identity()
    if (_sha256_file(manifest_path)
            != record.get("dispatch_manifest_sha256")
            or current_tree != "clean"
            or current_commit != record.get("recovery_git_commit")
            or code_fingerprint()
            != record.get("recovery_code_fingerprint")):
        raise RuntimeError(
            "DeepSeek initial transport recovery identity has drifted")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = record.get("deepseek_resume_samples")
    if not isinstance(samples, list) or not samples:
        raise RuntimeError(
            "DeepSeek initial transport recovery sample scope is invalid")
    _assert_worker_leases_free(out_dir, samples)

    archived_relative = record.get("archived_stop_path")
    if not isinstance(archived_relative, str) or not archived_relative:
        raise RuntimeError(
            "DeepSeek initial transport recovery archive is invalid")
    archived_stop_path = os.path.realpath(os.path.join(
        out_dir, archived_relative))
    history_dir = os.path.dirname(archived_stop_path)
    try:
        archive_inside = (
            os.path.commonpath([out_dir, archived_stop_path]) == out_dir)
    except ValueError:
        archive_inside = False
    if (not archive_inside
            or os.path.basename(archived_stop_path)
            != "campaign_stop.json"):
        raise RuntimeError(
            "DeepSeek initial transport recovery archive is invalid")
    os.makedirs(history_dir, exist_ok=True)
    if set(os.listdir(history_dir)) - {
            "campaign_stop.json", EMERGENCY_STOP_DIRECTORY}:
        raise RuntimeError(
            "DeepSeek initial transport recovery history contains "
            "unexpected artifacts")

    sidecar_entries = record.get("incident_transport_sidecars")
    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    api_by_hash = {
        _canonical_record_sha256(row): row for row in api_rows
    }
    if (not isinstance(sidecar_entries, list)
            or len(sidecar_entries) != 1
            or sidecar_entries[0].get("api_row_sha256")
            not in api_by_hash
            or sidecar_entries[0]
            != _deepseek_dispatcher_stopped_sidecar_evidence(
                out_dir,
                api_by_hash[sidecar_entries[0]["api_row_sha256"]],
            )):
        raise RuntimeError(
            "DeepSeek initial transport recovery sidecar has drifted")

    existing_authorization = (
        _read_json(auth_path) if os.path.isfile(auth_path) else None
    )
    if existing_authorization not in (None, record):
        raise RuntimeError(
            "DeepSeek initial transport recovery authorization has drifted")
    if existing_authorization is None:
        write_json_atomic(auth_path, record)
    authorization_sha256 = _sha256_file(auth_path)
    event = {
        "event": (
            "user_authorized_deepseek_transport_disconnect_retry_recovery"),
        "created_at": record.get("created_at"),
        "campaign_recovery_authorization_id": record["authorization_id"],
        "campaign_recovery_authorization_sha256": authorization_sha256,
        "deepseek_transport_disconnect_retry_samples": record[
            "deepseek_transport_disconnect_retry_samples"],
        "incident_api_rows": len(record["incident_api_rows"]),
        "prior_git_commit": record.get("prior_git_commit"),
        "recovery_git_commit": record.get("recovery_git_commit"),
    }
    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    dispatch_prefix = (
        record.get(
            "deepseek_transport_disconnect_initial_prefixes") or {}
    ).get("dispatch_log.jsonl")
    _append_recoverable_jsonl_tail(
        dispatch_path, dispatch_prefix, event)
    matches = [
        row for row in _read_jsonl(dispatch_path)
        if row.get("event") == event["event"]
        and row.get("campaign_recovery_authorization_id")
        == record["authorization_id"]
    ]
    if len(matches) != 1 or matches[0] != event:
        raise RuntimeError(
            "DeepSeek initial transport recovery event has drifted")

    archived_stop, archived_emergency = _archive_campaign_stop_cohort(
        out_dir, stop_path, history_dir)
    if (_sha256_file(archived_stop)
            != record.get("archived_stop_sha256")
            or os.path.realpath(archived_stop) != archived_stop_path
            or archived_emergency
            != record.get("archived_emergency_stop_records")):
        raise RuntimeError(
            "DeepSeek initial transport recovery stop has drifted")
    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(out_dir)
    return {
        "authorization_id": record["authorization_id"],
        "authorization_sha256": verified["authorization_sha256"],
        "deepseek_transport_disconnect_retry_samples": record[
            "deepseek_transport_disconnect_retry_samples"],
        "incident_api_rows": len(record["incident_api_rows"]),
        "recovered_workers": len(record["recovered_worker_launch_ids"]),
        "pending_samples": len(record["deepseek_pending_samples"]),
    }


def _authorize_deepseek_transport_disconnect_retry(
        out_dir, manifest, stop, stop_path, auth_path, prior_authorization):
    """Authorize this campaign's exact pre-generation disconnect replay."""
    if prior_authorization is not None:
        if (prior_authorization.get(
                "deepseek_transport_disconnect_initial_transaction") is True
                and stop.get("condition") == "worker_fatal_error"):
            return _commit_deepseek_transport_disconnect_initial(
                out_dir, manifest, stop_path, auth_path,
                prior_authorization)
        return _authorize_deepseek_transport_inspector_followup(
            out_dir, manifest, stop, stop_path, auth_path,
            prior_authorization)
    config = manifest.get("config") or {}
    if (config.get("campaign_role") != "deepseek_full234"
            or config.get("num_round_trips")
            != DEEPSEEK_FULL234_ROUND_TRIPS
            or config.get("transport") != DEEPSEEK_TRANSPORT
            or config.get("transport_revision")
            != DEEPSEEK_TRANSPORT_REVISION
            or stop.get("condition") != "worker_fatal_error"
            or stop.get("error_type")
            != "OpenAICompatibleTransportError"
            or stop.get("error")
            != "OpenAI-compatible provider failed after 1 attempt(s)"
            or not isinstance(stop.get("sample"), str)
            or not stop.get("sample")
            or not isinstance(stop.get("worker_launch_id"), str)
            or not stop.get("worker_launch_id")):
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery boundary is invalid")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = config.get("samples") or []
    _assert_worker_leases_free(out_dir, samples)

    current_commit, current_tree = _git_identity()
    if current_tree != "clean":
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery requires a clean Git tree")
    prior_commit = manifest.get("run_git_commit")
    if current_commit == prior_commit:
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery code commit has not changed")

    scope = _deepseek_checkpoint_resume_scope(out_dir, manifest)
    recovered_workers = scope["recovered_workers"]
    recovered_by_id = {
        item["worker_launch_id"]: item for item in recovered_workers
    }
    failures = _deepseek_transport_disconnect_retry_rows(out_dir, stop)
    if len(failures) != 1:
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery requires exactly one "
            "matching failure row"
        )
    failure_number, failure_row = failures[0]
    retry_sample = failure_row.get("sample")
    if (retry_sample not in samples
            or failure_row.get("worker_launch_id") not in recovered_by_id
            or recovered_by_id[
                failure_row.get("worker_launch_id")].get("status")
            != "failed"):
        raise RuntimeError(
            "DeepSeek transport-disconnect worker scope is invalid")

    failure_hash = _canonical_record_sha256(failure_row)
    dispatcher_stopped_rows = []
    for number, row in scope["incident_api_rows"]:
        digest = _canonical_record_sha256(row)
        if digest == failure_hash:
            continue
        attempts = row.get("transport_attempts")
        if (row.get("classification") is None
                and row.get("http_status") == 200
                and row.get("stream_complete") is True
                and row.get("input_tokens") is not None
                and row.get("output_tokens") is not None
                and isinstance(attempts, list) and attempts
                and attempts[-1].get("status") == "success"):
            continue
        if (row.get("classification") == "runner_exception"
                and row.get("error_type") == "CampaignStoppedError"
                and row.get("http_status") is None
                and row.get("stream_complete") is False
                and row.get("count_as_method_failure") is True
                and row.get("http_attempts_used") is None
                and attempts == []
                and row.get("runner_exception")
                == (
                    "CampaignStoppedError: campaign stop latch is set: "
                    "worker_fatal_error"
                )):
            dispatcher_stopped_rows.append((number, row))
            continue
        raise RuntimeError(
            "DeepSeek transport-disconnect uncommitted API scope is invalid")
    if len(dispatcher_stopped_rows) != 1:
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery requires exactly one "
            "dispatcher-stopped API row"
        )

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
        "HP_V8/src/model_openai.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
        "transport/src/model_openai.py",
        "transport/src/test_model_openai.py",
    }
    if (fingerprint_changes != ["model_openai.py", "run_meta.py"]
            or set(changed_paths) != required_paths):
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery commit scope is invalid")

    authorization_id = (
        "deepseek-transport-disconnect-retry-"
        + datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
        + "-" + uuid.uuid4().hex[:8]
    )
    created_at = datetime.now().astimezone().isoformat(
        timespec="seconds")
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    emergency_dir = os.path.join(out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_names = (
        sorted(
            name for name in os.listdir(emergency_dir)
            if name.endswith(".json"))
        if os.path.isdir(emergency_dir) else []
    )
    if emergency_names:
        raise RuntimeError(
            "DeepSeek transport-disconnect recovery requires one canonical "
            "stop latch")

    worker_ids = sorted(recovered_by_id)
    dispatcher_stopped_hashes = {
        _canonical_record_sha256(row)
        for _number, row in dispatcher_stopped_rows
    }
    incident_transport_sidecars = [
        _deepseek_dispatcher_stopped_sidecar_evidence(out_dir, row)
        for _number, row in dispatcher_stopped_rows
    ]
    initial_prefixes = {
        "dispatch_log.jsonl": _file_prefix_evidence(os.path.join(
            out_dir, "dispatch_log.jsonl")),
    }
    record = {
        "schema": CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
        "authorization_id": authorization_id,
        "recovery_kind": LEDGER_LOCK_RECOVERY_KIND,
        "created_at": created_at,
        "authorization_basis": (
            "explicit_user_resume_after_deepseek_transport_disconnect_"
            "classifier_fix"
        ),
        "deepseek_transport_disconnect_retry_recovery": True,
        "deepseek_transport_disconnect_initial_transaction": True,
        "deepseek_transport_disconnect_initial_prefixes": initial_prefixes,
        "deepseek_transport_disconnect_retry_samples": [retry_sample],
        "deepseek_transport_disconnect_retry_worker_launch_id": (
            failure_row.get("worker_launch_id")),
        "deepseek_resume_samples": scope["resume_samples"],
        "deepseek_pending_samples": scope["pending_samples"],
        "deepseek_recovered_workers": recovered_workers,
        "provider_access_resume_samples": [retry_sample],
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
        "archived_stop_sha256": _sha256_file(stop_path),
        "archived_emergency_stop_records": [],
        "incident_api_rows": [
            _incident_entry(
                number,
                row,
                (
                    "deepseek_transport_disconnect_misclassification"
                    if _canonical_record_sha256(row) == failure_hash
                    else (
                        "deepseek_dispatcher_stopped_uncommitted_api"
                        if _canonical_record_sha256(row)
                        in dispatcher_stopped_hashes
                        else "deepseek_interrupted_uncommitted_api"
                    )
                ),
            )
            for number, row in scope["incident_api_rows"]
        ],
        "incident_attempt_rows": [],
        "incident_transport_sidecars": incident_transport_sidecars,
        "recovered_worker_launch_ids": worker_ids,
        "preauthorization_worker_launch_ids": [],
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    }
    return _commit_deepseek_transport_disconnect_initial(
        out_dir, manifest, stop_path, auth_path, record)


def _commit_deepseek_transport_inspector_followup(
        out_dir, manifest, stop_path, auth_path, pending_path, pending):
    """Finish one prepared inspector recovery; every step is idempotent."""
    if (not isinstance(pending, dict)
            or pending.get("schema")
            != _DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA
            or not isinstance(pending.get("authorization_record"), dict)):
        raise RuntimeError(
            "DeepSeek transport inspector pending recovery is invalid")
    record = pending["authorization_record"]
    classifier_followup = (
        record.get(
            "deepseek_resume_classifier_followup_recovery") is True
    )
    pending_kind = pending.get("followup_kind")
    if (pending_kind not in {None, "resume_classifier"}
            or classifier_followup
            != (pending_kind == "resume_classifier")):
        raise RuntimeError(
            "DeepSeek recovery pending mode binding is invalid")
    followup_prefix = (
        "deepseek_resume_classifier_"
        if classifier_followup
        else "deepseek_transport_disconnect_inspector_"
    )
    expected_basis = (
        _DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS
        if classifier_followup
        else (
            "explicit_user_resume_after_deepseek_recovery_"
            "inspector_fix"
        )
    )
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if (_sha256_file(manifest_path)
            != record.get("dispatch_manifest_sha256")):
        raise RuntimeError(
            "DeepSeek transport inspector pending manifest has drifted")
    current_commit, current_tree = _git_identity()
    if (current_tree != "clean"
            or current_commit != record.get("recovery_git_commit")
            or code_fingerprint()
            != record.get("recovery_code_fingerprint")):
        raise RuntimeError(
            "DeepSeek transport inspector pending identity has drifted")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = record.get("deepseek_resume_samples")
    if not isinstance(samples, list) or not samples:
        raise RuntimeError(
            "DeepSeek transport inspector pending sample scope is invalid")
    _assert_worker_leases_free(out_dir, samples)
    if (record.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or record.get("recovery_kind") != LEDGER_LOCK_RECOVERY_KIND
            or record.get(
                "deepseek_transport_disconnect_retry_recovery") is not True
            or record.get(
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery") is not True
            or record.get("authorization_basis")
            != expected_basis
            or record.get("recovery_git_tree_state") != "clean"
            or not isinstance(
                record.get(
                    "deepseek_transport_disconnect_retry_samples"), list)
            or not record.get(
                "deepseek_transport_disconnect_retry_samples")
            or not isinstance(record.get("incident_api_rows"), list)
            or not isinstance(
                record.get("recovered_worker_launch_ids"), list)
            or not isinstance(
                record.get("deepseek_pending_samples"), list)
            or record.get("archived_emergency_stop_records") != []
            or record.get("committed_results_modified") is not False
            or record.get("checkpoint_rows_modified") is not False
            or record.get("provider_post_replay_scope")
            != "uncommitted_steps_only"
            or not isinstance(
                record.get(followup_prefix + "prior_authorization_id"),
                str,
            )
            or not record.get(followup_prefix + "prior_authorization_id")
            or not isinstance(
                record.get(followup_prefix + "prior_recovery_git_commit"),
                str,
            )
            or not record.get(
                followup_prefix + "prior_recovery_git_commit")
            or (
                classifier_followup
                and (
                    not isinstance(
                        record.get(
                            "deepseek_resume_classifier_sample"), str)
                    or not record.get(
                        "deepseek_resume_classifier_sample")
                    or not isinstance(
                        record.get(
                            "deepseek_resume_classifier_stop_error"), str)
                    or not record.get(
                        "deepseek_resume_classifier_stop_error").startswith(
                            _DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX)
                    or not isinstance(
                        record.get(
                            "deepseek_resume_classifier_evidence"), list)
                    or not record.get(
                        "deepseek_resume_classifier_evidence")
                )
            )):
        raise RuntimeError(
            "DeepSeek transport inspector pending record is invalid")
    if not classifier_followup:
        _validate_deepseek_inspector_reprepare_evidence(
            out_dir, pending, record)

    history_relative = pending.get("history_dir")
    authorization_id = record.get("authorization_id")
    if (not isinstance(authorization_id, str)
            or not authorization_id
            or authorization_id in {".", ".."}
            or "/" in authorization_id
            or "\\" in authorization_id
            or (
                classifier_followup
                and re.fullmatch(
                    r"dsr-[0-9a-f]{12}", authorization_id) is None
            )):
        raise RuntimeError(
            "DeepSeek transport inspector pending authorization id is "
            "invalid")
    expected_history_relative = (
        "recovery_history/" + authorization_id)
    expected_superseded_relative = (
        expected_history_relative
        + "/superseded_campaign_recovery_authorization.json"
    )
    expected_stop_relative = (
        expected_history_relative + "/campaign_stop.json")
    if (pending.get("created_at") != record.get("created_at")
            or not isinstance(record.get("created_at"), str)
            or not record.get("created_at")
            or history_relative != expected_history_relative
            or record.get("superseded_authorization_path")
            != expected_superseded_relative
            or record.get("archived_stop_path")
            != expected_stop_relative):
        raise RuntimeError(
            "DeepSeek transport inspector pending archive path has drifted")
    history_dir = os.path.realpath(os.path.join(
        out_dir, history_relative))
    superseded_path = os.path.realpath(os.path.join(
        out_dir, expected_superseded_relative))
    archived_stop_path = os.path.realpath(os.path.join(
        out_dir, expected_stop_relative))
    expected_history_dir = os.path.realpath(os.path.join(
        out_dir, "recovery_history", authorization_id))
    try:
        history_inside = (
            os.path.commonpath([out_dir, history_dir]) == out_dir
            and history_dir == expected_history_dir
            and os.path.dirname(superseded_path) == history_dir
            and os.path.dirname(archived_stop_path) == history_dir
            and superseded_path != archived_stop_path
            and superseded_path != os.path.realpath(auth_path)
            and superseded_path != os.path.realpath(stop_path)
            and archived_stop_path != os.path.realpath(auth_path)
            and archived_stop_path != os.path.realpath(stop_path)
        )
    except ValueError:
        history_inside = False
    if not history_inside:
        raise RuntimeError(
            "DeepSeek transport inspector pending archive path has drifted")
    if os.path.exists(history_dir) and not os.path.isdir(history_dir):
        raise RuntimeError(
            "DeepSeek transport inspector pending history path is invalid")
    allowed_history_names = {
        "superseded_campaign_recovery_authorization.json",
        (
            "superseded_campaign_recovery_authorization.json"
            ".pending-copy"
        ),
        ".recovery-copy.pending",
        "campaign_stop.json",
        EMERGENCY_STOP_DIRECTORY,
    }
    if (os.path.isdir(history_dir)
            and set(os.listdir(history_dir)) - allowed_history_names):
        raise RuntimeError(
            "DeepSeek transport inspector pending history contains "
            "unexpected artifacts")

    existing_authorization = (
        _read_json(auth_path) if os.path.isfile(auth_path) else None
    )
    expected_superseded_sha256 = record.get(
        "superseded_authorization_sha256")
    if (not isinstance(expected_superseded_sha256, str)
            or len(expected_superseded_sha256) != 64
            or record.get(followup_prefix + "prior_authorization_sha256")
            != expected_superseded_sha256):
        raise RuntimeError(
            "DeepSeek transport inspector superseded authorization "
            "digest is invalid")
    if os.path.isfile(superseded_path):
        if _sha256_file(superseded_path) != expected_superseded_sha256:
            raise RuntimeError(
                "DeepSeek transport inspector superseded authorization "
                "archive has drifted")
        superseded_authorization = _read_json(superseded_path)
    elif existing_authorization is not None:
        if _sha256_file(auth_path) != expected_superseded_sha256:
            raise RuntimeError(
                "DeepSeek transport inspector superseded authorization "
                "source has drifted")
        superseded_authorization = existing_authorization
    else:
        raise RuntimeError(
            "DeepSeek transport inspector superseded authorization "
            "source is missing")
    if (superseded_authorization.get("authorization_id")
            != record.get(followup_prefix + "prior_authorization_id")):
        raise RuntimeError(
            "DeepSeek transport inspector superseded authorization "
            "identity has drifted")
    if (existing_authorization is not None
            and existing_authorization != superseded_authorization
            and existing_authorization != record):
        raise RuntimeError(
            "DeepSeek transport inspector active authorization has drifted")

    expected_stop_sha256 = record.get("archived_stop_sha256")
    stop_source = (
        stop_path if os.path.isfile(stop_path) else archived_stop_path)
    if (not isinstance(expected_stop_sha256, str)
            or len(expected_stop_sha256) != 64
            or not os.path.isfile(stop_source)
            or _sha256_file(stop_source) != expected_stop_sha256):
        raise RuntimeError(
            "DeepSeek transport inspector stop source has drifted")
    emergency_dir = os.path.join(
        out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_history = os.path.join(
        history_dir, EMERGENCY_STOP_DIRECTORY)
    if any(
            os.path.isdir(path)
            and any(name.endswith(".json") for name in os.listdir(path))
            for path in (emergency_dir, emergency_history)):
        raise RuntimeError(
            "DeepSeek transport inspector emergency stop scope has drifted")

    authorization_sha256 = hashlib.sha256(
        json.dumps(record, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    event = {
        "event": (
            "user_authorized_deepseek_resume_classifier_followup"
            if classifier_followup
            else "user_authorized_deepseek_transport_inspector_followup"
        ),
        "created_at": pending.get("created_at"),
        "campaign_recovery_authorization_id": record["authorization_id"],
        "campaign_recovery_authorization_sha256": authorization_sha256,
        "superseded_authorization_id": superseded_authorization.get(
            "authorization_id"),
        "incident_api_rows": len(record["incident_api_rows"]),
        "prior_recovery_git_commit": record[
            followup_prefix + "prior_recovery_git_commit"],
        "recovery_git_commit": record["recovery_git_commit"],
    }
    if classifier_followup:
        event["resume_classifier_sample"] = record[
            "deepseek_resume_classifier_sample"]
    dispatch_prefix = (
        record.get(followup_prefix + "prefixes") or {}
    ).get("dispatch_log.jsonl")
    _assert_recoverable_jsonl_tail(
        dispatch_path, dispatch_prefix, event)

    os.makedirs(history_dir, exist_ok=True)
    _ensure_bound_file_copy(
        auth_path, superseded_path, expected_superseded_sha256)
    archived_stop, archived_emergency = _archive_campaign_stop_cohort(
        out_dir, stop_path, history_dir)
    if (_sha256_file(archived_stop)
            != record.get("archived_stop_sha256")
            or os.path.realpath(archived_stop) != archived_stop_path
            or archived_emergency
            != record.get("archived_emergency_stop_records")):
        raise RuntimeError(
            "DeepSeek transport inspector archived stop cohort has drifted")
    if existing_authorization != record:
        write_json_atomic(auth_path, record)

    if _sha256_file(auth_path) != authorization_sha256:
        raise RuntimeError(
            "DeepSeek transport inspector active authorization digest "
            "has drifted")
    _append_recoverable_jsonl_tail(
        dispatch_path, dispatch_prefix, event)
    matches = [
        row for row in _read_jsonl(dispatch_path)
        if row.get("event") == event["event"]
        and row.get("campaign_recovery_authorization_id")
        == record["authorization_id"]
    ]
    if len(matches) != 1 or matches[0] != event:
        raise RuntimeError(
            "DeepSeek transport inspector authorization event has drifted")

    campaign_recovery_incident_evidence.cache_clear()
    verified = read_campaign_recovery_authorization(
        out_dir, allow_pending_transaction=True)
    _unlink_with_sharing_retry(pending_path)
    return {
        "authorization_id": record["authorization_id"],
        "authorization_sha256": verified["authorization_sha256"],
        "deepseek_transport_disconnect_retry_samples": record[
            "deepseek_transport_disconnect_retry_samples"],
        "incident_api_rows": len(record["incident_api_rows"]),
        "recovered_workers": len(record["recovered_worker_launch_ids"]),
        "pending_samples": len(record["deepseek_pending_samples"]),
        "inspector_followup": True,
        "resume_classifier_followup": classifier_followup,
    }


def _authorize_deepseek_transport_inspector_followup(
        out_dir, manifest, stop, stop_path, auth_path, prior_authorization):
    """Supersede the exact zero-worker inspector gap from the first recovery."""
    config = manifest.get("config") or {}
    prior_authorization_sha256 = _sha256_file(auth_path)
    if (prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or prior_authorization.get("recovery_kind")
            != LEDGER_LOCK_RECOVERY_KIND
            or prior_authorization.get(
                "deepseek_transport_disconnect_retry_recovery") is not True
            or prior_authorization.get(
                "deepseek_transport_disconnect_inspector_followup_recovery")
            is not None
            or prior_authorization.get("authorization_basis")
            != (
                "explicit_user_resume_after_deepseek_transport_disconnect_"
                "classifier_fix"
            )
            or config.get("campaign_role") != "deepseek_full234"
            or config.get("num_round_trips")
            != DEEPSEEK_FULL234_ROUND_TRIPS
            or config.get("transport") != DEEPSEEK_TRANSPORT
            or config.get("transport_revision")
            != DEEPSEEK_TRANSPORT_REVISION
            or stop.get("schema") != STOP_CONDITION_SCHEMA
            or stop.get("condition") != "dispatcher_integrity_failure"
            or stop.get("worker_launch_id") is not None
            or not isinstance(stop.get("worker_pid"), int)
            or isinstance(stop.get("worker_pid"), bool)
            or stop.get("worker_pid") <= 0
            or stop.get("error_type") != "RuntimeError"
            or stop.get("error")
            != _DEEPSEEK_TRANSPORT_INSPECTOR_STOP_ERROR):
        raise RuntimeError(
            "DeepSeek transport inspector follow-up boundary is invalid")
    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = config.get("samples") or []
    _assert_worker_leases_free(out_dir, samples)

    manifest_digest = _sha256_file(os.path.join(
        out_dir, "dispatch_manifest.json"))
    prior_commit = manifest.get("run_git_commit")
    prior_fingerprint = manifest.get("code_fingerprint")
    prior_recovery_commit = prior_authorization.get("recovery_git_commit")
    prior_recovery_fingerprint = prior_authorization.get(
        "recovery_code_fingerprint")
    if (manifest_digest
            != prior_authorization.get("dispatch_manifest_sha256")
            or prior_commit != prior_authorization.get("prior_git_commit")
            or prior_fingerprint
            != prior_authorization.get("prior_code_fingerprint")
            or not isinstance(prior_recovery_commit, str)
            or not isinstance(prior_recovery_fingerprint, dict)):
        raise RuntimeError(
            "DeepSeek transport inspector prior authorization has drifted")

    current_commit, current_tree = _git_identity()
    if current_tree != "clean" or current_commit == prior_recovery_commit:
        raise RuntimeError(
            "DeepSeek transport inspector follow-up requires a new clean "
            "Git commit"
        )
    delta_changed_paths = _git_changed_paths(
        prior_recovery_commit, current_commit)
    required_delta_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    if set(delta_changed_paths) != required_delta_paths:
        raise RuntimeError(
            "DeepSeek transport inspector follow-up commit scope is invalid")

    recovery_fingerprint = code_fingerprint()
    delta_fingerprint_changes = sorted(
        key for key in set(prior_recovery_fingerprint) | set(
            recovery_fingerprint)
        if prior_recovery_fingerprint.get(key)
        != recovery_fingerprint.get(key)
    )
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    changed_paths = _git_changed_paths(prior_commit, current_commit)
    required_cumulative_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/model_openai.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
        "transport/src/model_openai.py",
        "transport/src/test_model_openai.py",
    }
    if (delta_fingerprint_changes != ["run_meta.py"]
            or fingerprint_changes != ["model_openai.py", "run_meta.py"]
            or set(changed_paths) != required_cumulative_paths):
        raise RuntimeError(
            "DeepSeek transport inspector follow-up fingerprint scope is "
            "invalid"
        )

    scope = _deepseek_checkpoint_resume_scope(out_dir, manifest)
    prior_incidents = prior_authorization.get("incident_api_rows") or []
    observed_incidents = [
        (number, _canonical_record_sha256(row))
        for number, row in scope["incident_api_rows"]
    ]
    expected_incidents = [
        (item.get("row_number"), item.get("canonical_sha256"))
        for item in prior_incidents if isinstance(item, dict)
    ]
    if (observed_incidents != expected_incidents
            or scope["resume_samples"]
            != prior_authorization.get("deepseek_resume_samples")
            or scope["pending_samples"]
            != prior_authorization.get("deepseek_pending_samples")
            or scope["recovered_workers"]
            != prior_authorization.get("deepseek_recovered_workers")):
        raise RuntimeError(
            "DeepSeek transport inspector follow-up evidence has drifted")

    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    dispatch_rows = _read_jsonl(dispatch_path)
    prior_events = [
        index for index, row in enumerate(dispatch_rows)
        if (row.get("event")
            == "user_authorized_deepseek_transport_disconnect_retry_recovery"
            and row.get("campaign_recovery_authorization_id")
            == prior_authorization.get("authorization_id")
            and row.get("campaign_recovery_authorization_sha256")
            == prior_authorization_sha256)
    ]
    if len(prior_events) != 1:
        raise RuntimeError(
            "DeepSeek transport inspector prior authorization event is "
            "invalid"
        )
    post_authorization_rows = dispatch_rows[prior_events[0] + 1:]
    if (len(post_authorization_rows) != 1
            or post_authorization_rows[0].get("event") != "campaign_stop"
            or post_authorization_rows[0].get("error")
            != _DEEPSEEK_TRANSPORT_INSPECTOR_STOP_ERROR
            or (post_authorization_rows[0].get(
                "worker_reconciliation") or {}).get(
                    "closed_invocations") != []
            or (post_authorization_rows[0].get(
                "worker_reconciliation") or {}).get(
                    "audited_invocations") != []):
        raise RuntimeError(
            "DeepSeek transport inspector follow-up was not zero-worker")

    dispatcher_stopped_rows = [
        row
        for (_number, row), entry in zip(
            scope["incident_api_rows"], prior_incidents)
        if entry.get("incident_kind")
        == "deepseek_dispatcher_stopped_uncommitted_api"
    ]
    if len(dispatcher_stopped_rows) != 1:
        raise RuntimeError(
            "DeepSeek transport inspector partial-stream scope is invalid")
    incident_transport_sidecars = [
        _deepseek_dispatcher_stopped_sidecar_evidence(
            out_dir, dispatcher_stopped_rows[0])
    ]
    prefix_evidence = {
        name: _file_prefix_evidence(os.path.join(out_dir, name))
        for name in (
            "api_calls.jsonl",
            "dispatch_log.jsonl",
        )
    }
    metadata_identities = _run_metadata_recovery_identities(
        os.path.join(out_dir, "run_metadata.jsonl"))
    created_at = datetime.now().astimezone().isoformat(
        timespec="seconds")
    authorization_id = "dsi-" + uuid.uuid4().hex[:12]
    history_dir = os.path.join(
        out_dir, "recovery_history", authorization_id)
    archived_authorization = os.path.join(
        history_dir, "superseded_campaign_recovery_authorization.json")
    archived_stop = os.path.join(history_dir, "campaign_stop.json")
    emergency_dir = os.path.join(out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_names = (
        sorted(
            name for name in os.listdir(emergency_dir)
            if name.endswith(".json"))
        if os.path.isdir(emergency_dir) else []
    )
    if emergency_names:
        raise RuntimeError(
            "DeepSeek transport inspector follow-up requires one canonical "
            "stop latch")

    record = json.loads(json.dumps(prior_authorization))
    record.update({
        "authorization_id": authorization_id,
        "created_at": created_at,
        "authorization_basis": (
            "explicit_user_resume_after_deepseek_recovery_inspector_fix"
        ),
        "deepseek_transport_disconnect_inspector_followup_recovery": True,
        "deepseek_transport_disconnect_inspector_prior_authorization_id": (
            prior_authorization.get("authorization_id")),
        "deepseek_transport_disconnect_inspector_prior_authorization_sha256": (
            prior_authorization_sha256),
        "deepseek_transport_disconnect_inspector_prior_recovery_git_commit": (
            prior_recovery_commit),
        "deepseek_transport_disconnect_inspector_delta_changed_paths": (
            delta_changed_paths),
        "deepseek_transport_disconnect_inspector_prefixes": prefix_evidence,
        "deepseek_transport_disconnect_inspector_"
        "run_metadata_identities": metadata_identities,
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": recovery_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": os.path.relpath(
            archived_stop, out_dir).replace("\\", "/"),
        "archived_stop_sha256": _sha256_file(stop_path),
        "archived_emergency_stop_records": [],
        "superseded_authorization_path": os.path.relpath(
            archived_authorization, out_dir).replace("\\", "/"),
        "superseded_authorization_sha256": _sha256_file(
            auth_path),
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
        "incident_transport_sidecars": incident_transport_sidecars,
    })
    pending_path = os.path.join(
        out_dir, DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME)
    pending = {
        "schema": _DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA,
        "created_at": created_at,
        "history_dir": os.path.relpath(
            history_dir, out_dir).replace("\\", "/"),
        "authorization_record": record,
    }
    write_json_atomic(pending_path, pending)
    return _commit_deepseek_transport_inspector_followup(
        out_dir, manifest, stop_path, auth_path, pending_path, pending)


def _authorize_deepseek_resume_classifier_followup(
        out_dir, manifest, stop, stop_path, auth_path, prior_authorization):
    """Supersede the exact zero-worker recovered-worker classifier gap."""
    config = manifest.get("config") or {}
    prior_authorization_sha256 = _sha256_file(auth_path)
    if (prior_authorization.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or prior_authorization.get("recovery_kind")
            != LEDGER_LOCK_RECOVERY_KIND
            or prior_authorization.get(
                "deepseek_transport_disconnect_retry_recovery") is not True
            or prior_authorization.get(
                "deepseek_transport_disconnect_inspector_followup_recovery")
            is not True
            or prior_authorization.get(
                "deepseek_resume_classifier_followup_recovery") is not None
            or prior_authorization.get("authorization_basis")
            != (
                "explicit_user_resume_after_deepseek_recovery_"
                "inspector_fix"
            )
            or config.get("campaign_role") != "deepseek_full234"
            or config.get("num_round_trips")
            != DEEPSEEK_FULL234_ROUND_TRIPS
            or config.get("transport") != DEEPSEEK_TRANSPORT
            or config.get("transport_revision")
            != DEEPSEEK_TRANSPORT_REVISION
            or stop.get("schema") != STOP_CONDITION_SCHEMA
            or stop.get("condition") != "dispatcher_integrity_failure"
            or stop.get("worker_launch_id") is not None
            or not isinstance(stop.get("worker_pid"), int)
            or isinstance(stop.get("worker_pid"), bool)
            or stop.get("worker_pid") <= 0
            or stop.get("error_type") != "RuntimeError"
            or not isinstance(stop.get("error"), str)
            or not stop["error"].startswith(
                _DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX)):
        raise RuntimeError(
            "DeepSeek resume-classifier follow-up boundary is invalid")

    remainder = stop["error"][
        len(_DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX):]
    sample, separator, _serialized_evidence = remainder.partition(
        "; evidence=")
    assignments = {
        item.get("sample"): item
        for item in manifest.get("assignments") or []
        if isinstance(item, dict)
    }
    assignment = assignments.get(sample)
    recovered_workers = {
        item.get("sample"): item
        for item in prior_authorization.get(
            "deepseek_recovered_workers", [])
        if isinstance(item, dict)
    }
    recovered = recovered_workers.get(sample)
    evidence = (
        _queued_pending_evidence(
            out_dir, sample, assignment.get("methods") or [])
        if isinstance(assignment, dict) else None
    )
    expected_error = (
        _DEEPSEEK_RESUME_CLASSIFIER_STOP_PREFIX
        + sample + f"; evidence={evidence}"
        if isinstance(evidence, list) else None
    )
    if (not separator
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", sample)
            or not isinstance(assignment, dict)
            or not isinstance(recovered, dict)
            or recovered.get("status") != "failed"
            or sample not in (
                prior_authorization.get("deepseek_resume_samples") or [])
            or sample in (
                prior_authorization.get("deepseek_pending_samples") or [])
            or not evidence
            or stop.get("error") != expected_error):
        raise RuntimeError(
            "DeepSeek resume-classifier failure evidence is invalid")

    active = _read_json(os.path.join(out_dir, "active_worker_set.json"))
    if active.get("workers") != {}:
        raise RuntimeError("workers are still active")
    samples = config.get("samples") or []
    _assert_worker_leases_free(out_dir, samples)

    manifest_digest = _sha256_file(os.path.join(
        out_dir, "dispatch_manifest.json"))
    prior_commit = manifest.get("run_git_commit")
    prior_fingerprint = manifest.get("code_fingerprint")
    prior_recovery_commit = prior_authorization.get("recovery_git_commit")
    prior_recovery_fingerprint = prior_authorization.get(
        "recovery_code_fingerprint")
    if (manifest_digest
            != prior_authorization.get("dispatch_manifest_sha256")
            or prior_commit != prior_authorization.get("prior_git_commit")
            or prior_fingerprint
            != prior_authorization.get("prior_code_fingerprint")
            or not isinstance(prior_recovery_commit, str)
            or not isinstance(prior_recovery_fingerprint, dict)):
        raise RuntimeError(
            "DeepSeek resume-classifier prior authorization has drifted")

    current_commit, current_tree = _git_identity()
    recovery_fingerprint = code_fingerprint()
    parent_line = subprocess.run(
        ["git", "rev-list", "--parents", "-n", "1", current_commit],
        check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.split()
    delta_changed_paths = _git_changed_paths(
        prior_recovery_commit, current_commit)
    required_delta_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
    }
    delta_fingerprint_changes = sorted(
        key for key in set(prior_recovery_fingerprint) | set(
            recovery_fingerprint)
        if prior_recovery_fingerprint.get(key)
        != recovery_fingerprint.get(key)
    )
    fingerprint_changes = sorted(
        key for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    changed_paths = _git_changed_paths(prior_commit, current_commit)
    required_cumulative_paths = {
        "HP_V8/src/authorize_ledger_lock_recovery.py",
        "HP_V8/src/model_openai.py",
        "HP_V8/src/paired_campaign_dispatch.py",
        "HP_V8/src/run_meta.py",
        "HP_V8/src/test_model_openai.py",
        "transport/src/model_openai.py",
        "transport/src/test_model_openai.py",
    }
    if (current_tree != "clean"
            or parent_line != [current_commit, prior_recovery_commit]
            or set(delta_changed_paths) != required_delta_paths
            or delta_fingerprint_changes != ["run_meta.py"]
            or fingerprint_changes != ["model_openai.py", "run_meta.py"]
            or set(changed_paths) != required_cumulative_paths):
        raise RuntimeError(
            "DeepSeek resume-classifier follow-up commit scope is invalid")

    scope = _deepseek_checkpoint_resume_scope(out_dir, manifest)
    observed_incidents = [
        (number, _canonical_record_sha256(row))
        for number, row in scope["incident_api_rows"]
    ]
    expected_incidents = [
        (item.get("row_number"), item.get("canonical_sha256"))
        for item in prior_authorization.get("incident_api_rows") or []
        if isinstance(item, dict)
    ]
    if (observed_incidents != expected_incidents
            or scope["resume_samples"]
            != prior_authorization.get("deepseek_resume_samples")
            or scope["pending_samples"]
            != prior_authorization.get("deepseek_pending_samples")
            or scope["recovered_workers"]
            != prior_authorization.get("deepseek_recovered_workers")):
        raise RuntimeError(
            "DeepSeek resume-classifier follow-up evidence has drifted")

    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    dispatch_rows = _read_jsonl(dispatch_path)
    prior_events = [
        index for index, row in enumerate(dispatch_rows)
        if (row.get("event")
            == "user_authorized_deepseek_transport_inspector_followup"
            and row.get("campaign_recovery_authorization_id")
            == prior_authorization.get("authorization_id")
            and row.get("campaign_recovery_authorization_sha256")
            == prior_authorization_sha256)
    ]
    if len(prior_events) != 1:
        raise RuntimeError(
            "DeepSeek resume-classifier prior authorization event is invalid")
    post_authorization_rows = dispatch_rows[prior_events[0] + 1:]
    if (len(post_authorization_rows) != 1
            or post_authorization_rows[0].get("event") != "campaign_stop"
            or post_authorization_rows[0].get("error") != expected_error
            or (post_authorization_rows[0].get(
                "worker_reconciliation") or {}).get(
                    "closed_invocations") != []
            or (post_authorization_rows[0].get(
                "worker_reconciliation") or {}).get(
                    "audited_invocations") != []):
        raise RuntimeError(
            "DeepSeek resume-classifier follow-up was not zero-worker")

    prefix_evidence = {
        name: _file_prefix_evidence(os.path.join(out_dir, name))
        for name in ("api_calls.jsonl", "dispatch_log.jsonl")
    }
    metadata_identities = _run_metadata_recovery_identities(
        os.path.join(out_dir, "run_metadata.jsonl"))
    created_at = datetime.now().astimezone().isoformat(timespec="seconds")
    authorization_id = "dsr-" + uuid.uuid4().hex[:12]
    history_relative = "recovery_history/" + authorization_id
    record = json.loads(json.dumps(prior_authorization))
    record.update({
        "authorization_id": authorization_id,
        "created_at": created_at,
        "authorization_basis": (
            _DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS),
        "deepseek_resume_classifier_followup_recovery": True,
        "deepseek_resume_classifier_prior_authorization_id": (
            prior_authorization.get("authorization_id")),
        "deepseek_resume_classifier_prior_authorization_sha256": (
            prior_authorization_sha256),
        "deepseek_resume_classifier_prior_recovery_git_commit": (
            prior_recovery_commit),
        "deepseek_resume_classifier_delta_changed_paths": (
            delta_changed_paths),
        "deepseek_resume_classifier_prefixes": prefix_evidence,
        "deepseek_resume_classifier_run_metadata_identities": (
            metadata_identities),
        "deepseek_resume_classifier_sample": sample,
        "deepseek_resume_classifier_stop_error": expected_error,
        "deepseek_resume_classifier_evidence": evidence,
        "recovery_git_commit": current_commit,
        "recovery_git_tree_state": "clean",
        "recovery_code_fingerprint": recovery_fingerprint,
        "changed_code_fingerprint_keys": fingerprint_changes,
        "changed_tracked_paths": changed_paths,
        "archived_stop_path": (
            history_relative + "/campaign_stop.json"),
        "archived_stop_sha256": _sha256_file(stop_path),
        "archived_emergency_stop_records": [],
        "superseded_authorization_path": (
            history_relative
            + "/superseded_campaign_recovery_authorization.json"),
        "superseded_authorization_sha256": prior_authorization_sha256,
        "committed_results_modified": False,
        "checkpoint_rows_modified": False,
        "provider_post_replay_scope": "uncommitted_steps_only",
    })
    pending_path = os.path.join(
        out_dir, DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME)
    pending = {
        "schema": _DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA,
        "followup_kind": "resume_classifier",
        "created_at": created_at,
        "history_dir": history_relative,
        "authorization_record": record,
    }
    write_json_atomic(pending_path, pending)
    return _commit_deepseek_transport_inspector_followup(
        out_dir, manifest, stop_path, auth_path, pending_path, pending)


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
        out_dir, manifest, stop_path, auth_path, prior_authorization,
        validated_prior_authorization_sha256=None, *,
        stop_condition="dispatcher_process_lost",
        authorization_basis=(
            "explicit_user_resume_after_dispatcher_process_loss"),
        operator_pause_recovery=False):
    """Authorize an exact DeepSeek checkpoint resume after parent loss."""
    pending_path = os.path.join(
        out_dir, _DISPATCHER_PARENT_LOSS_PENDING_FILENAME)
    if os.path.isfile(pending_path):
        pending = _read_json(pending_path)
        pending_record = (
            pending.get("authorization_record")
            if isinstance(pending, dict) else None
        )
        pending_is_operator_pause = (
            isinstance(pending_record, dict)
            and pending_record.get("dispatcher_operator_pause_recovery")
            is True
        )
        if pending_is_operator_pause != bool(operator_pause_recovery):
            required_mode = (
                "--operator_dispatcher_pause"
                if pending_is_operator_pause
                else "--dispatcher_process_lost"
            )
            raise RuntimeError(
                "dispatcher recovery pending transaction requires "
                + required_mode)
        pending = _reprepare_dispatcher_parent_loss_pending_short_path(
            out_dir, manifest, pending_path, pending, auth_path,
            stop_path)
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
    code_transition = None
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
                != prior_fingerprint):
            raise RuntimeError(
                "dispatcher parent-loss prior authorization identity drift")
        prior_identity_matches = (
            prior_authorization.get("recovery_git_commit")
            == current_commit
            and prior_authorization.get("recovery_code_fingerprint")
            == current_fingerprint
        )
        if prior_identity_matches:
            verified_prior = read_campaign_recovery_authorization(
                out_dir, allow_active_dispatcher_stop=True)
            if (not isinstance(verified_prior, dict)
                    or verified_prior.get("authorization_id")
                    != prior_authorization.get("authorization_id")
                    or verified_prior.get("authorization_sha256")
                    != _sha256_file(auth_path)):
                raise RuntimeError(
                    "dispatcher parent-loss prior authorization is invalid")
        else:
            code_transition = (
                _validated_dispatcher_parent_loss_code_transition(
                    manifest,
                    manifest_digest,
                    auth_path,
                    prior_authorization,
                    validated_prior_authorization_sha256,
                    current_commit,
                    current_tree,
                    current_fingerprint,
                )
            )

    stop_records = read_campaign_stop_conditions(out_dir)
    if not stop_records and stop_condition == "dispatcher_process_lost":
        _record_stopless_registered_prelaunch_parent_loss(
            out_dir, manifest, stop_path)
        stop_records = read_campaign_stop_conditions(out_dir)
    if (not stop_records
            or any(
                row.get("condition") != stop_condition
                for row in stop_records)):
        raise RuntimeError(
            "campaign is not stopped by the expected dispatcher boundary")

    preflight_recovery_scope = None
    preflight_incidents = None
    if operator_pause_recovery:
        active_path = os.path.join(out_dir, "active_worker_set.json")
        canonical_stop = _read_json(stop_path)
        active_record = _read_json(active_path)
        if (canonical_stop.get("worker_launch_id") is not None
                or canonical_stop.get("dispatch_manifest_sha256")
                != manifest_digest
                or canonical_stop.get("active_worker_set_sha256")
                != _sha256_file(active_path)
                or canonical_stop.get(
                    "active_worker_set_canonical_sha256")
                != _canonical_record_sha256(active_record)
                or canonical_stop.get("publication_mode")
                not in {"canonical", "emergency_fallback"}):
            raise RuntimeError(
                "operator pause manifest or active-worker witness has drifted")
        # DeepSeek /6 has no durable response journal.  A provider request
        # that was open, or a complete API response not yet linked to a
        # committed RT, cannot be resumed without risking a second POST.
        # Refuse before creating history/pending artifacts so the paused
        # campaign remains byte-for-byte available for a future replay tool.
        preflight_recovery_scope = _reconcile_deepseek_parent_loss_workers(
            out_dir, manifest, stop_records, apply=False,
            stop_condition=stop_condition)
        preflight_incidents = _deepseek_parent_loss_incidents(
            out_dir, manifest, preflight_recovery_scope)
        if any(preflight_incidents.get(name) for name in (
                "api", "attempts", "transport_sidecars")):
            raise RuntimeError(
                "operator pause recovery is blocked by ambiguous or "
                "uncommitted provider activity; DeepSeek durable replay is "
                "required before resume"
            )
        if prior_authorization is not None and any(
                prior_authorization.get(name) for name in (
                    "incident_api_rows", "incident_attempt_rows",
                    "incident_transport_sidecars")):
            raise RuntimeError(
                "operator pause recovery cannot supersede prior provider "
                "replay incidents"
            )

    authorization_id = "dpl-" + uuid.uuid4().hex[:12]
    history_dir = os.path.join(
        out_dir, _DISPATCHER_PARENT_LOSS_HISTORY_ROOT,
        authorization_id)
    emergency_dir = os.path.join(
        out_dir, EMERGENCY_STOP_DIRECTORY)
    emergency_names = (
        sorted(
            name for name in os.listdir(emergency_dir)
            if name.endswith(".json"))
        if os.path.isdir(emergency_dir) else []
    )
    _assert_dispatcher_parent_loss_path_budget(
        history_dir, emergency_names)
    os.makedirs(history_dir, exist_ok=False)
    active_path = os.path.join(out_dir, "active_worker_set.json")
    metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
    archived_active = os.path.join(
        history_dir, "active_worker_set.before.json")
    archived_metadata = os.path.join(
        history_dir, "run_metadata.before.jsonl")
    _copy_file_durable(active_path, archived_active)
    has_metadata = (
        os.path.isfile(metadata_path)
        or os.path.isfile(os.path.join(out_dir, METADATA_EVENTS_FILENAME))
    )
    captured_metadata_identities, metadata_before = (
        _capture_run_metadata_recovery_snapshot(metadata_path)
        if has_metadata else (None, [])
    )
    metadata_identities = (
        captured_metadata_identities
        if isinstance(captured_metadata_identities, dict) else None
    )
    if metadata_identities is not None:
        _write_jsonl_atomic(archived_metadata, metadata_before)
    else:
        _copy_file_durable(
            metadata_path, archived_metadata, allow_missing=True)

    recovery_scope = (
        preflight_recovery_scope
        if preflight_recovery_scope is not None else
        _reconcile_deepseek_parent_loss_workers(
            out_dir, manifest, stop_records, apply=False,
            stop_condition=stop_condition)
    )
    incidents = (
        preflight_incidents
        if preflight_incidents is not None else
        _deepseek_parent_loss_incidents(
            out_dir, manifest, recovery_scope)
    )
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
    record.pop("deepseek_transport_disconnect_retry_recovery", None)
    record.pop(
        "deepseek_transport_disconnect_inspector_followup_recovery",
        None,
    )
    record.pop("deepseek_resume_classifier_followup_recovery", None)
    reader_prefix = (
        "dispatcher_parent_loss_authorization_reader_sha_reprepared_")
    for key in list(record):
        if key.startswith(reader_prefix):
            record.pop(key)
    record.update({
        "schema": CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2,
        "authorization_id": authorization_id,
        "recovery_kind": DISPATCHER_PROCESS_LOST_RECOVERY_KIND,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "authorization_basis": authorization_basis,
        "dispatcher_process_lost_recovery": True,
        "dispatcher_stop_condition": stop_condition,
        "dispatcher_operator_pause_recovery": operator_pause_recovery,
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
        "dispatcher_parent_loss_run_metadata_identities": (
            metadata_identities),
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
        "operator_pre_provider_api_rows": (
            list(incidents.get("pre_provider_api") or [])
            if operator_pause_recovery else []
        ),
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
        "provider_post_replay_scope": (
            "none_required" if operator_pause_recovery
            else "uncommitted_steps_only"
        ),
    })
    if code_transition is None:
        record.pop("dispatcher_parent_loss_code_transition", None)
    else:
        record["authorization_basis"] = (
            authorization_basis + "_and_recovery_tool_fix"
        )
        record["dispatcher_parent_loss_code_transition"] = code_transition
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


def _authorize_dispatcher_parent_loss_reader_sha_fix(
        out_dir, manifest, auth_path,
        validated_current_authorization_sha256):
    """Rebind one completed parent-loss authorization to the SHA reader fix."""
    if (not os.path.isfile(auth_path)
            or read_campaign_stop_conditions(out_dir)
            or any(os.path.isfile(os.path.join(out_dir, name)) for name in (
                DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
                DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            ))):
        raise RuntimeError(
            "dispatcher parent-loss reader-SHA fix requires one completed "
            "recovery with no active stop or pending transaction")
    record = _read_json(auth_path)
    current_authorization_sha256 = _sha256_file(auth_path)
    current_commit, current_tree = _git_identity()
    current_fingerprint = code_fingerprint()
    prefix = (
        "dispatcher_parent_loss_authorization_reader_sha_reprepared_")
    provenance_fields = {
        prefix + suffix
        for suffix in ("from_commit", "from_sha256", "from_path", "at")
    }
    if (record.get("schema")
            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2
            or record.get("recovery_kind")
            != DISPATCHER_PROCESS_LOST_RECOVERY_KIND
            or record.get("dispatcher_process_lost_recovery") is not True
            or re.fullmatch(
                r"dpl-[0-9a-f]{12}",
                str(record.get("authorization_id") or ""),
            ) is None
            or current_tree != "clean"
            or not isinstance(current_fingerprint, dict)
            or _sha256_file(os.path.join(
                out_dir, "dispatch_manifest.json"))
            != record.get("dispatch_manifest_sha256")
            or manifest.get("run_git_commit")
            != record.get("prior_git_commit")
            or manifest.get("code_fingerprint")
            != record.get("prior_code_fingerprint")):
        raise RuntimeError(
            "dispatcher parent-loss reader-SHA authorization is invalid")

    active = _read_json(os.path.join(
        out_dir, "active_worker_set.json"))
    if (active.get("schema") != "anchorpatch.active_worker_set/1"
            or active.get("run_git_commit")
            != manifest.get("run_git_commit")
            or active.get("dispatcher_pid") is not None
            or active.get("dispatcher_instance_id") is not None
            or active.get("workers") != {}):
        raise RuntimeError(
            "dispatcher parent-loss reader-SHA active set is invalid")
    _assert_worker_leases_free(
        out_dir,
        [
            item.get("sample")
            for item in record.get(
                "dispatcher_parent_loss_workers") or []
            if isinstance(item, dict)
        ],
    )

    def _finalize_result(already_authorized):
        authorization_sha256 = _sha256_file(auth_path)
        event = {
            "event": (
                "user_authorized_dispatcher_parent_loss_reader_sha_fix"),
            "created_at": record.get(prefix + "at"),
            "campaign_recovery_authorization_id": record[
                "authorization_id"],
            "campaign_recovery_authorization_sha256": authorization_sha256,
            "superseded_authorization_sha256": record[
                prefix + "from_sha256"],
            "prior_recovery_git_commit": record[
                prefix + "from_commit"],
            "recovery_git_commit": record["recovery_git_commit"],
        }
        dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
        matches = [
            row for row in _read_jsonl(dispatch_path)
            if row.get("event") == event["event"]
            and row.get("campaign_recovery_authorization_id")
            == record["authorization_id"]
        ]
        if not matches:
            append_jsonl_locked(dispatch_path, event)
        elif len(matches) != 1 or matches[0] != event:
            raise RuntimeError(
                "dispatcher parent-loss reader-SHA witness has drifted")
        campaign_recovery_incident_evidence.cache_clear()
        verified = read_campaign_recovery_authorization(out_dir)
        if verified["authorization_sha256"] != authorization_sha256:
            raise RuntimeError(
                "dispatcher parent-loss reader-SHA result digest mismatch")
        return {
            "authorization_id": record["authorization_id"],
            "authorization_sha256": authorization_sha256,
            "superseded_authorization_sha256": record[
                prefix + "from_sha256"],
            "reader_sha_fixed": True,
            "already_authorized": already_authorized,
        }

    if (current_commit == record.get("recovery_git_commit")
            and current_fingerprint
            == record.get("recovery_code_fingerprint")):
        if {
                key for key in record if key.startswith(prefix)
        } != provenance_fields or (
                validated_current_authorization_sha256
                != record.get(prefix + "from_sha256")):
            raise RuntimeError(
                "dispatcher parent-loss reader-SHA provenance is missing")
        return _finalize_result(True)

    prior_recovery_commit = (
        record.get("dispatcher_parent_loss_code_transition") or {}
    ).get("prior_recovery_git_commit")
    prepared_commit = record.get("recovery_git_commit")
    prepared_fingerprint = record.get("recovery_code_fingerprint")
    prior_recovery_fingerprint = (
        record.get("dispatcher_parent_loss_code_transition") or {}
    ).get("prior_recovery_code_fingerprint")
    if (current_authorization_sha256
            != validated_current_authorization_sha256
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(validated_current_authorization_sha256 or ""),
            ) is None
            or re.fullmatch(
                r"[0-9a-f]{40}", str(prepared_commit or "")) is None
            or re.fullmatch(
                r"[0-9a-f]{40}",
                str(prior_recovery_commit or ""),
            ) is None
            or not isinstance(prepared_fingerprint, dict)
            or not isinstance(prior_recovery_fingerprint, dict)
            or any(key.startswith(prefix) for key in record)):
        raise RuntimeError(
            "dispatcher parent-loss reader-SHA prior authority is invalid")
    metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
    if any(
            isinstance(row.get("campaign_recovery_authorization"), dict)
            and row["campaign_recovery_authorization"].get(
                "authorization_id") == record["authorization_id"]
            for row in read_run_metadata_snapshot(out_dir)):
        raise RuntimeError(
            "dispatcher parent-loss reader-SHA fix cannot amend an "
            "authorization already bound into run metadata")

    def _parent_line(commit):
        return subprocess.run(
            ["git", "rev-list", "--parents", "-n", "1", commit],
            check=True, capture_output=True, text=True, encoding="utf-8",
        ).stdout.split()

    prepared_delta_paths = _git_changed_paths(
        prior_recovery_commit, prepared_commit)
    current_delta_paths = _git_changed_paths(
        prior_recovery_commit, current_commit)
    amendment_paths = _git_changed_paths(
        prepared_commit, current_commit)
    cumulative_paths = _git_changed_paths(
        record.get("prior_git_commit"), current_commit)
    prepared_to_current_fingerprint_changes = sorted(
        key for key in set(prepared_fingerprint) | set(
            current_fingerprint)
        if prepared_fingerprint.get(key)
        != current_fingerprint.get(key)
    )
    prior_to_current_fingerprint_changes = sorted(
        key for key in set(prior_recovery_fingerprint) | set(
            current_fingerprint)
        if prior_recovery_fingerprint.get(key)
        != current_fingerprint.get(key)
    )
    cumulative_fingerprint_changes = sorted(
        key for key in set(record.get("prior_code_fingerprint") or {}) | set(
            current_fingerprint)
        if (record.get("prior_code_fingerprint") or {}).get(key)
        != current_fingerprint.get(key)
    )
    if (_parent_line(prepared_commit)
            != [prepared_commit, prior_recovery_commit]
            or _parent_line(current_commit)
            != [current_commit, prior_recovery_commit]
            or set(prepared_delta_paths)
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            or set(current_delta_paths)
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            or set(amendment_paths)
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            or prepared_to_current_fingerprint_changes
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS
            or prior_to_current_fingerprint_changes
            != _DISPATCHER_PARENT_LOSS_CODE_TRANSITION_FINGERPRINT_KEYS
            or cumulative_paths != record.get("changed_tracked_paths")
            or cumulative_fingerprint_changes
            != record.get("changed_code_fingerprint_keys")):
        raise RuntimeError(
            "dispatcher parent-loss reader-SHA tooling amendment is invalid")

    _validate_dispatcher_parent_loss_pending_reprepare_witnesses(
        out_dir, record)
    history_relative = record["archived_stop_path"].rsplit("/", 1)[0]
    archived_relative = (
        history_relative + "/authorization.reader-sha-before.json")
    archived_path = os.path.join(out_dir, archived_relative)
    _ensure_bound_file_copy(
        auth_path, archived_path, current_authorization_sha256)
    updated = json.loads(json.dumps(record))
    updated["recovery_git_commit"] = current_commit
    updated["recovery_code_fingerprint"] = current_fingerprint
    updated["dispatcher_parent_loss_code_transition"][
        "delta_changed_paths"] = current_delta_paths
    updated["dispatcher_parent_loss_code_transition"][
        "delta_changed_code_fingerprint_keys"
    ] = prior_to_current_fingerprint_changes
    reprepared_at = datetime.now().astimezone().isoformat(
        timespec="seconds")
    updated[prefix + "from_commit"] = prepared_commit
    updated[prefix + "from_sha256"] = current_authorization_sha256
    updated[prefix + "from_path"] = archived_relative
    updated[prefix + "at"] = reprepared_at
    write_json_atomic(auth_path, updated)
    record = updated
    return _finalize_result(False)


def authorize(
        out_dir, *, operator_pause=False, operator_pause_reason=None,
        operator_interrupted_samples=None, provider_access_retry=False,
        deepseek_server_retry=False,
        deepseek_transport_disconnect_retry=False,
        deepseek_resume_classifier_retry=False,
        dispatcher_process_lost=False,
        validated_prior_authorization_sha256=None,
        dispatcher_parent_loss_reader_sha_fix=False,
        validated_current_authorization_sha256=None):
    out_dir = os.path.abspath(out_dir)
    selected_modes = sum(bool(value) for value in (
        operator_pause, provider_access_retry, deepseek_server_retry,
        deepseek_transport_disconnect_retry,
        deepseek_resume_classifier_retry, dispatcher_process_lost,
        dispatcher_parent_loss_reader_sha_fix,
    ))
    if selected_modes > 1:
        raise RuntimeError("campaign recovery modes are mutually exclusive")
    if (validated_prior_authorization_sha256 is not None
            and not dispatcher_process_lost):
        raise RuntimeError(
            "validated prior authorization digest is only valid for "
            "dispatcher parent-loss recovery")
    if (validated_current_authorization_sha256 is not None
            and not dispatcher_parent_loss_reader_sha_fix):
        raise RuntimeError(
            "validated current authorization digest is only valid for "
            "dispatcher parent-loss reader-SHA fix")
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    stop_path = os.path.join(out_dir, "campaign_stop.json")
    auth_path = os.path.join(
        out_dir, CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
    if dispatcher_parent_loss_reader_sha_fix:
        if not validated_current_authorization_sha256:
            raise RuntimeError(
                "dispatcher parent-loss reader-SHA fix requires the exact "
                "current authorization digest")
        lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
        with open(lease_path, "a+", encoding="utf-8") as lease:
            try:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.LockException as exc:
                raise RuntimeError(
                    "dispatcher parent-loss reader-SHA fix requires the "
                    "dispatcher lease to be free") from exc
            try:
                manifest = _read_json(manifest_path)
                with _campaign_stop_publication_lock(out_dir):
                    return _authorize_dispatcher_parent_loss_reader_sha_fix(
                        out_dir,
                        manifest,
                        auth_path,
                        validated_current_authorization_sha256,
                    )
            finally:
                portalocker.unlock(lease)
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
                        prior_authorization,
                        validated_prior_authorization_sha256)
            finally:
                portalocker.unlock(lease)
    parent_loss_pending_path = os.path.join(
        out_dir, DISPATCHER_PARENT_LOSS_PENDING_FILENAME)
    if operator_pause and (
            os.path.exists(stop_path)
            or os.path.isfile(parent_loss_pending_path)):
        if operator_interrupted_samples:
            raise RuntimeError(
                "dispatcher-recorded operator pause derives its worker "
                "cohort from the durable active set; explicit sample scope "
                "is not allowed"
            )
        lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
        with open(lease_path, "a+", encoding="utf-8") as lease:
            try:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.LockException as exc:
                raise RuntimeError(
                    "operator pause recovery requires the dispatcher lease "
                    "to be free"
                ) from exc
            try:
                with _campaign_stop_publication_lock(out_dir):
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
                    if os.path.exists(stop_path):
                        stop = _read_json(stop_path)
                        if (stop.get("condition")
                                != "operator_directed_dispatcher_pause"):
                            raise RuntimeError(
                                "operator pause recovery boundary is invalid")
                    return _authorize_dispatcher_process_lost(
                        out_dir, manifest, stop_path, auth_path,
                        prior_authorization,
                        stop_condition=(
                            "operator_directed_dispatcher_pause"),
                        authorization_basis=(
                            "explicit_user_resume_after_operator_directed_"
                            "dispatcher_pause"),
                        operator_pause_recovery=True,
                    )
            finally:
                portalocker.unlock(lease)
    if deepseek_transport_disconnect_retry:
        if (operator_pause or operator_interrupted_samples
                or provider_access_retry):
            raise RuntimeError(
                "DeepSeek transport-disconnect retry cannot be combined "
                "with other recovery modes"
            )
        lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
        with open(lease_path, "a+", encoding="utf-8") as lease:
            try:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.LockException as exc:
                raise RuntimeError(
                    "DeepSeek transport-disconnect recovery requires the "
                    "dispatcher lease to be free") from exc
            try:
                with _campaign_stop_publication_lock(out_dir):
                    manifest = _read_json(manifest_path)
                    pending_path = os.path.join(
                        out_dir,
                        DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
                    )
                    parent_loss_pending_path = os.path.join(
                        out_dir, DISPATCHER_PARENT_LOSS_PENDING_FILENAME)
                    if os.path.isfile(parent_loss_pending_path):
                        raise RuntimeError(
                            "dispatcher parent-loss recovery transaction "
                            "must be completed first")
                    if os.path.isfile(pending_path):
                        pending = _read_json(pending_path)
                        if pending.get("followup_kind") == "resume_classifier":
                            raise RuntimeError(
                                "DeepSeek resume-classifier recovery "
                                "requires its explicit recovery mode")
                        pending = (
                            _reprepare_pristine_deepseek_inspector_pending(
                                out_dir, pending_path, pending
                            )
                        )
                        return (
                            _commit_deepseek_transport_inspector_followup(
                                out_dir, manifest, stop_path, auth_path,
                                pending_path, pending
                            )
                        )
                    prior_authorization = (
                        _read_json(auth_path)
                        if os.path.exists(auth_path) else None
                    )
                    if (prior_authorization is not None
                            and prior_authorization.get("schema")
                            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2):
                        raise RuntimeError(
                            "cannot supersede a non-V2 recovery "
                            "authorization"
                        )
                    completed_initial = (
                        prior_authorization is not None
                        and prior_authorization.get(
                            "deepseek_transport_disconnect_"
                            "initial_transaction") is True
                        and prior_authorization.get(
                            "deepseek_transport_disconnect_"
                            "inspector_followup_recovery") is not True
                    )
                    completed_followup = (
                        prior_authorization is not None
                        and prior_authorization.get(
                            "deepseek_transport_disconnect_"
                            "inspector_followup_recovery") is True
                        and prior_authorization.get(
                            "deepseek_resume_classifier_"
                            "followup_recovery") is not True
                    )
                    if ((completed_initial or completed_followup)
                            and not os.path.exists(stop_path)):
                        verified = read_campaign_recovery_authorization(
                            out_dir)
                        return {
                            "authorization_id": verified[
                                "authorization_id"],
                            "authorization_sha256": verified[
                                "authorization_sha256"],
                            "deepseek_transport_disconnect_retry_samples": (
                                verified[
                                    "deepseek_transport_disconnect_"
                                    "retry_samples"
                                ]
                            ),
                            "incident_api_rows": len(
                                verified["incident_api_rows"]),
                            "recovered_workers": len(
                                verified["recovered_worker_launch_ids"]),
                            "pending_samples": len(
                                verified["deepseek_pending_samples"]),
                            "inspector_followup": completed_followup,
                            "already_authorized": True,
                        }
                    stop = _read_json(stop_path)
                    return _authorize_deepseek_transport_disconnect_retry(
                        out_dir, manifest, stop, stop_path, auth_path,
                        prior_authorization)
            finally:
                portalocker.unlock(lease)
    if deepseek_resume_classifier_retry:
        if (operator_pause or operator_interrupted_samples
                or provider_access_retry or deepseek_server_retry
                or deepseek_transport_disconnect_retry
                or dispatcher_process_lost):
            raise RuntimeError(
                "DeepSeek resume-classifier retry cannot be combined "
                "with other recovery modes"
            )
        lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
        with open(lease_path, "a+", encoding="utf-8") as lease:
            try:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.LockException as exc:
                raise RuntimeError(
                    "DeepSeek resume-classifier recovery requires the "
                    "dispatcher lease to be free") from exc
            try:
                with _campaign_stop_publication_lock(out_dir):
                    manifest = _read_json(manifest_path)
                    pending_path = os.path.join(
                        out_dir,
                        DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
                    )
                    if os.path.isfile(os.path.join(
                            out_dir,
                            DISPATCHER_PARENT_LOSS_PENDING_FILENAME)):
                        raise RuntimeError(
                            "dispatcher parent-loss recovery transaction "
                            "must be completed first")
                    if os.path.isfile(pending_path):
                        pending = _read_json(pending_path)
                        if pending.get("followup_kind") != "resume_classifier":
                            raise RuntimeError(
                                "a different DeepSeek recovery transaction "
                                "is pending")
                        return (
                            _commit_deepseek_transport_inspector_followup(
                                out_dir, manifest, stop_path, auth_path,
                                pending_path, pending
                            )
                        )
                    prior_authorization = (
                        _read_json(auth_path)
                        if os.path.exists(auth_path) else None
                    )
                    completed = (
                        prior_authorization is not None
                        and prior_authorization.get(
                            "deepseek_resume_classifier_"
                            "followup_recovery") is True
                    )
                    if completed and not os.path.exists(stop_path):
                        verified = read_campaign_recovery_authorization(
                            out_dir)
                        return {
                            "authorization_id": verified[
                                "authorization_id"],
                            "authorization_sha256": verified[
                                "authorization_sha256"],
                            "resume_classifier_followup": True,
                            "already_authorized": True,
                        }
                    if (prior_authorization is None
                            or prior_authorization.get("schema")
                            != CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2):
                        raise RuntimeError(
                            "DeepSeek resume-classifier recovery requires "
                            "one V2 prior authorization")
                    stop = _read_json(stop_path)
                    return _authorize_deepseek_resume_classifier_followup(
                        out_dir, manifest, stop, stop_path, auth_path,
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
        if (operator_pause or operator_interrupted_samples
                or provider_access_retry
                or deepseek_transport_disconnect_retry):
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
        if (prior_authorization is not None
                and prior_authorization.get(
                    "dispatcher_operator_pause_recovery") is True
                and not os.path.exists(stop_path)
                and not os.path.isfile(parent_loss_pending_path)):
            verified = read_campaign_recovery_authorization(out_dir)
            return {
                "authorization_id": verified["authorization_id"],
                "authorization_sha256": verified["authorization_sha256"],
                "resume_samples": verified[
                    "dispatcher_parent_loss_resume_samples"],
                "already_authorized": True,
            }
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

    metadata = read_run_metadata_snapshot(out_dir)
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
    parser.add_argument(
        "--deepseek_transport_disconnect_retry", action="store_true")
    parser.add_argument(
        "--deepseek_resume_classifier_retry", action="store_true")
    parser.add_argument("--dispatcher_process_lost", action="store_true")
    parser.add_argument("--validated_prior_authorization_sha256")
    parser.add_argument(
        "--dispatcher_parent_loss_reader_sha_fix", action="store_true")
    parser.add_argument("--validated_current_authorization_sha256")
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
        deepseek_transport_disconnect_retry=(
            args.deepseek_transport_disconnect_retry),
        deepseek_resume_classifier_retry=(
            args.deepseek_resume_classifier_retry),
        dispatcher_process_lost=args.dispatcher_process_lost,
        validated_prior_authorization_sha256=(
            args.validated_prior_authorization_sha256),
        dispatcher_parent_loss_reader_sha_fix=(
            args.dispatcher_parent_loss_reader_sha_fix),
        validated_current_authorization_sha256=(
            args.validated_current_authorization_sha256),
    ), sort_keys=True))


if __name__ == "__main__":
    main()
