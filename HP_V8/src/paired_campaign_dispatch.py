"""Integrity-fail-fast paired MiniMax campaign launcher for HP_V8.

The dispatcher never prints key values.  It pre-generates and hashes every
task plan, records a deterministic sample/key-label/method-order manifest,
launches one process per sample, isolates only fully evidenced provider/transport
budget exhaustion to that sample, and stops all workers on preservation or
shared campaign-integrity failures.
"""

import argparse
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import portalocker

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _path in (_ROOT, _HERE):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from experiment_runner import _require_formal_opencode_transport
from fr_baseline_dispatch import read_keys
from run_meta import (
    _git_identity,
    _validated_transport_ledger_state,
    append_jsonl_locked,
    code_fingerprint,
    interrupt_audited_running_invocations,
    read_campaign_stop_conditions,
    read_sample_outcomes,
    read_run_metadata_snapshot,
    record_campaign_stop_condition,
    write_json_atomic,
)
from utils_env import load_sample
from utils_relay_plan import (
    build_relay_task_plan,
    load_relay_task_plan,
    save_relay_task_plan,
)


SCHEMA = "anchorpatch.paired_campaign_manifest/1"
TRANSPORT_REVISION = "opencode_anthropic_sdk/4"
API_CALL_SCHEMA = "anchorpatch.api_call/4"
API_ATTEMPT_SCHEMA = "anchorpatch.api_attempt/4"
API_RESPONSE_JOURNAL_SCHEMA = "anchorpatch.api_response_journal/4"
TRANSPORT_RESUME_POLICY = "exact_payload_new_semantic_call/1"
SAMPLES_ROOT = os.path.join(_ROOT, "data", "samples_delegate52")
SMOKE_SAMPLES = ["treebank4", "obj3d2"]
MAIN_SAMPLES = [
    "treebank4", "obj3d2", "filesystem3", "jobboard3", "json2",
    "satellite4", "docker6", "mathlean2", "musicsheet2", "circuit2",
]
SMOKE_GRID_STEPS = 16
MAIN_GRID_STEPS = 400
SMOKE_PROJECTED_COST_LIMIT_USD = 50.0


def _is_exact_int(value):
    """Return true only for JSON integer values, never booleans."""
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_semantic_call_id(value):
    if not isinstance(value, str):
        return None
    parts = value.split("/")
    if (len(parts) != 6 or not re.fullmatch(r"rt\d+", parts[2])
            or not re.fullmatch(r"g\d{3,}", parts[5])):
        return None
    rt_index = int(parts[2][2:])
    generation_index = int(parts[5][1:])
    if (parts[2] != f"rt{rt_index:02d}"
            or parts[5] != f"g{generation_index:03d}"):
        return None
    return {
        "method": parts[0],
        "sample": parts[1],
        "rt_index": rt_index,
        "direction": parts[3],
        "call_kind": parts[4],
        "semantic_root_id": "/".join(parts[:5]),
        "semantic_call_id": value,
        "generation_index": generation_index,
    }


def _canonical_resume_authorization(authorization):
    """Return the lineage fields persisted by run_meta for one recovery."""
    if authorization is None:
        return None
    return {
        "parent_semantic_call_id": authorization[
            "parent_semantic_call_id"],
        "semantic_root_id": authorization["semantic_root_id"],
        "semantic_call_id": authorization["semantic_call_id"],
        "generation_index": authorization["generation_index"],
        "request_fingerprint": authorization["request_fingerprint"],
        "next_attempt_index": authorization["next_attempt_index"],
    }


def _validate_resume_authorization(authorization):
    """Fail closed on a dispatch authorization that is not exact lineage."""
    if not isinstance(authorization, dict):
        raise RuntimeError("transport recovery authorization is missing")
    parent = _parse_semantic_call_id(
        authorization.get("parent_semantic_call_id"))
    generation = authorization.get("generation_index")
    next_attempt = authorization.get("next_attempt_index")
    fingerprint = authorization.get("request_fingerprint")
    if (parent is None
            or authorization.get("semantic_root_id")
            != parent["semantic_root_id"]
            or not _is_exact_int(generation)
            or generation != parent["generation_index"] + 1
            or not isinstance(fingerprint, str) or not fingerprint
            or not _is_exact_int(next_attempt) or next_attempt < 1):
        raise RuntimeError("transport recovery authorization is invalid")
    expected_exact = (
        f"{parent['semantic_root_id']}/g{generation:03d}"
    )
    if authorization.get("semantic_call_id") != expected_exact:
        raise RuntimeError(
            "transport recovery authorization exact semantic ID is invalid"
        )
    return parent


def _validated_attempt_lineage(
        attempt_rows, semantic_root_id, *, allow_open_attempt=False):
    """Validate one root's exact generations and global HTTP attempt order."""
    groups = {}
    prior_generation = None
    prior_row = None
    for row in attempt_rows:
        identity = _parse_semantic_call_id(row.get("semantic_call_id"))
        if identity is None or identity["semantic_root_id"] != semantic_root_id:
            continue
        if (row.get("schema") != API_ATTEMPT_SCHEMA
                or row.get("semantic_root_id") != semantic_root_id
                or row.get("generation_index")
                != identity["generation_index"]
                or row.get("step_id")
                != "/".join(semantic_root_id.split("/")[:4])):
            raise RuntimeError("attempt ledger semantic lineage is invalid")
        generation = identity["generation_index"]
        if prior_generation is None:
            if generation != 0:
                raise RuntimeError("semantic lineage does not begin at g000")
        elif generation < prior_generation or generation > prior_generation + 1:
            raise RuntimeError("semantic lineage generation order is invalid")
        elif generation == prior_generation + 1 and (
                row.get("event") != "semantic_request"
                or (prior_row or {}).get("event") != "call_failed"):
            raise RuntimeError(
                "semantic lineage advances before its parent is terminal")
        groups.setdefault(identity["generation_index"], []).append(row)
        prior_generation = generation
        prior_row = row
    if not groups:
        raise RuntimeError("attempt ledger semantic lineage is missing")
    generations = sorted(groups)
    if generations != list(range(len(generations))):
        raise RuntimeError("semantic lineage generation sequence has a gap")

    lineage_fingerprint = None
    committed_seen = False
    all_attempt_indexes = []
    validated = {}
    previous_exact_id = None
    for generation_index in generations:
        records = groups[generation_index]
        exact_id = f"{semantic_root_id}/g{generation_index:03d}"
        if any(row.get("semantic_call_id") != exact_id for row in records):
            raise RuntimeError("semantic lineage generation identity is invalid")
        expected_parent = previous_exact_id
        if any(row.get("parent_semantic_call_id") != expected_parent
               for row in records):
            raise RuntimeError("semantic lineage parent chain is invalid")
        state = _validated_transport_ledger_state(
            records, semantic_call_id=exact_id,
            allow_open_attempt=(
                allow_open_attempt and generation_index == generations[-1]
            ),
        )
        fingerprints = state.get("request_fingerprints") or []
        if len(fingerprints) != 1:
            raise RuntimeError("semantic lineage request fingerprint is missing")
        fingerprint = fingerprints[0]
        if lineage_fingerprint is None:
            lineage_fingerprint = fingerprint
        elif fingerprint != lineage_fingerprint:
            raise RuntimeError(
                "semantic lineage request fingerprint is inconsistent"
            )
        attempt_indexes = [
            row["attempt_index"] for row in records
            if row.get("event") == "attempt_start"
        ]
        all_attempt_indexes.extend(attempt_indexes)
        committed = any(
            row.get("event") == "response_committed" for row in records
        )
        if committed_seen:
            raise RuntimeError(
                "semantic lineage continues after a committed response"
            )
        if committed:
            committed_seen = True
        elif generation_index < generations[-1]:
            terminal = state.get("terminal_failure") or {}
            if (terminal.get("status") != "provider_failure"
                    or not state.get("call_failed")
                    or state.get("last_attempt_status")
                    != "retryable_error"
                    or not state.get("retry_budget_exhausted")):
                raise RuntimeError(
                    "semantic lineage advances from a non-exhausted parent"
                )
        validated[generation_index] = {
            "semantic_call_id": exact_id,
            "parent_semantic_call_id": expected_parent,
            "state": state,
            "records": records,
            "committed": committed,
        }
        previous_exact_id = exact_id

    if (len(all_attempt_indexes) != len(set(all_attempt_indexes))
            or all_attempt_indexes != sorted(all_attempt_indexes)
            or (all_attempt_indexes
                and all_attempt_indexes
                != list(range(1, max(all_attempt_indexes) + 1)))):
        raise RuntimeError(
            "semantic lineage global attempt indexes are duplicated or gapped"
        )
    max_attempt_index = max(all_attempt_indexes, default=0)
    return {
        "semantic_root_id": semantic_root_id,
        "request_fingerprint": lineage_fingerprint,
        "generations": validated,
        "max_attempt_index": max_attempt_index,
        "next_attempt_index": max_attempt_index + 1,
    }


def method_order(index):
    """Deterministic alternating order: exact 5/5 balance for ten samples."""
    if index % 2 == 0:
        return ["hybridpatch", "fullrewrite"]
    return ["fullrewrite", "hybridpatch"]


def _validate_campaign_grid(args):
    if args.seed != 42:
        raise RuntimeError("formal paired campaigns require seed=42")
    if args.campaign_role == "smoke":
        if list(args.samples) != SMOKE_SAMPLES or args.num_round_trips != 2:
            raise RuntimeError(
                "smoke requires the fixed 16-step grid"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError("smoke cannot declare --smoke_dir")
    elif args.campaign_role == "main":
        if list(args.samples) != MAIN_SAMPLES or args.num_round_trips != 10:
            raise RuntimeError(
                "main requires the fixed 400-step grid"
            )
        if not getattr(args, "smoke_dir", None):
            raise RuntimeError("main requires completed --smoke_dir")
    else:
        raise RuntimeError(f"unsupported campaign role: {args.campaign_role}")


def _sha256(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def _read_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _read_jsonl(path):
    records = []
    if not os.path.exists(path):
        return records
    with open(path, encoding="utf-8") as handle:
        # Writers use an exclusive portalocker lock for each complete append.
        # A shared read lock prevents the monitor from mistaking a live tail
        # fragment (large raw responses included) for permanent corruption.
        portalocker.lock(handle, portalocker.LOCK_SH)
        try:
            for line_number, line in enumerate(
                    handle.read().splitlines(), 1):
                if not line.strip():
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
        finally:
            portalocker.unlock(handle)
    return records


def _latest_sample_outcomes(out_dir, expected_samples=None):
    latest = {}
    expected = set(expected_samples or [])
    for index, record in enumerate(read_sample_outcomes(out_dir), 1):
        sample = record.get("sample")
        status = record.get("status")
        if (not isinstance(sample, str) or not sample
                or status not in {"finished", "infrastructure_incomplete"}):
            raise RuntimeError(f"invalid sample outcome row {index}")
        if expected and sample not in expected:
            raise RuntimeError(
                f"sample outcome row {index} is outside campaign: {sample}")
        try:
            created_at = datetime.fromisoformat(record.get("created_at") or "")
        except ValueError as exc:
            raise RuntimeError(
                f"sample outcome row {index} has invalid timestamp") from exc
        if created_at.tzinfo is None:
            raise RuntimeError(
                f"sample outcome row {index} timestamp lacks timezone")
        prior = latest.get(sample)
        if prior and prior.get("status") == "finished":
            raise RuntimeError(
                f"sample outcome appears after finished state: {sample}")
        latest[sample] = record
    return latest


def _actual_sample_progress(out_dir, sample, methods):
    progress = {}
    for method in methods:
        result_path = os.path.join(out_dir, method, f"{sample}.jsonl")
        checkpoint_path = os.path.join(
            out_dir, method, f"{sample}.ckpt.json")
        rows = _read_jsonl(result_path)
        committed = set()
        for row in rows:
            key = (row.get("round_trip_num"),
                   row.get("round_trip_direction"))
            if (row.get("sample_id") != sample
                    or row.get("method") != method
                    or key in committed):
                raise RuntimeError(
                    f"invalid committed progress for outcome: {method}/{sample}")
            committed.add(key)
        completed = 0
        if os.path.isfile(checkpoint_path):
            checkpoint = _read_json(checkpoint_path)
            completed = checkpoint.get("completed_round_trips")
            if not _is_exact_int(completed) or completed < 0:
                raise RuntimeError(
                    f"invalid checkpoint for outcome: {method}/{sample}")
        elif rows:
            raise RuntimeError(
                f"missing checkpoint for outcome: {method}/{sample}")
        expected = {
            (rt, direction)
            for rt in range(1, completed + 1)
            for direction in ("forward", "backward")
        }
        if committed != expected:
            raise RuntimeError(
                f"partial or non-prefix progress for outcome: {method}/{sample}")
        progress[method] = {
            "completed_round_trips": completed,
            "committed_rows": len(rows),
        }
    return progress


def _verified_infrastructure_incomplete(out_dir, sample, item):
    """Return the exact failure evidence or fail closed.

    A non-zero worker exit is sample-local only when four append-only sources
    agree: sample outcome, run metadata, API call row, and attempt ledger.
    """
    if read_campaign_stop_conditions(out_dir):
        raise RuntimeError(
            "campaign-wide stop latch forbids sample-local isolation")
    latest = _latest_sample_outcomes(out_dir)
    outcome = latest.get(sample)
    if (not isinstance(outcome, dict)
            or outcome.get("status") != "infrastructure_incomplete"):
        raise RuntimeError(
            f"worker {sample} failed without infrastructure outcome")
    worker_id = item["worker_launch_id"]
    process = item.get("process")
    worker_pid = (
        process.pid if process is not None else item.get("worker_pid")
    )
    if (outcome.get("worker_launch_id") != worker_id
            or outcome.get("worker_pid") != worker_pid
            or outcome.get("methods") != item.get("methods")
            or outcome.get("classification") != "provider/API failure"):
        raise RuntimeError(
            f"worker {sample} infrastructure outcome provenance mismatch")
    if _worker_lease_is_held(out_dir, sample):
        raise RuntimeError(
            f"worker {sample} lease remains held after infrastructure exit")
    semantic_call_id = outcome.get("semantic_call_id")
    semantic_root_id = outcome.get("semantic_root_id")
    generation_index = outcome.get("generation_index")
    parent_semantic_call_id = outcome.get("parent_semantic_call_id")
    request_fingerprint = outcome.get("request_fingerprint")
    next_attempt_index = outcome.get("next_attempt_index")
    request_id = outcome.get("request_id")
    invocation_id = outcome.get("invocation_id")
    if not all(isinstance(value, str) and value for value in (
            semantic_call_id, semantic_root_id, request_fingerprint,
            request_id, invocation_id)):
        raise RuntimeError(
            f"worker {sample} infrastructure outcome identity is incomplete")
    semantic_identity = _parse_semantic_call_id(semantic_call_id)
    parent_identity = _parse_semantic_call_id(parent_semantic_call_id)
    if (semantic_identity is None
            or semantic_identity["semantic_root_id"] != semantic_root_id
            or semantic_identity["generation_index"] != generation_index
            or (generation_index == 0 and parent_semantic_call_id is not None)
            or (generation_index > 0
                and (parent_identity is None
                     or parent_identity["semantic_root_id"] != semantic_root_id
                     or parent_identity["generation_index"]
                     != generation_index - 1))
            or not _is_exact_int(next_attempt_index)
            or next_attempt_index < 1):
        raise RuntimeError(
            f"worker {sample} infrastructure outcome lineage is invalid")
    actual_progress = _actual_sample_progress(
        out_dir, sample, item.get("methods") or [])
    if outcome.get("checkpoint_progress") != actual_progress:
        raise RuntimeError(
            f"worker {sample} infrastructure checkpoint evidence drift")
    target_round_trips = item.get("target_round_trips")
    failure_method = outcome.get("method")
    failure_rt = outcome.get("rt_index")
    methods = item.get("methods") or []
    if (failure_method not in methods or not _is_exact_int(failure_rt)
            or failure_rt < 1
            or (_is_exact_int(target_round_trips)
                and failure_rt > target_round_trips)
            or semantic_identity["method"] != failure_method
            or semantic_identity["sample"] != sample
            or semantic_identity["rt_index"] != failure_rt
            or semantic_identity["direction"] != outcome.get("direction")
            or semantic_identity["call_kind"] != outcome.get("call_kind")):
        raise RuntimeError(
            f"worker {sample} infrastructure failed-step identity is invalid")
    if _is_exact_int(target_round_trips):
        failed_index = methods.index(failure_method)
        for index, method in enumerate(methods):
            committed_rt = actual_progress[method]["completed_round_trips"]
            expected_rt = (
                target_round_trips if index < failed_index
                else failure_rt - 1 if index == failed_index else 0
            )
            if committed_rt != expected_rt:
                raise RuntimeError(
                    f"worker {sample} method-order/checkpoint mismatch")

    metadata = [
        record for record in read_run_metadata_snapshot(out_dir)
        if record.get("invocation_id") == invocation_id
    ]
    if (len(metadata) != 1
            or metadata[0].get("status") != "infrastructure_incomplete"
            or metadata[0].get("worker_launch_id") != worker_id
            or metadata[0].get("worker_pid") != worker_pid
            or metadata[0].get("samples") != [sample]):
        raise RuntimeError(
            f"worker {sample} infrastructure run metadata mismatch")
    expected_metadata_resume = None
    if generation_index > 0:
        expected_metadata_resume = _canonical_resume_authorization({
            "parent_semantic_call_id": parent_semantic_call_id,
            "semantic_root_id": semantic_root_id,
            "generation_index": generation_index,
        })
    if (metadata[0].get("transport_resume_authorization")
            != expected_metadata_resume):
        raise RuntimeError(
            f"worker {sample} infrastructure resume metadata mismatch")

    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    api_matches = [
        (index, row) for index, row in enumerate(api_rows, 1)
        if row.get("request_id") == request_id
        and row.get("semantic_call_id") == semantic_call_id
    ]
    if len(api_matches) != 1:
        raise RuntimeError(
            f"worker {sample} infrastructure API evidence is not unique")
    api_index, api_row = api_matches[0]
    if (api_row.get("schema") != API_CALL_SCHEMA
            or api_row.get("transport_revision") != TRANSPORT_REVISION
            or api_row.get("transport_resume_policy")
            != TRANSPORT_RESUME_POLICY
            or api_row.get("sample") != sample
            or api_row.get("method") != outcome.get("method")
            or api_row.get("rt_index") != outcome.get("rt_index")
            or api_row.get("direction") != outcome.get("direction")
            or api_row.get("call_kind") != outcome.get("call_kind")
            or api_row.get("error_type") != outcome.get("error_type")
            or api_row.get("worker_launch_id") != worker_id
            or api_row.get("worker_pid") != worker_pid
            or api_row.get("classification") != "provider/API failure"
            or api_row.get("count_as_method_failure") is not False
            or api_row.get("provider_called") is not True
            or api_row.get("response_replayed") is not False
            or api_row.get("semantic_root_id") != semantic_root_id
            or api_row.get("generation_index") != generation_index
            or api_row.get("parent_semantic_call_id")
            != parent_semantic_call_id
            or api_row.get("request_fingerprint") != request_fingerprint
            or api_row.get("transport_recovery_index") != generation_index):
        raise RuntimeError(
            f"worker {sample} infrastructure API provenance mismatch")
    journal_digest = hashlib.sha256(
        semantic_call_id.encode("utf-8")).hexdigest()[:24]
    if os.path.exists(os.path.join(
            out_dir, "api_journal", f"{journal_digest}.response.json")):
        raise RuntimeError(
            f"worker {sample} failed semantic call has a response journal")
    response_slots = api_row.get("response_slots_used")
    transient_failures = api_row.get("transient_failure_count")
    http_attempts_used = api_row.get("http_attempts_used")
    if (outcome.get("response_slots_used") != response_slots
            or outcome.get("transient_failure_count") != transient_failures
            or outcome.get("http_attempts_used")
            != http_attempts_used
            or outcome.get("transport_recovery_index") != generation_index
            or not _is_exact_int(http_attempts_used)
            or http_attempts_used < 1
            or http_attempts_used + 1 != next_attempt_index):
        raise RuntimeError(
            f"worker {sample} infrastructure outcome budget mismatch")
    exhausted = (
        response_slots == api_row.get("max_response_slots") == 2
        or transient_failures == api_row.get("max_transient_failures") == 3
    )
    if not exhausted:
        raise RuntimeError(
            f"worker {sample} exited before a transport budget was exhausted")

    ledger = _read_jsonl(os.path.join(
        out_dir, "api_attempt_ledger.jsonl"))
    lineage = _validated_attempt_lineage(ledger, semantic_root_id)
    if (lineage["request_fingerprint"] != request_fingerprint
            or lineage["next_attempt_index"] != next_attempt_index
            or generation_index != max(lineage["generations"])
            or generation_index not in lineage["generations"]
            or lineage["generations"][generation_index][
                "semantic_call_id"] != semantic_call_id
            or lineage["generations"][generation_index][
                "parent_semantic_call_id"] != parent_semantic_call_id):
        raise RuntimeError(
            f"worker {sample} infrastructure attempt lineage mismatch")
    failed_state = lineage["generations"][generation_index]["state"]
    if (not failed_state.get("call_failed")
            or failed_state.get("last_attempt_status")
            != "retryable_error"
            or not failed_state.get("retry_budget_exhausted")
            or failed_state.get("open_attempt_index") is not None):
        raise RuntimeError(
            f"worker {sample} did not end in exact R2/I3 retry exhaustion")
    ledger_matches = [
        (index, row) for index, row in enumerate(ledger, 1)
        if row.get("schema") == API_ATTEMPT_SCHEMA
        and row.get("event") == "call_failed"
        and row.get("semantic_call_id") == semantic_call_id
        and row.get("call_id") == request_id
    ]
    if len(ledger_matches) != 1:
        raise RuntimeError(
            f"worker {sample} infrastructure attempt evidence is not unique")
    ledger_index, ledger_row = ledger_matches[0]
    if (ledger_row.get("status") != "provider_failure"
            or ledger_row.get("worker_launch_id") != worker_id
            or ledger_row.get("error_type") != api_row.get("error_type")
            or ledger_row.get("request_fingerprint")
            != api_row.get("request_fingerprint")
            or ledger_row.get("semantic_root_id") != semantic_root_id
            or ledger_row.get("generation_index") != generation_index
            or ledger_row.get("parent_semantic_call_id")
            != parent_semantic_call_id
            or ledger_row.get("response_slots_used") != response_slots
            or ledger_row.get("transient_failure_count")
            != transient_failures
            or ledger_row.get("http_attempts_used")
            != api_row.get("http_attempts_used")):
        raise RuntimeError(
            f"worker {sample} infrastructure budget evidence mismatch")
    return {
        "sample_outcome_created_at": outcome.get("created_at"),
        "invocation_id": invocation_id,
        "semantic_root_id": semantic_root_id,
        "semantic_call_id": semantic_call_id,
        "generation_index": generation_index,
        "parent_semantic_call_id": parent_semantic_call_id,
        "request_id": request_id,
        "request_fingerprint": request_fingerprint,
        "next_attempt_index": next_attempt_index,
        "api_row": api_index,
        "attempt_ledger_row": ledger_index,
        "checkpoint_progress": actual_progress,
    }


def _select_invocation_assignments(out_dir, assignments, *, resume,
                                   target_round_trips):
    """Select all samples for a new campaign, only incomplete ones on resume."""
    if not resume:
        if read_sample_outcomes(out_dir):
            raise RuntimeError(
                "new campaign directory already contains sample outcomes")
        return list(assignments), {}
    latest = _latest_sample_outcomes(
        out_dir, [item["sample"] for item in assignments])
    missing = [
        item["sample"] for item in assignments
        if item["sample"] not in latest
    ]
    if missing:
        raise RuntimeError(
            f"resume is limited to explicitly incomplete samples; "
            f"missing outcomes: {missing}")
    selected = []
    authorizations = {}
    for item in assignments:
        sample = item["sample"]
        outcome = latest[sample]
        if outcome["status"] == "finished":
            progress = _actual_sample_progress(
                out_dir, sample, item["methods"])
            expected = {
                method: {
                    "completed_round_trips": target_round_trips,
                    "committed_rows": 2 * target_round_trips,
                }
                for method in item["methods"]
            }
            if (progress != expected
                    or outcome.get("checkpoint_progress") != expected):
                raise RuntimeError(
                    f"finished sample evidence is incomplete: {sample}")
            continue
        evidence = _verified_infrastructure_incomplete(
            out_dir, sample, {
                "worker_launch_id": outcome.get("worker_launch_id"),
                "worker_pid": outcome.get("worker_pid"),
                "methods": item["methods"],
                "target_round_trips": target_round_trips,
            })
        prior_generation = evidence.get("generation_index")
        if not _is_exact_int(prior_generation) or prior_generation < 0:
            raise RuntimeError(
                f"invalid semantic generation for incomplete sample: {sample}")
        next_generation = prior_generation + 1
        selected.append(item)
        authorizations[sample] = {
            "parent_semantic_call_id": evidence["semantic_call_id"],
            "semantic_root_id": evidence["semantic_root_id"],
            "semantic_call_id": (
                f"{evidence['semantic_root_id']}/g{next_generation:03d}"
            ),
            "generation_index": next_generation,
            "request_fingerprint": evidence["request_fingerprint"],
            "next_attempt_index": evidence["next_attempt_index"],
            "prior_worker_launch_id": outcome.get("worker_launch_id"),
            "prior_invocation_id": evidence["invocation_id"],
        }
    return selected, authorizations


def _worker_lease_path(out_dir, sample):
    safe_sample = "".join(
        char if char.isalnum() or char in "._-" else "_"
        for char in sample
    )
    return os.path.join(out_dir, "worker_leases", f"{safe_sample}.lock")


def _worker_barrier_paths(out_dir, worker_launch_id):
    safe_id = "".join(
        char if char.isalnum() or char in "._-" else "_"
        for char in worker_launch_id
    )
    root = os.path.join(out_dir, "worker_barriers")
    return (
        os.path.join(root, f"{safe_id}.ready.json"),
        os.path.join(root, f"{safe_id}.start.json"),
    )


def _active_worker_set_path(out_dir):
    return os.path.join(out_dir, "active_worker_set.json")


def _write_active_worker_set(out_dir, manifest, workers):
    record = {
        "schema": "anchorpatch.active_worker_set/1",
        "run_git_commit": manifest["run_git_commit"],
        "workers": {
            item["worker_launch_id"]: {"sample": item["sample"]}
            for item in workers
        },
    }
    write_json_atomic(_active_worker_set_path(out_dir), record)
    return record


def _worker_lease_is_held(out_dir, sample):
    path = _worker_lease_path(out_dir, sample)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handle = open(path, "a+", encoding="utf-8")
    try:
        try:
            portalocker.lock(
                handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException:
            return True
        portalocker.unlock(handle)
        return False
    finally:
        handle.close()


def _assert_worker_leases_free(out_dir, samples):
    """Refuse launch/resume while an orphan runner still owns a sample."""
    held = []
    for sample in samples:
        path = _worker_lease_path(out_dir, sample)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        handle = open(path, "a+", encoding="utf-8")
        try:
            portalocker.lock(
                handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException:
            held.append(sample)
        else:
            portalocker.unlock(handle)
        finally:
            handle.close()
    if held:
        raise RuntimeError(
            f"worker process lease is still held for samples: {held}"
        )


def _authorize_workers(out_dir, running, task_plans, dispatch_log,
                       timeout_seconds):
    """Release workers only after lease/PID/metadata/plan identity closes."""
    deadline = time.monotonic() + timeout_seconds
    ready_by_sample = {}
    while len(ready_by_sample) != len(running):
        stop_records = read_campaign_stop_conditions(out_dir)
        if stop_records:
            raise RuntimeError(
                "campaign stop latch set before worker authorization: "
                f"{stop_records[0].get('condition')}"
            )
        for sample, item in running.items():
            if sample in ready_by_sample:
                continue
            returncode = item["process"].poll()
            if returncode is not None:
                raise RuntimeError(
                    f"worker {sample} exited before authorization with {returncode}"
                )
            ready_path = item["ready_path"]
            if not os.path.isfile(ready_path):
                continue
            ready = _read_json(ready_path)
            expected_plan = task_plans[sample]["sha256"]
            expected = {
                "schema": "anchorpatch.worker_ready/1",
                "worker_launch_id": item["worker_launch_id"],
                "worker_pid": item["process"].pid,
                "sample": sample,
                "task_plan_path": os.path.abspath(os.path.join(
                    out_dir, task_plans[sample]["path"])),
                "task_plan_sha256": expected_plan,
            }
            if any(ready.get(key) != value for key, value in expected.items()):
                raise RuntimeError(f"worker ready identity mismatch: {sample}")
            invocation_id = ready.get("invocation_id")
            if not isinstance(invocation_id, str) or not invocation_id:
                raise RuntimeError(f"worker ready invocation missing: {sample}")
            if not _worker_lease_is_held(out_dir, sample):
                raise RuntimeError(f"worker lease not held at authorization: {sample}")
            metadata = read_run_metadata_snapshot(out_dir)
            matches = [
                record for record in metadata
                if record.get("invocation_id") == invocation_id
                and record.get("status") == "running"
                and record.get("worker_launch_id") == item["worker_launch_id"]
                and record.get("worker_pid") == item["process"].pid
                and record.get("samples") == [sample]
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    f"worker metadata handshake mismatch: {sample}"
                )
            if (matches[0].get("task_plans") or {}).get(sample) != {
                    "sha256": expected_plan,
                    "round_trips": len(task_plans[sample][
                        "forward_state_sequence"]),
            }:
                raise RuntimeError(
                    f"worker task-plan handshake mismatch: {sample}"
                )
            expected_resume = item.get("resume_authorization")
            metadata_resume = matches[0].get(
                "transport_resume_authorization")
            if expected_resume is not None:
                _validate_resume_authorization(expected_resume)
            expected_metadata_resume = _canonical_resume_authorization(
                expected_resume
            )
            if metadata_resume != expected_metadata_resume:
                raise RuntimeError(
                    f"worker transport-resume handshake mismatch: {sample}")
            ready_by_sample[sample] = ready
        if len(ready_by_sample) == len(running):
            break
        if time.monotonic() >= deadline:
            missing = sorted(set(running) - set(ready_by_sample))
            raise RuntimeError(
                f"timed out waiting for worker start barrier: {missing}"
            )
        time.sleep(0.1)

    acknowledgements = {}
    for sample, item in running.items():
        ready = ready_by_sample[sample]
        ack = {
            "schema": "anchorpatch.worker_start/1",
            "worker_launch_id": item["worker_launch_id"],
            "worker_pid": item["process"].pid,
            "invocation_id": ready["invocation_id"],
            "sample": sample,
            "task_plan_sha256": task_plans[sample]["sha256"],
        }
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "worker_authorized",
                "created_at": datetime.now().astimezone().isoformat(
                    timespec="seconds"),
                "transport_resume_authorization": item.get(
                    "resume_authorization"),
                **ack,
            },
        )
        acknowledgements[sample] = ack

    # Authorization is a cohort barrier: no worker may observe an ACK until
    # every worker's authorization event has been durably appended.  This
    # prevents an early worker from calling the provider if a later fsync
    # fails.
    stop_records = read_campaign_stop_conditions(out_dir)
    if stop_records:
        raise RuntimeError(
            "campaign stop latch set before cohort ACK publication: "
            f"{stop_records[0].get('condition')}"
        )
    for sample, item in running.items():
        returncode = item["process"].poll()
        if returncode is not None:
            raise RuntimeError(
                f"worker {sample} exited before cohort ACK with {returncode}"
            )
        if not _worker_lease_is_held(out_dir, sample):
            raise RuntimeError(
                f"worker lease not held before cohort ACK: {sample}"
            )
    for sample, item in running.items():
        ack = acknowledgements[sample]
        write_json_atomic(item["ack_path"], ack)


def _audit_running_invocation_provenance(out_dir):
    """Bind each stale metadata record to its dispatch intent and PID."""
    dispatch_rows = _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
    intents = {
        row.get("worker_launch_id"): row
        for row in dispatch_rows if row.get("event") == "launch_intent"
    }
    launches = {
        row.get("worker_launch_id"): row
        for row in dispatch_rows if row.get("event") == "launch"
    }
    audited = []
    seen_invocations = set()
    for record in read_run_metadata_snapshot(out_dir):
        if record.get("status") != "running":
            continue
        invocation_id = record.get("invocation_id")
        worker_launch_id = record.get("worker_launch_id")
        worker_pid = record.get("worker_pid")
        samples = list(record.get("samples") or [])
        intent = intents.get(worker_launch_id)
        launch = launches.get(worker_launch_id)
        if (not isinstance(invocation_id, str) or not invocation_id
                or invocation_id in seen_invocations
                or not isinstance(worker_launch_id, str) or not worker_launch_id
                or not _is_exact_int(worker_pid) or worker_pid <= 0
                or len(samples) != 1
                or not isinstance(intent, dict)
                or intent.get("sample") != samples[0]):
            raise RuntimeError(
                "cannot audit stale invocation provenance: "
                f"{record.get('invocation_id')}"
            )
        seen_invocations.add(invocation_id)
        if launch is not None and (
                launch.get("sample") != samples[0]
                or launch.get("pid") != worker_pid):
            raise RuntimeError(
                "stale invocation launch/PID mismatch: "
                f"{record.get('invocation_id')}"
            )
        audited.append({
            "invocation_id": invocation_id,
            "worker_launch_id": worker_launch_id,
            "worker_pid": worker_pid,
            "sample": samples[0],
            "launch_recorded": launch is not None,
        })
    return audited


def prepare_task_plans(out_dir, samples, num_round_trips, seed):
    manifest = {}
    for sample_id in samples:
        sample, _folder, states = load_sample(
            sample_id, samples_folder=SAMPLES_ROOT + os.sep)
        initial = states[sample["start_state"]]
        possible = [item["target_state"] for item in initial["prompts"]]
        expected = build_relay_task_plan(
            possible, num_round_trips, seed=seed)
        plan_path = os.path.join(out_dir, f"{sample_id}.task_plan.json")
        if os.path.exists(plan_path):
            actual = load_relay_task_plan(plan_path)
            if actual != expected:
                raise RuntimeError(
                    f"existing task plan differs from seeded plan: {sample_id}"
                )
        else:
            save_relay_task_plan(plan_path, expected)
        if len(expected) != num_round_trips or not all(
                state_id in states for state_id in expected):
            raise RuntimeError(f"invalid task plan targets for {sample_id}")
        manifest[sample_id] = {
            "path": os.path.basename(plan_path),
            "sha256": _sha256(plan_path),
            "forward_state_sequence": expected,
        }
    return manifest


def build_manifest(out_dir, samples, assignments, task_plans, args,
                   upstream_smoke_gate=None):
    commit, tree_state = _git_identity()
    if tree_state != "clean":
        raise RuntimeError("formal campaign requires a clean Git worktree")
    return {
        "schema": SCHEMA,
        "experiment_id": os.path.basename(os.path.abspath(out_dir)),
        "run_git_commit": commit,
        "git_tree_state": tree_state,
        "code_fingerprint": code_fingerprint(),
        "config": {
            "campaign_role": args.campaign_role,
            "samples": list(samples),
            "method_set": ["fullrewrite", "hybridpatch"],
            "num_round_trips": args.num_round_trips,
            "seed": args.seed,
            "model": "minimax-m3",
            "max_tokens": 131072,
            "distractor": True,
            "opencode_transport": "anthropic_sdk_v2",
            "minimax_transport": "opencode",
            "transport_revision": TRANSPORT_REVISION,
            "transport_resume_policy": TRANSPORT_RESUME_POLICY,
            "stop_on_preservation_violation": True,
        },
        "assignments": assignments,
        "task_plans": task_plans,
        "upstream_smoke_gate": upstream_smoke_gate,
    }


def _manifest_identity(manifest):
    value = dict(manifest)
    value["assignments"] = [
        {"sample": item["sample"], "methods": item["methods"]}
        for item in manifest.get("assignments") or []
    ]
    return value


def write_or_verify_manifest(out_dir, manifest, *, resume=False):
    path = os.path.join(out_dir, "dispatch_manifest.json")
    if os.path.exists(path):
        prior = _read_json(path)
        same = (
            _manifest_identity(prior) == _manifest_identity(manifest)
            if resume else prior == manifest
        )
        if not same:
            raise RuntimeError(
                "dispatch manifest differs from the existing campaign; "
                "use a new --out_dir"
            )
        return path, prior
    else:
        if resume:
            raise RuntimeError("--resume requires an existing dispatch manifest")
        write_json_atomic(path, manifest)
    return path, manifest


def inspect_campaign(out_dir, manifest, *, require_complete=False,
                     active_samples=None, required_complete_samples=None):
    config = manifest["config"]
    expected_samples = set(config["samples"])
    expected_methods = set(config["method_set"])
    target_rt = config["num_round_trips"]
    errors = []
    preservation = 0
    latched_preservation = 0
    preservation_not_applicable = 0
    formal_manifest = manifest.get("schema") == SCHEMA
    completion_samples = (
        expected_samples if require_complete
        else set(required_complete_samples or [])
    )
    active_samples = set(active_samples or [])
    active_worker_rows = {}
    try:
        active_payload = _read_json(_active_worker_set_path(out_dir))
        if active_payload.get("schema") != "anchorpatch.active_worker_set/1":
            raise RuntimeError("active worker set schema is invalid")
        active_worker_rows = active_payload.get("workers") or {}
        if not isinstance(active_worker_rows, dict):
            raise RuntimeError("active worker set workers are invalid")
    except (OSError, ValueError, RuntimeError):
        if active_samples:
            errors.append("active worker set is missing or invalid")
        active_worker_rows = {}

    try:
        stop_records = read_campaign_stop_conditions(out_dir)
    except RuntimeError as exc:
        stop_records = []
        errors.append(str(exc))
    for record in stop_records:
        if (record.get("schema") != "anchorpatch.campaign_stop_condition/1"
                or not isinstance(record.get("condition"), str)
                or not record.get("condition")):
            errors.append("invalid campaign stop latch")
        else:
            errors.append(
                f"campaign stop latch={record.get('condition')}"
            )
            if record.get("condition") == "preservation_violation":
                count = record.get("preservation_violations")
                if (not isinstance(count, int) or isinstance(count, bool)
                        or count <= 0):
                    errors.append("invalid preservation stop count")
                else:
                    latched_preservation += count

    metadata = read_run_metadata_snapshot(out_dir)
    metadata_by_worker = {}
    for record in metadata:
        worker_id = record.get("worker_launch_id")
        if isinstance(worker_id, str) and worker_id:
            metadata_by_worker.setdefault(worker_id, []).append(record)
    dispatch_rows = _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
    launches_by_worker = {}
    authorizations_by_worker = {}
    exits_by_worker = {}
    reconciliations_by_worker = {}
    for row in dispatch_rows:
        worker_id = row.get("worker_launch_id")
        event = row.get("event")
        if event not in {
                "launch", "worker_authorized", "worker_exit",
                "stale_worker_reconciled"}:
            continue
        target = {
            "launch": launches_by_worker,
            "worker_authorized": authorizations_by_worker,
            "worker_exit": exits_by_worker,
            "stale_worker_reconciled": reconciliations_by_worker,
        }[event]
        if not isinstance(worker_id, str) or not worker_id:
            errors.append(f"{event} missing worker_launch_id")
        elif worker_id in target:
            errors.append(f"duplicate {event} record: {worker_id}")
        else:
            target[worker_id] = row

    commit, tree_state = _git_identity()
    if commit != manifest["run_git_commit"] or tree_state != "clean":
        errors.append("Git commit/tree state changed during campaign")
    for sample, plan in (manifest.get("task_plans") or {}).items():
        plan_path = os.path.join(out_dir, plan.get("path") or "")
        if not os.path.isfile(plan_path):
            errors.append(f"task plan missing: {sample}")
        elif _sha256(plan_path) != plan.get("sha256"):
            errors.append(f"task-plan hash drift: {sample}")

    # Relay publication is causally ordered as attempt ledger/journal, then the
    # terminal API row, then the committed result/checkpoint.  Read that chain
    # in reverse publication order so a live inspection can observe either the
    # old prefix or the new prefix, never an old API snapshot paired with a new
    # result row.  Per-file locks alone cannot provide a cross-file snapshot.
    committed_rows = {}
    for sample in config["samples"]:
        for method in config["method_set"]:
            result_path = os.path.join(
                out_dir, method, f"{sample}.jsonl")
            committed_rows[(sample, method)] = _read_jsonl(result_path)

    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    api_keys = set()
    semantic_groups = {}
    semantic_root_groups = {}
    api_rows_by_worker = {}
    for index, row in enumerate(api_rows, 1):
        sample = row.get("sample")
        method = row.get("method")
        rt = row.get("rt_index")
        direction = row.get("direction")
        if (sample not in expected_samples or method not in expected_methods
                or not _is_exact_int(rt) or not 1 <= rt <= target_rt
                or direction not in {"forward", "backward"}):
            errors.append(f"unmappable API ledger row {index}")
        else:
            api_keys.add((sample, method, rt, direction))
            call_kind = row.get("call_kind")
            allowed_kinds = (
                {"hybridpatch_primary", "hybridpatch_repair"}
                if method == "hybridpatch" else {"fullrewrite_primary"}
            )
            if call_kind not in allowed_kinds:
                errors.append(
                    f"unexpected call_kind at API row {index}: {call_kind!r}"
                )
            if row.get("schema") != API_CALL_SCHEMA:
                errors.append(f"API call schema mismatch at row {index}")
            if row.get("transport_revision") != TRANSPORT_REVISION:
                errors.append(f"transport revision mismatch at API row {index}")
            if row.get("transport_resume_policy") != TRANSPORT_RESUME_POLICY:
                errors.append(
                    f"transport resume policy mismatch at API row {index}")
            if row.get("max_response_slots") != 2:
                errors.append(f"response-slot policy mismatch at API row {index}")
            slots_used = row.get("response_slots_used")
            if not _is_exact_int(slots_used) or not 0 <= slots_used <= 2:
                errors.append(f"response-slot overrun at API row {index}")
            if row.get("max_transient_failures") != 3:
                errors.append(f"transient policy mismatch at API row {index}")
            transient_used = row.get("transient_failure_count")
            if (not _is_exact_int(transient_used)
                    or not 0 <= transient_used <= 3):
                errors.append(f"transient budget overrun at API row {index}")
            semantic_call_id = row.get("semantic_call_id")
            identity = _parse_semantic_call_id(semantic_call_id)
            if (identity is None
                    or identity["sample"] != sample
                    or identity["method"] != method
                    or identity["rt_index"] != rt
                    or identity["direction"] != direction
                    or identity["call_kind"] != call_kind
                    or row.get("semantic_root_id")
                    != identity["semantic_root_id"]
                    or row.get("generation_index")
                    != identity["generation_index"]
                    or row.get("transport_recovery_index")
                    != identity["generation_index"]
                    or (identity["generation_index"] == 0
                        and row.get("parent_semantic_call_id") is not None)
                    or (identity["generation_index"] > 0
                        and _parse_semantic_call_id(
                            row.get("parent_semantic_call_id")) is None)):
                errors.append(
                    f"invalid exact semantic lineage at API row {index}")
                identity = None
            elif (not isinstance(row.get("request_fingerprint"), str)
                  or not row.get("request_fingerprint")):
                errors.append(
                    f"missing request fingerprint at API row {index}")
            if not isinstance(row.get("provider_called"), bool):
                errors.append(f"invalid provider_called at API row {index}")
            elif identity is not None:
                semantic_groups.setdefault(semantic_call_id, []).append(
                    (index, row)
                )
                semantic_root_groups.setdefault(
                    identity["semantic_root_id"], []
                ).append((index, row))

            if formal_manifest:
                worker_id = row.get("worker_launch_id")
                worker_pid = row.get("worker_pid")
                if isinstance(worker_id, str) and worker_id:
                    api_rows_by_worker.setdefault(worker_id, []).append(
                        (index, row))
                launch = launches_by_worker.get(worker_id)
                authorization = authorizations_by_worker.get(worker_id)
                worker_metadata = metadata_by_worker.get(worker_id) or []
                if (not isinstance(worker_id, str) or not worker_id
                        or not _is_exact_int(worker_pid) or worker_pid <= 0
                        or not isinstance(launch, dict)
                        or launch.get("sample") != sample
                        or launch.get("pid") != worker_pid
                        or not isinstance(authorization, dict)
                        or authorization.get("sample") != sample
                        or authorization.get("worker_pid") != worker_pid
                        or len(worker_metadata) != 1
                        or authorization.get("invocation_id")
                        != worker_metadata[0].get("invocation_id")
                        or authorization.get("task_plan_sha256")
                        != (manifest.get("task_plans") or {}).get(
                            sample, {}).get("sha256")
                        or worker_metadata[0].get("worker_pid") != worker_pid
                        or worker_metadata[0].get("samples") != [sample]):
                    errors.append(
                        f"API worker provenance mismatch at row {index}"
                    )
                if identity is not None:
                    dispatch_resume = authorization.get(
                        "transport_resume_authorization"
                    ) if isinstance(authorization, dict) else None
                    metadata_resume = (
                        worker_metadata[0].get(
                            "transport_resume_authorization")
                        if len(worker_metadata) == 1 else None
                    )
                    resume_valid = True
                    if dispatch_resume is None:
                        if metadata_resume is not None:
                            errors.append(
                                f"recovery metadata without dispatch authorization "
                                f"at API row {index}"
                            )
                            resume_valid = False
                    else:
                        try:
                            _validate_resume_authorization(dispatch_resume)
                        except RuntimeError:
                            errors.append(
                                f"invalid recovery authorization at API row {index}"
                            )
                            resume_valid = False
                        else:
                            if metadata_resume != _canonical_resume_authorization(
                                    dispatch_resume):
                                errors.append(
                                    f"recovery metadata mismatch at API row {index}"
                                )
                                resume_valid = False
                    if identity["generation_index"] > 0:
                        if (not resume_valid or not isinstance(
                                dispatch_resume, dict)
                                or dispatch_resume.get("semantic_call_id")
                                    != semantic_call_id
                                or dispatch_resume.get(
                                    "parent_semantic_call_id")
                                    != row.get("parent_semantic_call_id")
                                    or dispatch_resume.get("semantic_root_id")
                                    != identity["semantic_root_id"]
                                or dispatch_resume.get("generation_index")
                                    != identity["generation_index"]
                                or dispatch_resume.get("request_fingerprint")
                                    != row.get("request_fingerprint")):
                                errors.append(
                                    f"API recovery authorization mismatch at row {index}"
                                )

    if formal_manifest:
        # A recovery worker can legitimately replay earlier g000 journals
        # before it reaches the authorized failed call (for example, replaying
        # an uncommitted forward before recovering backward), and it continues
        # with ordinary g000 calls after recovery.  The authorization therefore
        # applies to exactly one row, not to every row produced by the worker.
        # Before that exact row appears, only zero-POST journal replay is legal.
        for worker_id, authorization in authorizations_by_worker.items():
            resume = authorization.get("transport_resume_authorization")
            if resume is None:
                continue
            try:
                parent = _validate_resume_authorization(resume)
            except RuntimeError:
                errors.append(
                    f"invalid worker recovery authorization: {worker_id}")
                continue
            worker_rows = api_rows_by_worker.get(worker_id) or []
            exact_uses = [
                (index, row) for index, row in worker_rows
                if row.get("semantic_call_id") == resume["semantic_call_id"]
            ]
            if len(exact_uses) > 1:
                errors.append(
                    f"recovery authorization used more than once: {worker_id}"
                )
            exact_index = exact_uses[0][0] if exact_uses else None
            for index, row in worker_rows:
                if exact_index is not None and index >= exact_index:
                    break
                if (row.get("provider_called") is not False
                        or row.get("response_replayed") is not True
                        or row.get("semantic_root_id")
                        == resume["semantic_root_id"]):
                    errors.append(
                        "provider call preceded recovery authorization use at "
                        f"API row {index}"
                    )
            worker_active = (
                active_worker_rows.get(worker_id)
                == {"sample": parent["sample"]}
                and worker_id not in exits_by_worker
            )
            if not worker_active and len(exact_uses) != 1:
                errors.append(
                    f"recovery authorization was not consumed: {worker_id}"
                )

    attempt_rows = _read_jsonl(os.path.join(
        out_dir, "api_attempt_ledger.jsonl"))
    attempt_groups = {}
    attempt_root_ids = set()
    allowed_attempt_events = {
        "semantic_request", "attempt_start", "generation_progress",
        "attempt_end", "attempt_budget", "response_committed",
        "call_failed",
    }
    for index, row in enumerate(attempt_rows, 1):
        semantic_call_id = row.get("semantic_call_id")
        identity = _parse_semantic_call_id(semantic_call_id)
        event = row.get("event")
        worker_id = row.get("worker_launch_id")
        if row.get("schema") != API_ATTEMPT_SCHEMA:
            errors.append(f"attempt schema mismatch at ledger row {index}")
        if (identity is None
                or identity["sample"] not in expected_samples
                or identity["method"] not in expected_methods
                or not 1 <= identity["rt_index"] <= target_rt
                or identity["direction"] not in {"forward", "backward"}):
            errors.append(f"unmappable attempt ledger row {index}")
            continue
        if (row.get("semantic_root_id") != identity["semantic_root_id"]
                or row.get("generation_index")
                != identity["generation_index"]
                or (identity["generation_index"] == 0
                    and row.get("parent_semantic_call_id") is not None)
                or (identity["generation_index"] > 0
                    and _parse_semantic_call_id(
                        row.get("parent_semantic_call_id")) is None)):
            errors.append(f"attempt lineage mismatch at ledger row {index}")
        allowed_kinds = (
            {"hybridpatch_primary", "hybridpatch_repair"}
            if identity["method"] == "hybridpatch"
            else {"fullrewrite_primary"}
        )
        if identity["call_kind"] not in allowed_kinds:
            errors.append(f"invalid call kind at attempt ledger row {index}")
        if event not in allowed_attempt_events:
            errors.append(f"unknown attempt event at ledger row {index}")
        launch = launches_by_worker.get(worker_id)
        if (not isinstance(worker_id, str) or not worker_id
                or not isinstance(launch, dict)
                or launch.get("sample") != identity["sample"]):
            errors.append(f"attempt worker provenance mismatch at row {index}")
        attempt_index = row.get("attempt_index")
        if event in {
                "attempt_start", "generation_progress", "attempt_end",
                "attempt_budget"} and (
                not _is_exact_int(attempt_index) or attempt_index < 1):
            errors.append(f"invalid attempt index at ledger row {index}")
        if event == "attempt_start" and (
                row.get("attempt_kind") not in {
                    (
                        "transport_initial"
                        if identity["generation_index"] == 0
                        else "transport_recovery_initial"
                    ),
                    (
                        "transport_retry"
                        if identity["generation_index"] == 0
                        else "transport_recovery_retry"
                    ),
                }
                or row.get("call_kind") != identity["call_kind"]):
            errors.append(f"attempt kind/call kind mismatch at row {index}")
        attempt_groups.setdefault(semantic_call_id, []).append((index, row))
        attempt_root_ids.add(identity["semantic_root_id"])

    for semantic_call_id, group in attempt_groups.items():
        identity = _parse_semantic_call_id(semantic_call_id) or {}
        if (semantic_call_id not in semantic_groups
                and identity.get("sample") not in active_samples):
            errors.append(
                f"attempt ledger has no mapped API call: {semantic_call_id}")

    validated_attempt_lineages = {}
    for semantic_root_id in sorted(attempt_root_ids):
        try:
            root_rows = [
                row for row in attempt_rows
                if row.get("semantic_root_id") == semantic_root_id
            ]
            root_identity = _parse_semantic_call_id(
                (root_rows[-1] if root_rows else {}).get("semantic_call_id")
            ) or {}
            tail_worker = (
                root_rows[-1].get("worker_launch_id") if root_rows else None
            )
            allow_open_attempt = bool(
                root_identity.get("sample") in active_samples
                and active_worker_rows.get(tail_worker)
                == {"sample": root_identity.get("sample")}
                and tail_worker not in exits_by_worker
            )
            validated_attempt_lineages[semantic_root_id] = (
                _validated_attempt_lineage(
                    attempt_rows, semantic_root_id,
                    allow_open_attempt=allow_open_attempt,
                )
            )
        except RuntimeError as exc:
            errors.append(
                f"invalid attempt lineage {semantic_root_id}: {exc}"
            )

    calls_by_step = {}
    provider_call_rows = 0
    for semantic_root_id, root_group in semantic_root_groups.items():
        signatures = {
            (
                row.get("sample"), row.get("method"), row.get("rt_index"),
                row.get("direction"), row.get("call_kind"),
            )
            for _index, row in root_group
        }
        if len(signatures) != 1:
            errors.append(
                f"semantic-root identity drift: {semantic_root_id}"
            )
            continue
        sample, method, rt, direction, call_kind = next(iter(signatures))
        step = (sample, method, rt, direction)
        calls_by_step.setdefault(step, []).append(
            (call_kind, semantic_root_id)
        )
        lineage = validated_attempt_lineages.get(semantic_root_id)
        if lineage is None:
            errors.append(
                f"semantic root has no valid attempt lineage: {semantic_root_id}"
            )
            continue
        root_fingerprints = {
            row.get("request_fingerprint") for _index, row in root_group
        }
        if root_fingerprints != {lineage["request_fingerprint"]}:
            errors.append(
                f"provider request fingerprint drift: {semantic_root_id}"
            )

        root_exact_ids = sorted({
            row.get("semantic_call_id") for _index, row in root_group
        })
        for semantic_call_id in root_exact_ids:
            group = semantic_groups.get(semantic_call_id) or []
            identity = _parse_semantic_call_id(semantic_call_id) or {}
            generation_index = identity.get("generation_index")
            generation = (lineage.get("generations") or {}).get(
                generation_index
            )
            if generation is None:
                errors.append(
                    f"API generation has no attempt lineage: {semantic_call_id}"
                )
                continue
            provider_rows = [
                (index, row) for index, row in group
                if row.get("provider_called") is True
            ]
            replay_rows = [
                (index, row) for index, row in group
                if row.get("provider_called") is False
                and row.get("response_replayed") is True
            ]
            non_provider_failures = [
                (index, row) for index, row in group
                if row.get("provider_called") is False
                and row.get("response_replayed") is not True
            ]
            provider_call_rows += len(provider_rows)
            if len(provider_rows) > 1:
                errors.append(
                    f"duplicate provider semantic generation: {semantic_call_id}"
                )
            if (generation_index < max(lineage["generations"])
                    and (len(provider_rows) != 1
                         or provider_rows[0][1].get("classification")
                         != "provider/API failure"
                         or provider_rows[0][1].get(
                             "count_as_method_failure") is not False)):
                errors.append(
                    "non-terminal recovery generation is not "
                    f"infrastructure-only: {semantic_call_id}"
                )
            if non_provider_failures:
                errors.append(
                    f"non-provider semantic failure row: {semantic_call_id}"
                )
            for index, row in provider_rows:
                if row.get("response_replayed") or row.get(
                        "replayed_from_call_id"):
                    errors.append(
                        f"provider row has replay markers at API row {index}"
                    )
                event_name = (
                    "call_failed"
                    if row.get("classification") == "provider/API failure"
                    else "response_committed"
                )
                matching_events = [
                    attempt for _attempt_index, attempt
                    in attempt_groups.get(semantic_call_id, [])
                    if attempt.get("event") == event_name
                    and attempt.get("call_id") == row.get("request_id")
                    and attempt.get("request_fingerprint")
                    == row.get("request_fingerprint")
                    and attempt.get("generation_index") == generation_index
                    and attempt.get("parent_semantic_call_id")
                    == row.get("parent_semantic_call_id")
                ]
                if len(matching_events) != 1:
                    errors.append(
                        f"API/attempt terminal evidence mismatch at row {index}"
                    )
                if (row.get("response_slots_used")
                        != generation["state"].get("response_slots_used")
                        or row.get("transient_failure_count")
                        != generation["state"].get(
                            "transient_failure_count")
                        or row.get("http_attempts_used")
                        != generation["state"].get("http_attempts_used")):
                    errors.append(
                        f"API/attempt budget mismatch at row {index}"
                    )
                if generation_index > 0 and formal_manifest:
                    authorization = authorizations_by_worker.get(
                        row.get("worker_launch_id"), {}
                    ).get("transport_resume_authorization")
                    starts = [
                        attempt.get("attempt_index")
                        for attempt in generation["records"]
                        if attempt.get("event") == "attempt_start"
                    ]
                    if (not starts or not isinstance(authorization, dict)
                            or authorization.get("next_attempt_index")
                            != min(starts)):
                        errors.append(
                            f"API recovery next-attempt mismatch at row {index}"
                        )
                    parent_rows = semantic_groups.get(
                        generation["parent_semantic_call_id"], []
                    )
                    parent_failures = [
                        parent_row for _parent_index, parent_row in parent_rows
                        if parent_row.get("provider_called") is True
                        and parent_row.get("classification")
                        == "provider/API failure"
                    ]
                    if (len(parent_failures) != 1
                            or not isinstance(authorization, dict)
                            or authorization.get("prior_worker_launch_id")
                            != parent_failures[0].get("worker_launch_id")
                            or authorization.get("prior_invocation_id")
                            not in {
                                metadata_row.get("invocation_id")
                                for metadata_row in metadata_by_worker.get(
                                    parent_failures[0].get(
                                        "worker_launch_id"), []
                                )
                                if metadata_row.get("status")
                                == "infrastructure_incomplete"
                            }):
                        errors.append(
                            f"API recovery parent provenance mismatch at row {index}"
                        )
            if generation["state"].get("response_committed") or replay_rows:
                digest = hashlib.sha256(
                    semantic_call_id.encode("utf-8")
                ).hexdigest()[:24]
                journal_path = os.path.join(
                    out_dir, "api_journal", f"{digest}.response.json"
                )
                journal = _read_json(journal_path) if os.path.isfile(
                    journal_path) else None
                result = (
                    journal.get("result")
                    if isinstance(journal, dict) else None
                )
                committed_events = [
                    attempt for _attempt_index, attempt
                    in attempt_groups.get(semantic_call_id, [])
                    if attempt.get("event") == "response_committed"
                ]
                if (not isinstance(journal, dict)
                        or journal.get("schema")
                        != API_RESPONSE_JOURNAL_SCHEMA
                        or journal.get("semantic_call_id") != semantic_call_id
                        or journal.get("semantic_root_id") != semantic_root_id
                        or journal.get("generation_index") != generation_index
                        or journal.get("parent_semantic_call_id")
                        != generation["parent_semantic_call_id"]
                        or not isinstance(journal.get("call_id"), str)
                        or not journal.get("call_id")
                        or journal.get("request_fingerprint")
                        != lineage["request_fingerprint"]
                        or len(committed_events) != 1
                        or journal.get("call_id")
                        != committed_events[0].get("call_id")
                        or not isinstance(result, dict)
                        or result.get("semantic_call_id")
                        != semantic_call_id
                        or result.get("semantic_root_id")
                        != semantic_root_id
                        or result.get("generation_index")
                        != generation_index
                        or result.get("parent_semantic_call_id")
                        != generation["parent_semantic_call_id"]
                        or result.get("transport_revision")
                        != TRANSPORT_REVISION
                        or result.get("transport_resume_policy")
                        != TRANSPORT_RESUME_POLICY
                        or result.get("call_kind")
                        != identity.get("call_kind")
                        or result.get("stream_complete") is not True
                        or not isinstance(result.get("stop_reason"), str)
                        or not result.get("stop_reason").strip()
                        or not isinstance(result.get("message"), str)
                        or result.get("input_tokens") is None
                        or result.get("output_tokens") is None
                        or result.get("max_response_slots") != 2
                        or result.get("max_transient_failures") != 3
                        or result.get("response_slots_used")
                        != generation["state"].get("response_slots_used")
                        or result.get("transient_failure_count")
                        != generation["state"].get(
                            "transient_failure_count")
                        or result.get("http_attempts_used")
                        != generation["state"].get("http_attempts_used")
                        or len(provider_rows) != 1
                        or provider_rows[0][1].get("request_id")
                        != journal.get("call_id")):
                    errors.append(
                        f"missing/mismatched response journal: {semantic_call_id}"
                    )
                else:
                    journal_call_id = journal["call_id"]
                    for index, row in replay_rows:
                        if (not row.get("response_replayed")
                                or row.get("replayed_from_call_id")
                                != journal_call_id):
                            errors.append(
                                f"invalid response replay chain at API row {index}"
                            )

    for step, semantic_calls in calls_by_step.items():
        method = step[1]
        expected_primary = (
            "hybridpatch_primary"
            if method == "hybridpatch" else "fullrewrite_primary"
        )
        call_kinds = [item[0] for item in semantic_calls]
        if call_kinds.count(expected_primary) > 1:
            errors.append(f"duplicate primary semantic call: {step}")
        if method == "hybridpatch" and call_kinds.count(
                "hybridpatch_repair") > 1:
            errors.append(f"duplicate repair semantic call: {step}")
        if method == "fullrewrite" and len(call_kinds) > 1:
            errors.append(f"FullRewrite extra semantic call: {step}")

    for sample in config["samples"]:
        for method in config["method_set"]:
            rows = committed_rows[(sample, method)]
            seen = set()
            by_rt = {}
            for row in rows:
                if (row.get("sample_id") != sample
                        or row.get("method") != method):
                    errors.append(
                        f"committed row identity mismatch: {method}/{sample}"
                    )
                key = (row.get("round_trip_num"), row.get("round_trip_direction"))
                if key in seen:
                    errors.append(f"duplicate committed row: {method}/{sample}/{key}")
                seen.add(key)
                rt, direction = key
                if (not _is_exact_int(rt) or not 1 <= rt <= target_rt
                        or direction not in {"forward", "backward"}):
                    errors.append(f"invalid committed row key: {method}/{sample}/{key}")
                    continue
                by_rt.setdefault(rt, set()).add(direction)
                if (sample, method, rt, direction) not in api_keys:
                    errors.append(
                        f"committed row has no mapped API call: "
                        f"{method}/{sample}/RT{rt}/{direction}"
                    )
                step = (sample, method, rt, direction)
                expected_primary = (
                    "hybridpatch_primary"
                    if method == "hybridpatch" else "fullrewrite_primary"
                )
                step_call_kinds = [
                    item[0] for item in calls_by_step.get(step, [])
                ]
                if step_call_kinds.count(expected_primary) != 1:
                    errors.append(
                        f"committed row requires exactly one primary "
                        f"semantic call: {method}/{sample}/RT{rt}/{direction}"
                    )
                if method == "hybridpatch":
                    bdpatch = row.get("bdpatch")
                    count = (
                        bdpatch.get("preservation_violations")
                        if isinstance(bdpatch, dict) else None
                    )
                    exec_log = (
                        bdpatch.get("exec_log")
                        if isinstance(bdpatch, dict) else None
                    )
                    nested_count = (
                        exec_log.get("preservation_violations")
                        if isinstance(exec_log, dict) else None
                    )
                    telemetry = (
                        bdpatch.get("hybrid")
                        if isinstance(bdpatch, dict) else None
                    )
                    explicit_na = (
                        isinstance(bdpatch, dict)
                        and "preservation_violations" in bdpatch
                        and count is None
                        and exec_log is None
                        and bdpatch.get("actual_method")
                        == "hybridpatch_protocol_failure_kept_context"
                        and isinstance(telemetry, dict)
                        and telemetry.get("failed_step_kept_context") is True
                        and telemetry.get("effective_modification") is False
                    )
                    if explicit_na:
                        preservation_not_applicable += 1
                    elif (not _is_exact_int(count) or count < 0
                          or not isinstance(exec_log, dict)
                          or not _is_exact_int(nested_count)
                          or nested_count < 0
                          or nested_count != count):
                        errors.append(
                            "invalid/missing preservation telemetry: "
                            f"{method}/{sample}/RT{rt}/{direction}"
                        )
                    else:
                        preservation += count
                if rt == target_rt and direction == "backward":
                    evaluation = row.get("evaluation")
                    score = (
                        evaluation.get("score")
                        if isinstance(evaluation, dict) else None
                    )
                    score_ok = (
                        isinstance(score, (int, float))
                        and not isinstance(score, bool)
                        and math.isfinite(float(score))
                    )
                    error_ok = (
                        isinstance(evaluation, dict)
                        and isinstance(evaluation.get("error"), str)
                        and bool(evaluation.get("error"))
                    )
                    if not score_ok and not error_ok:
                        errors.append(
                            f"unscoreable exact backward RT{target_rt}: "
                            f"{method}/{sample}"
                        )
            partial = [
                rt for rt, directions in by_rt.items()
                if directions != {"forward", "backward"}
            ]
            if partial:
                errors.append(
                    f"partial committed RTs: {method}/{sample}/{partial}"
                )
            checkpoint_path = os.path.join(
                out_dir, method, f"{sample}.ckpt.json")
            checkpoint_exists = os.path.exists(checkpoint_path)
            checkpoint = (
                _read_json(checkpoint_path) if checkpoint_exists else None
            )
            completed = None
            if checkpoint_exists:
                completed_value = (
                    checkpoint.get("completed_round_trips")
                    if isinstance(checkpoint, dict) else None
                )
                completed = completed_value if _is_exact_int(
                    completed_value) and completed_value >= 0 else None
                if completed is None:
                    errors.append(
                        f"invalid checkpoint completed_round_trips: "
                        f"{method}/{sample}"
                    )
            elif rows:
                errors.append(f"missing checkpoint: {method}/{sample}")
            if sample in completion_samples and (
                    completed != target_rt or len(rows) != 2 * target_rt):
                errors.append(
                    f"incomplete task: {method}/{sample} "
                    f"checkpoint={completed}/{target_rt} rows={len(rows)}/{2 * target_rt}"
                )

    if preservation:
        errors.append(f"preservation_violations={preservation}")

    try:
        latest_outcomes = _latest_sample_outcomes(
            out_dir, expected_samples)
    except RuntimeError as exc:
        latest_outcomes = {}
        errors.append(str(exc))
    latest_by_sample = {}
    for record in metadata:
        for sample in record.get("samples") or []:
            if sample in expected_samples:
                latest_by_sample[sample] = record
    for sample, record in latest_by_sample.items():
        if record.get("status") == "failed" and sample not in active_samples:
            errors.append(f"latest run_metadata invocation failed: {sample}")
        expected_plan = (manifest.get("task_plans") or {}).get(sample) or {}
        registered_plan = (record.get("task_plans") or {}).get(sample)
        expected_registration = {
            "sha256": expected_plan.get("sha256"),
            "round_trips": target_rt,
        }
        if (registered_plan != expected_registration
                and sample not in active_samples):
            errors.append(f"run_metadata task-plan mismatch: {sample}")
        outcome = latest_outcomes.get(sample)
        if (record.get("status") in {
                "finished", "infrastructure_incomplete"}
                and (not isinstance(outcome, dict)
                     or outcome.get("status") != record.get("status")
                     or outcome.get("invocation_id")
                     != record.get("invocation_id"))):
            errors.append(f"sample outcome/run metadata mismatch: {sample}")
    if completion_samples:
        missing_metadata = completion_samples - set(latest_by_sample)
        if missing_metadata:
            errors.append(
                f"samples missing run_metadata: {sorted(missing_metadata)}"
            )
        if any(
            latest_by_sample.get(sample, {}).get("status") != "finished"
            for sample in completion_samples
        ):
            errors.append(
                "not all required latest run_metadata invocations are finished"
            )
        if require_complete and any(
                record.get("finished_at") is None for record in metadata):
            errors.append("campaign finished_at is incomplete")
    if formal_manifest and require_complete:
        for worker_id, launch in launches_by_worker.items():
            exit_row = exits_by_worker.get(worker_id)
            reconciliation = reconciliations_by_worker.get(worker_id)
            created_at = (
                exit_row.get("created_at")
                if isinstance(exit_row, dict) else None
            )
            timestamp_ok = False
            if isinstance(created_at, str):
                try:
                    timestamp_ok = (
                        datetime.fromisoformat(created_at).tzinfo is not None
                    )
                except ValueError:
                    timestamp_ok = False
            exit_ok = (isinstance(exit_row, dict)
                    and exit_row.get("sample") == launch.get("sample")
                    and exit_row.get("pid") == launch.get("pid")
                    and isinstance(exit_row.get("returncode"), int)
                    and not isinstance(exit_row.get("returncode"), bool)
                    and timestamp_ok)
            reconciliation_time_ok = False
            if isinstance(reconciliation, dict):
                try:
                    reconciliation_time_ok = (
                        datetime.fromisoformat(
                            reconciliation.get("created_at") or ""
                        ).tzinfo is not None
                    )
                except ValueError:
                    reconciliation_time_ok = False
            reconciliation_ok = (
                isinstance(reconciliation, dict)
                and reconciliation.get("sample") == launch.get("sample")
                and reconciliation.get("pid") == launch.get("pid")
                and reconciliation.get("exit_code_observed") is False
                and any(
                    record.get("invocation_id")
                    == reconciliation.get("invocation_id")
                    and record.get("status")
                    == "interrupted_before_audited_resume"
                    for record in metadata_by_worker.get(worker_id, [])
                )
                and reconciliation_time_ok
            )
            if not exit_ok and not reconciliation_ok:
                errors.append(
                    f"worker exit provenance incomplete: {worker_id}"
                )

    return {
        "errors": sorted(set(errors)),
        "preservation_violations": preservation,
        "latched_preservation_violations": latched_preservation,
        "preservation_not_applicable": preservation_not_applicable,
        "stop_conditions": stop_records,
        "api_calls": len(api_rows),
        "semantic_calls": len(semantic_groups),
        "provider_call_rows": provider_call_rows,
    }


def _campaign_evidence_digest(out_dir, manifest):
    paths = {
        os.path.join(out_dir, name) for name in (
            "dispatch_manifest.json", "api_calls.jsonl",
            "api_attempt_ledger.jsonl", "run_metadata.jsonl",
            "dispatch_log.jsonl",
            "sample_outcomes.jsonl",
        )
    }
    for plan in (manifest.get("task_plans") or {}).values():
        paths.add(os.path.join(out_dir, plan.get("path") or ""))
    config = manifest.get("config") or {}
    for sample in config.get("samples") or []:
        for method in config.get("method_set") or []:
            paths.add(os.path.join(out_dir, method, f"{sample}.jsonl"))
            paths.add(os.path.join(out_dir, method, f"{sample}.ckpt.json"))
    journal_dir = os.path.join(out_dir, "api_journal")
    if os.path.isdir(journal_dir):
        paths.update(str(path) for path in Path(journal_dir).glob("*.json"))
    digest = hashlib.sha256()
    for path in sorted(paths):
        if not os.path.isfile(path):
            continue
        relative = os.path.relpath(path, out_dir).replace(os.sep, "/")
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        with open(path, "rb") as handle:
            digest.update(handle.read())
        digest.update(b"\0")
    return digest.hexdigest()


def evaluate_smoke_cost_gate(smoke_dir, main_out_dir):
    """Recompute the fixed 16-step smoke gate before any main-campaign Key use."""
    smoke_dir = os.path.abspath(smoke_dir)
    report_path = os.path.join(main_out_dir, "smoke_cost_gate.json")
    failure_codes = []
    smoke_manifest = None
    inspection = {"errors": ["smoke manifest unavailable"]}
    current_commit = None
    current_tree = None
    usage_rows = 0
    total_rows = 0
    total_usd = 0.0
    source_digest = None
    try:
        current_commit, current_tree = _git_identity()
        smoke_manifest = _read_json(
            os.path.join(smoke_dir, "dispatch_manifest.json"))
        config = smoke_manifest.get("config") or {}
        expected_config = {
            "campaign_role": "smoke",
            "samples": SMOKE_SAMPLES,
            "method_set": ["fullrewrite", "hybridpatch"],
            "num_round_trips": 2,
            "seed": 42,
            "model": "minimax-m3",
            "max_tokens": 131072,
            "distractor": True,
            "opencode_transport": "anthropic_sdk_v2",
            "minimax_transport": "opencode",
            "transport_revision": TRANSPORT_REVISION,
            "transport_resume_policy": TRANSPORT_RESUME_POLICY,
            "stop_on_preservation_violation": True,
        }
        if any(config.get(key) != value
               for key, value in expected_config.items()):
            failure_codes.append("smoke_config_mismatch")
        if smoke_manifest.get("schema") != SCHEMA:
            failure_codes.append("smoke_manifest_schema")
        if (smoke_manifest.get("run_git_commit") != current_commit
                or smoke_manifest.get("git_tree_state") != "clean"
                or current_tree != "clean"):
            failure_codes.append("smoke_git_identity_mismatch")
        if smoke_manifest.get("code_fingerprint") != code_fingerprint():
            failure_codes.append("smoke_code_fingerprint_mismatch")
        inspection = inspect_campaign(
            smoke_dir, smoke_manifest, require_complete=True)
        if inspection["errors"]:
            failure_codes.append("smoke_campaign_incomplete")
        for sample in SMOKE_SAMPLES:
            for method in ("fullrewrite", "hybridpatch"):
                rows = _read_jsonl(
                    os.path.join(smoke_dir, method, f"{sample}.jsonl"))
                total_rows += len(rows)
                for row in rows:
                    usd = row.get("total_usd")
                    if (isinstance(usd, (int, float))
                            and not isinstance(usd, bool)
                            and math.isfinite(float(usd)) and usd >= 0):
                        total_usd += float(usd)
                        usage_rows += 1
        if total_rows != SMOKE_GRID_STEPS:
            failure_codes.append("smoke_grid_size")
        if usage_rows != SMOKE_GRID_STEPS:
            failure_codes.append("smoke_usage_incomplete")
        source_digest = _campaign_evidence_digest(
            smoke_dir, smoke_manifest)
    except Exception as exc:
        failure_codes.append("smoke_gate_read_error")
        inspection = {"errors": [f"{type(exc).__name__}: {exc}"]}

    multiplier = MAIN_GRID_STEPS / SMOKE_GRID_STEPS
    projected_usd = total_usd * multiplier
    if (not math.isfinite(projected_usd)
            or projected_usd > SMOKE_PROJECTED_COST_LIMIT_USD + 1e-9):
        failure_codes.append("projected_cost_limit_exceeded")
    failure_codes = sorted(set(failure_codes))
    report = {
        "schema": "anchorpatch.smoke_cost_gate/1",
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "source_experiment": os.path.basename(smoke_dir),
        "source_path": smoke_dir,
        "source_digest": source_digest,
        "run_git_commit": current_commit,
        "smoke_steps_expected": SMOKE_GRID_STEPS,
        "smoke_rows_observed": total_rows,
        "usage_rows": usage_rows,
        "smoke_total_usd": total_usd,
        "projection_multiplier": multiplier,
        "projected_main_usd": projected_usd,
        "projected_cost_limit_usd": SMOKE_PROJECTED_COST_LIMIT_USD,
        "preservation_violations": inspection.get(
            "preservation_violations"),
        "inspection_errors": inspection.get("errors") or [],
        "failure_codes": failure_codes,
        "decision": "GO" if not failure_codes else "NO_GO",
    }
    if os.path.isfile(report_path):
        prior_report = _read_json(report_path)
        current_identity = dict(report)
        prior_identity = dict(prior_report)
        current_identity.pop("created_at", None)
        prior_identity.pop("created_at", None)
        if current_identity == prior_identity:
            report = prior_report
        else:
            write_json_atomic(report_path, report)
    else:
        write_json_atomic(report_path, report)
    if failure_codes:
        raise RuntimeError(
            "smoke cost gate NO_GO: " + ", ".join(failure_codes)
        )
    return {
        "report": os.path.basename(report_path),
        "report_sha256": _sha256(report_path),
        "source_experiment": report["source_experiment"],
        "source_digest": source_digest,
        "smoke_total_usd": total_usd,
        "projection_multiplier": multiplier,
        "projected_main_usd": projected_usd,
        "projected_cost_limit_usd": SMOKE_PROJECTED_COST_LIMIT_USD,
        "decision": "GO",
    }


def _terminate_workers(running):
    errors = []
    for item in running.values():
        process = item["process"]
        if process.poll() is None:
            try:
                process.terminate()
            except OSError as exc:
                errors.append(f"terminate PID {process.pid}: {exc}")
    deadline = time.time() + 15
    while time.time() < deadline and any(
            item["process"].poll() is None for item in running.values()):
        time.sleep(0.5)
    for item in running.values():
        process = item["process"]
        if process.poll() is None:
            try:
                process.kill()
            except OSError as exc:
                errors.append(f"kill PID {process.pid}: {exc}")
    for item in running.values():
        process = item["process"]
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired as exc:
            errors.append(f"wait PID {process.pid}: {exc}")
    if errors:
        raise RuntimeError("; ".join(errors))


def _append_worker_exit(sample, item, returncode, dispatch_log, *,
                        disposition=None, evidence=None):
    if item.get("exit_recorded"):
        return
    record = {
        "event": "worker_exit", "sample": sample,
        "key_label": item["key_label"],
        "worker_launch_id": item["worker_launch_id"],
        "pid": item["process"].pid,
        "returncode": returncode,
        "disposition": disposition or (
            "finished" if returncode == 0 else "campaign_fatal"),
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
    }
    if evidence is not None:
        record["evidence"] = evidence
    append_jsonl_locked(dispatch_log, record)
    item["exit_recorded"] = True


def _stop_and_reconcile_workers(out_dir, running, dispatch_log=None):
    """Best-effort stop, then close metadata only after leases are free."""
    result = {
        "termination_error": None,
        "lease_error": None,
        "closed_invocations": [],
    }
    try:
        _terminate_workers(running)
    except BaseException as exc:
        result["termination_error"] = str(exc)
    try:
        _assert_worker_leases_free(out_dir, running)
    except BaseException as exc:
        result["lease_error"] = str(exc)
        return result
    if dispatch_log:
        for sample, item in running.items():
            returncode = item["process"].poll()
            if returncode is not None:
                try:
                    _append_worker_exit(
                        sample, item, returncode, dispatch_log)
                except BaseException as exc:
                    result.setdefault("exit_record_errors", []).append(
                        f"{sample}: {exc}"
                    )
    try:
        audited = _audit_running_invocation_provenance(out_dir)
        result["audited_invocations"] = audited
        result["closed_invocations"] = (
            interrupt_audited_running_invocations(
                out_dir,
                status="interrupted_by_dispatcher",
                audited=audited,
            )
        )
    except BaseException as exc:
        result["metadata_error"] = str(exc)
    return result


def _record_worker_exit(out_dir, running, sample, item, returncode,
                        dispatch_log):
    """Remove a finished or strictly evidenced infrastructure-only worker."""
    item["log"].close()
    if returncode == 0:
        _append_worker_exit(
            sample, item, returncode, dispatch_log,
            disposition="finished")
        del running[sample]
        return "finished"
    try:
        evidence = _verified_infrastructure_incomplete(
            out_dir, sample, item)
    except BaseException as exc:
        _append_worker_exit(
            sample, item, returncode, dispatch_log,
            disposition="campaign_fatal",
            evidence={"verification_error": str(exc)},
        )
        raise RuntimeError(
            f"worker {sample} exited with {returncode}: {exc}") from exc
    _append_worker_exit(
        sample, item, returncode, dispatch_log,
        disposition="infrastructure_incomplete", evidence=evidence)
    del running[sample]
    return "infrastructure_incomplete"


def _launch_under_lease(args, out_dir):
    _validate_campaign_grid(args)
    _require_formal_opencode_transport("minimax-m3")
    upstream_smoke_gate = None
    if args.campaign_role == "main":
        upstream_smoke_gate = evaluate_smoke_cost_gate(
            args.smoke_dir, out_dir)
    keys = dict(read_keys(os.path.abspath(args.keys_file)))
    selected_labels = list(args.key_labels or sorted(keys)[:len(args.samples)])
    if len(selected_labels) != len(args.samples):
        raise RuntimeError("--key_labels must map exactly one label per sample")
    if len(set(selected_labels)) != len(selected_labels):
        raise RuntimeError("--key_labels contains duplicates")
    missing_labels = [label for label in selected_labels if label not in keys]
    if missing_labels:
        raise RuntimeError(f"unknown key labels: {missing_labels}")
    assignments = []
    for index, (sample, label) in enumerate(zip(args.samples, selected_labels)):
        assignments.append({
            "sample": sample,
            "key_label": label,
            "methods": method_order(index),
            "console_log": f"dispatch_logs/{sample}__{label}.console.log",
        })

    task_plans = prepare_task_plans(
        out_dir, args.samples, args.num_round_trips, args.seed)
    manifest = build_manifest(
        out_dir, args.samples, assignments, task_plans, args,
        upstream_smoke_gate=upstream_smoke_gate)
    manifest_path, inspection_manifest = write_or_verify_manifest(
        out_dir, manifest, resume=args.resume)
    print(f"MANIFEST {manifest_path}", flush=True)
    for item in assignments:
        print(
            f"ASSIGN {item['sample']} {item['key_label']} "
            f"{'->'.join(item['methods'])} {item['console_log']}",
            flush=True,
        )
    if args.dry_run:
        dry_assignments, _dry_authorizations = (
            _select_invocation_assignments(
                out_dir, assignments, resume=args.resume,
                target_round_trips=args.num_round_trips)
        )
        dry_active = {item["sample"] for item in dry_assignments}
        dry_complete = set(args.samples) - dry_active
        # A new dry-run has no worker metadata yet, but the inspector still
        # requires a well-formed active-set artifact before treating an open
        # sample as launchable. Mirror the real launch preflight's explicit
        # empty set; never invent worker identities during a zero-POST check.
        _write_active_worker_set(out_dir, inspection_manifest, [])
        dry_inspection = inspect_campaign(
            out_dir, inspection_manifest,
            active_samples=dry_active,
            required_complete_samples=dry_complete,
        )
        if dry_inspection["errors"]:
            raise RuntimeError(
                "dry-run campaign preflight failed: "
                + "; ".join(dry_inspection["errors"])
            )
        return 0

    dispatch_logs = os.path.join(out_dir, "dispatch_logs")
    os.makedirs(dispatch_logs, exist_ok=True)
    dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
    running = {}
    launch_assignments = list(assignments)
    resume_authorizations = {}
    incomplete_samples = set()
    completed_samples = set()
    try:
        # Never revoke a prior worker's authorization before proving its
        # process lease is free. A live orphan must remain globally visible
        # and block resume rather than being converted into authorization
        # drift by the new dispatcher.
        _assert_worker_leases_free(out_dir, args.samples)
        existing_metadata = read_run_metadata_snapshot(out_dir)
        if existing_metadata and not args.resume:
            raise RuntimeError(
                "existing run metadata requires --resume, --resume_reason, "
                "and --confirm_workers_stopped"
            )
        if args.resume:
            audited_stale = _audit_running_invocation_provenance(out_dir)
            stale = interrupt_audited_running_invocations(
                out_dir,
                status="interrupted_before_audited_resume",
                audited=audited_stale,
            )
            _assert_worker_leases_free(out_dir, args.samples)
            if any(
                    record.get("status") == "running"
                    for record in read_run_metadata_snapshot(out_dir)):
                raise RuntimeError(
                    "unaudited running invocation appeared during resume"
                )
            closed_ids = {
                item.get("invocation_id") for item in stale
            }
            for item in audited_stale:
                if item.get("invocation_id") not in closed_ids:
                    raise RuntimeError(
                        "audited stale invocation was not atomically closed"
                    )
                append_jsonl_locked(
                    dispatch_log,
                    {
                        "event": "stale_worker_reconciled",
                        "created_at": datetime.now().astimezone().isoformat(
                            timespec="seconds"),
                        "worker_launch_id": item["worker_launch_id"],
                        "pid": item["worker_pid"],
                        "sample": item["sample"],
                        "invocation_id": item["invocation_id"],
                        "exit_code_observed": False,
                        "reason": args.resume_reason,
                    },
                )
        _write_active_worker_set(out_dir, inspection_manifest, [])
        preflight = inspect_campaign(
            out_dir, inspection_manifest,
            active_samples=set(args.samples),
        )
        if preflight["errors"]:
            raise RuntimeError(
                "campaign preflight failed before worker launch: "
                + "; ".join(preflight["errors"])
            )
        launch_assignments, resume_authorizations = (
            _select_invocation_assignments(
                out_dir, assignments, resume=args.resume,
                target_round_trips=args.num_round_trips)
        )
        completed_samples = {
            item["sample"] for item in assignments
            if item not in launch_assignments
        }
        if args.resume:
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "resume_start",
                    "reason": args.resume_reason,
                    "workers_stopped_confirmed": bool(
                        args.confirm_workers_stopped
                    ),
                    "audited_stale_invocations": audited_stale,
                    "closed_stale_invocations": stale,
                    "launch_samples": [
                        item["sample"] for item in launch_assignments
                    ],
                    "skipped_finished_samples": sorted(completed_samples),
                    "transport_authorizations": resume_authorizations,
                },
            )
        launch_specs = {}
        for item in launch_assignments:
            sample = item["sample"]
            worker_id = f"paired-{sample}-{uuid.uuid4().hex[:12]}"
            ready_path, ack_path = _worker_barrier_paths(
                out_dir, worker_id)
            launch_specs[sample] = {
                "sample": sample,
                "worker_launch_id": worker_id,
                "ready_path": ready_path,
                "ack_path": ack_path,
            }
        _write_active_worker_set(
            out_dir, inspection_manifest, launch_specs.values())
        for item in launch_assignments:
            sample = item["sample"]
            label = item["key_label"]
            launch_spec = launch_specs[sample]
            worker_id = launch_spec["worker_launch_id"]
            ready_path = launch_spec["ready_path"]
            ack_path = launch_spec["ack_path"]
            command = [
                sys.executable, os.path.join("src", "experiment_runner.py"),
                "--sample", sample,
                "--methods", *item["methods"],
                "--num_round_trips", str(args.num_round_trips),
                "--seed", str(args.seed),
                "--model", "minimax-m3",
                "--max_tokens", "131072",
                "--out_dir", out_dir,
                "--stop_on_preservation_violation",
                "--notes", f"{args.notes}, key={label}",
            ]
            environment = dict(
                os.environ,
                OPENCODE_API_KEY=keys[label],
                OPENCODE_TRANSPORT="anthropic_sdk_v2",
                MINIMAX_TRANSPORT="opencode",
                MINIMAX_HARD_TIMEOUT="7200",
                PYTHONUTF8="1",
                ANCHORPATCH_WORKER_LAUNCH_ID=worker_id,
                ANCHORPATCH_WORKER_LOCK_PATH=_worker_lease_path(
                    out_dir, sample
                ),
                ANCHORPATCH_WORKER_READY_PATH=os.path.abspath(ready_path),
                ANCHORPATCH_WORKER_ACK_PATH=os.path.abspath(ack_path),
                ANCHORPATCH_ACTIVE_WORKER_SET_PATH=os.path.abspath(
                    _active_worker_set_path(out_dir)
                ),
                ANCHORPATCH_START_BARRIER_TIMEOUT=str(args.start_timeout),
                ANCHORPATCH_EXPECTED_GIT_COMMIT=(
                    inspection_manifest["run_git_commit"]
                ),
                ANCHORPATCH_EXPECTED_GIT_TREE_STATE="clean",
                ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256=(
                    task_plans[sample]["sha256"]
                ),
                ANCHORPATCH_EXPECTED_TASK_PLAN_PATH=os.path.abspath(
                    os.path.join(out_dir, task_plans[sample]["path"])
                ),
            )
            environment.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID", None)
            environment.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX", None)
            environment.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT", None)
            environment.pop(
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX", None)
            authorization = resume_authorizations.get(sample)
            if authorization is not None:
                _validate_resume_authorization(authorization)
                environment[
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID"
                ] = authorization["parent_semantic_call_id"]
                environment[
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX"
                ] = str(authorization["generation_index"])
                environment[
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT"
                ] = authorization["request_fingerprint"]
                environment[
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX"
                ] = str(authorization["next_attempt_index"])
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "launch_intent", "sample": sample,
                    "key_label": label, "methods": item["methods"],
                    "worker_launch_id": worker_id,
                    "console_log": item["console_log"],
                },
            )
            log_path = os.path.join(out_dir, item["console_log"])
            log_handle = open(log_path, "a", encoding="utf-8")
            process = subprocess.Popen(
                command, cwd=_ROOT, env=environment,
                stdout=log_handle, stderr=subprocess.STDOUT,
            )
            running[sample] = {
                "sample": sample,
                "process": process,
                "log": log_handle,
                "key_label": label,
                "methods": list(item["methods"]),
                "target_round_trips": args.num_round_trips,
                "resume_authorization": authorization,
                "worker_launch_id": worker_id,
                "ready_path": ready_path,
                "ack_path": ack_path,
                "exit_recorded": False,
            }
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "launch", "sample": sample,
                    "key_label": label, "methods": item["methods"],
                    "pid": process.pid, "worker_launch_id": worker_id,
                    "console_log": item["console_log"],
                },
            )

        if running:
            _authorize_workers(
                out_dir, running, task_plans, dispatch_log,
                args.start_timeout,
            )
        last_report = 0.0
        while running:
            time.sleep(args.poll_interval)
            inspection = inspect_campaign(
                out_dir, inspection_manifest,
                active_samples=set(running),
            )
            if inspection["errors"]:
                raise RuntimeError("; ".join(inspection["errors"]))
            for sample, item in list(running.items()):
                returncode = item["process"].poll()
                if returncode is None:
                    continue
                disposition = _record_worker_exit(
                    out_dir, running, sample, item, returncode,
                    dispatch_log)
                _write_active_worker_set(
                    out_dir, inspection_manifest, running.values())
                if disposition == "infrastructure_incomplete":
                    incomplete_samples.add(sample)
                    continue
                completed_samples.add(sample)
                sample_inspection = inspect_campaign(
                    out_dir, inspection_manifest,
                    active_samples=set(running),
                    required_complete_samples={sample},
                )
                if sample_inspection["errors"]:
                    raise RuntimeError(
                        "; ".join(sample_inspection["errors"])
                    )
            if time.time() - last_report >= args.progress_interval:
                print(
                    f"PROGRESS running={len(running)}/{len(assignments)} "
                    f"api_calls={inspection['api_calls']} preservation=0",
                    flush=True,
                )
                last_report = time.time()

        _write_active_worker_set(out_dir, inspection_manifest, [])
        if incomplete_samples:
            inspection = inspect_campaign(
                out_dir, inspection_manifest,
                required_complete_samples=completed_samples)
            if inspection["errors"]:
                raise RuntimeError("; ".join(inspection["errors"]))
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "campaign_incomplete",
                    "infrastructure_incomplete_samples": sorted(
                        incomplete_samples),
                    "completed_samples": sorted(completed_samples),
                    "api_calls": inspection["api_calls"],
                    "preservation_violations": 0,
                },
            )
            print(
                "RESULT INCOMPLETE infrastructure_samples="
                + ",".join(sorted(incomplete_samples)),
                file=sys.stderr, flush=True,
            )
            return 2
        inspection = inspect_campaign(
            out_dir, inspection_manifest, require_complete=True)
        if inspection["errors"]:
            raise RuntimeError("; ".join(inspection["errors"]))
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "campaign_complete",
                "api_calls": inspection["api_calls"],
                "preservation_violations": 0,
            },
        )
        print(
            f"RESULT PASS workers={len(assignments)} "
            f"api_calls={inspection['api_calls']} preservation=0",
            flush=True,
        )
        return 0
    except BaseException as exc:
        try:
            record_campaign_stop_condition(
                out_dir, "dispatcher_integrity_failure",
                error_type=type(exc).__name__, error=str(exc),
            )
        except BaseException:
            pass
        reconciliation = _stop_and_reconcile_workers(
            out_dir, running, dispatch_log)
        safe_to_revoke = (
            reconciliation.get("lease_error") is None
            and reconciliation.get("metadata_error") is None
            and not reconciliation.get("exit_record_errors")
        )
        if safe_to_revoke:
            try:
                _write_active_worker_set(out_dir, inspection_manifest, [])
            except BaseException as active_error:
                reconciliation["active_set_error"] = str(active_error)
        else:
            # A worker that still owns its sample lease must retain its active
            # authorization until an audited later reconciliation proves it
            # stopped.  Revoking it here would create artificial provenance
            # drift while an in-flight call is still unwinding.
            reconciliation["active_set_retained"] = sorted(running)
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "campaign_stop", "error": str(exc),
                "worker_reconciliation": reconciliation,
            },
        )
        print(f"RESULT FAIL {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        for item in running.values():
            try:
                item["log"].close()
            except Exception:
                pass


def launch(args):
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
    lease = open(lease_path, "a+", encoding="utf-8")
    try:
        try:
            portalocker.lock(
                lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException as exc:
            raise RuntimeError(
                f"another paired dispatcher owns {out_dir}"
            ) from exc
        return _launch_under_lease(args, out_dir)
    finally:
        try:
            portalocker.unlock(lease)
        finally:
            lease.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out_dir", required=True)
    parser.add_argument(
        "--campaign_role", choices=("smoke", "main"), required=True)
    parser.add_argument(
        "--smoke_dir",
        help="required completed 2-sample smoke directory for main",
    )
    parser.add_argument("--samples", nargs="+", required=True)
    parser.add_argument("--num_round_trips", type=int, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--keys_file", default=os.path.abspath(
            os.path.join(_ROOT, "..", ".env.frkeys")),
    )
    parser.add_argument("--key_labels", nargs="+", default=None)
    parser.add_argument("--notes", required=True)
    parser.add_argument("--poll_interval", type=float, default=5.0)
    parser.add_argument("--progress_interval", type=float, default=30.0)
    parser.add_argument("--start_timeout", type=float, default=300.0)
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--resume_reason", default=None)
    parser.add_argument(
        "--confirm_workers_stopped", action="store_true",
        help="required for audited resume; the dispatcher also verifies every "
             "per-sample process lease before closing stale metadata",
    )
    args = parser.parse_args()
    if args.campaign_role == "smoke":
        if args.samples != SMOKE_SAMPLES or args.num_round_trips != 2:
            parser.error(
                "smoke requires exactly treebank4 obj3d2 with 2 round trips"
            )
        if args.smoke_dir:
            parser.error("--smoke_dir is only valid for main")
    else:
        if args.samples != MAIN_SAMPLES or args.num_round_trips != 10:
            parser.error(
                "main requires the fixed 10-sample order with 10 round trips"
            )
        if not args.smoke_dir:
            parser.error("main requires --smoke_dir")
    if args.num_round_trips < 1:
        parser.error("--num_round_trips must be >= 1")
    if args.poll_interval < 1 or args.progress_interval < 1:
        parser.error("poll/progress intervals must be >= 1 second")
    if args.start_timeout < 1:
        parser.error("--start_timeout must be >= 1 second")
    if len(set(args.samples)) != len(args.samples):
        parser.error("--samples contains duplicates")
    if args.resume and not args.resume_reason:
        parser.error("--resume requires --resume_reason")
    if args.resume and not args.confirm_workers_stopped:
        parser.error("--resume requires --confirm_workers_stopped")
    if args.resume_reason and not args.resume:
        parser.error("--resume_reason requires --resume")
    if args.confirm_workers_stopped and not args.resume:
        parser.error("--confirm_workers_stopped requires --resume")
    return launch(args)


if __name__ == "__main__":
    raise SystemExit(main())
