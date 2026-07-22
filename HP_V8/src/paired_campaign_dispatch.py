"""Integrity-fail-fast paired MiniMax campaign launcher for HP_V8.

The dispatcher never prints key values.  It pre-generates and hashes every
task plan, records a deterministic sample/key-label/method-order manifest,
launches one process per sample through work-conserving per-key queues, can
enforce a durable all-HP-before-any-FR phase barrier, isolates only fully
evidenced provider/transport exhaustion or domain-evaluator failure to that
sample, and stops all workers on preservation or shared campaign-integrity
failures.
"""

import argparse
import contextlib
import copy
from datetime import datetime
import hashlib
import importlib.util
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
    _canonical_record_sha256,
    _git_identity,
    _validated_transport_ledger_state,
    append_jsonl_locked,
    campaign_recovery_incident_evidence,
    code_fingerprint,
    interrupt_audited_running_invocations,
    read_campaign_recovery_authorization,
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
SUPPLEMENTAL_SAMPLES = [
    "crystal6", "docker3", "earncall1", "emails5", "filesystem2",
    "filesystem3", "fonteng1", "fonteng5", "foodmenu1", "geodata1",
    "geodata4", "geotrack5", "geotrack6", "hamradio6", "jobboard3",
    "json2", "json4", "landmarks2", "landmarks3", "libcatalog2",
    "libcatalog5", "mathlean4", "mathlean5", "obj3d2", "obj3d5",
    "quantum1", "quantum5", "robotics1", "robotics3", "satellite4",
    "screenplay4", "screenplay5", "spreadsheet1", "spreadsheet6",
    "subtitles2", "subtitles6", "transit1", "transit2", "treebank2",
    "treebank4",
]
SMOKE_GRID_STEPS = 16
MAIN_GRID_STEPS = 400
SUPPLEMENTAL_GRID_STEPS = 1600
CONFIRMATION_SAMPLE_COUNT = 68
CONFIRMATION_RESERVE_COUNT = 17
CONFIRMATION_CANDIDATE_COUNT = 85
CONFIRMATION_KEY_COUNT = 13
CONFIRMATION_SLOTS_PER_KEY = 4
CONFIRMATION_GRID_STEPS = 2720
FULL234_SAMPLE_COUNT = 234
FULL234_KEY_COUNT = 13
FULL234_SLOTS_PER_KEY = 4
REMAINING134_SAMPLE_COUNT = 134
REMAINING134_EXCLUDED_SAMPLE_COUNT = 100
REMAINING134_KEY_COUNT = 14
REMAINING134_SLOTS_PER_KEY = 4
REMAINING134_METHOD_PHASES = ("hybridpatch", "fullrewrite")
REMAINING134_SELECTION_PATH = os.path.join(
    _ROOT, "analysis", "20260719_hybridv8_transportv4_mixed_confirmation100",
    "selection.json",
)
REMAINING134_SELECTION_SHA256 = (
    "26da1c3d27a1eb83d70af444197afde7a7f20ade20c6e232f2d6abe605d3d654"
)
WORK_CONSERVING_DISPATCH_POLICY = "per_key_work_conserving_v1"
PHASED_WORK_CONSERVING_DISPATCH_POLICY = (
    "per_key_work_conserving_hp_then_fr_v1"
)
METHOD_PHASE_COMPLETE_SCHEMA = "anchorpatch.method_phase_complete/1"
CONFIRMATION_KNOWN_USAGE_LIMIT_USD = 130.0
CONFIRMATION_EXPERIMENT_ID = (
    "exp_20260719_hybridv8_transportv4_method_unseen_confirmation68"
)
CONFIRMATION_DOCUMENTED_DEVELOPER_EXPOSURE = {"python1"}
CONFIRMATION_SELECTION_SCHEMA = (
    "anchorpatch.method_unseen_confirmation_split/1"
)
MIXED_CONFIRMATION_SELECTION_SCHEMA = "anchorpatch.mixed_confirmation_split/1"
MIXED_CONFIRMATION_EXPERIMENT_ID = (
    "exp_20260719_hybridv8_transportv4_mixed_confirmation100"
)
MIXED_CONFIRMATION_CANDIDATE_COUNT = 190
MIXED_CONFIRMATION_SAMPLE_COUNT = 100
MIXED_CONFIRMATION_RESERVE_COUNT = 90
CONFIRMATION_ANALYSIS_POLICY = {
    "schema": "anchorpatch.confirmation_analysis_policy/1",
    "sensitivity_sets": [["python4"], ["audiosyn1"],
                         ["python4", "audiosyn1"]],
    "bootstrap": {"seed": 42, "resamples": 10000},
    "critical_failure": {
        "metric": "CriticalFailure",
        "threshold": 0.10,
    },
    "evaluator_error_policy": {
        "backward_rows_require": "finite_score_0_1_or_nonempty_error",
        "error_row_score": 0.0,
        "error_rows_reported_separately": True,
    },
    "known_committed_usage_wave_boundary_stop": {
        "scope": "refill_boundary_committed_result_rows",
        "usd_threshold": CONFIRMATION_KNOWN_USAGE_LIMIT_USD,
    },
}
MIXED_CONFIRMATION_ANALYSIS_POLICY = {
    "schema": "anchorpatch.mixed_confirmation_analysis_policy/1",
    "headline_scope": "selected100",
    "pre_registered_cohorts": [
        "method_unseen",
        "historical_hp_method_exposed",
    ],
    "bootstrap": {"seed": 42, "resamples": 10000},
    "critical_failure": {
        "metric": "CriticalFailure",
        "threshold": 0.10,
    },
    "evaluator_error_policy": {
        "backward_rows_require": "finite_score_0_1_or_nonempty_error",
        "error_row_score": 0.0,
        "error_rows_reported_separately": True,
    },
    "known_committed_usage_wave_boundary_stop": {
        "scope": "refill_boundary_committed_result_rows",
        "usd_threshold": CONFIRMATION_KNOWN_USAGE_LIMIT_USD,
    },
}
_CONFIRMATION_SELECTION_VERIFIED_TOKEN = object()
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


def _resume_authorization_consumed_before(
        resume, api_rows, api_index, *, sample, worker_launch_id, out_dir):
    """Return true when an invocation-level resume auth was already consumed."""
    try:
        parent = _validate_resume_authorization(resume)
    except RuntimeError:
        return False
    if parent["sample"] != sample:
        return False
    matches = [
        row for index, row in enumerate(api_rows, 1)
        if index < api_index
        and row.get("worker_launch_id") == worker_launch_id
        and row.get("sample") == sample
        and row.get("semantic_call_id") == resume["semantic_call_id"]
        and row.get("semantic_root_id") == resume["semantic_root_id"]
        and row.get("generation_index") == resume["generation_index"]
        and row.get("parent_semantic_call_id")
        == resume["parent_semantic_call_id"]
        and row.get("request_fingerprint") == resume["request_fingerprint"]
        and row.get("provider_called") is True
        and row.get("response_replayed") is False
        and row.get("classification") != "provider/API failure"
        and row.get("count_as_method_failure") is False
    ]
    if len(matches) != 1:
        return False
    prior = matches[0]
    digest = hashlib.sha256(
        resume["semantic_call_id"].encode("utf-8")).hexdigest()[:24]
    journal_path = os.path.join(
        out_dir, "api_journal", f"{digest}.response.json")
    if not os.path.isfile(journal_path):
        return False
    try:
        journal = _read_json(journal_path)
    except (OSError, ValueError, RuntimeError):
        return False
    result = journal.get("result") if isinstance(journal, dict) else None
    if (not isinstance(result, dict)
            or journal.get("schema") != API_RESPONSE_JOURNAL_SCHEMA
            or journal.get("semantic_root_id") != resume["semantic_root_id"]
            or journal.get("semantic_call_id") != resume["semantic_call_id"]
            or journal.get("generation_index") != resume["generation_index"]
            or journal.get("parent_semantic_call_id")
            != resume["parent_semantic_call_id"]
            or journal.get("call_id") != prior.get("request_id")
            or journal.get("request_fingerprint")
            != resume["request_fingerprint"]
            or result.get("semantic_root_id") != resume["semantic_root_id"]
            or result.get("semantic_call_id") != resume["semantic_call_id"]
            or result.get("generation_index") != resume["generation_index"]
            or result.get("parent_semantic_call_id")
            != resume["parent_semantic_call_id"]
            or result.get("stream_complete") is not True
            or result.get("transport_revision") != TRANSPORT_REVISION
            or result.get("transport_resume_policy")
            != TRANSPORT_RESUME_POLICY):
        return False
    ledger = _read_jsonl(os.path.join(out_dir, "api_attempt_ledger.jsonl"))
    committed = [
        row for row in ledger
        if row.get("schema") == API_ATTEMPT_SCHEMA
        and row.get("event") == "response_committed"
        and row.get("semantic_call_id") == resume["semantic_call_id"]
        and row.get("semantic_root_id") == resume["semantic_root_id"]
        and row.get("generation_index") == resume["generation_index"]
        and row.get("parent_semantic_call_id")
        == resume["parent_semantic_call_id"]
        and row.get("call_id") == prior.get("request_id")
        and row.get("request_fingerprint") == resume["request_fingerprint"]
    ]
    return len(committed) == 1


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
        attempt_rows, semantic_root_id, *, allow_open_attempt=False,
        authorized_provider_access_parents=frozenset()):
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
            ordinary_exhaustion = (
                terminal.get("status") == "provider_failure"
                and state.get("call_failed")
                and state.get("last_attempt_status") == "retryable_error"
                and state.get("retry_budget_exhausted")
            )
            authorized_access_denial = (
                exact_id in authorized_provider_access_parents
                and terminal.get("status") == "provider_failure"
                and terminal.get("error_type") == "provider_access_denied"
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
    """Return the deterministic counterbalanced paired-method order."""
    if index % 2 == 0:
        return ["hybridpatch", "fullrewrite"]
    return ["fullrewrite", "hybridpatch"]


def build_key_assignments(
        samples, key_labels, slots_per_key, *, alternate_within_key=False,
        allow_queue=False):
    """Balance sample workers over keys and alternate each key's first arm.

    A sample remains one audited worker that runs both paired methods.  The
    concurrency limit is therefore expressed in sample workers per key.  The
    first method alternates independently within every key so a multi-slot key
    starts HybridPatch and FullRewrite work concurrently instead of creating
    method-wide waves.
    """
    labels = list(key_labels)
    if not labels or len(labels) != len(set(labels)):
        raise RuntimeError("--key_labels must be a non-empty unique key pool")
    if (not _is_exact_int(slots_per_key) or slots_per_key < 1):
        raise RuntimeError("--slots_per_key must be >= 1")
    capacity = len(labels) * slots_per_key
    if len(samples) > capacity and not allow_queue:
        raise RuntimeError(
            "sample count exceeds key concurrency capacity: "
            f"{len(samples)} > {len(labels)} x {slots_per_key}"
        )
    schedule = []
    while len(schedule) < len(samples):
        schedule.extend(
            label
            for _slot_index in range(slots_per_key)
            for label in labels
        )
    schedule = schedule[:len(samples)]
    per_key_indexes = {label: 0 for label in labels}
    per_key_starts = {
        label: index % 2 for index, label in enumerate(labels)
    }
    assignments = []
    for sample, label in zip(samples, schedule):
        order_index = (
            per_key_indexes[label] + per_key_starts[label]
            if alternate_within_key
            else len(assignments)
        )
        assignments.append({
            "sample": sample,
            "key_label": label,
            "methods": method_order(order_index),
            "console_log": f"dispatch_logs/{sample}__{label}.console.log",
        })
        per_key_indexes[label] += 1
    return assignments


def _mixed_confirmation_sample_order(
        samples, key_labels, slots_per_key, selection_record):
    """Order selected100 so both pre-registered cohorts are arm-balanced."""
    cohorts = selection_record.get("cohorts") or {}
    unseen = sorted(cohorts.get("method_unseen") or [])
    historical = sorted(
        cohorts.get("historical_hp_method_exposed") or [])
    if (len(unseen) != 60 or len(historical) != 40
            or set(unseen) & set(historical)
            or set(unseen) | set(historical) != set(samples)):
        raise RuntimeError("mixed confirmation cohort assignment is invalid")
    placeholders = [f"position-{index:03d}" for index in range(len(samples))]
    template = build_key_assignments(
        placeholders, key_labels, slots_per_key,
        alternate_within_key=True, allow_queue=True)
    hp_positions = [
        index for index, item in enumerate(template)
        if item["methods"][0] == "hybridpatch"]
    fr_positions = [
        index for index, item in enumerate(template)
        if item["methods"][0] == "fullrewrite"]
    if len(hp_positions) != 50 or len(fr_positions) != 50:
        raise RuntimeError("mixed confirmation method template is not 50/50")
    ordered = [None] * len(samples)
    for position, sample in zip(hp_positions[:30], unseen[:30]):
        ordered[position] = sample
    for position, sample in zip(fr_positions[:30], unseen[30:]):
        ordered[position] = sample
    for position, sample in zip(hp_positions[30:], historical[:20]):
        ordered[position] = sample
    for position, sample in zip(fr_positions[30:], historical[20:]):
        ordered[position] = sample
    if any(sample is None for sample in ordered):
        raise RuntimeError("mixed confirmation sample ordering is incomplete")
    return ordered


def _assignment_key_queues(assignments):
    """Describe stable FIFO queues without imposing cross-key barriers."""
    queues = {}
    for item in assignments:
        label = item.get("key_label")
        sample = item.get("sample")
        if (not isinstance(label, str) or not label
                or not isinstance(sample, str) or not sample):
            raise RuntimeError("assignment queue identity is invalid")
        queues.setdefault(label, []).append(item)
    return [
        {
            "key_label": label,
            "sample_ids": [item["sample"] for item in items],
            "worker_count": len(items),
            "hybridpatch_first": sum(
                item["methods"][0] == "hybridpatch" for item in items),
            "fullrewrite_first": sum(
                item["methods"][0] == "fullrewrite" for item in items),
        }
        for label, items in queues.items()
    ]


def _assignments_for_method_phase(assignments, method_phase):
    if method_phase not in REMAINING134_METHOD_PHASES:
        raise RuntimeError(f"unsupported method phase: {method_phase!r}")
    phased = []
    for item in assignments:
        clone = dict(item)
        clone["methods"] = [method_phase]
        clone["method_phase"] = method_phase
        clone["console_log"] = (
            f"dispatch_logs/{item['sample']}__{item['key_label']}__"
            f"{method_phase}.console.log"
        )
        phased.append(clone)
    return phased


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
    elif args.campaign_role == "supplemental":
        if (list(args.samples) != SUPPLEMENTAL_SAMPLES
                or args.num_round_trips != 10):
            raise RuntimeError(
                "supplemental requires the fixed 1,600-step val40 grid"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError("supplemental cannot declare --smoke_dir")
        if getattr(args, "slots_per_key", None) != 4:
            raise RuntimeError(
                "supplemental requires --slots_per_key 4"
            )
    elif args.campaign_role == "confirmation":
        selection_record = _argument_value(
            args, "_selection_manifest_record", {}) or {}
        expected_count = (
            MIXED_CONFIRMATION_SAMPLE_COUNT
            if selection_record.get("schema")
            == MIXED_CONFIRMATION_SELECTION_SCHEMA
            else CONFIRMATION_SAMPLE_COUNT
        )
        if (expected_count not in {
                CONFIRMATION_SAMPLE_COUNT, MIXED_CONFIRMATION_SAMPLE_COUNT}
                or len(list(args.samples)) != expected_count
                or len(set(args.samples)) != expected_count
                or args.num_round_trips != 10):
            raise RuntimeError(
                "confirmation requires the committed selection grid at 10 RT"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError("confirmation cannot declare --smoke_dir")
        if (getattr(args, "slots_per_key", None)
                != CONFIRMATION_SLOTS_PER_KEY):
            raise RuntimeError(
                "confirmation requires --slots_per_key 4"
            )
    elif args.campaign_role == "full234":
        scope = _argument_value(args, "_full234_scope_record", {}) or {}
        if (scope.get("schema") != "anchorpatch.full234_scope/1"
                or list(args.samples) != list(scope.get("sample_ids") or [])
                or len(args.samples) != FULL234_SAMPLE_COUNT
                or args.num_round_trips != 10):
            raise RuntimeError(
                "full234 requires the exact 234-sample inventory at 10 RT"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError("full234 cannot declare --smoke_dir")
        if (getattr(args, "slots_per_key", None)
                != FULL234_SLOTS_PER_KEY):
            raise RuntimeError("full234 requires --slots_per_key 4")
    elif args.campaign_role == "remaining134":
        scope = _argument_value(args, "_remaining134_scope_record", {}) or {}
        if (scope.get("schema") != "anchorpatch.remaining134_scope/1"
                or list(args.samples) != list(scope.get("sample_ids") or [])
                or len(args.samples) != REMAINING134_SAMPLE_COUNT
                or args.num_round_trips != 10):
            raise RuntimeError(
                "remaining134 requires the exact unselected 134-sample "
                "inventory at 10 RT"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError("remaining134 cannot declare --smoke_dir")
        if (getattr(args, "slots_per_key", None)
                != REMAINING134_SLOTS_PER_KEY):
            raise RuntimeError("remaining134 requires --slots_per_key 4")
    else:
        raise RuntimeError(f"unsupported campaign role: {args.campaign_role}")


def _sha256(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def _read_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _canonical_json_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _load_full234_scope():
    """Return the exact current 234-sample inventory and its content digest."""
    root = Path(SAMPLES_ROOT)
    sample_paths = {
        child.name: child / "sample.json"
        for child in root.iterdir()
        if child.is_dir() and (child / "sample.json").is_file()
    }
    sample_ids = sorted(sample_paths)
    if (len(sample_ids) != FULL234_SAMPLE_COUNT
            or len(set(sample_ids)) != FULL234_SAMPLE_COUNT):
        raise RuntimeError(
            "full234 requires exactly 234 unique runnable sample folders"
        )
    sample_hashes = {
        sample: _sha256(sample_paths[sample]) for sample in sample_ids
    }
    return sample_ids, {
        "schema": "anchorpatch.full234_scope/1",
        "sample_count": len(sample_ids),
        "sample_ids": sample_ids,
        "sample_json_sha256": hashlib.sha256(
            _canonical_json_bytes(sample_hashes)).hexdigest(),
    }


def _resolve_full234_scope(args):
    if _argument_value(args, "campaign_role") != "full234":
        return None
    cached = _argument_value(args, "_full234_scope_record")
    if cached is None:
        samples, cached = _load_full234_scope()
        args._full234_scope_record = cached
    else:
        samples = list(cached.get("sample_ids") or [])
    manual = _argument_value(args, "samples")
    if manual not in (None, []) and list(manual) != samples:
        raise RuntimeError(
            "full234 samples are the exact sorted samples_delegate52 inventory"
        )
    args.samples = samples
    return dict(cached)


def _load_remaining134_scope():
    """Freeze all samples outside the prior planned confirmation100 scope."""
    all_samples, _full_scope = _load_full234_scope()
    selection_path = Path(REMAINING134_SELECTION_PATH)
    if (not selection_path.is_file()
            or _sha256(selection_path) != REMAINING134_SELECTION_SHA256):
        raise RuntimeError(
            "remaining134 exclusion selection is missing or changed"
        )
    selection = _read_json(selection_path)
    excluded = selection.get("selected_sample_ids") or []
    if (selection.get("schema") != MIXED_CONFIRMATION_SELECTION_SCHEMA
            or selection.get("experiment_id")
            != MIXED_CONFIRMATION_EXPERIMENT_ID
            or len(excluded) != REMAINING134_EXCLUDED_SAMPLE_COUNT
            or len(set(excluded)) != REMAINING134_EXCLUDED_SAMPLE_COUNT
            or not set(excluded).issubset(all_samples)):
        raise RuntimeError(
            "remaining134 exclusion selection is not the frozen planned100"
        )
    samples = sorted(set(all_samples) - set(excluded))
    if len(samples) != REMAINING134_SAMPLE_COUNT:
        raise RuntimeError(
            "remaining134 complement does not contain exactly 134 samples"
        )
    sample_hashes = {
        sample: _sha256(Path(SAMPLES_ROOT, sample, "sample.json"))
        for sample in samples
    }
    excluded_digest = hashlib.sha256(
        _canonical_json_bytes(sorted(excluded))).hexdigest()
    return samples, {
        "schema": "anchorpatch.remaining134_scope/1",
        "sample_count": len(samples),
        "sample_ids": samples,
        "sample_json_sha256": hashlib.sha256(
            _canonical_json_bytes(sample_hashes)).hexdigest(),
        "excluded_sample_count": len(excluded),
        "excluded_sample_ids_sha256": excluded_digest,
        "exclusion_source": {
            "experiment_id": MIXED_CONFIRMATION_EXPERIMENT_ID,
            "path": os.path.relpath(selection_path, _ROOT).replace("\\", "/"),
            "sha256": REMAINING134_SELECTION_SHA256,
        },
        "method_phases": list(REMAINING134_METHOD_PHASES),
    }


def _resolve_remaining134_scope(args):
    if _argument_value(args, "campaign_role") != "remaining134":
        return None
    cached = _argument_value(args, "_remaining134_scope_record")
    if cached is None:
        samples, cached = _load_remaining134_scope()
        args._remaining134_scope_record = cached
    else:
        samples = list(cached.get("sample_ids") or [])
    manual = _argument_value(args, "samples")
    if manual not in (None, []) and list(manual) != samples:
        raise RuntimeError(
            "remaining134 samples are the exact complement of the frozen "
            "confirmation100 selection"
        )
    args.samples = samples
    return dict(cached)


def _validate_mixed_confirmation_selection_payload(
        payload, available_samples=None):
    rule = payload.get("candidate_rule")
    if not isinstance(rule, dict):
        raise RuntimeError("selection manifest candidate_rule is missing")
    expected_rule = {
        "seed": 42,
        "rule": "authorized_mixed_60_method_unseen_plus_40_historical",
        "candidate_count": MIXED_CONFIRMATION_CANDIDATE_COUNT,
        "selected_count": MIXED_CONFIRMATION_SAMPLE_COUNT,
        "reserve_count": MIXED_CONFIRMATION_RESERVE_COUNT,
        "method_unseen_candidate_count": 81,
        "method_unseen_selected_count": 60,
        "historical_candidate_count": 109,
        "historical_selected_count": 40,
    }
    if any(rule.get(key) != value for key, value in expected_rule.items()):
        raise RuntimeError("mixed confirmation candidate rule is not frozen")
    lists = {}
    for field, expected_count in (
            ("candidate_sample_ids", MIXED_CONFIRMATION_CANDIDATE_COUNT),
            ("selected_sample_ids", MIXED_CONFIRMATION_SAMPLE_COUNT),
            ("reserve_sample_ids", MIXED_CONFIRMATION_RESERVE_COUNT)):
        values = payload.get(field)
        if (not isinstance(values, list)
                or len(values) != expected_count
                or values != sorted(values)
                or len(set(values)) != expected_count
                or any(not isinstance(value, str) or not value
                       for value in values)):
            raise RuntimeError(
                f"mixed selection {field} must contain {expected_count} "
                "sorted unique sample IDs")
        lists[field] = list(values)
    candidates = set(lists["candidate_sample_ids"])
    selected = set(lists["selected_sample_ids"])
    reserve = set(lists["reserve_sample_ids"])
    if selected & reserve or selected | reserve != candidates:
        raise RuntimeError("mixed selection selected/reserve partition is invalid")
    if available_samples is not None and not candidates <= set(available_samples):
        raise RuntimeError(
            "mixed selection contains a sample outside samples_delegate52")

    cohorts = payload.get("cohorts")
    if not isinstance(cohorts, dict):
        raise RuntimeError("mixed selection cohorts are missing")
    unseen = cohorts.get("method_unseen")
    historical = cohorts.get("historical_hp_method_exposed")
    if not isinstance(unseen, dict) or not isinstance(historical, dict):
        raise RuntimeError("mixed selection cohort objects are missing")
    unseen_candidates = unseen.get("candidate_sample_ids")
    unseen_selected = unseen.get("selected_sample_ids")
    historical_candidates = historical.get("candidate_sample_ids")
    historical_selected = historical.get("selected_sample_ids")
    mandatory = historical.get("mandatory_actual_hp_api_sample_ids")
    if (not isinstance(unseen_candidates, list)
            or len(unseen_candidates) != 81
            or len(set(unseen_candidates)) != 81
            or unseen_candidates != sorted(unseen_candidates)
            or not isinstance(unseen_selected, list)
            or len(unseen_selected) != 60
            or unseen_selected != sorted(unseen_selected)
            or not set(unseen_selected) <= set(unseen_candidates)
            or not isinstance(historical_candidates, list)
            or len(historical_candidates) != 109
            or len(set(historical_candidates)) != 109
            or historical_candidates != sorted(historical_candidates)
            or not isinstance(historical_selected, list)
            or len(historical_selected) != 40
            or historical_selected != sorted(historical_selected)
            or not set(historical_selected) <= set(historical_candidates)
            or set(unseen_candidates) & set(historical_candidates)
            or set(unseen_candidates) | set(historical_candidates) != candidates
            or set(unseen_selected) | set(historical_selected) != selected
            or not isinstance(mandatory, list)
            or len(mandatory) != 17
            or mandatory != sorted(mandatory)
            or not set(mandatory) <= set(historical_selected)):
        raise RuntimeError("mixed selection cohort partition is invalid")

    audit = payload.get("exposure_audit")
    strict = (audit or {}).get("strict_any_provider_call")
    recent = set((audit or {}).get("recent_campaign_sample_ids") or [])
    if (not isinstance(audit, dict)
            or not isinstance(strict, dict)
            or strict.get("sample_count") != 234
            or audit.get("method_unseen_candidate_count") != 81
            or audit.get(
                "method_exposed_candidate_count_after_recent_exclusion") != 109
            or audit.get("actual_hp_api_historical_mandatory_count") != 17
            or set((audit.get("source_level_hp_exposure") or {}).keys())
                != {"json1", "molecule1", "obj3d1", "starcatalog1"}
            or len(recent) != 44
            or selected & recent):
        raise RuntimeError("mixed selection exposure audit is invalid")
    runtime = payload.get("runtime_evaluator_smoke")
    if (not isinstance(runtime, dict)
            or runtime.get("checked_count") != 234
            or runtime.get("runnable_count") != 234
            or runtime.get("failed_count") != 0):
        raise RuntimeError(
            "mixed selection requires a passing 234/234 runtime smoke")
    if payload.get("experiment_id") != MIXED_CONFIRMATION_EXPERIMENT_ID:
        raise RuntimeError("mixed selection experiment_id is invalid")

    inputs = payload.get("inputs")
    if not isinstance(inputs, dict) or inputs.get("sample_count") != 234:
        raise RuntimeError("mixed selection inputs are invalid")
    expected_paths = {
        "selector_script": "tools/build_mixed_confirmation_split.py",
        "base_selector_script": "tools/build_unseen_confirmation_split.py",
        "registry": "data/CONTAMINATION_REGISTRY.json",
        "hybrid_split": "data/hybrid_split.json",
    }
    normalized_inputs = {}
    for name, expected_path in expected_paths.items():
        entry = inputs.get(name)
        if (not isinstance(entry, dict)
                or entry.get("path") != expected_path
                or not re.fullmatch(r"[0-9a-f]{64}", str(entry.get("sha256")))):
            raise RuntimeError(f"mixed selection {name} input digest is invalid")
        normalized_inputs[name] = {
            "path": expected_path,
            "sha256": entry["sha256"],
        }
    recent_inputs = inputs.get("recent_campaign_manifests")
    if (not isinstance(recent_inputs, dict) or len(recent_inputs) != 2):
        raise RuntimeError("mixed selection recent manifest inputs are invalid")
    for relative, entry in recent_inputs.items():
        if (not isinstance(entry, dict)
                or entry.get("path") != f"{relative}/dispatch_manifest.json"
                or not re.fullmatch(r"[0-9a-f]{64}", str(entry.get("sha256")))):
            raise RuntimeError("mixed selection recent manifest digest is invalid")
    content_digest = inputs.get("sample_content_manifest_sha256")
    if not re.fullmatch(r"[0-9a-f]{64}", str(content_digest)):
        raise RuntimeError("mixed selection sample content digest is invalid")
    normalized_inputs["recent_campaign_manifests"] = copy.deepcopy(recent_inputs)
    normalized_inputs["sample_content_manifest"] = {
        "path": "data/samples_delegate52",
        "sample_count": 234,
        "sha256": content_digest,
    }

    features = payload.get("features")
    if (not isinstance(features, dict)
            or set(features) != candidates
            or any(not isinstance(value, dict) for value in features.values())):
        raise RuntimeError("mixed selection features do not cover candidates")
    preview = payload.get("artifact_sha256_preview")
    expected_preview = hashlib.sha256(_canonical_json_bytes({
        "candidate_sample_ids": lists["candidate_sample_ids"],
        "selected_sample_ids": lists["selected_sample_ids"],
        "reserve_sample_ids": lists["reserve_sample_ids"],
        "cohorts": cohorts,
        "features": features,
    })).hexdigest()
    if preview != expected_preview:
        raise RuntimeError("mixed selection canonical digest mismatch")
    return {
        "selection_kind": "mixed100",
        "experiment_id": payload["experiment_id"],
        "selected_sample_ids": lists["selected_sample_ids"],
        "reserve_sample_ids": lists["reserve_sample_ids"],
        "reserve_selection_order": lists["reserve_sample_ids"],
        "candidate_sample_ids": lists["candidate_sample_ids"],
        "artifact_sha256_preview": preview,
        "inputs": normalized_inputs,
        "cohorts": {
            "method_unseen": list(unseen_selected),
            "historical_hp_method_exposed": list(historical_selected),
        },
    }


def _validate_confirmation_selection_payload(payload, available_samples=None):
    """Validate the committed zero-API selection artifact and its digest."""
    if not isinstance(payload, dict):
        raise RuntimeError("selection manifest must be a JSON object")
    if payload.get("schema") == MIXED_CONFIRMATION_SELECTION_SCHEMA:
        return _validate_mixed_confirmation_selection_payload(
            payload, available_samples=available_samples)
    if payload.get("schema") != CONFIRMATION_SELECTION_SCHEMA:
        raise RuntimeError("selection manifest schema is not supported")
    rule = payload.get("candidate_rule")
    if not isinstance(rule, dict):
        raise RuntimeError("selection manifest candidate_rule is missing")
    expected_rule = {
        "seed": 42,
        "candidate_count": CONFIRMATION_CANDIDATE_COUNT,
        "selected_count": CONFIRMATION_SAMPLE_COUNT,
        "reserve_count": CONFIRMATION_RESERVE_COUNT,
    }
    if any(rule.get(key) != value for key, value in expected_rule.items()):
        raise RuntimeError(
            "selection manifest must freeze seed42 with selected68/reserve17"
        )
    if rule.get("rule") != "candidate_count_below100_select_about80_percent":
        raise RuntimeError("selection manifest candidate rule is not frozen")

    lists = {}
    for field, expected_count in (
            ("candidate_sample_ids", CONFIRMATION_CANDIDATE_COUNT),
            ("selected_sample_ids", CONFIRMATION_SAMPLE_COUNT),
            ("reserve_sample_ids", CONFIRMATION_RESERVE_COUNT)):
        values = payload.get(field)
        if (not isinstance(values, list)
                or len(values) != expected_count
                or any(not isinstance(value, str) or not value
                       for value in values)
                or len(set(values)) != expected_count
                or values != sorted(values)):
            raise RuntimeError(
                f"selection manifest {field} is not a sorted unique list "
                f"of {expected_count} samples"
            )
        lists[field] = list(values)
    selected = set(lists["selected_sample_ids"])
    reserve = set(lists["reserve_sample_ids"])
    candidates = set(lists["candidate_sample_ids"])
    if selected & reserve or selected | reserve != candidates:
        raise RuntimeError(
            "selection manifest selected/reserve partition is invalid"
        )
    reserve_order = payload.get("reserve_selection_order")
    if (not isinstance(reserve_order, list)
            or len(reserve_order) != CONFIRMATION_RESERVE_COUNT
            or any(not isinstance(value, str) or not value
                   for value in reserve_order)
            or set(reserve_order) != reserve):
        raise RuntimeError(
            "selection manifest reserve_selection_order must exactly cover reserve"
        )
    if available_samples is not None and not candidates <= set(
            available_samples):
        raise RuntimeError(
            "selection manifest contains a sample outside samples_delegate52"
        )

    exposure = payload.get("exposure_policy")
    if not isinstance(exposure, dict):
        raise RuntimeError("selection manifest exposure_policy is missing")
    strict = exposure.get("strict_any_provider_call")
    method_policy = exposure.get("method_developer_unseen")
    if (not isinstance(strict, dict)
            or strict.get("sample_count") != 234
            or rule.get("strict_unseen_candidate_count") != 0):
        raise RuntimeError(
            "selection manifest strict exposure audit must show 234 exposed, 0 unseen"
        )
    if (not isinstance(method_policy, dict)
            or method_policy.get("excluded_sample_count") != 149
            or method_policy.get("registry_clean_candidate_count") != 86
            or method_policy.get("split_test_count") != 20
            or method_policy.get(
                "clean_candidate_with_method_exposure_count") != 1
            or method_policy.get(
                "clean_candidate_with_method_exposure_ids") != ["python1"]
            or set((method_policy.get(
                "documented_developer_content_exposure") or {}).keys())
                != CONFIRMATION_DOCUMENTED_DEVELOPER_EXPOSURE):
        raise RuntimeError(
            "selection manifest method exposure audit must freeze the clean85 "
            "holdout and exclude all 149 exposed samples"
        )

    runtime = payload.get("runtime_evaluator_smoke")
    if (not isinstance(runtime, dict)
            or runtime.get("checked_count") != 234
            or runtime.get("runnable_count") != 234
            or runtime.get("failed_count") != 0):
        raise RuntimeError(
            "selection manifest must include a passing 234/234 runtime smoke"
        )

    experiment_id = payload.get("experiment_id")
    if experiment_id != CONFIRMATION_EXPERIMENT_ID:
        raise RuntimeError("selection manifest experiment_id is invalid")
    inputs = payload.get("inputs")
    if not isinstance(inputs, dict):
        raise RuntimeError("selection manifest inputs are missing")
    normalized_inputs = {}
    expected_paths = {
        "selector_script": "tools/build_unseen_confirmation_split.py",
        "registry": "data/CONTAMINATION_REGISTRY.json",
        "hybrid_split": "data/hybrid_split.json",
    }
    for name, expected_path in expected_paths.items():
        entry = inputs.get(name)
        if (not isinstance(entry, dict)
                or entry.get("path") != expected_path
                or not isinstance(entry.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])):
            raise RuntimeError(
                f"selection manifest {name} input digest is invalid"
            )
        normalized_inputs[name] = {
            "path": expected_path,
            "sha256": entry["sha256"],
        }
    sample_digest = inputs.get("sample_json_manifest_sha256")
    if (inputs.get("samples_root") != "data/samples_delegate52"
            or inputs.get("sample_count") != 234
            or not isinstance(sample_digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", sample_digest)):
        raise RuntimeError(
            "selection manifest sample input digest is invalid"
        )
    normalized_inputs["sample_json_manifest"] = {
        "path": inputs["samples_root"],
        "sample_count": inputs["sample_count"],
        "sha256": sample_digest,
    }

    features = payload.get("features")
    if (not isinstance(features, dict)
            or set(features) != candidates
            or any(not isinstance(value, dict) for value in features.values())):
        raise RuntimeError(
            "selection manifest features do not cover the candidate partition"
        )
    preview = payload.get("artifact_sha256_preview")
    expected_preview = hashlib.sha256(_canonical_json_bytes({
        "candidate_sample_ids": lists["candidate_sample_ids"],
        "selected_sample_ids": lists["selected_sample_ids"],
        "reserve_sample_ids": lists["reserve_sample_ids"],
        "features": features,
    })).hexdigest()
    if (not isinstance(preview, str)
            or not re.fullmatch(r"[0-9a-f]{64}", preview)
            or preview != expected_preview):
        raise RuntimeError("selection manifest canonical digest mismatch")
    return {
        "experiment_id": experiment_id,
        "selected_sample_ids": lists["selected_sample_ids"],
        "reserve_sample_ids": lists["reserve_sample_ids"],
        "reserve_selection_order": list(reserve_order),
        "candidate_sample_ids": lists["candidate_sample_ids"],
        "artifact_sha256_preview": preview,
        "inputs": normalized_inputs,
    }


def _validate_confirmation_candidate_semantics(validated, registry, hybrid_split):
    """Bind a byte-valid selection to the frozen registry and sealed splits."""
    clean_candidates = {
        item.get("sample_id")
        for item in registry.get("entries", [])
        if item.get("status") == "clean_candidate"
    }
    sealed_exposure = set()
    for name in ("dev", "val", "test", "unused_reserve"):
        values = (hybrid_split.get("splits") or {}).get(name, [])
        if not isinstance(values, list):
            raise RuntimeError(f"hybrid split {name} is not a list")
        sealed_exposure.update(values)
    candidate_set = set(validated["candidate_sample_ids"])
    expected_candidates = (
        clean_candidates - CONFIRMATION_DOCUMENTED_DEVELOPER_EXPOSURE
    )
    if candidate_set != expected_candidates:
        raise RuntimeError(
            "selection candidates do not exactly match registry clean_candidate "
            "minus documented developer exposure"
        )
    if candidate_set & sealed_exposure:
        raise RuntimeError(
            "selection candidates overlap a sealed dev/val/test/reserve split"
        )


def _load_verified_confirmation_selector(repo_root, entry):
    """Import the committed selector only after its path and digest are verified."""
    path = repo_root / entry["path"]
    if (not path.is_file() or _sha256(path) != entry["sha256"]):
        raise RuntimeError("selection selector script bytes are not verified")
    module_name = (
        "_anchorpatch_confirmation_selector_"
        + entry["sha256"][:16]
    )
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("selection selector script cannot be imported")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _require_equal_selection_field(field, actual, expected):
    if actual != expected:
        raise RuntimeError(
            f"selection manifest recompute mismatch: {field}")


def _current_experiment_exclusion(
        repo_root, current_experiment_dir, experiment_id):
    if current_experiment_dir is None:
        return None
    current = Path(current_experiment_dir).resolve()
    if current.name != experiment_id:
        raise RuntimeError(
            "current confirmation out_dir does not match selection experiment_id"
        )
    try:
        current.relative_to(repo_root)
    except ValueError:
        return None
    return current


@contextlib.contextmanager
def _selector_excluding_current_experiment(selector, excluded_dir):
    if excluded_dir is None:
        yield
        return
    original_walk = getattr(selector, "_walk_experiment_paths", None)
    if not callable(original_walk):
        raise RuntimeError(
            "selection selector cannot exclude current experiment evidence"
        )

    def filtered_walk(repo):
        for path in original_walk(repo):
            try:
                path.resolve().relative_to(excluded_dir)
            except ValueError:
                yield path

    selector._walk_experiment_paths = filtered_walk
    try:
        yield
    finally:
        selector._walk_experiment_paths = original_walk


def _validate_confirmation_recomputed_selection(
        payload, validated, repo_root, selector, registry, hybrid_split,
        current_experiment_dir=None):
    """Recompute the committed selector facts without trusting artifact fields."""
    excluded_dir = _current_experiment_exclusion(
        repo_root, current_experiment_dir,
        validated.get("experiment_id") or payload.get("experiment_id"))
    samples_root = repo_root / "data" / "samples_delegate52"
    with _selector_excluding_current_experiment(selector, excluded_dir):
        all_ids = selector.sample_ids(samples_root)
        all_set = set(all_ids)
        any_provider = selector.scan_any_provider_exposure(repo_root, all_set)
        actual_method = selector.scan_method_exposure(repo_root, all_set)
    policy = payload.get("exposure_policy") or {}
    strict = policy.get("strict_any_provider_call") or {}
    method_policy = policy.get("method_developer_unseen") or {}
    for field in ("sample_ids", "sample_count", "evidence_manifest_sha256"):
        _require_equal_selection_field(
            f"strict_any_provider_call.{field}",
            strict.get(field), any_provider.get(field))
    for field in ("api_call_files", "api_raw_request_files"):
        _require_equal_selection_field(
            f"strict_any_provider_call.{field}",
            strict.get(field), any_provider.get(field))

    registry_sets = selector.load_registry_sets(registry)
    split = selector.split_sets(hybrid_split)
    method_holdout_pool, policy_method_exposed, clean_with_method_exposure = (
        selector.method_holdout_candidates(
            all_set, registry_sets, split, actual_method["sample_ids"])
    )
    method_expected = {
        "actual_method_scan_count": actual_method["sample_count"],
        "actual_method_scan_sample_ids": actual_method["sample_ids"],
        "actual_method_path_manifest_sha256": actual_method[
            "path_manifest_sha256"],
        "clean_candidate_with_method_exposure_count": len(
            clean_with_method_exposure),
        "clean_candidate_with_method_exposure_ids": sorted(
            clean_with_method_exposure),
        "excluded_sample_count": len(policy_method_exposed),
        "excluded_sample_ids": sorted(policy_method_exposed),
    }
    for field, expected in method_expected.items():
        _require_equal_selection_field(
            f"method_developer_unseen.{field}",
            method_policy.get(field), expected)

    runtime = payload.get("runtime_evaluator_smoke") or {}
    runnable = runtime.get("runnable_sample_ids")
    if runnable is None:
        runnable = all_ids
    _require_equal_selection_field(
        "runtime_evaluator_smoke.runnable_sample_ids",
        sorted(runnable), all_ids)
    runnable_candidates = sorted(method_holdout_pool & set(runnable))
    features = {
        sample_id: selector.sample_feature(repo_root, sample_id)
        for sample_id in runnable_candidates
    }
    selector.add_length_bins(features)
    selected_n, reserve_n, count_rule = selector.selection_counts(
        len(runnable_candidates))
    reserve, reserve_order, _tie_rank = selector.stratified_reserve(
        runnable_candidates, features, reserve_n, 42)
    selected = sorted(set(runnable_candidates) - set(reserve))
    if len(selected) != selected_n:
        raise RuntimeError("selection recompute produced invalid counts")
    candidate_rule = payload.get("candidate_rule") or {}
    recomputed_rule = {
        "seed": 42,
        "rule": count_rule,
        "candidate_count": len(runnable_candidates),
        "selected_count": len(selected),
        "reserve_count": len(reserve),
        "strict_unseen_candidate_count": len(
            all_set - set(any_provider["sample_ids"])),
    }
    for field, expected in recomputed_rule.items():
        _require_equal_selection_field(
            f"candidate_rule.{field}", candidate_rule.get(field), expected)
    _require_equal_selection_field(
        "candidate_sample_ids", payload.get("candidate_sample_ids"),
        runnable_candidates)
    _require_equal_selection_field(
        "selected_sample_ids", validated["selected_sample_ids"], selected)
    _require_equal_selection_field(
        "reserve_sample_ids", validated["reserve_sample_ids"], reserve)
    _require_equal_selection_field(
        "reserve_selection_order", validated["reserve_selection_order"],
        reserve_order)
    _require_equal_selection_field(
        "features", payload.get("features"), features)
    return {
        "excluded_current_experiment_dir": (
            excluded_dir.relative_to(repo_root).as_posix()
            if excluded_dir is not None else None
        ),
    }


def _validate_mixed_confirmation_recomputed_selection(
        payload, validated, repo_root, selector,
        current_experiment_dir=None):
    excluded_dir = _current_experiment_exclusion(
        repo_root, current_experiment_dir, validated["experiment_id"])
    recomputed = selector.build_split(
        repo_root,
        skip_runtime_smoke=True,
        exclude_experiment_dir=excluded_dir,
    )
    for field in (
            "candidate_rule", "candidate_sample_ids", "selected_sample_ids",
            "reserve_sample_ids", "cohorts", "features",
            "artifact_sha256_preview"):
        _require_equal_selection_field(
            field, payload.get(field), recomputed.get(field))
    expected_audit = recomputed.get("exposure_audit") or {}
    actual_audit = payload.get("exposure_audit") or {}
    for field in (
            "strict_any_provider_call", "base_method_unseen_count",
            "source_level_hp_exposure", "method_unseen_candidate_count",
            "method_exposed_candidate_count_after_recent_exclusion",
            "actual_hp_api_historical_mandatory_count",
            "actual_hp_api_historical_mandatory_ids",
            "recent_campaign_sample_count", "recent_campaign_sample_ids",
            "recent_campaign_samples", "actual_method_scan_count",
            "actual_method_scan_sample_ids",
            "actual_method_path_manifest_sha256"):
        _require_equal_selection_field(
            f"exposure_audit.{field}", actual_audit.get(field),
            expected_audit.get(field))
    for field in (
            "base_selector_script", "registry", "hybrid_split",
            "recent_campaign_manifests", "sample_count",
            "sample_content_manifest_sha256"):
        _require_equal_selection_field(
            f"inputs.{field}", (payload.get("inputs") or {}).get(field),
            (recomputed.get("inputs") or {}).get(field))
    return {
        "excluded_current_experiment_dir": (
            excluded_dir.relative_to(repo_root).as_posix()
            if excluded_dir is not None else None
        ),
        "recomputed_selected_count": len(
            recomputed["selected_sample_ids"]),
    }


def _require_unique_key_values_for_queued_campaign(keys, selected_labels):
    """Fail closed if two queued-campaign labels map to one secret value."""
    digests = {}
    for label in selected_labels:
        value = keys[label]
        if not isinstance(value, str) or not value:
            raise RuntimeError("campaign key value is empty or invalid")
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        if digest in digests:
            raise RuntimeError(
                "campaign key labels must map to physically unique key values"
            )
        digests[digest] = label


def _resolve_selection_path(path_value):
    if not isinstance(path_value, (str, os.PathLike)) or not str(path_value):
        raise RuntimeError("confirmation requires --selection_manifest")
    raw = Path(path_value)
    if raw.is_absolute():
        candidates = [raw]
    else:
        candidates = [Path.cwd() / raw, Path(_ROOT) / raw,
                      Path(_ROOT).parent / raw]
    existing = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file() and resolved not in existing:
            existing.append(resolved)
    if len(existing) != 1:
        raise RuntimeError(
            "selection manifest path is missing or ambiguous"
        )
    repo_root = Path(_ROOT).parent.resolve()
    try:
        existing[0].relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(
            "selection manifest must be inside the repository"
        ) from exc
    return existing[0], repo_root


def _load_confirmation_selection(path_value, current_experiment_dir=None):
    """Load an exact committed selection artifact and return dispatch metadata."""
    path, repo_root = _resolve_selection_path(path_value)
    relative = path.relative_to(repo_root).as_posix()
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=repo_root, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        check=False,
    )
    if tracked.returncode != 0:
        raise RuntimeError(
            "selection manifest must be committed before confirmation"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"], cwd=repo_root,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    file_bytes = path.read_bytes()
    if committed.returncode != 0 or committed.stdout != file_bytes:
        raise RuntimeError(
            "selection manifest bytes differ from the committed Git blob"
        )
    try:
        payload = json.loads(file_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("selection manifest is not valid UTF-8 JSON") from exc
    available = {
        child.name for child in Path(SAMPLES_ROOT).iterdir()
        if child.is_dir() and (child / "sample.json").is_file()
    }
    validated = _validate_confirmation_selection_payload(
        payload, available_samples=available)
    required_inputs = (
        ("selector_script", "base_selector_script", "registry", "hybrid_split")
        if validated.get("selection_kind") == "mixed100"
        else ("selector_script", "registry", "hybrid_split")
    )
    for name in required_inputs:
        entry = validated["inputs"][name]
        input_path = repo_root / entry["path"]
        if (not input_path.is_file()
                or _sha256(input_path) != entry["sha256"]):
            raise RuntimeError(
                f"selection manifest {name} bytes changed after selection"
            )
    selector = _load_verified_confirmation_selector(
        repo_root, validated["inputs"]["selector_script"])
    if validated.get("selection_kind") == "mixed100":
        for entry in validated["inputs"]["recent_campaign_manifests"].values():
            input_path = repo_root / entry["path"]
            if (not input_path.is_file()
                    or _sha256(input_path) != entry["sha256"]):
                raise RuntimeError(
                    "mixed selection recent campaign manifest changed")
        recompute_record = _validate_mixed_confirmation_recomputed_selection(
            payload, validated, repo_root, selector,
            current_experiment_dir=current_experiment_dir)
    else:
        registry = _read_json(
            repo_root / validated["inputs"]["registry"]["path"])
        hybrid_split = _read_json(
            repo_root / validated["inputs"]["hybrid_split"]["path"])
        _validate_confirmation_candidate_semantics(
            validated, registry, hybrid_split)
        sample_entry = validated["inputs"]["sample_json_manifest"]
        sample_files = {
            sample_id: _sha256(
                repo_root / sample_entry["path"] / sample_id / "sample.json")
            for sample_id in sorted(available)
        }
        current_sample_digest = hashlib.sha256(
            _canonical_json_bytes(sample_files)).hexdigest()
        if (len(sample_files) != sample_entry["sample_count"]
                or current_sample_digest != sample_entry["sha256"]):
            raise RuntimeError(
                "selection manifest sample JSON digest changed after selection")
        recompute_record = _validate_confirmation_recomputed_selection(
            payload, validated, repo_root, selector, registry, hybrid_split,
            current_experiment_dir=current_experiment_dir)
    record = {
        "experiment_id": validated["experiment_id"],
        "schema": payload["schema"],
        "path": relative,
        "sha256": hashlib.sha256(file_bytes).hexdigest(),
        "artifact_sha256_preview": validated["artifact_sha256_preview"],
        "candidate_count": len(validated["candidate_sample_ids"]),
        "selected_count": len(validated["selected_sample_ids"]),
        "reserve_count": len(validated["reserve_sample_ids"]),
        "reserve_selection_order": validated["reserve_selection_order"],
        "seed": payload["candidate_rule"]["seed"],
        "inputs": validated["inputs"],
        "recompute": recompute_record,
    }
    if validated.get("selection_kind") == "mixed100":
        record["cohorts"] = copy.deepcopy(validated["cohorts"])
    return validated["selected_sample_ids"], record


def _argument_value(args, name, default=None):
    return vars(args).get(name, default)


def _resolve_confirmation_selection(args, out_dir=None):
    """Make the committed selection the sole authority for confirmation files."""
    if _argument_value(args, "campaign_role") != "confirmation":
        if _argument_value(args, "selection_manifest"):
            raise RuntimeError(
                "--selection_manifest is only valid for confirmation"
            )
        return None
    cached = _argument_value(args, "_selection_manifest_record")
    selected = _argument_value(args, "_selection_samples")
    verified = (
        _argument_value(args, "_selection_manifest_verified_token")
        is _CONFIRMATION_SELECTION_VERIFIED_TOKEN
    )
    if not verified or cached is None or selected is None:
        selected, cached = _load_confirmation_selection(
            _argument_value(args, "selection_manifest"),
            current_experiment_dir=out_dir)
        args._selection_samples = list(selected)
        args._selection_manifest_record = dict(cached)
        args._selection_manifest_verified_token = (
            _CONFIRMATION_SELECTION_VERIFIED_TOKEN
        )
    manual = _argument_value(args, "samples")
    if manual not in (None, []) and list(manual) != list(selected):
        raise RuntimeError(
            "confirmation samples must come only from --selection_manifest"
        )
    args.samples = list(selected)
    if (out_dir is not None
            and cached.get("experiment_id")
            != os.path.basename(os.path.abspath(out_dir))):
        raise RuntimeError(
            "selection manifest experiment_id must match --out_dir basename"
        )
    return dict(cached)


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


def _latest_sample_outcomes(
        out_dir, expected_samples=None, *, method_phase=None):
    latest = {}
    latest_by_phase = {}
    expected = set(expected_samples or [])
    for index, record in enumerate(read_sample_outcomes(out_dir), 1):
        sample = record.get("sample")
        status = record.get("status")
        record_phase = record.get("method_phase")
        if (not isinstance(sample, str) or not sample
                or status not in {
                    "finished", "infrastructure_incomplete",
                    "evaluator_incomplete",
                }
                or (record_phase is not None
                    and (record_phase not in REMAINING134_METHOD_PHASES
                         or record.get("methods") != [record_phase]))):
            raise RuntimeError(f"invalid sample outcome row {index}")
        if (expected and sample not in expected
                and (method_phase is None or record_phase == method_phase)):
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
        key = (sample, record_phase)
        prior = latest_by_phase.get(key)
        if prior and prior.get("status") == "finished":
            raise RuntimeError(
                "sample outcome appears after finished state: "
                f"{sample}/{record_phase or 'legacy'}")
        latest_by_phase[key] = record
        if method_phase is None or record_phase == method_phase:
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


def _record_mentions_sample(record, sample):
    """Return whether an append-only campaign record names one sample."""
    if not isinstance(record, dict):
        return False
    if record.get("sample") == sample or record.get("sample_id") == sample:
        return True
    samples = record.get("samples")
    if isinstance(samples, list) and sample in samples:
        return True
    for field in (
            "semantic_call_id", "semantic_root_id",
            "parent_semantic_call_id"):
        value = record.get(field)
        if isinstance(value, str):
            parts = value.split("/")
            if len(parts) >= 2 and parts[1] == sample:
                return True
    return False


def _record_mentions_method(record, method):
    if not isinstance(record, dict):
        return False
    if record.get("method") == method or record.get("method_phase") == method:
        return True
    methods = record.get("methods")
    if isinstance(methods, list) and method in methods:
        return True
    for field in (
            "semantic_call_id", "semantic_root_id",
            "parent_semantic_call_id"):
        value = record.get(field)
        if isinstance(value, str) and value.split("/", 1)[0] == method:
            return True
    return False


def _queued_pending_evidence(
        out_dir, sample, methods, *, method_phase=None):
    """List durable evidence that a nominally pending worker ever started.

    Task plans and the dispatch manifest are intentionally absent: they are
    pre-created for the full queued scope before workers start.  Every
    execution-side artifact is fail-closed, including an empty result,
    checkpoint, raw-call directory, console log, or worker barrier.
    """
    evidence = []
    safe_sample = "".join(
        char if char.isalnum() or char in "._-" else "_"
        for char in sample
    )
    for method in methods:
        for suffix in (".jsonl", ".ckpt.json"):
            path = os.path.join(out_dir, method, f"{sample}{suffix}")
            if os.path.exists(path):
                evidence.append(os.path.relpath(path, out_dir))
        docs_path = os.path.join(out_dir, "docs", method, safe_sample)
        if os.path.exists(docs_path):
            evidence.append(os.path.relpath(docs_path, out_dir))
        raw_path = os.path.join(out_dir, "api_raw", method, safe_sample)
        if os.path.exists(raw_path):
            evidence.append(os.path.relpath(raw_path, out_dir))
        log_path = os.path.join(
            out_dir, "logs", f"{method}__{safe_sample}.log")
        if os.path.exists(log_path):
            evidence.append(os.path.relpath(log_path, out_dir))

    record_files = (
        "sample_outcomes.jsonl", "run_metadata.jsonl", "api_calls.jsonl",
        "api_anomalies.jsonl", "evaluator_incomplete_samples.jsonl",
    )
    for filename in record_files:
        path = os.path.join(out_dir, filename)
        for row_number, record in enumerate(_read_jsonl(path), 1):
            if (_record_mentions_sample(record, sample)
                    and (method_phase is None
                         or _record_mentions_method(record, method_phase))):
                evidence.append(f"{filename}:{row_number}")

    attempt_path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
    for row_number, record in enumerate(_read_jsonl(attempt_path), 1):
        identity = _parse_semantic_call_id(record.get("semantic_call_id"))
        if identity is None:
            raise RuntimeError(
                "queued pending audit cannot map attempt ledger row "
                f"{row_number}"
            )
        if (identity["sample"] == sample
                and (method_phase is None
                     or identity["method"] == method_phase)):
            evidence.append(f"api_attempt_ledger.jsonl:{row_number}")

    dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
    for row_number, record in enumerate(_read_jsonl(dispatch_path), 1):
        event = record.get("event")
        # queue_start/wave_start and resume_start describe queued intent only.
        # A durable per-worker launch_intent is appended before Popen and is
        # the earliest execution evidence.  queue_refill and historical
        # wave_complete both prove that a worker was launched.
        mentions = record.get("sample") == sample
        if event == "wave_complete":
            mentions = mentions or sample in (record.get("samples") or [])
        if event == "queue_refill":
            mentions = mentions or sample in (record.get("samples") or [])
        if event == "queue_complete":
            mentions = mentions or any(
                sample in (record.get(field) or [])
                for field in (
                    "completed_samples",
                    "infrastructure_incomplete_samples",
                    "evaluator_incomplete_samples",
                )
            )
        if event == "campaign_incomplete":
            mentions = mentions or any(
                sample in (record.get(field) or [])
                for field in (
                    "infrastructure_incomplete_samples",
                    "evaluator_incomplete_samples",
                    "completed_samples",
                )
            )
        phase_matches = (
            method_phase is None
            or record.get("method_phase") == method_phase
            or _record_mentions_method(record, method_phase)
        )
        if mentions and phase_matches:
            evidence.append(f"dispatch_log.jsonl:{row_number}")

    journal_dir = Path(out_dir, "api_journal")
    if journal_dir.is_dir():
        for path in sorted(journal_dir.glob("*.json")):
            payload = _read_json(path)
            semantic_call_id = (
                payload.get("semantic_call_id")
                if isinstance(payload, dict) else None
            )
            identity = _parse_semantic_call_id(semantic_call_id)
            expected_name = (
                hashlib.sha256(semantic_call_id.encode("utf-8"))
                .hexdigest()[:24] + ".response.json"
                if identity is not None else None
            )
            if (not isinstance(payload, dict)
                    or payload.get("schema")
                    != API_RESPONSE_JOURNAL_SCHEMA
                    or identity is None
                    or path.name != expected_name):
                raise RuntimeError(
                    "queued pending audit cannot map response journal: "
                    f"{path.name}"
                )
            if (identity["sample"] == sample
                    and (method_phase is None
                         or identity["method"] == method_phase)):
                evidence.append(os.path.relpath(path, out_dir))

    barrier_dir = Path(out_dir, "worker_barriers")
    if barrier_dir.is_dir():
        for path in sorted(barrier_dir.glob("*.json")):
            payload = _read_json(path)
            if (_record_mentions_sample(payload, sample)
                    and (method_phase is None
                         or _record_mentions_method(payload, method_phase))):
                evidence.append(os.path.relpath(path, out_dir))

    active_path = os.path.join(out_dir, "active_worker_set.json")
    if os.path.isfile(active_path):
        active = _read_json(active_path)
        workers = active.get("workers") if isinstance(active, dict) else None
        if (isinstance(workers, dict)
                and any(
                    isinstance(worker, dict)
                    and worker.get("sample") == sample
                    for worker in workers.values()
                )):
            evidence.append("active_worker_set.json")

    dispatch_dir = Path(out_dir, "dispatch_logs")
    if dispatch_dir.is_dir():
        evidence.extend(
            os.path.relpath(path, out_dir)
            for path in sorted(dispatch_dir.glob(
                f"{safe_sample}__*.console.log"))
            if (method_phase is None
                or f"__{method_phase}.console.log" in path.name)
        )
    if _worker_lease_is_held(out_dir, sample):
        evidence.append(os.path.relpath(
            _worker_lease_path(out_dir, sample), out_dir) + ":held")
    return sorted(set(evidence))


def _verify_queued_pending_samples(
        out_dir, assignments, *, method_phase=None):
    """Allow only samples proven never launched in a queued campaign."""
    for item in assignments:
        sample = item["sample"]
        evidence = _queued_pending_evidence(
            out_dir, sample, item.get("methods") or [],
            method_phase=method_phase)
        if evidence:
            raise RuntimeError(
                "queued resume cannot prove pending sample was never "
                f"started: {sample}; evidence={evidence}"
            )


def _verify_pristine_method_phase(out_dir, assignments, method_phase):
    """Prove a queued phase has no execution evidence in one bounded scan."""
    if method_phase not in REMAINING134_METHOD_PHASES:
        raise RuntimeError("invalid pristine method phase")
    samples = {item["sample"] for item in assignments}
    evidence = {sample: [] for sample in samples}
    for sample in samples:
        safe_sample = "".join(
            char if char.isalnum() or char in "._-" else "_"
            for char in sample
        )
        for suffix in (".jsonl", ".ckpt.json"):
            path = os.path.join(out_dir, method_phase, f"{sample}{suffix}")
            if os.path.exists(path):
                evidence[sample].append(os.path.relpath(path, out_dir))
        paths = (
            Path(out_dir, "docs", method_phase, safe_sample),
            Path(out_dir, "api_raw", method_phase, safe_sample),
            Path(out_dir, "logs", f"{method_phase}__{safe_sample}.log"),
        )
        for path in paths:
            if path.exists():
                evidence[sample].append(os.path.relpath(path, out_dir))
        dispatch_dir = Path(out_dir, "dispatch_logs")
        if dispatch_dir.is_dir():
            for path in dispatch_dir.glob(
                    f"{safe_sample}__*__{method_phase}.console.log"):
                evidence[sample].append(os.path.relpath(path, out_dir))

    for filename in (
            "sample_outcomes.jsonl", "run_metadata.jsonl", "api_calls.jsonl",
            "api_anomalies.jsonl", "evaluator_incomplete_samples.jsonl"):
        for row_number, record in enumerate(
                _read_jsonl(os.path.join(out_dir, filename)), 1):
            if not _record_mentions_method(record, method_phase):
                continue
            for sample in samples:
                if _record_mentions_sample(record, sample):
                    evidence[sample].append(f"{filename}:{row_number}")

    for row_number, record in enumerate(_read_jsonl(os.path.join(
            out_dir, "api_attempt_ledger.jsonl")), 1):
        identity = _parse_semantic_call_id(record.get("semantic_call_id"))
        if identity is None:
            raise RuntimeError(
                f"phase pending audit cannot map attempt row {row_number}")
        if (identity["method"] == method_phase
                and identity["sample"] in samples):
            evidence[identity["sample"]].append(
                f"api_attempt_ledger.jsonl:{row_number}")

    journal_dir = Path(out_dir, "api_journal")
    if journal_dir.is_dir():
        for path in sorted(journal_dir.glob("*.json")):
            payload = _read_json(path)
            semantic_call_id = (
                payload.get("semantic_call_id")
                if isinstance(payload, dict) else None
            )
            identity = _parse_semantic_call_id(semantic_call_id)
            expected_name = (
                hashlib.sha256(semantic_call_id.encode("utf-8"))
                .hexdigest()[:24] + ".response.json"
                if identity is not None else None
            )
            if (not isinstance(payload, dict)
                    or payload.get("schema") != API_RESPONSE_JOURNAL_SCHEMA
                    or identity is None
                    or path.name != expected_name):
                raise RuntimeError(
                    "phase pending audit cannot map response journal: "
                    f"{path.name}")
            if (identity["method"] == method_phase
                    and identity["sample"] in samples):
                evidence[identity["sample"]].append(
                    os.path.relpath(path, out_dir))

    for row_number, record in enumerate(_read_jsonl(os.path.join(
            out_dir, "dispatch_log.jsonl")), 1):
        if (record.get("method_phase") != method_phase
                and not _record_mentions_method(record, method_phase)):
            continue
        mentioned = set()
        sample = record.get("sample")
        if sample in samples:
            mentioned.add(sample)
        for field in (
                "samples", "completed_samples",
                "infrastructure_incomplete_samples",
                "evaluator_incomplete_samples"):
            mentioned.update(samples & set(record.get(field) or []))
        for sample in mentioned:
            evidence[sample].append(f"dispatch_log.jsonl:{row_number}")

    failures = {
        sample: sorted(set(items))
        for sample, items in evidence.items() if items
    }
    if failures:
        sample = sorted(failures)[0]
        raise RuntimeError(
            "queued resume cannot prove method phase was never started: "
            f"{method_phase}/{sample}; evidence={failures[sample]}"
        )


def _verified_interrupted_phase_resume(
        out_dir, item, target_round_trips, task_plan):
    """Authorize a stopped worker with an atomic committed RT prefix."""
    sample = item.get("sample")
    method_phase = item.get("method_phase")
    methods = item.get("methods")
    if (not isinstance(sample, str) or not sample
            or method_phase not in REMAINING134_METHOD_PHASES
            or methods != [method_phase]
            or not isinstance(task_plan, dict)
            or not isinstance(task_plan.get("sha256"), str)):
        return None
    recovery_incidents = campaign_recovery_incident_evidence(out_dir)
    candidates = [
        record for record in read_run_metadata_snapshot(out_dir)
        if record.get("samples") == [sample]
        and record.get("methods") == [method_phase]
        and record.get("method_phase") == method_phase
        and record.get("status") in {
            "running", "interrupted_before_audited_resume",
            "interrupted_by_dispatcher", "failed",
        }
    ]
    if not candidates:
        return None
    record = candidates[-1]
    invocation_id = record.get("invocation_id")
    worker_id = record.get("worker_launch_id")
    worker_pid = record.get("worker_pid")
    if (not isinstance(invocation_id, str) or not invocation_id
            or not isinstance(worker_id, str) or not worker_id
            or not _is_exact_int(worker_pid) or worker_pid <= 0):
        raise RuntimeError(
            f"interrupted phase metadata identity is invalid: {sample}")
    expected_registration = {
        "sha256": task_plan["sha256"],
        "round_trips": target_round_trips,
    }
    if (record.get("task_plans") or {}).get(sample) != expected_registration:
        raise RuntimeError(
            f"interrupted phase task plan is invalid: {sample}")
    if _worker_lease_is_held(out_dir, sample):
        raise RuntimeError(
            f"interrupted phase worker lease is still held: {sample}")

    dispatch_rows = _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
    intents = [
        row for row in dispatch_rows
        if row.get("event") == "launch_intent"
        and row.get("worker_launch_id") == worker_id
    ]
    launches = [
        row for row in dispatch_rows
        if row.get("event") == "launch"
        and row.get("worker_launch_id") == worker_id
    ]
    expected_identity = {
        "sample": sample,
        "methods": [method_phase],
        "method_phase": method_phase,
    }
    if (len(intents) != 1 or len(launches) != 1
            or any(intents[0].get(key) != value
                   for key, value in expected_identity.items())
            or launches[0].get("sample") != sample
            or launches[0].get("pid") != worker_pid
            or launches[0].get("method_phase") != method_phase):
        raise RuntimeError(
            f"interrupted phase launch provenance is invalid: {sample}")
    if record.get("status") == "running":
        audited = _audit_running_invocation_provenance(out_dir)
        if not any(
                row.get("invocation_id") == invocation_id
                and row.get("worker_launch_id") == worker_id
                and row.get("method_phase") == method_phase
                for row in audited):
            raise RuntimeError(
                f"running phase interruption was not audited: {sample}")
    elif record.get("status") == "interrupted_before_audited_resume":
        reconciled = [
            row for row in dispatch_rows
            if row.get("event") == "stale_worker_reconciled"
            and row.get("invocation_id") == invocation_id
            and row.get("worker_launch_id") == worker_id
            and row.get("sample") == sample
            and row.get("pid") == worker_pid
        ]
        if len(reconciled) != 1:
            raise RuntimeError(
                f"interrupted phase reconciliation is invalid: {sample}")
    else:
        if worker_id not in recovery_incidents["worker_launch_ids"]:
            raise RuntimeError(
                f"interrupted phase failure lacks recovery authorization: {sample}")
        exits = [
            row for row in dispatch_rows
            if row.get("event") == "worker_exit"
            and row.get("worker_launch_id") == worker_id
            and row.get("sample") == sample
            and row.get("pid") == worker_pid
            and row.get("disposition") == "campaign_fatal"
        ]
        if len(exits) != 1:
            raise RuntimeError(
                f"interrupted phase recovered exit is invalid: {sample}")
    progress = _actual_sample_progress(out_dir, sample, [method_phase])
    phase_progress = progress[method_phase]
    completed = phase_progress["completed_round_trips"]
    if (completed > target_round_trips
            or phase_progress["committed_rows"] != 2 * completed):
        raise RuntimeError(
            f"interrupted phase checkpoint exceeds target: {sample}")
    evidence = {
        "schema": "anchorpatch.interrupted_phase_resume/1",
        "sample": sample,
        "method_phase": method_phase,
        "prior_invocation_id": invocation_id,
        "prior_worker_launch_id": worker_id,
        "prior_worker_pid": worker_pid,
        "prior_status": record["status"],
        "checkpoint_progress": progress,
        "task_plan_sha256": task_plan["sha256"],
    }
    if recovery_incidents["authorization_id"] is not None:
        evidence["campaign_recovery_authorization_id"] = (
            recovery_incidents["authorization_id"])
    return evidence


def _verified_infrastructure_incomplete(out_dir, sample, item):
    """Return the exact failure evidence or fail closed.

    A non-zero worker exit is sample-local only when four append-only sources
    agree: sample outcome, run metadata, API call row, and attempt ledger.
    """
    if read_campaign_stop_conditions(out_dir):
        raise RuntimeError(
            "campaign-wide stop latch forbids sample-local isolation")
    method_phase = item.get("method_phase")
    latest = _latest_sample_outcomes(
        out_dir, method_phase=method_phase)
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
            or outcome.get("classification") != "provider/API failure"
            or outcome.get("method_phase") != method_phase):
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
    metadata_resume = metadata[0].get("transport_resume_authorization")
    if generation_index == 0 and metadata_resume is not None:
        consumed_prior_resume = (
            isinstance(metadata_resume, dict)
            and _resume_authorization_consumed_before(
                metadata_resume, api_rows, api_index, sample=sample,
                worker_launch_id=worker_id, out_dir=out_dir,
            )
        )
        if not consumed_prior_resume:
            raise RuntimeError(
                f"worker {sample} infrastructure resume metadata mismatch")
    elif generation_index > 0:
        try:
            _validate_resume_authorization(metadata_resume)
        except RuntimeError as exc:
            raise RuntimeError(
                f"worker {sample} infrastructure resume metadata mismatch"
            ) from exc
        if (metadata_resume.get("parent_semantic_call_id")
                != parent_semantic_call_id
                or metadata_resume.get("semantic_root_id") != semantic_root_id
                or metadata_resume.get("semantic_call_id") != semantic_call_id
                or metadata_resume.get("generation_index") != generation_index
                or metadata_resume.get("request_fingerprint")
                != request_fingerprint):
            raise RuntimeError(
                f"worker {sample} infrastructure resume metadata mismatch")
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
    recovery_incidents = campaign_recovery_incident_evidence(out_dir)
    provider_access_authorizations = recovery_incidents[
        "provider_access_retry_authorizations"
    ]
    provider_access_retry = next((
        authorization for authorization
        in provider_access_authorizations.values()
        if authorization.get("semantic_call_id") == semantic_call_id
        and authorization.get("sample") == sample
    ), None)
    exhausted = (
        response_slots == api_row.get("max_response_slots") == 2
        or transient_failures == api_row.get("max_transient_failures") == 3
        or (
            provider_access_retry is not None
            and outcome.get("error_type") == "provider_access_denied"
            and api_row.get("error_type") == "provider_access_denied"
            and isinstance(response_slots, int)
            and 0 <= response_slots < 2
            and transient_failures == 0
        )
    )
    if not exhausted:
        raise RuntimeError(
            f"worker {sample} exited before a transport budget was exhausted")

    ledger = _read_jsonl(os.path.join(
        out_dir, "api_attempt_ledger.jsonl"))
    lineage = _validated_attempt_lineage(
        ledger, semantic_root_id,
        authorized_provider_access_parents=frozenset(
            authorization["parent_semantic_call_id"]
            for authorization in provider_access_authorizations.values()
        ),
    )
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
    if generation_index > 0:
        expected_metadata_resume = _canonical_resume_authorization({
            "parent_semantic_call_id": parent_semantic_call_id,
            "semantic_root_id": semantic_root_id,
            "semantic_call_id": semantic_call_id,
            "generation_index": generation_index,
            "request_fingerprint": request_fingerprint,
            "next_attempt_index": failed_state.get("first_attempt_index"),
        })
        if metadata_resume != expected_metadata_resume:
            raise RuntimeError(
                f"worker {sample} infrastructure resume metadata mismatch")
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


def _verified_evaluator_incomplete(out_dir, sample, item):
    """Verify a sample-local evaluator exception without accepting score rows."""
    if read_campaign_stop_conditions(out_dir):
        raise RuntimeError(
            "campaign-wide stop latch forbids sample-local isolation")
    method_phase = item.get("method_phase")
    latest = _latest_sample_outcomes(
        out_dir, method_phase=method_phase)
    outcome = latest.get(sample)
    if (not isinstance(outcome, dict)
            or outcome.get("status") != "evaluator_incomplete"):
        raise RuntimeError(
            f"worker {sample} failed without evaluator outcome")
    worker_id = item["worker_launch_id"]
    process = item.get("process")
    worker_pid = (
        process.pid if process is not None else item.get("worker_pid")
    )
    methods = item.get("methods") or []
    if (outcome.get("worker_launch_id") != worker_id
            or outcome.get("worker_pid") != worker_pid
            or outcome.get("methods") != methods
            or outcome.get("failure_stage") != "evaluator"
            or outcome.get("result_committed_for_failed_step") is not False
            or outcome.get("score_imputed") is not False
            or outcome.get("method_phase") != method_phase):
        raise RuntimeError(
            f"worker {sample} evaluator outcome provenance mismatch")
    if _worker_lease_is_held(out_dir, sample):
        raise RuntimeError(
            f"worker {sample} lease remains held after evaluator exit")

    invocation_id = outcome.get("invocation_id")
    failure_method = outcome.get("method")
    failure_rt = outcome.get("rt_index")
    direction = outcome.get("direction")
    target_round_trips = item.get("target_round_trips")
    if (not isinstance(invocation_id, str) or not invocation_id
            or failure_method not in methods
            or not _is_exact_int(failure_rt) or failure_rt < 1
            or direction not in {"forward", "backward"}
            or (_is_exact_int(target_round_trips)
                and failure_rt > target_round_trips)
            or not isinstance(outcome.get("error_type"), str)
            or not outcome.get("error_type")
            or not isinstance(outcome.get("error_message"), str)):
        raise RuntimeError(
            f"worker {sample} evaluator failed-step identity is invalid")

    actual_progress = _actual_sample_progress(out_dir, sample, methods)
    if outcome.get("checkpoint_progress") != actual_progress:
        raise RuntimeError(
            f"worker {sample} evaluator checkpoint evidence drift")
    if _is_exact_int(target_round_trips):
        failure_index = methods.index(failure_method)
        for index, method in enumerate(methods):
            expected_rt = (
                target_round_trips if index < failure_index
                else failure_rt - 1 if index == failure_index else 0
            )
            if actual_progress[method] != {
                    "completed_round_trips": expected_rt,
                    "committed_rows": 2 * expected_rt}:
                raise RuntimeError(
                    f"worker {sample} evaluator method-order/checkpoint mismatch")

    metadata = [
        record for record in read_run_metadata_snapshot(out_dir)
        if record.get("invocation_id") == invocation_id
    ]
    if (len(metadata) != 1
            or metadata[0].get("status") != "evaluator_incomplete"
            or metadata[0].get("worker_launch_id") != worker_id
            or metadata[0].get("worker_pid") != worker_pid
            or metadata[0].get("samples") != [sample]):
        raise RuntimeError(
            f"worker {sample} evaluator run metadata mismatch")

    sidecar_rows = _read_jsonl(os.path.join(
        out_dir, "evaluator_incomplete_samples.jsonl"))
    sidecar_matches = [
        (index, row) for index, row in enumerate(sidecar_rows, 1)
        if row.get("sample") == sample
        and row.get("invocation_id") == invocation_id
    ]
    if len(sidecar_matches) != 1:
        raise RuntimeError(
            f"worker {sample} evaluator sidecar evidence is not unique")
    sidecar_index, sidecar = sidecar_matches[0]
    mirrored_fields = (
        "created_at", "worker_launch_id", "worker_pid", "methods",
        "method", "rt_index", "direction", "target_state",
        "failure_stage", "error_type", "error_message",
        "checkpoint_progress", "result_committed_for_failed_step",
        "score_imputed",
    )
    if (sidecar.get("schema") != "anchorpatch.evaluator_incomplete/2"
            or sidecar.get("status") != "evaluator_incomplete"
            or sidecar.get("disposition")
            != "cancel_sample_continue_campaign"
            or any(sidecar.get(field) != outcome.get(field)
                   for field in mirrored_fields)):
        raise RuntimeError(
            f"worker {sample} evaluator sidecar provenance mismatch")

    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    step_rows = [
        (index, row) for index, row in enumerate(api_rows, 1)
        if row.get("sample") == sample
        and row.get("method") == failure_method
        and row.get("rt_index") == failure_rt
        and row.get("direction") == direction
        and row.get("worker_launch_id") == worker_id
    ]
    expected_primary = (
        "hybridpatch_primary"
        if failure_method == "hybridpatch" else "fullrewrite_primary"
    )
    if (not step_rows
            or sum(row.get("call_kind") == expected_primary
                   for _index, row in step_rows) != 1
            or step_rows[-1][1].get("classification")
            == "provider/API failure"):
        raise RuntimeError(
            f"worker {sample} evaluator model-response evidence is invalid")
    return {
        "sample_outcome_created_at": outcome.get("created_at"),
        "invocation_id": invocation_id,
        "method": failure_method,
        "rt_index": failure_rt,
        "direction": direction,
        "error_type": outcome.get("error_type"),
        "evaluator_sidecar_row": sidecar_index,
        "api_rows": [index for index, _row in step_rows],
        "checkpoint_progress": actual_progress,
        "result_committed_for_failed_step": False,
        "score_imputed": False,
    }


def _select_invocation_assignments(
        out_dir, assignments, *, resume, target_round_trips,
        allow_pristine_pending=False, method_phase=None,
        allow_audited_interrupted=False, task_plans=None):
    """Select all samples for a new campaign, only incomplete ones on resume."""
    if not resume:
        if read_sample_outcomes(out_dir):
            raise RuntimeError(
                "new campaign directory already contains sample outcomes")
        return list(assignments), {}
    recovery_incidents = campaign_recovery_incident_evidence(out_dir)
    provider_access_retries = recovery_incidents[
        "provider_access_retry_authorizations"
    ]
    latest = _latest_sample_outcomes(
        out_dir, [item["sample"] for item in assignments],
        method_phase=method_phase)
    # The authorization is bound to a newer failed worker than any stale
    # retry-exhaustion outcome retained for the same sample.
    for sample in recovery_incidents["provider_access_resume_samples"]:
        latest.pop(sample, None)
    missing_assignments = [
        item for item in assignments if item["sample"] not in latest
    ]
    missing = [item["sample"] for item in missing_assignments]
    interrupted_evidence = {}
    if missing and allow_audited_interrupted:
        for item in missing_assignments:
            sample = item["sample"]
            evidence = _verified_interrupted_phase_resume(
                out_dir, item, target_round_trips,
                (task_plans or {}).get(sample))
            if evidence is not None:
                interrupted_evidence[sample] = evidence
        missing_assignments = [
            item for item in missing_assignments
            if item["sample"] not in interrupted_evidence
        ]
        missing = [item["sample"] for item in missing_assignments]
    if missing:
        if not allow_pristine_pending:
            raise RuntimeError(
                f"resume is limited to explicitly incomplete samples; "
                f"missing outcomes: {missing}")
        if method_phase is None:
            _verify_queued_pending_samples(out_dir, missing_assignments)
        else:
            _verify_pristine_method_phase(
                out_dir, missing_assignments, method_phase)
    selected = []
    authorizations = {}
    for item in assignments:
        sample = item["sample"]
        if sample not in latest:
            selected_item = dict(item)
            if sample in interrupted_evidence:
                selected_item["interrupted_resume_evidence"] = (
                    interrupted_evidence[sample])
            selected.append(selected_item)
            if sample in provider_access_retries:
                authorizations[sample] = dict(
                    provider_access_retries[sample])
            continue
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
        if outcome["status"] == "evaluator_incomplete":
            _verified_evaluator_incomplete(
                out_dir, sample, {
                    "worker_launch_id": outcome.get("worker_launch_id"),
                    "worker_pid": outcome.get("worker_pid"),
                    "methods": item["methods"],
                    "target_round_trips": target_round_trips,
                    "method_phase": method_phase,
                })
            # An evaluator-broken sample is terminal for this campaign.  Its
            # missing endpoint stays null; resume must not issue another POST.
            continue
        evidence = _verified_infrastructure_incomplete(
            out_dir, sample, {
                "worker_launch_id": outcome.get("worker_launch_id"),
                "worker_pid": outcome.get("worker_pid"),
                "methods": item["methods"],
                "target_round_trips": target_round_trips,
                "method_phase": method_phase,
            })
        prior_generation = evidence.get("generation_index")
        if not _is_exact_int(prior_generation) or prior_generation < 0:
            raise RuntimeError(
                f"invalid semantic generation for incomplete sample: {sample}")
        if prior_generation >= 1:
            raise RuntimeError(
                "recovery generation limit exhausted; g002+ is forbidden: "
                f"{sample} generation={prior_generation}"
            )
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


def _method_phase_outcome_sets(out_dir, samples, method_phase):
    """Return the mutually exclusive terminal/retry states for one phase."""
    sample_set = set(samples)
    latest = _latest_sample_outcomes(
        out_dir, sample_set, method_phase=method_phase)
    states = {
        "finished": set(),
        "evaluator_incomplete": set(),
        "infrastructure_incomplete": set(),
        "missing": sample_set - set(latest),
    }
    for sample, outcome in latest.items():
        states[outcome["status"]].add(sample)
    covered = set().union(*states.values())
    if covered != sample_set or sum(len(items) for items in states.values()) != len(
            sample_set):
        raise RuntimeError(
            f"method phase outcome partition is invalid: {method_phase}")
    return states


def _sample_ids_sha256(samples):
    return hashlib.sha256(
        _canonical_json_bytes(sorted(samples))).hexdigest()


def _phase_api_call_count(out_dir, method_phase):
    return sum(
        record.get("method") == method_phase
        for record in _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    )


def _method_phase_complete_events(out_dir, method_phase):
    return [
        row for row in _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
        if row.get("event") == "method_phase_complete"
        and row.get("method_phase") == method_phase
    ]


def _record_method_phase_complete(
        out_dir, manifest, method_phase, eligible_assignments,
        *, next_phase=None, next_phase_assignments=None):
    """Verify one phase is terminal and durably publish its phase barrier."""
    samples = [item["sample"] for item in eligible_assignments]
    if len(samples) != len(set(samples)):
        raise RuntimeError("method phase contains duplicate sample assignments")
    states = _method_phase_outcome_sets(out_dir, samples, method_phase)
    if states["missing"] or states["infrastructure_incomplete"]:
        raise RuntimeError(
            f"method phase is not terminal: {method_phase}; "
            f"missing={sorted(states['missing'])}; "
            "infrastructure_incomplete="
            f"{sorted(states['infrastructure_incomplete'])}"
        )
    inspection = inspect_campaign(
        out_dir, manifest,
        required_complete_samples=states["finished"],
        method_phase=method_phase,
    )
    if inspection["errors"]:
        raise RuntimeError(
            f"method phase completion audit failed: {method_phase}; "
            + "; ".join(inspection["errors"])
        )
    expected = {
        "schema": METHOD_PHASE_COMPLETE_SCHEMA,
        "event": "method_phase_complete",
        "method_phase": method_phase,
        "next_method_phase": next_phase,
        "eligible_sample_count": len(samples),
        "eligible_sample_ids_sha256": _sample_ids_sha256(samples),
        "finished_samples": sorted(states["finished"]),
        "evaluator_incomplete_samples": sorted(
            states["evaluator_incomplete"]),
        "phase_api_calls": _phase_api_call_count(out_dir, method_phase),
        "preservation_violations": 0,
        "run_git_commit": manifest["run_git_commit"],
    }
    prior = _method_phase_complete_events(out_dir, method_phase)
    if len(prior) > 1:
        raise RuntimeError(
            f"duplicate method phase completion record: {method_phase}")
    if prior:
        observed = {
            key: prior[0].get(key) for key in expected
        }
        if observed != expected:
            raise RuntimeError(
                f"method phase completion record drift: {method_phase}")
        return prior[0]
    if next_phase is not None:
        if next_phase not in REMAINING134_METHOD_PHASES:
            raise RuntimeError(f"invalid next method phase: {next_phase}")
        _verify_pristine_method_phase(
            out_dir, next_phase_assignments or [], next_phase)
    record = {
        **expected,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
    }
    append_jsonl_locked(
        os.path.join(out_dir, "dispatch_log.jsonl"), record)
    return record


def _require_hybridpatch_phase_barrier(out_dir, manifest):
    """Refuse every FR launch until the exact HP terminal barrier exists."""
    records = _method_phase_complete_events(out_dir, "hybridpatch")
    if len(records) != 1:
        raise RuntimeError(
            "fullrewrite launch requires exactly one hybridpatch phase barrier"
        )
    record = records[0]
    expected_samples = manifest["config"]["samples"]
    if (record.get("schema") != METHOD_PHASE_COMPLETE_SCHEMA
            or record.get("next_method_phase") != "fullrewrite"
            or record.get("run_git_commit") != manifest["run_git_commit"]
            or record.get("eligible_sample_count") != len(expected_samples)
            or record.get("eligible_sample_ids_sha256")
            != _sample_ids_sha256(expected_samples)
            or record.get("preservation_violations") != 0):
        raise RuntimeError("hybridpatch phase barrier identity is invalid")
    terminal = set(record.get("finished_samples") or []) | set(
        record.get("evaluator_incomplete_samples") or [])
    if terminal != set(expected_samples):
        raise RuntimeError("hybridpatch phase barrier scope is incomplete")
    return record


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
            expected_interrupted = item.get(
                "interrupted_resume_evidence")
            if matches[0].get(
                    "interrupted_resume_authorization") != expected_interrupted:
                raise RuntimeError(
                    f"worker interrupted-resume handshake mismatch: {sample}")
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
                "interrupted_resume_evidence": item.get(
                    "interrupted_resume_evidence"),
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
        record_phase = record.get("method_phase")
        if record_phase is not None and (
                intent.get("method_phase") != record_phase
                or intent.get("methods") != [record_phase]
                or record.get("methods") != [record_phase]):
            raise RuntimeError(
                "stale phased invocation intent mismatch: "
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
            "method_phase": record_phase,
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
    manifest = {
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
    if args.campaign_role in {"confirmation", "full234", "remaining134"}:
        if args.campaign_role == "confirmation":
            slots_per_key = CONFIRMATION_SLOTS_PER_KEY
            key_count = CONFIRMATION_KEY_COUNT
        elif args.campaign_role == "full234":
            slots_per_key = FULL234_SLOTS_PER_KEY
            key_count = FULL234_KEY_COUNT
        else:
            slots_per_key = REMAINING134_SLOTS_PER_KEY
            key_count = REMAINING134_KEY_COUNT
        queues = _assignment_key_queues(assignments)
        manifest["config"].update({
            "dispatch_policy": (
                PHASED_WORK_CONSERVING_DISPATCH_POLICY
                if args.campaign_role == "remaining134"
                else WORK_CONSERVING_DISPATCH_POLICY
            ),
            "slots_per_key": slots_per_key,
            "key_count": key_count,
            "max_worker_count": slots_per_key * key_count,
            "queued_worker_count": max(
                0, len(assignments) - slots_per_key * key_count),
        })
        manifest["assignment_queues"] = queues
    if args.campaign_role == "confirmation":
        manifest["selection_manifest"] = dict(
            _argument_value(args, "_selection_manifest_record") or {})
        analysis_policy = (
            MIXED_CONFIRMATION_ANALYSIS_POLICY
            if manifest["selection_manifest"].get("schema")
            == MIXED_CONFIRMATION_SELECTION_SCHEMA
            else CONFIRMATION_ANALYSIS_POLICY
        )
        manifest["analysis_policy"] = copy.deepcopy(analysis_policy)
    elif args.campaign_role == "full234":
        manifest["full234_scope"] = dict(
            _argument_value(args, "_full234_scope_record") or {})
    elif args.campaign_role == "remaining134":
        manifest["remaining134_scope"] = dict(
            _argument_value(args, "_remaining134_scope_record") or {})
        manifest["config"].update({
            "method_phases": list(REMAINING134_METHOD_PHASES),
            "phase_barrier": "all_hybridpatch_terminal_before_fullrewrite",
            "phase_worker_count": len(samples),
            "total_worker_invocations": len(samples) * 2,
        })
    return manifest


def _manifest_identity(manifest):
    value = copy.deepcopy(manifest)
    value["assignments"] = [
        {"sample": item["sample"], "methods": item["methods"]}
        for item in manifest.get("assignments") or []
    ]
    if "assignment_waves" in value:
        normalized_waves = []
        for wave in value.get("assignment_waves") or []:
            normalized = dict(wave)
            counts = normalized.get("key_worker_counts")
            if isinstance(counts, dict):
                normalized["key_worker_counts"] = sorted(counts.values())
            normalized_waves.append(normalized)
        value["assignment_waves"] = normalized_waves
    if "assignment_queues" in value:
        value["assignment_queues"] = [
            {
                key: item.get(key)
                for key in (
                    "sample_ids", "worker_count", "hybridpatch_first",
                    "fullrewrite_first",
                )
            }
            for item in value.get("assignment_queues") or []
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
        if resume and not same:
            authorization = read_campaign_recovery_authorization(out_dir)
            if authorization:
                authorized_manifest = copy.deepcopy(manifest)
                authorized_manifest["run_git_commit"] = prior.get(
                    "run_git_commit")
                authorized_manifest["git_tree_state"] = prior.get(
                    "git_tree_state")
                authorized_manifest["code_fingerprint"] = prior.get(
                    "code_fingerprint")
                same = (
                    authorization.get("prior_git_commit")
                    == prior.get("run_git_commit")
                    and authorization.get("recovery_git_commit")
                    == manifest.get("run_git_commit")
                    and _manifest_identity(prior)
                    == _manifest_identity(authorized_manifest)
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
                     active_samples=None, required_complete_samples=None,
                     method_phase=None):
    config = manifest["config"]
    declared_phases = config.get("method_phases")
    if method_phase is None and declared_phases is not None:
        if declared_phases != list(REMAINING134_METHOD_PHASES):
            raise RuntimeError("campaign method phases are invalid")
        phase_results = [
            inspect_campaign(
                out_dir, manifest, require_complete=require_complete,
                active_samples=active_samples,
                required_complete_samples=required_complete_samples,
                method_phase=phase,
            )
            for phase in declared_phases
        ]
        merged = dict(phase_results[0])
        merged["errors"] = list(dict.fromkeys(
            error
            for result in phase_results
            for error in result.get("errors") or []
        ))
        for key in (
                "api_calls", "semantic_calls", "preservation_violations",
                "latched_preservation_violations",
                "preservation_not_applicable", "stop_conditions"):
            values = [result.get(key) for result in phase_results]
            if any(value != values[0] for value in values[1:]):
                merged["errors"].append(
                    f"method phase inspection disagrees on {key}")
        return merged
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
    completion_methods = (
        {method_phase} if method_phase else set(expected_methods)
    )
    active_samples = set(active_samples or [])
    default_methods = (
        [method_phase] if method_phase else list(config["method_set"])
    )
    methods_by_sample = {
        sample: list(default_methods) for sample in config["samples"]
    }
    for assignment in manifest.get("assignments") or []:
        sample = assignment.get("sample")
        methods = assignment.get("methods")
        if (method_phase is None and sample in expected_samples
                and isinstance(methods, list)
                and methods):
            methods_by_sample[sample] = list(methods)
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

    for worker_id, authorization in authorizations_by_worker.items():
        worker_metadata = metadata_by_worker.get(worker_id) or []
        dispatch_interrupted = authorization.get(
            "interrupted_resume_evidence")
        metadata_interrupted = (
            worker_metadata[0].get("interrupted_resume_authorization")
            if len(worker_metadata) == 1 else None
        )
        if dispatch_interrupted != metadata_interrupted:
            errors.append(
                f"interrupted resume provenance mismatch: {worker_id}")
        elif (dispatch_interrupted is not None
              and (not isinstance(dispatch_interrupted, dict)
                   or dispatch_interrupted.get("schema")
                   != "anchorpatch.interrupted_phase_resume/1")):
            errors.append(
                f"invalid interrupted resume authorization: {worker_id}")

    commit, tree_state = _git_identity()
    recovery_authorization = read_campaign_recovery_authorization(out_dir)
    expected_runtime_commit = (
        recovery_authorization.get("recovery_git_commit")
        if recovery_authorization else manifest["run_git_commit"]
    )
    if commit != expected_runtime_commit or tree_state != "clean":
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
    recovery_incidents = campaign_recovery_incident_evidence(out_dir)
    authorized_api_incident_hashes = recovery_incidents["api_row_hashes"]
    authorized_attempt_incident_hashes = recovery_incidents[
        "attempt_row_hashes"
    ]
    recovered_worker_ids = recovery_incidents["worker_launch_ids"]
    recovered_preauthorization_worker_ids = recovery_incidents[
        "preauthorization_worker_launch_ids"
    ]
    local_infrastructure_incident_rows = 0
    api_keys = set()
    semantic_groups = {}
    semantic_root_groups = {}
    api_rows_by_worker = {}
    for index, row in enumerate(api_rows, 1):
        sample = row.get("sample")
        method = row.get("method")
        rt = row.get("rt_index")
        direction = row.get("direction")
        if _canonical_record_sha256(row) in authorized_api_incident_hashes:
            local_infrastructure_incident_rows += 1
            if (sample not in expected_samples
                    or method != "fullrewrite"
                    or not _is_exact_int(rt) or not 1 <= rt <= target_rt
                    or direction not in {"forward", "backward"}
                    or row.get("worker_launch_id") not in recovered_worker_ids):
                errors.append(
                    f"authorized local incident is unmappable at API row {index}")
            continue
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

    attempt_rows = [
        row for row in _read_jsonl(os.path.join(
            out_dir, "api_attempt_ledger.jsonl"))
        if _canonical_record_sha256(row)
        not in authorized_attempt_incident_hashes
    ]
    attempt_groups = {}
    attempt_root_ids = set()
    attempt_worker_ids = set()
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
        if isinstance(worker_id, str) and worker_id:
            attempt_worker_ids.add(worker_id)
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
                    root_rows, semantic_root_id,
                    allow_open_attempt=allow_open_attempt,
                    authorized_provider_access_parents=frozenset(
                        authorization["parent_semantic_call_id"]
                        for authorization in recovery_incidents[
                            "provider_access_retry_authorizations"
                        ].values()
                    ),
                )
            )
        except RuntimeError as exc:
            errors.append(
                f"invalid attempt lineage {semantic_root_id}: {exc}"
            )

    calls_by_step = {}
    journal_backed_success_steps = set()
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
                elif provider_rows[0][1].get(
                        "classification") == "provider/API failure":
                    errors.append(
                        f"provider failure cannot back response journal: "
                        f"{semantic_call_id}"
                    )
                else:
                    journal_backed_success_steps.add(step)
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
                if step not in journal_backed_success_steps:
                    errors.append(
                        "committed row requires a response-journal-backed "
                        "successful semantic call: "
                        f"{method}/{sample}/RT{rt}/{direction}"
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
                if direction == "backward":
                    evaluation = row.get("evaluation")
                    score = (
                        evaluation.get("score")
                        if isinstance(evaluation, dict) else None
                    )
                    score_ok = (
                        isinstance(score, (int, float))
                        and not isinstance(score, bool)
                        and math.isfinite(float(score))
                        and 0.0 <= float(score) <= 1.0
                    )
                    error_ok = (
                        isinstance(evaluation, dict)
                        and isinstance(evaluation.get("error"), str)
                        and bool(evaluation.get("error"))
                    )
                    if not score_ok and not error_ok:
                        errors.append(
                            f"unscoreable backward RT{rt}: "
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
            if (sample in completion_samples
                    and method in completion_methods
                    and (completed != target_rt
                         or len(rows) != 2 * target_rt)):
                errors.append(
                    f"incomplete task: {method}/{sample} "
                    f"checkpoint={completed}/{target_rt} rows={len(rows)}/{2 * target_rt}"
                )

    if preservation:
        errors.append(f"preservation_violations={preservation}")

    try:
        latest_outcomes = _latest_sample_outcomes(
            out_dir, expected_samples, method_phase=method_phase)
    except RuntimeError as exc:
        latest_outcomes = {}
        errors.append(str(exc))
    latest_by_sample = {}
    for record in metadata:
        if (method_phase is not None
                and record.get("methods") != [method_phase]):
            continue
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
                "finished", "infrastructure_incomplete",
                "evaluator_incomplete"}
                and (not isinstance(outcome, dict)
                     or outcome.get("status") != record.get("status")
                     or outcome.get("invocation_id")
                     != record.get("invocation_id"))):
            errors.append(f"sample outcome/run metadata mismatch: {sample}")
        if (record.get("status") == "finished"
                and isinstance(outcome, dict)
                and sample not in active_samples):
            expected_sample_methods = methods_by_sample.get(
                sample, default_methods)
            worker_id = record.get("worker_launch_id")
            worker_pid = record.get("worker_pid")
            if outcome.get("schema") != "anchorpatch.sample_outcome/1":
                errors.append(
                    f"finished sample outcome schema mismatch: {sample}")
            if (not isinstance(worker_id, str) or not worker_id
                    or not _is_exact_int(worker_pid) or worker_pid <= 0
                    or outcome.get("worker_launch_id") != worker_id
                    or outcome.get("worker_pid") != worker_pid):
                errors.append(
                    f"finished sample worker provenance mismatch: {sample}")
            if outcome.get("methods") != expected_sample_methods:
                errors.append(
                    f"finished sample method order mismatch: {sample}")
            try:
                actual_progress = _actual_sample_progress(
                    out_dir, sample, expected_sample_methods)
            except RuntimeError as exc:
                errors.append(str(exc))
            else:
                if outcome.get("checkpoint_progress") != actual_progress:
                    errors.append(
                        "finished sample checkpoint evidence drift: "
                        f"{sample}")
        if (record.get("status") == "evaluator_incomplete"
                and isinstance(outcome, dict)
                and sample not in active_samples):
            try:
                _verified_evaluator_incomplete(
                    out_dir, sample, {
                        "worker_launch_id": outcome.get("worker_launch_id"),
                        "worker_pid": outcome.get("worker_pid"),
                        "methods": methods_by_sample.get(
                            sample, default_methods),
                        "target_round_trips": target_rt,
                        "method_phase": method_phase,
                    })
            except RuntimeError as exc:
                errors.append(str(exc))
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
            terminal_metadata = None
            for record in reversed(metadata_by_worker.get(worker_id, [])):
                if record.get("status") in {
                        "finished", "infrastructure_incomplete",
                        "evaluator_incomplete"}:
                    terminal_metadata = record
                    break
            basic_exit_ok = (
                isinstance(exit_row, dict)
                and exit_row.get("sample") == launch.get("sample")
                and exit_row.get("pid") == launch.get("pid")
                and isinstance(exit_row.get("returncode"), int)
                and not isinstance(exit_row.get("returncode"), bool)
                and isinstance(terminal_metadata, dict)
                and terminal_metadata.get("worker_pid") == launch.get("pid")
                and terminal_metadata.get("samples") == [launch.get("sample")]
                and timestamp_ok
            )
            if basic_exit_ok and terminal_metadata.get("status") == "finished":
                exit_ok = (
                    exit_row.get("returncode") == 0
                    and exit_row.get("disposition") == "finished"
                )
            elif (basic_exit_ok
                  and terminal_metadata.get("status") in {
                      "infrastructure_incomplete", "evaluator_incomplete"}):
                exit_ok = (
                    exit_row.get("returncode") != 0
                    and exit_row.get("disposition")
                    == terminal_metadata.get("status")
                    and isinstance(exit_row.get("evidence"), dict)
                )
            else:
                exit_ok = False
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
            recovered_interruption_ok = (
                worker_id in recovered_worker_ids
                and isinstance(exit_row, dict)
                and exit_row.get("sample") == launch.get("sample")
                and exit_row.get("pid") == launch.get("pid")
                and isinstance(exit_row.get("returncode"), int)
                and not isinstance(exit_row.get("returncode"), bool)
                and exit_row.get("returncode") != 0
                and exit_row.get("disposition") == "campaign_fatal"
                and isinstance(terminal_metadata, dict)
                and terminal_metadata.get("worker_pid") == launch.get("pid")
                and terminal_metadata.get("samples") == [launch.get("sample")]
                and terminal_metadata.get("status") in {
                    "failed", "interrupted_by_dispatcher"
                }
                and timestamp_ok
            )
            recovered_preauthorization_ok = (
                worker_id in recovered_preauthorization_worker_ids
                and isinstance(exit_row, dict)
                and exit_row.get("sample") == launch.get("sample")
                and exit_row.get("pid") == launch.get("pid")
                and isinstance(exit_row.get("returncode"), int)
                and not isinstance(exit_row.get("returncode"), bool)
                and exit_row.get("returncode") != 0
                and exit_row.get("disposition") == "campaign_fatal"
                and not metadata_by_worker.get(worker_id)
                and worker_id not in authorizations_by_worker
                and worker_id not in api_rows_by_worker
                and worker_id not in attempt_worker_ids
                and timestamp_ok
            )
            if (not exit_ok and not reconciliation_ok
                    and not recovered_interruption_ok
                    and not recovered_preauthorization_ok):
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
        "local_infrastructure_incident_rows": (
            local_infrastructure_incident_rows),
    }


def _campaign_evidence_digest(out_dir, manifest):
    paths = {
        os.path.join(out_dir, name) for name in (
            "dispatch_manifest.json", "api_calls.jsonl",
            "api_attempt_ledger.jsonl", "run_metadata.jsonl",
            "dispatch_log.jsonl",
            "sample_outcomes.jsonl",
            "evaluator_incomplete_samples.jsonl",
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


def evaluate_confirmation_known_usage_gate(
        out_dir, manifest, boundary_index=None, *, phase="post_wave",
        wave_index=None):
    """Stop confirmation when known committed USD exceeds its boundary limit."""
    if boundary_index is None:
        boundary_index = wave_index
    elif wave_index is not None and wave_index != boundary_index:
        raise RuntimeError("usage gate boundary_index/wave_index mismatch")
    if not _is_exact_int(boundary_index) or boundary_index < 0:
        raise RuntimeError("usage gate boundary index must be a non-negative int")
    config = manifest.get("config") or {}
    samples = config.get("samples") or []
    methods = config.get("method_set") or []
    total_rows = 0
    usage_rows = 0
    total_usd = 0.0
    failure_codes = []
    for sample in samples:
        for method in methods:
            rows = _read_jsonl(os.path.join(out_dir, method, f"{sample}.jsonl"))
            total_rows += len(rows)
            for row in rows:
                usd = row.get("total_usd")
                if (isinstance(usd, (int, float))
                        and not isinstance(usd, bool)
                        and math.isfinite(float(usd)) and usd >= 0):
                    total_usd += float(usd)
                    usage_rows += 1
                else:
                    failure_codes.append("committed_usage_unknown")
    threshold_policy = (
        (manifest.get("analysis_policy") or {})
        .get("known_committed_usage_wave_boundary_stop", {})
    )
    threshold = float(threshold_policy.get(
        "usd_threshold", CONFIRMATION_KNOWN_USAGE_LIMIT_USD))
    if total_usd > threshold + 1e-9:
        failure_codes.append("known_committed_usage_threshold_exceeded")
    failure_codes = sorted(set(failure_codes))
    report = {
        "schema": "anchorpatch.confirmation_known_usage_gate/1",
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "boundary_index": boundary_index,
        "wave_index": boundary_index,
        "phase": phase,
        "rows_observed": total_rows,
        "usage_rows": usage_rows,
        "known_committed_usage_usd": total_usd,
        "known_committed_usage_threshold_usd": threshold,
        "failure_codes": failure_codes,
        "decision": "GO" if not failure_codes else "NO_GO",
    }
    append_jsonl_locked(
        os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"),
        report,
    )
    if failure_codes:
        try:
            record_campaign_stop_condition(
                out_dir,
                "known_committed_usage_wave_boundary_stop",
                boundary_index=boundary_index,
                wave_index=boundary_index,
                phase=phase,
                known_committed_usage_usd=total_usd,
                known_committed_usage_threshold_usd=threshold,
                failure_codes=failure_codes,
                rows_observed=total_rows,
                usage_rows=usage_rows,
            )
        except BaseException as exc:
            raise RuntimeError(
                "confirmation known committed usage refill-boundary stop "
                "failed to persist latch"
            ) from exc
        raise RuntimeError(
            "confirmation known committed usage refill-boundary stop NO_GO: "
            + ", ".join(failure_codes)
        )
    return report


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
    """Remove a finished or strictly evidenced sample-local worker."""
    item["log"].close()
    if returncode == 0:
        _append_worker_exit(
            sample, item, returncode, dispatch_log,
            disposition="finished")
        del running[sample]
        return "finished"
    try:
        outcome = _latest_sample_outcomes(
            out_dir, method_phase=item.get("method_phase")
        ).get(sample) or {}
        process = item.get("process")
        worker_pid = process.pid if process is not None else item.get(
            "worker_pid")
        if (outcome.get("worker_launch_id") != item.get("worker_launch_id")
                or outcome.get("worker_pid") != worker_pid):
            outcome = {}
        status = outcome.get("status")
        if status == "infrastructure_incomplete":
            evidence = _verified_infrastructure_incomplete(
                out_dir, sample, item)
        elif status == "evaluator_incomplete":
            evidence = _verified_evaluator_incomplete(
                out_dir, sample, item)
        else:
            raise RuntimeError(
                f"worker {sample} failed without a supported sample-local outcome")
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
        disposition=status, evidence=evidence)
    del running[sample]
    return status


def _launch_worker_batch(
        args, out_dir, inspection_manifest, task_plans, keys, assignments,
        resume_authorizations, dispatch_log, running):
    """Launch and authorize one refill batch while existing workers continue."""
    phases = {item.get("method_phase") for item in assignments}
    if len(phases) > 1:
        raise RuntimeError("one launch batch cannot mix method phases")
    method_phase = next(iter(phases), None)
    if method_phase == "fullrewrite":
        _require_hybridpatch_phase_barrier(out_dir, inspection_manifest)
    recovery_authorization = read_campaign_recovery_authorization(out_dir)
    runtime_git_commit = (
        recovery_authorization.get("recovery_git_commit")
        if recovery_authorization
        else inspection_manifest["run_git_commit"]
    )
    launch_specs = {}
    for item in assignments:
        sample = item["sample"]
        worker_id = f"paired-{sample}-{uuid.uuid4().hex[:12]}"
        ready_path, ack_path = _worker_barrier_paths(out_dir, worker_id)
        launch_specs[sample] = {
            "sample": sample,
            "worker_launch_id": worker_id,
            "ready_path": ready_path,
            "ack_path": ack_path,
        }
    _write_active_worker_set(
        out_dir, inspection_manifest,
        [*running.values(), *launch_specs.values()])
    batch_running = {}
    for item in assignments:
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
            ANCHORPATCH_WORKER_CONSOLE_LOG=item["console_log"],
            ANCHORPATCH_EXPECTED_GIT_COMMIT=runtime_git_commit,
            ANCHORPATCH_EXPECTED_GIT_TREE_STATE="clean",
            ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256=(
                task_plans[sample]["sha256"]
            ),
            ANCHORPATCH_EXPECTED_TASK_PLAN_PATH=os.path.abspath(
                os.path.join(out_dir, task_plans[sample]["path"])
            ),
        )
        item_method_phase = item.get("method_phase")
        if item_method_phase:
            environment["ANCHORPATCH_METHOD_PHASE"] = item_method_phase
            environment["ANCHORPATCH_CAMPAIGN_METHOD_SET"] = ",".join(
                REMAINING134_METHOD_PHASES)
        else:
            environment.pop("ANCHORPATCH_METHOD_PHASE", None)
            environment.pop("ANCHORPATCH_CAMPAIGN_METHOD_SET", None)
        interrupted_resume = item.get("interrupted_resume_evidence")
        if interrupted_resume is not None:
            environment["ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE"] = (
                json.dumps(
                    interrupted_resume, ensure_ascii=False,
                    sort_keys=True, separators=(",", ":"))
            )
        else:
            environment.pop(
                "ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE", None)
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
                "method_phase": item_method_phase,
                "interrupted_resume_evidence": interrupted_resume,
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
            "method_phase": item_method_phase,
            "interrupted_resume_evidence": interrupted_resume,
            "ready_path": ready_path,
            "ack_path": ack_path,
            "exit_recorded": False,
        }
        batch_running[sample] = running[sample]
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "launch", "sample": sample,
                "key_label": label, "methods": item["methods"],
                "pid": process.pid, "worker_launch_id": worker_id,
                "console_log": item["console_log"],
                "method_phase": item_method_phase,
            },
        )

    if batch_running:
        _authorize_workers(
            out_dir, batch_running, task_plans, dispatch_log,
            args.start_timeout,
        )
    return list(batch_running)


def _take_available_assignments(pending_by_key, running, slots_per_key):
    """Pop stable per-key FIFO work for every currently free key slot."""
    if not _is_exact_int(slots_per_key) or slots_per_key < 1:
        raise RuntimeError("queue slots_per_key must be >= 1")
    running_counts = {}
    for item in running.values():
        label = item.get("key_label")
        running_counts[label] = running_counts.get(label, 0) + 1
    batch = []
    for label, pending in pending_by_key.items():
        free = slots_per_key - running_counts.get(label, 0)
        if free < 0:
            raise RuntimeError(
                f"per-key concurrency exceeded before refill: {label}")
        for _index in range(min(free, len(pending))):
            batch.append(pending.pop(0))
    return batch


def _run_worker_queue(
        args, out_dir, inspection_manifest, task_plans, keys, assignments,
        resume_authorizations, dispatch_log, running,
        infrastructure_incomplete_samples, evaluator_incomplete_samples,
        completed_samples, total_assignment_count, slots_per_key):
    """Drain stable per-key FIFO queues without a cross-key wave barrier."""
    if running:
        raise RuntimeError("cannot start a worker queue while workers are active")
    pending_by_key = {}
    seen_samples = set()
    for item in assignments:
        sample = item.get("sample")
        label = item.get("key_label")
        if (not isinstance(sample, str) or not sample
                or sample in seen_samples
                or not isinstance(label, str) or not label):
            raise RuntimeError("worker queue assignment identity is invalid")
        seen_samples.add(sample)
        pending_by_key.setdefault(label, []).append(item)
    phases = {item.get("method_phase") for item in assignments}
    if len(phases) > 1:
        raise RuntimeError("one worker queue cannot mix method phases")
    method_phase = next(iter(phases), None)
    append_jsonl_locked(
        dispatch_log,
        {
            "event": "queue_start",
            "dispatch_policy": (
                PHASED_WORK_CONSERVING_DISPATCH_POLICY
                if method_phase else WORK_CONSERVING_DISPATCH_POLICY
            ),
            "slots_per_key": slots_per_key,
            "method_phase": method_phase,
            "pending_by_key": {
                label: len(items) for label, items in pending_by_key.items()
            },
        },
    )
    refill_index = 0
    last_report = 0.0
    last_inspection = {
        "errors": [], "api_calls": 0, "preservation_violations": 0}
    while running or any(pending_by_key.values()):
        batch = _take_available_assignments(
            pending_by_key, running, slots_per_key)
        if batch:
            refill_index += 1
            if args.campaign_role == "confirmation":
                evaluate_confirmation_known_usage_gate(
                    out_dir, inspection_manifest, refill_index,
                    phase="pre_refill")
            launched = _launch_worker_batch(
                args, out_dir, inspection_manifest, task_plans, keys, batch,
                resume_authorizations, dispatch_log, running)
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "queue_refill",
                    "refill_index": refill_index,
                    "method_phase": method_phase,
                    "samples": launched,
                    "running_by_key": {
                        label: sum(
                            item.get("key_label") == label
                            for item in running.values())
                        for label in pending_by_key
                    },
                    "pending_by_key": {
                        label: len(items)
                        for label, items in pending_by_key.items()
                    },
                },
            )
        if not running:
            if any(pending_by_key.values()):
                raise RuntimeError("worker queue cannot make progress")
            break

        time.sleep(args.poll_interval)
        exited = []
        completed_in_poll = set()
        for sample, item in list(running.items()):
            returncode = item["process"].poll()
            if returncode is None:
                continue
            disposition = _record_worker_exit(
                out_dir, running, sample, item, returncode,
                dispatch_log)
            exited.append({"sample": sample, "disposition": disposition})
            if disposition == "infrastructure_incomplete":
                infrastructure_incomplete_samples.add(sample)
                continue
            if disposition == "evaluator_incomplete":
                evaluator_incomplete_samples.add(sample)
                continue
            completed_samples.add(sample)
            completed_in_poll.add(sample)
        if exited:
            _write_active_worker_set(
                out_dir, inspection_manifest, running.values())
        # One inspection covers every worker observed exiting in this poll.
        # Re-reading the full append-only campaign ledgers once per sample
        # serialized otherwise independent key queues and left healthy slots
        # idle for minutes as campaigns grew.
        last_inspection = inspect_campaign(
            out_dir, inspection_manifest,
            active_samples=set(running),
            required_complete_samples=completed_in_poll,
            method_phase=method_phase,
        )
        if last_inspection["errors"]:
            raise RuntimeError("; ".join(last_inspection["errors"]))
        if exited:
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "queue_slots_released",
                    "refill_index": refill_index,
                    "method_phase": method_phase,
                    "workers": exited,
                },
            )
        if time.time() - last_report >= args.progress_interval:
            pending_count = sum(len(items) for items in pending_by_key.values())
            print(
                f"PROGRESS running={len(running)}/{total_assignment_count} "
                f"pending={pending_count} "
                f"api_calls={last_inspection['api_calls']} preservation=0",
                flush=True,
            )
            last_report = time.time()
    _write_active_worker_set(out_dir, inspection_manifest, [])
    append_jsonl_locked(
        dispatch_log,
        {
            "event": "queue_complete",
            "refill_count": refill_index,
            "method_phase": method_phase,
            "completed_samples": sorted(completed_samples),
            "infrastructure_incomplete_samples": sorted(
                infrastructure_incomplete_samples),
            "evaluator_incomplete_samples": sorted(
                evaluator_incomplete_samples),
        },
    )
    if args.campaign_role == "confirmation":
        evaluate_confirmation_known_usage_gate(
            out_dir, inspection_manifest, refill_index,
            phase="pre_final")


def _append_remaining134_incomplete(
        out_dir, manifest, method_phase, states, *, skipped_samples=None):
    inspection = inspect_campaign(
        out_dir, manifest,
        required_complete_samples=states["finished"],
        method_phase=method_phase,
    )
    if inspection["errors"]:
        raise RuntimeError("; ".join(inspection["errors"]))
    record = {
        "event": "campaign_incomplete",
        "campaign_role": "remaining134",
        "method_phase": method_phase,
        "infrastructure_incomplete_samples": sorted(
            states["infrastructure_incomplete"]),
        "evaluator_incomplete_samples": sorted(
            states["evaluator_incomplete"]),
        "missing_samples": sorted(states["missing"]),
        "skipped_samples": sorted(skipped_samples or []),
        "completed_samples": sorted(states["finished"]),
        "api_calls": inspection["api_calls"],
        "preservation_violations": 0,
    }
    append_jsonl_locked(
        os.path.join(out_dir, "dispatch_log.jsonl"), record)
    print(
        f"RESULT INCOMPLETE method_phase={method_phase} "
        "infrastructure_samples="
        + ",".join(sorted(states["infrastructure_incomplete"]))
        + " evaluator_samples="
        + ",".join(sorted(states["evaluator_incomplete"])),
        file=sys.stderr, flush=True,
    )


def _run_remaining134_phase(
        args, out_dir, manifest, task_plans, keys, base_assignments,
        method_phase, *, resume, dispatch_log, running,
        next_phase=None, next_phase_assignments=None,
        skipped_samples=None, audited_stale=None, closed_stale=None):
    """Drain one method phase and return its audited outcome partition."""
    phase_assignments = _assignments_for_method_phase(
        base_assignments, method_phase)
    states = _method_phase_outcome_sets(
        out_dir, [item["sample"] for item in phase_assignments],
        method_phase)
    launch_assignments, resume_authorizations = (
        _select_invocation_assignments(
            out_dir, phase_assignments, resume=resume,
            target_round_trips=args.num_round_trips,
            allow_pristine_pending=True,
            method_phase=method_phase,
            allow_audited_interrupted=resume,
            task_plans=task_plans,
        )
    )
    preflight = inspect_campaign(
        out_dir, manifest,
        active_samples={item["sample"] for item in launch_assignments},
        required_complete_samples=states["finished"],
        method_phase=method_phase,
    )
    if preflight["errors"]:
        raise RuntimeError(
            f"{method_phase} phase preflight failed before worker launch: "
            + "; ".join(preflight["errors"])
        )
    append_jsonl_locked(
        dispatch_log,
        {
            "event": "method_phase_start",
            "method_phase": method_phase,
            "next_method_phase": next_phase,
            "resume": bool(resume),
            "reason": args.resume_reason if args.resume else None,
            "workers_stopped_confirmed": bool(
                args.confirm_workers_stopped) if args.resume else None,
            "audited_stale_invocations": list(audited_stale or []),
            "closed_stale_invocations": list(closed_stale or []),
            "eligible_sample_count": len(phase_assignments),
            "eligible_sample_ids_sha256": _sample_ids_sha256(
                item["sample"] for item in phase_assignments),
            "launch_samples": [
                item["sample"] for item in launch_assignments],
            "skipped_finished_samples": sorted(states["finished"]),
            "skipped_evaluator_incomplete_samples": sorted(
                states["evaluator_incomplete"]),
            "excluded_by_prior_phase_samples": sorted(
                skipped_samples or []),
            "transport_authorizations": resume_authorizations,
            "interrupted_resume_evidence": {
                item["sample"]: item["interrupted_resume_evidence"]
                for item in launch_assignments
                if item.get("interrupted_resume_evidence") is not None
            },
        },
    )
    launch_samples = {item["sample"] for item in launch_assignments}
    infrastructure_incomplete = (
        set(states["infrastructure_incomplete"]) - launch_samples
    )
    evaluator_incomplete = set(states["evaluator_incomplete"])
    completed = set(states["finished"])
    if launch_assignments:
        _run_worker_queue(
            args, out_dir, manifest, task_plans, keys,
            launch_assignments, resume_authorizations, dispatch_log,
            running, infrastructure_incomplete, evaluator_incomplete,
            completed, len(phase_assignments), REMAINING134_SLOTS_PER_KEY,
        )
    states = _method_phase_outcome_sets(
        out_dir, [item["sample"] for item in phase_assignments],
        method_phase)
    if states["missing"] or states["infrastructure_incomplete"]:
        _append_remaining134_incomplete(
            out_dir, manifest, method_phase, states,
            skipped_samples=skipped_samples)
        return states, False
    _record_method_phase_complete(
        out_dir, manifest, method_phase, phase_assignments,
        next_phase=next_phase,
        next_phase_assignments=next_phase_assignments,
    )
    return states, True


def _run_remaining134_campaign(
        args, out_dir, manifest, task_plans, keys, assignments,
        dispatch_log, running, *, audited_stale=None, closed_stale=None):
    """Run all HP samples, publish a barrier, then run eligible FR samples."""
    fr_assignments = _assignments_for_method_phase(
        assignments, "fullrewrite")
    hp_barriers = (
        _method_phase_complete_events(out_dir, "hybridpatch")
        if args.resume else []
    )
    if hp_barriers:
        hp_barrier = _require_hybridpatch_phase_barrier(out_dir, manifest)
        hp_states = {
            "finished": set(hp_barrier["finished_samples"]),
            "evaluator_incomplete": set(
                hp_barrier["evaluator_incomplete_samples"]),
            "infrastructure_incomplete": set(),
            "missing": set(),
        }
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "method_phase_resume_reused",
                "method_phase": "hybridpatch",
                "next_method_phase": "fullrewrite",
                "reason": args.resume_reason,
                "eligible_sample_count": hp_barrier[
                    "eligible_sample_count"],
                "eligible_sample_ids_sha256": hp_barrier[
                    "eligible_sample_ids_sha256"],
                "preservation_violations": 0,
            },
        )
    else:
        hp_states, hp_terminal = _run_remaining134_phase(
            args, out_dir, manifest, task_plans, keys, assignments,
            "hybridpatch", resume=args.resume,
            dispatch_log=dispatch_log, running=running,
            next_phase="fullrewrite",
            next_phase_assignments=fr_assignments,
            audited_stale=audited_stale, closed_stale=closed_stale,
        )
        if not hp_terminal:
            return 2
        _require_hybridpatch_phase_barrier(out_dir, manifest)

    hp_evaluator_incomplete = set(hp_states["evaluator_incomplete"])
    fr_base_assignments = [
        item for item in assignments
        if item["sample"] not in hp_evaluator_incomplete
    ]
    fr_states, fr_terminal = _run_remaining134_phase(
        args, out_dir, manifest, task_plans, keys, fr_base_assignments,
        "fullrewrite", resume=True,
        dispatch_log=dispatch_log, running=running,
        skipped_samples=hp_evaluator_incomplete,
    )
    if not fr_terminal:
        return 2

    all_evaluator_incomplete = (
        hp_evaluator_incomplete | set(fr_states["evaluator_incomplete"])
    )
    if all_evaluator_incomplete:
        _append_remaining134_incomplete(
            out_dir, manifest, "fullrewrite", fr_states,
            skipped_samples=hp_evaluator_incomplete)
        return 2

    for phase in REMAINING134_METHOD_PHASES:
        inspection = inspect_campaign(
            out_dir, manifest, require_complete=True,
            method_phase=phase)
        if inspection["errors"]:
            raise RuntimeError("; ".join(inspection["errors"]))
    prior_complete = [
        row for row in _read_jsonl(dispatch_log)
        if row.get("event") == "campaign_complete"
    ]
    if len(prior_complete) > 1:
        raise RuntimeError("duplicate campaign completion record")
    if not prior_complete:
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "campaign_complete",
                "campaign_role": "remaining134",
                "method_phases": list(REMAINING134_METHOD_PHASES),
                "sample_count": len(assignments),
                "sample_ids_sha256": _sample_ids_sha256(
                    item["sample"] for item in assignments),
                "api_calls": sum(
                    _phase_api_call_count(out_dir, phase)
                    for phase in REMAINING134_METHOD_PHASES),
                "preservation_violations": 0,
            },
        )
    print(
        f"RESULT PASS samples={len(assignments)} "
        "method_phases=hybridpatch->fullrewrite preservation=0",
        flush=True,
    )
    return 0


def _launch_under_lease(args, out_dir):
    _resolve_confirmation_selection(args, out_dir=out_dir)
    _resolve_full234_scope(args)
    _resolve_remaining134_scope(args)
    _validate_campaign_grid(args)
    _require_formal_opencode_transport("minimax-m3")
    upstream_smoke_gate = None
    if args.campaign_role == "main":
        upstream_smoke_gate = evaluate_smoke_cost_gate(
            args.smoke_dir, out_dir)
    keys = dict(read_keys(os.path.abspath(args.keys_file)))
    selected_labels = list(args.key_labels or sorted(keys))
    required_key_count = (
        CONFIRMATION_KEY_COUNT
        if args.campaign_role == "confirmation"
        else FULL234_KEY_COUNT if args.campaign_role == "full234"
        else REMAINING134_KEY_COUNT
        if args.campaign_role == "remaining134" else None
    )
    if (required_key_count is not None
            and len(selected_labels) != required_key_count):
        raise RuntimeError(
            f"{args.campaign_role} requires exactly "
            f"{required_key_count} unique key labels"
        )
    missing_labels = [label for label in selected_labels if label not in keys]
    if missing_labels:
        raise RuntimeError(f"unknown key labels: {missing_labels}")
    if args.campaign_role in {"confirmation", "full234", "remaining134"}:
        _require_unique_key_values_for_queued_campaign(keys, selected_labels)
    slots_per_key = getattr(args, "slots_per_key", 1)
    if not _is_exact_int(slots_per_key):
        slots_per_key = 1
    selection_record = _argument_value(
        args, "_selection_manifest_record", {}) or {}
    assignment_samples = list(args.samples)
    if (args.campaign_role == "confirmation"
            and selection_record.get("schema")
            == MIXED_CONFIRMATION_SELECTION_SCHEMA):
        assignment_samples = _mixed_confirmation_sample_order(
            args.samples, selected_labels, slots_per_key,
            selection_record)
    assignments = build_key_assignments(
        assignment_samples, selected_labels, slots_per_key,
        alternate_within_key=(
            args.campaign_role in {
                "supplemental", "confirmation", "full234"}
        ),
        allow_queue=(args.campaign_role in {
            "confirmation", "full234", "remaining134"}),
    )
    if args.campaign_role == "remaining134":
        for item in assignments:
            item["methods"] = list(REMAINING134_METHOD_PHASES)

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
        if args.campaign_role == "remaining134":
            hp_assignments = _assignments_for_method_phase(
                assignments, "hybridpatch")
            hp_states = _method_phase_outcome_sets(
                out_dir, args.samples, "hybridpatch")
            if (hp_states["missing"]
                    or hp_states["infrastructure_incomplete"]):
                dry_phase = "hybridpatch"
                dry_phase_assignments = hp_assignments
                dry_resume = args.resume
                dry_states = hp_states
            else:
                hp_audit = inspect_campaign(
                    out_dir, inspection_manifest,
                    required_complete_samples=hp_states["finished"],
                    method_phase="hybridpatch")
                if hp_audit["errors"]:
                    raise RuntimeError(
                        "dry-run hybridpatch phase audit failed: "
                        + "; ".join(hp_audit["errors"]))
                fr_assignments = _assignments_for_method_phase(
                    [
                        item for item in assignments
                        if item["sample"]
                        not in hp_states["evaluator_incomplete"]
                    ],
                    "fullrewrite",
                )
                barrier = _method_phase_complete_events(
                    out_dir, "hybridpatch")
                if barrier:
                    _require_hybridpatch_phase_barrier(
                        out_dir, inspection_manifest)
                else:
                    _verify_pristine_method_phase(
                        out_dir, fr_assignments, "fullrewrite")
                dry_phase = "fullrewrite"
                dry_phase_assignments = fr_assignments
                dry_resume = True
                dry_states = _method_phase_outcome_sets(
                    out_dir,
                    [item["sample"] for item in fr_assignments],
                    "fullrewrite")
            dry_assignments, _dry_authorizations = (
                _select_invocation_assignments(
                    out_dir, dry_phase_assignments,
                    resume=dry_resume,
                    target_round_trips=args.num_round_trips,
                    allow_pristine_pending=True,
                    method_phase=dry_phase,
                    allow_audited_interrupted=args.resume,
                    task_plans=task_plans,
                )
            )
            _write_active_worker_set(out_dir, inspection_manifest, [])
            dry_inspection = inspect_campaign(
                out_dir, inspection_manifest,
                active_samples={
                    item["sample"] for item in dry_assignments},
                required_complete_samples=dry_states["finished"],
                method_phase=dry_phase,
            )
            if dry_inspection["errors"]:
                raise RuntimeError(
                    "dry-run campaign preflight failed: "
                    + "; ".join(dry_inspection["errors"]))
            return 0
        dry_assignments, _dry_authorizations = (
            _select_invocation_assignments(
                out_dir, assignments, resume=args.resume,
                target_round_trips=args.num_round_trips,
                allow_pristine_pending=(
                    args.campaign_role in {"confirmation", "full234"}),
            )
        )
        dry_active = {item["sample"] for item in dry_assignments}
        dry_outcomes = _latest_sample_outcomes(
            out_dir, [item["sample"] for item in assignments])
        dry_complete = {
            sample for sample, outcome in dry_outcomes.items()
            if outcome.get("status") == "finished"
        }
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
    evaluator_incomplete_samples = set()
    completed_samples = set()
    audited_stale = []
    stale = []
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
        if args.campaign_role == "remaining134":
            return _run_remaining134_campaign(
                args, out_dir, inspection_manifest, task_plans, keys,
                assignments, dispatch_log, running,
                audited_stale=audited_stale, closed_stale=stale,
            )
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
                target_round_trips=args.num_round_trips,
                allow_pristine_pending=(
                    args.campaign_role in {"confirmation", "full234"}),
            )
        )
        latest_outcomes = _latest_sample_outcomes(
            out_dir, [item["sample"] for item in assignments])
        completed_samples = {
            sample for sample, outcome in latest_outcomes.items()
            if outcome.get("status") == "finished"
        }
        evaluator_incomplete_samples = {
            sample for sample, outcome in latest_outcomes.items()
            if outcome.get("status") == "evaluator_incomplete"
        }
        if args.resume:
            resume_record = {
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
            }
            if args.campaign_role in {"confirmation", "full234"}:
                resume_record.update({
                    "launch_pending_samples": [
                        item["sample"] for item in launch_assignments
                        if item["sample"] not in resume_authorizations
                    ],
                    "launch_infrastructure_incomplete_samples": sorted(
                        resume_authorizations
                    ),
                    "skipped_evaluator_incomplete_samples": sorted(
                        evaluator_incomplete_samples
                    ),
                })
            append_jsonl_locked(dispatch_log, resume_record)
        _run_worker_queue(
            args, out_dir, inspection_manifest, task_plans, keys,
            launch_assignments, resume_authorizations, dispatch_log,
            running, incomplete_samples, evaluator_incomplete_samples,
            completed_samples, len(assignments), slots_per_key,
        )
        if incomplete_samples or evaluator_incomplete_samples:
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
                    "evaluator_incomplete_samples": sorted(
                        evaluator_incomplete_samples),
                    "completed_samples": sorted(completed_samples),
                    "api_calls": inspection["api_calls"],
                    "preservation_violations": 0,
                },
            )
            print(
                "RESULT INCOMPLETE infrastructure_samples="
                + ",".join(sorted(incomplete_samples))
                + " evaluator_samples="
                + ",".join(sorted(evaluator_incomplete_samples)),
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
        "--campaign_role",
        choices=(
            "smoke", "main", "supplemental", "confirmation", "full234",
            "remaining134"),
        required=True)
    parser.add_argument(
        "--smoke_dir",
        help="required completed 2-sample smoke directory for main",
    )
    parser.add_argument("--samples", nargs="+")
    parser.add_argument(
        "--selection_manifest",
        help="committed method-unseen selection JSON for confirmation",
    )
    parser.add_argument("--num_round_trips", type=int, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--keys_file", default=os.path.abspath(
            os.path.join(_ROOT, "..", ".env.frkeys")),
    )
    parser.add_argument("--key_labels", nargs="+", default=None)
    parser.add_argument(
        "--slots_per_key", type=int, default=1,
        help="maximum concurrently launched paired sample workers per key",
    )
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
    manual_samples = args.samples
    if args.campaign_role == "confirmation":
        if manual_samples:
            parser.error(
                "confirmation samples come only from --selection_manifest"
            )
        try:
            _resolve_confirmation_selection(args, out_dir=args.out_dir)
        except RuntimeError as exc:
            parser.error(str(exc))
    elif args.campaign_role in {"full234", "remaining134"}:
        if manual_samples:
            parser.error(
                f"{args.campaign_role} samples are derived automatically"
            )
        if args.selection_manifest:
            parser.error(
                "--selection_manifest is only valid for confirmation"
            )
        try:
            if args.campaign_role == "full234":
                _resolve_full234_scope(args)
            else:
                _resolve_remaining134_scope(args)
        except RuntimeError as exc:
            parser.error(str(exc))
    else:
        if not args.samples:
            parser.error("--samples is required for this campaign role")
        if args.selection_manifest:
            parser.error(
                "--selection_manifest is only valid for confirmation"
            )
    if args.campaign_role == "smoke":
        if args.samples != SMOKE_SAMPLES or args.num_round_trips != 2:
            parser.error(
                "smoke requires exactly treebank4 obj3d2 with 2 round trips"
            )
        if args.smoke_dir:
            parser.error("--smoke_dir is only valid for main")
    elif args.campaign_role == "main":
        if args.samples != MAIN_SAMPLES or args.num_round_trips != 10:
            parser.error(
                "main requires the fixed 10-sample order with 10 round trips"
            )
        if not args.smoke_dir:
            parser.error("main requires --smoke_dir")
    elif args.campaign_role == "supplemental":
        if (args.samples != SUPPLEMENTAL_SAMPLES
                or args.num_round_trips != 10):
            parser.error(
                "supplemental requires the fixed val40 order with 10 round trips"
            )
        if args.smoke_dir:
            parser.error("--smoke_dir is not valid for supplemental")
        if args.slots_per_key != 4:
            parser.error("supplemental requires --slots_per_key 4")
    elif args.campaign_role == "confirmation":
        selection_record = _argument_value(
            args, "_selection_manifest_record", {}) or {}
        expected_count = (
            MIXED_CONFIRMATION_SAMPLE_COUNT
            if selection_record.get("schema")
            == MIXED_CONFIRMATION_SELECTION_SCHEMA
            else CONFIRMATION_SAMPLE_COUNT
        )
        if (expected_count not in {
                CONFIRMATION_SAMPLE_COUNT, MIXED_CONFIRMATION_SAMPLE_COUNT}
                or len(args.samples) != expected_count
                or args.num_round_trips != 10):
            parser.error(
                "confirmation requires the committed selection with 10 round trips"
            )
        if args.smoke_dir:
            parser.error("--smoke_dir is not valid for confirmation")
        if args.slots_per_key != CONFIRMATION_SLOTS_PER_KEY:
            parser.error("confirmation requires --slots_per_key 4")
    elif args.campaign_role == "full234":
        if (len(args.samples) != FULL234_SAMPLE_COUNT
                or args.num_round_trips != 10):
            parser.error(
                "full234 requires the exact 234-sample inventory with 10 round trips"
            )
        if args.smoke_dir:
            parser.error("--smoke_dir is not valid for full234")
        if args.slots_per_key != FULL234_SLOTS_PER_KEY:
            parser.error("full234 requires --slots_per_key 4")
    else:
        if (len(args.samples) != REMAINING134_SAMPLE_COUNT
                or args.num_round_trips != 10):
            parser.error(
                "remaining134 requires the exact complement of the prior "
                "planned100 with 10 round trips"
            )
        if args.smoke_dir:
            parser.error("--smoke_dir is not valid for remaining134")
        if args.slots_per_key != REMAINING134_SLOTS_PER_KEY:
            parser.error("remaining134 requires --slots_per_key 4")
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
