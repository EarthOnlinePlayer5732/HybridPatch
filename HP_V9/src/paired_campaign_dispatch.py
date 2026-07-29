"""Integrity-fail-fast paired MiniMax campaign launcher for HP_V9.

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
import signal
import subprocess
import sys
import threading
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
    DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
    DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
    RUN_METADATA_STORAGE_EVENT_V1,
    SNAPSHOT_MODE_ALL,
    SNAPSHOT_MODE_FAILURES,
    SNAPSHOT_MODE_OFF,
    _canonical_record_sha256,
    _deepseek_dispatcher_stopped_sidecar_evidence,
    _fold_run_metadata_events,
    _git_identity,
    _validate_deepseek_compact_records,
    _validate_deepseek_linear_records,
    _validated_transport_ledger_state,
    append_jsonl_locked,
    append_jsonl_records_locked,
    campaign_recovery_incident_evidence,
    code_fingerprint,
    interrupt_audited_running_invocations,
    read_campaign_recovery_authorization,
    read_campaign_stop_conditions,
    read_sample_outcomes,
    read_run_metadata_snapshot,
    read_quiescent_run_metadata_snapshot,
    record_campaign_stop_condition,
    record_emergency_campaign_stop_condition,
    normalize_snapshot_mode,
    snapshot_docs_sample_dirs,
    write_json_atomic,
)
from control_telemetry import record_slow_control_operation
from utils_env import load_sample
from utils_relay_plan import (
    build_relay_task_plan,
    load_relay_task_plan,
    save_relay_task_plan,
)


SCHEMA = "anchorpatch.paired_campaign_manifest/1"
TRANSPORT_REVISION = "opencode_anthropic_sdk/4"
DEEPSEEK_TRANSPORT = "openai_sdk_stream"
DEEPSEEK_TRANSPORT_REVISION = "opencode_openai_compatible/6"
DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION = "opencode_openai_compatible/5"
DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION = (
    "opencode_openai_compatible/4"
)
DEEPSEEK_LEGACY_TRANSPORT = "openai_sdk_nonstream"
DEEPSEEK_LEGACY_TRANSPORT_REVISION = "opencode_openai_compatible/3"
DEEPSEEK_MODEL = "deepseek-v4-flash"
DEEPSEEK_BASE_URL = "https://opencode.ai/zen/go/v1"
DEEPSEEK_REASONING_EFFORT = "high"
DEEPSEEK_MAX_TOKENS = 20000
DEEPSEEK_FULL234_ROUND_TRIPS = 10
AUDITED_OPERATOR_PAUSE_RUNTIME = {
    "model": DEEPSEEK_MODEL,
    "max_tokens": DEEPSEEK_MAX_TOKENS,
    "provider": "opencode_zen",
    "transport": DEEPSEEK_TRANSPORT,
    "transport_revision": DEEPSEEK_TRANSPORT_REVISION,
    "transport_resume_policy": None,
    "openai_base_url": DEEPSEEK_BASE_URL,
    "reasoning_effort": DEEPSEEK_REASONING_EFFORT,
}
API_CALL_SCHEMA = "anchorpatch.api_call/4"
API_ATTEMPT_SCHEMA = "anchorpatch.api_attempt/4"
API_RESPONSE_JOURNAL_SCHEMA = "anchorpatch.api_response_journal/4"
PROVIDER_GUARD_ACTIVE_SET_V1 = "active_worker_set_v1"
PROVIDER_GUARD_WORKER_START_CAPABILITY_V1 = (
    "worker_start_capability_v1"
)
WORKER_START_CAPABILITY_SCHEMA = "anchorpatch.worker_start/2"
_WORKER_START_CAPABILITY_FIELDS = (
    "schema", "worker_launch_id", "worker_pid", "invocation_id", "sample",
    "task_plan_sha256", "provider_guard_mode", "dispatcher_pid",
    "dispatcher_instance_id", "run_git_commit", "transport_revision",
    "dispatch_manifest_canonical_sha256",
)
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
DEEPSEEK_CAPACITY_SLOTS_PER_KEY = 15
DEEPSEEK_FULL_SLOTS_PER_KEY = 10
DEEPSEEK_CAPACITY_KEY_COUNT = 1
DEEPSEEK_FULL_KEY_COUNT = 3
DEEPSEEK_CAPACITY_SAMPLES = [
    "earncall1", "latex6", "screenplay4", "dbschema1", "circuit4",
    "json4", "vector2", "fonteng1", "treebank1", "genealogy6",
    "jobboard6", "python2", "obj3d4", "mathlean3", "molecule2",
]
DEEPSEEK_CAMPAIGN_ROLES = {
    "deepseek_capacity15", "deepseek_full234",
}
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
DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON = "opencode_monthly_usage_limit"
DEEPSEEK_MONTHLY_USAGE_LIMIT_MESSAGE_PREFIX = (
    "Monthly usage limit reached."
)
KEY_REACTIVATION_EVENT = "key_reactivated"
PENDING_PROVENANCE_FIELD = "_pending_provenance"
PENDING_NEVER_STARTED = "never_started"
PENDING_INFRASTRUCTURE_INCOMPLETE = "infrastructure_incomplete"
PENDING_INTERRUPTED = "interrupted"
OPERATOR_PAUSE_CONDITION = "operator_directed_dispatcher_pause"
DEEPSEEK_EXIT_FULL_AUDIT_INTERVAL = 20
_DEEPSEEK_INCREMENTAL_LEDGER_FILES = (
    # Read terminal-publication ledgers newest-to-oldest.  Workers publish
    # API -> outcome -> metadata, so this order can observe an older causal
    # prefix but cannot pair a newer outcome/metadata event with a missing API
    # row while other workers are still active.
    "run_metadata_events.jsonl",
    "sample_outcomes.jsonl",
    "api_calls.jsonl",
    "api_attempt_ledger.jsonl",
    # Dispatcher-only routing evidence is read last; the current exit and
    # authorization rows were already durably appended by this process.
    "dispatch_log.jsonl",
)
_OPERATOR_PAUSE_EXIT_CODES = {
    "SIGINT": 130,
    "SIGTERM": 143,
}
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


class OperatorPauseRequested(RuntimeError):
    """Internal control-flow exception for a user-directed dispatcher pause."""

    def __init__(self, *, signal_name=None, signum=None, boundary=None):
        self.signal_name = signal_name or "operator_pause"
        self.signum = signum
        self.boundary = boundary
        self.exit_code = _OPERATOR_PAUSE_EXIT_CODES.get(self.signal_name, 130)
        message = "operator pause requested"
        if self.signal_name:
            message += f" by {self.signal_name}"
        if boundary:
            message += f" at {boundary}"
        super().__init__(message)


class _OperatorPauseState:
    def __init__(self):
        self.event = threading.Event()
        self.signum = None
        self.signal_name = None

    def request(self, signum):
        if self.event.is_set():
            return
        self.signum = signum
        try:
            self.signal_name = signal.Signals(signum).name
        except ValueError:
            self.signal_name = f"signal_{signum}"
        self.event.set()

    def raise_if_requested(self, boundary):
        if self.event.is_set():
            raise OperatorPauseRequested(
                signal_name=self.signal_name,
                signum=self.signum,
                boundary=boundary,
            )


def _operator_pause_state(args):
    return getattr(args, "_operator_pause_state", None)


def _raise_if_operator_pause_requested(args, boundary):
    state = _operator_pause_state(args)
    if state is not None:
        state.raise_if_requested(boundary)


def _supports_audited_operator_pause(args):
    """Return whether the offline recovery reader supports this campaign."""
    return (
        getattr(args, "campaign_role", None) == "deepseek_full234"
        and getattr(args, "num_round_trips", None)
        == DEEPSEEK_FULL234_ROUND_TRIPS
        and getattr(args, "slots_per_key", DEEPSEEK_FULL_SLOTS_PER_KEY)
        == DEEPSEEK_FULL_SLOTS_PER_KEY
        and _campaign_runtime_config(args) == AUDITED_OPERATOR_PAUSE_RUNTIME
    )


def _worker_popen_kwargs():
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", None)
        if creationflags is None:
            raise RuntimeError(
                "Windows worker isolation requires CREATE_NEW_PROCESS_GROUP")
        return {
            "creationflags": creationflags,
        }
    return {"start_new_session": True}


@contextlib.contextmanager
def _operator_pause_signal_scope(args):
    state = _OperatorPauseState()
    previous_state = getattr(args, "_operator_pause_state", None)
    args._operator_pause_state = state
    signal_numbers = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        signal_numbers.append(signal.SIGTERM)
    previous_handlers = {}

    def _handler(signum, _frame):
        state.request(signum)

    try:
        for signum in signal_numbers:
            try:
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, _handler)
            except (OSError, RuntimeError, ValueError):
                previous_handlers.pop(signum, None)
        yield state
    finally:
        for signum, handler in previous_handlers.items():
            try:
                signal.signal(signum, handler)
            except (OSError, RuntimeError, ValueError):
                pass
        if previous_state is None:
            try:
                delattr(args, "_operator_pause_state")
            except AttributeError:
                pass
        else:
            args._operator_pause_state = previous_state


def _campaign_runtime_config(args):
    if args.campaign_role in DEEPSEEK_CAMPAIGN_ROLES:
        return {
            "model": DEEPSEEK_MODEL,
            "max_tokens": DEEPSEEK_MAX_TOKENS,
            "provider": "opencode_zen",
            "transport": DEEPSEEK_TRANSPORT,
            "transport_revision": DEEPSEEK_TRANSPORT_REVISION,
            "transport_resume_policy": None,
            "openai_base_url": DEEPSEEK_BASE_URL,
            "reasoning_effort": DEEPSEEK_REASONING_EFFORT,
        }
    return {
        "model": "minimax-m3",
        "max_tokens": 131072,
        "provider": "opencode_go",
        "transport": "anthropic_sdk_v2",
        "transport_revision": TRANSPORT_REVISION,
        "transport_resume_policy": TRANSPORT_RESUME_POLICY,
        "opencode_transport": "anthropic_sdk_v2",
        "minimax_transport": "opencode",
        "reasoning_effort": None,
    }
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
    elif args.campaign_role == "deepseek_capacity15":
        if (list(args.samples) != DEEPSEEK_CAPACITY_SAMPLES
                or args.num_round_trips != 2):
            raise RuntimeError(
                "deepseek_capacity15 requires the fixed 15-sample RT2 grid"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError(
                "deepseek_capacity15 cannot declare --smoke_dir")
        if (getattr(args, "slots_per_key", None)
                != DEEPSEEK_CAPACITY_SLOTS_PER_KEY):
            raise RuntimeError(
                "deepseek_capacity15 requires --slots_per_key 15")
    elif args.campaign_role in {"full234", "deepseek_full234"}:
        scope = _argument_value(args, "_full234_scope_record", {}) or {}
        expected_rt = (
            DEEPSEEK_FULL234_ROUND_TRIPS
            if args.campaign_role == "deepseek_full234" else 10
        )
        expected_slots = (
            DEEPSEEK_FULL_SLOTS_PER_KEY
            if args.campaign_role == "deepseek_full234"
            else FULL234_SLOTS_PER_KEY
        )
        if (scope.get("schema") != "anchorpatch.full234_scope/1"
                or list(args.samples) != list(scope.get("sample_ids") or [])
                or len(args.samples) != FULL234_SAMPLE_COUNT
                or args.num_round_trips != expected_rt):
            raise RuntimeError(
                f"{args.campaign_role} requires the exact 234-sample "
                f"inventory at {expected_rt} RT"
            )
        if getattr(args, "smoke_dir", None):
            raise RuntimeError(
                f"{args.campaign_role} cannot declare --smoke_dir")
        if (getattr(args, "slots_per_key", None)
                != expected_slots):
            raise RuntimeError(
                f"{args.campaign_role} requires --slots_per_key "
                f"{expected_slots}")
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
    deadline = time.monotonic() + 1.0
    while True:
        try:
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            raise
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


def _manifest_snapshot_mode(manifest):
    config = manifest.get("config") if isinstance(manifest, dict) else None
    if not isinstance(config, dict):
        return SNAPSHOT_MODE_ALL, False
    present = "snapshot_mode" in config
    return (
        normalize_snapshot_mode(
            config.get("snapshot_mode"), default=SNAPSHOT_MODE_ALL),
        present,
    )


def _existing_manifest_snapshot_mode(out_dir):
    path = os.path.join(out_dir, "dispatch_manifest.json")
    if not os.path.exists(path):
        return None, False, False
    mode, present = _manifest_snapshot_mode(_read_json(path))
    return mode, present, True


def _snapshot_mode_arg(args):
    value = getattr(args, "snapshot_mode", None)
    if value is None or isinstance(value, str):
        return value
    return None


def _resolve_paired_snapshot_mode(out_dir, args):
    requested = _snapshot_mode_arg(args)
    if requested is not None:
        return normalize_snapshot_mode(
            requested, default=SNAPSHOT_MODE_FAILURES)
    prior_mode, _present, has_manifest = _existing_manifest_snapshot_mode(
        out_dir)
    if getattr(args, "resume", False) and has_manifest:
        return prior_mode
    return SNAPSHOT_MODE_FAILURES


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
    if _argument_value(args, "campaign_role") not in {
            "full234", "deepseek_full234"}:
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
    if not os.path.exists(path):
        return []
    deadline = time.monotonic() + 60.0
    while True:
        try:
            records = []
            with open(path, encoding="utf-8") as handle:
                # Writers use an exclusive lock for each complete append.
                # Wait through ordinary live-writer contention instead of
                # turning one Windows sharing failure into a campaign stop.
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
                                f"invalid JSONL record at "
                                f"{path}:{line_number}"
                            ) from exc
                        if not isinstance(record, dict):
                            raise RuntimeError(
                                f"non-object JSONL record at "
                                f"{path}:{line_number}"
                            )
                        records.append(record)
                finally:
                    portalocker.unlock(handle)
            return records
        except FileNotFoundError:
            return []
        except (OSError, portalocker.exceptions.LockException):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


def _latest_sample_outcomes(
        out_dir, expected_samples=None, *, method_phase=None,
        records=None):
    latest = {}
    latest_by_phase = {}
    expected = set(expected_samples or [])
    outcome_records = (
        read_sample_outcomes(out_dir) if records is None else records
    )
    for index, record in enumerate(outcome_records, 1):
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
        for docs_path in snapshot_docs_sample_dirs(out_dir, method, sample):
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
        "sample_outcomes.jsonl", "api_calls.jsonl",
        "api_anomalies.jsonl", "evaluator_incomplete_samples.jsonl",
    )
    for filename in record_files:
        path = os.path.join(out_dir, filename)
        for row_number, record in enumerate(_read_jsonl(path), 1):
            if (_record_mentions_sample(record, sample)
                    and (method_phase is None
                         or _record_mentions_method(record, method_phase))):
                evidence.append(f"{filename}:{row_number}")
    metadata_source = (
        "run_metadata_events.jsonl:projection"
        if os.path.isfile(os.path.join(out_dir, "run_metadata_events.jsonl"))
        else "run_metadata.jsonl"
    )
    for row_number, record in enumerate(
            read_run_metadata_snapshot(out_dir), 1):
        if (_record_mentions_sample(record, sample)
                and (method_phase is None
                     or _record_mentions_method(record, method_phase))):
            evidence.append(f"{metadata_source}:{row_number}")

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
                    "not_started_no_healthy_key_samples",
                    "resume_pending_no_healthy_key_samples",
                    "evaluator_incomplete_samples",
                )
            )
        if event == "campaign_incomplete":
            mentions = mentions or any(
                sample in (record.get(field) or [])
                for field in (
                    "infrastructure_incomplete_samples",
                    "not_started_no_healthy_key_samples",
                    "resume_pending_no_healthy_key_samples",
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


def _legacy_queue_exhausted_pending_evidence(
        out_dir, sample, evidence, *, method_phase=None):
    """Accept ef90019 queue-exhausted aggregates only for never-started work."""
    if not evidence:
        return False
    dispatch_rows = _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
    dispatch_indices = []
    for item in evidence:
        if not item.startswith("dispatch_log.jsonl:"):
            return False
        try:
            index = int(item.rsplit(":", 1)[1])
        except (TypeError, ValueError):
            return False
        if index < 1 or index > len(dispatch_rows):
            return False
        dispatch_indices.append(index)

    queue_exhausted_indices = []
    for row_index, record in enumerate(dispatch_rows, 1):
        if record.get("event") != "queue_exhausted_no_healthy_key":
            continue
        if method_phase is not None and record.get("method_phase") != method_phase:
            continue
        pending = record.get("pending_samples")
        if sample not in (pending or []):
            continue
        not_started = record.get("not_started_pending_samples")
        if not_started is not None and sample not in not_started:
            continue
        if (record.get("reason") == DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON
                and isinstance(record.get("quarantined_key_labels"), list)):
            queue_exhausted_indices.append(row_index)
    if not any(
            index < min(dispatch_indices)
            for index in queue_exhausted_indices):
        return False

    allowed_events = {"queue_complete", "campaign_incomplete"}
    for index in dispatch_indices:
        row = dispatch_rows[index - 1]
        event = row.get("event")
        if event not in allowed_events:
            return False
        if method_phase is not None and row.get("method_phase") != method_phase:
            return False
        if sample in (row.get("completed_samples") or []):
            return False
        if sample in (row.get("evaluator_incomplete_samples") or []):
            return False
        if sample in (row.get("not_started_no_healthy_key_samples") or []):
            continue
        if sample in (row.get("infrastructure_incomplete_samples") or []):
            continue
        return False
    return True


def _valid_deepseek_failed_retry_evidence(row):
    """Validate the complete attempt chain for one exhausted DeepSeek call."""
    attempts = row.get("transport_attempts")
    runtime_identity = (
        row.get("transport"), row.get("transport_revision"))
    is_stream = runtime_identity in {
        (DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION),
        (DEEPSEEK_TRANSPORT, DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION),
        (
            DEEPSEEK_TRANSPORT,
            DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
        ),
    }
    is_frozen_legacy = runtime_identity == (
        DEEPSEEK_LEGACY_TRANSPORT,
        DEEPSEEK_LEGACY_TRANSPORT_REVISION,
    )
    http_attempts_used = row.get("http_attempts_used")
    if (not (is_stream or is_frozen_legacy)
            or not isinstance(attempts, list) or not attempts
            or (
                http_attempts_used != len(attempts)
                and not (
                    is_frozen_legacy and http_attempts_used is None
                )
            )
            or row.get("retry_count") != len(attempts) - 1
            or row.get("failed_attempt_count") != len(attempts)):
        return False

    max_attempts = row.get("max_retries")
    if is_frozen_legacy and max_attempts is None:
        # Frozen /3 failure rows predate persistence of the request-side
        # max_retries field; the formal runner was fixed at three.
        max_attempts = 3
    if (not _is_exact_int(max_attempts) or max_attempts < 1):
        return False

    budget_attempt_index = 0
    for expected_index, attempt in enumerate(attempts, 1):
        if (not isinstance(attempt, dict)
                or attempt.get("attempt_index") != expected_index
                or attempt.get("status") != "retryable_error"
                or not isinstance(attempt.get("error_type"), str)
                or not attempt.get("error_type")):
            return False
        if (is_stream
                and not isinstance(
                    attempt.get("generation_delta_seen"), bool)):
            return False
        free_503 = (
            attempt.get("http_status") == 503
            and (
                attempt.get("generation_delta_seen") is False
                if is_stream
                else not attempt.get("generation_delta_seen")
            )
        )
        if attempt.get("retry_budget_consumed") is not (not free_503):
            return False
        if not free_503:
            budget_attempt_index += 1
        if attempt.get(
                "retry_budget_attempt_index") != budget_attempt_index:
            return False

    final_attempt = attempts[-1]
    return (
        budget_attempt_index == max_attempts
        and final_attempt.get("retry_budget_consumed") is True
        and row.get("http_status") == final_attempt.get("http_status")
        and row.get("error_type") == final_attempt.get("error_type")
    )


def _deepseek_failed_retry_row(row):
    return (
        row.get("model") == DEEPSEEK_MODEL
        and row.get("classification") == "provider/API failure"
        and isinstance(row.get("error_type"), str)
        and row.get("provider_called") is True
        and row.get("stream_complete") is False
        and row.get("response_replayed") is False
        and row.get("count_as_method_failure") is False
        and _valid_deepseek_failed_retry_evidence(row)
    )


def _deepseek_usage_limit_error_payload(error_body):
    if not isinstance(error_body, dict):
        return None
    if (set(error_body) == {"type", "message"}
            and error_body.get("type") == "GoUsageLimitError"
            and isinstance(error_body.get("message"), str)
            and error_body["message"].startswith(
                DEEPSEEK_MONTHLY_USAGE_LIMIT_MESSAGE_PREFIX)):
        return error_body
    return None


def _deepseek_monthly_usage_limit_row(row):
    """Return true only for the exact OpenCode monthly-quota terminal row."""
    if (not _deepseek_failed_retry_row(row)
            or row.get("http_status") != 429
            or row.get("error_type") != "rate_limit"):
        return False
    attempts = row.get("transport_attempts")
    if not isinstance(attempts, list) or not attempts:
        return False
    final_attempt = attempts[-1]
    if (not isinstance(final_attempt, dict)
            or final_attempt.get("http_status") != 429
            or final_attempt.get("error_type") != "rate_limit"
            or final_attempt.get("status") != "retryable_error"
            or final_attempt.get("response_started_http_status") is not None
            or final_attempt.get("generation_delta_seen") is not False
            or final_attempt.get("stream_complete") is not False):
        return False
    return _deepseek_usage_limit_error_payload(
        final_attempt.get("error_body")) is not None


def _deepseek_campaign_runtime_identity(out_dir):
    manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if not os.path.isfile(manifest_path):
        return None
    manifest = _read_json(manifest_path)
    config = manifest.get("config") if isinstance(manifest, dict) else None
    identity = (
        (config or {}).get("transport"),
        (config or {}).get("transport_revision"),
    )
    if ((config or {}).get("model") != DEEPSEEK_MODEL
            or identity not in {
                (DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION),
                (
                    DEEPSEEK_TRANSPORT,
                    DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION,
                ),
                (
                    DEEPSEEK_TRANSPORT,
                    DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
                ),
                (
                    DEEPSEEK_LEGACY_TRANSPORT,
                    DEEPSEEK_LEGACY_TRANSPORT_REVISION,
                ),
            }):
        return None
    return identity


def _verified_deepseek_resume_missing_samples(out_dir, assignments):
    """Allow DeepSeek queued, interrupted, or server-retry-exhausted samples."""
    return _verified_deepseek_resume_missing_samples_with_provenance(
        out_dir, assignments)


def _record_pending_provenance(provenance_out, sample, provenance):
    if provenance_out is None:
        return
    prior = provenance_out.get(sample)
    if prior is not None and prior != provenance:
        raise RuntimeError(
            "queued resume pending provenance is inconsistent: "
            f"{sample} {prior!r}!={provenance!r}")
    provenance_out[sample] = provenance


def _verified_deepseek_resume_missing_samples_with_provenance(
        out_dir, assignments, *, provenance_out=None):
    """Allow DeepSeek resume and classify pending samples when requested."""
    samples = [item["sample"] for item in assignments]
    sample_set = set(samples)
    recovery_incidents = campaign_recovery_incident_evidence(out_dir)
    recovered_worker_ids = recovery_incidents["worker_launch_ids"]
    recovered_workers_by_sample = recovery_incidents.get(
        "deepseek_recovered_workers", {})
    api_incident_kinds = recovery_incidents.get(
        "api_incident_kinds", {})
    parent_loss_workers = recovery_incidents.get(
        "dispatcher_parent_loss_workers", {})
    latest_metadata = {}
    metadata_rows = read_run_metadata_snapshot(out_dir)
    for record in metadata_rows:
        for sample in record.get("samples") or []:
            if sample in sample_set:
                latest_metadata[sample] = record
    dispatch_rows = _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
    launches_by_worker = {
        row.get("worker_launch_id"): row for row in dispatch_rows
        if row.get("event") == "launch"
    }
    exits_by_worker = {
        row.get("worker_launch_id"): row for row in dispatch_rows
        if row.get("event") == "worker_exit"
    }
    api_rows_by_worker = {}
    for row in _read_jsonl(os.path.join(out_dir, "api_calls.jsonl")):
        worker_id = row.get("worker_launch_id")
        if isinstance(worker_id, str) and worker_id:
            api_rows_by_worker.setdefault(worker_id, []).append(row)
    allowed = set()
    for item in assignments:
        sample = item["sample"]
        evidence = _queued_pending_evidence(
            out_dir, sample, item.get("methods") or [])
        if not evidence:
            allowed.add(sample)
            _record_pending_provenance(
                provenance_out, sample, PENDING_NEVER_STARTED)
            continue
        if _legacy_queue_exhausted_pending_evidence(
                out_dir, sample, evidence):
            allowed.add(sample)
            _record_pending_provenance(
                provenance_out, sample, PENDING_NEVER_STARTED)
            continue
        parent_worker = parent_loss_workers.get(sample)
        if isinstance(parent_worker, dict) and parent_worker.get(
                "status") in {"preauthorization", "registered_prelaunch"}:
            parent_worker_id = parent_worker.get("worker_launch_id")
            parent_status = parent_worker.get("status")
            parent_exit = exits_by_worker.get(parent_worker_id)
            parent_launch = launches_by_worker.get(parent_worker_id)
            parent_metadata = [
                row for row in metadata_rows
                if row.get("worker_launch_id") == parent_worker_id
            ]
            preauthorization_valid = (
                parent_status == "preauthorization"
                and isinstance(parent_launch, dict)
                and parent_launch.get("sample") == sample
                and isinstance(parent_exit, dict)
                and parent_exit.get("sample") == sample
                and parent_exit.get("pid") == parent_launch.get("pid")
                and parent_exit.get("returncode") == 97
                and parent_exit.get("disposition") == "campaign_fatal"
            )
            registered_valid = (
                parent_status == "registered_prelaunch"
                and parent_launch is None
                and parent_exit is None
            )
            if (parent_worker_id not in recovered_worker_ids
                    or _worker_lease_is_held(out_dir, sample)
                    or parent_metadata
                    or api_rows_by_worker.get(parent_worker_id)
                    or not (preauthorization_valid or registered_valid)):
                raise RuntimeError(
                    "queued resume parent-loss preauthorization evidence "
                    f"is invalid: {sample}")
            allowed.add(sample)
            _record_pending_provenance(
                provenance_out, sample, PENDING_INTERRUPTED)
            continue
        metadata = latest_metadata.get(sample)
        worker_id = (
            metadata.get("worker_launch_id")
            if isinstance(metadata, dict) else None
        )
        exit_row = exits_by_worker.get(worker_id)
        if (not isinstance(metadata, dict)
                or not isinstance(worker_id, str) or not worker_id
                or worker_id not in recovered_worker_ids
                or _worker_lease_is_held(out_dir, sample)
                or not isinstance(exit_row, dict)
                or exit_row.get("sample") != sample
                or exit_row.get("disposition") != "campaign_fatal"):
            raise RuntimeError(
                "queued resume cannot prove pending sample was never "
                f"started: {sample}; evidence={evidence}"
            )
        status = metadata.get("status")
        recovered_worker = recovered_workers_by_sample.get(sample)
        recovered_identity = {
            key: metadata.get(key)
            for key in (
                "sample", "status", "worker_launch_id", "worker_pid",
                "invocation_id",
            )
        }
        recovered_identity["sample"] = sample
        if recovered_worker is not None:
            if recovered_worker != recovered_identity:
                raise RuntimeError(
                    "queued resume recovered worker identity is invalid: "
                    f"{sample}"
                )
            if status == "interrupted_by_dispatcher":
                allowed.add(sample)
                _record_pending_provenance(
                    provenance_out, sample, PENDING_INTERRUPTED)
                continue
            if status == "failed":
                allowed.add(sample)
                _record_pending_provenance(
                    provenance_out, sample,
                    PENDING_INFRASTRUCTURE_INCOMPLETE)
                continue
        if status == "interrupted_by_dispatcher":
            allowed.add(sample)
            _record_pending_provenance(
                provenance_out, sample, PENDING_INTERRUPTED)
            continue
        if status == "failed":
            failed_rows = [
                row for row in api_rows_by_worker.get(worker_id, [])
                if row.get("sample") == sample
                and _deepseek_failed_retry_row(row)
            ]
            authorized_disconnect_rows = [
                row for row in api_rows_by_worker.get(worker_id, [])
                if (row.get("sample") == sample
                    and api_incident_kinds.get(
                        _canonical_record_sha256(row))
                    == (
                        "deepseek_transport_disconnect_"
                        "misclassification"
                    )
                    and row.get("classification") == "runner_exception"
                    and row.get("error_type") == "transport_disconnect"
                    and row.get("http_status") is None
                    and row.get("provider_called") is True
                    and row.get("stream_complete") is False
                    and row.get("response_replayed") is False
                    and row.get("count_as_method_failure") is True
                    and isinstance(row.get("transport_attempts"), list)
                    and len(row.get("transport_attempts")) == 1
                    and row["transport_attempts"][0].get("status")
                    == "fatal_error")
            ]
            if (len(failed_rows) == 1
                    or len(authorized_disconnect_rows) == 1):
                allowed.add(sample)
                _record_pending_provenance(
                    provenance_out, sample,
                    PENDING_INFRASTRUCTURE_INCOMPLETE)
                continue
        raise RuntimeError(
            "queued resume cannot prove pending sample was never "
            f"started: {sample}; evidence={evidence}"
        )
    return allowed


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
        paths = [
            *[Path(path) for path in snapshot_docs_sample_dirs(
                out_dir, method_phase, sample)],
            Path(out_dir, "api_raw", method_phase, safe_sample),
            Path(out_dir, "logs", f"{method_phase}__{safe_sample}.log"),
        ]
        for path in paths:
            if path.exists():
                evidence[sample].append(os.path.relpath(path, out_dir))
        dispatch_dir = Path(out_dir, "dispatch_logs")
        if dispatch_dir.is_dir():
            for path in dispatch_dir.glob(
                    f"{safe_sample}__*__{method_phase}.console.log"):
                evidence[sample].append(os.path.relpath(path, out_dir))

    for filename in (
            "sample_outcomes.jsonl", "api_calls.jsonl",
            "api_anomalies.jsonl", "evaluator_incomplete_samples.jsonl"):
        for row_number, record in enumerate(
                _read_jsonl(os.path.join(out_dir, filename)), 1):
            if not _record_mentions_method(record, method_phase):
                continue
            for sample in samples:
                if _record_mentions_sample(record, sample):
                    evidence[sample].append(f"{filename}:{row_number}")
    metadata_source = (
        "run_metadata_events.jsonl:projection"
        if os.path.isfile(os.path.join(out_dir, "run_metadata_events.jsonl"))
        else "run_metadata.jsonl"
    )
    for row_number, record in enumerate(
            read_run_metadata_snapshot(out_dir), 1):
        if not _record_mentions_method(record, method_phase):
            continue
        for sample in samples:
            if _record_mentions_sample(record, sample):
                evidence[sample].append(
                    f"{metadata_source}:{row_number}")

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


def _verified_deepseek_infrastructure_incomplete(
        out_dir, sample, item, outcome, *, require_current_progress=True,
        metadata_snapshot=None, api_snapshot=None,
        allow_active_running_metadata=False):
    """Verify one bounded OpenAI-compatible retry exhaustion."""
    runtime_identity = _deepseek_campaign_runtime_identity(out_dir)
    if runtime_identity is None:
        raise RuntimeError(
            f"worker {sample} DeepSeek campaign runtime is invalid")
    worker_id = item["worker_launch_id"]
    process = item.get("process")
    worker_pid = (
        process.pid if process is not None else item.get("worker_pid")
    )
    methods = item.get("methods") or []
    target_round_trips = item.get("target_round_trips")
    failure_method = outcome.get("method")
    failure_rt = outcome.get("rt_index")
    direction = outcome.get("direction")
    request_id = outcome.get("request_id")
    invocation_id = outcome.get("invocation_id")
    if (failure_method not in methods
            or not _is_exact_int(failure_rt) or failure_rt < 1
            or (_is_exact_int(target_round_trips)
                and failure_rt > target_round_trips)
            or direction not in {"forward", "backward"}
            or not isinstance(request_id, str) or not request_id
            or not isinstance(invocation_id, str) or not invocation_id):
        raise RuntimeError(
            f"worker {sample} DeepSeek failure identity is invalid")

    recorded_progress = outcome.get("checkpoint_progress")
    if require_current_progress:
        actual_progress = _actual_sample_progress(out_dir, sample, methods)
        if recorded_progress != actual_progress:
            raise RuntimeError(
                f"worker {sample} DeepSeek checkpoint evidence drift")
    else:
        actual_progress = recorded_progress
    if (not isinstance(actual_progress, dict)
            or set(actual_progress) != set(methods)):
        raise RuntimeError(
            f"worker {sample} DeepSeek checkpoint evidence is invalid")
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
                    f"worker {sample} DeepSeek method-order mismatch")

    metadata_records = (
        read_run_metadata_snapshot(out_dir)
        if metadata_snapshot is None else metadata_snapshot
    )
    metadata = [
        record for record in metadata_records
        if record.get("invocation_id") == invocation_id
    ]
    allowed_metadata_statuses = {"infrastructure_incomplete"}
    if allow_active_running_metadata:
        allowed_metadata_statuses.add("running")
    if (len(metadata) != 1
            or metadata[0].get("status") not in allowed_metadata_statuses
            or metadata[0].get("worker_launch_id") != worker_id
            or metadata[0].get("worker_pid") != worker_pid
            or metadata[0].get("samples") != [sample]):
        raise RuntimeError(
            f"worker {sample} DeepSeek run metadata mismatch")

    api_records = (
        _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
        if api_snapshot is None else api_snapshot
    )
    api_matches = [
        (index, row) for index, row in enumerate(api_records, 1)
        if row.get("request_id") == request_id
    ]
    if len(api_matches) != 1:
        raise RuntimeError(
            f"worker {sample} DeepSeek API evidence is not unique")
    api_index, api_row = api_matches[0]
    attempts = api_row.get("transport_attempts")
    if (not _deepseek_failed_retry_row(api_row)
            or api_row.get("sample") != sample
            or api_row.get("method") != failure_method
            or api_row.get("rt_index") != failure_rt
            or api_row.get("direction") != direction
            or api_row.get("call_kind") != outcome.get("call_kind")
            or api_row.get("error_type") != outcome.get("error_type")
            or api_row.get("worker_launch_id") != worker_id
            or api_row.get("worker_pid") != worker_pid
            or (
                api_row.get("transport"),
                api_row.get("transport_revision"),
            ) != runtime_identity
            or not isinstance(attempts, list) or not attempts
            or api_row.get("http_attempts_used") != len(attempts)
            or outcome.get("http_attempts_used") != len(attempts)
            or outcome.get("next_attempt_index") != len(attempts) + 1
            or not _valid_deepseek_transport_sidecar(out_dir, api_row)):
        raise RuntimeError(
            f"worker {sample} DeepSeek retry evidence mismatch")
    return {
        "sample_outcome_created_at": outcome.get("created_at"),
        "invocation_id": invocation_id,
        "request_id": request_id,
        "worker_launch_id": worker_id,
        "worker_pid": worker_pid,
        "api_row": api_index,
        "http_attempts_used": len(attempts),
        "checkpoint_progress": actual_progress,
    }


def _deepseek_monthly_quota_quarantine_evidence(out_dir, sample, item):
    if _deepseek_campaign_runtime_identity(out_dir) is None:
        return None
    try:
        outcome_rows = read_sample_outcomes(out_dir)
        outcome = _latest_sample_outcomes(
            out_dir, method_phase=item.get("method_phase"),
            records=outcome_rows).get(sample) or {}
        if outcome.get("status") != "infrastructure_incomplete":
            return None
        evidence = _verified_deepseek_infrastructure_incomplete(
            out_dir, sample, item, outcome)
        api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
        api_index = evidence.get("api_row")
        if (not _is_exact_int(api_index)
                or not 1 <= api_index <= len(api_rows)):
            return None
        api_row = api_rows[api_index - 1]
        outcome_index = next(
            (index for index, row in enumerate(outcome_rows, 1)
             if row is outcome), None)
        if not _is_exact_int(outcome_index):
            return None
    except RuntimeError:
        return None
    if not _deepseek_monthly_usage_limit_row(api_row):
        return None
    attempts = api_row.get("transport_attempts") or []
    return {
        "sample": sample,
        "request_id": api_row.get("request_id"),
        "worker_launch_id": item.get("worker_launch_id"),
        "api_row": api_index,
        "api_row_count": len(api_rows),
        "api_row_sha256": _canonical_record_sha256(api_row),
        "sample_outcome_row": outcome_index,
        "sample_outcome_sha256": _canonical_record_sha256(outcome),
        "http_attempt_count": len(attempts),
        "sample_outcome_created_at": evidence.get(
            "sample_outcome_created_at"),
    }


def _verified_deepseek_uncommitted_incident_request_ids(
        out_dir, outcome, evidence, api_records, committed_call_ids):
    """Return every uncommitted API row from the failed RT invocation."""
    worker_id = evidence["worker_launch_id"]
    worker_pid = evidence["worker_pid"]
    sample = outcome["sample"]
    failure_method = outcome["method"]
    failure_rt = outcome["rt_index"]
    failure_direction = outcome["direction"]
    failure_kind = outcome["call_kind"]
    terminal_request_id = evidence["request_id"]
    runtime_identity = _deepseek_campaign_runtime_identity(out_dir)
    if runtime_identity is None:
        raise RuntimeError(
            f"worker {sample} DeepSeek incident runtime is invalid")

    worker_rows = [
        (index, row) for index, row in enumerate(api_records, 1)
        if row.get("worker_launch_id") == worker_id
    ]
    if (not worker_rows
            or worker_rows[-1][1].get("request_id")
            != terminal_request_id):
        raise RuntimeError(
            f"worker {sample} DeepSeek terminal call is not last")
    cohort = [
        (index, row) for index, row in worker_rows
        if row.get("request_id") not in committed_call_ids
    ]
    if (not cohort
            or cohort[-1][1].get("request_id")
            != terminal_request_id):
        raise RuntimeError(
            f"worker {sample} DeepSeek incident cohort is invalid")

    direction_rank = {"forward": 0, "backward": 1}
    allowed_kinds = (
        {"hybridpatch_primary": 0, "hybridpatch_repair": 1}
        if failure_method == "hybridpatch"
        else {"fullrewrite_primary": 0}
    )
    failure_position = (
        direction_rank[failure_direction],
        allowed_kinds.get(failure_kind),
    )
    if failure_position[1] is None:
        raise RuntimeError(
            f"worker {sample} DeepSeek failure call kind is invalid")

    request_ids = set()
    positions = []
    for api_index, row in cohort:
        request_id = row.get("request_id")
        direction = row.get("direction")
        call_kind = row.get("call_kind")
        position = (
            direction_rank.get(direction),
            allowed_kinds.get(call_kind),
        )
        if (not isinstance(request_id, str) or not request_id
                or request_id in request_ids
                or position[0] is None or position[1] is None
                or position > failure_position
                or row.get("sample") != sample
                or row.get("method") != failure_method
                or row.get("rt_index") != failure_rt
                or row.get("worker_pid") != worker_pid
                or row.get("model") != DEEPSEEK_MODEL
                or (
                    row.get("transport"),
                    row.get("transport_revision"),
                ) != runtime_identity
                or row.get("provider_called") is not True
                or row.get("response_replayed") is not False
                or row.get("generation_index") != 0
                or not _valid_deepseek_transport_sidecar(out_dir, row)):
            raise RuntimeError(
                f"worker {sample} DeepSeek incident row {api_index} "
                "is invalid")
        if request_id == terminal_request_id:
            valid_terminal = _deepseek_failed_retry_row(row)
            valid_success = False
        else:
            valid_terminal = False
            valid_success = (
                row.get("classification") in {
                    None, "transport-valid but model-empty"}
                and row.get("stream_complete") is True
                and row.get("input_tokens") is not None
                and row.get("output_tokens") is not None
                and _valid_deepseek_retry_evidence(row)
            )
        if not (valid_terminal or valid_success):
            raise RuntimeError(
                f"worker {sample} DeepSeek incident row {api_index} "
                "has invalid terminal state")
        request_ids.add(request_id)
        positions.append(position)

    if (positions != sorted(positions)
            or len(positions) != len(set(positions))
            or positions[-1] != failure_position
            or (0, 0) not in positions
            or (
                failure_direction == "backward"
                and (1, 0) not in positions
            )
            or (
                failure_kind.endswith("_repair")
                and (direction_rank[failure_direction], 0)
                not in positions
            )):
        raise RuntimeError(
            f"worker {sample} DeepSeek incident call sequence is invalid")
    return request_ids


def _verified_deepseek_infrastructure_request_ids(
        out_dir, config, *, metadata_snapshot=None, api_snapshot=None,
        committed_call_ids=None, outcome_snapshot=None,
        active_samples=None, active_worker_rows=None,
        launch_snapshot=None):
    """Audit every historical sample-local retry exhaustion, not only latest."""
    expected_samples = set(config.get("samples") or [])
    outcome_records = (
        read_sample_outcomes(out_dir)
        if outcome_snapshot is None else outcome_snapshot
    )
    # This validates ordering, status, timestamps, and campaign membership for
    # the whole append-only outcome ledger before any historical exemption is
    # granted to an API row.
    _latest_sample_outcomes(
        out_dir, expected_samples, records=outcome_records)
    api_records = (
        _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
        if api_snapshot is None else api_snapshot
    )
    if committed_call_ids is None:
        committed_call_ids = set()
        for sample in expected_samples:
            for method in config.get("method_set") or []:
                for row in _read_jsonl(os.path.join(
                        out_dir, method, f"{sample}.jsonl")):
                    committed_call_ids.update(row.get("api_call_ids") or [])
    else:
        committed_call_ids = set(committed_call_ids)
    request_ids = set()
    active_samples = set(active_samples or [])
    active_worker_rows = active_worker_rows or {}
    launch_snapshot = launch_snapshot or {}
    for index, outcome in enumerate(outcome_records, 1):
        if outcome.get("status") != "infrastructure_incomplete":
            continue
        sample = outcome.get("sample")
        methods = outcome.get("methods")
        worker_id = outcome.get("worker_launch_id")
        worker_pid = outcome.get("worker_pid")
        if (sample not in expected_samples
                or not isinstance(methods, list) or not methods
                or len(methods) != len(set(methods))
                or set(methods) != set(config.get("method_set") or [])
                or not isinstance(worker_id, str) or not worker_id
                or not _is_exact_int(worker_pid) or worker_pid <= 0
                or outcome.get("classification")
                != "provider/API failure"
                or outcome.get("method_phase") is not None):
            raise RuntimeError(
                f"invalid DeepSeek infrastructure outcome row {index}")
        evidence = _verified_deepseek_infrastructure_incomplete(
            out_dir,
            sample,
            {
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "methods": methods,
                "target_round_trips": config.get("num_round_trips"),
            },
            outcome,
            require_current_progress=False,
            metadata_snapshot=metadata_snapshot,
            api_snapshot=api_records,
            allow_active_running_metadata=(
                sample in active_samples
                and isinstance(active_worker_rows.get(worker_id), dict)
                and active_worker_rows[worker_id].get("sample") == sample
                and isinstance(launch_snapshot.get(worker_id), dict)
                and launch_snapshot[worker_id].get("sample") == sample
                and launch_snapshot[worker_id].get("pid") == worker_pid
            ),
        )
        incident_request_ids = (
            _verified_deepseek_uncommitted_incident_request_ids(
                out_dir, outcome, evidence, api_records,
                committed_call_ids)
        )
        if request_ids & incident_request_ids:
            raise RuntimeError(
                "duplicate DeepSeek infrastructure incident request IDs")
        request_ids.update(incident_request_ids)
    return request_ids


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
    if _deepseek_campaign_runtime_identity(out_dir) is not None:
        return _verified_deepseek_infrastructure_incomplete(
            out_dir, sample, item, outcome)
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


def _verified_evaluator_incomplete(
        out_dir, sample, item, *, allow_campaign_stop=False,
        allow_active_running_metadata=False):
    """Verify a sample-local evaluator exception without accepting score rows."""
    if not allow_campaign_stop and read_campaign_stop_conditions(out_dir):
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
    allowed_metadata_statuses = {"evaluator_incomplete"}
    if allow_active_running_metadata:
        allowed_metadata_statuses.add("running")
    if (len(metadata) != 1
            or metadata[0].get("status") not in allowed_metadata_statuses
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
        allow_audited_interrupted=False, task_plans=None,
        allow_deepseek_resume=False):
    """Select all samples for a new campaign, only incomplete ones on resume."""
    if not resume:
        if read_sample_outcomes(out_dir):
            raise RuntimeError(
                "new campaign directory already contains sample outcomes")
        return [
            _mark_proven_never_started_assignment(item)
            for item in assignments
        ], {}
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
        stale = latest.get(sample)
        if (isinstance(stale, dict)
                and stale.get("status") not in {
                    "finished", "evaluator_incomplete"}
                and stale.get("worker_launch_id")
                in recovery_incidents["worker_launch_ids"]):
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
    if missing and allow_deepseek_resume:
        pending_provenance = {}
        allowed = _verified_deepseek_resume_missing_samples_with_provenance(
            out_dir, missing_assignments,
            provenance_out=pending_provenance)
        missing_assignments = [
            item for item in missing_assignments
            if item["sample"] not in allowed
        ]
        missing = [item["sample"] for item in missing_assignments]
    else:
        pending_provenance = {}
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
        for sample in missing:
            pending_provenance[sample] = PENDING_NEVER_STARTED
    selected = []
    authorizations = {}
    for item in assignments:
        sample = item["sample"]
        if sample not in latest:
            selected_item = dict(item)
            if sample in interrupted_evidence:
                selected_item["interrupted_resume_evidence"] = (
                    interrupted_evidence[sample])
                pending_provenance[sample] = PENDING_INTERRUPTED
            if sample in provider_access_retries:
                pending_provenance[sample] = (
                    PENDING_INFRASTRUCTURE_INCOMPLETE)
            provenance = pending_provenance.get(sample)
            if provenance is None:
                raise RuntimeError(
                    "queued resume missing pending provenance: "
                    f"{sample}")
            selected_item = _mark_pending_provenance_assignment(
                selected_item, provenance)
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
        if (allow_deepseek_resume
                and "generation_index" not in evidence):
            selected.append(_mark_pending_provenance_assignment(
                item, PENDING_INFRASTRUCTURE_INCOMPLETE))
            continue
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


def _sha256_path(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_active_worker_set(out_dir, manifest, workers):
    workers = list(workers)
    dispatcher_identities = {
        (
            item.get("dispatcher_pid"),
            item.get("dispatcher_instance_id"),
        )
        for item in workers
        if (item.get("dispatcher_pid") is not None
            or item.get("dispatcher_instance_id") is not None)
    }
    if len(dispatcher_identities) > 1:
        raise RuntimeError(
            "active workers span multiple dispatcher instances")
    dispatcher_pid, dispatcher_instance_id = (
        next(iter(dispatcher_identities))
        if dispatcher_identities else (None, None)
    )
    if dispatcher_identities and (
            not _is_exact_int(dispatcher_pid) or dispatcher_pid <= 0
            or not isinstance(dispatcher_instance_id, str)
            or not dispatcher_instance_id):
        raise RuntimeError("active dispatcher identity is invalid")
    record = {
        "schema": "anchorpatch.active_worker_set/1",
        "run_git_commit": manifest["run_git_commit"],
        "dispatcher_pid": dispatcher_pid,
        "dispatcher_instance_id": dispatcher_instance_id,
        "workers": {
            item["worker_launch_id"]: {"sample": item["sample"]}
            for item in workers
        },
    }
    write_json_atomic(_active_worker_set_path(out_dir), record)
    return record


def _active_worker_launch_ids_from_records(workers):
    ids = []
    for item in workers:
        worker_id = item.get("worker_launch_id") if isinstance(item, dict) else None
        if isinstance(worker_id, str) and worker_id:
            ids.append(worker_id)
    return sorted(set(ids))


def _remember_active_worker_set(args, workers, record, path):
    workers = list(workers)
    ids = _active_worker_launch_ids_from_records(workers)
    args._operator_pause_active_worker_launch_ids = ids
    args._operator_pause_active_worker_count = len(ids)
    if not os.path.isfile(path):
        if (_supports_audited_operator_pause(args)
                and _operator_pause_state(args) is not None):
            raise RuntimeError(
                "active worker set was not durably published")
        return
    args._operator_pause_active_worker_set_sha256 = _sha256_path(path)
    args._operator_pause_active_worker_set_canonical_sha256 = (
        _canonical_record_sha256(record)
    )


def _publish_active_worker_set(out_dir, manifest, workers, args=None):
    workers = list(workers)
    record = _write_active_worker_set(out_dir, manifest, workers)
    if args is not None:
        _remember_active_worker_set(
            args, workers, record, _active_worker_set_path(out_dir))
    return record


def _operator_pause_active_worker_ids(args, running):
    cached = getattr(args, "_operator_pause_active_worker_launch_ids", None)
    if cached is not None:
        return list(cached)
    return _active_worker_launch_ids_from_records(running.values())


def _record_operator_pause_condition(
        out_dir, args, exc, inspection_manifest, running):
    active_ids = _operator_pause_active_worker_ids(args, running)
    dispatcher_instance_id = getattr(args, "_dispatcher_instance_id", None)
    manifest_sha256, active_set_sha256, active_set_canonical_sha256 = (
        _operator_pause_evidence_digests(args))
    details = {
        "reason": f"dispatcher received {exc.signal_name}",
        "signal_name": exc.signal_name,
        "signal_number": exc.signum,
        "exit_code": exc.exit_code,
        "boundary": exc.boundary,
        "stopped_git_commit": inspection_manifest["run_git_commit"],
        "stopped_git_tree_state": inspection_manifest.get(
            "git_tree_state", "clean"),
        "git_status_porcelain": "",
        "dispatcher_pid": os.getpid(),
        "dispatcher_instance_id": dispatcher_instance_id,
        "active_worker_launch_ids": active_ids,
        "active_worker_count": len(active_ids),
        "dispatch_manifest_sha256": manifest_sha256,
        "active_worker_set_sha256": active_set_sha256,
        "active_worker_set_canonical_sha256": active_set_canonical_sha256,
        "publication_mode": "canonical",
    }
    record = record_campaign_stop_condition(
        out_dir, OPERATOR_PAUSE_CONDITION, **details)
    _validate_operator_pause_stop_record(
        record, exc, dispatcher_instance_id, active_ids, manifest_sha256,
        active_set_sha256, active_set_canonical_sha256)
    return record


def _record_operator_pause_emergency_condition(
        out_dir, args, exc, inspection_manifest, running,
        canonical_publication_error):
    active_ids = _operator_pause_active_worker_ids(args, running)
    dispatcher_instance_id = getattr(args, "_dispatcher_instance_id", None)
    manifest_sha256, active_set_sha256, active_set_canonical_sha256 = (
        _operator_pause_evidence_digests(args))
    details = {
        "reason": f"dispatcher received {exc.signal_name}",
        "signal_name": exc.signal_name,
        "signal_number": exc.signum,
        "exit_code": exc.exit_code,
        "boundary": exc.boundary,
        "stopped_git_commit": inspection_manifest["run_git_commit"],
        "stopped_git_tree_state": inspection_manifest.get(
            "git_tree_state", "clean"),
        "git_status_porcelain": "",
        "dispatcher_pid": os.getpid(),
        "dispatcher_instance_id": dispatcher_instance_id,
        "active_worker_launch_ids": active_ids,
        "active_worker_count": len(active_ids),
        "dispatch_manifest_sha256": manifest_sha256,
        "active_worker_set_sha256": active_set_sha256,
        "active_worker_set_canonical_sha256": active_set_canonical_sha256,
        "publication_mode": "emergency_fallback",
        "canonical_publication_error": str(canonical_publication_error),
    }
    record = record_emergency_campaign_stop_condition(
        out_dir, OPERATOR_PAUSE_CONDITION, **details)
    _validate_operator_pause_stop_record(
        record, exc, dispatcher_instance_id, active_ids, manifest_sha256,
        active_set_sha256, active_set_canonical_sha256)
    durable_records = read_campaign_stop_conditions(out_dir)
    record_digest = _canonical_record_sha256(record)
    canonical_path = os.path.join(out_dir, "campaign_stop.json")
    canonical_record = (
        _read_json(canonical_path) if os.path.isfile(canonical_path) else None
    )
    if (not isinstance(canonical_record, dict)
            or _canonical_record_sha256(canonical_record) != record_digest
            or not durable_records
            or any(
                _canonical_record_sha256(item) != record_digest
                for item in durable_records
            )):
        raise RuntimeError(
            "operator pause emergency stop did not become the unique durable "
            "campaign boundary")
    return record


def _operator_pause_evidence_digests(args):
    if os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID"):
        raise RuntimeError(
            "dispatcher operator pause cannot inherit worker identity")
    values = (
        getattr(args, "_operator_pause_dispatch_manifest_sha256", None),
        getattr(args, "_operator_pause_active_worker_set_sha256", None),
        getattr(
            args, "_operator_pause_active_worker_set_canonical_sha256", None),
    )
    if any(
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in values):
        raise RuntimeError(
            "operator pause evidence digests are not initialized")
    return values


def _validate_operator_pause_stop_record(
        record, exc, dispatcher_instance_id, active_ids, manifest_sha256,
        active_set_sha256, active_set_canonical_sha256):
    if record.get("condition") != OPERATOR_PAUSE_CONDITION:
        raise RuntimeError(
            "operator pause cannot supersede existing campaign stop: "
            + str(record.get("condition") or "unknown")
        )
    if (record.get("signal_name") != exc.signal_name
            or record.get("signal_number") != exc.signum
            or record.get("exit_code") != exc.exit_code
            or record.get("boundary") != exc.boundary
            or record.get("dispatcher_pid") != os.getpid()
            or record.get("dispatcher_instance_id")
            != dispatcher_instance_id
            or record.get("worker_launch_id") is not None
            or record.get("active_worker_launch_ids") != active_ids
            or record.get("active_worker_count") != len(active_ids)
            or record.get("dispatch_manifest_sha256") != manifest_sha256
            or record.get("active_worker_set_sha256") != active_set_sha256
            or record.get("active_worker_set_canonical_sha256")
            != active_set_canonical_sha256):
        raise RuntimeError(
            "operator pause stop identity does not match this dispatcher")
    return record


def _assert_no_active_campaign_stop_before_launch(out_dir):
    stop_records = read_campaign_stop_conditions(out_dir)
    if stop_records:
        conditions = sorted({
            str(record.get("condition") or "unknown")
            for record in stop_records
        })
        raise RuntimeError(
            "active campaign stop requires offline recovery authorization "
            "before dispatcher launch: " + ", ".join(conditions)
        )
    active_path = _active_worker_set_path(out_dir)
    if not os.path.isfile(active_path):
        return
    active = _read_json(active_path)
    workers = active.get("workers") if isinstance(active, dict) else None
    if (active.get("schema") != "anchorpatch.active_worker_set/1"
            or not isinstance(active.get("run_git_commit"), str)
            or not active.get("run_git_commit")
            or not isinstance(workers, dict)
            or (
                not workers
                and (
                    active.get("dispatcher_pid") is not None
                    or active.get("dispatcher_instance_id") is not None
                )
            )):
        raise RuntimeError(
            "active worker witness is invalid; offline recovery is required")
    if workers:
        raise RuntimeError(
            "active worker witness requires offline recovery authorization "
            "before dispatcher launch")


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


def _worker_start_ack(manifest, sample, item, ready, task_plan_sha256):
    mode = (
        _provider_guard_mode(manifest)
        if isinstance(manifest, dict) else PROVIDER_GUARD_ACTIVE_SET_V1)
    base = {
        "worker_launch_id": item["worker_launch_id"],
        "worker_pid": item["process"].pid,
        "invocation_id": ready["invocation_id"],
        "sample": sample,
        "task_plan_sha256": task_plan_sha256,
    }
    if mode == PROVIDER_GUARD_ACTIVE_SET_V1:
        return {"schema": "anchorpatch.worker_start/1", **base}
    runtime_git_commit = item.get("runtime_git_commit")
    if not isinstance(runtime_git_commit, str) or not runtime_git_commit:
        raise RuntimeError("worker capability runtime Git identity is missing")
    return {
        "schema": WORKER_START_CAPABILITY_SCHEMA,
        **base,
        "provider_guard_mode": mode,
        "dispatcher_pid": item.get("dispatcher_pid"),
        "dispatcher_instance_id": item.get("dispatcher_instance_id"),
        "run_git_commit": runtime_git_commit,
        "transport_revision": (manifest.get("config") or {}).get(
            "transport_revision"),
        "dispatch_manifest_canonical_sha256": (
            _canonical_record_sha256(manifest)),
    }


def _write_or_verify_worker_start_ack(path, ack):
    if os.path.isfile(path):
        if _read_json(path) != ack:
            raise RuntimeError("existing worker start capability differs")
        return
    write_json_atomic(path, ack)
    if _read_json(path) != ack:
        raise RuntimeError("worker start capability publication mismatch")


def _authorize_workers(out_dir, running, task_plans, dispatch_log,
                       timeout_seconds, *, pause_check=None, manifest=None):
    """Release workers only after lease/PID/metadata/plan identity closes."""
    deadline = time.monotonic() + timeout_seconds
    ready_by_sample = {}
    while len(ready_by_sample) != len(running):
        if pause_check is not None:
            pause_check("worker_authorization_wait")
        stop_records = read_campaign_stop_conditions(out_dir)
        if stop_records:
            raise RuntimeError(
                "campaign stop latch set before worker authorization: "
                f"{stop_records[0].get('condition')}"
            )
        metadata_by_invocation = None
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
            if metadata_by_invocation is None:
                metadata_by_invocation = {}
                for record in read_run_metadata_snapshot(out_dir):
                    metadata_by_invocation.setdefault(
                        record.get("invocation_id"), []).append(record)
            matches = [
                record for record in metadata_by_invocation.get(
                    invocation_id, [])
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
            metadata_record = matches[0]
            if (metadata_record.get("task_plans") or {}).get(sample) != {
                    "sha256": expected_plan,
                    "round_trips": len(task_plans[sample][
                        "forward_state_sequence"]),
            }:
                raise RuntimeError(
                    f"worker task-plan handshake mismatch: {sample}"
                )
            if "key_label" in item:
                launch_key_fields = _worker_key_launch_fields(item)
                expected_key_metadata = {
                    "dispatch_key_label": launch_key_fields["key_label"],
                    "original_key_label": (
                        launch_key_fields["original_key_label"]),
                    "prior_key_label": launch_key_fields["prior_key_label"],
                    "failover_count": launch_key_fields["failover_count"],
                    "failover_reason": (
                        launch_key_fields["failover_reason"]),
                }
                if any(
                        metadata_record.get(key) != value
                        for key, value in expected_key_metadata.items()):
                    raise RuntimeError(
                        f"worker key failover handshake mismatch: {sample}")
            expected_resume = item.get("resume_authorization")
            metadata_resume = metadata_record.get(
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
            if metadata_record.get(
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
    authorization_records = []
    if pause_check is not None:
        pause_check("before_worker_authorization_records")
    for sample, item in running.items():
        ready = ready_by_sample[sample]
        ack = _worker_start_ack(
            manifest, sample, item, ready, task_plans[sample]["sha256"])
        authorization_record = {
            "event": "worker_authorized",
            "created_at": datetime.now().astimezone().isoformat(
                timespec="seconds"),
            "transport_resume_authorization": item.get(
                "resume_authorization"),
            "interrupted_resume_evidence": item.get(
                "interrupted_resume_evidence"),
            "dispatcher_pid": item.get("dispatcher_pid"),
            "dispatcher_instance_id": item.get(
                "dispatcher_instance_id"),
            **ack,
        }
        if ack["schema"] == WORKER_START_CAPABILITY_SCHEMA:
            authorization_record["worker_start_capability_sha256"] = (
                _canonical_record_sha256(ack))
        authorization_records.append(authorization_record)
        acknowledgements[sample] = ack

    append_jsonl_records_locked(dispatch_log, authorization_records)

    # Authorization is a cohort barrier: no worker may observe an ACK until
    # every worker's authorization event has been durably appended.  This
    # prevents an early worker from calling the provider if a later fsync
    # fails.
    if pause_check is not None:
        pause_check("before_worker_ack_publication")
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
    if pause_check is not None:
        pause_check("before_worker_ack_write")
    for sample, item in running.items():
        ack = acknowledgements[sample]
        _write_or_verify_worker_start_ack(item["ack_path"], ack)


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


def _provider_guard_mode(manifest):
    config = manifest.get("config") if isinstance(manifest, dict) else None
    if not isinstance(config, dict):
        # Frozen callers and narrow launch tests predate the config field and
        # retain the active-set guard.  Formal manifests are independently
        # schema-checked before this helper is reached.
        return PROVIDER_GUARD_ACTIVE_SET_V1
    mode = config.get("provider_guard_mode", PROVIDER_GUARD_ACTIVE_SET_V1)
    if mode == PROVIDER_GUARD_ACTIVE_SET_V1:
        return mode
    if (mode == PROVIDER_GUARD_WORKER_START_CAPABILITY_V1
            and config.get("campaign_role") == "deepseek_full234"
            and config.get("num_round_trips") == DEEPSEEK_FULL234_ROUND_TRIPS
            and config.get("transport") == DEEPSEEK_TRANSPORT
            and config.get("transport_revision")
            == DEEPSEEK_TRANSPORT_REVISION
            and config.get("run_metadata_storage")
            == RUN_METADATA_STORAGE_EVENT_V1):
        return mode
    raise RuntimeError("provider guard mode is invalid for this campaign")


def build_manifest(out_dir, samples, assignments, task_plans, args,
                   upstream_smoke_gate=None):
    commit, tree_state = _git_identity()
    if tree_state != "clean":
        raise RuntimeError("formal campaign requires a clean Git worktree")
    runtime = _campaign_runtime_config(args)
    config = {
        "campaign_role": args.campaign_role,
        "samples": list(samples),
        "method_set": ["fullrewrite", "hybridpatch"],
        "num_round_trips": args.num_round_trips,
        "seed": args.seed,
        "model": runtime["model"],
        "max_tokens": runtime["max_tokens"],
        "distractor": True,
        "provider": runtime["provider"],
        "transport": runtime["transport"],
        "transport_revision": runtime["transport_revision"],
        "transport_resume_policy": runtime["transport_resume_policy"],
        "reasoning_effort": runtime["reasoning_effort"],
        "run_metadata_storage": RUN_METADATA_STORAGE_EVENT_V1,
        "stop_on_preservation_violation": True,
        "snapshot_mode": normalize_snapshot_mode(
            _snapshot_mode_arg(args),
            default=SNAPSHOT_MODE_FAILURES),
    }
    for optional in (
            "openai_base_url", "opencode_transport", "minimax_transport"):
        if optional in runtime:
            config[optional] = runtime[optional]
    prior_manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
    if os.path.isfile(prior_manifest_path):
        prior_manifest = _read_json(prior_manifest_path)
        prior_config = (
            prior_manifest.get("config")
            if isinstance(prior_manifest, dict) else None)
        if (isinstance(prior_config, dict)
                and "provider_guard_mode" in prior_config):
            config["provider_guard_mode"] = prior_config[
                "provider_guard_mode"]
    elif (config.get("campaign_role") == "deepseek_full234"
          and config.get("num_round_trips")
          == DEEPSEEK_FULL234_ROUND_TRIPS
          and config.get("transport") == DEEPSEEK_TRANSPORT
          and config.get("transport_revision")
          == DEEPSEEK_TRANSPORT_REVISION):
        config["provider_guard_mode"] = (
            PROVIDER_GUARD_WORKER_START_CAPABILITY_V1)
    _provider_guard_mode({"config": config})
    manifest = {
        "schema": SCHEMA,
        "experiment_id": os.path.basename(os.path.abspath(out_dir)),
        "run_git_commit": commit,
        "git_tree_state": tree_state,
        "code_fingerprint": code_fingerprint(),
        "config": config,
        "assignments": assignments,
        "task_plans": task_plans,
        "upstream_smoke_gate": upstream_smoke_gate,
    }
    queued_roles = {
        "confirmation", "full234", "remaining134",
        "deepseek_capacity15", "deepseek_full234",
    }
    if args.campaign_role in queued_roles:
        if args.campaign_role == "confirmation":
            slots_per_key = CONFIRMATION_SLOTS_PER_KEY
            key_count = CONFIRMATION_KEY_COUNT
        elif args.campaign_role == "full234":
            slots_per_key = FULL234_SLOTS_PER_KEY
            key_count = FULL234_KEY_COUNT
        elif args.campaign_role == "deepseek_capacity15":
            slots_per_key = DEEPSEEK_CAPACITY_SLOTS_PER_KEY
            key_count = len({
                item["key_label"] for item in assignments
            })
        elif args.campaign_role == "deepseek_full234":
            slots_per_key = DEEPSEEK_FULL_SLOTS_PER_KEY
            actual_key_count = len({
                item["key_label"] for item in assignments
            })
            if actual_key_count != DEEPSEEK_FULL_KEY_COUNT:
                raise RuntimeError(
                    "deepseek_full234 manifest requires exactly three keys")
            key_count = DEEPSEEK_FULL_KEY_COUNT
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
    elif args.campaign_role in {"full234", "deepseek_full234"}:
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
    config = value.get("config")
    if isinstance(config, dict):
        config["snapshot_mode"] = normalize_snapshot_mode(
            config.get("snapshot_mode"), default=SNAPSHOT_MODE_ALL)
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


def _read_relay_publication_snapshot(
        out_dir, samples, methods, *, api_snapshot=None):
    """Read a causally consistent live prefix of relay results and API rows.

    A worker publishes each API row before committing the result/checkpoint
    that references it.  Reading those files in reverse publication order
    therefore cannot pair a new result row with an older API-ledger prefix.
    Keep this ordering shared by every provider-specific inspector.
    """
    committed_rows = {}
    for sample in samples:
        for method in methods:
            result_path = os.path.join(
                out_dir, method, f"{sample}.jsonl")
            committed_rows[(sample, method)] = _read_jsonl(result_path)
    api_rows = (
        _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
        if api_snapshot is None else list(api_snapshot)
    )
    return committed_rows, api_rows


def _valid_deepseek_retry_evidence(row):
    attempts = row.get("transport_attempts")
    runtime_identity = (
        row.get("transport"), row.get("transport_revision"))
    is_current_compact = runtime_identity == (
        DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION)
    is_stream = runtime_identity in {
        (DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION),
        (DEEPSEEK_TRANSPORT, DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION),
        (
            DEEPSEEK_TRANSPORT,
            DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
        ),
    }
    if (not isinstance(attempts, list) or not attempts
            or row.get("http_attempts_used") != len(attempts)
            or row.get("retry_count") != len(attempts) - 1
            or row.get("failed_attempt_count") != len(attempts) - 1):
        return False
    terminal = attempts[-1]
    if (terminal.get("status") != "success"
            or terminal.get("http_status") != 200
            or terminal.get("stream_complete") is not True):
        return False
    if (is_stream
            and (
                terminal.get("message_start_seen") is not True
                or terminal.get("message_stop_seen") is not True
                or terminal.get("final_usage_seen") is not True
                or terminal.get("terminal_sequence_valid") is not True
                or (is_current_compact and (
                    not isinstance(terminal.get("finish_reason"), str)
                    or not terminal.get("finish_reason").strip()
                ))
            )):
        return False

    budget_attempt_index = 0
    for expected_index, attempt in enumerate(attempts, 1):
        if attempt.get("attempt_index") != expected_index:
            return False
        if expected_index == len(attempts):
            continue
        if attempt.get("status") != "retryable_error":
            return False
        if (is_current_compact and (
                attempt.get("stream_complete") is not False
                or attempt.get("terminal_sequence_valid") is not False
                or not isinstance(attempt.get("error_type"), str)
                or not attempt.get("error_type"))):
            return False
        if (is_stream
                and not isinstance(
                    attempt.get("generation_delta_seen"), bool)):
            return False
        free_503 = (
            attempt.get("http_status") == 503
            and (
                attempt.get("generation_delta_seen") is False
                if is_stream
                else not attempt.get("generation_delta_seen")
            )
        )
        if attempt.get("retry_budget_consumed") is not (not free_503):
            return False
        if not free_503:
            budget_attempt_index += 1
        if attempt.get("retry_budget_attempt_index") != budget_attempt_index:
            return False

    max_attempts = row.get("max_retries")
    return (
        _is_exact_int(max_attempts)
        and max_attempts >= 1
        and budget_attempt_index < max_attempts
    )


def _exact_nonnegative_int(value):
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
    )


def _worker_terminal_provenance(launch, exit_row, worker_metadata):
    """Return the shared launch/exit/metadata terminal-state proof."""
    terminal = next((
        record for record in reversed(worker_metadata or [])
        if record.get("status") in {
            "finished", "infrastructure_incomplete", "evaluator_incomplete",
        }
    ), (worker_metadata or [{}])[-1])
    created_at = (
        exit_row.get("created_at") if isinstance(exit_row, dict) else None
    )
    timestamp_ok = False
    if isinstance(created_at, str):
        try:
            timestamp_ok = datetime.fromisoformat(created_at).tzinfo is not None
        except ValueError:
            timestamp_ok = False
    basic_exit_ok = (
        isinstance(exit_row, dict)
        and exit_row.get("sample") == launch.get("sample")
        and exit_row.get("pid") == launch.get("pid")
        and isinstance(exit_row.get("returncode"), int)
        and not isinstance(exit_row.get("returncode"), bool)
        and isinstance(terminal, dict)
        and terminal.get("worker_pid") == launch.get("pid")
        and terminal.get("samples") == [launch.get("sample")]
        and timestamp_ok
    )
    if basic_exit_ok and terminal.get("status") == "finished":
        ordinary_ok = (
            exit_row.get("returncode") == 0
            and exit_row.get("disposition") == "finished"
        )
    elif (basic_exit_ok
          and terminal.get("status") in {
              "infrastructure_incomplete", "evaluator_incomplete"}):
        ordinary_ok = (
            exit_row.get("returncode") != 0
            and exit_row.get("disposition") == terminal.get("status")
            and isinstance(exit_row.get("evidence"), dict)
        )
    else:
        ordinary_ok = False
    return {
        "terminal": terminal,
        "timestamp_ok": timestamp_ok,
        "basic_exit_ok": basic_exit_ok,
        "ordinary_ok": ordinary_ok,
    }


def _valid_deepseek_compact_transport_sidecar(row, path):
    digest = hashlib.sha256()
    records = []
    size_bytes = 0
    try:
        with open(path, "rb") as handle:
            for raw_line in handle:
                digest.update(raw_line)
                size_bytes += len(raw_line)
                if not raw_line.strip():
                    continue
                record = json.loads(raw_line.decode("utf-8"))
                if not isinstance(record, dict):
                    return False
                records.append(record)
    except (OSError, UnicodeError, ValueError):
        return False
    if (not records
            or row.get("transport_sidecar_sha256") != digest.hexdigest()
            or row.get("transport_sidecar_size_bytes") != size_bytes
            or row.get("transport_sidecar_record_count") != len(records)):
        return False
    header = records[0]
    expected_header = {
        "transport_event_schema": "anchorpatch.transport_event/2",
        "record_type": "transport_header",
        "transport_revision": DEEPSEEK_TRANSPORT_REVISION,
        "worker_launch_id": row.get("worker_launch_id"),
        "worker_pid": row.get("worker_pid"),
        "sample": row.get("sample"),
        "method": row.get("method"),
        "rt_index": row.get("rt_index"),
        "direction": row.get("direction"),
        "call_id": row.get("request_id"),
    }
    if header != expected_header:
        return False
    attempts = row.get("transport_attempts")
    if not isinstance(attempts, list) or not attempts:
        return False
    try:
        shared = _validate_deepseek_compact_records(
            records,
            expected_linkage={
                key: expected_header[key]
                for key in (
                    "worker_launch_id", "worker_pid", "sample", "method",
                    "rt_index", "direction", "call_id",
                )
            },
        )
    except RuntimeError:
        return False
    if ([item["attempt"] for item in shared["closed_attempts"]]
            != attempts):
        return False
    for item in shared["closed_attempts"]:
        if item["attempt"].get("status") != "success":
            continue
        usage = item["summary"].get("usage")
        if any((
                row.get("prompt_tokens") != usage["prompt_tokens"],
                row.get("completion_tokens") != usage["completion_tokens"],
                row.get("total_tokens") != usage["total_tokens"],
        )):
            return False
    return True



def _valid_deepseek_transport_sidecar(out_dir, row):
    """Require current stream attempts to match the durable linear sidecar."""
    runtime_identity = (
        row.get("transport"), row.get("transport_revision"))
    if runtime_identity == (
            DEEPSEEK_LEGACY_TRANSPORT,
            DEEPSEEK_LEGACY_TRANSPORT_REVISION):
        return True
    if runtime_identity not in {
        (DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION),
        (DEEPSEEK_TRANSPORT, DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION),
        (
            DEEPSEEK_TRANSPORT,
            DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
        ),
    }:
        return False
    path = row.get("raw_sse_saved_path")
    if not isinstance(path, str) or not path or not os.path.isfile(path):
        return False
    try:
        if os.path.commonpath((
                os.path.abspath(out_dir), os.path.abspath(path)
        )) != os.path.abspath(out_dir):
            return False
        if runtime_identity == (
                DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION):
            return _valid_deepseek_compact_transport_sidecar(row, path)
        events = _read_jsonl(path)
    except (OSError, ValueError, RuntimeError):
        return False
    attempts = row.get("transport_attempts")
    if not isinstance(attempts, list) or not attempts:
        return False
    expected_linkage = {
        "worker_launch_id": row.get("worker_launch_id"),
        "worker_pid": row.get("worker_pid"),
        "sample": row.get("sample"),
        "method": row.get("method"),
        "rt_index": row.get("rt_index"),
        "direction": row.get("direction"),
        "call_id": row.get("request_id"),
    }
    try:
        _validate_deepseek_linear_records(
            events,
            expected_linkage=expected_linkage,
            expected_attempts=attempts,
        )
    except RuntimeError:
        return False
    return True



def _deepseek_manifest_original_keys(manifest):
    config = manifest.get("config") or {}
    manifest_samples = list(config.get("samples") or [])
    assignments = manifest.get("assignments") or []
    errors = []
    original_by_sample = {}
    assignment_samples = []
    if not assignments:
        return manifest_samples, original_by_sample, errors
    for item in assignments:
        sample = item.get("sample")
        key_label = item.get("key_label")
        if (not isinstance(sample, str) or not sample
                or not isinstance(key_label, str) or not key_label):
            errors.append("DeepSeek manifest assignment key provenance invalid")
            continue
        assignment_samples.append(sample)
        if sample in original_by_sample:
            errors.append(
                f"duplicate DeepSeek manifest assignment sample: {sample}")
        original_by_sample[sample] = key_label
    if assignment_samples != manifest_samples:
        errors.append("DeepSeek manifest assignment sample/order drift")
    return manifest_samples, original_by_sample, errors


def _inspect_deepseek_key_failover_audit(
        manifest, dispatch_rows, api_rows, outcome_rows, launches):
    """Bind key quarantine/failover events to exact quota evidence."""
    manifest_samples, _manifest_key_by_sample, errors = (
        _deepseek_manifest_original_keys(manifest)
    )
    expected_samples = set(manifest_samples)
    routing_origin_by_sample = {}
    current_key_by_sample = {}
    api_by_request_worker = {}
    for index, row in enumerate(api_rows, 1):
        request_id = row.get("request_id")
        worker_id = row.get("worker_launch_id")
        if (isinstance(request_id, str) and request_id
                and isinstance(worker_id, str) and worker_id):
            api_by_request_worker.setdefault(
                (request_id, worker_id), []).append((index, row))

    quarantined_at = {}
    quota_observations = {}
    key_reactivations = {}
    used_quota_api_rows = set()
    failover_counts = {}
    assigned_failovers = {}
    has_failover_history = any(
        row.get("event") in {
            "key_quarantined", "key_quota_observed",
            "key_failover_assigned", KEY_REACTIVATION_EVENT,
        }
        for row in dispatch_rows
    )
    for event_index, row in enumerate(dispatch_rows, 1):
        event = row.get("event")
        if event in {"key_quarantined", "key_quota_observed"}:
            key_label = row.get("key_label")
            if not isinstance(key_label, str) or not key_label:
                errors.append("key quota observation missing key label")
                continue
            expected_observation = quota_observations.get(key_label, 0) + 1
            if row.get("observation_index") != expected_observation:
                errors.append(
                    f"key quota observation count is not contiguous: {key_label}")
            else:
                quota_observations[key_label] = expected_observation
            if row.get("reason") != DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON:
                errors.append(f"key quota observation reason mismatch: {key_label}")

            if event == "key_quarantined":
                if key_label in quarantined_at:
                    errors.append(f"duplicate active key quarantine: {key_label}")
                expected_quarantined_count = len(quarantined_at) + 1
                if (row.get("quarantine_index")
                        != expected_quarantined_count
                        or row.get("quarantined_key_count")
                        != expected_quarantined_count):
                    errors.append(
                        f"key quarantine count is not contiguous: {key_label}")
            elif key_label not in quarantined_at:
                errors.append(
                    f"key quota observation precedes quarantine: {key_label}")

            trigger_sample = row.get("trigger_sample")
            trigger_worker = row.get("trigger_worker_launch_id")
            trigger_request = row.get("trigger_request_id")
            if (not isinstance(trigger_sample, str) or not trigger_sample
                    or not isinstance(trigger_worker, str)
                    or not trigger_worker
                    or not isinstance(trigger_request, str)
                    or not trigger_request):
                errors.append(
                    f"key quarantine trigger identity invalid: {key_label}")
            launch = launches.get(trigger_worker)
            if (not isinstance(launch, dict)
                    or launch.get("sample") != trigger_sample
                    or launch.get("key_label") != key_label):
                errors.append(
                    f"key quarantine launch provenance invalid: {key_label}")
            matches = api_by_request_worker.get(
                (trigger_request, trigger_worker), [])
            if len(matches) != 1:
                errors.append(
                    f"key quarantine API evidence is not unique: {key_label}")
            else:
                api_index, api_row = matches[0]
                if api_index in used_quota_api_rows:
                    errors.append(
                        f"key quota API evidence reused: {key_label}")
                else:
                    used_quota_api_rows.add(api_index)
                attempts = api_row.get("transport_attempts") or []
                observed_api_count = row.get("api_row_count")
                if (row.get("api_row") != api_index
                        or not _is_exact_int(observed_api_count)
                        or not api_index <= observed_api_count <= len(api_rows)
                        or row.get("api_row_sha256")
                        != _canonical_record_sha256(api_row)
                        or row.get("trigger_http_attempt_count")
                        != len(attempts)):
                    errors.append(
                        f"key quarantine API row/count mismatch: {key_label}")
                if api_row.get("sample") != trigger_sample:
                    errors.append(
                        f"key quarantine API sample mismatch: {key_label}")
                if not _deepseek_monthly_usage_limit_row(api_row):
                    errors.append(
                        f"key quarantine API row is not monthly quota: "
                        f"{key_label}")
            outcome_matches = [
                (index, outcome)
                for index, outcome in enumerate(outcome_rows, 1)
                if outcome.get("sample") == trigger_sample
                and outcome.get("worker_launch_id") == trigger_worker
                and outcome.get("request_id") == trigger_request
                and outcome.get("status") == "infrastructure_incomplete"
                and outcome.get("created_at")
                == row.get("sample_outcome_created_at")
            ]
            if len(outcome_matches) != 1:
                errors.append(
                    f"key quota sample outcome is not unique: {key_label}")
            else:
                outcome_index, outcome = outcome_matches[0]
                if (row.get("sample_outcome_row") != outcome_index
                        or row.get("sample_outcome_sha256")
                        != _canonical_record_sha256(outcome)):
                    errors.append(
                        f"key quota sample outcome mismatch: {key_label}")
            if event == "key_quarantined":
                quarantined_at[key_label] = event_index
            continue

        if event == KEY_REACTIVATION_EVENT:
            key_label = row.get("key_label")
            expected_index = key_reactivations.get(key_label, 0) + 1
            before = sorted(quarantined_at)
            after = sorted(label for label in quarantined_at
                           if label != key_label)
            if (not isinstance(key_label, str) or not key_label
                    or key_label not in quarantined_at
                    or row.get("reactivation_index") != expected_index
                    or row.get("observed_quota_count")
                    != quota_observations.get(key_label, 0)
                    or row.get("quarantined_key_labels_before") != before
                    or row.get("quarantined_key_labels_after") != after
                    or not isinstance(row.get("reason"), str)
                    or not row.get("reason").strip()):
                errors.append(f"key reactivation evidence invalid: {key_label}")
                continue
            key_reactivations[key_label] = expected_index
            del quarantined_at[key_label]
            continue

        if event == "key_failover_assigned":
            sample = row.get("sample")
            from_label = row.get("from_key_label")
            to_label = row.get("to_key_label")
            count = row.get("failover_count")
            if (not isinstance(sample, str) or not sample
                    or not isinstance(from_label, str) or not from_label
                    or not isinstance(to_label, str) or not to_label):
                errors.append("key failover assignment identity invalid")
                continue
            if sample not in expected_samples:
                errors.append(f"key failover sample outside manifest: {sample}")
                continue
            if row.get("reason") != DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON:
                errors.append(f"key failover reason mismatch: {sample}")
            if from_label == to_label:
                errors.append(
                    f"key failover destination matches source: {sample}")
            if (from_label not in quarantined_at
                    or quarantined_at[from_label] >= event_index):
                errors.append(f"key failover before quarantine: {sample}")
            if (to_label in quarantined_at
                    and quarantined_at[to_label] < event_index):
                errors.append(
                    f"key failover destination quarantined: {sample}")
            if sample not in current_key_by_sample and count == 1:
                current_key_by_sample[sample] = from_label
                routing_origin_by_sample[sample] = from_label
            expected_original = routing_origin_by_sample.get(sample)
            if (expected_original is not None
                    and row.get("original_key_label") != expected_original):
                errors.append(f"key failover original key mismatch: {sample}")
            if current_key_by_sample.get(sample) != from_label:
                errors.append(f"key failover source chain mismatch: {sample}")
            expected_count = failover_counts.get(sample, 0) + 1
            if count != expected_count:
                errors.append(f"key failover count is not contiguous: {sample}")
            if _is_exact_int(count) and count > 0:
                failover_counts[sample] = count
                assigned_failovers[(sample, count)] = row
                current_key_by_sample[sample] = to_label
            continue

        if event == "queue_exhausted_no_healthy_key":
            known_keys = sorted(quarantined_at)
            pending_samples = row.get("pending_samples")
            infrastructure_samples = row.get("infrastructure_incomplete_samples")
            not_started_samples = row.get("not_started_pending_samples")
            resume_pending_samples = row.get(
                "resume_pending_no_healthy_key_samples")
            pending_provenance = row.get("pending_sample_provenance")
            if (row.get("reason") != DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON
                    or row.get("quarantined_key_labels") != known_keys
                    or len(known_keys)
                    != (manifest.get("config") or {}).get("key_count")
                    or not isinstance(pending_samples, list)
                    or pending_samples != sorted(set(pending_samples))
                    or not pending_samples
                    or any(sample not in expected_samples
                           for sample in pending_samples)):
                errors.append("queue exhaustion key/sample evidence invalid")
            if (infrastructure_samples is not None
                    and (not isinstance(infrastructure_samples, list)
                         or infrastructure_samples
                         != sorted(set(infrastructure_samples))
                         or not set(infrastructure_samples).issubset(
                             set(pending_samples or [])))):
                errors.append(
                    "queue exhaustion infrastructure evidence invalid")
            if (not_started_samples is not None
                    and (not isinstance(not_started_samples, list)
                         or not_started_samples
                         != sorted(set(not_started_samples))
                         or not set(not_started_samples).issubset(
                             set(pending_samples or [])))):
                errors.append(
                    "queue exhaustion not-started evidence invalid")
            if (resume_pending_samples is not None
                    and (not isinstance(resume_pending_samples, list)
                         or resume_pending_samples
                         != sorted(set(resume_pending_samples))
                         or not set(resume_pending_samples).issubset(
                             set(pending_samples or [])))):
                errors.append(
                    "queue exhaustion resume-pending evidence invalid")
            partition_fields = (
                infrastructure_samples, not_started_samples,
                resume_pending_samples)
            if all(item is not None for item in partition_fields):
                partition_sets = [set(item) for item in partition_fields]
                union = set().union(*partition_sets)
                overlap = sum(len(item) for item in partition_sets) != len(union)
                if overlap or union != set(pending_samples or []):
                    errors.append(
                        "queue exhaustion pending partition is invalid")
                if pending_provenance is not None:
                    expected_provenance = {}
                    for sample in infrastructure_samples:
                        expected_provenance[sample] = (
                            PENDING_INFRASTRUCTURE_INCOMPLETE)
                    for sample in not_started_samples:
                        expected_provenance[sample] = PENDING_NEVER_STARTED
                    for sample in resume_pending_samples:
                        expected_provenance[sample] = PENDING_INTERRUPTED
                    if (not isinstance(pending_provenance, dict)
                            or pending_provenance != expected_provenance):
                        errors.append(
                            "queue exhaustion pending provenance invalid")
            continue

        if event not in {"launch", "launch_intent"}:
            continue
        sample = row.get("sample")
        key_label = row.get("key_label")
        if "failover_count" not in row:
            # Frozen pre-failover archives did not record routing provenance on
            # launch rows.  They remain readable only while the dispatch log
            # contains no durable quarantine/failover state.  Once failover
            # history exists, every launch must participate in the explicit
            # source chain so recovery cannot infer a missing routing epoch.
            if has_failover_history:
                errors.append(f"launch key provenance missing: {sample}")
            continue
        count = row.get("failover_count", 0)
        if (isinstance(key_label, str) and key_label in quarantined_at
                and quarantined_at[key_label] < event_index):
            errors.append(f"launch after key quarantine: {sample}")
        if not _is_exact_int(count) or count < 0:
            errors.append(f"launch failover count invalid: {sample}")
            continue
        if count == 0:
            if sample not in expected_samples:
                errors.append(f"launch sample outside manifest: {sample}")
            if not has_failover_history:
                # Key labels are runtime slots, not immutable campaign
                # identity.  A resume with no durable failover state may
                # rotate KEY_01 to KEY_11.  Validate each routing epoch
                # locally instead of chaining it to an earlier count=0
                # launch.
                current_key_by_sample[sample] = key_label
                routing_origin_by_sample[sample] = key_label
            elif sample not in current_key_by_sample:
                current_key_by_sample[sample] = key_label
                routing_origin_by_sample[sample] = key_label
            if (current_key_by_sample.get(sample) != key_label
                    or row.get("original_key_label") != key_label
                    or row.get("prior_key_label") is not None
                    or row.get("failover_reason") is not None):
                errors.append(f"launch key chain mismatch: {sample}")
            continue
        if (sample in current_key_by_sample
                and current_key_by_sample[sample] != key_label):
            errors.append(f"launch key chain mismatch: {sample}")
        assigned = assigned_failovers.get((sample, count))
        expected_original = routing_origin_by_sample.get(sample)
        if assigned is None:
            errors.append(f"failover launch lacks assignment: {sample}")
            continue
        if (row.get("original_key_label") != expected_original
                or row.get("prior_key_label") != assigned.get("from_key_label")
                or row.get("failover_reason")
                != DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON
                or key_label != assigned.get("to_key_label")):
            errors.append(f"failover launch provenance mismatch: {sample}")
    return errors


def _inspect_deepseek_campaign(
        out_dir, manifest, *, require_complete=False,
        require_terminal_provenance=None, active_samples=None,
        required_complete_samples=None, audit_samples=None,
        terminal_audit_samples=None, preloaded_ledgers=None):
    """Audit a DeepSeek prefix, including frozen /3-/4-/5 and current /6."""
    if require_terminal_provenance is None:
        require_terminal_provenance = require_complete
    config = manifest.get("config") or {}
    expected_samples = set(config.get("samples") or [])
    scoped_audit = audit_samples is not None
    if preloaded_ledgers is not None:
        if (not scoped_audit
                or set(preloaded_ledgers)
                != set(_DEEPSEEK_INCREMENTAL_LEDGER_FILES)
                or any(
                    not isinstance(preloaded_ledgers.get(name), list)
                    or any(not isinstance(row, dict)
                           for row in preloaded_ledgers[name])
                    for name in _DEEPSEEK_INCREMENTAL_LEDGER_FILES
                )):
            raise RuntimeError(
                "preloaded DeepSeek ledgers require a complete scoped snapshot")
    sample_scope = (
        set(audit_samples or []) if scoped_audit else set(expected_samples)
    )
    expected_methods = set(config.get("method_set") or [])
    target_rt = config.get("num_round_trips")
    active_samples = set(active_samples or []) & sample_scope
    completion_samples = (
        expected_samples if require_complete
        else set(required_complete_samples or [])
    )
    terminal_scope = (
        sample_scope
        if terminal_audit_samples is None
        else set(terminal_audit_samples or [])
    )
    errors = []
    preservation = 0
    preservation_not_applicable = 0
    try:
        provider_guard_mode = _provider_guard_mode(manifest)
    except RuntimeError as exc:
        provider_guard_mode = PROVIDER_GUARD_ACTIVE_SET_V1
        errors.append(str(exc))
    if (not sample_scope or not sample_scope <= expected_samples):
        errors.append("DeepSeek audit sample scope is invalid")
    if not completion_samples <= sample_scope:
        errors.append("DeepSeek completion scope exceeds audit sample scope")
    if not terminal_scope <= sample_scope:
        errors.append("DeepSeek terminal scope exceeds audit sample scope")

    runtime_identity = (
        config.get("transport"), config.get("transport_revision")
    )
    supported_runtime_identities = {
        (DEEPSEEK_LEGACY_TRANSPORT, DEEPSEEK_LEGACY_TRANSPORT_REVISION),
        (
            DEEPSEEK_TRANSPORT,
            DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
        ),
        (
            DEEPSEEK_TRANSPORT,
            DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION,
        ),
        (DEEPSEEK_TRANSPORT, DEEPSEEK_TRANSPORT_REVISION),
    }
    if runtime_identity not in supported_runtime_identities:
        errors.append("DeepSeek manifest transport identity mismatch")
    expected_transport, expected_transport_revision = runtime_identity
    expected_runtime = {
        "model": DEEPSEEK_MODEL,
        "provider": "opencode_zen",
        "transport": expected_transport,
        "transport_revision": expected_transport_revision,
        "transport_resume_policy": None,
        "openai_base_url": DEEPSEEK_BASE_URL,
        "reasoning_effort": DEEPSEEK_REASONING_EFFORT,
        "max_tokens": DEEPSEEK_MAX_TOKENS,
    }
    for key, expected in expected_runtime.items():
        if config.get(key) != expected:
            errors.append(f"DeepSeek manifest {key} mismatch")
    expected_rt = (
        2 if config.get("campaign_role") == "deepseek_capacity15"
        else 2 if runtime_identity == (
            DEEPSEEK_LEGACY_TRANSPORT,
            DEEPSEEK_LEGACY_TRANSPORT_REVISION,
        )
        else DEEPSEEK_FULL234_ROUND_TRIPS
    )
    if (expected_methods != {"fullrewrite", "hybridpatch"}
            or not _is_exact_int(target_rt) or target_rt != expected_rt):
        errors.append("DeepSeek campaign grid is invalid")
    expected_key_count = (
        DEEPSEEK_CAPACITY_KEY_COUNT
        if config.get("campaign_role") == "deepseek_capacity15"
        else DEEPSEEK_FULL_KEY_COUNT
    )
    expected_slots_per_key = (
        DEEPSEEK_CAPACITY_SLOTS_PER_KEY
        if config.get("campaign_role") == "deepseek_capacity15"
        else DEEPSEEK_FULL_SLOTS_PER_KEY
    )
    if (config.get("key_count") != expected_key_count
            or config.get("slots_per_key") != expected_slots_per_key
            or config.get("max_worker_count")
            != expected_key_count * expected_slots_per_key):
        errors.append("DeepSeek campaign key/concurrency grid is invalid")

    active_worker_rows = {}
    try:
        active_payload = _read_json(_active_worker_set_path(out_dir))
        if (active_payload.get("schema")
                != "anchorpatch.active_worker_set/1"
                or not isinstance(active_payload.get("workers"), dict)):
            raise RuntimeError("active worker set schema is invalid")
        active_worker_rows = active_payload["workers"]
    except (OSError, ValueError, RuntimeError):
        if active_samples:
            errors.append("active worker set is missing or invalid")

    try:
        stop_records = read_campaign_stop_conditions(out_dir)
    except RuntimeError as exc:
        stop_records = []
        errors.append(str(exc))
    for record in stop_records:
        condition = record.get("condition")
        if (record.get("schema")
                != "anchorpatch.campaign_stop_condition/1"
                or not isinstance(condition, str) or not condition):
            errors.append("invalid campaign stop latch")
        else:
            errors.append(f"campaign stop latch={condition}")

    recovery_authorization = read_campaign_recovery_authorization(out_dir)
    recovery_incidents = campaign_recovery_incident_evidence(out_dir)
    authorized_api_incident_hashes = recovery_incidents["api_row_hashes"]
    api_incident_kinds = recovery_incidents.get(
        "api_incident_kinds", {})
    transport_sidecars_by_api_row_hash = recovery_incidents.get(
        "transport_sidecars_by_api_row_hash", {})
    recovered_worker_ids = recovery_incidents["worker_launch_ids"]
    recovered_preauthorization_worker_ids = recovery_incidents[
        "preauthorization_worker_launch_ids"
    ]
    unpublished_worker_start_capability_ids = recovery_incidents.get(
        "unpublished_worker_start_capability_ids", frozenset())
    expected_commit = (
        recovery_authorization.get("recovery_git_commit")
        if recovery_authorization else manifest.get("run_git_commit")
    )
    commit, tree_state = _git_identity()
    if commit != expected_commit or tree_state != "clean":
        errors.append("Git commit/tree state changed during campaign")
    for sample, plan in (manifest.get("task_plans") or {}).items():
        if sample not in sample_scope:
            continue
        plan_path = os.path.join(out_dir, plan.get("path") or "")
        if not os.path.isfile(plan_path):
            errors.append(f"task plan missing: {sample}")
        elif _sha256(plan_path) != plan.get("sha256"):
            errors.append(f"task-plan hash drift: {sample}")

    dispatch_rows = (
        _read_jsonl(os.path.join(out_dir, "dispatch_log.jsonl"))
        if preloaded_ledgers is None
        else list(preloaded_ledgers["dispatch_log.jsonl"])
    )
    launches = {}
    authorizations = {}
    exits = {}
    for row in dispatch_rows:
        event = row.get("event")
        if event not in {"launch", "worker_authorized", "worker_exit"}:
            continue
        worker_id = row.get("worker_launch_id")
        target = {
            "launch": launches,
            "worker_authorized": authorizations,
            "worker_exit": exits,
        }[event]
        if not isinstance(worker_id, str) or not worker_id:
            errors.append(f"{event} missing worker_launch_id")
        elif worker_id in target:
            errors.append(f"duplicate {event} record: {worker_id}")
        else:
            target[worker_id] = row

    if scoped_audit:
        scoped_worker_ids = {
            worker_id for worker_id, launch in launches.items()
            if launch.get("sample") in sample_scope
        }
        launches = {
            worker_id: row for worker_id, row in launches.items()
            if worker_id in scoped_worker_ids
        }
        authorizations = {
            worker_id: row for worker_id, row in authorizations.items()
            if worker_id in scoped_worker_ids
        }
        exits = {
            worker_id: row for worker_id, row in exits.items()
            if worker_id in scoped_worker_ids
        }

    capability_sha_by_worker = {}
    capabilities_by_worker = {}
    if provider_guard_mode == PROVIDER_GUARD_WORKER_START_CAPABILITY_V1:
        manifest_digest = _canonical_record_sha256(manifest)
        for worker_id, authorization in authorizations.items():
            capability = {
                key: authorization.get(key)
                for key in _WORKER_START_CAPABILITY_FIELDS
            }
            capability_digest = _canonical_record_sha256(capability)
            launch = launches.get(worker_id)
            sample = authorization.get("sample")
            plan = (manifest.get("task_plans") or {}).get(sample) or {}
            _ready_path, start_path = _worker_barrier_paths(
                out_dir, worker_id)
            try:
                published_capability = _read_json(start_path)
            except (OSError, ValueError, RuntimeError):
                published_capability = None
            recovered_unpublished_capability = (
                worker_id in unpublished_worker_start_capability_ids)
            capability_was_recovered_before_publication = (
                recovered_unpublished_capability
                and not os.path.exists(start_path))
            if (not isinstance(launch, dict)
                    or capability.get("schema")
                    != WORKER_START_CAPABILITY_SCHEMA
                    or capability.get("worker_launch_id") != worker_id
                    or capability.get("worker_pid") != launch.get("pid")
                    or capability.get("sample") != launch.get("sample")
                    or not isinstance(capability.get("invocation_id"), str)
                    or not capability.get("invocation_id")
                    or capability.get("task_plan_sha256")
                    != plan.get("sha256")
                    or capability.get("provider_guard_mode")
                    != provider_guard_mode
                    or capability.get("dispatcher_pid")
                    != launch.get("dispatcher_pid")
                    or capability.get("dispatcher_instance_id")
                    != launch.get("dispatcher_instance_id")
                    or capability.get("transport_revision")
                    != expected_transport_revision
                    or capability.get("dispatch_manifest_canonical_sha256")
                    != manifest_digest
                    or authorization.get("worker_start_capability_sha256")
                    != capability_digest
                    or (
                        recovered_unpublished_capability
                        and not capability_was_recovered_before_publication)
                    or (
                        not capability_was_recovered_before_publication
                        and published_capability != capability
                    )):
                errors.append(
                    f"worker start capability mismatch: {worker_id}")
                continue
            capability_sha_by_worker[worker_id] = capability_digest
            capabilities_by_worker[worker_id] = capability

    if preloaded_ledgers is None:
        metadata = read_run_metadata_snapshot(out_dir)
    else:
        fold_started = time.monotonic()
        metadata = _fold_run_metadata_events(
            preloaded_ledgers["run_metadata_events.jsonl"])
        record_slow_control_operation(
            out_dir, "incremental_metadata_fold",
            time.monotonic() - fold_started,
            event_count=len(
                preloaded_ledgers["run_metadata_events.jsonl"]),
            projection_count=len(metadata))
    audited_metadata = []
    metadata_by_worker = {}
    latest_by_sample = {}
    for record in metadata:
        worker_id = record.get("worker_launch_id")
        record_samples = record.get("samples") or []
        if (scoped_audit
                and worker_id not in launches
                and not (set(record_samples) & sample_scope)):
            continue
        audited_metadata.append(record)
        if isinstance(worker_id, str) and worker_id:
            metadata_by_worker.setdefault(worker_id, []).append(record)
        for sample in record_samples:
            if sample in sample_scope:
                latest_by_sample[sample] = record
        if record.get("model") != DEEPSEEK_MODEL:
            errors.append("run_metadata model mismatch")
        for key, expected in {
                "provider": "opencode_zen",
                "transport": expected_transport,
                "transport_revision": expected_transport_revision,
                "transport_resume_policy": None,
                "base_url": DEEPSEEK_BASE_URL,
                "reasoning_effort": DEEPSEEK_REASONING_EFFORT,
                "max_tokens": DEEPSEEK_MAX_TOKENS,
        }.items():
            if record.get(key) != expected:
                errors.append(f"run_metadata {key} mismatch")
        campaign_config = record.get("campaign_config") or {}
        if (campaign_config.get("reasoning_effort")
                != DEEPSEEK_REASONING_EFFORT):
            errors.append("run_metadata campaign reasoning effort mismatch")
        if (record.get("status") == "failed"
                and worker_id not in recovered_worker_ids):
            errors.append(
                f"latest run_metadata invocation failed: "
                f"{(record_samples or ['unknown'])[0]}")
        launch = launches.get(worker_id)
        if isinstance(launch, dict) and "failover_count" in launch:
            expected_key_metadata = {
                "dispatch_key_label": launch.get("key_label"),
                "original_key_label": launch.get("original_key_label"),
                "prior_key_label": launch.get("prior_key_label"),
                "failover_count": launch.get("failover_count"),
                "failover_reason": launch.get("failover_reason"),
            }
            if any(
                    record.get(key) != value
                    for key, value in expected_key_metadata.items()):
                errors.append(
                    "run_metadata key failover provenance mismatch: "
                    f"{(record_samples or ['unknown'])[0]}")

    if provider_guard_mode == PROVIDER_GUARD_WORKER_START_CAPABILITY_V1:
        for worker_id, capability in capabilities_by_worker.items():
            worker_metadata = metadata_by_worker.get(worker_id) or []
            if (len(worker_metadata) != 1
                    or worker_metadata[0].get("invocation_id")
                    != capability.get("invocation_id")
                    or worker_metadata[0].get("run_git_commit")
                    != capability.get("run_git_commit")):
                errors.append(
                    "worker capability/metadata identity mismatch: "
                    f"{worker_id}")

    # Terminal publication order is API -> sample outcome -> run metadata.
    # Read the three append-only ledgers in reverse order so a live inspection
    # always observes a causal prefix rather than a newer outcome paired with
    # an older API snapshot.
    outcome_snapshot = (
        read_sample_outcomes(out_dir)
        if preloaded_ledgers is None
        else list(preloaded_ledgers["sample_outcomes.jsonl"])
    )
    if scoped_audit:
        outcome_snapshot = [
            row for row in outcome_snapshot
            if row.get("sample") in sample_scope
        ]
    committed_rows, api_rows = _read_relay_publication_snapshot(
        out_dir,
        [
            sample for sample in config.get("samples") or []
            if sample in sample_scope
        ],
        config.get("method_set") or [],
        api_snapshot=(
            None if preloaded_ledgers is None
            else preloaded_ledgers["api_calls.jsonl"]),
    )
    if scoped_audit:
        api_rows = [
            row for row in api_rows if row.get("sample") in sample_scope
        ]
    else:
        errors.extend(_inspect_deepseek_key_failover_audit(
            manifest, dispatch_rows, api_rows, outcome_snapshot, launches))
    committed_call_ids = {
        call_id
        for rows in committed_rows.values()
        for row in rows
        for call_id in (row.get("api_call_ids") or [])
    }
    infrastructure_config = dict(config)
    infrastructure_config["samples"] = [
        sample for sample in config.get("samples") or []
        if sample in sample_scope
    ]
    infrastructure_request_ids = (
        _verified_deepseek_infrastructure_request_ids(
            out_dir, infrastructure_config,
            metadata_snapshot=audited_metadata,
            api_snapshot=api_rows,
            committed_call_ids=committed_call_ids,
            outcome_snapshot=outcome_snapshot,
            active_samples=active_samples,
            active_worker_rows=active_worker_rows,
            launch_snapshot=launches,
        )
    )
    provisional_active_failure_request_ids = set()
    api_by_id = {}
    calls_by_step = {}
    api_rows_by_worker = {}
    for index, row in enumerate(api_rows, 1):
        sample = row.get("sample")
        method = row.get("method")
        rt = row.get("rt_index")
        direction = row.get("direction")
        call_kind = row.get("call_kind")
        step = (sample, method, rt, direction)
        allowed_kinds = (
            {"hybridpatch_primary", "hybridpatch_repair"}
            if method == "hybridpatch" else {"fullrewrite_primary"}
        )
        if (sample not in expected_samples or method not in expected_methods
                or not _is_exact_int(rt) or not 1 <= rt <= target_rt
                or direction not in {"forward", "backward"}
                or call_kind not in allowed_kinds):
            errors.append(f"unmappable API ledger row {index}")
            continue
        request_id = row.get("request_id")
        if (not isinstance(request_id, str) or not request_id
                or request_id in api_by_id):
            errors.append(f"duplicate/invalid API request id at row {index}")
        else:
            api_by_id[request_id] = row
        worker_id = row.get("worker_launch_id")
        row_hash = _canonical_record_sha256(row)
        authorized_incident = row_hash in authorized_api_incident_hashes
        incident_kind = (
            api_incident_kinds.get(row_hash)
            if authorized_incident else None
        )
        authorized_historical_failure = (
            authorized_incident
            and request_id not in committed_call_ids
            and incident_kind in {
                "deepseek_transport_disconnect_misclassification",
                "deepseek_dispatcher_stopped_uncommitted_api",
            }
        )
        authorized_partial_sidecar = False
        if (authorized_historical_failure
                and incident_kind
                == "deepseek_dispatcher_stopped_uncommitted_api"):
            try:
                authorized_partial_sidecar = (
                    _deepseek_dispatcher_stopped_sidecar_evidence(
                        out_dir, row)
                    == transport_sidecars_by_api_row_hash.get(row_hash)
                )
            except RuntimeError:
                authorized_partial_sidecar = False
        local_infrastructure_incident = (
            request_id in infrastructure_request_ids
        )
        sidecar_valid = _valid_deepseek_transport_sidecar(out_dir, row)
        provisional_active_failure = (
            not authorized_incident
            and not local_infrastructure_incident
            and sample in active_samples
            and isinstance(worker_id, str)
            and isinstance(active_worker_rows.get(worker_id), dict)
            and active_worker_rows[worker_id].get("sample") == sample
            and _deepseek_failed_retry_row(row)
            and sidecar_valid
        )
        if provisional_active_failure:
            provisional_active_failure_request_ids.add(request_id)
        if not (
                authorized_incident
                or local_infrastructure_incident
                or provisional_active_failure):
            calls_by_step.setdefault(step, []).append(row)
        api_rows_by_worker.setdefault(worker_id, []).append(row)
        for key, expected in {
                "schema": API_CALL_SCHEMA,
                "model": DEEPSEEK_MODEL,
                "base_url": DEEPSEEK_BASE_URL,
                "request_url": (
                    f"{DEEPSEEK_BASE_URL}/chat/completions"
                ),
                "transport": expected_transport,
                "transport_revision": expected_transport_revision,
                "transport_resume_policy": None,
                "reasoning_effort": DEEPSEEK_REASONING_EFFORT,
                "max_tokens": DEEPSEEK_MAX_TOKENS,
                "provider_called": True,
                "response_replayed": False,
        }.items():
            if row.get(key) != expected:
                errors.append(f"DeepSeek API {key} mismatch at row {index}")
        if row.get("generation_index") != 0:
            errors.append(f"DeepSeek API generation mismatch at row {index}")
        classification = row.get("classification")
        response_classification = row.get("response_classification")
        auditable_model_empty = (
            classification == "transport-valid but model-empty"
            and response_classification == "model_empty"
            and row.get("error_type") == "model_empty"
            and row.get("raw_content_length") == 0
            and row.get("stream_complete") is True
            and row.get("http_status") == 200
        )
        auditable_failed_retry = (
            (
                authorized_incident
                or local_infrastructure_incident
                or provisional_active_failure
            )
            and _deepseek_failed_retry_row(row))
        auditable_failure = (
            auditable_failed_retry or authorized_historical_failure)
        if ((classification is not None
             and not auditable_model_empty
             and not auditable_failure)
                or row.get("stream_complete") is not True
                and not auditable_failure
                or (row.get("input_tokens") is None
                    and not auditable_failure)
                or (row.get("output_tokens") is None
                    and not auditable_failure)):
            errors.append(f"DeepSeek API response incomplete at row {index}")
        if (not auditable_failure
                and not _valid_deepseek_retry_evidence(row)):
            errors.append(f"DeepSeek retry evidence invalid at row {index}")
        if not sidecar_valid and not authorized_partial_sidecar:
            errors.append(
                f"DeepSeek transport sidecar invalid at row {index}")
        request_path = row.get("raw_request_saved_path")
        try:
            request_payload = (
                _read_json(request_path)
                if isinstance(request_path, str) and request_path else {}
            )
        except (OSError, ValueError, RuntimeError):
            request_payload = {}
        request_body = request_payload.get("request_body")
        stream_request_valid = (
            runtime_identity == (
                DEEPSEEK_LEGACY_TRANSPORT,
                DEEPSEEK_LEGACY_TRANSPORT_REVISION,
            )
            or (
                isinstance(request_body, dict)
                and request_body.get("stream") is True
                and request_body.get("stream_options")
                == {"include_usage": True}
            )
        )
        if (not auditable_failure
                and (not isinstance(request_body, dict)
                or request_body.get("model") != DEEPSEEK_MODEL
                or request_body.get("reasoning_effort")
                != DEEPSEEK_REASONING_EFFORT
                or request_body.get("max_completion_tokens")
                != DEEPSEEK_MAX_TOKENS
                or not stream_request_valid)):
            errors.append(
                f"DeepSeek raw request transport audit failed at row {index}")

        launch = launches.get(worker_id)
        authorization = authorizations.get(worker_id)
        worker_metadata = metadata_by_worker.get(worker_id) or []
        worker_pid = row.get("worker_pid")
        if (not isinstance(launch, dict)
                or launch.get("sample") != sample
                or launch.get("pid") != worker_pid
                or not isinstance(authorization, dict)
                or authorization.get("sample") != sample
                or authorization.get("worker_pid") != worker_pid
                or len(worker_metadata) != 1
                or worker_metadata[0].get("worker_pid") != worker_pid
                or worker_metadata[0].get("samples") != [sample]):
            errors.append(f"API worker provenance mismatch at row {index}")
        if (provider_guard_mode
                == PROVIDER_GUARD_WORKER_START_CAPABILITY_V1
                and row.get("worker_start_capability_sha256")
                != capability_sha_by_worker.get(worker_id)):
            errors.append(f"API worker capability mismatch at row {index}")

    for worker_id in unpublished_worker_start_capability_ids:
        if api_rows_by_worker.get(worker_id):
            errors.append(
                "unpublished worker capability has API evidence: "
                f"{worker_id}")

    for sample in config.get("samples") or []:
        if sample not in sample_scope:
            continue
        for method in config.get("method_set") or []:
            rows = committed_rows[(sample, method)]
            seen = set()
            for row in rows:
                key = (
                    row.get("round_trip_num"),
                    row.get("round_trip_direction"),
                )
                if (row.get("sample_id") != sample
                        or row.get("method") != method
                        or key in seen
                        or not _is_exact_int(key[0])
                        or not 1 <= key[0] <= target_rt
                        or key[1] not in {"forward", "backward"}):
                    errors.append(
                        f"invalid committed row: {method}/{sample}/{key}")
                    continue
                seen.add(key)
                step = (sample, method, key[0], key[1])
                expected_primary = (
                    "hybridpatch_primary"
                    if method == "hybridpatch" else "fullrewrite_primary"
                )
                step_calls = calls_by_step.get(step) or []
                if sum(
                        item.get("call_kind") == expected_primary
                        for item in step_calls) != 1:
                    errors.append(
                        f"committed row primary call mismatch: "
                        f"{method}/{sample}/{key}")
                row_call_ids = row.get("api_call_ids") or []
                if (not row_call_ids
                        or any(call_id not in api_by_id
                               for call_id in row_call_ids)):
                    errors.append(
                        f"committed row API linkage mismatch: "
                        f"{method}/{sample}/{key}")
                if (row.get("model_name") != DEEPSEEK_MODEL
                        or row.get("reasoning_effort")
                        != DEEPSEEK_REASONING_EFFORT):
                    errors.append(
                        f"committed row runtime mismatch: "
                        f"{method}/{sample}/{key}")
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
                        and count is None and exec_log is None
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
                          or nested_count != count):
                        errors.append(
                            f"invalid preservation telemetry: "
                            f"{method}/{sample}/{key}")
                    else:
                        preservation += count
            checkpoint_path = os.path.join(
                out_dir, method, f"{sample}.ckpt.json")
            checkpoint = (
                _read_json(checkpoint_path)
                if os.path.isfile(checkpoint_path) else None
            )
            if rows and not isinstance(checkpoint, dict):
                errors.append(f"missing checkpoint: {method}/{sample}")
            if sample in completion_samples:
                completed = (
                    checkpoint.get("completed_round_trips")
                    if isinstance(checkpoint, dict) else None
                )
                if completed != target_rt or len(rows) != 2 * target_rt:
                    errors.append(
                        f"incomplete task: {method}/{sample} "
                        f"checkpoint={completed}/{target_rt} "
                        f"rows={len(rows)}/{2 * target_rt}")

    if preservation:
        errors.append(f"preservation_violations={preservation}")
    if completion_samples:
        missing_metadata = completion_samples - set(latest_by_sample)
        if missing_metadata:
            errors.append(
                f"samples missing run_metadata: {sorted(missing_metadata)}")
        if any(
                latest_by_sample.get(sample, {}).get("status") != "finished"
                for sample in completion_samples):
            errors.append(
                "not all required latest run_metadata invocations are finished")
    if require_terminal_provenance:
        if any(
                record.get("finished_at") is None
                for record in audited_metadata
                if set(record.get("samples") or []) & terminal_scope):
            errors.append("campaign finished_at is incomplete")
        for worker_id, launch in launches.items():
            if launch.get("sample") not in terminal_scope:
                continue
            exit_row = exits.get(worker_id)
            worker_metadata = metadata_by_worker.get(worker_id) or []
            terminal_provenance = _worker_terminal_provenance(
                launch, exit_row, worker_metadata)
            terminal = terminal_provenance["terminal"]
            ordinary_terminal = terminal_provenance["ordinary_ok"]
            recovered_terminal = (
                worker_id in recovered_worker_ids
                and isinstance(exit_row, dict)
                and exit_row.get("sample") == launch.get("sample")
                and exit_row.get("pid") == launch.get("pid")
                and exit_row.get("returncode") != 0
                and exit_row.get("disposition") == "campaign_fatal"
                and terminal.get("status") in {
                    "failed", "interrupted_by_dispatcher"}
            )
            recovered_preauthorization = (
                worker_id in recovered_preauthorization_worker_ids
                and isinstance(exit_row, dict)
                and exit_row.get("sample") == launch.get("sample")
                and exit_row.get("pid") == launch.get("pid")
                and exit_row.get("returncode") != 0
                and exit_row.get("disposition") == "campaign_fatal"
                and not worker_metadata
                and worker_id not in authorizations
                and worker_id not in api_rows_by_worker
            )
            if (not ordinary_terminal
                    and not recovered_terminal
                    and not recovered_preauthorization):
                errors.append(
                    f"worker exit provenance incomplete: {worker_id}")

    return {
        "errors": sorted(set(errors)),
        "preservation_violations": preservation,
        "latched_preservation_violations": 0,
        "preservation_not_applicable": preservation_not_applicable,
        "stop_conditions": stop_records,
        "api_calls": len(api_rows),
        "semantic_calls": len({
            row.get("semantic_call_id") for row in api_rows
        }),
        "provider_call_rows": sum(
            row.get("provider_called") is True for row in api_rows),
        "local_infrastructure_incident_rows": len(
            authorized_api_incident_hashes) + len(
                infrastructure_request_ids)
            + len(provisional_active_failure_request_ids),
    }


def inspect_campaign(out_dir, manifest, *, require_complete=False,
                     require_terminal_provenance=None, active_samples=None,
                     required_complete_samples=None, method_phase=None,
                     audit_samples=None, terminal_audit_samples=None,
                     preloaded_ledgers=None):
    if require_terminal_provenance is None:
        require_terminal_provenance = require_complete
    config = manifest["config"]
    snapshot_mode_error = None
    try:
        normalize_snapshot_mode(
            config.get("snapshot_mode"), default=SNAPSHOT_MODE_ALL)
    except ValueError as exc:
        snapshot_mode_error = str(exc)
    quiescent_metadata_error = None
    if (require_complete and config.get("run_metadata_storage")
            == RUN_METADATA_STORAGE_EVENT_V1):
        try:
            if not read_quiescent_run_metadata_snapshot(out_dir):
                quiescent_metadata_error = "run metadata is missing"
        except RuntimeError as exc:
            quiescent_metadata_error = str(exc)
    if config.get("campaign_role") in DEEPSEEK_CAMPAIGN_ROLES:
        if method_phase is not None:
            raise RuntimeError(
                "DeepSeek campaigns do not support phased inspection")
        result = _inspect_deepseek_campaign(
            out_dir, manifest, require_complete=require_complete,
            require_terminal_provenance=require_terminal_provenance,
            active_samples=active_samples,
            required_complete_samples=required_complete_samples,
            audit_samples=audit_samples,
            terminal_audit_samples=terminal_audit_samples,
            preloaded_ledgers=preloaded_ledgers,
        )
        if quiescent_metadata_error is not None:
            result["errors"] = list(result.get("errors") or []) + [
                "run metadata is not quiescent: " + quiescent_metadata_error
            ]
        if snapshot_mode_error is not None:
            result["errors"] = list(result.get("errors") or []) + [
                "snapshot mode is invalid: " + snapshot_mode_error
            ]
        return result
    if audit_samples is not None:
        raise RuntimeError(
            "sample-scoped campaign inspection is only supported for DeepSeek")
    if preloaded_ledgers is not None:
        raise RuntimeError(
            "preloaded campaign ledgers are only supported for DeepSeek")
    declared_phases = config.get("method_phases")
    if method_phase is None and declared_phases is not None:
        if declared_phases != list(REMAINING134_METHOD_PHASES):
            raise RuntimeError("campaign method phases are invalid")
        phase_results = [
            inspect_campaign(
                out_dir, manifest, require_complete=require_complete,
                require_terminal_provenance=require_terminal_provenance,
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
    if snapshot_mode_error is not None:
        errors.append("snapshot mode is invalid: " + snapshot_mode_error)
    if quiescent_metadata_error is not None:
        errors.append(
            "run metadata is not quiescent: " + quiescent_metadata_error)
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

    committed_rows, api_rows = _read_relay_publication_snapshot(
        out_dir, config["samples"], config["method_set"])
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
    if require_terminal_provenance and any(
            record.get("finished_at") is None for record in metadata):
        errors.append("campaign finished_at is incomplete")
    if formal_manifest and require_terminal_provenance:
        for worker_id, launch in launches_by_worker.items():
            exit_row = exits_by_worker.get(worker_id)
            reconciliation = reconciliations_by_worker.get(worker_id)
            terminal_provenance = _worker_terminal_provenance(
                launch, exit_row, metadata_by_worker.get(worker_id, []))
            terminal_metadata = terminal_provenance["terminal"]
            basic_exit_ok = terminal_provenance["basic_exit_ok"]
            timestamp_ok = terminal_provenance["timestamp_ok"]
            exit_ok = terminal_provenance["ordinary_ok"]
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
            "run_metadata_events.jsonl",
            "run_metadata_projection_receipt.json",
            "run_metadata_event_pending.json",
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
    metadata_recovery_dir = os.path.join(
        out_dir, "run_metadata_event_recoveries")
    if os.path.isdir(metadata_recovery_dir):
        paths.update(
            str(path) for path in Path(metadata_recovery_dir).glob("*.json"))
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
            "run_metadata_storage": RUN_METADATA_STORAGE_EVENT_V1,
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
        if not read_quiescent_run_metadata_snapshot(smoke_dir):
            failure_codes.append("smoke_run_metadata_missing")
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
        "original_key_label": item.get("original_key_label"),
        "prior_key_label": item.get("prior_key_label"),
        "failover_count": item.get("failover_count", 0),
        "failover_reason": item.get("failover_reason"),
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


def _stop_and_reconcile_workers(
        out_dir, running, dispatch_log=None, *, lease_scope=None,
        record_worker_exits=True, close_running_metadata=True):
    """Best-effort stop, then close metadata only after leases are free."""
    samples_to_verify = sorted(
        set(lease_scope or ()) | set(running)
    )
    result = {
        "termination_error": None,
        "lease_error": None,
        "closed_invocations": [],
        "lease_scope": samples_to_verify,
    }
    try:
        _terminate_workers(running)
    except BaseException as exc:
        result["termination_error"] = str(exc)
    try:
        _assert_worker_leases_free(out_dir, samples_to_verify)
    except BaseException as exc:
        result["lease_error"] = str(exc)
        return result
    if dispatch_log and record_worker_exits:
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
    if not close_running_metadata:
        result["metadata_closure_deferred"] = True
        return result
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


def _original_key_label(item):
    return item.get("original_key_label") or item.get("key_label")


def _mark_pending_provenance_assignment(item, provenance):
    if provenance not in {
            PENDING_NEVER_STARTED,
            PENDING_INFRASTRUCTURE_INCOMPLETE,
            PENDING_INTERRUPTED,
    }:
        raise RuntimeError(f"invalid pending provenance: {provenance!r}")
    marked = dict(item)
    marked[PENDING_PROVENANCE_FIELD] = provenance
    return marked


def _mark_proven_never_started_assignment(item):
    return _mark_pending_provenance_assignment(item, PENDING_NEVER_STARTED)


def _pending_provenance(item):
    return item.get(PENDING_PROVENANCE_FIELD)


def _is_proven_never_started_assignment(item):
    return _pending_provenance(item) == PENDING_NEVER_STARTED


def _preserve_pending_provenance_assignment(item):
    return dict(item)


def _clear_pending_provenance_assignment(item):
    cleared = dict(item)
    cleared.pop(PENDING_PROVENANCE_FIELD, None)
    return cleared


def _failover_count(item):
    value = item.get("failover_count", 0)
    if not _is_exact_int(value) or value < 0:
        raise RuntimeError("worker failover count is invalid")
    return value


def _worker_key_launch_fields(item):
    label = item["key_label"]
    original = _original_key_label(item)
    count = _failover_count(item)
    prior = item.get("prior_key_label")
    reason = item.get("failover_reason")
    if (not isinstance(original, str) or not original
            or (count == 0 and (
                label != original or prior is not None or reason is not None
            ))
            or (count > 0 and (
                not isinstance(prior, str) or not prior
                or not isinstance(reason, str) or not reason
                or label == prior
            ))):
        raise RuntimeError("worker key failover provenance is invalid")
    return {
        "key_label": label,
        "original_key_label": original,
        "prior_key_label": prior,
        "failover_count": count,
        "failover_reason": reason,
    }


def _launch_worker_batch(
        args, out_dir, inspection_manifest, task_plans, keys, assignments,
        resume_authorizations, dispatch_log, running):
    """Launch and authorize one refill batch while existing workers continue."""
    _raise_if_operator_pause_requested(args, "before_worker_batch")
    if (not isinstance(
            getattr(args, "_dispatcher_instance_id", None), str)
            or not args._dispatcher_instance_id):
        args._dispatcher_instance_id = (
            f"dispatcher-{os.getpid()}-{uuid.uuid4().hex}"
        )
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
    runtime = _campaign_runtime_config(args)
    provider_guard_mode = _provider_guard_mode(inspection_manifest)
    snapshot_mode = normalize_snapshot_mode(
        _snapshot_mode_arg(args), default=SNAPSHOT_MODE_FAILURES)
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
            "dispatcher_pid": os.getpid(),
            "dispatcher_instance_id": args._dispatcher_instance_id,
            "runtime_git_commit": runtime_git_commit,
            "provider_guard_mode": provider_guard_mode,
        }
    _publish_active_worker_set(
        out_dir, inspection_manifest,
        [*running.values(), *launch_specs.values()], args=args)
    batch_running = {}
    for item in assignments:
        _raise_if_operator_pause_requested(args, "before_worker_launch_intent")
        sample = item["sample"]
        label = item["key_label"]
        launch_key_fields = _worker_key_launch_fields(item)
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
            "--model", runtime["model"],
            "--max_tokens", str(runtime["max_tokens"]),
            "--out_dir", out_dir,
            "--snapshot_mode", snapshot_mode,
            "--stop_on_preservation_violation",
            "--notes", f"{args.notes}, key={label}",
        ]
        if runtime.get("reasoning_effort"):
            command.extend([
                "--reasoning_effort", runtime["reasoning_effort"],
            ])
        environment = dict(os.environ)
        environment.update({
            "PYTHONUTF8": "1",
            "ANCHORPATCH_DISPATCHER_PID": str(os.getpid()),
            "ANCHORPATCH_DISPATCHER_INSTANCE_ID": (
                args._dispatcher_instance_id),
            "ANCHORPATCH_WORKER_LAUNCH_ID": worker_id,
            "ANCHORPATCH_WORKER_LOCK_PATH": _worker_lease_path(
                out_dir, sample),
            "ANCHORPATCH_WORKER_READY_PATH": os.path.abspath(ready_path),
            "ANCHORPATCH_WORKER_ACK_PATH": os.path.abspath(ack_path),
            "ANCHORPATCH_ACTIVE_WORKER_SET_PATH": os.path.abspath(
                _active_worker_set_path(out_dir)),
            "ANCHORPATCH_PROVIDER_GUARD_MODE": provider_guard_mode,
            "ANCHORPATCH_START_BARRIER_TIMEOUT": str(args.start_timeout),
            "ANCHORPATCH_WORKER_CONSOLE_LOG": item["console_log"],
            "ANCHORPATCH_EXPECTED_GIT_COMMIT": runtime_git_commit,
            "ANCHORPATCH_EXPECTED_GIT_TREE_STATE": "clean",
            "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256": (
                task_plans[sample]["sha256"]),
            "ANCHORPATCH_EXPECTED_TASK_PLAN_PATH": os.path.abspath(
                os.path.join(out_dir, task_plans[sample]["path"])),
            "ANCHORPATCH_DISPATCH_KEY_LABEL": label,
            "ANCHORPATCH_ORIGINAL_KEY_LABEL": (
                launch_key_fields["original_key_label"]),
            "ANCHORPATCH_KEY_FAILOVER_COUNT": str(
                launch_key_fields["failover_count"]),
        })
        if launch_key_fields["prior_key_label"] is not None:
            environment["ANCHORPATCH_PRIOR_KEY_LABEL"] = (
                launch_key_fields["prior_key_label"])
        else:
            environment.pop("ANCHORPATCH_PRIOR_KEY_LABEL", None)
        if launch_key_fields["failover_reason"] is not None:
            environment["ANCHORPATCH_KEY_FAILOVER_REASON"] = (
                launch_key_fields["failover_reason"])
        else:
            environment.pop("ANCHORPATCH_KEY_FAILOVER_REASON", None)
        if args.campaign_role in DEEPSEEK_CAMPAIGN_ROLES:
            for name in (
                    "OPENCODE_API_KEY", "OPENCODE_GO_API_KEY",
                    "OPENCODE_TRANSPORT", "MINIMAX_TRANSPORT",
                    "MINIMAX_API_KEY", "AZURE_OPENAI_API_KEY",
                    "AZURE_OPENAI_ENDPOINT"):
                environment.pop(name, None)
            environment["OPENAI_API_KEY"] = keys[label]
            environment["OPENAI_BASE_URL"] = DEEPSEEK_BASE_URL
        else:
            environment["OPENCODE_API_KEY"] = keys[label]
            environment["OPENCODE_TRANSPORT"] = "anthropic_sdk_v2"
            environment["MINIMAX_TRANSPORT"] = "opencode"
            environment["MINIMAX_HARD_TIMEOUT"] = "7200"
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
                **launch_key_fields, "methods": item["methods"],
                "worker_launch_id": worker_id,
                "console_log": item["console_log"],
                "method_phase": item_method_phase,
                "interrupted_resume_evidence": interrupted_resume,
                "dispatcher_pid": os.getpid(),
                "dispatcher_instance_id": args._dispatcher_instance_id,
            },
        )
        _raise_if_operator_pause_requested(args, "before_worker_popen")
        log_path = os.path.join(out_dir, item["console_log"])
        log_handle = open(log_path, "a", encoding="utf-8")
        worker_state = {
            "sample": sample,
            "process": None,
            "log": log_handle,
            **launch_key_fields,
            "methods": list(item["methods"]),
            "console_log": item["console_log"],
            "target_round_trips": args.num_round_trips,
            "resume_authorization": authorization,
            "worker_launch_id": worker_id,
            "method_phase": item_method_phase,
            "interrupted_resume_evidence": interrupted_resume,
            "ready_path": ready_path,
            "ack_path": ack_path,
            "dispatcher_pid": os.getpid(),
            "dispatcher_instance_id": args._dispatcher_instance_id,
            "runtime_git_commit": runtime_git_commit,
            "provider_guard_mode": provider_guard_mode,
            "exit_recorded": False,
        }
        process = None
        try:
            process = subprocess.Popen(
                command, cwd=_ROOT, env=environment,
                stdout=log_handle, stderr=subprocess.STDOUT,
                **_worker_popen_kwargs(),
            )
            worker_state["process"] = process
            running[sample] = worker_state
        except BaseException:
            # If CreateProcess returned but a Python-level interruption landed
            # before normal registration, keep the child in the reconciliation
            # set.  A spawned worker must never become invisible to cleanup.
            if process is not None:
                worker_state["process"] = process
                running[sample] = worker_state
            else:
                log_handle.close()
            raise
        batch_running[sample] = running[sample]
        append_jsonl_locked(
            dispatch_log,
            {
                "event": "launch", "sample": sample,
                **launch_key_fields, "methods": item["methods"],
                "pid": process.pid, "worker_launch_id": worker_id,
                "console_log": item["console_log"],
                "method_phase": item_method_phase,
                "dispatcher_pid": os.getpid(),
                "dispatcher_instance_id": args._dispatcher_instance_id,
            },
        )
        _raise_if_operator_pause_requested(args, "after_worker_launch")

    if batch_running:
        _authorize_workers(
            out_dir, batch_running, task_plans, dispatch_log,
            args.start_timeout,
            pause_check=lambda boundary:
                _raise_if_operator_pause_requested(args, boundary),
            manifest=inspection_manifest,
        )
        _raise_if_operator_pause_requested(args, "after_worker_authorization")
    return list(batch_running)


def _append_key_failover_assignment(
        dispatch_log, item, destination_label, failover_counts):
    source_label = item["key_label"]
    original_label = _original_key_label(item)
    sample = item["sample"]
    next_count = failover_counts.get(sample, _failover_count(item)) + 1
    assigned = _clear_pending_provenance_assignment(item)
    assigned.update({
        "key_label": destination_label,
        "original_key_label": original_label,
        "prior_key_label": source_label,
        "failover_count": next_count,
        "failover_reason": DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
    })
    failover_counts[sample] = next_count
    append_jsonl_locked(
        dispatch_log,
        {
            "event": "key_failover_assigned",
            "sample": sample,
            "from_key_label": source_label,
            "to_key_label": destination_label,
            "reason": DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
            "failover_count": next_count,
            "original_key_label": original_label,
            "created_at": datetime.now().astimezone().isoformat(
                timespec="seconds"),
        },
    )
    return assigned


def _take_available_assignments(
        pending_by_key, running, slots_per_key, *,
        quarantined_keys=None, handoff_fifo=None, dispatch_log=None,
        failover_counts=None):
    """Pop stable per-key FIFO work for every currently free key slot."""
    if not _is_exact_int(slots_per_key) or slots_per_key < 1:
        raise RuntimeError("queue slots_per_key must be >= 1")
    quarantined_keys = set(quarantined_keys or ())
    handoff_fifo = handoff_fifo if handoff_fifo is not None else []
    failover_counts = failover_counts if failover_counts is not None else {}
    if handoff_fifo and dispatch_log is None:
        raise RuntimeError("handoff failover requires a dispatch log")
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
        if label in quarantined_keys:
            continue
        for _index in range(min(free, len(pending))):
            batch.append(_clear_pending_provenance_assignment(
                pending.pop(0)))
            free -= 1
        while free > 0 and handoff_fifo:
            batch.append(_append_key_failover_assignment(
                dispatch_log, handoff_fifo.pop(0), label, failover_counts))
            free -= 1
    return batch


def _append_key_quarantine(
        dispatch_log, key_label, trigger_sample, trigger_item, evidence,
        pending_by_key, handoff_fifo, quarantined_keys,
        quota_observation_counts):
    observation_index = quota_observation_counts.get(key_label, 0) + 1
    quota_observation_counts[key_label] = observation_index
    first_observation = key_label not in quarantined_keys
    if first_observation:
        quarantined_keys.add(key_label)
    quarantine_index = len(quarantined_keys)
    record = {
        "event": (
            "key_quarantined" if first_observation
            else "key_quota_observed"),
        "key_label": key_label,
        "reason": DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
        "observation_index": observation_index,
        "trigger_sample": trigger_sample,
        "trigger_worker_launch_id": trigger_item.get(
            "worker_launch_id"),
        "trigger_request_id": evidence.get("request_id"),
        "trigger_http_attempt_count": evidence.get(
            "http_attempt_count"),
        "api_row": evidence.get("api_row"),
        "api_row_count": evidence.get("api_row_count"),
        "api_row_sha256": evidence.get("api_row_sha256"),
        "sample_outcome_row": evidence.get("sample_outcome_row"),
        "sample_outcome_sha256": evidence.get("sample_outcome_sha256"),
        "sample_outcome_created_at": evidence.get(
            "sample_outcome_created_at"),
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"),
    }
    if first_observation:
        record.update({
            "quarantine_index": quarantine_index,
            "quarantined_key_count": quarantine_index,
        })
    append_jsonl_locked(
        dispatch_log,
        record,
    )
    if not first_observation:
        return False
    queued = pending_by_key.get(key_label) or []
    pending_by_key[key_label] = []
    handoff_fifo.extend(
        _preserve_pending_provenance_assignment(item) for item in queued)
    return True


def _append_key_reactivation_events(
        dispatch_log, dispatch_rows, keys, requested_labels, reason,
        pending_work_exists=None):
    """Record operator-attested replacement key material for quarantined labels."""
    labels = list(requested_labels or [])
    if not labels:
        return dispatch_rows
    if len(labels) != len(set(labels)):
        raise RuntimeError("duplicate key reactivation labels are invalid")
    if not isinstance(reason, str) or not reason.strip():
        raise RuntimeError("key reactivation requires a non-empty reason")
    unknown = [label for label in labels if label not in keys]
    if unknown:
        raise RuntimeError(f"cannot reactivate unknown key labels: {unknown}")

    quarantined_keys = set()
    quota_observation_counts = {}
    reactivation_counts = {}
    for row in dispatch_rows:
        event = row.get("event")
        label = row.get("key_label")
        if not isinstance(label, str) or not label:
            continue
        if event in {"key_quarantined", "key_quota_observed"}:
            observed = row.get("observation_index")
            if _is_exact_int(observed):
                quota_observation_counts[label] = max(
                    quota_observation_counts.get(label, 0), observed)
            if event == "key_quarantined":
                quarantined_keys.add(label)
        elif event == KEY_REACTIVATION_EVENT:
            index = row.get("reactivation_index")
            if _is_exact_int(index):
                reactivation_counts[label] = max(
                    reactivation_counts.get(label, 0), index)
            quarantined_keys.discard(label)

    for label in labels:
        if label not in quarantined_keys:
            if reactivation_counts.get(label, 0) > 0:
                continue
            raise RuntimeError(
                f"cannot reactivate a key label that is not quarantined: {label}")
    if pending_work_exists is not None and not pending_work_exists:
        raise RuntimeError(
            "cannot reactivate key labels with no pending work: "
            f"{labels}")

    appended = []
    for label in labels:
        if label not in quarantined_keys:
            continue
        before = sorted(quarantined_keys)
        quarantined_keys.remove(label)
        after = sorted(quarantined_keys)
        record = {
            "event": KEY_REACTIVATION_EVENT,
            "key_label": label,
            "reason": reason.strip(),
            "reactivation_index": reactivation_counts.get(label, 0) + 1,
            "observed_quota_count": quota_observation_counts.get(label, 0),
            "quarantined_key_labels_before": before,
            "quarantined_key_labels_after": after,
            "created_at": datetime.now().astimezone().isoformat(
                timespec="seconds"),
        }
        append_jsonl_locked(dispatch_log, record)
        reactivation_counts[label] = record["reactivation_index"]
        appended.append(record)
    return [*dispatch_rows, *appended]


def _restore_key_failover_queue_state(
        manifest, dispatch_rows, assignments, keys):
    """Rebuild durable quarantine and per-sample routing before any launch."""
    key_labels = list(keys)
    if (not key_labels or len(key_labels) != len(set(key_labels))
            or any(not isinstance(label, str) or not label
                   for label in key_labels)):
        raise RuntimeError("runtime key labels are invalid")
    _manifest_samples, manifest_key_by_sample, manifest_errors = (
        _deepseek_manifest_original_keys(manifest)
    )
    if manifest_errors:
        raise RuntimeError("; ".join(manifest_errors))

    runtime_assignment_by_sample = {}
    for item in assignments:
        sample = item.get("sample")
        label = item.get("key_label")
        if (sample not in manifest_key_by_sample
                or sample in runtime_assignment_by_sample
                or label not in keys):
            raise RuntimeError("worker queue assignment identity is invalid")
        runtime_assignment_by_sample[sample] = label

    quarantined_keys = set()
    quota_observation_counts = {}
    key_reactivations = {}
    failover_counts = {sample: 0 for sample in manifest_key_by_sample}
    current_key_by_sample = {}
    routing_origin_by_sample = {}
    latest_key_fields = {}
    has_failover_history = any(
        row.get("event") in {
            "key_quarantined", "key_quota_observed",
            "key_failover_assigned", KEY_REACTIVATION_EVENT,
        }
        for row in dispatch_rows
    )
    if not has_failover_history:
        current_key_by_sample.update(runtime_assignment_by_sample)
        routing_origin_by_sample.update(runtime_assignment_by_sample)
    for row in dispatch_rows:
        event = row.get("event")
        if event in {"key_quarantined", "key_quota_observed"}:
            label = row.get("key_label")
            expected_observation = quota_observation_counts.get(label, 0) + 1
            if (not isinstance(label, str) or not label
                    or row.get("reason")
                    != DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON
                    or row.get("observation_index") != expected_observation
                    or (event == "key_quarantined"
                        and label in quarantined_keys)
                    or (event == "key_quota_observed"
                        and label not in quarantined_keys)):
                raise RuntimeError(
                    "durable key quarantine history is invalid")
            quota_observation_counts[label] = expected_observation
            if event == "key_quarantined":
                quarantined_keys.add(label)
            continue
        if event == KEY_REACTIVATION_EVENT:
            label = row.get("key_label")
            expected_index = key_reactivations.get(label, 0) + 1
            before = sorted(quarantined_keys)
            after = sorted(item for item in quarantined_keys if item != label)
            if (label not in key_labels
                    or label not in quarantined_keys
                    or row.get("reactivation_index") != expected_index
                    or row.get("observed_quota_count")
                    != quota_observation_counts.get(label, 0)
                    or row.get("quarantined_key_labels_before") != before
                    or row.get("quarantined_key_labels_after") != after
                    or not isinstance(row.get("reason"), str)
                    or not row.get("reason").strip()):
                raise RuntimeError(
                    "durable key reactivation history is invalid")
            key_reactivations[label] = expected_index
            quarantined_keys.remove(label)
            continue
        if event == "key_failover_assigned":
            sample = row.get("sample")
            source = row.get("from_key_label")
            destination = row.get("to_key_label")
            count = row.get("failover_count")
            if sample not in manifest_key_by_sample:
                raise RuntimeError("durable key failover history is invalid")
            if sample not in current_key_by_sample and count == 1:
                current_key_by_sample[sample] = source
                routing_origin_by_sample[sample] = source
            if (sample not in current_key_by_sample
                    or source != current_key_by_sample[sample]
                    or source not in quarantined_keys
                    or not isinstance(destination, str) or not destination
                    or destination in quarantined_keys
                    or count != failover_counts[sample] + 1
                    or row.get("original_key_label")
                    != routing_origin_by_sample.get(sample)
                    or row.get("reason")
                    != DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON):
                raise RuntimeError("durable key failover history is invalid")
            failover_counts[sample] = count
            current_key_by_sample[sample] = destination
            latest_key_fields[sample] = {
                "key_label": destination,
                "original_key_label": routing_origin_by_sample[sample],
                "prior_key_label": source,
                "failover_count": count,
                "failover_reason": DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
            }
            continue
        if event not in {"launch", "launch_intent"}:
            continue
        if not has_failover_history:
            continue
        sample = row.get("sample")
        if sample not in manifest_key_by_sample or "failover_count" not in row:
            continue
        fields = _worker_key_launch_fields(row)
        if fields["failover_count"] == 0 and sample not in current_key_by_sample:
            current_key_by_sample[sample] = fields["key_label"]
            routing_origin_by_sample[sample] = fields["key_label"]
        if (fields["failover_count"] != failover_counts[sample]
                or fields["key_label"] != current_key_by_sample[sample]
                or fields["original_key_label"]
                != routing_origin_by_sample[sample]):
            raise RuntimeError("durable key launch history is invalid")
        latest_key_fields[sample] = fields

    pending_by_key = {label: [] for label in key_labels}
    handoff_fifo = []
    seen_samples = set()
    selected_failover_counts = {}
    for source_item in assignments:
        sample = source_item.get("sample")
        if (sample not in manifest_key_by_sample or sample in seen_samples):
            raise RuntimeError("worker queue assignment identity is invalid")
        seen_samples.add(sample)
        count = failover_counts[sample]
        item = dict(source_item)
        if sample not in current_key_by_sample:
            current_key_by_sample[sample] = item["key_label"]
            routing_origin_by_sample[sample] = item["key_label"]
        if count:
            fields = latest_key_fields.get(sample)
            if (not isinstance(fields, dict)
                    or fields.get("failover_count") != count):
                raise RuntimeError(
                    "durable key failover launch provenance is incomplete")
            item.update(fields)
        else:
            item.update({
                "key_label": current_key_by_sample[sample],
                "original_key_label": routing_origin_by_sample[sample],
                "prior_key_label": None,
                "failover_count": 0,
                "failover_reason": None,
            })
        selected_failover_counts[sample] = count
        if item["key_label"] in quarantined_keys:
            handoff_fifo.append(
                _preserve_pending_provenance_assignment(item))
        else:
            if item["key_label"] not in pending_by_key:
                raise RuntimeError(
                    "active routing key is missing at runtime")
            pending_by_key[item["key_label"]].append(item)
    return {
        "pending_by_key": pending_by_key,
        "handoff_fifo": handoff_fifo,
        "quarantined_keys": quarantined_keys,
        "quota_observation_counts": quota_observation_counts,
        "failover_counts": selected_failover_counts,
    }


def _queue_has_pending_work(restored):
    return bool(
        restored["handoff_fifo"]
        or any(restored["pending_by_key"].values()))


def _supports_incremental_exit_audit(manifest):
    config = manifest.get("config") if isinstance(manifest, dict) else None
    return (
        isinstance(manifest, dict)
        and manifest.get("schema") == SCHEMA
        and isinstance(config, dict)
        and config.get("campaign_role") == "deepseek_full234"
        and config.get("transport") == DEEPSEEK_TRANSPORT
        and config.get("transport_revision") == DEEPSEEK_TRANSPORT_REVISION
        and config.get("run_metadata_storage")
        == RUN_METADATA_STORAGE_EVENT_V1
    )


def _read_complete_jsonl_bytes(path):
    deadline = time.monotonic() + 60.0
    while True:
        try:
            with open(path, "rb") as handle:
                # Match _read_jsonl(): a writer holds LOCK_EX across the whole
                # append, flush and fsync.  Reading only after LOCK_SH is
                # granted distinguishes a genuinely torn tail from an ordinary
                # in-progress append or Windows sharing violation.
                portalocker.lock(handle, portalocker.LOCK_SH)
                try:
                    payload = handle.read()
                finally:
                    portalocker.unlock(handle)
        except FileNotFoundError:
            return b""
        except (OSError, portalocker.exceptions.LockException):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)
            continue
        if payload and not payload.endswith(b"\n"):
            raise RuntimeError(
                f"append-only JSONL has an incomplete tail: {path}")
        return payload


def _decode_jsonl_suffix(payload, *, path, first_row_number):
    rows = []
    if not payload:
        return rows
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            f"append-only JSONL is not UTF-8: {path}") from exc
    for offset, line in enumerate(text.splitlines(), first_row_number):
        if not line.strip():
            raise RuntimeError(
                f"append-only JSONL has a blank row {offset}: {path}")
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise RuntimeError(
                f"append-only JSONL row {offset} is invalid: {path}") from exc
        if not isinstance(row, dict):
            raise RuntimeError(
                f"append-only JSONL row {offset} is not an object: {path}")
        rows.append(row)
    return rows


def _advance_append_only_ledger_cursor(path, cursor):
    prior = dict(cursor or {})
    prior_bytes = prior.get("byte_count", 0)
    prior_rows = prior.get("row_count", 0)
    prior_digest = prior.get(
        "prefix_sha256", hashlib.sha256(b"").hexdigest())
    if (not _is_exact_int(prior_bytes) or prior_bytes < 0
            or not _is_exact_int(prior_rows) or prior_rows < 0
            or not isinstance(prior_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", prior_digest) is None):
        raise RuntimeError(f"append-only ledger cursor is invalid: {path}")
    payload = _read_complete_jsonl_bytes(path)
    if len(payload) < prior_bytes:
        raise RuntimeError(f"append-only ledger was truncated: {path}")
    if hashlib.sha256(payload[:prior_bytes]).hexdigest() != prior_digest:
        raise RuntimeError(f"append-only ledger prefix drifted: {path}")
    rows = _decode_jsonl_suffix(
        payload[prior_bytes:], path=path,
        first_row_number=prior_rows + 1)
    return {
        "byte_count": len(payload),
        "row_count": prior_rows + len(rows),
        "prefix_sha256": hashlib.sha256(payload).hexdigest(),
    }, rows


def _initialize_deepseek_exit_audit_state(out_dir):
    cursors = {}
    rows_by_name = {}
    empty_cursor = {
        "byte_count": 0,
        "row_count": 0,
        "prefix_sha256": hashlib.sha256(b"").hexdigest(),
    }
    for name in _DEEPSEEK_INCREMENTAL_LEDGER_FILES:
        cursor, rows = _advance_append_only_ledger_cursor(
            os.path.join(out_dir, name), empty_cursor)
        cursors[name] = cursor
        rows_by_name[name] = rows
    request_ids = set()
    for number, row in enumerate(rows_by_name["api_calls.jsonl"], 1):
        request_id = row.get("request_id")
        if (not isinstance(request_id, str) or not request_id
                or request_id in request_ids):
            raise RuntimeError(
                f"duplicate/invalid API request id at row {number}")
        request_ids.add(request_id)
    return {
        "cursors": cursors,
        "rows": rows_by_name,
        "request_ids": request_ids,
        "terminal_since_full": 0,
    }


def _audit_deepseek_exit_delta(
        out_dir, manifest, state, *, exited_samples, active_samples,
        required_complete_samples):
    next_cursors = {}
    delta_rows = {}
    for name in _DEEPSEEK_INCREMENTAL_LEDGER_FILES:
        cursor, rows = _advance_append_only_ledger_cursor(
            os.path.join(out_dir, name), state["cursors"][name])
        next_cursors[name] = cursor
        delta_rows[name] = rows
    next_rows = {
        name: list(state["rows"][name]) + list(delta_rows[name])
        for name in _DEEPSEEK_INCREMENTAL_LEDGER_FILES
    }
    next_request_ids = set(state["request_ids"])
    errors = []
    expected_samples = set((manifest.get("config") or {}).get("samples") or [])
    new_api_samples = set()
    first_api_number = (
        state["cursors"]["api_calls.jsonl"]["row_count"] + 1)
    for number, row in enumerate(
            delta_rows["api_calls.jsonl"], first_api_number):
        request_id = row.get("request_id")
        sample = row.get("sample")
        if (not isinstance(request_id, str) or not request_id
                or request_id in next_request_ids):
            errors.append(
                f"duplicate/invalid API request id at row {number}")
        else:
            next_request_ids.add(request_id)
        if sample not in expected_samples:
            errors.append(f"unmappable API ledger row {number}")
        else:
            new_api_samples.add(sample)

    dispatch_rows = next_rows["dispatch_log.jsonl"]
    launches = {
        row.get("worker_launch_id"): row
        for row in dispatch_rows
        if row.get("event") == "launch"
        and isinstance(row.get("worker_launch_id"), str)
    }
    errors.extend(_inspect_deepseek_key_failover_audit(
        manifest,
        dispatch_rows,
        next_rows["api_calls.jsonl"],
        next_rows["sample_outcomes.jsonl"],
        launches,
    ))
    audit_samples = set(exited_samples) | new_api_samples
    inspect_started = time.monotonic()
    local = inspect_campaign(
        out_dir,
        manifest,
        require_terminal_provenance=True,
        active_samples=set(active_samples),
        required_complete_samples=set(required_complete_samples),
        audit_samples=audit_samples,
        terminal_audit_samples=set(exited_samples),
        preloaded_ledgers=next_rows,
    )
    record_slow_control_operation(
        out_dir, "incremental_exit_audit",
        time.monotonic() - inspect_started,
        audit_sample_count=len(audit_samples),
        terminal_sample_count=len(exited_samples),
        api_row_count=next_cursors["api_calls.jsonl"]["row_count"],
    )
    errors.extend(local.get("errors") or [])
    if errors:
        return {
            "errors": sorted(set(errors)),
            "api_calls": next_cursors["api_calls.jsonl"]["row_count"],
            "preservation_violations": local.get(
                "preservation_violations", 0),
        }, None
    next_state = {
        "cursors": next_cursors,
        "rows": next_rows,
        "request_ids": next_request_ids,
        "terminal_since_full": state["terminal_since_full"],
    }
    local["api_calls"] = next_cursors["api_calls.jsonl"]["row_count"]
    return local, next_state


def _run_worker_queue(
        args, out_dir, inspection_manifest, task_plans, keys, assignments,
        resume_authorizations, dispatch_log, running,
        infrastructure_incomplete_samples, evaluator_incomplete_samples,
        completed_samples, total_assignment_count, slots_per_key,
        not_started_no_healthy_key_samples=None,
        resume_pending_no_healthy_key_samples=None):
    """Drain stable per-key FIFO queues without a cross-key wave barrier."""
    if running:
        raise RuntimeError("cannot start a worker queue while workers are active")
    if args.campaign_role in DEEPSEEK_CAMPAIGN_ROLES:
        dispatch_rows = _read_jsonl(dispatch_log)
        # Validate the existing durable routing history before appending an
        # operator reactivation event.  A corrupt history must remain
        # byte-for-byte unchanged when resume fails closed.
        restored = _restore_key_failover_queue_state(
            inspection_manifest, dispatch_rows, assignments, keys)
        dispatch_rows = _append_key_reactivation_events(
            dispatch_log, dispatch_rows, keys,
            getattr(args, "reactivate_key_label", None),
            getattr(args, "key_reactivation_reason", None)
            or getattr(args, "resume_reason", None),
            pending_work_exists=_queue_has_pending_work(restored),
        )
        if getattr(args, "reactivate_key_label", None):
            restored = _restore_key_failover_queue_state(
                inspection_manifest, dispatch_rows, assignments, keys)
    else:
        pending_by_key = {}
        failover_counts = {}
        seen_samples = set()
        for source_item in assignments:
            sample = source_item.get("sample")
            label = source_item.get("key_label")
            if (not isinstance(sample, str) or not sample
                    or sample in seen_samples
                    or not isinstance(label, str) or not label):
                raise RuntimeError("worker queue assignment identity is invalid")
            seen_samples.add(sample)
            item = dict(source_item)
            item.setdefault("original_key_label", label)
            item.setdefault("failover_count", 0)
            pending_by_key.setdefault(label, []).append(item)
            failover_counts[sample] = _failover_count(item)
        restored = {
            "pending_by_key": pending_by_key,
            "handoff_fifo": [],
            "quarantined_keys": set(),
            "quota_observation_counts": {},
            "failover_counts": failover_counts,
        }
    pending_by_key = restored["pending_by_key"]
    failover_counts = restored["failover_counts"]
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
    quarantined_keys = restored["quarantined_keys"]
    quota_observation_counts = restored["quota_observation_counts"]
    handoff_fifo = restored["handoff_fifo"]
    not_started_no_healthy_key_samples = (
        not_started_no_healthy_key_samples
        if not_started_no_healthy_key_samples is not None else set()
    )
    resume_pending_no_healthy_key_samples = (
        resume_pending_no_healthy_key_samples
        if resume_pending_no_healthy_key_samples is not None else set()
    )
    last_report = 0.0
    last_inspection = {
        "errors": [], "api_calls": 0, "preservation_violations": 0}
    incremental_exit_audit = (
        method_phase is None
        and _supports_incremental_exit_audit(inspection_manifest)
        and getattr(
            args, "_deepseek_exit_audit_preflight_manifest_sha256", None)
        == _canonical_record_sha256(inspection_manifest)
    )
    exit_audit_state = (
        _initialize_deepseek_exit_audit_state(out_dir)
        if incremental_exit_audit else None
    )
    while running or any(pending_by_key.values()) or handoff_fifo:
        _raise_if_operator_pause_requested(args, "worker_queue_refill")
        # Recheck the durable stop latch at the refill boundary.  The check
        # after each process poll protects already-running workers, while this
        # one closes the narrow window between a clean post-exit inspection
        # and launching replacement workers on the next loop iteration.
        stop_records = read_campaign_stop_conditions(out_dir)
        if stop_records:
            conditions = sorted({
                str(record.get("condition") or "unknown")
                for record in stop_records
            })
            raise RuntimeError(
                "campaign stop latch set before worker queue refill: "
                + ", ".join(conditions)
            )
        batch = _take_available_assignments(
            pending_by_key, running, slots_per_key,
            quarantined_keys=quarantined_keys,
            handoff_fifo=handoff_fifo,
            dispatch_log=dispatch_log,
            failover_counts=failover_counts)
        if batch:
            refill_index += 1
            if args.campaign_role == "confirmation":
                evaluate_confirmation_known_usage_gate(
                    out_dir, inspection_manifest, refill_index,
                    phase="pre_refill")
            _raise_if_operator_pause_requested(
                args, "before_worker_queue_refill_launch")
            launched = _launch_worker_batch(
                args, out_dir, inspection_manifest, task_plans, keys, batch,
                resume_authorizations, dispatch_log, running)
            launched_samples = set(launched)
            if launched_samples != {item["sample"] for item in batch}:
                raise RuntimeError("worker launch batch returned incomplete")
            for sample in launched_samples:
                # Once a replacement worker is actually launched, any prior
                # terminal infrastructure status is superseded regardless of
                # whether routing changed label or reactivated the same label.
                infrastructure_incomplete_samples.discard(sample)
                not_started_no_healthy_key_samples.discard(sample)
                resume_pending_no_healthy_key_samples.discard(sample)
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
            if any(pending_by_key.values()) or handoff_fifo:
                stranded_items = [
                    item
                    for items in pending_by_key.values()
                    for item in items
                ] + list(handoff_fifo)
                not_started = sorted({
                    item["sample"] for item in stranded_items
                    if _pending_provenance(item) == PENDING_NEVER_STARTED
                })
                stranded = sorted({item["sample"] for item in stranded_items})
                provenance_by_sample = {
                    item["sample"]: _pending_provenance(item)
                    for item in stranded_items
                    if _pending_provenance(item) is not None
                }
                infrastructure_pending = sorted(
                    (set(stranded) & set(infrastructure_incomplete_samples))
                    | {
                        item["sample"] for item in stranded_items
                        if _pending_provenance(item)
                        == PENDING_INFRASTRUCTURE_INCOMPLETE
                    })
                for sample in infrastructure_pending:
                    provenance_by_sample.setdefault(
                        sample, PENDING_INFRASTRUCTURE_INCOMPLETE)
                not_started = sorted(
                    set(not_started) - set(infrastructure_pending))
                resume_pending = sorted(
                    item["sample"] for item in stranded_items
                    if _pending_provenance(item) == PENDING_INTERRUPTED
                )
                resume_pending = sorted(
                    set(resume_pending)
                    - set(not_started)
                    - set(infrastructure_pending))
                partition_sets = [
                    set(infrastructure_pending), set(not_started),
                    set(resume_pending),
                ]
                partition_union = set().union(*partition_sets)
                if (sum(len(items) for items in partition_sets)
                        != len(partition_union)
                        or partition_union != set(stranded)):
                    raise RuntimeError(
                        "queue exhaustion pending partition is invalid")
                if (set(provenance_by_sample) != set(stranded)
                        or any(value not in {
                            PENDING_INFRASTRUCTURE_INCOMPLETE,
                            PENDING_NEVER_STARTED,
                            PENDING_INTERRUPTED,
                        } for value in provenance_by_sample.values())):
                    raise RuntimeError(
                        "queue exhaustion pending provenance is invalid")
                infrastructure_incomplete_samples.update(
                    infrastructure_pending)
                not_started_no_healthy_key_samples.update(not_started)
                resume_pending_no_healthy_key_samples.update(resume_pending)
                append_jsonl_locked(
                    dispatch_log,
                    {
                        "event": "queue_exhausted_no_healthy_key",
                        "reason": DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                        "method_phase": method_phase,
                        "quarantined_key_labels": sorted(
                            quarantined_keys),
                        "pending_samples": stranded,
                        "infrastructure_incomplete_samples": (
                            infrastructure_pending),
                        "not_started_pending_samples": not_started,
                        "resume_pending_no_healthy_key_samples": (
                            resume_pending),
                        "pending_sample_provenance": {
                            sample: provenance_by_sample[sample]
                            for sample in stranded
                        },
                        "created_at": datetime.now().astimezone().isoformat(
                            timespec="seconds"),
                    },
                )
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
                evidence = _deepseek_monthly_quota_quarantine_evidence(
                    out_dir, sample, item)
                if evidence is not None:
                    handoff_fifo.append(dict(item))
                    _append_key_quarantine(
                        dispatch_log, item["key_label"], sample, item,
                        evidence, pending_by_key, handoff_fifo,
                        quarantined_keys, quota_observation_counts)
                continue
            if disposition == "evaluator_incomplete":
                infrastructure_incomplete_samples.discard(sample)
                evaluator_incomplete_samples.add(sample)
                continue
            infrastructure_incomplete_samples.discard(sample)
            completed_samples.add(sample)
            completed_in_poll.add(sample)
        if exited:
            _publish_active_worker_set(
                out_dir, inspection_manifest, running.values(), args=args)
        stop_records = read_campaign_stop_conditions(out_dir)
        if stop_records:
            conditions = sorted({
                str(record.get("condition") or "unknown")
                for record in stop_records
            })
            raise RuntimeError(
                "campaign stop latch set while worker queue is running: "
                + ", ".join(conditions)
            )
        if exited:
            _raise_if_operator_pause_requested(
                args, "before_worker_exit_campaign_audit")
            if incremental_exit_audit:
                last_inspection, candidate_state = (
                    _audit_deepseek_exit_delta(
                        out_dir,
                        inspection_manifest,
                        exit_audit_state,
                        exited_samples={item["sample"] for item in exited},
                        active_samples=set(running),
                        required_complete_samples=completed_in_poll,
                    )
                )
                if last_inspection["errors"]:
                    raise RuntimeError(
                        "; ".join(last_inspection["errors"]))
                candidate_state["terminal_since_full"] += len(exited)
                if (candidate_state["terminal_since_full"]
                        >= DEEPSEEK_EXIT_FULL_AUDIT_INTERVAL):
                    full_audit_started = time.monotonic()
                    full_inspection = inspect_campaign(
                        out_dir,
                        inspection_manifest,
                        active_samples=set(running),
                        required_complete_samples=set(completed_samples),
                    )
                    record_slow_control_operation(
                        out_dir, "periodic_full_campaign_audit",
                        time.monotonic() - full_audit_started,
                        terminal_worker_interval=(
                            candidate_state["terminal_since_full"]),
                        completed_sample_count=len(completed_samples),
                        active_sample_count=len(running),
                    )
                    if full_inspection["errors"]:
                        raise RuntimeError(
                            "; ".join(full_inspection["errors"]))
                    candidate_state["terminal_since_full"] = 0
                    last_inspection = full_inspection
                exit_audit_state = candidate_state
            else:
                last_inspection = inspect_campaign(
                    out_dir, inspection_manifest,
                    active_samples=set(running),
                    required_complete_samples=completed_in_poll,
                    method_phase=method_phase,
                )
            _raise_if_operator_pause_requested(
                args, "after_worker_exit_campaign_audit")
            if last_inspection["errors"]:
                raise RuntimeError("; ".join(last_inspection["errors"]))
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
        _raise_if_operator_pause_requested(args, "after_worker_queue_poll")
    _publish_active_worker_set(out_dir, inspection_manifest, [], args=args)
    append_jsonl_locked(
        dispatch_log,
        {
            "event": "queue_complete",
            "refill_count": refill_index,
            "method_phase": method_phase,
            "completed_samples": sorted(completed_samples),
            "infrastructure_incomplete_samples": sorted(
                infrastructure_incomplete_samples),
            "not_started_no_healthy_key_samples": sorted(
                not_started_no_healthy_key_samples),
            "resume_pending_no_healthy_key_samples": sorted(
                resume_pending_no_healthy_key_samples),
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
        require_terminal_provenance=True,
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


def _launch_under_lease_impl(args, out_dir):
    # A recovery authorizer must archive the active latch before a dispatcher
    # is allowed to mutate metadata, task plans, the manifest, or active-set
    # evidence.  In particular, ordinary --resume must not destroy the cohort
    # witness left by an operator pause.
    _assert_no_active_campaign_stop_before_launch(out_dir)
    _raise_if_operator_pause_requested(args, "dispatcher_initialization")
    args._dispatcher_instance_id = (
        f"dispatcher-{os.getpid()}-{uuid.uuid4().hex}"
    )
    pending_parent_loss = os.path.join(
        out_dir, DISPATCHER_PARENT_LOSS_PENDING_FILENAME)
    pending_inspector = os.path.join(
        out_dir, DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME)
    if os.path.isfile(pending_parent_loss):
        raise RuntimeError(
            "dispatcher parent-loss recovery transaction is pending; "
            "rerun authorize_ledger_lock_recovery.py "
            "--dispatcher_process_lost before resume"
        )
    if os.path.isfile(pending_inspector):
        pending = _read_json(pending_inspector)
        recovery_mode = (
            "--deepseek_resume_classifier_retry"
            if pending.get("followup_kind") == "resume_classifier"
            else "--deepseek_transport_disconnect_retry"
        )
        raise RuntimeError(
            "DeepSeek transport inspector recovery transaction is pending; "
            "rerun authorize_ledger_lock_recovery.py "
            f"{recovery_mode} before resume"
        )
    _resolve_confirmation_selection(args, out_dir=out_dir)
    _resolve_full234_scope(args)
    _resolve_remaining134_scope(args)
    _validate_campaign_grid(args)
    if args.campaign_role not in DEEPSEEK_CAMPAIGN_ROLES:
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
        if args.campaign_role == "remaining134"
        else DEEPSEEK_CAPACITY_KEY_COUNT
        if args.campaign_role == "deepseek_capacity15"
        else DEEPSEEK_FULL_KEY_COUNT
        if args.campaign_role == "deepseek_full234" else None
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
    if args.campaign_role in {
            "confirmation", "full234", "remaining134",
            "deepseek_capacity15", "deepseek_full234"}:
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
                "supplemental", "confirmation", "full234",
                "deepseek_capacity15", "deepseek_full234"}
        ),
        allow_queue=(args.campaign_role in {
            "confirmation", "full234", "remaining134",
            "deepseek_capacity15", "deepseek_full234"}),
    )
    if args.campaign_role == "remaining134":
        for item in assignments:
            item["methods"] = list(REMAINING134_METHOD_PHASES)

    args.snapshot_mode = _resolve_paired_snapshot_mode(out_dir, args)
    _raise_if_operator_pause_requested(args, "before_task_plan_preparation")
    task_plans = prepare_task_plans(
        out_dir, args.samples, args.num_round_trips, args.seed)
    manifest = build_manifest(
        out_dir, args.samples, assignments, task_plans, args,
        upstream_smoke_gate=upstream_smoke_gate)
    manifest_path, inspection_manifest = write_or_verify_manifest(
        out_dir, manifest, resume=args.resume)
    if _supports_audited_operator_pause(args):
        args._operator_pause_dispatch_manifest_sha256 = _sha256_path(
            manifest_path)
    else:
        for attr in (
                "_operator_pause_dispatch_manifest_sha256",
                "_operator_pause_active_worker_set_sha256",
                "_operator_pause_active_worker_set_canonical_sha256",
                "_operator_pause_active_worker_launch_ids"):
            if hasattr(args, attr):
                delattr(args, attr)
    _raise_if_operator_pause_requested(args, "after_manifest_preparation")
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
                    args.campaign_role in {
                        "confirmation", "full234",
                        "deepseek_capacity15", "deepseek_full234"}),
                allow_deepseek_resume=(
                    args.resume and args.campaign_role in
                    DEEPSEEK_CAMPAIGN_ROLES),
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
    not_started_no_healthy_key_samples = set()
    resume_pending_no_healthy_key_samples = set()
    evaluator_incomplete_samples = set()
    completed_samples = set()
    audited_stale = []
    stale = []
    try:
        _raise_if_operator_pause_requested(args, "before_resume_reconciliation")
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
        _publish_active_worker_set(
            out_dir, inspection_manifest, [], args=args)
        if args.campaign_role == "remaining134":
            return _run_remaining134_campaign(
                args, out_dir, inspection_manifest, task_plans, keys,
                assignments, dispatch_log, running,
                audited_stale=audited_stale, closed_stale=stale,
            )
        preflight_started = time.monotonic()
        preflight = inspect_campaign(
            out_dir, inspection_manifest,
            active_samples=set(args.samples),
        )
        record_slow_control_operation(
            out_dir, "campaign_preflight_full_audit",
            time.monotonic() - preflight_started,
            sample_count=len(args.samples),
            campaign_role=args.campaign_role,
        )
        if preflight["errors"]:
            raise RuntimeError(
                "campaign preflight failed before worker launch: "
                + "; ".join(preflight["errors"])
            )
        # The append-only exit-audit cursor is only a safe optimization after
        # this exact manifest has passed a full campaign preflight.  Bind that
        # fact in memory so direct queue callers and legacy resumes continue to
        # use the conservative full inspector.
        args._deepseek_exit_audit_preflight_manifest_sha256 = (
            _canonical_record_sha256(inspection_manifest)
        )
        launch_assignments, resume_authorizations = (
            _select_invocation_assignments(
                out_dir, assignments, resume=args.resume,
                target_round_trips=args.num_round_trips,
                allow_pristine_pending=(
                    args.campaign_role in {
                        "confirmation", "full234",
                        "deepseek_capacity15", "deepseek_full234"}),
                allow_deepseek_resume=(
                    args.resume and args.campaign_role in
                    DEEPSEEK_CAMPAIGN_ROLES),
            )
        )
        latest_outcomes = _latest_sample_outcomes(
            out_dir, [item["sample"] for item in assignments])
        incomplete_samples = {
            sample for sample, outcome in latest_outcomes.items()
            if outcome.get("status") == "infrastructure_incomplete"
        }
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
            if args.campaign_role in {
                    "confirmation", "full234",
                    "deepseek_capacity15", "deepseek_full234"}:
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
        queue_kwargs = {}
        if args.campaign_role in DEEPSEEK_CAMPAIGN_ROLES:
            queue_kwargs["not_started_no_healthy_key_samples"] = (
                not_started_no_healthy_key_samples)
            queue_kwargs["resume_pending_no_healthy_key_samples"] = (
                resume_pending_no_healthy_key_samples)
        _run_worker_queue(
            args, out_dir, inspection_manifest, task_plans, keys,
            launch_assignments, resume_authorizations, dispatch_log,
            running, incomplete_samples, evaluator_incomplete_samples,
            completed_samples, len(assignments), slots_per_key,
            **queue_kwargs,
        )
        if (incomplete_samples or evaluator_incomplete_samples
                or not_started_no_healthy_key_samples
                or resume_pending_no_healthy_key_samples):
            terminal_audit_started = time.monotonic()
            inspection = inspect_campaign(
                out_dir, inspection_manifest,
                required_complete_samples=completed_samples,
                require_terminal_provenance=True)
            record_slow_control_operation(
                out_dir, "campaign_terminal_full_audit",
                time.monotonic() - terminal_audit_started,
                campaign_complete=False,
                completed_sample_count=len(completed_samples),
            )
            if inspection["errors"]:
                raise RuntimeError("; ".join(inspection["errors"]))
            append_jsonl_locked(
                dispatch_log,
                {
                    "event": "campaign_incomplete",
                    "infrastructure_incomplete_samples": sorted(
                        incomplete_samples),
                    "not_started_no_healthy_key_samples": sorted(
                        not_started_no_healthy_key_samples),
                    "resume_pending_no_healthy_key_samples": sorted(
                        resume_pending_no_healthy_key_samples),
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
                + " not_started_no_healthy_key_samples="
                + ",".join(sorted(not_started_no_healthy_key_samples))
                + " resume_pending_no_healthy_key_samples="
                + ",".join(sorted(resume_pending_no_healthy_key_samples))
                + " evaluator_samples="
                + ",".join(sorted(evaluator_incomplete_samples)),
                file=sys.stderr, flush=True,
            )
            return 2
        terminal_audit_started = time.monotonic()
        inspection = inspect_campaign(
            out_dir, inspection_manifest, require_complete=True)
        record_slow_control_operation(
            out_dir, "campaign_terminal_full_audit",
            time.monotonic() - terminal_audit_started,
            campaign_complete=True,
            completed_sample_count=len(completed_samples),
        )
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
    except OperatorPauseRequested as exc:
        active_worker_ids = _operator_pause_active_worker_ids(args, running)
        if active_worker_ids:
            try:
                # Publish the latch before process teardown.  Workers that are
                # between provider guards must observe a durable stop even if
                # dispatcher-side termination or lease reconciliation later
                # fails.
                stop_record = _record_operator_pause_condition(
                    out_dir, args, exc, inspection_manifest, running)
            except BaseException as stop_error:
                canonical_stop_error_text = str(stop_error)
                try:
                    stop_record = _record_operator_pause_emergency_condition(
                        out_dir, args, exc, inspection_manifest, running,
                        stop_error)
                except BaseException as emergency_stop_error:
                    stop_record = None
                    stop_error_text = (
                        canonical_stop_error_text
                        + "; emergency fallback failed: "
                        + str(emergency_stop_error)
                    )
                    stop_publication = "failed"
                else:
                    stop_error_text = None
                    stop_publication = "emergency_fallback"
            else:
                stop_error_text = None
                canonical_stop_error_text = None
                stop_publication = stop_record.get(
                    "publication_mode", "canonical")
        else:
            # Between cohorts there is no provider-capable process to revoke.
            # Returning without a campaign-wide latch keeps the next ordinary
            # checkpoint resume available while the dispatch log records the
            # operator cancellation.
            stop_record = None
            stop_error_text = None
            canonical_stop_error_text = None
            stop_publication = "not_required"
        reconciliation = _stop_and_reconcile_workers(
            out_dir, running, dispatch_log, lease_scope=args.samples,
            record_worker_exits=False, close_running_metadata=False)
        reconciliation["active_set_retained"] = True
        if stop_error_text is not None:
            reconciliation["operator_pause_stop_error"] = stop_error_text
        if canonical_stop_error_text is not None:
            reconciliation["operator_pause_canonical_stop_error"] = (
                canonical_stop_error_text)
        pause_valid = (
            (not active_worker_ids or stop_record is not None)
            and stop_error_text is None
            and reconciliation.get("termination_error") is None
            and reconciliation.get("lease_error") is None
        )
        append_jsonl_locked(dispatch_log, {
            "event": "operator_pause" if pause_valid
            else "operator_pause_failed",
            "condition": OPERATOR_PAUSE_CONDITION,
            "signal_name": exc.signal_name,
            "signal_number": exc.signum,
            "exit_code": exc.exit_code,
            "boundary": exc.boundary,
            "campaign_stop_created": stop_record is not None,
            "campaign_stop_publication": stop_publication,
            "active_worker_launch_ids": active_worker_ids,
            "worker_reconciliation": reconciliation,
        })
        if not pause_valid:
            print(
                "RESULT FAIL operator pause could not establish a durable "
                "stopped cohort",
                file=sys.stderr, flush=True,
            )
            return 1
        print(
            f"RESULT PAUSED operator_pause signal={exc.signal_name}",
            file=sys.stderr, flush=True,
        )
        return exc.exit_code
    except BaseException as exc:
        try:
            record_campaign_stop_condition(
                out_dir, "dispatcher_integrity_failure",
                error_type=type(exc).__name__, error=str(exc),
            )
        except BaseException:
            pass
        reconciliation = _stop_and_reconcile_workers(
            out_dir, running, dispatch_log, lease_scope=args.samples)
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
            reconciliation["active_set_retained"] = (
                reconciliation.get("lease_scope") or sorted(running)
            )
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


def _launch_under_lease(args, out_dir):
    """Run one dispatcher while treating pre-worker signals as cancellation.

    Once the runtime try-block has been entered, `_launch_under_lease_impl`
    publishes a durable operator-pause latch and reconciles its worker cohort.
    A signal during local preflight has no worker/provider state to recover, so
    it returns the conventional signal exit code without inventing a campaign
    stop record.
    """
    try:
        return _launch_under_lease_impl(args, out_dir)
    except OperatorPauseRequested as exc:
        print(
            f"RESULT PAUSED prelaunch signal={exc.signal_name}",
            file=sys.stderr, flush=True,
        )
        return exc.exit_code


def launch(args):
    if os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID"):
        raise RuntimeError(
            "paired dispatcher cannot inherit ANCHORPATCH_WORKER_LAUNCH_ID")
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
    lease = open(lease_path, "a+", encoding="utf-8")
    try:
        signal_scope = (
            _operator_pause_signal_scope(args)
            if _supports_audited_operator_pause(args)
            else contextlib.nullcontext()
        )
        with signal_scope:
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
            "remaining134", "deepseek_capacity15", "deepseek_full234"),
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
        "--reactivate_key_label", action="append", default=[],
        help="resume-only DeepSeek key label whose secret was operator-replaced")
    parser.add_argument(
        "--key_reactivation_reason",
        help="optional reason for --reactivate_key_label; defaults to --resume_reason")
    parser.add_argument(
        "--snapshot_mode",
        choices=(SNAPSHOT_MODE_ALL, SNAPSHOT_MODE_FAILURES, SNAPSHOT_MODE_OFF),
        default=None,
        help=(
            "worker document snapshot policy; new paired campaigns default to "
            "failures, while resumes inherit the existing manifest and legacy "
            "manifests without this field resume as all"
        ),
    )
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
    elif args.campaign_role in {
            "full234", "remaining134", "deepseek_full234"}:
        if manual_samples:
            parser.error(
                f"{args.campaign_role} samples are derived automatically"
            )
        if args.selection_manifest:
            parser.error(
                "--selection_manifest is only valid for confirmation"
            )
        try:
            if args.campaign_role in {"full234", "deepseek_full234"}:
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
    elif args.campaign_role == "deepseek_capacity15":
        if (args.samples != DEEPSEEK_CAPACITY_SAMPLES
                or args.num_round_trips != 2):
            parser.error(
                "deepseek_capacity15 requires the fixed 15-sample order "
                "with 2 round trips")
        if args.smoke_dir:
            parser.error(
                "--smoke_dir is not valid for deepseek_capacity15")
        if args.slots_per_key != DEEPSEEK_CAPACITY_SLOTS_PER_KEY:
            parser.error(
                "deepseek_capacity15 requires --slots_per_key 15")
        if not args.key_labels or len(args.key_labels) != 1:
            parser.error(
                "deepseek_capacity15 requires exactly one --key_labels entry")
    elif args.campaign_role == "deepseek_full234":
        if (len(args.samples) != FULL234_SAMPLE_COUNT
                or args.num_round_trips
                != DEEPSEEK_FULL234_ROUND_TRIPS):
            parser.error(
                "deepseek_full234 requires the exact 234-sample inventory "
                f"with {DEEPSEEK_FULL234_ROUND_TRIPS} round trips")
        if args.smoke_dir:
            parser.error("--smoke_dir is not valid for deepseek_full234")
        if args.slots_per_key != DEEPSEEK_FULL_SLOTS_PER_KEY:
            parser.error(
                "deepseek_full234 requires --slots_per_key 10")
        if (not args.key_labels
                or len(args.key_labels) != DEEPSEEK_FULL_KEY_COUNT):
            parser.error(
                "deepseek_full234 requires exactly three "
                "--key_labels entries")
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
    if args.reactivate_key_label:
        if not args.resume:
            parser.error("--reactivate_key_label requires --resume")
        if args.campaign_role not in DEEPSEEK_CAMPAIGN_ROLES:
            parser.error("--reactivate_key_label is only valid for DeepSeek campaigns")
        if len(args.reactivate_key_label) != len(set(args.reactivate_key_label)):
            parser.error("--reactivate_key_label contains duplicates")
    if args.key_reactivation_reason and not args.reactivate_key_label:
        parser.error("--key_reactivation_reason requires --reactivate_key_label")
    return launch(args)


if __name__ == "__main__":
    raise SystemExit(main())
