"""Fail-fast paired MiniMax campaign launcher for HP_V8.

The dispatcher never prints key values.  It pre-generates and hashes every
task plan, records a deterministic sample/key-label/method-order manifest,
launches one process per sample, and stops all workers on identity drift,
worker failure, preservation violations, duplicate/partial committed rounds,
or an unmappable API ledger row.
"""

import argparse
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
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
    append_jsonl_locked,
    code_fingerprint,
    interrupt_audited_running_invocations,
    interrupt_running_invocations,
    read_campaign_stop_conditions,
    read_run_metadata_snapshot,
    write_json_atomic,
)
from utils_env import load_sample
from utils_relay_plan import (
    build_relay_task_plan,
    load_relay_task_plan,
    save_relay_task_plan,
)


SCHEMA = "anchorpatch.paired_campaign_manifest/1"
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
            for line_number, line in enumerate(handle, 1):
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
            "transport_revision": "opencode_anthropic_sdk/3",
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

    api_rows = _read_jsonl(os.path.join(out_dir, "api_calls.jsonl"))
    api_keys = set()
    semantic_groups = {}
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
            if formal_manifest:
                worker_id = row.get("worker_launch_id")
                worker_pid = row.get("worker_pid")
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
            call_kind = row.get("call_kind")
            allowed_kinds = (
                {"hybridpatch_primary", "hybridpatch_repair"}
                if method == "hybridpatch" else {"fullrewrite_primary"}
            )
            if call_kind not in allowed_kinds:
                errors.append(
                    f"unexpected call_kind at API row {index}: {call_kind!r}"
                )
            if row.get("transport_revision") != "opencode_anthropic_sdk/3":
                errors.append(f"transport revision mismatch at API row {index}")
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
            if not isinstance(semantic_call_id, str) or not semantic_call_id:
                errors.append(f"missing semantic_call_id at API row {index}")
            elif not isinstance(row.get("provider_called"), bool):
                errors.append(f"invalid provider_called at API row {index}")
            else:
                semantic_groups.setdefault(semantic_call_id, []).append(
                    (index, row)
                )

    calls_by_step = {}
    provider_call_rows = 0
    for semantic_call_id, group in semantic_groups.items():
        signatures = {
            (
                row.get("sample"), row.get("method"), row.get("rt_index"),
                row.get("direction"), row.get("call_kind"),
            )
            for _index, row in group
        }
        if len(signatures) != 1:
            errors.append(
                f"semantic-call identity drift: {semantic_call_id}"
            )
            continue
        sample, method, rt, direction, call_kind = next(iter(signatures))
        step = (sample, method, rt, direction)
        calls_by_step.setdefault(step, []).append(
            (call_kind, semantic_call_id)
        )
        provider_rows = [
            (index, row) for index, row in group
            if row.get("provider_called") is True
        ]
        replay_rows = [
            (index, row) for index, row in group
            if row.get("provider_called") is False
        ]
        provider_call_rows += len(provider_rows)
        if len(provider_rows) > 1:
            errors.append(
                f"duplicate provider POST for semantic call: {semantic_call_id}"
            )
        for index, row in provider_rows:
            if row.get("response_replayed") or row.get("replayed_from_call_id"):
                errors.append(
                    f"provider row has replay markers at API row {index}"
                )
        if replay_rows:
            digest = hashlib.sha256(
                semantic_call_id.encode("utf-8")
            ).hexdigest()[:24]
            journal_path = os.path.join(
                out_dir, "api_journal", f"{digest}.response.json"
            )
            journal = _read_json(journal_path) if os.path.isfile(
                journal_path) else None
            if (not isinstance(journal, dict)
                    or journal.get("semantic_call_id") != semantic_call_id
                    or not isinstance(journal.get("call_id"), str)
                    or not journal.get("call_id")):
                errors.append(
                    f"missing/mismatched response journal: {semantic_call_id}"
                )
            else:
                journal_call_id = journal["call_id"]
                for index, row in replay_rows:
                    if (not row.get("response_replayed")
                            or row.get("replayed_from_call_id") != journal_call_id):
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
            result_path = os.path.join(out_dir, method, f"{sample}.jsonl")
            rows = _read_jsonl(result_path)
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
            "transport_revision": "opencode_anthropic_sdk/3",
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


def _append_worker_exit(sample, item, returncode, dispatch_log):
    if item.get("exit_recorded"):
        return
    append_jsonl_locked(
        dispatch_log,
        {
            "event": "worker_exit", "sample": sample,
            "key_label": item["key_label"],
            "worker_launch_id": item["worker_launch_id"],
            "pid": item["process"].pid,
            "returncode": returncode,
            "created_at": datetime.now().astimezone().isoformat(
                timespec="seconds"),
        },
    )
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
        result["closed_invocations"] = interrupt_running_invocations(
            out_dir,
            status="interrupted_by_dispatcher",
            worker_launch_ids={
                item["worker_launch_id"] for item in running.values()
            },
        )
    except BaseException as exc:
        result["metadata_error"] = str(exc)
    return result


def _record_worker_exit(running, sample, item, returncode, dispatch_log):
    """Keep failed workers in ``running`` until metadata reconciliation."""
    item["log"].close()
    _append_worker_exit(sample, item, returncode, dispatch_log)
    if returncode != 0:
        raise RuntimeError(f"worker {sample} exited with {returncode}")
    del running[sample]


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
        dry_inspection = inspect_campaign(
            out_dir, inspection_manifest,
            active_samples=set(args.samples),
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
    try:
        _write_active_worker_set(out_dir, inspection_manifest, [])
        _assert_worker_leases_free(out_dir, args.samples)
        preflight = inspect_campaign(
            out_dir, inspection_manifest,
            active_samples=set(args.samples),
        )
        if preflight["errors"]:
            raise RuntimeError(
                "campaign preflight failed before worker launch: "
                + "; ".join(preflight["errors"])
            )
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
                    "assignments": assignments,
                },
            )
        launch_specs = {}
        for item in assignments:
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
                "process": process,
                "log": log_handle,
                "key_label": label,
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
                _record_worker_exit(
                    running, sample, item, returncode, dispatch_log)
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
            _write_active_worker_set(out_dir, inspection_manifest, [])
        except BaseException:
            pass
        reconciliation = _stop_and_reconcile_workers(
            out_dir, running, dispatch_log)
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
