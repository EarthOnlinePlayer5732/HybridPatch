"""
HybridPatch vs FullRewrite round-trip relay on DELEGATE-52.

Both methods share the same samples, the same seeded forward-state sequence
(utils_relay_plan), the same domain evaluator, the same model, and the same
distractor setting — a fair paired comparison. RS is the headline metric
(evaluation.score at each backward step); preservation / op-accept / no-op /
ECR telemetry are recorded alongside (see analyze.py).

Results are written as run_relay-compatible JSONL: <out_dir>/<method>/<sample>.jsonl
Each round trip is atomic (both rows + checkpoint advance together) -> idempotent resume.

Run from repo root, single process, one sample per invocation for subagent parallelism:
  PYTHONUTF8=1 python src/experiment_runner.py --sample malware6 --methods hybridpatch fullrewrite
"""
import os
import sys
import json
import argparse
import re
import random
import fnmatch
import time
import threading

import portalocker

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from utils_env import load_sample, shuffle_context, merge_distractor, load_distractor_context
from utils_context import (build_context_from_folder, parse_context_string,
                           is_context_complete, is_wildcard, validate_wildcard_context)
from utils_relay_plan import build_relay_task_plan, save_relay_task_plan, load_relay_task_plan
from utils_results import generate_response_id
from domains import get_domain

from run_meta import (RunLogger, dump_step_docs, append_run_metadata,
                      finish_run_metadata, register_task_plan,
                      CampaignStoppedError,
                      ApiCallRecorder, record_model_content_anomaly,
                      append_relay_rows_and_checkpoint, write_json_atomic,
                      append_jsonl_locked,
                      enforce_active_worker_authorization,
                      enforce_campaign_runtime_guards,
                      install_worker_start_capability,
                      clear_worker_start_capability,
                      read_campaign_stop_conditions,
                      record_campaign_stop_condition,
                      record_emergency_campaign_stop_condition,
                      record_sample_outcome, normalize_snapshot_mode,
                      SNAPSHOT_MODE_ALL, SNAPSHOT_MODE_FAILURES,
                      SNAPSHOT_MODE_OFF, warn_best_effort_io,
                      PROVIDER_GUARD_ACTIVE_SET_V1,
                      PROVIDER_GUARD_WORKER_START_CAPABILITY_V1,
                      WORKER_START_CAPABILITY_SCHEMA)
from hybrid_prompt import (build_hybrid_prompt, build_hybrid_repair_prompt,
                           classify_operation_family, extract_hybrid_json)
from hybrid_executor import apply_hybrid
from hybrid_gate import (validate_hybrid_output, audit_forward_completion,
                         partial_acceptance_eligible)
from hybrid_schema import (PROTOCOL_V8, measure_protocol_burden,
                           protocol_burden_overages,
                           validate_hybrid_envelope)

MODEL_DEFAULT = "deepseek-v4-flash"
LLM_MAX_RETRIES = 3
SAMPLES_ROOT = os.path.join(_ROOT, "data", "samples_delegate52")
RESULTS_DIR = os.path.join(_HERE, "experiment_results")
DEFAULT_SAMPLES = ["accounting1", "accounting2", "accounting3", "accounting4",
                   "accounting5", "accounting6", "calendar1", "calendar5"]


def _record_dispatcher_parent_loss(out_dir, dispatcher_pid, instance_id):
    """Write lock-independent stop evidence before an orphan exits."""
    try:
        record_emergency_campaign_stop_condition(
            out_dir,
            "dispatcher_process_lost",
            dispatcher_pid=dispatcher_pid,
            dispatcher_instance_id=instance_id,
        )
    except BaseException:
        # Parent loss must still terminate the paid worker if the filesystem
        # itself can no longer accept durable evidence.
        pass


def _start_dispatcher_parent_watchdog(out_dir):
    """Stop a paid worker if its owning dispatcher process disappears."""
    raw_pid = os.environ.get("ANCHORPATCH_DISPATCHER_PID")
    instance_id = os.environ.get("ANCHORPATCH_DISPATCHER_INSTANCE_ID")
    if raw_pid is None and instance_id is None:
        return None
    try:
        dispatcher_pid = int(raw_pid or "")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("dispatcher parent watchdog identity is invalid") from exc
    if dispatcher_pid <= 0 or not instance_id:
        raise RuntimeError("dispatcher parent watchdog identity is invalid")

    if os.name == "nt":
        import ctypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.restype = ctypes.c_void_p
        handle = kernel32.OpenProcess(0x00100000, False, dispatcher_pid)
        if not handle:
            _record_dispatcher_parent_loss(
                out_dir, dispatcher_pid, instance_id)
            raise RuntimeError("dispatcher process exited before worker start")

        def _watch():
            try:
                result = kernel32.WaitForSingleObject(
                    ctypes.c_void_p(handle), 0xFFFFFFFF)
                if result in (0, 0xFFFFFFFF):
                    _record_dispatcher_parent_loss(
                        out_dir, dispatcher_pid, instance_id)
                    os._exit(97)
            finally:
                kernel32.CloseHandle(ctypes.c_void_p(handle))
    else:
        try:
            os.kill(dispatcher_pid, 0)
        except OSError as exc:
            _record_dispatcher_parent_loss(
                out_dir, dispatcher_pid, instance_id)
            raise RuntimeError(
                "dispatcher process exited before worker start") from exc

        def _watch():
            while True:
                if os.getppid() != dispatcher_pid:
                    _record_dispatcher_parent_loss(
                        out_dir, dispatcher_pid, instance_id)
                    os._exit(97)
                try:
                    os.kill(dispatcher_pid, 0)
                except OSError:
                    _record_dispatcher_parent_loss(
                        out_dir, dispatcher_pid, instance_id)
                    os._exit(97)
                time.sleep(1)

    thread = threading.Thread(
        target=_watch,
        name=f"dispatcher-watchdog-{dispatcher_pid}",
        daemon=True,
    )
    thread.start()
    return thread


class EvaluatorIncompleteError(RuntimeError):
    """A sample-local domain evaluator exception after model generation."""

    _anchorpatch_failure_class = "evaluator_incomplete"

    def __init__(self, sample_id, method, rt_index, direction,
                 target_state_id, cause):
        error_type = (
            f"{type(cause).__module__}.{type(cause).__qualname__}"
        )
        error_message = str(cause) or repr(cause)
        super().__init__(
            "domain evaluator failed for "
            f"{method}/{sample_id}/RT{rt_index}/{direction}: "
            f"{error_type}: {error_message}"
        )
        self.sample_id = sample_id
        self.method = method
        self.rt_index = rt_index
        self.direction = direction
        self.target_state_id = target_state_id
        self.error_type = error_type
        self.error_message = error_message


def _dispatch_worker_start_barrier(out_dir, samples, num_round_trips,
                                   invocation_id):
    """Register the task plan and wait for dispatcher authorization pre-API."""
    worker_id = os.environ.get("ANCHORPATCH_WORKER_LAUNCH_ID")
    ready_path = os.environ.get("ANCHORPATCH_WORKER_READY_PATH")
    ack_path = os.environ.get("ANCHORPATCH_WORKER_ACK_PATH")
    configured = [worker_id, ready_path, ack_path]
    if not any(configured):
        return None
    if not all(configured) or len(samples) != 1:
        raise RuntimeError(
            "formal dispatcher barrier requires one sample and complete worker identity"
        )
    sample_id = samples[0]
    plan_path = os.path.abspath(
        os.path.join(out_dir, f"{sample_id}.task_plan.json"))
    entry = register_task_plan(
        out_dir, sample_id, plan_path,
        num_round_trips=num_round_trips,
    )
    ready = {
        "schema": "anchorpatch.worker_ready/1",
        "worker_launch_id": worker_id,
        "worker_pid": os.getpid(),
        "invocation_id": invocation_id,
        "sample": sample_id,
        "task_plan_path": plan_path,
        "task_plan_sha256": entry["sha256"],
    }
    write_json_atomic(os.path.abspath(ready_path), ready)
    timeout = float(os.environ.get(
        "ANCHORPATCH_START_BARRIER_TIMEOUT", "300"))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stop_records = read_campaign_stop_conditions(out_dir)
        if stop_records:
            conditions = sorted({
                str(row.get("condition")) for row in stop_records
            })
            raise CampaignStoppedError(
                "campaign stop latch is set: " + ", ".join(conditions))
        if os.path.isfile(ack_path):
            with open(ack_path, encoding="utf-8") as handle:
                ack = json.load(handle)
            guard_mode = os.environ.get(
                "ANCHORPATCH_PROVIDER_GUARD_MODE",
                PROVIDER_GUARD_ACTIVE_SET_V1,
            )
            if guard_mode == PROVIDER_GUARD_ACTIVE_SET_V1:
                expected = {
                    "schema": "anchorpatch.worker_start/1",
                    "worker_launch_id": worker_id,
                    "worker_pid": os.getpid(),
                    "invocation_id": invocation_id,
                    "sample": sample_id,
                    "task_plan_sha256": entry["sha256"],
                }
                if ack != expected:
                    raise RuntimeError(
                        "dispatcher worker authorization mismatch")
            elif guard_mode == PROVIDER_GUARD_WORKER_START_CAPABILITY_V1:
                if (not isinstance(ack, dict)
                        or ack.get("schema")
                        != WORKER_START_CAPABILITY_SCHEMA):
                    raise RuntimeError(
                        "dispatcher worker capability schema mismatch")
                install_worker_start_capability(
                    out_dir, sample_id, invocation_id, entry["sha256"], ack)
            else:
                raise RuntimeError("unknown provider guard mode")
            return ready
        time.sleep(0.1)
    raise RuntimeError("timed out waiting for dispatcher worker authorization")


def _record_preservation_stop(out_dir, method, sample_id, rt_num,
                              direction, row):
    bdpatch = row.get("bdpatch") if isinstance(row, dict) else None
    count = (bdpatch or {}).get("preservation_violations")
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        return False
    record_campaign_stop_condition(
        out_dir,
        "preservation_violation",
        method=method,
        sample=sample_id,
        round_trip_num=rt_num,
        direction=direction,
        preservation_violations=count,
        result_committed=False,
        api_call_ids=list(row.get("api_call_ids") or []),
    )
    return True


def _record_preservation_stop_from_step(
        out_dir, method, sample_id, rt_num, direction, exec_log, meta):
    """Latch a violation before evaluator code can obscure it."""
    count = getattr(exec_log, "preservation_violations", None)
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        return False
    call_ids = []
    if isinstance(meta, dict):
        call_ids = list(meta.get("api_call_ids") or [])
        if not call_ids and meta.get("api_call_id"):
            call_ids = [meta["api_call_id"]]
    record_campaign_stop_condition(
        out_dir,
        "preservation_violation",
        method=method,
        sample=sample_id,
        round_trip_num=rt_num,
        direction=direction,
        preservation_violations=count,
        result_committed=False,
        api_call_ids=call_ids,
    )
    return True


def _score_collapsed(evaluation):
    score = (evaluation or {}).get("score") if isinstance(evaluation, dict) else None
    return (
        isinstance(score, (int, float))
        and not isinstance(score, bool)
        and score <= 1e-9
    )


def _step_has_snapshot_failure(row=None, evaluation=None, exec_log=None,
                               v2_info=None, evaluator_exception=None):
    if evaluator_exception is not None:
        return True
    if isinstance(evaluation, dict):
        if evaluation.get("error"):
            return True
        if _score_collapsed(evaluation):
            return True
    if exec_log is not None:
        count = getattr(exec_log, "preservation_violations", None)
        if isinstance(count, int) and not isinstance(count, bool) and count > 0:
            return True
    if isinstance(row, dict):
        bdpatch = row.get("bdpatch") if isinstance(row.get("bdpatch"), dict) else {}
        count = bdpatch.get("preservation_violations")
        if isinstance(count, int) and not isinstance(count, bool) and count > 0:
            return True
        telemetry = bdpatch.get("hybrid")
        if isinstance(telemetry, dict):
            v2_info = telemetry
        elif isinstance(bdpatch.get("v2"), dict):
            v2_info = bdpatch["v2"]
    if isinstance(v2_info, dict):
        if v2_info.get("invalid_json"):
            return True
        if v2_info.get("partial_extraction") or v2_info.get("failure_reason"):
            return True
        if v2_info.get("schema_error_count"):
            return True
        if v2_info.get("validation_gate_errors"):
            return True
        if v2_info.get("failed_step_kept_context"):
            return True
    return False


def _maybe_dump_step_docs(snapshot_mode, out_dir, method, sample_id, rt_num,
                          direction, state_id, gen_docs, *, step_info=None,
                          row=None, evaluation=None, exec_log=None,
                          v2_info=None, evaluator_exception=None):
    mode = normalize_snapshot_mode(snapshot_mode, default=SNAPSHOT_MODE_ALL)
    if mode == SNAPSHOT_MODE_OFF:
        return None
    if (mode == SNAPSHOT_MODE_FAILURES
            and not _step_has_snapshot_failure(
                row=row, evaluation=evaluation, exec_log=exec_log,
                v2_info=v2_info, evaluator_exception=evaluator_exception)):
        return None
    try:
        return dump_step_docs(
            out_dir, method, sample_id, rt_num, direction, state_id, gen_docs,
            step_info=step_info,
        )
    except Exception as exc:
        try:
            warn_best_effort_io("snapshot", out_dir, exc)
        except Exception:
            pass
        return None


def _enforce_post_call_campaign_guard(out_dir, sample_id):
    """Prevent an in-flight response from committing after a global stop.

    The provider call has its own pre-call guard, but a sibling can set the
    durable latch while that call is in flight.  Recheck both the latch and
    active-worker authorization before local evaluation; the final relay
    commit repeats the latch check under the campaign metadata lock.
    """
    enforce_campaign_runtime_guards(out_dir, sample_id)


def _require_formal_dispatch_environment(out_dir, samples, model=None):
    """Prevent an unleased standalone runner from joining a paired campaign."""
    manifest_path = os.path.join(
        os.path.abspath(out_dir), "dispatch_manifest.json")
    if not os.path.isfile(manifest_path):
        if model is not None:
            from model_openai import resolve_model_name
            if (
                    resolve_model_name(str(model)).lower().startswith(
                        "deepseek-v4-")
                    and len(samples) > 1):
                raise RuntimeError(
                    "multi-sample DeepSeek-V4 experiments require the paired "
                    "dispatcher manifest"
                )
        return
    required = [
        "ANCHORPATCH_WORKER_LAUNCH_ID",
        "ANCHORPATCH_WORKER_LOCK_PATH",
        "ANCHORPATCH_WORKER_READY_PATH",
        "ANCHORPATCH_WORKER_ACK_PATH",
        "ANCHORPATCH_ACTIVE_WORKER_SET_PATH",
        "ANCHORPATCH_EXPECTED_GIT_COMMIT",
        "ANCHORPATCH_EXPECTED_GIT_TREE_STATE",
        "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256",
        "ANCHORPATCH_EXPECTED_TASK_PLAN_PATH",
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if len(samples) != 1 or missing:
        raise RuntimeError(
            "formal paired campaign requires one leased dispatcher worker; "
            f"missing={missing}"
        )
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    config = manifest.get("config") if isinstance(manifest, dict) else None
    if not isinstance(config, dict):
        raise RuntimeError("formal paired campaign manifest config is invalid")
    guard_mode = config.get(
        "provider_guard_mode", PROVIDER_GUARD_ACTIVE_SET_V1)
    environment_mode = os.environ.get(
        "ANCHORPATCH_PROVIDER_GUARD_MODE", PROVIDER_GUARD_ACTIVE_SET_V1)
    if guard_mode != environment_mode:
        raise RuntimeError("provider guard mode differs from dispatcher manifest")
    if guard_mode == PROVIDER_GUARD_ACTIVE_SET_V1:
        enforce_active_worker_authorization(out_dir, samples[0])
    elif guard_mode == PROVIDER_GUARD_WORKER_START_CAPABILITY_V1:
        if (config.get("campaign_role") != "deepseek_full234"
                or config.get("num_round_trips") != 10
                or config.get("transport_revision")
                != "opencode_openai_compatible/6"
                or config.get("run_metadata_storage") != "event_v1"):
            raise RuntimeError(
                "worker start capability is not valid for this campaign")
        # The capability does not exist yet.  It is installed only after the
        # dispatcher has durably published worker_authorized and released the
        # start barrier.
    else:
        raise RuntimeError("unknown provider guard mode")


def _read_committed_rounds(jsonl_path):
    by_rt = {}
    duplicates = 0
    if not os.path.exists(jsonl_path):
        return by_rt, 0, duplicates, []
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            rt = row.get("round_trip_num")
            direction = row.get("round_trip_direction")
            if not isinstance(rt, int) or direction not in ("forward", "backward"):
                continue
            slot = by_rt.setdefault(rt, {})
            if direction in slot:
                duplicates += 1
            slot[direction] = row
    complete = 0
    while {"forward", "backward"} <= set(by_rt.get(complete + 1, {})):
        complete += 1
    partial = sorted(rt for rt, rows in by_rt.items()
                     if rt > complete and rows and {"forward", "backward"} > set(rows))
    return by_rt, complete, duplicates, partial


def _tuple_tree(value):
    if isinstance(value, list):
        return tuple(_tuple_tree(item) for item in value)
    return value


def _reconcile_checkpoint_from_jsonl(jsonl_path, ckpt_path, start_rt, current_context,
                                     rid_chain, state_chain, id2state, initial_state,
                                     distractor, include_distractor, log):
    """Fast-forward a stale checkpoint from already committed raw rows.

    This is an offline replay only. It calls no provider and leaves raw JSONL
    untouched. It exists for the crash window where rows were appended but the
    checkpoint was not advanced.
    """
    by_rt, complete, duplicates, partial = _read_committed_rounds(jsonl_path)
    if partial:
        raise RuntimeError(
            f"JSONL has partial committed round(s) after RT{complete}: {partial}; "
            "manual raw-row audit is required before resume")
    if complete < start_rt:
        raise RuntimeError(
            f"checkpoint is ahead of committed JSONL rows: ckpt RT{start_rt}, jsonl RT{complete}")
    if complete == start_rt:
        if duplicates:
            log.line(f"[resume] JSONL duplicate row count observed before RT{start_rt}: {duplicates}; raw unchanged")
        return start_rt, current_context, rid_chain, state_chain

    from verify_anchorpatch import _step as replay_step

    readonly_names = list(distractor) if include_distractor else []
    log.line(f"[resume] checkpoint RT{start_rt} behind JSONL RT{complete}; "
             f"replaying committed rows without API calls")
    if duplicates:
        log.line(f"[resume] duplicate row count in JSONL={duplicates}; using latest row per RT/direction for replay")

    for rt in range(start_rt + 1, complete + 1):
        rows = by_rt[rt]
        fwd = rows["forward"]
        bwd = rows["backward"]
        fwd_state = id2state[fwd["target_state_id"]]
        gen = replay_step(fwd, _editable(current_context, distractor),
                          list(fwd_state["context"]), readonly_names)
        current_context = shuffle_context(merge_distractor(gen, distractor))
        gen = replay_step(bwd, _editable(current_context, distractor),
                          list(initial_state["context"]), readonly_names)
        current_context = shuffle_context(merge_distractor(gen, distractor))
        rid_chain = list(bwd.get("rid_chain") or rid_chain)
        state_chain = list(bwd.get("state_chain") or state_chain)

    ckpt = {"completed_round_trips": complete, "current_context": current_context,
            "rid_chain": rid_chain, "state_chain": state_chain,
            "context_shuffle_random_state": random.getstate()}
    write_json_atomic(ckpt_path, ckpt)
    return complete, current_context, rid_chain, state_chain


def _real_generate(*a, **k):
    from model_openai import generate as g
    return g(*a, **k)


# The active scientific implementation is shared with the simplified runtime.
# Keep this legacy wrapper only for archived dispatcher compatibility and its
# existing metadata/stop/snapshot side effects.
from relay_core import (
    EvaluatorIncompleteError as _SharedEvaluatorIncompleteError,
    RelayHooks as _SharedRelayHooks,
    _attempt_hybrid_repair as _shared_attempt_hybrid_repair,
    _edit_step as _shared_edit_step,
    _editable as _shared_editable,
    _evaluate as _shared_evaluate,
    _has_hybrid_protocol_signal as _shared_has_hybrid_protocol_signal,
    _hybrid_key as _shared_hybrid_key,
    _merge_meta as _shared_merge_meta,
    _readonly as _shared_readonly,
    _row as _shared_row,
    _run_attempt_hybrid as _shared_run_attempt_hybrid,
    require_formal_opencode_transport as _shared_require_transport,
    run_method as _shared_run_method,
)


def _run_relay_via_shared_core(
        method, sample_id, num_round_trips=10, seed=42,
        include_distractor=True, out_dir=RESULTS_DIR, model=MODEL_DEFAULT,
        max_tokens=None, generate_fn=None, printing=True, inline_report=False,
        fr_baseline=None, stop_on_collapse=False,
        stop_on_preservation_violation=False, reasoning_effort=None,
        snapshot_mode=SNAPSHOT_MODE_ALL):
    del inline_report, fr_baseline
    _require_formal_opencode_transport(model, reasoning_effort)
    snapshot_mode = normalize_snapshot_mode(
        snapshot_mode, default=SNAPSHOT_MODE_ALL)
    if max_tokens is None and not str(model).lower().startswith("minimax-m3"):
        max_tokens = 20000
    base_generate = generate_fn or _real_generate
    recorder = None
    if generate_fn is None:
        recorder = ApiCallRecorder(
            out_dir, method, sample_id, None, model, base_generate)
        generate_fn = recorder.generate
    else:
        generate_fn = base_generate

    sample, sample_folder, states = load_sample(
        sample_id, samples_folder=os.path.join(SAMPLES_ROOT, ""))
    initial_id = sample["start_state"]
    initial = states[initial_id]
    possible = [item["target_state"] for item in initial["prompts"]]
    plan_path = os.path.join(out_dir, f"{sample_id}.task_plan.json")
    if os.path.exists(plan_path):
        task_plan = load_relay_task_plan(plan_path)
    else:
        os.makedirs(out_dir, exist_ok=True)
        task_plan = build_relay_task_plan(
            possible, num_round_trips, seed=seed)
        save_relay_task_plan(plan_path, task_plan)
    task_plan = task_plan[:num_round_trips]
    registered = register_task_plan(
        out_dir, sample_id, plan_path, num_round_trips=num_round_trips)
    expected_sha = os.environ.get("ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256")
    if expected_sha and registered.get("sha256") != expected_sha:
        raise RuntimeError(
            f"task-plan hash differs from dispatch manifest for {sample_id}")

    method_dir = os.path.join(out_dir, method)
    os.makedirs(method_dir, exist_ok=True)
    jsonl_path = os.path.join(method_dir, f"{sample_id}.jsonl")
    ckpt_path = os.path.join(method_dir, f"{sample_id}.ckpt.json")
    log = RunLogger(
        out_dir, method, sample_id, to_console=printing,
        header=f"{method}/{sample_id} model={model} seed={seed} "
               f"RT={num_round_trips} distractor={include_distractor}")
    resume_state = None
    try:
        if os.path.exists(ckpt_path):
            with open(ckpt_path, encoding="utf-8") as handle:
                resume_state = json.load(handle)
            if resume_state.get("stopped_early"):
                log.line(
                    f"[{method}/{sample_id}] already stopped early at "
                    f"RT{resume_state['completed_round_trips']}")
                return jsonl_path
        else:
            context = build_context_from_folder(
                os.path.join(sample_folder, initial["solution_folder"]))
            distractor = (
                load_distractor_context(sample_folder)
                if include_distractor else {})
            if include_distractor:
                context = merge_distractor(context, distractor)
            random.seed(seed)
            context = shuffle_context(context)
            resume_state = {
                "completed_round_trips": 0, "current_context": context,
                "rid_chain": [], "state_chain": [],
                "context_shuffle_random_state": random.getstate(),
            }
        distractor = (
            load_distractor_context(sample_folder) if include_distractor else {})
        start_rt = int(resume_state["completed_round_trips"])
        start_rt, context, rid_chain, state_chain = _reconcile_checkpoint_from_jsonl(
            jsonl_path, ckpt_path, start_rt,
            resume_state["current_context"], list(resume_state["rid_chain"]),
            list(resume_state["state_chain"]), states, initial, distractor,
            include_distractor, log)
        resume_state.update(
            completed_round_trips=start_rt, current_context=context,
            rid_chain=rid_chain, state_chain=state_chain)
        if os.path.exists(ckpt_path):
            with open(ckpt_path, encoding="utf-8") as handle:
                disk_checkpoint = json.load(handle)
            resume_state["context_shuffle_random_state"] = disk_checkpoint.get(
                "context_shuffle_random_state",
                resume_state.get("context_shuffle_random_state"))

        def on_step(event):
            direction = event["direction"]
            short = "fwd" if direction == "forward" else "bwd"
            if event.get("error") == "preservation_violation":
                _record_preservation_stop_from_step(
                    out_dir, method, sample_id, event["rt_num"], direction,
                    event.get("exec_log"), event.get("meta"))
            row = event.get("row")
            if row is not None and recorder:
                record_model_content_anomaly(out_dir, row)
            _maybe_dump_step_docs(
                snapshot_mode, out_dir, method, sample_id,
                event["rt_num"], short, event["target_state_id"],
                event.get("generated") or {}, row=row,
                evaluation=event.get("evaluation"),
                exec_log=event.get("exec_log"),
                v2_info=event.get("telemetry"),
                evaluator_exception=event.get("exception"))

        hooks = _SharedRelayHooks(
            commit_round_trip=lambda rows, checkpoint:
                append_relay_rows_and_checkpoint(
                    jsonl_path, ckpt_path, rows, checkpoint,
                    campaign_out_dir=out_dir),
            set_step=(recorder.set_step if recorder else None),
            after_generate=lambda *_args: _enforce_post_call_campaign_guard(
                out_dir, sample_id),
            before_commit=lambda _rt: enforce_campaign_runtime_guards(
                out_dir, sample_id),
            on_step=on_step,
            log=lambda event: log.line(
                f"[{sample_id}] RT{event['round_trip']} "
                f"score={event.get('backward_score')}")
        )
        _shared_run_method(
            method, sample_id, task_plan,
            num_round_trips=num_round_trips, seed=seed,
            include_distractor=include_distractor, model=model,
            max_tokens=max_tokens, generate_fn=generate_fn, hooks=hooks,
            resume_state=resume_state, reasoning_effort=reasoning_effort,
            stop_on_collapse=stop_on_collapse,
            stop_on_preservation_violation=stop_on_preservation_violation,
            samples_root=SAMPLES_ROOT, validate_transport=False,
            load_sample_fn=load_sample, get_domain_fn=get_domain,
            load_distractor_fn=load_distractor_context,
            build_context_fn=build_context_from_folder,
            edit_step_fn=_edit_step, evaluate_fn=_evaluate, row_fn=_row,
            response_id_fn=generate_response_id,
            merge_fn=merge_distractor, shuffle_fn=shuffle_context)
        return jsonl_path
    finally:
        log.close()


# Public helper compatibility now points at the one shared implementation.
EvaluatorIncompleteError = _SharedEvaluatorIncompleteError
_run_attempt_hybrid = _shared_run_attempt_hybrid
_attempt_hybrid_repair = _shared_attempt_hybrid_repair
_has_hybrid_protocol_signal = _shared_has_hybrid_protocol_signal
_hybrid_key = _shared_hybrid_key
_merge_meta = _shared_merge_meta
_edit_step = _shared_edit_step
_evaluate = _shared_evaluate
_row = _shared_row
_editable = _shared_editable
_readonly = _shared_readonly
_require_formal_opencode_transport = _shared_require_transport
run_relay = _run_relay_via_shared_core


def _sample_checkpoint_progress(out_dir, sample_id, methods):
    progress = {}
    for method in methods:
        ckpt_path = os.path.join(out_dir, method, f"{sample_id}.ckpt.json")
        result_path = os.path.join(out_dir, method, f"{sample_id}.jsonl")
        completed = 0
        if os.path.isfile(ckpt_path):
            with open(ckpt_path, encoding="utf-8") as handle:
                checkpoint = json.load(handle)
            value = checkpoint.get("completed_round_trips")
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                completed = value
        committed_rows = 0
        if os.path.isfile(result_path):
            with open(result_path, encoding="utf-8") as handle:
                committed_rows = sum(1 for line in handle if line.strip())
        progress[method] = {
            "completed_round_trips": completed,
            "committed_rows": committed_rows,
        }
    return progress


def _record_evaluator_incomplete(out_dir, sample_id, methods,
                                 num_round_trips, invocation_id, exc):
    """Persist a fail-closed, sample-local evaluator failure record."""
    progress = _sample_checkpoint_progress(out_dir, sample_id, methods)
    failure_index = methods.index(exc.method)
    expected = {}
    for index, method in enumerate(methods):
        completed = (
            num_round_trips if index < failure_index
            else exc.rt_index - 1 if index == failure_index
            else 0
        )
        expected[method] = {
            "completed_round_trips": completed,
            "committed_rows": 2 * completed,
        }
    if progress != expected:
        raise RuntimeError(
            "evaluator failure cannot be isolated because committed progress "
            f"is not the expected atomic prefix: actual={progress!r} "
            f"expected={expected!r}"
        ) from exc
    details = {
        "invocation_id": invocation_id,
        "methods": list(methods),
        "method": exc.method,
        "rt_index": exc.rt_index,
        "direction": exc.direction,
        "target_state": exc.target_state_id,
        "failure_stage": "evaluator",
        "error_type": exc.error_type,
        "error_message": exc.error_message,
        "checkpoint_progress": progress,
        "result_committed_for_failed_step": False,
        "score_imputed": False,
        "evidence": {
            "api_calls": "api_calls.jsonl",
            "attempt_ledger": "api_attempt_ledger.jsonl",
            "run_metadata": "run_metadata.jsonl",
            "run_metadata_events": "run_metadata_events.jsonl",
            "run_metadata_projection_receipt": (
                "run_metadata_projection_receipt.json"),
            "run_metadata_event_pending": "run_metadata_event_pending.json",
            "run_metadata_event_recoveries": "run_metadata_event_recoveries/",
            "sample_outcomes": "sample_outcomes.jsonl",
        },
    }
    method_phase = os.environ.get("ANCHORPATCH_METHOD_PHASE")
    if method_phase:
        details["method_phase"] = method_phase
    outcome = record_sample_outcome(
        out_dir, sample_id, "evaluator_incomplete", **details)
    sidecar = {
        "schema": "anchorpatch.evaluator_incomplete/2",
        "created_at": outcome["created_at"],
        "sample": sample_id,
        "status": "evaluator_incomplete",
        "disposition": "cancel_sample_continue_campaign",
        "worker_launch_id": outcome.get("worker_launch_id"),
        "worker_pid": outcome.get("worker_pid"),
        **details,
    }
    source_log = os.environ.get("ANCHORPATCH_WORKER_CONSOLE_LOG")
    if source_log:
        sidecar["source_log"] = source_log
    append_jsonl_locked(
        os.path.join(out_dir, "evaluator_incomplete_samples.jsonl"),
        sidecar,
    )
    return sidecar


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", nargs="+", default=DEFAULT_SAMPLES)
    ap.add_argument("--methods", nargs="+", default=["hybridpatch", "fullrewrite"])
    ap.add_argument("--num_round_trips", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--skip_distractor", action="store_true")
    ap.add_argument("--out_dir", default=RESULTS_DIR)
    ap.add_argument("--model", default=MODEL_DEFAULT)
    ap.add_argument(
        "--reasoning_effort",
        choices=("low", "medium", "high"),
        default=None,
        help="OpenAI-compatible reasoning effort; formal OpenCode DeepSeek-V4 "
             "campaigns require high.",
    )
    ap.add_argument("--max_tokens", type=int, default=None,
                    help="MiniMax default/ceiling is 131072; 0 selects that default. "
                         "Other models default to 20000.")
    ap.add_argument("--inline_report", action="store_true",
                    help="print RS + FR-baseline comparison + failure diagnosis after each round trip")
    ap.add_argument("--fr_baseline", default=None,
                    help="path to a JSON {sample_id: {k: fr_rs}} baseline for inline Δ vs FullRewrite")
    ap.add_argument("--notes", default="",
                    help="free-text label for this run, recorded in run_metadata.jsonl")
    ap.add_argument("--stop_on_collapse", action="store_true",
                    help="early-stop a sample's relay once a backward RS collapses to 0 "
                         "(cliff-drop) — the remaining round trips are skipped to save compute. "
                         "Off by default to preserve the full-10RT paired-comparison semantics.")
    ap.add_argument(
        "--stop_on_preservation_violation", action="store_true",
        help="fail before the next API call if any generated HP row reports a "
             "preservation violation",
    )
    ap.add_argument(
        "--snapshot_mode",
        choices=(SNAPSHOT_MODE_ALL, SNAPSHOT_MODE_FAILURES, SNAPSHOT_MODE_OFF),
        default=SNAPSHOT_MODE_ALL,
        help="write per-step document snapshots for all steps, deterministic "
             "failures only, or never",
    )
    args = ap.parse_args()
    _start_dispatcher_parent_watchdog(args.out_dir)
    method_phase = os.environ.get("ANCHORPATCH_METHOD_PHASE")
    if method_phase and args.methods != [method_phase]:
        raise RuntimeError(
            "phased worker must run exactly its declared method phase"
        )
    outcome_phase = (
        {"method_phase": method_phase} if method_phase else {}
    )
    if args.max_tokens == 0:
        args.max_tokens = None  # MiniMax model layer substitutes 131072.
    elif args.max_tokens is None and not str(args.model).lower().startswith("minimax-m3"):
        args.max_tokens = 20000
    _require_formal_opencode_transport(args.model, args.reasoning_effort)
    _require_formal_dispatch_environment(
        args.out_dir, args.sample, args.model)

    fr_baseline = None
    if args.fr_baseline and os.path.exists(args.fr_baseline):
        fr_baseline = json.load(open(args.fr_baseline, encoding="utf-8"))

    run_metadata = append_run_metadata(
        args.out_dir, command="python " + " ".join(sys.argv),
        samples=args.sample, methods=args.methods, num_round_trips=args.num_round_trips,
        seed=args.seed, model=args.model, distractor=not args.skip_distractor,
        max_tokens=args.max_tokens, notes=args.notes,
        reasoning_effort=args.reasoning_effort,
        context_shuffle_seeded=True,
        context_shuffle_seed_version="global_random_seed_v1",
        stop_on_collapse=args.stop_on_collapse,
        stop_on_preservation_violation=args.stop_on_preservation_violation,
        snapshot_mode=args.snapshot_mode)

    finish_status = "failed"
    try:
        _dispatch_worker_start_barrier(
            args.out_dir, args.sample, args.num_round_trips,
            run_metadata["invocation_id"],
        )
        for sample_id in args.sample:
            for method in args.methods:
                run_relay(
                    method, sample_id,
                    num_round_trips=args.num_round_trips, seed=args.seed,
                    include_distractor=not args.skip_distractor,
                    out_dir=args.out_dir, model=args.model,
                    max_tokens=args.max_tokens, inline_report=args.inline_report,
                    fr_baseline=fr_baseline,
                    stop_on_collapse=args.stop_on_collapse,
                    stop_on_preservation_violation=(
                        args.stop_on_preservation_violation
                    ),
                    reasoning_effort=args.reasoning_effort,
                    snapshot_mode=args.snapshot_mode,
                )
        finish_status = "finished"
        if len(args.sample) == 1:
            record_sample_outcome(
                args.out_dir, args.sample[0], "finished",
                invocation_id=run_metadata["invocation_id"],
                methods=list(args.methods),
                checkpoint_progress=_sample_checkpoint_progress(
                    args.out_dir, args.sample[0], args.methods),
                **outcome_phase,
            )
    except Exception as exc:
        failure_class = getattr(exc, "_anchorpatch_failure_class", None)
        stop_conditions = (
            read_campaign_stop_conditions(args.out_dir)
            if isinstance(exc, CampaignStoppedError) else []
        )
        dispatcher_interruption = (
            isinstance(exc, CampaignStoppedError)
            and stop_conditions
            and all(
                row.get("condition") in {
                    "operator_directed_dispatcher_pause",
                    "dispatcher_process_lost",
                }
                for row in stop_conditions
            )
        )
        if dispatcher_interruption:
            # The dispatcher retains the active-set witness.  Publishing this
            # exact terminal metadata status lets the offline, hash-bound
            # dispatcher recovery transaction close the worker without
            # converting a cooperative stop into an unrelated fatal error.
            finish_status = "interrupted_by_dispatcher"
        elif (failure_class == "evaluator_incomplete"
                and len(args.sample) == 1
                and isinstance(exc, EvaluatorIncompleteError)):
            finish_status = "evaluator_incomplete"
            _record_evaluator_incomplete(
                args.out_dir, args.sample[0], list(args.methods),
                args.num_round_trips, run_metadata["invocation_id"], exc,
            )
        elif (failure_class == "infrastructure_incomplete"
                and len(args.sample) == 1):
            finish_status = "infrastructure_incomplete"
            api_record = getattr(exc, "_anchorpatch_api_record", None) or {}
            record_sample_outcome(
                args.out_dir, args.sample[0], "infrastructure_incomplete",
                invocation_id=run_metadata["invocation_id"],
                methods=list(args.methods),
                method=api_record.get("method"),
                rt_index=api_record.get("rt_index"),
                direction=api_record.get("direction"),
                call_kind=api_record.get("call_kind"),
                semantic_root_id=api_record.get("semantic_root_id"),
                semantic_call_id=api_record.get("semantic_call_id"),
                generation_index=api_record.get("generation_index"),
                parent_semantic_call_id=api_record.get(
                    "parent_semantic_call_id"),
                request_fingerprint=api_record.get("request_fingerprint"),
                request_id=api_record.get("request_id"),
                error_type=api_record.get("error_type"),
                classification=api_record.get("classification"),
                response_slots_used=api_record.get("response_slots_used"),
                transient_failure_count=api_record.get(
                    "transient_failure_count"),
                http_attempts_used=api_record.get("http_attempts_used"),
                next_attempt_index=(
                    api_record.get("http_attempts_used") + 1
                    if isinstance(api_record.get("http_attempts_used"), int)
                    else None
                ),
                transport_recovery_index=api_record.get(
                    "transport_recovery_index"),
                checkpoint_progress=_sample_checkpoint_progress(
                    args.out_dir, args.sample[0], args.methods),
                evidence={
                    "api_calls": "api_calls.jsonl",
                    "attempt_ledger": "api_attempt_ledger.jsonl",
                    "run_metadata": "run_metadata.jsonl",
                    "run_metadata_events": "run_metadata_events.jsonl",
                    "run_metadata_projection_receipt": (
                        "run_metadata_projection_receipt.json"),
                    "run_metadata_event_pending": (
                        "run_metadata_event_pending.json"),
                    "run_metadata_event_recoveries": (
                        "run_metadata_event_recoveries/"),
                },
                **outcome_phase,
            )
        else:
            # Unknown worker failures are campaign-wide by default.  Latch
            # immediately so sibling pre-call guards stop before the
            # dispatcher reaches its next polling cycle.
            try:
                record_campaign_stop_condition(
                    args.out_dir,
                    "worker_fatal_error",
                    sample=(args.sample[0] if len(args.sample) == 1 else None),
                    methods=list(args.methods),
                    error_type=type(exc).__name__,
                    error=str(exc),
                    invocation_id=run_metadata["invocation_id"],
                )
            except BaseException:
                pass
        raise
    finally:
        try:
            finish_run_metadata(
                args.out_dir, run_metadata["invocation_id"],
                status=finish_status)
        finally:
            clear_worker_start_capability()


def _run_cli_with_worker_lease():
    """Hold the dispatcher's per-sample lease for this process lifetime."""
    lock_path = os.environ.get("ANCHORPATCH_WORKER_LOCK_PATH")
    if not lock_path:
        try:
            out_index = sys.argv.index("--out_dir") + 1
            standalone_out_dir = os.path.abspath(sys.argv[out_index])
        except (ValueError, IndexError) as exc:
            raise RuntimeError(
                "standalone runner requires explicit --out_dir"
            ) from exc
        os.makedirs(standalone_out_dir, exist_ok=True)
        dispatcher_lock_path = os.path.join(
            standalone_out_dir, ".paired_dispatch.lock")
        with open(dispatcher_lock_path, "a+", encoding="utf-8") as lease:
            try:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.LockException as exc:
                raise RuntimeError(
                    "paired dispatcher already owns this out_dir"
                ) from exc
            try:
                return main()
            finally:
                portalocker.unlock(lease)
    os.makedirs(os.path.dirname(os.path.abspath(lock_path)), exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lease:
        try:
            portalocker.lock(
                lease, portalocker.LOCK_EX | portalocker.LOCK_NB
            )
        except portalocker.exceptions.LockException as exc:
            raise RuntimeError(
                f"another runner owns worker lease {lock_path}"
            ) from exc
        try:
            return main()
        finally:
            portalocker.unlock(lease)


if __name__ == "__main__":
    _run_cli_with_worker_lease()
