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
                      ApiCallRecorder, record_model_content_anomaly,
                      append_relay_rows_and_checkpoint, write_json_atomic,
                      enforce_active_worker_authorization,
                      enforce_campaign_runtime_guards,
                      read_campaign_stop_conditions,
                      record_campaign_stop_condition,
                      record_sample_outcome)
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
            raise RuntimeError(
                "campaign stopped before worker authorization: "
                f"{stop_records[0].get('condition')}"
            )
        if os.path.isfile(ack_path):
            with open(ack_path, encoding="utf-8") as handle:
                ack = json.load(handle)
            expected = {
                "schema": "anchorpatch.worker_start/1",
                "worker_launch_id": worker_id,
                "worker_pid": os.getpid(),
                "invocation_id": invocation_id,
                "sample": sample_id,
                "task_plan_sha256": entry["sha256"],
            }
            if ack != expected:
                raise RuntimeError("dispatcher worker authorization mismatch")
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


def _enforce_post_call_campaign_guard(out_dir, sample_id):
    """Prevent an in-flight response from committing after a global stop.

    The provider call has its own pre-call guard, but a sibling can set the
    durable latch while that call is in flight.  Recheck both the latch and
    active-worker authorization before local evaluation; the final relay
    commit repeats the latch check under the campaign metadata lock.
    """
    enforce_campaign_runtime_guards(out_dir, sample_id)


def _require_formal_dispatch_environment(out_dir, samples):
    """Prevent an unleased standalone runner from joining a paired campaign."""
    manifest_path = os.path.join(
        os.path.abspath(out_dir), "dispatch_manifest.json")
    if not os.path.isfile(manifest_path):
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
    enforce_active_worker_authorization(out_dir, samples[0])


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


def _require_formal_opencode_transport(model):
    if not str(model).lower().startswith("minimax-m3"):
        return
    transport = (os.environ.get("OPENCODE_TRANSPORT") or "anthropic_sdk_v2").strip()
    if transport != "anthropic_sdk_v2":
        raise RuntimeError(
            "formal MiniMax-M3 experiments require OPENCODE_TRANSPORT="
            "anthropic_sdk_v2; urllib_v1 is diagnostic-only"
        )
    minimax_transport = (
        os.environ.get("MINIMAX_TRANSPORT") or "opencode"
    ).strip()
    if minimax_transport != "opencode":
        raise RuntimeError(
            "formal MiniMax-M3 experiments require MINIMAX_TRANSPORT=opencode; "
            "official_nonstream belongs to a different transport revision"
        )


def _editable(ctx, distractor):
    return {k: v for k, v in ctx.items() if k not in distractor}


def _readonly(ctx, distractor):
    return {k: v for k, v in ctx.items() if k in distractor}


def _matches_target(filename, target_filenames):
    for target in target_filenames or []:
        if is_wildcard(target):
            if fnmatch.fnmatch(filename, target):
                return True
        elif filename == target:
            return True
    return False


def _utf8_context_size(context, filenames=None):
    selected = None if filenames is None else set(filenames)
    total = 0
    for filename, content in (context or {}).items():
        if selected is not None and filename not in selected:
            continue
        total += len(str(content).encode("utf-8"))
    return total


def _final_target_metrics(input_real, final_context, target_filenames):
    target_outputs = {
        filename: content for filename, content in (final_context or {}).items()
        if _matches_target(filename, target_filenames)
    }
    touched = sum(
        1 for filename in set(input_real or {}) | set(final_context or {})
        if _matches_target(filename, target_filenames)
        and (input_real or {}).get(filename) != (final_context or {}).get(filename)
    )
    input_bytes = _utf8_context_size(input_real)
    output_bytes = _utf8_context_size(target_outputs)
    ratio = (output_bytes / input_bytes) if input_bytes else None
    return touched, ratio


def _hybrid_key(attempt):
    log = attempt.get("exec_log")
    rate = round(log.op_accept_rate, 4) if log is not None else 0.0
    envelope = attempt.get("envelope")
    # A V8 schema refusal happens before any action is eligible to run.
    # HybridExecLog historically reports the empty 0/0 operation set as a
    # 1.0 acceptance rate; that legacy convention must not make a refused
    # primary outrank an executable (including partially acceptable) repair.
    # Restrict the adjustment to V8 so archived V1-V7 attempt selection remains
    # byte-for-byte compatible with its existing policy.
    if (isinstance(envelope, dict) and envelope.get("protocol") == PROTOCOL_V8
            and log is not None
            and log.error == "schema_error"
            and log.ops_total == 0):
        rate = -1.0
    return (
        int(not attempt.get("invalid_json") and bool(attempt.get("envelope"))),
        int(bool(attempt.get("gate_pass")) and bool(attempt.get("gen"))),
        int(bool(attempt.get("clean_protocol"))),
        rate,
    )


def _run_attempt_hybrid(raw, input_real, target_filenames, readonly_names,
                        edit_instruction=None, require_effective_change=False):
    envelope, em = extract_hybrid_json(raw)
    bodies = em.get("bodies") or {}
    a = {
        "raw": raw,
        "envelope": envelope,
        "bodies": bodies,
        "burden": {},
        "burden_overages": {},
        "protocol_burden_exceeded": False,
        "partial_extraction": em.get("partial_extraction"),
        "fence_complete": em.get("fence_complete"),
        "invalid_json": envelope is None,
        "gen": None,
        "exec_log": None,
        "gate_pass": False,
        "errors": [],
        "gate_errors": [],
        "trigger": None,
        "need_repair": False,
        "clean_protocol": False,
        "partial_ok": False,
        "forward_audit": None,
        "schema_errors": [],
        "schema_warnings": [],
    }
    if envelope is None:
        a["errors"] = ["no valid HybridPatch JSON envelope could be extracted from the response"]
        a["trigger"] = "invalid_json"
        a["need_repair"] = True
        a["key"] = _hybrid_key(a)
        return a

    burden = measure_protocol_burden(envelope, bodies=bodies)
    overages = (
        protocol_burden_overages(burden)
        if envelope.get("protocol") == PROTOCOL_V8 else {}
    )
    schema_errors, schema_warnings = validate_hybrid_envelope(
        envelope, bodies=bodies, editable_filenames=input_real.keys())
    a["burden"] = dict(burden or {})
    a["burden_overages"] = overages
    a["protocol_burden_exceeded"] = bool(overages)
    a["schema_errors"] = list(schema_errors)
    a["schema_warnings"] = list(schema_warnings)
    gen, log = apply_hybrid(input_real, envelope, target_filenames, bodies=bodies)
    a["gen"] = gen
    a["exec_log"] = log

    gate_pass, gate_errors = validate_hybrid_output(
        input_real, gen, target_filenames, log,
        readonly_filenames=readonly_names,
        require_effective_change=require_effective_change)
    reject_errors = [
        f"op[{d.get('i')}] {d.get('op')} rejected: {d.get('reason')}"
        for d in (log.reject_reasons() if log is not None else [])
    ]
    if reject_errors:
        gate_pass = False
        gate_errors = list(gate_errors) + ["op_rejected"]
    route_violations = ((getattr(log, "hybrid", None) or {}).get("route_violations") or [])
    if route_violations:
        gate_pass = False
    # v7 policy (FINDINGS §226): op-rejection-only failures may commit the
    # partial application. Eligibility is shared with the honesty gate and
    # hard-gated on the envelope's protocol rev, so pre-v7 rows are unaffected.
    a["partial_ok"] = partial_acceptance_eligible(
        input_real, gen, target_filenames, log,
        readonly_filenames=readonly_names,
        require_effective_change=require_effective_change)
    if require_effective_change:
        a["forward_audit"] = audit_forward_completion(
            input_real, gen, target_filenames, edit_instruction)

    errors = []
    if em.get("partial_extraction"):
        errors.append("HybridPatch JSON was not emitted in a complete fenced json block")
    errors += list(schema_errors)
    errors += reject_errors
    if not gen:
        errors.append("the HybridPatch action produced no output files")
    if not gate_pass:
        errors += [f"validation: {e}" for e in gate_errors]

    if em.get("partial_extraction"):
        trigger = "partial_extraction"
    elif schema_errors:
        trigger = "schema_error"
    elif reject_errors:
        trigger = "op_rejected"
    elif route_violations:
        trigger = "route_violation"
    elif not gen:
        trigger = "empty_output"
    elif not gate_pass:
        trigger = "validation_gate"
    else:
        trigger = None
    a.update(
        errors=errors,
        gate_errors=gate_errors,
        trigger=trigger,
        gate_pass=bool(gate_pass and gen),
        # Any failure class is worth the one bounded repair call: op rejects,
        # route violations, empty output and gate failures are all precisely
        # describable errors the model can fix (exp_20260706_hybridthink5: 6 of
        # 9 kept-context steps never got a repair chance under the old
        # json/schema-only trigger).
        need_repair=trigger is not None,
        clean_protocol=bool(not em.get("partial_extraction") and not schema_errors
                            and not reject_errors and not route_violations),
    )
    a["key"] = _hybrid_key(a)
    return a


def _has_hybrid_protocol_signal(raw):
    """Whether a non-empty response made a recognizable patch attempt.

    This deliberately avoids a byte/token threshold. Plain prose, refusals,
    and greetings are Baseline-style model failures; JSON/fence/protocol
    markers are eligible for the single semantic repair call.
    """
    text = "" if raw is None else str(raw)
    lowered = text.lower()
    return (
        any(marker in lowered for marker in (
            "hybridpatch/", '"protocol_rev"', "[file bodies]", "<anchorpatch",
        ))
        or re.search(
            r'"route"\s*:\s*"(?:local_patch|bulk_patch|dsl_rules|bounded_rewrite)"',
            lowered,
        ) is not None
    )


def _attempt_hybrid_repair(errors, model=None, max_tokens=None, generate_fn=None,
                           *legacy_args,
                           previous_envelope=None, editable_context=None,
                           edit_instruction=None, target_filenames=None,
                           readonly_filenames=None, prompt_classification=None,
                           current_route=None):
    # Narrow compatibility for the frozen direct helper test, whose old shape
    # was (raw, errors, model, max_tokens, generate_fn, ...).  The discarded raw
    # value is intentionally never forwarded to the compact V8 repair prompt.
    if not callable(generate_fn) and len(legacy_args) == 1 and callable(legacy_args[0]):
        _discarded_raw = errors
        errors, model, max_tokens, generate_fn = (
            model, max_tokens, generate_fn, legacy_args[0])
        del _discarded_raw
    if not callable(generate_fn):
        raise TypeError("generate_fn must be callable")
    repair_prompt = build_hybrid_repair_prompt(
        errors, previous_envelope=previous_envelope,
        editable_context=editable_context, edit_instruction=edit_instruction,
        target_filenames=target_filenames, readonly_filenames=readonly_filenames,
        prompt_classification=prompt_classification, current_route=current_route)
    out = generate_fn([{"role": "user", "content": repair_prompt}], model=model,
                      max_tokens=max_tokens, return_metadata=True,
                      timeout=1800, max_retries=LLM_MAX_RETRIES,
                      thinking_mode="adaptive", call_kind="hybridpatch_repair")
    rraw = out["message"] if isinstance(out, dict) else str(out)
    return rraw, (out if isinstance(out, dict) else {}), len(repair_prompt)


_NUM_META = ("prompt_tokens", "completion_tokens", "total_tokens",
             "total_usd", "total_cny", "elapsed_time",
             "retry_count", "failed_attempt_count", "quota_wait_count",
             "rate_limit_wait_count", "transient_wait_count",
             "input_tokens", "output_tokens", "cache_read_input_tokens",
             "cache_creation_input_tokens", "prompt_cache_hit_tokens",
             "prompt_cache_miss_tokens")


def _merge_meta(m0, m1):
    merged = dict(m0)
    for k in _NUM_META:
        a, b = m0.get(k), m1.get(k)
        if a is not None or b is not None:
            merged[k] = (a or 0) + (b or 0)
    for k in ("api_call_ids", "api_raw_paths", "finish_reasons", "call_kinds",
              "response_classifications", "transport_attempts"):
        xs = list(m0.get(k) or [])
        xs.extend(list(m1.get(k) or []))
        if xs:
            merged[k] = xs
    counts = {}
    for source in (m0, m1):
        for name, value in (source.get("content_block_counts") or {}).items():
            counts[name] = counts.get(name, 0) + (value or 0)
    if counts:
        merged["content_block_counts"] = counts
        merged["content_block_count"] = sum(counts.values())
    if m0.get("stream_complete") is not None or m1.get("stream_complete") is not None:
        merged["stream_complete"] = all(
            source.get("stream_complete") is not False for source in (m0, m1)
        )
    if m0.get("timeout_hit") or m1.get("timeout_hit"):
        merged["timeout_hit"] = True
    for k in ("provider_request_id", "http_status", "finish_reason", "stop_reason",
              "base_url", "request_url", "transport", "transport_revision",
              "anthropic_sdk_version", "temperature", "max_tokens", "timeout",
              "max_retries", "thinking_mode", "response_classification"):
        if merged.get(k) is None:
            merged[k] = m1.get(k)
    return merged


# ---------------------------------------------------------------------------
# one edit step (forward or backward), method-specific generation, shared eval
# ---------------------------------------------------------------------------
def _edit_step(method, domain, sample_id, model, current_context, distractor,
               target_state, edit_instruction, max_tokens, generate_fn,
               step_direction=None):
    """Returns (raw, gen_real, meta, exec_log, method_tag, input_real, v2_info)."""
    input_real = _editable(current_context, distractor)
    target_filenames = list(target_state["context"])
    v2_info = None

    if method == "hybridpatch":
        readonly = _readonly(current_context, distractor)
        readonly_names = list(readonly)
        require_effective_change = step_direction == "forward"
        prompt_classification = classify_operation_family(edit_instruction)
        prompt = build_hybrid_prompt(
            input_real, edit_instruction, target_filenames,
            readonly_context=readonly or None,
            prompt_classification=prompt_classification)
        prompt_chars = len(prompt)
        out = generate_fn([{"role": "user", "content": prompt}], model=model,
                          max_tokens=max_tokens, return_metadata=True,
                          timeout=1800, max_retries=LLM_MAX_RETRIES,
                          thinking_mode="adaptive", call_kind="hybridpatch_primary")
        raw0 = out["message"] if isinstance(out, dict) else str(out)
        meta = out if isinstance(out, dict) else {}
        a0 = _run_attempt_hybrid(
            raw0, input_real, target_filenames, readonly_names,
            edit_instruction=edit_instruction,
            require_effective_change=require_effective_change)
        response_classification = meta.get("response_classification")
        if (not str(raw0 or "").strip()
                or response_classification in {
                    "thinking_budget_exhausted", "model_empty", "model_refusal",
                }):
            a0["trigger"] = "empty_response"
            a0["need_repair"] = False
            a0["errors"] = [
                "complete provider response contained no usable model output"
            ]
        elif a0.get("invalid_json") and not _has_hybrid_protocol_signal(raw0):
            a0["trigger"] = "non_protocol_response"
            a0["need_repair"] = False
            a0["errors"] = [
                "model returned non-protocol prose without a recognizable patch attempt"
            ]
        repair = {"attempted": False, "trigger": None, "used": False, "success": False,
                  "original_raw": None, "repair_raw": None, "repair_errors": None,
                  "repair_tokens": None, "repair_prompt_chars": 0,
                  "pick_rule": "hybrid_key_v1"}
        chosen = a0
        a1 = None
        if a0["need_repair"]:
            previous_action = ((a0.get("envelope") or {}).get("action") or {})
            rraw, rmeta, repair_prompt_chars = _attempt_hybrid_repair(
                a0["errors"], model, max_tokens, generate_fn,
                previous_envelope=a0.get("envelope"), editable_context=input_real,
                edit_instruction=edit_instruction, target_filenames=target_filenames,
                readonly_filenames=readonly_names,
                prompt_classification=prompt_classification,
                current_route=previous_action.get("route"))
            a1 = _run_attempt_hybrid(
                rraw, input_real, target_filenames, readonly_names,
                edit_instruction=edit_instruction,
                require_effective_change=require_effective_change)
            meta = _merge_meta(meta, rmeta)
            if a1["key"] > a0["key"]:
                chosen = a1
            # repair_errors = the PRIMARY attempt's errors (what the repair
            # prompt was asked to fix), NOT the repair output's own errors —
            # those surface via the chosen attempt's gate/telemetry fields
            # (protein1 RT4 label investigation, FINDINGS §226 附记).
            repair.update(attempted=True, trigger=a0["trigger"],
                          used=(chosen is a1),
                          success=(chosen is a1 and not a1["errors"]),
                          original_raw=raw0, repair_raw=rraw,
                          repair_errors=a0["errors"][:25],
                          repair_tokens=rmeta.get("completion_tokens"),
                          repair_prompt_chars=repair_prompt_chars)

        raw = chosen["raw"]
        exec_log = chosen["exec_log"]
        failed_kept = chosen["gen"] is None or not chosen["gen"] or not chosen["gate_pass"]
        partial_acceptance = False
        if failed_kept and chosen.get("partial_ok"):
            # v7: commit the accepted-op subset; rejected ops are skipped and
            # disclosed below. The repair call already had its chance — this
            # replaces only the terminal kept-context outcome.
            gen_real, method_tag = chosen["gen"], "hybridpatch"
            failed_kept = False
            partial_acceptance = True
        elif failed_kept:
            gen_real, method_tag = dict(input_real), "hybridpatch_protocol_failure_kept_context"
        else:
            gen_real, method_tag = chosen["gen"], "hybridpatch"
        lh = (exec_log.hybrid if exec_log is not None and exec_log.hybrid else {})
        chosen_envelope = chosen.get("envelope") or {}
        chosen_action = chosen_envelope.get("action") or {}
        chosen_plan = chosen_envelope.get("plan") or {}
        burden = chosen.get("burden") or {}
        burden_exceeded = bool(
            a0.get("protocol_burden_exceeded")
            or (a1 and a1.get("protocol_burden_exceeded"))
        )
        attempt_overages = {"primary": a0.get("burden_overages") or {}}
        if a1 is not None:
            attempt_overages["repair"] = a1.get("burden_overages") or {}
        touched_file_count, size_ratio = _final_target_metrics(
            input_real, gen_real, target_filenames)
        v2_info = {
            "schema": "anchorpatch.hybrid.telemetry/1",
            # Real executed protocol rev (from the envelope), not a hardcoded label.
            "protocol_version": lh.get("protocol_rev") or "hybridpatch/1",
            "protocol_rev": lh.get("protocol_rev"),
            "route": lh.get("route") or chosen_action.get("route"),
            "task_family": lh.get("task_family") or chosen_plan.get("task_family"),
            "prompt_classification": prompt_classification,
            "prompt_classifier": prompt_classification.get("classifier"),
            "operation_family": prompt_classification.get("operation_family"),
            "classifier_matched_families": prompt_classification.get("matched_families") or [],
            "classifier_matched_terms": [
                match.get("term") for match in (prompt_classification.get("matches") or [])
                if isinstance(match, dict) and match.get("term")
            ],
            "prompt_profile": prompt_classification.get("prompt_profile"),
            "prompt_chars": prompt_chars,
            "repair_prompt_chars": repair.get("repair_prompt_chars") or 0,
            "envelope_chars": burden.get("envelope_chars"),
            "envelope_bytes": burden.get("envelope_bytes"),
            "local_op_count": burden.get("local_op_count"),
            "bulk_op_count": burden.get("bulk_op_count"),
            "explicit_op_count": burden.get("explicit_op_count"),
            "anchor_bytes": burden.get("anchor_bytes"),
            "explicit_block_id_count": burden.get("explicit_block_id_count"),
            "touched_file_count": touched_file_count,
            "input_output_size_ratio": size_ratio,
            "protocol_burden_exceeded": burden_exceeded,
            "protocol_burden_overages": chosen.get("burden_overages") or {},
            "protocol_burden_attempt_overages": attempt_overages,
            "invalid_json": chosen.get("invalid_json"),
            "partial_extraction": chosen.get("partial_extraction"),
            "fence_complete": chosen.get("fence_complete"),
            "schema_error_count": len(chosen.get("schema_errors") or []),
            "schema_warnings": chosen.get("schema_warnings") or [],
            "failed_step_kept_context": bool(failed_kept),
            "partial_acceptance": partial_acceptance,
            "partial_skipped_ops": (
                [d for d in exec_log.reject_reasons()][:12]
                if partial_acceptance and exec_log is not None else []),
            "validation_gate_errors": chosen.get("gate_errors") or [],
            "failure_reason": chosen.get("trigger"),
            "effective_modification": bool(gen_real != input_real),
            "forward_audit": chosen.get("forward_audit"),
            "repair": repair,
            "copied_source_bytes": lh.get("bytes_copied_from_source"),
            "generated_bytes": lh.get("bytes_generated_by_model"),
            "generated_byte_ratio": lh.get("generated_byte_ratio"),
            "bounded_rewrite": bool(lh.get("bounded_rewrite")),
            "route_share_key": lh.get("route"),
            "call_budget": {"primary_calls": 1, "repair_calls": int(repair["attempted"])},
            "route_violations": lh.get("route_violations") or [],
        }


    else:  # fullrewrite
        prompt = domain.prepare_prompt(current_context, target_state, edit_instruction)
        out = generate_fn([{"role": "user", "content": prompt}], model=model,
                          max_tokens=max_tokens, return_metadata=True,
                          timeout=1800, max_retries=LLM_MAX_RETRIES,
                          thinking_mode="adaptive", call_kind="fullrewrite_primary")
        raw = out["message"] if isinstance(out, dict) else str(out)
        gen_real, exec_log, method_tag = parse_context_string(raw), None, "full_rewrite"
        meta = out if isinstance(out, dict) else {}

    return raw, gen_real, meta, exec_log, method_tag, input_real, v2_info


def _evaluate(domain, sample_id, gen_real, target_state, target_filenames):
    if not is_context_complete(gen_real, target_filenames):
        return {"error": "context_mismatch",
                "detailed_error": "one or more target files missing from output"}
    # Upstream run_single_step_edit parity: on wildcard targets every generated
    # file must match some target pattern, else the step is a wildcard_mismatch.
    if any(is_wildcard(f) for f in target_filenames):
        valid, err_msg = validate_wildcard_context(gen_real, target_filenames)
        if not valid:
            return {"error": "wildcard_mismatch", "detailed_error": err_msg}
    return domain.evaluate_context(sample_id, gen_real, target_state)


def _row(method, sample_id, sample_type, model, rid_chain, state_chain, rt_num,
         direction, target_state_id, initial_state_id, raw, evaluation, meta,
         exec_log, method_tag, doc_changed, fwd_changed, distractor_included,
         v2_info=None, edit_instruction=None):
    bd = {
        "actual_method": method_tag,
        "noop_forward": (direction == "forward" and not doc_changed),
        "bytes_changed": doc_changed,
        "ecr_available": True,
        "ecr_pass": bool(fwd_changed),          # round-trip forward actually edited the doc
        "preservation_violations": (exec_log.preservation_violations if exec_log else None),
        "op_accept_rate": (round(exec_log.op_accept_rate, 4) if exec_log else None),
        "used_emit_file": (exec_log.used_emit_file if exec_log else None),
        "survival_rate": (round(exec_log.survival_rate, 4) if exec_log else None),
        "preservation_rate": (round(exec_log.preservation_rate, 4) if exec_log else None),
        "exec_log": (exec_log.to_dict() if exec_log else None),
        "fallback_used": (method_tag == "full_rewrite_fallback"),
    }
    if v2_info is not None:
        if isinstance(v2_info, dict) and v2_info.get("schema") == "anchorpatch.hybrid.telemetry/1":
            bd["hybrid"] = v2_info
        else:
            bd["v2"] = v2_info
    return {
        "sample_id": sample_id, "sample_type": sample_type, "method": method,
        "model_name": model, "response_id": rid_chain[-1], "rid_chain": list(rid_chain),
        "state_chain": list(state_chain), "round_trip_num": rt_num,
        "round_trip_direction": direction, "target_state_id": target_state_id,
        "task_state_id": target_state_id, "initial_state_id": initial_state_id,
        "edit_instruction": edit_instruction,
        "raw_llm_response": raw, "evaluation": evaluation,
        "prompt_tokens": meta.get("prompt_tokens"), "completion_tokens": meta.get("completion_tokens"),
        "total_tokens": meta.get("total_tokens"), "total_usd": meta.get("total_usd"),
        "total_cny": meta.get("total_cny"), "latency": meta.get("elapsed_time"),
        "input_tokens": meta.get("input_tokens"), "output_tokens": meta.get("output_tokens"),
        "cache_read_input_tokens": meta.get("cache_read_input_tokens"),
        "cache_creation_input_tokens": meta.get("cache_creation_input_tokens"),
        "api_call_ids": meta.get("api_call_ids"), "api_raw_paths": meta.get("api_raw_paths"),
        "provider_request_id": meta.get("provider_request_id"),
        "http_status": meta.get("http_status"), "finish_reason": meta.get("finish_reason"),
        "finish_reasons": meta.get("finish_reasons"),
        "stop_reason": meta.get("stop_reason"),
        "stream_complete": meta.get("stream_complete"),
        "response_classification": meta.get("response_classification"),
        "response_classifications": meta.get("response_classifications"),
        "thinking_mode": meta.get("thinking_mode"),
        "call_kinds": meta.get("call_kinds"),
        "transport": meta.get("transport"),
        "transport_revision": meta.get("transport_revision"),
        "api_transport_attempts": meta.get("transport_attempts"),
        "api_retry_count": meta.get("retry_count"),
        "api_failed_attempt_count": meta.get("failed_attempt_count"),
        "api_quota_wait_count": meta.get("quota_wait_count"),
        "api_rate_limit_wait_count": meta.get("rate_limit_wait_count"),
        "api_transient_wait_count": meta.get("transient_wait_count"),
        "api_timeout_hit": meta.get("timeout_hit"),
        "distractor_included": distractor_included, "bdpatch": bd,
    }


# ---------------------------------------------------------------------------
# relay
# ---------------------------------------------------------------------------
def run_relay(method, sample_id, num_round_trips=10, seed=42, include_distractor=True,
              out_dir=RESULTS_DIR, model=MODEL_DEFAULT, max_tokens=None,
              generate_fn=None, printing=True, inline_report=False, fr_baseline=None,
              stop_on_collapse=False, stop_on_preservation_violation=False):
    _require_formal_opencode_transport(model)
    if max_tokens is None and not str(model).lower().startswith("minimax-m3"):
        max_tokens = 20000
    random.seed(seed)
    base_generate_fn = generate_fn or _real_generate
    api_recorder = None
    if generate_fn is None:
        api_recorder = ApiCallRecorder(out_dir, method, sample_id, None,
                                       model, base_generate_fn)
        generate_fn = api_recorder.generate
    else:
        generate_fn = base_generate_fn
    sample, sample_folder, id2state = load_sample(sample_id, samples_folder=os.path.join(SAMPLES_ROOT, ""))
    sample_type = sample["sample_type"]
    domain = get_domain(sample_type)
    domain.samples_folder = os.path.join(SAMPLES_ROOT, "")

    distractor = load_distractor_context(sample_folder) if include_distractor else {}
    initial_state_id = sample["start_state"]
    initial_state = id2state[initial_state_id]
    possible_forward = [p["target_state"] for p in initial_state["prompts"]]

    # shared, reproducible forward sequence (saved once, reused by both methods)
    plan_path = os.path.join(out_dir, f"{sample_id}.task_plan.json")
    if os.path.exists(plan_path):
        task_plan = load_relay_task_plan(plan_path)
    else:
        os.makedirs(out_dir, exist_ok=True)
        task_plan = build_relay_task_plan(possible_forward, num_round_trips, seed=seed)
        save_relay_task_plan(plan_path, task_plan)
    task_plan = task_plan[:num_round_trips]
    registered_plan = register_task_plan(
        out_dir, sample_id, plan_path, num_round_trips=num_round_trips)
    expected_plan_sha = os.environ.get(
        "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256")
    if (expected_plan_sha
            and registered_plan.get("sha256") != expected_plan_sha):
        raise RuntimeError(
            f"task-plan hash differs from dispatch manifest for {sample_id}"
        )

    method_dir = os.path.join(out_dir, method)
    os.makedirs(method_dir, exist_ok=True)
    jsonl_path = os.path.join(method_dir, f"{sample_id}.jsonl")
    ckpt_path = os.path.join(method_dir, f"{sample_id}.ckpt.json")

    log = RunLogger(out_dir, method, sample_id, to_console=printing,
                    header=f"{method}/{sample_id} model={model} seed={seed} "
                           f"RT={num_round_trips} distractor={include_distractor}")

    # init / resume
    if os.path.exists(ckpt_path):
        with open(ckpt_path, encoding="utf-8") as ckpt_file:
            ck = json.load(ckpt_file)
        start_rt = ck["completed_round_trips"]
        current_context = ck["current_context"]
        rid_chain = ck["rid_chain"]
        state_chain = ck["state_chain"]
        if ck.get("context_shuffle_random_state") is not None:
            random.setstate(_tuple_tree(ck["context_shuffle_random_state"]))
        if ck.get("stopped_early"):
            log.line(f"[{method}/{sample_id}] already stopped early at RT{start_rt} "
                     f"({ck.get('stop_reason')}) — nothing to resume")
            log.close()
            return jsonl_path
        log.line(f"[{method}/{sample_id}] resume from round trip {start_rt + 1}")
    else:
        current_context = build_context_from_folder(os.path.join(sample_folder, initial_state["solution_folder"]))
        if include_distractor:
            current_context = merge_distractor(current_context, distractor)
        current_context = shuffle_context(current_context)
        rid_chain, state_chain, start_rt = [], [], 0

    start_rt, current_context, rid_chain, state_chain = _reconcile_checkpoint_from_jsonl(
        jsonl_path, ckpt_path, start_rt, current_context, rid_chain, state_chain,
        id2state, initial_state, distractor, include_distractor, log)
    current_state = initial_state if start_rt == 0 else id2state[initial_state_id]
    prev_ap = None

    for rt_idx in range(start_rt, num_round_trips):
        rt_num = rt_idx + 1
        pending_rows = []

        # ----- forward -----
        fwd_target_id = task_plan[rt_idx]
        fwd_state = id2state[fwd_target_id]
        fwd_instr = [p for p in current_state["prompts"] if p["target_state"] == fwd_target_id][0]["prompt"]
        if api_recorder:
            api_recorder.set_step(rt_num, "forward", fwd_target_id)
        try:
            raw, gen_real, meta, elog, tag, in_real, v2i = _edit_step(
                method, domain, sample_id, model, current_context, distractor,
                fwd_state, fwd_instr, max_tokens, generate_fn,
                step_direction="forward")
        except Exception as e:
            if api_recorder and not getattr(e, "_anchorpatch_api_recorded", False):
                api_recorder.record_runner_exception(e)
            log.close()
            raise
        try:
            _enforce_post_call_campaign_guard(out_dir, sample_id)
        except Exception:
            log.close()
            raise
        fwd_changed = (gen_real != in_real)
        with log.capture("eval"):
            evaluation = _evaluate(domain, sample_id, gen_real, fwd_state, list(fwd_state["context"]))
        fwd_target = list(fwd_state["context"])
        fwd_out = sorted(gen_real.keys())
        fwd_complete = is_context_complete(gen_real, fwd_target)
        rid_chain.append(generate_response_id()); state_chain.append(fwd_target_id)
        fwd_row = _row(method, sample_id, sample_type, model, rid_chain, state_chain,
                       rt_num, "forward", fwd_target_id, initial_state_id, raw,
                       evaluation, meta, elog, tag, fwd_changed, fwd_changed,
                       include_distractor, v2_info=v2i, edit_instruction=fwd_instr)
        pending_rows.append(fwd_row)
        if (stop_on_preservation_violation
                and _record_preservation_stop(
                    out_dir, method, sample_id, rt_num, "forward", fwd_row)):
            log.close()
            raise RuntimeError(
                f"preservation_violations>0 at {method}/{sample_id}/RT{rt_num}/forward"
            )
        if api_recorder:
            record_model_content_anomaly(out_dir, fwd_row)
        dump_step_docs(out_dir, method, sample_id, rt_num, "fwd", fwd_target_id, gen_real,
                       step_info={"score": evaluation.get("score"), "error": evaluation.get("error"),
                                  "actual_method": tag, "bytes_changed": fwd_changed,
                                  "ops": (elog.to_dict().get("ops_accepted") if elog else None,
                                          elog.to_dict().get("ops_total") if elog else None)})
        current_context = shuffle_context(merge_distractor(gen_real, distractor))
        current_state = fwd_state

        # ----- backward (to basic_state) -----
        bwd_instr = [p for p in current_state["prompts"] if p["target_state"] == initial_state_id][0]["prompt"]
        if api_recorder:
            api_recorder.set_step(rt_num, "backward", initial_state_id)
        try:
            raw, gen_real, meta, elog, tag, in_real, v2i = _edit_step(
                method, domain, sample_id, model, current_context, distractor,
                initial_state, bwd_instr, max_tokens, generate_fn,
                step_direction="backward")
        except Exception as e:
            if api_recorder and not getattr(e, "_anchorpatch_api_recorded", False):
                api_recorder.record_runner_exception(e)
            log.close()
            raise
        try:
            _enforce_post_call_campaign_guard(out_dir, sample_id)
        except Exception:
            log.close()
            raise
        bwd_changed = (gen_real != in_real)
        with log.capture("eval"):
            evaluation = _evaluate(domain, sample_id, gen_real, initial_state, list(initial_state["context"]))
        bwd_out = sorted(gen_real.keys())
        rid_chain.append(generate_response_id()); state_chain.append(initial_state_id)
        bwd_row = _row(method, sample_id, sample_type, model, rid_chain, state_chain,
                       rt_num, "backward", initial_state_id, initial_state_id, raw,
                       evaluation, meta, elog, tag, bwd_changed, fwd_changed,
                       include_distractor, v2_info=v2i, edit_instruction=bwd_instr)
        pending_rows.append(bwd_row)
        if (stop_on_preservation_violation
                and _record_preservation_stop(
                    out_dir, method, sample_id, rt_num, "backward", bwd_row)):
            log.close()
            raise RuntimeError(
                f"preservation_violations>0 at {method}/{sample_id}/RT{rt_num}/backward"
            )
        if api_recorder:
            record_model_content_anomaly(out_dir, bwd_row)
        dump_step_docs(out_dir, method, sample_id, rt_num, "bwd", initial_state_id, gen_real,
                       step_info={"score": evaluation.get("score"), "error": evaluation.get("error"),
                                  "actual_method": tag, "bytes_changed": bwd_changed,
                                  "ops": (elog.to_dict().get("ops_accepted") if elog else None,
                                          elog.to_dict().get("ops_total") if elog else None)})
        current_context = shuffle_context(merge_distractor(gen_real, distractor))
        current_state = initial_state

        # ----- collapse detection: backward RS hit 0 (parse error / mismatch / score 0) -----
        bev = pending_rows[-1].get("evaluation") or {}
        bwd_rs = bev.get("score")
        collapsed = (("error" in bev and bwd_rs is None)
                     or (bwd_rs is not None and bwd_rs <= 1e-9))

        # ----- atomic commit: both rows + checkpoint -----
        # Repeat immutable identity checks after evaluation and immediately
        # before the latch-ordered commit.  This catches Git/task-plan drift
        # that appeared while either provider call was in flight.
        try:
            enforce_campaign_runtime_guards(out_dir, sample_id)
        except Exception:
            log.close()
            raise
        ckpt = {"completed_round_trips": rt_num, "current_context": current_context,
                "rid_chain": rid_chain, "state_chain": state_chain,
                "context_shuffle_random_state": random.getstate()}
        if stop_on_collapse and collapsed:
            ckpt["stopped_early"] = True
            ckpt["stop_reason"] = f"backward_RS_collapsed_to_0_at_RT{rt_num}"
        try:
            commit = append_relay_rows_and_checkpoint(
                jsonl_path, ckpt_path, pending_rows, ckpt,
                campaign_out_dir=out_dir,
            )
        except Exception:
            log.close()
            raise
        if commit.get("status") != "appended":
            log.line(f"[{method}/{sample_id}] duplicate committed row keys detected at RT{rt_num}: "
                     f"{commit.get('overlap_keys')} — stopping duplicate runner without appending")
            break

        # ----- inline per-round-trip score + failure analysis (no waiting for the end) -----
        if inline_report or printing:
            b = pending_rows[-1]
            ev = b["evaluation"]; bd = b["bdpatch"]; el = bd.get("exec_log") or {}
            ap_rs = ev.get("score"); err = ev.get("error")
            # Baseline error mapping: an error row scores 0 — display that
            # effective score; the failure reason stays in the FAIL: flag.
            eff_rs = ap_rs if ap_rs is not None else (0.0 if err else None)
            frm = (fr_baseline or {}).get(sample_id, {})
            fr_rs = frm.get(str(rt_num), frm.get(rt_num))
            flags = []
            if err:
                flags.append(f"FAIL:{err}")
            elif eff_rs is not None and eff_rs < 0.5:
                flags.append("LOW(<50)")
            if eff_rs is not None and prev_ap is not None and (prev_ap - eff_rs) > 0.10:
                flags.append(f"DROP(-{100 * (prev_ap - eff_rs):.1f})")
            # Scores display as percent-style (82.5, not 0.825); the reference
            # column is the frozen FR baseline passed via --fr_baseline.
            method_label = {"hybridpatch": "HP", "fullrewrite": "FR"}.get(
                method, method[:2].upper())
            ap_disp = f"{100 * eff_rs:.1f}" if eff_rs is not None else "n/a"
            fr_disp = f"{100 * fr_rs:.1f}" if fr_rs is not None else "n/a"
            delta = (f"{100 * (eff_rs - fr_rs):+.1f}"
                     if (eff_rs is not None and fr_rs is not None) else "n/a")
            log.line(f"[{sample_id}] RT{rt_num:>2} fwd={fwd_target_id:28s} "
                     f"{method_label}={ap_disp:>9} "
                     f"FRbase={fr_disp:>6} Δ={delta:>7} ecr={int(fwd_changed)} "
                     f"{(' '.join(flags)) if flags else 'ok'}")
            if flags and inline_report:
                log.line(f"      ↳ fwd[{fwd_target_id}] expects {fwd_target} -> got {fwd_out} complete={fwd_complete}")
                log.line(f"      ↳ bwd target {list(initial_state['context'])} -> got {bwd_out} "
                         f"ops={el.get('ops_accepted')}/{el.get('ops_total')} emit={bd.get('used_emit_file')} "
                         f"surv={bd.get('survival_rate')} preserv_viol={bd.get('preservation_violations')}")
            if eff_rs is not None:
                prev_ap = eff_rs

        if stop_on_collapse and collapsed:
            log.line(f"[{sample_id}] EARLY-STOP at RT{rt_num}: backward RS collapsed to 0 "
                     f"— skipping the remaining {num_round_trips - rt_num} round trip(s)")
            break

    log.close()
    return jsonl_path


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", nargs="+", default=DEFAULT_SAMPLES)
    ap.add_argument("--methods", nargs="+", default=["hybridpatch", "fullrewrite"])
    ap.add_argument("--num_round_trips", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--skip_distractor", action="store_true")
    ap.add_argument("--out_dir", default=RESULTS_DIR)
    ap.add_argument("--model", default=MODEL_DEFAULT)
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
    args = ap.parse_args()
    if args.max_tokens == 0:
        args.max_tokens = None  # MiniMax model layer substitutes 131072.
    elif args.max_tokens is None and not str(args.model).lower().startswith("minimax-m3"):
        args.max_tokens = 20000
    _require_formal_opencode_transport(args.model)
    _require_formal_dispatch_environment(args.out_dir, args.sample)

    fr_baseline = None
    if args.fr_baseline and os.path.exists(args.fr_baseline):
        fr_baseline = json.load(open(args.fr_baseline, encoding="utf-8"))

    run_metadata = append_run_metadata(
        args.out_dir, command="python " + " ".join(sys.argv),
        samples=args.sample, methods=args.methods, num_round_trips=args.num_round_trips,
        seed=args.seed, model=args.model, distractor=not args.skip_distractor,
        max_tokens=args.max_tokens, notes=args.notes,
        context_shuffle_seeded=True,
        context_shuffle_seed_version="global_random_seed_v1",
        stop_on_collapse=args.stop_on_collapse,
        stop_on_preservation_violation=args.stop_on_preservation_violation)

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
                )
        finish_status = "finished"
        if len(args.sample) == 1:
            record_sample_outcome(
                args.out_dir, args.sample[0], "finished",
                invocation_id=run_metadata["invocation_id"],
                methods=list(args.methods),
                checkpoint_progress=_sample_checkpoint_progress(
                    args.out_dir, args.sample[0], args.methods),
            )
    except Exception as exc:
        if (getattr(exc, "_anchorpatch_failure_class", None)
                == "infrastructure_incomplete" and len(args.sample) == 1):
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
                },
            )
        raise
    finally:
        finish_run_metadata(
            args.out_dir, run_metadata["invocation_id"], status=finish_status)


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
