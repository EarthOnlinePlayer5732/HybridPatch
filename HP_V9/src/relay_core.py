"""Shared DELEGATE-52 relay semantics, independent of campaign control state.

The legacy runner and the simplified runtime both call :func:`run_method`.
Infrastructure is injected through ``RelayHooks``; this module never reads or
writes campaign metadata, worker registries, stop latches, or recovery records.
"""
from dataclasses import dataclass
import fnmatch
import os
import random
import re

from domains import get_domain
from hybrid_executor import apply_hybrid
from hybrid_gate import (audit_forward_completion, partial_acceptance_eligible,
                         validate_hybrid_output)
from hybrid_prompt import (build_hybrid_prompt, build_hybrid_repair_prompt,
                           classify_operation_family, extract_hybrid_json)
from hybrid_schema import (PROTOCOL_V8, measure_protocol_burden,
                           protocol_burden_overages,
                           validate_hybrid_envelope)
from utils_context import (build_context_from_folder, is_context_complete,
                           is_wildcard, parse_context_string,
                           validate_wildcard_context)
from utils_env import (load_distractor_context, load_sample, merge_distractor,
                       shuffle_context)
from utils_results import generate_response_id


MODEL_DEFAULT = "deepseek-v4-flash"
LLM_MAX_RETRIES = 3
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
SAMPLES_ROOT = os.path.join(_ROOT, "data", "samples_delegate52")


class EvaluatorIncompleteError(RuntimeError):
    """A sample-local domain evaluator exception after model generation."""

    _anchorpatch_failure_class = "evaluator_incomplete"

    def __init__(self, sample_id, method, rt_index, direction,
                 target_state_id, cause):
        error_type = f"{type(cause).__module__}.{type(cause).__qualname__}"
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


class PreservationViolationError(RuntimeError):
    """The deterministic executor reported a sample-local preservation bug."""

    _anchorpatch_failure_class = "preservation_invalid"

    def __init__(self, method, sample_id, rt_index, direction, count, meta=None):
        super().__init__(
            f"preservation_violations={count} at "
            f"{method}/{sample_id}/RT{rt_index}/{direction}"
        )
        self.method = method
        self.sample_id = sample_id
        self.rt_index = rt_index
        self.direction = direction
        self.count = count
        self.meta = dict(meta or {})


@dataclass
class RelayHooks:
    """Sample-local side effects supplied by a runtime."""

    commit_round_trip: object
    set_step: object = None
    after_generate: object = None
    before_commit: object = None
    on_step: object = None
    log: object = None

    def call(self, name, *args, **kwargs):
        fn = getattr(self, name, None)
        return fn(*args, **kwargs) if callable(fn) else None


def require_formal_opencode_transport(model, reasoning_effort=None):
    """Keep the frozen DeepSeek transport/request contract unchanged."""
    from model_openai import model_runtime_config, resolve_model_name
    resolved = resolve_model_name(str(model))
    model_l = resolved.lower()
    if model_l.startswith("deepseek-v4-"):
        base_url = (os.environ.get("OPENAI_BASE_URL") or "").rstrip("/")
        if base_url != "https://opencode.ai/zen/go/v1":
            raise RuntimeError(
                "formal DeepSeek-V4 experiments require OPENAI_BASE_URL="
                "https://opencode.ai/zen/go/v1")
        if reasoning_effort != "high":
            raise RuntimeError(
                "formal OpenCode DeepSeek-V4 experiments require "
                "reasoning_effort=high")
        runtime = model_runtime_config(
            resolved, max_tokens=20000, reasoning_effort=reasoning_effort)
        if (runtime.get("transport") != "openai_sdk_stream"
                or runtime.get("transport_revision")
                != "opencode_openai_compatible/6"):
            raise RuntimeError(
                "formal OpenCode DeepSeek-V4 experiments require "
                "opencode_openai_compatible/6 compact streaming transport")
        return
    if not model_l.startswith("minimax-m3"):
        return
    if (os.environ.get("OPENCODE_TRANSPORT") or "anthropic_sdk_v2").strip() != "anthropic_sdk_v2":
        raise RuntimeError(
            "formal MiniMax-M3 experiments require OPENCODE_TRANSPORT="
            "anthropic_sdk_v2; urllib_v1 is diagnostic-only")
    if (os.environ.get("MINIMAX_TRANSPORT") or "opencode").strip() != "opencode":
        raise RuntimeError(
            "formal MiniMax-M3 experiments require MINIMAX_TRANSPORT=opencode")


def _editable(ctx, distractor):
    return {k: v for k, v in ctx.items() if k not in distractor}


def _readonly(ctx, distractor):
    return {k: v for k, v in ctx.items() if k in distractor}


def _matches_target(filename, target_filenames):
    return any(
        fnmatch.fnmatch(filename, target) if is_wildcard(target)
        else filename == target
        for target in (target_filenames or [])
    )


def _utf8_context_size(context, filenames=None):
    selected = None if filenames is None else set(filenames)
    return sum(
        len(str(content).encode("utf-8"))
        for filename, content in (context or {}).items()
        if selected is None or filename in selected
    )


def _final_target_metrics(input_real, final_context, target_filenames):
    target_outputs = {
        name: content for name, content in (final_context or {}).items()
        if _matches_target(name, target_filenames)
    }
    touched = sum(
        1 for name in set(input_real or {}) | set(final_context or {})
        if _matches_target(name, target_filenames)
        and (input_real or {}).get(name) != (final_context or {}).get(name)
    )
    input_bytes = _utf8_context_size(input_real)
    output_bytes = _utf8_context_size(target_outputs)
    return touched, (output_bytes / input_bytes) if input_bytes else None


def _hybrid_key(attempt):
    log = attempt.get("exec_log")
    rate = round(log.op_accept_rate, 4) if log is not None else 0.0
    envelope = attempt.get("envelope")
    if (isinstance(envelope, dict) and envelope.get("protocol") == PROTOCOL_V8
            and log is not None and log.error == "schema_error"
            and log.ops_total == 0):
        rate = -1.0
    return (
        int(not attempt.get("invalid_json") and bool(envelope)),
        int(bool(attempt.get("gate_pass")) and bool(attempt.get("gen"))),
        int(bool(attempt.get("clean_protocol"))),
        rate,
    )


def _run_attempt_hybrid(raw, input_real, target_filenames, readonly_names,
                        edit_instruction=None, require_effective_change=False):
    envelope, extraction = extract_hybrid_json(raw)
    bodies = extraction.get("bodies") or {}
    attempt = {
        "raw": raw, "envelope": envelope, "bodies": bodies, "burden": {},
        "burden_overages": {}, "protocol_burden_exceeded": False,
        "partial_extraction": extraction.get("partial_extraction"),
        "fence_complete": extraction.get("fence_complete"),
        "invalid_json": envelope is None, "gen": None, "exec_log": None,
        "gate_pass": False, "errors": [], "gate_errors": [], "trigger": None,
        "need_repair": False, "clean_protocol": False, "partial_ok": False,
        "forward_audit": None, "schema_errors": [], "schema_warnings": [],
    }
    if envelope is None:
        attempt.update(
            errors=["no valid HybridPatch JSON envelope could be extracted from the response"],
            trigger="invalid_json", need_repair=True)
        attempt["key"] = _hybrid_key(attempt)
        return attempt

    burden = measure_protocol_burden(envelope, bodies=bodies)
    overages = (protocol_burden_overages(burden)
                if envelope.get("protocol") == PROTOCOL_V8 else {})
    schema_errors, schema_warnings = validate_hybrid_envelope(
        envelope, bodies=bodies, editable_filenames=input_real.keys())
    gen, exec_log = apply_hybrid(
        input_real, envelope, target_filenames, bodies=bodies)
    attempt.update(
        burden=dict(burden or {}), burden_overages=overages,
        protocol_burden_exceeded=bool(overages),
        schema_errors=list(schema_errors), schema_warnings=list(schema_warnings),
        gen=gen, exec_log=exec_log)

    gate_pass, gate_errors = validate_hybrid_output(
        input_real, gen, target_filenames, exec_log,
        readonly_filenames=readonly_names,
        require_effective_change=require_effective_change)
    reject_errors = [
        f"op[{d.get('i')}] {d.get('op')} rejected: {d.get('reason')}"
        for d in (exec_log.reject_reasons() if exec_log is not None else [])
    ]
    if reject_errors:
        gate_pass = False
        gate_errors = list(gate_errors) + ["op_rejected"]
    route_violations = (
        (getattr(exec_log, "hybrid", None) or {}).get("route_violations") or [])
    if route_violations:
        gate_pass = False
    attempt["partial_ok"] = partial_acceptance_eligible(
        input_real, gen, target_filenames, exec_log,
        readonly_filenames=readonly_names,
        require_effective_change=require_effective_change)
    if require_effective_change:
        attempt["forward_audit"] = audit_forward_completion(
            input_real, gen, target_filenames, edit_instruction)

    errors = []
    if extraction.get("partial_extraction"):
        errors.append("HybridPatch JSON was not emitted in a complete fenced json block")
    errors += list(schema_errors) + reject_errors
    if not gen:
        errors.append("the HybridPatch action produced no output files")
    if not gate_pass:
        errors += [f"validation: {item}" for item in gate_errors]
    if extraction.get("partial_extraction"):
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
    attempt.update(
        errors=errors, gate_errors=gate_errors, trigger=trigger,
        gate_pass=bool(gate_pass and gen), need_repair=trigger is not None,
        clean_protocol=bool(not extraction.get("partial_extraction")
                            and not schema_errors and not reject_errors
                            and not route_violations))
    attempt["key"] = _hybrid_key(attempt)
    return attempt


def _has_hybrid_protocol_signal(raw):
    text = "" if raw is None else str(raw)
    lowered = text.lower()
    return (
        any(marker in lowered for marker in (
            "hybridpatch/", '"protocol_rev"', "[file bodies]", "<anchorpatch"))
        or re.search(
            r'"route"\s*:\s*"(?:local_patch|bulk_patch|dsl_rules|bounded_rewrite)"',
            lowered) is not None
    )


def _attempt_hybrid_repair(errors, model=None, max_tokens=None, generate_fn=None,
                           *legacy_args, previous_envelope=None,
                           editable_context=None, edit_instruction=None,
                           target_filenames=None, readonly_filenames=None,
                           prompt_classification=None, current_route=None,
                           reasoning_effort=None):
    if not callable(generate_fn) and len(legacy_args) == 1 and callable(legacy_args[0]):
        errors, model, max_tokens, generate_fn = (
            model, max_tokens, generate_fn, legacy_args[0])
    if not callable(generate_fn):
        raise TypeError("generate_fn must be callable")
    prompt = build_hybrid_repair_prompt(
        errors, previous_envelope=previous_envelope,
        editable_context=editable_context, edit_instruction=edit_instruction,
        target_filenames=target_filenames, readonly_filenames=readonly_filenames,
        prompt_classification=prompt_classification, current_route=current_route)
    out = generate_fn(
        [{"role": "user", "content": prompt}], model=model,
        max_tokens=max_tokens, return_metadata=True, timeout=1800,
        max_retries=LLM_MAX_RETRIES, thinking_mode="adaptive",
        reasoning_effort=reasoning_effort, call_kind="hybridpatch_repair")
    raw = out["message"] if isinstance(out, dict) else str(out)
    return raw, (out if isinstance(out, dict) else {}), len(prompt)


_NUM_META = (
    "prompt_tokens", "completion_tokens", "total_tokens", "total_usd",
    "total_cny", "elapsed_time", "retry_count", "failed_attempt_count",
    "quota_wait_count", "rate_limit_wait_count", "transient_wait_count",
    "input_tokens", "output_tokens", "cache_read_input_tokens",
    "cache_creation_input_tokens", "prompt_cache_hit_tokens",
    "prompt_cache_miss_tokens")


def _merge_meta(first, second):
    merged = dict(first)
    for key in _NUM_META:
        a, b = first.get(key), second.get(key)
        if a is not None or b is not None:
            merged[key] = (a or 0) + (b or 0)
    for key in ("api_call_ids", "api_raw_paths", "finish_reasons", "call_kinds",
                "response_classifications", "transport_attempts"):
        values = list(first.get(key) or []) + list(second.get(key) or [])
        if values:
            merged[key] = values
    counts = {}
    for source in (first, second):
        for name, value in (source.get("content_block_counts") or {}).items():
            counts[name] = counts.get(name, 0) + (value or 0)
    if counts:
        merged["content_block_counts"] = counts
        merged["content_block_count"] = sum(counts.values())
    if first.get("stream_complete") is not None or second.get("stream_complete") is not None:
        merged["stream_complete"] = all(
            source.get("stream_complete") is not False
            for source in (first, second))
    if first.get("timeout_hit") or second.get("timeout_hit"):
        merged["timeout_hit"] = True
    for key in (
            "provider_request_id", "http_status", "finish_reason", "stop_reason",
            "base_url", "request_url", "transport", "transport_revision",
            "anthropic_sdk_version", "temperature", "max_tokens", "timeout",
            "max_retries", "thinking_mode", "reasoning_effort",
            "response_classification"):
        if merged.get(key) is None:
            merged[key] = second.get(key)
    return merged


def _edit_step(method, domain, sample_id, model, current_context, distractor,
               target_state, edit_instruction, max_tokens, generate_fn,
               step_direction=None, reasoning_effort=None):
    """Return raw, generated context, metadata, exec log, tag, input, telemetry."""
    input_real = _editable(current_context, distractor)
    target_filenames = list(target_state["context"])
    telemetry = None
    if method == "hybridpatch":
        readonly_names = list(_readonly(current_context, distractor))
        require_change = step_direction == "forward"
        classification = classify_operation_family(edit_instruction)
        prompt = build_hybrid_prompt(
            input_real, edit_instruction, target_filenames,
            readonly_context=_readonly(current_context, distractor) or None,
            prompt_classification=classification)
        out = generate_fn(
            [{"role": "user", "content": prompt}], model=model,
            max_tokens=max_tokens, return_metadata=True, timeout=1800,
            max_retries=LLM_MAX_RETRIES, thinking_mode="adaptive",
            reasoning_effort=reasoning_effort,
            call_kind="hybridpatch_primary")
        raw0 = out["message"] if isinstance(out, dict) else str(out)
        meta = out if isinstance(out, dict) else {}
        primary = _run_attempt_hybrid(
            raw0, input_real, target_filenames, readonly_names,
            edit_instruction=edit_instruction,
            require_effective_change=require_change)
        response_class = meta.get("response_classification")
        if (not str(raw0 or "").strip()
                or response_class in {
                    "thinking_budget_exhausted", "model_empty", "model_refusal"}):
            primary.update(
                trigger="empty_response", need_repair=False,
                errors=["complete provider response contained no usable model output"])
        elif primary.get("invalid_json") and not _has_hybrid_protocol_signal(raw0):
            primary.update(
                trigger="non_protocol_response", need_repair=False,
                errors=["model returned non-protocol prose without a recognizable patch attempt"])
        repair = {
            "attempted": False, "trigger": None, "used": False,
            "success": False, "original_raw": None, "repair_raw": None,
            "repair_errors": None, "repair_tokens": None,
            "repair_prompt_chars": 0, "pick_rule": "hybrid_key_v1"}
        chosen, repaired = primary, None
        if primary["need_repair"]:
            previous_action = ((primary.get("envelope") or {}).get("action") or {})
            repaired_raw, repaired_meta, repair_chars = _attempt_hybrid_repair(
                primary["errors"], model, max_tokens, generate_fn,
                previous_envelope=primary.get("envelope"),
                editable_context=input_real, edit_instruction=edit_instruction,
                target_filenames=target_filenames,
                readonly_filenames=readonly_names,
                prompt_classification=classification,
                current_route=previous_action.get("route"),
                reasoning_effort=reasoning_effort)
            repaired = _run_attempt_hybrid(
                repaired_raw, input_real, target_filenames, readonly_names,
                edit_instruction=edit_instruction,
                require_effective_change=require_change)
            meta = _merge_meta(meta, repaired_meta)
            if repaired["key"] > primary["key"]:
                chosen = repaired
            repair.update(
                attempted=True, trigger=primary["trigger"],
                used=(chosen is repaired),
                success=(chosen is repaired and not repaired["errors"]),
                original_raw=raw0, repair_raw=repaired_raw,
                repair_errors=primary["errors"][:25],
                repair_tokens=repaired_meta.get("completion_tokens"),
                repair_prompt_chars=repair_chars)
        raw = chosen["raw"]
        exec_log = chosen["exec_log"]
        failed_kept = not chosen["gen"] or not chosen["gate_pass"]
        partial_acceptance = False
        if failed_kept and chosen.get("partial_ok"):
            gen_real, method_tag = chosen["gen"], "hybridpatch"
            failed_kept = False
            partial_acceptance = True
        elif failed_kept:
            gen_real = dict(input_real)
            method_tag = "hybridpatch_protocol_failure_kept_context"
        else:
            gen_real, method_tag = chosen["gen"], "hybridpatch"
        hybrid_log = (exec_log.hybrid if exec_log is not None and exec_log.hybrid else {})
        envelope = chosen.get("envelope") or {}
        action, plan = envelope.get("action") or {}, envelope.get("plan") or {}
        burden = chosen.get("burden") or {}
        attempt_overages = {"primary": primary.get("burden_overages") or {}}
        if repaired is not None:
            attempt_overages["repair"] = repaired.get("burden_overages") or {}
        touched, size_ratio = _final_target_metrics(
            input_real, gen_real, target_filenames)
        telemetry = {
            "schema": "anchorpatch.hybrid.telemetry/1",
            "protocol_version": hybrid_log.get("protocol_rev") or "hybridpatch/1",
            "protocol_rev": hybrid_log.get("protocol_rev"),
            "route": hybrid_log.get("route") or action.get("route"),
            "task_family": hybrid_log.get("task_family") or plan.get("task_family"),
            "prompt_classification": classification,
            "prompt_classifier": classification.get("classifier"),
            "operation_family": classification.get("operation_family"),
            "classifier_matched_families": classification.get("matched_families") or [],
            "classifier_matched_terms": [
                item.get("term") for item in (classification.get("matches") or [])
                if isinstance(item, dict) and item.get("term")],
            "prompt_profile": classification.get("prompt_profile"),
            "prompt_chars": len(prompt),
            "repair_prompt_chars": repair.get("repair_prompt_chars") or 0,
            "envelope_chars": burden.get("envelope_chars"),
            "envelope_bytes": burden.get("envelope_bytes"),
            "local_op_count": burden.get("local_op_count"),
            "bulk_op_count": burden.get("bulk_op_count"),
            "explicit_op_count": burden.get("explicit_op_count"),
            "anchor_bytes": burden.get("anchor_bytes"),
            "explicit_block_id_count": burden.get("explicit_block_id_count"),
            "touched_file_count": touched,
            "input_output_size_ratio": size_ratio,
            "protocol_burden_exceeded": bool(
                primary.get("protocol_burden_exceeded")
                or (repaired and repaired.get("protocol_burden_exceeded"))),
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
                [item for item in exec_log.reject_reasons()][:12]
                if partial_acceptance and exec_log is not None else []),
            "validation_gate_errors": chosen.get("gate_errors") or [],
            "failure_reason": chosen.get("trigger"),
            "effective_modification": bool(gen_real != input_real),
            "forward_audit": chosen.get("forward_audit"),
            "repair": repair,
            "copied_source_bytes": hybrid_log.get("bytes_copied_from_source"),
            "generated_bytes": hybrid_log.get("bytes_generated_by_model"),
            "generated_byte_ratio": hybrid_log.get("generated_byte_ratio"),
            "bounded_rewrite": bool(hybrid_log.get("bounded_rewrite")),
            "route_share_key": hybrid_log.get("route"),
            "call_budget": {"primary_calls": 1,
                            "repair_calls": int(repair["attempted"])},
            "route_violations": hybrid_log.get("route_violations") or [],
        }
    else:
        prompt = domain.prepare_prompt(current_context, target_state, edit_instruction)
        out = generate_fn(
            [{"role": "user", "content": prompt}], model=model,
            max_tokens=max_tokens, return_metadata=True, timeout=1800,
            max_retries=LLM_MAX_RETRIES, thinking_mode="adaptive",
            reasoning_effort=reasoning_effort,
            call_kind="fullrewrite_primary")
        raw = out["message"] if isinstance(out, dict) else str(out)
        gen_real, exec_log, method_tag = (
            parse_context_string(raw), None, "full_rewrite")
        meta = out if isinstance(out, dict) else {}
    return raw, gen_real, meta, exec_log, method_tag, input_real, telemetry


def _evaluate(domain, sample_id, gen_real, target_state, target_filenames, *,
              method=None, rt_index=None, direction=None, target_state_id=None):
    if not is_context_complete(gen_real, target_filenames):
        return {"error": "context_mismatch",
                "detailed_error": "one or more target files missing from output"}
    if any(is_wildcard(name) for name in target_filenames):
        valid, message = validate_wildcard_context(gen_real, target_filenames)
        if not valid:
            return {"error": "wildcard_mismatch", "detailed_error": message}
    try:
        return domain.evaluate_context(sample_id, gen_real, target_state)
    except Exception as exc:
        if not (method and isinstance(rt_index, int) and rt_index >= 1
                and direction in {"forward", "backward"} and target_state_id):
            raise
        raise EvaluatorIncompleteError(
            sample_id, method, rt_index, direction, target_state_id, exc) from exc


def _row(method, sample_id, sample_type, model, rid_chain, state_chain, rt_num,
         direction, target_state_id, initial_state_id, raw, evaluation, meta,
         exec_log, method_tag, doc_changed, fwd_changed, distractor_included,
         v2_info=None, edit_instruction=None):
    bdpatch = {
        "actual_method": method_tag,
        "noop_forward": direction == "forward" and not doc_changed,
        "bytes_changed": doc_changed, "ecr_available": True,
        "ecr_pass": bool(fwd_changed),
        "preservation_violations": (
            exec_log.preservation_violations if exec_log else None),
        "op_accept_rate": round(exec_log.op_accept_rate, 4) if exec_log else None,
        "used_emit_file": exec_log.used_emit_file if exec_log else None,
        "survival_rate": round(exec_log.survival_rate, 4) if exec_log else None,
        "preservation_rate": round(exec_log.preservation_rate, 4) if exec_log else None,
        "exec_log": exec_log.to_dict() if exec_log else None,
        "fallback_used": method_tag == "full_rewrite_fallback",
    }
    if v2_info is not None:
        bdpatch["hybrid" if v2_info.get("schema") == "anchorpatch.hybrid.telemetry/1"
                else "v2"] = v2_info
    row = {
        "sample_id": sample_id, "sample_type": sample_type, "method": method,
        "model_name": model, "response_id": rid_chain[-1],
        "rid_chain": list(rid_chain), "state_chain": list(state_chain),
        "round_trip_num": rt_num, "round_trip_direction": direction,
        "target_state_id": target_state_id, "task_state_id": target_state_id,
        "initial_state_id": initial_state_id,
        "edit_instruction": edit_instruction, "raw_llm_response": raw,
        "evaluation": evaluation, "distractor_included": distractor_included,
        "bdpatch": bdpatch,
    }
    field_map = {
        "prompt_tokens": "prompt_tokens", "completion_tokens": "completion_tokens",
        "total_tokens": "total_tokens", "total_usd": "total_usd",
        "total_cny": "total_cny", "latency": "elapsed_time",
        "input_tokens": "input_tokens", "output_tokens": "output_tokens",
        "cache_read_input_tokens": "cache_read_input_tokens",
        "cache_creation_input_tokens": "cache_creation_input_tokens",
        "api_call_ids": "api_call_ids", "api_raw_paths": "api_raw_paths",
        "provider_request_id": "provider_request_id", "http_status": "http_status",
        "finish_reason": "finish_reason", "finish_reasons": "finish_reasons",
        "stop_reason": "stop_reason", "stream_complete": "stream_complete",
        "response_classification": "response_classification",
        "response_classifications": "response_classifications",
        "thinking_mode": "thinking_mode", "reasoning_effort": "reasoning_effort",
        "call_kinds": "call_kinds", "transport": "transport",
        "transport_revision": "transport_revision",
        "api_transport_attempts": "transport_attempts",
        "api_retry_count": "retry_count",
        "api_failed_attempt_count": "failed_attempt_count",
        "api_quota_wait_count": "quota_wait_count",
        "api_rate_limit_wait_count": "rate_limit_wait_count",
        "api_transient_wait_count": "transient_wait_count",
        "api_timeout_hit": "timeout_hit",
    }
    row.update({output: meta.get(source) for output, source in field_map.items()})
    return row


def _tuple_tree(value):
    return tuple(_tuple_tree(item) for item in value) if isinstance(value, list) else value


def _direction_step(method, domain, sample_id, model, context, distractor,
                    target_state, instruction, target_state_id, initial_state_id,
                    sample_type, rt_num, direction, max_tokens, generate_fn,
                    reasoning_effort, rid_chain, state_chain, include_distractor,
                    fwd_changed, hooks, stop_on_preservation_violation,
                    edit_step_fn, evaluate_fn, row_fn, response_id_fn,
                    merge_fn, shuffle_fn):
    hooks.call("set_step", rt_num, direction, target_state_id)
    raw, generated, meta, exec_log, tag, input_real, telemetry = edit_step_fn(
        method, domain, sample_id, model, context, distractor, target_state,
        instruction, max_tokens, generate_fn, step_direction=direction,
        reasoning_effort=reasoning_effort)
    hooks.call("after_generate", rt_num, direction, target_state_id)
    changed = generated != input_real
    preservation_count = getattr(exec_log, "preservation_violations", None)
    if (stop_on_preservation_violation
            and isinstance(preservation_count, int)
            and not isinstance(preservation_count, bool)
            and preservation_count > 0):
        event = {
            "rt_num": rt_num, "direction": direction,
            "target_state_id": target_state_id, "generated": generated,
            "meta": meta, "exec_log": exec_log, "telemetry": telemetry,
            "error": "preservation_violation"}
        hooks.call("on_step", event)
        raise PreservationViolationError(
            method, sample_id, rt_num, direction, preservation_count, meta)
    try:
        evaluation = evaluate_fn(
            domain, sample_id, generated, target_state,
            list(target_state["context"]), method=method, rt_index=rt_num,
            direction=direction, target_state_id=target_state_id)
    except EvaluatorIncompleteError as exc:
        hooks.call("on_step", {
            "rt_num": rt_num, "direction": direction,
            "target_state_id": target_state_id, "generated": generated,
            "meta": meta, "exec_log": exec_log, "telemetry": telemetry,
            "error": "evaluator_exception", "exception": exc})
        raise
    rid_chain.append(response_id_fn())
    state_chain.append(target_state_id)
    row = row_fn(
        method, sample_id, sample_type, model, rid_chain, state_chain, rt_num,
        direction, target_state_id, initial_state_id, raw, evaluation, meta,
        exec_log, tag, changed,
        changed if direction == "forward" else fwd_changed,
        include_distractor, v2_info=telemetry, edit_instruction=instruction)
    hooks.call("on_step", {
        "rt_num": rt_num, "direction": direction,
        "target_state_id": target_state_id, "generated": generated,
        "meta": meta, "exec_log": exec_log, "telemetry": telemetry,
        "evaluation": evaluation, "row": row, "changed": changed})
    return row, shuffle_fn(merge_fn(generated, distractor)), changed


def run_method(method, sample_id, task_plan, *, num_round_trips=10, seed=42,
               include_distractor=True, model=MODEL_DEFAULT, max_tokens=None,
               generate_fn, hooks, resume_state=None,
               reasoning_effort=None, stop_on_collapse=False,
               stop_on_preservation_violation=False,
               samples_root=SAMPLES_ROOT, validate_transport=True,
               load_sample_fn=load_sample, get_domain_fn=get_domain,
               load_distractor_fn=load_distractor_context,
               build_context_fn=build_context_from_folder,
               edit_step_fn=_edit_step, evaluate_fn=_evaluate, row_fn=_row,
               response_id_fn=generate_response_id,
               merge_fn=merge_distractor, shuffle_fn=shuffle_context):
    """Run or resume one method for one sample using injected local I/O."""
    if method not in {"hybridpatch", "fullrewrite"}:
        raise ValueError(f"unsupported method: {method}")
    if validate_transport:
        require_formal_opencode_transport(model, reasoning_effort)
    if max_tokens is None and not str(model).lower().startswith("minimax-m3"):
        max_tokens = 20000
    random.seed(seed)
    sample, sample_folder, id2state = load_sample_fn(
        sample_id, samples_folder=os.path.join(samples_root, ""))
    sample_type = sample["sample_type"]
    domain = get_domain_fn(sample_type)
    domain.samples_folder = os.path.join(samples_root, "")
    distractor = load_distractor_fn(sample_folder) if include_distractor else {}
    initial_state_id = sample["start_state"]
    initial_state = id2state[initial_state_id]
    if len(task_plan) < num_round_trips:
        raise RuntimeError("task plan is shorter than requested round trips")

    if resume_state:
        start_rt = int(resume_state["completed_round_trips"])
        current_context = resume_state["current_context"]
        rid_chain = list(resume_state["rid_chain"])
        state_chain = list(resume_state["state_chain"])
        if resume_state.get("context_shuffle_random_state") is not None:
            random.setstate(_tuple_tree(
                resume_state["context_shuffle_random_state"]))
    else:
        start_rt = 0
        current_context = build_context_fn(
            os.path.join(sample_folder, initial_state["solution_folder"]))
        if include_distractor:
            current_context = merge_fn(current_context, distractor)
        current_context = shuffle_fn(current_context)
        rid_chain, state_chain = [], []
    if start_rt < 0 or start_rt > num_round_trips:
        raise RuntimeError("checkpoint round-trip progress is invalid")

    final_rt = start_rt
    for rt_idx in range(start_rt, num_round_trips):
        rt_num = rt_idx + 1
        forward_id = task_plan[rt_idx]
        forward_state = id2state[forward_id]
        forward_instruction = next(
            item["prompt"] for item in initial_state["prompts"]
            if item["target_state"] == forward_id)
        forward_row, current_context, forward_changed = _direction_step(
            method, domain, sample_id, model, current_context, distractor,
            forward_state, forward_instruction, forward_id, initial_state_id,
            sample_type, rt_num, "forward", max_tokens, generate_fn,
            reasoning_effort, rid_chain, state_chain, include_distractor,
            False, hooks, stop_on_preservation_violation,
            edit_step_fn, evaluate_fn, row_fn, response_id_fn,
            merge_fn, shuffle_fn)
        backward_instruction = next(
            item["prompt"] for item in forward_state["prompts"]
            if item["target_state"] == initial_state_id)
        backward_row, current_context, _ = _direction_step(
            method, domain, sample_id, model, current_context, distractor,
            initial_state, backward_instruction, initial_state_id,
            initial_state_id, sample_type, rt_num, "backward", max_tokens,
            generate_fn, reasoning_effort, rid_chain, state_chain,
            include_distractor, forward_changed, hooks,
            stop_on_preservation_violation, edit_step_fn, evaluate_fn,
            row_fn, response_id_fn, merge_fn, shuffle_fn)
        evaluation = backward_row.get("evaluation") or {}
        score = evaluation.get("score")
        collapsed = (("error" in evaluation and score is None)
                     or (score is not None and score <= 1e-9))
        checkpoint = {
            "completed_round_trips": rt_num,
            "current_context": current_context,
            "rid_chain": rid_chain,
            "state_chain": state_chain,
            "context_shuffle_random_state": random.getstate(),
        }
        if stop_on_collapse and collapsed:
            checkpoint.update(
                stopped_early=True,
                stop_reason=f"backward_RS_collapsed_to_0_at_RT{rt_num}")
        hooks.call("before_commit", rt_num)
        result = hooks.commit_round_trip(
            [forward_row, backward_row], checkpoint)
        if isinstance(result, dict) and result.get("status") not in {None, "appended"}:
            break
        hooks.call("log", {
            "sample": sample_id, "method": method, "round_trip": rt_num,
            "forward_target": forward_id, "backward_score": score,
            "forward_changed": forward_changed})
        final_rt = rt_num
        if stop_on_collapse and collapsed:
            break
    return {
        "sample": sample_id, "method": method,
        "completed_round_trips": final_rt,
    }
