"""Read-only structural/full verification for simplified V9 campaigns."""
import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import random

from domains import get_domain
from relay_core import _evaluate
from simple_api_recorder import (LocalCallEvidenceError, scan_call_journals,
                                 validate_complete_success_journal)
from simple_runtime_io import (LocalEvidenceError, _replay_generated,
                               load_task_plan, method_dir, read_json,
                               read_result_rows, read_status, utc_now,
                               validate_checkpoint_state,
                               validate_result_pairs, validate_result_trajectory,
                               write_json_atomic)
from utils_context import build_context_from_folder
from utils_env import (load_distractor_context, load_sample, merge_distractor,
                       shuffle_context)


QUICK_SCHEMA = "anchorpatch.simple_quick_summary/1"
VERIFY_SCHEMA = "anchorpatch.simple_campaign_verification/1"


def _checkpoint(path):
    if not Path(path).is_file():
        return None
    try:
        return read_json(path)
    except Exception:
        return None


def _score(row):
    if not isinstance(row, dict):
        return None
    evaluation = row.get("evaluation") or {}
    if not isinstance(evaluation, dict):
        return None
    value = evaluation.get("score")
    return float(value) if isinstance(value, (int, float)) else None


def _method_summary(out_dir, sample, method, target_rt, *, skip=False):
    path = method_dir(out_dir, sample, method)
    errors = []
    summary = {
        "method": method, "result_rows": 0,
        "committed_round_trips": 0, "checkpoint_round_trips": None,
        "complete": False, "success_journals": 0, "failure_journals": 0,
        "failure_types": {}, "http_statuses": {},
        "retry_budget_consumed": 0, "preservation_violations": 0,
        "backward_score_mean": None, "final_backward_score": None,
        "errors": errors,
    }
    if skip:
        errors.append("sample lock is held by an external worker; evidence not read")
        return summary
    try:
        rows = read_result_rows(path / "result.jsonl")
        _by_rt, complete = validate_result_pairs(rows, target_rt)
    except Exception as exc:
        rows, complete = [], 0
        errors.append(str(exc))
    summary["result_rows"] = len(rows)
    summary["committed_round_trips"] = complete
    try:
        checkpoint = _checkpoint(path / "checkpoint.json")
        checkpoint_rt = (
            checkpoint.get("completed_round_trips")
            if isinstance(checkpoint, dict) else None)
        if (checkpoint_rt is not None
                and (not isinstance(checkpoint_rt, int)
                     or isinstance(checkpoint_rt, bool))):
            raise LocalEvidenceError("checkpoint progress is not an integer")
        summary["checkpoint_round_trips"] = checkpoint_rt
        if checkpoint_rt != complete:
            errors.append(
                f"checkpoint/result mismatch: checkpoint={checkpoint_rt}, pairs={complete}")
    except Exception as exc:
        errors.append(f"checkpoint summary failed: {exc}")
        checkpoint_rt = None
    try:
        successes, failures = scan_call_journals(path)
    except Exception as exc:
        successes, failures = [], []
        errors.append(f"call journal scan failed: {exc}")
    summary["success_journals"] = len(successes)
    summary["failure_journals"] = len(failures)
    failure_types = Counter()
    http_statuses = Counter()
    retry_consumed = 0
    for failure in failures:
        if not isinstance(failure, dict):
            errors.append("failure journal is not an object")
            continue
        details = failure.get("failure") or {}
        if not isinstance(details, dict):
            errors.append(f"failure journal payload is invalid: {failure.get('_path')}")
            continue
        failure_types[str(details.get("provider_error_type")
                          or details.get("error_type") or "unknown")] += 1
        if details.get("http_status") is not None:
            http_statuses[str(details["http_status"])] += 1
        attempts = details.get("transport_attempts") or []
        if not isinstance(attempts, list):
            errors.append(f"failure attempts are invalid: {failure.get('_path')}")
            continue
        for attempt in attempts:
            if not isinstance(attempt, dict):
                errors.append(f"failure attempt is invalid: {failure.get('_path')}")
                continue
            retry_consumed += int(bool(attempt.get("retry_budget_consumed")))
    for success in successes:
        if not isinstance(success, dict):
            errors.append("success journal is not an object")
            continue
        response = success.get("response") or {}
        if not isinstance(response, dict):
            errors.append(f"success response is invalid: {success.get('_path')}")
            continue
        attempts = response.get("transport_attempts") or []
        if not isinstance(attempts, list):
            errors.append(f"success attempts are invalid: {success.get('_path')}")
            continue
        for attempt in attempts:
            if not isinstance(attempt, dict):
                errors.append(f"success attempt is invalid: {success.get('_path')}")
                continue
            status = attempt.get("http_status")
            if status is not None:
                http_statuses[str(status)] += 1
            retry_consumed += int(bool(attempt.get("retry_budget_consumed")))
    backward_scores = [
        _score(row) for row in rows
        if row.get("round_trip_direction") == "backward"
        and _score(row) is not None]
    preservation = 0
    for row in rows:
        try:
            if not isinstance(row, dict):
                raise TypeError("result row is not an object")
            bdpatch = row.get("bdpatch") or {}
            if not isinstance(bdpatch, dict):
                raise TypeError("bdpatch is not an object")
            value = bdpatch.get("preservation_violations")
            if value is not None:
                if not isinstance(value, int) or isinstance(value, bool):
                    raise TypeError("preservation_violations is not an integer")
                preservation += value
        except Exception as exc:
            errors.append(f"result preservation summary failed: {exc}")
    summary.update({
        "complete": complete == target_rt and checkpoint_rt == target_rt and not errors,
        "failure_types": dict(sorted(failure_types.items())),
        "http_statuses": dict(sorted(http_statuses.items())),
        "retry_budget_consumed": retry_consumed,
        "preservation_violations": preservation,
        "backward_score_mean": (
            sum(backward_scores) / len(backward_scores)
            if backward_scores else None),
        "final_backward_score": next((
            _score(row) for row in reversed(rows)
            if row.get("round_trip_direction") == "backward"
            and row.get("round_trip_num") == target_rt), None),
    })
    return summary


def build_quick_summary(out_dir, campaign_state_hint=None, *, skip_samples=None):
    out_dir = Path(out_dir).resolve()
    run = read_json(out_dir / "run.json")
    scientific = run["scientific"]
    target_rt = int(scientific["round_trips"])
    samples = list(scientific["samples"])
    sample_rows = {}
    state_counts = Counter()
    totals = Counter()
    all_complete = True
    skip_samples = set(skip_samples or ())
    for sample in samples:
        methods = list(scientific["method_order_by_sample"][sample])
        try:
            status = read_status(out_dir, sample, methods)
            state = status["state"]
        except Exception as exc:
            status = {"state": "worker_failed", "status_error": str(exc)}
            state = "worker_failed"
        method_rows = {}
        for method in methods:
            try:
                method_rows[method] = _method_summary(
                    out_dir, sample, method, target_rt,
                    skip=sample in skip_samples)
            except Exception as exc:
                method_rows[method] = _method_summary(
                    out_dir, sample, method, target_rt, skip=True)
                method_rows[method]["errors"] = [
                    f"method summary failed: {type(exc).__name__}: {exc}"]
        complete = state == "complete" and all(
            item["complete"] for item in method_rows.values())
        all_complete = all_complete and complete
        state_counts[state] += 1
        for item in method_rows.values():
            totals["result_rows"] += item["result_rows"]
            totals["success_journals"] += item["success_journals"]
            totals["failure_journals"] += item["failure_journals"]
            totals["retry_budget_consumed"] += item["retry_budget_consumed"]
            totals["preservation_violations"] += item["preservation_violations"]
        sample_rows[sample] = {
            "state": state, "complete": complete,
            "methods": method_rows,
            "last_error": status.get("last_error"),
            "runtime_observation": (
                "running_external" if sample in skip_samples else None),
        }
    if campaign_state_hint in {"interrupted", "incomplete"}:
        campaign_state = campaign_state_hint
    elif all_complete:
        campaign_state = "complete"
    elif state_counts.get("running"):
        campaign_state = "running"
    else:
        campaign_state = "incomplete"
    return {
        "schema": QUICK_SCHEMA, "created_at": utc_now(),
        "campaign_state": campaign_state, "sample_count": len(samples),
        "sample_state_counts": dict(sorted(state_counts.items())),
        "totals": dict(totals), "samples": sample_rows,
    }


def write_quick_summary(out_dir, campaign_state_hint=None, *, skip_samples=None):
    summary = build_quick_summary(
        out_dir, campaign_state_hint, skip_samples=skip_samples)
    write_json_atomic(
        Path(out_dir) / "reports" / "quick_summary.json", summary)
    return summary


def _compare_score(expected, actual, tolerance=1e-6):
    if expected is None or actual is None:
        return expected is actual
    return math.isclose(float(expected), float(actual), abs_tol=tolerance)


def _same_number(left, right):
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(float(left), float(right), abs_tol=1e-12)
    return left == right


def _verify_row_journals(method_path, sample, method, row, journals_by_path,
                         used_call_ids):
    issues = []
    rt = row.get("round_trip_num")
    direction = row.get("round_trip_direction")
    call_ids = row.get("api_call_ids")
    raw_paths = row.get("api_raw_paths")
    if (not isinstance(call_ids, list) or not call_ids
            or not all(isinstance(item, str) and item for item in call_ids)
            or not isinstance(raw_paths, list) or len(raw_paths) != len(call_ids)
            or not all(isinstance(item, str) and item for item in raw_paths)):
        return [f"{sample}/{method}/RT{rt}/{direction}: invalid API journal links"]
    payloads = []
    for call_id, raw_path in zip(call_ids, raw_paths):
        relative = Path(raw_path)
        expected_prefix = Path("calls") / f"rt{rt:02d}" / direction
        if (relative.is_absolute() or ".." in relative.parts
                or relative.name != "success.json"
                or relative.parent.parent != expected_prefix
                or relative.parent.name not in {"primary", "repair"}):
            issues.append(
                f"{sample}/{method}/RT{rt}/{direction}: "
                f"success journal path does not match the result step")
            continue
        absolute = (Path(method_path) / relative).resolve()
        payload = journals_by_path.get(absolute)
        if payload is None:
            issues.append(
                f"{sample}/{method}/RT{rt}/{direction}: "
                f"missing success journal for {call_id}")
            continue
        try:
            validate_complete_success_journal(
                payload, path=absolute, sample=sample, method=method,
                rt_num=rt, direction=direction,
                call_leaf=relative.parent.name)
        except LocalCallEvidenceError as exc:
            issues.append(
                f"{sample}/{method}/RT{rt}/{direction}: "
                f"malformed success journal: {exc}")
            continue
        if payload.get("call_id") != call_id:
            issues.append(
                f"{sample}/{method}/RT{rt}/{direction}: call_id/path mismatch")
            continue
        payloads.append(payload)
        used_call_ids.add(call_id)
    if len(payloads) != len(call_ids):
        return issues

    responses = [payload["response"] for payload in payloads]
    call_kinds = [payload["request"]["parameters"]["call_kind"]
                  for payload in payloads]
    combined_attempts = [
        attempt for response in responses
        for attempt in response.get("transport_attempts") or []]
    if row.get("call_kinds") != call_kinds:
        issues.append(
            f"{sample}/{method}/RT{rt}/{direction}: call kind linkage mismatch")
    if row.get("api_transport_attempts") != combined_attempts:
        issues.append(
            f"{sample}/{method}/RT{rt}/{direction}: transport attempt linkage mismatch")
    if row.get("raw_llm_response") not in {
            response.get("message") for response in responses}:
        issues.append(
            f"{sample}/{method}/RT{rt}/{direction}: raw response/journal mismatch")
    for key in ("finish_reason", "response_classification", "transport",
                "transport_revision", "reasoning_effort"):
        if row.get(key) != responses[0].get(key):
            issues.append(
                f"{sample}/{method}/RT{rt}/{direction}: {key} linkage mismatch")
    numeric_fields = (
        "prompt_tokens", "completion_tokens", "total_tokens", "total_usd",
        "total_cny", "elapsed_time", "retry_count", "failed_attempt_count",
        "quota_wait_count", "rate_limit_wait_count", "transient_wait_count",
        "input_tokens", "output_tokens", "cache_read_input_tokens",
        "cache_creation_input_tokens", "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens")
    for key in numeric_fields:
        values = [response.get(key) for response in responses]
        if not any(value is not None for value in values):
            continue
        expected = sum((value or 0) for value in values)
        if not _same_number(row.get(key), expected):
            issues.append(
                f"{sample}/{method}/RT{rt}/{direction}: {key} linkage mismatch")
    return issues


def _verify_method_replay(out_dir, scientific, sample, method, task_plan):
    target_rt = int(scientific["round_trips"])
    rows = read_result_rows(method_dir(out_dir, sample, method) / "result.jsonl")
    samples_root = Path(scientific["samples_root"])
    if not samples_root.is_absolute():
        samples_root = Path(__file__).resolve().parent.parent / samples_root
    try:
        by_rt, complete, expected_state_chain = validate_result_trajectory(
            rows, sample, method, target_rt, task_plan, samples_root)
    except Exception as exc:
        return [f"{sample}/{method}: task-plan trajectory invalid: {exc}"]
    issues = []
    if complete != target_rt:
        return [f"{sample}/{method}: only {complete}/{target_rt} RT committed"]
    random.seed(int(scientific["seed"]))
    loaded, sample_folder, states = load_sample(
        sample, samples_folder=str(samples_root) + os.sep)
    initial_id = loaded["start_state"]
    initial = states[initial_id]
    distractor = (
        load_distractor_context(sample_folder)
        if scientific["include_distractor"] else {})
    context = build_context_from_folder(
        os.path.join(sample_folder, initial["solution_folder"]))
    if scientific["include_distractor"]:
        context = merge_distractor(context, distractor)
    context = shuffle_context(context)
    domain = get_domain(loaded["sample_type"])
    domain.samples_folder = str(samples_root) + os.sep
    for rt in range(1, target_rt + 1):
        for direction in ("forward", "backward"):
            row = by_rt[rt][direction]
            target = states[row["target_state_id"]]
            try:
                generated = _replay_generated(
                    method, row, context, distractor, target)
                evaluation = _evaluate(
                    domain, sample, generated, target, list(target["context"]))
            except Exception as exc:
                issues.append(
                    f"{sample}/{method}/RT{rt}/{direction}: replay failed: {exc}")
                return issues
            stored = (row.get("evaluation") or {}).get("score")
            replayed = evaluation.get("score")
            if not _compare_score(stored, replayed):
                issues.append(
                    f"{sample}/{method}/RT{rt}/{direction}: "
                    f"score mismatch stored={stored} replayed={replayed}")
            context = shuffle_context(merge_distractor(generated, distractor))
    checkpoint = _checkpoint(
        method_dir(out_dir, sample, method) / "checkpoint.json")
    final_row = by_rt[target_rt]["backward"]
    if isinstance(checkpoint, dict):
        try:
            validate_checkpoint_state(
                checkpoint, complete, by_rt, expected_state_chain)
        except Exception as exc:
            issues.append(f"{sample}/{method}: checkpoint invalid: {exc}")
        if checkpoint.get("current_context") != context:
            issues.append(f"{sample}/{method}: checkpoint context mismatch")
        if checkpoint.get("rid_chain") != final_row.get("rid_chain"):
            issues.append(f"{sample}/{method}: checkpoint rid_chain mismatch")
        if checkpoint.get("state_chain") != final_row.get("state_chain"):
            issues.append(f"{sample}/{method}: checkpoint state_chain mismatch")
    return issues


def verify_full(out_dir):
    """Validate evidence without changing results, checkpoints, calls or status."""
    out_dir = Path(out_dir).resolve()
    run = read_json(out_dir / "run.json")
    scientific = run["scientific"]
    quick = build_quick_summary(out_dir)
    issues = []
    success_ids = {}
    task_plans = {}
    for sample in scientific["samples"]:
        try:
            task_plan, _payload = load_task_plan(out_dir, sample)
            task_plans[sample] = task_plan
        except Exception as exc:
            issues.append(f"{sample}: task plan unreadable: {exc}")
    for sample in scientific["samples"]:
        methods = scientific["method_order_by_sample"][sample]
        for method in methods:
            path = method_dir(out_dir, sample, method)
            method_quick = quick["samples"][sample]["methods"][method]
            if not method_quick["complete"]:
                issues.append(
                    f"{sample}/{method}: incomplete endpoint "
                    f"{method_quick['committed_round_trips']}/"
                    f"{scientific['round_trips']} RT")
            successes, _failures = scan_call_journals(path)
            journals_by_path = {}
            method_success_ids = set()
            for payload in successes:
                call_id = payload.get("call_id")
                journal_path = Path(payload.get("_path") or "").resolve()
                try:
                    validate_complete_success_journal(
                        payload, path=journal_path,
                        sample=sample, method=method)
                except Exception as exc:
                    issues.append(
                        f"{sample}/{method}: malformed success journal "
                        f"{payload.get('_path')}: {exc}")
                    continue
                if not call_id or call_id in success_ids:
                    issues.append(
                        f"{sample}/{method}: duplicate or missing success call_id")
                else:
                    success_ids[call_id] = payload.get("_path")
                    method_success_ids.add(call_id)
                    journals_by_path[journal_path] = payload
            try:
                rows = read_result_rows(path / "result.jsonl")
            except Exception as exc:
                issues.append(f"{sample}/{method}: {exc}")
                continue
            for row in rows:
                if not isinstance(row, dict):
                    issues.append(f"{sample}/{method}: result row is not an object")
                    continue
                issues.extend(_verify_row_journals(
                    path, sample, method, row, journals_by_path,
                    used_call_ids := set()))
                method_success_ids -= used_call_ids
            for call_id in sorted(method_success_ids):
                issues.append(
                    f"{sample}/{method}: orphan success journal {call_id}")
            if method_quick["complete"]:
                if sample in task_plans:
                    issues.extend(_verify_method_replay(
                        out_dir, scientific, sample, method,
                        task_plans[sample]))
    paired_missing = []
    endpoint_scores = {"hybridpatch": [], "fullrewrite": []}
    paired_deltas = []
    for sample in scientific["samples"]:
        methods = scientific["method_order_by_sample"][sample]
        scores = {}
        for method in methods:
            value = quick["samples"][sample]["methods"][method].get(
                "final_backward_score")
            if value is not None:
                endpoint_scores[method].append(value)
                scores[method] = value
        if set(methods) == {"hybridpatch", "fullrewrite"}:
            incomplete = [
                method for method in methods
                if not quick["samples"][sample]["methods"][method]["complete"]]
            if incomplete:
                paired_missing.append({"sample": sample, "methods": incomplete})
            elif set(scores) == {"hybridpatch", "fullrewrite"}:
                paired_deltas.append(
                    scores["hybridpatch"] - scores["fullrewrite"])
    analysis = {
        "endpoint_count": {
            method: len(values) for method, values in endpoint_scores.items()},
        "endpoint_mean": {
            method: (sum(values) / len(values) if values else None)
            for method, values in endpoint_scores.items()},
        "paired_count": len(paired_deltas),
        "paired_mean_delta_hp_minus_fr": (
            sum(paired_deltas) / len(paired_deltas)
            if paired_deltas else None),
    }
    return {
        "schema": VERIFY_SCHEMA, "created_at": utc_now(),
        "ok": not issues and not paired_missing,
        "issues": issues, "paired_endpoint_missing": paired_missing,
        "quick_summary": quick, "analysis": analysis,
        "recommend_rerun_samples": sorted({
            item.split("/", 1)[0] for item in issues
            if "/" in item} | {item["sample"] for item in paired_missing}),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args(argv)
    if args.full:
        report = verify_full(args.dir)
        write_json_atomic(
            Path(args.dir) / "reports" / "verification.json", report)
        print(json.dumps({
            "ok": report["ok"], "issue_count": len(report["issues"]),
            "paired_missing_count": len(report["paired_endpoint_missing"]),
        }, ensure_ascii=False))
        return 0 if report["ok"] else 2
    summary = write_quick_summary(args.dir)
    print(json.dumps({
        "campaign_state": summary["campaign_state"],
        "sample_state_counts": summary["sample_state_counts"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
