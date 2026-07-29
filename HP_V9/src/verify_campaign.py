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
from simple_api_recorder import scan_call_journals
from simple_runtime_io import (LocalEvidenceError, _replay_generated,
                               method_dir, read_json, read_result_rows,
                               read_status, utc_now,
                               sha256_json, validate_result_pairs,
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
    evaluation = row.get("evaluation") or {}
    value = evaluation.get("score")
    return float(value) if isinstance(value, (int, float)) else None


def _method_summary(out_dir, sample, method, target_rt):
    path = method_dir(out_dir, sample, method)
    errors = []
    try:
        rows = read_result_rows(path / "result.jsonl")
        _by_rt, complete = validate_result_pairs(rows, target_rt)
    except Exception as exc:
        rows, complete = [], 0
        errors.append(str(exc))
    checkpoint = _checkpoint(path / "checkpoint.json")
    checkpoint_rt = (
        checkpoint.get("completed_round_trips")
        if isinstance(checkpoint, dict) else None)
    if checkpoint_rt != complete:
        errors.append(
            f"checkpoint/result mismatch: checkpoint={checkpoint_rt}, pairs={complete}")
    successes, failures = scan_call_journals(path)
    failure_types = Counter()
    http_statuses = Counter()
    retry_consumed = 0
    for failure in failures:
        details = failure.get("failure") or {}
        failure_types[str(details.get("provider_error_type")
                          or details.get("error_type") or "unknown")] += 1
        if details.get("http_status") is not None:
            http_statuses[str(details["http_status"])] += 1
        for attempt in details.get("transport_attempts") or []:
            retry_consumed += int(bool(attempt.get("retry_budget_consumed")))
    for success in successes:
        response = success.get("response") or {}
        for attempt in response.get("transport_attempts") or []:
            status = attempt.get("http_status")
            if status is not None:
                http_statuses[str(status)] += 1
            retry_consumed += int(bool(attempt.get("retry_budget_consumed")))
    backward_scores = [
        _score(row) for row in rows
        if row.get("round_trip_direction") == "backward"
        and _score(row) is not None]
    preservation = sum(
        int(((row.get("bdpatch") or {}).get("preservation_violations") or 0))
        for row in rows)
    return {
        "method": method, "result_rows": len(rows),
        "committed_round_trips": complete, "checkpoint_round_trips": checkpoint_rt,
        "complete": complete == target_rt and checkpoint_rt == target_rt,
        "success_journals": len(successes), "failure_journals": len(failures),
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
        "errors": errors,
    }


def build_quick_summary(out_dir, campaign_state_hint=None):
    out_dir = Path(out_dir).resolve()
    run = read_json(out_dir / "run.json")
    scientific = run["scientific"]
    target_rt = int(scientific["round_trips"])
    samples = list(scientific["samples"])
    sample_rows = {}
    state_counts = Counter()
    totals = Counter()
    all_complete = True
    for sample in samples:
        methods = list(scientific["method_order_by_sample"][sample])
        try:
            status = read_status(out_dir, sample, methods)
            state = status["state"]
        except Exception as exc:
            status = {"state": "worker_failed", "status_error": str(exc)}
            state = "worker_failed"
        method_rows = {
            method: _method_summary(out_dir, sample, method, target_rt)
            for method in methods}
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


def write_quick_summary(out_dir, campaign_state_hint=None):
    summary = build_quick_summary(out_dir, campaign_state_hint)
    write_json_atomic(
        Path(out_dir) / "reports" / "quick_summary.json", summary)
    return summary


def _compare_score(expected, actual, tolerance=1e-6):
    if expected is None or actual is None:
        return expected is actual
    return math.isclose(float(expected), float(actual), abs_tol=tolerance)


def _verify_method_replay(out_dir, scientific, sample, method):
    target_rt = int(scientific["round_trips"])
    rows = read_result_rows(method_dir(out_dir, sample, method) / "result.jsonl")
    by_rt, complete = validate_result_pairs(rows, target_rt)
    issues = []
    if complete != target_rt:
        return [f"{sample}/{method}: only {complete}/{target_rt} RT committed"]
    random.seed(int(scientific["seed"]))
    samples_root = Path(scientific["samples_root"])
    if not samples_root.is_absolute():
        samples_root = Path(__file__).resolve().parent.parent / samples_root
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
    result_links = []
    for sample, record in (scientific.get("task_plans") or {}).items():
        try:
            payload = read_json(out_dir / record["path"])
        except Exception as exc:
            issues.append(f"{sample}: task plan unreadable: {exc}")
            continue
        if sha256_json(payload) != record.get("sha256"):
            issues.append(f"{sample}: task plan SHA mismatch")
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
            for payload in successes:
                call_id = payload.get("call_id")
                if (payload.get("schema") != "anchorpatch.simple_api_success/1"
                        or not isinstance(payload.get("request_sha256"), str)
                        or len(payload.get("request_sha256")) != 64
                        or sha256_json(payload.get("request"))
                        != payload.get("request_sha256")
                        or not isinstance(payload.get("response"), dict)):
                    issues.append(
                        f"{sample}/{method}: malformed success journal "
                        f"{payload.get('_path')}")
                if not call_id or call_id in success_ids:
                    issues.append(
                        f"{sample}/{method}: duplicate or missing success call_id")
                else:
                    success_ids[call_id] = payload.get("_path")
            try:
                rows = read_result_rows(path / "result.jsonl")
            except Exception as exc:
                issues.append(f"{sample}/{method}: {exc}")
                continue
            for row in rows:
                for call_id in row.get("api_call_ids") or []:
                    result_links.append((sample, method, row.get("round_trip_num"), call_id))
            if method_quick["complete"]:
                issues.extend(_verify_method_replay(
                    out_dir, scientific, sample, method))
    for sample, method, rt, call_id in result_links:
        if call_id not in success_ids:
            issues.append(
                f"{sample}/{method}/RT{rt}: missing success journal for {call_id}")
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
