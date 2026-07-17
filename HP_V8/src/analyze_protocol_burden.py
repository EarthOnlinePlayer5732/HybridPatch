"""Zero-API V7 envelope and V7/V8 prompt-burden analysis.

The tool reads the frozen V7 dev20 archive, never calls a model/provider, and
writes deterministic JSON and Markdown reports under HP_V8/analysis.

Run from HP_V8:
  python -B ./src/analyze_protocol_burden.py
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import os
from pathlib import Path
import statistics
import sys


HERE = Path(__file__).resolve().parent
HP_ROOT = HERE.parent
REPO_ROOT = HP_ROOT.parent
for item in (str(HP_ROOT), str(HERE)):
    if item not in sys.path:
        sys.path.insert(0, item)

from hybrid_prompt import (  # noqa: E402
    build_hybrid_prompt,
    build_hybrid_repair_prompt,
    classify_operation_family,
    extract_hybrid_json,
)
from hybrid_schema import (  # noqa: E402
    PROTOCOL_BURDEN_LIMITS,
    PROTOCOL_V7,
    measure_protocol_burden,
    route_of,
)
from utils_context import parse_context_string  # noqa: E402
from utils_env import load_sample  # noqa: E402


REPORT_SCHEMA = "hybridpatch.protocol_burden_v7_v8/1"
DEFAULT_ARCHIVE = REPO_ROOT / "HP_V7" / "exp_20260711_hybridv7dev20full"
DEFAULT_OUTPUT_DIR = HP_ROOT / "analysis"
EXPECTED = {
    "total_rows": 400,
    "success_rows": 394,
    "kept_rows": 6,
    "partial_acceptance_rows": 5,
    "repair_attempt_rows": 36,
    "route_distribution": {
        "bounded_rewrite": 233,
        "bulk_patch": 44,
        "dsl_rules": 1,
        "local_patch": 116,
    },
    "conditional_p95": {
        "local_op_count": 31,
        "bulk_op_count": 30,
        "anchor_bytes": 4080,
        "envelope_bytes": 2898,
        "explicit_block_id_count": 39,
    },
    "threshold_union_exceeded": 25,
}
LIMITATIONS = [
    "Prompt and envelope character counts are not tokenizer-measured tokens.",
    "The source is one frozen dev20 campaign: 20 samples, one model, one seed, and sequential round trips; observations are not independent.",
    "The burden distribution selects finally chosen and committed envelopes, so it has survivorship bias and cannot establish that high burden causes protocol failure.",
    "Chosen attempts mix primary and repair outputs; the distribution is not a distribution of all model responses.",
    "Routes are highly imbalanced and DSL has only one successful envelope, so its 39-ID ceiling is provisional rather than statistically stable.",
    "Canonical envelope size excludes sidecar FILE BODIES and therefore is not total completion size.",
    "The zero-API replay cannot demonstrate score retention, token reduction, or protocol-failure-rate improvement; those require a controlled API experiment.",
]


def _read_json(path: Path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _load_rows(archive: Path):
    rows = []
    result_dir = archive / "hybridpatch"
    for path in sorted(result_dir.glob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for lineno, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except Exception as exc:
                    raise RuntimeError(f"invalid JSON at {path}:{lineno}") from exc
                row["_source_result"] = str(path.relative_to(REPO_ROOT)).replace(os.sep, "/")
                rows.append(row)
    rows.sort(key=lambda row: (
        str(row.get("sample_id")), int(row.get("round_trip_num") or 0),
        0 if row.get("round_trip_direction") == "forward" else 1,
    ))
    return rows


def _request_path(archive: Path, row, call_index: int):
    call_ids = list(row.get("api_call_ids") or [])
    if call_index >= len(call_ids):
        raise RuntimeError(
            f"missing call {call_index} for {row.get('sample_id')} "
            f"RT{row.get('round_trip_num')} {row.get('round_trip_direction')}"
        )
    name = (
        f"rt{int(row['round_trip_num']):02d}_{row['round_trip_direction']}_"
        f"{call_ids[call_index]}.request.json"
    )
    path = archive / "api_raw" / "hybridpatch" / str(row["sample_id"]) / name
    if not path.is_file():
        raise RuntimeError(f"archived relative request is missing: {path}")
    return path


def _request_prompt(path: Path):
    payload = _read_json(path)
    messages = payload.get("request_messages")
    if not isinstance(messages, list) or not messages:
        raise RuntimeError(f"request_messages missing from {path}")
    content = messages[0].get("content") if isinstance(messages[0], dict) else None
    if not isinstance(content, str):
        raise RuntimeError(f"request prompt missing from {path}")
    return content


def _archived_contexts(prompt: str):
    editable_marker = "\n[EDITABLE DOCUMENTS]\n"
    readonly_marker = "\n[READ-ONLY CONTEXT - never output these files]\n"
    file_index_marker = "\n[FILE INDEX]\n"
    editable_start = prompt.find(editable_marker)
    file_index = prompt.rfind(file_index_marker)
    if editable_start < 0 or file_index < 0 or file_index <= editable_start:
        raise RuntimeError("archived V7 prompt lacks editable/file-index boundaries")
    editable_start += len(editable_marker)
    readonly_at = prompt.rfind(readonly_marker, editable_start, file_index)
    editable_end = readonly_at if readonly_at >= 0 else file_index
    editable = parse_context_string(prompt[editable_start:editable_end].strip())
    if not editable:
        raise RuntimeError("archived V7 prompt yielded no editable documents")
    readonly = {}
    if readonly_at >= 0:
        readonly_start = readonly_at + len(readonly_marker)
        readonly = parse_context_string(prompt[readonly_start:file_index].strip())
    return editable, readonly


def _targets_for_row(row, cache):
    sample_id = str(row["sample_id"])
    if sample_id not in cache:
        _sample, _folder, id2state = load_sample(
            sample_id,
            samples_folder=str(HP_ROOT / "data" / "samples_delegate52") + os.sep,
        )
        cache[sample_id] = id2state
    state = cache[sample_id].get(row.get("target_state_id"))
    if not isinstance(state, dict) or not isinstance(state.get("context"), list):
        raise RuntimeError(
            f"target state missing for {sample_id}:{row.get('target_state_id')}"
        )
    return list(state["context"])


def _nearest_rank(values, percentile):
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil((percentile / 100.0) * len(ordered)))
    return ordered[rank - 1]


def _distribution(values):
    values = list(values)
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "min": min(values),
        "p50": _nearest_rank(values, 50),
        "p75": _nearest_rank(values, 75),
        "p90": _nearest_rank(values, 90),
        "p95": _nearest_rank(values, 95),
        "p99": _nearest_rank(values, 99),
        "max": max(values),
        "mean": round(statistics.fmean(values), 3),
    }


def _comparison(v7_values, v8_values):
    if len(v7_values) != len(v8_values):
        raise RuntimeError("paired prompt arrays differ in length")
    deltas = [new - old for old, new in zip(v7_values, v8_values)]
    old_total = sum(v7_values)
    new_total = sum(v8_values)
    return {
        "n": len(v7_values),
        "v7_chars": _distribution(v7_values),
        "v8_chars": _distribution(v8_values),
        "paired_delta_chars": _distribution(deltas),
        "total_v7_chars": old_total,
        "total_v8_chars": new_total,
        "total_delta_chars": new_total - old_total,
        "relative_total_change_percent": (
            round(100.0 * (new_total - old_total) / old_total, 3)
            if old_total else None
        ),
        "v8_shorter_count": sum(new < old for old, new in zip(v7_values, v8_values)),
        "equal_count": sum(new == old for old, new in zip(v7_values, v8_values)),
    }


def _chosen_success(row):
    bd = row.get("bdpatch") or {}
    return bd.get("actual_method") == "hybridpatch"


def _extract_chosen(row):
    envelope, meta = extract_hybrid_json(row.get("raw_llm_response") or "")
    if envelope is None:
        raise RuntimeError(
            f"chosen committed envelope cannot be parsed: {row.get('sample_id')} "
            f"RT{row.get('round_trip_num')} {row.get('round_trip_direction')}"
        )
    if envelope.get("protocol") != PROTOCOL_V7:
        raise RuntimeError(f"success subset contains non-V7 protocol: {envelope.get('protocol')!r}")
    return envelope, meta.get("bodies") or {}


def _verify_known(report):
    source = report["source"]
    for key in ("total_rows", "success_rows", "kept_rows",
                "partial_acceptance_rows", "repair_attempt_rows"):
        if source[key] != EXPECTED[key]:
            raise RuntimeError(f"known-data check failed for {key}: {source[key]} != {EXPECTED[key]}")
    if report["success_subset"]["route_distribution"] != EXPECTED["route_distribution"]:
        raise RuntimeError("known-data route distribution mismatch")
    for metric, expected in EXPECTED["conditional_p95"].items():
        actual = report["burden_distributions"][metric].get("p95")
        if actual != expected:
            raise RuntimeError(f"known-data P95 mismatch for {metric}: {actual} != {expected}")
    actual_union = report["threshold_coverage"]["union_exceeded"]
    if actual_union != EXPECTED["threshold_union_exceeded"]:
        raise RuntimeError(
            f"known-data threshold union mismatch: {actual_union} != "
            f"{EXPECTED['threshold_union_exceeded']}"
        )


def analyze(archive: Path):
    rows = _load_rows(archive)
    success_rows = [row for row in rows if _chosen_success(row)]
    kept_rows = [row for row in rows if not _chosen_success(row)]
    repair_rows = [row for row in rows if (((row.get("bdpatch") or {}).get("hybrid") or {})
                                           .get("repair") or {}).get("attempted")]
    partial_rows = [row for row in success_rows if (((row.get("bdpatch") or {})
                                                     .get("hybrid") or {})
                                                    .get("partial_acceptance"))]

    measurements = []
    route_distribution = collections.Counter()
    for row in success_rows:
        envelope, bodies = _extract_chosen(row)
        burden = measure_protocol_burden(envelope, bodies=bodies)
        route = route_of(envelope)
        route_distribution[route] += 1
        measurements.append({
            "sample_id": row["sample_id"],
            "round_trip_num": row["round_trip_num"],
            "round_trip_direction": row["round_trip_direction"],
            "route": route,
            **burden,
        })

    conditional = {
        "local_op_count": [m["local_op_count"] for m in measurements
                           if m["route"] == "local_patch"],
        "bulk_op_count": [m["bulk_op_count"] for m in measurements
                          if m["route"] == "bulk_patch"],
        "anchor_bytes": [m["anchor_bytes"] for m in measurements
                         if m["route"] in ("local_patch", "bulk_patch")],
        "envelope_bytes": [m["envelope_bytes"] for m in measurements],
        "envelope_chars": [m["envelope_chars"] for m in measurements],
        "explicit_block_id_count": [m["explicit_block_id_count"] for m in measurements
                                    if m["route"] == "dsl_rules"],
    }
    burden_distributions = {
        key: _distribution(values) for key, values in conditional.items()
    }

    per_metric_exceeded = {
        key: sum(m[key] > limit for m in measurements)
        for key, limit in PROTOCOL_BURDEN_LIMITS.items()
    }
    union_exceeded_rows = [
        m for m in measurements
        if any(m[key] > limit for key, limit in PROTOCOL_BURDEN_LIMITS.items())
    ]
    threshold_coverage = {
        "limits": dict(PROTOCOL_BURDEN_LIMITS),
        "comparison": "value <= limit passes; value > limit is rejected",
        "per_metric_exceeded": per_metric_exceeded,
        "union_exceeded": len(union_exceeded_rows),
        "union_covered": len(measurements) - len(union_exceeded_rows),
        "union_exceeded_by_route": dict(sorted(collections.Counter(
            row["route"] for row in union_exceeded_rows).items())),
        "union_coverage_percent": round(
            100.0 * (len(measurements) - len(union_exceeded_rows)) / len(measurements), 2
        ),
        "union_exceeded_percent": round(
            100.0 * len(union_exceeded_rows) / len(measurements), 2
        ),
        "exceeded_rows": union_exceeded_rows,
    }

    state_cache = {}
    v7_primary_chars = []
    v8_primary_chars = []
    v7_repair_chars = []
    v8_repair_chars = []
    profile_distribution = collections.Counter()
    family_distribution = collections.Counter()
    request_paths = []
    for row in rows:
        primary_path = _request_path(archive, row, 0)
        primary_prompt = _request_prompt(primary_path)
        request_paths.append(str(primary_path.relative_to(archive)).replace(os.sep, "/"))
        editable, readonly = _archived_contexts(primary_prompt)
        targets = _targets_for_row(row, state_cache)
        instruction = row.get("edit_instruction") or ""
        classification = classify_operation_family(instruction)
        v8_prompt = build_hybrid_prompt(
            editable, instruction, targets, readonly_context=readonly or None,
            prompt_classification=classification,
        )
        v7_primary_chars.append(len(primary_prompt))
        v8_primary_chars.append(len(v8_prompt))
        profile_distribution[classification["prompt_profile"]] += 1
        family_distribution[classification["operation_family"]] += 1

        repair = (((row.get("bdpatch") or {}).get("hybrid") or {}).get("repair") or {})
        if repair.get("attempted"):
            repair_path = _request_path(archive, row, 1)
            actual_repair_prompt = _request_prompt(repair_path)
            request_paths.append(str(repair_path.relative_to(archive)).replace(os.sep, "/"))
            previous, _meta = extract_hybrid_json(repair.get("original_raw") or "")
            action = previous.get("action") if isinstance(previous, dict) else None
            route = action.get("route") if isinstance(action, dict) else None
            counterfactual = build_hybrid_repair_prompt(
                repair.get("repair_errors") or [], previous_envelope=previous,
                editable_context=editable, edit_instruction=instruction,
                target_filenames=targets, readonly_filenames=sorted(readonly),
                prompt_classification=classification, current_route=route,
            )
            v7_repair_chars.append(len(actual_repair_prompt))
            v8_repair_chars.append(len(counterfactual))

    if len(set(request_paths)) != len(request_paths):
        raise RuntimeError("the same archived request was assigned to multiple semantic calls")

    report = {
        "schema": REPORT_SCHEMA,
        "analysis_mode": "zero_api",
        "source": {
            "archive": str(archive.relative_to(REPO_ROOT)).replace(os.sep, "/"),
            "success_definition": "bdpatch.actual_method == hybridpatch",
            "total_rows": len(rows),
            "success_rows": len(success_rows),
            "kept_rows": len(kept_rows),
            "partial_acceptance_rows": len(partial_rows),
            "repair_attempt_rows": len(repair_rows),
            "relative_request_files_read": len(request_paths),
            "request_path_policy": "archive-relative api_raw paths; stale stored absolute paths are not opened",
        },
        "success_subset": {
            "protocol": PROTOCOL_V7,
            "includes_partial_acceptance": True,
            "route_distribution": dict(sorted(route_distribution.items())),
        },
        "quantile_method": "nearest-rank: sorted_values[ceil(p*n)-1]",
        "burden_distributions": burden_distributions,
        "threshold_coverage": threshold_coverage,
        "prompt_comparison": {
            "primary_all_400_paired": _comparison(v7_primary_chars, v8_primary_chars),
            "repair_actual_v7_vs_counterfactual_v8": _comparison(
                v7_repair_chars, v8_repair_chars),
            "v8_prompt_profile_distribution": dict(sorted(profile_distribution.items())),
            "v8_operation_family_distribution": dict(sorted(family_distribution.items())),
            "interpretation": "character-count comparison only; no provider or tokenizer was called",
        },
        "limitations": LIMITATIONS,
    }
    _verify_known(report)
    return report


def _markdown(report):
    source = report["source"]
    routes = report["success_subset"]["route_distribution"]
    burden = report["burden_distributions"]
    coverage = report["threshold_coverage"]
    primary = report["prompt_comparison"]["primary_all_400_paired"]
    repair = report["prompt_comparison"]["repair_actual_v7_vs_counterfactual_v8"]
    lines = [
        "# HP_V8 零 API 协议与提示负担报告",
        "",
        "本报告只做确定性离线重算与提示重放；未调用任何模型或 provider。",
        "",
        "## 数据口径",
        "",
        f"- 来源：`{source['archive']}`。",
        f"- 总行数：{source['total_rows']}；最终提交 HybridPatch：{source['success_rows']}；kept-context：{source['kept_rows']}。",
        f"- 成功子集包含 {source['partial_acceptance_rows']} 个 V7 partial acceptance；repair 尝试共 {source['repair_attempt_rows']} 步。",
        "- 成功定义：`bdpatch.actual_method == \"hybridpatch\"`，并逐条断言 chosen envelope 为 `hybridpatch/7`。",
        f"- route：bounded_rewrite={routes.get('bounded_rewrite', 0)}，local_patch={routes.get('local_patch', 0)}，bulk_patch={routes.get('bulk_patch', 0)}，dsl_rules={routes.get('dsl_rules', 0)}。",
        "- 分位数：nearest-rank，`sorted_values[ceil(p*n)-1]`。",
        "",
        "## V7 成功信封分布与冻结阈值",
        "",
        "| 指标 | n | p50 | p90 | p95 | p99 | max | V8 上限 | 超限行数 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for metric in ("local_op_count", "bulk_op_count", "anchor_bytes",
                   "envelope_bytes", "explicit_block_id_count"):
        row = burden[metric]
        lines.append(
            f"| `{metric}` | {row.get('n')} | {row.get('p50')} | {row.get('p90')} | "
            f"{row.get('p95')} | {row.get('p99')} | {row.get('max')} | "
            f"{coverage['limits'][metric]} | {coverage['per_metric_exceeded'][metric]} |"
        )
    lines += [
        "",
        f"五项阈值的并集会要求 {coverage['union_exceeded']}/{source['success_rows']} "
        f"（{coverage['union_exceeded_percent']:.2f}%）个历史成功形态合并重复操作或改用更合适路径；"
        f"阈值等值放行，只有严格大于才拒绝。",
        "",
        "## V7/V8 提示字符数",
        "",
        "| 比较 | n | V7 mean | V7 p50 | V7 p95 | V8 mean | V8 p50 | V8 p95 | 总变化 | 相对变化 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        (
            f"| primary 全量配对 | {primary['n']} | {primary['v7_chars']['mean']} | "
            f"{primary['v7_chars']['p50']} | {primary['v7_chars']['p95']} | "
            f"{primary['v8_chars']['mean']} | {primary['v8_chars']['p50']} | "
            f"{primary['v8_chars']['p95']} | {primary['total_delta_chars']} | "
            f"{primary['relative_total_change_percent']}% |"
        ),
        (
            f"| actual V7 repair vs counterfactual V8 repair | {repair['n']} | "
            f"{repair['v7_chars']['mean']} | {repair['v7_chars']['p50']} | "
            f"{repair['v7_chars']['p95']} | {repair['v8_chars']['mean']} | "
            f"{repair['v8_chars']['p50']} | {repair['v8_chars']['p95']} | "
            f"{repair['total_delta_chars']} | {repair['relative_total_change_percent']}% |"
        ),
        "",
        "V7 长度来自归档中的实际相对 `*.request.json`；V8 primary 使用同一 400 步的任务、"
        "editable、readonly 和 target 上下文离线重建。repair 比较只覆盖 V7 实际发生 repair 的步骤。",
        "",
        "## 局限",
        "",
    ]
    lines.extend(f"- {item}" for item in report["limitations"])
    lines.append("")
    return "\n".join(lines)


def write_report(report, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "protocol_burden_v7_v8.json"
    md_path = output_dir / "protocol_burden_v7_v8.md"
    json_text = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    md_text = _markdown(report)
    json_path.write_text(json_text, encoding="utf-8")
    md_path.write_text(md_text, encoding="utf-8")
    return json_path, md_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    archive = args.archive.resolve()
    output_dir = args.output_dir.resolve()
    report = analyze(archive)
    json_path, md_path = write_report(report, output_dir)
    primary = report["prompt_comparison"]["primary_all_400_paired"]
    repair = report["prompt_comparison"]["repair_actual_v7_vs_counterfactual_v8"]
    print(
        "RESULT: PASS zero_api "
        f"rows={report['source']['total_rows']} success={report['source']['success_rows']} "
        f"union_exceeded={report['threshold_coverage']['union_exceeded']} "
        f"primary_v7_mean={primary['v7_chars']['mean']} "
        f"primary_v8_mean={primary['v8_chars']['mean']} "
        f"repair_v7_mean={repair['v7_chars']['mean']} "
        f"repair_v8_mean={repair['v8_chars']['mean']}"
    )
    print(f"JSON: {json_path}")
    print(f"MARKDOWN: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
