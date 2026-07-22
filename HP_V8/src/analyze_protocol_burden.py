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
    _routes_for_profile,
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


REPORT_SCHEMA = "hybridpatch.protocol_burden_v7_v8/3"
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
    "prompt_profile_distribution": {
        "block_movement": 216,
        "default": 184,
    },
    "path_compatibility": {
        "chosen_route_available_steps": 399,
        "chosen_route_unavailable_steps": 1,
        "compatible_steps": 398,
        "incompatible_steps": 1,
        "success_incompatible_steps": 1,
    },
}
LIMITATIONS = [
    "Prompt and envelope character counts are not tokenizer-measured tokens.",
    "The source is one frozen dev20 campaign: 20 samples, one model, one seed, and sequential round trips; observations are not independent.",
    "The burden distribution selects finally chosen and committed envelopes, so it has survivorship bias and cannot establish that high burden causes protocol failure.",
    "Chosen attempts mix primary and repair outputs; the distribution is not a distribution of all model responses.",
    "Routes are highly imbalanced and DSL has only one successful envelope, so its 39-ID ceiling is provisional rather than statistically stable.",
    "Canonical envelope size excludes sidecar FILE BODIES and therefore is not total completion size.",
    "The empirical P95 burden thresholds are soft analysis and telemetry markers only; crossing them does not reject, truncate, repair, or reroute an envelope.",
    "The path-compatibility audit only checks whether an archived V7 chosen route is listed by the counterfactual V8 prompt profile; it does not establish route equivalence or predict the route a model would choose under V8.",
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


def _chosen_route_for_path_audit(row):
    envelope, _meta = extract_hybrid_json(row.get("raw_llm_response") or "")
    parsed_route = route_of(envelope)
    telemetry_route = (((row.get("bdpatch") or {}).get("hybrid") or {}).get("route"))
    if parsed_route != telemetry_route:
        raise RuntimeError(
            "chosen route differs between raw envelope and telemetry: "
            f"{row.get('sample_id')} RT{row.get('round_trip_num')} "
            f"{row.get('round_trip_direction')}: {parsed_route!r} != {telemetry_route!r}"
        )
    return parsed_route


def _path_compatibility_summary(step_rows, profile_distribution):
    available = [row for row in step_rows if row["chosen_route_available"]]
    unavailable = [row for row in step_rows if not row["chosen_route_available"]]
    incompatible = [row for row in available if not row["compatible"]]
    compatible = [row for row in available if row["compatible"]]
    success_rows = [row for row in available if row["actual_method"] == "hybridpatch"]
    success_incompatible = [row for row in success_rows if not row["compatible"]]

    by_profile_route = {}
    for row in incompatible:
        profile_counts = by_profile_route.setdefault(row["prompt_profile"], {})
        route = row["chosen_route"]
        profile_counts[route] = profile_counts.get(route, 0) + 1
    by_profile_route = {
        profile: dict(sorted(routes.items()))
        for profile, routes in sorted(by_profile_route.items())
    }

    term_stats = {}
    representative_cases = []
    representative_terms = set()
    for row in incompatible:
        details = row["matched_term_details"] or [{
            "family": "default", "term": "<none>", "count": 1,
        }]
        for detail in details:
            key = (detail.get("family"), detail.get("term"), detail.get("role"))
            stats = term_stats.setdefault(key, {
                "family": detail.get("family"),
                "term": detail.get("term"),
                "role": detail.get("role"),
                "row_hit_count": 0,
                "occurrence_count": 0,
            })
            stats["row_hit_count"] += 1
            stats["occurrence_count"] += int(detail.get("count") or 0)
            if key not in representative_terms:
                representative_terms.add(key)
                representative_cases.append({
                    "selected_matched_term": {
                        "family": detail.get("family"),
                        "term": detail.get("term"),
                        "role": detail.get("role"),
                    },
                    "sample_id": row["sample_id"],
                    "round_trip_num": row["round_trip_num"],
                    "round_trip_direction": row["round_trip_direction"],
                    "prompt_profile": row["prompt_profile"],
                    "operation_family": row["operation_family"],
                    "matched_terms": row["matched_terms"],
                    "chosen_route": row["chosen_route"],
                    "allowed_routes": row["allowed_routes"],
                    "actual_method": row["actual_method"],
                })

    term_rows = sorted(
        term_stats.values(),
        key=lambda item: (
            -item["row_hit_count"], str(item["family"]), str(item["term"]),
            str(item.get("role") or ""),
        ),
    )
    representative_cases.sort(key=lambda item: (
        str(item["selected_matched_term"].get("family")),
        str(item["selected_matched_term"].get("term")),
        str(item["selected_matched_term"].get("role") or ""),
    ))

    total = len(step_rows)
    available_count = len(available)
    incompatible_count = len(incompatible)
    return {
        "audit_mode": "zero_api_counterfactual_route_availability",
        "profile_allowed_routes": {
            profile: list(_routes_for_profile(profile))
            for profile in sorted(profile_distribution)
        },
        "prompt_profile_distribution": dict(sorted(profile_distribution.items())),
        "total_steps": total,
        "chosen_route_available_steps": available_count,
        "chosen_route_unavailable_steps": len(unavailable),
        "compatible_steps": len(compatible),
        "incompatible_steps": incompatible_count,
        "incompatible_percent_of_available": round(
            100.0 * incompatible_count / available_count, 2) if available_count else None,
        "incompatible_percent_of_all": round(
            100.0 * incompatible_count / total, 2) if total else None,
        "success_subset": {
            "total_steps": len(success_rows),
            "compatible_steps": len(success_rows) - len(success_incompatible),
            "incompatible_steps": len(success_incompatible),
            "incompatible_percent": round(
                100.0 * len(success_incompatible) / len(success_rows), 2
            ) if success_rows else None,
        },
        "incompatible_by_route": dict(sorted(collections.Counter(
            row["chosen_route"] for row in incompatible).items())),
        "incompatible_by_profile": dict(sorted(collections.Counter(
            row["prompt_profile"] for row in incompatible).items())),
        "incompatible_by_profile_route": by_profile_route,
        "incompatible_by_sample": dict(sorted(collections.Counter(
            row["sample_id"] for row in incompatible).items())),
        "incompatible_by_round_trip": {
            str(key): value for key, value in sorted(collections.Counter(
                row["round_trip_num"] for row in incompatible).items())
        },
        "incompatible_by_direction": dict(sorted(collections.Counter(
            row["round_trip_direction"] for row in incompatible).items())),
        "incompatible_by_matched_term": term_rows,
        "representative_cases": representative_cases,
        "unavailable_cases": unavailable,
        "step_results": step_rows,
        "interpretation": (
            "Offline availability check only: whether each archived V7 chosen route "
            "appears in the allowed routes of the counterfactual V8 prompt profile. "
            "This is not an effectiveness experiment."
        ),
    }


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
    path_audit = report["path_compatibility_audit"]
    if path_audit["prompt_profile_distribution"] != EXPECTED["prompt_profile_distribution"]:
        raise RuntimeError("known-data prompt profile distribution mismatch")
    for key, expected in EXPECTED["path_compatibility"].items():
        if key == "success_incompatible_steps":
            actual = path_audit["success_subset"]["incompatible_steps"]
        else:
            actual = path_audit[key]
        if actual != expected:
            raise RuntimeError(
                f"known-data path compatibility mismatch for {key}: "
                f"{actual} != {expected}"
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
        "policy": "soft_analysis_and_telemetry_only",
        "comparison": (
            "value > empirical threshold is recorded as an overage; "
            "it has no schema, execution, repair, truncation, or routing effect"
        ),
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
    path_step_rows = []
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
        allowed_routes = list(_routes_for_profile(classification["prompt_profile"]))
        chosen_route = _chosen_route_for_path_audit(row)
        chosen_route_available = chosen_route is not None
        path_step_rows.append({
            "sample_id": row["sample_id"],
            "round_trip_num": row["round_trip_num"],
            "round_trip_direction": row["round_trip_direction"],
            "source_result": row["_source_result"],
            "actual_method": (row.get("bdpatch") or {}).get("actual_method"),
            "prompt_profile": classification["prompt_profile"],
            "operation_family": classification["operation_family"],
            "matched_families": list(classification.get("matched_families") or []),
            "matched_terms": [
                match.get("term") for match in (classification.get("matches") or [])
            ],
            "matched_term_details": [dict(match) for match in (
                classification.get("matches") or [])],
            "allowed_routes": allowed_routes,
            "chosen_route": chosen_route,
            "chosen_route_available": chosen_route_available,
            "compatible": (
                chosen_route in allowed_routes if chosen_route_available else None
            ),
        })

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

    path_compatibility = _path_compatibility_summary(
        path_step_rows, profile_distribution)

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
        "path_compatibility_audit": path_compatibility,
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
    path_audit = report["path_compatibility_audit"]
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
        "## V7 成功信封分布与经验软阈值",
        "",
        "| 指标 | n | p50 | p90 | p95 | p99 | max | V8 经验阈值 | 超阈值行数 |",
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
        f"五项经验阈值的并集标记了 {coverage['union_exceeded']}/{source['success_rows']} "
        f"（{coverage['union_exceeded_percent']:.2f}%）个历史成功信封。阈值现在只用于离线分析和 "
        "telemetry；超阈值信封仍完整执行，不触发 schema 拒绝、repair、截断或强制换路。",
        "",
        "## 零 API 路径兼容审计",
        "",
        "本节只检查历史 V7 chosen route 是否出现在同一步反事实 V8 prompt profile 的 allowed routes 中；"
        "它不判断路径语义等效，不预测模型在 V8 下会选择哪条路径，也不是效果实验。",
        "",
        f"- V8 profile 分布：default={path_audit['prompt_profile_distribution'].get('default', 0)}，"
        f"block_movement={path_audit['prompt_profile_distribution'].get('block_movement', 0)}。",
        f"- default allowed routes：{', '.join(path_audit['profile_allowed_routes']['default'])}；"
        f"block_movement allowed routes：{', '.join(path_audit['profile_allowed_routes']['block_movement'])}。",
        f"- {path_audit['chosen_route_available_steps']}/{path_audit['total_steps']} 步有可解析 chosen route；"
        f"{path_audit['chosen_route_unavailable_steps']} 步 route unavailable，单列且不进入不兼容分母。",
        f"- 不兼容：{path_audit['incompatible_steps']}/{path_audit['chosen_route_available_steps']} "
        f"（{path_audit['incompatible_percent_of_available']:.2f}%）；按全部 400 步为 "
        f"{path_audit['incompatible_percent_of_all']:.2f}%。",
        f"- 成功提交子集：{path_audit['success_subset']['incompatible_steps']}/"
        f"{path_audit['success_subset']['total_steps']} 不兼容"
        f"（{path_audit['success_subset']['incompatible_percent']:.2f}%）。",
        f"- 不兼容 route：{json.dumps(path_audit['incompatible_by_route'], ensure_ascii=False, sort_keys=True)}；"
        f"方向：{json.dumps(path_audit['incompatible_by_direction'], ensure_ascii=False, sort_keys=True)}。",
        "",
        "### 按 matched term 的不兼容计数",
        "",
        "同一步可命中多个 term，因此下表行数可重叠，不能相加作为不兼容总数。",
        "",
        "| family | matched term | role | row hits | occurrences |",
        "|---|---|---|---:|---:|",
    ]
    for item in path_audit["incompatible_by_matched_term"]:
        lines.append(
            f"| {item.get('family')} | {item.get('term')} | {item.get('role') or '-'} | "
            f"{item.get('row_hit_count')} | {item.get('occurrence_count')} |"
        )
    lines += [
        "",
        "### 代表性不兼容案例",
        "",
        "每个 matched term 选取稳定排序后的首个案例；完整 400 步逐步结果保存在 JSON 报告的 "
        "`path_compatibility_audit.step_results`。",
        "",
        "| matched term | sample | RT | direction | profile | V7 chosen route | V8 allowed routes |",
        "|---|---|---:|---|---|---|---|",
    ]
    for case in path_audit["representative_cases"]:
        selected = case["selected_matched_term"]
        term = f"{selected.get('family')}:{selected.get('term')}"
        if selected.get("role"):
            term += f" ({selected['role']})"
        lines.append(
            f"| {term} | {case['sample_id']} | {case['round_trip_num']} | "
            f"{case['round_trip_direction']} | {case['prompt_profile']} | "
            f"{case['chosen_route']} | {', '.join(case['allowed_routes'])} |"
        )
    if path_audit["unavailable_cases"]:
        unavailable = path_audit["unavailable_cases"][0]
        lines += [
            "",
            f"Route unavailable 案例：{unavailable['sample_id']} RT{unavailable['round_trip_num']} "
            f"{unavailable['round_trip_direction']}，profile={unavailable['prompt_profile']}，"
            f"actual_method={unavailable['actual_method']}。",
        ]
    lines += [
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
    path_audit = report["path_compatibility_audit"]
    print(
        "RESULT: PASS zero_api "
        f"rows={report['source']['total_rows']} success={report['source']['success_rows']} "
        f"union_exceeded={report['threshold_coverage']['union_exceeded']} "
        f"path_incompatible={path_audit['incompatible_steps']}/"
        f"{path_audit['chosen_route_available_steps']} "
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
