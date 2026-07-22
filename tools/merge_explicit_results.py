#!/usr/bin/env python3
"""Merge explicitly selected result chains without replaying or reading API logs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


METHODS = ("hybridpatch", "fullrewrite")
DIRECTIONS = ("forward", "backward")
TIE_TOLERANCE = 1e-12
CRITICAL_FAILURE_THRESHOLD = 0.10


class MergeError(RuntimeError):
    pass


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MergeError(f"cannot read JSON {path}: {exc}") from exc


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise MergeError(
                        f"invalid JSONL at {path}:{line_number}: {exc}"
                    ) from exc
                if not isinstance(row, dict):
                    raise MergeError(f"JSONL row is not an object: {path}:{line_number}")
                rows.append(row)
    except OSError as exc:
        raise MergeError(f"cannot read result JSONL {path}: {exc}") from exc
    return rows


def _finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _score(row: dict[str, Any], source: str) -> float:
    evaluation = row.get("evaluation")
    if not isinstance(evaluation, dict):
        raise MergeError(f"evaluation is missing: {source}")
    score = evaluation.get("score")
    if score is not None:
        if not _finite_number(score) or not 0.0 <= float(score) <= 1.0:
            raise MergeError(f"score is outside finite [0, 1]: {source}")
        return float(score)
    error = evaluation.get("error")
    if isinstance(error, str) and error:
        return 0.0
    raise MergeError(f"backward result has neither score nor error: {source}")


def _grid_rows(
    path: Path, sample: str, method: str, round_trips: int
) -> list[dict[str, Any]]:
    rows = _read_jsonl(path)
    expected = {
        (round_trip, direction)
        for round_trip in range(1, round_trips + 1)
        for direction in DIRECTIONS
    }
    seen: set[tuple[int, str]] = set()
    for line_number, row in enumerate(rows, 1):
        source = f"{path}:{line_number}"
        if row.get("sample_id") != sample or row.get("method") != method:
            raise MergeError(f"result identity mismatch: {source}")
        coordinate = (row.get("round_trip_num"), row.get("round_trip_direction"))
        if coordinate not in expected:
            raise MergeError(f"invalid result coordinate {coordinate!r}: {source}")
        if coordinate in seen:
            raise MergeError(f"duplicate result coordinate {coordinate!r}: {source}")
        seen.add(coordinate)
    missing = sorted(expected - seen)
    if missing or len(rows) != len(expected):
        raise MergeError(
            f"incomplete result grid for {sample}/{method}: "
            f"rows={len(rows)}, missing={missing}"
        )
    return rows


def _mean(values: Iterable[float]) -> float | None:
    values = list(values)
    return statistics.mean(values) if values else None


def _median(values: Iterable[float]) -> float | None:
    values = list(values)
    return statistics.median(values) if values else None


def _nearest_rank(values: Iterable[float], probability: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    rank = max(1, math.ceil(probability * len(ordered)))
    return ordered[rank - 1]


def _scaled(value: float | None) -> float | None:
    return None if value is None else 100.0 * value


def _usage(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "result_rows": 0,
        "input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "total_usd": 0.0,
        "rows_with_total_usd": 0,
        "rows_with_complete_token_usage": 0,
    }
    for row in rows:
        result["result_rows"] += 1
        complete = True
        for field in (
            "input_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "output_tokens",
        ):
            value = row.get(field)
            if _finite_number(value):
                result[field] += value
            elif field in {"cache_read_input_tokens", "cache_creation_input_tokens"}:
                pass
            else:
                complete = False
        total_tokens = row.get("total_tokens")
        if _finite_number(total_tokens):
            result["total_tokens"] += total_tokens
        else:
            complete = False
        if complete:
            result["rows_with_complete_token_usage"] += 1
        total_usd = row.get("total_usd")
        if _finite_number(total_usd):
            result["total_usd"] += float(total_usd)
            result["rows_with_total_usd"] += 1
    return result


def _hybrid_telemetry(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    routes: Counter[str] = Counter()
    profiles: Counter[str] = Counter()
    repair_attempted = repair_used = repair_success = 0
    protocol_failures = invalid_json = schema_errors = 0
    telemetry_steps = 0
    preservation_violations = preservation_steps_with_violation = 0
    preservation_rates: list[float] = []
    for row in rows:
        bdpatch = row.get("bdpatch") or {}
        if not isinstance(bdpatch, dict):
            raise MergeError("HybridPatch bdpatch telemetry must be an object")
        violations = bdpatch.get("preservation_violations") or 0
        if not _finite_number(violations) or float(violations) < 0:
            raise MergeError("invalid preservation_violations telemetry")
        preservation_violations += int(violations)
        preservation_steps_with_violation += bool(violations)
        rate = bdpatch.get("preservation_rate")
        if _finite_number(rate):
            preservation_rates.append(float(rate))
        hybrid = bdpatch.get("hybrid") or {}
        if not hybrid:
            continue
        if not isinstance(hybrid, dict):
            raise MergeError("HybridPatch hybrid telemetry must be an object")
        telemetry_steps += 1
        routes[str(hybrid.get("route") or bdpatch.get("actual_method") or "unknown")] += 1
        profiles[str(hybrid.get("prompt_profile") or "unknown")] += 1
        repair = hybrid.get("repair") or {}
        if not isinstance(repair, dict):
            raise MergeError("HybridPatch repair telemetry must be an object")
        repair_attempted += bool(repair.get("attempted"))
        repair_used += bool(repair.get("used"))
        repair_success += bool(repair.get("success"))
        protocol_failures += bool(hybrid.get("failed_step_kept_context"))
        invalid_json += bool(hybrid.get("invalid_json"))
        schema_errors += bool(hybrid.get("schema_error_count"))
    return {
        "result_steps": len(rows),
        "telemetry_steps": telemetry_steps,
        "routes": dict(sorted(routes.items())),
        "prompt_profiles": dict(sorted(profiles.items())),
        "repair": {
            "attempted": repair_attempted,
            "used": repair_used,
            "success": repair_success,
            "rate": repair_attempted / telemetry_steps if telemetry_steps else None,
        },
        "protocol_failure": {
            "definition": "failed_step_kept_context",
            "steps": protocol_failures,
            "rate": protocol_failures / telemetry_steps if telemetry_steps else None,
            "invalid_json_steps": invalid_json,
            "schema_error_steps": schema_errors,
        },
        "preservation": {
            "violations": preservation_violations,
            "steps_with_violation": preservation_steps_with_violation,
            "ok": preservation_violations == 0,
            "mean_preservation_rate": _mean(preservation_rates),
            "minimum_preservation_rate": min(preservation_rates) if preservation_rates else None,
        },
    }


def _noop_forward(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    forward_rows = [row for row in rows if row.get("round_trip_direction") == "forward"]
    noop_rows = [
        row for row in forward_rows
        if (row.get("bdpatch") or {}).get("noop_forward") is True
    ]
    all_unchanged = [
        row for row in rows
        if (row.get("bdpatch") or {}).get("bytes_changed") is False
    ]
    return {
        "count": len(noop_rows),
        "forward_steps": len(forward_rows),
        "rate": len(noop_rows) / len(forward_rows) if forward_rows else None,
        "unique_samples": len({row.get("sample_id") for row in noop_rows}),
        "all_direction_exact_unchanged_count": len(all_unchanged),
        "all_steps": len(rows),
        "all_direction_exact_unchanged_by_direction": dict(sorted(Counter(
            str(row.get("round_trip_direction")) for row in all_unchanged
        ).items())),
        "by_round_trip": dict(sorted(Counter(
            str(row.get("round_trip_num")) for row in noop_rows
        ).items())),
        "by_actual_method": dict(sorted(Counter(
            str((row.get("bdpatch") or {}).get("actual_method"))
            for row in noop_rows
        ).items())),
    }


def _score_stability(
    scores: dict[tuple[str, str, int], float],
    samples: list[str],
    round_trips: int,
    method: str,
) -> dict[str, Any]:
    equal = transitions = both_zero = both_one = 0
    for sample in samples:
        for round_trip in range(2, round_trips + 1):
            previous = scores[(method, sample, round_trip - 1)]
            current = scores[(method, sample, round_trip)]
            transitions += 1
            if abs(previous - current) <= TIE_TOLERANCE:
                equal += 1
                both_zero += previous == 0.0 and current == 0.0
                both_one += previous == 1.0 and current == 1.0
    return {
        "equal_consecutive_backward_scores": equal,
        "eligible_transitions": transitions,
        "rate": equal / transitions if transitions else None,
        "both_zero": both_zero,
        "both_one": both_one,
        "not_a_document_noop_metric": True,
    }


def _paired_inference(
    deltas: list[float], *, seed: int = 42, resamples: int = 10_000
) -> dict[str, Any]:
    if not deltas:
        return {
            "bootstrap_seed": seed,
            "bootstrap_resamples": resamples,
            "mean_delta_ci95_raw_0_1": [None, None],
            "mean_delta_ci95_percentage_points": [None, None],
            "sign_test_non_ties": 0,
            "sign_test_two_sided_p": None,
        }
    rng = random.Random(seed)
    count = len(deltas)
    means = sorted(
        sum(deltas[rng.randrange(count)] for _ in range(count)) / count
        for _ in range(resamples)
    )
    lower = _nearest_rank(means, 0.025)
    upper = _nearest_rank(means, 0.975)
    wins = sum(delta > TIE_TOLERANCE for delta in deltas)
    losses = sum(delta < -TIE_TOLERANCE for delta in deltas)
    non_ties = wins + losses
    if non_ties:
        tail = sum(
            math.comb(non_ties, index)
            for index in range(min(wins, losses) + 1)
        ) / (2 ** non_ties)
        sign_p = min(1.0, 2.0 * tail)
    else:
        sign_p = 1.0
    return {
        "bootstrap_seed": seed,
        "bootstrap_resamples": resamples,
        "bootstrap_interval": "nearest-rank percentile 95% CI",
        "mean_delta_ci95_raw_0_1": [lower, upper],
        "mean_delta_ci95_percentage_points": [_scaled(lower), _scaled(upper)],
        "sign_test_non_ties": non_ties,
        "sign_test_two_sided_p": sign_p,
    }


def _critical_failures(
    scores: dict[tuple[str, str, int], float],
    samples: list[str],
    round_trips: int,
    method: str,
) -> dict[str, Any]:
    count = transitions = 0
    by_sample: Counter[str] = Counter()
    for sample in samples:
        for round_trip in range(1, round_trips):
            previous = scores[(method, sample, round_trip)]
            current = scores[(method, sample, round_trip + 1)]
            transitions += 1
            if (
                (previous > 0.0 and current <= 1e-9)
                or previous - current + TIE_TOLERANCE >= CRITICAL_FAILURE_THRESHOLD
            ):
                count += 1
                by_sample[sample] += 1
    return {
        "threshold_raw_0_1": CRITICAL_FAILURE_THRESHOLD,
        "threshold_percentage_points": 100.0 * CRITICAL_FAILURE_THRESHOLD,
        "count": count,
        "eligible_transitions": transitions,
        "rate": count / transitions if transitions else None,
        "by_sample": dict(sorted(by_sample.items())),
    }


def _load_spec(spec_path: Path, repository_root: Path) -> tuple[list[str], list[dict[str, Any]]]:
    spec = _read_json(spec_path)
    if spec.get("schema") != "hybridpatch.explicit_result_merge/1":
        raise MergeError(f"unsupported merge spec schema: {spec.get('schema')!r}")
    round_trips = spec.get("round_trips")
    if round_trips != 10:
        raise MergeError("this report requires exactly 10 round trips")

    sources: dict[tuple[str, str], dict[str, Any]] = {}
    selected_samples: list[str] = []
    for batch in spec.get("fixed_batches") or []:
        label = batch["label"]
        if "scope_path" in batch:
            scope = _read_json(repository_root / batch["scope_path"])
            samples = scope.get(batch.get("sample_field", "analysis_sample_ids"))
        else:
            manifest = _read_json(repository_root / batch["manifest_path"])
            samples = (manifest.get("config") or {}).get("samples")
        if not isinstance(samples, list) or not all(isinstance(x, str) and x for x in samples):
            raise MergeError(f"invalid sample scope for fixed batch {label}")
        for sample in samples:
            if sample not in selected_samples:
                selected_samples.append(sample)
            for method in batch["methods"]:
                path = batch["result_path_template"].format(method=method, sample=sample)
                sources[(sample, method)] = {
                    "sample_id": sample,
                    "method": method,
                    "source_label": label,
                    "source_path": path,
                    "override": False,
                }

    for sample in spec.get("additional_samples") or []:
        if sample not in selected_samples:
            selected_samples.append(sample)

    for item in spec.get("overrides") or []:
        sample = item["sample_id"]
        method = item["method"]
        if method not in METHODS:
            raise MergeError(f"invalid override method: {method!r}")
        sources[(sample, method)] = {
            "sample_id": sample,
            "method": method,
            "source_label": item["source_label"],
            "source_path": item["source_path"],
            "override": True,
        }

    selected_samples = sorted(selected_samples)
    expected = {(sample, method) for sample in selected_samples for method in METHODS}
    missing = sorted(expected - set(sources))
    extra = sorted(set(sources) - expected)
    if missing or extra:
        raise MergeError(f"source coverage mismatch: missing={missing}, extra={extra}")
    return selected_samples, [sources[key] for key in sorted(sources)]


def build_report(spec_path: Path, out_dir: Path, repository_root: Path) -> dict[str, Any]:
    spec = _read_json(spec_path)
    round_trips = spec["round_trips"]
    samples, sources = _load_spec(spec_path, repository_root)
    if len(samples) != spec.get("expected_sample_count"):
        raise MergeError(
            f"sample count mismatch: expected={spec.get('expected_sample_count')}, "
            f"observed={len(samples)}"
        )

    rows_by_cell: dict[tuple[str, str, int, str], dict[str, Any]] = {}
    scores: dict[tuple[str, str, int], float] = {}
    rows_by_method: dict[str, list[dict[str, Any]]] = {method: [] for method in METHODS}
    committed_errors: list[dict[str, Any]] = []
    for source in sources:
        path = repository_root / source["source_path"]
        rows = _grid_rows(path, source["sample_id"], source["method"], round_trips)
        for line_number, row in enumerate(rows, 1):
            key = (
                source["method"],
                source["sample_id"],
                row["round_trip_num"],
                row["round_trip_direction"],
            )
            if key in rows_by_cell:
                raise MergeError(f"duplicate merged result cell: {key!r}")
            rows_by_cell[key] = row
            rows_by_method[source["method"]].append(row)
            if row["round_trip_direction"] == "backward":
                score = _score(row, f"{path}:{line_number}")
                scores[key[:3]] = score
                evaluation = row.get("evaluation") or {}
                if evaluation.get("score") is None and evaluation.get("error"):
                    committed_errors.append(
                        {
                            "sample_id": source["sample_id"],
                            "method": source["method"],
                            "round_trip": row["round_trip_num"],
                            "error": evaluation["error"],
                        }
                    )

    expected_cells = len(samples) * len(METHODS) * round_trips * len(DIRECTIONS)
    if len(rows_by_cell) != expected_cells:
        raise MergeError(
            f"merged cell count mismatch: expected={expected_cells}, observed={len(rows_by_cell)}"
        )

    round_trip_scores: list[dict[str, Any]] = []
    for round_trip in range(1, round_trips + 1):
        hp = [scores[("hybridpatch", sample, round_trip)] for sample in samples]
        fr = [scores[("fullrewrite", sample, round_trip)] for sample in samples]
        hp_mean = statistics.mean(hp)
        fr_mean = statistics.mean(fr)
        round_trip_scores.append(
            {
                "round_trip": round_trip,
                "fixed_n": len(samples),
                "paired_n": len(samples),
                "complete": True,
                "hybridpatch_mean_raw_0_1": hp_mean,
                "fullrewrite_mean_raw_0_1": fr_mean,
                "delta_raw_0_1": hp_mean - fr_mean,
                "hybridpatch_percent": _scaled(hp_mean),
                "fullrewrite_percent": _scaled(fr_mean),
                "delta_percentage_points": _scaled(hp_mean - fr_mean),
            }
        )

    endpoint_rows: list[dict[str, Any]] = []
    deltas: list[float] = []
    hp_endpoint: list[float] = []
    fr_endpoint: list[float] = []
    for sample in samples:
        hp = scores[("hybridpatch", sample, round_trips)]
        fr = scores[("fullrewrite", sample, round_trips)]
        delta = hp - fr
        hp_endpoint.append(hp)
        fr_endpoint.append(fr)
        deltas.append(delta)
        endpoint_rows.append(
            {
                "sample_id": sample,
                "hybridpatch_raw_0_1": hp,
                "fullrewrite_raw_0_1": fr,
                "delta_raw_0_1": delta,
                "hybridpatch_percent": _scaled(hp),
                "fullrewrite_percent": _scaled(fr),
                "delta_percentage_points": _scaled(delta),
            }
        )
    delta_median = statistics.median(deltas)
    p25 = _nearest_rank(deltas, 0.25)
    p75 = _nearest_rank(deltas, 0.75)
    wins = sum(delta > TIE_TOLERANCE for delta in deltas)
    losses = sum(delta < -TIE_TOLERANCE for delta in deltas)
    endpoint_summary = {
        "raw_0_1": {
            "paired_n": len(samples),
            "fixed_n": len(samples),
            "hybridpatch_mean": statistics.mean(hp_endpoint),
            "fullrewrite_mean": statistics.mean(fr_endpoint),
            "delta_mean": statistics.mean(deltas),
            "hybridpatch_median": statistics.median(hp_endpoint),
            "fullrewrite_median": statistics.median(fr_endpoint),
            "delta_median": delta_median,
            "sample_delta_sd": statistics.stdev(deltas),
            "delta_p25_nearest_rank": p25,
            "delta_p75_nearest_rank": p75,
            "delta_iqr": p75 - p25,
            "delta_mad": statistics.median(abs(delta - delta_median) for delta in deltas),
        },
        "win_loss_tie": {
            "hybridpatch_wins": wins,
            "hybridpatch_losses": losses,
            "ties": len(samples) - wins - losses,
            "tie_tolerance_raw": TIE_TOLERANCE,
        },
        "difference_concentration": {
            "absolute_delta_le_5pp_count": sum(abs(delta) <= 0.05 + TIE_TOLERANCE for delta in deltas),
            "absolute_delta_le_5pp_share": sum(abs(delta) <= 0.05 + TIE_TOLERANCE for delta in deltas) / len(deltas),
            "absolute_delta_le_10pp_count": sum(abs(delta) <= 0.10 + TIE_TOLERANCE for delta in deltas),
            "absolute_delta_le_10pp_share": sum(abs(delta) <= 0.10 + TIE_TOLERANCE for delta in deltas) / len(deltas),
            "quartile_method": "nearest_rank",
            "mad_definition": "median(abs(delta - median(delta)))",
        },
        "paired_inference": _paired_inference(deltas),
    }
    endpoint_summary["display_percent_and_percentage_points"] = {
        key: (_scaled(value) if key not in {"paired_n", "fixed_n"} else value)
        for key, value in endpoint_summary["raw_0_1"].items()
    }

    noop_flags = {
        (method, sample, round_trip): bool(
            (rows_by_cell[(method, sample, round_trip, "forward")].get("bdpatch") or {})
            .get("noop_forward")
        )
        for method in METHODS
        for sample in samples
        for round_trip in range(1, round_trips + 1)
    }
    noop_adjusted_scores = {
        key: (0.0 if noop_flags[key] else value)
        for key, value in scores.items()
    }
    noop_adjusted_rt: list[dict[str, Any]] = []
    for round_trip in range(1, round_trips + 1):
        hp_mean = statistics.mean(
            noop_adjusted_scores[("hybridpatch", sample, round_trip)]
            for sample in samples
        )
        fr_mean = statistics.mean(
            noop_adjusted_scores[("fullrewrite", sample, round_trip)]
            for sample in samples
        )
        original = round_trip_scores[round_trip - 1]
        noop_adjusted_rt.append({
            "round_trip": round_trip,
            "fixed_n": len(samples),
            "paired_n": len(samples),
            "complete": True,
            "hybridpatch_mean_raw_0_1": hp_mean,
            "fullrewrite_mean_raw_0_1": fr_mean,
            "delta_raw_0_1": hp_mean - fr_mean,
            "hybridpatch_percent": _scaled(hp_mean),
            "fullrewrite_percent": _scaled(fr_mean),
            "delta_percentage_points": _scaled(hp_mean - fr_mean),
            "original_delta_percentage_points": original["delta_percentage_points"],
            "gap_reduction_percentage_points": (
                original["delta_percentage_points"] - _scaled(hp_mean - fr_mean)
            ),
        })
    adjusted_deltas = [
        noop_adjusted_scores[("hybridpatch", sample, round_trips)]
        - noop_adjusted_scores[("fullrewrite", sample, round_trips)]
        for sample in samples
    ]
    noop_contributions = [
        original - adjusted
        for original, adjusted in zip(deltas, adjusted_deltas)
    ]
    adjusted_wins = sum(delta > TIE_TOLERANCE for delta in adjusted_deltas)
    adjusted_losses = sum(delta < -TIE_TOLERANCE for delta in adjusted_deltas)
    contribution_wins = sum(
        delta > TIE_TOLERANCE for delta in noop_contributions
    )
    contribution_losses = sum(
        delta < -TIE_TOLERANCE for delta in noop_contributions
    )
    noop_penalized_sensitivity = {
        "label": "post_hoc_noop_penalized_sensitivity",
        "headline_replacement": False,
        "policy": (
            "For either method, set a round trip backward score to 0.0 when "
            "the corresponding forward editable context is byte-identical to its input."
        ),
        "fixed_n": len(samples),
        "round_trip_scores": noop_adjusted_rt,
        "rt10": {
            "hybridpatch_mean_raw_0_1": statistics.mean(
                noop_adjusted_scores[("hybridpatch", sample, round_trips)]
                for sample in samples
            ),
            "fullrewrite_mean_raw_0_1": statistics.mean(
                noop_adjusted_scores[("fullrewrite", sample, round_trips)]
                for sample in samples
            ),
            "delta_mean_raw_0_1": statistics.mean(adjusted_deltas),
            "delta_median_raw_0_1": statistics.median(adjusted_deltas),
            "win_loss_tie": {
                "hybridpatch_wins": adjusted_wins,
                "hybridpatch_losses": adjusted_losses,
                "ties": len(samples) - adjusted_wins - adjusted_losses,
            },
            "paired_inference": _paired_inference(adjusted_deltas),
        },
        "direct_noop_gap_contribution": {
            "mean_raw_0_1": statistics.mean(noop_contributions),
            "mean_percentage_points": _scaled(statistics.mean(noop_contributions)),
            "win_loss_tie": {
                "positive": contribution_wins,
                "negative": contribution_losses,
                "zero": len(samples) - contribution_wins - contribution_losses,
            },
            "paired_inference": _paired_inference(noop_contributions),
        },
        "interpretation_limit": (
            "This removes direct same-round-trip recovery credit only; downstream "
            "state effects require a separately controlled rerun for causal attribution."
        ),
    }

    source_counts = Counter(source["source_label"] for source in sources)
    integrity = {
        "complete": True,
        "expected_sample_count": spec["expected_sample_count"],
        "observed_sample_count": len(samples),
        "expected_method_count": len(METHODS),
        "observed_method_count": len({source["method"] for source in sources}),
        "expected_sample_method_pairs": len(samples) * len(METHODS),
        "observed_sample_method_pairs": len(sources),
        "expected_result_cells": expected_cells,
        "observed_result_cells": len(rows_by_cell),
        "duplicate_result_cells": [],
        "missing_result_cells": [],
        "all_round_trips_fixed_n": all(row["paired_n"] == len(samples) for row in round_trip_scores),
        "source_counts_by_label": dict(sorted(source_counts.items())),
        "checks_deliberately_not_run": [
            "sha256",
            "recursive_experiment_scan",
            "raw_api_request_response_review",
            "api_attempt_ledger_review",
            "dry_run_or_preflight",
            "evaluator_replay",
            "prepare_review_finalize",
        ],
    }
    report = {
        "schema": "hybridpatch.explicit_merged_summary/1",
        "score_storage_scale": "raw_0_1_unchanged",
        "display_scale": "percent_and_percentage_points",
        "merge_spec": spec_path.relative_to(repository_root).as_posix(),
        "integrity": integrity,
        "round_trip_scores": round_trip_scores,
        "rt10_by_sample": endpoint_rows,
        "rt10_summary": endpoint_summary,
        "noop_penalized_sensitivity": noop_penalized_sensitivity,
        "critical_failure_at_0_10": {
            method: _critical_failures(scores, samples, round_trips, method)
            for method in METHODS
        },
        "noop_forward": {
            method: _noop_forward(rows_by_method[method]) for method in METHODS
        },
        "backward_score_stability": {
            method: _score_stability(scores, samples, round_trips, method)
            for method in METHODS
        },
        "hybridpatch_telemetry": _hybrid_telemetry(rows_by_method["hybridpatch"]),
        "usage": {method: _usage(rows_by_method[method]) for method in METHODS},
        "committed_evaluation_errors": {
            "count": len(committed_errors),
            "cells": committed_errors,
            "scoring_policy": "committed evaluation.error without numeric score is scored 0.0",
        },
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    with (out_dir / "source_manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "sample_id", "method", "source_label", "source_path", "override"
        ])
        writer.writeheader()
        writer.writerows(sources)
    with (out_dir / "backward_scores.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["sample_id", "method", "round_trip", "score_raw_0_1", "score_percent"])
        for sample in samples:
            for method in METHODS:
                for round_trip in range(1, round_trips + 1):
                    score = scores[(method, sample, round_trip)]
                    writer.writerow([sample, method, round_trip, score, _scaled(score)])
    with (out_dir / "rt10_by_sample.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(endpoint_rows[0]))
        writer.writeheader()
        writer.writerows(endpoint_rows)

    lines = [
        "# HP_V8 / FullRewrite 234-sample 快速显式合并",
        "",
        "本报告只聚合显式 manifest 指定的结果 JSONL；不重跑 evaluator/scoring，不读取 raw API、ledger 或失败 attempt，也不执行 preflight 或 prepare → review → finalize。原始 0–1 分数保持不变，表格按百分制显示。",
        "",
        "## 完整性",
        "",
        f"- 样本：{len(samples)}/{spec['expected_sample_count']}",
        f"- 样本—方法对：{len(sources)}/{len(samples) * len(METHODS)}",
        f"- RT 结果格：{len(rows_by_cell)}/{expected_cells}",
        "- 重复：0；缺失：0；RT1–RT10 均为固定 n=234。",
        "- 来源：" + ", ".join(f"{key}={value}" for key, value in sorted(source_counts.items())),
        "",
        "## RT1–RT10",
        "",
        "| RT | HP_V8 | FullRewrite | 差值（百分点） | n |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in round_trip_scores:
        lines.append(
            f"| {row['round_trip']} | {row['hybridpatch_percent']:.4f} | "
            f"{row['fullrewrite_percent']:.4f} | {row['delta_percentage_points']:+.4f} | "
            f"{row['paired_n']} |"
        )
    summary_display = endpoint_summary["display_percent_and_percentage_points"]
    wlt = endpoint_summary["win_loss_tie"]
    concentration = endpoint_summary["difference_concentration"]
    original_inference = endpoint_summary["paired_inference"]
    noop_sensitivity = report["noop_penalized_sensitivity"]
    adjusted_rt10 = noop_sensitivity["rt10"]
    adjusted_inference = adjusted_rt10["paired_inference"]
    noop_contribution = noop_sensitivity["direct_noop_gap_contribution"]
    contribution_inference = noop_contribution["paired_inference"]
    telemetry = report["hybridpatch_telemetry"]
    critical = report["critical_failure_at_0_10"]
    noop = report["noop_forward"]
    score_stability = report["backward_score_stability"]
    usage = report["usage"]
    route_text = ", ".join(
        f"{route}={count}" for route, count in telemetry["routes"].items()
    )
    lines.extend([
        "",
        "## RT10 摘要",
        "",
        f"- 均值：HP_V8 {summary_display['hybridpatch_mean']:.4f}，FullRewrite {summary_display['fullrewrite_mean']:.4f}，差值 {summary_display['delta_mean']:+.4f} 个百分点。",
        f"- 中位数：HP_V8 {summary_display['hybridpatch_median']:.4f}，FullRewrite {summary_display['fullrewrite_median']:.4f}，样本差值中位数 {summary_display['delta_median']:+.4f} 个百分点。",
        f"- Win/Loss/Tie：{wlt['hybridpatch_wins']}/{wlt['hybridpatch_losses']}/{wlt['ties']}。",
        f"- 差值集中度：|Δ|≤5pp 为 {concentration['absolute_delta_le_5pp_count']}/{len(samples)}，|Δ|≤10pp 为 {concentration['absolute_delta_le_10pp_count']}/{len(samples)}。",
        f"- 样本级配对 bootstrap 95% CI：[{original_inference['mean_delta_ci95_percentage_points'][0]:.4f}, {original_inference['mean_delta_ci95_percentage_points'][1]:.4f}] 个百分点；exact two-sided sign test p={original_inference['sign_test_two_sided_p']:.6g}（non-ties={original_inference['sign_test_non_ties']}）。",
        "",
        "## no-op 惩罚敏感性分析",
        "",
        "为排除无效 forward 编辑带来的直接恢复分数收益，对两种方法对称应用以下规则：若 forward 输出与输入可编辑上下文逐文件完全相同，则将同一 round trip 的 backward score 置为 0。全部 234 个样本继续保留，分母不变；原始结果仍是主分析，本节是 post-hoc sensitivity，不替代 headline。",
        "",
        "| RT | 调整后 HP_V8 | 调整后 FullRewrite | 调整后差值 | 原差值 | 差值缩减 | n |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in noop_sensitivity["round_trip_scores"]:
        lines.append(
            f"| {row['round_trip']} | {row['hybridpatch_percent']:.4f} | "
            f"{row['fullrewrite_percent']:.4f} | {row['delta_percentage_points']:+.4f} | "
            f"{row['original_delta_percentage_points']:+.4f} | "
            f"{row['gap_reduction_percentage_points']:.4f} | {row['paired_n']} |"
        )
    adjusted_wlt = adjusted_rt10["win_loss_tie"]
    lines.extend([
        "",
        f"- 调整后 RT10：HP_V8 {_scaled(adjusted_rt10['hybridpatch_mean_raw_0_1']):.4f}，FullRewrite {_scaled(adjusted_rt10['fullrewrite_mean_raw_0_1']):.4f}，差值 {_scaled(adjusted_rt10['delta_mean_raw_0_1']):+.4f} 个百分点；Win/Loss/Tie={adjusted_wlt['hybridpatch_wins']}/{adjusted_wlt['hybridpatch_losses']}/{adjusted_wlt['ties']}。",
        f"- 调整后 RT10 配对 bootstrap 95% CI：[{adjusted_inference['mean_delta_ci95_percentage_points'][0]:.4f}, {adjusted_inference['mean_delta_ci95_percentage_points'][1]:.4f}] 个百分点；exact two-sided sign test p={adjusted_inference['sign_test_two_sided_p']:.6g}（non-ties={adjusted_inference['sign_test_non_ties']}）。",
        f"- RT10 no-op 直接贡献：{noop_contribution['mean_percentage_points']:.4f} 个百分点，bootstrap 95% CI [{contribution_inference['mean_delta_ci95_percentage_points'][0]:.4f}, {contribution_inference['mean_delta_ci95_percentage_points'][1]:.4f}]；sign test p={contribution_inference['sign_test_two_sided_p']:.6g}。",
        "- 解释边界：该处理只移除同一 RT 的直接收益；no-op 对后续链状态的因果影响必须通过单独受控重跑估计。不要对 RT1–RT10 分别做十次显著性检验，也不要把 2340 个重复测量点当作独立样本。",
        "",
        "## 运行与协议汇总",
        "",
        f"- CriticalFailure：HP_V8 {critical['hybridpatch']['count']}/{critical['hybridpatch']['eligible_transitions']}，FullRewrite {critical['fullrewrite']['count']}/{critical['fullrewrite']['eligible_transitions']}。",
        f"- no-op forward：HP_V8 {noop['hybridpatch']['count']}/{noop['hybridpatch']['forward_steps']}（{100.0 * noop['hybridpatch']['rate']:.4f}%），FullRewrite {noop['fullrewrite']['count']}/{noop['fullrewrite']['forward_steps']}（{100.0 * noop['fullrewrite']['rate']:.4f}%）。",
        f"- 全方向逐文件完全不变：HP_V8 {noop['hybridpatch']['all_direction_exact_unchanged_count']}/{noop['hybridpatch']['all_steps']}，FullRewrite {noop['fullrewrite']['all_direction_exact_unchanged_count']}/{noop['fullrewrite']['all_steps']}。",
        f"- 相邻 backward 分数完全相同（不是内容 no-op）：HP_V8 {score_stability['hybridpatch']['equal_consecutive_backward_scores']}/{score_stability['hybridpatch']['eligible_transitions']}，FullRewrite {score_stability['fullrewrite']['equal_consecutive_backward_scores']}/{score_stability['fullrewrite']['eligible_transitions']}。",
        f"- HP route：{route_text}。",
        f"- HP repair：{telemetry['repair']['attempted']}/{telemetry['telemetry_steps']} attempted，{telemetry['repair']['used']} used，{telemetry['repair']['success']} success。",
        f"- HP protocol failure：{telemetry['protocol_failure']['steps']}/{telemetry['telemetry_steps']}；preservation_violations={telemetry['preservation']['violations']}。",
        f"- Token：HP_V8 {usage['hybridpatch']['total_tokens']:,}，FullRewrite {usage['fullrewrite']['total_tokens']:,}。",
        f"- 费用：HP_V8 ${usage['hybridpatch']['total_usd']:.6f}，FullRewrite ${usage['fullrewrite']['total_usd']:.6f}。",
        f"- 已提交 evaluation.error 且按冻结政策计 0 的 backward cell：{len(committed_errors)}。",
        "",
        "## Git 保存范围",
        "",
        "- Git 仅保存本摘要与轻量汇总代码；本地派生的 `summary.json`、CSV、merge spec 和实验结果目录不纳入提交。",
        "",
    ])
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    repository_root = args.repository_root.resolve()
    spec_path = (repository_root / args.spec).resolve() if not args.spec.is_absolute() else args.spec.resolve()
    out_dir = (repository_root / args.out_dir).resolve() if not args.out_dir.is_absolute() else args.out_dir.resolve()
    report = build_report(spec_path, out_dir, repository_root)
    print(json.dumps({
        "complete": report["integrity"]["complete"],
        "sample_count": report["integrity"]["observed_sample_count"],
        "sample_method_pairs": report["integrity"]["observed_sample_method_pairs"],
        "result_cells": report["integrity"]["observed_result_cells"],
        "out_dir": out_dir.as_posix(),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
