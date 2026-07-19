#!/usr/bin/env python3
"""Strict, zero-API report for a manifest-bound paired confirmation campaign.

The source result rows keep their native 0--1 scores.  This tool emits both the
raw values and presentation-only percentages/percentage-point differences.
It never calls a provider and never mutates the experiment directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "hybridpatch.confirmation_campaign_analysis/1"
ROOT = Path(__file__).resolve().parents[1]
MANIFEST_SCHEMA_PREFIX = "anchorpatch.paired_campaign_manifest/"
METHODS = ("hybridpatch", "fullrewrite")
METHOD_LABELS = {
    "hybridpatch": "HP_V8",
    "fullrewrite": "FullRewrite",
}
TIE_TOLERANCE = 1e-12
CRITICAL_FAILURE_THRESHOLD = 0.10
CONFIRMATION_EXPERIMENT_ID = (
    "exp_20260719_hybridv8_transportv4_method_unseen_confirmation68"
)
CONFIRMATION_SAMPLE_COUNT = 68
CONFIRMATION_ROUND_TRIPS = 10
CONFIRMATION_SELECTION_PATH = (
    "HP_V8/analysis/"
    "exp_20260719_hybridv8_transportv4_method_unseen_confirmation68_selection.json"
)
CONFIRMATION_SELECTION_COUNTS = {
    "candidate_count": 85,
    "selected_count": 68,
    "reserve_count": 17,
}
CONFIRMATION_SENSITIVITY_SETS = {
    frozenset(("python4",)),
    frozenset(("audiosyn1",)),
    frozenset(("python4", "audiosyn1")),
}
MIXED_CONFIRMATION_EXPERIMENT_ID = (
    "exp_20260719_hybridv8_transportv4_mixed_confirmation100"
)
MIXED_CONFIRMATION_SELECTION_PATH = (
    "HP_V8/analysis/20260719_hybridv8_transportv4_mixed_confirmation100/"
    "selection.json"
)
MIXED_CONFIRMATION_SELECTION_COUNTS = {
    "candidate_count": 190,
    "selected_count": 100,
    "reserve_count": 90,
}


class AnalysisError(RuntimeError):
    """The campaign artifacts violate a structural reporting contract."""


class CampaignIncompleteError(AnalysisError):
    """The selected manifest scope is not complete enough for formal reporting."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AnalysisError(f"required file is missing: {path}") from exc
    except (OSError, ValueError) as exc:
        raise AnalysisError(f"cannot read valid JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AnalysisError(f"expected a JSON object: {path}")
    return value


def _read_jsonl(path: Path, *, required: bool = True) -> list[dict[str, Any]]:
    if not path.is_file():
        if required:
            raise AnalysisError(f"required file is missing: {path}")
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except ValueError as exc:
                    raise AnalysisError(
                        f"invalid JSONL row: {path}:{line_number}"
                    ) from exc
                if not isinstance(row, dict):
                    raise AnalysisError(
                        f"expected a JSON object: {path}:{line_number}"
                    )
                rows.append(row)
    except OSError as exc:
        raise AnalysisError(f"cannot read JSONL: {path}: {exc}") from exc
    return rows


def _exact_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _confirmation_identity(
        manifest: dict[str, Any],
        samples: list[str],
        round_trips: int,
        *,
        repository_root: Path = ROOT,
) -> dict[str, Any]:
    config = manifest.get("config") or {}
    experiment_id = manifest.get("experiment_id")
    specs = {
        CONFIRMATION_EXPERIMENT_ID: {
            "sample_count": 68,
            "method_first": Counter({"hybridpatch": 34, "fullrewrite": 34}),
            "waves": [52, 16],
            "selection_path": CONFIRMATION_SELECTION_PATH,
            "selection_counts": CONFIRMATION_SELECTION_COUNTS,
            "policy_schema": "anchorpatch.confirmation_analysis_policy/1",
            "cohort_first": {},
        },
        MIXED_CONFIRMATION_EXPERIMENT_ID: {
            "sample_count": 100,
            "method_first": Counter({"hybridpatch": 50, "fullrewrite": 50}),
            "waves": [52, 48],
            "selection_path": MIXED_CONFIRMATION_SELECTION_PATH,
            "selection_counts": MIXED_CONFIRMATION_SELECTION_COUNTS,
            "policy_schema": "anchorpatch.mixed_confirmation_analysis_policy/1",
            "cohort_first": {
                "method_unseen": {"hybridpatch": 30, "fullrewrite": 30},
                "historical_hp_method_exposed": {
                    "hybridpatch": 20,
                    "fullrewrite": 20,
                },
            },
        },
    }
    applicable = (
        config.get("campaign_role") == "confirmation"
        or experiment_id in specs
    )
    if not applicable:
        return {
            "applicable": False,
            "valid": False,
            "problems": ["manifest is outside the frozen confirmation identity"],
        }
    problems: list[str] = []
    spec = specs.get(experiment_id)
    if spec is None:
        spec = specs[CONFIRMATION_EXPERIMENT_ID]
        problems.append("experiment_id mismatch")
    expected_config = {
        "seed": 42,
        "model": "minimax-m3",
        "max_tokens": 131072,
        "distractor": True,
        "transport_revision": "opencode_anthropic_sdk/4",
    }
    if config.get("campaign_role") != "confirmation":
        problems.append("config.campaign_role mismatch")
    if len(samples) != spec["sample_count"]:
        problems.append("selected sample count mismatch")
    if round_trips != CONFIRMATION_ROUND_TRIPS:
        problems.append("round-trip count mismatch")
    for key, value in expected_config.items():
        if config.get(key) != value:
            problems.append(f"config.{key} mismatch")
    commit = manifest.get("run_git_commit")
    if not isinstance(commit, str) or len(commit) != 40 or any(
            char not in "0123456789abcdef" for char in commit):
        problems.append("run_git_commit is not a full lowercase SHA")
    if manifest.get("git_tree_state") != "clean":
        problems.append("git_tree_state is not clean")

    assignments = manifest.get("assignments") or []
    first_counts = Counter(
        (item.get("methods") or [None])[0]
        for item in assignments
        if isinstance(item, dict)
    )
    if first_counts != spec["method_first"]:
        problems.append("method-first balance mismatch")
    waves = manifest.get("assignment_waves")
    if not isinstance(waves, list) or [
            wave.get("worker_count") for wave in waves if isinstance(wave, dict)
    ] != spec["waves"]:
        problems.append("assignment waves mismatch")

    selection = manifest.get("selection_manifest")
    selection_payload: dict[str, Any] | None = None
    if not isinstance(selection, dict):
        problems.append("selection manifest record is missing")
    else:
        if selection.get("path") != spec["selection_path"]:
            problems.append("selection manifest path mismatch")
        for key, value in spec["selection_counts"].items():
            if selection.get(key) != value:
                problems.append(f"selection manifest {key} mismatch")
        selection_path = repository_root / str(selection.get("path") or "")
        try:
            selection_payload = _read_json(selection_path)
        except AnalysisError as exc:
            problems.append(str(exc))
        else:
            if _sha256(selection_path) != selection.get("sha256"):
                problems.append("selection manifest SHA mismatch")
            if selection_payload.get("experiment_id") != experiment_id:
                problems.append("selection payload experiment_id mismatch")
            if selection_payload.get("selected_sample_ids") != samples:
                problems.append("manifest samples differ from frozen selection")

    cohort_samples: dict[str, list[str]] = {}
    if spec["cohort_first"] and selection_payload is not None:
        payload_cohorts = selection_payload.get("cohorts") or {}
        for cohort, expected_counts in spec["cohort_first"].items():
            values = (payload_cohorts.get(cohort) or {}).get(
                "selected_sample_ids")
            if not isinstance(values, list):
                problems.append(f"selection cohort missing: {cohort}")
                continue
            cohort_samples[cohort] = list(values)
            cohort_set = set(values)
            observed = Counter(
                (item.get("methods") or [None])[0]
                for item in assignments
                if isinstance(item, dict) and item.get("sample") in cohort_set
            )
            if observed != Counter(expected_counts):
                problems.append(f"cohort method-first balance mismatch: {cohort}")

    policy = manifest.get("analysis_policy")
    policy_values = policy if isinstance(policy, dict) else {}
    expected_sets = sorted(
        [sorted(values) for values in CONFIRMATION_SENSITIVITY_SETS]
    )
    if (
        not isinstance(policy, dict)
        or policy_values.get("schema") != spec["policy_schema"]
        or (policy_values.get("bootstrap") or {}).get("seed") != 42
        or (policy_values.get("bootstrap") or {}).get("resamples") != 10_000
        or (policy_values.get("critical_failure") or {}).get("metric")
        != "CriticalFailure"
        or (policy_values.get("critical_failure") or {}).get("threshold") != 0.10
        or (policy_values.get("evaluator_error_policy") or {}).get(
            "backward_rows_require"
        ) != "finite_score_0_1_or_nonempty_error"
        or (policy_values.get("evaluator_error_policy") or {}).get(
            "error_row_score"
        ) != 0.0
        or (policy_values.get("evaluator_error_policy") or {}).get(
            "error_rows_reported_separately"
        ) is not True
        or (policy_values.get(
            "known_committed_usage_wave_boundary_stop"
        ) or {}).get("scope")
        != "wave_boundary_committed_result_rows"
        or (policy_values.get(
            "known_committed_usage_wave_boundary_stop"
        ) or {}).get("usd_threshold")
        != 130.0
    ):
        problems.append("analysis policy is not the pre-registered policy")
    if experiment_id == CONFIRMATION_EXPERIMENT_ID and (
        sorted(
            sorted(values) for values in (
                policy_values.get("sensitivity_sets") or [])
        ) != expected_sets
    ):
        problems.append("analysis sensitivity sets are not pre-registered")
    if experiment_id == MIXED_CONFIRMATION_EXPERIMENT_ID and (
        policy_values.get("headline_scope") != "selected100"
        or policy_values.get("pre_registered_cohorts")
        != ["method_unseen", "historical_hp_method_exposed"]
    ):
        problems.append("mixed cohort analysis policy is not pre-registered")
    return {
        "applicable": True,
        "valid": not problems,
        "problems": problems,
        "experiment_id": experiment_id,
        "expected_sample_count": spec["sample_count"],
        "expected_round_trips": CONFIRMATION_ROUND_TRIPS,
        "selection_path": spec["selection_path"],
        "analysis_policy": policy,
        "cohort_samples": cohort_samples,
    }


def _postrun_verification(
        experiment_dir: Path, *, expected_backward_rows: int
) -> dict[str, Any]:
    verification_path = experiment_dir / "analysis" / "verification.log"
    inspection_path = experiment_dir / "analysis" / "strict_inspection_postrun.json"
    problems = []
    verification_text = ""
    if verification_path.is_file():
        verification_text = verification_path.read_text(
            encoding="utf-8", errors="replace"
        )
        if "exit_code=0" not in verification_text:
            problems.append("verification.log does not record exit_code=0")
        marker = (
            f"HONESTY GATE: PASS — {expected_backward_rows} backward RS "
            "independently reproduced from raw responses"
        )
        if marker not in verification_text:
            problems.append("verification.log lacks the exact honesty-gate PASS")
    else:
        problems.append("analysis/verification.log is missing")
    inspection = None
    if inspection_path.is_file():
        try:
            inspection = _read_json(inspection_path)
        except AnalysisError as exc:
            problems.append(str(exc))
        else:
            if inspection.get("errors") != []:
                problems.append("strict postrun inspection has errors")
            if inspection.get("preservation_violations") != 0:
                problems.append("strict postrun inspection has preservation violations")
    else:
        problems.append("analysis/strict_inspection_postrun.json is missing")
    return {
        "valid": not problems,
        "problems": problems,
        "verification_log": verification_path.as_posix(),
        "verification_log_sha256": (
            _sha256(verification_path) if verification_path.is_file() else None
        ),
        "strict_inspection": inspection,
        "strict_inspection_sha256": (
            _sha256(inspection_path) if inspection_path.is_file() else None
        ),
    }


def _manifest_scope(
    experiment_dir: Path,
) -> tuple[dict[str, Any], list[str], int]:
    manifest = _read_json(experiment_dir / "dispatch_manifest.json")
    schema = manifest.get("schema")
    if not isinstance(schema, str) or not schema.startswith(MANIFEST_SCHEMA_PREFIX):
        raise AnalysisError(f"unsupported paired campaign manifest schema: {schema!r}")
    config = manifest.get("config")
    if not isinstance(config, dict):
        raise AnalysisError("dispatch manifest config must be an object")
    samples = config.get("samples")
    if (
        not isinstance(samples, list)
        or not samples
        or not all(isinstance(sample, str) and sample for sample in samples)
        or len(set(samples)) != len(samples)
    ):
        raise AnalysisError("manifest config.samples must be unique non-empty strings")
    round_trips = config.get("num_round_trips")
    if not _exact_int(round_trips) or round_trips < 1:
        raise AnalysisError("manifest num_round_trips must be a positive integer")
    method_set = config.get("method_set")
    if not isinstance(method_set, list) or set(method_set) != set(METHODS):
        raise AnalysisError(
            "manifest method_set must be exactly hybridpatch and fullrewrite"
        )

    assignments = manifest.get("assignments")
    if assignments is not None:
        if not isinstance(assignments, list):
            raise AnalysisError("manifest assignments must be a list")
        assigned: list[str] = []
        for item in assignments:
            if not isinstance(item, dict):
                raise AnalysisError("manifest assignment must be an object")
            sample = item.get("sample")
            methods = item.get("methods")
            if sample not in samples or set(methods or []) != set(METHODS):
                raise AnalysisError(f"invalid manifest assignment: {item!r}")
            assigned.append(sample)
        if len(assigned) != len(samples) or set(assigned) != set(samples):
            raise AnalysisError(
                "manifest assignments do not cover selected samples exactly once"
            )

    task_plans = manifest.get("task_plans")
    if task_plans is not None:
        if not isinstance(task_plans, dict) or set(task_plans) != set(samples):
            raise AnalysisError(
                "manifest task_plans do not exactly match selected samples"
            )
    return manifest, list(samples), round_trips


def _formal_score(row: dict[str, Any], source: str) -> float:
    evaluation = row.get("evaluation")
    if not isinstance(evaluation, dict):
        raise AnalysisError(f"formal result evaluation is missing: {source}")
    score = evaluation.get("score")
    if score is not None:
        if not _finite_number(score):
            raise AnalysisError(f"formal score is not finite numeric: {source}")
        score = float(score)
        if score < 0.0 or score > 1.0:
            raise AnalysisError(f"formal score is outside [0, 1]: {source}")
        return score
    error = evaluation.get("error")
    if not isinstance(error, str) or not error:
        raise AnalysisError(
            f"formal result requires a score or non-empty evaluation error: {source}"
        )
    # Repository policy: a committed reconstruction/evaluation error is scored 0.
    return 0.0


def _committed_evaluation_errors(
        rows_by_cell: dict[tuple[str, str, int, str], dict[str, Any]]
) -> dict[str, Any]:
    by_method = Counter()
    cells = []
    for (method, sample, rt, direction), row in sorted(rows_by_cell.items()):
        evaluation = row.get("evaluation")
        if not isinstance(evaluation, dict):
            continue
        error = evaluation.get("error")
        if evaluation.get("score") is None and isinstance(error, str) and error:
            by_method[method] += 1
            cells.append({
                "method": method,
                "sample_id": sample,
                "round_trip": rt,
                "direction": direction,
                "error": error,
            })
    return {
        "count": len(cells),
        "by_method": dict(sorted(by_method.items())),
        "cells": cells,
        "scoring_policy": (
            "committed evaluation.error without numeric score is scored 0.0 "
            "by the frozen repository policy and is disclosed separately"
        ),
    }


def _load_result_rows(
    experiment_dir: Path,
    samples: list[str],
    round_trips: int,
) -> tuple[
    dict[tuple[str, str, int, str], dict[str, Any]],
    dict[tuple[str, str, int], float],
    list[dict[str, Any]],
]:
    selected = set(samples)
    rows_by_cell: dict[tuple[str, str, int, str], dict[str, Any]] = {}
    scores: dict[tuple[str, str, int], float] = {}
    all_rows: list[dict[str, Any]] = []

    for method in METHODS:
        folder = experiment_dir / method
        files = sorted(folder.glob("*.jsonl")) if folder.is_dir() else []
        for path in files:
            path_sample = path.stem
            if path_sample not in selected:
                raise AnalysisError(f"unexpected formal result file: {path}")
            for line_number, row in enumerate(_read_jsonl(path), 1):
                source = f"{path}:{line_number}"
                sample = row.get("sample_id")
                row_method = row.get("method")
                rt = row.get("round_trip_num")
                direction = row.get("round_trip_direction")
                if sample != path_sample or row_method != method:
                    raise AnalysisError(f"formal result identity mismatch: {source}")
                if (
                    not _exact_int(rt)
                    or rt < 1
                    or rt > round_trips
                    or direction not in {"forward", "backward"}
                ):
                    raise AnalysisError(f"formal result grid coordinate is invalid: {source}")
                cell = (method, sample, rt, direction)
                if cell in rows_by_cell:
                    raise AnalysisError(f"duplicate formal result cell: {cell!r}")
                rows_by_cell[cell] = row
                all_rows.append(row)
                if direction == "backward":
                    scores[(method, sample, rt)] = _formal_score(row, source)

    missing_cells: list[dict[str, Any]] = []
    for sample in samples:
        for method in METHODS:
            for rt in range(1, round_trips + 1):
                for direction in ("forward", "backward"):
                    cell = (method, sample, rt, direction)
                    if cell not in rows_by_cell:
                        missing_cells.append(
                            {
                                "sample_id": sample,
                                "method": method,
                                "round_trip": rt,
                                "direction": direction,
                            }
                        )
    return rows_by_cell, scores, missing_cells


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


def _scaled(value: float | None, factor: float = 100.0) -> float | None:
    return None if value is None else value * factor


def _paired_inference(
        deltas: list[float], *, seed: int = 42, resamples: int = 10_000
) -> dict[str, Any]:
    """Deterministic paired bootstrap CI plus exact two-sided sign test."""
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
    n = len(deltas)
    means = sorted(
        sum(deltas[rng.randrange(n)] for _ in range(n)) / n
        for _ in range(resamples)
    )
    lower = _nearest_rank(means, 0.025)
    upper = _nearest_rank(means, 0.975)
    wins = sum(delta > TIE_TOLERANCE for delta in deltas)
    losses = sum(delta < -TIE_TOLERANCE for delta in deltas)
    non_ties = wins + losses
    if non_ties:
        tail = sum(
            math.comb(non_ties, index) for index in range(min(wins, losses) + 1)
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


def _round_trip_table(
    scores: dict[tuple[str, str, int], float],
    samples: list[str],
    round_trips: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rt in range(1, round_trips + 1):
        paired_samples = [
            sample
            for sample in samples
            if ("hybridpatch", sample, rt) in scores
            and ("fullrewrite", sample, rt) in scores
        ]
        hp = [scores[("hybridpatch", sample, rt)] for sample in paired_samples]
        fr = [scores[("fullrewrite", sample, rt)] for sample in paired_samples]
        hp_mean = _mean(hp)
        fr_mean = _mean(fr)
        delta = (
            hp_mean - fr_mean
            if hp_mean is not None and fr_mean is not None
            else None
        )
        rows.append(
            {
                "round_trip": rt,
                "fixed_n": len(samples),
                "paired_n": len(paired_samples),
                "complete": len(paired_samples) == len(samples),
                "hybridpatch_mean_raw_0_1": hp_mean,
                "fullrewrite_mean_raw_0_1": fr_mean,
                "delta_raw_0_1": delta,
                "hybridpatch_percent": _scaled(hp_mean),
                "fullrewrite_percent": _scaled(fr_mean),
                "delta_percentage_points": _scaled(delta),
            }
        )
    return rows


def _sample_endpoint(
    scores: dict[tuple[str, str, int], float],
    samples: list[str],
    round_trips: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    paired: list[tuple[float, float, float]] = []
    for sample in samples:
        hp = scores.get(("hybridpatch", sample, round_trips))
        fr = scores.get(("fullrewrite", sample, round_trips))
        delta = hp - fr if hp is not None and fr is not None else None
        complete = delta is not None
        if complete:
            paired.append((hp, fr, delta))
        rows.append(
            {
                "sample_id": sample,
                "complete": complete,
                "hybridpatch_raw_0_1": hp,
                "fullrewrite_raw_0_1": fr,
                "delta_raw_0_1": delta,
                "hybridpatch_percent": _scaled(hp),
                "fullrewrite_percent": _scaled(fr),
                "delta_percentage_points": _scaled(delta),
            }
        )

    hp_values = [item[0] for item in paired]
    fr_values = [item[1] for item in paired]
    deltas = [item[2] for item in paired]
    delta_median = _median(deltas)
    p25 = _nearest_rank(deltas, 0.25)
    p75 = _nearest_rank(deltas, 0.75)
    mad = (
        _median(abs(value - delta_median) for value in deltas)
        if delta_median is not None
        else None
    )
    wins = sum(delta > TIE_TOLERANCE for delta in deltas)
    losses = sum(delta < -TIE_TOLERANCE for delta in deltas)
    ties = len(deltas) - wins - losses

    raw = {
        "paired_n": len(paired),
        "fixed_n": len(samples),
        "hybridpatch_mean": _mean(hp_values),
        "fullrewrite_mean": _mean(fr_values),
        "delta_mean": _mean(deltas),
        "hybridpatch_median": _median(hp_values),
        "fullrewrite_median": _median(fr_values),
        "delta_median": delta_median,
        "sample_delta_sd": statistics.stdev(deltas) if len(deltas) >= 2 else None,
        "delta_p25_nearest_rank": p25,
        "delta_p75_nearest_rank": p75,
        "delta_iqr": p75 - p25 if p25 is not None and p75 is not None else None,
        "delta_mad": mad,
    }
    stats = {
        "raw_0_1": raw,
        "display_percent_and_percentage_points": {
            key: _scaled(value)
            for key, value in raw.items()
            if key not in {"paired_n", "fixed_n"}
        },
        "win_loss_tie": {
            "hybridpatch_wins": wins,
            "hybridpatch_losses": losses,
            "ties": ties,
            "tie_tolerance_raw": TIE_TOLERANCE,
        },
        "difference_concentration": {
            "absolute_delta_le_5pp_count": sum(
                abs(delta) <= 0.05 + TIE_TOLERANCE for delta in deltas
            ),
            "absolute_delta_le_5pp_share": (
                sum(abs(delta) <= 0.05 + TIE_TOLERANCE for delta in deltas)
                / len(deltas)
                if deltas
                else None
            ),
            "absolute_delta_le_10pp_count": sum(
                abs(delta) <= 0.10 + TIE_TOLERANCE for delta in deltas
            ),
            "absolute_delta_le_10pp_share": (
                sum(abs(delta) <= 0.10 + TIE_TOLERANCE for delta in deltas)
                / len(deltas)
                if deltas
                else None
            ),
            "quartile_method": "nearest_rank",
            "mad_definition": "median(abs(delta - median(delta)))",
        },
        "paired_inference": _paired_inference(deltas),
    }
    stats["display_percent_and_percentage_points"]["paired_n"] = len(paired)
    stats["display_percent_and_percentage_points"]["fixed_n"] = len(samples)
    return rows, stats


def _critical_failures(
    scores: dict[tuple[str, str, int], float],
    samples: list[str],
    round_trips: int,
    method: str,
) -> dict[str, Any]:
    failures = 0
    transitions = 0
    by_sample: Counter[str] = Counter()
    for sample in samples:
        for rt in range(1, round_trips):
            previous = scores.get((method, sample, rt))
            current = scores.get((method, sample, rt + 1))
            if previous is None or current is None:
                continue
            transitions += 1
            drop = previous - current
            if (
                (previous > 0.0 and current <= 1e-9)
                or drop + TIE_TOLERANCE >= CRITICAL_FAILURE_THRESHOLD
            ):
                failures += 1
                by_sample[sample] += 1
    return {
        "threshold_raw_0_1": CRITICAL_FAILURE_THRESHOLD,
        "threshold_percentage_points": 100.0 * CRITICAL_FAILURE_THRESHOLD,
        "count": failures,
        "eligible_transitions": transitions,
        "rate": failures / transitions if transitions else None,
        "by_sample": dict(sorted(by_sample.items())),
    }


def _usage_accumulator() -> dict[str, Any]:
    return {
        "calls_or_rows": 0,
        "input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "total_usd": 0.0,
        "rows_with_total_usd": 0,
        "rows_with_complete_token_usage": 0,
    }


def _add_usage(acc: dict[str, Any], row: dict[str, Any]) -> None:
    acc["calls_or_rows"] += 1
    complete = True
    for field in (
        "input_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
        "output_tokens",
    ):
        value = row.get(field)
        if _finite_number(value):
            acc[field] += value
        elif field in {"cache_read_input_tokens", "cache_creation_input_tokens"}:
            # Providers may omit zero-valued cache fields.
            pass
        else:
            complete = False
    total = row.get("total_tokens")
    if _finite_number(total):
        acc["total_tokens"] += total
    else:
        complete = False
    if complete:
        acc["rows_with_complete_token_usage"] += 1
    usd = row.get("total_usd")
    if _finite_number(usd):
        acc["total_usd"] += float(usd)
        acc["rows_with_total_usd"] += 1


def _method_from_semantic(record: dict[str, Any]) -> str | None:
    method = record.get("method")
    if method in METHODS:
        return method
    semantic_id = record.get("semantic_call_id")
    if isinstance(semantic_id, str):
        prefix = semantic_id.split("/", 1)[0]
        if prefix in METHODS:
            return prefix
    call_kind = record.get("call_kind")
    if isinstance(call_kind, str):
        for candidate in METHODS:
            if call_kind.startswith(candidate):
                return candidate
    return None


def _call_kind(record: dict[str, Any]) -> str:
    call_kind = record.get("call_kind")
    if isinstance(call_kind, str) and call_kind:
        return call_kind
    semantic_id = record.get("semantic_call_id")
    if isinstance(semantic_id, str):
        parts = semantic_id.split("/")
        if len(parts) >= 5 and parts[-1].startswith("g"):
            return parts[-2]
    return "unknown"


def _api_analysis(
    experiment_dir: Path,
    samples: list[str],
    round_trips: int,
    rows_by_cell: dict[tuple[str, str, int, str], dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    problems: list[str] = []
    selected = set(samples)
    calls_path = experiment_dir / "api_calls.jsonl"
    ledger_path = experiment_dir / "api_attempt_ledger.jsonl"
    journal_dir = experiment_dir / "api_journal"
    api_calls = _read_jsonl(calls_path, required=False)
    ledger = _read_jsonl(ledger_path, required=False)
    if not calls_path.is_file():
        problems.append("missing api_calls.jsonl")
    if not ledger_path.is_file():
        problems.append("missing api_attempt_ledger.jsonl")
    if not journal_dir.is_dir():
        problems.append("missing api_journal directory")

    api_ids: set[str] = set()
    api_by_id: dict[str, dict[str, Any]] = {}
    call_kinds: Counter[str] = Counter()
    for line_number, row in enumerate(api_calls, 1):
        request_id = row.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise AnalysisError(f"API call lacks request_id at line {line_number}")
        if request_id in api_ids:
            raise AnalysisError(f"duplicate API request_id: {request_id}")
        api_ids.add(request_id)
        api_by_id[request_id] = row
        sample = row.get("sample")
        method = row.get("method")
        rt = row.get("rt_index")
        direction = row.get("direction")
        if (
            sample not in selected
            or method not in METHODS
            or not _exact_int(rt)
            or not 1 <= rt <= round_trips
            or direction not in {"forward", "backward"}
        ):
            raise AnalysisError(
                f"API call is outside manifest scope: api_calls.jsonl:{line_number}"
            )
        call_kinds[_call_kind(row)] += 1

    result_reference_counts: Counter[str] = Counter()
    result_cell_call_ids: dict[
        tuple[str, str, int, str], list[str]
    ] = {}
    for cell, row in rows_by_cell.items():
        call_ids = row.get("api_call_ids")
        if (
            not isinstance(call_ids, list)
            or not call_ids
            or not all(isinstance(call_id, str) and call_id for call_id in call_ids)
            or len(set(call_ids)) != len(call_ids)
        ):
            raise AnalysisError(f"result cell has invalid api_call_ids: {cell!r}")
        result_reference_counts.update(call_ids)
        result_cell_call_ids[cell] = list(call_ids)
        method, sample, rt, direction = cell
        for call_id in call_ids:
            api_row = api_by_id.get(call_id)
            if api_row is None:
                continue
            if (
                api_row.get("method") != method
                or api_row.get("sample") != sample
                or api_row.get("rt_index") != rt
                or api_row.get("direction") != direction
            ):
                raise AnalysisError(
                    f"result/API call identity mismatch: {cell!r}, {call_id}"
                )
    duplicate_references = sorted(
        call_id for call_id, count in result_reference_counts.items() if count > 1
    )
    if duplicate_references:
        raise AnalysisError(
            "API calls are referenced by multiple result cells: "
            + ", ".join(duplicate_references[:5])
        )
    result_references = set(result_reference_counts)
    if result_references != api_ids:
        problems.append(
            "result/api_calls call-id mismatch "
            f"(results_only={len(result_references - api_ids)}, "
            f"calls_only={len(api_ids - result_references)})"
        )

    by_method = {method: _usage_accumulator() for method in METHODS}
    by_method_call_kind: dict[str, dict[str, dict[str, Any]]] = {
        method: {} for method in METHODS
    }
    journal_ids: set[str] = set()
    journal_files = sorted(journal_dir.glob("*.response.json")) if journal_dir.is_dir() else []
    for path in journal_files:
        document = _read_json(path)
        call_id = document.get("call_id")
        if not isinstance(call_id, str) or not call_id:
            raise AnalysisError(f"API journal lacks call_id: {path}")
        if call_id in journal_ids:
            raise AnalysisError(f"duplicate API journal call_id: {call_id}")
        journal_ids.add(call_id)
        result = document.get("result")
        if not isinstance(result, dict):
            raise AnalysisError(f"API journal lacks result object: {path}")
        semantic = dict(result)
        semantic.setdefault("semantic_call_id", document.get("semantic_call_id"))
        method = _method_from_semantic(semantic)
        if method not in METHODS:
            raise AnalysisError(f"cannot map API journal to a method: {path}")
        kind = _call_kind(semantic)
        acc = by_method_call_kind[method].setdefault(kind, _usage_accumulator())
        _add_usage(acc, result)
        _add_usage(by_method[method], result)

    if api_ids != journal_ids:
        problems.append(
            "api_calls/api_journal call-id mismatch "
            f"(calls_only={len(api_ids - journal_ids)}, "
            f"journals_only={len(journal_ids - api_ids)})"
        )

    failure_backed_cells = []
    for cell, call_ids in sorted(result_cell_call_ids.items()):
        method, sample, rt, direction = cell
        primary_kind = f"{method}_primary"
        primary_ids = [
            call_id
            for call_id in call_ids
            if _call_kind(api_by_id.get(call_id) or {}) == primary_kind
        ]
        successful_primary_ids = [
            call_id for call_id in primary_ids if call_id in journal_ids
        ]
        if not successful_primary_ids:
            failure_backed_cells.append({
                "method": method,
                "sample_id": sample,
                "round_trip": rt,
                "direction": direction,
                "referenced_call_ids": call_ids,
                "primary_call_ids": primary_ids,
            })
    if failure_backed_cells:
        problems.append(
            "committed result cells lack a successful response-journal-backed "
            f"primary semantic call ({len(failure_backed_cells)} cells)"
        )

    unknown_usage: list[dict[str, Any]] = []
    for line_number, row in enumerate(ledger, 1):
        if row.get("event") != "attempt_end" or row.get("final_usage_seen") is not False:
            continue
        method = _method_from_semantic(row) or "unknown"
        kind = _call_kind(row)
        unknown_usage.append(
            {
                "ledger_line": line_number,
                "call_id": row.get("call_id"),
                "semantic_call_id": row.get("semantic_call_id"),
                "method": method,
                "call_kind": kind,
                "error_type": row.get("error_type"),
                "generation_delta_seen": bool(row.get("generation_delta_seen")),
                "status": row.get("status"),
            }
        )
    unknown_by_method = Counter(item["method"] for item in unknown_usage)
    unknown_by_kind = Counter(item["call_kind"] for item in unknown_usage)
    unknown_by_error = Counter(str(item["error_type"]) for item in unknown_usage)

    return (
        {
            "api_calls": len(api_calls),
            "result_referenced_api_calls": len(result_references),
            "api_call_kinds": dict(sorted(call_kinds.items())),
            "api_journals": len(journal_files),
            "failure_backed_committed_cells": failure_backed_cells,
            "usage_is_known_committed_semantic_call_usage": True,
            "failed_attempt_usage_is_not_imputed": True,
            "by_method": by_method,
            "by_method_call_kind": {
                method: dict(sorted(kinds.items()))
                for method, kinds in by_method_call_kind.items()
            },
            "unknown_final_usage_attempts": {
                "count": len(unknown_usage),
                "generation_started_count": sum(
                    item["generation_delta_seen"] for item in unknown_usage
                ),
                "by_method": dict(sorted(unknown_by_method.items())),
                "by_call_kind": dict(sorted(unknown_by_kind.items())),
                "by_error_type": dict(sorted(unknown_by_error.items())),
                "attempts": unknown_usage,
            },
        },
        problems,
    )


def _result_usage(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result = {method: _usage_accumulator() for method in METHODS}
    for row in rows:
        method = row.get("method")
        if method in result:
            _add_usage(result[method], row)
    return result


def _hybrid_telemetry(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    hp_rows = [row for row in rows if row.get("method") == "hybridpatch"]
    routes: Counter[str] = Counter()
    profiles: Counter[str] = Counter()
    repair_attempted = repair_used = repair_success = 0
    protocol_failures = invalid_json = schema_errors = 0
    telemetry_steps = 0
    preservation_violations = 0
    preservation_steps_with_violation = 0
    preservation_rates: list[float] = []
    for row in hp_rows:
        bdpatch = row.get("bdpatch") or {}
        if not isinstance(bdpatch, dict):
            raise AnalysisError("HybridPatch bdpatch telemetry must be an object")
        violations = bdpatch.get("preservation_violations") or 0
        if not _finite_number(violations) or float(violations) < 0:
            raise AnalysisError("invalid preservation_violations telemetry")
        preservation_violations += int(violations)
        preservation_steps_with_violation += bool(violations)
        rate = bdpatch.get("preservation_rate")
        if _finite_number(rate):
            preservation_rates.append(float(rate))

        hybrid = bdpatch.get("hybrid") or {}
        if not hybrid:
            continue
        if not isinstance(hybrid, dict):
            raise AnalysisError("HybridPatch hybrid telemetry must be an object")
        telemetry_steps += 1
        routes[str(hybrid.get("route") or bdpatch.get("actual_method") or "unknown")] += 1
        profiles[str(hybrid.get("prompt_profile") or "unknown")] += 1
        repair = hybrid.get("repair") or {}
        if not isinstance(repair, dict):
            raise AnalysisError("HybridPatch repair telemetry must be an object")
        repair_attempted += bool(repair.get("attempted"))
        repair_used += bool(repair.get("used"))
        repair_success += bool(repair.get("success"))
        protocol_failures += bool(hybrid.get("failed_step_kept_context"))
        invalid_json += bool(hybrid.get("invalid_json"))
        schema_errors += bool(hybrid.get("schema_error_count"))

    return {
        "result_steps": len(hp_rows),
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


def _sample_outcomes(experiment_dir: Path, samples: list[str]) -> dict[str, Any]:
    rows = _read_jsonl(experiment_dir / "sample_outcomes.jsonl", required=False)
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        sample = row.get("sample")
        if sample in samples:
            latest[sample] = row
    return {
        "latest_status": {
            sample: (latest.get(sample) or {}).get("status")
            for sample in samples
        },
        "not_finished": [
            sample
            for sample in samples
            if latest.get(sample) and latest[sample].get("status") != "finished"
        ],
        "missing_outcome": [sample for sample in samples if sample not in latest],
    }


def analyze_campaign(
    experiment_dir: Path | str,
    *,
    allow_incomplete: bool = False,
    sensitivity_exclude: Iterable[str] = (),
    repository_root: Path | str = ROOT,
) -> dict[str, Any]:
    experiment_dir = Path(experiment_dir).resolve()
    repository_root = Path(repository_root).resolve()
    if not experiment_dir.is_dir():
        raise AnalysisError(f"experiment directory does not exist: {experiment_dir}")
    manifest, samples, round_trips = _manifest_scope(experiment_dir)
    confirmation_identity = _confirmation_identity(
        manifest, samples, round_trips, repository_root=repository_root
    )
    rows_by_cell, scores, missing_cells = _load_result_rows(
        experiment_dir, samples, round_trips
    )
    evaluation_errors = _committed_evaluation_errors(rows_by_cell)
    rt_rows = _round_trip_table(scores, samples, round_trips)
    endpoint_rows, endpoint_stats = _sample_endpoint(
        scores, samples, round_trips
    )
    api, api_problems = _api_analysis(
        experiment_dir, samples, round_trips, rows_by_cell
    )
    sample_outcomes = _sample_outcomes(experiment_dir, samples)
    outcome_incomplete = sorted(
        set(sample_outcomes["not_finished"])
        | set(sample_outcomes["missing_outcome"])
    )
    incomplete_samples = sorted(
        {cell["sample_id"] for cell in missing_cells}
        | {
            row["sample_id"]
            for row in endpoint_rows
            if not row["complete"]
        }
        | set(outcome_incomplete)
    )
    fixed_n_ok = all(
        row["paired_n"] == len(samples) and row["complete"] for row in rt_rows
    )
    structural_complete = (
        not missing_cells
        and fixed_n_ok
        and not api_problems
        and not outcome_incomplete
    )
    telemetry = _hybrid_telemetry(rows_by_cell.values())
    if confirmation_identity["applicable"]:
        postrun_verification = _postrun_verification(
            experiment_dir,
            expected_backward_rows=len(samples) * len(METHODS) * round_trips,
        )
    else:
        postrun_verification = {
            "applicable": False,
            "valid": False,
            "problems": ["post-run proof is only defined for the frozen confirmation"],
        }
    formal_reporting_ready = (
        structural_complete
        and confirmation_identity["applicable"]
        and confirmation_identity["valid"]
        and postrun_verification["valid"]
        and telemetry["preservation"]["violations"] == 0
    )
    sensitivity_excluded = sorted(set(sensitivity_exclude))
    unknown_exclusions = sorted(set(sensitivity_excluded) - set(samples))
    if unknown_exclusions:
        raise AnalysisError(
            "sensitivity exclusion is outside manifest scope: "
            + ", ".join(unknown_exclusions)
        )
    if (
        confirmation_identity["applicable"]
        and sensitivity_excluded
        and (
            confirmation_identity.get("experiment_id")
            != CONFIRMATION_EXPERIMENT_ID
            or frozenset(sensitivity_excluded)
            not in CONFIRMATION_SENSITIVITY_SETS
        )
    ):
        raise AnalysisError(
            "sensitivity exclusion is not one of the pre-registered sets: "
            + ", ".join(sensitivity_excluded)
        )
    sensitivity = None
    if sensitivity_excluded:
        included = [
            sample for sample in samples if sample not in set(sensitivity_excluded)
        ]
        sensitivity_rt = _round_trip_table(scores, included, round_trips)
        sensitivity_rows, sensitivity_endpoint = _sample_endpoint(
            scores, included, round_trips
        )
        sensitivity = {
            "label": "pre_registered_evaluator_validity_sensitivity",
            "headline_replacement": False,
            "excluded_samples": sensitivity_excluded,
            "included_samples": included,
            "fixed_n": len(included),
            "complete": all(
                row["paired_n"] == len(included) and row["complete"]
                for row in sensitivity_rt
            ),
            "round_trip_scores": sensitivity_rt,
            "rt_final_by_sample": sensitivity_rows,
            "rt_final_summary": sensitivity_endpoint,
            "critical_failure_at_0_10": {
                method: _critical_failures(
                    scores, included, round_trips, method
                )
                for method in METHODS
            },
        }

    cohort_reports = {}
    sample_to_cohort = {}
    for cohort, cohort_samples in (
            confirmation_identity.get("cohort_samples") or {}).items():
        sample_to_cohort.update({sample: cohort for sample in cohort_samples})
        cohort_rt = _round_trip_table(scores, cohort_samples, round_trips)
        cohort_rows, cohort_endpoint = _sample_endpoint(
            scores, cohort_samples, round_trips)
        cohort_reports[cohort] = {
            "sample_ids": cohort_samples,
            "fixed_n": len(cohort_samples),
            "complete": all(
                row["paired_n"] == len(cohort_samples) and row["complete"]
                for row in cohort_rt
            ),
            "round_trip_scores": cohort_rt,
            "rt_final_by_sample": cohort_rows,
            "rt_final_summary": cohort_endpoint,
            "critical_failure_at_0_10": {
                method: _critical_failures(
                    scores, cohort_samples, round_trips, method)
                for method in METHODS
            },
        }
    for row in endpoint_rows:
        row["cohort"] = sample_to_cohort.get(row["sample_id"])

    result = {
        "schema": SCHEMA,
        "experiment_dir": experiment_dir.as_posix(),
        "experiment_id": manifest.get("experiment_id") or experiment_dir.name,
        "source_manifest_schema": manifest.get("schema"),
        "score_storage_scale": "raw_0_1_unchanged",
        "display_scale": "percent_and_percentage_points",
        "manifest_scope": {
            "selected_samples": samples,
            "selected_sample_count": len(samples),
            "round_trips": round_trips,
            "methods": list(METHODS),
            "seed": (manifest.get("config") or {}).get("seed"),
            "model": (manifest.get("config") or {}).get("model"),
            "transport_revision": (manifest.get("config") or {}).get(
                "transport_revision"
            ),
        },
        "integrity": {
            "complete": structural_complete,
            "structural_complete": structural_complete,
            "formal_reporting_ready": formal_reporting_ready,
            "confirmation_identity": confirmation_identity,
            "postrun_verification": postrun_verification,
            "fixed_n_required": len(samples),
            "fixed_n_satisfied_at_every_round_trip": fixed_n_ok,
            "expected_result_cells": len(samples) * len(METHODS) * round_trips * 2,
            "observed_result_cells": len(rows_by_cell),
            "missing_result_cells": missing_cells,
            "incomplete_samples": incomplete_samples,
            "unfinished_or_missing_sample_outcomes": outcome_incomplete,
            "api_integrity_problems": api_problems,
            "committed_evaluation_errors": evaluation_errors,
        },
        "round_trip_scores": rt_rows,
        "rt_final_by_sample": endpoint_rows,
        "rt_final_summary": endpoint_stats,
        "pre_registered_sensitivity": sensitivity,
        "pre_registered_cohorts": cohort_reports,
        "critical_failure_at_0_10": {
            method: _critical_failures(scores, samples, round_trips, method)
            for method in METHODS
        },
        "hybridpatch_telemetry": telemetry,
        "usage": {
            "result_rows_by_method": _result_usage(rows_by_cell.values()),
            "api_semantic_calls": api,
        },
        "sample_outcomes": sample_outcomes,
    }
    if not structural_complete and not allow_incomplete:
        raise CampaignIncompleteError(
            "manifest-selected campaign is incomplete: "
            f"missing_result_cells={len(missing_cells)}, "
            f"fixed_n_ok={fixed_n_ok}, "
            f"outcome_incomplete={outcome_incomplete or 'none'}, "
            f"api_problems={api_problems or 'none'}"
        )
    if (
        confirmation_identity["applicable"]
        and not formal_reporting_ready
        and not allow_incomplete
    ):
        raise CampaignIncompleteError(
            "frozen confirmation is not formally reportable: "
            f"identity_problems={confirmation_identity['problems'] or 'none'}, "
            f"postrun_problems={postrun_verification['problems'] or 'none'}, "
            "preservation_violations="
            f"{telemetry['preservation']['violations']}"
        )
    return result


def _fmt(value: Any, digits: int = 3, *, signed: bool = False) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:+.{digits}f}" if signed else f"{value:.{digits}f}"
    return str(value)


def render_markdown(report: dict[str, Any]) -> str:
    scope = report["manifest_scope"]
    integrity = report["integrity"]
    endpoint = report["rt_final_summary"]
    raw = endpoint["raw_0_1"]
    display = endpoint["display_percent_and_percentage_points"]
    wlt = endpoint["win_loss_tie"]
    concentration = endpoint["difference_concentration"]
    inference = endpoint["paired_inference"]
    lines = [
        f"# {report['experiment_id']} — paired confirmation report",
        "",
        "> Scores below are presentation-only percentages; source result rows remain "
        "unchanged on the 0–1 scale. Differences are percentage points.",
        "",
        "## Completeness",
        "",
        f"- Formal reporting ready: **{str(integrity['formal_reporting_ready']).lower()}**",
        f"- Structural grid/API completeness: "
        f"{str(integrity['structural_complete']).lower()}",
        f"- Frozen confirmation identity valid: "
        f"{str(integrity['confirmation_identity']['valid']).lower()} "
        f"({'; '.join(integrity['confirmation_identity']['problems']) or 'no problems'}).",
        f"- Post-run verifier and strict inspection valid: "
        f"{str(integrity['postrun_verification']['valid']).lower()} "
        f"({'; '.join(integrity['postrun_verification']['problems']) or 'no problems'}).",
        f"- Manifest-selected samples: {scope['selected_sample_count']}",
        f"- Result cells: {integrity['observed_result_cells']}/{integrity['expected_result_cells']}",
        f"- Fixed n at every RT: {str(integrity['fixed_n_satisfied_at_every_round_trip']).lower()}",
        f"- Incomplete samples: {', '.join(integrity['incomplete_samples']) or 'none'}",
        f"- API integrity problems: {', '.join(integrity['api_integrity_problems']) or 'none'}",
        f"- Failure-backed committed result cells: "
        f"{len(report['usage']['api_semantic_calls']['failure_backed_committed_cells'])}.",
        f"- Committed evaluation-error cells (frozen score=0 policy): "
        f"{integrity['committed_evaluation_errors']['count']} "
        f"({integrity['committed_evaluation_errors']['by_method']}).",
        "",
        "## RT trajectory",
        "",
        "| RT | fixed n | paired n | HP_V8 (%) | FullRewrite (%) | Δ (pp) |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["round_trip_scores"]:
        lines.append(
            f"| RT{row['round_trip']} | {row['fixed_n']} | {row['paired_n']} | "
            f"{_fmt(row['hybridpatch_percent'])} | "
            f"{_fmt(row['fullrewrite_percent'])} | "
            f"{_fmt(row['delta_percentage_points'], signed=True)} |"
        )

    final_rt = scope["round_trips"]
    lines.extend(
        [
            "",
            f"## RT{final_rt} by sample",
            "",
            "| sample | cohort | HP_V8 (%) | FullRewrite (%) | Δ (pp) | complete |",
            "|---|---|---:|---:|---:|:---:|",
        ]
    )
    for row in report["rt_final_by_sample"]:
        lines.append(
            f"| {row['sample_id']} | {row.get('cohort') or 'all'} | "
            f"{_fmt(row['hybridpatch_percent'])} | "
            f"{_fmt(row['fullrewrite_percent'])} | "
            f"{_fmt(row['delta_percentage_points'], signed=True)} | "
            f"{str(row['complete']).lower()} |"
        )

    lines.extend(
        [
            "",
            f"## RT{final_rt} paired summary",
            "",
            "| statistic | HP_V8 | FullRewrite | Δ |",
            "|---|---:|---:|---:|",
            f"| mean (%) / pp | {_fmt(display['hybridpatch_mean'])} | "
            f"{_fmt(display['fullrewrite_mean'])} | "
            f"{_fmt(display['delta_mean'], signed=True)} |",
            f"| median (%) / pp | {_fmt(display['hybridpatch_median'])} | "
            f"{_fmt(display['fullrewrite_median'])} | "
            f"{_fmt(display['delta_median'], signed=True)} |",
            "",
            f"- W/L/T for HP_V8: {wlt['hybridpatch_wins']}/"
            f"{wlt['hybridpatch_losses']}/{wlt['ties']}.",
            f"- Sample SD of Δ: {_fmt(display['sample_delta_sd'])} pp.",
            f"- Δ IQR: {_fmt(display['delta_iqr'])} pp "
            f"(P25={_fmt(display['delta_p25_nearest_rank'], signed=True)}, "
            f"P75={_fmt(display['delta_p75_nearest_rank'], signed=True)}; nearest-rank).",
            f"- Δ MAD: {_fmt(display['delta_mad'])} pp.",
            f"- |Δ| ≤ 5 pp: {concentration['absolute_delta_le_5pp_count']}/"
            f"{raw['paired_n']} ({_fmt(_scaled(concentration['absolute_delta_le_5pp_share']))}%).",
            f"- |Δ| ≤ 10 pp: {concentration['absolute_delta_le_10pp_count']}/"
            f"{raw['paired_n']} ({_fmt(_scaled(concentration['absolute_delta_le_10pp_share']))}%).",
            f"- Paired mean Δ bootstrap 95% CI (seed "
            f"{inference['bootstrap_seed']}, {inference['bootstrap_resamples']} resamples): "
            f"[{_fmt(inference['mean_delta_ci95_percentage_points'][0], signed=True)}, "
            f"{_fmt(inference['mean_delta_ci95_percentage_points'][1], signed=True)}] pp.",
            f"- Exact two-sided sign test (ties omitted, n="
            f"{inference['sign_test_non_ties']}): p="
            f"{_fmt(inference['sign_test_two_sided_p'], digits=6)}.",
            "",
            "## CriticalFailure@0.10",
            "",
            "| method | failures | eligible adjacent transitions | rate |",
            "|---|---:|---:|---:|",
        ]
    )
    for method in METHODS:
        cf = report["critical_failure_at_0_10"][method]
        lines.append(
            f"| {METHOD_LABELS[method]} | {cf['count']} | "
            f"{cf['eligible_transitions']} | {_fmt(_scaled(cf['rate']))}% |"
        )

    sensitivity = report.get("pre_registered_sensitivity")
    if sensitivity:
        sensitivity_display = sensitivity["rt_final_summary"][
            "display_percent_and_percentage_points"
        ]
        sensitivity_wlt = sensitivity["rt_final_summary"]["win_loss_tie"]
        lines.extend(
            [
                "",
                "## Pre-registered evaluator-validity sensitivity",
                "",
                "> This view is descriptive and does not replace the fixed-n headline.",
                "",
                f"- Excluded before results were observed: "
                f"{', '.join(sensitivity['excluded_samples'])}.",
                f"- Fixed n: {sensitivity['fixed_n']}; complete: "
                f"{str(sensitivity['complete']).lower()}.",
                f"- RT{scope['round_trips']} HP_V8 / FullRewrite / delta: "
                f"{_fmt(sensitivity_display['hybridpatch_mean'])}% / "
                f"{_fmt(sensitivity_display['fullrewrite_mean'])}% / "
                f"{_fmt(sensitivity_display['delta_mean'], signed=True)} pp.",
                f"- W/L/T: {sensitivity_wlt['hybridpatch_wins']}/"
                f"{sensitivity_wlt['hybridpatch_losses']}/"
                f"{sensitivity_wlt['ties']}.",
            ]
        )

    for cohort, cohort_report in report.get(
            "pre_registered_cohorts", {}).items():
        cohort_display = cohort_report["rt_final_summary"][
            "display_percent_and_percentage_points"]
        cohort_wlt = cohort_report["rt_final_summary"]["win_loss_tie"]
        lines.extend([
            "",
            f"## Pre-registered cohort: {cohort}",
            "",
            f"- Fixed n: {cohort_report['fixed_n']}; complete: "
            f"{str(cohort_report['complete']).lower()}.",
            f"- RT{scope['round_trips']} HP_V8 / FullRewrite / delta: "
            f"{_fmt(cohort_display['hybridpatch_mean'])}% / "
            f"{_fmt(cohort_display['fullrewrite_mean'])}% / "
            f"{_fmt(cohort_display['delta_mean'], signed=True)} pp.",
            f"- W/L/T: {cohort_wlt['hybridpatch_wins']}/"
            f"{cohort_wlt['hybridpatch_losses']}/{cohort_wlt['ties']}.",
            "",
            "| RT | fixed n | HP_V8 (%) | FullRewrite (%) | Δ (pp) |",
            "|---:|---:|---:|---:|---:|",
        ])
        for row in cohort_report["round_trip_scores"]:
            lines.append(
                f"| RT{row['round_trip']} | {row['fixed_n']} | "
                f"{_fmt(row['hybridpatch_percent'])} | "
                f"{_fmt(row['fullrewrite_percent'])} | "
                f"{_fmt(row['delta_percentage_points'], signed=True)} |"
            )

    telemetry = report["hybridpatch_telemetry"]
    repair = telemetry["repair"]
    protocol = telemetry["protocol_failure"]
    preservation = telemetry["preservation"]
    lines.extend(
        [
            "",
            "## HybridPatch telemetry",
            "",
            "- Routes: "
            + (", ".join(f"`{key}`={value}" for key, value in telemetry["routes"].items()) or "none"),
            f"- Repair: attempted={repair['attempted']}, used={repair['used']}, "
            f"success={repair['success']}, rate={_fmt(_scaled(repair['rate']))}%.",
            f"- Protocol failure (`{protocol['definition']}`): "
            f"{protocol['steps']}/{telemetry['telemetry_steps']} "
            f"({_fmt(_scaled(protocol['rate']))}%).",
            f"- Preservation violations: **{preservation['violations']}**; "
            f"ok={str(preservation['ok']).lower()}.",
            "",
            "## Tokens and known USD",
            "",
            "> Journal totals cover committed semantic calls. Failed attempts without final "
            "usage are listed separately and are not imputed into tokens or cost.",
            "",
            "| method | call kind | calls | total tokens | output tokens | known USD | USD rows |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    calls = report["usage"]["api_semantic_calls"]
    for method in METHODS:
        for kind, usage in calls["by_method_call_kind"][method].items():
            lines.append(
                f"| {METHOD_LABELS[method]} | `{kind}` | {usage['calls_or_rows']} | "
                f"{usage['total_tokens']} | {usage['output_tokens']} | "
                f"{usage['total_usd']:.6f} | "
                f"{usage['rows_with_total_usd']}/{usage['calls_or_rows']} |"
            )
    unknown = calls["unknown_final_usage_attempts"]
    lines.extend(
        [
            "",
            f"- Unknown-final-usage attempts: {unknown['count']} "
            f"(generation started: {unknown['generation_started_count']}).",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_prefix: Path | str) -> tuple[Path, Path]:
    prefix = Path(output_prefix)
    if prefix.suffix in {".json", ".md"}:
        prefix = prefix.with_suffix("")
    json_path = Path(str(prefix) + ".json")
    markdown_path = Path(str(prefix) + ".md")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    document = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report) + "\n"
    json_tmp = json_path.with_suffix(json_path.suffix + ".tmp")
    markdown_tmp = markdown_path.with_suffix(markdown_path.suffix + ".tmp")
    json_tmp.write_text(document, encoding="utf-8")
    markdown_tmp.write_text(markdown, encoding="utf-8")
    json_tmp.replace(json_path)
    markdown_tmp.replace(markdown_path)
    return json_path, markdown_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a strict zero-API paired confirmation report."
    )
    parser.add_argument("--experiment-dir", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help=(
            "emit an explicitly non-formal diagnostic report with null/missing cells; "
            "the default rejects incomplete selected scope"
        ),
    )
    parser.add_argument(
        "--sensitivity-exclude",
        nargs="*",
        default=(),
        metavar="SAMPLE_ID",
        help=(
            "pre-registered evaluator-validity exclusions for a labeled "
            "secondary view; never changes the headline manifest scope"
        ),
    )
    args = parser.parse_args(argv)
    try:
        report = analyze_campaign(
            args.experiment_dir,
            allow_incomplete=args.allow_incomplete,
            sensitivity_exclude=args.sensitivity_exclude,
        )
        json_path, markdown_path = write_report(report, args.output_prefix)
    except AnalysisError as exc:
        parser.exit(2, f"analysis failed: {exc}\n")
    print(f"wrote {json_path}")
    print(f"wrote {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
