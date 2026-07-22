#!/usr/bin/env python3
"""Build the authorized mixed HP_V8 confirmation100 split without API calls.

The fixed design contains 60 samples unseen to HP method development/HP
experiments and 40 historically HP-exposed samples that are absent from the
two immediately preceding paired10 and supplement40 campaigns.  Selection is
deterministic at seed 42 and the output is written with LF bytes on Windows.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


SCHEMA = "anchorpatch.mixed_confirmation_split/1"
EXPERIMENT_ID = "exp_20260719_hybridv8_transportv4_mixed_confirmation100"
SEED = 42
UNSEEN_SELECTED_COUNT = 60
HISTORICAL_SELECTED_COUNT = 40
RECENT_CAMPAIGNS = (
    "HP_V8/exp_20260718_hybridv8_transportv4_snapshotfix_paired10",
    "HP_V8/exp_20260719_hybridv8_transportv4_supplement40_lockfix",
)
SOURCE_LEVEL_HP_EXPOSURE = {
    "json1": "splitter real-sample self-test",
    "molecule1": "splitter real-sample self-test",
    "obj3d1": "domain evaluator source self-test reference",
    "starcatalog1": "domain evaluator source self-test reference",
}
DEFAULT_OUTPUT_PREFIX = (
    Path("HP_V8") / "analysis" / EXPERIMENT_ID.removeprefix("exp_")
    / "selection"
)


def _load_base(repo: Path):
    path = repo / "tools" / "build_unseen_confirmation_split.py"
    spec = importlib.util.spec_from_file_location("_hp_v8_unseen_selector", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import base selector: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


def _sample_content_manifest(repo: Path, sample_ids: list[str]) -> str:
    root = repo / "data" / "samples_delegate52"
    entries: dict[str, str] = {}
    for sample_id in sample_ids:
        folder = root / sample_id
        for path in sorted(item for item in folder.rglob("*") if item.is_file()):
            relative = path.relative_to(repo).as_posix()
            entries[relative] = _sha256(path)
    return hashlib.sha256(_canonical(entries)).hexdigest()


def _select_complement(base, candidates, features, selected_count, seed):
    reserve_count = len(candidates) - selected_count
    if reserve_count < 0:
        raise RuntimeError("requested selection exceeds candidate count")
    reserve, reserve_order, tie_rank = base.stratified_reserve(
        candidates, features, reserve_count, seed
    )
    selected = sorted(set(candidates) - set(reserve))
    if len(selected) != selected_count:
        raise RuntimeError("deterministic selection count mismatch")
    return selected, reserve, reserve_order, tie_rank


@contextlib.contextmanager
def _exclude_current_experiment(base, excluded_dir: Path | None):
    if excluded_dir is None:
        yield
        return
    original = base._walk_experiment_paths

    def filtered(repo):
        for path in original(repo):
            try:
                path.resolve().relative_to(excluded_dir)
            except ValueError:
                yield path

    base._walk_experiment_paths = filtered
    try:
        yield
    finally:
        base._walk_experiment_paths = original


def build_split(
    repo: Path,
    *,
    skip_runtime_smoke: bool = False,
    exclude_experiment_dir: Path | None = None,
) -> dict[str, Any]:
    base = _load_base(repo)
    selector_script = Path(__file__).resolve()
    base_script = repo / "tools" / "build_unseen_confirmation_split.py"
    samples_root = repo / "data" / "samples_delegate52"
    registry_path = repo / "data" / "CONTAMINATION_REGISTRY.json"
    split_path = repo / "data" / "hybrid_split.json"
    registry = _read_json(registry_path)
    hybrid_split = _read_json(split_path)
    all_ids = base.sample_ids(samples_root)
    all_set = set(all_ids)
    registry_sets = base.load_registry_sets(registry)
    split_sets = base.split_sets(hybrid_split)
    excluded_dir = (
        exclude_experiment_dir.resolve()
        if exclude_experiment_dir is not None
        else None
    )
    with _exclude_current_experiment(base, excluded_dir):
        strict_exposure = base.scan_any_provider_exposure(repo, all_set)
        actual_method = base.scan_method_exposure(repo, all_set)
    base_unseen, base_exposed, _clean_method_overlap = base.method_holdout_candidates(
        all_set, registry_sets, split_sets, actual_method["sample_ids"]
    )

    extra_source_exposure = set(SOURCE_LEVEL_HP_EXPOSURE)
    method_unseen_pool = set(base_unseen) - extra_source_exposure
    method_exposed_pool = set(base_exposed) | extra_source_exposure
    if method_unseen_pool & method_exposed_pool:
        raise RuntimeError("method-unseen and method-exposed pools overlap")
    if method_unseen_pool | method_exposed_pool != all_set:
        raise RuntimeError("method exposure partition does not cover all samples")

    recent_by_campaign: dict[str, list[str]] = {}
    recent_union: set[str] = set()
    recent_manifest_inputs: dict[str, dict[str, Any]] = {}
    for relative in RECENT_CAMPAIGNS:
        manifest_path = repo / relative / "dispatch_manifest.json"
        manifest = _read_json(manifest_path)
        sample_values = (manifest.get("config") or {}).get("samples")
        if not isinstance(sample_values, list) or not all(
            isinstance(item, str) for item in sample_values
        ):
            raise RuntimeError(f"invalid recent campaign sample list: {manifest_path}")
        samples = sorted(set(sample_values))
        recent_by_campaign[relative] = samples
        recent_union.update(samples)
        recent_manifest_inputs[relative] = {
            "path": f"{relative}/dispatch_manifest.json",
            "sha256": _sha256(manifest_path),
            "sample_count": len(samples),
        }

    historical_pool = method_exposed_pool - recent_union
    actual_historical = set(actual_method["sample_ids"]) - recent_union
    if not actual_historical <= historical_pool:
        raise RuntimeError("actual HP history is outside the historical cohort pool")
    historical_remaining = historical_pool - actual_historical

    runtime = (
        base.runtime_runnability(repo, all_ids)
        if not skip_runtime_smoke
        else {
            "checked_count": 0,
            "runnable_count": len(all_ids),
            "failed_count": 0,
            "runnable_sample_ids": all_ids,
            "failed": {},
            "nonunit_reference_scores": {},
            "note": "runtime smoke skipped for a zero-API debug run",
        }
    )
    runnable = set(runtime["runnable_sample_ids"])
    unseen_candidates = sorted(method_unseen_pool & runnable)
    historical_candidates = sorted(historical_pool & runnable)
    actual_historical = set(actual_historical) & runnable
    historical_remaining = sorted(historical_remaining & runnable)
    if len(actual_historical) > HISTORICAL_SELECTED_COUNT:
        raise RuntimeError("actual HP historical mandatory set exceeds cohort size")

    unseen_features = {
        sample_id: base.sample_feature(repo, sample_id)
        for sample_id in unseen_candidates
    }
    base.add_length_bins(unseen_features)
    unseen_selected, unseen_reserve, unseen_reserve_order, unseen_tie_rank = (
        _select_complement(
            base,
            unseen_candidates,
            unseen_features,
            UNSEEN_SELECTED_COUNT,
            SEED,
        )
    )

    remaining_features = {
        sample_id: base.sample_feature(repo, sample_id)
        for sample_id in historical_remaining
    }
    base.add_length_bins(remaining_features)
    additional_count = HISTORICAL_SELECTED_COUNT - len(actual_historical)
    historical_additional, historical_remaining_reserve, historical_order, historical_rank = (
        _select_complement(
            base,
            historical_remaining,
            remaining_features,
            additional_count,
            SEED,
        )
    )
    historical_selected = sorted(actual_historical | set(historical_additional))
    historical_reserve = sorted(set(historical_candidates) - set(historical_selected))
    selected = sorted(set(unseen_selected) | set(historical_selected))
    candidates = sorted(set(unseen_candidates) | set(historical_candidates))
    reserve = sorted(set(candidates) - set(selected))
    if len(selected) != 100 or set(unseen_selected) & set(historical_selected):
        raise RuntimeError("mixed confirmation selection is not exactly 60+40")
    if set(selected) & recent_union:
        raise RuntimeError("selected cohort overlaps the two recent campaigns")

    combined_features = {
        **unseen_features,
        **{
            sample_id: base.sample_feature(repo, sample_id)
            for sample_id in historical_candidates
        },
    }
    base.add_length_bins(combined_features)
    result = {
        "schema": SCHEMA,
        "experiment_id": EXPERIMENT_ID,
        "inputs": {
            "selector_script": {
                "path": selector_script.relative_to(repo).as_posix(),
                "sha256": _sha256(selector_script),
                "python_version": sys.version,
            },
            "base_selector_script": {
                "path": base_script.relative_to(repo).as_posix(),
                "sha256": _sha256(base_script),
            },
            "registry": {
                "path": registry_path.relative_to(repo).as_posix(),
                "sha256": _sha256(registry_path),
            },
            "hybrid_split": {
                "path": split_path.relative_to(repo).as_posix(),
                "sha256": _sha256(split_path),
            },
            "recent_campaign_manifests": recent_manifest_inputs,
            "sample_count": len(all_ids),
            "sample_content_manifest_sha256": _sample_content_manifest(repo, all_ids),
        },
        "authorization": {
            "scope": "60 method/developer-unseen plus 40 historical HP/method-exposed",
            "seed": SEED,
            "method_unseen_selected_count": UNSEEN_SELECTED_COUNT,
            "historical_selected_count": HISTORICAL_SELECTED_COUNT,
            "recent_campaigns_excluded": list(RECENT_CAMPAIGNS),
        },
        "exposure_audit": {
            "strict_any_provider_call": strict_exposure,
            "base_method_unseen_count": len(base_unseen),
            "source_level_hp_exposure": SOURCE_LEVEL_HP_EXPOSURE,
            "method_unseen_candidate_count": len(unseen_candidates),
            "method_exposed_candidate_count_after_recent_exclusion": len(
                historical_candidates
            ),
            "actual_hp_api_historical_mandatory_count": len(actual_historical),
            "actual_hp_api_historical_mandatory_ids": sorted(actual_historical),
            "recent_campaign_sample_count": len(recent_union),
            "recent_campaign_sample_ids": sorted(recent_union),
            "recent_campaign_samples": recent_by_campaign,
            "actual_method_scan_count": actual_method["sample_count"],
            "actual_method_scan_sample_ids": actual_method["sample_ids"],
            "actual_method_path_manifest_sha256": actual_method[
                "path_manifest_sha256"
            ],
        },
        "runtime_evaluator_smoke": runtime,
        "candidate_rule": {
            "seed": SEED,
            "rule": "authorized_mixed_60_method_unseen_plus_40_historical",
            "candidate_count": len(candidates),
            "selected_count": len(selected),
            "reserve_count": len(reserve),
            "method_unseen_candidate_count": len(unseen_candidates),
            "method_unseen_selected_count": len(unseen_selected),
            "historical_candidate_count": len(historical_candidates),
            "historical_selected_count": len(historical_selected),
        },
        "candidate_sample_ids": candidates,
        "selected_sample_ids": selected,
        "reserve_sample_ids": reserve,
        "cohorts": {
            "method_unseen": {
                "candidate_sample_ids": unseen_candidates,
                "selected_sample_ids": unseen_selected,
                "reserve_sample_ids": unseen_reserve,
                "reserve_selection_order": unseen_reserve_order,
                "tie_rank": unseen_tie_rank,
            },
            "historical_hp_method_exposed": {
                "candidate_sample_ids": historical_candidates,
                "mandatory_actual_hp_api_sample_ids": sorted(actual_historical),
                "stratified_additional_sample_ids": historical_additional,
                "selected_sample_ids": historical_selected,
                "reserve_sample_ids": historical_reserve,
                "reserve_selection_order_for_nonmandatory_pool": historical_order,
                "tie_rank_for_nonmandatory_pool": historical_rank,
            },
        },
        "features": {sample_id: combined_features[sample_id] for sample_id in candidates},
        "coverage": {
            "selected_all": base.coverage_summary(selected, combined_features),
            "method_unseen_selected": base.coverage_summary(
                unseen_selected, combined_features
            ),
            "historical_selected": base.coverage_summary(
                historical_selected, combined_features
            ),
        },
        "caveats": [
            "All 234 samples have prior provider exposure through the frozen FullRewrite baseline; this is not an absolute provider-unseen experiment.",
            "The 60-sample cohort is unseen only under the explicitly authorized HP method/developer exposure definition.",
            "Only 17 eligible historical samples have direct prior HP API evidence outside the recent 10+40; all 17 are included, and the remaining 23 are selected from documented method/development-exposed samples.",
            "The previous paired10 and supplement40 union is excluded from both cohorts.",
            "This selector performs zero API calls and observes no outcome from the new experiment.",
        ],
    }
    result["artifact_sha256_preview"] = hashlib.sha256(_canonical({
        "candidate_sample_ids": candidates,
        "selected_sample_ids": selected,
        "reserve_sample_ids": reserve,
        "cohorts": result["cohorts"],
        "features": result["features"],
    })).hexdigest()
    return result


def _render_markdown(result: dict[str, Any]) -> str:
    rule = result["candidate_rule"]
    cohorts = result["cohorts"]
    lines = [
        f"# {result['experiment_id']} selection",
        "",
        "Zero-API, outcome-blind mixed confirmation selection.",
        "",
        f"- Candidates: {rule['candidate_count']}; selected: {rule['selected_count']}; reserve: {rule['reserve_count']}.",
        f"- Method/developer-unseen: {rule['method_unseen_selected_count']}/{rule['method_unseen_candidate_count']} selected.",
        f"- Historical HP/method-exposed: {rule['historical_selected_count']}/{rule['historical_candidate_count']} selected.",
        f"- Direct prior HP API samples outside recent campaigns: {result['exposure_audit']['actual_hp_api_historical_mandatory_count']} (all included).",
        "",
        "## Selected 60 method/developer-unseen",
        "",
        "```text",
        " ".join(cohorts["method_unseen"]["selected_sample_ids"]),
        "```",
        "",
        "## Selected 40 historical HP/method-exposed",
        "",
        "```text",
        " ".join(cohorts["historical_hp_method_exposed"]["selected_sample_ids"]),
        "```",
        "",
        "## Combined selected100",
        "",
        "```text",
        " ".join(result["selected_sample_ids"]),
        "```",
        "",
        "## Caveats",
        "",
        *[f"- {item}" for item in result["caveats"]],
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--output-prefix", default=str(DEFAULT_OUTPUT_PREFIX))
    parser.add_argument("--skip-runtime-smoke", action="store_true")
    args = parser.parse_args(argv)
    repo = Path(args.repo).resolve()
    result = build_split(repo, skip_runtime_smoke=args.skip_runtime_smoke)
    prefix = (repo / args.output_prefix).resolve()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = prefix.with_suffix(".json")
    markdown_path = prefix.with_suffix(".md")
    json_path.write_bytes(
        (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    )
    markdown_path.write_bytes((_render_markdown(result) + "\n").encode("utf-8"))
    print(
        "PASS mixed confirmation split "
        f"candidates={result['candidate_rule']['candidate_count']} "
        f"selected={result['candidate_rule']['selected_count']} "
        "cohorts=60+40"
    )
    print(json_path.relative_to(repo).as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
