#!/usr/bin/env python3
"""Build the HP_V8 method-unseen confirmation split.

This is a zero-API audit/selection tool.  It never reads provider payload
contents; historical exposure is inferred from registry entries, result file
paths, task-plan paths, API ledger rows, and api_raw filenames only.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import math
import os
import random
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "anchorpatch.method_unseen_confirmation_split/1"
DEFAULT_EXPERIMENT_ID = (
    "exp_20260719_hybridv8_transportv4_method_unseen_confirmation68"
)
DEFAULT_OUTPUT_PREFIX = (
    Path("HP_V8")
    / "analysis"
    / f"{DEFAULT_EXPERIMENT_ID}_selection"
)
SEED = 42
DOCUMENTED_DEVELOPER_CONTENT_EXPOSURE = {
    "python1": "docs/FINDINGS.md:370-399",
}

_LOCAL_OP_TERMS = (
    "replace",
    "delete",
    "insert",
    "append",
    "prepend",
    "rename",
    "update",
    "change",
    "fix",
    "correct",
    "remove",
    "add",
)
_BULK_TERMS = (
    "all",
    "every",
    "each",
    "throughout",
    "globally",
    "replace all",
    "bulk",
)
_RESTRUCTURE_TERMS = (
    "split",
    "merge",
    "sort",
    "group",
    "reorder",
    "move",
    "relocate",
    "organize",
    "convert",
    "transform",
    "reformat",
    "reshape",
)
_GENERATIVE_TERMS = (
    "create",
    "generate",
    "draft",
    "write",
    "summarize",
    "expand",
    "new",
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def compact_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sample_ids(samples_root: Path) -> list[str]:
    return sorted(
        path.parent.name
        for path in samples_root.glob("*/sample.json")
        if path.parent.is_dir()
    )


def _walk_experiment_paths(repo: Path) -> Iterable[Path]:
    roots = [
        repo / "HP_V3",
        repo / "HP_V4",
        repo / "HP_V5",
        repo / "HP_V6",
        repo / "HP_V7",
        repo / "HP_V8",
        repo / "Baseline",
        repo / "transport",
        repo / "_attic",
    ]
    for root in roots:
        if not root.exists():
            continue
        yield from root.rglob("*")


def scan_any_provider_exposure(repo: Path, all_samples: set[str]) -> dict[str, Any]:
    """Return samples with any historical provider call evidence.

    The scan intentionally does not open request/response payload JSON files.
    """

    samples: set[str] = set()
    evidence: dict[str, list[str]] = defaultdict(list)
    api_call_files = 0
    api_raw_request_files = 0
    api_call_sources: list[dict[str, str]] = []
    api_raw_request_paths: list[str] = []

    for path in _walk_experiment_paths(repo):
        if not path.is_file():
            continue
        name = path.name
        rel = path.relative_to(repo).as_posix()
        if name == "api_calls.jsonl":
            api_call_files += 1
            api_call_sources.append({"path": rel, "sha256": sha256_file(path)})
            with path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    sample = row.get("sample") or row.get("sample_id")
                    if isinstance(sample, str) and sample in all_samples:
                        samples.add(sample)
                        if len(evidence[sample]) < 3:
                            evidence[sample].append(rel)
            continue
        if name.endswith(".request.json") and "api_raw" in path.parts:
            api_raw_request_files += 1
            api_raw_request_paths.append(rel)
            for part in path.parts:
                if part in all_samples:
                    samples.add(part)
                    if len(evidence[part]) < 3:
                        evidence[part].append(rel)
                    break

    return {
        "sample_ids": sorted(samples),
        "sample_count": len(samples),
        "evidence_examples": dict(sorted(evidence.items())),
        "api_call_files": api_call_files,
        "api_raw_request_files": api_raw_request_files,
        "evidence_manifest_sha256": hashlib.sha256(compact_json_bytes({
            "api_call_sources": sorted(api_call_sources, key=lambda item: item["path"]),
            "api_raw_request_paths": sorted(api_raw_request_paths),
        })).hexdigest(),
    }


def scan_method_exposure(repo: Path, all_samples: set[str]) -> dict[str, Any]:
    """Return samples historically exposed to HP/method development.

    FullRewrite-only frozen control evidence is deliberately not included in
    this set; that distinction is documented in the generated report.
    """

    samples: set[str] = set()
    evidence: dict[str, list[str]] = defaultdict(list)
    method_names = {"hybridpatch", "bdpatch", "anchorpatch"}
    matched_paths: set[str] = set()
    matched_api_call_sources: list[dict[str, str]] = []

    for path in _walk_experiment_paths(repo):
        if not path.is_file():
            continue
        rel = path.relative_to(repo).as_posix()
        lower_parts = {part.lower() for part in path.parts}
        if path.name == "api_calls.jsonl":
            path_matched = False
            with path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except (TypeError, json.JSONDecodeError):
                        continue
                    method = str(row.get("method") or "").casefold()
                    call_kind = str(row.get("call_kind") or "").casefold()
                    sample = row.get("sample") or row.get("sample_id")
                    if (
                        isinstance(sample, str)
                        and sample in all_samples
                        and (
                            method in method_names
                            or any(call_kind.startswith(name) for name in method_names)
                        )
                    ):
                        samples.add(sample)
                        path_matched = True
                        if len(evidence[sample]) < 3:
                            evidence[sample].append(rel)
            if path_matched:
                matched_paths.add(rel)
                matched_api_call_sources.append({
                    "path": rel,
                    "sha256": sha256_file(path),
                })
        if lower_parts & method_names:
            matched_paths.add(rel)
            if path.suffix == ".jsonl" and path.stem in all_samples:
                samples.add(path.stem)
                if len(evidence[path.stem]) < 3:
                    evidence[path.stem].append(rel)
            else:
                for part in path.parts:
                    if part in all_samples:
                        samples.add(part)
                        if len(evidence[part]) < 3:
                            evidence[part].append(rel)
                        break
        if path.name.endswith(".task_plan.json") and path.stem in all_samples:
            # Task plans alone are not method exposure; they are handled via
            # the registry/split policy.  Keep this explicit no-op here to make
            # the scan boundary visible.
            continue

    return {
        "sample_ids": sorted(samples),
        "sample_count": len(samples),
        "evidence_examples": dict(sorted(evidence.items())),
        "path_manifest_sha256": hashlib.sha256(
            compact_json_bytes({
                "matched_paths": sorted(matched_paths),
                "matched_api_call_sources": sorted(
                    matched_api_call_sources, key=lambda item: item["path"]
                ),
            })
        ).hexdigest(),
    }


def load_registry_sets(registry: dict[str, Any]) -> dict[str, set[str]]:
    entries = registry.get("entries") or []
    contaminated = {
        item["sample_id"]
        for item in entries
        if item.get("status") == "contaminated"
    }
    unsupported = {
        item["sample_id"]
        for item in entries
        if item.get("unavailable_reason") == "unsupported_generation_domain"
    }
    clean = {
        item["sample_id"]
        for item in entries
        if item.get("status") == "clean_candidate"
    }
    reserve = {
        item["sample_id"]
        for item in entries
        if item.get("status") == "reserved_holdout_candidate"
    }
    return {
        "contaminated": contaminated,
        "unsupported_generation_domain": unsupported,
        "clean_candidate": clean,
        "reserved_holdout_candidate": reserve,
    }


def split_sets(hybrid_split: dict[str, Any]) -> dict[str, set[str]]:
    splits = hybrid_split.get("splits") or {}
    return {
        name: set(values)
        for name, values in splits.items()
        if isinstance(values, list)
    }


def method_holdout_candidates(
        all_samples: set[str],
        registry_sets: dict[str, set[str]],
        split: dict[str, set[str]],
        scanned_method_exposure: Iterable[str],
) -> tuple[set[str], set[str], set[str]]:
    split_exposed = set().union(
        split.get("dev", set()),
        split.get("val", set()),
        split.get("test", set()),
        split.get("unused_reserve", set()),
    )
    registry_clean = set(registry_sets["clean_candidate"])
    documented_or_scanned_exposure = (
        split_exposed
        | set(scanned_method_exposure)
        | set(DOCUMENTED_DEVELOPER_CONTENT_EXPOSURE)
    )
    clean_with_method_exposure = registry_clean & documented_or_scanned_exposure
    pool = registry_clean - documented_or_scanned_exposure
    if not pool <= all_samples:
        raise RuntimeError(
            "contamination registry clean_candidate contains unknown samples: "
            f"{sorted(pool - all_samples)}"
        )
    return pool, all_samples - pool, clean_with_method_exposure


def classify_prompt(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.casefold())
    if any(term in normalized for term in _RESTRUCTURE_TERMS):
        return "global_restructure"
    if any(term in normalized for term in _BULK_TERMS):
        return "bulk_homogeneous"
    if any(term in normalized for term in _GENERATIVE_TERMS):
        return "generative"
    if any(term in normalized for term in _LOCAL_OP_TERMS):
        return "local_edit"
    return "other"


def sample_feature(repo: Path, sample_id: str) -> dict[str, Any]:
    sample_root = repo / "data" / "samples_delegate52" / sample_id
    sample = read_json(sample_root / "sample.json")
    states = sample.get("states") or []
    start_state_id = sample["start_state"]
    start_state = next(
        item for item in states if item.get("state_id") == start_state_id
    )
    filenames = list(start_state.get("context") or [])
    extensions = []
    total_bytes = 0
    for filename in filenames:
        ext = Path(filename).suffix.lower() or "[none]"
        extensions.append(ext)
        file_path = sample_root / start_state.get("solution_folder", start_state_id) / filename
        if file_path.exists():
            total_bytes += len(file_path.read_bytes())
    if total_bytes == 0:
        total_bytes = int(sample.get(f"{start_state_id}_num_chars") or 0)
    operations: set[str] = set()
    target_by_state = {
        state.get("state_id"): state
        for state in states
        if isinstance(state, dict)
    }
    for prompt in start_state.get("prompts") or []:
        target = target_by_state.get(prompt.get("target_state")) or {}
        semantic_operations = [
            str(op) for op in target.get("semantic_operations") or []
            if str(op)
        ]
        if semantic_operations:
            operations.update(semantic_operations)
        else:
            operations.add(classify_prompt(prompt.get("prompt") or ""))
    operations.discard("")
    if not operations:
        operations.add("unknown")
    file_count = len(filenames)
    return {
        "sample_id": sample_id,
        "domain": sample.get("sample_type"),
        "formats": sorted(set(extensions)) if extensions else ["[none]"],
        "format_group": "+".join(sorted(set(extensions))) if extensions else "[none]",
        "task_types": sorted(operations),
        "file_count": file_count,
        "file_count_bin": "1" if file_count <= 1 else "2" if file_count == 2 else "3+",
        "doc_bytes": total_bytes,
        "context_files": filenames,
    }


def add_length_bins(features: dict[str, dict[str, Any]]) -> None:
    ordered = sorted(
        features.values(),
        key=lambda item: (int(item["doc_bytes"]), item["sample_id"]),
    )
    n = len(ordered)
    if n == 0:
        return
    for index, item in enumerate(ordered):
        q = min(5, int(index * 5 / n) + 1)
        item["doc_length_bin"] = f"q{q}"


def feature_label_groups(feature: dict[str, Any]) -> dict[str, list[str]]:
    return {
        "domain": [f"domain={feature['domain']}"],
        "format": [f"format={name}" for name in feature["formats"]],
        "task_type": [f"task_type={name}" for name in feature["task_types"]],
        "file_count": [f"file_count={feature['file_count_bin']}"],
        "doc_length": [f"doc_length={feature['doc_length_bin']}"],
    }


def selection_counts(candidate_count: int) -> tuple[int, int, str]:
    if candidate_count >= 120:
        selected = 100
        reason = "candidate_count>=120_select100"
    elif candidate_count >= 100:
        selected = 80
        reason = "candidate_count_100_to_119_select80"
    else:
        selected = int(round(candidate_count * 0.8))
        if candidate_count and selected == candidate_count:
            selected -= 1
        reason = "candidate_count_below100_select_about80_percent"
    selected = max(0, min(selected, candidate_count))
    reserve = candidate_count - selected
    return selected, reserve, reason


def _label_counts_by_group(
        sample_list: Iterable[str],
        features: dict[str, dict[str, Any]]) -> dict[str, Counter[str]]:
    counts: dict[str, Counter[str]] = {
        "domain": Counter(),
        "format": Counter(),
        "task_type": Counter(),
        "file_count": Counter(),
        "doc_length": Counter(),
    }
    for sample_id in sample_list:
        for group, labels in feature_label_groups(features[sample_id]).items():
            counts[group].update(labels)
    return counts


def _stratification_loss(
        chosen: set[str],
        reserve_n: int,
        total_n: int,
        totals: dict[str, Counter[str]],
        features: dict[str, dict[str, Any]],
        labels_by_group: dict[str, list[str]]) -> float:
    chosen_counts = _label_counts_by_group(chosen, features)
    loss = 0.0
    for group, labels in labels_by_group.items():
        group_loss = 0.0
        for label in labels:
            target = totals[group][label] * reserve_n / total_n
            group_loss += (chosen_counts[group][label] - target) ** 2
        loss += group_loss / max(1, len(labels))
    return loss


def stratified_reserve(
        candidate_ids: list[str],
        features: dict[str, dict[str, Any]],
        reserve_n: int,
        seed: int = SEED) -> tuple[list[str], list[str], dict[str, int]]:
    if reserve_n <= 0:
        return [], [], {}
    total_n = len(candidate_ids)
    totals = _label_counts_by_group(candidate_ids, features)
    labels_by_group = {
        group: sorted(counter)
        for group, counter in totals.items()
    }
    rng = random.Random(seed)
    ordered = list(candidate_ids)
    rng.shuffle(ordered)
    tie_rank = {sample_id: index for index, sample_id in enumerate(ordered)}
    chosen: set[str] = set()
    greedy_selection_order: list[str] = []

    for _ in range(reserve_n):
        best_sample = None
        best_key = None
        for sample_id in ordered:
            if sample_id in chosen:
                continue
            trial = chosen | {sample_id}
            loss = _stratification_loss(
                trial, reserve_n, total_n, totals, features, labels_by_group)
            key = (loss, tie_rank[sample_id])
            if best_key is None or key < best_key:
                best_key = key
                best_sample = sample_id
        assert best_sample is not None
        chosen.add(best_sample)
        greedy_selection_order.append(best_sample)

    improved = True
    while improved:
        improved = False
        current_loss = _stratification_loss(
            chosen, reserve_n, total_n, totals, features, labels_by_group)
        for out_sample in sorted(chosen):
            for in_sample in sorted(set(candidate_ids) - chosen):
                trial = (chosen - {out_sample}) | {in_sample}
                loss = _stratification_loss(
                    trial, reserve_n, total_n, totals, features, labels_by_group)
                if loss + 1e-12 < current_loss:
                    chosen = trial
                    current_loss = loss
                    improved = True
                    break
            if improved:
                break
    final_order = sorted(chosen, key=lambda sample_id: tie_rank[sample_id])
    return sorted(chosen), final_order, dict(sorted(tie_rank.items()))


def coverage_summary(
        ids: list[str],
        features: dict[str, dict[str, Any]]) -> dict[str, dict[str, int]]:
    dims = {
        "domain": Counter(),
        "format": Counter(),
        "task_type": Counter(),
        "file_count": Counter(),
        "doc_length": Counter(),
    }
    for sample_id in ids:
        feature = features[sample_id]
        dims["domain"][feature["domain"]] += 1
        for format_name in feature["formats"]:
            dims["format"][format_name] += 1
        for task_type in feature["task_types"]:
            dims["task_type"][task_type] += 1
        dims["file_count"][feature["file_count_bin"]] += 1
        dims["doc_length"][feature["doc_length_bin"]] += 1
    return {name: dict(sorted(counter.items())) for name, counter in dims.items()}


def runtime_runnability(
        repo: Path,
        sample_list: list[str],
        timeout_note_only: bool = False) -> dict[str, Any]:
    """Run current HP_V8 evaluator entry on reference state without API."""

    src = repo / "HP_V8" / "src"
    sys.path.insert(0, str(src))
    import model_openai  # type: ignore
    import domains.domain_base as domain_base  # type: ignore
    import domains.domain_fiction as domain_fiction  # type: ignore
    from domains import get_domain  # type: ignore
    from experiment_runner import _evaluate  # type: ignore
    from utils_context import build_context_from_folder  # type: ignore
    from utils_env import load_sample  # type: ignore

    def no_api(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("ZERO_API_GUARD: evaluator attempted model API")

    model_openai.generate = no_api
    model_openai.generate_json = no_api
    domain_base.generate = no_api
    domain_fiction.generate_json = no_api

    samples_root = repo / "data" / "samples_delegate52"
    runnable: dict[str, dict[str, Any]] = {}
    failed: dict[str, str] = {}
    nonunit: dict[str, float] = {}

    for sample_id in sample_list:
        capture = io.StringIO()
        try:
            sample, folder, states = load_sample(
                sample_id, samples_folder=str(samples_root) + os.sep
            )
            initial = states[sample["start_state"]]
            context = build_context_from_folder(
                os.path.join(folder, initial["solution_folder"])
            )
            domain = get_domain(sample["sample_type"])
            domain.samples_folder = str(samples_root) + os.sep
            with contextlib.redirect_stdout(capture), contextlib.redirect_stderr(capture):
                result = _evaluate(
                    domain, sample_id, context, initial, list(initial["context"])
                )
            score = result.get("score") if isinstance(result, dict) else None
            ok = (
                isinstance(result, dict)
                and "error" not in result
                and isinstance(score, (int, float))
                and not isinstance(score, bool)
                and math.isfinite(score)
                and 0.0 <= float(score) <= 1.0
            )
            if ok:
                runnable[sample_id] = {"score": float(score)}
                if abs(float(score) - 1.0) > 1e-12:
                    nonunit[sample_id] = float(score)
            else:
                failed[sample_id] = repr(result)
        except Exception as exc:  # pragma: no cover - exercised in repo smoke
            failed[sample_id] = f"{type(exc).__name__}: {exc}"

    note = None
    if timeout_note_only:
        note = "runtime check was skipped by caller"
    return {
        "checked_count": len(sample_list),
        "runnable_count": len(runnable),
        "failed_count": len(failed),
        "runnable_sample_ids": sorted(runnable),
        "failed": dict(sorted(failed.items())),
        "nonunit_reference_scores": dict(sorted(nonunit.items())),
        "note": note,
    }


def build_split(repo: Path, *, skip_runtime_smoke: bool = False) -> dict[str, Any]:
    selector_script = Path(__file__).resolve()
    samples_root = repo / "data" / "samples_delegate52"
    registry_path = repo / "data" / "CONTAMINATION_REGISTRY.json"
    split_path = repo / "data" / "hybrid_split.json"
    registry = read_json(registry_path)
    hybrid_split = read_json(split_path)
    all_ids = sample_ids(samples_root)
    all_set = set(all_ids)
    registry_sets = load_registry_sets(registry)
    split = split_sets(hybrid_split)
    any_provider = scan_any_provider_exposure(repo, all_set)
    actual_method = scan_method_exposure(repo, all_set)

    registry_clean = set(registry_sets["clean_candidate"])
    method_holdout_pool, policy_method_exposed, clean_with_method_exposure = (
        method_holdout_candidates(
            all_set, registry_sets, split, actual_method["sample_ids"]
        )
    )

    runtime = (
        runtime_runnability(repo, all_ids)
        if not skip_runtime_smoke
        else {
            "checked_count": 0,
            "runnable_count": len(all_ids),
            "failed_count": 0,
            "runnable_sample_ids": all_ids,
            "failed": {},
            "nonunit_reference_scores": {},
            "note": "runtime smoke skipped; all samples provisionally treated runnable",
        }
    )
    runnable = set(runtime["runnable_sample_ids"])
    candidates = sorted(method_holdout_pool)
    runnable_candidates = sorted(set(candidates) & runnable)

    features = {sample_id: sample_feature(repo, sample_id) for sample_id in runnable_candidates}
    add_length_bins(features)
    selected_n, reserve_n, count_rule = selection_counts(len(runnable_candidates))
    reserve, reserve_selection_order, tie_rank = stratified_reserve(
        runnable_candidates, features, reserve_n, SEED)
    selected = sorted(set(runnable_candidates) - set(reserve))
    if len(selected) != selected_n:
        raise RuntimeError(
            f"selection count mismatch: expected {selected_n}, got {len(selected)}"
        )

    result = {
        "schema": SCHEMA,
        "experiment_id": DEFAULT_EXPERIMENT_ID,
        "repo": {
            "note": (
                "Dynamic generation time, Git HEAD, and dirty status are "
                "intentionally omitted so identical input bytes reproduce "
                "identical selection bytes. Formal API launch separately "
                "requires a clean tree and the committed artifact."
            ),
        },
        "inputs": {
            "selector_script": {
                "path": selector_script.relative_to(repo).as_posix()
                if selector_script.is_relative_to(repo)
                else str(selector_script),
                "sha256": sha256_file(selector_script),
                "python_version": sys.version,
            },
            "registry": {
                "path": "data/CONTAMINATION_REGISTRY.json",
                "sha256": sha256_file(registry_path),
            },
            "hybrid_split": {
                "path": "data/hybrid_split.json",
                "sha256": sha256_file(split_path),
            },
            "samples_root": "data/samples_delegate52",
            "sample_count": len(all_ids),
            "sample_json_manifest_sha256": hashlib.sha256(
                compact_json_bytes({
                    sample_id: sha256_file(samples_root / sample_id / "sample.json")
                    for sample_id in all_ids
                })
            ).hexdigest(),
        },
        "exposure_policy": {
            "strict_any_provider_call": {
                "description": (
                    "Any historical provider/API request counts as exposure; "
                    "this includes the frozen FullRewrite control baseline."
                ),
                **{k: v for k, v in any_provider.items() if k != "evidence_examples"},
            },
            "method_developer_unseen": {
                "description": (
                    "Repository holdout policy used for HP method iteration: "
                    "registry contaminated samples and HP/development exposures "
                    "are excluded; FR-only frozen control calls are reported but "
                    "do not consume the HP holdout."
                ),
                "registry_contaminated_count": len(registry_sets["contaminated"]),
                "registry_clean_candidate_count": len(registry_clean),
                "split_dev_count": len(split.get("dev", set())),
                "split_val_count": len(split.get("val", set())),
                "split_test_count": len(split.get("test", set())),
                "split_unused_reserve_count": len(split.get("unused_reserve", set())),
                "actual_method_scan_count": actual_method["sample_count"],
                "actual_method_scan_sample_ids": actual_method["sample_ids"],
                "actual_method_path_manifest_sha256": actual_method[
                    "path_manifest_sha256"
                ],
                "documented_developer_content_exposure": dict(
                    DOCUMENTED_DEVELOPER_CONTENT_EXPOSURE
                ),
                "clean_candidate_with_method_exposure_count": len(
                    clean_with_method_exposure
                ),
                "clean_candidate_with_method_exposure_ids": sorted(
                    clean_with_method_exposure
                ),
                "excluded_sample_count": len(policy_method_exposed),
                "excluded_sample_ids": sorted(policy_method_exposed),
            },
            "unsupported_generation_domain_from_registry": sorted(
                registry_sets["unsupported_generation_domain"]
            ),
        },
        "runtime_evaluator_smoke": runtime,
        "candidate_rule": {
            "seed": SEED,
            "rule": count_rule,
            "candidate_count": len(runnable_candidates),
            "selected_count": len(selected),
            "reserve_count": len(reserve),
            "strict_unseen_candidate_count": len(all_set - set(any_provider["sample_ids"])),
        },
        "candidate_sample_ids": runnable_candidates,
        "selected_sample_ids": selected,
        "reserve_sample_ids": reserve,
        "reserve_selection_order": reserve_selection_order,
        "features": {sample_id: features[sample_id] for sample_id in runnable_candidates},
        "coverage": {
            "candidate": coverage_summary(runnable_candidates, features),
            "selected": coverage_summary(selected, features),
            "reserve": coverage_summary(reserve, features),
        },
        "algorithm": {
            "name": "reserve-first greedy stratified selection with local swaps",
            "reserve_selection_order": (
                "final reserve set sorted by the seed42 tie-rank after local swaps"
            ),
            "tie_breaker": (
                "random.Random(seed).shuffle(candidate_ids) rank; equal-loss "
                "ties are not resolved by sample_id"
            ),
            "tie_rank": tie_rank,
            "stratification_dimensions": [
                "domain",
                "format multi-label",
                "semantic_operations/task_type",
                "file_count_bin",
                "doc_length_quintile",
            ],
            "selected_is_complement_of_reserve": True,
        },
        "caveats": [
            "Strict any-provider exposure leaves zero unseen samples because the frozen FullRewrite baseline has raw API evidence for all 234 delegate52 samples.",
            "The selected confirmation set is method/developer-unseen under the repository holdout policy, not absolute provider-unseen.",
            "The sealed dev, val, test, and unused-reserve splits are all excluded; in particular, the 2026-07-03 live HybridPatch test20 cannot re-enter through a stale registry label.",
            "python1 is excluded because docs/FINDINGS.md:370-399 records a developer-level task/evaluator investigation even though the older registry still labels it clean_candidate.",
            "audiosyn1 and audiosyn4 retain the registry's historical unsupported_generation_domain annotation but are eligible because the user-specified current evaluator runtime criterion passes; the annotation remains disclosed.",
            "Runtime-runnable means the current evaluator entry returns a finite score without local/API exceptions; it does not prove every evaluator is discriminative.",
            "This tool performs zero API calls and is not an effect experiment.",
        ],
    }
    result["artifact_sha256_preview"] = hashlib.sha256(
        compact_json_bytes({
            "candidate_sample_ids": result["candidate_sample_ids"],
            "selected_sample_ids": result["selected_sample_ids"],
            "reserve_sample_ids": result["reserve_sample_ids"],
            "features": result["features"],
        })
    ).hexdigest()
    return result


def markdown_report(result: dict[str, Any]) -> str:
    policy = result["exposure_policy"]
    rule = result["candidate_rule"]
    lines = [
        f"# {result['experiment_id']} selection",
        "",
        "This is a zero-API split audit. It does not read provider payload bodies and it is not an effect experiment.",
        "",
        "## Exposure conclusion",
        "",
        (
            f"- Strict any-provider exposure: "
            f"{policy['strict_any_provider_call']['sample_count']}/"
            f"{result['inputs']['sample_count']} exposed; strict unseen candidates = "
            f"{rule['strict_unseen_candidate_count']}."
        ),
        (
            f"- Repository method/developer-unseen policy: "
            f"{policy['method_developer_unseen']['excluded_sample_count']} excluded; "
            f"{rule['candidate_count']} runnable candidates."
        ),
        "- FR-only frozen FullRewrite control calls are reported as strict exposure but are not treated as HP method exposure by this repository holdout policy.",
        "",
        "## Selection",
        "",
        f"- Seed: `{rule['seed']}`",
        f"- Rule: `{rule['rule']}`",
        f"- Selected: `{rule['selected_count']}`",
        f"- Reserve: `{rule['reserve_count']}`",
        "",
        "### Selected sample IDs",
        "",
        "```text",
        " ".join(result["selected_sample_ids"]),
        "```",
        "",
        "### Reserve sample IDs",
        "",
        "```text",
        " ".join(result["reserve_sample_ids"]),
        "```",
        "",
        "## Coverage summary",
        "",
    ]
    for group in ("candidate", "selected", "reserve"):
        lines.append(f"### {group}")
        lines.append("")
        for dim, counts in result["coverage"][group].items():
            rendered = ", ".join(f"{k}:{v}" for k, v in counts.items())
            lines.append(f"- {dim}: {rendered}")
        lines.append("")
    runtime = result["runtime_evaluator_smoke"]
    lines.extend([
        "## Runtime evaluator smoke",
        "",
        (
            f"- Checked: `{runtime['checked_count']}`; runnable: "
            f"`{runtime['runnable_count']}`; failed: `{runtime['failed_count']}`."
        ),
    ])
    if runtime.get("nonunit_reference_scores"):
        nonunit = ", ".join(
            f"{k}:{v}" for k, v in runtime["nonunit_reference_scores"].items()
        )
        lines.append(f"- Non-unit reference scores: {nonunit}")
    if runtime.get("note"):
        lines.append(f"- Note: {runtime['note']}")
    lines.extend([
        "",
        "## Input digests",
        "",
        f"- selector_script: `{result['inputs']['selector_script']['sha256']}`",
        f"- registry: `{result['inputs']['registry']['sha256']}`",
        f"- hybrid_split: `{result['inputs']['hybrid_split']['sha256']}`",
        f"- sample_json_manifest: `{result['inputs']['sample_json_manifest_sha256']}`",
        f"- strict provider-evidence manifest: `{policy['strict_any_provider_call']['evidence_manifest_sha256']}`",
        f"- method-evidence manifest: `{policy['method_developer_unseen']['actual_method_path_manifest_sha256']}`",
        f"- selection_preview: `{result['artifact_sha256_preview']}`",
        "- Dynamic generation metadata is omitted for byte reproducibility.",
        "- Formal API launch must use the committed selection artifact from a clean tree.",
        "",
        "## Caveats",
        "",
    ])
    lines.extend(f"- {item}" for item in result["caveats"])
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    parser.add_argument(
        "--output-prefix",
        default=str(DEFAULT_OUTPUT_PREFIX),
        help="output prefix without .json/.md",
    )
    parser.add_argument(
        "--skip-runtime-smoke",
        action="store_true",
        help="skip evaluator smoke; intended only for unit tests/debug",
    )
    args = parser.parse_args(argv)
    repo = Path(args.repo).resolve()
    result = build_split(repo, skip_runtime_smoke=args.skip_runtime_smoke)
    prefix = (repo / args.output_prefix).resolve()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = prefix.with_suffix(".json")
    md_path = prefix.with_suffix(".md")
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(markdown_report(result), encoding="utf-8")
    try:
        json_display = json_path.relative_to(repo).as_posix()
        markdown_display = md_path.relative_to(repo).as_posix()
    except ValueError:
        json_display = str(json_path)
        markdown_display = str(md_path)
    print(f"WROTE {json_display}")
    print(f"WROTE {markdown_display}")
    print(
        "COUNTS "
        f"candidates={result['candidate_rule']['candidate_count']} "
        f"selected={result['candidate_rule']['selected_count']} "
        f"reserve={result['candidate_rule']['reserve_count']} "
        f"strict_unseen={result['candidate_rule']['strict_unseen_candidate_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
