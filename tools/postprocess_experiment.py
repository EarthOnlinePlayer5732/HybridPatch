"""Resumable zero-API closeout for HP_V8+ experiments.

The command keeps human judgment as an explicit checkpoint, but removes the
manual orchestration around it:

1. cache one strict paired-campaign inspection;
2. run/reuse official ``prepare``;
3. stop cleanly until ``record_review.yaml`` is reviewed;
4. validate every review/catalog/record contract before heavy I/O;
5. run the authoritative scan and native compression concurrently;
6. finalize while reusing the sealed tree digest.

It never starts a model or provider request.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

import experiment_artifacts as artifacts
import process_experiment as process


ROOT = Path(__file__).resolve().parents[1]
INSPECTION_SCHEMA = "anchorpatch.strict_postrun_inspection/1"
INSPECTION_CACHE_SCHEMA = "hybridpatch.strict_inspection_cache/1"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command, help_text in (
        ("status", "Show cheap closeout state without reading raw response bodies."),
        ("inspect", "Run or reuse the strict paired-campaign inspection."),
        ("review-check", "Validate the reviewed record contract before heavy I/O."),
    ):
        child = subparsers.add_parser(command, help=help_text)
        child.add_argument("--experiment", required=True, type=Path)
        if command == "inspect":
            child.add_argument("--force", action="store_true")
            child.add_argument(
                "--adopt-existing",
                action="store_true",
                help=(
                    "Create a cache sidecar for an immutable existing strict "
                    "artifact only when a finalized analysis report references "
                    "its exact SHA-256."
                ),
            )
        if command == "review-check":
            child.add_argument("--review", type=Path, default=None)

    seal = subparsers.add_parser(
        "seal",
        help="Run credential/tree scan and native private .tgz creation in parallel.",
    )
    seal.add_argument("--experiment", required=True, type=Path)
    seal.add_argument("--archive", required=True, type=Path)
    seal.add_argument("--force", action="store_true")

    closeout = subparsers.add_parser(
        "closeout",
        help="Resume the standard closeout through the next safe checkpoint.",
    )
    closeout.add_argument("--experiment", required=True, type=Path)
    closeout.add_argument("--archive", required=True, type=Path)
    closeout.add_argument("--review", type=Path, default=None)
    closeout.add_argument("--critical-theta", type=float, default=0.10)
    closeout.add_argument("--confirm-stopped", action="store_true")
    closeout.add_argument("--force-inspection", action="store_true")
    closeout.add_argument("--force-seal", action="store_true")
    closeout.add_argument(
        "--private-record-bundle",
        type=Path,
        default=None,
        help=(
            "Archive the validated generated record and restore the existing "
            "published catalog/index/records afterward."
        ),
    )
    return parser.parse_args(argv)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return value


def _review_path(archive: Path, selected: Path | None) -> Path:
    if selected is None:
        return archive / "analysis" / "record_review.yaml"
    return selected if selected.is_absolute() else ROOT / selected


def _prepared_is_current(owner: str, archive: Path) -> bool:
    path = archive / "analysis" / "derived_facts.json"
    if not path.is_file():
        return False
    prepared = _read_json(path)
    current = process.collect_facts(owner, archive)
    return (
        prepared.get("input_sha256") == current.get("input_sha256")
        and prepared.get("experiment_plan_sha256")
        == current.get("experiment_plan_sha256")
    )


def status_document(experiment: Path) -> dict[str, Any]:
    owner, _, archive = process.resolve_experiment(experiment)
    analysis = archive / "analysis"
    review_path = analysis / "record_review.yaml"
    process_state_path = analysis / "process_state.json"
    review_pending = True
    if review_path.is_file():
        with review_path.open(encoding="utf-8") as handle:
            review = yaml.safe_load(handle)
        review_pending = not isinstance(review, dict) or process.contains_placeholder(
            review
        )
    state = _read_json(process_state_path) if process_state_path.is_file() else {}
    return {
        "schema": "hybridpatch.postprocess_status/1",
        "experiment_id": archive.name,
        "owner": owner,
        "prepared_current": _prepared_is_current(owner, archive),
        "strict_inspection_present": (
            analysis / "strict_inspection_postrun.json"
        ).is_file(),
        "review_present": review_path.is_file(),
        "review_pending": review_pending,
        "process_stage": state.get("stage"),
        "finalized": state.get("stage") in {
            "finalized",
            "finalized_private",
        },
    }


def _load_dispatch_module(owner_path: Path):
    source = owner_path / "src" / "paired_campaign_dispatch.py"
    if not source.is_file():
        raise RuntimeError(f"paired dispatcher is missing: {process.repo_ref(source)}")
    module_name = f"_postprocess_dispatch_{owner_path.name}"
    spec = importlib.util.spec_from_file_location(module_name, source)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load paired campaign dispatcher")
    original = list(sys.path)
    try:
        sys.path.insert(0, str(owner_path / "src"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original
    return module


def _contains_digest(value: Any, digest: str) -> bool:
    if isinstance(value, dict):
        return any(
            (key == "strict_inspection_sha256" and item == digest)
            or _contains_digest(item, digest)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_digest(item, digest) for item in value)
    return False


def _inspection_cache_path(archive: Path) -> Path:
    return archive / "analysis" / "strict_inspection_postrun.cache.json"


def _write_inspection_cache(
    archive: Path,
    inspection_path: Path,
    quick: dict[str, Any],
    *,
    evidence_refs: list[str],
    adoption: str,
) -> None:
    cache = {
        "schema": INSPECTION_CACHE_SCHEMA,
        "experiment_id": archive.name,
        "inspection_ref": process.repo_ref(inspection_path),
        "inspection_sha256": artifacts.sha256_file(inspection_path),
        "input_quick_fingerprint": quick,
        "evidence_refs": evidence_refs,
        "adoption": adoption,
        "created_at": datetime.now().astimezone().isoformat(),
    }
    process.write_atomic(
        _inspection_cache_path(archive),
        process.stable_json(cache),
    )


def _inspection_cache_matches(
    archive: Path,
    inspection_path: Path,
    quick: dict[str, Any],
) -> bool:
    cache_path = _inspection_cache_path(archive)
    if not inspection_path.is_file() or not cache_path.is_file():
        return False
    try:
        cache = _read_json(cache_path)
    except (OSError, ValueError, RuntimeError):
        return False
    return (
        cache.get("schema") == INSPECTION_CACHE_SCHEMA
        and cache.get("experiment_id") == archive.name
        and cache.get("inspection_sha256")
        == artifacts.sha256_file(inspection_path)
        and cache.get("input_quick_fingerprint") == quick
    )


def adopt_existing_inspection(
    archive: Path,
    inspection_path: Path,
    quick: dict[str, Any],
) -> dict[str, Any]:
    if not inspection_path.is_file():
        raise RuntimeError("there is no strict inspection artifact to adopt")
    state_path = archive / "analysis" / "process_state.json"
    state = _read_json(state_path) if state_path.is_file() else {}
    if state.get("stage") not in {"finalized", "finalized_private"}:
        raise RuntimeError(
            "existing strict inspection can be adopted only for a finalized experiment"
        )
    digest = artifacts.sha256_file(inspection_path)
    evidence_refs: list[str] = []
    for candidate in sorted((archive / "analysis").glob("*.json")):
        if candidate in {inspection_path, _inspection_cache_path(archive)}:
            continue
        try:
            value = _read_json(candidate)
        except (OSError, ValueError, RuntimeError):
            continue
        if _contains_digest(value, digest):
            evidence_refs.append(process.repo_ref(candidate))
    if not evidence_refs:
        raise RuntimeError(
            "existing strict inspection is not SHA-256-linked by a finalized "
            "analysis report; refusing cache adoption"
        )
    _write_inspection_cache(
        archive,
        inspection_path,
        quick,
        evidence_refs=evidence_refs,
        adoption="sha256_linked_finalized_analysis",
    )
    print(
        "[postprocess] adopted existing strict inspection cache: "
        + ", ".join(evidence_refs)
    )
    return _read_json(inspection_path)


def inspect_experiment(
    experiment: Path,
    *,
    force: bool = False,
    adopt_existing: bool = False,
) -> tuple[dict, bool]:
    owner, owner_path, archive = process.resolve_experiment(experiment)
    manifest_path = archive / "dispatch_manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError("strict inspection requires dispatch_manifest.json")
    output = archive / "analysis" / "strict_inspection_postrun.json"
    quick = artifacts.quick_tree_fingerprint(
        archive,
        excluded_prefixes=("analysis",),
    )
    if not force and _inspection_cache_matches(archive, output, quick):
        print("[postprocess] strict inspection cache hit")
        return _read_json(output), True
    if adopt_existing:
        return adopt_existing_inspection(archive, output, quick), True

    facts = process.collect_facts(owner, archive)
    manifest = _read_json(manifest_path)
    dispatch = _load_dispatch_module(owner_path)
    required = set(facts["complete_all_methods_sample_ids"])
    raw = dispatch.inspect_campaign(
        str(archive),
        manifest,
        required_complete_samples=required,
    )
    inspection = {
        "schema": INSPECTION_SCHEMA,
        "errors": raw.get("errors") or [],
        "preservation_violations": raw.get("preservation_violations", 0),
        "latched_preservation_violations": raw.get(
            "latched_preservation_violations", 0
        ),
        "preservation_not_applicable": raw.get(
            "preservation_not_applicable", 0
        ),
        "stop_conditions": raw.get("stop_conditions") or [],
        "api_calls": raw.get("api_calls"),
        "semantic_calls": raw.get("semantic_calls"),
        "provider_call_rows": raw.get("provider_call_rows"),
        "inspection_source": {
            "generated_at": datetime.now().astimezone().isoformat(),
            "zero_api": True,
            "required_complete_sample_count": len(required),
            "incomplete_sample_ids": sorted(facts["incomplete_samples"]),
            "input_quick_fingerprint": quick,
        },
    }
    process.write_atomic(output, process.stable_json(inspection))
    _write_inspection_cache(
        archive,
        output,
        quick,
        evidence_refs=[],
        adoption="generated_by_current_inspector",
    )
    if inspection["preservation_violations"]:
        raise RuntimeError("strict inspection found preservation violations")
    print(
        "[postprocess] strict inspection written: "
        f"{process.repo_ref(output)} errors={len(inspection['errors'])}"
    )
    return inspection, False


def seal_experiment(
    experiment: Path,
    archive_path: Path,
    *,
    force: bool = False,
) -> tuple[dict[str, Any], bool]:
    _, _, source = process.resolve_experiment(experiment)
    seal, cached = artifacts.seal_experiment(
        source,
        archive_path,
        force=force,
    )
    suffix = "cache hit" if cached else "created"
    print(
        f"[postprocess] private artifact {suffix}: {archive_path.resolve()}"
    )
    print(f"[postprocess] archive sha256={seal['archive']['sha256']}")
    print(
        "[postprocess] exact local secret matches="
        f"{seal['tree']['credential_scan']['exact_local_secret_match_count']}"
    )
    return seal, cached


def closeout(args: argparse.Namespace) -> int:
    if not args.confirm_stopped:
        raise RuntimeError("--confirm-stopped is required for closeout")
    owner, _, archive = process.resolve_experiment(args.experiment)
    state = status_document(args.experiment)
    if state["finalized"]:
        print("[postprocess] FINALIZED; nothing to resume")
        return 0

    inspect_experiment(args.experiment, force=args.force_inspection)
    if not state["prepared_current"]:
        process.prepare_experiment(
            args.experiment,
            critical_theta=args.critical_theta,
            confirm_stopped=True,
            force_review_draft=False,
        )

    selected_review = _review_path(archive, args.review)
    if not selected_review.is_file():
        raise RuntimeError("prepare did not create record_review.yaml")
    with selected_review.open(encoding="utf-8") as handle:
        review = yaml.safe_load(handle)
    if not isinstance(review, dict) or process.contains_placeholder(review):
        print(
            "[postprocess] WAITING_FOR_REVIEW: edit and independently review "
            f"{process.repo_ref(selected_review)}; rerun the same command afterward"
        )
        return 0

    process.review_check_experiment(args.experiment, selected_review)
    _, _ = seal_experiment(
        args.experiment,
        args.archive,
        force=args.force_seal,
    )
    seal_path = args.archive.resolve().with_name(args.archive.name + ".seal.json")
    process.finalize_experiment(
        args.experiment,
        selected_review,
        seal_path,
        args.private_record_bundle,
    )
    print("[postprocess] CLOSEOUT PASS")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "status":
        print(artifacts.stable_json(status_document(args.experiment)), end="")
    elif args.command == "inspect":
        inspect_experiment(
            args.experiment,
            force=args.force,
            adopt_existing=args.adopt_existing,
        )
    elif args.command == "review-check":
        process.review_check_experiment(args.experiment, args.review)
    elif args.command == "seal":
        seal_experiment(args.experiment, args.archive, force=args.force)
    elif args.command == "closeout":
        return closeout(args)
    else:
        raise RuntimeError(f"unsupported command: {args.command}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"[postprocess] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
