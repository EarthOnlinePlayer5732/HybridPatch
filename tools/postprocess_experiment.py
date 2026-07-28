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
import hashlib
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
INSPECTION_SCHEMA = "anchorpatch.strict_postrun_inspection/2"
INSPECTION_CACHE_SCHEMA = "hybridpatch.strict_inspection_cache/3"
INSPECTION_OWNER_DEPENDENCY_FILES = (
    "paired_campaign_dispatch.py",
    "run_meta.py",
)


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


def _inspection_dependency_identity(
    owner_path: Path,
) -> tuple[dict[str, str], str]:
    sources: dict[str, str] = {}
    digest = hashlib.sha256()
    dependencies = [
        (f"owner/src/{name}", owner_path / "src" / name)
        for name in INSPECTION_OWNER_DEPENDENCY_FILES
    ]
    dependencies.extend((
        ("tools/postprocess_experiment.py", Path(__file__).resolve()),
        ("tools/process_experiment.py", Path(process.__file__).resolve()),
        ("tools/versioned_run_metadata.py",
         ROOT / "tools" / "versioned_run_metadata.py"),
        ("tools/experiment_artifacts.py", Path(artifacts.__file__).resolve()),
    ))
    for identity, path in dependencies:
        if not path.is_file():
            raise RuntimeError(
                f"inspection dependency is missing: {process.repo_ref(path)}"
            )
        sha256 = artifacts.sha256_file(path)
        sources[identity] = sha256
        digest.update(identity.encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha256.encode("ascii"))
        digest.update(b"\n")
    return sources, digest.hexdigest()


def _inspection_evidence_identity(archive: Path) -> dict[str, Any]:
    """Hash every non-sensitive evidence file by stable relative path."""

    digest = hashlib.sha256()
    file_count = 0
    size_bytes = 0
    skipped_links: list[str] = []
    skipped_sensitive: list[str] = []
    for candidate in sorted(
            archive.rglob("*"), key=lambda item: item.as_posix()):
        relative = candidate.relative_to(archive).as_posix()
        if relative == "analysis" or relative.startswith("analysis/"):
            continue
        if candidate.is_symlink():
            skipped_links.append(relative)
            raise RuntimeError(
                "strict inspection evidence contains a symlink: "
                f"{relative}"
            )
        if not candidate.is_file():
            continue
        if candidate.name == ".env" or candidate.name.startswith(".env."):
            skipped_sensitive.append(relative)
            continue
        before = candidate.stat()
        content_sha256 = artifacts.sha256_file(candidate)
        after = candidate.stat()
        if (before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns):
            raise RuntimeError(
                "strict inspection evidence changed while hashing: "
                f"{process.repo_ref(candidate)}"
            )
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content_sha256.encode("ascii"))
        digest.update(b"\n")
        file_count += 1
        size_bytes += after.st_size
    return {
        "algorithm": "path-content-sha256-v1",
        "sha256": digest.hexdigest(),
        "file_count": file_count,
        "size_bytes": size_bytes,
        "skipped_sensitive_files": skipped_sensitive,
        "skipped_symlinks": skipped_links,
    }


def _authoritative_inspection_report_matches(
    value: dict[str, Any],
    *,
    experiment_id: str,
    inspection: dict[str, Any],
    digest: str,
) -> bool:
    integrity = value.get("integrity")
    proof = (
        integrity.get("postrun_verification")
        if isinstance(integrity, dict)
        else None
    )
    return (
        value.get("schema") == "hybridpatch.confirmation_campaign_analysis/1"
        and value.get("experiment_id") == experiment_id
        and isinstance(proof, dict)
        and proof.get("valid") is True
        and proof.get("problems") == []
        and proof.get("unaccepted_inspection_errors") == []
        and proof.get("accepted_sample_level_incomplete_errors")
        == inspection.get("errors")
        and proof.get("strict_inspection_sha256") == digest
        and proof.get("strict_inspection") == inspection
    )


def _inspection_cache_path(archive: Path) -> Path:
    return archive / "analysis" / "strict_inspection_postrun.cache.json"


def _write_inspection_cache(
    archive: Path,
    inspection_path: Path,
    quick: dict[str, Any],
    inspector_sha256: str,
    inspector_sources: dict[str, str],
    evidence_identity: dict[str, Any],
    *,
    evidence_refs: list[str],
    adoption: str,
) -> None:
    cache = {
        "schema": INSPECTION_CACHE_SCHEMA,
        "experiment_id": archive.name,
        "inspection_ref": process.repo_ref(inspection_path),
        "inspection_sha256": artifacts.sha256_file(inspection_path),
        "inspector_sha256": inspector_sha256,
        "inspector_sources": inspector_sources,
        "input_evidence_identity": evidence_identity,
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
    inspector_sha256: str,
    inspector_sources: dict[str, str],
    evidence_identity: dict[str, Any],
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
        and cache.get("inspector_sha256") == inspector_sha256
        and cache.get("inspector_sources") == inspector_sources
        and cache.get("input_evidence_identity") == evidence_identity
        and cache.get("input_quick_fingerprint") == quick
    )


def adopt_existing_inspection(
    archive: Path,
    inspection_path: Path,
    quick: dict[str, Any],
    inspector_sha256: str,
    inspector_sources: dict[str, str],
    evidence_identity: dict[str, Any],
    facts: dict[str, Any],
) -> dict[str, Any]:
    if not inspection_path.is_file():
        raise RuntimeError("there is no strict inspection artifact to adopt")
    state_path = archive / "analysis" / "process_state.json"
    state = _read_json(state_path) if state_path.is_file() else {}
    if (state.get("schema") != "hybridpatch.experiment_process_state/1"
            or state.get("experiment_id") != archive.name
            or state.get("stage") not in {"finalized", "finalized_private"}):
        raise RuntimeError(
            "existing strict inspection requires a valid finalized process state"
        )
    inspection = _read_json(inspection_path)
    _require_current_inspection(
        inspection,
        archive=archive,
        quick=quick,
        inspector_sha256=inspector_sha256,
        inspector_sources=inspector_sources,
        evidence_identity=evidence_identity,
        facts=facts,
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
        if _authoritative_inspection_report_matches(
                value,
                experiment_id=archive.name,
                inspection=inspection,
                digest=digest):
            evidence_refs.append(process.repo_ref(candidate))
    if not evidence_refs:
        raise RuntimeError(
            "existing strict inspection is not SHA-256-linked by a finalized "
            "analysis report; refusing cache adoption"
        )
    if (state.get("input_sha256") != facts.get("input_sha256")
            or state.get("input_sha256") != (
                inspection.get("inspection_source") or {}
            ).get("input_sha256")):
        raise RuntimeError(
            "finalized process state does not bind the strict inspection input"
        )
    _write_inspection_cache(
        archive,
        inspection_path,
        quick,
        inspector_sha256,
        inspector_sources,
        evidence_identity,
        evidence_refs=evidence_refs,
        adoption="sha256_linked_finalized_analysis",
    )
    print(
        "[postprocess] adopted existing strict inspection cache: "
        + ", ".join(evidence_refs)
    )
    return inspection


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
    inspector_sources, inspector_sha256 = _inspection_dependency_identity(
        owner_path)
    quick = artifacts.quick_tree_fingerprint(
        archive,
        excluded_prefixes=("analysis",),
    )
    evidence_identity = _inspection_evidence_identity(archive)
    facts = process.collect_facts(owner, archive)
    if not force and _inspection_cache_matches(
            archive,
            output,
            quick,
            inspector_sha256,
            inspector_sources,
            evidence_identity,
    ):
        print("[postprocess] strict inspection cache hit")
        inspection = _read_json(output)
        _require_current_inspection(
            inspection,
            archive=archive,
            quick=quick,
            inspector_sha256=inspector_sha256,
            inspector_sources=inspector_sources,
            evidence_identity=evidence_identity,
            facts=facts,
        )
        return inspection, True
    if adopt_existing:
        inspection = adopt_existing_inspection(
            archive,
            output,
            quick,
            inspector_sha256,
            inspector_sources,
            evidence_identity,
            facts,
        )
        return inspection, True

    incomplete_samples = set(facts["incomplete_samples"])
    incomplete_outcomes = facts.get("incomplete_sample_outcomes") or {}
    allowed_incomplete = {
        "infrastructure_incomplete", "evaluator_incomplete"}
    if (set(incomplete_outcomes) != incomplete_samples
            or any(
                status not in allowed_incomplete
                for status in incomplete_outcomes.values()
            )):
        raise RuntimeError(
            "incomplete campaign lacks a supported terminal sample outcome"
        )
    manifest = _read_json(manifest_path)
    dispatch = _load_dispatch_module(owner_path)
    required = set(facts["complete_all_methods_sample_ids"])
    raw = dispatch.inspect_campaign(
        str(archive),
        manifest,
        require_complete=not bool(incomplete_samples),
        require_terminal_provenance=True,
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
            "experiment_id": archive.name,
            "manifest_sha256": artifacts.sha256_file(manifest_path),
            "input_sha256": facts.get("input_sha256"),
            "incomplete_sample_ids": sorted(incomplete_samples),
            "incomplete_sample_outcomes": incomplete_outcomes,
            "campaign_complete": not bool(incomplete_samples),
            "require_complete": not bool(incomplete_samples),
            "require_terminal_provenance": True,
            "inspector_sha256": inspector_sha256,
            "inspector_sources": inspector_sources,
            "input_quick_fingerprint": quick,
            "input_evidence_identity": evidence_identity,
        },
    }
    _require_clean_inspection(inspection)
    final_quick = artifacts.quick_tree_fingerprint(
        archive,
        excluded_prefixes=("analysis",),
    )
    final_sources, final_inspector_sha256 = (
        _inspection_dependency_identity(owner_path)
    )
    final_evidence_identity = _inspection_evidence_identity(archive)
    final_facts = process.collect_facts(owner, archive)
    if (final_quick != quick
            or final_sources != inspector_sources
            or final_inspector_sha256 != inspector_sha256
            or final_evidence_identity != evidence_identity):
        raise RuntimeError(
            "strict inspection inputs changed while inspection was running"
        )
    _require_current_inspection(
        inspection,
        archive=archive,
        quick=final_quick,
        inspector_sha256=final_inspector_sha256,
        inspector_sources=final_sources,
        evidence_identity=final_evidence_identity,
        facts=final_facts,
    )
    process.write_atomic(output, process.stable_json(inspection))
    _write_inspection_cache(
        archive,
        output,
        quick,
        inspector_sha256,
        inspector_sources,
        evidence_identity,
        evidence_refs=[],
        adoption="generated_by_current_inspector",
    )
    print(
        "[postprocess] strict inspection written: "
        f"{process.repo_ref(output)} errors={len(inspection['errors'])}"
    )
    return inspection, False


def _require_clean_inspection(inspection: dict) -> None:
    if inspection.get("schema") != INSPECTION_SCHEMA:
        raise RuntimeError("strict inspection schema is invalid")
    errors = inspection.get("errors")
    stop_conditions = inspection.get("stop_conditions")
    preservation = inspection.get("preservation_violations")
    latched = inspection.get("latched_preservation_violations")
    if not isinstance(errors, list):
        raise RuntimeError("strict inspection errors field is invalid")
    if not isinstance(stop_conditions, list):
        raise RuntimeError("strict inspection stop_conditions field is invalid")
    for label, value in (
        ("preservation_violations", preservation),
        ("latched_preservation_violations", latched),
    ):
        if (not isinstance(value, int) or isinstance(value, bool) or value < 0):
            raise RuntimeError(f"strict inspection {label} field is invalid")
    if preservation:
        raise RuntimeError("strict inspection found preservation violations")
    if latched:
        raise RuntimeError(
            "strict inspection found latched preservation violations"
        )
    if stop_conditions:
        raise RuntimeError("strict inspection found campaign stop conditions")
    if errors:
        raise RuntimeError(
            "strict inspection found integrity errors: "
            + "; ".join(str(error) for error in errors)
        )


def _require_current_inspection(
    inspection: dict,
    *,
    archive: Path,
    quick: dict[str, Any],
    inspector_sha256: str,
    inspector_sources: dict[str, str],
    evidence_identity: dict[str, Any],
    facts: dict[str, Any],
) -> None:
    _require_clean_inspection(inspection)
    source = inspection.get("inspection_source")
    if not isinstance(source, dict):
        raise RuntimeError("strict inspection source binding is missing")
    incomplete_ids = source.get("incomplete_sample_ids")
    incomplete_outcomes = source.get("incomplete_sample_outcomes")
    current_input_sha256 = facts.get("input_sha256")
    current_incomplete_ids = sorted(facts.get("incomplete_samples") or {})
    current_incomplete_outcomes = (
        facts.get("incomplete_sample_outcomes") or {}
    )
    current_complete_ids = facts.get("complete_all_methods_sample_ids")
    valid_input_sha256 = (
        isinstance(current_input_sha256, str)
        and len(current_input_sha256) == 64
        and all(character in "0123456789abcdef" for character in current_input_sha256)
    )
    manifest_path = archive / "dispatch_manifest.json"
    if (source.get("zero_api") is not True
            or source.get("experiment_id") != archive.name
            or source.get("require_terminal_provenance") is not True
            or source.get("inspector_sha256") != inspector_sha256
            or source.get("inspector_sources") != inspector_sources
            or source.get("input_evidence_identity") != evidence_identity
            or source.get("input_quick_fingerprint") != quick
            or not manifest_path.is_file()
            or source.get("manifest_sha256")
            != artifacts.sha256_file(manifest_path)
            or not valid_input_sha256
            or source.get("input_sha256") != current_input_sha256
            or not isinstance(current_complete_ids, (list, tuple, set))
            or isinstance(current_complete_ids, (str, bytes))
            or source.get("required_complete_sample_count")
            != len(current_complete_ids)
            or not isinstance(incomplete_ids, list)
            or not all(isinstance(item, str) for item in incomplete_ids)
            or incomplete_ids != current_incomplete_ids
            or not isinstance(incomplete_outcomes, dict)
            or set(incomplete_outcomes) != set(incomplete_ids)
            or incomplete_outcomes != current_incomplete_outcomes
            or any(
                status not in {
                    "infrastructure_incomplete", "evaluator_incomplete"}
                for status in incomplete_outcomes.values()
            )
            or source.get("campaign_complete") is not (not incomplete_ids)
            or source.get("require_complete") is not (not incomplete_ids)):
        raise RuntimeError("strict inspection source binding is invalid or stale")


def _require_final_record(
    owner: str,
    archive: Path,
    process_state: dict[str, Any],
) -> Path:
    stage = process_state.get("stage")
    record_ref = process_state.get("record_ref")
    if not isinstance(record_ref, str) or not record_ref:
        raise RuntimeError("finalized process state record_ref is missing")
    selected = Path(record_ref)
    if stage == "finalized":
        expected_ref = f"{owner}/records/{archive.name}/report.md"
        if selected.is_absolute() or record_ref != expected_ref:
            raise RuntimeError(
                "finalized process state record_ref is not the canonical report"
            )
        expected_sha256 = process_state.get("public_record_sha256")
        if (not isinstance(expected_sha256, str)
                or len(expected_sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in expected_sha256
                )):
            raise RuntimeError(
                "finalized process state public record SHA-256 is invalid"
            )
        record_path = (ROOT / selected).resolve()
        try:
            record_path.relative_to(ROOT.resolve())
        except ValueError as exc:
            raise RuntimeError(
                "finalized process state record_ref escapes the repository"
            ) from exc
    elif stage == "finalized_private":
        if not selected.is_absolute():
            raise RuntimeError(
                "finalized private process state record_ref must be absolute"
            )
        record_path = selected.resolve()
        expected_sha256 = process_state.get("private_record_bundle_sha256")
        if (not isinstance(expected_sha256, str)
                or len(expected_sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in expected_sha256
                )):
            raise RuntimeError(
                "finalized private process state bundle SHA-256 is invalid"
            )
    else:
        raise RuntimeError("finalized process state stage is invalid")
    if not record_path.is_file():
        raise RuntimeError(
            f"finalized record_ref does not exist: {record_path}"
        )
    if (stage == "finalized"
            and artifacts.sha256_file(record_path)
            != process_state["public_record_sha256"]):
        raise RuntimeError(
            "finalized public record does not match process state"
        )
    if (stage == "finalized_private"
            and artifacts.sha256_file(record_path)
            != process_state["private_record_bundle_sha256"]):
        raise RuntimeError(
            "finalized private record bundle does not match process state"
        )
    return record_path


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
    owner, owner_path, archive = process.resolve_experiment(args.experiment)
    state = status_document(args.experiment)
    if state["finalized"]:
        inspection_path = archive / "analysis" / "strict_inspection_postrun.json"
        quick = artifacts.quick_tree_fingerprint(
            archive,
            excluded_prefixes=("analysis",),
        )
        inspector_sources, inspector_sha256 = (
            _inspection_dependency_identity(owner_path)
        )
        evidence_identity = _inspection_evidence_identity(archive)
        if (not state["prepared_current"]
                or not _inspection_cache_matches(
                    archive,
                    inspection_path,
                    quick,
                    inspector_sha256,
                    inspector_sources,
                    evidence_identity,
                )):
            raise RuntimeError(
                "finalized experiment no longer matches its prepared/strict "
                "inspection inputs"
            )
        process_state = _read_json(
            archive / "analysis" / "process_state.json")
        inspection = _read_json(inspection_path)
        facts = process.collect_facts(owner, archive)
        inspection_input = (
            inspection.get("inspection_source") or {}).get("input_sha256")
        if (process_state.get("schema")
                != "hybridpatch.experiment_process_state/1"
                or process_state.get("experiment_id") != archive.name
                or process_state.get("stage")
                not in {"finalized", "finalized_private"}
                or process_state.get("input_sha256") != inspection_input
                or process_state.get("input_sha256")
                != facts.get("input_sha256")):
            raise RuntimeError(
                "finalized process state is invalid or bound to stale inputs"
            )
        _require_current_inspection(
            inspection,
            archive=archive,
            quick=quick,
            inspector_sha256=inspector_sha256,
            inspector_sources=inspector_sources,
            evidence_identity=evidence_identity,
            facts=facts,
        )
        _require_final_record(owner, archive, process_state)
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
