"""Review-gated post-processing for new HybridPatch experiments.

``prepare`` runs the version-local verifier and analyzer, records only
mechanically derived facts, and writes a review YAML for the judgments that
must not be guessed. ``review-check`` exercises record contracts before heavy
I/O. ``finalize`` verifies that review, atomically registers the experiment,
and delegates Git-record generation to finalize_experiment.py; it can consume
a parallel artifact seal and optionally restore a private generated bundle.

This tool is offline: it never calls a model. It refuses frozen HP_V3-HP_V7.
Event-ledger archives must have a current receipt-bound quiescent projection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import yaml

from versioned_run_metadata import (
    read_quiescent_run_metadata,
    run_metadata_artifact_paths,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "tools" / "experiment_records_catalog.json"
FINALIZER = ROOT / "tools" / "finalize_experiment.py"
VERSION_STATES = ROOT / "tools" / "version_states.json"
RETAINED_ABANDONMENT_DIRECTORY = ROOT / "tools" / "retained_abandonments"
EXPERIMENT_PLANS = ROOT / "docs" / "experiment_plans"
OWNER_RE = re.compile(r"^HP_V(\d+)$")
PLACEHOLDER = "REVIEW_REQUIRED"
METHOD_NAMES = ("hybridpatch", "fullrewrite", "anchorpatch")
CATALOG_STATUSES = {"canonical", "diagnostic", "source-only", "superseded"}
LIFECYCLE_STATUSES = {
    "complete",
    "incomplete",
    "failed_informative",
    "superseded",
}
CATALOG_EVIDENCE_ROLES = {
    "canonical",
    "historical",
    "diagnostic",
    "source_only",
    "sensitivity",
}
RETAINED_ABANDONMENT_SCHEMA = "hybridpatch.retained_abandonment/1"
RETAINED_ABANDONMENT_REASON = "operator_abandoned_non_claim_evidence"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    activate = subparsers.add_parser(
        "activate",
        help="Mark a newly created HP_V8+ directory as the writable active version.",
    )
    activate.add_argument("--owner", required=True)

    freeze = subparsers.add_parser(
        "freeze",
        help="Freeze the current active HP_V8+ version after all experiments finish.",
    )
    freeze.add_argument("--owner", required=True)
    freeze.add_argument(
        "--confirm-no-running-experiments",
        action="store_true",
        help="Required acknowledgement that all workers for this version have stopped.",
    )

    transition = subparsers.add_parser(
        "transition-active",
        help=(
            "Auditably hand off the active HP_V8+ version to a new HP_Vx "
            "while preserving uncatalogued historical raw experiment dirs."
        ),
    )
    transition.add_argument("--from-owner", required=True)
    transition.add_argument("--to-owner", required=True)
    transition.add_argument(
        "--confirm-no-running-experiments",
        action="store_true",
        help="Required acknowledgement that all workers for the old version have stopped.",
    )
    transition.add_argument(
        "--confirm-uncatalogued-raw-preserved",
        action="store_true",
        help=(
            "Required acknowledgement that listed uncatalogued exp_* dirs are "
            "historical raw evidence and will remain preserved in place."
        ),
    )
    transition.add_argument(
        "--reason",
        required=True,
        help="Human reason recorded in tools/version_states.json.",
    )

    retain = subparsers.add_parser(
        "prepare-retained-abandonment",
        help=(
            "Inventory unrecorded retained raw experiments without modifying "
            "them, for an explicit non-claim version freeze."
        ),
    )
    retain.add_argument("--owner", required=True)
    retain.add_argument(
        "--confirm-no-running-experiments",
        action="store_true",
        help="Required acknowledgement that all workers for this version have stopped.",
    )

    prepare = subparsers.add_parser(
        "prepare",
        help="Verify, analyze, derive facts, and write the review YAML.",
    )
    prepare.add_argument("--experiment", required=True, type=Path)
    prepare.add_argument("--critical-theta", type=float, default=0.10)
    prepare.add_argument(
        "--confirm-stopped",
        action="store_true",
        help="Required acknowledgement that no worker is writing this out_dir.",
    )
    prepare.add_argument(
        "--force-review-draft",
        action="store_true",
        help="Replace an existing generated review draft after re-running checks.",
    )

    finalize = subparsers.add_parser(
        "finalize",
        help="Validate the reviewed facts, register the catalog entry, and publish records.",
    )
    finalize.add_argument("--experiment", required=True, type=Path)
    finalize.add_argument(
        "--review",
        type=Path,
        default=None,
        help="Default: <experiment>/analysis/record_review.yaml",
    )
    finalize.add_argument(
        "--sealed-manifest",
        type=Path,
        default=None,
        help=(
            "Reuse a parallel private artifact seal and avoid another raw-tree scan."
        ),
    )
    finalize.add_argument(
        "--private-record-bundle",
        type=Path,
        default=None,
        help=(
            "After validators pass, archive the generated record state and "
            "restore the pre-existing catalog/index/records byte-for-byte."
        ),
    )

    review = subparsers.add_parser(
        "review-check",
        help=(
            "Validate reviewed YAML and record-generation contracts without "
            "hashing the raw archive or changing generated records."
        ),
    )
    review.add_argument("--experiment", required=True, type=Path)
    review.add_argument(
        "--review",
        type=Path,
        default=None,
        help="Default: <experiment>/analysis/record_review.yaml",
    )
    return parser.parse_args()


def stable_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"


def write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    temporary.replace(path)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError(f"{repo_ref(path)} must contain a JSON object")
    return value


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"{repo_ref(path)}:{line_number}: invalid JSONL"
                ) from exc
            if not isinstance(value, dict):
                raise RuntimeError(
                    f"{repo_ref(path)}:{line_number}: row is not an object"
                )
            rows.append(value)
    return rows


def repo_ref(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def experiment_plan(owner: str, experiment_id: str) -> dict[str, str]:
    path = EXPERIMENT_PLANS / f"{experiment_id}.md"
    if not path.is_file():
        raise RuntimeError(
            "pre-experiment plan is required before post-processing: "
            f"{repo_ref(path)}"
        )
    text = path.read_text(encoding="utf-8", errors="replace")
    title_match = re.search(
        r"(?m)^#\s+实验计划[：:]\s*`?([^`\r\n]+)`?\s*$",
        text,
    )
    if title_match is None or title_match.group(1).strip() != experiment_id:
        raise RuntimeError(
            f"{repo_ref(path)} does not identify experiment {experiment_id}"
        )
    owner_match = re.search(
        r"(?m)^-\s*owner[：:]\s*`?([^`\r\n]+)`?\s*$",
        text,
    )
    if owner_match is None or owner_match.group(1).strip() != owner:
        raise RuntimeError(f"{repo_ref(path)} does not identify owner {owner}")
    return {
        "ref": repo_ref(path),
        "sha256": sha256_file(path),
    }


def owner_number(owner: str) -> int:
    match = OWNER_RE.fullmatch(owner)
    if not match:
        raise RuntimeError(f"owner must match HP_V<number>, got {owner!r}")
    return int(match.group(1))


def load_states() -> dict[str, Any]:
    value = load_json(VERSION_STATES)
    if value.get("schema") != "hybridpatch.version_states/1":
        raise RuntimeError("unsupported tools/version_states.json schema")
    return value


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def repository_git_identity() -> tuple[str, str]:
    commit_result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    status_result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    commit = (commit_result.stdout or "").strip()
    if (commit_result.returncode != 0
            or not re.fullmatch(r"[0-9a-f]{40}", commit)):
        raise RuntimeError("cannot resolve repository Git commit")
    if status_result.returncode != 0:
        raise RuntimeError("cannot resolve repository Git tree state")
    tree_state = "dirty" if (status_result.stdout or "").strip() else "clean"
    return commit, tree_state


def retained_abandonment_path(owner: str) -> Path:
    owner_number(owner)
    return RETAINED_ABANDONMENT_DIRECTORY / f"{owner}.json"


def catalogued_experiment_ids(owner: str) -> set[str]:
    catalog = load_json(CATALOG)
    return {
        str(entry.get("experiment_id"))
        for record_set in catalog.get("record_sets") or []
        if record_set.get("owner") == owner
        for entry in record_set.get("experiments") or []
    }


def unrecorded_experiment_paths(owner: str) -> list[Path]:
    owner_path = ROOT / owner
    catalogued = catalogued_experiment_ids(owner)
    return sorted(
        (
            path for path in owner_path.glob("exp_*")
            if path.is_dir() and path.name not in catalogued
        ),
        key=lambda path: path.name,
    )


def _top_level_entry_identity(experiment: Path) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for path in sorted(experiment.iterdir(), key=lambda item: item.name):
        stat = path.lstat()
        if path.is_symlink():
            kind = "symlink"
        elif path.is_dir():
            kind = "directory"
        elif path.is_file():
            kind = "file"
        else:
            kind = "other"
        entries.append({
            "name": path.name,
            "kind": kind,
            "size_bytes": stat.st_size if kind in {"file", "symlink"} else None,
        })
    return {
        "top_level_entry_count": len(entries),
        "top_level_entries_sha256": canonical_sha256(entries),
    }


def retained_experiment_identity(owner: str, experiment: Path) -> dict[str, Any]:
    if experiment.parent.resolve() != (ROOT / owner).resolve():
        raise RuntimeError("retained experiment is outside its owner directory")
    if (not experiment.is_dir() or experiment.is_symlink()
            or not experiment.name.startswith("exp_")):
        raise RuntimeError(
            f"retained experiment directory is missing or invalid: {experiment.name}"
        )
    manifest = experiment / "dispatch_manifest.json"
    if manifest.is_symlink():
        raise RuntimeError(
            f"retained experiment manifest must not be a symlink: {experiment.name}"
        )
    manifest_exists = manifest.is_file()
    identity = {
        "experiment_id": experiment.name,
        "archive_ref": f"{owner}/{experiment.name}",
        "exists": True,
        "dispatch_manifest_exists": manifest_exists,
        "dispatch_manifest_sha256": (
            sha256_file(manifest) if manifest_exists else None),
        "dispatch_manifest_size_bytes": (
            manifest.stat().st_size if manifest_exists else None),
        **_top_level_entry_identity(experiment),
    }
    identity["identity_sha256"] = canonical_sha256(identity)
    return identity


def _retained_abandonment_payload_sha256(receipt: dict[str, Any]) -> str:
    payload = dict(receipt)
    payload.pop("receipt_payload_sha256", None)
    return canonical_sha256(payload)


def _retained_abandonment_receipt_is_tracked_clean(path: Path) -> bool:
    try:
        relative = path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return False
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", relative],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return (
        tracked.returncode == 0
        and status.returncode == 0
        and not (status.stdout or "").strip()
    )


def prepare_retained_abandonment(
    owner: str,
    *,
    confirm_no_running_experiments: bool,
) -> Path:
    if not confirm_no_running_experiments:
        raise RuntimeError("--confirm-no-running-experiments is required")
    states = load_states()
    if states.get("active_version") != owner:
        raise RuntimeError(f"{owner} is not the current active version")
    commit, tree_state = repository_git_identity()
    if tree_state != "clean":
        raise RuntimeError(
            "retained-abandonment receipt requires a clean Git worktree"
        )
    experiments = unrecorded_experiment_paths(owner)
    if not experiments:
        raise RuntimeError("there are no unrecorded experiments to retain")
    identities = [
        retained_experiment_identity(owner, experiment)
        for experiment in experiments
    ]
    experiment_ids = [item["experiment_id"] for item in identities]
    receipt: dict[str, Any] = {
        "schema": RETAINED_ABANDONMENT_SCHEMA,
        "owner": owner,
        "reason": RETAINED_ABANDONMENT_REASON,
        "claim_eligible": False,
        "raw_experiments_modified": False,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source_git_commit": commit,
        "source_git_tree_state": tree_state,
        "retained_experiment_count": len(identities),
        "retained_experiment_ids": experiment_ids,
        "retained_experiment_ids_sha256": canonical_sha256(experiment_ids),
        "experiments": identities,
    }
    receipt["receipt_payload_sha256"] = (
        _retained_abandonment_payload_sha256(receipt))
    path = retained_abandonment_path(owner)
    write_atomic(path, stable_json(receipt))
    print(f"[process] retained-abandonment receipt: {repo_ref(path)}")
    print(
        "[process] commit this receipt before freeze; raw experiments were not modified"
    )
    return path


def validate_retained_abandonment(
    owner: str,
    experiments: list[Path],
    *,
    require_tracked_clean: bool,
) -> dict[str, Any]:
    path = retained_abandonment_path(owner)
    if not path.is_file() or path.is_symlink():
        raise RuntimeError(
            "unrecorded experiments require a retained-abandonment receipt"
        )
    if require_tracked_clean and not _retained_abandonment_receipt_is_tracked_clean(
            path):
        raise RuntimeError(
            "retained-abandonment receipt must be Git-tracked and clean"
        )
    receipt = load_json(path)
    payload_sha = receipt.get("receipt_payload_sha256")
    if (receipt.get("schema") != RETAINED_ABANDONMENT_SCHEMA
            or receipt.get("owner") != owner
            or receipt.get("reason") != RETAINED_ABANDONMENT_REASON
            or receipt.get("claim_eligible") is not False
            or receipt.get("raw_experiments_modified") is not False
            or receipt.get("source_git_tree_state") != "clean"
            or not re.fullmatch(
                r"[0-9a-f]{40}", str(receipt.get("source_git_commit") or ""))
            or not isinstance(receipt.get("created_at"), str)
            or not receipt.get("created_at")
            or not re.fullmatch(r"[0-9a-f]{64}", str(payload_sha or ""))
            or payload_sha != _retained_abandonment_payload_sha256(receipt)):
        raise RuntimeError("retained-abandonment receipt identity is invalid")
    current = [
        retained_experiment_identity(owner, experiment)
        for experiment in sorted(experiments, key=lambda item: item.name)
    ]
    current_ids = [item["experiment_id"] for item in current]
    if (receipt.get("retained_experiment_count") != len(current)
            or receipt.get("retained_experiment_ids") != current_ids
            or receipt.get("retained_experiment_ids_sha256")
            != canonical_sha256(current_ids)
            or receipt.get("experiments") != current):
        raise RuntimeError(
            "retained-abandonment receipt does not match the exact current "
            "unrecorded experiment set/identity"
        )
    return receipt


def _require_active_prerequisites(owner: str) -> int:
    number = owner_number(owner)
    if number < 8:
        raise RuntimeError("HP_V3-HP_V7 are frozen and cannot be activated")
    owner_path = ROOT / owner
    for required in (
        owner_path,
        owner_path / "VERSION.md",
        owner_path / "src" / "verify_anchorpatch.py",
        owner_path / "src" / "analyze.py",
        owner_path / "src" / "run_meta.py",
        owner_path / "src" / "experiment_runner.py",
    ):
        if not required.exists():
            raise RuntimeError(f"active-version prerequisite missing: {required}")
    if owner_path.is_symlink():
        raise RuntimeError("active version directory must not be a symlink")
    runtime_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in (
            owner_path / "src" / "run_meta.py",
            owner_path / "src" / "experiment_runner.py",
        )
    )
    missing_runtime_fields = [
        field
        for field in ("run_git_commit", "git_tree_state", "started_at", "finished_at")
        if field not in runtime_text
    ]
    if missing_runtime_fields:
        raise RuntimeError(
            "implement and zero-API test V8 run metadata before activation; "
            f"missing runtime fields: {missing_runtime_fields}"
        )
    return number


def _unfinished_process_state_paths(owner_path: Path) -> list[str]:
    prepared = []
    for state_path in owner_path.glob("exp_*/analysis/process_state.json"):
        state = load_json(state_path)
        if state.get("stage") != "finalized":
            prepared.append(repo_ref(state_path))
    return prepared


def _catalogued_experiment_ids(owner: str) -> set[str]:
    catalog = load_json(CATALOG)
    return {
        str(entry.get("experiment_id"))
        for record_set in catalog.get("record_sets") or []
        if record_set.get("owner") == owner
        for entry in record_set.get("experiments") or []
    }


def _unrecorded_experiment_dirs(owner: str) -> list[str]:
    owner_path = ROOT / owner
    catalogued = _catalogued_experiment_ids(owner)
    return sorted(
        path.name
        for path in owner_path.glob("exp_*")
        if path.is_dir() and path.name not in catalogued
    )


def activate_owner(owner: str) -> None:
    number = _require_active_prerequisites(owner)
    states = load_states()
    active = states.get("active_version")
    if active not in (None, owner):
        raise RuntimeError(
            f"{active} is already active; freeze it explicitly before activating {owner}"
        )
    versions = dict(states.get("versions") or {})
    versions[owner] = "active"
    states["versions"] = versions
    states["active_version"] = owner
    states["next_version"] = f"HP_V{number + 1}"
    write_atomic(VERSION_STATES, stable_json(states))
    print(f"[process] active version: {owner}")


def freeze_owner(owner: str, *, confirm_no_running_experiments: bool) -> None:
    if not confirm_no_running_experiments:
        raise RuntimeError("--confirm-no-running-experiments is required")
    number = owner_number(owner)
    if number < 8:
        raise RuntimeError("historical frozen versions are not managed by this command")
    states = load_states()
    if states.get("active_version") != owner:
        raise RuntimeError(f"{owner} is not the current active version")
    owner_path = ROOT / owner
    prepared = _unfinished_process_state_paths(owner_path)
    if prepared:
        raise RuntimeError(
            "cannot freeze with unfinished post-processing states: "
            f"{prepared}"
        )
    unrecorded = _unrecorded_experiment_dirs(owner)
    if unrecorded:
        raise RuntimeError(
            "cannot freeze with unrecorded experiment directories: "
            f"{unrecorded}"
        )
    versions = dict(states.get("versions") or {})
    versions[owner] = "frozen"
    states["versions"] = versions
    states["active_version"] = None
    states["next_version"] = f"HP_V{number + 1}"
    write_atomic(VERSION_STATES, stable_json(states))
    print(f"[process] frozen version: {owner}")


def transition_active_owner(
    from_owner: str,
    to_owner: str,
    *,
    confirm_no_running_experiments: bool,
    confirm_uncatalogued_raw_preserved: bool,
    reason: str,
) -> None:
    if not confirm_no_running_experiments:
        raise RuntimeError("--confirm-no-running-experiments is required")
    if not confirm_uncatalogued_raw_preserved:
        raise RuntimeError("--confirm-uncatalogued-raw-preserved is required")
    reason = reason.strip()
    if not reason:
        raise RuntimeError("--reason must be non-empty")
    from_number = owner_number(from_owner)
    to_number = _require_active_prerequisites(to_owner)
    if from_number < 8:
        raise RuntimeError("historical frozen versions are not managed by this command")
    if to_number != from_number + 1:
        raise RuntimeError(
            "active transition must move to the next HP version: "
            f"{from_owner} -> {to_owner}"
        )
    states = load_states()
    if states.get("active_version") != from_owner:
        raise RuntimeError(f"{from_owner} is not the current active version")
    versions = dict(states.get("versions") or {})
    if versions.get(to_owner) not in (None, "active"):
        raise RuntimeError(f"{to_owner} already has state {versions.get(to_owner)!r}")
    prepared = _unfinished_process_state_paths(ROOT / from_owner)
    if prepared:
        raise RuntimeError(
            "cannot transition with unfinished post-processing states: "
            f"{prepared}"
        )
    unrecorded_paths = unrecorded_experiment_paths(from_owner)
    if not unrecorded_paths:
        raise RuntimeError(
            "transition-active is only for preserving explicit uncatalogued "
            "historical raw directories; use freeze then activate instead"
        )
    receipt = validate_retained_abandonment(
        from_owner,
        unrecorded_paths,
        require_tracked_clean=True,
    )
    unrecorded = [path.name for path in unrecorded_paths]
    transition_record = {
        "schema": "hybridpatch.version_active_transition/1",
        "from_version": from_owner,
        "to_version": to_owner,
        "transition": "active_handoff_with_uncatalogued_raw_preserved",
        "reason": reason,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "uncatalogued_raw_experiment_directories": unrecorded,
        "uncatalogued_raw_experiment_count": len(unrecorded),
        "retained_abandonment_receipt": repo_ref(
            retained_abandonment_path(from_owner)
        ),
        "retained_abandonment_receipt_payload_sha256": receipt[
            "receipt_payload_sha256"
        ],
        "confirm_no_running_experiments": True,
        "confirm_uncatalogued_raw_preserved": True,
    }
    versions[from_owner] = "frozen"
    versions[to_owner] = "active"
    states["versions"] = versions
    states["active_version"] = to_owner
    states["next_version"] = f"HP_V{to_number + 1}"
    states.setdefault("transitions", []).append(transition_record)
    write_atomic(VERSION_STATES, stable_json(states))
    print(
        f"[process] active transition: {from_owner} -> {to_owner} "
        f"(preserved_uncatalogued_raw={len(unrecorded)})"
    )


def resolve_experiment(raw: Path) -> tuple[str, Path, Path]:
    path = raw if raw.is_absolute() else ROOT / raw
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise RuntimeError("experiment must be inside this repository") from exc
    if len(relative.parts) != 2:
        raise RuntimeError("experiment must be a direct HP_Vx/exp_* directory")
    owner, experiment_id = relative.parts
    number = owner_number(owner)
    if number < 8:
        raise RuntimeError(f"{owner} is frozen; post-process only new HP_V8+ runs")
    if not experiment_id.startswith("exp_"):
        raise RuntimeError("experiment directory name must start with exp_")
    if not resolved.is_dir():
        raise RuntimeError(f"experiment directory is missing: {relative.as_posix()}")
    if resolved.is_symlink() or (ROOT / owner).is_symlink():
        raise RuntimeError("owner and experiment directories must not be symlinks")
    states = load_states()
    if states.get("active_version") != owner:
        raise RuntimeError(
            f"{owner} is not the declared active version in tools/version_states.json"
        )
    return owner, ROOT / owner, resolved


def unique(records: Iterable[dict[str, Any]], key: str) -> list[Any]:
    output: list[Any] = []
    seen: set[str] = set()
    for record in records:
        value = record.get(key)
        if value is None:
            continue
        marker = json.dumps(value, ensure_ascii=False, sort_keys=True)
        if marker not in seen:
            seen.add(marker)
            output.append(value)
    return output


def one_value(records: list[dict[str, Any]], key: str, *, required: bool) -> Any:
    values = unique(records, key)
    if len(values) > 1:
        raise RuntimeError(f"run_metadata has conflicting {key} values: {values}")
    if required and not values:
        raise RuntimeError(f"run_metadata is missing required field {key}")
    return values[0] if values else None


def code_provenance(
    archive: Path,
    metadata: list[dict[str, Any]],
) -> dict[str, Any]:
    """Resolve one code identity or a strictly witnessed recovery transition."""
    fingerprints = unique(metadata, "code_fingerprint")
    commits = unique(metadata, "run_git_commit")
    if len(fingerprints) == 1:
        if len(commits) > 1:
            raise RuntimeError(
                f"run_metadata has conflicting run_git_commit values: {commits}"
            )
        return {
            "code_fingerprint": fingerprints[0],
            "code_fingerprints": fingerprints,
            "run_git_commit": commits[0] if commits else None,
            "run_git_commits": commits,
            "campaign_recovery_authorization": None,
            "provenance_warnings": [],
        }

    if len(fingerprints) != 2:
        raise RuntimeError(
            "new method experiment must have one code fingerprint, or exactly "
            "two covered by a recovery authorization; "
            f"found {len(fingerprints)}"
        )
    authorization_path = archive / "campaign_recovery_authorization.json"
    if not authorization_path.is_file():
        raise RuntimeError(
            "new method experiment must have one code fingerprint, found 2; "
            "campaign_recovery_authorization.json is missing"
        )
    authorization = load_json(authorization_path)
    if authorization.get("schema") != (
        "anchorpatch.campaign_recovery_authorization/1"
    ):
        raise RuntimeError("campaign recovery authorization schema is invalid")

    prior_commit = authorization.get("prior_git_commit")
    recovery_commit = authorization.get("recovery_git_commit")
    prior_fingerprint = authorization.get("prior_code_fingerprint")
    recovery_fingerprint = authorization.get("recovery_code_fingerprint")
    if not all(
        isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value)
        for value in (prior_commit, recovery_commit)
    ):
        raise RuntimeError("campaign recovery authorization commits are invalid")
    if not all(
        isinstance(value, dict)
        for value in (prior_fingerprint, recovery_fingerprint)
    ):
        raise RuntimeError(
            "campaign recovery authorization fingerprints are invalid"
        )

    def fingerprint_marker(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    expected_fingerprints = {
        fingerprint_marker(prior_fingerprint),
        fingerprint_marker(recovery_fingerprint),
    }
    observed_fingerprints = {
        fingerprint_marker(value) for value in fingerprints
    }
    if observed_fingerprints != expected_fingerprints:
        raise RuntimeError(
            "campaign recovery authorization does not match metadata fingerprints"
        )
    expected_pairs = {
        (prior_commit, fingerprint_marker(prior_fingerprint)),
        (recovery_commit, fingerprint_marker(recovery_fingerprint)),
    }
    observed_pairs: set[tuple[str, str]] = set()
    for record in metadata:
        commit = record.get("run_git_commit")
        fingerprint = record.get("code_fingerprint")
        if commit is None and fingerprint is None:
            continue
        if not isinstance(commit, str) or not isinstance(fingerprint, dict):
            raise RuntimeError(
                "authorized recovery metadata must pair every commit and fingerprint"
            )
        observed_pairs.add((commit, fingerprint_marker(fingerprint)))
    if observed_pairs != expected_pairs:
        raise RuntimeError(
            "campaign recovery authorization does not match metadata commit/fingerprint pairs"
        )

    changed_keys = sorted(
        key
        for key in set(prior_fingerprint) | set(recovery_fingerprint)
        if prior_fingerprint.get(key) != recovery_fingerprint.get(key)
    )
    if authorization.get("changed_code_fingerprint_keys") != changed_keys:
        raise RuntimeError(
            "campaign recovery authorization changed fingerprint keys are invalid"
        )
    if authorization.get("recovery_git_tree_state") != "clean":
        raise RuntimeError("campaign recovery authorization tree state is not clean")

    authorization_sha256 = sha256_file(authorization_path)
    dispatch_path = archive / "dispatch_log.jsonl"
    if not dispatch_path.is_file():
        raise RuntimeError("authorized recovery lacks dispatch_log.jsonl")
    witnesses = [
        row
        for row in load_jsonl(dispatch_path)
        if row.get("campaign_recovery_authorization_id")
        == authorization.get("authorization_id")
        and row.get("campaign_recovery_authorization_sha256")
        == authorization_sha256
        and row.get("prior_git_commit") == prior_commit
        and row.get("recovery_git_commit") == recovery_commit
    ]
    if not witnesses:
        raise RuntimeError(
            "campaign recovery authorization lacks a matching dispatch witness"
        )

    authorization_fact = {
        "schema": authorization["schema"],
        "authorization_id": authorization.get("authorization_id"),
        "path": repo_ref(authorization_path),
        "sha256": authorization_sha256,
        "prior_git_commit": prior_commit,
        "recovery_git_commit": recovery_commit,
        "changed_code_fingerprint_keys": changed_keys,
        "dispatch_witness_count": len(witnesses),
    }
    return {
        "code_fingerprint": recovery_fingerprint,
        "code_fingerprints": [prior_fingerprint, recovery_fingerprint],
        "run_git_commit": recovery_commit,
        "run_git_commits": [prior_commit, recovery_commit],
        "campaign_recovery_authorization": authorization_fact,
        "provenance_warnings": [
            "authorized recovery spans two recorded code identities; "
            f"changed fingerprint keys: {', '.join(changed_keys)}"
        ],
    }


def sha256_inputs(paths: list[Path], base: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.relative_to(base).as_posix()):
        relative = path.relative_to(base).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def result_protocols(rows: Iterable[dict[str, Any]]) -> list[str]:
    revisions: set[str] = set()
    for row in rows:
        hybrid = ((row.get("bdpatch") or {}).get("hybrid") or {})
        revision = hybrid.get("protocol_rev")
        route = hybrid.get("route") or hybrid.get("route_share_key")
        # Legacy telemetry defaults protocol_version to v1 even when no
        # envelope parsed. A route is the existing witness that the fallback
        # revision came from an observed executable envelope.
        if not revision and route:
            revision = hybrid.get("protocol_version")
        if revision:
            revisions.add(str(revision))
    return sorted(revisions)


def collect_facts(owner: str, archive: Path) -> dict[str, Any]:
    plan = experiment_plan(owner, archive.name)
    if list(archive.rglob(".env*")):
        raise RuntimeError("experiment archive contains a forbidden .env* file")
    metadata = read_quiescent_run_metadata(
        archive, repository_root=ROOT, owner=owner
    )
    if not metadata:
        raise RuntimeError("run metadata is empty")

    round_trips = int(one_value(metadata, "num_round_trips", required=True))
    seed = one_value(metadata, "seed", required=True)
    model = one_value(metadata, "model", required=True)
    distractor = one_value(metadata, "distractor", required=True)
    provider = one_value(metadata, "provider", required=False)
    transport = one_value(metadata, "transport", required=False)
    transport_revision = one_value(metadata, "transport_revision", required=True)
    thinking_mode = one_value(metadata, "thinking_mode", required=False)
    max_tokens = one_value(metadata, "effective_max_tokens", required=False)
    if max_tokens is None:
        max_tokens = one_value(metadata, "max_tokens", required=False)

    methods = [
        method for method in METHOD_NAMES if (archive / method).is_dir()
    ]
    if not methods:
        raise RuntimeError("no hybridpatch/fullrewrite/anchorpatch result directory exists")
    metadata_methods = {
        str(method)
        for record in metadata
        for method in (record.get("methods") or [])
    }
    if metadata_methods and not set(methods) <= metadata_methods:
        raise RuntimeError(
            f"result methods {methods} are not covered by metadata {sorted(metadata_methods)}"
        )

    planned = sorted(
        path.name.removesuffix(".task_plan.json")
        for path in archive.glob("*.task_plan.json")
    )
    if not planned:
        raise RuntimeError("no *.task_plan.json files found")

    input_paths = [
        *run_metadata_artifact_paths(archive),
        *archive.glob("*.task_plan.json"),
    ]
    per_method: dict[str, dict[str, dict[str, int]]] = {}
    all_result_rows: list[dict[str, Any]] = []
    result_samples: set[str] = set()
    for method in methods:
        sample_counts: dict[str, dict[str, int]] = {}
        for path in sorted((archive / method).glob("*.jsonl")):
            rows = load_jsonl(path)
            seen: set[tuple[int, str]] = set()
            counts = {"forward": 0, "backward": 0, "total": 0}
            for row in rows:
                key = (
                    int(row.get("round_trip_num")),
                    str(row.get("round_trip_direction")),
                )
                if key[1] not in {"forward", "backward"}:
                    raise RuntimeError(f"{repo_ref(path)} has invalid direction {key[1]}")
                if key in seen:
                    raise RuntimeError(f"{repo_ref(path)} has duplicate row {key}")
                seen.add(key)
                counts[key[1]] += 1
                counts["total"] += 1
                all_result_rows.append(row)
            sample_counts[path.stem] = counts
            result_samples.add(path.stem)
            input_paths.append(path)
        per_method[method] = sample_counts

    unknown_samples = sorted(result_samples - set(planned))
    if unknown_samples:
        raise RuntimeError(f"result samples lack task plans: {unknown_samples}")
    complete_samples = [
        sample
        for sample in planned
        if all(
            per_method.get(method, {}).get(sample, {}).get("total", 0)
            == round_trips * 2
            for method in methods
        )
    ]
    incomplete = {
        sample: {
            method: per_method.get(method, {}).get(
                sample,
                {"forward": 0, "backward": 0, "total": 0},
            )
            for method in methods
        }
        for sample in planned
        if sample not in complete_samples
    }
    sample_outcome_path = archive / "sample_outcomes.jsonl"
    latest_sample_outcomes: dict[str, str] = {}
    if sample_outcome_path.is_file():
        for row in load_jsonl(sample_outcome_path):
            sample = row.get("sample")
            status = row.get("status")
            if sample in planned and isinstance(status, str) and status:
                latest_sample_outcomes[str(sample)] = status
        input_paths.append(sample_outcome_path)
    evaluator_incomplete_path = archive / "evaluator_incomplete_samples.jsonl"
    if evaluator_incomplete_path.is_file():
        input_paths.append(evaluator_incomplete_path)
    incomplete_sample_outcomes = {
        sample: latest_sample_outcomes.get(sample, "missing_outcome")
        for sample in incomplete
    }

    provenance = code_provenance(archive, metadata)
    protocols = result_protocols(all_result_rows)
    if len(protocols) > 1:
        raise RuntimeError(f"mixed protocol revisions: {protocols}")
    run_commit = provenance["run_git_commit"]
    if run_commit is None:
        run_commit = one_value(metadata, "git_commit", required=False)
    git_tree_state = one_value(metadata, "git_tree_state", required=False)
    started_at = one_value(metadata, "started_at", required=False)
    finished_at = one_value(metadata, "finished_at", required=False)
    missing_provenance = [
        field
        for field, value in (
            ("run_git_commit", run_commit),
            ("git_tree_state", git_tree_state),
            ("started_at", started_at),
            ("finished_at", finished_at),
        )
        if value is None
    ]
    if missing_provenance:
        raise RuntimeError(
            "new HP_V8+ metadata is incomplete; record these fields before the "
            f"paid run/finalization: {missing_provenance}"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", str(run_commit)):
        raise RuntimeError("run_git_commit must be a 40-character lowercase hash")
    if git_tree_state not in {"clean", "dirty"}:
        raise RuntimeError("git_tree_state must be clean or dirty")
    try:
        datetime.fromisoformat(str(started_at))
    except ValueError as exc:
        raise RuntimeError("started_at must be an ISO-8601 timestamp") from exc
    try:
        datetime.fromisoformat(str(finished_at))
    except ValueError as exc:
        raise RuntimeError("finished_at must be an ISO-8601 timestamp") from exc

    if provenance["campaign_recovery_authorization"] is not None:
        input_paths.extend([
            archive / "campaign_recovery_authorization.json",
            archive / "dispatch_log.jsonl",
        ])

    return {
        "schema": "hybridpatch.derived_experiment_facts/1",
        "owner": owner,
        "experiment_id": archive.name,
        "archive_ref": repo_ref(archive),
        "experiment_plan_ref": plan["ref"],
        "experiment_plan_sha256": plan["sha256"],
        "input_sha256": sha256_inputs(input_paths, archive),
        "input_file_count": len(input_paths),
        "planned_sample_ids": planned,
        "planned_sample_count": len(planned),
        "complete_all_methods_sample_ids": complete_samples,
        "incomplete_samples": incomplete,
        "sample_outcomes": latest_sample_outcomes,
        "incomplete_sample_outcomes": incomplete_sample_outcomes,
        "methods": methods,
        "per_method_sample_counts": per_method,
        "round_trips": round_trips,
        "committed_backward_rows": sum(
            counts["backward"]
            for samples in per_method.values()
            for counts in samples.values()
        ),
        "seed": seed,
        "model": model,
        "distractor": distractor,
        "provider": provider,
        "transport": transport,
        "transport_revision": transport_revision,
        "thinking_mode": thinking_mode,
        "max_tokens": max_tokens,
        "protocol_revision": protocols[0] if protocols else None,
        "code_fingerprint": provenance["code_fingerprint"],
        "code_fingerprints": provenance["code_fingerprints"],
        "run_git_commit": run_commit,
        "run_git_commits": provenance["run_git_commits"],
        "git_tree_state": git_tree_state,
        "started_at": started_at,
        "finished_at": finished_at,
        "campaign_recovery_authorization": provenance[
            "campaign_recovery_authorization"
        ],
        "provenance_warnings": provenance["provenance_warnings"],
    }


def run_logged(
    command: list[str],
    *,
    cwd: Path,
    log_path: Path,
    display: str,
) -> subprocess.CompletedProcess[str]:
    print(f"[process] {display}", flush=True)
    environment = dict(os.environ)
    environment["PYTHONUTF8"] = "1"
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    content = (
        f"command={display}\n"
        f"exit_code={completed.returncode}\n"
        "--- stdout ---\n"
        f"{completed.stdout}"
        "--- stderr ---\n"
        f"{completed.stderr}"
    )
    write_atomic(log_path, content)
    return completed


def review_template(facts: dict[str, Any], verification_rows: int) -> dict[str, Any]:
    archive_ref = str(facts["archive_ref"])
    owner = str(facts["owner"])
    arms = {method: PLACEHOLDER for method in facts["methods"]}
    return {
        "schema": "hybridpatch.experiment_record_review/1",
        "owner": owner,
        "experiment_id": facts["experiment_id"],
        "prepared_input_sha256": facts["input_sha256"],
        "derived_facts_ref": f"{archive_ref}/analysis/derived_facts.json",
        "review_confirmations": {
            "canonical_scope_reviewed": False,
            "failure_and_exclusions_reviewed": False,
            "source_reports_reviewed": False,
            "claim_boundary_reviewed": False,
            "complete_sample_exclusions_pre_registered": False,
        },
        "review_provenance": {
            "reviewed_by": PLACEHOLDER,
            "reviewed_at": PLACEHOLDER,
            "review_method": PLACEHOLDER,
            "independent_reviews": [
                {
                    "role": PLACEHOLDER,
                    "reviewer": PLACEHOLDER,
                    "verdict": PLACEHOLDER,
                    "summary": PLACEHOLDER,
                }
            ],
            "independent_review_exception": None,
        },
        "catalog_entry": {
            "experiment_id": facts["experiment_id"],
            "scope_id": PLACEHOLDER,
            "archive_path": archive_ref,
            "status": PLACEHOLDER,
            "lifecycle_status": PLACEHOLDER,
            "evidence_role": PLACEHOLDER,
            "research_stage": PLACEHOLDER,
            "purpose": PLACEHOLDER,
            "round_trips": facts["round_trips"],
            "expected_sample_count": facts["planned_sample_count"],
            "sample_policy": PLACEHOLDER,
            "excluded_samples": {},
            "protocol_revision": facts["protocol_revision"],
            "dataset": PLACEHOLDER,
            "split": PLACEHOLDER,
            "task_plan_source": "task plans stored in the experiment archive",
            "arms": arms,
            "code_reference": f"{owner}/VERSION.md",
            "comparison_policy": PLACEHOLDER,
            "canonical_source_report": PLACEHOLDER,
            "source_reports": [
                {
                    "path": facts["experiment_plan_ref"],
                    "role": "pre_registered_experiment_plan",
                },
                {
                    "path": f"{archive_ref}/analysis/comparison.md",
                    "role": "source_analysis_review_required"
                },
                {
                    "path": f"{owner}/VERSION.md",
                    "role": "version_provenance"
                }
            ],
            "verification": {
                "status": "pass",
                "replayed_backward_rows": verification_rows,
                "canonical_backward_rows": "AUTO",
                "source": PLACEHOLDER,
                "log": f"{archive_ref}/analysis/verification.log",
                "exceptions": []
            },
            "known_exceptions": [],
            "raw_retention": PLACEHOLDER,
            "raw_credential_scan": "not_rechecked",
            "paper_reporting": None,
        },
    }


def populate_complete_pair_view(
    archive: Path,
    view: Path,
    methods: list[str],
    samples: list[str],
) -> None:
    """Copy only complete paired result rows into a temporary analysis view."""
    for method in methods:
        destination = view / method
        destination.mkdir(parents=True, exist_ok=True)
        for sample in samples:
            source = archive / method / f"{sample}.jsonl"
            if not source.is_file():
                raise RuntimeError(
                    f"complete-pair analysis source is missing: {repo_ref(source)}"
                )
            shutil.copyfile(source, destination / source.name)


def annotate_incomplete_analysis(
    analysis: Path,
    scope: dict[str, Any],
) -> None:
    comparison_path = analysis / "comparison.md"
    comparison = comparison_path.read_text(encoding="utf-8")
    incomplete = ", ".join(scope["incomplete_sample_ids"])
    notice = (
        "> Campaign status: **incomplete**. This report is a derived "
        f"complete-pair view over {scope['analysis_sample_count']}/"
        f"{scope['planned_sample_count']} planned samples. Excluded incomplete "
        f"samples: {incomplete}. Missing endpoints remain null; no zero score "
        "was imputed. This is not the canonical complete campaign endpoint.\n\n"
    )
    write_atomic(comparison_path, notice + comparison)

    endpoint_path = analysis / "sample_level_final_endpoint.json"
    endpoint = load_json(endpoint_path)
    endpoint.update({
        "campaign_complete": False,
        "campaign_expected_n": scope["planned_sample_count"],
        "campaign_incomplete_sample_ids": scope["incomplete_sample_ids"],
        "campaign_incomplete_sample_outcomes": scope.get(
            "incomplete_sample_outcomes", {}),
        "analysis_sample_policy": scope["sample_policy"],
        "score_imputed_for_incomplete_samples": False,
    })
    write_atomic(endpoint_path, stable_json(endpoint))


def prepare_experiment(
    experiment: Path,
    *,
    critical_theta: float,
    confirm_stopped: bool,
    force_review_draft: bool,
) -> None:
    if not confirm_stopped:
        raise RuntimeError(
            "--confirm-stopped is required; do not post-process a live out_dir"
        )
    if not math.isfinite(critical_theta) or critical_theta <= 0:
        raise RuntimeError("--critical-theta must be a positive finite number")
    owner, owner_path, archive = resolve_experiment(experiment)
    analysis = archive / "analysis"
    review_path = analysis / "record_review.yaml"
    if review_path.exists() and not force_review_draft:
        raise RuntimeError(
            f"{repo_ref(review_path)} already exists; use --force-review-draft "
            "only after preserving reviewed changes"
        )
    facts = collect_facts(owner, archive)
    analysis.mkdir(parents=True, exist_ok=True)

    verifier = run_logged(
        [
            sys.executable,
            "-B",
            "src/verify_anchorpatch.py",
            "--dir",
            f"./{archive.name}",
        ],
        cwd=owner_path,
        log_path=analysis / "verification.log",
        display=(
            f"python -B ./src/verify_anchorpatch.py --dir ./{archive.name}"
        ),
    )
    if verifier.returncode != 0:
        raise RuntimeError(
            f"verifier failed; inspect {repo_ref(analysis / 'verification.log')}"
        )
    match = re.search(
        r"HONESTY GATE: PASS\s+[—-]\s+(\d+) backward RS",
        verifier.stdout,
    )
    if not match:
        raise RuntimeError("verifier exited zero without the required PASS marker")
    verified_rows = int(match.group(1))
    if verified_rows != int(facts["committed_backward_rows"]):
        raise RuntimeError(
            f"verifier covered {verified_rows} backward rows, but facts contain "
            f"{facts['committed_backward_rows']}"
        )

    incomplete_sample_ids = sorted(facts["incomplete_samples"])
    analysis_scope = {
        "schema": "hybridpatch.complete_pair_analysis_scope/1",
        "campaign_complete": not incomplete_sample_ids,
        "planned_sample_count": facts["planned_sample_count"],
        "analysis_sample_count": len(facts["complete_all_methods_sample_ids"]),
        "analysis_sample_ids": facts["complete_all_methods_sample_ids"],
        "incomplete_sample_ids": incomplete_sample_ids,
        "incomplete_sample_outcomes": facts.get(
            "incomplete_sample_outcomes", {}),
        "sample_policy": "complete_all_methods",
        "score_imputed_for_incomplete_samples": False,
    }
    if incomplete_sample_ids:
        write_atomic(
            analysis / "complete_pair_analysis_scope.json",
            stable_json(analysis_scope),
        )
        with tempfile.TemporaryDirectory(
            prefix=".complete_pair_analysis_", dir=owner_path
        ) as temporary:
            view = Path(temporary)
            populate_complete_pair_view(
                archive,
                view,
                facts["methods"],
                facts["complete_all_methods_sample_ids"],
            )
            analyzer = run_logged(
                [
                    sys.executable,
                    "-B",
                    "src/analyze.py",
                    "--dir",
                    str(view),
                    "--out",
                    str(analysis),
                    "--K",
                    str(facts["round_trips"]),
                    "--critical_theta",
                    str(critical_theta),
                ],
                cwd=owner_path,
                log_path=analysis / "analysis.log",
                display=(
                    "python -B ./src/analyze.py --dir <complete-pair-view> "
                    f"--out ./{archive.name}/analysis "
                    f"--K {facts['round_trips']} "
                    f"--critical_theta {critical_theta}"
                ),
            )
    else:
        analyzer = run_logged(
            [
                sys.executable,
                "-B",
                "src/analyze.py",
                "--dir",
                f"./{archive.name}",
                "--K",
                str(facts["round_trips"]),
                "--critical_theta",
                str(critical_theta),
            ],
            cwd=owner_path,
            log_path=analysis / "analysis.log",
            display=(
                f"python -B ./src/analyze.py --dir ./{archive.name} "
                f"--K {facts['round_trips']} --critical_theta {critical_theta}"
            ),
        )
    if analyzer.returncode != 0:
        raise RuntimeError(
            f"analysis failed; inspect {repo_ref(analysis / 'analysis.log')}"
        )
    for required in (
        analysis / "comparison.md",
        analysis / "experiment_results.csv",
    ):
        if not required.is_file():
            raise RuntimeError(f"analysis output missing: {repo_ref(required)}")
    if incomplete_sample_ids:
        annotate_incomplete_analysis(analysis, analysis_scope)

    facts["verification"] = {
        "status": "pass",
        "replayed_backward_rows": verified_rows,
        "log_ref": repo_ref(analysis / "verification.log"),
    }
    facts["analysis"] = {
        "comparison_ref": repo_ref(analysis / "comparison.md"),
        "rows_ref": repo_ref(analysis / "experiment_results.csv"),
        "log_ref": repo_ref(analysis / "analysis.log"),
        "critical_theta": critical_theta,
        "campaign_complete": analysis_scope["campaign_complete"],
        "sample_policy": analysis_scope["sample_policy"],
        "analysis_sample_count": analysis_scope["analysis_sample_count"],
        "incomplete_sample_ids": analysis_scope["incomplete_sample_ids"],
        "incomplete_sample_outcomes": analysis_scope.get(
            "incomplete_sample_outcomes", {}),
        "score_imputed_for_incomplete_samples": False,
    }
    if incomplete_sample_ids:
        facts["analysis"]["scope_ref"] = repo_ref(
            analysis / "complete_pair_analysis_scope.json"
        )
    write_atomic(analysis / "derived_facts.json", stable_json(facts))
    review = review_template(facts, verified_rows)
    yaml_content = yaml.safe_dump(
        review,
        allow_unicode=True,
        sort_keys=False,
        width=100,
    )
    write_atomic(review_path, yaml_content)
    write_atomic(
        analysis / "process_state.json",
        stable_json(
            {
                "schema": "hybridpatch.experiment_process_state/1",
                "experiment_id": archive.name,
                "stage": "prepared_for_human_review",
                "input_sha256": facts["input_sha256"],
                "review_ref": repo_ref(review_path),
            }
        ),
    )
    print(f"[process] derived facts: {repo_ref(analysis / 'derived_facts.json')}")
    print(f"[process] review required: {repo_ref(review_path)}")
    print("[process] PREPARE PASS")


def contains_placeholder(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        upper = value.upper()
        return PLACEHOLDER in upper or "TODO" in upper or "CHANGEME" in upper
    if isinstance(value, dict):
        return any(contains_placeholder(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_placeholder(item) for item in value)
    return False


def selected_samples(
    entry: dict[str, Any],
    facts: dict[str, Any],
) -> tuple[list[str], set[str]]:
    planned = list(facts["planned_sample_ids"])
    complete = set(facts["complete_all_methods_sample_ids"])
    excluded = set((entry.get("excluded_samples") or {}).keys())
    policy = entry.get("sample_policy")
    if policy == "complete_all_methods":
        selected = complete
    elif policy == "exclude":
        selected = set(planned) - excluded
        incomplete_selected = selected - complete
        if incomplete_selected:
            raise RuntimeError(
                f"exclude policy leaves incomplete canonical samples: "
                f"{sorted(incomplete_selected)}"
            )
    elif policy == "all_result_samples":
        selected = {
            sample
            for sample in planned
            if any(
                facts["per_method_sample_counts"]
                .get(method, {})
                .get(sample, {})
                .get("total", 0)
                > 0
                for method in facts["methods"]
            )
        }
    else:
        raise RuntimeError(
            "new standard HP experiments support complete_all_methods, exclude, "
            "or all_result_samples"
        )
    return sorted(selected), excluded


def validate_review(
    review: dict[str, Any],
    facts: dict[str, Any],
) -> dict[str, Any]:
    if review.get("schema") != "hybridpatch.experiment_record_review/1":
        raise RuntimeError("unsupported review schema")
    if review.get("owner") != facts["owner"]:
        raise RuntimeError("review owner differs from derived facts")
    if review.get("experiment_id") != facts["experiment_id"]:
        raise RuntimeError("review experiment_id differs from derived facts")
    if review.get("prepared_input_sha256") != facts["input_sha256"]:
        raise RuntimeError("review was prepared from a different input digest")
    confirmations = review.get("review_confirmations")
    if not isinstance(confirmations, dict):
        raise RuntimeError("review_confirmations must be an object")
    required_confirmations = (
        "canonical_scope_reviewed",
        "failure_and_exclusions_reviewed",
        "source_reports_reviewed",
        "claim_boundary_reviewed",
    )
    missing_confirmations = [
        field for field in required_confirmations if confirmations.get(field) is not True
    ]
    if missing_confirmations:
        raise RuntimeError(
            f"review confirmations are incomplete: {missing_confirmations}"
        )
    review_provenance = review.get("review_provenance")
    if not isinstance(review_provenance, dict):
        raise RuntimeError("review_provenance must be an object")
    for field in ("reviewed_by", "reviewed_at", "review_method"):
        value = review_provenance.get(field)
        if (
            not isinstance(value, str)
            or not value
            or contains_placeholder(value)
        ):
            raise RuntimeError(f"review_provenance.{field} must be reviewed")
    try:
        datetime.fromisoformat(review_provenance["reviewed_at"])
    except ValueError as exc:
        raise RuntimeError(
            "review_provenance.reviewed_at must be an ISO-8601 timestamp"
        ) from exc
    independent_reviews = review_provenance.get("independent_reviews")
    if not isinstance(independent_reviews, list):
        raise RuntimeError("review_provenance.independent_reviews must be a list")
    for index, independent in enumerate(independent_reviews):
        if not isinstance(independent, dict):
            raise RuntimeError(
                f"review_provenance.independent_reviews[{index}] must be an object"
            )
        for field in ("role", "reviewer", "verdict", "summary"):
            value = independent.get(field)
            if (
                not isinstance(value, str)
                or not value
                or contains_placeholder(value)
            ):
                raise RuntimeError(
                    "review_provenance.independent_reviews"
                    f"[{index}].{field} must be reviewed"
                )
    exception = review_provenance.get("independent_review_exception")
    if not independent_reviews and (
        not isinstance(exception, str)
        or not exception
        or contains_placeholder(exception)
    ):
        raise RuntimeError(
            "record at least one independent review or a non-empty "
            "independent_review_exception"
        )
    if exception is not None and not isinstance(exception, str):
        raise RuntimeError(
            "review_provenance.independent_review_exception must be null or a string"
        )
    entry = review.get("catalog_entry")
    if not isinstance(entry, dict):
        raise RuntimeError("catalog_entry must be an object")
    if contains_placeholder(entry):
        raise RuntimeError("catalog_entry still contains REVIEW_REQUIRED/TODO values")
    required_text_fields = (
        "experiment_id",
        "scope_id",
        "archive_path",
        "status",
        "lifecycle_status",
        "evidence_role",
        "research_stage",
        "purpose",
        "dataset",
        "split",
        "task_plan_source",
        "code_reference",
        "comparison_policy",
        "raw_retention",
        "raw_credential_scan",
    )
    for field in required_text_fields:
        if not isinstance(entry.get(field), str) or not entry.get(field):
            raise RuntimeError(f"catalog_entry.{field} must be a non-empty string")
    for field, allowed in (
        ("status", CATALOG_STATUSES),
        ("lifecycle_status", LIFECYCLE_STATUSES),
        ("evidence_role", CATALOG_EVIDENCE_ROLES),
    ):
        if entry[field] not in allowed:
            raise RuntimeError(
                f"catalog_entry.{field} has unsupported value {entry[field]!r}; "
                f"expected one of {sorted(allowed)}"
            )
    if not isinstance(entry.get("source_reports"), list) or not entry["source_reports"]:
        raise RuntimeError("catalog_entry.source_reports must be a non-empty list")
    for index, report in enumerate(entry["source_reports"]):
        if (
            not isinstance(report, dict)
            or not isinstance(report.get("path"), str)
            or not report.get("path")
            or not isinstance(report.get("role"), str)
            or not report.get("role")
        ):
            raise RuntimeError(f"source_reports[{index}] is incomplete")
    expected_plan_report = {
        "path": facts["experiment_plan_ref"],
        "role": "pre_registered_experiment_plan",
    }
    if expected_plan_report not in entry["source_reports"]:
        raise RuntimeError(
            "source_reports must include the pre-registered experiment plan"
        )
    excluded_values = entry.get("excluded_samples")
    if excluded_values is not None and (
        not isinstance(excluded_values, dict)
        or any(
            not isinstance(sample, str)
            or not sample
            or not isinstance(reason, str)
            or not reason
            for sample, reason in excluded_values.items()
        )
    ):
        raise RuntimeError("excluded_samples must map sample ids to non-empty reasons")
    if entry.get("experiment_id") != facts["experiment_id"]:
        raise RuntimeError("catalog entry experiment_id mismatch")
    if entry.get("archive_path") != facts["archive_ref"]:
        raise RuntimeError("catalog entry archive_path mismatch")
    if int(entry.get("round_trips", -1)) != int(facts["round_trips"]):
        raise RuntimeError("catalog entry round_trips differs from observed facts")
    if int(entry.get("expected_sample_count", -1)) != int(
        facts["planned_sample_count"]
    ):
        raise RuntimeError("catalog expected_sample_count differs from task plans")
    if set((entry.get("arms") or {}).keys()) != set(facts["methods"]):
        raise RuntimeError("catalog arm names differ from observed result methods")
    if entry.get("protocol_revision") != facts.get("protocol_revision"):
        raise RuntimeError("catalog protocol_revision differs from observed envelopes")
    selected, excluded = selected_samples(entry, facts)
    complete_excluded = excluded & set(facts["complete_all_methods_sample_ids"])
    if complete_excluded and confirmations.get(
        "complete_sample_exclusions_pre_registered"
    ) is not True:
        raise RuntimeError(
            "complete samples are excluded without the pre-registration confirmation: "
            f"{sorted(complete_excluded)}"
        )
    if (
        entry.get("lifecycle_status") == "complete"
        and entry.get("sample_policy") == "complete_all_methods"
        and len(selected) != len(facts["planned_sample_ids"])
    ):
        raise RuntimeError("complete lifecycle cannot hide incomplete planned samples")

    canonical_backward = sum(
        int(
            facts["per_method_sample_counts"]
            .get(method, {})
            .get(sample, {})
            .get("backward", 0)
        )
        for method in facts["methods"]
        for sample in selected
    )
    verification = entry.get("verification")
    if not isinstance(verification, dict):
        raise RuntimeError("catalog verification must be an object")
    if verification.get("status") != "pass":
        raise RuntimeError("new V8+ finalization requires verifier status pass")
    if int(verification.get("replayed_backward_rows", -1)) != int(
        facts["committed_backward_rows"]
    ):
        raise RuntimeError("verification replay count differs from prepare facts")
    verification["canonical_backward_rows"] = canonical_backward
    if {"hybridpatch", "fullrewrite"} <= set(facts["methods"]):
        paired_backward = sum(
            min(
                int(
                    facts["per_method_sample_counts"]["hybridpatch"]
                    .get(sample, {})
                    .get("backward", 0)
                ),
                int(
                    facts["per_method_sample_counts"]["fullrewrite"]
                    .get(sample, {})
                    .get("backward", 0)
                ),
            )
            for sample in selected
        )
        verification["canonical_paired_backward_rows"] = paired_backward
    entry["verification"] = verification
    entry["review_provenance"] = review_provenance
    return entry


def snapshot_generated(
    catalog: dict[str, Any],
    destination: Path,
    *,
    extra_owners: Iterable[str] = (),
) -> list[tuple[Path, Path]]:
    targets: list[Path] = [CATALOG, ROOT / "docs" / "EXPERIMENT_INDEX.md"]
    owners = {
        str(record_set["owner"])
        for record_set in catalog.get("record_sets") or []
    } | set(extra_owners)
    for owner_name in sorted(owners):
        owner = ROOT / owner_name
        targets.extend([owner / "EXPERIMENTS.md", owner / "records"])
    snapshots: list[tuple[Path, Path]] = []
    for index, target in enumerate(targets):
        backup = destination / f"item_{index}"
        if target.is_dir():
            shutil.copytree(target, backup)
        elif target.is_file():
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, backup)
        snapshots.append((target, backup))
    return snapshots


def restore_generated(snapshots: list[tuple[Path, Path]]) -> None:
    for target, backup in snapshots:
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
        if backup.is_dir():
            shutil.copytree(backup, target)
        elif backup.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup, target)


def orphan_record_directories(
    catalog: dict[str, Any],
    *,
    extra_owners: Iterable[str] = (),
) -> list[Path]:
    declared: dict[str, set[str]] = {}
    for record_set in catalog.get("record_sets") or []:
        owner = str(record_set["owner"])
        declared[owner] = {
            str(entry["experiment_id"])
            for entry in record_set.get("experiments") or []
        }
    for owner in extra_owners:
        declared.setdefault(owner, set())
    orphans: list[Path] = []
    for owner, experiment_ids in sorted(declared.items()):
        records = ROOT / owner / "records"
        if not records.is_dir():
            continue
        for candidate in sorted(records.iterdir()):
            if candidate.is_dir() and candidate.name not in experiment_ids:
                orphans.append(candidate)
    return orphans


def write_private_record_bundle(
    destination: Path,
    *,
    owner: str,
    experiment_id: str,
) -> str:
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.unlink(missing_ok=True)
    sources = (
        CATALOG,
        ROOT / "docs" / "EXPERIMENT_INDEX.md",
        ROOT / owner / "EXPERIMENTS.md",
        ROOT / owner / "records" / experiment_id,
    )
    try:
        with tarfile.open(temporary, "w:gz") as archive:
            for source in sources:
                if not source.exists():
                    raise RuntimeError(
                        f"private record bundle source is missing: {repo_ref(source)}"
                    )
                archive.add(source, arcname=repo_ref(source), recursive=True)
        temporary.replace(destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return sha256_file(destination)


def register_entry(catalog: dict[str, Any], owner: str, entry: dict[str, Any]) -> None:
    for record_set in catalog.get("record_sets") or []:
        for existing in record_set.get("experiments") or []:
            if existing.get("experiment_id") == entry["experiment_id"]:
                raise RuntimeError(
                    f"experiment_id already exists: {entry['experiment_id']}"
                )
    record_set = next(
        (
            item
            for item in catalog.get("record_sets") or []
            if item.get("owner") == owner
        ),
        None,
    )
    if record_set is None:
        record_set = {
            "owner": owner,
            "version_reference": f"{owner}/VERSION.md",
            "experiments": [],
        }
        catalog.setdefault("record_sets", []).append(record_set)
    record_set.setdefault("experiments", []).append(entry)


def reviewed_finalization_inputs(
    experiment: Path,
    review_path: Path | None,
) -> tuple[str, Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    owner, _, archive = resolve_experiment(experiment)
    facts_path = archive / "analysis" / "derived_facts.json"
    if not facts_path.is_file():
        raise RuntimeError("prepare has not produced analysis/derived_facts.json")
    facts = load_json(facts_path)
    current = collect_facts(owner, archive)
    if current["input_sha256"] != facts.get("input_sha256"):
        raise RuntimeError("raw result inputs changed after prepare; run prepare again")
    if current["experiment_plan_sha256"] != facts.get("experiment_plan_sha256"):
        raise RuntimeError("experiment plan changed after prepare; run prepare again")
    selected_review = review_path or archive / "analysis" / "record_review.yaml"
    if not selected_review.is_absolute():
        selected_review = ROOT / selected_review
    with selected_review.open(encoding="utf-8") as handle:
        review = yaml.safe_load(handle)
    if not isinstance(review, dict):
        raise RuntimeError("review YAML must contain an object")
    entry = validate_review(review, facts)
    catalog = load_json(CATALOG)
    register_entry(catalog, owner, entry)
    return owner, archive, facts, entry, catalog


def review_check_experiment(
    experiment: Path,
    review_path: Path | None,
) -> None:
    owner, archive, _, entry, catalog = reviewed_finalization_inputs(
        experiment,
        review_path,
    )
    with tempfile.TemporaryDirectory(
        prefix="hybridpatch-review-contract-"
    ) as temporary:
        prospective_catalog = Path(temporary) / "catalog.json"
        write_atomic(prospective_catalog, stable_json(catalog))
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "tools" / "build_experiment_records.py"),
                "--catalog",
                str(prospective_catalog),
                "--only",
                entry["experiment_id"],
                "--skip-tree-hash",
                "--validate-only",
            ],
            cwd=ROOT,
            env={**os.environ, "PYTHONUTF8": "1"},
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "review/catalog record contract failed before raw-tree hashing"
            )
    print(
        f"[process] review contract: {owner}/{archive.name} (zero raw-tree scan)"
    )
    print("[process] REVIEW-CHECK PASS")


def finalize_experiment(
    experiment: Path,
    review_path: Path | None,
    sealed_manifest: Path | None = None,
    private_record_bundle: Path | None = None,
) -> None:
    owner, archive, facts, _, catalog = reviewed_finalization_inputs(
        experiment,
        review_path,
    )
    analysis = archive / "analysis"
    record_directory = ROOT / owner / "records" / archive.name
    if record_directory.exists() and private_record_bundle is None:
        raise RuntimeError(
            f"record directory already exists; refusing overwrite: {repo_ref(record_directory)}"
        )
    original_catalog = load_json(CATALOG)

    private_bundle_sha256 = None
    with tempfile.TemporaryDirectory(
        prefix=".hybridpatch-record-backup-",
        dir=ROOT,
    ) as temporary:
        snapshots = snapshot_generated(
            original_catalog,
            Path(temporary),
            extra_owners=[owner],
        )
        try:
            if private_record_bundle is not None:
                orphan_root = Path(temporary) / "orphans"
                for orphan in orphan_record_directories(
                    original_catalog,
                    extra_owners=[owner],
                ):
                    destination = orphan_root / orphan.relative_to(ROOT)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(orphan), str(destination))
            write_atomic(CATALOG, stable_json(catalog))
            command = [sys.executable, str(FINALIZER), archive.name]
            if sealed_manifest is not None:
                command.extend(
                    ["--sealed-manifest", str(sealed_manifest.resolve())]
                )
            completed = subprocess.run(
                command,
                cwd=ROOT,
                env={**os.environ, "PYTHONUTF8": "1"},
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"finalize_experiment.py failed with {completed.returncode}"
                )
            if private_record_bundle is not None:
                private_bundle_sha256 = write_private_record_bundle(
                    private_record_bundle,
                    owner=owner,
                    experiment_id=archive.name,
                )
        except Exception:
            restore_generated(snapshots)
            if private_record_bundle is None and record_directory.exists():
                shutil.rmtree(record_directory)
            raise
        if private_record_bundle is not None:
            restore_generated(snapshots)

    public_record_sha256 = None
    if private_record_bundle is None:
        public_report = record_directory / "report.md"
        if not public_report.is_file():
            raise RuntimeError(
                "finalizer did not publish the canonical public report"
            )
        public_record_sha256 = sha256_file(public_report)

    write_atomic(
        analysis / "process_state.json",
        stable_json(
            {
                "schema": "hybridpatch.experiment_process_state/1",
                "experiment_id": archive.name,
                "stage": (
                    "finalized_private"
                    if private_record_bundle is not None
                    else "finalized"
                ),
                "input_sha256": facts["input_sha256"],
                "record_ref": (
                    str(private_record_bundle.resolve())
                    if private_record_bundle is not None
                    else f"{owner}/records/{archive.name}/report.md"
                ),
                "public_record_sha256": public_record_sha256,
                "private_record_bundle_sha256": private_bundle_sha256,
            }
        ),
    )
    if private_record_bundle is not None:
        print(
            "[process] private record bundle: "
            f"{private_record_bundle.resolve()} sha256={private_bundle_sha256}"
        )
        print("[process] generated catalog/index/records restored")
    else:
        print(f"[process] report: {owner}/records/{archive.name}/report.md")
    print("[process] FINALIZE PASS")


def main() -> int:
    args = parse_args()
    if args.command == "activate":
        activate_owner(args.owner)
    elif args.command == "freeze":
        freeze_owner(
            args.owner,
            confirm_no_running_experiments=args.confirm_no_running_experiments,
        )
    elif args.command == "prepare-retained-abandonment":
        prepare_retained_abandonment(
            args.owner,
            confirm_no_running_experiments=args.confirm_no_running_experiments,
        )
    elif args.command == "transition-active":
        transition_active_owner(
            args.from_owner,
            args.to_owner,
            confirm_no_running_experiments=args.confirm_no_running_experiments,
            confirm_uncatalogued_raw_preserved=(
                args.confirm_uncatalogued_raw_preserved
            ),
            reason=args.reason,
        )
    elif args.command == "prepare":
        prepare_experiment(
            args.experiment,
            critical_theta=args.critical_theta,
            confirm_stopped=args.confirm_stopped,
            force_review_draft=args.force_review_draft,
        )
    elif args.command == "finalize":
        finalize_experiment(
            args.experiment,
            args.review,
            args.sealed_manifest,
            args.private_record_bundle,
        )
    elif args.command == "review-check":
        review_check_experiment(args.experiment, args.review)
    else:
        raise RuntimeError(f"unsupported command: {args.command}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"[process] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
