"""Read version-local run metadata without trusting a stale projection cache."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from pathlib import Path
from types import ModuleType


EVENTS_FILENAME = "run_metadata_events.jsonl"
SNAPSHOT_FILENAME = "run_metadata.jsonl"
RECEIPT_FILENAME = "run_metadata_projection_receipt.json"
PENDING_FILENAME = "run_metadata_event_pending.json"
RECOVERY_DIRECTORY = "run_metadata_event_recoveries"
EVENT_STORAGE_V1 = "event_v1"
OWNER_RE = re.compile(r"^HP_V\d+$")


def _read_legacy_jsonl(path: Path, *, required: bool) -> list[dict]:
    if not path.is_file():
        if required:
            raise RuntimeError(f"{SNAPSHOT_FILENAME} is required")
        return []
    rows: list[dict] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise RuntimeError(
                        f"run metadata row is not an object: {path}:{line_number}"
                    )
                rows.append(row)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read valid run metadata: {path}") from exc
    return rows


def _owner_for_archive(
    archive: Path, repository_root: Path, owner: str | None
) -> str:
    if owner is not None:
        if not OWNER_RE.fullmatch(owner):
            raise RuntimeError(f"invalid experiment owner for event metadata: {owner!r}")
        try:
            relative = archive.resolve().relative_to(repository_root.resolve())
        except ValueError:
            return owner
        path_owners = [part for part in relative.parts if OWNER_RE.fullmatch(part)]
        if path_owners and owner not in path_owners:
            raise RuntimeError(
                "event metadata owner does not match the archive path")
        return owner
    try:
        relative = archive.resolve().relative_to(repository_root.resolve())
    except ValueError as exc:
        raise RuntimeError(
            "event metadata archive must be inside a version directory"
        ) from exc
    for part in relative.parts:
        if OWNER_RE.fullmatch(part):
            return part
    raise RuntimeError("cannot resolve the owner of event run metadata")


def _load_version_run_meta(source_path_text: str) -> ModuleType:
    source_path = Path(source_path_text)
    module_name = (
        "_anchorpatch_versioned_run_meta_"
        + hashlib.sha256(str(source_path).encode("utf-8")).hexdigest()[:16]
    )
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load version-local run metadata reader: {source_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_quiescent_run_metadata(
    archive: Path,
    *,
    repository_root: Path,
    owner: str | None = None,
    required: bool = True,
) -> list[dict]:
    """Read legacy `/3`, or the exact final projection of an event campaign."""
    archive = Path(archive)
    events_path = archive / EVENTS_FILENAME
    snapshot_path = archive / SNAPSHOT_FILENAME
    receipt_path = archive / RECEIPT_FILENAME
    pending_path = archive / PENDING_FILENAME
    declared_storage = None
    manifest_path = archive / "dispatch_manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RuntimeError("dispatch manifest is invalid") from exc
        config = manifest.get("config") if isinstance(manifest, dict) else None
        declared_storage = (
            config.get("run_metadata_storage")
            if isinstance(config, dict) else None
        )
        if declared_storage not in {None, EVENT_STORAGE_V1}:
            raise RuntimeError("dispatch manifest run metadata storage is invalid")
    if not events_path.is_file() and not pending_path.is_file():
        if declared_storage == EVENT_STORAGE_V1:
            raise RuntimeError(
                "dispatch manifest requires a missing run metadata event ledger"
            )
        if receipt_path.exists():
            raise RuntimeError(
                "run metadata projection receipt exists without event ledger"
            )
        return _read_legacy_jsonl(snapshot_path, required=required)

    version_owner = _owner_for_archive(
        archive, Path(repository_root), owner
    )
    source_path = Path(repository_root) / version_owner / "src" / "run_meta.py"
    if not source_path.is_file():
        raise RuntimeError(
            f"version-local run metadata reader is missing: {source_path}"
        )
    module = _load_version_run_meta(str(source_path.resolve()))
    reader = getattr(module, "read_quiescent_run_metadata_snapshot", None)
    if not callable(reader):
        raise RuntimeError(
            f"{version_owner} does not support event run metadata finalization"
        )
    records = reader(str(archive))
    if not isinstance(records, list) or any(
        not isinstance(record, dict) for record in records
    ):
        raise RuntimeError("version-local run metadata projection is invalid")
    if required and not records:
        raise RuntimeError("run metadata projection is empty")
    return records


def run_metadata_artifact_paths(archive: Path) -> list[Path]:
    """Return every metadata artifact whose bytes affect offline provenance."""
    archive = Path(archive)
    events_path = archive / EVENTS_FILENAME
    recovery_dir = archive / RECOVERY_DIRECTORY
    event_artifacts_exist = any(
        path.exists()
        for path in (
            events_path,
            archive / RECEIPT_FILENAME,
            archive / PENDING_FILENAME,
            recovery_dir,
        )
    )
    if event_artifacts_exist:
        paths = [
            path
            for path in (
                events_path,
                archive / SNAPSHOT_FILENAME,
                archive / RECEIPT_FILENAME,
                archive / PENDING_FILENAME,
            )
            if path.is_file()
        ]
        if recovery_dir.is_dir():
            paths.extend(sorted(recovery_dir.glob("*.json")))
        return paths
    snapshot_path = archive / SNAPSHOT_FILENAME
    return [snapshot_path] if snapshot_path.is_file() else []
