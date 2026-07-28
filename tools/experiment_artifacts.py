"""Offline experiment artifact sealing and cache validation.

The sealer runs the repository's authoritative content/credential scan and a
native Git-Bash ``tar`` concurrently. The resulting seal is reused by record
generation, avoiding another multi-gigabyte content scan during finalize while
keeping first-run wall time close to the slower of scan and compression.

No secret value is written to the seal, scan report, archive, or console.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
SEAL_SCHEMA = "hybridpatch.experiment_artifact_seal/1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def quick_tree_fingerprint(
    root: Path,
    *,
    excluded_prefixes: Iterable[str] = (),
) -> dict[str, Any]:
    """Cheap cache key over path, size and mtime; it does not read contents."""

    digest = hashlib.sha256()
    file_count = 0
    size_bytes = 0
    skipped_links: list[str] = []
    skipped_sensitive: list[str] = []
    excluded = tuple(prefix.rstrip("/") for prefix in excluded_prefixes)
    for candidate in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = candidate.relative_to(root).as_posix()
        if any(
            relative == prefix or relative.startswith(prefix + "/")
            for prefix in excluded
        ):
            continue
        if candidate.is_symlink():
            skipped_links.append(relative)
            continue
        if not candidate.is_file():
            continue
        if candidate.name == ".env" or candidate.name.startswith(".env."):
            skipped_sensitive.append(relative)
            continue
        stat = candidate.stat()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        digest.update(b"\n")
        file_count += 1
        size_bytes += stat.st_size
    return {
        "algorithm": "path-size-mtime-ns-v1",
        "sha256": digest.hexdigest(),
        "file_count": file_count,
        "size_bytes": size_bytes,
        "skipped_sensitive_files": skipped_sensitive,
        "skipped_symlinks": skipped_links,
    }


def _content_tree_identity(root: Path) -> dict[str, Any]:
    """Return the strong, credential-free identity stored inside a seal."""

    from build_experiment_records import tree_digest

    tree = tree_digest(root, scan_credentials=False)
    return {
        key: tree.get(key)
        for key in (
            "algorithm",
            "tree_sha256",
            "file_count",
            "size_bytes",
            "skipped_sensitive_files",
            "skipped_symlinks",
        )
    }


def _stored_tree_identity(tree: Any) -> dict[str, Any] | None:
    if not isinstance(tree, dict):
        return None
    return {
        key: tree.get(key)
        for key in (
            "algorithm",
            "tree_sha256",
            "file_count",
            "size_bytes",
            "skipped_sensitive_files",
            "skipped_symlinks",
        )
    }


def _load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return value


def _cache_matches(source: Path, archive: Path, seal_path: Path) -> bool:
    if not archive.is_file() or not seal_path.is_file():
        return False
    try:
        seal = _load_json(seal_path)
    except (OSError, ValueError, RuntimeError):
        return False
    if seal.get("schema") != SEAL_SCHEMA:
        return False
    if seal.get("experiment_id") != source.name:
        return False
    quick = quick_tree_fingerprint(source)
    archive_stat = archive.stat()
    return (
        seal.get("source_quick_fingerprint") == quick
        and _stored_tree_identity(seal.get("tree"))
        == _content_tree_identity(source)
        and seal.get("archive", {}).get("size_bytes") == archive_stat.st_size
        and seal.get("archive", {}).get("mtime_ns") == archive_stat.st_mtime_ns
        and seal.get("archive", {}).get("sha256") == sha256_file(archive)
    )


def _native_tar_archive(source: Path, destination: Path) -> None:
    executable = shutil.which("tar")
    if executable is None:
        raise RuntimeError(
            "native tar is unavailable; run post-processing from the required Git Bash"
        )
    def git_bash_path(path: Path) -> str:
        resolved = path.resolve()
        drive = resolved.drive
        if drive:
            tail = str(resolved)[len(drive) :].replace("\\", "/")
            return f"/{drive[0].lower()}{tail}"
        return resolved.as_posix()

    completed = subprocess.run(
        [
            executable,
            "-czf",
            git_bash_path(destination),
            "-C",
            git_bash_path(source.parent),
            source.name,
        ],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"native tar failed with exit code {completed.returncode}: {detail}"
        )


def seal_experiment(
    source: Path,
    archive: Path,
    *,
    force: bool = False,
    root: Path = ROOT,
) -> tuple[dict[str, Any], bool]:
    """Create or reuse a private archive and its content/credential seal."""

    source = source.resolve()
    archive = archive.resolve()
    seal_path = archive.with_name(archive.name + ".seal.json")
    if not source.is_dir():
        raise RuntimeError(f"experiment directory is missing: {source}")
    if not force and _cache_matches(source, archive, seal_path):
        return _load_json(seal_path), True
    if (archive.exists() or seal_path.exists()) and not force:
        raise RuntimeError(
            "archive or seal already exists but the source cache key changed; "
            "choose a new archive name or use --force"
        )

    initial_quick = quick_tree_fingerprint(source)
    if initial_quick["skipped_sensitive_files"]:
        raise RuntimeError(
            "experiment contains forbidden .env-like files: "
            f"{initial_quick['skipped_sensitive_files']}"
        )
    if initial_quick["skipped_symlinks"]:
        raise RuntimeError(
            "experiment contains symlinks that cannot be sealed safely: "
            f"{initial_quick['skipped_symlinks']}"
        )
    archive.parent.mkdir(parents=True, exist_ok=True)
    temporary_archive = archive.with_name(archive.name + ".tmp")
    if temporary_archive.exists():
        temporary_archive.unlink()

    generated_at = datetime.now().astimezone().isoformat()
    try:
        from build_experiment_records import tree_digest

        with ThreadPoolExecutor(max_workers=2) as executor:
            scan_future = executor.submit(
                tree_digest,
                source,
                scan_credentials=True,
                secret_root=root,
            )
            archive_future = executor.submit(
                _native_tar_archive,
                source,
                temporary_archive,
            )
            tree = scan_future.result()
            archive_future.result()
        final_quick = quick_tree_fingerprint(source)
        if final_quick != initial_quick:
            temporary_archive.unlink(missing_ok=True)
            raise RuntimeError(
                "experiment changed while artifact sealing was in progress"
            )
        scan = tree.get("credential_scan") or {}
        if scan.get("exact_local_secret_match_count"):
            temporary_archive.unlink(missing_ok=True)
            raise RuntimeError(
                "exact local secret value detected; private archive was not published"
            )
        if not str(scan.get("status") or "").startswith("pass"):
            temporary_archive.unlink(missing_ok=True)
            raise RuntimeError(
                "credential scan is not complete/pass; private archive was not published"
            )
        archive_sha256 = sha256_file(temporary_archive)
        temporary_archive.replace(archive)
        archive_stat = archive.stat()
        seal = {
            "schema": SEAL_SCHEMA,
            "experiment_id": source.name,
            "source_ref": source.relative_to(root.resolve()).as_posix(),
            "created_at": generated_at,
            "tree": tree,
            "source_quick_fingerprint": final_quick,
            "archive": {
                "format": "tar+gzip",
                "sha256": archive_sha256,
                "size_bytes": archive_stat.st_size,
                "mtime_ns": archive_stat.st_mtime_ns,
                "member_root": source.name,
                "scan_report_member": None,
                "scan_execution": "parallel_with_native_tar",
            },
        }
        write_atomic(seal_path, stable_json(seal))
        return seal, False
    except BaseException:
        temporary_archive.unlink(missing_ok=True)
        raise


def load_valid_seal(
    path: Path,
    *,
    experiment_id: str,
    source: Path,
    root: Path = ROOT,
) -> dict[str, Any]:
    seal = _load_json(path)
    if seal.get("schema") != SEAL_SCHEMA:
        raise RuntimeError("unsupported sealed artifact manifest schema")
    if seal.get("experiment_id") != experiment_id:
        raise RuntimeError("sealed artifact experiment_id mismatch")
    expected_ref = source.resolve().relative_to(root.resolve()).as_posix()
    if seal.get("source_ref") != expected_ref:
        raise RuntimeError("sealed artifact source_ref mismatch")
    tree = seal.get("tree")
    if not isinstance(tree, dict):
        raise RuntimeError("sealed artifact tree must be an object")
    if tree.get("algorithm") != "sha256-tree-v1" or not SHA256_RE.fullmatch(
        str(tree.get("tree_sha256") or "")
    ):
        raise RuntimeError("sealed artifact tree digest is invalid")
    scan = tree.get("credential_scan")
    if (
        not isinstance(scan, dict)
        or not str(scan.get("status") or "").startswith("pass")
        or scan.get("exact_local_secret_match_count") != 0
    ):
        raise RuntimeError("sealed artifact credential scan is not pass/zero-exact")
    current = quick_tree_fingerprint(source)
    if seal.get("source_quick_fingerprint") != current:
        raise RuntimeError("experiment changed after artifact sealing")
    if _stored_tree_identity(tree) != _content_tree_identity(source):
        raise RuntimeError("experiment content changed after artifact sealing")
    suffix = ".seal.json"
    if not path.name.endswith(suffix):
        raise RuntimeError("sealed artifact manifest filename is invalid")
    archive_path = path.with_name(path.name[:-len(suffix)])
    archive_record = seal.get("archive")
    if (not archive_path.is_file()
            or not isinstance(archive_record, dict)
            or archive_record.get("size_bytes") != archive_path.stat().st_size
            or archive_record.get("sha256") != sha256_file(archive_path)):
        raise RuntimeError("sealed private archive content is missing or changed")
    return seal
