"""Validate compact experiment records without reading raw prompts or responses.

The validator treats ``samples.csv`` as the compact row-level evidence and
independently recomputes the generated summaries.  It also checks the catalog
binding, schema enums, references, record-directory allowlist, secret leakage,
and Git ignore behavior.  Raw archive tree hashes are only recomputed when
``--verify-tree-hash`` is requested because the historical archives are large.
Use ``--records-only`` in a clean clone to validate the committed evidence
layer without requiring ignored raw archives, source reports, or verifier logs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlsplit

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG = ROOT / "tools" / "experiment_records_catalog.json"

SAMPLE_FIELDS = [
    "experiment_id",
    "sample_id",
    "domain",
    "method",
    "round_trip",
    "direction",
    "canonical_included",
    "exclusion_reason",
    "execution_route",
    "execution_status",
    "commit_outcome",
    "preservation_violations",
    "rs",
    "critical_failure",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "latency_ms",
    "failure_stage",
    "failure_label",
    "raw_artifact_id",
    "raw_call_ids",
    "raw_ref",
]

RECORD_FILENAMES = {
    "experiment.yaml",
    "summary.json",
    "samples.csv",
    "failure_stats.json",
    "representative_failures.jsonl",
    "report.md",
    "casebook.jsonl",
    "verification.txt",
    "raw_manifest.json",
}

FILE_SIZE_LIMITS = {
    "experiment.yaml": 64 * 1024,
    "summary.json": 1024 * 1024,
    "samples.csv": 5 * 1024 * 1024,
    "failure_stats.json": 1024 * 1024,
    "representative_failures.jsonl": 1024 * 1024,
    "report.md": 1024 * 1024,
    "casebook.jsonl": 2 * 1024 * 1024,
    "verification.txt": 64 * 1024,
    "raw_manifest.json": 1024 * 1024,
}
RECORD_SIZE_LIMIT = 13 * 1024 * 1024

RECORD_KINDS = {"run", "derived"}
LIFECYCLE_STATUSES = {
    "complete",
    "incomplete",
    "failed_informative",
    "superseded",
}
EVIDENCE_ROLES = {
    "primary",
    "baseline",
    "supporting",
    "ablation",
    "diagnostic",
    "smoke",
    "transport",
    "sensitivity",
    "excluded",
}
CLAIM_ELIGIBILITY = {"canonical", "supporting", "diagnostic_only", "none"}
EXECUTION_ROUTES = {
    "local_patch",
    "bulk_patch",
    "dsl_rules",
    "bounded_rewrite",
    "fullrewrite",
    "hybridpatch_protocol_failure_kept_context",
    "none",
    "unknown",
}
EXECUTION_STATUSES = {"success", "degraded", "failure", "missing"}
COMMIT_OUTCOMES = {
    "applied",
    "partial_applied",
    "kept_context",
    "unchanged",
    "not_committed",
    "missing",
    "not_applicable",
}
FAILURE_STAGES = {
    "none",
    "model",
    "transport",
    "protocol",
    "executor",
    "gate",
    "evaluator",
    "data",
    "infrastructure",
    "quality",
    "unknown",
}
FAILURE_STAGE = {
    "none": "none",
    "empty_response": "model",
    "near_empty_response": "model",
    "refusal": "model",
    "text_truncated": "model",
    "thinking_budget_exhausted": "model",
    "timeout": "transport",
    "incomplete_stream": "transport",
    "rate_limit": "transport",
    "quota_exhausted": "infrastructure",
    "http_error": "transport",
    "non_protocol_response": "protocol",
    "invalid_json": "protocol",
    "schema_error": "protocol",
    "op_rejected": "executor",
    "route_violation": "executor",
    "validation_gate": "gate",
    "format_regression": "gate",
    "evaluator_error": "evaluator",
    "dependency_missing": "data",
    "infrastructure_incomplete": "infrastructure",
    "content_regression": "quality",
    "unknown_failure": "unknown",
}
FAILURE_LABELS = set(FAILURE_STAGE)

CATALOG_STATUSES = {
    "canonical",
    "diagnostic",
    "source-only",
    "superseded",
}
CATALOG_EVIDENCE = {
    "canonical",
    "historical",
    "diagnostic",
    "source_only",
    "sensitivity",
}
SAMPLE_POLICIES = {
    "manifest_selected",
    "complete_all_methods",
    "exclude",
    "all_result_samples",
}
VERIFICATION_STATUSES = {
    "pass",
    "pass_with_known_exception",
    "structural_only",
    "derived_validation_pass",
    "pass_committed_rows",
    "fail",
    "not_run",
    "unknown",
}
EVIDENCE_BASES = {"fresh_run", "archived_log", "documented_claim", "none"}
CASEBOOK_KINDS = {"paired_comparison", "single_method_event"}
CASEBOOK_FIELDS = {
    "schema",
    "case_id",
    "experiment_id",
    "case_kind",
    "categories",
    "selection_reason",
    "sample_id",
    "round_trip",
    "direction",
    "methods",
    "score_delta_hybridpatch_minus_fullrewrite",
}
CASEBOOK_METHOD_FIELDS = {
    "rs",
    "route",
    "status",
    "commit_outcome",
    "failure_label",
    "raw_ref",
}
PAPER_REPORTING_FIELDS = {
    "status",
    "protocol_condition",
    "scope_id",
    "paired_backward_rows",
    "hybridpatch_mean",
    "fullrewrite_mean",
    "delta_hybridpatch_minus_fullrewrite",
    "p",
    "cohen_d",
    "official_replacements_in_scope",
    "interpretation",
    "validity_caveat",
    "source_ref",
    "machine_source_ref",
}

FORBIDDEN_DATA_KEYS = {
    "authorization",
    "cookie",
    "set-cookie",
    "api_key",
    "access_token",
    "refresh_token",
    "password",
    "secret",
    "request_body",
    "request_messages",
    "raw_llm_response",
    "response_body",
    "document_content",
    "headers",
}

WINDOWS_ABSOLUTE_RE = re.compile(r"(?i)(?<![A-Za-z0-9])[A-Z]:[\\/]")
UNC_PATH_RE = re.compile(r"(?<![A-Za-z0-9<])\\\\[A-Za-z0-9_$.-]+[\\/]")
POSIX_PRIVATE_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9:])/(?:home|Users|root|var/folders|private/tmp|tmp)/"
)
AUTH_HEADER_RE = re.compile(
    r"(?im)\bauthorization\s*[:=]\s*(?!<redacted>(?:\s|$))\S+"
)
BEARER_RE = re.compile(
    r"(?i)\bbearer\s+(?!<redacted>(?:\s|$))[A-Za-z0-9._~+/=-]{8,}"
)
COOKIE_HEADER_RE = re.compile(r"(?im)^\s*(?:cookie|set-cookie)\s*:")
PEM_RE = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")
GENERIC_KEY_RE = re.compile(r"(?i)\b(?:sk|key)-[A-Za-z0-9_-]{16,}\b")
SECRET_NAME_RE = re.compile(
    r"(?i)(?:^|_)(?:api_?key|access_?token|refresh_?token|password|secret|auth|cookie)(?:$|_)"
)
SAFE_CALL_ID_RE = re.compile(r"^[A-Za-z0-9._:-]+$")
HEX12_RE = re.compile(r"^[0-9a-f]{12}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class Issues:
    """Collect bounded, non-sensitive validation diagnostics."""

    def __init__(self, limit: int = 500) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.limit = limit
        self.suppressed_errors = 0
        self.suppressed_warnings = 0

    @staticmethod
    def _format(location: str | Path, message: str) -> str:
        if isinstance(location, Path):
            try:
                location = location.resolve().relative_to(ROOT.resolve()).as_posix()
            except (OSError, ValueError):
                location = str(location)
        return f"{location}: {message}"

    def error(self, location: str | Path, message: str) -> None:
        if len(self.errors) < self.limit:
            self.errors.append(self._format(location, message))
        else:
            self.suppressed_errors += 1

    def warn(self, location: str | Path, message: str) -> None:
        if len(self.warnings) < self.limit:
            self.warnings.append(self._format(location, message))
        else:
            self.suppressed_warnings += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument(
        "--only",
        nargs="*",
        default=[],
        help="Optional experiment ids to validate.",
    )
    parser.add_argument(
        "--verify-tree-hash",
        action="store_true",
        help="Recompute raw directory tree hashes (potentially several GiB).",
    )
    parser.add_argument(
        "--records-only",
        action="store_true",
        help=(
            "Validate the committed catalog/record layer without requiring ignored "
            "raw archives, source reports, or verifier logs to exist locally."
        ),
    )
    parser.add_argument(
        "--skip-git-check",
        action="store_true",
        help="Skip git-ignore and tracked-file allowlist checks.",
    )
    parser.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="Return non-zero when warnings are present.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print failures and the final status line.",
    )
    args = parser.parse_args()
    if args.records_only and args.verify_tree_hash:
        parser.error("--records-only cannot be combined with --verify-tree-hash")
    return args


def reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def read_json(path: Path, issues: Issues) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle, parse_constant=reject_json_constant)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        issues.error(path, f"cannot parse strict JSON: {exc}")
        return None
    if not isinstance(value, dict):
        issues.error(path, "top-level JSON value must be an object")
        return None
    return value


def read_yaml(path: Path, issues: Issues) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as handle:
            value = yaml.safe_load(handle)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        issues.error(path, f"cannot parse safe YAML: {exc}")
        return None
    if not isinstance(value, dict):
        issues.error(path, "top-level YAML value must be a mapping")
        return None
    return value


def read_jsonl(path: Path, issues: Issues) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        handle = path.open(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        issues.error(path, f"cannot open JSONL: {exc}")
        return rows
    with handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line, parse_constant=reject_json_constant)
            except (json.JSONDecodeError, ValueError) as exc:
                issues.error(path, f"line {line_number}: invalid strict JSON: {exc}")
                continue
            if not isinstance(value, dict):
                issues.error(path, f"line {line_number}: value must be an object")
                continue
            rows.append(value)
    return rows


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_digest(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    files = 0
    total_bytes = 0
    skipped_sensitive: list[str] = []
    skipped_links: list[str] = []
    for candidate in sorted(path.rglob("*"), key=lambda item: item.as_posix()):
        relative = candidate.relative_to(path).as_posix()
        if candidate.is_symlink():
            skipped_links.append(relative)
            continue
        if not candidate.is_file():
            continue
        if candidate.name == ".env" or candidate.name.startswith(".env."):
            skipped_sensitive.append(relative)
            continue
        size = candidate.stat().st_size
        file_digest = sha256_file(candidate)
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        digest.update(file_digest.encode("ascii"))
        digest.update(b"\n")
        files += 1
        total_bytes += size
    return {
        "algorithm": "sha256-tree-v1",
        "tree_sha256": digest.hexdigest(),
        "file_count": files,
        "size_bytes": total_bytes,
        "skipped_sensitive_files": skipped_sensitive,
        "skipped_symlinks": skipped_links,
    }


def safe_repo_ref(value: Any) -> bool:
    if not isinstance(value, str) or not value or "\0" in value:
        return False
    if WINDOWS_ABSOLUTE_RE.search(value) or UNC_PATH_RE.search(value):
        return False
    if value.startswith(("/", "\\", "file://")):
        return False
    normalized = value.replace("\\", "/")
    return ".." not in PurePosixPath(normalized).parts


def resolve_repo_ref(
    value: Any,
    location: str,
    issues: Issues,
    *,
    must_exist: bool = True,
) -> Path | None:
    if not safe_repo_ref(value):
        issues.error(location, f"unsafe repository-relative reference: {value!r}")
        return None
    path = (ROOT / str(value).replace("\\", os.sep)).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError:
        issues.error(location, "reference resolves outside the repository")
        return None
    if must_exist and not path.exists():
        issues.error(location, f"referenced path does not exist: {value}")
    return path


def check_url(value: Any, location: str, issues: Issues) -> None:
    values = value if isinstance(value, list) else [value]
    for item in values:
        if item is None:
            continue
        if not isinstance(item, str):
            issues.error(location, "URL must be a string, list of strings, or null")
            continue
        parts = urlsplit(item)
        if parts.scheme != "https" or not parts.hostname:
            issues.error(location, f"endpoint must be an absolute HTTPS URL: {item!r}")
        if parts.username or parts.password or parts.query or parts.fragment:
            issues.error(location, "endpoint URL must not contain credentials, query, or fragment")


def check_enum(
    value: Any,
    allowed: set[str],
    location: str,
    issues: Issues,
    *,
    nullable: bool = False,
) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str) or value not in allowed:
        issues.error(location, f"expected one of {sorted(allowed)}, got {value!r}")


def check_schema(
    document: dict[str, Any],
    expected: str,
    path: Path,
    issues: Issues,
) -> None:
    if document.get("schema") != expected:
        issues.error(path, f"schema must be {expected!r}, got {document.get('schema')!r}")


def check_forbidden_keys(
    value: Any,
    location: str,
    issues: Issues,
) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in FORBIDDEN_DATA_KEYS:
                issues.error(location, f"forbidden raw/sensitive field name: {key_text}")
            check_forbidden_keys(item, f"{location}.{key_text}", issues)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            check_forbidden_keys(item, f"{location}[{index}]", issues)


def load_local_secrets() -> dict[str, str]:
    secrets: dict[str, str] = {}
    candidates = sorted(ROOT.glob(".env*"))
    for path in candidates:
        if not path.is_file():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError):
            continue
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            if stripped.lower().startswith("export "):
                stripped = stripped[7:].lstrip()
            name, value = stripped.split("=", 1)
            name = name.strip()
            value = value.strip().strip("\"'")
            if (
                SECRET_NAME_RE.search(name)
                and len(value) >= 8
                and not re.search(r"(?i)(placeholder|example|changeme|your[_-])", value)
            ):
                secrets[name] = value
    for name, value in os.environ.items():
        if SECRET_NAME_RE.search(name) and len(value) >= 8:
            secrets.setdefault(name, value)
    return secrets


def scan_text_security(
    path: Path,
    text: str,
    local_secrets: dict[str, str],
    issues: Issues,
) -> None:
    patterns = [
        (WINDOWS_ABSOLUTE_RE, "contains a Windows absolute path"),
        (UNC_PATH_RE, "contains a UNC path"),
        (POSIX_PRIVATE_PATH_RE, "contains a user/private POSIX absolute path"),
        (re.compile(r"(?i)\bfile://"), "contains a file:// URI"),
        (AUTH_HEADER_RE, "contains an unredacted Authorization value"),
        (BEARER_RE, "contains an unredacted Bearer token"),
        (COOKIE_HEADER_RE, "contains a Cookie/Set-Cookie header"),
        (PEM_RE, "contains a private-key PEM header"),
        (GENERIC_KEY_RE, "contains a key-like token"),
    ]
    for pattern, message in patterns:
        if pattern.search(text):
            issues.error(path, message)
    for name, secret in local_secrets.items():
        if secret in text:
            issues.error(path, f"contains a local secret value from variable {name}")


def validate_record_files(
    directory: Path,
    local_secrets: dict[str, str],
    issues: Issues,
) -> bool:
    if not directory.is_dir():
        issues.error(directory, "record directory is missing")
        return False
    total_size = 0
    present: set[str] = set()
    for candidate in directory.iterdir():
        if candidate.is_symlink():
            issues.error(candidate, "symlinks are forbidden in record directories")
            continue
        if candidate.is_dir():
            issues.error(candidate, "nested directories are forbidden in record directories")
            continue
        if not candidate.is_file():
            issues.error(candidate, "only regular files are allowed")
            continue
        present.add(candidate.name)
        total_size += candidate.stat().st_size
        if candidate.name not in RECORD_FILENAMES:
            issues.error(candidate, "file is not in the record allowlist")
            continue
        limit = FILE_SIZE_LIMITS[candidate.name]
        if candidate.stat().st_size > limit:
            issues.error(candidate, f"file exceeds size limit {limit} bytes")
        try:
            raw = candidate.read_bytes()
            if b"\0" in raw:
                issues.error(candidate, "NUL byte/binary payload is forbidden")
            text = raw.decode("utf-8")
        except (OSError, UnicodeError) as exc:
            issues.error(candidate, f"record file must be UTF-8 text: {exc}")
            continue
        scan_text_security(candidate, text, local_secrets, issues)
    missing = RECORD_FILENAMES - present
    extra = present - RECORD_FILENAMES
    if missing:
        issues.error(directory, f"missing required files: {sorted(missing)}")
    if extra:
        issues.error(directory, f"unexpected files: {sorted(extra)}")
    if total_size > RECORD_SIZE_LIMIT:
        issues.error(directory, f"record exceeds total size limit {RECORD_SIZE_LIMIT} bytes")
    return not missing


def load_catalog(path: Path, issues: Issues) -> tuple[dict[str, Any], str]:
    catalog = read_json(path, issues)
    if catalog is None:
        return {}, ""
    if catalog.get("schema_version") != 1:
        issues.error(path, "catalog schema_version must be 1")
    digest = sha256_file(path)
    return catalog, digest


def catalog_entries(
    catalog: dict[str, Any],
    issues: Issues,
    *,
    records_only: bool,
) -> dict[str, tuple[str, dict[str, Any]]]:
    output: dict[str, tuple[str, dict[str, Any]]] = {}
    record_sets = catalog.get("record_sets")
    if not isinstance(record_sets, list):
        issues.error(DEFAULT_CATALOG, "record_sets must be a list")
        return output
    for set_index, record_set in enumerate(record_sets):
        where = f"catalog.record_sets[{set_index}]"
        if not isinstance(record_set, dict):
            issues.error(where, "record set must be an object")
            continue
        owner = record_set.get("owner")
        if not isinstance(owner, str) or not SLUG_RE.fullmatch(owner):
            issues.error(where, f"invalid owner: {owner!r}")
            continue
        owner_path = resolve_repo_ref(owner, f"{where}.owner", issues)
        if owner_path is not None and not owner_path.is_dir():
            issues.error(where, "owner must reference a repository directory")
        experiments = record_set.get("experiments")
        if not isinstance(experiments, list):
            issues.error(where, "experiments must be a list")
            continue
        for entry_index, entry in enumerate(experiments):
            entry_where = f"{where}.experiments[{entry_index}]"
            if not isinstance(entry, dict):
                issues.error(entry_where, "experiment entry must be an object")
                continue
            experiment_id = entry.get("experiment_id")
            if not isinstance(experiment_id, str) or not SLUG_RE.fullmatch(experiment_id):
                issues.error(entry_where, f"invalid experiment_id: {experiment_id!r}")
                continue
            if experiment_id in output:
                issues.error(entry_where, f"duplicate experiment_id: {experiment_id}")
                continue
            output[experiment_id] = (owner, entry)
            validate_catalog_entry(
                owner,
                entry,
                entry_where,
                issues,
                records_only=records_only,
            )
    return output


def validate_review_provenance(
    value: Any,
    location: str,
    issues: Issues,
) -> None:
    if not isinstance(value, dict):
        issues.error(location, "review_provenance must be an object")
        return
    for field in ("reviewed_by", "reviewed_at", "review_method"):
        field_value = value.get(field)
        if not isinstance(field_value, str) or not field_value:
            issues.error(f"{location}.{field}", "must be a non-empty string")
    check_datetime(value.get("reviewed_at"), f"{location}.reviewed_at", issues)
    reviews = value.get("independent_reviews")
    if not isinstance(reviews, list):
        issues.error(f"{location}.independent_reviews", "must be a list")
        reviews = []
    for index, review in enumerate(reviews):
        review_where = f"{location}.independent_reviews[{index}]"
        if not isinstance(review, dict):
            issues.error(review_where, "must be an object")
            continue
        for field in ("role", "reviewer", "verdict", "summary"):
            field_value = review.get(field)
            if not isinstance(field_value, str) or not field_value:
                issues.error(f"{review_where}.{field}", "must be a non-empty string")
    exception = value.get("independent_review_exception")
    if exception is not None and (
        not isinstance(exception, str) or not exception
    ):
        issues.error(
            f"{location}.independent_review_exception",
            "must be null or a non-empty string",
        )
    if not reviews and not exception:
        issues.error(
            location,
            "requires at least one independent review or an exception reason",
        )


def validate_catalog_entry(
    owner: str,
    entry: dict[str, Any],
    location: str,
    issues: Issues,
    *,
    records_only: bool,
) -> None:
    check_enum(entry.get("status"), CATALOG_STATUSES, f"{location}.status", issues)
    check_enum(
        entry.get("evidence_role"),
        CATALOG_EVIDENCE,
        f"{location}.evidence_role",
        issues,
    )
    check_enum(
        entry.get("sample_policy"),
        SAMPLE_POLICIES,
        f"{location}.sample_policy",
        issues,
    )
    check_enum(
        entry.get("lifecycle_status"),
        LIFECYCLE_STATUSES,
        f"{location}.lifecycle_status",
        issues,
    )
    if not isinstance(entry.get("scope_id"), str) or not entry.get("scope_id"):
        issues.error(location, "scope_id must be a non-empty string")
    if entry.get("kind") not in (None, "manifest_view"):
        issues.error(location, f"unsupported kind: {entry.get('kind')!r}")
    archive = resolve_repo_ref(
        entry.get("archive_path"),
        f"{location}.archive_path",
        issues,
        must_exist=not records_only,
    )
    if archive is not None and archive.exists() and not archive.is_dir():
        issues.error(location, "archive_path must reference a directory")
    if entry.get("kind") == "manifest_view":
        manifest = resolve_repo_ref(
            entry.get("manifest_path"),
            f"{location}.manifest_path",
            issues,
            must_exist=not records_only,
        )
        if manifest is not None and manifest.exists() and not manifest.is_file():
            issues.error(location, "manifest_path must reference a file")
    arms = entry.get("arms")
    if not isinstance(arms, dict) or not arms:
        issues.error(location, "arms must be a non-empty object")
    elif any(not isinstance(key, str) or not key for key in arms):
        issues.error(location, "arm names must be non-empty strings")
    for key in ("round_trips", "expected_sample_count"):
        value = entry.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            issues.error(f"{location}.{key}", "must be a positive integer")
    if not isinstance(entry.get("dataset"), str) or not entry.get("dataset"):
        issues.error(location, "dataset must be a non-empty string")
    if not isinstance(entry.get("split"), str) or not entry.get("split"):
        issues.error(location, "split must be a non-empty string")
    reports = entry.get("source_reports")
    if not isinstance(reports, list) or not reports:
        issues.error(location, "source_reports must be a non-empty list")
    else:
        for index, report in enumerate(reports):
            report_where = f"{location}.source_reports[{index}]"
            if not isinstance(report, dict):
                issues.error(report_where, "source report must be an object")
                continue
            resolve_repo_ref(
                report.get("path"),
                f"{report_where}.path",
                issues,
                must_exist=not records_only,
            )
            if not isinstance(report.get("role"), str) or not report.get("role"):
                issues.error(report_where, "source report role must be non-empty")
    canonical_source = entry.get("canonical_source_report")
    if canonical_source is not None:
        resolve_repo_ref(
            canonical_source,
            f"{location}.canonical_source_report",
            issues,
            must_exist=not records_only,
        )
        report_paths = {
            report.get("path")
            for report in reports or []
            if isinstance(report, dict)
        }
        if canonical_source not in report_paths:
            issues.error(
                location,
                "canonical_source_report must be listed in source_reports",
            )
    paper_reporting = entry.get("paper_reporting")
    if paper_reporting is not None:
        paper_where = f"{location}.paper_reporting"
        if not isinstance(paper_reporting, dict):
            issues.error(paper_where, "must be an object when present")
        else:
            if set(paper_reporting) != PAPER_REPORTING_FIELDS:
                issues.error(
                    paper_where,
                    "fields differ; "
                    f"expected {sorted(PAPER_REPORTING_FIELDS)}, "
                    f"got {sorted(paper_reporting)}",
                )
            for field in (
                "status",
                "protocol_condition",
                "scope_id",
                "interpretation",
                "validity_caveat",
            ):
                if (
                    not isinstance(paper_reporting.get(field), str)
                    or not paper_reporting.get(field)
                ):
                    issues.error(f"{paper_where}.{field}", "must be a non-empty string")
            for field in ("paired_backward_rows", "official_replacements_in_scope"):
                value = paper_reporting.get(field)
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value <= 0
                ):
                    issues.error(f"{paper_where}.{field}", "must be a positive integer")
            for field in (
                "hybridpatch_mean",
                "fullrewrite_mean",
                "delta_hybridpatch_minus_fullrewrite",
                "p",
                "cohen_d",
            ):
                value = paper_reporting.get(field)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                ):
                    issues.error(f"{paper_where}.{field}", "must be a finite number")
            for field in ("hybridpatch_mean", "fullrewrite_mean", "p"):
                value = paper_reporting.get(field)
                if isinstance(value, (int, float)) and not 0.0 <= float(value) <= 1.0:
                    issues.error(f"{paper_where}.{field}", "must be in [0, 1]")
            hp = paper_reporting.get("hybridpatch_mean")
            fr = paper_reporting.get("fullrewrite_mean")
            delta = paper_reporting.get("delta_hybridpatch_minus_fullrewrite")
            if all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in (hp, fr, delta)
            ) and not math.isclose(
                float(hp) - float(fr),
                float(delta),
                rel_tol=1e-9,
                abs_tol=1e-12,
            ):
                issues.error(
                    paper_where,
                    "delta_hybridpatch_minus_fullrewrite does not match means",
                )
            for field in ("source_ref", "machine_source_ref"):
                resolve_repo_ref(
                    paper_reporting.get(field),
                    f"{paper_where}.{field}",
                    issues,
                    must_exist=not records_only,
                )
    if entry.get("review_provenance") is not None:
        validate_review_provenance(
            entry["review_provenance"],
            f"{location}.review_provenance",
            issues,
        )
    verification = entry.get("verification")
    if not isinstance(verification, dict):
        issues.error(location, "verification must be an object")
    else:
        check_enum(
            verification.get("status"),
            VERIFICATION_STATUSES,
            f"{location}.verification.status",
            issues,
        )
        for field in ("replayed_backward_rows", "canonical_backward_rows"):
            value = verification.get(field)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                issues.error(
                    f"{location}.verification.{field}",
                    "must be a non-negative integer or null",
                )
        for field in ("source", "log"):
            value = verification.get(field)
            if value is not None:
                resolve_repo_ref(
                    value,
                    f"{location}.verification.{field}",
                    issues,
                    must_exist=not records_only,
                )
    excluded = entry.get("excluded_samples")
    if excluded is not None and not isinstance(excluded, dict):
        issues.error(location, "excluded_samples must be an object when present")
    if owner.startswith("HP_V") and entry.get("kind") == "manifest_view":
        issues.warn(location, "manifest-derived records are unusual under frozen HP owners")


def expected_evidence_role(entry: dict[str, Any]) -> str:
    role = entry["evidence_role"]
    if role == "canonical":
        return "baseline" if entry["research_stage"] == "baseline" else "primary"
    if role == "historical":
        return "supporting"
    if role == "diagnostic":
        return "transport" if "transport" in entry["research_stage"] else "diagnostic"
    if role == "source_only":
        return "supporting"
    if role == "sensitivity":
        return "sensitivity"
    raise ValueError(role)


def expected_claim_eligibility(entry: dict[str, Any]) -> str:
    if entry["status"] == "canonical" and entry["evidence_role"] == "canonical":
        return "canonical"
    if entry["evidence_role"] in {"historical", "sensitivity"}:
        return "supporting"
    if entry["evidence_role"] == "diagnostic":
        return "diagnostic_only"
    return "none"


def expected_lifecycle(
    entry: dict[str, Any],
    planned_count: int,
    canonical_count: int,
) -> str:
    del planned_count, canonical_count
    return str(entry["lifecycle_status"])


def expected_source_canonical(entry: dict[str, Any]) -> str | None:
    return entry.get("canonical_source_report")


def check_datetime(value: Any, location: str, issues: Issues) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        issues.error(location, "timestamp must be a string or null")
        return
    try:
        datetime.fromisoformat(value)
    except ValueError:
        issues.error(location, f"invalid ISO-8601 timestamp: {value!r}")


def validate_experiment_document(
    document: dict[str, Any],
    path: Path,
    owner: str,
    entry: dict[str, Any],
    catalog_digest: str,
    summary: dict[str, Any],
    issues: Issues,
    *,
    records_only: bool,
) -> None:
    check_schema(document, "hybridpatch.experiment_record/1", path, issues)
    check_forbidden_keys(document, path.as_posix(), issues)
    experiment_id = entry["experiment_id"]
    if document.get("record_revision") != 1:
        issues.error(path, "record_revision must be 1")
    if document.get("catalog_sha256") != catalog_digest:
        issues.error(path, "catalog_sha256 does not match the current catalog")
    if document.get("experiment_id") != experiment_id:
        issues.error(path, "experiment_id does not match the catalog/directory")
    if document.get("owner") != owner:
        issues.error(path, f"owner must be {owner!r}")
    expected_kind = "derived" if entry.get("kind") == "manifest_view" else "run"
    if document.get("record_kind") != expected_kind:
        issues.error(path, f"record_kind must be {expected_kind!r}")
    check_enum(document.get("record_kind"), RECORD_KINDS, f"{path}:record_kind", issues)
    check_enum(
        document.get("lifecycle_status"),
        LIFECYCLE_STATUSES,
        f"{path}:lifecycle_status",
        issues,
    )
    check_enum(
        document.get("evidence_role"),
        EVIDENCE_ROLES,
        f"{path}:evidence_role",
        issues,
    )
    check_enum(
        document.get("claim_eligibility"),
        CLAIM_ELIGIBILITY,
        f"{path}:claim_eligibility",
        issues,
    )
    if document.get("evidence_role") != expected_evidence_role(entry):
        issues.error(path, "evidence_role does not match the catalog mapping")
    if document.get("claim_eligibility") != expected_claim_eligibility(entry):
        issues.error(path, "claim_eligibility does not match the catalog mapping")
    if document.get("title") != entry.get("purpose") or document.get("purpose") != entry.get(
        "purpose"
    ):
        issues.error(path, "title/purpose must match catalog purpose")

    source = document.get("source")
    if not isinstance(source, dict):
        issues.error(path, "source must be an object")
    else:
        if source.get("experiment_root_ref") != entry.get("archive_path"):
            issues.error(path, "source.experiment_root_ref must match catalog archive_path")
        resolve_repo_ref(
            source.get("experiment_root_ref"),
            f"{path}:source.experiment_root_ref",
            issues,
            must_exist=not records_only,
        )
        expected_artifact = f"{experiment_id}-raw-v1"
        if source.get("raw_artifact_id") != expected_artifact:
            issues.error(path, f"source.raw_artifact_id must be {expected_artifact!r}")

    code = document.get("code")
    if not isinstance(code, dict):
        issues.error(path, "code must be an object")
    else:
        commit = code.get("run_git_commit")
        if commit is not None and not re.fullmatch(r"[0-9a-f]{40}", str(commit)):
            issues.error(path, "code.run_git_commit must be a 40-char lowercase hash or null")
        fingerprints = code.get("fingerprints")
        if not isinstance(fingerprints, list):
            issues.error(path, "code.fingerprints must be a list")
            fingerprints = []
        if code.get("fingerprint_count") != len(fingerprints):
            issues.error(path, "code.fingerprint_count does not match fingerprints")
        for index, fingerprint in enumerate(fingerprints):
            if not isinstance(fingerprint, dict):
                issues.error(path, f"fingerprints[{index}] must be an object")
                continue
            for filename, digest in fingerprint.items():
                if not isinstance(filename, str) or "/" in filename or "\\" in filename:
                    issues.error(path, f"unsafe fingerprint filename: {filename!r}")
                if not isinstance(digest, str) or not HEX12_RE.fullmatch(digest):
                    issues.error(path, f"invalid sha1-12 fingerprint for {filename!r}")
        if fingerprints and code.get("fingerprint_algorithm") != "sha1-12":
            issues.error(path, "fingerprint_algorithm must be sha1-12 when fingerprints exist")
        if not fingerprints and code.get("fingerprint_algorithm") is not None:
            issues.error(path, "fingerprint_algorithm must be null without fingerprints")
        if code.get("version_reference") is not None:
            resolve_repo_ref(
                code.get("version_reference"),
                f"{path}:code.version_reference",
                issues,
            )

    execution = document.get("execution")
    methods: list[str] = []
    round_trips = entry["round_trips"]
    if not isinstance(execution, dict):
        issues.error(path, "execution must be an object")
    else:
        methods_value = execution.get("methods")
        if not isinstance(methods_value, list) or any(
            not isinstance(method, str) or not method for method in methods_value
        ):
            issues.error(path, "execution.methods must be a list of non-empty strings")
        else:
            methods = methods_value
        if methods != list(entry["arms"]):
            issues.error(path, "execution.methods must match catalog arm order")
        if execution.get("arms") != entry.get("arms"):
            issues.error(path, "execution.arms must exactly match the catalog")
        if execution.get("round_trips") != round_trips:
            issues.error(path, "execution.round_trips must match the catalog")
        if execution.get("comparison_policy") != entry.get("comparison_policy"):
            issues.error(path, "execution.comparison_policy must match the catalog")
        commands = execution.get("command_templates")
        if not isinstance(commands, list) or any(
            not isinstance(command, str) for command in commands
        ):
            issues.error(path, "execution.command_templates must be a list of strings")

    model = document.get("model")
    if not isinstance(model, dict):
        issues.error(path, "model must be an object")
    else:
        check_url(model.get("base_url"), f"{path}:model.base_url", issues)
        check_url(model.get("request_url"), f"{path}:model.request_url", issues)

    protocol = document.get("protocol")
    if not isinstance(protocol, dict):
        issues.error(path, "protocol must be an object")
    else:
        if protocol.get("declared_campaign_revision") != entry.get(
            "protocol_revision"
        ):
            issues.error(
                path,
                "protocol.declared_campaign_revision must match the catalog",
            )
        revisions = protocol.get("observed_valid_envelope_revisions")
        if not isinstance(revisions, list) or any(
            not isinstance(item, str) for item in revisions
        ):
            issues.error(
                path,
                "protocol.observed_valid_envelope_revisions must be a string list",
            )
        declared = entry.get("protocol_revision")
        if declared and any(item != declared for item in revisions or []):
            issues.error(
                path,
                "observed valid envelope revisions conflict with declared campaign revision",
            )
        if revisions and not declared:
            issues.error(
                path,
                "observed protocol revisions require a catalog protocol_revision",
            )

    data = document.get("data")
    planned: list[str] = []
    canonical: list[str] = []
    excluded: list[dict[str, Any]] = []
    if not isinstance(data, dict):
        issues.error(path, "data must be an object")
    else:
        if data.get("dataset") != entry.get("dataset"):
            issues.error(path, "data.dataset must match the catalog")
        if data.get("split") != entry.get("split"):
            issues.error(path, "data.split must match the catalog")
        planned_value = data.get("planned_sample_ids")
        canonical_value = data.get("canonical_sample_ids")
        excluded_value = data.get("excluded_samples")
        if isinstance(planned_value, list) and all(
            isinstance(item, str) and item for item in planned_value
        ):
            planned = planned_value
        else:
            issues.error(path, "data.planned_sample_ids must be a string list")
        if isinstance(canonical_value, list) and all(
            isinstance(item, str) and item for item in canonical_value
        ):
            canonical = canonical_value
        else:
            issues.error(path, "data.canonical_sample_ids must be a string list")
        if isinstance(excluded_value, list) and all(
            isinstance(item, dict) for item in excluded_value
        ):
            excluded = excluded_value
        else:
            issues.error(path, "data.excluded_samples must be an object list")
        if len(planned) != len(set(planned)):
            issues.error(path, "planned_sample_ids contains duplicates")
        if len(canonical) != len(set(canonical)):
            issues.error(path, "canonical_sample_ids contains duplicates")
        if planned != sorted(planned) or canonical != sorted(canonical):
            issues.error(path, "sample id lists must be sorted")
        if len(planned) != entry.get("expected_sample_count"):
            issues.error(
                path,
                "planned sample count does not match catalog expected_sample_count",
            )
        if not set(canonical).issubset(planned):
            issues.error(path, "canonical samples must be a subset of planned samples")
        excluded_ids = [item.get("sample_id") for item in excluded]
        if any(not isinstance(item, str) or not item for item in excluded_ids):
            issues.error(path, "each excluded sample needs a non-empty sample_id")
        if len(excluded_ids) != len(set(excluded_ids)):
            issues.error(path, "excluded_samples contains duplicate sample ids")
        if set(canonical) & set(excluded_ids):
            issues.error(path, "canonical and excluded sample sets overlap")
        if set(planned) != set(canonical) | set(excluded_ids):
            issues.error(path, "planned samples must equal canonical union excluded samples")
        for item in excluded:
            if not isinstance(item.get("reason"), str) or not item.get("reason"):
                issues.error(path, "each excluded sample needs a non-empty reason")

    expected_lifecycle_value = expected_lifecycle(
        entry,
        len(planned),
        len(canonical),
    )
    if document.get("lifecycle_status") != expected_lifecycle_value:
        issues.error(path, "lifecycle_status does not match builder/catalog policy")
    if (
        document.get("claim_eligibility") == "canonical"
        and document.get("lifecycle_status") == "incomplete"
    ):
        issues.warn(
            path,
            "canonical claim is attached to an incomplete lifecycle; exclusions must remain explicit",
        )

    time = document.get("time")
    if not isinstance(time, dict):
        issues.error(path, "time must be an object")
    else:
        check_datetime(time.get("started_at_local"), f"{path}:time.started_at_local", issues)
        check_datetime(
            time.get("last_dispatch_started_at_local"),
            f"{path}:time.last_dispatch_started_at_local",
            issues,
        )
        check_datetime(time.get("finished_at"), f"{path}:time.finished_at", issues)

    canonical_report = document.get("canonical_report")
    expected_report = expected_source_canonical(entry)
    if not isinstance(canonical_report, dict):
        issues.error(path, "canonical_report must be an object")
    else:
        if canonical_report.get("record_ref") != "summary.json":
            issues.error(path, "canonical_report.record_ref must be summary.json")
        if canonical_report.get("scope_id") != summary.get("canonical_scope_id"):
            issues.error(path, "canonical report scope does not match summary")
        if canonical_report.get("source_report_ref") != expected_report:
            issues.error(path, "canonical source report does not match catalog selection")
        if expected_report is not None:
            report_path = resolve_repo_ref(
                expected_report,
                f"{path}:canonical_report.source_report_ref",
                issues,
                must_exist=not records_only,
            )
            stored_hash = canonical_report.get("source_report_sha256")
            if not isinstance(stored_hash, str) or not SHA256_RE.fullmatch(stored_hash):
                issues.error(path, "canonical source report digest is not SHA-256")
            if (
                not records_only
                and report_path is not None
                and report_path.is_file()
            ):
                expected_hash = sha256_file(report_path)
                if stored_hash != expected_hash:
                    issues.error(path, "canonical source report SHA-256 mismatch")
        elif canonical_report.get("source_report_sha256") is not None:
            issues.error(
                path,
                "source-only record must not claim a canonical source report hash",
            )

    actual_reports = document.get("source_reports")
    if not isinstance(actual_reports, list):
        issues.error(path, "source_reports must be a list")
        actual_reports = []
    expected_reports: list[dict[str, Any]] = []
    catalog_reports = entry.get("source_reports", [])
    for index, report in enumerate(catalog_reports):
        stored = actual_reports[index] if index < len(actual_reports) else {}
        if not isinstance(stored, dict):
            issues.error(path, f"source_reports[{index}] must be an object")
            stored = {}
        report_path = resolve_repo_ref(
            report["path"],
            f"{path}:source_reports[{index}].path",
            issues,
            must_exist=not records_only,
        )
        stored_hash = stored.get("sha256")
        stored_size = stored.get("size_bytes")
        if not isinstance(stored_hash, str) or not SHA256_RE.fullmatch(stored_hash):
            issues.error(path, f"source_reports[{index}].sha256 is not SHA-256")
        if (
            not isinstance(stored_size, int)
            or isinstance(stored_size, bool)
            or stored_size < 0
        ):
            issues.error(
                path,
                f"source_reports[{index}].size_bytes must be a non-negative integer",
            )
        if (
            not records_only
            and report_path is not None
            and report_path.is_file()
        ):
            stored_hash = sha256_file(report_path)
            stored_size = report_path.stat().st_size
        expected_reports.append(
            {
                **report,
                "sha256": stored_hash,
                "size_bytes": stored_size,
            }
        )
    if document.get("source_reports") != expected_reports:
        issues.error(path, "source_reports hashes/sizes do not match catalog evidence")
    if expected_report is not None:
        canonical_stored_hash = next(
            (
                report.get("sha256")
                for report in expected_reports
                if report.get("path") == expected_report
            ),
            None,
        )
        if (
            isinstance(canonical_report, dict)
            and canonical_report.get("source_report_sha256") != canonical_stored_hash
        ):
            issues.error(
                path,
                "canonical source report digest differs from source_reports entry",
            )
    expected_noncanonical = [
        {
            "source_report_ref": report["path"],
            "role": report["role"],
            "sha256": report["sha256"],
            "size_bytes": report["size_bytes"],
            "reason": (
                "Source evidence only; the generated summary.json is the sole canonical machine scope."
            ),
        }
        for report in expected_reports
        if report["path"] != expected_source_canonical(entry)
    ]
    if document.get("noncanonical_reports") != expected_noncanonical:
        issues.error(path, "noncanonical_reports does not match catalog source reports")
    for index, report in enumerate(document.get("noncanonical_reports") or []):
        resolve_repo_ref(
            report.get("source_report_ref"),
            f"{path}:noncanonical_reports[{index}]",
            issues,
            must_exist=not records_only,
        )

    if document.get("paper_reporting") != entry.get("paper_reporting"):
        issues.error(path, "paper_reporting does not match the catalog")
    if document.get("review_provenance") != entry.get("review_provenance"):
        issues.error(path, "review_provenance does not match the catalog")
    if document.get("verification") != entry.get("verification"):
        issues.error(path, "embedded verification does not match the catalog")
    if document.get("known_exceptions") != (entry.get("known_exceptions") or []):
        issues.error(path, "known_exceptions does not match the catalog")
    gaps = document.get("provenance_gaps")
    if not isinstance(gaps, list) or any(not isinstance(item, dict) for item in gaps):
        issues.error(path, "provenance_gaps must be a list of objects")
    if document.get("raw_manifest_ref") != "raw_manifest.json":
        issues.error(path, "raw_manifest_ref must be raw_manifest.json")


def parse_csv(
    path: Path,
    issues: Issues,
) -> list[dict[str, str]]:
    try:
        handle = path.open(encoding="utf-8", newline="")
    except (OSError, UnicodeError) as exc:
        issues.error(path, f"cannot open CSV: {exc}")
        return []
    with handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != SAMPLE_FIELDS:
            issues.error(path, f"CSV header must exactly equal {SAMPLE_FIELDS}")
        rows: list[dict[str, str]] = []
        for line_number, row in enumerate(reader, 2):
            if None in row:
                issues.error(path, f"line {line_number}: extra CSV columns")
                continue
            if any(value is None for value in row.values()):
                issues.error(path, f"line {line_number}: missing CSV columns")
                continue
            rows.append({key: str(value) for key, value in row.items()})
        return rows


def parse_int_cell(
    value: str,
    path: Path,
    field: str,
    issues: Issues,
    *,
    nullable: bool = True,
) -> int | None:
    if value == "":
        if not nullable:
            issues.error(path, f"{field} must not be empty")
        return None
    if not re.fullmatch(r"-?\d+", value):
        issues.error(path, f"{field} must be an integer, got {value!r}")
        return None
    return int(value)


def parse_float_cell(
    value: str,
    path: Path,
    field: str,
    issues: Issues,
) -> float | None:
    if value == "":
        return None
    try:
        result = float(value)
    except ValueError:
        issues.error(path, f"{field} must be a float, got {value!r}")
        return None
    if not math.isfinite(result):
        issues.error(path, f"{field} must be finite")
        return None
    return result


def domain_of(sample_id: str) -> str:
    return "".join(character for character in sample_id if not character.isdigit())


def validate_sample_rows(
    rows: list[dict[str, str]],
    path: Path,
    experiment: dict[str, Any],
    entry: dict[str, Any],
    issues: Issues,
) -> list[dict[str, Any]]:
    experiment_id = entry["experiment_id"]
    methods = list(entry["arms"])
    round_trips = int(entry["round_trips"])
    data = experiment.get("data") or {}
    planned = list(data.get("planned_sample_ids") or [])
    canonical = set(data.get("canonical_sample_ids") or [])
    exclusions = {
        item.get("sample_id"): item.get("reason")
        for item in data.get("excluded_samples") or []
        if isinstance(item, dict)
    }
    artifact_id = f"{experiment_id}-raw-v1"
    expected_order = [
        (sample, method, round_trip, direction)
        for sample in planned
        for method in methods
        for round_trip in range(1, round_trips + 1)
        for direction in ("forward", "backward")
    ]
    parsed: list[dict[str, Any]] = []
    actual_order: list[tuple[str, str, int, str]] = []
    seen: set[tuple[str, str, int, str]] = set()
    for line_index, row in enumerate(rows, 2):
        where = f"{path}:line {line_index}"
        if row["experiment_id"] != experiment_id:
            issues.error(where, "experiment_id mismatch")
        sample = row["sample_id"]
        method = row["method"]
        round_trip = parse_int_cell(
            row["round_trip"],
            path,
            f"line {line_index} round_trip",
            issues,
            nullable=False,
        )
        direction = row["direction"]
        if sample not in planned:
            issues.error(where, f"sample is outside planned set: {sample!r}")
        if row["domain"] != domain_of(sample):
            issues.error(where, "domain does not match sample_id")
        if method not in methods:
            issues.error(where, f"unknown method: {method!r}")
        if round_trip is not None and not 1 <= round_trip <= round_trips:
            issues.error(where, "round_trip is outside configured range")
        if direction not in {"forward", "backward"}:
            issues.error(where, f"invalid direction: {direction!r}")
        if round_trip is None:
            continue
        key = (sample, method, round_trip, direction)
        actual_order.append(key)
        if key in seen:
            issues.error(where, f"duplicate row key: {key}")
        seen.add(key)

        included_text = row["canonical_included"]
        if included_text not in {"true", "false"}:
            issues.error(where, "canonical_included must be true or false")
        included = included_text == "true"
        check_enum(
            row["execution_route"],
            EXECUTION_ROUTES,
            f"{where}.execution_route",
            issues,
        )
        check_enum(
            row["execution_status"],
            EXECUTION_STATUSES,
            f"{where}.execution_status",
            issues,
        )
        check_enum(
            row["commit_outcome"],
            COMMIT_OUTCOMES,
            f"{where}.commit_outcome",
            issues,
        )
        check_enum(
            row["failure_stage"],
            FAILURE_STAGES,
            f"{where}.failure_stage",
            issues,
        )
        check_enum(
            row["failure_label"],
            FAILURE_LABELS,
            f"{where}.failure_label",
            issues,
        )
        expected_stage = FAILURE_STAGE.get(row["failure_label"])
        if row["failure_stage"] != expected_stage:
            issues.error(where, "failure_stage does not match failure_label")

        score = parse_float_cell(row["rs"], path, f"line {line_index} rs", issues)
        if score is not None and not 0.0 <= score <= 1.0:
            issues.error(where, "rs must be in [0, 1]")
        preservation_value = parse_int_cell(
            row["preservation_violations"],
            path,
            f"line {line_index} preservation_violations",
            issues,
        )
        input_value = parse_int_cell(
            row["input_tokens"], path, f"line {line_index} input_tokens", issues
        )
        output_value = parse_int_cell(
            row["output_tokens"], path, f"line {line_index} output_tokens", issues
        )
        total_value = parse_int_cell(
            row["total_tokens"], path, f"line {line_index} total_tokens", issues
        )
        latency_value = parse_int_cell(
            row["latency_ms"], path, f"line {line_index} latency_ms", issues
        )
        for name, value in (
            ("preservation_violations", preservation_value),
            ("input_tokens", input_value),
            ("output_tokens", output_value),
            ("total_tokens", total_value),
            ("latency_ms", latency_value),
        ):
            if value is not None and value < 0:
                issues.error(where, f"{name} must be non-negative")
        if (
            input_value is not None
            and output_value is not None
            and total_value is not None
            and total_value < input_value + output_value
        ):
            issues.error(
                where,
                "total_tokens is smaller than input_tokens + output_tokens",
            )

        missing = row["execution_status"] == "missing"
        expected_included = sample in canonical and not missing
        if included != expected_included:
            issues.error(where, "canonical_included conflicts with sample scope/status")
        expected_reason = ""
        if sample not in canonical:
            expected_reason = str(exclusions.get(sample) or "Outside canonical scope.")
        elif missing:
            expected_reason = "Missing committed row."
        if row["exclusion_reason"] != expected_reason:
            issues.error(where, "exclusion_reason does not match scope/status")
        if not included and not row["exclusion_reason"]:
            issues.error(where, "excluded row requires an exclusion_reason")

        critical_text = row["critical_failure"]
        if missing or direction == "forward":
            if critical_text != "":
                issues.error(where, "critical_failure must be empty for forward/missing rows")
            critical = None
        else:
            if critical_text not in {"true", "false"}:
                issues.error(where, "committed backward row needs critical_failure boolean")
            critical = critical_text == "true"

        if missing:
            expected_missing = {
                "execution_route": "none",
                "commit_outcome": "missing",
                "failure_stage": "infrastructure",
                "failure_label": "infrastructure_incomplete",
            }
            for field, expected in expected_missing.items():
                if row[field] != expected:
                    issues.error(where, f"missing row {field} must be {expected!r}")
            if any(
                value not in (None, "")
                for value in (
                    score,
                    preservation_value,
                    input_value,
                    output_value,
                    total_value,
                    latency_value,
                    row["raw_call_ids"],
                )
            ):
                issues.error(where, "missing row must not contain score, usage, latency, or calls")
        if row["raw_artifact_id"] != artifact_id:
            issues.error(where, "raw_artifact_id mismatch")
        for call_id in filter(None, row["raw_call_ids"].split(";")):
            if not SAFE_CALL_ID_RE.fullmatch(call_id):
                issues.error(where, f"unsafe raw call id: {call_id!r}")
        expected_raw_ref = (
            f"{artifact_id}#{method}/{sample}/rt{round_trip:02d}/{direction}"
        )
        if row["raw_ref"] != expected_raw_ref:
            issues.error(where, "raw_ref does not match the deterministic logical reference")

        parsed.append(
            {
                **row,
                "_key": key,
                "_included": included,
                "_missing": missing,
                "_round_trip": round_trip,
                "_rs": score,
                "_critical": critical,
                "_preservation_violations": preservation_value,
                "_input_tokens": input_value,
                "_output_tokens": output_value,
                "_total_tokens": total_value,
                "_latency_ms": latency_value,
            }
        )

    expected_set = set(expected_order)
    missing_keys = expected_set - seen
    extra_keys = seen - expected_set
    if missing_keys:
        issues.error(path, f"CSV is missing {len(missing_keys)} planned grid rows")
    if extra_keys:
        issues.error(path, f"CSV has {len(extra_keys)} rows outside the planned grid")
    if actual_order != expected_order:
        issues.error(path, "CSV rows are not in deterministic sample/method/RT/direction order")

    scores: dict[tuple[str, str], dict[int, float]] = defaultdict(dict)
    for row in parsed:
        if (
            not row["_missing"]
            and row["direction"] == "backward"
            and row["_rs"] is not None
        ):
            scores[(row["sample_id"], row["method"])][row["_round_trip"]] = row["_rs"]
    critical_keys: set[tuple[str, str, int, str]] = set()
    for (sample, method), values in scores.items():
        rounds = sorted(values)
        for previous, current in zip(rounds, rounds[1:]):
            before, after = values[previous], values[current]
            if (before > 0 and after <= 1e-9) or before - after >= 0.10:
                critical_keys.add((sample, method, current, "backward"))
    for row in parsed:
        if row["_critical"] is None:
            continue
        expected = row["_key"] in critical_keys
        if row["_critical"] != expected:
            issues.error(
                path,
                f"critical_failure mismatch for {row['_key']}: expected {expected}",
            )
    return parsed


def finite_mean(values: Iterable[float]) -> float | None:
    items = list(values)
    return statistics.mean(items) if items else None


def method_summary(
    rows: list[dict[str, Any]],
    round_trips: int,
) -> dict[str, Any]:
    backward = [row for row in rows if row["direction"] == "backward"]
    scores = [row["_rs"] for row in backward if row["_rs"] is not None]
    by_rt: dict[int, list[float]] = defaultdict(list)
    for row in backward:
        if row["_rs"] is not None:
            by_rt[row["_round_trip"]].append(row["_rs"])
    selected_k = sorted({1, min(5, round_trips), round_trips})
    latency = [row["_latency_ms"] for row in rows if row["_latency_ms"] is not None]
    tokens_in = sum(row["_input_tokens"] or 0 for row in rows)
    tokens_out = sum(row["_output_tokens"] or 0 for row in rows)
    tokens_total = sum(row["_total_tokens"] or 0 for row in rows)
    preservation = [
        row["_preservation_violations"]
        for row in rows
        if row["_preservation_violations"] is not None
    ]
    return {
        "committed_rows": len(rows),
        "backward_rows": len(backward),
        "mean_backward_rs": finite_mean(scores),
        "rs_at_k": {str(key): finite_mean(by_rt.get(key, [])) for key in selected_k},
        "critical_failures": {
            "count": sum(row["_critical"] is True for row in backward),
            "transitions": sum(
                row["_round_trip"] > 1 and row["_rs"] is not None for row in backward
            ),
            "theta": 0.10,
        },
        "tokens": {
            "input": tokens_in,
            "output": tokens_out,
            "input_plus_output": tokens_in + tokens_out,
            "semantic_call_total": tokens_total,
            "total": tokens_total,
        },
        "latency_ms": {
            "count": len(latency),
            "mean": round(statistics.mean(latency), 3) if latency else None,
            "median": round(statistics.median(latency), 3) if latency else None,
            "max": max(latency) if latency else None,
        },
        "execution_status": dict(Counter(row["execution_status"] for row in rows)),
        "commit_outcomes": dict(Counter(row["commit_outcome"] for row in rows)),
        "preservation_violations": {
            "recorded_rows": len(preservation),
            "unknown_rows": len(rows) - len(preservation),
            "total": sum(preservation),
            "max": max(preservation) if preservation else None,
        },
        "routes": dict(Counter(row["execution_route"] for row in rows)),
        "failure_labels": dict(
            Counter(
                row["failure_label"]
                for row in rows
                if row["failure_label"] != "none"
            )
        ),
    }


def paired_summary(
    rows: list[dict[str, Any]],
    method_a: str,
    method_b: str,
    *,
    scope_id: str,
    headline: bool,
) -> tuple[dict[str, Any], bool]:
    index: dict[str, dict[tuple[str, int], float]] = {
        method_a: {},
        method_b: {},
    }
    for row in rows:
        if (
            row["direction"] != "backward"
            or row["_rs"] is None
            or row["method"] not in index
        ):
            continue
        index[row["method"]][(row["sample_id"], row["_round_trip"])] = row["_rs"]
    keys = sorted(set(index[method_a]) & set(index[method_b]))
    values_a = [index[method_a][key] for key in keys]
    values_b = [index[method_b][key] for key in keys]
    differences = [left - right for left, right in zip(values_a, values_b)]
    t_statistic = p_value = None
    scipy_available = True
    if len(values_a) >= 2:
        try:
            from scipy.stats import ttest_rel

            result = ttest_rel(values_a, values_b)
            t_statistic = float(result.statistic)
            p_value = float(result.pvalue)
            if not math.isfinite(t_statistic):
                t_statistic = None
            if not math.isfinite(p_value):
                p_value = None
        except Exception:
            scipy_available = False
    sample_sd = statistics.stdev(differences) if len(differences) > 1 else 0.0
    delta = finite_mean(differences)
    return (
        {
            "scope_id": scope_id,
            "headline": headline,
            "method_a": method_a,
            "method_b": method_b,
            "paired_backward_rows": len(keys),
            "mean_a": finite_mean(values_a),
            "mean_b": finite_mean(values_b),
            "delta_a_minus_b": delta,
            "t": t_statistic,
            "p": p_value,
            "cohen_d": delta / sample_sd if delta is not None and sample_sd > 0 else 0.0,
        },
        scipy_available,
    )


def compare_tree(
    actual: Any,
    expected: Any,
    location: str,
    issues: Issues,
    *,
    skip_paths: set[str] | None = None,
) -> None:
    skip_paths = skip_paths or set()
    if location in skip_paths:
        return
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            issues.error(location, f"expected object, got {type(actual).__name__}")
            return
        if set(actual) != set(expected):
            issues.error(
                location,
                f"object keys differ; expected {sorted(expected)}, got {sorted(actual)}",
            )
        for key in sorted(set(actual) & set(expected)):
            compare_tree(
                actual[key],
                expected[key],
                f"{location}.{key}",
                issues,
                skip_paths=skip_paths,
            )
        return
    if isinstance(expected, list):
        if not isinstance(actual, list):
            issues.error(location, f"expected list, got {type(actual).__name__}")
            return
        if len(actual) != len(expected):
            issues.error(location, f"list length differs: {len(actual)} != {len(expected)}")
            return
        for index, (actual_item, expected_item) in enumerate(zip(actual, expected)):
            compare_tree(
                actual_item,
                expected_item,
                f"{location}[{index}]",
                issues,
                skip_paths=skip_paths,
            )
        return
    if isinstance(expected, float) or isinstance(actual, float):
        if expected is None or actual is None:
            if actual != expected:
                issues.error(location, f"value differs: {actual!r} != {expected!r}")
            return
        if not isinstance(actual, (int, float)) or not math.isclose(
            float(actual),
            float(expected),
            rel_tol=1e-9,
            abs_tol=1e-12,
        ):
            issues.error(location, f"numeric value differs: {actual!r} != {expected!r}")
        return
    if actual != expected:
        issues.error(location, f"value differs: {actual!r} != {expected!r}")


def validate_summary(
    summary: dict[str, Any],
    path: Path,
    experiment: dict[str, Any],
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
    issues: Issues,
    *,
    records_only: bool,
) -> None:
    check_schema(summary, "hybridpatch.experiment_summary/1", path, issues)
    check_forbidden_keys(summary, path.as_posix(), issues)
    experiment_id = entry["experiment_id"]
    if summary.get("experiment_id") != experiment_id:
        issues.error(path, "experiment_id mismatch")
    if summary.get("canonical_scope_id") != (
        experiment.get("canonical_report") or {}
    ).get("scope_id"):
        issues.error(path, "canonical_scope_id mismatch")
    methods = list(entry["arms"])
    canonical_rows = [
        row for row in rows if not row["_missing"] and row["_included"]
    ]
    committed = [row for row in rows if not row["_missing"]]
    expected_methods = {
        method: method_summary(
            [row for row in canonical_rows if row["method"] == method],
            int(entry["round_trips"]),
        )
        for method in methods
    }
    compare_tree(
        summary.get("methods"),
        expected_methods,
        f"{path}:methods",
        issues,
    )

    expected_comparisons: list[dict[str, Any]] = []
    scipy_available = True
    if "hybridpatch" in methods and "fullrewrite" in methods:
        comparison, scipy_available = paired_summary(
            canonical_rows,
            "hybridpatch",
            "fullrewrite",
            scope_id=str(summary.get("canonical_scope_id")),
            headline=experiment.get("claim_eligibility") == "canonical",
        )
        expected_comparisons.append(comparison)
    skip_paths: set[str] = set()
    if not scipy_available:
        skip_paths = {
            f"{path}:comparisons[0].t",
            f"{path}:comparisons[0].p",
        }
        issues.warn(path, "SciPy unavailable; paired t and p were not independently checked")
    compare_tree(
        summary.get("comparisons"),
        expected_comparisons,
        f"{path}:comparisons",
        issues,
        skip_paths=skip_paths,
    )
    if (
        experiment.get("claim_eligibility") in {"diagnostic_only", "none"}
        and any(item.get("headline") for item in summary.get("comparisons") or [])
    ):
        issues.warn(
            path,
            "diagnostic/non-claim record exposes a comparison with headline=true",
        )

    data = experiment.get("data") or {}
    exclusions = [
        {"sample_id": item.get("sample_id"), "reason": item.get("reason")}
        for item in data.get("excluded_samples") or []
    ]
    expected_scope = {
        "planned_samples": len(data.get("planned_sample_ids") or []),
        "canonical_samples": len(data.get("canonical_sample_ids") or []),
        "planned_rows": len(rows),
        "committed_rows": len(committed),
        "canonical_committed_rows": len(canonical_rows),
        "canonical_backward_rows": sum(
            row["direction"] == "backward" for row in canonical_rows
        ),
        "excluded_samples": exclusions,
    }
    compare_tree(summary.get("scope"), expected_scope, f"{path}:scope", issues)
    if summary.get("verification") != entry.get("verification"):
        issues.error(path, "summary verification does not match catalog")
    if summary.get("paper_reporting") != entry.get("paper_reporting"):
        issues.error(path, "summary paper_reporting does not match catalog")
    expected_source_files = [
        entry["archive_path"],
        *[report["path"] for report in entry.get("source_reports", [])],
    ]
    if summary.get("source_files") != expected_source_files:
        issues.error(path, "source_files does not match catalog evidence")
    for index, source_ref in enumerate(summary.get("source_files") or []):
        resolve_repo_ref(
            source_ref,
            f"{path}:source_files[{index}]",
            issues,
            must_exist=not records_only,
        )
    expected_policy = {
        "committed_method_failure": "stored evaluation score; defined reconstruction error is zero",
        "infrastructure_missing": "null and excluded from canonical statistics",
    }
    if summary.get("score_policy") != expected_policy:
        issues.error(path, "score_policy differs from schema v1")
    expected_definitions = {
        "rs_range": [0.0, 1.0],
        "critical_failure_theta": 0.10,
        "critical_failure_definition": "backward RS drop >= theta or collapse to zero",
    }
    if summary.get("metric_definitions") != expected_definitions:
        issues.error(path, "metric_definitions differs from schema v1")


def group_rows(
    rows: list[dict[str, Any]],
    key: str,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[key])].append(row)
    return dict(grouped)


def validate_failure_stats(
    document: dict[str, Any],
    path: Path,
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
    issues: Issues,
) -> None:
    check_schema(document, "hybridpatch.failure_stats/1", path, issues)
    check_forbidden_keys(document, path.as_posix(), issues)
    failures = [row for row in rows if row["failure_label"] != "none"]
    by_label: dict[str, dict[str, Any]] = {}
    for label, label_rows in group_rows(failures, "failure_label").items():
        by_label[label] = {
            "row_count": len(label_rows),
            "sample_method_count": len(
                {(row["sample_id"], row["method"]) for row in label_rows}
            ),
            "canonical_row_count": sum(row["_included"] for row in label_rows),
        }
    expected = {
        "schema": "hybridpatch.failure_stats/1",
        "experiment_id": entry["experiment_id"],
        "primary_label_policy": "deterministic_root_cause_v1",
        "total_planned_rows": len(rows),
        "committed_rows": sum(not row["_missing"] for row in rows),
        "non_none_failure_rows": len(failures),
        "affected_sample_methods": len(
            {(row["sample_id"], row["method"]) for row in failures}
        ),
        "by_stage": dict(Counter(row["failure_stage"] for row in failures)),
        "by_label": by_label,
        "by_method": {
            method: dict(Counter(row["failure_label"] for row in method_rows))
            for method, method_rows in group_rows(failures, "method").items()
        },
        "canonical_exclusions": dict(
            Counter(
                row["failure_label"]
                for row in rows
                if not row["_included"] and row["failure_label"] != "none"
            )
        ),
        "critical_failures": {
            "count": sum(row["_critical"] is True for row in rows),
            "by_method": dict(
                Counter(
                    row["method"] for row in rows if row["_critical"] is True
                )
            ),
        },
    }
    compare_tree(document, expected, str(path), issues)


def validate_representative_cases(
    cases: list[dict[str, Any]],
    path: Path,
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
    issues: Issues,
) -> None:
    row_index = {row["_key"]: row for row in rows}
    case_ids: set[str] = set()
    represented_labels: set[str] = set()
    for index, case in enumerate(cases, 1):
        where = f"{path}:line {index}"
        check_schema(case, "hybridpatch.failure_case/1", path, issues)
        check_forbidden_keys(case, where, issues)
        case_id = case.get("case_id")
        if not isinstance(case_id, str) or not case_id:
            issues.error(where, "case_id must be non-empty")
        elif case_id in case_ids:
            issues.error(where, f"duplicate case_id: {case_id}")
        else:
            case_ids.add(case_id)
        if case.get("experiment_id") != entry["experiment_id"]:
            issues.error(where, "experiment_id mismatch")
        try:
            key = (
                str(case["sample_id"]),
                str(case["method"]),
                int(case["round_trip"]),
                str(case["direction"]),
            )
        except (KeyError, TypeError, ValueError):
            issues.error(where, "invalid row identity fields")
            continue
        row = row_index.get(key)
        if row is None:
            issues.error(where, f"case does not map to samples.csv row: {key}")
            continue
        label = case.get("failure_label")
        represented_labels.add(str(label))
        if label != row["failure_label"] or case.get("failure_stage") != row["failure_stage"]:
            issues.error(where, "failure label/stage differs from samples.csv")
        if case.get("canonical_included") != row["_included"]:
            issues.error(where, "canonical_included differs from samples.csv")
        if case.get("severity") not in {"critical", "high", "medium", "low"}:
            issues.error(where, "invalid severity")
        reason = case.get("reason_summary")
        if not isinstance(reason, str) or len(reason) > 500:
            issues.error(where, "reason_summary must be a string of at most 500 chars")
        evidence = case.get("evidence")
        if not isinstance(evidence, dict):
            issues.error(where, "evidence must be an object")
            continue
        if evidence.get("raw_artifact_id") != row["raw_artifact_id"]:
            issues.error(where, "evidence raw_artifact_id mismatch")
        if evidence.get("raw_ref") != row["raw_ref"]:
            issues.error(where, "evidence raw_ref mismatch")
        expected_calls = list(filter(None, row["raw_call_ids"].split(";")))
        if evidence.get("raw_call_ids") != expected_calls:
            issues.error(where, "evidence raw_call_ids mismatch")
        expected_rs = row["_rs"]
        actual_rs = evidence.get("rs")
        if expected_rs is None:
            if actual_rs is not None:
                issues.error(where, "evidence rs must be null")
        elif not isinstance(actual_rs, (int, float)) or not math.isclose(
            float(actual_rs), expected_rs, rel_tol=1e-9, abs_tol=1e-12
        ):
            issues.error(where, "evidence rs mismatch")
    failure_labels = {
        row["failure_label"] for row in rows if row["failure_label"] != "none"
    }
    if represented_labels != failure_labels:
        issues.error(
            path,
            "representative cases must cover every non-none failure label at least once",
        )


def validate_report(
    path: Path,
    experiment: dict[str, Any],
    summary: dict[str, Any],
    issues: Issues,
) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        issues.error(path, f"cannot read report: {exc}")
        return
    nonempty = [line.strip() for line in text.splitlines() if line.strip()]
    if not nonempty:
        issues.error(path, "report must not be empty")
        return
    if not nonempty[0].startswith("# "):
        issues.error(path, "report must start with a level-1 Markdown heading")
    experiment_id = str(experiment.get("experiment_id") or "")
    scope_id = str(summary.get("canonical_scope_id") or "")
    if experiment_id and experiment_id not in text:
        issues.error(path, "report must name its experiment_id")
    if scope_id and scope_id not in text:
        issues.error(path, "report must name its canonical scope id")
    if not any(line.startswith("## ") for line in nonempty[1:]):
        issues.error(path, "report must contain at least one level-2 section")
    paper_reporting = experiment.get("paper_reporting")
    if isinstance(paper_reporting, dict):
        if "## Paper protocol reporting" not in text:
            issues.error(path, "report must include the paper protocol section")
        paper_scope = str(paper_reporting.get("scope_id") or "")
        if paper_scope and paper_scope not in text:
            issues.error(path, "report must name the paper protocol scope")
        delta = paper_reporting.get("delta_hybridpatch_minus_fullrewrite")
        if isinstance(delta, (int, float)) and not isinstance(delta, bool):
            formatted_delta = f"{float(delta):+.6f}"
            if formatted_delta not in text:
                issues.error(path, "report must include the paper protocol delta")


def validate_casebook(
    cases: list[dict[str, Any]],
    path: Path,
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
    issues: Issues,
) -> None:
    row_index = {row["_key"]: row for row in rows}
    case_ids: set[str] = set()
    for index, case in enumerate(cases, 1):
        where = f"{path}:line {index}"
        if set(case) != CASEBOOK_FIELDS:
            issues.error(
                where,
                "casebook fields differ; "
                f"expected {sorted(CASEBOOK_FIELDS)}, got {sorted(case)}",
            )
        if case.get("schema") != "hybridpatch.casebook_case/1":
            issues.error(where, "invalid casebook schema")
        check_forbidden_keys(case, where, issues)
        case_id = case.get("case_id")
        if not isinstance(case_id, str) or not SAFE_CALL_ID_RE.fullmatch(case_id):
            issues.error(where, "case_id must be a non-empty safe identifier")
        elif case_id in case_ids:
            issues.error(where, f"duplicate case_id: {case_id}")
        else:
            case_ids.add(case_id)
        if case.get("experiment_id") != entry["experiment_id"]:
            issues.error(where, "experiment_id mismatch")
        case_kind = case.get("case_kind")
        check_enum(case_kind, CASEBOOK_KINDS, f"{where}.case_kind", issues)
        categories = case.get("categories")
        if (
            not isinstance(categories, list)
            or not categories
            or any(not isinstance(item, str) or not item.strip() for item in categories)
        ):
            issues.error(where, "categories must be a non-empty string list")
        elif len(categories) != len(set(categories)):
            issues.error(where, "categories must not contain duplicates")
        selection_reason = case.get("selection_reason")
        if (
            not isinstance(selection_reason, str)
            or not selection_reason.strip()
            or len(selection_reason) > 1000
        ):
            issues.error(
                where,
                "selection_reason must be a non-empty string of at most 1000 chars",
            )
        sample_id = case.get("sample_id")
        if not isinstance(sample_id, str) or not sample_id:
            issues.error(where, "sample_id must be a non-empty string")
        round_trip = case.get("round_trip")
        if (
            not isinstance(round_trip, int)
            or isinstance(round_trip, bool)
            or not 1 <= round_trip <= int(entry["round_trips"])
        ):
            issues.error(where, "round_trip is outside the catalog range")
        direction = case.get("direction")
        if direction not in {"forward", "backward"}:
            issues.error(where, "direction must be forward or backward")
        methods = case.get("methods")
        if not isinstance(methods, dict) or not methods:
            issues.error(where, "methods must be a non-empty object")
            methods = {}
        method_names = set(methods)
        unknown_methods = method_names - set(entry["arms"])
        if unknown_methods:
            issues.error(where, f"methods contains unknown arms: {sorted(unknown_methods)}")
        if case_kind == "paired_comparison" and method_names != {
            "hybridpatch",
            "fullrewrite",
        }:
            issues.error(
                where,
                "paired_comparison requires exactly hybridpatch and fullrewrite",
            )
        if case_kind == "single_method_event" and len(method_names) != 1:
            issues.error(where, "single_method_event requires exactly one method")

        numeric_scores: dict[str, float | None] = {}
        for method, method_value in methods.items():
            method_where = f"{where}.methods.{method}"
            if not isinstance(method_value, dict):
                issues.error(method_where, "method value must be an object")
                continue
            if set(method_value) != CASEBOOK_METHOD_FIELDS:
                issues.error(
                    method_where,
                    "method fields differ; "
                    f"expected {sorted(CASEBOOK_METHOD_FIELDS)}, "
                    f"got {sorted(method_value)}",
                )
            score = method_value.get("rs")
            if score is None:
                numeric_scores[method] = None
            elif (
                isinstance(score, bool)
                or not isinstance(score, (int, float))
                or not math.isfinite(float(score))
                or not 0.0 <= float(score) <= 1.0
            ):
                issues.error(method_where, "rs must be null or a finite number in [0, 1]")
                numeric_scores[method] = None
            else:
                numeric_scores[method] = float(score)
            check_enum(
                method_value.get("route"),
                EXECUTION_ROUTES,
                f"{method_where}.route",
                issues,
            )
            check_enum(
                method_value.get("status"),
                EXECUTION_STATUSES,
                f"{method_where}.status",
                issues,
            )
            check_enum(
                method_value.get("commit_outcome"),
                COMMIT_OUTCOMES,
                f"{method_where}.commit_outcome",
                issues,
            )
            check_enum(
                method_value.get("failure_label"),
                FAILURE_LABELS,
                f"{method_where}.failure_label",
                issues,
            )
            if not isinstance(method_value.get("raw_ref"), str):
                issues.error(method_where, "raw_ref must be a string")
            if (
                isinstance(sample_id, str)
                and isinstance(round_trip, int)
                and not isinstance(round_trip, bool)
                and direction in {"forward", "backward"}
            ):
                row = row_index.get((sample_id, method, round_trip, direction))
                if row is None:
                    issues.error(
                        method_where,
                        "casebook method does not map to a samples.csv row",
                    )
                    continue
                expected_values = {
                    "route": row["execution_route"],
                    "status": row["execution_status"],
                    "commit_outcome": row["commit_outcome"],
                    "failure_label": row["failure_label"],
                    "raw_ref": row["raw_ref"],
                }
                for field, expected in expected_values.items():
                    if method_value.get(field) != expected:
                        issues.error(
                            method_where,
                            f"{field} differs from samples.csv",
                        )
                expected_rs = row["_rs"]
                actual_rs = numeric_scores.get(method)
                if expected_rs is None:
                    if score is not None:
                        issues.error(method_where, "rs must be null for this samples.csv row")
                elif actual_rs is None or not math.isclose(
                    actual_rs,
                    expected_rs,
                    rel_tol=1e-9,
                    abs_tol=1e-12,
                ):
                    issues.error(method_where, "rs differs from samples.csv")

        delta = case.get("score_delta_hybridpatch_minus_fullrewrite")
        expected_delta: float | None = None
        if case_kind == "paired_comparison":
            hp_score = numeric_scores.get("hybridpatch")
            fr_score = numeric_scores.get("fullrewrite")
            if hp_score is not None and fr_score is not None:
                expected_delta = hp_score - fr_score
        if expected_delta is None:
            if delta is not None:
                issues.error(where, "score delta must be null when a paired score is unavailable")
        elif (
            isinstance(delta, bool)
            or not isinstance(delta, (int, float))
            or not math.isfinite(float(delta))
            or not math.isclose(
                float(delta),
                expected_delta,
                rel_tol=1e-9,
                abs_tol=1e-12,
            )
        ):
            issues.error(where, "score delta differs from method scores")


def parse_verification(path: Path, issues: Issues) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        issues.error(path, f"cannot read verification file: {exc}")
        return values
    for line_number, line in enumerate(lines, 1):
        if not line:
            continue
        if "=" not in line:
            issues.error(path, f"line {line_number}: expected key=value")
            continue
        key, value = line.split("=", 1)
        if not key or key in values:
            issues.error(path, f"line {line_number}: duplicate/empty key")
            continue
        values[key] = value
    return values


def nullable_text_int(
    value: str | None,
    location: str,
    issues: Issues,
) -> int | None:
    if value in (None, ""):
        return None
    if not re.fullmatch(r"-?\d+", value):
        issues.error(location, f"expected integer or empty, got {value!r}")
        return None
    return int(value)


def validate_verification(
    values: dict[str, str],
    path: Path,
    entry: dict[str, Any],
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    issues: Issues,
    *,
    records_only: bool,
) -> None:
    expected_keys = {
        "schema",
        "experiment_id",
        "status",
        "evidence_basis",
        "verified_at",
        "verifier_command",
        "verifier_exit_code",
        "verified_rows_total",
        "canonical_rows_expected",
        "canonical_rows_covered",
        "mismatched_rows",
        "accepted_known_exception",
        "structural_validation_status",
        "derivation_validation_status",
        "archive_inclusive_verified_rows",
        "source_ref",
        "source_ref_sha256",
        "source_log_ref",
        "source_log_sha256",
        "notes",
    }
    if set(values) != expected_keys:
        issues.error(
            path,
            f"verification keys differ; expected {sorted(expected_keys)}, got {sorted(values)}",
        )
    if values.get("schema") != "hybridpatch.verification/1":
        issues.error(path, "invalid verification schema")
    if values.get("experiment_id") != entry["experiment_id"]:
        issues.error(path, "experiment_id mismatch")
    verification = entry["verification"]
    if values.get("status") != str(verification.get("status")):
        issues.error(path, "status differs from catalog")
    check_enum(values.get("status"), VERIFICATION_STATUSES, f"{path}:status", issues)
    check_enum(
        values.get("evidence_basis"),
        EVIDENCE_BASES,
        f"{path}:evidence_basis",
        issues,
    )
    source_log = verification.get("log")
    source_log_path: Path | None = None
    expected_log_hash = ""
    if source_log:
        source_log_path = resolve_repo_ref(
            source_log,
            f"{path}:source_log_ref",
            issues,
            must_exist=not records_only,
        )
        if (
            not records_only
            and source_log_path is not None
            and source_log_path.is_file()
        ):
            expected_log_hash = sha256_file(source_log_path)
        elif records_only:
            expected_log_hash = values.get("source_log_sha256", "")
    source_ref = verification.get("source")
    source_ref_path: Path | None = None
    expected_source_hash = ""
    if source_ref:
        source_ref_path = resolve_repo_ref(
            source_ref,
            f"{path}:source_ref",
            issues,
            must_exist=not records_only,
        )
        if (
            not records_only
            and source_ref_path is not None
            and source_ref_path.is_file()
        ):
            expected_source_hash = sha256_file(source_ref_path)
        elif records_only:
            expected_source_hash = values.get("source_ref_sha256", "")
    expected_basis = (
        "archived_log"
        if expected_log_hash
        else "documented_claim"
        if expected_source_hash
        else "none"
    )
    if values.get("evidence_basis") != expected_basis:
        issues.error(path, f"evidence_basis must be {expected_basis!r}")
    if values.get("source_log_ref", "") != (source_log or ""):
        issues.error(path, "source_log_ref differs from catalog")
    if values.get("source_log_sha256", "") != expected_log_hash:
        issues.error(path, "source_log_sha256 mismatch")
    if source_log and not SHA256_RE.fullmatch(expected_log_hash):
        issues.error(path, "source log digest is not SHA-256")
    if not source_log and values.get("source_log_sha256", ""):
        issues.error(path, "source_log_sha256 must be empty without source_log_ref")
    has_replay = verification.get("replayed_backward_rows") is not None
    expected_exit = None
    if has_replay and verification.get("status") == "pass":
        expected_exit = 0
    elif has_replay and verification.get("status") == "fail":
        expected_exit = 1
    actual_exit = nullable_text_int(
        values.get("verifier_exit_code"),
        f"{path}:verifier_exit_code",
        issues,
    )
    if actual_exit != expected_exit:
        issues.error(path, f"verifier_exit_code must be {expected_exit!r}")
    verified_total = nullable_text_int(
        values.get("verified_rows_total"),
        f"{path}:verified_rows_total",
        issues,
    )
    if verified_total != verification.get("replayed_backward_rows"):
        issues.error(path, "verified_rows_total differs from catalog")
    expected_canonical = (summary.get("scope") or {}).get("canonical_backward_rows")
    canonical_expected = nullable_text_int(
        values.get("canonical_rows_expected"),
        f"{path}:canonical_rows_expected",
        issues,
    )
    canonical_covered = nullable_text_int(
        values.get("canonical_rows_covered"),
        f"{path}:canonical_rows_covered",
        issues,
    )
    if canonical_expected != expected_canonical:
        issues.error(path, "canonical_rows_expected differs from summary scope")
    expected_covered = expected_canonical if has_replay else None
    if canonical_covered != expected_covered:
        issues.error(path, "canonical_rows_covered differs from replay status/scope")
    if verified_total is not None and canonical_covered is not None:
        if verified_total < canonical_covered:
            issues.error(path, "verified_rows_total cannot be smaller than canonical coverage")
    mismatches = nullable_text_int(
        values.get("mismatched_rows"),
        f"{path}:mismatched_rows",
        issues,
    )
    if mismatches != verification.get("mismatches", 0):
        issues.error(path, "mismatched_rows differs from catalog")
    if values.get("accepted_known_exception", "") != (
        ""
        if verification.get("accepted_known_exception") is None
        else str(verification.get("accepted_known_exception"))
    ):
        issues.error(path, "accepted_known_exception differs from catalog")
    if values.get("structural_validation_status", "") != str(
        verification.get("structural_validation_status") or ""
    ):
        issues.error(path, "structural_validation_status differs from catalog")
    if values.get("derivation_validation_status", "") != str(
        verification.get("derivation_validation_status") or ""
    ):
        issues.error(path, "derivation_validation_status differs from catalog")
    archive_inclusive = nullable_text_int(
        values.get("archive_inclusive_verified_rows"),
        f"{path}:archive_inclusive_verified_rows",
        issues,
    )
    if archive_inclusive != verification.get(
        "archive_inclusive_replayed_backward_rows"
    ):
        issues.error(path, "archive_inclusive_verified_rows differs from catalog")
    all_committed_backward = sum(
        not row["_missing"] and row["direction"] == "backward" for row in rows
    )
    if archive_inclusive is not None:
        if archive_inclusive != all_committed_backward:
            issues.error(
                path,
                "archive-inclusive replay count differs from committed backward rows",
            )
        if verified_total != expected_canonical:
            issues.error(
                path,
                "canonical replay count differs from canonical backward scope",
            )
    elif verified_total is not None and verified_total != all_committed_backward:
        issues.error(
            path,
            "replay count differs from all committed backward rows",
        )
    if values.get("source_ref", "") != str(source_ref or ""):
        issues.error(path, "source_ref differs from catalog")
    if values.get("source_ref_sha256", "") != expected_source_hash:
        issues.error(path, "source_ref_sha256 mismatch")
    if source_ref and not SHA256_RE.fullmatch(expected_source_hash):
        issues.error(path, "source reference digest is not SHA-256")
    if not source_ref and values.get("source_ref_sha256", ""):
        issues.error(path, "source_ref_sha256 must be empty without source_ref")
    expected_notes = " | ".join(verification.get("exceptions") or [])
    if values.get("notes", "") != expected_notes:
        issues.error(path, "notes differs from catalog exceptions")
    command = values.get("verifier_command", "")
    if has_replay:
        if "<experiment-root>" not in command or WINDOWS_ABSOLUTE_RE.search(command):
            issues.error(path, "verifier_command must use the logical <experiment-root>")
    elif command:
        issues.error(path, "verifier_command must be empty when replay was not run")


def validate_raw_manifest(
    document: dict[str, Any],
    path: Path,
    entry: dict[str, Any],
    experiment: dict[str, Any],
    issues: Issues,
    *,
    verify_tree_hash: bool,
    records_only: bool,
) -> None:
    check_schema(document, "hybridpatch.raw_manifest/1", path, issues)
    check_forbidden_keys(document, path.as_posix(), issues)
    experiment_id = entry["experiment_id"]
    if document.get("experiment_id") != experiment_id:
        issues.error(path, "experiment_id mismatch")
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        issues.error(path, "artifacts must be a non-empty list")
        return
    ids: set[str] = set()
    for index, artifact in enumerate(artifacts):
        where = f"{path}:artifacts[{index}]"
        if not isinstance(artifact, dict):
            issues.error(where, "artifact must be an object")
            continue
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str) or not artifact_id:
            issues.error(where, "artifact_id must be non-empty")
        elif artifact_id in ids:
            issues.error(where, "duplicate artifact_id")
        else:
            ids.add(artifact_id)
        storage_key = artifact.get("storage_key")
        storage_path: Path | None = None
        if artifact.get("storage") in {
            "ignored_local_directory",
            "repository_worktree_derived_view",
        }:
            storage_path = resolve_repo_ref(
                storage_key,
                f"{where}.storage_key",
                issues,
                must_exist=not records_only,
            )
            if (
                storage_path is not None
                and storage_path.exists()
                and not storage_path.is_dir()
            ):
                issues.error(where, "storage_key must reference a directory")
        for field in ("file_count", "size_bytes"):
            value = artifact.get(field)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                issues.error(where, f"{field} must be a non-negative integer or null")
        algorithm = artifact.get("tree_hash_algorithm")
        digest = artifact.get("tree_sha256")
        if (algorithm is None) != (digest is None):
            issues.error(where, "tree hash algorithm and digest must both be set or null")
        if digest is not None and (
            algorithm != "sha256-tree-v1"
            or not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
        ):
            issues.error(where, "invalid sha256-tree-v1 digest")
        if digest is None:
            issues.warn(where, "raw tree hash is not recorded")
        if verify_tree_hash and not records_only and storage_path is not None:
            actual = tree_digest(storage_path)
            for field, expected_field in (
                ("tree_hash_algorithm", "algorithm"),
                ("tree_sha256", "tree_sha256"),
                ("file_count", "file_count"),
                ("size_bytes", "size_bytes"),
                ("skipped_sensitive_files", "skipped_sensitive_files"),
                ("skipped_symlinks", "skipped_symlinks"),
            ):
                if artifact.get(field) != actual.get(expected_field):
                    issues.error(where, f"{field} differs from recomputed raw tree")
        skipped_sensitive = artifact.get("skipped_sensitive_files")
        if not isinstance(skipped_sensitive, list):
            issues.error(where, "skipped_sensitive_files must be a list")
        elif skipped_sensitive:
            issues.error(where, "raw archive contains .env-like sensitive files")
        skipped_links = artifact.get("skipped_symlinks")
        if not isinstance(skipped_links, list):
            issues.error(where, "skipped_symlinks must be a list")
        elif skipped_links:
            issues.warn(where, "raw tree hash excludes symlinks")
        flags = artifact.get("content_flags")
        if not isinstance(flags, dict) or any(
            not isinstance(value, bool) for value in flags.values()
        ):
            issues.error(where, "content_flags must be a boolean object")
        scan = artifact.get("credential_scan")
        if not isinstance(scan, dict) or not isinstance(scan.get("status"), str):
            issues.error(where, "credential_scan.status must be a string")
        else:
            status = scan["status"].lower()
            if not (
                status.startswith("pass")
                or status == "not_applicable"
                or "zero matches" in status
            ):
                issues.warn(where, f"raw credential scan is not current/pass: {scan['status']}")
        sanitization = artifact.get("sanitization")
        if not isinstance(sanitization, dict) or sanitization.get("status") not in {
            "raw",
            "curated_manifest",
            "sanitized",
        }:
            issues.error(where, "invalid sanitization status")
        for source_index, source_ref in enumerate(artifact.get("source_experiments") or []):
            resolve_repo_ref(
                source_ref,
                f"{where}.source_experiments[{source_index}]",
                issues,
                must_exist=not records_only,
            )
    expected_artifact = ((experiment.get("source") or {}).get("raw_artifact_id"))
    if expected_artifact not in ids:
        issues.error(path, "experiment source.raw_artifact_id is absent from manifest")


def git_command(arguments: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def validate_git_rules(directory: Path, issues: Issues) -> None:
    ignored: list[str] = []
    for filename in sorted(RECORD_FILENAMES):
        path = directory / filename
        relative = path.relative_to(ROOT).as_posix()
        result = git_command(["check-ignore", "--no-index", "-q", "--", relative])
        if result.returncode == 0:
            ignored.append(filename)
        elif result.returncode not in {0, 1}:
            issues.error(directory, "git check-ignore failed for required record files")
            break
    if ignored:
        issues.error(
            directory,
            "required record files are ignored by .gitignore: "
            f"{ignored}; check broad exp_*/records patterns",
        )
    raw_probe = (directory / "api_raw" / "probe.request.json").relative_to(ROOT).as_posix()
    probe_result = git_command(
        ["check-ignore", "--no-index", "-q", "--", raw_probe]
    )
    if probe_result.returncode == 1:
        issues.error(directory, "unexpected raw payload path is not ignored by Git")
    elif probe_result.returncode not in {0, 1}:
        issues.error(directory, "git check-ignore failed for raw-payload probe")
    tracked = git_command(
        ["ls-files", "-z", "--", directory.relative_to(ROOT).as_posix()]
    )
    if tracked.returncode != 0:
        issues.error(directory, "git ls-files failed")
        return
    for raw_path in tracked.stdout.split(b"\0"):
        if not raw_path:
            continue
        try:
            tracked_path = Path(raw_path.decode("utf-8"))
        except UnicodeError:
            issues.error(directory, "tracked record path is not UTF-8")
            continue
        if tracked_path.name not in RECORD_FILENAMES:
            issues.error(tracked_path.as_posix(), "tracked record file is not allowlisted")


def validate_unexpected_record_dirs(
    entries: dict[str, tuple[str, dict[str, Any]]],
    issues: Issues,
) -> None:
    by_owner: dict[str, set[str]] = defaultdict(set)
    for experiment_id, (owner, _) in entries.items():
        by_owner[owner].add(experiment_id)
    for owner, expected in by_owner.items():
        root = ROOT / owner / "records"
        if not root.exists():
            issues.error(root, "owner records directory is missing")
            continue
        for candidate in root.iterdir():
            if candidate.is_dir() and candidate.name not in expected:
                issues.error(candidate, "record directory is not declared in the catalog")


def validate_one(
    owner: str,
    entry: dict[str, Any],
    catalog_digest: str,
    local_secrets: dict[str, str],
    issues: Issues,
    *,
    verify_tree_hash: bool,
    check_git: bool,
    records_only: bool,
) -> None:
    experiment_id = entry["experiment_id"]
    directory = ROOT / owner / "records" / experiment_id
    if not validate_record_files(directory, local_secrets, issues):
        return
    experiment_path = directory / "experiment.yaml"
    summary_path = directory / "summary.json"
    samples_path = directory / "samples.csv"
    failure_path = directory / "failure_stats.json"
    cases_path = directory / "representative_failures.jsonl"
    report_path = directory / "report.md"
    casebook_path = directory / "casebook.jsonl"
    verification_path = directory / "verification.txt"
    raw_manifest_path = directory / "raw_manifest.json"

    experiment = read_yaml(experiment_path, issues)
    summary = read_json(summary_path, issues)
    failure_stats = read_json(failure_path, issues)
    raw_manifest = read_json(raw_manifest_path, issues)
    cases = read_jsonl(cases_path, issues)
    casebook = read_jsonl(casebook_path, issues)
    csv_rows = parse_csv(samples_path, issues)
    verification = parse_verification(verification_path, issues)
    if None in (experiment, summary, failure_stats, raw_manifest):
        return
    assert experiment is not None
    assert summary is not None
    assert failure_stats is not None
    assert raw_manifest is not None

    validate_experiment_document(
        experiment,
        experiment_path,
        owner,
        entry,
        catalog_digest,
        summary,
        issues,
        records_only=records_only,
    )
    parsed_rows = validate_sample_rows(
        csv_rows,
        samples_path,
        experiment,
        entry,
        issues,
    )
    validate_summary(
        summary,
        summary_path,
        experiment,
        entry,
        parsed_rows,
        issues,
        records_only=records_only,
    )
    validate_failure_stats(failure_stats, failure_path, entry, parsed_rows, issues)
    validate_representative_cases(cases, cases_path, entry, parsed_rows, issues)
    validate_report(report_path, experiment, summary, issues)
    validate_casebook(casebook, casebook_path, entry, parsed_rows, issues)
    validate_verification(
        verification,
        verification_path,
        entry,
        summary,
        parsed_rows,
        issues,
        records_only=records_only,
    )
    validate_raw_manifest(
        raw_manifest,
        raw_manifest_path,
        entry,
        experiment,
        issues,
        verify_tree_hash=verify_tree_hash,
        records_only=records_only,
    )
    artifact_id = ((experiment.get("source") or {}).get("raw_artifact_id"))
    if any(row["raw_artifact_id"] != artifact_id for row in parsed_rows):
        issues.error(samples_path, "CSV contains raw artifact ids not linked by experiment.yaml")
    for relative_ref in (
        (experiment.get("canonical_report") or {}).get("record_ref"),
        experiment.get("raw_manifest_ref"),
    ):
        if relative_ref:
            target = directory / str(relative_ref)
            if not target.is_file():
                issues.error(directory, f"record-local reference is missing: {relative_ref}")
    if check_git:
        validate_git_rules(directory, issues)


def print_issues(issues: Issues, quiet: bool) -> None:
    for message in issues.errors:
        print(f"ERROR: {message}")
    if not quiet:
        for message in issues.warnings:
            print(f"WARN:  {message}")
    if issues.suppressed_errors:
        print(f"ERROR: ... {issues.suppressed_errors} additional errors suppressed")
    if not quiet and issues.suppressed_warnings:
        print(f"WARN:  ... {issues.suppressed_warnings} additional warnings suppressed")


def main() -> int:
    args = parse_args()
    issues = Issues()
    catalog_path = args.catalog.resolve()
    try:
        catalog_path.relative_to(ROOT.resolve())
    except ValueError:
        issues.error(catalog_path, "catalog must be inside the repository")
        print_issues(issues, args.quiet)
        return 1
    catalog, catalog_digest = load_catalog(catalog_path, issues)
    entries = catalog_entries(
        catalog,
        issues,
        records_only=args.records_only,
    )
    requested = set(args.only)
    unknown = requested - set(entries)
    for experiment_id in sorted(unknown):
        issues.error("command line", f"unknown --only experiment id: {experiment_id}")
    selected = {
        experiment_id: value
        for experiment_id, value in entries.items()
        if not requested or experiment_id in requested
    }
    if not requested:
        validate_unexpected_record_dirs(entries, issues)
    local_secrets = load_local_secrets()
    for experiment_id in sorted(selected):
        owner, entry = selected[experiment_id]
        validate_one(
            owner,
            entry,
            catalog_digest,
            local_secrets,
            issues,
            verify_tree_hash=args.verify_tree_hash,
            check_git=not args.skip_git_check,
            records_only=args.records_only,
        )
    print_issues(issues, args.quiet)
    warning_failure = args.warnings_as_errors and bool(issues.warnings)
    if issues.errors or warning_failure:
        print(
            "[records-validator] FAIL "
            f"experiments={len(selected)} errors={len(issues.errors) + issues.suppressed_errors} "
            f"warnings={len(issues.warnings) + issues.suppressed_warnings}"
        )
        return 1
    print(
        "[records-validator] PASS "
        f"experiments={len(selected)} warnings={len(issues.warnings) + issues.suppressed_warnings}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
