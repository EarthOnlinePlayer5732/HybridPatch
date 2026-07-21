"""Build compact, auditable experiment records from ignored raw archives.

The builder is intentionally offline.  It never calls a model, mutates an
experiment archive, or re-runs result verification.  Historical gaps remain
explicitly null; archived per-file fingerprints are preferred over the current
checkout.
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
import tempfile
from collections import Counter, defaultdict
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Iterable

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

RECORD_FILENAMES = [
    "experiment.yaml",
    "report.md",
    "summary.json",
    "samples.csv",
    "casebook.jsonl",
    "failure_stats.json",
    "representative_failures.jsonl",
    "verification.txt",
    "raw_manifest.json",
]

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
    "evaluator_incomplete": "evaluator",
    "infrastructure_incomplete": "infrastructure",
    "content_regression": "quality",
    "unknown_failure": "unknown",
}

CREDENTIAL_NAME_RE = re.compile(
    r"(?i)(?:^|_)(?:api_?key|key|access_?token|refresh_?token|password|secret|authorization|cookie)(?:$|_)"
)
CREDENTIAL_PATTERNS = {
    "authorization_header": re.compile(rb"(?i)\bauthorization\s*:"),
    "api_key_header": re.compile(rb"(?i)\bx-api-key\s*:"),
    "cookie_header": re.compile(rb"(?i)\b(?:set-)?cookie\s*:"),
    "bearer_token": re.compile(rb"(?i)\bbearer\s+[A-Za-z0-9._~+/\-=]{16,}"),
    "private_key_pem": re.compile(
        rb"(?i)-----begin (?:rsa |ec |openssh )?private key-----"
    ),
    "openai_like_key": re.compile(rb"\bsk-[A-Za-z0-9_-]{20,}\b"),
    "github_token": re.compile(rb"\bghp_[A-Za-z0-9]{20,}\b"),
    "slack_token": re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "google_api_key": re.compile(rb"\bAIza[A-Za-z0-9_-]{20,}\b"),
    "credential_assignment": re.compile(
        rb"(?i)\b(?:api_?key|access_?token|refresh_?token|password|secret)"
        rb"\s*[=:]\s*[\"']?[A-Za-z0-9._~+/\-=]{16,}"
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument(
        "--only",
        nargs="*",
        default=[],
        help="Optional experiment ids to build.",
    )
    parser.add_argument(
        "--skip-tree-hash",
        action="store_true",
        help="Keep an existing tree hash or emit null instead of hashing archives.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Compare generated content with disk instead of writing it.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help=(
            "Build selected records in a temporary directory to validate all "
            "contracts without changing generated records or indexes."
        ),
    )
    parser.add_argument(
        "--sealed-manifest",
        type=Path,
        default=None,
        help=(
            "Reuse a validated artifact seal for the single --only experiment "
            "instead of scanning the raw tree again."
        ),
    )
    return parser.parse_args()


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"{path}:{line_number}: invalid JSONL") from exc
            if not isinstance(value, dict):
                raise RuntimeError(f"{path}:{line_number}: JSONL row is not an object")
            rows.append(value)
    return rows


def stable_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"


def write_atomic(path: Path, content: str, *, check: bool) -> bool:
    old = path.read_text(encoding="utf-8") if path.exists() else None
    if old == content:
        return False
    if check:
        raise RuntimeError(f"generated record is stale: {path.relative_to(ROOT)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    temporary.replace(path)
    return True


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_local_secret_values(root: Path = ROOT) -> list[bytes]:
    values: set[bytes] = set()
    for env_path in sorted(root.glob(".env*")):
        if not env_path.is_file() or env_path.name.endswith(".example"):
            continue
        with env_path.open(encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                name, value = stripped.split("=", 1)
                if not CREDENTIAL_NAME_RE.search(name.strip()):
                    continue
                value = value.strip()
                if (
                    len(value) >= 2
                    and value[0] == value[-1]
                    and value[0] in {"'", '"'}
                ):
                    value = value[1:-1]
                lowered = value.lower()
                if (
                    len(value) < 8
                    or not value
                    or any(
                        token in lowered
                        for token in ("changeme", "example", "placeholder", "your_key")
                    )
                ):
                    continue
                values.add(value.encode("utf-8"))
    return sorted(values)


def hash_and_scan_file(
    path: Path,
    secret_values: list[bytes],
) -> tuple[str, int, Counter[str]]:
    digest = hashlib.sha256()
    exact_matches = 0
    pattern_matches: Counter[str] = Counter()
    overlap = max(
        [len(value) for value in secret_values]
        + [512]
    ) - 1
    tail = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            window = tail + chunk
            for value in secret_values:
                exact_matches += window.count(value) - tail.count(value)
            for label, pattern in CREDENTIAL_PATTERNS.items():
                hits = (
                    sum(1 for _ in pattern.finditer(window))
                    - sum(1 for _ in pattern.finditer(tail))
                )
                if hits:
                    pattern_matches[label] += hits
            tail = window[-overlap:] if overlap else b""
    return digest.hexdigest(), exact_matches, pattern_matches


def tree_digest(
    path: Path,
    *,
    scan_credentials: bool = False,
    secret_root: Path | None = None,
) -> dict[str, Any]:
    """Hash path + size + content digest for every safe regular file."""

    digest = hashlib.sha256()
    files = 0
    total_bytes = 0
    exact_secret_matches = 0
    credential_pattern_matches: Counter[str] = Counter()
    skipped_sensitive: list[str] = []
    skipped_links: list[str] = []
    secret_values = (
        load_local_secret_values(secret_root or ROOT) if scan_credentials else []
    )
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
        if scan_credentials:
            file_digest, exact_matches, pattern_matches = hash_and_scan_file(
                candidate,
                secret_values,
            )
            exact_secret_matches += exact_matches
            credential_pattern_matches.update(pattern_matches)
        else:
            file_digest = sha256_file(candidate)
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        digest.update(file_digest.encode("ascii"))
        digest.update(b"\n")
        files += 1
        total_bytes += size
    credential_scan = None
    if scan_credentials:
        credential_pattern_matches = +credential_pattern_matches
        if skipped_sensitive:
            scan_status = "fail_sensitive_env_file_present"
        elif not secret_values:
            scan_status = "incomplete_no_local_secret_values"
        elif exact_secret_matches:
            scan_status = "fail_exact_local_secret_match"
        elif credential_pattern_matches:
            scan_status = "pass_exact_secrets_with_heuristic_markers"
        else:
            scan_status = "pass_zero_matches"
        credential_scan = {
            "status": scan_status,
            "scanner_version": "hybridpatch.raw_credential_scan/1",
            "scanned_at": None,
            "files_scanned": files,
            "local_secret_value_count": len(secret_values),
            "exact_local_secret_match_count": exact_secret_matches,
            "heuristic_marker_match_count": sum(
                credential_pattern_matches.values()
            ),
            "heuristic_matches": dict(sorted(credential_pattern_matches.items())),
        }
    return {
        "algorithm": "sha256-tree-v1",
        "tree_sha256": digest.hexdigest(),
        "file_count": files,
        "size_bytes": total_bytes,
        "skipped_sensitive_files": skipped_sensitive,
        "skipped_symlinks": skipped_links,
        "credential_scan": credential_scan,
    }


def repo_path(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def sanitize_text(value: Any) -> Any:
    if value is None or not isinstance(value, str):
        return value
    text = value.replace(str(ROOT), "<workspace>").replace(ROOT.as_posix(), "<workspace>")
    text = re.sub(
        r"(?i)\b(OPENCODE_API_KEY|MINIMAX_API_KEY)\s*=\s*[^\s;]+",
        r"\1=<redacted>",
        text,
    )
    text = re.sub(r"(?i)\bkey=KEY_[A-Za-z0-9_-]+", "key=<key_label>", text)
    text = re.sub(r"(?i)Authorization\s*:\s*\S+", "Authorization:<redacted>", text)
    text = re.sub(r"(?i)Bearer\s+[A-Za-z0-9._~-]+", "Bearer <redacted>", text)
    text = re.sub(r"(?<![A-Za-z0-9])[A-Za-z]:[\\/][^\s\"']+", "<absolute-path>", text)
    return " ".join(text.split())


def sanitize_markdown(value: str) -> str:
    text = value.replace(str(ROOT), "<workspace>").replace(
        ROOT.as_posix(), "<workspace>"
    )
    for secret in load_local_secret_values():
        decoded = secret.decode("utf-8", errors="ignore")
        if decoded:
            text = text.replace(decoded, "<redacted-secret>")
    text = re.sub(
        r"(?i)\b(OPENCODE_API_KEY|MINIMAX_API_KEY|KEY_\d+)\s*=\s*[^\s`]+",
        r"\1=<redacted>",
        text,
    )
    text = re.sub(
        r"(?i)Authorization\s*:\s*[^\r\n]+",
        "Authorization: <redacted>",
        text,
    )
    text = re.sub(
        r"(?i)\bBearer\s+[A-Za-z0-9._~+/\-=]{16,}",
        "Bearer <redacted>",
        text,
    )
    text = re.sub(
        r"(?<![A-Za-z0-9])[A-Za-z]:[\\/][^\s`\"')]+",
        "<absolute-path>",
        text,
    )
    return text.rstrip() + "\n"


def normalize_command(command: str) -> str:
    command = sanitize_text(command)
    command = re.sub(
        r"(--sample)\s+.+?\s+(--methods)\b",
        r"\1 <samples> \2",
        command,
    )
    command = re.sub(r"key=<key_label>", "key=<key_label>", command)
    return command


def domain_of(sample_id: str) -> str:
    return "".join(character for character in sample_id if not character.isdigit())


def score_of(row: dict[str, Any]) -> float | None:
    evaluation = row.get("evaluation") or {}
    score = evaluation.get("score")
    if score is not None:
        return float(score)
    if evaluation.get("error") is not None:
        return 0.0
    return None


def finite_or_none(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def safe_mean(values: Iterable[float]) -> float | None:
    items = list(values)
    return statistics.mean(items) if items else None


def unique_values(records: list[dict[str, Any]], key: str) -> list[Any]:
    values: list[Any] = []
    seen: set[str] = set()
    for record in records:
        value = record.get(key)
        if value is None:
            continue
        marker = json.dumps(value, ensure_ascii=False, sort_keys=True)
        if marker in seen:
            continue
        seen.add(marker)
        values.append(value)
    return values


def resolve_metadata_code_provenance(
    archive: Path,
    metadata: list[dict[str, Any]],
) -> dict[str, Any]:
    """Preserve legacy identities and strictly validate recovery pairs."""
    fingerprints = unique_values(metadata, "code_fingerprint")
    commits = unique_values(metadata, "run_git_commit") or unique_values(
        metadata, "git_commit"
    )
    if len(commits) <= 1:
        return {
            "code_fingerprint": fingerprints[0] if len(fingerprints) == 1 else None,
            "code_fingerprints": fingerprints,
            "run_git_commit": commits[0] if commits else None,
            "run_git_commits": commits,
            "campaign_recovery_authorization": None,
            "provenance_warnings": [],
        }

    try:
        from process_experiment import code_provenance
    except ModuleNotFoundError:  # pragma: no cover - package-style import
        from tools.process_experiment import code_provenance
    return code_provenance(archive, metadata)


def invariant_fingerprint_value(
    fingerprints: list[dict[str, Any]],
    key: str,
) -> Any:
    values = [
        fingerprint.get(key)
        for fingerprint in fingerprints
        if isinstance(fingerprint, dict)
    ]
    if len(values) != len(fingerprints) or not values or any(
        value is None for value in values
    ):
        return None
    markers = {
        json.dumps(value, ensure_ascii=False, sort_keys=True)
        for value in values
    }
    return values[0] if len(markers) == 1 else None


def scalar_or_list(values: list[Any]) -> Any:
    if not values:
        return None
    return values[0] if len(values) == 1 else values


def load_catalog(path: Path) -> dict[str, Any]:
    catalog = read_json(path)
    if catalog.get("schema_version") != 1:
        raise RuntimeError(f"unsupported catalog schema: {catalog.get('schema_version')}")
    return catalog


def catalog_entries(catalog: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    entries: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for record_set in catalog.get("record_sets", []):
        for entry in record_set.get("experiments", []):
            entries.append((record_set, entry))
    return entries


def record_dir(owner: str, experiment_id: str) -> Path:
    return ROOT / owner / "records" / experiment_id


def load_standard_results(
    archive: Path, methods: list[str]
) -> dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]]:
    output: dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]] = {}
    for method in methods:
        method_dir = archive / method
        samples: dict[str, dict[tuple[int, str], dict[str, Any]]] = {}
        if method_dir.exists():
            for path in sorted(method_dir.glob("*.jsonl")):
                indexed: dict[tuple[int, str], dict[str, Any]] = {}
                for row in read_jsonl(path):
                    key = (
                        int(row.get("round_trip_num")),
                        str(row.get("round_trip_direction")),
                    )
                    if key in indexed:
                        raise RuntimeError(f"duplicate committed row {path}: {key}")
                    indexed[key] = row
                samples[path.stem] = indexed
        output[method] = samples
    return output


def load_manifest_view(
    manifest_path: Path,
) -> tuple[
    dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]],
    list[str],
]:
    samples: dict[str, dict[tuple[int, str], dict[str, Any]]] = {}
    source_experiments: set[str] = set()
    with manifest_path.open(encoding="utf-8", newline="") as handle:
        for item in csv.DictReader(handle):
            sample = item["sample"]
            source_path = ROOT / item["selected_result"]
            if not source_path.exists():
                raise RuntimeError(f"selected result missing: {source_path}")
            expected_digest = item.get("result_sha256")
            if expected_digest and sha256_file(source_path) != expected_digest:
                raise RuntimeError(f"selected result hash mismatch: {source_path}")
            indexed: dict[tuple[int, str], dict[str, Any]] = {}
            for row in read_jsonl(source_path):
                key = (
                    int(row.get("round_trip_num")),
                    str(row.get("round_trip_direction")),
                )
                if key in indexed:
                    raise RuntimeError(f"duplicate selected row {source_path}: {key}")
                indexed[key] = row
            samples[sample] = indexed
            for parent in source_path.parents:
                if parent.name.startswith("exp_"):
                    source_experiments.add(repo_path(parent))
                    break
    return {"fullrewrite": samples}, sorted(source_experiments)


def planned_samples(
    archive: Path,
    entry: dict[str, Any],
    results: dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]],
) -> list[str]:
    if entry.get("kind") == "manifest_view":
        return sorted(next(iter(results.values())))
    task_plans = sorted(path.name.removesuffix(".task_plan.json") for path in archive.glob("*.task_plan.json"))
    if task_plans:
        return task_plans
    return sorted(
        {
            sample
            for method_samples in results.values()
            for sample in method_samples
        }
    )


def canonical_samples(
    entry: dict[str, Any],
    planned: list[str],
    results: dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]],
    methods: list[str],
) -> tuple[list[str], dict[str, str]]:
    policy = entry.get("sample_policy")
    excluded = dict(entry.get("excluded_samples") or {})
    expected_rows = int(entry["round_trips"]) * 2
    result_samples = {
        sample
        for method_samples in results.values()
        for sample in method_samples
    }
    if policy == "manifest_selected":
        selected = set(planned)
    elif policy == "complete_all_methods":
        selected = {
            sample
            for sample in planned
            if all(
                len(results.get(method, {}).get(sample, {})) == expected_rows
                for method in methods
            )
        }
        for sample in planned:
            if sample not in selected:
                excluded.setdefault(
                    sample,
                    "Incomplete or missing result trajectory; source-only canonical scope requires a complete chain.",
                )
    elif policy == "exclude":
        selected = set(planned) - set(excluded)
    elif policy == "all_result_samples":
        selected = set(planned) & result_samples
        for sample in planned:
            if sample not in selected:
                excluded.setdefault(sample, "No committed result rows.")
    else:
        raise RuntimeError(f"unsupported sample policy {policy!r}")
    incomplete_selected = [
        (sample, method, len(results.get(method, {}).get(sample, {})))
        for sample in sorted(selected)
        for method in methods
        if len(results.get(method, {}).get(sample, {})) != expected_rows
    ]
    if policy in {"manifest_selected", "exclude"} and incomplete_selected:
        raise RuntimeError(
            f"{entry['experiment_id']}: canonical scope contains incomplete "
            f"trajectories: {incomplete_selected[:5]}"
        )
    if (
        policy == "complete_all_methods"
        and entry.get("lifecycle_status") == "complete"
        and len(selected) != len(planned)
    ):
        raise RuntimeError(
            f"{entry['experiment_id']}: complete experiment has "
            f"{len(planned) - len(selected)} incomplete planned samples"
        )
    return sorted(selected), excluded


def result_protocol_revisions(
    results: dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]]
) -> list[str]:
    revisions: set[str] = set()
    for method_samples in results.values():
        for rows in method_samples.values():
            for row in rows.values():
                hybrid = ((row.get("bdpatch") or {}).get("hybrid") or {})
                revision = hybrid.get("protocol_rev") or hybrid.get("protocol_version")
                route = hybrid.get("route") or hybrid.get("route_share_key")
                if revision and route:
                    revisions.add(str(revision))
    return sorted(revisions)


def critical_keys(
    results: dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]],
    theta: float = 0.10,
) -> set[tuple[str, str, int, str]]:
    critical: set[tuple[str, str, int, str]] = set()
    for method, method_samples in results.items():
        for sample, rows in method_samples.items():
            scores = {
                rt: score_of(row)
                for (rt, direction), row in rows.items()
                if direction == "backward" and score_of(row) is not None
            }
            for previous, current in zip(sorted(scores), sorted(scores)[1:]):
                before, after = scores[previous], scores[current]
                if before is None or after is None:
                    continue
                if (before > 0 and after <= 1e-9) or before - after >= theta:
                    critical.add((sample, method, current, "backward"))
    return critical


def classify_row(
    row: dict[str, Any] | None,
    *,
    missing_reason: str = "",
    critical: bool = False,
) -> dict[str, Any]:
    if row is None:
        failure_label = (
            "evaluator_incomplete"
            if "evaluator_incomplete" in missing_reason.lower()
            else "infrastructure_incomplete"
        )
        return {
            "execution_route": "none",
            "execution_status": "missing",
            "commit_outcome": "missing",
            "failure_stage": FAILURE_STAGE[failure_label],
            "failure_label": failure_label,
            "reason_summary": sanitize_text(missing_reason or "No committed row exists."),
        }

    bdpatch = row.get("bdpatch") or {}
    hybrid = bdpatch.get("hybrid") or {}
    execution_log = bdpatch.get("exec_log") or {}
    evaluation = row.get("evaluation") or {}
    actual_method = str(bdpatch.get("actual_method") or "")
    classification = (
        row.get("response_classification")
        or (row.get("response_classifications") or [None])[-1]
    )
    raw = row.get("raw_llm_response")
    raw_bytes = len(raw.encode("utf-8")) if isinstance(raw, str) else None
    route = (
        hybrid.get("route")
        or hybrid.get("route_share_key")
        or actual_method
    )
    if route == "full_rewrite":
        route = "fullrewrite"
    if not route:
        route = "fullrewrite" if row.get("method") == "fullrewrite" else "unknown"

    label = "none"
    reason_parts: list[str] = []
    classification_text = str(classification or "").lower()
    finish_reason = str(row.get("finish_reason") or "").lower()
    error_type = str(row.get("error_type") or "").lower()

    if any(token in classification_text for token in ("provider/api failure", "incomplete_stream")):
        label = "incomplete_stream"
    elif row.get("api_timeout_hit") or "timeout" in error_type:
        label = "timeout"
    elif "rate" in classification_text or row.get("api_rate_limit_wait_count"):
        label = "rate_limit"
    elif raw_bytes == 0 or classification_text in {
        "model_empty",
        "thinking_budget_exhausted",
    }:
        label = (
            "thinking_budget_exhausted"
            if classification_text == "thinking_budget_exhausted"
            else "empty_response"
        )
    elif raw_bytes is not None and 0 < raw_bytes < 200:
        label = "near_empty_response"
    elif classification_text in {"text_truncated", "aborted_partial_text"} or finish_reason in {
        "length",
        "max_tokens",
        "abort",
    }:
        label = "text_truncated"
    elif "refusal" in classification_text:
        label = "refusal"
    elif hybrid.get("invalid_json"):
        label = "invalid_json"
    elif hybrid.get("schema_error_count"):
        label = "schema_error"
    elif hybrid.get("route_violations"):
        label = "route_violation"
    elif hybrid.get("validation_gate_errors"):
        errors = [str(item) for item in hybrid.get("validation_gate_errors") or []]
        label = (
            "format_regression"
            if any("format_regression" in item for item in errors)
            else "validation_gate"
        )
        reason_parts.extend(errors)
    elif execution_log.get("ops_rejected") or execution_log.get("reject_reasons"):
        label = "op_rejected"
    elif hybrid.get("failure_reason"):
        label = "unknown_failure"

    evaluation_error = evaluation.get("error")
    if label == "none" and evaluation_error:
        error_text = str(evaluation_error)
        if "No module named" in error_text or "dependency" in error_text.lower():
            label = "dependency_missing"
        elif "context_mismatch" in error_text or "mismatch" in error_text.lower():
            label = "content_regression"
        else:
            label = "evaluator_error"
        reason_parts.append(error_text)
    if label == "none" and critical:
        label = "content_regression"
        reason_parts.append("Backward RS crossed the frozen critical-failure threshold.")

    partial = bool(hybrid.get("partial_acceptance"))
    kept = bool(
        hybrid.get("failed_step_kept_context")
        or "kept_context" in actual_method
    )
    noop = bool(bdpatch.get("noop_forward") or execution_log.get("noop"))
    if partial:
        status = "degraded"
        outcome = "partial_applied"
        if label == "none":
            label = "op_rejected"
    elif kept:
        status = "degraded"
        outcome = "kept_context"
    elif noop:
        status = "degraded" if label != "none" else "success"
        outcome = "unchanged"
    elif label == "none":
        status = "success"
        outcome = "applied" if row.get("method") == "hybridpatch" else "not_applicable"
    else:
        status = "failure" if evaluation_error else "degraded"
        if row.get("method") == "fullrewrite":
            outcome = "not_applicable"
        elif actual_method == "hybridpatch" or (
            route in {"local_patch", "bulk_patch", "dsl_rules", "bounded_rewrite"}
            and not hybrid.get("failure_reason")
        ):
            outcome = "applied"
        else:
            outcome = "kept_context"

    if classification:
        reason_parts.append(f"response_classification={classification}")
    if hybrid.get("failure_reason"):
        reason_parts.append(f"failure_reason={hybrid.get('failure_reason')}")
    reject_reasons = execution_log.get("reject_reasons") or []
    if reject_reasons:
        reason_parts.append(f"reject_reasons={reject_reasons}")

    return {
        "execution_route": route,
        "execution_status": status,
        "commit_outcome": outcome,
        "failure_stage": FAILURE_STAGE.get(label, "unknown"),
        "failure_label": label,
        "reason_summary": sanitize_text("; ".join(reason_parts))[:500],
    }


def input_tokens(row: dict[str, Any]) -> int | None:
    value = row.get("input_tokens")
    if value is None:
        value = row.get("prompt_tokens")
    return int(value) if value is not None else None


def output_tokens(row: dict[str, Any]) -> int | None:
    value = row.get("output_tokens")
    if value is None:
        value = row.get("completion_tokens")
    return int(value) if value is not None else None


def total_tokens(row: dict[str, Any]) -> int | None:
    value = row.get("total_tokens")
    if value is not None:
        return int(value)
    left, right = input_tokens(row), output_tokens(row)
    return left + right if left is not None and right is not None else None


def latency_ms(row: dict[str, Any]) -> int | None:
    value = row.get("latency")
    return round(float(value) * 1000) if value is not None else None


def preservation_violations(row: dict[str, Any]) -> int | None:
    bdpatch = row.get("bdpatch") or {}
    execution_log = bdpatch.get("exec_log") or {}
    value = execution_log.get("preservation_violations")
    return int(value) if value is not None else None


def row_grid(
    entry: dict[str, Any],
    planned: list[str],
    canonical: set[str],
    excluded: dict[str, str],
    methods: list[str],
    results: dict[str, dict[str, dict[tuple[int, str], dict[str, Any]]]],
    artifact_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    critical = critical_keys(results)
    rows: list[dict[str, Any]] = []
    cases: list[dict[str, Any]] = []
    for sample in planned:
        for method in methods:
            indexed = results.get(method, {}).get(sample, {})
            for round_trip in range(1, int(entry["round_trips"]) + 1):
                for direction in ("forward", "backward"):
                    source = indexed.get((round_trip, direction))
                    included = sample in canonical and source is not None
                    exclusion_reason = ""
                    if sample not in canonical:
                        exclusion_reason = excluded.get(sample, "Outside canonical scope.")
                    elif source is None:
                        exclusion_reason = "Missing committed row."
                    is_critical = (sample, method, round_trip, direction) in critical
                    classification = classify_row(
                        source,
                        missing_reason=exclusion_reason,
                        critical=is_critical,
                    )
                    call_ids = []
                    if source is not None:
                        call_ids = [
                            str(value)
                            for value in (source.get("api_call_ids") or [])
                            if value
                        ]
                    raw_ref = (
                        f"{artifact_id}#{method}/{sample}/"
                        f"rt{round_trip:02d}/{direction}"
                    )
                    score = score_of(source) if source is not None else None
                    record = {
                        "experiment_id": entry["experiment_id"],
                        "sample_id": sample,
                        "domain": domain_of(sample),
                        "method": method,
                        "round_trip": round_trip,
                        "direction": direction,
                        "canonical_included": str(bool(included)).lower(),
                        "exclusion_reason": exclusion_reason,
                        "execution_route": classification["execution_route"],
                        "execution_status": classification["execution_status"],
                        "commit_outcome": classification["commit_outcome"],
                        "preservation_violations": (
                            preservation_violations(source) if source else None
                        ),
                        "rs": finite_or_none(score),
                        "critical_failure": (
                            str(bool(is_critical)).lower()
                            if direction == "backward" and source is not None
                            else ""
                        ),
                        "input_tokens": input_tokens(source) if source else None,
                        "output_tokens": output_tokens(source) if source else None,
                        "total_tokens": total_tokens(source) if source else None,
                        "latency_ms": latency_ms(source) if source else None,
                        "failure_stage": classification["failure_stage"],
                        "failure_label": classification["failure_label"],
                        "raw_artifact_id": artifact_id,
                        "raw_call_ids": ";".join(call_ids),
                        "raw_ref": raw_ref,
                    }
                    rows.append(record)
                    if classification["failure_label"] != "none":
                        cases.append(
                            {
                                "schema": "hybridpatch.failure_case/1",
                                "case_id": (
                                    f"{entry['experiment_id']}:{sample}:{method}:"
                                    f"rt{round_trip:02d}:{direction}"
                                ),
                                "experiment_id": entry["experiment_id"],
                                "sample_id": sample,
                                "method": method,
                                "round_trip": round_trip,
                                "direction": direction,
                                "canonical_included": included,
                                "severity": (
                                    "high"
                                    if is_critical
                                    or classification["execution_status"] in {"failure", "missing"}
                                    else "medium"
                                ),
                                "failure_stage": classification["failure_stage"],
                                "failure_label": classification["failure_label"],
                                "selection_reason": (
                                    "canonical exclusion"
                                    if not included
                                    else "representative failure label"
                                ),
                                "reason_summary": classification["reason_summary"],
                                "evidence": {
                                    "rs": finite_or_none(score),
                                    "critical_failure": is_critical if direction == "backward" else None,
                                    "raw_artifact_id": artifact_id,
                                    "raw_call_ids": call_ids,
                                    "raw_ref": raw_ref,
                                },
                            }
                        )
    return rows, cases


def committed_rows(
    sample_rows: list[dict[str, Any]],
    *,
    canonical_only: bool = False,
) -> list[dict[str, Any]]:
    return [
        row
        for row in sample_rows
        if row["execution_status"] != "missing"
        and (not canonical_only or row["canonical_included"] == "true")
    ]


def method_summary(rows: list[dict[str, Any]], round_trips: int) -> dict[str, Any]:
    backward = [row for row in rows if row["direction"] == "backward"]
    scores = [float(row["rs"]) for row in backward if row["rs"] not in (None, "")]
    by_rt: dict[int, list[float]] = defaultdict(list)
    for row in backward:
        if row["rs"] not in (None, ""):
            by_rt[int(row["round_trip"])].append(float(row["rs"]))
    selected_k = sorted({1, min(5, round_trips), round_trips})
    latency_values = [
        int(row["latency_ms"]) for row in rows if row["latency_ms"] not in (None, "")
    ]
    tokens_in = sum(int(row["input_tokens"] or 0) for row in rows)
    tokens_out = sum(int(row["output_tokens"] or 0) for row in rows)
    tokens_total = sum(int(row["total_tokens"] or 0) for row in rows)
    preservation_values = [
        int(row["preservation_violations"])
        for row in rows
        if row["preservation_violations"] not in (None, "")
    ]
    return {
        "committed_rows": len(rows),
        "backward_rows": len(backward),
        "mean_backward_rs": finite_or_none(safe_mean(scores)),
        "rs_at_k": {
            str(key): finite_or_none(safe_mean(by_rt.get(key, [])))
            for key in selected_k
        },
        "critical_failures": {
            "count": sum(row["critical_failure"] == "true" for row in backward),
            "transitions": sum(
                1
                for row in backward
                if int(row["round_trip"]) > 1 and row["rs"] not in (None, "")
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
            "count": len(latency_values),
            "mean": round(statistics.mean(latency_values), 3) if latency_values else None,
            "median": round(statistics.median(latency_values), 3) if latency_values else None,
            "max": max(latency_values) if latency_values else None,
        },
        "execution_status": dict(Counter(row["execution_status"] for row in rows)),
        "commit_outcomes": dict(Counter(row["commit_outcome"] for row in rows)),
        "preservation_violations": {
            "recorded_rows": len(preservation_values),
            "unknown_rows": len(rows) - len(preservation_values),
            "total": sum(preservation_values),
            "max": max(preservation_values) if preservation_values else None,
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
) -> dict[str, Any]:
    index: dict[str, dict[tuple[str, int], float]] = {
        method_a: {},
        method_b: {},
    }
    for row in rows:
        if row["direction"] != "backward" or row["rs"] in (None, ""):
            continue
        if row["method"] not in index:
            continue
        index[row["method"]][(row["sample_id"], int(row["round_trip"]))] = float(
            row["rs"]
        )
    keys = sorted(set(index[method_a]) & set(index[method_b]))
    values_a = [index[method_a][key] for key in keys]
    values_b = [index[method_b][key] for key in keys]
    differences = [left - right for left, right in zip(values_a, values_b)]
    t_statistic = p_value = None
    if len(values_a) >= 2:
        try:
            from scipy.stats import ttest_rel

            result = ttest_rel(values_a, values_b)
            t_statistic = finite_or_none(float(result.statistic))
            p_value = finite_or_none(float(result.pvalue))
        except Exception:
            pass
    sample_sd = statistics.stdev(differences) if len(differences) > 1 else 0.0
    delta = safe_mean(differences)
    return {
        "scope_id": scope_id,
        "headline": headline,
        "method_a": method_a,
        "method_b": method_b,
        "paired_backward_rows": len(keys),
        "mean_a": finite_or_none(safe_mean(values_a)),
        "mean_b": finite_or_none(safe_mean(values_b)),
        "delta_a_minus_b": finite_or_none(delta),
        "t": t_statistic,
        "p": p_value,
        "cohen_d": (
            finite_or_none(delta / sample_sd)
            if delta is not None and sample_sd > 0
            else 0.0
        ),
    }


def lifecycle_status(entry: dict[str, Any]) -> str:
    status = entry.get("lifecycle_status")
    if status not in {"complete", "incomplete", "failed_informative", "superseded"}:
        raise RuntimeError(
            f"{entry['experiment_id']}: invalid lifecycle_status {status!r}"
        )
    return status


def evidence_role(entry: dict[str, Any]) -> str:
    role = entry["evidence_role"]
    return {
        "canonical": "baseline" if entry["research_stage"] == "baseline" else "primary",
        "historical": "supporting",
        "diagnostic": (
            "transport" if "transport" in entry["research_stage"] else "diagnostic"
        ),
        "source_only": "supporting",
        "sensitivity": "sensitivity",
    }[role]


def claim_eligibility(entry: dict[str, Any]) -> str:
    if entry["status"] == "canonical" and entry["evidence_role"] == "canonical":
        return "canonical"
    if entry["evidence_role"] in {"historical", "sensitivity"}:
        return "supporting"
    if entry["evidence_role"] == "diagnostic":
        return "diagnostic_only"
    return "none"


def report_records(entry: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for report in entry.get("source_reports", []):
        path = ROOT / report["path"]
        if not path.is_file():
            raise RuntimeError(
                f"{entry['experiment_id']}: source report missing: {report['path']}"
            )
        item = dict(report)
        item["sha256"] = sha256_file(path)
        item["size_bytes"] = path.stat().st_size
        output.append(item)
    return output


def canonical_scope_id(entry: dict[str, Any], canonical_count: int) -> str:
    scope_id = entry.get("scope_id")
    if not scope_id:
        raise RuntimeError(
            f"{entry['experiment_id']}: scope_id is required "
            f"for the {canonical_count}-sample record"
        )
    return str(scope_id)


def summary_document(
    entry: dict[str, Any],
    planned: list[str],
    canonical: list[str],
    excluded: dict[str, str],
    sample_rows: list[dict[str, Any]],
    methods: list[str],
    verification: dict[str, Any],
    scope_id: str,
) -> dict[str, Any]:
    canonical_rows = committed_rows(sample_rows, canonical_only=True)
    all_rows = committed_rows(sample_rows)
    methods_summary = {
        method: method_summary(
            [row for row in canonical_rows if row["method"] == method],
            int(entry["round_trips"]),
        )
        for method in methods
    }
    comparisons = []
    if "hybridpatch" in methods and "fullrewrite" in methods:
        comparisons.append(
            paired_summary(
                canonical_rows,
                "hybridpatch",
                "fullrewrite",
                scope_id=scope_id,
                headline=claim_eligibility(entry) == "canonical",
            )
        )
    return {
        "schema": "hybridpatch.experiment_summary/1",
        "experiment_id": entry["experiment_id"],
        "canonical_scope_id": scope_id,
        "score_policy": {
            "committed_method_failure": "stored evaluation score; defined reconstruction error is zero",
            "infrastructure_missing": "null and excluded from canonical statistics",
        },
        "metric_definitions": {
            "rs_range": [0.0, 1.0],
            "critical_failure_theta": 0.10,
            "critical_failure_definition": "backward RS drop >= theta or collapse to zero",
        },
        "scope": {
            "planned_samples": len(planned),
            "canonical_samples": len(canonical),
            "planned_rows": len(planned)
            * len(methods)
            * int(entry["round_trips"])
            * 2,
            "committed_rows": len(all_rows),
            "canonical_committed_rows": len(canonical_rows),
            "canonical_backward_rows": sum(
                row["direction"] == "backward" for row in canonical_rows
            ),
            "excluded_samples": [
                {"sample_id": sample, "reason": reason}
                for sample, reason in sorted(excluded.items())
            ],
        },
        "methods": methods_summary,
        "comparisons": comparisons,
        "paper_reporting": entry.get("paper_reporting"),
        "verification": verification,
        "source_files": [
            entry["archive_path"],
            *[report["path"] for report in entry.get("source_reports", [])],
        ],
    }


def failure_stats_document(
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    failures = [row for row in rows if row["failure_label"] != "none"]
    by_label: dict[str, dict[str, Any]] = {}
    for label, label_rows in _group(failures, "failure_label").items():
        by_label[label] = {
            "row_count": len(label_rows),
            "sample_method_count": len(
                {(row["sample_id"], row["method"]) for row in label_rows}
            ),
            "canonical_row_count": sum(
                row["canonical_included"] == "true" for row in label_rows
            ),
        }
    return {
        "schema": "hybridpatch.failure_stats/1",
        "experiment_id": entry["experiment_id"],
        "primary_label_policy": "deterministic_root_cause_v1",
        "total_planned_rows": len(rows),
        "committed_rows": sum(row["execution_status"] != "missing" for row in rows),
        "non_none_failure_rows": len(failures),
        "affected_sample_methods": len(
            {(row["sample_id"], row["method"]) for row in failures}
        ),
        "by_stage": dict(Counter(row["failure_stage"] for row in failures)),
        "by_label": by_label,
        "by_method": {
            method: dict(Counter(row["failure_label"] for row in method_rows))
            for method, method_rows in _group(failures, "method").items()
        },
        "canonical_exclusions": dict(
            Counter(
                row["failure_label"]
                for row in rows
                if row["canonical_included"] == "false"
                and row["failure_label"] != "none"
            )
        ),
        "critical_failures": {
            "count": sum(row["critical_failure"] == "true" for row in rows),
            "by_method": dict(
                Counter(
                    row["method"]
                    for row in rows
                    if row["critical_failure"] == "true"
                )
            ),
        },
    }


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[key])].append(row)
    return dict(grouped)


def representative_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: Counter[str] = Counter()
    for case in sorted(
        cases,
        key=lambda item: (
            item["failure_label"],
            not item["canonical_included"],
            item["sample_id"],
            item["method"],
            item["round_trip"],
            item["direction"],
        ),
    ):
        label = case["failure_label"]
        limit = 3 if case["selection_reason"] == "canonical exclusion" else 2
        if seen[label] >= limit:
            continue
        seen[label] += 1
        selected.append(case)
    return selected


def _case_method(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rs": finite_or_none(row["rs"]),
        "route": row["execution_route"],
        "status": row["execution_status"],
        "commit_outcome": row["commit_outcome"],
        "failure_label": row["failure_label"],
        "raw_ref": row["raw_ref"],
    }


def build_casebook(
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
    methods: list[str],
) -> list[dict[str, Any]]:
    indexed = {
        (
            row["sample_id"],
            row["method"],
            int(row["round_trip"]),
            row["direction"],
        ): row
        for row in rows
    }
    cases: list[dict[str, Any]] = []
    selected_steps: set[tuple[str, int, str]] = set()
    selected_events: set[tuple[str, str, int, str]] = set()

    def add_case(
        *,
        sample_id: str,
        round_trip: int,
        direction: str,
        selected_methods: list[str],
        case_kind: str,
        categories: list[str],
        selection_reason: str,
    ) -> None:
        available = {
            method: indexed[(sample_id, method, round_trip, direction)]
            for method in selected_methods
            if (sample_id, method, round_trip, direction) in indexed
        }
        if not available:
            return
        delta = None
        if {"hybridpatch", "fullrewrite"} <= set(available):
            hp_score = available["hybridpatch"]["rs"]
            fr_score = available["fullrewrite"]["rs"]
            if hp_score not in (None, "") and fr_score not in (None, ""):
                delta = float(hp_score) - float(fr_score)
        category_slug = re.sub(r"[^a-z0-9]+", "-", categories[0].lower()).strip("-")
        method_slug = "paired" if len(available) > 1 else next(iter(available))
        cases.append(
            {
                "schema": "hybridpatch.casebook_case/1",
                "case_id": (
                    f"{entry['experiment_id']}:{sample_id}:rt{round_trip:02d}:"
                    f"{direction}:{method_slug}:{category_slug}"
                ),
                "experiment_id": entry["experiment_id"],
                "case_kind": case_kind,
                "categories": categories,
                "selection_reason": selection_reason,
                "sample_id": sample_id,
                "round_trip": round_trip,
                "direction": direction,
                "methods": {
                    method: _case_method(row)
                    for method, row in sorted(available.items())
                },
                "score_delta_hybridpatch_minus_fullrewrite": finite_or_none(delta),
            }
        )

    if {"hybridpatch", "fullrewrite"} <= set(methods):
        paired: list[tuple[float, str, int]] = []
        sample_ids = sorted({row["sample_id"] for row in rows})
        for sample_id in sample_ids:
            for round_trip in range(1, int(entry["round_trips"]) + 1):
                hp = indexed.get((sample_id, "hybridpatch", round_trip, "backward"))
                fr = indexed.get((sample_id, "fullrewrite", round_trip, "backward"))
                if not hp or not fr:
                    continue
                if (
                    hp["canonical_included"] != "true"
                    or fr["canonical_included"] != "true"
                    or hp["rs"] in (None, "")
                    or fr["rs"] in (None, "")
                ):
                    continue
                paired.append((float(hp["rs"]) - float(fr["rs"]), sample_id, round_trip))

        paired_groups = [
            (
                "hybridpatch_win",
                "Largest canonical HybridPatch advantages.",
                sorted((item for item in paired if item[0] > 0), reverse=True)[:3],
            ),
            (
                "fullrewrite_win",
                "Largest canonical FullRewrite advantages.",
                sorted((item for item in paired if item[0] < 0))[:3],
            ),
            (
                "near_tie",
                "Closest canonical paired outcomes.",
                sorted(paired, key=lambda item: (abs(item[0]), item[1], item[2]))[:2],
            ),
        ]
        for category, reason, candidates in paired_groups:
            for delta, sample_id, round_trip in candidates:
                step = (sample_id, round_trip, "backward")
                if step in selected_steps:
                    continue
                selected_steps.add(step)
                add_case(
                    sample_id=sample_id,
                    round_trip=round_trip,
                    direction="backward",
                    selected_methods=["hybridpatch", "fullrewrite"],
                    case_kind="paired_comparison",
                    categories=[category],
                    selection_reason=f"{reason} Paired delta={delta:+.6f}.",
                )
    else:
        method = methods[0]
        scored = [
            row
            for row in rows
            if row["method"] == method
            and row["direction"] == "backward"
            and row["canonical_included"] == "true"
            and row["rs"] not in (None, "")
        ]
        groups = [
            (
                "high_score",
                "Highest canonical backward scores.",
                sorted(
                    scored,
                    key=lambda row: (
                        -float(row["rs"]),
                        row["sample_id"],
                        int(row["round_trip"]),
                    ),
                )[:3],
            ),
            (
                "low_score",
                "Lowest canonical backward scores.",
                sorted(
                    scored,
                    key=lambda row: (
                        float(row["rs"]),
                        row["sample_id"],
                        int(row["round_trip"]),
                    ),
                )[:3],
            ),
        ]
        for category, reason, candidates in groups:
            for row in candidates:
                event = (
                    row["sample_id"],
                    method,
                    int(row["round_trip"]),
                    "backward",
                )
                if event in selected_events:
                    continue
                selected_events.add(event)
                add_case(
                    sample_id=row["sample_id"],
                    round_trip=int(row["round_trip"]),
                    direction="backward",
                    selected_methods=[method],
                    case_kind="single_method_event",
                    categories=[category],
                    selection_reason=reason,
                )

    event_groups = [
        (
            "partial_acceptance",
            lambda row: row["commit_outcome"] == "partial_applied",
            3,
            "Representative partial-acceptance event.",
        ),
        (
            "kept_context",
            lambda row: row["commit_outcome"] == "kept_context",
            3,
            "Representative kept-context event.",
        ),
        (
            "critical_failure",
            lambda row: row["critical_failure"] == "true",
            3,
            "Representative critical backward-score transition.",
        ),
        (
            "failure_event",
            lambda row: (
                row["failure_label"] != "none"
                and row["canonical_included"] == "true"
            ),
            3,
            "Representative canonical failure label.",
        ),
        (
            "canonical_exclusion",
            lambda row: (
                row["canonical_included"] == "false"
                and (
                    row["execution_status"] == "missing"
                    or bool(row["exclusion_reason"])
                )
            ),
            4,
            "Representative excluded or missing planned event.",
        ),
    ]
    for category, predicate, limit, reason in event_groups:
        candidates = sorted(
            (row for row in rows if predicate(row)),
            key=lambda row: (
                row["direction"] != "backward",
                row["sample_id"],
                row["method"],
                int(row["round_trip"]),
            ),
        )
        added = 0
        for row in candidates:
            event = (
                row["sample_id"],
                row["method"],
                int(row["round_trip"]),
                row["direction"],
            )
            if event in selected_events:
                continue
            selected_events.add(event)
            added += 1
            categories = [category]
            if row["failure_label"] != "none":
                categories.append(f"failure:{row['failure_label']}")
            add_case(
                sample_id=row["sample_id"],
                round_trip=int(row["round_trip"]),
                direction=row["direction"],
                selected_methods=[row["method"]],
                case_kind="single_method_event",
                categories=categories,
                selection_reason=reason,
            )
            if added >= limit:
                break

    return cases[:24]


def _format_number(value: Any, digits: int = 3, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    return f"{number:+.{digits}f}" if signed else f"{number:.{digits}f}"


def _md_cell(value: Any) -> str:
    if value is None:
        return "n/a"
    return sanitize_text(str(value)).replace("|", r"\|")


def _dict_counts(value: dict[str, Any]) -> str:
    if not value:
        return "none"
    return ", ".join(f"{key}={count}" for key, count in sorted(value.items()))


def _source_snapshot_block(source: str) -> str:
    longest = max((len(match.group(0)) for match in re.finditer(r"`+", source)), default=0)
    fence = "`" * max(4, longest + 1)
    return f"{fence}text\n{source.rstrip()}\n{fence}\n"


def report_markdown(
    entry: dict[str, Any],
    experiment_document: dict[str, Any],
    summary: dict[str, Any],
    failures: dict[str, Any],
    casebook: list[dict[str, Any]],
    *,
    source_report_link: str | None,
    source_report_snapshot: str | None,
) -> str:
    scope = summary["scope"]
    claim = experiment_document["claim_eligibility"]
    lines = [
        f"# {entry['experiment_id']} experiment report",
        "",
        "> This is the canonical human-readable Git record for this experiment. "
        "The authoritative machine scope is [`summary.json`](summary.json).",
        "",
        "## Status and scope",
        "",
        f"- Purpose: {_md_cell(entry['purpose'])}",
        f"- Lifecycle: `{experiment_document['lifecycle_status']}`",
        f"- Evidence role: `{experiment_document['evidence_role']}`",
        f"- Claim eligibility: `{claim}`",
        f"- Canonical scope: `{summary['canonical_scope_id']}` "
        f"({scope['canonical_samples']}/{scope['planned_samples']} samples)",
        f"- Canonical backward rows: {scope['canonical_backward_rows']}",
        f"- Comparison policy: {_md_cell(entry.get('comparison_policy'))}",
        "",
        "## Run identity",
        "",
        f"- Code reference: `{_md_cell(entry.get('code_reference'))}`",
        f"- Protocol: `{_md_cell(entry.get('protocol_revision'))}`",
        f"- Dataset / split: `{_md_cell(entry.get('dataset'))}` / "
        f"`{_md_cell(entry.get('split'))}`",
        f"- Round trips: {entry['round_trips']}",
        f"- Model: `{_md_cell(experiment_document['model'].get('name'))}`",
        f"- Transport revision: "
        f"`{_md_cell(experiment_document['model'].get('transport_revision'))}`",
        f"- Seed: `{_md_cell(experiment_document['execution'].get('seed'))}`",
        f"- Started: `{_md_cell(experiment_document['time'].get('started_at_local'))}`",
        f"- Finished: `{_md_cell(experiment_document['time'].get('finished_at'))}`",
        f"- Run Git commit: `{_md_cell(experiment_document['code'].get('run_git_commit'))}`",
        f"- Archived fingerprint count: "
        f"{experiment_document['code'].get('fingerprint_count')}",
        "",
        "### Recorded command templates",
        "",
    ]
    commands = experiment_document["execution"].get("command_templates") or []
    if commands:
        lines.append(_source_snapshot_block("\n".join(commands)).rstrip())
    else:
        lines.append("No command template was recorded for this historical experiment.")
    lines.extend(
        [
            "",
            "The complete planned and canonical sample identifiers are in "
            "[`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).",
            "",
            "## Canonical results",
            "",
            "| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for method, values in summary["methods"].items():
        rs_at_k = values.get("rs_at_k") or {}
        lines.append(
            "| "
            f"`{method}` | {_format_number(values.get('mean_backward_rs'))} | "
            f"{_format_number(rs_at_k.get('1'))} | "
            f"{_format_number(rs_at_k.get('5'))} | "
            f"{_format_number(rs_at_k.get('10'))} | "
            f"{values['critical_failures']['count']} | "
            f"{values['tokens']['total']} | "
            f"{_dict_counts(values.get('commit_outcomes') or {})} |"
        )
    lines.extend(["", "### Paired comparisons", ""])
    comparisons = summary.get("comparisons") or []
    if comparisons:
        lines.extend(
            [
                "| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |",
                "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for comparison in comparisons:
            lines.append(
                "| "
                f"`{comparison['scope_id']}` | `{comparison['method_a']}` | "
                f"`{comparison['method_b']}` | {comparison['paired_backward_rows']} | "
                f"{_format_number(comparison.get('mean_a'))} | "
                f"{_format_number(comparison.get('mean_b'))} | "
                f"{_format_number(comparison.get('delta_a_minus_b'), signed=True)} | "
                f"{_format_number(comparison.get('p'))} | "
                f"{_format_number(comparison.get('cohen_d'))} | "
                f"{'yes' if comparison.get('headline') else 'no'} |"
            )
    else:
        lines.append("No paired comparison is defined for this experiment.")

    paper_reporting = experiment_document.get("paper_reporting")
    if paper_reporting:
        lines.extend(
            [
                "",
                "## Paper protocol reporting",
                "",
                f"- Protocol condition: {_md_cell(paper_reporting.get('protocol_condition'))}",
                f"- Reporting status: `{_md_cell(paper_reporting.get('status'))}`",
                f"- Scope: `{_md_cell(paper_reporting.get('scope_id'))}`",
                f"- Official FullRewrite replacements in this scope: "
                f"{paper_reporting.get('official_replacements_in_scope')}",
                "",
                "| Paired rows | HybridPatch | FR+Official | Δ HP−FR | p | Cohen d |",
                "|---:|---:|---:|---:|---:|---:|",
                "| "
                f"{paper_reporting.get('paired_backward_rows')} | "
                f"{_format_number(paper_reporting.get('hybridpatch_mean'), 6)} | "
                f"{_format_number(paper_reporting.get('fullrewrite_mean'), 6)} | "
                f"{_format_number(paper_reporting.get('delta_hybridpatch_minus_fullrewrite'), 6, signed=True)} | "
                f"{paper_reporting.get('p'):.3e} | "
                f"{_format_number(paper_reporting.get('cohen_d'), 3)} |",
                "",
                f"**Paper interpretation:** "
                f"{_md_cell(paper_reporting.get('interpretation'))}",
                "",
                f"**Required validity disclosure:** "
                f"{_md_cell(paper_reporting.get('validity_caveat'))}",
                "",
                f"Sources: `{_md_cell(paper_reporting.get('source_ref'))}` and "
                f"`{_md_cell(paper_reporting.get('machine_source_ref'))}`.",
            ]
        )

    lines.extend(["", "## Analysis for the next iteration", ""])
    if comparisons:
        comparison = comparisons[0]
        delta = comparison.get("delta_a_minus_b")
        p_value = comparison.get("p")
        if claim == "canonical":
            if delta is not None and delta > 0 and p_value is not None and p_value < 0.05:
                interpretation = (
                    "Within this exact canonical scope, the paired result supports a "
                    "HybridPatch advantage."
                )
            elif delta is not None and delta > 0:
                interpretation = (
                    "The point estimate favors HybridPatch, but this scope does not "
                    "establish a statistically distinguishable advantage."
                )
            elif delta is not None:
                interpretation = (
                    "The canonical point estimate does not favor HybridPatch in this scope."
                )
            else:
                interpretation = "The paired effect is unavailable."
        else:
            interpretation = (
                "This comparison is supporting or diagnostic evidence and must not be "
                "promoted to a standalone method claim."
            )
        lines.extend(
            [
                "1. **Observation:** "
                f"{comparison['method_a']} − {comparison['method_b']} = "
                f"{_format_number(delta, signed=True)} across "
                f"{comparison['paired_backward_rows']} paired backward rows "
                f"(p={_format_number(p_value)}).",
                f"   **Interpretation:** {interpretation}",
                "   **Implication:** Use this declared scope—not a larger partial directory "
                "or a more favorable sensitivity view—when stating the experiment result.",
                "   **Next step:** Inspect the paired win/loss cases below before proposing "
                "a method change; treat recurring mechanisms, not isolated scores, as the "
                "iteration target.",
            ]
        )
    else:
        method, values = next(iter(summary["methods"].items()))
        lines.extend(
            [
                "1. **Observation:** "
                f"`{method}` mean backward RS is "
                f"{_format_number(values.get('mean_backward_rs'))} over "
                f"{values.get('backward_rows')} canonical rows.",
                "   **Interpretation:** This is a single-arm, baseline, source, or "
                "diagnostic record; it does not identify a paired method effect.",
                "   **Implication:** Use it only for the evidence role declared above.",
                "   **Next step:** Compare it only through a catalogued derivation or a "
                "new controlled paired experiment.",
            ]
        )

    hp = summary["methods"].get("hybridpatch")
    fr = summary["methods"].get("fullrewrite")
    cost_observation = "No cross-method token ratio is defined."
    if hp and fr and fr["tokens"]["total"]:
        ratio = hp["tokens"]["total"] / fr["tokens"]["total"]
        cost_observation = (
            f"HybridPatch recorded token total is {ratio:.2f}× FullRewrite "
            f"({hp['tokens']['total']} vs {fr['tokens']['total']})."
        )
    lines.extend(
        [
            "",
            f"2. **Observation:** {cost_observation} "
            f"Failure labels: {_dict_counts(failures.get('by_stage') or {})}.",
            "   **Interpretation:** Quality, protocol, transport, evaluator, and "
            "infrastructure failures are separate mechanisms and should not be merged "
            "into one model-error bucket.",
            "   **Implication:** A next-version change should name the failure class it "
            "is expected to improve and preserve unaffected behavior.",
            "   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small "
            "counterfactual or smoke set before any full paid rerun.",
            "",
            "3. **Observation:** "
            f"Verification status is `{_md_cell(summary['verification'].get('status'))}`; "
            f"replayed backward rows="
            f"{_md_cell(summary['verification'].get('replayed_backward_rows'))}.",
            "   **Interpretation:** Verification evidence and its documented exceptions "
            "bound what can be independently checked from the preserved archive.",
            "   **Implication:** Missing commit IDs, timestamps, or independent replay "
            "must remain explicit provenance gaps.",
            "   **Next step:** For new experiments, finalize the record immediately after "
            "the verifier and analysis complete.",
            "",
            "## Representative casebook",
            "",
            "The casebook is selected deterministically from canonical paired score "
            "extremes plus partial acceptance, kept-context, critical failures, ordinary "
            "failure labels, and canonical exclusions. It contains no prompt or model "
            "response body.",
            "",
            "| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |",
            "|---|---|---|---|---:|",
        ]
    )
    for case in casebook[:16]:
        outcomes = "; ".join(
            f"{method}: RS={_format_number(values.get('rs'))}, "
            f"{values.get('commit_outcome')}, {values.get('failure_label')}"
            for method, values in case["methods"].items()
        )
        lines.append(
            "| "
            f"`{case['case_id']}` | {_md_cell(', '.join(case['categories']))} | "
            f"`{case['sample_id']}` / {case['round_trip']} / `{case['direction']}` | "
            f"{_md_cell(outcomes)} | "
            f"{_format_number(case.get('score_delta_hybridpatch_minus_fullrewrite'), signed=True)} |"
        )
    if not casebook:
        lines.append("| n/a | n/a | n/a | No representative rows available. | n/a |")

    lines.extend(
        [
            "",
            "## Failures and exclusions",
            "",
            f"- Non-none failure rows: {failures['non_none_failure_rows']}",
            f"- Affected sample-method pairs: {failures['affected_sample_methods']}",
            f"- Failure stages: {_dict_counts(failures.get('by_stage') or {})}",
            f"- Canonical exclusions: "
            f"{_dict_counts(failures.get('canonical_exclusions') or {})}",
        ]
    )
    for excluded in scope.get("excluded_samples") or []:
        lines.append(
            f"- Excluded `{_md_cell(excluded['sample_id'])}`: "
            f"{_md_cell(excluded['reason'])}"
        )

    lines.extend(["", "## Known exceptions and provenance gaps", ""])
    exceptions = experiment_document.get("known_exceptions") or []
    verification_exceptions = (
        experiment_document.get("verification", {}).get("exceptions") or []
    )
    if exceptions:
        for exception in exceptions:
            lines.append(f"- Known exception: {_md_cell(exception)}")
    if verification_exceptions:
        for exception in verification_exceptions:
            lines.append(f"- Verification exception: {_md_cell(exception)}")
    gaps = experiment_document.get("provenance_gaps") or []
    if gaps:
        for gap in gaps:
            lines.append(
                f"- Provenance gap `{_md_cell(gap.get('field'))}`: "
                f"{_md_cell(gap.get('reason'))} "
                f"Impact: {_md_cell(gap.get('impact'))}"
            )
    if not exceptions and not verification_exceptions and not gaps:
        lines.append("- None recorded.")

    lines.extend(
        [
            "",
            "## Audit and provenance",
            "",
            "- [`experiment.yaml`](experiment.yaml): run identity, configuration, "
            "scope policy, source reports, and known gaps.",
            "- [`summary.json`](summary.json): canonical machine metrics.",
            "- [`samples.csv`](samples.csv): complete planned row grid.",
            "- [`casebook.jsonl`](casebook.jsonl): representative comparison and "
            "failure cases.",
            "- [`failure_stats.json`](failure_stats.json): failure counts.",
            "- [`representative_failures.jsonl`](representative_failures.jsonl): "
            "failure-focused evidence.",
            "- [`verification.txt`](verification.txt): fixed-field verifier evidence.",
            "- [`raw_manifest.json`](raw_manifest.json): private raw artifact hash and "
            "retention status.",
        ]
    )
    review_provenance = experiment_document.get("review_provenance")
    if review_provenance:
        review_roles = ", ".join(
            f"{review.get('role')}={review.get('verdict')}"
            for review in review_provenance.get("independent_reviews") or []
        )
        if not review_roles:
            review_roles = (
                "independent review exception: "
                f"{review_provenance.get('independent_review_exception')}"
            )
        lines.append(
            "- Review provenance: "
            f"{_md_cell(review_provenance.get('reviewed_by'))} at "
            f"{_md_cell(review_provenance.get('reviewed_at'))}; "
            f"{_md_cell(review_roles)}."
        )
    lines.extend(
        [
            "",
            "Raw prompts, full source documents, model response bodies, credentials, "
            "HTTP headers, and local absolute paths are intentionally not included.",
            "",
            "## Source human report",
            "",
        ]
    )
    source_ref = experiment_document["canonical_report"].get("source_report_ref")
    if source_report_snapshot is not None:
        lines.extend(
            [
                f"The original source report `{_md_cell(source_ref)}` is inside the "
                "ignored private experiment archive. A sanitized immutable snapshot is "
                "embedded below so a clean Git clone can audit the original analysis "
                "without the raw archive.",
                "",
                "<details>",
                "<summary>Sanitized source report snapshot</summary>",
                "",
                _source_snapshot_block(source_report_snapshot).rstrip(),
                "",
                "</details>",
            ]
        )
    elif source_report_link is not None:
        lines.append(
            f"Repository source: [{_md_cell(source_ref)}]({source_report_link})."
        )
    else:
        lines.append(
            "No separate canonical source report exists. This generated report and "
            "`summary.json` are the declared Git review entry points."
        )
    return "\n".join(lines).rstrip() + "\n"


def verification_text(
    entry: dict[str, Any],
    verification: dict[str, Any],
) -> str:
    source_log = verification.get("log")
    source_log_sha256 = None
    if source_log and (ROOT / source_log).is_file():
        source_log_sha256 = sha256_file(ROOT / source_log)
    source_ref = verification.get("source")
    source_ref_sha256 = None
    if source_ref and (ROOT / source_ref).is_file():
        source_ref_sha256 = sha256_file(ROOT / source_ref)
    has_replay = verification.get("replayed_backward_rows") is not None
    command = None
    if has_replay:
        command = (
            "python -B src/verify_anchorpatch.py --dir "
            f"<experiment-root>/{Path(entry['archive_path']).name}"
        )
    status = verification.get("status")
    exit_code = None
    if has_replay and status == "pass":
        exit_code = 0
    elif has_replay and status == "fail":
        exit_code = 1
    values = {
        "schema": "hybridpatch.verification/1",
        "experiment_id": entry["experiment_id"],
        "status": status,
        "evidence_basis": (
            "archived_log"
            if source_log_sha256
            else "documented_claim"
            if source_ref_sha256
            else "none"
        ),
        "verified_at": None,
        "verifier_command": command,
        "verifier_exit_code": exit_code,
        "verified_rows_total": verification.get("replayed_backward_rows"),
        "canonical_rows_expected": verification.get("canonical_backward_rows"),
        "canonical_rows_covered": (
            verification.get("canonical_backward_rows") if has_replay else None
        ),
        "mismatched_rows": verification.get("mismatches", 0),
        "accepted_known_exception": verification.get("accepted_known_exception"),
        "structural_validation_status": verification.get(
            "structural_validation_status"
        ),
        "derivation_validation_status": verification.get(
            "derivation_validation_status"
        ),
        "archive_inclusive_verified_rows": verification.get(
            "archive_inclusive_replayed_backward_rows"
        ),
        "source_ref": source_ref,
        "source_ref_sha256": source_ref_sha256,
        "source_log_ref": source_log,
        "source_log_sha256": source_log_sha256,
        "notes": " | ".join(verification.get("exceptions") or []),
    }
    return "".join(
        f"{key}={'' if value is None else sanitize_text(str(value))}\n"
        for key, value in values.items()
    )


def raw_manifest_document(
    entry: dict[str, Any],
    archive: Path,
    artifact_id: str,
    *,
    skip_tree_hash: bool,
    existing_path: Path,
    source_experiments: list[str],
    sealed_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prior = read_json(existing_path) if skip_tree_hash and existing_path.exists() else {}
    prior_artifact = (prior.get("artifacts") or [{}])[0]
    is_raw_archive = Path(entry["archive_path"]).name.startswith("exp_")
    tree = {
        "algorithm": prior_artifact.get("tree_hash_algorithm"),
        "tree_sha256": prior_artifact.get("tree_sha256"),
        "file_count": prior_artifact.get("file_count"),
        "size_bytes": prior_artifact.get("size_bytes"),
        "skipped_sensitive_files": prior_artifact.get("skipped_sensitive_files", []),
        "skipped_symlinks": prior_artifact.get("skipped_symlinks", []),
        "credential_scan": prior_artifact.get("credential_scan"),
    }
    if sealed_manifest is not None:
        sealed_tree = sealed_manifest["tree"]
        tree = {
            "algorithm": sealed_tree["algorithm"],
            "tree_sha256": sealed_tree["tree_sha256"],
            "file_count": sealed_tree["file_count"],
            "size_bytes": sealed_tree["size_bytes"],
            "skipped_sensitive_files": sealed_tree.get(
                "skipped_sensitive_files", []
            ),
            "skipped_symlinks": sealed_tree.get("skipped_symlinks", []),
            "credential_scan": sealed_tree.get("credential_scan"),
        }
    elif not skip_tree_hash:
        tree = tree_digest(archive, scan_credentials=is_raw_archive)
    credential_scan = tree.get("credential_scan")
    if credential_scan is None:
        credential_scan = {
            "status": entry.get("raw_credential_scan"),
            "scanner_version": None,
            "scanned_at": None,
        }
    if credential_scan.get("heuristic_marker_match_count"):
        credential_scan.setdefault(
            "interpretation",
            "Heuristic markers may be benchmark content rather than credentials; "
            "the artifact remains private and must be sanitized before sharing.",
        )
    artifact = {
        "artifact_id": artifact_id,
        "artifact_kind": (
            "logical_manifest_view"
            if entry.get("kind") == "manifest_view"
            else "experiment_directory"
        ),
        "archive_status": (
            "compressed_private" if sealed_manifest is not None else "not_compressed"
        ),
        "storage": (
            "repository_worktree_derived_view"
            if entry.get("kind") == "manifest_view"
            else "ignored_local_directory"
        ),
        "storage_key": entry["archive_path"],
        "tree_hash_algorithm": tree.get("algorithm"),
        "tree_sha256": tree.get("tree_sha256"),
        "file_count": tree.get("file_count"),
        "size_bytes": tree.get("size_bytes"),
        "skipped_sensitive_files": tree.get("skipped_sensitive_files"),
        "skipped_symlinks": tree.get("skipped_symlinks"),
        "compression": (
            {
                "format": sealed_manifest["archive"]["format"],
                "sha256": sealed_manifest["archive"]["sha256"],
                "size_bytes": sealed_manifest["archive"]["size_bytes"],
            }
            if sealed_manifest is not None
            else None
        ),
        "shareability": "private_only" if is_raw_archive else "repository_safe",
        "retention": entry.get("raw_retention"),
        "source_experiments": source_experiments,
        "content_flags": {
            "contains_full_prompts": is_raw_archive,
            "contains_model_outputs": is_raw_archive,
            "contains_source_documents": is_raw_archive,
            "contains_transport_details": is_raw_archive,
        },
        "credential_scan": credential_scan,
        "sanitization": {
            "status": "raw" if is_raw_archive else "curated_manifest",
            "sanitizer_version": None,
        },
    }
    return {
        "schema": "hybridpatch.raw_manifest/1",
        "experiment_id": entry["experiment_id"],
        "artifacts": [artifact],
    }


def build_experiment(
    record_set: dict[str, Any],
    entry: dict[str, Any],
    *,
    catalog_digest: str,
    skip_tree_hash: bool,
    check: bool,
    sealed_manifest: dict[str, Any] | None = None,
    output_directory: Path | None = None,
) -> dict[str, Any]:
    owner = record_set["owner"]
    archive = ROOT / entry["archive_path"]
    if not archive.exists():
        raise RuntimeError(f"archive missing: {entry['archive_path']}")
    methods = list(entry["arms"])
    source_experiments: list[str] = []
    if entry.get("kind") == "manifest_view":
        results, source_experiments = load_manifest_view(ROOT / entry["manifest_path"])
    else:
        results = load_standard_results(archive, methods)

    planned = planned_samples(archive, entry, results)
    expected_sample_count = int(entry["expected_sample_count"])
    if len(planned) != expected_sample_count:
        raise RuntimeError(
            f"{entry['experiment_id']}: planned sample count {len(planned)} "
            f"does not match catalog {expected_sample_count}"
        )
    canonical, excluded = canonical_samples(entry, planned, results, methods)
    canonical_set = set(canonical)
    artifact_id = f"{entry['experiment_id']}-raw-v1"
    sample_rows, cases = row_grid(
        entry,
        planned,
        canonical_set,
        excluded,
        methods,
        results,
        artifact_id,
    )
    verification = dict(entry["verification"])
    scope_id = canonical_scope_id(entry, len(canonical))
    summary = summary_document(
        entry,
        planned,
        canonical,
        excluded,
        sample_rows,
        methods,
        verification,
        scope_id,
    )
    expected_canonical_backward = verification.get("canonical_backward_rows")
    actual_canonical_backward = summary["scope"]["canonical_backward_rows"]
    if (
        expected_canonical_backward is not None
        and int(expected_canonical_backward) != actual_canonical_backward
    ):
        raise RuntimeError(
            f"{entry['experiment_id']}: canonical backward rows "
            f"{actual_canonical_backward} do not match verification claim "
            f"{expected_canonical_backward}"
        )
    all_committed_backward = sum(
        row["direction"] == "backward"
        and row["execution_status"] != "missing"
        for row in sample_rows
    )
    replayed_backward = verification.get("replayed_backward_rows")
    archive_inclusive_replayed = verification.get(
        "archive_inclusive_replayed_backward_rows"
    )
    if archive_inclusive_replayed is not None:
        if int(archive_inclusive_replayed) != all_committed_backward:
            raise RuntimeError(
                f"{entry['experiment_id']}: archive-inclusive replay claim "
                f"{archive_inclusive_replayed} does not match "
                f"{all_committed_backward} committed backward rows"
            )
        if (
            replayed_backward is not None
            and int(replayed_backward) != actual_canonical_backward
        ):
            raise RuntimeError(
                f"{entry['experiment_id']}: canonical replay claim "
                f"{replayed_backward} does not match "
                f"{actual_canonical_backward} canonical backward rows"
            )
    elif (
        replayed_backward is not None
        and int(replayed_backward) != all_committed_backward
    ):
        raise RuntimeError(
            f"{entry['experiment_id']}: replay claim {replayed_backward} "
            f"does not match {all_committed_backward} committed backward rows"
        )
    expected_paired = verification.get("canonical_paired_backward_rows")
    if expected_paired is not None:
        actual_paired = (
            summary["comparisons"][0]["paired_backward_rows"]
            if summary["comparisons"]
            else 0
        )
        if int(expected_paired) != actual_paired:
            raise RuntimeError(
                f"{entry['experiment_id']}: paired replay scope "
                f"{expected_paired} does not match {actual_paired}"
            )
    exact_matches = verification.get("exact_matches")
    mismatches = verification.get("mismatches")
    if (
        replayed_backward is not None
        and exact_matches is not None
        and mismatches is not None
        and int(exact_matches) + int(mismatches) != int(replayed_backward)
    ):
        raise RuntimeError(
            f"{entry['experiment_id']}: exact_matches + mismatches "
            "does not equal replayed_backward_rows"
        )
    nonzero_preservation = [
        row
        for row in sample_rows
        if row["preservation_violations"] not in (None, "", 0)
    ]
    if nonzero_preservation:
        raise RuntimeError(
            f"{entry['experiment_id']}: preservation invariant violated in "
            f"{len(nonzero_preservation)} committed rows"
        )
    failures = failure_stats_document(entry, sample_rows)
    reports = report_records(entry)

    metadata_path = archive / "run_metadata.jsonl"
    metadata = read_jsonl(metadata_path) if metadata_path.exists() else []
    code_provenance = resolve_metadata_code_provenance(archive, metadata)
    fingerprints = code_provenance["code_fingerprints"]
    command_templates = sorted(
        {
            normalize_command(str(record["command"]))
            for record in metadata
            if record.get("command")
        }
    )
    started = sorted(
        str(value)
        for value in (
            unique_values(metadata, "started_at")
            or unique_values(metadata, "created_local")
        )
    )
    run_git_commits = code_provenance["run_git_commits"]
    git_tree_states = unique_values(metadata, "git_tree_state")
    finished = unique_values(metadata, "finished_at")
    timezones = unique_values(metadata, "timezone")
    for field, values in (
        ("git_tree_state", git_tree_states),
        ("finished_at", finished),
        ("timezone", timezones),
    ):
        if len(values) > 1:
            raise RuntimeError(
                f"{entry['experiment_id']}: conflicting metadata {field}: {values}"
            )
    protocols = result_protocol_revisions(results)
    declared_protocol = entry.get("protocol_revision")
    if declared_protocol and any(
        revision != declared_protocol for revision in protocols
    ):
        raise RuntimeError(
            f"{entry['experiment_id']}: observed valid envelope revisions "
            f"{protocols} do not match declared {declared_protocol}"
        )
    if protocols and not declared_protocol:
        raise RuntimeError(
            f"{entry['experiment_id']}: observed protocol revisions {protocols} "
            "but catalog has no protocol_revision"
        )
    canonical_source_path = entry.get("canonical_source_report")
    source_canonical_record = next(
        (
            report
            for report in reports
            if report["path"] == canonical_source_path
        ),
        None,
    )
    if canonical_source_path and source_canonical_record is None:
        raise RuntimeError(
            f"{entry['experiment_id']}: canonical source report "
            f"{canonical_source_path!r} is not listed in source_reports"
        )
    source_canonical = (
        source_canonical_record["path"] if source_canonical_record else None
    )
    directory = output_directory or record_dir(owner, entry["experiment_id"])
    source_report_link = None
    source_report_snapshot = None
    source_report_embedded = False
    if source_canonical:
        source_path = ROOT / source_canonical
        is_raw_archive = Path(entry["archive_path"]).name.startswith("exp_")
        try:
            source_path.resolve().relative_to(archive.resolve())
            source_inside_archive = True
        except ValueError:
            source_inside_archive = False
        if is_raw_archive and source_inside_archive:
            source_report_snapshot = sanitize_markdown(
                source_path.read_text(encoding="utf-8", errors="replace")
            )
            source_report_embedded = True
        else:
            source_report_link = os.path.relpath(source_path, directory).replace(
                "\\", "/"
            )
    noncanonical_reports = [
        {
            "source_report_ref": report["path"],
            "role": report["role"],
            "sha256": report["sha256"],
            "size_bytes": report["size_bytes"],
            "reason": (
                "Source evidence only; the generated summary.json is the sole canonical machine scope."
            ),
        }
        for report in reports
        if report["path"] != source_canonical
    ]
    gaps = []
    if not run_git_commits:
        gaps.append(
            {
                "field": "code.run_git_commit",
                "reason": "The historical run metadata did not record a Git commit.",
                "impact": "Exact commit cannot be asserted; archived per-file fingerprints remain authoritative.",
            }
        )
    if not git_tree_states:
        gaps.append(
            {
                "field": "code.git_tree_state",
                "reason": "The historical run metadata did not record whether the run tree was clean or dirty.",
                "impact": "Uncommitted run-time edits cannot be ruled out from Git metadata alone.",
            }
        )
    if not finished:
        gaps.append(
            {
                "field": "time.finished_at",
                "reason": "No explicit completion timestamp was recorded.",
                "impact": "The latest worker start is not treated as an experiment finish time.",
            }
        )
    transport_revisions = unique_values(metadata, "transport_revision")
    if not transport_revisions:
        gaps.append(
            {
                "field": "model.transport_revision",
                "reason": "Legacy run metadata did not record a transport revision.",
                "impact": "Transport semantics must be interpreted from versioned project documentation.",
            }
        )

    code_document = {
        "run_git_commit": code_provenance["run_git_commit"],
        "git_tree_state": git_tree_states[0] if git_tree_states else None,
        "provenance_level": (
            "git_commit_and_fingerprints"
            if run_git_commits and fingerprints
            else "git_commit_only"
            if run_git_commits
            else "fingerprints_only"
            if fingerprints
            else "documented_reference_only"
        ),
        "fingerprint_algorithm": "sha1-12" if fingerprints else None,
        "fingerprint_count": len(fingerprints),
        "fingerprints": fingerprints,
        "version_reference": entry.get("code_reference"),
    }
    if code_provenance["campaign_recovery_authorization"] is not None:
        code_document.update({
            "run_git_commits": run_git_commits,
            "campaign_recovery_authorization": code_provenance[
                "campaign_recovery_authorization"
            ],
            "provenance_warnings": code_provenance["provenance_warnings"],
        })

    def protocol_fingerprint(key: str) -> Any:
        if len(fingerprints) == 1 and fingerprints[0]:
            return fingerprints[0].get(key)
        if code_provenance["campaign_recovery_authorization"] is not None:
            return invariant_fingerprint_value(fingerprints, key)
        return None

    experiment_document = {
        "schema": "hybridpatch.experiment_record/1",
        "record_revision": 1,
        "catalog_sha256": catalog_digest,
        "experiment_id": entry["experiment_id"],
        "record_kind": "derived" if entry.get("kind") == "manifest_view" else "run",
        "owner": owner,
        "title": entry["purpose"],
        "purpose": entry["purpose"],
        "lifecycle_status": lifecycle_status(entry),
        "evidence_role": evidence_role(entry),
        "claim_eligibility": claim_eligibility(entry),
        "source": {
            "experiment_root_ref": entry["archive_path"],
            "run_metadata_schema": scalar_or_list(unique_values(metadata, "schema")),
            "raw_artifact_id": artifact_id,
            "source_experiments": source_experiments,
        },
        "code": code_document,
        "execution": {
            "working_directory_ref": owner if owner.startswith("HP_V") else None,
            "command_templates": command_templates,
            "methods": methods,
            "arms": entry["arms"],
            "seed": (
                scalar_or_list(unique_values(metadata, "seed"))
                if unique_values(metadata, "seed")
                else entry.get("seed")
            ),
            "round_trips": entry["round_trips"],
            "distractor": (
                scalar_or_list(unique_values(metadata, "distractor"))
                if unique_values(metadata, "distractor")
                else entry.get("distractor")
            ),
            "comparison_policy": entry.get("comparison_policy"),
        },
        "model": {
            "name": (
                scalar_or_list(unique_values(metadata, "model"))
                if unique_values(metadata, "model")
                else entry.get("model")
            ),
            "provider": (
                scalar_or_list(unique_values(metadata, "provider"))
                if unique_values(metadata, "provider")
                else entry.get("provider")
            ),
            "base_url": scalar_or_list(unique_values(metadata, "base_url")),
            "request_url": scalar_or_list(unique_values(metadata, "request_url")),
            "transport": scalar_or_list(unique_values(metadata, "transport")),
            "transport_revision": scalar_or_list(transport_revisions),
            "thinking_mode": (
                scalar_or_list(unique_values(metadata, "thinking_mode"))
                if unique_values(metadata, "thinking_mode")
                else entry.get("thinking_mode")
            ),
            "max_tokens": scalar_or_list(
                unique_values(metadata, "effective_max_tokens")
                or unique_values(metadata, "max_tokens")
            ),
            "temperature": None,
        },
        "protocol": {
            "declared_campaign_revision": declared_protocol,
            "observed_valid_envelope_revisions": protocols,
            "prompt_revision": None,
            "prompt_fingerprint": protocol_fingerprint("hybrid_prompt.py"),
            "executor_fingerprint": protocol_fingerprint("hybrid_executor.py"),
            "runner_fingerprint": protocol_fingerprint("experiment_runner.py"),
        },
        "data": {
            "dataset": entry["dataset"],
            "split": entry["split"],
            "planned_sample_ids": planned,
            "canonical_sample_ids": canonical,
            "excluded_samples": [
                {"sample_id": sample, "reason": reason}
                for sample, reason in sorted(excluded.items())
            ],
            "task_plan_source": entry.get("task_plan_source"),
        },
        "time": {
            "started_at_local": started[0] if started else None,
            "last_dispatch_started_at_local": started[-1] if started else None,
            "finished_at": finished[0] if finished else None,
            "timezone": timezones[0] if timezones else None,
        },
        "canonical_report": {
            "scope_id": scope_id,
            "record_ref": "summary.json",
            "machine_record_ref": "summary.json",
            "human_record_ref": "report.md",
            "source_report_ref": source_canonical,
            "source_report_sha256": (
                source_canonical_record["sha256"]
                if source_canonical_record
                else None
            ),
            "source_report_embedded": source_report_embedded,
            "source_report_embedding_policy": (
                "sanitized_snapshot_v1"
                if source_report_embedded
                else "linked_repository_source"
                if source_canonical
                else "none"
            ),
            "selection_reason": entry.get("comparison_policy"),
        },
        "analysis_artifacts": {
            "human_report_ref": "report.md",
            "machine_summary_ref": "summary.json",
            "casebook_ref": "casebook.jsonl",
            "samples_ref": "samples.csv",
            "failure_stats_ref": "failure_stats.json",
            "representative_failures_ref": "representative_failures.jsonl",
        },
        "paper_reporting": entry.get("paper_reporting"),
        "source_reports": reports,
        "noncanonical_reports": noncanonical_reports,
        "verification": verification,
        "provenance_gaps": gaps,
        "known_exceptions": entry.get("known_exceptions") or [],
        "raw_manifest_ref": "raw_manifest.json",
    }
    if entry.get("review_provenance") is not None:
        experiment_document["review_provenance"] = entry["review_provenance"]

    raw_manifest = raw_manifest_document(
        entry,
        archive,
        artifact_id,
        skip_tree_hash=skip_tree_hash,
        existing_path=directory / "raw_manifest.json",
        source_experiments=source_experiments,
        sealed_manifest=sealed_manifest,
    )
    representative = representative_cases(cases)
    casebook = build_casebook(entry, sample_rows, methods)
    report_content = report_markdown(
        entry,
        experiment_document,
        summary,
        failures,
        casebook,
        source_report_link=source_report_link,
        source_report_snapshot=source_report_snapshot,
    )
    from io import StringIO

    csv_buffer = StringIO(newline="")
    writer = csv.DictWriter(csv_buffer, fieldnames=SAMPLE_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(sample_rows)
    csv_content = csv_buffer.getvalue()
    representative_content = "".join(
        json.dumps(case, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
        for case in representative
    )
    casebook_content = "".join(
        json.dumps(case, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
        for case in casebook
    )
    yaml_content = yaml.safe_dump(
        experiment_document,
        allow_unicode=True,
        sort_keys=False,
        width=100,
    )
    if not yaml_content.endswith("\n"):
        yaml_content += "\n"

    outputs = {
        "experiment.yaml": yaml_content,
        "report.md": report_content,
        "summary.json": stable_json(summary),
        "samples.csv": csv_content,
        "casebook.jsonl": casebook_content,
        "failure_stats.json": stable_json(failures),
        "representative_failures.jsonl": representative_content,
        "verification.txt": verification_text(entry, verification),
        "raw_manifest.json": stable_json(raw_manifest),
    }
    changed = 0
    for filename, content in outputs.items():
        changed += write_atomic(directory / filename, content, check=check)
    return {
        "owner": owner,
        "experiment_id": entry["experiment_id"],
        "status": entry["status"],
        "evidence_role": experiment_document["evidence_role"],
        "claim_eligibility": experiment_document["claim_eligibility"],
        "lifecycle_status": experiment_document["lifecycle_status"],
        "scope_id": scope_id,
        "planned_samples": len(planned),
        "canonical_samples": len(canonical),
        "summary": summary,
        "verification": verification,
        "paper_reporting": entry.get("paper_reporting"),
        "raw_manifest": raw_manifest,
        "changed_files": changed,
    }


def headline(result: dict[str, Any]) -> str:
    summary = result["summary"]
    comparisons = summary.get("comparisons") or []
    paper_reporting = result.get("paper_reporting")
    if paper_reporting:
        delta = paper_reporting.get("delta_hybridpatch_minus_fullrewrite")
        return (
            f"paper-policy Δ={_format_number(delta, signed=True)}, "
            f"n={paper_reporting.get('paired_backward_rows')}"
        )
    if result["claim_eligibility"] == "none":
        return (
            f"{result['canonical_samples']} complete source chains; "
            "no standalone claim"
        )
    headline_comparisons = [
        comparison for comparison in comparisons if comparison.get("headline")
    ]
    if headline_comparisons:
        comparison = headline_comparisons[0]
        delta = comparison.get("delta_a_minus_b")
        delta_text = "n/a" if delta is None else f"{delta:+.3f}"
        return f"Δ={delta_text}, n={comparison['paired_backward_rows']}"
    if comparisons:
        verified = result["verification"].get("replayed_backward_rows")
        rows = (
            verified
            if verified is not None
            else summary["scope"]["canonical_backward_rows"]
        )
        return (
            f"diagnostic/supporting only; {rows} auditable backward rows"
        )
    if result["claim_eligibility"] == "diagnostic_only":
        verified = result["verification"].get("replayed_backward_rows")
        rows = (
            verified
            if verified is not None
            else summary["scope"]["canonical_backward_rows"]
        )
        return f"diagnostic only; {rows} auditable backward rows"
    methods = summary.get("methods") or {}
    if not methods:
        return "no metric rows"
    method, values = next(iter(methods.items()))
    rs_at_k = values.get("rs_at_k") or {}
    points = [
        f"RS@{key}={rs_at_k[key]:.3f}"
        for key in ("1", "5", "10")
        if rs_at_k.get(key) is not None
    ]
    if points:
        return f"{method} " + "/".join(points)
    score = values.get("mean_backward_rs")
    return (
        f"{method} mean RS={'n/a' if score is None else f'{score:.3f}'}"
    )


def verification_label(verification: dict[str, Any]) -> str:
    status = str(verification.get("status") or "unknown")
    if status == "fail" and verification.get("accepted_known_exception"):
        return "fail (known exception)"
    if status == "not_run" and verification.get("structural_validation_status"):
        return "not_run (structural pass)"
    if status == "not_run" and verification.get("derivation_validation_status"):
        return "not_run (derivation pass)"
    return status


def index_markdown(owner: str, results: list[dict[str, Any]]) -> str:
    lines = [
        f"# {owner} experiment records",
        "",
        "> This is a provenance overlay. It does not alter frozen runtime, prompt,",
        "> checkpoint, raw-response, or evaluator bytes. Each record's `report.md` is",
        "> the canonical human entry; `summary.json` is the sole canonical machine scope.",
        "",
        "| Experiment | Lifecycle | Evidence | Claim use | Canonical scope | Human report | Machine summary | Verification |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for result in results:
        experiment = result["experiment_id"]
        relative = f"records/{experiment}"
        verification = verification_label(result["verification"])
        lines.append(
            "| "
            f"[{experiment}]({relative}/experiment.yaml) | "
            f"{result['lifecycle_status']} | {result['evidence_role']} | "
            f"{result['claim_eligibility']} | "
            f"{result['scope_id']} ({result['canonical_samples']} samples) | "
            f"[read report]({relative}/report.md) | "
            f"[{headline(result)}]({relative}/summary.json) | "
            f"[{verification}]({relative}/verification.txt) |"
        )
    lines.extend(
        [
            "",
            "Raw experiment directories remain ignored and private. Their content tree hash,",
            "size, retention class, and sanitization status are recorded in each",
            "`raw_manifest.json`.",
            "",
        ]
    )
    return "\n".join(lines)


def global_index(results: list[dict[str, Any]]) -> str:
    lines = [
        "# Experiment record index",
        "",
        "Every experiment has a generated canonical human entry (`report.md`) and a",
        "canonical machine scope (`summary.json`). Source reports remain linked or",
        "embedded with an explicit role; they do not silently override the declared scope.",
        "",
        "| Owner | Experiment | Claim use | Canonical scope | Human report | Machine summary | Verification |",
        "|---|---|---|---|---|---|---|",
    ]
    for result in results:
        owner = result["owner"]
        experiment = result["experiment_id"]
        base = f"../{owner}/records/{experiment}"
        lines.append(
            "| "
            f"[{owner}](../{owner}/EXPERIMENTS.md) | "
            f"[{experiment}]({base}/experiment.yaml) | "
            f"{result['claim_eligibility']} | "
            f"{result['scope_id']} ({result['canonical_samples']} samples) | "
            f"[read report]({base}/report.md) | "
            f"[{headline(result)}]({base}/summary.json) | "
            f"[{verification_label(result['verification'])}]({base}/verification.txt) |"
        )
    lines.extend(
        [
            "",
            "Schema and maintenance rules: [EXPERIMENT_RECORDS.md](EXPERIMENT_RECORDS.md).",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    if args.validate_only and not args.only:
        raise RuntimeError("--validate-only requires at least one --only experiment")
    if args.sealed_manifest is not None and len(args.only) != 1:
        raise RuntimeError("--sealed-manifest requires exactly one --only experiment")
    if args.sealed_manifest is not None and args.skip_tree_hash:
        raise RuntimeError("--sealed-manifest and --skip-tree-hash are mutually exclusive")
    catalog_path = args.catalog.resolve()
    catalog = load_catalog(catalog_path)
    digest = sha256_file(catalog_path)
    only = set(args.only)
    built: list[dict[str, Any]] = []
    selected = [
        (record_set, entry)
        for record_set, entry in catalog_entries(catalog)
        if not only or entry["experiment_id"] in only
    ]
    sealed = None
    if args.sealed_manifest is not None:
        from experiment_artifacts import load_valid_seal

        entry = selected[0][1] if selected else None
        if entry is None:
            raise RuntimeError("sealed experiment is absent from the catalog")
        sealed = load_valid_seal(
            args.sealed_manifest.resolve(),
            experiment_id=entry["experiment_id"],
            source=ROOT / entry["archive_path"],
        )

    context = (
        tempfile.TemporaryDirectory(
            prefix=".hybridpatch-record-validation-",
            dir=ROOT,
        )
        if args.validate_only
        else nullcontext(None)
    )
    with context as temporary:
        for record_set, entry in selected:
            output_directory = (
                Path(temporary) / record_set["owner"] / entry["experiment_id"]
                if args.validate_only
                else None
            )
            built.append(
                build_experiment(
                    record_set,
                    entry,
                    catalog_digest=digest,
                    skip_tree_hash=args.skip_tree_hash,
                    check=False if args.validate_only else args.check,
                    sealed_manifest=sealed,
                    output_directory=output_directory,
                )
            )
            print(
                f"[records] {record_set['owner']}/{entry['experiment_id']}: "
                f"canonical_samples={built[-1]['canonical_samples']} "
                f"changed={built[-1]['changed_files']}"
            )

    if args.validate_only:
        print("[records] VALIDATE-ONLY PASS; generated state was not changed")
        return 0

    if only:
        print("[records] --only selected: aggregate indexes were not rewritten")
        return 0

    by_owner: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for result in built:
        by_owner[result["owner"]].append(result)
    for owner, owner_results in by_owner.items():
        owner_results.sort(key=lambda item: item["experiment_id"])
        write_atomic(
            ROOT / owner / "EXPERIMENTS.md",
            index_markdown(owner, owner_results),
            check=args.check,
        )
    built.sort(key=lambda item: (item["owner"], item["experiment_id"]))
    write_atomic(
        ROOT / "docs" / "EXPERIMENT_INDEX.md",
        global_index(built),
        check=args.check,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
