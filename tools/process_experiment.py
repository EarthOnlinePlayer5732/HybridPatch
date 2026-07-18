"""Two-stage post-processing for new HybridPatch experiments.

``prepare`` runs the version-local verifier and analyzer, records only
mechanically derived facts, and writes a review YAML for the judgments that
must not be guessed. ``finalize`` verifies that review, atomically registers
the experiment, and delegates Git-record generation to finalize_experiment.py.

This tool is offline: it never calls a model. It refuses frozen HP_V3-HP_V7.
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
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import yaml


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "tools" / "experiment_records_catalog.json"
FINALIZER = ROOT / "tools" / "finalize_experiment.py"
VERSION_STATES = ROOT / "tools" / "version_states.json"
EXPERIMENT_PLANS = ROOT / "docs" / "experiment_plans"
OWNER_RE = re.compile(r"^HP_V(\d+)$")
PLACEHOLDER = "REVIEW_REQUIRED"
METHOD_NAMES = ("hybridpatch", "fullrewrite", "anchorpatch")


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


def activate_owner(owner: str) -> None:
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
    prepared = []
    for state_path in owner_path.glob("exp_*/analysis/process_state.json"):
        state = load_json(state_path)
        if state.get("stage") != "finalized":
            prepared.append(repo_ref(state_path))
    if prepared:
        raise RuntimeError(
            "cannot freeze with unfinished post-processing states: "
            f"{prepared}"
        )
    catalog = load_json(CATALOG)
    catalogued = {
        str(entry.get("experiment_id"))
        for record_set in catalog.get("record_sets") or []
        if record_set.get("owner") == owner
        for entry in record_set.get("experiments") or []
    }
    unrecorded = sorted(
        path.name
        for path in owner_path.glob("exp_*")
        if path.is_dir() and path.name not in catalogued
    )
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
    metadata_path = archive / "run_metadata.jsonl"
    if not metadata_path.is_file():
        raise RuntimeError("run_metadata.jsonl is required")
    if list(archive.rglob(".env*")):
        raise RuntimeError("experiment archive contains a forbidden .env* file")
    metadata = load_jsonl(metadata_path)
    if not metadata:
        raise RuntimeError("run_metadata.jsonl is empty")

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

    input_paths = [metadata_path, *archive.glob("*.task_plan.json")]
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

    fingerprints = unique(metadata, "code_fingerprint")
    if len(fingerprints) != 1:
        raise RuntimeError(
            f"new method experiment must have one code fingerprint, found {len(fingerprints)}"
        )
    protocols = result_protocols(all_result_rows)
    if len(protocols) > 1:
        raise RuntimeError(f"mixed protocol revisions: {protocols}")
    run_commit = one_value(metadata, "run_git_commit", required=False)
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
        "code_fingerprint": fingerprints[0],
        "run_git_commit": run_commit,
        "git_tree_state": git_tree_state,
        "started_at": started_at,
        "finished_at": finished_at,
        "provenance_warnings": [],
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
    }
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


def finalize_experiment(experiment: Path, review_path: Path | None) -> None:
    owner, _, archive = resolve_experiment(experiment)
    analysis = archive / "analysis"
    facts_path = analysis / "derived_facts.json"
    if not facts_path.is_file():
        raise RuntimeError("prepare has not produced analysis/derived_facts.json")
    facts = load_json(facts_path)
    current = collect_facts(owner, archive)
    if current["input_sha256"] != facts.get("input_sha256"):
        raise RuntimeError("raw result inputs changed after prepare; run prepare again")
    if current["experiment_plan_sha256"] != facts.get("experiment_plan_sha256"):
        raise RuntimeError("experiment plan changed after prepare; run prepare again")
    selected_review = review_path or analysis / "record_review.yaml"
    if not selected_review.is_absolute():
        selected_review = ROOT / selected_review
    with selected_review.open(encoding="utf-8") as handle:
        review = yaml.safe_load(handle)
    if not isinstance(review, dict):
        raise RuntimeError("review YAML must contain an object")
    entry = validate_review(review, facts)

    catalog = load_json(CATALOG)
    record_directory = ROOT / owner / "records" / archive.name
    if record_directory.exists():
        raise RuntimeError(
            f"record directory already exists; refusing overwrite: {repo_ref(record_directory)}"
        )
    original_catalog = json.loads(json.dumps(catalog))
    register_entry(catalog, owner, entry)

    with tempfile.TemporaryDirectory(prefix="hybridpatch-record-backup-") as temporary:
        snapshots = snapshot_generated(
            original_catalog,
            Path(temporary),
            extra_owners=[owner],
        )
        try:
            write_atomic(CATALOG, stable_json(catalog))
            completed = subprocess.run(
                [sys.executable, str(FINALIZER), archive.name],
                cwd=ROOT,
                env={**os.environ, "PYTHONUTF8": "1"},
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"finalize_experiment.py failed with {completed.returncode}"
                )
        except Exception:
            restore_generated(snapshots)
            if record_directory.exists():
                shutil.rmtree(record_directory)
            raise

    write_atomic(
        analysis / "process_state.json",
        stable_json(
            {
                "schema": "hybridpatch.experiment_process_state/1",
                "experiment_id": archive.name,
                "stage": "finalized",
                "input_sha256": facts["input_sha256"],
                "record_ref": f"{owner}/records/{archive.name}/report.md",
            }
        ),
    )
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
    elif args.command == "prepare":
        prepare_experiment(
            args.experiment,
            critical_theta=args.critical_theta,
            confirm_stopped=args.confirm_stopped,
            force_review_draft=args.force_review_draft,
        )
    elif args.command == "finalize":
        finalize_experiment(args.experiment, args.review)
    else:
        raise RuntimeError(f"unsupported command: {args.command}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"[process] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
