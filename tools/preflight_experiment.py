#!/usr/bin/env python3
"""Build a cached, zero-API launch receipt for one paired campaign.

This is a thin orchestrator over the existing version-local regression tests,
paired dispatcher ``--dry_run``, and evaluator runtime preflight.  It never
runs the Key liveness probe and never launches a campaign worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import evaluator_runtime_preflight as evaluator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "hybridpatch.experiment_preflight_receipt/2"
REGRESSION_SCHEMA = "hybridpatch.zero_api_regression_receipt/2"
REGRESSION_SCRIPTS = (
    "src/test_hybrid_executor.py",
    "src/splitters.py",
    "src/test_model_openai.py",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True, type=Path)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=ROOT / ".cache" / "hybridpatch" / "preflight",
    )
    parser.add_argument("--evaluator-jobs", type=int, default=8)
    parser.add_argument("--evaluator-timeout", type=float, default=120.0)
    parser.add_argument(
        "dispatcher_args",
        nargs=argparse.REMAINDER,
        help="Arguments for paired_campaign_dispatch.py after --.",
    )
    args = parser.parse_args(argv)
    if args.evaluator_jobs < 1:
        parser.error("--evaluator-jobs must be >= 1")
    if args.evaluator_timeout <= 0:
        parser.error("--evaluator-timeout must be > 0")
    if args.dispatcher_args[:1] == ["--"]:
        args.dispatcher_args = args.dispatcher_args[1:]
    if not args.dispatcher_args:
        parser.error("dispatcher arguments are required after --")
    forbidden = ("--out_dir", "--dry_run")
    for value in args.dispatcher_args:
        if any(value == item or value.startswith(item + "=") for item in forbidden):
            parser.error(f"{value.split('=', 1)[0]} is managed by this command")
    return args


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


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError(f"expected a JSON object: {path}")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _paths_digest(root: Path, paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def _is_regression_input(path: Path) -> bool:
    return (
        path.is_file()
        and not {"__pycache__", ".pytest_cache"}.intersection(path.parts)
        and path.suffix not in {".pyc", ".pyo"}
    )


def regression_code_sha256(version_root: Path) -> str:
    candidates = {
        path
        for path in (version_root / "src").rglob("*.py")
        if _is_regression_input(path)
    }
    fixture_root = version_root / "src" / "test_fixtures"
    if fixture_root.is_dir():
        candidates.update(
            path for path in fixture_root.rglob("*") if _is_regression_input(path)
        )
    tests_root = version_root / "tests"
    if tests_root.is_dir():
        candidates.update(
            path for path in tests_root.rglob("*") if _is_regression_input(path)
        )
    requirements = version_root / "requirements.txt"
    if requirements.is_file():
        candidates.add(requirements)
    return _paths_digest(version_root, list(candidates))


def repo_ref(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def active_version_name() -> str:
    states = read_json(ROOT / "tools" / "version_states.json")
    active = states.get("active_version")
    if not isinstance(active, str) or not active.startswith("HP_V"):
        raise RuntimeError("version_states.json has no valid active version")
    if (states.get("versions") or {}).get(active) != "active":
        raise RuntimeError(f"version_states.json does not mark {active} active")
    return active


def _command_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for name in evaluator.CREDENTIAL_ENV_VARS:
        environment.pop(name, None)
    environment.update(PYTHONUTF8="1", PYTHONDONTWRITEBYTECODE="1")
    return environment


def run_checked(command: list[str], *, cwd: Path) -> dict[str, Any]:
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=_command_environment(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    record = {
        "command": command,
        "cwd": repo_ref(cwd),
        "exit_code": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 6),
        "stdout_tail": (completed.stdout or "")[-2000:],
        "stderr_tail": (completed.stderr or "")[-2000:],
    }
    if completed.returncode != 0:
        detail = record["stderr_tail"] or record["stdout_tail"]
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}: "
            f"{' '.join(command)}\n{detail}"
        )
    return record


def _regression_commands(version_root: Path) -> list[list[str]]:
    commands = []
    for relative in REGRESSION_SCRIPTS:
        path = version_root / relative
        if not path.is_file():
            raise RuntimeError(f"zero-API regression entry is missing: {path}")
        commands.append([sys.executable, "-B", f"./{relative}"])
    return commands


def ensure_regression_receipt(
    version_root: Path,
    cache_path: Path,
) -> tuple[dict[str, Any], bool]:
    code_sha256 = regression_code_sha256(version_root)
    runtime = evaluator.runtime_identity()
    commands = _regression_commands(version_root)
    if cache_path.is_file():
        try:
            cached = read_json(cache_path)
        except (OSError, ValueError, RuntimeError):
            cached = {}
        if (
            cached.get("schema") == REGRESSION_SCHEMA
            and cached.get("version") == version_root.name
            and cached.get("regression_code_sha256") == code_sha256
            and cached.get("runtime_identity") == runtime
            and cached.get("commands") == commands
            and all(item.get("exit_code") == 0 for item in cached.get("results") or [])
            and len(cached.get("results") or []) == len(commands)
        ):
            return cached, True

    results = [run_checked(command, cwd=version_root) for command in commands]
    receipt = {
        "schema": REGRESSION_SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "version": version_root.name,
        "regression_code_sha256": code_sha256,
        "runtime_identity": runtime,
        "commands": commands,
        "results": results,
        "status": "pass",
        "zero_api": True,
    }
    write_atomic(cache_path, stable_json(receipt))
    return receipt, False


def _resolve_inputs(args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    experiment = args.experiment.expanduser().resolve()
    version_root = experiment.parent
    if version_root.parent.resolve() != ROOT.resolve() or not version_root.name.startswith(
        "HP_V"
    ):
        raise RuntimeError("--experiment must be directly under an HP_Vx root")
    if version_root.name != active_version_name():
        raise RuntimeError(
            f"experiment owner {version_root.name} is not the active version"
        )
    dispatcher = version_root / "src" / "paired_campaign_dispatch.py"
    if not dispatcher.is_file():
        raise RuntimeError(f"paired dispatcher is missing: {dispatcher}")
    plan = args.plan.expanduser().resolve()
    if not plan.is_file():
        raise RuntimeError(f"experiment plan is missing: {plan}")
    if plan.stem != experiment.name:
        raise RuntimeError("experiment plan filename must match the experiment id")
    cache_dir = args.cache_dir.expanduser().resolve()
    return experiment, version_root, plan, cache_dir


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    experiment, version_root, plan, cache_dir = _resolve_inputs(args)
    cache_dir.mkdir(parents=True, exist_ok=True)

    regression_cache = cache_dir / f"{version_root.name}_regression.json"
    regression, regression_cached = ensure_regression_receipt(
        version_root,
        regression_cache,
    )
    print(
        "[preflight] zero-API regression "
        + ("cache hit" if regression_cached else "executed"),
        flush=True,
    )

    dispatcher_command = [
        sys.executable,
        "-B",
        "./src/paired_campaign_dispatch.py",
        "--out_dir",
        str(experiment),
        *args.dispatcher_args,
        "--dry_run",
    ]
    dispatcher_result = run_checked(dispatcher_command, cwd=version_root)
    manifest_path = experiment / "dispatch_manifest.json"
    manifest = read_json(manifest_path)
    if manifest.get("experiment_id") != experiment.name:
        raise RuntimeError("dispatcher manifest experiment identity mismatch")
    samples = (manifest.get("config") or {}).get("samples")
    if (
        not isinstance(samples, list)
        or not samples
        or not all(isinstance(sample, str) and sample for sample in samples)
        or len(set(samples)) != len(samples)
    ):
        raise RuntimeError("dispatcher manifest has no valid unique sample scope")

    evaluator_cache = cache_dir / f"{version_root.name}_evaluator.json"
    evaluator_command = [
        sys.executable,
        "-B",
        str(ROOT / "tools" / "evaluator_runtime_preflight.py"),
        "--samples",
        *samples,
        "--timeout",
        str(args.evaluator_timeout),
        "--jobs",
        str(args.evaluator_jobs),
        "--output",
        str(evaluator_cache),
        "--reuse-output",
        "--quiet",
    ]
    evaluator_result = run_checked(evaluator_command, cwd=ROOT)
    evaluator_report = read_json(evaluator_cache)
    if (
        evaluator_report.get("selection") != samples
        or not (evaluator_report.get("summary") or {}).get("passed")
    ):
        raise RuntimeError("evaluator preflight report does not pass manifest scope")

    receipt = {
        "schema": SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "experiment_id": experiment.name,
        "owner": version_root.name,
        "zero_api": True,
        "provider_posts": 0,
        "status": "ready_for_key_probe_and_final_command_review",
        "plan": {
            "path": repo_ref(plan),
            "sha256": sha256_file(plan),
        },
        "manifest": {
            "path": repo_ref(manifest_path),
            "sha256": sha256_file(manifest_path),
            "run_git_commit": manifest.get("run_git_commit"),
            "git_tree_state": manifest.get("git_tree_state"),
            "sample_ids": samples,
            "sample_count": len(samples),
        },
        "regression": {
            "cache_path": repo_ref(regression_cache),
            "cache_sha256": sha256_file(regression_cache),
            "cache_hit": regression_cached,
            "regression_code_sha256": regression["regression_code_sha256"],
            "results": regression["results"],
        },
        "dispatcher_dry_run": dispatcher_result,
        "evaluator_preflight": {
            "cache_path": repo_ref(evaluator_cache),
            "cache_sha256": sha256_file(evaluator_cache),
            "input_fingerprints": evaluator_report["input_fingerprints"],
            "execution": evaluator_report["execution"],
            "summary": evaluator_report["summary"],
            "command": evaluator_result["command"],
        },
        "key_probe": {
            "status": "required_separate_paid_step_not_run",
            "reason": "Key liveness is a real provider request, not zero-API preflight.",
        },
    }
    receipt_path = experiment / "preflight" / "preflight_receipt.json"
    write_atomic(receipt_path, stable_json(receipt))
    print(f"[preflight] PASS receipt={repo_ref(receipt_path)}", flush=True)
    print("[preflight] NEXT: run the separate Key probe, then review launch command")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"[preflight] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
