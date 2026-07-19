"""Run zero-API, per-sample evaluator runtime preflight for the active HP version.

Each sample is evaluated in a fresh subprocess through the same initial-state
path used by ``experiment_runner``.  The parent enforces a wall-clock timeout;
the worker removes credential variables, replaces model-call entry points with
hard failures, and verifies that the sample tree was not modified.

Examples (from the repository root, in Git Bash):

    PYTHONUTF8=1 python -B tools/evaluator_runtime_preflight.py --all
    PYTHONUTF8=1 python -B tools/evaluator_runtime_preflight.py \
      --samples audiosyn1 obj3d5 --timeout 120
    PYTHONUTF8=1 python -B tools/evaluator_runtime_preflight.py \
      --all --output HP_V8/analysis/evaluator_runtime_preflight.json

Without ``--output``, the full aggregate result is emitted as JSON on stdout.
With ``--output``, the full report is written atomically and stdout contains a
compact JSON summary. Progress is written only to stderr.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import math
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
VERSION_STATES = ROOT / "tools" / "version_states.json"
SCHEMA = "hybridpatch.evaluator_runtime_preflight/1"
WORKER_SCHEMA = "hybridpatch.evaluator_runtime_preflight_worker/1"
SUMMARY_SCHEMA = "hybridpatch.evaluator_runtime_preflight_summary/1"
AUDIT_EVIDENCE_VERSION = "HP_V8"

# Remove every credential name used by the current provider implementations.
# Values are never read or included in output.
CREDENTIAL_ENV_VARS = (
    "OPENCODE_API_KEY",
    "OPENCODE_GO_API_KEY",
    "MINIMAX_API_KEY",
    "OPENAI_API_KEY",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "ANTHROPIC_API_KEY",
    "DEEPSEEK_API_KEY",
    "DEEPINFRA_API_KEY",
    "OPENROUTER_API_KEY",
)
API_BACKED_EVALUATOR_TYPES = {"fiction"}
NON_TEXT_EDIT_TYPES = {"audio", "image"}
KNOWN_NONUNIT_SELF_SCORES = {
    "makefile1": 0.95,
    "makefile2": 0.95,
    "makefile3": 0.95,
    "makefile4": 0.95,
    "makefile6": 0.95,
    "obj3d5": 0.9129,
    "transit3": 0.9941747572815534,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--all",
        action="store_true",
        help="Run every sample currently present under the active version's data junction.",
    )
    selection.add_argument(
        "--samples",
        nargs="+",
        metavar="SAMPLE_ID",
        help="Run only the listed sample IDs, preserving the requested order.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Per-sample worker timeout in seconds (default: 120).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress messages on stderr; JSON stdout is unchanged.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Atomically write the full JSON report to this path. When set, "
            "stdout contains only a compact JSON summary."
        ),
    )
    args = parser.parse_args(argv)
    if args.timeout <= 0 or not math.isfinite(args.timeout):
        parser.error("--timeout must be a finite positive number")
    return args


def active_version_root() -> Path:
    states = json.loads(VERSION_STATES.read_text(encoding="utf-8"))
    if states.get("schema") != "hybridpatch.version_states/1":
        raise RuntimeError("unsupported tools/version_states.json schema")
    active = states.get("active_version")
    if not isinstance(active, str) or not active.startswith("HP_V"):
        raise RuntimeError("version_states.json has no valid active_version")
    if (states.get("versions") or {}).get(active) != "active":
        raise RuntimeError(f"version_states.json does not mark {active} active")
    version_root = (ROOT / active).resolve()
    if not version_root.is_dir():
        raise RuntimeError(f"active version directory is missing: {version_root}")
    if not (version_root / "src" / "experiment_runner.py").is_file():
        raise RuntimeError(f"active version has no experiment runner: {version_root}")
    return version_root


def samples_root(version_root: Path) -> Path:
    root = version_root / "data" / "samples_delegate52"
    if not root.is_dir():
        raise RuntimeError(f"sample root is missing: {root}")
    return root


def available_samples(version_root: Path) -> list[str]:
    root = samples_root(version_root)
    return sorted(
        path.parent.name
        for path in root.glob("*/sample.json")
        if path.is_file()
    )


def select_samples(
    version_root: Path,
    *,
    run_all: bool,
    requested: Iterable[str] | None,
) -> list[str]:
    available = available_samples(version_root)
    if run_all:
        return available
    selected = list(dict.fromkeys(requested or []))
    missing = [sample for sample in selected if sample not in set(available)]
    if missing:
        raise RuntimeError("sample(s) not found: " + ", ".join(missing))
    return selected


def _sample_type(version_root: Path, sample_id: str) -> str | None:
    path = samples_root(version_root) / sample_id / "sample.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8")).get("sample_type")
    except Exception:
        return None
    return value if isinstance(value, str) else None


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def classify_evaluation(evaluation: Any) -> dict[str, Any]:
    if not isinstance(evaluation, dict):
        return {
            "status": "invalid_result",
            "score": None,
            "self_score_one": False,
            "error": "evaluate_context returned a non-object result",
            "evaluation_keys": [],
        }
    keys = sorted(str(key) for key in evaluation)
    if "error" in evaluation:
        error_score = _finite_float(evaluation.get("score"))
        return {
            "status": "invalid_result",
            "score": error_score,
            "self_score_one": False,
            "error": str(evaluation.get("error")),
            "detailed_error": (
                str(evaluation.get("detailed_error"))
                if evaluation.get("detailed_error") is not None
                else None
            ),
            "evaluation_keys": keys,
        }
    score = _finite_float(evaluation.get("score"))
    valid_score = score is not None and 0.0 <= score <= 1.0
    if not valid_score:
        return {
            "status": "invalid_result",
            "score": None,
            "self_score_one": False,
            "error": "evaluate_context did not return a finite score in [0, 1]",
            "evaluation_keys": keys,
        }
    return {
        "status": "runtime_runnable",
        "score": score,
        "self_score_one": math.isclose(score, 1.0, rel_tol=0.0, abs_tol=1e-12),
        "error": None,
        "evaluation_keys": keys,
    }


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _clear_credentials(environment: dict[str, str] | None = None) -> None:
    target = os.environ if environment is None else environment
    for name in CREDENTIAL_ENV_VARS:
        target.pop(name, None)


def _zero_api_guard(*_args: Any, **_kwargs: Any) -> Any:
    raise RuntimeError("ZERO_API_GUARD: evaluator attempted a model API call")


def evaluate_sample_worker(version_root: Path, sample_id: str) -> dict[str, Any]:
    started = time.monotonic()
    version_root = version_root.resolve()
    sample_dir = samples_root(version_root) / sample_id
    sample_path = sample_dir / "sample.json"
    if not sample_path.is_file():
        return {
            "schema": WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": None,
            "status": "sample_missing",
            "score": None,
            "self_score_one": False,
            "error": "sample.json is missing",
            "duration_seconds": round(time.monotonic() - started, 6),
        }

    metadata = json.loads(sample_path.read_text(encoding="utf-8"))
    sample_type = metadata.get("sample_type")
    if sample_type in API_BACKED_EVALUATOR_TYPES:
        return {
            "schema": WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": sample_type,
            "status": "unsupported_zero_api",
            "score": None,
            "self_score_one": False,
            "error": "fiction evaluate_context uses an LLM judge",
            "duration_seconds": round(time.monotonic() - started, 6),
        }
    if sample_type in NON_TEXT_EDIT_TYPES:
        return {
            "schema": WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": sample_type,
            "status": "unsupported_campaign_type",
            "score": None,
            "self_score_one": False,
            "error": "image/audio editing is unsupported by the text runner",
            "duration_seconds": round(time.monotonic() - started, 6),
        }

    before_digest = _tree_digest(sample_dir)
    before_tmp = {path.name for path in version_root.glob("tmp_eval_*")}
    captured = io.StringIO()
    evaluation: Any = None
    result: dict[str, Any]
    try:
        os.chdir(version_root)
        src = str(version_root / "src")
        if src not in sys.path:
            sys.path.insert(0, src)
        _clear_credentials()
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            import experiment_runner as runner
            import model_openai
            import domains.domain_base as domain_base
            import domains.domain_fiction as domain_fiction
            from domains import get_domain
            from utils_context import build_context_from_folder
            from utils_env import load_sample

            # load_dotenv runs while importing model_openai. Clear again, then
            # replace every model entry point reachable from the runner/domains.
            _clear_credentials()
            model_openai.generate = _zero_api_guard
            model_openai.generate_json = _zero_api_guard
            domain_base.generate = _zero_api_guard
            domain_fiction.generate_json = _zero_api_guard
            runner._real_generate = _zero_api_guard

            sample, folder, states = load_sample(
                sample_id,
                samples_folder=str(samples_root(version_root)) + os.sep,
            )
            initial = states[sample["start_state"]]
            context = build_context_from_folder(
                os.path.join(folder, initial["solution_folder"])
            )
            domain = get_domain(sample["sample_type"])
            domain.samples_folder = str(samples_root(version_root)) + os.sep
            evaluation = runner._evaluate(
                domain,
                sample_id,
                context,
                initial,
                list(initial["context"]),
            )
        result = classify_evaluation(evaluation)
    except Exception as exc:
        result = {
            "status": "exception",
            "score": None,
            "self_score_one": False,
            "error": f"{type(exc).__name__}: {exc}",
            "evaluation_keys": [],
        }

    after_digest = _tree_digest(sample_dir)
    new_tmp = sorted(
        path.name
        for path in version_root.glob("tmp_eval_*")
        if path.name not in before_tmp
    )
    if before_digest != after_digest:
        result.update(
            status="data_modified",
            self_score_one=False,
            error="sample tree changed during evaluator preflight",
        )
    elif new_tmp:
        result.update(
            status="temporary_artifact_leak",
            self_score_one=False,
            error="evaluator left tmp_eval_* directories behind",
        )

    payload = {
        "schema": WORKER_SCHEMA,
        "sample_id": sample_id,
        "sample_type": sample_type,
        **result,
        "sample_tree_unchanged": before_digest == after_digest,
        "new_tmp_eval_artifacts": new_tmp,
        "api_guard_active": True,
        "duration_seconds": round(time.monotonic() - started, 6),
    }
    if payload["status"] != "runtime_runnable" and captured.getvalue():
        payload["captured_output_tail"] = captured.getvalue()[-2000:]
    return payload


def _worker_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--_worker", action="store_true")
    parser.add_argument("--version-root", required=True, type=Path)
    parser.add_argument("--sample", required=True)
    args = parser.parse_args(argv)
    try:
        payload = evaluate_sample_worker(args.version_root, args.sample)
    except Exception as exc:
        payload = {
            "schema": WORKER_SCHEMA,
            "sample_id": args.sample,
            "sample_type": None,
            "status": "worker_exception",
            "score": None,
            "self_score_one": False,
            "error": f"{type(exc).__name__}: {exc}",
            "api_guard_active": True,
        }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload.get("status") == "runtime_runnable" else 1


def run_sample(version_root: Path, sample_id: str, timeout: float) -> dict[str, Any]:
    command = [
        sys.executable,
        "-B",
        str(Path(__file__).resolve()),
        "--_worker",
        "--version-root",
        str(version_root),
        "--sample",
        sample_id,
    ]
    environment = dict(os.environ)
    _clear_credentials(environment)
    environment.update(
        PYTHONUTF8="1",
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=str(version_root / "src"),
        ANCHORPATCH_EVALUATOR_PREFLIGHT="1",
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=version_root,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "schema": WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": _sample_type(version_root, sample_id),
            "status": "timeout",
            "score": None,
            "self_score_one": False,
            "error": f"worker exceeded {timeout:g}s timeout",
            "api_guard_active": True,
            "wall_seconds": round(time.monotonic() - started, 6),
        }

    lines = [line for line in (completed.stdout or "").splitlines() if line.strip()]
    try:
        payload = json.loads(lines[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        return {
            "schema": WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": _sample_type(version_root, sample_id),
            "status": "worker_protocol_error",
            "score": None,
            "self_score_one": False,
            "error": f"worker emitted no valid JSON: {exc}",
            "worker_exit_code": completed.returncode,
            "stdout_tail": (completed.stdout or "")[-2000:],
            "stderr_tail": (completed.stderr or "")[-2000:],
            "api_guard_active": True,
            "wall_seconds": round(time.monotonic() - started, 6),
        }
    if payload.get("sample_id") != sample_id or payload.get("schema") != WORKER_SCHEMA:
        payload = {
            "schema": WORKER_SCHEMA,
            "sample_id": sample_id,
            "sample_type": _sample_type(version_root, sample_id),
            "status": "worker_protocol_error",
            "score": None,
            "self_score_one": False,
            "error": "worker JSON identity/schema mismatch",
            "api_guard_active": True,
        }
    payload["worker_exit_code"] = completed.returncode
    payload["wall_seconds"] = round(time.monotonic() - started, 6)
    if payload.get("status") != "runtime_runnable" and completed.stderr:
        payload["worker_stderr_tail"] = completed.stderr[-2000:]
    return payload


def _relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _logical_relative(path: Path) -> str:
    absolute = path.absolute()
    try:
        return absolute.relative_to(ROOT.absolute()).as_posix()
    except ValueError:
        return str(absolute)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def resolve_output_path(output: Path, version_root: Path) -> Path:
    resolved = output.expanduser().resolve()
    data_root = (version_root / "data").resolve()
    if _is_relative_to(resolved, data_root):
        raise RuntimeError(f"--output must not write under {version_root.name}/data")
    return resolved


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(descriptor)
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise


def build_report(
    version_root: Path,
    selected: list[str],
    results: list[dict[str, Any]],
    timeout: float,
    preexisting_tmp: list[str],
    postexisting_tmp: list[str],
) -> dict[str, Any]:
    counts = Counter(str(result.get("status")) for result in results)
    runnable = [r for r in results if r.get("status") == "runtime_runnable"]
    nonunit = [r for r in runnable if not r.get("self_score_one")]
    passed = len(runnable) == len(selected) and not preexisting_tmp and not postexisting_tmp
    return {
        "schema": SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "active_version": version_root.name,
        "version_root": _relative(version_root),
        "samples_root": _logical_relative(samples_root(version_root)),
        "samples_root_resolved": _relative(samples_root(version_root)),
        "selection": selected,
        "timeout_seconds": timeout,
        "api_guard": {
            "credential_environment_removed": list(CREDENTIAL_ENV_VARS),
            "model_entry_points_blocked": True,
            "api_backed_evaluator_types_rejected": sorted(API_BACKED_EVALUATOR_TYPES),
            "non_text_edit_types_rejected": sorted(NON_TEXT_EDIT_TYPES),
        },
        "caveats": [
            {
                "id": "runtime_runnable_is_not_campaign_completeness",
                "detail": (
                    "This preflight evaluates only each sample's exact initial solution "
                    "through the runner evaluator. It does not exercise relay transitions, "
                    "model generation, paid API transport, or campaign completeness."
                ),
            },
            {
                "id": "self_score_is_not_a_universal_one_point_oracle",
                "detail": (
                    "A finite score in [0, 1] establishes runtime execution. Some correct "
                    "initial states have evaluator/reference ceilings below 1.0, so "
                    "self_score_one is reported separately and is not the runnable criterion."
                ),
                "observed_in_version": AUDIT_EVIDENCE_VERSION,
                "observed_nonunit_initial_self_scores": KNOWN_NONUNIT_SELF_SCORES,
            },
        ],
        "known_risks": [
            {
                "id": "python2_7_negative_control_nondiscriminative",
                "observed_in_version": AUDIT_EVIDENCE_VERSION,
                "affected_samples": [f"python{index}" for index in range(2, 8)],
                "detail": (
                    "A zero-API invalid-code negative control still scored 1.0: these "
                    "sample testing scaffolds import copied basic_state modules instead of "
                    "the generated files at the sandbox root. Runtime-runnable therefore "
                    "does not establish evaluator discriminative validity for python2-python7."
                ),
            },
            {
                "id": "python1_uses_locally_rebuilt_evaluator_dependency",
                "observed_in_version": AUDIT_EVIDENCE_VERSION,
                "affected_samples": ["python1"],
                "detail": (
                    "python1 runs only with the locally reconstructed src/utils_eval.py and "
                    "the Windows redirect shim. Its absolute score is not guaranteed to match "
                    "Microsoft's unavailable internal implementation, and frozen FullRewrite "
                    "policy excludes python1 from the canonical 233-sample baseline."
                ),
            },
            {
                "id": "api_guard_is_process_level_not_network_sandbox",
                "detail": (
                    "The worker removes known credential variables and blocks every current "
                    "model entry point reachable from the runner/domains. It is not an OS-level "
                    "network sandbox for arbitrary evaluator subprocesses."
                ),
            },
        ],
        "preexisting_tmp_eval_artifacts": preexisting_tmp,
        "postexisting_tmp_eval_artifacts": postexisting_tmp,
        "summary": {
            "passed": passed,
            "requested": len(selected),
            "runtime_runnable": len(runnable),
            "failed": len(selected) - len(runnable),
            "self_score_one": len(runnable) - len(nonunit),
            "self_score_nonunit": len(nonunit),
            "nonunit_samples": {
                str(result["sample_id"]): result.get("score") for result in nonunit
            },
            "status_counts": dict(sorted(counts.items())),
        },
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    version_root = active_version_root()
    selected = select_samples(
        version_root,
        run_all=args.all,
        requested=args.samples,
    )
    output_path = (
        resolve_output_path(args.output, version_root) if args.output is not None else None
    )
    preexisting_tmp = sorted(path.name for path in version_root.glob("tmp_eval_*"))
    results = []
    if preexisting_tmp:
        results = [
            {
                "schema": WORKER_SCHEMA,
                "sample_id": sample_id,
                "sample_type": _sample_type(version_root, sample_id),
                "status": "blocked_preexisting_temp_artifact",
                "score": None,
                "self_score_one": False,
                "error": "preexisting tmp_eval_* artifact requires manual inspection",
                "api_guard_active": True,
            }
            for sample_id in selected
        ]
    else:
        for index, sample_id in enumerate(selected, 1):
            if not args.quiet:
                print(
                    f"[evaluator-preflight] {index}/{len(selected)} {sample_id}",
                    file=sys.stderr,
                    flush=True,
                )
            results.append(run_sample(version_root, sample_id, args.timeout))
    postexisting_tmp = sorted(path.name for path in version_root.glob("tmp_eval_*"))
    report = build_report(
        version_root,
        selected,
        results,
        args.timeout,
        preexisting_tmp,
        postexisting_tmp,
    )
    if output_path is None:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        report["output_path"] = _relative(output_path)
        write_json_atomic(output_path, report)
        print(
            json.dumps(
                {
                    "schema": SUMMARY_SCHEMA,
                    "output_path": report["output_path"],
                    "summary": report["summary"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    return 0 if report["summary"]["passed"] else 1


if __name__ == "__main__":
    if "--_worker" in sys.argv[1:]:
        raise SystemExit(_worker_main(sys.argv[1:]))
    raise SystemExit(main())
