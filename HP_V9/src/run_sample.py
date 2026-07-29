"""One-process, one-sample worker for the simplified V9 campaign runtime."""
import argparse
import os
from pathlib import Path
import signal
import traceback

from relay_core import (EvaluatorIncompleteError, PreservationViolationError,
                        RelayHooks, run_method)
from simple_api_recorder import SimpleApiRecorder
from simple_runtime_io import (LocalEvidenceError, commit_round_trip,
                               load_resume_state, method_dir, read_json,
                               read_status, sample_dir, sample_lock, utc_now,
                               sha256_json, write_status)


EXIT_COMPLETE = 0
EXIT_API_INCOMPLETE = 10
EXIT_EVALUATOR_FAILED = 11
EXIT_PRESERVATION_INVALID = 12
EXIT_WORKER_FAILED = 13
EXIT_INTERRUPTED = 130


class WorkerInterrupted(RuntimeError):
    pass


def _signal_handler(signum, _frame):
    raise WorkerInterrupted(f"worker received signal {signum}")


def _append_worker_log(path, message):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(f"{utc_now()} {message}\n")
        handle.flush()


def _error_record(exc):
    return {
        "type": f"{type(exc).__module__}.{type(exc).__qualname__}",
        "message": (str(exc) or repr(exc))[:4000],
        "traceback": "".join(traceback.format_exception(exc))[-12000:],
        "api_failure_path": getattr(
            exc, "_anchorpatch_simple_failure_path", None),
        "key_unusable_reason": getattr(
            exc, "_anchorpatch_key_unusable_reason", None),
        "http_status": getattr(exc, "status_code", None),
    }


def _is_api_exception(exc):
    return bool(
        getattr(exc, "_anchorpatch_simple_failure_path", None)
        or hasattr(exc, "transport_attempts")
        or type(exc).__module__.endswith("model_openai")
    )


def run_sample(out_dir, sample, key_label, *, generate_impl=None):
    out_dir = Path(out_dir).resolve()
    run = read_json(out_dir / "run.json")
    scientific = run["scientific"]
    samples_root = Path(scientific["samples_root"])
    if not samples_root.is_absolute():
        samples_root = Path(__file__).resolve().parent.parent / samples_root
    methods = list(scientific["method_order_by_sample"][sample])
    plan_record = scientific["task_plans"][sample]
    plan_payload = read_json(out_dir / plan_record["path"])
    if sha256_json(plan_payload) != plan_record["sha256"]:
        raise LocalEvidenceError(f"task plan SHA mismatch for {sample}")
    task_plan = list(plan_payload["target_state_ids"])
    round_trips = int(scientific["round_trips"])
    seed = int(scientific["seed"])
    include_distractor = bool(scientific["include_distractor"])
    model = scientific["model"]
    max_tokens = scientific["max_tokens"]
    reasoning_effort = scientific["reasoning_effort"]
    log_path = sample_dir(out_dir, sample) / "worker.log"

    with sample_lock(out_dir, sample):
        status = read_status(out_dir, sample, methods)
        status["attempt"] = int(status.get("attempt") or 0) + 1
        status.update(
            state="running", key_label=key_label,
            worker_pid=os.getpid(), started_at=utc_now(),
            finished_at=None, last_error=None)
        write_status(out_dir, sample, status)
        _append_worker_log(
            log_path, f"start sample={sample} key_label={key_label} "
                      f"attempt={status['attempt']}")

        try:
            for method in methods:
                method_path = method_dir(out_dir, sample, method)
                checkpoint, completed = load_resume_state(
                    out_dir, sample, method, round_trips, seed,
                    include_distractor, samples_root)
                if completed >= round_trips:
                    status["methods"][method] = "complete"
                    write_status(out_dir, sample, status)
                    continue
                status["methods"][method] = "running"
                write_status(out_dir, sample, status)
                recorder = SimpleApiRecorder(
                    method_path, sample, method, model,
                    generate_impl=generate_impl)

                def log_event(event):
                    _append_worker_log(
                        log_path,
                        f"commit method={method} rt={event['round_trip']} "
                        f"score={event.get('backward_score')}")

                hooks = RelayHooks(
                    commit_round_trip=lambda rows, ckpt, path=method_path:
                        commit_round_trip(path, rows, ckpt),
                    set_step=recorder.set_step,
                    log=log_event,
                )
                run_method(
                    method, sample, task_plan,
                    num_round_trips=round_trips, seed=seed,
                    include_distractor=include_distractor,
                    model=model, max_tokens=max_tokens,
                    generate_fn=recorder.generate, hooks=hooks,
                    resume_state=checkpoint,
                    reasoning_effort=reasoning_effort,
                    stop_on_preservation_violation=True,
                    samples_root=str(samples_root))
                status["methods"][method] = "complete"
                write_status(out_dir, sample, status)

            status.update(
                state="complete", finished_at=utc_now(), last_error=None)
            write_status(out_dir, sample, status)
            _append_worker_log(log_path, f"complete sample={sample}")
            return EXIT_COMPLETE
        except WorkerInterrupted as exc:
            for method, value in list(status["methods"].items()):
                if value == "running":
                    status["methods"][method] = "incomplete"
            status.update(
                state="pending", finished_at=utc_now(),
                last_error=_error_record(exc))
            write_status(out_dir, sample, status)
            _append_worker_log(log_path, f"interrupted sample={sample}")
            return EXIT_INTERRUPTED
        except EvaluatorIncompleteError as exc:
            status["methods"][exc.method] = "incomplete"
            status.update(
                state="evaluator_failed", finished_at=utc_now(),
                last_error=_error_record(exc))
            write_status(out_dir, sample, status)
            _append_worker_log(log_path, f"evaluator_failed sample={sample}")
            return EXIT_EVALUATOR_FAILED
        except PreservationViolationError as exc:
            status["methods"][exc.method] = "incomplete"
            status.update(
                state="preservation_invalid", finished_at=utc_now(),
                last_error=_error_record(exc))
            write_status(out_dir, sample, status)
            _append_worker_log(log_path, f"preservation_invalid sample={sample}")
            return EXIT_PRESERVATION_INVALID
        except Exception as exc:
            for method, value in list(status["methods"].items()):
                if value == "running":
                    status["methods"][method] = "incomplete"
            state = "api_incomplete" if _is_api_exception(exc) else "worker_failed"
            status.update(
                state=state, finished_at=utc_now(),
                last_error=_error_record(exc))
            write_status(out_dir, sample, status)
            _append_worker_log(log_path, f"{state} sample={sample}")
            return EXIT_API_INCOMPLETE if state == "api_incomplete" else EXIT_WORKER_FAILED


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--sample", required=True)
    parser.add_argument("--key-label", required=True)
    args = parser.parse_args(argv)
    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)
    return run_sample(args.out_dir, args.sample, args.key_label)


if __name__ == "__main__":
    raise SystemExit(main())
