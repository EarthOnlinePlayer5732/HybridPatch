"""Single official entry point for new HP_V9 campaigns and same-command resume."""
import argparse
from collections import defaultdict, deque
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from domains import get_domain
from hybrid_schema import PROTOCOL_V8
from model_openai import model_runtime_config
from simple_runtime_io import (DISPATCH_SCHEMA, RUN_SCHEMA, append_jsonl_owned,
                               all_sample_ids, campaign_lock,
                               create_or_validate_run, git_identity, load_keys,
                               lock_is_held, method_orders, prepare_task_plans,
                               read_json, read_status, utc_now,
                               validate_samples)
from utils_env import load_sample
from verify_campaign import write_quick_summary


_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_REPO_ROOT = _ROOT.parent
_SAMPLES_ROOT = _ROOT / "data" / "samples_delegate52"
_WORKER = _HERE / "run_sample.py"
_MODEL = "deepseek-v4-flash"
_ENDPOINT = "https://opencode.ai/zen/go/v1"
_REQUEST_URL = f"{_ENDPOINT}/chat/completions"
_REASONING_EFFORT = "high"
_MAX_TOKENS = 20000


def _dispatch_event(out_dir, event, **fields):
    append_jsonl_owned(Path(out_dir) / "dispatch.jsonl", {
        "schema": DISPATCH_SCHEMA, "created_at": utc_now(),
        "event": event, **fields,
    })


def _validate_evaluators(samples):
    seen = set()
    for sample in samples:
        loaded, _folder, _states = load_sample(
            sample, samples_folder=str(_SAMPLES_ROOT) + os.sep)
        sample_type = loaded["sample_type"]
        if sample_type not in seen:
            get_domain(sample_type)
            seen.add(sample_type)


def _base_scientific(samples, orders, args, commit, tree_state):
    os.environ["OPENAI_BASE_URL"] = _ENDPOINT
    runtime = model_runtime_config(
        _MODEL, max_tokens=_MAX_TOKENS,
        reasoning_effort=_REASONING_EFFORT)
    if (runtime.get("transport_revision") != "opencode_openai_compatible/6"
            or runtime.get("transport") != "openai_sdk_stream"):
        raise RuntimeError("DeepSeek runtime is not compact streaming transport /6")
    return {
        "samples": list(samples),
        "method_order_by_sample": orders,
        "method_set": sorted(set(args.methods)),
        "round_trips": args.round_trips,
        "seed": args.seed,
        "model": _MODEL,
        "endpoint": _ENDPOINT,
        "request_url": _REQUEST_URL,
        "stream": True,
        "stream_options": {"include_usage": True},
        "reasoning_effort": _REASONING_EFFORT,
        "max_tokens": _MAX_TOKENS,
        "include_distractor": True,
        "protocol": PROTOCOL_V8,
        "prompt_builder": "hybrid_prompt.build_hybrid_prompt",
        "executor": "hybrid_executor.apply_hybrid",
        "transport": runtime.get("transport"),
        "transport_revision": runtime.get("transport_revision"),
        "run_git_commit": commit,
        "git_tree_state": tree_state,
        "samples_root": "data/samples_delegate52",
    }


def _reject_existing_mismatch(out_dir, base):
    path = Path(out_dir) / "run.json"
    if not path.exists():
        return
    run = read_json(path)
    if run.get("schema") != RUN_SCHEMA:
        raise RuntimeError("out_dir is not a simplified V9 campaign")
    prior = dict(run.get("scientific") or {})
    prior.pop("task_plans", None)
    if prior != base:
        raise RuntimeError(
            "existing run.json scientific configuration differs; use a new out_dir")


def _initial_queue(out_dir, samples, orders):
    pending, external = deque(), {}
    terminal = {}
    for sample in samples:
        try:
            status = read_status(out_dir, sample, orders[sample])
        except Exception:
            terminal[sample] = "worker_failed"
            continue
        state = status["state"]
        lock_path = Path(out_dir) / "locks" / f"{sample}.lock"
        held = lock_is_held(lock_path)
        if state == "complete":
            terminal[sample] = state
        elif held:
            external[sample] = status.get("key_label")
        elif state in {"pending", "running", "api_incomplete"}:
            pending.append(sample)
        else:
            terminal[sample] = state
    return pending, external, terminal


def _worker_env(key, key_label):
    env = os.environ.copy()
    env.update({
        "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1",
        "OPENAI_API_KEY": key, "OPENAI_BASE_URL": _ENDPOINT,
        "ANCHORPATCH_KEY_LABEL": key_label,
    })
    return env


def _launch_worker(out_dir, sample, key_label, key):
    command = [
        sys.executable, "-u", "-B", str(_WORKER),
        "--out-dir", str(Path(out_dir).resolve()),
        "--sample", sample, "--key-label", key_label,
    ]
    return subprocess.Popen(
        command, cwd=str(_ROOT), env=_worker_env(key, key_label),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)


def _wait_and_stop(children, grace_seconds):
    for item in children.values():
        if item["process"].poll() is None:
            item["process"].terminate()
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if all(item["process"].poll() is not None for item in children.values()):
            break
        time.sleep(0.1)
    for item in children.values():
        if item["process"].poll() is None:
            item["process"].kill()
    for item in children.values():
        try:
            item["process"].wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def run_campaign(args):
    out_dir = Path(args.out_dir).resolve()
    samples = (
        all_sample_ids(_SAMPLES_ROOT) if args.all
        else validate_samples(args.samples, _SAMPLES_ROOT))
    if args.all and len(samples) != 234:
        raise RuntimeError(f"--all requires exactly 234 samples, found {len(samples)}")
    orders = method_orders(samples, args.methods)
    keys = load_keys(args.keys_file, args.key_labels)
    _validate_evaluators(samples)
    commit, tree_state = git_identity(_REPO_ROOT)
    base = _base_scientific(samples, orders, args, commit, tree_state)

    out_dir.mkdir(parents=True, exist_ok=True)
    with campaign_lock(out_dir):
        run_exists = (out_dir / "run.json").exists()
        if not run_exists:
            foreign = [
                path.name for path in out_dir.iterdir()
                if path.name != ".campaign.lock"]
            if foreign:
                raise RuntimeError(
                    "fresh simplified runtime out_dir is not empty: "
                    + ", ".join(sorted(foreign)[:10]))
        _reject_existing_mismatch(out_dir, base)
        task_plans = prepare_task_plans(
            out_dir, samples, args.round_trips, args.seed, _SAMPLES_ROOT,
            allow_create=not run_exists)
        scientific = {**base, "task_plans": task_plans}
        execution = {
            "keys_file": str(Path(args.keys_file).resolve()),
            "key_labels": list(args.key_labels),
            "slots_per_key": args.slots_per_key,
            "log_level": args.log_level,
        }
        _run, created = create_or_validate_run(out_dir, scientific, execution)
        invocation = uuid.uuid4().hex
        _dispatch_event(
            out_dir, "campaign_started", invocation_id=invocation,
            new_run=created, sample_count=len(samples),
            key_labels=list(args.key_labels), slots_per_key=args.slots_per_key)

        pending, external, terminal = _initial_queue(out_dir, samples, orders)
        running = {}
        quarantined = {}
        per_key = defaultdict(int)
        for label in external.values():
            if label in keys:
                per_key[label] += 1
        interrupted = {"signum": None}
        prior_handlers = {}

        def request_stop(signum, _frame):
            if interrupted["signum"] is None:
                interrupted["signum"] = signum

        for name in ("SIGINT", "SIGTERM"):
            if hasattr(signal, name):
                signum = getattr(signal, name)
                prior_handlers[signum] = signal.signal(signum, request_stop)

        try:
            while pending or running or external:
                if interrupted["signum"] is not None:
                    break

                for sample in list(external):
                    lock_path = out_dir / "locks" / f"{sample}.lock"
                    if lock_is_held(lock_path):
                        continue
                    label = external.pop(sample)
                    if label in keys:
                        per_key[label] = max(0, per_key[label] - 1)
                    try:
                        status = read_status(out_dir, sample, orders[sample])
                    except Exception:
                        terminal[sample] = "worker_failed"
                        continue
                    if status["state"] == "complete":
                        terminal[sample] = "complete"
                    elif status["state"] in {"pending", "running", "api_incomplete"}:
                        key_reason = ((status.get("last_error") or {})
                                      .get("key_unusable_reason"))
                        if (status["state"] == "api_incomplete" and key_reason
                                and label in keys and label not in quarantined):
                            quarantined[label] = key_reason
                            _dispatch_event(
                                out_dir, "key_quarantined",
                                invocation_id=invocation, key_label=label,
                                reason=key_reason, sample=sample,
                                source="external_worker")
                        pending.append(sample)
                    else:
                        terminal[sample] = status["state"]

                for sample, item in list(running.items()):
                    code = item["process"].poll()
                    if code is None:
                        continue
                    del running[sample]
                    per_key[item["key_label"]] -= 1
                    try:
                        status = read_status(out_dir, sample, orders[sample])
                        state = status["state"]
                    except Exception as exc:
                        status = {"last_error": None}
                        state = "worker_failed"
                        _dispatch_event(
                            out_dir, "worker_status_invalid",
                            invocation_id=invocation, sample=sample,
                            key_label=item["key_label"],
                            error_type=(
                                f"{type(exc).__module__}."
                                f"{type(exc).__qualname__}"),
                            error_message=(str(exc) or repr(exc))[:2000])
                    _dispatch_event(
                        out_dir, "worker_exited", invocation_id=invocation,
                        sample=sample, key_label=item["key_label"],
                        pid=item["process"].pid, returncode=code,
                        sample_state=state)
                    key_reason = ((status.get("last_error") or {})
                                  .get("key_unusable_reason"))
                    if state == "api_incomplete" and key_reason:
                        label = item["key_label"]
                        if label not in quarantined:
                            quarantined[label] = key_reason
                            _dispatch_event(
                                out_dir, "key_quarantined",
                                invocation_id=invocation, key_label=label,
                                reason=key_reason, sample=sample)
                        pending.append(sample)
                    else:
                        terminal[sample] = state

                healthy = [
                    label for label in args.key_labels
                    if label not in quarantined]
                launched = True
                while pending and healthy and launched:
                    launched = False
                    for label in healthy:
                        if not pending:
                            break
                        if per_key[label] >= args.slots_per_key:
                            continue
                        sample = pending.popleft()
                        try:
                            process = _launch_worker(
                                out_dir, sample, label, keys[label])
                        except Exception as exc:
                            terminal[sample] = "worker_failed"
                            _dispatch_event(
                                out_dir, "worker_launch_failed",
                                invocation_id=invocation, sample=sample,
                                key_label=label,
                                error_type=(
                                    f"{type(exc).__module__}."
                                    f"{type(exc).__qualname__}"),
                                error_message=(str(exc) or repr(exc))[:2000])
                            launched = True
                            continue
                        running[sample] = {
                            "process": process, "key_label": label}
                        per_key[label] += 1
                        _dispatch_event(
                            out_dir, "worker_started",
                            invocation_id=invocation, sample=sample,
                            key_label=label, pid=process.pid)
                        launched = True

                if pending and not healthy and not running:
                    break
                if pending or running or external:
                    time.sleep(0.2)
        finally:
            for signum, handler in prior_handlers.items():
                signal.signal(signum, handler)

        if interrupted["signum"] is not None:
            _wait_and_stop(running, args.grace_seconds)
            _dispatch_event(
                out_dir, "campaign_interrupted", invocation_id=invocation,
                signal=interrupted["signum"], pending_count=len(pending),
                running_count=len(running), external_count=len(external))
            write_quick_summary(out_dir, "interrupted")
            return 130 if interrupted["signum"] == signal.SIGINT else 143

        summary = write_quick_summary(out_dir, "incomplete")
        if all(item["complete"] for item in summary["samples"].values()):
            summary = write_quick_summary(out_dir)
            state, code = "complete", 0
        else:
            state, code = "incomplete", 2
        _dispatch_event(
            out_dir, "campaign_finished", invocation_id=invocation,
            campaign_state=state, pending_count=len(pending),
            quarantined_keys=sorted(quarantined))
        return code


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--samples", nargs="+")
    selection.add_argument("--all", action="store_true")
    parser.add_argument(
        "--methods", nargs="+", required=True,
        choices=("hybridpatch", "fullrewrite"))
    parser.add_argument("--round-trips", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keys-file", required=True)
    parser.add_argument("--key-labels", nargs="+", required=True)
    parser.add_argument("--slots-per-key", type=int, default=10)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--log-level", choices=("normal", "quiet"), default="normal")
    parser.add_argument("--grace-seconds", type=float, default=20.0)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.round_trips < 1:
        parser.error("--round-trips must be positive")
    if args.slots_per_key < 1:
        parser.error("--slots-per-key must be positive")
    if args.grace_seconds < 0:
        parser.error("--grace-seconds must be non-negative")
    try:
        return run_campaign(args)
    except Exception as exc:
        print(f"run_campaign: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
