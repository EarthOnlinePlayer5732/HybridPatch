"""Bounded one-key concurrency diagnostic for OpenCode Zen DeepSeek.

This is not a method experiment.  It sends the same short prompt from isolated
child processes and preserves every sanitized transport event so 5xx response
bodies remain available even when a child reaches its wall-clock limit.
"""

import argparse
from collections import Counter
import json
import os
import subprocess
import sys
import time

from fr_baseline_dispatch import read_keys
from model_openai import generate
from run_meta import write_json_atomic


BASE_URL = "https://opencode.ai/zen/go/v1"
MODEL = "deepseek-v4-flash"


def _redact(value, secret):
    text = json.dumps(
        value, ensure_ascii=False, default=str, separators=(",", ":"))
    if secret:
        text = text.replace(secret, "<redacted-key>")
    return json.loads(text)


def _child(args):
    secret = os.environ.get("OPENAI_API_KEY")
    if not secret:
        raise RuntimeError("OPENAI_API_KEY is missing")
    event_path = os.path.abspath(args.event_path)
    result_path = os.path.abspath(args.result_path)
    os.makedirs(os.path.dirname(event_path), exist_ok=True)
    with open(event_path, "x", encoding="utf-8", newline="") as event_fh:
        def sink(payload):
            event_fh.write(
                json.dumps(
                    _redact(payload, secret),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ) + "\n"
            )
            event_fh.flush()
            os.fsync(event_fh.fileno())

        sink._anchorpatch_critical = True
        started = time.monotonic()
        try:
            result = generate(
                [{"role": "user", "content": "Reply with exactly: OK"}],
                model=MODEL,
                max_tokens=args.max_tokens,
                timeout=args.request_timeout,
                max_retries=3,
                return_metadata=True,
                reasoning_effort="high",
                call_kind="concurrency_diagnostic",
                _raw_event_sink=sink,
            )
            record = {
                "status": "completed",
                "elapsed_seconds": time.monotonic() - started,
                "http_status": result.get("http_status"),
                "stream_complete": result.get("stream_complete"),
                "finish_reason": result.get("finish_reason"),
                "response_classification": result.get(
                    "response_classification"),
                "provider": result.get("provider"),
                "transport": result.get("transport"),
                "transport_revision": result.get("transport_revision"),
                "base_url": result.get("base_url"),
                "request_url": result.get("request_url"),
                "reasoning_effort": result.get("reasoning_effort"),
                "input_tokens": result.get("input_tokens"),
                "output_tokens": result.get("output_tokens"),
                "total_tokens": result.get("total_tokens"),
                "http_attempts_used": result.get("http_attempts_used"),
                "retry_count": result.get("retry_count"),
                "transport_attempts": result.get("transport_attempts") or [],
            }
        except BaseException as exc:
            record = {
                "status": "failed",
                "elapsed_seconds": time.monotonic() - started,
                "exception_type": (
                    f"{type(exc).__module__}.{type(exc).__qualname__}"),
                "error_message": str(exc),
                "http_status": getattr(exc, "status_code", None),
                "transport_attempts": (
                    getattr(exc, "transport_attempts", None) or []),
            }
        write_json_atomic(result_path, _redact(record, secret))
    return 0 if record["status"] == "completed" else 1


def _read_json(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        value = json.load(handle)
    return value if isinstance(value, dict) else None


def _attempts_from_events(path):
    attempts = []
    if not os.path.exists(path):
        return attempts
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("record_type") == "attempt_end":
                attempt = event.get("attempt")
                if isinstance(attempt, dict):
                    attempts.append(attempt)
    return attempts


def _parent(args):
    if not 1 <= args.concurrency <= 15:
        raise RuntimeError("--concurrency must be in 1..15")
    if not 1 <= args.max_tokens <= 20000:
        raise RuntimeError("--max_tokens must be in 1..20000")
    if args.call_wall_timeout < 1 or args.request_timeout < 1:
        raise RuntimeError("timeouts must be positive")

    keys = dict(read_keys(os.path.abspath(args.keys_file)))
    if args.key_label not in keys:
        raise RuntimeError(f"unknown key label: {args.key_label}")
    secret = keys[args.key_label]
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=False)

    env = dict(os.environ)
    for name in (
            "OPENCODE_API_KEY", "OPENCODE_GO_API_KEY",
            "OPENCODE_TRANSPORT", "MINIMAX_API_KEY", "MINIMAX_TRANSPORT",
            "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT"):
        env.pop(name, None)
    env.update({
        "OPENAI_API_KEY": secret,
        "OPENAI_BASE_URL": BASE_URL,
        "PYTHONUTF8": "1",
    })

    workers = []
    for index in range(1, args.concurrency + 1):
        stem = f"call-{index:02d}"
        event_path = os.path.join(out_dir, stem + ".transport.jsonl")
        result_path = os.path.join(out_dir, stem + ".result.json")
        console_path = os.path.join(out_dir, stem + ".console.log")
        console_fh = open(console_path, "x", encoding="utf-8", newline="")
        command = [
            sys.executable, "-B", os.path.abspath(__file__),
            "--child",
            "--event_path", event_path,
            "--result_path", result_path,
            "--max_tokens", str(args.max_tokens),
            "--request_timeout", str(args.request_timeout),
        ]
        process = subprocess.Popen(
            command,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            env=env,
            stdout=console_fh,
            stderr=subprocess.STDOUT,
            text=True,
        )
        workers.append({
            "index": index,
            "process": process,
            "started": time.monotonic(),
            "events": event_path,
            "result": result_path,
            "console": console_path,
            "console_fh": console_fh,
            "timed_out": False,
        })

    pending = set(range(len(workers)))
    while pending:
        for worker_index in list(pending):
            worker = workers[worker_index]
            process = worker["process"]
            if process.poll() is not None:
                worker["console_fh"].close()
                pending.remove(worker_index)
                continue
            if (time.monotonic() - worker["started"]
                    >= args.call_wall_timeout):
                worker["timed_out"] = True
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                worker["console_fh"].close()
                pending.remove(worker_index)
        if pending:
            time.sleep(0.2)

    status_counts = Counter()
    http_counts = Counter()
    unique_error_bodies = {}
    call_rows = []
    for worker in workers:
        result = _read_json(worker["result"])
        attempts = (
            (result or {}).get("transport_attempts")
            or _attempts_from_events(worker["events"])
        )
        for attempt in attempts:
            status = attempt.get("http_status")
            if isinstance(status, int):
                http_counts[str(status)] += 1
            body = attempt.get("error_body")
            if body is not None:
                canonical = json.dumps(
                    body, ensure_ascii=False, sort_keys=True, default=str)
                unique_error_bodies[canonical] = body
        status = (
            "timed_out" if worker["timed_out"]
            else (result or {}).get("status") or "missing_result"
        )
        status_counts[status] += 1
        call_rows.append({
            "call_index": worker["index"],
            "status": status,
            "returncode": worker["process"].returncode,
            "result_path": os.path.relpath(
                worker["result"], out_dir).replace("\\", "/"),
            "transport_path": os.path.relpath(
                worker["events"], out_dir).replace("\\", "/"),
            "console_path": os.path.relpath(
                worker["console"], out_dir).replace("\\", "/"),
            "attempt_count": len(attempts),
        })

    summary = {
        "schema": "anchorpatch.deepseek_concurrency_diagnostic/1",
        "key_label": args.key_label,
        "concurrency": args.concurrency,
        "model": MODEL,
        "base_url": BASE_URL,
        "reasoning_effort": "high",
        "max_tokens": args.max_tokens,
        "request_timeout": args.request_timeout,
        "call_wall_timeout": args.call_wall_timeout,
        "status_counts": dict(sorted(status_counts.items())),
        "http_attempt_counts": dict(sorted(http_counts.items())),
        "unique_error_bodies": list(unique_error_bodies.values()),
        "calls": call_rows,
    }
    write_json_atomic(os.path.join(out_dir, "summary.json"), summary)
    print(json.dumps({
        "out_dir": out_dir,
        "status_counts": summary["status_counts"],
        "http_attempt_counts": summary["http_attempt_counts"],
        "unique_error_body_count": len(summary["unique_error_bodies"]),
    }, ensure_ascii=False))
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keys_file")
    parser.add_argument("--key_label")
    parser.add_argument("--out_dir")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--max_tokens", type=int, default=256)
    parser.add_argument("--request_timeout", type=int, default=120)
    parser.add_argument("--call_wall_timeout", type=int, default=240)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--event_path", help=argparse.SUPPRESS)
    parser.add_argument("--result_path", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        if not args.event_path or not args.result_path:
            parser.error("child paths are required")
        return _child(args)
    for field in ("keys_file", "key_label", "out_dir"):
        if not getattr(args, field):
            parser.error(f"--{field} is required")
    return _parent(args)


if __name__ == "__main__":
    raise SystemExit(main())
