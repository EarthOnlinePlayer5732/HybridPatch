"""Send one tiny MiniMax request with every key in ``.env.frkeys``.

This is intentionally separate from ``fr_baseline_dispatch.py`` so checking
key liveness does not create an experiment directory or touch checkpoints.
Only key labels are printed; key values are passed to isolated child processes
through their environment and are redacted from captured error messages.

Run from the ``hybridpatch_clean`` root::

    python src/probe_fr_keys.py
"""

import argparse
import json
import os
import subprocess
import sys
import time

from fr_baseline_dispatch import read_keys


_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_DEFAULT_KEYS_FILE = os.path.join(_ROOT, ".env.frkeys")

_PROBE_SOURCE = (
    "import json;"
    "from model_openai import generate;"
    "r=generate([{'role':'user','content':'Reply exactly: Hello'}],"
    "model='minimax-m3',max_tokens=__MAX_TOKENS__,timeout=120,return_metadata=True,"
    "thinking_mode='adaptive',call_kind='key_probe',max_retries=3);"
    "text=(r.get('message') or '').strip();"
    "print('PROBE_RESULT='+json.dumps({"
    "'alive':bool(r.get('http_status')==200 and r.get('stream_complete') is True "
    "and r.get('input_tokens') is not None and r.get('output_tokens') is not None),"
    "'text_ok':bool(text),"
    "'stream_complete':r.get('stream_complete'),"
    "'stop_reason':r.get('stop_reason'),"
    "'response_classification':r.get('response_classification'),"
    "'input_tokens':r.get('input_tokens',0) or 0,"
    "'cache_read_input_tokens':r.get('cache_read_input_tokens',0) or 0,"
    "'cache_creation_input_tokens':r.get('cache_creation_input_tokens',0) or 0,"
    "'output_tokens':r.get('output_tokens',0) or 0,"
    "'prompt_tokens':r.get('prompt_tokens',0) or 0,"
    "'total_tokens':r.get('total_tokens',0) or 0,"
    "'response_slots_used':r.get('response_slots_used',0) or 0,"
    "'transient_failure_count':r.get('transient_failure_count',0) or 0,"
    "'http_attempts_used':r.get('http_attempts_used',0) or 0}))"
)


def probe_key(label, value, timeout, max_tokens=1024):
    """Return liveness, detail, elapsed time, usage and response diagnostics."""
    python_path = os.pathsep.join(
        p for p in (_HERE, _ROOT, os.environ.get("PYTHONPATH")) if p
    )
    env = dict(
        os.environ,
        OPENCODE_API_KEY=value,
        OPENCODE_TRANSPORT="anthropic_sdk_v2",
        PYTHONUTF8="1",
        PYTHONPATH=python_path,
    )

    started = time.monotonic()
    usage = {
        "input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "prompt_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }
    result = {}
    try:
        completed = subprocess.run(
            [sys.executable, "-c", _PROBE_SOURCE.replace(
                "__MAX_TOKENS__", str(int(max_tokens))
            )],
            cwd=_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = "\n".join(
            part.strip() for part in (completed.stdout, completed.stderr) if part.strip()
        )
        result_line = next(
            (line for line in reversed(output.splitlines())
             if line.startswith("PROBE_RESULT=")),
            None,
        )
        result = json.loads(result_line.split("=", 1)[1]) if result_line else {}
        for field in usage:
            usage[field] = int(result.get(field) or 0)
        passed = completed.returncode == 0 and bool(result.get("alive"))
        if passed and result.get("text_ok"):
            detail = "Hello response received"
        elif passed:
            detail = (
                "key valid but provider returned no text "
                f"({result.get('response_classification') or 'model_empty'})"
            )
        elif completed.returncode == 0 and result_line:
            detail = "provider response was incomplete or usage was unavailable"
        else:
            detail = output[-240:] or f"child exit code {completed.returncode}"
    except subprocess.TimeoutExpired:
        passed = False
        detail = f"timed out after {timeout}s"

    # Defense in depth: never print a key even if a dependency includes it in
    # an exception message.
    detail = detail.replace(value, "<redacted-key>")
    return passed, detail, time.monotonic() - started, usage, result


def main():
    parser = argparse.ArgumentParser(
        description="Send one tiny Hello request with every labeled FR key."
    )
    parser.add_argument(
        "--keys_file",
        default=_DEFAULT_KEYS_FILE,
        help="LABEL=value key file (default: .env.frkeys)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="wall-clock timeout per key in seconds (default: 180)",
    )
    parser.add_argument(
        "--max_tokens", type=int, default=1024,
        help="per-probe MiniMax output budget (default: 1024; ceiling: 131072)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="test only the first N labeled keys",
    )
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("--timeout must be >= 1")
    if not 1 <= args.max_tokens <= 131072:
        parser.error("--max_tokens must be in 1..131072")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be >= 1")

    keys = read_keys(os.path.abspath(args.keys_file))
    if args.limit is not None:
        keys = keys[:args.limit]
    print(
        f"Testing {len(keys)} keys with one adaptive-thinking Hello request each "
        f"(max_tokens={args.max_tokens})..."
    )

    failures = []
    total_usage = {
        "input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "prompt_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }
    for label, value in keys:
        passed, detail, elapsed, usage, result = probe_key(
            label, value, args.timeout, max_tokens=args.max_tokens
        )
        for field in total_usage:
            total_usage[field] += usage[field]
        status = (
            "ALIVE" if passed and result.get("text_ok")
            else "ALIVE_NO_TEXT" if passed else "FAILED"
        )
        print(
            f"  {label}: {status} ({elapsed:.1f}s) - {detail}; "
            f"stream={result.get('stream_complete')} stop={result.get('stop_reason')}; "
            f"response_slots={result.get('response_slots_used')}/2 "
            f"transient_failures={result.get('transient_failure_count')}/3 "
            f"http_attempts={result.get('http_attempts_used')}; "
            "tokens input/cache_read/cache_create/prompt_total/output/total="
            f"{usage['input_tokens']}/{usage['cache_read_input_tokens']}/"
            f"{usage['cache_creation_input_tokens']}/{usage['prompt_tokens']}/"
            f"{usage['output_tokens']}/{usage['total_tokens']}"
        )
        if not passed:
            failures.append(label)

    alive = len(keys) - len(failures)
    print(f"Summary: {alive}/{len(keys)} alive, {len(failures)} failed")
    print(
        "Token summary input/cache_read/cache_create/prompt_total/output/total="
        f"{total_usage['input_tokens']}/{total_usage['cache_read_input_tokens']}/"
        f"{total_usage['cache_creation_input_tokens']}/{total_usage['prompt_tokens']}/"
        f"{total_usage['output_tokens']}/{total_usage['total_tokens']}"
    )
    if failures:
        print("Failed labels: " + ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
