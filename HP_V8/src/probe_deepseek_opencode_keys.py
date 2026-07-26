"""Probe labeled keys against OpenCode Zen DeepSeek V4 Flash.

Each key is tested in an isolated child process.  Only labels and redacted
diagnostics are printed; key values never enter argv or experiment artifacts.
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
_DEFAULT_KEYS_FILE = os.path.join(_ROOT, "..", ".env.frkeys")
_BASE_URL = "https://opencode.ai/zen/go/v1"
_MODEL = "deepseek-v4-flash"

_PROBE_SOURCE = (
    "import json;"
    "from model_openai import generate;"
    "r=generate([{'role':'user','content':'Reply exactly: OK'}],"
    "model='deepseek-v4-flash',max_tokens=__MAX_TOKENS__,timeout=120,"
    "return_metadata=True,reasoning_effort='high',"
    "call_kind='key_probe',max_retries=3);"
    "text=(r.get('message') or '').strip();"
    "print('PROBE_RESULT='+json.dumps({"
    "'alive':bool(r.get('http_status')==200 and r.get('stream_complete') is True "
    "and r.get('input_tokens') is not None and r.get('output_tokens') is not None),"
    "'text_ok':bool(text),"
    "'provider':r.get('provider'),"
    "'transport_revision':r.get('transport_revision'),"
    "'base_url':r.get('base_url'),"
    "'reasoning_effort':r.get('reasoning_effort'),"
    "'stream_complete':r.get('stream_complete'),"
    "'stop_reason':r.get('stop_reason'),"
    "'response_classification':r.get('response_classification'),"
    "'input_tokens':r.get('input_tokens',0) or 0,"
    "'output_tokens':r.get('output_tokens',0) or 0,"
    "'total_tokens':r.get('total_tokens',0) or 0,"
    "'http_attempts_used':r.get('http_attempts_used',0) or 0,"
    "'retry_count':r.get('retry_count',0) or 0}))"
)


def probe_key(label, value, timeout, max_tokens=512):
    """Return liveness, redacted detail, elapsed time, usage, and metadata."""
    del label
    python_path = os.pathsep.join(
        item for item in (_HERE, _ROOT, os.environ.get("PYTHONPATH"))
        if item
    )
    env = dict(os.environ)
    for name in (
            "OPENCODE_API_KEY", "OPENCODE_GO_API_KEY",
            "OPENCODE_TRANSPORT", "MINIMAX_API_KEY", "MINIMAX_TRANSPORT"):
        env.pop(name, None)
    env.update({
        "OPENAI_API_KEY": value,
        "OPENAI_BASE_URL": _BASE_URL,
        "PYTHONUTF8": "1",
        "PYTHONPATH": python_path,
    })
    started = time.monotonic()
    usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    result = {}
    try:
        completed = subprocess.run(
            [
                sys.executable, "-c",
                _PROBE_SOURCE.replace(
                    "__MAX_TOKENS__", str(int(max_tokens))),
            ],
            cwd=_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = "\n".join(
            item.strip()
            for item in (completed.stdout, completed.stderr)
            if item.strip()
        )
        result_line = next(
            (
                line for line in reversed(output.splitlines())
                if line.startswith("PROBE_RESULT=")
            ),
            None,
        )
        result = (
            json.loads(result_line.split("=", 1)[1])
            if result_line else {}
        )
        for field in usage:
            usage[field] = int(result.get(field) or 0)
        identity_ok = (
            result.get("provider") == "opencode_zen"
            and result.get("transport_revision")
            == "opencode_openai_compatible/2"
            and result.get("base_url") == _BASE_URL
            and result.get("reasoning_effort") == "high"
        )
        passed = (
            completed.returncode == 0
            and bool(result.get("alive"))
            and identity_ok
        )
        if passed and result.get("text_ok"):
            detail = "complete text response"
        elif passed:
            detail = (
                "provider alive; complete response had no text "
                f"({result.get('response_classification') or 'model_empty'})"
            )
        elif completed.returncode == 0 and result_line:
            detail = "response or audited runtime identity was incomplete"
        else:
            detail = output[-300:] or f"child exit code {completed.returncode}"
    except subprocess.TimeoutExpired:
        passed = False
        detail = f"timed out after {timeout}s"
    detail = detail.replace(value, "<redacted-key>")
    return passed, detail, time.monotonic() - started, usage, result


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Probe labeled keys on OpenCode Zen DeepSeek V4 Flash with "
            "reasoning_effort=high."
        )
    )
    parser.add_argument("--keys_file", default=_DEFAULT_KEYS_FILE)
    parser.add_argument("--key_labels", nargs="+")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--max_tokens", type=int, default=512)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("--timeout must be >= 1")
    if not 1 <= args.max_tokens <= 20000:
        parser.error("--max_tokens must be in 1..20000")

    all_keys = dict(read_keys(os.path.abspath(args.keys_file)))
    labels = list(args.key_labels or sorted(all_keys))
    if len(labels) != len(set(labels)):
        parser.error("--key_labels contains duplicates")
    missing = [label for label in labels if label not in all_keys]
    if missing:
        parser.error(f"unknown key labels: {missing}")

    print(
        f"Testing {len(labels)} labeled keys on {_MODEL} via OpenCode Zen "
        f"(reasoning_effort=high, max_tokens={args.max_tokens})...",
        flush=True,
    )
    alive_labels = []
    failures = []
    totals = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    for label in labels:
        passed, detail, elapsed, usage, result = probe_key(
            label, all_keys[label], args.timeout, args.max_tokens)
        for field in totals:
            totals[field] += usage[field]
        status = (
            "ALIVE" if passed and result.get("text_ok")
            else "ALIVE_NO_TEXT" if passed else "FAILED"
        )
        print(
            f"  {label}: {status} ({elapsed:.1f}s) - {detail}; "
            f"stream={result.get('stream_complete')} "
            f"stop={result.get('stop_reason')} "
            f"attempts={result.get('http_attempts_used')} "
            f"retries={result.get('retry_count')} "
            f"tokens={usage['input_tokens']}/"
            f"{usage['output_tokens']}/{usage['total_tokens']}",
            flush=True,
        )
        if passed:
            alive_labels.append(label)
        else:
            failures.append(label)
    print(
        f"Summary: {len(alive_labels)}/{len(labels)} alive; "
        f"alive_labels={','.join(alive_labels) or '-'}; "
        f"failed_labels={','.join(failures) or '-'}; "
        "tokens input/output/total="
        f"{totals['input_tokens']}/{totals['output_tokens']}/"
        f"{totals['total_tokens']}",
        flush=True,
    )
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
