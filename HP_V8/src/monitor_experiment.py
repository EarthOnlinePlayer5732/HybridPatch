"""Read-only local monitor for HybridPatch/FullRewrite experiment progress.

Progress is derived from committed checkpoints plus the newest transport stream;
it never treats a live process or a partial API response as a completed RT.
"""

import argparse
import json
import os
from pathlib import Path
import re
import time


_TRANSPORT_NAME = re.compile(
    r"rt(?P<rt>\d+|NA)_(?P<direction>forward|backward|unknown)_.*\.transport\.jsonl$"
)


def _read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _read_jsonl(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        pass
    return rows


def _tail_json(path, block_size=8192):
    """Read only the final JSONL record, even for multi-hundred-MB stream logs."""
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            position = handle.tell()
            data = b""
            while position > 0:
                take = min(block_size, position)
                position -= take
                handle.seek(position)
                data = handle.read(take) + data
                lines = [line for line in data.splitlines() if line.strip()]
                if len(lines) >= 2 or position == 0:
                    return json.loads(lines[-1].decode("utf-8", errors="replace"))
    except (OSError, ValueError, IndexError):
        pass
    return {}


def _target_round_trips(out_dir, method, fallback=10):
    rows = _read_jsonl(Path(out_dir) / "run_metadata.jsonl")
    matching = [row for row in rows if method in (row.get("methods") or [])]
    if matching:
        return int(matching[-1].get("num_round_trips") or fallback)
    return fallback


def _discover_samples(out_dir, method):
    found = set()
    method_dir = Path(out_dir) / method
    if method_dir.is_dir():
        for path in method_dir.glob("*.ckpt.json"):
            found.add(path.name[:-10])
        for path in method_dir.glob("*.jsonl"):
            found.add(path.stem)
    raw_dir = Path(out_dir) / "api_raw" / method
    if raw_dir.is_dir():
        found.update(path.name for path in raw_dir.iterdir() if path.is_dir())
    return sorted(found)


def collect_progress(out_dir, method, samples=None, target_round_trips=None):
    out_dir = Path(out_dir).resolve()
    samples = list(samples or _discover_samples(out_dir, method))
    target = target_round_trips or _target_round_trips(out_dir, method)
    api_rows = [
        row for row in _read_jsonl(out_dir / "api_calls.jsonl")
        if row.get("method") == method
    ]
    rows = []
    now = time.time()
    for sample in samples:
        checkpoint = _read_json(out_dir / method / f"{sample}.ckpt.json")
        committed = int(checkpoint.get("completed_round_trips") or 0)
        sample_api = [row for row in api_rows if row.get("sample") == sample]
        complete_calls = sum(row.get("stream_complete") is True for row in sample_api)
        failed_calls = sum(row.get("stream_complete") is False for row in sample_api)
        total_tokens = sum(int(row.get("total_tokens") or 0) for row in sample_api)
        latest_call = sample_api[-1] if sample_api else {}
        response_slots = int(latest_call.get("response_slots_used") or 0)
        max_response_slots = int(latest_call.get("max_response_slots") or 2)
        transient_failures = int(latest_call.get("transient_failure_count") or 0)
        max_transient_failures = int(latest_call.get("max_transient_failures") or 3)

        raw_dir = out_dir / "api_raw" / method / sample
        transports = sorted(
            raw_dir.glob("*.transport.jsonl") if raw_dir.is_dir() else [],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        current_rt = None
        direction = None
        last_event = None
        age_s = None
        elapsed_s = None
        transport_bytes = None
        if transports:
            latest = transports[0]
            match = _TRANSPORT_NAME.match(latest.name)
            if match:
                current_rt = match.group("rt")
                direction = match.group("direction")
            tail = _tail_json(latest)
            event = tail.get("event") or {}
            last_event = event.get("type") or tail.get("record_type")
            if last_event == "content_block_delta":
                delta_type = (event.get("delta") or {}).get("type")
                last_event = {
                    "thinking_delta": "thinking",
                    "text_delta": "text",
                    "signature_delta": "signature",
                }.get(delta_type, delta_type or last_event)
            if tail.get("record_type") == "attempt_end":
                last_event = (tail.get("attempt") or {}).get("status") or last_event
            stat = latest.stat()
            age_s = max(0, int(now - stat.st_mtime))
            elapsed_s = max(0, int(now - stat.st_ctime))
            transport_bytes = stat.st_size

        if committed >= target:
            status = "done"
            stage = f"RT{target} committed"
        elif current_rt and current_rt != "NA":
            status = "running"
            stage = f"RT{current_rt} {direction or '?'} / {last_event or 'waiting'}"
        elif sample_api:
            status = "running"
            last = sample_api[-1]
            stage = (
                f"RT{last.get('rt_index') or '?'} {last.get('direction') or '?'} "
                f"/ {last.get('call_kind') or 'API complete; local work'}"
            )
        else:
            status = "not_started"
            stage = "waiting"

        rows.append({
            "method": method,
            "sample": sample,
            "status": status,
            "committed_round_trips": committed,
            "target_round_trips": target,
            "stage": stage,
            "complete_api_calls": complete_calls,
            "failed_api_calls": failed_calls,
            "total_tokens": total_tokens,
            "response_slots": response_slots,
            "max_response_slots": max_response_slots,
            "transient_failures": transient_failures,
            "max_transient_failures": max_transient_failures,
            "last_stream_update_age_s": age_s,
            "current_transport_elapsed_s": elapsed_s,
            "current_transport_bytes": transport_bytes,
        })
    return rows


def format_progress(rows):
    lines = [
        "method       sample        status       committed  current stage                         calls ok/fail budget       tokens elapsed/idle",
        "------------ ------------- ------------ ---------- ------------------------------------- ------------- ------------ ----------- ------------",
    ]
    for row in rows:
        committed = f"{row['committed_round_trips']}/{row['target_round_trips']}"
        calls = f"{row['complete_api_calls']}/{row['failed_api_calls']}"
        budget = (
            f"R{row['response_slots']}/{row['max_response_slots']} "
            f"I{row['transient_failures']}/{row['max_transient_failures']}"
        )
        age = (
            "-" if row["last_stream_update_age_s"] is None
            else f"{row['last_stream_update_age_s']}s"
        )
        elapsed = (
            "-" if row["current_transport_elapsed_s"] is None
            else f"{row['current_transport_elapsed_s']}s"
        )
        lines.append(
            f"{row['method'][:12]:12} {row['sample'][:13]:13} "
            f"{row['status'][:12]:12} {committed:10} {row['stage'][:37]:37} "
            f"{calls:13} {budget:12} {row['total_tokens']:11d} {elapsed:>6}/{age:<5}"
        )
    done = sum(row["status"] == "done" for row in rows)
    lines.append(f"summary: {done}/{len(rows)} sample-method tasks complete")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--methods", nargs="+", default=["hybridpatch", "fullrewrite"])
    parser.add_argument("--samples", nargs="+", default=None)
    parser.add_argument("--target_round_trips", type=int, default=None)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.interval < 1:
        parser.error("--interval must be >= 1")

    while True:
        rows = []
        for method in args.methods:
            rows.extend(collect_progress(
                args.dir, method, samples=args.samples,
                target_round_trips=args.target_round_trips,
            ))
        if args.json:
            print(json.dumps(rows, ensure_ascii=False))
        else:
            print(time.strftime("[%Y-%m-%d %H:%M:%S]"))
            print(format_progress(rows), flush=True)
        if not args.watch or (rows and all(row["status"] == "done" for row in rows)):
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
