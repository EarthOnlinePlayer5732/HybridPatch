"""Best-effort, per-process timing for HP_V9 control-plane slow paths.

Telemetry is deliberately outside the critical evidence ledgers.  Each process
writes only its own file, records only operations above a threshold, and never
turns an observability failure into an experiment failure.
"""

import json
import os
import threading
from datetime import datetime


CONTROL_TIMING_SCHEMA = "anchorpatch.control_timing/1"
DEFAULT_SLOW_THRESHOLD_MS = 250.0
_WRITE_LOCK = threading.Lock()


def _threshold_ms(default):
    raw = os.environ.get("ANCHORPATCH_SLOW_CONTROL_THRESHOLD_MS")
    if raw is None:
        return float(default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return value if value >= 0 else float(default)


def record_slow_control_operation(
        out_dir, operation, elapsed_seconds, *, threshold_ms=None, **details):
    """Append one slow-path record without shared locks or durability claims."""
    threshold = _threshold_ms(
        DEFAULT_SLOW_THRESHOLD_MS if threshold_ms is None else threshold_ms)
    elapsed_ms = max(0.0, float(elapsed_seconds) * 1000.0)
    if elapsed_ms < threshold:
        return None
    record = {
        "schema": CONTROL_TIMING_SCHEMA,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="milliseconds"),
        "operation": str(operation),
        "elapsed_ms": round(elapsed_ms, 3),
        "threshold_ms": round(threshold, 3),
        "pid": os.getpid(),
        "worker_launch_id": os.environ.get(
            "ANCHORPATCH_WORKER_LAUNCH_ID"),
    }
    record.update(details)
    try:
        target_dir = os.path.join(
            os.path.abspath(out_dir), "control_telemetry")
        os.makedirs(target_dir, exist_ok=True)
        target = os.path.join(target_dir, f"pid-{os.getpid()}.jsonl")
        line = json.dumps(
            record, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        with _WRITE_LOCK:
            with open(target, "a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
        return record
    except Exception:
        return None
