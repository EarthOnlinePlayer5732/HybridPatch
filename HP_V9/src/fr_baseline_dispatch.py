"""FR frozen-baseline dispatcher: run fullrewrite over all 234 samples with a
static multi-key slot pool (see docs/FR冻结计划.md §6). Also reused for
single-arm HybridPatch campaigns via --method hybridpatch (val40).

Each child process gets its own OPENCODE_API_KEY via env override (real env
beats .env, key is read per call). Key VALUES never appear in logs or metadata
— labels only.

Usage (from repo root):
  PYTHONUTF8=1 python src/fr_baseline_dispatch.py --out_dir exp_<date>_frbaseline234 --preflight
  PYTHONUTF8=1 python src/fr_baseline_dispatch.py --out_dir exp_<date>_frbaseline234 --dry_run
  PYTHONUTF8=1 python src/fr_baseline_dispatch.py --out_dir exp_<date>_frbaseline234
  PYTHONUTF8=1 python src/fr_baseline_dispatch.py --method hybridpatch \
    --samples <...> --plans_from exp_20260710_frbaseline234 --require_plans \
    --reserve_key KEY_09 KEY_10 KEY_11 --out_dir exp_<date>_val40

Keys file (default .env.frkeys, gitignored by the .env.* rule), one per line:
  KEY_01=sk-...
  KEY_02=sk-...
"""

import argparse
from collections import Counter
import json
import os
import shutil
import subprocess
import sys
import time
import uuid

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for p in (_ROOT, _HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

SAMPLES_ROOT = os.path.join(_ROOT, "data", "samples_delegate52")
DEV20_PLANS = os.path.join(_ROOT, "data", "research_splits", "dev20_taskplans_20260625")
NUM_ROUND_TRIPS = 10
NON_TEXT_SAMPLE_TYPES = {"image", "audio"}


def read_keys(path):
    if not os.path.exists(path):
        sys.exit(f"keys file not found: {path} (format: LABEL=value per line)")
    keys = []
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        label, value = line.split("=", 1)
        label, value = label.strip(), value.strip()
        if label and value:
            keys.append((label, value))
    if not keys:
        sys.exit(f"no keys parsed from {path}")
    labels = [l for l, _ in keys]
    if len(set(labels)) != len(labels):
        sys.exit("duplicate key labels in keys file")
    values = [v for _, v in keys]
    if len(set(values)) != len(values):
        sys.exit("duplicate key values in keys file")
    return keys


def all_samples():
    return sorted(
        d for d in os.listdir(SAMPLES_ROOT)
        if os.path.isdir(os.path.join(SAMPLES_ROOT, d))
    )


def sample_types(samples):
    result = {}
    for sample in samples:
        path = os.path.join(SAMPLES_ROOT, sample, "sample.json")
        if not os.path.exists(path):
            sys.exit(f"sample not found: {sample}")
        meta = json.load(open(path, encoding="utf-8"))
        result[sample] = meta["sample_type"]
    return result


def build_slot_keys(keys, slots_per_key, total_slots=None):
    """Build a balanced static slot pool without exceeding a per-key cap."""
    if slots_per_key < 1:
        raise ValueError("slots_per_key must be >= 1")
    capacity = len(keys) * slots_per_key
    if total_slots is None:
        total_slots = capacity
    if total_slots < 1:
        raise ValueError("slots must be >= 1")
    if total_slots > capacity:
        raise ValueError(
            f"slots={total_slots} exceeds {capacity} key slots "
            f"({len(keys)} keys x {slots_per_key} slots_per_key)"
        )
    # Fill one slot per key per pass so a total cap remains evenly distributed.
    slot_keys = []
    for _ in range(slots_per_key):
        for key in keys:
            slot_keys.append(key)
            if len(slot_keys) == total_slots:
                return slot_keys
    return slot_keys


def pop_eligible_sample(queue, attempts, key_label, max_key_attempts, retry_only=False):
    """Pop work for a key; reserve slots accept requeued samples only."""
    for i, sample in enumerate(queue):
        tried = attempts.get(sample, [])
        if retry_only and not tried:
            continue
        if len(tried) < max_key_attempts and key_label not in tried:
            return queue.pop(i)
    return None


def sample_complete(out_dir, sample, method, num_round_trips=NUM_ROUND_TRIPS):
    ckpt = os.path.join(out_dir, method, f"{sample}.ckpt.json")
    if not os.path.exists(ckpt):
        return False
    try:
        d = json.load(open(ckpt, encoding="utf-8"))
        return int(d.get("completed_round_trips") or 0) >= num_round_trips
    except Exception:
        return False


def log_event(out_dir, **ev):
    ev["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    path = os.path.join(out_dir, "dispatch_log.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(ev, ensure_ascii=False) + "\n")


def copy_plans(samples, plans_from, out_dir):
    """Copy frozen <sample>.task_plan.json files for the selected samples.

    Returns (copied, missing). Samples without a source plan are not an error
    here — the runner regenerates seeded plans — callers decide via
    --require_plans whether missing plans abort the launch.
    """
    copied, missing = 0, []
    for sample in samples:
        name = f"{sample}.task_plan.json"
        src = os.path.join(plans_from, name)
        if not os.path.exists(src):
            missing.append(sample)
            continue
        dst = os.path.join(out_dir, name)
        if not os.path.exists(dst):
            shutil.copyfile(src, dst)
            copied += 1
    return copied, missing


def preflight(samples, keys, out_dir, plans_from, skip_probe=False,
              probe_concurrency=1, require_plans=False):
    ok = True

    # 1. domain evaluator import check (zero API; heavy deps surface here)
    types = {}
    for s in samples:
        with open(os.path.join(
                SAMPLES_ROOT, s, "sample.json"), encoding="utf-8") as handle:
            meta = json.load(handle)
        types.setdefault(meta["sample_type"], []).append(s)
    print(f"[preflight] {len(samples)} samples across {len(types)} domains")
    from domains import get_domain
    for t in sorted(types):
        try:
            get_domain(t)
            print(f"  domain {t:<16} OK ({len(types[t])} samples)")
        except Exception as e:
            ok = False
            print(f"  domain {t:<16} FAIL: {e!r} — samples {types[t]} would waste spend")

    # 2. frozen task plans into out_dir
    copied, missing = copy_plans(samples, plans_from, out_dir)
    print(f"[preflight] frozen task plans from {plans_from}: {copied} new, "
          f"{len(missing)} missing ({missing[:5] if missing else 'none'})")
    if missing and require_plans:
        print(
            "[preflight] FAIL: --require_plans missing frozen plans for "
            + ", ".join(missing)
        )
        return False

    # 3. per-key liveness/concurrency probe (tiny calls, adaptive thinking)
    if skip_probe:
        print("[preflight] key probe skipped by flag")
    else:
        probe_src = (
            "from model_openai import generate;"
            "r=generate([{'role':'user','content':'Reply exactly: OK'}],"
            "model='minimax-m3',max_tokens=1024,timeout=120,return_metadata=True,"
            "thinking_mode='adaptive',call_kind='key_probe',max_retries=3);"
            "print('PROBE_OK='+str(bool((r.get('message') or '').strip())))"
        )
        for label, value in keys:
            python_path = os.pathsep.join(
                p for p in (_HERE, _ROOT, os.environ.get("PYTHONPATH")) if p
            )
            env = dict(
                os.environ, OPENCODE_API_KEY=value, PYTHONUTF8="1",
                PYTHONPATH=python_path, OPENCODE_TRANSPORT="anthropic_sdk_v2",
            )
            processes = [
                subprocess.Popen(
                    [sys.executable, "-c", probe_src], env=env, cwd=_ROOT,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                )
                for _ in range(probe_concurrency)
            ]
            results = []
            for p in processes:
                try:
                    output, _ = p.communicate(timeout=180)
                except subprocess.TimeoutExpired:
                    p.kill()
                    output, _ = p.communicate()
                    output = (output or "") + "\nprobe timed out after 180s"
                passed = p.returncode == 0 and "PROBE_OK=True" in (output or "")
                detail = (output or "").strip().replace(value, "<key>")[-120:]
                results.append((passed, detail))
            passed_count = sum(passed for passed, _ in results)
            status = "OK" if passed_count == probe_concurrency else "FAIL"
            if status == "FAIL":
                ok = False
            failure_detail = next((detail for passed, detail in results if not passed), "")
            suffix = f" ({passed_count}/{probe_concurrency})"
            if status == "FAIL" and failure_detail:
                suffix += f"  {failure_detail}"
            print(f"  key {label}: {status}{suffix}")
    return ok


def runner_cmd(sample, out_dir, notes, key_label, method,
               num_round_trips=NUM_ROUND_TRIPS):
    return [
        sys.executable, os.path.join("src", "experiment_runner.py"),
        "--sample", sample, "--methods", method,
        "--num_round_trips", str(num_round_trips), "--skip_distractor",
        "--model", "minimax-m3", "--max_tokens", "131072",
        "--out_dir", out_dir,
        "--notes", f"{notes}, key={key_label}",
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--method", default="fullrewrite",
                    choices=["fullrewrite", "hybridpatch"])
    ap.add_argument("--keys_file", default=os.path.join(_ROOT, ".env.frkeys"))
    ap.add_argument(
        "--slots_per_key", type=int, default=5,
        help="maximum concurrent runner processes bound to each key (default: 5)",
    )
    ap.add_argument(
        "--slots", type=int, default=None,
        help="optional active-slot cap; default is active_key_count * slots_per_key",
    )
    ap.add_argument(
        "--reserve_key", nargs="+", default=["KEY_11"],
        help="key label(s) reserved for requeued samples only (default: KEY_11)",
    )
    ap.add_argument("--samples", nargs="+", default=None)
    ap.add_argument(
        "--plans_from", default=DEV20_PLANS,
        help="dir holding frozen <sample>.task_plan.json files to copy into out_dir",
    )
    ap.add_argument(
        "--require_plans", action="store_true",
        help="abort if any selected sample has no frozen plan in --plans_from",
    )
    ap.add_argument("--max_key_attempts", type=int, default=3)
    ap.add_argument("--num_round_trips", type=int, default=NUM_ROUND_TRIPS)
    ap.add_argument(
        "--progress_interval", type=float, default=60.0,
        help="seconds between checkpoint/stream progress tables; 0 disables",
    )
    ap.add_argument("--notes", default="FR frozen baseline one-shot 234, no rewind")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--skip_probe", action="store_true")
    ap.add_argument(
        "--probe_concurrency", type=int, default=1,
        help="simultaneous tiny liveness probes per key during preflight (default: 1)",
    )
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    keys = read_keys(args.keys_file)
    samples = args.samples or all_samples()
    types_by_sample = sample_types(samples)
    non_text = [
        sample for sample, sample_type in types_by_sample.items()
        if sample_type in NON_TEXT_SAMPLE_TYPES
    ]
    if non_text:
        sys.exit(
            "dispatch is text-only; refusing non-text samples: "
            + ", ".join(non_text)
        )
    print(
        f"[selection] text-only gate OK: {len(samples)} samples, "
        f"0 image/audio samples"
    )

    if args.max_key_attempts < 1:
        sys.exit("max_key_attempts must be >= 1")
    if args.num_round_trips < 1:
        sys.exit("num_round_trips must be >= 1")
    if args.progress_interval < 0:
        sys.exit("progress_interval must be >= 0")
    if args.probe_concurrency < 1:
        sys.exit("probe_concurrency must be >= 1")
    reserve_labels = list(dict.fromkeys(args.reserve_key))
    reserve_keys = []
    for label in reserve_labels:
        matches = [key for key in keys if key[0] == label]
        if len(matches) != 1:
            sys.exit(f"reserve key label not found exactly once: {label}")
        reserve_keys.append(matches[0])
    active_keys = [key for key in keys if key[0] not in set(reserve_labels)]
    if not active_keys:
        sys.exit("at least one non-reserve key is required")
    try:
        active_slot_keys = build_slot_keys(active_keys, args.slots_per_key, args.slots)
        reserve_slot_keys = build_slot_keys(reserve_keys, args.slots_per_key)
    except ValueError as e:
        sys.exit(str(e))
    slot_specs = (
        [(label, value, False) for label, value in active_slot_keys]
        + [(label, value, True) for label, value in reserve_slot_keys]
    )
    active_slots = len(active_slot_keys)
    reserve_slots = len(reserve_slot_keys)

    if args.preflight:
        sys.exit(0 if preflight(
            samples, keys, out_dir, args.plans_from,
            args.skip_probe, args.probe_concurrency, args.require_plans
        ) else 1)

    copied, missing = copy_plans(samples, args.plans_from, out_dir)
    if copied:
        print(f"[dispatch] copied {copied} frozen task plans from {args.plans_from}")
    if missing and args.require_plans:
        sys.exit(f"--require_plans: no frozen plan for {missing}")

    queue = [
        s for s in samples
        if not sample_complete(out_dir, s, args.method, args.num_round_trips)
    ]
    done_already = len(samples) - len(queue)
    active_slot_counts = Counter(label for label, _ in active_slot_keys)
    reserve_slot_counts = Counter(label for label, _ in reserve_slot_keys)

    print(f"[dispatch] method={args.method}: {len(samples)} samples, "
          f"{done_already} already complete, "
          f"{len(queue)} queued; {active_slots} active slots over {len(active_keys)} keys "
          f"+ {reserve_slots} retry-only slots on {','.join(reserve_labels)} "
          f"(cap {args.slots_per_key}/key)")
    for label, _ in keys:
        active_count = active_slot_counts.get(label, 0)
        reserve_count = reserve_slot_counts.get(label, 0)
        role = "reserve/requeue-only" if reserve_count else "active"
        print(f"  key {label}: {active_count + reserve_count} slots ({role})")
    if args.dry_run:
        print("[dispatch] dry run — first 10 queued:", queue[:10])
        return

    dlog_dir = os.path.join(out_dir, "dispatch_logs")
    os.makedirs(dlog_dir, exist_ok=True)
    log_event(out_dir, event="dispatch_start", queued=len(queue),
              method=args.method,
              active_slots=active_slots, reserve_slots=reserve_slots,
              slots_per_key=args.slots_per_key, reserve_key=reserve_labels,
              active_slot_counts=dict(active_slot_counts),
              reserve_slot_counts=dict(reserve_slot_counts),
              key_labels=[l for l, _ in keys])

    running = {}   # slot -> (sample, key_label, Popen, logfile)
    attempts = {}  # sample -> [key_labels tried]
    failed = []
    completed = 0
    last_progress = 0.0

    def launch(slot, sample, label, value):
        worker_launch_id = f"dispatch-{sample}-{uuid.uuid4().hex[:12]}"
        env = dict(os.environ, OPENCODE_API_KEY=value,
                   OPENCODE_TRANSPORT="anthropic_sdk_v2",
                   MINIMAX_HARD_TIMEOUT="7200", PYTHONUTF8="1",
                   ANCHORPATCH_WORKER_LAUNCH_ID=worker_launch_id)
        lf = open(os.path.join(dlog_dir, f"{sample}__{label}.console.log"), "a", encoding="utf-8")
        p = subprocess.Popen(runner_cmd(
            sample, out_dir, args.notes, label, args.method,
            num_round_trips=args.num_round_trips),
                             env=env, cwd=_ROOT, stdout=lf, stderr=subprocess.STDOUT)
        running[slot] = (sample, label, p, lf)
        attempts.setdefault(sample, []).append(label)
        log_event(out_dir, event="launch", sample=sample, key=label, slot=slot, pid=p.pid,
                  worker_launch_id=worker_launch_id)
        print(f"[dispatch] slot {slot} <- {sample} (key {label}, pid {p.pid})")

    try:
        while queue or running:
            launched = 0
            # Give the dedicated reserve first claim on requeued samples. It
            # never consumes a fresh sample, so reserve keys stay cold in normal flow.
            reserve_start = active_slots
            slot_order = list(range(reserve_start, len(slot_specs))) + list(range(active_slots))
            for slot in slot_order:
                label, value, retry_only = slot_specs[slot]
                if slot in running or not queue:
                    continue
                sample = pop_eligible_sample(
                    queue, attempts, label, args.max_key_attempts,
                    retry_only=retry_only,
                )
                if sample is not None:
                    launch(slot, sample, label, value)
                    launched += 1
            if queue and not running and not launched:
                # Every remaining sample exhausted all distinct available keys.
                for sample in queue:
                    failed.append(sample)
                    log_event(out_dir, event="failed", sample=sample,
                              reason="no_untried_key", tried=attempts.get(sample, []))
                    print(f"[dispatch] FAILED {sample}: no untried key remains")
                queue.clear()
                break
            time.sleep(20)
            if (
                args.progress_interval
                and time.time() - last_progress >= args.progress_interval
            ):
                from monitor_experiment import collect_progress, format_progress
                progress = collect_progress(
                    out_dir, args.method, samples=samples,
                    target_round_trips=args.num_round_trips,
                )
                print("[dispatch progress]\n" + format_progress(progress), flush=True)
                last_progress = time.time()
            for slot, (sample, label, p, lf) in list(running.items()):
                if p.poll() is None:
                    continue
                lf.close()
                del running[slot]
                if sample_complete(
                    out_dir, sample, args.method, args.num_round_trips
                ):
                    completed += 1
                    log_event(out_dir, event="complete", sample=sample, key=label)
                    print(f"[dispatch] DONE {sample} ({completed} complete, "
                          f"{len(queue)} queued, {len(running)} running)")
                elif (len(attempts[sample]) < args.max_key_attempts
                      and len(set(attempts[sample])) < len(keys)):
                    log_event(out_dir, event="requeue", sample=sample,
                              key=label, tried=attempts[sample])
                    print(f"[dispatch] INCOMPLETE {sample} on {label} — requeue with new key")
                    queue.insert(0, sample)
                else:
                    failed.append(sample)
                    log_event(out_dir, event="failed", sample=sample, tried=attempts[sample])
                    print(f"[dispatch] FAILED {sample} after keys {attempts[sample]}")
    except KeyboardInterrupt:
        print("[dispatch] interrupted — terminating children")
        for sample, label, p, lf in running.values():
            p.terminate()
            lf.close()
        log_event(out_dir, event="interrupted",
                  running=[s for s, *_ in running.values()], queued=len(queue))
        raise

    log_event(out_dir, event="dispatch_end", completed=completed, failed=failed)
    print(f"[dispatch] finished: {completed} completed this run, "
          f"{done_already} pre-existing, failed={failed or 'none'}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
