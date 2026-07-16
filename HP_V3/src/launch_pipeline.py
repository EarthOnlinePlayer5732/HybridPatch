"""Sliding-pool launcher: keep N runner processes alive, refill as each exits.

Replaces the wave-of-10-then-wait pattern: as soon as one (method, sample)
relay finishes, the next queued task starts, so slow samples never hold up the
whole batch. Tasks are queued method-major (all of the first --methods entry
first), matching the "run hybridpatch before fullrewrite" discipline while
still pipelining. Already-complete samples (checkpoint at --num_round_trips,
or stopped_early) are skipped, so re-invoking after a crash or a rewind_rt.py
rewind resumes exactly the unfinished work.

Environment (MINIMAX_THINKING / MINIMAX_HARD_TIMEOUT / keys) is inherited by
the children. Per-task console output appends to
<out_dir>/launcher_<method>_<sample>.stdout|stderr.log.

  MINIMAX_THINKING=1 MINIMAX_HARD_TIMEOUT=7200 PYTHONUTF8=1 \
  python src/launch_pipeline.py --out_dir exp_X --concurrency 10 \
      --methods hybridpatch fullrewrite --samples s1 s2 ... \
      --num_round_trips 10 --model minimax-m3 --max_tokens 0 --notes "..."
"""
import argparse
import json
import os
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER = os.path.join(_HERE, "experiment_runner.py")


def _task_done(out_dir, method, sample, num_round_trips):
    ckpt = os.path.join(out_dir, method, f"{sample}.ckpt.json")
    if not os.path.exists(ckpt):
        return False
    try:
        ck = json.load(open(ckpt, encoding="utf-8"))
    except Exception:
        return False
    return bool(ck.get("stopped_early")) or ck.get("completed_round_trips", 0) >= num_round_trips


def _launch(task, args):
    method, sample = task
    cmd = [sys.executable, RUNNER,
           "--sample", sample, "--methods", method,
           "--num_round_trips", str(args.num_round_trips),
           "--seed", str(args.seed),
           "--model", args.model,
           "--max_tokens", str(args.max_tokens),
           "--out_dir", args.out_dir,
           "--notes", args.notes]
    if not args.include_distractor:
        cmd.append("--skip_distractor")
    sout = open(os.path.join(args.out_dir, f"launcher_{method}_{sample}.stdout.log"), "a", encoding="utf-8")
    serr = open(os.path.join(args.out_dir, f"launcher_{method}_{sample}.stderr.log"), "a", encoding="utf-8")
    proc = subprocess.Popen(cmd, stdout=sout, stderr=serr)
    print(f"[pipeline] start {method}/{sample} (pid {proc.pid})", flush=True)
    return proc, sout, serr


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--samples", nargs="+", required=True)
    ap.add_argument("--methods", nargs="+", default=["hybridpatch", "fullrewrite"])
    ap.add_argument("--concurrency", type=int, default=10,
                    help="max simultaneous runner processes (minimax server limit is 10)")
    ap.add_argument("--num_round_trips", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--model", default="minimax-m3")
    ap.add_argument("--max_tokens", type=int, default=0, help="0 = uncapped")
    ap.add_argument("--notes", default="")
    ap.add_argument("--include_distractor", action="store_true",
                    help="default is --skip_distractor, matching all campaigns so far")
    ap.add_argument("--stagger", type=float, default=3.0,
                    help="seconds between process starts (avoid a thundering herd)")
    ap.add_argument("--dry_run", action="store_true", help="print the task queue and exit")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    queue, skipped = [], []
    for method in args.methods:            # method-major: HP fully queued before FR
        for sample in args.samples:
            if _task_done(args.out_dir, method, sample, args.num_round_trips):
                skipped.append((method, sample))
            else:
                queue.append((method, sample))

    print(f"[pipeline] {len(queue)} task(s) queued, {len(skipped)} already complete, "
          f"concurrency={args.concurrency}")
    for t in queue:
        print(f"  queued: {t[0]}/{t[1]}")
    if args.dry_run:
        return

    running = {}   # proc -> (task, sout, serr, t0)
    results = []
    try:
        while queue or running:
            while queue and len(running) < args.concurrency:
                task = queue.pop(0)
                proc, sout, serr = _launch(task, args)
                running[proc] = (task, sout, serr, time.time())
                time.sleep(args.stagger)
            time.sleep(5)
            for proc in list(running):
                if proc.poll() is None:
                    continue
                task, sout, serr, t0 = running.pop(proc)
                sout.close(); serr.close()
                mins = (time.time() - t0) / 60
                results.append((task, proc.returncode, mins))
                status = "OK" if proc.returncode == 0 else f"EXIT {proc.returncode}"
                print(f"[pipeline] done  {task[0]}/{task[1]}: {status} ({mins:.1f} min); "
                      f"{len(queue)} queued, {len(running)} running", flush=True)
    except KeyboardInterrupt:
        print(f"[pipeline] interrupted: terminating {len(running)} running task(s); "
              f"checkpoints keep committed RTs, just re-invoke to resume", flush=True)
        for proc in running:
            proc.terminate()
        raise

    failed = [(t, rc) for t, rc, _m in results if rc != 0]
    print(f"[pipeline] all done: {len(results)} ran, {len(failed)} failed, {len(skipped)} skipped")
    for t, rc in failed:
        print(f"  FAILED {t[0]}/{t[1]} exit={rc} (see launcher_{t[0]}_{t[1]}.stderr.log)")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
