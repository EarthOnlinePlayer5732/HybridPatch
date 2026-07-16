"""Rewind a relay sample to just before a round trip and let the runner redo it.

Why: a provider-side empty response (HTTP 200, zero/thinking-only content)
poisons every later round trip of that sample. This tool truncates the
committed rows back to the last good RT so the existing runner re-executes
from there — it deletes NOTHING (originals go to a backup folder) and reuses
the runner's own resume path (`_reconcile_checkpoint_from_jsonl` rebuilds the
relay state by replaying the remaining rows, no API calls).

Scan mode — find affected samples and print ready-to-run rewind commands:
  PYTHONUTF8=1 python src/rewind_rt.py --dir <out_dir> --scan

Rewind mode — redo sample X of method M from round trip N (N=1 = full rerun):
  PYTHONUTF8=1 python src/rewind_rt.py --dir <out_dir> --method fullrewrite \
      --sample protein1 --from_rt 4 [--yes]

Then relaunch the SAME runner command for that sample (or launch_pipeline.py);
it resumes at RT N automatically.
"""
import argparse
import glob
import json
import os
import shutil
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

METHODS = ("hybridpatch", "fullrewrite")


def _rows(path):
    out = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            try:
                out.append((json.loads(line), line))
            except Exception:
                raise SystemExit(f"unparseable JSONL line {i + 1} in {path}; aborting (no changes made)")
    return out


def _classify(row):
    """Return a short problem tag for a committed row, or None if clean."""
    raw = row.get("raw_llm_response") or ""
    hy = (row.get("bdpatch") or {}).get("hybrid") or {}
    ev = row.get("evaluation") or {}
    if not raw.strip():
        return "empty_response"
    if hy.get("failed_step_kept_context"):
        return f"kept_context({hy.get('failure_reason')})"
    if ev.get("error"):
        return f"eval_error({ev.get('error')})"
    return None


def scan(out_dir):
    """Only a provider-side EMPTY response (and its downstream contamination)
    justifies a rewind. An evaluator error on a real, non-empty response is an
    honest capability failure — rerunning those until they pass would be
    cherry-picking, so they are reported but never suggested for rewind."""
    print(f"[scan] {out_dir}: rows with empty raw / kept-context / evaluator error\n")
    suggestions = []
    for method in METHODS:
        for path in sorted(glob.glob(os.path.join(out_dir, method, "*.jsonl"))):
            sample = os.path.basename(path)[:-6]
            hits = []
            for row, _line in _rows(path):
                tag = _classify(row)
                if tag:
                    hits.append((row.get("round_trip_num"), row.get("round_trip_direction"), tag))
            if not hits:
                continue
            empties = [rt for rt, _d, t in hits if t == "empty_response"]
            first_empty = min(empties) if empties else None
            print(f"  {method}/{sample}:" + (f" first EMPTY at RT{first_empty}" if first_empty
                                             else " honest failures only (no empty response)"))
            for rt, d, t in hits:
                honest = " <- honest failure, keep" if (
                    t != "empty_response" and (first_empty is None or rt < first_empty)) else ""
                print(f"    RT{rt} {d[:3]}: {t}{honest}")
            if first_empty is not None:
                suggestions.append(
                    f"PYTHONUTF8=1 python src/rewind_rt.py --dir {out_dir} "
                    f"--method {method} --sample {sample} --from_rt {first_empty} --yes")
    print()
    if suggestions:
        print("[scan] suggested rewinds (empty-anchored only; rerun the samples afterwards):")
        for s in suggestions:
            print("  " + s)
    else:
        print("[scan] no empty-response rows found; nothing to rewind")


def rewind(out_dir, method, sample, from_rt, assume_yes):
    jsonl_path = os.path.join(out_dir, method, f"{sample}.jsonl")
    ckpt_path = os.path.join(out_dir, method, f"{sample}.ckpt.json")
    if not os.path.exists(jsonl_path):
        raise SystemExit(f"no committed rows at {jsonl_path}; nothing to rewind (just run the sample)")

    rows = _rows(jsonl_path)
    keep = [(r, line) for r, line in rows if (r.get("round_trip_num") or 0) < from_rt]
    drop = [(r, line) for r, line in rows if (r.get("round_trip_num") or 0) >= from_rt]
    if not drop:
        raise SystemExit(f"no rows at RT>={from_rt}; nothing to rewind")

    print(f"[rewind] {method}/{sample}: keep {len(keep)} rows (RT<{from_rt}), "
          f"move {len(drop)} rows (RT>={from_rt}) to backup")
    for r, _line in drop:
        tag = _classify(r) or "ok"
        print(f"    drop RT{r.get('round_trip_num')} {r.get('round_trip_direction')[:3]} [{tag}]")
    if not assume_yes:
        if input("proceed? [y/N] ").strip().lower() not in ("y", "yes"):
            raise SystemExit("aborted; no changes made")

    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup = os.path.join(out_dir, "rewind_backups", f"{method}__{sample}__from_rt{from_rt}__{stamp}")
    os.makedirs(backup, exist_ok=True)

    # 1) original jsonl -> backup; truncated rows -> fresh jsonl (or none if RT1)
    shutil.copy2(jsonl_path, os.path.join(backup, f"{sample}.jsonl.orig"))
    if keep:
        tmp = jsonl_path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            for _r, line in keep:
                f.write(line + "\n")
        os.replace(tmp, jsonl_path)
    else:
        os.remove(jsonl_path)

    # 2) checkpoint -> backup (the runner rebuilds it from the remaining rows)
    if os.path.exists(ckpt_path):
        shutil.move(ckpt_path, os.path.join(backup, f"{sample}.ckpt.json"))

    # 3) stale per-step doc snapshots for RT>=from_rt -> backup
    doc_root = os.path.join(out_dir, "docs", method, sample)
    moved_docs = 0
    if os.path.isdir(doc_root):
        for d in sorted(os.listdir(doc_root)):
            if d[:2] == "rt" and d[2:4].isdigit() and int(d[2:4]) >= from_rt:
                os.makedirs(os.path.join(backup, "docs"), exist_ok=True)
                shutil.move(os.path.join(doc_root, d), os.path.join(backup, "docs", d))
                moved_docs += 1

    print(f"[rewind] done. backup: {backup} (rows kept: {len(keep)}, docs moved: {moved_docs})")
    print(f"[rewind] now rerun the sample; the runner resumes at RT{from_rt}:")
    print(f"  MINIMAX_THINKING=1 MINIMAX_HARD_TIMEOUT=7200 PYTHONUTF8=1 \\")
    print(f"  python src/experiment_runner.py --sample {sample} --methods {method} \\")
    print(f"    --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 \\")
    print(f"    --out_dir {out_dir} --notes \"rewind redo from RT{from_rt}\"")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--scan", action="store_true", help="list affected samples and suggested rewinds")
    ap.add_argument("--method", choices=METHODS)
    ap.add_argument("--sample")
    ap.add_argument("--from_rt", type=int, help="redo this round trip and everything after it (1 = full rerun)")
    ap.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = ap.parse_args()

    if args.scan:
        scan(args.dir)
        return
    if not (args.method and args.sample and args.from_rt):
        ap.error("rewind mode needs --method, --sample and --from_rt (or use --scan)")
    if args.from_rt < 1:
        ap.error("--from_rt must be >= 1")
    rewind(args.dir, args.method, args.sample, args.from_rt, args.yes)


if __name__ == "__main__":
    main()
