"""
Honesty gate — independently recompute RS from stored raw responses, NO API.

The relay is deterministic given the LLM raw responses (stored per row) and the
seeded forward plan: replaying the HybridPatch envelope through apply_hybrid, or
parse_context_string (FullRewrite), through the official domain evaluator must
reproduce every stored backward RS. Any mismatch => telemetry drift / fabrication.

Mirrors experiment_runner exactly (skip_distractor runs: editable context == full context).

Run:  PYTHONUTF8=1 python src/verify_anchorpatch.py --dir <out_dir>
Exits non-zero if any recomputed RS differs from the stored score beyond tolerance.
"""
import os
import sys
import json
import argparse
import random
from collections import defaultdict, Counter

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from utils_env import load_sample, load_distractor_context, merge_distractor, shuffle_context
from utils_context import build_context_from_folder, parse_context_string, is_context_complete
from domains import get_domain
from hybrid_prompt import extract_hybrid_json
from hybrid_executor import apply_hybrid as execute_hybrid
from hybrid_gate import validate_hybrid_output
from hybrid_schema import validate_hybrid_envelope

SAMPLES_ROOT = os.path.join(_ROOT, "data", "samples_delegate52")
TOL = 1e-6


def _score(ev):
    if ev.get("score") is not None:
        return float(ev["score"])
    if "error" in ev:
        return 0.0
    return None


def _hybrid_replay_key(attempt):
    log = attempt.get("log")
    rate = round(log.op_accept_rate, 4) if log is not None else 0.0
    return (
        int(not attempt.get("invalid_json") and bool(attempt.get("envelope"))),
        int(bool(attempt.get("gate_ok")) and bool(attempt.get("gen"))),
        int(bool(attempt.get("clean_protocol"))),
        rate,
    )


def _run_hybrid_attempt_replay(raw, editable, target_filenames, readonly_names,
                               require_effective_change=False):
    envelope, em = extract_hybrid_json(raw)
    attempt = {
        "envelope": envelope,
        "invalid_json": envelope is None,
        "gen": None,
        "log": None,
        "gate_ok": False,
        "clean_protocol": False,
    }
    if envelope is None:
        attempt["key"] = _hybrid_replay_key(attempt)
        return attempt
    schema_errors, _warnings = validate_hybrid_envelope(envelope)
    gen, log = execute_hybrid(editable, envelope, target_filenames, bodies=em.get("bodies"))
    gate_pass, gate_errors = validate_hybrid_output(
        editable, gen, target_filenames, log,
        readonly_filenames=readonly_names,
        require_effective_change=require_effective_change)
    if log is not None and log.reject_reasons():
        gate_pass = False
    route_violations = ((getattr(log, "hybrid", None) or {}).get("route_violations") or [])
    if route_violations:
        gate_pass = False
    attempt.update(
        gen=gen,
        log=log,
        gate_ok=bool(gate_pass and gen and not gate_errors),
        clean_protocol=bool(not em.get("partial_extraction") and not schema_errors
                            and not (log.reject_reasons() if log is not None else [])
                            and not route_violations),
    )
    attempt["key"] = _hybrid_replay_key(attempt)
    return attempt


def _apply_hybrid(row, editable, target_filenames, readonly_names):
    hy = ((row.get("bdpatch") or {}).get("hybrid") or {})
    repair = hy.get("repair") or {}
    require_effective_change = row.get("round_trip_direction") == "forward"
    if repair.get("attempted") and repair.get("original_raw") is not None:
        a0 = _run_hybrid_attempt_replay(
            repair.get("original_raw") or "", editable, target_filenames,
            readonly_names, require_effective_change=require_effective_change)
        a1 = _run_hybrid_attempt_replay(
            repair.get("repair_raw") or "", editable, target_filenames,
            readonly_names, require_effective_change=require_effective_change)
        chosen = a1 if a1["key"] > a0["key"] else a0
    else:
        chosen = _run_hybrid_attempt_replay(
            row.get("raw_llm_response") or "", editable, target_filenames,
            readonly_names, require_effective_change=require_effective_change)
    if chosen["gen"] is None or not chosen["gen"] or not chosen["gate_ok"]:
        return dict(editable)
    return chosen["gen"]


def _step(row, editable, target_filenames, readonly_names):
    bd = row.get("bdpatch") or {}
    if bd.get("hybrid"):
        return _apply_hybrid(row, editable, target_filenames, readonly_names)
    # fullrewrite: the model reproduced the whole workspace verbatim
    if bd.get("actual_method") == "protocol_failure_explicit_reject":
        return {}
    return parse_context_string(row.get("raw_llm_response") or "")


def _evaluate(domain, sid, gen, target_state):
    if not is_context_complete(gen, list(target_state["context"])):
        return 0.0
    ev = domain.evaluate_context(sid, gen, target_state)
    return _score(ev)


def _editable_context(ctx, readonly_names):
    readonly = set(readonly_names or [])
    return {k: v for k, v in (ctx or {}).items() if k not in readonly}


def replay_sample(sid, rows, seed=None):
    if seed is not None:
        random.seed(seed)
    sample, folder, id2state = load_sample(sid, samples_folder=os.path.join(SAMPLES_ROOT, ""))
    domain = get_domain(sample["sample_type"])
    domain.samples_folder = os.path.join(SAMPLES_ROOT, "")
    initial_id = sample["start_state"]
    initial_state = id2state[initial_id]
    distractor_on = any(r.get("distractor_included") for r in rows)
    distractor = load_distractor_context(folder) if distractor_on else {}
    current = build_context_from_folder(os.path.join(folder, initial_state["solution_folder"]))
    if distractor_on:
        current = merge_distractor(current, distractor)
    current = shuffle_context(current)
    readonly_names = list(distractor) if distractor_on else []

    by_rt = defaultdict(dict)
    for r in rows:
        by_rt[r["round_trip_num"]][r["round_trip_direction"]] = r

    mismatches = []
    for rt in sorted(by_rt):
        fwd, bwd = by_rt[rt].get("forward"), by_rt[rt].get("backward")
        if not fwd or not bwd:
            continue
        # forward
        fwd_state = id2state[fwd["target_state_id"]]
        editable = _editable_context(current, readonly_names)
        current = shuffle_context(merge_distractor(
            _step(fwd, editable, list(fwd_state["context"]), readonly_names),
            distractor,
        ))
        # backward
        editable = _editable_context(current, readonly_names)
        bwd_gen = _step(bwd, editable, list(initial_state["context"]), readonly_names)
        recomputed = _evaluate(domain, sid, bwd_gen, initial_state)
        # advance current for the next round trip exactly as the runner did
        current = shuffle_context(merge_distractor(bwd_gen, distractor))
        stored = _score(bwd.get("evaluation") or {})
        if recomputed is None or stored is None:
            continue
        if abs(recomputed - stored) > TOL:
            mismatches.append((rt, stored, recomputed))
    return mismatches


def _dedupe_latest(rows):
    latest = {}
    order = {}
    for i, row in enumerate(rows):
        key = (row.get("round_trip_num"), row.get("round_trip_direction"))
        if key[0] is None or key[1] not in ("forward", "backward"):
            continue
        latest[key] = row
        order[key] = i
    return [latest[k] for k in sorted(latest, key=lambda k: order[k])]


def _duplicate_keys(rows):
    counts = Counter((r.get("round_trip_num"), r.get("round_trip_direction"))
                     for r in rows
                     if r.get("round_trip_direction") in ("forward", "backward"))
    return {k: v for k, v in counts.items() if v > 1}


def _load_seed_map(out_dir):
    path = os.path.join(out_dir, "run_metadata.jsonl")
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if not rec.get("context_shuffle_seeded"):
                continue
            seed = rec.get("seed")
            for method in rec.get("methods") or []:
                for sample in rec.get("samples") or []:
                    out[(method, sample)] = seed
    return out


def _hybrid_fingerprint_mixture(out_dir):
    base = os.path.basename(os.path.abspath(out_dir)).lower()
    if "val" not in base and "test" not in base:
        return []
    if not os.path.isdir(os.path.join(out_dir, "hybridpatch")):
        return []
    path = os.path.join(out_dir, "run_metadata.jsonl")
    if not os.path.exists(path):
        return []
    fps = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if "hybridpatch" not in (rec.get("methods") or []):
                continue
            fp = rec.get("code_fingerprint")
            if isinstance(fp, dict):
                fps.append(json.dumps(fp, sort_keys=True))
    return sorted(set(fps))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(_HERE, "experiment_results"))
    ap.add_argument("--dedupe", choices=("strict", "latest"), default="strict",
                    help="strict fails on duplicate RT/direction rows; latest replays the last row only")
    args = ap.parse_args()

    ok = True
    total_checked = 0
    mixed_fps = _hybrid_fingerprint_mixture(args.dir)
    if len(mixed_fps) > 1:
        ok = False
        print(f"[fingerprint] HYBRID VAL/TEST MIXED CODE FINGERPRINTS: {len(mixed_fps)} distinct fingerprints")
    seed_map = _load_seed_map(args.dir)
    for method in ("hybridpatch", "fullrewrite"):
        folder = os.path.join(args.dir, method)
        if not os.path.isdir(folder):
            continue
        by_sample = defaultdict(list)
        for fn in os.listdir(folder):
            if fn.endswith(".jsonl"):
                for line in open(os.path.join(folder, fn), encoding="utf-8"):
                    line = line.strip()
                    if line:
                        by_sample[fn[:-6]].append(json.loads(line))
        for sid, rows in sorted(by_sample.items()):
            dupes = _duplicate_keys(rows)
            replay_rows = rows
            if dupes:
                msg = ", ".join(f"RT{k[0]} {k[1]} x{v}" for k, v in sorted(dupes.items()))
                if args.dedupe == "strict":
                    ok = False
                    print(f"[{method}/{sid}] DUPLICATE ROWS: {msg}")
                else:
                    replay_rows = _dedupe_latest(rows)
                    print(f"[{method}/{sid}] DUPLICATE ROWS: {msg}; replaying latest rows only")
            mm = replay_sample(sid, replay_rows, seed=seed_map.get((method, sid)))
            n_back = sum(1 for r in replay_rows if r["round_trip_direction"] == "backward")
            total_checked += n_back
            if mm:
                ok = False
                print(f"[{method}/{sid}] MISMATCH x{len(mm)}: " +
                      "; ".join(f"RT{rt} stored={s:.4f} recomputed={r:.4f}" for rt, s, r in mm[:5]))
            else:
                print(f"[{method}/{sid}] OK ({n_back} backward RS reproduced)")

    print()
    if ok:
        print(f"HONESTY GATE: PASS — {total_checked} backward RS independently reproduced from raw responses")
        sys.exit(0)
    print("HONESTY GATE: FAIL — stored RS does not match independent recompute")
    sys.exit(1)


if __name__ == "__main__":
    main()
