"""Offline counterfactual sweep for v7 partial acceptance (design proof).

Policy under test: when a HybridPatch step's ONLY failure class is op
rejection (local_patch/bulk_patch routes), commit the executor's
partially-applied output (accepted ops applied, rejected ops skipped)
instead of discarding it for kept-context. Snapshot semantics (v3 local /
v5 bulk) make accepted ops independent of rejected ones, so the partial
application is well-defined and order-free; the preservation invariant is
untouched (only declared spans are edited either way).

This sweep replays archived campaigns read-only (zero API) and, for every
step where the policy would change the committed document, scores both
outcomes with the real domain evaluator (backward steps; forward steps have
no per-step reference and are reported unscored). Chain-frozen caveat: each
step's input is the ACTUAL archived chain state — downstream divergence a
changed forward step would cause cannot be replayed offline (same framing
as the C2 counterfactual sweep, FINDINGS §222).

Run:  PYTHONUTF8=1 python src/sweep_partial_acceptance.py \
        --dirs exp_20260710_hybridv6dev20full exp_20260709_hybridv5dev20full \
        --out analysis_partial_acceptance_sweep.json
"""
import argparse
import json
import os
import random
import sys
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from verify_anchorpatch import (
    SAMPLES_ROOT,
    _dedupe_latest,
    _editable_context,
    _load_seed_map,
    _run_hybrid_attempt_replay,
    _score,
)
from hybrid_gate import validate_hybrid_output
from hybrid_schema import ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH
from utils_env import load_sample, load_distractor_context, merge_distractor, shuffle_context
from utils_context import build_context_from_folder, is_context_complete
from domains import get_domain


def _evaluate(domain, sid, gen, target_state):
    if not is_context_complete(gen, list(target_state["context"])):
        return 0.0
    ev = domain.evaluate_context(sid, gen, target_state)
    return _score(ev)


def _partial_eligibility(attempt, editable, target_filenames, readonly_names,
                         require_effective_change):
    """Return (eligible, detail). Mirrors the planned v7 runner policy, minus
    the protocol-rev gate (this is a policy counterfactual over old archives)."""
    log = attempt.get("log")
    gen = attempt.get("gen")
    detail = {"route": None, "ops_total": 0, "ops_accepted": 0, "ops_rejected": 0,
              "reject_reasons": [], "pure_gate_errors": [], "why_not": None}
    if log is None or not gen:
        detail["why_not"] = "no_output"
        return False, detail
    hy = log.hybrid or {}
    route = hy.get("route")
    detail.update(route=route, ops_total=log.ops_total,
                  ops_accepted=log.ops_accepted, ops_rejected=log.ops_rejected,
                  reject_reasons=[d.get("reason") for d in log.reject_reasons()][:12])
    if route not in (ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH):
        detail["why_not"] = f"route_{route}"
        return False, detail
    if not log.reject_reasons():
        detail["why_not"] = "no_op_rejections"
        return False, detail
    if log.ops_accepted < 1:
        detail["why_not"] = "zero_accepted_ops"
        return False, detail
    if hy.get("route_violations"):
        detail["why_not"] = "route_violations"
        return False, detail
    _gate_pass, pure_errors = validate_hybrid_output(
        editable, gen, target_filenames, log,
        readonly_filenames=readonly_names,
        require_effective_change=require_effective_change)
    detail["pure_gate_errors"] = pure_errors
    if pure_errors:
        detail["why_not"] = "other_gate_errors"
        return False, detail
    return True, detail


def sweep_sample(sid, rows, seed=None):
    if seed is not None:
        random.seed(seed)
    sample, folder, id2state = load_sample(sid, samples_folder=os.path.join(SAMPLES_ROOT, ""))
    domain = get_domain(sample["sample_type"])
    domain.samples_folder = os.path.join(SAMPLES_ROOT, "")
    initial_state = id2state[sample["start_state"]]
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

    events = []

    def replay_step(row, editable, target_filenames):
        """Replicates verify_anchorpatch._apply_hybrid, additionally reporting
        the chosen attempt and the v7-policy counterfactual."""
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
            chosen_which = "repair" if chosen is a1 else "primary"
        else:
            chosen = _run_hybrid_attempt_replay(
                row.get("raw_llm_response") or "", editable, target_filenames,
                readonly_names, require_effective_change=require_effective_change)
            chosen_which = "single"
        kept = chosen["gen"] is None or not chosen["gen"] or not chosen["gate_ok"]
        committed = dict(editable) if kept else chosen["gen"]
        cf = None
        if kept:
            eligible, detail = _partial_eligibility(
                chosen, editable, target_filenames, readonly_names,
                require_effective_change)
            if eligible and chosen["gen"] != editable:
                cf = {"gen": chosen["gen"], "detail": detail,
                      "chosen_which": chosen_which}
            elif detail["reject_reasons"]:
                # near-miss bookkeeping: op-rejected steps that stay ineligible
                cf = {"gen": None, "detail": detail, "chosen_which": chosen_which}
        return committed, cf

    for rt in sorted(by_rt):
        fwd, bwd = by_rt[rt].get("forward"), by_rt[rt].get("backward")
        if not fwd or not bwd:
            continue
        fwd_state = id2state[fwd["target_state_id"]]

        # forward
        editable = _editable_context(current, readonly_names)
        if (fwd.get("bdpatch") or {}).get("hybrid"):
            committed, cf = replay_step(fwd, editable, list(fwd_state["context"]))
            if cf is not None:
                ev = {"sample": sid, "rt": rt, "direction": "forward",
                      "eligible": cf["gen"] is not None,
                      "chosen_which": cf["chosen_which"], **cf["detail"]}
                if cf["gen"] is not None:
                    ev["divergent"] = True
                events.append(ev)
        else:
            committed = editable  # non-hybrid rows should not appear here
        current = shuffle_context(merge_distractor(committed, distractor))

        # backward
        editable = _editable_context(current, readonly_names)
        committed, cf = replay_step(bwd, editable, list(initial_state["context"]))
        if cf is not None:
            ev = {"sample": sid, "rt": rt, "direction": "backward",
                  "eligible": cf["gen"] is not None,
                  "chosen_which": cf["chosen_which"], **cf["detail"]}
            if cf["gen"] is not None:
                stored = _score(bwd.get("evaluation") or {})
                v7_score = _evaluate(domain, sid, cf["gen"], initial_state)
                ev.update(divergent=True, v6_score=stored, v7_score=v7_score,
                          delta=(None if (stored is None or v7_score is None)
                                 else round(v7_score - stored, 6)))
            events.append(ev)
        current = shuffle_context(merge_distractor(committed, distractor))

    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dirs", nargs="+", required=True)
    ap.add_argument("--out", default=None, help="write full event JSON here")
    args = ap.parse_args()

    all_events = []
    for d in args.dirs:
        seed_map = _load_seed_map(d)
        folder = os.path.join(d, "hybridpatch")
        if not os.path.isdir(folder):
            print(f"[{d}] no hybridpatch dir — skipped")
            continue
        by_sample = defaultdict(list)
        for fn in os.listdir(folder):
            if fn.endswith(".jsonl"):
                for line in open(os.path.join(folder, fn), encoding="utf-8"):
                    line = line.strip()
                    if line:
                        by_sample[fn[:-6]].append(json.loads(line))
        for sid, rows in sorted(by_sample.items()):
            rows = _dedupe_latest(rows)
            events = sweep_sample(sid, rows, seed=seed_map.get(("hybridpatch", sid)))
            for ev in events:
                ev["archive"] = os.path.basename(os.path.abspath(d))
            all_events.extend(events)
            marks = [e for e in events if e.get("divergent")]
            if marks:
                print(f"[{d}/{sid}] {len(marks)} divergent step(s): "
                      + "; ".join(
                          f"RT{e['rt']} {e['direction']}"
                          + (f" {e['v6_score']:.3f}->{e['v7_score']:.3f}"
                             if e.get("v7_score") is not None else "")
                          for e in marks))

    divergent = [e for e in all_events if e.get("divergent")]
    scored = [e for e in divergent if e.get("delta") is not None]
    near_miss = [e for e in all_events if not e.get("eligible")]
    print()
    print(f"total op-rejected kept-context steps examined: {len(all_events)}")
    print(f"  policy-divergent (v7 would commit partial): {len(divergent)} "
          f"({sum(1 for e in divergent if e['direction'] == 'forward')} fwd unscored, "
          f"{len(scored)} bwd scored)")
    if scored:
        deltas = [e["delta"] for e in scored]
        pos = [d for d in deltas if d > 1e-9]
        neg = [d for d in deltas if d < -1e-9]
        print(f"  bwd score deltas: n={len(deltas)} mean={sum(deltas)/len(deltas):+.4f} "
              f"pos={len(pos)} neg={len(neg)} zero={len(deltas)-len(pos)-len(neg)}")
        print(f"    worst={min(deltas):+.4f} best={max(deltas):+.4f}")
    if near_miss:
        why = defaultdict(int)
        for e in near_miss:
            why[e.get("why_not")] += 1
        print(f"  ineligible op-rejected steps: {len(near_miss)} — {dict(why)}")
    if args.out:
        json.dump(all_events, open(args.out, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        print(f"events written: {args.out}")


if __name__ == "__main__":
    main()
