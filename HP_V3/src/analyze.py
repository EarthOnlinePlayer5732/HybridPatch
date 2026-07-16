"""
Analysis — HybridPatch vs FullRewrite.

Headline metric is RS@k (user's choice), reported with the full rigor the BD-Patch
retrospective demands:
  - paired SAME-task comparison (same seed plan -> backward task at round-trip k is
    identical for both methods), paired t-test + Cohen's d, per-domain breakdown;
  - failures (context_mismatch) counted as RS=0, NOT dropped;
  - ECR-conditioned RS (only round trips whose forward actually changed the doc);
  - preservation telemetry (op accept rate, preservation_violations, byte survival),
    three-layer disclosure (anchored / emit_file / fallback), no-op & fixed-point rates,
    token usage.

Run from repo root:
  PYTHONUTF8=1 python src/analyze.py --dir <out_dir>
Writes <out_dir>/analysis/experiment_results.csv and <out_dir>/analysis/comparison.md.
"""
import os
import sys
import csv
import json
import argparse
import statistics
from collections import Counter, defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

RESULTS = os.path.join(_HERE, "results")


def load_rows(folder):
    rows = []
    if not os.path.isdir(folder):
        return rows
    for fn in os.listdir(folder):
        if not fn.endswith(".jsonl"):
            continue
        for line in open(os.path.join(folder, fn), encoding="utf-8"):
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    return rows


def domain_of(sid):
    return "".join(c for c in sid if not c.isdigit())


def score_of(row):
    """RS for a row: evaluation.score, or 0.0 on a reconstruction error (NOT dropped)."""
    ev = row.get("evaluation") or {}
    if ev.get("score") is not None:
        return float(ev["score"])
    if "error" in ev:
        return 0.0
    return None


def backward(rows):
    return [r for r in rows if r.get("round_trip_direction") == "backward"]


def rs_at_k(rows, K=10):
    by_k = defaultdict(list)
    for r in backward(rows):
        s = score_of(r)
        if s is not None:
            by_k[r["round_trip_num"]].append(s)
    return {k: (statistics.mean(by_k[k]) if by_k.get(k) else float("nan")) for k in range(1, K + 1)}


def summary_k_values(K):
    return sorted({k for k in (1, 5, 10) if k <= K} | {K})


def paired(ap_rows, fr_rows, ecr_only=False):
    """Pair backward RS by (sample_id, round_trip_num). Returns (ap_list, fr_list, keys)."""
    def index(rows):
        out = {}
        for r in backward(rows):
            if ecr_only and not (r.get("bdpatch") or {}).get("ecr_pass"):
                continue
            out[(r["sample_id"], r["round_trip_num"])] = score_of(r)
        return out
    A, F = index(ap_rows), index(fr_rows)
    keys = sorted(set(A) & set(F))
    a = [A[k] for k in keys if A[k] is not None and F[k] is not None]
    f = [F[k] for k in keys if A[k] is not None and F[k] is not None]
    return a, f, keys


def paired_stats(a, f):
    if len(a) < 2:
        return {"n": len(a), "mean_ap": _m(a), "mean_fr": _m(f), "delta": None,
                "t": None, "p": None, "cohend": None}
    diff = [x - y for x, y in zip(a, f)]
    md = statistics.mean(diff)
    sd = statistics.pstdev(diff) if len(diff) > 1 else 0.0
    sd_s = statistics.stdev(diff) if len(diff) > 1 else 0.0
    try:
        from scipy.stats import ttest_rel
        t, p = ttest_rel(a, f)
        t, p = float(t), float(p)
    except Exception:
        t, p = None, None
    cohend = (md / sd_s) if sd_s > 0 else 0.0
    return {"n": len(a), "mean_ap": statistics.mean(a), "mean_fr": statistics.mean(f),
            "delta": md, "t": t, "p": p, "cohend": cohend}


def _m(xs):
    return statistics.mean(xs) if xs else float("nan")


def model_label(*row_groups):
    models = sorted({r.get("model_name") for rows in row_groups for r in rows if r.get("model_name")})
    return ", ".join(models) if models else "unknown"


def distractor_label(*row_groups):
    values = {bool(r.get("distractor_included")) for rows in row_groups for r in rows
              if r.get("distractor_included") is not None}
    if values == {True}:
        return "INCLUDED"
    if values == {False}:
        return "EXCLUDED"
    if values == {False, True}:
        return "MIXED"
    return "UNKNOWN"


def method_layers(ap_rows):
    """Counts of step method tags + telemetry on anchorpatch steps."""
    tags = defaultdict(int)
    accept, surv, preserv, viol = [], [], [], 0
    noop_fwd, emit = 0, 0
    for r in ap_rows:
        bd = r.get("bdpatch") or {}
        tags[bd.get("actual_method")] += 1
        if bd.get("noop_forward"):
            noop_fwd += 1
        if bd.get("used_emit_file"):
            emit += 1
        if bd.get("op_accept_rate") is not None:
            accept.append(bd["op_accept_rate"])
        if bd.get("survival_rate") is not None:
            surv.append(bd["survival_rate"])
        if bd.get("preservation_rate") is not None:
            preserv.append(bd["preservation_rate"])
        if bd.get("preservation_violations"):
            viol += bd["preservation_violations"]
    return tags, {"mean_op_accept": _m(accept), "mean_survival": _m(surv),
                  "mean_preservation": _m(preserv), "preservation_violations": viol,
                  "noop_forward_steps": noop_fwd, "emit_file_steps": emit}


def fixed_point_chains(rows):
    """Round trips whose forward did NOT change the document (trivial recovery risk)."""
    n_noop = sum(1 for r in rows if r.get("round_trip_direction") == "forward"
                 and (r.get("bdpatch") or {}).get("noop_forward"))
    n_fwd = sum(1 for r in rows if r.get("round_trip_direction") == "forward")
    return n_noop, n_fwd


def tokens(rows):
    p = sum((r.get("prompt_tokens") or 0) for r in rows)
    c = sum((r.get("completion_tokens") or 0) for r in rows)
    return p, c, p + c


def critical_failures(rows, theta=0.10):
    """Round trips where backward RS drops by theta or collapses to 0 vs previous RT."""
    by_sample = defaultdict(dict)
    for r in backward(rows):
        s = score_of(r)
        if s is not None:
            by_sample[r["sample_id"]][r["round_trip_num"]] = s
    n_crit, n_trans = 0, 0
    for sid, km in by_sample.items():
        ks = sorted(km)
        for a, b in zip(ks, ks[1:]):
            n_trans += 1
            drop = km[a] - km[b]
            if (km[a] > 0 and km[b] <= 1e-9) or drop >= theta:
                n_crit += 1
    return n_crit, n_trans


def calibrate_critical_theta(*row_groups):
    drops = []
    by_key = defaultdict(dict)
    for rows in row_groups:
        for r in backward(rows):
            s = score_of(r)
            if s is not None:
                by_key[(r.get("method"), r.get("sample_id"))][r["round_trip_num"]] = s
    for km in by_key.values():
        ks = sorted(km)
        for a, b in zip(ks, ks[1:]):
            d = km[a] - km[b]
            if d > 0:
                drops.append(d)
    if not drops:
        return 0.10
    drops.sort()
    idx = min(len(drops) - 1, int(0.75 * (len(drops) - 1)))
    p75 = drops[idx]
    for cand in (0.05, 0.10, 0.15, 0.20):
        if cand >= p75:
            return cand
    return 0.20


def hybrid_metrics(rows):
    n = 0
    routes = Counter()
    families = Counter()
    bounded = 0
    kept = 0
    gate_fail = 0
    invalid = 0
    schema = 0
    repair_attempted = 0
    repair_used = 0
    repair_success = 0
    no_effective_forward = 0
    forward_audits = 0
    copied = 0
    generated = 0
    ratios = []
    route_violations = Counter()
    for r in rows:
        bd = r.get("bdpatch") or {}
        hy = bd.get("hybrid") or {}
        if not hy:
            continue
        n += 1
        routes[hy.get("route") or bd.get("actual_method") or "unknown"] += 1
        families[hy.get("task_family") or "unknown"] += 1
        bounded += bool(hy.get("bounded_rewrite"))
        kept += bool(hy.get("failed_step_kept_context"))
        gate_fail += bool(hy.get("validation_gate_errors"))
        invalid += bool(hy.get("invalid_json"))
        schema += bool(hy.get("schema_error_count"))
        rep = hy.get("repair") or {}
        repair_attempted += bool(rep.get("attempted"))
        repair_used += bool(rep.get("used"))
        repair_success += bool(rep.get("success"))
        copied += int(hy.get("copied_source_bytes") or 0)
        generated += int(hy.get("generated_bytes") or 0)
        if hy.get("generated_byte_ratio") is not None:
            ratios.append(float(hy.get("generated_byte_ratio") or 0.0))
        for reason in hy.get("route_violations") or []:
            route_violations[str(reason)] += 1
        audit = hy.get("forward_audit") or {}
        if audit:
            forward_audits += 1
            no_effective_forward += not bool(audit.get("effective_modification"))
    if not n:
        return None
    return {
        "steps": n,
        "routes": dict(routes),
        "task_families": dict(families),
        "bounded_rewrite_steps": bounded,
        "bounded_rewrite_share": bounded / n,
        "failed_step_kept_context": kept,
        "kept_context_rate": kept / n,
        "validation_gate_failed_steps": gate_fail,
        "gate_failure_rate": gate_fail / n,
        "invalid_json": invalid,
        "schema_error_steps": schema,
        "repair_attempted": repair_attempted,
        "repair_used": repair_used,
        "repair_success": repair_success,
        "repair_rate": repair_attempted / n,
        "forward_audit_steps": forward_audits,
        "no_effective_forward_steps": no_effective_forward,
        "no_effective_forward_rate": (no_effective_forward / forward_audits) if forward_audits else 0.0,
        "copied_source_bytes": copied,
        "generated_bytes": generated,
        "generated_byte_ratio_mean": _m(ratios),
        "copy_generated_ratio": (copied / generated) if generated else None,
        "route_violations": dict(route_violations),
    }


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


def _emit_hybrid_telemetry(L, hm):
    L.append(f"- steps with hybrid telemetry: {hm['steps']}")
    L.append("- route share: " + ", ".join(
        f"`{k}`={v}/{hm['steps']} ({100*v/hm['steps']:.1f}%)"
        for k, v in sorted(hm["routes"].items())))
    L.append(f"- bounded rewrite share: {hm['bounded_rewrite_steps']}/{hm['steps']} "
             f"({100*hm['bounded_rewrite_share']:.1f}%)")
    L.append(f"- copied/generated bytes: {hm['copied_source_bytes']}/{hm['generated_bytes']} "
             f"| copy:generated ratio={hm['copy_generated_ratio'] if hm['copy_generated_ratio'] is not None else 'n/a'} "
             f"| mean generated byte ratio={hm['generated_byte_ratio_mean']:.3f}")
    L.append(f"- repair: attempted={hm['repair_attempted']} used={hm['repair_used']} "
             f"success={hm['repair_success']} rate={100*hm['repair_rate']:.1f}%")
    L.append(f"- gate failures: {hm['validation_gate_failed_steps']} "
             f"({100*hm['gate_failure_rate']:.1f}%) | kept-context failures: "
             f"{hm['failed_step_kept_context']} ({100*hm['kept_context_rate']:.1f}%)")
    L.append(f"- forward no-effective-modification audit: {hm['no_effective_forward_steps']}/"
             f"{hm['forward_audit_steps']} ({100*hm['no_effective_forward_rate']:.1f}%)")
    if hm.get("route_violations"):
        L.append("- route violations: " + ", ".join(
            f"`{k}`={v}" for k, v in sorted(hm["route_violations"].items())))
    L.append("")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(_HERE, "experiment_results"))
    ap.add_argument("--K", type=int, default=10)
    ap.add_argument("--out", default=None,
                    help="output dir for CSV + comparison.md (default: <dir>/analysis)")
    ap.add_argument("--critical_theta", type=float, default=0.10,
                    help="CriticalFailure threshold for adjacent backward RS drops")
    ap.add_argument("--calibrate_critical_theta", action="store_true",
                    help="calibrate theta from positive nonzero adjacent dev drops")
    args = ap.parse_args()
    out_dir = args.out or os.path.join(args.dir, "analysis")
    mixed_fps = _hybrid_fingerprint_mixture(args.dir)
    if len(mixed_fps) > 1:
        raise RuntimeError(
            f"hybridpatch val/test reporting forbidden: mixed code fingerprints ({len(mixed_fps)})")

    ap_rows = load_rows(os.path.join(args.dir, "anchorpatch"))
    hybrid_rows = load_rows(os.path.join(args.dir, "hybridpatch"))
    fr_rows = load_rows(os.path.join(args.dir, "fullrewrite"))
    critical_theta = (
        calibrate_critical_theta(hybrid_rows, fr_rows)
        if args.calibrate_critical_theta else args.critical_theta
    )

    samples = sorted({r["sample_id"] for r in ap_rows} | {r["sample_id"] for r in fr_rows}
                     | {r["sample_id"] for r in hybrid_rows})
    domains = sorted({domain_of(s) for s in samples})

    # ---- CSV (per backward round trip, both methods) ----
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "experiment_results.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["sample", "domain", "method", "round_trip", "direction", "rs",
                    "method_tag", "op_accept_rate", "preservation_violations",
                    "used_emit_file", "survival_rate", "ecr_pass",
                    "prompt_tokens", "completion_tokens", "forward_task"])
        # map forward task per (sample, rt)
        fwd_task = {}
        for r in ap_rows + hybrid_rows + fr_rows:
            if r["round_trip_direction"] == "forward":
                fwd_task[(r["method"], r["sample_id"], r["round_trip_num"])] = r["target_state_id"]
        for tag, rows in (("anchorpatch", ap_rows),
                          ("hybridpatch", hybrid_rows),
                          ("fullrewrite", fr_rows)):
            for r in rows:
                bd = r.get("bdpatch") or {}
                w.writerow([r["sample_id"], domain_of(r["sample_id"]), tag,
                            r["round_trip_num"], r["round_trip_direction"], score_of(r),
                            bd.get("actual_method"), bd.get("op_accept_rate"),
                            bd.get("preservation_violations"), bd.get("used_emit_file"),
                            bd.get("survival_rate"), bd.get("ecr_pass"),
                            r.get("prompt_tokens"), r.get("completion_tokens"),
                            fwd_task.get((r["method"], r["sample_id"], r["round_trip_num"]))])

    # ---- report ----
    primary_rows = hybrid_rows if hybrid_rows and not ap_rows else ap_rows
    primary_label = "HybridPatch" if hybrid_rows and not ap_rows else "AnchorPatch"
    L = [f"# {primary_label} vs FullRewrite — diagnostic comparison", ""]
    L.append(f"Model: {model_label(ap_rows, hybrid_rows, fr_rows)} | samples: {', '.join(samples)} "
             f"(n={len(samples)}) | round trips: {args.K} | distractor: {distractor_label(ap_rows, hybrid_rows, fr_rows)}")
    L.append("")
    L.append("> Positioning: a **diagnostic** experiment (small n; accounting is a known "
             "AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). "
             "RS is the headline metric per request; preservation is the reliable secondary. "
             "Not a final universal superiority claim.")
    L.append("")

    # RS@k overall
    apk, frk = rs_at_k(primary_rows, args.K), rs_at_k(fr_rows, args.K)
    L.append("## RS@k (overall, failures counted as 0)")
    L.append("| k | " + " | ".join(f"{k}" for k in range(1, args.K + 1)) + " |")
    L.append("|---|" + "---|" * args.K)
    L.append(f"| {primary_label} | " + " | ".join(f"{apk[k]:.3f}" for k in range(1, args.K + 1)) + " |")
    L.append("| FullRewrite | " + " | ".join(f"{frk[k]:.3f}" for k in range(1, args.K + 1)) + " |")
    L.append("")

    # per domain summary
    summary_ks = summary_k_values(args.K)
    L.append("## RS@" + "{" + ",".join(str(k) for k in summary_ks) + "} per domain")
    L.append("| domain | method | " + " | ".join(f"RS@{k}" for k in summary_ks) + " |")
    L.append("|---|---|" + "---|" * len(summary_ks))
    for dom in domains:
        a = [r for r in primary_rows if domain_of(r["sample_id"]) == dom]
        f = [r for r in fr_rows if domain_of(r["sample_id"]) == dom]
        ak, fk = rs_at_k(a, args.K), rs_at_k(f, args.K)
        L.append(f"| {dom} | {primary_label} | " + " | ".join(f"{ak[k]:.3f}" for k in summary_ks) + " |")
        L.append(f"| {dom} | FullRewrite | " + " | ".join(f"{fk[k]:.3f}" for k in summary_ks) + " |")
    L.append("")

    # paired same-task
    a, f, keys = paired(primary_rows, fr_rows)
    st = paired_stats(a, f)
    ae, fe, _ = paired(primary_rows, fr_rows, ecr_only=True)
    ste = paired_stats(ae, fe)
    L.append("## Paired same-task comparison (backward RS, matched by sample+round-trip)")
    L.append(f"| condition | n pairs | mean {primary_label} | mean FullRewrite | Δ | t | p | Cohen d |")
    L.append("|---|---|---|---|---|---|---|---|")
    def fmt(s):
        return (f"| {s['n']} | {s['mean_ap']:.3f} | {s['mean_fr']:.3f} | "
                f"{('%+.3f'%s['delta']) if s['delta'] is not None else 'n/a'} | "
                f"{('%.2f'%s['t']) if s['t'] is not None else 'n/a'} | "
                f"{('%.3f'%s['p']) if s['p'] is not None else 'n/a'} | "
                f"{('%.2f'%s['cohend']) if s['cohend'] is not None else 'n/a'} |")
    L.append("| all pairs " + fmt(st))
    L.append("| ECR-conditioned (forward actually edited) " + fmt(ste))
    L.append("")

    # preservation / three layers
    tags, tel = method_layers(primary_rows)
    total_steps = sum(tags.values())
    L.append(f"## {primary_label} capability layering (per step) + preservation")
    L.append(f"- step method tags: " + ", ".join(f"`{k}`={v}" for k, v in sorted(tags.items(), key=lambda x: -x[1])))
    if total_steps:
        anc = tags.get("anchorpatch", 0)
        L.append(f"- anchored-op steps: {anc}/{total_steps} ({100*anc/total_steps:.0f}%) "
                 f"| emit_file steps: {tel['emit_file_steps']} | fallback(FR) steps: {tags.get('full_rewrite_fallback',0)}")
    L.append(f"- mean op_accept_rate (anchored steps): {tel['mean_op_accept']:.3f}")
    L.append(f"- **preservation_violations (live executor assertion): {tel['preservation_violations']}**")
    L.append(f"- mean verbatim block survival: {tel['mean_survival']:.3f} | mean byte preservation: {tel['mean_preservation']:.3f}")
    L.append("")

    # no-op / fixed point / critical / tokens
    ap_noop, ap_fwd = fixed_point_chains(primary_rows)
    fr_noop, fr_fwd = fixed_point_chains(fr_rows)
    ap_crit, ap_tr = critical_failures(primary_rows, critical_theta)
    fr_crit, fr_tr = critical_failures(fr_rows, critical_theta)
    apt, frt = tokens(primary_rows), tokens(fr_rows)
    L.append("## Inflation guards, critical failures, tokens")
    L.append(f"- no-op forward steps: {primary_label} {ap_noop}/{ap_fwd}, FullRewrite {fr_noop}/{fr_fwd}")
    L.append(f"- critical failures (backward RS drop >= {critical_theta:.2f} or collapse to 0 between round trips): "
             f"{primary_label} {ap_crit}/{ap_tr}, FullRewrite {fr_crit}/{fr_tr}")
    L.append(f"- tokens (prompt+completion): {primary_label} {apt[2]:,} ({apt[0]:,}+{apt[1]:,}), "
             f"FullRewrite {frt[2]:,} ({frt[0]:,}+{frt[1]:,})")
    L.append("")

    if hybrid_rows:
        hk = rs_at_k(hybrid_rows, args.K)
        L.append("## HybridPatch — RS@k")
        L.append("| k | " + " | ".join(f"{k}" for k in range(1, args.K + 1)) + " |")
        L.append("|---|" + "---|" * args.K)
        L.append("| HybridPatch | " + " | ".join(f"{hk[k]:.3f}" for k in range(1, args.K + 1)) + " |")
        L.append("")
        L.append("## HybridPatch vs FullRewrite (paired backward RS)")
        L.append("| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |")
        L.append("|---|---|---|---|---|---|---|---|")
        hh, ff, _ = paired(hybrid_rows, fr_rows)
        L.append("| hybridpatch vs FR " + fmt(paired_stats(hh, ff)))
        hc, ht = critical_failures(hybrid_rows, critical_theta)
        fc, ft = critical_failures(fr_rows, critical_theta)
        L.append("")
        L.append(f"CriticalFailure@{args.K}: hybridpatch {hc}/{ht}, fullrewrite {fc}/{ft} "
                 f"(theta={critical_theta:.2f}; collapse-to-0 always counted).")
        L.append("")
        hm = hybrid_metrics(hybrid_rows)
        if hm:
            L.append("## HybridPatch telemetry")
            _emit_hybrid_telemetry(L, hm)

    report = "\n".join(L)
    with open(os.path.join(out_dir, "comparison.md"), "w", encoding="utf-8") as fh:
        fh.write(report + "\n")
    print(report)
    print(f"\n[analyze] wrote {os.path.join(out_dir, 'comparison.md')} and experiment_results.csv")


if __name__ == "__main__":
    main()
