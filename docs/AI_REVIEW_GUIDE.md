# AI code-and-results review guide

This repository is prepared so an external code-analysis model can inspect the
method, the experiment evidence, and the next-iteration decision without access
to private raw prompts or model responses.

## What `canonical` means here

For each experiment:

- `records/<experiment_id>/report.md` is the canonical **human-readable**
  review entry.
- `records/<experiment_id>/summary.json` is the canonical **machine scope**:
  the authoritative sample set, exclusions, metrics, and comparison.
- `casebook.jsonl` contains deterministic representative cases for diagnosis.
- `experiment.yaml > canonical_report.source_report_ref` points to the original
  human source analysis. If that source lived in an ignored raw archive, a
  sanitized snapshot is embedded in `report.md`.

`canonical` does not mean newest, highest-scoring, or most polished. It means
the one declared scope that may be used for the experiment's formal claim.

## Recommended reading order

1. [`README.md`](../README.md) and [`CLAUDE.md`](../CLAUDE.md) for repository
   boundaries and version discipline.
2. [`HYBRIDPATCH_DESIGN.md`](HYBRIDPATCH_DESIGN.md) for the current method and
   protocol semantics.
3. [`项目结构与迭代史.md`](项目结构与迭代史.md) for v3→v8 decisions and the
   current V9 decision gate.
4. [`标准实验流程.md`](标准实验流程.md) for how experiments are planned,
   independently reviewed, finalized, and archived.
5. [`EXPERIMENT_INDEX.md`](EXPERIMENT_INDEX.md) for all catalogued experiments.
6. If present, read `experiment_plans/<experiment_id>.md` before the result so
   planned hypotheses, exclusions, and decision rules are not reconstructed
   post-hoc.
7. The relevant experiment's `report.md`, then `casebook.jsonl`,
   `summary.json`, and `samples.csv`.
8. [`FINDINGS.md`](FINDINGS.md) and [`RESEARCH_JOURNAL.md`](RESEARCH_JOURNAL.md)
   only after the scoped records, for deeper causal hypotheses and chronology.
9. The matching `HP_Vx/src/`, tests, prompts, and `VERSION.md` when checking
   whether a proposed change is consistent with the observed mechanism.

## Current result boundary

- The current same-transport canonical result is the
  [HP_V7 strict38 report](../HP_V7/records/exp_20260712_hybridv7val40_transportv3/report.md):
  strict 38
  complete pairs, HybridPatch `0.825`, FullRewrite `0.817`,
  `Δ=+0.008`, `p=0.514`. It does not establish a statistically distinguishable
  overall advantage.
- If the paper defines its FullRewrite baseline processing as the MiniMax
  official API, non-streaming, one-shot complete-response policy, the
  paper-protocol-aligned result is the
  [FR+Official strict38 comparison](../Baseline/FR+Official/val40_comparison.md):
  `Δ=+0.262`.
- That `+0.262` result must carry its validity disclosure: only 16 strict38 FR
  chains have complete official reruns, the remaining chains use the frozen FR
  source, and the replacement pool was selected post-hoc. It is therefore the
  available paper-policy result, not a full-scope unbiased same-transport
  method-effect estimate.
- Earlier dev and val campaigns remain useful mechanism evidence, but their
  claim role is exactly the `claim_eligibility` declared in each record.
- The current V8 operational/method diagnostic is the
  [HP_V8 snapshotfix paired10 summary](../HP_V8/analysis/exp_20260718_hybridv8_transportv4_snapshotfix_paired10.md):
  ten previously exposed samples, exact backward RT10 HP `0.745` versus
  FullRewrite `0.696`, paired endpoint delta `+0.049`, and preservation
  `0/199 applicable + 1 N/A`. It is diagnostic-only, has large sample
  heterogeneity, and does not replace the strict38 same-transport result or
  prove that V8 prompt reduction caused a score change.
  Its official generated record bundle is privately retained rather than
  catalogued because the current catalog hash design would rewrite every frozen
  HP_V3–HP_V7/Baseline/transport record.

Do not conflate the two questions: use `+0.008` for the same-transport method
effect and `+0.262` when reporting the paper's stated official non-streaming
one-shot baseline policy, with the mixed-source disclosure attached.

## How to use the casebook

Each case records only identifiers, scores, route/status/outcome, failure label,
and a logical raw reference. Typical categories include:

- largest HybridPatch and FullRewrite paired advantages;
- closest paired outcomes;
- partial acceptance and kept-context;
- critical score drops and ordinary failure labels;
- excluded or missing planned rows.

Use these cases to locate recurring mechanisms in `samples.csv` and the code.
Do not infer the full prompt, document, or model response from the compact row.
If a proposed change depends on raw content, state that private raw-archive
inspection is still required.

## Expected review output

A useful next-iteration review should separate:

1. **Observation** — the exact code path or record evidence.
2. **Interpretation** — the mechanism that could explain it.
3. **Implication** — which claim or behavior is affected.
4. **Next change** — the smallest versioned modification.
5. **Test** — a zero-API regression or small controlled experiment that could
   falsify the interpretation.

Rank recommendations by expected effect on the current canonical weakness, not
by how interesting the change sounds. Preserve the byte-level preservation
invariant and do not edit frozen `HP_V3`–`HP_V7`; do not mutate active HP_V8
method semantics after its recorded run. A new method change begins in HP_V9.

## Suggested review prompt

```text
Review this repository as a research engineer. Read docs/AI_REVIEW_GUIDE.md
first and obey every experiment's claim_eligibility and canonical scope.

Goal: recommend the next HybridPatch iteration after HP_V8.

1. Audit whether the current code matches the documented v8 semantics without
   reinterpreting v1-v7 replay.
2. Analyze the canonical strict38 record and the diagnostic V8 paired10 record
   at their declared, different claim boundaries before using historical or
   sensitivity evidence.
3. Identify recurring, code-addressable failure mechanisms; distinguish model,
   protocol, executor, gate, evaluator, transport, and infrastructure causes.
4. Propose at most three V9 changes. For each give observation,
   interpretation, implication, minimal change, regression test, small paid
   experiment, expected metric movement, and risk.
5. State which conclusions require private raw-log inspection.

Treat FR+Official `+0.262` as the paper-protocol-aligned available result while
preserving its mixed-source disclosure; do not invent missing raw content, and
do not recommend modifying frozen HP_V3–HP_V7 or the recorded HP_V8 method.
```

## Known evidence gaps

- Raw `exp_*` archives remain private and are represented by hashes and logical
  references only.
- Some historical runs did not record an exact Git commit or finish timestamp;
  archived per-file fingerprints and explicit provenance gaps are authoritative.
- The transport-v3 five-sample end-to-end smoke artifact is retained outside
  this repository and is not available to a clean Git clone. Claims that depend
  on its raw contents require separate private artifact access.
- Pre-v3 experiments are summarized in the research documents but do not yet
  have the same compact per-experiment record coverage as v3–v7.
