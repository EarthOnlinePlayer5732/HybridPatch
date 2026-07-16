# fr_plus_official experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Logical 233-sample derived baseline combining complete official trajectories with frozen FullRewrite fallback trajectories.
- Lifecycle: `complete`
- Evidence role: `sensitivity`
- Claim eligibility: `supporting`
- Canonical scope: `logical233` (233/233 samples)
- Canonical backward rows: 2330
- Comparison policy: canonical only for the FR+Official derived sensitivity baseline; not a replacement for same-transport method comparison

## Run identity

- Code reference: `Baseline/build_fr_plus_official.py`
- Protocol: `n/a`
- Dataset / split: `DELEGATE-52` / `233-sample logical baseline view`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `n/a`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 0

### Recorded command templates

No command template was recorded for this historical experiment.

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `fullrewrite` | 0.564 | 0.735 | 0.550 | 0.508 | 162 | 71420901 | not_applicable=4555, unchanged=105 |

### Paired comparisons

No paired comparison is defined for this experiment.

## Paper protocol reporting

- Protocol condition: When the paper defines FullRewrite by the MiniMax official non-streaming one-shot response policy, use the FR+Official strict38 comparison.
- Reporting status: `paper_protocol_aligned_available_estimate`
- Scope: `strict38_paper_protocol`
- Official FullRewrite replacements in this scope: 16

| Paired rows | HybridPatch | FR+Official | Δ HP−FR | p | Cohen d |
|---:|---:|---:|---:|---:|---:|
| 380 | 0.825442 | 0.563780 | +0.261662 | 2.542e-28 | 0.615 |

**Paper interpretation:** This is the result aligned with the paper's stated non-streaming one-shot baseline processing policy.

**Required validity disclosure:** It remains a mixed-source post-hoc estimate because only 16 strict38 FullRewrite chains have complete official reruns; report this caveat alongside the result.

Sources: `Baseline/FR+Official/val40_comparison.md` and `Baseline/FR+Official/summary.json`.

## Analysis for the next iteration

1. **Observation:** `fullrewrite` mean backward RS is 0.564 over 2330 canonical rows.
   **Interpretation:** This is a single-arm, baseline, source, or diagnostic record; it does not identify a paired method effect.
   **Implication:** Use it only for the evidence role declared above.
   **Next step:** Compare it only through a catalogued derivation or a new controlled paired experiment.

2. **Observation:** No cross-method token ratio is defined. Failure labels: evaluator=33, model=403, quality=279, transport=4.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `not_run`; replayed backward rows=n/a.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `fr_plus_official:accounting2:rt01:backward:fullrewrite:high-score` | high_score | `accounting2` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `fr_plus_official:accounting3:rt01:backward:fullrewrite:high-score` | high_score | `accounting3` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `fr_plus_official:accounting3:rt02:backward:fullrewrite:high-score` | high_score | `accounting3` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `fr_plus_official:accounting2:rt04:backward:fullrewrite:low-score` | low_score | `accounting2` / 4 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `fr_plus_official:accounting2:rt05:backward:fullrewrite:low-score` | low_score | `accounting2` / 5 / `backward` | fullrewrite: RS=0.000, not_applicable, none | n/a |
| `fr_plus_official:accounting2:rt06:backward:fullrewrite:low-score` | low_score | `accounting2` / 6 / `backward` | fullrewrite: RS=0.000, not_applicable, none | n/a |
| `fr_plus_official:accounting3:rt05:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `accounting3` / 5 / `backward` | fullrewrite: RS=0.844, not_applicable, content_regression | n/a |
| `fr_plus_official:accounting4:rt07:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `accounting4` / 7 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `fr_plus_official:accounting6:rt05:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `accounting6` / 5 / `backward` | fullrewrite: RS=0.810, not_applicable, content_regression | n/a |
| `fr_plus_official:audiosyn1:rt02:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `audiosyn1` / 2 / `backward` | fullrewrite: RS=0.041, not_applicable, content_regression | n/a |
| `fr_plus_official:calendar1:rt03:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `calendar1` / 3 / `backward` | fullrewrite: RS=0.779, not_applicable, content_regression | n/a |
| `fr_plus_official:calendar1:rt08:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `calendar1` / 8 / `backward` | fullrewrite: RS=0.461, not_applicable, content_regression | n/a |

## Failures and exclusions

- Non-none failure rows: 719
- Affected sample-method pairs: 145
- Failure stages: evaluator=33, model=403, quality=279, transport=4
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: The view mixes transport sources.
- Known exception: Official trajectories come from a post-hoc anomalous/problem sample pool.
- Known exception: The strict38 delta is endpoint/policy sensitivity, not an unbiased method-effect estimate.
- Verification exception: Independent result replay was not rerun; the view inherits source results and validates file/task-plan hashes plus complete-chain structure.
- Provenance gap `code.run_git_commit`: The historical run metadata did not record a Git commit. Impact: Exact commit cannot be asserted; archived per-file fingerprints remain authoritative.
- Provenance gap `code.git_tree_state`: The historical run metadata did not record whether the run tree was clean or dirty. Impact: Uncommitted run-time edits cannot be ruled out from Git metadata alone.
- Provenance gap `time.finished_at`: No explicit completion timestamp was recorded. Impact: The latest worker start is not treated as an experiment finish time.
- Provenance gap `model.transport_revision`: Legacy run metadata did not record a transport revision. Impact: Transport semantics must be interpreted from versioned project documentation.

## Audit and provenance

- [`experiment.yaml`](experiment.yaml): run identity, configuration, scope policy, source reports, and known gaps.
- [`summary.json`](summary.json): canonical machine metrics.
- [`samples.csv`](samples.csv): complete planned row grid.
- [`casebook.jsonl`](casebook.jsonl): representative comparison and failure cases.
- [`failure_stats.json`](failure_stats.json): failure counts.
- [`representative_failures.jsonl`](representative_failures.jsonl): failure-focused evidence.
- [`verification.txt`](verification.txt): fixed-field verifier evidence.
- [`raw_manifest.json`](raw_manifest.json): private raw artifact hash and retention status.

Raw prompts, full source documents, model response bodies, credentials, HTTP headers, and local absolute paths are intentionally not included.

## Source human report

Repository source: [Baseline/FR+Official/README.md](../../FR+Official/README.md).
