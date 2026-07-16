# exp_20260710_frbaseline234 experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Frozen one-shot FullRewrite baseline; canonical scope excludes python1.
- Lifecycle: `complete`
- Evidence role: `baseline`
- Claim eligibility: `canonical`
- Canonical scope: `frozen233` (233/234 samples)
- Canonical backward rows: 2330
- Comparison policy: 233-sample frozen baseline; archive analysis/comparison.md is an outdated 234-sample diagnostic

## Run identity

- Code reference: `Baseline/FR冻结计划.md`
- Protocol: `n/a`
- Dataset / split: `DELEGATE-52` / `233-sample frozen FullRewrite baseline; python1 excluded`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-10T07:48:32`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_frbaseline234 --notes FR frozen baseline one-shot 234, no rewind, key=<key_label>
python src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_frbaseline234 --notes FR frozen baseline python1 rerun after utils_eval reconstruction + win devnull shim + Levenshtein install, key=<key_label>
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `fullrewrite` | 0.685 | 0.925 | 0.682 | 0.558 | 167 | 85874973 | not_applicable=4617, unchanged=43 |

### Paired comparisons

No paired comparison is defined for this experiment.

## Analysis for the next iteration

1. **Observation:** `fullrewrite` mean backward RS is 0.685 over 2330 canonical rows.
   **Interpretation:** This is a single-arm, baseline, source, or diagnostic record; it does not identify a paired method effect.
   **Implication:** Use it only for the evidence role declared above.
   **Next step:** Compare it only through a catalogued derivation or a new controlled paired experiment.

2. **Observation:** No cross-method token ratio is defined. Failure labels: evaluator=15, infrastructure=12, model=116, quality=230.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=2330.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260710_frbaseline234:accounting2:rt01:backward:fullrewrite:high-score` | high_score | `accounting2` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:accounting3:rt01:backward:fullrewrite:high-score` | high_score | `accounting3` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:accounting3:rt02:backward:fullrewrite:high-score` | high_score | `accounting3` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:accounting2:rt04:backward:fullrewrite:low-score` | low_score | `accounting2` / 4 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:accounting2:rt05:backward:fullrewrite:low-score` | low_score | `accounting2` / 5 / `backward` | fullrewrite: RS=0.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:accounting2:rt06:backward:fullrewrite:low-score` | low_score | `accounting2` / 6 / `backward` | fullrewrite: RS=0.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:accounting3:rt05:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `accounting3` / 5 / `backward` | fullrewrite: RS=0.844, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:accounting4:rt07:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `accounting4` / 7 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:accounting6:rt05:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `accounting6` / 5 / `backward` | fullrewrite: RS=0.810, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:audiosyn1:rt02:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `audiosyn1` / 2 / `backward` | fullrewrite: RS=0.041, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:calendar1:rt03:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `calendar1` / 3 / `backward` | fullrewrite: RS=0.779, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:calendar1:rt08:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `calendar1` / 8 / `backward` | fullrewrite: RS=0.461, not_applicable, content_regression | n/a |
| `exp_20260710_frbaseline234:python1:rt01:backward:fullrewrite:canonical-exclusion` | canonical_exclusion | `python1` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:python1:rt02:backward:fullrewrite:canonical-exclusion` | canonical_exclusion | `python1` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:python1:rt03:backward:fullrewrite:canonical-exclusion` | canonical_exclusion | `python1` / 3 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260710_frbaseline234:python1:rt04:backward:fullrewrite:canonical-exclusion` | canonical_exclusion | `python1` / 4 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |

## Failures and exclusions

- Non-none failure rows: 373
- Affected sample-method pairs: 137
- Failure stages: evaluator=15, infrastructure=12, model=116, quality=230
- Canonical exclusions: infrastructure_incomplete=12
- Excluded `python1`: Evaluator packaging failure; four committed RTs are retained but excluded from the 233-sample baseline.

## Known exceptions and provenance gaps

- Known exception: The baseline is a dated sample of provider behavior and uses the pre-transport-v3 OpenCode path.
- Known exception: run_metadata.jsonl records code fingerprints but not a Git commit.
- Verification exception: The linked verifier log covers the 2330 canonical rows; a separate archive-inclusive replay reports 2334 rows including four retained python1 rows outside the canonical baseline.
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

Repository source: [Baseline/FR冻结计划.md](../../FR冻结计划.md).
