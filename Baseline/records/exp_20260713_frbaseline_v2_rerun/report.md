# exp_20260713_frbaseline_v2_rerun experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Official non-stream rerun source for FR+Official; only complete chains are eligible.
- Lifecycle: `incomplete`
- Evidence role: `supporting`
- Claim eligibility: `none`
- Canonical scope: `complete83_source_chains` (83/85 samples)
- Canonical backward rows: 830
- Comparison policy: source-only; no standalone method headline

## Run identity

- Code reference: `transport/docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md`
- Protocol: `n/a`
- Dataset / split: `DELEGATE-52` / `85 preselected historically anomalous baseline samples`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `minimax_official_nonstream/1`
- Seed: `42`
- Started: `2026-07-13T02:34:56`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python .\src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --seed 42 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir ..\Baseline\exp_20260713_frbaseline_v2_rerun --notes FR baseline v2 contaminated rerun, one-shot official nonstream, worker A
python .\src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --seed 42 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir ..\Baseline\exp_20260713_frbaseline_v2_rerun --notes FR baseline v2 contaminated rerun, one-shot official nonstream, worker B
python .\src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --seed 42 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir ..\Baseline\exp_20260713_frbaseline_v2_rerun --notes FR baseline v2 contaminated rerun, one-shot official nonstream, worker C
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `fullrewrite` | 0.145 | 0.357 | 0.117 | 0.115 | 85 | 12426967 | not_applicable=1572, unchanged=88 |

### Paired comparisons

No paired comparison is defined for this experiment.

## Analysis for the next iteration

1. **Observation:** `fullrewrite` mean backward RS is 0.145 over 830 canonical rows.
   **Interpretation:** This is a single-arm, baseline, source, or diagnostic record; it does not identify a paired method effect.
   **Implication:** Use it only for the evidence role declared above.
   **Next step:** Compare it only through a catalogued derivation or a new controlled paired experiment.

2. **Observation:** No cross-method token ratio is defined. Failure labels: evaluator=23, infrastructure=34, model=375, quality=186, transport=4.
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
| `exp_20260713_frbaseline_v2_rerun:dbschema5:rt01:backward:fullrewrite:high-score` | high_score | `dbschema5` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260713_frbaseline_v2_rerun:emails5:rt01:backward:fullrewrite:high-score` | high_score | `emails5` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260713_frbaseline_v2_rerun:foodmenu5:rt01:backward:fullrewrite:high-score` | high_score | `foodmenu5` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none | n/a |
| `exp_20260713_frbaseline_v2_rerun:calendar5:rt01:backward:fullrewrite:low-score` | low_score | `calendar5` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_frbaseline_v2_rerun:calendar5:rt02:backward:fullrewrite:low-score` | low_score | `calendar5` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, text_truncated | n/a |
| `exp_20260713_frbaseline_v2_rerun:calendar5:rt03:backward:fullrewrite:low-score` | low_score | `calendar5` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_frbaseline_v2_rerun:chess6:rt03:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `chess6` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_frbaseline_v2_rerun:chess6:rt06:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `chess6` / 6 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_frbaseline_v2_rerun:dbschema5:rt03:backward:fullrewrite:critical-failure` | critical_failure, failure:empty_response | `dbschema5` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response | n/a |
| `exp_20260713_frbaseline_v2_rerun:calendar5:rt04:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `calendar5` / 4 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_frbaseline_v2_rerun:calendar5:rt05:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `calendar5` / 5 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_frbaseline_v2_rerun:chess6:rt01:backward:fullrewrite:failure-event` | failure_event, failure:text_truncated | `chess6` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, text_truncated | n/a |
| `exp_20260713_frbaseline_v2_rerun:musicsheet3:rt01:backward:fullrewrite:canonical-exclusion` | canonical_exclusion, failure:infrastructure_incomplete | `musicsheet3` / 1 / `backward` | fullrewrite: RS=n/a, missing, infrastructure_incomplete | n/a |
| `exp_20260713_frbaseline_v2_rerun:musicsheet3:rt02:backward:fullrewrite:canonical-exclusion` | canonical_exclusion, failure:infrastructure_incomplete | `musicsheet3` / 2 / `backward` | fullrewrite: RS=n/a, missing, infrastructure_incomplete | n/a |
| `exp_20260713_frbaseline_v2_rerun:musicsheet3:rt03:backward:fullrewrite:canonical-exclusion` | canonical_exclusion, failure:infrastructure_incomplete | `musicsheet3` / 3 / `backward` | fullrewrite: RS=n/a, missing, infrastructure_incomplete | n/a |
| `exp_20260713_frbaseline_v2_rerun:musicsheet3:rt04:backward:fullrewrite:canonical-exclusion` | canonical_exclusion, failure:infrastructure_incomplete | `musicsheet3` / 4 / `backward` | fullrewrite: RS=n/a, missing, infrastructure_incomplete | n/a |

## Failures and exclusions

- Non-none failure rows: 622
- Affected sample-method pairs: 85
- Failure stages: evaluator=23, infrastructure=34, model=375, quality=186, transport=4
- Canonical exclusions: empty_response=1, infrastructure_incomplete=34, text_truncated=3
- Excluded `musicsheet3`: Incomplete or missing result trajectory; source-only canonical scope requires a complete chain.
- Excluded `transit2`: Incomplete or missing result trajectory; source-only canonical scope requires a complete chain.

## Known exceptions and provenance gaps

- Known exception: The sample pool was selected post-hoc for historical anomalies and cannot estimate official API background anomaly rates.
- Known exception: The rerun is frozen incomplete: transit2 has 3RT and musicsheet3 has no result file.
- Verification exception: No completed independent result replay is recorded for this rerun source.
- Verification exception: transit2 is partial and musicsheet3 has no result file; neither is selected into FR+Official.
- Provenance gap `code.run_git_commit`: The historical run metadata did not record a Git commit. Impact: Exact commit cannot be asserted; archived per-file fingerprints remain authoritative.
- Provenance gap `code.git_tree_state`: The historical run metadata did not record whether the run tree was clean or dirty. Impact: Uncommitted run-time edits cannot be ruled out from Git metadata alone.
- Provenance gap `time.finished_at`: No explicit completion timestamp was recorded. Impact: The latest worker start is not treated as an experiment finish time.

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

No separate canonical source report exists. This generated report and `summary.json` are the declared Git review entry points.
