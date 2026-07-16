# exp_20260713_problem15_official experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Official non-stream problem-sample diagnostic and four-chain fill source for FR+Official.
- Lifecycle: `incomplete`
- Evidence role: `transport`
- Claim eligibility: `diagnostic_only`
- Canonical scope: `committed_partial_11_samples` (11/15 samples)
- Canonical backward rows: 109
- Comparison policy: transport diagnostic only; incomplete campaign cannot support a method comparison

## Run identity

- Code reference: `transport/docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md`
- Protocol: `hybridpatch/7`
- Dataset / split: `DELEGATE-52` / `15 preselected problem samples`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `minimax_official_nonstream/1`
- Seed: `42`
- Started: `2026-07-12T22:25:28`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 3

### Recorded command templates

````text
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260713_problem15_official --notes problem15 FR-only, MiniMax official nonstream, worker A
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260713_problem15_official --notes problem15 FR-only, MiniMax official nonstream, worker B
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260713_problem15_official --notes problem15 HP arm, robotics1, display-fix shakedown, MiniMax official nonstream
python src/experiment_runner.py --sample <samples> --methods hybridpatch fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260713_problem15_official --notes problem15 diagnostic, MiniMax official nonstream baseline-aligned, worker A
python src/experiment_runner.py --sample <samples> --methods hybridpatch fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260713_problem15_official --notes problem15 diagnostic, MiniMax official nonstream baseline-aligned, worker B
python src/experiment_runner.py --sample <samples> --methods hybridpatch fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260713_problem15_official --notes problem15 diagnostic, MiniMax official nonstream baseline-aligned, worker C
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.978 | 1.000 | n/a | n/a | 0 | 639797 | applied=7, kept_context=6, unchanged=5 |
| `fullrewrite` | 0.082 | 0.344 | 0.079 | 0.004 | 12 | 1081173 | not_applicable=185, unchanged=15 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `committed_partial_11_samples` | `hybridpatch` | `fullrewrite` | 9 | 0.978 | 0.322 | +0.655 | 0.002 | 1.471 | no |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.655 across 9 paired backward rows (p=0.002).
   **Interpretation:** This comparison is supporting or diagnostic evidence and must not be promoted to a standalone method claim.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 0.59× FullRewrite (639797 vs 1081173). Failure labels: evaluator=4, infrastructure=382, model=63, quality=27.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=109.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260713_problem15_official:robotics1:rt01:backward:paired:hybridpatch-win` | hybridpatch_win | `robotics1` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response; hybridpatch: RS=1.000, unchanged, none | +1.000 |
| `exp_20260713_problem15_official:filesystem3:rt02:backward:paired:hybridpatch-win` | hybridpatch_win | `filesystem3` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, none; hybridpatch: RS=1.000, unchanged, none | +1.000 |
| `exp_20260713_problem15_official:filesystem3:rt01:backward:paired:hybridpatch-win` | hybridpatch_win | `filesystem3` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, near_empty_response; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260713_problem15_official:crystal6:rt01:backward:paired:near-tie` | near_tie | `crystal6` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, unchanged, none | +0.000 |
| `exp_20260713_problem15_official:json4:rt01:backward:paired:near-tie` | near_tie | `json4` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, unchanged, none | +0.000 |
| `exp_20260713_problem15_official:json4:rt02:backward:hybridpatch:kept-context` | kept_context, failure:empty_response | `json4` / 2 / `backward` | hybridpatch: RS=0.934, kept_context, empty_response | n/a |
| `exp_20260713_problem15_official:crystal6:rt01:forward:hybridpatch:kept-context` | kept_context, failure:empty_response | `crystal6` / 1 / `forward` | hybridpatch: RS=n/a, kept_context, empty_response | n/a |
| `exp_20260713_problem15_official:filesystem3:rt02:forward:hybridpatch:kept-context` | kept_context, failure:empty_response | `filesystem3` / 2 / `forward` | hybridpatch: RS=0.000, kept_context, empty_response | n/a |
| `exp_20260713_problem15_official:crystal6:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:text_truncated | `crystal6` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, text_truncated | n/a |
| `exp_20260713_problem15_official:crystal6:rt05:backward:fullrewrite:critical-failure` | critical_failure, failure:empty_response | `crystal6` / 5 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response | n/a |
| `exp_20260713_problem15_official:crystal6:rt07:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `crystal6` / 7 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_problem15_official:crystal6:rt08:backward:fullrewrite:failure-event` | failure_event, failure:empty_response | `crystal6` / 8 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response | n/a |
| `exp_20260713_problem15_official:crystal6:rt09:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `crystal6` / 9 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260713_problem15_official:crystal6:rt10:backward:fullrewrite:failure-event` | failure_event, failure:empty_response | `crystal6` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response | n/a |
| `exp_20260713_problem15_official:crystal6:rt02:backward:hybridpatch:canonical-exclusion` | canonical_exclusion, failure:infrastructure_incomplete | `crystal6` / 2 / `backward` | hybridpatch: RS=n/a, missing, infrastructure_incomplete | n/a |
| `exp_20260713_problem15_official:crystal6:rt03:backward:hybridpatch:canonical-exclusion` | canonical_exclusion, failure:infrastructure_incomplete | `crystal6` / 3 / `backward` | hybridpatch: RS=n/a, missing, infrastructure_incomplete | n/a |

## Failures and exclusions

- Non-none failure rows: 476
- Affected sample-method pairs: 30
- Failure stages: evaluator=4, infrastructure=382, model=63, quality=27
- Canonical exclusions: infrastructure_incomplete=382
- Excluded `foodmenu6`: No committed result rows.
- Excluded `mathlean2`: No committed result rows.
- Excluded `satellite6`: No committed result rows.
- Excluded `translation4`: No committed result rows.

## Known exceptions and provenance gaps

- Known exception: The problem-sample selection cannot estimate provider background rates.
- Known exception: Four complete FullRewrite chains are consumed by FR+Official.
- Verification exception: The campaign is intentionally frozen partial and must not be analyzed as a complete paired experiment.
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

Repository source: [transport/docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md](../../docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md).
