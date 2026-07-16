# exp_20260711_opencode_transport_v2_smoke5 experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Forensic transport-v2 E2E smoke that exposed cumulative-snapshot O(n^2) raw-log amplification.
- Lifecycle: `superseded`
- Evidence role: `transport`
- Claim eligibility: `diagnostic_only`
- Canonical scope: `transport_v2_smoke5_complete` (5/5 samples)
- Canonical backward rows: 20
- Comparison policy: transport forensic evidence only; the paired score is not a method claim

## Run identity

- Code reference: `docs/FINDINGS.md`
- Protocol: `hybridpatch/7`
- Dataset / split: `DELEGATE-52` / `five representative transport-smoke samples`
- Round trips: 2
- Model: `minimax-m3`
- Transport revision: `opencode_anthropic_sdk/2`
- Seed: `42`
- Started: `2026-07-11T10:16:16`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 2

### Recorded command templates

````text
python src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 2 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260711_opencode_transport_v2_smoke5 --notes transport-v2 smoke, seed=42, no distractor, adaptive, max_tokens=131072, key=<key_label>
python src\experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 2 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260711_opencode_transport_v2_smoke5 --notes transport-v2 smoke, seed=42, no distractor, adaptive, max_tokens=131072, key=<key_label>
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.917 | 0.979 | n/a | n/a | 1 | 1016471 | applied=20 |
| `fullrewrite` | 0.725 | 0.785 | n/a | n/a | 1 | 723494 | not_applicable=20 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `transport_v2_smoke5_complete` | `hybridpatch` | `fullrewrite` | 10 | 0.917 | 0.725 | +0.192 | 0.179 | 0.461 | no |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.192 across 10 paired backward rows (p=0.179).
   **Interpretation:** This comparison is supporting or diagnostic evidence and must not be promoted to a standalone method claim.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 1.40× FullRewrite (1016471 vs 723494). Failure labels: model=2, quality=3.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=20.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260711_opencode_transport_v2_smoke5:translation4:rt01:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_opencode_transport_v2_smoke5:translation4:rt02:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, none; hybridpatch: RS=0.964, applied, none | +0.964 |
| `exp_20260711_opencode_transport_v2_smoke5:docker6:rt02:backward:paired:hybridpatch-win` | hybridpatch_win | `docker6` / 2 / `backward` | fullrewrite: RS=0.921, not_applicable, none; hybridpatch: RS=0.932, applied, none | +0.011 |
| `exp_20260711_opencode_transport_v2_smoke5:protein1:rt01:backward:paired:fullrewrite-win` | fullrewrite_win | `protein1` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.965, applied, none | -0.035 |
| `exp_20260711_opencode_transport_v2_smoke5:protein1:rt02:backward:paired:fullrewrite-win` | fullrewrite_win | `protein1` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.965, applied, none | -0.035 |
| `exp_20260711_opencode_transport_v2_smoke5:foodmenu6:rt01:backward:paired:fullrewrite-win` | fullrewrite_win | `foodmenu6` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.999, applied, none | -0.001 |
| `exp_20260711_opencode_transport_v2_smoke5:mathlean2:rt01:backward:paired:near-tie` | near_tie | `mathlean2` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.999, applied, none | -0.000 |
| `exp_20260711_opencode_transport_v2_smoke5:foodmenu6:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 2 / `backward` | fullrewrite: RS=0.417, not_applicable, content_regression | n/a |
| `exp_20260711_opencode_transport_v2_smoke5:foodmenu6:rt02:backward:hybridpatch:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 2 / `backward` | hybridpatch: RS=0.422, applied, content_regression | n/a |
| `exp_20260711_opencode_transport_v2_smoke5:translation4:rt01:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `translation4` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260711_opencode_transport_v2_smoke5:translation4:rt01:forward:fullrewrite:failure-event` | failure_event, failure:thinking_budget_exhausted | `translation4` / 1 / `forward` | fullrewrite: RS=0.000, not_applicable, thinking_budget_exhausted | n/a |
| `exp_20260711_opencode_transport_v2_smoke5:translation4:rt01:forward:hybridpatch:failure-event` | failure_event, failure:text_truncated | `translation4` / 1 / `forward` | hybridpatch: RS=n/a, applied, text_truncated | n/a |

## Failures and exclusions

- Non-none failure rows: 5
- Affected sample-method pairs: 4
- Failure stages: model=2, quality=3
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: The two arms do not share one final code fingerprint because retry/logging fixes landed between them.
- Known exception: The inflated transport logs are retained for forensic evidence and are not normal second-tier artifacts.
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

Repository source: [docs/FINDINGS.md](../../../docs/FINDINGS.md).
