# exp_20260709_hybridv4smoke experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Seven-sample mechanism smoke for bulk file-level matching and routing pressure.
- Lifecycle: `complete`
- Evidence role: `diagnostic`
- Claim eligibility: `diagnostic_only`
- Canonical scope: `smoke7_complete` (7/7 samples)
- Canonical backward rows: 70
- Comparison policy: single-arm longitudinal diagnostic; not a live paired method claim

## Run identity

- Code reference: `HP_V4/VERSION.md`
- Protocol: `hybridpatch/4`
- Dataset / split: `DELEGATE-52` / `targeted seven-sample dev smoke`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-09T08:08:59`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv4smoke --notes hybridpatch/4 targeted smoke: D1(docker6/fonteng3) D2(mathlean2) D3(latex2) regression(malware6) P1-watch(geotrack3/foodmenu6)
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv4smoke --notes resume after 429 quota stop
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv4smoke --notes resume after weekly quota reset
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.887 | 0.976 | 0.871 | 0.898 | 3 | 4312772 | applied=140 |

### Paired comparisons

No paired comparison is defined for this experiment.

## Analysis for the next iteration

1. **Observation:** `hybridpatch` mean backward RS is 0.887 over 70 canonical rows.
   **Interpretation:** This is a single-arm, baseline, source, or diagnostic record; it does not identify a paired method effect.
   **Implication:** Use it only for the evidence role declared above.
   **Next step:** Compare it only through a catalogued derivation or a new controlled paired experiment.

2. **Observation:** No cross-method token ratio is defined. Failure labels: quality=3.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=70.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260709_hybridv4smoke:fonteng3:rt01:backward:hybridpatch:high-score` | high_score | `fonteng3` / 1 / `backward` | hybridpatch: RS=1.000, applied, none | n/a |
| `exp_20260709_hybridv4smoke:fonteng3:rt02:backward:hybridpatch:high-score` | high_score | `fonteng3` / 2 / `backward` | hybridpatch: RS=1.000, applied, none | n/a |
| `exp_20260709_hybridv4smoke:fonteng3:rt03:backward:hybridpatch:high-score` | high_score | `fonteng3` / 3 / `backward` | hybridpatch: RS=1.000, applied, none | n/a |
| `exp_20260709_hybridv4smoke:foodmenu6:rt02:backward:hybridpatch:low-score` | low_score | `foodmenu6` / 2 / `backward` | hybridpatch: RS=0.352, applied, content_regression | n/a |
| `exp_20260709_hybridv4smoke:foodmenu6:rt03:backward:hybridpatch:low-score` | low_score | `foodmenu6` / 3 / `backward` | hybridpatch: RS=0.352, applied, none | n/a |
| `exp_20260709_hybridv4smoke:foodmenu6:rt04:backward:hybridpatch:low-score` | low_score | `foodmenu6` / 4 / `backward` | hybridpatch: RS=0.352, applied, none | n/a |
| `exp_20260709_hybridv4smoke:fonteng3:rt07:backward:hybridpatch:critical-failure` | critical_failure, failure:content_regression | `fonteng3` / 7 / `backward` | hybridpatch: RS=0.772, applied, content_regression | n/a |
| `exp_20260709_hybridv4smoke:mathlean2:rt04:backward:hybridpatch:critical-failure` | critical_failure, failure:content_regression | `mathlean2` / 4 / `backward` | hybridpatch: RS=0.359, applied, content_regression | n/a |

## Failures and exclusions

- Non-none failure rows: 3
- Affected sample-method pairs: 3
- Failure stages: quality=3
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: The run was interrupted by quota limits and later completed from checkpoints.
- Known exception: This targeted set is patch-friendly and is not an untargeted validation set.
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

The original source report `HP_V4/exp_20260709_hybridv4smoke/analysis/comparison.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# HybridPatch vs FullRewrite — diagnostic comparison

Model: minimax-m3 | samples: docker6, fonteng3, foodmenu6, geotrack3, latex2, malware6, mathlean2 (n=7) | round trips: 10 | distractor: EXCLUDED

> Positioning: a **diagnostic** experiment (small n; accounting is a known AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). RS is the headline metric per request; preservation is the reliable secondary. Not a final universal superiority claim.

## RS@k (overall, failures counted as 0)
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.976 | 0.883 | 0.877 | 0.778 | 0.871 | 0.870 | 0.912 | 0.909 | 0.899 | 0.898 |
| FullRewrite | nan | nan | nan | nan | nan | nan | nan | nan | nan | nan |

## RS@{1,5,10} per domain
| domain | method | RS@1 | RS@5 | RS@10 |
|---|---|---|---|---|
| docker | HybridPatch | 0.990 | 0.985 | 0.942 |
| docker | FullRewrite | nan | nan | nan |
| fonteng | HybridPatch | 1.000 | 0.930 | 0.772 |
| fonteng | FullRewrite | nan | nan | nan |
| foodmenu | HybridPatch | 0.998 | 0.998 | 0.983 |
| foodmenu | FullRewrite | nan | nan | nan |
| geotrack | HybridPatch | 0.844 | 0.824 | 0.824 |
| geotrack | FullRewrite | nan | nan | nan |
| latex | HybridPatch | 0.999 | 0.999 | 0.933 |
| latex | FullRewrite | nan | nan | nan |
| malware | HybridPatch | 1.000 | 1.000 | 1.000 |
| malware | FullRewrite | nan | nan | nan |
| mathlean | HybridPatch | 1.000 | 0.358 | 0.835 |
| mathlean | FullRewrite | nan | nan | nan |

## Paired same-task comparison (backward RS, matched by sample+round-trip)
| condition | n pairs | mean HybridPatch | mean FullRewrite | Δ | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| all pairs | 0 | nan | nan | n/a | n/a | n/a | n/a |
| ECR-conditioned (forward actually edited) | 0 | nan | nan | n/a | n/a | n/a | n/a |

## HybridPatch capability layering (per step) + preservation
- step method tags: `hybridpatch`=140
- anchored-op steps: 0/140 (0%) | emit_file steps: 0 | fallback(FR) steps: 0
- mean op_accept_rate (anchored steps): 1.000
- **preservation_violations (live executor assertion): 0**
- mean verbatim block survival: 0.572 | mean byte preservation: 0.527

## Inflation guards, critical failures, tokens
- no-op forward steps: HybridPatch 0/70, FullRewrite 0/0
- critical failures (backward RS drop >= 0.10 or collapse to 0 between round trips): HybridPatch 3/63, FullRewrite 0/0
- tokens (prompt+completion): HybridPatch 4,312,772 (1,460,108+2,852,664), FullRewrite 0 (0+0)

## HybridPatch — RS@k
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.976 | 0.883 | 0.877 | 0.778 | 0.871 | 0.870 | 0.912 | 0.909 | 0.899 | 0.898 |

## HybridPatch vs FullRewrite (paired backward RS)
| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| hybridpatch vs FR | 0 | nan | nan | n/a | n/a | n/a | n/a |

CriticalFailure@10: hybridpatch 3/63, fullrewrite 0/0 (theta=0.10; collapse-to-0 always counted).

## HybridPatch telemetry
- steps with hybrid telemetry: 140
- route share: `bounded_rewrite`=55/140 (39.3%), `bulk_patch`=23/140 (16.4%), `local_patch`=62/140 (44.3%)
- bounded rewrite share: 55/140 (39.3%)
- copied/generated bytes: 1111313/766654 | copy:generated ratio=1.449562644948047 | mean generated byte ratio=0.395
- repair: attempted=6 used=6 success=6 rate=4.3%
- gate failures: 0 (0.0%) | kept-context failures: 0 (0.0%)
- forward no-effective-modification audit: 0/70 (0.0%)
````

</details>
