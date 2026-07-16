# exp_20260711_hybridv7dev20full experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Live validation of HybridPatch/7 partial-acceptance policy on dev20.
- Lifecycle: `complete`
- Evidence role: `primary`
- Claim eligibility: `canonical`
- Canonical scope: `dev20_complete_pairs` (20/20 samples)
- Canonical backward rows: 400
- Comparison policy: HybridPatch live arm paired with immutable FullRewrite baseline rows

## Run identity

- Code reference: `HP_V7/VERSION.md`
- Protocol: `hybridpatch/7`
- Dataset / split: `DELEGATE-52` / `frozen dev20`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-11T06:22:03`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src\experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260711_hybridv7dev20full --notes hybridpatch/7 partial-acceptance full dev20 single-arm campaign vs frozen FR baseline, key=<key_label>
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.888 | 0.968 | 0.878 | 0.828 | 9 | 15577399 | applied=389, kept_context=6, partial_applied=5 |
| `fullrewrite` | 0.726 | 0.959 | 0.784 | 0.632 | 13 | 7084848 | not_applicable=397, unchanged=3 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `dev20_complete_pairs` | `hybridpatch` | `fullrewrite` | 200 | 0.888 | 0.726 | +0.161 | 0.000 | 0.465 | yes |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.161 across 200 paired backward rows (p=0.000).
   **Interpretation:** Within this exact canonical scope, the paired result supports a HybridPatch advantage.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 2.20× FullRewrite (15577399 vs 7084848). Failure labels: evaluator=1, gate=10, model=15, quality=19, unknown=1.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=400.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260711_hybridv7dev20full:translation4:rt05:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 5 / `backward` | fullrewrite: RS=0.000, not_applicable, near_empty_response; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_hybridv7dev20full:translation4:rt04:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 4 / `backward` | fullrewrite: RS=0.000, not_applicable, near_empty_response; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_hybridv7dev20full:translation4:rt03:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_hybridv7dev20full:satellite6:rt09:backward:paired:fullrewrite-win` | fullrewrite_win | `satellite6` / 9 / `backward` | fullrewrite: RS=0.853, not_applicable, none; hybridpatch: RS=0.000, kept_context, empty_response | -0.853 |
| `exp_20260711_hybridv7dev20full:satellite6:rt10:backward:paired:fullrewrite-win` | fullrewrite_win | `satellite6` / 10 / `backward` | fullrewrite: RS=0.853, not_applicable, none; hybridpatch: RS=0.000, applied, evaluator_error | -0.853 |
| `exp_20260711_hybridv7dev20full:starcatalog4:rt05:backward:paired:fullrewrite-win` | fullrewrite_win | `starcatalog4` / 5 / `backward` | fullrewrite: RS=0.986, not_applicable, none; hybridpatch: RS=0.528, applied, content_regression | -0.458 |
| `exp_20260711_hybridv7dev20full:edifact6:rt01:backward:paired:near-tie` | near_tie | `edifact6` / 1 / `backward` | fullrewrite: RS=0.960, not_applicable, none; hybridpatch: RS=0.960, applied, none | +0.000 |
| `exp_20260711_hybridv7dev20full:edifact6:rt02:backward:paired:near-tie` | near_tie | `edifact6` / 2 / `backward` | fullrewrite: RS=0.960, not_applicable, none; hybridpatch: RS=0.960, applied, none | +0.000 |
| `exp_20260711_hybridv7dev20full:fonteng3:rt07:backward:hybridpatch:partial-acceptance` | partial_acceptance, failure:validation_gate | `fonteng3` / 7 / `backward` | hybridpatch: RS=0.933, partial_applied, validation_gate | n/a |
| `exp_20260711_hybridv7dev20full:latex2:rt07:backward:hybridpatch:partial-acceptance` | partial_acceptance, failure:validation_gate | `latex2` / 7 / `backward` | hybridpatch: RS=0.995, partial_applied, validation_gate | n/a |
| `exp_20260711_hybridv7dev20full:mathlean2:rt04:backward:hybridpatch:partial-acceptance` | partial_acceptance, failure:validation_gate | `mathlean2` / 4 / `backward` | hybridpatch: RS=0.982, partial_applied, validation_gate | n/a |
| `exp_20260711_hybridv7dev20full:protein1:rt05:backward:hybridpatch:kept-context` | kept_context, failure:format_regression | `protein1` / 5 / `backward` | hybridpatch: RS=0.049, kept_context, format_regression | n/a |
| `exp_20260711_hybridv7dev20full:satellite6:rt09:backward:hybridpatch:kept-context` | kept_context, failure:empty_response | `satellite6` / 9 / `backward` | hybridpatch: RS=0.000, kept_context, empty_response | n/a |
| `exp_20260711_hybridv7dev20full:mathlean2:rt01:forward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `mathlean2` / 1 / `forward` | hybridpatch: RS=n/a, kept_context, validation_gate | n/a |
| `exp_20260711_hybridv7dev20full:docker6:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `docker6` / 2 / `backward` | fullrewrite: RS=0.049, not_applicable, content_regression | n/a |
| `exp_20260711_hybridv7dev20full:foodmenu6:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 2 / `backward` | fullrewrite: RS=0.350, not_applicable, content_regression | n/a |

## Failures and exclusions

- Non-none failure rows: 46
- Affected sample-method pairs: 22
- Failure stages: evaluator=1, gate=10, model=15, quality=19, unknown=1
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: Current HP_V7 runtime fingerprints differ from the archived runtime in experiment_runner.py; archive metadata is authoritative.
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

The original source report `HP_V7/exp_20260711_hybridv7dev20full/analysis/comparison.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# HybridPatch vs FullRewrite — diagnostic comparison

Model: minimax-m3 | samples: circuit2, docker6, edifact6, fonteng3, foodmenu6, geotrack3, landmarks1, latex2, makefile4, malware6, mathlean2, molecule2, musicsheet2, protein1, python7, quantum4, satellite6, screenplay6, starcatalog4, translation4 (n=20) | round trips: 10 | distractor: EXCLUDED

> Positioning: a **diagnostic** experiment (small n; accounting is a known AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). RS is the headline metric per request; preservation is the reliable secondary. Not a final universal superiority claim.

## RS@k (overall, failures counted as 0)
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.968 | 0.937 | 0.932 | 0.928 | 0.878 | 0.852 | 0.850 | 0.874 | 0.829 | 0.828 |
| FullRewrite | 0.959 | 0.816 | 0.715 | 0.754 | 0.784 | 0.719 | 0.625 | 0.626 | 0.630 | 0.632 |

## RS@{1,5,10} per domain
| domain | method | RS@1 | RS@5 | RS@10 |
|---|---|---|---|---|
| circuit | HybridPatch | 1.000 | 1.000 | 1.000 |
| circuit | FullRewrite | 1.000 | 1.000 | 1.000 |
| docker | HybridPatch | 0.981 | 0.969 | 0.969 |
| docker | FullRewrite | 0.951 | 0.112 | 0.114 |
| edifact | HybridPatch | 0.960 | 0.960 | 0.960 |
| edifact | FullRewrite | 0.960 | 0.960 | 1.000 |
| fonteng | HybridPatch | 1.000 | 0.933 | 0.933 |
| fonteng | FullRewrite | 1.000 | 0.942 | 0.942 |
| foodmenu | HybridPatch | 0.998 | 0.998 | 0.865 |
| foodmenu | FullRewrite | 0.999 | 0.999 | 0.880 |
| geotrack | HybridPatch | 1.000 | 0.844 | 0.844 |
| geotrack | FullRewrite | 0.844 | 0.734 | 0.824 |
| landmarks | HybridPatch | 1.000 | 1.000 | 1.000 |
| landmarks | FullRewrite | 1.000 | 1.000 | 1.000 |
| latex | HybridPatch | 0.999 | 0.996 | 0.759 |
| latex | FullRewrite | 0.999 | 0.996 | 0.833 |
| makefile | HybridPatch | 0.950 | 0.899 | 0.891 |
| makefile | FullRewrite | 0.950 | 0.930 | 0.921 |
| malware | HybridPatch | 1.000 | 1.000 | 1.000 |
| malware | FullRewrite | 1.000 | 1.000 | 0.080 |
| mathlean | HybridPatch | 0.995 | 0.982 | 0.907 |
| mathlean | FullRewrite | 0.991 | 0.903 | 0.933 |
| molecule | HybridPatch | 1.000 | 1.000 | 1.000 |
| molecule | FullRewrite | 1.000 | 1.000 | 1.000 |
| musicsheet | HybridPatch | 0.563 | 0.490 | 0.413 |
| musicsheet | FullRewrite | 0.647 | 0.468 | 0.386 |
| protein | HybridPatch | 0.933 | 0.049 | 0.994 |
| protein | FullRewrite | 0.976 | 0.000 | 0.000 |
| python | HybridPatch | 1.000 | 1.000 | 1.000 |
| python | FullRewrite | 1.000 | 1.000 | 1.000 |
| quantum | HybridPatch | 1.000 | 1.000 | 0.985 |
| quantum | FullRewrite | 0.879 | 0.879 | 0.003 |
| satellite | HybridPatch | 1.000 | 0.905 | 0.000 |
| satellite | FullRewrite | 1.000 | 0.924 | 0.853 |
| screenplay | HybridPatch | 1.000 | 0.997 | 0.997 |
| screenplay | FullRewrite | 1.000 | 0.852 | 0.868 |
| starcatalog | HybridPatch | 0.987 | 0.528 | 0.528 |
| starcatalog | FullRewrite | 0.987 | 0.986 | 0.000 |
| translation | HybridPatch | 1.000 | 1.000 | 0.513 |
| translation | FullRewrite | 1.000 | 0.000 | 0.000 |

## Paired same-task comparison (backward RS, matched by sample+round-trip)
| condition | n pairs | mean HybridPatch | mean FullRewrite | Δ | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| all pairs | 200 | 0.888 | 0.726 | +0.161 | 6.58 | 0.000 | 0.47 |
| ECR-conditioned (forward actually edited) | 193 | 0.894 | 0.740 | +0.154 | 6.49 | 0.000 | 0.47 |

## HybridPatch capability layering (per step) + preservation
- step method tags: `hybridpatch`=394, `hybridpatch_protocol_failure_kept_context`=6
- anchored-op steps: 0/400 (0%) | emit_file steps: 0 | fallback(FR) steps: 0
- mean op_accept_rate (anchored steps): 0.994
- **preservation_violations (live executor assertion): 0**
- mean verbatim block survival: 0.373 | mean byte preservation: 0.336

## Inflation guards, critical failures, tokens
- no-op forward steps: HybridPatch 4/200, FullRewrite 3/200
- critical failures (backward RS drop >= 0.10 or collapse to 0 between round trips): HybridPatch 9/180, FullRewrite 13/180
- tokens (prompt+completion): HybridPatch 15,577,399 (4,216,889+11,360,510), FullRewrite 7,084,848 (1,448,869+5,635,979)

## HybridPatch — RS@k
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.968 | 0.937 | 0.932 | 0.928 | 0.878 | 0.852 | 0.850 | 0.874 | 0.829 | 0.828 |

## HybridPatch vs FullRewrite (paired backward RS)
| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| hybridpatch vs FR | 200 | 0.888 | 0.726 | +0.161 | 6.58 | 0.000 | 0.47 |

CriticalFailure@10: hybridpatch 9/180, fullrewrite 13/180 (theta=0.10; collapse-to-0 always counted).

## HybridPatch telemetry
- steps with hybrid telemetry: 400
- route share: `bounded_rewrite`=234/400 (58.5%), `bulk_patch`=45/400 (11.2%), `dsl_rules`=1/400 (0.2%), `hybridpatch_protocol_failure_kept_context`=1/400 (0.2%), `local_patch`=119/400 (29.8%)
- bounded rewrite share: 234/400 (58.5%)
- copied/generated bytes: 1819417/2855932 | copy:generated ratio=0.6370659385447552 | mean generated byte ratio=0.603
- repair: attempted=36 used=29 success=24 rate=9.0%
- gate failures: 10 (2.5%) | kept-context failures: 6 (1.5%) | partial acceptance (v7): 5 (1.2%)
- forward no-effective-modification audit: 2/200 (1.0%)
````

</details>
