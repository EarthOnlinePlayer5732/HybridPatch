# exp_20260711_hybridv6val40 experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: HybridPatch/6 validation on val20 plus unused_reserve20, paired with copied frozen FullRewrite rows.
- Lifecycle: `complete`
- Evidence role: `primary`
- Claim eligibility: `canonical`
- Canonical scope: `val40_complete_pairs` (40/40 samples)
- Canonical backward rows: 800
- Comparison policy: HybridPatch live validation paired with immutable FullRewrite baseline rows

## Run identity

- Code reference: `HP_V6/VERSION.md`
- Protocol: `hybridpatch/6`
- Dataset / split: `DELEGATE-52` / `val20 + unused_reserve20`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-11T04:24:32`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src\experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir <absolute-path> --notes hybridpatch/6 val40 single-arm campaign vs frozen FR baseline (val20+unused_reserve20), key=<key_label>
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.848 | 0.970 | 0.830 | 0.792 | 23 | 29403416 | applied=767, kept_context=23, unchanged=10 |
| `fullrewrite` | 0.711 | 0.948 | 0.723 | 0.553 | 29 | 15125044 | not_applicable=791, unchanged=9 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `val40_complete_pairs` | `hybridpatch` | `fullrewrite` | 400 | 0.848 | 0.711 | +0.137 | 0.000 | 0.311 | yes |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.137 across 400 paired backward rows (p=0.000).
   **Interpretation:** Within this exact canonical scope, the paired result supports a HybridPatch advantage.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 1.94× FullRewrite (29403416 vs 15125044). Failure labels: evaluator=9, gate=19, model=28, quality=66, unknown=1.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=800.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260711_hybridv6val40:satellite4:rt02:backward:paired:hybridpatch-win` | hybridpatch_win | `satellite4` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_hybridv6val40:obj3d2:rt10:backward:paired:hybridpatch-win` | hybridpatch_win | `obj3d2` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_hybridv6val40:obj3d2:rt09:backward:paired:hybridpatch-win` | hybridpatch_win | `obj3d2` / 9 / `backward` | fullrewrite: RS=0.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260711_hybridv6val40:subtitles6:rt01:backward:paired:fullrewrite-win` | fullrewrite_win | `subtitles6` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.000, kept_context, validation_gate | -1.000 |
| `exp_20260711_hybridv6val40:crystal6:rt04:backward:paired:fullrewrite-win` | fullrewrite_win | `crystal6` / 4 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.000, applied, evaluator_error | -1.000 |
| `exp_20260711_hybridv6val40:crystal6:rt05:backward:paired:fullrewrite-win` | fullrewrite_win | `crystal6` / 5 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.000, applied, evaluator_error | -1.000 |
| `exp_20260711_hybridv6val40:crystal6:rt01:backward:paired:near-tie` | near_tie | `crystal6` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260711_hybridv6val40:crystal6:rt02:backward:paired:near-tie` | near_tie | `crystal6` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260711_hybridv6val40:emails5:rt03:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `emails5` / 3 / `backward` | hybridpatch: RS=0.709, kept_context, validation_gate | n/a |
| `exp_20260711_hybridv6val40:geotrack5:rt04:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `geotrack5` / 4 / `backward` | hybridpatch: RS=0.339, kept_context, validation_gate | n/a |
| `exp_20260711_hybridv6val40:json2:rt01:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `json2` / 1 / `backward` | hybridpatch: RS=1.000, kept_context, validation_gate | n/a |
| `exp_20260711_hybridv6val40:crystal6:rt10:backward:fullrewrite:critical-failure` | critical_failure, failure:evaluator_error | `crystal6` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, evaluator_error | n/a |
| `exp_20260711_hybridv6val40:crystal6:rt04:backward:hybridpatch:critical-failure` | critical_failure, failure:evaluator_error | `crystal6` / 4 / `backward` | hybridpatch: RS=0.000, applied, evaluator_error | n/a |
| `exp_20260711_hybridv6val40:earncall1:rt04:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `earncall1` / 4 / `backward` | fullrewrite: RS=0.798, not_applicable, content_regression | n/a |
| `exp_20260711_hybridv6val40:crystal6:rt05:backward:hybridpatch:failure-event` | failure_event, failure:evaluator_error | `crystal6` / 5 / `backward` | hybridpatch: RS=0.000, applied, evaluator_error | n/a |
| `exp_20260711_hybridv6val40:crystal6:rt06:backward:hybridpatch:failure-event` | failure_event, failure:evaluator_error | `crystal6` / 6 / `backward` | hybridpatch: RS=0.000, applied, evaluator_error | n/a |

## Failures and exclusions

- Non-none failure rows: 123
- Affected sample-method pairs: 48
- Failure stages: evaluator=9, gate=19, model=28, quality=66, unknown=1
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: val20 had prior early-method exposure; unused_reserve20 was fresh to HybridPatch.
- Known exception: FullRewrite rows were copied from the frozen baseline rather than run live.
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

The original source report `HP_V6/exp_20260711_hybridv6val40/analysis/comparison.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# HybridPatch vs FullRewrite — diagnostic comparison

Model: minimax-m3 | samples: crystal6, docker3, earncall1, emails5, filesystem2, filesystem3, fonteng1, fonteng5, foodmenu1, geodata1, geodata4, geotrack5, geotrack6, hamradio6, jobboard3, json2, json4, landmarks2, landmarks3, libcatalog2, libcatalog5, mathlean4, mathlean5, obj3d2, obj3d5, quantum1, quantum5, robotics1, robotics3, satellite4, screenplay4, screenplay5, spreadsheet1, spreadsheet6, subtitles2, subtitles6, transit1, transit2, treebank2, treebank4 (n=40) | round trips: 10 | distractor: EXCLUDED

> Positioning: a **diagnostic** experiment (small n; accounting is a known AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). RS is the headline metric per request; preservation is the reliable secondary. Not a final universal superiority claim.

## RS@k (overall, failures counted as 0)
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.970 | 0.948 | 0.896 | 0.829 | 0.830 | 0.807 | 0.805 | 0.802 | 0.797 | 0.792 |
| FullRewrite | 0.948 | 0.847 | 0.772 | 0.778 | 0.723 | 0.682 | 0.620 | 0.599 | 0.584 | 0.553 |

## RS@{1,5,10} per domain
| domain | method | RS@1 | RS@5 | RS@10 |
|---|---|---|---|---|
| crystal | HybridPatch | 1.000 | 0.000 | 0.000 |
| crystal | FullRewrite | 1.000 | 1.000 | 0.000 |
| docker | HybridPatch | 1.000 | 0.982 | 0.931 |
| docker | FullRewrite | 0.997 | 0.981 | 0.898 |
| earncall | HybridPatch | 1.000 | 0.981 | 0.808 |
| earncall | FullRewrite | 0.983 | 0.797 | 0.000 |
| emails | HybridPatch | 1.000 | 0.331 | 0.619 |
| emails | FullRewrite | 1.000 | 0.000 | 0.165 |
| filesystem | HybridPatch | 1.000 | 0.851 | 0.820 |
| filesystem | FullRewrite | 1.000 | 1.000 | 0.973 |
| fonteng | HybridPatch | 1.000 | 0.974 | 0.958 |
| fonteng | FullRewrite | 1.000 | 0.997 | 0.811 |
| foodmenu | HybridPatch | 0.995 | 0.755 | 0.755 |
| foodmenu | FullRewrite | 0.995 | 0.689 | 0.666 |
| geodata | HybridPatch | 1.000 | 1.000 | 0.931 |
| geodata | FullRewrite | 0.500 | 0.500 | 0.000 |
| geotrack | HybridPatch | 1.000 | 0.625 | 0.623 |
| geotrack | FullRewrite | 1.000 | 0.712 | 0.161 |
| hamradio | HybridPatch | 1.000 | 0.895 | 0.894 |
| hamradio | FullRewrite | 1.000 | 0.989 | 0.925 |
| jobboard | HybridPatch | 1.000 | 0.150 | 0.150 |
| jobboard | FullRewrite | 1.000 | 0.000 | 0.000 |
| json | HybridPatch | 1.000 | 0.959 | 0.943 |
| json | FullRewrite | 1.000 | 0.530 | 0.518 |
| landmarks | HybridPatch | 1.000 | 1.000 | 0.871 |
| landmarks | FullRewrite | 1.000 | 0.934 | 0.829 |
| libcatalog | HybridPatch | 1.000 | 0.997 | 0.986 |
| libcatalog | FullRewrite | 0.984 | 0.966 | 0.971 |
| mathlean | HybridPatch | 0.952 | 0.830 | 0.755 |
| mathlean | FullRewrite | 0.915 | 0.770 | 0.740 |
| objd | HybridPatch | 0.953 | 0.953 | 0.953 |
| objd | FullRewrite | 0.956 | 0.456 | 0.000 |
| quantum | HybridPatch | 1.000 | 0.998 | 0.980 |
| quantum | FullRewrite | 1.000 | 0.990 | 0.651 |
| robotics | HybridPatch | 1.000 | 0.957 | 0.506 |
| robotics | FullRewrite | 1.000 | 1.000 | 0.989 |
| satellite | HybridPatch | 1.000 | 0.944 | 0.884 |
| satellite | FullRewrite | 1.000 | 0.000 | 0.029 |
| screenplay | HybridPatch | 0.999 | 0.999 | 0.997 |
| screenplay | FullRewrite | 0.999 | 0.998 | 0.969 |
| spreadsheet | HybridPatch | 1.000 | 0.564 | 0.201 |
| spreadsheet | FullRewrite | 0.622 | 0.564 | 0.488 |
| subtitles | HybridPatch | 0.500 | 0.472 | 0.962 |
| subtitles | FullRewrite | 1.000 | 0.870 | 0.696 |
| transit | HybridPatch | 0.997 | 0.901 | 0.944 |
| transit | FullRewrite | 0.997 | 0.453 | 0.422 |
| treebank | HybridPatch | 1.000 | 0.999 | 0.889 |
| treebank | FullRewrite | 0.999 | 0.503 | 0.497 |

## Paired same-task comparison (backward RS, matched by sample+round-trip)
| condition | n pairs | mean HybridPatch | mean FullRewrite | Δ | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| all pairs | 400 | 0.848 | 0.711 | +0.137 | 6.21 | 0.000 | 0.31 |
| ECR-conditioned (forward actually edited) | 378 | 0.844 | 0.724 | +0.121 | 5.46 | 0.000 | 0.28 |

## HybridPatch capability layering (per step) + preservation
- step method tags: `hybridpatch`=777, `hybridpatch_protocol_failure_kept_context`=23
- anchored-op steps: 0/800 (0%) | emit_file steps: 0 | fallback(FR) steps: 0
- mean op_accept_rate (anchored steps): 0.995
- **preservation_violations (live executor assertion): 0**
- mean verbatim block survival: 0.323 | mean byte preservation: 0.299

## Inflation guards, critical failures, tokens
- no-op forward steps: HybridPatch 13/400, FullRewrite 9/400
- critical failures (backward RS drop >= 0.10 or collapse to 0 between round trips): HybridPatch 23/360, FullRewrite 29/360
- tokens (prompt+completion): HybridPatch 29,403,416 (9,481,059+19,922,357), FullRewrite 15,125,044 (3,184,299+11,940,745)

## HybridPatch — RS@k
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.970 | 0.948 | 0.896 | 0.829 | 0.830 | 0.807 | 0.805 | 0.802 | 0.797 | 0.792 |

## HybridPatch vs FullRewrite (paired backward RS)
| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| hybridpatch vs FR | 400 | 0.848 | 0.711 | +0.137 | 6.21 | 0.000 | 0.31 |

CriticalFailure@10: hybridpatch 23/360, fullrewrite 29/360 (theta=0.10; collapse-to-0 always counted).

## HybridPatch telemetry
- steps with hybrid telemetry: 800
- route share: `bounded_rewrite`=531/800 (66.4%), `bulk_patch`=89/800 (11.1%), `hybridpatch_protocol_failure_kept_context`=4/800 (0.5%), `local_patch`=176/800 (22.0%)
- bounded rewrite share: 531/800 (66.4%)
- copied/generated bytes: 3368633/6775873 | copy:generated ratio=0.49715114200044774 | mean generated byte ratio=0.651
- repair: attempted=115 used=99 success=91 rate=14.4%
- gate failures: 19 (2.4%) | kept-context failures: 23 (2.9%)
- forward no-effective-modification audit: 2/396 (0.5%)
````

</details>
