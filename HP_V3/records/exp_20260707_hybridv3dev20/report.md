# exp_20260707_hybridv3dev20 experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: HybridPatch/3 first dev20 run retained as a replay-regression reference.
- Lifecycle: `superseded`
- Evidence role: `supporting`
- Claim eligibility: `supporting`
- Canonical scope: `historical20_complete_pairs` (20/20 samples)
- Canonical backward rows: 400
- Comparison policy: live paired; superseded by exp_20260708_hybridv3dev20full

## Run identity

- Code reference: `HP_V3/VERSION.md`
- Protocol: `hybridpatch/3`
- Dataset / split: `DELEGATE-52` / `frozen dev20 actual sample set`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-07T09:33:57`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 2

### Recorded command templates

````text
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260707_hybridv3dev20 --notes full FR baseline for completed HP run; uncapped tokens; fresh API key
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260707_hybridv3dev20 --notes paired FR baseline subset for completed HP run; uncapped tokens
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 64000 --out_dir exp_20260707_hybridv3dev20 --notes paired FR baseline, thinking on
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260707_hybridv3dev20 --notes rerun empty-response victims; uncapped tokens; schema-in-repair
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 64000 --out_dir exp_20260707_hybridv3dev20 --notes hybridpatch/3 + expanded grounded repair; dev20 full; clean repo
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.903 | 0.974 | 0.936 | 0.838 | 10 | 13552522 | applied=390, kept_context=8, unchanged=2 |
| `fullrewrite` | 0.426 | 0.819 | 0.429 | 0.259 | 22 | 5870788 | not_applicable=389, unchanged=11 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `historical20_complete_pairs` | `hybridpatch` | `fullrewrite` | 200 | 0.903 | 0.426 | +0.477 | 0.000 | 1.103 | no |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.477 across 200 paired backward rows (p=0.000).
   **Interpretation:** This comparison is supporting or diagnostic evidence and must not be promoted to a standalone method claim.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 2.31× FullRewrite (13552522 vs 5870788). Failure labels: evaluator=1, gate=6, model=27, quality=39, unknown=1.
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
| `exp_20260707_hybridv3dev20:translation4:rt01:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260707_hybridv3dev20:satellite6:rt01:backward:paired:hybridpatch-win` | hybridpatch_win | `satellite6` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, evaluator_error; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260707_hybridv3dev20:quantum4:rt02:backward:paired:hybridpatch-win` | hybridpatch_win | `quantum4` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260707_hybridv3dev20:foodmenu6:rt06:backward:paired:fullrewrite-win` | fullrewrite_win | `foodmenu6` / 6 / `backward` | fullrewrite: RS=0.946, not_applicable, none; hybridpatch: RS=0.401, kept_context, validation_gate | -0.546 |
| `exp_20260707_hybridv3dev20:foodmenu6:rt07:backward:paired:fullrewrite-win` | fullrewrite_win | `foodmenu6` / 7 / `backward` | fullrewrite: RS=0.946, not_applicable, none; hybridpatch: RS=0.660, applied, none | -0.287 |
| `exp_20260707_hybridv3dev20:foodmenu6:rt08:backward:paired:fullrewrite-win` | fullrewrite_win | `foodmenu6` / 8 / `backward` | fullrewrite: RS=0.850, not_applicable, none; hybridpatch: RS=0.652, applied, none | -0.198 |
| `exp_20260707_hybridv3dev20:circuit2:rt01:backward:paired:near-tie` | near_tie | `circuit2` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260707_hybridv3dev20:circuit2:rt02:backward:paired:near-tie` | near_tie | `circuit2` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260707_hybridv3dev20:fonteng3:rt07:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `fonteng3` / 7 / `backward` | hybridpatch: RS=0.773, kept_context, validation_gate | n/a |
| `exp_20260707_hybridv3dev20:foodmenu6:rt03:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `foodmenu6` / 3 / `backward` | hybridpatch: RS=0.997, kept_context, validation_gate | n/a |
| `exp_20260707_hybridv3dev20:foodmenu6:rt06:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `foodmenu6` / 6 / `backward` | hybridpatch: RS=0.401, kept_context, validation_gate | n/a |
| `exp_20260707_hybridv3dev20:circuit2:rt06:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `circuit2` / 6 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260707_hybridv3dev20:edifact6:rt10:backward:fullrewrite:critical-failure` | critical_failure, failure:empty_response | `edifact6` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response | n/a |
| `exp_20260707_hybridv3dev20:foodmenu6:rt09:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 9 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260707_hybridv3dev20:geotrack3:rt01:backward:fullrewrite:failure-event` | failure_event, failure:empty_response | `geotrack3` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response | n/a |
| `exp_20260707_hybridv3dev20:geotrack3:rt10:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `geotrack3` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |

## Failures and exclusions

- Non-none failure rows: 74
- Affected sample-method pairs: 28
- Failure stages: evaluator=1, gate=6, model=27, quality=39, unknown=1
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: run_metadata.jsonl contains two code fingerprints; this archive is not single-fingerprint canonical evidence.
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

The original source report `HP_V3/exp_20260707_hybridv3dev20/analysis/comparison.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# HybridPatch vs FullRewrite — diagnostic comparison

Model: minimax-m3 | samples: circuit2, docker6, edifact6, fonteng3, foodmenu6, geotrack3, landmarks1, latex2, makefile4, malware6, mathlean2, molecule2, musicsheet2, protein1, python7, quantum4, satellite6, screenplay6, starcatalog4, translation4 (n=20) | round trips: 10 | distractor: EXCLUDED

> Positioning: a **diagnostic** experiment (small n; accounting is a known AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). RS is the headline metric per request; preservation is the reliable secondary. Not a final universal superiority claim.

## RS@k (overall, failures counted as 0)
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.974 | 0.961 | 0.944 | 0.935 | 0.936 | 0.902 | 0.904 | 0.839 | 0.793 | 0.838 |
| FullRewrite | 0.819 | 0.488 | 0.405 | 0.459 | 0.429 | 0.359 | 0.357 | 0.370 | 0.314 | 0.259 |

## RS@{1,5,10} per domain
| domain | method | RS@1 | RS@5 | RS@10 |
|---|---|---|---|---|
| circuit | HybridPatch | 1.000 | 1.000 | 0.945 |
| circuit | FullRewrite | 1.000 | 1.000 | 0.090 |
| docker | HybridPatch | 0.932 | 0.927 | 0.934 |
| docker | FullRewrite | 0.929 | 0.926 | 0.977 |
| edifact | HybridPatch | 0.960 | 0.960 | 0.960 |
| edifact | FullRewrite | 0.960 | 0.960 | 0.000 |
| fonteng | HybridPatch | 1.000 | 0.931 | 0.773 |
| fonteng | FullRewrite | 1.000 | 0.933 | 0.933 |
| foodmenu | HybridPatch | 0.997 | 0.997 | 0.650 |
| foodmenu | FullRewrite | 1.000 | 0.994 | 0.000 |
| geotrack | HybridPatch | 1.000 | 0.980 | 0.824 |
| geotrack | FullRewrite | 0.000 | 0.151 | 0.000 |
| landmarks | HybridPatch | 1.000 | 1.000 | 0.000 |
| landmarks | FullRewrite | 1.000 | 0.000 | 0.000 |
| latex | HybridPatch | 0.999 | 0.993 | 0.792 |
| latex | FullRewrite | 0.999 | 0.205 | 0.005 |
| makefile | HybridPatch | 0.950 | 0.930 | 0.922 |
| makefile | FullRewrite | 0.950 | 0.044 | 0.120 |
| malware | HybridPatch | 1.000 | 0.893 | 0.893 |
| malware | FullRewrite | 1.000 | 0.138 | 0.150 |
| mathlean | HybridPatch | 0.999 | 0.970 | 0.906 |
| mathlean | FullRewrite | 0.997 | 0.168 | 0.011 |
| molecule | HybridPatch | 1.000 | 1.000 | 1.000 |
| molecule | FullRewrite | 1.000 | 0.496 | 0.496 |
| musicsheet | HybridPatch | 0.703 | 0.562 | 0.557 |
| musicsheet | FullRewrite | 0.673 | 0.547 | 0.188 |
| protein | HybridPatch | 0.976 | 0.776 | 0.800 |
| protein | FullRewrite | 1.000 | 0.003 | 0.008 |
| python | HybridPatch | 1.000 | 1.000 | 1.000 |
| python | FullRewrite | 1.000 | 1.000 | 1.000 |
| quantum | HybridPatch | 0.985 | 1.000 | 0.985 |
| quantum | FullRewrite | 0.879 | 0.147 | 0.328 |
| satellite | HybridPatch | 1.000 | 0.843 | 0.879 |
| satellite | FullRewrite | 0.000 | 0.014 | 0.000 |
| screenplay | HybridPatch | 1.000 | 0.999 | 0.999 |
| screenplay | FullRewrite | 1.000 | 0.850 | 0.870 |
| starcatalog | HybridPatch | 0.987 | 0.986 | 0.986 |
| starcatalog | FullRewrite | 0.987 | 0.000 | 0.000 |
| translation | HybridPatch | 1.000 | 0.964 | 0.964 |
| translation | FullRewrite | 0.000 | 0.000 | 0.000 |

## Paired same-task comparison (backward RS, matched by sample+round-trip)
| condition | n pairs | mean HybridPatch | mean FullRewrite | Δ | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| all pairs | 200 | 0.903 | 0.426 | +0.477 | 15.60 | 0.000 | 1.10 |
| ECR-conditioned (forward actually edited) | 186 | 0.906 | 0.444 | +0.462 | 14.49 | 0.000 | 1.06 |

## HybridPatch capability layering (per step) + preservation
- step method tags: `hybridpatch`=392, `hybridpatch_protocol_failure_kept_context`=8
- anchored-op steps: 0/400 (0%) | emit_file steps: 0 | fallback(FR) steps: 0
- mean op_accept_rate (anchored steps): 0.993
- **preservation_violations (live executor assertion): 0**
- mean verbatim block survival: 0.290 | mean byte preservation: 0.270

## Inflation guards, critical failures, tokens
- no-op forward steps: HybridPatch 3/200, FullRewrite 11/200
- critical failures (backward RS drop >= 0.10 or collapse to 0 between round trips): HybridPatch 10/180, FullRewrite 22/180
- tokens (prompt+completion): HybridPatch 13,552,522 (4,395,058+9,157,464), FullRewrite 5,870,788 (1,076,475+4,794,313)

## HybridPatch — RS@k
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.974 | 0.961 | 0.944 | 0.935 | 0.936 | 0.902 | 0.904 | 0.839 | 0.793 | 0.838 |

## HybridPatch vs FullRewrite (paired backward RS)
| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| hybridpatch vs FR | 200 | 0.903 | 0.426 | +0.477 | 15.60 | 0.000 | 1.10 |

CriticalFailure@10: hybridpatch 10/180, fullrewrite 22/180 (theta=0.10; collapse-to-0 always counted).

## HybridPatch telemetry
- steps with hybrid telemetry: 400
- route share: `bounded_rewrite`=287/400 (71.8%), `bulk_patch`=31/400 (7.8%), `hybridpatch_protocol_failure_kept_context`=2/400 (0.5%), `local_patch`=80/400 (20.0%)
- bounded rewrite share: 287/400 (71.8%)
- copied/generated bytes: 1533104/3293281 | copy:generated ratio=0.4655248064164582 | mean generated byte ratio=0.697
- repair: attempted=36 used=29 success=27 rate=9.0%
- gate failures: 6 (1.5%) | kept-context failures: 8 (2.0%)
- forward no-effective-modification audit: 2/200 (1.0%)
````

</details>
