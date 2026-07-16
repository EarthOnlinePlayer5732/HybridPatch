# exp_20260709_hybridv5dev20full experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Formal HybridPatch/5 dev20 paired experiment.
- Lifecycle: `complete`
- Evidence role: `primary`
- Claim eligibility: `canonical`
- Canonical scope: `dev20_complete_pairs` (20/20 samples)
- Canonical backward rows: 400
- Comparison policy: live paired with targeted FullRewrite rewinds; not directly comparable to frozen-baseline policy

## Run identity

- Code reference: `HP_V5/VERSION.md`
- Protocol: `hybridpatch/5`
- Dataset / split: `DELEGATE-52` / `frozen dev20`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-09T12:48:50`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes FR resume after 5h quota window + mathlean2 second empty-anchored rewind
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes FR rewind redo (empty-anchored)
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes FR second empty-anchored rewind (final round)
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes phase2: hybridpatch/5 full dev20 paired campaign, FR batch1
python src/experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes phase2: hybridpatch/5 full dev20 paired campaign, FR batch2
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes phase2: hybridpatch/5 full dev20 paired campaign, HP batch1
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260709_hybridv5dev20full --notes phase2: hybridpatch/5 full dev20 paired campaign, HP batch2
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.849 | 0.982 | 0.841 | 0.804 | 10 | 15861272 | applied=393, kept_context=7 |
| `fullrewrite` | 0.814 | 0.949 | 0.858 | 0.672 | 12 | 7809126 | not_applicable=400 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `dev20_complete_pairs` | `hybridpatch` | `fullrewrite` | 200 | 0.849 | 0.814 | +0.035 | 0.161 | 0.099 | yes |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.035 across 200 paired backward rows (p=0.161).
   **Interpretation:** The point estimate favors HybridPatch, but this scope does not establish a statistically distinguishable advantage.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 2.03× FullRewrite (15861272 vs 7809126). Failure labels: gate=7, model=7, quality=22, unknown=1.
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
| `exp_20260709_hybridv5dev20full:latex2:rt03:backward:paired:hybridpatch-win` | hybridpatch_win | `latex2` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression; hybridpatch: RS=0.999, applied, none | +0.999 |
| `exp_20260709_hybridv5dev20full:translation4:rt06:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 6 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression; hybridpatch: RS=0.963, applied, none | +0.963 |
| `exp_20260709_hybridv5dev20full:translation4:rt10:backward:paired:hybridpatch-win` | hybridpatch_win | `translation4` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, none; hybridpatch: RS=0.957, applied, none | +0.957 |
| `exp_20260709_hybridv5dev20full:protein1:rt04:backward:paired:fullrewrite-win` | fullrewrite_win | `protein1` / 4 / `backward` | fullrewrite: RS=0.991, not_applicable, none; hybridpatch: RS=0.000, applied, content_regression | -0.991 |
| `exp_20260709_hybridv5dev20full:protein1:rt05:backward:paired:fullrewrite-win` | fullrewrite_win | `protein1` / 5 / `backward` | fullrewrite: RS=0.991, not_applicable, none; hybridpatch: RS=0.000, applied, none | -0.991 |
| `exp_20260709_hybridv5dev20full:protein1:rt06:backward:paired:fullrewrite-win` | fullrewrite_win | `protein1` / 6 / `backward` | fullrewrite: RS=0.991, not_applicable, none; hybridpatch: RS=0.000, applied, none | -0.991 |
| `exp_20260709_hybridv5dev20full:circuit2:rt01:backward:paired:near-tie` | near_tie | `circuit2` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260709_hybridv5dev20full:edifact6:rt07:backward:paired:near-tie` | near_tie | `edifact6` / 7 / `backward` | fullrewrite: RS=0.960, not_applicable, none; hybridpatch: RS=0.960, applied, none | +0.000 |
| `exp_20260709_hybridv5dev20full:circuit2:rt02:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `circuit2` / 2 / `backward` | hybridpatch: RS=0.886, kept_context, validation_gate | n/a |
| `exp_20260709_hybridv5dev20full:fonteng3:rt07:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `fonteng3` / 7 / `backward` | hybridpatch: RS=0.775, kept_context, validation_gate | n/a |
| `exp_20260709_hybridv5dev20full:foodmenu6:rt07:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `foodmenu6` / 7 / `backward` | hybridpatch: RS=0.909, kept_context, validation_gate | n/a |
| `exp_20260709_hybridv5dev20full:foodmenu6:rt02:backward:hybridpatch:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 2 / `backward` | hybridpatch: RS=0.350, applied, content_regression | n/a |
| `exp_20260709_hybridv5dev20full:landmarks1:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `landmarks1` / 2 / `backward` | fullrewrite: RS=0.825, not_applicable, content_regression | n/a |
| `exp_20260709_hybridv5dev20full:latex2:rt03:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `latex2` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression | n/a |
| `exp_20260709_hybridv5dev20full:landmarks1:rt10:backward:hybridpatch:failure-event` | failure_event, failure:unknown_failure | `landmarks1` / 10 / `backward` | hybridpatch: RS=1.000, applied, unknown_failure | n/a |
| `exp_20260709_hybridv5dev20full:latex2:rt08:backward:hybridpatch:failure-event` | failure_event, failure:content_regression | `latex2` / 8 / `backward` | hybridpatch: RS=0.683, applied, content_regression | n/a |

## Failures and exclusions

- Non-none failure rows: 37
- Affected sample-method pairs: 21
- Failure stages: gate=7, model=7, quality=22, unknown=1
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: protein1, satellite6, and translation4 retained recurrent provider-empty contamination after the bounded rewind policy.
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

The original source report `HP_V5/exp_20260709_hybridv5dev20full/analysis/comparison.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# HybridPatch vs FullRewrite — diagnostic comparison

Model: minimax-m3 | samples: circuit2, docker6, edifact6, fonteng3, foodmenu6, geotrack3, landmarks1, latex2, makefile4, malware6, mathlean2, molecule2, musicsheet2, protein1, python7, quantum4, satellite6, screenplay6, starcatalog4, translation4 (n=20) | round trips: 10 | distractor: EXCLUDED

> Positioning: a **diagnostic** experiment (small n; accounting is a known AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). RS is the headline metric per request; preservation is the reliable secondary. Not a final universal superiority claim.

## RS@k (overall, failures counted as 0)
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.982 | 0.888 | 0.879 | 0.829 | 0.841 | 0.840 | 0.820 | 0.807 | 0.801 | 0.804 |
| FullRewrite | 0.949 | 0.934 | 0.833 | 0.857 | 0.858 | 0.807 | 0.787 | 0.774 | 0.673 | 0.672 |

## RS@{1,5,10} per domain
| domain | method | RS@1 | RS@5 | RS@10 |
|---|---|---|---|---|
| circuit | HybridPatch | 1.000 | 0.886 | 0.886 |
| circuit | FullRewrite | 1.000 | 1.000 | 0.996 |
| docker | HybridPatch | 0.929 | 0.925 | 0.921 |
| docker | FullRewrite | 0.989 | 0.983 | 0.918 |
| edifact | HybridPatch | 1.000 | 1.000 | 0.960 |
| edifact | FullRewrite | 0.960 | 0.960 | 0.960 |
| fonteng | HybridPatch | 1.000 | 0.933 | 0.775 |
| fonteng | FullRewrite | 1.000 | 0.930 | 0.918 |
| foodmenu | HybridPatch | 1.000 | 1.000 | 0.899 |
| foodmenu | FullRewrite | 0.998 | 0.998 | 0.902 |
| geotrack | HybridPatch | 0.844 | 0.844 | 0.844 |
| geotrack | FullRewrite | 0.824 | 0.734 | 0.790 |
| landmarks | HybridPatch | 1.000 | 1.000 | 1.000 |
| landmarks | FullRewrite | 1.000 | 1.000 | 1.000 |
| latex | HybridPatch | 0.999 | 0.987 | 0.690 |
| latex | FullRewrite | 0.852 | 0.545 | 0.416 |
| makefile | HybridPatch | 0.950 | 0.930 | 0.922 |
| makefile | FullRewrite | 0.950 | 0.929 | 0.000 |
| malware | HybridPatch | 1.000 | 1.000 | 0.998 |
| malware | FullRewrite | 1.000 | 1.000 | 1.000 |
| mathlean | HybridPatch | 0.992 | 0.917 | 0.920 |
| mathlean | FullRewrite | 0.991 | 0.975 | 0.922 |
| molecule | HybridPatch | 1.000 | 1.000 | 1.000 |
| molecule | FullRewrite | 1.000 | 1.000 | 0.734 |
| musicsheet | HybridPatch | 1.000 | 0.586 | 0.586 |
| musicsheet | FullRewrite | 0.558 | 0.367 | 0.150 |
| protein | HybridPatch | 0.932 | 0.000 | 0.051 |
| protein | FullRewrite | 1.000 | 0.991 | 0.000 |
| python | HybridPatch | 1.000 | 1.000 | 1.000 |
| python | FullRewrite | 1.000 | 1.000 | 1.000 |
| quantum | HybridPatch | 1.000 | 0.393 | 0.324 |
| quantum | FullRewrite | 0.879 | 0.749 | 0.879 |
| satellite | HybridPatch | 1.000 | 0.890 | 0.777 |
| satellite | FullRewrite | 0.991 | 0.027 | 0.027 |
| screenplay | HybridPatch | 1.000 | 0.966 | 0.965 |
| screenplay | FullRewrite | 1.000 | 0.994 | 0.993 |
| starcatalog | HybridPatch | 0.987 | 0.608 | 0.603 |
| starcatalog | FullRewrite | 0.987 | 0.989 | 0.835 |
| translation | HybridPatch | 1.000 | 0.964 | 0.957 |
| translation | FullRewrite | 0.997 | 0.997 | 0.000 |

## Paired same-task comparison (backward RS, matched by sample+round-trip)
| condition | n pairs | mean HybridPatch | mean FullRewrite | Δ | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| all pairs | 200 | 0.849 | 0.814 | +0.035 | 1.41 | 0.161 | 0.10 |
| ECR-conditioned (forward actually edited) | 198 | 0.852 | 0.820 | +0.032 | 1.28 | 0.203 | 0.09 |

## HybridPatch capability layering (per step) + preservation
- step method tags: `hybridpatch`=393, `hybridpatch_protocol_failure_kept_context`=7
- anchored-op steps: 0/400 (0%) | emit_file steps: 0 | fallback(FR) steps: 0
- mean op_accept_rate (anchored steps): 0.995
- **preservation_violations (live executor assertion): 0**
- mean verbatim block survival: 0.372 | mean byte preservation: 0.341

## Inflation guards, critical failures, tokens
- no-op forward steps: HybridPatch 2/200, FullRewrite 0/200
- critical failures (backward RS drop >= 0.10 or collapse to 0 between round trips): HybridPatch 10/180, FullRewrite 12/180
- tokens (prompt+completion): HybridPatch 15,861,272 (4,323,460+11,537,812), FullRewrite 7,809,126 (1,623,260+6,185,866)

## HybridPatch — RS@k
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.982 | 0.888 | 0.879 | 0.829 | 0.841 | 0.840 | 0.820 | 0.807 | 0.801 | 0.804 |

## HybridPatch vs FullRewrite (paired backward RS)
| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| hybridpatch vs FR | 200 | 0.849 | 0.814 | +0.035 | 1.41 | 0.161 | 0.10 |

CriticalFailure@10: hybridpatch 10/180, fullrewrite 12/180 (theta=0.10; collapse-to-0 always counted).

## HybridPatch telemetry
- steps with hybrid telemetry: 400
- route share: `bounded_rewrite`=230/400 (57.5%), `bulk_patch`=49/400 (12.2%), `local_patch`=121/400 (30.2%)
- bounded rewrite share: 230/400 (57.5%)
- copied/generated bytes: 1831657/2892754 | copy:generated ratio=0.6331879586027709 | mean generated byte ratio=0.595
- repair: attempted=41 used=35 success=33 rate=10.2%
- gate failures: 7 (1.8%) | kept-context failures: 7 (1.8%)
- forward no-effective-modification audit: 0/200 (0.0%)
````

</details>
