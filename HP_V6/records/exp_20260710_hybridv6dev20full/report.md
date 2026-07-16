# exp_20260710_hybridv6dev20full experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Formal HybridPatch/6 dev20 evaluation paired with the copied frozen FullRewrite baseline.
- Lifecycle: `complete`
- Evidence role: `primary`
- Claim eligibility: `canonical`
- Canonical scope: `dev20_complete_pairs` (20/20 samples)
- Canonical backward rows: 400
- Comparison policy: HybridPatch live arm paired with immutable FullRewrite baseline rows

## Run identity

- Code reference: `HP_V6/VERSION.md`
- Protocol: `hybridpatch/6`
- Dataset / split: `DELEGATE-52` / `frozen dev20`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `n/a`
- Seed: `42`
- Started: `2026-07-10T00:27:31`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes HP resume after 5h quota reset
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes HP tail resume after quota reset
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes HP tail resume probe
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes hybridpatch/6 HP tail resume after API key swap
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes hybridpatch/6 HP translation4 final resume RT7-10
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes hybridpatch/6 full dev20 paired campaign, HP batch1
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes hybridpatch/6 full dev20 paired campaign, HP batch2
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes hybridpatch/6 full dev20 paired campaign, HP batch2a
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes hybridpatch/6 full dev20 paired campaign, HP batch2b
python src/experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0 --out_dir exp_20260710_hybridv6dev20full --notes manual quota probe
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.921 | 0.991 | 0.906 | 0.918 | 8 | 15159773 | applied=388, kept_context=9, unchanged=3 |
| `fullrewrite` | 0.726 | 0.959 | 0.784 | 0.632 | 13 | 7084848 | not_applicable=397, unchanged=3 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `dev20_complete_pairs` | `hybridpatch` | `fullrewrite` | 200 | 0.921 | 0.726 | +0.195 | 0.000 | 0.540 | yes |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.195 across 200 paired backward rows (p=0.000).
   **Interpretation:** Within this exact canonical scope, the paired result supports a HybridPatch advantage.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 2.14× FullRewrite (15159773 vs 7084848). Failure labels: gate=9, model=14, quality=18, unknown=3.
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
| `exp_20260710_hybridv6dev20full:quantum4:rt08:backward:paired:hybridpatch-win` | hybridpatch_win | `quantum4` / 8 / `backward` | fullrewrite: RS=0.000, not_applicable, near_empty_response; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260710_hybridv6dev20full:python7:rt07:backward:paired:hybridpatch-win` | hybridpatch_win | `python7` / 7 / `backward` | fullrewrite: RS=0.000, not_applicable, empty_response; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260710_hybridv6dev20full:protein1:rt03:backward:paired:hybridpatch-win` | hybridpatch_win | `protein1` / 3 / `backward` | fullrewrite: RS=0.000, not_applicable, content_regression; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260710_hybridv6dev20full:satellite6:rt06:backward:paired:fullrewrite-win` | fullrewrite_win | `satellite6` / 6 / `backward` | fullrewrite: RS=0.924, not_applicable, none; hybridpatch: RS=0.731, applied, none | -0.192 |
| `exp_20260710_hybridv6dev20full:satellite6:rt07:backward:paired:fullrewrite-win` | fullrewrite_win | `satellite6` / 7 / `backward` | fullrewrite: RS=0.916, not_applicable, none; hybridpatch: RS=0.728, applied, none | -0.188 |
| `exp_20260710_hybridv6dev20full:satellite6:rt08:backward:paired:fullrewrite-win` | fullrewrite_win | `satellite6` / 8 / `backward` | fullrewrite: RS=0.916, not_applicable, none; hybridpatch: RS=0.728, applied, none | -0.188 |
| `exp_20260710_hybridv6dev20full:edifact6:rt01:backward:paired:near-tie` | near_tie | `edifact6` / 1 / `backward` | fullrewrite: RS=0.960, not_applicable, none; hybridpatch: RS=0.960, applied, none | +0.000 |
| `exp_20260710_hybridv6dev20full:edifact6:rt02:backward:paired:near-tie` | near_tie | `edifact6` / 2 / `backward` | fullrewrite: RS=0.960, not_applicable, none; hybridpatch: RS=0.960, applied, none | +0.000 |
| `exp_20260710_hybridv6dev20full:fonteng3:rt07:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `fonteng3` / 7 / `backward` | hybridpatch: RS=0.842, kept_context, validation_gate | n/a |
| `exp_20260710_hybridv6dev20full:foodmenu6:rt07:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `foodmenu6` / 7 / `backward` | hybridpatch: RS=0.909, kept_context, validation_gate | n/a |
| `exp_20260710_hybridv6dev20full:protein1:rt04:backward:hybridpatch:kept-context` | kept_context, failure:format_regression | `protein1` / 4 / `backward` | hybridpatch: RS=0.000, kept_context, format_regression | n/a |
| `exp_20260710_hybridv6dev20full:docker6:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `docker6` / 2 / `backward` | fullrewrite: RS=0.049, not_applicable, content_regression | n/a |
| `exp_20260710_hybridv6dev20full:foodmenu6:rt02:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 2 / `backward` | fullrewrite: RS=0.350, not_applicable, content_regression | n/a |
| `exp_20260710_hybridv6dev20full:foodmenu6:rt06:backward:fullrewrite:critical-failure` | critical_failure, failure:content_regression | `foodmenu6` / 6 / `backward` | fullrewrite: RS=0.675, not_applicable, content_regression | n/a |
| `exp_20260710_hybridv6dev20full:foodmenu6:rt08:backward:fullrewrite:failure-event` | failure_event, failure:content_regression | `foodmenu6` / 8 / `backward` | fullrewrite: RS=0.880, not_applicable, content_regression | n/a |
| `exp_20260710_hybridv6dev20full:foodmenu6:rt02:backward:hybridpatch:failure-event` | failure_event, failure:content_regression | `foodmenu6` / 2 / `backward` | hybridpatch: RS=0.425, applied, content_regression | n/a |

## Failures and exclusions

- Non-none failure rows: 44
- Affected sample-method pairs: 20
- Failure stages: gate=9, model=14, quality=18, unknown=3
- Canonical exclusions: none

## Known exceptions and provenance gaps

- Known exception: Only the HybridPatch arm was live in this directory; FullRewrite rows were copied byte-for-byte from the frozen baseline.
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

The original source report `HP_V6/exp_20260710_hybridv6dev20full/analysis/comparison.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# HybridPatch vs FullRewrite — diagnostic comparison

Model: minimax-m3 | samples: circuit2, docker6, edifact6, fonteng3, foodmenu6, geotrack3, landmarks1, latex2, makefile4, malware6, mathlean2, molecule2, musicsheet2, protein1, python7, quantum4, satellite6, screenplay6, starcatalog4, translation4 (n=20) | round trips: 10 | distractor: EXCLUDED

> Positioning: a **diagnostic** experiment (small n; accounting is a known AnchorPatch/HybridPatch-favorable domain with order-invariant coverage² scoring). RS is the headline metric per request; preservation is the reliable secondary. Not a final universal superiority claim.

## RS@k (overall, failures counted as 0)
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.991 | 0.949 | 0.934 | 0.885 | 0.906 | 0.900 | 0.888 | 0.912 | 0.923 | 0.918 |
| FullRewrite | 0.959 | 0.816 | 0.715 | 0.754 | 0.784 | 0.719 | 0.625 | 0.626 | 0.630 | 0.632 |

## RS@{1,5,10} per domain
| domain | method | RS@1 | RS@5 | RS@10 |
|---|---|---|---|---|
| circuit | HybridPatch | 1.000 | 1.000 | 1.000 |
| circuit | FullRewrite | 1.000 | 1.000 | 1.000 |
| docker | HybridPatch | 0.932 | 0.928 | 0.927 |
| docker | FullRewrite | 0.951 | 0.112 | 0.114 |
| edifact | HybridPatch | 0.960 | 0.960 | 0.960 |
| edifact | FullRewrite | 0.960 | 0.960 | 1.000 |
| fonteng | HybridPatch | 1.000 | 1.000 | 0.770 |
| fonteng | FullRewrite | 1.000 | 0.942 | 0.942 |
| foodmenu | HybridPatch | 1.000 | 1.000 | 0.827 |
| foodmenu | FullRewrite | 0.999 | 0.999 | 0.880 |
| geotrack | HybridPatch | 1.000 | 0.824 | 0.824 |
| geotrack | FullRewrite | 0.844 | 0.734 | 0.824 |
| landmarks | HybridPatch | 1.000 | 0.998 | 0.998 |
| landmarks | FullRewrite | 1.000 | 1.000 | 1.000 |
| latex | HybridPatch | 0.999 | 0.999 | 0.853 |
| latex | FullRewrite | 0.999 | 0.996 | 0.833 |
| makefile | HybridPatch | 0.950 | 0.930 | 0.922 |
| makefile | FullRewrite | 0.950 | 0.930 | 0.921 |
| malware | HybridPatch | 1.000 | 1.000 | 1.000 |
| malware | FullRewrite | 1.000 | 1.000 | 0.080 |
| mathlean | HybridPatch | 1.000 | 0.942 | 0.927 |
| mathlean | FullRewrite | 0.991 | 0.903 | 0.933 |
| molecule | HybridPatch | 1.000 | 1.000 | 1.000 |
| molecule | FullRewrite | 1.000 | 1.000 | 1.000 |
| musicsheet | HybridPatch | 1.000 | 0.776 | 0.651 |
| musicsheet | FullRewrite | 0.647 | 0.468 | 0.386 |
| protein | HybridPatch | 1.000 | 0.054 | 0.765 |
| protein | FullRewrite | 0.976 | 0.000 | 0.000 |
| python | HybridPatch | 1.000 | 1.000 | 1.000 |
| python | FullRewrite | 1.000 | 1.000 | 1.000 |
| quantum | HybridPatch | 1.000 | 1.000 | 0.985 |
| quantum | FullRewrite | 0.879 | 0.879 | 0.003 |
| satellite | HybridPatch | 1.000 | 0.736 | 0.987 |
| satellite | FullRewrite | 1.000 | 0.924 | 0.853 |
| screenplay | HybridPatch | 1.000 | 0.981 | 0.981 |
| screenplay | FullRewrite | 1.000 | 0.852 | 0.868 |
| starcatalog | HybridPatch | 0.987 | 0.986 | 0.986 |
| starcatalog | FullRewrite | 0.987 | 0.986 | 0.000 |
| translation | HybridPatch | 0.999 | 0.999 | 0.999 |
| translation | FullRewrite | 1.000 | 0.000 | 0.000 |

## Paired same-task comparison (backward RS, matched by sample+round-trip)
| condition | n pairs | mean HybridPatch | mean FullRewrite | Δ | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| all pairs | 200 | 0.921 | 0.726 | +0.195 | 7.64 | 0.000 | 0.54 |
| ECR-conditioned (forward actually edited) | 193 | 0.926 | 0.739 | +0.188 | 7.30 | 0.000 | 0.53 |

## HybridPatch capability layering (per step) + preservation
- step method tags: `hybridpatch`=391, `hybridpatch_protocol_failure_kept_context`=9
- anchored-op steps: 0/400 (0%) | emit_file steps: 0 | fallback(FR) steps: 0
- mean op_accept_rate (anchored steps): 0.991
- **preservation_violations (live executor assertion): 0**
- mean verbatim block survival: 0.379 | mean byte preservation: 0.343

## Inflation guards, critical failures, tokens
- no-op forward steps: HybridPatch 4/200, FullRewrite 3/200
- critical failures (backward RS drop >= 0.10 or collapse to 0 between round trips): HybridPatch 8/180, FullRewrite 13/180
- tokens (prompt+completion): HybridPatch 15,159,773 (4,279,023+10,880,750), FullRewrite 7,084,848 (1,448,869+5,635,979)

## HybridPatch — RS@k
| k | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| HybridPatch | 0.991 | 0.949 | 0.934 | 0.885 | 0.906 | 0.900 | 0.888 | 0.912 | 0.923 | 0.918 |

## HybridPatch vs FullRewrite (paired backward RS)
| pairing | n pairs | mean A | mean B | Δ(A−B) | t | p | Cohen d |
|---|---|---|---|---|---|---|---|
| hybridpatch vs FR | 200 | 0.921 | 0.726 | +0.195 | 7.64 | 0.000 | 0.54 |

CriticalFailure@10: hybridpatch 8/180, fullrewrite 13/180 (theta=0.10; collapse-to-0 always counted).

## HybridPatch telemetry
- steps with hybrid telemetry: 400
- route share: `bounded_rewrite`=232/400 (58.0%), `bulk_patch`=48/400 (12.0%), `local_patch`=120/400 (30.0%)
- bounded rewrite share: 232/400 (58.0%)
- copied/generated bytes: 1845772/2844964 | copy:generated ratio=0.6487857139844301 | mean generated byte ratio=0.597
- repair: attempted=41 used=30 success=29 rate=10.2%
- gate failures: 9 (2.2%) | kept-context failures: 9 (2.2%)
- forward no-effective-modification audit: 1/200 (0.5%)
````

</details>
