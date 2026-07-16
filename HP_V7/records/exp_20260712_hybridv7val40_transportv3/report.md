# exp_20260712_hybridv7val40_transportv3 experiment report

> This is the canonical human-readable Git record for this experiment. The authoritative machine scope is [`summary.json`](summary.json).

## Status and scope

- Purpose: Current canonical same-transport comparison of HybridPatch/7 and FullRewrite under transport-v3.
- Lifecycle: `complete`
- Evidence role: `primary`
- Claim eligibility: `canonical`
- Canonical scope: `strict38_complete_pairs` (38/40 samples)
- Canonical backward rows: 760
- Comparison policy: strict38 complete-pair analysis; partial committed rows remain auditable but are excluded from canonical statistics

## Run identity

- Code reference: `HP_V7/VERSION.md`
- Protocol: `hybridpatch/7`
- Dataset / split: `DELEGATE-52` / `val20 + unused_reserve20; strict38 canonical subset`
- Round trips: 10
- Model: `minimax-m3`
- Transport revision: `opencode_anthropic_sdk/3`
- Seed: `42`
- Started: `2026-07-12T07:08:36`
- Finished: `n/a`
- Run Git commit: `n/a`
- Archived fingerprint count: 1

### Recorded command templates

````text
python src\experiment_runner.py --sample <samples> --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260712_hybridv7val40_transportv3 --notes val40 transport-v3 FR arm (frozen FR-v3 baseline for this set); same plans and dir as HP arm, key=<key_label>
python src\experiment_runner.py --sample <samples> --methods hybridpatch --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_20260712_hybridv7val40_transportv3 --notes v7 val40 under transport-v3; HP arm; plans from frozen FR baseline seed42; paired FR-v3 arm follows in same dir, key=<key_label>
````

The complete planned and canonical sample identifiers are in [`experiment.yaml`](experiment.yaml) and [`samples.csv`](samples.csv).

## Canonical results

| Method | Mean backward RS | RS@1 | RS@5 | RS@10 | Critical failures | Tokens | Commit outcomes |
|---|---:|---:|---:|---:|---:|---:|---|
| `hybridpatch` | 0.825 | 0.975 | 0.831 | 0.724 | 21 | 26879707 | applied=730, kept_context=14, partial_applied=11, unchanged=5 |
| `fullrewrite` | 0.817 | 0.941 | 0.822 | 0.705 | 21 | 16089314 | not_applicable=758, unchanged=2 |

### Paired comparisons

| Scope | A | B | Paired rows | Mean A | Mean B | Δ A−B | p | Cohen d | Headline |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `strict38_complete_pairs` | `hybridpatch` | `fullrewrite` | 380 | 0.825 | 0.817 | +0.008 | 0.514 | 0.034 | yes |

## Analysis for the next iteration

1. **Observation:** hybridpatch − fullrewrite = +0.008 across 380 paired backward rows (p=0.514).
   **Interpretation:** The point estimate favors HybridPatch, but this scope does not establish a statistically distinguishable advantage.
   **Implication:** Use this declared scope—not a larger partial directory or a more favorable sensitivity view—when stating the experiment result.
   **Next step:** Inspect the paired win/loss cases below before proposing a method change; treat recurring mechanisms, not isolated scores, as the iteration target.

2. **Observation:** HybridPatch recorded token total is 1.67× FullRewrite (26879707 vs 16089314). Failure labels: evaluator=22, gate=19, infrastructure=40, model=13, quality=49.
   **Interpretation:** Quality, protocol, transport, evaluator, and infrastructure failures are separate mechanisms and should not be merged into one model-error bucket.
   **Implication:** A next-version change should name the failure class it is expected to improve and preserve unaffected behavior.
   **Next step:** Use `casebook.jsonl` and `samples.csv` to define a small counterfactual or smoke set before any full paid rerun.

3. **Observation:** Verification status is `pass`; replayed backward rows=780.
   **Interpretation:** Verification evidence and its documented exceptions bound what can be independently checked from the preserved archive.
   **Implication:** Missing commit IDs, timestamps, or independent replay must remain explicit provenance gaps.
   **Next step:** For new experiments, finalize the record immediately after the verifier and analysis complete.

## Representative casebook

The casebook is selected deterministically from canonical paired score extremes plus partial acceptance, kept-context, critical failures, ordinary failure labels, and canonical exclusions. It contains no prompt or model response body.

| Case | Categories | Sample / RT / direction | Method outcomes | HP−FR |
|---|---|---|---|---:|
| `exp_20260712_hybridv7val40_transportv3:json2:rt02:backward:paired:hybridpatch-win` | hybridpatch_win | `json2` / 2 / `backward` | fullrewrite: RS=0.000, not_applicable, evaluator_error; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260712_hybridv7val40_transportv3:json2:rt01:backward:paired:hybridpatch-win` | hybridpatch_win | `json2` / 1 / `backward` | fullrewrite: RS=0.000, not_applicable, evaluator_error; hybridpatch: RS=1.000, applied, none | +1.000 |
| `exp_20260712_hybridv7val40_transportv3:satellite4:rt10:backward:paired:hybridpatch-win` | hybridpatch_win | `satellite4` / 10 / `backward` | fullrewrite: RS=0.000, not_applicable, evaluator_error; hybridpatch: RS=0.909, applied, none | +0.909 |
| `exp_20260712_hybridv7val40_transportv3:jobboard3:rt02:backward:paired:fullrewrite-win` | fullrewrite_win | `jobboard3` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.150, applied, content_regression | -0.850 |
| `exp_20260712_hybridv7val40_transportv3:jobboard3:rt03:backward:paired:fullrewrite-win` | fullrewrite_win | `jobboard3` / 3 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=0.170, applied, none | -0.830 |
| `exp_20260712_hybridv7val40_transportv3:treebank4:rt06:backward:paired:fullrewrite-win` | fullrewrite_win | `treebank4` / 6 / `backward` | fullrewrite: RS=0.684, not_applicable, content_regression; hybridpatch: RS=0.000, applied, content_regression | -0.684 |
| `exp_20260712_hybridv7val40_transportv3:crystal6:rt01:backward:paired:near-tie` | near_tie | `crystal6` / 1 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260712_hybridv7val40_transportv3:crystal6:rt02:backward:paired:near-tie` | near_tie | `crystal6` / 2 / `backward` | fullrewrite: RS=1.000, not_applicable, none; hybridpatch: RS=1.000, applied, none | +0.000 |
| `exp_20260712_hybridv7val40_transportv3:emails5:rt03:backward:hybridpatch:partial-acceptance` | partial_acceptance, failure:validation_gate | `emails5` / 3 / `backward` | hybridpatch: RS=0.944, partial_applied, validation_gate | n/a |
| `exp_20260712_hybridv7val40_transportv3:mathlean5:rt01:backward:hybridpatch:partial-acceptance` | partial_acceptance, failure:validation_gate | `mathlean5` / 1 / `backward` | hybridpatch: RS=0.998, partial_applied, validation_gate | n/a |
| `exp_20260712_hybridv7val40_transportv3:quantum1:rt07:backward:hybridpatch:partial-acceptance` | partial_acceptance, failure:validation_gate | `quantum1` / 7 / `backward` | hybridpatch: RS=0.965, partial_applied, validation_gate | n/a |
| `exp_20260712_hybridv7val40_transportv3:satellite4:rt09:backward:hybridpatch:kept-context` | kept_context, failure:thinking_budget_exhausted | `satellite4` / 9 / `backward` | hybridpatch: RS=0.909, kept_context, thinking_budget_exhausted | n/a |
| `exp_20260712_hybridv7val40_transportv3:screenplay5:rt08:backward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `screenplay5` / 8 / `backward` | hybridpatch: RS=0.984, kept_context, validation_gate | n/a |
| `exp_20260712_hybridv7val40_transportv3:earncall1:rt01:forward:hybridpatch:kept-context` | kept_context, failure:validation_gate | `earncall1` / 1 / `forward` | hybridpatch: RS=n/a, kept_context, validation_gate | n/a |
| `exp_20260712_hybridv7val40_transportv3:crystal6:rt04:backward:fullrewrite:critical-failure` | critical_failure, failure:evaluator_error | `crystal6` / 4 / `backward` | fullrewrite: RS=0.000, not_applicable, evaluator_error | n/a |
| `exp_20260712_hybridv7val40_transportv3:crystal6:rt04:backward:hybridpatch:critical-failure` | critical_failure, failure:evaluator_error | `crystal6` / 4 / `backward` | hybridpatch: RS=0.000, applied, evaluator_error | n/a |

## Failures and exclusions

- Non-none failure rows: 143
- Affected sample-method pairs: 48
- Failure stages: evaluator=22, gate=19, infrastructure=40, model=13, quality=49
- Canonical exclusions: content_regression=2, infrastructure_incomplete=40, validation_gate=2
- Excluded `earncall1`: FullRewrite infrastructure failure after RT6; score=null and excluded from canonical pairing.
- Excluded `json4`: Both arms infrastructure failure after RT2; score=null and excluded from canonical pairing.

## Known exceptions and provenance gaps

- Known exception: analysis/comparison.md includes partial committed rows and is not the canonical statistical scope.
- Known exception: Current HP_V7 runtime fingerprints differ from the archived runtime in experiment_runner.py and model_openai.py; archive metadata is authoritative.
- Verification exception: Twenty replayed backward rows belong to the preserved partial histories of excluded infrastructure-failure samples.
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

The original source report `HP_V7/exp_20260712_hybridv7val40_transportv3/analysis/val40v3_38sample_paired.md` is inside the ignored private experiment archive. A sanitized immutable snapshot is embedded below so a clean Git clone can audit the original analysis without the raw archive.

<details>
<summary>Sanitized source report snapshot</summary>

````text
# val40 transport-v3 canonical paired analysis (38 samples)

Generated 2026-07-12 from committed JSONL rows; honesty gate PASS 780/780 backward RS
(`val40v3_verify.log`). Canonical scope: **38 complete paired samples** — `json4` (both
arms, transport budget exhausted after 3 keys, stopped at RT2) and `earncall1` (FR arm,
stopped at RT6) are infrastructure failures per `docs/API_TRANSPORT_FROZEN_V3.md` §4
(score=null, not model 0) and are excluded from all numbers below. The sibling
`comparison.md` is the standard full-dir artifact and additionally pairs the committed
partial rows of the two failed samples — its aggregates differ slightly from this file;
this file is the canonical report.

Both arms fresh under transport revision `opencode_anthropic_sdk/3` (adaptive thinking,
max_tokens 131072), same frozen task plans byte-copied from `exp_20260710_frbaseline234`
(seed 42), 10 dispatch slots (KEY_01–10 × 1) + KEY_11 reserve. HP protocol
`hybridpatch/7`.

## Paired backward RS (n=380)

| scope | HP | FR | delta | t | p | d |
|---|---|---|---|---|---|---|
| headline | 0.825 | 0.817 | +0.008 | 0.65 | 0.51 | 0.03 |
| ECR-only (n=366) | 0.826 | 0.827 | -0.001 | -0.08 | 0.94 | -0.00 |
| disclosure: excl 2 FR-pathology samples (n=360) | 0.822 | 0.832 | -0.010 | -0.88 | 0.38 | -0.05 |

- RS@1/5/10: HP 0.975/0.831/0.724 vs FR 0.941/0.822/0.705.
- CriticalFailure@10 (theta 0.10): HP 21/342, FR 21/342 (identical).
- Tokens: HP 26.88M vs FR 16.09M (1.67x). `preservation_violations=0` throughout.
- FR pathology samples under v3 (raw<200B or empty-class rows): only 2 —
  landmarks3 (RT10 fwd `thinking_budget_exhausted`), satellite4 (RT3 fwd empty + 3 short rows).

## FR transport sensitivity (the mechanism finding)

Same 38 samples, same task plans, per-step paired:

- **FR-v3 0.817 vs frozen FR baseline (transport-v2 era) 0.728: delta +0.089 (t=5.18, p=3.7e-07).**
- v6-era like-for-like delta on the same 38 (HP-v6 vs FR-baseline): +0.119 (t=5.42).
- HP longitudinal (HP-v7-v3 vs HP-v6-val40): 0.825 vs 0.848, delta -0.022 (t=-1.70, p=0.09).

Reading: transport-v3's response-slot retry (full replay POST after `incomplete_stream`)
absorbs the server-side mid-thinking stream truncation identified in FINDINGS §227. FR —
which had zero guardrails against it — recovers +0.089; HP, which already absorbed the
same pathology via its repair second call, gains nothing. The former val40 headline
advantage (+0.137 / +0.119 on these 38) collapses to +0.008 (n.s.). This matches the
§225/§227 disclosure-scope prediction (clean-subset delta +0.030/+0.042).

## HP telemetry (760 steps)

- Routes: bounded_rewrite 473 (62.2%), local_patch 186, bulk_patch 93, kept-context 8-route rows.
- kept-context 14/760; partial_acceptance **11/760**; repair attempted 44 / used 27 / success 27.
- generated_byte_ratio mean 0.619; forward no-effective-modification 2/373.

Partial acceptance events (scored backward): emails5 RT3 0.944, mathlean5 RT1 0.998,
quantum1 RT7 0.965, screenplay4 RT3 0.963, subtitles6 RT4 0.977, spreadsheet1 RT5 0.296
(8 ops skipped; v6 same sample fell to 0.278 at RT6 — recurring task-position class,
mechanical kept-context counterfactual pending), plus 5 forward events (unscored).

## Largest per-sample HP moves vs v6-val40 (same task plans; transport + sampling confounded)

- Down: obj3d2 -0.500 (zero from RT6; clean bounded_rewrite, evaluator parsed —
  vertex/face accuracy 0.0, content-level damage), treebank4 -0.477 (zero from RT6;
  bulk_patch + repair used, completeness 0.0 with per-token quality 1.0),
  filesystem3 -0.303 (RT5 bounded regeneration, path_coverage 0.368), spreadsheet6 -0.231.
- Up: robotics1 +0.529 (v6's RT6 lock-in did not recur), subtitles6 +0.290, emails5 +0.181.
- None of the three collapse samples involved partial acceptance; forms are the known
  content-level regeneration/damage classes, **not** C2-lint-gap parse regressions.
````

</details>
