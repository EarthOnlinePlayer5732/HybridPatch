# HP_V8 transport-v4 supplement40 diagnostic summary

Status: complete, diagnostic-only. This is a sanitized derived view; raw prompts,
documents, responses, transport events and credentials remain in the private archive.

## Identity and completeness

- Experiment: `exp_20260719_hybridv8_transportv4_supplement40_lockfix`
- Run commit/tree: `effad42675b6d112c62e42d695617166e27f989c` / clean
- Method/transport: `hybridpatch/8` vs FullRewrite / `opencode_anthropic_sdk/4`
- Model/config: MiniMax-M3, seed 42, distractor-on, fixed V6 val40 set, 10 RT
- Scheduling: 13 live Keys, at most four workers per Key, HP-first/FR-first 20/20
- Result grid: 1,600/1,600 unique rows; forward/backward 800/800
- Checkpoints/outcomes: 80/80 at RT10; all 40 latest sample outcomes `finished`
- Independent recomputation: PASS, 800/800 backward RS reproduced from raw responses
- Strict inspector: `errors=[]`; 1,704 semantic/provider calls; no duplicate,
  half-committed or unmapped evidence
- Preservation: 0 violations across 797 applicable HP steps; three no-route
  kept-context steps are explicit N/A

The earlier metadata-lock launch at commit `139fbdc...` is retained separately as
`failed_informative`: it produced zero result rows, checkpoints or response commits.
No request, response or score from that directory was spliced into this campaign.

## RT1–RT10: supplement40, fixed n=40

| RT | HP_V8 | FullRewrite | paired HP−FR |
|---:|---:|---:|---:|
| 1 | 0.962799 | 0.911714 | +0.051086 |
| 2 | 0.958183 | 0.870429 | +0.087754 |
| 3 | 0.907385 | 0.765497 | +0.141888 |
| 4 | 0.876782 | 0.742846 | +0.133937 |
| 5 | 0.872095 | 0.715892 | +0.156203 |
| 6 | 0.861985 | 0.647067 | +0.214918 |
| 7 | 0.860351 | 0.644850 | +0.215502 |
| 8 | 0.835554 | 0.622366 | +0.213188 |
| 9 | 0.845766 | 0.604390 | +0.241376 |
| 10 | 0.837339 | 0.573381 | +0.263959 |

At RT10 the paired sample SD is `0.402060`; wins/losses/ties are `26/10/4`.
CriticalFailure@0.10 is `20/360` for HP_V8 and `27/360` for FullRewrite.

## RT1–RT10: prior10, fixed n=10

| RT | HP_V8 | FullRewrite | paired HP−FR |
|---:|---:|---:|---:|
| 1 | 0.951867 | 0.939410 | +0.012456 |
| 2 | 0.766623 | 0.788462 | -0.021839 |
| 3 | 0.673989 | 0.726827 | -0.052838 |
| 4 | 0.671289 | 0.708776 | -0.037487 |
| 5 | 0.750934 | 0.660609 | +0.090326 |
| 6 | 0.751401 | 0.660281 | +0.091120 |
| 7 | 0.758411 | 0.693513 | +0.064899 |
| 8 | 0.748846 | 0.692798 | +0.056047 |
| 9 | 0.744897 | 0.688658 | +0.056239 |
| 10 | 0.745178 | 0.695854 | +0.049324 |

At RT10 the paired sample SD is `0.344047`; wins/losses/ties are `4/4/2`.
CriticalFailure@0.10 is `4/90` for HP_V8 and `8/90` for FullRewrite.

## RT1–RT10: pooled 50 completed campaign chains, fixed n=50

Six sample IDs occur in both campaigns. This table keeps both completed chains and
therefore does not claim 50 independent sample identities.

| RT | HP_V8 | FullRewrite | paired HP−FR |
|---:|---:|---:|---:|
| 1 | 0.960613 | 0.917253 | +0.043360 |
| 2 | 0.919871 | 0.854035 | +0.065836 |
| 3 | 0.860706 | 0.757763 | +0.102943 |
| 4 | 0.835684 | 0.736032 | +0.099652 |
| 5 | 0.847863 | 0.704835 | +0.143028 |
| 6 | 0.839868 | 0.649710 | +0.190159 |
| 7 | 0.839963 | 0.654582 | +0.185381 |
| 8 | 0.818212 | 0.636452 | +0.181760 |
| 9 | 0.825592 | 0.621244 | +0.204348 |
| 10 | 0.818907 | 0.597876 | +0.221032 |

At RT10 the paired chain SD is `0.397397`; wins/losses/ties are `30/14/6`.
CriticalFailure@0.10 is `24/450` for HP_V8 and `35/450` for FullRewrite.

## RT1–RT10: deduplicated 44-ID descriptive view, fixed n=44

For each overlapping ID, the two campaign chains are averaged within method and RT
before the 44 IDs receive equal weight.

| RT | HP_V8 | FullRewrite | paired HP−FR |
|---:|---:|---:|---:|
| 1 | 0.961193 | 0.917790 | +0.043403 |
| 2 | 0.925792 | 0.856490 | +0.069302 |
| 3 | 0.868251 | 0.756247 | +0.112004 |
| 4 | 0.846458 | 0.744613 | +0.101844 |
| 5 | 0.851869 | 0.715410 | +0.136459 |
| 6 | 0.842926 | 0.652303 | +0.190623 |
| 7 | 0.843015 | 0.652367 | +0.190647 |
| 8 | 0.818350 | 0.631822 | +0.186528 |
| 9 | 0.827466 | 0.615564 | +0.211902 |
| 10 | 0.819838 | 0.585641 | +0.234197 |

At RT10 the paired ID SD is `0.404467`; wins/losses/ties are `26/13/5`.
CriticalFailure@0.10 is `21/396` for HP_V8 and `32/396` for FullRewrite.

## Protocol, transport and cost telemetry

- Prompt profiles: block movement/default = `423/377`.
- Valid declared routes: bounded/local/bulk/DSL = `579/148/54/16`; three
  complete model-empty rows have no valid route and keep context.
- HP repair: attempted `104/800`, used `88/800`, successful `78/104`.
- Final protocol failure: `20/800` (`2.5%`); partial acceptance `3/800`.
- Soft burden threshold exceeded: `49/800`; every envelope continued execution.
- Ten complete primary responses are thinking-only max-tokens model failures:
  HP `3/800`, FullRewrite `7/800`.
- Twelve generation-started incomplete-stream attempts lack final usage; all
  recovered in the second response slot of the same semantic call. Terminal
  transport/sample infrastructure failures are zero.

Known final usage is:

| method / call kind | calls | provider tokens | known USD |
|---|---:|---:|---:|
| HP primary | 800 | 31,043,628 | 24.652002 |
| HP repair | 104 | 2,018,263 | 1.929892 |
| **HP total** | **904** | **33,061,891** | **26.581893** |
| **FullRewrite** | **800** | **25,796,080** | **21.104516** |
| **supplement40 total** | **1,704** | **58,857,971** | **47.686409** |

For the twelve attempts without final usage, a deliberately pessimistic bound adds
at most USD `2.261795`, giving a supplement archive-based upper value of USD
`49.948204`. Prior10 plus supplement40 have `73,966,812` known tokens and USD
`60.294154` known cost; their 22 unknown-usage attempts raise the descriptive
archive-based upper value to USD `64.436236`. These are not provider billing
statements. The failed launch and two probe rounds are excluded from method usage;
their pre-registered USD `6.60` reserve gives a lockfix campaign-family upper value
of USD `56.548204`, below its USD `75` startup budget.

## Claim boundary and retention

All 44 IDs were previously exposed. The two campaigns differ in commit, date,
concurrency and generation randomness; paired deltas are highly heterogeneous.
The results support a complete diagnostic observation, not population
generalization, statistical significance, universal HP_V8 superiority or a causal
score effect from the metadata-lock fix.

Standard `prepare`, two independent read-only reviews, generated-state `finalize`
and both record validators passed. The global catalog snapshot is not published
because its digest would rewrite every frozen HP_V3–HP_V7/Baseline/transport record.
The generated HP_V8 record is preserved privately instead.

After active research documentation was updated, the final records-only validator
passes all 13 published records. The source-linked mode reports 21 stale source-hash
errors embedded in frozen V3–V7/Baseline/transport records; those records are not
rewritten merely to follow active-document hashes.

- Final complete raw archive:
  `../hybridpatch_private_archives/exp_20260719_hybridv8_transportv4_supplement40_lockfix_complete_final.tgz`
  (`79d5573ea606544559c68f6630df1c1f90de4679642bcec5f038f9e0e28b9c10`)
- Generated-record snapshot:
  `../hybridpatch_private_archives/exp_20260719_hybridv8_transportv4_supplement40_lockfix_generated_records.tgz`
  (`803d78492e0aa9e308b8263c38668800a5ca7c99dd553aa3350a60b0ef2588d0`)
- Final credential scan: 15,264 files / 951,396,234 bytes checked against 13
  recognized local Key values; zero exact-Key, Authorization, Cookie or private-key
  matches. Bare `Bearer ` text occurs only in 83 json2 task-content artifacts.

No paid API call was made during analysis, review, finalization or reporting.
