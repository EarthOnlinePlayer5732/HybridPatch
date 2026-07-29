# HP_V8 transport-v4 snapshotfix paired10 diagnostic summary

Status: complete, diagnostic-only. This is a sanitized derived view; raw prompts,
documents, responses, transport events and credentials remain in the private archive.

## Identity and completeness

- Experiment: `exp_20260718_hybridv8_transportv4_snapshotfix_paired10`
- Run commit/tree: `bc4a39f7dd1476848927d45bcf1840a74677e6be` / clean
- Method/transport: `hybridpatch/8` vs FullRewrite / `opencode_anthropic_sdk/4`
- Model/config: MiniMax-M3, seed 42, distractor-on, 10 fixed exposed samples, 10 RT
- Result grid: 400/400 unique rows; forward/backward 200/200
- Checkpoints/outcomes: 20/20 at RT10; all 10 latest sample outcomes `finished`
- Verifier: PASS, 200/200 backward RS independently reproduced from raw responses
- Strict inspector: `errors=[]`; evidence digest
  `6e0750f7779ee6c20d4c11c4d0f15a5c84c52c02472e0e0d15e25d0f217de3f2`
- Preservation: 0 violations across 199 applicable HP steps; one model-empty
  kept-context step is explicit N/A

The host lost power at 362/400 committed rows. Seven samples were already complete.
After sealing the full pre-recovery archive, only `filesystem3`, `musicsheet2` and
`satellite4` resumed from their first uncommitted checkpoint suffix. The 82 rows
already committed for those samples caused zero later provider POSTs. Three local
journal replays have `provider_called=false`. Satellite4 exhausted the second g000
response slot, was isolated as `infrastructure_incomplete`, and then completed via
the single authorized g001; the other samples continued independently.

## Exact backward RT10 endpoint

Each sample contributes exactly one pair. No earlier RT substitutes for RT10.

| sample | HP_V8 | FullRewrite | HP−FR |
|---|---:|---:|---:|
| circuit2 | 1.000000 | 1.000000 | +0.000000 |
| docker6 | 0.926669 | 0.977431 | -0.050762 |
| filesystem3 | 0.769635 | 0.000000 | +0.769635 |
| jobboard3 | 0.136595 | 0.278016 | -0.141420 |
| json2 | 0.956561 | 0.926884 | +0.029677 |
| mathlean2 | 0.217009 | 0.791496 | -0.574488 |
| musicsheet2 | 0.586748 | 0.461518 | +0.125229 |
| obj3d2 | 1.000000 | 1.000000 | +0.000000 |
| satellite4 | 0.881563 | 0.524281 | +0.357281 |
| treebank4 | 0.976999 | 0.998917 | -0.021917 |
| **mean** | **0.745178** | **0.695854** | **+0.049324** |

Sample SD of paired deltas is `0.344047`; positive/negative/tie counts are `4/4/2`.
CriticalFailure@0.10 is `4/90` for HP_V8 and `8/90` for FullRewrite.

## Protocol and cost telemetry

- Declared HP routes: bounded/local/bulk/DSL = `124/54/14/7`; one complete
  thinking-only max-tokens response has no legal route and keeps context.
- Prompt profiles: block movement/default = `117/83`.
- Repair: attempted `32/200`; selected and successful `21/32`.
- Final kept-context protocol failure: `11/200`.
- Soft burden threshold exceeded: `12/200`; all envelopes continued execution.
- Known final usage: HP (including repair) `8,448,270` tokens / USD `7.036488`;
  FullRewrite `6,660,571` / USD `5.571257`; total `15,108,841` / USD `12.607745`.

Seven incomplete streams and three host-power-loss attempts observed generation
deltas but have no final usage. The token and USD totals above are therefore the
auditable known portion, not an exact provider bill.

## Claim boundary and retention

All samples were previously exposed, the endpoint deltas are highly heterogeneous,
and removing `filesystem3` changes the mean delta to negative. This campaign does
not establish population generalization, universal HP_V8 superiority, or a causal
score effect from the dispatcher snapshot fix. The fix's causal evidence is the
deterministic zero-API concurrency test.

The standard `prepare`, two independent read-only reviews, `finalize`, and both
record validators passed in the generated state. The Git catalog snapshot is not
published because registering HP_V8 changes the catalog hash embedded in every
frozen HP_V3–HP_V7/Baseline/transport record, violating the frozen-record boundary.
The generated record bundle is preserved privately instead.

- Complete raw archive:
  `../hybridpatch_private_archives/exp_20260718_hybridv8_transportv4_snapshotfix_paired10_complete.tgz`
  (`d24f416498c754d0be317d02c5b8f4916a7f21b4eb558c8e6d64184a89834508`)
- Pre-recovery 362/400 archive:
  `../hybridpatch_private_archives/exp_20260718_hybridv8_transportv4_snapshotfix_paired10_powerloss_362of400.tgz`
  (`7fe215abe20e311f426e5329e9e8ba0125c9bedbec0f512b8e2d5e373e262354`)
- Recovery runtime bundle:
  `../hybridpatch_private_archives/exp_20260718_hybridv8_transportv4_snapshotfix_paired10_recovery_runtime.tgz`
  (`245001818ba8afaa8974f6b48ff6b7f42ac13a17fdb6c030dcd1f2b09f8522f1`)
- Generated record snapshot:
  `../hybridpatch_private_archives/exp_20260718_hybridv8_transportv4_snapshotfix_paired10_generated_records.tgz`
  (`d86e944e5ac5cd225c21670626a288861f958d3ed24026635dda5756ca9d5f53`)

The complete raw archive contains 3,754 files. Scanning against 11 local Key
values and Authorization, Cookie, and private-key markers found zero matches; Key
values were never printed.

## Final zero-API regression

- Python compile: 98 tracked HP_V8/transport/tools files PASS
- Hybrid executor/replay matrix: 72/72 PASS
- Splitters: byte-exact PASS
- HP integrated transport/dispatcher: 85/85 PASS
- Transport-core: 47/47 PASS
- Analyzer/process tests: 5/5 and 14/14 PASS
- Burden report: 400 rows, 394 successful V7 envelopes, 25 soft-overage union,
  one path incompatibility among 399 observed routes
- Frozen V7 dev20 replay: PASS 400 backward RS
- This campaign replay: PASS 200 backward RS
- Final protocol collection: exactly `hybridpatch/8`
- `git diff --check` and `validate_experiment_records.py --records-only`: PASS

The full source-linked records validator reports one pre-existing HEAD mismatch:
the frozen transport-v2 record stores the hash and 5,184-byte size of an older
`transport/API_ITERATION_LOG.md`, while current HEAD already contains the later
7,583-byte log. Updating that frozen record is outside this change's boundary.
