# HP_V3 experiment records

> This is a provenance overlay. It does not alter frozen runtime, prompt,
> checkpoint, raw-response, or evaluator bytes. Each record's `report.md` is
> the canonical human entry; `summary.json` is the sole canonical machine scope.

| Experiment | Lifecycle | Evidence | Claim use | Canonical scope | Human report | Machine summary | Verification |
|---|---|---|---|---|---|---|---|
| [exp_20260707_hybridv3dev20](records/exp_20260707_hybridv3dev20/experiment.yaml) | superseded | supporting | supporting | historical20_complete_pairs (20 samples) | [read report](records/exp_20260707_hybridv3dev20/report.md) | [diagnostic/supporting only; 400 auditable backward rows](records/exp_20260707_hybridv3dev20/summary.json) | [pass](records/exp_20260707_hybridv3dev20/verification.txt) |
| [exp_20260708_hybridv3dev20full](records/exp_20260708_hybridv3dev20full/experiment.yaml) | complete | primary | canonical | dev20_complete_pairs (20 samples) | [read report](records/exp_20260708_hybridv3dev20full/report.md) | [Δ=+0.141, n=200](records/exp_20260708_hybridv3dev20full/summary.json) | [fail (known exception)](records/exp_20260708_hybridv3dev20full/verification.txt) |

Raw experiment directories remain ignored and private. Their content tree hash,
size, retention class, and sanitization status are recorded in each
`raw_manifest.json`.
