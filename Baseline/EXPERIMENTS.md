# Baseline experiment records

> This is a provenance overlay. It does not alter frozen runtime, prompt,
> checkpoint, raw-response, or evaluator bytes. Each record's `report.md` is
> the canonical human entry; `summary.json` is the sole canonical machine scope.

| Experiment | Lifecycle | Evidence | Claim use | Canonical scope | Human report | Machine summary | Verification |
|---|---|---|---|---|---|---|---|
| [exp_20260710_frbaseline234](records/exp_20260710_frbaseline234/experiment.yaml) | complete | baseline | canonical | frozen233 (233 samples) | [read report](records/exp_20260710_frbaseline234/report.md) | [fullrewrite RS@1=0.925/RS@5=0.682/RS@10=0.558](records/exp_20260710_frbaseline234/summary.json) | [pass](records/exp_20260710_frbaseline234/verification.txt) |
| [exp_20260713_frbaseline_v2_rerun](records/exp_20260713_frbaseline_v2_rerun/experiment.yaml) | incomplete | supporting | none | complete83_source_chains (83 samples) | [read report](records/exp_20260713_frbaseline_v2_rerun/report.md) | [83 complete source chains; no standalone claim](records/exp_20260713_frbaseline_v2_rerun/summary.json) | [not_run (structural pass)](records/exp_20260713_frbaseline_v2_rerun/verification.txt) |
| [fr_plus_official](records/fr_plus_official/experiment.yaml) | complete | sensitivity | supporting | logical233 (233 samples) | [read report](records/fr_plus_official/report.md) | [paper-policy Δ=+0.262, n=380](records/fr_plus_official/summary.json) | [not_run (derivation pass)](records/fr_plus_official/verification.txt) |

Raw experiment directories remain ignored and private. Their content tree hash,
size, retention class, and sanitization status are recorded in each
`raw_manifest.json`.
