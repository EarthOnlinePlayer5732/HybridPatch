# HP_V9 zero-API readiness

Run project commands from `HP_V9/` with Git Bash.  Every suite below is local
and zero-API; provider calls are mocked or rejected by the tests.

## Fast transport and normalization

```bash
PYTHONUTF8=1 python -B ./src/run_regression_tier.py --tier fast
```

This tier contains the Anthropic-compatible, DeepSeek/OpenAI-compatible, and
MiniMax official transport test classes.  It avoids dispatcher/recovery cases.

## Component integration

```bash
PYTHONUTF8=1 python -B ./src/run_regression_tier.py --tier component
PYTHONUTF8=1 python -B ./src/test_snapshot_io.py
PYTHONUTF8=1 python -B ./src/test_analyze.py
```

This tier covers recorder, ledger, runner, dispatcher, inspector, evaluator,
and campaign behavior, excluding the crash/recovery contracts listed in the
explicit class/method manifest.

## Crash and recovery

```bash
PYTHONUTF8=1 python -B ./src/run_regression_tier.py --tier recovery
```

This tier retains the original test IDs and selects the exact contracts in
`src/regression_tier_manifest.py`.  The selector validates that every declared
class and method exists; its own test proves that fast, component, and recovery
remain disjoint and together cover the canonical compatibility suite.

## Complete compatibility suite

```bash
PYTHONUTF8=1 python -B ./src/test_model_openai.py
PYTHONUTF8=1 python -B ./src/run_regression_tier.py --tier all
PYTHONUTF8=1 python -B ./src/test_regression_tiers.py
```

The first command remains the canonical compatibility entry point.  The tier
runner is only a selection layer; it does not move or rename those tests.

## V9 infrastructure readiness gates

```bash
PYTHONUTF8=1 python -B ./src/test_infrastructure_stress.py
PYTHONUTF8=1 python -B ./src/test_v9_core_identity.py
PYTHONUTF8=1 python -B -m unittest -v test_quick_experiment_summary
```

`test_infrastructure_stress.py` uses the formal 30-worker shape without calling
a provider: shared JSONL append, metadata registration/finish, and concurrent
round-trip commit versus exclusive stop publication. `test_v9_core_identity.py`
compares prompts, domain implementations, executor/model/evaluator helpers, and
requirements byte-for-byte with frozen HP_V8 commit
`7b8fe009a892039db4718ef0cfbb03d35d97d10c`.

## Fast provisional result view

```bash
PYTHONUTF8=1 python -B ./src/quick_experiment_summary.py --dir <out_dir>
```

This command is read-only and prints JSON to stdout. It reports committed-row
campaign completeness separately from the exact paired backward RS@RT10
endpoint. Its output is always labeled `provisional_unverified`: it does not run
the campaign inspector, honesty replay, artifact seal, or record finalization.

## GitHub Actions

`.github/workflows/hp-v9-zero-api.yml` runs the three regression tiers, the
30-worker stress gate, scientific-core identity, quick-summary test, tracked
Python compilation, and `git diff --check` on `windows-latest`. It has no model
provider secrets and performs no provider calls.
