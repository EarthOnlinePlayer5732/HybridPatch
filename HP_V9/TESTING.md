# HP_V8 regression tiers

Run project commands from `HP_V8/` with Git Bash.  Every suite below is local
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
