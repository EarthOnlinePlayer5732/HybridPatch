# HybridPatch

HybridPatch is a constrained-write alternative to full document rewriting for long-horizon editing. An LLM emits a structured envelope; a deterministic executor applies it and preserves every undeclared source byte verbatim. The research goal is one claim: **beat FullRewrite overall** on the same seeded task sequence.

## Versioned layout

```text
HP_V3/ ... HP_V7/  frozen, self-contained method snapshots
HP_V8/             active draft method snapshot and diagnostic records
  src/              method + runner + verifier + transport state of that version
  prompts/          real copy; domain evaluators load it relative to cwd
  data/             Windows junction to the shared top-level data/
  exp_*/            ignored private raw archives owned by that method version
  records/          compact Git-tracked audit records
  EXPERIMENTS.md    experiment index for that version
  VERSION.md        provenance, fingerprints and real acceptance evidence
transport/          API/transport development source, docs and diagnostics
Baseline/           frozen/fullrewrite baselines and baseline reruns
data/               shared read-only DELEGATE-52 samples and frozen splits
docs/               cross-version project/design/history documents
_attic/             retained deletion candidates and pre-restructure fallback
MIGRATION_MAP.md    old path -> new path inventory
```

`HP_V7` is the latest frozen runnable reference; `HP_V8` is the current active draft. Do not mutate HP_V8 method semantics after its recorded campaign—a new method change begins in `HP_V9`. Every HP directory is its own run root: enter it before invoking `src/*.py`. Top-level `.env` files are never copied into versions, so API commands must inject the parent environment explicitly.

## Setup

From `hybridpatch_clean`:

```bash
pip install -r ./requirements.txt
cd ./HP_V8
export PYTHONUTF8=1
```

## No-API sanity

```bash
python -B ./src/test_hybrid_executor.py
python -B ./src/splitters.py
python -B ./src/test_model_openai.py
```

## Run a paired experiment

Only run a new method experiment inside the active writable version, and create `HP_V9` before changing the recorded HP_V8 method; do not add results to frozen `HP_V3`–`HP_V7`. Use a new out_dir. Return to the `hybridpatch_clean` root before running this block. `python-dotenv` loads the single top-level secret file into the child process without copying it:

```bash
export PYTHONUTF8=1
cd ./HP_V8
python -m dotenv -f ../.env run -- python ./src/experiment_runner.py --sample malware6 latex2 --methods hybridpatch fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --out_dir exp_demo --notes "demo"
python -B ./src/verify_anchorpatch.py --dir ./exp_demo
python -B ./src/analyze.py --dir ./exp_demo --K 10 --critical_theta 0.10
```

Method experiments belong in their `HP_Vx/`; transport diagnostics use `../transport/exp_*`; baselines use `../Baseline/exp_*`. New experiments require result verification before citation. For frozen archives,
cite the existing verification record; do not rerun verification merely to
regenerate documentation.

## Review code and results

Start with [the AI review guide](./docs/AI_REVIEW_GUIDE.md), then use
[the global experiment index](./docs/EXPERIMENT_INDEX.md). Every experiment
links to a Git-readable `report.md`, a machine-authoritative `summary.json`, and
a representative `casebook.jsonl`. A clean clone can validate the committed
evidence without private raw archives:

Reports are not stored at the repository root. They are generated under the
experiment owner:

```text
HP_Vx/records/<experiment_id>/report.md
Baseline/records/<experiment_id>/report.md
transport/records/<experiment_id>/report.md
```

The same-transport canonical boundary remains the
[`HP_V7 strict38 report`](./HP_V7/records/exp_20260712_hybridv7val40_transportv3/report.md).
The current active-version diagnostic is the
[`HP_V8 snapshotfix paired10 summary`](./HP_V8/analysis/exp_20260718_hybridv8_transportv4_snapshotfix_paired10.md).
These compact records are the Git-tracked, GitHub-readable audit surface. Raw
`exp_*` archives and API payloads remain ignored and are retained separately as
described in [the experiment-record policy](./docs/EXPERIMENT_RECORDS.md).

```bash
python ./tools/validate_experiment_records.py --records-only
```

Before a new paid HP_V8+ experiment, create
`docs/experiment_plans/<experiment_id>.md` from
[`docs/experiment_plans/TEMPLATE.md`](./docs/experiment_plans/TEMPLATE.md) and
follow the [standard experiment process](./docs/标准实验流程.md). After all workers
stop, use the two-stage post-processing workflow:

```bash
python ./tools/process_experiment.py prepare \
  --experiment ./HP_V8/exp_YYYYMMDD_SLUG \
  --confirm-stopped

# Review analysis/record_review.yaml, then:
python ./tools/process_experiment.py finalize \
  --experiment ./HP_V8/exp_YYYYMMDD_SLUG
```

See [the record specification](./docs/EXPERIMENT_RECORDS.md) for record schema
and validation. [CLAUDE.md](./CLAUDE.md) defines working discipline,
[MIGRATION_MAP.md](./MIGRATION_MAP.md) records the restructure inventory,
`HP_Vx/VERSION.md` provides version provenance, and `transport/docs/` contains
the transport specifications.
