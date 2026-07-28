"""Small schema-aware builders shared by infrastructure regression tests."""

import os


def run_metadata_kwargs(
        *, command="python test", samples=("sample",),
        methods=("hybridpatch", "fullrewrite"), num_round_trips=1,
        seed=42, model="offline-test-model", distractor=False,
        max_tokens=16, printing=False, **overrides):
    """Return a fresh valid baseline for ``append_run_metadata`` tests.

    ``methods=None`` intentionally omits that argument for phased tests which
    supply the active method at the call site.  Every mutable field is copied so
    one test cannot alter another test's fixture.
    """
    fields = {
        "command": command,
        "num_round_trips": num_round_trips,
        "seed": seed,
        "model": model,
        "distractor": bool(distractor),
        "max_tokens": max_tokens,
        "printing": bool(printing),
    }
    if samples is not None:
        fields["samples"] = list(samples)
    if methods is not None:
        fields["methods"] = list(methods)
    fields.update(overrides)
    return fields


def event_append_crash(
        cut, message="injected event append death", *, include_cut=False):
    """Build a crash injector that durably writes exactly ``cut`` bytes."""
    if not isinstance(cut, int) or isinstance(cut, bool) or cut < 0:
        raise ValueError("cut must be a non-negative integer")

    def crash(events_path, event_bytes):
        with open(events_path, "ab") as handle:
            handle.write(event_bytes[:cut])
            handle.flush()
            os.fsync(handle.fileno())
        detail = f"{message} at byte {cut}" if include_cut else message
        raise SystemExit(detail)

    return crash


__all__ = ["event_append_crash", "run_metadata_kwargs"]
