"""Read-only provisional summary for a live or completed HP_V9 campaign.

This command deliberately does not run honesty replay, the campaign inspector,
artifact sealing, or record finalization.  It is a fast view of committed result
rows, never a substitute for the canonical post-processing workflow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict

from analyze import load_formal_rows, sample_level_final_endpoint


METHODS = ("hybridpatch", "fullrewrite")
DIRECTIONS = ("forward", "backward")


def _read_manifest(out_dir):
    path = os.path.join(os.path.abspath(out_dir), "dispatch_manifest.json")
    with open(path, "rb") as handle:
        payload = handle.read()
    try:
        manifest = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError("dispatch manifest is not valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict) or not isinstance(
            manifest.get("config"), dict):
        raise RuntimeError("dispatch manifest/config is invalid")
    return manifest, hashlib.sha256(payload).hexdigest()


def _method_completeness(rows, samples, round_trips):
    expected_keys = {
        (round_trip, direction)
        for round_trip in range(1, round_trips + 1)
        for direction in DIRECTIONS
    }
    rows_by_sample = defaultdict(list)
    for row in rows:
        rows_by_sample[row["sample_id"]].append(row)

    complete = []
    incomplete = {}
    for sample in samples:
        keys = [
            (row.get("round_trip_num"), row.get("round_trip_direction"))
            for row in rows_by_sample.get(sample, [])
        ]
        counts = Counter(keys)
        duplicates = sorted(
            f"RT{key[0]}:{key[1]}"
            for key, count in counts.items() if count > 1
        )
        observed = set(keys)
        missing = expected_keys - observed
        unexpected = observed - expected_keys
        if not duplicates and not missing and not unexpected:
            complete.append(sample)
            continue
        incomplete[sample] = {
            "committed_row_count": len(keys),
            "unique_expected_step_count": len(observed & expected_keys),
            "missing_step_count": len(missing),
            "duplicate_steps": duplicates,
            "unexpected_steps": sorted(
                f"RT{key[0]}:{key[1]}" for key in unexpected),
        }
    return {
        "complete_sample_count": len(complete),
        "complete_samples": complete,
        "incomplete_sample_count": len(incomplete),
        "incomplete_samples": incomplete,
        "committed_row_count": len(rows),
        "expected_row_count": len(samples) * len(expected_keys),
    }


def build_quick_summary(out_dir):
    """Return a provisional summary without writing to ``out_dir``."""
    out_dir = os.path.abspath(out_dir)
    manifest, manifest_sha256 = _read_manifest(out_dir)
    config = manifest["config"]
    samples = config.get("samples")
    round_trips = config.get("num_round_trips")
    method_set = config.get("method_set")
    if (not isinstance(samples, list) or not samples
            or any(not isinstance(sample, str) or not sample for sample in samples)
            or len(set(samples)) != len(samples)):
        raise RuntimeError("manifest sample scope is invalid")
    if (not isinstance(round_trips, int) or isinstance(round_trips, bool)
            or round_trips < 1):
        raise RuntimeError("manifest round-trip count is invalid")
    if set(method_set or []) != set(METHODS):
        raise RuntimeError(
            "quick summary requires hybridpatch/fullrewrite paired methods")

    rows = {
        method: load_formal_rows(
            os.path.join(out_dir, method), method, samples)
        for method in METHODS
    }
    completeness = {
        method: _method_completeness(
            rows[method], samples, round_trips)
        for method in METHODS
    }
    fully_paired = sorted(
        set(completeness["hybridpatch"]["complete_samples"])
        & set(completeness["fullrewrite"]["complete_samples"])
    )

    endpoint_raw = sample_level_final_endpoint(
        rows["hybridpatch"],
        rows["fullrewrite"],
        K=round_trips,
        expected_samples=samples,
    )
    endpoint = {
        "round_trip": endpoint_raw["round_trip"],
        "unit": endpoint_raw["unit"],
        "paired_sample_count": endpoint_raw["n"],
        "mean_hybridpatch": endpoint_raw["mean_method_a"],
        "mean_fullrewrite": endpoint_raw["mean_method_b"],
        "paired_delta_hybridpatch_minus_fullrewrite": endpoint_raw[
            "paired_delta_a_minus_b"],
        "sample_delta_sd": endpoint_raw["sample_delta_sd"],
        "wins_hybridpatch": endpoint_raw["positive"],
        "wins_fullrewrite": endpoint_raw["negative"],
        "ties": endpoint_raw["ties"],
        "missing_hybridpatch_endpoint": endpoint_raw["missing_method_a"],
        "missing_fullrewrite_endpoint": endpoint_raw["missing_method_b"],
    }

    return {
        "schema": "anchorpatch.quick_experiment_summary/1",
        "result_kind": "provisional_unverified",
        "canonical": False,
        "warning": (
            "Committed-row summary only; campaign inspection, honesty replay, "
            "artifact sealing, and record finalization were not run."
        ),
        "checks_not_run": [
            "campaign_full_inspector",
            "honesty_replay",
            "artifact_seal",
            "record_finalization",
        ],
        "source": {
            "out_dir": out_dir,
            "dispatch_manifest_sha256": manifest_sha256,
            "campaign_role": config.get("campaign_role"),
            "run_git_commit": manifest.get("run_git_commit"),
            "transport_revision": config.get("transport_revision"),
            "round_trips": round_trips,
        },
        "campaign_completeness": {
            "expected_sample_count": len(samples),
            "fully_paired_complete_sample_count": len(fully_paired),
            "fully_paired_complete_samples": fully_paired,
            "complete_by_committed_rows": len(fully_paired) == len(samples),
            "methods": completeness,
        },
        "paired_final_endpoint": endpoint,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, dest="out_dir")
    args = parser.parse_args(argv)
    print(json.dumps(
        build_quick_summary(args.out_dir),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
