"""Tests for the read-only HP_V9 provisional result summary."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from quick_experiment_summary import build_quick_summary


def _row(sample, method, round_trip, direction, score):
    return {
        "sample_id": sample,
        "method": method,
        "round_trip_num": round_trip,
        "round_trip_direction": direction,
        "evaluation": {"score": score},
    }


class QuickExperimentSummaryTests(unittest.TestCase):
    def test_completeness_is_separate_from_exact_final_endpoint(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest = {
                "schema": "anchorpatch.paired_dispatch/1",
                "run_git_commit": "1" * 40,
                "config": {
                    "campaign_role": "deepseek_full234",
                    "samples": ["sample-a", "sample-b"],
                    "method_set": ["hybridpatch", "fullrewrite"],
                    "num_round_trips": 2,
                    "transport_revision": "opencode_openai_compatible/6",
                },
            }
            with open(os.path.join(
                    out_dir, "dispatch_manifest.json"), "w",
                    encoding="utf-8") as handle:
                json.dump(manifest, handle)

            for method in ("hybridpatch", "fullrewrite"):
                os.makedirs(os.path.join(out_dir, method))
                for sample in ("sample-a", "sample-b"):
                    rows = [
                        _row(sample, method, 1, "forward", 0.1),
                        _row(sample, method, 1, "backward", 0.2),
                        _row(sample, method, 2, "forward", 0.3),
                        _row(
                            sample, method, 2, "backward",
                            0.8 if method == "hybridpatch" else 0.5),
                    ]
                    if method == "fullrewrite" and sample == "sample-b":
                        rows = [
                            row for row in rows
                            if not (
                                row["round_trip_num"] == 2
                                and row["round_trip_direction"] == "forward")
                        ]
                    with open(os.path.join(
                            out_dir, method, f"{sample}.jsonl"), "w",
                            encoding="utf-8") as handle:
                        for row in rows:
                            handle.write(json.dumps(row) + "\n")

            before = {
                os.path.join(root, filename)
                for root, _dirs, files in os.walk(out_dir)
                for filename in files
            }
            summary = build_quick_summary(out_dir)
            after = {
                os.path.join(root, filename)
                for root, _dirs, files in os.walk(out_dir)
                for filename in files
            }

            self.assertEqual(before, after)
            self.assertEqual(summary["result_kind"], "provisional_unverified")
            self.assertFalse(summary["canonical"])
            completeness = summary["campaign_completeness"]
            self.assertFalse(completeness["complete_by_committed_rows"])
            self.assertEqual(
                completeness["fully_paired_complete_sample_count"], 1)
            self.assertEqual(
                completeness["methods"]["hybridpatch"][
                    "complete_sample_count"], 2)
            self.assertEqual(
                completeness["methods"]["fullrewrite"][
                    "complete_sample_count"], 1)
            endpoint = summary["paired_final_endpoint"]
            self.assertEqual(endpoint["paired_sample_count"], 2)
            self.assertAlmostEqual(
                endpoint["paired_delta_hybridpatch_minus_fullrewrite"], 0.3)


if __name__ == "__main__":
    unittest.main()
