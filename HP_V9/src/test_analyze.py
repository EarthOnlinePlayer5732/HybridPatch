import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

import analyze


def _backward(sample_id, round_trip, score, method="hybridpatch", error=None):
    evaluation = {"score": score} if score is not None else {}
    if error is not None:
        evaluation["error"] = error
    return {
        "sample_id": sample_id,
        "round_trip_num": round_trip,
        "round_trip_direction": "backward",
        "method": method,
        "model_name": "offline-test",
        "distractor_included": True,
        "evaluation": evaluation,
    }


def _write_quiescent_run_metadata(out_dir):
    with open(
            os.path.join(out_dir, "run_metadata.jsonl"),
            "w", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "schema": "anchorpatch.run_metadata/3",
            "status": "finished",
        }) + "\n")


class SampleLevelFinalEndpointTests(unittest.TestCase):
    def test_exact_final_rt_one_pair_per_sample(self):
        hybrid = [
            _backward("s1", 9, 0.99),
            _backward("s1", 10, 0.8),
            _backward("s2", 10, 0.6),
            _backward("s3", 9, 0.5),
            _backward("s4", 10, None, error="reconstruction_failed"),
        ]
        rewrite = [
            _backward("s1", 10, 0.5, "fullrewrite"),
            _backward("s2", 10, 0.7, "fullrewrite"),
            _backward("s3", 10, 0.4, "fullrewrite"),
            _backward("s4", 10, 0.2, "fullrewrite"),
        ]

        endpoint = analyze.sample_level_final_endpoint(hybrid, rewrite, K=10)

        self.assertEqual(endpoint["n"], 3)
        self.assertEqual([p["sample_id"] for p in endpoint["pairs"]], ["s1", "s2", "s4"])
        self.assertAlmostEqual(endpoint["mean_method_a"], (0.8 + 0.6 + 0.0) / 3)
        self.assertAlmostEqual(endpoint["mean_method_b"], (0.5 + 0.7 + 0.2) / 3)
        self.assertAlmostEqual(endpoint["paired_delta_a_minus_b"], 0.0)
        self.assertAlmostEqual(endpoint["sample_delta_sd"], 0.2645751311064591)
        self.assertEqual(
            (endpoint["positive"], endpoint["negative"], endpoint["ties"]),
            (1, 2, 0),
        )
        self.assertEqual(endpoint["missing_method_a"], ["s3"])
        self.assertEqual(endpoint["missing_method_b"], [])

    def test_cli_report_separates_canonical_endpoint_from_trajectory(self):
        with tempfile.TemporaryDirectory() as out_dir:
            for method, score in (("hybridpatch", 0.8), ("fullrewrite", 0.5)):
                method_dir = os.path.join(out_dir, method)
                os.makedirs(method_dir)
                rows = [
                    _backward("s1", 1, score - 0.1, method),
                    _backward("s1", 2, score, method),
                ]
                with open(os.path.join(method_dir, "s1.jsonl"), "w", encoding="utf-8") as fh:
                    for row in rows:
                        fh.write(json.dumps(row) + "\n")
            with open(
                os.path.join(out_dir, "dispatch_manifest.json"),
                "w", encoding="utf-8",
            ) as fh:
                json.dump({
                    "schema": "anchorpatch.paired_campaign_manifest/1",
                    "config": {
                        "samples": ["s1"],
                        "method_set": ["hybridpatch", "fullrewrite"],
                        "num_round_trips": 2,
                    },
                }, fh)
            _write_quiescent_run_metadata(out_dir)

            old_argv = sys.argv
            sys.argv = ["analyze.py", "--dir", out_dir, "--K", "2"]
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    analyze.main()
            finally:
                sys.argv = old_argv

            report_path = os.path.join(out_dir, "analysis", "comparison.md")
            with open(report_path, encoding="utf-8") as fh:
                report = fh.read()
            self.assertIn("Sample-level final endpoint (canonical paired estimator", report)
            self.assertIn("matched samples | mean HybridPatch", report)
            self.assertIn("Backward trajectory points (descriptive only; noncanonical)", report)
            self.assertIn("| method | input | cache-read | cache-create | output | provider total | USD |", report)
            self.assertIn("pre-registered sample scope", report)
            self.assertNotIn("accounting", report)
            self.assertNotIn("| p |", report)

            endpoint_path = os.path.join(
                out_dir, "analysis", "sample_level_final_endpoint.json"
            )
            with open(endpoint_path, encoding="utf-8") as fh:
                endpoint = json.load(fh)
            self.assertEqual(endpoint["round_trip"], 2)
            self.assertEqual(endpoint["n"], 1)
            self.assertEqual(endpoint["expected_n"], 1)
            self.assertTrue(endpoint["complete"])
            self.assertAlmostEqual(endpoint["paired_delta_a_minus_b"], 0.3)
            self.assertIsNone(endpoint["sample_delta_sd"])

    def test_duplicate_exact_endpoint_is_rejected(self):
        hybrid = [
            _backward("s1", 10, 0.8),
            _backward("s1", 10, 0.7),
        ]
        rewrite = [_backward("s1", 10, 0.5, "fullrewrite")]

        with self.assertRaisesRegex(ValueError, "duplicate backward RT10 endpoint"):
            analyze.sample_level_final_endpoint(hybrid, rewrite, K=10)

    def test_formal_manifest_rejects_incomplete_exact_endpoint(self):
        with tempfile.TemporaryDirectory() as out_dir:
            for method, score in (("hybridpatch", 0.8), ("fullrewrite", 0.5)):
                method_dir = os.path.join(out_dir, method)
                os.makedirs(method_dir)
                with open(
                    os.path.join(method_dir, "s1.jsonl"),
                    "w", encoding="utf-8",
                ) as fh:
                    fh.write(json.dumps(
                        _backward("s1", 2, score, method)
                    ) + "\n")
            with open(
                os.path.join(out_dir, "dispatch_manifest.json"),
                "w", encoding="utf-8",
            ) as fh:
                json.dump({
                    "schema": "anchorpatch.paired_campaign_manifest/1",
                    "config": {
                        "samples": ["s1", "s2"],
                        "method_set": ["hybridpatch", "fullrewrite"],
                        "num_round_trips": 2,
                    },
                }, fh)
            _write_quiescent_run_metadata(out_dir)
            old_argv = sys.argv
            sys.argv = ["analyze.py", "--dir", out_dir, "--K", "2"]
            try:
                with self.assertRaisesRegex(
                        RuntimeError, "canonical endpoint incomplete"):
                    with contextlib.redirect_stdout(io.StringIO()):
                        analyze.main()
            finally:
                sys.argv = old_argv
            with open(
                os.path.join(
                    out_dir, "analysis", "sample_level_final_endpoint.json"),
                encoding="utf-8",
            ) as fh:
                endpoint = json.load(fh)
            self.assertEqual(endpoint["expected_n"], 2)
            self.assertEqual(endpoint["n"], 1)
            self.assertFalse(endpoint["complete"])
            self.assertEqual(endpoint["missing_method_a"], ["s2"])
            self.assertEqual(endpoint["missing_method_b"], ["s2"])

    def test_formal_analysis_rejects_invalid_scores_and_arm_identity(self):
        def prepare(out_dir, hybrid_evaluation):
            with open(
                os.path.join(out_dir, "dispatch_manifest.json"),
                "w", encoding="utf-8",
            ) as handle:
                json.dump({
                    "schema": "anchorpatch.paired_campaign_manifest/1",
                    "config": {
                        "samples": ["s1"],
                        "method_set": ["hybridpatch", "fullrewrite"],
                        "num_round_trips": 1,
                    },
                }, handle)
            _write_quiescent_run_metadata(out_dir)
            for method, evaluation in (
                    ("hybridpatch", hybrid_evaluation),
                    ("fullrewrite", {"score": 0.5})):
                method_dir = os.path.join(out_dir, method)
                os.makedirs(method_dir)
                row = _backward("s1", 1, 0.5, method)
                row["evaluation"] = evaluation
                with open(
                    os.path.join(method_dir, "s1.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    handle.write(json.dumps(row) + "\n")

        for invalid in (float("nan"), float("inf"), True, "0.5"):
            with self.subTest(score=invalid), \
                    tempfile.TemporaryDirectory() as out_dir:
                prepare(out_dir, {"score": invalid})
                old_argv = sys.argv
                sys.argv = ["analyze.py", "--dir", out_dir, "--K", "1"]
                try:
                    with self.assertRaisesRegex(
                            RuntimeError, "score is not finite numeric"):
                        analyze.main()
                finally:
                    sys.argv = old_argv

        with tempfile.TemporaryDirectory() as out_dir:
            prepare(out_dir, {"error": ""})
            old_argv = sys.argv
            sys.argv = ["analyze.py", "--dir", out_dir, "--K", "1"]
            try:
                with self.assertRaisesRegex(
                        RuntimeError, "finite score or non-empty error"):
                    analyze.main()
            finally:
                sys.argv = old_argv

        with tempfile.TemporaryDirectory() as out_dir:
            prepare(out_dir, {"score": 0.5})
            path = os.path.join(out_dir, "hybridpatch", "s1.jsonl")
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            row["method"] = "fullrewrite"
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(row) + "\n")
            old_argv = sys.argv
            sys.argv = ["analyze.py", "--dir", out_dir, "--K", "1"]
            try:
                with self.assertRaisesRegex(
                        RuntimeError, "identity mismatch"):
                    analyze.main()
            finally:
                sys.argv = old_argv

        with tempfile.TemporaryDirectory() as out_dir:
            prepare(out_dir, {"score": 0.5})
            legacy_dir = os.path.join(out_dir, "anchorpatch")
            os.makedirs(legacy_dir)
            with open(
                os.path.join(legacy_dir, "s1.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write("{}\n")
            old_argv = sys.argv
            sys.argv = ["analyze.py", "--dir", out_dir, "--K", "1"]
            try:
                with self.assertRaisesRegex(
                        RuntimeError, "legacy anchorpatch arm"):
                    analyze.main()
            finally:
                sys.argv = old_argv

    def test_formal_manifest_requires_quiescent_run_metadata(self):
        with tempfile.TemporaryDirectory() as out_dir:
            with open(
                    os.path.join(out_dir, "dispatch_manifest.json"),
                    "w", encoding="utf-8") as handle:
                json.dump({
                    "schema": "anchorpatch.paired_campaign_manifest/1",
                    "config": {
                        "samples": ["s1"],
                        "method_set": ["hybridpatch", "fullrewrite"],
                        "num_round_trips": 1,
                    },
                }, handle)
            old_argv = sys.argv
            sys.argv = ["analyze.py", "--dir", out_dir, "--K", "1"]
            try:
                with self.assertRaisesRegex(
                        RuntimeError,
                        "formal campaign run metadata is missing"):
                    analyze.main()
            finally:
                sys.argv = old_argv


if __name__ == "__main__":
    unittest.main()
