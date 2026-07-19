#!/usr/bin/env python3
"""Unit checks for build_unseen_confirmation_split.py."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile


SCRIPT = Path(__file__).with_name("build_unseen_confirmation_split.py")
SPEC = importlib.util.spec_from_file_location("build_unseen_confirmation_split", SCRIPT)
assert SPEC and SPEC.loader
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


def test_selection_counts() -> None:
    assert mod.selection_counts(120)[:2] == (100, 20)
    assert mod.selection_counts(119)[:2] == (80, 39)
    assert mod.selection_counts(106)[:2] == (80, 26)
    assert mod.selection_counts(99)[:2] == (79, 20)
    assert mod.selection_counts(86)[:2] == (69, 17)
    assert mod.selection_counts(85)[:2] == (68, 17)


def test_stratified_reserve_is_deterministic_partition() -> None:
    candidates = [f"s{i}" for i in range(12)]
    features = {}
    for index, sample_id in enumerate(candidates):
        features[sample_id] = {
            "sample_id": sample_id,
            "domain": f"d{index % 3}",
            "formats": [".txt" if index % 2 else ".json"],
            "task_types": ["local_edit", "bulk_homogeneous"][index % 2:index % 2 + 1],
            "file_count_bin": "1" if index < 6 else "3+",
            "doc_length_bin": f"q{index % 5 + 1}",
        }
    first, first_order, first_tie = mod.stratified_reserve(candidates, features, 4, seed=42)
    second, second_order, second_tie = mod.stratified_reserve(candidates, features, 4, seed=42)
    assert first == second
    assert first_order == second_order
    assert first_tie == second_tie
    assert len(first) == 4
    assert len(set(first)) == 4
    assert set(first) <= set(candidates)
    assert set(first_order) == set(first)
    assert len(first_order) == 4


def test_seed_controls_identical_feature_ties() -> None:
    candidates = [f"s{i}" for i in range(20)]
    features = {
        sample_id: {
            "sample_id": sample_id,
            "domain": "same",
            "formats": [".txt"],
            "task_types": ["same_task"],
            "file_count_bin": "1",
            "doc_length_bin": "q1",
        }
        for sample_id in candidates
    }
    reserve42, order42, _ = mod.stratified_reserve(candidates, features, 5, seed=42)
    reserve42_again, order42_again, _ = mod.stratified_reserve(candidates, features, 5, seed=42)
    reserve7, order7, _ = mod.stratified_reserve(candidates, features, 5, seed=7)
    assert reserve42 == reserve42_again
    assert order42 == order42_again
    assert reserve42 != reserve7
    assert order42 != order7
    assert set(order42) == set(reserve42)
    assert set(order7) == set(reserve7)


def test_semantic_operations_take_precedence_over_lexical_fallback() -> None:
    with tempfile.TemporaryDirectory() as directory:
        repo = Path(directory)
        sample_root = repo / "data" / "samples_delegate52" / "sample1"
        solution = sample_root / "basic_state"
        solution.mkdir(parents=True)
        (solution / "input.txt").write_text("x", encoding="utf-8")
        sample = {
            "sample_type": "example",
            "start_state": "basic",
            "states": [
                {
                    "state_id": "basic",
                    "solution_folder": "basic_state",
                    "context": ["input.txt"],
                    "prompts": [
                        {
                            "prompt": "move and replace all blocks",
                            "target_state": "with_ops",
                        },
                        {
                            "prompt": "create a new section",
                            "target_state": "without_ops",
                        },
                    ],
                },
                {
                    "state_id": "with_ops",
                    "semantic_operations": ["domain_specific_operation"],
                },
                {"state_id": "without_ops"},
            ],
        }
        (sample_root / "sample.json").write_text(
            json.dumps(sample), encoding="utf-8"
        )
        feature = mod.sample_feature(repo, "sample1")
        assert "domain_specific_operation" in feature["task_types"]
        assert "generative" in feature["task_types"]
        assert "global_restructure" not in feature["task_types"]
        assert "bulk_homogeneous" not in feature["task_types"]


def test_method_holdout_excludes_every_sealed_split_and_scanned_exposure() -> None:
    all_samples = {
        "clean", "stale", "python1", "dev", "val", "test", "unused", "dirty"
    }
    registry_sets = {
        "clean_candidate": {"clean", "stale", "python1"},
        "contaminated": {"dirty"},
        "unsupported_generation_domain": set(),
        "reserved_holdout_candidate": {"dev", "val", "test", "unused"},
    }
    split = {
        "dev": {"dev"},
        "val": {"val"},
        "test": {"test"},
        "unused_reserve": {"unused"},
    }
    pool, excluded, conflicts = mod.method_holdout_candidates(
        all_samples, registry_sets, split, ["stale"]
    )
    assert pool == {"clean"}
    assert excluded == all_samples - {"clean"}
    assert conflicts == {"python1", "stale"}


if __name__ == "__main__":
    test_selection_counts()
    test_stratified_reserve_is_deterministic_partition()
    test_seed_controls_identical_feature_ties()
    test_semantic_operations_take_precedence_over_lexical_fallback()
    test_method_holdout_excludes_every_sealed_split_and_scanned_exposure()
    print("PASS build_unseen_confirmation_split tests")
