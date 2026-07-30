"""Behavioral identity checks for legacy and simplified V9 relay entry points."""
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import experiment_runner
import relay_core
from utils_context import stringify_context


class V9CoreIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo_root = HERE.parent.parent
        source = subprocess.run(
            ["git", "show",
             "717e470525c89366415ae9397147dcb75c9eda87:"
             "HP_V9/src/experiment_runner.py"],
            cwd=repo_root, check=True, text=True, encoding="utf-8",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
        module = types.ModuleType("hp_v9_frozen_717e470_experiment_runner")
        module.__file__ = str(HERE / "experiment_runner.py")
        exec(compile(source, module.__file__, "exec"), module.__dict__)
        cls.frozen = module

    def test_legacy_runner_exports_the_shared_scientific_helpers(self):
        self.assertIs(experiment_runner._edit_step, relay_core._edit_step)
        self.assertIs(
            experiment_runner._run_attempt_hybrid,
            relay_core._run_attempt_hybrid)
        self.assertIs(experiment_runner._evaluate, relay_core._evaluate)
        self.assertIs(experiment_runner._row, relay_core._row)
        self.assertIs(
            experiment_runner._require_formal_opencode_transport,
            relay_core.require_formal_opencode_transport)

    def test_legacy_run_relay_delegates_to_shared_run_method(self):
        sample = {"start_state": "initial", "sample_type": "dummy"}
        states = {
            "initial": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{"target_state": "target", "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"],
                "prompts": [{"target_state": "initial", "prompt": "backward"}],
            },
        }
        with mock.patch.object(
                experiment_runner, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    experiment_runner, "load_sample",
                    return_value=(sample, str(HERE), states)), \
                mock.patch.object(
                    experiment_runner, "build_relay_task_plan",
                    return_value=["target"]), \
                mock.patch.object(
                    experiment_runner, "save_relay_task_plan"), \
                mock.patch.object(
                    experiment_runner, "register_task_plan",
                    return_value={"sha256": "a" * 64}), \
                mock.patch.object(
                    experiment_runner, "build_context_from_folder",
                    return_value={"a.txt": "initial"}), \
                mock.patch.object(
                    experiment_runner, "load_distractor_context",
                    return_value={}), \
                mock.patch.object(
                    experiment_runner, "_shared_run_method",
                    return_value={"completed_round_trips": 1}) as shared, \
                mock.patch.object(experiment_runner, "RunLogger"):
            with self.subTest("same wrapper"):
                import tempfile
                with tempfile.TemporaryDirectory() as out_dir:
                    experiment_runner.run_relay(
                        "fullrewrite", "sample", num_round_trips=1,
                        include_distractor=False, out_dir=out_dir,
                        model="offline-test", max_tokens=16,
                        generate_fn=mock.Mock(), printing=False)
        shared.assert_called_once()

    def test_shared_core_matches_frozen_717e470_fullrewrite_behavior(self):
        class Domain:
            def prepare_prompt(self, context, target, instruction):
                return f"{instruction}|{sorted(context)}|{sorted(target['context'])}"

            def evaluate_context(self, sample_id, generated, target):
                return {
                    "score": 1.0 if generated == {"a.txt": "updated"} else 0.0,
                    "sample": sample_id, "target": list(target["context"]),
                }

        raw = stringify_context({"a.txt": "updated"})
        metadata = {
            "message": raw, "response_id": "provider-response",
            "api_call_ids": ["call-1"], "api_raw_paths": ["calls/x.json"],
            "call_kinds": ["fullrewrite_primary"], "total_tokens": 7,
        }
        prompts = {"frozen": [], "shared": []}

        def generate(which):
            def inner(messages, **_kwargs):
                prompts[which].append(messages)
                return dict(metadata)
            return inner

        args = (
            "fullrewrite", Domain(), "sample", "offline-model",
            {"a.txt": "old"}, {}, {"context": ["a.txt"]},
            "replace old with updated", 20000)
        frozen_step = self.frozen._edit_step(
            *args, generate("frozen"), step_direction="forward",
            reasoning_effort="high")
        shared_step = relay_core._edit_step(
            *args, generate("shared"), step_direction="forward",
            reasoning_effort="high")
        self.assertEqual(prompts["frozen"], prompts["shared"])
        self.assertEqual(frozen_step, shared_step)

        frozen_eval = self.frozen._evaluate(
            Domain(), "sample", frozen_step[1], {"context": ["a.txt"]},
            ["a.txt"])
        shared_eval = relay_core._evaluate(
            Domain(), "sample", shared_step[1], {"context": ["a.txt"]},
            ["a.txt"])
        self.assertEqual(frozen_eval, shared_eval)
        row_args = (
            "fullrewrite", "sample", "dummy", "offline-model",
            ["rid-1"], ["target"], 1, "forward", "target", "initial",
            raw, shared_eval, metadata, None, "full_rewrite", True, True, False)
        frozen_row = self.frozen._row(
            *row_args, edit_instruction="replace old with updated")
        shared_row = relay_core._row(
            *row_args, edit_instruction="replace old with updated")
        self.assertEqual(frozen_row, shared_row)
        self.assertEqual(
            {"completed_round_trips": 1, "current_context": frozen_step[1],
             "rid_chain": frozen_row["rid_chain"],
             "state_chain": frozen_row["state_chain"]},
            {"completed_round_trips": 1, "current_context": shared_step[1],
             "rid_chain": shared_row["rid_chain"],
             "state_chain": shared_row["state_chain"]})


if __name__ == "__main__":
    unittest.main()
