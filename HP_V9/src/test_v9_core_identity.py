"""Behavioral identity checks for legacy and simplified V9 relay entry points."""
import sys
import unittest
from pathlib import Path
from unittest import mock


HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import experiment_runner
import relay_core


class V9CoreIdentityTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
