"""Tests for the non-invasive HP_V8 regression tier selector."""

import contextlib
import io
import unittest

import run_regression_tier


class RegressionTierTests(unittest.TestCase):
    def test_tiers_are_disjoint_complete_and_keep_original_ids(self):
        fast = set(run_regression_tier.tier_test_ids("fast"))
        component = set(run_regression_tier.tier_test_ids("component"))
        recovery = set(run_regression_tier.tier_test_ids("recovery"))
        all_ids = run_regression_tier.tier_test_ids("all")

        self.assertTrue(fast)
        self.assertTrue(component)
        self.assertTrue(recovery)
        self.assertFalse(fast & component)
        self.assertFalse(fast & recovery)
        self.assertFalse(component & recovery)
        self.assertEqual(fast | component | recovery, set(all_ids))
        self.assertEqual(len(all_ids), len(set(all_ids)))
        self.assertEqual(len(all_ids), 251)
        self.assertTrue(all(
            test_id.startswith("test_model_openai.") for test_id in all_ids))

    def test_representative_failure_domains_are_stable(self):
        tiers = {
            name: set(run_regression_tier.tier_test_ids(name))
            for name in ("fast", "component", "recovery")
        }
        self.assertIn(
            "test_model_openai.OpenCodeZenDeepSeekTests."
            "test_usage_without_finish_is_incomplete",
            tiers["fast"],
        )
        self.assertIn(
            "test_model_openai.IntegrationContractTests."
            "test_runtime_guard_never_calls_git_or_reads_task_plan",
            tiers["component"],
        )
        self.assertIn(
            "test_model_openai.DeepSeekOpenCodeCampaignTests."
            "test_dispatcher_parent_loss_recovery_retries_transaction_once",
            tiers["recovery"],
        )

    def test_list_mode_does_not_execute_tests(self):
        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(
                run_regression_tier.main(["--tier", "fast", "--list"]), 0)


if __name__ == "__main__":
    unittest.main()
