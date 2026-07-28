"""Run the HP_V8 infrastructure regressions by failure domain.

This is a selection layer over the existing unittest classes.  It does not
move, rename, wrap, or duplicate any compatibility test method.
"""

import argparse
import sys
import unittest

import test_model_openai


FAST_CLASSES = (
    test_model_openai.OpenCodeTransportTests,
    test_model_openai.OpenCodeZenDeepSeekTests,
    test_model_openai.MinimaxOfficialTransportTests,
)

SYSTEM_CLASSES = (
    test_model_openai.IntegrationContractTests,
    test_model_openai.DeepSeekOpenCodeCampaignTests,
)

RECOVERY_NAME_MARKERS = (
    "atomic_replace",
    "audited_resume",
    "crash",
    "emergency_",
    "fsync_failure",
    "hard_crash",
    "interrupt",
    "operator_pause",
    "orphan",
    "parent_loss",
    "pending",
    "reconcile",
    "recovered",
    "recovery",
    "resume",
    "sharing_violation",
    "snapshot_failure",
    "watchdog",
    "write_failure",
)

TIERS = ("fast", "component", "recovery", "all")


def _is_recovery_test(method_name):
    return any(marker in method_name for marker in RECOVERY_NAME_MARKERS)


def _iter_cases(test):
    if isinstance(test, unittest.TestSuite):
        for child in test:
            yield from _iter_cases(child)
        return
    yield test


def build_tier_suite(tier):
    if tier not in TIERS:
        raise ValueError(f"unknown regression tier: {tier!r}")
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite()
    if tier in {"fast", "all"}:
        for case_class in FAST_CLASSES:
            suite.addTests(loader.loadTestsFromTestCase(case_class))
    if tier in {"component", "recovery", "all"}:
        for case_class in SYSTEM_CLASSES:
            for method_name in loader.getTestCaseNames(case_class):
                is_recovery = _is_recovery_test(method_name)
                if (tier == "all"
                        or tier == "recovery" and is_recovery
                        or tier == "component" and not is_recovery):
                    suite.addTest(case_class(method_name))
    return suite


def tier_test_ids(tier):
    return tuple(test.id() for test in _iter_cases(build_tier_suite(tier)))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", choices=TIERS, required=True)
    parser.add_argument(
        "--list", action="store_true",
        help="print selected unittest IDs without executing them",
    )
    args = parser.parse_args(argv)
    suite = build_tier_suite(args.tier)
    ids = tuple(test.id() for test in _iter_cases(suite))
    if args.list:
        for test_id in ids:
            print(test_id)
        print(f"TIER {args.tier}: {len(ids)} tests", file=sys.stderr)
        return 0
    print(f"TIER {args.tier}: {len(ids)} tests", flush=True)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
