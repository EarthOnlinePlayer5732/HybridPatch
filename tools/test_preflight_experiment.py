from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import evaluator_runtime_preflight as evaluator
import preflight_experiment as preflight


class ExperimentPreflightTests(unittest.TestCase):
    def _fixture(self, directory: str) -> tuple[Path, Path, Path, Path, Path]:
        root = Path(directory)
        version = root / "HP_V8"
        experiment = version / "exp_fixture"
        plan = root / "docs" / "experiment_plans" / "exp_fixture.md"
        cache = root / ".cache" / "hybridpatch" / "preflight"
        (version / "src").mkdir(parents=True)
        plan.parent.mkdir(parents=True)
        plan.write_text("# exp_fixture\n", encoding="utf-8")
        for relative in (
            *preflight.REGRESSION_SCRIPTS,
            "src/paired_campaign_dispatch.py",
        ):
            path = version / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# fixture\n", encoding="utf-8")
        return root, version, experiment, plan, cache

    @staticmethod
    def _fake_run_factory(
        experiment: Path,
        samples: list[str],
        calls: list[list[str]],
    ):
        def fake_run(command: list[str], *, cwd: Path) -> dict[str, object]:
            calls.append(list(command))
            if "paired_campaign_dispatch.py" in " ".join(command):
                experiment.mkdir(parents=True, exist_ok=True)
                manifest = {
                    "schema": "fixture.dispatch/1",
                    "experiment_id": experiment.name,
                    "run_git_commit": "a" * 40,
                    "git_tree_state": "clean",
                    "config": {"samples": samples},
                }
                (experiment / "dispatch_manifest.json").write_text(
                    json.dumps(manifest), encoding="utf-8"
                )
            elif "evaluator_runtime_preflight.py" in " ".join(command):
                output = Path(command[command.index("--output") + 1])
                output.parent.mkdir(parents=True, exist_ok=True)
                report = {
                    "schema": evaluator.SCHEMA,
                    "selection": samples,
                    "input_fingerprints": {
                        "evaluator_code_sha256": "b" * 64,
                        "runtime_identity": {"python_version": "fixture"},
                        "sample_input_sha256": {
                            sample: "c" * 64 for sample in samples
                        },
                    },
                    "execution": {
                        "jobs": 4,
                        "executed_sample_ids": samples,
                        "executed_sample_count": len(samples),
                        "reused_sample_ids": [],
                        "reused_sample_count": 0,
                    },
                    "summary": {
                        "passed": True,
                        "requested": len(samples),
                        "runtime_runnable": len(samples),
                        "failed": 0,
                    },
                }
                output.write_text(json.dumps(report), encoding="utf-8")
            return {
                "command": command,
                "cwd": str(cwd),
                "exit_code": 0,
                "duration_seconds": 0.01,
                "stdout_tail": "",
                "stderr_tail": "",
            }

        return fake_run

    def test_builds_zero_api_receipt_without_key_probe_or_worker_launch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, version, experiment, plan, cache = self._fixture(directory)
            calls: list[list[str]] = []
            fake_run = self._fake_run_factory(
                experiment, ["sample1", "sample2"], calls
            )
            with (
                mock.patch.object(preflight, "ROOT", root),
                mock.patch.object(
                    preflight, "active_version_name", return_value="HP_V8"
                ),
                mock.patch.object(
                    evaluator,
                    "evaluator_code_sha256",
                    side_effect=AssertionError(
                        "regression receipt must not use evaluator cache identity"
                    ),
                ),
                mock.patch.object(
                    evaluator,
                    "runtime_identity",
                    return_value={"python_version": "fixture"},
                ),
                mock.patch.object(preflight, "run_checked", side_effect=fake_run),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                exit_code = preflight.main(
                    [
                        "--experiment",
                        str(experiment),
                        "--plan",
                        str(plan),
                        "--cache-dir",
                        str(cache),
                        "--evaluator-jobs",
                        "4",
                        "--",
                        "--campaign_role",
                        "supplemental",
                    ]
                )

            self.assertEqual(exit_code, 0)
            receipt = json.loads(
                (experiment / "preflight" / "preflight_receipt.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(receipt["schema"], preflight.SCHEMA)
            self.assertTrue(receipt["zero_api"])
            self.assertEqual(receipt["provider_posts"], 0)
            self.assertEqual(
                receipt["key_probe"]["status"],
                "required_separate_paid_step_not_run",
            )
            self.assertEqual(
                receipt["regression"]["regression_code_sha256"],
                preflight.regression_code_sha256(version),
            )
            dispatcher = next(
                command
                for command in calls
                if "paired_campaign_dispatch.py" in " ".join(command)
            )
            self.assertIn("--dry_run", dispatcher)
            self.assertNotIn("--resume", dispatcher)
            self.assertFalse(
                any("key" in " ".join(command).casefold() for command in calls)
            )

    def test_regression_receipt_is_reused_for_unchanged_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, version, _experiment, _plan, cache = self._fixture(directory)
            calls: list[list[str]] = []

            def fake_run(command: list[str], *, cwd: Path) -> dict[str, object]:
                calls.append(command)
                return {
                    "command": command,
                    "cwd": str(cwd),
                    "exit_code": 0,
                    "duration_seconds": 0.01,
                    "stdout_tail": "",
                    "stderr_tail": "",
                }

            cache_path = cache / "HP_V8_regression.json"
            with (
                mock.patch.object(preflight, "ROOT", root),
                mock.patch.object(
                    evaluator,
                    "evaluator_code_sha256",
                    side_effect=AssertionError(
                        "regression receipt must not use evaluator cache identity"
                    ),
                ),
                mock.patch.object(
                    evaluator,
                    "runtime_identity",
                    return_value={"python_version": "fixture"},
                ),
                mock.patch.object(preflight, "run_checked", side_effect=fake_run),
            ):
                first, first_cached = preflight.ensure_regression_receipt(
                    version, cache_path
                )
                second, second_cached = preflight.ensure_regression_receipt(
                    version, cache_path
                )

            self.assertFalse(first_cached)
            self.assertTrue(second_cached)
            self.assertEqual(first, second)
            self.assertEqual(len(calls), len(preflight.REGRESSION_SCRIPTS))

    def test_regression_case_file_change_invalidates_cache_without_new_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, version, _experiment, _plan, cache = self._fixture(directory)
            case_file = (
                version
                / "src"
                / "model_openai_test_cases"
                / "test_case_fixture.py"
            )
            case_file.parent.mkdir(parents=True)
            case_file.write_text("CASE_VERSION = 1\n", encoding="utf-8")
            calls: list[list[str]] = []

            def fake_run(command: list[str], *, cwd: Path) -> dict[str, object]:
                calls.append(command)
                return {
                    "command": command,
                    "cwd": str(cwd),
                    "exit_code": 0,
                    "duration_seconds": 0.01,
                    "stdout_tail": "",
                    "stderr_tail": "",
                }

            cache_path = cache / "HP_V8_regression.json"
            with (
                mock.patch.object(preflight, "ROOT", root),
                mock.patch.object(
                    evaluator,
                    "runtime_identity",
                    return_value={"python_version": "fixture"},
                ),
                mock.patch.object(preflight, "run_checked", side_effect=fake_run),
            ):
                first, first_cached = preflight.ensure_regression_receipt(
                    version, cache_path
                )
                second, second_cached = preflight.ensure_regression_receipt(
                    version, cache_path
                )
                case_file.write_text("CASE_VERSION = 2\n", encoding="utf-8")
                third, third_cached = preflight.ensure_regression_receipt(
                    version, cache_path
                )

            self.assertFalse(first_cached)
            self.assertTrue(second_cached)
            self.assertFalse(third_cached)
            self.assertNotEqual(
                first["regression_code_sha256"],
                third["regression_code_sha256"],
            )
            self.assertEqual(len(calls), 2 * len(preflight.REGRESSION_SCRIPTS))
            self.assertEqual(
                [
                    command
                    for command in calls
                    if command[-1] == "./src/test_model_openai.py"
                ],
                [
                    [mock.ANY, "-B", "./src/test_model_openai.py"],
                    [mock.ANY, "-B", "./src/test_model_openai.py"],
                ],
            )
            self.assertFalse(
                any("model_openai_test_cases" in " ".join(command) for command in calls)
            )

    def test_regression_code_sha256_covers_future_tests_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _root, version, _experiment, _plan, _cache = self._fixture(directory)
            future_fixture = version / "tests" / "fixtures" / "future_case.json"
            future_fixture.parent.mkdir(parents=True)
            before = preflight.regression_code_sha256(version)
            future_fixture.write_text('{"version": 1}\n', encoding="utf-8")
            after = preflight.regression_code_sha256(version)
            self.assertNotEqual(before, after)

    def test_regression_code_sha256_ignores_python_cache_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _root, version, _experiment, _plan, _cache = self._fixture(directory)
            before = preflight.regression_code_sha256(version)
            cache_file = version / "tests" / "__pycache__" / "case.cpython-311.pyc"
            cache_file.parent.mkdir(parents=True)
            cache_file.write_bytes(b"generated bytecode")
            after = preflight.regression_code_sha256(version)
            self.assertEqual(before, after)

    def test_regression_code_sha256_covers_version_test_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _root, version, _experiment, _plan, _cache = self._fixture(directory)
            fixture = (
                version
                / "src"
                / "test_fixtures"
                / "opencode_transport_v2_anomalies.json"
            )
            fixture.parent.mkdir(parents=True)
            fixture.write_text('{"version": 1}\n', encoding="utf-8")
            before = preflight.regression_code_sha256(version)
            fixture.write_text('{"version": 2}\n', encoding="utf-8")
            after = preflight.regression_code_sha256(version)
            self.assertNotEqual(before, after)

    def test_rejects_managed_dispatcher_arguments(self) -> None:
        for argument in ("--dry_run", "--out_dir=elsewhere"):
            with self.subTest(argument=argument):
                with self.assertRaises(SystemExit):
                    with contextlib.redirect_stderr(io.StringIO()):
                        preflight.parse_args(
                            [
                                "--experiment",
                                "HP_V8/exp_fixture",
                                "--plan",
                                "docs/experiment_plans/exp_fixture.md",
                                "--",
                                argument,
                            ]
                        )


if __name__ == "__main__":
    unittest.main()
