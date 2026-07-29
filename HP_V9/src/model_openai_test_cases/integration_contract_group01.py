"""Mixin slice for test_model_openai.IntegrationContractGroup01Mixin."""

from .support import *


class IntegrationContractGroup01Mixin:
    @staticmethod
    def _incremental_exit_audit_manifest(samples, *, revision=None,
                                         metadata_storage=None):
        return {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": "1" * 40,
            "config": {
                "campaign_role": "deepseek_full234",
                "samples": list(samples),
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION
                    if revision is None else revision
                ),
                "run_metadata_storage": (
                    run_meta.RUN_METADATA_STORAGE_EVENT_V1
                    if metadata_storage is None else metadata_storage
                ),
            },
        }

    def _exercise_worker_exit_audit_queue(
            self, manifest, *, sample_count=1, slots_per_key=None,
            delta_inspection=None, full_inspection=None,
            full_interval=None, expected_error=None):
        class FakeProcess:
            returncode = None

            def __init__(self, pid):
                self.pid = pid

            def poll(self):
                self.returncode = 0
                return self.returncode

        samples = [f"sample-{index:03d}" for index in range(sample_count)]
        assignments = [
            {
                "sample": sample,
                "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
            }
            for sample in samples
        ]
        launched = []

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            for index, item in enumerate(batch, len(launched) + 1):
                sample = item["sample"]
                launched.append(sample)
                running[sample] = {
                    **item,
                    "process": FakeProcess(1000 + index),
                    "log": mock.Mock(),
                    "worker_launch_id": f"worker-{sample}",
                    "exit_recorded": False,
                }
            return [item["sample"] for item in batch]

        def fake_exit(_out_dir, running, sample, _item, returncode,
                      _dispatch_log):
            self.assertEqual(returncode, 0)
            del running[sample]
            return "finished"

        delta_inspection = dict(delta_inspection or {
            "errors": [], "api_calls": sample_count,
            "preservation_violations": 0,
        })
        full_inspection = dict(full_inspection or {
            "errors": [], "api_calls": sample_count,
            "preservation_violations": 0,
        })

        def fake_delta(_out_dir, _manifest, state, **_kwargs):
            return dict(delta_inspection), dict(state)

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        args._deepseek_exit_audit_preflight_manifest_sha256 = (
            paired_dispatch._canonical_record_sha256(manifest))
        initial_state = {"terminal_since_full": 0}
        interval_patch = (
            mock.patch.object(
                paired_dispatch, "DEEPSEEK_EXIT_FULL_AUDIT_INTERVAL",
                full_interval)
            if full_interval is not None else contextlib.nullcontext()
        )
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "_record_worker_exit",
                    side_effect=fake_exit), \
                mock.patch.object(
                    paired_dispatch, "_initialize_deepseek_exit_audit_state",
                    return_value=initial_state) as initialize, \
                mock.patch.object(
                    paired_dispatch, "_audit_deepseek_exit_delta",
                    side_effect=fake_delta) as delta_audit, \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value=full_inspection) as full_audit, \
                mock.patch.object(
                    paired_dispatch, "_publish_active_worker_set"), \
                mock.patch.object(
                    paired_dispatch, "_raise_if_operator_pause_requested"), \
                mock.patch.object(paired_dispatch.time, "sleep"), \
                interval_patch:
            call = lambda: paired_dispatch._run_worker_queue(
                args, out_dir, manifest, {}, {}, assignments, {},
                os.path.join(out_dir, "dispatch_log.jsonl"), {},
                set(), set(), set(), len(assignments),
                slots_per_key or sample_count,
            )
            if expected_error is None:
                call()
            else:
                with self.assertRaisesRegex(RuntimeError, expected_error):
                    call()
            dispatch_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))

        return {
            "launched": launched,
            "initialize_calls": initialize.call_count,
            "delta_calls": delta_audit.call_count,
            "delta_call_args": list(delta_audit.call_args_list),
            "full_calls": full_audit.call_count,
            "full_call_args": list(full_audit.call_args_list),
            "dispatch_rows": dispatch_rows,
        }

    def test_deepseek_exit_audit_cursor_accepts_append_and_rejects_drift(self):
        empty_cursor = {
            "byte_count": 0,
            "row_count": 0,
            "prefix_sha256": hashlib.sha256(b"").hexdigest(),
        }
        with tempfile.TemporaryDirectory() as out_dir:
            path = pathlib.Path(out_dir, "ledger.jsonl")
            path.write_bytes(b'{"row":1}\n')
            cursor, rows = paired_dispatch._advance_append_only_ledger_cursor(
                str(path), empty_cursor)
            self.assertEqual(rows, [{"row": 1}])
            path.write_bytes(path.read_bytes() + b'{"row":2}\n')
            advanced, rows = paired_dispatch._advance_append_only_ledger_cursor(
                str(path), cursor)
            self.assertEqual(rows, [{"row": 2}])
            self.assertEqual(advanced["row_count"], 2)

        with tempfile.TemporaryDirectory() as out_dir:
            path = pathlib.Path(out_dir, "ledger.jsonl")
            path.write_bytes(b'{"row":1}\n')
            cursor, _ = paired_dispatch._advance_append_only_ledger_cursor(
                str(path), empty_cursor)
            path.write_bytes(b"")
            with self.assertRaisesRegex(RuntimeError, "was truncated"):
                paired_dispatch._advance_append_only_ledger_cursor(
                    str(path), cursor)

        with tempfile.TemporaryDirectory() as out_dir:
            path = pathlib.Path(out_dir, "ledger.jsonl")
            path.write_bytes(b'{"row":1}\n')
            cursor, _ = paired_dispatch._advance_append_only_ledger_cursor(
                str(path), empty_cursor)
            path.write_bytes(b'{"row":2}\n')
            with self.assertRaisesRegex(RuntimeError, "prefix drifted"):
                paired_dispatch._advance_append_only_ledger_cursor(
                    str(path), cursor)

    def test_deepseek_exit_audit_cursor_waits_for_locked_append(self):
        empty_cursor = {
            "byte_count": 0,
            "row_count": 0,
            "prefix_sha256": hashlib.sha256(b"").hexdigest(),
        }
        with tempfile.TemporaryDirectory() as out_dir:
            path = pathlib.Path(out_dir, "ledger.jsonl")
            observed = []
            failures = []
            with open(path, "a+b") as writer:
                portalocker.lock(writer, portalocker.LOCK_EX)
                writer.write(b'{"row":')
                writer.flush()

                def read_cursor():
                    try:
                        observed.append(
                            paired_dispatch._advance_append_only_ledger_cursor(
                                str(path), empty_cursor))
                    except BaseException as exc:
                        failures.append(exc)

                reader = threading.Thread(target=read_cursor, daemon=True)
                reader.start()
                threading.Event().wait(0.05)
                self.assertTrue(reader.is_alive())
                writer.write(b'1}\n')
                writer.flush()
                os.fsync(writer.fileno())
                portalocker.unlock(writer)
                reader.join(5)

            self.assertFalse(reader.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(observed[0][1], [{"row": 1}])

    def test_deepseek_exit_audit_fast_path_skips_full_scan(self):
        manifest = self._incremental_exit_audit_manifest(["sample-000"])
        result = self._exercise_worker_exit_audit_queue(manifest)
        self.assertEqual(result["launched"], ["sample-000"])
        self.assertEqual(result["initialize_calls"], 1)
        self.assertEqual(result["delta_calls"], 1)
        self.assertEqual(result["full_calls"], 0)
        self.assertEqual(
            result["delta_call_args"][0].kwargs["exited_samples"],
            {"sample-000"})

    def test_deepseek_exit_audit_twentieth_terminal_triggers_full_scan(self):
        samples = [f"sample-{index:03d}" for index in range(20)]
        manifest = self._incremental_exit_audit_manifest(samples)
        result = self._exercise_worker_exit_audit_queue(
            manifest, sample_count=20, slots_per_key=20)
        self.assertEqual(result["initialize_calls"], 1)
        self.assertEqual(result["delta_calls"], 1)
        self.assertEqual(result["full_calls"], 1)
        self.assertEqual(
            result["full_call_args"][0].kwargs["required_complete_samples"],
            set(samples))

    def test_deepseek_exit_audit_legacy_transport_falls_back_to_full_scan(self):
        manifest = self._incremental_exit_audit_manifest(
            ["sample-000"], revision="opencode_openai_compatible/5")
        result = self._exercise_worker_exit_audit_queue(manifest)
        self.assertEqual(result["initialize_calls"], 0)
        self.assertEqual(result["delta_calls"], 0)
        self.assertEqual(result["full_calls"], 1)
        self.assertEqual(
            result["full_call_args"][0].kwargs[
                "required_complete_samples"],
            {"sample-000"})

    def test_deepseek_exit_local_audit_failure_does_not_release_or_refill(self):
        manifest = self._incremental_exit_audit_manifest(
            ["sample-000", "sample-001"])
        result = self._exercise_worker_exit_audit_queue(
            manifest, sample_count=2, slots_per_key=1,
            delta_inspection={
                "errors": ["local exit audit failed"], "api_calls": 1,
                "preservation_violations": 0,
            },
            expected_error="local exit audit failed",
        )
        self.assertEqual(result["launched"], ["sample-000"])
        self.assertEqual(result["delta_calls"], 1)
        self.assertEqual(result["full_calls"], 0)
        self.assertFalse(any(
            row.get("event") == "queue_slots_released"
            for row in result["dispatch_rows"]))

    def test_deepseek_due_full_audit_failure_does_not_release_or_refill(self):
        manifest = self._incremental_exit_audit_manifest(
            ["sample-000", "sample-001"])
        result = self._exercise_worker_exit_audit_queue(
            manifest, sample_count=2, slots_per_key=1, full_interval=1,
            full_inspection={
                "errors": ["periodic full audit failed"], "api_calls": 1,
                "preservation_violations": 0,
            },
            expected_error="periodic full audit failed",
        )
        self.assertEqual(result["launched"], ["sample-000"])
        self.assertEqual(result["delta_calls"], 1)
        self.assertEqual(result["full_calls"], 1)
        self.assertFalse(any(
            row.get("event") == "queue_slots_released"
            for row in result["dispatch_rows"]))

    def test_deepseek_exit_delta_audits_new_api_sample_from_active_worker(self):
        manifest = self._incremental_exit_audit_manifest(
            ["sample-exited", "sample-active"])
        with tempfile.TemporaryDirectory() as out_dir:
            state = paired_dispatch._initialize_deepseek_exit_audit_state(
                out_dir)
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), {
                    "request_id": "request-from-active-worker",
                    "sample": "sample-active",
                })
            with mock.patch.object(
                    paired_dispatch, "_inspect_deepseek_key_failover_audit",
                    return_value=[]), mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={
                            "errors": [], "api_calls": 1,
                            "preservation_violations": 0,
                        }) as inspect:
                inspection, next_state = (
                    paired_dispatch._audit_deepseek_exit_delta(
                        out_dir, manifest, state,
                        exited_samples={"sample-exited"},
                        active_samples={"sample-active"},
                        required_complete_samples={"sample-exited"},
                    ))

        self.assertEqual(inspection["errors"], [])
        self.assertEqual(inspection["api_calls"], 1)
        self.assertIsNotNone(next_state)
        self.assertEqual(
            inspect.call_args.kwargs["audit_samples"],
            {"sample-exited", "sample-active"})
        self.assertEqual(
            inspect.call_args.kwargs["terminal_audit_samples"],
            {"sample-exited"})

    def test_transport_exhaustion_isolates_one_worker_and_sibling_completes(self):
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_infrastructure_fixture(out_dir)
            failed_process = mock.Mock(pid=101)
            failed_process.poll.return_value = 7
            sibling_process = mock.Mock(pid=202)
            sibling_process.poll.return_value = None
            running = {
                "sample-a": {
                    "sample": "sample-a", "process": failed_process,
                    "log": mock.Mock(), "key_label": "KEY_01",
                    "worker_launch_id": "worker-a",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "target_round_trips": 1, "exit_recorded": False,
                },
                "sample-b": {
                    "sample": "sample-b", "process": sibling_process,
                    "log": mock.Mock(), "key_label": "KEY_02",
                    "worker_launch_id": "worker-b",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "target_round_trips": 1, "exit_recorded": False,
                },
            }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, "sample-a", running["sample-a"], 7,
                dispatch_log)
            self.assertEqual(disposition, "infrastructure_incomplete")
            self.assertNotIn("sample-a", running)
            self.assertIn("sample-b", running)
            self.assertIsNone(sibling_process.poll())

            sibling_process.poll.return_value = 0
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, "sample-b", running["sample-b"], 0,
                dispatch_log)
            self.assertEqual(disposition, "finished")
            self.assertEqual(running, {})
            exit_rows = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(
                [row["disposition"] for row in exit_rows],
                ["infrastructure_incomplete", "finished"])
            self.assertEqual(
                paired_dispatch.read_campaign_stop_conditions(out_dir), [])

    def test_evaluator_exception_is_sample_local_and_resume_never_reposts(self):
        class BrokenDomain:
            @staticmethod
            def evaluate_context(*_args):
                raise ValueError("broken sample evaluator")

        with self.assertRaises(
                experiment_runner.EvaluatorIncompleteError) as caught:
            experiment_runner._evaluate(
                BrokenDomain(), "sample-eval", {"a.txt": "generated"},
                {"context": ["a.txt"]}, ["a.txt"],
                method="hybridpatch", rt_index=1, direction="backward",
                target_state_id="basic_state",
            )
        self.assertEqual(caught.exception.error_type, "builtins.ValueError")

        sample = "sample-eval"
        worker_id = "worker-eval"
        invocation_id = "invocation-eval"
        methods = ["hybridpatch", "fullrewrite"]
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {"ANCHORPATCH_WORKER_LAUNCH_ID": worker_id},
                clear=False):
            sidecar = experiment_runner._record_evaluator_incomplete(
                out_dir, sample, methods, 1, invocation_id,
                caught.exception,
            )
            self.assertFalse(sidecar["result_committed_for_failed_step"])
            self.assertFalse(sidecar["score_imputed"])
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "run_metadata.jsonl"), {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": invocation_id,
                    "worker_launch_id": worker_id,
                    "worker_pid": os.getpid(),
                    "samples": [sample],
                    "status": "evaluator_incomplete",
                })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), {
                    "schema": "anchorpatch.api_call/4",
                    "sample": sample,
                    "method": "hybridpatch",
                    "rt_index": 1,
                    "direction": "backward",
                    "call_kind": "hybridpatch_primary",
                    "worker_launch_id": "prior-recovery-worker",
                    "classification": "provider/API failure",
                })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), {
                    "schema": "anchorpatch.api_call/4",
                    "sample": sample,
                    "method": "hybridpatch",
                    "rt_index": 1,
                    "direction": "backward",
                    "call_kind": "hybridpatch_primary",
                    "worker_launch_id": worker_id,
                    "classification": "success",
                })
            process = mock.Mock(pid=os.getpid())
            running = {
                sample: {
                    "sample": sample,
                    "process": process,
                    "log": mock.Mock(),
                    "key_label": "KEY_01",
                    "worker_launch_id": worker_id,
                    "methods": methods,
                    "target_round_trips": 1,
                    "exit_recorded": False,
                },
            }
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, sample, running[sample], 7,
                os.path.join(out_dir, "dispatch_log.jsonl"),
            )
            self.assertEqual(disposition, "evaluator_incomplete")
            self.assertEqual(running, {})

            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir,
                    [{"sample": sample, "methods": methods}],
                    resume=True, target_round_trips=1,
                ))
            self.assertEqual(selected, [])
            self.assertEqual(authorizations, {})
            provider_post = mock.Mock(
                side_effect=AssertionError(
                    "evaluator-incomplete sample must not be reposted"))
            for _item in selected:
                provider_post()
            provider_post.assert_not_called()
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "hybridpatch", f"{sample}.jsonl")))
            self.assertEqual(
                paired_dispatch.read_campaign_stop_conditions(out_dir), [])

    def test_runner_main_records_evaluator_terminal_status(self):
        failure = experiment_runner.EvaluatorIncompleteError(
            "sample", "hybridpatch", 1, "forward", "target",
            ValueError("broken evaluator"),
        )
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    sys, "argv", [
                        "experiment_runner.py", "--sample", "sample",
                        "--methods", "hybridpatch", "fullrewrite",
                        "--num_round_trips", "1", "--out_dir", out_dir,
                        "--model", "offline-test-model",
                    ]), \
                mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    experiment_runner, "_require_formal_dispatch_environment"), \
                mock.patch.object(
                    experiment_runner, "append_run_metadata",
                    return_value={"invocation_id": "invocation"}), \
                mock.patch.object(
                    experiment_runner, "_dispatch_worker_start_barrier"), \
                mock.patch.object(
                    experiment_runner, "run_relay", side_effect=failure), \
                mock.patch.object(
                    experiment_runner, "_record_evaluator_incomplete") as record, \
                mock.patch.object(
                    experiment_runner, "finish_run_metadata") as finish:
            with self.assertRaises(
                    experiment_runner.EvaluatorIncompleteError):
                experiment_runner.main()

        record.assert_called_once_with(
            out_dir, "sample", ["hybridpatch", "fullrewrite"], 1,
            "invocation", failure,
        )
        finish.assert_called_once_with(
            out_dir, "invocation", status="evaluator_incomplete")

    def test_runner_barrier_stop_records_dispatcher_interruption_metadata(self):
        for condition in (
                "operator_directed_dispatcher_pause",
                "dispatcher_process_lost"):
            with self.subTest(condition=condition), \
                    tempfile.TemporaryDirectory() as out_dir:
                ready_path = os.path.join(out_dir, "worker.ready.json")
                ack_path = os.path.join(out_dir, "worker.start.json")
                run_meta.record_campaign_stop_condition(
                    out_dir, condition, dispatcher_pid=4242,
                    dispatcher_instance_id="dispatcher-a")
                environment = {
                    "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
                    "ANCHORPATCH_WORKER_READY_PATH": ready_path,
                    "ANCHORPATCH_WORKER_ACK_PATH": ack_path,
                    "ANCHORPATCH_START_BARRIER_TIMEOUT": "1",
                }
                with mock.patch.dict(os.environ, environment, clear=False), \
                        mock.patch.object(
                            sys, "argv", [
                                "experiment_runner.py", "--sample", "sample",
                                "--methods", "hybridpatch", "fullrewrite",
                                "--num_round_trips", "1", "--out_dir", out_dir,
                                "--model", "offline-test-model",
                            ]), \
                        mock.patch.object(
                            experiment_runner,
                            "_require_formal_opencode_transport"), \
                        mock.patch.object(
                            experiment_runner,
                            "_require_formal_dispatch_environment"), \
                        mock.patch.object(
                            experiment_runner, "append_run_metadata",
                            return_value={"invocation_id": "invocation"}), \
                        mock.patch.object(
                            experiment_runner, "register_task_plan",
                            return_value={"sha256": "a" * 64}), \
                        mock.patch.object(
                            experiment_runner, "run_relay") as relay, \
                        mock.patch.object(
                            experiment_runner, "finish_run_metadata") as finish:
                    with self.assertRaises(run_meta.CampaignStoppedError):
                        experiment_runner.main()

                relay.assert_not_called()
                finish.assert_called_once_with(
                    out_dir, "invocation",
                    status="interrupted_by_dispatcher")
                self.assertTrue(os.path.isfile(ready_path))
                self.assertFalse(os.path.exists(ack_path))

    def test_launch_poll_isolates_exhausted_worker_and_keeps_sibling_running(self):
        class FakeProcess:
            def __init__(self, pid, poll_sequence):
                self.pid = pid
                self._poll_sequence = list(poll_sequence)
                self.returncode = None

            def poll(self):
                if self._poll_sequence:
                    self.returncode = self._poll_sequence.pop(0)
                return self.returncode

        def make_task_plans(out_dir, samples, *_args):
            plans = {}
            for sample in samples:
                plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="smoke",
                smoke_dir=None,
                samples=["sample-a", "sample-b"],
                key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env",
                num_round_trips=1,
                seed=42,
                dry_run=False,
                resume=False,
                start_timeout=0.1,
                poll_interval=0,
                progress_interval=9999,
                notes="unit",
            )
            active_snapshots = []
            real_write_active = paired_dispatch._write_active_worker_set

            def capture_active(active_out_dir, manifest, workers):
                record = real_write_active(active_out_dir, manifest, workers)
                active_snapshots.append(sorted(
                    item["sample"] for item in record["workers"].values()))
                return record

            processes = {}

            def fake_popen(command, **_kwargs):
                sample = command[command.index("--sample") + 1]
                process = (
                    FakeProcess(101, [7])
                    if sample == "sample-a"
                    else FakeProcess(202, [None, 0])
                )
                processes[sample] = process
                return process

            def fake_latest_sample_outcomes(
                    active_out_dir, *_args, **_kwargs):
                if "sample-a" not in processes:
                    return {}
                active = paired_dispatch._read_json(
                    paired_dispatch._active_worker_set_path(active_out_dir))
                worker_id = next(
                    (
                        candidate for candidate, payload
                        in (active.get("workers") or {}).items()
                        if payload.get("sample") == "sample-a"
                    ),
                    None,
                )
                return {
                    "sample-a": {
                        "status": "infrastructure_incomplete",
                        "worker_launch_id": worker_id,
                        "worker_pid": processes["sample-a"].pid,
                    }
                }

            with mock.patch.object(
                    paired_dispatch, "_validate_campaign_grid"), \
                    mock.patch.object(
                        paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={"errors": [], "api_calls": 2,
                                      "preservation_violations": 0}) as inspect, \
                    mock.patch.object(
                        paired_dispatch, "_authorize_workers"), \
                    mock.patch.object(
                        paired_dispatch, "_verified_infrastructure_incomplete",
                        return_value={"semantic_call_id": "failed-call",
                                      "transport_recovery_index": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_latest_sample_outcomes",
                        side_effect=fake_latest_sample_outcomes), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen",
                        side_effect=fake_popen), \
                    mock.patch.object(
                        paired_dispatch.time, "sleep"), \
                    mock.patch.object(
                        paired_dispatch, "_write_active_worker_set",
                        side_effect=capture_active):
                result = paired_dispatch._launch_under_lease(args, out_dir)

            self.assertEqual(result, 2)
            self.assertEqual(set(processes), {"sample-a", "sample-b"})
            self.assertIn(["sample-a", "sample-b"], active_snapshots)
            self.assertIn(["sample-b"], active_snapshots)
            self.assertEqual(active_snapshots[-1], [])
            self.assertEqual(
                paired_dispatch.read_campaign_stop_conditions(out_dir), [])
            dispatch_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            self.assertEqual(
                [row.get("disposition") for row in dispatch_rows
                 if row.get("event") == "worker_exit"],
                ["infrastructure_incomplete", "finished"])
            self.assertEqual(
                dispatch_rows[-1]["event"], "campaign_incomplete")
            self.assertEqual(
                dispatch_rows[-1]["infrastructure_incomplete_samples"],
                ["sample-a"])
            self.assertTrue(
                inspect.call_args.kwargs["require_terminal_provenance"])

    def test_confirmation_queue_isolates_incomplete_and_preservation_stops(self):
        samples = ["sample-a", "sample-b", "sample-c"]
        lifecycle = []

        def make_task_plans(out_dir, sample_ids, *_args):
            lifecycle.append("all_plans")
            plans = {}
            for sample in sample_ids:
                path = os.path.join(out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(path),
                    "sha256": paired_dispatch._sha256(path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="confirmation", smoke_dir=None,
                samples=list(samples), key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env", num_round_trips=10, seed=42,
                slots_per_key=1, dry_run=False, resume=False,
                start_timeout=0.1, poll_interval=0,
                progress_interval=9999, notes="unit",
                selection_manifest="selection.json",
            )
            selection_record = {
                "experiment_id": os.path.basename(out_dir),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "HP_V8/analysis/selection.json",
                "sha256": "1" * 64,
                "artifact_sha256_preview": "2" * 64,
                "candidate_count": 85, "selected_count": 68,
                "reserve_count": 17, "seed": 42,
            }
            launched_queues = []

            def fake_queue(
                    _args, _out_dir, _manifest, _plans, _keys, assignments,
                    _authorizations, _dispatch_log, _running, incomplete,
                    _evaluator_incomplete, completed, _total, _slots):
                self.assertEqual(lifecycle, ["all_plans"])
                manifest = paired_dispatch._read_json(os.path.join(
                    out_dir, "dispatch_manifest.json"))
                self.assertEqual(set(manifest["task_plans"]), set(samples))
                queue_samples = [item["sample"] for item in assignments]
                launched_queues.append(queue_samples)
                incomplete.add("sample-a")
                completed.update({"sample-b", "sample-c"})

            with mock.patch.object(
                    paired_dispatch, "CONFIRMATION_SAMPLE_COUNT", 3), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_KEY_COUNT", 2), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_SLOTS_PER_KEY", 1), \
                    mock.patch.object(
                        paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={"errors": [], "api_calls": 3,
                                      "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(list(samples), selection_record)) as load_selection, \
                    mock.patch.object(
                        paired_dispatch, "_run_worker_queue",
                        side_effect=fake_queue):
                result = paired_dispatch._launch_under_lease(args, out_dir)
                load_selection.assert_called_once_with(
                    "selection.json", current_experiment_dir=out_dir)

            self.assertEqual(result, 2)
            self.assertEqual(
                launched_queues, [["sample-a", "sample-b", "sample-c"]])
            dispatch_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            self.assertFalse(any(
                row["event"].startswith("wave_") for row in dispatch_rows))

        lifecycle.clear()
        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="confirmation", smoke_dir=None,
                samples=list(samples), key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env", num_round_trips=10, seed=42,
                slots_per_key=1, dry_run=False, resume=False,
                start_timeout=0.1, poll_interval=0,
                progress_interval=9999, notes="unit",
                selection_manifest="selection.json",
            )
            selection_record = {
                "experiment_id": os.path.basename(out_dir),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "HP_V8/analysis/selection.json",
                "sha256": "1" * 64,
                "artifact_sha256_preview": "2" * 64,
                "candidate_count": 85, "selected_count": 68,
                "reserve_count": 17, "seed": 42,
            }
            stopped_queues = []

            def preservation_stop(
                    _args, wave_out_dir, _manifest, _plans, _keys,
                    assignments, *_rest):
                stopped_queues.append(
                    [item["sample"] for item in assignments])
                run_meta.record_campaign_stop_condition(
                    wave_out_dir, "preservation_violation",
                    preservation_violations=1)
                raise RuntimeError("preservation_violations=1")

            with mock.patch.object(
                    paired_dispatch, "CONFIRMATION_SAMPLE_COUNT", 3), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_KEY_COUNT", 2), \
                    mock.patch.object(
                        paired_dispatch, "CONFIRMATION_SLOTS_PER_KEY", 1), \
                    mock.patch.object(
                        paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={"errors": [], "api_calls": 0,
                                      "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(list(samples), selection_record)) as load_selection, \
                    mock.patch.object(
                        paired_dispatch, "_run_worker_queue",
                        side_effect=preservation_stop):
                result = paired_dispatch._launch_under_lease(args, out_dir)
                load_selection.assert_called_once_with(
                    "selection.json", current_experiment_dir=out_dir)

            self.assertEqual(result, 1)
            self.assertEqual(
                stopped_queues, [["sample-a", "sample-b", "sample-c"]])
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0][
                    "condition"], "preservation_violation")

    def test_work_conserving_queue_refills_key_after_evaluator_incomplete(self):
        class FakeProcess:
            def __init__(self, pid, polls):
                self.pid = pid
                self.polls = list(polls)
                self.returncode = None

            def poll(self):
                if self.polls:
                    self.returncode = self.polls.pop(0)
                return self.returncode

        assignments = [
            {"sample": "sample-a", "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"]},
            {"sample": "sample-b", "key_label": "KEY_01",
             "methods": ["fullrewrite", "hybridpatch"]},
            {"sample": "sample-c", "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"]},
            {"sample": "sample-x", "key_label": "KEY_02",
             "methods": ["fullrewrite", "hybridpatch"]},
        ]
        polls = {
            "sample-a": [7],
            "sample-b": [None, 0],
            "sample-c": [0],
            "sample-x": [None, None, 0],
        }
        lifecycle = []

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            lifecycle.append({
                "event": "launch",
                "samples": [item["sample"] for item in batch],
                "already_running": sorted(running),
            })
            for index, item in enumerate(batch, 1):
                sample = item["sample"]
                running[sample] = {
                    **item,
                    "process": FakeProcess(index, polls[sample]),
                    "log": mock.Mock(),
                    "worker_launch_id": f"worker-{sample}",
                    "exit_recorded": False,
                }
            by_key = {}
            for item in running.values():
                by_key[item["key_label"]] = (
                    by_key.get(item["key_label"], 0) + 1)
            self.assertTrue(all(count <= 2 for count in by_key.values()))
            return [item["sample"] for item in batch]

        def fake_exit(_out_dir, running, sample, _item, returncode,
                      _dispatch_log):
            del running[sample]
            disposition = (
                "evaluator_incomplete"
                if sample == "sample-a" else "finished")
            self.assertEqual(returncode != 0, sample == "sample-a")
            lifecycle.append({"event": "exit", "sample": sample,
                              "disposition": disposition})
            return disposition

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        infrastructure_incomplete = set()
        evaluator_incomplete = set()
        completed = set()
        inspection_calls = []

        def fake_inspect(*_args, **kwargs):
            inspection_calls.append({
                "active_samples": set(kwargs.get("active_samples") or []),
                "required_complete_samples": set(
                    kwargs.get("required_complete_samples") or []),
            })
            return {"errors": [], "api_calls": 0,
                    "preservation_violations": 0}

        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "_record_worker_exit",
                    side_effect=fake_exit), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    side_effect=fake_inspect), \
                mock.patch.object(
                    paired_dispatch,
                    "_write_active_worker_set") as write_active, \
                mock.patch.object(paired_dispatch.time, "sleep"):
            paired_dispatch._run_worker_queue(
                args, out_dir, {"run_git_commit": "1" * 40}, {}, {},
                assignments, {}, os.path.join(out_dir, "dispatch_log.jsonl"),
                {}, infrastructure_incomplete, evaluator_incomplete,
                completed, len(assignments), 2,
            )

        launches = [row for row in lifecycle if row["event"] == "launch"]
        self.assertEqual(
            [row["samples"] for row in launches],
            [["sample-a", "sample-b", "sample-x"], ["sample-c"]],
        )
        self.assertIn("sample-b", launches[1]["already_running"])
        self.assertEqual(infrastructure_incomplete, set())
        self.assertEqual(evaluator_incomplete, {"sample-a"})
        self.assertEqual(completed, {"sample-b", "sample-c", "sample-x"})
        self.assertEqual(len(inspection_calls), 3)
        self.assertEqual(
            [call["required_complete_samples"]
             for call in inspection_calls],
            [set(), {"sample-b", "sample-c"}, {"sample-x"}],
        )
        self.assertEqual(write_active.call_count, 4)

    def test_worker_queue_idle_polls_do_not_run_full_inspection(self):
        class FakeProcess:
            pid = 101
            returncode = None

            def __init__(self):
                self.polls = [None, None, 0]

            def poll(self):
                self.returncode = self.polls.pop(0)
                return self.returncode

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            item = batch[0]
            running[item["sample"]] = {
                **item,
                "process": FakeProcess(),
                "log": mock.Mock(),
                "worker_launch_id": "worker-sample-a",
                "exit_recorded": False,
            }
            return [item["sample"]]

        def fake_exit(_out_dir, running, sample, _item, returncode,
                      _dispatch_log):
            self.assertEqual(returncode, 0)
            del running[sample]
            return "finished"

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        inspection = {
            "errors": [], "api_calls": 2, "preservation_violations": 0,
        }
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "_record_worker_exit",
                    side_effect=fake_exit), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value=inspection) as inspect, \
                mock.patch.object(
                    paired_dispatch, "_write_active_worker_set"), \
                mock.patch.object(paired_dispatch.time, "sleep"):
            paired_dispatch._run_worker_queue(
                args,
                out_dir,
                {"run_git_commit": "1" * 40},
                {},
                {},
                [{
                    "sample": "sample-a", "key_label": "KEY_01",
                    "methods": ["hybridpatch", "fullrewrite"],
                }],
                {},
                os.path.join(out_dir, "dispatch_log.jsonl"),
                {},
                set(),
                set(),
                set(),
                1,
                1,
            )

        inspect.assert_called_once()
        self.assertEqual(
            inspect.call_args.kwargs["required_complete_samples"],
            {"sample-a"},
        )

    def test_worker_queue_audit_failure_does_not_refill_released_slot(self):
        class FakeProcess:
            pid = 101
            returncode = None

            def poll(self):
                self.returncode = 0
                return self.returncode

        launched = []

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            launched.extend(item["sample"] for item in batch)
            for item in batch:
                running[item["sample"]] = {
                    **item,
                    "process": FakeProcess(),
                    "log": mock.Mock(),
                    "worker_launch_id": f"worker-{item['sample']}",
                    "exit_recorded": False,
                }
            return [item["sample"] for item in batch]

        def fake_exit(_out_dir, running, sample, _item, returncode,
                      _dispatch_log):
            self.assertEqual(returncode, 0)
            del running[sample]
            return "finished"

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        assignments = [
            {
                "sample": "sample-a", "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
            },
            {
                "sample": "sample-b", "key_label": "KEY_01",
                "methods": ["fullrewrite", "hybridpatch"],
            },
        ]
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "_record_worker_exit",
                    side_effect=fake_exit), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={
                        "errors": ["post-exit audit failure"],
                        "api_calls": 0,
                        "preservation_violations": 0,
                    }) as inspect, \
                mock.patch.object(
                    paired_dispatch, "_write_active_worker_set"), \
                mock.patch.object(paired_dispatch.time, "sleep"):
            with self.assertRaisesRegex(
                    RuntimeError, "post-exit audit failure"):
                paired_dispatch._run_worker_queue(
                    args, out_dir, {"run_git_commit": "1" * 40}, {}, {},
                    assignments, {},
                    os.path.join(out_dir, "dispatch_log.jsonl"), {},
                    set(), set(), set(), len(assignments), 1,
                )

        self.assertEqual(launched, ["sample-a"])
        inspect.assert_called_once()
        self.assertEqual(
            inspect.call_args.kwargs["required_complete_samples"],
            {"sample-a"},
        )

    def test_worker_terminal_provenance_accepts_finished_and_local_incomplete(self):
        launch = {"sample": "sample", "pid": 101}
        created_at = "2026-07-29T12:00:00+08:00"
        for status, returncode in (
                ("finished", 0),
                ("infrastructure_incomplete", 2),
                ("evaluator_incomplete", 3)):
            with self.subTest(status=status):
                exit_row = {
                    "sample": "sample",
                    "pid": 101,
                    "returncode": returncode,
                    "disposition": status,
                    "created_at": created_at,
                }
                if status != "finished":
                    exit_row["evidence"] = {"verified": True}
                proof = paired_dispatch._worker_terminal_provenance(
                    launch,
                    exit_row,
                    [{
                        "status": status,
                        "worker_pid": 101,
                        "samples": ["sample"],
                    }],
                )
                self.assertTrue(proof["ordinary_ok"])

        invalid = paired_dispatch._worker_terminal_provenance(
            launch,
            {
                "sample": "sample",
                "pid": 101,
                "returncode": 2,
                "disposition": "finished",
                "created_at": created_at,
                "evidence": {"verified": True},
            },
            [{
                "status": "infrastructure_incomplete",
                "worker_pid": 101,
                "samples": ["sample"],
            }],
        )
        self.assertFalse(invalid["ordinary_ok"])

    def test_worker_queue_idle_poll_stops_on_campaign_latch(self):
        class FakeProcess:
            pid = 101
            returncode = None

            @staticmethod
            def poll():
                return None

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            item = batch[0]
            running[item["sample"]] = {
                **item,
                "process": FakeProcess(),
                "log": mock.Mock(),
                "worker_launch_id": "worker-sample-a",
                "exit_recorded": False,
            }
            return [item["sample"]]

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "read_campaign_stop_conditions",
                    return_value=[{"condition": "preservation_violation"}]), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign") as inspect, \
                mock.patch.object(
                    paired_dispatch, "_write_active_worker_set"), \
                mock.patch.object(paired_dispatch.time, "sleep"):
            with self.assertRaisesRegex(
                    RuntimeError, "preservation_violation"):
                paired_dispatch._run_worker_queue(
                    args,
                    out_dir,
                    {"run_git_commit": "1" * 40},
                    {},
                    {},
                    [{
                        "sample": "sample-a", "key_label": "KEY_01",
                        "methods": ["hybridpatch", "fullrewrite"],
                    }],
                    {},
                    os.path.join(out_dir, "dispatch_log.jsonl"),
                    {},
                    set(),
                    set(),
                    set(),
                    1,
                    1,
                )

        inspect.assert_not_called()

    def test_worker_queue_refill_boundary_stop_blocks_next_launch(self):
        class FakeProcess:
            pid = 101
            returncode = None

            @staticmethod
            def poll():
                return 0

        launched = []

        def fake_launch(_args, _out_dir, _manifest, _plans, _keys,
                        batch, _authorizations, _dispatch_log, running):
            launched.extend(item["sample"] for item in batch)
            for item in batch:
                running[item["sample"]] = {
                    **item,
                    "process": FakeProcess(),
                    "log": mock.Mock(),
                    "worker_launch_id": f"worker-{item['sample']}",
                    "exit_recorded": False,
                }
            return [item["sample"] for item in batch]

        def fake_exit(_out_dir, running, sample, _item, _returncode,
                      _dispatch_log):
            del running[sample]
            return "finished"

        args = mock.Mock(
            campaign_role="full234", poll_interval=0,
            progress_interval=9999,
        )
        assignments = [
            {
                "sample": "sample-a", "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
            },
            {
                "sample": "sample-b", "key_label": "KEY_01",
                "methods": ["fullrewrite", "hybridpatch"],
            },
        ]
        stop_checks = [[], [], [{"condition": "preservation_violation"}]]
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), \
                mock.patch.object(
                    paired_dispatch, "_record_worker_exit",
                    side_effect=fake_exit), \
                mock.patch.object(
                    paired_dispatch, "read_campaign_stop_conditions",
                    side_effect=stop_checks), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={
                        "errors": [], "api_calls": 1,
                        "preservation_violations": 0,
                    }) as inspect, \
                mock.patch.object(
                    paired_dispatch, "_write_active_worker_set"), \
                mock.patch.object(paired_dispatch.time, "sleep"):
            with self.assertRaisesRegex(
                    RuntimeError, "before worker queue refill.*preservation"):
                paired_dispatch._run_worker_queue(
                    args, out_dir, {"run_git_commit": "1" * 40}, {}, {},
                    assignments, {},
                    os.path.join(out_dir, "dispatch_log.jsonl"), {},
                    set(), set(), set(), len(assignments), 1,
                )

        self.assertEqual(launched, ["sample-a"])
        inspect.assert_called_once()

    def test_operator_pause_reconciles_terminal_active_workers_by_truth(self):
        expected_progress = {
            "fullrewrite": {
                "completed_round_trips": 10,
                "committed_rows": 20,
            }
        }
        workers = {
            "worker-finished": {"sample": "sample-finished"},
            "worker-incomplete": {"sample": "sample-incomplete"},
        }
        launches = [
            {
                "event": "launch", "sample": "sample-finished",
                "key_label": "KEY_01", "worker_launch_id": "worker-finished",
                "pid": 101,
            },
            {
                "event": "launch", "sample": "sample-incomplete",
                "key_label": "KEY_02", "worker_launch_id": "worker-incomplete",
                "pid": 202,
            },
        ]
        metadata = [
            {
                "worker_launch_id": "worker-finished", "worker_pid": 101,
                "samples": ["sample-finished"], "methods": ["fullrewrite"],
                "method_phase": "fullrewrite", "status": "finished",
            },
            {
                "worker_launch_id": "worker-incomplete", "worker_pid": 202,
                "samples": ["sample-incomplete"], "methods": ["fullrewrite"],
                "method_phase": "fullrewrite",
                "status": "infrastructure_incomplete",
            },
        ]
        outcomes = [
            {
                "worker_launch_id": "worker-finished", "worker_pid": 101,
                "sample": "sample-finished", "status": "finished",
                "checkpoint_progress": expected_progress,
            },
            {
                "worker_launch_id": "worker-incomplete", "worker_pid": 202,
                "sample": "sample-incomplete",
                "status": "infrastructure_incomplete",
            },
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"num_round_trips": 10},
        }
        with tempfile.TemporaryDirectory() as out_dir:
            pathlib.Path(out_dir, "active_worker_set.json").write_text(
                json.dumps({"workers": workers}), encoding="utf-8")
            dispatch_path = pathlib.Path(out_dir, "dispatch_log.jsonl")
            dispatch_path.write_text(
                "".join(json.dumps(row) + "\n" for row in launches),
                encoding="utf-8",
            )
            with mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery, "read_run_metadata_snapshot",
                        return_value=metadata), \
                    mock.patch.object(
                        ledger_recovery, "read_sample_outcomes",
                        return_value=outcomes), \
                    mock.patch.object(
                        ledger_recovery, "_actual_sample_progress",
                        return_value=expected_progress), \
                    mock.patch.object(
                        ledger_recovery, "_verified_infrastructure_incomplete",
                        return_value={"semantic_call_id": "call"}), \
                    mock.patch.object(
                        ledger_recovery, "_write_active_worker_set") as write_active:
                reconciled = ledger_recovery._reconcile_terminal_active_workers(
                    out_dir, manifest)
            rows = ledger_recovery._read_jsonl(str(dispatch_path))

        exits = [row for row in rows if row.get("event") == "worker_exit"]
        self.assertEqual(len(exits), 2)
        by_sample = {row["sample"]: row for row in exits}
        self.assertEqual(by_sample["sample-finished"]["returncode"], 0)
        self.assertEqual(
            by_sample["sample-finished"]["disposition"], "finished")
        self.assertEqual(by_sample["sample-incomplete"]["returncode"], 1)
        self.assertEqual(
            by_sample["sample-incomplete"]["disposition"],
            "infrastructure_incomplete")
        self.assertTrue(all(
            row["exit_code_observed"] is False for row in exits))
        self.assertEqual(
            [item["sample"] for item in reconciled],
            ["sample-finished", "sample-incomplete"])
        write_active.assert_called_once_with(out_dir, manifest, [])

    def test_operator_pause_reconciles_only_explicit_interrupted_worker(self):
        worker_id = "worker-interrupted"
        sample = "sample-interrupted"
        launch = {
            "event": "launch", "sample": sample,
            "key_label": "KEY_01", "worker_launch_id": worker_id,
            "pid": 303,
        }
        metadata = [{
            "invocation_id": "invocation-interrupted",
            "worker_launch_id": worker_id, "worker_pid": 303,
            "samples": [sample], "methods": ["fullrewrite"],
            "method_phase": "fullrewrite", "status": "running",
        }]
        audit = [{
            "invocation_id": "invocation-interrupted",
            "worker_launch_id": worker_id, "worker_pid": 303,
            "sample": sample, "method_phase": "fullrewrite",
            "launch_recorded": True,
        }]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"num_round_trips": 10},
        }
        with tempfile.TemporaryDirectory() as out_dir:
            pathlib.Path(out_dir, "active_worker_set.json").write_text(
                json.dumps({"workers": {
                    worker_id: {"sample": sample}
                }}), encoding="utf-8")
            dispatch_path = pathlib.Path(out_dir, "dispatch_log.jsonl")
            dispatch_path.write_text(
                json.dumps(launch) + "\n", encoding="utf-8")
            with mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_audit_running_invocation_provenance",
                        return_value=audit), \
                    mock.patch.object(
                        ledger_recovery, "read_run_metadata_snapshot",
                        return_value=metadata), \
                    mock.patch.object(
                        ledger_recovery, "read_sample_outcomes",
                        return_value=[]), \
                    mock.patch.object(
                        ledger_recovery,
                        "interrupt_audited_running_invocations",
                        return_value=[{
                            "invocation_id": "invocation-interrupted",
                            "worker_launch_id": worker_id,
                            "worker_pid": 303, "samples": [sample],
                        }]) as interrupt, \
                    mock.patch.object(
                        ledger_recovery,
                        "_write_active_worker_set") as write_active:
                reconciled = (
                    ledger_recovery._reconcile_terminal_active_workers(
                        out_dir, manifest,
                        operator_interrupted_samples=[sample],
                        operator_pause_reason="stalled stream",
                    )
                )
            rows = ledger_recovery._read_jsonl(str(dispatch_path))

        interrupt.assert_called_once_with(
            out_dir, status="interrupted_before_audited_resume",
            audited=audit)
        self.assertEqual(reconciled, [{
            "sample": sample, "worker_launch_id": worker_id,
            "worker_pid": 303,
            "status": "interrupted_before_audited_resume",
        }])
        stale = [
            row for row in rows
            if row.get("event") == "stale_worker_reconciled"
        ]
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["sample"], sample)
        self.assertEqual(stale[0]["reason"], "stalled stream")
        self.assertFalse(any(
            row.get("event") == "worker_exit" for row in rows))
        write_active.assert_called_once_with(out_dir, manifest, [])

    def test_operator_pause_hash_binds_interrupted_open_attempt(self):
        worker_id = "worker-interrupted"
        semantic_call_id = (
            "fullrewrite/sample-interrupted/rt09/backward/"
            "fullrewrite_primary")
        rows = [
            {
                "event": "semantic_request",
                "worker_launch_id": worker_id,
                "semantic_call_id": semantic_call_id,
            },
            {
                "event": "attempt_start", "attempt_index": 1,
                "worker_launch_id": worker_id,
                "semantic_call_id": semantic_call_id,
            },
            {
                "event": "generation_progress",
                "delta_type": "thinking_delta", "attempt_index": 1,
                "worker_launch_id": worker_id,
                "semantic_call_id": semantic_call_id,
            },
        ]
        prior = {
            "recovered_worker_launch_ids": ["worker-prior"],
            "incident_attempt_rows": [],
            "operator_pause_reconciled_workers": [{
                "sample": "sample-interrupted",
                "worker_launch_id": worker_id,
                "worker_pid": 303,
                "status": "interrupted_before_audited_resume",
            }],
        }
        with tempfile.TemporaryDirectory() as out_dir:
            attempt_path = pathlib.Path(
                out_dir, "api_attempt_ledger.jsonl")
            attempt_path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            result = (
                ledger_recovery
                ._extend_operator_interrupted_attempt_evidence(
                    out_dir, prior, []))

        self.assertEqual(
            result["recovered_worker_launch_ids"],
            ["worker-prior", worker_id])
        self.assertEqual(
            result["operator_interrupted_attempt_row_count"], 3)
        self.assertEqual(
            [entry["row_number"]
             for entry in result["incident_attempt_rows"]],
            [1, 2, 3])
        self.assertTrue(all(
            entry["incident_kind"]
            == "dispatcher_interrupted_open_attempt"
            for entry in result["incident_attempt_rows"]))

    def test_confirmation_duplicate_key_value_fails_before_provider(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        key_values = {
            label: f"secret-value-{index}"
            for index, label in enumerate(labels)
        }
        key_values["KEY_02"] = key_values["KEY_01"]
        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="confirmation", smoke_dir=None,
                samples=[], key_labels=labels, keys_file="unused.env",
                num_round_trips=10, seed=42, slots_per_key=4,
                dry_run=False, resume=False, start_timeout=0.1,
                poll_interval=0, progress_interval=9999, notes="unit",
                selection_manifest="selection.json",
            )
            selection_record = {
                "experiment_id": os.path.basename(out_dir),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "HP_V8/analysis/selection.json",
                "sha256": "1" * 64,
                "artifact_sha256_preview": "2" * 64,
                "candidate_count": 85, "selected_count": 68,
                "reserve_count": 17, "seed": 42,
            }
            with mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(samples, selection_record)), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value=key_values), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=AssertionError(
                            "duplicate keys must fail before task plans")), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen",
                        side_effect=AssertionError(
                            "duplicate keys must fail before provider")):
                with self.assertRaisesRegex(RuntimeError, "physically unique"):
                    paired_dispatch._launch_under_lease(args, out_dir)

    def test_fresh_campaign_dry_run_writes_empty_active_set_and_passes(self):
        def make_task_plans(out_dir, samples, *_args):
            plans = {}
            for sample in samples:
                plan_path = os.path.join(
                    out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(
                    plan_path, ["target"])
                plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                }
            return plans

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock(
                campaign_role="smoke", smoke_dir=None,
                samples=["sample-a", "sample-b"],
                key_labels=["KEY_01", "KEY_02"],
                keys_file="unused.env", num_round_trips=1, seed=42,
                dry_run=True, resume=False, notes="unit",
            )
            with mock.patch.object(
                    paired_dispatch, "_validate_campaign_grid"), \
                    mock.patch.object(
                        paired_dispatch,
                        "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={"KEY_01": "redacted-a",
                                      "KEY_02": "redacted-b"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        side_effect=make_task_plans), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}):
                result = paired_dispatch._launch_under_lease(args, out_dir)

            self.assertEqual(result, 0)
            active = paired_dispatch._read_json(
                paired_dispatch._active_worker_set_path(out_dir))
            self.assertEqual(active, {
                "schema": "anchorpatch.active_worker_set/1",
                "run_git_commit": "1" * 40,
                "dispatcher_pid": None,
                "dispatcher_instance_id": None,
                "workers": {},
            })
            manifest = paired_dispatch._read_json(os.path.join(
                out_dir, "dispatch_manifest.json"))
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest,
                    active_samples={"sample-a", "sample-b"})
            self.assertEqual(inspection["errors"], [])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "api_calls.jsonl")))

    def test_preservation_latch_keeps_failure_global_for_all_workers(self):
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_infrastructure_fixture(out_dir)
            run_meta.record_campaign_stop_condition(
                out_dir, "preservation_violation",
                preservation_violations=1)
            running = {}
            for sample, worker, pid in (
                    ("sample-a", "worker-a", 101),
                    ("sample-b", "worker-b", 202)):
                process = mock.Mock(pid=pid)
                process.poll.return_value = 7 if sample == "sample-a" else None
                running[sample] = {
                    "sample": sample, "process": process,
                    "log": mock.Mock(), "key_label": f"KEY_{pid}",
                    "worker_launch_id": worker,
                    "methods": ["hybridpatch", "fullrewrite"],
                    "target_round_trips": 1, "exit_recorded": False,
                }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with self.assertRaisesRegex(
                    RuntimeError, "campaign-wide stop latch"):
                paired_dispatch._record_worker_exit(
                    out_dir, running, "sample-a", running["sample-a"], 7,
                    dispatch_log)
            self.assertEqual(set(running), {"sample-a", "sample-b"})
            with mock.patch.object(
                    paired_dispatch, "_terminate_workers") as terminate, \
                    mock.patch.object(
                        paired_dispatch, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        paired_dispatch,
                        "_audit_running_invocation_provenance",
                        return_value=[]), \
                    mock.patch.object(
                        paired_dispatch,
                        "interrupt_audited_running_invocations",
                        return_value=[]):
                paired_dispatch._stop_and_reconcile_workers(out_dir, running)
            terminate.assert_called_once_with(running)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0][
                    "condition"], "preservation_violation")

    def test_run_relay_resume_skips_committed_round_trip_without_provider_post(self):
        class DummyDomain:
            samples_folder = None

        states = {
            "initial": {
                "context": ["a.txt"],
                "solution_folder": "solution",
                "prompts": [{"target_state": "target", "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"],
                "solution_folder": "solution",
                "prompts": [{"target_state": "initial", "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}

        with tempfile.TemporaryDirectory() as out_dir:
            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            utils_relay_plan.save_relay_task_plan(
                os.path.join(out_dir, "sample.task_plan.json"),
                ["target"],
            )
            with open(
                os.path.join(method_dir, "sample.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for direction in ("forward", "backward"):
                    handle.write(json.dumps({
                        "sample_id": "sample",
                        "method": "hybridpatch",
                        "round_trip_num": 1,
                        "round_trip_direction": direction,
                    }) + "\n")
            run_meta.write_json_atomic(
                os.path.join(method_dir, "sample.ckpt.json"),
                {
                    "completed_round_trips": 1,
                    "current_context": {"a.txt": "already committed"},
                    "rid_chain": ["rid-forward", "rid-backward"],
                    "state_chain": ["initial", "target", "initial"],
                    "context_shuffle_random_state": None,
                },
            )
            provider_post = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            with mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        experiment_runner, "load_sample",
                        return_value=(sample, out_dir, states)), \
                    mock.patch.object(
                        experiment_runner, "get_domain",
                        return_value=DummyDomain()), \
                    mock.patch.object(
                        experiment_runner, "load_distractor_context",
                        return_value={}), \
                    mock.patch.object(
                        experiment_runner, "register_task_plan",
                        return_value={"sha256": "a" * 64,
                                      "round_trips": 1}), \
                    mock.patch.object(
                        experiment_runner, "_edit_step",
                        side_effect=AssertionError(
                            "committed RT must not be regenerated")) as edit_step:
                result_path = experiment_runner.run_relay(
                    "hybridpatch", "sample", num_round_trips=1,
                    include_distractor=True, out_dir=out_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=provider_post, printing=False,
                )

            self.assertEqual(
                result_path,
                os.path.join(method_dir, "sample.jsonl"))
            provider_post.assert_not_called()
            edit_step.assert_not_called()

    def test_interrupted_phase_prefix_resumes_without_reposting_committed_rt(self):
        class DummyDomain:
            samples_folder = None

        sample_id = "sample"
        method = "hybridpatch"
        plan_hash = "a" * 64
        states = {
            "initial": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{"target_state": "target",
                             "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{"target_state": "initial",
                             "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}
        with tempfile.TemporaryDirectory() as out_dir:
            method_dir = os.path.join(out_dir, method)
            os.makedirs(method_dir, exist_ok=True)
            utils_relay_plan.save_relay_task_plan(
                os.path.join(out_dir, "sample.task_plan.json"), ["target"])
            with open(
                    os.path.join(method_dir, "sample.jsonl"),
                    "w", encoding="utf-8") as handle:
                for direction in ("forward", "backward"):
                    handle.write(json.dumps({
                        "sample_id": sample_id,
                        "method": method,
                        "round_trip_num": 1,
                        "round_trip_direction": direction,
                    }) + "\n")
            run_meta.write_json_atomic(
                os.path.join(method_dir, "sample.ckpt.json"), {
                    "completed_round_trips": 1,
                    "current_context": {"a.txt": "already committed"},
                    "rid_chain": ["rid-forward", "rid-backward"],
                    "state_chain": ["initial", "target", "initial"],
                    "context_shuffle_random_state": None,
                })
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"), [{
                    "invocation_id": "inv-old",
                    "worker_launch_id": "worker-old",
                    "worker_pid": 101,
                    "samples": [sample_id],
                    "methods": [method],
                    "method_phase": method,
                    "status": "interrupted_before_audited_resume",
                    "task_plans": {sample_id: {
                        "sha256": plan_hash, "round_trips": 1}},
                }])
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            for row in (
                {"event": "launch_intent", "sample": sample_id,
                 "methods": [method], "method_phase": method,
                 "worker_launch_id": "worker-old"},
                {"event": "launch", "sample": sample_id,
                 "method_phase": method, "worker_launch_id": "worker-old",
                 "pid": 101},
                {"event": "stale_worker_reconciled", "sample": sample_id,
                 "worker_launch_id": "worker-old", "pid": 101,
                 "invocation_id": "inv-old"},
            ):
                run_meta.append_jsonl_locked(dispatch_log, row)
            assignment = {
                "sample": sample_id, "key_label": "KEY_01",
                "methods": [method], "method_phase": method,
            }
            selected, transport_authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, [assignment], resume=True,
                    target_round_trips=1,
                    allow_pristine_pending=True,
                    method_phase=method,
                    allow_audited_interrupted=True,
                    task_plans={sample_id: {"sha256": plan_hash}}))
            self.assertEqual(len(selected), 1)
            self.assertEqual(transport_authorizations, {})
            self.assertEqual(
                selected[0]["interrupted_resume_evidence"][
                    "checkpoint_progress"][method][
                        "completed_round_trips"],
                1)

            provider_post = mock.Mock(
                side_effect=AssertionError("committed RT was reposted"))
            with mock.patch.object(
                    experiment_runner,
                    "_require_formal_opencode_transport"), mock.patch.object(
                        experiment_runner, "load_sample",
                        return_value=(sample, out_dir, states)), mock.patch.object(
                            experiment_runner, "get_domain",
                            return_value=DummyDomain()), mock.patch.object(
                                experiment_runner,
                                "load_distractor_context",
                                return_value={}), mock.patch.object(
                                    experiment_runner,
                                    "register_task_plan",
                                    return_value={"sha256": plan_hash,
                                                  "round_trips": 1}), \
                    mock.patch.object(
                        experiment_runner, "_edit_step",
                        side_effect=AssertionError(
                            "committed RT must not be regenerated")):
                experiment_runner.run_relay(
                    method, sample_id, num_round_trips=1,
                    include_distractor=True, out_dir=out_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=provider_post, printing=False)
            provider_post.assert_not_called()

    def test_resume_selects_only_incomplete_sample_and_skips_committed_sample(self):
        methods_a = ["hybridpatch", "fullrewrite"]
        methods_b = ["fullrewrite", "hybridpatch"]
        with tempfile.TemporaryDirectory() as out_dir:
            evidence = self._write_infrastructure_fixture(
                out_dir, methods=methods_a)
            self._write_finished_fixture(out_dir, "sample-b", methods_b)
            assignments = [
                {"sample": "sample-a", "methods": methods_a},
                {"sample": "sample-b", "methods": methods_b},
            ]
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=1))
            self.assertEqual(
                [item["sample"] for item in selected], ["sample-a"])
            self.assertEqual(
                authorizations["sample-a"]["parent_semantic_call_id"],
                evidence["semantic_call_id"])
            self.assertEqual(
                authorizations["sample-a"]["semantic_root_id"],
                evidence["semantic_root_id"])
            self.assertEqual(
                authorizations["sample-a"]["generation_index"], 1)
            self.assertEqual(
                authorizations["sample-a"]["semantic_call_id"],
                f"{evidence['semantic_root_id']}/g001")
            self.assertEqual(
                authorizations["sample-a"]["next_attempt_index"], 3)
            provider_post = mock.Mock()
            for item in selected:
                if item["sample"] == "sample-b":
                    provider_post(item["sample"])
            provider_post.assert_not_called()

    def test_resume_real_g001_exhaustion_is_verified_then_g002_forbidden(self):
        sample = "sample-a"
        methods = ["hybridpatch", "fullrewrite"]
        worker_old, worker_new = "worker-old", "worker-new"
        old_pid, new_pid = 101, 202
        old_invocation, new_invocation = "inv-old", "inv-new"
        root = f"hybridpatch/{sample}/rt01/forward/hybridpatch_primary"
        g000, g001 = f"{root}/g000", f"{root}/g001"
        fingerprint = "fingerprint-recovery"
        progress = {
            method: {"completed_round_trips": 0, "committed_rows": 0}
            for method in methods
        }
        with tempfile.TemporaryDirectory() as out_dir:
            old_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            old_recorder.worker_launch_id = worker_old
            old_recorder.set_step(1, "forward", "target")
            self._append_exhausted_response_generation(
                old_recorder, g000, "failed-g000", fingerprint, (1, 2))
            new_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            new_recorder.worker_launch_id = worker_new
            new_recorder.set_step(1, "forward", "target")
            self._append_exhausted_response_generation(
                new_recorder, g001, "failed-g001", fingerprint, (3, 4),
                parent_semantic_call_id=g000)
            for row in (
                self._api_call_fixture_row(
                    root=root, exact_id=g000, request_id="failed-g000",
                    fingerprint=fingerprint, worker_id=worker_old,
                    worker_pid=old_pid, failure=True, http_attempts=2),
                self._api_call_fixture_row(
                    root=root, exact_id=g001, request_id="failed-g001",
                    fingerprint=fingerprint, worker_id=worker_new,
                    worker_pid=new_pid, parent=g000, failure=True,
                    http_attempts=4),
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            resume_authorization = {
                "parent_semantic_call_id": g000,
                "semantic_root_id": root,
                "semantic_call_id": g001,
                "generation_index": 1,
                "request_fingerprint": fingerprint,
                "next_attempt_index": 3,
            }
            for row in (
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": old_invocation,
                    "worker_launch_id": worker_old,
                    "worker_pid": old_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": None,
                },
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": new_invocation,
                    "worker_launch_id": worker_new,
                    "worker_pid": new_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": (
                        paired_dispatch._canonical_resume_authorization(
                            resume_authorization)),
                },
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), row)
            for created_at, worker, pid, invocation, exact_id, parent, \
                    request_id, generation, next_attempt in (
                        (
                            "2026-07-18T00:00:00+08:00",
                            worker_old, old_pid, old_invocation, g000, None,
                            "failed-g000", 0, 3,
                        ),
                        (
                            "2026-07-18T00:00:01+08:00",
                            worker_new, new_pid, new_invocation, g001, g000,
                            "failed-g001", 1, 5,
                        ),
                    ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "sample_outcomes.jsonl"), {
                        "schema": "anchorpatch.sample_outcome/1",
                        "created_at": created_at,
                        "sample": sample,
                        "status": "infrastructure_incomplete",
                        "worker_launch_id": worker,
                        "worker_pid": pid,
                        "invocation_id": invocation,
                        "methods": methods,
                        "method": "hybridpatch",
                        "rt_index": 1,
                        "direction": "forward",
                        "call_kind": "hybridpatch_primary",
                        "semantic_root_id": root,
                        "semantic_call_id": exact_id,
                        "request_id": request_id,
                        "generation_index": generation,
                        "parent_semantic_call_id": parent,
                        "request_fingerprint": fingerprint,
                        "error_type": "incomplete_stream",
                        "classification": "provider/API failure",
                        "response_slots_used": 2,
                        "transient_failure_count": 0,
                        "http_attempts_used": next_attempt - 1,
                        "next_attempt_index": next_attempt,
                        "transport_recovery_index": generation,
                        "checkpoint_progress": progress,
                    })

            evidence = paired_dispatch._verified_infrastructure_incomplete(
                out_dir, sample, {
                    "worker_launch_id": worker_new,
                    "worker_pid": new_pid,
                    "methods": methods,
                    "target_round_trips": 1,
                })
            self.assertEqual(evidence["generation_index"], 1)
            self.assertEqual(evidence["next_attempt_index"], 5)
            with self.assertRaisesRegex(RuntimeError, "g002\\+ is forbidden"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, [{"sample": sample, "methods": methods}],
                    resume=True, target_round_trips=1)

    def test_later_g000_exhaustion_allows_consumed_resume_metadata(self):
        sample = "sample-a"
        methods = ["hybridpatch", "fullrewrite"]
        worker_old, worker_new = "worker-old", "worker-new"
        old_pid, new_pid = 101, 202
        old_invocation, new_invocation = "inv-old", "inv-new"
        recovered_root = (
            f"hybridpatch/{sample}/rt01/forward/hybridpatch_primary")
        recovered_g000 = f"{recovered_root}/g000"
        recovered_g001 = f"{recovered_root}/g001"
        backward_root = (
            f"hybridpatch/{sample}/rt01/backward/hybridpatch_primary")
        backward_g000 = f"{backward_root}/g000"
        later_root = f"hybridpatch/{sample}/rt02/forward/hybridpatch_primary"
        later_g000 = f"{later_root}/g000"
        recovered_fp = "fingerprint-recovered"
        backward_fp = "fingerprint-backward"
        later_fp = "fingerprint-later"
        with tempfile.TemporaryDirectory() as out_dir:
            old_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            old_recorder.worker_launch_id = worker_old
            old_recorder.set_step(1, "forward", "target")
            self._append_exhausted_response_generation(
                old_recorder, recovered_g000, "failed-g000",
                recovered_fp, (1, 2))
            new_recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", sample, None, "minimax-m3",
                mock.Mock())
            new_recorder.worker_launch_id = worker_new
            new_recorder.set_step(1, "forward", "target")
            self._append_successful_response_generation(
                new_recorder, recovered_g001, "recovered-g001",
                recovered_fp, 3, parent_semantic_call_id=recovered_g000)
            self._write_success_journal(
                out_dir, root=recovered_root, exact_id=recovered_g001,
                request_id="recovered-g001", fingerprint=recovered_fp,
                parent=recovered_g000, http_attempts=3)
            new_recorder.set_step(1, "backward", "target")
            self._append_successful_response_generation(
                new_recorder, backward_g000, "backward-g000",
                backward_fp, 1)
            self._write_success_journal(
                out_dir, root=backward_root, exact_id=backward_g000,
                request_id="backward-g000", fingerprint=backward_fp,
                http_attempts=1)
            new_recorder.set_step(2, "forward", "target-2")
            self._append_exhausted_response_generation(
                new_recorder, later_g000, "failed-later-g000",
                later_fp, (1, 2))
            for row in (
                self._api_call_fixture_row(
                    root=recovered_root, exact_id=recovered_g000,
                    request_id="failed-g000", fingerprint=recovered_fp,
                    worker_id=worker_old, worker_pid=old_pid,
                    failure=True, http_attempts=2),
                self._api_call_fixture_row(
                    root=recovered_root, exact_id=recovered_g001,
                    request_id="recovered-g001", fingerprint=recovered_fp,
                    worker_id=worker_new, worker_pid=new_pid,
                    parent=recovered_g000, http_attempts=3),
                self._api_call_fixture_row(
                    root=backward_root, exact_id=backward_g000,
                    request_id="backward-g000", fingerprint=backward_fp,
                    worker_id=worker_new, worker_pid=new_pid,
                    http_attempts=1),
                self._api_call_fixture_row(
                    root=later_root, exact_id=later_g000,
                    request_id="failed-later-g000", fingerprint=later_fp,
                    worker_id=worker_new, worker_pid=new_pid,
                    failure=True, http_attempts=2),
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            resume_authorization = {
                "parent_semantic_call_id": recovered_g000,
                "semantic_root_id": recovered_root,
                "semantic_call_id": recovered_g001,
                "generation_index": 1,
                "request_fingerprint": recovered_fp,
                "next_attempt_index": 3,
            }
            for row in (
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": old_invocation,
                    "worker_launch_id": worker_old,
                    "worker_pid": old_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": None,
                },
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": new_invocation,
                    "worker_launch_id": worker_new,
                    "worker_pid": new_pid,
                    "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "transport_resume_authorization": (
                        paired_dispatch._canonical_resume_authorization(
                            resume_authorization)),
                },
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), row)
            self._write_completed_method_prefix(
                out_dir, sample, "hybridpatch", 1)
            progress = {
                "hybridpatch": {
                    "completed_round_trips": 1,
                    "committed_rows": 2,
                },
                "fullrewrite": {
                    "completed_round_trips": 0,
                    "committed_rows": 0,
                },
            }
            for created_at, worker, pid, invocation, root, exact_id, \
                    request_id, fingerprint, rt, next_attempt in (
                        (
                            "2026-07-18T00:00:00+08:00",
                            worker_old, old_pid, old_invocation,
                            recovered_root, recovered_g000, "failed-g000",
                            recovered_fp, 1, 3,
                        ),
                        (
                            "2026-07-18T00:00:02+08:00",
                            worker_new, new_pid, new_invocation,
                            later_root, later_g000, "failed-later-g000",
                            later_fp, 2, 3,
                        ),
                    ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "sample_outcomes.jsonl"), {
                        "schema": "anchorpatch.sample_outcome/1",
                        "created_at": created_at,
                        "sample": sample,
                        "status": "infrastructure_incomplete",
                        "worker_launch_id": worker,
                        "worker_pid": pid,
                        "invocation_id": invocation,
                        "methods": methods,
                        "method": "hybridpatch",
                        "rt_index": rt,
                        "direction": "forward",
                        "call_kind": "hybridpatch_primary",
                        "semantic_root_id": root,
                        "semantic_call_id": exact_id,
                        "request_id": request_id,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "request_fingerprint": fingerprint,
                        "error_type": "incomplete_stream",
                        "classification": "provider/API failure",
                        "response_slots_used": 2,
                        "transient_failure_count": 0,
                        "http_attempts_used": next_attempt - 1,
                        "next_attempt_index": next_attempt,
                        "transport_recovery_index": 0,
                        "checkpoint_progress": (
                            {
                                method: {
                                    "completed_round_trips": 0,
                                    "committed_rows": 0,
                                }
                                for method in methods
                            }
                            if worker == worker_old else progress
                        ),
                    })

            process = mock.Mock(pid=new_pid)
            process.poll.return_value = 7
            sibling_process = mock.Mock(pid=303)
            sibling_process.poll.return_value = None
            running = {
                sample: {
                    "sample": sample,
                    "process": process,
                    "log": mock.Mock(),
                    "key_label": "KEY_01",
                    "worker_launch_id": worker_new,
                    "methods": methods,
                    "target_round_trips": 2,
                    "exit_recorded": False,
                },
                "sample-b": {
                    "sample": "sample-b",
                    "process": sibling_process,
                    "log": mock.Mock(),
                    "key_label": "KEY_02",
                    "worker_launch_id": "worker-sibling",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "target_round_trips": 2,
                    "exit_recorded": False,
                },
            }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            disposition = paired_dispatch._record_worker_exit(
                out_dir, running, sample, running[sample], 7, dispatch_log)
            self.assertEqual(disposition, "infrastructure_incomplete")
            self.assertNotIn(sample, running)
            self.assertIn("sample-b", running)
            provider_post = mock.Mock(
                side_effect=AssertionError(
                    "sibling/committed sample must not be posted here"))
            for item in running.values():
                if item["sample"] == sample:
                    provider_post(item["sample"])
            provider_post.assert_not_called()

            sibling_methods = ["fullrewrite", "hybridpatch"]
            sibling_progress = {}
            for method in sibling_methods:
                self._write_completed_method_prefix(
                    out_dir, "sample-b", method, 2)
                sibling_progress[method] = {
                    "completed_round_trips": 2,
                    "committed_rows": 4,
                }
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "sample_outcomes.jsonl"), {
                    "schema": "anchorpatch.sample_outcome/1",
                    "created_at": "2026-07-18T00:00:03+08:00",
                    "sample": "sample-b",
                    "status": "finished",
                    "worker_launch_id": "worker-sibling",
                    "worker_pid": 303,
                    "invocation_id": "inv-sibling",
                    "methods": sibling_methods,
                    "checkpoint_progress": sibling_progress,
                })
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir,
                    [
                        {"sample": sample, "methods": methods},
                        {
                            "sample": "sample-b",
                            "methods": sibling_methods,
                        },
                    ],
                    resume=True, target_round_trips=2))
            self.assertEqual([item["sample"] for item in selected], [sample])
            self.assertEqual(
                authorizations[sample]["parent_semantic_call_id"],
                later_g000)

    def test_confirmation_resume_selects_pristine_unstarted_wave_only(self):
        finished_methods = ["hybridpatch", "fullrewrite"]
        pending_methods = ["fullrewrite", "hybridpatch"]
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_finished_fixture(
                out_dir, "sample-finished", finished_methods)
            utils_relay_plan.save_relay_task_plan(
                os.path.join(out_dir, "sample-pending.task_plan.json"),
                ["target"],
            )
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"), {
                    "event": "wave_start", "wave_index": 2,
                    "samples": ["sample-pending"],
                })
            lease_path = paired_dispatch._worker_lease_path(
                out_dir, "sample-pending")
            os.makedirs(os.path.dirname(lease_path), exist_ok=True)
            open(lease_path, "a", encoding="utf-8").close()
            assignments = [
                {"sample": "sample-finished", "methods": finished_methods},
                {"sample": "sample-pending", "methods": pending_methods},
            ]
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=1,
                    allow_pristine_pending=True,
                ))
            self.assertEqual(
                [item["sample"] for item in selected], ["sample-pending"])
            self.assertEqual(authorizations, {})

            provider_post = mock.Mock(
                side_effect=AssertionError(
                    "a committed sample must never produce a provider POST"))
            for item in selected:
                if item["sample"] == "sample-finished":
                    provider_post(item["sample"])
            provider_post.assert_not_called()

            with self.assertRaisesRegex(RuntimeError, "missing outcomes"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=1,
                )

    def test_confirmation_resume_rejects_pending_with_execution_evidence(self):
        assignments = [{
            "sample": "sample-pending",
            "methods": ["hybridpatch", "fullrewrite"],
        }]

        def write_result(out_dir):
            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            open(
                os.path.join(method_dir, "sample-pending.jsonl"),
                "w", encoding="utf-8",
            ).close()

        def write_metadata(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "run_metadata.jsonl"),
                {"samples": ["sample-pending"], "status": "running"},
            )

        def write_api_call(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"),
                {"sample": "sample-pending", "provider_called": True},
            )

        def write_attempt(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                {
                    "semantic_call_id": (
                        "hybridpatch/sample-pending/rt01/forward/"
                        "hybridpatch_primary/g000"
                    ),
                    "event": "attempt_start",
                },
            )

        def write_dispatch_launch(out_dir):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"),
                {"event": "launch", "sample": "sample-pending"},
            )

        def write_journal(out_dir):
            semantic = (
                "hybridpatch/sample-pending/rt01/forward/"
                "hybridpatch_primary/g000"
            )
            digest = hashlib.sha256(semantic.encode("utf-8")).hexdigest()[:24]
            journal_dir = os.path.join(out_dir, "api_journal")
            os.makedirs(journal_dir, exist_ok=True)
            run_meta.write_json_atomic(
                os.path.join(journal_dir, f"{digest}.response.json"), {
                    "schema": paired_dispatch.API_RESPONSE_JOURNAL_SCHEMA,
                    "semantic_call_id": semantic,
                })

        writers = {
            "result": write_result,
            "metadata": write_metadata,
            "api_call": write_api_call,
            "attempt": write_attempt,
            "dispatch_launch": write_dispatch_launch,
            "journal": write_journal,
        }
        for label, writer in writers.items():
            with self.subTest(evidence=label), \
                    tempfile.TemporaryDirectory() as out_dir:
                writer(out_dir)
                with self.assertRaisesRegex(RuntimeError, "never started"):
                    paired_dispatch._select_invocation_assignments(
                        out_dir, assignments, resume=True,
                        target_round_trips=10,
                        allow_pristine_pending=True,
                    )

    def test_resume_rejects_g002_after_g001_exhaustion(self):
        assignments = [{
            "sample": "sample-a",
            "methods": ["hybridpatch", "fullrewrite"],
        }]
        evidence = {
            "generation_index": 1,
            "semantic_call_id": (
                "hybridpatch/sample-a/rt01/forward/"
                "hybridpatch_primary/g001"
            ),
            "semantic_root_id": (
                "hybridpatch/sample-a/rt01/forward/"
                "hybridpatch_primary"
            ),
            "request_fingerprint": "fingerprint",
            "next_attempt_index": 4,
            "invocation_id": "invocation",
        }
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_latest_sample_outcomes",
                    return_value={
                        "sample-a": {"status": "infrastructure_incomplete"}
                    }), \
                mock.patch.object(
                    paired_dispatch, "_verified_infrastructure_incomplete",
                    return_value=evidence):
            with self.assertRaisesRegex(
                    RuntimeError, "g002\\+ is forbidden"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=10)

    def test_paired_dispatch_preflight_allows_missing_new_checkpoints(self):
        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": ["sample"],
                    "method_set": ["fullrewrite", "hybridpatch"],
                    "num_round_trips": 1,
                },
                "task_plans": {
                    "sample": {
                        "path": "sample.task_plan.json",
                        "sha256": paired_dispatch._sha256(plan_path),
                        "forward_state_sequence": ["state_a"],
                    },
                },
            }
            with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean"),
            ):
                paired_dispatch._write_active_worker_set(
                    out_dir, manifest, [])
                preflight = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                completion = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)

            self.assertEqual(preflight["errors"], [])
            self.assertTrue(any(
                "incomplete task" in error
                for error in completion["errors"]
            ))

    def test_dispatch_inspection_allows_only_proven_active_open_attempt(self):
        def build_fixture(out_dir, *, ledger_worker="worker-a",
                          active_worker="worker-a", include_launch=True):
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": ["sample"],
                    "method_set": ["hybridpatch"],
                    "num_round_trips": 1,
                },
                "task_plans": {
                    "sample": {
                        "path": "sample.task_plan.json",
                        "sha256": paired_dispatch._sha256(plan_path),
                        "forward_state_sequence": ["state_a"],
                    },
                },
            }
            paired_dispatch.write_json_atomic(
                paired_dispatch._active_worker_set_path(out_dir), {
                    "schema": "anchorpatch.active_worker_set/1",
                    "run_git_commit": "1" * 40,
                    "workers": {
                        active_worker: {"sample": "sample"},
                    },
                })
            if include_launch:
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "dispatch_log.jsonl"), {
                        "event": "launch",
                        "worker_launch_id": "worker-a",
                        "sample": "sample", "pid": 101,
                    })
            semantic_root = (
                "hybridpatch/sample/rt01/forward/hybridpatch_primary")
            semantic = f"{semantic_root}/g000"
            for event, fields in (
                ("semantic_request", {
                    "call_id": "live-call",
                    "call_kind": "hybridpatch_primary",
                    "request_fingerprint": "live-fingerprint",
                }),
                ("attempt_start", {
                    "call_id": "live-call", "attempt_index": 1,
                    "call_kind": "hybridpatch_primary",
                    "attempt_kind": "transport_initial",
                    "request_fingerprint": "live-fingerprint",
                }),
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"), {
                        "schema": "anchorpatch.api_attempt/4",
                        "step_id": "hybridpatch/sample/rt01/forward",
                        "semantic_root_id": semantic_root,
                        "semantic_call_id": semantic,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "worker_launch_id": ledger_worker,
                        "event": event,
                        **fields,
                    })
            return manifest

        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")):
            with tempfile.TemporaryDirectory() as out_dir:
                manifest = build_fixture(out_dir)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                self.assertEqual(inspection["errors"], [])

            with tempfile.TemporaryDirectory() as out_dir:
                manifest = build_fixture(out_dir)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples=set())
                self.assertTrue(any(
                    "unclosed HTTP attempt" in error
                    for error in inspection["errors"]), inspection["errors"])

            with tempfile.TemporaryDirectory() as out_dir:
                manifest = build_fixture(
                    out_dir, ledger_worker="worker-b",
                    active_worker="worker-b")
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                self.assertTrue(any(
                    "attempt worker provenance mismatch" in error
                    for error in inspection["errors"]), inspection["errors"])
