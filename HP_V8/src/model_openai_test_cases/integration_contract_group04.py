"""Mixin slice for test_model_openai.IntegrationContractGroup04Mixin."""

from .support import *


class IntegrationContractGroup04Mixin:
    def test_campaign_stop_latch_is_first_writer_wins_and_blocks_work(self):
        with tempfile.TemporaryDirectory() as out_dir:
            first = run_meta.record_campaign_stop_condition(
                out_dir, "preservation_violation",
                sample="sample", result_committed=False,
            )
            second = run_meta.record_campaign_stop_condition(
                out_dir, "git_identity_drift", sample="other")
            self.assertEqual(second, first)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [first])
            with self.assertRaises(ValueError):
                run_meta.record_campaign_stop_condition(
                    out_dir, "x", schema="override")

            provider_calls = []
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "offline-test-model",
                lambda *_args, **_kwargs: provider_calls.append(1),
            )
            recorder.set_step(1, "forward", "target")
            for call_kind in ("hybridpatch_primary", "hybridpatch_repair"):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    recorder.generate(
                        [], model="offline-test-model",
                        call_kind=call_kind,
                    )
            self.assertEqual(provider_calls, [])

            kwargs = run_metadata_kwargs(distractor=True)
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint", return_value={"x": "y"}):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            self.assertEqual(
                run_meta.read_run_metadata_snapshot(out_dir), [])

        with tempfile.TemporaryDirectory() as out_dir:
            with open(
                os.path.join(out_dir, "campaign_stop.json"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write("{broken")
            with self.assertRaisesRegex(RuntimeError, "invalid campaign stop"):
                run_meta.read_campaign_stop_conditions(out_dir)
            with self.assertRaisesRegex(RuntimeError, "invalid campaign stop"):
                run_meta.record_campaign_stop_condition(out_dir, "new")

    def test_runtime_guard_never_calls_git_or_reads_task_plan(self):
        with tempfile.TemporaryDirectory() as out_dir:
            active_path = os.path.join(out_dir, "active_worker_set.json")
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "workers": {"worker-a": {"sample": "sample"}},
            })
            utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
            blocked_plan = os.path.abspath(plan_path)
            plan_reads = []
            real_open = open

            def guarded_open(path, *args, **kwargs):
                try:
                    candidate = os.path.abspath(os.fspath(path))
                except TypeError:
                    candidate = None
                if candidate == blocked_plan:
                    plan_reads.append((path, args, kwargs))
                    raise AssertionError(
                        "runtime guard must not read task-plan bytes")
                return real_open(path, *args, **kwargs)

            env = {
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
                "ANCHORPATCH_ACTIVE_WORKER_SET_PATH": active_path,
                "ANCHORPATCH_EXPECTED_GIT_COMMIT": "1" * 40,
                "ANCHORPATCH_EXPECTED_GIT_TREE_STATE": "clean",
                "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256": "0" * 64,
                "ANCHORPATCH_EXPECTED_TASK_PLAN_PATH": plan_path,
            }
            with mock.patch.dict(os.environ, env, clear=False), \
                    mock.patch.object(
                        run_meta, "_git_identity_details",
                        side_effect=AssertionError(
                            "runtime guard must not call Git")), \
                    mock.patch.object(
                        run_meta, "_git_identity",
                        side_effect=AssertionError(
                            "runtime guard must not call Git")), \
                    mock.patch("builtins.open", side_effect=guarded_open):
                run_meta.enforce_campaign_runtime_guards(out_dir, "sample")

            self.assertEqual(plan_reads, [])
            self.assertEqual(run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_stop_latched_during_transport_preflight_blocks_provider_post(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", provider)
            recorder.set_step(1, "forward", "target")

            def latch_after_ledger_preflight(*_args, **_kwargs):
                run_meta.record_campaign_stop_condition(
                    out_dir, "sibling_integrity_failure",
                    sample="sibling")
                return None, None

            with mock.patch.object(
                    recorder, "_find_lineage_journal",
                    side_effect=latch_after_ledger_preflight):
                with self.assertRaises(run_meta.CampaignStoppedError):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3",
                        call_kind="hybridpatch_primary")

            provider.assert_not_called()
            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            self.assertFalse(any(
                row.get("event") == "attempt_start" for row in ledger))

    def test_stop_latched_during_backoff_blocks_retry_provider_post(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider_posts = []
            first_failure = model_openai._HTTPStatusError(
                503, "temporary service failure")

            def fake_transport_call(*_args, **kwargs):
                attempt_index = kwargs["attempt_index"]
                sink = kwargs["raw_event_sink"]
                sink({
                    "record_type": "attempt_start",
                    "attempt_index": attempt_index,
                    "attempt_kind": (
                        "transport_initial" if attempt_index == 1
                        else "transport_retry"),
                })
                # The SDK emits attempt_start immediately before opening the
                # stream; reaching here represents an actual provider POST.
                provider_posts.append(attempt_index)
                if attempt_index == 1:
                    attempt = {
                        "attempt_index": 1,
                        **self._failed_attempt_end_fields(
                            generation_delta_seen=False,
                            error_type="server_error"),
                        "http_status": 503,
                    }
                    first_failure._opencode_attempt = attempt
                    sink({"record_type": "attempt_end", "attempt": attempt})
                    raise first_failure
                raise AssertionError("retry POST must be stopped by latch")

            def latch_during_backoff(_delay):
                run_meta.record_campaign_stop_condition(
                    out_dir, "sibling_integrity_failure",
                    sample="sibling")

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                model_openai.OpenAI_Model().generate)
            recorder.set_step(1, "forward", "target")
            with mock.patch.object(
                    model_openai, "_call_opencode_messages",
                    side_effect=fake_transport_call) as transport_call, \
                    mock.patch.object(
                        model_openai.time, "sleep",
                        side_effect=latch_during_backoff):
                with self.assertRaises(model_openai.OpenCodeTransportError):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3", return_metadata=True,
                        call_kind="hybridpatch_primary")

            self.assertEqual(transport_call.call_count, 2)
            self.assertEqual(provider_posts, [1])
            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            self.assertEqual(
                [row.get("event") for row in ledger].count(
                    "attempt_start"),
                1)
            self.assertFalse(any(
                row.get("event") == "response_committed" for row in ledger))

    def test_stop_latch_wins_commit_ordering_lock_with_zero_relay_commit(self):
        with tempfile.TemporaryDirectory() as out_dir:
            result_path = os.path.join(out_dir, "hybridpatch", "sample.jsonl")
            checkpoint_path = os.path.join(
                out_dir, "hybridpatch", "sample.ckpt.json")
            rows = [
                {"round_trip_num": 1,
                 "round_trip_direction": "forward"},
                {"round_trip_num": 1,
                 "round_trip_direction": "backward"},
            ]
            checkpoint = {"completed_round_trips": 1}
            stop_has_ordering_lock = threading.Event()
            allow_stop_write = threading.Event()
            stop_errors = []
            commit_errors = []
            original_write = run_meta.write_json_atomic

            def blocked_stop_write(path, payload):
                if os.path.basename(path) == "campaign_stop.json":
                    stop_has_ordering_lock.set()
                    if not allow_stop_write.wait(5):
                        raise RuntimeError("unit-test stop barrier timed out")
                return original_write(path, payload)

            def stop_worker():
                try:
                    run_meta.record_campaign_stop_condition(
                        out_dir, "sibling_integrity_failure")
                except BaseException as exc:
                    stop_errors.append(exc)

            def commit_worker():
                try:
                    run_meta.append_relay_rows_and_checkpoint(
                        result_path, checkpoint_path, rows, checkpoint,
                        campaign_out_dir=out_dir)
                except BaseException as exc:
                    commit_errors.append(exc)

            with mock.patch.object(
                    run_meta, "write_json_atomic",
                    side_effect=blocked_stop_write):
                stopper = threading.Thread(target=stop_worker, daemon=True)
                stopper.start()
                self.assertTrue(stop_has_ordering_lock.wait(5))
                committer = threading.Thread(target=commit_worker, daemon=True)
                committer.start()
                self.assertTrue(committer.is_alive())
                allow_stop_write.set()
                stopper.join(5)
                committer.join(5)

            self.assertFalse(stopper.is_alive())
            self.assertFalse(committer.is_alive())
            self.assertEqual(stop_errors, [])
            self.assertEqual(len(commit_errors), 1)
            self.assertIsInstance(
                commit_errors[0], run_meta.CampaignStoppedError)
            self.assertFalse(os.path.exists(result_path))
            self.assertFalse(os.path.exists(checkpoint_path))
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir)[0][
                    "condition"],
                "sibling_integrity_failure")

    def test_dispatch_worker_barrier_closes_lease_metadata_pid_and_plan(self):
        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
            plan_sha = paired_dispatch._sha256(plan_path)
            task_plans = {
                "sample": {
                    "path": "sample.task_plan.json",
                    "sha256": plan_sha,
                    "forward_state_sequence": ["target"],
                },
            }
            worker_id = "worker-a"
            ready_path, ack_path = paired_dispatch._worker_barrier_paths(
                out_dir, worker_id)
            invocation_id = "invocation-a"
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                [{
                    "invocation_id": invocation_id,
                    "status": "running",
                    "worker_launch_id": worker_id,
                    "worker_pid": 101,
                    "samples": ["sample"],
                    "task_plans": {
                        "sample": {
                            "sha256": plan_sha,
                            "round_trips": 1,
                        },
                    },
                }],
            )
            run_meta.write_json_atomic(ready_path, {
                "schema": "anchorpatch.worker_ready/1",
                "worker_launch_id": worker_id,
                "worker_pid": 101,
                "invocation_id": invocation_id,
                "sample": "sample",
                "task_plan_path": os.path.abspath(plan_path),
                "task_plan_sha256": plan_sha,
            })
            process = mock.Mock(pid=101)
            process.poll.return_value = None
            running = {
                "sample": {
                    "process": process,
                    "worker_launch_id": worker_id,
                    "ready_path": ready_path,
                    "ack_path": ack_path,
                },
            }
            lease_path = paired_dispatch._worker_lease_path(
                out_dir, "sample")
            os.makedirs(os.path.dirname(lease_path), exist_ok=True)
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    paired_dispatch._authorize_workers(
                        out_dir, running, task_plans, dispatch_log, 1.0)
                finally:
                    portalocker.unlock(lease)
            with open(ack_path, encoding="utf-8") as handle:
                ack = json.load(handle)
            self.assertEqual(ack["invocation_id"], invocation_id)
            self.assertEqual(ack["worker_pid"], 101)
            events = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(events[-1]["event"], "worker_authorized")
            os.remove(ack_path)
            with open(lease_path, "a+", encoding="utf-8") as lease, \
                    mock.patch.object(
                        paired_dispatch, "append_jsonl_records_locked",
                        side_effect=OSError("fsync failed")):
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaisesRegex(OSError, "fsync failed"):
                        paired_dispatch._authorize_workers(
                            out_dir, running, task_plans,
                            dispatch_log, 1.0)
                finally:
                    portalocker.unlock(lease)
            self.assertFalse(os.path.exists(ack_path))

    def test_dispatch_cohort_authorization_is_durable_before_any_ack(self):
        with tempfile.TemporaryDirectory() as out_dir:
            task_plans = {}
            running = {}
            metadata = []
            ack_paths = []
            for index, sample in enumerate(("sample-a", "sample-b"), 1):
                plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
                utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
                plan_sha = paired_dispatch._sha256(plan_path)
                task_plans[sample] = {
                    "path": os.path.basename(plan_path),
                    "sha256": plan_sha,
                    "forward_state_sequence": ["target"],
                }
                worker_id = f"worker-{index}"
                invocation_id = f"invocation-{index}"
                pid = 100 + index
                ready_path, ack_path = paired_dispatch._worker_barrier_paths(
                    out_dir, worker_id)
                run_meta.write_json_atomic(ready_path, {
                    "schema": "anchorpatch.worker_ready/1",
                    "worker_launch_id": worker_id,
                    "worker_pid": pid,
                    "invocation_id": invocation_id,
                    "sample": sample,
                    "task_plan_path": os.path.abspath(plan_path),
                    "task_plan_sha256": plan_sha,
                })
                process = mock.Mock(pid=pid)
                process.poll.return_value = None
                running[sample] = {
                    "process": process,
                    "worker_launch_id": worker_id,
                    "ready_path": ready_path,
                    "ack_path": ack_path,
                }
                ack_paths.append(ack_path)
                metadata.append({
                    "invocation_id": invocation_id,
                    "status": "running",
                    "worker_launch_id": worker_id,
                    "worker_pid": pid,
                    "samples": [sample],
                    "task_plans": {
                        sample: {
                            "sha256": plan_sha,
                            "round_trips": 1,
                        },
                    },
                })
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"), metadata)

            leases = []
            try:
                for sample in running:
                    lease_path = paired_dispatch._worker_lease_path(
                        out_dir, sample)
                    os.makedirs(os.path.dirname(lease_path), exist_ok=True)
                    lease = open(lease_path, "a+", encoding="utf-8")
                    portalocker.lock(
                        lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                    leases.append(lease)
                dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
                paired_dispatch._authorize_workers(
                    out_dir, running, task_plans, dispatch_log, 1.0)
                authorization_rows = run_meta._read_jsonl_records_with_retry(
                    dispatch_log)
                self.assertEqual(
                    [row["sample"] for row in authorization_rows],
                    ["sample-a", "sample-b"],
                )
                self.assertTrue(all(
                    os.path.isfile(path) for path in ack_paths))
                for path in ack_paths:
                    os.remove(path)
                os.remove(dispatch_log)

                with mock.patch.object(
                        paired_dispatch, "append_jsonl_records_locked",
                        side_effect=OSError("cohort fsync failed")
                ) as append:
                    with self.assertRaisesRegex(
                            OSError, "cohort fsync failed"):
                        paired_dispatch._authorize_workers(
                            out_dir, running, task_plans,
                            dispatch_log,
                            1.0,
                        )
                    self.assertEqual(append.call_count, 1)
                    batched_rows = append.call_args.args[1]
                    self.assertEqual(
                        [row["sample"] for row in batched_rows],
                        ["sample-a", "sample-b"],
                    )
            finally:
                for lease in leases:
                    portalocker.unlock(lease)
                    lease.close()
            self.assertTrue(all(
                not os.path.exists(path) for path in ack_paths
            ))

    def test_runner_waits_for_exact_dispatch_authorization_before_api(self):
        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
            plan_sha = paired_dispatch._sha256(plan_path)
            invocation_id = "invocation-a"
            worker_id = "worker-a"
            active_path = paired_dispatch._active_worker_set_path(out_dir)
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "run_git_commit": "1" * 40,
                "workers": {worker_id: {"sample": "sample"}},
            })
            ready_path, ack_path = paired_dispatch._worker_barrier_paths(
                out_dir, worker_id)
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                [{
                    "invocation_id": invocation_id,
                    "status": "running",
                    "worker_launch_id": worker_id,
                    "worker_pid": os.getpid(),
                    "samples": ["sample"],
                    "task_plans": {},
                }],
            )
            run_meta.write_json_atomic(ack_path, {
                "schema": "anchorpatch.worker_start/1",
                "worker_launch_id": worker_id,
                "worker_pid": os.getpid(),
                "invocation_id": invocation_id,
                "sample": "sample",
                "task_plan_sha256": plan_sha,
            })
            environment = {
                "ANCHORPATCH_WORKER_LAUNCH_ID": worker_id,
                "ANCHORPATCH_WORKER_READY_PATH": ready_path,
                "ANCHORPATCH_WORKER_ACK_PATH": ack_path,
                "ANCHORPATCH_ACTIVE_WORKER_SET_PATH": active_path,
                "ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256": plan_sha,
                "ANCHORPATCH_START_BARRIER_TIMEOUT": "1",
            }
            with mock.patch.dict(os.environ, environment, clear=False):
                ready = experiment_runner._dispatch_worker_start_barrier(
                    out_dir, ["sample"], 1, invocation_id)
            self.assertEqual(ready["task_plan_sha256"], plan_sha)
            with open(ready_path, encoding="utf-8") as handle:
                saved_ready = json.load(handle)
            self.assertEqual(saved_ready["worker_pid"], os.getpid())
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(
                snapshot[0]["task_plans"]["sample"]["sha256"], plan_sha)

    def test_worker_authorization_retries_transient_active_set_read(self):
        with tempfile.TemporaryDirectory() as out_dir:
            worker_id = "worker-a"
            active_path = os.path.join(out_dir, "active_worker_set.json")
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "workers": {worker_id: {"sample": "sample"}},
            })
            real_open = open
            reads = []

            def flaky_open(path, *args, **kwargs):
                if os.path.abspath(path) == os.path.abspath(active_path):
                    reads.append(path)
                    if len(reads) == 1:
                        error = PermissionError(
                            13, "transient sharing violation")
                        error.winerror = 5
                        raise error
                return real_open(path, *args, **kwargs)

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_WORKER_LAUNCH_ID": worker_id,
                "ANCHORPATCH_ACTIVE_WORKER_SET_PATH": active_path,
            }, clear=False), mock.patch(
                "builtins.open", side_effect=flaky_open
            ), mock.patch.object(run_meta.time, "sleep") as sleep:
                run_meta.enforce_active_worker_authorization(
                    out_dir, "sample")
            self.assertEqual(len(reads), 2)
            sleep.assert_called_once_with(0.05)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_standalone_runner_cannot_race_paired_dispatcher_lock(self):
        with tempfile.TemporaryDirectory() as out_dir:
            argv = [
                "experiment_runner.py", "--out_dir", out_dir,
            ]
            lock_path = os.path.join(out_dir, ".paired_dispatch.lock")
            with open(lock_path, "a+", encoding="utf-8") as lease, \
                    mock.patch.object(sys, "argv", argv), \
                    mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(experiment_runner, "main") as main:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaisesRegex(
                            RuntimeError, "dispatcher already owns"):
                        experiment_runner._run_cli_with_worker_lease()
                finally:
                    portalocker.unlock(lease)
                main.assert_not_called()
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(
                        experiment_runner, "main", return_value=17) as main:
                self.assertEqual(
                    experiment_runner._run_cli_with_worker_lease(), 17)
                main.assert_called_once_with()

    def test_smoke_cost_gate_enforces_complete_usage_and_fifty_usd_limit(self):
        commit = "1" * 40

        def make_smoke(smoke_dir, total_usd):
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": commit,
                "git_tree_state": "clean",
                "code_fingerprint": {"x": "y"},
                "config": {
                    "campaign_role": "smoke",
                    "samples": paired_dispatch.SMOKE_SAMPLES,
                    "method_set": ["fullrewrite", "hybridpatch"],
                    "num_round_trips": 2,
                    "seed": 42,
                    "model": "minimax-m3",
                    "max_tokens": 131072,
                    "distractor": True,
                    "opencode_transport": "anthropic_sdk_v2",
                    "minimax_transport": "opencode",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "run_metadata_storage": (
                        run_meta.RUN_METADATA_STORAGE_EVENT_V1),
                    "stop_on_preservation_violation": True,
                },
                "task_plans": {},
            }
            run_meta.write_json_atomic(
                os.path.join(smoke_dir, "dispatch_manifest.json"), manifest)
            per_row = total_usd / paired_dispatch.SMOKE_GRID_STEPS
            for sample in paired_dispatch.SMOKE_SAMPLES:
                for method in ("fullrewrite", "hybridpatch"):
                    method_dir = os.path.join(smoke_dir, method)
                    os.makedirs(method_dir, exist_ok=True)
                    with open(
                        os.path.join(method_dir, f"{sample}.jsonl"),
                        "w", encoding="utf-8",
                    ) as handle:
                        for rt in (1, 2):
                            for direction in ("forward", "backward"):
                                handle.write(json.dumps({
                                    "sample_id": sample,
                                    "method": method,
                                    "round_trip_num": rt,
                                    "round_trip_direction": direction,
                                    "total_usd": per_row,
                                }) + "\n")

        with tempfile.TemporaryDirectory() as smoke_dir, \
                tempfile.TemporaryDirectory() as main_dir, \
                mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(commit, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"x": "y"}), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={
                        "errors": [], "preservation_violations": 0,
                    }), \
                mock.patch.object(
                    paired_dispatch, "read_quiescent_run_metadata_snapshot",
                    return_value=[{"status": "finished"}]):
            make_smoke(smoke_dir, 2.0)
            gate = paired_dispatch.evaluate_smoke_cost_gate(
                smoke_dir, main_dir)
            self.assertEqual(gate["decision"], "GO")
            self.assertEqual(gate["projected_main_usd"], 50.0)
            with open(
                os.path.join(main_dir, "smoke_cost_gate.json"),
                encoding="utf-8",
            ) as handle:
                report = json.load(handle)
            self.assertEqual(report["usage_rows"], 16)
            missing_path = os.path.join(
                smoke_dir, "hybridpatch", "treebank4.jsonl")
            with open(missing_path, encoding="utf-8") as handle:
                missing_rows = [json.loads(line) for line in handle]
            missing_rows[0].pop("total_usd")
            with open(missing_path, "w", encoding="utf-8") as handle:
                for row in missing_rows:
                    handle.write(json.dumps(row) + "\n")
            with tempfile.TemporaryDirectory() as missing_main:
                with self.assertRaisesRegex(RuntimeError, "NO_GO"):
                    paired_dispatch.evaluate_smoke_cost_gate(
                        smoke_dir, missing_main)
                with open(
                    os.path.join(missing_main, "smoke_cost_gate.json"),
                    encoding="utf-8",
                ) as handle:
                    missing_report = json.load(handle)
                self.assertIn(
                    "smoke_usage_incomplete",
                    missing_report["failure_codes"],
                )

        with tempfile.TemporaryDirectory() as smoke_dir, \
                tempfile.TemporaryDirectory() as main_dir, \
                mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(commit, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"x": "y"}), \
                mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={
                        "errors": [], "preservation_violations": 0,
                    }), \
                mock.patch.object(
                    paired_dispatch, "read_quiescent_run_metadata_snapshot",
                    return_value=[{"status": "finished"}]):
            make_smoke(smoke_dir, 2.01)
            with self.assertRaisesRegex(RuntimeError, "NO_GO"):
                paired_dispatch.evaluate_smoke_cost_gate(
                    smoke_dir, main_dir)
            with open(
                os.path.join(main_dir, "smoke_cost_gate.json"),
                encoding="utf-8",
            ) as handle:
                report = json.load(handle)
            self.assertIn(
                "projected_cost_limit_exceeded", report["failure_codes"])

        args = mock.Mock(
            campaign_role="main", smoke_dir="missing",
            samples=paired_dispatch.MAIN_SAMPLES,
            num_round_trips=10, seed=42,
        )
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    paired_dispatch, "evaluate_smoke_cost_gate",
                    side_effect=RuntimeError("gate stopped")), \
                mock.patch.object(paired_dispatch, "read_keys") as read_keys, \
                mock.patch.object(
                    paired_dispatch.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "gate stopped"):
                paired_dispatch._launch_under_lease(args, out_dir)
            read_keys.assert_not_called()
            popen.assert_not_called()

        for role, samples, round_trips, smoke_dir in (
            ("smoke", paired_dispatch.SMOKE_SAMPLES, 2, None),
            ("main", paired_dispatch.MAIN_SAMPLES, 10, "smoke"),
        ):
            with self.subTest(role=role), tempfile.TemporaryDirectory() as out_dir:
                bad_seed_args = mock.Mock(
                    campaign_role=role,
                    smoke_dir=smoke_dir,
                    samples=samples,
                    num_round_trips=round_trips,
                    seed=99,
                )
                with mock.patch.object(
                        paired_dispatch,
                        "_require_formal_opencode_transport") as transport, \
                        mock.patch.object(
                            paired_dispatch, "evaluate_smoke_cost_gate"
                        ) as gate, \
                        mock.patch.object(
                            paired_dispatch, "read_keys") as read_keys, \
                        mock.patch.object(
                            paired_dispatch.subprocess, "Popen") as popen:
                    with self.assertRaisesRegex(RuntimeError, "seed=42"):
                        paired_dispatch._launch_under_lease(
                            bad_seed_args, out_dir)
                    transport.assert_not_called()
                    gate.assert_not_called()
                    read_keys.assert_not_called()
                    popen.assert_not_called()

    def test_confirmation_known_usage_gate_enforces_limit_and_known_rows(self):
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "config": {
                "campaign_role": "confirmation",
                "samples": ["sample"],
                "method_set": ["hybridpatch"],
            },
            "analysis_policy": copy.deepcopy(
                paired_dispatch.CONFIRMATION_ANALYSIS_POLICY),
        }

        def write_result_rows(out_dir, rows):
            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            with open(
                os.path.join(method_dir, "sample.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")

        with tempfile.TemporaryDirectory() as out_dir:
            write_result_rows(out_dir, [{"total_usd": 129.5}])
            report = paired_dispatch.evaluate_confirmation_known_usage_gate(
                out_dir, manifest, wave_index=1)
            self.assertEqual(report["decision"], "GO")
            self.assertEqual(report["rows_observed"], 1)
            self.assertEqual(report["usage_rows"], 1)
            self.assertEqual(report["known_committed_usage_usd"], 129.5)
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertEqual(reports[-1]["decision"], "GO")

        with tempfile.TemporaryDirectory() as out_dir:
            write_result_rows(out_dir, [{}])
            with self.assertRaisesRegex(
                    RuntimeError, "committed_usage_unknown"):
                paired_dispatch.evaluate_confirmation_known_usage_gate(
                    out_dir, manifest, wave_index=1)
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertIn(
                "committed_usage_unknown", reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

        with tempfile.TemporaryDirectory() as out_dir:
            write_result_rows(out_dir, [{"total_usd": 131.0}])
            with self.assertRaisesRegex(
                    RuntimeError,
                    "known_committed_usage_threshold_exceeded"):
                paired_dispatch.evaluate_confirmation_known_usage_gate(
                    out_dir, manifest, wave_index=2)
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertIn(
                "known_committed_usage_threshold_exceeded",
                reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

    def test_confirmation_known_usage_no_go_blocks_resume_before_popen(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock()
            args.campaign_role = "confirmation"
            args.samples = []
            args.selection_manifest = "selection.json"
            args.smoke_dir = None
            args.num_round_trips = 10
            args.seed = 42
            args.slots_per_key = 4
            args.keys_file = "keys.env"
            args.key_labels = labels
            args.resume = True
            args.resume_reason = "retry after known usage stop"
            args.confirm_workers_stopped = True
            args.dry_run = False
            args.start_timeout = 1
            args.poll_interval = 0.01
            args.progress_interval = 999
            args.notes = "unit"
            args.skip_distractor = False

            method_dir = os.path.join(out_dir, "fullrewrite")
            os.makedirs(method_dir, exist_ok=True)
            with open(
                os.path.join(method_dir, f"{samples[0]}.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({"total_usd": 131.0}) + "\n")

            selection_record = {
                "experiment_id": os.path.basename(os.path.abspath(out_dir)),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "selection.json",
                "sha256": "1" * 64,
            }
            task_plans = {
                sample: {
                    "path": f"{sample}.task_plan.json",
                    "sha256": "2" * 64,
                    "forward_state_sequence": ["state"],
                }
                for sample in samples
            }

            def use_current_manifest(_out_dir, manifest, *, resume):
                self.assertTrue(resume)
                return os.path.join(out_dir, "dispatch_manifest.json"), manifest

            def select_all(_out_dir, assignments, **_kwargs):
                return list(assignments), {}

            with mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(samples, selection_record)), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={
                            label: f"unit-key-value-{index}"
                            for index, label in enumerate(labels)
                        }), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        return_value=task_plans), \
                    mock.patch.object(
                        paired_dispatch, "write_or_verify_manifest",
                        side_effect=use_current_manifest), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={
                            "errors": [], "api_calls": 0,
                            "preservation_violations": 0,
                        }), \
                    mock.patch.object(
                        paired_dispatch,
                        "_select_invocation_assignments",
                        side_effect=select_all), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen") as popen:
                self.assertEqual(
                    paired_dispatch._launch_under_lease(args, out_dir), 1)
                popen.assert_not_called()
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertEqual(reports[-1]["phase"], "pre_refill")
            self.assertIn(
                "known_committed_usage_threshold_exceeded",
                reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")

    def test_confirmation_known_usage_blocks_all_finished_empty_resume(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]

        with tempfile.TemporaryDirectory() as out_dir:
            args = mock.Mock()
            args.campaign_role = "confirmation"
            args.samples = []
            args.selection_manifest = "selection.json"
            args.smoke_dir = None
            args.num_round_trips = 10
            args.seed = 42
            args.slots_per_key = 4
            args.keys_file = "keys.env"
            args.key_labels = labels
            args.resume = True
            args.resume_reason = "finish gate after crash"
            args.confirm_workers_stopped = True
            args.dry_run = False
            args.start_timeout = 1
            args.poll_interval = 0.01
            args.progress_interval = 999
            args.notes = "unit"
            args.skip_distractor = False

            method_dir = os.path.join(out_dir, "hybridpatch")
            os.makedirs(method_dir, exist_ok=True)
            with open(
                os.path.join(method_dir, f"{samples[0]}.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({"total_usd": 131.0}) + "\n")

            selection_record = {
                "experiment_id": os.path.basename(os.path.abspath(out_dir)),
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "path": "selection.json",
                "sha256": "1" * 64,
            }
            task_plans = {
                sample: {
                    "path": f"{sample}.task_plan.json",
                    "sha256": "2" * 64,
                    "forward_state_sequence": ["state"],
                }
                for sample in samples
            }

            def use_current_manifest(_out_dir, manifest, *, resume):
                self.assertTrue(resume)
                return os.path.join(out_dir, "dispatch_manifest.json"), manifest

            def select_none(_out_dir, _assignments, **_kwargs):
                return [], {}

            with mock.patch.object(
                    paired_dispatch, "_require_formal_opencode_transport"), \
                    mock.patch.object(
                        paired_dispatch, "_load_confirmation_selection",
                        return_value=(samples, selection_record)), \
                    mock.patch.object(
                        paired_dispatch, "read_keys",
                        return_value={
                            label: f"unit-key-value-{index}"
                            for index, label in enumerate(labels)
                        }), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "code_fingerprint",
                        return_value={"unit": "test"}), \
                    mock.patch.object(
                        paired_dispatch, "prepare_task_plans",
                        return_value=task_plans), \
                    mock.patch.object(
                        paired_dispatch, "write_or_verify_manifest",
                        side_effect=use_current_manifest), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={
                            "errors": [], "api_calls": 0,
                            "preservation_violations": 0,
                        }), \
                    mock.patch.object(
                        paired_dispatch,
                        "_select_invocation_assignments",
                        side_effect=select_none), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "Popen") as popen:
                self.assertEqual(
                    paired_dispatch._launch_under_lease(args, out_dir), 1)
                popen.assert_not_called()
            reports = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "confirmation_known_usage_gate.jsonl"))
            self.assertEqual(reports[-1]["phase"], "pre_final")
            self.assertIn(
                "known_committed_usage_threshold_exceeded",
                reports[-1]["failure_codes"])
            stop = run_meta.read_campaign_stop_conditions(out_dir)[0]
            self.assertEqual(
                stop["condition"],
                "known_committed_usage_wave_boundary_stop")
