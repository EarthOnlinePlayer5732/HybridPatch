"""Mixin slice for test_model_openai.DeepSeekOpenCodeCampaignGroup04Mixin."""

from .support import *


class DeepSeekOpenCodeCampaignGroup04Mixin:
    def test_capacity_manifest_is_one_key_fifteen_slot_rt2(self):
        args = argparse.Namespace(
            campaign_role="deepseek_capacity15",
            num_round_trips=2,
            seed=42,
            slots_per_key=paired_dispatch.DEEPSEEK_CAPACITY_SLOTS_PER_KEY,
            smoke_dir=None,
        )
        samples = list(paired_dispatch.DEEPSEEK_CAPACITY_SAMPLES)
        args.samples = samples
        paired_dispatch._validate_campaign_grid(args)
        assignments = paired_dispatch.build_key_assignments(
            samples, ["KEY_1"], 15,
            alternate_within_key=True, allow_queue=True)
        task_plans = {
            sample: {
                "path": f"{sample}.task_plan.json",
                "sha256": "a" * 64,
                "forward_state_sequence": ["state"],
            }
            for sample in samples
        }
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "unused", samples, assignments, task_plans, args)
        config = manifest["config"]
        self.assertEqual(config["model"], "deepseek-v4-flash")
        self.assertEqual(config["reasoning_effort"], "high")
        self.assertEqual(config["num_round_trips"], 2)
        self.assertEqual(config["slots_per_key"], 15)
        self.assertEqual(config["key_count"], 1)
        self.assertEqual(config["max_worker_count"], 15)
        self.assertEqual(config["queued_worker_count"], 0)
        self.assertEqual(
            config["dispatch_policy"], "per_key_work_conserving_v1")

    def test_full_manifest_is_three_key_work_conserving_queue(self):
        args = argparse.Namespace(
            campaign_role="deepseek_full234",
            num_round_trips=(
                paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
            seed=42,
            slots_per_key=paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY,
            smoke_dir=None,
            _full234_scope_record={
                "schema": "anchorpatch.full234_scope/1",
                "sample_count": 234,
                "sample_ids": [f"sample-{index:03d}" for index in range(234)],
                "sample_json_sha256": "c" * 64,
            },
        )
        samples = list(args._full234_scope_record["sample_ids"])
        args.samples = samples
        paired_dispatch._validate_campaign_grid(args)
        labels = ["KEY_1", "KEY_2", "KEY_3"]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 10,
            alternate_within_key=True, allow_queue=True)
        task_plans = {
            sample: {
                "path": f"{sample}.task_plan.json",
                "sha256": "a" * 64,
                "forward_state_sequence": ["state"],
            }
            for sample in samples
        }
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "unused", samples, assignments, task_plans, args)
        config = manifest["config"]
        self.assertEqual(
            config["num_round_trips"],
            paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS)
        self.assertEqual(config["key_count"], 3)
        self.assertEqual(config["slots_per_key"], 10)
        self.assertEqual(config["max_worker_count"], 30)
        self.assertEqual(config["queued_worker_count"], 204)
        self.assertEqual(
            config["dispatch_policy"], "per_key_work_conserving_v1")
        queues = manifest["assignment_queues"]
        self.assertEqual(len(queues), 3)
        self.assertEqual(
            sorted(queue["worker_count"] for queue in queues),
            [78, 78, 78])

        two_key_assignments = paired_dispatch.build_key_assignments(
            samples, labels[:2], 10,
            alternate_within_key=True, allow_queue=True)
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            with self.assertRaisesRegex(
                    RuntimeError, "requires exactly three keys"):
                paired_dispatch.build_manifest(
                    "unused", samples, two_key_assignments,
                    task_plans, args)

    def test_formal_deepseek_full234_rejects_rt2(self):
        samples = [f"sample-{index:03d}" for index in range(234)]
        args = argparse.Namespace(
            campaign_role="deepseek_full234",
            num_round_trips=2,
            seed=42,
            slots_per_key=paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY,
            smoke_dir=None,
            samples=samples,
            _full234_scope_record={
                "schema": "anchorpatch.full234_scope/1",
                "sample_count": 234,
                "sample_ids": samples,
                "sample_json_sha256": "c" * 64,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "at 10 RT"):
            paired_dispatch._validate_campaign_grid(args)

    def test_worker_launch_uses_per_key_openai_env_and_high(self):
        captured = {}

        class FakeProcess:
            pid = 12345

            @staticmethod
            def poll():
                return None

        def fake_popen(command, **kwargs):
            captured["command"] = list(command)
            captured["env"] = dict(kwargs["env"])
            return FakeProcess()

        args = argparse.Namespace(
            campaign_role="deepseek_capacity15",
            num_round_trips=2,
            seed=42,
            notes="unit",
            start_timeout=10,
        )
        sample = "earncall1"
        assignment = {
            "sample": sample,
            "key_label": "KEY_1",
            "methods": ["hybridpatch", "fullrewrite"],
            "console_log": "dispatch_logs/earncall1.log",
        }
        with tempfile.TemporaryDirectory() as out_dir:
            os.makedirs(os.path.join(out_dir, "dispatch_logs"))
            plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
            with open(plan_path, "w", encoding="utf-8") as handle:
                json.dump({"targets": []}, handle)
            task_plans = {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": "b" * 64,
                }
            }
            manifest = {"run_git_commit": "1" * 40}
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen",
                    side_effect=fake_popen), mock.patch.object(
                        paired_dispatch, "_authorize_workers"), mock.patch.object(
                            paired_dispatch, "_write_active_worker_set"), \
                    mock.patch.dict(
                        os.environ,
                        {
                            "OPENCODE_API_KEY": "wrong",
                            "MINIMAX_API_KEY": "wrong",
                            "MINIMAX_TRANSPORT": "wrong",
                            "AZURE_OPENAI_API_KEY": "wrong",
                            "AZURE_OPENAI_ENDPOINT": "https://wrong.invalid",
                        },
                        clear=False):
                running = {}
                paired_dispatch._launch_worker_batch(
                    args, out_dir, manifest, task_plans,
                    {"KEY_1": "secret-key"}, [assignment], {},
                    os.path.join(out_dir, "dispatch_log.jsonl"), running)
                running[sample]["log"].close()
        command = captured["command"]
        environment = captured["env"]
        self.assertIn("deepseek-v4-flash", command)
        self.assertIn("20000", command)
        self.assertEqual(
            command[command.index("--reasoning_effort") + 1], "high")
        self.assertEqual(environment["OPENAI_API_KEY"], "secret-key")
        self.assertEqual(
            environment["OPENAI_BASE_URL"],
            "https://opencode.ai/zen/go/v1")
        self.assertEqual(
            environment["ANCHORPATCH_DISPATCHER_PID"], str(os.getpid()))
        self.assertTrue(
            environment["ANCHORPATCH_DISPATCHER_INSTANCE_ID"].startswith(
                "dispatcher-"))
        self.assertNotIn("OPENCODE_API_KEY", environment)
        self.assertNotIn("MINIMAX_API_KEY", environment)
        self.assertNotIn("MINIMAX_TRANSPORT", environment)
        self.assertNotIn("AZURE_OPENAI_API_KEY", environment)
        self.assertNotIn("AZURE_OPENAI_ENDPOINT", environment)

    def test_dispatcher_parent_watchdog_requires_complete_identity(self):
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANCHORPATCH_DISPATCHER_PID", None)
            os.environ.pop("ANCHORPATCH_DISPATCHER_INSTANCE_ID", None)
            self.assertIsNone(
                experiment_runner._start_dispatcher_parent_watchdog(out_dir))

            os.environ["ANCHORPATCH_DISPATCHER_PID"] = str(os.getpid())
            with self.assertRaisesRegex(
                    RuntimeError, "watchdog identity is invalid"):
                experiment_runner._start_dispatcher_parent_watchdog(out_dir)

    def test_dispatcher_parent_loss_records_campaign_stop_latch(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a"},
                clear=False), mock.patch.object(
                    run_meta, "_campaign_metadata_lock",
                    side_effect=AssertionError(
                        "emergency writer must not take metadata lock")):
            experiment_runner._record_dispatcher_parent_loss(
                out_dir, 12345, "dispatcher-instance-a")
            records = run_meta.read_campaign_stop_conditions(out_dir)
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, "campaign_stop.json")))
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            self.assertEqual(
                [
                    name for name in os.listdir(emergency_dir)
                    if name.endswith(".json")
                ],
                [],
            )
        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["condition"], "dispatcher_process_lost")
        self.assertEqual(records[0]["dispatcher_pid"], 12345)
        self.assertEqual(
            records[0]["dispatcher_instance_id"],
            "dispatcher-instance-a")
        self.assertEqual(records[0]["worker_pid"], os.getpid())

    def test_dispatcher_parent_loss_recovery_closes_and_resumes_worker(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir,
                ["running"],
                methods=["fullrewrite", "hybridpatch"],
            )
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            worker = fixture["workers"][0]
            methods = fixture["methods"]
            api_row = self._deepseek_api_row(
                out_dir,
                worker["sample"],
                worker["worker_launch_id"],
                worker["worker_pid"],
                "forward",
            )
            self.assertNotIn("model_empty", api_row)
            self.assertEqual(
                api_row["response_classification"], "normal")
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), api_row)
            git_result = mock.Mock(stdout="")
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=[]), mock.patch.object(
                                run_meta, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=fingerprint), mock.patch.object(
                            run_meta.subprocess, "run",
                            return_value=git_result):
                result = ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                allowed = (
                    paired_dispatch
                    ._verified_deepseek_resume_missing_samples(
                        out_dir, [{
                            "sample": worker["sample"],
                            "methods": methods,
                        }]))

            authorization_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            authorization_sha256 = run_meta._sha256_file(
                authorization_path)
            self.assertEqual(result["reconciled_workers"], 1)
            self.assertRegex(
                result["authorization_id"], r"^dpl-[0-9a-f]{12}$")
            self.assertEqual(
                result["authorization_sha256"], authorization_sha256)
            self.assertEqual(result["resume_samples"], [worker["sample"]])
            self.assertEqual(
                authorization["authorization_path"], authorization_path)
            self.assertEqual(
                authorization["authorization_sha256"],
                authorization_sha256,
            )
            self.assertEqual(
                authorization["recovery_identity_history"][-1][
                    "authorization_sha256"
                ],
                authorization_sha256,
            )
            self.assertEqual(
                authorization["recovery_kind"],
                run_meta.DISPATCHER_PROCESS_LOST_RECOVERY_KIND)
            self.assertEqual(
                len(authorization["incident_api_rows"]), 1)
            self.assertEqual(
                authorization["dispatcher_parent_loss_workers"], [{
                    "sample": worker["sample"],
                    "worker_launch_id": worker["worker_launch_id"],
                    "worker_pid": worker["worker_pid"],
                    "invocation_id": worker["invocation_id"],
                    "status": "interrupted_by_dispatcher",
                }])
            self.assertEqual(allowed, {worker["sample"]})
            self.assertEqual(run_meta.read_campaign_stop_conditions(out_dir), [])
            active = ledger_recovery._read_json(os.path.join(
                out_dir, "active_worker_set.json"))
            self.assertEqual(active["workers"], {})
            metadata = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(
                [row["status"] for row in metadata],
                ["interrupted_by_dispatcher"])
            self.assertEqual(metadata[0]["methods"], methods)
            events = ledger_recovery._read_jsonl(
                os.path.join(out_dir, "dispatch_log.jsonl"))
            event_names = [row["event"] for row in events]
            self.assertEqual(event_names.count("worker_exit"), 1)
            self.assertEqual(
                event_names.count("stale_worker_reconciled"), 1)
            authorization_events = [
                row for row in events
                if row["event"]
                == "user_authorized_dispatcher_parent_loss_recovery"
            ]
            self.assertEqual(len(authorization_events), 1)
            self.assertEqual(
                authorization_events[0][
                    "campaign_recovery_authorization_sha256"
                ],
                authorization_sha256,
            )
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))

    def test_parent_loss_preserves_prior_server_retry_incident_validation(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"], record_stop=False)
            manifest = fixture["manifest"]
            worker = fixture["workers"][0]
            prior_commit = fixture["commit"]
            prior_fingerprint = fixture["fingerprint"]
            recovery_commit = "5" * 40
            recovery_fingerprint = dict(prior_fingerprint)
            recovery_fingerprint["run_meta.py"] = "5" * 64
            changed_paths = [
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/paired_campaign_dispatch.py",
                "HP_V8/src/run_meta.py",
            ]

            metadata = run_meta.read_run_metadata_snapshot(out_dir)
            metadata[0].update({
                "status": "failed",
                "finished_at": "2026-07-28T20:00:00+08:00",
            })
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"), metadata)
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"), {
                    "event": "worker_exit",
                    "sample": worker["sample"],
                    "worker_launch_id": worker["worker_launch_id"],
                    "pid": worker["worker_pid"],
                    "returncode": 1,
                    "disposition": "campaign_fatal",
                })
            paired_dispatch._write_active_worker_set(out_dir, manifest, [])

            server_row = self._deepseek_api_row(
                out_dir,
                worker["sample"],
                worker["worker_launch_id"],
                worker["worker_pid"],
                "forward",
                request_id="call-prior-server-retry",
            )
            server_attempts = [
                {
                    "attempt_index": index,
                    "status": "retryable_error",
                    "error_type": "server_error",
                    "http_status": 502,
                    "stream_complete": False,
                    "generation_delta_seen": True,
                    "terminal_sequence_valid": False,
                    "retry_budget_consumed": True,
                    "retry_budget_attempt_index": index,
                }
                for index in range(1, 4)
            ]
            server_row.update({
                "classification": "provider/API failure",
                "error_type": "server_error",
                "http_status": 502,
                "stream_complete": False,
                "response_classification": None,
                "count_as_method_failure": False,
                "transport_attempts": server_attempts,
                "http_attempts_used": len(server_attempts),
                "retry_count": len(server_attempts) - 1,
                "failed_attempt_count": len(server_attempts),
            })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), server_row)
            run_meta.write_json_atomic(
                os.path.join(out_dir, "campaign_stop.json"), {
                    "schema": run_meta.STOP_CONDITION_SCHEMA,
                    "condition": "dispatcher_integrity_failure",
                    "error": (
                        "worker failed without a supported sample-local "
                        "outcome"),
                })

            git_result = mock.Mock(
                stdout="\n".join(changed_paths) + "\n")
            common_patches = (
                mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(recovery_commit, "clean")),
                mock.patch.object(
                    ledger_recovery, "code_fingerprint",
                    return_value=recovery_fingerprint),
                mock.patch.object(
                    ledger_recovery, "_git_changed_paths",
                    return_value=changed_paths),
                mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"),
                mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(recovery_commit, "clean")),
                mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=recovery_fingerprint),
                mock.patch.object(
                    run_meta.subprocess, "run",
                    return_value=git_result),
            )
            with contextlib.ExitStack() as stack:
                for patcher in common_patches:
                    stack.enter_context(patcher)
                ledger_recovery.authorize(
                    out_dir, deepseek_server_retry=True)
                prior = run_meta.read_campaign_recovery_authorization(out_dir)

            self.assertEqual(
                prior["incident_api_rows"][0]["incident_kind"],
                "deepseek_server_retry_exhaustion",
            )
            self.assertNotIn(
                "transport_sidecar_path", prior["incident_api_rows"][0])
            self.assertEqual(
                prior["incident_api_rows"][0]["canonical_sha256"],
                run_meta._canonical_record_sha256(server_row),
            )

            worker_id = "worker-after-server-retry"
            worker_pid = os.getpid()
            invocation_id = "invocation-after-server-retry"
            dispatcher_pid = 54321
            dispatcher_instance_id = "dispatcher-instance-after-retry"
            paired_dispatch._write_active_worker_set(
                out_dir, manifest, [{
                    "sample": worker["sample"],
                    "worker_launch_id": worker_id,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                }])
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            intent = {
                "event": "launch_intent",
                "sample": worker["sample"],
                "key_label": "KEY_1",
                "methods": fixture["methods"],
                "worker_launch_id": worker_id,
                "console_log": "dispatch_logs/after-retry.console.log",
                "method_phase": None,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            }
            run_meta.append_jsonl_locked(dispatch_path, intent)
            run_meta.append_jsonl_locked(dispatch_path, {
                **intent,
                "event": "launch",
                "pid": worker_pid,
            })
            run_meta.append_jsonl_locked(dispatch_path, {
                "event": "worker_authorized",
                "worker_launch_id": worker_id,
                "sample": worker["sample"],
                "worker_pid": worker_pid,
                "invocation_id": invocation_id,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            })
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "run_metadata.jsonl"), {
                    "schema": run_meta.METADATA_SCHEMA,
                    "invocation_id": invocation_id,
                    "worker_launch_id": worker_id,
                    "worker_pid": worker_pid,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                    "samples": [worker["sample"]],
                    "methods": fixture["methods"],
                    "method_phase": None,
                    "model": paired_dispatch.DEEPSEEK_MODEL,
                    "provider": "opencode_zen",
                    "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                    "transport_revision": (
                        paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                    "transport_resume_policy": None,
                    "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                    "reasoning_effort": (
                        paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                    "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    "campaign_config": {
                        "reasoning_effort": (
                            paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                    },
                    "run_git_commit": recovery_commit,
                    "git_tree_state": "clean",
                    "code_fingerprint": recovery_fingerprint,
                    "status": "running",
                    "finished_at": None,
                })
            current_row = self._deepseek_api_row(
                out_dir,
                worker["sample"],
                worker_id,
                worker_pid,
                "forward",
                request_id="call-current-parent-loss",
            )
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), current_row)
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": worker_id},
                    clear=False):
                run_meta.record_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=dispatcher_pid,
                    dispatcher_instance_id=dispatcher_instance_id,
                )

            with contextlib.ExitStack() as stack:
                for patcher in common_patches:
                    stack.enter_context(patcher)
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                combined = run_meta.read_campaign_recovery_authorization(
                    out_dir)

            incidents = {
                item["incident_kind"]: item
                for item in combined["incident_api_rows"]
            }
            self.assertEqual(set(incidents), {
                "deepseek_server_retry_exhaustion",
                "dispatcher_parent_loss_uncommitted_api",
            })
            self.assertNotIn(
                "transport_sidecar_path",
                incidents["deepseek_server_retry_exhaustion"],
            )
            self.assertEqual(
                incidents["dispatcher_parent_loss_uncommitted_api"]
                ["transport_sidecar_sha256"],
                current_row["transport_sidecar_sha256"],
            )
