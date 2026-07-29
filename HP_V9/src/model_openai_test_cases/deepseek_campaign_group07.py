"""Mixin slice for audited operator-pause recovery regressions."""

from .support import *


class DeepSeekOpenCodeCampaignGroup07Mixin:
    @staticmethod
    def _minimal_dispatch_args(**overrides):
        values = {
            "campaign_role": "deepseek_full234",
            "num_round_trips": paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS,
            "slots_per_key": paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY,
            "resume": False,
            "dry_run": False,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def _write_operator_pause_fixture(self, out_dir, *, stages=None):
        fixture = self._write_dispatcher_parent_loss_fixture(
            out_dir, stages or ["running"], record_stop=False)
        manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
        active_path = os.path.join(out_dir, "active_worker_set.json")
        active = ledger_recovery._read_json(active_path)
        active_ids = sorted(active["workers"])
        stop = {
            "schema": run_meta.STOP_CONDITION_SCHEMA,
            "created_at": "2026-07-29T00:00:00+08:00",
            "condition": paired_dispatch.OPERATOR_PAUSE_CONDITION,
            "worker_launch_id": None,
            "worker_pid": fixture["dispatcher_pid"],
            "reason": "dispatcher received SIGINT",
            "signal_name": "SIGINT",
            "signal_number": getattr(paired_dispatch.signal, "SIGINT", 2),
            "exit_code": 130,
            "boundary": "unit-test",
            "stopped_git_commit": fixture["commit"],
            "stopped_git_tree_state": "clean",
            "git_status_porcelain": "",
            "dispatcher_pid": fixture["dispatcher_pid"],
            "dispatcher_instance_id": fixture["dispatcher_instance_id"],
            "active_worker_launch_ids": active_ids,
            "active_worker_count": len(active_ids),
            "dispatch_manifest_sha256": run_meta._sha256_file(manifest_path),
            "active_worker_set_sha256": run_meta._sha256_file(active_path),
            "active_worker_set_canonical_sha256": (
                run_meta._canonical_record_sha256(active)),
            "publication_mode": "canonical",
        }
        run_meta.write_json_atomic(
            os.path.join(out_dir, "campaign_stop.json"), stop)
        return fixture

    @staticmethod
    def _operator_pause_identity_patches(commit, fingerprint):
        git_result = mock.Mock(stdout="")
        return (
            mock.patch.object(
                ledger_recovery, "_git_identity",
                return_value=(commit, "clean")),
            mock.patch.object(
                ledger_recovery, "code_fingerprint",
                return_value=fingerprint),
            mock.patch.object(
                ledger_recovery, "_git_changed_paths", return_value=[]),
            mock.patch.object(
                run_meta, "_git_identity", return_value=(commit, "clean")),
            mock.patch.object(
                run_meta, "code_fingerprint", return_value=fingerprint),
            mock.patch.object(
                run_meta.subprocess, "run", return_value=git_result),
        )

    def test_operator_pause_signal_first_wins_and_scope_is_exact(self):
        state = paired_dispatch._OperatorPauseState()
        state.request(paired_dispatch.signal.SIGTERM)
        state.request(paired_dispatch.signal.SIGINT)
        with self.assertRaises(paired_dispatch.OperatorPauseRequested) as ctx:
            state.raise_if_requested("unit")
        self.assertEqual(ctx.exception.signal_name, "SIGTERM")
        self.assertEqual(ctx.exception.exit_code, 143)

        args = self._minimal_dispatch_args()
        self.assertTrue(paired_dispatch._supports_audited_operator_pause(args))
        self.assertFalse(paired_dispatch._supports_audited_operator_pause(
            self._minimal_dispatch_args(campaign_role="deepseek_capacity15")))
        self.assertFalse(paired_dispatch._supports_audited_operator_pause(
            self._minimal_dispatch_args(num_round_trips=2)))
        self.assertFalse(paired_dispatch._supports_audited_operator_pause(
            self._minimal_dispatch_args(slots_per_key=1)))
        with mock.patch.object(
                paired_dispatch, "DEEPSEEK_TRANSPORT_REVISION",
                "opencode_openai_compatible/999"):
            self.assertFalse(
                paired_dispatch._supports_audited_operator_pause(args))

    def test_worker_popen_kwargs_isolates_worker_process_group(self):
        with mock.patch.object(paired_dispatch.os, "name", "nt"), \
                mock.patch.object(
                    paired_dispatch.subprocess,
                    "CREATE_NEW_PROCESS_GROUP", 512, create=True):
            self.assertEqual(
                paired_dispatch._worker_popen_kwargs(),
                {"creationflags": 512})
        with mock.patch.object(paired_dispatch.os, "name", "posix"):
            self.assertEqual(
                paired_dispatch._worker_popen_kwargs(),
                {"start_new_session": True})

    def test_active_stop_or_worker_witness_blocks_before_mutation(self):
        for witness_kind in ("campaign_stop", "active_workers"):
            with self.subTest(witness_kind=witness_kind), \
                    tempfile.TemporaryDirectory() as out_dir:
                if witness_kind == "campaign_stop":
                    run_meta.write_json_atomic(
                        os.path.join(out_dir, "campaign_stop.json"), {
                            "schema": run_meta.STOP_CONDITION_SCHEMA,
                            "created_at": "2026-07-29T00:00:00+08:00",
                            "condition": (
                                paired_dispatch.OPERATOR_PAUSE_CONDITION),
                            "worker_launch_id": None,
                            "worker_pid": 123,
                        })
                else:
                    run_meta.write_json_atomic(
                        os.path.join(out_dir, "active_worker_set.json"), {
                            "schema": "anchorpatch.active_worker_set/1",
                            "run_git_commit": "1" * 40,
                            "dispatcher_pid": 123,
                            "dispatcher_instance_id": "dispatcher",
                            "workers": {
                                "worker-a": {"sample": "sample-a"},
                            },
                        })
                args = self._minimal_dispatch_args(resume=True, dry_run=True)
                with mock.patch.object(
                        paired_dispatch, "prepare_task_plans") as plans, \
                        mock.patch.object(
                            paired_dispatch, "write_or_verify_manifest"
                        ) as manifest_writer, \
                        mock.patch.object(
                            paired_dispatch.subprocess, "Popen") as popen:
                    with self.assertRaisesRegex(
                            RuntimeError,
                            "offline recovery authorization"):
                        paired_dispatch._launch_under_lease_impl(args, out_dir)
                plans.assert_not_called()
                manifest_writer.assert_not_called()
                popen.assert_not_called()
                self.assertFalse(os.path.exists(os.path.join(
                    out_dir, "dispatch_manifest.json")))

    def test_dispatcher_rejects_inherited_worker_identity_before_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            out_dir = os.path.join(root, "campaign")
            args = self._minimal_dispatch_args(out_dir=out_dir)
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a"},
                    clear=False):
                with self.assertRaisesRegex(
                        RuntimeError, "cannot inherit"):
                    paired_dispatch.launch(args)
            self.assertFalse(os.path.exists(out_dir))

    def test_worker_operator_stop_finishes_metadata_without_fatal_latch(self):
        failure = run_meta.CampaignStoppedError(
            "campaign stop latch is set: operator_directed_dispatcher_pause")
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(sys, "argv", [
                    "experiment_runner.py",
                    "--sample", "sample",
                    "--methods", "hybridpatch", "fullrewrite",
                    "--num_round_trips", "1",
                    "--out_dir", out_dir,
                    "--model", paired_dispatch.DEEPSEEK_MODEL,
                    "--reasoning_effort", "high",
                ]), \
                mock.patch.object(
                    experiment_runner,
                    "_start_dispatcher_parent_watchdog"), \
                mock.patch.object(
                    experiment_runner,
                    "_require_formal_opencode_transport"), \
                mock.patch.object(
                    experiment_runner,
                    "_require_formal_dispatch_environment"), \
                mock.patch.object(
                    experiment_runner,
                    "append_run_metadata",
                    return_value={"invocation_id": "invocation"}), \
                mock.patch.object(
                    experiment_runner,
                    "_dispatch_worker_start_barrier"), \
                mock.patch.object(
                    experiment_runner, "run_relay", side_effect=failure), \
                mock.patch.object(
                    experiment_runner,
                    "read_campaign_stop_conditions",
                    return_value=[{
                        "condition": paired_dispatch.OPERATOR_PAUSE_CONDITION,
                    }]), \
                mock.patch.object(
                    experiment_runner,
                    "record_campaign_stop_condition") as stop, \
                mock.patch.object(
                    experiment_runner, "finish_run_metadata") as finish:
            with self.assertRaises(run_meta.CampaignStoppedError):
                experiment_runner.main()

        stop.assert_not_called()
        finish.assert_called_once_with(
            out_dir, "invocation", status="interrupted_by_dispatcher")

    def test_operator_pause_stop_binds_manifest_and_active_hashes(self):
        for tamper in ("manifest", "active"):
            with self.subTest(tamper=tamper), \
                    tempfile.TemporaryDirectory() as out_dir:
                fixture = self._write_operator_pause_fixture(out_dir)
                commit = fixture["commit"]
                fingerprint = fixture["fingerprint"]
                if tamper == "manifest":
                    manifest_path = os.path.join(
                        out_dir, "dispatch_manifest.json")
                    manifest = ledger_recovery._read_json(manifest_path)
                    manifest["config"]["tampered"] = True
                    run_meta.write_json_atomic(manifest_path, manifest)
                else:
                    active_path = os.path.join(
                        out_dir, "active_worker_set.json")
                    active = ledger_recovery._read_json(active_path)
                    active["workers"]["worker-extra"] = {"sample": "extra"}
                    run_meta.write_json_atomic(active_path, active)

                patches = self._operator_pause_identity_patches(
                    commit, fingerprint)
                with patches[0], patches[1], patches[2], \
                        patches[3], patches[4], patches[5]:
                    with self.assertRaisesRegex(
                            RuntimeError,
                            "operator pause manifest or active-worker "
                            "witness has drifted"):
                        ledger_recovery.authorize(
                            out_dir, operator_pause=True)
                self.assertFalse(os.path.exists(os.path.join(
                    out_dir,
                    ledger_recovery._DISPATCHER_PARENT_LOSS_HISTORY_ROOT)))
                self.assertFalse(os.path.exists(os.path.join(
                    out_dir,
                    run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME)))

    def test_operator_pending_mode_is_not_reused_by_parent_loss(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_operator_pause_fixture(out_dir)
            pending_path = os.path.join(
                out_dir, run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME)
            run_meta.write_json_atomic(pending_path, {
                "schema": (
                    ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_SCHEMA),
                "authorization_record": {
                    "dispatcher_operator_pause_recovery": True,
                    "recovery_git_commit": fixture["commit"],
                    "recovery_code_fingerprint": fixture["fingerprint"],
                },
            })
            patches = self._operator_pause_identity_patches(
                fixture["commit"], fixture["fingerprint"])
            with patches[0], patches[1]:
                with self.assertRaisesRegex(
                        RuntimeError,
                        "requires --operator_dispatcher_pause"):
                    ledger_recovery._authorize_dispatcher_process_lost(
                        out_dir, fixture["manifest"],
                        os.path.join(out_dir, "campaign_stop.json"),
                        os.path.join(
                            out_dir,
                            run_meta
                            .CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME),
                        None,
                        operator_pause_recovery=False)

    def test_operator_recovery_prior_provider_incident_fails_before_mutation(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_operator_pause_fixture(out_dir)
            prior = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "prior",
                "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
                "dispatch_manifest_sha256": run_meta._sha256_file(
                    os.path.join(out_dir, "dispatch_manifest.json")),
                "prior_git_commit": fixture["commit"],
                "prior_code_fingerprint": fixture["fingerprint"],
                "recovery_git_commit": fixture["commit"],
                "recovery_code_fingerprint": fixture["fingerprint"],
                "incident_api_rows": [{"row_number": 1}],
            }
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME)
            run_meta.write_json_atomic(auth_path, prior)
            patches = self._operator_pause_identity_patches(
                fixture["commit"], fixture["fingerprint"])
            with patches[0], patches[1], patches[2], \
                    patches[3], patches[4], patches[5], \
                    mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        return_value={
                            "authorization_id": "prior",
                            "authorization_sha256": run_meta._sha256_file(
                                auth_path),
                        }), \
                    mock.patch.object(
                        ledger_recovery,
                        "_reconcile_deepseek_parent_loss_workers",
                        return_value={
                            "dispatcher_pid": fixture["dispatcher_pid"],
                            "dispatcher_instance_id": (
                                fixture["dispatcher_instance_id"]),
                            "workers": [],
                            "interrupted_workers": [],
                            "preauthorization_workers": [],
                            "registered_prelaunch_workers": [],
                        }), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_parent_loss_incidents",
                        return_value={
                            "api": [],
                            "attempts": [],
                            "transport_sidecars": [],
                        }):
                with self.assertRaisesRegex(
                        RuntimeError,
                        "cannot supersede prior provider replay incidents"):
                    ledger_recovery.authorize(out_dir, operator_pause=True)
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_HISTORY_ROOT)))
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME)))

    def test_operator_pause_emergency_fallback_is_unique_durable_stop(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "git_tree_state": "clean",
            }
            manifest_path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(manifest_path, manifest)
            args = self._minimal_dispatch_args()
            args._dispatcher_instance_id = "dispatcher"
            args._operator_pause_dispatch_manifest_sha256 = (
                run_meta._sha256_file(manifest_path))
            worker = {
                "sample": "sample-a",
                "worker_launch_id": "worker-a",
                "dispatcher_pid": os.getpid(),
                "dispatcher_instance_id": "dispatcher",
            }
            paired_dispatch._publish_active_worker_set(
                out_dir, manifest, [worker], args=args)
            exc = paired_dispatch.OperatorPauseRequested(
                signal_name="SIGINT",
                signum=paired_dispatch.signal.SIGINT,
                boundary="unit-test")
            with mock.patch.object(
                    paired_dispatch,
                    "record_campaign_stop_condition",
                    side_effect=RuntimeError("canonical writer failed")):
                record = paired_dispatch._record_operator_pause_emergency_condition(
                    out_dir, args, exc, manifest, {worker["sample"]: worker},
                    RuntimeError("canonical writer failed"))

            self.assertEqual(record["publication_mode"], "emergency_fallback")
            durable = run_meta.read_campaign_stop_conditions(out_dir)
            self.assertEqual(len(durable), 1)
            self.assertEqual(
                run_meta._canonical_record_sha256(durable[0]),
                run_meta._canonical_record_sha256(record))
