"""Mixin slice for test_model_openai.DeepSeekOpenCodeCampaignGroup02Mixin."""

from .support import *


class DeepSeekOpenCodeCampaignGroup02Mixin:
    def _make_deepseek_initial_recovery_fixture(self, out_dir):
        manifest_path = os.path.join(
            out_dir, "dispatch_manifest.json")
        auth_path = os.path.join(
            out_dir,
            run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
        )
        stop_path = os.path.join(out_dir, "campaign_stop.json")
        dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
        api_path = os.path.join(out_dir, "api_calls.jsonl")
        manifest = {"schema": paired_dispatch.SCHEMA}
        stop = {
            "schema": run_meta.STOP_CONDITION_SCHEMA,
            "created_at": "2026-07-28T13:00:00+08:00",
            "condition": "worker_fatal_error",
            "error_type": "OpenAICompatibleTransportError",
            "error": "OpenAI-compatible provider failed after 1 attempt(s)",
            "sample": "sample-a",
            "worker_launch_id": "worker-a",
        }
        api_row = {
            "request_id": "request-a",
            "sample": "sample-a",
            "worker_launch_id": "worker-a",
        }
        run_meta.write_json_atomic(manifest_path, manifest)
        run_meta.write_json_atomic(stop_path, stop)
        run_meta.write_json_atomic(
            os.path.join(out_dir, "active_worker_set.json"),
            {"workers": {}},
        )
        run_meta.append_jsonl_locked(dispatch_path, {"event": "prior"})
        run_meta.append_jsonl_locked(api_path, api_row)
        api_row_sha256 = run_meta._canonical_record_sha256(api_row)
        sidecar = {
            "api_row_sha256": api_row_sha256,
            "sha256": "c" * 64,
        }
        fingerprint = {"run_meta.py": "fingerprint"}
        history_relative = (
            "recovery_history/deepseek-initial-transaction-test")
        archived_stop = os.path.join(
            out_dir, history_relative, "campaign_stop.json")
        record = {
            "schema": (
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
            "authorization_id": "deepseek-initial-auth",
            "created_at": "2026-07-28T13:01:00+08:00",
            "deepseek_transport_disconnect_retry_recovery": True,
            "deepseek_transport_disconnect_initial_transaction": True,
            "deepseek_transport_disconnect_initial_prefixes": {
                "dispatch_log.jsonl": (
                    ledger_recovery._file_prefix_evidence(dispatch_path)),
            },
            "deepseek_transport_disconnect_retry_samples": ["sample-a"],
            "deepseek_resume_samples": ["sample-a"],
            "deepseek_pending_samples": [],
            "dispatch_manifest_sha256": (
                run_meta._sha256_file(manifest_path)),
            "prior_git_commit": "b" * 40,
            "recovery_git_commit": "a" * 40,
            "recovery_code_fingerprint": fingerprint,
            "archived_stop_path": os.path.relpath(
                archived_stop, out_dir).replace("\\", "/"),
            "archived_stop_sha256": run_meta._sha256_file(stop_path),
            "archived_emergency_stop_records": [],
            "incident_api_rows": [{"row_number": 1}],
            "incident_transport_sidecars": [sidecar],
            "recovered_worker_launch_ids": ["worker-a"],
        }
        return {
            "manifest": manifest,
            "record": record,
            "sidecar": sidecar,
            "fingerprint": fingerprint,
            "auth_path": auth_path,
            "stop_path": stop_path,
            "dispatch_path": dispatch_path,
            "archived_stop": archived_stop,
        }

    def test_deepseek_initial_recovery_validates_sidecar_before_stop_archive(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._make_deepseek_initial_recovery_fixture(out_dir)
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fixture["fingerprint"]), \
                    mock.patch.object(
                        ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_dispatcher_stopped_sidecar_evidence",
                        return_value={
                            **fixture["sidecar"],
                            "sha256": "d" * 64,
                        }):
                with self.assertRaisesRegex(
                        RuntimeError, "sidecar has drifted"):
                    (
                        ledger_recovery
                        ._commit_deepseek_transport_disconnect_initial(
                            out_dir, fixture["manifest"],
                            fixture["stop_path"], fixture["auth_path"],
                            fixture["record"],
                        )
                    )
            self.assertTrue(os.path.isfile(fixture["stop_path"]))
            self.assertFalse(os.path.exists(fixture["auth_path"]))
            self.assertFalse(os.path.exists(fixture["archived_stop"]))

    def test_deepseek_initial_recovery_repairs_partial_witness(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._make_deepseek_initial_recovery_fixture(out_dir)
            record = fixture["record"]
            run_meta.write_json_atomic(fixture["auth_path"], record)
            event = {
                "event": (
                    "user_authorized_deepseek_transport_disconnect_"
                    "retry_recovery"),
                "created_at": record["created_at"],
                "campaign_recovery_authorization_id": (
                    record["authorization_id"]),
                "campaign_recovery_authorization_sha256": (
                    run_meta._sha256_file(fixture["auth_path"])),
                "deepseek_transport_disconnect_retry_samples": [
                    "sample-a"],
                "incident_api_rows": 1,
                "prior_git_commit": "b" * 40,
                "recovery_git_commit": "a" * 40,
            }
            encoded = (
                json.dumps(event, ensure_ascii=False).encode("utf-8")
                + b"\n"
            )
            with open(fixture["dispatch_path"], "ab") as handle:
                handle.write(encoded[:len(encoded) // 2])
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fixture["fingerprint"]), \
                    mock.patch.object(
                        ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_dispatcher_stopped_sidecar_evidence",
                        return_value=fixture["sidecar"]), \
                    mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        return_value={"authorization_sha256": "e" * 64}):
                result = (
                    ledger_recovery
                    ._commit_deepseek_transport_disconnect_initial(
                        out_dir, fixture["manifest"],
                        fixture["stop_path"], fixture["auth_path"],
                        record,
                    )
                )
            self.assertEqual(
                result["authorization_id"], "deepseek-initial-auth")
            self.assertFalse(os.path.exists(fixture["stop_path"]))
            self.assertTrue(os.path.isfile(fixture["archived_stop"]))
            witnesses = [
                row for row in ledger_recovery._read_jsonl(
                    fixture["dispatch_path"])
                if row.get("event") == event["event"]
            ]
            self.assertEqual(witnesses, [event])

    def test_deepseek_initial_recovery_cli_is_idempotent_after_stop_move(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._make_deepseek_initial_recovery_fixture(out_dir)
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fixture["fingerprint"]), \
                    mock.patch.object(
                        ledger_recovery, "_assert_worker_leases_free"), \
                    mock.patch.object(
                        ledger_recovery,
                        "_deepseek_dispatcher_stopped_sidecar_evidence",
                        return_value=fixture["sidecar"]), \
                    mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        side_effect=RuntimeError("simulated crash")):
                with self.assertRaisesRegex(
                        RuntimeError, "simulated crash"):
                    (
                        ledger_recovery
                        ._commit_deepseek_transport_disconnect_initial(
                            out_dir, fixture["manifest"],
                            fixture["stop_path"], fixture["auth_path"],
                            fixture["record"],
                        )
                    )
            self.assertFalse(os.path.exists(fixture["stop_path"]))
            self.assertTrue(os.path.isfile(fixture["auth_path"]))
            self.assertTrue(os.path.isfile(fixture["archived_stop"]))

            verified = {
                "authorization_id": "deepseek-initial-auth",
                "authorization_sha256": "e" * 64,
                "deepseek_transport_disconnect_retry_samples": [
                    "sample-a"],
                "incident_api_rows": [{"row_number": 1}],
                "recovered_worker_launch_ids": ["worker-a"],
                "deepseek_pending_samples": [],
                "committed_results_modified": False,
                "checkpoint_rows_modified": False,
                "provider_post_replay_scope": "uncommitted_steps_only",
            }
            with mock.patch.object(
                    ledger_recovery,
                    "read_campaign_recovery_authorization",
                    return_value=verified):
                result = ledger_recovery.authorize(
                    out_dir,
                    deepseek_transport_disconnect_retry=True,
                )
            self.assertTrue(result["already_authorized"])
            self.assertFalse(result["inspector_followup"])
            self.assertEqual(
                result["authorization_id"], "deepseek-initial-auth")

    def test_deepseek_inspector_pending_commit_is_idempotent(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest_path = os.path.join(
                out_dir, "dispatch_manifest.json")
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            pending_path = os.path.join(
                out_dir,
                run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            )
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            history_relative = "recovery_history/followup-auth"
            history_dir = os.path.join(out_dir, history_relative)
            archived_auth = os.path.join(
                history_dir,
                "superseded_campaign_recovery_authorization.json",
            )
            archived_stop = os.path.join(
                history_dir, "campaign_stop.json")
            manifest = {"schema": paired_dispatch.SCHEMA}
            prior_auth = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "prior-auth",
            }
            stop = {
                "schema": run_meta.STOP_CONDITION_SCHEMA,
                "condition": "dispatcher_integrity_failure",
                "error": "fixture",
            }
            run_meta.write_json_atomic(manifest_path, manifest)
            run_meta.write_json_atomic(auth_path, prior_auth)
            run_meta.write_json_atomic(stop_path, stop)
            run_meta.write_json_atomic(
                os.path.join(out_dir, "active_worker_set.json"),
                {"workers": {}},
            )
            run_meta.append_jsonl_locked(
                dispatch_path, {"event": "prior"})
            dispatch_prefix = (
                ledger_recovery._file_prefix_evidence(dispatch_path)
            )
            fingerprint = {"run_meta.py": "fingerprint"}
            prior_auth_sha256 = run_meta._sha256_file(auth_path)
            record = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "followup-auth",
                "created_at": "2026-07-28T14:00:00+08:00",
                "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
                "authorization_basis": (
                    "explicit_user_resume_after_deepseek_recovery_"
                    "inspector_fix"
                ),
                "deepseek_transport_disconnect_retry_recovery": True,
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery": True,
                "dispatch_manifest_sha256": (
                    run_meta._sha256_file(manifest_path)),
                "recovery_git_commit": "a" * 40,
                "recovery_git_tree_state": "clean",
                "recovery_code_fingerprint": fingerprint,
                "deepseek_resume_samples": ["sample-a"],
                "superseded_authorization_path": (
                    os.path.relpath(
                        archived_auth, out_dir).replace("\\", "/")),
                "superseded_authorization_sha256": (
                    prior_auth_sha256),
                "archived_stop_path": (
                    os.path.relpath(
                        archived_stop, out_dir).replace("\\", "/")),
                "archived_stop_sha256": (
                    run_meta._sha256_file(stop_path)),
                "archived_emergency_stop_records": [],
                "deepseek_transport_disconnect_inspector_"
                "prior_authorization_id": "prior-auth",
                "deepseek_transport_disconnect_inspector_"
                "prior_authorization_sha256": prior_auth_sha256,
                "deepseek_transport_disconnect_inspector_"
                "prior_recovery_git_commit": "b" * 40,
                "deepseek_transport_disconnect_inspector_prefixes": {
                    "dispatch_log.jsonl": dispatch_prefix,
                },
                "deepseek_transport_disconnect_retry_samples": [
                    "sample-a"],
                "incident_api_rows": [{"row_number": 1}],
                "recovered_worker_launch_ids": ["worker-a"],
                "deepseek_pending_samples": [],
                "committed_results_modified": False,
                "checkpoint_rows_modified": False,
                "provider_post_replay_scope": "uncommitted_steps_only",
            }
            pending = {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "created_at": "2026-07-28T14:00:00+08:00",
                "history_dir": history_relative,
                "authorization_record": record,
            }
            run_meta.write_json_atomic(pending_path, pending)
            verified = {
                "authorization_sha256": "c" * 64,
            }
            common_patches = (
                mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=("a" * 40, "clean")),
                mock.patch.object(
                    ledger_recovery, "code_fingerprint",
                    return_value=fingerprint),
                mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"),
                mock.patch.object(
                    ledger_recovery,
                    "read_campaign_recovery_authorization",
                    return_value=verified),
            )
            malicious = json.loads(json.dumps(pending))
            malicious["authorization_record"][
                "superseded_authorization_path"
            ] = os.path.basename(auth_path)
            run_meta.write_json_atomic(pending_path, malicious)
            prior_auth_sha256 = run_meta._sha256_file(auth_path)
            prior_stop_sha256 = run_meta._sha256_file(stop_path)
            with common_patches[0], common_patches[1], \
                    common_patches[2], common_patches[3]:
                with self.assertRaisesRegex(
                        RuntimeError, "archive path has drifted"):
                    (
                        ledger_recovery
                        ._commit_deepseek_transport_inspector_followup(
                            out_dir, manifest, stop_path, auth_path,
                            pending_path, malicious
                        )
                    )
            self.assertEqual(
                run_meta._sha256_file(auth_path), prior_auth_sha256)
            self.assertEqual(
                run_meta._sha256_file(stop_path), prior_stop_sha256)
            self.assertFalse(os.path.exists(history_dir))
            run_meta.write_json_atomic(pending_path, pending)

            with common_patches[0], common_patches[1], \
                    common_patches[2], common_patches[3], \
                    mock.patch.object(
                        ledger_recovery,
                        "_unlink_with_sharing_retry"):
                first = (
                    ledger_recovery
                    ._commit_deepseek_transport_inspector_followup(
                        out_dir, manifest, stop_path, auth_path,
                        pending_path, pending
                    )
                )
            self.assertEqual(first["authorization_id"], "followup-auth")
            self.assertTrue(os.path.isfile(pending_path))
            self.assertFalse(os.path.isfile(stop_path))
            self.assertEqual(
                ledger_recovery._read_json(auth_path), record)

            with common_patches[0], common_patches[1], \
                    common_patches[2], common_patches[3]:
                second = (
                    ledger_recovery
                    ._commit_deepseek_transport_inspector_followup(
                        out_dir, manifest, stop_path, auth_path,
                        pending_path, pending
                    )
                )
            self.assertEqual(second["authorization_id"], "followup-auth")
            self.assertFalse(os.path.exists(pending_path))
            witness = [
                row for row in ledger_recovery._read_jsonl(dispatch_path)
                if row.get("event")
                == "user_authorized_deepseek_transport_inspector_followup"
            ]
            self.assertEqual(len(witness), 1)

    def test_deepseek_pristine_pending_reprepare_shortens_history_path(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            pending_path = os.path.join(
                out_dir,
                run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            )
            manifest_path = os.path.join(
                out_dir, "dispatch_manifest.json")
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            api_path = os.path.join(out_dir, "api_calls.jsonl")
            old_authorization_id = (
                "dsv4f-inspector-20260728T144037+0800-16a9a52c")
            old_history_relative = (
                "recovery_history/" + old_authorization_id)
            old_history_dir = os.path.join(
                out_dir, old_history_relative)
            os.makedirs(old_history_dir)
            prior_auth = {"authorization_id": "prior-auth"}
            stop = {"condition": "dispatcher_integrity_failure"}
            run_meta.write_json_atomic(
                manifest_path, {"schema": paired_dispatch.SCHEMA})
            run_meta.write_json_atomic(auth_path, prior_auth)
            run_meta.write_json_atomic(stop_path, stop)
            run_meta.write_json_atomic(
                os.path.join(out_dir, "active_worker_set.json"),
                {"workers": {}},
            )
            run_meta.append_jsonl_locked(
                dispatch_path, {"event": "prior"})
            run_meta.append_jsonl_locked(
                api_path, {"request_id": "request-a"})
            prepared_fingerprint = {
                "model_openai.py": "same",
                "run_meta.py": "old",
            }
            current_fingerprint = {
                "model_openai.py": "same",
                "run_meta.py": "new",
            }
            prepared_commit = "a" * 40
            current_commit = "c" * 40
            prior_recovery_commit = "b" * 40
            prior_git_commit = "0" * 40
            record = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": old_authorization_id,
                "created_at": "2026-07-28T14:40:37+08:00",
                "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
                "authorization_basis": (
                    "explicit_user_resume_after_deepseek_recovery_"
                    "inspector_fix"
                ),
                "deepseek_transport_disconnect_retry_recovery": True,
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery": True,
                "dispatch_manifest_sha256": (
                    run_meta._sha256_file(manifest_path)),
                "recovery_git_commit": prepared_commit,
                "recovery_git_tree_state": "clean",
                "recovery_code_fingerprint": prepared_fingerprint,
                "prior_git_commit": prior_git_commit,
                "deepseek_resume_samples": ["sample-a"],
                "incident_api_rows": [{"row_number": 1}],
                "recovered_worker_launch_ids": ["worker-a"],
                "deepseek_pending_samples": [],
                "archived_emergency_stop_records": [],
                "deepseek_transport_disconnect_inspector_"
                "prior_recovery_git_commit": prior_recovery_commit,
                "deepseek_transport_disconnect_inspector_"
                "delta_changed_paths": ["delta.py"],
                "changed_tracked_paths": ["cumulative.py"],
                "superseded_authorization_path": (
                    old_history_relative
                    + "/superseded_campaign_recovery_authorization.json"
                ),
                "superseded_authorization_sha256": (
                    run_meta._sha256_file(auth_path)),
                "archived_stop_path": (
                    old_history_relative + "/campaign_stop.json"),
                "archived_stop_sha256": (
                    run_meta._sha256_file(stop_path)),
                "deepseek_transport_disconnect_inspector_prefixes": {
                    "api_calls.jsonl": (
                        ledger_recovery._file_prefix_evidence(api_path)),
                    "dispatch_log.jsonl": (
                        ledger_recovery._file_prefix_evidence(
                            dispatch_path)),
                },
            }
            pending = {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "created_at": "2026-07-28T14:40:37+08:00",
                "history_dir": old_history_relative,
                "authorization_record": record,
            }
            run_meta.write_json_atomic(pending_path, pending)
            old_pending_sha256 = run_meta._sha256_file(pending_path)

            tooling_paths = sorted({
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/run_meta.py",
                "HP_V8/src/test_model_openai.py",
            })

            def changed_paths(prior, current):
                self.assertEqual(current, current_commit)
                if prior == prepared_commit:
                    return tooling_paths
                if prior == prior_recovery_commit:
                    return ["delta.py"]
                if prior == prior_git_commit:
                    return ["cumulative.py"]
                self.fail(f"unexpected prior commit: {prior}")

            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(current_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=current_fingerprint), \
                    mock.patch.object(
                        ledger_recovery, "_git_changed_paths",
                        side_effect=changed_paths), \
                    mock.patch.object(
                        ledger_recovery.subprocess, "run",
                        return_value=mock.Mock(
                            stdout=(
                                current_commit + " "
                                + prepared_commit + "\n"
                            ))):
                updated = (
                    ledger_recovery
                    ._reprepare_pristine_deepseek_inspector_pending(
                        out_dir, pending_path, pending
                    )
                )
                (
                    ledger_recovery
                    ._validate_deepseek_inspector_reprepare_evidence(
                        out_dir, updated,
                        updated["authorization_record"],
                    )
                )
                tampered_pending = json.loads(json.dumps(updated))
                tampered_pending["authorization_record"][
                    "deepseek_transport_inspector_pending_reprepared_"
                    "from_sha256"
                ] = "f" * 64
                with self.assertRaisesRegex(
                        RuntimeError, "prior pending archive mismatch"):
                    (
                        ledger_recovery
                        ._validate_deepseek_inspector_reprepare_evidence(
                            out_dir, tampered_pending,
                            tampered_pending["authorization_record"],
                        )
                    )
                stripped_pending = json.loads(json.dumps(updated))
                stripped_record = stripped_pending[
                    "authorization_record"]
                for key in list(stripped_record):
                    if key.startswith(
                            "deepseek_transport_inspector_pending_"
                            "reprepared_"):
                        stripped_record.pop(key)
                stripped_pending.pop("reprepared_at")
                with self.assertRaisesRegex(
                        RuntimeError, "provenance was removed"):
                    (
                        ledger_recovery
                        ._validate_deepseek_inspector_reprepare_evidence(
                            out_dir, stripped_pending, stripped_record,
                        )
                    )
            updated_record = updated["authorization_record"]
            self.assertTrue(
                updated_record["authorization_id"].startswith("dsi-"))
            self.assertLess(
                len(updated_record["authorization_id"]),
                len(old_authorization_id),
            )
            self.assertEqual(
                updated_record["recovery_git_commit"], current_commit)
            self.assertEqual(
                updated_record["recovery_code_fingerprint"],
                current_fingerprint,
            )
            self.assertEqual(
                updated_record[
                    "deepseek_transport_inspector_pending_"
                    "reprepared_from_sha256"
                ],
                old_pending_sha256,
            )
            self.assertEqual(
                updated_record[
                    "deepseek_transport_inspector_pending_reprepared_"
                    "from_authorization_id"
                ],
                old_authorization_id,
            )
            self.assertEqual(os.listdir(old_history_dir), ["pending.json"])
            archived_pending = os.path.join(
                old_history_dir, "pending.json")
            self.assertEqual(
                run_meta._sha256_file(archived_pending),
                old_pending_sha256,
            )
            self.assertTrue(os.path.isfile(auth_path))
            self.assertTrue(os.path.isfile(stop_path))

            def run_meta_git(args, **_kwargs):
                if "rev-list" in args:
                    return mock.Mock(stdout=(
                        current_commit + " " + prepared_commit + "\n"))
                self.assertEqual(args[3:5], ["diff", "--name-only"])
                return mock.Mock(stdout=(
                    "HP_V8/src/authorize_ledger_lock_recovery.py\n"
                    "HP_V8/src/run_meta.py\n"
                    "HP_V8/src/test_model_openai.py\n"
                ))

            with mock.patch.object(
                    run_meta.subprocess, "run",
                    side_effect=run_meta_git), mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value=current_fingerprint):
                self.assertTrue(
                    run_meta
                    ._deepseek_inspector_reprepare_evidence_matches(
                        out_dir, updated_record
                    )
                )
                tampered_record = json.loads(json.dumps(updated_record))
                tampered_record[
                    "deepseek_transport_inspector_pending_reprepared_"
                    "from_authorization_id"
                ] = "forged"
                self.assertFalse(
                    run_meta
                    ._deepseek_inspector_reprepare_evidence_matches(
                        out_dir, tampered_record
                    )
                )
                stripped_record = json.loads(json.dumps(updated_record))
                for key in list(stripped_record):
                    if key.startswith(
                            "deepseek_transport_inspector_pending_"
                            "reprepared_"):
                        stripped_record.pop(key)
                self.assertFalse(
                    run_meta
                    ._deepseek_inspector_reprepare_evidence_matches(
                        out_dir, stripped_record
                    )
                )

    def test_deepseek_recovery_rebuilds_partial_archive_temp(self):
        with tempfile.TemporaryDirectory() as out_dir:
            source = os.path.join(out_dir, "source.json")
            destination = os.path.join(out_dir, "archive.json")
            with open(source, "wb") as handle:
                handle.write(b'{"authorization":"prior"}')
            short_temp = os.path.join(
                out_dir, ".recovery-copy.pending")
            with open(short_temp, "wb") as handle:
                handle.write(b'{"authorization":')
            expected = run_meta._sha256_file(source)
            ledger_recovery._ensure_bound_file_copy(
                source, destination, expected)
            self.assertEqual(
                run_meta._sha256_file(destination), expected)
            self.assertFalse(os.path.exists(short_temp))

    def test_deepseek_recovery_repairs_only_expected_partial_witness(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_log.jsonl")
            run_meta.append_jsonl_locked(path, {"event": "prior"})
            prefix = ledger_recovery._file_prefix_evidence(path)
            witness = {
                "event": (
                    "user_authorized_deepseek_transport_"
                    "inspector_followup"),
                "created_at": "2026-07-28T14:00:00+08:00",
                "campaign_recovery_authorization_id": "auth-a",
            }
            encoded = (
                json.dumps(
                    witness, ensure_ascii=False).encode("utf-8")
                + b"\n"
            )
            with open(path, "ab") as handle:
                handle.write(encoded[:len(encoded) // 2])
            ledger_recovery._append_recoverable_jsonl_tail(
                path, prefix, witness)
            self.assertEqual(
                ledger_recovery._read_jsonl(path),
                [{"event": "prior"}, witness],
            )

            with open(path, "r+b") as handle:
                handle.truncate(prefix["byte_count"])
                handle.seek(prefix["byte_count"])
                handle.write(b"conflict")
            with self.assertRaisesRegex(
                    RuntimeError, "JSONL tail conflicts"):
                ledger_recovery._append_recoverable_jsonl_tail(
                    path, prefix, witness)
