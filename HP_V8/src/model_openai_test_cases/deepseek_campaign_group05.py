"""Mixin slice for test_model_openai.DeepSeekOpenCodeCampaignGroup05Mixin."""

from .support import *


class DeepSeekOpenCodeCampaignGroup05Mixin:
    def test_dispatcher_parent_loss_code_transition_rejects_scope_drift(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            prior_commit = "1" * 40
            prior_recovery_commit = "2" * 40
            current_commit = "3" * 40
            manifest_fingerprint = {
                "model_openai.py": "manifest-model",
                "run_meta.py": "manifest-meta",
            }
            prior_recovery_fingerprint = {
                "model_openai.py": "same-model",
                "run_meta.py": "old-meta",
            }
            current_fingerprint = {
                "model_openai.py": "same-model",
                "run_meta.py": "new-meta",
            }
            manifest_digest = "4" * 64
            manifest = {
                "run_git_commit": prior_commit,
                "code_fingerprint": manifest_fingerprint,
            }
            authorization = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "prior-authorization",
                "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
                "dispatch_manifest_sha256": manifest_digest,
                "prior_git_commit": prior_commit,
                "prior_git_tree_state": "clean",
                "prior_code_fingerprint": manifest_fingerprint,
                "recovery_git_commit": prior_recovery_commit,
                "recovery_git_tree_state": "clean",
                "recovery_code_fingerprint": (
                    prior_recovery_fingerprint),
            }
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            run_meta.write_json_atomic(auth_path, authorization)
            authorization_sha256 = run_meta._sha256_file(auth_path)
            changed_paths = [
                "HP_V8/VERSION.md",
                "HP_V8/src/authorize_ledger_lock_recovery.py",
                "HP_V8/src/run_meta.py",
                "HP_V8/src/test_model_openai.py",
                "docs/active_log.md",
            ]

            def validate(parent_stdout, paths, fingerprint):
                with mock.patch.object(
                        ledger_recovery.subprocess, "run",
                        return_value=mock.Mock(stdout=parent_stdout)), \
                        mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=paths):
                    return (
                        ledger_recovery
                        ._validated_dispatcher_parent_loss_code_transition(
                            manifest,
                            manifest_digest,
                            auth_path,
                            authorization,
                            authorization_sha256,
                            current_commit,
                            "clean",
                            fingerprint,
                        )
                    )

            valid = validate(
                f"{current_commit} {prior_recovery_commit}\n",
                changed_paths,
                current_fingerprint,
            )
            self.assertEqual(
                valid["prior_authorization_sha256"],
                authorization_sha256,
            )
            drift_cases = [
                (
                    "merge-parent",
                    (
                        f"{current_commit} {prior_recovery_commit} "
                        f"{'5' * 40}\n"
                    ),
                    changed_paths,
                    current_fingerprint,
                ),
                (
                    "extra-path",
                    f"{current_commit} {prior_recovery_commit}\n",
                    changed_paths + ["HP_V8/src/model_openai.py"],
                    current_fingerprint,
                ),
                (
                    "extra-fingerprint",
                    f"{current_commit} {prior_recovery_commit}\n",
                    changed_paths,
                    {
                        **current_fingerprint,
                        "model_openai.py": "changed-model",
                    },
                ),
            ]
            for name, parent_stdout, paths, fingerprint in drift_cases:
                with self.subTest(name=name), self.assertRaisesRegex(
                        RuntimeError,
                        "recovery-tool transition is invalid"):
                    validate(parent_stdout, paths, fingerprint)

    def test_dispatcher_parent_loss_pending_reprepare_shortens_history_path(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            prior_git_commit = "0" * 40
            prior_recovery_commit = "1" * 40
            prepared_commit = "2" * 40
            current_commit = "3" * 40
            prior_fingerprint = {
                "model_openai.py": "same-model",
                "run_meta.py": "old-meta",
            }
            prepared_fingerprint = {
                "model_openai.py": "same-model",
                "run_meta.py": "new-meta",
            }
            transition_paths = sorted(
                ledger_recovery
                ._DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
            )
            amendment_paths = sorted(
                ledger_recovery
                ._DISPATCHER_PARENT_LOSS_PENDING_REPREPARE_CHANGED_PATHS
            )
            old_authorization_id = (
                "dispatcher-parent-loss-20260728T201341+0800-5798b181"
            )
            old_history_relative = (
                "recovery_history/" + old_authorization_id)
            old_history_dir = os.path.join(
                out_dir, old_history_relative)
            old_emergency_dir = os.path.join(
                old_history_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            os.makedirs(old_emergency_dir)

            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": prior_git_commit,
                "git_tree_state": "clean",
                "code_fingerprint": prior_fingerprint,
            }
            manifest_path = os.path.join(
                out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(manifest_path, manifest)
            active_path = os.path.join(
                out_dir, "active_worker_set.json")
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "run_git_commit": prior_git_commit,
                "dispatcher_pid": None,
                "dispatcher_instance_id": None,
                "workers": {},
            })
            metadata_path = os.path.join(
                out_dir, "run_metadata.jsonl")
            run_meta.append_jsonl_locked(
                metadata_path, {"status": "interrupted_by_dispatcher"})
            archived_active_path = os.path.join(
                old_history_dir, "active_worker_set.before.json")
            archived_metadata_path = os.path.join(
                old_history_dir, "run_metadata.before.jsonl")
            ledger_recovery._copy_file_durable(
                active_path, archived_active_path)
            ledger_recovery._copy_file_durable(
                metadata_path, archived_metadata_path)

            prior_authorization = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": "prior-authorization",
                "recovery_git_commit": prior_recovery_commit,
                "recovery_code_fingerprint": prior_fingerprint,
            }
            superseded_path = os.path.join(
                old_history_dir,
                "superseded_campaign_recovery_authorization.json",
            )
            run_meta.write_json_atomic(
                superseded_path, prior_authorization)

            stop_path = os.path.join(out_dir, "campaign_stop.json")
            stop = {
                "condition": "dispatcher_process_lost",
                "worker_launch_id": "worker-a",
            }
            run_meta.write_json_atomic(stop_path, stop)
            emergency_source_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            os.makedirs(emergency_source_dir)
            emergency_name = "worker-a.json"
            emergency_source = os.path.join(
                emergency_source_dir, emergency_name)
            run_meta.write_json_atomic(emergency_source, stop)

            worker = {
                "sample": "sample-a",
                "worker_launch_id": "worker-a",
                "worker_pid": 123,
                "invocation_id": "invocation-a",
                "status": "interrupted_by_dispatcher",
            }
            recovery_plan = {
                "dispatcher_pid": 456,
                "dispatcher_instance_id": "dispatcher-a",
                "workers": [worker],
            }
            record = {
                "schema": (
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_SCHEMA_V2),
                "authorization_id": old_authorization_id,
                "recovery_kind": (
                    run_meta.DISPATCHER_PROCESS_LOST_RECOVERY_KIND),
                "dispatcher_process_lost_recovery": True,
                "dispatcher_pid": recovery_plan["dispatcher_pid"],
                "dispatcher_instance_id": (
                    recovery_plan["dispatcher_instance_id"]),
                "dispatcher_parent_loss_workers": [worker],
                "dispatch_manifest_sha256": (
                    run_meta._sha256_file(manifest_path)),
                "prior_git_commit": prior_git_commit,
                "prior_code_fingerprint": prior_fingerprint,
                "recovery_git_commit": prepared_commit,
                "recovery_git_tree_state": "clean",
                "recovery_code_fingerprint": prepared_fingerprint,
                "changed_tracked_paths": transition_paths,
                "changed_code_fingerprint_keys": ["run_meta.py"],
                "archived_stop_path": (
                    old_history_relative + "/campaign_stop.json"),
                "archived_stop_sha256": (
                    run_meta._sha256_file(stop_path)),
                "archived_emergency_stop_records": [{
                    "path": (
                        old_history_relative + "/"
                        + run_meta.EMERGENCY_STOP_DIRECTORY + "/"
                        + emergency_name
                    ),
                    "sha256": run_meta._sha256_file(emergency_source),
                }],
                "archived_active_worker_set_path": (
                    old_history_relative
                    + "/active_worker_set.before.json"),
                "archived_active_worker_set_sha256": (
                    run_meta._sha256_file(archived_active_path)),
                "archived_run_metadata_path": (
                    old_history_relative
                    + "/run_metadata.before.jsonl"),
                "archived_run_metadata_sha256": (
                    run_meta._sha256_file(archived_metadata_path)),
                "superseded_authorization_path": (
                    old_history_relative
                    + "/superseded_campaign_recovery_authorization.json"
                ),
                "superseded_authorization_sha256": (
                    run_meta._sha256_file(superseded_path)),
                "dispatcher_parent_loss_code_transition": {
                    "prior_authorization_id": "prior-authorization",
                    "prior_authorization_sha256": (
                        run_meta._sha256_file(superseded_path)),
                    "prior_recovery_git_commit": prior_recovery_commit,
                    "prior_recovery_code_fingerprint": prior_fingerprint,
                    "delta_changed_paths": transition_paths,
                    "delta_changed_code_fingerprint_keys": [
                        "run_meta.py"],
                },
            }
            pending = {
                "schema": (
                    ledger_recovery
                    ._DISPATCHER_PARENT_LOSS_PENDING_SCHEMA),
                "created_at": "2026-07-28T20:13:57+08:00",
                "history_dir": old_history_relative,
                "authorization_record": record,
                "recovery_plan": recovery_plan,
            }
            auth_path = os.path.join(
                out_dir,
                run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
            )
            pending_path = os.path.join(
                out_dir,
                run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )
            run_meta.write_json_atomic(auth_path, record)
            run_meta.write_json_atomic(pending_path, pending)
            old_pending_sha256 = run_meta._sha256_file(pending_path)

            def changed_paths(prior, current):
                if (prior, current) in {
                    (prior_recovery_commit, prepared_commit),
                    (prior_recovery_commit, current_commit),
                    (prior_git_commit, prepared_commit),
                    (prior_git_commit, current_commit),
                }:
                    return transition_paths
                if (prior, current) == (
                        prepared_commit, current_commit):
                    return amendment_paths
                self.fail(
                    f"unexpected changed-path query: {prior} {current}")

            def git_run(args, **kwargs):
                commit = args[-1]
                if commit == prepared_commit:
                    return mock.Mock(stdout=(
                        f"{prepared_commit} {prior_recovery_commit}\n"))
                if commit == current_commit:
                    return mock.Mock(stdout=(
                        f"{current_commit} {prior_recovery_commit}\n"))
                self.fail(f"unexpected git query: {args}")

            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(current_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=prepared_fingerprint), \
                    mock.patch.object(
                        ledger_recovery, "_git_changed_paths",
                        side_effect=changed_paths), \
                    mock.patch.object(
                        ledger_recovery.subprocess, "run",
                        side_effect=git_run):
                updated = (
                    ledger_recovery
                    ._reprepare_dispatcher_parent_loss_pending_short_path(
                        out_dir, manifest, pending_path, pending,
                        auth_path, stop_path
                    )
                )
            short_history_root = os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_HISTORY_ROOT,
            )
            short_history_entries = sorted(
                os.listdir(short_history_root))
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(current_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=prepared_fingerprint):
                self.assertEqual(
                    ledger_recovery
                    ._reprepare_dispatcher_parent_loss_pending_short_path(
                        out_dir, manifest, pending_path, updated,
                        auth_path, stop_path
                    ),
                    updated,
                )
            self.assertEqual(
                sorted(os.listdir(short_history_root)),
                short_history_entries,
            )
            active_after_recovery = ledger_recovery._read_json(active_path)
            run_meta.write_json_atomic(active_path, {"workers": {}})
            with self.assertRaisesRegex(
                    RuntimeError, "pending active set is invalid"):
                (
                    ledger_recovery
                    ._reprepare_dispatcher_parent_loss_pending_short_path(
                        out_dir, manifest, pending_path, updated,
                        auth_path, stop_path
                    )
                )
            run_meta.write_json_atomic(active_path, active_after_recovery)

            updated_record = updated["authorization_record"]
            self.assertRegex(
                updated_record["authorization_id"],
                r"^dpl-[0-9a-f]{12}$",
            )
            self.assertEqual(
                updated["history_dir"],
                ledger_recovery._DISPATCHER_PARENT_LOSS_HISTORY_ROOT + "/"
                + updated_record["authorization_id"],
            )
            self.assertEqual(
                updated_record["recovery_git_commit"], current_commit)
            self.assertEqual(
                updated_record[
                    "dispatcher_parent_loss_pending_reprepared_from_"
                    "sha256"
                ],
                old_pending_sha256,
            )
            old_pending_archive = os.path.join(
                old_history_dir, "pending.json")
            self.assertEqual(
                run_meta._sha256_file(old_pending_archive),
                old_pending_sha256,
            )
            new_history_dir = os.path.join(
                out_dir, updated["history_dir"])
            for name in (
                    "active_worker_set.before.json",
                    "run_metadata.before.jsonl",
                    "superseded_campaign_recovery_authorization.json"):
                self.assertEqual(
                    run_meta._sha256_file(
                        os.path.join(old_history_dir, name)),
                    run_meta._sha256_file(
                        os.path.join(new_history_dir, name)),
                )
            self.assertTrue(os.path.isfile(stop_path))
            self.assertTrue(os.path.isfile(emergency_source))
            self.assertEqual(
                ledger_recovery._read_json(auth_path), record)

            archived_stop = os.path.join(
                new_history_dir, "campaign_stop.json")
            os.replace(stop_path, archived_stop)
            archived_emergency_dir = os.path.join(
                new_history_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            os.makedirs(archived_emergency_dir)
            os.replace(
                emergency_source,
                os.path.join(archived_emergency_dir, emergency_name),
            )
            validator_pending = json.loads(json.dumps(updated))
            validator_record = validator_pending[
                "authorization_record"]
            validator_record[
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery"
            ] = True
            validator_record[
                "deepseek_resume_classifier_followup_recovery"
            ] = True
            run_meta.write_json_atomic(auth_path, validator_record)
            run_meta.write_json_atomic(
                pending_path, validator_pending)
            validator_pending_sha256 = run_meta._sha256_file(
                pending_path)
            validator_commit = "4" * 40

            def validator_changed_paths(prior, current):
                if (prior, current) in {
                    (prior_recovery_commit, current_commit),
                    (prior_recovery_commit, validator_commit),
                    (prior_git_commit, current_commit),
                    (prior_git_commit, validator_commit),
                }:
                    return transition_paths
                if (prior, current) == (
                        current_commit, validator_commit):
                    return amendment_paths
                self.fail(
                    f"unexpected validator path query: {prior} {current}")

            def validator_git_run(args, **kwargs):
                commit = args[-1]
                if commit == current_commit:
                    return mock.Mock(stdout=(
                        f"{current_commit} {prior_recovery_commit}\n"))
                if commit == validator_commit:
                    return mock.Mock(stdout=(
                        f"{validator_commit} "
                        f"{prior_recovery_commit}\n"))
                self.fail(f"unexpected validator git query: {args}")

            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(validator_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=prepared_fingerprint), \
                    mock.patch.object(
                        ledger_recovery, "_git_changed_paths",
                        side_effect=validator_changed_paths), \
                    mock.patch.object(
                        ledger_recovery.subprocess, "run",
                        side_effect=validator_git_run):
                validator_updated = (
                    ledger_recovery
                    ._reprepare_dispatcher_parent_loss_pending_short_path(
                        out_dir, manifest, pending_path,
                        validator_pending, auth_path, stop_path
                    )
                )
            validator_updated_record = validator_updated[
                "authorization_record"]
            self.assertEqual(
                validator_updated_record["recovery_git_commit"],
                validator_commit,
            )
            self.assertNotIn(
                "deepseek_transport_disconnect_"
                "inspector_followup_recovery",
                validator_updated_record,
            )
            self.assertNotIn(
                "deepseek_resume_classifier_followup_recovery",
                validator_updated_record,
            )
            validator_archive = os.path.join(
                new_history_dir, "pending.validator-before.json")
            self.assertEqual(
                run_meta._sha256_file(validator_archive),
                validator_pending_sha256,
            )

            run_meta.write_json_atomic(
                auth_path, validator_updated_record)
            api_validator_pending_sha256 = run_meta._sha256_file(
                pending_path)
            api_validator_commit = "5" * 40
            api_validator_fingerprint = {
                **prepared_fingerprint,
                "run_meta.py": "latest-meta",
            }

            def api_validator_changed_paths(prior, current):
                if (prior, current) in {
                    (prior_recovery_commit, validator_commit),
                    (prior_recovery_commit, api_validator_commit),
                    (prior_git_commit, validator_commit),
                    (prior_git_commit, api_validator_commit),
                    (validator_commit, api_validator_commit),
                }:
                    return transition_paths
                self.fail(
                    "unexpected API-validator path query: "
                    f"{prior} {current}")

            def api_validator_git_run(args, **kwargs):
                commit = args[-1]
                if commit == validator_commit:
                    return mock.Mock(stdout=(
                        f"{validator_commit} "
                        f"{prior_recovery_commit}\n"))
                if commit == api_validator_commit:
                    return mock.Mock(stdout=(
                        f"{api_validator_commit} "
                        f"{prior_recovery_commit}\n"))
                self.fail(f"unexpected API-validator git query: {args}")

            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(api_validator_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=api_validator_fingerprint), \
                    mock.patch.object(
                        ledger_recovery, "_git_changed_paths",
                        side_effect=api_validator_changed_paths), \
                    mock.patch.object(
                        ledger_recovery.subprocess, "run",
                        side_effect=api_validator_git_run):
                api_validator_updated = (
                    ledger_recovery
                    ._reprepare_dispatcher_parent_loss_pending_short_path(
                        out_dir, manifest, pending_path,
                        validator_updated, auth_path, stop_path
                    )
                )
            api_validator_updated_record = api_validator_updated[
                "authorization_record"]
            self.assertEqual(
                api_validator_updated_record["recovery_git_commit"],
                api_validator_commit,
            )
            self.assertEqual(
                api_validator_updated_record[
                    "recovery_code_fingerprint"],
                api_validator_fingerprint,
            )
            api_validator_archive = os.path.join(
                new_history_dir,
                "pending.api-validator-before.json",
            )
            self.assertEqual(
                run_meta._sha256_file(api_validator_archive),
                api_validator_pending_sha256,
            )
            self.assertEqual(
                api_validator_updated_record[
                    "dispatcher_parent_loss_pending_api_validator_"
                    "reprepared_from_path"
                ],
                (
                    api_validator_updated["history_dir"]
                    + "/pending.api-validator-before.json"
                ),
            )
            run_meta._validate_dispatcher_parent_loss_pending_reprepare_witnesses(
                out_dir, api_validator_updated_record)
            run_meta.write_json_atomic(
                auth_path, api_validator_updated_record)
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(api_validator_commit, "clean")), \
                    mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=api_validator_fingerprint):
                self.assertEqual(
                    ledger_recovery
                    ._reprepare_dispatcher_parent_loss_pending_short_path(
                        out_dir, manifest, pending_path,
                        api_validator_updated, auth_path, stop_path
                    ),
                    api_validator_updated,
                )

            os.remove(pending_path)
            run_meta.write_json_atomic(active_path, {
                "schema": "anchorpatch.active_worker_set/1",
                "run_git_commit": prior_git_commit,
                "dispatcher_pid": None,
                "dispatcher_instance_id": None,
                "workers": {},
            })
            old_authorization_sha256 = run_meta._sha256_file(auth_path)
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"),
                {"event": "fixture"},
            )
            reader_commit = "6" * 40
            reader_fingerprint = {
                **api_validator_fingerprint,
                "run_meta.py": "reader-meta",
            }

            def reader_changed_paths(prior, current):
                if (prior, current) in {
                    (prior_recovery_commit, api_validator_commit),
                    (prior_recovery_commit, reader_commit),
                    (api_validator_commit, reader_commit),
                    (prior_git_commit, reader_commit),
                }:
                    return transition_paths
                self.fail(
                    f"unexpected reader-SHA path query: {prior} {current}")

            def reader_git_run(args, **kwargs):
                commit = args[-1]
                if commit in {api_validator_commit, reader_commit}:
                    return mock.Mock(stdout=(
                        f"{commit} {prior_recovery_commit}\n"))
                self.fail(f"unexpected reader-SHA git query: {args}")

            def read_updated_authorization(_out_dir):
                return {
                    "authorization_sha256": run_meta._sha256_file(
                        auth_path),
                }

            reader_patches = (
                mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(reader_commit, "clean")),
                mock.patch.object(
                    ledger_recovery, "code_fingerprint",
                    return_value=reader_fingerprint),
                mock.patch.object(
                    ledger_recovery, "_git_changed_paths",
                    side_effect=reader_changed_paths),
                mock.patch.object(
                    ledger_recovery.subprocess, "run",
                    side_effect=reader_git_run),
                mock.patch.object(
                    ledger_recovery, "_assert_worker_leases_free"),
                mock.patch.object(
                    ledger_recovery,
                    "read_campaign_recovery_authorization",
                    side_effect=read_updated_authorization),
            )
            metadata_bound_rows = [{
                "campaign_recovery_authorization": {
                    "authorization_id": (
                        api_validator_updated_record["authorization_id"]
                    ),
                },
            }]

            with contextlib.ExitStack() as stack:
                for patcher in reader_patches:
                    stack.enter_context(patcher)
                stack.enter_context(mock.patch.object(
                    ledger_recovery,
                    "read_run_metadata_snapshot",
                    return_value=metadata_bound_rows,
                ))
                with self.assertRaisesRegex(
                        RuntimeError, "already bound into run metadata"):
                    (
                        ledger_recovery
                        ._authorize_dispatcher_parent_loss_reader_sha_fix(
                            out_dir,
                            manifest,
                            auth_path,
                            old_authorization_sha256,
                        )
                    )
            with contextlib.ExitStack() as stack:
                for patcher in reader_patches:
                    stack.enter_context(patcher)
                reader_result = (
                    ledger_recovery
                    ._authorize_dispatcher_parent_loss_reader_sha_fix(
                        out_dir,
                        manifest,
                        auth_path,
                        old_authorization_sha256,
                    )
                )
            self.assertFalse(reader_result["already_authorized"])
            reader_record = ledger_recovery._read_json(auth_path)
            reader_prefix = (
                "dispatcher_parent_loss_authorization_"
                "reader_sha_reprepared_"
            )
            self.assertEqual(
                reader_record[reader_prefix + "from_sha256"],
                old_authorization_sha256,
            )
            reader_archive = os.path.join(
                new_history_dir, "authorization.reader-sha-before.json")
            self.assertEqual(
                run_meta._sha256_file(reader_archive),
                old_authorization_sha256,
            )
            run_meta._validate_dispatcher_parent_loss_pending_reprepare_witnesses(
                out_dir, reader_record)
            tampered_reader_record = copy.deepcopy(reader_record)
            tampered_reader_record[
                reader_prefix + "from_sha256"] = "f" * 64
            with self.assertRaisesRegex(
                    RuntimeError, "reader witness is invalid"):
                (
                    run_meta
                    ._validate_dispatcher_parent_loss_pending_reprepare_witnesses(
                        out_dir, tampered_reader_record
                    )
                )

            with contextlib.ExitStack() as stack:
                for patcher in reader_patches:
                    stack.enter_context(patcher)
                with self.assertRaisesRegex(
                        RuntimeError, "provenance is missing"):
                    (
                        ledger_recovery
                        ._authorize_dispatcher_parent_loss_reader_sha_fix(
                            out_dir,
                            manifest,
                            auth_path,
                            "f" * 64,
                        )
                    )
            with contextlib.ExitStack() as stack:
                for patcher in reader_patches:
                    stack.enter_context(patcher)
                repeated = (
                    ledger_recovery
                    ._authorize_dispatcher_parent_loss_reader_sha_fix(
                        out_dir,
                        manifest,
                        auth_path,
                        old_authorization_sha256,
                    )
                )
            self.assertTrue(repeated["already_authorized"])
            reader_events = [
                row for row in ledger_recovery._read_jsonl(
                    os.path.join(out_dir, "dispatch_log.jsonl"))
                if row.get("event")
                == "user_authorized_dispatcher_parent_loss_reader_sha_fix"
            ]
            self.assertEqual(len(reader_events), 1)

    def test_dispatcher_parent_loss_short_history_has_windows_path_budget(
            self):
        emergency_name = (
            "dispatcher_process_lost.12345."
            + "a" * 32 + ".json"
        )
        out_dir = "X:/" + "x" * 136
        history_dir = (
            out_dir + "/"
            + ledger_recovery._DISPATCHER_PARENT_LOSS_HISTORY_ROOT
            + "/dpl-" + "b" * 12
        )
        longest = os.path.join(
            history_dir, run_meta.EMERGENCY_STOP_DIRECTORY,
            emergency_name)
        self.assertEqual(len(out_dir), 139)
        self.assertEqual(len(emergency_name), 67)
        self.assertEqual(len(longest), 250)
        with mock.patch.object(ledger_recovery.os, "name", "nt"):
            ledger_recovery._assert_dispatcher_parent_loss_path_budget(
                history_dir, [emergency_name])
            with self.assertRaisesRegex(
                    RuntimeError, "Windows legacy path budget"):
                (
                    ledger_recovery
                    ._assert_dispatcher_parent_loss_path_budget(
                        history_dir + "x" * 10,
                        [emergency_name],
                    )
                )

    def test_dispatcher_parent_loss_recovery_covers_startup_windows(self):
        stages = ["preauthorization", "registered_prelaunch"]
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, stages)
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
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
                            "sample": item["sample"],
                            "methods": ["hybridpatch", "fullrewrite"],
                        } for item in fixture["workers"]]))

            expected_samples = {
                item["sample"] for item in fixture["workers"]
            }
            self.assertEqual(result["reconciled_workers"], 2)
            self.assertEqual(set(result["resume_samples"]), expected_samples)
            self.assertEqual(allowed, expected_samples)
            workers = {
                item["sample"]: item
                for item in authorization[
                    "dispatcher_parent_loss_workers"]
            }
            self.assertEqual(
                workers["parent-loss-0"]["status"], "preauthorization")
            self.assertEqual(
                workers["parent-loss-1"]["status"],
                "registered_prelaunch")
            self.assertEqual(
                authorization["preauthorization_worker_launch_ids"],
                ["worker-parent-loss-0"])
            self.assertEqual(
                authorization[
                    "dispatcher_parent_loss_registered_prelaunch_worker_launch_ids"],
                ["worker-parent-loss-1"])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "run_metadata.jsonl")))
            events = ledger_recovery._read_jsonl(os.path.join(
                out_dir, "dispatch_log.jsonl"))
            self.assertEqual(
                [
                    row["worker_launch_id"] for row in events
                    if row.get("event") == "worker_exit"
                ],
                ["worker-parent-loss-0"])

    def test_parent_loss_authorizer_proves_only_zero_post_unpublished_capability(self):
        for with_attempt_evidence in (False, True):
            with self.subTest(with_attempt_evidence=with_attempt_evidence), \
                    tempfile.TemporaryDirectory() as out_dir:
                fixture = self._write_dispatcher_parent_loss_fixture(
                    out_dir, ["running", "running"], metadata_mode="event")
                manifest = fixture["manifest"]
                manifest["config"]["provider_guard_mode"] = (
                    paired_dispatch
                    .PROVIDER_GUARD_WORKER_START_CAPABILITY_V1)
                states = [
                    f"state-{index}"
                    for index in range(
                        1,
                        paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS + 1,
                    )
                ]
                for worker in fixture["workers"]:
                    sample = worker["sample"]
                    plan_path = os.path.join(
                        out_dir, f"{sample}.task_plan.json")
                    utils_relay_plan.save_relay_task_plan(plan_path, states)
                    manifest["task_plans"][sample] = {
                        "path": os.path.basename(plan_path),
                        "sha256": paired_dispatch._sha256(plan_path),
                        "forward_state_sequence": states,
                    }
                run_meta.write_json_atomic(
                    os.path.join(out_dir, "dispatch_manifest.json"), manifest)
                manifest_digest = (
                    paired_dispatch._canonical_record_sha256(manifest))
                workers_by_id = {
                    worker["worker_launch_id"]: worker
                    for worker in fixture["workers"]
                }
                dispatch_path = os.path.join(
                    out_dir, "dispatch_log.jsonl")
                dispatch_rows = ledger_recovery._read_jsonl(dispatch_path)
                capabilities = {}
                rewritten = []
                for row in dispatch_rows:
                    if row.get("event") != "worker_authorized":
                        rewritten.append(row)
                        continue
                    worker_id = row["worker_launch_id"]
                    worker = workers_by_id[worker_id]
                    sample = worker["sample"]
                    capability = {
                        "schema": "anchorpatch.worker_start/2",
                        "worker_launch_id": worker_id,
                        "worker_pid": worker["worker_pid"],
                        "invocation_id": worker["invocation_id"],
                        "sample": sample,
                        "task_plan_sha256": manifest[
                            "task_plans"][sample]["sha256"],
                        "provider_guard_mode": (
                            paired_dispatch
                            .PROVIDER_GUARD_WORKER_START_CAPABILITY_V1),
                        "dispatcher_pid": fixture["dispatcher_pid"],
                        "dispatcher_instance_id": fixture[
                            "dispatcher_instance_id"],
                        "run_git_commit": fixture["commit"],
                        "transport_revision": (
                            paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                        "dispatch_manifest_canonical_sha256": (
                            manifest_digest),
                    }
                    capabilities[worker_id] = capability
                    rewritten.append({
                        "event": "worker_authorized",
                        **capability,
                        "worker_start_capability_sha256": (
                            paired_dispatch._canonical_record_sha256(
                                capability)),
                    })
                run_meta._write_jsonl_atomic(dispatch_path, rewritten)

                published_worker = fixture["workers"][0]
                _ready_path, start_path = paired_dispatch._worker_barrier_paths(
                    out_dir, published_worker["worker_launch_id"])
                run_meta.write_json_atomic(
                    start_path,
                    capabilities[published_worker["worker_launch_id"]])
                missing_worker = fixture["workers"][1]
                if with_attempt_evidence:
                    run_meta.append_jsonl_locked(
                        os.path.join(
                            out_dir, "api_attempt_ledger.jsonl"), {
                            "schema": run_meta.API_ATTEMPT_SCHEMA,
                            "worker_launch_id": missing_worker[
                                "worker_launch_id"],
                            "sample": missing_worker["sample"],
                            "semantic_call_id": (
                                "fullrewrite/"
                                f"{missing_worker['sample']}/rt01/forward/"
                                "fullrewrite_primary/g000"),
                            "event": "semantic_request",
                        })

                commit = fixture["commit"]
                fingerprint = fixture["fingerprint"]
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
                    ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)
                    authorization = (
                        run_meta.read_campaign_recovery_authorization(out_dir))

                expected = (
                    [] if with_attempt_evidence else
                    [missing_worker["worker_launch_id"]])
                self.assertEqual(
                    authorization[
                        "unpublished_worker_start_capability_ids"],
                    expected,
                )

    def test_stopless_registered_prelaunch_has_recovery_entry(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            worker = fixture["workers"][0]
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
                            "methods": ["hybridpatch", "fullrewrite"],
                        }]))

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertEqual(result["resume_samples"], [worker["sample"]])
            self.assertEqual(allowed, {worker["sample"]})
            self.assertEqual(
                authorization["dispatcher_parent_loss_workers"], [{
                    "sample": worker["sample"],
                    "worker_launch_id": worker["worker_launch_id"],
                    "worker_pid": None,
                    "invocation_id": None,
                    "status": "registered_prelaunch",
                }])
            archived_stop = ledger_recovery._read_json(os.path.join(
                out_dir,
                authorization["archived_stop_path"],
            ))
            self.assertIs(archived_stop["registered_prelaunch_only"], True)
            self.assertEqual(
                archived_stop["registered_worker_launch_ids"],
                [worker["worker_launch_id"]],
            )
            self.assertIsNone(archived_stop["worker_launch_id"])
            self.assertIsNone(archived_stop["worker_pid"])
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_stopless_parent_loss_rejects_launched_worker(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["preauthorization"], record_stop=False)
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            with mock.patch.object(
                    ledger_recovery, "_git_identity",
                    return_value=(commit, "clean")), mock.patch.object(
                        ledger_recovery, "code_fingerprint",
                        return_value=fingerprint):
                with self.assertRaisesRegex(
                        RuntimeError,
                        "limited to workers registered before process launch"):
                    ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)

            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))

    def test_synthetic_prelaunch_stop_absorbs_late_watchdog_record(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            worker = fixture["workers"][0]
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            synthetic = (
                ledger_recovery
                ._record_stopless_registered_prelaunch_parent_loss(
                    out_dir, fixture["manifest"], stop_path))
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": worker[
                            "worker_launch_id"],
                    },
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=os.getpid() + 1000):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=fixture["dispatcher_pid"],
                    dispatcher_instance_id=fixture[
                        "dispatcher_instance_id"],
                )

            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir),
                [synthetic],
            )
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            self.assertFalse(
                os.path.isdir(emergency_dir)
                and any(
                    name.endswith(".json")
                    for name in os.listdir(emergency_dir)
                )
            )

    def test_recovered_active_set_blocks_late_watchdog_relatched_stop(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            worker = fixture["workers"][0]
            paired_dispatch._write_active_worker_set(
                out_dir, fixture["manifest"], [])
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": worker[
                            "worker_launch_id"],
                    },
                    clear=False):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=fixture["dispatcher_pid"],
                    dispatcher_instance_id=fixture[
                        "dispatcher_instance_id"],
                )

            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_parent_loss_recovery_serializes_late_watchdog_publication(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["registered_prelaunch"], record_stop=False)
            worker = fixture["workers"][0]
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            ledger_recovery._record_stopless_registered_prelaunch_parent_loss(
                out_dir, fixture["manifest"], stop_path)
            publication_entered = threading.Event()
            allow_publication = threading.Event()
            original_link = os.link
            watchdog_errors = []
            recovery_results = []
            recovery_errors = []

            def blocked_link(source, destination):
                publication_entered.set()
                if not allow_publication.wait(timeout=5):
                    raise RuntimeError("test publication barrier timed out")
                return original_link(source, destination)

            def publish_watchdog():
                try:
                    run_meta.record_emergency_campaign_stop_condition(
                        out_dir,
                        "dispatcher_process_lost",
                        dispatcher_pid=fixture["dispatcher_pid"],
                        dispatcher_instance_id=fixture[
                            "dispatcher_instance_id"],
                    )
                except Exception as exc:
                    watchdog_errors.append(exc)

            def recover_parent():
                try:
                    recovery_results.append(ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True))
                except Exception as exc:
                    recovery_errors.append(exc)

            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": worker[
                            "worker_launch_id"],
                    },
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=os.getpid() + 1000), mock.patch.object(
                            run_meta.os, "link",
                            side_effect=blocked_link), mock.patch.object(
                                ledger_recovery, "_git_identity",
                                return_value=(commit, "clean")), \
                    mock.patch.object(
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
                watchdog = threading.Thread(
                    target=publish_watchdog, daemon=True)
                watchdog.start()
                self.assertTrue(publication_entered.wait(timeout=5))
                recovery = threading.Thread(
                    target=recover_parent, daemon=True)
                recovery.start()
                recovery.join(timeout=0.1)
                self.assertTrue(recovery.is_alive())
                allow_publication.set()
                watchdog.join(timeout=5)
                recovery.join(timeout=5)

            self.assertFalse(watchdog.is_alive())
            self.assertFalse(recovery.is_alive())
            self.assertEqual(watchdog_errors, [])
            self.assertEqual(recovery_errors, [])
            self.assertEqual(len(recovery_results), 1)
            self.assertEqual(
                recovery_results[0]["resume_samples"], [worker["sample"]])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                ledger_recovery._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_parent_loss_stop_recovers_unrecorded_worker_launch(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running", "registered_prelaunch"])
            unrecorded = fixture["workers"][1]
            unrecorded_pid = os.getpid() + 1000
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": unrecorded[
                            "worker_launch_id"],
                    },
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=unrecorded_pid):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=fixture["dispatcher_pid"],
                    dispatcher_instance_id=fixture[
                        "dispatcher_instance_id"],
                )
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
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
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))

            workers = {
                item["worker_launch_id"]: item
                for item in authorization[
                    "dispatcher_parent_loss_workers"]
            }
            self.assertEqual(
                workers[unrecorded["worker_launch_id"]]["status"],
                "preauthorization")
            self.assertEqual(
                workers[unrecorded["worker_launch_id"]]["worker_pid"],
                unrecorded_pid)
            launches = [
                row for row in ledger_recovery._read_jsonl(os.path.join(
                    out_dir, "dispatch_log.jsonl"))
                if row.get("event") == "launch"
                and row.get("worker_launch_id")
                == unrecorded["worker_launch_id"]
            ]
            self.assertEqual(len(launches), 1)
            self.assertIs(launches[0]["launch_observed"], False)
            self.assertTrue(launches[0]["reconciled_after_parent_loss"])
            self.assertEqual(
                len(authorization["archived_emergency_stop_records"]), 1)

    def test_dispatcher_parent_loss_recovery_retries_transaction_once(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            real_archive = ledger_recovery._archive_campaign_stop_cohort
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
                with mock.patch.object(
                        ledger_recovery,
                        "_archive_campaign_stop_cohort",
                        side_effect=RuntimeError(
                            "injected archive interruption")):
                    with self.assertRaisesRegex(
                            RuntimeError, "injected archive interruption"):
                        ledger_recovery.authorize(
                            out_dir, dispatcher_process_lost=True)
                pending_path = os.path.join(
                    out_dir,
                    ledger_recovery
                    ._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
                )
                self.assertTrue(os.path.isfile(pending_path))
                self.assertTrue(os.path.isfile(os.path.join(
                    out_dir,
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
                )))
                self.assertTrue(
                    run_meta.read_campaign_stop_conditions(out_dir))
                with mock.patch.object(
                        ledger_recovery,
                        "_archive_campaign_stop_cohort",
                        wraps=real_archive):
                    result = ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertFalse(os.path.exists(pending_path))
            self.assertEqual(run_meta.read_campaign_stop_conditions(out_dir), [])
            events = ledger_recovery._read_jsonl(os.path.join(
                out_dir, "dispatch_log.jsonl"))
            self.assertEqual(sum(
                row.get("event") == "worker_exit" for row in events), 1)
            self.assertEqual(sum(
                row.get("event") == "stale_worker_reconciled"
                for row in events), 1)
            self.assertEqual(sum(
                row.get("event")
                == "user_authorized_dispatcher_parent_loss_recovery"
                for row in events), 1)

    def test_parent_loss_pending_after_archive_blocks_resume(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            real_read = (
                ledger_recovery.read_campaign_recovery_authorization)
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
                with mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        side_effect=RuntimeError(
                            "injected post-archive interruption")):
                    with self.assertRaisesRegex(
                            RuntimeError, "post-archive interruption"):
                        ledger_recovery.authorize(
                            out_dir, dispatcher_process_lost=True)
                self.assertEqual(
                    run_meta.read_campaign_stop_conditions(out_dir), [])
                with self.assertRaisesRegex(
                        RuntimeError, "transaction is pending"):
                    run_meta.read_campaign_recovery_authorization(out_dir)
                with self.assertRaisesRegex(
                        RuntimeError, "transaction is pending"):
                    paired_dispatch._launch_under_lease(
                        mock.Mock(), out_dir)
                with mock.patch.object(
                        ledger_recovery,
                        "read_campaign_recovery_authorization",
                        wraps=real_read):
                    result = ledger_recovery.authorize(
                        out_dir, dispatcher_process_lost=True)

            self.assertEqual(result["reconciled_workers"], 1)
            self.assertFalse(os.path.exists(os.path.join(
                out_dir,
                run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
            )))

    def test_parent_loss_pending_after_event_unlinks_idempotently(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["running"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            git_result = mock.Mock(stdout="")
            real_unlink = ledger_recovery._unlink_with_sharing_retry
            pending_name = (
                run_meta.DISPATCHER_PARENT_LOSS_PENDING_FILENAME)

            def interrupt_pending_unlink(path):
                if os.path.basename(path) == pending_name:
                    raise RuntimeError("injected pending unlink interruption")
                return real_unlink(path)

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
                with mock.patch.object(
                        ledger_recovery, "_unlink_with_sharing_retry",
                        side_effect=interrupt_pending_unlink):
                    with self.assertRaisesRegex(
                            RuntimeError, "pending unlink interruption"):
                        ledger_recovery.authorize(
                            out_dir, dispatcher_process_lost=True)
                with self.assertRaisesRegex(
                        RuntimeError, "transaction is pending"):
                    run_meta.read_campaign_recovery_authorization(out_dir)
                result = ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)

            self.assertEqual(result["reconciled_workers"], 1)
            events = ledger_recovery._read_jsonl(os.path.join(
                out_dir, "dispatch_log.jsonl"))
            self.assertEqual(sum(
                row.get("event")
                == "user_authorized_dispatcher_parent_loss_recovery"
                for row in events), 1)

    def test_emergency_only_archive_retry_rebuilds_canonical_stop(self):
        stop = {
            "schema": run_meta.STOP_CONDITION_SCHEMA,
            "created_at": "2026-07-28T00:00:00+08:00",
            "condition": "dispatcher_process_lost",
            "worker_launch_id": "worker-a",
            "worker_pid": 123,
            "dispatcher_pid": 456,
            "dispatcher_instance_id": "dispatcher-instance-a",
        }
        with tempfile.TemporaryDirectory() as out_dir:
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            history_dir = os.path.join(
                out_dir, "recovery_history", "authorization-a")
            os.makedirs(emergency_dir)
            os.makedirs(history_dir)
            run_meta.write_json_atomic(
                os.path.join(emergency_dir, "worker-a.json"), stop)
            stop_path = os.path.join(out_dir, "campaign_stop.json")
            with mock.patch.object(
                    ledger_recovery, "_copy_file_durable",
                    side_effect=RuntimeError(
                        "injected canonical reconstruction interruption")):
                with self.assertRaisesRegex(
                        RuntimeError, "canonical reconstruction"):
                    ledger_recovery._archive_campaign_stop_cohort(
                        out_dir, stop_path, history_dir)
            archived_stop, archived_emergency = (
                ledger_recovery._archive_campaign_stop_cohort(
                    out_dir, stop_path, history_dir))

            self.assertTrue(os.path.isfile(archived_stop))
            self.assertEqual(len(archived_emergency), 1)
            self.assertEqual(
                ledger_recovery._read_json(archived_stop), stop)
            self.assertEqual(
                run_meta.read_campaign_stop_conditions(out_dir), [])

    def test_repeated_dispatcher_parent_loss_keeps_prior_pending_worker(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["preauthorization", "registered_prelaunch"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            manifest = fixture["manifest"]
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
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                prior_authorization_path = os.path.join(
                    out_dir,
                    run_meta.CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
                )
                prior_authorization = ledger_recovery._read_json(
                    prior_authorization_path)
                prior_authorization[
                    "dispatcher_parent_loss_code_transition"] = {
                        "fixture": "reader-amendment",
                    }
                run_meta.write_json_atomic(
                    prior_authorization_path, prior_authorization)
                prior_authorization_before_sha256 = (
                    run_meta._sha256_file(prior_authorization_path))
                reader_prefix = (
                    "dispatcher_parent_loss_authorization_"
                    "reader_sha_reprepared_"
                )
                prior_history_relative = prior_authorization[
                    "archived_stop_path"].rsplit("/", 1)[0]
                reader_archive_relative = (
                    prior_history_relative
                    + "/authorization.reader-sha-before.json"
                )
                reader_archive_path = os.path.join(
                    out_dir, reader_archive_relative)
                ledger_recovery._copy_file_durable(
                    prior_authorization_path, reader_archive_path)
                prior_authorization.update({
                    reader_prefix + "from_commit": commit,
                    reader_prefix + "from_sha256": (
                        prior_authorization_before_sha256),
                    reader_prefix + "from_path": reader_archive_relative,
                    reader_prefix + "at": "2026-07-28T20:30:00+08:00",
                })
                run_meta.write_json_atomic(
                    prior_authorization_path, prior_authorization)
                prior_authorization_sha256 = run_meta._sha256_file(
                    prior_authorization_path)
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "dispatch_log.jsonl"), {
                        "event": (
                            "user_authorized_dispatcher_parent_loss_"
                            "reader_sha_fix"
                        ),
                        "created_at": prior_authorization[
                            reader_prefix + "at"],
                        "campaign_recovery_authorization_id": (
                            prior_authorization["authorization_id"]),
                        "campaign_recovery_authorization_sha256": (
                            prior_authorization_sha256),
                        "superseded_authorization_sha256": (
                            prior_authorization_before_sha256),
                        "prior_recovery_git_commit": commit,
                        "recovery_git_commit": commit,
                    },
                )
                self.assertEqual(
                    run_meta.read_campaign_recovery_authorization(
                        out_dir)["authorization_sha256"],
                    prior_authorization_sha256,
                )
                parent_loss_history = os.path.join(
                    out_dir,
                    ledger_recovery._DISPATCHER_PARENT_LOSS_HISTORY_ROOT,
                )
                recovery_history_before = sorted(os.listdir(
                    parent_loss_history))

                sample = fixture["workers"][0]["sample"]
                worker_id = "worker-parent-loss-repeat"
                worker_pid = os.getpid()
                invocation_id = "invocation-parent-loss-repeat"
                dispatcher_pid = 54321
                dispatcher_instance_id = "dispatcher-instance-b"
                paired_dispatch._write_active_worker_set(
                    out_dir, manifest, [{
                        "sample": sample,
                        "worker_launch_id": worker_id,
                        "dispatcher_pid": dispatcher_pid,
                        "dispatcher_instance_id": dispatcher_instance_id,
                    }])
                dispatch_path = os.path.join(
                    out_dir, "dispatch_log.jsonl")
                intent = {
                    "event": "launch_intent",
                    "sample": sample,
                    "key_label": "KEY_1",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "worker_launch_id": worker_id,
                    "console_log": (
                        f"dispatch_logs/{sample}.repeat.console.log"),
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
                    "sample": sample,
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
                        "samples": [sample],
                        "methods": ["hybridpatch", "fullrewrite"],
                        "method_phase": None,
                        "model": paired_dispatch.DEEPSEEK_MODEL,
                        "provider": "opencode_zen",
                        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                        "transport_revision": (
                            paired_dispatch
                            .DEEPSEEK_TRANSPORT_REVISION),
                        "transport_resume_policy": None,
                        "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                        "reasoning_effort": (
                            paired_dispatch
                            .DEEPSEEK_REASONING_EFFORT),
                        "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                        "campaign_config": {
                            "reasoning_effort": (
                                paired_dispatch
                                .DEEPSEEK_REASONING_EFFORT),
                        },
                        "run_git_commit": commit,
                        "git_tree_state": "clean",
                        "code_fingerprint": fingerprint,
                        "status": "running",
                        "finished_at": None,
                    })
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
                transition_paths = sorted(
                    ledger_recovery
                    ._DISPATCHER_PARENT_LOSS_CODE_TRANSITION_CHANGED_PATHS
                )
                self.assertEqual(transition_paths, [
                    "HP_V8/VERSION.md",
                    "HP_V8/src/authorize_ledger_lock_recovery.py",
                    "HP_V8/src/run_meta.py",
                    "HP_V8/src/test_model_openai.py",
                    "docs/active_log.md",
                ])
                recovery_commit = "5" * 40
                recovery_fingerprint = dict(fingerprint)
                recovery_fingerprint["run_meta.py"] = "5" * 64

                def transition_git_run(args, **kwargs):
                    if "rev-list" in args:
                        return mock.Mock(stdout=(
                            f"{recovery_commit} {commit}\n"))
                    if "diff" in args:
                        return mock.Mock(
                            stdout="\n".join(transition_paths) + "\n")
                    return mock.Mock(stdout="")

                with mock.patch.object(
                        ledger_recovery, "_git_identity",
                        return_value=(recovery_commit, "clean")), \
                        mock.patch.object(
                            ledger_recovery, "code_fingerprint",
                            return_value=recovery_fingerprint), \
                        mock.patch.object(
                            ledger_recovery, "_git_changed_paths",
                            return_value=transition_paths), \
                        mock.patch.object(
                            run_meta, "_git_identity",
                            return_value=(recovery_commit, "clean")), \
                        mock.patch.object(
                            run_meta, "code_fingerprint",
                            return_value=recovery_fingerprint), \
                        mock.patch.object(
                            run_meta.subprocess, "run",
                            side_effect=transition_git_run):
                    with self.assertRaisesRegex(
                            RuntimeError,
                            "prior authorization transition is invalid"):
                        ledger_recovery.authorize(
                            out_dir,
                            dispatcher_process_lost=True,
                            validated_prior_authorization_sha256="0" * 64,
                        )
                    self.assertEqual(
                        run_meta._sha256_file(os.path.join(
                            out_dir,
                            run_meta
                            .CAMPAIGN_RECOVERY_AUTHORIZATION_FILENAME,
                        )),
                        prior_authorization_sha256,
                    )
                    self.assertEqual(
                        sorted(os.listdir(parent_loss_history)),
                        recovery_history_before,
                    )
                    self.assertFalse(os.path.exists(os.path.join(
                        out_dir,
                        ledger_recovery
                        ._DISPATCHER_PARENT_LOSS_PENDING_FILENAME,
                    )))
                    self.assertTrue(os.path.isfile(os.path.join(
                        out_dir, "campaign_stop.json")))
                    ledger_recovery.authorize(
                        out_dir,
                        dispatcher_process_lost=True,
                        validated_prior_authorization_sha256=(
                            prior_authorization_sha256),
                    )
                    authorization = (
                        run_meta.read_campaign_recovery_authorization(
                            out_dir))
                    self.assertFalse(any(
                        key.startswith(reader_prefix)
                        for key in authorization
                    ))
                    incidents = (
                        run_meta.campaign_recovery_incident_evidence(out_dir))
                    prior_pending_sample = fixture["workers"][1]["sample"]
                    allowed = (
                        paired_dispatch
                        ._verified_deepseek_resume_missing_samples(
                            out_dir, [{
                                "sample": prior_pending_sample,
                                "methods": [
                                    "hybridpatch", "fullrewrite"],
                            }]))

            resume_workers = {
                item["sample"]: item
                for item in authorization[
                    "dispatcher_parent_loss_resume_workers"]
            }
            self.assertEqual(
                resume_workers[sample]["worker_launch_id"], worker_id)
            self.assertEqual(
                resume_workers[sample]["status"],
                "interrupted_by_dispatcher")
            self.assertEqual(
                resume_workers[prior_pending_sample]["status"],
                "registered_prelaunch")
            self.assertEqual(
                incidents["dispatcher_parent_loss_workers"][
                    prior_pending_sample]["status"],
                "registered_prelaunch")
            self.assertEqual(allowed, {prior_pending_sample})
            self.assertEqual(
                authorization["dispatcher_parent_loss_code_transition"]
                ["prior_authorization_sha256"],
                prior_authorization_sha256,
            )
            self.assertEqual(
                authorization["recovery_git_commit"], recovery_commit)
            self.assertEqual(
                authorization["recovery_code_fingerprint"],
                recovery_fingerprint,
            )
            self.assertEqual(
                authorization["superseded_authorization_sha256"],
                prior_authorization_sha256,
            )
            transition = authorization[
                "dispatcher_parent_loss_code_transition"]
            self.assertEqual(
                transition["delta_changed_paths"], transition_paths)
            self.assertEqual(
                transition["delta_changed_code_fingerprint_keys"],
                ["run_meta.py"],
            )

    def test_repeated_parent_loss_removes_now_finished_resume_sample(self):
        with tempfile.TemporaryDirectory() as out_dir:
            fixture = self._write_dispatcher_parent_loss_fixture(
                out_dir, ["preauthorization", "registered_prelaunch"])
            commit = fixture["commit"]
            fingerprint = fixture["fingerprint"]
            manifest = fixture["manifest"]
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
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)

                sample = fixture["workers"][0]["sample"]
                prior_pending_sample = fixture["workers"][1]["sample"]
                worker_id = "worker-parent-loss-finished"
                worker_pid = os.getpid() + 1000
                invocation_id = "invocation-parent-loss-finished"
                dispatcher_pid = 54321
                dispatcher_instance_id = "dispatcher-instance-finished"
                paired_dispatch._write_active_worker_set(
                    out_dir, manifest, [{
                        "sample": sample,
                        "worker_launch_id": worker_id,
                        "dispatcher_pid": dispatcher_pid,
                        "dispatcher_instance_id": dispatcher_instance_id,
                    }])
                dispatch_path = os.path.join(
                    out_dir, "dispatch_log.jsonl")
                intent = {
                    "event": "launch_intent",
                    "sample": sample,
                    "key_label": "KEY_1",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "worker_launch_id": worker_id,
                    "console_log": (
                        f"dispatch_logs/{sample}.finished.console.log"),
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
                    "sample": sample,
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
                        "samples": [sample],
                        "methods": ["hybridpatch", "fullrewrite"],
                        "method_phase": None,
                        "model": paired_dispatch.DEEPSEEK_MODEL,
                        "provider": "opencode_zen",
                        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                        "transport_revision": (
                            paired_dispatch
                            .DEEPSEEK_TRANSPORT_REVISION),
                        "transport_resume_policy": None,
                        "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                        "reasoning_effort": (
                            paired_dispatch
                            .DEEPSEEK_REASONING_EFFORT),
                        "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                        "campaign_config": {
                            "reasoning_effort": (
                                paired_dispatch
                                .DEEPSEEK_REASONING_EFFORT),
                        },
                        "run_git_commit": commit,
                        "git_tree_state": "clean",
                        "code_fingerprint": fingerprint,
                        "status": "finished",
                        "finished_at": "2026-07-28T00:00:00+08:00",
                    })
                expected_progress = {}
                for method in ("hybridpatch", "fullrewrite"):
                    method_dir = os.path.join(out_dir, method)
                    os.makedirs(method_dir, exist_ok=True)
                    rows = [
                        {
                            "sample_id": sample,
                            "method": method,
                            "round_trip_num": rt_index,
                            "round_trip_direction": direction,
                        }
                        for rt_index in range(1, 11)
                        for direction in ("forward", "backward")
                    ]
                    with open(
                            os.path.join(
                                method_dir, f"{sample}.jsonl"),
                            "w", encoding="utf-8") as handle:
                        for row in rows:
                            handle.write(json.dumps(row) + "\n")
                    run_meta.write_json_atomic(
                        os.path.join(
                            method_dir, f"{sample}.ckpt.json"),
                        {"completed_round_trips": 10},
                    )
                    expected_progress[method] = {
                        "completed_round_trips": 10,
                        "committed_rows": 20,
                    }
                with mock.patch.dict(
                        os.environ,
                        {"ANCHORPATCH_WORKER_LAUNCH_ID": worker_id},
                        clear=False), mock.patch.object(
                            run_meta.os, "getpid",
                            return_value=worker_pid):
                    run_meta.record_sample_outcome(
                        out_dir,
                        sample,
                        "finished",
                        invocation_id=invocation_id,
                        methods=["hybridpatch", "fullrewrite"],
                        checkpoint_progress=expected_progress,
                    )
                    run_meta.record_campaign_stop_condition(
                        out_dir,
                        "dispatcher_process_lost",
                        dispatcher_pid=dispatcher_pid,
                        dispatcher_instance_id=dispatcher_instance_id,
                    )
                ledger_recovery.authorize(
                    out_dir, dispatcher_process_lost=True)
                authorization = (
                    run_meta.read_campaign_recovery_authorization(out_dir))
                selected, _authorizations = (
                    paired_dispatch._select_invocation_assignments(
                        out_dir,
                        [
                            {
                                "sample": sample,
                                "methods": [
                                    "hybridpatch", "fullrewrite"],
                            },
                            {
                                "sample": prior_pending_sample,
                                "methods": [
                                    "hybridpatch", "fullrewrite"],
                            },
                        ],
                        resume=True,
                        target_round_trips=10,
                        allow_deepseek_resume=True,
                    ))

            self.assertNotIn(
                sample,
                authorization["provider_access_resume_samples"],
            )
            self.assertNotIn(
                sample,
                {
                    item["sample"] for item in authorization[
                        "dispatcher_parent_loss_resume_workers"]
                },
            )
            self.assertEqual(
                [item["sample"] for item in selected],
                [prior_pending_sample],
            )

    def test_emergency_stop_retries_transient_secondary_name_cleanup(self):
        real_unlink = os.unlink
        transient_failures = []

        def flaky_unlink(path):
            if (path.endswith(".json")
                    and run_meta.EMERGENCY_STOP_DIRECTORY in path
                    and not transient_failures):
                transient_failures.append(path)
                exc = OSError("simulated Windows sharing violation")
                exc.winerror = 32
                raise exc
            return real_unlink(path)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta.os, "unlink", side_effect=flaky_unlink):
            run_meta.record_emergency_campaign_stop_condition(
                out_dir, "dispatcher_process_lost",
                dispatcher_pid=12345,
                dispatcher_instance_id="dispatcher-instance-a",
            )
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
        self.assertEqual(len(transient_failures), 1)

    def test_emergency_stop_keeps_each_distinct_worker_record(self):
        real_pid = os.getpid()
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a"},
                    clear=False):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=12345,
                    dispatcher_instance_id="dispatcher-instance-a",
                )
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-b"},
                    clear=False), mock.patch.object(
                        run_meta.os, "getpid",
                        return_value=real_pid + 1000):
                run_meta.record_emergency_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=12345,
                    dispatcher_instance_id="dispatcher-instance-a",
                )
            records = run_meta.read_campaign_stop_conditions(out_dir)
            emergency_dir = os.path.join(
                out_dir, run_meta.EMERGENCY_STOP_DIRECTORY)
            emergency_names = [
                name for name in os.listdir(emergency_dir)
                if name.endswith(".json")
            ]

        self.assertEqual(len(records), 2)
        self.assertEqual(
            {row["worker_launch_id"] for row in records},
            {"worker-a", "worker-b"})
        self.assertEqual(
            {row["worker_pid"] for row in records},
            {real_pid, real_pid + 1000})
        self.assertEqual(len(emergency_names), 1)
