"""Mixin slice for test_model_openai.IntegrationContractGroup03Mixin."""

from .support import *


class IntegrationContractGroup03Mixin:
    def test_remaining134_scope_excludes_planned100_and_uses_fourteen_keys(self):
        all_samples, _full_scope = paired_dispatch._load_full234_scope()
        samples, scope = paired_dispatch._load_remaining134_scope()
        selection = json.loads(pathlib.Path(
            paired_dispatch.REMAINING134_SELECTION_PATH
        ).read_text(encoding="utf-8"))
        excluded = set(selection["selected_sample_ids"])
        self.assertEqual(len(all_samples), 234)
        self.assertEqual(len(excluded), 100)
        self.assertEqual(len(samples), 134)
        self.assertFalse(excluded & set(samples))
        self.assertEqual(excluded | set(samples), set(all_samples))
        self.assertEqual(scope["sample_count"], 134)
        self.assertEqual(
            scope["exclusion_source"]["sha256"],
            paired_dispatch.REMAINING134_SELECTION_SHA256)

        args = mock.Mock(
            campaign_role="remaining134", smoke_dir=None, samples=None,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        resolved = paired_dispatch._resolve_remaining134_scope(args)
        self.assertEqual(resolved, scope)
        self.assertEqual(args.samples, samples)
        paired_dispatch._validate_campaign_grid(args)

        labels = [f"KEY_{index:02d}" for index in range(1, 15)]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 4, allow_queue=True)
        for item in assignments:
            item["methods"] = list(paired_dispatch.REMAINING134_METHOD_PHASES)
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "out", samples, assignments, {}, args)
        self.assertEqual(manifest["remaining134_scope"], scope)
        self.assertEqual(len(manifest["assignment_queues"]), 14)
        self.assertEqual(
            sorted(queue["worker_count"]
                   for queue in manifest["assignment_queues"]),
            [9] * 6 + [10] * 8,
        )
        self.assertTrue(all(
            queue["hybridpatch_first"] == queue["worker_count"]
            and queue["fullrewrite_first"] == 0
            for queue in manifest["assignment_queues"]
        ))
        self.assertEqual(manifest["config"]["key_count"], 14)
        self.assertEqual(manifest["config"]["max_worker_count"], 56)
        self.assertEqual(manifest["config"]["queued_worker_count"], 78)
        self.assertEqual(manifest["config"]["total_worker_invocations"], 268)
        self.assertEqual(
            manifest["config"]["method_phases"],
            ["hybridpatch", "fullrewrite"])

    def test_remaining134_runs_all_hp_before_any_fr_and_persists_barrier(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": f"KEY_0{index}",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for index, sample in enumerate(samples, 1)
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=False, resume_reason=None,
            confirm_workers_stopped=False,
        )
        phases = []

        def fake_queue(
                _args, out_dir, _manifest, _plans, _keys, phase_items,
                _authorizations, _dispatch_log, _running, _infra, _eval,
                completed, _total, _slots):
            phase = phase_items[0]["method_phase"]
            phases.append(phase)
            if phase == "fullrewrite":
                paired_dispatch._require_hybridpatch_phase_barrier(
                    out_dir, manifest)
                hp = paired_dispatch._latest_sample_outcomes(
                    out_dir, samples, method_phase="hybridpatch")
                self.assertEqual(set(hp), set(samples))
            for item in phase_items:
                run_meta.record_sample_outcome(
                    out_dir, item["sample"], "finished",
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})
                completed.add(item["sample"])

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value={"errors": [], "api_calls": 0,
                              "preservation_violations": 0}), mock.patch.object(
                    paired_dispatch, "_run_worker_queue",
                    side_effect=fake_queue):
            result = paired_dispatch._run_remaining134_campaign(
                args, out_dir, manifest, {}, {}, assignments,
                os.path.join(out_dir, "dispatch_log.jsonl"), {})
            rows = paired_dispatch._read_jsonl(
                os.path.join(out_dir, "dispatch_log.jsonl"))
        self.assertEqual(result, 0)
        self.assertEqual(phases, ["hybridpatch", "fullrewrite"])
        hp_barrier_index = next(
            index for index, row in enumerate(rows)
            if row.get("event") == "method_phase_complete"
            and row.get("method_phase") == "hybridpatch")
        fr_start_index = next(
            index for index, row in enumerate(rows)
            if row.get("event") == "method_phase_start"
            and row.get("method_phase") == "fullrewrite")
        self.assertLess(hp_barrier_index, fr_start_index)

    def test_remaining134_resume_reuses_hp_barrier_and_runs_only_fr(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": f"KEY_0{index}",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for index, sample in enumerate(samples, 1)
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=True,
            resume_reason="continue unfinished FR only",
            confirm_workers_stopped=True,
        )
        phases = []

        def fake_phase(
                _args, _out_dir, _manifest, _plans, _keys,
                _assignments, method_phase, **_kwargs):
            phases.append(method_phase)
            self.assertEqual(method_phase, "fullrewrite")
            return ({
                "finished": set(samples),
                "evaluator_incomplete": set(),
                "infrastructure_incomplete": set(),
                "missing": set(),
            }, True)

        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            run_meta.append_jsonl_locked(dispatch_log, {
                "schema": paired_dispatch.METHOD_PHASE_COMPLETE_SCHEMA,
                "event": "method_phase_complete",
                "method_phase": "hybridpatch",
                "next_method_phase": "fullrewrite",
                "eligible_sample_count": len(samples),
                "eligible_sample_ids_sha256": (
                    paired_dispatch._sample_ids_sha256(samples)),
                "finished_samples": samples,
                "evaluator_incomplete_samples": [],
                "phase_api_calls": 20,
                "preservation_violations": 0,
                "run_git_commit": "1" * 40,
            })
            with mock.patch.object(
                    paired_dispatch, "inspect_campaign",
                    return_value={"errors": [], "api_calls": 0,
                                  "preservation_violations": 0}), \
                    mock.patch.object(
                        paired_dispatch, "_run_remaining134_phase",
                        side_effect=fake_phase):
                result = paired_dispatch._run_remaining134_campaign(
                    args, out_dir, manifest, {}, {}, assignments,
                    dispatch_log, {})
            rows = paired_dispatch._read_jsonl(dispatch_log)

        self.assertEqual(result, 0)
        self.assertEqual(phases, ["fullrewrite"])
        reused = [
            row for row in rows
            if row.get("event") == "method_phase_resume_reused"
        ]
        self.assertEqual(len(reused), 1)
        self.assertEqual(reused[0]["method_phase"], "hybridpatch")

    def test_remaining134_hp_infrastructure_failure_never_launches_fr(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for sample in samples
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=False, resume_reason=None,
            confirm_workers_stopped=False,
        )
        phases = []

        def fake_queue(
                _args, out_dir, _manifest, _plans, _keys, phase_items,
                _authorizations, _dispatch_log, _running, _infra, _eval,
                _completed, _total, _slots):
            phase = phase_items[0]["method_phase"]
            phases.append(phase)
            for item in phase_items:
                status = (
                    "infrastructure_incomplete"
                    if item["sample"] == "sample-a" else "finished")
                run_meta.record_sample_outcome(
                    out_dir, item["sample"], status,
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value={"errors": [], "api_calls": 0,
                              "preservation_violations": 0}), mock.patch.object(
                    paired_dispatch, "_run_worker_queue",
                    side_effect=fake_queue):
            result = paired_dispatch._run_remaining134_campaign(
                args, out_dir, manifest, {}, {}, assignments,
                os.path.join(out_dir, "dispatch_log.jsonl"), {})
            barriers = paired_dispatch._method_phase_complete_events(
                out_dir, "hybridpatch")
        self.assertEqual(result, 2)
        self.assertEqual(phases, ["hybridpatch"])
        self.assertEqual(barriers, [])

    def test_remaining134_incomplete_requires_terminal_worker_provenance(self):
        states = {
            "finished": {"sample-b"},
            "infrastructure_incomplete": {"sample-a"},
            "evaluator_incomplete": set(),
            "missing": set(),
        }
        inspection = {
            "errors": [], "api_calls": 1, "preservation_violations": 0,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value=inspection) as inspect:
            paired_dispatch._append_remaining134_incomplete(
                out_dir,
                {"run_git_commit": "1" * 40},
                "hybridpatch",
                states,
            )

        self.assertTrue(
            inspect.call_args.kwargs["require_terminal_provenance"])

    def test_remaining134_hp_evaluator_incomplete_skips_only_its_fr(self):
        samples = ["sample-a", "sample-b"]
        assignments = [
            {"sample": sample, "key_label": "KEY_01",
             "methods": ["hybridpatch", "fullrewrite"],
             "console_log": f"dispatch_logs/{sample}.log"}
            for sample in samples
        ]
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": samples, "method_set": [
                "fullrewrite", "hybridpatch"], "num_round_trips": 10},
        }
        args = mock.Mock(
            num_round_trips=10, resume=False, resume_reason=None,
            confirm_workers_stopped=False,
        )
        phase_samples = []

        def fake_queue(
                _args, out_dir, _manifest, _plans, _keys, phase_items,
                _authorizations, _dispatch_log, _running, _infra, _eval,
                _completed, _total, _slots):
            phase = phase_items[0]["method_phase"]
            launched = [item["sample"] for item in phase_items]
            phase_samples.append((phase, launched))
            for item in phase_items:
                status = (
                    "evaluator_incomplete"
                    if phase == "hybridpatch"
                    and item["sample"] == "sample-a" else "finished")
                run_meta.record_sample_outcome(
                    out_dir, item["sample"], status,
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "inspect_campaign",
                return_value={"errors": [], "api_calls": 0,
                              "preservation_violations": 0}), mock.patch.object(
                    paired_dispatch, "_run_worker_queue",
                    side_effect=fake_queue):
            result = paired_dispatch._run_remaining134_campaign(
                args, out_dir, manifest, {}, {}, assignments,
                os.path.join(out_dir, "dispatch_log.jsonl"), {})
            fr_outcomes = paired_dispatch._latest_sample_outcomes(
                out_dir, method_phase="fullrewrite")
        self.assertEqual(result, 2)
        self.assertEqual(
            phase_samples,
            [("hybridpatch", samples), ("fullrewrite", ["sample-b"])])
        self.assertNotIn("sample-a", fr_outcomes)
        self.assertEqual(fr_outcomes["sample-b"]["status"], "finished")

    def test_phase_resume_skips_committed_sample_and_keeps_pending_sample(self):
        sample_done = "sample-done"
        sample_pending = "sample-pending"
        assignments = [
            {"sample": sample_done, "key_label": "KEY_01",
             "methods": ["fullrewrite"], "method_phase": "fullrewrite"},
            {"sample": sample_pending, "key_label": "KEY_02",
             "methods": ["fullrewrite"], "method_phase": "fullrewrite"},
        ]
        expected_progress = {
            "fullrewrite": {
                "completed_round_trips": 10,
                "committed_rows": 20,
            }
        }
        with tempfile.TemporaryDirectory() as out_dir:
            self._write_completed_method_prefix(
                out_dir, sample_done, "fullrewrite", 10)
            run_meta.record_sample_outcome(
                out_dir, sample_done, "finished",
                methods=["fullrewrite"], method_phase="fullrewrite",
                checkpoint_progress=expected_progress)
            selected, authorizations = (
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=10, allow_pristine_pending=True,
                    method_phase="fullrewrite"))
            root = (
                "fullrewrite/sample-pending/rt01/forward/"
                "fullrewrite_primary")
            self._write_success_journal(
                out_dir, root=root, exact_id=f"{root}/g000",
                request_id="unexpected-fr", fingerprint="fingerprint")
            with self.assertRaisesRegex(
                    RuntimeError, "method phase was never started"):
                paired_dispatch._select_invocation_assignments(
                    out_dir, assignments, resume=True,
                    target_round_trips=10, allow_pristine_pending=True,
                    method_phase="fullrewrite")
        self.assertEqual(
            [item["sample"] for item in selected], [sample_pending])
        self.assertEqual(authorizations, {})

    def test_fullrewrite_worker_launch_requires_hp_phase_barrier(self):
        sample = "sample-a"
        manifest = {
            "run_git_commit": "1" * 40,
            "config": {"samples": [sample]},
        }
        assignment = {
            "sample": sample, "key_label": "KEY_01",
            "methods": ["fullrewrite"], "method_phase": "fullrewrite",
            "console_log": (
                "dispatch_logs/sample-a__KEY_01__fullrewrite.console.log"),
        }
        args = mock.Mock(
            num_round_trips=10, seed=42, notes="unit", start_timeout=1)
        with tempfile.TemporaryDirectory() as out_dir:
            os.makedirs(os.path.join(out_dir, "dispatch_logs"))
            task_plans = {
                sample: {"path": "sample-a.task_plan.json",
                         "sha256": "a" * 64}
            }
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen") as popen:
                with self.assertRaisesRegex(RuntimeError, "phase barrier"):
                    paired_dispatch._launch_worker_batch(
                        args, out_dir, manifest, task_plans,
                        {"KEY_01": "redacted"}, [assignment], {},
                        os.path.join(out_dir, "dispatch_log.jsonl"), {})
            popen.assert_not_called()

            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "dispatch_log.jsonl"), {
                    "schema": paired_dispatch.METHOD_PHASE_COMPLETE_SCHEMA,
                    "event": "method_phase_complete",
                    "method_phase": "hybridpatch",
                    "next_method_phase": "fullrewrite",
                    "eligible_sample_count": 1,
                    "eligible_sample_ids_sha256": (
                        paired_dispatch._sample_ids_sha256([sample])),
                    "finished_samples": [sample],
                    "evaluator_incomplete_samples": [],
                    "preservation_violations": 0,
                    "run_git_commit": "1" * 40,
                })

            class FakeProcess:
                pid = 12345

                @staticmethod
                def poll():
                    return None

            running = {}
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen",
                    return_value=FakeProcess()) as popen, mock.patch.object(
                        paired_dispatch, "_authorize_workers"):
                launched = paired_dispatch._launch_worker_batch(
                    args, out_dir, manifest, task_plans,
                    {"KEY_01": "redacted"}, [assignment], {},
                    os.path.join(out_dir, "dispatch_log.jsonl"), running)
            self.assertEqual(launched, [sample])
            popen.assert_called_once()
            self.assertEqual(
                popen.call_args.kwargs["env"]["ANCHORPATCH_METHOD_PHASE"],
                "fullrewrite")
            running[sample]["log"].close()

    def test_phase_outcomes_allow_hp_then_fr_but_reject_duplicate_terminal(self):
        with tempfile.TemporaryDirectory() as out_dir:
            for phase in ("hybridpatch", "fullrewrite"):
                run_meta.record_sample_outcome(
                    out_dir, "sample-a", "finished",
                    methods=[phase], method_phase=phase,
                    checkpoint_progress={})
            self.assertEqual(
                paired_dispatch._latest_sample_outcomes(
                    out_dir, ["sample-a"],
                    method_phase="hybridpatch")["sample-a"]["status"],
                "finished")
            self.assertEqual(
                paired_dispatch._latest_sample_outcomes(
                    out_dir, ["sample-a"],
                    method_phase="fullrewrite")["sample-a"]["status"],
                "finished")
            run_meta.record_sample_outcome(
                out_dir, "sample-a", "infrastructure_incomplete",
                methods=["fullrewrite"], method_phase="fullrewrite",
                checkpoint_progress={})
            with self.assertRaisesRegex(
                    RuntimeError, "after finished state"):
                paired_dispatch._latest_sample_outcomes(
                    out_dir, ["sample-a"],
                    method_phase="fullrewrite")

    def test_strict_inspector_auto_aggregates_declared_method_phases(self):
        sample = "sample-a"
        args = mock.Mock(
            campaign_role="remaining134", num_round_trips=10, seed=42,
            slots_per_key=4,
        )
        args._remaining134_scope_record = {
            "schema": "anchorpatch.remaining134_scope/1",
            "sample_count": 1,
            "sample_ids": [sample],
        }
        assignments = [{
            "sample": sample, "key_label": "KEY_01",
            "methods": ["hybridpatch", "fullrewrite"],
            "console_log": "dispatch_logs/sample-a.log",
        }]
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                out_dir, [sample], assignments, {}, args)
            paired_dispatch._write_active_worker_set(out_dir, manifest, [])
            inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
            self.assertEqual(inspection["errors"], [])
            manifest["config"]["method_phases"] = ["fullrewrite"]
            with self.assertRaisesRegex(RuntimeError, "phases are invalid"):
                paired_dispatch.inspect_campaign(out_dir, manifest)

    def test_preflight_require_plans_fails_on_missing_frozen_plan(self):
        with tempfile.TemporaryDirectory() as out_dir, \
                tempfile.TemporaryDirectory() as plans_from, \
                mock.patch.object(
                    fr_baseline_dispatch.subprocess, "Popen") as popen:
            passed = fr_baseline_dispatch.preflight(
                ["treebank4"], [("KEY_01", "redacted")],
                out_dir, plans_from, skip_probe=False,
                require_plans=True,
            )
        self.assertFalse(passed)
        popen.assert_not_called()

    def test_paired_dispatch_uses_exclusive_out_dir_lease(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaises(RuntimeError):
                        paired_dispatch.launch(mock.Mock(out_dir=out_dir))
                finally:
                    portalocker.unlock(lease)

    def test_campaign_stop_read_does_not_contend_on_metadata_writer_lock(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lock_path = os.path.join(out_dir, ".run_metadata.lock")
            with open(lock_path, "a+", encoding="utf-8") as writer:
                portalocker.lock(
                    writer, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    errors = []
                    results = []

                    def read_latch():
                        try:
                            results.append(
                                run_meta.read_campaign_stop_conditions(out_dir))
                        except BaseException as exc:
                            errors.append(exc)

                    readers = [
                        threading.Thread(target=read_latch, daemon=True)
                        for _ in range(40)
                    ]
                    for reader in readers:
                        reader.start()
                    for reader in readers:
                        reader.join(2)
                finally:
                    portalocker.unlock(writer)
            self.assertTrue(all(not reader.is_alive() for reader in readers))
            self.assertEqual(errors, [])
            self.assertEqual(results, [[]] * 40)

    def test_campaign_metadata_writer_waits_for_existing_writer(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lock_path = os.path.join(out_dir, ".run_metadata.lock")
            entered = threading.Event()
            errors = []
            with open(lock_path, "a+", encoding="utf-8") as first_writer:
                portalocker.lock(
                    first_writer, portalocker.LOCK_EX | portalocker.LOCK_NB)

                def acquire_after_contention():
                    try:
                        with run_meta._campaign_metadata_lock(out_dir):
                            entered.set()
                    except BaseException as exc:
                        errors.append(exc)

                contender = threading.Thread(
                    target=acquire_after_contention, daemon=True)
                contender.start()
                self.assertFalse(entered.wait(0.15))
                portalocker.unlock(first_writer)
                contender.join(3)
            self.assertFalse(contender.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(entered.is_set())

    def test_shared_jsonl_writer_waits_for_existing_writer(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
            appended = threading.Event()
            errors = []
            with open(path, "a+", encoding="utf-8") as first_writer:
                portalocker.lock(
                    first_writer,
                    portalocker.LOCK_EX | portalocker.LOCK_NB)

                def append_after_contention():
                    try:
                        run_meta.append_jsonl_locked(path, {"value": 1})
                        appended.set()
                    except BaseException as exc:
                        errors.append(exc)

                contender = threading.Thread(
                    target=append_after_contention, daemon=True)
                contender.start()
                self.assertFalse(appended.wait(0.15))
                portalocker.unlock(first_writer)
                contender.join(3)
            self.assertFalse(contender.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(appended.is_set())
            self.assertEqual(
                run_meta._read_jsonl_records_with_retry(path),
                [{"value": 1}],
            )

    def test_run_metadata_atomic_replace_retries_windows_sharing_violation(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "run_metadata.jsonl")
            real_replace = run_meta.os.replace
            attempts = []

            def flaky_replace(source, target):
                attempts.append((source, target))
                if len(attempts) == 1:
                    error = PermissionError(13, "transient sharing violation")
                    error.winerror = 5
                    raise error
                return real_replace(source, target)

            with mock.patch.object(
                    run_meta.os, "replace",
                    side_effect=flaky_replace), mock.patch.object(
                    run_meta.time, "sleep") as sleep:
                run_meta._write_jsonl_atomic(path, [{"value": 1}])

            self.assertEqual(len(attempts), 2)
            sleep.assert_called_once_with(0.05)
            self.assertEqual(
                run_meta._read_jsonl_records_with_retry(path),
                [{"value": 1}],
            )

            json_path = os.path.join(out_dir, "active_worker_set.json")
            attempts.clear()
            with mock.patch.object(
                    run_meta.os, "replace",
                    side_effect=flaky_replace), mock.patch.object(
                    run_meta.time, "sleep") as sleep:
                run_meta.write_json_atomic(json_path, {"workers": {}})
            self.assertEqual(len(attempts), 2)
            sleep.assert_called_once_with(0.05)
            with open(json_path, encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), {"workers": {}})

    def test_api_recorder_filters_only_authorized_lock_incident_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            semantic = (
                "fullrewrite/sample/rt01/forward/fullrewrite_primary/g000"
            )
            context = run_meta._semantic_lineage_fields(semantic)
            incident = {
                "schema": run_meta.API_ATTEMPT_SCHEMA,
                "created_local": "2026-07-21T18:00:00",
                "step_id": context["step_id"],
                "semantic_root_id": context["semantic_root_id"],
                "semantic_call_id": semantic,
                "generation_index": 0,
                "parent_semantic_call_id": None,
                "worker_launch_id": "old-worker",
                "event": "semantic_request",
                "call_id": "old-call",
                "call_kind": "fullrewrite_primary",
                "request_fingerprint": "old-fingerprint",
            }
            path = os.path.join(out_dir, "api_attempt_ledger.jsonl")
            run_meta.append_jsonl_locked(path, incident)
            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", mock.Mock())
            self.assertEqual(
                recorder._ledger_state(semantic)["request_fingerprints"],
                ["old-fingerprint"],
            )
            recorder._authorized_attempt_incident_hashes = {
                run_meta._canonical_record_sha256(incident)
            }
            state = recorder._ledger_state(semantic)
            self.assertEqual(state["request_fingerprints"], [])
            self.assertEqual(state["http_attempts_used"], 0)

    def test_paired_dispatch_resume_allows_only_audited_key_rotation(self):
        prior = {
            "schema": paired_dispatch.SCHEMA,
            "experiment_id": "exp_test",
            "run_git_commit": "1" * 40,
            "git_tree_state": "clean",
            "code_fingerprint": {"x": "y"},
            "config": {"samples": ["sample"]},
            "assignments": [{
                "sample": "sample", "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
                "console_log": "dispatch_logs/sample__KEY_01.console.log",
            }],
            "task_plans": {"sample": {"sha256": "a" * 64}},
        }
        rotated = json.loads(json.dumps(prior))
        rotated["assignments"][0]["key_label"] = "KEY_11"
        rotated["assignments"][0]["console_log"] = (
            "dispatch_logs/sample__KEY_11.console.log"
        )
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(path, prior)
            with self.assertRaises(RuntimeError):
                paired_dispatch.write_or_verify_manifest(
                    out_dir, rotated, resume=False)
            found_path, found_manifest = (
                paired_dispatch.write_or_verify_manifest(
                    out_dir, rotated, resume=True)
            )
            self.assertEqual(found_path, path)
            self.assertEqual(found_manifest, prior)

    def test_paired_dispatch_resume_accepts_only_authorized_git_transition(self):
        prior = {
            "schema": paired_dispatch.SCHEMA,
            "experiment_id": "exp_test",
            "run_git_commit": "1" * 40,
            "git_tree_state": "clean",
            "code_fingerprint": {"run_meta.py": "old"},
            "config": {"samples": ["sample"]},
            "assignments": [{
                "sample": "sample", "key_label": "KEY_01",
                "methods": ["hybridpatch", "fullrewrite"],
                "console_log": "dispatch_logs/sample__KEY_01.console.log",
            }],
            "task_plans": {"sample": {"sha256": "a" * 64}},
        }
        recovered = copy.deepcopy(prior)
        recovered["run_git_commit"] = "2" * 40
        recovered["code_fingerprint"] = {"run_meta.py": "new"}
        authorization = {
            "prior_git_commit": prior["run_git_commit"],
            "recovery_git_commit": recovered["run_git_commit"],
        }
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(path, prior)
            with mock.patch.object(
                    paired_dispatch, "read_campaign_recovery_authorization",
                    return_value=authorization):
                found_path, found_manifest = (
                    paired_dispatch.write_or_verify_manifest(
                        out_dir, recovered, resume=True)
                )
                self.assertEqual(found_path, path)
                self.assertEqual(found_manifest, prior)

                changed_config = copy.deepcopy(recovered)
                changed_config["config"]["samples"] = ["other"]
                with self.assertRaises(RuntimeError):
                    paired_dispatch.write_or_verify_manifest(
                        out_dir, changed_config, resume=True)

    def test_confirmation_resume_identity_ignores_wave_key_label_names(self):
        prior = {
            "schema": paired_dispatch.SCHEMA,
            "experiment_id": "exp_test",
            "run_git_commit": "1" * 40,
            "git_tree_state": "clean",
            "code_fingerprint": {"x": "y"},
            "config": {
                "campaign_role": "confirmation",
                "samples": ["sample-a", "sample-b"],
            },
            "assignments": [
                {
                    "sample": "sample-a", "key_label": "KEY_01",
                    "methods": ["hybridpatch", "fullrewrite"],
                    "console_log": (
                        "dispatch_logs/sample-a__KEY_01.console.log"),
                },
                {
                    "sample": "sample-b", "key_label": "KEY_02",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "console_log": (
                        "dispatch_logs/sample-b__KEY_02.console.log"),
                },
            ],
            "assignment_waves": [{
                "wave_index": 1,
                "sample_ids": ["sample-a", "sample-b"],
                "worker_count": 2,
                "key_worker_counts": {"KEY_01": 1, "KEY_02": 1},
                "hybridpatch_first": 1,
                "fullrewrite_first": 1,
            }],
            "assignment_queues": [
                {
                    "key_label": "KEY_01", "sample_ids": ["sample-a"],
                    "worker_count": 1, "hybridpatch_first": 1,
                    "fullrewrite_first": 0,
                },
                {
                    "key_label": "KEY_02", "sample_ids": ["sample-b"],
                    "worker_count": 1, "hybridpatch_first": 0,
                    "fullrewrite_first": 1,
                },
            ],
            "task_plans": {
                "sample-a": {"sha256": "a" * 64},
                "sample-b": {"sha256": "b" * 64},
            },
        }
        rotated = json.loads(json.dumps(prior))
        for index, label in enumerate(("KEY_11", "KEY_12")):
            rotated["assignments"][index]["key_label"] = label
            rotated["assignments"][index]["console_log"] = (
                f"dispatch_logs/sample-{chr(ord('a') + index)}__"
                f"{label}.console.log"
            )
        rotated["assignment_waves"][0]["key_worker_counts"] = {
            "KEY_11": 1, "KEY_12": 1}
        rotated["assignment_queues"][0]["key_label"] = "KEY_11"
        rotated["assignment_queues"][1]["key_label"] = "KEY_12"
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "dispatch_manifest.json")
            run_meta.write_json_atomic(path, prior)
            found_path, found_manifest = (
                paired_dispatch.write_or_verify_manifest(
                    out_dir, rotated, resume=True)
            )
            self.assertEqual(found_path, path)
            self.assertEqual(found_manifest, prior)

            bad_rotation = json.loads(json.dumps(rotated))
            bad_rotation["assignment_waves"][0]["key_worker_counts"] = {
                "KEY_11": 2}
            with self.assertRaises(RuntimeError):
                paired_dispatch.write_or_verify_manifest(
                    out_dir, bad_rotation, resume=True)

    def test_dispatch_worker_lease_and_stale_metadata_closure(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lease_path = paired_dispatch._worker_lease_path(out_dir, "sample")
            os.makedirs(os.path.dirname(lease_path), exist_ok=True)
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaises(RuntimeError):
                        paired_dispatch._assert_worker_leases_free(
                            out_dir, ["sample"])
                finally:
                    portalocker.unlock(lease)
            paired_dispatch._assert_worker_leases_free(out_dir, ["sample"])

            metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
            run_meta._write_jsonl_atomic(metadata_path, [
                {
                    "invocation_id": "invocation-a",
                    "worker_launch_id": "worker-a",
                    "worker_pid": 101,
                    "samples": ["sample"],
                    "status": "running",
                    "invocation_finished_at": None,
                    "finished_at": None,
                },
                {
                    "invocation_id": "invocation-b",
                    "worker_launch_id": "worker-b",
                    "worker_pid": 202,
                    "samples": ["other"],
                    "status": "running",
                    "invocation_finished_at": None,
                    "finished_at": None,
                },
            ])
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            for worker, sample, pid in (
                ("worker-a", "sample", 101),
                ("worker-b", "other", 202),
            ):
                run_meta.append_jsonl_locked(dispatch_path, {
                    "event": "launch_intent",
                    "worker_launch_id": worker,
                    "sample": sample,
                })
                run_meta.append_jsonl_locked(dispatch_path, {
                    "event": "launch",
                    "worker_launch_id": worker,
                    "sample": sample,
                    "pid": pid,
                })
            audited = paired_dispatch._audit_running_invocation_provenance(
                out_dir)
            self.assertEqual(
                {item["worker_launch_id"] for item in audited},
                {"worker-a", "worker-b"},
            )
            self.assertTrue(all(item["launch_recorded"] for item in audited))

            closed = run_meta.interrupt_running_invocations(
                out_dir,
                status="interrupted_by_dispatcher",
                worker_launch_ids={"worker-a"},
            )
            self.assertEqual(
                [item["invocation_id"] for item in closed],
                ["invocation-a"],
            )
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(snapshot[0]["status"], "interrupted_by_dispatcher")
            self.assertEqual(snapshot[1]["status"], "running")
            self.assertTrue(all(row["finished_at"] is None for row in snapshot))

            run_meta.interrupt_running_invocations(
                out_dir,
                status="interrupted_before_audited_resume",
            )
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(row["status"] != "running" for row in snapshot))
            self.assertTrue(all(row["finished_at"] for row in snapshot))

        with tempfile.TemporaryDirectory() as out_dir:
            running = {
                "sample": {
                    "worker_launch_id": "worker-a",
                    "key_label": "KEY_01",
                    "log": mock.Mock(),
                    "process": mock.Mock(pid=101),
                },
            }
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with self.assertRaisesRegex(RuntimeError, "exited with 7"):
                paired_dispatch._record_worker_exit(
                    out_dir, running, "sample", running["sample"], 7,
                    dispatch_log
                )
            self.assertIn("sample", running)
            exit_rows = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(exit_rows[-1]["returncode"], 7)
            self.assertEqual(exit_rows[-1]["worker_launch_id"], "worker-a")
            self.assertEqual(exit_rows[-1]["pid"], 101)
            running["sample"]["process"].poll.return_value = 7
            running["sample"]["process"].wait.return_value = 7
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                [{
                    "invocation_id": "invocation-a",
                    "worker_launch_id": "worker-a",
                    "worker_pid": 101,
                    "samples": ["sample"],
                    "status": "running",
                    "invocation_finished_at": None,
                    "finished_at": None,
                }],
            )
            run_meta.append_jsonl_locked(dispatch_log, {
                "event": "launch_intent",
                "worker_launch_id": "worker-a",
                "sample": "sample",
            })
            run_meta.append_jsonl_locked(dispatch_log, {
                "event": "launch",
                "worker_launch_id": "worker-a",
                "sample": "sample",
                "pid": 101,
            })
            reconciled = paired_dispatch._stop_and_reconcile_workers(
                out_dir, running)
            self.assertEqual(
                reconciled["closed_invocations"][0]["invocation_id"],
                "invocation-a",
            )
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(
                snapshot[0]["status"], "interrupted_by_dispatcher"
            )
            self.assertIsNotNone(snapshot[0]["finished_at"])

    def test_dispatch_reconciles_metadata_even_if_termination_reports_error(self):
        running = {
            "sample": {
                "worker_launch_id": "worker-a",
                "key_label": "KEY_01",
                "log": mock.Mock(),
                "process": mock.Mock(pid=101),
            },
        }
        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with self.assertRaisesRegex(RuntimeError, "exited with 7"):
                paired_dispatch._record_worker_exit(
                    out_dir, running, "sample", running["sample"], 7,
                    dispatch_log
                )
            self.assertIn("sample", running)
            exit_rows = run_meta._read_jsonl_records_with_retry(dispatch_log)
            self.assertEqual(exit_rows[-1]["returncode"], 7)
            self.assertEqual(exit_rows[-1]["worker_launch_id"], "worker-a")
            self.assertEqual(exit_rows[-1]["pid"], 101)

        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_terminate_workers",
                    side_effect=RuntimeError("terminate failed"),
                ), \
                mock.patch.object(
                    paired_dispatch, "_assert_worker_leases_free"
                ) as leases_free, \
                mock.patch.object(
                    paired_dispatch,
                    "_audit_running_invocation_provenance",
                    return_value=[{
                        "invocation_id": "invocation-a",
                        "worker_launch_id": "worker-a",
                        "worker_pid": 101,
                        "sample": "sample",
                    }],
                ) as audit, \
                mock.patch.object(
                    paired_dispatch,
                    "interrupt_audited_running_invocations",
                    return_value=[{"invocation_id": "invocation-a"}],
                ) as interrupt:
            result = paired_dispatch._stop_and_reconcile_workers(
                out_dir, running)
        self.assertEqual(result["termination_error"], "terminate failed")
        self.assertIsNone(result["lease_error"])
        self.assertEqual(
            result["closed_invocations"],
            [{"invocation_id": "invocation-a"}],
        )
        leases_free.assert_called_once_with(out_dir, ["sample"])
        audit.assert_called_once_with(out_dir)
        interrupt.assert_called_once_with(
            out_dir,
            status="interrupted_by_dispatcher",
            audited=[{
                "invocation_id": "invocation-a",
                "worker_launch_id": "worker-a",
                "worker_pid": 101,
                "sample": "sample",
            }],
        )

    def test_reconcile_full_lease_scope_preserves_live_orphan_metadata(self):
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(
                    paired_dispatch, "_terminate_workers") as terminate, \
                mock.patch.object(
                    paired_dispatch, "_assert_worker_leases_free",
                    side_effect=RuntimeError("worker lease still held: orphan"),
                ) as leases_free, \
                mock.patch.object(
                    paired_dispatch,
                    "_audit_running_invocation_provenance",
                ) as audit, \
                mock.patch.object(
                    paired_dispatch,
                    "interrupt_audited_running_invocations",
                ) as interrupt:
            result = paired_dispatch._stop_and_reconcile_workers(
                out_dir, {}, lease_scope=["orphan"])
        terminate.assert_called_once_with({})
        leases_free.assert_called_once_with(out_dir, ["orphan"])
        self.assertIn("still held", result["lease_error"])
        self.assertEqual(result["lease_scope"], ["orphan"])
        self.assertEqual(result["closed_invocations"], [])
        audit.assert_not_called()
        interrupt.assert_not_called()

    def test_dispatch_retains_active_worker_until_lease_and_metadata_close(self):
        class FakeProcess:
            pid = 101
            poll_failures = 1

            @classmethod
            def poll(cls):
                if cls.poll_failures:
                    cls.poll_failures -= 1
                    raise RuntimeError("forced global integrity error")
                return None

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

        for hold_lease in (True, False):
            with self.subTest(hold_lease=hold_lease), \
                    tempfile.TemporaryDirectory() as out_dir:
                FakeProcess.poll_failures = 1
                args = mock.Mock(
                    campaign_role="smoke", smoke_dir=None,
                    samples=["sample"], key_labels=["KEY_01"],
                    keys_file="unused.env", num_round_trips=1, seed=42,
                    dry_run=False, resume=False, start_timeout=0.1,
                    poll_interval=0, progress_interval=9999,
                    notes="unit",
                )
                held_lease = []
                latch_seen_before_termination = []

                def fake_popen(_command, **kwargs):
                    worker_id = kwargs["env"][
                        "ANCHORPATCH_WORKER_LAUNCH_ID"]
                    with open(os.path.join(
                            out_dir, "dispatch_manifest.json"),
                            encoding="utf-8") as handle:
                        manifest = json.load(handle)
                    config = manifest["config"]
                    started_at = "2026-07-28T12:00:00+08:00"
                    run_meta._append_run_metadata_event_unlocked(
                        out_dir, {
                            "event": "invocation_registered",
                            "record": {
                            "schema": run_meta.METADATA_SCHEMA,
                            "invocation_id": f"invocation-{worker_id}",
                            "worker_launch_id": worker_id,
                            "worker_pid": 101,
                            "dispatcher_pid": None,
                            "dispatcher_instance_id": None,
                            "samples": ["sample"],
                            "methods": ["hybridpatch", "fullrewrite"],
                            "command": "python fake-worker",
                            "out_dir": os.path.abspath(out_dir),
                            "num_round_trips": 1,
                            "seed": 42,
                            "model": config["model"],
                            "distractor": True,
                            "max_tokens": config["max_tokens"],
                            "reasoning_effort": config.get("reasoning_effort"),
                            "campaign_config": {
                                "method_set": ["fullrewrite", "hybridpatch"],
                                "num_round_trips": 1,
                                "seed": 42,
                                "model": config["model"],
                                "distractor": True,
                                "max_tokens": config["max_tokens"],
                                "reasoning_effort": config.get(
                                    "reasoning_effort"),
                            },
                            "run_git_commit": "1" * 40,
                            "git_tree_state": "clean",
                            "code_fingerprint": {"unit": "test"},
                            "campaign_recovery_authorization": None,
                            "transport": config.get("transport"),
                            "transport_revision": config.get(
                                "transport_revision"),
                            "transport_resume_policy": config.get(
                                "transport_resume_policy"),
                            "status": "running",
                            "created_local": started_at,
                            "invocation_started_at": started_at,
                            "invocation_finished_at": None,
                            "started_at": started_at,
                            "finished_at": None,
                            "timezone": "Asia/Singapore",
                        }})
                    if hold_lease:
                        lease = open(
                            kwargs["env"]["ANCHORPATCH_WORKER_LOCK_PATH"],
                            "a+", encoding="utf-8")
                        portalocker.lock(
                            lease,
                            portalocker.LOCK_EX | portalocker.LOCK_NB)
                        held_lease.append(lease)
                    return FakeProcess()

                def fail_termination(_running):
                    latch_seen_before_termination.extend(
                        run_meta.read_campaign_stop_conditions(out_dir))
                    raise RuntimeError("terminate/kill failed")

                inspections = iter([{
                    "errors": [], "api_calls": 0,
                    "preservation_violations": 0,
                }])
                try:
                    with mock.patch.object(
                            paired_dispatch,
                            "_validate_campaign_grid"), \
                            mock.patch.object(
                                paired_dispatch,
                                "_require_formal_opencode_transport"), \
                            mock.patch.object(
                                paired_dispatch, "read_keys",
                                return_value={"KEY_01": "redacted"}), \
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
                                side_effect=lambda *_args, **_kwargs: next(
                                    inspections)), \
                            mock.patch.object(
                                paired_dispatch, "_authorize_workers"), \
                            mock.patch.object(
                                paired_dispatch.subprocess, "Popen",
                                side_effect=fake_popen), \
                            mock.patch.object(
                                paired_dispatch, "_terminate_workers",
                                side_effect=fail_termination), \
                            mock.patch.object(
                                paired_dispatch.time, "sleep"):
                        result = paired_dispatch._launch_under_lease(
                            args, out_dir)
                finally:
                    for lease in held_lease:
                        portalocker.unlock(lease)
                        lease.close()

                self.assertEqual(result, 1)
                self.assertEqual(len(latch_seen_before_termination), 1)
                self.assertEqual(
                    latch_seen_before_termination[0]["condition"],
                    "dispatcher_integrity_failure")
                self.assertEqual(
                    latch_seen_before_termination[0]["error_type"],
                    "RuntimeError")
                self.assertIn(
                    "forced global integrity error",
                    latch_seen_before_termination[0]["error"])
                self.assertEqual(
                    run_meta.read_campaign_stop_conditions(out_dir),
                    latch_seen_before_termination)
                with open(
                    paired_dispatch._active_worker_set_path(out_dir),
                    encoding="utf-8",
                ) as handle:
                    active = json.load(handle)
                dispatch_rows = run_meta._read_jsonl_records_with_retry(
                    os.path.join(out_dir, "dispatch_log.jsonl"))
                stop = next(
                    row for row in reversed(dispatch_rows)
                    if row.get("event") == "campaign_stop")
                reconciliation = stop["worker_reconciliation"]
                self.assertEqual(
                    reconciliation["termination_error"],
                    "terminate/kill failed")
                metadata = run_meta.read_run_metadata_snapshot(out_dir)

                if hold_lease:
                    self.assertEqual(
                        [item["sample"]
                         for item in active["workers"].values()],
                        ["sample"])
                    self.assertIn(
                        "sample", reconciliation["lease_error"])
                    self.assertEqual(
                        reconciliation["active_set_retained"],
                        ["sample"])
                    self.assertNotIn(
                        "audited_invocations", reconciliation)
                    self.assertEqual(metadata[0]["status"], "running")
                else:
                    self.assertEqual(active["workers"], {})
                    self.assertIsNone(reconciliation["lease_error"])
                    self.assertEqual(
                        len(reconciliation["closed_invocations"]), 1)
                    self.assertEqual(
                        reconciliation["audited_invocations"][0]["sample"],
                        "sample")
                    self.assertEqual(
                        reconciliation["audited_invocations"][0][
                            "worker_pid"],
                        101)
                    self.assertNotIn("active_set_retained", reconciliation)
                    self.assertEqual(
                        metadata[0]["status"],
                        "interrupted_by_dispatcher")

    def test_audited_resume_cas_rejects_toctou_and_malformed_identity(self):
        def running(invocation, worker, pid, sample):
            return {
                "invocation_id": invocation,
                "worker_launch_id": worker,
                "worker_pid": pid,
                "samples": [sample],
                "status": "running",
                "invocation_finished_at": None,
                "finished_at": None,
            }

        audited_a = [{
            "invocation_id": "invocation-a",
            "worker_launch_id": "worker-a",
            "worker_pid": 101,
            "sample": "sample-a",
        }]
        with tempfile.TemporaryDirectory() as out_dir:
            metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
            rows = [
                running("invocation-a", "worker-a", 101, "sample-a"),
                running("invocation-b", "worker-b", 202, "sample-b"),
            ]
            run_meta._write_jsonl_atomic(metadata_path, rows)
            with self.assertRaisesRegex(RuntimeError, "set changed"):
                run_meta.interrupt_audited_running_invocations(
                    out_dir, status="interrupted", audited=audited_a)
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(row["status"] == "running" for row in snapshot))

            duplicate = [
                running("invocation-a", "worker-a", 101, "sample-a"),
                running("invocation-a", "worker-c", 303, "sample-c"),
            ]
            run_meta._write_jsonl_atomic(metadata_path, duplicate)
            with self.assertRaisesRegex(RuntimeError, "identities are invalid"):
                run_meta.interrupt_audited_running_invocations(
                    out_dir, status="interrupted", audited=audited_a)
            snapshot = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(row["status"] == "running" for row in snapshot))

            run_meta._write_jsonl_atomic(metadata_path, [rows[0]])
            closed = run_meta.interrupt_audited_running_invocations(
                out_dir, status="interrupted", audited=audited_a)
            self.assertEqual(closed[0]["invocation_id"], "invocation-a")
            with self.assertRaisesRegex(RuntimeError, "non-running"):
                run_meta.finish_run_metadata(
                    out_dir, "invocation-a", status="finished")

        with self.assertRaisesRegex(RuntimeError, "identities are invalid"):
            run_meta.interrupt_audited_running_invocations(
                tempfile.gettempdir(), status="interrupted",
                audited=[dict(audited_a[0], invocation_id="")],
            )
