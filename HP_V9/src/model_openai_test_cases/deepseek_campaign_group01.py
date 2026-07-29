"""Mixin slice for test_model_openai.DeepSeekOpenCodeCampaignGroup01Mixin."""

from .support import *


class DeepSeekOpenCodeCampaignGroup01Mixin:
    def _write_capability_recovery_inspection_fixture(
            self, out_dir, worker_specs):
        manifest, first_sample, _worker_id, worker_pid = (
            self._write_inspection_fixture(out_dir))
        samples = [spec["sample"] for spec in worker_specs]
        first_plan = manifest["task_plans"][first_sample]
        states = list(first_plan["forward_state_sequence"])
        manifest["config"].update({
            "samples": samples,
            "run_metadata_storage": run_meta.RUN_METADATA_STORAGE_EVENT_V1,
            "provider_guard_mode": (
                paired_dispatch.PROVIDER_GUARD_WORKER_START_CAPABILITY_V1),
        })
        manifest["assignments"] = []
        manifest["task_plans"] = {}
        for spec in worker_specs:
            sample = spec["sample"]
            plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, states)
            manifest["task_plans"][sample] = {
                "path": os.path.basename(plan_path),
                "sha256": paired_dispatch._sha256(plan_path),
                "forward_state_sequence": states,
            }
            for method in manifest["config"]["method_set"]:
                os.makedirs(os.path.join(out_dir, method), exist_ok=True)
        run_meta.write_json_atomic(
            os.path.join(out_dir, "dispatch_manifest.json"), manifest)
        manifest_digest = paired_dispatch._canonical_record_sha256(manifest)
        dispatcher_pid = 4242
        dispatcher_instance_id = "dispatcher-recovery"
        template = run_meta._read_jsonl_records_with_retry(
            os.path.join(out_dir, "run_metadata.jsonl"))[0]
        dispatch_rows = []
        metadata = []
        capabilities = {}
        for index, spec in enumerate(worker_specs, 1):
            sample = spec["sample"]
            worker_id = spec["worker_launch_id"]
            pid = worker_pid + index
            invocation_id = f"invocation-{worker_id}"
            plan_sha = manifest["task_plans"][sample]["sha256"]
            capability = {
                "schema": "anchorpatch.worker_start/2",
                "worker_launch_id": worker_id,
                "worker_pid": pid,
                "invocation_id": invocation_id,
                "sample": sample,
                "task_plan_sha256": plan_sha,
                "provider_guard_mode": (
                    paired_dispatch
                    .PROVIDER_GUARD_WORKER_START_CAPABILITY_V1),
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "run_git_commit": spec["capability_commit"],
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "dispatch_manifest_canonical_sha256": manifest_digest,
            }
            capability_sha = paired_dispatch._canonical_record_sha256(
                capability)
            dispatch_rows.extend([{
                "event": "launch",
                "worker_launch_id": worker_id,
                "sample": sample,
                "pid": pid,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            }, {
                "event": "worker_authorized",
                **capability,
                "worker_start_capability_sha256": capability_sha,
            }])
            if spec.get("ack_published", True):
                _ready_path, start_path = (
                    paired_dispatch._worker_barrier_paths(
                        out_dir, worker_id))
                run_meta.write_json_atomic(start_path, capability)
            record = copy.deepcopy(template)
            record.update({
                "invocation_id": invocation_id,
                "worker_launch_id": worker_id,
                "worker_pid": pid,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "samples": [sample],
                "run_git_commit": spec["metadata_commit"],
                "git_tree_state": "clean",
                "status": "interrupted_by_dispatcher",
                "finished_at": "2026-07-29T00:00:00+08:00",
            })
            metadata.append(record)
            capabilities[worker_id] = {
                "record": capability,
                "sha256": capability_sha,
                "worker_pid": pid,
            }
        run_meta._write_jsonl_atomic(
            os.path.join(out_dir, "dispatch_log.jsonl"), dispatch_rows)
        paired_dispatch._write_active_worker_set(out_dir, manifest, [])
        return manifest, metadata, capabilities

    @staticmethod
    def _capability_recovery_incident_evidence(worker_specs, capabilities):
        recovered_ids = frozenset(
            spec["worker_launch_id"] for spec in worker_specs)
        return {
            "api_row_hashes": frozenset(),
            "api_incident_kinds": {},
            "attempt_row_hashes": frozenset(),
            "transport_sidecar_hashes": frozenset(),
            "transport_sidecars_by_api_row_hash": {},
            "worker_launch_ids": recovered_ids,
            "unpublished_worker_start_capability_ids": frozenset(
                spec["worker_launch_id"] for spec in worker_specs
                if not spec.get("ack_published", True)),
            "deepseek_recovered_workers": {},
            "preauthorization_worker_launch_ids": frozenset(),
            "provider_access_retry_authorizations": {},
            "provider_access_resume_samples": frozenset(),
            "dispatcher_parent_loss_workers": {
                spec["sample"]: {
                    "sample": spec["sample"],
                    "worker_launch_id": spec["worker_launch_id"],
                    "worker_pid": capabilities[
                        spec["worker_launch_id"]]["worker_pid"],
                    "invocation_id": (
                        f"invocation-{spec['worker_launch_id']}"),
                    "status": "running",
                }
                for spec in worker_specs
            },
            "authorization_id": "recovery-a",
        }

    def test_live_inspection_avoids_deepseek_api_linkage_torn_snapshot(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_rows = [
                self._deepseek_api_row(
                    out_dir, sample, worker_id, worker_pid, direction)
                for direction in ("forward", "backward")
            ]
            result_rows = self._fullrewrite_result_rows(
                sample, [row["request_id"] for row in api_rows])
            api_snapshot_taken = threading.Event()
            writer_done = threading.Event()
            inspection_results = []
            inspection_errors = []
            real_read_jsonl = paired_dispatch._read_jsonl

            def blocked_read_jsonl(path):
                rows = real_read_jsonl(path)
                if (os.path.basename(path) == "api_calls.jsonl"
                        and not api_snapshot_taken.is_set()):
                    api_snapshot_taken.set()
                    if not writer_done.wait(5):
                        raise RuntimeError(
                            "unit-test publication barrier timed out")
                return rows

            def inspect_live_campaign():
                try:
                    inspection_results.append(
                        paired_dispatch.inspect_campaign(
                            out_dir, manifest,
                            active_samples={sample}))
                except BaseException as exc:
                    inspection_errors.append(exc)

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=blocked_read_jsonl):
                inspector = threading.Thread(
                    target=inspect_live_campaign, daemon=True)
                inspector.start()
                self.assertTrue(api_snapshot_taken.wait(5))
                try:
                    for row in api_rows:
                        run_meta.append_jsonl_locked(
                            os.path.join(out_dir, "api_calls.jsonl"), row)
                    commit = run_meta.append_relay_rows_and_checkpoint(
                        os.path.join(
                            out_dir, "fullrewrite", f"{sample}.jsonl"),
                        os.path.join(
                            out_dir, "fullrewrite",
                            f"{sample}.ckpt.json"),
                        result_rows, {"completed_round_trips": 1},
                        campaign_out_dir=out_dir)
                    self.assertEqual(commit["status"], "appended")
                finally:
                    writer_done.set()
                inspector.join(5)

            self.assertFalse(inspector.is_alive())
            self.assertEqual(inspection_errors, [])
            self.assertEqual(len(inspection_results), 1)
            self.assertEqual(inspection_results[0]["errors"], [])
            self.assertEqual(len(real_read_jsonl(os.path.join(
                out_dir, "api_calls.jsonl"))), 2)
            self.assertEqual(len(real_read_jsonl(os.path.join(
                out_dir, "fullrewrite", f"{sample}.jsonl"))), 2)

    def test_active_retry_exhaustion_waits_for_sample_outcome_publication(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            request_id = "call-provisional-failure"
            api_row = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="fullrewrite", direction="forward",
                call_kind="fullrewrite_primary", request_id=request_id)
            api_row["semantic_call_id"] = "semantic-provisional"
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), api_row)
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                active = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples={sample})
                inactive = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples=set())
            self.assertEqual(active["errors"], [])
            self.assertIn(
                "DeepSeek API response incomplete at row 1",
                inactive["errors"])

    def test_active_infrastructure_outcome_waits_for_metadata_finish(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_row = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="hybridpatch", direction="forward",
                call_kind="hybridpatch_primary",
                request_id="call-outcome-before-metadata")
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), api_row)
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "sample_outcomes.jsonl"),
                self._deepseek_infrastructure_outcome(
                    sample, worker_id, worker_pid, api_row))

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                active = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples={sample})
                with self.assertRaisesRegex(
                        RuntimeError, "run metadata mismatch"):
                    paired_dispatch._inspect_deepseek_campaign(
                        out_dir, manifest, active_samples=set())

            self.assertEqual(active["errors"], [])

    def test_infrastructure_outcome_snapshot_precedes_api_snapshot(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_row = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="hybridpatch", direction="forward",
                call_kind="hybridpatch_primary",
                request_id="call-after-api-snapshot")
            outcome = self._deepseek_infrastructure_outcome(
                sample, worker_id, worker_pid, api_row)
            api_snapshot_taken = threading.Event()
            writer_done = threading.Event()
            inspection_results = []
            inspection_errors = []
            real_read_jsonl = paired_dispatch._read_jsonl

            def blocked_read_jsonl(path):
                rows = real_read_jsonl(path)
                if (os.path.basename(path) == "api_calls.jsonl"
                        and not api_snapshot_taken.is_set()):
                    api_snapshot_taken.set()
                    if not writer_done.wait(5):
                        raise RuntimeError(
                            "unit-test publication barrier timed out")
                return rows

            def inspect_live_campaign():
                try:
                    inspection_results.append(
                        paired_dispatch._inspect_deepseek_campaign(
                            out_dir, manifest, active_samples={sample}))
                except BaseException as exc:
                    inspection_errors.append(exc)

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=blocked_read_jsonl):
                inspector = threading.Thread(
                    target=inspect_live_campaign, daemon=True)
                inspector.start()
                self.assertTrue(api_snapshot_taken.wait(5))
                try:
                    run_meta.append_jsonl_locked(
                        os.path.join(out_dir, "api_calls.jsonl"), api_row)
                    run_meta.append_jsonl_locked(
                        os.path.join(out_dir, "sample_outcomes.jsonl"),
                        outcome)
                finally:
                    writer_done.set()
                inspector.join(5)

            self.assertFalse(inspector.is_alive())
            self.assertEqual(inspection_errors, [])
            self.assertEqual(len(inspection_results), 1)
            self.assertEqual(inspection_results[0]["errors"], [])

    def test_retry_incident_cohort_covers_uncommitted_primary_calls(self):
        with tempfile.TemporaryDirectory() as out_dir:
            _manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            forward = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                request_id="call-fr-forward")
            backward_failure = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="fullrewrite", direction="backward",
                call_kind="fullrewrite_primary",
                request_id="call-fr-backward-failure")
            fullrewrite_outcome = {
                "sample": sample,
                "method": "fullrewrite",
                "rt_index": 1,
                "direction": "backward",
                "call_kind": "fullrewrite_primary",
            }
            fullrewrite_evidence = {
                "request_id": backward_failure["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
            }
            self.assertEqual(
                paired_dispatch
                ._verified_deepseek_uncommitted_incident_request_ids(
                    out_dir, fullrewrite_outcome,
                    fullrewrite_evidence,
                    [forward, backward_failure], set()),
                {"call-fr-forward", "call-fr-backward-failure"},
            )

        with tempfile.TemporaryDirectory() as out_dir:
            _manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            primary = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                method="hybridpatch",
                call_kind="hybridpatch_primary",
                request_id="call-hp-primary")
            repair_failure = self._deepseek_failed_api_row(
                out_dir, sample, worker_id, worker_pid,
                method="hybridpatch", direction="forward",
                call_kind="hybridpatch_repair",
                request_id="call-hp-repair-failure")
            hybrid_outcome = {
                "sample": sample,
                "method": "hybridpatch",
                "rt_index": 1,
                "direction": "forward",
                "call_kind": "hybridpatch_repair",
            }
            hybrid_evidence = {
                "request_id": repair_failure["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
            }
            self.assertEqual(
                paired_dispatch
                ._verified_deepseek_uncommitted_incident_request_ids(
                    out_dir, hybrid_outcome, hybrid_evidence,
                    [primary, repair_failure], set()),
                {"call-hp-primary", "call-hp-repair-failure"},
            )

    def test_deepseek_inspection_rejects_persistent_missing_api_linkage(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            for direction in ("forward", "backward"):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    self._deepseek_api_row(
                        out_dir, sample, worker_id, worker_pid, direction))
            result_rows = self._fullrewrite_result_rows(
                sample, ["missing-forward", "missing-backward"])
            commit = run_meta.append_relay_rows_and_checkpoint(
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.jsonl"),
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.ckpt.json"),
                result_rows, {"completed_round_trips": 1},
                campaign_out_dir=out_dir)
            self.assertEqual(commit["status"], "appended")
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertEqual(set(inspection["errors"]), {
                "committed row API linkage mismatch: "
                "fullrewrite/sample/(1, 'forward')",
                "committed row API linkage mismatch: "
                "fullrewrite/sample/(1, 'backward')",
            })

    def test_current_stream_inspection_requires_matching_transport_sidecar(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            api_rows = [
                self._deepseek_api_row(
                    out_dir, sample, worker_id, worker_pid, direction)
                for direction in ("forward", "backward")
            ]
            api_rows[0]["raw_sse_saved_path"] = os.path.join(
                out_dir, "api_raw", "missing.transport.jsonl")
            for row in api_rows:
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            commit = run_meta.append_relay_rows_and_checkpoint(
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.jsonl"),
                os.path.join(
                    out_dir, "fullrewrite", f"{sample}.ckpt.json"),
                self._fullrewrite_result_rows(
                    sample, [row["request_id"] for row in api_rows]),
                {"completed_round_trips": 1},
                campaign_out_dir=out_dir,
            )
            self.assertEqual(commit["status"], "appended")
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "DeepSeek transport sidecar invalid at row 1",
                inspection["errors"],
            )

    def test_inspection_allows_only_hash_bound_historical_disconnect_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            disconnect = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                request_id="call-disconnect")
            disconnect_attempt = dict(
                disconnect["transport_attempts"][0],
                status="fatal_error",
                error_type="transport_disconnect",
                http_status=None,
                response_started_http_status=None,
                stream_complete=False,
                message_start_seen=False,
                message_delta_seen=False,
                message_stop_seen=False,
                final_usage_seen=False,
                generation_delta_seen=False,
                text_delta_seen=False,
                content_blocks_balanced=False,
                terminal_sequence_valid=False,
                retry_budget_consumed=True,
                retry_budget_attempt_index=1,
                error_message="Connection error.",
                finish_reason=None,
                stream_event_count=0,
                stream_event_canonical_bytes=0,
                stream_event_sha256=hashlib.sha256(b"").hexdigest(),
                text_delta_count=0,
                text_delta_utf8_bytes=0,
                reasoning_delta_count=0,
                reasoning_delta_utf8_bytes=0,
                tool_delta_count=0,
                tool_delta_utf8_bytes=0,
            )
            disconnect_sidecar = os.path.join(
                out_dir, "api_raw",
                "call-disconnect.failure.transport.jsonl")
            for event in ({
                    "transport_event_schema": (
                        "anchorpatch.transport_event/2"),
                    "record_type": "transport_header",
                    "transport_revision": (
                        paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                    "call_id": disconnect["request_id"],
                    "worker_launch_id": worker_id,
                    "worker_pid": worker_pid,
                    "sample": sample,
                    "method": disconnect["method"],
                    "rt_index": disconnect["rt_index"],
                    "direction": disconnect["direction"],
            }, {
                    "record_type": "attempt_start",
                    "attempt_index": 1,
                    "attempt_kind": "openai_compatible_initial",
            }, {
                    "record_type": "stream_summary",
                    "attempt_index": 1,
                    **{
                        key: value
                        for key, value in disconnect_attempt.items()
                        if key not in {
                            "attempt_index", "status", "error_type",
                            "http_status", "response_started_http_status",
                            "stream_complete", "message_delta_seen",
                            "thinking_delta_seen", "text_delta_seen",
                            "tool_delta_seen", "content_blocks_started",
                            "content_blocks_stopped",
                            "content_blocks_balanced",
                            "retry_budget_consumed",
                            "retry_budget_attempt_index",
                            "error_message",
                        }
                    },
                    "usage": None,
            }, {
                    "record_type": "attempt_end",
                    "attempt_index": 1,
                    "attempt": disconnect_attempt,
            }):
                run_meta.append_jsonl_locked(disconnect_sidecar, event)
            disconnect.update({
                "classification": "runner_exception",
                "response_classification": None,
                "error_type": "transport_disconnect",
                "http_status": None,
                "stream_complete": False,
                "count_as_method_failure": True,
                "input_tokens": None,
                "output_tokens": None,
                "transport_attempts": [disconnect_attempt],
                "http_attempts_used": 1,
                "retry_count": 0,
                "failed_attempt_count": 1,
                "raw_sse_saved_path": disconnect_sidecar,
                "runner_exception": (
                    "OpenAICompatibleTransportError: OpenAI-compatible "
                    "provider failed after 1 attempt(s)"
                ),
            })
            disconnect.update(
                run_meta._transport_sidecar_facts(disconnect_sidecar))

            stopped = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "backward",
                request_id="call-dispatcher-stopped")
            stopped_success_attempt = stopped["transport_attempts"][0]
            stopped.update({
                "classification": "runner_exception",
                "response_classification": None,
                "error_type": "CampaignStoppedError",
                "http_status": None,
                "stream_complete": False,
                "count_as_method_failure": True,
                "input_tokens": None,
                "output_tokens": None,
                "transport_attempts": [],
                "http_attempts_used": None,
                "retry_count": None,
                "failed_attempt_count": None,
                "raw_request_saved_path": None,
                "runner_exception": (
                    "CampaignStoppedError: campaign stop latch is set: "
                    "worker_fatal_error"
                ),
            })
            partial_attempt = dict(
                stopped_success_attempt,
                status="retryable_error",
                http_status=200,
                error_type="incomplete_stream",
                error_message=(
                    "OpenAI-compatible stream ended without finish_reason "
                    "and final_usage"
                ),
                response_started_http_status=200,
                stream_complete=False,
                message_start_seen=True,
                message_stop_seen=False,
                final_usage_seen=False,
                generation_delta_seen=True,
                terminal_sequence_valid=False,
                retry_budget_consumed=True,
                retry_budget_attempt_index=1,
                finish_reason=None,
                stream_event_count=1,
                stream_event_canonical_bytes=10,
                stream_event_sha256="b" * 64,
                text_delta_count=1,
                text_delta_utf8_bytes=1,
                reasoning_delta_count=0,
                reasoning_delta_utf8_bytes=0,
                tool_delta_count=0,
                tool_delta_utf8_bytes=0,
            )
            partial_sidecar = os.path.join(
                out_dir, "api_raw",
                "call-dispatcher-stopped.partial.transport.jsonl")
            linkage = {
                "transport_event_schema": (
                    "anchorpatch.transport_event/2"),
                "record_type": "transport_header",
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "call_id": stopped["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "sample": sample,
                "method": stopped["method"],
                "rt_index": stopped["rt_index"],
                "direction": stopped["direction"],
            }
            for event in ({
                    **linkage,
            }, {
                    "record_type": "attempt_start",
                    "attempt_index": 1,
                    "attempt_kind": "openai_compatible_initial",
            }, {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "first_chunk",
                    "stream_event_count": 1,
            }, {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "generation_started",
                    "stream_event_count": 1,
                    "text_delta_seen": True,
                    "thinking_delta_seen": False,
                    "tool_delta_seen": False,
            }, {
                    "record_type": "stream_summary",
                    "attempt_index": 1,
                    **{
                        key: value for key, value in partial_attempt.items()
                        if key not in {
                            "attempt_index", "status", "error_type",
                            "http_status", "response_started_http_status",
                            "stream_complete", "message_delta_seen",
                            "thinking_delta_seen", "text_delta_seen",
                            "tool_delta_seen", "content_blocks_started",
                            "content_blocks_stopped", "content_blocks_balanced",
                            "retry_budget_consumed",
                            "retry_budget_attempt_index", "error_message",
                        }
                    },
                    "usage": None,
            }, {
                    "record_type": "attempt_end",
                    "attempt_index": 1,
                    "attempt": partial_attempt,
            }):
                run_meta.append_jsonl_locked(partial_sidecar, event)
            stopped["raw_sse_saved_path"] = partial_sidecar
            rows = [disconnect, stopped]
            for row in rows:
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_calls.jsonl"), row)
            incident_kinds = {
                run_meta._canonical_record_sha256(disconnect): (
                    "deepseek_transport_disconnect_misclassification"),
                run_meta._canonical_record_sha256(stopped): (
                    "deepseek_dispatcher_stopped_uncommitted_api"),
            }
            recovery = {
                "api_row_hashes": frozenset(incident_kinds),
                "api_incident_kinds": incident_kinds,
                "attempt_row_hashes": frozenset(),
                "transport_sidecar_hashes": frozenset(),
                "transport_sidecars_by_api_row_hash": {},
                "worker_launch_ids": frozenset({worker_id}),
                "preauthorization_worker_launch_ids": frozenset(),
                "provider_access_retry_authorizations": {},
                "provider_access_resume_samples": frozenset(),
                "dispatcher_parent_loss_workers": {},
                "authorization_id": "recovery-a",
            }
            partial_evidence = (
                run_meta._deepseek_dispatcher_stopped_sidecar_evidence(
                    out_dir, stopped)
            )
            recovery["transport_sidecar_hashes"] = frozenset({
                partial_evidence["sha256"],
            })
            recovery["transport_sidecars_by_api_row_hash"] = {
                run_meta._canonical_record_sha256(stopped):
                partial_evidence,
            }
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch,
                        "campaign_recovery_incident_evidence",
                        return_value=recovery):
                accepted = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertEqual(accepted["errors"], [])

            run_meta.append_jsonl_locked(partial_sidecar, {
                **linkage,
                "record_type": "sdk_stream_event",
                "event": {"choices": [{"delta": {"content": "tamper"}}]},
            })
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch,
                        "campaign_recovery_incident_evidence",
                        return_value=recovery):
                tampered = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "DeepSeek transport sidecar invalid at row 2",
                tampered["errors"],
            )

            forged = {
                **recovery,
                "api_row_hashes": frozenset({"0" * 64}),
                "api_incident_kinds": {
                    "0" * 64: (
                        "deepseek_transport_disconnect_misclassification"),
                },
            }
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        paired_dispatch,
                        "campaign_recovery_incident_evidence",
                        return_value=forged):
                rejected = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "DeepSeek API response incomplete at row 1",
                rejected["errors"],
            )
            self.assertIn(
                "DeepSeek retry evidence invalid at row 2",
                rejected["errors"],
            )
            self.assertIn(
                "DeepSeek transport sidecar invalid at row 2",
                rejected["errors"],
            )

    def test_current_dispatcher_stopped_sidecar_rejects_event1_downgrade(self):
        with tempfile.TemporaryDirectory() as out_dir:
            _manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            row = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                request_id="call-schema-downgrade")
            path = os.path.join(
                out_dir, "api_raw", "schema-downgrade.transport.jsonl")
            linkage = {
                "transport_event_schema": "anchorpatch.transport_event/1",
                "call_id": row["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "sample": sample,
                "method": row["method"],
                "rt_index": row["rt_index"],
                "direction": row["direction"],
                "attempt_index": 1,
            }
            for event in ({
                    **linkage,
                    "record_type": "attempt_start",
            }, {
                    **linkage,
                    "record_type": "sdk_stream_event",
                    "event": {"choices": [{"delta": {"content": "x"}}]},
            }, {
                    **linkage,
                    "record_type": "attempt_end",
                    "attempt": row["transport_attempts"][0],
            }):
                run_meta.append_jsonl_locked(path, event)
            row["raw_sse_saved_path"] = path
            with self.assertRaisesRegex(RuntimeError, "was downgraded"):
                run_meta._deepseek_dispatcher_stopped_sidecar_evidence(
                    out_dir, row)

    def test_compact_recovery_parser_distinguishes_closed_and_open_prefixes(self):
        with tempfile.TemporaryDirectory() as out_dir:
            _manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            row = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward",
                request_id="call-compact-parser")
            records = run_meta._read_deepseek_transport_sidecar(
                row["raw_sse_saved_path"])["rows"]
            linkage = {
                "call_id": row["request_id"],
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "sample": sample,
                "method": row["method"],
                "rt_index": row["rt_index"],
                "direction": row["direction"],
            }
            closed = run_meta._validate_deepseek_compact_records(
                records, expected_linkage=linkage)
            self.assertEqual(
                closed["recovery_state"], "complete_unpublished_response")

            header_only = run_meta._validate_deepseek_compact_records(
                records[:1], expected_linkage=linkage,
                allow_open_final=True)
            self.assertEqual(
                header_only["recovery_state"], "pre_attempt_no_post")
            with self.assertRaisesRegex(RuntimeError, "not closed"):
                run_meta._validate_deepseek_compact_records(
                    records[:1], expected_linkage=linkage)

            complete_summary = run_meta._validate_deepseek_compact_records(
                records[:-1], expected_linkage=linkage,
                allow_open_final=True)
            self.assertEqual(
                complete_summary["recovery_state"],
                "complete_unpublished_response")

            empty_terminal_summary = copy.deepcopy(records[-2])
            empty_terminal_summary.update({
                "stream_event_count": 0,
                "stream_event_canonical_bytes": 0,
                "stream_event_sha256": hashlib.sha256(b"").hexdigest(),
                "text_delta_count": 0,
                "text_delta_utf8_bytes": 0,
                "reasoning_delta_count": 0,
                "reasoning_delta_utf8_bytes": 0,
                "tool_delta_count": 0,
                "tool_delta_utf8_bytes": 0,
                "message_start_seen": False,
                "generation_delta_seen": False,
            })
            empty_terminal_records = [
                records[0],
                records[1],
                {
                    **records[4],
                    "stream_event_count": 0,
                },
                {
                    **records[5],
                    "stream_event_count": 0,
                },
                empty_terminal_summary,
            ]
            with self.assertRaisesRegex(
                    RuntimeError, "complete summary is invalid"):
                run_meta._validate_deepseek_compact_records(
                    empty_terminal_records,
                    expected_linkage=linkage,
                    allow_open_final=True,
                )

            invalid_usage = [
                records[0], records[1], {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "usage_seen",
                    "stream_event_count": 1,
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 99,
                    },
                },
            ]
            with self.assertRaisesRegex(RuntimeError, "usage checkpoint"):
                run_meta._validate_deepseek_compact_records(
                    invalid_usage, expected_linkage=linkage,
                    allow_open_final=True)

            failed_out_of_order = copy.deepcopy(records)
            checkpoint_indexes = [
                index for index, item in enumerate(failed_out_of_order)
                if item.get("record_type") == "stream_checkpoint"
            ]
            generation_index = next(
                index for index in checkpoint_indexes
                if failed_out_of_order[index].get("checkpoint")
                == "generation_started"
            )
            finish_index = next(
                index for index in checkpoint_indexes
                if failed_out_of_order[index].get("checkpoint")
                == "finish_seen"
            )
            failed_out_of_order[generation_index], failed_out_of_order[
                finish_index] = (
                    failed_out_of_order[finish_index],
                    failed_out_of_order[generation_index],
                )
            for index in checkpoint_indexes:
                failed_out_of_order[index]["stream_event_count"] = 1
            failed_summary = next(
                item for item in failed_out_of_order
                if item.get("record_type") == "stream_summary"
            )
            failed_summary["terminal_sequence_valid"] = False
            failed_attempt = next(
                item["attempt"] for item in failed_out_of_order
                if item.get("record_type") == "attempt_end"
            )
            failed_attempt.update({
                "status": "retryable_error",
                "http_status": 502,
                "error_type": "http_502",
                "stream_complete": False,
                "terminal_sequence_valid": False,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": 1,
            })
            with self.assertRaisesRegex(RuntimeError, "checkpoint order"):
                run_meta._validate_deepseek_compact_records(
                    failed_out_of_order,
                    expected_linkage=linkage,
                )

            with self.assertRaisesRegex(RuntimeError, "attempt_start"):
                run_meta._validate_deepseek_compact_records(
                    [records[0], records[-1]],
                    expected_linkage=linkage,
                    allow_open_final=True)

    def test_recovery_file_prefix_allows_append_but_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "ledger.jsonl")
            with open(path, "wb") as handle:
                handle.write(b'{"row":1}\n')
            evidence = ledger_recovery._file_prefix_evidence(path)
            with open(path, "ab") as handle:
                handle.write(b'{"row":2}\n')
            self.assertTrue(run_meta._file_prefix_matches(path, evidence))
            with open(path, "r+b") as handle:
                handle.write(b"X")
            self.assertFalse(run_meta._file_prefix_matches(path, evidence))

    def test_recovery_metadata_allows_finish_and_manifest_plan_extension(
            self):
        with tempfile.TemporaryDirectory() as out_dir:
            path = os.path.join(out_dir, "run_metadata.jsonl")
            old_plan = {"sha256": "1" * 64, "round_trips": 10}
            new_plan_path = os.path.join(out_dir, "new.task_plan.json")
            run_meta.write_json_atomic(new_plan_path, {
                "forward_state_sequence": [
                    f"state-{index}" for index in range(10)
                ],
            })
            new_plan = {
                "sha256": run_meta._sha256_file(new_plan_path),
                "round_trips": 10,
            }
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"),
                {
                    "config": {"num_round_trips": 10},
                    "task_plans": {
                        "old": {"sha256": old_plan["sha256"]},
                        "new": {"sha256": new_plan["sha256"]},
                    },
                },
            )
            rows = [{
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": "old-a",
                "worker_launch_id": "worker-a",
                "status": "failed",
                "finished_at": "2026-07-28T10:26:09+08:00",
                "invocation_finished_at": (
                    "2026-07-28T10:26:09+08:00"),
                "task_plans": {"old": old_plan},
            }]
            run_meta.append_jsonl_locked(path, rows[0])
            identities = run_meta._run_metadata_recovery_identities(path)

            rows[0]["finished_at"] = None
            rows.append({
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": "new-b",
                "worker_launch_id": "worker-b",
                "status": "running",
                "finished_at": None,
                "task_plans": {"old": old_plan},
            })
            run_meta._write_jsonl_atomic(path, rows)
            run_meta.register_task_plan(
                out_dir, "new", new_plan_path, num_round_trips=10)
            self.assertTrue(
                run_meta._run_metadata_recovery_prefix_matches(
                    path, identities))

            run_meta.finish_run_metadata(
                out_dir, "new-b", status="finished")
            self.assertTrue(
                run_meta._run_metadata_recovery_prefix_matches(
                    path, identities))
            rows = run_meta.read_run_metadata_snapshot(out_dir)
            rows[0]["status"] = "forged"
            run_meta._write_jsonl_atomic(path, rows)
            self.assertFalse(
                run_meta._run_metadata_recovery_prefix_matches(
                    path, identities))

    def test_event_metadata_recovery_binds_prefix_and_projection(self):
        kwargs = run_metadata_kwargs(command="python event-recovery-test")
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "recovery"}):
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            run_meta.write_json_atomic(plan_path, {
                "forward_state_sequence": ["state"]})
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"), {
                    "config": {
                        "num_round_trips": 1,
                        "run_metadata_storage": (
                            run_meta.RUN_METADATA_STORAGE_EVENT_V1),
                    },
                    "task_plans": {
                        "sample": {"sha256": run_meta._sha256_file(plan_path)},
                    },
                })
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
            evidence = run_meta._run_metadata_recovery_identities(
                metadata_path)
            self.assertEqual(
                evidence["schema"], run_meta.METADATA_RECOVERY_EVIDENCE_SCHEMA)

            run_meta.register_task_plan(
                out_dir, "sample", plan_path, num_round_trips=1)
            self.assertTrue(run_meta._run_metadata_recovery_prefix_matches(
                metadata_path, evidence))
            run_meta.finish_run_metadata(
                out_dir, invocation["invocation_id"])
            self.assertTrue(run_meta._run_metadata_recovery_prefix_matches(
                metadata_path, evidence))
            self.assertFalse(run_meta._run_metadata_recovery_prefix_matches(
                metadata_path, [{"row_number": 1}]))

            events_path = os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            original = pathlib.Path(events_path).read_bytes()
            with open(events_path, "r+b") as handle:
                handle.write(b"X")
            self.assertFalse(run_meta._run_metadata_recovery_prefix_matches(
                metadata_path, evidence))
            pathlib.Path(events_path).write_bytes(original)
            pathlib.Path(events_path).write_bytes(
                original[:evidence["event_prefix_size_bytes"] - 1])
            self.assertFalse(run_meta._run_metadata_recovery_prefix_matches(
                metadata_path, evidence))

    def test_deepseek_transport_recovery_requires_free_dispatcher_lease(self):
        with tempfile.TemporaryDirectory() as out_dir:
            lease_path = os.path.join(out_dir, ".paired_dispatch.lock")
            with open(lease_path, "a+", encoding="utf-8") as lease:
                portalocker.lock(
                    lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
                try:
                    with self.assertRaisesRegex(
                            RuntimeError,
                            "dispatcher lease to be free"):
                        ledger_recovery.authorize(
                            out_dir,
                            deepseek_transport_disconnect_retry=True,
                        )
                finally:
                    portalocker.unlock(lease)

    def test_inspector_rejects_api_row_with_wrong_worker_capability_sha(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, sample, worker_id, worker_pid = (
                self._write_inspection_fixture(out_dir))
            manifest["config"].update({
                "run_metadata_storage": run_meta.RUN_METADATA_STORAGE_EVENT_V1,
                "provider_guard_mode": (
                    paired_dispatch
                    .PROVIDER_GUARD_WORKER_START_CAPABILITY_V1),
            })
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"), manifest)
            dispatcher_pid = 4242
            dispatcher_instance_id = "dispatcher-a"
            plan_sha = manifest["task_plans"][sample]["sha256"]
            capability = {
                "schema": "anchorpatch.worker_start/2",
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "invocation_id": "invocation-a",
                "sample": sample,
                "task_plan_sha256": plan_sha,
                "provider_guard_mode": (
                    paired_dispatch
                    .PROVIDER_GUARD_WORKER_START_CAPABILITY_V1),
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "run_git_commit": manifest["run_git_commit"],
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "dispatch_manifest_canonical_sha256": (
                    paired_dispatch._canonical_record_sha256(manifest)),
            }
            _ready_path, start_path = paired_dispatch._worker_barrier_paths(
                out_dir, worker_id)
            run_meta.write_json_atomic(start_path, capability)
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "dispatch_log.jsonl"), [{
                    "event": "launch",
                    "worker_launch_id": worker_id,
                    "sample": sample,
                    "pid": worker_pid,
                    "dispatcher_pid": dispatcher_pid,
                    "dispatcher_instance_id": dispatcher_instance_id,
                }, {
                    "event": "worker_authorized",
                    **capability,
                    "worker_start_capability_sha256": (
                        paired_dispatch._canonical_record_sha256(capability)),
                }])
            api_row = self._deepseek_api_row(
                out_dir, sample, worker_id, worker_pid, "forward")
            api_row["worker_start_capability_sha256"] = "f" * 64
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), api_row)
            metadata = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "run_metadata.jsonl"))

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(manifest["run_git_commit"], "clean")), \
                    mock.patch.object(
                        paired_dispatch, "read_run_metadata_snapshot",
                        return_value=metadata):
                inspection = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples={sample})
            self.assertIn(
                "API worker capability mismatch at row 1",
                inspection["errors"],
            )
            self.assertNotIn(
                f"worker start capability mismatch: {worker_id}",
                inspection["errors"],
            )

    def test_inspector_accepts_mixed_original_and_recovery_capability_commits(self):
        original_commit = "1" * 40
        recovery_commit = "2" * 40
        worker_specs = [{
            "sample": "historical-sample",
            "worker_launch_id": "worker-historical",
            "capability_commit": original_commit,
            "metadata_commit": original_commit,
            "ack_published": True,
        }, {
            "sample": "recovery-sample",
            "worker_launch_id": "worker-recovery",
            "capability_commit": recovery_commit,
            "metadata_commit": recovery_commit,
            "ack_published": True,
        }]
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, metadata, capabilities = (
                self._write_capability_recovery_inspection_fixture(
                    out_dir, worker_specs))
            evidence = self._capability_recovery_incident_evidence(
                worker_specs, capabilities)
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(recovery_commit, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "read_run_metadata_snapshot",
                        return_value=metadata), mock.patch.object(
                            paired_dispatch,
                            "read_campaign_recovery_authorization",
                            return_value={
                                "recovery_kind": (
                                    run_meta
                                    .DISPATCHER_PROCESS_LOST_RECOVERY_KIND),
                                "recovery_git_commit": recovery_commit,
                            }), mock.patch.object(
                                paired_dispatch,
                                "campaign_recovery_incident_evidence",
                                return_value=evidence):
                inspection = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples=set())
            self.assertFalse(any(
                "worker start capability mismatch" in error
                for error in inspection["errors"]
            ), inspection["errors"])

    def test_inspector_rejects_capability_commit_mismatched_with_metadata(self):
        original_commit = "1" * 40
        recovery_commit = "2" * 40
        worker_specs = [{
            "sample": "historical-sample",
            "worker_launch_id": "worker-historical",
            "capability_commit": original_commit,
            "metadata_commit": recovery_commit,
            "ack_published": True,
        }]
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, metadata, capabilities = (
                self._write_capability_recovery_inspection_fixture(
                    out_dir, worker_specs))
            evidence = self._capability_recovery_incident_evidence(
                worker_specs, capabilities)
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=(recovery_commit, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "read_run_metadata_snapshot",
                        return_value=metadata), mock.patch.object(
                            paired_dispatch,
                            "read_campaign_recovery_authorization",
                            return_value={
                                "recovery_kind": (
                                    run_meta
                                    .DISPATCHER_PROCESS_LOST_RECOVERY_KIND),
                                "recovery_git_commit": recovery_commit,
                            }), mock.patch.object(
                                paired_dispatch,
                                "campaign_recovery_incident_evidence",
                                return_value=evidence):
                inspection = paired_dispatch._inspect_deepseek_campaign(
                    out_dir, manifest, active_samples=set())
            self.assertIn(
                "worker capability/metadata identity mismatch: "
                "worker-historical",
                inspection["errors"],
            )

    def test_recovered_partial_ack_zero_post_is_accepted_but_post_is_rejected(self):
        recovery_commit = "2" * 40
        worker_specs = [{
            "sample": "ack-published-sample",
            "worker_launch_id": "worker-ack-published",
            "capability_commit": recovery_commit,
            "metadata_commit": recovery_commit,
            "ack_published": True,
        }, {
            "sample": "ack-missing-sample",
            "worker_launch_id": "worker-ack-missing",
            "capability_commit": recovery_commit,
            "metadata_commit": recovery_commit,
            "ack_published": False,
        }]
        with tempfile.TemporaryDirectory() as out_dir:
            manifest, metadata, capabilities = (
                self._write_capability_recovery_inspection_fixture(
                    out_dir, worker_specs))
            evidence = self._capability_recovery_incident_evidence(
                worker_specs, capabilities)

            def inspect():
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=(recovery_commit, "clean")), \
                        mock.patch.object(
                            paired_dispatch, "read_run_metadata_snapshot",
                            return_value=metadata), mock.patch.object(
                                paired_dispatch,
                                "read_campaign_recovery_authorization",
                                return_value={
                                    "recovery_kind": (
                                        run_meta
                                        .DISPATCHER_PROCESS_LOST_RECOVERY_KIND),
                                    "recovery_git_commit": recovery_commit,
                                }), mock.patch.object(
                                    paired_dispatch,
                                    "campaign_recovery_incident_evidence",
                                    return_value=evidence):
                    return paired_dispatch._inspect_deepseek_campaign(
                        out_dir, manifest, active_samples=set())

            before_post = inspect()
            self.assertNotIn(
                "worker start capability mismatch: worker-ack-missing",
                before_post["errors"],
            )

            missing = capabilities["worker-ack-missing"]
            api_row = self._deepseek_api_row(
                out_dir, "ack-missing-sample", "worker-ack-missing",
                missing["worker_pid"], "forward",
                request_id="call-after-missing-ack")
            api_row["worker_start_capability_sha256"] = missing["sha256"]
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_calls.jsonl"), api_row)
            after_post = inspect()
            self.assertIn(
                "unpublished worker capability has API evidence: "
                "worker-ack-missing",
                after_post["errors"],
            )

    def test_resume_classifier_pending_requires_explicit_recovery_mode(self):
        with tempfile.TemporaryDirectory() as out_dir:
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"),
                {"schema": paired_dispatch.SCHEMA},
            )
            run_meta.write_json_atomic(
                os.path.join(
                    out_dir,
                    run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
                ),
                {"followup_kind": "resume_classifier"},
            )
            with self.assertRaisesRegex(
                    RuntimeError, "requires its explicit recovery mode"):
                ledger_recovery.authorize(
                    out_dir,
                    deepseek_transport_disconnect_retry=True,
                )
            pending_path = os.path.join(
                out_dir,
                run_meta.DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_FILENAME,
            )
            run_meta.write_json_atomic(pending_path, {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "authorization_record": {
                    "deepseek_resume_classifier_followup_recovery": True,
                    "authorization_basis": (
                        ledger_recovery
                        ._DEEPSEEK_RESUME_CLASSIFIER_AUTHORIZATION_BASIS),
                },
            })
            with self.assertRaisesRegex(
                    RuntimeError, "pending mode binding is invalid"):
                ledger_recovery.authorize(
                    out_dir,
                    deepseek_transport_disconnect_retry=True,
                )
            run_meta.write_json_atomic(pending_path, {
                "schema": (
                    ledger_recovery
                    ._DEEPSEEK_TRANSPORT_INSPECTOR_PENDING_SCHEMA),
                "followup_kind": "resume_classifier",
                "authorization_record": {},
            })
            with self.assertRaisesRegex(
                    RuntimeError, "pending mode binding is invalid"):
                ledger_recovery.authorize(
                    out_dir,
                    deepseek_resume_classifier_retry=True,
                )
