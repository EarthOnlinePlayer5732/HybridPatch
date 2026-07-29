"""Mixin slice for test_model_openai.DeepSeekOpenCodeCampaignGroup03Mixin."""

from .support import *


class DeepSeekOpenCodeCampaignGroup03Mixin:
    def test_retry_audit_allows_unbounded_free_503_attempts(self):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "stream_complete": False,
                "terminal_sequence_valid": False,
                "generation_delta_seen": False,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            }
            for index in range(1, 6)
        ]
        attempts.append({
            "attempt_index": 6,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
            "finish_reason": "stop",
        })
        self.assertTrue(paired_dispatch._valid_deepseek_retry_evidence({
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_attempts": attempts,
            "http_attempts_used": 6,
            "retry_count": 5,
            "failed_attempt_count": 5,
            "max_retries": 3,
        }))

    def test_retry_audit_rejects_exhausted_non_503_budget(self):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "rate_limit",
                "http_status": 429,
                "stream_complete": False,
                "terminal_sequence_valid": False,
                "generation_delta_seen": False,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": index,
            }
            for index in range(1, 4)
        ]
        attempts.append({
            "attempt_index": 4,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
            "finish_reason": "stop",
        })
        self.assertFalse(paired_dispatch._valid_deepseek_retry_evidence({
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_attempts": attempts,
            "http_attempts_used": 4,
            "retry_count": 3,
            "failed_attempt_count": 3,
            "max_retries": 3,
        }))

    def test_retry_audit_requires_explicit_current_stream_terminal_evidence(self):
        success = {
            "attempt_index": 2,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
            "finish_reason": "stop",
        }
        row = {
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "stream_complete": False,
                "terminal_sequence_valid": False,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            }, success],
            "http_attempts_used": 2,
            "retry_count": 1,
            "failed_attempt_count": 1,
            "max_retries": 3,
        }
        self.assertFalse(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        row["transport_attempts"][0]["generation_delta_seen"] = False
        self.assertTrue(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        del success["terminal_sequence_valid"]
        self.assertFalse(
            paired_dispatch._valid_deepseek_retry_evidence(row))

    def test_retry_audit_keeps_frozen_legacy_nonstream_rows_readable(self):
        row = {
            "transport": paired_dispatch.DEEPSEEK_LEGACY_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_LEGACY_TRANSPORT_REVISION),
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            }, {
                "attempt_index": 2,
                "status": "success",
                "http_status": 200,
                "stream_complete": True,
            }],
            "http_attempts_used": 2,
            "retry_count": 1,
            "failed_attempt_count": 1,
            "max_retries": 3,
        }
        self.assertTrue(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        self.assertTrue(
            paired_dispatch._valid_deepseek_transport_sidecar(
                "unused", row))

    def test_previous_stream_revision_remains_readable(self):
        row = {
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch
                .DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION),
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "success",
                "http_status": 200,
                "stream_complete": True,
                "message_start_seen": True,
                "message_stop_seen": True,
                "final_usage_seen": True,
                "terminal_sequence_valid": True,
            }],
            "http_attempts_used": 1,
            "retry_count": 0,
            "failed_attempt_count": 0,
            "max_retries": 3,
        }
        self.assertTrue(
            paired_dispatch._valid_deepseek_retry_evidence(row))
        with tempfile.TemporaryDirectory() as out_dir:
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"), {
                    "config": {
                        "model": paired_dispatch.DEEPSEEK_MODEL,
                        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                        "transport_revision": (
                            paired_dispatch
                            .DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION),
                    },
                })
            self.assertEqual(
                paired_dispatch._deepseek_campaign_runtime_identity(out_dir),
                (
                    paired_dispatch.DEEPSEEK_TRANSPORT,
                    paired_dispatch
                    .DEEPSEEK_PREVIOUS_STREAM_TRANSPORT_REVISION,
                ),
            )

    def test_linear_stream_revision_requires_exact_sidecar_linkage(self):
        attempt = {
            "attempt_index": 1,
            "status": "success",
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "terminal_sequence_valid": True,
            "finish_reason": "stop",
        }
        row = {
            "request_id": "call-linear",
            "worker_launch_id": "worker-linear",
            "worker_pid": 101,
            "sample": "sample",
            "method": "fullrewrite",
            "rt_index": 1,
            "direction": "forward",
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_LINEAR_STREAM_TRANSPORT_REVISION),
            "transport_attempts": [attempt],
        }
        linkage = {
            "transport_event_schema": "anchorpatch.transport_event/1",
            "worker_launch_id": row["worker_launch_id"],
            "worker_pid": row["worker_pid"],
            "sample": row["sample"],
            "method": row["method"],
            "rt_index": row["rt_index"],
            "direction": row["direction"],
            "call_id": row["request_id"],
            "attempt_index": 1,
        }
        with tempfile.TemporaryDirectory() as out_dir:
            sidecar = os.path.join(out_dir, "linear.transport.jsonl")
            for event in (
                    {**linkage, "record_type": "attempt_start"},
                    {
                        **linkage,
                        "record_type": "sdk_stream_event",
                        "event": {"choices": [{"finish_reason": "stop"}]},
                    },
                    {
                        **linkage,
                        "record_type": "attempt_end",
                        "attempt": attempt,
                    }):
                run_meta.append_jsonl_locked(sidecar, event)
            row["raw_sse_saved_path"] = sidecar
            self.assertTrue(
                paired_dispatch._valid_deepseek_transport_sidecar(
                    out_dir, row))

            forged = [
                {**event, "call_id": "other-call"}
                for event in run_meta._read_jsonl_records_with_retry(sidecar)
            ]
            run_meta._write_jsonl_atomic(sidecar, forged)
            self.assertFalse(
                paired_dispatch._valid_deepseek_transport_sidecar(
                    out_dir, row))

    def test_partial_stream_exhaustion_routes_to_deepseek_sample_isolation(self):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "incomplete_stream",
                "http_status": 503,
                "stream_complete": False,
                "generation_delta_seen": True,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": index,
            }
            for index in range(1, 4)
        ]
        api_row = {
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "classification": "provider/API failure",
            "error_type": "incomplete_stream",
            "http_status": 503,
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "provider_called": True,
            "stream_complete": False,
            "response_replayed": False,
            "count_as_method_failure": False,
            "transport_attempts": attempts,
            "http_attempts_used": len(attempts),
            "retry_count": len(attempts) - 1,
            "failed_attempt_count": len(attempts),
            "max_retries": 3,
        }
        self.assertTrue(
            paired_dispatch._deepseek_failed_retry_row(api_row))

        outcome = {
            "status": "infrastructure_incomplete",
            "worker_launch_id": "worker-a",
            "worker_pid": 123,
            "methods": ["hybridpatch", "fullrewrite"],
            "classification": "provider/API failure",
            "error_type": "incomplete_stream",
            "method_phase": None,
        }
        expected = {"request_id": "call-a"}
        with mock.patch.object(
                paired_dispatch, "read_campaign_stop_conditions",
                return_value=[]), mock.patch.object(
                    paired_dispatch, "_latest_sample_outcomes",
                    return_value={"sample": outcome}), mock.patch.object(
                        paired_dispatch, "_worker_lease_is_held",
                        return_value=False), mock.patch.object(
                            paired_dispatch,
                            "_deepseek_campaign_runtime_identity",
                            return_value=(
                                paired_dispatch.DEEPSEEK_TRANSPORT,
                                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION,
                            )), mock.patch.object(
                                paired_dispatch,
                                "_verified_deepseek_infrastructure_incomplete",
                                return_value=expected) as verify:
            actual = paired_dispatch._verified_infrastructure_incomplete(
                "unused", "sample", {
                    "worker_launch_id": "worker-a",
                    "worker_pid": 123,
                    "methods": ["hybridpatch", "fullrewrite"],
                    "method_phase": None,
                })
        self.assertEqual(actual, expected)
        verify.assert_called_once_with(
            "unused", "sample",
            {
                "worker_launch_id": "worker-a",
                "worker_pid": 123,
                "methods": ["hybridpatch", "fullrewrite"],
                "method_phase": None,
            },
            outcome,
        )

    def test_resume_accepts_only_hash_bound_disconnect_misclassification(self):
        sample = "sample"
        worker_id = "worker-a"
        worker_pid = 123
        api_row = {
            "sample": sample,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "classification": "runner_exception",
            "error_type": "transport_disconnect",
            "http_status": None,
            "provider_called": True,
            "stream_complete": False,
            "response_replayed": False,
            "count_as_method_failure": True,
            "transport_attempts": [{
                "attempt_index": 1,
                "status": "fatal_error",
                "error_type": "transport_disconnect",
            }],
        }
        api_digest = run_meta._canonical_record_sha256(api_row)
        assignments = [{
            "sample": sample,
            "methods": ["hybridpatch", "fullrewrite"],
        }]
        metadata = [{
            "samples": [sample],
            "status": "failed",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
        }]
        dispatch_rows = [{
            "event": "worker_exit",
            "sample": sample,
            "worker_launch_id": worker_id,
            "pid": worker_pid,
            "returncode": 1,
            "disposition": "campaign_fatal",
        }]

        def read_jsonl(path):
            if os.path.basename(path) == "dispatch_log.jsonl":
                return dispatch_rows
            if os.path.basename(path) == "api_calls.jsonl":
                return [api_row]
            raise AssertionError(path)

        recovery = {
            "worker_launch_ids": frozenset({worker_id}),
            "dispatcher_parent_loss_workers": {},
            "api_incident_kinds": {
                api_digest: (
                    "deepseek_transport_disconnect_misclassification"),
            },
        }
        patches = (
            mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=recovery),
            mock.patch.object(
                paired_dispatch, "read_run_metadata_snapshot",
                return_value=metadata),
            mock.patch.object(
                paired_dispatch, "_read_jsonl", side_effect=read_jsonl),
            mock.patch.object(
                paired_dispatch, "_queued_pending_evidence",
                return_value=["run_metadata.jsonl"]),
            mock.patch.object(
                paired_dispatch, "_worker_lease_is_held",
                return_value=False),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            self.assertEqual(
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments),
                {sample},
            )

        forged_recovery = {
            **recovery,
            "api_incident_kinds": {
                "0" * 64: (
                    "deepseek_transport_disconnect_misclassification"),
            },
        }
        with mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=forged_recovery), mock.patch.object(
                    paired_dispatch, "read_run_metadata_snapshot",
                    return_value=metadata), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=read_jsonl), mock.patch.object(
                            paired_dispatch, "_queued_pending_evidence",
                            return_value=["run_metadata.jsonl"]), \
                mock.patch.object(
                    paired_dispatch, "_worker_lease_is_held",
                    return_value=False):
            with self.assertRaisesRegex(
                    RuntimeError, "cannot prove pending sample"):
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments)

    def test_resume_accepts_exact_authorized_failed_recovered_worker(self):
        sample = "sample"
        worker_id = "worker-a"
        worker_pid = 123
        invocation_id = "invocation-a"
        assignments = [{
            "sample": sample,
            "methods": ["hybridpatch", "fullrewrite"],
        }]
        metadata = [{
            "samples": [sample],
            "status": "failed",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": invocation_id,
        }]
        dispatch_rows = [{
            "event": "worker_exit",
            "sample": sample,
            "worker_launch_id": worker_id,
            "pid": worker_pid,
            "returncode": 1,
            "disposition": "campaign_fatal",
        }]
        recovered_worker = {
            "sample": sample,
            "status": "failed",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": invocation_id,
        }
        recovery = {
            "worker_launch_ids": frozenset({worker_id}),
            "deepseek_recovered_workers": {
                sample: recovered_worker,
            },
            "dispatcher_parent_loss_workers": {},
            "api_incident_kinds": {},
        }

        def read_jsonl(path):
            if os.path.basename(path) == "dispatch_log.jsonl":
                return dispatch_rows
            if os.path.basename(path) == "api_calls.jsonl":
                return []
            raise AssertionError(path)

        patches = (
            mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=recovery),
            mock.patch.object(
                paired_dispatch, "read_run_metadata_snapshot",
                return_value=metadata),
            mock.patch.object(
                paired_dispatch, "_read_jsonl", side_effect=read_jsonl),
            mock.patch.object(
                paired_dispatch, "_queued_pending_evidence",
                return_value=["run_metadata.jsonl"]),
            mock.patch.object(
                paired_dispatch, "_worker_lease_is_held",
                return_value=False),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            self.assertEqual(
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments),
                {sample},
            )

        forged = {
            **recovery,
            "deepseek_recovered_workers": {
                sample: {
                    **recovered_worker,
                    "invocation_id": "forged",
                },
            },
        }
        with mock.patch.object(
                paired_dispatch, "campaign_recovery_incident_evidence",
                return_value=forged), mock.patch.object(
                    paired_dispatch, "read_run_metadata_snapshot",
                    return_value=metadata), mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=read_jsonl), mock.patch.object(
                            paired_dispatch, "_queued_pending_evidence",
                            return_value=["run_metadata.jsonl"]), \
                mock.patch.object(
                    paired_dispatch, "_worker_lease_is_held",
                    return_value=False):
            with self.assertRaisesRegex(
                    RuntimeError, "recovered worker identity is invalid"):
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    "unused", assignments)

    def test_deepseek_resume_consumes_recovered_before_pristine_audit(self):
        assignment = {
            "sample": "sample",
            "methods": ["hybridpatch", "fullrewrite"],
        }
        recovery = {
            "provider_access_retry_authorizations": {},
            "provider_access_resume_samples": frozenset(),
            "worker_launch_ids": frozenset({"worker-a"}),
        }

        def allow_recovered(
                _out_dir, _assignments, *, provenance_out=None):
            provenance_out["sample"] = paired_dispatch.PENDING_NEVER_STARTED
            return {"sample"}

        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.object(
                    paired_dispatch, "campaign_recovery_incident_evidence",
                    return_value=recovery), mock.patch.object(
                        paired_dispatch, "_latest_sample_outcomes",
                        return_value={}), mock.patch.object(
                            paired_dispatch,
                            "_verified_deepseek_resume_missing_samples_"
                            "with_provenance",
                            side_effect=allow_recovered), mock.patch.object(
                                paired_dispatch,
                                "_verify_queued_pending_samples") as pristine:
                selected, authorizations = (
                    paired_dispatch._select_invocation_assignments(
                        out_dir,
                        [assignment],
                        resume=True,
                        target_round_trips=10,
                        allow_pristine_pending=True,
                        allow_deepseek_resume=True,
                    )
                )
        self.assertEqual(
            selected,
            [paired_dispatch._mark_pending_provenance_assignment(
                assignment, paired_dispatch.PENDING_NEVER_STARTED)],
        )
        self.assertEqual(authorizations, {})
        pristine.assert_not_called()

    def test_deepseek_recovered_worker_scope_is_exact_and_isolated(self):
        worker = {
            "sample": "sample",
            "status": "failed",
            "worker_launch_id": "worker-a",
            "worker_pid": 123,
            "invocation_id": "invocation-a",
        }
        self.assertTrue(
            run_meta._deepseek_recovered_worker_scope_is_valid(
                [worker], ["worker-a"], ["sample"], []))
        self.assertFalse(
            run_meta._deepseek_recovered_worker_scope_is_valid(
                [{**worker, "worker_pid": True}],
                ["worker-a"],
                ["sample"],
                [],
            ))
        self.assertFalse(
            run_meta._deepseek_recovered_worker_scope_is_valid(
                [
                    worker,
                    {
                        **worker,
                        "worker_launch_id": "worker-b",
                        "invocation_id": "invocation-b",
                    },
                ],
                ["worker-a", "worker-b"],
                ["sample", "sample-b"],
                [],
            ))

        authorization = {
            "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
            "incident_api_rows": [],
            "incident_attempt_rows": [],
            "incident_transport_sidecars": [],
            "recovered_worker_launch_ids": ["worker-a"],
            "deepseek_recovered_workers": [worker],
            "preauthorization_worker_launch_ids": [],
            "provider_access_retry_authorizations": [],
            "provider_access_resume_samples": [],
            "authorization_id": "authorization-a",
        }
        run_meta.campaign_recovery_incident_evidence.cache_clear()
        try:
            with mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=authorization):
                evidence = run_meta.campaign_recovery_incident_evidence(
                    "unused")
        finally:
            run_meta.campaign_recovery_incident_evidence.cache_clear()
        self.assertEqual(evidence["deepseek_recovered_workers"], {})

    def test_failed_retry_audit_validates_every_attempt_and_budget_step(self):
        attempts = [
            {
                "attempt_index": 1,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 503,
                "stream_complete": False,
                "generation_delta_seen": False,
                "retry_budget_consumed": False,
                "retry_budget_attempt_index": 0,
            },
            *[
                {
                    "attempt_index": index,
                    "status": "retryable_error",
                    "error_type": "incomplete_stream",
                    "http_status": 200,
                    "stream_complete": False,
                    "generation_delta_seen": True,
                    "retry_budget_consumed": True,
                    "retry_budget_attempt_index": index - 1,
                }
                for index in range(2, 5)
            ],
        ]
        row = {
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "classification": "provider/API failure",
            "error_type": "incomplete_stream",
            "http_status": 200,
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "provider_called": True,
            "stream_complete": False,
            "response_replayed": False,
            "count_as_method_failure": False,
            "transport_attempts": attempts,
            "http_attempts_used": 4,
            "retry_count": 3,
            "failed_attempt_count": 4,
            "max_retries": 3,
        }
        self.assertTrue(paired_dispatch._deepseek_failed_retry_row(row))

        malformed = copy.deepcopy(row)
        malformed["transport_attempts"][1][
            "retry_budget_attempt_index"] = 0
        self.assertFalse(
            paired_dispatch._deepseek_failed_retry_row(malformed))
        malformed = copy.deepcopy(row)
        malformed["transport_attempts"][2]["attempt_index"] = 99
        self.assertFalse(
            paired_dispatch._deepseek_failed_retry_row(malformed))

    def test_historical_infrastructure_outcomes_remain_auditable(self):
        def outcome(sample, request_id, worker_id):
            return {
                "sample": sample,
                "status": "infrastructure_incomplete",
                "worker_launch_id": worker_id,
                "worker_pid": 123,
                "methods": ["hybridpatch", "fullrewrite"],
                "classification": "provider/API failure",
                "method_phase": None,
                "request_id": request_id,
            }

        first = outcome("sample-a", "call-a1", "worker-a1")
        second = outcome("sample-b", "call-b1", "worker-b1")
        third = outcome("sample-b", "call-b2", "worker-b2")
        rows = [
            first,
            {
                "sample": "sample-a",
                "status": "finished",
            },
            second,
            third,
        ]
        config = {
            "samples": ["sample-a", "sample-b"],
            "method_set": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 10,
        }

        def verified(_out_dir, _sample, _item, row, **kwargs):
            self.assertFalse(kwargs["require_current_progress"])
            return {
                "request_id": row["request_id"],
                "worker_launch_id": row["worker_launch_id"],
                "worker_pid": row["worker_pid"],
            }

        def incident(_out_dir, row, _evidence, _api, _committed):
            return {row["request_id"]}

        with mock.patch.object(
                paired_dispatch, "_latest_sample_outcomes"), \
                mock.patch.object(
                    paired_dispatch, "read_sample_outcomes",
                    return_value=rows), \
                mock.patch.object(
                    paired_dispatch,
                    "_verified_deepseek_infrastructure_incomplete",
                    side_effect=verified) as verify, mock.patch.object(
                        paired_dispatch,
                        "_verified_deepseek_uncommitted_incident_request_ids",
                        side_effect=incident) as verify_incident:
            request_ids = (
                paired_dispatch
                ._verified_deepseek_infrastructure_request_ids(
                    "unused", config, api_snapshot=[],
                    committed_call_ids=set())
            )
        self.assertEqual(
            request_ids, {"call-a1", "call-b1", "call-b2"})
        self.assertEqual(verify.call_count, 3)
        self.assertEqual(verify_incident.call_count, 3)

        forged = copy.deepcopy(first)
        forged["classification"] = "runner_exception"
        with mock.patch.object(
                paired_dispatch, "_latest_sample_outcomes"), \
                mock.patch.object(
                    paired_dispatch, "read_sample_outcomes",
                    return_value=[forged]):
            with self.assertRaisesRegex(
                    RuntimeError, "invalid DeepSeek infrastructure outcome"):
                paired_dispatch._verified_deepseek_infrastructure_request_ids(
                    "unused", config)

    def test_formal_runner_requires_opencode_zen_and_high(self):
        with mock.patch.dict(
                os.environ,
                {
                    "OPENAI_BASE_URL": "https://opencode.ai/zen/go/v1",
                    "AZURE_OPENAI_API_KEY": "",
                    "AZURE_OPENAI_ENDPOINT": "",
                },
                clear=False):
            experiment_runner._require_formal_opencode_transport(
                "deepseek-v4-flash", "high")
            experiment_runner._require_formal_opencode_transport(
                "t-deepseek-v4-flash", "high")
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport(
                    "deepseek-v4-flash", "medium")
        with mock.patch.dict(
                os.environ,
                {"OPENAI_BASE_URL": "https://api.deepseek.com"},
                clear=False):
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport(
                    "deepseek-v4-flash", "high")

    def test_multisample_deepseek_runner_requires_dispatch_manifest(self):
        with tempfile.TemporaryDirectory() as out_dir:
            experiment_runner._require_formal_dispatch_environment(
                out_dir, ["sample"], "deepseek-v4-flash")
            with self.assertRaisesRegex(
                    RuntimeError, "require.*paired dispatcher manifest"):
                experiment_runner._require_formal_dispatch_environment(
                    out_dir, ["sample-a", "sample-b"],
                    "t-deepseek-v4-flash")
