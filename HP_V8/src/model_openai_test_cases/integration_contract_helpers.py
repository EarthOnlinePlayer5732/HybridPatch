"""Mixin slice for test_model_openai.IntegrationContractHelpersMixin."""

from .support import *


class IntegrationContractHelpersMixin:
    @staticmethod
    def _failed_attempt_end_fields(*, generation_delta_seen=True,
                                   error_type="incomplete_stream"):
        return {
            "status": "retryable_error",
            "error_type": error_type,
            "stream_complete": False,
            "message_stop_seen": False,
            "final_usage_seen": False,
            "generation_delta_seen": generation_delta_seen,
            "content_blocks_started": 1 if generation_delta_seen else 0,
            "content_blocks_stopped": 0,
            "content_blocks_balanced": not generation_delta_seen,
            "terminal_sequence_valid": False,
        }

    @staticmethod
    def _successful_attempt_end_fields():
        return {
            "status": "success",
            "stream_complete": True,
            "stop_reason": "end_turn",
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "content_blocks_started": 1,
            "content_blocks_stopped": 1,
            "content_blocks_balanced": True,
            "terminal_sequence_valid": True,
        }

    def _append_exhausted_response_generation(
            self, recorder, semantic_call_id, call_id,
            request_fingerprint, attempt_indices, *,
            parent_semantic_call_id=None,
            call_kind="hybridpatch_primary"):
        generation = int(semantic_call_id.rsplit("/g", 1)[1])
        parent_field = (
            {"parent_semantic_call_id": parent_semantic_call_id}
            if parent_semantic_call_id is not None else {}
        )
        recorder._append_ledger(
            semantic_call_id, "semantic_request", call_id=call_id,
            call_kind=call_kind, request_fingerprint=request_fingerprint,
            **parent_field)

        for local_index, attempt_index in enumerate(attempt_indices, 1):
            recorder._append_ledger(
                semantic_call_id, "attempt_start",
                attempt_index=attempt_index, call_id=call_id,
                call_kind=call_kind,
                attempt_kind=(
                    "transport_initial"
                    if generation == 0 and local_index == 1
                    else "transport_retry"
                    if generation == 0
                    else "transport_recovery_initial"
                    if local_index == 1
                    else "transport_recovery_retry"),
                request_fingerprint=request_fingerprint, **parent_field)
            recorder._append_ledger(
                semantic_call_id, "generation_progress",
                attempt_index=attempt_index, call_id=call_id,
                delta_type="text_delta", **parent_field)
            recorder._append_ledger(
                semantic_call_id, "attempt_end",
                attempt_index=attempt_index, call_id=call_id,
                **self._failed_attempt_end_fields(), **parent_field)
            recorder._append_ledger(
                semantic_call_id, "attempt_budget",
                attempt_index=attempt_index, call_id=call_id,
                budget_class="response_slot",
                response_slots_used=local_index,
                transient_failure_count=0, **parent_field)
        recorder._append_ledger(
            semantic_call_id, "call_failed", call_id=call_id,
            status="provider_failure", error_type="incomplete_stream",
            response_slots_used=len(attempt_indices),
            transient_failure_count=0,
            http_attempts_used=attempt_indices[-1],
            attempt_index=attempt_indices[-1],
            request_fingerprint=request_fingerprint, **parent_field)

    def _append_successful_response_generation(
            self, recorder, semantic_call_id, call_id,
            request_fingerprint, attempt_index, *,
            parent_semantic_call_id=None,
            call_kind="hybridpatch_primary"):
        generation = int(semantic_call_id.rsplit("/g", 1)[1])
        parent_field = (
            {"parent_semantic_call_id": parent_semantic_call_id}
            if parent_semantic_call_id is not None else {}
        )
        recorder._append_ledger(
            semantic_call_id, "semantic_request", call_id=call_id,
            call_kind=call_kind, request_fingerprint=request_fingerprint,
            **parent_field)
        recorder._append_ledger(
            semantic_call_id, "attempt_start", call_id=call_id,
            attempt_index=attempt_index, call_kind=call_kind,
            attempt_kind=(
                "transport_initial" if generation == 0
                else "transport_recovery_initial"),
            request_fingerprint=request_fingerprint, **parent_field)
        recorder._append_ledger(
            semantic_call_id, "generation_progress", call_id=call_id,
            attempt_index=attempt_index, delta_type="text_delta",
            **parent_field)
        recorder._append_ledger(
            semantic_call_id, "attempt_end", call_id=call_id,
            attempt_index=attempt_index,
            **self._successful_attempt_end_fields(), **parent_field)
        recorder._append_ledger(
            semantic_call_id, "response_committed", call_id=call_id,
            attempt_index=attempt_index, response_slots_used=1,
            transient_failure_count=0, http_attempts_used=attempt_index,
            request_fingerprint=request_fingerprint, **parent_field)

    @staticmethod
    def _write_active_inspection_fixture(
            out_dir, *, sample="sample", method="hybridpatch",
            worker_id="worker-a"):
        plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
        utils_relay_plan.save_relay_task_plan(plan_path, ["target"])
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": "1" * 40,
            "config": {
                "samples": [sample],
                "method_set": [method],
                "num_round_trips": 1,
            },
            "task_plans": {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": ["target"],
                },
            },
        }
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "dispatch_log.jsonl"), {
                "event": "launch", "worker_launch_id": worker_id,
                "sample": sample, "pid": os.getpid(),
            })
        paired_dispatch._write_active_worker_set(
            out_dir, manifest, [{
                "worker_launch_id": worker_id, "sample": sample,
            }])
        return manifest

    def _write_infrastructure_fixture(
            self, out_dir, sample="sample-a", worker_id="worker-a",
            worker_pid=101, methods=None):
        methods = list(methods or ["hybridpatch", "fullrewrite"])
        step_id = f"hybridpatch/{sample}/rt01/forward"
        semantic_root = f"{step_id}/hybridpatch_primary"
        semantic = f"{semantic_root}/g000"
        request_id = f"failed-{sample}"
        fingerprint = f"fingerprint-{sample}"
        invocation = f"invocation-{sample}"
        api_row = {
            "schema": "anchorpatch.api_call/4",
            "sample": sample, "method": "hybridpatch",
            "rt_index": 1, "direction": "forward",
            "call_kind": "hybridpatch_primary",
            "step_id": step_id,
            "semantic_root_id": semantic_root,
            "semantic_call_id": semantic, "request_id": request_id,
            "generation_index": 0,
            "parent_semantic_call_id": None,
            "worker_launch_id": worker_id, "worker_pid": worker_pid,
            "provider_called": True, "response_replayed": False,
            "replayed_from_call_id": None,
            "transport_revision": "opencode_anthropic_sdk/4",
            "transport_resume_policy": (
                "exact_payload_new_semantic_call/1"),
            "transport_recovery_index": 0,
            "request_fingerprint": fingerprint,
            "classification": "provider/API failure",
            "count_as_method_failure": False,
            "error_type": "incomplete_stream",
            "max_response_slots": 2, "response_slots_used": 2,
            "max_transient_failures": 3,
            "transient_failure_count": 0,
            "http_attempts_used": 2,
        }
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "api_calls.jsonl"), api_row)
        for event, fields in (
            ("semantic_request", {
                "call_id": request_id, "call_kind": "hybridpatch_primary",
                "request_fingerprint": fingerprint,
            }),
            ("attempt_start", {
                "call_id": request_id, "attempt_index": 1,
                "call_kind": "hybridpatch_primary",
                "attempt_kind": "transport_initial",
                "request_fingerprint": fingerprint,
            }),
            ("generation_progress", {
                "call_id": request_id, "attempt_index": 1,
                "delta_type": "text_delta",
            }),
            ("attempt_end", {
                "call_id": request_id, "attempt_index": 1,
                **self._failed_attempt_end_fields(),
            }),
            ("attempt_budget", {
                "call_id": request_id, "attempt_index": 1,
                "budget_class": "response_slot",
                "response_slots_used": 1,
                "transient_failure_count": 0,
            }),
            ("attempt_start", {
                "call_id": request_id, "attempt_index": 2,
                "call_kind": "hybridpatch_primary",
                "attempt_kind": "transport_retry",
                "request_fingerprint": fingerprint,
            }),
            ("generation_progress", {
                "call_id": request_id, "attempt_index": 2,
                "delta_type": "text_delta",
            }),
            ("attempt_end", {
                "call_id": request_id, "attempt_index": 2,
                **self._failed_attempt_end_fields(),
            }),
            ("attempt_budget", {
                "call_id": request_id, "attempt_index": 2,
                "budget_class": "response_slot",
                "response_slots_used": 2,
                "transient_failure_count": 0,
            }),
            ("call_failed", {
                "call_id": request_id, "attempt_index": 2,
                "status": "provider_failure",
                "error_type": "incomplete_stream",
                "response_slots_used": 2,
                "transient_failure_count": 0,
                "http_attempts_used": 2,
                "request_fingerprint": fingerprint,
            }),
        ):
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"), {
                    "schema": "anchorpatch.api_attempt/4",
                    "step_id": step_id,
                    "semantic_root_id": semantic_root,
                    "semantic_call_id": semantic,
                    "generation_index": 0,
                    "parent_semantic_call_id": None,
                    "worker_launch_id": worker_id,
                    "event": event, **fields,
                })
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "run_metadata.jsonl"), {
                "schema": "anchorpatch.run_metadata/3",
                "invocation_id": invocation,
                "worker_launch_id": worker_id, "worker_pid": worker_pid,
                "samples": [sample], "status": "infrastructure_incomplete",
                "transport_resume_authorization": None,
            })
        progress = {
            method: {"completed_round_trips": 0, "committed_rows": 0}
            for method in methods
        }
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "sample_outcomes.jsonl"), {
                "schema": "anchorpatch.sample_outcome/1",
                "created_at": "2026-07-18T00:00:00+08:00",
                "sample": sample, "status": "infrastructure_incomplete",
                "worker_launch_id": worker_id, "worker_pid": worker_pid,
                "invocation_id": invocation, "methods": methods,
                "method": "hybridpatch", "rt_index": 1,
                "direction": "forward",
                "call_kind": "hybridpatch_primary",
                "semantic_root_id": semantic_root,
                "semantic_call_id": semantic, "request_id": request_id,
                "generation_index": 0,
                "parent_semantic_call_id": None,
                "request_fingerprint": fingerprint,
                "error_type": "incomplete_stream",
                "classification": "provider/API failure",
                "response_slots_used": 2,
                "transient_failure_count": 0,
                "http_attempts_used": 2,
                "next_attempt_index": 3,
                "transport_recovery_index": 0,
                "checkpoint_progress": progress,
            })
        return {
            "semantic_root_id": semantic_root,
            "semantic_call_id": semantic,
            "generation_index": 0,
            "request_fingerprint": fingerprint,
            "next_attempt_index": 3,
            "progress": progress,
        }

    def _write_finished_fixture(self, out_dir, sample, methods):
        progress = {}
        for method in methods:
            method_dir = os.path.join(out_dir, method)
            os.makedirs(method_dir, exist_ok=True)
            rows = []
            for direction in ("forward", "backward"):
                rows.append({
                    "sample_id": sample, "method": method,
                    "round_trip_num": 1,
                    "round_trip_direction": direction,
                })
            with open(
                os.path.join(method_dir, f"{sample}.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")
            run_meta.write_json_atomic(
                os.path.join(method_dir, f"{sample}.ckpt.json"),
                {"completed_round_trips": 1})
            progress[method] = {
                "completed_round_trips": 1, "committed_rows": 2}
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "sample_outcomes.jsonl"), {
                "schema": "anchorpatch.sample_outcome/1",
                "created_at": "2026-07-18T00:00:01+08:00",
                "sample": sample, "status": "finished",
                "worker_launch_id": f"worker-{sample}",
                "worker_pid": 202,
                "invocation_id": f"invocation-{sample}",
                "methods": list(methods),
                "checkpoint_progress": progress,
            })

    @staticmethod
    def _api_call_fixture_row(*, root, exact_id, request_id, fingerprint,
                              worker_id, worker_pid, parent=None,
                              failure=False, http_attempts=1):
        method, sample, rt_segment, direction, call_kind = root.split("/")
        generation = int(exact_id.rsplit("/g", 1)[1])
        return {
            "schema": "anchorpatch.api_call/4",
            "sample": sample, "method": method,
            "rt_index": int(rt_segment[2:]), "direction": direction,
            "call_kind": call_kind,
            "step_id": "/".join(root.split("/")[:4]),
            "semantic_root_id": root,
            "semantic_call_id": exact_id,
            "generation_index": generation,
            "parent_semantic_call_id": parent,
            "request_id": request_id,
            "worker_launch_id": worker_id, "worker_pid": worker_pid,
            "provider_called": True, "response_replayed": False,
            "replayed_from_call_id": None,
            "transport_revision": "opencode_anthropic_sdk/4",
            "transport_resume_policy": "exact_payload_new_semantic_call/1",
            "transport_recovery_index": generation,
            "request_fingerprint": fingerprint,
            "classification": "provider/API failure" if failure else None,
            "count_as_method_failure": False,
            "error_type": "incomplete_stream" if failure else None,
            "max_response_slots": 2,
            "response_slots_used": 2 if failure else 1,
            "max_transient_failures": 3,
            "transient_failure_count": 0,
            "http_attempts_used": http_attempts,
        }

    @staticmethod
    def _write_success_journal(out_dir, *, root, exact_id, request_id,
                               fingerprint, parent=None, http_attempts=1):
        generation = int(exact_id.rsplit("/g", 1)[1])
        digest = hashlib.sha256(exact_id.encode("utf-8")).hexdigest()[:24]
        os.makedirs(os.path.join(out_dir, "api_journal"), exist_ok=True)
        run_meta.write_json_atomic(
            os.path.join(out_dir, "api_journal", f"{digest}.response.json"), {
                "schema": "anchorpatch.api_response_journal/4",
                "semantic_root_id": root,
                "semantic_call_id": exact_id,
                "generation_index": generation,
                "parent_semantic_call_id": parent,
                "call_id": request_id,
                "request_fingerprint": fingerprint,
                "result": {
                    "message": "complete",
                    "stream_complete": True,
                    "stop_reason": "end_turn",
                    "input_tokens": 5, "output_tokens": 1,
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "call_kind": root.split("/")[-1],
                    "semantic_root_id": root,
                    "semantic_call_id": exact_id,
                    "generation_index": generation,
                    "parent_semantic_call_id": parent,
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": http_attempts,
                },
            })

    @staticmethod
    def _write_completed_method_prefix(out_dir, sample, method,
                                       completed_round_trips):
        if completed_round_trips <= 0:
            return
        method_dir = os.path.join(out_dir, method)
        os.makedirs(method_dir, exist_ok=True)
        rows = []
        for rt_index in range(1, completed_round_trips + 1):
            for direction in ("forward", "backward"):
                rows.append({
                    "sample_id": sample, "method": method,
                    "round_trip_num": rt_index,
                    "round_trip_direction": direction,
                })
        with open(
            os.path.join(method_dir, f"{sample}.jsonl"),
            "w", encoding="utf-8",
        ) as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        run_meta.write_json_atomic(
            os.path.join(method_dir, f"{sample}.ckpt.json"),
            {"completed_round_trips": completed_round_trips},
        )
