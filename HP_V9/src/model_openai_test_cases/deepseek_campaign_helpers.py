"""Mixin slice for test_model_openai.DeepSeekOpenCodeCampaignHelpersMixin."""

from .support import *


class DeepSeekOpenCodeCampaignHelpersMixin:
    @staticmethod
    def _write_inspection_fixture(out_dir):
        sample = "sample"
        worker_id = "worker-a"
        worker_pid = os.getpid()
        methods = ["hybridpatch", "fullrewrite"]
        plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
        states = [
            f"state-{index}"
            for index in range(
                1, paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS + 1)
        ]
        utils_relay_plan.save_relay_task_plan(plan_path, states)
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": "1" * 40,
            "config": {
                "campaign_role": "deepseek_full234",
                "samples": [sample],
                "method_set": methods,
                "num_round_trips": (
                    paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "provider": "opencode_zen",
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "transport_resume_policy": None,
                "openai_base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "slots_per_key": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY),
                "key_count": paired_dispatch.DEEPSEEK_FULL_KEY_COUNT,
                "max_worker_count": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY
                    * paired_dispatch.DEEPSEEK_FULL_KEY_COUNT),
            },
            "task_plans": {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": paired_dispatch._sha256(plan_path),
                    "forward_state_sequence": states,
                },
            },
        }
        run_meta.write_json_atomic(
            os.path.join(out_dir, "dispatch_manifest.json"), manifest)
        paired_dispatch._write_active_worker_set(
            out_dir, manifest, [{
                "worker_launch_id": worker_id,
                "sample": sample,
            }])
        dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
        run_meta.append_jsonl_locked(dispatch_log, {
            "event": "launch",
            "worker_launch_id": worker_id,
            "sample": sample,
            "pid": worker_pid,
        })
        run_meta.append_jsonl_locked(dispatch_log, {
            "event": "worker_authorized",
            "worker_launch_id": worker_id,
            "sample": sample,
            "worker_pid": worker_pid,
        })
        run_meta.append_jsonl_locked(
            os.path.join(out_dir, "run_metadata.jsonl"), {
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": "invocation-a",
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "samples": [sample],
                "methods": methods,
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
                "status": "running",
                "finished_at": None,
            })
        for method in methods:
            os.makedirs(os.path.join(out_dir, method), exist_ok=True)
        return manifest, sample, worker_id, worker_pid

    @staticmethod
    def _write_dispatcher_parent_loss_fixture(
            out_dir, stages, *, record_stop=True, methods=None,
            metadata_mode="legacy"):
        if metadata_mode not in {"legacy", "event"}:
            raise ValueError("metadata_mode must be legacy or event")
        commit = "1" * 40
        fingerprint = {
            "model_openai.py": "2" * 64,
            "paired_campaign_dispatch.py": "3" * 64,
            "run_meta.py": "4" * 64,
        }
        methods = methods or ["hybridpatch", "fullrewrite"]
        samples = [
            f"parent-loss-{index}" for index in range(len(stages))
        ]
        dispatcher_pid = 43210
        dispatcher_instance_id = "dispatcher-instance-a"
        manifest = {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": commit,
            "git_tree_state": "clean",
            "code_fingerprint": fingerprint,
            "config": {
                "campaign_role": "deepseek_full234",
                "samples": samples,
                "method_set": methods,
                "num_round_trips": (
                    paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "provider": "opencode_zen",
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": (
                    paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                "transport_resume_policy": None,
                "openai_base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "slots_per_key": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY),
                "key_count": paired_dispatch.DEEPSEEK_FULL_KEY_COUNT,
                "max_worker_count": (
                    paired_dispatch.DEEPSEEK_FULL_SLOTS_PER_KEY
                    * paired_dispatch.DEEPSEEK_FULL_KEY_COUNT),
            },
            "assignments": [
                {
                    "sample": sample,
                    "key_label": f"KEY_{index + 1}",
                    "methods": methods,
                    "console_log": (
                        f"dispatch_logs/{sample}.console.log"),
                }
                for index, sample in enumerate(samples)
            ],
            "task_plans": {},
        }
        if metadata_mode == "event":
            manifest["config"]["run_metadata_storage"] = (
                run_meta.RUN_METADATA_STORAGE_EVENT_V1)
        run_meta.write_json_atomic(
            os.path.join(out_dir, "dispatch_manifest.json"), manifest)
        workers = []
        details = []
        for index, stage in enumerate(stages):
            sample = f"parent-loss-{index}"
            worker_id = f"worker-parent-loss-{index}"
            worker_pid = (
                None
                if stage == "registered_prelaunch"
                else os.getpid() + index
            )
            item = {
                "sample": sample,
                "worker_launch_id": worker_id,
                "worker_pid": worker_pid,
                "invocation_id": (
                    f"invocation-parent-loss-{index}"
                    if stage == "running" else None
                ),
                "stage": stage,
            }
            details.append(item)
            workers.append({
                "sample": sample,
                "worker_launch_id": worker_id,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            })
        paired_dispatch._write_active_worker_set(
            out_dir, manifest, workers)

        dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
        metadata_path = os.path.join(out_dir, "run_metadata.jsonl")
        for index, item in enumerate(details):
            intent = {
                "event": "launch_intent",
                "sample": item["sample"],
                "key_label": f"KEY_{index + 1}",
                "methods": methods,
                "worker_launch_id": item["worker_launch_id"],
                "console_log": (
                    f"dispatch_logs/{item['sample']}.console.log"),
                "method_phase": None,
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            }
            run_meta.append_jsonl_locked(dispatch_path, intent)
            if item["stage"] == "registered_prelaunch":
                continue
            run_meta.append_jsonl_locked(dispatch_path, {
                **intent,
                "event": "launch",
                "pid": item["worker_pid"],
            })
            if item["stage"] != "running":
                continue
            run_meta.append_jsonl_locked(dispatch_path, {
                "event": "worker_authorized",
                "worker_launch_id": item["worker_launch_id"],
                "sample": item["sample"],
                "worker_pid": item["worker_pid"],
                "invocation_id": item["invocation_id"],
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
            })
            metadata_record = {
                "schema": run_meta.METADATA_SCHEMA,
                "invocation_id": item["invocation_id"],
                "worker_launch_id": item["worker_launch_id"],
                "worker_pid": item["worker_pid"],
                "dispatcher_pid": dispatcher_pid,
                "dispatcher_instance_id": dispatcher_instance_id,
                "samples": [item["sample"]],
                "methods": methods,
                "method_phase": None,
                "command": "python parent-loss-fixture",
                "out_dir": os.path.abspath(out_dir),
                "num_round_trips": (
                    paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                "seed": 42,
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "distractor": True,
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
                    "method_set": methods,
                    "num_round_trips": (
                        paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                    "seed": 42,
                    "model": paired_dispatch.DEEPSEEK_MODEL,
                    "distractor": True,
                    "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    "reasoning_effort": (
                        paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                },
                "run_git_commit": commit,
                "git_tree_state": "clean",
                "code_fingerprint": fingerprint,
                "campaign_recovery_authorization": None,
                "status": "running",
                "finished_at": None,
            }
            if metadata_mode == "event":
                started_at = "2026-07-28T20:00:00+08:00"
                metadata_record.update({
                    "created_local": started_at,
                    "invocation_started_at": started_at,
                    "invocation_finished_at": None,
                    "started_at": started_at,
                    "timezone": "Asia/Singapore",
                })
                run_meta._append_run_metadata_event_unlocked(out_dir, {
                    "event": "invocation_registered",
                    "record": metadata_record,
                })
            else:
                run_meta.append_jsonl_locked(metadata_path, metadata_record)

        if record_stop:
            stop_worker = next(
                item for item in details
                if item["stage"] != "registered_prelaunch"
            )
            with mock.patch.dict(
                    os.environ,
                    {
                        "ANCHORPATCH_WORKER_LAUNCH_ID": stop_worker[
                            "worker_launch_id"],
                    },
                    clear=False):
                run_meta.record_campaign_stop_condition(
                    out_dir,
                    "dispatcher_process_lost",
                    dispatcher_pid=dispatcher_pid,
                    dispatcher_instance_id=dispatcher_instance_id,
                )
        return {
            "commit": commit,
            "fingerprint": fingerprint,
            "manifest": manifest,
            "dispatcher_pid": dispatcher_pid,
            "dispatcher_instance_id": dispatcher_instance_id,
            "workers": details,
            "methods": methods,
        }

    @staticmethod
    def _deepseek_api_row(
            out_dir, sample, worker_id, worker_pid, direction, *,
            method="fullrewrite", call_kind=None, request_id=None):
        request_id = request_id or f"call-{direction}"
        call_kind = call_kind or (
            "hybridpatch_primary"
            if method == "hybridpatch" else "fullrewrite_primary")
        raw_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.request.json")
        sidecar_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.transport.jsonl")
        run_meta.write_json_atomic(raw_path, {
            "request_body": {
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_completion_tokens": (
                    paired_dispatch.DEEPSEEK_MAX_TOKENS),
                "stream": True,
                "stream_options": {"include_usage": True},
            },
        })
        attempt = {
            "attempt_index": 1,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "response_started_http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_delta_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "thinking_delta_seen": False,
            "text_delta_seen": True,
            "tool_delta_seen": False,
            "content_blocks_started": 0,
            "content_blocks_stopped": 0,
            "content_blocks_balanced": True,
            "terminal_sequence_valid": True,
            "finish_reason": "stop",
            "stream_event_count": 1,
            "stream_event_canonical_bytes": 100,
            "stream_event_sha256": "a" * 64,
            "text_delta_count": 1,
            "text_delta_utf8_bytes": 7,
            "reasoning_delta_count": 0,
            "reasoning_delta_utf8_bytes": 0,
            "tool_delta_count": 0,
            "tool_delta_utf8_bytes": 0,
        }
        usage = {
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
        }
        for event in (
                {
                    "transport_event_schema": "anchorpatch.transport_event/2",
                    "record_type": "transport_header",
                    "transport_revision": (
                        paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
                    "call_id": request_id,
                    "worker_launch_id": worker_id,
                    "worker_pid": worker_pid,
                    "sample": sample,
                    "method": method,
                    "rt_index": 1,
                    "direction": direction,
                },
                {
                    "record_type": "attempt_start",
                    "attempt_index": 1,
                    "attempt_kind": "openai_compatible_initial",
                },
                {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "first_chunk",
                    "stream_event_count": 1,
                },
                {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "generation_started",
                    "stream_event_count": 1,
                    "text_delta_seen": True,
                    "thinking_delta_seen": False,
                    "tool_delta_seen": False,
                },
                {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "finish_seen",
                    "stream_event_count": 1,
                    "finish_reason": "stop",
                },
                {
                    "record_type": "stream_checkpoint",
                    "attempt_index": 1,
                    "checkpoint": "usage_seen",
                    "stream_event_count": 1,
                    "usage": usage,
                },
                {
                    "record_type": "stream_summary",
                    "attempt_index": 1,
                    **{
                        key: value for key, value in attempt.items()
                        if key not in {
                            "attempt_index", "status", "error_type",
                            "http_status", "response_started_http_status",
                            "stream_complete", "message_delta_seen",
                            "thinking_delta_seen", "text_delta_seen",
                            "tool_delta_seen", "content_blocks_started",
                            "content_blocks_stopped",
                            "content_blocks_balanced",
                        }
                    },
                    "usage": usage,
                },
                {
                    "record_type": "attempt_end",
                    "attempt_index": 1,
                    "attempt": attempt,
                }):
            run_meta.append_jsonl_locked(sidecar_path, event)
        row = {
            "schema": paired_dispatch.API_CALL_SCHEMA,
            "request_id": request_id,
            "semantic_call_id": f"semantic-{direction}",
            "sample": sample,
            "method": method,
            "rt_index": 1,
            "direction": direction,
            "call_kind": call_kind,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
            "request_url": (
                f"{paired_dispatch.DEEPSEEK_BASE_URL}/chat/completions"),
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_resume_policy": None,
            "reasoning_effort": (
                paired_dispatch.DEEPSEEK_REASONING_EFFORT),
            "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
            "provider_called": True,
            "response_replayed": False,
            "generation_index": 0,
            "classification": None,
            "response_classification": "normal",
            "stream_complete": True,
            "http_status": 200,
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
            "input_tokens": 1,
            "output_tokens": 1,
            "transport_attempts": [attempt],
            "http_attempts_used": 1,
            "retry_count": 0,
            "failed_attempt_count": 0,
            "max_retries": 3,
            "raw_request_saved_path": raw_path,
            "raw_sse_saved_path": sidecar_path,
        }
        row.update(run_meta._transport_sidecar_facts(sidecar_path))
        return row

    @staticmethod
    def _deepseek_failed_api_row(
            out_dir, sample, worker_id, worker_pid, *, method,
            direction, call_kind, request_id):
        attempts = [
            {
                "attempt_index": index,
                "status": "retryable_error",
                "error_type": "server_error",
                "http_status": 502,
                "response_started_http_status": None,
                "stream_complete": False,
                "message_start_seen": False,
                "message_delta_seen": False,
                "message_stop_seen": False,
                "final_usage_seen": False,
                "generation_delta_seen": False,
                "thinking_delta_seen": False,
                "text_delta_seen": False,
                "tool_delta_seen": False,
                "content_blocks_started": 0,
                "content_blocks_stopped": 0,
                "content_blocks_balanced": False,
                "terminal_sequence_valid": False,
                "retry_budget_consumed": True,
                "retry_budget_attempt_index": index,
                "finish_reason": None,
                "stream_event_count": 0,
                "stream_event_canonical_bytes": 0,
                "stream_event_sha256": hashlib.sha256(b"").hexdigest(),
                "text_delta_count": 0,
                "text_delta_utf8_bytes": 0,
                "reasoning_delta_count": 0,
                "reasoning_delta_utf8_bytes": 0,
                "tool_delta_count": 0,
                "tool_delta_utf8_bytes": 0,
            }
            for index in range(1, 4)
        ]
        raw_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.request.json")
        sidecar_path = os.path.join(
            out_dir, "api_raw", f"{request_id}.transport.jsonl")
        run_meta.write_json_atomic(raw_path, {
            "request_body": {
                "model": paired_dispatch.DEEPSEEK_MODEL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "max_completion_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
                "stream": True,
                "stream_options": {"include_usage": True},
            },
        })
        run_meta.append_jsonl_locked(sidecar_path, {
            "transport_event_schema": "anchorpatch.transport_event/2",
            "record_type": "transport_header",
            "transport_revision": paired_dispatch.DEEPSEEK_TRANSPORT_REVISION,
            "call_id": request_id,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "sample": sample,
            "method": method,
            "rt_index": 1,
            "direction": direction,
        })
        for attempt in attempts:
            run_meta.append_jsonl_locked(sidecar_path, {
                "record_type": "attempt_start",
                "attempt_index": attempt["attempt_index"],
                "attempt_kind": (
                    "openai_compatible_initial"
                    if attempt["attempt_index"] == 1
                    else "openai_compatible_retry"
                ),
            })
            run_meta.append_jsonl_locked(sidecar_path, {
                "record_type": "stream_summary",
                "attempt_index": attempt["attempt_index"],
                **{
                    key: value for key, value in attempt.items()
                    if key not in {
                        "attempt_index", "status", "error_type",
                        "http_status", "response_started_http_status",
                        "stream_complete", "message_delta_seen",
                        "thinking_delta_seen", "text_delta_seen",
                        "tool_delta_seen", "content_blocks_started",
                        "content_blocks_stopped", "content_blocks_balanced",
                        "retry_budget_consumed",
                        "retry_budget_attempt_index",
                    }
                },
                "usage": None,
            })
            run_meta.append_jsonl_locked(sidecar_path, {
                "record_type": "attempt_end",
                "attempt_index": attempt["attempt_index"],
                "attempt": attempt,
            })
        row = {
            "schema": paired_dispatch.API_CALL_SCHEMA,
            "request_id": request_id,
            "semantic_call_id": f"semantic-{request_id}",
            "sample": sample,
            "method": method,
            "rt_index": 1,
            "direction": direction,
            "call_kind": call_kind,
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "model": paired_dispatch.DEEPSEEK_MODEL,
            "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
            "request_url": (
                f"{paired_dispatch.DEEPSEEK_BASE_URL}/chat/completions"),
            "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
            "transport_revision": (
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION),
            "transport_resume_policy": None,
            "reasoning_effort": (
                paired_dispatch.DEEPSEEK_REASONING_EFFORT),
            "max_tokens": paired_dispatch.DEEPSEEK_MAX_TOKENS,
            "provider_called": True,
            "response_replayed": False,
            "generation_index": 0,
            "classification": "provider/API failure",
            "response_classification": None,
            "error_type": "server_error",
            "http_status": 502,
            "raw_content_length": 0,
            "stream_complete": False,
            "count_as_method_failure": False,
            "input_tokens": None,
            "output_tokens": None,
            "transport_attempts": attempts,
            "http_attempts_used": 3,
            "retry_count": 2,
            "failed_attempt_count": 3,
            "max_retries": 3,
            "raw_request_saved_path": raw_path,
            "raw_sse_saved_path": sidecar_path,
        }
        row.update(run_meta._transport_sidecar_facts(sidecar_path))
        return row

    @staticmethod
    def _deepseek_infrastructure_outcome(
            sample, worker_id, worker_pid, api_row):
        methods = ["hybridpatch", "fullrewrite"]
        return {
            "schema": run_meta.SAMPLE_OUTCOME_SCHEMA,
            "created_at": datetime.now().astimezone().isoformat(),
            "sample": sample,
            "status": "infrastructure_incomplete",
            "worker_launch_id": worker_id,
            "worker_pid": worker_pid,
            "invocation_id": "invocation-a",
            "methods": methods,
            "method": api_row["method"],
            "rt_index": api_row["rt_index"],
            "direction": api_row["direction"],
            "call_kind": api_row["call_kind"],
            "semantic_root_id": api_row.get("semantic_root_id"),
            "semantic_call_id": api_row["semantic_call_id"],
            "generation_index": api_row["generation_index"],
            "parent_semantic_call_id": api_row.get(
                "parent_semantic_call_id"),
            "request_fingerprint": api_row.get("request_fingerprint"),
            "request_id": api_row["request_id"],
            "error_type": api_row["error_type"],
            "classification": api_row["classification"],
            "response_slots_used": api_row.get("response_slots_used"),
            "transient_failure_count": api_row.get(
                "transient_failure_count"),
            "http_attempts_used": api_row["http_attempts_used"],
            "next_attempt_index": api_row["http_attempts_used"] + 1,
            "transport_recovery_index": api_row.get(
                "transport_recovery_index"),
            "checkpoint_progress": {
                method: {
                    "completed_round_trips": 0,
                    "committed_rows": 0,
                }
                for method in methods
            },
            "evidence": {
                "api_calls": "api_calls.jsonl",
                "attempt_ledger": "api_attempt_ledger.jsonl",
                "run_metadata": "run_metadata.jsonl",
            },
        }

    @staticmethod
    def _fullrewrite_result_rows(sample, call_ids):
        return [
            {
                "sample_id": sample,
                "method": "fullrewrite",
                "round_trip_num": 1,
                "round_trip_direction": direction,
                "api_call_ids": [call_id],
                "model_name": paired_dispatch.DEEPSEEK_MODEL,
                "reasoning_effort": (
                    paired_dispatch.DEEPSEEK_REASONING_EFFORT),
                "evaluation": (
                    {"score": 1.0} if direction == "backward" else {}),
            }
            for direction, call_id in zip(
                ("forward", "backward"), call_ids)
        ]
