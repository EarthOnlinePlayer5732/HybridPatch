"""Mixin slice for test_model_openai.IntegrationContractGroup07Mixin."""

from .support import *


class IntegrationContractGroup07Mixin:
    def test_api_log_schema_v4_and_secret_redaction(self):
        secret = "unit-test-secret-key"
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {"OPENCODE_API_KEY": secret, "OPENCODE_TRANSPORT": "anthropic_sdk_v2"},
            clear=False,
        ):
            def fake_generate(*_args, **kwargs):
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_start", "attempt_index": 1,
                    "attempt_kind": "transport_initial",
                    "error": f"must redact {secret}",
                })
                kwargs["_raw_event_sink"]({
                    "record_type": "sdk_stream_event", "attempt_index": 1,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": 1,
                        **self._successful_attempt_end_fields(),
                    },
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn", "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
                    "input_tokens": 5, "output_tokens": 1,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                    "transport_attempts": [{"attempt_index": 1, "status": "success"}],
                    "call_kind": "key_probe", "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3", fake_generate
            )
            recorder.set_step(1, "forward", "target")
            out = recorder.generate(
                [{"role": "user", "content": "Hello"}], model="minimax-m3",
                call_kind="key_probe", thinking_mode="adaptive",
            )
            with open(os.path.join(out_dir, "api_calls.jsonl"), encoding="utf-8") as handle:
                record = json.loads(handle.readline())
            self.assertEqual(record["schema"], "anchorpatch.api_call/4")
            self.assertEqual(record["call_kind"], "key_probe")
            self.assertTrue(record["stream_complete"])
            self.assertEqual(len(record["transport_attempts"]), 1)
            self.assertIn("api_call_id", out)
            for root, _dirs, files in os.walk(out_dir):
                for name in files:
                    with open(os.path.join(root, name), encoding="utf-8") as handle:
                        text = handle.read()
                    self.assertNotIn(secret, text)

    def test_failed_api_log_keeps_effective_runtime_config(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {"OPENCODE_API_KEY": "unit-test-key", "OPENCODE_TRANSPORT": "anthropic_sdk_v2"},
            clear=False,
        ):
            last_error = RuntimeError("peer closed connection (incomplete chunked read)")
            terminal = model_openai.OpenCodeTransportError(
                "failed transport",
                attempts=[{
                    "attempt_index": 1, "status": "retryable_error",
                    "error_type": "transport_disconnect", "stream_complete": False,
                }],
                last_error=last_error,
            )

            def fail_generate(*_args, **_kwargs):
                raise terminal

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3", fail_generate
            )
            recorder.set_step(1, "forward", "target")
            with self.assertRaises(model_openai.OpenCodeTransportError):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}], model="minimax-m3",
                    max_tokens=None, thinking_mode="adaptive",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["schema"], "anchorpatch.api_call/4")
            self.assertEqual(record["transport_revision"], "opencode_anthropic_sdk/4")
            self.assertEqual(record["max_tokens"], 131072)
            self.assertEqual(record["thinking_mode"], "adaptive")

    def test_deepseek_failed_attempt_body_persists_in_api_row_and_sidecar(self):
        secret = "unit-test-key"
        sanitized_attempt = {
            "attempt_index": 1,
            "status": "retryable_error",
            "error_type": "server_error",
            "http_status": 502,
            "stream_complete": False,
            "retry_budget_consumed": True,
            "retry_budget_attempt_index": 3,
            "error_body": {
                "error_name": "origin_bad_gateway",
                "detail": "<redacted-key>",
            },
        }
        raw_attempt = copy.deepcopy(sanitized_attempt)
        raw_attempt["error_body"]["detail"] = secret
        last_error = model_openai._HTTPStatusError(
            502, json.dumps(raw_attempt["error_body"]))
        terminal = model_openai.OpenAICompatibleTransportError(
            "failed transport",
            attempts=[sanitized_attempt],
            last_error=last_error,
        )

        def fail_generate(*_args, **kwargs):
            sink = kwargs["_raw_event_sink"]
            sink({
                "record_type": "attempt_start",
                "attempt_index": 1,
            })
            sink({
                "record_type": "attempt_end",
                "attempt_index": 1,
                "attempt": raw_attempt,
            })
            raise terminal

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": secret,
                "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
            },
            clear=False,
        ):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fail_generate)
            recorder.set_step(1, "forward", "target")
            with self.assertRaises(
                    model_openai.OpenAICompatibleTransportError):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model=paired_dispatch.DEEPSEEK_MODEL,
                    max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    reasoning_effort="high",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(
                record["transport_revision"],
                paired_dispatch.DEEPSEEK_TRANSPORT_REVISION)
            self.assertEqual(
                record["transport_attempts"][0]["error_body"],
                sanitized_attempt["error_body"])
            sidecar = record["raw_sse_saved_path"]
            self.assertTrue(os.path.isfile(sidecar))
            with open(sidecar, encoding="utf-8") as handle:
                sidecar_text = handle.read()
            self.assertIn("origin_bad_gateway", sidecar_text)
            self.assertIn("<redacted-key>", sidecar_text)
            self.assertNotIn(secret, sidecar_text)
            with open(
                os.path.join(out_dir, "api_calls.jsonl"),
                encoding="utf-8",
            ) as handle:
                self.assertNotIn(secret, handle.read())

    def test_deepseek_compact_sidecar_is_bounded_linked_and_not_duplicated(self):
        stream_sha = "a" * 64
        usage = {
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 14,
        }
        attempt = {
            "attempt_index": 1,
            "status": "success",
            "error_type": None,
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "terminal_sequence_valid": True,
            "finish_reason": "stop",
            "stream_event_count": 50_003,
            "stream_event_canonical_bytes": 8_000_000,
            "stream_event_sha256": stream_sha,
            "text_delta_count": 1,
            "text_delta_utf8_bytes": 13,
            "reasoning_delta_count": 50_000,
            "reasoning_delta_utf8_bytes": 7_000_000,
            "tool_delta_count": 0,
            "tool_delta_utf8_bytes": 0,
        }

        def fake_generate(*_args, **kwargs):
            sink = kwargs["_raw_event_sink"]
            sink({
                "record_type": "attempt_start",
                "attempt_index": 1,
                "attempt_kind": "openai_compatible_initial",
            })
            sink({
                "record_type": "stream_checkpoint",
                "attempt_index": 1,
                "checkpoint": "first_chunk",
                "stream_event_count": 1,
            })
            sink({
                "record_type": "stream_checkpoint",
                "attempt_index": 1,
                "checkpoint": "generation_started",
                "stream_event_count": 1,
                "text_delta_seen": False,
                "thinking_delta_seen": True,
                "tool_delta_seen": False,
            })
            sink({
                "record_type": "stream_checkpoint",
                "attempt_index": 1,
                "checkpoint": "finish_seen",
                "stream_event_count": 50_002,
                "finish_reason": "stop",
            })
            sink({
                "record_type": "stream_checkpoint",
                "attempt_index": 1,
                "checkpoint": "usage_seen",
                "stream_event_count": 50_003,
                "usage": usage,
            })
            sink({
                "record_type": "stream_summary",
                "attempt_index": 1,
                **{
                    key: value for key, value in attempt.items()
                    if key not in {
                        "attempt_index", "status", "error_type",
                        "http_status", "stream_complete",
                    }
                },
                "usage": usage,
            })
            sink({
                "record_type": "attempt_end",
                "attempt_index": 1,
                "attempt": attempt,
            })
            return {
                "message": "VISIBLE_FINAL",
                "http_status": 200,
                "stream_complete": True,
                "finish_reason": "stop",
                "stop_reason": "stop",
                "response_classification": "normal",
                "prompt_tokens": 10,
                "completion_tokens": 4,
                "total_tokens": 14,
                "input_tokens": 10,
                "output_tokens": 4,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
                "transport_attempts": [attempt],
                "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
                "transport_revision": paired_dispatch.DEEPSEEK_TRANSPORT_REVISION,
                "reasoning_effort": "high",
                "provider": "opencode_zen",
                "base_url": paired_dispatch.DEEPSEEK_BASE_URL,
                "request_url": (
                    paired_dispatch.DEEPSEEK_BASE_URL + "/chat/completions"),
                "_raw_request_body": {"stream": True},
            }

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": "unit-test-key",
                "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
            },
            clear=False,
        ):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fake_generate)
            recorder.set_step(1, "forward", "target")
            recorder.generate(
                [{"role": "user", "content": "Hello"}],
                model=paired_dispatch.DEEPSEEK_MODEL,
                max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                reasoning_effort="high",
                call_kind="hybridpatch_primary",
            )
            record = next(iter(recorder.records_by_id.values()))
            sidecar = record["raw_sse_saved_path"]

            self.assertTrue(sidecar.endswith(".transport.jsonl"))
            self.assertLess(os.path.getsize(sidecar), 32 * 1024)
            self.assertEqual(record["transport_sidecar_record_count"], 8)
            self.assertTrue(record["transport_sidecar_sha256"])
            self.assertTrue(paired_dispatch._valid_deepseek_transport_sidecar(
                out_dir, record))
            with open(sidecar, encoding="utf-8") as handle:
                sidecar_text = handle.read()
            self.assertNotIn("VISIBLE_FINAL", sidecar_text)
            self.assertFalse(any(
                name.endswith(".sse.jsonl")
                for _root, _dirs, files in os.walk(out_dir)
                for name in files
            ))
            tampered = dict(record, transport_sidecar_size_bytes=1)
            self.assertFalse(paired_dispatch._valid_deepseek_transport_sidecar(
                out_dir, tampered))

            with open(sidecar, encoding="utf-8") as handle:
                compact_rows = [json.loads(line) for line in handle if line.strip()]
            summary_row = next(
                row for row in compact_rows
                if row.get("record_type") == "stream_summary")
            summary_row["raw_chunk"] = "must-not-be-accepted"
            with open(sidecar, "w", encoding="utf-8", newline="") as handle:
                for row in compact_rows:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            unexpected_field = dict(record)
            unexpected_field.update(run_meta._transport_sidecar_facts(sidecar))
            self.assertFalse(paired_dispatch._valid_deepseek_transport_sidecar(
                out_dir, unexpected_field))

            compact_rows = [
                json.loads(line) for line in sidecar_text.splitlines()
                if line.strip()
            ]
            usage_checkpoint = next(
                row for row in compact_rows
                if row.get("record_type") == "stream_checkpoint"
                and row.get("checkpoint") == "usage_seen")
            usage_checkpoint["stream_event_count"] = 0
            with open(sidecar, "w", encoding="utf-8", newline="") as handle:
                for row in compact_rows:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            regressed_checkpoint = dict(record)
            regressed_checkpoint.update(
                run_meta._transport_sidecar_facts(sidecar))
            self.assertFalse(paired_dispatch._valid_deepseek_transport_sidecar(
                out_dir, regressed_checkpoint))

            compact_rows = [
                json.loads(line) for line in sidecar_text.splitlines()
                if line.strip()
            ]
            next(row for row in compact_rows
                 if row.get("record_type") == "stream_summary")[
                     "finish_reason"] = None
            next(row for row in compact_rows
                 if row.get("record_type") == "stream_checkpoint"
                 and row.get("checkpoint") == "finish_seen")[
                     "finish_reason"] = None
            end_attempt = next(
                row for row in compact_rows
                if row.get("record_type") == "attempt_end")["attempt"]
            end_attempt["finish_reason"] = None
            with open(sidecar, "w", encoding="utf-8", newline="") as handle:
                for row in compact_rows:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            blank_finish = dict(record)
            blank_finish["transport_attempts"] = [end_attempt]
            blank_finish.update(run_meta._transport_sidecar_facts(sidecar))
            self.assertFalse(paired_dispatch._valid_deepseek_transport_sidecar(
                out_dir, blank_finish))

    def test_deepseek_sidecar_write_failure_aborts_before_success_commit(self):
        real_open = open

        class FailingTransportFile:
            def __init__(self, inner):
                self._inner = inner

            @property
            def closed(self):
                return self._inner.closed

            def write(self, _value):
                raise OSError("transport sidecar write failed")

            def flush(self):
                self._inner.flush()

            def close(self):
                self._inner.close()

        def selective_open(path, *args, **kwargs):
            inner = real_open(path, *args, **kwargs)
            if str(path).endswith(".transport.jsonl"):
                return FailingTransportFile(inner)
            return inner

        def fake_generate(*_args, **kwargs):
            model_openai._emit_transport_event(
                kwargs["_raw_event_sink"],
                {
                    "record_type": "attempt_start",
                    "attempt_index": 1,
                    "attempt_kind": "openai_compatible_initial",
                },
            )
            self.fail("provider call continued after sidecar write failure")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "unit-test-key",
                    "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
                },
                clear=False), mock.patch.object(
                    run_meta, "open", side_effect=selective_open,
                    create=True):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fake_generate)
            recorder.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    OSError, "transport sidecar write failed"):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model=paired_dispatch.DEEPSEEK_MODEL,
                    max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    reasoning_effort="high",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["classification"], "runner_exception")
            self.assertFalse(record["provider_called"])
            self.assertFalse(record["stream_complete"])
            self.assertIsNone(record["raw_sse_saved_path"])

    def test_deepseek_sidecar_fsync_failure_aborts_before_success_commit(self):
        real_open = open
        real_fsync = run_meta.os.fsync
        transport_fd = 987654321

        class FailingFsyncTransportFile:
            def __init__(self, inner):
                self._inner = inner

            @property
            def closed(self):
                return self._inner.closed

            def write(self, value):
                return self._inner.write(value)

            def flush(self):
                return self._inner.flush()

            def fileno(self):
                return transport_fd

            def close(self):
                self._inner.close()

        def selective_open(path, *args, **kwargs):
            inner = real_open(path, *args, **kwargs)
            if str(path).endswith(".transport.jsonl"):
                return FailingFsyncTransportFile(inner)
            return inner

        def selective_fsync(fd):
            if fd == transport_fd:
                raise OSError("transport sidecar fsync failed")
            return real_fsync(fd)

        def fake_generate(*_args, **kwargs):
            sink = kwargs["_raw_event_sink"]
            model_openai._emit_transport_event(sink, {
                "record_type": "attempt_start",
                "attempt_index": 1,
            })
            model_openai._emit_transport_event(sink, {
                "record_type": "attempt_end",
                "attempt_index": 1,
                "attempt": {
                    "attempt_index": 1,
                    "status": "success",
                },
            })
            self.fail("provider result continued after sidecar fsync failure")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "unit-test-key",
                    "OPENAI_BASE_URL": paired_dispatch.DEEPSEEK_BASE_URL,
                },
                clear=False), mock.patch.object(
                    run_meta, "open", side_effect=selective_open,
                    create=True), mock.patch.object(
                        run_meta.os, "fsync", side_effect=selective_fsync):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, fake_generate)
            recorder.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    OSError, "transport sidecar fsync failed"):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model=paired_dispatch.DEEPSEEK_MODEL,
                    max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                    reasoning_effort="high",
                    call_kind="hybridpatch_primary",
                )
            record = next(iter(recorder.records_by_id.values()))
            self.assertEqual(record["classification"], "runner_exception")
            self.assertTrue(record["provider_called"])
            self.assertFalse(record["stream_complete"])

    def test_run_metadata_rejects_unversioned_resume(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_TRANSPORT": "anthropic_sdk_v2"}, clear=False
        ):
            path = os.path.join(out_dir, "run_metadata.jsonl")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"schema": "anchorpatch.run_metadata/1"}) + "\n")
            with self.assertRaises(RuntimeError):
                run_meta.append_run_metadata(
                    out_dir,
                    command="test",
                    samples=["sample"],
                    methods=["hybridpatch"],
                    num_round_trips=1,
                    seed=42,
                    model="minimax-m3",
                    distractor=False,
                    max_tokens=None,
                    printing=False,
                )
