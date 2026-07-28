"""Mixin slice for test_model_openai.IntegrationContractGroup06Mixin."""

from .support import *


class IntegrationContractGroup06Mixin:
    def test_response_journal_replays_without_second_provider_post(self):
        calls = []
        result = {
            "message": "Hello", "http_status": 200, "stream_complete": True,
            "finish_reason": "end_turn", "stop_reason": "end_turn",
            "response_classification": "normal", "prompt_tokens": 5,
            "completion_tokens": 1, "total_tokens": 6, "input_tokens": 5,
            "output_tokens": 1, "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0, "transport_attempts": [],
            "call_kind": "fullrewrite_primary", "thinking_mode": "adaptive",
            "transport": "anthropic_sdk_v2",
            "transport_revision": "opencode_anthropic_sdk/4",
            "transport_resume_policy": (
                "exact_payload_new_semantic_call/1"),
            "max_response_slots": 2, "response_slots_used": 1,
            "max_transient_failures": 3, "transient_failure_count": 0,
            "http_attempts_used": 1,
        }

        def fake_generate(*_args, **kwargs):
            calls.append(1)
            kwargs["_raw_event_sink"]({
                "record_type": "attempt_start", "attempt_index": 1,
                "attempt_kind": "transport_initial",
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
            kwargs["_response_commit_sink"](result)
            return dict(result)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            first = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3", fake_generate
            )
            first.set_step(1, "forward", "target")
            first.generate([{"role": "user", "content": "Hello"}],
                           model="minimax-m3", call_kind="fullrewrite_primary")

            ledger_path = os.path.join(
                out_dir, "api_attempt_ledger.jsonl")
            ledger = run_meta._read_jsonl_records_with_retry(ledger_path)
            self.assertEqual(
                [row.get("event") for row in ledger].count(
                    "response_committed"), 1)
            with open(ledger_path, "w", encoding="utf-8") as handle:
                for row in ledger:
                    if row.get("event") != "response_committed":
                        handle.write(json.dumps(row) + "\n")

            second = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None, "minimax-m3",
                mock.Mock(side_effect=AssertionError("provider must not be called")),
            )
            second.set_step(1, "forward", "target")
            replay = second.generate([{"role": "user", "content": "Hello"}],
                                     model="minimax-m3", call_kind="fullrewrite_primary")
            self.assertEqual(len(calls), 1)
            self.assertTrue(replay["response_replayed"])
            records = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl")
            )
            self.assertEqual([r["provider_called"] for r in records], [True, False])
            reconciled = run_meta._read_jsonl_records_with_retry(ledger_path)
            self.assertEqual(
                [row.get("event") for row in reconciled].count(
                    "response_committed"), 1)

            journal_path = second._journal_path(
                second._semantic_ids("fullrewrite_primary")[1])
            with open(journal_path, encoding="utf-8") as handle:
                valid = json.load(handle)
            broken = dict(valid)
            broken["request_fingerprint"] = "wrong-fingerprint"
            run_meta.write_json_atomic(journal_path, broken)
            fingerprint_forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            third = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", fingerprint_forbidden)
            third.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "fingerprint differs within"):
                third.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary")
            fingerprint_forbidden.assert_not_called()

            broken = dict(valid)
            broken["schema"] = "anchorpatch.api_response_journal/3"
            run_meta.write_json_atomic(journal_path, broken)
            forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            fourth = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", forbidden)
            fourth.set_step(1, "forward", "target")
            with self.assertRaisesRegex(RuntimeError, "journal schema"):
                fourth.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary")
            forbidden.assert_not_called()

    def test_blank_stop_reason_fails_closed_in_ledger_and_journal(self):
        for stop_reason in ("", " \t "):
            with self.subTest(
                    artifact="ledger", stop_reason=repr(stop_reason)), \
                    tempfile.TemporaryDirectory() as out_dir:
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                _step, semantic = recorder._semantic_ids(
                    "hybridpatch_primary")
                call_id = "call-ledger"
                fingerprint = "fingerprint-ledger"
                recorder._append_ledger(
                    semantic, "semantic_request", call_id=call_id,
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                recorder._append_ledger(
                    semantic, "attempt_start", attempt_index=1,
                    call_id=call_id, call_kind="hybridpatch_primary",
                    attempt_kind="transport_initial",
                    request_fingerprint=fingerprint)
                recorder._append_ledger(
                    semantic, "generation_progress", attempt_index=1,
                    call_id=call_id, delta_type="text_delta")
                terminal = self._successful_attempt_end_fields()
                terminal["stop_reason"] = stop_reason
                recorder._append_ledger(
                    semantic, "attempt_end", attempt_index=1,
                    call_id=call_id, **terminal)
                with self.assertRaisesRegex(
                        RuntimeError, "incomplete successful stream"):
                    recorder._ledger_state(semantic)
                provider.assert_not_called()

            with self.subTest(
                    artifact="journal", stop_reason=repr(stop_reason)), \
                    tempfile.TemporaryDirectory() as out_dir:
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                _step, semantic = recorder._semantic_ids(
                    "hybridpatch_primary")
                lineage = run_meta._semantic_lineage_fields(semantic)
                fingerprint = "fingerprint-journal"
                result = {
                    "semantic_root_id": lineage["semantic_root_id"],
                    "semantic_call_id": semantic,
                    "generation_index": 0,
                    "parent_semantic_call_id": None,
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "call_kind": "hybridpatch_primary",
                    "stream_complete": True,
                    "stop_reason": stop_reason,
                    "message": "complete text",
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "max_response_slots": 2,
                    "max_transient_failures": 3,
                    "response_slots_used": 1,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                run_meta.write_json_atomic(
                    recorder._journal_path(semantic), {
                        "schema": run_meta.API_RESPONSE_JOURNAL_SCHEMA,
                        "semantic_root_id": lineage["semantic_root_id"],
                        "semantic_call_id": semantic,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "call_id": "call-journal",
                        "request_fingerprint": fingerprint,
                        "result": result,
                    })
                with self.assertRaisesRegex(
                        RuntimeError, "journal result is incomplete"):
                    recorder._find_lineage_journal(
                        lineage["semantic_root_id"], fingerprint)
                provider.assert_not_called()

    def test_committed_ledger_without_journal_fails_before_provider(self):
        result = {
            "message": "Hello", "http_status": 200,
            "stream_complete": True, "finish_reason": "end_turn",
            "stop_reason": "end_turn", "response_classification": "normal",
            "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6,
            "input_tokens": 5, "output_tokens": 1,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0, "transport_attempts": [],
            "call_kind": "fullrewrite_primary", "thinking_mode": "adaptive",
            "transport": "anthropic_sdk_v2",
            "transport_revision": "opencode_anthropic_sdk/4",
            "max_response_slots": 2, "response_slots_used": 1,
            "max_transient_failures": 3, "transient_failure_count": 0,
            "http_attempts_used": 1,
        }

        def fake_generate(*_args, **kwargs):
            kwargs["_raw_event_sink"]({
                "record_type": "attempt_start", "attempt_index": 1,
                "attempt_kind": "transport_initial",
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
            kwargs["_response_commit_sink"](result)
            return dict(result)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            first = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", fake_generate)
            first.set_step(1, "forward", "target")
            first.generate(
                [{"role": "user", "content": "Hello"}],
                model="minimax-m3", call_kind="fullrewrite_primary")
            journal_path = first._journal_path(
                first._semantic_ids("fullrewrite_primary")[1])
            os.remove(journal_path)

            forbidden = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            second = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", forbidden)
            second.set_step(1, "forward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "committed response.*journal is missing"):
                second.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3", call_kind="fullrewrite_primary")
            forbidden.assert_not_called()

    def test_dangling_generation_attempt_fails_closed_before_provider(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            first = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                provider)
            first.set_step(2, "backward", "target")
            messages = [{"role": "user", "content": "Hello"}]
            generate_kwargs = {
                "model": "minimax-m3",
                "call_kind": "hybridpatch_primary",
            }
            fingerprint = run_meta._semantic_request_fingerprint(
                (messages,), generate_kwargs, "minimax-m3",
                "hybridpatch_primary")
            _step, semantic = first._semantic_ids("hybridpatch_primary")
            first._append_ledger(
                semantic, "semantic_request", call_id="crashed-call",
                call_kind="hybridpatch_primary",
                request_fingerprint=fingerprint)
            first._append_ledger(
                semantic, "attempt_start", attempt_index=1,
                call_id="crashed-call", call_kind="hybridpatch_primary",
                attempt_kind="transport_initial",
                request_fingerprint=fingerprint)
            second = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None, "minimax-m3",
                provider)
            second.set_step(2, "backward", "target")
            with self.assertRaisesRegex(
                    RuntimeError, "unclosed HTTP attempt"):
                second.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
            provider.assert_not_called()

    def test_malformed_or_v3_attempt_ledger_fails_closed_before_provider(self):
        for payload, expected in (
            ('{"schema":', "invalid JSONL"),
            (json.dumps({
                "schema": "anchorpatch.api_attempt/3",
                "semantic_call_id": (
                    "hybridpatch/sample/rt01/forward/hybridpatch_primary"),
                "event": "semantic_request",
            }), "schema differs"),
            (json.dumps({
                "schema": "anchorpatch.api_attempt/4",
                "step_id": "hybridpatch/sample/rt01/forward",
                "semantic_root_id": (
                    "hybridpatch/sample/rt01/forward/hybridpatch_primary"),
                "semantic_call_id": (
                    "hybridpatch/sample/rt01/forward/"
                    "hybridpatch_primary/g000"),
                "generation_index": 0,
                "parent_semantic_call_id": None,
                "event": "transport_resume",
            }), "unknown event|retired same-ID"),
        ):
            with self.subTest(expected=expected), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ,
                        {"OPENCODE_API_KEY": "unit-test-key"}, clear=False):
                with open(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    handle.write(payload + "\n")
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                with self.assertRaisesRegex(RuntimeError, expected):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3",
                        call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_lineage_gap_and_fingerprint_mismatch_fail_before_provider(self):
        cases = (
            ("gap", 2, "same", "semantic lineage generation order"),
            ("fingerprint", 1, "different",
             "request fingerprint is inconsistent"),
        )
        for label, next_generation, next_fingerprint, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ,
                        {"OPENCODE_API_KEY": "unit-test-key"}, clear=False):
                provider = mock.Mock(
                    side_effect=AssertionError("provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                step_id, root = recorder._semantic_root(
                    "hybridpatch_primary")
                generation_zero = f"{root}/g000"
                recorder._append_ledger(
                    generation_zero, "semantic_request", call_id="call-0",
                    call_kind="hybridpatch_primary",
                    request_fingerprint="same")
                for attempt_index in (1, 2):
                    recorder._append_ledger(
                        generation_zero, "attempt_start",
                        attempt_index=attempt_index, call_id="call-0",
                        call_kind="hybridpatch_primary",
                        attempt_kind=(
                            "transport_initial" if attempt_index == 1
                            else "transport_retry"),
                        request_fingerprint="same")
                    recorder._append_ledger(
                        generation_zero, "generation_progress",
                        attempt_index=attempt_index, call_id="call-0",
                        delta_type="text_delta")
                    recorder._append_ledger(
                        generation_zero, "attempt_end",
                        attempt_index=attempt_index, call_id="call-0",
                        **self._failed_attempt_end_fields())
                    recorder._append_ledger(
                        generation_zero, "attempt_budget",
                        attempt_index=attempt_index, call_id="call-0",
                        budget_class="response_slot",
                        response_slots_used=attempt_index,
                        transient_failure_count=0)
                recorder._append_ledger(
                    generation_zero, "call_failed", call_id="call-0",
                    attempt_index=2, status="provider_failure",
                    error_type="incomplete_stream", response_slots_used=2,
                    transient_failure_count=0, http_attempts_used=2,
                    request_fingerprint="same")
                exact = f"{root}/g{next_generation:03d}"
                parent = f"{root}/g{next_generation - 1:03d}"
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"), {
                        "schema": "anchorpatch.api_attempt/4",
                        "step_id": step_id,
                        "semantic_root_id": root,
                        "semantic_call_id": exact,
                        "generation_index": next_generation,
                        "parent_semantic_call_id": parent,
                        "worker_launch_id": recorder.worker_launch_id,
                        "event": "semantic_request",
                        "call_id": f"call-{next_generation}",
                        "call_kind": "hybridpatch_primary",
                        "request_fingerprint": next_fingerprint,
                    })
                with self.assertRaisesRegex(RuntimeError, expected):
                    recorder.generate(
                        [{"role": "user", "content": "Hello"}],
                        model="minimax-m3",
                        call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_forged_transport_budget_counters_fail_before_provider(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            provider = mock.Mock(
                side_effect=AssertionError("provider must not be called"))
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", provider)
            recorder.set_step(1, "forward", "target")
            _step, semantic = recorder._semantic_ids(
                "hybridpatch_primary")
            fingerprint = "fingerprint"
            recorder._append_ledger(
                semantic, "semantic_request", call_id="call-0",
                call_kind="hybridpatch_primary",
                request_fingerprint=fingerprint)
            recorder._append_ledger(
                semantic, "attempt_start", attempt_index=1,
                call_id="call-0", call_kind="hybridpatch_primary",
                attempt_kind="transport_initial",
                request_fingerprint=fingerprint)
            recorder._append_ledger(
                semantic, "attempt_end", attempt_index=1,
                call_id="call-0",
                **self._failed_attempt_end_fields(
                    generation_delta_seen=False,
                    error_type="connection_error"))
            recorder._append_ledger(
                semantic, "attempt_budget", attempt_index=1,
                call_id="call-0", budget_class="response_slot",
                response_slots_used=1, transient_failure_count=0)
            with self.assertRaisesRegex(
                    RuntimeError, "budget counters disagree"):
                recorder.generate(
                    [{"role": "user", "content": "Hello"}],
                    model="minimax-m3",
                    call_kind="hybridpatch_primary")
            provider.assert_not_called()

    def test_request_fingerprint_includes_positional_generate_arguments(self):
        base = ([{"role": "user", "content": "one"}],)
        changed = ([{"role": "user", "content": "two"}],)
        kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        self.assertNotEqual(
            run_meta._semantic_request_fingerprint(
                base, kwargs, "minimax-m3", "hybridpatch_primary"),
            run_meta._semantic_request_fingerprint(
                changed, kwargs, "minimax-m3", "hybridpatch_primary"),
        )

    def test_future_generation_delta_and_signature_delta_restore_distinct_budgets(self):
        def state_after(delta_type):
            with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
                os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
            ):
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", mock.Mock())
                recorder.set_step(3, "forward", "target")
                messages = [{"role": "user", "content": "Hello"}]
                kwargs = {
                    "model": "minimax-m3",
                    "call_kind": "hybridpatch_primary",
                }
                fingerprint = run_meta._semantic_request_fingerprint(
                    (messages,), kwargs, "minimax-m3",
                    "hybridpatch_primary")
                _step, semantic = recorder._semantic_ids(
                    "hybridpatch_primary")
                recorder._append_ledger(
                    semantic, "semantic_request", call_id="failed-call",
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                recorder._append_ledger(
                    semantic, "attempt_start", attempt_index=1,
                    call_id="failed-call", call_kind="hybridpatch_primary",
                    attempt_kind="transport_initial",
                    request_fingerprint=fingerprint)
                generation_delta_seen = run_meta._generation_delta_type(
                    delta_type)
                if generation_delta_seen:
                    recorder._append_ledger(
                        semantic, "generation_progress", attempt_index=1,
                        call_id="failed-call", delta_type=delta_type)
                recorder._append_ledger(
                    semantic, "attempt_end", attempt_index=1,
                    call_id="failed-call",
                    **self._failed_attempt_end_fields(
                        generation_delta_seen=generation_delta_seen))
                recorder._append_ledger(
                    semantic, "attempt_budget", attempt_index=1,
                    call_id="failed-call",
                    budget_class=(
                        "response_slot" if generation_delta_seen
                        else "transient_failure"),
                    response_slots_used=int(generation_delta_seen),
                    transient_failure_count=int(not generation_delta_seen))
                return recorder._ledger_state(semantic)

        future = state_after("audio_delta")
        self.assertEqual(future["response_slots_used"], 1)
        self.assertEqual(future["transient_failure_count"], 0)
        signature = state_after("signature_delta")
        self.assertEqual(signature["response_slots_used"], 0)
        self.assertEqual(signature["transient_failure_count"], 1)

    def test_infrastructure_resume_keeps_attempts_and_uses_next_index(self):
        messages = [{"role": "user", "content": "Hello"}]
        kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), kwargs, "minimax-m3", "hybridpatch_primary")
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            prior = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", mock.Mock())
            prior.set_step(2, "backward", "target")
            _step, semantic = prior._semantic_ids("hybridpatch_primary")
            prior._append_ledger(
                semantic, "semantic_request", call_id="failed-call",
                call_kind="hybridpatch_primary",
                request_fingerprint=fingerprint)
            for attempt_index in (1, 2):
                prior._append_ledger(
                    semantic, "attempt_start", attempt_index=attempt_index,
                    call_id="failed-call", call_kind="hybridpatch_primary",
                    attempt_kind=(
                        "transport_initial" if attempt_index == 1
                        else "transport_retry"),
                    request_fingerprint=fingerprint)
                prior._append_ledger(
                    semantic, "generation_progress",
                    attempt_index=attempt_index,
                    call_id="failed-call", delta_type="thinking_delta")
                prior._append_ledger(
                    semantic, "attempt_end", attempt_index=attempt_index,
                    call_id="failed-call",
                    **self._failed_attempt_end_fields())
                prior._append_ledger(
                    semantic, "attempt_budget", attempt_index=attempt_index,
                    call_id="failed-call", budget_class="response_slot",
                    response_slots_used=attempt_index,
                    transient_failure_count=0)
            prior._append_ledger(
                semantic, "call_failed", call_id="failed-call",
                status="provider_failure", error_type="incomplete_stream",
                response_slots_used=2, transient_failure_count=0,
                http_attempts_used=2, attempt_index=2,
                recovery_index=0, request_fingerprint=fingerprint)
            observed = {}

            def resumed_generate(*_args, **inner):
                observed.update(inner["_retry_state"])
                next_index = observed["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "attempt_index": next_index,
                    "attempt_kind": "transport_recovery_initial",
                })
                inner["_raw_event_sink"]({
                    "record_type": "sdk_stream_event",
                    "attempt_index": next_index,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": next_index, "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": next_index,
                }
                inner["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": next_index,
                        **self._successful_attempt_end_fields(),
                    },
                })
                inner["_response_commit_sink"](result)
                return result

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": semantic,
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "1",
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                    fingerprint),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": "3",
            }, clear=False):
                resumed = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", resumed_generate)
                resumed.set_step(2, "backward", "target")
                out = resumed.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
            self.assertEqual(observed["response_slots_used"], 0)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 2)
            self.assertEqual(observed["generation_index"], 1)
            self.assertEqual(observed["parent_semantic_call_id"], semantic)
            self.assertEqual(out["message"], "Hello")
            self.assertEqual(out["generation_index"], 1)
            self.assertEqual(out["parent_semantic_call_id"], semantic)
            root = semantic.rsplit("/", 1)[0]
            recovered_semantic = f"{root}/g001"
            self.assertEqual(out["semantic_call_id"], recovered_semantic)
            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            attempt_indices = [
                row.get("attempt_index") for row in ledger
                if row.get("event") == "attempt_start"]
            self.assertEqual(attempt_indices, [1, 2, 3])
            self.assertNotIn(
                "transport_resume", [row.get("event") for row in ledger])
            exact_ids = {
                row["semantic_call_id"] for row in ledger
                if row.get("semantic_root_id") == root
            }
            self.assertEqual(exact_ids, {semantic, recovered_semantic})
            parent_state = resumed._ledger_state(semantic)
            recovered_state = resumed._ledger_state(recovered_semantic)
            self.assertEqual(parent_state["response_slots_used"], 2)
            self.assertEqual(parent_state["http_attempts_used"], 2)
            self.assertEqual(recovered_state["response_slots_used"], 1)
            self.assertEqual(recovered_state["http_attempts_used"], 3)
            self.assertEqual(
                recovered_state["parent_semantic_call_id"], semantic)

    def test_multiple_exact_recoveries_keep_per_generation_budgets(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", mock.Mock())
            recorder.set_step(4, "forward", "target")
            _step, generation_zero = recorder._semantic_ids(
                "hybridpatch_primary")
            generation_one_context = recorder._semantic_context(
                "hybridpatch_primary", 1,
                parent_semantic_call_id=generation_zero)
            generation_one = generation_one_context["semantic_call_id"]

            def append_exhausted_generation(
                    semantic_call_id, call_id, attempt_indices, generation):
                recorder._append_ledger(
                    semantic_call_id, "semantic_request", call_id=call_id,
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                for local_index, attempt_index in enumerate(
                        attempt_indices, 1):
                    recorder._append_ledger(
                        semantic_call_id, "attempt_start",
                        attempt_index=attempt_index, call_id=call_id,
                        call_kind="hybridpatch_primary",
                        attempt_kind=(
                            "transport_initial"
                            if generation == 0 and local_index == 1
                            else "transport_retry"
                            if generation == 0
                            else "transport_recovery_initial"
                            if local_index == 1
                            else "transport_recovery_retry"),
                        request_fingerprint=fingerprint)
                    recorder._append_ledger(
                        semantic_call_id, "generation_progress",
                        attempt_index=attempt_index, call_id=call_id,
                        delta_type="text_delta")
                    recorder._append_ledger(
                        semantic_call_id, "attempt_end",
                        attempt_index=attempt_index, call_id=call_id,
                        **self._failed_attempt_end_fields())
                    recorder._append_ledger(
                        semantic_call_id, "attempt_budget",
                        attempt_index=attempt_index, call_id=call_id,
                        budget_class="response_slot",
                        response_slots_used=local_index,
                        transient_failure_count=0)
                recorder._append_ledger(
                    semantic_call_id, "call_failed", call_id=call_id,
                    status="provider_failure",
                    error_type="incomplete_stream",
                    response_slots_used=2, transient_failure_count=0,
                    http_attempts_used=attempt_indices[-1],
                    attempt_index=attempt_indices[-1],
                    request_fingerprint=fingerprint)

            append_exhausted_generation(
                generation_zero, "failed-g000", (1, 2), 0)
            append_exhausted_generation(
                generation_one, "failed-g001", (3, 4), 1)

            observed = {}

            def generation_two_generate(*_args, **inner):
                observed.update(inner["_retry_state"])
                attempt_index = observed["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "attempt_index": attempt_index,
                    "attempt_kind": "transport_recovery_initial",
                })
                inner["_raw_event_sink"]({
                    "record_type": "sdk_stream_event",
                    "attempt_index": attempt_index,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                inner["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": attempt_index,
                        **self._successful_attempt_end_fields(),
                    },
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": attempt_index, "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": attempt_index,
                }
                inner["_response_commit_sink"](result)
                return result

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                    generation_one),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "2",
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                    fingerprint),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": "5",
            }, clear=False):
                resumed = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", generation_two_generate)
                resumed.set_step(4, "forward", "target")
                out = resumed.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")

            root = generation_zero.rsplit("/", 1)[0]
            generation_two = f"{root}/g002"
            self.assertEqual(observed["response_slots_used"], 0)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 4)
            self.assertEqual(observed["generation_index"], 2)
            self.assertEqual(
                observed["parent_semantic_call_id"], generation_one)
            self.assertEqual(out["semantic_call_id"], generation_two)
            self.assertEqual(out["generation_index"], 2)

            ledger = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"))
            self.assertEqual([
                row["attempt_index"] for row in ledger
                if row.get("event") == "attempt_start"
            ], [1, 2, 3, 4, 5])
            self.assertNotIn(
                "transport_resume", [row.get("event") for row in ledger])
            for semantic_call_id, expected_slots in (
                    (generation_zero, 2), (generation_one, 2),
                    (generation_two, 1)):
                state = resumed._ledger_state(semantic_call_id)
                self.assertEqual(
                    state["response_slots_used"], expected_slots)
                self.assertEqual(state["transient_failure_count"], 0)

    def test_committed_recovery_does_not_block_next_semantic_root(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            seed = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", mock.Mock())
            seed.set_step(1, "forward", "target")
            _step, generation_zero = seed._semantic_ids(
                "hybridpatch_primary")
            self._append_exhausted_response_generation(
                seed, generation_zero, "failed-g000", fingerprint, (1, 2))

            observed = []

            def successful_generate(*_args, **inner):
                state = dict(inner["_retry_state"])
                observed.append(state)
                attempt_index = state["http_attempts_used"] + 1
                inner["_raw_event_sink"]({
                    "record_type": "attempt_start",
                    "attempt_index": attempt_index,
                    "attempt_kind": (
                        "transport_recovery_initial"
                        if state["generation_index"] else
                        "transport_initial"),
                })
                inner["_raw_event_sink"]({
                    "record_type": "sdk_stream_event",
                    "attempt_index": attempt_index,
                    "event": {"type": "content_block_delta",
                              "delta": {"type": "text_delta"}},
                })
                inner["_raw_event_sink"]({
                    "record_type": "attempt_end", "attempt": {
                        "attempt_index": attempt_index,
                        **self._successful_attempt_end_fields(),
                    },
                })
                result = {
                    "message": "Hello", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": attempt_index,
                        "status": "success"}],
                    "call_kind": "hybridpatch_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": attempt_index,
                }
                inner["_response_commit_sink"](result)
                return result

            with mock.patch.dict(os.environ, {
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                    generation_zero),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "1",
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                    fingerprint),
                "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": "3",
            }, clear=False):
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", successful_generate)
                recorder.set_step(1, "forward", "target")
                recovered = recorder.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")
                recorder.set_step(2, "backward", "initial")
                next_root = recorder.generate(
                    messages, model="minimax-m3",
                    call_kind="hybridpatch_primary")

            self.assertEqual(recovered["generation_index"], 1)
            self.assertEqual(next_root["generation_index"], 0)
            self.assertNotEqual(
                recovered["semantic_root_id"], next_root["semantic_root_id"])
            self.assertEqual(
                [state["http_attempts_used"] for state in observed], [2, 0])

    def test_resume_authorization_mismatch_fails_before_provider(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")
        cases = (
            ("wrong-fingerprint", "3", "request fingerprint changed"),
            (fingerprint, "99", "next HTTP attempt"),
        )
        for authorized_fingerprint, next_attempt, expected in cases:
            with self.subTest(expected=expected), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ, {"OPENCODE_API_KEY": "unit-test-key"},
                        clear=False):
                seed = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", mock.Mock())
                seed.set_step(1, "forward", "target")
                _step, generation_zero = seed._semantic_ids(
                    "hybridpatch_primary")
                self._append_exhausted_response_generation(
                    seed, generation_zero, "failed-g000", fingerprint,
                    (1, 2))
                provider = mock.Mock(side_effect=AssertionError(
                    "provider must not be called"))
                with mock.patch.dict(os.environ, {
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                        generation_zero),
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "1",
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                        authorized_fingerprint),
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": (
                        next_attempt),
                }, clear=False):
                    recorder = run_meta.ApiCallRecorder(
                        out_dir, "hybridpatch", "sample", None,
                        "minimax-m3", provider)
                    recorder.set_step(1, "forward", "target")
                    with self.assertRaisesRegex(RuntimeError, expected):
                        recorder.generate(
                            messages, model="minimax-m3",
                            call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_budget_overrun_and_cross_generation_attempt_order_fail_closed(self):
        messages = [{"role": "user", "content": "Hello"}]
        generate_kwargs = {
            "model": "minimax-m3", "call_kind": "hybridpatch_primary"}
        fingerprint = run_meta._semantic_request_fingerprint(
            (messages,), generate_kwargs, "minimax-m3",
            "hybridpatch_primary")

        for label in ("extra-attempt", "cross-generation-order"):
            with self.subTest(label=label), \
                    tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.dict(
                        os.environ, {"OPENCODE_API_KEY": "unit-test-key"},
                        clear=False):
                provider = mock.Mock(side_effect=AssertionError(
                    "provider must not be called"))
                recorder = run_meta.ApiCallRecorder(
                    out_dir, "hybridpatch", "sample", None,
                    "minimax-m3", provider)
                recorder.set_step(1, "forward", "target")
                _step, generation_zero = recorder._semantic_ids(
                    "hybridpatch_primary")
                if label == "extra-attempt":
                    recorder._append_ledger(
                        generation_zero, "semantic_request",
                        call_id="over-budget",
                        call_kind="hybridpatch_primary",
                        request_fingerprint=fingerprint)
                    for attempt_index in (1, 2, 3):
                        recorder._append_ledger(
                            generation_zero, "attempt_start",
                            attempt_index=attempt_index,
                            call_id="over-budget",
                            call_kind="hybridpatch_primary",
                            attempt_kind=(
                                "transport_initial" if attempt_index == 1
                                else "transport_retry"),
                            request_fingerprint=fingerprint)
                        recorder._append_ledger(
                            generation_zero, "generation_progress",
                            attempt_index=attempt_index,
                            call_id="over-budget", delta_type="text_delta")
                        recorder._append_ledger(
                            generation_zero, "attempt_end",
                            attempt_index=attempt_index,
                            call_id="over-budget",
                            **self._failed_attempt_end_fields())
                        recorder._append_ledger(
                            generation_zero, "attempt_budget",
                            attempt_index=attempt_index,
                            call_id="over-budget",
                            budget_class="response_slot",
                            response_slots_used=attempt_index,
                            transient_failure_count=0)
                    expected = "exceeds the frozen R2/I3 budget"
                else:
                    self._append_exhausted_response_generation(
                        recorder, generation_zero, "failed-g000",
                        fingerprint, (1, 2))
                    generation_one = recorder._semantic_context(
                        "hybridpatch_primary", 1,
                        parent_semantic_call_id=generation_zero
                    )["semantic_call_id"]
                    recorder._append_ledger(
                        generation_one, "semantic_request",
                        call_id="out-of-order",
                        call_kind="hybridpatch_primary",
                        request_fingerprint=fingerprint)
                    recorder._append_ledger(
                        generation_one, "attempt_start", attempt_index=1,
                        call_id="out-of-order",
                        call_kind="hybridpatch_primary",
                        attempt_kind="transport_recovery_initial",
                        request_fingerprint=fingerprint)
                    recorder._append_ledger(
                        generation_one, "attempt_end", attempt_index=1,
                        call_id="out-of-order",
                        **self._failed_attempt_end_fields(
                            generation_delta_seen=False,
                            error_type="connection_error"))
                    recorder._append_ledger(
                        generation_one, "attempt_budget", attempt_index=1,
                        call_id="out-of-order",
                        budget_class="transient_failure",
                        response_slots_used=0,
                        transient_failure_count=1)
                    expected = "attempt indexes are duplicated/out of order"
                with self.assertRaisesRegex(RuntimeError, expected):
                    recorder.generate(
                        messages, model="minimax-m3",
                        call_kind="hybridpatch_primary")
                provider.assert_not_called()

    def test_hybrid_repair_has_independent_transport_budget(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {"OPENCODE_API_KEY": "unit-test-key"}, clear=False,
        ):
            observed = {}

            def repair_generate(*_args, **kwargs):
                observed.update(kwargs["_retry_state"])
                result = {
                    "message": "{}", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 1, "completion_tokens": 1,
                    "total_tokens": 2, "input_tokens": 1,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [],
                    "call_kind": "hybridpatch_repair",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2, "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_raw_event_sink"]({
                    "record_type": "attempt_start", "attempt_index": 1,
                    "attempt_kind": "transport_initial"})
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
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", repair_generate)
            recorder.set_step(1, "forward", "target")
            _step, primary = recorder._semantic_ids(
                "hybridpatch_primary")
            self._append_exhausted_response_generation(
                recorder, primary, "primary-failed",
                "primary-fingerprint", (1, 2))
            recorder.generate(
                [{"role": "user", "content": "repair"}],
                model="minimax-m3", call_kind="hybridpatch_repair")
            self.assertEqual(observed["response_slots_used"], 0)
            self.assertEqual(observed["transient_failure_count"], 0)
            self.assertEqual(observed["http_attempts_used"], 0)

    def test_raw_io_does_not_duplicate_critical_stream_events(self):
        with tempfile.TemporaryDirectory() as out_dir:
            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                paired_dispatch.DEEPSEEK_MODEL, mock.Mock())
            recorder.set_step(1, "forward", "target")
            meta = {
                "_raw_request_body": {"stream": True},
                "_raw_stream_events": [{"choices": [{"delta": {"content": "x"}}]}],
                "_raw_response_full": {"message": "x"},
            }

            critical = recorder._dump_raw_io(
                "critical", meta, stream_events_already_saved=True)
            fallback = recorder._dump_raw_io("fallback", meta)

            self.assertNotIn("sse", critical)
            self.assertTrue(os.path.isfile(critical["request"]))
            self.assertTrue(os.path.isfile(critical["response_full"]))
            self.assertTrue(os.path.isfile(fallback["sse"]))
            self.assertFalse(any(
                name.endswith("critical.sse.jsonl")
                for _root, _dirs, files in os.walk(out_dir)
                for name in files
            ))
