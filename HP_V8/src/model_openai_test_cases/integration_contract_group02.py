"""Mixin slice for test_model_openai.IntegrationContractGroup02Mixin."""

from .support import *


class IntegrationContractGroup02Mixin:
    def test_failure_terminal_ledger_precedes_api_row_during_active_poll(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {
                "OPENCODE_API_KEY": "unit-test-key",
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
            }, clear=False,
        ):
            manifest = self._write_active_inspection_fixture(out_dir)

            def exhausted_generate(*_args, **kwargs):
                attempts = []
                for attempt_index in (1, 2):
                    kwargs["_raw_event_sink"]({
                        "record_type": "attempt_start",
                        "attempt_index": attempt_index,
                        "attempt_kind": (
                            "transport_initial" if attempt_index == 1
                            else "transport_retry"),
                    })
                    kwargs["_raw_event_sink"]({
                        "record_type": "sdk_stream_event",
                        "attempt_index": attempt_index,
                        "event": {"type": "content_block_delta",
                                  "delta": {"type": "text_delta"}},
                    })
                    attempt = {
                        "attempt_index": attempt_index,
                        **self._failed_attempt_end_fields(),
                    }
                    kwargs["_raw_event_sink"]({
                        "record_type": "attempt_end",
                        "attempt": attempt,
                    })
                    kwargs["_raw_event_sink"]({
                        "record_type": "attempt_budget",
                        "attempt_index": attempt_index,
                        "budget_class": "response_slot",
                        "response_slots_used": attempt_index,
                        "transient_failure_count": 0,
                    })
                    attempts.append(attempt)
                raise model_openai.OpenCodeTransportError(
                    "response slots exhausted", attempts=attempts,
                    last_error=model_openai._IncompleteStreamError(
                        "incomplete_stream"))

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", exhausted_generate)
            recorder.set_step(1, "forward", "target")
            original_write = recorder._write_record
            write_entered = threading.Event()
            allow_write = threading.Event()
            worker_errors = []

            def blocked_write(record):
                write_entered.set()
                if not allow_write.wait(5):
                    raise RuntimeError("unit-test write barrier timed out")
                original_write(record)

            recorder._write_record = blocked_write

            def run_failure():
                try:
                    with mock.patch.object(
                            run_meta, "_enforce_pre_call_campaign_guards"):
                        recorder.generate(
                            [{"role": "user", "content": "Hello"}],
                            model="minimax-m3",
                            call_kind="hybridpatch_primary")
                except BaseException as exc:
                    worker_errors.append(exc)

            worker = threading.Thread(target=run_failure, daemon=True)
            worker.start()
            self.assertTrue(write_entered.wait(5), worker_errors)
            try:
                ledger = run_meta._read_jsonl_records_with_retry(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"))
                self.assertEqual(ledger[-1]["event"], "call_failed")
                self.assertFalse(os.path.exists(
                    os.path.join(out_dir, "api_calls.jsonl")))
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")):
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, active_samples={"sample"})
                self.assertEqual(inspection["errors"], [])
            finally:
                allow_write.set()
                worker.join(5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(worker_errors), 1)
            self.assertIsInstance(
                worker_errors[0], model_openai.OpenCodeTransportError)
            api_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl"))
            self.assertEqual(len(api_rows), 1)
            self.assertEqual(
                api_rows[0]["classification"], "provider/API failure")

    def test_success_terminal_holds_root_lock_through_api_row_and_active_poll(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {
                "OPENCODE_API_KEY": "unit-test-key",
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
            }, clear=False,
        ):
            manifest = self._write_active_inspection_fixture(out_dir)

            def successful_generate(*_args, **kwargs):
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
                        "attempt_index": 1, "status": "success"}],
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
                    "http_attempts_used": 1,
                }
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "hybridpatch", "sample", None,
                "minimax-m3", successful_generate)
            recorder.set_step(1, "forward", "target")
            _step, semantic_root = recorder._semantic_root(
                "hybridpatch_primary")
            lock_path = os.path.join(
                out_dir, "api_semantic_locks",
                f"{recorder._semantic_digest(semantic_root)}.lock")
            original_write = recorder._write_record
            write_entered = threading.Event()
            allow_write = threading.Event()
            worker_results = []
            worker_errors = []

            def blocked_write(record):
                write_entered.set()
                if not allow_write.wait(5):
                    raise RuntimeError("unit-test write barrier timed out")
                original_write(record)

            recorder._write_record = blocked_write

            def run_success():
                try:
                    with mock.patch.object(
                            run_meta, "_enforce_pre_call_campaign_guards"):
                        worker_results.append(recorder.generate(
                            [{"role": "user", "content": "Hello"}],
                            model="minimax-m3",
                            call_kind="hybridpatch_primary"))
                except BaseException as exc:
                    worker_errors.append(exc)

            worker = threading.Thread(target=run_success, daemon=True)
            worker.start()
            self.assertTrue(write_entered.wait(5), worker_errors)
            contender = open(lock_path, "a+", encoding="utf-8")
            try:
                with self.assertRaises(portalocker.exceptions.LockException):
                    portalocker.lock(
                        contender,
                        portalocker.LOCK_EX | portalocker.LOCK_NB)
                ledger = run_meta._read_jsonl_records_with_retry(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"))
                self.assertEqual(ledger[-1]["event"], "response_committed")
                self.assertFalse(os.path.exists(
                    os.path.join(out_dir, "api_calls.jsonl")))
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")):
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, active_samples={"sample"})
                self.assertEqual(inspection["errors"], [])
            finally:
                allow_write.set()
                worker.join(5)
                contender.close()
            self.assertFalse(worker.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertEqual(len(worker_results), 1)
            with open(lock_path, "a+", encoding="utf-8") as post_commit:
                portalocker.lock(
                    post_commit,
                    portalocker.LOCK_EX | portalocker.LOCK_NB)
                portalocker.unlock(post_commit)
            api_rows = run_meta._read_jsonl_records_with_retry(
                os.path.join(out_dir, "api_calls.jsonl"))
            self.assertEqual(len(api_rows), 1)
            self.assertIsNone(api_rows[0]["classification"])

    def test_live_inspection_avoids_cross_file_torn_snapshot(self):
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.dict(
            os.environ, {
                "OPENCODE_API_KEY": "unit-test-key",
                "ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a",
            }, clear=False,
        ):
            manifest = self._write_active_inspection_fixture(
                out_dir, method="fullrewrite")

            def successful_generate(*_args, **kwargs):
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
                result = {
                    "message": "complete", "http_status": 200,
                    "stream_complete": True, "finish_reason": "end_turn",
                    "stop_reason": "end_turn",
                    "response_classification": "normal",
                    "prompt_tokens": 5, "completion_tokens": 1,
                    "total_tokens": 6, "input_tokens": 5,
                    "output_tokens": 1, "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "transport_attempts": [{
                        "attempt_index": 1, "status": "success"}],
                    "call_kind": "fullrewrite_primary",
                    "thinking_mode": "adaptive",
                    "transport": "anthropic_sdk_v2",
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "max_response_slots": 2,
                    "response_slots_used": 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": 1,
                }
                kwargs["_response_commit_sink"](result)
                return result

            recorder = run_meta.ApiCallRecorder(
                out_dir, "fullrewrite", "sample", None,
                "minimax-m3", successful_generate)
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
                            active_samples={"sample"}))
                except BaseException as exc:
                    inspection_errors.append(exc)

            with mock.patch.object(
                    run_meta, "_enforce_pre_call_campaign_guards"), \
                    mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        paired_dispatch, "_read_jsonl",
                        side_effect=blocked_read_jsonl):
                inspector = threading.Thread(
                    target=inspect_live_campaign, daemon=True)
                inspector.start()
                self.assertTrue(api_snapshot_taken.wait(5))
                try:
                    for direction in ("forward", "backward"):
                        recorder.set_step(1, direction, "target")
                        recorder.generate(
                            [{"role": "user", "content": "Hello"}],
                            model="minimax-m3",
                            call_kind="fullrewrite_primary")
                    rows = [
                        {
                            "sample_id": "sample",
                            "method": "fullrewrite",
                            "round_trip_num": 1,
                            "round_trip_direction": "forward",
                            "evaluation": {},
                        },
                        {
                            "sample_id": "sample",
                            "method": "fullrewrite",
                            "round_trip_num": 1,
                            "round_trip_direction": "backward",
                            "evaluation": {"score": 1.0},
                        },
                    ]
                    commit = run_meta.append_relay_rows_and_checkpoint(
                        os.path.join(
                            out_dir, "fullrewrite", "sample.jsonl"),
                        os.path.join(
                            out_dir, "fullrewrite", "sample.ckpt.json"),
                        rows, {"completed_round_trips": 1},
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
                out_dir, "fullrewrite", "sample.jsonl"))), 2)

    def test_formal_recovery_authorization_state_machine(self):
        def build_fixture(out_dir, variant):
            sample = "sample"
            method = "hybridpatch"
            old_worker, new_worker = "worker-old", "worker-new"
            old_pid, new_pid = 101, 202
            old_invocation, new_invocation = "inv-old", "inv-new"
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(
                plan_path, ["target", "next"])
            plan_sha = paired_dispatch._sha256(plan_path)
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": [sample], "method_set": [method],
                    "num_round_trips": 2,
                },
                "task_plans": {
                    sample: {
                        "path": os.path.basename(plan_path),
                        "sha256": plan_sha,
                        "forward_state_sequence": ["target", "next"],
                    },
                },
            }
            forward_root = (
                "hybridpatch/sample/rt01/forward/hybridpatch_primary")
            backward_root = (
                "hybridpatch/sample/rt01/backward/hybridpatch_primary")
            next_root = (
                "hybridpatch/sample/rt02/forward/hybridpatch_primary")
            forward_g000 = f"{forward_root}/g000"
            backward_g000 = f"{backward_root}/g000"
            backward_g001 = f"{backward_root}/g001"
            next_g000 = f"{next_root}/g000"
            forward_fp = "fingerprint-forward"
            backward_fp = "fingerprint-backward"
            next_fp = "fingerprint-next"
            resume = {
                "parent_semantic_call_id": backward_g000,
                "semantic_root_id": backward_root,
                "semantic_call_id": backward_g001,
                "generation_index": 1,
                "request_fingerprint": backward_fp,
                "next_attempt_index": 3,
                "prior_worker_launch_id": old_worker,
                "prior_invocation_id": old_invocation,
            }

            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            for row in (
                {"event": "launch", "worker_launch_id": old_worker,
                 "sample": sample, "pid": old_pid},
                {"event": "worker_authorized",
                 "worker_launch_id": old_worker, "sample": sample,
                 "worker_pid": old_pid, "invocation_id": old_invocation,
                 "task_plan_sha256": plan_sha,
                 "transport_resume_authorization": None},
                {"event": "launch", "worker_launch_id": new_worker,
                 "sample": sample, "pid": new_pid},
                {"event": "worker_authorized",
                 "worker_launch_id": new_worker, "sample": sample,
                 "worker_pid": new_pid, "invocation_id": new_invocation,
                 "task_plan_sha256": plan_sha,
                 "transport_resume_authorization": resume},
            ):
                run_meta.append_jsonl_locked(dispatch_path, row)
            for row in (
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": old_invocation,
                    "worker_launch_id": old_worker,
                    "worker_pid": old_pid, "samples": [sample],
                    "status": "infrastructure_incomplete",
                    "task_plans": {sample: {
                        "sha256": plan_sha, "round_trips": 2}},
                    "transport_resume_authorization": None,
                },
                {
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": new_invocation,
                    "worker_launch_id": new_worker,
                    "worker_pid": new_pid, "samples": [sample],
                    "status": "started",
                    "task_plans": {sample: {
                        "sha256": plan_sha, "round_trips": 2}},
                    "transport_resume_authorization": (
                        paired_dispatch._canonical_resume_authorization(
                            resume)),
                },
            ):
                run_meta.append_jsonl_locked(
                    os.path.join(out_dir, "run_metadata.jsonl"), row)
            paired_dispatch._write_active_worker_set(
                out_dir, manifest,
                [] if variant == "unconsumed" else [{
                    "worker_launch_id": new_worker, "sample": sample,
                }])

            attempt_rows = []

            def append_event(exact_id, root, generation, parent, worker,
                             event, **fields):
                attempt_rows.append({
                    "schema": "anchorpatch.api_attempt/4",
                    "step_id": "/".join(root.split("/")[:4]),
                    "semantic_root_id": root,
                    "semantic_call_id": exact_id,
                    "generation_index": generation,
                    "parent_semantic_call_id": parent,
                    "worker_launch_id": worker,
                    "event": event,
                    **fields,
                })

            def append_success(exact_id, root, generation, parent, worker,
                               call_id, fingerprint, attempt_index):
                append_event(
                    exact_id, root, generation, parent, worker,
                    "semantic_request", call_id=call_id,
                    call_kind="hybridpatch_primary",
                    request_fingerprint=fingerprint)
                append_event(
                    exact_id, root, generation, parent, worker,
                    "attempt_start", call_id=call_id,
                    attempt_index=attempt_index,
                    call_kind="hybridpatch_primary",
                    attempt_kind=(
                        "transport_initial" if generation == 0
                        else "transport_recovery_initial"),
                    request_fingerprint=fingerprint)
                append_event(
                    exact_id, root, generation, parent, worker,
                    "generation_progress", call_id=call_id,
                    attempt_index=attempt_index, delta_type="text_delta")
                append_event(
                    exact_id, root, generation, parent, worker,
                    "attempt_end", call_id=call_id,
                    attempt_index=attempt_index,
                    **self._successful_attempt_end_fields())
                append_event(
                    exact_id, root, generation, parent, worker,
                    "response_committed", call_id=call_id,
                    attempt_index=attempt_index,
                    response_slots_used=1,
                    transient_failure_count=0,
                    http_attempts_used=attempt_index,
                    request_fingerprint=fingerprint)

            def append_failure():
                append_event(
                    backward_g000, backward_root, 0, None, old_worker,
                    "semantic_request", call_id="old-backward",
                    call_kind="hybridpatch_primary",
                    request_fingerprint=backward_fp)
                for attempt_index in (1, 2):
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "attempt_start", call_id="old-backward",
                        attempt_index=attempt_index,
                        call_kind="hybridpatch_primary",
                        attempt_kind=(
                            "transport_initial" if attempt_index == 1
                            else "transport_retry"),
                        request_fingerprint=backward_fp)
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "generation_progress", call_id="old-backward",
                        attempt_index=attempt_index,
                        delta_type="text_delta")
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "attempt_end", call_id="old-backward",
                        attempt_index=attempt_index,
                        **self._failed_attempt_end_fields())
                    append_event(
                        backward_g000, backward_root, 0, None, old_worker,
                        "attempt_budget", call_id="old-backward",
                        attempt_index=attempt_index,
                        budget_class="response_slot",
                        response_slots_used=attempt_index,
                        transient_failure_count=0)
                append_event(
                    backward_g000, backward_root, 0, None, old_worker,
                    "call_failed", call_id="old-backward",
                    attempt_index=2, status="provider_failure",
                    error_type="incomplete_stream",
                    response_slots_used=2,
                    transient_failure_count=0,
                    http_attempts_used=2,
                    request_fingerprint=backward_fp)

            append_success(
                forward_g000, forward_root, 0, None, old_worker,
                "old-forward", forward_fp, 1)
            append_failure()
            if variant != "unconsumed":
                append_success(
                    backward_g001, backward_root, 1, backward_g000,
                    new_worker, "new-backward", backward_fp, 3)
            if variant == "accepted":
                append_success(
                    next_g000, next_root, 0, None, new_worker,
                    "new-next", next_fp, 1)
            with open(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in attempt_rows:
                    handle.write(json.dumps(row) + "\n")

            def api_row(*, exact_id, root, rt, direction, generation,
                        parent, request_id, fingerprint, worker, pid,
                        provider_called=True, response_replayed=False,
                        replayed_from=None, failure=False,
                        http_attempts=1):
                return {
                    "schema": "anchorpatch.api_call/4",
                    "sample": sample, "method": method,
                    "rt_index": rt, "direction": direction,
                    "call_kind": "hybridpatch_primary",
                    "step_id": "/".join(root.split("/")[:4]),
                    "semantic_root_id": root,
                    "semantic_call_id": exact_id,
                    "generation_index": generation,
                    "parent_semantic_call_id": parent,
                    "request_id": request_id,
                    "worker_launch_id": worker, "worker_pid": pid,
                    "provider_called": provider_called,
                    "response_replayed": response_replayed,
                    "replayed_from_call_id": replayed_from,
                    "transport_revision": "opencode_anthropic_sdk/4",
                    "transport_resume_policy": (
                        "exact_payload_new_semantic_call/1"),
                    "transport_recovery_index": generation,
                    "request_fingerprint": fingerprint,
                    "classification": (
                        "provider/API failure" if failure else None),
                    "count_as_method_failure": False,
                    "error_type": (
                        "incomplete_stream" if failure else None),
                    "max_response_slots": 2,
                    "response_slots_used": 2 if failure else 1,
                    "max_transient_failures": 3,
                    "transient_failure_count": 0,
                    "http_attempts_used": http_attempts,
                }

            rows = [
                api_row(
                    exact_id=forward_g000, root=forward_root, rt=1,
                    direction="forward", generation=0, parent=None,
                    request_id="old-forward", fingerprint=forward_fp,
                    worker=old_worker, pid=old_pid),
                api_row(
                    exact_id=backward_g000, root=backward_root, rt=1,
                    direction="backward", generation=0, parent=None,
                    request_id="old-backward", fingerprint=backward_fp,
                    worker=old_worker, pid=old_pid, failure=True,
                    http_attempts=2),
                api_row(
                    exact_id=forward_g000, root=forward_root, rt=1,
                    direction="forward", generation=0, parent=None,
                    request_id="replay-forward", fingerprint=forward_fp,
                    worker=new_worker, pid=new_pid,
                    provider_called=False, response_replayed=True,
                    replayed_from="old-forward"),
            ]
            if variant == "provider-before":
                rows[-1].update({
                    "provider_called": True,
                    "response_replayed": False,
                    "replayed_from_call_id": None,
                })
            if variant != "unconsumed":
                recovery_row = api_row(
                    exact_id=backward_g001, root=backward_root, rt=1,
                    direction="backward", generation=1,
                    parent=backward_g000, request_id="new-backward",
                    fingerprint=backward_fp, worker=new_worker,
                    pid=new_pid, http_attempts=3)
                rows.append(recovery_row)
                if variant == "duplicate":
                    rows.append(dict(recovery_row))
            if variant == "accepted":
                rows.append(api_row(
                    exact_id=next_g000, root=next_root, rt=2,
                    direction="forward", generation=0, parent=None,
                    request_id="new-next", fingerprint=next_fp,
                    worker=new_worker, pid=new_pid))
            with open(
                os.path.join(out_dir, "api_calls.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")

            os.makedirs(os.path.join(out_dir, "api_journal"), exist_ok=True)

            def write_journal(exact_id, root, generation, parent, call_id,
                              fingerprint, attempt_index):
                digest = hashlib.sha256(
                    exact_id.encode("utf-8")).hexdigest()[:24]
                run_meta.write_json_atomic(
                    os.path.join(
                        out_dir, "api_journal",
                        f"{digest}.response.json"), {
                            "schema": "anchorpatch.api_response_journal/4",
                            "semantic_root_id": root,
                            "semantic_call_id": exact_id,
                            "generation_index": generation,
                            "parent_semantic_call_id": parent,
                            "call_id": call_id,
                            "request_fingerprint": fingerprint,
                            "result": {
                                "message": "complete",
                                "stream_complete": True,
                                "stop_reason": "end_turn",
                                "input_tokens": 5, "output_tokens": 1,
                                "transport_revision": (
                                    "opencode_anthropic_sdk/4"),
                                "transport_resume_policy": (
                                    "exact_payload_new_semantic_call/1"),
                                "call_kind": "hybridpatch_primary",
                                "semantic_root_id": root,
                                "semantic_call_id": exact_id,
                                "generation_index": generation,
                                "parent_semantic_call_id": parent,
                                "max_response_slots": 2,
                                "response_slots_used": 1,
                                "max_transient_failures": 3,
                                "transient_failure_count": 0,
                                "http_attempts_used": attempt_index,
                            },
                        })

            write_journal(
                forward_g000, forward_root, 0, None,
                "old-forward", forward_fp, 1)
            if variant != "unconsumed":
                write_journal(
                    backward_g001, backward_root, 1, backward_g000,
                    "new-backward", backward_fp, 3)
            if variant == "accepted":
                write_journal(
                    next_g000, next_root, 0, None,
                    "new-next", next_fp, 1)
            return manifest, rows

        cases = (
            ("accepted", None),
            ("provider-before",
             "provider call preceded recovery authorization use"),
            ("unconsumed", "recovery authorization was not consumed"),
            ("duplicate", "recovery authorization used more than once"),
        )
        for variant, expected_error in cases:
            with self.subTest(variant=variant), \
                    tempfile.TemporaryDirectory() as out_dir:
                manifest, rows = build_fixture(out_dir, variant)
                with mock.patch.object(
                        paired_dispatch, "_git_identity",
                        return_value=("1" * 40, "clean")):
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, active_samples={"sample"})
                if expected_error is None:
                    self.assertEqual(inspection["errors"], [])
                    new_worker_ids = [
                        row["semantic_call_id"] for row in rows
                        if row["worker_launch_id"] == "worker-new"
                    ]
                    self.assertEqual(new_worker_ids, [
                        "hybridpatch/sample/rt01/forward/"
                        "hybridpatch_primary/g000",
                        "hybridpatch/sample/rt01/backward/"
                        "hybridpatch_primary/g001",
                        "hybridpatch/sample/rt02/forward/"
                        "hybridpatch_primary/g000",
                    ])
                else:
                    self.assertTrue(any(
                        expected_error in error
                        for error in inspection["errors"]),
                        inspection["errors"])

    def test_paired_dispatch_counterbalances_and_checks_campaign_integrity(self):
        orders = [paired_dispatch.method_order(index) for index in range(10)]
        self.assertEqual(
            sum(order[0] == "hybridpatch" for order in orders), 5)
        self.assertEqual(
            sum(order[0] == "fullrewrite" for order in orders), 5)

        with tempfile.TemporaryDirectory() as out_dir:
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
            plan_sha = paired_dispatch._sha256(plan_path)
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "config": {
                    "samples": ["sample"],
                    "method_set": ["fullrewrite", "hybridpatch"],
                    "num_round_trips": 1,
                },
                "task_plans": {
                    "sample": {
                        "path": "sample.task_plan.json",
                        "sha256": plan_sha,
                        "forward_state_sequence": ["state_a"],
                    },
                },
            }
            api_rows = []
            attempt_rows = []
            journal_rows = []
            for method in manifest["config"]["method_set"]:
                os.makedirs(os.path.join(out_dir, method), exist_ok=True)
                result_rows = []
                for direction in ("forward", "backward"):
                    call_kind = (
                        "hybridpatch_primary"
                        if method == "hybridpatch"
                        else "fullrewrite_primary"
                    )
                    step_id = f"{method}/sample/rt01/{direction}"
                    semantic_root_id = f"{step_id}/{call_kind}"
                    semantic_call_id = f"{semantic_root_id}/g000"
                    request_id = f"original-{method}-{direction}"
                    request_fingerprint = f"fingerprint-{method}-{direction}"
                    api_rows.append({
                        "schema": "anchorpatch.api_call/4",
                        "sample": "sample", "method": method,
                        "rt_index": 1, "direction": direction,
                        "call_kind": call_kind,
                        "step_id": step_id,
                        "semantic_root_id": semantic_root_id,
                        "semantic_call_id": semantic_call_id,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "request_id": request_id,
                        "provider_called": True,
                        "worker_launch_id": "worker-a",
                        "worker_pid": 101,
                        "response_replayed": False,
                        "replayed_from_call_id": None,
                        "transport_revision": "opencode_anthropic_sdk/4",
                        "transport_resume_policy": (
                            "exact_payload_new_semantic_call/1"),
                        "transport_recovery_index": 0,
                        "request_fingerprint": request_fingerprint,
                        "classification": None,
                        "count_as_method_failure": False,
                        "max_response_slots": 2,
                        "response_slots_used": 1,
                        "max_transient_failures": 3,
                        "transient_failure_count": 0,
                        "http_attempts_used": 1,
                    })
                    journal_rows.append((semantic_call_id, {
                        "schema": "anchorpatch.api_response_journal/4",
                        "semantic_root_id": semantic_root_id,
                        "semantic_call_id": semantic_call_id,
                        "generation_index": 0,
                        "parent_semantic_call_id": None,
                        "call_id": request_id,
                        "request_fingerprint": request_fingerprint,
                        "result": {
                            "message": "complete",
                            "stream_complete": True,
                            "stop_reason": "end_turn",
                            "input_tokens": 5,
                            "output_tokens": 1,
                            "transport_revision": (
                                "opencode_anthropic_sdk/4"),
                            "transport_resume_policy": (
                                "exact_payload_new_semantic_call/1"),
                            "call_kind": call_kind,
                            "semantic_root_id": semantic_root_id,
                            "semantic_call_id": semantic_call_id,
                            "generation_index": 0,
                            "parent_semantic_call_id": None,
                            "max_response_slots": 2,
                            "response_slots_used": 1,
                            "max_transient_failures": 3,
                            "transient_failure_count": 0,
                            "http_attempts_used": 1,
                        },
                    }))
                    for event, fields in (
                        ("semantic_request", {
                            "call_id": request_id,
                            "call_kind": call_kind,
                            "request_fingerprint": request_fingerprint,
                        }),
                        ("attempt_start", {
                            "call_id": request_id, "attempt_index": 1,
                            "call_kind": call_kind,
                            "attempt_kind": "transport_initial",
                            "request_fingerprint": request_fingerprint,
                        }),
                        ("generation_progress", {
                            "call_id": request_id, "attempt_index": 1,
                            "delta_type": "text_delta",
                        }),
                        ("attempt_end", {
                            "call_id": request_id, "attempt_index": 1,
                            **self._successful_attempt_end_fields(),
                        }),
                        ("response_committed", {
                            "call_id": request_id, "attempt_index": 1,
                            "response_slots_used": 1,
                            "transient_failure_count": 0,
                            "http_attempts_used": 1,
                            "request_fingerprint": request_fingerprint,
                        }),
                    ):
                        attempt_rows.append({
                            "schema": "anchorpatch.api_attempt/4",
                            "step_id": step_id,
                            "semantic_root_id": semantic_root_id,
                            "semantic_call_id": semantic_call_id,
                            "generation_index": 0,
                            "parent_semantic_call_id": None,
                            "worker_launch_id": "worker-a",
                            "event": event,
                            **fields,
                        })
                    result_rows.append({
                        "sample_id": "sample", "method": method,
                        "round_trip_num": 1,
                        "round_trip_direction": direction,
                        "evaluation": (
                            {"score": 1.0}
                            if direction == "backward" else {}
                        ),
                        "bdpatch": (
                            {
                                "preservation_violations": 0,
                                "exec_log": {
                                    "preservation_violations": 0,
                                },
                            }
                            if method == "hybridpatch" else {}
                        ),
                    })
                with open(
                    os.path.join(out_dir, method, "sample.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in result_rows:
                        handle.write(json.dumps(row) + "\n")
                run_meta.write_json_atomic(
                    os.path.join(out_dir, method, "sample.ckpt.json"),
                    {"completed_round_trips": 1},
                )
            with open(
                os.path.join(out_dir, "api_calls.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in api_rows:
                    handle.write(json.dumps(row) + "\n")
            with open(
                os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                for row in attempt_rows:
                    handle.write(json.dumps(row) + "\n")
            os.makedirs(os.path.join(out_dir, "api_journal"), exist_ok=True)
            for semantic_call_id, journal in journal_rows:
                digest = hashlib.sha256(
                    semantic_call_id.encode("utf-8")).hexdigest()[:24]
                run_meta.write_json_atomic(
                    os.path.join(
                        out_dir, "api_journal",
                        f"{digest}.response.json"),
                    journal)
            with open(
                os.path.join(out_dir, "run_metadata.jsonl"),
                "w", encoding="utf-8",
            ) as handle:
                handle.write(json.dumps({
                    "schema": "anchorpatch.run_metadata/3",
                    "invocation_id": "invocation-a",
                    "worker_launch_id": "worker-a",
                    "worker_pid": 101,
                    "samples": ["sample"], "status": "finished",
                    "finished_at": "2026-07-17T00:00:00+08:00",
                    "task_plans": {
                        "sample": {
                            "sha256": plan_sha,
                            "round_trips": 1,
                        },
                    },
                    "transport_resume_authorization": None,
                }) + "\n")
            run_meta.append_jsonl_locked(
                os.path.join(out_dir, "sample_outcomes.jsonl"), {
                    "schema": "anchorpatch.sample_outcome/1",
                    "created_at": "2026-07-17T00:00:00+08:00",
                    "sample": "sample", "status": "finished",
                    "worker_launch_id": "worker-a", "worker_pid": 101,
                    "invocation_id": "invocation-a",
                    "methods": ["fullrewrite", "hybridpatch"],
                    "checkpoint_progress": {
                        "fullrewrite": {
                            "completed_round_trips": 1,
                            "committed_rows": 2,
                        },
                        "hybridpatch": {
                            "completed_round_trips": 1,
                            "committed_rows": 2,
                        },
                    },
                })
            dispatch_path = os.path.join(out_dir, "dispatch_log.jsonl")
            for dispatch_row in (
                {
                    "event": "launch", "worker_launch_id": "worker-a",
                    "sample": "sample", "pid": 101,
                },
                {
                    "event": "worker_authorized",
                    "worker_launch_id": "worker-a", "sample": "sample",
                    "worker_pid": 101,
                    "invocation_id": "invocation-a",
                    "task_plan_sha256": plan_sha,
                },
                {
                    "event": "worker_exit", "worker_launch_id": "worker-a",
                    "sample": "sample", "pid": 101, "returncode": 0,
                    "disposition": "finished",
                    "created_at": "2026-07-17T00:00:00+08:00",
                },
            ):
                run_meta.append_jsonl_locked(dispatch_path, dispatch_row)

            with mock.patch.object(
                paired_dispatch, "_git_identity", return_value=("1" * 40, "clean")
            ):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])

                outcome_path = os.path.join(out_dir, "sample_outcomes.jsonl")
                with open(outcome_path, encoding="utf-8") as handle:
                    outcome_rows = [
                        json.loads(line) for line in handle if line.strip()
                    ]
                original_outcome_rows = copy.deepcopy(outcome_rows)

                def write_outcome_rows():
                    run_meta._write_jsonl_atomic(outcome_path, outcome_rows)

                outcome_rows[0]["checkpoint_progress"] = None
                write_outcome_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertTrue(any(
                    "finished sample checkpoint evidence drift" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                outcome_rows[:] = copy.deepcopy(original_outcome_rows)
                outcome_rows[0]["worker_pid"] = 202
                write_outcome_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertTrue(any(
                    "finished sample worker provenance mismatch" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                outcome_rows[:] = copy.deepcopy(original_outcome_rows)
                write_outcome_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])

                with open(dispatch_path, encoding="utf-8") as handle:
                    dispatch_rows = [
                        json.loads(line) for line in handle if line.strip()
                    ]
                original_dispatch_rows = copy.deepcopy(dispatch_rows)
                worker_exit = next(
                    row for row in dispatch_rows
                    if row.get("event") == "worker_exit"
                )
                worker_exit["returncode"] = 7
                worker_exit["disposition"] = "campaign_fatal"
                run_meta._write_jsonl_atomic(dispatch_path, dispatch_rows)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertTrue(any(
                    "worker exit provenance incomplete" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                dispatch_rows[:] = copy.deepcopy(original_dispatch_rows)
                run_meta._write_jsonl_atomic(dispatch_path, dispatch_rows)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])

                fullrewrite_checkpoint = os.path.join(
                    out_dir, "fullrewrite", "sample.ckpt.json")
                os.remove(fullrewrite_checkpoint)
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, active_samples={"sample"})
                self.assertTrue(any(
                    "missing checkpoint: fullrewrite/sample" in error
                    for error in inspection["errors"]
                ))
                run_meta.write_json_atomic(
                    fullrewrite_checkpoint, {"completed_round_trips": 1})
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertEqual(inspection["errors"], [])
                self.assertEqual(inspection["preservation_violations"], 0)

                hybrid_result_path = os.path.join(
                    out_dir, "hybridpatch", "sample.jsonl"
                )
                with open(hybrid_result_path, encoding="utf-8") as handle:
                    hybrid_result_rows = [
                        json.loads(line) for line in handle if line.strip()
                    ]

                def write_hybrid_rows():
                    with open(
                        hybrid_result_path, "w", encoding="utf-8"
                    ) as result_handle:
                        for result_row in hybrid_result_rows:
                            result_handle.write(json.dumps(result_row) + "\n")

                hybrid_result_rows[0]["sample_id"] = "wrong"
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "committed row identity mismatch" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["sample_id"] = "sample"
                hybrid_result_rows[0]["method"] = "fullrewrite"
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "committed row identity mismatch" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["method"] = "hybridpatch"

                backward_row = next(
                    row for row in hybrid_result_rows
                    if row["round_trip_direction"] == "backward"
                )
                backward_row["evaluation"] = {}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unscoreable backward RT1" in error
                    for error in inspection["errors"]
                ))
                backward_row["evaluation"] = {"error": "context_mismatch"}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])
                backward_row["evaluation"] = {"score": 1.5}
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unscoreable backward RT1" in error
                    for error in inspection["errors"]
                ))
                backward_row["evaluation"] = {"score": 1.0}

                original_bdpatch = dict(hybrid_result_rows[0]["bdpatch"])
                original_bdpatch["exec_log"] = dict(
                    original_bdpatch["exec_log"])
                for invalid in (True, "0", 0.5, -1):
                    hybrid_result_rows[0]["bdpatch"][
                        "preservation_violations"] = invalid
                    write_hybrid_rows()
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest)
                    self.assertTrue(any(
                        "invalid/missing preservation telemetry" in error
                        for error in inspection["errors"]
                    ))
                hybrid_result_rows[0]["bdpatch"] = dict(original_bdpatch)
                hybrid_result_rows[0]["bdpatch"]["exec_log"] = dict(
                    original_bdpatch["exec_log"])
                for invalid_nested in (False, 0.0):
                    hybrid_result_rows[0]["bdpatch"]["exec_log"][
                        "preservation_violations"
                    ] = invalid_nested
                    write_hybrid_rows()
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest)
                    self.assertTrue(any(
                        "invalid/missing preservation telemetry" in error
                        for error in inspection["errors"]
                    ))
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"
                ] = 0
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"] = 1
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "invalid/missing preservation telemetry" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["bdpatch"] = {
                    "actual_method": (
                        "hybridpatch_protocol_failure_kept_context"
                    ),
                    "preservation_violations": None,
                    "exec_log": None,
                    "hybrid": {
                        "failed_step_kept_context": True,
                        "effective_modification": False,
                    },
                }
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])
                self.assertEqual(inspection["preservation_not_applicable"], 1)
                hybrid_result_rows[0]["bdpatch"] = original_bdpatch

                hybrid_result_rows[0]["bdpatch"]["preservation_violations"] = 1
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"] = 1
                write_hybrid_rows()
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest)
                self.assertEqual(inspection["preservation_violations"], 1)
                self.assertIn(
                    "preservation_violations=1", inspection["errors"]
                )
                hybrid_result_rows[0]["bdpatch"]["preservation_violations"] = 0
                hybrid_result_rows[0]["bdpatch"]["exec_log"][
                    "preservation_violations"] = 0
                write_hybrid_rows()

                failed_semantic = api_rows[0]["semantic_call_id"]
                failed_api_rows = [dict(row) for row in api_rows]
                failed_api_rows[0].update({
                    "classification": "provider/API failure",
                    "error_type": "incomplete_stream",
                    "response_slots_used": 2,
                    "http_attempts_used": 2,
                })

                def failed_attempt_records(api_row):
                    base = {
                        "schema": "anchorpatch.api_attempt/4",
                        "step_id": (
                            f"{api_row['method']}/{api_row['sample']}/"
                            f"rt{api_row['rt_index']:02d}/"
                            f"{api_row['direction']}"
                        ),
                        "semantic_root_id": api_row["semantic_root_id"],
                        "semantic_call_id": api_row["semantic_call_id"],
                        "generation_index": api_row["generation_index"],
                        "parent_semantic_call_id": (
                            api_row["parent_semantic_call_id"]
                        ),
                        "worker_launch_id": api_row["worker_launch_id"],
                    }
                    rows = [{
                        **base,
                        "event": "semantic_request",
                        "call_id": api_row["request_id"],
                        "call_kind": api_row["call_kind"],
                        "request_fingerprint": (
                            api_row["request_fingerprint"]
                        ),
                    }]
                    for attempt_index in (1, 2):
                        rows.extend([
                            {
                                **base,
                                "event": "attempt_start",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                "call_kind": api_row["call_kind"],
                                "attempt_kind": (
                                    "transport_initial"
                                    if attempt_index == 1
                                    else "transport_retry"
                                ),
                                "request_fingerprint": (
                                    api_row["request_fingerprint"]
                                ),
                            },
                            {
                                **base,
                                "event": "generation_progress",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                "delta_type": "text_delta",
                            },
                            {
                                **base,
                                "event": "attempt_end",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                **self._failed_attempt_end_fields(),
                            },
                            {
                                **base,
                                "event": "attempt_budget",
                                "call_id": api_row["request_id"],
                                "attempt_index": attempt_index,
                                "budget_class": "response_slot",
                                "response_slots_used": attempt_index,
                                "transient_failure_count": 0,
                            },
                        ])
                    rows.append({
                        **base,
                        "event": "call_failed",
                        "call_id": api_row["request_id"],
                        "attempt_index": 2,
                        "status": "provider_failure",
                        "error_type": "incomplete_stream",
                        "response_slots_used": 2,
                        "transient_failure_count": 0,
                        "http_attempts_used": 2,
                        "request_fingerprint": (
                            api_row["request_fingerprint"]
                        ),
                    })
                    return rows

                failed_attempt_rows = []
                for row in attempt_rows:
                    if row["semantic_call_id"] == failed_semantic:
                        continue
                    failed_attempt_rows.append(row)
                failed_attempt_rows.extend(
                    failed_attempt_records(failed_api_rows[0]))
                failed_journal_rows = [
                    item for item in journal_rows
                    if item[0] != failed_semantic
                ]
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    failed_api_rows)
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                    failed_attempt_rows)
                journal_dir = os.path.join(out_dir, "api_journal")
                failed_digest = hashlib.sha256(
                    failed_semantic.encode("utf-8")).hexdigest()[:24]
                os.remove(os.path.join(
                    journal_dir, f"{failed_digest}.response.json"))
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest)
                self.assertTrue(any(
                    "committed row requires a response-journal-backed "
                    "successful semantic call" in error
                    for error in inspection["errors"]
                ), inspection["errors"])
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    api_rows)
                run_meta._write_jsonl_atomic(
                    os.path.join(out_dir, "api_attempt_ledger.jsonl"),
                    attempt_rows)
                for semantic_call_id, journal in failed_journal_rows:
                    digest = hashlib.sha256(
                        semantic_call_id.encode("utf-8")).hexdigest()[:24]
                    run_meta.write_json_atomic(
                        os.path.join(
                            journal_dir, f"{digest}.response.json"),
                        journal)
                for semantic_call_id, journal in journal_rows:
                    digest = hashlib.sha256(
                        semantic_call_id.encode("utf-8")).hexdigest()[:24]
                    run_meta.write_json_atomic(
                        os.path.join(
                            journal_dir, f"{digest}.response.json"),
                        journal)

                original = api_rows[0]
                replay = dict(original)
                replay.update({
                    "request_id": "replay-request",
                    "provider_called": False,
                    "response_replayed": True,
                    "replayed_from_call_id": original["request_id"],
                })
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "a", encoding="utf-8",
                ) as handle:
                    handle.write(json.dumps(replay) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertEqual(inspection["errors"], [])

                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "a", encoding="utf-8",
                ) as handle:
                    handle.write(json.dumps(original) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "duplicate provider semantic generation" in error
                    for error in inspection["errors"]
                ))

                api_rows[0]["sample"] = "unknown"
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unmappable API ledger" in error
                    for error in inspection["errors"]
                ))

                api_rows[0]["sample"] = "sample"
                api_rows[0]["rt_index"] = True
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "unmappable API ledger" in error
                    for error in inspection["errors"]
                ))
                api_rows[0]["rt_index"] = 1

                api_rows[0]["response_slots_used"] = True
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "response-slot overrun" in error
                    for error in inspection["errors"]
                ))
                api_rows[0]["response_slots_used"] = 1

                api_rows[0]["transient_failure_count"] = False
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "transient budget overrun" in error
                    for error in inspection["errors"]
                ))
                api_rows[0]["transient_failure_count"] = 0

                hybrid_result_rows[0]["round_trip_num"] = True
                write_hybrid_rows()
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "invalid committed row key" in error
                    for error in inspection["errors"]
                ))
                hybrid_result_rows[0]["round_trip_num"] = 1
                write_hybrid_rows()

                checkpoint_path = os.path.join(
                    out_dir, "hybridpatch", "sample.ckpt.json"
                )
                for invalid_completed in (True, "1", 1.0):
                    run_meta.write_json_atomic(
                        checkpoint_path,
                        {"completed_round_trips": invalid_completed},
                    )
                    inspection = paired_dispatch.inspect_campaign(
                        out_dir, manifest, require_complete=True)
                    self.assertTrue(any(
                        "invalid checkpoint completed_round_trips" in error
                        for error in inspection["errors"]
                    ))
                run_meta.write_json_atomic(
                    checkpoint_path, {"completed_round_trips": 1}
                )

                hybrid_primary = next(
                    row for row in api_rows
                    if row["method"] == "hybridpatch"
                    and row["call_kind"] == "hybridpatch_primary"
                )
                hybrid_primary["call_kind"] = "hybridpatch_repair"
                hybrid_primary["semantic_call_id"] = (
                    hybrid_primary["semantic_call_id"].replace(
                        "hybridpatch_primary", "hybridpatch_repair"
                    )
                )
                with open(
                    os.path.join(out_dir, "api_calls.jsonl"),
                    "w", encoding="utf-8",
                ) as handle:
                    for row in api_rows:
                        handle.write(json.dumps(row) + "\n")
                inspection = paired_dispatch.inspect_campaign(out_dir, manifest)
                self.assertTrue(any(
                    "requires exactly one primary semantic call" in error
                    for error in inspection["errors"]
                ))

    def test_supplemental_key_pool_caps_four_and_mixes_method_starts(self):
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        assignments = paired_dispatch.build_key_assignments(
            paired_dispatch.SUPPLEMENTAL_SAMPLES, labels, 4,
            alternate_within_key=True)
        self.assertEqual(len(assignments), 40)
        by_key = {label: [] for label in labels}
        for item in assignments:
            by_key[item["key_label"]].append(item)
        self.assertEqual(
            sorted(len(items) for items in by_key.values()),
            [3] * 12 + [4],
        )
        for items in by_key.values():
            self.assertLessEqual(len(items), 4)
            self.assertEqual(
                {item["methods"][0] for item in items},
                {"hybridpatch", "fullrewrite"},
            )
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments), 20)
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments), 20)

        args = mock.Mock(
            campaign_role="supplemental", smoke_dir=None,
            samples=paired_dispatch.SUPPLEMENTAL_SAMPLES,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        paired_dispatch._validate_campaign_grid(args)
        args.slots_per_key = 3
        with self.assertRaisesRegex(RuntimeError, "slots_per_key 4"):
            paired_dispatch._validate_campaign_grid(args)

        legacy = paired_dispatch.build_key_assignments(
            paired_dispatch.MAIN_SAMPLES, labels[:10], 1)
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch" for item in legacy), 5)
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite" for item in legacy), 5)

    def test_confirmation_selection_digest_and_partition_are_strict(self):
        candidates = [f"sample-{index:03d}" for index in range(85)]
        selected = candidates[:68]
        reserve = candidates[68:]
        features = {
            sample: {"domain": f"domain-{index % 7}"}
            for index, sample in enumerate(candidates)
        }
        payload = {
            "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
            "experiment_id": paired_dispatch.CONFIRMATION_EXPERIMENT_ID,
            "candidate_rule": {
                "seed": 42, "candidate_count": 85,
                "selected_count": 68, "reserve_count": 17,
                "rule": "candidate_count_below100_select_about80_percent",
                "strict_unseen_candidate_count": 0,
            },
            "candidate_sample_ids": candidates,
            "selected_sample_ids": selected,
            "reserve_sample_ids": reserve,
            "reserve_selection_order": list(reversed(reserve)),
            "features": features,
            "exposure_policy": {
                "strict_any_provider_call": {"sample_count": 234},
                "method_developer_unseen": {
                    "excluded_sample_count": 149,
                    "registry_clean_candidate_count": 86,
                    "split_test_count": 20,
                    "clean_candidate_with_method_exposure_count": 1,
                    "clean_candidate_with_method_exposure_ids": ["python1"],
                    "documented_developer_content_exposure": {
                        "python1": "docs/FINDINGS.md:370-399",
                    },
                },
            },
            "runtime_evaluator_smoke": {
                "checked_count": 234, "runnable_count": 234,
                "failed_count": 0,
            },
            "inputs": {
                "selector_script": {
                    "path": "tools/build_unseen_confirmation_split.py",
                    "sha256": "1" * 64,
                },
                "registry": {
                    "path": "data/CONTAMINATION_REGISTRY.json",
                    "sha256": "2" * 64,
                },
                "hybrid_split": {
                    "path": "data/hybrid_split.json",
                    "sha256": "3" * 64,
                },
                "samples_root": "data/samples_delegate52",
                "sample_count": 234,
                "sample_json_manifest_sha256": "4" * 64,
            },
        }
        payload["artifact_sha256_preview"] = hashlib.sha256(
            paired_dispatch._canonical_json_bytes({
                "candidate_sample_ids": candidates,
                "selected_sample_ids": selected,
                "reserve_sample_ids": reserve,
                "features": features,
            })
        ).hexdigest()
        validated = paired_dispatch._validate_confirmation_selection_payload(
            payload, available_samples=candidates)
        self.assertEqual(validated["selected_sample_ids"], selected)
        self.assertEqual(validated["reserve_sample_ids"], reserve)
        registry = {
            "entries": [
                {"sample_id": sample, "status": "clean_candidate"}
                for sample in [*candidates, "python1"]
            ]
        }
        sealed_split = {
            "splits": {
                "dev": [], "val": [], "test": [], "unused_reserve": []
            }
        }
        paired_dispatch._validate_confirmation_candidate_semantics(
            validated, registry, sealed_split)
        bad_split = json.loads(json.dumps(sealed_split))
        bad_split["splits"]["test"] = [candidates[0]]
        with self.assertRaisesRegex(RuntimeError, "sealed"):
            paired_dispatch._validate_confirmation_candidate_semantics(
                validated, registry, bad_split)
        bad_registry = json.loads(json.dumps(registry))
        bad_registry["entries"].pop(0)
        with self.assertRaisesRegex(RuntimeError, "clean_candidate"):
            paired_dispatch._validate_confirmation_candidate_semantics(
                validated, bad_registry, sealed_split)
        args = mock.Mock(
            campaign_role="confirmation", samples=selected,
            selection_manifest="selection.json")
        with self.assertRaisesRegex(RuntimeError, "match --out_dir"):
            with mock.patch.object(
                    paired_dispatch, "_load_confirmation_selection",
                    return_value=(
                        selected, {"experiment_id": "exp_confirmation"})):
                paired_dispatch._resolve_confirmation_selection(
                    args, out_dir="different_experiment")

        mutations = []
        bad = json.loads(json.dumps(payload))
        bad["schema"] = "anchorpatch.method_unseen_confirmation_split/0"
        mutations.append((bad, "schema"))
        bad = json.loads(json.dumps(payload))
        bad["candidate_rule"]["seed"] = 7
        mutations.append((bad, "seed42"))
        bad = json.loads(json.dumps(payload))
        bad["reserve_sample_ids"][0] = bad["selected_sample_ids"][0]
        mutations.append((bad, "partition"))
        bad = json.loads(json.dumps(payload))
        bad["artifact_sha256_preview"] = "0" * 64
        mutations.append((bad, "digest"))
        for value, message in mutations:
            with self.subTest(message=message), self.assertRaisesRegex(
                    RuntimeError, message):
                paired_dispatch._validate_confirmation_selection_payload(
                    value, available_samples=candidates)

    def test_confirmation_selection_prefill_does_not_bypass_loader(self):
        selected = ["sample-a", "sample-b"]
        verified_record = {
            "experiment_id": "exp_verified",
            "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
            "path": "HP_V8/analysis/selection.json",
            "sha256": "1" * 64,
        }
        args = mock.Mock(
            campaign_role="confirmation", samples=[],
            selection_manifest="selection.json")
        args._selection_samples = ["forged"]
        args._selection_manifest_record = {"experiment_id": "forged"}
        with mock.patch.object(
                paired_dispatch, "_load_confirmation_selection",
                return_value=(selected, verified_record)) as load_selection:
            resolved = paired_dispatch._resolve_confirmation_selection(
                args, out_dir="exp_verified")
            load_selection.assert_called_once_with(
                "selection.json", current_experiment_dir="exp_verified")
        self.assertEqual(resolved, verified_record)
        self.assertEqual(args.samples, selected)

        with mock.patch.object(
                paired_dispatch, "_load_confirmation_selection",
                side_effect=AssertionError("verified cache should be reused")):
            self.assertEqual(
                paired_dispatch._resolve_confirmation_selection(
                    args, out_dir="exp_verified"),
                verified_record)

    def test_confirmation_selection_recompute_detects_swap_and_evidence_drift(self):
        candidates = [f"sample-{index}" for index in range(5)]
        selected = candidates[:4]
        reserve = candidates[4:]
        features = {
            sample: {
                "sample_id": sample,
                "domain": "unit",
                "formats": [".txt"],
                "format_group": ".txt",
                "task_types": ["local_edit"],
                "file_count": 1,
                "file_count_bin": "1",
                "doc_bytes": 100 + index,
                "context_files": ["a.txt"],
                "doc_length_bin": "q1",
            }
            for index, sample in enumerate(candidates)
        }

        class FakeSelector:
            @staticmethod
            def sample_ids(_samples_root):
                return list(candidates)

            @staticmethod
            def scan_any_provider_exposure(_repo, _all_samples):
                return {
                    "sample_ids": list(candidates),
                    "sample_count": len(candidates),
                    "evidence_manifest_sha256": "any-evidence",
                    "api_call_files": 1,
                    "api_raw_request_files": 0,
                }

            @staticmethod
            def scan_method_exposure(_repo, _all_samples):
                return {
                    "sample_ids": [],
                    "sample_count": 0,
                    "path_manifest_sha256": "method-evidence",
                }

            @staticmethod
            def load_registry_sets(_registry):
                return {
                    "contaminated": set(),
                    "unsupported_generation_domain": set(),
                    "clean_candidate": set(candidates),
                    "reserved_holdout_candidate": set(),
                }

            @staticmethod
            def split_sets(_hybrid_split):
                return {}

            @staticmethod
            def method_holdout_candidates(
                    _all_samples, registry_sets, _split, _method_exposure):
                return set(registry_sets["clean_candidate"]), set(), set()

            @staticmethod
            def sample_feature(_repo, sample_id):
                return copy.deepcopy(features[sample_id])

            @staticmethod
            def add_length_bins(_features):
                return None

            @staticmethod
            def selection_counts(candidate_count):
                self.assertEqual(candidate_count, 5)
                return 4, 1, "candidate_count_below100_select_about80_percent"

            @staticmethod
            def stratified_reserve(
                    _candidate_ids, _features, reserve_n, seed):
                self.assertEqual((reserve_n, seed), (1, 42))
                return list(reserve), list(reserve), {}

        payload = {
            "exposure_policy": {
                "strict_any_provider_call": {
                    "sample_ids": list(candidates),
                    "sample_count": len(candidates),
                    "evidence_manifest_sha256": "any-evidence",
                    "api_call_files": 1,
                    "api_raw_request_files": 0,
                },
                "method_developer_unseen": {
                    "actual_method_scan_count": 0,
                    "actual_method_scan_sample_ids": [],
                    "actual_method_path_manifest_sha256": "method-evidence",
                    "clean_candidate_with_method_exposure_count": 0,
                    "clean_candidate_with_method_exposure_ids": [],
                    "excluded_sample_count": 0,
                    "excluded_sample_ids": [],
                },
            },
            "runtime_evaluator_smoke": {
                "runnable_sample_ids": list(candidates),
            },
            "candidate_rule": {
                "seed": 42,
                "rule": "candidate_count_below100_select_about80_percent",
                "candidate_count": 5,
                "selected_count": 4,
                "reserve_count": 1,
                "strict_unseen_candidate_count": 0,
            },
            "candidate_sample_ids": list(candidates),
            "selected_sample_ids": list(selected),
            "reserve_sample_ids": list(reserve),
            "reserve_selection_order": list(reserve),
            "features": copy.deepcopy(features),
        }
        validated = {
            "selected_sample_ids": list(selected),
            "reserve_sample_ids": list(reserve),
            "reserve_selection_order": list(reserve),
        }
        paired_dispatch._validate_confirmation_recomputed_selection(
            payload, validated, pathlib.Path("."), FakeSelector, {}, {})

        swapped_payload = copy.deepcopy(payload)
        swapped_payload["selected_sample_ids"] = [
            *candidates[:3], candidates[4]]
        swapped_payload["reserve_sample_ids"] = [candidates[3]]
        swapped_validated = {
            "selected_sample_ids": swapped_payload["selected_sample_ids"],
            "reserve_sample_ids": swapped_payload["reserve_sample_ids"],
            "reserve_selection_order": swapped_payload["reserve_sample_ids"],
        }
        with self.assertRaisesRegex(RuntimeError, "selected_sample_ids"):
            paired_dispatch._validate_confirmation_recomputed_selection(
                swapped_payload, swapped_validated, pathlib.Path("."),
                FakeSelector, {}, {})

        drifted_payload = copy.deepcopy(payload)
        drifted_payload["exposure_policy"]["strict_any_provider_call"][
            "evidence_manifest_sha256"] = "changed"
        with self.assertRaisesRegex(RuntimeError, "evidence_manifest_sha256"):
            paired_dispatch._validate_confirmation_recomputed_selection(
                drifted_payload, validated, pathlib.Path("."),
                FakeSelector, {}, {})

    def test_confirmation_selection_loader_excludes_current_experiment_only(self):
        all_samples = [f"sample-{index:03d}" for index in range(234)]
        candidates = all_samples[:85]
        selected = candidates[:68]
        reserve = candidates[68:]
        excluded = all_samples[85:]

        def feature(sample_id):
            return {
                "sample_id": sample_id,
                "domain": "unit",
                "formats": [".txt"],
                "format_group": ".txt",
                "task_types": ["local_edit"],
                "file_count": 1,
                "file_count_bin": "1",
                "doc_bytes": 100,
                "context_files": ["a.txt"],
                "doc_length_bin": "q1",
            }

        selector_source = textwrap.dedent("""
            from collections import defaultdict
            import hashlib
            import json

            def compact_json_bytes(value):
                return json.dumps(
                    value, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")).encode("utf-8")

            def sha256_file(path):
                h = hashlib.sha256()
                with path.open("rb") as handle:
                    h.update(handle.read())
                return h.hexdigest()

            def sample_ids(samples_root):
                return sorted(
                    path.parent.name
                    for path in samples_root.glob("*/sample.json")
                    if path.parent.is_dir())

            def _walk_experiment_paths(repo):
                root = repo / "HP_V8"
                if root.exists():
                    yield from root.rglob("*")

            def scan_any_provider_exposure(repo, all_samples):
                samples = set()
                evidence = defaultdict(list)
                api_call_files = 0
                api_call_sources = []
                for path in _walk_experiment_paths(repo):
                    if not path.is_file() or path.name != "api_calls.jsonl":
                        continue
                    api_call_files += 1
                    rel = path.relative_to(repo).as_posix()
                    api_call_sources.append({
                        "path": rel, "sha256": sha256_file(path)})
                    with path.open(encoding="utf-8") as handle:
                        for line in handle:
                            try:
                                row = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            sample = row.get("sample") or row.get("sample_id")
                            if sample in all_samples:
                                samples.add(sample)
                                if len(evidence[sample]) < 3:
                                    evidence[sample].append(rel)
                return {
                    "sample_ids": sorted(samples),
                    "sample_count": len(samples),
                    "evidence_examples": dict(sorted(evidence.items())),
                    "api_call_files": api_call_files,
                    "api_raw_request_files": 0,
                    "evidence_manifest_sha256": hashlib.sha256(
                        compact_json_bytes({
                            "api_call_sources": sorted(
                                api_call_sources,
                                key=lambda item: item["path"]),
                            "api_raw_request_paths": [],
                        })).hexdigest(),
                }

            def scan_method_exposure(_repo, _all_samples):
                return {
                    "sample_ids": [],
                    "sample_count": 0,
                    "evidence_examples": {},
                    "path_manifest_sha256": hashlib.sha256(
                        compact_json_bytes({
                            "matched_paths": [],
                            "matched_api_call_sources": [],
                        })).hexdigest(),
                }

            def load_registry_sets(registry):
                clean = {
                    item["sample_id"] for item in registry["entries"]
                    if item.get("status") == "clean_candidate"}
                return {
                    "contaminated": set(),
                    "unsupported_generation_domain": set(),
                    "clean_candidate": clean,
                    "reserved_holdout_candidate": set(),
                }

            def split_sets(_hybrid_split):
                return {}

            def method_holdout_candidates(
                    all_samples, registry_sets, _split, _method_exposure):
                pool = set(registry_sets["clean_candidate"]) - {"python1"}
                exposed = set(all_samples) - pool
                return pool, exposed, {"python1"}

            def sample_feature(_repo, sample_id):
                return {
                    "sample_id": sample_id,
                    "domain": "unit",
                    "formats": [".txt"],
                    "format_group": ".txt",
                    "task_types": ["local_edit"],
                    "file_count": 1,
                    "file_count_bin": "1",
                    "doc_bytes": 100,
                    "context_files": ["a.txt"],
                    "doc_length_bin": "q1",
                }

            def add_length_bins(_features):
                return None

            def selection_counts(candidate_count):
                assert candidate_count == 85
                return 68, 17, "candidate_count_below100_select_about80_percent"

            def stratified_reserve(_candidate_ids, _features, reserve_n, seed):
                assert (reserve_n, seed) == (17, 42)
                reserve = [f"sample-{index:03d}" for index in range(68, 85)]
                return reserve, reserve, {}
        """)

        with tempfile.TemporaryDirectory() as root_dir:
            repo_root = pathlib.Path(root_dir)
            samples_root = repo_root / "data" / "samples_delegate52"
            for sample in all_samples:
                sample_dir = samples_root / sample
                sample_dir.mkdir(parents=True, exist_ok=True)
                (sample_dir / "sample.json").write_text(
                    "{}", encoding="utf-8")
            sample_files = {
                sample: paired_dispatch._sha256(
                    samples_root / sample / "sample.json")
                for sample in all_samples
            }
            sample_digest = hashlib.sha256(
                paired_dispatch._canonical_json_bytes(sample_files)
            ).hexdigest()

            tools_dir = repo_root / "tools"
            tools_dir.mkdir()
            selector_path = tools_dir / "build_unseen_confirmation_split.py"
            selector_path.write_text(selector_source, encoding="utf-8")
            registry_path = repo_root / "data" / "CONTAMINATION_REGISTRY.json"
            registry = {
                "entries": [
                    {"sample_id": sample, "status": "clean_candidate"}
                    for sample in [*candidates, "python1"]
                ]
            }
            paired_dispatch.write_json_atomic(registry_path, registry)
            split_path = repo_root / "data" / "hybrid_split.json"
            paired_dispatch.write_json_atomic(split_path, {
                "splits": {
                    "dev": [], "val": [], "test": [],
                    "unused_reserve": [],
                }
            })

            history_dir = repo_root / "HP_V8" / "exp_history"
            history_dir.mkdir(parents=True)
            history_api = history_dir / "api_calls.jsonl"
            with history_api.open("w", encoding="utf-8") as handle:
                for sample in all_samples:
                    handle.write(json.dumps({
                        "sample": sample,
                        "method": "fullrewrite",
                    }) + "\n")
            strict_digest = hashlib.sha256(
                paired_dispatch._canonical_json_bytes({
                    "api_call_sources": [{
                        "path": history_api.relative_to(repo_root).as_posix(),
                        "sha256": paired_dispatch._sha256(history_api),
                    }],
                    "api_raw_request_paths": [],
                })
            ).hexdigest()
            method_digest = hashlib.sha256(
                paired_dispatch._canonical_json_bytes({
                    "matched_paths": [],
                    "matched_api_call_sources": [],
                })
            ).hexdigest()
            features = {sample: feature(sample) for sample in candidates}
            payload = {
                "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
                "experiment_id": paired_dispatch.CONFIRMATION_EXPERIMENT_ID,
                "candidate_rule": {
                    "seed": 42,
                    "candidate_count": 85,
                    "selected_count": 68,
                    "reserve_count": 17,
                    "rule": (
                        "candidate_count_below100_select_about80_percent"),
                    "strict_unseen_candidate_count": 0,
                },
                "candidate_sample_ids": candidates,
                "selected_sample_ids": selected,
                "reserve_sample_ids": reserve,
                "reserve_selection_order": reserve,
                "features": features,
                "exposure_policy": {
                    "strict_any_provider_call": {
                        "sample_ids": all_samples,
                        "sample_count": 234,
                        "evidence_manifest_sha256": strict_digest,
                        "api_call_files": 1,
                        "api_raw_request_files": 0,
                    },
                    "method_developer_unseen": {
                        "actual_method_scan_count": 0,
                        "actual_method_scan_sample_ids": [],
                        "actual_method_path_manifest_sha256": method_digest,
                        "excluded_sample_count": 149,
                        "excluded_sample_ids": excluded,
                        "registry_clean_candidate_count": 86,
                        "split_test_count": 20,
                        "clean_candidate_with_method_exposure_count": 1,
                        "clean_candidate_with_method_exposure_ids": ["python1"],
                        "documented_developer_content_exposure": {
                            "python1": "docs/FINDINGS.md:370-399",
                        },
                    },
                },
                "runtime_evaluator_smoke": {
                    "checked_count": 234,
                    "runnable_count": 234,
                    "failed_count": 0,
                    "runnable_sample_ids": all_samples,
                },
                "inputs": {
                    "selector_script": {
                        "path": "tools/build_unseen_confirmation_split.py",
                        "sha256": paired_dispatch._sha256(selector_path),
                    },
                    "registry": {
                        "path": "data/CONTAMINATION_REGISTRY.json",
                        "sha256": paired_dispatch._sha256(registry_path),
                    },
                    "hybrid_split": {
                        "path": "data/hybrid_split.json",
                        "sha256": paired_dispatch._sha256(split_path),
                    },
                    "samples_root": "data/samples_delegate52",
                    "sample_count": 234,
                    "sample_json_manifest_sha256": sample_digest,
                },
            }
            payload["artifact_sha256_preview"] = hashlib.sha256(
                paired_dispatch._canonical_json_bytes({
                    "candidate_sample_ids": candidates,
                    "selected_sample_ids": selected,
                    "reserve_sample_ids": reserve,
                    "features": features,
                })
            ).hexdigest()
            selection_path = (
                repo_root / "HP_V8" / "analysis"
                / "selection.json")
            selection_path.parent.mkdir(parents=True)
            paired_dispatch.write_json_atomic(selection_path, payload)
            current_dir = (
                repo_root / "HP_V8"
                / paired_dispatch.CONFIRMATION_EXPERIMENT_ID)
            current_dir.mkdir()
            with (current_dir / "api_calls.jsonl").open(
                    "w", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "sample": all_samples[0],
                    "method": "fullrewrite",
                }) + "\n")

            class Completed:
                returncode = 0
                stdout = b""
                stderr = b""

            def fake_git(command, **_kwargs):
                result = Completed()
                if command[:2] == ["git", "show"]:
                    result.stdout = selection_path.read_bytes()
                return result

            with mock.patch.object(
                    paired_dispatch, "_resolve_selection_path",
                    return_value=(selection_path, repo_root)), \
                    mock.patch.object(
                        paired_dispatch.subprocess, "run",
                        side_effect=fake_git), \
                    mock.patch.object(
                        paired_dispatch, "SAMPLES_ROOT", str(samples_root)):
                selected_ids, record = paired_dispatch._load_confirmation_selection(
                    str(selection_path), current_experiment_dir=current_dir)
                self.assertEqual(selected_ids, selected)
                self.assertEqual(
                    record["recompute"]["excluded_current_experiment_dir"],
                    current_dir.relative_to(repo_root).as_posix())

                other_dir = repo_root / "HP_V8" / "exp_new_history"
                other_dir.mkdir()
                with (other_dir / "api_calls.jsonl").open(
                        "w", encoding="utf-8") as handle:
                    handle.write(json.dumps({
                        "sample": all_samples[1],
                        "method": "fullrewrite",
                    }) + "\n")
                with self.assertRaisesRegex(
                        RuntimeError,
                        "strict_any_provider_call.evidence_manifest_sha256"):
                    paired_dispatch._load_confirmation_selection(
                        str(selection_path),
                        current_experiment_dir=current_dir)

    def test_confirmation_manifest_uses_balanced_work_conserving_key_queues(self):
        samples = [f"sample-{index:03d}" for index in range(68)]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 4, alternate_within_key=True,
            allow_queue=True)
        for label in labels:
            initial = [
                item for item in assignments if item["key_label"] == label
            ][:4]
            self.assertEqual(
                sum(item["methods"][0] == "hybridpatch" for item in initial),
                2,
            )
            self.assertEqual(
                sum(item["methods"][0] == "fullrewrite" for item in initial),
                2,
            )
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments), 34)
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments), 34)
        queues = paired_dispatch._assignment_key_queues(assignments)
        self.assertEqual(len(queues), 13)
        self.assertEqual(
            sorted(queue["worker_count"] for queue in queues),
            [5] * 10 + [6] * 3,
        )
        with self.assertRaisesRegex(RuntimeError, "concurrency capacity"):
            paired_dispatch.build_key_assignments(samples, labels, 4)

        args = mock.Mock(
            campaign_role="confirmation", smoke_dir=None, samples=samples,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        args._selection_manifest_record = {
            "experiment_id": "out",
            "schema": paired_dispatch.CONFIRMATION_SELECTION_SCHEMA,
            "path": "HP_V8/analysis/selection.json",
            "sha256": "1" * 64,
            "artifact_sha256_preview": "2" * 64,
            "candidate_count": 85, "selected_count": 68,
            "reserve_count": 17, "seed": 42,
            "inputs": {
                "selector_script": {"path": "tools/selector.py",
                                    "sha256": "3" * 64},
                "registry": {"path": "data/registry.json",
                             "sha256": "4" * 64},
                "hybrid_split": {"path": "data/split.json",
                                 "sha256": "5" * 64},
                "sample_json_manifest": {
                    "path": "data/samples_delegate52",
                    "sample_count": 234, "sha256": "6" * 64,
                },
            },
        }
        paired_dispatch._validate_campaign_grid(args)
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "out", samples, assignments, {}, args)
        self.assertNotIn("assignment_waves", manifest)
        self.assertEqual(
            sum(queue["worker_count"]
                for queue in manifest["assignment_queues"]),
            68)
        self.assertEqual(
            manifest["config"]["dispatch_policy"],
            paired_dispatch.WORK_CONSERVING_DISPATCH_POLICY)
        self.assertEqual(manifest["config"]["max_worker_count"], 52)
        self.assertEqual(manifest["config"]["queued_worker_count"], 16)
        self.assertEqual(
            manifest["selection_manifest"]["sha256"], "1" * 64)
        self.assertEqual(
            manifest["analysis_policy"],
            paired_dispatch.CONFIRMATION_ANALYSIS_POLICY)
        self.assertEqual(
            manifest["analysis_policy"]["evaluator_error_policy"][
                "error_row_score"],
            0.0)
        self.assertEqual(
            set(manifest["selection_manifest"]["inputs"]),
            {"selector_script", "registry", "hybrid_split",
             "sample_json_manifest"})
        self.assertEqual(manifest["config"]["slots_per_key"], 4)

    def test_mixed_confirmation_selection_and_balanced_100_grid(self):
        repo_root = pathlib.Path(paired_dispatch._ROOT).parent
        selection_path = (
            repo_root / "HP_V8" / "analysis"
            / "20260719_hybridv8_transportv4_mixed_confirmation100"
            / "selection.json"
        )
        payload = json.loads(selection_path.read_text(encoding="utf-8"))
        validated = paired_dispatch._validate_confirmation_selection_payload(
            payload,
            available_samples=set(payload["candidate_sample_ids"]),
        )
        self.assertEqual(validated["selection_kind"], "mixed100")
        self.assertEqual(len(validated["selected_sample_ids"]), 100)
        self.assertEqual(
            len(payload["cohorts"]["method_unseen"]["selected_sample_ids"]),
            60,
        )
        self.assertEqual(
            len(payload["cohorts"]["historical_hp_method_exposed"]
                ["selected_sample_ids"]),
            40,
        )
        samples = validated["selected_sample_ids"]
        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        selection_record = {
            "schema": paired_dispatch.MIXED_CONFIRMATION_SELECTION_SCHEMA,
            "cohorts": validated["cohorts"],
        }
        assignment_samples = paired_dispatch._mixed_confirmation_sample_order(
            samples, labels, 4, selection_record)
        assignments = paired_dispatch.build_key_assignments(
            assignment_samples, labels, 4, alternate_within_key=True,
            allow_queue=True)
        queues = paired_dispatch._assignment_key_queues(assignments)
        self.assertEqual(len(queues), 13)
        self.assertEqual(
            sorted(queue["worker_count"] for queue in queues),
            [7] * 4 + [8] * 9,
        )
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments),
            50,
        )
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments),
            50,
        )
        for cohort, expected in (
                (set(validated["cohorts"]["method_unseen"]), 30),
                (set(validated["cohorts"]
                     ["historical_hp_method_exposed"]), 20)):
            cohort_assignments = [
                item for item in assignments if item["sample"] in cohort]
            self.assertEqual(
                sum(item["methods"][0] == "hybridpatch"
                    for item in cohort_assignments),
                expected,
            )
            self.assertEqual(
                sum(item["methods"][0] == "fullrewrite"
                    for item in cohort_assignments),
                expected,
            )
        args = mock.Mock(
            campaign_role="confirmation", smoke_dir=None, samples=samples,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        args._selection_manifest_record = {
            "selected_count": 100,
            "schema": paired_dispatch.MIXED_CONFIRMATION_SELECTION_SCHEMA,
            "experiment_id": paired_dispatch.MIXED_CONFIRMATION_EXPERIMENT_ID,
        }
        paired_dispatch._validate_campaign_grid(args)

    def test_full234_scope_is_exact_and_uses_thirteen_rolling_key_queues(self):
        samples, scope = paired_dispatch._load_full234_scope()
        self.assertEqual(len(samples), 234)
        self.assertEqual(samples, sorted(samples))
        self.assertEqual(scope["sample_count"], 234)
        self.assertEqual(len(scope["sample_json_sha256"]), 64)

        args = mock.Mock(
            campaign_role="full234", smoke_dir=None, samples=None,
            num_round_trips=10, seed=42, slots_per_key=4,
        )
        resolved = paired_dispatch._resolve_full234_scope(args)
        self.assertEqual(resolved, scope)
        self.assertEqual(args.samples, samples)
        paired_dispatch._validate_campaign_grid(args)

        labels = [f"KEY_{index:02d}" for index in range(1, 14)]
        assignments = paired_dispatch.build_key_assignments(
            samples, labels, 4, alternate_within_key=True,
            allow_queue=True)
        self.assertEqual(
            sum(item["methods"][0] == "hybridpatch"
                for item in assignments),
            117,
        )
        self.assertEqual(
            sum(item["methods"][0] == "fullrewrite"
                for item in assignments),
            117,
        )
        with mock.patch.object(
                paired_dispatch, "_git_identity",
                return_value=("1" * 40, "clean")), \
                mock.patch.object(
                    paired_dispatch, "code_fingerprint",
                    return_value={"unit": "test"}):
            manifest = paired_dispatch.build_manifest(
                "out", samples, assignments, {}, args)
        self.assertEqual(manifest["full234_scope"], scope)
        self.assertEqual(len(manifest["assignment_queues"]), 13)
        self.assertTrue(all(
            queue["worker_count"] == 18
            for queue in manifest["assignment_queues"]))
        self.assertEqual(manifest["config"]["max_worker_count"], 52)
        self.assertEqual(manifest["config"]["queued_worker_count"], 182)
        self.assertNotIn("assignment_waves", manifest)
