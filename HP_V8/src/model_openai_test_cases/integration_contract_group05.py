"""Mixin slice for test_model_openai.IntegrationContractGroup05Mixin."""

from .support import *


class IntegrationContractGroup05Mixin:
    def test_runner_call_kinds_are_all_adaptive(self):
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs)
            return {"message": "", "completion_tokens": 0}

        experiment_runner._attempt_hybrid_repair(
            "bad", ["invalid"], "minimax-m3", 16, fake_generate,
            editable_context={"a.txt": "x"}, edit_instruction="edit",
        )
        self.assertEqual(calls[-1]["call_kind"], "hybridpatch_repair")
        self.assertEqual(calls[-1]["thinking_mode"], "adaptive")

        class DummyDomain:
            @staticmethod
            def prepare_prompt(*_args):
                return "rewrite"

        experiment_runner._edit_step(
            "fullrewrite", DummyDomain(), "sample", "minimax-m3",
            {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
            fake_generate, step_direction="forward",
        )
        self.assertEqual(calls[-1]["call_kind"], "fullrewrite_primary")
        self.assertEqual(calls[-1]["thinking_mode"], "adaptive")

        clean_attempt = {
            "raw": "{}", "gen": {"a.txt": "x"}, "exec_log": None,
            "gate_pass": True, "need_repair": False, "partial_ok": False,
            "invalid_json": False, "partial_extraction": False,
            "fence_complete": True, "schema_errors": [], "schema_warnings": [],
            "gate_errors": [], "trigger": None, "forward_audit": None,
            "errors": [], "key": (1, 1, 1, 1.0),
        }
        with mock.patch.object(experiment_runner, "build_hybrid_prompt", return_value="patch"), \
             mock.patch.object(experiment_runner, "_run_attempt_hybrid", return_value=clean_attempt):
            experiment_runner._edit_step(
                "hybridpatch", DummyDomain(), "sample", "minimax-m3",
                {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                fake_generate, step_direction="forward",
            )
        self.assertEqual(calls[-1]["call_kind"], "hybridpatch_primary")
        self.assertEqual(calls[-1]["thinking_mode"], "adaptive")
        self.assertIn("thinking_mode='adaptive'", probe_fr_keys._PROBE_SOURCE)
        self.assertIn("call_kind='key_probe'", probe_fr_keys._PROBE_SOURCE)

    def test_run_relay_preservation_gate_stops_before_next_api_or_commit(self):
        class DummyDomain:
            samples_folder = None

        class DummyExecLog:
            def __init__(self, preservation_violations):
                self.preservation_violations = preservation_violations

            def to_dict(self):
                return {"ops_accepted": 1, "ops_total": 1}

        states = {
            "initial": {
                "context": ["a.txt"],
                "solution_folder": "solution",
                "prompts": [{"target_state": "target", "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"],
                "solution_folder": "solution",
                "prompts": [{"target_state": "initial", "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}
        def edit_result(preservation_violations):
            return (
                "raw", {"a.txt": "new"}, {},
                DummyExecLog(preservation_violations), "hybridpatch",
                {"a.txt": "old"}, {},
            )
        with tempfile.TemporaryDirectory() as forward_dir, \
                tempfile.TemporaryDirectory() as backward_dir, \
                mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"
                ), \
                mock.patch.object(
                    experiment_runner, "load_sample",
                    return_value=(sample, forward_dir, states),
                ), \
                mock.patch.object(
                    experiment_runner, "get_domain", return_value=DummyDomain()
                ), \
                mock.patch.object(
                    experiment_runner, "load_distractor_context",
                    return_value={},
                ), \
                mock.patch.object(
                    experiment_runner, "build_context_from_folder",
                    return_value={"a.txt": "old"},
                ), \
                mock.patch.object(
                    experiment_runner, "build_relay_task_plan",
                    return_value=["target"],
                ), \
                mock.patch.object(
                    experiment_runner, "register_task_plan",
                    return_value={"sha256": "a" * 64, "round_trips": 1},
                ), \
                mock.patch.object(
                    experiment_runner, "shuffle_context",
                    side_effect=lambda value: value,
                ), \
                mock.patch.object(
                    experiment_runner, "merge_distractor",
                    side_effect=lambda value, _distractor: value,
                ), \
                mock.patch.object(
                    experiment_runner, "_evaluate",
                    return_value={"score": 1.0},
                ) as evaluate, \
                mock.patch.object(
                    experiment_runner, "is_context_complete",
                    return_value=True,
                ), \
                mock.patch.object(
                    experiment_runner, "dump_step_docs"
                ), \
                mock.patch.object(
                    experiment_runner, "generate_response_id",
                    side_effect=["rid-fwd", "rid-fwd-2", "rid-bwd"],
                ), \
                mock.patch.object(
                    experiment_runner, "_edit_step",
                    return_value=edit_result(1),
                ) as edit_step, \
                mock.patch.object(
                    experiment_runner, "append_relay_rows_and_checkpoint"
                ) as commit, \
                mock.patch.object(experiment_runner, "_row") as make_row:
            make_row.side_effect = [
                {"bdpatch": {"preservation_violations": 1}},
            ]
            with self.assertRaisesRegex(RuntimeError, "/forward"):
                experiment_runner.run_relay(
                    "hybridpatch", "sample", num_round_trips=1,
                    include_distractor=True, out_dir=forward_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=mock.Mock(), printing=False,
                    stop_on_preservation_violation=True,
                )
            self.assertEqual(edit_step.call_count, 1)
            evaluate.assert_not_called()
            commit.assert_not_called()
            forward_stop = run_meta.read_campaign_stop_conditions(
                forward_dir)[0]
            self.assertEqual(
                forward_stop["condition"], "preservation_violation")
            self.assertEqual(forward_stop["direction"], "forward")
            self.assertFalse(forward_stop["result_committed"])

            edit_step.reset_mock()
            evaluate.reset_mock()
            commit.reset_mock()
            edit_step.side_effect = [
                edit_result(0), edit_result(1),
            ]
            make_row.side_effect = [
                {
                    "bdpatch": {"preservation_violations": 0},
                    "evaluation": {"score": 1.0},
                },
            ]
            with self.assertRaisesRegex(RuntimeError, "/backward"):
                experiment_runner.run_relay(
                    "hybridpatch", "sample", num_round_trips=1,
                    include_distractor=True, out_dir=backward_dir,
                    model="offline-test-model", max_tokens=16,
                    generate_fn=mock.Mock(), printing=False,
                    stop_on_preservation_violation=True,
                )
            self.assertEqual(edit_step.call_count, 2)
            self.assertEqual(evaluate.call_count, 1)
            commit.assert_not_called()
            backward_stop = run_meta.read_campaign_stop_conditions(
                backward_dir)[0]
            self.assertEqual(backward_stop["direction"], "backward")
            self.assertFalse(backward_stop["result_committed"])

    def test_sibling_stop_during_backward_generation_blocks_eval_and_commit(self):
        class DummyDomain:
            samples_folder = None

        class DummyExecLog:
            def to_dict(self):
                return {"ops_accepted": 1, "ops_total": 1}

        states = {
            "initial": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{
                    "target_state": "target", "prompt": "forward"}],
            },
            "target": {
                "context": ["a.txt"], "solution_folder": "solution",
                "prompts": [{
                    "target_state": "initial", "prompt": "backward"}],
            },
        }
        sample = {"start_state": "initial", "sample_type": "dummy"}
        edit_result = (
            "raw", {"a.txt": "new"}, {}, DummyExecLog(), "hybridpatch",
            {"a.txt": "old"}, {},
        )
        backward_entered = threading.Event()
        release_backward = threading.Event()
        calls = []
        runner_errors = []

        def blocking_edit(*_args, **kwargs):
            direction = kwargs.get("step_direction")
            calls.append(direction)
            if direction == "backward":
                backward_entered.set()
                if not release_backward.wait(5):
                    raise RuntimeError("unit-test backward barrier timed out")
            return edit_result

        with tempfile.TemporaryDirectory() as out_dir, \
                tempfile.TemporaryDirectory() as sample_dir, \
                mock.patch.object(
                    experiment_runner, "_require_formal_opencode_transport"), \
                mock.patch.object(
                    experiment_runner, "load_sample",
                    return_value=(sample, sample_dir, states)), \
                mock.patch.object(
                    experiment_runner, "get_domain",
                    return_value=DummyDomain()), \
                mock.patch.object(
                    experiment_runner, "load_distractor_context",
                    return_value={}), \
                mock.patch.object(
                    experiment_runner, "build_context_from_folder",
                    return_value={"a.txt": "old"}), \
                mock.patch.object(
                    experiment_runner, "build_relay_task_plan",
                    return_value=["target"]), \
                mock.patch.object(
                    experiment_runner, "register_task_plan",
                    return_value={"sha256": "a" * 64,
                                  "round_trips": 1}), \
                mock.patch.object(
                    experiment_runner, "shuffle_context",
                    side_effect=lambda value: value), \
                mock.patch.object(
                    experiment_runner, "merge_distractor",
                    side_effect=lambda value, _distractor: value), \
                mock.patch.object(
                    experiment_runner, "_edit_step",
                    side_effect=blocking_edit), \
                mock.patch.object(
                    experiment_runner, "_evaluate",
                    return_value={"score": 1.0}) as evaluate, \
                mock.patch.object(
                    experiment_runner, "is_context_complete",
                    return_value=True), \
                mock.patch.object(experiment_runner, "dump_step_docs"), \
                mock.patch.object(
                    experiment_runner, "generate_response_id",
                    return_value="rid-fwd"), \
                mock.patch.object(
                    experiment_runner, "_row",
                    return_value={"evaluation": {"score": 1.0},
                                  "bdpatch": {}}), \
                mock.patch.object(
                    experiment_runner,
                    "append_relay_rows_and_checkpoint") as commit:

            def run_worker():
                try:
                    experiment_runner.run_relay(
                        "hybridpatch", "sample", num_round_trips=1,
                        include_distractor=True, out_dir=out_dir,
                        model="offline-test-model", max_tokens=16,
                        generate_fn=mock.Mock(), printing=False)
                except BaseException as exc:
                    runner_errors.append(exc)

            worker = threading.Thread(target=run_worker, daemon=True)
            worker.start()
            self.assertTrue(backward_entered.wait(5), runner_errors)
            run_meta.record_campaign_stop_condition(
                out_dir, "sibling_integrity_failure",
                sample="sibling")
            release_backward.set()
            worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(calls, ["forward", "backward"])
            self.assertEqual(evaluate.call_count, 1)
            commit.assert_not_called()
            self.assertEqual(len(runner_errors), 1)
            self.assertIsInstance(
                runner_errors[0], run_meta.CampaignStoppedError)
            self.assertRegex(
                str(runner_errors[0]),
                "campaign stop")
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "hybridpatch", "sample.jsonl")))
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "hybridpatch", "sample.ckpt.json")))

    def test_formal_runner_rejects_legacy_transport(self):
        with mock.patch.dict(
            os.environ, {
                "OPENCODE_TRANSPORT": "urllib_v1",
                "MINIMAX_TRANSPORT": "opencode",
            }, clear=False
        ):
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport("minimax-m3")
        with mock.patch.dict(
            os.environ, {
                "OPENCODE_TRANSPORT": "anthropic_sdk_v2",
                "MINIMAX_TRANSPORT": "official_nonstream",
            }, clear=False
        ):
            with self.assertRaises(RuntimeError):
                experiment_runner._require_formal_opencode_transport("minimax-m3")

    def test_empty_classification_uses_protocol_state_not_token_heuristic(self):
        classification, error_type = run_meta._empty_classification(
            {
                "stream_complete": True,
                "completion_tokens": 262144,
                "response_classification": "thinking_budget_exhausted",
            },
            "",
        )
        self.assertEqual(classification, "transport-valid but model-empty")
        self.assertEqual(error_type, "thinking_budget_exhausted")
        self.assertEqual(run_meta._empty_classification({}, "nonempty"), (None, None))

    def test_thinking_budget_empty_is_not_mislabeled_truncated_json(self):
        with tempfile.TemporaryDirectory() as out_dir:
            record = run_meta.record_model_content_anomaly(out_dir, {
                "sample_id": "translation4", "round_trip_num": 1,
                "round_trip_direction": "forward", "method": "hybridpatch",
                "raw_llm_response": "", "finish_reasons": ["max_tokens"],
                "response_classification": "thinking_budget_exhausted",
                "bdpatch": {"hybrid": {
                    "invalid_json": True,
                    "failed_step_kept_context": True,
                }},
            })
        self.assertIsNotNone(record)
        self.assertNotIn("finish_reason_max_tokens_truncated_json", record["error_type"])

    def test_hybrid_empty_and_non_protocol_outputs_do_not_trigger_repair(self):
        class DummyDomain:
            pass

        for raw, classification in (
            ("", "model_empty"),
            ("I cannot perform this task.", "normal"),
            ("Use {ordinary braces} in prose.", "normal"),
            ('```json\n{"answer":"ordinary data"}\n```', "normal"),
            ("I cannot perform this task; {no protocol was emitted}.",
             "normal"),
        ):
            calls = []

            def fake_generate(*_args, **kwargs):
                calls.append(kwargs.get("call_kind"))
                return {
                    "message": raw,
                    "stream_complete": True,
                    "response_classification": classification,
                    "finish_reason": "end_turn",
                }

            with mock.patch.object(experiment_runner, "build_hybrid_prompt", return_value="patch"):
                _raw, _gen, _meta, _log, _tag, _input, info = experiment_runner._edit_step(
                    "hybridpatch", DummyDomain(), "sample", "minimax-m3",
                    {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                    fake_generate, step_direction="forward",
                )
            self.assertEqual(calls, ["hybridpatch_primary"])
            self.assertFalse(info["repair"]["attempted"])

    def test_malformed_protocol_attempt_gets_one_semantic_repair(self):
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            return {
                "message": '{"protocol":"hybridpatch/2","ops":',
                "stream_complete": True,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        with mock.patch.object(experiment_runner, "build_hybrid_prompt", return_value="patch"):
            experiment_runner._edit_step(
                "hybridpatch", object(), "sample", "minimax-m3",
                {"a.txt": "x"}, {}, {"context": ["a.txt"]}, "edit", 16,
                fake_generate, step_direction="forward",
            )
        self.assertEqual(calls, ["hybridpatch_primary", "hybridpatch_repair"])

    def test_v8_soft_burden_executes_without_repair_and_records_telemetry(self):
        def envelope(ops):
            return {
                "protocol": "hybridpatch/8",
                "plan": {
                    "task_family": "precise replacement",
                    "edit_footprint": "few_precise_edits",
                },
                "action": {"route": "local_patch", "ops": ops},
            }

        tokens = [f"item_{i:02d}_token" for i in range(32)]
        primary_ops = [
            {"op": "replace", "file": "a.txt", "old_text": token,
             "new_text": token.upper()}
            for token in tokens
        ]
        primary = (
            "```json\n"
            + json.dumps(envelope(primary_ops), ensure_ascii=False)
            + "\n```"
        )
        calls = []

        def fake_generate(_messages, *_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            return {
                "message": primary,
                "completion_tokens": 1,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        current = {
            "a.txt": " ".join(tokens) + "\n",
            "unrelated.txt": "UNRELATED_EDITABLE_CONTENT\n",
            "reference.txt": "READONLY_CONTENT\n",
        }
        distractor = {"reference.txt": current["reference.txt"]}
        raw, generated, _meta, log, tag, input_real, info = experiment_runner._edit_step(
            "hybridpatch", object(), "sample", "minimax-m3",
            current, distractor, {"context": ["a.txt"]},
            "Replace old with new in a.txt.", 16, fake_generate,
            step_direction="backward",
        )
        self.assertEqual(calls, ["hybridpatch_primary"])
        self.assertEqual(generated, {"a.txt": " ".join(
            token.upper() for token in tokens) + "\n"})
        self.assertEqual(tag, "hybridpatch")
        self.assertEqual(log.preservation_violations, 0)
        self.assertEqual(raw, primary)
        self.assertEqual(log.ops_accepted, 32)
        self.assertIsNone(log.error)
        self.assertTrue(info["protocol_burden_exceeded"])
        self.assertEqual(info["local_op_count"], 32)
        self.assertEqual(info["bulk_op_count"], 0)
        self.assertEqual(info["explicit_op_count"], 32)
        self.assertGreater(info["anchor_bytes"], 0)
        self.assertGreater(info["envelope_bytes"], 0)
        self.assertEqual(info["explicit_block_id_count"], 0)
        self.assertEqual(info["touched_file_count"], 1)
        self.assertEqual(info["prompt_profile"], "default")
        self.assertEqual(info["prompt_classifier"], "operation_family_lexical/1")
        self.assertGreater(info["prompt_chars"], 0)
        self.assertEqual(info["repair_prompt_chars"], 0)
        self.assertEqual(info["call_budget"], {"primary_calls": 1, "repair_calls": 0})
        self.assertFalse(info["repair"]["attempted"])
        self.assertEqual(info["schema_error_count"], 0)
        self.assertIsNone(info["failure_reason"])
        self.assertEqual(
            info["protocol_burden_overages"]["local_op_count"],
            {"actual": 32, "threshold": 31},
        )
        self.assertEqual(
            info["protocol_burden_attempt_overages"]["primary"],
            info["protocol_burden_overages"],
        )

    def test_v8_below_burden_threshold_records_false(self):
        envelope = {
            "protocol": "hybridpatch/8",
            "plan": {
                "task_family": "precise replacement",
                "edit_footprint": "few_precise_edits",
            },
            "action": {"route": "local_patch", "ops": [{
                "op": "replace", "file": "a.txt", "old_text": "old",
                "new_text": "new",
            }]},
        }
        primary = "```json\n" + json.dumps(envelope) + "\n```"
        calls = []

        def fake_generate(*_args, **kwargs):
            calls.append(kwargs.get("call_kind"))
            return {
                "message": primary,
                "completion_tokens": 1,
                "response_classification": "normal",
                "finish_reason": "end_turn",
            }

        raw, generated, _meta, log, tag, _input, info = experiment_runner._edit_step(
            "hybridpatch", object(), "sample", "minimax-m3",
            {"a.txt": "old\n"}, {}, {"context": ["a.txt"]},
            "Replace old with new in a.txt.", 16, fake_generate,
            step_direction="backward",
        )
        self.assertEqual(calls, ["hybridpatch_primary"])
        self.assertEqual(raw, primary)
        self.assertEqual(generated, {"a.txt": "new\n"})
        self.assertEqual(tag, "hybridpatch")
        self.assertFalse(info["protocol_burden_exceeded"])
        self.assertEqual(info["protocol_burden_overages"], {})
        self.assertEqual(
            info["protocol_burden_attempt_overages"], {"primary": {}},
        )
        self.assertFalse(info["repair"]["attempted"])
        self.assertEqual(log.ops_accepted, 1)
        self.assertEqual(log.ops_rejected, 0)
        self.assertEqual(log.preservation_violations, 0)

    def test_run_metadata_v3_shares_campaign_times_and_rejects_identity_mix(self):
        kwargs = {
            "command": "python test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        commit = "1" * 40
        fingerprint = {"hybrid_schema.py": "abc123"}
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(run_meta, "_git_identity", return_value=(commit, "clean")), \
                mock.patch.object(run_meta, "code_fingerprint", return_value=fingerprint):
            first = run_meta.append_run_metadata(out_dir, **kwargs)
            reordered = dict(kwargs, methods=["fullrewrite", "hybridpatch"])
            second = run_meta.append_run_metadata(out_dir, **reordered)
            self.assertEqual(first["schema"], "anchorpatch.run_metadata/3")
            self.assertEqual(first["run_git_commit"], commit)
            self.assertEqual(first["git_tree_state"], "clean")
            self.assertNotIn("method_phase", first)
            self.assertEqual(first["started_at"], second["started_at"])
            self.assertIsNotNone(datetime.fromisoformat(first["started_at"]).tzinfo)

            run_meta.finish_run_metadata(out_dir, first["invocation_id"])
            records = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(record["finished_at"] is None for record in records))
            run_meta.finish_run_metadata(out_dir, second["invocation_id"])
            records = run_meta.read_run_metadata_snapshot(out_dir)
            finished = {record["finished_at"] for record in records}
            self.assertEqual(len(finished), 1)
            self.assertNotIn(None, finished)
            self.assertTrue(all(record["status"] == "finished" for record in records))

            with mock.patch.object(
                    run_meta, "_git_identity", return_value=("2" * 40, "clean")):
                with self.assertRaises(RuntimeError):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_git_identity", return_value=(commit, "dirty")):
                with self.assertRaises(RuntimeError):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            with self.assertRaises(RuntimeError):
                run_meta.append_run_metadata(out_dir, **dict(kwargs, seed=43))

    def test_run_metadata_event_ledger_reopens_from_receipted_prefix(self):
        kwargs = {
            "command": "python event-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 2,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "v1"}):
            first = run_meta.append_run_metadata(out_dir, **kwargs)
            events_path = os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            snapshot_path = os.path.join(out_dir, "run_metadata.jsonl")
            receipt_path = os.path.join(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)
            self.assertTrue(os.path.isfile(events_path))
            self.assertFalse(os.path.exists(snapshot_path))
            self.assertFalse(os.path.exists(receipt_path))

            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(
                plan_path, ["state-a", "state-b"])
            plan = run_meta.register_task_plan(
                out_dir, "sample", plan_path, num_round_trips=2)
            event_bytes_after_plan = pathlib.Path(events_path).read_bytes()
            self.assertEqual(
                run_meta.register_task_plan(
                    out_dir, "sample", plan_path, num_round_trips=2),
                plan,
            )
            self.assertEqual(
                pathlib.Path(events_path).read_bytes(), event_bytes_after_plan)

            run_meta.finish_run_metadata(out_dir, first["invocation_id"])
            prefix = pathlib.Path(events_path).read_bytes()
            cached_snapshot = pathlib.Path(snapshot_path).read_bytes()
            cached_receipt = pathlib.Path(receipt_path).read_bytes()

            second = run_meta.append_run_metadata(out_dir, **kwargs)
            self.assertTrue(pathlib.Path(events_path).read_bytes().startswith(prefix))
            self.assertEqual(pathlib.Path(snapshot_path).read_bytes(), cached_snapshot)
            self.assertEqual(pathlib.Path(receipt_path).read_bytes(), cached_receipt)
            live = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(len(live), 2)
            self.assertTrue(all(row["finished_at"] is None for row in live))
            self.assertTrue(all(row["task_plans"] == {"sample": plan}
                                for row in live))

            run_meta.finish_run_metadata(out_dir, second["invocation_id"])
            final = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(len(final), 2)
            self.assertTrue(all(row["status"] == "finished" for row in final))
            receipt = json.loads(pathlib.Path(receipt_path).read_text(
                encoding="utf-8"))
            self.assertEqual(
                receipt["event_prefix_size_bytes"],
                os.path.getsize(events_path),
            )
            self.assertEqual(
                run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                final,
            )

    def test_run_metadata_event_snapshot_failure_keeps_authoritative_terminal(self):
        kwargs = {
            "command": "python event-failure-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "v1"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            with self.assertRaises(ValueError):
                run_meta.finish_run_metadata(
                    out_dir, invocation["invocation_id"], status="bogus")
            with mock.patch.object(
                    run_meta, "_write_jsonl_atomic",
                    side_effect=OSError("injected snapshot replace failure")):
                with self.assertRaisesRegex(RuntimeError, "retry is safe"):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            terminal = run_meta.finish_run_metadata(
                out_dir, invocation["invocation_id"])
            self.assertEqual(terminal["status"], "finished")
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, "run_metadata.jsonl")))
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)))
            events = run_meta._read_jsonl_records_with_retry(os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME))
            self.assertEqual(
                [event["event"] for event in events],
                ["invocation_registered", "invocation_terminal"],
            )
            self.assertEqual(
                run_meta.read_run_metadata_snapshot(out_dir)[0]["status"],
                "finished",
            )
            self.assertEqual(
                run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                [terminal],
            )
            before_retry = pathlib.Path(
                out_dir, run_meta.METADATA_EVENTS_FILENAME).read_bytes()
            self.assertEqual(
                run_meta.finish_run_metadata(
                    out_dir, invocation["invocation_id"]),
                terminal,
            )
            self.assertEqual(
                pathlib.Path(
                    out_dir, run_meta.METADATA_EVENTS_FILENAME).read_bytes(),
                before_retry,
            )

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "v1"}):
            first = run_meta.append_run_metadata(out_dir, **kwargs)
            run_meta.finish_run_metadata(out_dir, first["invocation_id"])
            snapshot_path = pathlib.Path(out_dir, "run_metadata.jsonl")
            receipt_path = pathlib.Path(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)
            old_snapshot = snapshot_path.read_bytes()
            old_receipt = receipt_path.read_bytes()
            second = run_meta.append_run_metadata(out_dir, **kwargs)
            real_write_json = run_meta.write_json_atomic

            def fail_receipt(path, record):
                if os.path.abspath(path) == os.path.abspath(receipt_path):
                    raise OSError("injected receipt replace failure")
                return real_write_json(path, record)

            with mock.patch.object(
                    run_meta, "write_json_atomic", side_effect=fail_receipt):
                with self.assertRaisesRegex(RuntimeError, "retry is safe"):
                    run_meta.finish_run_metadata(
                        out_dir, second["invocation_id"])
            terminal = run_meta.finish_run_metadata(
                out_dir, second["invocation_id"])
            self.assertEqual(terminal["status"], "finished")
            self.assertNotEqual(snapshot_path.read_bytes(), old_snapshot)
            self.assertNotEqual(receipt_path.read_bytes(), old_receipt)
            self.assertTrue(all(
                row["status"] == "finished"
                for row in run_meta.read_run_metadata_snapshot(out_dir)
            ))
            self.assertEqual(
                run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                run_meta.read_run_metadata_snapshot(out_dir),
            )

    def test_run_metadata_event_hard_crash_publication_is_reentrant(self):
        kwargs = {
            "command": "python event-hard-crash-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "hard-crash"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            real_write_json = run_meta.write_json_atomic

            def crash_before_receipt(path, record):
                if path.endswith(run_meta.METADATA_PROJECTION_RECEIPT_FILENAME):
                    raise SystemExit("injected process death")
                return real_write_json(path, record)

            with mock.patch.object(
                    run_meta, "write_json_atomic",
                    side_effect=crash_before_receipt):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)))
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, "run_metadata.jsonl")))
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)))
            self.assertEqual(
                run_meta.finish_run_metadata(
                    out_dir, invocation["invocation_id"])["status"],
                "finished",
            )
            self.assertEqual(
                run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                run_meta.read_run_metadata_snapshot(out_dir),
            )

            reopened = run_meta.append_run_metadata(out_dir, **kwargs)
            run_meta.finish_run_metadata(out_dir, reopened["invocation_id"])
            self.assertEqual(
                len(run_meta.read_quiescent_run_metadata_snapshot(out_dir)), 2)

    def test_run_metadata_event_pending_recovers_every_byte_cut_once(self):
        kwargs = {
            "command": "python event-byte-cut-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        fixed_now = datetime.fromisoformat("2026-07-29T00:00:00+00:00")

        def crash_after_prefix(events_path, event_bytes, *, cut):
            with open(events_path, "ab") as handle:
                handle.write(event_bytes[:cut])
                handle.flush()
                os.fsync(handle.fileno())
            raise SystemExit(f"injected event append death at byte {cut}")

        # Discover the exact terminal event length from the durable intent. The
        # fixed timestamp and fixed-width invocation UUID make it stable across
        # every isolated subcase below.
        with tempfile.TemporaryDirectory() as probe_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "byte-cut"}), mock.patch.object(
                        run_meta, "_aware_now", return_value=fixed_now):
            invocation = run_meta.append_run_metadata(probe_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_append_run_metadata_event_bytes",
                    side_effect=lambda path, payload: crash_after_prefix(
                        path, payload, cut=0)):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        probe_dir, invocation["invocation_id"])
            pending = json.loads(pathlib.Path(
                probe_dir, run_meta.METADATA_EVENT_PENDING_FILENAME
            ).read_text(encoding="utf-8"))
            terminal_size = len(pending["event_line"].encode("utf-8"))

        for cut in range(terminal_size + 1):
            with self.subTest(cut=cut), tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.object(
                        run_meta, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value={"event": "byte-cut"}), \
                    mock.patch.object(
                        run_meta, "_aware_now", return_value=fixed_now):
                invocation = run_meta.append_run_metadata(out_dir, **kwargs)
                with mock.patch.object(
                        run_meta, "_append_run_metadata_event_bytes",
                        side_effect=lambda path, payload, cut=cut:
                            crash_after_prefix(path, payload, cut=cut)):
                    with self.assertRaises(SystemExit):
                        run_meta.finish_run_metadata(
                            out_dir, invocation["invocation_id"])
                pending_path = pathlib.Path(
                    out_dir, run_meta.METADATA_EVENT_PENDING_FILENAME)
                self.assertTrue(pending_path.is_file())
                terminal = run_meta.finish_run_metadata(
                    out_dir, invocation["invocation_id"])
                self.assertEqual(terminal["status"], "finished")
                self.assertFalse(pending_path.exists())
                events = run_meta._read_jsonl_records_with_retry(os.path.join(
                    out_dir, run_meta.METADATA_EVENTS_FILENAME))
                self.assertEqual(
                    [event["event"] for event in events],
                    ["invocation_registered", "invocation_terminal"],
                )
                recovery_paths = list(pathlib.Path(
                    out_dir, run_meta.METADATA_EVENT_RECOVERY_DIRECTORY
                ).glob("*.json"))
                self.assertEqual(len(recovery_paths), 1)
                recovery = json.loads(recovery_paths[0].read_text(
                    encoding="utf-8"))
                self.assertEqual(recovery["state"], "completed")
                self.assertEqual(
                    recovery["observed_suffix_size_bytes"], cut)
                self.assertEqual(
                    run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                    [terminal],
                )

    def test_run_metadata_event_pending_recovery_is_reentrant_at_both_barriers(self):
        kwargs = {
            "command": "python event-recovery-barrier-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        fixed_now = datetime.fromisoformat("2026-07-29T00:00:00+00:00")

        def seed_partial(events_path, event_bytes):
            with open(events_path, "ab") as handle:
                handle.write(event_bytes[:7])
                handle.flush()
                os.fsync(handle.fileno())
            raise SystemExit("injected partial event")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "barrier"}), mock.patch.object(
                        run_meta, "_aware_now", return_value=fixed_now):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_append_run_metadata_event_bytes",
                    side_effect=seed_partial):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])

            real_write_json = run_meta.write_json_atomic

            def crash_before_completed_receipt(path, record):
                if (isinstance(record, dict)
                        and record.get("schema")
                        == run_meta.METADATA_EVENT_RECOVERY_SCHEMA
                        and record.get("state") == "completed"):
                    raise SystemExit("injected death after ledger completion")
                return real_write_json(path, record)

            with mock.patch.object(
                    run_meta, "write_json_atomic",
                    side_effect=crash_before_completed_receipt):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            recovery_path = next(pathlib.Path(
                out_dir, run_meta.METADATA_EVENT_RECOVERY_DIRECTORY
            ).glob("*.json"))
            prepared = json.loads(recovery_path.read_text(encoding="utf-8"))
            self.assertEqual(prepared["state"], "prepared")
            self.assertEqual(prepared["observed_suffix_size_bytes"], 7)
            recovery_path.unlink()

            real_clear = run_meta._clear_run_metadata_event_pending

            def crash_before_pending_clear(_out_dir):
                raise SystemExit("injected death after completed receipt")

            with mock.patch.object(
                    run_meta, "_clear_run_metadata_event_pending",
                    side_effect=crash_before_pending_clear):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            completed = json.loads(recovery_path.read_text(encoding="utf-8"))
            self.assertEqual(completed["state"], "completed")
            self.assertEqual(completed["observed_suffix_size_bytes"], 7)
            self.assertTrue(pathlib.Path(
                out_dir, run_meta.METADATA_EVENT_PENDING_FILENAME).exists())
            recovery_path.unlink()

            with mock.patch.object(
                    run_meta, "_clear_run_metadata_event_pending",
                    side_effect=real_clear):
                terminal = run_meta.finish_run_metadata(
                    out_dir, invocation["invocation_id"])
            self.assertEqual(terminal["status"], "finished")
            recreated = json.loads(recovery_path.read_text(encoding="utf-8"))
            self.assertEqual(recreated["state"], "completed")
            self.assertEqual(recreated["observed_suffix_size_bytes"], 7)
            self.assertFalse(pathlib.Path(
                out_dir, run_meta.METADATA_EVENT_PENDING_FILENAME).exists())
            events = run_meta._read_jsonl_records_with_retry(os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME))
            self.assertEqual(len(events), 2)
            self.assertEqual(
                run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                [terminal],
            )

    def test_public_projection_reconcile_completes_pending_event_cuts(self):
        kwargs = {
            "command": "python public-reconcile-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }

        def crash_writer(events_path, event_bytes, *, cut):
            with open(events_path, "ab") as handle:
                handle.write(event_bytes[:cut])
                handle.flush()
                os.fsync(handle.fileno())
            raise SystemExit("injected pending projection event")

        for cut in (0, 7):
            with self.subTest(cut=cut), tempfile.TemporaryDirectory() as out_dir, \
                    mock.patch.object(
                        run_meta, "_git_identity",
                        return_value=("1" * 40, "clean")), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value={"event": "public-reconcile"}):
                invocation = run_meta.append_run_metadata(out_dir, **kwargs)
                with mock.patch.object(
                        run_meta, "_append_run_metadata_event_bytes",
                        side_effect=lambda path, payload, cut=cut:
                            crash_writer(path, payload, cut=cut)):
                    with self.assertRaises(SystemExit):
                        run_meta.finish_run_metadata(
                            out_dir, invocation["invocation_id"])
                self.assertTrue(run_meta.reconcile_run_metadata_projection(out_dir))
                self.assertFalse(pathlib.Path(
                    out_dir, run_meta.METADATA_EVENT_PENDING_FILENAME).exists())
                rows = run_meta.read_quiescent_run_metadata_snapshot(out_dir)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["status"], "finished")
                events = run_meta._read_jsonl_records_with_retry(os.path.join(
                    out_dir, run_meta.METADATA_EVENTS_FILENAME))
                self.assertEqual(len(events), 2)

    def test_recovery_receipts_are_strict_and_projection_bound(self):
        kwargs = {
            "command": "python recovery-receipt-binding-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }

        def seed_partial(events_path, event_bytes):
            with open(events_path, "ab") as handle:
                handle.write(event_bytes[:7])
                handle.flush()
                os.fsync(handle.fileno())
            raise SystemExit("injected partial event")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "receipt-binding"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_append_run_metadata_event_bytes",
                    side_effect=seed_partial):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            terminal = run_meta.finish_run_metadata(
                out_dir, invocation["invocation_id"])
            self.assertEqual(terminal["status"], "finished")
            recovery_path = next(pathlib.Path(
                out_dir, run_meta.METADATA_EVENT_RECOVERY_DIRECTORY
            ).glob("*.json"))
            projection_receipt_path = pathlib.Path(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)
            projection_receipt = json.loads(
                projection_receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(
                projection_receipt["event_recovery_receipt_count"], 1)
            original = recovery_path.read_bytes()

            recovery_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(
                    RuntimeError, "recovery receipt is invalid"):
                run_meta.read_quiescent_run_metadata_snapshot(out_dir)
            recovery_path.write_bytes(original)

            events_path = pathlib.Path(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            receipt = json.loads(original.decode("utf-8"))
            receipt["pending_sha256"] = "0" * 64
            run_meta.write_json_atomic(recovery_path, receipt)
            with self.assertRaisesRegex(
                    RuntimeError, "pending identity is invalid"):
                run_meta._read_run_metadata_event_recovery_receipts(
                    out_dir, events_path)
            recovery_path.write_bytes(original)

            receipt = json.loads(original.decode("utf-8"))
            ledger = events_path.read_bytes()
            receipt["prior_prefix_size_bytes"] += 1
            prior_size = receipt["prior_prefix_size_bytes"]
            final_size = receipt["final_ledger_size_bytes"]
            shifted_event = ledger[prior_size:final_size]
            receipt["prior_prefix_sha256"] = hashlib.sha256(
                ledger[:prior_size]).hexdigest()
            receipt["event_sha256"] = hashlib.sha256(
                shifted_event).hexdigest()
            receipt["observed_suffix_size_bytes"] = min(
                receipt["observed_suffix_size_bytes"], len(shifted_event))
            receipt["observed_suffix_sha256"] = hashlib.sha256(
                shifted_event[:receipt["observed_suffix_size_bytes"]]
            ).hexdigest()
            reconstructed_pending = {
                "schema": run_meta.METADATA_EVENT_PENDING_SCHEMA,
                "events_file": run_meta.METADATA_EVENTS_FILENAME,
                "event_schema": run_meta.METADATA_EVENT_SCHEMA,
                "prior_prefix_size_bytes": prior_size,
                "prior_prefix_sha256": receipt["prior_prefix_sha256"],
                "prior_event_count": receipt["prior_event_count"],
                "event_index": receipt["event_index"],
                "event_line": shifted_event.decode("utf-8"),
                "event_sha256": receipt["event_sha256"],
                "recovery_observation": {
                    "prepared_at": receipt["prepared_at"],
                    "observed_suffix_size_bytes": receipt[
                        "observed_suffix_size_bytes"],
                    "observed_suffix_sha256": receipt[
                        "observed_suffix_sha256"],
                },
            }
            receipt["pending_sha256"] = run_meta._canonical_record_sha256(
                reconstructed_pending)
            shifted_path = recovery_path.with_name(
                f"{receipt['event_index']:08d}_"
                f"{receipt['event_sha256'][:16]}.json")
            recovery_path.unlink()
            run_meta.write_json_atomic(shifted_path, receipt)
            with self.assertRaisesRegex(
                    RuntimeError, "prefix|uncommitted"):
                run_meta._read_run_metadata_event_recovery_receipts(
                    out_dir, events_path)
            shifted_path.unlink()
            recovery_path.write_bytes(original)
            self.assertEqual(
                run_meta.read_quiescent_run_metadata_snapshot(out_dir),
                [terminal],
            )

            recovery_path.unlink()
            with self.assertRaisesRegex(RuntimeError, "receipt is invalid"):
                run_meta.read_quiescent_run_metadata_snapshot(out_dir)
            recovery_path.write_bytes(original)

            extra = recovery_path.with_name("00000099_deadbeefdeadbeef.json")
            extra.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(
                    RuntimeError, "recovery receipt is invalid"):
                run_meta.read_quiescent_run_metadata_snapshot(out_dir)
            extra.unlink()

            prepared = json.loads(recovery_path.read_text(encoding="utf-8"))
            prepared["state"] = "prepared"
            prepared["completed_at"] = None
            run_meta.write_json_atomic(recovery_path, prepared)
            with self.assertRaisesRegex(
                    RuntimeError, "recovery receipt is invalid"):
                run_meta.read_quiescent_run_metadata_snapshot(out_dir)

    def test_run_metadata_event_pending_tamper_and_divergence_fail_closed(self):
        kwargs = {
            "command": "python event-pending-corruption-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }

        def stop_before_event(_events_path, _event_bytes):
            raise SystemExit("injected event stop")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "pending-corruption"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_append_run_metadata_event_bytes",
                    side_effect=stop_before_event):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            pending_path = pathlib.Path(
                out_dir, run_meta.METADATA_EVENT_PENDING_FILENAME)
            pending = json.loads(pending_path.read_text(encoding="utf-8"))
            pending["event_sha256"] = "0" * 64
            run_meta.write_json_atomic(pending_path, pending)
            with self.assertRaisesRegex(RuntimeError, "pending intent is invalid"):
                run_meta.read_run_metadata_snapshot(out_dir)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "pending-divergence"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_append_run_metadata_event_bytes",
                    side_effect=stop_before_event):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            with open(os.path.join(
                    out_dir, run_meta.METADATA_EVENTS_FILENAME), "ab") as handle:
                handle.write(b"X")
                handle.flush()
                os.fsync(handle.fileno())
            with self.assertRaisesRegex(RuntimeError, "suffix diverged"):
                run_meta.read_run_metadata_snapshot(out_dir)

        def seed_seven_bytes(events_path, event_bytes):
            with open(events_path, "ab") as handle:
                handle.write(event_bytes[:7])
                handle.flush()
                os.fsync(handle.fileno())
            raise SystemExit("injected partial event")

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "completed-regression"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            with mock.patch.object(
                    run_meta, "_append_run_metadata_event_bytes",
                    side_effect=seed_seven_bytes):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])

            def stop_after_completed_receipt(_out_dir):
                raise SystemExit("injected stop before pending unlink")

            with mock.patch.object(
                    run_meta, "_clear_run_metadata_event_pending",
                    side_effect=stop_after_completed_receipt):
                with self.assertRaises(SystemExit):
                    run_meta.finish_run_metadata(
                        out_dir, invocation["invocation_id"])
            pending = json.loads(pathlib.Path(
                out_dir, run_meta.METADATA_EVENT_PENDING_FILENAME
            ).read_text(encoding="utf-8"))
            events_path = pathlib.Path(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            events_path.write_bytes(
                events_path.read_bytes()[:
                    pending["prior_prefix_size_bytes"] + 7
                ]
            )
            with self.assertRaisesRegex(
                    RuntimeError, "regressed after completed recovery"):
                run_meta.read_run_metadata_snapshot(out_dir)

    def test_run_metadata_legacy_v3_passthrough_never_creates_events(self):
        kwargs = {
            "command": "python legacy-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"legacy": "v3"}):
            pathlib.Path(out_dir, "run_metadata.jsonl").write_text(
                "", encoding="utf-8")
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)))
            self.assertEqual(
                run_meta.read_run_metadata_snapshot(out_dir)[0]["invocation_id"],
                invocation["invocation_id"],
            )
            run_meta.finish_run_metadata(out_dir, invocation["invocation_id"])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)))
            reopened = run_meta.append_run_metadata(out_dir, **kwargs)
            run_meta.finish_run_metadata(out_dir, reopened["invocation_id"])
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)))
            self.assertTrue(all(
                row["status"] == "finished"
                for row in run_meta.read_quiescent_run_metadata_snapshot(out_dir)
            ))

    def test_run_metadata_event_concurrent_registration_and_finish(self):
        kwargs = {
            "command": "python concurrent-event-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "v1"}):
            invocations = []
            errors = []

            def register():
                try:
                    invocations.append(
                        run_meta.append_run_metadata(out_dir, **kwargs))
                except Exception as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            threads = [threading.Thread(target=register) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            self.assertEqual(len(invocations), 4)
            self.assertEqual(
                len(run_meta.read_run_metadata_snapshot(out_dir)), 4)

            def finish(invocation_id):
                try:
                    run_meta.finish_run_metadata(out_dir, invocation_id)
                except Exception as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            threads = [
                threading.Thread(
                    target=finish, args=(record["invocation_id"],))
                for record in invocations
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            events = run_meta._read_jsonl_records_with_retry(os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME))
            self.assertEqual(
                [event["event_index"] for event in events],
                list(range(1, 9)),
            )
            final = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertTrue(all(record["status"] == "finished"
                                for record in final))
            self.assertEqual(
                {record["finished_at"] for record in final},
                {max(
                    (record["invocation_finished_at"] for record in final),
                    key=datetime.fromisoformat,
                )},
            )

    def test_run_metadata_event_interrupt_and_audited_cas(self):
        kwargs = {
            "command": "python event-interrupt-test",
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "interrupt"}):
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-a"},
                    clear=False):
                first = run_meta.append_run_metadata(
                    out_dir, samples=["sample-a"], **kwargs)
            with mock.patch.dict(
                    os.environ,
                    {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-b"},
                    clear=False):
                second = run_meta.append_run_metadata(
                    out_dir, samples=["sample-b"], **kwargs)
            changed = run_meta.interrupt_running_invocations(
                out_dir,
                status="interrupted_by_dispatcher",
                worker_launch_ids={"worker-a"},
            )
            self.assertEqual(
                [item["worker_launch_id"] for item in changed], ["worker-a"])
            live = {
                row["worker_launch_id"]: row
                for row in run_meta.read_run_metadata_snapshot(out_dir)
            }
            self.assertEqual(live["worker-a"]["status"],
                             "interrupted_by_dispatcher")
            self.assertEqual(live["worker-b"]["status"], "running")
            self.assertFalse(os.path.exists(os.path.join(
                out_dir, "run_metadata.jsonl")))
            run_meta.finish_run_metadata(out_dir, second["invocation_id"])
            self.assertTrue(os.path.isfile(os.path.join(
                out_dir, "run_metadata.jsonl")))

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "cas"}), mock.patch.dict(
                        os.environ,
                        {"ANCHORPATCH_WORKER_LAUNCH_ID": "worker-c"},
                        clear=False):
            record = run_meta.append_run_metadata(
                out_dir, samples=["sample-c"], **kwargs)
            events_path = os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            before = pathlib.Path(events_path).read_bytes()
            audited = [{
                "invocation_id": record["invocation_id"],
                "worker_launch_id": "worker-c",
                "worker_pid": record["worker_pid"] + 1,
                "sample": "sample-c",
            }]
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                run_meta.interrupt_audited_running_invocations(
                    out_dir,
                    status="interrupted_by_dispatcher",
                    audited=audited,
                )
            self.assertEqual(pathlib.Path(events_path).read_bytes(), before)
            audited[0]["worker_pid"] = record["worker_pid"]
            run_meta.interrupt_audited_running_invocations(
                out_dir,
                status="interrupted_by_dispatcher",
                audited=audited,
            )
            final = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(final[0]["status"], "interrupted_by_dispatcher")

    def test_run_metadata_event_corruption_matrix_fails_closed(self):
        kwargs = {
            "command": "python event-corruption-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "corruption"}):
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            events_path = os.path.join(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            registered = run_meta._read_jsonl_records_with_retry(events_path)[0]
            missing_campaign = copy.deepcopy(registered)
            missing_campaign["record"].pop("campaign_config")
            drifted_record = copy.deepcopy(registered["record"])
            drifted_record.update({
                "invocation_id": "drifted-invocation",
                "run_git_commit": "2" * 40,
                "code_fingerprint": {"event": "drifted"},
                "campaign_recovery_authorization": None,
            })
            wrong_boundary_record = copy.deepcopy(drifted_record)
            wrong_boundary_record["campaign_recovery_authorization"] = {
                "authorization_id": "wrong-boundary",
                "authorization_sha256": "a" * 64,
                "prior_git_commit": "3" * 40,
                "recovery_git_commit": "2" * 40,
            }
            task_plan = {"sha256": "a" * 64, "round_trips": 1}
            mutations = {
                "float_index": [{**registered, "event_index": 1.0}],
                "index_gap": [{**registered, "event_index": 2}],
                "unknown_event": [{**registered, "event": "unknown"}],
                "extra_field": [{**registered, "extra": True}],
                "missing_campaign_config": [missing_campaign],
                "duplicate_registration": [
                    registered, {**registered, "event_index": 2}],
                "identity_drift_without_boundary": [
                    registered, {
                        "schema": run_meta.METADATA_EVENT_SCHEMA,
                        "event": "invocation_registered",
                        "event_index": 2,
                        "record": drifted_record,
                    },
                ],
                "identity_drift_wrong_boundary": [
                    registered, {
                        "schema": run_meta.METADATA_EVENT_SCHEMA,
                        "event": "invocation_registered",
                        "event_index": 2,
                        "record": wrong_boundary_record,
                    },
                ],
                "foreign_task_plan": [
                    registered, {
                        "schema": run_meta.METADATA_EVENT_SCHEMA,
                        "event": "task_plan_registered",
                        "event_index": 2,
                        "sample_id": "foreign",
                        "task_plan": task_plan,
                    },
                ],
                "wrong_task_plan_round_trips": [
                    registered, {
                        "schema": run_meta.METADATA_EVENT_SCHEMA,
                        "event": "task_plan_registered",
                        "event_index": 2,
                        "sample_id": "sample",
                        "task_plan": {**task_plan, "round_trips": 2},
                    },
                ],
                "terminal_before_start": [
                    registered, {
                        "schema": run_meta.METADATA_EVENT_SCHEMA,
                        "event": "invocation_terminal",
                        "event_index": 2,
                        "invocation_id": registered["record"]["invocation_id"],
                        "status": "finished",
                        "finished_at": "2000-01-01T00:00:00+00:00",
                    },
                ],
            }
            for label, events in mutations.items():
                with self.subTest(label=label), self.assertRaises(RuntimeError):
                    run_meta._fold_run_metadata_events(events)

            run_meta.finish_run_metadata(out_dir, invocation["invocation_id"])
            complete = run_meta._read_jsonl_records_with_retry(events_path)
            duplicate_terminal = dict(complete[-1], event_index=3)
            with self.assertRaises(RuntimeError):
                run_meta._fold_run_metadata_events(
                    complete + [duplicate_terminal])
            post_terminal_plan = {
                "schema": run_meta.METADATA_EVENT_SCHEMA,
                "event": "task_plan_registered",
                "event_index": 3,
                "sample_id": "sample",
                "task_plan": task_plan,
            }
            with self.assertRaises(RuntimeError):
                run_meta._fold_run_metadata_events(
                    complete + [post_terminal_plan])

            receipt_path = os.path.join(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME)
            receipt = json.loads(pathlib.Path(receipt_path).read_text(
                encoding="utf-8"))
            receipt["projection_sha256"] = "0" * 64
            run_meta.write_json_atomic(receipt_path, receipt)
            with self.assertRaisesRegex(
                    RuntimeError, "receipt is invalid|snapshot drifted"):
                run_meta.read_run_metadata_snapshot(out_dir)

        with tempfile.TemporaryDirectory() as out_dir:
            run_meta.write_json_atomic(
                os.path.join(
                    out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME),
                {},
            )
            with self.assertRaisesRegex(RuntimeError, "without event ledger"):
                run_meta.read_run_metadata_snapshot(out_dir)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "corruption"}):
            run_meta.append_run_metadata(out_dir, **kwargs)
            run_meta._write_jsonl_atomic(
                os.path.join(out_dir, "run_metadata.jsonl"),
                run_meta.read_run_metadata_snapshot(out_dir),
            )
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                run_meta.read_run_metadata_snapshot(out_dir)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "corruption"}):
            run_meta.append_run_metadata(out_dir, **kwargs)
            with open(os.path.join(
                    out_dir, run_meta.METADATA_EVENTS_FILENAME), "ab") as handle:
                handle.write(b'{"torn":')
            with self.assertRaisesRegex(
                    RuntimeError, "uncommitted tail|invalid JSONL record"):
                run_meta.read_run_metadata_snapshot(out_dir)

        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "uncommitted"}):
            run_meta.append_run_metadata(out_dir, **kwargs)
            events_path = pathlib.Path(
                out_dir, run_meta.METADATA_EVENTS_FILENAME)
            events_path.write_bytes(events_path.read_bytes().rstrip(b"\n"))
            with self.assertRaisesRegex(RuntimeError, "uncommitted"):
                run_meta.read_run_metadata_snapshot(out_dir)

    def test_run_metadata_event_manifest_prevents_legacy_downgrade(self):
        kwargs = {
            "command": "python event-mode-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"event": "mode"}):
            run_meta.write_json_atomic(
                os.path.join(out_dir, "dispatch_manifest.json"), {
                    "config": {
                        "run_metadata_storage": (
                            run_meta.RUN_METADATA_STORAGE_EVENT_V1),
                    },
                })
            invocation = run_meta.append_run_metadata(out_dir, **kwargs)
            run_meta.finish_run_metadata(out_dir, invocation["invocation_id"])
            os.unlink(os.path.join(out_dir, run_meta.METADATA_EVENTS_FILENAME))
            os.unlink(os.path.join(
                out_dir, run_meta.METADATA_PROJECTION_RECEIPT_FILENAME))
            with self.assertRaisesRegex(RuntimeError, "requires a missing"):
                run_meta.read_run_metadata_snapshot(out_dir)
            with self.assertRaisesRegex(RuntimeError, "requires a missing"):
                run_meta.read_quiescent_run_metadata_snapshot(out_dir)
            with self.assertRaisesRegex(RuntimeError, "requires a missing"):
                run_meta.append_run_metadata(out_dir, **kwargs)

    def test_run_metadata_accepts_hp_then_fr_phases_without_changing_legacy(self):
        kwargs = {
            "command": "python phased-test",
            "samples": ["sample"],
            "num_round_trips": 10,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": True,
            "max_tokens": 16,
            "printing": False,
        }
        phase_env = {
            "ANCHORPATCH_CAMPAIGN_METHOD_SET": "hybridpatch,fullrewrite",
            "ANCHORPATCH_METHOD_PHASE": "hybridpatch",
            "ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE": json.dumps({
                "schema": "anchorpatch.interrupted_phase_resume/1",
                "sample": "sample",
                "method_phase": "hybridpatch",
                "prior_invocation_id": "prior-invocation",
                "task_plan_sha256": "a" * 64,
                "checkpoint_progress": {
                    "hybridpatch": {
                        "completed_round_trips": 1,
                        "committed_rows": 2,
                    }
                },
            }),
        }
        with tempfile.TemporaryDirectory() as out_dir, mock.patch.object(
                run_meta, "_git_identity",
                return_value=("1" * 40, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value={"unit": "test"}), mock.patch.dict(
                        os.environ, phase_env, clear=False):
            hp = run_meta.append_run_metadata(
                out_dir, methods=["hybridpatch"], **kwargs)
            run_meta.finish_run_metadata(out_dir, hp["invocation_id"])
            os.environ["ANCHORPATCH_METHOD_PHASE"] = "fullrewrite"
            os.environ.pop("ANCHORPATCH_INTERRUPTED_RESUME_EVIDENCE", None)
            fr = run_meta.append_run_metadata(
                out_dir, methods=["fullrewrite"], **kwargs)
            run_meta.finish_run_metadata(out_dir, fr["invocation_id"])
        self.assertEqual(hp["method_phase"], "hybridpatch")
        self.assertEqual(fr["method_phase"], "fullrewrite")
        self.assertEqual(
            hp["interrupted_resume_authorization"]["prior_invocation_id"],
            "prior-invocation")
        self.assertNotIn("interrupted_resume_authorization", fr)
        self.assertEqual(
            hp["campaign_config"]["method_set"],
            ["fullrewrite", "hybridpatch"])
        self.assertEqual(hp["campaign_config"], fr["campaign_config"])

    def test_run_metadata_records_exact_authorized_git_recovery_boundary(self):
        kwargs = {
            "command": "python test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        prior_commit = "1" * 40
        recovery_commit = "2" * 40
        prior_fingerprint = {"run_meta.py": "old", "hybrid_schema.py": "same"}
        recovery_fingerprint = {
            "run_meta.py": "new", "hybrid_schema.py": "same"}
        authorization = {
            "authorization_id": "test-recovery",
            "authorization_sha256": "a" * 64,
            "prior_git_commit": prior_commit,
            "recovery_git_commit": recovery_commit,
            "prior_code_fingerprint": prior_fingerprint,
            "recovery_code_fingerprint": recovery_fingerprint,
            "archived_stop_path": "recovery_history/stop.json",
            "archived_stop_sha256": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(prior_commit, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=prior_fingerprint), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=None):
                first = run_meta.append_run_metadata(out_dir, **kwargs)
                run_meta.finish_run_metadata(out_dir, first["invocation_id"])
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(recovery_commit, "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=recovery_fingerprint), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=authorization):
                recovered = run_meta.append_run_metadata(out_dir, **kwargs)
                second_recovered = run_meta.append_run_metadata(
                    out_dir, **kwargs)
            boundary = recovered["campaign_recovery_authorization"]
            self.assertEqual(boundary["authorization_id"], "test-recovery")
            self.assertEqual(boundary["prior_git_commit"], prior_commit)
            self.assertEqual(boundary["recovery_git_commit"], recovery_commit)
            self.assertEqual(recovered["run_git_commit"], recovery_commit)
            self.assertEqual(
                second_recovered["run_git_commit"], recovery_commit)

    def test_run_metadata_accepts_sha_pinned_chained_recovery_identities(self):
        kwargs = {
            "command": "python chained-recovery-test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 1,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": False,
            "max_tokens": 16,
            "printing": False,
        }
        commits = [character * 40 for character in "123"]
        fingerprints = [
            {"run_meta.py": f"revision-{index}"}
            for index in range(3)
        ]

        def authorization(index, history):
            return {
                "authorization_id": f"recovery-{index}",
                "authorization_sha256": str(index) * 64,
                "prior_git_commit": commits[0],
                "recovery_git_commit": commits[index],
                "prior_code_fingerprint": fingerprints[0],
                "recovery_code_fingerprint": fingerprints[index],
                "archived_stop_path": f"recovery-{index}/stop.json",
                "archived_stop_sha256": "f" * 64,
                "recovery_identity_history": history,
            }

        base_identity = {
            "authorization_id": None,
            "authorization_sha256": None,
            "run_git_commit": commits[0],
            "git_tree_state": "clean",
            "code_fingerprint": fingerprints[0],
        }
        first_recovery_identity = {
            "authorization_id": "recovery-1",
            "authorization_sha256": "1" * 64,
            "run_git_commit": commits[1],
            "git_tree_state": "clean",
            "code_fingerprint": fingerprints[1],
        }
        current_recovery_identity = {
            "authorization_id": "recovery-2",
            "authorization_sha256": "2" * 64,
            "run_git_commit": commits[2],
            "git_tree_state": "clean",
            "code_fingerprint": fingerprints[2],
        }
        with tempfile.TemporaryDirectory() as out_dir:
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(commits[0], "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=fingerprints[0]), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=None):
                original = run_meta.append_run_metadata(out_dir, **kwargs)
                run_meta.finish_run_metadata(
                    out_dir, original["invocation_id"])

            first_authorization = authorization(
                1, [base_identity, first_recovery_identity])
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(commits[1], "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=fingerprints[1]), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=first_authorization):
                first_recovery = run_meta.append_run_metadata(
                    out_dir, **kwargs)
                run_meta.finish_run_metadata(
                    out_dir, first_recovery["invocation_id"])

            current_authorization = authorization(
                2, [
                    base_identity,
                    first_recovery_identity,
                    current_recovery_identity,
                ])
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=(commits[2], "clean")), mock.patch.object(
                    run_meta, "code_fingerprint",
                    return_value=fingerprints[2]), mock.patch.object(
                    run_meta, "read_campaign_recovery_authorization",
                    return_value=current_authorization):
                latest = run_meta.append_run_metadata(out_dir, **kwargs)

        self.assertEqual(latest["run_git_commit"], commits[2])
        self.assertEqual(
            latest["campaign_recovery_authorization"]["authorization_id"],
            "recovery-2",
        )

    def test_recovery_incident_evidence_marks_preauthorization_workers(self):
        authorization = {
            "recovery_kind": run_meta.LEDGER_LOCK_RECOVERY_KIND,
            "authorization_id": "recovery-test",
            "incident_api_rows": [],
            "incident_attempt_rows": [],
            "recovered_worker_launch_ids": ["worker-old", "worker-preauth"],
            "preauthorization_worker_launch_ids": ["worker-preauth"],
        }
        run_meta.campaign_recovery_incident_evidence.cache_clear()
        with mock.patch.object(
                run_meta, "read_campaign_recovery_authorization",
                return_value=authorization):
            evidence = run_meta.campaign_recovery_incident_evidence(
                "unit-test-out-dir")
        run_meta.campaign_recovery_incident_evidence.cache_clear()
        self.assertEqual(
            evidence["worker_launch_ids"],
            frozenset({"worker-old", "worker-preauth"}),
        )
        self.assertEqual(
            evidence["preauthorization_worker_launch_ids"],
            frozenset({"worker-preauth"}),
        )

    def test_operator_pause_preserves_prior_preauthorization_evidence(self):
        workers = {"worker-preauth"}
        expected_errors = {
            "worker sample exited before authorization with 1"}
        self.assertTrue(run_meta._preauthorization_stop_evidence_matches(
            {"condition": "operator_directed_dispatcher_pause"},
            workers, expected_errors))
        self.assertTrue(run_meta._preauthorization_stop_evidence_matches(
            {
                "condition": "dispatcher_integrity_failure",
                "error": "worker sample exited before authorization with 1",
            },
            workers, expected_errors))
        self.assertFalse(run_meta._preauthorization_stop_evidence_matches(
            {
                "condition": "dispatcher_integrity_failure",
                "error": "unrelated",
            },
            workers, expected_errors))

    def test_git_identity_ignores_evaluator_tmp_but_not_other_untracked_files(self):
        root_ignore = pathlib.Path(ROOT).parent / ".gitignore"
        ignore_text = root_ignore.read_text(encoding="utf-8")
        self.assertIn("HP_V**/tmp_eval_**/", ignore_text.splitlines())

        with tempfile.TemporaryDirectory() as repo_dir:
            repo = pathlib.Path(repo_dir)
            source_dir = repo / "HP_V8" / "src"
            source_dir.mkdir(parents=True)
            (repo / ".gitignore").write_text(ignore_text, encoding="utf-8")
            (source_dir / "tracked.py").write_text("TRACKED = True\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=repo, check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "HybridPatch Test"],
                cwd=repo, check=True,
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "fixture"],
                cwd=repo, check=True,
            )

            evaluator_tmp = repo / "HP_V8" / "tmp_eval_unit_test" / "work"
            evaluator_tmp.mkdir(parents=True)
            (evaluator_tmp / "artifact.py").write_text("temporary\n", encoding="utf-8")
            with mock.patch.object(run_meta, "_HERE", str(source_dir)):
                _commit, tree_state, porcelain = run_meta._git_identity_details()
            self.assertEqual(tree_state, "clean")
            self.assertEqual(porcelain, "")

            untracked_source = source_dir / "untracked_source.py"
            untracked_source.write_text("untracked\n", encoding="utf-8")
            with mock.patch.object(run_meta, "_HERE", str(source_dir)):
                _commit, tree_state, porcelain = run_meta._git_identity_details()
            self.assertEqual(tree_state, "dirty")
            self.assertEqual(porcelain, "?? HP_V8/src/untracked_source.py\n")

    def test_run_metadata_startup_rejects_dispatch_git_identity_drift(self):
        with tempfile.TemporaryDirectory() as repo_dir, \
                tempfile.TemporaryDirectory() as out_dir:
            repo = pathlib.Path(repo_dir)
            source_dir = repo / "HP_V8" / "src"
            source_dir.mkdir(parents=True)
            (repo / ".gitignore").write_text(
                "HP_V**/tmp_eval_**/\n", encoding="utf-8")
            (source_dir / "tracked.py").write_text("TRACKED = True\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=repo, check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "HybridPatch Test"],
                cwd=repo, check=True,
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "fixture"],
                cwd=repo, check=True,
            )
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
                capture_output=True, text=True, encoding="utf-8",
            ).stdout.strip()
            untracked_source = source_dir / "unexpected.py"
            untracked_source.write_text("unexpected\n", encoding="utf-8")
            env = {
                "ANCHORPATCH_EXPECTED_GIT_COMMIT": commit,
                "ANCHORPATCH_EXPECTED_GIT_TREE_STATE": "clean",
            }
            kwargs = {
                "command": "python test",
                "samples": ["sample"],
                "methods": ["hybridpatch", "fullrewrite"],
                "num_round_trips": 1,
                "seed": 42,
                "model": "offline-test-model",
                "distractor": True,
                "max_tokens": 16,
                "printing": False,
            }
            with mock.patch.object(run_meta, "_HERE", str(source_dir)), \
                    mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value={"x": "y"}), \
                    mock.patch.dict(os.environ, env, clear=False):
                with self.assertRaisesRegex(
                        RuntimeError,
                        "runner Git identity differs from dispatch manifest"):
                    run_meta.append_run_metadata(out_dir, **kwargs)
            self.assertEqual(run_meta.read_run_metadata_snapshot(out_dir), [])
            self.assertEqual(run_meta.read_campaign_stop_conditions(out_dir), [])
            self.assertFalse(os.path.exists(
                os.path.join(out_dir, "api_calls.jsonl")))
            self.assertFalse(os.path.exists(
                os.path.join(out_dir, "api_attempt_ledger.jsonl")))

    def test_run_metadata_locks_task_plan_hash_before_api(self):
        kwargs = {
            "command": "python test",
            "samples": ["sample"],
            "methods": ["hybridpatch", "fullrewrite"],
            "num_round_trips": 2,
            "seed": 42,
            "model": "offline-test-model",
            "distractor": True,
            "max_tokens": 16,
            "printing": False,
            "context_shuffle_seeded": True,
            "context_shuffle_seed_version": "global_random_seed_v1",
            "stop_on_preservation_violation": True,
        }
        commit = "1" * 40
        with tempfile.TemporaryDirectory() as out_dir, \
                mock.patch.object(run_meta, "_git_identity", return_value=(commit, "clean")), \
                mock.patch.object(run_meta, "code_fingerprint", return_value={"x": "y"}):
            run_meta.append_run_metadata(out_dir, **kwargs)
            plan_path = os.path.join(out_dir, "sample.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a", "state_b"])
            with mock.patch.dict(
                os.environ,
                {"ANCHORPATCH_EXPECTED_TASK_PLAN_SHA256": "0" * 64},
                clear=False,
            ):
                with self.assertRaises(RuntimeError):
                    run_meta.register_task_plan(
                        out_dir, "sample", plan_path, num_round_trips=2)
            records = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(records[0]["task_plans"], {})

            entry = run_meta.register_task_plan(
                out_dir, "sample", plan_path, num_round_trips=2)
            self.assertEqual(len(entry["sha256"]), 64)
            records = run_meta.read_run_metadata_snapshot(out_dir)
            self.assertEqual(records[0]["task_plans"]["sample"], entry)
            self.assertFalse(any(".tmp-" in name for name in os.listdir(out_dir)))

            utils_relay_plan.save_relay_task_plan(plan_path, ["state_b", "state_a"])
            with self.assertRaises(RuntimeError):
                run_meta.register_task_plan(
                    out_dir, "sample", plan_path, num_round_trips=2)

    def test_dispatcher_inspector_rejects_git_and_task_plan_drift(self):
        with tempfile.TemporaryDirectory() as out_dir:
            sample = "sample"
            plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
            utils_relay_plan.save_relay_task_plan(plan_path, ["state_a"])
            plan_sha = paired_dispatch._sha256(plan_path)
            manifest = {
                "schema": paired_dispatch.SCHEMA,
                "run_git_commit": "1" * 40,
                "git_tree_state": "clean",
                "config": {
                    "samples": [sample],
                    "method_set": ["fullrewrite", "hybridpatch"],
                    "num_round_trips": 1,
                },
                "task_plans": {
                    sample: {
                        "path": os.path.basename(plan_path),
                        "sha256": plan_sha,
                    },
                },
            }
            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("1" * 40, "clean")):
                utils_relay_plan.save_relay_task_plan(
                    plan_path, ["state_b"])
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertIn(
                    "task-plan hash drift: sample",
                    inspection["errors"],
                )

                utils_relay_plan.save_relay_task_plan(
                    plan_path, ["state_a"])
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertNotIn(
                    "task-plan hash drift: sample",
                    inspection["errors"],
                )

            with mock.patch.object(
                    paired_dispatch, "_git_identity",
                    return_value=("2" * 40, "clean")):
                inspection = paired_dispatch.inspect_campaign(
                    out_dir, manifest, require_complete=True)
                self.assertIn(
                    "Git commit/tree state changed during campaign",
                    inspection["errors"],
                )
