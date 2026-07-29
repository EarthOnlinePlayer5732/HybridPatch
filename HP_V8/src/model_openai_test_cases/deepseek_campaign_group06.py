"""Mixin slice for DeepSeek monthly-quota key failover regressions."""

from .support import *


def _quota_error_body():
    return {
        "type": "GoUsageLimitError",
        "message": "Monthly usage limit reached. Please upgrade.",
    }


def _quota_attempt(index, **overrides):
    attempt = {
        "attempt_index": index,
        "status": "retryable_error",
        "http_status": 429,
        "error_type": "rate_limit",
        "error_body": _quota_error_body(),
        "generation_delta_seen": False,
        "stream_complete": False,
        "response_started_http_status": None,
        "retry_budget_consumed": True,
        "retry_budget_attempt_index": index,
    }
    attempt.update(overrides)
    return attempt


def _quota_row(
        *, sample="sample-a", worker_id="worker-a",
        request_id="request-quota", attempts=None, **overrides):
    attempts = attempts or [_quota_attempt(index) for index in range(1, 4)]
    row = {
        "schema": paired_dispatch.API_CALL_SCHEMA,
        "sample": sample,
        "method": "hybridpatch",
        "rt_index": 1,
        "direction": "forward",
        "call_kind": "hybridpatch_primary",
        "request_id": request_id,
        "worker_launch_id": worker_id,
        "worker_pid": os.getpid(),
        "model": paired_dispatch.DEEPSEEK_MODEL,
        "classification": "provider/API failure",
        "error_type": "rate_limit",
        "http_status": 429,
        "provider_called": True,
        "stream_complete": False,
        "response_replayed": False,
        "count_as_method_failure": False,
        "transport": paired_dispatch.DEEPSEEK_TRANSPORT,
        "transport_revision": paired_dispatch.DEEPSEEK_TRANSPORT_REVISION,
        "transport_resume_policy": None,
        "transport_attempts": attempts,
        "http_attempts_used": len(attempts),
        "retry_count": len(attempts) - 1,
        "failed_attempt_count": len(attempts),
        "max_retries": len(attempts),
    }
    row.update(overrides)
    return row


def _quota_outcome(
        *, sample="sample-a", worker_id="worker-a",
        request_id="request-quota", created_at="2026-07-29T00:00:00+00:00"):
    return {
        "sample": sample,
        "worker_launch_id": worker_id,
        "request_id": request_id,
        "status": "infrastructure_incomplete",
        "created_at": created_at,
    }


class DeepSeekOpenCodeCampaignGroup06Mixin:
    @staticmethod
    def _queue_args():
        return argparse.Namespace(
            campaign_role="deepseek_full234",
            num_round_trips=paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS,
            seed=42,
            notes="unit",
            start_timeout=1,
            poll_interval=0,
            progress_interval=10 ** 12,
            _dispatcher_instance_id="dispatcher-unit",
        )

    @staticmethod
    def _assignment(sample, key_label):
        return {
            "sample": sample,
            "key_label": key_label,
            "methods": ["hybridpatch", "fullrewrite"],
            "console_log": f"dispatch_logs/{sample}.console.log",
        }

    @staticmethod
    def _queue_manifest(assignments):
        return {
            "schema": paired_dispatch.SCHEMA,
            "run_git_commit": "1" * 40,
            "config": {
                "samples": [item["sample"] for item in assignments],
                "method_set": ["hybridpatch", "fullrewrite"],
                "num_round_trips": (
                    paired_dispatch.DEEPSEEK_FULL234_ROUND_TRIPS),
                "campaign_role": "deepseek_full234",
            },
            "assignments": [dict(item) for item in assignments],
        }

    def _run_fake_queue(self, assignments, failing, *, slots_per_key=1):
        class FakeProcess:
            def __init__(self, pid, returncode):
                self.pid = pid
                self.returncode = returncode

            def poll(self):
                return self.returncode

        args = self._queue_args()
        manifest = self._queue_manifest(assignments)
        task_plans = {
            item["sample"]: {
                "path": f"{item['sample']}.task_plan.json",
                "sha256": "a" * 64,
                "forward_state_sequence": ["state"],
            }
            for item in assignments
        }
        launch_batches = []
        lifecycle = []
        infrastructure = set()
        not_started = set()
        evaluator = set()
        completed = set()
        labels = sorted({item["key_label"] for item in assignments})
        keys = {label: f"secret-{label}" for label in labels}

        with tempfile.TemporaryDirectory() as out_dir:
            os.makedirs(os.path.join(out_dir, "dispatch_logs"))
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")

            def fake_launch(
                    _args, launch_out_dir, _manifest, _plans, _keys, batch,
                    _resume_authorizations, launch_dispatch_log, running):
                encoded = []
                for item in batch:
                    key_fields = paired_dispatch._worker_key_launch_fields(item)
                    encoded.append(
                        f"{item['sample']}:{key_fields['key_label']}:"
                        f"{key_fields['failover_count']}")
                    worker_id = (
                        f"worker-{len(lifecycle) + 1}-{item['sample']}")
                    log_path = os.path.join(
                        launch_out_dir, item["console_log"])
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    returncode = 1 if failing(item) else 0
                    state = {
                        "sample": item["sample"],
                        "process": FakeProcess(20000 + len(lifecycle),
                                               returncode),
                        "log": open(log_path, "a", encoding="utf-8"),
                        **key_fields,
                        "methods": list(item["methods"]),
                        "console_log": item["console_log"],
                        "target_round_trips": _args.num_round_trips,
                        "resume_authorization": None,
                        "worker_launch_id": worker_id,
                        "method_phase": item.get("method_phase"),
                        "interrupted_resume_evidence": None,
                        "ready_path": os.path.join(
                            launch_out_dir, f"{worker_id}.ready.json"),
                        "ack_path": os.path.join(
                            launch_out_dir, f"{worker_id}.ack.json"),
                        "dispatcher_pid": os.getpid(),
                        "dispatcher_instance_id": (
                            _args._dispatcher_instance_id),
                        "exit_recorded": False,
                    }
                    running[item["sample"]] = state
                    paired_dispatch.append_jsonl_locked(
                        launch_dispatch_log,
                        {
                            "event": "launch",
                            "sample": item["sample"],
                            **key_fields,
                            "methods": list(item["methods"]),
                            "worker_launch_id": worker_id,
                            "pid": state["process"].pid,
                        },
                    )
                    lifecycle.append(dict(state))
                launch_batches.append(encoded)
                return [item["sample"] for item in batch]

            def fake_record_exit(
                    _out_dir, running, sample, item, returncode,
                    exit_dispatch_log):
                item["log"].close()
                disposition = (
                    "finished" if returncode == 0
                    else "infrastructure_incomplete"
                )
                paired_dispatch._append_worker_exit(
                    sample, item, returncode, exit_dispatch_log,
                    disposition=disposition)
                del running[sample]
                return disposition

            def fake_quota_evidence(_out_dir, sample, item):
                return {
                    "sample": sample,
                    "request_id": (
                        f"request-{sample}-{item['key_label']}-"
                        f"{item.get('failover_count', 0)}"),
                    "worker_launch_id": item["worker_launch_id"],
                    "api_row": 1,
                    "api_row_count": 1,
                    "api_row_sha256": "a" * 64,
                    "sample_outcome_row": 1,
                    "sample_outcome_sha256": "b" * 64,
                    "http_attempt_count": 3,
                    "sample_outcome_created_at": (
                        "2026-07-29T00:00:00+00:00"),
                }

            with mock.patch.object(
                    paired_dispatch, "_launch_worker_batch",
                    side_effect=fake_launch), mock.patch.object(
                        paired_dispatch, "_record_worker_exit",
                        side_effect=fake_record_exit), mock.patch.object(
                            paired_dispatch,
                            "_deepseek_monthly_quota_quarantine_evidence",
                            side_effect=fake_quota_evidence), mock.patch.object(
                                paired_dispatch, "_write_active_worker_set"), \
                    mock.patch.object(
                        paired_dispatch, "inspect_campaign",
                        return_value={
                            "errors": [],
                            "api_calls": 0,
                            "preservation_violations": 0,
                        }):
                paired_dispatch._run_worker_queue(
                    args, out_dir, manifest, task_plans, keys, assignments,
                    {}, dispatch_log, {}, infrastructure, evaluator,
                    completed, len(assignments), slots_per_key,
                    not_started)

            rows = paired_dispatch._read_jsonl(dispatch_log)
        return {
            "launch_batches": launch_batches,
            "rows": rows,
            "infrastructure": infrastructure,
            "not_started": not_started,
            "evaluator": evaluator,
            "completed": completed,
            "lifecycle": lifecycle,
        }

    @staticmethod
    def _audit_fixture():
        manifest = {
            "config": {
                "samples": ["sample-a", "sample-b"],
                "method_set": ["hybridpatch", "fullrewrite"],
            },
            "assignments": [
                {"sample": "sample-a", "key_label": "KEY_1"},
                {"sample": "sample-b", "key_label": "KEY_1"},
            ],
        }
        api_rows = [_quota_row()]
        outcome_rows = [_quota_outcome()]
        dispatch_rows = [
            {
                "event": "launch",
                "sample": "sample-a",
                "key_label": "KEY_1",
                "original_key_label": "KEY_1",
                "prior_key_label": None,
                "failover_count": 0,
                "failover_reason": None,
                "worker_launch_id": "worker-a",
            },
            {
                "event": "key_quarantined",
                "key_label": "KEY_1",
                "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                "quarantine_index": 1,
                "quarantined_key_count": 1,
                "observation_index": 1,
                "trigger_sample": "sample-a",
                "trigger_worker_launch_id": "worker-a",
                "trigger_request_id": "request-quota",
                "trigger_http_attempt_count": 3,
                "api_row": 1,
                "api_row_count": 1,
                "api_row_sha256": paired_dispatch._canonical_record_sha256(
                    api_rows[0]),
                "sample_outcome_row": 1,
                "sample_outcome_sha256": (
                    paired_dispatch._canonical_record_sha256(outcome_rows[0])),
                "sample_outcome_created_at": outcome_rows[0]["created_at"],
            },
            {
                "event": "key_failover_assigned",
                "sample": "sample-b",
                "from_key_label": "KEY_1",
                "to_key_label": "KEY_2",
                "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                "failover_count": 1,
                "original_key_label": "KEY_1",
            },
            {
                "event": "launch",
                "sample": "sample-b",
                "key_label": "KEY_2",
                "original_key_label": "KEY_1",
                "prior_key_label": "KEY_1",
                "failover_count": 1,
                "failover_reason": (
                    paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON),
                "worker_launch_id": "worker-b",
            },
        ]
        launches = {
            row["worker_launch_id"]: row
            for row in dispatch_rows
            if row.get("event") == "launch"
        }
        return manifest, dispatch_rows, api_rows, outcome_rows, launches

    def test_monthly_quota_predicate_is_exact(self):
        self.assertTrue(paired_dispatch._deepseek_monthly_usage_limit_row(
            _quota_row()))

        cases = {
            "generic_429": {
                "transport_attempts": [
                    _quota_attempt(index) for index in range(1, 3)
                ] + [_quota_attempt(
                    3, error_body={
                        "type": "RateLimitError",
                        "message": "Too many requests",
                    })],
            },
            "nested_wrapper": {
                "transport_attempts": [
                    _quota_attempt(index) for index in range(1, 3)
                ] + [_quota_attempt(
                    3, error_body={"error": _quota_error_body()})],
            },
            "extra_error_field": {
                "transport_attempts": [
                    _quota_attempt(index) for index in range(1, 3)
                ] + [_quota_attempt(
                    3, error_body={
                        **_quota_error_body(),
                        "code": "monthly_limit",
                    })],
            },
            "pre_generation_503": {
                "http_status": 503,
                "error_type": "server_error",
                "transport_attempts": [
                    _quota_attempt(index) for index in range(1, 3)
                ] + [_quota_attempt(
                    3, http_status=503, error_type="server_error",
                    retry_budget_consumed=False,
                    retry_budget_attempt_index=2)],
            },
            "partial_streaming": {
                "transport_attempts": [
                    _quota_attempt(index) for index in range(1, 3)
                ] + [_quota_attempt(
                    3, response_started_http_status=200,
                    generation_delta_seen=True)],
            },
            "provider_timeout": {
                "http_status": None,
                "error_type": "timeout",
                "transport_attempts": [
                    _quota_attempt(index) for index in range(1, 3)
                ] + [_quota_attempt(
                    3, http_status=None, error_type="timeout")],
            },
        }
        for label, overrides in cases.items():
            with self.subTest(label=label):
                self.assertFalse(
                    paired_dispatch._deepseek_monthly_usage_limit_row(
                        _quota_row(**overrides)))

    def test_key_quarantine_failover_is_work_conserving(self):
        assignments = [
            self._assignment("quota-a", "KEY_1"),
            self._assignment("quota-b", "KEY_1"),
            self._assignment("quota-c", "KEY_1"),
            self._assignment("healthy-x1", "KEY_2"),
            self._assignment("healthy-x2", "KEY_2"),
            self._assignment("healthy-y", "KEY_3"),
        ]

        def failing(item):
            return (
                item["sample"] == "quota-a"
                and item["key_label"] == "KEY_1"
                and item.get("failover_count", 0) == 0
            )

        result = self._run_fake_queue(assignments, failing)
        self.assertEqual(result["launch_batches"], [
            ["quota-a:KEY_1:0", "healthy-x1:KEY_2:0",
             "healthy-y:KEY_3:0"],
            ["healthy-x2:KEY_2:0", "quota-a:KEY_3:1"],
            ["quota-b:KEY_2:1", "quota-c:KEY_3:1"],
        ])
        self.assertEqual(
            result["completed"],
            {"quota-a", "quota-b", "quota-c", "healthy-x1",
             "healthy-x2", "healthy-y"},
        )
        self.assertEqual(result["infrastructure"], set())
        quarantines = [
            row for row in result["rows"]
            if row.get("event") == "key_quarantined"
        ]
        self.assertEqual(len(quarantines), 1)
        self.assertEqual(quarantines[0]["key_label"], "KEY_1")
        failovers = [
            row for row in result["rows"]
            if row.get("event") == "key_failover_assigned"
        ]
        self.assertEqual(
            [(row["sample"], row["to_key_label"], row["failover_count"])
             for row in failovers],
            [("quota-a", "KEY_3", 1), ("quota-b", "KEY_2", 1),
             ("quota-c", "KEY_3", 1)],
        )
        quarantine_index = result["rows"].index(quarantines[0])
        self.assertFalse(any(
            index > quarantine_index
            and row.get("event") == "launch"
            and row.get("key_label") == "KEY_1"
            for index, row in enumerate(result["rows"])
        ))

    def test_repeated_same_key_quota_observation_quarantines_once(self):
        assignments = [
            self._assignment("quota-a", "KEY_1"),
            self._assignment("quota-b", "KEY_1"),
            self._assignment("healthy-x", "KEY_2"),
        ]

        def failing(item):
            return (
                item["sample"] in {"quota-a", "quota-b"}
                and item["key_label"] == "KEY_1"
                and item.get("failover_count", 0) == 0
            )

        result = self._run_fake_queue(
            assignments, failing, slots_per_key=2)
        quarantines = [
            row for row in result["rows"]
            if row.get("event") == "key_quarantined"
        ]
        self.assertEqual(len(quarantines), 1)
        observations = [
            row for row in result["rows"]
            if row.get("event") in {
                "key_quarantined", "key_quota_observed"
            }
        ]
        self.assertEqual(
            [(row["event"], row["observation_index"])
             for row in observations],
            [("key_quarantined", 1), ("key_quota_observed", 2)],
        )
        failovers = [
            row for row in result["rows"]
            if row.get("event") == "key_failover_assigned"
        ]
        self.assertEqual(
            [(row["sample"], row["to_key_label"]) for row in failovers],
            [("quota-a", "KEY_2"), ("quota-b", "KEY_2")],
        )
        self.assertEqual(
            result["launch_batches"],
            [
                ["quota-a:KEY_1:0", "quota-b:KEY_1:0",
                 "healthy-x:KEY_2:0"],
                ["quota-a:KEY_2:1", "quota-b:KEY_2:1"],
            ],
        )
        self.assertEqual(
            result["completed"], {"quota-a", "quota-b", "healthy-x"})

    def test_all_keys_exhausted_records_incomplete_supporting_evidence(self):
        assignments = [
            self._assignment("quota-a", "KEY_1"),
            self._assignment("quota-b", "KEY_1"),
            self._assignment("quota-x", "KEY_2"),
        ]

        def failing(item):
            return item.get("failover_count", 0) == 0

        result = self._run_fake_queue(assignments, failing)
        exhausted = [
            row for row in result["rows"]
            if row.get("event") == "queue_exhausted_no_healthy_key"
        ]
        self.assertEqual(len(exhausted), 1)
        self.assertEqual(
            exhausted[0]["quarantined_key_labels"], ["KEY_1", "KEY_2"])
        self.assertEqual(
            exhausted[0]["pending_samples"],
            ["quota-a", "quota-b", "quota-x"],
        )
        self.assertEqual(
            exhausted[0]["not_started_pending_samples"],
            ["quota-b"],
        )
        self.assertEqual(
            result["infrastructure"],
            {"quota-a", "quota-x"},
        )
        self.assertEqual(result["not_started"], {"quota-b"})
        self.assertEqual(result["completed"], set())

    def test_legacy_queue_exhausted_pending_can_resume_after_new_key(self):
        assignment = self._assignment("quota-b", "KEY_1")
        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            paired_dispatch.append_jsonl_locked(dispatch_log, {
                "event": "queue_exhausted_no_healthy_key",
                "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                "quarantined_key_labels": ["KEY_1", "KEY_2"],
                "pending_samples": ["quota-b"],
            })
            for event in ("queue_complete", "campaign_incomplete"):
                paired_dispatch.append_jsonl_locked(dispatch_log, {
                    "event": event,
                    "completed_samples": [],
                    "infrastructure_incomplete_samples": ["quota-b"],
                    "evaluator_incomplete_samples": [],
                })

            self.assertEqual(
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    out_dir, [assignment]),
                {"quota-b"},
            )

            paired_dispatch.append_jsonl_locked(dispatch_log, {
                "event": "launch",
                "sample": "quota-b",
                "key_label": "KEY_1",
                "worker_launch_id": "worker-started",
            })
            with self.assertRaisesRegex(RuntimeError, "cannot prove pending"):
                paired_dispatch._verified_deepseek_resume_missing_samples(
                    out_dir, [assignment])

    def test_key_failover_inspector_rejects_tamper(self):
        manifest, dispatch_rows, api_rows, outcome_rows, launches = (
            self._audit_fixture())
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, dispatch_rows, api_rows, outcome_rows, launches),
            [],
        )

        cases = []
        bad_api = copy.deepcopy(api_rows)
        bad_api[0]["transport_attempts"][-1]["error_body"] = {
            "error": _quota_error_body()
        }
        cases.append((
            "nested-wrapper",
            manifest, dispatch_rows, bad_api, launches,
            "not monthly quota",
        ))

        reordered = [
            dispatch_rows[0], dispatch_rows[2], dispatch_rows[1],
            dispatch_rows[3],
        ]
        cases.append((
            "failover-before-quarantine",
            manifest, reordered, api_rows, launches,
            "before quarantine",
        ))

        same_destination = copy.deepcopy(dispatch_rows)
        same_destination[2]["to_key_label"] = "KEY_1"
        cases.append((
            "same-destination",
            manifest, same_destination, api_rows, launches,
            "destination matches source",
        ))

        late_launch = copy.deepcopy(dispatch_rows)
        late_launch.insert(2, {
            "event": "launch",
            "sample": "sample-c",
            "key_label": "KEY_1",
            "failover_count": 0,
            "worker_launch_id": "worker-c",
        })
        late_launches = {
            row["worker_launch_id"]: row
            for row in late_launch
            if row.get("event") == "launch"
        }
        cases.append((
            "launch-after-quarantine",
            manifest, late_launch, api_rows, late_launches,
            "launch after key quarantine",
        ))

        drifted_manifest = copy.deepcopy(manifest)
        drifted_manifest["config"]["samples"] = ["sample-b", "sample-a"]
        cases.append((
            "manifest-order",
            drifted_manifest, dispatch_rows, api_rows, launches,
            "manifest assignment sample/order drift",
        ))

        for label, case_manifest, case_dispatch, case_api, case_launches, text in cases:
            with self.subTest(label=label):
                errors = paired_dispatch._inspect_deepseek_key_failover_audit(
                    case_manifest, case_dispatch, case_api, outcome_rows,
                    case_launches)
                self.assertIn(text, "; ".join(errors))

    def test_key_failover_inspector_accepts_later_ledger_growth_and_quarantine(self):
        manifest, dispatch_rows, api_rows, outcome_rows, launches = (
            self._audit_fixture())
        second_api = _quota_row(
            sample="sample-b", worker_id="worker-b",
            request_id="request-quota-b")
        api_rows.append(second_api)
        second_outcome = _quota_outcome(
            sample="sample-b", worker_id="worker-b",
            request_id="request-quota-b",
            created_at="2026-07-29T00:01:00+00:00")
        outcome_rows.append(second_outcome)
        dispatch_rows.append({
            "event": "key_quarantined",
            "key_label": "KEY_2",
            "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
            "quarantine_index": 2,
            "quarantined_key_count": 2,
            "observation_index": 1,
            "trigger_sample": "sample-b",
            "trigger_worker_launch_id": "worker-b",
            "trigger_request_id": "request-quota-b",
            "trigger_http_attempt_count": 3,
            "api_row": 2,
            "api_row_count": 2,
            "api_row_sha256": paired_dispatch._canonical_record_sha256(
                second_api),
            "sample_outcome_row": 2,
            "sample_outcome_sha256": (
                paired_dispatch._canonical_record_sha256(second_outcome)),
            "sample_outcome_created_at": second_outcome["created_at"],
        })
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, dispatch_rows, api_rows, outcome_rows, launches),
            [],
        )
        dispatch_rows.extend([
            {
                "event": "key_failover_assigned",
                "sample": "sample-b",
                "from_key_label": "KEY_2",
                "to_key_label": "KEY_3",
                "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                "failover_count": 2,
                "original_key_label": "KEY_1",
            },
            {
                "event": "launch", "sample": "sample-b",
                "key_label": "KEY_3", "original_key_label": "KEY_1",
                "prior_key_label": "KEY_2", "failover_count": 2,
                "failover_reason": (
                    paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON),
                "worker_launch_id": "worker-c",
            },
        ])
        launches["worker-c"] = dispatch_rows[-1]
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, dispatch_rows, api_rows, outcome_rows, launches),
            [],
        )
        tampered = copy.deepcopy(dispatch_rows)
        tampered[-2]["from_key_label"] = "KEY_1"
        tampered[-1]["prior_key_label"] = "KEY_1"
        self.assertIn(
            "key failover source chain mismatch",
            "; ".join(paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, tampered, api_rows, outcome_rows, {
                    row["worker_launch_id"]: row
                    for row in tampered if row.get("event") == "launch"
                })),
        )

    def test_durable_quarantine_rebuild_uses_idle_healthy_keys(self):
        assignments = [self._assignment("sample-a", "KEY_1")]
        manifest = self._queue_manifest(assignments)
        quarantine = {
            "event": "key_quarantined",
            "key_label": "KEY_1",
            "observation_index": 1,
            "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
        }
        keys = {
            "KEY_1": "secret-1",
            "KEY_2": "secret-2",
            "KEY_3": "secret-3",
        }
        restored = paired_dispatch._restore_key_failover_queue_state(
            manifest, [quarantine], assignments, keys)
        self.assertEqual(restored["quarantined_keys"], {"KEY_1"})
        self.assertEqual(
            [item["sample"] for item in restored["handoff_fifo"]],
            ["sample-a"],
        )
        self.assertEqual(
            {label: len(items)
             for label, items in restored["pending_by_key"].items()},
            {"KEY_1": 0, "KEY_2": 0, "KEY_3": 0},
        )
        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            batch = paired_dispatch._take_available_assignments(
                restored["pending_by_key"], {}, 1,
                quarantined_keys=restored["quarantined_keys"],
                handoff_fifo=restored["handoff_fifo"],
                dispatch_log=dispatch_log,
                failover_counts=restored["failover_counts"],
            )
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0]["key_label"], "KEY_2")
        self.assertEqual(batch[0]["failover_count"], 1)

        assigned = {
            "event": "key_failover_assigned",
            "sample": "sample-a",
            "from_key_label": "KEY_1",
            "to_key_label": "KEY_2",
            "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
            "failover_count": 1,
            "original_key_label": "KEY_1",
        }
        restored_after_assignment = (
            paired_dispatch._restore_key_failover_queue_state(
                manifest, [quarantine, assigned], assignments, keys))
        self.assertEqual(restored_after_assignment["handoff_fifo"], [])
        self.assertEqual(
            restored_after_assignment["pending_by_key"]["KEY_2"][0][
                "failover_count"],
            1,
        )

        rotated_assignment = [
            {**assignments[0], "key_label": "KEY_11"}
        ]
        rotated = paired_dispatch._restore_key_failover_queue_state(
            manifest, [], rotated_assignment, {"KEY_11": "rotated-secret"})
        self.assertEqual(
            rotated["pending_by_key"]["KEY_11"][0]["key_label"],
            "KEY_11",
        )
        self.assertEqual(
            rotated["pending_by_key"]["KEY_11"][0][
                "original_key_label"],
            "KEY_11",
        )

        legacy_launch = {
            "event": "launch", "sample": "sample-a",
            "key_label": "KEY_1", "worker_launch_id": "worker-legacy",
        }
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, [legacy_launch], [], [], {
                    "worker-legacy": legacy_launch,
                }),
            [],
        )
        rotated_launches = [
            {
                "event": "launch", "sample": "sample-a",
                "key_label": label, "original_key_label": label,
                "prior_key_label": None, "failover_count": 0,
                "failover_reason": None,
                "worker_launch_id": worker_id,
            }
            for label, worker_id in (
                ("KEY_1", "worker-before-rotation"),
                ("KEY_11", "worker-after-rotation"),
            )
        ]
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, rotated_launches, [], [], {
                    row["worker_launch_id"]: row for row in rotated_launches
                }),
            [],
        )

    def test_replaced_key_label_can_be_reactivated_for_resume(self):
        assignments = [
            self._assignment("sample-a", "KEY_1"),
            self._assignment("sample-b", "KEY_1"),
        ]
        manifest = self._queue_manifest(assignments)
        quarantine = {
            "event": "key_quarantined",
            "key_label": "KEY_1",
            "observation_index": 1,
            "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
        }
        keys = {"KEY_1": "replacement-secret", "KEY_2": "healthy-secret"}
        blocked = paired_dispatch._restore_key_failover_queue_state(
            manifest, [quarantine], assignments, {"KEY_1": "replacement-secret"})
        self.assertEqual(blocked["quarantined_keys"], {"KEY_1"})
        self.assertEqual(
            [item["sample"] for item in blocked["handoff_fifo"]],
            ["sample-a", "sample-b"],
        )

        with tempfile.TemporaryDirectory() as out_dir:
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            rows = paired_dispatch._append_key_reactivation_events(
                dispatch_log, [quarantine], keys, ["KEY_1"],
                "operator replaced key material")
            persisted = paired_dispatch._read_jsonl(dispatch_log)

        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0]["event"], paired_dispatch.KEY_REACTIVATION_EVENT)
        self.assertNotIn("replacement-secret", json.dumps(persisted[0]))
        self.assertEqual(rows[-1], persisted[0])
        restored = paired_dispatch._restore_key_failover_queue_state(
            manifest, rows, assignments, keys)
        self.assertEqual(restored["quarantined_keys"], set())
        self.assertEqual(restored["handoff_fifo"], [])
        self.assertEqual(
            [item["sample"] for item in restored["pending_by_key"]["KEY_1"]],
            ["sample-a", "sample-b"],
        )

    def test_key_reactivation_inspector_binds_epoch(self):
        manifest, dispatch_rows, api_rows, outcome_rows, _launches = (
            self._audit_fixture())
        reactivation = {
            "event": paired_dispatch.KEY_REACTIVATION_EVENT,
            "key_label": "KEY_1",
            "reason": "operator replaced key material",
            "reactivation_index": 1,
            "observed_quota_count": 1,
            "quarantined_key_labels_before": ["KEY_1"],
            "quarantined_key_labels_after": [],
        }
        relaunch = {
            "event": "launch",
            "sample": "sample-a",
            "key_label": "KEY_1",
            "original_key_label": "KEY_1",
            "prior_key_label": None,
            "failover_count": 0,
            "failover_reason": None,
            "worker_launch_id": "worker-a-retry",
        }
        rows = [dispatch_rows[0], dispatch_rows[1], reactivation, relaunch]
        launches = {
            row["worker_launch_id"]: row
            for row in rows if row.get("event") == "launch"
        }
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, rows, api_rows, outcome_rows, launches),
            [],
        )

        tampered = copy.deepcopy(rows)
        tampered[2]["reactivation_index"] = 2
        self.assertIn(
            "key reactivation evidence invalid",
            "; ".join(paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, tampered, api_rows, outcome_rows, launches)),
        )

    def test_key_failover_inspector_binds_repeated_quota_observations(self):
        manifest = {
            "config": {
                "samples": ["sample-a", "sample-b", "sample-c"],
                "method_set": ["hybridpatch", "fullrewrite"],
            },
            "assignments": [
                {"sample": sample, "key_label": "KEY_1"}
                for sample in ("sample-a", "sample-b", "sample-c")
            ],
        }
        api_rows = [
            _quota_row(sample="sample-a", worker_id="worker-a",
                       request_id="request-a"),
            _quota_row(sample="sample-b", worker_id="worker-b",
                       request_id="request-b"),
        ]
        outcome_rows = [
            _quota_outcome(sample="sample-a", worker_id="worker-a",
                           request_id="request-a"),
            _quota_outcome(
                sample="sample-b", worker_id="worker-b",
                request_id="request-b",
                created_at="2026-07-29T00:01:00+00:00"),
        ]
        dispatch_rows = [
            {
                "event": "launch", "sample": "sample-a",
                "key_label": "KEY_1", "original_key_label": "KEY_1",
                "prior_key_label": None, "failover_count": 0,
                "failover_reason": None, "worker_launch_id": "worker-a",
            },
            {
                "event": "launch", "sample": "sample-b",
                "key_label": "KEY_1", "original_key_label": "KEY_1",
                "prior_key_label": None, "failover_count": 0,
                "failover_reason": None, "worker_launch_id": "worker-b",
            },
            {
                "event": "key_quarantined", "key_label": "KEY_1",
                "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                "quarantine_index": 1, "quarantined_key_count": 1,
                "observation_index": 1, "trigger_sample": "sample-a",
                "trigger_worker_launch_id": "worker-a",
                "trigger_request_id": "request-a",
                "trigger_http_attempt_count": 3, "api_row": 1,
                "api_row_count": 1,
                "api_row_sha256": paired_dispatch._canonical_record_sha256(
                    api_rows[0]),
                "sample_outcome_row": 1,
                "sample_outcome_sha256": (
                    paired_dispatch._canonical_record_sha256(outcome_rows[0])),
                "sample_outcome_created_at": outcome_rows[0]["created_at"],
            },
            {
                "event": "key_quota_observed", "key_label": "KEY_1",
                "reason": paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
                "observation_index": 2, "trigger_sample": "sample-b",
                "trigger_worker_launch_id": "worker-b",
                "trigger_request_id": "request-b",
                "trigger_http_attempt_count": 3, "api_row": 2,
                "api_row_count": 2,
                "api_row_sha256": paired_dispatch._canonical_record_sha256(
                    api_rows[1]),
                "sample_outcome_row": 2,
                "sample_outcome_sha256": (
                    paired_dispatch._canonical_record_sha256(outcome_rows[1])),
                "sample_outcome_created_at": outcome_rows[1]["created_at"],
            },
        ]
        launches = {
            row["worker_launch_id"]: row
            for row in dispatch_rows if row.get("event") == "launch"
        }
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, dispatch_rows, api_rows, outcome_rows, launches),
            [],
        )

        reused = copy.deepcopy(dispatch_rows)
        reused[-1].update({
            "trigger_sample": "sample-a",
            "trigger_worker_launch_id": "worker-a",
            "trigger_request_id": "request-a",
            "api_row": 1,
            "api_row_sha256": paired_dispatch._canonical_record_sha256(
                api_rows[0]),
        })
        self.assertIn(
            "key quota API evidence reused",
            "; ".join(paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, reused, api_rows, outcome_rows, launches)),
        )

        # The first quarantine bound a one-row prefix. Later append-only API
        # growth must not make that already-audited evidence stale.
        api_rows.append(_quota_row(
            sample="sample-c", worker_id="worker-c",
            request_id="request-unrelated"))
        self.assertEqual(
            paired_dispatch._inspect_deepseek_key_failover_audit(
                manifest, dispatch_rows, api_rows, outcome_rows, launches),
            [],
        )

    def test_failover_launch_env_and_run_metadata_provenance(self):
        captured = {}

        class FakeProcess:
            pid = 34567

            @staticmethod
            def poll():
                return None

        def fake_popen(command, **kwargs):
            captured["command"] = list(command)
            captured["env"] = dict(kwargs["env"])
            return FakeProcess()

        args = argparse.Namespace(
            campaign_role="deepseek_capacity15",
            num_round_trips=2,
            seed=42,
            notes="unit",
            start_timeout=10,
        )
        sample = "earncall1"
        assignment = {
            "sample": sample,
            "key_label": "KEY_2",
            "original_key_label": "KEY_1",
            "prior_key_label": "KEY_1",
            "failover_count": 1,
            "failover_reason": (
                paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON),
            "methods": ["hybridpatch", "fullrewrite"],
            "console_log": "dispatch_logs/earncall1.log",
        }
        with tempfile.TemporaryDirectory() as out_dir:
            os.makedirs(os.path.join(out_dir, "dispatch_logs"))
            plan_path = os.path.join(out_dir, f"{sample}.task_plan.json")
            with open(plan_path, "w", encoding="utf-8") as handle:
                json.dump({"targets": []}, handle)
            task_plans = {
                sample: {
                    "path": os.path.basename(plan_path),
                    "sha256": "b" * 64,
                }
            }
            manifest = {"run_git_commit": "1" * 40}
            dispatch_log = os.path.join(out_dir, "dispatch_log.jsonl")
            with mock.patch.object(
                    paired_dispatch.subprocess, "Popen",
                    side_effect=fake_popen), mock.patch.object(
                        paired_dispatch, "_authorize_workers"), \
                    mock.patch.object(
                        paired_dispatch, "_write_active_worker_set"), \
                    mock.patch.dict(os.environ, {
                        "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID": (
                            "stale"),
                        "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX": "9",
                        "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT": (
                            "stale"),
                        "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX": (
                            "9"),
                    }, clear=False):
                running = {}
                paired_dispatch._launch_worker_batch(
                    args, out_dir, manifest, task_plans,
                    {"KEY_2": "secret-key-2"}, [assignment], {},
                    dispatch_log, running)
                running[sample]["log"].close()

            environment = captured["env"]
            self.assertEqual(environment["OPENAI_API_KEY"], "secret-key-2")
            self.assertEqual(
                environment["ANCHORPATCH_DISPATCH_KEY_LABEL"], "KEY_2")
            self.assertEqual(
                environment["ANCHORPATCH_ORIGINAL_KEY_LABEL"], "KEY_1")
            self.assertEqual(
                environment["ANCHORPATCH_PRIOR_KEY_LABEL"], "KEY_1")
            self.assertEqual(
                environment["ANCHORPATCH_KEY_FAILOVER_COUNT"], "1")
            self.assertEqual(
                environment["ANCHORPATCH_KEY_FAILOVER_REASON"],
                paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
            )
            for name in (
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_SEMANTIC_CALL_ID",
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_INDEX",
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_REQUEST_FINGERPRINT",
                    "ANCHORPATCH_INFRASTRUCTURE_RESUME_NEXT_ATTEMPT_INDEX"):
                self.assertNotIn(name, environment)
            launch_rows = [
                row for row in paired_dispatch._read_jsonl(dispatch_log)
                if row.get("event") == "launch"
            ]
            self.assertEqual(len(launch_rows), 1)
            self.assertEqual(launch_rows[0]["original_key_label"], "KEY_1")
            self.assertEqual(launch_rows[0]["prior_key_label"], "KEY_1")
            self.assertEqual(launch_rows[0]["failover_count"], 1)

            kwargs = run_metadata_kwargs(
                command="python worker",
                samples=(sample,),
                methods=("hybridpatch", "fullrewrite"),
                num_round_trips=2,
                model=paired_dispatch.DEEPSEEK_MODEL,
                max_tokens=paired_dispatch.DEEPSEEK_MAX_TOKENS,
                distractor=True,
                reasoning_effort=paired_dispatch.DEEPSEEK_REASONING_EFFORT,
            )
            with mock.patch.object(
                    run_meta, "_git_identity",
                    return_value=("1" * 40, "clean")), mock.patch.object(
                        run_meta, "code_fingerprint",
                        return_value={"unit": "test"}), mock.patch.dict(
                            os.environ, environment, clear=False):
                record = run_meta.append_run_metadata(out_dir, **kwargs)
            self.assertEqual(record["dispatch_key_label"], "KEY_2")
            self.assertEqual(record["original_key_label"], "KEY_1")
            self.assertEqual(record["prior_key_label"], "KEY_1")
            self.assertEqual(record["failover_count"], 1)
            self.assertEqual(
                record["failover_reason"],
                paired_dispatch.DEEPSEEK_MONTHLY_USAGE_LIMIT_REASON,
            )
