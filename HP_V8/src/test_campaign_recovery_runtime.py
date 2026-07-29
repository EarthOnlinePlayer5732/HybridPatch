"""Behavioral regression checks for the campaign recovery runtime facade."""

import json
import os
import tempfile
import unittest

import authorize_ledger_lock_recovery as authorizer
import campaign_recovery_runtime as runtime
from run_meta import _canonical_record_sha256


MOVED_FUNCTIONS = [
    "_read_json",
    "_replace_with_sharing_retry",
    "_copy_file_durable",
    "_ensure_bound_file_copy",
    "_assert_dispatcher_parent_loss_path_budget",
    "_recoverable_jsonl_tail",
    "_assert_recoverable_jsonl_tail",
    "_append_recoverable_jsonl_tail",
    "_write_json_no_replace",
    "_read_jsonl",
    "_file_prefix_evidence",
    "_git_changed_paths",
    "_incident_entry",
    "_reconcile_deepseek_parent_loss_workers",
    "_record_stopless_registered_prelaunch_parent_loss",
    "_apply_deepseek_parent_loss_recovery_plan",
    "_deepseek_parent_loss_incidents",
    "_merge_incident_entries",
    "_merge_transport_sidecar_entries",
    "_unlink_with_sharing_retry",
]


class CampaignRecoveryRuntimeTests(unittest.TestCase):
    def test_authorizer_reexports_runtime_functions(self):
        for name in MOVED_FUNCTIONS:
            with self.subTest(name=name):
                self.assertIs(getattr(authorizer, name), getattr(runtime, name))

    def test_facade_and_runtime_write_identical_json_bytes(self):
        record = {
            "schema": "anchorpatch.test/1",
            "items": [{"b": 2, "a": 1}],
            "flag": True,
        }
        expected = json.dumps(record, ensure_ascii=False).encode("utf-8")
        with tempfile.TemporaryDirectory() as temp_dir:
            facade_path = os.path.join(temp_dir, "facade.json")
            runtime_path = os.path.join(temp_dir, "runtime.json")
            authorizer._write_json_no_replace(facade_path, record)
            runtime._write_json_no_replace(runtime_path, record)
            with open(facade_path, "rb") as handle:
                facade_bytes = handle.read()
            with open(runtime_path, "rb") as handle:
                runtime_bytes = handle.read()
        self.assertEqual(facade_bytes, expected)
        self.assertEqual(runtime_bytes, expected)

    def test_incident_and_merge_order_parity(self):
        row = {
            "sample": "sample-a",
            "worker_launch_id": "worker-a",
            "semantic_root_id": "root-a",
            "event": "attempt_start",
            "call_kind": "chat",
            "provider_called": True,
        }
        expected = {
            "row_number": 7,
            "canonical_sha256": _canonical_record_sha256(row),
            "incident_kind": "dispatcher_parent_loss_open_attempt",
            "sample": "sample-a",
            "worker_launch_id": "worker-a",
            "semantic_root_id": "root-a",
            "event": "attempt_start",
            "call_kind": "chat",
            "provider_called": True,
        }
        self.assertEqual(
            json.dumps(authorizer._incident_entry(
                7, row, "dispatcher_parent_loss_open_attempt")),
            json.dumps(expected),
        )
        self.assertEqual(
            authorizer._incident_entry(
                7, row, "dispatcher_parent_loss_open_attempt"),
            runtime._incident_entry(
                7, row, "dispatcher_parent_loss_open_attempt"),
        )

        first = {
            "row_number": 2,
            "canonical_sha256": "b" * 64,
            "incident_kind": "second",
        }
        second = {
            "row_number": 1,
            "canonical_sha256": "a" * 64,
            "incident_kind": "first",
        }
        self.assertEqual(
            authorizer._merge_incident_entries([first], [second, first]),
            [second, first],
        )
        self.assertEqual(
            authorizer._merge_incident_entries([first], [second, first]),
            runtime._merge_incident_entries([first], [second, first]),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
