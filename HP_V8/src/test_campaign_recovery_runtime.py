"""Regression checks for the campaign recovery runtime extraction."""

import ast
import hashlib
import inspect
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
RUNTIME_INTERNAL_FUNCTIONS = [
    "_latest_outcomes_by_worker",
]
RUNTIME_DIGEST_FUNCTIONS = MOVED_FUNCTIONS + RUNTIME_INTERNAL_FUNCTIONS


PRE_EXTRACTION_COMMIT = "26fefbb04f28ec2d134251dccabfedd5c8527cff"
EXPECTED_FUNCTION_DIGESTS = {
    "_read_json": (
        "b5b3846b12ef3e40557fba15e31f338557a4b6def43be088aa40cad39e67eb10",
        "512c34d7dfcd647f54518fd29eae68b65bd0b8b235b7708e0d9c6e25e919edd6",
    ),
    "_replace_with_sharing_retry": (
        "c97b3d466f27c5f0dfc06b0dfd6853f1a9aa007f4121f806ee8e03068c95d911",
        "b0d2ccde13bc72c8e7ecc314be4b367f49e98c9ef3867534d616aa5d41d0b8fa",
    ),
    "_copy_file_durable": (
        "2aa41396c4a987274bc8c03884700cfdb92dc2f45bd69652b47e3cae4ee140cf",
        "eb95b0d4993d4157943cdf233211614ab269a97733c7de9a3ed771a5b7ca3811",
    ),
    "_ensure_bound_file_copy": (
        "f2c6e356f8fb8b836123b038e664d6c3289c2aa89a48a30cee84b3a180e30bcf",
        "f4076eed7e8cbec5388c0b483d98dffd27553c576d95bfcf1b8d6b25c9361123",
    ),
    "_assert_dispatcher_parent_loss_path_budget": (
        "1c0b4bf71d3142cb478a9a8118017af427eb9a0d646f5496ad199a6bd4585367",
        "dfc415faafd2410cdcc398d700ed3250194c367b998d1e00a0eb55482ae6d19e",
    ),
    "_recoverable_jsonl_tail": (
        "2d7ff859d74439791a42ad498dd335f6a2cead51b3b842d9f6cd3076d9da0a3a",
        "46f3f4b33aff31c848423a5b36c5f168a007d19ba5d699f55146ec9e307f6c9d",
    ),
    "_assert_recoverable_jsonl_tail": (
        "a1ca8c58bc95d904cd5e0622942f6415f2fa32b2a625fa4c639a988187123526",
        "ea27d010814cb71562dd08596949348a3c7be55d051dc38447f6aea224e423f0",
    ),
    "_append_recoverable_jsonl_tail": (
        "1f52ee94bfe5027ddc48f25597fb2208694a63102cf3ffc983772466df91680c",
        "c45bbd79742b7de83247fe3c02964fbc074a1f1400b9d82044b7bfd8bf83eb99",
    ),
    "_write_json_no_replace": (
        "2a972fa3fcca1964e602ec0b05445de0e56d872604b5f0ea968be7740f3c3d1b",
        "8275ab348bed8cc9bd1e42047d029574111494d980ae5647205ceb634427e362",
    ),
    "_read_jsonl": (
        "f439599d338a293db53aa0f06227a8c5aaf915635a188ebb02b0a4a9ac822be3",
        "751bcba65eb5db373e7651b8fba3b9a14eb031f4658e922c624d3cf6d394cd4e",
    ),
    "_file_prefix_evidence": (
        "822c8870b715c5e635ee4f757e6be05e15573f72f749f73adddce60e4623a67c",
        "bc16e8d716e765b474e7b6d3bf78c6f2503de1c8e648668c96b10d9e2d4d4c74",
    ),
    "_git_changed_paths": (
        "293471dff3cc61776addf6f55ff4059d43053661503e6a82503d3f1194c17a48",
        "e731129006abd435539cebad952ab7118d16bd119796c63999efc8d8e42fa15e",
    ),
    "_incident_entry": (
        "668f85c9e51755d3d354ab5695206f015e9ef25e9ce65dc3f87ee1be56c98f82",
        "482f86ee94bf97c08b519991068f4ba5188b06e22e5f6e3fe7e8f85ecdd47858",
    ),
    "_latest_outcomes_by_worker": (
        "4a75994dec49d5564887436ce0cf19ccb614bea1d2a1a055927e3e64047feef4",
        "47ad4565872a9bc290ddd03aec61687b85e8631564e63e7e3b8c104526fcf47c",
    ),
    "_reconcile_deepseek_parent_loss_workers": (
        "921ec7ed3f89bcb815174d658be327b467b7992ab8fa59e2734b99f405ef5464",
        "ad3ae62201c91c68816672883c5cd664949f5f350789a8384467d8424e222304",
    ),
    "_record_stopless_registered_prelaunch_parent_loss": (
        "e9294da3a46bc048ea79c0d9bfdd48733a876d29c4992fcc732a8f467d5eb9e6",
        "c20d52cbfb0cb18b5646dafe0a78c82460251d1733ee9e918d17e199cdf38e28",
    ),
    "_apply_deepseek_parent_loss_recovery_plan": (
        "1478fb0b9671808b777da823ccdd587d8cabbf17ce1a2e2ff5d4c646fdcd2c29",
        "606f9db3541cacf84cbe377c54e68b7ab434ba26ae0e48c432ca67f103402f3c",
    ),
    "_deepseek_parent_loss_incidents": (
        "7a865d4840118ebdec5ffce62d35136653f37bc1278bd90b4ca7fd5553d61650",
        "f730b93b03dadcbd3183cc196b81bb2a9daedefb51dfea7628464d77b5deaebf",
    ),
    "_merge_incident_entries": (
        "f13362d44a91a99b68b747e84a40bf01ef8104e6d24f2e71417772cc90146910",
        "d242b9e66ce58a97ad513e5310ab077c81ffc7bbfb1691ccd8339f217f3b749b",
    ),
    "_merge_transport_sidecar_entries": (
        "9d09f5b24fe4b49bccc949bccbc662175d6e721692945b69c54fc31213848ced",
        "0d8bf57e9edecfd3935b6c3235104a5fb2ae5cb0a57ba8d5c36ed3c394a65c8e",
    ),
    "_unlink_with_sharing_retry": (
        "5fa50444829ebad52ce2d4b2f5cd28ce73c86995f0f0197c7cec7393f70b2879",
        "0dc36a4e3718b93b302efd619e8ba9329b67ec6a752ede17c493dde0453fd850",
    ),
}


def _canonical_source(source):
    return "\n".join(
        line.rstrip()
        for line in source.replace("\r\n", "\n").replace("\r", "\n").strip(
        ).splitlines()
    )


class CampaignRecoveryRuntimeTests(unittest.TestCase):
    def test_authorizer_reexports_runtime_functions(self):
        for name in MOVED_FUNCTIONS:
            with self.subTest(name=name):
                self.assertIs(getattr(authorizer, name), getattr(runtime, name))

    def test_moved_functions_match_pre_extraction_source_and_ast(self):
        self.assertEqual(
            set(RUNTIME_DIGEST_FUNCTIONS),
            set(EXPECTED_FUNCTION_DIGESTS),
        )
        for name in RUNTIME_DIGEST_FUNCTIONS:
            with self.subTest(name=name):
                runtime_source = inspect.getsource(getattr(runtime, name))
                source_digest = hashlib.sha256(
                    _canonical_source(runtime_source).encode("utf-8")
                ).hexdigest()
                ast_digest = hashlib.sha256(ast.dump(
                    ast.parse(runtime_source),
                    include_attributes=False,
                ).encode("utf-8")).hexdigest()
                self.assertEqual(
                    source_digest,
                    EXPECTED_FUNCTION_DIGESTS[name][0],
                    PRE_EXTRACTION_COMMIT,
                )
                self.assertEqual(
                    ast_digest,
                    EXPECTED_FUNCTION_DIGESTS[name][1],
                    PRE_EXTRACTION_COMMIT,
                )

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
