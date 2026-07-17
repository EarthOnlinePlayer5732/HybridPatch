"""HybridPatch executor tests.

Run from repo root:
  PYTHONUTF8=1 python src/test_hybrid_executor.py
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from hybrid_executor import apply_hybrid
from hybrid_gate import validate_hybrid_output, partial_acceptance_eligible
import hybrid_index
import hybrid_prompt
import splitters
from hybrid_prompt import (extract_hybrid_json, BODIES_HEADER,
                           build_hybrid_prompt, build_hybrid_repair_prompt,
                           classify_operation_family)
from hybrid_schema import (PROTOCOL, PROTOCOL_V1, PROTOCOL_V2, PROTOCOL_V3, PROTOCOL_V4,
                           PROTOCOL_V5, PROTOCOL_V6, PROTOCOL_V7, PROTOCOL_V8,
                           BODY_REF_PREFIX, PROTOCOL_BURDEN_LIMITS,
                           measure_protocol_burden, validate_hybrid_envelope)
from patch_schema import block_id_for
from splitters import split_struct2
from utils_context import stringify_context


def env(route, action_fields, protocol=PROTOCOL):
    if protocol == PROTOCOL_V8:
        footprints = {
            "local_patch": "few_precise_edits",
            "bulk_patch": "many_repeated_edits",
            "dsl_rules": "block_movement",
            "bounded_rewrite": "whole_file_change",
        }
        plan = {"task_family": route, "edit_footprint": footprints[route]}
    else:
        plan = {
            "task_family": route,
            "writable_files": ["out.txt"],
            "readonly_files": [],
            "target_files": ["out.txt"],
            "obligations": ["test"],
        }
    return {
        "protocol": protocol,
        "plan": plan,
        "action": dict({"route": route}, **action_fields),
    }


def assert_true(ok, msg):
    if not ok:
        raise AssertionError(msg)


def test_local_patch():
    ctx = {"out.txt": "alpha\nbeta\ngamma\n"}
    patch = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": "beta", "new_text": "BETA"}],
    })
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out["out.txt"] == "alpha\nBETA\ngamma\n", out)
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_bulk_patch():
    ctx = {"out.txt": "A USD\nB USD\nDROP me\n"}
    patch = env("bulk_patch", {
        "ops": [
            {"op": "replace_all", "old_text": "USD", "new_text": "EUR", "scope": ["out.txt"], "expected_count_exact": 2},
            {"op": "delete_lines_containing", "text": "DROP", "scope": ["out.txt"]},
        ],
    })
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out["out.txt"] == "A EUR\nB EUR\n", out)
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.hybrid["bytes_generated_by_model"] >= len("EUR".encode()) * 2, log.to_dict())


def test_dsl_copy_blocks():
    ctx = {"src.txt": "one\n\ntwo\n\nthree\n"}
    blocks = split_struct2(ctx["src.txt"].encode("utf-8"))
    bids = [block_id_for("src.txt", b.block_id) for b in blocks]
    patch = env("dsl_rules", {
        "rules": [{"rule": "copy_blocks", "output": "out.txt", "block_ids": [bids[2], bids[0]]}],
    })
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    expect = blocks[2].data.decode("utf-8") + blocks[0].data.decode("utf-8")
    assert_true(out["out.txt"] == expect, out)
    assert_true(log.hybrid["route"] == "dsl_rules", log.to_dict())


def test_dsl_distribution_violation_gate_v1():
    # v1 (old archives): distribute must cover every block; a partial assignment
    # is a distribution_missing_block route violation. Frozen for replay.
    ctx = {"src.txt": "one\n\ntwo\n"}
    blocks = split_struct2(ctx["src.txt"].encode("utf-8"))
    bid = block_id_for("src.txt", blocks[0].block_id)
    patch = env("dsl_rules", {
        "rules": [{"rule": "distribute_blocks", "assignments": [{"block_id": bid, "file": "out.txt"}]}],
    }, protocol=PROTOCOL_V1)
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    passed, errors = validate_hybrid_output(ctx, out, ["out.txt"], log)
    assert_true(not passed, errors)
    assert_true(any(e.startswith("route_violation:distribution_missing_block") for e in errors), errors)


def test_bounded_rewrite():
    ctx = {"src.txt": "old\n"}
    patch = env("bounded_rewrite", {
        "files": [{"file": "out.txt", "content": "new\n"}],
    })
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out == {"out.txt": "new\n"}, out)
    assert_true(log.hybrid["bounded_rewrite"], log.to_dict())
    assert_true(log.hybrid["bytes_generated_by_model"] == len("new\n".encode()), log.to_dict())


def test_effective_noop_gate():
    ctx = {"out.txt": "same\n"}
    patch = env("bulk_patch", {"ops": []})
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    passed, errors = validate_hybrid_output(ctx, out, ["out.txt"], log, require_effective_change=True)
    assert_true(not passed and "effective_noop" in errors, errors)


# Backslash-dense body (malware3-style YARA) that broke under nested-JSON
# transport: a single `\G` inside a JSON string is an invalid escape once the
# inner string is decoded. The @body: channel carries it verbatim.
_YARA_BODY = (
    'rule apt_turla {\n'
    '  strings:\n'
    '    $x7 = "\\\\.\\Global\\PIPE\\sdlrpc"\n'
    '    $re = /C:\\\\Windows\\\\System32\\\\.*\\.dll/\n'
    '  condition:\n'
    '    any of them\n'
    '}\n'
)


def _simulate_body_response(envelope, files):
    """Serialize an envelope + [FILE BODIES] section like the model would emit."""
    import json as _json
    body_ctx = {name: content for name, content in files.items()}
    return (
        "```json\n" + _json.dumps(envelope, ensure_ascii=False) + "\n```\n\n"
        + BODIES_HEADER + "\n" + stringify_context(body_ctx) + "\n"
    )


def test_bounded_rewrite_body_ref_roundtrip():
    # bounded_rewrite via @body: reference must reproduce the backslash-dense
    # body byte-for-byte through extract -> executor.
    ctx = {"apt_turla.yar": "old rule\n"}
    envelope = env("bounded_rewrite", {
        "files": [{"file": "apt_turla.yar", "content": BODY_REF_PREFIX + "apt_turla.yar"}],
    })
    raw = _simulate_body_response(envelope, {"apt_turla.yar": _YARA_BODY})
    parsed, meta = extract_hybrid_json(raw)
    assert_true(parsed is not None and meta["bodies"].get("apt_turla.yar") == _YARA_BODY, meta)
    out, log = apply_hybrid(ctx, parsed, ["apt_turla.yar"], bodies=meta["bodies"])
    assert_true(out.get("apt_turla.yar") == _YARA_BODY, repr(out.get("apt_turla.yar")))
    assert_true(log.ops_rejected == 0 and log.hybrid.get("body_ref_unresolved") == 0, log.to_dict())
    # The literal backslash sequence that YARA needs survived intact.
    assert_true("\\Global\\PIPE" in out["apt_turla.yar"], "backslashes corrupted")


def test_local_patch_body_ref_roundtrip():
    ctx = {"out.txt": "alpha\nPLACEHOLDER\ngamma\n"}
    repl = 'path = "C:\\Windows\\System32"\n'
    envelope = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": "PLACEHOLDER\n",
                 "new_text": BODY_REF_PREFIX + "snippet"}],
    })
    raw = _simulate_body_response(envelope, {"snippet": repl})
    parsed, meta = extract_hybrid_json(raw)
    out, log = apply_hybrid(ctx, parsed, ["out.txt"], bodies=meta["bodies"])
    assert_true(out["out.txt"] == "alpha\n" + repl + "gamma\n", repr(out["out.txt"]))
    assert_true(log.ops_rejected == 0, log.to_dict())


def test_body_ref_unresolved_rejected():
    # A dangling @body: reference (no matching fenced block) must be rejected,
    # never silently written as the literal sentinel string.
    ctx = {"out.txt": "old\n"}
    envelope = env("bounded_rewrite", {
        "files": [{"file": "out.txt", "content": BODY_REF_PREFIX + "missing"}],
    })
    out, log = apply_hybrid(ctx, envelope, ["out.txt"], bodies={})
    assert_true(log.ops_rejected == 1, log.to_dict())
    assert_true(log.hybrid.get("body_ref_unresolved") == 1, log.to_dict())
    assert_true("out.txt" not in out or BODY_REF_PREFIX not in out.get("out.txt", ""), out)


def test_inline_content_still_works():
    # Backward compatibility: inline content (no sentinel) is unchanged.
    ctx = {"src.txt": "old\n"}
    envelope = env("bounded_rewrite", {"files": [{"file": "out.txt", "content": "new\n"}]})
    out, log = apply_hybrid(ctx, envelope, ["out.txt"])
    assert_true(out == {"out.txt": "new\n"}, out)
    assert_true(log.ops_rejected == 0, log.to_dict())


def test_v2_local_old_text_body_ref():
    # Regression for the malware6 RT4 failure: a backslash-dense match anchor sent
    # via @body: must be resolved BEFORE matching (not matched literally as the
    # sentinel string). Both old_text and new_text ride the [FILE BODIES] channel.
    old_body = '$re = /C:\\\\Windows\\\\System32\\\\.*\\.dll/'
    new_body = '$re = /D:\\\\Program Files\\\\.*\\.exe/'
    ctx = {"rule.yar": "rule r {\n  strings:\n    " + old_body + "\n  condition:\n    any of them\n}\n"}
    envelope = env("local_patch", {
        "ops": [{"op": "replace", "file": "rule.yar",
                 "old_text": BODY_REF_PREFIX + "o", "new_text": BODY_REF_PREFIX + "n"}],
    })
    raw = _simulate_body_response(envelope, {"o": old_body, "n": new_body})
    parsed, meta = extract_hybrid_json(raw)
    out, log = apply_hybrid(ctx, parsed, ["rule.yar"], bodies=meta["bodies"])
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(new_body in out["rule.yar"] and old_body not in out["rule.yar"], repr(out["rule.yar"]))


def test_v2_local_old_text_body_ref_unresolved():
    # A dangling @body: match anchor is rejected as body_ref_not_found, never
    # matched literally (which would always be not_found and mislead diagnosis).
    ctx = {"out.txt": "alpha\nbeta\ngamma\n"}
    envelope = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt",
                 "old_text": BODY_REF_PREFIX + "missing", "new_text": "X"}],
    })
    out, log = apply_hybrid(ctx, envelope, ["out.txt"], bodies={})
    assert_true(log.ops_rejected == 1, log.to_dict())
    reasons = [d.get("reason") for d in log.reject_reasons()]
    assert_true("body_ref_not_found" in reasons, reasons)


def test_v2_bulk_old_text_body_ref():
    # Bulk replace_all match side also resolves @body:.
    ctx = {"out.txt": "path=C:\\a\\b\npath=C:\\a\\b\n"}
    old_body = "C:\\a\\b"
    envelope = env("bulk_patch", {
        "ops": [{"op": "replace_all", "old_text": BODY_REF_PREFIX + "o",
                 "new_text": "D:/x", "scope": ["out.txt"]}],
    })
    raw = _simulate_body_response(envelope, {"o": old_body})
    parsed, meta = extract_hybrid_json(raw)
    out, log = apply_hybrid(ctx, parsed, ["out.txt"], bodies=meta["bodies"])
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(out["out.txt"] == "path=D:/x\npath=D:/x\n", repr(out["out.txt"]))


def test_v2_local_whitespace_tolerant():
    # E1: exact match fails on a whitespace mismatch (two spaces on disk vs one in
    # the anchor); the v2 whitespace-tolerant ladder still matches uniquely.
    ctx = {"out.txt": "x = 1\ndef f():\n    return  42\n"}
    patch = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": "return 42", "new_text": "return 43"}],
    })  # default protocol = v2
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out["out.txt"] == "x = 1\ndef f():\n    return 43\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v2_local_cross_block_replace():
    # E2: an anchor that spans a struct2 block boundary is a hard not_found under
    # v1 block-local matching but resolves at the file level under v2.
    ctx = {"out.txt": "alpha\n\nbeta\n\ngamma\n"}
    blocks = split_struct2(ctx["out.txt"].encode("utf-8"))
    assert_true(len(blocks) >= 2, f"need multi-block fixture, got {len(blocks)}")
    file_text = "".join(b.data.decode("utf-8") for b in blocks)
    boundary_off = len(blocks[0].data.decode("utf-8"))
    # Widen a window centered on the block0/block1 boundary until it is unique.
    span = 2
    while span < len(file_text):
        s = max(0, boundary_off - span)
        e = min(len(file_text), boundary_off + span)
        boundary = file_text[s:e]
        if boundary.strip() and file_text.count(boundary) == 1:
            break
        span += 1
    assert_true(file_text.count(boundary) == 1, repr(boundary))
    # v1 must reject (within-block find can't see across the boundary).
    v1 = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": boundary, "new_text": "X"}],
    }, protocol=PROTOCOL_V1)
    _o1, l1 = apply_hybrid(ctx, v1, ["out.txt"])
    assert_true(l1.ops_rejected == 1, l1.to_dict())
    # v2 must accept and produce the correct cross-block edit.
    v2 = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": boundary, "new_text": "X"}],
    })
    out, log = apply_hybrid(ctx, v2, ["out.txt"])
    assert_true(out["out.txt"] == file_text.replace(boundary, "X"), repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v2_local_occurrence():
    # M1/M2: a non-unique anchor is rejected without occurrence, accepted with it.
    ctx = {"out.txt": "total\ntotal\ntotal\n"}
    no_occ = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": "total", "new_text": "sum"}],
    })
    _o, l = apply_hybrid(ctx, no_occ, ["out.txt"])
    assert_true(l.ops_rejected == 1, l.to_dict())
    with_occ = env("local_patch", {
        "ops": [{"op": "replace", "file": "out.txt", "old_text": "total", "occurrence": 2, "new_text": "sum"}],
    })
    out, log = apply_hybrid(ctx, with_occ, ["out.txt"])
    assert_true(out["out.txt"] == "total\nsum\ntotal\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())


def test_v2_distribute_partial_keeps_unassigned():
    # E3: under v2, distribute may assign only some blocks; unassigned blocks stay
    # in their source file (no distribution_missing_block violation).
    ctx = {"src.txt": "one\n\ntwo\n\nthree\n"}
    blocks = split_struct2(ctx["src.txt"].encode("utf-8"))
    assert_true(len(blocks) >= 3, f"need >=3 blocks, got {len(blocks)}")
    b0 = block_id_for("src.txt", blocks[0].block_id)
    patch = env("dsl_rules", {
        "rules": [{"rule": "distribute_blocks", "assignments": [{"block_id": b0, "file": "out.txt"}]}],
    })  # v2
    out, log = apply_hybrid(ctx, patch, ["out.txt", "src.txt"])
    violations = (log.hybrid or {}).get("route_violations") or []
    assert_true("distribution_missing_block" not in violations, violations)
    passed, errors = validate_hybrid_output(ctx, out, ["out.txt", "src.txt"], log)
    assert_true(passed, errors)
    assert_true("one" in out.get("out.txt", ""), out)
    assert_true("three" in out.get("src.txt", ""), out)  # unassigned tail preserved
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v3_occurrence_enumeration_snapshot():
    # The exp_20260706_hybridthink5 docker6 failure: the model enumerates
    # occurrence=1..4 against the document it saw. v2 resolves each op against
    # the mutating state (counts shrink as ops apply -> out_of_range rejects);
    # v3 resolves every op against the step-input snapshot, so all four apply.
    ctx = {"out.txt": ("COPY --from=libvips A\nCOPY --from=libvips B\n"
                       "COPY --from=libvips C\nCOPY --from=libvips D\n")}
    ops = [{"op": "replace", "file": "out.txt", "old_text": "COPY --from=libvips",
            "new_text": "COPY --from=media", "occurrence": k} for k in (1, 2, 3, 4)]
    # v2 regression lock: index shift rejects occurrences 3 and 4.
    v2 = env("local_patch", {"ops": [dict(op) for op in ops]}, protocol=PROTOCOL_V2)
    _o2, l2 = apply_hybrid(ctx, v2, ["out.txt"])
    assert_true(l2.ops_accepted == 2 and l2.ops_rejected == 2, l2.to_dict())
    reasons = {d["reason"] for d in l2.reject_reasons()}
    assert_true(reasons == {"occurrence_out_of_range"}, reasons)
    # v3: all four occurrences resolve on the snapshot and apply in one pass.
    v3 = env("local_patch", {"ops": [dict(op) for op in ops]}, protocol=PROTOCOL_V3)
    out, log = apply_hybrid(ctx, v3, ["out.txt"])
    assert_true(out["out.txt"] == ("COPY --from=media A\nCOPY --from=media B\n"
                                   "COPY --from=media C\nCOPY --from=media D\n"),
                repr(out["out.txt"]))
    assert_true(log.ops_accepted == 4 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v3_snapshot_not_shifted_by_earlier_edits():
    # An earlier op that consumes occurrence #1 must not shift a later op's
    # occurrence numbering (both are resolved against the same snapshot).
    ctx = {"out.txt": "alpha foo\nmid\nbeta foo\n"}
    patch = env("local_patch", {"ops": [
        {"op": "replace", "file": "out.txt", "old_text": "alpha foo", "new_text": "alpha bar"},
        {"op": "replace", "file": "out.txt", "old_text": "foo", "occurrence": 2, "new_text": "qux"},
    ]}, protocol=PROTOCOL_V3)
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out["out.txt"] == "alpha bar\nmid\nbeta qux\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v3_overlapping_span_rejected():
    # Two spans that overlap on the snapshot cannot both apply in one pass;
    # the later op is rejected explicitly instead of corrupting the file.
    ctx = {"out.txt": "abcdef\n"}
    patch = env("local_patch", {"ops": [
        {"op": "replace", "file": "out.txt", "old_text": "abcd", "new_text": "X"},
        {"op": "replace", "file": "out.txt", "old_text": "cdef", "new_text": "Y"},
    ]}, protocol=PROTOCOL_V3)
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out["out.txt"] == "Xef\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 1, log.to_dict())
    assert_true(log.reject_reasons()[0]["reason"] == "overlapping_span",
                log.reject_reasons())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v3_colocated_inserts_compose_in_op_order():
    # Two inserts anchored at the same position compose in op order (earlier
    # op's text leftmost) instead of being order-dependent or rejected.
    ctx = {"out.txt": "head\ntail\n"}
    patch = env("local_patch", {"ops": [
        {"op": "insert", "file": "out.txt", "position": "after", "anchor_text": "head", "new_text": "-A"},
        {"op": "insert", "file": "out.txt", "position": "after", "anchor_text": "head", "new_text": "-B"},
    ]}, protocol=PROTOCOL_V3)
    out, log = apply_hybrid(ctx, patch, ["out.txt"])
    assert_true(out["out.txt"] == "head-A-B\ntail\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def _unique_boundary_needle(ctx, filename):
    """A needle spanning the block0/block1 struct2 boundary, unique in the file."""
    blocks = split_struct2(ctx[filename].encode("utf-8"))
    assert_true(len(blocks) >= 2, f"need multi-block fixture, got {len(blocks)}")
    file_text = "".join(b.data.decode("utf-8") for b in blocks)
    boundary_off = len(blocks[0].data.decode("utf-8"))
    span = 2
    while span < len(file_text):
        s = max(0, boundary_off - span)
        e = min(len(file_text), boundary_off + span)
        needle = file_text[s:e]
        if needle.strip() and file_text.count(needle) == 1:
            return file_text, needle
        span += 1
    raise AssertionError("no unique boundary needle found")


def test_v4_bulk_cross_block_replace_all():
    # The docker6 RT3 / fonteng3 RT1 kept-context gap (FINDINGS §217): a literal
    # replace_all anchor that exists uniquely in the file but spans a struct2
    # block boundary. v3 bulk matches per block -> match_zero; v4 matches at the
    # file level like local_patch.
    ctx = {"out.txt": "alpha\n\nbeta\n\ngamma\n"}
    file_text, needle = _unique_boundary_needle(ctx, "out.txt")
    op = {"op": "replace_all", "old_text": needle, "new_text": "X",
          "scope": ["out.txt"], "expected_count_exact": 1}
    v3 = env("bulk_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V3)
    _o3, l3 = apply_hybrid(ctx, v3, ["out.txt"])
    assert_true(l3.ops_rejected == 1, l3.to_dict())
    assert_true(l3.reject_reasons()[0]["reason"] == "match_zero", l3.reject_reasons())
    v4 = env("bulk_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V4)
    out, log = apply_hybrid(ctx, v4, ["out.txt"])
    assert_true(out["out.txt"] == file_text.replace(needle, "X"), repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v4_bulk_replace_all_within_block_unchanged():
    # Within-block replace_all must produce byte-identical results under v4
    # (file-level matching is a superset, not a behavior change here).
    ctx = {"out.txt": "A USD\nB USD\nDROP me\n"}
    ops = [{"op": "replace_all", "old_text": "USD", "new_text": "EUR",
            "scope": ["out.txt"], "expected_count_exact": 2}]
    v3 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V3)
    o3, _l3 = apply_hybrid(ctx, v3, ["out.txt"])
    v4 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V4)
    o4, l4 = apply_hybrid(ctx, v4, ["out.txt"])
    assert_true(o3 == o4 == {"out.txt": "A EUR\nB EUR\nDROP me\n"}, (o3, o4))
    assert_true(l4.ops_accepted == 1 and l4.preservation_violations == 0, l4.to_dict())


def test_v4_bulk_delete_lines_equivalence():
    # delete_lines_containing under v4 resolves lines at the file level; on a
    # normal multi-block fixture the result is identical to v3.
    ctx = {"out.txt": "keep\n\nDROP a\n\nkeep2\nDROP b\n"}
    op = {"op": "delete_lines_containing", "text": "DROP", "scope": ["out.txt"]}
    v3 = env("bulk_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V3)
    o3, _l3 = apply_hybrid(ctx, v3, ["out.txt"])
    v4 = env("bulk_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V4)
    o4, l4 = apply_hybrid(ctx, v4, ["out.txt"])
    assert_true(o3 == o4 == {"out.txt": "keep\n\n\nkeep2\n"}, (o3, o4))
    assert_true(l4.ops_accepted == 1, l4.to_dict())
    accepted = [d for d in l4.op_details if d["status"] == "accepted"]
    assert_true(accepted[0].get("deleted_lines") == 2, accepted)
    assert_true(l4.preservation_violations == 0, l4.to_dict())


def test_v4_body_ref_whitespace_name():
    # mathlean2 RT2 (FINDINGS §217): the model embedded '\n\n' in the @body:
    # sentinel and the ref dangled. v4 whitespace-normalizes the ref before
    # resolution; v3 keeps its original reject behavior (replay lock).
    ctx = {"out.txt": "old\n"}
    ref = BODY_REF_PREFIX + "out.txt\n\n"
    bodies = {"out.txt": "new\n"}
    v3 = env("bounded_rewrite", {"files": [{"file": "out.txt", "content": ref}]},
             protocol=PROTOCOL_V3)
    _o3, l3 = apply_hybrid(ctx, v3, ["out.txt"], bodies=dict(bodies))
    assert_true(l3.ops_rejected == 1, l3.to_dict())
    assert_true(l3.hybrid.get("body_ref_unresolved") == 1, l3.to_dict())
    v4 = env("bounded_rewrite", {"files": [{"file": "out.txt", "content": ref}]},
             protocol=PROTOCOL_V4)
    out, log = apply_hybrid(ctx, v4, ["out.txt"], bodies=dict(bodies))
    assert_true(out == {"out.txt": "new\n"}, out)
    assert_true(log.ops_rejected == 0 and log.hybrid.get("body_ref_unresolved") == 0,
                log.to_dict())


def test_v4_ws_ladder_backslash_token():
    # latex2 RT5 secondary defect (FINDINGS §217): _unescape_ws turns the \r of
    # \ref into CR, so the v3 ws ladder cannot match LaTeX anchors at all. v4
    # tries raw tokens first, so a whitespace mismatch around \ref still lands.
    ctx = {"out.txt": "intro\nsee \\ref{fig:a}  end\ntail\n"}
    op = {"op": "replace", "file": "out.txt",
          "old_text": "see \\ref{fig:a} end", "new_text": "see \\ref{fig:b} end"}
    v3 = env("local_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V3)
    _o3, l3 = apply_hybrid(ctx, v3, ["out.txt"])
    assert_true(l3.ops_rejected == 1, l3.to_dict())
    assert_true(l3.reject_reasons()[0]["reason"] == "not_found", l3.reject_reasons())
    v4 = env("local_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V4)
    out, log = apply_hybrid(ctx, v4, ["out.txt"])
    assert_true(out["out.txt"] == "intro\nsee \\ref{fig:b} end\ntail\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v4_ws_ladder_overescaped_fallback():
    # The original v1 quirk the unescape exists for must still work under v4:
    # a needle carrying literal '\n' between tokens falls back to the unescaped
    # pattern when the raw-token pass matches nothing.
    ctx = {"out.txt": "alpha\nbeta\ngamma\n"}
    op = {"op": "replace", "file": "out.txt",
          "old_text": "alpha\\nbeta", "new_text": "ALPHA\nbeta"}
    v4 = env("local_patch", {"ops": [dict(op)]}, protocol=PROTOCOL_V4)
    out, log = apply_hybrid(ctx, v4, ["out.txt"])
    assert_true(out["out.txt"] == "ALPHA\nbeta\ngamma\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 0, log.to_dict())


def test_v5_bulk_snapshot_preserves_inserted_breadcrumb():
    # fonteng3 RT7 (FINDINGS §219): op0 rewrites a header to append a rename-
    # mapping comment; op1 replace_all old->new. Under v4 sequential mutation
    # op1 also rewrites the freshly inserted comment (breadcrumb destroyed);
    # under v5 snapshot resolution the comment is never re-matched.
    ctx = {"out.txt": "# GSUB\nlookup oldname A\nuse oldname B\n"}
    ops = [
        {"op": "replace_all", "old_text": "# GSUB",
         "new_text": "# GSUB\n# oldname -> newname", "scope": ["out.txt"]},
        {"op": "replace_all", "old_text": "oldname", "new_text": "newname",
         "scope": ["out.txt"]},
    ]
    v4 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V4)
    o4, l4 = apply_hybrid(ctx, v4, ["out.txt"])
    assert_true("# newname -> newname" in o4["out.txt"], repr(o4["out.txt"]))  # v4 lock: poisoned
    v5 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V5)
    o5, l5 = apply_hybrid(ctx, v5, ["out.txt"])
    assert_true(o5["out.txt"] == "# GSUB\n# oldname -> newname\nlookup newname A\nuse newname B\n",
                repr(o5["out.txt"]))
    assert_true(l5.ops_accepted == 2 and l5.ops_rejected == 0, l5.to_dict())
    assert_true(l5.preservation_violations == 0, l5.to_dict())
    # snapshot counting: op1 matched the 2 occurrences visible in the input
    accepted = [d for d in l5.op_details if d["status"] == "accepted"]
    assert_true(accepted[1].get("match_count") == 2, accepted)


def test_v5_bulk_overlapping_op_rejected():
    # Two replace_all ops whose snapshot spans overlap cannot both apply in one
    # pass; the later op is rejected explicitly instead of corrupting the file.
    ctx = {"out.txt": "abcd\n"}
    ops = [
        {"op": "replace_all", "old_text": "abc", "new_text": "X", "scope": ["out.txt"]},
        {"op": "replace_all", "old_text": "bcd", "new_text": "Y", "scope": ["out.txt"]},
    ]
    v5 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V5)
    out, log = apply_hybrid(ctx, v5, ["out.txt"])
    assert_true(out["out.txt"] == "Xd\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 1, log.to_dict())
    assert_true(log.reject_reasons()[0]["reason"] == "overlapping_span", log.reject_reasons())


def test_v5_bulk_delete_lines_duplicate_span_dedupes():
    # Two line filters legitimately hitting the same line: the shared span
    # dedupes (applied once) instead of rejecting the second op.
    ctx = {"out.txt": "keep\nDROP foo BAR\nkeep2\nBAR only\n"}
    ops = [
        {"op": "delete_lines_containing", "text": "DROP", "scope": ["out.txt"]},
        {"op": "delete_lines_containing", "text": "BAR", "scope": ["out.txt"]},
    ]
    v5 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V5)
    out, log = apply_hybrid(ctx, v5, ["out.txt"])
    assert_true(out["out.txt"] == "keep\nkeep2\n", repr(out["out.txt"]))
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v5_bulk_within_block_equivalence():
    # Independent literal renames must produce byte-identical results under
    # v4 sequential and v5 snapshot semantics.
    ctx = {"out.txt": "A USD\nB USD\nDROP me\n"}
    ops = [{"op": "replace_all", "old_text": "USD", "new_text": "EUR",
            "scope": ["out.txt"], "expected_count_exact": 2},
           {"op": "delete_lines_containing", "text": "DROP", "scope": ["out.txt"]}]
    o4, _l4 = apply_hybrid(ctx, env("bulk_patch", {"ops": [dict(o) for o in ops]},
                                    protocol=PROTOCOL_V4), ["out.txt"])
    o5, l5 = apply_hybrid(ctx, env("bulk_patch", {"ops": [dict(o) for o in ops]},
                                   protocol=PROTOCOL_V5), ["out.txt"])
    assert_true(o4 == o5 == {"out.txt": "A EUR\nB EUR\n"}, (o4, o5))
    assert_true(l5.ops_accepted == 2 and l5.ops_rejected == 0, l5.to_dict())


def test_v5_bulk_cross_block_still_matches():
    # D1 (file-level cross-block matching) carries into v5's snapshot matcher.
    ctx = {"out.txt": "alpha\n\nbeta\n\ngamma\n"}
    file_text, needle = _unique_boundary_needle(ctx, "out.txt")
    v5 = env("bulk_patch", {"ops": [{"op": "replace_all", "old_text": needle,
                                     "new_text": "X", "scope": ["out.txt"],
                                     "expected_count_exact": 1}]}, protocol=PROTOCOL_V5)
    out, log = apply_hybrid(ctx, v5, ["out.txt"])
    assert_true(out["out.txt"] == file_text.replace(needle, "X"), repr(out["out.txt"]))
    assert_true(log.ops_accepted == 1 and log.preservation_violations == 0, log.to_dict())


def test_v6_bulk_containment_subsumption():
    # circuit2 RT2 (FINDINGS §221): replace_all rewrites a parameter name
    # everywhere INCLUDING inside the .param definition line, and a
    # delete_lines op removes that line. On the snapshot the delete-line span
    # contains one replace span. v5 rejects the delete (overlapping_span,
    # kept-context in the relay); v6 subsumes: line deleted, other occurrences
    # replaced — the v4 sequential outcome.
    ctx = {"out.cir": ".param R_REF=0.025k\nR1 n1 n2 R_REF\n.end\n"}
    ops = [
        {"op": "replace_all", "old_text": "R_REF", "new_text": "0.025k",
         "scope": ["out.cir"], "expected_count_min": 2},
        {"op": "delete_lines_containing", "text": ".param", "scope": ["out.cir"]},
    ]
    v5 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V5)
    _o5, l5 = apply_hybrid(ctx, v5, ["out.cir"])
    assert_true(l5.ops_rejected == 1, l5.to_dict())
    assert_true(l5.reject_reasons()[0]["reason"] == "overlapping_span", l5.reject_reasons())
    v6 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, v6, ["out.cir"])
    assert_true(out["out.cir"] == "R1 n1 n2 0.025k\n.end\n", repr(out["out.cir"]))
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 0, log.to_dict())
    assert_true(log.preservation_violations == 0, log.to_dict())


def test_v6_bulk_containment_reverse_order():
    # Same shape but the delete op comes FIRST: the later replace span falls
    # inside an already-accepted delete-line span and is swallowed.
    ctx = {"out.cir": ".param R_REF=0.025k\nR1 n1 n2 R_REF\n.end\n"}
    ops = [
        {"op": "delete_lines_containing", "text": ".param", "scope": ["out.cir"]},
        {"op": "replace_all", "old_text": "R_REF", "new_text": "0.025k",
         "scope": ["out.cir"], "expected_count_min": 2},
    ]
    v6 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, v6, ["out.cir"])
    assert_true(out["out.cir"] == "R1 n1 n2 0.025k\n.end\n", repr(out["out.cir"]))
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 0, log.to_dict())


def test_v6_bulk_partial_overlap_still_rejected():
    # A replace span that only PARTIALLY overlaps a delete-line span is
    # genuinely order-dependent and must still reject under v6.
    ctx = {"out.txt": "xxaaa\nbbbyy\nccc\n"}
    ops = [
        {"op": "replace_all", "old_text": "aaa\nbbb", "new_text": "Z", "scope": ["out.txt"]},
        {"op": "delete_lines_containing", "text": "yy", "scope": ["out.txt"]},
    ]
    v6 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, v6, ["out.txt"])
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 1, log.to_dict())
    assert_true(log.reject_reasons()[0]["reason"] == "overlapping_span", log.reject_reasons())
    assert_true(out["out.txt"] == "xxZyy\nccc\n", repr(out["out.txt"]))


def test_v6_bulk_replace_replace_overlap_still_rejected():
    ctx = {"out.txt": "abcd\n"}
    ops = [
        {"op": "replace_all", "old_text": "abc", "new_text": "X", "scope": ["out.txt"]},
        {"op": "replace_all", "old_text": "bcd", "new_text": "Y", "scope": ["out.txt"]},
    ]
    v6 = env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, v6, ["out.txt"])
    assert_true(out["out.txt"] == "Xd\n", repr(out["out.txt"]))
    assert_true(log.ops_rejected == 1, log.to_dict())


def test_v6_gate_format_regression_json():
    # C2: a changed .json output that no longer parses is flagged under v6;
    # the identical output under a v5 envelope passes (replay lock).
    ctx = {"a.json": '{"k": 1}\n'}
    broken = '{"k": 1,,}\n'
    for proto, expect_error in ((PROTOCOL_V6, True), (PROTOCOL_V5, False)):
        envelope = env("bounded_rewrite", {"files": [{"file": "a.json", "content": broken}]},
                       protocol=proto)
        out, log = apply_hybrid(ctx, envelope, ["a.json"])
        passed, errors = validate_hybrid_output(ctx, out, ["a.json"], log)
        has = any(e.startswith("format_regression:a.json") for e in errors)
        assert_true(has == expect_error, (proto, errors))


def test_v6_gate_new_lintable_file_must_parse():
    ctx = {"src.txt": "data\n"}
    envelope = env("bounded_rewrite", {"files": [{"file": "new.json", "content": "{broken"}]},
                   protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, envelope, ["new.json"])
    _passed, errors = validate_hybrid_output(ctx, out, ["new.json"], log)
    assert_true(any(e.startswith("format_regression:new.json") for e in errors), errors)


def test_v6_gate_broken_input_no_baseline():
    # Non-regression semantics: if the step INPUT already fails the lint, the
    # output is not judged (we cannot regress from broken).
    ctx = {"a.json": "{already broken"}
    envelope = env("bounded_rewrite", {"files": [{"file": "a.json", "content": "{still broken"}]},
                   protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, envelope, ["a.json"])
    _passed, errors = validate_hybrid_output(ctx, out, ["a.json"], log)
    assert_true(not any(e.startswith("format_regression") for e in errors), errors)


def test_v6_gate_good_output_passes():
    ctx = {"a.json": '{"k": 1}\n'}
    envelope = env("bounded_rewrite", {"files": [{"file": "a.json", "content": '{"k": 2}\n'}]},
                   protocol=PROTOCOL_V6)
    out, log = apply_hybrid(ctx, envelope, ["a.json"])
    passed, errors = validate_hybrid_output(ctx, out, ["a.json"], log)
    assert_true(passed and not errors, errors)


def test_v7_exec_parity_with_v6():
    # v7 executes with v6 semantics: C1 containment subsumption active,
    # snapshot bulk, same output byte-for-byte.
    ctx = {"out.txt": "keep\nfoo bar\nkeep2\n"}
    ops = [
        {"op": "replace_all", "old_text": "foo", "new_text": "FOO", "scope": ["out.txt"]},
        {"op": "delete_lines_containing", "text": "bar", "scope": ["out.txt"]},
    ]
    v6_out, v6_log = apply_hybrid(
        ctx, env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V6), ["out.txt"])
    v7_out, v7_log = apply_hybrid(
        ctx, env("bulk_patch", {"ops": [dict(o) for o in ops]}, protocol=PROTOCOL_V7), ["out.txt"])
    assert_true(v7_out == v6_out == {"out.txt": "keep\nkeep2\n"}, (v6_out, v7_out))
    assert_true(v7_log.ops_accepted == v6_log.ops_accepted == 2, v7_log.to_dict())


def test_v7_gate_format_regression_active():
    # C2 format-health gate stays on under v7.
    ctx = {"a.json": '{"k": 1}\n'}
    envelope = env("bounded_rewrite", {"files": [{"file": "a.json", "content": '{"k": 1,,}\n'}]},
                   protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, envelope, ["a.json"])
    _passed, errors = validate_hybrid_output(ctx, out, ["a.json"], log)
    assert_true(any(e.startswith("format_regression:a.json") for e in errors), errors)


def _v7_partial_case(protocol=PROTOCOL_V7):
    ctx = {"out.txt": "alpha\nbeta\ngamma\n"}
    envelope = env("local_patch", {"ops": [
        {"op": "replace", "file": "out.txt", "old_text": "beta", "new_text": "BETA"},
        {"op": "replace", "file": "out.txt", "old_text": "nosuchanchor", "new_text": "X"},
        {"op": "delete", "file": "out.txt", "old_text": "gamma\n"},
    ]}, protocol=protocol)
    out, log = apply_hybrid(ctx, envelope, ["out.txt"])
    return ctx, out, log


def test_v7_partial_acceptance_eligible():
    # fonteng3 RT7 shape: one dead op among valid ops — the partial output is
    # committed under the v7 policy (accepted ops applied, dead op skipped).
    ctx, out, log = _v7_partial_case()
    assert_true(out == {"out.txt": "alpha\nBETA\n"}, out)
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 1, log.to_dict())
    assert_true(partial_acceptance_eligible(ctx, out, ["out.txt"], log), log.to_dict())


def test_v7_partial_rev_gated():
    # Identical failure shape under a v6 envelope stays ineligible (replay lock).
    ctx, out, log = _v7_partial_case(protocol=PROTOCOL_V6)
    assert_true(log.ops_accepted == 2 and log.ops_rejected == 1, log.to_dict())
    assert_true(not partial_acceptance_eligible(ctx, out, ["out.txt"], log), log.to_dict())


def test_v7_partial_ineligible_zero_accepted():
    ctx = {"out.txt": "alpha\n"}
    envelope = env("local_patch", {"ops": [
        {"op": "replace", "file": "out.txt", "old_text": "nosuch", "new_text": "X"},
    ]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, envelope, ["out.txt"])
    assert_true(not partial_acceptance_eligible(ctx, out, ["out.txt"], log), log.to_dict())


def test_v7_partial_ineligible_no_rejects():
    # Clean execution is not "partial" — the normal commit path owns it.
    ctx = {"out.txt": "alpha\n"}
    envelope = env("local_patch", {"ops": [
        {"op": "replace", "file": "out.txt", "old_text": "alpha", "new_text": "ALPHA"},
    ]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, envelope, ["out.txt"])
    assert_true(log.ops_rejected == 0, log.to_dict())
    assert_true(not partial_acceptance_eligible(ctx, out, ["out.txt"], log), log.to_dict())


def test_v7_partial_ineligible_gate_error():
    # The partial output must pass every OTHER gate check: here the accepted
    # op breaks the JSON format, so C2 blocks partial acceptance.
    ctx = {"a.json": '{"k": 1}\n'}
    envelope = env("local_patch", {"ops": [
        {"op": "replace", "file": "a.json", "old_text": '"k": 1', "new_text": '"k": 1,,'},
        {"op": "replace", "file": "a.json", "old_text": "nosuch", "new_text": "X"},
    ]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, envelope, ["a.json"])
    assert_true(log.ops_accepted == 1 and log.ops_rejected == 1, log.to_dict())
    assert_true(not partial_acceptance_eligible(ctx, out, ["a.json"], log), out)


def test_v7_partial_ineligible_route():
    # bounded_rewrite has no op-local rescue: a dangling body ref drops the
    # file entirely; partial acceptance must not commit that.
    ctx = {"out.txt": "alpha\n", "other.txt": "keep\n"}
    envelope = env("bounded_rewrite", {"files": [
        {"file": "out.txt", "content": BODY_REF_PREFIX + "missing"},
        {"file": "other.txt", "content": "kept\n"},
    ]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, envelope, ["out.txt", "other.txt"])
    assert_true(log.ops_rejected >= 1, log.to_dict())
    assert_true(not partial_acceptance_eligible(ctx, out, ["out.txt", "other.txt"], log), out)


def test_v7_partial_ineligible_noop_output():
    # Accepted ops that net zero change: identical to kept-context, so the
    # policy declines (telemetry must not claim a rescue).
    ctx = {"out.txt": "alpha\nbeta\n"}
    envelope = env("local_patch", {"ops": [
        {"op": "replace", "file": "out.txt", "old_text": "beta", "new_text": "beta"},
        {"op": "replace", "file": "out.txt", "old_text": "nosuch", "new_text": "X"},
    ]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, envelope, ["out.txt"])
    assert_true(out == ctx, out)
    assert_true(not partial_acceptance_eligible(ctx, out, ["out.txt"], log), log.to_dict())


def test_v8_plan_route_mismatch_rejected():
    envelope = env("local_patch", {"ops": []}, protocol=PROTOCOL_V8)
    envelope["plan"]["edit_footprint"] = "many_repeated_edits"
    out, log = apply_hybrid({"out.txt": "x\n"}, envelope, ["out.txt"])
    assert_true(out == {} and log.error == "schema_error", log.to_dict())
    assert_true(any(e.startswith("plan_route_mismatch:")
                    for e in log.hybrid["schema_errors"]), log.to_dict())


def test_v8_plan_extra_fields_rejected():
    envelope = env("local_patch", {"ops": []}, protocol=PROTOCOL_V8)
    envelope["plan"]["target_files"] = ["out.txt"]
    errors, _warnings = validate_hybrid_envelope(envelope)
    assert_true(any("unsupported fields" in e for e in errors), errors)


def test_v8_local_requires_known_file():
    ctx = {"out.txt": "alpha\n"}
    for op in (
        {"op": "replace", "old_text": "alpha", "new_text": "ALPHA"},
        {"op": "replace", "file": "missing.txt", "old_text": "alpha", "new_text": "ALPHA"},
    ):
        envelope = env("local_patch", {"ops": [op]}, protocol=PROTOCOL_V8)
        out, log = apply_hybrid(ctx, envelope, ["out.txt"])
        assert_true(out == {} and log.error == "schema_error", log.to_dict())
        assert_true(log.ops_accepted == 0 and log.ops_rejected == 0, log.to_dict())


def test_v8_local_file_cannot_be_overridden_by_legacy_selectors():
    ctx = {"a.txt": "alpha\n", "b.txt": "beta\n"}
    b_block = split_struct2(ctx["b.txt"].encode("utf-8"))[0]
    selectors = (
        {"block_id": block_id_for("b.txt", b_block.block_id)},
        {"scope": ["b.txt"]},
    )
    for selector in selectors:
        op = {
            "op": "replace", "file": "a.txt",
            "old_text": "beta", "new_text": "BETA",
        }
        op.update(selector)
        envelope = env("local_patch", {"ops": [op]}, protocol=PROTOCOL_V8)
        out, log = apply_hybrid(ctx, envelope, ["a.txt", "b.txt"])
        assert_true(out == {} and log.error == "schema_error", log.to_dict())
        assert_true(log.ops_accepted == 0 and log.ops_rejected == 0, log.to_dict())
        assert_true(any("unsupported for hybridpatch/8 local_patch" in error
                        for error in log.hybrid["schema_errors"]), log.to_dict())


def test_v8_bulk_requires_nonempty_scope():
    ctx = {"out.txt": "alpha alpha\n"}
    for scope_fields in ({}, {"scope": []}, {"scope": [""]}):
        op = {"op": "replace_all", "old_text": "alpha", "new_text": "A"}
        op.update(scope_fields)
        envelope = env("bulk_patch", {"ops": [op]}, protocol=PROTOCOL_V8)
        out, log = apply_hybrid(ctx, envelope, ["out.txt"])
        assert_true(out == {} and log.error == "schema_error", log.to_dict())
        assert_true(log.ops_accepted == 0 and log.ops_rejected == 0, log.to_dict())


def _assert_burden_rejected(ctx, envelope, metric, bodies=None):
    burden = measure_protocol_burden(envelope, bodies=bodies)
    assert_true(burden[metric] > PROTOCOL_BURDEN_LIMITS[metric], burden)
    out, log = apply_hybrid(ctx, envelope, list(ctx), bodies=bodies)
    assert_true(out == {} and log.error == "protocol_burden_exceeded", log.to_dict())
    assert_true(log.ops_accepted == 0 and log.ops_rejected == 0, log.to_dict())
    assert_true(any(e.startswith("protocol_burden_exceeded:" + metric + ":")
                    for e in log.hybrid["schema_errors"]), log.to_dict())


def test_v8_each_protocol_burden_limit_rejected_before_execution():
    ctx = {"out.txt": "x\n"}
    local_ops = [
        {"op": "replace", "file": "out.txt", "old_text": "x", "new_text": "x"}
        for _ in range(PROTOCOL_BURDEN_LIMITS["local_op_count"] + 1)
    ]
    _assert_burden_rejected(
        ctx, env("local_patch", {"ops": local_ops}, protocol=PROTOCOL_V8),
        "local_op_count")

    bulk_ops = [
        {"op": "replace_all", "scope": ["out.txt"], "old_text": "x", "new_text": "x"}
        for _ in range(PROTOCOL_BURDEN_LIMITS["bulk_op_count"] + 1)
    ]
    _assert_burden_rejected(
        ctx, env("bulk_patch", {"ops": bulk_ops}, protocol=PROTOCOL_V8),
        "bulk_op_count")

    anchor_body = "x" * (PROTOCOL_BURDEN_LIMITS["anchor_bytes"] + 1)
    anchor_env = env("local_patch", {"ops": [{
        "op": "replace", "file": "out.txt", "old_text": BODY_REF_PREFIX + "anchor",
        "new_text": "x",
    }]}, protocol=PROTOCOL_V8)
    _assert_burden_rejected(ctx, anchor_env, "anchor_bytes", bodies={"anchor": anchor_body})

    envelope_env = env("bounded_rewrite", {"files": [{
        "file": "out.txt", "content": "z" * PROTOCOL_BURDEN_LIMITS["envelope_bytes"],
    }]}, protocol=PROTOCOL_V8)
    _assert_burden_rejected(ctx, envelope_env, "envelope_bytes")

    ids = [f"out.txt:B{i}" for i in range(PROTOCOL_BURDEN_LIMITS["explicit_block_id_count"] + 1)]
    dsl_env = env("dsl_rules", {"rules": [{
        "rule": "copy_blocks", "output": "out.txt", "block_ids": ids,
    }]}, protocol=PROTOCOL_V8)
    _assert_burden_rejected(ctx, dsl_env, "explicit_block_id_count")


def test_v8_burden_limit_equality_is_allowed():
    body = "x" * PROTOCOL_BURDEN_LIMITS["anchor_bytes"]
    envelope = env("local_patch", {"ops": [{
        "op": "replace", "file": "out.txt", "old_text": BODY_REF_PREFIX + "anchor",
        "new_text": "x",
    }]}, protocol=PROTOCOL_V8)
    errors, _warnings = validate_hybrid_envelope(
        envelope, bodies={"anchor": body}, editable_filenames=["out.txt"])
    assert_true("protocol_burden_exceeded" not in errors, errors)


def test_v7_legacy_missing_ranges_and_over_budget_still_execute():
    # New v8 authorization declarations and burden budgets must never leak into
    # v7 replay. Both legacy workspace searches remain active here.
    ctx = {"out.txt": "alpha alpha\n"}
    local = env("local_patch", {"ops": [{
        "op": "replace", "old_text": "alpha alpha", "new_text": "beta",
    }]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, local, ["out.txt"])
    assert_true(out == {"out.txt": "beta\n"} and not log.error, log.to_dict())

    bulk = env("bulk_patch", {"ops": [{
        "op": "replace_all", "old_text": "alpha", "new_text": "A",
    }]}, protocol=PROTOCOL_V7)
    out, log = apply_hybrid(ctx, bulk, ["out.txt"])
    assert_true(out == {"out.txt": "A A\n"} and not log.error, log.to_dict())

    huge = env("local_patch", {"ops": [{
        "op": "replace", "file": "out.txt", "old_text": "alpha",
        "new_text": "A",
    } for _ in range(PROTOCOL_BURDEN_LIMITS["local_op_count"] + 1)]}, protocol=PROTOCOL_V7)
    errors, _warnings = validate_hybrid_envelope(huge)
    assert_true("protocol_burden_exceeded" not in errors, errors)


def test_v8_inherits_v7_execution_gate_and_partial_semantics():
    ctx = {"out.txt": "keep\nfoo bar\nkeep2\n"}
    ops = [
        {"op": "replace_all", "old_text": "foo", "new_text": "FOO", "scope": ["out.txt"]},
        {"op": "delete_lines_containing", "text": "bar", "scope": ["out.txt"]},
    ]
    v7_out, v7_log = apply_hybrid(
        ctx, env("bulk_patch", {"ops": [dict(o) for o in ops]}, PROTOCOL_V7), ["out.txt"])
    v8_out, v8_log = apply_hybrid(
        ctx, env("bulk_patch", {"ops": [dict(o) for o in ops]}, PROTOCOL_V8), ["out.txt"])
    assert_true(v8_out == v7_out == {"out.txt": "keep\nkeep2\n"}, (v7_out, v8_out))
    assert_true(v8_log.preservation_violations == v7_log.preservation_violations == 0,
                v8_log.to_dict())

    bad_json = {"a.json": '{"k": 1}\n'}
    envelope = env("bounded_rewrite", {"files": [{
        "file": "a.json", "content": '{"k": 1,,}\n',
    }]}, protocol=PROTOCOL_V8)
    out, log = apply_hybrid(bad_json, envelope, ["a.json"])
    _passed, errors = validate_hybrid_output(bad_json, out, ["a.json"], log)
    assert_true(any(e.startswith("format_regression:a.json") for e in errors), errors)

    source, partial, partial_log = _v7_partial_case(protocol=PROTOCOL_V8)
    assert_true(partial_acceptance_eligible(source, partial, ["out.txt"], partial_log),
                partial_log.to_dict())


def test_v8_v7_snapshot_body_ref_c2_and_partial_parity():
    snapshot_ctx = {"out.txt": "x x x\n"}
    snapshot_ops = [
        {"op": "replace", "file": "out.txt", "old_text": "x", "new_text": value,
         "occurrence": index}
        for index, value in enumerate(("A", "B", "C"), 1)
    ]
    v7_out, v7_log = apply_hybrid(
        snapshot_ctx,
        env("local_patch", {"ops": [dict(op) for op in snapshot_ops]}, PROTOCOL_V7),
        ["out.txt"])
    v8_out, v8_log = apply_hybrid(
        snapshot_ctx,
        env("local_patch", {"ops": [dict(op) for op in snapshot_ops]}, PROTOCOL_V8),
        ["out.txt"])
    assert_true(v8_out == v7_out == {"out.txt": "A B C\n"}, (v7_out, v8_out))
    assert_true(v7_log.preservation_violations == v8_log.preservation_violations == 0,
                (v7_log.to_dict(), v8_log.to_dict()))

    body_ctx = {"out.txt": "alpha\nbeta\n"}
    body_ops = [{
        "op": "replace", "file": "out.txt", "old_text": BODY_REF_PREFIX + "old",
        "new_text": BODY_REF_PREFIX + "new",
    }]
    bodies = {"old": "beta", "new": "BETA"}
    v7_out, v7_log = apply_hybrid(
        body_ctx, env("local_patch", {"ops": body_ops}, PROTOCOL_V7),
        ["out.txt"], bodies=bodies)
    v8_out, v8_log = apply_hybrid(
        body_ctx, env("local_patch", {"ops": body_ops}, PROTOCOL_V8),
        ["out.txt"], bodies=bodies)
    assert_true(v8_out == v7_out == {"out.txt": "alpha\nBETA\n"}, (v7_out, v8_out))

    json_ctx = {"a.json": '{"k": 1}\n'}
    bounded = {"files": [{"file": "a.json", "content": '{"k": 1,,}\n'}]}
    parity_errors = []
    for protocol in (PROTOCOL_V7, PROTOCOL_V8):
        out, log = apply_hybrid(
            json_ctx, env("bounded_rewrite", bounded, protocol), ["a.json"])
        _passed, errors = validate_hybrid_output(json_ctx, out, ["a.json"], log)
        parity_errors.append(errors)
    assert_true(parity_errors[0] == parity_errors[1], parity_errors)

    v7_src, v7_partial, v7_partial_log = _v7_partial_case(PROTOCOL_V7)
    v8_src, v8_partial, v8_partial_log = _v7_partial_case(PROTOCOL_V8)
    assert_true(v8_src == v7_src and v8_partial == v7_partial, (v7_partial, v8_partial))
    assert_true(
        partial_acceptance_eligible(v7_src, v7_partial, ["out.txt"], v7_partial_log)
        and partial_acceptance_eligible(v8_src, v8_partial, ["out.txt"], v8_partial_log),
        (v7_partial_log.to_dict(), v8_partial_log.to_dict()))


def test_v8_all_routes_preserve_untouched_blocks():
    cases = [
        ({"out.txt": "alpha\nbeta\n"}, env("local_patch", {"ops": [{
            "op": "replace", "file": "out.txt", "old_text": "beta", "new_text": "BETA",
        }]}), ["out.txt"]),
        ({"out.txt": "alpha alpha\n"}, env("bulk_patch", {"ops": [{
            "op": "replace_all", "scope": ["out.txt"], "old_text": "alpha", "new_text": "A",
        }]}), ["out.txt"]),
        ({"src.txt": "one\n\ntwo\n"}, None, ["out.txt"]),
        ({"out.txt": "old\n"}, env("bounded_rewrite", {"files": [{
            "file": "out.txt", "content": "new\n",
        }]}), ["out.txt"]),
    ]
    blocks = split_struct2(cases[2][0]["src.txt"].encode("utf-8"))
    cases[2] = (cases[2][0], env("dsl_rules", {"rules": [{
        "rule": "copy_blocks", "output": "out.txt",
        "block_ids": [block_id_for("src.txt", blocks[0].block_id)],
    }]}), ["out.txt"])
    for ctx, envelope, targets in cases:
        _out, log = apply_hybrid(ctx, envelope, targets)
        assert_true(log.error is None and log.preservation_violations == 0, log.to_dict())


def test_v8_default_prompt_has_no_index_or_dsl():
    original = hybrid_prompt.build_hybrid_index

    def forbidden_index(*_args, **_kwargs):
        raise AssertionError("default prompt must not build an index")

    hybrid_prompt.build_hybrid_index = forbidden_index
    try:
        classification = classify_operation_family("Replace beta with BETA in the document.")
        prompt = build_hybrid_prompt(
            {"out.txt": "alpha\nbeta\n"}, "Replace beta with BETA in the document.",
            ["out.txt"], readonly_context={"reference.txt": "read only\n"},
            prompt_classification=classification)
    finally:
        hybrid_prompt.build_hybrid_index = original
    assert_true(classification["prompt_profile"] == "default", classification)
    for forbidden in ("[FILE INDEX]", "[BLOCK INDEX]", "dsl_rules"):
        assert_true(forbidden not in prompt, forbidden)
    for required in ("local_patch", "bulk_patch", "bounded_rewrite"):
        assert_true(required in prompt, required)


def test_v8_block_prompt_uses_only_coarse_index():
    original_coarse = hybrid_index.split_struct2
    original_medium = splitters.split_medium
    original_fine = splitters.split_fine
    calls = []

    def coarse(data):
        calls.append("coarse")
        return original_coarse(data)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unused medium/fine splitter was called")

    hybrid_index.split_struct2 = coarse
    splitters.split_medium = forbidden
    splitters.split_fine = forbidden
    try:
        classification = classify_operation_family("Sort the sections by title.")
        prompt = build_hybrid_prompt(
            {"out.txt": "B\n\nA\n"}, "Sort the sections by title.", ["out.txt"],
            prompt_classification=classification)
    finally:
        hybrid_index.split_struct2 = original_coarse
        splitters.split_medium = original_medium
        splitters.split_fine = original_fine
    assert_true(classification["operation_family"] == "sort", classification)
    assert_true(classification["prompt_profile"] == "block_movement", classification)
    assert_true(calls == ["coarse"], calls)
    for required in ("[BLOCK INDEX]", "| coarse |", "dsl_rules", "bounded_rewrite"):
        assert_true(required in prompt, required)
    for forbidden in ("[FILE INDEX]", "local_patch", "bulk_patch", "medium", "fine"):
        assert_true(forbidden not in prompt, forbidden)


def test_v8_movement_classifier_requires_structure_or_position_cue():
    lone = classify_operation_family("Move carefully and preserve everything.")
    explicit = classify_operation_family("Move this section after the introduction.")
    assert_true(lone["prompt_profile"] == "default", lone)
    assert_true(explicit["operation_family"] == "block_movement", explicit)
    assert_true(explicit["prompt_profile"] == "block_movement", explicit)


def test_v8_repair_prompt_is_relevant_and_compact():
    previous = env("local_patch", {"ops": [{
        "op": "replace", "file": "a.txt", "old_text": "old", "new_text": "new",
    }]}, protocol=PROTOCOL_V8)
    errors = ["protocol_burden_exceeded", "protocol_burden_exceeded:anchor_bytes:4081>4080"]
    prompt = build_hybrid_repair_prompt(
        errors, previous_envelope=previous,
        editable_context={"a.txt": "old\n", "unrelated.txt": "SECRET_UNRELATED\n"},
        edit_instruction="Update the requested token.", target_filenames=["a.txt"],
        readonly_filenames=["reference.txt"], current_route="local_patch")
    canonical = __import__("json").dumps(
        previous, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    for required in (
        "Update the requested token.", errors[1], "local_patch", "a.txt",
        "reference.txt", canonical,
    ):
        assert_true(required in prompt, required)
    for forbidden in ("SECRET_UNRELATED", "bulk_patch:", "dsl_rules:", "ORIGINAL RAW"):
        assert_true(forbidden not in prompt, forbidden)
    assert_true(prompt.count("Protocol burden fix:") == 1, prompt)


def main():
    tests = [
        test_local_patch,
        test_bulk_patch,
        test_dsl_copy_blocks,
        test_dsl_distribution_violation_gate_v1,
        test_bounded_rewrite,
        test_effective_noop_gate,
        test_bounded_rewrite_body_ref_roundtrip,
        test_local_patch_body_ref_roundtrip,
        test_body_ref_unresolved_rejected,
        test_inline_content_still_works,
        test_v2_local_old_text_body_ref,
        test_v2_local_old_text_body_ref_unresolved,
        test_v2_bulk_old_text_body_ref,
        test_v2_local_whitespace_tolerant,
        test_v2_local_cross_block_replace,
        test_v2_local_occurrence,
        test_v2_distribute_partial_keeps_unassigned,
        test_v3_occurrence_enumeration_snapshot,
        test_v3_snapshot_not_shifted_by_earlier_edits,
        test_v3_overlapping_span_rejected,
        test_v3_colocated_inserts_compose_in_op_order,
        test_v4_bulk_cross_block_replace_all,
        test_v4_bulk_replace_all_within_block_unchanged,
        test_v4_bulk_delete_lines_equivalence,
        test_v4_body_ref_whitespace_name,
        test_v4_ws_ladder_backslash_token,
        test_v4_ws_ladder_overescaped_fallback,
        test_v5_bulk_snapshot_preserves_inserted_breadcrumb,
        test_v5_bulk_overlapping_op_rejected,
        test_v5_bulk_delete_lines_duplicate_span_dedupes,
        test_v5_bulk_within_block_equivalence,
        test_v5_bulk_cross_block_still_matches,
        test_v6_bulk_containment_subsumption,
        test_v6_bulk_containment_reverse_order,
        test_v6_bulk_partial_overlap_still_rejected,
        test_v6_bulk_replace_replace_overlap_still_rejected,
        test_v6_gate_format_regression_json,
        test_v6_gate_new_lintable_file_must_parse,
        test_v6_gate_broken_input_no_baseline,
        test_v6_gate_good_output_passes,
        test_v7_exec_parity_with_v6,
        test_v7_gate_format_regression_active,
        test_v7_partial_acceptance_eligible,
        test_v7_partial_rev_gated,
        test_v7_partial_ineligible_zero_accepted,
        test_v7_partial_ineligible_no_rejects,
        test_v7_partial_ineligible_gate_error,
        test_v7_partial_ineligible_route,
        test_v7_partial_ineligible_noop_output,
        test_v8_plan_route_mismatch_rejected,
        test_v8_plan_extra_fields_rejected,
        test_v8_local_requires_known_file,
        test_v8_local_file_cannot_be_overridden_by_legacy_selectors,
        test_v8_bulk_requires_nonempty_scope,
        test_v8_each_protocol_burden_limit_rejected_before_execution,
        test_v8_burden_limit_equality_is_allowed,
        test_v7_legacy_missing_ranges_and_over_budget_still_execute,
        test_v8_inherits_v7_execution_gate_and_partial_semantics,
        test_v8_v7_snapshot_body_ref_c2_and_partial_parity,
        test_v8_all_routes_preserve_untouched_blocks,
        test_v8_default_prompt_has_no_index_or_dsl,
        test_v8_block_prompt_uses_only_coarse_index,
        test_v8_movement_classifier_requires_structure_or_position_cue,
        test_v8_repair_prompt_is_relevant_and_compact,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"RESULT: PASS ({len(tests)} tests)")


if __name__ == "__main__":
    main()
