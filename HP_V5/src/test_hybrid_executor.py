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
from hybrid_gate import validate_hybrid_output
from hybrid_prompt import extract_hybrid_json, BODIES_HEADER
from hybrid_schema import (PROTOCOL, PROTOCOL_V1, PROTOCOL_V2, PROTOCOL_V3, PROTOCOL_V4,
                           PROTOCOL_V5, BODY_REF_PREFIX)
from patch_schema import block_id_for
from splitters import split_struct2
from utils_context import stringify_context


def env(route, action_fields, protocol=PROTOCOL):
    return {
        "protocol": protocol,
        "plan": {
            "task_family": route,
            "writable_files": ["out.txt"],
            "readonly_files": [],
            "target_files": ["out.txt"],
            "obligations": ["test"],
        },
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
    envelope["plan"]["writable_files"] = ["apt_turla.yar"]
    envelope["plan"]["target_files"] = ["apt_turla.yar"]
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
    envelope["plan"]["writable_files"] = ["rule.yar"]
    envelope["plan"]["target_files"] = ["rule.yar"]
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
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"RESULT: PASS ({len(tests)} tests)")


if __name__ == "__main__":
    main()
