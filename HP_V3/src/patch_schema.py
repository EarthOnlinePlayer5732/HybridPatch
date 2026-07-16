"""
AnchorPatch patch protocol — op definitions, block-table construction, validation.

The LLM emits a JSON object {"ops": [ <op>, ... ]}. Five op types:

  replace_exact {block_id, old_text, new_text}   in-place edit; old_text unique in block
  delete_exact  {block_id, old_text}             remove old_text (unique in block)
  insert_anchor {block_id, position, anchor_text, new_text}  position in {before, after}
  project_blocks{outputs:[{file, block_ids:[...]}]}  assemble output files from source blocks
  emit_file     {file, content}                  full new-file content (NOT byte-preserved)

block_id format: "<filename>:<seq>" where <seq> is splitters.Block.block_id (the
sequential index within that file). Globally unique across a multi-file context.
The content hash (Block.hash) is carried in the block table for anchor verification,
not used as the primary key (identical content across files would collide).

This module is intentionally dependency-light: it only needs splitters. The executor
(executor.py) re-validates and applies leniently; validate_patch here is advisory and
used by tests + the prompt-side sanity check.
"""
from splitters import split_struct2


OP_REPLACE = "replace_exact"
OP_DELETE = "delete_exact"
OP_INSERT = "insert_anchor"
OP_PROJECT = "project_blocks"
OP_EMIT = "emit_file"

EDIT_OPS = {OP_REPLACE, OP_DELETE, OP_INSERT}
ALL_OPS = EDIT_OPS | {OP_PROJECT, OP_EMIT}


def block_id_for(filename, seq):
    """Stable, globally-unique block id for the (filename, sequential index) pair."""
    return f"{filename}:{seq}"


def source_file_of(block_id):
    """Recover the filename from a block_id (filenames contain no ':')."""
    return block_id.rsplit(":", 1)[0]


def build_block_table(context, splitter_fn=split_struct2):
    """context: {filename: str}. Returns (block_table, file_blocks).

    block_table : {block_id: bytes}      — content bytes per block
    file_blocks : {filename: [Block]}    — ordered blocks per file (for reconstruction)
    """
    block_table = {}
    file_blocks = {}
    for filename, content in context.items():
        data = content.encode("utf-8")
        blocks = splitter_fn(data)
        file_blocks[filename] = blocks
        for b in blocks:
            block_table[block_id_for(filename, b.block_id)] = b.data
    return block_table, file_blocks


def count_occurrences(text, sub):
    """Occurrence count of sub in text; -1 marks an empty/invalid needle."""
    if not isinstance(sub, str) or sub == "":
        return -1
    return text.count(sub)


def _op_type(op):
    return op.get("op") if isinstance(op, dict) else None


def validate_patch(patch, block_table, target_filenames=None):
    """Advisory validation. Returns a list of error strings (empty => structurally valid).

    Checks: known op names; edit-op block_id existence; old_text/anchor_text uniqueness
    within the referenced block; one replace/delete per block (no conflicting edits);
    project/emit output filenames don't collide. Does NOT enforce target coverage
    (default reconstruction may satisfy it) — that is the runner's is_context_complete check.
    """
    errors = []
    if not isinstance(patch, dict):
        return ["patch is not a JSON object"]
    ops = patch.get("ops")
    if not isinstance(ops, list):
        return ["patch.ops missing or not a list"]

    def block_text(bid):
        return block_table[bid].decode("utf-8", errors="replace")

    edit_targets = {}   # block_id -> number of replace/delete ops
    out_files = set()
    for idx, op in enumerate(ops):
        t = _op_type(op)
        tag = f"ops[{idx}]({t})"
        if t not in ALL_OPS:
            errors.append(f"{tag}: unknown op")
            continue

        if t in (OP_REPLACE, OP_DELETE):
            bid = op.get("block_id")
            if bid not in block_table:
                errors.append(f"{tag}: block_id '{bid}' not found")
                continue
            old = op.get("old_text")
            c = count_occurrences(block_text(bid), old)
            if c == -1:
                errors.append(f"{tag}: empty/invalid old_text")
            elif c == 0:
                errors.append(f"{tag}: old_text not found in block")
            elif c > 1:
                errors.append(f"{tag}: old_text not unique ({c}x) in block")
            if t == OP_REPLACE and not isinstance(op.get("new_text"), str):
                errors.append(f"{tag}: new_text not a string")
            edit_targets[bid] = edit_targets.get(bid, 0) + 1

        elif t == OP_INSERT:
            bid = op.get("block_id")
            if bid not in block_table:
                errors.append(f"{tag}: block_id '{bid}' not found")
                continue
            if op.get("position") not in ("before", "after"):
                errors.append(f"{tag}: position must be 'before' or 'after'")
            c = count_occurrences(block_text(bid), op.get("anchor_text"))
            if c == -1:
                errors.append(f"{tag}: empty/invalid anchor_text")
            elif c == 0:
                errors.append(f"{tag}: anchor_text not found in block")
            elif c > 1:
                errors.append(f"{tag}: anchor_text not unique ({c}x) in block")
            if not isinstance(op.get("new_text"), str):
                errors.append(f"{tag}: new_text not a string")

        elif t == OP_PROJECT:
            outs = op.get("outputs")
            if not isinstance(outs, list):
                errors.append(f"{tag}: outputs not a list")
                continue
            for o in outs:
                f = (o or {}).get("file")
                bids = (o or {}).get("block_ids")
                if not f:
                    errors.append(f"{tag}: output missing 'file'")
                else:
                    out_files.add(f)
                if not isinstance(bids, list):
                    errors.append(f"{tag}: block_ids not a list for '{f}'")
                else:
                    for bid in bids:
                        if bid not in block_table:
                            errors.append(f"{tag}: block_id '{bid}' not found (file '{f}')")

        elif t == OP_EMIT:
            f = op.get("file")
            if not f:
                errors.append(f"{tag}: emit_file missing 'file'")
            elif f in out_files:
                errors.append(f"{tag}: file '{f}' already produced by another op")
            else:
                out_files.add(f)
            if not isinstance(op.get("content"), str):
                errors.append(f"{tag}: emit_file content not a string")

    for bid, c in edit_targets.items():
        if c > 1:
            errors.append(f"block '{bid}' targeted by {c} replace/delete ops (conflict)")

    return errors


# ===========================================================================
# v2 protocol (AP-hybrid-v2) — envelope, new ops, multi-scale ids, validator
#
# v2 PatchSet envelope (v1 stays {"ops": [...]}):
#   {"protocol":"v2", "task_family":"...", "routing_confidence":0.0,
#    "manifest":[{"block_id","action","destination"?}], "ops":[...]}
#
# v2 additionally allows the ops below; emit_file is BANNED in v2 (compose_file
# is the only controlled generation entry point).
# ===========================================================================
OP_REPLACE_ALL = "replace_all_exact"
OP_DELETE_ALL = "delete_all_exact"
OP_REPLACE_PAIRS = "replace_pairs"        # alias accepted at extraction: map_replace
OP_DELETE_LINES = "delete_lines_matching"
OP_REPLACE_LINES = "replace_lines_matching"
OP_REORDER = "reorder_blocks"
OP_COMPOSE = "compose_file"

V2_BULK_OPS = {OP_REPLACE_ALL, OP_DELETE_ALL, OP_REPLACE_PAIRS, OP_DELETE_LINES, OP_REPLACE_LINES}
V2_NEW_OPS = V2_BULK_OPS | {OP_REORDER, OP_COMPOSE}
ALL_OPS_V2 = EDIT_OPS | {OP_PROJECT} | V2_NEW_OPS   # no OP_EMIT

TASK_FAMILIES = {"local_replace", "dense_undo", "reorder", "split_project",
                 "classify_project", "format_convert", "mixed"}
MANIFEST_ACTIONS = {"project", "preserve", "delete", "edit"}
COMPOSE_PART_TYPES = {"copy_block", "copy_span", "generated_text", "separator",
                      "template_literal"}

_SCALE_PREFIX = {"C": "coarse", "M": "medium", "F": "fine"}


def parse_block_id(block_id):
    """Parse a (possibly multi-scale) block id.

    "file:12"  -> ("file", "coarse", 12)     (v1-compatible, no scale prefix)
    "file:F3"  -> ("file", "fine", 3)
    "file:M2"  -> ("file", "medium", 2)
    "file:C5"  -> ("file", "coarse", 5)
    Returns (None, None, None) on malformed input.
    """
    if not isinstance(block_id, str) or ":" not in block_id:
        return None, None, None
    fname, _, tail = block_id.rpartition(":")
    if not fname or not tail:
        return None, None, None
    scale = "coarse"
    if tail[0] in _SCALE_PREFIX and tail[1:].isdigit():
        scale, tail = _SCALE_PREFIX[tail[0]], tail[1:]
    if not tail.isdigit():
        return None, None, None
    return fname, scale, int(tail)


def scaled_block_id(filename, scale, seq):
    """Inverse of parse_block_id. coarse ids keep the v1 'file:seq' form."""
    if scale == "coarse":
        return f"{filename}:{seq}"
    pfx = {"medium": "M", "fine": "F"}[scale]
    return f"{filename}:{pfx}{seq}"


def _regex_lite_error(pattern):
    """Validate a delete_lines_matching 're:' pattern against the restricted
    subset:  literals, '.', '*', '+', '?', '^', '$', '\\d', '\\w', '\\s' (and
    upper-case negations), '\\' escapes of literal chars, and one-level [...]
    classes. Forbidden: groups, alternation, counted quantifiers, backrefs,
    lookaround. Returns an error string or None."""
    i, n = 0, len(pattern)
    in_class = False
    while i < n:
        ch = pattern[i]
        if ch == "\\":
            if i + 1 >= n:
                return "trailing backslash"
            nxt = pattern[i + 1]
            if nxt.isdigit():
                return "backreferences not allowed"
            i += 2
            continue
        if in_class:
            if ch == "[":
                return "nested character class"
            if ch == "]":
                in_class = False
            i += 1
            continue
        if ch == "[":
            in_class = True
        elif ch in "(){}|":
            return f"'{ch}' not allowed (regex-lite: no groups/alternation/counted quantifiers)"
        i += 1
    if in_class:
        return "unterminated character class"
    return None


def validate_envelope_v2(patch):
    """Structural check of the v2 PatchSet envelope. Returns (errors, warnings).
    Missing routing metadata is a WARNING (telemetry), not a repair trigger."""
    errors, warnings = [], []
    if not isinstance(patch, dict):
        return ["patch is not a JSON object"], warnings
    ops = patch.get("ops")
    if not isinstance(ops, list):
        errors.append("patch.ops missing or not a list")
    if patch.get("protocol") not in (None, "v2"):
        warnings.append(f"unexpected protocol '{patch.get('protocol')}'")
    if "protocol" not in patch:
        warnings.append("missing 'protocol'")
    tf = patch.get("task_family")
    if tf is None:
        warnings.append("missing 'task_family'")
    elif tf not in TASK_FAMILIES:
        warnings.append(f"unknown task_family '{tf}'")
    rc = patch.get("routing_confidence")
    if rc is not None and not (isinstance(rc, (int, float)) and 0.0 <= rc <= 1.0):
        warnings.append("routing_confidence not a number in [0,1]")
    man = patch.get("manifest")
    if man is not None and not isinstance(man, list):
        errors.append("manifest is not a list")
    return errors, warnings


def _validate_manifest_v2(manifest, known_ids, errors, warnings):
    for idx, ent in enumerate(manifest):
        tag = f"manifest[{idx}]"
        if not isinstance(ent, dict):
            errors.append(f"{tag}: not an object")
            continue
        bid = ent.get("block_id")
        if bid not in known_ids:
            errors.append(f"{tag}: block_id '{bid}' not found")
        action = ent.get("action")
        if action not in MANIFEST_ACTIONS:
            errors.append(f"{tag}: unknown action '{action}'")
        elif action == "project":
            dest = ent.get("destination")
            if not dest or not isinstance(dest, str):
                errors.append(f"{tag}: action 'project' requires a string 'destination'")


def validate_patch_v2(patch, known_ids, target_filenames=None):
    """Main-path structural validation for the v2 protocol (called by
    executor.apply_patch_v2 BEFORE execution; errors are repair triggers).

    known_ids: set of every addressable block_id across all scales.
    Checks types/fields/known ops/known ids only — content matching is the
    executor's job (its ladder + telemetry handle tolerant matching)."""
    errors, warnings = validate_envelope_v2(patch)
    ops = patch.get("ops") if isinstance(patch, dict) else None
    if not isinstance(ops, list):
        return errors, warnings
    man = patch.get("manifest")
    if isinstance(man, list):
        _validate_manifest_v2(man, known_ids, errors, warnings)

    out_files = set()
    for idx, op in enumerate(ops):
        t = _op_type(op)
        tag = f"ops[{idx}]({t})"
        if t == OP_EMIT:
            errors.append(f"{tag}: emit_file is banned in v2 — use compose_file")
            continue
        if t not in ALL_OPS_V2:
            errors.append(f"{tag}: unknown op")
            continue

        if t in (OP_REPLACE, OP_DELETE, OP_INSERT):
            bid = op.get("block_id")
            if bid not in known_ids:
                errors.append(f"{tag}: block_id '{bid}' not found")
            if t == OP_INSERT:
                if op.get("position") not in ("before", "after"):
                    errors.append(f"{tag}: position must be 'before' or 'after'")
                if not isinstance(op.get("anchor_text"), str) or not op.get("anchor_text"):
                    errors.append(f"{tag}: anchor_text missing/empty")
            else:
                if not isinstance(op.get("old_text"), str) or not op.get("old_text"):
                    errors.append(f"{tag}: old_text missing/empty")
            if t in (OP_REPLACE, OP_INSERT) and not isinstance(op.get("new_text"), str):
                errors.append(f"{tag}: new_text not a string")

        elif t in (OP_REPLACE_ALL, OP_DELETE_ALL):
            if not isinstance(op.get("old_text"), str) or not op.get("old_text"):
                errors.append(f"{tag}: old_text missing/empty")
            if t == OP_REPLACE_ALL and not isinstance(op.get("new_text"), str):
                errors.append(f"{tag}: new_text not a string")
            sc = op.get("scope")
            if sc is not None and not (isinstance(sc, list) and all(isinstance(s, str) for s in sc)):
                errors.append(f"{tag}: scope not a list of filenames")
            for k in ("expected_count_min", "expected_count_exact"):
                v = op.get(k)
                if v is not None and not (isinstance(v, int) and v >= 0):
                    errors.append(f"{tag}: {k} not a non-negative integer")

        elif t == OP_REPLACE_PAIRS:
            pairs = op.get("pairs")
            if not isinstance(pairs, list) or not pairs:
                errors.append(f"{tag}: pairs missing/empty")
            else:
                for pi, pr in enumerate(pairs):
                    if not isinstance(pr, dict) or not isinstance(pr.get("old_text"), str) \
                            or not pr.get("old_text") or not isinstance(pr.get("new_text"), str):
                        errors.append(f"{tag}: pairs[{pi}] needs non-empty old_text + new_text")
                    else:
                        ec = pr.get("expected_count")   # v2.2 optional per-pair blast-radius bound
                        if ec is not None and not (isinstance(ec, int) and ec >= 0):
                            errors.append(f"{tag}: pairs[{pi}] expected_count not a non-negative integer")

        elif t == OP_DELETE_LINES:
            pat = op.get("pattern")
            if not isinstance(pat, str) or not pat:
                errors.append(f"{tag}: pattern missing/empty")
            elif pat.startswith("re:"):
                err = _regex_lite_error(pat[3:])
                if err:
                    errors.append(f"{tag}: invalid regex-lite pattern ({err})")

        elif t == OP_REPLACE_LINES:
            pat = op.get("pattern")
            if not isinstance(pat, str) or not pat:
                errors.append(f"{tag}: pattern missing/empty")
            elif pat.startswith("re:"):
                err = _regex_lite_error(pat[3:])
                if err:
                    errors.append(f"{tag}: invalid regex-lite pattern ({err})")
            new_line = op.get("new_line")
            if not isinstance(new_line, str):
                errors.append(f"{tag}: new_line not a string")
            elif "\n" in new_line or "\r" in new_line:
                errors.append(f"{tag}: new_line must not include newline characters")
            sc = op.get("scope")
            if sc is not None and not (isinstance(sc, list) and all(isinstance(s, str) for s in sc)):
                errors.append(f"{tag}: scope not a list of filenames")
            for k in ("expected_count_min", "expected_count_exact"):
                v = op.get(k)
                if v is not None and not (isinstance(v, int) and v >= 0):
                    errors.append(f"{tag}: {k} not a non-negative integer")

        elif t == OP_REORDER:
            f = op.get("file")
            if not f:
                errors.append(f"{tag}: missing 'file'")
            bids = op.get("block_ids")
            if not isinstance(bids, list) or not bids:
                errors.append(f"{tag}: block_ids missing/empty")
            else:
                for bid in bids:
                    if bid not in known_ids:
                        errors.append(f"{tag}: block_id '{bid}' not found")

        elif t == OP_PROJECT:
            outs = op.get("outputs")
            if not isinstance(outs, list):
                errors.append(f"{tag}: outputs not a list")
                continue
            for o in outs:
                f = (o or {}).get("file")
                bids = (o or {}).get("block_ids")
                if not f:
                    errors.append(f"{tag}: output missing 'file'")
                else:
                    out_files.add(f)
                if not isinstance(bids, list):
                    errors.append(f"{tag}: block_ids not a list for '{f}'")
                else:
                    for bid in bids:
                        if bid not in known_ids:
                            errors.append(f"{tag}: block_id '{bid}' not found (file '{f}')")

        elif t == OP_COMPOSE:
            f = op.get("file")
            if not f:
                errors.append(f"{tag}: missing 'file'")
            elif f in out_files:
                errors.append(f"{tag}: file '{f}' already produced by another op")
            else:
                out_files.add(f)
            parts = op.get("parts")
            if not isinstance(parts, list) or not parts:
                errors.append(f"{tag}: parts missing/empty")
                continue
            for pi, part in enumerate(parts):
                ptag = f"{tag}.parts[{pi}]"
                if not isinstance(part, dict):
                    errors.append(f"{ptag}: not an object")
                    continue
                pt = part.get("type")
                if pt not in COMPOSE_PART_TYPES:
                    errors.append(f"{ptag}: unknown part type '{pt}'")
                elif pt == "copy_block":
                    if part.get("block_id") not in known_ids:
                        errors.append(f"{ptag}: block_id '{part.get('block_id')}' not found")
                elif pt == "copy_span":
                    if part.get("block_id") not in known_ids:
                        errors.append(f"{ptag}: block_id '{part.get('block_id')}' not found")
                    if not isinstance(part.get("start_text"), str) or not part.get("start_text") \
                            or not isinstance(part.get("end_text"), str) or not part.get("end_text"):
                        errors.append(f"{ptag}: copy_span needs non-empty start_text + end_text")
                elif pt in ("generated_text", "separator"):
                    if not isinstance(part.get("content"), str):
                        errors.append(f"{ptag}: content not a string")
                elif pt == "template_literal":
                    if not isinstance(part.get("template"), str):
                        errors.append(f"{ptag}: template not a string")
                    v = part.get("vars")
                    if v is not None and not isinstance(v, dict):
                        errors.append(f"{ptag}: vars not an object")
    return errors, warnings
