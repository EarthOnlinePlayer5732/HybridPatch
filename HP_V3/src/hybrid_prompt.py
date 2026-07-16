"""HybridPatch prompt construction and JSON extraction."""

import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from hybrid_index import build_hybrid_index, format_block_table, format_file_table
from hybrid_schema import (
    PROTOCOL,
    PROTOCOL_VERSIONS,
    BODY_REF_PREFIX,
    DSL_MAX_RULES,
    DSL_MAX_EXPLICIT_IDS,
    DSL_MAX_EXPANDED_ACTIONS,
)
from utils_context import stringify_context, format_file_names_for_prompt

BODIES_HEADER = "[FILE BODIES]"


def _schema_sections():
    """The envelope schema block, shared by the main prompt and the repair
    prompt. The repair prompt MUST restate the schema: when the original
    response is empty or garbled there is nothing to imitate, and without the
    schema the model invents its own patch format (observed in
    exp_20260707_hybridv3dev20: 19/50 repair responses had no protocol field)."""
    return [
        "[OUTPUT SCHEMA]",
        f'{{"protocol":"{PROTOCOL}","plan":{{...}},"action":{{...}}}}',
        "",
        "plan fields:",
        '- task_family: one of local_edit, bulk_literal, dsl_transform, bounded_rewrite, structure_distribution, mixed',
        "- writable_files: files you may create or modify",
        "- readonly_files: background files you must not output",
        "- target_files: required output filenames or wildcard patterns",
        "- obligations: short checklist of task requirements",
        "",
        "action.route options:",
        '1. local_patch: {"route":"local_patch","ops":[{"op":"replace","file":"...","old_text":"...","new_text":"..."}, {"op":"delete","file":"...","old_text":"..."}, {"op":"insert","file":"...","position":"before|after","anchor_text":"...","new_text":"..."}]}',
        '   old_text/anchor_text is matched at the FILE level (it may span structural blocks) and must occur EXACTLY ONCE in the file. If it is not unique, either extend it until it is unique, or add "occurrence":N (1-based) to pick the Nth match, e.g. {"op":"replace","file":"a.txt","old_text":"total","occurrence":2,"new_text":"sum"}. All ops are located against the document exactly as shown above — earlier ops in your list do NOT shift later occurrence numbers. To replace MANY/ALL occurrences of the same old_text, do not enumerate local_patch ops: use bulk_patch replace_all.',
        '2. bulk_patch: {"route":"bulk_patch","ops":[{"op":"replace_all","old_text":"...","new_text":"...","scope":["..."],"expected_count_min":1}, {"op":"delete_lines_containing","text":"...","scope":["..."]}]}',
        f'3. bounded_rewrite: {{"route":"bounded_rewrite","files":[{{"file":"report.html","content":"{BODY_REF_PREFIX}report.html"}}]}}',
        f'4. dsl_rules (rarely needed; ONLY pure block copy/move, never content transformation): {{"route":"dsl_rules","rules":[{{"rule":"copy_blocks","output":"...","block_ids":["file:0"]}}, {{"rule":"distribute_blocks","assignments":[{{"block_id":"file:0","file":"out.txt"}}],"discard_block_ids":[]}}]}}. block_ids are the coarse ids from the BLOCK INDEX. R-ENUM limits: rules<={DSL_MAX_RULES}, explicit_ids<={DSL_MAX_EXPLICIT_IDS}, expanded_actions<={DSL_MAX_EXPANDED_ACTIONS}. For any format/content transformation, use bounded_rewrite instead.',
        "",
        "[FILE BODY TRANSPORT]",
        f'For any text that contains backslashes, quotes, or is more than a few lines, DO NOT inline it as a JSON string. This applies to BOTH the content you write (content/new_text) AND the text you match against (old_text/anchor_text/text). Put a reference "{BODY_REF_PREFIX}<name>" in that field, and emit the literal text after the JSON in a {BODIES_HEADER} section using fenced blocks:',
        f"{BODIES_HEADER}",
        "```<name>",
        "<verbatim file body, no JSON escaping>",
        "```",
        f'The <name> in the fence must match the "{BODY_REF_PREFIX}<name>" reference exactly (usually the filename). Short literal replacements may still be written inline.',
    ]


def build_hybrid_prompt(editable_context, edit_instruction, target_filenames,
                        readonly_context=None):
    index = build_hybrid_index(editable_context)
    sections = [
        "You are HybridPatch. Emit one JSON object only.",
        "Do not rewrite every file unless the action route is bounded_rewrite and each rewritten file is declared.",
        "The deterministic executor will preserve undeclared source bytes and apply your declared action.",
        "",
        f"[TASK]\n{edit_instruction}",
        "",
        "[TARGET FILES]\n" + format_file_names_for_prompt(target_filenames),
        "",
        "[EDITABLE DOCUMENTS]",
        stringify_context(editable_context),
    ]
    if readonly_context:
        sections += [
            "",
            "[READ-ONLY CONTEXT - never output these files]",
            stringify_context(readonly_context),
        ]
    sections += [
        "",
        "[FILE INDEX]",
        format_file_table(index),
        "",
        "[BLOCK INDEX]",
        format_block_table(index, include_scales=("coarse",)),
        "",
    ]
    sections += _schema_sections()
    sections += [
        "",
        "Rules:",
        "1. Use local_patch for a few targeted edits, bulk_patch for literal repeated replacements or whole-line deletion, bounded_rewrite for whole-file generation or any content/format transformation. dsl_rules is only for pure block rearrangement and is rarely the right choice.",
        "2. local_patch anchors match at the file level and must be unique; if not unique, lengthen the anchor or set occurrence. Repeated identical replacements belong to bulk_patch replace_all, not per-occurrence local_patch ops.",
        "3. Bulk patch is literal only. Do not use regex. If a conversion requires computation per item, use bounded_rewrite.",
        "4. Bounded rewrite is not a fallback: every writable whole file must be declared in action.files.",
        "5. Never output read-only context files.",
        f"6. Emit the JSON object in a ```json fenced block. If you use body references, follow it with the {BODIES_HEADER} section. No other prose.",
        "",
        "```json",
    ]
    return "\n".join(sections)


def extract_file_bodies(text):
    """Parse the [FILE BODIES] section into {name: literal_content}.

    Bodies use the same variable-length fenced format as stringify_context, so
    backslash-dense content survives without any JSON escaping. Only fenced
    blocks that appear at or after the header are collected.
    """
    bodies = {}
    if not isinstance(text, str):
        return bodies
    idx = text.find(BODIES_HEADER)
    if idx < 0:
        return bodies
    section = text[idx + len(BODIES_HEADER):]
    for m in re.finditer(r"(`{3,})([^\n]+)\n(.*?)\1", section, re.DOTALL):
        name = m.group(2).strip()
        if name and name not in bodies:
            bodies[name] = m.group(3)
    return bodies


def build_hybrid_repair_prompt(original_raw, errors, editable_context=None,
                               edit_instruction=None):
    """One-shot repair prompt. Covers JSON/schema shape failures AND op-level
    failures (rejected anchors/occurrences, missing @body entries, gate errors).
    When the editable documents are supplied the model can re-ground its
    anchors instead of guessing from memory."""
    err_lines = "\n".join(f"- {e}" for e in (errors or [])[:25])
    sections = [
        "Your previous HybridPatch response failed validation.",
        "Fix ONLY what the errors below point at (JSON shape, schema fields, "
        "op anchors/occurrences, missing [FILE BODIES] entries, or missing "
        "target files). Do not change the intended task semantics.",
        "If an occurrence error persists or the same old_text must change in "
        "many places, switch that edit to bulk_patch replace_all.",
        "",
        "[ERRORS]",
        err_lines or "- unknown error",
        "",
    ]
    # Restate the full schema: if the previous response was empty or mangled
    # the model has no valid example to imitate and must not invent a format.
    sections += _schema_sections()
    if edit_instruction:
        sections += ["", "[TASK]", str(edit_instruction)]
    if editable_context:
        sections += ["", "[EDITABLE DOCUMENTS]", stringify_context(editable_context)]
    sections += [
        "",
        "[PREVIOUS RESPONSE]",
        str(original_raw or "")[:12000],
        "",
        f"Output the corrected complete response: the full JSON object in a "
        f"```json fenced block, followed by a {BODIES_HEADER} section when you "
        f"use {BODY_REF_PREFIX}<name> references. No other prose.",
        "",
        "```json",
    ]
    return "\n".join(sections)


def _normalize(obj):
    if not isinstance(obj, dict):
        return None
    if obj.get("protocol") in PROTOCOL_VERSIONS and isinstance(obj.get("action"), dict):
        return obj
    return None


def extract_hybrid_json(text):
    meta = {"partial_extraction": False, "fence_complete": False, "bodies": {}}
    if not isinstance(text, str) or not text.strip():
        return None, meta
    # File bodies live after a [FILE BODIES] header; parse them first so the
    # envelope decoders below never mistake a body fence for the json envelope.
    bodies_start = text.find(BODIES_HEADER)
    envelope_region = text[:bodies_start] if bodies_start >= 0 else text
    meta["bodies"] = extract_file_bodies(text)
    dec = json.JSONDecoder(strict=False)
    for m in re.finditer(r"```(?:json|JSON)?\s*\n?(.*?)```", envelope_region, re.DOTALL):
        cand = m.group(1).strip()
        if not cand:
            continue
        try:
            obj = _normalize(dec.decode(cand))
            if obj is not None:
                meta["fence_complete"] = True
                return obj, meta
        except Exception:
            pass
    for i, ch in enumerate(envelope_region):
        if ch != "{":
            continue
        try:
            obj, _ = dec.raw_decode(envelope_region, i)
        except Exception:
            continue
        obj = _normalize(obj)
        if obj is not None:
            meta["partial_extraction"] = True
            return obj, meta
    return None, meta
