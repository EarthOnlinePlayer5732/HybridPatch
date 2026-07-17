"""HybridPatch prompt construction and JSON extraction."""

import json
import fnmatch
import os
import re
import sys
import unicodedata

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from hybrid_index import build_hybrid_index, format_block_table
from hybrid_schema import (
    PROTOCOL,
    PROTOCOL_VERSIONS,
    BODY_REF_PREFIX,
    ROUTE_LOCAL_PATCH,
    ROUTE_BULK_PATCH,
    ROUTE_DSL_RULES,
    ROUTE_BOUNDED_REWRITE,
    DSL_MAX_RULES,
    DSL_MAX_EXPLICIT_IDS,
    DSL_MAX_EXPANDED_ACTIONS,
)
from utils_context import stringify_context, format_file_names_for_prompt

BODIES_HEADER = "[FILE BODIES]"
OPERATION_FAMILY_CLASSIFIER = "operation_family_lexical/1"
PROMPT_PROFILE_DEFAULT = "default"
PROMPT_PROFILE_BLOCK_MOVEMENT = "block_movement"

# Fixed priority and fixed lexical inventory.  These terms describe document
# operations rather than any evaluator/domain.  Matching uses the normalized
# task text below; the result records every matched term even though the first
# matching family in this table selects ``operation_family``.
_OPERATION_FAMILY_LEXICON = (
    ("split", (
        ("split", r"\bsplit(?:s|ting)?\b"),
        ("separate", r"\bseparat(?:e|es|ed|ing)\b"),
        ("partition", r"\bpartition(?:s|ed|ing)?\b"),
        ("divide_into", r"\bdivid(?:e|es|ed|ing)\s+into\b"),
        ("拆分", r"拆分"),
        ("分割", r"分割"),
    )),
    ("merge", (
        ("merge", r"\bmerg(?:e|es|ed|ing)\b"),
        ("combine", r"\bcombin(?:e|es|ed|ing)\b"),
        ("consolidate", r"\bconsolidat(?:e|es|ed|ing)\b"),
        ("join", r"\bjoin(?:s|ed|ing)?\b"),
        ("合并", r"合并"),
    )),
    ("sort", (
        ("sort", r"\bsort(?:s|ed|ing)?\b"),
        ("reorder", r"\breorder(?:s|ed|ing)?\b"),
        ("order_by", r"\border(?:ed|ing)?\s+by\b"),
        ("rank", r"\brank(?:s|ed|ing)?\b"),
        ("排序", r"排序"),
        ("重排", r"重排"),
    )),
    ("group", (
        ("group", r"\bgroup(?:s|ed|ing)?\b"),
        ("classify", r"\bclassif(?:y|ies|ied|ying)\b"),
        ("categorize", r"\bcategoriz(?:e|es|ed|ing)\b"),
        ("cluster", r"\bcluster(?:s|ed|ing)?\b"),
        ("分组", r"分组"),
        ("归类", r"归类"),
    )),
)

# A bare movement verb is too broad (for example, "move to JSON" describes a
# conversion).  The block-movement family therefore requires one verb AND one
# domain-neutral structure/location cue.  Both inventories are fixed and every
# triggering hit is recorded below.
_BLOCK_MOVEMENT_VERBS = (
    ("move", r"\bmov(?:e|es|ed|ing)\b"),
    ("relocate", r"\brelocat(?:e|es|ed|ing)\b"),
    ("redistribute", r"\bredistribut(?:e|es|ed|ing)\b"),
    ("distribute", r"(?<!re)\bdistribut(?:e|es|ed|ing)\b"),
    ("rearrange", r"\brearrang(?:e|es|ed|ing)\b"),
    ("移动", r"移动"),
    ("搬运", r"搬运"),
)
_BLOCK_MOVEMENT_CUES = (
    ("block", r"\bblocks?\b"),
    ("section", r"\bsections?\b"),
    ("paragraph", r"\bparagraphs?\b"),
    ("record", r"\brecords?\b"),
    ("entry", r"\bentr(?:y|ies)\b"),
    ("item", r"\bitems?\b"),
    ("chunk", r"\bchunks?\b"),
    ("line", r"\blines?\b"),
    ("row", r"\brows?\b"),
    ("heading", r"\bheadings?\b"),
    ("file", r"\bfiles?\b"),
    ("before", r"\bbefore\b"),
    ("after", r"\bafter\b"),
    ("above", r"\babove\b"),
    ("below", r"\bbelow\b"),
    ("start", r"\b(?:start|beginning|top)\b"),
    ("end", r"\b(?:end|bottom)\b"),
    ("结构", r"结构"),
    ("块", r"块"),
    ("段", r"段"),
    ("节", r"节"),
    ("记录", r"记录"),
    ("条目", r"条目"),
    ("行", r"行"),
    ("之前", r"之前"),
    ("之后", r"之后"),
    ("顶部", r"顶部"),
    ("底部", r"底部"),
    ("开头", r"开头"),
    ("末尾", r"末尾"),
)


def _normalize_instruction(text):
    normalized = unicodedata.normalize("NFKC", str(text or "")).casefold()
    return " ".join(normalized.split())


def classify_operation_family(edit_instruction):
    """Return a deterministic, auditable prompt-profile classification."""
    text = _normalize_instruction(edit_instruction)
    matches = []
    matched_families = []
    for family, terms in _OPERATION_FAMILY_LEXICON:
        family_hit = False
        for term, pattern in terms:
            count = len(re.findall(pattern, text))
            if count:
                matches.append({"family": family, "term": term, "count": count})
                family_hit = True
        if family_hit:
            matched_families.append(family)
    movement_verbs = []
    movement_cues = []
    for term, pattern in _BLOCK_MOVEMENT_VERBS:
        count = len(re.findall(pattern, text))
        if count:
            movement_verbs.append({
                "family": "block_movement", "term": term,
                "count": count, "role": "movement_verb",
            })
    for term, pattern in _BLOCK_MOVEMENT_CUES:
        count = len(re.findall(pattern, text))
        if count:
            movement_cues.append({
                "family": "block_movement", "term": term,
                "count": count, "role": "structure_or_position_cue",
            })
    if movement_verbs and movement_cues:
        matched_families.append("block_movement")
        matches.extend(movement_verbs)
        matches.extend(movement_cues)
    operation_family = matched_families[0] if matched_families else "default"
    return {
        "classifier": OPERATION_FAMILY_CLASSIFIER,
        "operation_family": operation_family,
        "prompt_profile": (
            PROMPT_PROFILE_BLOCK_MOVEMENT
            if matched_families else PROMPT_PROFILE_DEFAULT
        ),
        "matched_families": matched_families,
        "matches": matches,
    }


def _routes_for_profile(profile):
    if profile == PROMPT_PROFILE_DEFAULT:
        return (ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH, ROUTE_BOUNDED_REWRITE)
    if profile == PROMPT_PROFILE_BLOCK_MOVEMENT:
        return (ROUTE_DSL_RULES, ROUTE_BOUNDED_REWRITE)
    raise ValueError(f"unknown HybridPatch prompt profile: {profile!r}")


_ROUTE_FOOTPRINT = {
    ROUTE_LOCAL_PATCH: "few_precise_edits",
    ROUTE_BULK_PATCH: "many_repeated_edits",
    ROUTE_DSL_RULES: "block_movement",
    ROUTE_BOUNDED_REWRITE: "whole_file_change",
}

_BURDEN_REPAIR_ROUTES = {
    ROUTE_LOCAL_PATCH: (
        ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH, ROUTE_BOUNDED_REWRITE,
    ),
    ROUTE_BULK_PATCH: (ROUTE_BULK_PATCH, ROUTE_BOUNDED_REWRITE),
    ROUTE_DSL_RULES: (ROUTE_DSL_RULES, ROUTE_BOUNDED_REWRITE),
    ROUTE_BOUNDED_REWRITE: (ROUTE_BOUNDED_REWRITE,),
}


def _action_schema_lines(route):
    if route == ROUTE_LOCAL_PATCH:
        return [
            'local_patch: {"route":"local_patch","ops":[{"op":"replace","file":"...","old_text":"...","new_text":"..."}, {"op":"delete","file":"...","old_text":"..."}, {"op":"insert","file":"...","position":"before|after","anchor_text":"...","new_text":"..."}]}',
            'Every local op MUST name one concrete "file". old_text/anchor_text matches at file level and must be unique, or use a 1-based "occurrence". All occurrences are resolved against the document shown in this prompt.',
        ]
    if route == ROUTE_BULK_PATCH:
        return [
            'bulk_patch: {"route":"bulk_patch","ops":[{"op":"replace_all","old_text":"...","new_text":"...","scope":["..."],"expected_count_min":1}, {"op":"delete_lines_containing","text":"...","scope":["..."]}]}',
            'Every bulk op MUST declare a non-empty "scope". Bulk matching is literal, never regex, and never searches outside that declared scope.',
        ]
    if route == ROUTE_DSL_RULES:
        return [
            'dsl_rules: {"route":"dsl_rules","rules":[{"rule":"copy_blocks","output":"...","block_ids":["file:0"]}, {"rule":"distribute_blocks","assignments":[{"block_id":"file:0","file":"out.txt"}],"discard_block_ids":[]}]}',
            f'Use only coarse ids from BLOCK INDEX for pure block copy/movement. Limits: rules<={DSL_MAX_RULES}, explicit_ids<={DSL_MAX_EXPLICIT_IDS}, expanded_actions<={DSL_MAX_EXPANDED_ACTIONS}.',
        ]
    if route == ROUTE_BOUNDED_REWRITE:
        return [
            f'bounded_rewrite: {{"route":"bounded_rewrite","files":[{{"file":"report.html","content":"{BODY_REF_PREFIX}report.html"}}]}}',
            'This is the explicit safety path for whole-file change, conversion, or generation. Declare every rewritten file. It is always available but is NEVER selected as a silent fallback.',
        ]
    raise ValueError(f"unknown HybridPatch route: {route!r}")


def _body_transport_lines(routes):
    fields = []
    if ROUTE_BOUNDED_REWRITE in routes:
        fields.append("content")
    if ROUTE_LOCAL_PATCH in routes:
        fields.extend(("new_text", "old_text", "anchor_text"))
    if ROUTE_BULK_PATCH in routes:
        fields.extend(("new_text", "old_text", "text"))
    fields = list(dict.fromkeys(fields))
    if not fields:
        return []
    return [
        "[FILE BODY TRANSPORT]",
        f'When {"/".join(fields)} contains backslashes, quotes, or more than a few lines, put "{BODY_REF_PREFIX}<name>" in that field and emit the literal text after the JSON:',
        BODIES_HEADER,
        "```<name>",
        "<verbatim body, no JSON escaping>",
        "```",
    ]


def _schema_sections(routes):
    footprints = [_ROUTE_FOOTPRINT[route] for route in routes]
    sections = [
        "[OUTPUT SCHEMA]",
        f'{{"protocol":"{PROTOCOL}","plan":{{"task_family":"...","edit_footprint":"..."}},"action":{{...}}}}',
        "",
        "plan has exactly two fields:",
        "- task_family: a short non-empty description of the requested operation family",
        "- edit_footprint: one of " + ", ".join(footprints),
        "The edit_footprint must match action.route.",
        "",
        "Allowed action routes for this prompt:",
    ]
    for route in routes:
        sections.extend(_action_schema_lines(route))
    body_lines = _body_transport_lines(routes)
    if body_lines:
        sections.extend([""] + body_lines)
    return sections


def build_hybrid_prompt(editable_context, edit_instruction, target_filenames,
                        readonly_context=None, prompt_classification=None):
    classification = prompt_classification or classify_operation_family(edit_instruction)
    profile = classification.get("prompt_profile")
    routes = _routes_for_profile(profile)
    sections = [
        "You are HybridPatch. Emit one JSON object only.",
        "The runner's target and read-only lists below are authoritative; do not repeat them in plan.",
        "The deterministic executor preserves undeclared source bytes and applies only the declared action.",
        f"Prompt profile: {profile} ({classification.get('classifier') or OPERATION_FAMILY_CLASSIFIER}; operation_family={classification.get('operation_family') or 'default'}).",
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
    if profile == PROMPT_PROFILE_BLOCK_MOVEMENT:
        index = build_hybrid_index(editable_context, include_files=False)
        sections += ["", "[BLOCK INDEX]", format_block_table(index), ""]
    sections += _schema_sections(routes)
    sections += ["", "Rules:"]
    if profile == PROMPT_PROFILE_DEFAULT:
        sections += [
            "- Use local_patch for a few precise edits and bulk_patch for repeated literal edits.",
            "- Use bounded_rewrite explicitly for whole-file change, conversion, or generation; it is always available and never a silent fallback.",
            "- Do not search outside each operation's declared file or scope.",
        ]
    else:
        sections += [
            "- Use dsl_rules only for pure coarse-block movement without content transformation.",
            "- Use bounded_rewrite explicitly when content must be transformed or generated; it is always available and never a silent fallback.",
        ]
    sections += [
        "- Never output read-only context files.",
        f"- Emit the JSON object in a ```json fenced block. If body references are used, follow it with {BODIES_HEADER}. No other prose.",
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


def _declared_relevant_files(envelope):
    names = set()
    action = envelope.get("action") if isinstance(envelope, dict) else None
    if not isinstance(action, dict):
        return names
    route = action.get("route")
    if route in (ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH):
        for op in action.get("ops") or []:
            if not isinstance(op, dict):
                continue
            if isinstance(op.get("file"), str):
                names.add(op["file"])
            for name in op.get("scope") or []:
                if isinstance(name, str):
                    names.add(name)
    elif route == ROUTE_BOUNDED_REWRITE:
        for item in action.get("files") or []:
            if isinstance(item, dict) and isinstance(item.get("file"), str):
                names.add(item["file"])
    elif route == ROUTE_DSL_RULES:
        for rule in action.get("rules") or []:
            if not isinstance(rule, dict):
                continue
            for key in ("output", "file"):
                if isinstance(rule.get(key), str):
                    names.add(rule[key])
            block_ids = list(rule.get("block_ids") or [])
            block_ids.extend(rule.get("discard_block_ids") or [])
            block_ids.extend(
                item.get("block_id") for item in (rule.get("assignments") or [])
                if isinstance(item, dict)
            )
            names.update(
                item.get("file") for item in (rule.get("assignments") or [])
                if isinstance(item, dict) and isinstance(item.get("file"), str)
            )
            for block_id in block_ids:
                if isinstance(block_id, str) and ":" in block_id:
                    names.add(block_id.rsplit(":", 1)[0])
    return names


def _relevant_editable_context(envelope, errors, editable_context, target_filenames,
                               current_route=None):
    editable_context = editable_context or {}
    if not envelope or current_route == ROUTE_BOUNDED_REWRITE:
        return {name: editable_context[name] for name in sorted(editable_context)}
    names = _declared_relevant_files(envelope)
    for target in target_filenames or []:
        if not isinstance(target, str):
            continue
        if target in editable_context:
            names.add(target)
        elif "*" in target or "?" in target:
            names.update(name for name in editable_context if fnmatch.fnmatch(name, target))
    error_text = "\n".join(str(error) for error in (errors or []))
    names.update(name for name in editable_context if name and name in error_text)
    return {name: editable_context[name] for name in sorted(names) if name in editable_context}


def _canonical_envelope(envelope):
    value = envelope if isinstance(envelope, dict) else None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _repair_routes(errors, previous_envelope, current_route,
                   prompt_classification, edit_instruction):
    """Choose the compact set of routes exposed to the single repair call."""
    classification = prompt_classification or classify_operation_family(edit_instruction)
    profile = classification.get("prompt_profile")
    try:
        profile_routes = _routes_for_profile(profile)
    except ValueError:
        classification = classify_operation_family(edit_instruction)
        profile_routes = _routes_for_profile(classification["prompt_profile"])

    action = previous_envelope.get("action") if isinstance(previous_envelope, dict) else None
    parsed_route = action.get("route") if isinstance(action, dict) else None
    route = parsed_route if parsed_route in _ROUTE_FOOTPRINT else current_route
    if route not in _ROUTE_FOOTPRINT:
        route = None

    burden_exceeded = any(
        "protocol_burden_exceeded" in str(error) for error in (errors or [])
    )
    if route is None:
        routes = profile_routes
    elif burden_exceeded:
        routes = _BURDEN_REPAIR_ROUTES[route]
    else:
        routes = (route,)
    return routes, route, classification


def _repair_route_format_lines(routes):
    lines = [
        f'{{"protocol":"{PROTOCOL}","plan":{{"task_family":"...",'
        '"edit_footprint":"<matching value below>"},"action":{...}}}',
        "Select exactly one allowed route. Its edit_footprint MUST use the matching value below.",
    ]
    for route in routes:
        lines.append(
            f'- {route} -> edit_footprint="{_ROUTE_FOOTPRINT[route]}"'
        )
        lines.extend(_action_schema_lines(route))
    return lines


def build_hybrid_repair_prompt(errors, *, previous_envelope=None,
                               editable_context=None, edit_instruction=None,
                               target_filenames=None, readonly_filenames=None,
                               prompt_classification=None, current_route=None):
    """Build the single compact V8 semantic-repair prompt.

    The raw provider response is intentionally not accepted.  Only its parsed,
    canonical envelope (or JSON null) is shown to the model.
    """
    err_list = [str(error) for error in (errors or [])]
    err_lines = "\n".join(f"- {error}" for error in err_list)
    routes, route, classification = _repair_routes(
        err_list, previous_envelope, current_route,
        prompt_classification, edit_instruction,
    )
    relevant = _relevant_editable_context(
        previous_envelope, err_list, editable_context, target_filenames,
        current_route=route)
    targets_json = json.dumps(list(target_filenames or []), ensure_ascii=False,
                              separators=(",", ":"))
    readonly_json = json.dumps(list(readonly_filenames or []), ensure_ascii=False,
                               separators=(",", ":"))
    sections = [
        "Your previous HybridPatch response failed validation.",
        "",
        "[TASK]",
        str(edit_instruction or ""),
        "",
        "[ERRORS]",
        err_lines or "- unknown error",
        "",
        "[CURRENT ROUTE FORMAT]" if len(routes) == 1 else "[ALLOWED ROUTE FORMATS]",
    ]
    sections += _repair_route_format_lines(routes)
    body_lines = _body_transport_lines(routes)
    if body_lines:
        sections += [""] + body_lines
    sections += [
        "",
        "[AUTHORITATIVE TARGET FILES]",
        targets_json,
        "",
        "[AUTHORITATIVE READ-ONLY FILES]",
        readonly_json,
        "",
        "[RELEVANT EDITABLE FILES]",
        stringify_context(relevant) if relevant else "null",
    ]
    if ROUTE_DSL_RULES in routes and relevant:
        index = build_hybrid_index(relevant, include_files=False)
        sections += ["", "[BLOCK INDEX]", format_block_table(index)]
    sections += [
        "",
        "[CANONICAL PREVIOUS ENVELOPE]",
        _canonical_envelope(previous_envelope),
        "",
        f"Repair prompt profile: {classification.get('prompt_profile')}",
    ]
    if any("protocol_burden_exceeded" in error for error in err_list):
        sections += [
            "",
            "Protocol burden fix: merge repeated operations or explicitly choose a more suitable route; never rely on truncation or silent fallback.",
        ]
    sections += [
        "",
        "Correct only the listed errors without changing the task semantics.",
        f"Output one corrected complete JSON object in a ```json fenced block; append {BODIES_HEADER} only when a body reference is used. No other prose.",
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
