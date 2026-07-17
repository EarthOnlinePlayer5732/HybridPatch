"""HybridPatch protocol schema helpers.

The HybridPatch method emits one envelope:
{"protocol":"hybridpatch/1","plan":{...},"action":{...}}

The schema here is intentionally structural. Content matching and deterministic
execution live in hybrid_executor.py; reference-free output checks live in
hybrid_gate.py.
"""

import json

ROUTE_LOCAL_PATCH = "local_patch"
ROUTE_BULK_PATCH = "bulk_patch"
ROUTE_DSL_RULES = "dsl_rules"
ROUTE_BOUNDED_REWRITE = "bounded_rewrite"

ROUTES = {
    ROUTE_LOCAL_PATCH,
    ROUTE_BULK_PATCH,
    ROUTE_DSL_RULES,
    ROUTE_BOUNDED_REWRITE,
}

# Protocol versions. v1 = original strict block-local matching (frozen for
# byte-identical replay of pre-2026-07-04 archives). v2 = executor-first
# relaxations: file-level cross-block matching + whitespace-tolerant anchor
# ladder + distribute keeps unassigned blocks in place (see FINDINGS §214).
# v3 = v2 semantics plus snapshot-resolved local_patch: occurrence indices and
# uniqueness are resolved against the step-input document (not the mutating
# intermediate state), spans are checked non-overlapping and applied in one
# pass, so "occurrence":1..N over the same old_text means the N occurrences the
# model saw in the prompt (fixes the index-shift rejects seen in
# exp_20260706_hybridthink5 docker6).
# v4 = v3 semantics plus: bulk_patch matches at the whole-file level (a literal
# anchor spanning a struct2 block boundary resolves instead of match_zero —
# the docker6 RT3 / fonteng3 RT1 kept-context gap, FINDINGS §217); @body:
# reference names are whitespace-normalized before resolution (mathlean2 RT2);
# and the whitespace-tolerant ladder tries raw tokens before the
# _unescape_ws variant (so \ref/\rho are not corrupted to CR, latex2 RT5).
# v5 = v4 semantics plus snapshot-resolved bulk_patch: ALL bulk ops' matches
# and expected counts are resolved against the step-input document, so text
# inserted by an earlier op is never re-matched by a later one (the fonteng3
# RT7 self-poisoned rename breadcrumb, FINDINGS §219); cross-op span conflicts
# are rejected explicitly (identical delete-line spans dedupe), then all spans
# apply right-to-left in one pass, mirroring local_patch v3.
# v6 = v5 semantics plus: (C1) containment subsumption in bulk conflict
# resolution — a delete-lines span that fully contains a replace span subsumes
# it (net result equals v4 sequential outcome; the circuit2 RT2 self-inflict,
# FINDINGS §221) instead of rejecting; partial and replace-vs-replace overlaps
# still reject. (C2) the validation gate adds a reference-free format-health
# check (extension-keyed public-format lints, non-regression semantics) on v6
# outputs; failures flow into the existing single repair.
# v7 = v6 executor/gate semantics unchanged, plus the PARTIAL-ACCEPTANCE
# commit policy in runner/verify (design proof: FINDINGS §226 sweep over v6+v5
# archives): when the chosen attempt after the single repair still fails with
# op rejections ONLY (local/bulk route, >=1 accepted op, zero pure gate
# errors, no route violations, output != input), the partially-applied output
# is committed instead of kept-context. Snapshot semantics (v3 local / v5
# bulk) make accepted ops independent of rejected ones, so the partial
# application is well-defined; the preservation invariant is untouched.
# Eligibility hard-gates on this rev (hybrid_gate.partial_acceptance_eligible)
# so v6-and-older archives replay byte-identically.
# v8 keeps the v7 executor, preservation and partial-acceptance semantics, but
# reduces the model-facing plan and hard-bounds explicit protocol burden.
# apply_hybrid branches on the envelope's own protocol string, so old archives
# replay under their original semantics and new runs under v8 automatically.
PROTOCOL_V1 = "hybridpatch/1"
PROTOCOL_V2 = "hybridpatch/2"
PROTOCOL_V3 = "hybridpatch/3"
PROTOCOL_V4 = "hybridpatch/4"
PROTOCOL_V5 = "hybridpatch/5"
PROTOCOL_V6 = "hybridpatch/6"
PROTOCOL_V7 = "hybridpatch/7"
PROTOCOL_V8 = "hybridpatch/8"
PROTOCOL_VERSIONS = {PROTOCOL_V1, PROTOCOL_V2, PROTOCOL_V3, PROTOCOL_V4,
                     PROTOCOL_V5, PROTOCOL_V6, PROTOCOL_V7, PROTOCOL_V8}
# Generation default (build_hybrid_prompt emits this); extraction accepts all.
PROTOCOL = PROTOCOL_V8

# Sentinel prefix for file bodies transported outside the JSON envelope.
# A field carrying "@body:<name>" is resolved to the literal content of the
# matching fenced block in the [FILE BODIES] section, avoiding a second layer
# of JSON string escaping (which corrupted backslash-dense formats, see
# FINDINGS §213). Inline string content still works for short bodies and old
# archives.
BODY_REF_PREFIX = "@body:"

DSL_MAX_RULES = 16
DSL_MAX_EXPLICIT_IDS = 64
DSL_MAX_EXPANDED_ACTIONS = 128

# Frozen from the nearest-rank conditional P95 distribution of the 394 chosen
# and committed hybridpatch/7 envelopes in exp_20260711_hybridv7dev20full.
# These are v8-only protocol-burden limits; the older global DSL safety limits
# above deliberately retain their historical values.
PROTOCOL_BURDEN_LIMITS = {
    "local_op_count": 31,
    "bulk_op_count": 30,
    "anchor_bytes": 4080,
    "envelope_bytes": 2898,
    "explicit_block_id_count": 39,
}

V8_FOOTPRINT_ROUTES = {
    "few_precise_edits": ROUTE_LOCAL_PATCH,
    "many_repeated_edits": ROUTE_BULK_PATCH,
    "block_movement": ROUTE_DSL_RULES,
    "whole_file_change": ROUTE_BOUNDED_REWRITE,
}


def rev_of(envelope):
    """Semantic revision selector: the envelope's own protocol string decides
    which executor semantics apply. V1-V7 archives replay under their original
    revisions; new runs use v8. Unknown/missing protocol defaults to v1
    (strict)."""
    proto = envelope.get("protocol") if isinstance(envelope, dict) else None
    if proto in (PROTOCOL_V8, PROTOCOL_V7, PROTOCOL_V6, PROTOCOL_V5,
                 PROTOCOL_V4, PROTOCOL_V3, PROTOCOL_V2):
        return proto
    return PROTOCOL_V1


LOCAL_OPS = {"replace", "delete", "insert"}
BULK_OPS = {"replace_all", "delete_lines_containing"}
DSL_RULES = {"copy_blocks", "distribute_blocks"}


def route_of(envelope):
    if not isinstance(envelope, dict):
        return None
    action = envelope.get("action")
    if isinstance(action, dict):
        return action.get("route")
    return None


def task_family_of(envelope):
    plan = envelope.get("plan") if isinstance(envelope, dict) else None
    if isinstance(plan, dict):
        return plan.get("task_family")
    return None


def _list_of_str(value):
    return isinstance(value, list) and all(isinstance(x, str) for x in value)


def _check_plan(plan, errors, warnings):
    if not isinstance(plan, dict):
        errors.append("plan must be an object")
        return
    if not isinstance(plan.get("task_family"), str) or not plan.get("task_family"):
        warnings.append("plan.task_family missing or not a string")
    for key in ("writable_files", "readonly_files", "target_files", "obligations"):
        if key in plan and not _list_of_str(plan.get(key)):
            errors.append(f"plan.{key} must be a list of strings")


def _check_plan_v8(plan, route, errors):
    """Validate the deliberately minimal v8 plan and its route declaration."""
    if not isinstance(plan, dict):
        errors.append("plan must be an object")
        return
    expected = {"task_family", "edit_footprint"}
    actual = set(plan)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        if missing:
            errors.append("plan missing required fields: " + ",".join(missing))
        if extra:
            errors.append("plan has unsupported fields for hybridpatch/8: " + ",".join(extra))
    family = plan.get("task_family")
    if not isinstance(family, str) or not family.strip():
        errors.append("plan.task_family must be a non-empty string")
    footprint = plan.get("edit_footprint")
    if footprint not in V8_FOOTPRINT_ROUTES:
        errors.append(
            "plan.edit_footprint must be one of "
            + repr(sorted(V8_FOOTPRINT_ROUTES))
        )
    elif route is not None and V8_FOOTPRINT_ROUTES[footprint] != route:
        errors.append(
            "plan_route_mismatch:"
            f"{footprint} requires {V8_FOOTPRINT_ROUTES[footprint]}, got {route}"
        )


def _check_local(action, errors):
    ops = action.get("ops")
    if not isinstance(ops, list):
        errors.append("action.ops must be a list for local_patch")
        return
    for i, op in enumerate(ops):
        tag = f"action.ops[{i}]"
        if not isinstance(op, dict):
            errors.append(f"{tag} must be an object")
            continue
        t = op.get("op")
        if t not in LOCAL_OPS:
            errors.append(f"{tag}.op unknown for local_patch: {t!r}")
            continue
        if op.get("block_id") is None and op.get("file") is not None and not isinstance(op.get("file"), str):
            errors.append(f"{tag}.file must be a string when present")
        if op.get("block_id") is not None and not isinstance(op.get("block_id"), str):
            errors.append(f"{tag}.block_id must be a string when present")
        if t in ("replace", "delete"):
            if not isinstance(op.get("old_text"), str) or not op.get("old_text"):
                errors.append(f"{tag}.old_text must be a non-empty string")
        if t == "replace" and not isinstance(op.get("new_text"), str):
            errors.append(f"{tag}.new_text must be a string")
        if t == "insert":
            if op.get("position") not in ("before", "after"):
                errors.append(f"{tag}.position must be 'before' or 'after'")
            if not isinstance(op.get("anchor_text"), str) or not op.get("anchor_text"):
                errors.append(f"{tag}.anchor_text must be a non-empty string")
            if not isinstance(op.get("new_text"), str):
                errors.append(f"{tag}.new_text must be a string")
        occ = op.get("occurrence")
        if occ is not None and not (isinstance(occ, int) and occ >= 1):
            errors.append(f"{tag}.occurrence must be a positive integer")


def _check_local_v8(action, errors, editable_filenames=None):
    ops = action.get("ops")
    if not isinstance(ops, list):
        return
    known = set(editable_filenames) if editable_filenames is not None else None
    for i, op in enumerate(ops):
        if not isinstance(op, dict):
            continue
        tag = f"action.ops[{i}]"
        filename = op.get("file")
        if not isinstance(filename, str) or not filename.strip():
            errors.append(f"{tag}.file must be a non-empty string for hybridpatch/8 local_patch")
        elif known is not None and filename not in known:
            errors.append(f"{tag}.file is not an editable file: {filename}")
        # Legacy local operations could override ``file`` through either a
        # block id or a scope list.  V8 makes the concrete file declaration
        # the sole operation boundary, so ambiguous legacy selectors are a
        # schema error rather than an alternate path into another file.
        for legacy_selector in ("block_id", "scope"):
            if legacy_selector in op:
                errors.append(
                    f"{tag}.{legacy_selector} is unsupported for hybridpatch/8 "
                    "local_patch; use file only"
                )


def _check_bulk(action, errors):
    ops = action.get("ops")
    if not isinstance(ops, list):
        errors.append("action.ops must be a list for bulk_patch")
        return
    for i, op in enumerate(ops):
        tag = f"action.ops[{i}]"
        if not isinstance(op, dict):
            errors.append(f"{tag} must be an object")
            continue
        t = op.get("op")
        if t not in BULK_OPS:
            errors.append(f"{tag}.op unknown for bulk_patch: {t!r}")
            continue
        scope = op.get("scope")
        if scope is not None and not _list_of_str(scope):
            errors.append(f"{tag}.scope must be a list of strings")
        if t == "replace_all":
            if not isinstance(op.get("old_text"), str) or not op.get("old_text"):
                errors.append(f"{tag}.old_text must be a non-empty string")
            if not isinstance(op.get("new_text"), str):
                errors.append(f"{tag}.new_text must be a string")
        if t == "delete_lines_containing":
            if not isinstance(op.get("text"), str) or not op.get("text"):
                errors.append(f"{tag}.text must be a non-empty string")
        for key in ("expected_count_min", "expected_count_exact"):
            value = op.get(key)
            if value is not None and not (isinstance(value, int) and value >= 0):
                errors.append(f"{tag}.{key} must be a non-negative integer")


def _check_bulk_v8(action, errors, editable_filenames=None):
    ops = action.get("ops")
    if not isinstance(ops, list):
        return
    known = set(editable_filenames) if editable_filenames is not None else None
    for i, op in enumerate(ops):
        if not isinstance(op, dict):
            continue
        scope = op.get("scope")
        if (not isinstance(scope, list) or not scope
                or any(not isinstance(name, str) or not name.strip() for name in scope)):
            errors.append(
                f"action.ops[{i}].scope must be a non-empty list of non-empty strings "
                "for hybridpatch/8 bulk_patch"
            )
            continue
        if known is not None:
            for filename in scope:
                if filename not in known:
                    errors.append(
                        f"action.ops[{i}].scope contains non-editable file: {filename}"
                    )


def _check_dsl(action, errors):
    rules = action.get("rules")
    if not isinstance(rules, list):
        errors.append("action.rules must be a list for dsl_rules")
        return
    if len(rules) > DSL_MAX_RULES:
        errors.append(f"dsl rule count exceeds limit {DSL_MAX_RULES}")
    explicit_ids = 0
    expanded_actions = 0
    for i, rule in enumerate(rules):
        tag = f"action.rules[{i}]"
        if not isinstance(rule, dict):
            errors.append(f"{tag} must be an object")
            continue
        kind = rule.get("rule")
        if kind not in DSL_RULES:
            errors.append(f"{tag}.rule unknown: {kind!r}")
            continue
        if kind == "copy_blocks":
            output = rule.get("output") or rule.get("file")
            if not isinstance(output, str) or not output:
                errors.append(f"{tag}.output must be a non-empty string")
            block_ids = rule.get("block_ids")
            if not _list_of_str(block_ids):
                errors.append(f"{tag}.block_ids must be a list of strings")
            else:
                explicit_ids += len(block_ids)
                expanded_actions += len(block_ids)
        elif kind == "distribute_blocks":
            assignments = rule.get("assignments")
            if not isinstance(assignments, list):
                errors.append(f"{tag}.assignments must be a list")
            else:
                for j, item in enumerate(assignments):
                    if not isinstance(item, dict):
                        errors.append(f"{tag}.assignments[{j}] must be an object")
                        continue
                    if not isinstance(item.get("block_id"), str):
                        errors.append(f"{tag}.assignments[{j}].block_id must be a string")
                    if not isinstance(item.get("file"), str):
                        errors.append(f"{tag}.assignments[{j}].file must be a string")
                explicit_ids += len(assignments)
                expanded_actions += len(assignments)
            discard = rule.get("discard_block_ids") or []
            if not _list_of_str(discard):
                errors.append(f"{tag}.discard_block_ids must be a list of strings")
            else:
                explicit_ids += len(discard)
                expanded_actions += len(discard)
    if explicit_ids > DSL_MAX_EXPLICIT_IDS:
        errors.append(f"dsl explicit id count exceeds limit {DSL_MAX_EXPLICIT_IDS}")
    if expanded_actions > DSL_MAX_EXPANDED_ACTIONS:
        errors.append(f"dsl expanded action count exceeds limit {DSL_MAX_EXPANDED_ACTIONS}")


def _check_bounded_rewrite(action, errors):
    files = action.get("files")
    if not isinstance(files, list) or not files:
        errors.append("action.files must be a non-empty list for bounded_rewrite")
        return
    for i, item in enumerate(files):
        tag = f"action.files[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{tag} must be an object")
            continue
        if not isinstance(item.get("file"), str) or not item.get("file"):
            errors.append(f"{tag}.file must be a non-empty string")
        if not isinstance(item.get("content"), str):
            errors.append(f"{tag}.content must be a string")


def _canonical_envelope(envelope):
    try:
        return json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    except (TypeError, ValueError):
        return ""


def _resolved_burden_text(value, bodies):
    if not isinstance(value, str):
        return ""
    stripped = value.strip()
    if stripped.startswith(BODY_REF_PREFIX):
        name = stripped[len(BODY_REF_PREFIX):].strip()
        if isinstance(bodies, dict) and isinstance(bodies.get(name), str):
            return bodies[name]
    return value


def measure_protocol_burden(envelope, bodies=None):
    """Return deterministic explicit-protocol burden measurements.

    Anchor fields are measured after resolving a body reference. Reusing the
    same body in two anchor fields therefore counts twice, matching the amount
    of matching work explicitly requested by the envelope.
    """
    canonical = _canonical_envelope(envelope)
    action = envelope.get("action") if isinstance(envelope, dict) else None
    action = action if isinstance(action, dict) else {}
    route = action.get("route")
    ops = action.get("ops") if isinstance(action.get("ops"), list) else []
    rules = action.get("rules") if isinstance(action.get("rules"), list) else []
    files = action.get("files") if isinstance(action.get("files"), list) else []
    local_count = len(ops) if route == ROUTE_LOCAL_PATCH else 0
    bulk_count = len(ops) if route == ROUTE_BULK_PATCH else 0
    if route in (ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH):
        explicit_count = len(ops)
    elif route == ROUTE_DSL_RULES:
        explicit_count = len(rules)
    elif route == ROUTE_BOUNDED_REWRITE:
        explicit_count = len(files)
    else:
        explicit_count = 0

    anchor_bytes = 0
    if route == ROUTE_LOCAL_PATCH:
        keys = ("old_text", "anchor_text")
    elif route == ROUTE_BULK_PATCH:
        keys = ("old_text", "text")
    else:
        keys = ()
    for op in ops:
        if not isinstance(op, dict):
            continue
        for key in keys:
            if key in op:
                anchor_bytes += len(_resolved_burden_text(op.get(key), bodies).encode("utf-8"))

    explicit_ids = 0
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if rule.get("rule") == "copy_blocks":
            ids = rule.get("block_ids")
            if isinstance(ids, list):
                explicit_ids += sum(1 for value in ids if isinstance(value, str))
        elif rule.get("rule") == "distribute_blocks":
            assignments = rule.get("assignments")
            if isinstance(assignments, list):
                explicit_ids += sum(
                    1 for item in assignments
                    if isinstance(item, dict) and isinstance(item.get("block_id"), str)
                )
            discard = rule.get("discard_block_ids")
            if isinstance(discard, list):
                explicit_ids += sum(1 for value in discard if isinstance(value, str))

    return {
        "local_op_count": local_count,
        "bulk_op_count": bulk_count,
        "explicit_op_count": explicit_count,
        "anchor_bytes": anchor_bytes,
        "envelope_chars": len(canonical),
        "envelope_bytes": len(canonical.encode("utf-8")),
        "explicit_block_id_count": explicit_ids,
    }


def validate_hybrid_envelope(envelope, bodies=None, editable_filenames=None):
    """Return (errors, warnings). Errors are repair-triggering schema failures."""
    errors, warnings = [], []
    if not isinstance(envelope, dict):
        return ["envelope must be a JSON object"], warnings
    protocol = envelope.get("protocol")
    if protocol not in PROTOCOL_VERSIONS:
        errors.append(f"protocol must be one of {sorted(PROTOCOL_VERSIONS)!r}")
    action = envelope.get("action")
    if not isinstance(action, dict):
        if protocol == PROTOCOL_V8:
            _check_plan_v8(envelope.get("plan"), None, errors)
        else:
            _check_plan(envelope.get("plan"), errors, warnings)
        errors.append("action must be an object")
        return errors, warnings
    route = action.get("route")
    if protocol == PROTOCOL_V8:
        _check_plan_v8(envelope.get("plan"), route, errors)
    else:
        _check_plan(envelope.get("plan"), errors, warnings)
    if route not in ROUTES:
        errors.append(f"action.route unknown: {route!r}")
        return errors, warnings
    if route == ROUTE_LOCAL_PATCH:
        _check_local(action, errors)
        if protocol == PROTOCOL_V8:
            _check_local_v8(action, errors, editable_filenames=editable_filenames)
    elif route == ROUTE_BULK_PATCH:
        _check_bulk(action, errors)
        if protocol == PROTOCOL_V8:
            _check_bulk_v8(action, errors, editable_filenames=editable_filenames)
    elif route == ROUTE_DSL_RULES:
        _check_dsl(action, errors)
    elif route == ROUTE_BOUNDED_REWRITE:
        _check_bounded_rewrite(action, errors)

    if protocol == PROTOCOL_V8:
        burden = measure_protocol_burden(envelope, bodies=bodies)
        exceeded = [
            (key, burden[key], limit)
            for key, limit in PROTOCOL_BURDEN_LIMITS.items()
            if burden[key] > limit
        ]
        if exceeded:
            errors.append("protocol_burden_exceeded")
            errors.extend(
                f"protocol_burden_exceeded:{key}:{actual}>{limit}"
                for key, actual, limit in exceeded
            )
    return errors, warnings
