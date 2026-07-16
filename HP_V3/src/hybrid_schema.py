"""HybridPatch protocol schema helpers.

The HybridPatch method emits one envelope:
{"protocol":"hybridpatch/1","plan":{...},"action":{...}}

The schema here is intentionally structural. Content matching and deterministic
execution live in hybrid_executor.py; reference-free output checks live in
hybrid_gate.py.
"""

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
# apply_hybrid branches on the envelope's own protocol string, so old archives
# replay under their original semantics and new runs under v3 automatically.
PROTOCOL_V1 = "hybridpatch/1"
PROTOCOL_V2 = "hybridpatch/2"
PROTOCOL_V3 = "hybridpatch/3"
PROTOCOL_VERSIONS = {PROTOCOL_V1, PROTOCOL_V2, PROTOCOL_V3}
# Generation default (build_hybrid_prompt emits this); extraction accepts all.
PROTOCOL = PROTOCOL_V3

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

def rev_of(envelope):
    """Semantic revision selector: the envelope's own protocol string decides
    which executor semantics apply, so old archives (hybridpatch/1|2) replay
    under their original semantics and new runs (hybridpatch/3) under
    snapshot-resolved local_patch. Unknown/missing protocol defaults to v1
    (strict)."""
    proto = envelope.get("protocol") if isinstance(envelope, dict) else None
    if proto == PROTOCOL_V3:
        return PROTOCOL_V3
    return PROTOCOL_V2 if proto == PROTOCOL_V2 else PROTOCOL_V1


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


def validate_hybrid_envelope(envelope):
    """Return (errors, warnings). Errors are repair-triggering schema failures."""
    errors, warnings = [], []
    if not isinstance(envelope, dict):
        return ["envelope must be a JSON object"], warnings
    if envelope.get("protocol") not in PROTOCOL_VERSIONS:
        errors.append(f"protocol must be one of {sorted(PROTOCOL_VERSIONS)!r}")
    _check_plan(envelope.get("plan"), errors, warnings)
    action = envelope.get("action")
    if not isinstance(action, dict):
        errors.append("action must be an object")
        return errors, warnings
    route = action.get("route")
    if route not in ROUTES:
        errors.append(f"action.route unknown: {route!r}")
        return errors, warnings
    if route == ROUTE_LOCAL_PATCH:
        _check_local(action, errors)
    elif route == ROUTE_BULK_PATCH:
        _check_bulk(action, errors)
    elif route == ROUTE_DSL_RULES:
        _check_dsl(action, errors)
    elif route == ROUTE_BOUNDED_REWRITE:
        _check_bounded_rewrite(action, errors)
    return errors, warnings
