"""Reference-free validation and audit helpers for HybridPatch."""

import fnmatch
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from utils_context import is_context_complete
from hybrid_schema import (PROTOCOL_V6, PROTOCOL_V7, PROTOCOL_V8,
                           ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH)


# --- v6 format-health lints (C2, FINDINGS §221) -----------------------------
# Extension-keyed PUBLIC-format parsers (format-aware, domain-agnostic: the
# same class of check as json.loads — no task/sample-specific logic, no
# reference content). Reference-free semantics: a CHANGED output file must not
# regress from parseable to unparseable relative to the same step's input; a
# NEWLY created file of a lintable format must parse at all. Gated to
# hybridpatch/6 envelopes via the exec log's protocol_rev so older archives
# replay their original gate behavior byte-identically.

def _lint_json(text):
    import json
    json.loads(text)


def _lint_xml(text):
    import xml.etree.ElementTree as ET
    ET.fromstring(text)


def _lint_qasm(text):
    import openqasm3
    openqasm3.parse(text)


def _lint_pdb(text):
    # Strict Biopython parse: mis-padded fixed-width columns (e.g. a serial
    # shifted into the chain/resSeq columns, protein1 RT4 in FINDINGS §221)
    # raise instead of silently yielding zero atoms.
    from io import StringIO
    from Bio.PDB import PDBParser
    PDBParser(PERMISSIVE=False, QUIET=True).get_structure("x", StringIO(text))


_FORMAT_LINTS = {
    ".json": _lint_json,
    ".xml": _lint_xml,
    ".kml": _lint_xml,
    ".gpx": _lint_xml,
    ".svg": _lint_xml,
    ".qasm": _lint_qasm,
    ".pdb": _lint_pdb,
}


def _lint_passes(lint, text):
    """True/False on a parse verdict; None when the linter is unavailable
    (missing optional dependency) so callers treat it as no-signal."""
    try:
        lint(text)
        return True
    except ImportError:
        return None
    except Exception:
        return False


def _format_health_errors(source_context, output_context):
    errors = []
    for filename, content in (output_context or {}).items():
        lint = _FORMAT_LINTS.get(os.path.splitext(filename)[1].lower())
        if lint is None:
            continue
        src = (source_context or {}).get(filename)
        if src == content:
            continue  # untouched bytes cannot regress
        if src is not None and _lint_passes(lint, src) is not True:
            continue  # input already broken (or linter unavailable): no baseline
        if _lint_passes(lint, content or "") is False:
            errors.append("format_regression:" + filename)
    return errors


def _matches_any_target(filename, target_filenames):
    if not target_filenames:
        return True
    for t in target_filenames:
        if ("*" in t or "?" in t):
            if fnmatch.fnmatch(filename, t):
                return True
        elif filename == t:
            return True
    return False


def _begin_end_counts(text):
    b = sum(1 for line in text.split("\n") if line.lstrip().startswith("BEGIN:"))
    e = sum(1 for line in text.split("\n") if line.lstrip().startswith("END:"))
    return b, e


def _coarse_structure_errors(source_context, output_context):
    errors = []
    pairs = (("{", "}"), ("[", "]"), ("(", ")"))
    for filename, content in (output_context or {}).items():
        src = (source_context or {}).get(filename)
        if src is None:
            continue
        sb, se = _begin_end_counts(src)
        if sb and sb == se:
            ob, oe = _begin_end_counts(content)
            if ob != oe:
                errors.append(f"coarse_structure_unbalanced_begin_end:{filename}:{ob}vs{oe}")
        for op, cl in pairs:
            if src.count(op) and src.count(op) == src.count(cl):
                if content.count(op) != content.count(cl):
                    errors.append(
                        f"coarse_structure_unbalanced_pair:{filename}:{op}{cl}:{content.count(op)}vs{content.count(cl)}"
                    )
                    break
    return errors


def validate_hybrid_output(source_context, output_context, target_filenames, exec_log,
                           readonly_filenames=None, require_effective_change=False):
    errors = []
    output_context = output_context or {}
    readonly_filenames = list(readonly_filenames or [])
    if not output_context:
        errors.append("empty_output")
    if not is_context_complete(output_context, target_filenames):
        missing = [
            t for t in (target_filenames or [])
            if "*" not in t and "?" not in t and t not in output_context
        ]
        if missing:
            errors.append("missing_target_files:" + ",".join(missing))
    for filename in readonly_filenames:
        if filename in output_context:
            errors.append("readonly_output:" + filename)
    for filename, content in output_context.items():
        if not _matches_any_target(filename, target_filenames):
            errors.append("extra_output_file:" + filename)
        if not (content or "").strip():
            src = (source_context or {}).get(filename)
            if src is None or src.strip():
                errors.append("empty_file:" + filename)
    if require_effective_change and output_context == source_context:
        errors.append("effective_noop")
    errors.extend(_coarse_structure_errors(source_context, output_context))
    if ((getattr(exec_log, "hybrid", None) or {}).get("protocol_rev")) in (
            PROTOCOL_V6, PROTOCOL_V7, PROTOCOL_V8):
        errors.extend(_format_health_errors(source_context, output_context))
    route_violations = ((getattr(exec_log, "hybrid", None) or {}).get("route_violations") or [])
    for reason in route_violations:
        errors.append("route_violation:" + str(reason))
    if exec_log is not None and getattr(exec_log, "preservation_violations", 0):
        errors.append("preservation_violation")
    return (not errors), errors


def partial_acceptance_eligible(source_context, output_context, target_filenames, exec_log,
                                readonly_filenames=None, require_effective_change=False):
    """v7/v8 partial-acceptance policy (FINDINGS §226): a step whose ONLY failure
    class is op rejection commits the executor's partially-applied output
    (accepted ops applied, rejected ops skipped) instead of kept-context.

    Eligibility — every condition is deterministic from stored raw responses,
    so the honesty gate replays the exact same decision:
      - envelope protocol rev is hybridpatch/7 or hybridpatch/8 (older archives
        stay ineligible and replay byte-identically);
      - route is local_patch or bulk_patch (snapshot semantics make accepted
        ops independent of rejected ones; dsl/bounded have no op-local rescue);
      - at least one op was rejected AND at least one accepted;
      - no route violations;
      - the partial output differs from the step input;
      - the partial output passes EVERY other gate check (targets present,
        readonly untouched, coarse structure, C2 format health, effective
        change on forward steps): validate_hybrid_output returns zero errors.
    """
    if exec_log is None or not output_context:
        return False
    hy = getattr(exec_log, "hybrid", None) or {}
    if hy.get("protocol_rev") not in (PROTOCOL_V7, PROTOCOL_V8):
        return False
    if hy.get("route") not in (ROUTE_LOCAL_PATCH, ROUTE_BULK_PATCH):
        return False
    if hy.get("route_violations"):
        return False
    if not exec_log.reject_reasons() or exec_log.ops_accepted < 1:
        return False
    if output_context == source_context:
        return False
    _ok, errors = validate_hybrid_output(
        source_context, output_context, target_filenames, exec_log,
        readonly_filenames=readonly_filenames,
        require_effective_change=require_effective_change)
    return not errors


def _changed_text(source_context, output_context):
    chunks = []
    for filename, content in (output_context or {}).items():
        if (source_context or {}).get(filename) != content:
            chunks.append(filename)
            chunks.append(content or "")
    return "\n".join(chunks)


def _task_entities(edit_instruction):
    text = edit_instruction or ""
    entities = set()
    for token in re.findall(r"`([^`]+)`|'([^']+)'|\"([^\"]+)\"", text):
        for value in token:
            if value:
                entities.add(value.strip())
    for token in re.findall(r"\b[\w.-]+\.(?:txt|json|yaml|yml|xml|html|csv|tsv|md|tex|bib|py|js|sql|ledger|ics|srt|svg|dot|cir|net|fea|cif)\b", text, re.I):
        entities.add(token)
    for token in re.findall(r"\b[A-Z][A-Za-z0-9_-]{3,}\b", text):
        entities.add(token)
    return sorted(e for e in entities if e)


def audit_forward_completion(source_context, output_context, target_filenames, edit_instruction):
    changed = output_context != source_context
    produced = bool(is_context_complete(output_context or {}, target_filenames))
    changed_text = _changed_text(source_context, output_context)
    entities = _task_entities(edit_instruction)
    hits = [e for e in entities if e in changed_text]
    return {
        "effective_modification": changed,
        "target_files_present": produced,
        "task_entities": entities[:25],
        "task_entities_in_changed_region": hits[:25],
        "task_entity_hit_count": len(hits),
    }
