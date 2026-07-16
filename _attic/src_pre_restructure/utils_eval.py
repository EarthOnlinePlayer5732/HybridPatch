"""LOCAL RECONSTRUCTION of the upstream DELEGATE-52 `utils_eval` helper.

Provenance (investigated 2026-07-10): `data/samples_delegate52/python1/testing.py`
does `from utils_eval import deep_compare_objects`, but no utils_eval.py exists
anywhere in public upstream artifacts — the HF dataset (microsoft/delegate52,
delegate52.jsonl: python1's `files` dict lacks it; the string appears once in
the whole 19MB file, inside python1's own import line) or the public GitHub
repo (full tree, 194 paths, no samples). The helper evidently lived outside
the exported sample folders in the upstream-internal workspace and was never
published. python1 is the only sample that imports it, so any public
reproduction of the benchmark crashes on python1 exactly as we did.

Contract reconstructed from the call site: testing.py runs the generated
analysis program, loads its JSON output, and returns
`deep_compare_objects(reference_object, generated_object)` directly as the
evaluation dict; its own error paths return
{"score": 0, "error": ..., "exact_match": 0, "detailed_error": ...}, so the
comparison must yield at least {"score": float 0..1, "exact_match": 0|1}.

Semantics chosen: reference-anchored recursive leaf comparison — score is the
fraction of reference leaves reproduced at the same path (dict keys matched by
name, list items by index, numbers with tiny tolerance); generated-side extras
do not add score but void exact_match. Both experiment arms are scored by this
same implementation, so paired comparisons remain internally consistent;
absolute python1 scores are disclosed as locally reconstructed.
"""

_NUM_REL_TOL = 1e-9


def _is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _leaf_equal(ref, gen):
    if _is_num(ref) and _is_num(gen):
        if ref == gen:
            return True
        return abs(ref - gen) <= _NUM_REL_TOL * max(abs(ref), abs(gen))
    return type(ref) is type(gen) and ref == gen


def _compare(ref, gen, path, stats):
    """Count reference leaves and matches; record mismatch paths."""
    if isinstance(ref, dict):
        gen_is_dict = isinstance(gen, dict)
        for key, rv in ref.items():
            sub = f"{path}.{key}" if path else str(key)
            _compare(rv, gen[key] if gen_is_dict and key in gen else _MISSING, sub, stats)
        if gen_is_dict and set(gen) - set(ref):
            stats["extras"] = True
        elif not gen_is_dict:
            stats["extras"] = True
    elif isinstance(ref, list):
        gen_is_list = isinstance(gen, list)
        for i, rv in enumerate(ref):
            sub = f"{path}[{i}]"
            _compare(rv, gen[i] if gen_is_list and i < len(gen) else _MISSING, sub, stats)
        if gen_is_list and len(gen) > len(ref):
            stats["extras"] = True
        elif not gen_is_list:
            stats["extras"] = True
    else:
        stats["total"] += 1
        if gen is not _MISSING and _leaf_equal(ref, gen):
            stats["matched"] += 1
        else:
            if len(stats["mismatches"]) < 20:
                stats["mismatches"].append(path or "<root>")


class _Missing:
    __slots__ = ()


_MISSING = _Missing()


def deep_compare_objects(reference_object, generated_object):
    stats = {"total": 0, "matched": 0, "extras": False, "mismatches": []}
    _compare(reference_object, generated_object, "", stats)
    if stats["total"] == 0:
        score = 1.0 if reference_object == generated_object else 0.0
    else:
        score = stats["matched"] / stats["total"]
    exact = 1 if (score == 1.0 and not stats["extras"]) else 0
    result = {
        "score": score,
        "exact_match": exact,
        "matched_leaves": stats["matched"],
        "total_leaves": stats["total"],
    }
    if stats["mismatches"]:
        result["mismatched_paths"] = stats["mismatches"]
    return result
