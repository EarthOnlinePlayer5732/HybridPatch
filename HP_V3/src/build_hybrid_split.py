"""Build the HybridPatch dev/val/test split.

No API calls. The script reads only:
- CONTAMINATION_REGISTRY.json
- existing dev20 and holdout reserve split metadata
- sample.json prompt text for deterministic task-family coverage
"""

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

_DATA = os.path.join(_ROOT, "data")
SAMPLES_ROOT = os.path.join(_DATA, "samples_delegate52")
REGISTRY_PATH = os.path.join(_DATA, "CONTAMINATION_REGISTRY.json")
DEV20_PATH = os.path.join(_DATA, "research_splits", "dev20_20260625.json")
RESERVE_PATH = os.path.join(_DATA, "research_splits", "holdout_protocol_20260625.json")
DEFAULT_OUT = os.path.join(_DATA, "hybrid_split.json")

DEV_SIZE = 20
VAL_SIZE = 20
TEST_SIZE = 20


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _prompt_family(prompt):
    text = " ".join((prompt or "").lower().split())
    if any(k in text for k in ("convert", "export", "format", "website", "html", "csv", "json", "xml", "yaml", "sql", "markdown", "report", "summary")):
        return "format_conversion"
    if any(k in text for k in ("split", "separate", "partition", "classify", "categorize", "route", "extract")):
        return "global_restructure"
    if any(k in text for k in ("merge", "combine", "consolidate", "flatten", "inline")):
        return "global_restructure"
    if any(k in text for k in ("sort", "order", "reorder", "rank", "group")):
        return "bulk_homogeneous"
    if any(k in text for k in ("replace", "rename", "redact", "mask", "normalize", "annotate", "tag", "scale", "round", "update", "fix", "delete", "remove")):
        return "local_edit"
    if any(k in text for k in ("create", "generate", "synthesize", "write a")):
        return "generative"
    return "generative"


def _sample_features(sample_id):
    sample_path = os.path.join(SAMPLES_ROOT, sample_id, "sample.json")
    data = _load_json(sample_path)
    start = data["start_state"]
    states = {s["state_id"]: s for s in data.get("states") or []}
    initial = states[start]
    families = Counter()
    for item in initial.get("prompts") or []:
        families[_prompt_family(item.get("prompt") or "")] += 1
    return {
        "sample_id": sample_id,
        "sample_type": data.get("sample_type"),
        "prompt_count": sum(families.values()),
        "task_family_counts": dict(sorted(families.items())),
    }


def _coverage(features):
    domains = Counter()
    families = Counter()
    for feat in features:
        domains[feat["sample_type"]] += 1
        families.update(feat["task_family_counts"])
    return {
        "sample_count": len(features),
        "domain_count": len(domains),
        "domain_counts": dict(sorted(domains.items())),
        "task_family_counts": dict(sorted(families.items())),
    }


def _registry_index(registry):
    return {entry["sample_id"]: entry for entry in registry.get("entries") or []}


def build_split(registry_path=REGISTRY_PATH, dev20_path=DEV20_PATH, reserve_path=RESERVE_PATH):
    registry = _load_json(registry_path)
    dev_doc = _load_json(dev20_path)
    reserve_doc = _load_json(reserve_path)
    reg = _registry_index(registry)

    dev = list(dev_doc.get("dev20_samples") or [])[:DEV_SIZE]
    reserve = list(reserve_doc.get("holdout_reserve_samples") or [])
    val = reserve[:VAL_SIZE]
    test = reserve[VAL_SIZE:VAL_SIZE + TEST_SIZE]
    unused = reserve[VAL_SIZE + TEST_SIZE:]

    missing = [sid for sid in dev + val + test if sid not in reg]
    if missing:
        raise RuntimeError(f"sample(s) missing from contamination registry: {missing}")
    bad_test = [
        sid for sid in test
        if reg[sid].get("status") != "reserved_holdout_candidate"
        or reg[sid].get("unavailable_reason")
    ]
    if bad_test:
        raise RuntimeError(f"test contains non-reserved or unavailable samples: {bad_test}")

    feature_map = {sid: _sample_features(sid) for sid in dev + val + test + unused}
    return {
        "schema": "anchorpatch.hybrid_split/1",
        "created_local": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "workspace": os.path.abspath(_ROOT),
        "no_api_calls": True,
        "policy": {
            "dev_source": "data/research_splits/dev20_20260625.json",
            "val_test_source": "data/research_splits/holdout_protocol_20260625.json",
            "materialization": "reserve_order_first20_val_next20_test_remaining_unused",
            "sizes": {"dev": len(dev), "val": len(val), "test": len(test), "unused_reserve": len(unused)},
            "test_policy": "existing_holdout_reserve_materialization",
            "unsupported_domains_excluded_by_registry": True,
        },
        "critical_failure_theta": {
            "status": "pending_devmini_calibration",
            "candidate_values": [0.05, 0.10, 0.15, 0.20],
            "default_if_no_positive_drops": 0.10,
            "rule": "smallest candidate value at or above p75 of positive nonzero adjacent backward RS drops; drops to 0 or evaluator error always count",
        },
        "source_hashes": {
            "registry": {"path": os.path.abspath(registry_path), "sha256": _sha256_file(registry_path)},
            "dev20": {"path": os.path.abspath(dev20_path), "sha256": _sha256_file(dev20_path)},
            "reserve": {"path": os.path.abspath(reserve_path), "sha256": _sha256_file(reserve_path)},
            "script": {"path": os.path.abspath(__file__), "sha256": _sha256_file(__file__)},
        },
        "splits": {
            "dev": dev,
            "val": val,
            "test": test,
            "unused_reserve": unused,
        },
        "coverage": {
            "dev": _coverage([feature_map[sid] for sid in dev]),
            "val": _coverage([feature_map[sid] for sid in val]),
            "test": _coverage([feature_map[sid] for sid in test]),
            "unused_reserve": _coverage([feature_map[sid] for sid in unused]),
        },
        "features": {sid: feature_map[sid] for sid in dev + val + test + unused},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default=REGISTRY_PATH)
    ap.add_argument("--dev20", default=DEV20_PATH)
    ap.add_argument("--reserve", default=RESERVE_PATH)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    split = build_split(args.registry, args.dev20, args.reserve)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(split, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(json.dumps({
        "out": os.path.abspath(args.out),
        "sizes": split["policy"]["sizes"],
        "coverage": split["coverage"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
