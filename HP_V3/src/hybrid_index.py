"""Deterministic source index for HybridPatch prompts."""

import hashlib
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from patch_schema import block_id_for, scaled_block_id
from splitters import split_struct2, split_medium, split_fine

PREVIEW_CHARS = 80


def _sha1_12(data):
    return hashlib.sha1(data).hexdigest()[:12]


def _preview(data):
    text = data.decode("utf-8", errors="replace")
    first = text.splitlines()[0] if text.splitlines() else text
    first = first[:PREVIEW_CHARS]
    return first.replace("\\", "\\\\").replace("\t", "\\t").replace("|", "\\|")


def _file_summary(filename, content):
    data = (content or "").encode("utf-8")
    ext = os.path.splitext(filename)[1].lower() or "(none)"
    return {
        "file": filename,
        "bytes": len(data),
        "lines": (content or "").count("\n") + (1 if content else 0),
        "extension": ext,
        "sha1_12": _sha1_12(data),
    }


def _block_rows(filename, content):
    data = (content or "").encode("utf-8")
    rows = []
    for b in split_struct2(data):
        rows.append({
            "block_id": block_id_for(filename, b.block_id),
            "scale": "coarse",
            "hash": b.hash,
            "bytes": len(b.data),
            "preview": _preview(b.data),
        })
    for scale, splitter in (("medium", split_medium), ("fine", split_fine)):
        for b in splitter(data):
            rows.append({
                "block_id": scaled_block_id(filename, scale, b.block_id),
                "scale": scale,
                "hash": b.hash,
                "bytes": len(b.data),
                "preview": _preview(b.data),
            })
    return rows


def build_hybrid_index(editable_context):
    files = []
    blocks = []
    for filename in sorted(editable_context):
        content = editable_context[filename]
        files.append(_file_summary(filename, content))
        blocks.extend(_block_rows(filename, content))
    return {
        "schema": "anchorpatch.hybrid_index/1",
        "files": files,
        "blocks": blocks,
    }


def format_file_table(index):
    rows = ["| file | bytes | lines | ext | sha1_12 |", "|---|---:|---:|---|---|"]
    for f in index.get("files") or []:
        rows.append(
            f"| {f['file']} | {f['bytes']} | {f['lines']} | {f['extension']} | {f['sha1_12']} |"
        )
    return "\n".join(rows)


def format_block_table(index, include_scales=("coarse", "medium", "fine")):
    allowed = set(include_scales)
    rows = ["| block_id | scale | hash | bytes | preview |", "|---|---|---|---:|---|"]
    for b in index.get("blocks") or []:
        if b.get("scale") not in allowed:
            continue
        rows.append(
            f"| {b['block_id']} | {b['scale']} | {b['hash']} | {b['bytes']} | {b['preview']} |"
        )
    return "\n".join(rows)
