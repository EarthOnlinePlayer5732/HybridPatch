"""Finalize one experiment into the Git-tracked review layer.

This command is deliberately offline. It assumes the experiment has already
finished and that its verifier, analysis, scope policy, and catalog entry have
been reviewed. It never calls a model or reruns an evaluator.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "tools" / "experiment_records_catalog.json"
BUILDER = ROOT / "tools" / "build_experiment_records.py"
VALIDATOR = ROOT / "tools" / "validate_experiment_records.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment_id", help="Catalogued experiment id to finalize.")
    parser.add_argument(
        "--skip-tree-hash",
        action="store_true",
        help=(
            "Draft mode: reuse an existing raw tree hash instead of scanning the "
            "target archive. Do not use for the final archival commit."
        ),
    )
    return parser.parse_args()


def load_catalog() -> dict[str, Any]:
    with CATALOG.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError("catalog must be a JSON object")
    return value


def find_entry(
    catalog: dict[str, Any],
    experiment_id: str,
) -> tuple[str, dict[str, Any]]:
    for record_set in catalog.get("record_sets") or []:
        for entry in record_set.get("experiments") or []:
            if entry.get("experiment_id") == experiment_id:
                return str(record_set["owner"]), entry
    raise RuntimeError(
        f"{experiment_id!r} is not declared in tools/experiment_records_catalog.json"
    )


def run(*arguments: str) -> None:
    command = [sys.executable, *arguments]
    print("[finalize]", subprocess.list2cmdline(command), flush=True)
    completed = subprocess.run(command, cwd=ROOT, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}: "
            f"{subprocess.list2cmdline(command)}"
        )


def main() -> int:
    args = parse_args()
    catalog = load_catalog()
    owner, entry = find_entry(catalog, args.experiment_id)
    archive = ROOT / str(entry["archive_path"])
    if not archive.is_dir():
        raise RuntimeError(f"experiment archive is missing: {entry['archive_path']}")

    verification = entry.get("verification") or {}
    print(
        "[finalize] catalog entry:",
        f"owner={owner}",
        f"lifecycle={entry.get('lifecycle_status')}",
        f"claim={entry.get('evidence_role')}",
        f"verification={verification.get('status')}",
        flush=True,
    )
    print(
        "[finalize] prerequisite: verifier, analysis, exclusions, and catalog facts "
        "must already be reviewed; this command does not create those facts.",
        flush=True,
    )

    target_build = [
        str(BUILDER),
        "--only",
        args.experiment_id,
    ]
    if args.skip_tree_hash:
        target_build.append("--skip-tree-hash")
    run(*target_build)

    # Refresh every record's catalog digest and both aggregate indexes while
    # preserving already-computed raw hashes.
    run(str(BUILDER), "--skip-tree-hash")
    run(str(VALIDATOR))
    run(str(VALIDATOR), "--records-only")

    record = ROOT / owner / "records" / args.experiment_id
    print(f"[finalize] human report: {record / 'report.md'}")
    print(f"[finalize] casebook: {record / 'casebook.jsonl'}")
    print(f"[finalize] machine summary: {record / 'summary.json'}")
    print("[finalize] PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"[finalize] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
