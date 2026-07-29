"""Byte identity checks for the HP_V9 scientific core.

Run from HP_V9 root:
  PYTHONUTF8=1 python -B ./src/test_v9_core_identity.py
"""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


FROZEN_HP_V8_COMMIT = "7b8fe009a892039db4718ef0cfbb03d35d97d10c"

CORE_FILES = (
    Path("requirements.txt"),
    Path("src/analyze.py"),
    Path("src/experiment_runner.py"),
    Path("src/hybrid_executor.py"),
    Path("src/hybrid_gate.py"),
    Path("src/hybrid_index.py"),
    Path("src/hybrid_prompt.py"),
    Path("src/hybrid_schema.py"),
    Path("src/model_openai.py"),
    Path("src/patch_schema.py"),
    Path("src/splitters.py"),
    Path("src/utils_context.py"),
    Path("src/utils_eval.py"),
    Path("src/utils_relay_plan.py"),
    Path("src/utils_results.py"),
    Path("src/verify_anchorpatch.py"),
)


HERE = Path(__file__).resolve()
HP_V9_ROOT = HERE.parents[1]
REPO_ROOT = HP_V9_ROOT.parent


def _git(args: list[str]) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise AssertionError(f"git {' '.join(args)} failed: {message}")
    return result.stdout


def _frozen_tree_files(prefix: str) -> tuple[Path, ...]:
    output = _git([
        "ls-tree",
        "-r",
        "--name-only",
        FROZEN_HP_V8_COMMIT,
        f"HP_V8/{prefix}",
    ]).decode("utf-8").splitlines()
    return tuple(
        sorted(
            Path(line).relative_to("HP_V8")
            for line in output
            if line
        )
    )


def _identity_paths() -> tuple[Path, ...]:
    prompt_files = _frozen_tree_files("prompts")
    domain_files = tuple(
        path
        for path in _frozen_tree_files("src/domains")
        if path.suffix == ".py"
    )
    return tuple(dict.fromkeys((*prompt_files, *domain_files, *CORE_FILES)))


def _frozen_blob(rel_path: Path) -> bytes:
    return _git([
        "show",
        f"{FROZEN_HP_V8_COMMIT}:HP_V8/{rel_path.as_posix()}",
    ])


class V9CoreIdentityTests(unittest.TestCase):
    def test_v9_scientific_core_matches_frozen_v8_bytes(self) -> None:
        frozen_prompts = set(_frozen_tree_files("prompts"))
        current_prompts = {
            path.relative_to(HP_V9_ROOT)
            for path in (HP_V9_ROOT / "prompts").rglob("*")
            if path.is_file()
        }
        self.assertEqual(current_prompts, frozen_prompts)

        frozen_domains = {
            path for path in _frozen_tree_files("src/domains")
            if path.suffix == ".py"
        }
        current_domains = {
            path.relative_to(HP_V9_ROOT)
            for path in (HP_V9_ROOT / "src" / "domains").rglob("*.py")
            if path.is_file()
        }
        self.assertEqual(current_domains, frozen_domains)

        missing: list[str] = []
        mismatched: list[str] = []
        for rel_path in _identity_paths():
            current_path = HP_V9_ROOT / rel_path
            if not current_path.is_file():
                missing.append(rel_path.as_posix())
                continue
            if current_path.read_bytes() != _frozen_blob(rel_path):
                mismatched.append(rel_path.as_posix())

        details = []
        if missing:
            details.append("missing=" + ", ".join(missing))
        if mismatched:
            details.append("mismatched=" + ", ".join(mismatched))
        self.assertFalse(details, "; ".join(details))


if __name__ == "__main__":
    unittest.main()
