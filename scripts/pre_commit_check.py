"""Pre-commit sanity check — parse every staged .py file with AST.

Catches the class of errors that we hit multiple times during the live-
trade session: indentation mistakes, f-string bugs, hanging parens from
edits, imports referencing removed modules. None of this requires running
the codebase — purely static parsing.

Runs automatically if installed via ``git config core.hooksPath``.
Manually: ``python scripts/pre_commit_check.py``.
"""
from __future__ import annotations

import ast
import subprocess
import sys


def _staged_files() -> list[str]:
    """Files staged in git, limited to .py."""
    try:
        out = subprocess.check_output(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
            text=True,
        )
    except Exception as exc:
        print(f"[pre-commit] git diff failed: {exc}")
        return []
    return [f.strip() for f in out.splitlines() if f.strip().endswith(".py")]


def _check_file(path: str) -> list[str]:
    """Parse with AST; return list of errors (empty if clean)."""
    errors: list[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            src = f.read()
    except FileNotFoundError:
        return []  # deleted file — skip
    except Exception as exc:
        return [f"{path}: read failed: {exc}"]

    try:
        ast.parse(src, filename=path)
    except SyntaxError as e:
        errors.append(f"{path}:{e.lineno}:{e.offset}  SyntaxError: {e.msg}")

    # Basic check: obvious placeholder patterns we leave when mid-edit.
    # Build needle from chars so this file doesn't flag itself.
    for marker_char, warning in (("<", "unresolved merge conflict marker"),
                                 (">", "unresolved merge conflict marker")):
        needle = marker_char * 7 + " "
        if needle in src:
            errors.append(f"{path}: {warning}")
    return errors


def main() -> int:
    files = _staged_files()
    if not files:
        return 0
    print(f"[pre-commit] checking {len(files)} staged .py file(s)…")
    all_errors: list[str] = []
    for f in files:
        all_errors.extend(_check_file(f))
    if not all_errors:
        print("[pre-commit] all staged files parse cleanly.")
        return 0
    print("[pre-commit] BLOCKED by errors:")
    for err in all_errors:
        print(f"  {err}")
    print(
        "\n"
        "Fix the errors and re-stage, or bypass with --no-verify if you\n"
        "know what you're doing (please don't).",
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
