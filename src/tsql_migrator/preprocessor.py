"""
Preprocessor: normalize raw T-SQL text before AST parsing.

Handles:
- Strip single-line (--) and block (/* */) comments
- Split on GO batch separators
- Normalize whitespace
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from tsql_migrator.errors import HardError

# GO on its own line, optionally preceded/followed by whitespace, with optional count
_GO_PATTERN = re.compile(r"^\s*GO\b(\s+\d+)?\s*$", re.IGNORECASE | re.MULTILINE)

# Block comment: /* ... */ (handles nested comments via iterative stripping)
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

# Single-line comment: -- to end of line (but not inside string literals)
_LINE_COMMENT = re.compile(r"--[^\n]*")


@dataclass
class PreprocessResult:
    batches: list[str] = field(default_factory=list)
    original: str = ""
    stripped: str = ""  # comments removed, whitespace normalized


def preprocess(sql: str) -> PreprocessResult:
    """
    Normalize raw T-SQL text and split into GO-separated batches.

    Raises:
        HardError: if 'GO n' with a count argument is encountered (not supported).
        ValueError: if input is empty after stripping.
    """
    original = sql

    # Check for GO with count (e.g. GO 3) — unsupported
    go_with_count = re.search(r"^\s*GO\b\s+(\d+)\s*$", sql, re.IGNORECASE | re.MULTILINE)
    if go_with_count:
        raise HardError(
            "GO with repeat count (e.g. 'GO 3') is not supported — "
            "repeat the batch manually before migrating.",
            construct="GO n",
        )

    # Strip block comments (iterative to handle nesting)
    stripped = _strip_block_comments(sql)

    # Strip single-line comments
    stripped = _LINE_COMMENT.sub("", stripped)

    # Normalize whitespace: collapse multiple blank lines to one
    stripped = re.sub(r"\n{3,}", "\n\n", stripped).strip()

    if not stripped:
        raise ValueError("Input SQL is empty after stripping comments.")

    # Split on GO
    batches = _split_on_go(stripped)

    return PreprocessResult(batches=batches, original=original, stripped=stripped)


def _strip_block_comments(sql: str) -> str:
    """
    Iteratively strip /* ... */ block comments.
    SQL Server supports nested block comments; we strip from innermost outward.
    """
    prev = None
    result = sql
    while result != prev:
        prev = result
        result = _BLOCK_COMMENT.sub("", result)
    return result


def _split_on_go(sql: str) -> list[str]:
    """
    Split SQL text on GO batch separators.
    Returns a list of non-empty batch strings.
    """
    parts = _GO_PATTERN.split(sql)
    batches = []
    for part in parts:
        # _GO_PATTERN has a capture group (the optional count), which produces
        # None entries in the split result — skip those
        if part is None:
            continue
        # Also skip the captured group content itself (digits)
        if part.strip().isdigit():
            continue
        batch = part.strip()
        if batch:
            batches.append(batch)

    return batches if batches else [sql.strip()]
