"""
Base classes for all transform passes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

import sqlglot.expressions as exp

if TYPE_CHECKING:
    from tsql_migrator.schema.registry import SchemaRegistry


class Severity(Enum):
    INFO = auto()
    WARN = auto()
    ERROR = auto()


@dataclass
class Annotation:
    message: str
    severity: Severity
    line: int | None = None        # 1-based line number in the *output* SQL, if known
    original_sql: str | None = None  # the fragment that triggered this annotation


@dataclass
class TransformContext:
    """
    Mutable state threaded through all transform passes for a single translation.
    """
    annotations: list[Annotation] = field(default_factory=list)
    udf_blocks: list[str] = field(default_factory=list)
    llm_candidates: list[exp.Expression] = field(default_factory=list)
    schema_registry: "SchemaRegistry | None" = None
    renames_applied: int = 0
    # Populated by TableRenamer: src_table_name.lower() → tgt_table_name
    # Used by ColumnRenamer to resolve non-alias column qualifiers after table rename
    table_renames: dict = field(default_factory=dict)

    def info(self, message: str, line: int | None = None) -> None:
        self.annotations.append(Annotation(message=message, severity=Severity.INFO, line=line))

    def warn(self, message: str, line: int | None = None) -> None:
        self.annotations.append(Annotation(message=message, severity=Severity.WARN, line=line))

    def error(self, message: str, line: int | None = None) -> None:
        self.annotations.append(Annotation(message=message, severity=Severity.ERROR, line=line))


class TransformPass(ABC):
    """
    Abstract base for all AST transformation passes.

    Each pass receives an AST node and a mutable TransformContext,
    and returns a (possibly new) AST node. Passes must not raise unless
    a HardError is detected.
    """

    @abstractmethod
    def transform(
        self,
        ast: exp.Expression,
        ctx: TransformContext,
    ) -> exp.Expression:
        """
        Transform the AST. May mutate ctx (add annotations, udf_blocks, etc.).
        Returns the (possibly new) root AST node.
        """
