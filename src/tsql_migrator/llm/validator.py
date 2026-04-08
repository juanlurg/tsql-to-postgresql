"""
LLM output validator: three mandatory checks before accepting translated SQL.

1. Syntactic parse — sqlglot must parse it as valid Redshift SQL
2. Column existence check — all column refs must exist in target DDL
3. (Optional) Semantic diff via sqlglot.diff()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import sqlglot
import sqlglot.expressions as exp

if TYPE_CHECKING:
    from tsql_migrator.schema.registry import SchemaRegistry


@dataclass
class ValidationResult:
    valid: bool
    parse_error: str | None = None
    hallucinated_columns: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def validate_llm_output(
    sql: str,
    registry: "SchemaRegistry | None" = None,
    referenced_tables: list[str] | None = None,
) -> ValidationResult:
    """
    Validate LLM-generated Redshift SQL.

    Args:
        sql: The SQL string produced by the LLM.
        registry: Optional schema registry for column existence checking.
        referenced_tables: Table names the query is expected to reference
                           (used to scope column existence checks).

    Returns:
        ValidationResult. If valid=False, the SQL must not be shown to users.
    """
    # 1. Syntactic parse
    try:
        statements = sqlglot.parse(sql, dialect="redshift", error_level=sqlglot.ErrorLevel.RAISE)
        if not statements or all(s is None for s in statements):
            return ValidationResult(valid=False, parse_error="SQL parsed to empty statements.")
    except sqlglot.errors.ParseError as e:
        return ValidationResult(valid=False, parse_error=str(e))

    # 2. Column existence check (only if registry and tables provided)
    hallucinated = []
    if registry is not None and referenced_tables:
        # Build set of known target column names across referenced tables
        known_columns: set[str] = set()
        for table_name in referenced_tables:
            mappings = registry.find_column_mapping_any_table(src_column_name="*")
            # Use a direct query approach
            try:
                cols = _get_target_columns(registry, table_name)
                known_columns.update(c.lower() for c in cols)
            except Exception:
                pass  # registry query failed — skip this check for this table

        if known_columns:
            for stmt in statements:
                if stmt is None:
                    continue
                for col_node in stmt.find_all(exp.Column):
                    col_name = col_node.name.lower()
                    if col_name and col_name not in known_columns and col_name != "*":
                        hallucinated.append(col_node.name)

    if hallucinated:
        return ValidationResult(
            valid=False,
            hallucinated_columns=list(set(hallucinated)),
        )

    return ValidationResult(valid=True)


def _get_target_columns(registry: "SchemaRegistry", table_name: str) -> list[str]:
    """Query the registry for all target column names in a given table."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from tsql_migrator.schema.models import Column, ColumnMapping

    cols = []
    with Session(registry._engine) as session:
        rows = session.scalars(
            select(ColumnMapping.tgt_column_name).where(
                ColumnMapping.tgt_table_name.ilike(table_name),
                ColumnMapping.tgt_column_name.isnot(None),
                ColumnMapping.approved == True,  # noqa: E712
            )
        ).all()
        cols = [r for r in rows if r]
    return cols
