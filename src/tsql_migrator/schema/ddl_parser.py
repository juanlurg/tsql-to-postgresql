"""
DDL Parser: parse CREATE TABLE statements and load them into the SchemaRegistry.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import sqlglot
import sqlglot.expressions as exp

from tsql_migrator.errors import SchemaError

if TYPE_CHECKING:
    from tsql_migrator.schema.registry import SchemaRegistry


def load_ddl_file(
    path: str,
    source_name: str,
    dialect: Literal["tsql", "redshift"],
    registry: "SchemaRegistry",
) -> int:
    """
    Parse a DDL file and load its CREATE TABLE statements into the registry.

    Args:
        path: Path to the DDL file.
        source_name: Logical name for this source (e.g. 'sqlserver_prod').
        dialect: SQL dialect of the file ('tsql' or 'redshift').
        registry: SchemaRegistry to load into.

    Returns:
        Number of tables loaded.

    Raises:
        SchemaError: if the file cannot be read or parsed.
    """
    try:
        ddl_text = Path(path).read_text(encoding="utf-8-sig")
    except OSError as e:
        raise SchemaError(f"Cannot read DDL file '{path}': {e}") from e

    # Strip T-SQL batch separators and USE statements that confuse sqlglot
    ddl_text = re.sub(r"^\s*GO\s*$", "", ddl_text, flags=re.IGNORECASE | re.MULTILINE)
    ddl_text = re.sub(r"^\s*USE\s+\S+\s*;?\s*$", "", ddl_text, flags=re.IGNORECASE | re.MULTILINE)

    try:
        statements = sqlglot.parse(ddl_text, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    except Exception as e:
        raise SchemaError(f"Failed to parse DDL file '{path}': {e}") from e

    source_id = registry.upsert_source(name=source_name, dialect=dialect, ddl_path=path)
    tables_loaded = 0

    for stmt in statements:
        if stmt is None:
            continue
        if not isinstance(stmt, exp.Create):
            continue
        kind = (stmt.args.get("kind") or "").upper()
        if kind != "TABLE":
            continue

        # stmt.this is exp.Schema (wraps the table + column list)
        # stmt.this.this is the exp.Table (name and db)
        schema_expr = stmt.this
        table_expr = schema_expr.this if isinstance(schema_expr, exp.Schema) else schema_expr
        if not isinstance(table_expr, exp.Table):
            continue

        table_name = table_expr.name
        db_node = table_expr.args.get("db")
        schema_name = db_node.name if db_node else ("dbo" if dialect == "tsql" else "public")

        columns = _extract_columns(stmt)
        if not columns:
            continue

        registry.add_table(
            source_id=source_id,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
        )
        tables_loaded += 1

    return tables_loaded


def _extract_columns(create_stmt: exp.Create) -> list[dict]:
    """
    Extract column definitions from a CREATE TABLE statement.
    Returns a list of dicts: {name, data_type, is_nullable, ordinal}.
    """
    schema_node = create_stmt.find(exp.Schema)
    if schema_node is None:
        return []

    columns = []
    ordinal = 1

    for expr in schema_node.expressions:
        if not isinstance(expr, exp.ColumnDef):
            continue

        col_name = expr.name
        if not col_name:
            continue

        # Extract data type
        dtype_node = expr.find(exp.DataType)
        data_type = dtype_node.sql() if dtype_node else "UNKNOWN"

        # Check nullability — default is nullable; NOT NULL constraint makes it non-nullable
        is_nullable = True
        for constraint in expr.constraints:
            if isinstance(constraint, exp.ColumnConstraint):
                kind = constraint.args.get("kind")
                if isinstance(kind, exp.NotNullColumnConstraint):
                    is_nullable = False
                    break
                if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                    is_nullable = False

        columns.append({
            "name": col_name,
            "data_type": data_type,
            "is_nullable": is_nullable,
            "ordinal": ordinal,
        })
        ordinal += 1

    return columns
