"""
Generator: convert a transformed AST back to Redshift SQL string.
"""

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp


def generate_redshift(ast: exp.Expression) -> str:
    """
    Generate Redshift SQL from an AST node.

    Post-processes the output to:
    - Ensure semicolon termination
    - Convert any remaining [bracket] identifiers to "double_quoted"
    - Normalize to 2-space indentation
    """
    sql = ast.sql(dialect="redshift", pretty=True)
    sql = _fix_bracket_quotes(sql)
    sql = _ensure_semicolon(sql)
    return sql


def generate_redshift_statements(statements: list[exp.Expression]) -> str:
    """Generate multiple statements separated by semicolons and newlines."""
    parts = [generate_redshift(s) for s in statements]
    return "\n\n".join(parts)


def _fix_bracket_quotes(sql: str) -> str:
    """
    Convert any residual T-SQL [bracket quoted identifiers] to "double quoted".
    sqlglot handles most of these but edge cases can slip through.
    """
    return re.sub(r"\[([^\]]+)\]", r'"\1"', sql)


def _ensure_semicolon(sql: str) -> str:
    """Ensure the SQL statement ends with a semicolon."""
    stripped = sql.rstrip()
    if stripped and not stripped.endswith(";"):
        return stripped + ";"
    return sql
