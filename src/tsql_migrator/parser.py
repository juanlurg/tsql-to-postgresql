"""
Parser: wrap sqlglot T-SQL parsing and detect hard-error constructs.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from tsql_migrator.errors import HardError, ParseError

# Dynamic SQL function names that have no Redshift equivalent
_DYNAMIC_SQL_NAMES = frozenset({"EXEC", "EXECUTE", "SP_EXECUTESQL", "EXECUTE_IMMEDIATE"})


def parse_tsql(sql: str) -> list[exp.Expression]:
    """
    Parse T-SQL using sqlglot and return a list of statement AST nodes.

    Raises:
        ParseError: if sqlglot cannot parse the input — caller may try LLM fallback.
        HardError: if unsupported constructs are detected (recursive CTEs, dynamic SQL,
                   linked server refs, cursors, FOR XML).
    """
    try:
        statements = sqlglot.parse(sql, dialect="tsql", error_level=sqlglot.ErrorLevel.RAISE)
    except sqlglot.errors.ParseError as e:
        raise ParseError(str(e), sql=sql) from e

    if not statements:
        raise ParseError("No parseable statements found.", sql=sql)

    for stmt in statements:
        if stmt is not None:
            _check_hard_errors(stmt)

    return [s for s in statements if s is not None]


def _check_hard_errors(ast: exp.Expression) -> None:
    """
    Walk the AST once and raise HardError for untranslatable constructs.
    """
    for node in ast.walk():
        _check_recursive_cte(node)
        _check_dynamic_sql(node)
        _check_linked_server(node)
        _check_cursor(node)
        _check_for_xml(node)


def _check_recursive_cte(node: exp.Expression) -> None:
    """Detect self-referential (recursive) CTEs — not just chained CTEs."""
    if not isinstance(node, exp.With):
        return

    # A CTE is recursive only when its OWN body references ITS OWN alias.
    # Chained CTEs (B references A) are valid in both T-SQL and Redshift.
    for cte in node.expressions:
        if not isinstance(cte, exp.CTE):
            continue
        own_name = (cte.alias or "").upper()
        if not own_name:
            continue
        for table_node in cte.find_all(exp.Table):
            if table_node.name.upper() == own_name:
                raise HardError(
                    f"Recursive CTE '{cte.alias}' is not supported in Redshift. "
                    "Redshift does not support recursive CTEs. "
                    "Rewrite as a fixed-depth unrolled CTE or handle recursion in application code.",
                    construct="RECURSIVE CTE",
                )


def _check_dynamic_sql(node: exp.Expression) -> None:
    """Detect EXEC / sp_executesql dynamic SQL."""
    # sqlglot 30+ parses EXEC as exp.Execute
    if type(node).__name__ == "Execute":
        raise HardError(
            "Dynamic SQL (EXEC/EXECUTE) cannot be auto-migrated to Redshift. "
            "Rewrite as a Redshift stored procedure (PL/pgSQL) or application logic.",
            construct="EXEC",
        )
    if isinstance(node, exp.Command):
        cmd = node.this.upper() if isinstance(node.this, str) else ""
        if cmd in {"EXEC", "EXECUTE"}:
            raise HardError(
                "Dynamic SQL (EXEC/EXECUTE) cannot be auto-migrated to Redshift. "
                "Rewrite as a Redshift stored procedure (PL/pgSQL) or application logic.",
                construct="EXEC",
            )
    if isinstance(node, exp.Anonymous):
        name = (node.this or "").upper()
        if name in _DYNAMIC_SQL_NAMES:
            raise HardError(
                f"{node.this}() is dynamic SQL and cannot be auto-migrated to Redshift.",
                construct=node.this,
            )


def _check_linked_server(node: exp.Expression) -> None:
    """Detect 4-part linked server references like [SERVER].[DB].[schema].[table]."""
    if not isinstance(node, exp.Table):
        return
    # sqlglot represents multi-part names as nested Dot/Table expressions.
    # A 4-part name produces a catalog + db + schema + table structure.
    # Check if catalog has a catalog (i.e., there are 4 parts).
    if node.args.get("catalog") and node.args.get("db"):
        # db is set AND catalog is set → 4-part name (linked server)
        raise HardError(
            f"Linked server reference detected: '{node.sql(dialect='tsql')}'. "
            "Linked server queries are not supported in Redshift. "
            "Replace with Redshift external schemas or data sharing.",
            construct="LINKED_SERVER",
        )


def _check_cursor(node: exp.Expression) -> None:
    """Detect DECLARE CURSOR statements."""
    if isinstance(node, exp.Command):
        text = node.sql(dialect="tsql").upper()
        if "CURSOR" in text and "DECLARE" in text:
            raise HardError(
                "CURSOR declarations are not supported in Redshift SQL. "
                "Rewrite as a set-based query or Redshift stored procedure.",
                construct="CURSOR",
            )


def _check_for_xml(node: exp.Expression) -> None:
    """Detect FOR XML PATH / FOR XML AUTO etc."""
    if isinstance(node, exp.XMLTable):
        raise HardError(
            "FOR XML is not supported in Redshift. "
            "Use string aggregation (LISTAGG) or handle XML in application code.",
            construct="FOR XML",
        )
