"""
HintStripper: remove all T-SQL table and query hints.

Handles:
- WITH (NOLOCK), WITH (READPAST), WITH (UPDLOCK), etc. on table references
- OPTION (...) query hints: RECOMPILE, MAXDOP, HASH JOIN, etc.
- Join hints: LOOP JOIN, HASH JOIN, MERGE JOIN
"""

from __future__ import annotations

import sqlglot.expressions as exp

from tsql_migrator.transforms.base import TransformContext, TransformPass

# Table hints to strip (uppercase)
_TABLE_HINTS = frozenset({
    "NOLOCK", "READPAST", "UPDLOCK", "XLOCK", "ROWLOCK", "PAGLOCK",
    "TABLOCK", "TABLOCKX", "HOLDLOCK", "NOEXPAND", "READCOMMITTED",
    "READUNCOMMITTED", "REPEATABLEREAD", "SERIALIZABLE", "SNAPSHOT",
    "INDEX", "FORCESEEK", "FORCESCAN", "KEEPIDENTITY", "KEEPDEFAULTS",
    "READCOMMITTEDLOCK",
})


class HintStripper(TransformPass):
    """Remove T-SQL hints that have no Redshift equivalent."""

    def transform(self, ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        return ast.transform(self._strip_node, ctx)

    @staticmethod
    def _strip_node(node: exp.Expression, ctx: TransformContext) -> exp.Expression:
        # Strip WITH (...) table hints — represented as exp.WithTableHint
        if isinstance(node, exp.WithTableHint):
            hints = [
                h.name.upper() if hasattr(h, "name") else str(h).upper()
                for h in node.expressions
            ]
            stripped = [h for h in hints if h in _TABLE_HINTS or h.startswith("INDEX")]
            if stripped:
                ctx.info(f"Removed table hint(s): {', '.join(stripped)}")
            # Return None to remove the node; sqlglot.transform handles None returns
            # by removing the node from its parent
            return None  # type: ignore[return-value]

        # Strip OPTION (...) query hints
        if isinstance(node, exp.QueryOption):
            hint_name = node.name.upper() if hasattr(node, "name") else str(node).upper()
            ctx.info(f"Removed query hint: OPTION({hint_name})")
            return None  # type: ignore[return-value]

        # Strip WITH hints on joins (exp.JoinHint in some sqlglot versions)
        if type(node).__name__ == "JoinHint":
            ctx.info(f"Removed join hint: {node.sql(dialect='tsql')}")
            return None  # type: ignore[return-value]

        return node
