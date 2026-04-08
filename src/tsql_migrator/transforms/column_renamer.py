"""
ColumnRenamer: rename column references using the schema registry mapping.

Column renames are ALWAYS deterministic — the LLM is never allowed to guess.
When a column cannot be mapped, an /* UNMAPPED: ColName */ placeholder is injected.

Important: this pass runs AFTER TableRenamer so table names are already resolved
to their Redshift equivalents before we do column lookups.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from tsql_migrator.transforms.base import TransformContext, TransformPass

_UNMAPPED_TEMPLATE = "/* UNMAPPED: {col} */"


class ColumnRenamer(TransformPass):
    """Rename column references based on schema registry mappings."""

    def transform(self, ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        if ctx.schema_registry is None:
            return ast  # No registry — skip column renaming
        return ast.transform(self._rename_column, ctx)

    @staticmethod
    def _rename_column(node: exp.Expression, ctx: TransformContext) -> exp.Expression:
        if not isinstance(node, exp.Column):
            return node

        col_name = node.name
        table_ref = node.table  # may be None if unqualified

        # Resolve which table this column belongs to
        # For now: if table is specified, use it; otherwise we cannot scope it safely
        if not table_ref:
            # Unqualified column — attempt lookup across all mapped tables
            # If ambiguous, leave as-is with a warning
            mapping = _lookup_unqualified(col_name, ctx)
        else:
            mapping = _lookup_qualified(table_ref, col_name, ctx)

        if mapping is None:
            return node  # no registry entry — leave unchanged

        if mapping is False:
            # Column is in a mapped table but has no target mapping
            ctx.warn(
                f"Column '{col_name}' has no mapping in the target schema — "
                f"marked as UNMAPPED. Update the schema registry to resolve."
            )
            # Return a special comment expression as a placeholder
            # We use an Anonymous expression so the SQL is syntactically broken
            # (intentionally) until a human resolves it.
            return exp.Anonymous(
                this="__UNMAPPED__",
                expressions=[exp.Literal.string(col_name)],
            )

        new_col_name, _ = mapping
        if new_col_name == col_name:
            return node

        ctx.renames_applied += 1
        new_node = node.copy()
        new_node.set("this", exp.Identifier(this=new_col_name, quoted=False))
        return new_node


def _lookup_qualified(
    table_ref: str,
    col_name: str,
    ctx: TransformContext,
) -> tuple[str, float] | None | bool:
    """
    Look up a qualified column reference (table.column).
    Returns (new_col_name, confidence), False if unmapped, None if table not in registry.
    """
    registry = ctx.schema_registry
    if registry is None:
        return None

    result = registry.get_column_mapping(
        tgt_table_name=table_ref,
        src_column_name=col_name,
    )
    return result  # None | False | (str, float)


def _lookup_unqualified(
    col_name: str,
    ctx: TransformContext,
) -> tuple[str, float] | None | bool:
    """
    Attempt to look up an unqualified column.
    If found in exactly one table, return the mapping.
    If found in multiple tables (ambiguous), return None (leave unchanged).
    """
    registry = ctx.schema_registry
    if registry is None:
        return None

    results = registry.find_column_mapping_any_table(src_column_name=col_name)
    if len(results) == 1:
        return results[0]
    # Ambiguous or not found
    return None
