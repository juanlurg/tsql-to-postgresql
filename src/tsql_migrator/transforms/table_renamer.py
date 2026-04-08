"""
TableRenamer: map SQL Server schema.table names to Redshift schema.table names.

Uses the schema registry's table mapping.
If no registry is configured, applies default dbo → public schema substitution.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from tsql_migrator.transforms.base import TransformContext, TransformPass

# Default schema prefix mapping when no registry is available
_DEFAULT_SCHEMA_MAP = {
    "DBO": "public",
}


class TableRenamer(TransformPass):
    """Rename table and schema references from SQL Server DDL names to Redshift names."""

    def transform(self, ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        return ast.transform(self._rename_table, ctx)

    @staticmethod
    def _rename_table(node: exp.Expression, ctx: TransformContext) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node

        table_name = node.name
        db = node.args.get("db")
        catalog = node.args.get("catalog")

        # Determine source schema
        schema_name = db.name.upper() if db and hasattr(db, "name") else "DBO"

        # Query registry for table mapping if available
        if ctx.schema_registry is not None:
            mapping = ctx.schema_registry.get_table_mapping(
                src_schema=schema_name,
                src_table=table_name,
            )
            if mapping:
                new_schema = mapping.tgt_table_schema
                new_table = mapping.tgt_table_name
                ctx.renames_applied += 1
                return _rebuild_table(node, new_schema, new_table)

        # Fallback: apply default schema map and keep table name
        new_schema = _DEFAULT_SCHEMA_MAP.get(schema_name, schema_name.lower())

        if schema_name != new_schema.upper() or (db and db.name != new_schema):
            return _rebuild_table(node, new_schema, table_name)

        return node


def _rebuild_table(
    original: exp.Table,
    new_schema: str,
    new_table: str,
) -> exp.Table:
    """Construct a new Table node with updated schema and table name."""
    return exp.Table(
        this=exp.Identifier(this=new_table, quoted=False),
        db=exp.Identifier(this=new_schema, quoted=False),
        # Strip catalog (4-part names) — parser already catches linked servers
        alias=original.args.get("alias"),
        joins=original.args.get("joins"),
        laterals=original.args.get("laterals"),
        pivots=original.args.get("pivots"),
        hints=original.args.get("hints"),
    )
