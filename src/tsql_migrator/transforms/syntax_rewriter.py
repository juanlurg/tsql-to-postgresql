"""
SyntaxRewriter: structural query rewrites.

Handles:
- SELECT TOP n → LIMIT n (positional move)
- SELECT TOP n WITH TIES → WARN + llm_candidates
- CROSS APPLY → CROSS JOIN LATERAL
- OUTER APPLY → LEFT JOIN LATERAL ... ON TRUE
- SELECT INTO #temp → CREATE TEMP TABLE AS SELECT
- PIVOT / UNPIVOT → llm_candidates + placeholder
- + operator on string literals → WARN
"""

from __future__ import annotations

import sqlglot.expressions as exp

from tsql_migrator.transforms.base import TransformContext, TransformPass


class SyntaxRewriter(TransformPass):
    """Apply structural T-SQL → Redshift query rewrites."""

    def transform(self, ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        # Run multiple targeted rewrites
        ast = self._rewrite_top(ast, ctx)
        ast = self._rewrite_apply(ast, ctx)
        ast = self._rewrite_select_into_temp(ast, ctx)
        ast = self._rewrite_pivot(ast, ctx)
        ast = self._check_string_concat(ast, ctx)
        return ast

    @staticmethod
    def _rewrite_top(ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        """
        Handle TOP n / TOP n WITH TIES / TOP n PERCENT.

        sqlglot's tsql dialect already converts most TOP forms to Limit nodes.
        We just need to detect WITH TIES and PERCENT via LimitOptions.
        """
        for limit in ast.find_all(exp.Limit):
            opts = limit.args.get("limit_options")
            if opts is None:
                continue

            with_ties = opts.args.get("with_ties", False)
            percent = opts.args.get("percent", False)

            if with_ties:
                ctx.warn(
                    "SELECT TOP n WITH TIES has no direct Redshift equivalent. "
                    "Use RANK() / DENSE_RANK() window functions to replicate tie-breaking behaviour."
                )
                # Remove the LimitOptions (strip WITH TIES) — keep the LIMIT value
                limit.set("limit_options", None)

            elif percent:
                ctx.warn(
                    "SELECT TOP n PERCENT has no direct Redshift equivalent. "
                    "Rewrite using a subquery: LIMIT (SELECT CEIL(COUNT(*) * 0.n) FROM ...)."
                )
                limit.set("limit_options", None)

        # Also handle old-style exp.Select.top (fallback for older sqlglot versions)
        for select in ast.find_all(exp.Select):
            top = select.args.get("top")
            if top is None:
                continue
            with_ties = top.args.get("ties", False)
            percent = top.args.get("percent", False)
            if with_ties:
                ctx.warn(
                    "SELECT TOP n WITH TIES has no direct Redshift equivalent. "
                    "Use RANK() / DENSE_RANK() window functions."
                )
            elif percent:
                ctx.warn("SELECT TOP n PERCENT has no direct Redshift equivalent.")
            count_expr = top.this
            select.set("top", None)
            select.set("limit", exp.Limit(expression=count_expr))

        return ast

    @staticmethod
    def _rewrite_apply(ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        """Convert CROSS APPLY / OUTER APPLY to LATERAL joins."""
        for join in ast.find_all(exp.Join):
            join_kind = join.args.get("kind", "")
            join_side = join.args.get("side", "")

            kind_str = join_kind.upper() if isinstance(join_kind, str) else ""
            side_str = join_side.upper() if isinstance(join_side, str) else ""

            if kind_str == "CROSS" and "APPLY" in join.sql(dialect="tsql").upper():
                # CROSS APPLY → CROSS JOIN LATERAL
                ctx.info("CROSS APPLY converted to CROSS JOIN LATERAL.")
                join.set("kind", "")
                join.set("side", "CROSS")
                # Mark the joined expression as LATERAL
                joined = join.this
                if joined and not isinstance(joined, exp.Lateral):
                    join.set("this", exp.Lateral(this=joined))

            elif "APPLY" in join.sql(dialect="tsql").upper():
                # OUTER APPLY → LEFT JOIN LATERAL ... ON TRUE
                ctx.info("OUTER APPLY converted to LEFT JOIN LATERAL ... ON TRUE.")
                join.set("kind", "")
                join.set("side", "LEFT")
                joined = join.this
                if joined and not isinstance(joined, exp.Lateral):
                    join.set("this", exp.Lateral(this=joined))
                if not join.args.get("on"):
                    join.set("on", exp.true())

        return ast

    @staticmethod
    def _rewrite_select_into_temp(
        ast: exp.Expression, ctx: TransformContext
    ) -> exp.Expression:
        """
        Convert SELECT ... INTO #temp_table FROM ...
        to CREATE TEMP TABLE temp_table AS SELECT ... FROM ...
        """
        # sqlglot may parse SELECT INTO as exp.Create or keep it as exp.Select with into= set
        for select in ast.find_all(exp.Select):
            into = select.args.get("into")
            if into is None:
                continue

            table_ref = into.this if hasattr(into, "this") else into
            table_name = table_ref.name if hasattr(table_ref, "name") else str(table_ref)

            if table_name.startswith("#"):
                # Strip the # prefix for Redshift TEMP table
                clean_name = table_name.lstrip("#")
                ctx.info(
                    f"SELECT INTO #{clean_name} converted to "
                    f"CREATE TEMP TABLE {clean_name} AS SELECT ..."
                )
                select.set("into", None)
                # Wrap in a CREATE TABLE AS
                return exp.Create(
                    kind="TABLE",
                    this=exp.Table(
                        this=exp.Identifier(this=clean_name),
                        properties=exp.Properties(
                            expressions=[exp.TemporaryProperty()]
                        ),
                    ),
                    expression=select,
                )

        return ast

    @staticmethod
    def _rewrite_pivot(ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        """
        Detect PIVOT / UNPIVOT and flag for LLM rewrite.
        Replace with a placeholder comment in the output.
        """
        for pivot in ast.find_all(exp.Pivot):
            ctx.warn(
                "PIVOT/UNPIVOT has no native Redshift syntax. "
                "Rewrite as conditional aggregation: "
                "SELECT MAX(CASE WHEN col = 'val' THEN measure END) AS val_col ... GROUP BY ..."
            )
            ctx.llm_candidates.append(pivot)
            # Replace with a visible placeholder so the output is clearly incomplete
            placeholder = exp.Anonymous(
                this="__PIVOT_TODO__",
                expressions=[exp.Literal.string("Rewrite as conditional aggregation")],
            )
            pivot.replace(placeholder)

        return ast

    @staticmethod
    def _check_string_concat(ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        """
        Warn when + is used and at least one operand looks like a string (Literal.string).
        T-SQL coerces types; Redshift will throw a type error.
        """
        for add in ast.find_all(exp.Add):
            left, right = add.this, add.expression
            if isinstance(left, exp.Literal) and not left.is_number:
                ctx.warn(
                    "String concatenation with + detected. "
                    "Redshift requires || for string concatenation. "
                    "Replace: col1 + col2  →  col1 || col2"
                )
            elif isinstance(right, exp.Literal) and not right.is_number:
                ctx.warn(
                    "String concatenation with + detected. "
                    "Redshift requires || for string concatenation. "
                    "Replace: col1 + col2  →  col1 || col2"
                )
        return ast
