"""
FunctionRewriter: replace T-SQL functions with Redshift equivalents.

Two-tier approach:
1. Generic rules loaded from rules/function_rules.yaml (simple renames + arg reorder)
2. Special-case _rewrite_* methods for functions requiring structural changes
"""

from __future__ import annotations

import sqlglot.expressions as exp

from tsql_migrator.transforms.base import TransformContext, TransformPass

# CONVERT style codes → Redshift TO_CHAR format strings
_CONVERT_STYLE_MAP: dict[int, str] = {
    1: "MM/DD/YY",
    2: "YY.MM.DD",
    3: "DD/MM/YY",
    4: "DD.MM.YY",
    5: "DD-MM-YY",
    6: "DD Mon YY",
    7: "Mon DD, YY",
    10: "MM-DD-YY",
    11: "YY/MM/DD",
    12: "YYMMDD",
    100: "Mon DD YYYY HH:MIAM",
    101: "MM/DD/YYYY",
    102: "YYYY.MM.DD",
    103: "DD/MM/YYYY",
    104: "DD.MM.YYYY",
    105: "DD-MM-YYYY",
    106: "DD Mon YYYY",
    107: "Mon DD, YYYY",
    108: "HH:MI:SS",
    110: "MM-DD-YYYY",
    111: "YYYY/MM/DD",
    112: "YYYYMMDD",
    120: "YYYY-MM-DD HH24:MI:SS",
    121: "YYYY-MM-DD HH24:MI:SS.MS",
    126: "YYYY-MM-DD\"T\"HH24:MI:SS",
    127: "YYYY-MM-DD\"T\"HH24:MI:SS.MS",
    130: "DD Mon YYYY HH:MIAM",
    131: "DD/MM/YYYY HH:MIAM",
}

# TRY_CAST UDF template (generated once per type, prepended to output)
_TRY_CAST_UDF = """\
CREATE OR REPLACE FUNCTION safe_cast_{type_lower}(v VARCHAR)
RETURNS {redshift_type}
STABLE AS $$
BEGIN
  RETURN v::{redshift_type};
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;"""


class FunctionRewriter(TransformPass):
    """
    Rewrite T-SQL function calls to their Redshift equivalents.
    """

    def __init__(self) -> None:
        self.rule_registry = None  # injected by pipeline

    def transform(self, ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        return ast.transform(self._rewrite_node, ctx)

    def _rewrite_node(self, node: exp.Expression, ctx: TransformContext) -> exp.Expression:
        # Anonymous functions (CHARINDEX, DATEPART, etc. not parsed by sqlglot natively)
        if isinstance(node, exp.Anonymous):
            return self._dispatch_anonymous(node, ctx)

        # sqlglot pre-parses these T-SQL functions into specific node types
        if isinstance(node, exp.StrPosition):
            # sqlglot parses CHARINDEX(needle, haystack) → StrPosition(this=haystack, substr=needle)
            # Redshift prefers STRPOS(haystack, needle)
            return self._rewrite_str_position(node, ctx)

        if isinstance(node, exp.CurrentTimestamp):
            # sqlglot parses GETDATE() → CurrentTimestamp()
            return self._rewrite_current_timestamp(node, ctx)

        if isinstance(node, exp.Extract):
            return self._rewrite_extract(node, ctx)

        # Named function classes that sqlglot parses directly
        if isinstance(node, exp.Coalesce):
            return node  # already correct
        if isinstance(node, exp.TryCast):
            return self._rewrite_try_cast(node, ctx)
        if isinstance(node, exp.Cast):
            return self._rewrite_cast(node, ctx)

        # Apply YAML-driven rules for function nodes
        if isinstance(node, exp.Func):
            return self._apply_yaml_rule(node, ctx)

        return node

    def _dispatch_anonymous(self, node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        name = (node.this or "").upper()
        dispatch = {
            "CHARINDEX": self._rewrite_charindex,
            "DATEPART": self._rewrite_datepart,
            "GETDATE": self._rewrite_getdate,
            "GETUTCDATE": self._rewrite_getutcdate,
            "SYSDATETIME": self._rewrite_getdate,
            "SYSUTCDATETIME": self._rewrite_getdate,
            "STRING_AGG": self._rewrite_string_agg,
            "ISNULL": self._rewrite_isnull,
            "IIF": self._rewrite_iif,
            "STUFF": self._rewrite_stuff,
            "REPLICATE": self._rewrite_replicate,
            "LEN": self._rewrite_len,
            "EOMONTH": self._rewrite_eomonth,
            "COUNT_BIG": self._rewrite_count_big,
            "CONVERT": self._rewrite_convert,
            "FORMAT": self._rewrite_format,
            "NVL": self._rewrite_nvl,
            "CHARINDEX": self._rewrite_charindex,
            "PATINDEX": self._rewrite_patindex,
            "SOUNDEX": self._rewrite_soundex,
            "TRY_PARSE": self._rewrite_try_parse,
            "CHECKSUM": self._rewrite_checksum,
        }
        handler = dispatch.get(name)
        if handler:
            return handler(node, ctx)

        # Apply YAML rules for other anonymous functions
        if self.rule_registry:
            return self.rule_registry.apply_function_rule(node, ctx)

        return node

    # ── Special-case rewrites ──────────────────────────────────────────────

    @staticmethod
    def _rewrite_charindex(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) < 2:
            return node
        needle, haystack = args[0], args[1]
        if len(args) >= 3:
            # 3-arg form: no Redshift equivalent
            ctx.warn(
                "CHARINDEX(needle, haystack, start) has no direct Redshift equivalent. "
                "The start-offset argument has been dropped — verify correctness."
            )
        # STRPOS(haystack, needle) — args REVERSED
        return exp.Anonymous(this="STRPOS", expressions=[haystack, needle])

    @staticmethod
    def _rewrite_datepart(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) < 2:
            return node
        part_node, date_expr = args[0], args[1]
        part_str = (
            part_node.name.upper()
            if hasattr(part_node, "name")
            else str(part_node).strip("'\"").upper()
        )

        # weekday / dw: T-SQL Sunday=1 … Saturday=7; Redshift dow Sunday=0 … Saturday=6
        if part_str in ("WEEKDAY", "DW"):
            ctx.warn(
                "DATEPART(weekday/dw, ...) offset differs: T-SQL Sunday=1, Redshift Sunday=0. "
                "Added +1 to match T-SQL behaviour — verify if downstream code uses 0-based values."
            )
            extract = exp.Extract(this=exp.Var(this="dow"), expression=date_expr)
            return exp.Add(this=extract, expression=exp.Literal.number(1))

        # Map common DATEPART parts to EXTRACT equivalents
        part_map = {
            "YEAR": "year", "YY": "year", "YYYY": "year",
            "QUARTER": "quarter", "QQ": "quarter", "Q": "quarter",
            "MONTH": "month", "MM": "month", "M": "month",
            "DAYOFYEAR": "doy", "DY": "doy", "Y": "doy",
            "DAY": "day", "DD": "day", "D": "day",
            "WEEK": "week", "WK": "week", "WW": "week",
            "HOUR": "hour", "HH": "hour",
            "MINUTE": "minute", "MI": "minute", "N": "minute",
            "SECOND": "second", "SS": "second", "S": "second",
            "MILLISECOND": "milliseconds", "MS": "milliseconds",
            "MICROSECOND": "microseconds", "MCS": "microseconds",
            "NANOSECOND": "microseconds", "NS": "microseconds",
        }
        redshift_part = part_map.get(part_str, part_str.lower())
        return exp.Extract(this=exp.Var(this=redshift_part), expression=date_expr)

    @staticmethod
    def _rewrite_getdate(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        ctx.warn(
            "GETDATE() returns local server time in SQL Server but UTC in Redshift. "
            "If your SQL Server ran in a non-UTC timezone, date values may shift."
        )
        return exp.Var(this="SYSDATE")

    @staticmethod
    def _rewrite_getutcdate(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        # Redshift GETDATE() is already UTC — direct equivalent
        return exp.Anonymous(this="GETDATE", expressions=[])

    @staticmethod
    def _rewrite_string_agg(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) < 2:
            return node
        col_expr, sep_expr = args[0], args[1]
        ctx.warn(
            "STRING_AGG mapped to LISTAGG with ORDER BY 1 placeholder. "
            "Verify the ORDER BY matches your intended ordering."
        )
        # LISTAGG(col, sep) WITHIN GROUP (ORDER BY 1)
        return exp.Anonymous(
            this="LISTAGG",
            expressions=[
                col_expr,
                sep_expr,
                exp.Literal.string("ORDER BY 1"),  # placeholder
            ],
        )

    @staticmethod
    def _rewrite_isnull(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) != 2:
            return node
        return exp.Coalesce(expressions=list(args))

    @staticmethod
    def _rewrite_iif(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) != 3:
            return node
        cond, true_val, false_val = args
        return exp.Case(
            ifs=[exp.If(this=cond, true=true_val)],
            default=false_val,
        )

    @staticmethod
    def _rewrite_stuff(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) != 4:
            return node
        string, start, length, replacement = args
        # OVERLAY(string PLACING replacement FROM start FOR length)
        return exp.Anonymous(
            this="OVERLAY",
            expressions=[string, replacement, start, length],
        )

    @staticmethod
    def _rewrite_replicate(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        return exp.Anonymous(this="REPEAT", expressions=node.expressions)

    @staticmethod
    def _rewrite_len(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        return exp.Anonymous(this="LEN", expressions=node.expressions)

    @staticmethod
    def _rewrite_eomonth(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if args:
            return exp.Anonymous(this="LAST_DAY", expressions=[args[0]])
        return node

    @staticmethod
    def _rewrite_count_big(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        # Redshift COUNT always returns BIGINT
        return exp.Anonymous(this="COUNT", expressions=node.expressions)

    @staticmethod
    def _rewrite_convert(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) < 2:
            return node
        type_arg = args[0]
        value_arg = args[1]
        style_arg = args[2] if len(args) > 2 else None

        # If there's a style argument, map to TO_CHAR
        if style_arg is not None:
            try:
                style_num = int(style_arg.name)
            except (AttributeError, ValueError, TypeError):
                style_num = None

            if style_num is not None and style_num in _CONVERT_STYLE_MAP:
                fmt = _CONVERT_STYLE_MAP[style_num]
                return exp.Anonymous(
                    this="TO_CHAR",
                    expressions=[value_arg, exp.Literal.string(fmt)],
                )
            else:
                # Unknown style — emit CAST with warning
                ctx.warn(
                    f"CONVERT style code {style_arg} not mapped — "
                    "converted to CAST; verify output format."
                )

        # Default: rewrite as CAST(value AS type)
        return exp.Cast(this=value_arg, to=type_arg)

    @staticmethod
    def _rewrite_format(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) < 2:
            return node
        value_arg, pattern_arg = args[0], args[1]
        # Best-effort: map .NET format strings to TO_CHAR patterns
        pattern_str = (
            pattern_arg.name
            if hasattr(pattern_arg, "name")
            else str(pattern_arg).strip("'\"")
        )
        redshift_pattern = _dotnet_to_tochar(pattern_str)
        ctx.warn(
            f"FORMAT(value, '{pattern_str}') mapped to TO_CHAR — "
            "verify the format pattern is correct for your use case."
        )
        return exp.Anonymous(
            this="TO_CHAR",
            expressions=[value_arg, exp.Literal.string(redshift_pattern)],
        )

    @staticmethod
    def _rewrite_nvl(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        return exp.Coalesce(expressions=node.expressions)

    @staticmethod
    def _rewrite_patindex(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        args = node.expressions
        if len(args) < 2:
            return node
        pattern, string = args[0], args[1]
        ctx.warn(
            "PATINDEX uses % wildcard patterns; REGEXP_INSTR uses regex. "
            "Verify the pattern syntax is correct after migration."
        )
        # Strip leading/trailing % from pattern literal if present
        return exp.Anonymous(this="REGEXP_INSTR", expressions=[string, pattern])

    @staticmethod
    def _rewrite_soundex(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        ctx.warn("SOUNDEX is not supported in Redshift — no equivalent function available.")
        return exp.Anonymous(this="__UNSUPPORTED_SOUNDEX__", expressions=node.expressions)

    @staticmethod
    def _rewrite_try_parse(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        ctx.warn(
            "TRY_PARSE has no Redshift equivalent — "
            "consider using CASE WHEN ... ~ 'pattern' THEN CAST(...) ELSE NULL END."
        )
        return node

    @staticmethod
    def _rewrite_checksum(node: exp.Anonymous, ctx: TransformContext) -> exp.Expression:
        ctx.warn("CHECKSUM mapped to MD5 — semantics differ; verify usage.")
        args = node.expressions
        if args:
            return exp.Anonymous(
                this="MD5",
                expressions=[exp.Cast(this=args[0], to=exp.DataType(this=exp.DataType.Type.VARCHAR))],
            )
        return node

    def _rewrite_try_cast(self, node: exp.TryCast, ctx: TransformContext) -> exp.Expression:
        """
        TRY_CAST has no native Redshift equivalent.
        Generate a UDF block and replace the call with safe_cast_<type>().
        """
        target_type = node.to
        type_str = str(target_type.this.value).upper() if hasattr(target_type.this, "value") else "VARCHAR"
        type_lower = type_str.lower().replace(" ", "_")

        udf_body = _TRY_CAST_UDF.format(
            type_lower=type_lower,
            redshift_type=type_str,
        )
        # Only add the UDF once
        if udf_body not in ctx.udf_blocks:
            ctx.udf_blocks.append(udf_body)
            ctx.warn(
                f"TRY_CAST(... AS {type_str}) has no Redshift equivalent. "
                f"A safe_cast_{type_lower}() UDF has been generated — "
                "deploy it before running this query."
            )

        return exp.Anonymous(
            this=f"safe_cast_{type_lower}",
            expressions=[
                exp.Cast(
                    this=node.this,
                    to=exp.DataType(this=exp.DataType.Type.VARCHAR),
                )
            ],
        )

    @staticmethod
    def _rewrite_cast(node: exp.Cast, ctx: TransformContext) -> exp.Expression:
        # Cast is handled by DataTypeConverter for the type portion;
        # here we just pass through
        return node

    @staticmethod
    def _rewrite_str_position(node: exp.StrPosition, ctx: TransformContext) -> exp.Expression:
        """
        sqlglot parses CHARINDEX(needle, haystack) → StrPosition(this=haystack, substr=needle).
        Redshift uses STRPOS(haystack, needle).
        Also handle the 3-arg form (position argument).
        """
        haystack = node.this
        needle = node.args.get("substr")
        position = node.args.get("position")

        if position is not None:
            ctx.warn(
                "CHARINDEX(needle, haystack, start) has no direct Redshift equivalent. "
                "The start-offset argument has been dropped — verify correctness."
            )

        return exp.Anonymous(this="STRPOS", expressions=[haystack, needle])

    @staticmethod
    def _rewrite_current_timestamp(
        node: exp.CurrentTimestamp, ctx: TransformContext
    ) -> exp.Expression:
        """sqlglot parses GETDATE() → CurrentTimestamp(). Map to SYSDATE in Redshift."""
        ctx.warn(
            "GETDATE() returns local server time in SQL Server but UTC in Redshift (SYSDATE). "
            "If your SQL Server ran in a non-UTC timezone, date values may shift."
        )
        # SYSDATE is a keyword in Redshift (no parentheses) — use Var not Anonymous
        return exp.Var(this="SYSDATE")

    @staticmethod
    def _rewrite_extract(node: exp.Extract, ctx: TransformContext) -> exp.Expression:
        """
        Handle EXTRACT nodes, particularly DAYOFWEEK which has an off-by-one vs T-SQL.
        T-SQL DATEPART(weekday,...) → Sunday=1..Saturday=7
        Redshift EXTRACT(DOW FROM ...) → Sunday=0..Saturday=6
        Add +1 to match T-SQL output.
        """
        part = node.this
        part_name = part.name.upper() if hasattr(part, "name") else str(part).upper()

        if part_name == "DAYOFWEEK":
            ctx.warn(
                "DATEPART(weekday/dw, ...) offset differs: T-SQL Sunday=1, Redshift Sunday=0. "
                "Added +1 to match T-SQL behaviour — verify if downstream code uses 0-based values."
            )
            # Change DAYOFWEEK → DOW (Redshift standard) and add +1
            new_extract = exp.Extract(
                this=exp.Var(this="DOW"),
                expression=node.expression,
            )
            return exp.Add(this=new_extract, expression=exp.Literal.number(1))

        return node

    def _apply_yaml_rule(self, node: exp.Func, ctx: TransformContext) -> exp.Expression:
        if self.rule_registry:
            return self.rule_registry.apply_function_rule(node, ctx)
        return node


def _dotnet_to_tochar(pattern: str) -> str:
    """Best-effort conversion of .NET format strings to TO_CHAR patterns."""
    replacements = [
        ("yyyy", "YYYY"),
        ("yy", "YY"),
        ("MM", "MM"),
        ("dd", "DD"),
        ("HH", "HH24"),
        ("hh", "HH12"),
        ("mm", "MI"),
        ("ss", "SS"),
        ("fff", "MS"),
        ("tt", "AM"),
    ]
    result = pattern
    for dotnet, pg in replacements:
        result = result.replace(dotnet, pg)
    return result
