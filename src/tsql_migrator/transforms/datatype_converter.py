"""
DataTypeConverter: map T-SQL data types to Redshift equivalents.

Rules are loaded from rules/datatype_rules.yaml.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from tsql_migrator.transforms.base import TransformContext, TransformPass


class DataTypeConverter(TransformPass):
    """Replace T-SQL data types with their Redshift equivalents."""

    # Static mapping: (tsql_type_name_upper → conversion_info)
    # conversion_info keys: redshift_type, precision, scale, preserve_precision, warn, hard_error
    _TYPE_MAP: dict[str, dict] = {
        "NVARCHAR": {"redshift_type": "VARCHAR", "preserve_precision": True},
        "NCHAR": {"redshift_type": "CHAR", "preserve_precision": True},
        "BIT": {"redshift_type": "BOOLEAN"},
        "DATETIME": {"redshift_type": "TIMESTAMP"},
        "DATETIME2": {"redshift_type": "TIMESTAMP"},
        "SMALLDATETIME": {"redshift_type": "TIMESTAMP"},
        "MONEY": {"redshift_type": "DECIMAL", "precision": 19, "scale": 4},
        "SMALLMONEY": {"redshift_type": "DECIMAL", "precision": 10, "scale": 4},
        "TINYINT": {"redshift_type": "SMALLINT"},
        "UNIQUEIDENTIFIER": {
            "redshift_type": "VARCHAR",
            "precision": 36,
            "warn": "UNIQUEIDENTIFIER mapped to VARCHAR(36) — UUID semantics not enforced",
        },
        "TEXT": {
            "redshift_type": "VARCHAR",
            "precision": 65535,
            "warn": "TEXT mapped to VARCHAR(65535) — check for data truncation risk",
        },
        "NTEXT": {
            "redshift_type": "VARCHAR",
            "precision": 65535,
            "warn": "NTEXT mapped to VARCHAR(65535) — check for data truncation risk",
        },
        "IMAGE": {
            "redshift_type": "VARCHAR",
            "precision": 65535,
            "warn": "IMAGE has no Redshift equivalent — mapped to VARCHAR(65535); manual review required",
        },
        "XML": {
            "redshift_type": "VARCHAR",
            "precision": 65535,
            "warn": "XML mapped to VARCHAR(65535) — Redshift has no XML type; use JSON functions if applicable",
        },
        "HIERARCHYID": {
            "redshift_type": "VARCHAR",
            "precision": 4000,
            "warn": "HIERARCHYID has no Redshift equivalent — mapped to VARCHAR(4000)",
        },
        "GEOGRAPHY": {
            "redshift_type": "VARCHAR",
            "precision": 65535,
            "warn": "GEOGRAPHY has no Redshift equivalent — mapped to VARCHAR(65535); manual review required",
        },
        "GEOMETRY": {
            "redshift_type": "VARCHAR",
            "precision": 65535,
            "warn": "GEOMETRY has no Redshift equivalent — mapped to VARCHAR(65535); manual review required",
        },
        "DATETIMEOFFSET": {"redshift_type": "TIMESTAMPTZ"},
        "REAL": {"redshift_type": "FLOAT4"},
        "FLOAT": {"redshift_type": "FLOAT8"},
    }

    def transform(self, ast: exp.Expression, ctx: TransformContext) -> exp.Expression:
        return ast.transform(self._convert_type, ctx)

    def _convert_type(self, node: exp.Expression, ctx: TransformContext) -> exp.Expression:
        if not isinstance(node, exp.DataType):
            return node

        type_name = node.this
        # sqlglot uses DataType.Type enum values; get the string name
        type_str = type_name.value.upper() if hasattr(type_name, "value") else str(type_name).upper()

        # sqlglot maps some T-SQL types to its own internal names
        # Map those back to the T-SQL names we have rules for
        _INTERNAL_ALIASES = {
            "UTINYINT": "TINYINT",  # sqlglot parses TINYINT as UTINYINT
            "UUID": "UNIQUEIDENTIFIER",  # sqlglot parses UNIQUEIDENTIFIER as UUID
            "DOUBLE": "FLOAT",
        }
        type_str = _INTERNAL_ALIASES.get(type_str, type_str)

        rule = self._TYPE_MAP.get(type_str)
        if rule is None:
            return node

        if rule.get("warn"):
            ctx.warn(rule["warn"])

        new_type_str = rule["redshift_type"]
        new_type_enum = self._str_to_datatype(new_type_str)
        if new_type_enum is None:
            return node

        # Build new expressions (precision/scale args)
        new_expressions = []
        if rule.get("preserve_precision") and node.expressions:
            new_expressions = node.expressions
        elif rule.get("precision") is not None:
            new_expressions = [exp.DataTypeParam(this=exp.Literal.number(rule["precision"]))]
            if rule.get("scale") is not None:
                new_expressions.append(
                    exp.DataTypeParam(this=exp.Literal.number(rule["scale"]))
                )

        return exp.DataType(this=new_type_enum, expressions=new_expressions, nested=node.args.get("nested"))

    @staticmethod
    def _str_to_datatype(name: str) -> exp.DataType.Type | None:
        """Map a type name string to sqlglot's DataType.Type enum."""
        mapping = {
            "VARCHAR": exp.DataType.Type.VARCHAR,
            "CHAR": exp.DataType.Type.CHAR,
            "BOOLEAN": exp.DataType.Type.BOOLEAN,
            "TIMESTAMP": exp.DataType.Type.TIMESTAMP,
            "TIMESTAMPTZ": exp.DataType.Type.TIMESTAMPTZ,
            "DECIMAL": exp.DataType.Type.DECIMAL,
            "SMALLINT": exp.DataType.Type.SMALLINT,
            "FLOAT4": exp.DataType.Type.FLOAT,
            "FLOAT8": exp.DataType.Type.DOUBLE,
        }
        return mapping.get(name.upper())
