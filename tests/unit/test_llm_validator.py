"""Unit tests for llm/validator.py."""

from __future__ import annotations

import pytest

from tsql_migrator.llm.validator import ValidationResult, validate_llm_output


class TestValidateLLMOutputParse:
    def test_valid_simple_select(self):
        result = validate_llm_output("SELECT id, name FROM users")
        assert result.valid is True
        assert result.parse_error is None

    def test_invalid_sql_returns_parse_error(self):
        result = validate_llm_output("THIS IS NOT SQL @@@@")
        assert result.valid is False
        assert result.parse_error is not None

    def test_empty_sql_returns_error(self):
        result = validate_llm_output("")
        assert result.valid is False

    def test_valid_cte_query(self):
        sql = """
        WITH cte AS (SELECT id FROM orders WHERE status = 'open')
        SELECT * FROM cte
        """
        result = validate_llm_output(sql)
        assert result.valid is True

    def test_redshift_specific_syntax(self):
        # LISTAGG is Redshift-valid
        sql = "SELECT LISTAGG(name, ',') WITHIN GROUP (ORDER BY 1) FROM t"
        result = validate_llm_output(sql)
        assert result.valid is True


class TestValidateLLMOutputColumnCheck:
    def _make_registry(self, approved_columns: list[tuple[str, str]]):
        """Build an in-memory registry with given (table, column) pairs approved."""
        from tsql_migrator.schema.registry import SchemaRegistry

        reg = SchemaRegistry(db_path=":memory:")
        src_id = reg.upsert_source("src", "tsql")
        tgt_id = reg.upsert_source("tgt", "redshift")

        # Group columns by table
        by_table: dict[str, list[str]] = {}
        for tbl, col in approved_columns:
            by_table.setdefault(tbl, []).append(col)

        for tbl, cols in by_table.items():
            reg.add_table(tgt_id, "public", tbl, [
                {"name": col, "data_type": "VARCHAR", "is_nullable": True, "ordinal": i}
                for i, col in enumerate(cols)
            ])
            for col in cols:
                reg.upsert_column_mapping(
                    src_schema="dbo", src_table=tbl, src_col=col,
                    tgt_schema="public", tgt_table=tbl, tgt_col=col,
                    confidence=0.95, source="auto_exact", approved=True,
                )
        return reg

    def test_known_columns_pass(self):
        reg = self._make_registry([("orders", "order_id"), ("orders", "customer_id")])
        sql = "SELECT order_id, customer_id FROM orders"
        result = validate_llm_output(sql, registry=reg, referenced_tables=["orders"])
        assert result.valid is True
        assert result.hallucinated_columns == []

    def test_hallucinated_column_detected(self):
        reg = self._make_registry([("orders", "order_id")])
        sql = "SELECT order_id, fake_column FROM orders"
        result = validate_llm_output(sql, registry=reg, referenced_tables=["orders"])
        assert result.valid is False
        assert "fake_column" in result.hallucinated_columns

    def test_star_not_flagged(self):
        reg = self._make_registry([("orders", "order_id")])
        sql = "SELECT * FROM orders"
        result = validate_llm_output(sql, registry=reg, referenced_tables=["orders"])
        assert result.valid is True

    def test_no_registry_skips_column_check(self):
        sql = "SELECT totally_fake_col FROM nowhere"
        result = validate_llm_output(sql, registry=None, referenced_tables=None)
        assert result.valid is True  # only parse check runs

    def test_no_referenced_tables_skips_column_check(self):
        sql = "SELECT fake_col FROM t"
        from tsql_migrator.schema.registry import SchemaRegistry
        reg = SchemaRegistry(db_path=":memory:")
        result = validate_llm_output(sql, registry=reg, referenced_tables=None)
        assert result.valid is True
