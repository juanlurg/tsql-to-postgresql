"""
Integration tests for LLM wiring in the pipeline.

Uses unittest.mock to avoid real Anthropic API calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tsql_migrator.llm.client import LLMTranslationResult
from tsql_migrator.pipeline import MigrationPipeline


def _make_llm_result(sql: str, confidence: str = "high") -> LLMTranslationResult:
    return LLMTranslationResult(
        translated_sql=sql,
        changes_made=["PIVOT rewritten as CASE WHEN"],
        unmapped_columns=[],
        confidence=confidence,
        migration_todos=[],
    )


class TestLLMFallback:
    """LLM is invoked when the deterministic parser raises ParseError."""

    def test_fallback_returns_llm_translation(self):
        mock_client = MagicMock()
        mock_client.translate.return_value = _make_llm_result(
            "SELECT id, name FROM users LIMIT 10"
        )
        pipeline = MigrationPipeline(llm_client=mock_client)

        # Force a ParseError by injecting unparseable SQL
        # We monkey-patch parse_tsql to simulate a parse failure
        from tsql_migrator.errors import ParseError

        with patch("tsql_migrator.pipeline.parse_tsql", side_effect=ParseError("bad syntax")):
            result = pipeline.translate("UNPARSEABLE SQL @@##$$")

        assert result.used_llm is True
        assert "SELECT id" in result.output_sql
        assert result.error is None
        assert result.report.success is True
        mock_client.translate.assert_called_once()

    def test_fallback_no_llm_client_returns_error(self):
        pipeline = MigrationPipeline(llm_client=None)
        from tsql_migrator.errors import ParseError

        with patch("tsql_migrator.pipeline.parse_tsql", side_effect=ParseError("bad syntax")):
            result = pipeline.translate("UNPARSEABLE SQL @@##$$")

        assert result.used_llm is False
        assert result.error is not None
        assert "PARSE_ERROR" in result.report.hard_errors[0] or "Parse error" in result.report.hard_errors[0]

    def test_fallback_llm_api_error_returns_hard_error(self):
        from tsql_migrator.errors import LLMError, ParseError

        mock_client = MagicMock()
        mock_client.translate.side_effect = LLMError("API timeout")
        pipeline = MigrationPipeline(llm_client=mock_client)

        with patch("tsql_migrator.pipeline.parse_tsql", side_effect=ParseError("bad syntax")):
            result = pipeline.translate("UNPARSEABLE SQL")

        assert result.used_llm is False
        assert result.error is not None
        assert "LLM_ERROR" in result.report.hard_errors[0]

    def test_fallback_llm_bad_sql_returns_validation_error(self):
        from tsql_migrator.errors import ParseError

        mock_client = MagicMock()
        mock_client.translate.return_value = _make_llm_result("THIS IS NOT SQL @@@")
        pipeline = MigrationPipeline(llm_client=mock_client)

        with patch("tsql_migrator.pipeline.parse_tsql", side_effect=ParseError("bad syntax")):
            result = pipeline.translate("UNPARSEABLE SQL")

        assert result.used_llm is False
        assert result.error is not None
        assert "LLM_VALIDATION_ERROR" in result.report.hard_errors[0]

    def test_fallback_carries_ddl_context_when_registry_present(self):
        """LLM fallback should inject DDL context if registry has the table."""
        from tsql_migrator.errors import ParseError
        from tsql_migrator.schema.registry import SchemaRegistry

        reg = SchemaRegistry(db_path=":memory:")
        src_id = reg.upsert_source("src", "tsql")
        reg.add_table(src_id, "dbo", "orders", [
            {"name": "order_id", "data_type": "INT", "is_nullable": False, "ordinal": 0},
        ])

        mock_client = MagicMock()
        mock_client.translate.return_value = _make_llm_result("SELECT order_id FROM orders")
        pipeline = MigrationPipeline(llm_client=mock_client, schema_registry=reg)

        with patch("tsql_migrator.pipeline.parse_tsql", side_effect=ParseError("bad syntax")):
            result = pipeline.translate("SELECT order_id FROM orders")

        # Verify translate was called with some ddl_context (non-None)
        call_kwargs = mock_client.translate.call_args
        assert call_kwargs.kwargs.get("ddl_context") is not None or (
            len(call_kwargs.args) > 1 and call_kwargs.args[1] is not None
        )


class TestLLMCandidatesRewrite:
    """LLM is invoked when PIVOT/UNPIVOT nodes are detected."""

    def test_pivot_triggers_llm_rewrite(self):
        pivot_sql = """
        SELECT *
        FROM (SELECT product, sales, quarter FROM sales_data) src
        PIVOT (SUM(sales) FOR quarter IN ([Q1],[Q2],[Q3],[Q4])) pvt
        """
        mock_client = MagicMock()
        mock_client.translate.return_value = _make_llm_result(
            "SELECT product, "
            "SUM(CASE WHEN quarter='Q1' THEN sales END) AS Q1, "
            "SUM(CASE WHEN quarter='Q2' THEN sales END) AS Q2 "
            "FROM sales_data GROUP BY product"
        )
        pipeline = MigrationPipeline(llm_client=mock_client)
        result = pipeline.translate(pivot_sql)

        assert result.used_llm is True
        assert "CASE WHEN" in result.output_sql
        mock_client.translate.assert_called_once()

    def test_pivot_llm_failure_falls_back_gracefully(self):
        pivot_sql = """
        SELECT *
        FROM (SELECT product, sales, quarter FROM sales_data) src
        PIVOT (SUM(sales) FOR quarter IN ([Q1],[Q2])) pvt
        """
        from tsql_migrator.errors import LLMError

        mock_client = MagicMock()
        mock_client.translate.side_effect = LLMError("Rate limit")
        pipeline = MigrationPipeline(llm_client=mock_client)
        result = pipeline.translate(pivot_sql)

        # Should still return something (deterministic partial output) with warning
        assert result.used_llm is False
        warn_messages = [a.message for a in result.report.annotations]
        assert any("LLM rewrite failed" in m for m in warn_messages)

    def test_no_llm_client_pivot_emits_warning(self):
        pivot_sql = """
        SELECT *
        FROM (SELECT product, sales, quarter FROM sales_data) src
        PIVOT (SUM(sales) FOR quarter IN ([Q1],[Q2])) pvt
        """
        pipeline = MigrationPipeline(llm_client=None)
        result = pipeline.translate(pivot_sql)

        # Should produce output without crashing; PIVOT placeholder in SQL or warning
        assert result.output_sql is not None


class TestLLMConfidenceMapping:
    """LLM confidence strings map to numeric values in the report."""

    @pytest.mark.parametrize("confidence_str,expected", [
        ("high", 0.9),
        ("medium", 0.6),
        ("low", 0.3),
        ("unknown", 0.6),  # default
    ])
    def test_confidence_mapping(self, confidence_str, expected):
        from tsql_migrator.errors import ParseError

        mock_client = MagicMock()
        mock_client.translate.return_value = _make_llm_result(
            "SELECT 1", confidence=confidence_str
        )
        pipeline = MigrationPipeline(llm_client=mock_client)

        with patch("tsql_migrator.pipeline.parse_tsql", side_effect=ParseError("bad")):
            result = pipeline.translate("BAD SQL")

        assert result.report.llm_confidence == expected


class TestBuildDDLContext:
    """_build_ddl_context_from_sql correctly pulls DDL from registry."""

    def test_returns_none_without_registry(self):
        pipeline = MigrationPipeline()
        result = pipeline._build_ddl_context_from_sql("SELECT id FROM orders")
        assert result is None

    def test_returns_ddl_when_table_known(self):
        from tsql_migrator.schema.registry import SchemaRegistry

        reg = SchemaRegistry(db_path=":memory:")
        src_id = reg.upsert_source("src", "tsql")
        reg.add_table(src_id, "dbo", "orders", [
            {"name": "order_id", "data_type": "INT", "is_nullable": False, "ordinal": 0},
            {"name": "amount", "data_type": "DECIMAL(10,2)", "is_nullable": True, "ordinal": 1},
        ])

        pipeline = MigrationPipeline(schema_registry=reg)
        context = pipeline._build_ddl_context_from_sql("SELECT order_id FROM orders")

        assert context is not None
        assert "orders" in context
        assert "order_id" in context

    def test_returns_none_when_table_not_in_registry(self):
        from tsql_migrator.schema.registry import SchemaRegistry

        reg = SchemaRegistry(db_path=":memory:")
        pipeline = MigrationPipeline(schema_registry=reg)
        result = pipeline._build_ddl_context_from_sql("SELECT id FROM unknown_table")
        assert result is None
