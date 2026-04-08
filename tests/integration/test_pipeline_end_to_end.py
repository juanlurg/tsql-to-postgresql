"""End-to-end integration tests for the full pipeline."""

from __future__ import annotations

import pytest
from tsql_migrator.pipeline import MigrationPipeline
from tests.conftest import tsql_fixture


@pytest.fixture
def pipeline():
    return MigrationPipeline()


class TestRealisticQuery:
    def test_no_hard_errors(self, pipeline):
        result = pipeline.translate(tsql_fixture("realistic_query"))
        assert not result.report.hard_errors

    def test_top_converted_to_limit(self, pipeline):
        result = pipeline.translate(tsql_fixture("realistic_query"))
        assert "LIMIT 500" in result.output_sql
        assert "TOP" not in result.output_sql

    def test_nolock_removed(self, pipeline):
        result = pipeline.translate(tsql_fixture("realistic_query"))
        assert "NOLOCK" not in result.output_sql

    def test_isnull_converted(self, pipeline):
        result = pipeline.translate(tsql_fixture("realistic_query"))
        assert "COALESCE" in result.output_sql
        assert "ISNULL" not in result.output_sql

    def test_datepart_converted(self, pipeline):
        result = pipeline.translate(tsql_fixture("realistic_query"))
        # DATEPART(YEAR,...) and DATEPART(MONTH,...) should be converted
        assert "DATEPART" not in result.output_sql

    def test_schema_prefix_mapped(self, pipeline):
        result = pipeline.translate(tsql_fixture("realistic_query"))
        # dbo schema should be replaced with public
        assert "dbo." not in result.output_sql
        assert "public." in result.output_sql

    def test_output_is_valid_sql(self, pipeline):
        """The output must parse as valid Redshift SQL."""
        import sqlglot
        result = pipeline.translate(tsql_fixture("realistic_query"))
        # Strip annotation comments before parsing
        lines = [l for l in result.output_sql.splitlines() if not l.strip().startswith("--")]
        clean_sql = "\n".join(lines)
        try:
            parsed = sqlglot.parse(clean_sql, dialect="redshift")
            assert parsed
        except sqlglot.errors.ParseError as e:
            pytest.fail(f"Output SQL is not valid Redshift SQL: {e}\n\nSQL:\n{result.output_sql}")


class TestHardErrors:
    def test_recursive_cte(self, pipeline):
        result = pipeline.translate(tsql_fixture("recursive_cte"))
        assert result.report.hard_errors
        assert result.output_sql == ""

    def test_exec_dynamic_sql(self, pipeline):
        result = pipeline.translate("EXEC sp_helptext 'dbo.Orders'")
        assert result.report.hard_errors


class TestSilentBugs:
    def test_charindex_arg_order(self, pipeline):
        """The most dangerous silent bug: CHARINDEX args must be reversed."""
        result = pipeline.translate("SELECT CHARINDEX('needle', haystack_col) FROM dbo.t")
        sql = result.output_sql
        assert "STRPOS" in sql
        # haystack_col must come before 'needle'
        strpos_start = sql.index("STRPOS")
        segment = sql[strpos_start:]
        assert segment.index("haystack_col") < segment.index("'needle'")

    def test_datepart_weekday_adds_offset(self, pipeline):
        result = pipeline.translate(
            "SELECT DATEPART(weekday, d) AS wd FROM dbo.t"
        )
        # Must contain dow + 1 to correct the T-SQL vs Redshift off-by-one
        sql = result.output_sql
        assert "dow" in sql.lower()
        assert "+ 1" in sql or "+1" in sql
