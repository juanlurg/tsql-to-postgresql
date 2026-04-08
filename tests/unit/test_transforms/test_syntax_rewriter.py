"""Tests for SyntaxRewriter."""

import pytest
from tsql_migrator.pipeline import MigrationPipeline


@pytest.fixture
def pipeline():
    return MigrationPipeline()


def test_top_n_becomes_limit(pipeline):
    result = pipeline.translate("SELECT TOP 100 OrderID FROM dbo.Orders")
    sql = result.output_sql
    assert "LIMIT 100" in sql
    assert "TOP" not in sql


def test_top_n_at_end_of_query(pipeline):
    """LIMIT must appear at the end of the statement, not the top."""
    result = pipeline.translate("SELECT TOP 50 col FROM dbo.t ORDER BY col DESC")
    sql = result.output_sql
    limit_idx = sql.upper().index("LIMIT")
    from_idx = sql.upper().index("FROM")
    assert limit_idx > from_idx, "LIMIT should come after FROM"


def test_top_with_ties_warns(pipeline):
    result = pipeline.translate(
        "SELECT TOP 10 WITH TIES col FROM dbo.t ORDER BY col"
    )
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("ties" in w.lower() or "TIES" in w for w in warns)
    assert "TOP" not in result.output_sql


def test_recursive_cte_raises_hard_error(pipeline):
    from tests.conftest import tsql_fixture
    sql = tsql_fixture("recursive_cte")
    result = pipeline.translate(sql)
    assert result.report.hard_errors
    assert any("recursive" in e.lower() or "CTE" in e for e in result.report.hard_errors)


def test_select_into_temp(pipeline):
    sql = "SELECT col1, col2 INTO #temp_results FROM dbo.Orders WHERE Status = 1"
    result = pipeline.translate(sql)
    assert "TEMP" in result.output_sql or "TEMPORARY" in result.output_sql
    assert "#" not in result.output_sql


def test_pivot_flagged_for_rewrite(pipeline):
    sql = """
    SELECT [2023], [2024]
    FROM (SELECT year, amount FROM dbo.Sales) AS src
    PIVOT (SUM(amount) FOR year IN ([2023], [2024])) AS pvt
    """
    result = pipeline.translate(sql)
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("pivot" in w.lower() for w in warns)


def test_string_concat_warn(pipeline):
    result = pipeline.translate("SELECT 'Hello' + ' World' AS greeting")
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("concat" in w.lower() or "||" in w for w in warns)
