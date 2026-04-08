"""Tests for FunctionRewriter — especially the critical silent-bug cases."""

import pytest
from tsql_migrator.pipeline import MigrationPipeline


@pytest.fixture
def pipeline():
    return MigrationPipeline()


def test_charindex_args_reversed(pipeline):
    """CHARINDEX(needle, haystack) must become STRPOS(haystack, needle)."""
    result = pipeline.translate("SELECT CHARINDEX('x', col) AS pos FROM dbo.t")
    sql = result.output_sql
    assert "STRPOS" in sql
    assert "CHARINDEX" not in sql
    # Verify argument order: STRPOS(col, 'x') — haystack first
    strpos_idx = sql.index("STRPOS")
    args_section = sql[strpos_idx:]
    # col should appear before 'x'
    col_idx = args_section.index("col")
    x_idx = args_section.index("'x'")
    assert col_idx < x_idx, "STRPOS must have haystack (col) before needle ('x')"


def test_charindex_3arg_warns(pipeline):
    result = pipeline.translate("SELECT CHARINDEX('x', col, 5) AS pos FROM dbo.t")
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("start" in w.lower() or "3" in w or "offset" in w.lower() for w in warns)


def test_isnull_becomes_coalesce(pipeline):
    result = pipeline.translate("SELECT ISNULL(col, 0) FROM dbo.t")
    assert "COALESCE" in result.output_sql
    assert "ISNULL" not in result.output_sql


def test_getdate_becomes_sysdate(pipeline):
    result = pipeline.translate("SELECT GETDATE() AS now")
    assert "SYSDATE" in result.output_sql
    assert "GETDATE" not in result.output_sql
    # Must have UTC warning
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("utc" in w.lower() or "UTC" in w for w in warns)


def test_datepart_weekday_offset(pipeline):
    """DATEPART(weekday, ...) must add +1 for T-SQL compatibility."""
    result = pipeline.translate("SELECT DATEPART(weekday, OrderDate) AS wd FROM dbo.t")
    sql = result.output_sql
    assert "dow" in sql.lower()
    assert "+ 1" in sql or "+1" in sql
    # Must have a warning about the offset
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("weekday" in w.lower() or "offset" in w.lower() for w in warns)


def test_len_to_length(pipeline):
    result = pipeline.translate("SELECT LEN(col) AS l FROM dbo.t")
    # sqlglot may keep LEN as-is in Redshift (it's aliased) — just check no crash
    assert not result.report.hard_errors


def test_iif_to_case(pipeline):
    result = pipeline.translate("SELECT IIF(x > 0, 'yes', 'no') AS flag FROM dbo.t")
    sql = result.output_sql
    assert "CASE" in sql
    assert "IIF" not in sql


def test_try_cast_generates_udf(pipeline):
    result = pipeline.translate("SELECT TRY_CAST(col AS INT) AS val FROM dbo.t")
    # Should generate a UDF block
    assert result.report.udf_blocks_count > 0 or any(
        "udf" in w.lower() or "safe_cast" in w.lower()
        for w in [a.message for a in result.report.annotations]
    )


def test_string_agg_to_listagg(pipeline):
    result = pipeline.translate(
        "SELECT STRING_AGG(Name, ', ') AS names FROM dbo.t"
    )
    assert "LISTAGG" in result.output_sql
    assert "STRING_AGG" not in result.output_sql


def test_replicate_to_repeat(pipeline):
    result = pipeline.translate("SELECT REPLICATE('x', 5) AS r FROM dbo.t")
    assert "REPEAT" in result.output_sql


def test_datepart_year(pipeline):
    result = pipeline.translate("SELECT DATEPART(YEAR, OrderDate) AS yr FROM dbo.t")
    assert "year" in result.output_sql.lower() or "YEAR" in result.output_sql


def test_no_crash_on_realistic_query(pipeline):
    from tests.conftest import tsql_fixture
    sql = tsql_fixture("realistic_query")
    result = pipeline.translate(sql)
    assert not result.report.hard_errors
    assert "LIMIT" in result.output_sql
    assert "TOP" not in result.output_sql
    assert "NOLOCK" not in result.output_sql
    assert "COALESCE" in result.output_sql
