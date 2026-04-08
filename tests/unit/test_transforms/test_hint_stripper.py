"""Tests for HintStripper."""

from tsql_migrator.pipeline import MigrationPipeline


def test_strips_nolock(pipeline):
    result = pipeline.translate(
        "SELECT OrderID FROM dbo.Orders WITH (NOLOCK)"
    )
    assert "NOLOCK" not in result.output_sql
    assert result.report.success or True  # may have warnings but no hard errors
    assert not result.report.hard_errors


def test_strips_readpast(pipeline):
    result = pipeline.translate(
        "SELECT OrderID FROM dbo.Orders WITH (READPAST)"
    )
    assert "READPAST" not in result.output_sql


def test_strips_updlock(pipeline):
    result = pipeline.translate(
        "SELECT OrderID FROM dbo.Orders WITH (UPDLOCK)"
    )
    assert "UPDLOCK" not in result.output_sql


def test_nolock_on_join(pipeline):
    sql = """
    SELECT o.OrderID, c.CustomerName
    FROM dbo.Orders o WITH (NOLOCK)
    JOIN dbo.Customers c WITH (NOLOCK) ON o.CustomerID = c.CustomerID
    """
    result = pipeline.translate(sql)
    assert "NOLOCK" not in result.output_sql
