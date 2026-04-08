"""Tests for the parser module."""

import pytest
from tsql_migrator.parser import parse_tsql
from tsql_migrator.errors import HardError, ParseError


def test_parses_simple_select():
    stmts = parse_tsql("SELECT 1 AS val")
    assert len(stmts) == 1


def test_parses_select_with_from():
    stmts = parse_tsql("SELECT OrderID FROM dbo.Orders")
    assert len(stmts) == 1


def test_raises_hard_error_for_exec():
    with pytest.raises(HardError) as exc_info:
        parse_tsql("EXEC sp_helptext 'dbo.Orders'")
    assert "dynamic SQL" in str(exc_info.value).lower() or "EXEC" in str(exc_info.value)


def test_raises_hard_error_for_recursive_cte():
    sql = """
    WITH Emp AS (
        SELECT EmployeeID, ManagerID FROM dbo.Employees WHERE ManagerID IS NULL
        UNION ALL
        SELECT e.EmployeeID, e.ManagerID FROM dbo.Employees e
        INNER JOIN Emp m ON e.ManagerID = m.EmployeeID
    )
    SELECT * FROM Emp
    """
    with pytest.raises(HardError) as exc_info:
        parse_tsql(sql)
    assert "recursive" in str(exc_info.value).lower() or "CTE" in str(exc_info.value)


def test_parses_cte():
    sql = """
    WITH Sales AS (SELECT OrderID FROM dbo.Orders)
    SELECT * FROM Sales
    """
    stmts = parse_tsql(sql)
    assert len(stmts) == 1


def test_parses_multiple_statements():
    sql = "SELECT 1; SELECT 2"
    stmts = parse_tsql(sql)
    assert len(stmts) == 2
