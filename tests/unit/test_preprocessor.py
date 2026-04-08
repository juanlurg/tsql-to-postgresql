"""Tests for the preprocessor module."""

import pytest
from tsql_migrator.preprocessor import preprocess
from tsql_migrator.errors import HardError


def test_strips_single_line_comments():
    sql = "SELECT 1 -- this is a comment\nFROM t"
    result = preprocess(sql)
    assert "--" not in result.stripped


def test_strips_block_comments():
    sql = "SELECT /* inline comment */ 1 FROM t"
    result = preprocess(sql)
    assert "/*" not in result.stripped
    assert "*/" not in result.stripped


def test_splits_on_go():
    sql = "SELECT 1\nGO\nSELECT 2"
    result = preprocess(sql)
    assert len(result.batches) == 2
    assert "SELECT 1" in result.batches[0]
    assert "SELECT 2" in result.batches[1]


def test_go_case_insensitive():
    sql = "SELECT 1\ngo\nSELECT 2"
    result = preprocess(sql)
    assert len(result.batches) == 2


def test_single_batch_no_go():
    sql = "SELECT CustomerID FROM dbo.Orders"
    result = preprocess(sql)
    assert len(result.batches) == 1
    assert result.batches[0] == sql


def test_go_with_count_raises_hard_error():
    sql = "SELECT 1\nGO 3"
    with pytest.raises(HardError) as exc_info:
        preprocess(sql)
    assert "GO" in str(exc_info.value)


def test_empty_after_strip_raises():
    sql = "-- only a comment"
    with pytest.raises(ValueError):
        preprocess(sql)


def test_preserves_original():
    sql = "SELECT 1 -- comment"
    result = preprocess(sql)
    assert result.original == sql
