"""Tests for DataTypeConverter."""

import pytest
from tsql_migrator.pipeline import MigrationPipeline


@pytest.fixture
def pipeline():
    return MigrationPipeline()


def test_nvarchar_becomes_varchar(pipeline):
    result = pipeline.translate("SELECT CAST(col AS NVARCHAR(100)) FROM dbo.t")
    assert "VARCHAR" in result.output_sql
    assert "NVARCHAR" not in result.output_sql


def test_bit_becomes_boolean(pipeline):
    result = pipeline.translate("SELECT CAST(flag AS BIT) FROM dbo.t")
    assert "BOOLEAN" in result.output_sql
    assert "BIT" not in result.output_sql


def test_datetime_becomes_timestamp(pipeline):
    result = pipeline.translate("SELECT CAST(col AS DATETIME) FROM dbo.t")
    assert "TIMESTAMP" in result.output_sql
    assert "DATETIME" not in result.output_sql


def test_money_becomes_decimal(pipeline):
    result = pipeline.translate("SELECT CAST(amount AS MONEY) FROM dbo.t")
    assert "DECIMAL" in result.output_sql or "NUMERIC" in result.output_sql
    assert "MONEY" not in result.output_sql


def test_uniqueidentifier_warns(pipeline):
    result = pipeline.translate("SELECT CAST(id AS UNIQUEIDENTIFIER) FROM dbo.t")
    warns = [a.message for a in result.report.annotations if a.severity == "warn"]
    assert any("UNIQUEIDENTIFIER" in w or "uuid" in w.lower() for w in warns)


def test_tinyint_becomes_smallint(pipeline):
    result = pipeline.translate("SELECT CAST(x AS TINYINT) FROM dbo.t")
    assert "SMALLINT" in result.output_sql
    assert "TINYINT" not in result.output_sql
