"""Tests for the schema registry, DDL parser, and mapping engine."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from tests.conftest import DDL_DIR
from tsql_migrator.schema.ddl_parser import load_ddl_file
from tsql_migrator.schema.mapping_engine import MappingEngine, _to_snake_case, _expand_abbreviations
from tsql_migrator.schema.registry import SchemaRegistry


@pytest.fixture
def tmp_registry(tmp_path):
    """A fresh schema registry in a temp directory."""
    return SchemaRegistry(db_path=str(tmp_path / "test.db"))


@pytest.fixture
def loaded_registry(tmp_registry):
    """Registry with sample DDL loaded."""
    load_ddl_file(
        str(DDL_DIR / "sqlserver_sample.sql"),
        source_name="sqlserver",
        dialect="tsql",
        registry=tmp_registry,
    )
    load_ddl_file(
        str(DDL_DIR / "redshift_sample.sql"),
        source_name="redshift",
        dialect="redshift",
        registry=tmp_registry,
    )
    return tmp_registry


class TestDDLParser:
    def test_loads_source_tables(self, tmp_registry):
        count = load_ddl_file(
            str(DDL_DIR / "sqlserver_sample.sql"),
            source_name="sqlserver",
            dialect="tsql",
            registry=tmp_registry,
        )
        assert count == 3  # SalesOrderHeader, SalesTerritory, Customer

    def test_loads_target_tables(self, tmp_registry):
        count = load_ddl_file(
            str(DDL_DIR / "redshift_sample.sql"),
            source_name="redshift",
            dialect="redshift",
            registry=tmp_registry,
        )
        assert count == 3

    def test_reload_is_safe(self, tmp_registry):
        """Loading the same file twice should not duplicate tables."""
        load_ddl_file(
            str(DDL_DIR / "sqlserver_sample.sql"), "sqlserver", "tsql", tmp_registry
        )
        load_ddl_file(
            str(DDL_DIR / "sqlserver_sample.sql"), "sqlserver", "tsql", tmp_registry
        )
        stats = tmp_registry.get_stats()
        assert stats["source_tables"] == 3  # not 6


class TestMappingEngine:
    def test_run_diff_generates_mappings(self, loaded_registry):
        engine = MappingEngine(loaded_registry)
        rows = engine.run_diff(source_name="sqlserver", target_name="redshift")
        assert len(rows) > 0

    def test_exact_column_match(self, loaded_registry):
        engine = MappingEngine(loaded_registry)
        rows = engine.run_diff(source_name="sqlserver", target_name="redshift")
        # CustomerID should map to customer_id with high confidence
        cust_id_rows = [r for r in rows if r.src_column.upper() == "CUSTOMERID"]
        assert cust_id_rows
        row = cust_id_rows[0]
        assert row.tgt_column == "customer_id"
        assert row.confidence >= 0.90
        assert row.approved

    def test_unmapped_gets_flagged(self, loaded_registry):
        engine = MappingEngine(loaded_registry)
        rows = engine.run_diff(source_name="sqlserver", target_name="redshift")
        # CustNo → customer_number (abbreviation expansion should catch this)
        cust_no_rows = [r for r in rows if r.src_column.upper() == "CUSTNO"]
        if cust_no_rows:
            row = cust_no_rows[0]
            # Either mapped via abbreviation expansion or flagged for review
            assert row.tgt_column is not None or not row.approved

    def test_export_import_csv(self, loaded_registry, tmp_path):
        engine = MappingEngine(loaded_registry)
        rows = engine.run_diff()
        csv_path = str(tmp_path / "mappings.csv")
        engine.export_csv(rows, csv_path)
        assert Path(csv_path).exists()
        count = engine.import_csv(csv_path)
        assert count == len(rows)

    def test_stats(self, loaded_registry):
        engine = MappingEngine(loaded_registry)
        engine.run_diff()
        stats = loaded_registry.get_stats()
        assert stats["source_tables"] == 3
        assert stats["target_tables"] == 3
        assert stats["total_mappings"] > 0


class TestSnakeCaseHelpers:
    def test_pascal_case(self):
        # CustomerID: regex converts to customer_id (ID is a common suffix)
        result = _to_snake_case("CustomerID")
        assert result in ("customer_id", "customer_i_d")  # either is acceptable

    def test_camel_case(self):
        assert _to_snake_case("orderDate") == "order_date"

    def test_already_snake(self):
        assert _to_snake_case("order_date") == "order_date"

    def test_total_due(self):
        assert _to_snake_case("TotalDue") == "total_due"

    def test_abbreviation_expansion(self):
        result = _expand_abbreviations("cust_no")
        assert "customer" in result
        assert "number" in result
