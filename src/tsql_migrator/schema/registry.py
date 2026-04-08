"""
SchemaRegistry: SQLite-backed storage for DDL catalogs and column mappings.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from tsql_migrator.schema.models import (
    Base,
    Column,
    ColumnMapping,
    SchemaSource,
    Table,
    TableMapping,
    TranslationHistory,
)


@dataclass
class ColumnMappingResult:
    tgt_column_name: str | None
    confidence: float
    approved: bool


@dataclass
class TableMappingResult:
    tgt_table_schema: str
    tgt_table_name: str
    confidence: float


class SchemaRegistry:
    """
    SQLite-backed registry for source/target DDL and column mappings.
    Thread-safe for reads; writes are serialized via SQLAlchemy sessions.
    """

    def __init__(self, db_path: str = "./migrator.db") -> None:
        self._engine = create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False},
        )
        Base.metadata.create_all(self._engine)

    # ── Source management ──────────────────────────────────────────────────

    def upsert_source(
        self,
        name: str,
        dialect: Literal["tsql", "redshift"],
        ddl_path: str | None = None,
    ) -> int:
        """Create or replace a schema source entry. Returns source ID."""
        with Session(self._engine) as session:
            source = session.scalar(select(SchemaSource).where(SchemaSource.name == name))
            now = datetime.now(timezone.utc)
            if source is None:
                source = SchemaSource(name=name, dialect=dialect, loaded_at=now, ddl_path=ddl_path)
                session.add(source)
            else:
                # Delete existing tables/columns (cascade) and reload
                for table in source.tables:
                    session.delete(table)
                source.dialect = dialect
                source.loaded_at = now
                source.ddl_path = ddl_path
            session.commit()
            session.refresh(source)
            return source.id

    def add_table(
        self,
        source_id: int,
        schema_name: str,
        table_name: str,
        columns: list[dict],  # [{name, data_type, is_nullable, ordinal}]
    ) -> int:
        """Add a table and its columns to a source. Returns table ID."""
        with Session(self._engine) as session:
            table = Table(
                source_id=source_id,
                schema_name=schema_name,
                table_name=table_name,
            )
            session.add(table)
            session.flush()
            for col in columns:
                session.add(Column(
                    table_id=table.id,
                    column_name=col["name"],
                    data_type=col["data_type"],
                    is_nullable=col.get("is_nullable", True),
                    ordinal=col["ordinal"],
                ))
            session.commit()
            return table.id

    # ── Mapping queries ────────────────────────────────────────────────────

    def get_table_mapping(
        self, src_schema: str, src_table: str
    ) -> TableMappingResult | None:
        """Look up the Redshift equivalent for a SQL Server table."""
        with Session(self._engine) as session:
            mapping = session.scalar(
                select(TableMapping).where(
                    TableMapping.src_table_schema == src_schema.upper(),
                    TableMapping.src_table_name.ilike(src_table),
                    TableMapping.approved == True,  # noqa: E712
                )
            )
            if mapping is None:
                return None
            return TableMappingResult(
                tgt_table_schema=mapping.tgt_table_schema,
                tgt_table_name=mapping.tgt_table_name,
                confidence=mapping.confidence,
            )

    def get_column_mapping(
        self, tgt_table_name: str, src_column_name: str
    ) -> tuple[str, float] | None | bool:
        """
        Look up column mapping for a given (table, column) pair.
        Returns:
            (new_col_name, confidence) if mapped and approved
            False if the table is known but the column has no mapping
            None if the table is not in the registry
        """
        with Session(self._engine) as session:
            # Check if any mapping exists for this table
            any_for_table = session.scalar(
                select(func.count()).select_from(ColumnMapping).where(
                    ColumnMapping.tgt_table_name.ilike(tgt_table_name)
                )
            )
            if not any_for_table:
                return None  # table not in registry

            mapping = session.scalar(
                select(ColumnMapping).where(
                    ColumnMapping.tgt_table_name.ilike(tgt_table_name),
                    ColumnMapping.src_column_name.ilike(src_column_name),
                    ColumnMapping.approved == True,  # noqa: E712
                )
            )
            if mapping is None:
                return False  # table known, column unmapped
            if mapping.tgt_column_name is None:
                return False  # explicitly unmapped

            return (mapping.tgt_column_name, mapping.confidence)

    def find_column_mapping_any_table(
        self, src_column_name: str
    ) -> list[tuple[str, float]]:
        """Find all approved mappings for a column name across all tables."""
        with Session(self._engine) as session:
            rows = session.scalars(
                select(ColumnMapping).where(
                    ColumnMapping.src_column_name.ilike(src_column_name),
                    ColumnMapping.approved == True,  # noqa: E712
                    ColumnMapping.tgt_column_name.isnot(None),
                )
            ).all()
            return [(r.tgt_column_name, r.confidence) for r in rows]

    def upsert_column_mapping(
        self,
        src_schema: str,
        src_table: str,
        src_col: str,
        tgt_schema: str,
        tgt_table: str,
        tgt_col: str | None,
        confidence: float,
        source: Literal["human", "auto_exact", "auto_fuzzy", "llm_suggested"],
        approved: bool = False,
        notes: str | None = None,
    ) -> None:
        """Insert or update a column mapping."""
        with Session(self._engine) as session:
            existing = session.scalar(
                select(ColumnMapping).where(
                    ColumnMapping.src_table_schema == src_schema.upper(),
                    ColumnMapping.src_table_name.ilike(src_table),
                    ColumnMapping.src_column_name.ilike(src_col),
                )
            )
            now = datetime.now(timezone.utc)
            if existing is None:
                session.add(ColumnMapping(
                    src_table_schema=src_schema.upper(),
                    src_table_name=src_table,
                    src_column_name=src_col,
                    tgt_table_schema=tgt_schema,
                    tgt_table_name=tgt_table,
                    tgt_column_name=tgt_col,
                    confidence=confidence,
                    source=source,
                    approved=approved,
                    created_at=now,
                    updated_at=now,
                ))
            else:
                existing.tgt_column_name = tgt_col
                existing.confidence = confidence
                existing.source = source
                existing.approved = approved
                existing.notes = notes
                existing.updated_at = now
            session.commit()

    def upsert_table_mapping(
        self,
        src_schema: str,
        src_table: str,
        tgt_schema: str,
        tgt_table: str,
        confidence: float,
        source: str = "auto_exact",
        approved: bool = False,
    ) -> None:
        """Insert or update a table mapping."""
        with Session(self._engine) as session:
            existing = session.scalar(
                select(TableMapping).where(
                    TableMapping.src_table_schema == src_schema.upper(),
                    TableMapping.src_table_name.ilike(src_table),
                )
            )
            now = datetime.now(timezone.utc)
            if existing is None:
                session.add(TableMapping(
                    src_table_schema=src_schema.upper(),
                    src_table_name=src_table,
                    tgt_table_schema=tgt_schema,
                    tgt_table_name=tgt_table,
                    confidence=confidence,
                    source=source,
                    approved=approved,
                    created_at=now,
                    updated_at=now,
                ))
            else:
                existing.tgt_table_schema = tgt_schema
                existing.tgt_table_name = tgt_table
                existing.confidence = confidence
                existing.approved = approved
                existing.updated_at = now
            session.commit()

    # ── History ───────────────────────────────────────────────────────────

    def save_translation(
        self,
        input_sql: str,
        output_sql: str,
        report_json: str,
        used_llm: bool = False,
    ) -> None:
        with Session(self._engine) as session:
            session.add(TranslationHistory(
                input_sql=input_sql,
                output_sql=output_sql,
                report_json=report_json,
                used_llm=used_llm,
                created_at=datetime.now(timezone.utc),
            ))
            session.commit()

    def get_recent_history(self, limit: int = 20) -> list[dict]:
        with Session(self._engine) as session:
            rows = session.scalars(
                select(TranslationHistory)
                .order_by(TranslationHistory.created_at.desc())
                .limit(limit)
            ).all()
            return [
                {
                    "id": r.id,
                    "input_sql": r.input_sql[:200],
                    "output_sql": r.output_sql[:200],
                    "used_llm": r.used_llm,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]

    # ── DDL reconstruction ────────────────────────────────────────────────

    def get_table_ddl_string(self, table_name: str, dialect: Literal["tsql", "redshift"]) -> str | None:
        """
        Reconstruct a CREATE TABLE DDL string from stored columns for use in LLM context.
        Returns None if the table is not found in any source of the given dialect.
        """
        with Session(self._engine) as session:
            table_row = session.scalar(
                select(Table)
                .join(SchemaSource)
                .where(
                    Table.table_name.ilike(table_name),
                    SchemaSource.dialect == dialect,
                )
                .order_by(Table.id.desc())
            )
            if table_row is None:
                return None

            cols = session.scalars(
                select(Column)
                .where(Column.table_id == table_row.id)
                .order_by(Column.ordinal)
            ).all()

            if not cols:
                return None

            schema_prefix = f"{table_row.schema_name}." if table_row.schema_name else ""
            col_lines = []
            for col in cols:
                nullable = "" if col.is_nullable else " NOT NULL"
                col_lines.append(f"  {col.column_name} {col.data_type}{nullable}")

            return (
                f"CREATE TABLE {schema_prefix}{table_row.table_name} (\n"
                + ",\n".join(col_lines)
                + "\n);"
            )

    # ── Stats ─────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        with Session(self._engine) as session:
            src_count = session.scalar(
                select(func.count()).select_from(Table)
                .join(SchemaSource)
                .where(SchemaSource.dialect == "tsql")
            ) or 0
            tgt_count = session.scalar(
                select(func.count()).select_from(Table)
                .join(SchemaSource)
                .where(SchemaSource.dialect == "redshift")
            ) or 0
            total_mappings = session.scalar(
                select(func.count()).select_from(ColumnMapping)
            ) or 0
            approved = session.scalar(
                select(func.count()).select_from(ColumnMapping)
                .where(ColumnMapping.approved == True)  # noqa: E712
            ) or 0
            unmapped = session.scalar(
                select(func.count()).select_from(ColumnMapping)
                .where(ColumnMapping.tgt_column_name.is_(None))
            ) or 0
            return {
                "source_tables": src_count,
                "target_tables": tgt_count,
                "total_mappings": total_mappings,
                "approved_mappings": approved,
                "pending_mappings": total_mappings - approved,
                "unmapped_columns": unmapped,
            }
