"""SQLAlchemy ORM models for the schema registry SQLite database."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class SchemaSource(Base):
    __tablename__ = "schema_sources"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(unique=True)
    dialect: Mapped[str]           # 'tsql' | 'redshift'
    loaded_at: Mapped[datetime]
    ddl_path: Mapped[str | None]

    tables: Mapped[list[Table]] = relationship(
        back_populates="source", cascade="all, delete-orphan"
    )


class Table(Base):
    __tablename__ = "tables"
    __table_args__ = (UniqueConstraint("source_id", "schema_name", "table_name"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("schema_sources.id", ondelete="CASCADE"))
    schema_name: Mapped[str]
    table_name: Mapped[str]

    source: Mapped[SchemaSource] = relationship(back_populates="tables")
    columns: Mapped[list[Column]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )


class Column(Base):
    __tablename__ = "columns"
    __table_args__ = (UniqueConstraint("table_id", "column_name"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"))
    column_name: Mapped[str]
    data_type: Mapped[str]
    is_nullable: Mapped[bool] = mapped_column(default=True)
    ordinal: Mapped[int]

    table: Mapped[Table] = relationship(back_populates="columns")


class ColumnMapping(Base):
    __tablename__ = "column_mappings"
    __table_args__ = (
        UniqueConstraint("src_table_schema", "src_table_name", "src_column_name"),
        Index("idx_column_mappings_src", "src_table_name", "src_column_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    src_table_schema: Mapped[str]
    src_table_name: Mapped[str]
    src_column_name: Mapped[str]
    tgt_table_schema: Mapped[str]
    tgt_table_name: Mapped[str]
    tgt_column_name: Mapped[str | None]   # None = unmapped
    confidence: Mapped[float] = mapped_column(default=1.0)
    source: Mapped[str]                   # 'human' | 'auto_exact' | 'auto_fuzzy'
    approved: Mapped[bool] = mapped_column(default=False)
    notes: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]


class TableMapping(Base):
    __tablename__ = "table_mappings"
    __table_args__ = (
        UniqueConstraint("src_table_schema", "src_table_name"),
        Index("idx_table_mappings_src", "src_table_schema", "src_table_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    src_table_schema: Mapped[str]
    src_table_name: Mapped[str]
    tgt_table_schema: Mapped[str]
    tgt_table_name: Mapped[str]
    confidence: Mapped[float] = mapped_column(default=1.0)
    source: Mapped[str] = mapped_column(default="auto_exact")
    approved: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]


class TranslationHistory(Base):
    __tablename__ = "translation_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    input_sql: Mapped[str]
    output_sql: Mapped[str]
    report_json: Mapped[str]
    used_llm: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime]
