"""Pydantic request/response models for the REST API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ── Translation ────────────────────────────────────────────────────────────

class TranslateRequest(BaseModel):
    sql: str = Field(..., description="T-SQL query to translate")
    schema_name: str | None = Field(
        default=None,
        description="Schema registry name to use for column/table mapping",
    )
    enable_llm: bool = Field(
        default=True,
        description="Allow LLM fallback for complex constructs (PIVOT, etc.)",
    )
    target_context: Literal["query-editor", "quicksight", "power-bi", "tableau"] = Field(
        default="query-editor",
        description="Target execution environment (affects output constraints)",
    )


class AnnotationItem(BaseModel):
    line: int | None
    message: str
    severity: Literal["info", "warn", "error"]


class TransformationReport(BaseModel):
    success: bool
    annotations: list[AnnotationItem]
    hard_errors: list[str]
    renames_applied: int
    udf_blocks_count: int
    used_llm: bool
    llm_confidence: float | None


class TranslateResponse(BaseModel):
    output_sql: str
    report: TransformationReport


# ── Schema Registry ────────────────────────────────────────────────────────

class SchemaStatusResponse(BaseModel):
    source_tables: int
    target_tables: int
    total_mappings: int
    approved_mappings: int
    pending_mappings: int
    unmapped_columns: int


class MappingItem(BaseModel):
    id: int
    src_table_schema: str
    src_table_name: str
    src_column_name: str
    tgt_table_schema: str
    tgt_table_name: str
    tgt_column_name: str | None
    confidence: float
    source: str
    approved: bool
    notes: str | None


class MappingPatchRequest(BaseModel):
    tgt_column_name: str | None = None
    approved: bool | None = None
    notes: str | None = None


# ── Table Mappings ────────────────────────────────────────────────────────

class TableItem(BaseModel):
    schema_name: str
    table_name: str


class SourceTableItem(BaseModel):
    schema_name: str
    table_name: str
    mapped: bool
    tgt_schema: str | None
    tgt_table: str | None


class TableMappingItem(BaseModel):
    id: int
    src_table_schema: str
    src_table_name: str
    tgt_table_schema: str
    tgt_table_name: str
    confidence: float
    source: str
    approved: bool


class TableMappingCreateRequest(BaseModel):
    src_schema: str
    src_table: str
    tgt_schema: str
    tgt_table: str


# ── History ───────────────────────────────────────────────────────────────

class HistoryItem(BaseModel):
    id: int
    input_sql: str
    output_sql: str
    used_llm: bool
    created_at: str
