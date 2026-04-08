"""Schema registry endpoints."""

from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.orm import Session

from tsql_migrator.api.dependencies import RegistryDep
from tsql_migrator.api.models import (
    MappingItem,
    MappingPatchRequest,
    SchemaStatusResponse,
)
from tsql_migrator.schema.ddl_parser import load_ddl_file
from tsql_migrator.schema.mapping_engine import MappingEngine
from tsql_migrator.schema.models import ColumnMapping

router = APIRouter(prefix="/schema", tags=["schema"])


@router.post("/load")
async def load_schema(
    file: UploadFile,
    dialect: str,
    name: str,
    registry: RegistryDep,
):
    """Upload a DDL file and load it into the registry."""
    if dialect not in ("tsql", "redshift"):
        raise HTTPException(status_code=400, detail="dialect must be 'tsql' or 'redshift'")

    # Write to a temp file so the DDL parser can read it
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        count = load_ddl_file(tmp_path, source_name=name, dialect=dialect, registry=registry)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return {"tables_loaded": count, "source_name": name}


@router.post("/diff")
async def run_diff(
    registry: RegistryDep,
    source: str = "sqlserver",
    target: str = "redshift",
    llm_assist: bool = False,
):
    """
    Run the auto-mapping diff and return results.

    Set llm_assist=true to invoke the LLM for columns that deterministic tiers
    could not confidently match. Suggestions are stored with source='llm_suggested'
    and require human review before they are used in translations.
    """
    engine = MappingEngine(registry)
    try:
        rows = engine.run_diff(source_name=source, target_name=target, llm_assist=llm_assist)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "total": len(rows),
        "approved": sum(1 for r in rows if r.approved),
        "pending": sum(1 for r in rows if not r.approved),
        "llm_suggested": sum(1 for r in rows if r.source == "llm_suggested" and r.tgt_column),
    }


@router.get("/status", response_model=SchemaStatusResponse)
async def get_status(registry: RegistryDep):
    """Get schema registry coverage statistics."""
    return SchemaStatusResponse(**registry.get_stats())


@router.get("/mappings", response_model=list[MappingItem])
async def list_mappings(
    registry: RegistryDep,
    pending_only: bool = False,
    limit: int = 200,
):
    """List column mappings, optionally filtering to pending-review only."""
    with Session(registry._engine) as session:
        query = select(ColumnMapping)
        if pending_only:
            query = query.where(ColumnMapping.approved == False)  # noqa: E712
        query = query.limit(limit)
        rows = session.scalars(query).all()
        return [
            MappingItem(
                id=r.id,
                src_table_schema=r.src_table_schema,
                src_table_name=r.src_table_name,
                src_column_name=r.src_column_name,
                tgt_table_schema=r.tgt_table_schema,
                tgt_table_name=r.tgt_table_name,
                tgt_column_name=r.tgt_column_name,
                confidence=r.confidence,
                source=r.source,
                approved=r.approved,
                notes=r.notes,
            )
            for r in rows
        ]


@router.patch("/mappings/{mapping_id}", response_model=MappingItem)
async def update_mapping(
    mapping_id: int,
    patch: MappingPatchRequest,
    registry: RegistryDep,
):
    """Update a column mapping (approve, rename target column, add notes)."""
    from datetime import datetime, timezone
    with Session(registry._engine) as session:
        row = session.get(ColumnMapping, mapping_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")
        if patch.tgt_column_name is not None:
            row.tgt_column_name = patch.tgt_column_name
        if patch.approved is not None:
            row.approved = patch.approved
        if patch.notes is not None:
            row.notes = patch.notes
        row.updated_at = datetime.now(timezone.utc)
        session.commit()
        session.refresh(row)
        return MappingItem(
            id=row.id,
            src_table_schema=row.src_table_schema,
            src_table_name=row.src_table_name,
            src_column_name=row.src_column_name,
            tgt_table_schema=row.tgt_table_schema,
            tgt_table_name=row.tgt_table_name,
            tgt_column_name=row.tgt_column_name,
            confidence=row.confidence,
            source=row.source,
            approved=row.approved,
            notes=row.notes,
        )
