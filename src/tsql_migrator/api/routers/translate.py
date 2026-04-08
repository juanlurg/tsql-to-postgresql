"""POST /api/translate — translate T-SQL to Redshift SQL."""

from __future__ import annotations

import json

from fastapi import APIRouter

from tsql_migrator.api.dependencies import PipelineDep, RegistryDep
from tsql_migrator.api.models import (
    AnnotationItem,
    TransformationReport,
    TranslateRequest,
    TranslateResponse,
)

router = APIRouter(prefix="/translate", tags=["translate"])


@router.post("", response_model=TranslateResponse)
async def translate(request: TranslateRequest, pipeline: PipelineDep, registry: RegistryDep):
    """Translate a T-SQL query to Redshift SQL."""
    result = pipeline.translate(request.sql)

    annotations = [
        AnnotationItem(
            line=a.line,
            message=a.message,
            severity=a.severity,
        )
        for a in result.report.annotations
    ]

    report = TransformationReport(
        success=result.report.success,
        annotations=annotations,
        hard_errors=result.report.hard_errors,
        renames_applied=result.report.renames_applied,
        udf_blocks_count=result.report.udf_blocks_count,
        used_llm=result.report.used_llm,
        llm_confidence=result.report.llm_confidence,
    )

    # Persist to history
    try:
        registry.save_translation(
            input_sql=request.sql,
            output_sql=result.output_sql,
            report_json=json.dumps(report.model_dump()),
            used_llm=result.used_llm,
        )
    except Exception:
        pass  # history is non-critical

    return TranslateResponse(output_sql=result.output_sql, report=report)
