"""
Annotator: inject inline SQL comments and build the TransformationReport.

After all transform passes run, this module:
- Prepends UDF blocks (from TRY_CAST rewrites)
- Injects -- MIGRATION_TODO comments on affected lines
- Marks unmapped columns with /* UNMAPPED: ColName */ placeholders
- Returns the annotated SQL string + a structured TransformationReport
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

from tsql_migrator.transforms.base import Annotation, Severity


@dataclass
class AnnotationItem:
    line: int | None
    message: str
    severity: Literal["info", "warn", "error"]


@dataclass
class TransformationReport:
    success: bool
    annotations: list[AnnotationItem] = field(default_factory=list)
    hard_errors: list[str] = field(default_factory=list)
    renames_applied: int = 0
    udf_blocks_count: int = 0
    used_llm: bool = False
    llm_confidence: float | None = None
    original_sql: str = ""
    output_sql: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def annotate(
    sql: str,
    annotations: list[Annotation],
    udf_blocks: list[str],
    renames_applied: int = 0,
    used_llm: bool = False,
    llm_confidence: float | None = None,
    original_sql: str = "",
    hard_errors: list[str] | None = None,
) -> tuple[str, TransformationReport]:
    """
    Inject annotations as inline SQL comments and build a TransformationReport.

    Returns:
        (annotated_sql, report)
    """
    lines = sql.splitlines()
    hard_errors = hard_errors or []

    # Build a mapping from 1-based line number → list of messages
    line_annotations: dict[int, list[tuple[Severity, str]]] = {}
    report_items: list[AnnotationItem] = []

    for ann in annotations:
        line_num = ann.line  # may be None if position unknown
        sev_str: Literal["info", "warn", "error"] = {
            Severity.INFO: "info",
            Severity.WARN: "warn",
            Severity.ERROR: "error",
        }[ann.severity]
        report_items.append(AnnotationItem(line=line_num, message=ann.message, severity=sev_str))

        if line_num is not None and 1 <= line_num <= len(lines):
            line_annotations.setdefault(line_num, []).append((ann.severity, ann.message))

    # Inject comments — walk lines in reverse so line numbers stay valid
    annotated_lines = list(lines)
    for line_num in sorted(line_annotations.keys(), reverse=True):
        msgs = line_annotations[line_num]
        comment_lines = []
        for severity, msg in msgs:
            prefix = {
                Severity.INFO: "-- INFO",
                Severity.WARN: "-- MIGRATION_TODO",
                Severity.ERROR: "-- MIGRATION_TODO (ERROR)",
            }[severity]
            comment_lines.append(f"{prefix}: {msg}")
        # Insert comment lines before the affected line (0-based index)
        insert_at = line_num - 1
        for comment in reversed(comment_lines):
            annotated_lines.insert(insert_at, comment)

    annotated_sql = "\n".join(annotated_lines)

    # Prepend UDF blocks if any
    if udf_blocks:
        udf_section = "\n\n".join(udf_blocks)
        annotated_sql = (
            "-- === Generated UDF prerequisites (required before running below) ===\n"
            + udf_section
            + "\n\n-- === Translated Query ===\n"
            + annotated_sql
        )

    success = not hard_errors and not any(
        a.severity == Severity.ERROR for a in annotations
    )

    report = TransformationReport(
        success=success,
        annotations=report_items,
        hard_errors=hard_errors,
        renames_applied=renames_applied,
        udf_blocks_count=len(udf_blocks),
        used_llm=used_llm,
        llm_confidence=llm_confidence,
        original_sql=original_sql,
        output_sql=annotated_sql,
    )

    return annotated_sql, report
