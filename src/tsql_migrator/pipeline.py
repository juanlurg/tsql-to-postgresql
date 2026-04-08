"""
Pipeline: orchestrates preprocessing → parsing → transforms → generation → annotation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tsql_migrator.annotator import TransformationReport, annotate
from tsql_migrator.errors import HardError, ParseError
from tsql_migrator.generator import generate_redshift_statements
from tsql_migrator.parser import parse_tsql
from tsql_migrator.preprocessor import preprocess
from tsql_migrator.transforms.base import TransformContext
from tsql_migrator.transforms.column_renamer import ColumnRenamer
from tsql_migrator.transforms.datatype_converter import DataTypeConverter
from tsql_migrator.transforms.function_rewriter import FunctionRewriter
from tsql_migrator.transforms.hint_stripper import HintStripper
from tsql_migrator.transforms.syntax_rewriter import SyntaxRewriter
from tsql_migrator.transforms.table_renamer import TableRenamer


@dataclass
class TranslationResult:
    output_sql: str
    report: TransformationReport
    used_llm: bool = False
    udf_blocks: list[str] = field(default_factory=list)
    error: str | None = None  # set if pipeline failed entirely


class MigrationPipeline:
    """
    Stateless translation pipeline. Construct once, call translate() many times.

    Pass order (must not be changed):
    1. HintStripper      — remove noise before other passes inspect nodes
    2. DataTypeConverter — clean type nodes early for subsequent passes
    3. TableRenamer      — resolve table names before column lookup
    4. ColumnRenamer     — requires table context (must come after TableRenamer)
    5. FunctionRewriter  — function nodes before structural rewrites
    6. SyntaxRewriter    — structural changes (TOP→LIMIT, APPLY→LATERAL, etc.)
    """

    _PASS_CLASSES = [
        HintStripper,
        DataTypeConverter,
        TableRenamer,
        ColumnRenamer,
        FunctionRewriter,
        SyntaxRewriter,
    ]

    def __init__(
        self,
        schema_registry=None,  # tsql_migrator.schema.registry.SchemaRegistry | None
        llm_client=None,       # tsql_migrator.llm.client.LLMClient | None
        rule_registry=None,    # tsql_migrator.rules.registry.RuleRegistry | None
        enable_validator: bool = False,
        redshift_conn_str: str | None = None,
    ) -> None:
        self.schema_registry = schema_registry
        self.llm_client = llm_client
        self.enable_validator = enable_validator
        self.redshift_conn_str = redshift_conn_str

        # Build passes — inject dependencies lazily
        if rule_registry is None:
            from tsql_migrator.rules.registry import RuleRegistry
            rule_registry = RuleRegistry.load_defaults()
        self.rule_registry = rule_registry

        self._passes = [cls() for cls in self._PASS_CLASSES]
        # Inject rule_registry into the function rewriter
        for p in self._passes:
            if isinstance(p, FunctionRewriter):
                p.rule_registry = self.rule_registry

    def translate(self, tsql: str) -> TranslationResult:
        """
        Translate a T-SQL string to Redshift SQL.

        Returns a TranslationResult. Never raises — errors are captured
        in the result's report.
        """
        try:
            pre = preprocess(tsql)
        except HardError as e:
            return self._hard_error_result(tsql, str(e), e.construct)
        except ValueError as e:
            return self._hard_error_result(tsql, str(e), "EMPTY_INPUT")

        all_statements = []
        ctx = TransformContext(schema_registry=self.schema_registry)

        for batch in pre.batches:
            try:
                statements = parse_tsql(batch)
            except HardError as e:
                return self._hard_error_result(tsql, str(e), e.construct)
            except ParseError as e:
                # Attempt LLM fallback if available
                if self.llm_client is not None:
                    return self._llm_fallback(tsql, str(e))
                return self._hard_error_result(
                    tsql,
                    f"Parse error (no LLM fallback configured): {e}",
                    "PARSE_ERROR",
                )

            # Apply transform passes to each statement
            for stmt in statements:
                transformed = stmt
                for pass_ in self._passes:
                    try:
                        transformed = pass_.transform(transformed, ctx)
                    except HardError as e:
                        return self._hard_error_result(tsql, str(e), e.construct)
                all_statements.append(transformed)

        # If LLM candidates exist (e.g., PIVOT), attempt LLM rewrite of full query
        if ctx.llm_candidates and self.llm_client is not None:
            return self._llm_rewrite_with_candidates(tsql, ctx, all_statements)

        sql_out = generate_redshift_statements(all_statements)

        annotated_sql, report = annotate(
            sql=sql_out,
            annotations=ctx.annotations,
            udf_blocks=ctx.udf_blocks,
            renames_applied=ctx.renames_applied,
            used_llm=False,
            original_sql=tsql,
        )

        # Optional EXPLAIN validation
        if self.enable_validator and self.redshift_conn_str:
            from tsql_migrator.validator import validate_on_redshift
            vresult = validate_on_redshift(annotated_sql, self.redshift_conn_str)
            if not vresult.success:
                from tsql_migrator.transforms.base import Annotation, Severity
                ctx.annotations.append(
                    Annotation(
                        line=None,
                        message=f"Redshift EXPLAIN failed: {vresult.error_message}",
                        severity=Severity.ERROR,
                    )
                )

        return TranslationResult(
            output_sql=annotated_sql,
            report=report,
            used_llm=False,
            udf_blocks=ctx.udf_blocks,
        )

    def _hard_error_result(
        self, original_sql: str, message: str, construct: str
    ) -> TranslationResult:
        from tsql_migrator.annotator import TransformationReport
        report = TransformationReport(
            success=False,
            hard_errors=[f"[{construct}] {message}"],
            original_sql=original_sql,
            output_sql="",
        )
        return TranslationResult(output_sql="", report=report, error=message)

    def _llm_fallback(self, original_sql: str, parse_error: str) -> TranslationResult:
        """Translate SQL that failed deterministic parse via LLM."""
        from tsql_migrator.errors import LLMError
        from tsql_migrator.llm.validator import validate_llm_output

        ddl_context = self._build_ddl_context_from_sql(original_sql)
        try:
            result = self.llm_client.translate(
                tsql=original_sql,
                ddl_context=ddl_context,
                error_context=parse_error,
            )
        except LLMError as e:
            return self._hard_error_result(original_sql, str(e), "LLM_ERROR")

        referenced_tables = self._extract_table_names(original_sql)
        validation = validate_llm_output(
            result.translated_sql,
            registry=self.schema_registry,
            referenced_tables=referenced_tables,
        )

        if not validation.valid:
            msg = (
                validation.parse_error
                or f"Hallucinated columns: {', '.join(validation.hallucinated_columns)}"
            )
            return self._hard_error_result(
                original_sql, f"LLM output failed validation: {msg}", "LLM_VALIDATION_ERROR"
            )

        return self._build_llm_result(original_sql, result, validation, renames_applied=0)

    def _llm_rewrite_with_candidates(
        self,
        original_sql: str,
        ctx,
        all_statements: list,
    ) -> TranslationResult:
        """Handle PIVOT/UNPIVOT and other complex constructs via LLM full-query rewrite."""
        from tsql_migrator.errors import LLMError
        from tsql_migrator.llm.validator import validate_llm_output
        from tsql_migrator.transforms.base import Annotation, Severity

        ddl_context = self._build_ddl_context_from_sql(original_sql)
        try:
            result = self.llm_client.translate(tsql=original_sql, ddl_context=ddl_context)
        except LLMError as e:
            ctx.annotations.append(
                Annotation(
                    line=None,
                    message=f"LLM rewrite failed: {e}. PIVOT/complex constructs need manual review.",
                    severity=Severity.WARN,
                )
            )
            return self._finish_deterministic(original_sql, ctx, all_statements)

        referenced_tables = self._extract_table_names(original_sql)
        validation = validate_llm_output(
            result.translated_sql,
            registry=self.schema_registry,
            referenced_tables=referenced_tables,
        )

        if not validation.valid:
            msg = (
                validation.parse_error
                or f"Hallucinated columns: {', '.join(validation.hallucinated_columns)}"
            )
            ctx.annotations.append(
                Annotation(
                    line=None,
                    message=f"LLM output failed validation ({msg}). PIVOT constructs need manual review.",
                    severity=Severity.WARN,
                )
            )
            return self._finish_deterministic(original_sql, ctx, all_statements)

        return self._build_llm_result(
            original_sql, result, validation, renames_applied=ctx.renames_applied,
            extra_annotations=ctx.annotations, udf_blocks=ctx.udf_blocks,
        )

    def _finish_deterministic(self, original_sql: str, ctx, all_statements: list) -> TranslationResult:
        """Generate output from already-transformed statements (fallback path)."""
        sql_out = generate_redshift_statements(all_statements)
        annotated_sql, report = annotate(
            sql=sql_out,
            annotations=ctx.annotations,
            udf_blocks=ctx.udf_blocks,
            renames_applied=ctx.renames_applied,
            used_llm=False,
            original_sql=original_sql,
        )
        return TranslationResult(
            output_sql=annotated_sql,
            report=report,
            used_llm=False,
            udf_blocks=ctx.udf_blocks,
        )

    def _build_llm_result(
        self,
        original_sql: str,
        llm_result,
        validation,
        renames_applied: int = 0,
        extra_annotations=None,
        udf_blocks: list[str] | None = None,
    ) -> TranslationResult:
        """Build a TranslationResult from a validated LLM output."""
        from tsql_migrator.annotator import AnnotationItem, TransformationReport
        from tsql_migrator.transforms.base import Annotation

        annotations: list[AnnotationItem] = []
        # Carry over any deterministic-pass annotations
        if extra_annotations:
            for a in extra_annotations:
                if isinstance(a, Annotation):
                    annotations.append(AnnotationItem(line=a.line, message=a.message, severity=a.severity.value))
                else:
                    annotations.append(a)
        for w in validation.warnings:
            annotations.append(AnnotationItem(line=None, message=w, severity="warn"))
        for todo in llm_result.migration_todos:
            annotations.append(AnnotationItem(line=None, message=todo, severity="warn"))

        confidence_map = {"high": 0.9, "medium": 0.6, "low": 0.3}
        llm_confidence = confidence_map.get(llm_result.confidence, 0.6)

        report = TransformationReport(
            success=True,
            annotations=annotations,
            renames_applied=renames_applied,
            used_llm=True,
            llm_confidence=llm_confidence,
            original_sql=original_sql,
            output_sql=llm_result.translated_sql,
        )
        return TranslationResult(
            output_sql=llm_result.translated_sql,
            report=report,
            used_llm=True,
            udf_blocks=udf_blocks or [],
        )

    def _build_ddl_context_from_sql(self, sql: str) -> str | None:
        """Extract table references from SQL and build a DDL context string for the LLM prompt."""
        if self.schema_registry is None:
            return None
        try:
            table_names = self._extract_table_names(sql)
        except Exception:
            return None
        if not table_names:
            return None

        from tsql_migrator.llm.prompts import build_ddl_context

        table_ddls = []
        for name in table_names:
            src_ddl = self.schema_registry.get_table_ddl_string(name, dialect="tsql")
            tgt_ddl = self.schema_registry.get_table_ddl_string(name, dialect="redshift")
            if src_ddl or tgt_ddl:
                table_ddls.append({
                    "table": name,
                    "src_ddl": src_ddl or "-- (not available)",
                    "tgt_ddl": tgt_ddl or "-- (not available)",
                })
        return build_ddl_context(table_ddls) if table_ddls else None

    def _extract_table_names(self, sql: str) -> list[str]:
        """Extract distinct table names from SQL (best-effort; ignores temp tables)."""
        import sqlglot
        import sqlglot.expressions as exp

        try:
            stmts = sqlglot.parse(sql, dialect="tsql")
            names: set[str] = set()
            for stmt in stmts:
                if stmt is None:
                    continue
                for tbl in stmt.find_all(exp.Table):
                    if tbl.name and not tbl.name.startswith("#"):
                        names.add(tbl.name)
            return list(names)
        except Exception:
            return []
