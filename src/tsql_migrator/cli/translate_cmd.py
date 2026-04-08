"""
tsql-migrator translate — translate a T-SQL query to Redshift SQL.

Usage:
    # From stdin
    echo "SELECT TOP 10 * FROM dbo.Orders WITH(NOLOCK)" | tsql-migrator translate

    # From file
    tsql-migrator translate --input query.sql --output query_rs.sql

    # Show full report
    tsql-migrator translate --input query.sql --report
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

translate_app = typer.Typer(help="Translate T-SQL to Redshift SQL.")
console = Console()
err_console = Console(stderr=True)


@translate_app.callback(invoke_without_command=True)
def translate(
    input: Annotated[
        Path | None,
        typer.Option("--input", "-i", help="Input .sql file (default: stdin)"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output .sql file (default: stdout)"),
    ] = None,
    schema_name: Annotated[
        str | None,
        typer.Option("--schema", "-s", help="Schema registry name to use for column mapping"),
    ] = None,
    report: Annotated[
        bool,
        typer.Option("--report", "-r", help="Print transformation report"),
    ] = False,
    db_path: Annotated[
        str,
        typer.Option("--db", help="Path to schema registry SQLite database"),
    ] = "./migrator.db",
) -> None:
    """Translate T-SQL to Redshift SQL."""
    # Read input
    if input is not None:
        sql = input.read_text(encoding="utf-8")
    elif not sys.stdin.isatty():
        sql = sys.stdin.read()
    else:
        err_console.print("[red]Error:[/red] provide --input or pipe SQL via stdin.")
        raise typer.Exit(1)

    if not sql.strip():
        err_console.print("[red]Error:[/red] empty input.")
        raise typer.Exit(1)

    # Build pipeline
    schema_registry = None
    if schema_name:
        try:
            from tsql_migrator.schema.registry import SchemaRegistry
            schema_registry = SchemaRegistry(db_path=db_path)
        except Exception as e:
            err_console.print(f"[yellow]Warning:[/yellow] could not load schema registry: {e}")

    from tsql_migrator.pipeline import MigrationPipeline
    pipeline = MigrationPipeline(schema_registry=schema_registry)

    result = pipeline.translate(sql)

    # Output SQL
    if result.error and not result.output_sql:
        err_console.print(
            Panel(
                f"[red]{result.error}[/red]",
                title="[bold red]Migration Error[/bold red]",
            )
        )
        raise typer.Exit(1)

    if output is not None:
        output.write_text(result.output_sql, encoding="utf-8")
        console.print(f"[green]✓[/green] Output written to {output}")
    else:
        # Print to stdout (no rich markup so it can be piped)
        print(result.output_sql)

    # Print report if requested
    if report:
        _print_report(result.report)

    # Exit with non-zero if there were hard errors
    if result.report.hard_errors:
        raise typer.Exit(1)


def _print_report(report) -> None:
    if report.hard_errors:
        for err in report.hard_errors:
            err_console.print(f"[bold red]HARD ERROR:[/bold red] {err}")

    if report.annotations:
        table = Table(title="Transformation Report", show_header=True)
        table.add_column("Line", style="dim", width=6)
        table.add_column("Severity", width=10)
        table.add_column("Message")

        severity_styles = {"info": "cyan", "warn": "yellow", "error": "red"}
        for ann in report.annotations:
            line_str = str(ann.line) if ann.line else "—"
            style = severity_styles.get(ann.severity, "white")
            table.add_row(line_str, f"[{style}]{ann.severity.upper()}[/{style}]", ann.message)

        console.print(table)

    summary_parts = []
    if report.renames_applied:
        summary_parts.append(f"{report.renames_applied} column(s) renamed")
    if report.udf_blocks_count:
        summary_parts.append(f"{report.udf_blocks_count} UDF(s) generated")
    if report.used_llm:
        summary_parts.append("LLM used")

    if summary_parts:
        console.print(f"[dim]Summary: {', '.join(summary_parts)}[/dim]")
