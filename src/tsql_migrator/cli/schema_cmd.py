"""
tsql-migrator schema — manage the DDL schema registry.

Commands:
    load-source   Load a SQL Server DDL file
    load-target   Load a Redshift DDL file
    diff          Auto-generate column mappings and export to CSV for review
    import-map    Import a reviewed CSV and mark mappings as approved
    status        Show schema coverage statistics
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

schema_app = typer.Typer(help="Manage the DDL schema registry and column mappings.")
console = Console()
err_console = Console(stderr=True)


def _get_registry(db_path: str):
    from tsql_migrator.schema.registry import SchemaRegistry
    return SchemaRegistry(db_path=db_path)


@schema_app.command("load-source")
def load_source(
    file: Annotated[Path, typer.Option("--file", "-f", help="SQL Server DDL file")],
    name: Annotated[str, typer.Option("--name", "-n", help="Source name")] = "sqlserver",
    db_path: Annotated[str, typer.Option("--db")] = "./migrator.db",
) -> None:
    """Load a SQL Server DDL file into the registry."""
    registry = _get_registry(db_path)
    from tsql_migrator.schema.ddl_parser import load_ddl_file
    count = load_ddl_file(str(file), source_name=name, dialect="tsql", registry=registry)
    console.print(f"[green]✓[/green] Loaded {count} table(s) from [bold]{file}[/bold] as source '{name}'")


@schema_app.command("load-target")
def load_target(
    file: Annotated[Path, typer.Option("--file", "-f", help="Redshift DDL file")],
    name: Annotated[str, typer.Option("--name", "-n", help="Target name")] = "redshift",
    db_path: Annotated[str, typer.Option("--db")] = "./migrator.db",
) -> None:
    """Load a Redshift DDL file into the registry."""
    registry = _get_registry(db_path)
    from tsql_migrator.schema.ddl_parser import load_ddl_file
    count = load_ddl_file(str(file), source_name=name, dialect="redshift", registry=registry)
    console.print(f"[green]✓[/green] Loaded {count} table(s) from [bold]{file}[/bold] as target '{name}'")


@schema_app.command("diff")
def diff(
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output CSV file for review"),
    ] = Path("mappings_review.csv"),
    source: Annotated[str, typer.Option("--source")] = "sqlserver",
    target: Annotated[str, typer.Option("--target")] = "redshift",
    db_path: Annotated[str, typer.Option("--db")] = "./migrator.db",
    llm_assist: Annotated[
        bool,
        typer.Option(
            "--llm-assist/--no-llm-assist",
            help=(
                "Use the LLM to suggest mappings for (1) source tables that "
                "deterministic matching could not map to a target table, and "
                "(2) columns within matched tables that scored below 0.60 confidence. "
                "All LLM suggestions are stored with source='llm_suggested' and "
                "require human review before use."
            ),
        ),
    ] = False,
) -> None:
    """Auto-generate column mappings and export to CSV for human review."""
    registry = _get_registry(db_path)
    from tsql_migrator.schema.mapping_engine import MappingEngine
    engine = MappingEngine(registry)
    results = engine.run_diff(source_name=source, target_name=target, llm_assist=llm_assist)
    engine.export_csv(results, str(output))
    pending = sum(1 for r in results if not r.approved)
    llm_suggested = sum(1 for r in results if r.source == "llm_suggested" and r.tgt_column)
    summary = (
        f"[green]✓[/green] Generated {len(results)} mapping(s) "
        f"([yellow]{pending} pending review[/yellow]"
    )
    if llm_suggested:
        summary += f", [cyan]{llm_suggested} LLM-suggested[/cyan]"
    summary += f") → [bold]{output}[/bold]"
    console.print(summary)
    console.print(
        "Review and edit the CSV, then run: "
        f"[dim]tsql-migrator schema import-map --file {output}[/dim]"
    )


@schema_app.command("import-map")
def import_map(
    file: Annotated[Path, typer.Option("--file", "-f", help="Reviewed CSV file")],
    db_path: Annotated[str, typer.Option("--db")] = "./migrator.db",
) -> None:
    """Import a reviewed CSV and mark mappings as approved."""
    registry = _get_registry(db_path)
    from tsql_migrator.schema.mapping_engine import MappingEngine
    engine = MappingEngine(registry)
    count = engine.import_csv(str(file))
    console.print(f"[green]✓[/green] Imported and approved {count} mapping(s).")


@schema_app.command("status")
def status(
    db_path: Annotated[str, typer.Option("--db")] = "./migrator.db",
) -> None:
    """Show schema coverage statistics."""
    registry = _get_registry(db_path)
    stats = registry.get_stats()

    table = Table(title="Schema Registry Status")
    table.add_column("Metric")
    table.add_column("Value", justify="right")

    table.add_row("Source tables", str(stats.get("source_tables", 0)))
    table.add_row("Target tables", str(stats.get("target_tables", 0)))
    table.add_row("Total mappings", str(stats.get("total_mappings", 0)))
    table.add_row("Approved mappings", str(stats.get("approved_mappings", 0)))
    table.add_row("Pending review", str(stats.get("pending_mappings", 0)))
    table.add_row("Unmapped columns", str(stats.get("unmapped_columns", 0)))

    console.print(table)
