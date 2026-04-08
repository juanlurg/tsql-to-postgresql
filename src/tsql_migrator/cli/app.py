"""
tsql-migrator CLI — root app.
"""

import typer

from tsql_migrator.cli.translate_cmd import translate_app
from tsql_migrator.cli.schema_cmd import schema_app
from tsql_migrator.cli.serve_cmd import serve_app

app = typer.Typer(
    name="tsql-migrator",
    help="Translate T-SQL (SQL Server) queries to Amazon Redshift SQL.",
    no_args_is_help=True,
)

app.add_typer(translate_app, name="translate")
app.add_typer(schema_app, name="schema")
app.add_typer(serve_app, name="serve")


@app.command()
def version() -> None:
    """Show version information."""
    from importlib.metadata import version, PackageNotFoundError
    try:
        v = version("tsql-migrator")
    except PackageNotFoundError:
        v = "dev"
    typer.echo(f"tsql-migrator {v}")
