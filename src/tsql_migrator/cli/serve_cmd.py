"""tsql-migrator serve — start the API + web UI server."""

from __future__ import annotations

from typing import Annotated

import typer

serve_app = typer.Typer(help="Start the web server.")


@serve_app.callback(invoke_without_command=True)
def serve(
    host: Annotated[str, typer.Option("--host")] = "0.0.0.0",
    port: Annotated[int, typer.Option("--port", "-p")] = 8000,
    reload: Annotated[bool, typer.Option("--reload")] = False,
) -> None:
    """Start the T-SQL Migrator API server."""
    import uvicorn
    uvicorn.run(
        "tsql_migrator.api.main:app",
        host=host,
        port=port,
        reload=reload,
    )
