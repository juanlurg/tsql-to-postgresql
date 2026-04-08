"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from tsql_migrator.api.routers import history, schema, translate


def create_app() -> FastAPI:
    app = FastAPI(
        title="T-SQL → Redshift Migrator",
        description="Translate SQL Server queries to Amazon Redshift SQL",
        version="0.1.0",
    )

    # CORS — allow all origins in dev; tighten in production
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API routes
    app.include_router(translate.router, prefix="/api")
    app.include_router(schema.router, prefix="/api")
    app.include_router(history.router, prefix="/api")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # Serve React frontend if built (must be mounted LAST — catchall shadows routes registered after it)
    frontend_dist = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")

    return app


app = create_app()
