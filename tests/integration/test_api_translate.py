"""Integration tests for the FastAPI translate endpoint."""

from __future__ import annotations

import pytest
from httpx import AsyncClient, ASGITransport

from tsql_migrator.api.main import create_app
from tsql_migrator.api.dependencies import get_pipeline, get_registry
from tsql_migrator.pipeline import MigrationPipeline
from tsql_migrator.schema.registry import SchemaRegistry


@pytest.fixture
def fresh_app(tmp_path):
    """Create a test app with an isolated in-memory registry."""
    app = create_app()

    # Override dependencies with isolated instances
    registry = SchemaRegistry(db_path=str(tmp_path / "test.db"))
    pipeline = MigrationPipeline(schema_registry=registry)

    app.dependency_overrides[get_registry] = lambda: registry
    app.dependency_overrides[get_pipeline] = lambda: pipeline

    return app


@pytest.fixture
async def client(fresh_app):
    async with AsyncClient(
        transport=ASGITransport(app=fresh_app), base_url="http://test"
    ) as c:
        yield c


class TestTranslateEndpoint:
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_translate_basic(self, client):
        resp = await client.post("/api/translate", json={
            "sql": "SELECT TOP 10 CustomerID FROM dbo.Orders WITH(NOLOCK)"
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "LIMIT 10" in data["output_sql"]
        assert "NOLOCK" not in data["output_sql"]
        assert data["report"]["success"] is True

    async def test_translate_isnull(self, client):
        resp = await client.post("/api/translate", json={
            "sql": "SELECT ISNULL(col, 0) AS val FROM dbo.t"
        })
        assert resp.status_code == 200
        assert "COALESCE" in resp.json()["output_sql"]

    async def test_translate_hard_error(self, client):
        resp = await client.post("/api/translate", json={
            "sql": "EXEC sp_helptext 'dbo.Orders'"
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["report"]["hard_errors"]
        assert data["report"]["success"] is False

    async def test_translate_returns_annotations(self, client):
        resp = await client.post("/api/translate", json={
            "sql": "SELECT GETDATE() AS now FROM dbo.t"
        })
        assert resp.status_code == 200
        annotations = resp.json()["report"]["annotations"]
        assert any("utc" in a["message"].lower() or "UTC" in a["message"] for a in annotations)

    async def test_schema_status(self, client):
        resp = await client.get("/api/schema/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_mappings" in data

    async def test_history_empty(self, client):
        resp = await client.get("/api/history")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_history_populated_after_translate(self, client):
        await client.post("/api/translate", json={"sql": "SELECT 1 AS n"})
        resp = await client.get("/api/history?limit=1")
        assert resp.status_code == 200
        history = resp.json()
        assert len(history) >= 1
