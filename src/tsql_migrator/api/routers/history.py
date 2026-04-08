"""GET /api/history — translation history."""

from __future__ import annotations

from fastapi import APIRouter

from tsql_migrator.api.dependencies import RegistryDep
from tsql_migrator.api.models import HistoryItem

router = APIRouter(prefix="/history", tags=["history"])


@router.get("", response_model=list[HistoryItem])
async def get_history(registry: RegistryDep, limit: int = 20):
    """Return recent translation history."""
    rows = registry.get_recent_history(limit=limit)
    return [HistoryItem(**r) for r in rows]
