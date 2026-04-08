"""FastAPI dependency injection — pipeline, registry, settings."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from tsql_migrator.pipeline import MigrationPipeline
from tsql_migrator.schema.registry import SchemaRegistry


@lru_cache(maxsize=1)
def get_registry() -> SchemaRegistry:
    db_path = os.getenv("REGISTRY_DB_PATH", "./migrator.db")
    return SchemaRegistry(db_path=db_path)


@lru_cache(maxsize=1)
def get_pipeline() -> MigrationPipeline:
    registry = get_registry()
    llm_client = None
    if os.getenv("GEMINI_API_KEY"):
        try:
            from tsql_migrator.llm.client import LLMClient
            llm_client = LLMClient()
        except Exception:
            pass  # LLM optional — proceed without it
    return MigrationPipeline(schema_registry=registry, llm_client=llm_client)


RegistryDep = Annotated[SchemaRegistry, Depends(get_registry)]
PipelineDep = Annotated[MigrationPipeline, Depends(get_pipeline)]
