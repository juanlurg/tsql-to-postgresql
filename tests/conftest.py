"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from tsql_migrator.pipeline import MigrationPipeline
from tsql_migrator.transforms.base import TransformContext

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TSQL_DIR = FIXTURES_DIR / "tsql"
EXPECTED_DIR = FIXTURES_DIR / "expected"
DDL_DIR = FIXTURES_DIR / "ddl"


@pytest.fixture
def pipeline() -> MigrationPipeline:
    """A pipeline with no schema registry (syntax rewrites only)."""
    return MigrationPipeline()


@pytest.fixture
def ctx() -> TransformContext:
    """A fresh TransformContext."""
    return TransformContext()


def tsql_fixture(name: str) -> str:
    return (TSQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


def expected_fixture(name: str) -> str:
    path = EXPECTED_DIR / f"{name}.sql"
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""
