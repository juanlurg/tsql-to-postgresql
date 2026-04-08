"""
MappingEngine: auto-generate column mappings between source and target DDL.

Confidence-tiered matching algorithm:
1. Exact normalized match (lowercase):   confidence 0.95, source='auto_exact'
2. Snake-case normalization match:        confidence 0.90, source='auto_exact'
3. Abbreviation expansion match:          confidence 0.80, source='auto_fuzzy'
4. Type-constrained Levenshtein match:    confidence 0.40-0.70, source='auto_fuzzy'
5. LLM suggestion (opt-in):              confidence 0.65, source='llm_suggested'
6. No match:                              tgt_column_name=None, approved=False
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from tsql_migrator.schema.models import Column, ColumnMapping, SchemaSource, Table

if TYPE_CHECKING:
    from tsql_migrator.schema.registry import SchemaRegistry

# Abbreviation expansion dictionary
_ABBREV_MAP = {
    "cust": "customer",
    "custmr": "customer",
    "amt": "amount",
    "amnt": "amount",
    "qty": "quantity",
    "qtty": "quantity",
    "dt": "date",
    "num": "number",
    "no": "number",
    "nbr": "number",
    "id": "id",
    "desc": "description",
    "cd": "code",
    "nm": "name",
    "addr": "address",
    "phn": "phone",
    "ph": "phone",
    "usr": "user",
    "prod": "product",
    "ord": "order",
    "mgr": "manager",
    "emp": "employee",
    "empl": "employee",
    "dept": "department",
    "acct": "account",
    "bal": "balance",
    "prc": "price",
    "pr": "price",
    "cnt": "count",
    "flg": "flag",
    "ind": "indicator",
    "stat": "status",
    "sts": "status",
    "typ": "type",
    "tp": "type",
    "grp": "group",
    "cls": "class",
    "seq": "sequence",
    "strt": "start",
    "str": "start",
    "end": "end",
    "crte": "create",
    "crt": "create",
    "updt": "update",
    "upd": "update",
    "mod": "modified",
    "del": "deleted",
    "act": "active",
    "actv": "active",
}

# Numeric type families for type-constrained fuzzy matching
_NUMERIC_TYPES = {"int", "integer", "bigint", "smallint", "tinyint", "decimal", "numeric", "float", "real", "money"}
_STRING_TYPES = {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}
_DATE_TYPES = {"date", "datetime", "datetime2", "timestamp", "smalldatetime"}


@dataclass
class MappingRow:
    src_schema: str
    src_table: str
    src_column: str
    src_type: str
    tgt_schema: str
    tgt_table: str
    tgt_column: str | None
    confidence: float
    source: str
    approved: bool
    notes: str


class MappingEngine:
    """Generate and manage column mappings between source and target schemas."""

    def __init__(self, registry: "SchemaRegistry") -> None:
        self.registry = registry

    # Columns at or below this confidence are eligible for LLM re-evaluation.
    _LLM_ASSIST_THRESHOLD = 0.60

    def run_diff(
        self,
        source_name: str = "sqlserver",
        target_name: str = "redshift",
        llm_assist: bool = False,
    ) -> list[MappingRow]:
        """
        Compare source and target schemas and generate column mappings.
        Saves results to the registry and returns the mapping rows.

        Args:
            source_name: Name of the registered SQL Server DDL source.
            target_name: Name of the registered Redshift DDL source.
            llm_assist: When True, invoke the LLM for columns that Tiers 1-4
                could not confidently match (confidence < 0.60 or no match).
                LLM suggestions are stored as source='llm_suggested' with
                confidence=0.65 and approved=False — always require human review.
        """
        # Load everything eagerly within one session to avoid DetachedInstanceError
        with Session(self.registry._engine) as session:
            src_source = session.scalar(
                select(SchemaSource).where(SchemaSource.name == source_name)
            )
            tgt_source = session.scalar(
                select(SchemaSource).where(SchemaSource.name == target_name)
            )
            if src_source is None:
                raise ValueError(f"Source '{source_name}' not found. Run load-source first.")
            if tgt_source is None:
                raise ValueError(f"Target '{target_name}' not found. Run load-target first.")

            # Build plain-data structures (not ORM objects) to avoid session detachment
            src_tables_data: dict[tuple[str, str], dict] = {}
            for t in session.scalars(select(Table).where(Table.source_id == src_source.id)):
                cols = session.scalars(select(Column).where(Column.table_id == t.id)).all()
                src_tables_data[(_normalize(t.schema_name), _normalize(t.table_name))] = {
                    "schema": t.schema_name,
                    "table": t.table_name,
                    "id": t.id,
                    "cols": [{"name": c.column_name, "type": c.data_type} for c in cols],
                }

            tgt_tables_data: dict[tuple[str, str], dict] = {}
            for t in session.scalars(select(Table).where(Table.source_id == tgt_source.id)):
                cols = session.scalars(select(Column).where(Column.table_id == t.id)).all()
                tgt_tables_data[(_normalize(t.schema_name), _normalize(t.table_name))] = {
                    "schema": t.schema_name,
                    "table": t.table_name,
                    "cols": {_normalize(c.column_name): {"name": c.column_name, "type": c.data_type} for c in cols},
                }

        results: list[MappingRow] = []
        for src_key, src_data in src_tables_data.items():
            tgt_data = tgt_tables_data.get(src_key)

            if tgt_data is None:
                # Try fuzzy match on table name
                _, src_tname = src_key
                src_snake = _to_snake_case(src_tname)
                for (_, tgt_tname_norm), td in tgt_tables_data.items():
                    if _to_snake_case(td["table"]) == src_snake:
                        tgt_data = td
                        break

            if tgt_data is None:
                continue

            tgt_schema = tgt_data["schema"]
            tgt_tname = tgt_data["table"]
            tgt_cols: dict[str, dict] = tgt_data["cols"]

            # Save table mapping
            self.registry.upsert_table_mapping(
                src_schema=src_data["schema"],
                src_table=src_data["table"],
                tgt_schema=tgt_schema,
                tgt_table=tgt_tname,
                confidence=0.95,
                approved=True,
            )

            for src_col in src_data["cols"]:
                tgt_col, confidence, mapping_source = _match_column(
                    src_col["name"], src_col["type"], tgt_cols
                )
                approved = confidence >= 0.90

                self.registry.upsert_column_mapping(
                    src_schema=src_data["schema"],
                    src_table=src_data["table"],
                    src_col=src_col["name"],
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_tname,
                    tgt_col=tgt_col,
                    confidence=confidence,
                    source=mapping_source,
                    approved=approved,
                )
                results.append(MappingRow(
                    src_schema=src_data["schema"],
                    src_table=src_data["table"],
                    src_column=src_col["name"],
                    src_type=src_col["type"],
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_tname,
                    tgt_column=tgt_col,
                    confidence=confidence,
                    source=mapping_source,
                    approved=approved,
                    notes="" if approved else "REVIEW REQUIRED",
                ))

        if not llm_assist:
            return results

        # --- Tier 5: LLM suggestions for low-confidence / unmatched columns ---
        import logging
        from tsql_migrator.errors import LLMError
        from tsql_migrator.schema.llm_suggester import LLMSuggester, build_candidates

        _log = logging.getLogger(__name__)

        low_conf = [r for r in results if r.confidence < self._LLM_ASSIST_THRESHOLD]
        if not low_conf:
            return results

        # Build per-table lookups from data already in memory
        tgt_col_lookup: dict[str, dict[str, str]] = {
            tdata["table"]: {norm: col["name"] for norm, col in tdata["cols"].items()}
            for tdata in tgt_tables_data.values()
        }
        src_col_types: dict[str, dict[str, str]] = {
            sdata["table"]: {c["name"]: c["type"] for c in sdata["cols"]}
            for sdata in src_tables_data.values()
        }

        candidates = build_candidates(low_conf, tgt_col_lookup, src_col_types)

        try:
            suggester = LLMSuggester(self.registry)
            llm_rows = suggester.suggest(candidates)
        except LLMError as e:
            _log.warning("LLM assist unavailable, returning deterministic results: %s", e)
            return results

        # Index existing results by (src_schema, src_table, src_column) for O(1) replacement
        result_index: dict[tuple[str, str, str], int] = {
            (r.src_schema, r.src_table, r.src_column): i
            for i, r in enumerate(results)
        }

        for llm_row in llm_rows:
            self.registry.upsert_column_mapping(
                src_schema=llm_row.src_schema,
                src_table=llm_row.src_table,
                src_col=llm_row.src_column,
                tgt_schema=llm_row.tgt_schema,
                tgt_table=llm_row.tgt_table,
                tgt_col=llm_row.tgt_column,
                confidence=llm_row.confidence,
                source=llm_row.source,
                approved=llm_row.approved,
                notes=llm_row.notes,
            )
            key = (llm_row.src_schema, llm_row.src_table, llm_row.src_column)
            if key in result_index:
                results[result_index[key]] = llm_row

        return results

    def export_csv(self, rows: list[MappingRow], path: str) -> None:
        """Export mapping rows to a CSV file for human review."""
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "src_schema", "src_table", "src_column", "src_type",
                "tgt_schema", "tgt_table", "tgt_column",
                "confidence", "source", "approved", "notes",
            ])
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    "src_schema": row.src_schema,
                    "src_table": row.src_table,
                    "src_column": row.src_column,
                    "src_type": row.src_type,
                    "tgt_schema": row.tgt_schema,
                    "tgt_table": row.tgt_table,
                    "tgt_column": row.tgt_column or "",
                    "confidence": f"{row.confidence:.2f}",
                    "source": row.source,
                    "approved": "1" if row.approved else "0",
                    "notes": row.notes,
                })

    def import_csv(self, path: str) -> int:
        """Import a reviewed CSV and mark approved mappings."""
        count = 0
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tgt_col = row.get("tgt_column") or None
                self.registry.upsert_column_mapping(
                    src_schema=row["src_schema"],
                    src_table=row["src_table"],
                    src_col=row["src_column"],
                    tgt_schema=row.get("tgt_schema", "public"),
                    tgt_table=row["tgt_table"],
                    tgt_col=tgt_col,
                    confidence=float(row.get("confidence", 1.0)),
                    source="human",
                    approved=True,
                    notes=row.get("notes"),
                )
                count += 1
        return count


# ── Matching helpers ────────────────────────────────────────────────────────


def _normalize(name: str) -> str:
    """Lowercase and strip schema qualifiers."""
    return name.lower().strip()


def _to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase/mixed to snake_case."""
    # Insert _ before uppercase letters preceded by lowercase or digits
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert _ before sequences of uppercase followed by lowercase (e.g. XMLParser)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def _expand_abbreviations(name: str) -> str:
    """Expand common abbreviations in a snake_case column name."""
    parts = name.split("_")
    expanded = [_ABBREV_MAP.get(p, p) for p in parts]
    return "_".join(expanded)


def _type_family(data_type: str) -> str:
    dt = data_type.lower().split("(")[0].strip()
    if dt in _NUMERIC_TYPES:
        return "numeric"
    if dt in _STRING_TYPES:
        return "string"
    if dt in _DATE_TYPES:
        return "date"
    return "other"


def _levenshtein(a: str, b: str) -> int:
    """Compute Levenshtein distance between two strings."""
    if len(a) < len(b):
        return _levenshtein(b, a)
    if len(b) == 0:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            insertions = prev[j + 1] + 1
            deletions = curr[j] + 1
            subs = prev[j] + (0 if ca == cb else 1)
            curr.append(min(insertions, deletions, subs))
        prev = curr
    return prev[len(b)]


def _match_column(
    src_name: str,
    src_type: str,
    tgt_cols: dict[str, dict],  # normalized_name → {name, type}
) -> tuple[str | None, float, str]:
    """
    Match a source column name to a target column.
    Returns (tgt_column_name | None, confidence, source).
    tgt_cols keys are normalized (lowercase) column names.
    """
    src_lower = src_name.lower()
    src_snake = _to_snake_case(src_name)
    src_expanded = _expand_abbreviations(src_snake)
    src_family = _type_family(src_type)

    # 1. Exact normalized match
    if src_lower in tgt_cols:
        return (tgt_cols[src_lower]["name"], 0.95, "auto_exact")

    # 2. Snake-case match
    if src_snake in tgt_cols:
        return (tgt_cols[src_snake]["name"], 0.90, "auto_exact")

    # 3. Abbreviation expansion match
    if src_expanded in tgt_cols:
        return (tgt_cols[src_expanded]["name"], 0.80, "auto_fuzzy")

    # 4. Type-constrained Levenshtein fuzzy match
    best_name = None
    best_score = 0.0
    for tgt_key, tgt_col in tgt_cols.items():
        if _type_family(tgt_col["type"]) != src_family and src_family != "other":
            continue
        max_len = max(len(src_snake), len(tgt_key), 1)
        dist = _levenshtein(src_snake, tgt_key)
        similarity = 1.0 - (dist / max_len)
        if similarity > best_score and similarity >= 0.5:
            best_score = similarity
            best_name = tgt_col["name"]

    if best_name is not None:
        confidence = 0.40 + (best_score - 0.5) * 0.60  # scale 0.5–1.0 → 0.40–0.70
        return (best_name, round(confidence, 2), "auto_fuzzy")

    # 5. No match
    return (None, 0.0, "auto_fuzzy")


def _fuzzy_match_table(
    src_key: tuple[str, str],
    tgt_tables: dict[tuple[str, str], "Table"],
) -> "Table | None":
    """Attempt fuzzy match on table name (snake-case normalization only)."""
    _, src_tname = src_key
    src_snake = _to_snake_case(src_tname)
    for (_, tgt_tname), tgt_table in tgt_tables.items():
        if _to_snake_case(tgt_tname) == src_snake:
            return tgt_table
    return None
