"""
LLMSuggester: Tier 6 LLM-based column mapping suggestions.

Invoked by MappingEngine.run_diff() when --llm-assist is enabled, for columns
that Tiers 1-4 could not confidently match (confidence < 0.60 or no match).

One LLM call is made per table (batched), not per column, so the model has
full schema context. Suggestions are stored as source='llm_suggested' with
confidence=0.65 and approved=False — they always require human review.

Anti-hallucination: every suggested tgt_column is validated against the set
of actual target column names in the registry. Invalid suggestions are silently
discarded, leaving the column as unmatched.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tsql_migrator.errors import LLMError
from tsql_migrator.llm.prompts import (
    MAPPING_SUGGESTION_SYSTEM_PROMPT,
    TABLE_MATCHING_SYSTEM_PROMPT,
    build_mapping_suggestion_prompt,
    build_table_matching_prompt,
)
from tsql_migrator.schema.mapping_engine import MappingRow

if TYPE_CHECKING:
    from tsql_migrator.schema.registry import SchemaRegistry

logger = logging.getLogger(__name__)

# LLM suggestions are never auto-approved regardless of returned confidence.
_LLM_SUGGESTION_CONFIDENCE = 0.65


@dataclass
class _SuggestionCandidate:
    """Internal: one table's worth of unmapped columns to send to the LLM."""

    src_schema: str
    src_table: str
    tgt_schema: str
    tgt_table: str
    # col_name → type, for building MappingRow results
    src_col_types: dict[str, str]
    # normalized (lowercase) → actual name, for anti-hallucination validation
    tgt_col_lookup: dict[str, str]
    # source column names that need LLM help (original casing)
    unmatched_cols: list[str]


class LLMSuggester:
    """
    Suggest column mappings via LLM for columns that deterministic tiers missed.

    Usage::

        suggester = LLMSuggester(registry)
        llm_rows = suggester.suggest(candidates)
    """

    def __init__(
        self,
        registry: "SchemaRegistry",
        model: str | None = None,
    ) -> None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise LLMError(
                "GEMINI_API_KEY environment variable is not set. "
                "LLM mapping assistance is unavailable."
            )
        self._client = genai.Client(api_key=api_key)
        self._model_name = model or os.getenv("LLM_MODEL", "gemini-2.0-flash")
        self._system_instruction = MAPPING_SUGGESTION_SYSTEM_PROMPT
        self._registry = registry

    def suggest(
        self,
        candidates: list[_SuggestionCandidate],
    ) -> list[MappingRow]:
        """
        For each candidate table, call the LLM and return validated MappingRows.
        Tables where DDL is unavailable are skipped silently.
        """
        results: list[MappingRow] = []
        for candidate in candidates:
            try:
                rows = self._suggest_for_table(candidate)
                results.extend(rows)
            except LLMError as e:
                logger.warning(
                    "LLM mapping suggestion failed for %s.%s: %s",
                    candidate.src_schema,
                    candidate.src_table,
                    e,
                )
        return results

    def _suggest_for_table(self, candidate: _SuggestionCandidate) -> list[MappingRow]:
        src_ddl = self._registry.get_table_ddl_string(candidate.src_table, "tsql")
        tgt_ddl = self._registry.get_table_ddl_string(candidate.tgt_table, "redshift")

        if not src_ddl or not tgt_ddl:
            logger.debug(
                "Skipping LLM suggestion for %s.%s: DDL unavailable",
                candidate.src_schema,
                candidate.src_table,
            )
            return []

        prompt = build_mapping_suggestion_prompt(
            src_schema=candidate.src_schema,
            src_table=candidate.src_table,
            src_ddl=src_ddl,
            tgt_schema=candidate.tgt_schema,
            tgt_table=candidate.tgt_table,
            tgt_ddl=tgt_ddl,
            unmatched_cols=candidate.unmatched_cols,
        )

        raw = self._call_llm(prompt)
        suggestions = self._parse_response(raw)

        # Build a fast lookup of requested columns (case-insensitive)
        requested = {c.lower(): c for c in candidate.unmatched_cols}

        rows: list[MappingRow] = []
        for item in suggestions:
            src_col_lower = (item.get("src_column") or "").lower()
            tgt_col_raw = item.get("tgt_column")
            reasoning = item.get("reasoning", "")

            # Only process columns we actually asked about
            if src_col_lower not in requested:
                continue

            src_col_orig = requested[src_col_lower]
            src_type = candidate.src_col_types.get(src_col_orig, "")

            if tgt_col_raw is None:
                # LLM explicitly found no match — leave as no-match row (confidence=0)
                rows.append(MappingRow(
                    src_schema=candidate.src_schema,
                    src_table=candidate.src_table,
                    src_column=src_col_orig,
                    src_type=src_type,
                    tgt_schema=candidate.tgt_schema,
                    tgt_table=candidate.tgt_table,
                    tgt_column=None,
                    confidence=0.0,
                    source="llm_suggested",
                    approved=False,
                    notes=f"LLM: no match. {reasoning}".strip(". "),
                ))
                continue

            # Anti-hallucination: tgt_col must exist in actual target DDL
            tgt_col_norm = tgt_col_raw.lower()
            if tgt_col_norm not in candidate.tgt_col_lookup:
                logger.debug(
                    "LLM hallucinated column '%s' for %s.%s → discarded",
                    tgt_col_raw,
                    candidate.tgt_schema,
                    candidate.tgt_table,
                )
                continue

            actual_tgt_col = candidate.tgt_col_lookup[tgt_col_norm]
            rows.append(MappingRow(
                src_schema=candidate.src_schema,
                src_table=candidate.src_table,
                src_column=src_col_orig,
                src_type=src_type,
                tgt_schema=candidate.tgt_schema,
                tgt_table=candidate.tgt_table,
                tgt_column=actual_tgt_col,
                confidence=_LLM_SUGGESTION_CONFIDENCE,
                source="llm_suggested",
                approved=False,
                notes=f"LLM: {reasoning}" if reasoning else "LLM suggested",
            ))

        return rows

    @retry(
        retry=retry_if_exception_type(genai_errors.APIError),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _call_llm(self, prompt: str) -> str:
        try:
            response = self._client.models.generate_content(
                model=self._model_name,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    system_instruction=self._system_instruction,
                    temperature=0,
                    max_output_tokens=2048,
                ),
            )
        except genai_errors.APIError as e:
            raise LLMError(f"Gemini API error during mapping suggestion: {e}") from e
        return response.text or ""

    def _parse_response(self, content: str) -> list[dict]:
        stripped = content.strip()
        if stripped.startswith("```"):
            lines = stripped.splitlines()
            stripped = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            data = json.loads(stripped)
        except json.JSONDecodeError as e:
            raise LLMError(
                f"LLM returned non-JSON for mapping suggestion: {e}\n\nResponse:\n{content[:500]}"
            ) from e

        mappings = data.get("mappings")
        if not isinstance(mappings, list):
            raise LLMError("LLM mapping response missing 'mappings' list.")
        return mappings


class LLMTableMatcher:
    """
    Suggest target table matches for source tables that deterministic tiers could not match.

    One LLM call is made for all unmatched source tables (batched), so the model has
    full cross-table context. Suggestions require human review before use.

    Anti-hallucination: every suggested tgt_table is validated against the actual set
    of target tables in tgt_tables_data. Invalid suggestions are silently discarded.
    """

    def __init__(
        self,
        registry: "SchemaRegistry",
        model: str | None = None,
    ) -> None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise LLMError(
                "GEMINI_API_KEY environment variable is not set. "
                "LLM table matching is unavailable."
            )
        self._client = genai.Client(api_key=api_key)
        self._model_name = model or os.getenv("LLM_MODEL", "gemini-2.0-flash")

    def suggest(
        self,
        unmatched_src: list[dict],
        tgt_tables_data: dict[tuple, dict],
    ) -> list[dict]:
        """
        Match unmatched source tables to target tables via LLM.

        Args:
            unmatched_src: Source table dicts (schema, table, cols) with no deterministic match.
            tgt_tables_data: All target table data keyed by (norm_schema, norm_table).

        Returns:
            Validated list of dicts: {src_schema, src_table, tgt_schema, tgt_table, reasoning}.
            Only entries where tgt_table actually exists in tgt_tables_data are returned.
        """
        if not unmatched_src or not tgt_tables_data:
            return []

        tgt_list = [
            {"schema": v["schema"], "table": v["table"], "cols": v["cols"]}
            for v in tgt_tables_data.values()
        ]
        prompt = build_table_matching_prompt(
            unmatched_src=[
                {"schema": s["schema"], "table": s["table"], "cols": s["cols"]}
                for s in unmatched_src
            ],
            tgt_tables=tgt_list,
        )

        raw = self._call_llm(prompt)
        matches = self._parse_response(raw)

        # Anti-hallucination: only keep matches whose target actually exists
        valid_tgt_keys = set(tgt_tables_data.keys())
        validated = []
        for m in matches:
            tgt_schema = m.get("tgt_schema")
            tgt_table = m.get("tgt_table")
            if tgt_table is None or tgt_schema is None:
                continue
            key = (tgt_schema.lower().strip(), tgt_table.lower().strip())
            if key not in valid_tgt_keys:
                logger.debug(
                    "LLM suggested non-existent target table '%s.%s' — discarded",
                    tgt_schema,
                    tgt_table,
                )
                continue
            validated.append(m)

        return validated

    @retry(
        retry=retry_if_exception_type(genai_errors.APIError),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _call_llm(self, prompt: str) -> str:
        try:
            response = self._client.models.generate_content(
                model=self._model_name,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    system_instruction=TABLE_MATCHING_SYSTEM_PROMPT,
                    temperature=0,
                    max_output_tokens=2048,
                ),
            )
        except genai_errors.APIError as e:
            raise LLMError(f"Gemini API error during table matching: {e}") from e
        return response.text or ""

    def _parse_response(self, content: str) -> list[dict]:
        stripped = content.strip()
        if stripped.startswith("```"):
            lines = stripped.splitlines()
            stripped = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            data = json.loads(stripped)
        except json.JSONDecodeError as e:
            raise LLMError(
                f"LLM returned non-JSON for table matching: {e}\n\nResponse:\n{content[:500]}"
            ) from e

        mappings = data.get("table_mappings")
        if not isinstance(mappings, list):
            raise LLMError("LLM table matching response missing 'table_mappings' list.")
        return mappings


def build_candidates(
    table_rows: list[MappingRow],
    tgt_col_lookup_by_table: dict[str, dict[str, str]],
    src_col_types_by_table: dict[str, dict[str, str]],
) -> list[_SuggestionCandidate]:
    """
    Group low-confidence/unmatched MappingRows into SuggestionCandidates
    (one per table) for batch LLM calls.

    Args:
        table_rows: MappingRows with confidence < 0.60 or tgt_column=None.
        tgt_col_lookup_by_table: {tgt_table → {normalized_col → actual_col}}.
        src_col_types_by_table: {src_table → {col_name → type}}.
    """
    # Group by (src_schema, src_table, tgt_schema, tgt_table)
    groups: dict[tuple, _SuggestionCandidate] = {}
    for row in table_rows:
        key = (row.src_schema, row.src_table, row.tgt_schema, row.tgt_table)
        if key not in groups:
            tgt_key = row.tgt_table
            groups[key] = _SuggestionCandidate(
                src_schema=row.src_schema,
                src_table=row.src_table,
                tgt_schema=row.tgt_schema,
                tgt_table=row.tgt_table,
                src_col_types=src_col_types_by_table.get(row.src_table, {}),
                tgt_col_lookup=tgt_col_lookup_by_table.get(tgt_key, {}),
                unmatched_cols=[],
            )
        groups[key].unmatched_cols.append(row.src_column)

    return list(groups.values())
