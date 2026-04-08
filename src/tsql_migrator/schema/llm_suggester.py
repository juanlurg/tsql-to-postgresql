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

import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tsql_migrator.errors import LLMError
from tsql_migrator.llm.prompts import (
    MAPPING_SUGGESTION_SYSTEM_PROMPT,
    build_mapping_suggestion_prompt,
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
        genai.configure(api_key=api_key)
        model_name = model or os.getenv("LLM_MODEL", "gemini-3-flash-preview")
        self._model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=MAPPING_SUGGESTION_SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0,
                max_output_tokens=2048,
            ),
        )
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
        retry=retry_if_exception_type(
            (google_exceptions.ResourceExhausted, google_exceptions.DeadlineExceeded)
        ),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _call_llm(self, prompt: str) -> str:
        try:
            response = self._model.generate_content(prompt)
        except google_exceptions.GoogleAPIError as e:
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
