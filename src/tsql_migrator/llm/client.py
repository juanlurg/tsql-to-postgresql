"""
LLM client: wraps the Google Gemini API for SQL translation fallback.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Literal

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from tsql_migrator.errors import LLMError
from tsql_migrator.llm.prompts import SYSTEM_PROMPT, build_user_prompt


@dataclass
class LLMTranslationResult:
    translated_sql: str
    changes_made: list[str] = field(default_factory=list)
    unmapped_columns: list[str] = field(default_factory=list)
    confidence: str = "medium"   # "high" | "medium" | "low"
    migration_todos: list[str] = field(default_factory=list)


class _TranslationResponse(BaseModel):
    translated_sql: str
    changes_made: list[str]
    unmapped_columns: list[str]
    confidence: Literal["high", "medium", "low"]
    migration_todos: list[str]


class LLMClient:
    """Thin wrapper around the Google Gemini API."""

    def __init__(
        self,
        model: str | None = None,
        max_tokens: int = 4096,
    ) -> None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise LLMError(
                "GEMINI_API_KEY environment variable is not set. "
                "LLM translation is unavailable."
            )
        self._client = genai.Client(api_key=api_key)
        self._model_name = model or os.getenv("LLM_MODEL", "gemini-2.0-flash")
        self._system_instruction = SYSTEM_PROMPT
        self._max_tokens = max_tokens

    @retry(
        retry=retry_if_exception_type(genai_errors.APIError),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def translate(
        self,
        tsql: str,
        ddl_context: str | None = None,
        error_context: str | None = None,
    ) -> LLMTranslationResult:
        """
        Translate T-SQL to Redshift SQL via the Gemini API.

        Returns LLMTranslationResult with structured output.
        Raises LLMError if the response cannot be parsed or validated.
        """
        user_message = build_user_prompt(
            tsql=tsql,
            ddl_context=ddl_context,
            error_context=error_context,
        )

        try:
            response = self._client.models.generate_content(
                model=self._model_name,
                contents=user_message,
                config=genai_types.GenerateContentConfig(
                    system_instruction=self._system_instruction,
                    response_mime_type="application/json",
                    response_schema=_TranslationResponse,
                    temperature=0,
                    max_output_tokens=self._max_tokens,
                ),
            )
        except genai_errors.APIError as e:
            raise LLMError(f"Gemini API error: {e}") from e

        content = response.text if response.text else ""
        return self._parse_response(content)

    def _parse_response(self, content: str) -> LLMTranslationResult:
        """Parse the structured JSON response from the LLM."""
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise LLMError(
                f"LLM returned non-JSON response: {e}\n\nResponse:\n{content[:500]}"
            ) from e

        translated_sql = data.get("translated_sql", "")
        if not translated_sql or not isinstance(translated_sql, str):
            raise LLMError("LLM response missing 'translated_sql' field.")

        return LLMTranslationResult(
            translated_sql=translated_sql,
            changes_made=data.get("changes_made", []),
            unmapped_columns=data.get("unmapped_columns", []),
            confidence=data.get("confidence", "medium"),
            migration_todos=data.get("migration_todos", []),
        )
