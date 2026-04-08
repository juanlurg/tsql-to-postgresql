"""LLM prompt templates for SQL translation."""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are a SQL migration engine. You translate T-SQL (SQL Server) to Amazon Redshift SQL.

Rules:
1. Output ONLY valid JSON matching the schema below. No prose, no markdown fences.
2. Column names in output MUST exactly match the Redshift DDL provided. Never invent column names.
3. Preserve all query semantics exactly. Do not optimize or restructure logic unless required for Redshift compatibility.
4. If you cannot translate a construct, add a -- MIGRATION_TODO: <reason> comment in the SQL and list it in migration_todos.
5. Do NOT add ORDER BY, LIMIT, or other clauses not present in the input.
6. Do NOT add, remove, or reorder SELECT columns unless required for correctness.

Output JSON schema:
{
  "translated_sql": "<Redshift SQL string>",
  "changes_made": ["<description of each change>"],
  "unmapped_columns": ["<column names you could not confidently map>"],
  "confidence": "high" | "medium" | "low",
  "migration_todos": ["<description of items requiring human review>"]
}
"""

def build_user_prompt(
    tsql: str,
    ddl_context: str | None = None,
    error_context: str | None = None,
) -> str:
    """
    Build the user message for the translation request.

    Args:
        tsql: The T-SQL to translate.
        ddl_context: Relevant CREATE TABLE statements (both source + target).
        error_context: If this is a retry after parse failure, the error message.
    """
    parts: list[str] = []

    if ddl_context:
        parts.append(f"## Schema Context (SQL Server → Redshift)\n\n{ddl_context}")

    if error_context:
        parts.append(
            f"## Note\nThe deterministic translator could not parse this SQL "
            f"({error_context}). Please translate it directly."
        )

    parts.append(f"## T-SQL to translate\n\n```sql\n{tsql}\n```")
    parts.append("Translate the T-SQL above to Redshift SQL and respond with JSON only.")

    return "\n\n".join(parts)


MAPPING_SUGGESTION_SYSTEM_PROMPT = """\
You are a database schema migration expert. Given source (SQL Server) and target (Redshift) DDL, \
map source columns to their semantic equivalents in the target table.

Rules:
1. Output ONLY valid JSON matching the schema below. No prose, no markdown fences.
2. tgt_column MUST exactly match a column name from the target DDL. Never invent names.
3. Use null for tgt_column when no reasonable semantic match exists.
4. reasoning must explain why the mapping is correct or why there is no match.

Output JSON schema:
{
  "mappings": [
    {
      "src_column": "<source column name>",
      "tgt_column": "<target column name or null>",
      "reasoning": "<brief explanation>"
    }
  ]
}
"""


def build_mapping_suggestion_prompt(
    src_schema: str,
    src_table: str,
    src_ddl: str,
    tgt_schema: str,
    tgt_table: str,
    tgt_ddl: str,
    unmatched_cols: list[str],
) -> str:
    """
    Build the user message for a column mapping suggestion request.

    Args:
        src_schema: Source schema name.
        src_table: Source table name.
        src_ddl: CREATE TABLE DDL for the source table.
        tgt_schema: Target schema name.
        tgt_table: Target table name.
        tgt_ddl: CREATE TABLE DDL for the target table.
        unmatched_cols: Source column names that could not be matched deterministically.
    """
    col_list = ", ".join(unmatched_cols)
    return (
        f"Source table: {src_schema}.{src_table}\n"
        f"{src_ddl}\n\n"
        f"Target table: {tgt_schema}.{tgt_table}\n"
        f"{tgt_ddl}\n\n"
        f"Map these source columns to their target equivalents:\n{col_list}\n\n"
        "Respond with JSON only."
    )


def build_ddl_context(
    table_ddls: list[dict[str, str]],
) -> str:
    """
    Format DDL context for injection into the prompt.

    Args:
        table_ddls: List of dicts with keys 'table', 'src_ddl', 'tgt_ddl'.
    """
    if not table_ddls:
        return ""

    lines = []
    for entry in table_ddls:
        lines.append(f"-- Source (SQL Server): {entry['table']}")
        lines.append(entry.get("src_ddl", "-- (not available)"))
        lines.append(f"-- Target (Redshift): {entry['table']}")
        lines.append(entry.get("tgt_ddl", "-- (not available)"))
        lines.append("")

    return "\n".join(lines)
