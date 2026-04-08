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


TABLE_MATCHING_SYSTEM_PROMPT = """\
You are a database schema migration expert. Given a list of SQL Server source tables and \
Amazon Redshift target tables (with their columns), match each source table to its semantic \
equivalent in the target schema.

Rules:
1. Output ONLY valid JSON matching the schema below. No prose, no markdown fences.
2. tgt_schema and tgt_table MUST exactly match names from the provided target table list. Never invent names.
3. Use null for tgt_schema and tgt_table when no reasonable semantic match exists.
4. reasoning must briefly explain the match or why there is none.

Output JSON schema:
{
  "table_mappings": [
    {
      "src_schema": "<source schema>",
      "src_table": "<source table name>",
      "tgt_schema": "<target schema or null>",
      "tgt_table": "<target table name or null>",
      "reasoning": "<brief explanation>"
    }
  ]
}
"""


def build_table_matching_prompt(
    unmatched_src: list[dict],
    tgt_tables: list[dict],
) -> str:
    """
    Build the user message for a table matching request.

    Args:
        unmatched_src: Source tables with no deterministic match.
            Each dict has keys: schema, table, cols (list of {name, type}).
        tgt_tables: All available target tables.
            Each dict has keys: schema, table, cols (list of {name, type}).
    """
    def _col_summary(cols: list[dict]) -> str:
        return ", ".join(f"{c['name']} {c['type']}" for c in cols[:20])

    lines = ["## Source tables without a match (SQL Server):\n"]
    for src in unmatched_src:
        lines.append(f"### {src['schema']}.{src['table']}")
        lines.append(f"  {_col_summary(src['cols'])}")
        lines.append("")

    lines.append("## Available target tables (Redshift):\n")
    for tgt in tgt_tables:
        lines.append(f"### {tgt['schema']}.{tgt['table']}")
        lines.append(f"  {_col_summary(tgt['cols'])}")
        lines.append("")

    lines.append(
        "Match each source table to its Redshift equivalent. "
        "Respond with JSON only."
    )
    return "\n".join(lines)


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
