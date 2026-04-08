# tsql-migrator

Translate T-SQL (SQL Server) queries to Amazon Redshift SQL.

Handles both **dialect translation** (syntax/function/type differences) and **schema-level column renames** (when your Redshift tables have different column names than the SQL Server source).

## How it works

The pipeline runs in three stages:

1. **Preprocessor** — strips or rewrites T-SQL constructs that don't exist in Redshift (`WITH (NOLOCK)`, `SELECT TOP n`, table hints, etc.)
2. **Transform pipeline** — deterministic rewrites via sqlglot AST: function renames, data type conversions, column renames from the schema registry
3. **LLM fallback** — complex semantic rewrites (PIVOT, APPLY, etc.) that can't be expressed as simple rules are sent to Claude Sonnet with structured output and column hallucination validation

Hard errors are raised (never silently wrong SQL) for constructs with no Redshift equivalent: recursive CTEs, dynamic SQL, cursors, `FOR XML`, linked servers.

## Installation

```bash
pip install -e ".[dev]"
```

For direct Redshift connectivity:

```bash
pip install -e ".[redshift]"
```

Requires Python 3.12+.

## Quick start

```bash
# Translate from stdin
echo "SELECT TOP 10 CustomerID, Cust_Name FROM dbo.Orders WITH(NOLOCK)" | tsql-migrator translate

# Translate a file
tsql-migrator translate --input query.sql --output query_rs.sql

# Show a transformation report
tsql-migrator translate --input query.sql --report
```

## Schema registry

When column names differ between SQL Server and Redshift, load your DDL files and generate a mapping for human review:

```bash
# 1. Load source and target DDL
tsql-migrator schema load-source --file sqlserver.sql
tsql-migrator schema load-target --file redshift.sql

# 2. Auto-generate a mapping CSV for review
tsql-migrator schema diff --output mappings_review.csv

# 3. Edit the CSV to confirm/correct mappings, then import
tsql-migrator schema import-map --file mappings_review.csv

# Check coverage
tsql-migrator schema status
```

Once mappings are approved, pass `--schema <name>` to `translate` to apply them:

```bash
tsql-migrator translate --input query.sql --schema sqlserver
```

## What gets translated

**Syntax**

| T-SQL | Redshift |
|---|---|
| `SELECT TOP n` | `SELECT … LIMIT n` |
| `WITH (NOLOCK)` | stripped |
| `CROSS APPLY` | `CROSS JOIN LATERAL` |
| `OUTER APPLY` | `LEFT JOIN LATERAL … ON TRUE` |
| `SELECT INTO #temp` | `CREATE TEMP TABLE … AS SELECT` |
| `PIVOT` / `UNPIVOT` | LLM rewrite |

**Functions**

| T-SQL | Redshift |
|---|---|
| `ISNULL(a, b)` | `COALESCE(a, b)` |
| `CHARINDEX(sub, str)` | `POSITION(sub IN str)` (args reordered) |
| `REPLICATE(s, n)` | `REPEAT(s, n)` |
| `COUNT_BIG(…)` | `COUNT(…)` |
| `STDEV` / `VAR` | `STDDEV` / `VARIANCE` |
| `GETDATE()` | `SYSDATE` |
| `DATEPART(weekday, …)` | adjusted for Sunday=1 offset |
| `CONVERT(type, val)` | `CAST(val AS type)` |

**Data types**

| T-SQL | Redshift |
|---|---|
| `NVARCHAR(n)` | `VARCHAR(n)` |
| `BIT` | `BOOLEAN` |
| `DATETIME` / `DATETIME2` | `TIMESTAMP` |
| `DATETIMEOFFSET` | `TIMESTAMPTZ` |
| `MONEY` | `DECIMAL(19,4)` |
| `TINYINT` | `SMALLINT` |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` |

**Hard errors** (unsupported, raises immediately):

- Recursive CTEs
- Dynamic SQL (`EXEC`, `sp_executesql`)
- Cursors
- `FOR XML`
- Linked server references

## Development

```bash
# Run tests
pytest

# Lint / format
ruff check .
ruff format .

# Type check
mypy src/
```

## Project structure

```
src/tsql_migrator/
  preprocessor.py       # Pre-parse text cleanup
  parser.py             # sqlglot parse wrapper
  transforms/           # AST transform passes
    hint_stripper.py
    syntax_rewriter.py
    function_rewriter.py
    datatype_converter.py
    column_renamer.py
    table_renamer.py
  rules/                # Declarative rule YAML files
    syntax_rules.yaml
    function_rules.yaml
    datatype_rules.yaml
  schema/               # DDL parsing and column mapping registry
    ddl_parser.py
    mapping_engine.py
    models.py
  generator.py          # Redshift SQL emitter
  annotator.py          # Warning/error annotation
  validator.py          # Output validation
  cli/                  # Typer CLI commands
  api/                  # FastAPI HTTP service
  llm/                  # Claude Sonnet LLM fallback
```
