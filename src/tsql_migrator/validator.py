"""
Validator: optional EXPLAIN-based validation against a live Redshift cluster.

This runs EXPLAIN only — never SELECT or DML — and is always wrapped in a
transaction that is rolled back. Safe against production.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ValidationResult:
    success: bool
    plan_lines: list[str] | None = None
    error_message: str | None = None
    pg_code: str | None = None


def validate_on_redshift(sql: str, conn_str: str) -> ValidationResult:
    """
    Run EXPLAIN {sql} against a Redshift cluster.

    Args:
        sql: Redshift SQL string to validate.
        conn_str: Redshift connection string (redshift+redshift_connector://...).

    Returns:
        ValidationResult with success=True and the query plan on success,
        or success=False with error details on failure.
    """
    try:
        import redshift_connector  # type: ignore[import]
    except ImportError:
        return ValidationResult(
            success=False,
            error_message=(
                "redshift-connector is not installed. "
                "Run: pip install 'tsql-migrator[redshift]'"
            ),
        )

    # Parse a simple DSN: redshift+redshift_connector://user:pass@host:port/dbname
    import re
    m = re.match(
        r"redshift\+redshift_connector://([^:]+):([^@]+)@([^:/]+):?(\d+)?/(\S+)",
        conn_str,
    )
    if not m:
        return ValidationResult(
            success=False,
            error_message=f"Cannot parse Redshift DSN: {conn_str!r}",
        )
    user, password, host, port, database = m.groups()
    port = int(port) if port else 5439

    conn = None
    try:
        conn = redshift_connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        conn.autocommit = False
        cursor = conn.cursor()
        cursor.execute(f"EXPLAIN {sql}")
        plan_lines = [row[0] for row in cursor.fetchall()]
        conn.rollback()
        return ValidationResult(success=True, plan_lines=plan_lines)
    except Exception as e:
        pg_code = getattr(e, "pgcode", None)
        return ValidationResult(
            success=False,
            error_message=str(e),
            pg_code=pg_code,
        )
    finally:
        if conn is not None:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
