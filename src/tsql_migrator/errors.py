"""Custom exceptions for the T-SQL → Redshift migration pipeline."""


class MigratorError(Exception):
    """Base class for all migrator errors."""


class ParseError(MigratorError):
    """
    Raised when sqlglot cannot parse the input T-SQL.
    Triggers LLM fallback if enabled.
    """

    def __init__(self, message: str, sql: str = "") -> None:
        super().__init__(message)
        self.sql = sql


class HardError(MigratorError):
    """
    Raised for T-SQL constructs that cannot be auto-migrated under any path.
    Examples: recursive CTEs, dynamic SQL (EXEC), linked server references.
    """

    def __init__(self, message: str, construct: str = "") -> None:
        super().__init__(message)
        self.construct = construct


class SchemaError(MigratorError):
    """Raised during DDL loading or schema registry operation failures."""


class LLMError(MigratorError):
    """Raised when LLM output fails post-generation validation."""
