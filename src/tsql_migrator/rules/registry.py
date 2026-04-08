"""
RuleRegistry: loads YAML-defined function and datatype rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
import sqlglot.expressions as exp


@dataclass
class FunctionRule:
    tsql_name: str
    redshift_name: str
    arg_count: int | None = None
    arg_reorder: list[int] | None = None
    warn_message: str | None = None
    hard_error: bool = False


class RuleRegistry:
    """Holds all declarative transformation rules."""

    def __init__(self) -> None:
        self._function_rules: dict[str, FunctionRule] = {}

    @classmethod
    def load_defaults(cls) -> "RuleRegistry":
        """Load built-in rules from the YAML files in the rules/ directory."""
        registry = cls()
        rules_dir = Path(__file__).parent
        fn_file = rules_dir / "function_rules.yaml"
        if fn_file.exists():
            with fn_file.open() as f:
                data = yaml.safe_load(f) or []
            for item in data:
                rule = FunctionRule(
                    tsql_name=item["tsql_name"].upper(),
                    redshift_name=item["redshift_name"],
                    arg_count=item.get("arg_count"),
                    arg_reorder=item.get("arg_reorder"),
                    warn_message=item.get("warn_message"),
                    hard_error=item.get("hard_error", False),
                )
                registry._function_rules[rule.tsql_name] = rule
        return registry

    def apply_function_rule(
        self,
        node: exp.Expression,
        ctx: Any,  # TransformContext
    ) -> exp.Expression:
        """
        Apply a YAML-driven function rule if one exists for the given node.
        Returns the (possibly rewritten) node.
        """
        name = ""
        if isinstance(node, exp.Anonymous):
            name = (node.this or "").upper()
        elif isinstance(node, exp.Func):
            name = type(node).__name__.upper()

        rule = self._function_rules.get(name)
        if rule is None:
            return node

        if rule.hard_error:
            from tsql_migrator.errors import HardError
            raise HardError(
                f"{name} is not supported in Redshift.",
                construct=name,
            )

        if rule.warn_message:
            ctx.warn(rule.warn_message)

        # Build new expressions with optional arg reorder
        args = list(node.expressions) if hasattr(node, "expressions") else []
        if rule.arg_reorder and len(args) >= len(rule.arg_reorder):
            args = [args[i] for i in rule.arg_reorder]

        return exp.Anonymous(this=rule.redshift_name, expressions=args)
