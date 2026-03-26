from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Callable

import pandas as pd


class RuleScope(str, Enum):
    COLUMN = "column"
    ROW = "row"
    TABLE = "table"


class Severity(str, Enum):
    ERROR = "ERROR"
    WARN = "WARN"



@dataclass
class ValidationResult:
    rule: str
    scope: RuleScope
    passed: bool
    severity: Severity
    column: Optional[str] = None
    metadata: Dict[str, Any] | None = None
    failing_rows: Optional[pd.DataFrame] = None


@dataclass(frozen=True)
class ValidationRule:
    name: str
    scope: RuleScope
    fn: Callable
    default_arguments: Dict = field(default_factory=dict)


class ValidationRegistry:
    _rules: Dict[str, ValidationRule] = {}

    @classmethod
    def register(
        cls,
        *,
        name: str,
        scope: RuleScope,
        fn: Callable,
        default_arguments: Dict | None = None,
    ):
        cls._rules[name] = ValidationRule(
            name=name,
            scope=scope,
            fn=fn,
            default_arguments=default_arguments or {},
        )

    @classmethod
    def get(cls, name: str) -> ValidationRule:
        try:
            return cls._rules[name]
        except KeyError:
            raise ValueError(f"Unknown validation rule: {name}")

    @classmethod
    def list(cls):
        return {
            list(rules.keys())
            for rules in cls._rules.items()
        }


def validation(
    *,
    name: str,
    scope: RuleScope,
    default_arguments: Dict | None = None,
):
    def decorator(fn):
        ValidationRegistry.register(
            name=name,
            scope=scope,
            fn=fn,
            default_arguments=default_arguments,
        )
        return fn
    return decorator

