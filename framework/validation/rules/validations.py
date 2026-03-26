import datetime as dt
import decimal
import importlib
from time import strptime

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

from framework.validation.engine.validation_registry import (
    RuleScope,
    Severity,
    ValidationResult,
    validation,
)

# ============================================================
# COLUMN RULES (Great Expectations style)
# ============================================================


def is_date_only_column(df: pd.DataFrame, column: str) -> bool:

    series = df[column].dropna()

    if series.empty:
        return False

    # Case 1: Proper pandas datetime
    if is_datetime64_any_dtype(series):
        return (series == series.dt.normalize()).all()

    # Case 2: Python datetime.date (common from DB drivers)
    if series.map(type).eq(dt.date).all():
        return True

    # Case 3: String-like dates → attempt strict coercion
    try:
        parsed = pd.to_datetime(series, errors="raise")
    except Exception:
        return False

    return (parsed == parsed.dt.normalize()).all()


def _is_time_only_column(df: pd.DataFrame, column: str) -> bool:
    """
    True if datetime column AND at least one non‑midnight time exists
    """
    if not is_datetime64_any_dtype(df[column]):
        return False

    series = df[column].dropna()
    if series.empty:
        return False

    return (series != series.dt.normalize()).any()


@validation(
    name="expect_column_type",
    scope=RuleScope.COLUMN,
)
def expect_column_type(df, rule) -> ValidationResult:
    col = rule["column"]
    expected = rule["dtype"]
    severity = rule["severity"]

    series = df[col]

    if expected == "int":
        passed = bool(is_integer_dtype(series))

    elif expected == "float":
        passed = bool(is_float_dtype(series))

    elif expected == "string":
        passed = bool(is_string_dtype(series))

    elif expected == "datetime":
        passed = bool(is_datetime64_any_dtype(series))

    elif expected == "bool":
        passed = bool(is_bool_dtype(series))

    elif expected == "date":
        try:
            passed = bool(is_date_only_column(df, col))
        except Exception:
            passed = False

    elif expected == "time":
        try:
            passed = bool(_is_time_only_column(df, col))
        except Exception:
            passed = False

    else:
        raise ValueError(f"Unsupported dtype expectation: {expected}")

    return ValidationResult(
        rule="expect_column_type",
        scope=RuleScope.COLUMN,
        column=col,
        passed=passed,
        severity=severity,
        metadata={
            "expected": expected,
            "actual": str(series.dtype),
        },
    )


@validation(
    name="expect_column_values_to_not_be_null",
    scope=RuleScope.COLUMN,
)
def expect_column_values_to_not_be_null(df, rule) -> ValidationResult:
    col = rule["column"]
    severity = rule["severity"]

    mask = df[col].isnull()

    return ValidationResult(
        rule="expect_column_values_to_not_be_null",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={"null_count": int(mask.sum())},
        failing_rows=df[mask].head(),
    )


@validation(
    name="expect_column_values_to_be_unique",
    scope=RuleScope.COLUMN,
)
def expect_column_values_to_be_unique(df, rule) -> ValidationResult:
    col = rule["column"]
    severity = rule["severity"]

    mask = df[col].duplicated()

    return ValidationResult(
        rule="expect_column_values_to_be_unique",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={"duplicate_count": int(mask.sum())},
        failing_rows=df[mask].head(),
    )


@validation(
    name="expect_column_values_to_be_between",
    scope=RuleScope.COLUMN,
)
def expect_column_values_to_be_between(df, rule) -> ValidationResult:
    col = rule["column"]
    min_v = rule.get("min")
    max_v = rule.get("max")
    severity = rule["severity"]

    s = df[col]
    mask = False
    if min_v is not None:
        mask |= s < min_v
    if max_v is not None:
        mask |= s > max_v

    return ValidationResult(
        rule="expect_column_values_to_be_between",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={"min": min_v, "max": max_v},
        failing_rows=df[mask].head(),
    )


@validation(
    name="expect_column_values_to_be_in_set",
    scope=RuleScope.COLUMN,
)
def expect_column_values_to_be_in_set(df, rule) -> ValidationResult:
    col = rule["column"]
    allowed = set(rule["allowed"])
    severity = rule["severity"]

    mask = ~df[col].astype(str).isin(set(map(str, allowed)))

    return ValidationResult(
        rule="expect_column_values_to_be_in_set",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={"allowed": list(allowed)},
        failing_rows=df[mask].head(),
    )


@validation(
    name="expect_column_values_to_have_decimal_places",
    scope=RuleScope.COLUMN,
)
def expect_column_values_to_have_decimal_places(df, rule) -> ValidationResult:
    col = rule["column"]
    max_decimals = rule.get("max_decimal_places")
    severity = rule["severity"]

    if max_decimals is None:
        raise ValueError(
            "expect_column_values_to_have_decimal_places requires 'max_decimal_places'"
        )

    series = df[col]

    def decimal_places(value) -> int | None:
        if pd.isna(value):
            return None
        try:
            d = decimal.Decimal(str(value))
            return max(-d.as_tuple().exponent, 0)
        except (decimal.InvalidOperation, ValueError):
            return None

    decimals = series.apply(decimal_places)

    # Fail if decimal places exceed max
    mask = decimals > max_decimals

    return ValidationResult(
        rule="expect_column_values_to_have_decimal_places",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={
            "max_decimal_places": max_decimals,
            "violations": int(mask.sum()),
        },
        failing_rows=df[mask].head(),
    )


@validation(
    name="expect_column_value_length_equal_to",
    scope=RuleScope.COLUMN,
)
def expect_column_value_length_equal_to(df, rule) -> ValidationResult:
    col = rule["column"]
    expected = rule["length"]
    severity = rule["severity"]

    if expected is None:
        raise ValueError("expect_column_value_length_equal_to requires 'value'")

    series = df[col].dropna().astype(str)
    length = series.str.len()
    mask = length != expected

    return ValidationResult(
        rule="expect_column_value_length_equal_to",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={
            "expected_length": expected,
            "actual_length": length,
            "violations": int(mask.sum()),
        },
        failing_rows=df.loc[series.index[mask]].head(),
    )


@validation(
    name="expect_column_value_length_between",
    scope=RuleScope.COLUMN,
)
def expect_column_value_length_between(df, rule) -> ValidationResult:
    col = rule["column"]
    min_v = rule.get("min")
    max_v = rule.get("max")
    severity = rule["severity"]

    if min_v is None and max_v is None:
        raise ValueError("expect_column_value_length_between requires 'min' or 'max'")

    series = df[col].dropna().astype(str)
    lengths = series.str.len()

    mask = False
    if min_v is not None:
        mask |= lengths < min_v
    if max_v is not None:
        mask |= lengths > max_v

    return ValidationResult(
        rule="expect_column_value_length_between",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={
            "min": min_v,
            "max": max_v,
            "violations": int(mask.sum()),
        },
        failing_rows=df.loc[series.index[mask]].head(),
    )


@validation(
    name="expect_column_value_pattern_match",
    scope=RuleScope.COLUMN,
)
def expect_column_value_pattern_match(df, rule) -> ValidationResult:
    col = rule["column"]
    pattern = rule.get("pattern")
    severity = rule["severity"]

    if not pattern:
        raise ValueError("expect_column_value_pattern_match requires 'pattern'")

    series = df[col].dropna().astype(str)
    mask = ~series.str.match(pattern, na=False)

    return ValidationResult(
        rule="expect_column_value_pattern_match",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={
            "pattern": pattern,
            "violations": int(mask.sum()),
        },
        failing_rows=df.loc[series.index[mask]].head(),
    )


@validation(
    name="expect_column_datetime_value_format",
    scope=RuleScope.COLUMN,
)
def expect_column_datetime_value_format(df, rule) -> ValidationResult:
    col = rule["column"]
    fmt = rule.get("format")
    severity = rule["severity"]

    if not fmt:
        raise ValueError("expect_column_date_value_format requires 'format'")

    series = df[col].dropna().astype(str)

    def matches_format(value: str) -> bool:
        try:
            strptime(value, fmt)
            return True
        except ValueError:
            return False

    mask = ~series.apply(matches_format)

    return ValidationResult(
        rule="expect_column_date_value_format",
        scope=RuleScope.COLUMN,
        column=col,
        passed=not mask.any(),
        severity=severity,
        metadata={
            "format": fmt,
            "violations": int(mask.sum()),
        },
        failing_rows=df.loc[series.index[mask]].head(),
    )


# ============================================================
# ROW RULES
# ============================================================


@validation(
    name="expect_row_condition_to_be_true",
    scope=RuleScope.ROW,
)
def expect_row_condition_to_be_true(df, rule) -> ValidationResult:
    expr = rule["expression"]
    severity = rule["severity"]

    failed = df.query(expr)

    return ValidationResult(
        rule="expect_row_condition_to_be_true",
        scope=RuleScope.ROW,
        passed=failed.empty,
        severity=severity,
        metadata={"expression": expr},
        failing_rows=failed.head(),
    )


@validation(
    name="expect_row_if_then_condition",
    scope=RuleScope.ROW,
)
def expect_row_if_then_condition(df, rule) -> ValidationResult:
    if_expr = rule["if"]
    then_expr = rule["then"]
    severity = rule["severity"]

    failed = df.query(f"({if_expr}) & ~({then_expr})")

    return ValidationResult(
        rule="expect_row_if_then_condition",
        scope=RuleScope.ROW,
        passed=failed.empty,
        severity=severity,
        metadata={"if": if_expr, "then": then_expr},
        failing_rows=failed.head(),
    )


# ============================================================
# TABLE RULES
# ============================================================


@validation(
    name="expect_table_row_count_to_be_between",
    scope=RuleScope.TABLE,
)
def expect_table_row_count_to_be_between(df, rule) -> ValidationResult:
    min_v = rule.get("min")
    max_v = rule.get("max")
    severity = rule["severity"]

    count = len(df)
    passed = True

    if min_v is not None and count < min_v:
        passed = False
    if max_v is not None and count > max_v:
        passed = False

    return ValidationResult(
        rule="expect_table_row_count_to_be_between",
        scope=RuleScope.TABLE,
        passed=passed,
        severity=severity,
        metadata={"row_count": count, "min": min_v, "max": max_v},
    )


@validation(
    name="expect_table_rows_to_be_unique",
    scope=RuleScope.TABLE,
)
def expect_table_rows_to_be_unique(df, rule) -> ValidationResult:
    severity = rule["severity"]
    dup_count = int(df.duplicated().sum())

    return ValidationResult(
        rule="expect_table_rows_to_be_unique",
        scope=RuleScope.TABLE,
        passed=dup_count == 0,
        severity=severity,
        metadata={"duplicate_rows": dup_count},
    )


@validation(
    name="expect_table_column_count_to_equal",
    scope=RuleScope.TABLE,
)
def expect_table_column_count_to_equal(df, rule) -> ValidationResult:
    expected = rule["expected"]
    severity = rule["severity"]

    actual = len(df.columns)

    return ValidationResult(
        rule="expect_table_column_count_to_equal",
        scope=RuleScope.TABLE,
        passed=actual == expected,
        severity=severity,
        metadata={"expected": expected, "actual": actual},
    )


# ============================================================
# PLUGIN RULE
# ============================================================


@validation(
    name="expect_plugin_validation",
    scope=RuleScope.TABLE,
)
def expect_plugin_validation(df, rule) -> ValidationResult:
    plugin_fn = rule["plugin"]
    args = rule.get("args", {})
    severity = rule["severity"]

    try:
        module = importlib.import_module("validation.validation_plugins")

        fn = getattr(module, plugin_fn)
        passed, payload = fn(df, **args)

        return ValidationResult(
            rule="expect_plugin_validation",
            scope=RuleScope.TABLE,
            passed=passed,
            severity=severity,
            metadata=payload if isinstance(payload, dict) else {"message": payload},
        )

    except Exception as e:
        return ValidationResult(
            rule="expect_plugin_validation",
            scope=RuleScope.TABLE,
            passed=False,
            severity=Severity.ERROR,
            metadata={"exception": str(e)},
        )
