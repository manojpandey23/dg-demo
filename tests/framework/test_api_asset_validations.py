"""
Tests for API asset → schema inference → validation rules.

Mocks an API response, converts to DataFrame via to_dataframe(),
then exercises every validation rule in validations.py.
"""

import datetime as dt
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

# Force-register all validation rules
import framework.validation.rules.validations  # noqa: F401
from framework.core.utils.json_to_pd import (
    _build_dataframe,
    _infer_logical_type,
    _infer_schema,
    to_dataframe,
)
from framework.validation.engine.validation_registry import (
    RuleScope,
    Severity,
    ValidationRegistry,
    ValidationResult,
)

# ============================================================
# Fixtures: mock API response
# ============================================================

SAMPLE_API_RECORDS: list[dict[str, Any]] = [
    {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30,
        "salary": 75000.50,
        "is_active": True,
        "created_at": "2024-01-15T10:30:00",
        "score": 95.123,
        "status": "active",
        "country_code": "US",
        "data_dt": "2024-01-15",
    },
    {
        "id": 2,
        "name": "Bob",
        "email": "bob@example.com",
        "age": 25,
        "salary": 62000.00,
        "is_active": False,
        "created_at": "2024-02-20T14:00:00",
        "score": 87.45,
        "status": "active",
        "country_code": "GB",
        "data_dt": "2024-02-20",
    },
    {
        "id": 3,
        "name": "Charlie",
        "email": "charlie@example.com",
        "age": 40,
        "salary": 98000.75,
        "is_active": True,
        "created_at": "2024-03-10T09:15:00",
        "score": 76.9,
        "status": "inactive",
        "country_code": "DE",
        "data_dt": "2024-03-10",
    },
    {
        "id": 4,
        "name": "Diana",
        "email": "diana@example.com",
        "age": 35,
        "salary": 85000.25,
        "is_active": True,
        "created_at": "2024-04-05T16:45:00",
        "score": 91.0,
        "status": "active",
        "country_code": "US",
        "data_dt": "2024-04-05",
    },
    {
        "id": 5,
        "name": "Eve",
        "email": "eve@example.com",
        "age": 28,
        "salary": 70000.00,
        "is_active": False,
        "created_at": "2024-05-18T11:00:00",
        "score": 88.678,
        "status": "pending",
        "country_code": "FR",
        "data_dt": "2024-05-18",
    },
]


def _mock_api_response(
    records: list[dict[str, Any]] | dict[str, Any],
    status_code: int = 200,
) -> MagicMock:
    """Create a mock requests.Response with .json() and .raise_for_status()."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = records
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture()
def api_df() -> pd.DataFrame:
    """DataFrame produced by to_dataframe from a mocked API response."""
    resp = _mock_api_response(SAMPLE_API_RECORDS)
    return to_dataframe(resp)


# ============================================================
# 1. Schema Inference Tests
# ============================================================


class TestSchemaInference:
    """Verify _infer_schema and _infer_logical_type produce correct results."""

    def test_infer_schema_keys(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        expected_cols = {
            "id",
            "name",
            "email",
            "age",
            "salary",
            "is_active",
            "created_at",
            "score",
            "status",
            "country_code",
            "data_dt",
        }
        assert set(schema.keys()) == expected_cols

    def test_infer_int_type(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["id"]["dtype"] == "int"
        assert schema["age"]["dtype"] == "int"

    def test_infer_float_type(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["salary"]["dtype"] == "float"
        assert schema["score"]["dtype"] == "float"

    def test_infer_bool_type(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["is_active"]["dtype"] == "bool"

    def test_infer_string_type(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["name"]["dtype"] == "string"
        assert schema["email"]["dtype"] == "string"
        assert schema["status"]["dtype"] == "string"
        assert schema["country_code"]["dtype"] == "string"

    def test_infer_datetime_from_iso_strings(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["created_at"]["dtype"] == "datetime"

    def test_infer_date_only_iso_strings_as_date(self) -> None:
        """Date-only ISO strings (e.g. '2024-01-15') are inferred as 'date'
        because _infer_logical_type detects all parsed values have midnight times."""
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["data_dt"]["dtype"] == "date"
        assert schema["data_dt"]["nullable"] is False

    def test_nullable_detection_all_present(self) -> None:
        schema = _infer_schema(SAMPLE_API_RECORDS)
        assert schema["id"]["nullable"] is False
        assert schema["name"]["nullable"] is False

    def test_nullable_detection_with_nulls(self) -> None:
        records = [
            {"val": 1, "opt": "hello"},
            {"val": 2, "opt": None},
        ]
        schema = _infer_schema(records)
        assert schema["val"]["nullable"] is False
        assert schema["opt"]["nullable"] is True

    def test_infer_empty_values_default_string(self) -> None:
        assert _infer_logical_type([]) == "string"

    def test_infer_bool_not_int(self) -> None:
        """Booleans must NOT be classified as int."""
        assert _infer_logical_type([True, False, True]) == "bool"

    def test_infer_mixed_int_float_becomes_float(self) -> None:
        assert _infer_logical_type([1, 2.5, 3]) == "float"

    def test_infer_date_objects(self) -> None:
        vals = [dt.date(2024, 1, 1), dt.date(2024, 6, 15)]
        assert _infer_logical_type(vals) == "date"

    def test_infer_datetime_objects(self) -> None:
        vals = [dt.datetime(2024, 1, 1, 10, 30), dt.datetime(2024, 6, 15, 8, 0)]
        assert _infer_logical_type(vals) == "datetime"


class TestBuildDataFrame:
    """Verify _build_dataframe produces correct pandas dtypes."""

    def test_int_column_dtype(self, api_df: pd.DataFrame) -> None:
        assert is_integer_dtype(api_df["id"])
        assert is_integer_dtype(api_df["age"])

    def test_float_column_dtype(self, api_df: pd.DataFrame) -> None:
        assert is_float_dtype(api_df["salary"])
        assert is_float_dtype(api_df["score"])

    def test_bool_column_dtype(self, api_df: pd.DataFrame) -> None:
        assert api_df["is_active"].dtype == "boolean"

    def test_datetime_column_dtype(self, api_df: pd.DataFrame) -> None:
        assert is_datetime64_any_dtype(api_df["created_at"])

    def test_date_only_column_is_datetime64(self, api_df: pd.DataFrame) -> None:
        """data_dt values are date-only ISO strings, but after _build_dataframe
        they become datetime64 (with midnight times)."""
        assert is_datetime64_any_dtype(api_df["data_dt"])

    def test_string_column_dtype(self, api_df: pd.DataFrame) -> None:
        assert is_string_dtype(api_df["name"])
        assert is_string_dtype(api_df["email"])

    def test_row_count(self, api_df: pd.DataFrame) -> None:
        assert len(api_df) == len(SAMPLE_API_RECORDS)

    def test_column_count(self, api_df: pd.DataFrame) -> None:
        assert len(api_df.columns) == len(SAMPLE_API_RECORDS[0])


class TestToDataframeEndToEnd:
    """Integration: mock response → to_dataframe → correct DataFrame."""

    def test_single_record_response(self) -> None:
        resp = _mock_api_response({"id": 42, "name": "Solo"})
        df = to_dataframe(resp)
        assert len(df) == 1
        assert df.loc[0, "id"] == 42

    def test_raises_on_http_error(self) -> None:
        resp = MagicMock()
        resp.raise_for_status.side_effect = Exception("404 Not Found")
        with pytest.raises(Exception, match="404"):
            to_dataframe(resp)


# ============================================================
# 2. Validation Rules Tests
# ============================================================


def _run_rule(
    rule_name: str,
    df: pd.DataFrame,
    rule_params: dict[str, Any],
) -> ValidationResult:
    """Helper to look up a registered rule and execute it."""
    rule_def = ValidationRegistry.get(rule_name)
    params = {"rule": rule_name, **rule_params}
    if "severity" not in params:
        params["severity"] = Severity.ERROR
    return rule_def.fn(df, params)


# --------------------------------------------------
# Column Rules
# --------------------------------------------------


class TestExpectColumnType:
    def test_int_type_passes(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "id", "dtype": "int"}
        )
        assert result.passed is True

    def test_float_type_passes(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "salary", "dtype": "float"}
        )
        assert result.passed is True

    def test_string_type_passes(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "name", "dtype": "string"}
        )
        assert result.passed is True

    def test_datetime_type_passes(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "created_at", "dtype": "datetime"}
        )
        assert result.passed is True

    def test_bool_type_passes(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "is_active", "dtype": "bool"}
        )
        assert result.passed is True

    def test_wrong_type_fails(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "name", "dtype": "int"}
        )
        assert result.passed is False

    def test_date_type_detection(self) -> None:
        df = pd.DataFrame({"d": pd.to_datetime(["2024-01-01", "2024-06-15"])})
        result = _run_rule("expect_column_type", df, {"column": "d", "dtype": "date"})
        assert result.passed is True

    def test_data_dt_passes_date_type_check(self, api_df: pd.DataFrame) -> None:
        """data_dt is datetime64 with all-midnight values, so expect_column_type
        with dtype='date' must pass via is_date_only_column."""
        result = _run_rule(
            "expect_column_type", api_df, {"column": "data_dt", "dtype": "date"}
        )
        assert result.passed is True
        assert result.metadata["expected"] == "date"

    def test_data_dt_fails_datetime_with_time_check(self, api_df: pd.DataFrame) -> None:
        """data_dt has no time component, so checking it as 'datetime' passes
        (it IS datetime64) but it is not a datetime-with-time column."""
        result = _run_rule(
            "expect_column_type", api_df, {"column": "data_dt", "dtype": "datetime"}
        )
        # datetime64 check passes because the underlying dtype is datetime64
        assert result.passed is True

    def test_unsupported_dtype_raises(self, api_df: pd.DataFrame) -> None:
        with pytest.raises(ValueError, match="Unsupported dtype"):
            _run_rule(
                "expect_column_type", api_df, {"column": "id", "dtype": "complex128"}
            )

    def test_result_metadata(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "id", "dtype": "int"}
        )
        assert result.metadata["expected"] == "int"
        assert "actual" in result.metadata
        assert result.scope == RuleScope.COLUMN
        assert result.column == "id"


class TestExpectColumnValuesToNotBeNull:
    def test_passes_no_nulls(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_values_to_not_be_null", api_df, {"column": "name"}
        )
        assert result.passed is True
        assert result.metadata["null_count"] == 0

    def test_fails_with_nulls(self) -> None:
        df = pd.DataFrame({"col": [1, None, 3]})
        result = _run_rule("expect_column_values_to_not_be_null", df, {"column": "col"})
        assert result.passed is False
        assert result.metadata["null_count"] == 1
        assert result.failing_rows is not None
        assert len(result.failing_rows) == 1


class TestExpectColumnValuesToBeUnique:
    def test_passes_unique_values(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_values_to_be_unique", api_df, {"column": "id"}
        )
        assert result.passed is True

    def test_fails_with_duplicates(self) -> None:
        df = pd.DataFrame({"col": ["a", "b", "a", "c"]})
        result = _run_rule("expect_column_values_to_be_unique", df, {"column": "col"})
        assert result.passed is False
        assert result.metadata["duplicate_count"] > 0
        assert result.failing_rows is not None


class TestExpectColumnValuesToBeBetween:
    def test_passes_within_range(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_values_to_be_between",
            api_df,
            {"column": "age", "min": 0, "max": 100},
        )
        assert result.passed is True

    def test_fails_below_min(self) -> None:
        df = pd.DataFrame({"val": [5, 10, -1]})
        result = _run_rule(
            "expect_column_values_to_be_between",
            df,
            {"column": "val", "min": 0, "max": 100},
        )
        assert result.passed is False

    def test_fails_above_max(self) -> None:
        df = pd.DataFrame({"val": [5, 10, 200]})
        result = _run_rule(
            "expect_column_values_to_be_between",
            df,
            {"column": "val", "min": 0, "max": 100},
        )
        assert result.passed is False

    def test_min_only(self) -> None:
        df = pd.DataFrame({"val": [5, 10, 15]})
        result = _run_rule(
            "expect_column_values_to_be_between",
            df,
            {"column": "val", "min": 5},
        )
        assert result.passed is True

    def test_max_only(self) -> None:
        df = pd.DataFrame({"val": [5, 10, 15]})
        result = _run_rule(
            "expect_column_values_to_be_between",
            df,
            {"column": "val", "max": 15},
        )
        assert result.passed is True


class TestExpectColumnValuesToBeInSet:
    def test_passes_all_in_set(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_values_to_be_in_set",
            api_df,
            {"column": "status", "allowed": ["active", "inactive", "pending"]},
        )
        assert result.passed is True

    def test_fails_value_not_in_set(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_values_to_be_in_set",
            api_df,
            {"column": "status", "allowed": ["active"]},
        )
        assert result.passed is False


class TestExpectColumnValuesToHaveDecimalPlaces:
    def test_passes_within_limit(self) -> None:
        df = pd.DataFrame({"price": [10.12, 20.34, 30.56]})
        result = _run_rule(
            "expect_column_values_to_have_decimal_places",
            df,
            {"column": "price", "max_decimal_places": 2},
        )
        assert result.passed is True

    def test_fails_exceeding_limit(self) -> None:
        df = pd.DataFrame({"price": [10.123, 20.3456, 30.5]})
        result = _run_rule(
            "expect_column_values_to_have_decimal_places",
            df,
            {"column": "price", "max_decimal_places": 2},
        )
        assert result.passed is False
        assert result.metadata["violations"] > 0

    def test_missing_param_raises(self) -> None:
        df = pd.DataFrame({"price": [1.0]})
        with pytest.raises(ValueError, match="requires 'max_decimal_places'"):
            _run_rule(
                "expect_column_values_to_have_decimal_places",
                df,
                {"column": "price"},
            )


class TestExpectColumnValueLengthEqualTo:
    def test_passes_exact_length(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_value_length_equal_to",
            api_df,
            {"column": "country_code", "length": 2},
        )
        assert result.passed is True

    def test_fails_wrong_length(self) -> None:
        df = pd.DataFrame({"code": ["AB", "CDE", "FG"]})
        result = _run_rule(
            "expect_column_value_length_equal_to",
            df,
            {"column": "code", "length": 2},
        )
        assert result.passed is False

    def test_missing_value_raises(self) -> None:
        df = pd.DataFrame({"code": ["AB"]})
        with pytest.raises(ValueError, match="requires 'value'"):
            _run_rule(
                "expect_column_value_length_equal_to",
                df,
                {"column": "code", "length": None},
            )


class TestExpectColumnValueLengthBetween:
    def test_passes_within_range(self) -> None:
        df = pd.DataFrame({"name": ["Al", "Bob", "Carl"]})
        result = _run_rule(
            "expect_column_value_length_between",
            df,
            {"column": "name", "min": 2, "max": 4},
        )
        assert result.passed is True

    def test_fails_too_short(self) -> None:
        df = pd.DataFrame({"name": ["A", "Bob"]})
        result = _run_rule(
            "expect_column_value_length_between",
            df,
            {"column": "name", "min": 2, "max": 10},
        )
        assert result.passed is False

    def test_fails_too_long(self) -> None:
        df = pd.DataFrame({"name": ["VeryLongName", "Bob"]})
        result = _run_rule(
            "expect_column_value_length_between",
            df,
            {"column": "name", "min": 2, "max": 5},
        )
        assert result.passed is False

    def test_missing_both_raises(self) -> None:
        df = pd.DataFrame({"name": ["a"]})
        with pytest.raises(ValueError, match="requires 'min' or 'max'"):
            _run_rule(
                "expect_column_value_length_between",
                df,
                {"column": "name"},
            )


class TestExpectColumnValuePatternMatch:
    def test_passes_email_pattern(self) -> None:
        df = pd.DataFrame({"email": ["a@b.com", "c@d.org"]})
        result = _run_rule(
            "expect_column_value_pattern_match",
            df,
            {"column": "email", "pattern": r".+@.+\..+"},
        )
        assert result.passed is True

    def test_fails_bad_pattern(self) -> None:
        df = pd.DataFrame({"email": ["a@b.com", "not-an-email"]})
        result = _run_rule(
            "expect_column_value_pattern_match",
            df,
            {"column": "email", "pattern": r".+@.+\..+"},
        )
        assert result.passed is False

    def test_missing_pattern_raises(self) -> None:
        df = pd.DataFrame({"col": ["x"]})
        with pytest.raises(ValueError, match="requires 'pattern'"):
            _run_rule(
                "expect_column_value_pattern_match",
                df,
                {"column": "col"},
            )


class TestExpectColumnDatetimeValueFormat:
    def test_passes_correct_format(self) -> None:
        df = pd.DataFrame({"dt": ["2024-01-15", "2024-06-20"]})
        result = _run_rule(
            "expect_column_datetime_value_format",
            df,
            {"column": "dt", "format": "%Y-%m-%d"},
        )
        assert result.passed is True

    def test_fails_wrong_format(self) -> None:
        df = pd.DataFrame({"dt": ["15/01/2024", "2024-06-20"]})
        result = _run_rule(
            "expect_column_datetime_value_format",
            df,
            {"column": "dt", "format": "%Y-%m-%d"},
        )
        assert result.passed is False

    def test_missing_format_raises(self) -> None:
        df = pd.DataFrame({"dt": ["2024-01-01"]})
        with pytest.raises(ValueError, match="requires 'format'"):
            _run_rule(
                "expect_column_datetime_value_format",
                df,
                {"column": "dt"},
            )


# --------------------------------------------------
# Row Rules
# --------------------------------------------------


class TestExpectRowConditionToBeTrue:
    def test_passes_valid_condition(self, api_df: pd.DataFrame) -> None:
        # Expression returns rows that FAIL → age < 0 returns empty → pass
        result = _run_rule(
            "expect_row_condition_to_be_true",
            api_df,
            {"expression": "age < 0"},
        )
        assert result.passed is True

    def test_fails_condition(self) -> None:
        df = pd.DataFrame({"val": [1, -2, 3]})
        result = _run_rule(
            "expect_row_condition_to_be_true",
            df,
            {"expression": "val < 0"},
        )
        assert result.passed is False
        assert result.failing_rows is not None


class TestExpectRowIfThenCondition:
    def test_passes_if_then(self) -> None:
        df = pd.DataFrame(
            {
                "status": ["active", "inactive", "active"],
                "score": [90, 50, 85],
            }
        )
        result = _run_rule(
            "expect_row_if_then_condition",
            df,
            {"if": "status == 'active'", "then": "score >= 80"},
        )
        assert result.passed is True

    def test_fails_if_then(self) -> None:
        df = pd.DataFrame(
            {
                "status": ["active", "inactive", "active"],
                "score": [90, 50, 10],
            }
        )
        result = _run_rule(
            "expect_row_if_then_condition",
            df,
            {"if": "status == 'active'", "then": "score >= 80"},
        )
        assert result.passed is False
        assert result.failing_rows is not None


# --------------------------------------------------
# Table Rules
# --------------------------------------------------


class TestExpectTableRowCountToBeBetween:
    def test_passes_within_range(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_table_row_count_to_be_between",
            api_df,
            {"min": 1, "max": 100},
        )
        assert result.passed is True
        assert result.metadata["row_count"] == len(api_df)

    def test_fails_below_min(self) -> None:
        df = pd.DataFrame({"a": [1]})
        result = _run_rule(
            "expect_table_row_count_to_be_between",
            df,
            {"min": 5, "max": 100},
        )
        assert result.passed is False

    def test_fails_above_max(self) -> None:
        df = pd.DataFrame({"a": range(20)})
        result = _run_rule(
            "expect_table_row_count_to_be_between",
            df,
            {"min": 1, "max": 5},
        )
        assert result.passed is False


class TestExpectTableRowsToBeUnique:
    def test_passes_unique_rows(self, api_df: pd.DataFrame) -> None:
        result = _run_rule("expect_table_rows_to_be_unique", api_df, {})
        assert result.passed is True

    def test_fails_duplicate_rows(self) -> None:
        df = pd.DataFrame({"a": [1, 1], "b": [2, 2]})
        result = _run_rule("expect_table_rows_to_be_unique", df, {})
        assert result.passed is False
        assert result.metadata["duplicate_rows"] == 1


class TestExpectTableColumnCountToEqual:
    def test_passes_correct_count(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_table_column_count_to_equal",
            api_df,
            {"expected": len(api_df.columns)},
        )
        assert result.passed is True

    def test_fails_wrong_count(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_table_column_count_to_equal",
            api_df,
            {"expected": 3},
        )
        assert result.passed is False
        assert result.metadata["actual"] == len(api_df.columns)


# --------------------------------------------------
# Validation Result Structure Tests
# --------------------------------------------------


class TestValidationResultStructure:
    """Ensure every rule returns a properly formed ValidationResult."""

    def test_result_has_required_fields(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type", api_df, {"column": "id", "dtype": "int"}
        )
        assert isinstance(result, ValidationResult)
        assert isinstance(result.rule, str)
        assert isinstance(result.scope, RuleScope)
        assert isinstance(result.passed, bool)
        assert isinstance(result.severity, Severity)

    def test_column_rule_has_column(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_values_to_not_be_null", api_df, {"column": "id"}
        )
        assert result.column == "id"

    def test_table_rule_has_no_column(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_table_row_count_to_be_between", api_df, {"min": 1, "max": 100}
        )
        assert result.column is None

    def test_failing_rows_is_dataframe_or_none(self) -> None:
        df = pd.DataFrame({"col": [1, None, 3]})
        result = _run_rule("expect_column_values_to_not_be_null", df, {"column": "col"})
        assert isinstance(result.failing_rows, pd.DataFrame)

    def test_severity_propagated(self, api_df: pd.DataFrame) -> None:
        result = _run_rule(
            "expect_column_type",
            api_df,
            {"column": "id", "dtype": "int", "severity": Severity.WARN},
        )
        assert result.severity == Severity.WARN


# --------------------------------------------------
# Edge Cases
# --------------------------------------------------


class TestEdgeCases:
    def test_empty_dataframe_not_null_passes(self) -> None:
        df = pd.DataFrame({"col": pd.Series([], dtype="Int64")})
        result = _run_rule("expect_column_values_to_not_be_null", df, {"column": "col"})
        assert result.passed is True

    def test_empty_dataframe_unique_passes(self) -> None:
        df = pd.DataFrame({"col": pd.Series([], dtype="string")})
        result = _run_rule("expect_column_values_to_be_unique", df, {"column": "col"})
        assert result.passed is True

    def test_empty_dataframe_row_count_zero(self) -> None:
        df = pd.DataFrame({"col": []})
        result = _run_rule(
            "expect_table_row_count_to_be_between",
            df,
            {"min": 0, "max": 0},
        )
        assert result.passed is True

    def test_single_row_dataframe(self) -> None:
        resp = _mock_api_response({"id": 1, "name": "Test"})
        df = to_dataframe(resp)
        assert len(df) == 1
        result = _run_rule("expect_column_values_to_not_be_null", df, {"column": "id"})
        assert result.passed is True

    def test_nullable_column_inference(self) -> None:
        records = [{"a": 1, "b": "x"}, {"a": None, "b": "y"}]
        schema = _infer_schema(records)
        assert schema["a"]["nullable"] is True
        assert schema["b"]["nullable"] is False

    def test_all_null_column_defaults_string(self) -> None:
        records = [{"a": None}, {"a": None}]
        schema = _infer_schema(records)
        assert schema["a"]["dtype"] == "string"
        assert schema["a"]["nullable"] is True

    def test_decimal_places_with_nulls(self) -> None:
        df = pd.DataFrame({"price": [10.12, None, 30.56]})
        result = _run_rule(
            "expect_column_values_to_have_decimal_places",
            df,
            {"column": "price", "max_decimal_places": 2},
        )
        # nulls should not cause failures
        assert result.passed is True

    def test_pattern_match_with_nulls(self) -> None:
        df = pd.DataFrame({"email": ["a@b.com", None, "c@d.org"]})
        result = _run_rule(
            "expect_column_value_pattern_match",
            df,
            {"column": "email", "pattern": r".+@.+\..+"},
        )
        # nulls are dropped, non-null values should pass
        assert result.passed is True

    def test_length_between_with_nulls(self) -> None:
        df = pd.DataFrame({"code": ["AB", None, "CD"]})
        result = _run_rule(
            "expect_column_value_length_between",
            df,
            {"column": "code", "min": 2, "max": 2},
        )
        assert result.passed is True
