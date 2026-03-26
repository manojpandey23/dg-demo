"""
Tests for file_asset — validates reading CSV, JSON, and Excel files.

Creates real test fixture files, builds the Dagster asset via
handle_file_asset, mocks the execution context, and validates
the resulting DataFrames (schema, row counts, file_name column,
dtype inference, edge cases).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest
from pandas.api.types import (
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

from framework.core.asserts.assert_file import handle_file_asset
from framework.model.config_models import AssetConfig

# ============================================================
# Shared test data
# ============================================================

SAMPLE_ROWS: list[dict[str, Any]] = [
    {"account_cd": "ACC001", "amount": 1500.50, "ccy": "USD", "date": "2026-03-21"},
    {"account_cd": "ACC002", "amount": 2300.00, "ccy": "EUR", "date": "2026-03-22"},
    {"account_cd": "ACC003", "amount": 780.25, "ccy": "GBP", "date": "2026-03-23"},
]

EXPECTED_COLUMNS = {"account_cd", "amount", "ccy", "date", "file_name"}
EXPECTED_ROW_COUNT = len(SAMPLE_ROWS)

# ============================================================
# Fixtures
# ============================================================

TEST_FILES_DIR = Path(__file__).parent / "test_files"


@pytest.fixture(autouse=True)
def _ensure_test_dir() -> None:
    TEST_FILES_DIR.mkdir(parents=True, exist_ok=True)


@pytest.fixture()
def csv_file() -> Path:
    """Create a standard UTF-8 CSV fixture."""
    path = TEST_FILES_DIR / "test_data.csv"
    pd.DataFrame(SAMPLE_ROWS).to_csv(path, index=False)
    return path


@pytest.fixture()
def json_file() -> Path:
    """Create a JSON fixture (array of records)."""
    path = TEST_FILES_DIR / "test_data.json"
    path.write_text(json.dumps(SAMPLE_ROWS, indent=2))
    return path


@pytest.fixture()
def excel_file() -> Path:
    """Create an Excel (.xlsx) fixture."""
    path = TEST_FILES_DIR / "test_data.xlsx"
    pd.DataFrame(SAMPLE_ROWS).to_excel(path, index=False, engine="openpyxl")
    return path


@pytest.fixture()
def latin1_csv_file() -> Path:
    """Create a CSV encoded in latin-1 with non-ASCII characters."""
    path = TEST_FILES_DIR / "test_latin1.csv"
    df = pd.DataFrame([
        {"name": "José", "city": "São Paulo", "amount": 100.0},
        {"name": "François", "city": "Zürich", "amount": 200.0},
    ])
    df.to_csv(path, index=False, encoding="latin-1")
    return path


@pytest.fixture()
def empty_csv_file() -> Path:
    """Create a CSV with headers only (zero data rows)."""
    path = TEST_FILES_DIR / "test_empty.csv"
    path.write_text("account_cd,amount,ccy,date\n")
    return path


@pytest.fixture()
def unsupported_file() -> Path:
    """Create an unsupported file type."""
    path = TEST_FILES_DIR / "test_data.parquet"
    path.write_text("not a real parquet")
    return path


# ============================================================
# Helper: build and invoke the file asset
# ============================================================


def _build_file_asset_config(name: str = "test_file_asset") -> AssetConfig:
    """Build a minimal AssetConfig for a file-type asset."""
    return AssetConfig(
        name=name,
        type="file",
        partition_name=f"{name}_partition",
    )


def _invoke_file_asset(file_path: Path) -> pd.DataFrame:
    """Build the asset, create a Dagster test context, and execute it.

    Returns the DataFrame produced by the asset function.
    """
    config = _build_file_asset_config()
    asset_fn = handle_file_asset(config, asset_deps={})

    context = dg.build_asset_context(partition_key=str(file_path))
    return asset_fn(context)


def _invoke_file_asset_with_mock_context(
    file_path: Path,
) -> tuple[pd.DataFrame, MagicMock]:
    """Same as _invoke_file_asset but returns the mock context too
    for metadata / logging assertions."""
    config = _build_file_asset_config()
    asset_fn = handle_file_asset(config, asset_deps={})

    # Use a real Dagster context but wrap log/metadata with spies
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.partition_key = str(file_path)
    context.log = MagicMock()
    context.add_output_metadata = MagicMock()

    # Call the raw (unwrapped) function — bypass Dagster decorator
    # The inner fn is stored on the asset definition's compute_fn
    inner_fn = asset_fn.op.compute_fn.decorated_fn
    df = inner_fn(context)
    return df, context


# ============================================================
# 1. CSV Tests
# ============================================================


class TestCSVFileAsset:

    def test_reads_csv_successfully(self, csv_file: Path) -> None:
        df = _invoke_file_asset(csv_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == EXPECTED_ROW_COUNT

    def test_csv_has_all_columns(self, csv_file: Path) -> None:
        df = _invoke_file_asset(csv_file)
        assert set(df.columns) == EXPECTED_COLUMNS

    def test_csv_file_name_column(self, csv_file: Path) -> None:
        df = _invoke_file_asset(csv_file)
        assert (df["file_name"] == csv_file.name).all()

    def test_csv_data_values(self, csv_file: Path) -> None:
        df = _invoke_file_asset(csv_file)
        assert df.loc[0, "account_cd"] == "ACC001"
        assert df.loc[1, "amount"] == 2300.00
        assert df.loc[2, "ccy"] == "GBP"

    def test_csv_dtypes(self, csv_file: Path) -> None:
        df = _invoke_file_asset(csv_file)
        assert is_float_dtype(df["amount"])
        # account_cd and ccy are object (str) in CSV reads
        assert df["account_cd"].dtype == object or is_string_dtype(df["account_cd"])

    def test_csv_metadata_emitted(self, csv_file: Path) -> None:
        df, context = _invoke_file_asset_with_mock_context(csv_file)

        context.add_output_metadata.assert_called_once()
        metadata = context.add_output_metadata.call_args[0][0]
        assert metadata["file_path"] == str(csv_file)
        assert metadata["rows"] == EXPECTED_ROW_COUNT
        assert metadata["file_type"] == ".csv"
        assert "account_cd" in metadata["columns"]


# ============================================================
# 2. JSON Tests
# ============================================================


class TestJSONFileAsset:

    def test_reads_json_successfully(self, json_file: Path) -> None:
        df = _invoke_file_asset(json_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == EXPECTED_ROW_COUNT

    def test_json_has_all_columns(self, json_file: Path) -> None:
        df = _invoke_file_asset(json_file)
        assert set(df.columns) == EXPECTED_COLUMNS

    def test_json_file_name_column(self, json_file: Path) -> None:
        df = _invoke_file_asset(json_file)
        assert (df["file_name"] == json_file.name).all()

    def test_json_data_values(self, json_file: Path) -> None:
        df = _invoke_file_asset(json_file)
        assert df.loc[0, "account_cd"] == "ACC001"
        assert df.loc[2, "amount"] == 780.25


# ============================================================
# 3. Excel Tests
# ============================================================


class TestExcelFileAsset:

    def test_reads_xlsx_successfully(self, excel_file: Path) -> None:
        df = _invoke_file_asset(excel_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == EXPECTED_ROW_COUNT

    def test_xlsx_has_all_columns(self, excel_file: Path) -> None:
        df = _invoke_file_asset(excel_file)
        assert set(df.columns) == EXPECTED_COLUMNS

    def test_xlsx_file_name_column(self, excel_file: Path) -> None:
        df = _invoke_file_asset(excel_file)
        assert (df["file_name"] == excel_file.name).all()

    def test_xlsx_data_values(self, excel_file: Path) -> None:
        df = _invoke_file_asset(excel_file)
        assert df.loc[1, "ccy"] == "EUR"
        assert df.loc[0, "amount"] == 1500.50


# ============================================================
# 4. Encoding Tests
# ============================================================


class TestEncodingFallback:

    def test_latin1_fallback(self, latin1_csv_file: Path) -> None:
        """UTF-8 decode should fail, latin-1 fallback should succeed."""
        df = _invoke_file_asset(latin1_csv_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_latin1_data_preserved(self, latin1_csv_file: Path) -> None:
        df = _invoke_file_asset(latin1_csv_file)
        assert "José" in df["name"].values
        assert "São Paulo" in df["city"].values

    def test_latin1_warning_logged(self, latin1_csv_file: Path) -> None:
        df, context = _invoke_file_asset_with_mock_context(latin1_csv_file)

        context.log.warning.assert_called_once()
        assert "latin1" in context.log.warning.call_args[0][0].lower()


# ============================================================
# 5. Edge Cases
# ============================================================


class TestEdgeCases:

    def test_empty_csv_returns_empty_dataframe(self, empty_csv_file: Path) -> None:
        df = _invoke_file_asset(empty_csv_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "account_cd" in df.columns

    def test_empty_csv_has_file_name_column(self, empty_csv_file: Path) -> None:
        df = _invoke_file_asset(empty_csv_file)
        assert "file_name" in df.columns

    def test_unsupported_extension_raises(self, unsupported_file: Path) -> None:
        with pytest.raises(ValueError, match="Unsupported file type"):
            _invoke_file_asset(unsupported_file)

    def test_nonexistent_file_raises(self) -> None:
        fake = TEST_FILES_DIR / "nonexistent.csv"
        with pytest.raises(Exception):
            _invoke_file_asset(fake)


# ============================================================
# 6. file_name Column Validation
# ============================================================


class TestFileNameColumn:
    """The asset must inject a file_name column with the basename."""

    def test_csv_file_name(self, csv_file: Path) -> None:
        df = _invoke_file_asset(csv_file)
        assert df["file_name"].unique().tolist() == [csv_file.name]

    def test_json_file_name(self, json_file: Path) -> None:
        df = _invoke_file_asset(json_file)
        assert df["file_name"].unique().tolist() == [json_file.name]

    def test_excel_file_name(self, excel_file: Path) -> None:
        df = _invoke_file_asset(excel_file)
        assert df["file_name"].unique().tolist() == [excel_file.name]


# ============================================================
# 7. Schema Consistency Across Formats
# ============================================================


class TestSchemaConsistency:
    """Same data loaded from CSV, JSON, and Excel must produce
    the same column set and row count."""

    def test_same_columns(
        self, csv_file: Path, json_file: Path, excel_file: Path
    ) -> None:
        csv_df = _invoke_file_asset(csv_file)
        json_df = _invoke_file_asset(json_file)
        xlsx_df = _invoke_file_asset(excel_file)

        assert set(csv_df.columns) == set(json_df.columns) == set(xlsx_df.columns)

    def test_same_row_count(
        self, csv_file: Path, json_file: Path, excel_file: Path
    ) -> None:
        csv_df = _invoke_file_asset(csv_file)
        json_df = _invoke_file_asset(json_file)
        xlsx_df = _invoke_file_asset(excel_file)

        assert len(csv_df) == len(json_df) == len(xlsx_df) == EXPECTED_ROW_COUNT

    def test_amount_values_match(
        self, csv_file: Path, json_file: Path, excel_file: Path
    ) -> None:
        csv_df = _invoke_file_asset(csv_file)
        json_df = _invoke_file_asset(json_file)
        xlsx_df = _invoke_file_asset(excel_file)

        csv_amounts = csv_df["amount"].tolist()
        json_amounts = json_df["amount"].tolist()
        xlsx_amounts = xlsx_df["amount"].tolist()

        assert csv_amounts == json_amounts == xlsx_amounts





