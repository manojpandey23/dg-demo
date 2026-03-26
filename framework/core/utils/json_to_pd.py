import datetime as dt
from collections import defaultdict

import pandas as pd


def _infer_schema(records: list[dict]) -> dict:
    """
    Infer a logical schema from JSON records.

    Returns:
    {
        column_name: {
            "dtype": "string|int|float|bool|datetime|object",
            "nullable": bool,
        }
    }
    """
    values_by_column = defaultdict(list)

    for record in records:
        for k, v in record.items():
            values_by_column[k].append(v)

    schema = {}

    for col, values in values_by_column.items():
        non_null = [v for v in values if v is not None]

        nullable = len(non_null) < len(values)

        dtype = _infer_logical_type(non_null)

        schema[col] = {
            "dtype": dtype,
            "nullable": nullable,
        }

    return schema


def _infer_logical_type(values: list):
    if not values:
        return "string"

    if all(isinstance(v, bool) for v in values):
        return "bool"

    if all(isinstance(v, int) and not isinstance(v, bool) for v in values):
        return "int"

    if all(isinstance(v, (int, float)) for v in values):
        return "float"

    if all(isinstance(v, dt.date) and not isinstance(v, dt.datetime) for v in values):
        return "date"

    if all(isinstance(v, dt.datetime) for v in values):
        return "datetime"

    # ISO date/datetime strings
    if all(isinstance(v, str) for v in values):
        try:
            parsed = pd.to_datetime(values, errors="raise")
            if (parsed == parsed.normalize()).all():
                return "date"
            return "datetime"
        except Exception:
            return "string"

    return "object"


def _build_dataframe(records: list[dict], schema: dict) -> pd.DataFrame:
    df = pd.DataFrame(records)

    for col, meta in schema.items():
        dtype = meta["dtype"]

        if dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="raise")
        elif dtype == "bool":
            df[col] = df[col].astype("boolean")
        elif dtype in {"datetime", "date"}:
            df[col] = pd.to_datetime(df[col], errors="raise")
        else:
            df[col] = df[col].astype("string")

    return df


def _normalize_records(records):
    if isinstance(records, dict):
        return [records]
    if isinstance(records, list):
        return records
    raise ValueError("Unsupported JSON payload")


def to_dataframe(resp) -> pd.DataFrame:
    resp.raise_for_status()
    records = _normalize_records(resp.json())
    schema = _infer_schema(records)
    return _build_dataframe(records, schema)
