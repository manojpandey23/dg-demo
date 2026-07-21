import datetime as dt

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype


def is_datetime_column(df: pd.DataFrame, column: str) -> bool:
    """
    True if column is pandas datetime64[ns]
    """
    return is_datetime64_any_dtype(df[column])


def is_date_only_column(df: pd.DataFrame, column: str) -> bool:
    """
    True if column represents date-only values:
    - datetime64[ns] with all times = 00:00:00
    - Python datetime.date objects
    - ISO date strings (YYYY-MM-DD)
    """

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


def is_datetime_with_time_column(df: pd.DataFrame, column: str) -> bool:
    """
    True if datetime column AND at least one non‑midnight time exists
    """
    if not is_datetime64_any_dtype(df[column]):
        return False

    series = df[column].dropna()
    if series.empty:
        return False

    return (series != series.dt.normalize()).any()


def test_date_only_column_object_dtype_from_db():
    df = pd.DataFrame(
        {
            "event_date": [
                dt.date(2024, 2, 1),
                dt.date(2024, 2, 2),
                dt.date(2024, 2, 3),
            ]
        }
    )

    assert bool(is_datetime_column(df, "event_date")) is False
    assert bool(is_date_only_column(df, "event_date")) is True
    assert bool(is_datetime_with_time_column(df, "event_date")) is False


def test_date_only_column_string_object_dtype():
    df = pd.DataFrame(
        {
            "event_date": [
                "2024-02-01",
                "2024-02-02",
                "2024-02-03",
            ]
        }
    )

    assert bool(is_datetime_column(df, "event_date")) is False
    assert bool(is_date_only_column(df, "event_date")) is True


def test_date_only_column():
    df = pd.DataFrame(
        {"event_date": pd.to_datetime(["2024-02-01", "2024-02-02", "2024-02-03"])}
    )

    assert bool(is_datetime_column(df, "event_date")) is True
    assert bool(is_date_only_column(df, "event_date")) is True
    assert bool(is_datetime_with_time_column(df, "event_date")) is False


def test_datetime_with_time_column():
    df = pd.DataFrame(
        {"event_ts": pd.to_datetime(["2024-02-01 10:15:00", "2024-02-02 00:00:01"])}
    )

    assert bool(is_datetime_column(df, "event_ts")) is True
    assert bool(is_date_only_column(df, "event_ts")) is False
    assert bool(is_datetime_with_time_column(df, "event_ts")) is True


def test_string_date_column():
    df = pd.DataFrame({"event_date": ["2024-02-01", "2024-02-02"]})

    assert is_datetime_column(df, "event_date") is False
    assert bool(is_date_only_column(df, "event_date")) is True
    assert is_datetime_with_time_column(df, "event_date") is False
