"""
Column-level transforms — registered via ``@transform`` decorator.

Every function signature: ``(df, ctx, output_col, *user_args)``
    • ``df``         — the input DataFrame
    • ``ctx``        — TransformationContext (lineage tracking)
    • ``output_col`` — name of the column being built
    • remaining args — whatever the user passes in the YAML ``expr``
"""

from __future__ import annotations

import hashlib
import math
import uuid
from typing import Any

import numpy as np
import pandas as pd

from framework.transformation.transform_registry import transform
from framework.transformation.transformation_context import (
    TransformationContext,
)


# ────────────────────────────────────────────────────────────
# Lineage helper
# ────────────────────────────────────────────────────────────


def _track(ctx: TransformationContext, output_col: str, arg: Any) -> None:
    """Record lineage if *arg* is a Series that came from a column."""
    if isinstance(arg, pd.Series):
        name = getattr(arg, "name", None)
        if name:
            ctx.record_ref(output_col, name)


# ============================================================
# 1. REFERENCE (existing)
# ============================================================


@transform("value")
def value_of(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    literal,
):
    """value("ABC") → constant literal, no lineage."""
    return literal


@transform("ref")
def ref(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    column: str,
):
    """ref("col") → df[col] with lineage."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in dataframe")
    ctx.record_ref(output_col, column)
    return df[column]


# ============================================================
# 2. NULL HANDLING
# ============================================================


@transform("coalesce")
def coalesce(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    *args,
):
    """coalesce(a, b, c) → first non-null per row."""
    if not args:
        raise ValueError("coalesce requires at least one argument")

    result = None
    for arg in args:
        _track(ctx, output_col, arg)
        if result is None:
            result = arg
        elif isinstance(result, pd.Series):
            result = result.fillna(arg)
        else:
            result = arg
    return result


@transform("fill_na")
def fill_na(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    default,
):
    """fill_na(ref("col"), 0) → replace NaN with *default*."""
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.fillna(default)
    return series


@transform("null_if")
def null_if(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    sentinel,
):
    """null_if(ref("col"), "N/A") → replace *sentinel* with NaN."""
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.replace(sentinel, np.nan)
    return np.nan if series == sentinel else series


@transform("is_null")
def is_null(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    """is_null(ref("col")) → boolean Series."""
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.isna()
    return pd.isna(series)


# ============================================================
# 3. CONDITIONAL
# ============================================================


@transform("when")
def when_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    condition,
    then_val,
    else_val=None,
):
    """when(ref("amt") > 0, value("CR"), value("DR")) → per-row conditional."""
    _track(ctx, output_col, then_val)
    _track(ctx, output_col, else_val)
    return pd.Series(np.where(condition, then_val, else_val), index=df.index)


@transform("map_values")
def map_values(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    mapping: dict,
    default=np.nan,
):
    """map_values(ref("ccy"), {"USD": "US Dollar"}) → dictionary lookup."""
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.map(mapping).fillna(default)
    return mapping.get(series, default)


# ============================================================
# 4. STRING
# ============================================================


@transform("upper")
def upper(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.str.upper() if isinstance(series, pd.Series) else str(series).upper()


@transform("lower")
def lower(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.str.lower() if isinstance(series, pd.Series) else str(series).lower()


@transform("trim")
def trim(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.str.strip() if isinstance(series, pd.Series) else str(series).strip()


@transform("concat")
def concat_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    *args,
):
    """concat(ref("a"), value("_"), ref("b")) → element-wise string join."""
    if not args:
        raise ValueError("concat requires at least one argument")
    result = None
    for arg in args:
        _track(ctx, output_col, arg)
        part = (
            arg.astype(str)
            if isinstance(arg, pd.Series)
            else pd.Series(str(arg), index=df.index)
        )
        result = part if result is None else result + part
    return result


@transform("substr")
def substr(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    start: int,
    length: int,
):
    """substr(ref("col"), 0, 3) → substring slice."""
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.str[start : start + length]
    return str(series)[start : start + length]


@transform("replace_str")
def replace_str(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    old: str,
    new: str,
):
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.str.replace(old, new, regex=False)
    return str(series).replace(old, new)


@transform("split")
def split_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    sep: str,
    index: int,
):
    """split(ref("fqn"), ".", -1) → split and pick element."""
    _track(ctx, output_col, series)
    if isinstance(series, pd.Series):
        return series.str.split(sep).str[index]
    return str(series).split(sep)[index]


# ============================================================
# 5. MATH / NUMERIC
# ============================================================


@transform("round_val")
def round_val(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    decimals: int = 2,
):
    _track(ctx, output_col, series)
    return (
        series.round(decimals)
        if isinstance(series, pd.Series)
        else round(series, decimals)
    )


@transform("abs_val")
def abs_val(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.abs() if isinstance(series, pd.Series) else abs(series)


@transform("floor_val")
def floor_val(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return np.floor(series) if isinstance(series, pd.Series) else math.floor(series)


@transform("ceil_val")
def ceil_val(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return np.ceil(series) if isinstance(series, pd.Series) else math.ceil(series)


# ============================================================
# 6. TYPE CASTING
# ============================================================


@transform("to_date")
def to_date(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    fmt: str | None = None,
):
    _track(ctx, output_col, series)
    return pd.to_datetime(series, format=fmt, errors="coerce").dt.date


@transform("to_datetime")
def to_datetime_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    fmt: str | None = None,
):
    _track(ctx, output_col, series)
    return pd.to_datetime(series, format=fmt, errors="coerce")


@transform("to_numeric")
def to_numeric(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return pd.to_numeric(series, errors="coerce")


@transform("to_string")
def to_string(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.astype(str) if isinstance(series, pd.Series) else str(series)


# ============================================================
# 7. DATE / TIME
# ============================================================


@transform("now")
def now_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
):
    """now() → current UTC timestamp (broadcast)."""
    return pd.Timestamp.now("UTC")


@transform("today")
def today_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
):
    """today() → current date (broadcast)."""
    return pd.Timestamp.now("UTC").date()


@transform("date_diff")
def date_diff(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    end_series,
    start_series,
    unit: str = "days",
):
    """date_diff(ref("maturity"), ref("trade_dt"), "days")."""
    _track(ctx, output_col, end_series)
    _track(ctx, output_col, start_series)
    delta = pd.to_datetime(end_series) - pd.to_datetime(start_series)
    match unit:
        case "days":
            return delta.dt.days
        case "hours":
            return delta.dt.total_seconds() / 3600
        case "seconds":
            return delta.dt.total_seconds()
        case _:
            return delta.dt.days


@transform("date_add")
def date_add(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    n: int,
    unit: str = "days",
):
    """date_add(ref("settle_dt"), 2, "days")."""
    _track(ctx, output_col, series)
    return pd.to_datetime(series) + pd.to_timedelta(n, unit=unit)


@transform("date_trunc")
def date_trunc(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    period: str = "month",
):
    """date_trunc(ref("trade_dt"), "month") → first of month/quarter/year."""
    _track(ctx, output_col, series)
    dt = pd.to_datetime(series)
    freq_map = {
        "day": "D",
        "week": "W",
        "month": "M",
        "quarter": "Q",
        "year": "Y",
    }
    freq = freq_map.get(period)
    if not freq:
        raise ValueError(f"Unknown period '{period}'; use: {list(freq_map)}")
    return dt.dt.to_period(freq).dt.to_timestamp()


@transform("extract_part")
def extract_part(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
    part: str,
):
    """extract_part(ref("dt"), "year") → extract year/month/day/hour/minute/second."""
    _track(ctx, output_col, series)
    dt = pd.to_datetime(series)
    accessor = getattr(dt.dt, part, None)
    if accessor is None:
        raise ValueError(f"Unknown date part '{part}'")
    return accessor


# ============================================================
# 8. IDENTITY / HASHING
# ============================================================


@transform("hash_key")
def hash_key(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    *args,
):
    """hash_key(ref("a"), ref("b")) → MD5 surrogate key."""
    parts: list[pd.Series] = []
    for arg in args:
        _track(ctx, output_col, arg)
        s = (
            arg.astype(str)
            if isinstance(arg, pd.Series)
            else pd.Series(str(arg), index=df.index)
        )
        parts.append(s)
    combined = parts[0]
    for p in parts[1:]:
        combined = combined + "|" + p
    return combined.apply(lambda x: hashlib.md5(x.encode()).hexdigest())


@transform("uuid_key")
def uuid_key(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
):
    """uuid_key() → unique UUID per row."""
    return pd.Series(
        [str(uuid.uuid4()) for _ in range(len(df))],
        index=df.index,
    )


# ============================================================
# 9. SCALAR AGGREGATION (broadcast to all rows)
# ============================================================


@transform("sum_of")
def sum_of(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.sum() if isinstance(series, pd.Series) else series


@transform("mean_of")
def mean_of(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.mean() if isinstance(series, pd.Series) else series


@transform("min_of")
def min_of(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.min() if isinstance(series, pd.Series) else series


@transform("max_of")
def max_of(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.max() if isinstance(series, pd.Series) else series


@transform("count_of")
def count_of(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    series,
):
    _track(ctx, output_col, series)
    return series.count() if isinstance(series, pd.Series) else 1


# ============================================================
# 10. WINDOW FUNCTIONS (return aligned Series, no row-count change)
# ============================================================


@transform("row_number")
def row_number(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    partition_series,
    order_series,
):
    """row_number(ref("ccy"), ref("dt")) → 1-based row number within partition."""
    _track(ctx, output_col, partition_series)
    _track(ctx, output_col, order_series)
    part_name = partition_series.name
    ord_name = order_series.name
    sorted_df = df.sort_values(ord_name)
    return sorted_df.groupby(part_name).cumcount().reindex(df.index) + 1


@transform("rank")
def rank_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    partition_series,
    order_series,
    method: str = "min",
):
    """rank(ref("ccy"), ref("amt")) → rank within partition."""
    _track(ctx, output_col, partition_series)
    _track(ctx, output_col, order_series)
    part_name = partition_series.name
    ord_name = order_series.name
    return (
        df.groupby(part_name)[ord_name]
        .rank(method=method)
        .reindex(df.index)
        .astype("Int64")
    )


@transform("lag")
def lag_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    col_series,
    n: int,
    partition_series,
    order_series,
):
    """lag(ref("price"), 1, ref("ticker"), ref("dt")) → previous row value."""
    _track(ctx, output_col, col_series)
    _track(ctx, output_col, partition_series)
    _track(ctx, output_col, order_series)
    part_name = partition_series.name
    ord_name = order_series.name
    col_name = col_series.name
    sorted_df = df.sort_values([part_name, ord_name])
    return sorted_df.groupby(part_name)[col_name].shift(n).reindex(df.index)


@transform("lead")
def lead_fn(
    df: pd.DataFrame,
    ctx: TransformationContext,
    output_col: str,
    col_series,
    n: int,
    partition_series,
    order_series,
):
    """lead(ref("price"), 1, ref("ticker"), ref("dt")) → next row value."""
    _track(ctx, output_col, col_series)
    _track(ctx, output_col, partition_series)
    _track(ctx, output_col, order_series)
    part_name = partition_series.name
    ord_name = order_series.name
    col_name = col_series.name
    sorted_df = df.sort_values([part_name, ord_name])
    return sorted_df.groupby(part_name)[col_name].shift(-n).reindex(df.index)

