"""
Table-level transforms — fluent, chainable Frame API.

Used in asset ``transforms.pre`` / ``transforms.post`` expressions::

    frame.filter(col("status") != "DELETED").dedup(["id"]).order_by(asc("dt"))

Design principles:
    • Every method returns a NEW Frame (immutable chain).
    • ``col()`` produces a lazy ColExpr resolved when a Frame method applies it.
    • Aggregation goes through ``GroupedFrame`` returned by ``.group_by()``.
    • The scope for ``eval()`` is built by ``build_table_scope()``.
"""

from __future__ import annotations

from typing import Any, Sequence

import pandas as pd


# ============================================================
# Lazy Column Expression (deferred until applied to a DF)
# ============================================================


class ColExpr:
    """Lazy reference to a column — resolved against a DataFrame."""

    __slots__ = ("_name",)

    def __init__(self, name: str):
        self._name = name

    def resolve(self, df: pd.DataFrame) -> pd.Series:
        return df[self._name]

    # ── comparison operators → Condition ──

    def __eq__(self, other):  # type: ignore[override]
        return Condition(self, "==", other)

    def __ne__(self, other):  # type: ignore[override]
        return Condition(self, "!=", other)

    def __gt__(self, other):
        return Condition(self, ">", other)

    def __ge__(self, other):
        return Condition(self, ">=", other)

    def __lt__(self, other):
        return Condition(self, "<", other)

    def __le__(self, other):
        return Condition(self, "<=", other)

    # ── predicate methods → Condition ──

    def is_in(self, values: list) -> Condition:
        return Condition(self, "isin", values)

    def is_null(self) -> Condition:
        return Condition(self, "isna", None)

    def is_not_null(self) -> Condition:
        return Condition(self, "notna", None)

    def between(self, lower, upper) -> Condition:
        return Condition(self, "between", (lower, upper))

    def contains(self, pattern: str, case: bool = True) -> Condition:
        return Condition(self, "contains", (pattern, case))

    def starts_with(self, prefix: str) -> Condition:
        return Condition(self, "startswith", prefix)

    def ends_with(self, suffix: str) -> Condition:
        return Condition(self, "endswith", suffix)

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return f"col({self._name!r})"


# ============================================================
# Condition / CompoundCondition
# ============================================================


class Condition:
    """Deferred boolean expression — resolved against a DataFrame."""

    __slots__ = ("_col", "_op", "_other")

    def __init__(self, col_expr: ColExpr, op: str, other: Any):
        self._col = col_expr
        self._op = op
        self._other = other

    @staticmethod
    def _resolve_operand(operand: Any, df: pd.DataFrame) -> Any:
        if isinstance(operand, ColExpr):
            return operand.resolve(df)
        return operand

    def resolve(self, df: pd.DataFrame) -> pd.Series:
        series = self._col.resolve(df)
        other = self._resolve_operand(self._other, df)

        match self._op:
            case "==":
                return series == other
            case "!=":
                return series != other
            case ">":
                return series > other
            case ">=":
                return series >= other
            case "<":
                return series < other
            case "<=":
                return series <= other
            case "isin":
                return series.isin(other)
            case "isna":
                return series.isna()
            case "notna":
                return series.notna()
            case "between":
                lo, hi = other
                return series.between(lo, hi)
            case "contains":
                pattern, case = other
                return series.str.contains(pattern, case=case, na=False)
            case "startswith":
                return series.str.startswith(other, na=False)
            case "endswith":
                return series.str.endswith(other, na=False)
            case _:
                raise ValueError(f"Unknown condition operator: {self._op}")

    # ── boolean combinators ──

    def __and__(self, other: Condition | CompoundCondition) -> CompoundCondition:
        return CompoundCondition(self, "&", other)

    def __or__(self, other: Condition | CompoundCondition) -> CompoundCondition:
        return CompoundCondition(self, "|", other)

    def __invert__(self) -> CompoundCondition:
        return CompoundCondition(self, "~", None)


class CompoundCondition:
    """Boolean combination of Conditions."""

    __slots__ = ("_left", "_op", "_right")

    def __init__(self, left, op: str, right):
        self._left = left
        self._op = op
        self._right = right

    def resolve(self, df: pd.DataFrame) -> pd.Series:
        left = self._left.resolve(df)
        if self._op == "~":
            return ~left
        right = self._right.resolve(df)
        if self._op == "&":
            return left & right
        if self._op == "|":
            return left | right
        raise ValueError(f"Unknown boolean operator: {self._op}")

    def __and__(self, other) -> CompoundCondition:
        return CompoundCondition(self, "&", other)

    def __or__(self, other) -> CompoundCondition:
        return CompoundCondition(self, "|", other)

    def __invert__(self) -> CompoundCondition:
        return CompoundCondition(self, "~", None)


# ============================================================
# Sort helpers
# ============================================================


class SortSpec:
    __slots__ = ("column", "ascending")

    def __init__(self, column: str, ascending: bool = True):
        self.column = column
        self.ascending = ascending


def asc(column: str) -> SortSpec:
    return SortSpec(column, ascending=True)


def desc(column: str) -> SortSpec:
    return SortSpec(column, ascending=False)


# ============================================================
# Aggregation helpers
# ============================================================


class AggExpr:
    """Deferred aggregation — resolved inside GroupedFrame.agg()."""

    __slots__ = ("_column", "_func", "_alias")

    def __init__(self, column: str, func: str):
        self._column = column
        self._func = func
        self._alias: str | None = None

    def alias(self, name: str) -> AggExpr:
        self._alias = name
        return self

    def resolve_name(self) -> str:
        return self._alias or f"{self._func}_{self._column}"


def agg_sum(column: str) -> AggExpr:
    return AggExpr(column, "sum")


def agg_count(column: str) -> AggExpr:
    return AggExpr(column, "count")


def agg_mean(column: str) -> AggExpr:
    return AggExpr(column, "mean")


def agg_min(column: str) -> AggExpr:
    return AggExpr(column, "min")


def agg_max(column: str) -> AggExpr:
    return AggExpr(column, "max")


def agg_first(column: str) -> AggExpr:
    return AggExpr(column, "first")


def agg_last(column: str) -> AggExpr:
    return AggExpr(column, "last")


# ============================================================
# GroupedFrame
# ============================================================


class GroupedFrame:
    """Intermediate returned by ``Frame.group_by()``."""

    __slots__ = ("_df", "_keys")

    def __init__(self, df: pd.DataFrame, keys: list[str]):
        self._df = df
        self._keys = keys

    def agg(self, *agg_exprs: AggExpr) -> Frame:
        agg_kwargs = {
            expr.resolve_name(): pd.NamedAgg(
                column=expr._column,
                aggfunc=expr._func,
            )
            for expr in agg_exprs
        }
        result = self._df.groupby(self._keys, as_index=False).agg(**agg_kwargs)
        return Frame(result)


# ============================================================
# Frame — the core fluent wrapper
# ============================================================


class Frame:
    """Immutable, chainable DataFrame wrapper for table-level transforms.

    Every mutating method returns a **new** Frame.
    """

    __slots__ = ("_df",)

    def __init__(self, df: pd.DataFrame):
        self._df = df

    # ── row filtering ──

    def filter(self, condition: Condition | CompoundCondition) -> Frame:
        mask = condition.resolve(self._df)
        return Frame(self._df[mask].reset_index(drop=True))

    # ── column selection ──

    def select(self, *columns: str) -> Frame:
        return Frame(self._df[list(columns)])

    def drop(self, *columns: str) -> Frame:
        return Frame(self._df.drop(columns=list(columns)))

    def rename(self, mapping: dict[str, str]) -> Frame:
        return Frame(self._df.rename(columns=mapping))

    # ── deduplication ──

    def dedup(
        self,
        columns: Sequence[str],
        keep: str = "first",
    ) -> Frame:
        return Frame(
            self._df.drop_duplicates(subset=list(columns), keep=keep)
            .reset_index(drop=True)
        )

    def distinct(self, columns: Sequence[str] | None = None) -> Frame:
        subset = list(columns) if columns else None
        return Frame(
            self._df.drop_duplicates(subset=subset).reset_index(drop=True)
        )

    # ── sorting ──

    def order_by(self, *specs: SortSpec) -> Frame:
        cols = [s.column for s in specs]
        ascending = [s.ascending for s in specs]
        return Frame(
            self._df.sort_values(cols, ascending=ascending)
            .reset_index(drop=True)
        )

    # ── limiting ──

    def limit(self, n: int) -> Frame:
        return Frame(self._df.head(n).reset_index(drop=True))

    # ── grouping / aggregation ──

    def group_by(self, keys: Sequence[str]) -> GroupedFrame:
        return GroupedFrame(self._df, list(keys))

    # ── output ──

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def __repr__(self) -> str:
        return f"Frame(rows={len(self._df)}, cols={list(self._df.columns)})"


# ============================================================
# Scope builder & entry-point
# ============================================================


def build_table_scope(df: pd.DataFrame) -> dict[str, Any]:
    """Build the eval() scope for a table-level transform expression."""
    return {
        # entry-point
        "frame": Frame(df),
        # lazy column reference
        "col": ColExpr,
        # sort helpers
        "asc": asc,
        "desc": desc,
        # aggregation helpers
        "agg_sum": agg_sum,
        "agg_count": agg_count,
        "agg_mean": agg_mean,
        "agg_min": agg_min,
        "agg_max": agg_max,
        "agg_first": agg_first,
        "agg_last": agg_last,
    }


def apply_table_transform(
    df: pd.DataFrame,
    expr_str: str,
) -> pd.DataFrame:
    """Evaluate a table-level transform expression and return the result DF.

    The expression is wrapped in ``()`` so multi-line YAML strings
    (both ``>`` folded and ``|`` literal) work out of the box.
    """
    normalized = f"({expr_str.strip()})"
    scope = build_table_scope(df)
    result = eval(normalized, {"__builtins__": {}}, scope)  # noqa: S307

    if isinstance(result, Frame):
        return result.df

    raise TypeError(
        f"Table transform must return a Frame, got {type(result).__name__}. "
        f"Expression: {expr_str[:120]}"
    )

