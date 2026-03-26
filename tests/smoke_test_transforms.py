"""
Pytest tests for column-level and table-level transforms.

Column-level  → registered via @transform, evaluated per column expr
Table-level   → fluent Frame API, evaluated as pre/post expressions
"""

import pytest
import pandas as pd
import numpy as np
from dagster import AssetKey

from framework.transformation import builtin_transforms, table_transforms  # noqa: F401
from framework.transformation.transform_registry import TRANSFORMS
from framework.transformation.transformation_executor import apply_transformations
from framework.transformation.table_transforms import (
    Frame,
    ColExpr,
    CompoundCondition,
    asc,
    desc,
    agg_sum,
    agg_count,
    agg_mean,
    agg_max,
    agg_min,
    agg_first,
    agg_last,
    apply_table_transform,
)
from framework.model.config_models import AssetConfig, TransformConfig, AssetSchema


# ================================================================
# Fixtures
# ================================================================


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "name": ["  Alice  ", "  Bob  ", "  Charlie  "],
            "amt": [100.456, -50.0, 0.0],
            "ccy": ["usd", "eur", "gbp"],
            "status": ["ACTIVE", "DELETED", "ACTIVE"],
            "dt": ["2026-01-01", "2026-01-15", "2026-02-01"],
        }
    )


@pytest.fixture
def nullable_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "account_cd": ["FUND_A", "FUND_B", "FUND_C"],
            "amt": [100.456, -50.0, None],
            "ccy": ["  usd  ", "  EUR  ", "  gbp  "],
            "dt": ["2026-01-01", "2026-01-15", "2026-02-01"],
        }
    )


# ================================================================
# 1. Transform Registration
# ================================================================


EXPECTED_TRANSFORMS = [
    "abs_val", "ceil_val", "coalesce", "concat", "count_of",
    "date_add", "date_diff", "date_trunc", "extract_part",
    "fill_na", "floor_val", "hash_key", "is_null",
    "lag", "lead", "lower", "map_values", "max_of", "mean_of",
    "min_of", "now", "null_if", "rank", "ref", "replace_str",
    "round_val", "row_number", "split", "substr", "sum_of",
    "to_date", "to_datetime", "to_numeric", "to_string",
    "today", "trim", "upper", "uuid_key", "value", "when",
]


def test_all_transforms_registered():
    registered = sorted(TRANSFORMS._functions.keys())
    for name in EXPECTED_TRANSFORMS:
        assert name in registered, f"Transform '{name}' not registered"


# ================================================================
# 2. Frame API — filter
# ================================================================


class TestFrameFilter:
    def test_simple_filter(self, sample_df):
        r = Frame(sample_df).filter(ColExpr("status") != "DELETED")
        assert len(r.df) == 2

    def test_compound_and(self, sample_df):
        r = Frame(sample_df).filter(
            (ColExpr("status") == "ACTIVE") & (ColExpr("amt") > 0)
        )
        assert len(r.df) == 1

    def test_compound_or(self, sample_df):
        r = Frame(sample_df).filter(
            (ColExpr("ccy") == "usd") | (ColExpr("ccy") == "eur")
        )
        assert len(r.df) == 2

    def test_is_in(self, sample_df):
        r = Frame(sample_df).filter(ColExpr("ccy").is_in(["usd", "eur"]))
        assert len(r.df) == 2

    def test_is_null(self):
        df = pd.DataFrame({"x": [1, None, 3]})
        r = Frame(df).filter(ColExpr("x").is_null())
        assert len(r.df) == 1

    def test_is_not_null(self):
        df = pd.DataFrame({"x": [1, None, 3]})
        r = Frame(df).filter(ColExpr("x").is_not_null())
        assert len(r.df) == 2

    def test_between(self, sample_df):
        r = Frame(sample_df).filter(ColExpr("amt").between(-100, 50))
        assert len(r.df) == 2

    def test_invert(self, sample_df):
        r = Frame(sample_df).filter(~ColExpr("status").is_null())
        assert len(r.df) == 3

    def test_contains(self):
        df = pd.DataFrame({"txt": ["hello world", "foo bar", "hello again"]})
        r = Frame(df).filter(ColExpr("txt").contains("hello"))
        assert len(r.df) == 2

    def test_starts_with(self):
        df = pd.DataFrame({"txt": ["ABC_1", "DEF_2", "ABC_3"]})
        r = Frame(df).filter(ColExpr("txt").starts_with("ABC"))
        assert len(r.df) == 2

    def test_ends_with(self):
        df = pd.DataFrame({"txt": ["file.csv", "file.json", "data.csv"]})
        r = Frame(df).filter(ColExpr("txt").ends_with(".csv"))
        assert len(r.df) == 2

    def test_col_vs_col(self):
        df = pd.DataFrame({"a": [1, 5, 3], "b": [2, 4, 3]})
        r = Frame(df).filter(ColExpr("a") > ColExpr("b"))
        assert len(r.df) == 1
        assert r.df["a"].iloc[0] == 5


# ================================================================
# 3. Frame API — dedup / distinct
# ================================================================


class TestFrameDedup:
    def test_dedup_keep_first(self):
        df = pd.DataFrame({"id": [1, 1, 2, 3], "val": ["a", "b", "c", "d"]})
        r = Frame(df).dedup(["id"], keep="first")
        assert len(r.df) == 3
        assert r.df[r.df["id"] == 1]["val"].iloc[0] == "a"

    def test_dedup_keep_last(self):
        df = pd.DataFrame({"id": [1, 1, 2, 3], "val": ["a", "b", "c", "d"]})
        r = Frame(df).dedup(["id"], keep="last")
        assert len(r.df) == 3
        assert r.df[r.df["id"] == 1]["val"].iloc[0] == "b"

    def test_distinct_all(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        r = Frame(df).distinct()
        assert len(r.df) == 2

    def test_distinct_subset(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "y"]})
        r = Frame(df).distinct(["a"])
        assert len(r.df) == 2


# ================================================================
# 4. Frame API — order_by / limit / select / drop / rename
# ================================================================


class TestFrameShaping:
    def test_order_by_desc(self, sample_df):
        r = Frame(sample_df).order_by(desc("amt"))
        assert r.df["amt"].iloc[0] == 100.456

    def test_order_by_asc(self, sample_df):
        r = Frame(sample_df).order_by(asc("amt"))
        assert r.df["amt"].iloc[0] == -50.0

    def test_order_by_multi(self):
        df = pd.DataFrame({"grp": ["A", "A", "B"], "val": [2, 1, 3]})
        r = Frame(df).order_by(asc("grp"), desc("val"))
        assert list(r.df["val"]) == [2, 1, 3]

    def test_limit(self, sample_df):
        r = Frame(sample_df).limit(2)
        assert len(r.df) == 2

    def test_select(self, sample_df):
        r = Frame(sample_df).select("name", "amt")
        assert list(r.df.columns) == ["name", "amt"]

    def test_drop(self, sample_df):
        r = Frame(sample_df).drop("status", "dt")
        assert "status" not in r.df.columns
        assert "dt" not in r.df.columns

    def test_rename(self, sample_df):
        r = Frame(sample_df).rename({"amt": "amount", "ccy": "currency"})
        assert "amount" in r.df.columns
        assert "currency" in r.df.columns


# ================================================================
# 5. Frame API — group_by + agg
# ================================================================


class TestFrameGroupBy:
    def test_sum(self):
        df = pd.DataFrame({"ccy": ["USD", "USD", "EUR"], "amt": [10, 20, 30]})
        r = Frame(df).group_by(["ccy"]).agg(agg_sum("amt").alias("total"))
        assert len(r.df) == 2
        usd = r.df[r.df["ccy"] == "USD"]["total"].iloc[0]
        assert usd == 30

    def test_count(self):
        df = pd.DataFrame({"ccy": ["USD", "USD", "EUR"], "amt": [10, 20, 30]})
        r = Frame(df).group_by(["ccy"]).agg(agg_count("amt").alias("cnt"))
        usd = r.df[r.df["ccy"] == "USD"]["cnt"].iloc[0]
        assert usd == 2

    def test_multi_agg(self):
        df = pd.DataFrame({"g": ["A", "A", "B"], "v": [1, 3, 5]})
        r = Frame(df).group_by(["g"]).agg(
            agg_sum("v").alias("total"),
            agg_mean("v").alias("avg"),
            agg_min("v").alias("lo"),
            agg_max("v").alias("hi"),
        )
        a_row = r.df[r.df["g"] == "A"]
        assert a_row["total"].iloc[0] == 4
        assert a_row["avg"].iloc[0] == 2.0
        assert a_row["lo"].iloc[0] == 1
        assert a_row["hi"].iloc[0] == 3

    def test_default_alias(self):
        df = pd.DataFrame({"g": ["X"], "v": [42]})
        r = Frame(df).group_by(["g"]).agg(agg_sum("v"))
        assert "sum_v" in r.df.columns


# ================================================================
# 6. Frame API — chained string expression (eval)
# ================================================================


class TestApplyTableTransform:
    def test_chain_filter_order_limit(self, sample_df):
        expr = 'frame.filter(col("status") != "DELETED").order_by(desc("amt")).limit(1)'
        out = apply_table_transform(sample_df, expr)
        assert len(out) == 1
        assert out["amt"].iloc[0] == 100.456

    def test_chain_dedup_select(self):
        df = pd.DataFrame({"id": [1, 1, 2], "val": [10, 20, 30]})
        expr = 'frame.dedup(["id"], keep="last").select("id", "val")'
        out = apply_table_transform(df, expr)
        assert len(out) == 2
        assert list(out.columns) == ["id", "val"]

    def test_chain_group_agg_order(self):
        df = pd.DataFrame({"g": ["A", "A", "B"], "v": [1, 2, 10]})
        expr = (
            'frame.group_by(["g"])'
            '.agg(agg_sum("v").alias("total"))'
            '.order_by(desc("total"))'
        )
        out = apply_table_transform(df, expr)
        assert out["g"].iloc[0] == "B"

    def test_returns_error_on_non_frame(self):
        with pytest.raises(TypeError, match="must return a Frame"):
            apply_table_transform(pd.DataFrame({"a": [1]}), "42")

    def test_multiline_yaml_folded(self, sample_df):
        # YAML > folds newlines into spaces — verify that works
        expr = (
            'frame\n'
            '.filter(col("status") == "ACTIVE")\n'
            '.order_by(asc("amt"))\n'
            '.limit(1)'
        )
        out = apply_table_transform(sample_df, expr)
        assert len(out) == 1


# ================================================================
# 7. Column-level transforms via executor
# ================================================================


class TestColumnTransformsViaExecutor:
    def test_ref_value_fill_na(self, nullable_df):
        schema = [
            AssetSchema(name="cd", expr='ref("account_cd")'),
            AssetSchema(name="amt", expr='fill_na(ref("amt"), 0)'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert df["amt"].iloc[2] == 0.0

    def test_upper_trim(self, nullable_df):
        schema = [
            AssetSchema(name="ccy", expr='upper(trim(ref("ccy")))'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert list(df["ccy"]) == ["USD", "EUR", "GBP"]

    def test_when(self, nullable_df):
        schema = [
            AssetSchema(name="dir", expr='when(ref("amt") > 0, value("CR"), value("DR"))'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert list(df["dir"]) == ["CR", "DR", "DR"]

    def test_hash_key(self, nullable_df):
        schema = [
            AssetSchema(name="sk", expr='hash_key(ref("account_cd"), ref("ccy"))'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert len(df["sk"].iloc[0]) == 32  # MD5 hex

    def test_round_val(self, nullable_df):
        schema = [
            AssetSchema(name="r", expr='round_val(ref("amt"), 1)'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert df["r"].iloc[0] == 100.5

    def test_abs_val(self, nullable_df):
        schema = [
            AssetSchema(name="a", expr='abs_val(ref("amt"))'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert df["a"].iloc[1] == 50.0

    def test_extract_part(self, nullable_df):
        schema = [
            AssetSchema(name="yr", expr='extract_part(ref("dt"), "year")'),
            AssetSchema(name="mo", expr='extract_part(ref("dt"), "month")'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert df["yr"].iloc[0] == 2026
        assert df["mo"].iloc[0] == 1

    def test_concat(self, nullable_df):
        schema = [
            AssetSchema(name="full", expr='concat(ref("account_cd"), value("_"), ref("ccy"))'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert df["full"].iloc[0] == "FUND_A_  usd  "

    def test_substr(self, nullable_df):
        schema = [
            AssetSchema(name="sub", expr='substr(ref("account_cd"), 0, 4)'),
        ]
        df, _ = apply_transformations(
            nullable_df, schema, AssetKey("up"),
        )
        assert df["sub"].iloc[0] == "FUND"

    def test_replace_str(self):
        df = pd.DataFrame({"txt": ["hello_world", "foo_bar"]})
        schema = [
            AssetSchema(name="out", expr='replace_str(ref("txt"), "_", "-")'),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["out"].iloc[0] == "hello-world"

    def test_split(self):
        df = pd.DataFrame({"fqn": ["schema.table.column"]})
        schema = [
            AssetSchema(name="col", expr='split(ref("fqn"), ".", -1)'),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["col"].iloc[0] == "column"

    def test_lower(self):
        df = pd.DataFrame({"x": ["HELLO"]})
        schema = [AssetSchema(name="x", expr='lower(ref("x"))')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["x"].iloc[0] == "hello"

    def test_null_if(self):
        df = pd.DataFrame({"x": ["A", "N/A", "B"]})
        schema = [AssetSchema(name="x", expr='null_if(ref("x"), "N/A")')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert pd.isna(out["x"].iloc[1])

    def test_is_null(self):
        df = pd.DataFrame({"x": [1.0, None, 3.0]})
        schema = [AssetSchema(name="flag", expr='is_null(ref("x"))')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["flag"].iloc[1] is True or out["flag"].iloc[1] == True  # noqa

    def test_coalesce(self):
        df = pd.DataFrame({"a": [None, 2.0], "b": [10.0, 20.0]})
        schema = [AssetSchema(name="c", expr='coalesce(ref("a"), ref("b"))')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["c"].iloc[0] == 10.0
        assert out["c"].iloc[1] == 2.0

    def test_map_values(self):
        df = pd.DataFrame({"ccy": ["USD", "EUR", "XXX"]})
        schema = [
            AssetSchema(
                name="label",
                expr='map_values(ref("ccy"), {"USD": "US Dollar", "EUR": "Euro"}, value("Unknown"))',
            )
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["label"].iloc[0] == "US Dollar"
        assert out["label"].iloc[2] == "Unknown"

    def test_uuid_key(self):
        df = pd.DataFrame({"x": [1, 2]})
        schema = [AssetSchema(name="id", expr='uuid_key()')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["id"].iloc[0] != out["id"].iloc[1]
        assert len(out["id"].iloc[0]) == 36  # UUID4 format

    def test_now_and_today(self):
        df = pd.DataFrame({"x": [1]})
        schema = [
            AssetSchema(name="ts", expr="now()"),
            AssetSchema(name="dt", expr="today()"),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["ts"].iloc[0] is not None
        assert out["dt"].iloc[0] is not None

    def test_to_date(self):
        df = pd.DataFrame({"x": ["2026-03-25"]})
        schema = [AssetSchema(name="d", expr='to_date(ref("x"))')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        from datetime import date
        assert out["d"].iloc[0] == date(2026, 3, 25)

    def test_to_numeric(self):
        df = pd.DataFrame({"x": ["123", "abc", "456"]})
        schema = [AssetSchema(name="n", expr='to_numeric(ref("x"))')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["n"].iloc[0] == 123
        assert pd.isna(out["n"].iloc[1])

    def test_floor_ceil(self):
        df = pd.DataFrame({"x": [1.7, 2.3]})
        schema = [
            AssetSchema(name="f", expr='floor_val(ref("x"))'),
            AssetSchema(name="c", expr='ceil_val(ref("x"))'),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["f"].iloc[0] == 1.0
        assert out["c"].iloc[0] == 2.0

    def test_date_diff(self):
        df = pd.DataFrame({"a": ["2026-01-01"], "b": ["2026-01-10"]})
        schema = [AssetSchema(name="d", expr='date_diff(ref("b"), ref("a"), "days")')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["d"].iloc[0] == 9

    def test_date_add(self):
        df = pd.DataFrame({"x": ["2026-01-01"]})
        schema = [AssetSchema(name="d", expr='date_add(ref("x"), 5, "days")')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert str(out["d"].iloc[0])[:10] == "2026-01-06"

    def test_sum_of_broadcast(self):
        df = pd.DataFrame({"x": [10.0, 20.0, 30.0]})
        schema = [AssetSchema(name="total", expr='sum_of(ref("x"))')]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        # sum_of broadcasts scalar to every row
        assert out["total"].iloc[0] == 60.0
        assert out["total"].iloc[2] == 60.0


# ================================================================
# 8. Window functions
# ================================================================


class TestWindowFunctions:
    def test_row_number(self):
        df = pd.DataFrame({
            "grp": ["A", "A", "B", "B"],
            "val": [10, 20, 30, 40],
        })
        schema = [
            AssetSchema(name="rn", expr='row_number(ref("grp"), ref("val"))'),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        # Within each group, row_number 1-based
        assert set(out["rn"]) == {1, 2}

    def test_lag(self):
        df = pd.DataFrame({
            "grp": ["A", "A", "A"],
            "ord": [1, 2, 3],
            "val": [10, 20, 30],
        })
        schema = [
            AssetSchema(name="prev", expr='lag(ref("val"), 1, ref("grp"), ref("ord"))'),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert pd.isna(out["prev"].iloc[0])
        assert out["prev"].iloc[1] == 10

    def test_lead(self):
        df = pd.DataFrame({
            "grp": ["A", "A", "A"],
            "ord": [1, 2, 3],
            "val": [10, 20, 30],
        })
        schema = [
            AssetSchema(name="nxt", expr='lead(ref("val"), 1, ref("grp"), ref("ord"))'),
        ]
        out, _ = apply_transformations(df, schema, AssetKey("up"))
        assert out["nxt"].iloc[0] == 20
        assert pd.isna(out["nxt"].iloc[2])


# ================================================================
# 9. Pre + Post transforms through executor
# ================================================================


class TestPrePostTransforms:
    def test_pre_filter(self):
        df = pd.DataFrame({
            "status": ["ACTIVE", "DELETED", "ACTIVE"],
            "amt": [10.0, 20.0, 30.0],
        })
        transforms = TransformConfig(
            pre='frame.filter(col("status") == "ACTIVE")',
        )
        schema = [AssetSchema(name="amt", expr='ref("amt")')]
        out, _ = apply_transformations(
            df, schema, AssetKey("up"), transforms=transforms,
        )
        assert len(out) == 2

    def test_post_order_limit(self):
        df = pd.DataFrame({"val": [3, 1, 2]})
        transforms = TransformConfig(
            post='frame.order_by(asc("val")).limit(2)',
        )
        schema = [AssetSchema(name="val", expr='ref("val")')]
        out, _ = apply_transformations(
            df, schema, AssetKey("up"), transforms=transforms,
        )
        assert len(out) == 2
        assert out["val"].iloc[0] == 1

    def test_pre_and_post_combined(self):
        df = pd.DataFrame({
            "status": ["ACTIVE", "ACTIVE", "DELETED", "ACTIVE"],
            "amt": [50.0, 10.0, 999.0, 30.0],
        })
        transforms = TransformConfig(
            pre='frame.filter(col("status") == "ACTIVE")',
            post='frame.order_by(desc("amt")).limit(2)',
        )
        schema = [AssetSchema(name="amt", expr='ref("amt")')]
        out, _ = apply_transformations(
            df, schema, AssetKey("up"), transforms=transforms,
        )
        assert len(out) == 2
        assert out["amt"].iloc[0] == 50.0
        assert out["amt"].iloc[1] == 30.0

    def test_transforms_only_no_schema(self):
        """transforms without column schema — just reshape"""
        df = pd.DataFrame({"a": [3, 1, 2], "b": ["x", "y", "z"]})
        transforms = TransformConfig(
            post='frame.order_by(asc("a")).limit(2)',
        )
        out, _ = apply_transformations(
            df, [], AssetKey("up"), transforms=transforms,
        )
        assert len(out) == 2
        assert out["a"].iloc[0] == 1


# ================================================================
# 10. TransformConfig model parsing
# ================================================================


class TestTransformConfigModel:
    def test_from_dict(self):
        tc = TransformConfig(**{
            "pre": 'frame.filter(col("x") > 0)',
            "post": 'frame.limit(10)',
        })
        assert tc.pre is not None
        assert tc.post is not None

    def test_optional_fields(self):
        tc = TransformConfig()
        assert tc.pre is None
        assert tc.post is None

    def test_embedded_in_asset_config(self):
        cfg = AssetConfig(**{
            "name": "test",
            "type": "database",
            "source": {"resource": "pg", "table": "s.t"},
            "transforms": {
                "pre": 'frame.filter(col("status") == "ACTIVE")',
            },
        })
        assert cfg.transforms is not None
        assert cfg.transforms.pre is not None
        assert cfg.transforms.post is None

    def test_asset_config_without_transforms(self):
        cfg = AssetConfig(**{
            "name": "test",
            "type": "api",
            "source": {"endpoint": "/x"},
        })
        assert cfg.transforms is None


# ================================================================
# 11. Lineage tracking
# ================================================================


class TestLineageTracking:
    def test_ref_records_lineage(self, nullable_df):
        schema = [AssetSchema(name="cd", expr='ref("account_cd")')]
        _, lineage = apply_transformations(
            nullable_df, schema, AssetKey("upstream"),
        )
        assert lineage is not None
        assert "cd" in lineage
        assert "account_cd" in lineage["cd"][AssetKey("upstream")]

    def test_multi_ref_lineage(self, nullable_df):
        schema = [
            AssetSchema(name="full", expr='concat(ref("account_cd"), ref("ccy"))'),
        ]
        _, lineage = apply_transformations(
            nullable_df, schema, AssetKey("upstream"),
        )
        deps = lineage["full"][AssetKey("upstream")]
        assert "account_cd" in deps
        assert "ccy" in deps

    def test_value_no_lineage(self, nullable_df):
        schema = [AssetSchema(name="const", expr='value("FIXED")')]
        _, lineage = apply_transformations(
            nullable_df, schema, AssetKey("upstream"),
        )
        assert lineage is None  # no ref() calls → no lineage

