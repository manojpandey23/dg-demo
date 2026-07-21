"""
Integration tests for apply_schema_and_materialize.

Tests all materialization strategies end-to-end against a real PostgreSQL
instance using an ephemeral test database.

Coverage:
  - TABLE materialization with dbt-like swap
  - INCREMENTAL append / merge / delete+insert
  - SNAPSHOT SCD Type 2 (check strategy, idempotency)
  - Schema evolution (append_new_columns, fail policy)
  - Transformation persistence
"""

import os

import pandas as pd
import pytest

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

pytestmark = pytest.mark.skipif(
    psycopg2 is None, reason="psycopg2 not installed"
)

from framework.model.config_models import (
    AssetSchema,
    IncrementalStrategy,
    Materialization,
    OnSchemaChange,
)
from framework.postgres.schema.apply import (
    apply_schema_and_materialize,
    is_object_exist,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TEST_DB = "test_framework_integ"


def _admin_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "7432")),
        database="postgres",
        user=os.getenv("DB_USER", "ods"),
        password=os.getenv("DB_PASSWORD", "ods"),
    )


def _test_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "7432")),
        database=_TEST_DB,
        user=os.getenv("DB_USER", "ods"),
        password=os.getenv("DB_PASSWORD", "ods"),
    )


@pytest.fixture(scope="session")
def db_conn():
    """Create an ephemeral database for the whole test session."""
    try:
        admin = _admin_connect()
    except Exception:
        pytest.skip("PostgreSQL not reachable — skipping integration tests")
    admin.autocommit = True
    with admin.cursor() as c:
        c.execute(f"DROP DATABASE IF EXISTS {_TEST_DB}")
        c.execute(f"CREATE DATABASE {_TEST_DB}")
    admin.close()

    conn = _test_connect()
    with conn.cursor() as c:
        c.execute("CREATE SCHEMA IF NOT EXISTS price")
        c.execute("CREATE SCHEMA IF NOT EXISTS stage")
    conn.commit()

    yield conn

    conn.close()

    admin = _admin_connect()
    admin.autocommit = True
    with admin.cursor() as c:
        c.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{_TEST_DB}' AND pid <> pg_backend_pid()"
        )
        c.execute(f"DROP DATABASE IF EXISTS {_TEST_DB}")
    admin.close()


@pytest.fixture()
def cur(db_conn):
    """Per-test cursor; rolls back any uncommitted work on teardown."""
    cursor = db_conn.cursor()
    yield cursor
    db_conn.rollback()
    cursor.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema(*cols: dict) -> list[AssetSchema]:
    """Shorthand to build a ``list[AssetSchema]`` from plain dicts."""
    return [
        AssetSchema(
            name=c["name"],
            dtype=c.get("dtype", "string"),
            nullable=c.get("nullable", True),
            isKey=c.get("isKey", False),
        )
        for c in cols
    ]


def _run(cur, table_fqn: str, df: pd.DataFrame, schema, **kwargs) -> dict:
    """Execute apply_schema_and_materialize inside a transaction."""
    cur.execute("BEGIN")
    result = apply_schema_and_materialize(
        cursor=cur,
        table_fqn=table_fqn,
        target_df=df,
        schema=schema,
        **kwargs,
    )
    cur.connection.commit()
    return result


def _count(cur, table_fqn: str) -> int:
    cur.execute(f"SELECT count(*) FROM {table_fqn}")
    return cur.fetchone()[0]


def _fetch(cur, table_fqn: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {table_fqn}", cur.connection)


def _columns(cur, table_fqn: str) -> list[str]:
    schema, table = table_fqn.split(".")
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


# ===================================================================
# TABLE materialization
# ===================================================================


class TestTableMaterialization:

    def test_first_run_creates_and_loads(self, cur):
        tbl = "price.t_table"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True, "nullable": False},
            {"name": "val", "dtype": "string"},
        )
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})

        meta = _run(
            cur, tbl, df, schema,
            materialization=Materialization.table,
            on_schema_change=OnSchemaChange.append_new_columns,
        )

        assert meta["table_created"] is True
        assert meta["rows_loaded"] == 3
        assert is_object_exist(cur, tbl)
        assert _count(cur, tbl) == 3

    def test_subsequent_run_swaps_data(self, cur):
        tbl = "price.t_table_swap"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )

        # run 1
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.table,
            on_schema_change=OnSchemaChange.append_new_columns,
        )
        assert _count(cur, tbl) == 2

        # run 2 – completely new payload
        _run(
            cur, tbl,
            pd.DataFrame({"id": [10, 20, 30], "val": ["x", "y", "z"]}),
            schema,
            materialization=Materialization.table,
            on_schema_change=OnSchemaChange.append_new_columns,
        )

        assert _count(cur, tbl) == 3
        data = _fetch(cur, tbl)
        assert sorted(data["id"].tolist()) == [10, 20, 30]


# ===================================================================
# INCREMENTAL – append
# ===================================================================


class TestIncrementalAppend:

    def test_first_run_inserts_all(self, cur):
        tbl = "price.t_inc_append"
        schema = _schema(
            {"name": "id", "dtype": "integer"},
            {"name": "val", "dtype": "string"},
        )

        meta = _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.append,
        )
        assert meta["table_created"] is True
        assert _count(cur, tbl) == 2

    def test_subsequent_run_appends(self, cur):
        tbl = "price.t_inc_append2"
        schema = _schema(
            {"name": "id", "dtype": "integer"},
            {"name": "val", "dtype": "string"},
        )

        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.append,
        )
        _run(
            cur, tbl,
            pd.DataFrame({"id": [3, 4], "val": ["c", "d"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.append,
        )

        assert _count(cur, tbl) == 4
        data = _fetch(cur, tbl)
        assert sorted(data["id"].tolist()) == [1, 2, 3, 4]


# ===================================================================
# INCREMENTAL – merge (upsert)
# ===================================================================


class TestIncrementalMerge:

    def test_first_run_inserts_all(self, cur):
        tbl = "price.t_inc_merge"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )

        meta = _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.merge,
            unique_key="id",
        )
        assert meta["table_created"] is True
        assert _count(cur, tbl) == 2

    def test_subsequent_run_upserts(self, cur):
        tbl = "price.t_inc_merge2"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )

        # seed
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.merge,
            unique_key="id",
        )

        # upsert: update id=1, keep id=2 unchanged, insert id=3
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2, 3], "val": ["a_upd", "b", "c"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.merge,
            unique_key="id",
        )

        assert _count(cur, tbl) == 3
        data = _fetch(cur, tbl)
        assert data.loc[data["id"] == 1, "val"].iloc[0] == "a_upd"
        assert data.loc[data["id"] == 3, "val"].iloc[0] == "c"


# ===================================================================
# INCREMENTAL – delete+insert
# ===================================================================


class TestIncrementalDeleteInsert:

    def test_replaces_matching_keys(self, cur):
        tbl = "price.t_inc_delinst"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )

        # seed 3 rows
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.delete_insert,
            unique_key="id",
        )
        assert _count(cur, tbl) == 3

        # replace keys 1 and 2
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["x", "y"]}),
            schema,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.delete_insert,
            unique_key="id",
        )

        assert _count(cur, tbl) == 3
        data = _fetch(cur, tbl)
        assert data.loc[data["id"] == 1, "val"].iloc[0] == "x"
        assert data.loc[data["id"] == 3, "val"].iloc[0] == "c"


# ===================================================================
# SNAPSHOT (SCD Type 2)
# ===================================================================


class TestSnapshotMaterialization:

    def test_first_run_creates_system_columns(self, cur):
        tbl = "stage.t_snap"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )

        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.snapshot,
            on_schema_change=OnSchemaChange.append_new_columns,
            unique_key="id",
            snapshot_strategy="check",
            check_cols=["val"],
        )

        cols = _columns(cur, tbl)
        for c in ("valid_from", "valid_to", "is_current", "is_deleted"):
            assert c in cols, f"missing system column: {c}"

        cur.execute(f"SELECT count(*) FROM {tbl} WHERE is_current = TRUE")
        assert cur.fetchone()[0] == 2

    def test_change_detection_versions_records(self, cur):
        tbl = "stage.t_snap_cd"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )

        # initial load
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
            schema,
            materialization=Materialization.snapshot,
            on_schema_change=OnSchemaChange.append_new_columns,
            unique_key="id", snapshot_strategy="check", check_cols=["val"],
        )

        # second load: id=1 changed, id=2 unchanged, id=3 new
        _run(
            cur, tbl,
            pd.DataFrame({"id": [1, 2, 3], "val": ["a_new", "b", "c"]}),
            schema,
            materialization=Materialization.snapshot,
            on_schema_change=OnSchemaChange.append_new_columns,
            unique_key="id", snapshot_strategy="check", check_cols=["val"],
        )

        # id=1 → 2 rows (old closed + new current)
        cur.execute(f"SELECT count(*) FROM {tbl} WHERE id = 1")
        assert cur.fetchone()[0] == 2

        # id=2 → 1 row (no change)
        cur.execute(f"SELECT count(*) FROM {tbl} WHERE id = 2")
        assert cur.fetchone()[0] == 1

        # id=3 → 1 row (new)
        cur.execute(f"SELECT count(*) FROM {tbl} WHERE id = 3")
        assert cur.fetchone()[0] == 1

        # exactly 3 current records
        cur.execute(f"SELECT count(*) FROM {tbl} WHERE is_current = TRUE")
        assert cur.fetchone()[0] == 3

        # old id=1 row must have valid_to set and is_current=FALSE
        cur.execute(
            f"SELECT valid_to, is_current FROM {tbl} "
            f"WHERE id = 1 AND is_current = FALSE"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] is not None  # valid_to filled in
        assert row[1] is False

    def test_no_change_is_idempotent(self, cur):
        """Running snapshot twice with identical data must NOT create new versions."""
        tbl = "stage.t_snap_idem"
        schema = _schema(
            {"name": "id", "dtype": "integer", "isKey": True},
            {"name": "val", "dtype": "string"},
        )
        df = pd.DataFrame({"id": [1], "val": ["a"]})

        _run(
            cur, tbl, df, schema,
            materialization=Materialization.snapshot,
            on_schema_change=OnSchemaChange.append_new_columns,
            unique_key="id", snapshot_strategy="check", check_cols=["val"],
        )
        _run(
            cur, tbl, df, schema,
            materialization=Materialization.snapshot,
            on_schema_change=OnSchemaChange.append_new_columns,
            unique_key="id", snapshot_strategy="check", check_cols=["val"],
        )

        assert _count(cur, tbl) == 1  # still one row – no duplicate version


# ===================================================================
# Schema evolution
# ===================================================================


class TestSchemaEvolution:

    def test_append_new_columns(self, cur):
        tbl = "price.t_schema_evo"
        v1 = _schema({"name": "id", "dtype": "integer"}, {"name": "val", "dtype": "string"})

        _run(
            cur, tbl,
            pd.DataFrame({"id": [1], "val": ["a"]}),
            v1,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.append,
        )

        v2 = _schema(
            {"name": "id", "dtype": "integer"},
            {"name": "val", "dtype": "string"},
            {"name": "extra", "dtype": "string"},
        )

        _run(
            cur, tbl,
            pd.DataFrame({"id": [2], "val": ["b"], "extra": ["e"]}),
            v2,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.append_new_columns,
            inc_strategy=IncrementalStrategy.append,
        )

        assert "extra" in _columns(cur, tbl)
        assert _count(cur, tbl) == 2

    def test_fail_policy_raises_on_mismatch(self, cur):
        tbl = "price.t_schema_fail"
        v1 = _schema({"name": "id", "dtype": "integer"}, {"name": "val", "dtype": "string"})

        _run(
            cur, tbl,
            pd.DataFrame({"id": [1], "val": ["a"]}),
            v1,
            materialization=Materialization.incremental,
            on_schema_change=OnSchemaChange.fail,
            inc_strategy=IncrementalStrategy.append,
        )

        v2 = _schema(
            {"name": "id", "dtype": "integer"},
            {"name": "val", "dtype": "string"},
            {"name": "extra", "dtype": "string"},
        )

        with pytest.raises(ValueError, match="Schema mismatch"):
            _run(
                cur, tbl,
                pd.DataFrame({"id": [2], "val": ["b"], "extra": ["e"]}),
                v2,
                materialization=Materialization.incremental,
                on_schema_change=OnSchemaChange.fail,
                inc_strategy=IncrementalStrategy.append,
            )


# ===================================================================
# Transformation persistence
# ===================================================================


class TestTransformationPersistence:

    def test_derived_column_persisted(self, cur):
        tbl = "price.t_transform"
        schema = _schema(
            {"name": "id", "dtype": "integer"},
            {"name": "amount", "dtype": "float"},
            {"name": "doubled", "dtype": "float"},
        )
        df = pd.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
        df["doubled"] = df["amount"] * 2  # simulate transformation layer

        _run(
            cur, tbl, df, schema,
            materialization=Materialization.table,
            on_schema_change=OnSchemaChange.append_new_columns,
        )

        data = _fetch(cur, tbl)
        assert data["doubled"].tolist() == [20.0, 40.0, 60.0]

