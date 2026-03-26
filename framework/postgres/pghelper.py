import io
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from framework.model.config_models import AssetSchema
from psycopg2 import sql

from .sql.ddl import (
    close_snapshot_sql,
    column_ident,
    delete_by_keys_sql,
    incremental_merge_sql,
    insert_snapshot_row_sql,
    invalidate_snapshot_sql,
    select_current_by_key_sql,
    select_current_snapshot_sql,
    table_ident,
)


def compute_copy_chunk_size(df: pd.DataFrame) -> int:
    row_count = len(df)
    col_count = len(df.columns)

    if row_count <= 100_000:
        return row_count

    if col_count > 50:
        return 25_000

    if row_count > 5_000_000:
        return 250_000

    return 100_000


# ============================================================
# Pandas → Postgres type mapping
# ============================================================


def pandas_to_pg_type(dtype: pd.api.extensions.ExtensionDtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_string_dtype(dtype):
        return "TEXT"
    if pd.api.types.is_datetime64tz_dtype(dtype):
        return "TIMESTAMPTZ"
    if pd.api.types.is_datetime64_dtype(dtype):
        return "TIMESTAMP"

    raise ValueError(f"Unsupported pandas dtype: {dtype}")


# ============================================================
# Helpers: Python value normalization
# ============================================================


def _to_python(value):
    if pd.isna(value):
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


# ============================================================
# Insert helper
# ============================================================


def insert_dataframe_copy(cursor, table_fqn: str, df: pd.DataFrame, declared_schema):
    if df.empty:
        return

    df = df[declared_schema.keys()]
    df = df.where(pd.notnull(df), None)

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    # ---- COPY FROM STDIN ----
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')").format(
        table_ident(table_fqn),
        sql.SQL(", ").join(map(sql.Identifier, declared_schema.keys())),
    )

    cursor.copy_expert(copy_sql, buffer)


# ============================================================
# DDL helpers
# ============================================================


def extract_primary_keys(schema: list[AssetSchema]) -> list[str]:
    keys = [col.name for col in schema if col.isKey]
    return keys


def generate_create_table_sql(df: pd.DataFrame):
    return [
        sql.SQL("{} {}").format(
            column_ident(str(col)),
            sql.SQL(pandas_to_pg_type(str(dtype))),
        )
        for col, dtype in df.dtypes.items()
    ]


def materialize_table(
    cursor, table_fqn: str, df: pd.DataFrame, declared_schema: Dict[str, Dict[str, Any]]
) -> None:
    """
    Materialize dataframe into table via COPY FROM STDIN.
    Used for table materialization (full refresh).
    """
    chunk_size = compute_copy_chunk_size(df)
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        insert_dataframe_copy(cursor, table_fqn, chunk, declared_schema)


def _insert_dataframe(
    cursor, table_fqn: str, df: pd.DataFrame
) -> None:
    """
    Insert dataframe into table via COPY FROM STDIN.
    Used for incremental append operations.
    """
    if df.empty:
        return
    
    declared_schema = {col: {"name": col} for col in df.columns}
    chunk_size = compute_copy_chunk_size(df)
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        insert_dataframe_copy(cursor, table_fqn, chunk, declared_schema)


def materialize_table_with_insert(
    cursor, table_fqn: str, df: pd.DataFrame
) -> None:
    """
    Insert data into incremental table on first materialization.
    Table schema must already exist.
    """
    _insert_dataframe(cursor, table_fqn, df)


def build_column_defs(df: pd.DataFrame) -> List[sql.Composable]:
    return [
        sql.SQL("{} {}").format(
            column_ident(col),
            sql.SQL(pandas_to_pg_type(str(dtype))),
        )
        for col, dtype in df.dtypes.items()
    ]


def materialize_snapshot_table(
    cursor, table_fqn: str, df: pd.DataFrame
) -> None:
    """
    Initialize snapshot table with initial data.
    Adds dbt snapshot system columns (valid_from, valid_to, is_current, is_deleted).
    """
    now = datetime.now(timezone.utc)
    for _, row in df.iterrows():
        insert_snapshot_row(cursor, table_fqn, row, now, deleted=False)


# ============================================================
# Incremental helpers
# ============================================================


def incremental_append(cursor, table_fqn, df):
    _insert_dataframe(cursor, table_fqn, df)


def incremental_delete_insert(cursor, table_fqn, df, unique_key):
    cursor.execute(
        delete_by_keys_sql(table_fqn, unique_key), (df[unique_key].tolist(),)
    )
    _insert_dataframe(cursor, table_fqn, df)


def incremental_merge(cursor, table_fqn, df, unique_key):
    cols = [str(c) for c in df.columns]
    stmt = incremental_merge_sql(table_fqn, cols, unique_key)

    sql_str = stmt.as_string(cursor.connection)

    for _, row in df.iterrows():
        cursor.execute(sql_str, list(row))


# ============================================================
# Snapshot helpers
# ============================================================


def insert_snapshot_row(cursor, table_fqn, row, now, deleted: bool):
    cols = list(row.index) + ["valid_from", "valid_to", "is_current", "is_deleted"]
    values = list(row.values) + [now, None, True, deleted]
    cursor.execute(insert_snapshot_row_sql(table_fqn, cols), values)


# ============================================================
# Snapshot (SCD2)
# ============================================================


def snapshot_scd2(
    cursor,
    table_fqn: str,
    df: pd.DataFrame,
    unique_key: Optional[Union[str, List[str]]] = None,
    *,
    strategy: str,
    updated_at: str | None,
    check_cols: list[str] | str | None,
    hard_deletes: str,
) -> None:
    """
    Execute SCD Type 2 snapshot logic (dbt snapshots equivalent).
    
    Handles:
    - New records (insert with valid_from=now, is_current=true)
    - Changed records (close old, insert new)
    - Deleted records (based on hard_deletes strategy)
    
    Args:
        cursor: psycopg2 cursor
        table_fqn: fully qualified table name (schema.table)
        df: incoming dataframe with current values
        unique_key: column(s) that uniquely identify a record
        strategy: "timestamp" or "check" for change detection
        updated_at: timestamp column name (for timestamp strategy)
        check_cols: columns to check for changes (for check strategy)
        hard_deletes: "ignore", "invalidate", or "new_record" for deleted records
    """
    now = datetime.now(timezone.utc)

    # Convert unique_key to list for consistent handling
    if isinstance(unique_key, str):
        unique_key_list = [unique_key]
    else:
        unique_key_list = list(unique_key) if unique_key else []
    
    # ============================================================
    # Helper: turn a raw tuple row into a dict keyed by column name
    # ============================================================
    def _row_to_dict(cursor_desc, row_tuple) -> dict:
        return {col.name: val for col, val in zip(cursor_desc, row_tuple)}

    # ============================================================
    # PHASE 1: Fetch existing current records
    # ============================================================
    cursor.execute(select_current_snapshot_sql(table_fqn))
    col_desc = cursor.description
    existing_rows: dict[tuple, dict] = {}
    for raw in cursor.fetchall():
        row_dict = _row_to_dict(col_desc, raw)
        key = tuple(row_dict[k] for k in unique_key_list)
        existing_rows[key] = row_dict

    # ============================================================
    # PHASE 2: Handle hard deletes (records in DB but not in incoming DF)
    # ============================================================
    incoming_keys = set()
    for _, row in df.iterrows():
        key_tuple = tuple(row[col] for col in unique_key_list)
        incoming_keys.add(key_tuple)

    deleted_keys = set(existing_rows.keys()) - incoming_keys

    for key_tuple in deleted_keys:
        if hard_deletes == "invalidate":
            cursor.execute(
                invalidate_snapshot_sql(table_fqn, unique_key),
                (now, *key_tuple) if len(unique_key_list) > 1 else (now, key_tuple[0]),
            )
        elif hard_deletes == "new_record":
            cursor.execute(
                close_snapshot_sql(table_fqn, unique_key),
                (now, *key_tuple) if len(unique_key_list) > 1 else (now, key_tuple[0]),
            )

    # ============================================================
    # PHASE 3: Process incoming records
    # ============================================================
    for _, row in df.iterrows():
        key_tuple = tuple(row[col] for col in unique_key_list)

        # Fetch existing current record for this key
        cursor.execute(
            select_current_by_key_sql(table_fqn, unique_key),
            key_tuple,
        )
        raw_existing = cursor.fetchone()

        if not raw_existing:
            # ------ NEW RECORD ------
            insert_snapshot_row(cursor, table_fqn, row, now, deleted=False)
            continue

        existing = _row_to_dict(cursor.description, raw_existing)

        # ------ CHECK FOR CHANGES ------
        changed = False

        if strategy == "timestamp":
            if updated_at and row[updated_at] > existing[updated_at]:
                changed = True

        elif strategy == "check":
            if check_cols == "all":
                check_cols_list = list(row.index)
            else:
                check_cols_list = check_cols if isinstance(check_cols, list) else [check_cols]

            for col in check_cols_list:
                if col not in unique_key_list and str(row[col]) != str(existing[col]):
                    changed = True
                    break

        if changed:
            # ------ CHANGED RECORD: Close old, insert new ------
            cursor.execute(
                close_snapshot_sql(table_fqn, unique_key),
                (now, *key_tuple) if len(unique_key_list) > 1 else (now, key_tuple[0]),
            )
            insert_snapshot_row(cursor, table_fqn, row, now, deleted=False)
