"""SnowflakeBackend — real implementation for Snowflake data warehouse.

All snowflake-connector-python imports are lazy so this module can be
imported (and the class registered) even without the package installed.
The actual import happens at ``set_connection()`` time.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from framework.backends.base import DatabaseBackend
from framework.backends.registry import backend_handler
from framework.model.config_models import (
    AssetSchema,
    IncrementalStrategy,
    Materialization,
    OnSchemaChange,
)

_DTYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "text": "VARCHAR",
    "int": "NUMBER(38,0)",
    "integer": "NUMBER(38,0)",
    "float": "FLOAT",
    "double": "FLOAT",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP_NTZ",
    "timestamp": "TIMESTAMP_NTZ",
}


def _sf_type(dtype: str) -> str:
    key = dtype.lower()
    if key not in _DTYPE_MAP:
        raise ValueError(f"Unsupported dtype for Snowflake: {dtype}")
    return _DTYPE_MAP[key]


def _qi(name: str) -> str:
    """Quote a Snowflake identifier."""
    return f'"{name}"'


@backend_handler("snowflake")
class SnowflakeBackend(DatabaseBackend):

    def __init__(self) -> None:
        self._connection: Any = None

    # -- connection lifecycle --

    def set_connection(self, connection: Any) -> None:
        self._connection = connection

    def get_cursor(self) -> Any:
        return self._connection.cursor()

    def close_cursor(self, cursor: Any) -> None:
        cursor.close()

    # -- transaction control --

    def begin_transaction(self, cursor: Any) -> None:
        cursor.execute("BEGIN")

    def commit_transaction(self, cursor: Any) -> None:
        cursor.execute("COMMIT")

    def rollback_transaction(self, cursor: Any) -> None:
        cursor.execute("ROLLBACK")

    # -- schema helpers --

    def _build_schema_info(
        self,
        schema: list[AssetSchema],
        materialization: Materialization,
    ) -> dict[str, dict[str, Any]]:
        columns: dict[str, dict[str, Any]] = {}
        for col in schema:
            columns[col.name] = {
                "name": col.name,
                "nativeType": _sf_type(col.dtype),
                "pgType": _sf_type(col.dtype),
                "dtype": col.dtype,
                "isKey": (
                    False
                    if materialization == Materialization.snapshot
                    else col.isKey
                ),
                "nullable": False if col.isKey else col.nullable,
            }

        if materialization == Materialization.snapshot:
            for name, native, nullable in [
                ("valid_from", "TIMESTAMP_NTZ", False),
                ("valid_to", "TIMESTAMP_NTZ", True),
                ("is_current", "BOOLEAN", False),
                ("is_deleted", "BOOLEAN", False),
            ]:
                columns[name] = {
                    "name": name,
                    "nativeType": native,
                    "pgType": native,
                    "dtype": "datetime" if "TIMESTAMP" in native else "boolean",
                    "isKey": False,
                    "nullable": nullable,
                }
        return columns

    def _table_exists(self, cursor: Any, table_fqn: str) -> bool:
        schema_name, table_name = table_fqn.split(".", 1)
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema_name.upper(), table_name.upper()),
        )
        return cursor.fetchone()[0] > 0

    def _get_existing_schema(
        self, cursor: Any, table_fqn: str
    ) -> dict[str, dict[str, Any]]:
        schema_name, table_name = table_fqn.split(".", 1)
        cursor.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (schema_name.upper(), table_name.upper()),
        )
        result: dict[str, dict[str, Any]] = {}
        for col_name, data_type, is_nullable in cursor.fetchall():
            result[col_name.lower()] = {
                "name": col_name.lower(),
                "nativeType": data_type,
                "pgType": data_type,
                "isKey": False,
                "nullable": is_nullable == "YES",
            }
        return result

    def _create_table(
        self, cursor: Any, table_fqn: str, columns: dict[str, dict[str, Any]]
    ) -> None:
        col_defs = []
        key_cols = []
        for col in columns.values():
            parts = f'{_qi(col["name"])} {col["nativeType"]}'
            if not col.get("nullable", True):
                parts += " NOT NULL"
            col_defs.append(parts)
            if col.get("isKey"):
                key_cols.append(_qi(col["name"]))

        ddl = f"CREATE TABLE IF NOT EXISTS {table_fqn} ({', '.join(col_defs)}"
        if key_cols:
            ddl += f", PRIMARY KEY ({', '.join(key_cols)})"
        ddl += ")"
        cursor.execute(ddl)

    def _handle_schema_drift(
        self,
        cursor: Any,
        table_fqn: str,
        incoming: dict[str, dict[str, Any]],
        on_schema_change: OnSchemaChange,
    ) -> bool:
        from framework.backends.postgres.schema.apply import diff_schema

        existing = self._get_existing_schema(cursor, table_fqn)
        diff = diff_schema(existing, incoming)

        if not any(diff.values()):
            return False

        if on_schema_change == OnSchemaChange.fail:
            raise ValueError(f"Schema drift detected on {table_fqn}: {diff}")
        if on_schema_change == OnSchemaChange.ignore:
            return False
        if on_schema_change == OnSchemaChange.append_new_columns:
            for col_name in diff["added"]:
                col = incoming[col_name]
                cursor.execute(
                    f"ALTER TABLE {table_fqn} ADD COLUMN "
                    f'{_qi(col_name)} {col["nativeType"]}'
                )
            return bool(diff["added"])
        if on_schema_change == OnSchemaChange.sync_all_columns:
            for col_name in diff["added"]:
                col = incoming[col_name]
                cursor.execute(
                    f"ALTER TABLE {table_fqn} ADD COLUMN "
                    f'{_qi(col_name)} {col["nativeType"]}'
                )
            for col_name, change in diff.get("pg_type_changed", {}).items():
                cursor.execute(
                    f"ALTER TABLE {table_fqn} ALTER COLUMN "
                    f'{_qi(col_name)} SET DATA TYPE {change["to"]}'
                )
            return True
        return False

    # -- bulk loading --

    def _bulk_load(
        self,
        cursor: Any,
        table_fqn: str,
        df: pd.DataFrame,
        columns: dict[str, dict[str, Any]],
    ) -> None:
        from snowflake.connector.pandas_tools import write_pandas

        col_names = [c for c in columns if c in df.columns]
        load_df = df[col_names].copy()
        schema_name, table_name = table_fqn.split(".", 1)
        write_pandas(
            self._connection,
            load_df,
            table_name.upper(),
            schema=schema_name.upper(),
            auto_create_table=False,
        )

    # -- materialization strategies --

    def _mat_table(
        self, cursor: Any, table_fqn: str, df: pd.DataFrame,
        columns: dict[str, dict[str, Any]], exists: bool,
    ) -> None:
        if exists:
            cursor.execute(f"TRUNCATE TABLE {table_fqn}")
        self._bulk_load(cursor, table_fqn, df, columns)

    def _mat_incremental(
        self, cursor: Any, table_fqn: str, df: pd.DataFrame,
        columns: dict[str, dict[str, Any]], exists: bool,
        inc_strategy: Optional[IncrementalStrategy],
        unique_key: Optional[Union[str, List[str]]],
    ) -> None:
        if not exists:
            self._bulk_load(cursor, table_fqn, df, columns)
            return

        if inc_strategy == IncrementalStrategy.append:
            self._bulk_load(cursor, table_fqn, df, columns)
        elif inc_strategy == IncrementalStrategy.merge:
            self._merge(cursor, table_fqn, df, columns, unique_key)
        elif inc_strategy == IncrementalStrategy.delete_insert:
            self._delete_insert(cursor, table_fqn, df, columns, unique_key)
        else:
            raise ValueError(f"Unknown incremental strategy: {inc_strategy}")

    def _merge(
        self, cursor: Any, table_fqn: str, df: pd.DataFrame,
        columns: dict[str, dict[str, Any]],
        unique_key: Optional[Union[str, List[str]]],
    ) -> None:
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        if not unique_key:
            raise ValueError("unique_key required for merge strategy")

        col_names = list(columns.keys())
        non_key = [c for c in col_names if c not in unique_key]

        schema_name, table_name = table_fqn.split(".", 1)
        staging = f"{schema_name}.__staging_{table_name}"
        self._create_table(cursor, staging, columns)
        try:
            self._bulk_load(cursor, staging, df, columns)
            on_clause = " AND ".join(
                f"target.{_qi(k)} = source.{_qi(k)}" for k in unique_key
            )
            update_set = ", ".join(
                f"target.{_qi(c)} = source.{_qi(c)}" for c in non_key
            )
            insert_cols = ", ".join(_qi(c) for c in col_names)
            insert_vals = ", ".join(f"source.{_qi(c)}" for c in col_names)

            cursor.execute(
                f"MERGE INTO {table_fqn} AS target "
                f"USING {staging} AS source ON {on_clause} "
                f"WHEN MATCHED THEN UPDATE SET {update_set} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")

    def _delete_insert(
        self, cursor: Any, table_fqn: str, df: pd.DataFrame,
        columns: dict[str, dict[str, Any]],
        unique_key: Optional[Union[str, List[str]]],
    ) -> None:
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        if not unique_key:
            raise ValueError("unique_key required for delete+insert strategy")

        schema_name, table_name = table_fqn.split(".", 1)
        staging = f"{schema_name}.__staging_{table_name}"
        self._create_table(cursor, staging, columns)
        try:
            self._bulk_load(cursor, staging, df, columns)
            on_clause = " AND ".join(
                f"target.{_qi(k)} = staging.{_qi(k)}" for k in unique_key
            )
            cursor.execute(
                f"DELETE FROM {table_fqn} AS target "
                f"USING {staging} AS staging WHERE {on_clause}"
            )
            col_names = list(columns.keys())
            insert_cols = ", ".join(_qi(c) for c in col_names)
            cursor.execute(
                f"INSERT INTO {table_fqn} ({insert_cols}) "
                f"SELECT {insert_cols} FROM {staging}"
            )
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")

    # -- snapshot SCD2 --

    def _mat_snapshot(
        self, cursor: Any, table_fqn: str, df: pd.DataFrame,
        columns: dict[str, dict[str, Any]], exists: bool,
        unique_key: Optional[Union[str, List[str]]],
        strategy: str, updated_at: Optional[str],
        check_cols: Optional[Union[List[str], str]],
        hard_deletes: str,
    ) -> None:
        now = datetime.now(timezone.utc)
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        if not exists:
            snapshot_df = df.copy()
            snapshot_df["valid_from"] = now
            snapshot_df["valid_to"] = None
            snapshot_df["is_current"] = True
            snapshot_df["is_deleted"] = False
            self._bulk_load(cursor, table_fqn, snapshot_df, columns)
            return

        cursor.execute(
            f"SELECT * FROM {table_fqn} WHERE is_current = TRUE"
        )
        desc = [d[0].lower() for d in cursor.description]
        existing_rows: dict[tuple, dict] = {}
        for raw in cursor.fetchall():
            row_dict = dict(zip(desc, raw))
            key = tuple(row_dict[k] for k in unique_key) if unique_key else ()
            existing_rows[key] = row_dict

        incoming_keys: set[tuple] = set()
        for _, row in df.iterrows():
            key = tuple(row[k] for k in unique_key) if unique_key else ()
            incoming_keys.add(key)

        # hard deletes
        for key in set(existing_rows.keys()) - incoming_keys:
            where = " AND ".join(
                f"{_qi(k)} = %s" for k in unique_key
            )
            if hard_deletes == "invalidate":
                cursor.execute(
                    f"UPDATE {table_fqn} SET valid_to = %s, "
                    f"is_current = FALSE, is_deleted = TRUE "
                    f"WHERE {where} AND is_current = TRUE",
                    [now, *key],
                )

        # upserts
        for _, row in df.iterrows():
            key = tuple(row[k] for k in unique_key) if unique_key else ()
            existing = existing_rows.get(key)

            if not existing:
                data_cols = list(row.index)
                all_cols = data_cols + ["valid_from", "valid_to", "is_current", "is_deleted"]
                vals = list(row.values) + [now, None, True, False]
                placeholders = ", ".join(["%s"] * len(vals))
                col_str = ", ".join(_qi(c) for c in all_cols)
                cursor.execute(
                    f"INSERT INTO {table_fqn} ({col_str}) VALUES ({placeholders})",
                    vals,
                )
                continue

            changed = False
            if strategy == "timestamp" and updated_at:
                if row.get(updated_at) is not None and row[updated_at] > existing.get(updated_at):
                    changed = True
            elif strategy == "check":
                cols_to_check = (
                    [c for c in row.index if c not in (unique_key or [])]
                    if check_cols == "all" or check_cols is None
                    else (check_cols if isinstance(check_cols, list) else [check_cols])
                )
                for col in cols_to_check:
                    if col not in (unique_key or []) and str(row.get(col)) != str(existing.get(col)):
                        changed = True
                        break

            if changed:
                where = " AND ".join(
                    f"{_qi(k)} = %s" for k in unique_key
                )
                cursor.execute(
                    f"UPDATE {table_fqn} SET valid_to = %s, is_current = FALSE "
                    f"WHERE {where} AND is_current = TRUE",
                    [now, *key],
                )
                data_cols = list(row.index)
                all_cols = data_cols + ["valid_from", "valid_to", "is_current", "is_deleted"]
                vals = list(row.values) + [now, None, True, False]
                placeholders = ", ".join(["%s"] * len(vals))
                col_str = ", ".join(_qi(c) for c in all_cols)
                cursor.execute(
                    f"INSERT INTO {table_fqn} ({col_str}) VALUES ({placeholders})",
                    vals,
                )

    # -- main orchestrator --

    def apply_schema_and_materialize(
        self,
        cursor: Any,
        table_fqn: str,
        target_df: pd.DataFrame,
        schema: list[AssetSchema],
        materialization: Materialization,
        on_schema_change: OnSchemaChange,
        inc_strategy: Optional[IncrementalStrategy] = None,
        unique_key: Optional[Union[str, List[str]]] = None,
        snapshot_strategy: Optional[str] = None,
        updated_at: Optional[str] = None,
        check_cols: Optional[Union[List[str], str]] = None,
        hard_deletes: Optional[str] = None,
    ) -> Dict[str, Any]:
        columns = self._build_schema_info(schema, materialization)
        exists = self._table_exists(cursor, table_fqn)

        if not exists:
            self._create_table(cursor, table_fqn, columns)
            schema_updated = True
        else:
            schema_updated = self._handle_schema_drift(
                cursor, table_fqn, columns, on_schema_change
            )

        if materialization == Materialization.table:
            self._mat_table(cursor, table_fqn, target_df, columns, exists)
        elif materialization == Materialization.incremental:
            self._mat_incremental(
                cursor, table_fqn, target_df, columns, exists,
                inc_strategy, unique_key,
            )
        elif materialization == Materialization.snapshot:
            self._mat_snapshot(
                cursor, table_fqn, target_df, columns, exists,
                unique_key, snapshot_strategy or "check",
                updated_at, check_cols, hard_deletes or "ignore",
            )
        else:
            raise ValueError(f"Unknown materialization: {materialization}")

        return {
            "rows_loaded": len(target_df),
            "table_created": not exists,
            "schema_updated": schema_updated,
            "materialization_type": materialization.value,
        }

    # -- CDC store operations --

    def derive_change_log_table(self, table_fqn: str) -> str:
        schema_name, table_name = table_fqn.split(".", 1)
        return f"{schema_name}.__cdc_{table_name}"

    def ensure_change_log_table(
        self, cursor: Any, change_log_fqn: str
    ) -> None:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {change_log_fqn} (
                id          NUMBER AUTOINCREMENT PRIMARY KEY,
                asset_name  VARCHAR     NOT NULL,
                op          VARCHAR     NOT NULL,
                pk          VARIANT     NOT NULL,
                before_data VARIANT,
                after_data  VARIANT,
                run_id      VARCHAR     NOT NULL,
                captured_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
            )
        """)

    def persist_changes(
        self, cursor: Any, change_log_fqn: str, events: list
    ) -> int:
        if not events:
            return 0
        for e in events:
            cursor.execute(
                f"INSERT INTO {change_log_fqn} "
                f"(asset_name, op, pk, before_data, after_data, run_id, captured_at) "
                f"VALUES (%s, %s, PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), %s, CURRENT_TIMESTAMP())",
                (
                    e.asset,
                    e.op,
                    json.dumps(e.pk),
                    json.dumps(e.before) if e.before else "null",
                    json.dumps(e.after) if e.after else "null",
                    e.run_id,
                ),
            )
        return len(events)

    def fetch_pending_changes(
        self,
        cursor: Any,
        change_log_fqn: str,
        last_id: int,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.ensure_change_log_table(cursor, change_log_fqn)
        cursor.execute(
            f"SELECT id, asset_name, op, pk, before_data, after_data, "
            f"run_id, captured_at FROM {change_log_fqn} "
            f"WHERE id > %s ORDER BY id LIMIT %s",
            (last_id, limit),
        )
        cols = [d[0].lower() for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
