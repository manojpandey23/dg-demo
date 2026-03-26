from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from framework.model.config_models import (
    AssetSchema,
    IncrementalStrategy,
    Materialization,
    OnSchemaChange,
)
from framework.postgres.pghelper import (
    incremental_append,
    incremental_delete_insert,
    incremental_merge,
    materialize_snapshot_table,
    materialize_table,
    materialize_table_with_insert,
    snapshot_scd2,
)
from framework.postgres.schema.builder import build_pg_schema_from_config
from framework.postgres.sql.ddl import (
    add_column_sql,
    add_primary_key_sql,
    alter_column_type_sql,
    create_table_sql,
    drop_column_not_null_sql,
    drop_primary_key_sql,
    drop_table_sql,
    get_pk_constraint_name_sql,
    get_table_schema_sql,
    set_column_not_null_sql,
    table_exists_sql,
)
from psycopg2 import sql


def diff_schema(
    existing: Dict[str, Dict[str, Any]],
    incoming: Dict[str, Dict[str, Any]],
):
    existing_cols = set(existing)
    incoming_cols = set(incoming)

    added = incoming_cols - existing_cols
    removed = existing_cols - incoming_cols

    pg_type_changed = {}
    key_changed = {}
    nullable_changed = {}

    for col in existing_cols & incoming_cols:
        existing_col = existing[col]
        incoming_col = incoming[col]

        # --- pgType diff ---
        if existing_col.get("pgType") != incoming_col.get("pgType"):
            pg_type_changed[col] = {
                "from": existing_col.get("pgType"),
                "to": incoming_col.get("pgType"),
            }

        # --- primary key diff (force bool) ---
        if bool(existing_col.get("isKey")) != bool(incoming_col.get("isKey")):
            key_changed[col] = {
                "from": bool(existing_col.get("isKey")),
                "to": bool(incoming_col.get("isKey")),
            }

        # --- nullable diff (force bool, default True) ---
        if bool(existing_col.get("nullable", True)) != bool(
            incoming_col.get("nullable", True)
        ):
            nullable_changed[col] = {
                "from": bool(existing_col.get("nullable", True)),
                "to": bool(incoming_col.get("nullable", True)),
            }

    return {
        "added": added,
        "removed": removed,
        "pg_type_changed": pg_type_changed,
        "key_changed": key_changed,
        "nullable_changed": nullable_changed,
    }


# ============================================================
# Schema inspection
# ============================================================
def is_object_exist(cursor, table_fqn):
    cursor.execute(table_exists_sql(), (table_fqn,))
    return cursor.fetchone()[0] is not None


def get_postgres_schema(cursor, table_fqn) -> Dict[str, Dict[str, Any]]:
    schema, table = table_fqn.split(".")
    cursor.execute(get_table_schema_sql(), (schema, table))

    pg_schema = {
        col: {"name": col, "pgType": dtype, "isKey": is_pk, "nullable": nullable}
        for col, dtype, is_pk, nullable in cursor.fetchall()
    }

    return pg_schema


def get_pk_constraint_name(cursor, table_fqn):
    schema, table = table_fqn.split(".")
    cursor.execute(get_pk_constraint_name_sql(), (schema, table))
    row = cursor.fetchone()
    return row[0] if row else None


def pg_schema_to_sql_parts(
    pg_schema: Dict[str, Dict[str, Any]],
) -> Tuple[List[sql.SQL], List[sql.Identifier]]:
    cols = []
    keys = []

    for col in pg_schema.values():
        parts = [
            sql.Identifier(col["name"]),
            sql.SQL(col["pgType"]),
        ]

        if not col.get("nullable", True):
            parts.append(sql.SQL("NOT NULL"))

        cols.append(sql.SQL(" ").join(parts))

        if col.get("isKey"):
            keys.append(sql.Identifier(col["name"]))

    return cols, keys


# ============================================================
# Schema change application
# ============================================================


def apply_schema_changes(
    cursor,
    table_fqn,
    schema: list[AssetSchema],
    materialization: Materialization,
    on_schema_change: OnSchemaChange,
):
    exists = is_object_exist(cursor, table_fqn)
    declared_schema = build_pg_schema_from_config(schema, materialization)

    if not exists:
        cols, keys = pg_schema_to_sql_parts(declared_schema)
        cursor.execute(create_table_sql(table_fqn=table_fqn, cols=cols, keys=keys))
        return declared_schema

    if on_schema_change == OnSchemaChange.ignore:
        return declared_schema

    # --------------------------------------------------
    # Table Model
    # --------------------------------------------------
    if materialization == Materialization.table:
        cols, keys = pg_schema_to_sql_parts(declared_schema)
        cursor.execute(drop_table_sql(table_fqn=table_fqn))
        cursor.execute(create_table_sql(table_fqn=table_fqn, cols=cols, keys=keys))
        return declared_schema

    # --------------------------------------------------
    # Incremental Model
    # --------------------------------------------------
    if materialization == Materialization.incremental:
        existing_schema = get_postgres_schema(cursor, table_fqn)
        diff = diff_schema(existing_schema, declared_schema)

        if on_schema_change == OnSchemaChange.fail and any(diff.values()):
            raise ValueError(f"Schema mismatch: {diff}")

        # --------------------------------------------------
        # APPEND NEW COLUMNS (allowed for snapshots)
        # --------------------------------------------------
        if on_schema_change == OnSchemaChange.append_new_columns:
            # Add new columns
            for col in diff["added"]:
                cursor.execute(
                    add_column_sql(table_fqn, col, declared_schema[col]["pgType"])
                )
            return declared_schema

        # --------------------------------------------------
        # SYNC ALL COLUMNS (NOT allowed for snapshots)
        # --------------------------------------------------
        if on_schema_change == OnSchemaChange.sync_all_columns:
            # Add new columns
            for col in diff["added"]:
                cursor.execute(
                    add_column_sql(table_fqn, col, declared_schema[col]["pgType"])
                )

            # Modify DType
            for col, change in diff["pg_type_changed"].items():
                cursor.execute(alter_column_type_sql(table_fqn, col, change["to"]))

            # NULLABLE changes
            for col, change in diff["nullable_changed"].items():
                if change["to"] is False:
                    cursor.execute(set_column_not_null_sql(table_fqn, col))
                else:
                    cursor.execute(drop_column_not_null_sql(table_fqn, col))

            # PRIMARY KEY changes
            if diff["key_changed"]:
                constraint_name = get_pk_constraint_name(cursor, table_fqn)
                if constraint_name:
                    cursor.execute(drop_primary_key_sql(table_fqn, constraint_name))

                _, new_keys = pg_schema_to_sql_parts(declared_schema)
                if not new_keys:
                    raise ValueError("Primary key cannot be empty")

                cursor.execute(add_primary_key_sql(table_fqn, new_keys))

            return declared_schema

    # --------------------------------------------------
    # Snapshot Model
    # --------------------------------------------------
    if materialization == Materialization.snapshot:
        if on_schema_change == OnSchemaChange.fail and any(diff.values()):
            raise ValueError(f"Schema mismatch: {diff}")

        if on_schema_change == OnSchemaChange.sync_all_columns:
            raise ValueError(
                "OnSchemaChange.sync_all_columns is not allowed for snapshot materialization"
            )

        if on_schema_change == OnSchemaChange.append_new_columns:
            # Add new columns
            for col in diff["added"]:
                cursor.execute(
                    add_column_sql(table_fqn, col, declared_schema[col]["pgType"])
                )
            return declared_schema

    return declared_schema


# ============================================================
# ATOMIC DDL + DML OPERATION
# ============================================================


def apply_schema_and_materialize(
    cursor,
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
    """
    Atomically apply schema changes and materialize data.

    PHASE 1: Schema Management (DDL)
      ├─ Create/alter table to match config
      └─ Check table existence

    PHASE 2: Data Materialization (DML)
      ├─ TABLE: dbt-like swap (create staging, load, rename)
      ├─ INCREMENTAL: append/merge/delete+insert
      └─ SNAPSHOT: SCD Type 2 versioning

    Both phases happen together in same transaction.
    If either fails, transaction rolls back completely.

    Args:
        cursor: psycopg2 cursor (in transaction)
        table_fqn: fully qualified table name (schema.table)
        target_df: dataframe with transformed data
        schema: column schema configuration
        materialization: table|incremental|snapshot
        on_schema_change: ignore|append_new_columns|sync_all_columns|fail
        inc_strategy: append|merge|delete_insert (for incremental)
        unique_key: unique column(s) for incremental/snapshot
        snapshot_strategy: timestamp|check (for snapshot)
        updated_at: timestamp column (for timestamp strategy)
        check_cols: columns to check (for check strategy)
        hard_deletes: ignore|invalidate|new_record (for snapshot)

    Returns:
        {
            "rows_loaded": int,
            "table_created": bool,
            "schema_updated": bool,
            "materialization_type": str,
        }
    """
    # ============================================================
    # PHASE 1: SCHEMA MANAGEMENT (DDL)
    # ============================================================
    exists = is_object_exist(cursor, table_fqn)
    declared_schema = build_pg_schema_from_config(schema, materialization)

    if not exists:
        # First run: create table
        cols, keys = pg_schema_to_sql_parts(declared_schema)
        cursor.execute(create_table_sql(table_fqn=table_fqn, cols=cols, keys=keys))
        schema_updated = True
    else:
        # Existing table: check for schema changes
        schema_updated = _apply_schema_updates(
            cursor, table_fqn, schema, materialization, on_schema_change, declared_schema
        )

    # ============================================================
    # PHASE 2: DATA MATERIALIZATION (DML)
    # DDL and DML happen back-to-back, close together
    # ============================================================

    if materialization == Materialization.table:
        # TABLE materialization: dbt-like swap operation
        _materialize_table_with_swap(cursor, table_fqn, target_df, declared_schema)
        rows_loaded = len(target_df)

    elif materialization == Materialization.incremental:
        # INCREMENTAL materialization
        if not exists:
            # First run: insert all data
            materialize_table_with_insert(cursor, table_fqn, target_df)
        else:
            # Subsequent runs: apply strategy
            if inc_strategy == IncrementalStrategy.append:
                incremental_append(cursor, table_fqn, target_df)
            elif inc_strategy == IncrementalStrategy.merge:
                incremental_merge(cursor, table_fqn, target_df, unique_key)
            elif inc_strategy == IncrementalStrategy.delete_insert:
                incremental_delete_insert(cursor, table_fqn, target_df, unique_key)
            else:
                raise ValueError(f"Unknown incremental strategy: {inc_strategy}")

        rows_loaded = len(target_df)

    elif materialization == Materialization.snapshot:
        # SNAPSHOT materialization (SCD Type 2)
        if not exists:
            # First run: initialize snapshot with system columns
            materialize_snapshot_table(cursor, table_fqn, target_df)
        else:
            # Subsequent runs: apply SCD Type 2 logic
            snapshot_scd2(
                cursor,
                table_fqn,
                target_df,
                unique_key=unique_key,
                strategy=snapshot_strategy or "check",
                updated_at=updated_at,
                check_cols=check_cols,
                hard_deletes=hard_deletes or "ignore",
            )

        rows_loaded = len(target_df)

    else:
        raise ValueError(f"Unknown materialization type: {materialization}")

    # ============================================================
    # RETURN METADATA
    # ============================================================
    return {
        "rows_loaded": rows_loaded,
        "table_created": not exists,
        "schema_updated": schema_updated,
        "materialization_type": materialization.value,
    }


def _materialize_table_with_swap(cursor, table_fqn: str, df: pd.DataFrame, declared_schema: Dict[str, Dict[str, Any]]) -> None:
    """
    dbt-like swap operation for TABLE materialization.

    Semantics:
      1. Create staging table with same schema as target
      2. Load data into staging table
      3. Rename old table to backup (_backup)
      4. Rename staging table to target name
      5. Drop backup

    This is atomic at the database level and provides a zero-downtime swap.
    """
    schema_part, table_part = table_fqn.split(".")
    staging_fqn = f"{schema_part}.{table_part}_staging"
    backup_fqn = f"{schema_part}.{table_part}_backup"

    try:
        # 1. Create staging table
        cols, keys = pg_schema_to_sql_parts({k: v for k, v in declared_schema.items()})
        cursor.execute(create_table_sql(table_fqn=staging_fqn, cols=cols, keys=keys))

        # 2. Load data into staging
        materialize_table(cursor, staging_fqn, df, declared_schema)

        # 3. Rename old to backup (if exists)
        if is_object_exist(cursor, table_fqn):
            cursor.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    sql.Identifier(schema_part, table_part),
                    sql.Identifier(f"{table_part}_backup"),
                )
            )

        # 4. Rename staging to target
        cursor.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(schema_part, f"{table_part}_staging"),
                sql.Identifier(table_part),
            )
        )

        # 5. Drop backup
        if is_object_exist(cursor, backup_fqn):
            cursor.execute(drop_table_sql(backup_fqn))

    except Exception:
        # Cleanup on failure
        if is_object_exist(cursor, staging_fqn):
            cursor.execute(drop_table_sql(staging_fqn))
        raise


def _apply_schema_updates(
    cursor,
    table_fqn: str,
    schema: list[AssetSchema],
    materialization: Materialization,
    on_schema_change: OnSchemaChange,
    declared_schema: Dict[str, Dict[str, Any]],
) -> bool:
    """
    Apply schema changes based on policy.
    Returns True if schema was updated, False otherwise.
    """
    existing_schema = get_postgres_schema(cursor, table_fqn)
    diff = diff_schema(existing_schema, declared_schema)

    # No changes detected
    if not any(diff.values()):
        return False

    if on_schema_change == OnSchemaChange.fail:
        raise ValueError(f"Schema mismatch: {diff}")

    if on_schema_change == OnSchemaChange.ignore:
        return False

    if on_schema_change == OnSchemaChange.append_new_columns:
        # Add new columns only
        for col in diff["added"]:
            cursor.execute(
                add_column_sql(table_fqn, col, declared_schema[col]["pgType"])
            )
        return True

    if on_schema_change == OnSchemaChange.sync_all_columns:
        # Full schema sync (not allowed for snapshots)
        if materialization == Materialization.snapshot:
            raise ValueError(
                "sync_all_columns is not allowed for snapshot materialization"
            )

        # Add new columns
        for col in diff["added"]:
            cursor.execute(
                add_column_sql(table_fqn, col, declared_schema[col]["pgType"])
            )

        # Alter types
        for col, change in diff["pg_type_changed"].items():
            cursor.execute(alter_column_type_sql(table_fqn, col, change["to"]))

        # Update nullability
        for col, change in diff["nullable_changed"].items():
            if change["to"] is False:
                cursor.execute(set_column_not_null_sql(table_fqn, col))
            else:
                cursor.execute(drop_column_not_null_sql(table_fqn, col))

        # Update primary keys
        if diff["key_changed"]:
            constraint_name = get_pk_constraint_name(cursor, table_fqn)
            if constraint_name:
                cursor.execute(drop_primary_key_sql(table_fqn, constraint_name))

            _, new_keys = pg_schema_to_sql_parts(declared_schema)
            if new_keys:
                cursor.execute(add_primary_key_sql(table_fqn, new_keys))

        return True

    return False

