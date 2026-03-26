"""
CDC change log store.

Manages per-asset change log tables in PostgreSQL.  Each asset with
``change_tracking: true`` gets its own ``__cdc_{table}`` table in the
same schema as the asset's target table.

All SQL uses ``psycopg2.sql`` for safe identifier handling.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg2 import sql
from psycopg2.extras import execute_values

from framework.cdc.model.events import ChangeEvent


def _json_serial(obj: Any) -> Any:
    """JSON serializer for types not handled by the default encoder."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    # numpy / pandas scalar types
    type_name = type(obj).__name__
    if "int" in type_name or "float" in type_name:
        return float(obj)
    if "bool" in type_name:
        return bool(obj)
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type_name} is not JSON serializable")


# ------------------------------------------------------------------
# Table naming convention
# ------------------------------------------------------------------

def derive_change_log_table(table_fqn: str) -> str:
    """Derive the change log table FQN from the asset's target table.

    Example::

        "price.cash_balance_api_raw" → "price.__cdc_cash_balance_api_raw"
    """
    schema, table = table_fqn.split(".", 1)
    return f"{schema}.__cdc_{table}"


def _table_ident(table_fqn: str) -> sql.Composed:
    """Return a safe ``schema.table`` identifier."""
    schema, table = table_fqn.split(".", 1)
    return sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])


# ------------------------------------------------------------------
# DDL — ensure the change log table exists
# ------------------------------------------------------------------

_CREATE_CHANGE_LOG_SQL = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {tbl} (
        id          BIGSERIAL       PRIMARY KEY,
        asset_name  TEXT            NOT NULL,
        op          TEXT            NOT NULL,
        pk          JSONB           NOT NULL,
        before_data JSONB,
        after_data  JSONB,
        run_id      TEXT            NOT NULL,
        captured_at TIMESTAMPTZ     NOT NULL DEFAULT now()
    )
""")

_CREATE_INDEX_SQL = sql.SQL(
    "CREATE INDEX IF NOT EXISTS {idx} ON {tbl} (id)"
)


def ensure_change_log_table(cursor: Any, change_log_fqn: str) -> None:
    """Create the change log table and index if they don't exist.

    Safe to call on every materialization — uses ``IF NOT EXISTS``.
    """
    tbl = _table_ident(change_log_fqn)
    schema, table = change_log_fqn.split(".", 1)
    idx_name = sql.Identifier(f"idx_{table}_id")

    cursor.execute(_CREATE_CHANGE_LOG_SQL.format(tbl=tbl))
    cursor.execute(_CREATE_INDEX_SQL.format(idx=idx_name, tbl=tbl))


# ------------------------------------------------------------------
# Write — persist change events
# ------------------------------------------------------------------

def persist_changes(
    cursor: Any,
    change_log_fqn: str,
    events: list[ChangeEvent],
) -> int:
    """Bulk-insert change events into the per-asset change log.

    Returns the number of rows inserted.
    """
    if not events:
        return 0

    tbl = _table_ident(change_log_fqn)

    rows = [
        (
            e.asset,
            e.op,
            json.dumps(e.pk, default=_json_serial),
            json.dumps(e.before, default=_json_serial) if e.before else None,
            json.dumps(e.after, default=_json_serial) if e.after else None,
            e.run_id,
            e.ts,
        )
        for e in events
    ]

    insert_sql = sql.SQL(
        "INSERT INTO {tbl} "
        "(asset_name, op, pk, before_data, after_data, run_id, captured_at) "
        "VALUES %s"
    ).format(tbl=tbl)

    execute_values(cursor, insert_sql, rows)
    return len(rows)


# ------------------------------------------------------------------
# Read — fetch pending changes (used by CDC sensor)
# ------------------------------------------------------------------

_FETCH_SQL = sql.SQL("""
    SELECT id, asset_name, op, pk, before_data, after_data, run_id, captured_at
    FROM {tbl}
    WHERE id > %s
    ORDER BY id
    LIMIT %s
""")


def fetch_pending_changes(
    cursor: Any,
    change_log_fqn: str,
    last_id: int,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Read up to *limit* change events after *last_id*.

    Creates the change log table if it does not yet exist (the sensor
    may start polling before the first asset materialization).

    Returns a list of dicts suitable for serialisation.
    """
    ensure_change_log_table(cursor, change_log_fqn)

    tbl = _table_ident(change_log_fqn)
    cursor.execute(_FETCH_SQL.format(tbl=tbl), (last_id, limit))

    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
