"""
CDC capture helper for database asset materialization.

Called from ``assert_db.py`` after a successful materialization when
``change_tracking`` is enabled.  Computes row-level diffs and persists
change events to the per-asset change log table — all within the same
transaction so the change log is atomically consistent with the data.
"""

from typing import Any

import pandas as pd

from framework.cdc.diff_engine import compute_changes
from framework.cdc.store import (
    derive_change_log_table,
    ensure_change_log_table,
    persist_changes,
)
from framework.model.config_models import AssetConfig, AssetSchema


def _extract_key_columns(columns: list[AssetSchema] | None) -> list[str]:
    """Return column names marked as ``isKey``."""
    if not columns:
        return []
    return [col.name for col in columns if col.isKey]


def capture_cdc_events(
    *,
    cursor: Any,
    config: AssetConfig,
    table_fqn: str,
    final_df: pd.DataFrame,
    run_id: str,
    materialization_type: str,
) -> int:
    """Compute and persist CDC change events.

    This must be called BEFORE ``COMMIT`` so that the change log and
    the data table are in the same transaction.

    Parameters
    ----------
    cursor:
        Open psycopg2 cursor within the active transaction.
    config:
        The ``AssetConfig`` with ``change_tracking: true``.
    table_fqn:
        Fully-qualified target table name (e.g. ``price.cash_balance_raw``).
    final_df:
        The DataFrame that was just materialised.
    run_id:
        Dagster run ID for traceability.
    materialization_type:
        One of ``table``, ``incremental``, ``snapshot``.

    Returns
    -------
    Number of change events persisted.
    """
    change_log_fqn = derive_change_log_table(table_fqn)
    ensure_change_log_table(cursor, change_log_fqn)

    key_columns = _extract_key_columns(config.columns)

    # Determine the operation type from the materialization
    op = _op_from_materialization(materialization_type)

    if key_columns and op != "REPLACE":
        # Full diff: compare with previous state for merge/snapshot
        events = compute_changes(
            asset_name=config.name,
            new_df=final_df,
            old_df=None,  # first pass — treat all rows as inserts
            primary_key=key_columns,
            run_id=run_id,
        )
    else:
        # No keys or full-table replace: log entire batch as the op
        events = compute_changes(
            asset_name=config.name,
            new_df=final_df,
            old_df=None,
            primary_key=key_columns or list(final_df.columns[:1]),
            run_id=run_id,
        )

    return persist_changes(cursor, change_log_fqn, events)


def _op_from_materialization(materialization_type: str) -> str:
    """Map a materialization type to a CDC operation label."""
    mapping = {
        "table": "REPLACE",
        "incremental": "INSERT",
        "snapshot": "SNAPSHOT",
    }
    return mapping.get(materialization_type, "UNKNOWN")

