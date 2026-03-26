"""
CDC (Change Data Capture) module.

Provides Snowflake-stream-like change tracking for database assets.
When ``change_tracking: true`` is set on an asset, every materialization
records row-level change events to a per-asset change log table.  A CDC
sensor polls the log and dispatches events via configured streams.
"""

from framework.cdc.diff_engine import compute_changes
from framework.cdc.store import (
    derive_change_log_table,
    ensure_change_log_table,
    fetch_pending_changes,
    persist_changes,
)

__all__ = [
    "compute_changes",
    "derive_change_log_table",
    "ensure_change_log_table",
    "fetch_pending_changes",
    "persist_changes",
]

