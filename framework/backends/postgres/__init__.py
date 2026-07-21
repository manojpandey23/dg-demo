"""PostgresBackend — full Postgres database backend.

Contains DDL generators, bulk loaders (COPY FROM STDIN), schema drift
detection, materialization strategies, and SCD2 snapshot logic.
"""

from __future__ import annotations

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


@backend_handler("postgres")
class PostgresBackend(DatabaseBackend):

    def __init__(self) -> None:
        self._connection: Any = None

    def set_connection(self, connection: Any) -> None:
        self._connection = connection

    def get_cursor(self) -> Any:
        return self._connection.cursor()

    def close_cursor(self, cursor: Any) -> None:
        cursor.close()

    def begin_transaction(self, cursor: Any) -> None:
        from .sql.ddl import begin_txn

        cursor.execute(begin_txn())

    def commit_transaction(self, cursor: Any) -> None:
        from .sql.ddl import commit_txn

        cursor.execute(commit_txn())

    def rollback_transaction(self, cursor: Any) -> None:
        from .sql.ddl import rollback_txn

        cursor.execute(rollback_txn())

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
        from .schema.apply import (
            apply_schema_and_materialize as pg_apply,
        )

        return pg_apply(
            cursor=cursor,
            table_fqn=table_fqn,
            target_df=target_df,
            schema=schema,
            materialization=materialization,
            on_schema_change=on_schema_change,
            inc_strategy=inc_strategy,
            unique_key=unique_key,
            snapshot_strategy=snapshot_strategy,
            updated_at=updated_at,
            check_cols=check_cols,
            hard_deletes=hard_deletes,
        )

    def ensure_change_log_table(
        self, cursor: Any, change_log_fqn: str
    ) -> None:
        from framework.cdc.store import ensure_change_log_table

        ensure_change_log_table(cursor, change_log_fqn)

    def persist_changes(
        self, cursor: Any, change_log_fqn: str, events: list
    ) -> int:
        from framework.cdc.store import persist_changes

        return persist_changes(cursor, change_log_fqn, events)

    def fetch_pending_changes(
        self,
        cursor: Any,
        change_log_fqn: str,
        last_id: int,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        from framework.cdc.store import fetch_pending_changes

        return fetch_pending_changes(cursor, change_log_fqn, last_id, limit)

    def derive_change_log_table(self, table_fqn: str) -> str:
        from framework.cdc.store import derive_change_log_table

        return derive_change_log_table(table_fqn)
