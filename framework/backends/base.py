"""Abstract base class defining the database backend contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from framework.model.config_models import (
    AssetSchema,
    IncrementalStrategy,
    Materialization,
    OnSchemaChange,
)


class DatabaseBackend(ABC):
    """Contract for a pluggable database backend.

    Instantiated once per asset at BUILD time (no connection yet).
    The connection is injected at RUNTIME via ``set_connection()``.
    """

    @abstractmethod
    def set_connection(self, connection: Any) -> None: ...

    @abstractmethod
    def get_cursor(self) -> Any: ...

    @abstractmethod
    def close_cursor(self, cursor: Any) -> None: ...

    @abstractmethod
    def begin_transaction(self, cursor: Any) -> None: ...

    @abstractmethod
    def commit_transaction(self, cursor: Any) -> None: ...

    @abstractmethod
    def rollback_transaction(self, cursor: Any) -> None: ...

    @abstractmethod
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
    ) -> Dict[str, Any]: ...

    @abstractmethod
    def ensure_change_log_table(
        self, cursor: Any, change_log_fqn: str
    ) -> None: ...

    @abstractmethod
    def persist_changes(
        self, cursor: Any, change_log_fqn: str, events: list
    ) -> int: ...

    @abstractmethod
    def fetch_pending_changes(
        self, cursor: Any, change_log_fqn: str, last_id: int, limit: int = 100
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def derive_change_log_table(self, table_fqn: str) -> str: ...
