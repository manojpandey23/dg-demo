"""
Improved Pydantic models for framework configuration.

Aligned with raw_schema.yml structure:
- Column constraints (dtype, nullable, unique) as direct attributes
- Additional tests in a separate "tests" array
- Cleaner, more intuitive configuration
- Better mapping to ValidationEngine
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, RootModel, model_validator


class SensorType(str, Enum):
    polling = "polling"
    api_polling = "api_polling"
    schedule = "schedule"
    event = "event"
    asset_update = "asset_update"
    file_drop = "file_drop"
    cdc = "cdc"


class AssertType(str, Enum):
    api = "api"
    database = "database"
    file = "file"
    computed = "computed"
    dbt = "dbt"
    event = "event"


# ============================================================
# File Format Configuration (Snowflake-style)
# ============================================================
class FileFormatType(str, Enum):
    csv = "csv"
    json = "json"
    excel = "excel"


class FileFormatConfig(BaseModel):
    """Named, reusable file format definition.

    Declared in the ``file_formatters`` section and referenced by
    file-type assets via ``file_format``.

    Example YAML::

        file_formatters:
          - name: pipe_delimited_csv
            type: csv
            config:
              delimiter: "|"
              skip_header: 1
              encoding: "utf-8"
              null_if: ["", "NULL", "N/A"]
    """

    name: str
    type: FileFormatType
    description: Optional[str] = None

    # Format-specific options (passed to pandas reader)
    config: Dict[str, Any] = Field(default_factory=dict)

    def to_pandas_kwargs(self) -> Dict[str, Any]:
        """Convert the config into kwargs for the corresponding pandas reader.

        Maps Snowflake-style property names to pandas equivalents:
        - ``delimiter`` / ``field_delimiter`` → ``sep``
        - ``skip_header``                    → ``skiprows``
        - ``encoding``                       → ``encoding``
        - ``null_if``                        → ``na_values``
        - ``quote_char``                     → ``quotechar``
        - ``escape_char``                    → ``escapechar``
        - ``compression``                    → ``compression``
        - ``date_format``                    → ``date_format`` (JSON)
        - ``orient``                         → ``orient`` (JSON)
        - ``sheet_name``                     → ``sheet_name`` (Excel)

        Unknown keys are passed through as-is so advanced pandas
        options can still be used directly.
        """
        cfg = dict(self.config)
        kwargs: Dict[str, Any] = {}

        # ---- CSV mappings ----
        if self.type == FileFormatType.csv:
            if "delimiter" in cfg:
                kwargs["sep"] = cfg.pop("delimiter")
            if "field_delimiter" in cfg:
                kwargs["sep"] = cfg.pop("field_delimiter")
            if "skip_header" in cfg:
                kwargs["skiprows"] = cfg.pop("skip_header")
            if "encoding" in cfg:
                kwargs["encoding"] = cfg.pop("encoding")
            if "null_if" in cfg:
                kwargs["na_values"] = cfg.pop("null_if")
            if "quote_char" in cfg:
                kwargs["quotechar"] = cfg.pop("quote_char")
            if "escape_char" in cfg:
                kwargs["escapechar"] = cfg.pop("escape_char")
            if "compression" in cfg:
                kwargs["compression"] = cfg.pop("compression")
            if "header" in cfg:
                kwargs["header"] = cfg.pop("header")

        # ---- JSON mappings ----
        elif self.type == FileFormatType.json:
            if "orient" in cfg:
                kwargs["orient"] = cfg.pop("orient")
            if "encoding" in cfg:
                kwargs["encoding"] = cfg.pop("encoding")
            if "compression" in cfg:
                kwargs["compression"] = cfg.pop("compression")
            if "date_format" in cfg:
                kwargs["date_format"] = cfg.pop("date_format")
            if "lines" in cfg:
                kwargs["lines"] = cfg.pop("lines")

        # ---- Excel mappings ----
        elif self.type == FileFormatType.excel:
            if "sheet_name" in cfg:
                kwargs["sheet_name"] = cfg.pop("sheet_name")
            if "skip_header" in cfg:
                kwargs["skiprows"] = cfg.pop("skip_header")
            if "header" in cfg:
                kwargs["header"] = cfg.pop("header")

        # Pass-through any remaining keys as direct pandas kwargs
        kwargs.update(cfg)
        return kwargs


# ============================================================
# Stream Configuration (CDC dispatch targets)
# ============================================================
class StreamType(str, Enum):
    websocket = "websocket"
    kafka = "kafka"
    jms = "jms"


class StreamConfig(BaseModel):
    """Configuration for a single CDC event stream.

    Declared inline on database assets that have
    ``change_tracking: true``.

    Example YAML::

        streams:
          - type: websocket
            relay_endpoint: "ws://localhost:8000/emit"
          - type: kafka
            relay_endpoint: "kafka://broker:9092/topic"
            config:
              acks: "all"
    """

    type: StreamType
    relay_endpoint: str
    config: Dict[str, Any] = Field(default_factory=dict)


class Materialization(str, Enum):
    table = "table"
    incremental = "incremental"
    snapshot = "snapshot"


class IncrementalStrategy(str, Enum):
    append = "append"
    merge = "merge"
    delete_insert = "delete+insert"


class OnSchemaChange(str, Enum):
    ignore = "ignore"
    append_new_columns = "append_new_columns"
    fail = "fail"
    sync_all_columns = "sync_all_columns"


class SnapshotStrategy(str, Enum):
    timestamp = "timestamp"
    check = "check"


class HardDeletes(str, Enum):
    ignore = "ignore"
    invalidate = "invalidate"
    new_record = "new_record"


class DatabaseModelConfig(BaseModel):
    # -------------------------------------------------
    # Core materialization
    # -------------------------------------------------
    materialization: Materialization = Materialization.table

    unique_key: Optional[Union[str, List[str]]] = None

    incremental_strategy: Optional[IncrementalStrategy] = IncrementalStrategy.append

    on_schema_change: OnSchemaChange = OnSchemaChange.ignore

    # -------------------------------------------------
    # Snapshot-specific (dbt compatible)
    # -------------------------------------------------
    strategy: Optional[SnapshotStrategy] = None
    updated_at: Optional[str] = None
    check_cols: Optional[Union[List[str], str]] = None
    hard_deletes: HardDeletes = HardDeletes.ignore

    # Optional dbt-compatible metadata knobs
    snapshot_meta_column_names: Optional[Dict[str, str]] = None
    dbt_valid_to_current: Optional[str] = None

    # -------------------------------------------------
    # Misc
    # -------------------------------------------------
    indexes: Optional[List[Dict[str, Any]]] = None
    post_hook: Optional[str] = None

    # -------------------------------------------------
    # Validation (CRITICAL)
    # -------------------------------------------------
    @model_validator(mode="after")
    def validate_snapshot_config(self):
        if self.materialization != Materialization.snapshot:
            return self

        # Snapshot requires a unique key
        if not self.unique_key:
            raise ValueError("snapshot materialization requires 'unique_key'")

        # Strategy is mandatory for snapshot
        if not self.strategy:
            raise ValueError("snapshot materialization requires 'strategy'")

        if self.strategy == SnapshotStrategy.timestamp:
            if not self.updated_at:
                raise ValueError("snapshot strategy 'timestamp' requires 'updated_at'")

        if self.strategy == SnapshotStrategy.check:
            if not self.check_cols:
                raise ValueError("snapshot strategy 'check' requires 'check_cols'")

        if self.check_cols not in (None, "all") and not isinstance(
            self.check_cols, list
        ):
            raise ValueError("check_cols must be a list of columns or 'all'")

        # Disallow dangerous schema sync for snapshots
        if self.on_schema_change == OnSchemaChange.sync_all_columns:
            raise ValueError(
                "sync_all_columns is not allowed for snapshot materialization"
            )

        return self


class StructuredAssertTest(BaseModel):
    description: Optional[str] = None
    arguments: Optional[Dict[str, Any]] = None
    severity: Optional[str] = "ERROR"  # default
    blocking: Optional[bool] = True


# ============================================================
# Asset Tests (executed on entire dataframe)
# ============================================================


class AssetTests(RootModel[Union[str, Dict[str, StructuredAssertTest]]]):
    pass


# ============================================================
# Column Schema (with constraints + additional tests)
# ============================================================
class AssetSchema(BaseModel):
    """Column definition with constraints and tests"""

    name: str
    description: Optional[str] = None

    # Direct constraints (not in tests array)
    dtype: Optional[str] = None
    nullable: bool = True
    unique: bool = False
    isKey: bool = False

    expr: Optional[str] = None

    # Additional tests (beyond basic constraints)
    tests: List[AssetTests] = Field(default_factory=list)


# ============================================================
# Table-Level Transform Configuration
# ============================================================
class TransformConfig(BaseModel):
    """Table-level chainable transform expressions.

    Both fields are expression strings evaluated via ``eval()``
    against the fluent ``Frame`` API.

    Example YAML::

        transforms:
          pre: >
            frame
            .filter(col("status") != "DELETED")
            .dedup(["id", "dt"], keep="last")
          post: >
            frame
            .group_by(["ccy_cd"])
            .agg(agg_sum("amt").alias("total"))
            .order_by(desc("total"))
            .limit(100)
    """

    pre: Optional[str] = None
    post: Optional[str] = None


# ============================================================
# Asset Configuration
# ============================================================
class AssetConfig(BaseModel):
    """Complete asset configuration"""

    name: str
    type: AssertType
    description: Optional[str] = None
    partition_name: Optional[str] = None

    # Asset properties
    group_name: Optional[str] = "default"
    tags: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None

    # Source configuration
    source: Optional[Dict[str, Any]] = None

    # File format reference (file assets only)
    file_format: Optional[str] = None

    # Schema - column definitions
    columns: Optional[List[AssetSchema]] = None

    # Table-level tests
    tests: List[AssetTests] = Field(default_factory=list)

    # Dependencies
    depends_on: Optional[List[str]] = None

    model: Optional[DatabaseModelConfig] = None

    # Table-level transforms (fluent expression chains)
    transforms: Optional[TransformConfig] = None

    # CDC — change data capture
    change_tracking: bool = False
    streams: Optional[List[StreamConfig]] = None

    def get_database_model(self) -> DatabaseModelConfig:
        """
        Return a fully-initialized DatabaseModelConfig
        for database assets, otherwise raise.
        """
        if self.type != AssertType.database:
            raise ValueError(
                f"get_database_model() called on non-database asset '{self.name}'"
            )

        return self.model or DatabaseModelConfig()

    @model_validator(mode="after")
    def validate_model_for_type(self):
        if self.model and self.type != AssertType.database:
            raise ValueError(
                f"'model' is only valid for database assets (asset '{self.name}')"
            )
        return self

    @model_validator(mode="after")
    def validate_change_tracking(self):
        if self.change_tracking:
            if self.type != AssertType.database:
                raise ValueError(
                    f"change_tracking is only supported on database assets "
                    f"(asset '{self.name}' is type '{self.type}')"
                )
            if not self.streams:
                raise ValueError(
                    f"change_tracking requires at least one stream "
                    f"(asset '{self.name}')"
                )
        if self.streams and not self.change_tracking:
            raise ValueError(
                f"'streams' requires change_tracking: true "
                f"(asset '{self.name}')"
            )
        return self


# ============================================================
# Job Configuration
# ============================================================
class JobFlow(BaseModel):
    """Flow definition using DAG syntax"""

    definition: str  # e.g., "raw_api >> raw_db >> stage_table"
    description: Optional[str] = None


class JobConfig(BaseModel):
    """Job configuration"""

    name: str
    description: Optional[str] = None
    flow: JobFlow

    tags: Optional[Dict[str, str]] = None
    group: Optional[str] = "default"

    config: Optional[Dict[str, Any]] = None


# ============================================================
# Sensor Configuration
# ============================================================
class SensorTriggerConfig(BaseModel):
    """Sensor trigger configuration"""

    type: str  # job, asset, schedule, asset_materialization
    target: Optional[str] = None
    cron: Optional[str] = None
    minimum_interval_seconds: Optional[int] = 10
    default_status: str = "RUNNING"


class SensorConfig(BaseModel):
    """Sensor configuration"""

    name: str
    type: SensorType  # polling, schedule, event, asset_update, file_drop
    partition_name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

    trigger: SensorTriggerConfig
    config: Optional[Dict[str, Any]] = None


# ============================================================
# Framework Pipeline Configuration
# ============================================================
class FrameworkPipelineConfig(BaseModel):
    """Top-level framework configuration"""

    version: str = "1.0"
    name: str = "merged_pipeline"
    description: Optional[str] = None

    # Components
    file_formatters: List[FileFormatConfig] = Field(default_factory=list)
    assets: List[AssetConfig] = Field(default_factory=list)
    jobs: List[JobConfig] = Field(default_factory=list)
    sensors: List[SensorConfig] = Field(default_factory=list)

    # Global settings
    defaults: Optional[Dict[str, Any]] = None

    model_config = {"populate_by_name": True}
