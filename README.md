# dagster-config-framework

A config-driven framework for building data pipelines on [Dagster](https://dagster.io). Define assets, jobs, sensors, and validations in YAML — the framework compiles them into a fully functional Dagster deployment with schema management, data quality checks, and column lineage tracking.

## Why this framework?

Building data pipelines involves a lot of repetitive boilerplate: wiring up sources, defining schemas, handling incremental loads, tracking changes, validating data quality. This framework replaces that boilerplate with declarative YAML configs while keeping the full power of Dagster underneath.

- **No code needed for common patterns** — API ingestion, file drops, database persistence, SCD Type 2 snapshots
- **Schema-as-config** — column types, nullability, and validation rules defined alongside your pipeline
- **Built-in materialization strategies** — full refresh, incremental (append/merge/delete+insert), SCD2 snapshots
- **Transform DSL** — column-level expressions and a fluent table-level API, all in YAML
- **Extensible** — add custom asset types, transforms, validations, and resources via decorator-based registries

## Quick start

### With Docker/Podman (recommended for first look)

```bash
git clone https://github.com/manojpandey23/dg-demo.git && cd dg-demo
make demo
# Open Dagster UI at http://localhost:3000
```

This starts a self-contained stack: PostgreSQL, a mock API, and a Dagster webserver with a sample pipeline.

### Local development

```bash
# Requires Python 3.10+ and uv (https://docs.astral.sh/uv/)
make dev          # install all dependencies + pre-commit hooks
make test         # run the test suite
make dagster-dev  # start Dagster dev server at http://localhost:3000
```

## How it works

```
YAML configs (.macro + .resource files)
        |
        v
  Config Discovery & Merge
        |
        v
  Pydantic V2 Validation
        |
        v
  Build Dagster Objects:
    ├── Assets      (API, database, file, computed, dbt, event)
    ├── Jobs        (DAG flow syntax: a >> b >> c)
    ├── Sensors     (polling, schedule, file_drop, CDC)
    ├── Checks      (column + table validation rules)
    └── Resources   (database, API, Vault, S3, etc.)
        |
        v
  dagster.Definitions  ->  Dagster UI + scheduler
```

You write two types of config files:

- **`.resource`** files define connections (databases, APIs, secrets)
- **`.macro`** files define pipelines (assets, jobs, sensors, validations)

The framework discovers all config files in your config directory, validates them with Pydantic, and compiles them into Dagster objects. Your `definitions.py` is three lines:

```python
from pathlib import Path
from framework import FrameworkLoader

loader = FrameworkLoader(config_dir=Path(__file__).parent / "configs")
defs = loader.get_definitions()
```

## Configuration reference

### Resources (`.resource` files)

Resources define connections to external systems:

```yaml
resources:
  - name: postgres_resource
    type: postgres
    config:
      host: ${POSTGRES_HOST:-localhost}
      port: ${POSTGRES_PORT:-5432}
      database: ${POSTGRES_DB:-warehouse}
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}

  - name: api_resource
    type: api
    config:
      base_url: ${API_BASE_URL}
      timeout: 30
      headers:
        Accept: "application/json"
```

| Resource type | Purpose |
|---------------|---------|
| `postgres`    | PostgreSQL connection (psycopg2) — built-in materialization support |
| `api`         | REST API client (httpx) |
| `vault`       | HashiCorp Vault for runtime secret resolution |
| `s3`          | AWS S3 bucket access |
| `ftp`         | FTP/SFTP file transfer |
| `config`      | YAML config file loader |
| `http`        | Generic HTTP client |
| `logger`      | Dagster logging resource |

Environment variables are resolved at load time using `${VAR:-default}` syntax.

### Assets (in `.macro` files)

Assets represent data objects in your pipeline:

```yaml
assets:
  - name: orders_raw
    type: database
    depends_on: [orders_api]
    source:
      resource: postgres_resource
      table: warehouse.orders_raw
    model:
      materialization: incremental
      incremental_strategy: merge
      unique_key: order_id
      on_schema_change: append_new_columns
    columns:
      - name: order_id
        dtype: string
        isKey: true
        expr: ref("order_id")
      - name: amount
        dtype: float
        expr: ref("amount")
      - name: ingested_at
        dtype: datetime
        expr: pd.Timestamp.utcnow()
```

| Asset type  | Source           | Description |
|-------------|------------------|-------------|
| `api`       | REST endpoint    | Fetches JSON from an API, returns a DataFrame |
| `database`  | Database table   | Transforms upstream data and materializes to a table |
| `file`      | CSV/JSON/Excel   | Reads files discovered by a file_drop sensor |
| `computed`  | Pure Python      | Derives data from upstream assets without external I/O |
| `dbt`       | dbt model        | Delegates to a dbt model |
| `event`     | Event stream     | Processes events from a streaming source |

### Materializations

| Strategy      | Behavior |
|---------------|----------|
| `table`       | Full refresh — drop and recreate the target table |
| `incremental` | Append, merge (upsert), or delete+insert into existing table |
| `snapshot`    | SCD Type 2 — tracks change history with `valid_from`, `valid_to`, `is_current` |

Incremental strategies:

| Strategy         | Description |
|------------------|-------------|
| `append`         | Insert all rows (no deduplication) |
| `merge`          | Upsert — insert new rows, update existing by unique key |
| `delete+insert`  | Delete matching keys, then insert (atomic replace per key) |

Schema evolution policies:

| Policy               | Behavior |
|----------------------|----------|
| `ignore`             | Keep existing schema, drop extra columns |
| `append_new_columns` | Add new columns to the target table |
| `fail`               | Raise an error on schema mismatch |
| `sync_all_columns`   | Sync all columns (not available for snapshots) |

### Transform DSL

Column-level expressions in `expr` fields:

| Function | Example | Description |
|----------|---------|-------------|
| `ref()` | `ref("col")` | Reference an upstream column |
| `value()` | `value("USD")` | Constant literal |
| `when()` | `when(ref("amt") > 0, value("CR"), value("DR"))` | Conditional (nestable) |
| `coalesce()` | `coalesce(ref("a"), ref("b"), value(0))` | First non-null value |
| `hash_key()` | `hash_key(ref("id"), ref("dt"))` | MD5 surrogate key |
| `upper()`, `lower()`, `trim()` | `upper(ref("name"))` | String transforms |
| `round_val()`, `abs_val()` | `round_val(ref("price"), 2)` | Numeric transforms |
| `to_date()`, `to_datetime()` | `to_date(ref("dt"), "%Y-%m-%d")` | Type casting |
| `row_number()`, `rank()`, `lag()`, `lead()` | `row_number(ref("grp"), ref("dt"))` | Window functions |
| `pd.Timestamp.utcnow()` | `pd.Timestamp.utcnow()` | Current UTC timestamp |
| `context.run.run_id` | `context.run.run_id` | Dagster run ID |

Table-level transforms using the fluent Frame API:

```yaml
transforms:
  pre: >
    frame
    .filter(col("status") != "DELETED")
    .dedup(["id", "date"], keep="last")
  post: >
    frame
    .order_by(desc("amount"))
    .limit(1000)
```

Available Frame operations: `filter`, `dedup`, `order_by`, `limit`, `select`, `rename`, `drop`, `group_by`, `agg`, `join`.

### Validation rules

Column-level and table-level checks, configured in YAML:

```yaml
columns:
  - name: amount
    dtype: float
    nullable: false
    tests:
      - expect_column_values_to_be_between:
          arguments: { min: 0, max: 999999 }
          severity: ERROR
      - expect_column_values_to_be_in_set:
          arguments:
            allowed: ["USD", "EUR", "GBP"]

tests:
  - expect_table_row_count_to_be_between:
      arguments: { min: 1 }
  - expect_table_rows_to_be_unique:
      severity: ERROR
```

| Rule | Scope | Description |
|------|-------|-------------|
| `expect_column_values_to_not_be_null` | column | All values non-null |
| `expect_column_values_to_be_unique` | column | All values unique |
| `expect_column_values_to_be_between` | column | Values within min/max range |
| `expect_column_values_to_be_in_set` | column | Values in allowed set |
| `expect_column_value_length_equal_to` | column | String length equals N |
| `expect_column_value_pattern_match` | column | Values match regex pattern |
| `expect_table_row_count_to_be_between` | table | Row count within range |
| `expect_table_rows_to_be_unique` | table | No duplicate rows |

### Jobs and sensors

```yaml
jobs:
  - name: ingestion_pipeline
    flow:
      definition: source_api >> raw_table >> stage_table

sensors:
  - name: api_sensor
    type: api_polling
    trigger:
      type: job
      target: ingestion_pipeline
      minimum_interval_seconds: 300
```

Flow syntax supports sequential (`>>`), parallel (`[a, b] >> c`), and OR-bridge (`a | b >> c`) patterns.

| Sensor type    | Trigger |
|----------------|---------|
| `api_polling`  | Polls an API endpoint for health/data availability |
| `schedule`     | Cron-based schedule |
| `file_drop`    | Watches a directory for new files |
| `cdc`          | Triggers on change data capture events |
| `asset_update` | Triggers when an upstream asset materializes |
| `event`        | Listens for external events |

### File formatters

Reusable, named format definitions for file assets:

```yaml
file_formatters:
  - name: csv_standard
    type: csv
    config:
      delimiter: ","
      encoding: "utf-8"
      null_if: ["", "NULL", "N/A"]

  - name: csv_pipe_delimited
    type: csv
    config:
      delimiter: "|"
      encoding: "utf-8"

  - name: json_lines
    type: json
    config:
      lines: true
```

## Examples

The [`examples/`](examples/) directory contains complete pipeline configs for common patterns:

| Example | Pattern | Description |
|---------|---------|-------------|
| [01_api_ingestion](examples/01_api_ingestion.macro) | API -> DB | REST API polling with validation |
| [02_file_ingestion](examples/02_file_ingestion.macro) | File -> DB | CSV file drop with file_drop sensor |
| [03_scd2_snapshot](examples/03_scd2_snapshot.macro) | API -> raw -> SCD2 | Slowly-changing dimension tracking |
| [04_transforms_and_validations](examples/04_transforms_and_validations.macro) | Transform DSL | Column expressions, table transforms, quality checks |
| [05_cdc_streaming](examples/05_cdc_streaming.macro) | CDC | Change tracking with WebSocket/Kafka dispatch |
| [06_multi_source_merge](examples/06_multi_source_merge.macro) | Fan-in merge | Multiple sources into one staging table |
| [resources](examples/resources.resource) | Resources | Connection definitions for all resource types |

## Database support

The framework ships with a built-in PostgreSQL connector that handles all materialization strategies (table, incremental, snapshot), schema evolution, and CDC. PostgreSQL is the tested and recommended database for production use.

The architecture separates concerns so that adding support for other databases (MySQL, Snowflake, BigQuery, DuckDB) requires implementing the materialization interface without changing the config layer or transform DSL. This is planned for future releases.

Dagster itself uses its own storage database for run history, event logs, and schedules — this is separate from your pipeline's data targets.

## Project structure

```
framework/                  # The framework library (this is what gets packaged)
├── builder/                # Config -> Dagster object builders
│   ├── core_loader.py      # Main entry point (FrameworkLoader)
│   ├── asset_builder.py    # Asset factory
│   ├── job_builder.py      # Job factory
│   ├── sensor_builder.py   # Sensor factory
│   └── config_discovery.py # .macro/.resource file discovery
├── model/                  # Pydantic V2 config models
│   ├── config_models.py    # Pipeline config schema
│   └── resource_models.py  # Resource type definitions
├── core/                   # Runtime handlers
│   ├── asserts/            # Asset type handlers (api, db, file, etc.)
│   ├── sensors/            # Sensor type handlers
│   └── resources/          # Resource type builders
├── transformation/         # Transform DSL
│   ├── transform_registry.py    # Column-level transform functions
│   ├── builtin_transforms.py    # 30+ built-in transforms
│   ├── table_transforms.py      # Fluent Frame API
│   └── transformation_executor.py # Transform orchestration
├── validation/             # Data quality engine
│   ├── engine/             # Validation runner + registry
│   └── rules/              # Built-in validation rules
├── postgres/               # PostgreSQL materialization
│   ├── schema/             # DDL generation + schema evolution
│   └── sql/                # SQL generation (CREATE, ALTER, COPY, snapshot)
└── cdc/                    # Change data capture

examples/                   # Example pipeline configs (see table above)
demo/                       # Self-contained demo (docker compose)
src/test_domain/            # Development/test domain configs
tests/                      # Test suite (pytest)
docs/                       # Architecture documentation
```

## Development

```bash
make dev          # install deps + pre-commit hooks
make lint         # ruff + black check
make format       # auto-format code
make typecheck    # mypy
make test         # run unit tests
make test-cov     # tests with coverage report
make docker-build # build container image
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

## Security

All `eval()` calls in the transform DSL execute with `__builtins__: {}` and a restricted `pd` proxy that only exposes safe functions (`Timestamp`, `to_datetime`, `to_numeric`, `to_timedelta`, `Series`, `isna`, `NaT`). This prevents arbitrary code execution from YAML config expressions.

## License

[MIT](LICENSE)
