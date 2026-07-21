# Architecture

This project is a **Python-first, YAML-driven data orchestration framework** built on **Dagster**. It provides **dbt-like materialization semantics** without using dbt, Jinja, or SQL macros.

## Design principles

- **Explicit schema contracts** — every column has a declared type, nullability, and optional validation rules
- **Deterministic execution** — same config + same data = same result
- **Separation of intent from execution** — YAML declares what; Python handles how
- **Safe expression evaluation** — all `eval()` calls sandboxed with restricted builtins
- **Extensible via registries** — add asset types, transforms, validations, resources without modifying core code
- **Database-agnostic config layer** — pipeline configs don't encode database-specific logic; the materialization layer is pluggable (PostgreSQL ships built-in)

## Data flow

```
YAML Configuration (.macro + .resource files)
        |
        v
Config Discovery & Merge (config_discovery.py)
        |
        v
Pydantic V2 Validation (config_models.py)
        |
        v
Build Dagster Objects:
  +-- Assets      (asset_builder.py -> assert_registry -> per-type handlers)
  +-- Jobs        (job_builder.py -> flow_parser.py)
  +-- Sensors     (sensor_builder.py -> sensor_registry)
  +-- Checks      (validation_check_builder.py -> validation_engine)
  +-- Resources   (resources_builder.py -> resource_registry)
        |
        v
dagster.Definitions
```

## Module map

### `framework/builder/` — Config to Dagster objects

| Module | Responsibility |
|--------|---------------|
| `core_loader.py` | Main entry point (`FrameworkLoader`). Orchestrates discovery, merge, validation, and building. |
| `config_discovery.py` | Discovers `.resource` and `.macro` files, merges with duplicate detection. |
| `asset_builder.py` | Routes asset configs to type-specific handlers via registry. Handles OR-bridge dependencies. |
| `job_builder.py` | Parses flow definitions (`a >> b >> c`) into Dagster asset jobs. |
| `sensor_builder.py` | Routes sensor configs to type-specific handlers. |
| `resources_builder.py` | Builds Dagster resources from config (Postgres, API, Vault, etc.). |
| `flow_parser.py` | Parses DAG flow syntax including parallel (`[a, b] >> c`) and OR (`a \| b >> c`) expressions. |
| `dependency_builder.py` | Resolves inter-asset dependencies from job flow definitions. |
| `ref_resolver.py` | Resolves `ref()` placeholders across config files via JMESPath. |

### `framework/model/` — Pydantic models

| Module | Responsibility |
|--------|---------------|
| `config_models.py` | Full pipeline config schema: assets, jobs, sensors, file formatters, streams. Includes validators for snapshot/CDC/materialization rules. |
| `resource_models.py` | Resource type definitions: API, Postgres, S3, Vault, etc. |

### `framework/core/` — Runtime handlers

| Module | Responsibility |
|--------|---------------|
| `asserts/assert_registry.py` | Decorator-based registry mapping `AssertType` to handler function. |
| `asserts/assert_api.py` | API asset handler — fetches JSON via `api_resource`. |
| `asserts/assert_db.py` | Database asset handler — transforms, schema management, materialization. |
| `asserts/assert_file.py` | File asset handler — reads CSV/JSON/Excel with optional file formatters. |
| `sensors/sensor_registry.py` | Sensor type to handler registry. |
| `sensors/sensor_*.py` | Per-type sensor implementations (polling, schedule, file_drop, CDC, etc.). |
| `resources/resource_registry.py` | Resource type to builder registry. |
| `resources/resource_*.py` | Per-type resource builders. |

### `framework/transformation/` — Transform DSL

| Module | Responsibility |
|--------|---------------|
| `transform_registry.py` | Registry of column-level transform functions (ref, value, when, etc.). Sandboxed `eval()` with restricted `pd` proxy. |
| `builtin_transforms.py` | 30+ built-in transforms: string, numeric, date, window, hashing. |
| `transformation_executor.py` | Orchestrates pre -> column -> post -> coercion pipeline. |
| `table_transforms.py` | Fluent `Frame` API for table-level transforms (filter, dedup, group_by, etc.). |
| `system_context.py` | Exposes Dagster run context to transform expressions. |

### `framework/validation/` — Data quality

| Module | Responsibility |
|--------|---------------|
| `engine/validation_engine.py` | Runs configured validation rules against DataFrames. |
| `engine/validation_registry.py` | Registry of validation rule implementations. |
| `rules/validations.py` | Built-in rules: null, unique, between, in_set, pattern, row counts, etc. |

### `framework/backends/` — Database backends

Each database backend is a self-contained package under `framework/backends/`. The `DatabaseBackend` ABC defines the contract; backends register via the `@backend_handler` decorator. Per-asset backend selection happens through the `source.backend` key in YAML config (defaults to `"postgres"`).

| Module | Responsibility |
|--------|---------------|
| `base.py` | `DatabaseBackend` ABC — connection lifecycle, transactions, schema management, materialization, CDC. |
| `registry.py` | `BackendRegistry` + `@backend_handler` decorator for backend auto-registration. |
| `postgres/` | Postgres backend: DDL generation, COPY FROM bulk loading, schema drift, SCD2 snapshots. |
| `snowflake.py` | Snowflake backend: `write_pandas()` bulk load, MERGE upsert, INFORMATION_SCHEMA introspection. |

### `framework/cdc/` — Change Data Capture

| Module | Responsibility |
|--------|---------------|
| `cdc_builder.py` | Auto-generates CDC sensors and stream dispatcher resources for change-tracked assets. |
| `capture.py` | Captures row-level change events during materialization. |
| `diff_engine.py` | Computes row-level diffs between existing and incoming data. |
| `store.py` | Manages the per-asset change log table. |

## Materialization strategies

### Table (full refresh)

1. Create staging table with declared schema
2. COPY data into staging via `COPY FROM STDIN`
3. Rename existing table to backup, staging to target
4. Drop backup (zero-downtime swap)

### Incremental

- **Append**: direct `COPY FROM STDIN` into existing table
- **Merge**: per-row `INSERT ON CONFLICT UPDATE` (upsert)
- **Delete+Insert**: delete by unique key, then insert

### Snapshot (SCD Type 2)

- New records: insert with `valid_from=now`, `is_current=true`
- Changed records: close old row (`valid_to=now`), insert new version
- Hard deletes: configurable (`ignore`, `invalidate`, `new_record`)

## Extensibility

All major components use decorator-based registries. Adding a new type requires writing the handler function with the appropriate decorator — no changes to core framework code:

```python
# Add a new asset type
@assert_handler(AssertType.my_type)
def handle_my_asset(config, asset_deps):
    ...

# Add a new column transform
@transform("my_func")
def my_func(df, ctx, output_col, arg1, arg2):
    ...

# Add a new validation rule
@validation(name="my_rule", scope=RuleScope.COLUMN)
def my_rule(df, rule):
    ...

# Add a new resource type
@resource_handler(ResourceType.my_resource)
def build_my_resource(name, config):
    ...
```

## Security model

All expression evaluation (column `expr` fields, table transforms) runs through `eval()` with:

- `__builtins__: {}` — no access to `import`, `open`, `exec`, `__import__`, etc.
- `pd` access restricted to a safe proxy (`Timestamp`, `to_datetime`, `to_numeric`, `to_timedelta`, `Series`, `isna`, `NaT`)
- Plugin validation guards against private functions and non-callable attributes
