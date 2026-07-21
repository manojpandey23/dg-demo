# Architecture Overview

This project is a **Python-first, YAML-driven data orchestration framework**
built on **Dagster**. It provides **dbt-like materialization semantics**
without using dbt, Jinja, or SQL macros.

The framework is designed as a **general-purpose library** for building
reliable, schema-aware data pipelines.

---

## Core Design Principles

- Explicit schema contracts
- Deterministic execution
- Programmatic asset generation
- Safe and observable schema evolution
- Separation of intent (YAML) from execution (Python)
- Framework-level extensibility and maintainability
- Performance-conscious materialization strategies
- Testable, pure logic separate from orchestration
- Clear error handling and metadata emission for Dagster UI visibility
- Support for both DDL (schema changes) and DML (data transformations) in PostgreSQL

---

## High-Level Flow

```
YAML Configuration (.macro + .resource files)
        ↓
Config Discovery & Merge (config_discovery.py)
        ↓
Pydantic Validation (config_models.py)
        ↓
Build Dagster Objects:
  ├── Assets      (asset_builder.py → assert_registry → per-type handlers)
  ├── Jobs        (job_builder.py → flow_parser.py)
  ├── Sensors     (sensor_builder.py → sensor_registry)
  ├── Checks      (validation_check_builder.py → validation_engine)
  └── Resources   (resources_builder.py → resource_registry)
        ↓
dagster.Definitions
```

---

## Module Map

### `framework/builder/` — Config → Dagster Objects

| Module | Responsibility |
|--------|---------------|
| `core_loader.py` | Main entry point (`FrameworkLoader`). Orchestrates discovery, merge, validation, and building. |
| `config_discovery.py` | Discovers `.resource` and `.macro` files, merges with duplicate detection. |
| `asset_builder.py` | Routes asset configs to type-specific handlers via registry. Handles OR-bridge dependencies. |
| `job_builder.py` | Parses flow definitions (`a >> b >> c`) into Dagster asset jobs. |
| `sensor_builder.py` | Routes sensor configs to type-specific handlers. |
| `resources_builder.py` | Builds Dagster resources from config (Postgres, API, Vault, etc.). |
| `flow_parser.py` | Parses DAG flow syntax including parallel (`[a, b] >> c`) and OR (`a | b >> c`) expressions. |
| `dependency_builder.py` | Resolves inter-asset dependencies from job flow definitions. |
| `ref_resolver.py` | Resolves `ref()` placeholders across config files via JMESPath. |

### `framework/model/` — Pydantic Models

| Module | Responsibility |
|--------|---------------|
| `config_models.py` | Full pipeline config schema: assets, jobs, sensors, file formatters, streams. Includes validators for snapshot/CDC/materialization rules. |
| `resource_models.py` | Resource type definitions: API, Postgres, S3, Vault, etc. |

### `framework/core/` — Runtime Handlers

| Module | Responsibility |
|--------|---------------|
| `asserts/assert_registry.py` | Decorator-based registry mapping `AssertType` → handler function. |
| `asserts/assert_api.py` | API asset handler — fetches data via `api_resource`. |
| `asserts/assert_db.py` | Database asset handler — transforms, schema management, materialization. |
| `asserts/assert_file.py` | File asset handler — reads CSV/JSON/Excel with optional file formatters. |
| `sensors/sensor_registry.py` | Sensor type → handler registry. |
| `sensors/sensor_*.py` | Per-type sensor implementations (polling, schedule, file_drop, CDC, etc.). |
| `resources/resource_registry.py` | Resource type → builder registry. |
| `resources/resource_*.py` | Per-type resource builders. |

### `framework/transformation/` — Transform DSL

| Module | Responsibility |
|--------|---------------|
| `transform_registry.py` | Registry of column-level transform functions (ref, value, when, etc.). |
| `builtin_transforms.py` | 30+ built-in transforms: string, numeric, date, window, hashing. |
| `transformation_executor.py` | Orchestrates pre → column → post → coercion pipeline. |
| `table_transforms.py` | Fluent `Frame` API for table-level transforms (filter, dedup, group_by, etc.). |
| `system_context.py` | Exposes Dagster run context to transform expressions. |

### `framework/validation/` — Data Quality

| Module | Responsibility |
|--------|---------------|
| `engine/validation_engine.py` | Runs configured validation rules against DataFrames. |
| `engine/validation_registry.py` | Registry of validation rule implementations. |
| `rules/validations.py` | Built-in rules: column type, null, unique, between, pattern, row conditions, table counts. |

### `framework/postgres/` — Schema & Materialization

| Module | Responsibility |
|--------|---------------|
| `schema/apply.py` | Atomic DDL+DML: schema diffing, table creation, schema evolution, materialization dispatch. |
| `schema/builder.py` | Converts Pydantic schema config → Postgres DDL types. |
| `sql/ddl.py` | SQL generation functions (CREATE, ALTER, COPY, snapshot queries). |
| `pghelper.py` | Low-level Postgres operations: COPY FROM, incremental merge, SCD2 snapshot. |

### `framework/cdc/` — Change Data Capture

| Module | Responsibility |
|--------|---------------|
| `cdc_builder.py` | Auto-generates CDC sensors and stream dispatcher resources for change-tracked assets. |
| `capture.py` | Captures row-level change events during materialization. |
| `diff_engine.py` | Computes row-level diffs between existing and incoming data. |
| `store.py` | Manages the per-asset change log table. |

---

## Materialization Strategies

### Table (Full Refresh)
1. Create staging table with declared schema
2. COPY data into staging via `COPY FROM STDIN`
3. Rename existing → backup, staging → target
4. Drop backup (zero-downtime swap)

### Incremental
- **Append**: Direct `COPY FROM STDIN` into existing table
- **Merge**: Per-row `INSERT ON CONFLICT UPDATE` (upsert)
- **Delete+Insert**: Delete by unique key, then insert

### Snapshot (SCD Type 2)
- New records: insert with `valid_from=now, is_current=true`
- Changed records: close old row (`valid_to=now`), insert new
- Hard deletes: configurable (`ignore`, `invalidate`, `new_record`)

---

## Extensibility

All major components use decorator-based registries:

```python
# Add a new asset type
@assert_handler(AssertType.my_type)
def handle_my_asset(config, asset_deps):
    ...

# Add a new transform
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
