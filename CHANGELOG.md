# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-21

Initial public release of the config-driven Dagster pipeline framework.

### Added

- **YAML-driven pipeline definition** using `.macro` and `.resource` config files
- **Asset types**: API, database, file (CSV/JSON/Excel), computed, dbt, event
- **Materialization strategies**: table (full refresh), incremental (append/merge/delete+insert), snapshot (SCD Type 2)
- **Transform DSL**: 30+ column-level functions (`ref`, `value`, `when`, `coalesce`, `hash_key`, window functions) and a fluent table-level API (`frame.filter().dedup().order_by()`)
- **Validation engine**: Great Expectations-style column and table checks, configurable in YAML
- **Schema management**: automatic DDL generation with schema evolution policies (ignore, append, fail, sync)
- **Change Data Capture (CDC)**: row-level diff detection with WebSocket/Kafka stream dispatch
- **Column lineage tracking**: automatic Dagster column-lineage metadata emission
- **File formatters**: Snowflake-style reusable format definitions for CSV, JSON, Excel
- **Multi-file config merge**: split configs across files with duplicate detection
- **Flow parser**: DAG syntax (`a >> b >> c`, `[a, b] >> c`, `a | b >> c`)
- **Eval sandboxing**: restricted `__builtins__` and proxy-limited `pd` scope for all expression evaluation
- **Docker/Podman support**: multi-stage Dockerfile with non-root user
- **Demo stack**: docker-compose with Postgres, mock API, and Dagster webserver
- **CI pipeline**: GitHub Actions with lint, typecheck, test, and Docker build
- **Pre-commit hooks**: ruff, black, mypy
- **Makefile**: standardized development commands

### Security

- All `eval()` calls execute with `__builtins__: {}` to prevent arbitrary code execution
- Pandas access restricted to a safe subset via `_PdProxy` (Timestamp, to_datetime, to_numeric, Series, isna, NaT)
- Plugin validation guards against private functions and non-callable attributes
