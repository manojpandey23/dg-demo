# Demo

Self-contained demo of the config-driven Dagster framework. No local Python setup needed — everything runs in containers.

## Quick start

```bash
# From the repo root:
make demo

# Or manually:
docker compose up -d
```

## Services

| Service           | URL                      | Description |
|-------------------|--------------------------|-------------|
| Dagster UI        | http://localhost:3000     | Pipeline management and monitoring |
| Mock API          | http://localhost:8000     | Synthetic cash balance data |
| PostgreSQL        | localhost:7432            | Data warehouse (user: ods, db: ods) |

## What's included

A cash-balance ingestion pipeline with three stages:

```
Mock API (/cash_balance)
    -> cash_balance_api     (API asset — fetch JSON data)
    -> cash_balance_raw     (incremental append to PostgreSQL)
    -> cash_balance_stage   (full-refresh staging table)
```

This demonstrates:
- API data ingestion with automatic schema inference
- Column transforms (`ref()`, `upper()`, `pd.Timestamp.utcnow()`, `context.run.run_id`)
- Incremental materialization (append strategy)
- Table materialization (full refresh)
- API polling sensor with health checks
- Column lineage tracking in the Dagster UI

## Running the pipeline

1. Open http://localhost:3000
2. Navigate to **Jobs** > `demo_cash_balance_pipeline`
3. Click **Materialize all** to run the full pipeline
4. Check the **Assets** tab to see lineage and metadata

## Config files

The demo uses two config files:

- [`demo_domain/configs/demo.resource`](demo_domain/configs/demo.resource) — PostgreSQL and API connections
- [`demo_domain/configs/cash_balance.macro`](demo_domain/configs/cash_balance.macro) — assets, jobs, and sensors

## Stopping

```bash
make docker-down
# or: docker compose down -v
```
