# Demo

Self-contained demo of the config-driven Dagster framework.

## Quick Start

```bash
# From the repo root:
make demo

# Or manually:
docker compose up -d
```

This starts:

| Service           | URL                      |
|-------------------|--------------------------|
| Dagster UI        | http://localhost:3000     |
| Mock API          | http://localhost:8000     |
| PostgreSQL        | localhost:7432            |

## What's in the demo

A cash-balance ingestion pipeline:

```
Mock API (/cash_balance)
    → cash_balance_api     (API asset — fetch JSON)
    → cash_balance_raw     (incremental append to Postgres)
    → cash_balance_stage   (full-refresh staging table)
```

## Running the pipeline

1. Open http://localhost:3000
2. Navigate to **Jobs** → `demo_cash_balance_pipeline`
3. Click **Materialize all** to run the full pipeline
4. Check the **Assets** tab to see lineage and metadata

## Stopping

```bash
make docker-down
# or: docker compose down -v
```
