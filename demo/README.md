# Demo Pipelines

Five self-contained pipelines demonstrating the framework's capabilities. Each one exercises a different set of features — run them independently or all at once.

## Quick start

```bash
# Docker (everything containerized)
make demo

# Local (requires Postgres at localhost:7432 and mock API at localhost:8000)
make dagster-dev
```

## Services

| Service      | URL                  | Description |
|--------------|----------------------|-------------|
| Dagster UI   | http://localhost:3000 | Pipeline management and monitoring |
| Mock API     | http://localhost:8000 | Synthetic data for all pipelines |
| PostgreSQL   | localhost:7432        | Data warehouse (user: ods, db: ods) |

## Pipelines

### 1. Cash Balance — API Ingestion
**Config:** `01_cash_balance.macro`
**Flow:** `API → raw (append) → stage (full refresh)`

The simplest pipeline. Fetches JSON from the `/cash_balance` endpoint, appends to a raw table, then builds a staging table with full refresh. Demonstrates basic column transforms (`ref()`, `upper()`, `pd.Timestamp.utcnow()`) and validation rules.

### 2. Orders — Transforms and Merge
**Config:** `02_orders.macro`
**Flow:** `API → enriched orders (upsert)`

Fetches orders, enriches them with derived columns (computed amounts, conditional bucketing, hash keys), and upserts by `order_id`. Demonstrates the transform DSL: `when()`, `coalesce()`, `hash_key()`, arithmetic expressions, and table-level pre/post transforms (`filter`, `dedup`, `order_by`).

### 3. Customers — SCD Type 2 Dimension
**Config:** `03_customers_scd2.macro`
**Flow:** `API → raw (append) → dimension (SCD2 snapshot)`

Maintains a slowly-changing dimension with full history. Run this pipeline multiple times — the mock API returns randomized tiers and regions, so you'll see the framework close old versions and insert new ones with `valid_from`, `valid_to`, `is_current`, and `is_deleted` tracking.

### 4. Trades — CDC with Change Tracking
**Config:** `04_trades_cdc.macro`
**Flow:** `API → enriched trades (delete+insert with CDC)`

Ingests trade data with Change Data Capture enabled. The framework detects row-level inserts, updates, and deletes during materialization and dispatches change events to a configured WebSocket stream. Also demonstrates `delete+insert` incremental strategy and complex transforms.

### 5. File Ingestion — CSV File Drops
**Config:** `05_file_ingestion.macro`
**Flow:** `CSV file → raw table (append)`

Reads CSV files from a watched directory using named file formatters. Two sample CSV files are included in `sample_files/`. To run manually, use the partition selector in the Dagster UI to pick a file path, or drop new CSV files into the watched directory.

## Running individual pipelines

In the Dagster UI:
1. Go to **Jobs** in the left sidebar
2. Pick the pipeline you want to run
3. Click **Materialize all** (or **Launch run**)
4. Check the **Assets** tab to see lineage, metadata, and data quality results

## Config files

All configs are in `configs/`:

| File | Purpose |
|------|---------|
| `demo.resource` | Shared connections (Postgres, mock API) |
| `01_cash_balance.macro` | Pipeline 1 — API ingestion |
| `02_orders.macro` | Pipeline 2 — transforms and merge |
| `03_customers_scd2.macro` | Pipeline 3 — SCD Type 2 |
| `04_trades_cdc.macro` | Pipeline 4 — CDC streaming |
| `05_file_ingestion.macro` | Pipeline 5 — file drops |

## Stopping

```bash
make docker-down
```
