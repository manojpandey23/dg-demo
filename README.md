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

### macOS / Linux

```bash
# Clone and install (requires Python 3.10+ and uv)
git clone https://github.com/manojpandey23/dg-demo.git && cd dg-demo
make dev
```

### Windows (PowerShell)

```powershell
git clone https://github.com/manojpandey23/dg-demo.git
cd dg-demo
uv sync
uv run pre-commit install
```

### Start the infrastructure

The demo stack runs Postgres, a mock REST API, and Dagster in containers.
Requires Docker (macOS/Linux) or Docker Desktop / Podman (Windows).

**macOS / Linux:**

```bash
make demo
```

**Windows (PowerShell):**

```powershell
python manage.py reset
docker compose up -d
```

Once running:

| Service     | URL / Address                  |
|-------------|-------------------------------|
| Dagster UI  | http://localhost:3000          |
| Mock API    | http://localhost:8000          |
| PostgreSQL  | `localhost:7432` (user: `ods`, db: `ods`, password: `demo_password`) |

**Dagster starts with no pipelines loaded by default.** You choose which examples to load (or create your own).

### Choose pipelines to load

Use the pipeline manager to select which examples to run. These commands work on all platforms:

```bash
# See what's available
python manage.py list

# Load specific pipelines (by number or name)
python manage.py add 01 02          # by number
python manage.py add cash trades    # by partial name match
python manage.py add all            # load everything

# Remove pipelines
python manage.py remove 01          # unload one
python manage.py remove all         # unload everything
python manage.py reset              # same as remove all
```

**macOS / Linux** — also available via Make:

```bash
make pipeline-list
make pipeline-add P="01 02"
make pipeline-remove P="01"
make pipeline-reset
```

After adding or removing pipelines, reload Dagster to pick up the changes:

**macOS / Linux:**

```bash
make pipeline-reload
```

**Windows (PowerShell):**

```powershell
docker compose restart dagster-webserver
```

When running locally with `dagster dev`, hot-reload is automatic — no restart needed.

Pipelines that need extra dependencies (S3, Snowflake) print setup instructions when added.

### Start Dagster (local dev, without Docker)

If you want to run Dagster locally instead of in Docker:

**macOS / Linux:**

```bash
make dagster-dev
```

**Windows (PowerShell):**

```powershell
uv run dagster dev -m demo.definitions
```

This loads every `.macro` and `.resource` file in `demo/configs/` — whatever you added with `manage.py`. You can add or remove pipelines while the dev server is running; it detects file changes and reloads automatically.

### Full reset (all platforms)

To wipe everything — containers, database, loaded pipelines — and start fresh:

```bash
docker compose down -v
python manage.py reset
```

Then run `make demo` (macOS/Linux) or `docker compose up -d` (Windows) to start clean.

---

## Example pipelines

Eight complete pipelines demonstrating different framework capabilities. Load any combination and run them independently.

### 01 — API Ingestion (Cash Balance)

```bash
python manage.py add 01
```

**Config:** `demo/catalog/01_cash_balance.macro`
**Flow:** `API → raw (append) → stage (full refresh)`
**Trigger:** API polling sensor (`cash_balance_sensor`) — polls every 5 minutes

The sensor polls `GET /cash_balance` on the mock API. When it returns data, it triggers `cash_balance_pipeline` — fetches the JSON, validates it, and appends rows to `demo.cash_balance_raw`, then refreshes `demo.cash_balance_stage` (full table replace). Demonstrates basic column transforms (`ref()`, `upper()`, `pd.Timestamp.utcnow()`) and validation rules.

**Run it:**
1. Open Dagster UI → **Overview → Sensors**
2. Verify `cash_balance_sensor` is **Running** (starts automatically)
3. Wait up to 5 minutes for the sensor to fire, or trigger immediately: **Overview → Jobs → cash_balance_pipeline → Materialize all**
4. Check results:
   ```sql
   SELECT * FROM demo.cash_balance_raw LIMIT 10;
   SELECT * FROM demo.cash_balance_stage LIMIT 10;
   ```

### 02 — Transforms and Merge (Orders)

```bash
python manage.py add 02
```

**Config:** `demo/catalog/02_orders.macro`
**Flow:** `API → enriched orders (upsert)`
**Trigger:** API polling sensor (`orders_sensor`) — polls every 2 minutes

Fetches orders, enriches them with derived columns (computed amounts, conditional bucketing, hash keys), and upserts by `order_id`. Demonstrates the full transform DSL: `when()`, `coalesce()`, `hash_key()`, arithmetic expressions, and table-level pre/post transforms (`filter`, `dedup`, `order_by`).

**Run it:**
1. `orders_sensor` should be **Running** in the sensors panel
2. Wait ~2 minutes or trigger manually: **Jobs → orders_pipeline → Materialize all**
3. Query:
   ```sql
   SELECT * FROM demo.orders_raw LIMIT 10;
   SELECT * FROM demo.orders_enriched LIMIT 10;
   ```

### 03 — SCD Type 2 Snapshot (Customers)

```bash
python manage.py add 03
```

**Config:** `demo/catalog/03_customers_scd2.macro`
**Flow:** `API → raw (append) → dimension (SCD2 snapshot)`
**Trigger:** Manual (designed for scheduled runs, but the demo runs on-demand)

Fetches current customer records from `GET /customers`, appends to `demo.customers_raw`, then builds an SCD2 dimension table (`demo.customers_dim`) that tracks history. When a customer's tier or email changes, the old row is closed (`valid_to` is set) and a new row opens.

**Run it:**
1. Go to **Jobs → customers_dimension_pipeline → Materialize all**
2. Run it once to seed the dimension
3. Wait a moment, then run it **again** — the mock API returns slightly different data each time (random tier changes), so the second run creates SCD2 history rows
4. Query:
   ```sql
   -- Current state
   SELECT * FROM demo.customers_dim WHERE valid_to IS NULL;

   -- Full history (shows closed + open rows)
   SELECT * FROM demo.customers_dim ORDER BY customer_id, valid_from;
   ```

### 04 — CDC with Change Tracking (Trades)

```bash
python manage.py add 04
```

**Config:** `demo/catalog/04_trades_cdc.macro`
**Flow:** `API → enriched trades (delete+insert with CDC)`
**Trigger:** API polling sensor (`trades_sensor`) — polls every 60 seconds

Polls `GET /trades` for trade executions. Each run detects row-level changes (inserts, updates, deletes) compared to the previous snapshot and writes change events to a CDC log table. Demonstrates `delete+insert` incremental strategy and complex transforms.

**Run it:**
1. Verify `trades_sensor` is **Running**
2. Wait ~60 seconds or trigger manually: **Jobs → trades_pipeline → Materialize all**
3. Run it twice to see change detection (the mock API randomizes trade data)
4. Query:
   ```sql
   SELECT * FROM demo.trades_enriched LIMIT 10;

   -- CDC change log
   SELECT * FROM demo.trades_enriched__changes ORDER BY id DESC LIMIT 20;
   ```

### 05 — Local File Drop Ingestion

```bash
python manage.py add 05
```

**Config:** `demo/catalog/05_file_ingestion.macro`
**Flow:** `CSV file → raw table (append)`
**Trigger:** File drop sensor (`transactions_file_sensor`) — scans every 30 seconds
**Watched directory:** `/data/incoming/transactions/` (container) or `$FILE_DROP_DIR` (local)

The sensor watches a directory for CSV files matching `transactions_*.csv`. When a new file appears, it triggers `file_ingestion_pipeline` — reads the CSV, validates columns, and appends to `demo.transactions_raw`. Two sample files are included in `demo/sample_files/` and get picked up automatically.

**Run it (Docker — files inside container):**

The demo stack pre-loads two sample files. The sensor picks them up automatically. To drop a new file:

```bash
cat > /tmp/transactions_2026-07-22.csv << 'CSV'
txn_id,account,amount,txn_date,category
TXN-100,ACC-500,250.00,2026-07-22,salary
TXN-101,ACC-500,42.50,2026-07-22,groceries
CSV

docker cp /tmp/transactions_2026-07-22.csv \
  $(docker compose ps -q dagster-webserver):/data/incoming/transactions/
```

**Run it (local dev):**

```bash
export FILE_DROP_DIR="$PWD/demo/sample_files"
make dagster-dev

# Drop a new file while the dev server is running:
cat > demo/sample_files/transactions_2026-07-22.csv << 'CSV'
txn_id,account,amount,txn_date,category
TXN-100,ACC-500,250.00,2026-07-22,salary
TXN-101,ACC-500,42.50,2026-07-22,groceries
CSV
```

Within 30 seconds, the sensor detects the new file, creates a dynamic partition, and launches a run.

```sql
SELECT source_file, COUNT(*) as rows FROM demo.transactions_raw GROUP BY source_file;
```

### 06 — Multi-Source Merge (Fan-In)

```bash
python manage.py add 06
```

**Config:** `demo/catalog/06_multi_source_merge.macro`
**Flow:** `API + file → revenue_stage (fan-in merge)`
**Trigger:** API polling sensor (every 3 min) + file drop sensor

Two independent sources (API revenue data + file revenue data) feed into a single staging table using the OR-bridge pattern (`revenue_api | revenue_file >> revenue_stage`). Either source can trigger the merge independently.

**Run it:**
1. The API sensor fires automatically every 3 minutes
2. To trigger the file side:
   ```bash
   export REVENUE_DROP_DIR="$PWD/demo/sample_revenue"
   mkdir -p demo/sample_revenue

   cat > demo/sample_revenue/revenue_2026-07-22.csv << 'CSV'
   account_id,amount,currency,channel,revenue_date
   FUND-A,15000.00,USD,online,2026-07-22
   FUND-B,8500.50,EUR,retail,2026-07-22
   CSV
   ```
3. Query:
   ```sql
   SELECT * FROM demo.revenue_stage LIMIT 20;
   ```

### 07 — Mixed Backend (Postgres + Snowflake)

```bash
python manage.py add 07
```

The manager auto-copies `snowflake.resource` and prints setup instructions.

**Config:** `demo/catalog/07_mixed_backend.macro`
**Flow:** `API → Postgres raw → Snowflake analytics`
**Trigger:** Manual

Demonstrates writing to different databases in the same pipeline: API data lands in Postgres, then transforms push to Snowflake for analytics.

**Prerequisites:**
```bash
pip install 'dagster-config-framework[snowflake]'

export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="ANALYTICS"
export SNOWFLAKE_SCHEMA="PUBLIC"
```

**Run it:**
1. Start the dev server: `make dagster-dev`
2. Go to **Jobs → mixed_backend_pipeline → Materialize all**
3. Query Postgres for raw data and Snowflake for the analytics table

### 08 — S3 File Drop Ingestion

```bash
python manage.py add 08
```

The manager auto-copies `s3.resource` and prints setup instructions.

**Config:** `demo/catalog/08_s3_file_ingestion.macro`
**Flow:** `S3 file → raw table (append)`
**Trigger:** S3 file sensor (`s3_transactions_sensor`) — scans every 60 seconds

The sensor watches an S3 prefix (`incoming/transactions/{today()}/`) for new CSV files. When a file appears, the pipeline reads it from S3, validates columns, and appends to `demo.s3_transactions_raw`. Uses ETag-based change detection — re-uploading a file with different content triggers reprocessing.

**Prerequisites:**

```bash
# AWS CLI credentials (named profile or SSO)
aws configure --profile my-data-profile

# Set environment variables
export AWS_PROFILE="my-data-profile"
export AWS_REGION="us-east-1"
export S3_BUCKET="my-data-lake"

# Create bucket if needed
aws s3 mb s3://my-data-lake --profile my-data-profile
```

**Run it:**

```bash
make dagster-dev

# Upload a test file to S3
cat > /tmp/transactions_batch1.csv << 'CSV'
txn_id,account,amount,txn_date
TXN-S3-001,ACC-100,1500.50,2026-07-22
TXN-S3-002,ACC-200,2300.00,2026-07-22
CSV

aws s3 cp /tmp/transactions_batch1.csv \
  s3://my-data-lake/incoming/transactions/$(date +%Y-%m-%d)/transactions_batch1.csv \
  --profile my-data-profile
```

Within 60 seconds, the sensor detects the new object and triggers a run. To test change detection, re-upload the file with different content — the sensor detects the ETag change and reprocesses.

```sql
SELECT * FROM demo.s3_transactions_raw ORDER BY ingested_at DESC LIMIT 20;
```

**Testing with LocalStack (no AWS account needed):**

```bash
docker run -d -p 4566:4566 localstack/localstack
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-data-lake
aws --endpoint-url=http://localhost:4566 s3 cp /tmp/transactions_batch1.csv \
  s3://my-data-lake/incoming/transactions/$(date +%Y-%m-%d)/transactions_batch1.csv

# Set endpoint_url: http://localhost:4566 in s3.resource config
```

---

## Trigger reference

| Trigger Type         | Config Key             | How It Fires                        | Example |
|----------------------|------------------------|-------------------------------------|---------|
| API polling sensor   | `type: api_polling`    | Polls an API endpoint on interval   | 01, 02, 04, 06 |
| File drop sensor     | `type: file_drop`      | Watches directory for new files     | 05, 06 |
| S3 file sensor       | `type: file_drop` + `source: s3` | Watches S3 prefix for new objects | 08 |
| Manual / job launch  | (no sensor)            | Click "Materialize all" in the UI   | 03, 07 |
| Schedule (cron)      | `cron: "0 6 * * *"`   | Fires on a cron schedule            | (use in your own) |

## Tracking strategies

| Strategy   | Config                       | What It Tracks        | Best For                          |
|------------|------------------------------|-----------------------|-----------------------------------|
| `mtime`    | `tracking.strategy: mtime`   | File mtime + size     | Local filesystems (default, fast) |
| `checksum` | `tracking.strategy: checksum`| MD5 content hash      | Network mounts, S3 re-uploads     |
| Custom     | `tracking.filter_fn: fn_name`| User-defined function | Skip `.tmp` files, date filtering |

---

## Creating your own pipeline

1. Create a `.macro` file in `demo/configs/` (or your project's config dir):

   ```yaml
   assets:
     - name: my_asset
       type: api
       source:
         resource: api_resource
         endpoint: /my-endpoint
       columns:
         - name: id
           dtype: string
   jobs:
     - name: my_pipeline
       flow:
         definition: my_asset >> my_target
   ```

2. If you need custom functions, create a Python module and register them:

   ```python
   # my_project/custom.py
   from framework import expr_function

   @expr_function
   def my_pattern() -> str:
       return "custom_value"
   ```

   Then in `definitions.py`:

   ```python
   loader = FrameworkLoader(
       config_dir=Path("configs"),
       user_modules=["my_project.custom"],
   )
   ```

3. The dev server hot-reloads — save the file and check the UI.

---

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

---

## Production deployment

For a full production-like stack (separate Dagster metadata Postgres, gRPC code server, webserver, daemon):

### macOS / Linux (Docker or Podman)

```bash
cp deploy/.env.example deploy/.env    # edit with your settings

make deploy-up                        # uses Docker by default
make deploy-up ENGINE=podman          # use Podman instead

make deploy-push                      # push pipeline config changes
make deploy-status                    # check status
make deploy-down                      # stop everything
```

### Windows (PowerShell + Podman)

```powershell
Copy-Item deploy\.env.example deploy\.env

.\deploy\deploy.ps1 up               # start
.\deploy\deploy.ps1 push             # push changes
.\deploy\deploy.ps1 status           # check status
.\deploy\deploy.ps1 down             # stop
```

### Using your own Postgres for Dagster metadata

If you already have a Postgres instance for Dagster's internal storage:

1. Set the connection in `deploy/.env`:
   ```
   DAGSTER_PG_HOST=your-postgres-host.example.com
   DAGSTER_PG_PORT=5432
   DAGSTER_PG_DB=dagster
   DAGSTER_PG_USER=dagster
   DAGSTER_PG_PASSWORD=your_secure_password
   ```
2. Remove the `dagster-postgres` service from `deploy/docker-compose.yml`
3. Remove the `dagster-postgres` dependency from the `dagster-webserver` and `dagster-daemon` service definitions

---

## Connecting to the database

All demo pipelines write to the Postgres instance at `localhost:7432`.

```bash
# psql
psql -h localhost -p 7432 -U ods -d ods

# Or any GUI tool (DBeaver, pgAdmin, DataGrip)
# Host: localhost, Port: 7432, User: ods, Password: demo_password, DB: ods
```

List all demo tables:

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'demo' ORDER BY table_name;
```

---

## Database support

The framework ships with a built-in PostgreSQL connector that handles all materialization strategies (table, incremental, snapshot), schema evolution, and CDC. PostgreSQL is the tested and recommended database for production use.

The architecture separates concerns so that adding support for other databases (MySQL, Snowflake, BigQuery, DuckDB) requires implementing the materialization interface without changing the config layer or transform DSL. Snowflake support is included as an optional backend (`pip install 'dagster-config-framework[snowflake]'`).

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
├── backends/               # Database backends
│   ├── base.py             # DatabaseBackend ABC
│   ├── registry.py         # Backend auto-registration
│   ├── postgres/           # PostgreSQL: DDL, COPY, schema drift, SCD2
│   └── snowflake.py        # Snowflake: write_pandas(), MERGE upsert
└── cdc/                    # Change data capture

demo/                       # Self-contained demo
├── catalog/                # All example pipeline configs (source of truth)
├── configs/                # Active configs (symlinked by manage.py)
├── sample_files/           # Sample CSVs for file ingestion
├── mock_api.py             # Mock REST API for demo data
├── definitions.py          # Dagster entry point
└── seed.sql                # Database seed script

examples/                   # Standalone example configs (same as catalog)
manage.py                   # Pipeline manager CLI
deploy/                     # Production deployment stack
tests/                      # Test suite (pytest)
```

## Development

**macOS / Linux (Make):**

```bash
make dev          # install deps + pre-commit hooks
make lint         # ruff + black check
make format       # auto-format code
make typecheck    # mypy
make test         # run unit tests
make test-cov     # tests with coverage report
make docker-build # build container image
```

**Windows (PowerShell):**

```powershell
uv sync                                                          # install deps
uv run pre-commit install                                        # pre-commit hooks
uv run ruff check framework/ tests/                              # lint
uv run black --check framework/ tests/                           # format check
uv run black framework/ tests/                                   # auto-format
uv run mypy framework/                                           # typecheck
uv run pytest tests/                                             # run tests
uv run pytest tests/ --cov=framework --cov-report=term-missing   # tests + coverage
docker build -t dagster-config-framework:latest .                # build image
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

## Security

All `eval()` calls in the transform DSL execute with `__builtins__: {}` and a restricted `pd` proxy that only exposes safe functions (`Timestamp`, `to_datetime`, `to_numeric`, `to_timedelta`, `Series`, `isna`, `NaT`). This prevents arbitrary code execution from YAML config expressions.

## License

[MIT](LICENSE)
