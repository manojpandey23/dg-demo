# Running Pipelines

Step-by-step instructions for running every example pipeline — from
starting the dev server to triggering the first pipeline run.

---

## Prerequisites

```bash
# Clone and install
git clone https://github.com/manojpandey23/dg-demo.git
cd dg-demo
make dev              # installs all dev + prod dependencies via uv

# For S3 examples
pip install 'dagster-config-framework[s3]'

# For Snowflake examples
pip install 'dagster-config-framework[snowflake]'
```

You need Docker or Podman for the database and mock API.


---

## 1. Start the infrastructure

The demo stack runs Postgres and a mock REST API in containers.

### macOS / Linux

```bash
make demo
```

### Windows (Podman)

```powershell
# Start Postgres + mock API
podman compose up -d

# Or use the deployment script (auto-detects engine)
.\deploy\deploy.ps1 up
```

Once running:

| Service     | URL / Address                  |
|-------------|-------------------------------|
| Dagster UI  | http://localhost:3000          |
| Mock API    | http://localhost:8000          |
| PostgreSQL  | `localhost:7432` (user: `ods`, db: `ods`, password: `demo_password`) |


---

## 2. Start the Dagster dev server

The dev server hot-reloads your pipeline definitions from `demo/configs/`.

```bash
make dagster-dev
```

This runs `dagster dev -m demo.definitions`, which loads every `.macro`
and `.resource` file in `demo/configs/`.

Open http://localhost:3000 to see the Dagster UI.


---

## 3. Choose an example to run

Each example below explains what it does, how it triggers, and what
you need to do to see data flow end to end.


### Example 1 — API Ingestion (Cash Balance)

**File:** `demo/configs/01_cash_balance.macro`
**Trigger:** API polling sensor (`cash_balance_sensor`) — polls every 5 minutes
**Resources:** `api_resource` (mock API), `postgres_resource`

**What happens:**
The sensor polls `GET /cash_balance` on the mock API. When it returns
data, the sensor triggers `cash_balance_pipeline`, which fetches the
JSON, validates it, and appends rows to `demo.cash_balance_raw`, then
refreshes `demo.cash_balance_stage` (full table replace).

**Steps:**
1. Open Dagster UI → **Overview → Sensors**
2. Verify `cash_balance_sensor` is **Running** (it starts automatically)
3. Wait up to 5 minutes — the sensor fires and launches a run
4. **To trigger immediately:** go to **Overview → Jobs → cash_balance_pipeline** → click **Materialize all**
5. Check results: connect to Postgres and query:
   ```sql
   SELECT * FROM demo.cash_balance_raw LIMIT 10;
   SELECT * FROM demo.cash_balance_stage LIMIT 10;
   ```


### Example 2 — Transforms and Merge (Orders)

**File:** `demo/configs/02_orders.macro`
**Trigger:** API polling sensor (`orders_sensor`) — polls every 2 minutes
**Resources:** `api_resource`, `postgres_resource`

**What happens:**
The sensor polls `GET /orders`. Each run fetches order data, applies
column transforms (derived columns, type casts, `upper()`, hash keys),
and writes to `demo.orders_raw` with incremental append, then merges
into `demo.orders_enriched` using delete+insert on `order_id`.

**Steps:**
1. Open Dagster UI → **Overview → Sensors**
2. `orders_sensor` should be **Running**
3. Wait ~2 minutes or trigger manually: **Jobs → orders_pipeline → Materialize all**
4. Query the output:
   ```sql
   SELECT * FROM demo.orders_raw LIMIT 10;
   SELECT * FROM demo.orders_enriched LIMIT 10;
   ```


### Example 3 — SCD Type 2 Snapshot (Customers)

**File:** `demo/configs/03_customers_scd2.macro`
**Trigger:** Manual (no sensor — designed for scheduled runs via cron, but the demo runs on-demand)
**Resources:** `api_resource`, `postgres_resource`

**What happens:**
Fetches current customer records from `GET /customers`, appends to
`demo.customers_raw`, then builds an SCD2 dimension table
(`demo.customers_dim`) that tracks history. When a customer's tier
or email changes, the old row is closed (`valid_to` is set) and a
new row opens.

**Steps:**
1. Go to **Jobs → customers_dimension_pipeline → Materialize all**
2. Run it once to seed the dimension
3. Wait a moment, then run it **again** — the mock API returns slightly
   different data each time (random tier changes), so the second run
   creates SCD2 history rows
4. Query:
   ```sql
   -- Current state
   SELECT * FROM demo.customers_dim WHERE valid_to IS NULL;

   -- Full history (shows closed + open rows)
   SELECT * FROM demo.customers_dim ORDER BY customer_id, valid_from;
   ```


### Example 4 — CDC with Change Tracking (Trades)

**File:** `demo/configs/04_trades_cdc.macro`
**Trigger:** API polling sensor (`trades_sensor`) — polls every 60 seconds
**Resources:** `api_resource`, `postgres_resource`

**What happens:**
Polls `GET /trades` for trade executions. Each run detects row-level
changes (inserts, updates, deletes) compared to the previous snapshot
and writes change events to a CDC log table. Downstream consumers
(dashboards, microservices) read the change log for real-time updates.

**Steps:**
1. Verify `trades_sensor` is **Running** in the sensors panel
2. Wait ~60 seconds or trigger manually: **Jobs → trades_pipeline → Materialize all**
3. Run it twice to see change detection in action (the mock API
   randomizes trade data, so the second run detects changes)
4. Query:
   ```sql
   SELECT * FROM demo.trades_enriched LIMIT 10;

   -- CDC change log (if CDC is enabled in the config)
   SELECT * FROM demo.trades_enriched__changes ORDER BY id DESC LIMIT 20;
   ```


### Example 5 — Local File Drop Ingestion

**File:** `demo/configs/05_file_ingestion.macro`
**Trigger:** File drop sensor (`transactions_file_sensor`) — scans every 30 seconds
**Resources:** `postgres_resource`
**Watched directory:** `/data/incoming/transactions/` (inside the container) or
`$FILE_DROP_DIR` (when running locally)

**What happens:**
The sensor watches a directory for CSV files matching `transactions_*.csv`.
When a new file appears (or an existing file is modified), it triggers
`file_ingestion_pipeline` — reads the CSV, validates columns, and appends
to `demo.transactions_raw`.

**Steps (Docker/Podman — files inside container):**

The demo stack pre-loads two sample files from `demo/sample_files/`. The
sensor picks them up automatically on first run.

To drop a **new** file:

```bash
# Create a CSV matching the expected pattern
cat > /tmp/transactions_2026-07-22.csv << 'CSV'
txn_id,account,amount,txn_date,category
TXN-100,ACC-500,250.00,2026-07-22,salary
TXN-101,ACC-500,42.50,2026-07-22,groceries
CSV

# Copy into the container's watched directory
docker cp /tmp/transactions_2026-07-22.csv \
  $(docker compose ps -q dagster-webserver):/data/incoming/transactions/

# Or if using the deploy stack:
docker compose -f deploy/docker-compose.yml cp \
  /tmp/transactions_2026-07-22.csv code-server:/data/incoming/transactions/
```

**Steps (local dev — no container):**

```bash
# Set the watch directory
export FILE_DROP_DIR="$PWD/demo/sample_files"

# Start dagster dev
make dagster-dev

# The sensor picks up the two existing sample files automatically.

# Drop a new file while the dev server is running:
cat > demo/sample_files/transactions_2026-07-22.csv << 'CSV'
txn_id,account,amount,txn_date,category
TXN-100,ACC-500,250.00,2026-07-22,salary
TXN-101,ACC-500,42.50,2026-07-22,groceries
CSV
```

Within 30 seconds, the sensor detects the new file, creates a dynamic
partition, and launches a run.

**Query results:**

```sql
SELECT source_file, COUNT(*) as rows
FROM demo.transactions_raw
GROUP BY source_file;
```


### Example 6 — Multi-Source Merge (Fan-In)

**File:** `examples/06_multi_source_merge.macro`
**Trigger:** Manual
**Resources:** `api_resource`, `postgres_resource`

**What happens:**
Two independent sources (API revenue data + file revenue data) feed
into a single staging table using the OR-bridge pattern
(`revenue_api | revenue_file >> revenue_stage`). Either source
can run independently.

**Steps:**
1. Copy the example into the demo configs:
   ```bash
   cp examples/06_multi_source_merge.macro demo/configs/
   ```
2. Restart the dev server (or it hot-reloads)
3. Go to **Jobs → revenue_merge_pipeline → Materialize all**

> **Note:** This example requires a `revenue_file` partition to exist.
> In a real setup, a file sensor would create the partition. For the
> demo, run the API-side assets first.


### Example 7 — Mixed Backend (Postgres + Snowflake)

**File:** `examples/07_mixed_backend.macro`
**Trigger:** Manual
**Resources:** `api_resource`, `postgres_resource`, `snowflake_resource`

**What happens:**
Demonstrates writing to different databases in the same pipeline:
API data lands in Postgres, then transforms push to Snowflake for
analytics.

**Prerequisites:**
```bash
pip install 'dagster-config-framework[snowflake]'
```

**Steps:**
1. Copy configs into demo:
   ```bash
   cp examples/07_mixed_backend.macro demo/configs/
   cp examples/snowflake.resource demo/configs/
   ```
2. Set Snowflake credentials:
   ```bash
   export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
   export SNOWFLAKE_USER="your_user"
   export SNOWFLAKE_PASSWORD="your_password"
   export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
   export SNOWFLAKE_DATABASE="ANALYTICS"
   export SNOWFLAKE_SCHEMA="PUBLIC"
   ```
3. Restart the dev server
4. Go to **Jobs → mixed_backend_pipeline → Materialize all**
5. Query Postgres for the raw data and Snowflake for the analytics table


### Example 8 — S3 File Drop Ingestion

**File:** `examples/08_s3_file_ingestion.macro`
**Trigger:** S3 file sensor (`s3_transactions_sensor`) — scans every 60 seconds
**Resources:** `s3_resource`, `postgres_resource`

**What happens:**
The sensor watches an S3 prefix (`incoming/transactions/{today()}/`)
for new CSV files. When a file appears, the pipeline reads it from S3,
validates columns, and appends to `demo.s3_transactions_raw`. The
sensor uses ETag-based change detection — re-uploading a file with
different content triggers reprocessing.

**Prerequisites:**

1. **AWS CLI and credentials:**

   ```bash
   # Install AWS CLI
   pip install awscli

   # Configure a named profile (or use default)
   aws configure --profile my-data-profile
   # Enter: Access Key, Secret Key, Region (us-east-1), Output (json)
   ```

   Or if using SSO:
   ```bash
   aws configure sso --profile my-data-profile
   aws sso login --profile my-data-profile
   ```

2. **Create the S3 bucket** (if it doesn't exist):
   ```bash
   aws s3 mb s3://my-data-lake --profile my-data-profile
   ```

3. **Set environment variables:**
   ```bash
   export AWS_PROFILE="my-data-profile"
   export AWS_REGION="us-east-1"
   export S3_BUCKET="my-data-lake"
   ```

**Steps:**

1. Copy configs into demo:
   ```bash
   cp examples/08_s3_file_ingestion.macro demo/configs/
   cp examples/s3.resource demo/configs/
   ```

2. Start the dev server:
   ```bash
   make dagster-dev
   ```

3. Upload a file matching the expected pattern to S3:
   ```bash
   # Create a test CSV
   cat > /tmp/transactions_batch1.csv << 'CSV'
   txn_id,account,amount,txn_date
   TXN-S3-001,ACC-100,1500.50,2026-07-22
   TXN-S3-002,ACC-200,2300.00,2026-07-22
   TXN-S3-003,ACC-300,450.75,2026-07-22
   CSV

   # Upload to the S3 prefix the sensor watches
   # The prefix uses {today()} — which resolves to today's date
   aws s3 cp /tmp/transactions_batch1.csv \
     s3://my-data-lake/incoming/transactions/$(date +%Y-%m-%d)/transactions_batch1.csv \
     --profile my-data-profile
   ```

4. Within 60 seconds, the sensor detects the new object and triggers a run.

5. To test **change detection**, re-upload with different content:
   ```bash
   # Modify the file
   cat > /tmp/transactions_batch1.csv << 'CSV'
   txn_id,account,amount,txn_date
   TXN-S3-001,ACC-100,1500.50,2026-07-22
   TXN-S3-002,ACC-200,9999.00,2026-07-22
   TXN-S3-003,ACC-300,450.75,2026-07-22
   TXN-S3-004,ACC-400,100.00,2026-07-22
   CSV

   aws s3 cp /tmp/transactions_batch1.csv \
     s3://my-data-lake/incoming/transactions/$(date +%Y-%m-%d)/transactions_batch1.csv \
     --profile my-data-profile
   ```

   The sensor detects the ETag change and reprocesses the file.

6. Query results:
   ```sql
   SELECT * FROM demo.s3_transactions_raw ORDER BY ingested_at DESC LIMIT 20;
   ```

**Testing with LocalStack (no AWS account needed):**

```bash
# Start LocalStack
docker run -d -p 4566:4566 localstack/localstack

# Create a bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-data-lake

# Set the endpoint in s3.resource config:
#   endpoint_url: http://localhost:4566

# Upload test files using the LocalStack endpoint
aws --endpoint-url=http://localhost:4566 s3 cp /tmp/transactions_batch1.csv \
  s3://my-data-lake/incoming/transactions/$(date +%Y-%m-%d)/transactions_batch1.csv
```


---

## 4. Production deployment

For a full production-like stack (separate Dagster metadata Postgres,
gRPC code server, webserver, daemon):

### macOS / Linux (Docker or Podman)

```bash
# Copy and edit the env file
cp deploy/.env.example deploy/.env

# Start the full stack
make deploy-up                        # uses Docker by default
make deploy-up ENGINE=podman          # use Podman instead

# Push pipeline config changes to the running instance
make deploy-push

# Check status
make deploy-status

# Stop everything
make deploy-down
```

### Windows (PowerShell + Podman)

```powershell
# Copy and edit the env file
Copy-Item deploy\.env.example deploy\.env

# Start the stack
.\deploy\deploy.ps1 up

# Push changes
.\deploy\deploy.ps1 push

# Check status
.\deploy\deploy.ps1 status

# Stop
.\deploy\deploy.ps1 down
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

3. Remove the `dagster-postgres` dependency from the `dagster-webserver`
   and `dagster-daemon` service definitions


---

## 5. Adding your own pipeline

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

2. If you need custom functions, create a Python module and load it:
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

## Trigger reference

Quick reference for how each pipeline type gets triggered.

| Trigger Type         | Config Key             | How It Fires                        | Example Pipeline |
|----------------------|------------------------|-------------------------------------|------------------|
| API polling sensor   | `type: api_polling`    | Polls an API endpoint on interval   | 01, 02, 04       |
| File drop sensor     | `type: file_drop`      | Watches directory for new files     | 05               |
| S3 file sensor       | `type: file_drop` + `source: s3` | Watches S3 prefix for new objects | 08       |
| Manual / job launch  | (no sensor)            | Click "Materialize all" in the UI   | 03, 06, 07       |
| Schedule (cron)      | `cron: "0 6 * * *"`   | Fires on a cron schedule            | (use in your own) |

## Tracking strategies

| Strategy   | Config                       | What It Tracks        | Best For                              |
|------------|------------------------------|-----------------------|---------------------------------------|
| `mtime`    | `tracking.strategy: mtime`   | File mtime + size     | Local filesystems (default, fast)     |
| `checksum` | `tracking.strategy: checksum`| MD5 content hash      | Network mounts, S3 re-uploads         |
| Custom     | `tracking.filter_fn: fn_name`| User-defined function | Skip `.tmp` files, date filtering     |


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
