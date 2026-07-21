# dagster-config-framework

A config-driven framework for building data ingestion pipelines on [Dagster](https://dagster.io). Define your assets, jobs, sensors, and validations in YAML — the framework compiles them into a fully functional Dagster deployment.

## Features

- **YAML-driven pipelines** — define assets, jobs, sensors, and validations in `.macro` and `.resource` files
- **Multiple asset types** — API, database, file, computed, dbt, and event sources
- **Schema management** — automatic DDL with `table`, `incremental`, and `snapshot` (SCD2) materializations
- **Validation engine** — Great Expectations-style column and table checks, configurable in YAML
- **Transform DSL** — column-level expressions (`ref()`, `value()`, `when()`, `hash_key()`) and table-level fluent API (`frame.filter().dedup().order_by()`)
- **CDC** — change data capture with WebSocket/Kafka stream dispatch
- **Column lineage** — automatic Dagster column-lineage metadata tracking
- **Multi-file config** — split configs across files with duplicate detection and deterministic merge

## Quick Start

### With Docker/Podman

```bash
# Clone and run the demo
git clone <repo-url> && cd dg-demo
make demo

# Open Dagster UI at http://localhost:3000
```

### Local development

```bash
# Install dependencies
make dev

# Run the test suite
make test

# Start Dagster dev server
make dagster-dev
```

## Project Structure

```
framework/                  # The framework library
├── builder/                # Config → Dagster object builders
│   ├── core_loader.py      # Main entry point (FrameworkLoader)
│   ├── asset_builder.py    # Asset factory
│   ├── job_builder.py      # Job factory
│   ├── sensor_builder.py   # Sensor factory
│   └── config_discovery.py # .macro/.resource file discovery
├── model/                  # Pydantic config models
├── core/                   # Asset handlers, sensors, resources
├── transformation/         # Column + table transform DSL
├── validation/             # Validation engine + rules
├── postgres/               # Schema management + materialization
└── cdc/                    # Change data capture

demo/                       # Self-contained demo (docker compose)
src/test_domain/            # Example domain using the framework
tests/                      # Test suite
```

## Configuration

### Resource files (`.resource`)

Define infrastructure connections:

```yaml
resources:
  - name: postgres_resource
    type: postgres
    config:
      host: ${POSTGRES_HOST:-localhost}
      port: ${POSTGRES_PORT:-5432}
      database: mydb
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}

  - name: api_resource
    type: api
    config:
      base_url: "http://api.example.com"
      timeout: 10
```

### Pipeline files (`.macro`)

Define assets, jobs, and sensors:

```yaml
assets:
  - name: raw_prices
    type: api
    source:
      resource: api_resource
      endpoint: /prices
    columns:
      - name: ticker
        dtype: string
        nullable: false
      - name: price
        dtype: float
        tests:
          - expect_column_values_to_be_between:
              arguments: { min: 0 }

  - name: prices_db
    type: database
    depends_on: [raw_prices]
    source:
      resource: postgres_resource
      table: market.prices
    model:
      materialization: incremental
      incremental_strategy: append
    columns:
      - name: ticker
        dtype: string
        expr: ref("ticker")
      - name: price
        dtype: float
        expr: ref("price")
      - name: ingested_at
        dtype: datetime
        expr: pd.Timestamp.utcnow()

jobs:
  - name: price_ingestion
    flow:
      definition: raw_prices >> prices_db

sensors:
  - name: price_sensor
    type: api_polling
    trigger:
      type: job
      target: price_ingestion
      minimum_interval_seconds: 300
```

### Usage in code

```python
from pathlib import Path
from framework import FrameworkLoader

loader = FrameworkLoader(config_dir=Path("configs"))
defs = loader.get_definitions()  # Returns dagster.Definitions
```

## Materializations

| Type          | Behavior                                    |
|---------------|---------------------------------------------|
| `table`       | Full refresh — drop and recreate            |
| `incremental` | Append, merge, or delete+insert             |
| `snapshot`    | SCD Type 2 with valid_from/valid_to history |

## Transform DSL

Column expressions in `expr` fields:

| Function | Example | Description |
|----------|---------|-------------|
| `ref()` | `ref("col")` | Reference upstream column |
| `value()` | `value("USD")` | Constant literal |
| `when()` | `when(ref("amt") > 0, value("CR"), value("DR"))` | Conditional |
| `coalesce()` | `coalesce(ref("a"), ref("b"), value(0))` | First non-null |
| `hash_key()` | `hash_key(ref("id"), ref("dt"))` | MD5 surrogate key |
| `upper()`, `lower()`, `trim()` | `upper(ref("name"))` | String transforms |
| `round_val()`, `abs_val()` | `round_val(ref("price"), 2)` | Numeric transforms |
| `to_date()`, `to_datetime()` | `to_date(ref("dt"), "%Y-%m-%d")` | Type casting |
| `row_number()`, `rank()`, `lag()`, `lead()` | `row_number(ref("grp"), ref("dt"))` | Window functions |

Table-level transforms in `transforms.pre` / `transforms.post`:

```yaml
transforms:
  pre: >
    frame
    .filter(col("status") != "DELETED")
    .dedup(["id", "dt"], keep="last")
  post: >
    frame
    .order_by(desc("amount"))
    .limit(1000)
```

## Validation Rules

Configure Great Expectations-style checks:

- `expect_column_values_to_not_be_null`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_be_between`
- `expect_column_values_to_be_in_set`
- `expect_column_value_length_equal_to`
- `expect_column_value_pattern_match`
- `expect_table_row_count_to_be_between`
- `expect_table_rows_to_be_unique`

## Development

```bash
make lint          # Ruff + Black check
make format        # Auto-format
make typecheck     # mypy
make test          # pytest
make test-cov      # pytest with coverage
make docker-build  # Build container image
```

## License

MIT
