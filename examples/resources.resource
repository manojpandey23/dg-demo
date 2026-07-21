# ============================================================
# Example Resource Definitions
#
# Resources define connections to external systems.
# Use environment variables for credentials — never hardcode
# secrets in config files.
#
# Supported resource types:
#   api       — REST API client (uses httpx)
#   postgres  — PostgreSQL connection (uses psycopg2)
#   vault     — HashiCorp Vault for secret management
#   s3        — AWS S3 bucket access
#   ftp       — FTP/SFTP file transfer
#   config    — YAML config file loader
#   http      — Generic HTTP client
#   logger    — Dagster logging resource
# ============================================================

resources:

  # REST API — the base_url is the root for all endpoint paths
  - name: api_resource
    type: api
    description: "Primary REST API connection"
    config:
      base_url: ${API_BASE_URL:-http://localhost:8000}
      timeout: 30
      headers:
        User-Agent: "DagsterFramework/1.0"
        Accept: "application/json"

  # PostgreSQL — the built-in database connector
  # Supports all materialization strategies (table, incremental, snapshot)
  - name: postgres_resource
    type: postgres
    description: "PostgreSQL data warehouse"
    config:
      host: ${POSTGRES_HOST:-localhost}
      port: ${POSTGRES_PORT:-5432}
      database: ${POSTGRES_DB:-warehouse}
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}
      connect_timeout: 10

  # Vault — fetch database credentials at runtime instead of
  # storing them in environment variables
  - name: vault_resource
    type: vault
    config:
      url: ${VAULT_ADDR:-https://vault.example.com:8200}
      auth_type:
        token:
          token:
            env: VAULT_TOKEN

  # PostgreSQL with Vault credentials — reference the vault
  # resource to fetch user/password at runtime
  - name: postgres_with_vault
    type: postgres
    description: "PostgreSQL with Vault-managed credentials"
    credentials_from: vault_resource
    config:
      host: ${POSTGRES_HOST:-localhost}
      port: ${POSTGRES_PORT:-5432}
      database: ${POSTGRES_DB:-warehouse}
      secret_path: "secret/data/postgres/warehouse"

  # S3 — for file-based assets stored in cloud storage
  - name: s3_resource
    type: s3
    config:
      bucket: ${S3_BUCKET:-my-data-lake}
      region: ${AWS_REGION:-us-east-1}
      secret_path: "secret/data/aws/s3"
