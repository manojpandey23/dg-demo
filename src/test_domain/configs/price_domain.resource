# ============================================================
# Framework Resources
#
# Resource types:
#   - api:      REST API client (requests.Session)
#   - postgres: PostgreSQL connection (psycopg2)
#   - config:   YAML config file loader
#   - http:     Generic HTTP client
#   - logger:   Dagster logging
#   - vault:    HashiCorp Vault
# ============================================================

resources:

  - name: vault_main
    type: vault
    config:
      url: "https://vault.company.com:8200"
      auth_type:
        token:
          token:
            env: VAULT_TOKEN

  - name: api_resource
    type: api
    description: "REST API client for pricing data"
    config:
      base_url: "http://localhost:8000"
      timeout: 10
      headers:
        User-Agent: "DagsterFramework/1.0"

  - name: postgres_resource
    type: postgres
    description: "PostgreSQL connection for data persistence"
    credentials_from: vault_main
    config:
      host: "localhost"
      port: 7432
      database: "ods"
      user: "ods"
      password: "ods"
      connect_timeout: 10

  - name: config_resource
    type: config
    description: "YAML configuration file loader"
    config:
      config_dir: "src/price_domain/configs"

  - name: http_client_resource
    type: http
    description: "Generic HTTP client for any REST API"
    config:
      timeout: 30
      verify_ssl: true
      max_retries: 3

  - name: logger_resource
    type: logger
    description: "Dagster logging resource"
    config: {}

