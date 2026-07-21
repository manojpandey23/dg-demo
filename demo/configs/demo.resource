# ============================================================
# Demo Resources
#
# Shared connections used by all demo pipelines.
# In production you'd split these per domain or environment.
# ============================================================

resources:

  - name: api_resource
    type: api
    description: "Mock API serving synthetic data for all demo pipelines"
    config:
      base_url: ${API_BASE_URL:-http://localhost:8000}
      timeout: 10
      headers:
        User-Agent: "DagsterFramework-Demo/1.0"

  - name: postgres_resource
    type: postgres
    description: "Demo PostgreSQL database"
    config:
      host: ${POSTGRES_HOST:-localhost}
      port: ${POSTGRES_PORT:-7432}
      database: ${POSTGRES_DB:-ods}
      user: ${POSTGRES_USER:-ods}
      password: ${POSTGRES_PASSWORD:-demo_password}
      connect_timeout: 10
