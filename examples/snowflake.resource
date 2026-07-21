# ============================================================
# Snowflake Resource Configuration
#
# Used with assets that have: source.backend: snowflake
#
# All values support ${ENV_VAR:-default} syntax.
# ============================================================

resources:

  - name: snowflake_resource
    type: snowflake
    description: "Analytics Snowflake warehouse"
    config:
      account: ${SNOWFLAKE_ACCOUNT:-xy12345.us-east-1}
      warehouse: ${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}
      database: ${SNOWFLAKE_DATABASE:-ANALYTICS}
      schema: ${SNOWFLAKE_SCHEMA:-PUBLIC}
      role: ${SNOWFLAKE_ROLE:-ANALYST}
      user: ${SNOWFLAKE_USER:-demo_user}
      password: ${SNOWFLAKE_PASSWORD:-demo_password}
