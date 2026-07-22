# ============================================================
# S3 Resource Configuration
#
# Authentication methods (in priority order):
#   1. Vault (secret_path) — for production
#   2. AWS profile (profile) — for local dev / SSO
#   3. Explicit credentials (access_key/secret_key)
#   4. Default credential chain (IAM role, env vars, etc.)
#
# All values support ${ENV_VAR:-default} syntax.
# ============================================================

resources:

  - name: s3_resource
    type: s3
    description: "Data lake S3 bucket"
    config:
      bucket: ${S3_BUCKET:-my-data-lake}
      region: ${AWS_REGION:-us-east-1}
      profile: ${AWS_PROFILE:-default}
