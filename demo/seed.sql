-- Schema setup for the demo domain
CREATE SCHEMA IF NOT EXISTS price;

-- Grant permissions
GRANT ALL ON SCHEMA price TO ods;
ALTER DEFAULT PRIVILEGES IN SCHEMA price GRANT ALL ON TABLES TO ods;
