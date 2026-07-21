-- Schema setup for demo pipelines
-- Each pipeline writes to the 'demo' schema

CREATE SCHEMA IF NOT EXISTS demo;

GRANT ALL ON SCHEMA demo TO ods;
ALTER DEFAULT PRIVILEGES IN SCHEMA demo GRANT ALL ON TABLES TO ods;

-- Legacy schema used by src/test_domain configs (kept for compatibility)
CREATE SCHEMA IF NOT EXISTS price;
GRANT ALL ON SCHEMA price TO ods;
ALTER DEFAULT PRIVILEGES IN SCHEMA price GRANT ALL ON TABLES TO ods;
