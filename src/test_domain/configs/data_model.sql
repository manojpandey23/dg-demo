CREATE TABLE IF NOT EXISTS price.raw_table (
    id              BIGSERIAL PRIMARY KEY,
    file_name       TEXT        NOT NULL,
    record          JSONB       NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS price.stage_table (
    id              BIGSERIAL PRIMARY KEY,
    pid              INTEGER        NOT NULL,
    value           DOUBLE PRECISION,
    status_code     TEXT           NOT NULL,
    product_type    TEXT,
    source_file     TEXT           NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


select * from price.raw_table;

