-- pipeline_db schema
-- Run: docker exec -i postgres psql -U pgadmin -d pipeline_db < scripts/init_db.sql

CREATE TABLE IF NOT EXISTS source_system (
    source_id   SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50),
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS file_registry (
    file_id          BIGSERIAL PRIMARY KEY,
    source_id        INT REFERENCES source_system(source_id),
    file_name        VARCHAR(255) NOT NULL,
    bucket_name      VARCHAR(100) NOT NULL,
    object_key       VARCHAR(500) NOT NULL,
    file_size_bytes  BIGINT,
    file_hash        VARCHAR(128),
    ingestion_status VARCHAR(50) NOT NULL DEFAULT 'RECEIVED',
    received_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at     TIMESTAMP,
    source_name      VARCHAR(100),
    error_message    TEXT,
    client_code      VARCHAR(10),
    file_type        VARCHAR(20),
    env              VARCHAR(10),
    sequence         VARCHAR(3),
    CONSTRAINT chk_ingestion_status CHECK (ingestion_status IN (
        'RECEIVED','PROCESSING','CLEANSED',
        'TRANSFORMED','CURATED','FAILED','QUARANTINED'
    ))
);

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id           BIGSERIAL PRIMARY KEY,
    pipeline_name    VARCHAR(100) NOT NULL,
    run_status       VARCHAR(50) NOT NULL,
    started_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at         TIMESTAMP,
    records_read     INT DEFAULT 0,
    records_written  INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    error_message    TEXT,
    CONSTRAINT chk_run_status CHECK (run_status IN (
        'RUNNING','SUCCESS','FAILED','PARTIAL'
    ))
);
