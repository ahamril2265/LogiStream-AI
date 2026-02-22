-- Initialize PostgreSQL database for logistics platform
-- This script creates tables for:
-- 1. Airflow backend
-- 2. Data lineage tracking
-- 3. Data quality metrics

CREATE SCHEMA IF NOT EXISTS logistics;

-- Data lineage tracking table
CREATE TABLE IF NOT EXISTS logistics.data_lineage (
    id SERIAL PRIMARY KEY,
    stage VARCHAR(100) NOT NULL,
    dataset VARCHAR(100) NOT NULL,
    row_count BIGINT,
    partition_path VARCHAR(255),
    source_system VARCHAR(50),
    record_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingestion_id VARCHAR(50)
);

-- Data quality metrics
CREATE TABLE IF NOT EXISTS logistics.dq_metrics (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    dataset VARCHAR(100) NOT NULL,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    metric_name VARCHAR(100),
    metric_value FLOAT,
    threshold FLOAT,
    details JSONB
);

-- Model training runs
CREATE TABLE IF NOT EXISTS logistics.model_runs (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    version VARCHAR(50),
    feature_dataset_hash VARCHAR(64),
    training_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metrics JSONB,
    mlflow_run_id VARCHAR(100),
    status VARCHAR(20)
);

-- Create indexes
CREATE INDEX idx_data_lineage_stage ON logistics.data_lineage(stage, record_timestamp);
CREATE INDEX idx_dq_metrics_dataset ON logistics.dq_metrics(dataset, check_timestamp);
CREATE INDEX idx_model_runs_timestamp ON logistics.model_runs(training_timestamp DESC);

-- Grant privileges
GRANT USAGE ON SCHEMA logistics TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA logistics TO postgres;

COMMIT;
