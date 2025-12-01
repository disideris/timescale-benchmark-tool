-- Migration 001: Initialize cpu_usage hypertable
-- This script sets up the TimescaleDB hypertable for CPU usage data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the cpu_usage table (exact schema from assignment)
CREATE TABLE IF NOT EXISTS cpu_usage(
    ts    TIMESTAMPTZ,
    host  TEXT,
    usage DOUBLE PRECISION
);

-- Convert to hypertable
SELECT create_hypertable('cpu_usage', 'ts', if_not_exists => TRUE);

-- Create index on host for efficient hostname lookups (critical for our queries)
CREATE INDEX IF NOT EXISTS idx_cpu_usage_host_ts ON cpu_usage (host, ts);

-- Output confirmation
DO $$
BEGIN
    RAISE NOTICE 'TimescaleDB homework database initialized successfully';
END $$;
