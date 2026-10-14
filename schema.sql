-- ============================================================================
-- Tiger Cloud: Real-Time Financial Data Ingestion & OHLCV Candlesticks
-- Challenge: Ingest real-time financial data into Tiger Cloud
-- ============================================================================

-- 1. Create table for high-frequency financial tick ingestion
DROP TABLE IF EXISTS crypto_ticks CASCADE;

CREATE TABLE crypto_ticks (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    day_volume  DOUBLE PRECISION
);

-- 2. Convert to TimescaleDB Hypertable partitioned by time
SELECT create_hypertable(
    'crypto_ticks', 
    'time', 
    if_not_exists => TRUE
);

-- 3. Composite Index on (symbol, time DESC) for sub-millisecond lookups
CREATE INDEX IF NOT EXISTS idx_crypto_ticks_symbol_time 
ON crypto_ticks (symbol, time DESC);

-- 4. Create Continuous Aggregate (CAGG) for Real-Time 1-Minute OHLCV Candlesticks
-- Pre-computes Open, High, Low, Close, and Volume metrics
DROP MATERIALIZED VIEW IF EXISTS crypto_candlestick_1m CASCADE;

CREATE MATERIALIZED VIEW crypto_candlestick_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,
    first(price, time)            AS open,
    max(price)                    AS high,
    min(price)                    AS low,
    last(price, time)             AS close,
    max(day_volume)               AS volume
FROM crypto_ticks
GROUP BY bucket, symbol
WITH NO DATA;

-- 5. Enable Real-Time Aggregates
-- Ensures the view seamlessly combines materialized historical buckets with in-flight live raw ticks
ALTER MATERIALIZED VIEW crypto_candlestick_1m SET (timescaledb.materialized_only = false);

-- 6. Add Automated Background Refresh Policy
SELECT add_continuous_aggregate_policy(
    'crypto_candlestick_1m',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
);
