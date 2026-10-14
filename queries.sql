-- ============================================================================
-- Financial Pipeline Analytics & Queries
-- ============================================================================

-- Query 1: Latest Real-Time Market Ticks
SELECT 
    time, 
    symbol, 
    price, 
    day_volume 
FROM crypto_ticks 
ORDER BY time DESC 
LIMIT 10;

-- Query 2: 1-Minute OHLCV Candlesticks (for TradingView / Grafana charts)
SELECT 
    bucket, 
    symbol, 
    open, 
    high, 
    low, 
    close, 
    volume,
    ROUND(((close - open) / open * 100)::numeric, 2) AS pct_change
FROM crypto_candlestick_1m 
WHERE bucket >= NOW() - INTERVAL '1 hour'
ORDER BY bucket DESC, symbol;

-- Query 3: Multi-Timeframe Rollup: 5-Minute and 15-Minute Candlesticks
SELECT 
    time_bucket('5 minutes', bucket) AS interval_5m,
    symbol,
    first(open, bucket) AS open_5m,
    max(high) AS high_5m,
    min(low) AS low_5m,
    last(close, bucket) AS close_5m,
    sum(volume) AS volume_5m
FROM crypto_candlestick_1m
WHERE bucket >= NOW() - INTERVAL '6 hours'
GROUP BY interval_5m, symbol
ORDER BY interval_5m DESC;
