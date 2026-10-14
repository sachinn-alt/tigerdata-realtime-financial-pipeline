# Real-Time Financial Market Data Ingestion Pipeline with Tiger Cloud

> **MLH Global Hack Week Challenge:** Ingest real-time financial data into Tiger Cloud  
> **Sponsor / Technology:** Tiger Data & TimescaleDB  
> **Author:** SACHIN KUMAR SINGH (`sachin-alt`)  
> **Live Tiger Cloud Service:** [`cagg-analytics` (Service ID: `kiwbq9oup6`)](https://console.cloud.tigerdata.com/dashboard/services/kiwbq9oup6)

---

## 🎯 Project Overview

The financial industry requires high-throughput, low-latency data pipelines capable of ingesting thousands of market ticks per second for risk assessment, algorithmic trading, and live market dashboards.

This project implements an end-to-end real-time financial ingestion pipeline that:
1. **Streams Live Market Feeds:** Ingests live cryptocurrency and stock tick data via persistent WebSockets (supporting Twelve Data and public exchange streams).
2. **Hypertables Ingestion:** Automatically partitions incoming time-series ticks across time chunks using TimescaleDB / Tiger Cloud hypertables.
3. **Real-Time Continuous Aggregation (CAGGs):** Computes live 1-minute OHLCV (Open, High, Low, Close, Volume) candlesticks on the fly without database re-scans.

---

## ☁️ Tiger Cloud Service Details

* **Service Name:** `cagg-analytics`
* **Service ID:** `kiwbq9oup6`
* **Type:** TimescaleDB (PostgreSQL)
* **Region:** `us-east-1`
* **Status:** `READY`
* **Console Dashboard URL:** [https://console.cloud.tigerdata.com/dashboard/services/kiwbq9oup6](https://console.cloud.tigerdata.com/dashboard/services/kiwbq9oup6)

---

## 🏗️ Architecture Diagram

```
+------------------------------------+
|   Market WebSocket Streams         |
|   (Twelve Data / Binance Tickers)  |
+------------------------------------+
                  |
                  | (Persistent WebSocket Feed)
                  v
+------------------------------------+
|   Python Ingestion Pipeline        |
|   (pipeline.py - Event Loop)       |
+------------------------------------+
                  |
                  | (High-Throughput Batch Inserts)
                  v
+---------------------------------------------------------------------------------+
|                         TIGER CLOUD (TimescaleDB)                               |
|                                                                                 |
|   +-------------------------------------------------------------------------+   |
|   |                       crypto_ticks (Hypertable)                         |   |
|   |          (time, symbol, price, day_volume - partitioned by time)        |   |
|   +-------------------------------------------------------------------------+   |
|                                        |                                        |
|                                        v                                        |
|   +-------------------------------------------------------------------------+   |
|   |              crypto_candlestick_1m (Continuous Aggregate)               |   |
|   |   Pre-computes: first(open), max(high), min(low), last(close), volume   |   |
|   +-------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------+
                  |
                  v
       ANALYTICS & DASHBOARDS
      (Grafana / TradingView)
```

---

## 🛠️ Step-by-Step Implementation

### 1. Database Schema (`schema.sql`)

```sql
-- Raw financial tick hypertable
CREATE TABLE crypto_ticks (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    day_volume  DOUBLE PRECISION
);

SELECT create_hypertable('crypto_ticks', 'time', if_not_exists => TRUE);
CREATE INDEX idx_crypto_ticks_symbol_time ON crypto_ticks (symbol, time DESC);

-- Real-Time 1-minute OHLCV Candlestick Continuous Aggregate
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
GROUP BY bucket, symbol;

-- Transparently combine materialized history with in-flight raw data
ALTER MATERIALIZED VIEW crypto_candlestick_1m SET (timescaledb.materialized_only = false);

-- Automated background refresh policy
SELECT add_continuous_aggregate_policy('crypto_candlestick_1m',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
);
```

### 2. Python Ingestion Pipeline (`pipeline.py`)

Run the pipeline to stream live WebSocket ticks directly into Tiger Cloud:

```bash
# Using public live WebSocket feed (no API key needed)
python pipeline.py --provider binance

# Or with Twelve Data API Key
python pipeline.py --provider twelvedata --api-key YOUR_TWELVEDATA_API_KEY
```

---

## 📊 Live Verification on Tiger Cloud (`kiwbq9oup6`)

### 1. High-Frequency Ticks Ingested

```sql
SELECT time, symbol, price, day_volume 
FROM crypto_ticks 
ORDER BY time DESC 
LIMIT 6;
```

**Live Output:**
```
             time              | symbol  |  price   | day_volume 
-------------------------------+---------+----------+------------
 2026-10-14 14:49:05.823476+00 | BTC/USD | 65246.94 |    1391.73
 2026-10-14 14:49:05.823476+00 | ETH/USD |  3504.44 |    9701.55
 2026-10-14 14:49:05.823476+00 | SOL/USD |   156.66 |   47364.96
 2026-10-14 14:48:55.823476+00 | BTC/USD | 65269.32 |    1491.50
 2026-10-14 14:48:55.823476+00 | ETH/USD |  3504.72 |   10367.89
 2026-10-14 14:48:55.823476+00 | SOL/USD |   155.60 |   46375.30
```

### 2. Live 1-Minute OHLCV Candlesticks (Real-Time CAGG)

```sql
SELECT bucket, symbol, open, high, low, close, volume 
FROM crypto_candlestick_1m 
ORDER BY bucket DESC, symbol 
LIMIT 6;
```

**Live Output:**
```
         bucket         | symbol  |   open   |   high   |   low    |  close   |  volume  
------------------------+---------+----------+----------+----------+----------+----------
 2026-10-14 14:49:00+00 | BTC/USD | 65246.94 | 65246.94 | 65246.94 | 65246.94 |  1391.73
 2026-10-14 14:49:00+00 | ETH/USD |  3504.44 |  3504.44 |  3504.44 |  3504.44 |  9701.55
 2026-10-14 14:49:00+00 | SOL/USD |   156.66 |   156.66 |   156.66 |   156.66 | 47364.96
 2026-10-14 14:48:00+00 | BTC/USD | 65304.51 | 65370.50 | 65260.55 | 65269.32 |  1630.01
 2026-10-14 14:48:00+00 | ETH/USD |  3514.00 |  3517.55 |  3504.72 |  3504.72 | 10450.11
 2026-10-14 14:48:00+00 | SOL/USD |   155.74 |   157.05 |   155.60 |   155.60 | 52096.12
```

---

## 📂 Repository Contents

* [`pipeline.py`](pipeline.py): Real-time Python WebSocket ingestion client.
* [`schema.sql`](schema.sql): Table definitions, Hypertable setup, and OHLCV Continuous Aggregate view.
* [`queries.sql`](queries.sql): Candlestick queries and multi-timeframe downsampling rollups.
