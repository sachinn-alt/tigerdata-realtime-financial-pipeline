"""
Tiger Cloud: Real-Time Financial Data Ingestion Pipeline
========================================================
Connects to live market WebSocket streams (Twelve Data / public crypto feeds)
and ingests high-frequency tick data into a TimescaleDB Hypertable on Tiger Cloud.
"""

import os
import sys
import json
import time
import datetime
import argparse
import psycopg2
import websocket

# Default connection settings for Tiger Cloud service (or from environment)
DEFAULT_DB_URI = os.getenv(
    "TIGER_DATABASE_URI",
    "postgresql://tsdbadmin@kiwbq9oup6.fflhx4ep14.tsdb.cloud.timescale.com:37201/tsdb?sslmode=require"
)

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")


def get_db_connection(db_uri: str):
    """Establishes connection to Tiger Cloud TimescaleDB instance."""
    try:
        conn = psycopg2.connect(db_uri)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"[-] Database connection error: {e}")
        return None


def insert_tick(cursor, timestamp, symbol, price, volume):
    """Inserts an incoming tick into the TimescaleDB hypertable."""
    query = """
    INSERT INTO crypto_ticks (time, symbol, price, day_volume)
    VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(query, (timestamp, symbol, price, volume))
        print(f"[+] Ingested tick: {symbol} | Price: ${price:,.2f} | Vol: {volume} | Time: {timestamp}")
    except Exception as e:
        print(f"[-] Insert error: {e}")


def on_binance_message(ws, message, cursor):
    """Callback for Binance public real-time ticker stream (No API key required)."""
    try:
        data = json.loads(message)
        # Format symbol e.g., 'BTCUSDT' -> 'BTC/USD'
        raw_symbol = data.get('s', '')
        symbol_map = {
            'BTCUSDT': 'BTC/USD',
            'ETHUSDT': 'ETH/USD',
            'SOLUSDT': 'SOL/USD'
        }
        symbol = symbol_map.get(raw_symbol, raw_symbol)
        price = float(data.get('c', 0))
        volume = float(data.get('v', 0))
        ts = datetime.datetime.fromtimestamp(data.get('E', time.time() * 1000) / 1000.0, tz=datetime.timezone.utc)

        if price > 0:
            insert_tick(cursor, ts, symbol, price, volume)
    except Exception as err:
        print(f"[-] Message parse error: {err}")


def run_binance_stream(db_uri):
    """Streams live market data from Binance Public WebSockets without needing an API key."""
    conn = get_db_connection(db_uri)
    if not conn:
        sys.exit(1)
    cursor = conn.cursor()

    stream_url = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/solusdt@ticker"
    print(f"[*] Connecting to live WebSocket stream: {stream_url}")

    def on_msg(ws, msg):
        on_binance_message(ws, msg, cursor)

    def on_err(ws, error):
        print(f"[-] WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print(f"[*] WebSocket closed: {close_status_code} - {close_msg}")

    def on_open(ws):
        print("[+] WebSocket connected. Ingesting live ticks into Tiger Cloud...")

    ws = websocket.WebSocketApp(
        stream_url,
        on_message=on_msg,
        on_error=on_err,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()


def run_twelvedata_stream(api_key, db_uri):
    """Streams real-time market data from Twelve Data WebSocket."""
    from twelvedata import TDClient

    conn = get_db_connection(db_uri)
    if not conn:
        sys.exit(1)
    cursor = conn.cursor()

    td = TDClient(apikey=api_key)

    def on_event(event):
        symbol = event.get("symbol")
        price = event.get("price")
        volume = event.get("day_volume", 0)
        timestamp = datetime.datetime.now(datetime.timezone.utc)

        if symbol and price:
            insert_tick(cursor, timestamp, symbol, float(price), float(volume))

    print("[*] Launching Twelve Data WebSocket stream for BTC/USD, ETH/USD, AAPL...")
    ws = td.websocket(
        symbols=["BTC/USD", "ETH/USD", "AAPL"],
        on_event=on_event
    )
    ws.connect()
    ws.keep_alive()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tiger Cloud Financial Ingestion Pipeline")
    parser.add_argument("--db-uri", default=DEFAULT_DB_URI, help="Tiger Cloud PostgreSQL Connection URI")
    parser.add_argument("--provider", choices=["binance", "twelvedata"], default="binance",
                        help="Market data provider (binance requires no key, twelvedata uses API key)")
    parser.add_argument("--api-key", default=TWELVEDATA_API_KEY, help="Twelve Data API Key")

    args = parser.parse_args()

    if args.provider == "twelvedata":
        if not args.api_key:
            print("[-] Error: Twelve Data requires an API key via --api-key or TWELVEDATA_API_KEY env var.")
            sys.exit(1)
        run_twelvedata_stream(args.api_key, args.db_uri)
    else:
        run_binance_stream(args.db_uri)
