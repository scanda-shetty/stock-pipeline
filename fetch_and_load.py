import os
import time
import logging
from typing import Dict, List
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

API_KEY = os.getenv("API_KEY")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL").split(",") if s.strip()]

PG_CONN_INFO = dict(
    dbname=os.getenv("POSTGRES_DB", "stocks"),
    user=os.getenv("POSTGRES_USER", "stocks_user"),
    password=os.getenv("POSTGRES_PASSWORD", "supersecret"),
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
)

url = os.getenv('ALPHA_URL')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class DataFetchError(Exception):
    pass

def fetch_symbol_daily_adjusted(symbol):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
    except requests.RequestException as e:
        raise DataFetchError(f"Network error for {symbol}: {e}") from e

    if resp.status_code != 200:
        raise DataFetchError(f"HTTP {resp.status_code} for {symbol}")

    data = resp.json()

    if "Information" in data:
        raise DataFetchError(f"API limit reached for {symbol}: {data['Information']}")
    if "Error Message" in data:
        raise DataFetchError(f"API error for {symbol}: {data['Error Message']}")
    if "Time Series (Daily)" not in data:
        raise DataFetchError(f"Unexpected payload for {symbol}")

    return data["Time Series (Daily)"]

def parse_rows(symbol, series):
    rows = []
    for date, d in series.items():
        open = d.get("1. open")
        high = d.get("2. high")
        low = d.get("3. low")
        close = d.get("4. close")
        volume = d.get("5. volume")
        logging.info("date %s", date)
        logging.info("volume %s", volume)
        logging.info("type %s", type(volume))
        def safe(x, cast):
            try:
                return cast(x) if x is not None else None
            except Exception:
                return None

        rows.append((
            symbol,
            safe(date, str),
            safe(open, float),
            safe(high, float),
            safe(low, float),
            safe(close, float),
            safe(volume, str),
        ))
    return rows

query = """
INSERT INTO stock_prices
(symbol, date, open, high, low, close, volume)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (symbol, date) DO UPDATE SET
  open=EXCLUDED.open,
  high=EXCLUDED.high,
  low=EXCLUDED.low,
  close=EXCLUDED.close,
  volume=EXCLUDED.volume;
"""

def get_max_date(symbol):
    conn = psycopg2.connect(**PG_CONN_INFO)
    try:
        with conn, conn.cursor() as curr:
            curr.execute("SELECT MAX(date) FROM stock_prices WHERE symbol = %s;", (symbol,))
            row = curr.fetchone()
            return row[0] if row and row[0] else None
    finally:
        conn.close()

def insert_rows(rows):
    if not rows:
        logging.info("No rows to insert.")
        return
    conn = psycopg2.connect(**PG_CONN_INFO)
    try:
        with conn, conn.cursor() as cur:
            execute_batch(cur, query, rows, page_size=500)
        logging.info("Insertedd %d rows", len(rows))
    finally:
        conn.close()

def run_pipeline_once():
    if not API_KEY:
        raise RuntimeError("Alpha Vantage API Key is not set")
    for i, symbol in enumerate(SYMBOLS):
        logging.info("Fetching %s (%d/%d)", symbol, i + 1, len(SYMBOLS))
        try:
            series = fetch_symbol_daily_adjusted(symbol)
            rows = parse_rows(symbol, series)
            rows.sort(key=lambda r: r[1])

            max_date = get_max_date(symbol)
            logging.info("Fetching %s", max_date)
            if max_date is None:
                rows_to_insert = rows[-100:]
                logging.info("IF %d", len(rows_to_insert))
            else:
                rows_to_insert = [x for x in rows if x[1] > str(max_date)]
                logging.info("ELSE %d", len(rows_to_insert))

            insert_rows(rows_to_insert)
        except DataFetchError as e:
            logging.warning("Skipping %s due to error: %s", symbol, e)
        time.sleep(15)

if __name__ == "__main__":
    run_pipeline_once()