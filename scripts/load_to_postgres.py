#!/usr/bin/env python3
"""
load_to_postgres.py
────────────────────
Reads all per-ticker CSV files from data/raw/ and upserts them into
PostgreSQL under the 'raw' schema, table 'stock_prices'.

Usage
-----
    python scripts/load_to_postgres.py
    python scripts/load_to_postgres.py --raw-dir data/raw
"""

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = PROJECT_ROOT / os.getenv("RAW_DATA_DIR", "data/raw")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "stock_analytics"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
}

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS raw.stock_prices (
    id            SERIAL,
    date          DATE          NOT NULL,
    ticker        VARCHAR(10)   NOT NULL,
    open          NUMERIC(14,4),
    high          NUMERIC(14,4),
    low           NUMERIC(14,4),
    close         NUMERIC(14,4),
    adjusted_close NUMERIC(14,4),
    volume        BIGINT,
    ingested_at   TIMESTAMP     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_raw_stock_prices PRIMARY KEY (date, ticker)
);
"""

UPSERT_SQL = """
INSERT INTO raw.stock_prices
    (date, ticker, open, high, low, close, adjusted_close, volume, ingested_at)
VALUES %s
ON CONFLICT (date, ticker) DO UPDATE SET
    open           = EXCLUDED.open,
    high           = EXCLUDED.high,
    low            = EXCLUDED.low,
    close          = EXCLUDED.close,
    adjusted_close = EXCLUDED.adjusted_close,
    volume         = EXCLUDED.volume,
    ingested_at    = NOW();
"""

# Columns we expect from the CSVs (in order)
CSV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "adjusted_close", "volume"]


# ── Core ──────────────────────────────────────────────────────────────────────

def get_connection():
    log.info("Connecting to PostgreSQL  %s:%s/%s", DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_SCHEMA)
        cur.execute(DDL_TABLE)
    conn.commit()
    log.info("Schema/table verified: raw.stock_prices")


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    # Ensure adjusted_close column exists (may be named differently)
    for alt in ("adj_close", "adjclose"):
        if alt in df.columns and "adjusted_close" not in df.columns:
            df = df.rename(columns={alt: "adjusted_close"})

    if "adjusted_close" not in df.columns:
        df["adjusted_close"] = df.get("close")

    # Keep only the columns we need
    available = [c for c in CSV_COLUMNS if c in df.columns]
    missing = [c for c in CSV_COLUMNS if c not in df.columns]
    if missing:
        log.warning("  Missing columns in %s: %s – filling with NULL", path.name, missing)
        for col in missing:
            df[col] = None

    df = df[CSV_COLUMNS].copy()

    # Coerce types
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for num_col in ["open", "high", "low", "close", "adjusted_close"]:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    df = df.dropna(subset=["date", "ticker", "close"])
    return df


def upsert_dataframe(conn, df: pd.DataFrame, ticker: str) -> int:
    """Insert/update all rows for one ticker. Returns row count."""
    from datetime import datetime

    ingested_at = datetime.utcnow()

    rows = [
        (
            row["date"],
            row["ticker"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["adjusted_close"],
            int(row["volume"]) if pd.notna(row["volume"]) else None,
            ingested_at,
        )
        for _, row in df.iterrows()
    ]

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)


def run(raw_dir: Path) -> None:
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        log.error("No CSV files found in %s. Run ingest_stock_data.py first.", raw_dir)
        return

    conn = get_connection()
    try:
        ensure_schema(conn)
        total_rows = 0
        for csv_path in csv_files:
            log.info("Loading  %s", csv_path.name)
            df = load_csv(csv_path)
            if df.empty:
                log.warning("  %s produced an empty DataFrame – skipping", csv_path.name)
                continue
            ticker = df["ticker"].iloc[0]
            n = upsert_dataframe(conn, df, ticker)
            total_rows += n
            log.info("  %-8s  %d rows upserted", ticker, n)

        log.info("═" * 60)
        log.info("Load complete. %d total rows upserted into raw.stock_prices.", total_rows)
        log.info("═" * 60)
    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load raw CSV files into PostgreSQL raw.stock_prices.")
    p.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help="Directory containing per-ticker CSV files.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args.raw_dir)
