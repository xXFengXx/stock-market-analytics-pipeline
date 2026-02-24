#!/usr/bin/env python3
"""
ingest_stock_data.py
────────────────────
Downloads daily OHLCV data from Yahoo Finance for the configured tickers,
cleanses it, writes per-ticker CSVs (data/raw/<TICKER>.csv), and exports
a combined JSON for the HTML dashboard (data/processed/all_tickers.json).

Usage
-----
    python scripts/ingest_stock_data.py
    python scripts/ingest_stock_data.py --tickers AAPL MSFT --years 3
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

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
RAW_DIR = PROJECT_ROOT / os.getenv("RAW_DATA_DIR", "data/raw")
PROCESSED_DIR = PROJECT_ROOT / os.getenv("PROCESSED_DATA_DIR", "data/processed")

DEFAULT_TICKERS = os.getenv(
    "TICKERS", "AAPL,MSFT,AMZN,GOOGL,META,NVDA,TSLA,BRK-B,JPM,JNJ"
).split(",")
DEFAULT_YEARS = int(os.getenv("HISTORY_YEARS", "5"))

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _snake(col: str) -> str:
    """Lower-case and replace spaces/special chars with underscores."""
    return col.strip().lower().replace(" ", "_").replace("-", "_")


def _standardise(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Standardise a raw yfinance DataFrame:
    - Flatten MultiIndex columns (yfinance v0.2+ returns them).
    - Rename columns to snake_case.
    - Ensure 'date' is YYYY-MM-DD string.
    - Add 'ticker' column.
    - Drop rows missing all OHLCV values.
    - Sort by date ascending.
    """
    # ── Flatten MultiIndex ──────────────────────────────────────────────────
    if isinstance(df.columns, pd.MultiIndex):
        # yfinance.download with multiple tickers returns (metric, ticker)
        # When downloading one ticker at a time the second level is the ticker.
        df.columns = [_snake(c[0]) for c in df.columns]
    else:
        df.columns = [_snake(c) for c in df.columns]

    # ── Reset index so 'date' becomes a column ──────────────────────────────
    df = df.reset_index()
    if "date" not in df.columns and "datetime" in df.columns:
        df = df.rename(columns={"datetime": "date"})
    if "date" not in df.columns:
        # The index was the date
        df = df.rename(columns={df.columns[0]: "date"})

    # ── Normalise date format ───────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # ── Rename adj_close variants ───────────────────────────────────────────
    for raw_name in ("adj_close", "adjclose", "adjusted_close"):
        if raw_name in df.columns and raw_name != "adjusted_close":
            df = df.rename(columns={raw_name: "adjusted_close"})
            break

    # ── Add ticker ──────────────────────────────────────────────────────────
    df["ticker"] = ticker.upper()

    # ── Drop rows where ALL OHLCV values are null ───────────────────────────
    ohlcv_present = [c for c in REQUIRED_COLUMNS if c in df.columns]
    before = len(df)
    df = df.dropna(subset=ohlcv_present, how="all")
    dropped = before - len(df)
    if dropped:
        log.warning("  [%s] dropped %d fully-null rows", ticker, dropped)

    # ── Round numeric columns ───────────────────────────────────────────────
    numeric_cols = df.select_dtypes("number").columns
    df[numeric_cols] = df[numeric_cols].round(4)

    # ── Sort ────────────────────────────────────────────────────────────────
    df = df.sort_values("date").reset_index(drop=True)

    return df


def download_ticker(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """Download one ticker's data from Yahoo Finance and return a clean DataFrame."""
    log.info("Downloading  %-8s  %s → %s", ticker, start, end)
    try:
        raw = yf.download(
            ticker,
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
            threads=False,
        )
        if raw.empty:
            log.warning("  [%s] no data returned – skipping", ticker)
            return None
        df = _standardise(raw, ticker)
        log.info("  [%s] %d rows downloaded", ticker, len(df))
        return df
    except Exception as exc:  # noqa: BLE001
        log.error("  [%s] download failed: %s", ticker, exc)
        return None


def run(tickers: list[str], years: int) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=years * 365)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    log.info("═" * 60)
    log.info("Stock ingestion  |  tickers=%d  |  years=%d", len(tickers), years)
    log.info("Date range       |  %s → %s", start_str, end_str)
    log.info("═" * 60)

    all_frames: list[pd.DataFrame] = []

    for ticker in tickers:
        df = download_ticker(ticker, start_str, end_str)
        if df is None:
            continue

        # Save raw CSV
        csv_path = RAW_DIR / f"{ticker.replace('-', '_')}.csv"
        df.to_csv(csv_path, index=False)
        log.info("  [%s] saved → %s", ticker, csv_path.relative_to(PROJECT_ROOT))

        all_frames.append(df)

    if not all_frames:
        log.error("No data downloaded. Check ticker names and network connection.")
        sys.exit(1)

    combined = pd.concat(all_frames, ignore_index=True)

    # ── Dashboard JSON export ───────────────────────────────────────────────
    # Group by ticker, keep only date + close + volume (dashboard only needs these)
    dashboard_payload: dict = {}
    for ticker, grp in combined.groupby("ticker"):
        dashboard_payload[ticker] = {
            "dates": grp["date"].tolist(),
            "close": grp["close"].tolist(),
            "volume": grp["volume"].fillna(0).astype(int).tolist(),
            # Moving averages computed client-side in the dashboard
        }

    json_path = PROCESSED_DIR / "all_tickers.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(dashboard_payload, fh, separators=(",", ":"))
    log.info("Dashboard JSON  → %s", json_path.relative_to(PROJECT_ROOT))

    # ── Summary ─────────────────────────────────────────────────────────────
    log.info("═" * 60)
    log.info("Ingestion complete. %d tickers processed.", len(all_frames))
    log.info(
        "Combined dataset: %d rows  |  %s → %s",
        len(combined),
        combined["date"].min(),
        combined["date"].max(),
    )
    log.info("═" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest stock data from Yahoo Finance.")
    p.add_argument(
        "--tickers",
        nargs="+",
        default=DEFAULT_TICKERS,
        metavar="TICKER",
        help="Space-separated list of ticker symbols (default: from .env / TICKERS).",
    )
    p.add_argument(
        "--years",
        type=int,
        default=DEFAULT_YEARS,
        help="Years of history to download (default: 5).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args.tickers, args.years)
