# Stock Market Analytics Pipeline

End-to-end pipeline: **yfinance → PostgreSQL → DBT → Airflow → HTML Dashboard**

Tracks 10 major companies: `AAPL MSFT AMZN GOOGL META NVDA TSLA BRK-B JPM JNJ`

---

## Architecture

```
Yahoo Finance (yfinance)
        │
        ▼
scripts/ingest_stock_data.py
   ├── data/raw/<TICKER>.csv
   └── data/processed/all_tickers.json ──► dashboard/index.html
        │
        ▼
scripts/load_to_postgres.py
        │  UPSERT
        ▼
PostgreSQL: raw.stock_prices
        │
        ▼
DBT (stock_dbt/)
   ├── staging.stg_stock_prices       (view)
   ├── marts.fact_stock_prices        (table)
   ├── marts.mart_moving_averages     (table)
   ├── marts.mart_volatility          (table)
   └── marts.mart_performance_comparison (table)
        │
        ▼
Airflow DAG (WSL2): stock_market_pipeline
   ingest → load → dbt run → dbt test  (@daily, Mon–Fri 02:00 UTC)
```

---

## Project Structure

```
Stock Market Analytics Pipeline/
├── .env                                ← database credentials (not tracked in Git)
├── requirements.txt
├── open_airflow.bat                    ← double-click to start Airflow on Windows
├── start_airflow.sh                    ← internal WSL2 script to start Airflow
├── scripts/
│   ├── ingest_stock_data.py            ← yfinance download + CSV export
│   └── load_to_postgres.py             ← upsert CSVs into PostgreSQL
├── data/
│   ├── raw/                            ← per-ticker CSVs (auto-created)
│   └── processed/
│       └── all_tickers.json            ← combined JSON for dashboard
├── stock_dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                    ← reads from env vars
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   └── stg_stock_prices.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── fact_stock_prices.sql
│   │       ├── mart_moving_averages.sql
│   │       ├── mart_volatility.sql
│   │       └── mart_performance_comparison.sql
│   └── analyses/
│       └── exploratory_top_performers.sql
├── dags/
│   └── stock_pipeline_dag.py           ← Airflow DAG
└── dashboard/
    └── index.html                      ← open in any browser, no server needed
```

---

## Setup

### 1. Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.11 |
| PostgreSQL | ≥ 14 |
| Apache Airflow | ≥ 2.9 |
| dbt-postgres | ≥ 1.9 |

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

> **Airflow on Windows**: Airflow is Linux-native. We use WSL2 (Ubuntu) to run it, connecting back to PostgreSQL on the Windows host.

### 3. Configure environment

Make sure a `.env` file exists in the root directory with your PostgreSQL credentials:

```bash
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=stock_analytics
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

### 4. Create the database

```sql
CREATE DATABASE stock_analytics;
```

---

## Running the Pipeline

### Step 1 – Ingest data from Yahoo Finance

```bash
python scripts/ingest_stock_data.py
# Options:
#   --tickers AAPL MSFT NVDA   (override tickers)
#   --years 3                  (override history length, default 5)
```

Outputs:
- `data/raw/<TICKER>.csv` — per-ticker OHLCV CSV files
- `data/processed/all_tickers.json` — combined JSON for the dashboard

### Step 2 – Load into PostgreSQL

```bash
python scripts/load_to_postgres.py
```

Creates `raw.stock_prices` and upserts all rows (safe to re-run).

### Step 3 – Run DBT models

```bash
cd stock_dbt

# Copy profiles.yml to ~/.dbt/ (DBT looks there by default)
# OR pass --profiles-dir flag:
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .
```

### Step 4 – Open the dashboard

```bash
# Simply open in your browser — no web server required
start dashboard/index.html   # Windows
open  dashboard/index.html   # macOS
```

---

## DBT Models Reference

| Model | Type | Description |
|-------|------|-------------|
| `stg_stock_prices` | View | Standardised staging layer from `raw.stock_prices` |
| `fact_stock_prices` | Table | Core fact: one row per (date, ticker), OHLCV + derived metrics |
| `mart_moving_averages` | Table | 7d, 30d, 90d SMA of closing price per ticker |
| `mart_volatility` | Table | Daily return, 30d rolling stddev, annualised volatility |
| `mart_performance_comparison` | Table | Index-normalised performance (base = 100 on first day) |

### DBT Tests

All tests run automatically with `dbt test`:

- `not_null` — `date`, `ticker`, `close`, `volume` across all models
- `accepted_values` — `ticker` must be one of the 10 configured symbols
- `unique` — `(date, ticker)` combination in the fact table

---

## Airflow Orchestration (WSL2)

Because Apache Airflow requires a Unix-like environment, it runs inside **WSL2 (Ubuntu)** while connecting to the PostgreSQL database running on the Windows host. 

To start the Airflow scheduler and webserver:

1. Double-click `open_airflow.bat` on Windows.
2. It will automatically start WSL2, run `start_airflow.sh`, and open `http://localhost:8080` in your browser.

**Login details:**
- **Username:** `admin`
- **Password:** `admin123`

Visit [http://localhost:8080](http://localhost:8080) and enable the `stock_market_pipeline` switch.
The DAG runs automatically every weekday at 02:00 UTC (after US market close).

*Note: If you update `stock_pipeline_dag_wsl2.py`, you must sync it to the internal WSL2 Airflow folder:*
```powershell
wsl -e bash -c "cp '/mnt/d/Work/Data/Stock Market Analytics Pipeline/dags/stock_pipeline_dag_wsl2.py' /opt/airflow/dags/stock_pipeline_dag.py"
```

---

## Pushing Updates to GitHub

This repository is tracked via Git. To save your changes and upload them to GitHub, open a PowerShell terminal in the project folder and run:

```powershell
git add .
git commit -m "Describe your updates here"
git push
```
---

## Dashboard Features

| Tab | Content |
|-----|---------|
| **Closing Prices** | Multi-line chart for all 10 tickers; 1M / 3M / 6M / 1Y / ALL range |
| **Trading Volume** | Bar chart per ticker (switch from header) |
| **Moving Averages** | Raw close + 30d SMA overlay; select up to 10 tickers |
| **Performance** | Normalised to base 100 — who grew the most? 1Y / 2Y / ALL |

> **No server required.** The dashboard loads `data/processed/all_tickers.json` automatically.
> If the file is not found, it renders animated demo data so the charts are always visible.

---

## Key Investment Insights

- **Volatility mart** → compare annualised volatility across tickers → higher volatility = higher risk/reward.
- **Performance comparison** → equal starting point → see which stock compounded fastest over 5 years.
- **Moving averages** → 7d / 30d crossover signals → trend reversals and momentum.
- **Intraday range** (in fact table) → days with unusually large `high - low` highlight news events.
