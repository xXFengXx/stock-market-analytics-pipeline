"""
stock_pipeline_dag_wsl2.py
──────────────────────────
Airflow DAG for WSL2 environment.

In WSL2, the Windows D: drive is mounted at /mnt/d/
This DAG uses /mnt/d/ paths and the virtualenv at /opt/airflow-env/

Schedule: Mon-Fri at 02:00 UTC (after US market close)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Paths (WSL2 format) ────────────────────────────────────────────────────────
PROJECT_ROOT  = Path("/mnt/d/Work/Data/Stock Market Analytics Pipeline")
SCRIPTS_DIR   = PROJECT_ROOT / "scripts"
DBT_DIR       = PROJECT_ROOT / "stock_dbt"
PYTHON        = "/opt/airflow-env/bin/python"
DBT           = "/opt/airflow-env/bin/dbt"

# Dynamically resolve the Windows host IP so WSL2 can reach PostgreSQL on Windows.
# Default gateway pointing to host = Windows host IP from WSL2's perspective.
ENV_PREFIX = (
    "export POSTGRES_HOST=$(ip route show default | awk '{print $3}') && "
    "export POSTGRES_PORT=5432 && "
    "export POSTGRES_DB=stock_analytics && "
    "export POSTGRES_USER=postgres && "
    "export POSTGRES_PASSWORD=stock1234 && "
)

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="stock_market_pipeline",
    description="Daily stock data ingestion, PostgreSQL load, and DBT transforms.",
    schedule_interval="0 2 * * 1-5",   # Mon–Fri 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["finance", "stocks", "dbt"],
    doc_md="""
## Stock Market Analytics Pipeline (WSL2)

Runs daily Mon–Fri at 02:00 UTC and:
1. Downloads fresh OHLCV data from Yahoo Finance (yfinance)
2. Upserts data into PostgreSQL `raw.stock_prices`
3. Runs all DBT models (staging → fact → marts)
4. Validates data quality with DBT tests

**Project root:** `/mnt/d/Work/Data/Stock Market Analytics Pipeline`
    """,
) as dag:

    ingest = BashOperator(
        task_id="ingest_stock_data",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd '{PROJECT_ROOT}' && "
            f"{PYTHON} '{SCRIPTS_DIR}/ingest_stock_data.py' --years 1"
        ),
        doc_md="Downloads 1 year of OHLCV data from Yahoo Finance for all 10 tickers.",
    )

    load = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd '{PROJECT_ROOT}' && "
            f"{PYTHON} '{SCRIPTS_DIR}/load_to_postgres.py'"
        ),
        doc_md="Upserts raw CSVs into PostgreSQL raw.stock_prices.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd '{PROJECT_ROOT}' && "
            f"{DBT} run "
            f"--project-dir '{DBT_DIR}' "
            f"--profiles-dir '{DBT_DIR}' "
            f"--target dev"
        ),
        doc_md="Builds all DBT models: staging view + fact/mart tables.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd '{PROJECT_ROOT}' && "
            f"{DBT} test "
            f"--project-dir '{DBT_DIR}' "
            f"--profiles-dir '{DBT_DIR}' "
            f"--target dev"
        ),
        doc_md="Runs 23 DBT data quality tests (not_null, unique, accepted_values).",
    )

    # ── Pipeline order ─────────────────────────────────────────────────────────
    ingest >> load >> dbt_run >> dbt_test
