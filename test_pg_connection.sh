#!/bin/bash
# test_pg_connection.sh — Test PostgreSQL connectivity from WSL2

WINDOWS_IP=$(ip route show default | awk '{print $3}')
echo "Windows host IP (Gateway): $WINDOWS_IP"

/opt/airflow-env/bin/python - <<PYEOF
import psycopg2

host = "$WINDOWS_IP"
print(f"Connecting to PostgreSQL at {host}:5432 ...")
try:
    conn = psycopg2.connect(
        host=host, port=5432,
        dbname="stock_analytics",
        user="postgres",
        password="stock1234"
    )
    cur = conn.cursor()
    cur.execute("SELECT MAX(date), COUNT(*) FROM raw.stock_prices")
    row = cur.fetchone()
    print(f"SUCCESS! Connected to PostgreSQL on Windows host.")
    print(f"  Latest date : {row[0]}")
    print(f"  Total rows  : {row[1]}")
    conn.close()
except Exception as e:
    print(f"FAILED: {e}")
PYEOF
