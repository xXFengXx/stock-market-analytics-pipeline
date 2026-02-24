#!/bin/bash
# ============================================================
# start_airflow.sh
# 在 WSL2 Ubuntu 中启动 Airflow scheduler + webserver
# 用法：在 WSL2 终端中运行 bash start_airflow.sh
# ============================================================

export AIRFLOW_HOME=/opt/airflow
source /opt/airflow-env/bin/activate

# PostgreSQL runs on Windows — resolve its IP from WSL2's default gateway
WINDOWS_HOST_IP=$(ip route show default | awk '{print $3}')
export POSTGRES_HOST=$WINDOWS_HOST_IP
export POSTGRES_PORT=5432
export POSTGRES_DB=stock_analytics
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=stock1234

echo "========================================"
echo "  Starting Airflow Services"
echo "  Windows Host IP : $WINDOWS_HOST_IP"
echo "  PostgreSQL      : $POSTGRES_HOST:5432/stock_analytics"
echo "========================================"

# 停止已有进程
pkill -f "airflow scheduler" 2>/dev/null && echo "Stopped old scheduler"
pkill -f "airflow webserver" 2>/dev/null && echo "Stopped old webserver"
sleep 2

# 启动 scheduler（后台）
echo ""
echo "[1] Starting scheduler..."
airflow scheduler > $AIRFLOW_HOME/scheduler.log 2>&1 &
SCHED_PID=$!
echo "    Scheduler PID: $SCHED_PID"

# 等待 scheduler 初始化
sleep 5

# 启动 webserver（前台，这样终端关闭时会一起停止）
echo ""
echo "[2] Starting webserver on http://localhost:8080"
echo ""
echo "  Login:    admin / admin123"
echo "  Press Ctrl+C to stop both services"
echo "========================================"
echo ""

# 捕获 Ctrl+C 同时停止 scheduler
trap "kill $SCHED_PID 2>/dev/null; echo 'Airflow stopped.'" EXIT

airflow webserver --port 8080
