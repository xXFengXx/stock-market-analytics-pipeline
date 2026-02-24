@echo off
:: ============================================================
:: open_airflow.bat
:: 双击此文件在 WSL2 中启动 Airflow，并打开浏览器
:: ============================================================

echo Starting Airflow in WSL2...
start wsl -d Ubuntu-22.04 -u root bash "/mnt/d/Work/Data/Stock Market Analytics Pipeline/start_airflow.sh"

:: 等待 10 秒让 webserver 启动
timeout /t 10 /nobreak >nul

:: 打开浏览器
start "" "http://localhost:8080"
echo Done! Browser opened to http://localhost:8080
echo Login: admin / admin123
