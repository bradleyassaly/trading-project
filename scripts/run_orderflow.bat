@echo off
REM Auto-restart loop for Polymarket orderflow collector.
REM Logs to logs\orderflow.log with timestamps.
REM Restarts after 10 seconds on crash.

cd /d "%~dp0\.."
if not exist logs mkdir logs

:loop
echo [%date% %time%] Starting orderflow collector... >> logs\orderflow.log
.venv\Scripts\python.exe -m trading_platform.cli data polymarket collect-orderflow --interval 300 >> logs\orderflow.log 2>&1
echo [%date% %time%] Orderflow collector exited with code %ERRORLEVEL%. Restarting in 10s... >> logs\orderflow.log
timeout /t 10 /nobreak >nul
goto loop
