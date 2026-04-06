@echo off
REM Runs the daily Polymarket refresh pipeline.
REM Output is tee'd to logs/daily_refresh.log by the Python script itself.

cd /d "%~dp0\.."
if not exist logs mkdir logs

.venv\Scripts\python.exe -m trading_platform.cli data polymarket daily-refresh
