@echo off
REM Auto-restart loop for Polymarket real-time monitor.
REM Logs to logs\monitor_console.log with timestamps.
REM Restarts after 30 seconds on crash.

cd /d "%~dp0\.."
if not exist logs mkdir logs

:loop
echo [%date% %time%] Starting real-time monitor... >> logs\monitor_console.log
.venv\Scripts\python.exe -m trading_platform.cli data polymarket monitor-realtime --min-fill 100 --auto-trade >> logs\monitor_console.log 2>&1
echo [%date% %time%] Monitor exited with code %ERRORLEVEL%. Restarting in 30s... >> logs\monitor_console.log
timeout /t 30 /nobreak >nul
goto loop
