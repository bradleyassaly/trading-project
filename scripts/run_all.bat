@echo off
REM Start monitor and orderflow collector in separate minimized windows.
REM Run this manually when you want both services running.

cd /d "%~dp0\.."

echo Starting real-time monitor...
start "TradingPlatform Monitor" /min cmd /c "scripts\run_monitor.bat"

echo Starting orderflow collector...
start "TradingPlatform Orderflow" /min cmd /c "scripts\run_orderflow.bat"

echo Both services started in background windows.
echo Close the minimized windows to stop them.
