#!/bin/bash
# Start all trading platform processes
cd "C:\Users\bradl\PycharmProjects\trading_platform"

# API server (background)
"C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe" -m uvicorn trading_platform.api.main:app --port 8001 &
API_PID=$!

# Live monitor (background)
"C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe" scripts/run_live_monitor.py &
MONITOR_PID=$!

echo "API PID: $API_PID"
echo "Monitor PID: $MONITOR_PID"
echo "Press Ctrl+C to stop all"
wait