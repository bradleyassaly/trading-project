"""
Daily task health summary — runs the --summary mode of log_monitor.py.

Usage:
    python scripts/daily_task_summary.py

Schedule via Cowork or Task Scheduler at 8am daily.
"""
import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    script = Path(__file__).resolve().parent / "log_monitor.py"
    sys.exit(subprocess.call([sys.executable, str(script), "--summary"]))
