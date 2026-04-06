#!/usr/bin/env python
"""
Generate Windows Task Scheduler XML files for automation.

Creates XML task definitions for:
  A. polymarket-intelligence (daily pipeline every 4h)
  B. polymarket-monitor (continuous monitor with restart)
  C. api-server (FastAPI on port 8001)

Usage:
  python scripts/setup_automations.py
  python scripts/setup_automations.py --install   (register tasks directly)
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TASK_DIR = PROJECT_ROOT / "scripts" / "task_scheduler"
LOGS_DIR = PROJECT_ROOT / "logs"
PYTHON = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
TRADING_CLI = str(PROJECT_ROOT / ".venv" / "Scripts" / "trading-cli.exe")
WORKING_DIR = str(PROJECT_ROOT)


def _xml_task(name: str, description: str, command: str, arguments: str,
              schedule_type: str = "interval", interval_hours: int = 4,
              on_startup: bool = False) -> str:
    """Generate a Windows Task Scheduler XML definition."""

    trigger_block = ""
    if schedule_type == "interval":
        trigger_block = f"""
    <CalendarTrigger>
      <Repetition>
        <Interval>PT{interval_hours}H</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <StartBoundary>2026-04-06T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
    </CalendarTrigger>"""

    if on_startup:
        trigger_block += """
    <BootTrigger>
      <Enabled>true</Enabled>
      <Delay>PT2M</Delay>
    </BootTrigger>"""

    return dedent(f"""\
    <?xml version="1.0" encoding="UTF-16"?>
    <Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
      <RegistrationInfo>
        <Description>{description}</Description>
      </RegistrationInfo>
      <Triggers>{trigger_block}
      </Triggers>
      <Principals>
        <Principal id="Author">
          <LogonType>InteractiveToken</LogonType>
          <RunLevel>LeastPrivilege</RunLevel>
        </Principal>
      </Principals>
      <Settings>
        <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
        <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
        <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
        <AllowHardTerminate>true</AllowHardTerminate>
        <StartWhenAvailable>true</StartWhenAvailable>
        <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
        <Enabled>true</Enabled>
        <Hidden>false</Hidden>
        <RestartOnFailure>
          <Interval>PT5M</Interval>
          <Count>3</Count>
        </RestartOnFailure>
        <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
      </Settings>
      <Actions>
        <Exec>
          <Command>{command}</Command>
          <Arguments>{arguments}</Arguments>
          <WorkingDirectory>{WORKING_DIR}</WorkingDirectory>
        </Exec>
      </Actions>
    </Task>""")


TASKS = [
    {
        "name": "polymarket-intelligence",
        "description": "Polymarket daily intelligence pipeline (every 4h)",
        "command": PYTHON,
        "arguments": "scripts/run_daily_intelligence.py",
        "schedule_type": "interval",
        "interval_hours": 4,
    },
    {
        "name": "polymarket-monitor",
        "description": "Polymarket continuous live monitor with auto-restart",
        "command": PYTHON,
        "arguments": "scripts/run_live_monitor.py",
        "on_startup": True,
    },
    {
        "name": "trading-api-server",
        "description": "Trading platform FastAPI server on port 8001",
        "command": str(PROJECT_ROOT / ".venv" / "Scripts" / "uvicorn.exe"),
        "arguments": "trading_platform.api.main:app --port 8001",
        "on_startup": True,
    },
]


def generate_tasks() -> None:
    TASK_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    for task in TASKS:
        xml = _xml_task(
            name=task["name"],
            description=task["description"],
            command=task["command"],
            arguments=task["arguments"],
            schedule_type=task.get("schedule_type", "startup"),
            interval_hours=task.get("interval_hours", 4),
            on_startup=task.get("on_startup", False),
        )
        path = TASK_DIR / f"{task['name']}.xml"
        path.write_text(xml, encoding="utf-16")
        print(f"  Created: {path}")

    # Write start_all.sh
    start_script = PROJECT_ROOT / "scripts" / "start_all.sh"
    lines = [
        "#!/bin/bash",
        "# Start all trading platform processes",
        f'cd "{WORKING_DIR}"',
        "",
        "# API server (background)",
        f'"{PYTHON}" -m uvicorn trading_platform.api.main:app --port 8001 &',
        "API_PID=$!",
        "",
        "# Live monitor (background)",
        f'"{PYTHON}" scripts/run_live_monitor.py &',
        "MONITOR_PID=$!",
        "",
        'echo "API PID: $API_PID"',
        'echo "Monitor PID: $MONITOR_PID"',
        'echo "Press Ctrl+C to stop all"',
        "wait",
    ]
    start_script.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Created: {start_script}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate automation configs")
    parser.add_argument("--install", action="store_true", help="Register tasks with Task Scheduler")
    args = parser.parse_args()

    print("Generating automation files...")
    generate_tasks()

    print()
    if args.install:
        print("Registering tasks with Task Scheduler...")
        for task in TASKS:
            xml_path = TASK_DIR / f"{task['name']}.xml"
            cmd = ["schtasks", "/create", "/xml", str(xml_path), "/tn", task["name"], "/f"]
            print(f"  Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"    OK")
            else:
                print(f"    FAILED: {result.stderr.strip()}")
    else:
        print("To register tasks (run as Administrator):")
        for task in TASKS:
            xml_path = TASK_DIR / f"{task['name']}.xml"
            print(f'  schtasks /create /xml "{xml_path}" /tn "{task["name"]}" /f')

    print()
    print("Manual startup (no Task Scheduler):")
    print(f"  bash scripts/start_all.sh")


if __name__ == "__main__":
    main()
