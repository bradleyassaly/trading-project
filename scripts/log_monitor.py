"""
Shared log monitor and Telegram error alerting system.

Called after any Cowork task to check success/failure and alert on errors.

Usage:
    python scripts/log_monitor.py --task wallet_trade_sync --log logs/wallet_trade_sync.log --status 0
    python scripts/log_monitor.py --task wallet_trade_sync --log logs/wallet_trade_sync.log --status 1
    python scripts/log_monitor.py --summary
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

LOGS_DIR = PROJECT_ROOT / "logs"

# Generic error patterns (checked for all tasks)
_GENERIC_PATTERNS = [
    "ERROR", "FAILED", "Exception", "Traceback",
    "connection refused", "timeout",
]

# Task-specific warning patterns (checked only on success)
_TASK_PATTERNS: dict[str, list[str]] = {
    "wallet_trade_sync": ["new_trades: 0", "trades: 0,", "429"],
    "wallet_position_sync": ["positions: 0,"],
    "enrich_trade_resolution": ["Enriched 0 /", "enriched: 0"],
    "rebuild_wallet_profiles": ["rebuilt: 0 "],
    "compute_signals": [],
    "compute_open_positions": ["0 open positions"],
    "daily_refresh": ["ERROR", "FAILED"],
    "monitor": [],
}

# Expected run frequency per task (hours) — for summary staleness check
_EXPECTED_FREQUENCY: dict[str, float] = {
    "wallet_trade_sync": 3.0,
    "wallet_position_sync": 2.0,
    "enrich_trade_resolution": 24.0,
    "rebuild_wallet_profiles": 24.0,
    "compute_signals": 1.0,
    "compute_open_positions": 6.0,
    "daily_refresh": 24.0,
    "monitor": 0.5,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Log monitor and Telegram alerter")
    parser.add_argument("--task", type=str, help="Task name")
    parser.add_argument("--log", type=str, help="Path to log file")
    parser.add_argument("--status", type=int, default=0, help="Exit code of the task")
    parser.add_argument("--tail", type=int, default=20, help="Lines of log to include in alert")
    parser.add_argument("--summary", action="store_true", help="Run daily summary of all tasks")
    args = parser.parse_args()

    if args.summary:
        _run_summary()
        return

    if not args.task:
        parser.error("--task is required unless --summary is used")

    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_path = Path(args.log) if args.log else None
    log_tail = _read_tail(log_path, args.tail) if log_path else "(no log file specified)"

    if args.status != 0:
        # Task failed
        msg = (
            f"[FAIL] TASK FAILED: {args.task}\n"
            f"Time: {ts}\n"
            f"Exit code: {args.status}\n\n"
            f"Last {args.tail} lines of log:\n"
            f"```\n{log_tail}\n```"
        )
        _send(msg)
        return

    # Task succeeded — check for warning patterns
    suspicious = _find_suspicious_lines(args.task, log_path)
    if suspicious:
        lines_text = "\n".join(suspicious[:10])
        msg = (
            f"[WARN] TASK WARNING: {args.task}\n"
            f"Time: {ts}\n"
            f"Status: Completed but with warnings\n\n"
            f"Suspicious lines:\n"
            f"```\n{lines_text}\n```"
        )
        _send(msg)
        return

    # Clean success
    print(f"OK: {args.task} completed cleanly")


def _run_summary() -> None:
    """Scan all log files and produce a daily task digest."""
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    lines = [f"Daily Task Summary - {ts}\n"]
    any_issues = False

    for task, max_hours in sorted(_EXPECTED_FREQUENCY.items()):
        log_path = LOGS_DIR / f"{task}.log"
        if not log_path.exists():
            alt = LOGS_DIR / f"{task}_console.log"
            if alt.exists():
                log_path = alt
            else:
                lines.append(f"[X] {task} - no log found (never run?)")
                any_issues = True
                continue

        age_hours = (now - log_path.stat().st_mtime) / 3600
        if age_hours > max_hours * 2:
            age_str = f"{age_hours:.0f}h ago" if age_hours < 48 else f"{age_hours/24:.0f}d ago"
            lines.append(f"[!] {task} - last run {age_str} (expected every {max_hours:.0f}h)")
            any_issues = True
        else:
            age_str = f"{age_hours:.0f}h ago" if age_hours >= 1 else f"{age_hours*60:.0f}m ago"
            lines.append(f"[OK] {task} - last run {age_str}")

    msg = "[SUMMARY] " + "\n".join(lines)

    if any_issues:
        _send(msg)
    else:
        print(msg)
        print("\nAll tasks healthy — no Telegram alert sent.")


def _find_suspicious_lines(task: str, log_path: Path | None) -> list[str]:
    """Check log file for warning patterns. Returns matching lines."""
    if not log_path or not log_path.exists():
        return []

    patterns = _GENERIC_PATTERNS + _TASK_PATTERNS.get(task, [])
    if not patterns:
        return []

    matches = []
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            for p in patterns:
                if p.lower() in stripped.lower():
                    matches.append(stripped[:200])
                    break
    except Exception:
        pass

    return matches


def _read_tail(path: Path | None, n: int = 20) -> str:
    """Read the last N lines of a file."""
    if not path or not path.exists():
        return "(log file not found)"
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-n:])
    except Exception as exc:
        return f"(error reading log: {exc})"


def _send(message: str) -> None:
    """Send via Telegram if configured, otherwise print to stdout."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    # Always print to stdout for Cowork log capture
    print(message)

    if not token or not chat_id:
        print("(Telegram not configured — message printed to stdout only)")
        return

    try:
        import requests
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message[:4000], "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.status_code == 200:
            print("(Telegram alert sent)")
        else:
            print(f"(Telegram send failed: {resp.status_code})")
    except Exception as exc:
        print(f"(Telegram send error: {exc})")


if __name__ == "__main__":
    main()
