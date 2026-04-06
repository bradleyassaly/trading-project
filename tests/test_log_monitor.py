"""Tests for scripts/log_monitor.py"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "log_monitor.py"


def _run(args: list[str]) -> subprocess.CompletedProcess:
    env = {**os.environ, "TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": "",
           "PYTHONIOENCODING": "utf-8"}
    return subprocess.run(
        [sys.executable, str(SCRIPT)] + args,
        capture_output=True, text=True, env=env, timeout=10,
    )


class TestLogMonitor:
    def test_failure_sends_alert(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("line 1\nline 2\nsome error happened\n")
        result = _run(["--task", "test_task", "--log", str(log), "--status", "1"])
        assert "TASK FAILED: test_task" in result.stdout
        assert "Exit code: 1" in result.stdout
        assert "some error happened" in result.stdout

    def test_warning_pattern_match(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("Starting sync...\nnew_trades: 0, skipped: 100\nDone.\n")
        result = _run(["--task", "wallet_trade_sync", "--log", str(log), "--status", "0"])
        assert "TASK WARNING: wallet_trade_sync" in result.stdout
        assert "new_trades: 0" in result.stdout

    def test_clean_success_no_alert(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("Starting sync...\n50 trades synced\nDone.\n")
        result = _run(["--task", "wallet_trade_sync", "--log", str(log), "--status", "0"])
        assert "OK: wallet_trade_sync completed cleanly" in result.stdout
        assert "FAILED" not in result.stdout
        assert "WARNING" not in result.stdout

    def test_generic_error_pattern(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("Processing...\nERROR: something broke\nDone.\n")
        result = _run(["--task", "unknown_task", "--log", str(log), "--status", "0"])
        assert "WARNING" in result.stdout
        assert "ERROR: something broke" in result.stdout

    def test_missing_log_file(self, tmp_path: Path) -> None:
        result = _run(["--task", "test_task", "--log", str(tmp_path / "missing.log"), "--status", "1"])
        assert "TASK FAILED" in result.stdout
        assert "log file not found" in result.stdout

    def test_telegram_not_configured(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("error\n")
        result = _run(["--task", "t", "--log", str(log), "--status", "1"])
        assert "Telegram not configured" in result.stdout

    def test_summary_mode(self) -> None:
        result = _run(["--summary"])
        assert "Daily Task Summary" in result.stdout

    def test_traceback_pattern(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("Running...\nTraceback (most recent call last):\n  File...\nDone.\n")
        result = _run(["--task", "compute_signals", "--log", str(log), "--status", "0"])
        assert "WARNING" in result.stdout

    def test_429_rate_limit_warning(self, tmp_path: Path) -> None:
        log = tmp_path / "test.log"
        log.write_text("Syncing...\nHTTP 429 rate limited\n50 trades synced\nDone.\n")
        result = _run(["--task", "wallet_trade_sync", "--log", str(log), "--status", "0"])
        assert "WARNING" in result.stdout
        assert "429" in result.stdout
