"""
Multi-layer safety system for live autonomous trading.

Every live trade MUST pass through :meth:`KillSwitch.check` before any
CLOB order is submitted. The kill switch reads from
``polymarket_paper_trades`` (the ground truth for resolved-trade
performance) and applies hard limits that block trading when any
condition fails.

Hard limits (any one blocks the trade):
  * ``POLYMARKET_LIVE_ENABLED=1`` env var must be set explicitly
  * Emergency stop file (``data/KILL_SWITCH_ACTIVE``) must not exist
  * Daily loss must not exceed ``MAX_DAILY_LOSS_PCT`` of bankroll
  * Open live position count must be below ``MAX_OPEN_POSITIONS``
  * Trade size must be below ``MAX_TRADE_USD``
  * Signal must have at least ``MIN_RESOLVED_HARD`` resolved outcomes
  * Signal EV (avg return %) must be positive
  * Signal win rate must be above ``MIN_WIN_RATE`` once 20+ resolved

Soft limits log a warning but allow the trade:
  * < ``PREFERRED_MIN_RESOLVED`` outcomes
  * Negative profit factor with small sample
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_KILL_SWITCH_FILE = _PROJECT_ROOT / "data" / "KILL_SWITCH_ACTIVE"


@dataclass
class KillSwitchResult:
    allowed: bool
    reason: str | None = None
    warnings: list[str] = field(default_factory=list)


class KillSwitch:
    """Multi-layer pre-trade safety check."""

    MAX_DAILY_LOSS_PCT = 0.10
    MAX_OPEN_POSITIONS = 10
    MAX_TRADE_USD = 25  # week 1 hard cap
    MIN_WIN_RATE = 0.52
    MIN_RESOLVED_HARD = 15
    PREFERRED_MIN_RESOLVED = 30

    def __init__(self, db_path: str, bankroll: float = 350) -> None:
        self._db_path = str(db_path)
        self.BANKROLL = bankroll

    # ── Per-trade gate ─────────────────────────────────────────────────────

    def check(
        self,
        signal_type: str,
        trade_size_usd: float,
        confidence: float,
    ) -> KillSwitchResult:
        warnings: list[str] = []

        # 1. Master switch
        if os.getenv("POLYMARKET_LIVE_ENABLED", "").lower() not in ("1", "true", "yes"):
            return KillSwitchResult(
                False,
                "POLYMARKET_LIVE_ENABLED not set — set to 1 in .env to enable live trading",
                warnings,
            )

        # 2. Emergency stop file
        stopped, stop_reason = self.is_emergency_stopped()
        if stopped:
            return KillSwitchResult(False, f"Emergency stop active: {stop_reason}", warnings)

        # 3. Trade size cap
        if trade_size_usd > self.MAX_TRADE_USD:
            return KillSwitchResult(
                False,
                f"Trade size ${trade_size_usd:.0f} exceeds MAX_TRADE_USD ${self.MAX_TRADE_USD}",
                warnings,
            )

        # 4-7: read paper-trade history
        try:
            conn = sqlite3.connect(self._db_path)
        except Exception as exc:
            return KillSwitchResult(False, f"DB unavailable: {exc}", warnings)

        try:
            row = conn.execute(
                """SELECT COUNT(*) AS n,
                          AVG(return_pct) AS avg_return_pct,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins
                   FROM polymarket_paper_trades
                   WHERE signal_type = ? AND archived = 0
                     AND exit_ts IS NOT NULL""",
                (signal_type,),
            ).fetchone()
            n_resolved = (row[0] or 0) if row else 0
            avg_return_pct = (row[1] or 0.0) if row else 0.0
            wins = (row[2] or 0) if row else 0

            if n_resolved < self.MIN_RESOLVED_HARD:
                return KillSwitchResult(
                    False,
                    f"{signal_type}: only {n_resolved} resolved trades, need {self.MIN_RESOLVED_HARD}",
                    warnings,
                )
            if n_resolved < self.PREFERRED_MIN_RESOLVED:
                warnings.append(
                    f"Below preferred sample size ({n_resolved}/{self.PREFERRED_MIN_RESOLVED})"
                )

            ev = avg_return_pct / 100.0  # convert pct to fraction
            if ev <= 0:
                return KillSwitchResult(
                    False,
                    f"{signal_type}: EV={ev:.3f} not positive — no measured edge",
                    warnings,
                )

            wr = wins / n_resolved if n_resolved > 0 else 0
            if n_resolved >= 20 and wr < self.MIN_WIN_RATE:
                return KillSwitchResult(
                    False,
                    f"{signal_type}: win rate {wr:.0%} below minimum {self.MIN_WIN_RATE:.0%}",
                    warnings,
                )

            # 6. Daily loss limit
            today_start = int(time.time()) - 86400
            daily_pnl = conn.execute(
                """SELECT COALESCE(SUM(realized_pnl), 0) FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts >= ? AND realized_pnl IS NOT NULL""",
                (today_start,),
            ).fetchone()[0] or 0.0
            daily_loss_pct = abs(min(0.0, daily_pnl)) / self.BANKROLL
            if daily_loss_pct >= self.MAX_DAILY_LOSS_PCT:
                return KillSwitchResult(
                    False,
                    f"Daily loss limit hit: {daily_loss_pct:.1%} (max {self.MAX_DAILY_LOSS_PCT:.0%})",
                    warnings,
                )

            # 7. Open live position count (live_trades table — created lazily)
            try:
                live_open = conn.execute(
                    """SELECT COUNT(*) FROM live_trades
                       WHERE dry_run = 0 AND status IN ('submitted','live','matched')"""
                ).fetchone()[0]
            except sqlite3.OperationalError:
                live_open = 0
            if live_open >= self.MAX_OPEN_POSITIONS:
                return KillSwitchResult(
                    False,
                    f"Max open live positions reached ({live_open}/{self.MAX_OPEN_POSITIONS})",
                    warnings,
                )

            return KillSwitchResult(True, None, warnings)
        finally:
            conn.close()

    # ── Emergency stop ─────────────────────────────────────────────────────

    def emergency_stop(self, reason: str = "manual") -> None:
        """Write the kill-switch flag file. Live executor checks this on every trade."""
        _KILL_SWITCH_FILE.parent.mkdir(parents=True, exist_ok=True)
        _KILL_SWITCH_FILE.write_text(f"{int(time.time())} {reason}\n", encoding="utf-8")
        logger.warning("[KILL_SWITCH] EMERGENCY STOP ACTIVATED: %s", reason)

    def is_emergency_stopped(self) -> tuple[bool, str]:
        if not _KILL_SWITCH_FILE.exists():
            return False, ""
        try:
            return True, _KILL_SWITCH_FILE.read_text(encoding="utf-8").strip()
        except Exception:
            return True, "unreadable"

    def clear_emergency_stop(self) -> None:
        try:
            _KILL_SWITCH_FILE.unlink()
            logger.warning("[KILL_SWITCH] Emergency stop cleared")
        except FileNotFoundError:
            pass
