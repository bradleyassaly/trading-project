"""
Circuit breaker — cumulative drawdown protection for the LIVE book.

Tracks live portfolio equity (mark-to-market) and halts ALL trading if
cumulative drawdown from peak equity exceeds a threshold. Requires
explicit human reset to resume. Complements (does not replace)
:class:`KillSwitch`, which handles per-trade gates and same-day loss
limits.

Three protection layers stack — all fed by the LIVE book:

  Layer 1  Per-trade max stake     ← KillSwitch.MAX_TRADE_USD
  Layer 2  Daily loss limit        ← KillSwitch.MAX_DAILY_LOSS_PCT
                                      (realized, live_trades) AND
                                      CircuitBreaker daily_loss_limit
                                      (mark-to-market)
  Layer 3  Cumulative drawdown     ← THIS MODULE

Without Layer 3, a system that loses 5% per day for 10 days passes
every daily check while losing 50% cumulatively. The circuit breaker
catches that scenario by halting on cumulative drawdown from peak.

State feed (2026-07-16 redesign): the ONLY production writer is
``scripts/snapshot_live_equity.py``, which pushes the hourly truth
equity (on-chain USDC + data-api-valued open positions) through
:meth:`update_equity`. It is level-based on purpose — summing booked
``realized_pnl`` rows would inherit every booking bug (unbooked
liquidations, double-booked exits), while an equity level self-corrects
on the next snapshot. Before this redesign the breaker was fed PAPER
resolutions while the live executor consulted the gate: simulated
losses could halt real trading (2026-07-02) and a 14.7% real drawdown
read as 0% (2026-07-16). Paper code must never write breaker state.

Both executors READ the gate via :meth:`can_trade`; a live drawdown
halt quiesces paper entries too ("halts ALL trading").

State lives in two singleton-friendly tables:

  ``circuit_breaker_state``  one row (id=1) with the live state
  ``circuit_breaker_log``    append-only audit trail

Multiple imports of :class:`CircuitBreaker` are safe — every method
reads/writes the same DB row, so there's no in-memory state to keep
in sync.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db import connect_wallet_db

logger = logging.getLogger(__name__)


# Tunable defaults — overridden per-row in circuit_breaker_state
DEFAULT_MAX_DRAWDOWN_PCT = 0.20      # 20% from peak halts trading
DEFAULT_DAILY_LOSS_LIMIT = 0.05      # 5% of current equity
WARNING_DRAWDOWN_PCT = 0.10          # send Telegram warning at 10%


class CircuitBreaker:
    """Cumulative drawdown circuit breaker.

    Singleton-safe via the ``id=1`` row constraint on
    ``circuit_breaker_state`` — multiple instances all read/write the
    same row.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        if db_path is None:
            from trading_platform.polymarket.wallet_db import WalletDB
            db_path = WalletDB.default_path()
        self._db_path = str(db_path)

    # ── Public API ─────────────────────────────────────────────────────────

    def initialize(
        self,
        starting_capital: float,
        max_drawdown_pct: float = DEFAULT_MAX_DRAWDOWN_PCT,
        daily_loss_limit: float = DEFAULT_DAILY_LOSS_LIMIT,
    ) -> dict[str, Any]:
        """Initialize the singleton row. Idempotent — won't overwrite an
        existing initialized state.
        """
        existing = self._read_state()
        if existing:
            return existing
        now = int(time.time())
        conn = connect_wallet_db(self._db_path)
        try:
            # INSERT OR IGNORE — never overwrite a calibrated row.
            # The db_connection rewriter maps INSERT OR REPLACE to
            # ON CONFLICT DO UPDATE which would stomp a live-calibrated
            # starting_capital. Use INSERT OR IGNORE (→ ON CONFLICT DO
            # NOTHING) so a pre-existing row with the real bankroll wins.
            conn.execute(
                """INSERT OR IGNORE INTO circuit_breaker_state
                    (id, starting_capital, peak_equity, current_equity,
                     max_drawdown_pct, current_drawdown_pct, is_halted,
                     daily_pnl, daily_loss_limit, daily_halted, last_updated)
                   VALUES (1, ?, ?, ?, ?, 0.0, 0, 0.0, ?, 0, ?)""",
                (
                    float(starting_capital), float(starting_capital),
                    float(starting_capital), float(max_drawdown_pct),
                    float(daily_loss_limit), now,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        self._log_event(
            "init", equity_before=None, equity_after=starting_capital,
            drawdown_pct=0.0, daily_pnl=0.0,
            details={"max_drawdown_pct": max_drawdown_pct, "daily_loss_limit": daily_loss_limit},
        )
        return self._read_state() or {}

    def rebase(self, starting_capital: float) -> dict[str, Any]:
        """Reset starting_capital / peak_equity / current_equity to the given
        value. Use when the breaker was initialized against a different
        bankroll (e.g. $100k placeholder vs real $10k paper book) and the
        drawdown math is against the wrong anchor. Clears halt state.
        """
        now = int(time.time())
        existing = self._read_state()
        conn = connect_wallet_db(self._db_path)
        try:
            conn.execute(
                """UPDATE circuit_breaker_state
                   SET starting_capital = ?, peak_equity = ?, current_equity = ?,
                       current_drawdown_pct = 0.0, is_halted = 0,
                       halted_at = NULL, halted_reason = NULL,
                       daily_pnl = 0.0, daily_halted = 0, daily_reset_at = ?,
                       last_reset_at = ?, last_reset_by = 'rebase',
                       last_updated = ?
                   WHERE id = 1""",
                (float(starting_capital), float(starting_capital),
                 float(starting_capital), now, now, now),
            )
            conn.commit()
        finally:
            conn.close()
        self._log_event(
            "rebase",
            equity_before=float(existing.get("current_equity")) if existing else None,
            equity_after=starting_capital,
            drawdown_pct=0.0, daily_pnl=0.0,
            details={"prev_starting_capital": existing.get("starting_capital") if existing else None},
        )
        return self._read_state() or {}

    def can_trade(self) -> tuple[bool, str]:
        """Return ``(allowed, reason)``. Used as the pre-trade gate."""
        s = self._read_state()
        if not s:
            return False, "circuit breaker not initialized"
        if s["is_halted"]:
            reason = s.get("halted_reason") or "max drawdown halt active"
            return False, f"halted: {reason}"
        if s["daily_halted"]:
            return False, f"daily loss limit hit (resumes at midnight)"
        if (s.get("current_equity") or 0) <= 0:
            return False, "bankrupt"
        return True, "ok"

    def update_equity(
        self,
        total_equity: float,
        source: str = "live_equity_snapshot",
    ) -> dict[str, Any]:
        """Level-based mark-to-market sync — THE live state feed.

        Called by ``scripts/snapshot_live_equity.py`` with the truth
        equity (on-chain USDC + data-api-valued open positions). Seeds
        the state row on first-ever call. Peak/drawdown/halt logic
        applies to the implied delta vs the stored equity; the delta
        counts toward ``daily_pnl`` only when the PREVIOUS sync happened
        inside the current daily window (a first sync or a post-gap sync
        carries prior days' losses — cumulative drawdown still registers,
        but no false daily halt).

        Caveat: external cash flows (deposits/withdrawals) read as P&L
        here — call :meth:`rebase` after moving money in or out.
        """
        eq = float(total_equity)
        now = int(time.time())
        s = self._read_state()
        if not s:
            state = self.initialize(starting_capital=eq)
            self._stamp_equity_sync(now)
            return state

        prev_sync = self._read_last_equity_sync()
        delta = eq - float(s["current_equity"])
        if abs(delta) < 0.005:
            # No material move — refresh timestamps for staleness
            # monitors, but skip the log so hourly no-op syncs don't
            # drown recent_log.
            conn = connect_wallet_db(self._db_path)
            try:
                conn.execute(
                    "UPDATE circuit_breaker_state SET last_updated = ? WHERE id = 1",
                    (now,),
                )
                conn.commit()
            finally:
                conn.close()
            self._stamp_equity_sync(now)
            return self._read_state() or {}

        # Daily attribution: the delta is "today's P&L" only if the
        # PREVIOUS sync happened inside the current daily window. A first
        # sync (or one after a long gap) carries losses that economically
        # belong to prior days — booking them as today's would fire a
        # false daily halt (the 2026-07-02 stale-loss halt, again). The
        # cumulative drawdown check is day-agnostic and always applies.
        count_daily = (
            prev_sync is not None
            and prev_sync >= int(s.get("daily_reset_at") or 0)
        )
        state = self.record_trade(
            pnl_dollars=delta,
            trade_details={"kind": "equity_sync", "source": source, "equity": eq},
            event_type="equity_sync",
            count_toward_daily=count_daily,
        )
        self._stamp_equity_sync(now)
        return self._read_state() or state

    def record_trade(
        self,
        pnl_dollars: float,
        trade_details: dict[str, Any] | None = None,
        event_type: str = "trade",
        count_toward_daily: bool = True,
    ) -> dict[str, Any]:
        """Apply a P&L delta to the circuit breaker state.

        Updates current_equity, peak_equity (if new peak), daily_pnl,
        and current_drawdown_pct. Triggers daily / drawdown halts when
        thresholds are crossed and dispatches Telegram alerts.

        Production feeds go through :meth:`update_equity` (which calls
        this with ``event_type="equity_sync"``) — do not wire paper
        P&L into this method.
        """
        s = self._read_state()
        if not s:
            logger.debug("[CB] record_trade called before initialize()")
            return {}

        equity_before = float(s["current_equity"])
        equity_after = equity_before + float(pnl_dollars or 0)
        peak_before = float(s["peak_equity"])
        peak_after = max(peak_before, equity_after)
        peak_raised = peak_after > peak_before

        if peak_after > 0:
            drawdown_pct = max(0.0, (peak_after - equity_after) / peak_after)
        else:
            drawdown_pct = 0.0

        daily_pnl = float(s["daily_pnl"])
        if count_toward_daily:
            daily_pnl += float(pnl_dollars or 0)
        max_dd = float(s["max_drawdown_pct"])
        daily_limit = float(s["daily_loss_limit"])

        # Threshold checks
        new_halt = False
        new_halt_reason: str | None = None
        new_daily_halt = False

        if not s["is_halted"] and drawdown_pct >= max_dd:
            new_halt = True
            new_halt_reason = (
                f"drawdown {drawdown_pct:.1%} >= max {max_dd:.1%}"
            )

        # Daily loss check — uses CURRENT equity (after this trade) as
        # the base, so a 5% daily limit on a $10k portfolio = $500.
        # Compute against the equity at the START of today, which we
        # approximate as current_equity - daily_pnl.
        equity_at_day_start = equity_after - daily_pnl
        if count_toward_daily and equity_at_day_start > 0 and daily_pnl < 0:
            daily_loss_frac = abs(daily_pnl) / equity_at_day_start
            if daily_loss_frac >= daily_limit and not s["daily_halted"]:
                new_daily_halt = True

        now = int(time.time())
        # Final values computed in Python, written as plain params. The
        # previous CASE WHEN ?-with-int-param form raised DatatypeMismatch
        # under Postgres ("CASE/WHEN must be type boolean, not smallint"),
        # so EVERY record_trade write silently failed post-cutover — the
        # breaker froze at its rebase anchor while callers swallowed the
        # exception (found 2026-07-16).
        is_halted_final = bool(new_halt or s["is_halted"])
        halted_at_final = s.get("halted_at")
        if new_halt and not halted_at_final:
            halted_at_final = now
        halted_reason_final = new_halt_reason if new_halt else s.get("halted_reason")
        daily_halted_final = bool(new_daily_halt or s["daily_halted"])
        conn = connect_wallet_db(self._db_path)
        try:
            conn.execute(
                """UPDATE circuit_breaker_state SET
                    current_equity = ?,
                    peak_equity = ?,
                    current_drawdown_pct = ?,
                    daily_pnl = ?,
                    is_halted = ?,
                    halted_at = ?,
                    halted_reason = ?,
                    daily_halted = ?,
                    last_updated = ?
                   WHERE id = 1""",
                (
                    equity_after, peak_after, drawdown_pct, daily_pnl,
                    int(is_halted_final), halted_at_final, halted_reason_final,
                    int(daily_halted_final), now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        # Audit log
        log_event = event_type
        if new_halt:
            log_event = "halt"
        elif new_daily_halt:
            log_event = "daily_halt"
        elif peak_raised:
            log_event = "peak_update"

        self._log_event(
            log_event,
            equity_before=equity_before,
            equity_after=equity_after,
            drawdown_pct=drawdown_pct,
            daily_pnl=daily_pnl,
            details={**(trade_details or {}), "pnl_dollars": pnl_dollars},
        )

        # Telegram dispatch
        try:
            self._dispatch_alerts(
                drawdown_pct=drawdown_pct,
                equity_after=equity_after,
                peak_after=peak_after,
                daily_pnl=daily_pnl,
                max_dd=max_dd,
                daily_limit=daily_limit,
                new_halt=new_halt,
                new_halt_reason=new_halt_reason,
                new_daily_halt=new_daily_halt,
                prev_drawdown_pct=float(s["current_drawdown_pct"]),
            )
        except Exception as exc:
            logger.debug("[CB] alert dispatch failed: %s", exc)

        return self._read_state() or {}

    def reset_daily(self) -> dict[str, Any]:
        """Midnight reset — clears daily_pnl and daily_halted."""
        now = int(time.time())
        conn = connect_wallet_db(self._db_path)
        try:
            conn.execute(
                """UPDATE circuit_breaker_state
                   SET daily_pnl = 0.0, daily_halted = 0,
                       daily_reset_at = ?, last_updated = ?
                   WHERE id = 1""",
                (now, now),
            )
            conn.commit()
        finally:
            conn.close()
        self._log_event("daily_reset", equity_before=None, equity_after=None,
                        drawdown_pct=None, daily_pnl=0.0, details={})
        return self._read_state() or {}

    def reset_drawdown(
        self,
        reset_by: str = "manual",
        reset_peak: bool = False,
    ) -> dict[str, Any]:
        """Manually clear the max-drawdown halt.

        Does NOT reset peak_equity unless ``reset_peak=True``. The
        operator must accept the new lower equity as the starting
        point, add capital, or lower the threshold — otherwise the
        next losing trade will re-halt immediately.
        """
        s = self._read_state()
        if not s:
            return {}
        now = int(time.time())
        new_peak = float(s["current_equity"]) if reset_peak else float(s["peak_equity"])
        new_dd = 0.0 if reset_peak else float(s["current_drawdown_pct"])
        conn = connect_wallet_db(self._db_path)
        try:
            conn.execute(
                """UPDATE circuit_breaker_state SET
                    is_halted = 0,
                    halted_at = NULL,
                    halted_reason = NULL,
                    peak_equity = ?,
                    current_drawdown_pct = ?,
                    last_reset_at = ?,
                    last_reset_by = ?,
                    last_updated = ?
                   WHERE id = 1""",
                (new_peak, new_dd, now, reset_by, now),
            )
            conn.commit()
        finally:
            conn.close()
        self._log_event(
            "reset",
            equity_before=float(s["current_equity"]),
            equity_after=float(s["current_equity"]),
            drawdown_pct=new_dd,
            daily_pnl=float(s["daily_pnl"]),
            details={"reset_by": reset_by, "reset_peak": reset_peak},
        )
        # Telegram
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            alerter = get_alerter()
            if alerter.enabled:
                msg = (
                    f"\U0001f7e2 <b>CIRCUIT BREAKER RESET</b>\n"
                    f"Reset by: {reset_by}\n"
                    f"Equity: ${s['current_equity']:,.0f}\n"
                    f"Peak: ${new_peak:,.0f}{'  (RESET)' if reset_peak else ''}\n"
                    f"Drawdown: {new_dd*100:.1f}%\n"
                    f"\n\u2500\u2500\u2500\u2500\u2500\u2500\n\U0001f5a5 localhost:5173/live"
                )
                alerter._send(msg, disable_notification=False)
        except Exception:
            pass
        return self._read_state() or {}

    def configure(
        self,
        max_drawdown_pct: float | None = None,
        daily_loss_limit: float | None = None,
    ) -> dict[str, Any]:
        """Update thresholds. Refused while halted to prevent operators
        from quietly raising the limit instead of investigating."""
        s = self._read_state()
        if not s:
            return {}
        if s["is_halted"]:
            return {**s, "error": "cannot reconfigure while halted; reset first"}
        now = int(time.time())
        new_max = float(max_drawdown_pct) if max_drawdown_pct is not None else float(s["max_drawdown_pct"])
        new_daily = float(daily_loss_limit) if daily_loss_limit is not None else float(s["daily_loss_limit"])
        conn = connect_wallet_db(self._db_path)
        try:
            conn.execute(
                """UPDATE circuit_breaker_state
                   SET max_drawdown_pct = ?, daily_loss_limit = ?, last_updated = ?
                   WHERE id = 1""",
                (new_max, new_daily, now),
            )
            conn.commit()
        finally:
            conn.close()
        self._log_event(
            "configure", equity_before=None, equity_after=None,
            drawdown_pct=None, daily_pnl=None,
            details={"max_drawdown_pct": new_max, "daily_loss_limit": new_daily},
        )
        return self._read_state() or {}

    def get_status(self) -> dict[str, Any]:
        """Full state for the dashboard / API."""
        s = self._read_state()
        if not s:
            return {"available": False, "reason": "not initialized"}
        peak = float(s["peak_equity"])
        max_dd = float(s["max_drawdown_pct"])
        cur_dd = float(s["current_drawdown_pct"])
        # Headroom: how much more drawdown can we tolerate before halting?
        headroom_pct = max(0.0, max_dd - cur_dd)
        headroom_dollars = peak * headroom_pct
        # Recent log
        try:
            conn = connect_wallet_db(self._db_path)
            try:
                rows = conn.execute(
                    """SELECT event_type, equity_before, equity_after,
                              drawdown_pct, daily_pnl, details, created_at
                       FROM circuit_breaker_log
                       ORDER BY created_at DESC LIMIT 10"""
                ).fetchall()
            finally:
                conn.close()
            recent_log = [
                {
                    "event_type": r[0], "equity_before": r[1], "equity_after": r[2],
                    "drawdown_pct": r[3], "daily_pnl": r[4],
                    "details": _try_json(r[5]), "created_at": r[6],
                }
                for r in rows
            ]
        except Exception:
            recent_log = []
        return {
            "available": True,
            **s,
            "headroom_pct": round(headroom_pct, 4),
            "headroom_dollars": round(headroom_dollars, 2),
            "recent_log": recent_log,
        }

    def get_log(self, limit: int = 50) -> list[dict[str, Any]]:
        try:
            conn = connect_wallet_db(self._db_path)
            try:
                rows = conn.execute(
                    """SELECT id, event_type, equity_before, equity_after,
                              drawdown_pct, daily_pnl, details, created_at
                       FROM circuit_breaker_log
                       ORDER BY created_at DESC LIMIT ?""",
                    (int(limit),),
                ).fetchall()
            finally:
                conn.close()
        except Exception:
            return []
        return [
            {
                "id": r[0], "event_type": r[1],
                "equity_before": r[2], "equity_after": r[3],
                "drawdown_pct": r[4], "daily_pnl": r[5],
                "details": _try_json(r[6]), "created_at": r[7],
            }
            for r in rows
        ]

    # ── Internal helpers ───────────────────────────────────────────────────

    def _stamp_equity_sync(self, ts: int) -> None:
        """Record when the live equity feed last ran. Kept OUT of
        _read_state so a missing column can never brick can_trade();
        the column is lazy-added here (idempotent, both backends)."""
        try:
            # 2026-08-02: the lazy-add was a bare ALTER in try/except, so
            # every stamp took ACCESS EXCLUSIVE just to fail. ensure_columns
            # checks the catalog and caches the answer for the process.
            from trading_platform.polymarket.db_connection import ensure_columns
            ensure_columns("circuit_breaker_state",
                           [("last_equity_sync_at", "INTEGER")],
                           db_path=self._db_path)
            conn = connect_wallet_db(self._db_path)
            try:
                conn.execute(
                    "UPDATE circuit_breaker_state SET last_equity_sync_at = ? WHERE id = 1",
                    (int(ts),),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("[CB] equity-sync stamp failed: %s", exc)

    def _read_last_equity_sync(self) -> int | None:
        try:
            conn = connect_wallet_db(self._db_path)
            try:
                row = conn.execute(
                    "SELECT last_equity_sync_at FROM circuit_breaker_state WHERE id = 1"
                ).fetchone()
            finally:
                conn.close()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _read_state(self) -> dict[str, Any] | None:
        try:
            conn = connect_wallet_db(self._db_path)
            try:
                row = conn.execute(
                    """SELECT starting_capital, peak_equity, current_equity,
                              max_drawdown_pct, current_drawdown_pct,
                              is_halted, halted_at, halted_reason,
                              last_reset_at, last_reset_by, last_updated,
                              daily_pnl, daily_loss_limit, daily_halted,
                              daily_reset_at
                       FROM circuit_breaker_state WHERE id = 1"""
                ).fetchone()
            finally:
                conn.close()
        except Exception:
            return None
        if not row:
            return None
        keys = [
            "starting_capital", "peak_equity", "current_equity",
            "max_drawdown_pct", "current_drawdown_pct",
            "is_halted", "halted_at", "halted_reason",
            "last_reset_at", "last_reset_by", "last_updated",
            "daily_pnl", "daily_loss_limit", "daily_halted",
            "daily_reset_at",
        ]
        out = dict(zip(keys, row))
        out["is_halted"] = bool(out["is_halted"])
        out["daily_halted"] = bool(out["daily_halted"])
        return out

    def _log_event(
        self,
        event_type: str,
        *,
        equity_before: float | None,
        equity_after: float | None,
        drawdown_pct: float | None,
        daily_pnl: float | None,
        details: dict[str, Any] | None,
    ) -> None:
        try:
            conn = connect_wallet_db(self._db_path)
            try:
                conn.execute(
                    """INSERT INTO circuit_breaker_log
                        (event_type, equity_before, equity_after, drawdown_pct,
                         daily_pnl, details, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event_type, equity_before, equity_after,
                        drawdown_pct, daily_pnl,
                        json.dumps(details or {}, default=str),
                        int(time.time()),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("[CB] log write failed: %s", exc)

    def _dispatch_alerts(
        self,
        *,
        drawdown_pct: float,
        equity_after: float,
        peak_after: float,
        daily_pnl: float,
        max_dd: float,
        daily_limit: float,
        new_halt: bool,
        new_halt_reason: str | None,
        new_daily_halt: bool,
        prev_drawdown_pct: float,
    ) -> None:
        # Route through AlertManager so the same dedup / rate-limit
        # state applies to circuit-breaker alerts as everything else.
        try:
            from trading_platform.polymarket.alert_manager import get_alert_manager
            am = get_alert_manager()
        except Exception:
            am = None

        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if not alerter.enabled and am is None:
            return
        headroom_pct = max(0.0, max_dd - drawdown_pct)
        headroom_dollars = peak_after * headroom_pct
        footer = "\n\u2500\u2500\u2500\u2500\u2500\u2500\n\U0001f5a5 localhost:5173/live"

        if new_halt:
            if am is not None:
                am.alert_circuit_breaker_triggered(
                    equity=float(equity_after),
                    peak=float(peak_after),
                    drawdown_pct=float(drawdown_pct),
                    reason=new_halt_reason or "max drawdown exceeded",
                )
                return
            # Fallback path if AlertManager is unavailable for any reason.
            msg = (
                f"\U0001f6d1 <b>CIRCUIT BREAKER TRIGGERED \u2014 ALL TRADING HALTED</b>\n"
                f"Equity: ${equity_after:,.0f} | Peak: ${peak_after:,.0f}\n"
                f"Drawdown: {drawdown_pct*100:.1f}% | Threshold: {max_dd*100:.1f}%\n"
                f"Daily P&L: ${daily_pnl:,.0f}\n\n"
                f"No trades will execute until manual reset.\n"
                f"<code>POST /api/circuit-breaker/reset {{confirm: true}}</code>"
                f"{footer}"
            )
            alerter._send(msg, disable_notification=False)
            return

        if new_daily_halt:
            if am is not None:
                am.alert_daily_loss_limit(
                    daily_pnl=float(daily_pnl),
                    limit=float(daily_limit) * float(peak_after),
                    equity=float(equity_after),
                )
                return
            msg = (
                f"\u26a0\ufe0f <b>DAILY LOSS LIMIT HIT \u2014 trading paused until midnight</b>\n"
                f"Daily P&L: ${daily_pnl:,.0f}\n"
                f"Equity: ${equity_after:,.0f} | Drawdown: {drawdown_pct*100:.1f}%\n"
                f"Will auto-resume at midnight."
                f"{footer}"
            )
            alerter._send(msg, disable_notification=False)
            return

        # Warning band — fires once when crossing the warning threshold
        if (
            drawdown_pct >= WARNING_DRAWDOWN_PCT
            and prev_drawdown_pct < WARNING_DRAWDOWN_PCT
            and drawdown_pct < max_dd
        ):
            msg = (
                f"\u26a0\ufe0f <b>DRAWDOWN WARNING</b>\n"
                f"Equity: ${equity_after:,.0f} | Peak: ${peak_after:,.0f}\n"
                f"Drawdown: {drawdown_pct*100:.1f}%\n"
                f"Daily P&L: ${daily_pnl:,.0f} | Headroom: ${headroom_dollars:,.0f} ({headroom_pct*100:.1f}%)\n"
                f"Trading continues but approaching halt threshold."
                f"{footer}"
            )
            alerter._send(msg, disable_notification=True)


def _try_json(s: str | None) -> Any:
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return s
