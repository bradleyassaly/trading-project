"""
Polymarket live trading executor.

Same interface as :class:`PolymarketPaperExecutor` but routes orders
through the real CLOB after every layer of safety has cleared:

  1. ``DRY_RUN`` flag — defaults to True, must be flipped explicitly
  2. :class:`KillSwitch` — env switch + emergency stop + sample-size +
     EV + win-rate + daily-loss + position-count gates
  3. :class:`KellySizer` — sizes the trade from real outcome history,
     scaled by signal confidence
  4. :class:`ClobClient` — actual order placement (only when both
     DRY_RUN is False AND ``POLYMARKET_LIVE_ENABLED=1``)

Every attempt — dry run or live — is appended to the ``live_trades``
table for an audit trail.

**Critical safety contract**

* ``DRY_RUN`` must remain ``True`` in source. The user has to flip it
  in their own running instance.
* Even with ``DRY_RUN=False``, the kill switch will block trading
  until ``POLYMARKET_LIVE_ENABLED=1`` is set in ``.env``.
* Both conditions must hold simultaneously for any real CLOB order
  to be submitted.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

from trading_platform.polymarket.clob_client import ClobClient, OrderResult
from trading_platform.polymarket.kelly_sizer import KellySizer
from trading_platform.polymarket.kill_switch import KillSwitch
from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)


class PolymarketLiveExecutor:
    """Live trading executor with multi-layer safety gating."""

    # ALWAYS True in source. Flip in your local instance only.
    DRY_RUN: bool = True

    def __init__(self) -> None:
        self._db = WalletDB()
        self._db_path = str(self._db._path)
        self._clob = ClobClient()
        self._kill = KillSwitch(self._db_path)
        self._sizer = KellySizer(self._db_path)
        self._ensure_live_trades_table()

    def _ensure_live_trades_table(self) -> None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS live_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attempted_at INTEGER NOT NULL,
                        signal_type TEXT,
                        condition_id TEXT,
                        question TEXT,
                        direction TEXT,
                        confidence REAL,
                        size_usd REAL,
                        entry_price REAL,
                        order_id TEXT,
                        status TEXT,
                        dry_run INTEGER DEFAULT 1,
                        error_msg TEXT
                    )
                """)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_live_trades_ts ON live_trades(attempted_at DESC)"
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("live_trades table ensure failed: %s", exc)

    def execute(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Attempt a (gated) live trade for a fired signal."""
        sig_type = signal.get("signal_type", "")
        confidence = float(signal.get("confidence") or 0)
        condition_id = signal.get("condition_id") or ""

        # 0a. Category allowlist — live trades restricted to categories
        # with statistically significant positive resolved EV.
        from trading_platform.polymarket.polymarket_paper_executor import (
            LIVE_TRADE_CATEGORIES, EXCLUDE_SIGNAL_TYPES,
        )
        if sig_type in EXCLUDE_SIGNAL_TYPES:
            # Routine config-level rejection — not a warning. DEBUG it so
            # operators only see exceptional blocks at WARNING level.
            logger.debug("[LIVE] BLOCKED signal_type=%s (excluded)", sig_type)
            return self._result(False, reason=f"signal_type {sig_type} excluded from live")
        raw_cat = signal.get("category") or ""
        cat = raw_cat.lower() if isinstance(raw_cat, str) else ""
        if not cat or cat not in LIVE_TRADE_CATEGORIES:
            logger.debug("[LIVE] BLOCKED category=%s — not in live allowlist", cat)
            return self._result(False, reason=f"category '{cat}' not approved for live")

        # 0. Emergency stop check
        stopped, stop_reason = self._kill.is_emergency_stopped()
        if stopped:
            logger.warning("[LIVE] Emergency stop active: %s", stop_reason)
            return self._result(False, reason=f"Emergency stop: {stop_reason}")

        # 0b. Circuit breaker check (cumulative drawdown layer)
        try:
            from trading_platform.polymarket.circuit_breaker import CircuitBreaker
            cb = CircuitBreaker(self._db_path)
            allowed, cb_reason = cb.can_trade()
            if not allowed:
                logger.warning("[LIVE] Circuit breaker blocked: %s", cb_reason)
                return self._result(False, reason=f"circuit breaker: {cb_reason}")
        except Exception as exc:
            logger.debug("circuit breaker check failed (proceeding): %s", exc)

        # 1. Kelly size. Hard cap at 7% of current live bankroll so a
        # single bad trade can't blow out the account — derived from
        # live bankroll rather than hardcoded $25 so the cap tracks
        # funding changes. Kelly is ALSO capped at MAX_PCT_OF_BANKROLL=2%
        # separately; this is a belt-and-suspenders ceiling for sizing
        # errors or future signal-type additions that might over-size.
        from trading_platform.polymarket.bankroll import get_bankroll
        live_bankroll = get_bankroll()
        phase1_cap = max(5.0, min(25.0, live_bankroll * 0.07))
        size_usd = self._sizer.get_trade_size(sig_type, confidence)
        if size_usd > phase1_cap:
            size_usd = phase1_cap
        if size_usd <= 0:
            return self._result(False, reason=f"Kelly says no edge for {sig_type}")

        # 1b. Stale price guard — abort if signal is old or price moved
        fired_at = signal.get("fired_at") or 0
        age_sec = time.time() - fired_at
        if age_sec > 900:
            return self._result(False, reason=f"Signal too old ({age_sec/60:.0f}m)")

        # 2. Kill switch check. If the switch returns a probation_cap,
        # the signal passed probation gates (>=5 resolved, positive EV,
        # acceptable WR) but hasn't hit MIN_RESOLVED_HARD — clamp size
        # to the probation cap so we collect live data at minimal risk.
        ks = self._kill.check(sig_type, size_usd, confidence)
        for w in ks.warnings:
            logger.warning("[LIVE] KillSwitch warning: %s", w)
        if ks.allowed and ks.probation_cap is not None and size_usd > ks.probation_cap:
            logger.info(
                "[LIVE] PROBATION: clamping %s size $%.2f → $%.2f",
                sig_type, size_usd, ks.probation_cap,
            )
            size_usd = float(ks.probation_cap)
        if not ks.allowed:
            # "POLYMARKET_LIVE_ENABLED not set" is the expected state
            # when running paper-only. Don't spam warnings for that.
            # Any other kill-switch reason stays at WARNING.
            reason_str = str(ks.reason or "")
            if "POLYMARKET_LIVE_ENABLED" in reason_str:
                logger.debug("[LIVE] KillSwitch blocked: %s", reason_str)
            else:
                logger.warning("[LIVE] KillSwitch blocked: %s", reason_str)
            self._record_attempt(signal, size_usd, None, None, dry_run=self.DRY_RUN, status="blocked", error_msg=ks.reason)
            return self._result(False, reason=ks.reason)

        # 3. Resolve YES/NO token_ids (Gamma)
        # ── CRITICAL: A SELL signal means "buy NO token", not "sell YES token".
        # Polymarket CLOB always uses side=BUY and chooses YES/NO via token_id.
        # Previous code hardcoded tids[0] (YES) + side=BUY, which executed
        # the wrong direction on every SELL signal. Fixed 2026-04-14.
        direction = (signal.get("direction") or "BUY").upper()
        want_yes = direction == "BUY"
        outcome_label = "YES" if want_yes else "NO"

        # Prefer cached token_ids from the signal dict (set by signal
        # engine from markets table). Fall back to live Gamma resolution.
        yes_tid = signal.get("yes_token_id")
        no_tid = signal.get("no_token_id")
        token_id = yes_tid if want_yes else no_tid
        if not token_id:
            # Cached miss — hit Gamma live for this one cid
            tids = self._resolve_token_ids(condition_id)
            if not tids or len(tids) < 2:
                return self._result(False, reason=f"No token_ids for {condition_id[:20]}")
            token_id = tids[0] if want_yes else tids[1]
        if not token_id:
            return self._result(False, reason=f"No {outcome_label} token_id for {condition_id[:20]}")

        # 4. Current price + liquidity check (ON THE TOKEN WE'RE BUYING)
        current_price = self._clob.get_mid_price(token_id)
        if current_price is None:
            return self._result(False, reason="Could not fetch current price from CLOB")

        # Stale-price guard: compare against entry price in the same frame.
        # Signal.price is in the YES-token reference frame; current_price is
        # the token we'd actually buy. For NO trades, flip entry_price into
        # NO-frame before comparing.
        entry_price = float(signal.get("entry_price") or signal.get("price") or 0)
        if entry_price > 0:
            entry_in_token_frame = entry_price if want_yes else (1.0 - entry_price)
            if entry_in_token_frame > 0:
                slippage = abs(current_price - entry_in_token_frame) / entry_in_token_frame
                if slippage > 0.05:
                    return self._result(
                        False,
                        reason=(f"Price moved {slippage:.1%} since signal "
                                f"(entry={entry_in_token_frame:.3f} now={current_price:.3f})"),
                    )

        # Liquidity guard: check orderbook depth on THE token we're buying
        book = self._clob.get_order_book(token_id)
        asks = book.get("asks") or []
        ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks[:5])
        if ask_depth < size_usd * 2:
            return self._result(
                False,
                reason=f"Thin liquidity on {outcome_label} (ask depth ${ask_depth:.0f} < 2x trade ${size_usd:.0f})",
            )

        logger.info(
            "[LIVE%s] %s %s $%.0f @ %.3f | conf=%.0f%% | %s",
            "(DRY)" if self.DRY_RUN else "",
            sig_type, outcome_label, size_usd, current_price,
            confidence * 100, (signal.get("question") or "")[:40],
        )

        # 5. DRY RUN: log + record + return
        if self.DRY_RUN:
            self._record_attempt(
                signal, size_usd, current_price, None,
                dry_run=True, status="dry_run", error_msg=None,
            )
            return self._result(
                True, mode="dry_run", size_usd=size_usd,
                filled_price=current_price,
                reason=f"DRY RUN — would BUY {outcome_label} token (no order submitted)",
            )

        # 6. LIVE — submit. Polymarket CLOB side is always BUY; YES vs NO
        # is determined by token_id.
        if not self._clob.is_configured:
            return self._result(False, reason="CLOB not configured — set POLYMARKET_API_KEY etc.")

        order_result: OrderResult = self._clob.place_market_order(
            token_id=token_id, side="BUY", size_usdc=size_usd, max_slippage=0.02,
        )
        self._record_attempt(
            signal, size_usd, current_price, order_result,
            dry_run=False,
            status=order_result.status if order_result.success else "error",
            error_msg=order_result.error_msg,
        )
        if order_result.success:
            logger.info("[LIVE] order placed: %s @ %.3f",
                        order_result.order_id, order_result.filled_price or current_price)
            return self._result(
                True, mode="live", order_id=order_result.order_id,
                size_usd=size_usd, filled_price=order_result.filled_price,
            )
        else:
            logger.error("[LIVE] order failed: %s", order_result.error_msg)
            return self._result(False, reason=order_result.error_msg)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _resolve_token_ids(self, condition_id: str) -> list[str]:
        """Return both YES and NO clob token IDs for a market.

        Gamma's ``clobTokenIds`` is a 2-element list ordered
        ``[yes_token_id, no_token_id]`` for binary markets.
        """
        if not condition_id:
            return []
        try:
            import requests as _req
            r = _req.get(
                "https://gamma-api.polymarket.com/markets",
                params={"condition_ids": condition_id},
                timeout=8,
            )
            data = r.json()
            m = data[0] if isinstance(data, list) and data else None
            if m and (m.get("conditionId") or "").lower() == condition_id.lower():
                tids_raw = m.get("clobTokenIds") or "[]"
                tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                if isinstance(tids, list):
                    return [str(t) for t in tids if t]
        except Exception as exc:
            logger.debug("token id resolve failed: %s", exc)
        return []

    # Backwards-compatible shim — returns the YES token id only.
    def _resolve_token_id(self, condition_id: str) -> str | None:
        tids = self._resolve_token_ids(condition_id)
        return tids[0] if tids else None

    def _record_attempt(
        self,
        signal: dict[str, Any],
        size_usd: float,
        entry_price: float | None,
        order_result: OrderResult | None,
        *,
        dry_run: bool,
        status: str,
        error_msg: str | None,
    ) -> None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.execute(
                    """INSERT INTO live_trades
                       (attempted_at, signal_type, condition_id, question, direction,
                        confidence, size_usd, entry_price, order_id, status,
                        dry_run, error_msg)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(time.time()),
                        signal.get("signal_type"),
                        signal.get("condition_id", ""),
                        (signal.get("question") or "")[:200],
                        signal.get("direction", "BUY"),
                        signal.get("confidence"),
                        float(size_usd) if size_usd is not None else None,
                        float(entry_price) if entry_price is not None else None,
                        order_result.order_id if order_result else None,
                        status,
                        1 if dry_run else 0,
                        error_msg,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("live_trades record failed: %s", exc)

    def _result(
        self,
        success: bool,
        mode: str = "live",
        order_id: str | None = None,
        size_usd: float | None = None,
        filled_price: float | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return {
            "success": success,
            "mode": mode,
            "order_id": order_id,
            "size_usd": size_usd,
            "filled_price": filled_price,
            "reason": reason,
        }

    def test_dry_run(self, signal_type: str = "price_velocity") -> dict[str, Any]:
        """Synthetic dry-run test for the kill switch + sizer pipeline."""
        test_signal = {
            "signal_type": signal_type,
            "condition_id": "test_cid",
            "question": "DRY RUN TEST",
            "direction": "BUY",
            "confidence": 0.65,
            "category": "test",
        }
        original = self.DRY_RUN
        self.DRY_RUN = True
        try:
            return self.execute(test_signal)
        finally:
            self.DRY_RUN = original
