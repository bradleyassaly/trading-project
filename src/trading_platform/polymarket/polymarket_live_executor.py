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
import os
import threading
import time
from typing import Any

from trading_platform.polymarket.clob_client import ClobClient, OrderResult
from trading_platform.polymarket.kelly_sizer import KellySizer
from trading_platform.polymarket.kill_switch import KillSwitch
from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)


def _open_event_yes_cost(conn, condition_id: str, event_slug: str) -> tuple[float, int]:
    """(summed YES cost/share, leg count) of our OTHER open BUY legs on the
    same real-world event as ``condition_id`` (roadmap N7 helper).

    entry_price for a BUY row is stored YES-space, so summing entry_price is
    summing YES cost per share directly. Polymarket splits busy events across
    a "-more-markets" overflow page, so normalize that suffix before grouping.
    The candidate's own condition_id is excluded. Extracted for unit testing;
    callers add the candidate's own YES price to the sum before comparing to 1.0.
    """
    base = (event_slug or "").replace("-more-markets", "")
    row = conn.execute(
        """SELECT COALESCE(SUM(lt.entry_price), 0), COUNT(*)
             FROM live_trades lt
             JOIN markets m ON m.condition_id = lt.condition_id
            WHERE lt.dry_run = 0 AND lt.exit_ts IS NULL
              AND lt.status IN ('submitted','live','matched')
              AND lt.direction = 'BUY'
              AND lt.entry_price IS NOT NULL
              AND lt.condition_id <> ?
              AND REPLACE(m.event_slug, '-more-markets', '') = ?""",
        (condition_id, base),
    ).fetchone()
    return (float(row[0] or 0) if row else 0.0,
            int(row[1] or 0) if row else 0)


# In-process dedup: prevents concurrent signals for the same (condition_id, direction)
# from racing past the DB check before either INSERT completes.
_DEDUP_LOCKS: dict = {}
_DEDUP_LOCKS_META = threading.Lock()

# 2026-05-18: topic concentration cap. Weekend 5/15-18 booked 12 of 15 SELL
# entries on Musk/CZ tweet-count markets — one Musk posting burst would have
# correlated-lossed the whole pool. Cap open positions per "topic stem"
# (question with numbers/dates/years stripped, first 6 words).
MAX_OPEN_PER_TOPIC = 3

# R2 cluster-gate degraded-alert rate limit (10 min between Telegram pages).
_CLUSTER_GATE_LAST_ALERT: float = 0.0

_TOPIC_STRIP_DATE = __import__("re").compile(
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d+\b",
    __import__("re").IGNORECASE,
)
_TOPIC_STRIP_RANGE = __import__("re").compile(r"\d+\s*[-–]\s*\d+")
_TOPIC_STRIP_NUMS = __import__("re").compile(r"\b\d+\b")
_TOPIC_STRIP_WS = __import__("re").compile(r"\s+")


def _topic_stem(question: str | None) -> str:
    """Normalize a market question to a topic key for concentration grouping.

    "Will Elon Musk post 240-259 tweets from May 12 to May 19, 2026?"
    "Will Elon Musk post 65-89 tweets from May 16 to May 23, 2026?"
        → both produce "will elon musk post tweets from"

    Returns "" when input is empty/falsy so callers can skip the check.
    """
    if not question:
        return ""
    q = question.lower()
    q = _TOPIC_STRIP_DATE.sub("", q)
    q = _TOPIC_STRIP_RANGE.sub("", q)
    q = _TOPIC_STRIP_NUMS.sub("", q)
    q = _TOPIC_STRIP_WS.sub(" ", q).strip()
    return " ".join(q.split()[:6])


class PolymarketLiveExecutor:
    """Live trading executor with multi-layer safety gating."""

    # 2026-04-28: per-signal-type live execution allowlist.
    # Signals in this set bypass DRY_RUN — they fire REAL orders
    # subject to all the standard 14-gate safety stack PLUS the
    # discovery-tier $1 hard cap. Default policy: keep this set
    # narrow (only Bayesian-validated signals).
    #
    # Current allowlist:
    #   whale_entry — Bayesian validated 2026-04-27:
    #     n=13 resolved, WR=77%, P(acc≥55%)=0.915
    #     /api/ladder/status promotable_signals confirms
    # 2026-05-01: pivoted from {whale_entry} after fresh backtest.
    # whale_entry on n=1,892 historical replays: WR 47%, EV -30.9%.
    # The Bayesian gate's positive verdict was reading a small (n=15)
    # gate-filtered sample, not the real signal distribution. The
    # kill_switch's blended-EV block was correct.
    #
    # New allowlist points at the two signals with proven backtest edge:
    #   wallet_reversal:    n=192, WR=72%, EV=+10.1%
    #   specialist_entry:   n=60,  WR=80%, EV=+12.7%
    # 2026-05-03: added probation-tier signals (probation = $5 cap until n≥15):
    #   tier_entry:  n=15, WR=100%, EV=+32% (small sample, probation)
    #   whale_exit:  n=16, WR=100%, EV=+3.7%  (small sample, probation)
    LIVE_REAL_SIGNAL_TYPES: set[str] = {
        # wallet_reversal removed 2026-05-09: 60-day backtest WR=51%, EV=-0.024.
        # All 3 live exits today were stop-losses. Re-add if EV recovers above 0.0
        # on n>=50 resolved trades in a fresh 30-day window.
        # specialist_entry re-added 2026-05-09: IC30=+0.050, IC14=+0.107, decay_flag cleared.
        # 60-day backtest: WR=89%, EV=+0.204, n=79 resolved. Monitor IC30 daily.
        "specialist_entry",
        "tier_entry", "whale_exit",
        # whale_entry_filtered: IC30=+0.313, +$574 paper PnL n=148 (sports blocked).
        # cascade removed 2026-05-08: live WR=16% over 19 trades (paper 45% WR was in
        # sports/geopolitics/science which are now category-blocked). Re-add when
        # live WR recovers above 40% on n>=20 in allowed categories.
        "whale_entry_filtered",
        # whale_entry removed 2026-05-05: IC30=-0.383, IC14=-0.674, decay_flag=True.
        # Fires 5k+/week, 100% blocked by kill switch (EV=-0.291). Pure noise.
        # Re-add if IC30 recovers above 0.0 for 7+ consecutive days.
        "reversal_confluence", "pre_resolution_entry",
        # resolution_decay: 85% WR, +185% EV, n=27 — best WR of any signal.
        "resolution_decay",
        # oversized_bet removed 2026-05-09: 60-day backtest WR=36%, EV=-0.312.
        # Re-add only if a fresh 30-day window shows EV > 0.0 on n>=20 resolved.
    }

    # 2026-07-05: hard KILL list. Overrides EVERY live-promotion path
    # (allowlist above, 3-dim slice promotion, probation tiers). The daily
    # review's reconciled per-signal verdict (section 13) is the entry
    # criterion; removal requires a fresh positive reconciled window.
    #   wallet_reversal: 90d reconciled -$48.08, ev_act -40%, ev_res -54%,
    #   verdict KILL — worst live signal, largest booked losses on 7/2.
    LIVE_KILL_SIGNAL_TYPES: set[str] = {
        "wallet_reversal",
        # 2026-07-07 C1+C2 pre-registered KILL: 120d fill-modeled diagnostic,
        # 118,646 copies / 787 wallets at min_n / ZERO qualified — no
        # copyable cohort exists (run-1's "+$6.82 scalper cohort" was an 83%
        # survivorship artifact). Independently, the C4 regime monitor has
        # this signal in DRIFT_BREACH (30d EV -54.8%/$, entire CI < 0).
        # Copy-ENTRY is retired per reports/mirror_copy_kill_rule.md; the
        # wallet graph remains a feature source; paper keeps collecting.
        "whale_entry_filtered",
    }

    # ALWAYS True in source. Flip in your local instance only.
    # Class default applies to non-allowlisted signal types.
    DRY_RUN: bool = True

    # Hard cap on a real (non-DRY) trade size — defense-in-depth on
    # top of LIVE_DISCOVERY_MAX_STAKE_USD in kill_switch.
    # 2026-05-02: this is now a FALLBACK. The runtime cap is read from
    # stake_ladder.get_current_cap() which auto-promotes through tiers
    # ($1→$5→$25→$100→$500→$2500) based on real-money trade history.
    LIVE_REAL_MAX_STAKE_USD: float = 1.0

    def _live_real_cap(self, signal_type: str | None = None,
                       category: str | None = None) -> float:
        """Read the runtime stake cap. Per-slice scaling on top of the
        global ladder cap.

        Logic:
          base = ladder cap (current tier)
          if (signal, category) is a Tier-A proven slice with EV ≥ 30%
            → 1.0 × base
          elif EV ≥ 15%
            → 0.75 × base
          elif EV ≥ 5% (proven but marginal)
            → 0.5 × base
          else (no slice data, or weak EV)
            → 0.25 × base (conservative)
        """
        try:
            from trading_platform.polymarket.stake_ladder import get_current_cap
            base = float(get_current_cap(db_path=self._db_path) or self.LIVE_REAL_MAX_STAKE_USD)
        except Exception:
            base = self.LIVE_REAL_MAX_STAKE_USD
        if not signal_type or not category:
            return base * 0.25  # no slice info → conservative
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection(self._db_path)
            try:
                row = conn.execute(
                    """SELECT n_resolved, avg_ev FROM signal_category_ev
                        WHERE signal_type = ? AND category = ?""",
                    (signal_type, category),
                ).fetchone()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception:
            return base * 0.25
        if not row:
            return max(5.0, base * 0.25)
        n = int(row[0] or 0)
        ev = float(row[1] or 0)
        # Floor at $5 (CLOB minimum for 5 tokens at any price). Without this,
        # the 0.25× conservative slice on a $5 ladder cap returns $1.25 which
        # can never place a real order (CLOB rejects < 5 shares).
        CLOB_MIN_FLOOR = 5.0
        if n < 8:
            return max(CLOB_MIN_FLOOR, base * 0.25)
        if ev >= 0.30:
            return max(CLOB_MIN_FLOOR, base * 1.0)
        if ev >= 0.15:
            return max(CLOB_MIN_FLOOR, base * 0.75)
        if ev >= 0.05:
            return max(CLOB_MIN_FLOOR, base * 0.50)
        return max(CLOB_MIN_FLOOR, base * 0.25)

    def __init__(self) -> None:
        self._db = WalletDB()
        self._db_path = str(self._db._path)
        self._clob = ClobClient()
        self._kill = KillSwitch(self._db_path)
        self._sizer = KellySizer(self._db_path)
        self._ensure_live_trades_table()
        # Cache of condition_ids that returned 403 "Trading restricted".
        # Loaded from live_trades on startup so restarts don't re-attempt.
        self._restricted_conditions: set[str] = self._load_restricted_conditions()

    def _is_dry_run_for(
        self,
        signal_type: str,
        category: str | None = None,
        wallet_tier: str | None = None,
    ) -> bool:
        """Per-signal DRY_RUN check.

        Allowlisted types (LIVE_REAL_SIGNAL_TYPES) fire real orders.
        Non-allowlisted types are also promoted to live when their
        (signal, category, wallet_tier) 3-dim slice is proven
        (n>=5, EV>=15%) — this is the mechanism that lets slices
        discovered by the 3-dim slicer (e.g. whale_entry × sports ×
        tier1 +256% EV) reach live trading without requiring a code
        change to LIVE_REAL_SIGNAL_TYPES.

        In both paths, 3 consecutive real-money losses auto-demote
        the signal back to dry-run.
        """
        from trading_platform.polymarket.db_connection import get_connection as _gc
        # 2026-07-02: MASTER SWITCH FIRST. Previously the env master switch
        # was only enforced deep inside KillSwitch.check(), while both the
        # allowlist and the 3-dim slice promotion below bypassed DRY_RUN
        # entirely — two independent paths to real orders whose only
        # backstop was that one buried env read. The master switch is now
        # authoritative at the top of the gating chain: nothing below this
        # line can produce a real order without POLYMARKET_LIVE_ENABLED=1.
        if os.getenv("POLYMARKET_LIVE_ENABLED", "").lower() not in ("1", "true", "yes"):
            return True
        # Hard KILL list — beats every promotion path (allowlist, 3-dim
        # slice, probation). A signal lands here when the reconciled
        # per-signal EV verdict says KILL; nothing short of a code change
        # puts it back on real money.
        if signal_type in self.LIVE_KILL_SIGNAL_TYPES:
            logger.info(
                "[LIVE][KILL_LIST] %s — reconciled EV verdict KILL, dry-run only",
                signal_type,
            )
            return True
        # Signals with decay_flag=1 in signal_health are blocked from all live
        # paths — including 3-dim slice promotion. IC30 degradation means recent
        # predictions are anti-correlated with outcomes regardless of what a
        # specific (signal × category × tier) slice showed historically.
        try:
            _conn = _gc(self._db_path)
            try:
                _decay = _conn.execute(
                    "SELECT decay_flag FROM signal_health WHERE signal_type = ?",
                    (signal_type,),
                ).fetchone()
            finally:
                try: _conn.close()
                except Exception: pass
            if _decay and int(_decay[0] or 0):
                logger.warning(
                    "[LIVE][DECAY_BLOCK] %s — decay_flag=1 in signal_health, blocking live path",
                    signal_type,
                )
                return True
        except Exception:
            pass
        promoted_via_3dim = False
        if signal_type not in self.LIVE_REAL_SIGNAL_TYPES:
            # 2026-07-02: 3-dim slice promotion now requires explicit opt-in.
            # n>=5 / EV>=15% on one slice is far too weak to route a signal
            # to real money automatically (hundreds of slices are tested in
            # parallel — at that threshold noise WILL qualify). Off by
            # default until the reconciled ledger justifies re-enabling.
            _allow_3dim = os.getenv(
                "POLYMARKET_ALLOW_3DIM_LIVE", ""
            ).lower() in ("1", "true", "yes")
            if _allow_3dim and category and wallet_tier:
                try:
                    _conn = _gc(self._db_path)
                    try:
                        _r = _conn.execute(
                            """SELECT avg_ev FROM signal_category_wallet_ev
                                WHERE signal_type = ? AND category = ?
                                  AND wallet_tier = ?
                                  AND n_resolved >= 5 AND avg_ev >= 0.15""",
                            (signal_type, category, wallet_tier),
                        ).fetchone()
                    finally:
                        try: _conn.close()
                        except Exception: pass
                    if _r:
                        promoted_via_3dim = True
                        logger.info(
                            "[LIVE][3DIM_PROMOTE] %s × %s × %s EV=%+.0f%% — 3-dim proven, entering live path",
                            signal_type, category, wallet_tier, float(_r[0]) * 100,
                        )
                except Exception:
                    pass
            if not promoted_via_3dim:
                return self.DRY_RUN
        # Check last 3 closed real-money trades on this signal type.
        # Exclude discovery-tier ($1) stakes: those are probe trades on any
        # available market (including long-dated markets now gated by the
        # 30-day horizon filter). Three $1 stop-losses on wrong markets
        # should not demote a signal that has strong paper evidence.
        # Only trades with size_usd > $1.50 count toward the demote gate.
        #
        # 2026-05-31: also exclude resolved_zero_balance from the loss count.
        # When a market resolves against us (NO position on a YES-resolving
        # market), that's a MARKET OUTCOME not a signal-quality issue.
        # Bug surfaced today: whale_entry_filtered was auto-demoted to
        # paper because its last 3 closes were #26563/#20474 (both
        # resolved_zero_balance) + #9671 (time_decay). 2 of 3 were
        # market-outcome losses, not strategy losses. The signal had been
        # profitable on take_profit/trailing_stop exits. Filtering out
        # resolved_zero_balance restores accurate signal-quality assessment.
        try:
            _conn = _gc(self._db_path)
            try:
                _row = _conn.execute(
                    """SELECT outcome FROM live_trades
                        WHERE signal_type = ? AND dry_run = 0
                          AND outcome IN ('win','loss')
                          AND size_usd > 1.50
                          AND COALESCE(exit_reason, '') NOT IN
                              ('resolved_zero_balance', 'time_decay')
                        ORDER BY exit_ts DESC LIMIT 3""",
                    (signal_type,),
                ).fetchall()
            finally:
                try: _conn.close()
                except Exception: pass
            if len(_row) >= 3 and all((r[0] or "") == "loss" for r in _row):
                logger.warning(
                    "[LIVE][AUTO_DEMOTE] %s — 3 consecutive real losses "
                    "(size>$1.50, action-triggered exits only), back to dry-run",
                    signal_type,
                )
                return True
        except Exception:
            pass
        return False

    def _ensure_live_trades_table(self) -> None:
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection(self._db_path)
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
                        confidence_raw REAL,
                        size_usd REAL,
                        entry_price REAL,
                        order_id TEXT,
                        status TEXT,
                        dry_run INTEGER DEFAULT 1,
                        error_msg TEXT,
                        outcome TEXT,
                        exit_ts INTEGER
                    )
                """)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_live_trades_ts ON live_trades(attempted_at DESC)"
                )
                # Lazy-add columns that pre-date this schema — idempotent.
                # (P6 audit) The fresh-CREATE above lacks most columns the
                # attempt INSERT writes — on a truly fresh deployment every
                # record silently failed. All INSERT columns now covered here.
                for col_ddl in ("outcome TEXT", "exit_ts INTEGER", "signal_price REAL",
                                "whale_trade_ts BIGINT", "detection_latency_sec REAL",
                                "slippage_signed REAL", "slippage_cost_usd REAL",
                                "features_at_fire TEXT",
                                "token_id TEXT", "side TEXT", "fill_price REAL",
                                "shares REAL", "category TEXT", "signal_wallet TEXT",
                                "submitted_at INTEGER", "filled_at INTEGER",
                                "expected_price REAL", "slippage REAL",
                                "fill_time_ms REAL", "resolution_date BIGINT",
                                "ask_depth_entry REAL", "signal_fired_at BIGINT",
                                "wallet_tier TEXT", "realized_pnl REAL",
                                # P6: decision-time market context on EVERY
                                # attempt (incl. rejects/errors) — the
                                # measurement substrate execution work needs.
                                "mid_at_decision REAL", "yes_mid_at_decision REAL",
                                "best_ask_at_decision REAL", "best_bid_at_decision REAL",
                                "spread_at_decision REAL", "book_target_price REAL",
                                "slippage_c REAL", "spread_paid_c REAL",
                                # P3: real delivery-lane tag
                                "source_lane TEXT"):
                    try:
                        conn.execute(f"ALTER TABLE live_trades ADD COLUMN {col_ddl}")
                    except Exception:
                        pass
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("live_trades table ensure failed: %s", exc)

    def execute(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Attempt a (gated) live trade for a fired signal."""
        sig_type = signal.get("signal_type", "")
        confidence = float(signal.get("confidence") or 0)
        condition_id = signal.get("condition_id") or ""

        # Restricted-market fast-exit: skip condition_ids that previously
        # returned 403 "Trading restricted" to avoid burning error records.
        if condition_id and condition_id in self._restricted_conditions:
            logger.debug("[LIVE] SKIP restricted condition %s", condition_id[:16])
            return self._block(signal, "condition_id in restricted cache (prior 403)")

        # 2026-05-05: market horizon gate. Two bounds:
        #   UPPER (30 days) — resolution too slow stalls kill-switch sample
        #     accumulation; need 15 resolved to reach full Kelly.
        #   LOWER (4 hours, added 2026-05-21) — SELL orders on markets within
        #     hours of resolution face collapsed liquidity: wallets have
        #     already exited because they know the outcome, leaving no
        #     counter-side. Past-24h zero-fill rate was 57% (4 of 7) on
        #     near-resolution markets (Natural Gas May 21 Up/Down, Senate
        #     reconciliation by May, Gold Week of May 1, Brazil STF
        #     impeachment). Blocking these eliminates the bleed.
        #
        # Lookup strategy: local markets table first (fast), Gamma API
        # fallback when condition_id isn't in the local cache. Our trades
        # come from whale-flow signals across the whole Polymarket universe
        # but the markets table only caches top-N by volume — so Gamma
        # fallback is the common path, not the exception.
        LIVE_MAX_HORIZON_DAYS = 30
        LIVE_MIN_HOURS_TO_RESOLVE = 4.0
        if condition_id:
            # 2026-07-07 (roadmap N10): prefer end_date_iso carried on the
            # signal dict. Both signal engines already know it (whale from its
            # market pre-filter, resolution_decay from its query) — reading it
            # here turns the horizon gate into a pure dict read and removes a
            # synchronous Gamma round-trip from the fire-time hot path, which
            # was the dominant latency term on the resolution_decay lane. The
            # markets-table SELECT and Gamma GET remain as ordered fallbacks
            # for whale signals across the full universe that don't carry it.
            end_date_iso = signal.get("end_date_iso") or None
            if not end_date_iso:
                try:
                    from trading_platform.polymarket.db_connection import get_connection as _gc
                    _hconn = _gc(self._db_path)
                    try:
                        _hrow = _hconn.execute(
                            "SELECT end_date_iso FROM markets WHERE condition_id = ?",
                            (condition_id,),
                        ).fetchone()
                    finally:
                        try: _hconn.close()
                        except Exception: pass
                    if _hrow and _hrow[0]:
                        end_date_iso = str(_hrow[0])
                except Exception as _hex:
                    logger.debug("[LIVE] markets-table horizon lookup failed: %s", _hex)

            # Gamma fallback for markets not in local cache or signal dict.
            if not end_date_iso:
                try:
                    import requests as _req
                    r = _req.get(
                        "https://gamma-api.polymarket.com/markets",
                        params={"condition_ids": condition_id},
                        timeout=5,
                    )
                    data = r.json()
                    m = data[0] if isinstance(data, list) and data else None
                    if m and (m.get("conditionId") or "").lower() == condition_id.lower():
                        end_date_iso = m.get("endDate") or m.get("end_date_iso")
                except Exception as _gex:
                    logger.debug("[LIVE] gamma horizon fallback failed: %s", _gex)

            if end_date_iso:
                try:
                    from datetime import datetime, timezone as _tz
                    _end = datetime.fromisoformat(str(end_date_iso).replace("Z", "+00:00"))
                    _hours_out = (_end - datetime.now(tz=_tz.utc)).total_seconds() / 3600
                    _days_out = _hours_out / 24
                    signal["_resolution_ts"] = int(_end.timestamp())
                    if _hours_out < LIVE_MIN_HOURS_TO_RESOLVE:
                        _reason = (f"market resolves in {_hours_out:.1f}h "
                                   f"(min {LIVE_MIN_HOURS_TO_RESOLVE:.0f}h) — "
                                   f"thin-book zero-fill risk")
                        logger.info("[LIVE][HORIZON_LO] %s %s", sig_type, _reason)
                        self._record_attempt(signal, 0.0, None, None,
                                             dry_run=self.DRY_RUN, status="blocked",
                                             error_msg=_reason)
                        return self._result(False, reason=_reason)
                    if _days_out > LIVE_MAX_HORIZON_DAYS:
                        _reason = (f"market resolves in {_days_out:.0f}d "
                                   f"(max {LIVE_MAX_HORIZON_DAYS}d)")
                        logger.debug("[LIVE] SKIP %s — %s", condition_id[:16], _reason)
                        return self._block(signal, _reason)
                except Exception as _hex:
                    logger.debug("[LIVE] horizon parse failed (proceeding): %s", _hex)

        # 2026-04-25: isotonic calibration on the inbound confidence.
        # Mirrors paper executor — the same overconfidence bias affects
        # live-bound signals (Brier 0.35 → 0.24 after correction).
        # Falls back to identity if curve hasn't been fit yet.
        #
        # 2026-06-17: pass signal_type + direction so apply_calibration uses
        # the per-(signal × direction) curve when ENABLE_PER_SLICE_CALIB=1.
        # The per-slice curves have been fit + persisted weekly since
        # 2026-05-25 (refit_per_slice_calibration) and validated out-of-band
        # (Brier whale SELL 0.41→0.21, wallet_reversal SELL 0.56→0.28,
        # specialist SELL 0.27→0.10). Previously this call passed only
        # `category`, so the per-slice branch never fired at trade time and
        # the entire per-slice correction was carried by the sizing-multiplier
        # band-aid. Wiring the calibrated curve in directly makes `confidence`
        # trustworthy; the multipliers then self-retire toward 1.0 as the next
        # update_sizing_multipliers refit recomputes actual_WR / mean_confidence
        # on the now-calibrated confidence. Final size stays clamped to
        # runtime_cap, so the transition cannot over-size.
        try:
            from trading_platform.polymarket.isotonic_calibration import apply_calibration
            raw_conf = confidence
            confidence = apply_calibration(
                confidence,
                category=(signal.get("category") or "").lower() or None,
                db_path=self._db_path,
                signal_type=sig_type,
                direction=(signal.get("direction") or "").upper() or None,
            )
            if abs(confidence - raw_conf) > 0.02:
                logger.info(
                    "[LIVE][CALIB] %s raw=%.3f → calibrated=%.3f",
                    sig_type, raw_conf, confidence,
                )
            signal["confidence_raw"] = round(raw_conf, 4)
            signal["confidence"] = confidence
        except Exception as exc:
            logger.debug("[LIVE][CALIB] apply failed: %s", exc)

        # 0a-pre. Per-trade calibrated-EV gate for the hold-to-resolution
        # lane (2026-07-06). resolution_decay BUYs hold to resolution, so
        # for a binary payoff the trade is -EV whenever calibrated
        # P(win) <= entry price — Kelly sizes off signal-level stats and
        # never made this per-trade comparison, which let 2026-07-06
        # entries fill at 0.22-0.35 with calibrated conf 0.167 (-32%/$
        # expected). Whale-mirror lanes are exempt: they exit via
        # TP/trailing before resolution, so conf<=price does not imply
        # -EV there. Paper is exempt too — it must keep collecting the
        # outcomes that feed calibration.
        if sig_type == "resolution_decay" and \
                (signal.get("direction") or "BUY").upper() == "BUY":
            _gate_px = float(signal.get("price") or 0)
            _min_edge = float(os.environ.get("RESOLUTION_DECAY_MIN_EDGE", "0.05"))
            if _gate_px > 0 and confidence <= _gate_px * (1.0 + _min_edge):
                return self._block(
                    signal,
                    f"calibrated-EV gate: conf {confidence:.3f} <= "
                    f"px {_gate_px:.3f} × {1 + _min_edge:.2f} — hold-to-"
                    f"resolution BUY is -EV",
                )
            # 2026-07-08: veto-only decay-curve EV filter. The A2 challenger
            # (decay_lookup_p, honest empirical P(YES) per slice×hours×price
            # cell) rides in the payload; the hand-coded confidence above is
            # overconfident (0.5-0.85 vs true ~0.17), so the formula gate
            # alone passes -EV cells. This lets the honest P VETO a fire it
            # would clear — but NEVER authorize/up-size one (asymmetric
            # caution: strictly risk-reducing, so it respects the
            # champion/challenger discipline before the DECAY_CURVE_ENFORCE
            # Brier gate). LIVE ONLY — paper keeps collecting every fire so
            # the Brier comparison sees the full distribution. Fail-safe:
            # None lookup → no veto. This is what makes widening the scan
            # aperture to 48h add only +wedge volume, not more -EV volume.
            _decay_p = signal.get("decay_lookup_p")
            if (_gate_px > 0 and _decay_p is not None
                    and os.environ.get("DECAY_CURVE_VETO", "1").lower()
                        in ("1", "true", "yes")
                    and float(_decay_p) <= _gate_px * (1.0 + _min_edge)):
                try:
                    from trading_platform.polymarket.decision_trace import trace as _dt
                    _dt(signal=signal, gate="LIVE_DECAY_VETO", passed=False,
                        value=float(_decay_p), threshold=_gate_px * (1.0 + _min_edge),
                        detail=f"decay_p {float(_decay_p):.3f} <= px {_gate_px:.3f}",
                        surface="live", db_path=self._db_path)
                except Exception:
                    pass
                return self._block(
                    signal,
                    f"decay-curve veto: empirical P {float(_decay_p):.3f} <= "
                    f"px {_gate_px:.3f} × {1 + _min_edge:.2f} — cell has no "
                    f"+wedge (formula conf {confidence:.3f} overconfident)")

        # 0a. Category allowlist — live trades restricted to categories
        # with statistically significant positive resolved EV.
        from trading_platform.polymarket.polymarket_paper_executor import (
            LIVE_TRADE_CATEGORIES, EXCLUDE_SIGNAL_TYPES, EXCLUDE_SIGNAL_SIDE,
        )
        # LIVE_REAL_SIGNAL_TYPES is an explicit allowlist that overrides paper-level
        # exclusions. whale_entry is in paper EXCLUDE (backtest -EV aggregate) but
        # has proven live edge by slice — allowlist takes precedence.
        if sig_type in EXCLUDE_SIGNAL_TYPES and sig_type not in self.LIVE_REAL_SIGNAL_TYPES:
            logger.debug("[LIVE] BLOCKED signal_type=%s (excluded)", sig_type)
            return self._block(signal, f"signal_type {sig_type} excluded from live")
        _sig_side = (signal.get("side") or "").upper()
        if _sig_side and (sig_type, _sig_side) in EXCLUDE_SIGNAL_SIDE:
            logger.info("[LIVE][SIDE_GATE] BLOCKED %s side=%s", sig_type, _sig_side)
            return self._block(signal, f"{sig_type} {_sig_side} excluded from live")
        # N9: correlation auto-deprecation also blocks live (allowlist
        # overrides, matching EXCLUDE precedence). Protected signals never
        # carry auto_deprecated=1.
        if sig_type not in self.LIVE_REAL_SIGNAL_TYPES:
            try:
                from trading_platform.polymarket.db_connection import get_connection as _dgc
                _dconn = _dgc(self._db_path)
                try:
                    _drow = _dconn.execute(
                        "SELECT auto_deprecated, deprecated_reason FROM signal_health "
                        "WHERE signal_type = ?",
                        (sig_type,),
                    ).fetchone()
                finally:
                    try: _dconn.close()
                    except Exception: pass
                if _drow and int(_drow[0] or 0) == 1:
                    return self._block(
                        signal, f"corr-deprecated: {_drow[1] or 'redundant'}")
            except Exception as _dcerr:
                logger.debug("[LIVE] corr-deprecate check failed (continuing): %s", _dcerr)
        raw_cat = signal.get("category") or ""
        cat = raw_cat.lower() if isinstance(raw_cat, str) else ""
        # Re-classify when category is empty/other/wallet_derived — the
        # paper executor already does this (polymarket_paper_executor.py
        # around L495). Without it, sports markets tagged "other" by
        # signal generation get blocked here while their paper twins
        # correctly run as "sports". Mirrors the paper path exactly.
        if not cat or cat in ("other", "wallet_derived"):
            try:
                from trading_platform.polymarket.market_categorizer import classify_keywords
                resolved, _src = classify_keywords(
                    signal.get("slug") or "",
                    signal.get("question") or "",
                )
                if resolved and resolved != "other":
                    cat = resolved.lower()
                    signal["category"] = cat
            except Exception as exc:
                logger.debug("[LIVE] category classifier failed: %s", exc)
            # Sports fallback — see paper executor for rationale.
            if cat in ("", "other", "wallet_derived"):
                try:
                    from trading_platform.polymarket.whale_signal_engine import _is_sports_market
                    if _is_sports_market(cat, signal.get("question") or ""):
                        cat = "sports"
                        signal["category"] = "sports"
                except Exception as exc:
                    logger.debug("[LIVE] sports fallback failed: %s", exc)
        # Per-signal category exclusions. signal_category_ev shows these as
        # positive (stale resolution data), but paper trades are consistently
        # negative — trust the more recent execution evidence.
        # 2026-05-03: whale_entry_filtered×sports (-$0.72/trade, 30% WR n=60)
        #             cascade×sports (-$2.27/trade, 27.8% WR n=18)
        # 2026-05-05: whale_entry_filtered×geopolitics: n=12, WR=50%, avg_stake
        #             $32.69 on losses vs $2.25 on wins → -$94 net paper. Kelly
        #             over-sizes high-confidence geo trades that consistently lose.
        # 2026-05-06: cascade×geopolitics: 3 losses overnight (US-Iran peace deal).
        #             cascade×sports: PSG misclassified as 'science' — also block
        #             'science' for cascade since FC matches landing there.
        # P5 slice_gate: DERIVED negative-slice blocklist (Wilson-bound
        # demotions recomputed 6-hourly) — replaces the hardcoded lists
        # below once enforced. Runs BEFORE the Wilson SLICE_BYPASS so a
        # demoted slice can never bypass. Fail-safe: a stale/never-ran
        # table falls through to the existing hardcoded behavior.
        try:
            from trading_platform.polymarket.slice_multiplier_promoter import get_slice_gate
            from trading_platform.polymarket import flags as _flags
            _gate_status, _gate_fresh = get_slice_gate(sig_type, cat,
                                                       db_path=self._db_path)
            if _gate_fresh and _gate_status in ("demoted", "killed"):
                try:
                    from trading_platform.polymarket.decision_trace import trace as _dt
                    _dt(signal=signal,
                        gate="LIVE_SLICE_GATE" if _flags.SLICE_GATE_ENFORCE
                        else "LIVE_SLICE_GATE_SHADOW",
                        passed=False, detail=f"{_gate_status}: {sig_type}×{cat}",
                        surface="live", db_path=self._db_path)
                except Exception:
                    pass
                if _flags.SLICE_GATE_ENFORCE:
                    logger.info("[SLICE_GATE] BLOCK %s × %s (%s)",
                                sig_type, cat, _gate_status)
                    return self._block(
                        signal, f"slice_gate {_gate_status}: {sig_type}×{cat}")
                logger.info("[SLICE_GATE][SHADOW] would-block %s × %s (%s)",
                            sig_type, cat, _gate_status)
        except Exception as exc:
            logger.debug("[SLICE_GATE] lookup failed (fail-safe): %s", exc)

        _CAT_BLOCKS: dict[str, set[str]] = {
            "whale_entry_filtered": {"sports", "geopolitics", "crypto"},
            "cascade": {"sports", "geopolitics", "science"},
        }
        if cat in _CAT_BLOCKS.get(sig_type, set()):
            logger.info("[LIVE] BLOCKED %s × %s — per-signal category block", sig_type, cat)
            return self._block(signal, f"{sig_type} blocked in category {cat}")

        # 2026-05-02: per-(signal, category) bypass with Wilson-95
        # lower bound. Raw EV check (n>=10, EV>=10%) admits noisy
        # slices (n=10 EV+12% has Wilson lower of -8% — pure luck).
        # Wilson approach: require lower bound >= 0% for any n>=8.
        # Looser n requirement, safer EV requirement.
        slice_proven = False
        slice_n = 0
        slice_ev = 0.0
        try:
            from trading_platform.polymarket.db_connection import get_connection
            _conn = get_connection(self._db_path)
            try:
                _row = _conn.execute(
                    """SELECT n_resolved, win_rate, avg_ev FROM signal_category_ev
                        WHERE signal_type = ? AND category = ?
                          AND n_resolved >= 8""",
                    (sig_type, cat),
                ).fetchone()
            finally:
                try: _conn.close()
                except Exception: pass
            if _row:
                slice_n = int(_row[0])
                wr = float(_row[1])
                slice_ev = float(_row[2])
                # Wilson 95% lower bound for binary outcome (WR proxy
                # for EV direction). Then scale by slice EV to get a
                # rough EV lower bound. Conservative.
                import math
                if slice_n > 0:
                    z = 1.96
                    denom = 1 + z * z / slice_n
                    centre = (wr + z * z / (2 * slice_n)) / denom
                    spread = (z / denom) * math.sqrt(wr * (1 - wr) / slice_n + z * z / (4 * slice_n * slice_n))
                    wr_lower = max(0.0, centre - spread)
                    # Slice approved when (a) WR lower bound > 0.5 OR
                    # (b) WR lower bound > 0.4 AND raw EV >= 5%.
                    # Either tells us the edge is unlikely from noise.
                    if wr_lower > 0.50 or (wr_lower > 0.40 and slice_ev >= 0.05):
                        slice_proven = True
                        logger.info(
                            "[LIVE][SLICE_BYPASS] %s × %s n=%d WR=%.0f%% (Wilson-lower=%.0f%%) "
                            "EV=%+.0f%% — bypassing LIVE_CAT_GATE",
                            sig_type, cat, slice_n, wr * 100, wr_lower * 100, slice_ev * 100,
                        )
        except Exception:
            pass

        if not slice_proven and (not cat or cat not in LIVE_TRADE_CATEGORIES):
            logger.debug("[LIVE] BLOCKED category=%s — not in live allowlist", cat)
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate="LIVE_CAT_GATE", passed=False,
                    detail=cat or "<empty>", surface="live",
                    db_path=self._db_path)
            except Exception:
                pass
            return self._block(signal, f"category '{cat}' not approved for live")

        # 0. Emergency stop check
        stopped, stop_reason = self._kill.is_emergency_stopped()
        if stopped:
            logger.warning("[LIVE] Emergency stop active: %s", stop_reason)
            return self._block(signal, f"Emergency stop: {stop_reason}")

        # 0b. Circuit breaker check (cumulative drawdown layer)
        try:
            from trading_platform.polymarket.circuit_breaker import CircuitBreaker
            cb = CircuitBreaker(self._db_path)
            allowed, cb_reason = cb.can_trade()
            if not allowed:
                logger.warning("[LIVE] Circuit breaker blocked: %s", cb_reason)
                return self._block(signal, f"circuit breaker: {cb_reason}")
        except Exception as exc:
            logger.debug("circuit breaker check failed (proceeding): %s", exc)

        # 0b'. Forward-looking VaR gate (Phase 3 — vol-aware risk).
        # Halts new entries if last 30d VaR(5%) already exceeds VAR_HARD_CAP_FRAC
        # of equity. Defaults off (shadow). Set ENABLE_VAR_GATE=1 to enforce.
        try:
            import os as _os
            if _os.environ.get("ENABLE_VAR_GATE", "0").lower() in ("1", "true", "yes"):
                from trading_platform.polymarket.portfolio_risk import assess as _pf_assess
                _pf = _pf_assess()
                _cap = float(_os.environ.get("VAR_HARD_CAP_FRAC", "0.10"))
                _frac = _pf.get("var5_frac")
                if _frac is not None and _frac > _cap:
                    logger.warning(
                        "[LIVE] VaR gate blocked: VaR(5%%)=%.1f%% > cap %.1f%%",
                        _frac * 100, _cap * 100,
                    )
                    return self._block(
                        signal,
                        f"VaR(5%) {_frac*100:.1f}% exceeds cap {_cap*100:.0f}%",
                    )
        except Exception as exc:
            logger.debug("VaR gate check failed (proceeding): %s", exc)

        # 0c. Specialist boost (mirrors paper executor ~L628). Specialist
        # wallets trading in their proven category earn 1.25x confidence
        # and 2x alpha → roughly 2x stake. Applied pre-Kelly so the
        # boost flows through to `size_usd` below. Backtest on 164
        # resolved paper trades: specialists 52.6% WR / +$960 PnL vs
        # generalists 31.8% / -$1,026 on identical signal types.
        src_wallet = signal.get("wallet") or ""
        if src_wallet and src_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                from trading_platform.polymarket.alpha_scores import is_specialist
                spec = is_specialist(self._db_path, src_wallet, cat)
                if spec["is_specialist"]:
                    signal["is_specialist_source"] = True
                    old_conf = confidence
                    confidence = min(0.99, old_conf * 1.25)
                    signal["confidence"] = confidence
                    logger.info(
                        "[LIVE][SPECIALIST_BOOST] %s in %s (conc=%.0f%% wr=%.0f%% n=%d); conf %.2f→%.2f",
                        src_wallet[:14], cat,
                        spec["concentration"] * 100, spec["win_rate"] * 100,
                        spec["resolved_trades"], old_conf, confidence,
                    )
            except Exception as exc:
                logger.debug("[LIVE] specialist boost lookup failed: %s", exc)

        # 0d. Behavioral-metrics gates (mirrors paper executor; see
        # wallet_behavior_metrics.py). Three signals:
        #   - is_likely_farmer = 1 → BLOCK live entirely (defends real $)
        #   - z-score specialist in category → up to 1.4× confidence
        #   - sizing_p90 ≥ 10% (conviction whale) → 1.15×; <2% (mechanical) → 0.85×
        # Fail-open on lookup error (table missing on first deployment).
        if src_wallet and src_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                from trading_platform.polymarket.db_connection import get_connection
                _bc = get_connection(self._db_path)
                try:
                    bm_row = _bc.execute(
                        "SELECT is_likely_farmer, sizing_p90_pct, is_copyable_ci, "
                        "n_resolved, roi_lower_95 "
                        "FROM wallet_behavior_metrics WHERE wallet = ?",
                        (src_wallet.lower(),),
                    ).fetchone()
                    z_row = _bc.execute(
                        "SELECT z_score FROM wallet_category_zscore WHERE wallet = ? AND category = ?",
                        (src_wallet.lower(), cat),
                    ).fetchone()
                finally:
                    try: _bc.close()
                    except Exception: pass
                if bm_row and bm_row[0]:
                    logger.warning(
                        "[LIVE][FARMER_GATE] BLOCK %s wallet=%s — flagged farmer/wash",
                        sig_type, src_wallet[:14],
                    )
                    try:
                        from trading_platform.polymarket.decision_trace import trace as _dt
                        _dt(signal=signal, gate="LIVE_FARMER_GATE", passed=False,
                            detail=f"wallet={src_wallet[:14]}", surface="live",
                            db_path=self._db_path)
                    except Exception:
                        pass
                    return self._block(signal, f"farmer/wash wallet {src_wallet[:14]} blocked")
                # C3 (2026-07-07): anti-survivorship copyable-CI gate.
                # is_copyable_ci = roi_lower_95 > 0 AND n >= 20. SHADOW by
                # default — traces pass/fail for the promotion review but
                # never blocks; COPYABLE_CI_ENFORCE=1 blocks for real. Do
                # NOT flip enforcement before the N6/C1+C2 copyability
                # verdict (roadmap L127). Real 0x leaders only — synthetic
                # sources (phase_b_resolution_decay etc.) are skipped.
                if src_wallet.startswith("0x"):
                    from trading_platform.polymarket.wallet_behavior_metrics import (
                        copyable_ci_verdict,
                    )
                    _ci_row = (bm_row[2], bm_row[3], bm_row[4]) if bm_row else None
                    _ok, _why = copyable_ci_verdict(_ci_row, src_wallet)
                    _enforce = os.environ.get(
                        "COPYABLE_CI_ENFORCE", "0").lower() in ("1", "true", "yes")
                    try:
                        from trading_platform.polymarket.decision_trace import trace as _dt
                        _dt(signal=signal,
                            gate="LIVE_COPYABLE_CI" if _enforce else "LIVE_COPYABLE_CI_SHADOW",
                            passed=_ok,
                            value=float(bm_row[4]) if bm_row and bm_row[4] is not None else None,
                            threshold=0.0,
                            detail=f"wallet={src_wallet[:14]} {_why}",
                            surface="live", db_path=self._db_path)
                    except Exception:
                        pass
                    if not _ok:
                        if _enforce:
                            logger.warning(
                                "[LIVE][COPYABLE_CI] BLOCK %s wallet=%s %s",
                                sig_type, src_wallet[:14], _why,
                            )
                            return self._block(
                                signal, f"copyable-CI gate: {src_wallet[:14]} {_why}")
                        logger.info(
                            "[LIVE][COPYABLE_CI] WOULD_BLOCK %s wallet=%s %s (shadow)",
                            sig_type, src_wallet[:14], _why,
                        )
                if z_row and z_row[0] is not None and float(z_row[0]) >= 1.0:
                    z_boost = min(1.4, 1.0 + 0.15 * (float(z_row[0]) - 1.0))
                    if z_boost > 1.0:
                        old_conf = confidence
                        confidence = min(0.99, confidence * z_boost)
                        signal["confidence"] = confidence
                        logger.info(
                            "[LIVE][Z_SPECIALIST] %s in %s z=%.2f conf %.2f→%.2f",
                            src_wallet[:14], cat, float(z_row[0]), old_conf, confidence,
                        )
                if bm_row and bm_row[1] is not None:
                    p90 = float(bm_row[1])
                    if p90 >= 0.10:
                        old_conf = confidence
                        confidence = min(0.99, confidence * 1.15)
                        signal["confidence"] = confidence
                        logger.info(
                            "[LIVE][CONVICTION] %s p90=%.1f%% conf %.2f→%.2f",
                            src_wallet[:14], p90 * 100, old_conf, confidence,
                        )
                    elif p90 < 0.02:
                        old_conf = confidence
                        confidence = max(0.05, confidence * 0.85)
                        signal["confidence"] = confidence
                        logger.info(
                            "[LIVE][MECHANICAL] %s p90=%.1f%% conf %.2f→%.2f",
                            src_wallet[:14], p90 * 100, old_conf, confidence,
                        )
            except Exception as exc:
                logger.debug("[LIVE] behavioral metrics lookup failed: %s", exc)

        # 0e. Wallet override gate. Layer-5 attribution can auto-demote
        # wallets whose 30d live PnL drops below -$10 on n>=5
        # (wallet_attribution.py --apply populates wallet_overrides).
        # Closes the attribution feedback loop. Fail-open if table missing.
        if src_wallet:
            try:
                from trading_platform.polymarket.db_connection import get_connection as _gc
                _woconn = _gc(self._db_path)
                try:
                    _wo = _woconn.execute(
                        "SELECT tier_override, reason FROM wallet_overrides "
                        "WHERE wallet = ? LIMIT 1",
                        (src_wallet,),
                    ).fetchone()
                finally:
                    try: _woconn.close()
                    except Exception: pass
                if _wo and (_wo[0] or "").upper() == "DEMOTED":
                    return self._block(
                        signal,
                        f"wallet auto-demoted: {src_wallet[:14]} ({(_wo[1] or '')[:60]})"
                    )
            except Exception as _woexc:
                logger.debug("wallet_overrides check failed (proceeding): %s", _woexc)

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
        # Specialist boost on size: if source wallet qualifies, 2x the
        # Kelly-derived size (still capped by phase1_cap above).
        if signal.get("is_specialist_source") and size_usd > 0:
            boosted = min(phase1_cap, size_usd * 2.0)
            if boosted > size_usd:
                logger.info("[LIVE][SPECIALIST_BOOST] size $%.2f → $%.2f", size_usd, boosted)
                size_usd = boosted
        # 1a-ii. Earliness boost + collapsed-form gate (Phase 2, 2026-07-02).
        # Paper has applied the [0.7, 1.3] earliness multiplier since
        # 04-24; live never did. Additionally: a wallet at the 0.70-0.75
        # floor means its recent EWMA WR is far below lifetime — lifetime
        # WR is survivorship-biased and this wallet is likely "was smart
        # money". Don't copy it live at all.
        _e_wallet = signal.get("wallet") or signal.get("source_wallet") or ""
        # Leader-form machinery (earliness, crowding, per-leader budget)
        # only applies to REAL on-chain leaders. Synthetic signal labels
        # (phase_b_resolution_decay, confluence_det, velocity_detector…)
        # are one shared "wallet" per signal — treating them as a leader
        # made LEADER_MAX_OPEN=2 cap the entire resolution_decay lane at
        # 2 concurrent positions (277 blocks in the hour after the
        # 2026-07-05 halt recovery), and the $10 leader loss-pause would
        # have frozen the whole signal. Synthetic lanes are risk-bounded
        # by their own stake tiers, category gates and the kill switch.
        if _e_wallet and _e_wallet.startswith("0x"):
            try:
                from trading_platform.polymarket.wallet_earliness import get_earliness_boost
                _eboost = float(get_earliness_boost(
                    _e_wallet, (signal.get("category") or ""), db_path=self._db_path))
            except Exception:
                _eboost = 1.0
            if _eboost <= 0.75:
                _reason = (f"earliness gate: {_e_wallet[:14]} recent form collapsed "
                           f"(boost {_eboost:.2f} <= 0.75)")
                self._record_attempt(signal, 0.0, None, None,
                                     dry_run=self.DRY_RUN, status="blocked",
                                     error_msg=_reason)
                return self._result(False, reason=_reason)
            if _eboost != 1.0 and size_usd > 0:
                logger.info("[LIVE][EARLINESS] %s in %s ×%.2f",
                            _e_wallet[:14], signal.get("category"), _eboost)
                size_usd = min(phase1_cap, size_usd * _eboost)
            # 1a-iii. Crowding discount (Phase 2 item 4, 2026-07-02).
            # Heavily-copied whales → we're the marginal late copier;
            # shrink size instead of paying full spread for pre-moved
            # prices. [0.5, 1.0]; fail-open to 1.0.
            try:
                from trading_platform.polymarket.wallet_copy_graph import get_crowding_discount
                _crowd = float(get_crowding_discount(_e_wallet))
            except Exception:
                _crowd = 1.0
            if _crowd < 1.0 and size_usd > 0:
                logger.info("[LIVE][CROWDING] %s ×%.2f (heavily copied leader)",
                            _e_wallet[:14], _crowd)
                size_usd = size_usd * _crowd
            # 1a-iv. Per-leader budget (2026-07-02, state-of-the-art parity).
            # Each copied wallet gets bounded capital and an auto-pause:
            #   - max concurrent open live copies of one leader
            #   - rolling-window realized loss on their copies pauses new ones
            # Prevents one decaying leader from bleeding the whole book the
            # way wallet_reversal's sources did (-$48 before the KILL).
            try:
                _max_open_per_leader = int(os.environ.get("LEADER_MAX_OPEN", "2"))
                _leader_loss_pause = float(os.environ.get("LEADER_LOSS_PAUSE_USD", "10"))
                from trading_platform.polymarket.db_connection import get_connection as _lgc
                _lc = _lgc(self._db_path)
                try:
                    _lrow = _lc.execute(
                        """SELECT
                             SUM(CASE WHEN exit_ts IS NULL THEN 1 ELSE 0 END),
                             COALESCE(SUM(CASE WHEN exit_ts >= ?
                                          THEN realized_pnl ELSE 0 END), 0)
                           FROM live_trades
                           WHERE dry_run = 0
                             AND LOWER(COALESCE(signal_wallet, '')) = ?""",
                        (int(time.time()) - 14 * 86400, _e_wallet.lower()),
                    ).fetchone()
                finally:
                    try: _lc.close()
                    except Exception: pass
                _open_n = int((_lrow or [0, 0])[0] or 0)
                _pnl_14d = float((_lrow or [0, 0])[1] or 0)
                if _open_n >= _max_open_per_leader:
                    return self._block(
                        signal,
                        f"leader budget: {_e_wallet[:12]} already has "
                        f"{_open_n} open copies (max {_max_open_per_leader})",
                    )
                if _pnl_14d <= -_leader_loss_pause:
                    return self._block(
                        signal,
                        f"leader paused: {_e_wallet[:12]} copies lost "
                        f"${-_pnl_14d:.2f} in 14d (pause at ${_leader_loss_pause:.0f})",
                    )
            except Exception as _lbexc:
                logger.debug("leader-budget check failed (proceeding): %s", _lbexc)
        if size_usd <= 0:
            return self._block(signal, f"Kelly says no edge for {sig_type}")

        # 1b. Stale price guard — abort if signal is old or price moved
        fired_at = signal.get("fired_at") or 0
        age_sec = time.time() - fired_at
        if age_sec > 900:
            return self._block(signal, f"Signal too old ({age_sec/60:.0f}m)")
        # 1b-ii. Stale WHALE-TRADE guard (2026-07-02). fired_at is when OUR
        # engine fired; whale_trade_ts is when the whale actually filled.
        # First day of latency instrumentation showed p90 detection latency
        # of 10+ DAYS — the poller emits fresh-looking signals for whale
        # trades made long ago (fired_at=now, so the 900s gate passes).
        # Copying a days-old whale entry is not copy-trading, it's buying
        # whatever the market already repriced. 30min allows the normal
        # detection path (p50 ≈ 9min today) while cutting the tail.
        MAX_WHALE_TRADE_AGE_SEC = 1800
        _wt_ts = signal.get("whale_trade_ts") or 0
        if _wt_ts and (time.time() - float(_wt_ts)) > MAX_WHALE_TRADE_AGE_SEC:
            _wt_age_min = (time.time() - float(_wt_ts)) / 60
            self._record_attempt(signal, 0.0, None, None,
                                 dry_run=self.DRY_RUN, status="blocked",
                                 error_msg=f"whale trade too old ({_wt_age_min:.0f}m)")
            return self._result(
                False,
                reason=f"whale trade too old ({_wt_age_min:.0f}m > 30m): "
                       "market has already repriced; not a copy",
            )

        # 1c. Favorite guard. The data backing this changed.
        # Pre-Apr-18 (when set at >=0.50): only YES side with ep≥0.7 was the
        # kill zone (15% WR), not the 0.5-0.7 mid band (35% WR ≈ break-even).
        # Post-Apr-24: tightened analysis on 8d cohort showed:
        #   long_shot   <0.30:  74 trades, 50% WR, +$67  ← amplify
        #   underdog 0.30-0.50: 41 trades, 39% WR, +$5   ← keep
        #   mid       0.50-0.70: 53 trades, 36% WR, +$2  ← keep, tight
        #   favorite  0.70-0.85: 39 trades, 15% WR, -$6  ← block
        # Lifting the live cap from 0.50 → 0.65 expands the live-eligible
        # entry band from ~50% of signals to ~85%. NO-side blocking is
        # handled separately via EXCLUDE_SIGNAL_SIDE.
        want_yes = (signal.get("direction") or "BUY").upper() == "BUY"
        raw_price = signal.get("entry_price") or signal.get("price")
        try:
            entry_px = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            entry_px = None

        # 1c-i. Favorite guard + per-signal BUY entry ceilings.
        # NOTE 2026-07-02: these definitions were deleted by the 2026-05-12
        # SELL-only commit while their live-price checks (section 4 below)
        # survived — so when BUY was re-enabled for resolution_decay on
        # 2026-05-19, every BUY reaching section 4 raised NameError instead
        # of being risk-checked. Restored from commit 0a57525.
        # Favorite band data (8d cohort, Apr-24): favorite 0.70-0.85 = 15% WR,
        # -$6; live cap set at 0.65.
        FAVORITE_BLOCK_PRICE = 0.65
        # Per-signal BUY entry ceiling (tighter than FAVORITE_BLOCK_PRICE).
        # whale_entry_filtered BUY 0.50-0.65: 5W/26L, -$18 PnL.
        _BUY_ENTRY_CEILINGS: dict[str, float] = {
            "whale_entry_filtered": 0.50,
        }
        _sig_buy_ceil = _BUY_ENTRY_CEILINGS.get(sig_type)
        if want_yes and entry_px is not None and entry_px >= FAVORITE_BLOCK_PRICE:
            logger.info(
                "[LIVE][FAV_GATE] BLOCK %s BUY@%.3f (>= %.2f) — copy-trade tail risk",
                sig_type, entry_px, FAVORITE_BLOCK_PRICE,
            )
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate="LIVE_FAV_GATE", passed=False,
                    value=entry_px, threshold=FAVORITE_BLOCK_PRICE,
                    detail=f"BUY@{entry_px:.3f}", surface="live",
                    db_path=self._db_path)
            except Exception:
                pass
            return self._result(False, reason=f"BUY at {entry_px:.2f} (>= {FAVORITE_BLOCK_PRICE:.2f}): tail-risk blocked")
        if want_yes and _sig_buy_ceil is not None and entry_px is not None and entry_px >= _sig_buy_ceil:
            return self._result(
                False,
                reason=f"BUY@{entry_px:.3f} >= {sig_type} mid-market ceiling {_sig_buy_ceil:.2f}",
            )

        # 2026-05-12: SELL-only mode.
        # 2026-05-19: surgical BUY re-enablement for resolution_decay only —
        # the one signal that passed the rigorous criteria:
        #   n=50 resolved paper, WR=64%, ex-top-3 EV=+$1.99/trade.
        # Source alpha (time-decay mispricing of near-resolution markets) is
        # uncorrelated to whale-flow signals — adds real diversification.
        # Discovery tier ($1 cap) applies until n>=15 live resolved.
        # Category gate: science is hard-blocked (paper n=11, WR=18%, -$34).
        #
        # BUY re-entry criteria for other signals (apply quarterly):
        #   1. n>=50 resolved paper trades for the signal in allowed categories
        #   2. WR>=45% (not lottery-ticket-dependent)
        #   3. EV>+$0.25/trade EXCLUDING top-3 individual wins
        #      (rejects lottery-ticket distributions like politics BUY which
        #       had n=23, ex-top-3 EV=-$0.70 driven by one +$49.85 outlier)
        #   4. Add signal to BUY_REENABLED_SIGNALS; gate categories explicitly
        #   5. Discovery tier handles the first ~15 live trades at $1 cap
        BUY_REENABLED_SIGNALS = {"resolution_decay"}
        # Per-signal hard-block category list (paper data clearly negative).
        BUY_BLOCKED_CATEGORIES = {
            "resolution_decay": {"science"},
        }
        if want_yes:
            if sig_type not in BUY_REENABLED_SIGNALS:
                _reason = f"BUY suspended for {sig_type} — only resolution_decay re-enabled"
                # 2026-05-20: observability — record blocked BUY attempts so we
                # can audit which signals are being routed to live. Yesterday's
                # promotion of resolution_decay produced 0 visible attempts;
                # without a paper trail we can't tell where rejection happens.
                self._record_attempt(signal, 0.0, None, None,
                                     dry_run=self.DRY_RUN, status="blocked",
                                     error_msg=_reason)
                return self._result(False, reason=_reason)
            _blocked_cats = BUY_BLOCKED_CATEGORIES.get(sig_type, set())
            _sig_cat = (signal.get("category") or "").lower()
            if _sig_cat in _blocked_cats:
                _reason = f"BUY {sig_type} blocked in category {_sig_cat}"
                self._record_attempt(signal, 0.0, None, None,
                                     dry_run=self.DRY_RUN, status="blocked",
                                     error_msg=_reason)
                return self._result(False, reason=_reason)
            # Minimum live fill price for re-enabled BUY signals. Paper
            # accepts [0.05, 0.40] but live fillable band is [0.10, 0.85]
            # — below 0.10, ask-side books are too thin for reliable
            # fills (the bitcoin-79k market kept retrying for hours at
            # 0.052 because the signal generator emits but live never
            # fills). Block here with a clear reason instead of failing
            # silently in CLOB depth checks.
            BUY_LIVE_MIN_ENTRY = 0.10
            if entry_px is not None and entry_px < BUY_LIVE_MIN_ENTRY:
                _reason = (f"{sig_type} BUY entry_px={entry_px:.3f} < "
                           f"{BUY_LIVE_MIN_ENTRY:.2f}: below live fillable floor")
                self._record_attempt(signal, 0.0, None, None,
                                     dry_run=self.DRY_RUN, status="blocked",
                                     error_msg=_reason)
                return self._result(False, reason=_reason)
            # resolution_decay BUY reaching this point — log loudly so we can
            # see it cross the gate during normal operation.
            logger.info(
                "[LIVE][BUY_REENABLED] %s passing BUY gate, cat=%s entry_px=%.3f",
                sig_type, _sig_cat or "(unset)", entry_px if entry_px is not None else -1,
            )

        # Block SELL entries where YES is near certainty in either direction.
        # Lower bound: YES < 5¢ → buying NO at 95¢+ means one YES-resolution
        #   wipes the entire stake; need 50+ wins to recover.
        # Upper bound: YES > 95¢ → buying NO at < 5¢ means:
        #   (a) orders rarely fill (29 timeouts in 7d), and
        #   (b) the market is already near-resolved YES — NO has no upside.
        NEAR_CERTAINTY_SELL_BLOCK_LO = 0.05
        NEAR_CERTAINTY_SELL_BLOCK_HI = 0.95
        if entry_px is not None and entry_px < NEAR_CERTAINTY_SELL_BLOCK_LO:
            return self._result(
                False,
                reason=f"SELL@YES={entry_px:.3f} < {NEAR_CERTAINTY_SELL_BLOCK_LO:.2f}: near-certainty black-swan risk",
            )
        if entry_px is not None and entry_px > NEAR_CERTAINTY_SELL_BLOCK_HI:
            return self._result(
                False,
                reason=f"SELL@YES={entry_px:.3f} > {NEAR_CERTAINTY_SELL_BLOCK_HI:.2f}: near-resolved YES, NO has no upside",
            )

        # 2026-06-18: per-signal SELL entry floor. Some signals only have edge
        # on clearly-overpriced YES; their mid-range entries are near-coinflip
        # full-stake bets with no measured edge. specialist_entry SELL by entry
        # band (180d resolved):
        #     YES < 0.70:  n=5,  -$15.72  (every sub-band negative)
        #     YES >= 0.70: n=15, +$19.11  (avg +$1.27)
        # — vs whale_entry_filtered SELL, which is profitable across ALL bands
        # and is intentionally NOT floored. Revisit per-signal as n grows.
        SELL_MIN_YES_ENTRY = {"specialist_entry": 0.70}
        _sell_floor = SELL_MIN_YES_ENTRY.get(sig_type)
        if (_sell_floor is not None and entry_px is not None
                and entry_px < _sell_floor):
            _reason = (f"{sig_type} SELL@YES={entry_px:.3f} < {_sell_floor:.2f}: "
                       f"no measured edge below floor (mid-range -EV)")
            self._record_attempt(signal, 0.0, None, None,
                                 dry_run=self.DRY_RUN, status="blocked",
                                 error_msg=_reason)
            return self._result(False, reason=_reason)

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
            # 2026-04-24: structured drop-reason categorization. Previously
            # every kill-switch block logged the same WARNING/DEBUG, making
            # `grep` impossible to aggregate by cause. Now emits a fixed
            # KS_BLOCK code ({DISABLED|MIN_RESOLVED|EV|WR|ENV|EMERGENCY|UNKNOWN})
            # so we can `grep "[KS_BLOCK:" | sort | uniq -c` to see the
            # systemic vs one-off blockers at a glance.
            reason_str = str(ks.reason or "")
            r = reason_str.lower()
            if "polymarket_live_enabled" in r:
                code = "ENV"
                level = logger.debug
            elif "emergency" in r or "stop" in r:
                code = "EMERGENCY"
                level = logger.warning
            elif "min_resolved" in r or "minimum resolved" in r or "n=" in r and "<" in r:
                code = "MIN_RESOLVED"
                level = logger.warning
            elif "ev" in r and ("negative" in r or "<" in r or "below" in r or "not positive" in r or "no measured edge" in r):
                code = "EV"
                level = logger.warning
            elif "wr" in r and ("low" in r or "<" in r or "below" in r):
                code = "WR"
                level = logger.warning
            elif "disabled" in r or "not enabled" in r or "excluded" in r:
                code = "DISABLED"
                level = logger.warning
            else:
                code = "UNKNOWN"
                level = logger.warning
            level("[KS_BLOCK:%s] %s — %s", code, sig_type, reason_str[:200])
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate=f"KS_BLOCK_{code}", passed=False,
                    detail=reason_str[:120], surface="live", db_path=self._db_path)
            except Exception:
                pass
            self._record_attempt(signal, size_usd, None, None, dry_run=self.DRY_RUN, status="blocked", error_msg=ks.reason)
            return self._result(False, reason=ks.reason)

        # 3. Resolve YES/NO token_ids (Gamma)
        # ── CRITICAL: A SELL signal means "buy NO token", not "sell YES token".
        # Polymarket CLOB always uses side=BUY and chooses YES/NO via token_id.
        # Previous code hardcoded tids[0] (YES) + side=BUY, which executed
        # the wrong direction on every SELL signal. Fixed 2026-04-14.
        direction = "BUY" if want_yes else "SELL"
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
                return self._block(signal, f"No token_ids for {condition_id[:20]}", size_usd)
            token_id = tids[0] if want_yes else tids[1]
        if not token_id:
            return self._block(signal, f"No {outcome_label} token_id for {condition_id[:20]}", size_usd)

        # 4. Current price + liquidity check (ON THE TOKEN WE'RE BUYING)
        # Polymarket's /midpoint endpoint always returns the YES-token market
        # probability regardless of which token_id is queried. For SELL signals
        # (buying NO token), flip to get the actual NO-token price.
        # 2026-07-08: /midpoint quotes the QUERIED token in its own space
        # post-migration (probed; the pre-migration "always YES-space"
        # behavior is gone). token_id here is the side we'd trade, so the
        # mid IS the held-token current price for both directions.
        _held_mid = self._clob.get_mid_price(token_id)
        if _held_mid is None:
            return self._block(signal, "Could not fetch current price from CLOB", size_usd)
        current_price = _held_mid
        _yes_mid = _held_mid if want_yes else max(0.01, 1.0 - _held_mid)
        # P6: stash decision-time prices on the signal dict — _block() passes
        # the SAME dict into _record_attempt, so every later gate records the
        # market context it saw for free (incl. rejects, which the execution
        # comparator has never been able to see). Held-token space.
        signal["_mid_at_decision"] = current_price
        signal["_yes_mid_at_decision"] = _yes_mid

        # Live-price BUY gate: catches signals where entry_price was None/0 so
        # the FAVORITE_BLOCK check above didn't fire, but the live market is already
        # expensive (e.g. Bitcoin-5min buying YES at 0.99 — any tiny reversal = loss).
        if want_yes and current_price >= FAVORITE_BLOCK_PRICE:
            return self._result(
                False,
                reason=f"BUY at live price {current_price:.2f} (>= {FAVORITE_BLOCK_PRICE:.2f}): tail-risk blocked",
            )
        # Live-price check for per-signal ceiling (mirrors the signal-price check above).
        if want_yes and _sig_buy_ceil is not None and current_price >= _sig_buy_ceil:
            return self._result(
                False,
                reason=f"BUY at live price {current_price:.3f} >= {sig_type} mid-market ceiling {_sig_buy_ceil:.2f}",
            )

        # Stale-price guard: compare signal's predicted price against the live
        # CLOB mid. Use an absolute 0.10 drift threshold rather than relative
        # so the gate is meaningful on both cheap (0.05) and expensive (0.90)
        # tokens — 5% relative was ±0.0025 at 0.05, useless for drift detection.
        entry_price = float(signal.get("entry_price") or signal.get("price") or 0)
        # Max-chase bound (2026-07-02). The whale's edge is a few cents;
        # paying more than MAX_CHASE beyond THEIR fill price hands the whole
        # edge (and more) to the spread. Adverse-direction only — favorable
        # drift (token cheaper than the whale paid) is welcome. Also passed
        # into place_market_order as a hard ceiling so a thin book cannot
        # fill us deeper than the gate approved (the 0.815→0.99 bug class).
        MAX_CHASE = float(os.environ.get("POLYMARKET_MAX_CHASE", "0.04"))
        _order_max_price: float | None = None
        if entry_price > 0:
            entry_in_token_frame = entry_price if want_yes else (1.0 - entry_price)
            if entry_in_token_frame > 0:
                drift = abs(current_price - entry_in_token_frame)
                if drift > 0.10:
                    return self._result(
                        False,
                        reason=(f"Price drifted {drift:.2f} since signal fired "
                                f"(signal={entry_in_token_frame:.3f} now={current_price:.3f})"),
                    )
                adverse = current_price - entry_in_token_frame
                if adverse > MAX_CHASE:
                    return self._result(
                        False,
                        reason=(f"price chased {adverse:.2f} beyond signal fill "
                                f"({entry_in_token_frame:.3f}→{current_price:.3f}, "
                                f"max {MAX_CHASE:.2f}) — edge already paid away"),
                    )
                _order_max_price = max(0.01, min(0.99, entry_in_token_frame + MAX_CHASE))

        # Liquidity guard: check FILLABLE orderbook depth at or below our
        # limit price.
        #
        # 2026-05-22 bug fix: the previous check summed ALL top-5 asks
        # regardless of price. Yesterday 5 of 8 SELL attempts had depth
        # values of $50k-$298k but still zero-filled — the depth was at
        # prices ABOVE our limit. Polymarket's CLOB FOK semantics need
        # depth AT OR BELOW our limit price. Without that filter, the
        # check is meaningless.
        #
        # For SELL (we buy NO): our limit is current_price (NO mid).
        # Asks at price <= current_price * (1 + slack) are reachable.
        # We allow 5% slack so a market that's 1¢ above mid still counts.
        book = self._clob.get_order_book(token_id)
        asks = book.get("asks") or []
        # Sort by price ascending (best ask first)
        try:
            asks_sorted = sorted(asks, key=lambda a: float(a.get("price", 1.0)))
        except Exception:
            asks_sorted = asks
        # All-asks depth (legacy metric, kept for telemetry)
        all_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0))
                        for a in asks_sorted[:10])
        # Fillable depth: asks at price <= current_price × 1.05
        _limit_price = float(current_price) * 1.05
        fillable_asks = [a for a in asks_sorted
                         if float(a.get("price", 1)) <= _limit_price]
        fillable_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0))
                              for a in fillable_asks)
        signal["_ask_depth"] = fillable_depth
        signal["_ask_depth_total"] = all_depth
        # P6: book snapshot at decision time — spread_paid is computable on
        # EVERY attempt from here on, including the depth block right below.
        try:
            signal["_best_ask_at_decision"] = (
                float(asks_sorted[0]["price"]) if asks_sorted else None)
            _bids = book.get("bids") or []
            signal["_best_bid_at_decision"] = (
                float(_bids[0]["price"]) if _bids else None)
            if signal.get("_best_ask_at_decision") is not None \
                    and signal.get("_best_bid_at_decision") is not None:
                signal["_spread_at_decision"] = (
                    signal["_best_ask_at_decision"] - signal["_best_bid_at_decision"])
        except (TypeError, ValueError, KeyError):
            pass
        if fillable_depth < size_usd * 1.5:
            _reason = (f"Thin fillable {outcome_label} liquidity: "
                       f"${fillable_depth:.1f} at price<={_limit_price:.3f} "
                       f"(need >${size_usd * 1.5:.1f}, total book ${all_depth:.0f})")
            logger.info("[LIVE][DEPTH_BLOCK] %s %s", sig_type, _reason)
            return self._block(signal, _reason, size_usd)

        # 2026-04-28: per-signal DRY_RUN check + real-trade hard cap.
        # 2026-05-02: cap is now ladder-driven AND slice-scaled.
        # Higher-EV (signal × category) slices get full ladder cap;
        # marginal slices get conservative fraction.
        # 2026-05-03: pass category + wallet_tier so _is_dry_run_for can
        # promote signals proven in the 3-dim table to live execution.
        _sig_wallet_tier = signal.get("wallet_tier") or signal.get("wallet_tier_at_fire")
        is_dry = self._is_dry_run_for(sig_type, category=cat, wallet_tier=_sig_wallet_tier)
        runtime_cap = self._live_real_cap(signal_type=sig_type, category=cat)
        if not is_dry and size_usd > runtime_cap:
            logger.warning(
                "[LIVE][REAL_CAP] %s clamping size $%.2f → $%.2f (ladder cap)",
                sig_type, size_usd, runtime_cap,
            )
            size_usd = runtime_cap

        # 2026-05-12: per-(signal × direction) sizing correction.
        # Bridges the Brier-0.32 gap between predicted confidence and
        # realized WR until the upstream calibration is fixed. Read from
        # signal_sizing_multipliers (refreshed daily by
        # scripts/update_sizing_multipliers.py). Applied AFTER ladder cap
        # so the cap stays hard; under-confident signals can still go up
        # to the cap but no further.
        #
        # 2026-06-18: skip the multiplier for any slice the per-slice
        # isotonic curve already calibrates — confidence (and therefore
        # Kelly's size) is now corrected directly upstream, so applying the
        # multiplier on top would double-correct. The multiplier stays as a
        # fallback only for slices without a live per-slice curve. As the
        # multiplier window fills with per-slice-calibrated confidence it
        # would drift to ~1.0 anyway; this just removes the transient.
        from trading_platform.polymarket.isotonic_calibration import (
            per_slice_curve_active,
        )
        _per_slice_on = per_slice_curve_active(
            sig_type, (signal.get("direction") or "").upper() or None,
            self._db_path,
        )
        if not is_dry and _per_slice_on:
            logger.info(
                "[LIVE][CALIB_MULT] %s %s — skipped (per-slice curve active; "
                "avoids double-correction)",
                sig_type, signal.get("direction", "?"),
            )
        if not is_dry and not _per_slice_on:
            try:
                from trading_platform.polymarket.db_connection import get_connection as _gc
                _mc = _gc(self._db_path)
                try:
                    _mr = _mc.execute(
                        """SELECT multiplier, actual_wr, mean_confidence, updated_at
                             FROM signal_sizing_multipliers
                            WHERE signal_type = ? AND direction = ?""",
                        (sig_type, signal.get("direction", "BUY").upper()),
                    ).fetchone()
                finally:
                    try: _mc.close()
                    except Exception: pass
                # 2026-07-02: multipliers expire. The 07-02 review showed
                # 2.0x amplification still applied on 331h- and 1192h-old
                # stats because the refresh (n>=15 in 30d) couldn't
                # requalify any slice — staleness must decay to neutral,
                # not persist as leverage on evidence that no longer exists.
                MULT_MAX_AGE_SEC = 7 * 86400
                _mult_fresh = bool(
                    _mr and _mr[3]
                    and (time.time() - float(_mr[3])) <= MULT_MAX_AGE_SEC
                )
                if _mr and _mr[0] and not _mult_fresh:
                    logger.info(
                        "[LIVE][CALIB_MULT] %s %s ×%.2f IGNORED — stale "
                        "(%.0fh old > %dh max); sizing at 1.0x",
                        sig_type, signal.get("direction", "BUY"),
                        float(_mr[0]),
                        (time.time() - float(_mr[3] or 0)) / 3600,
                        MULT_MAX_AGE_SEC // 3600,
                    )
                if _mr and _mr[0] and _mult_fresh:
                    mult = float(_mr[0])
                    pre = size_usd
                    size_usd = min(runtime_cap, size_usd * mult)
                    if abs(mult - 1.0) > 0.05:
                        logger.info(
                            "[LIVE][CALIB_MULT] %s %s ×%.2f (WR=%.0f%% conf=%.0f%%) "
                            "$%.2f→$%.2f",
                            sig_type, signal.get("direction", "BUY"), mult,
                            float(_mr[1] or 0) * 100, float(_mr[2] or 0) * 100,
                            pre, size_usd,
                        )
            except Exception as _mex:
                logger.debug("[LIVE][CALIB_MULT] lookup failed: %s", _mex)

        logger.info(
            "[LIVE%s] %s %s $%.2f @ %.3f | conf=%.0f%% | %s",
            "(DRY)" if is_dry else "(REAL)",
            sig_type, outcome_label, size_usd, current_price,
            confidence * 100, (signal.get("question") or "")[:40],
        )

        # 5a. CLOB minimum size check: Polymarket requires at least 5 shares.
        # At price P, cost = 5 × P. If stake is below minimum, bump to minimum
        # UNLESS the probation cap is lower — in that case skip entirely rather
        # than submitting a guaranteed-fail order (was generating ~17 error rows/day).
        if not is_dry:
            clob_min_usd = max(5.0 * current_price, 1.0)
            if size_usd < clob_min_usd:
                cap = ks.probation_cap or clob_min_usd
                if clob_min_usd > cap:
                    return self._result(
                        False,
                        reason=(f"CLOB min ${clob_min_usd:.2f} (5 shares @ {current_price:.3f}) "
                                f"exceeds probation cap ${cap:.2f} — skipping"),
                    )
                logger.info(
                    "[LIVE] stake $%.2f bumped to CLOB min $%.2f (5 shares @ %.3f)",
                    size_usd, clob_min_usd, current_price,
                )
                size_usd = clob_min_usd

        # 5b. DRY RUN: log + record + return
        if is_dry:
            self._record_attempt(
                signal, size_usd, current_price, None,
                dry_run=True, status="dry_run", error_msg=None,
            )
            return self._result(
                True, mode="dry_run", size_usd=size_usd,
                filled_price=current_price,
                reason=f"DRY RUN — would BUY {outcome_label} token (no order submitted)",
            )

        # 5c. Dedup gate: one live position per (condition_id, direction).
        # Two-layer protection:
        #   (a) Module-level per-market lock closes the race window where
        #       concurrent signals both read "no open position" before either
        #       INSERT lands — was the primary cause of PSG (×15) / US-Iran (×6).
        #   (b) DB query covers ALL non-exited rows including error/blocked, so a
        #       failed first order still blocks the next signal on the same market.
        #       Previously the `status NOT IN ('error','blocked')` exclusion was
        #       letting every failed retry sail through the gate.
        _dedup_direction = signal.get("direction", "BUY")
        _dedup_key = f"{condition_id}:{_dedup_direction}" if condition_id else None
        _dedup_lock_obj = None
        if _dedup_key:
            with _DEDUP_LOCKS_META:
                _dedup_lock_obj = _DEDUP_LOCKS.setdefault(_dedup_key, threading.Lock())
            if not _dedup_lock_obj.acquire(blocking=False):
                return self._result(
                    False,
                    reason=f"concurrent entry in-flight: {_dedup_key[:30]}",
                )
        try:
            if condition_id:
                try:
                    from trading_platform.polymarket.db_connection import get_connection as _gc
                    _dconn = _gc(self._db_path)
                    try:
                        # Real (submitted/matched) open positions dedup
                        # permanently. error/blocked rows dedup only for a
                        # 2h cooldown: they still stop same-cycle retry
                        # storms (the PSG ×15 incident this gate was built
                        # for), but a transient failure no longer bans the
                        # market forever — on 2026-07-05 the book-ordering
                        # bug error'd 59 entries and the permanent dedup
                        # then blocked every retry AFTER the bug was fixed
                        # (error rows never get an exit_ts).
                        _dup = _dconn.execute(
                            """SELECT 1 FROM live_trades
                               WHERE condition_id = ? AND direction = ? AND dry_run = 0
                                 AND exit_ts IS NULL
                                 AND (status NOT IN ('error', 'blocked')
                                      OR attempted_at >= ?)
                               LIMIT 1""",
                            (condition_id, _dedup_direction,
                             int(time.time()) - 7200),
                        ).fetchone()
                    finally:
                        try: _dconn.close()
                        except Exception: pass
                    if _dup:
                        return self._result(
                            False,
                            reason=f"duplicate: open {_dedup_direction} on {condition_id[:20]}",
                        )
                except Exception as _derr:
                    logger.warning("[LIVE] dedup check failed — blocking: %s", _derr)
                    return self._block(signal, "dedup check error — blocked", size_usd)

                # Topic concentration cap. See MAX_OPEN_PER_TOPIC docstring.
                # Stems share across question variants (e.g. all Musk tweet-count
                # markets collapse to one stem); we cap open positions per stem
                # so one author's behavior can't correlated-loss the whole pool.
                _stem = _topic_stem(signal.get("question"))
                if _stem and len(_stem) >= 8:
                    try:
                        from trading_platform.polymarket.db_connection import get_connection as _gc
                        _tconn = _gc(self._db_path)
                        try:
                            # Count open SELL-direction matches with the same stem.
                            # We rebuild the stem in SQL with a LIKE prefix match
                            # using the first 4 stem words — Postgres can't easily
                            # run the regex normalization, so we just LIKE-match
                            # on the leading raw text and accept some false negatives.
                            _prefix_words = " ".join(_stem.split()[:4])
                            _row = _tconn.execute(
                                """SELECT COUNT(*) FROM live_trades
                                   WHERE dry_run = 0 AND exit_ts IS NULL
                                     AND direction = ?
                                     AND LOWER(question) LIKE ?""",
                                (_dedup_direction, f"{_prefix_words}%"),
                            ).fetchone()
                        finally:
                            try: _tconn.close()
                            except Exception: pass
                        _open_cnt = int(_row[0] or 0) if _row else 0
                        if _open_cnt >= MAX_OPEN_PER_TOPIC:
                            return self._result(
                                False,
                                reason=f"topic_cap: {_open_cnt} open {_dedup_direction} on '{_stem[:40]}'",
                            )
                    except Exception as _terr:
                        logger.warning("[LIVE] topic_cap check failed (continuing): %s", _terr)

                # R2 (2026-07-07): cluster DOLLAR cap — the enforcement gap
                # portfolio_risk.assess() computed but nothing read. Blocks
                # the trade that would push its topic cluster past 20% of
                # bankroll (marginal-delta). Cached 60s; degraded snapshots
                # fail OPEN with a LOUD alert (never the silent-warning
                # pattern of the neighbors — an erroring risk gate must be
                # heard); breaches fail CLOSED. N7's complementary-leg gate
                # below stays separate: a dollar cap does not subsume two
                # small locked-loss legs.
                try:
                    from trading_platform.polymarket.portfolio_risk import (
                        check_cluster_cap, record_open as _cluster_record_open,
                        CLUSTER_GATE_ENFORCE as _CC_ENFORCE,
                    )
                    _cc = check_cluster_cap(signal.get("question"), size_usd,
                                            db_path=self._db_path)
                    if _cc.get("degraded"):
                        logger.error("[LIVE][CLUSTER_GATE] DEGRADED — "
                                     "proceeding: %s", _cc.get("reason"))
                        global _CLUSTER_GATE_LAST_ALERT
                        _now_alert = time.time()
                        if _now_alert - _CLUSTER_GATE_LAST_ALERT > 600:
                            _CLUSTER_GATE_LAST_ALERT = _now_alert
                            try:
                                from trading_platform.polymarket.telegram_alerts import get_alerter
                                get_alerter().send_pipeline_alert(
                                    "cluster_gate", _cc.get("reason") or "degraded",
                                    level="error")
                            except Exception:
                                pass
                    elif not _cc.get("allowed"):
                        try:
                            from trading_platform.polymarket.decision_trace import trace as _dt
                            _dt(signal=signal,
                                gate="LIVE_CLUSTER_CAP" if _CC_ENFORCE
                                else "LIVE_CLUSTER_CAP_SHADOW",
                                passed=False,
                                value=_cc.get("projected_frac"),
                                threshold=0.20,
                                detail=(_cc.get("reason") or "")[:200],
                                surface="live", db_path=self._db_path)
                        except Exception:
                            pass
                        if _CC_ENFORCE:
                            return self._block(signal, _cc["reason"], size_usd)
                        logger.info("[LIVE][CLUSTER_GATE][SHADOW] would-block: %s",
                                    _cc.get("reason"))
                except Exception as _cerr:
                    logger.error("[LIVE][CLUSTER_GATE] gate errored — "
                                 "proceeding (fail-open): %s", _cerr)

                # Per-EVENT exposure cap (2026-07-06). The topic-stem cap
                # can't see that "Spread: Belgium (-1.5)", "Will Belgium
                # win..." and "US vs. Belgium: O/U 3.5" are ONE football
                # match — on 7/6 a single game carried 6 correlated
                # positions ($30), including mutually exclusive outcomes
                # whose combined YES cost exceeded $1 (guaranteed loss at
                # resolution). Group by the Gamma parent-event slug cached
                # in markets.event_slug and cap open live positions per
                # event. Markets with no cached event_slug pass (cap
                # degrades to topic-stem behavior until refresh catches up).
                try:
                    _max_per_event = int(os.environ.get("MAX_OPEN_PER_EVENT", "2"))
                    from trading_platform.polymarket.db_connection import get_connection as _gc
                    _econn = _gc(self._db_path)
                    try:
                        _ev_row = _econn.execute(
                            "SELECT event_slug FROM markets WHERE condition_id = ?",
                            (condition_id,),
                        ).fetchone()
                        _ev_slug = _ev_row[0] if _ev_row and _ev_row[0] else None
                        if _ev_slug:
                            # Polymarket splits busy games across two event
                            # pages ("fifwc-usa-bel-2026-07-06" and
                            # "...-more-markets") — same real-world event,
                            # so normalize the suffix away before grouping.
                            _ev_base = _ev_slug.replace("-more-markets", "")
                            _ev_cnt = _econn.execute(
                                """SELECT COUNT(*) FROM live_trades lt
                                    JOIN markets m ON m.condition_id = lt.condition_id
                                   WHERE lt.dry_run = 0 AND lt.exit_ts IS NULL
                                     AND lt.status IN ('submitted','live','matched')
                                     AND REPLACE(m.event_slug, '-more-markets', '') = ?""",
                                (_ev_base,),
                            ).fetchone()
                            _ev_open = int(_ev_cnt[0] or 0) if _ev_cnt else 0
                            if _ev_open >= _max_per_event:
                                return self._block(
                                    signal,
                                    f"event cap: {_ev_open} open on event "
                                    f"'{_ev_slug[:40]}' (max {_max_per_event})",
                                )
                    finally:
                        try: _econn.close()
                        except Exception: pass
                except Exception as _eerr:
                    logger.warning("[LIVE] event_cap check failed (continuing): %s", _eerr)

                # Complementary-leg guaranteed-loss block (roadmap N7). Two
                # mutually-exclusive YES legs on one event can each be
                # individually fine yet TOGETHER lock a loss: at resolution
                # exactly one outcome pays 1.0, so if the summed YES cost per
                # share across our open exclusive legs exceeds 1.0 we are
                # guaranteed to lose. The event-cap gate (position count) does
                # NOT catch this — two small legs pass it. (7/6 six-leg
                # football incident: combined YES cost 1.02 = locked loss.)
                # BUY-only: a SELL/NO entry is not part of the YES sum.
                if want_yes:
                    try:
                        _cyes_max = float(os.environ.get("COMPLEMENTARY_YES_MAX", "1.0"))
                        from trading_platform.polymarket.db_connection import get_connection as _cgc
                        _cconn = _cgc(self._db_path)
                        try:
                            _cslug_row = _cconn.execute(
                                "SELECT event_slug FROM markets WHERE condition_id = ?",
                                (condition_id,),
                            ).fetchone()
                            _cslug = _cslug_row[0] if _cslug_row and _cslug_row[0] else None
                            if _cslug:
                                _open_yes, _n_legs = _open_event_yes_cost(
                                    _cconn, condition_id, _cslug)
                                _combined = _open_yes + float(current_price or 0)
                                if _n_legs > 0 and _combined > _cyes_max + 1e-9:
                                    return self._block(
                                        signal,
                                        f"complementary_loss: YES cost {_combined:.3f} > "
                                        f"{_cyes_max:.2f} across {_n_legs + 1} open exclusive "
                                        f"legs on event '{_cslug[:40]}' (guaranteed loss)",
                                    )
                        finally:
                            try: _cconn.close()
                            except Exception: pass
                    except Exception as _cerr:
                        logger.warning("[LIVE] complementary_loss check failed (continuing): %s", _cerr)

            # 6. LIVE — submit. Polymarket CLOB side is always BUY; YES vs NO
            # is determined by token_id.
            if not self._clob.is_configured:
                return self._block(signal, "CLOB not configured — set POLYMARKET_API_KEY etc.", size_usd)

            # Aggression routing: SELL direction (buying NO token) and wallet_reversal
            # use aggressive pricing for guaranteed fills. Passive bidding on thin NO
            # books almost never fills within the 75s escalation window — wallet_reversal
            # had 86 errors vs 21 fills on SELL. Time-sensitive signals can't afford
            # the passive→mid→aggressive escalation delay.
            _aggression = (
                "aggressive"
                if (not want_yes or sig_type == "wallet_reversal")
                else "passive"
            )

            # Both BUY and SELL use place_market_order (GTC).
            # place_limit_order's passive→mid→aggressive escalation cancels after 30s;
            # thin books (YES or NO) produced 60+ "order timed out" / FOK errors/week.
            # GTC posts at ask+tick and returns "live" immediately — the position monitor
            # reconciles actual token balance on exit. For near-resolution SELL entries
            # (YES≈0.999) the GTC order sits until resolved or cancelled; no more error
            # status polluting stats from thin near-resolution NO books.
            order_result: OrderResult = self._clob.place_market_order(
                token_id=token_id, side="BUY", size_usdc=size_usd,
                price_hint=current_price,
                max_price=_order_max_price,
                # P4: reuse the depth-check book (re-fetched inside if >10s
                # old or an error-dict without a freshness stamp).
                book=book,
            )
            self._record_attempt(
                signal, size_usd, current_price, order_result,
                dry_run=False,
                status=order_result.status if order_result.success else "error",
                error_msg=order_result.error_msg,
                token_id=token_id,
            )
            if order_result.success:
                fill = order_result.filled_price or current_price
                logger.info("[LIVE] order placed: %s @ %.3f", order_result.order_id, fill)
                # R2: bump the cached cluster so back-to-back signals in one
                # burst see this exposure before the 60s cache refresh.
                try:
                    from trading_platform.polymarket.portfolio_risk import record_open
                    record_open(signal.get("question"), size_usd)
                except Exception as _rerr:
                    logger.debug("[CLUSTER_GATE] record_open failed: %s", _rerr)
                # LOUD Telegram alert — live fills get their own format so they
                # stand out from paper trade notifications.
                try:
                    from trading_platform.polymarket.telegram_alerts import get_alerter
                    alerter = get_alerter()
                    question = (signal.get("question") or "")[:60]
                    alerter._send(
                        f"\U0001f6a8 <b>LIVE TRADE EXECUTED</b> \U0001f6a8\n\n"
                        f"<b>{sig_type.upper()}</b> | {outcome_label} @ {fill:.3f}\n"
                        f"Stake: <b>${size_usd:.2f}</b>\n"
                        f"Market: {question}\n"
                        f"Order: <code>{order_result.order_id or 'n/a'}</code>\n"
                        f"Confidence: {confidence:.0%} | "
                        f"{'PROBATION $5 cap' if ks.probation_cap is not None else 'FULL KELLY'}\n"
                        f"\n\U0001f4b0 Bankroll: ${live_bankroll:.0f}",
                        disable_notification=False,  # LOUD — sound + vibration
                    )
                except Exception:
                    pass
                return self._result(
                    True, mode="live", order_id=order_result.order_id,
                    size_usd=size_usd, filled_price=order_result.filled_price,
                )
            else:
                err = order_result.error_msg or ""
                logger.error("[LIVE] order failed: %s", err)
                # Cache geo-restricted markets so we don't retry them.
                if "Trading restricted" in err and condition_id:
                    self._restricted_conditions.add(condition_id)
                    logger.info("[LIVE] cached restricted condition %s", condition_id[:16])
                return self._result(False, reason=err)
        finally:
            if _dedup_lock_obj is not None:
                try: _dedup_lock_obj.release()
                except RuntimeError: pass

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

    def _load_restricted_conditions(self) -> set[str]:
        """Load condition_ids that returned 403 Trading restricted from history."""
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection(self._db_path)
            try:
                rows = conn.execute(
                    "SELECT DISTINCT condition_id FROM live_trades"
                    " WHERE error_msg LIKE '%Trading restricted%'"
                    "   AND condition_id IS NOT NULL AND condition_id != ''",
                ).fetchall()
                return {r[0] for r in rows}
            finally:
                try: conn.close()
                except Exception: pass
        except Exception:
            return set()

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
        token_id: str | None = None,
    ) -> None:
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection(self._db_path)
            now_ts = int(time.time())
            fill_price = None
            shares = None
            # Use `is not None` so a 0.0 avgFilledPrice doesn't silently skip.
            if order_result and order_result.filled_price is not None:
                fill_price = float(order_result.filled_price) or None  # keep None if 0.0
                effective_price = fill_price or entry_price
                if effective_price and size_usd:
                    direction = signal.get("direction", "BUY").upper()
                    if direction == "SELL":
                        # fill_price is the YES-space price; NO token price = 1 - fill_price.
                        # Shares = USDC spent / NO token price so the exit sell targets
                        # the correct quantity (was using fill_price as denominator →
                        # ~100x under-count on near-resolution SELL entries).
                        no_price = max(1.0 - float(effective_price), 0.01)
                        shares = float(size_usd) / no_price
                    else:
                        shares = float(size_usd) / max(float(effective_price), 0.01)
            try:
                _sig_price_raw = signal.get("entry_price") or signal.get("price")
                _sig_price = float(_sig_price_raw) if _sig_price_raw is not None else None
                # expected_price = our target (signal entry).
                # slippage = |fill - expected| / expected  (legacy unsigned; kept for back-compat gate).
                # slippage_signed = adverse-direction slippage; positive = bad.
                #   BUY:  (fill - expected) / expected   (paid more than target)
                #   SELL: (expected - fill) / expected   (sold below target)
                # slippage_cost_usd = |fill - expected| × shares (always nonneg, $ terms)
                _expected = float(entry_price) if entry_price is not None else None
                _direction_for_slip = (signal.get("direction") or "BUY").upper()
                if fill_price is not None and _expected is not None and _expected > 0:
                    _delta = float(fill_price) - _expected
                    _slippage = round(abs(_delta) / _expected, 6)
                    if _direction_for_slip == "SELL":
                        _slippage_signed = round(-_delta / _expected, 6)
                    else:
                        _slippage_signed = round(_delta / _expected, 6)
                    _slippage_cost_usd = round(abs(_delta) * float(shares or 0), 4) if shares else None
                else:
                    _slippage = None
                    _slippage_signed = None
                    _slippage_cost_usd = None
                _fill_time_ms = (
                    order_result.fill_time_ms
                    if order_result and order_result.fill_time_ms is not None
                    else None
                )
                # Resolve src_wallet from any of the keys the signal engine
                # uses. The original `source_wallet`/`signal_wallet` lookup
                # missed every smart-money signal (which sets `wallet`) —
                # 2/397 trades populated before this fix.
                _src_wallet = (
                    signal.get("wallet")
                    or signal.get("source_wallet")
                    or signal.get("signal_wallet")
                )
                _wallet_tier = (
                    signal.get("wallet_tier")
                    or signal.get("wallet_tier_at_fire")
                )
                _fired_at = signal.get("fired_at") or None
                _resolution_ts = signal.get("_resolution_ts")
                _ask_depth = signal.get("_ask_depth")
                # P6: decision-time market context. Held-token cents; entries
                # are always a CLOB BUY of the held token, so positive =
                # adverse for both directions. spread_paid_c populates on
                # every attempt with a book snapshot INCLUDING rejects — the
                # information the execution comparator has never seen.
                _mid_dec = signal.get("_mid_at_decision")
                _best_ask_dec = (
                    (order_result.best_ask_at_submit if order_result else None)
                    or signal.get("_best_ask_at_decision")
                )
                _best_bid_dec = (
                    (order_result.best_bid_at_submit if order_result else None)
                    or signal.get("_best_bid_at_decision")
                )
                _book_target = order_result.target_price if order_result else None
                _slippage_c = (
                    round((float(fill_price) - float(_mid_dec)) * 100, 4)
                    if fill_price is not None and _mid_dec is not None else None
                )
                _spread_paid_c = (
                    round((float(_best_ask_dec) - float(_mid_dec)) * 100, 4)
                    if _best_ask_dec is not None and _mid_dec is not None else None
                )
                # Phase-2 latency measurement: whale's fill → this attempt.
                _whale_ts = signal.get("whale_trade_ts") or None
                _detect_latency = (
                    float(now_ts - int(_whale_ts))
                    if _whale_ts and int(_whale_ts) <= now_ts
                    else None
                )
                conn.execute(
                    """INSERT INTO live_trades
                       (attempted_at, signal_type, condition_id, question, direction,
                        confidence, confidence_raw, size_usd, entry_price, order_id, status,
                        dry_run, error_msg, token_id, side, fill_price, shares,
                        category, signal_wallet, submitted_at, filled_at, signal_price,
                        expected_price, slippage, slippage_signed, slippage_cost_usd, fill_time_ms,
                        resolution_date, ask_depth_entry, signal_fired_at, wallet_tier,
                        whale_trade_ts, detection_latency_sec, features_at_fire,
                        mid_at_decision, yes_mid_at_decision, best_ask_at_decision,
                        best_bid_at_decision, spread_at_decision, book_target_price,
                        slippage_c, spread_paid_c, source_lane)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        now_ts,
                        signal.get("signal_type"),
                        signal.get("condition_id", ""),
                        (signal.get("question") or "")[:200],
                        signal.get("direction", "BUY"),
                        signal.get("confidence"),
                        # confidence_raw: pre-calibration/pre-boost signal
                        # confidence — the value calibration must train on
                        # (audit F2). Falls back to confidence if the signal
                        # never went through the calibration wiring.
                        signal.get("confidence_raw", signal.get("confidence")),
                        float(size_usd) if size_usd is not None else None,
                        _expected,
                        order_result.order_id if order_result else None,
                        status,
                        1 if dry_run else 0,
                        error_msg,
                        token_id or signal.get("yes_token_id") or signal.get("no_token_id"),
                        signal.get("direction", "BUY"),
                        fill_price,
                        shares,
                        signal.get("category"),
                        _src_wallet,
                        now_ts,
                        now_ts if status in ("matched", "live") else None,
                        _sig_price,
                        _expected,
                        _slippage,
                        _slippage_signed,
                        _slippage_cost_usd,
                        _fill_time_ms,
                        _resolution_ts,
                        _ask_depth,
                        _fired_at,
                        _wallet_tier,
                        _whale_ts,
                        _detect_latency,
                        # N3: full point-in-time signal dict for forensic
                        # replay + future ML features. default=str tolerates
                        # any non-JSON values; order_id/paper_trade_id live in
                        # their own columns, not the blob.
                        json.dumps(signal, default=str),
                        _mid_dec,
                        signal.get("_yes_mid_at_decision"),
                        _best_ask_dec,
                        _best_bid_dec,
                        signal.get("_spread_at_decision"),
                        _book_target,
                        _slippage_c,
                        _spread_paid_c,
                        signal.get("source_lane"),
                    ),
                )
                # PG pool runs autocommit; raw sqlite (test lane) rolls back
                # on close without this.
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("live_trades record failed: %s", exc)

        # 2026-04-30: Telegram alert on the FIRST real trade. Loud + idempotent
        # — fires only if this row is dry_run=0 AND no prior real trades exist.
        # Goal: operator gets paged the moment autonomous live trading starts.
        if not dry_run:
            try:
                from trading_platform.polymarket.db_connection import get_connection as _gc2
                conn2 = _gc2(self._db_path)
                prior = conn2.execute(
                    """SELECT COUNT(*) FROM live_trades
                        WHERE dry_run = 0 AND status IN ('submitted','live','matched')""",
                ).fetchone()
                try: conn2.close()
                except Exception: pass
                is_first = (prior and int(prior[0]) <= 1)  # <=1 because we just inserted
                from trading_platform.polymarket.telegram_alerts import get_alerter
                alerter = get_alerter()
                tag = "🚨 FIRST REAL TRADE" if is_first else "💰 LIVE TRADE"
                ep_disp = f"{entry_price:.3f}" if entry_price is not None else "n/a"
                size_disp = f"${size_usd:.2f}" if size_usd is not None else "?"
                msg = (
                    f"<b>{tag}</b>\n"
                    f"signal=<code>{signal.get('signal_type')}</code> "
                    f"side={signal.get('direction','BUY')}\n"
                    f"size={size_disp} @ {ep_disp}\n"
                    f"market={(signal.get('question') or '')[:80]}\n"
                    f"status={status}"
                )
                alerter._send(msg, disable_notification=False)
            except Exception as exc:
                logger.debug("first-trade alert failed: %s", exc)

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

    def _block(
        self,
        signal: dict[str, Any],
        reason: str,
        size_usd: float = 0.0,
    ) -> dict[str, Any]:
        """Record a blocked attempt to live_trades + return _result(False).

        2026-05-21: every silent _result(False) early return now goes
        through this helper so the rejection appears in live_trades with
        a clear reason. Yesterday's recurring lesson — silent gates
        hide bugs (resolution_decay non-firing, $10 phantom losses).
        Default observability for the executor: rejected = recorded.
        """
        try:
            self._record_attempt(
                signal, size_usd, None, None,
                dry_run=self.DRY_RUN, status="blocked",
                error_msg=reason,
            )
        except Exception as exc:
            logger.debug("_block record_attempt failed (proceeding): %s", exc)
        return self._result(False, reason=reason)

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
