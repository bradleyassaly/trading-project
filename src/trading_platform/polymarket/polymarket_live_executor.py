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
import threading
import time
from typing import Any

from trading_platform.polymarket.clob_client import ClobClient, OrderResult
from trading_platform.polymarket.kelly_sizer import KellySizer
from trading_platform.polymarket.kill_switch import KillSwitch
from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)

# In-process dedup: prevents concurrent signals for the same (condition_id, direction)
# from racing past the DB check before either INSERT completes.
_DEDUP_LOCKS: dict = {}
_DEDUP_LOCKS_META = threading.Lock()


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
            if category and wallet_tier:
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
        try:
            _conn = _gc(self._db_path)
            try:
                _row = _conn.execute(
                    """SELECT outcome FROM live_trades
                        WHERE signal_type = ? AND dry_run = 0
                          AND outcome IN ('win','loss')
                          AND size_usd > 1.50
                        ORDER BY exit_ts DESC LIMIT 3""",
                    (signal_type,),
                ).fetchall()
            finally:
                try: _conn.close()
                except Exception: pass
            if len(_row) >= 3 and all((r[0] or "") == "loss" for r in _row):
                logger.warning(
                    "[LIVE][AUTO_DEMOTE] %s — 3 consecutive real losses (size>$1.50), "
                    "back to dry-run", signal_type,
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
                for col_ddl in ("outcome TEXT", "exit_ts INTEGER", "signal_price REAL"):
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
            return self._result(False, reason="condition_id in restricted cache")

        # 2026-05-05: market horizon gate. Live trades on markets resolving
        # more than 30 days out take too long to resolve, stalling the
        # kill switch's sample accumulation (need 15 resolved to reach full
        # Kelly). Skip live; paper still tracks them for signal validation.
        LIVE_MAX_HORIZON_DAYS = 30
        if condition_id:
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
                    from datetime import datetime, timezone as _tz
                    _end = datetime.fromisoformat(str(_hrow[0]).replace("Z", "+00:00"))
                    _days_out = (_end - datetime.now(tz=_tz.utc)).total_seconds() / 86400
                    if _days_out > LIVE_MAX_HORIZON_DAYS:
                        logger.debug(
                            "[LIVE] SKIP %s — market resolves in %.0f days (> %d day limit)",
                            condition_id[:16], _days_out, LIVE_MAX_HORIZON_DAYS,
                        )
                        return self._result(
                            False,
                            reason=f"market resolves in {_days_out:.0f}d (limit {LIVE_MAX_HORIZON_DAYS}d)",
                        )
            except Exception as _hex:
                logger.debug("[LIVE] horizon check failed (proceeding): %s", _hex)

        # 2026-04-25: isotonic calibration on the inbound confidence.
        # Mirrors paper executor — the same overconfidence bias affects
        # live-bound signals (Brier 0.35 → 0.24 after correction).
        # Falls back to identity if curve hasn't been fit yet.
        try:
            from trading_platform.polymarket.isotonic_calibration import apply_calibration
            raw_conf = confidence
            confidence = apply_calibration(
                confidence,
                category=(signal.get("category") or "").lower() or None,
                db_path=self._db_path,
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
            return self._result(False, reason=f"signal_type {sig_type} excluded from live")
        _sig_side = (signal.get("side") or "").upper()
        if _sig_side and (sig_type, _sig_side) in EXCLUDE_SIGNAL_SIDE:
            logger.info("[LIVE][SIDE_GATE] BLOCKED %s side=%s", sig_type, _sig_side)
            return self._result(False, reason=f"{sig_type} {_sig_side} excluded from live")
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
        _CAT_BLOCKS: dict[str, set[str]] = {
            "whale_entry_filtered": {"sports", "geopolitics", "crypto"},
            "cascade": {"sports", "geopolitics", "science"},
        }
        if cat in _CAT_BLOCKS.get(sig_type, set()):
            logger.info("[LIVE] BLOCKED %s × %s — per-signal category block", sig_type, cat)
            return self._result(False, reason=f"{sig_type} blocked in category {cat}")

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
                        "SELECT is_likely_farmer, sizing_p90_pct FROM wallet_behavior_metrics WHERE wallet = ?",
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
                    return self._result(False, reason=f"farmer/wash wallet {src_wallet[:14]} blocked")
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
        if size_usd <= 0:
            return self._result(False, reason=f"Kelly says no edge for {sig_type}")

        # 1b. Stale price guard — abort if signal is old or price moved
        fired_at = signal.get("fired_at") or 0
        age_sec = time.time() - fired_at
        if age_sec > 900:
            return self._result(False, reason=f"Signal too old ({age_sec/60:.0f}m)")

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
        FAVORITE_BLOCK_PRICE = 0.65
        raw_price = signal.get("entry_price") or signal.get("price")
        try:
            entry_px = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            entry_px = None
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

        # Per-signal BUY entry ceilings. Live data analysis (n=162 trades):
        #
        # whale_entry_filtered BUY: only the near-NO zone (YES<0.05) is profitable
        #   YES 0.00-0.05: n=16  WR=19%  EV=+$2.54  Total=+$40.68  ← keep
        #   YES 0.05-0.50: mixed  EV=-$0.47  Total=-$17.66          ← block
        #   YES 0.50+:     blocked by FAVORITE_BLOCK_PRICE already
        #   → lower ceiling 0.50 → 0.05 so only lottery-ticket BUYs get through.
        #
        # cascade/oversized_bet/wallet_reversal BUY: all negative EV, block entirely.
        #   cascade BUY:       n=14  WR=0%   EV=-$0.57  Total=-$7.95
        #   oversized_bet BUY: n=14  WR=0%   EV=-$0.73  Total=-$10.28
        #   wallet_reversal BUY: n=7 WR=29%  EV=-$0.48  Total=-$3.38
        #   Their SELL side is profitable — this is SELL-only alpha, not directional.
        #   Setting ceiling=0.0 blocks all BUY (any price ≥ 0.0).
        _BUY_ENTRY_CEILINGS: dict[str, float] = {
            "whale_entry_filtered": 0.05,
            "cascade":              0.0,
            "oversized_bet":        0.0,
            "wallet_reversal":      0.0,
        }
        _sig_buy_ceil = _BUY_ENTRY_CEILINGS.get(sig_type)
        if want_yes and _sig_buy_ceil is not None:
            if entry_px is not None and entry_px >= _sig_buy_ceil:
                return self._result(
                    False,
                    reason=f"BUY@{entry_px:.3f} >= {sig_type} mid-market ceiling {_sig_buy_ceil:.2f}",
                )

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
                return self._result(False, reason=f"No token_ids for {condition_id[:20]}")
            token_id = tids[0] if want_yes else tids[1]
        if not token_id:
            return self._result(False, reason=f"No {outcome_label} token_id for {condition_id[:20]}")

        # 4. Current price + liquidity check (ON THE TOKEN WE'RE BUYING)
        # Polymarket's /midpoint endpoint always returns the YES-token market
        # probability regardless of which token_id is queried. For SELL signals
        # (buying NO token), flip to get the actual NO-token price.
        _yes_mid = self._clob.get_mid_price(token_id)
        if _yes_mid is None:
            return self._result(False, reason="Could not fetch current price from CLOB")
        current_price = _yes_mid if want_yes else max(0.01, 1.0 - _yes_mid)

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

        # Liquidity guard: check orderbook depth on THE token we're buying
        book = self._clob.get_order_book(token_id)
        asks = book.get("asks") or []
        ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks[:5])
        if ask_depth < size_usd * 2:
            return self._result(
                False,
                reason=f"Thin liquidity on {outcome_label} (ask depth ${ask_depth:.0f} < 2x trade ${size_usd:.0f})",
            )

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
                        _dup = _dconn.execute(
                            """SELECT 1 FROM live_trades
                               WHERE condition_id = ? AND direction = ? AND dry_run = 0
                                 AND exit_ts IS NULL
                               LIMIT 1""",
                            (condition_id, _dedup_direction),
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
                    return self._result(False, reason="dedup check error — blocked")

            # 6. LIVE — submit. Polymarket CLOB side is always BUY; YES vs NO
            # is determined by token_id.
            if not self._clob.is_configured:
                return self._result(False, reason="CLOB not configured — set POLYMARKET_API_KEY etc.")

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
                # expected_price = CLOB mid at submission.
                # slippage_pct = |fill - expected| / expected (unsigned; gate = ≤2%).
                _expected = float(entry_price) if entry_price is not None else None
                _slippage = (
                    round(abs(fill_price - _expected) / _expected, 6)
                    if fill_price is not None and _expected is not None and _expected > 0
                    else None
                )
                _fill_time_ms = (
                    order_result.fill_time_ms
                    if order_result and order_result.fill_time_ms is not None
                    else None
                )
                conn.execute(
                    """INSERT INTO live_trades
                       (attempted_at, signal_type, condition_id, question, direction,
                        confidence, size_usd, entry_price, order_id, status,
                        dry_run, error_msg, token_id, side, fill_price, shares,
                        category, signal_wallet, submitted_at, filled_at, signal_price,
                        expected_price, slippage, fill_time_ms)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        now_ts,
                        signal.get("signal_type"),
                        signal.get("condition_id", ""),
                        (signal.get("question") or "")[:200],
                        signal.get("direction", "BUY"),
                        signal.get("confidence"),
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
                        signal.get("source_wallet") or signal.get("signal_wallet"),
                        now_ts,
                        now_ts if status in ("matched", "live") else None,
                        _sig_price,
                        _expected,
                        _slippage,
                        _fill_time_ms,
                    ),
                )
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
