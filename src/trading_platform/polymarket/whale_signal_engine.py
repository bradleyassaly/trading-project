"""
9-signal engine for whale trade detection.

Signal library (in conviction order):
  Tier 1: wallet_reversal, cascade, oversized_bet
  Tier 2: accumulation, market_maker_flip, convergence
  Tier 3: specialist_entry, pre_deadline_surge, whale_entry

Usage::

    from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
    engine = WhaleSignalEngine()
    signals = engine.on_whale_trade(whale_trade)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB
from trading_platform.polymarket.whale_tripwire import WhaleTrade

logger = logging.getLogger(__name__)

VALID_SIGNAL_TYPES = [
    "wallet_reversal", "cascade", "oversized_bet",
    "accumulation", "market_maker_flip", "convergence",
    "specialist_entry", "pre_deadline_surge", "whale_entry",
    # Sell/exit-aware signals (added with multi-side reconstructor)
    "whale_exit", "position_reduction", "no_position_entry",
    # Order-book + price-velocity signals (no wallet, market-derived)
    "price_velocity",
    # High-conviction insider: wallet in insider_wallets AND trade
    # ≥5× their avg size AND price ≥0.70 AND market has ≥7d life.
    # Captures Pattern A (late big favorite) strategy pattern — highest
    # observed PF in the wallet taxonomy (10–200).
    "high_conviction_insider",
]

# Signals that should never reach the paper executor. Kept very small —
# most "low edge" signals are now handled by the paper executor's
# discovery mode ($1 stakes, auto-graduate at 15 resolutions) instead
# of being hard-blocked here. Only truly untradeable types remain.
# 2026-04-25: `whale_entry` lifted from DISABLED. The original disable
# (mid-2025) was driven by noise from synthetic-monitor "wallets" that
# are now blocked upstream via `_NON_TRADEABLE`. The 30-day stats show
# `whale_entry` raw at 70% accuracy + $863 PnL on 10 resolved trades —
# the highest-EV signal in the system. The `whale_entry_filtered` fork
# remains in place; both fire and we A/B which wins on a 30-day
# rolling window via the new signal_health IC measurement.
DISABLED_SIGNAL_TYPES = {
    "price_velocity",      # synthetic wallet, 1300/day — pure noise
}

# Signals active but unproven — place paper trades at minimum stake,
# gather data for future validation. All pass through the alpha gate.
PROBATION_SIGNAL_TYPES = {
    "market_maker_flip",   # MM changes direction — no historical backtest
    "wallet_reversal",     # wallet flips side — thin data
    "pre_deadline_surge",  # activity near close — needs temporal analysis
}

# Informational signals — logged and alerted but never trade.
INFORMATIONAL_SIGNALS = {
    "whale_exit",          # warning for our positions, not actionable
    "position_reduction",  # partial exit, informational only
}

# Sports category gate moved to the paper executor (TIER1_ONLY_CATEGORIES)
# rather than hard-blocking at the signal engine level. This lets tier1
# sports signals flow through to discovery mode ($1 stakes) so we can
# validate the tier1-only thesis (+$55K/863 trades historical).
# Signal-level data shows sports at -0.06 EV overall in the fillable
# band, but sample is small (n=134) and tier1-only may be positive.
EXCLUDED_CATEGORIES: set[str] = set()

_SPORTS_PATTERNS = [
    "vs.", "vs ", "O/U ", "Spread:", "over/under",
    "NBA", "NFL", "NHL", "MLB", "MLS", "EPL",
    "Premier League", "La Liga", "Serie A", "Bundesliga",
    "Champions League", "UFC", "ATP", "WTA", "PGA",
    "NASCAR", "F1", "Grand Prix", "World Cup",
]


def _is_sports_market(category: str | None, question: str | None) -> bool:
    """True if the market is sports — excluded from trading."""
    if category and category.lower() in EXCLUDED_CATEGORIES:
        return True
    if question:
        q = question.lower()
        return any(p.lower() in q for p in _SPORTS_PATTERNS)
    return False


# Debounce: order-book and velocity signals only fire once per market every N
# minutes — they trigger from continuous polling so we don't want a flood.
ORDER_BOOK_DEBOUNCE_SECONDS = 30 * 60
PRICE_VELOCITY_DEBOUNCE_SECONDS = 30 * 60

WHALE_EXIT_THRESHOLD = 0.50         # >= 50% sold = full exit signal
POSITION_REDUCTION_THRESHOLD = 0.20  # 20-50% sold = partial reduction

MIN_TRADE_SIZE_USD = 25.0
CASCADE_WINDOW_HOURS = 8.0
CASCADE_MIN_WALLETS = 3
CONVERGENCE_WINDOW_HOURS = 2.0
CONVERGENCE_MIN_WALLETS = 2
ACCUMULATION_WINDOW_HOURS = 24.0
ACCUMULATION_MIN_GAP_SECONDS = 3600

# Reversal confluence: 3+ tier-1 wallets all flipping side on same market
REVERSAL_CONFLUENCE_MIN = 3
REVERSAL_CONFLUENCE_WINDOW_H = 4.0
# Resolution proximity: tier-1 wallet trades within N hours of market end
RESOLUTION_PROXIMITY_HOURS = 72
RESOLUTION_MIN_SIZE_USD = 75.0


class WhaleSignalEngine:
    """Generates signals from detected whale trades across 9 signal types."""

    def __init__(self, db: WalletDB | None = None, universe: Any | None = None) -> None:
        self.db = db or WalletDB()
        self.universe = universe

    def on_whale_trade(self, trade: WhaleTrade) -> dict[str, Any] | None:
        """Process a whale trade. Returns highest-conviction signal dict or None.

        A single trade can trigger multiple signals. We fire all that match
        and return the highest-conviction one.
        """
        if trade.size < MIN_TRADE_SIZE_USD:
            return None

        now_ts = int(time.time())
        signals_fired: list[dict[str, Any]] = []

        # Check signals in conviction order
        # Tier 1
        sig = self._check_wallet_reversal(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_reversal_confluence(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_oversized_bet(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Tier 2
        sig = self._check_accumulation(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_market_maker_flip(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_convergence(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Tier 3
        sig = self._check_specialist_entry(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_pre_deadline_surge(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Check cascade (needs multiple wallets)
        sig = self._check_cascade(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # High-conviction insider — Pattern A/E. Fires on the same
        # trade iff the wallet is in insider_wallets AND position is
        # ≥5× their historical avg AND entry ≥0.70 AND ≥7d to resolution.
        sig = self._check_high_conviction_insider(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Sell-side and NO-token signals (require asset/outcome awareness)
        sig = self._check_whale_exit(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_no_position_entry(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # New validated signals (v3)
        sig = self._check_late_conviction(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_tier_entry(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_resolution_proximity(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Strategy-specific alpha signals (wallet_strategy_profiles-driven)
        sig = self._check_copyable_contrarian(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        sig = self._check_strategy_specialist(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Consensus follower — 3+ leaderboard wallets already same-side in 24h
        sig = self._check_consensus_follower(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Network leader — wallet has proven downstream followers
        sig = self._check_network_leader_entry(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # News reactor — wallet trades within 1h of a ≥20pp price spike
        sig = self._check_news_reactor(trade, now_ts)
        if sig:
            signals_fired.append(sig)

        # Always fire baseline whale_entry
        base = self._fire_whale_entry(trade, now_ts)
        if base:
            signals_fired.append(base)

        if not signals_fired:
            return None

        # Return highest confidence signal
        signals_fired.sort(key=lambda s: s.get("confidence", 0), reverse=True)
        return signals_fired[0]

    # ── Signal implementations ───────────────────────────────────────────────

    def _check_wallet_reversal(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Wallet flips from one side to the opposite on same market."""
        prior = self.db.get_prior_position(trade.wallet, trade.condition_id)
        if not prior:
            return None
        prior_side = (prior.get("side") or "").upper()
        if not prior_side or prior_side == trade.side:
            return None

        confidence = min(trade.directional_win_rate * 1.3, 0.95)
        tier_mult = 1.1 if trade.wallet_tier == "tier1h" else 1.0 if trade.wallet_tier == "tier1" else 0.8
        confidence = round(min(confidence * tier_mult, 0.95), 4)

        return self._fire_signal("wallet_reversal", trade, confidence, now_ts)

    def _check_reversal_confluence(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """3+ tier-1 wallets all flipping side on same market within 4h.

        A single reversal is informative; coordinated multi-wallet reversals
        indicate a consensus shift in informed money — strong contrarian signal.
        """
        prior = self.db.get_prior_position(trade.wallet, trade.condition_id)
        if not prior or (prior.get("side") or "").upper() == trade.side:
            return None
        cutoff = now_ts - int(REVERSAL_CONFLUENCE_WINDOW_H * 3600)
        try:
            with self.db._lock:
                rows = self.db._conn.execute(
                    """SELECT DISTINCT wallet FROM market_signals
                       WHERE condition_id = ? AND signal_type = 'wallet_reversal'
                         AND direction = ? AND fired_at >= ?""",
                    (trade.condition_id, trade.side, cutoff),
                ).fetchall()
            other_reversers = {r[0] for r in rows if r[0] != trade.wallet}
        except Exception:
            return None
        total = len(other_reversers) + 1
        if total < REVERSAL_CONFLUENCE_MIN:
            return None
        extra_wallets = max(0, total - REVERSAL_CONFLUENCE_MIN)
        confidence = round(min(0.85 + 0.05 * extra_wallets, 0.95), 4)
        return self._fire_signal("reversal_confluence", trade, confidence, now_ts,
                                 extra={"confluence_wallets": total})

    def _check_resolution_proximity(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Tier-1 wallet makes a significant trade within 72h of market resolution.

        Informed money moving near resolution = late-breaking edge. Also fast
        resolution = rapid hypothesis feedback for ladder progression.
        """
        if trade.wallet_tier not in ("tier1h", "tier1"):
            return None
        if trade.size < RESOLUTION_MIN_SIZE_USD:
            return None
        end_str: str | None = None
        if self.universe:
            try:
                entry = self.universe._by_condition.get(trade.condition_id)
                if entry:
                    end_str = entry.get("end_date_iso", "")
            except Exception:
                pass
        if not end_str:
            try:
                with self.db._lock:
                    row = self.db._conn.execute(
                        "SELECT end_date_iso FROM markets WHERE condition_id = ?",
                        (trade.condition_id,),
                    ).fetchone()
                end_str = row[0] if row else None
            except Exception:
                pass
        if not end_str:
            return None
        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            hours_to_end = (end_dt - datetime.now(tz=timezone.utc)).total_seconds() / 3600
        except Exception:
            return None
        if hours_to_end <= 0 or hours_to_end > RESOLUTION_PROXIMITY_HOURS:
            return None
        prox_mult = 1.30 if hours_to_end < 24 else 1.15 if hours_to_end < 48 else 1.05
        confidence = round(min(trade.directional_win_rate * prox_mult, 0.95), 4)
        return self._fire_signal("pre_resolution_entry", trade, confidence, now_ts,
                                 extra={"hours_to_resolution": round(hours_to_end, 1)})

    def _check_oversized_bet(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Trade significantly above wallet's historical large_bet_threshold."""
        # Get wallet's large_bet_threshold from profile
        profile = self.db.get_profile(trade.wallet)
        if not profile:
            return None
        threshold = profile.get("large_bet_threshold") or 0
        if threshold <= 0 or trade.size < 50:
            return None
        if trade.size <= 2.5 * threshold:
            return None

        ratio = trade.size / threshold / 2.5
        confidence = min(trade.directional_win_rate * min(ratio, 1.2), 0.92)
        return self._fire_signal("oversized_bet", trade, round(confidence, 4), now_ts)

    def _check_accumulation(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Same wallet, same market, same side, 2+ trades spaced > 1h apart."""
        cutoff = now_ts - int(ACCUMULATION_WINDOW_HOURS * 3600)
        with self.db._lock:
            rows = self.db._conn.execute(
                """SELECT detected_at FROM wallet_alerts
                   WHERE wallet = ? AND token_id = ? AND side = ?
                     AND detected_at >= ?
                   ORDER BY detected_at DESC""",
                (trade.wallet, trade.condition_id, trade.side, cutoff),
            ).fetchall()

        if not rows:
            return None

        # Check if any prior alert is > 1h ago
        prior_ts = rows[0][0] if rows else 0
        if now_ts - prior_ts < ACCUMULATION_MIN_GAP_SECONDS:
            return None

        count = len(rows) + 1  # including current trade
        confidence = min(0.60 + 0.08 * count, 0.88)
        return self._fire_signal("accumulation", trade, round(confidence, 4), now_ts,
                                 extra={"accumulation_count": count})

    def _check_market_maker_flip(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Market maker or arb_bot takes directional position."""
        profile = self.db.get_profile(trade.wallet)
        if not profile:
            return None
        wtype = profile.get("wallet_type", "")
        if wtype not in ("market_maker", "arb_bot"):
            return None
        if trade.side != "BUY":
            return None  # Only directional buys are informative

        # Check no offsetting trade in last 1h
        cutoff = now_ts - 3600
        with self.db._lock:
            offset = self.db._conn.execute(
                """SELECT COUNT(*) FROM wallet_alerts
                   WHERE wallet = ? AND token_id = ? AND side = 'SELL'
                     AND detected_at >= ?""",
                (trade.wallet, trade.condition_id, cutoff),
            ).fetchone()[0]
        if offset > 0:
            return None

        return self._fire_signal("market_maker_flip", trade, 0.68, now_ts)

    def _check_convergence(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """2+ distinct wallets on same side within 2h."""
        cutoff = now_ts - int(CONVERGENCE_WINDOW_HOURS * 3600)
        with self.db._lock:
            rows = self.db._conn.execute(
                """SELECT DISTINCT wallet FROM market_signals
                   WHERE condition_id = ? AND direction = ?
                     AND fired_at >= ? AND signal_type = 'whale_entry'""",
                (trade.condition_id, trade.side, cutoff),
            ).fetchall()

        distinct = {r[0] for r in rows}
        if len(distinct) < CONVERGENCE_MIN_WALLETS:
            return None

        # Graduated boost by convergence count — validated by scripts/wallet_deep_dive.py:
        #   2+ wallets: WR=78%  PnL=+$347K (n=251 markets)
        #   3+ wallets: WR=75%  PnL=+$279K (n=53)
        #   4+ wallets: WR=77%  PnL=+$251K (n=13)
        #   5+ wallets: WR=90%  PnL=+$207K (n=3)
        # More convergence = harder evidence. Bigger boost rewards the rare signal.
        n_conv = len(distinct)
        if n_conv >= 5:
            boost_mult = 1.45
        elif n_conv >= 4:
            boost_mult = 1.35
        elif n_conv >= 3:
            boost_mult = 1.25
        else:
            boost_mult = 1.15  # 2-wallet convergence

        # Conviction-score divisor lowered 10.0 → 5.0 on 2026-04-14 after
        # audit showed stored conviction_scores cap at ~6.9 (actual range
        # 0-7, typical leaderboard 1-4). Old divisor under-predicted WR by
        # ~45pp across the [0.0, 0.3) confidence bucket. See
        # reports/live_execution_audit_followups_2026-04-14.md §Audit#3.
        base_conf = trade.directional_win_rate * min(trade.conviction_score / 5.0, 1.0)
        boosted = round(min(base_conf * boost_mult, 0.95), 4)

        # Count how many of the converging wallets are tier1h.
        # Backtest data (see Market Intelligence convergence backtest) shows
        # signals firing on 3+ tier1h wallets have meaningfully better win
        # rates. Surfaced as metadata only — does not modify the fired
        # confidence above (additive integration).
        tier1h_count = 0
        try:
            with self.db._lock:
                tier1h_rows = self.db._conn.execute(
                    f"""SELECT COUNT(*) FROM leaderboard
                        WHERE tier = 'tier1h' AND wallet IN ({','.join('?' * len(distinct))})""",
                    tuple(distinct),
                ).fetchone()
            tier1h_count = (tier1h_rows[0] if tier1h_rows else 0)
        except Exception:
            tier1h_count = 0

        # Update original signal
        with self.db._lock:
            orig = self.db._conn.execute(
                """SELECT id FROM market_signals
                   WHERE condition_id = ? AND signal_type = 'whale_entry'
                     AND fired_at >= ? ORDER BY fired_at ASC LIMIT 1""",
                (trade.condition_id, cutoff),
            ).fetchone()
        if orig:
            self.db.update_signal_confidence(orig[0], boosted, len(distinct))

        return self._fire_signal("convergence", trade, boosted, now_ts,
                                 extra={"converging_wallets": len(distinct),
                                        "tier1h_converging_wallets": tier1h_count})

    def _check_specialist_entry(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Domain specialist trading in their specialty category."""
        profile = self.db.get_profile(trade.wallet)
        if not profile:
            return None

        bucket = profile.get("wallet_type", "")
        cat = trade.category

        # Check if specialist in this category
        cat_wr = None
        overall_wr = profile.get("directional_win_rate") or 0
        if cat == "politics":
            cat_wr = profile.get("politics_win_rate")
        elif cat == "crypto":
            cat_wr = profile.get("crypto_win_rate")

        is_specialist = (
            bucket == "domain_specialist"
            or (cat_wr is not None and cat_wr > overall_wr + 0.05)
        )
        if not is_specialist or cat_wr is None:
            return None

        conv = profile.get("conviction_score") or 0
        # Divisor 10→5 per calibration audit (2026-04-14).
        confidence = min(cat_wr * min(conv / 5.0, 1.0), 0.80)
        return self._fire_signal("specialist_entry", trade, round(confidence, 4), now_ts)

    def _check_pre_deadline_surge(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Spike in whale activity within 48h of market close."""
        if not self.universe:
            return None

        # Get end date from universe
        entry = self.universe._by_condition.get(trade.condition_id)
        if not entry:
            return None
        end_str = entry.get("end_date_iso", "")
        if not end_str:
            return None

        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            hours_to_close = (end_dt - datetime.now(tz=timezone.utc)).total_seconds() / 3600
        except Exception:
            return None

        if hours_to_close <= 0 or hours_to_close > 48:
            return None

        activity = self.db.get_market_whale_activity(trade.condition_id, hours=6.0)
        if activity["total_trades"] < 3:
            return None

        # Check tier1 wallets in recent activity
        t1_count = sum(1 for w in activity["tier1_wallets"]
                       if w in (getattr(self, '_tripwire_tier1', set())))
        confidence = min(activity["total_trades"] / 3.0, 1.0) * 0.72
        return self._fire_signal("pre_deadline_surge", trade, round(confidence, 4), now_ts)

    def _check_late_conviction(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Top-tier wallet makes a large trade past 90% of market lifetime.

        Backtested: N=173 OOS, WR=73%, EV=+0.227, p<0.001.
        """
        # Must be a tracked-tier wallet
        if trade.wallet_tier not in ("tier1h", "tier1", "tier2"):
            return None
        # Must be in uncertain zone
        if trade.price is None or trade.price < 0.15 or trade.price > 0.85:
            return None
        # Check market lifetime percentage
        if not self.universe:
            return None
        entry = self.universe._by_condition.get(trade.condition_id)
        if not entry:
            return None
        end_iso = entry.get("end_date_iso") or entry.get("end_date")
        if not end_iso:
            return None
        try:
            from datetime import datetime, timezone
            end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
            created_iso = entry.get("created_at") or entry.get("start_date_iso")
            if created_iso:
                created_dt = datetime.fromisoformat(created_iso.replace("Z", "+00:00"))
            else:
                created_dt = end_dt  # fallback: unknown start → skip
            now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
            total_life = (end_dt - created_dt).total_seconds()
            elapsed = (now_dt - created_dt).total_seconds()
            if total_life <= 0:
                return None
            life_pct = elapsed / total_life
        except Exception:
            return None
        if life_pct < 0.90:
            return None
        # Check trade size is top-25% for this wallet (conviction).
        # avg_position_size_usdc is USDC — compare the trade's USDC
        # notional (size*price), not raw shares.
        trade_usdc = float(trade.size or 0) * float(trade.price or 0)
        profile = self.db.get_profile(trade.wallet)
        if profile:
            avg_size = profile.get("avg_position_size_usdc") or profile.get("avg_win_size_usdc") or 0
            if avg_size > 0 and trade_usdc > 0 and trade_usdc < avg_size * 0.75:
                return None
        # Confidence: higher for tier1h, boosted by life_pct closeness to 1.0
        base = 0.60 if trade.wallet_tier == "tier1h" else 0.55 if trade.wallet_tier == "tier1" else 0.50
        confidence = min(base + (life_pct - 0.90) * 3.0, 0.85)
        # Compute hours remaining + size vs avg for Telegram context
        hours_left = max(0, (end_dt.timestamp() - now_ts) / 3600)
        size_vs_avg = round(trade_usdc / avg_size, 1) if avg_size and avg_size > 0 else None
        return self._fire_signal("late_conviction", trade, round(confidence, 4), now_ts,
                                 extra={
                                     "market_life_pct": round(life_pct, 3),
                                     "hours_remaining": round(hours_left, 1),
                                     "size_vs_avg": size_vs_avg,
                                 })

    # ── Strategy-specific alpha signals ──────────────────────────────────
    #
    # These signals leverage wallet_strategy_profiles (populated by
    # wallet_strategy_observer every 12h). Instead of gating on overall
    # wallet WR/PnL, they gate on whether THIS wallet has proven alpha
    # ON THIS SPECIFIC STRATEGY. Key insight from 2026-04-14 observer
    # audit: most top-strategy wallets have NEGATIVE overall PnL — the
    # alpha is inside one strategy, masked by losses elsewhere.
    # e.g., 0x00425c has +$73K contrarian but overall just +$5K.

    # Minimum alpha bar for a wallet's strategy to be considered copyable.
    # Dual-gate: EITHER high WR OR demonstrably profitable per-trade.
    # A flipper wallet with 52% WR and +$51K PnL is a valid alpha source —
    # wins are larger than losses. A contrarian wallet with 98% WR is also
    # valid even if raw WR is what drives it. Per-trade profitability
    # (avg_pnl > 0) captures both.
    _STRATEGY_MIN_PNL = 5_000
    _STRATEGY_MIN_RESOLVED = 20    # wins + losses combined
    _STRATEGY_MIN_WR = 0.55         # primary gate
    _STRATEGY_MIN_AVG_PNL = 50      # fallback if WR gate fails: per-trade profit

    def _get_wallet_strategy_alpha(
        self, wallet: str, strategy: str,
    ) -> dict | None:
        """Return strategy stats from wallet_strategy_profiles, or None.

        Returns {n_trades, n_markets, wins, losses, net_pnl, wr, avg_pnl}.
        None if no row or below the alpha bar.
        """
        if not wallet:
            return None
        try:
            with self.db._lock:
                row = self.db._conn.execute(
                    "SELECT n_trades, n_markets, wins, losses, net_pnl_usd, avg_pnl_per_trade "
                    "FROM wallet_strategy_profiles WHERE wallet = ? AND strategy = ?",
                    (wallet, strategy),
                ).fetchone()
        except Exception:
            return None
        if not row:
            return None
        n, mkts, wins, losses, pnl, avg = row
        resolved = (wins or 0) + (losses or 0)
        if resolved < self._STRATEGY_MIN_RESOLVED:
            return None
        if (pnl or 0) < self._STRATEGY_MIN_PNL:
            return None
        wr = wins / resolved if resolved else 0
        # Pass if EITHER high WR OR positive per-trade average PnL (which
        # catches low-WR/high-payoff strategies like flipper and longshot).
        wr_ok = wr >= self._STRATEGY_MIN_WR
        avg_ok = (avg or 0) >= self._STRATEGY_MIN_AVG_PNL
        if not (wr_ok or avg_ok):
            return None
        return {
            "n_trades": n, "n_markets": mkts, "wins": wins, "losses": losses,
            "net_pnl": pnl, "wr": wr, "avg_pnl": avg,
        }

    def _check_copyable_contrarian(
        self, trade: WhaleTrade, now_ts: int,
    ) -> dict | None:
        """Wallet with proven contrarian alpha takes a contrarian position.

        Contrarian definition: BUY at price > 0.70 (betting high implied YES
        will not hit) OR SELL at price < 0.30 (betting low implied YES
        will hit against the crowd). Alpha bar: >= $5K profit + 30 resolved
        contrarian trades + WR >= 55% per wallet_strategy_profiles.

        Discovered by the 2026-04-14 strategy observer audit; candidates:
        0x00425c ($73K, 98%), 0x63c743 ($14K, 97%), 0xef43652 ($10K, 91%).
        """
        # Contrarian entry conditions
        side = (trade.side or "").upper()
        price = trade.price if trade.price is not None else 0.5
        if side == "BUY" and price <= 0.70:
            return None
        if side == "SELL" and price >= 0.30:
            return None
        # Proven alpha required
        alpha = self._get_wallet_strategy_alpha(trade.wallet, "contrarian")
        if not alpha:
            return None
        # Confidence scales with measured strategy WR (not overall WR)
        confidence = round(min(alpha["wr"] * 0.95, 0.90), 4)
        return self._fire_signal(
            "copyable_contrarian", trade, confidence, now_ts,
            extra={
                "strategy_wr": round(alpha["wr"], 3),
                "strategy_pnl_observed": round(alpha["net_pnl"], 2),
                "strategy_n_trades": alpha["n_trades"],
            },
        )

    def _check_strategy_specialist(
        self, trade: WhaleTrade, now_ts: int,
    ) -> dict | None:
        """Generalized version of copyable_contrarian for other strategies.

        At signal time we infer which strategy THIS TRADE matches:
          - longshot: price < 0.15
          - high_conviction: trade USDC notional >= 3x wallet avg_position_size_usdc
          - flipper: wallet has prior SELL on same market in last 24h
                    (or BUY after prior SELL)

        For the matched strategy, require the wallet has proven alpha
        (via _get_wallet_strategy_alpha). Skips if trade matches 'contrarian'
        since copyable_contrarian already covers that.
        """
        price = trade.price if trade.price is not None else 0.5
        strategy_match: str | None = None

        # 1) longshot — price < 0.15
        if price < 0.15:
            strategy_match = "longshot"

        # 2) high_conviction — USDC notional >= 3x wallet's own avg (USDC)
        if strategy_match is None:
            profile = self.db.get_profile(trade.wallet)
            if profile:
                avg_size = profile.get("avg_position_size_usdc") or 0
                trade_usdc = float(trade.size or 0) * float(price)
                if avg_size > 0 and trade_usdc >= 3 * avg_size:
                    strategy_match = "high_conviction"

        # 3) flipper — this trade flips the wallet's side on the same market
        if strategy_match is None:
            try:
                with self.db._lock:
                    prior = self.db._conn.execute(
                        "SELECT side FROM wallet_trades "
                        "WHERE wallet = ? AND condition_id = ? AND timestamp < ? "
                        "ORDER BY timestamp DESC LIMIT 1",
                        (trade.wallet, trade.condition_id, now_ts),
                    ).fetchone()
            except Exception:
                prior = None
            if prior and prior[0] and prior[0] != trade.side:
                # Was there a recent opposite-side trade within 24h?
                cutoff = now_ts - 24 * 3600
                with self.db._lock:
                    has_recent = self.db._conn.execute(
                        "SELECT COUNT(*) FROM wallet_trades "
                        "WHERE wallet = ? AND condition_id = ? "
                        "AND timestamp >= ? AND side != ?",
                        (trade.wallet, trade.condition_id, cutoff, trade.side),
                    ).fetchone()
                if has_recent and has_recent[0] > 0:
                    strategy_match = "flipper"

        if strategy_match is None:
            return None
        alpha = self._get_wallet_strategy_alpha(trade.wallet, strategy_match)
        if not alpha:
            return None

        confidence = round(min(alpha["wr"] * 0.95, 0.90), 4)
        return self._fire_signal(
            "strategy_specialist", trade, confidence, now_ts,
            extra={
                "matched_strategy": strategy_match,
                "strategy_wr": round(alpha["wr"], 3),
                "strategy_pnl_observed": round(alpha["net_pnl"], 2),
                "strategy_n_trades": alpha["n_trades"],
            },
        )

    # ── News reactor (trade follows a price spike) ─────────────────────────
    _NEWS_REACTOR_WINDOW_SEC = 3600  # 1h lookback for the price move
    _NEWS_REACTOR_MIN_MOVE = 0.20    # ≥20pp absolute price range triggers

    def _check_news_reactor(
        self, trade: WhaleTrade, now_ts: int,
    ) -> dict | None:
        """Fires when a wallet trades within 1h of a price spike.

        Uses market_ticks (price-only; volume not stored) as a proxy for
        news impact. A 20pp price move in 1h is news-sized — wallets
        reacting to it are either fast to the news or exploiting
        post-move mispricing.

        Gate: only fires for known tier1/tier2 wallets with positive
        overall PnL. Without this we'd flood with noise from every
        trader that happened to trade near a move. Stricter gating
        kicks in once the strategy observer tracks a 'news_reactor'
        strategy column (future work).
        """
        tier = trade.wallet_tier
        if tier not in ("tier1", "tier1h", "tier2"):
            return None
        profile = self.db.get_profile(trade.wallet)
        if not profile or (profile.get("net_pnl_usdc") or 0) <= 0:
            return None

        cutoff = now_ts - self._NEWS_REACTOR_WINDOW_SEC
        try:
            with self.db._lock:
                rows = self.db._conn.execute(
                    "SELECT price FROM market_ticks "
                    "WHERE condition_id = ? AND timestamp BETWEEN ? AND ?",
                    (trade.condition_id, cutoff, now_ts),
                ).fetchall()
        except Exception:
            return None
        if len(rows) < 3:
            return None  # too few ticks to establish a move

        prices = [float(r[0]) for r in rows if r[0] is not None]
        if len(prices) < 3:
            return None
        price_range = max(prices) - min(prices)
        if price_range < self._NEWS_REACTOR_MIN_MOVE:
            return None

        # Confidence: scale with move magnitude + wallet's base WR
        wr = profile.get("directional_win_rate") or 0.5
        move_boost = min(price_range / 0.50, 1.0)  # 50pp caps the boost
        confidence = round(min(wr * (0.6 + 0.3 * move_boost), 0.85), 4)
        return self._fire_signal(
            "news_reactor", trade, confidence, now_ts,
            extra={
                "price_range_1h": round(price_range, 3),
                "n_ticks": len(prices),
            },
        )

    # ── Network leader (wallet has quality followers) ──────────────────────
    _NETWORK_LEADER_MIN_FOLLOWERS = 3
    _NETWORK_LEADER_MIN_LAG_MIN = 10  # filters MM-bot clusters
    # 2026-07-28: the daily wallet_copy_graph task was deleted (OOM against
    # the 53M-row wallet_trades; copy-entry strategy killed 2026-07-07), so
    # wallet_copy_relationships no longer refreshes. Relationships older than
    # this are fossil evidence — go quiet rather than fire on a frozen graph.
    # An ad-hoc rerun of the miner revives the signal automatically.
    _NETWORK_LEADER_MAX_AGE_S = 7 * 86400

    def _check_network_leader_entry(
        self, trade: WhaleTrade, now_ts: int,
    ) -> dict | None:
        """Fires when a wallet with a proven follower network enters a market.

        Uses wallet_copy_graph's `wallet_copy_relationships` — a wallet
        with 3+ downstream followers at lag > 10min is a "leader": their
        trades systematically precede other wallets' same-side entries.
        Copying the LEADER as early as possible is higher-EV than
        copying a follower because info is freshest.

        Filters:
          - Exclude MM-bot clusters (lag < 10min)
          - Require the leader has non-negative PnL (their calls are
            at least break-even — we're not copying losers)
          - Require the graph itself is fresh (computed_at within
            _NETWORK_LEADER_MAX_AGE_S) — see note above the constant
        """
        if not trade.wallet:
            return None
        try:
            with self.db._lock:
                row = self.db._conn.execute(
                    "SELECT COUNT(*) AS n_followers, AVG(avg_lag_minutes) AS avg_lag, "
                    " MAX(computed_at) AS computed_at "
                    "FROM wallet_copy_relationships "
                    "WHERE leader_wallet = ? AND avg_lag_minutes > ?",
                    (trade.wallet, self._NETWORK_LEADER_MIN_LAG_MIN),
                ).fetchone()
        except Exception:
            return None
        if not row or not row[0]:
            return None
        n_followers, avg_lag, computed_at = row[0], row[1], row[2]
        if not computed_at or now_ts - int(computed_at) > self._NETWORK_LEADER_MAX_AGE_S:
            return None
        if n_followers < self._NETWORK_LEADER_MIN_FOLLOWERS:
            return None

        # Require leader's own WR is non-negative (from profile)
        profile = self.db.get_profile(trade.wallet)
        if not profile:
            return None
        if (profile.get("net_pnl_usdc") or 0) < 0:
            return None

        # Confidence scales with follower count + uses own WR
        wr = profile.get("directional_win_rate") or 0.5
        base = 0.55 + min(0.05 * (n_followers - 3), 0.25)  # 0.55 → 0.80 as followers grow
        confidence = round(min(wr * (base + 0.15), 0.90), 4)
        return self._fire_signal(
            "network_leader_entry", trade, confidence, now_ts,
            extra={
                "n_followers": n_followers,
                "avg_follower_lag_min": round(avg_lag or 0, 1),
            },
        )

    # ── Consensus follower ─────────────────────────────────────────────────
    _CONSENSUS_WINDOW_HOURS = 24.0
    _CONSENSUS_MIN_LEADERS = 3

    def _check_consensus_follower(
        self, trade: WhaleTrade, now_ts: int,
    ) -> dict | None:
        """Fires when THIS trade joins an established leaderboard consensus.

        Distinct from ``convergence`` (2h window, queries market_signals):
          - Longer 24h lookback captures slow-forming consensus
          - Queries wallet_trades directly — leader wallets don't need to
            have had a signal fired; their raw activity counts
          - Requires 3+ distinct leaderboard wallets already same-side on
            this market BEFORE this trade
          - The current wallet does NOT count toward the threshold

        Hypothesis: by the time 3+ leaderboard wallets agree, the edge
        direction is clear — but there's still room to profit if we join
        before retail catches on. Audit with SignalResolver outcomes.
        """
        if not trade.condition_id or not trade.wallet or not trade.side:
            return None
        cutoff = now_ts - int(self._CONSENSUS_WINDOW_HOURS * 3600)
        with self.db._lock:
            rows = self.db._conn.execute(
                "SELECT DISTINCT wt.wallet FROM wallet_trades wt "
                "JOIN leaderboard l ON wt.wallet = l.wallet "
                "WHERE wt.condition_id = ? AND wt.side = ? "
                "  AND wt.timestamp BETWEEN ? AND ? "
                "  AND wt.wallet != ?",
                (trade.condition_id, trade.side, cutoff, now_ts, trade.wallet),
            ).fetchall()
        n_leaders = len(rows)
        if n_leaders < self._CONSENSUS_MIN_LEADERS:
            return None

        # Scale confidence with consensus strength; cap at 0.85
        if n_leaders >= 5:
            confidence = 0.85
        elif n_leaders == 4:
            confidence = 0.75
        else:
            confidence = 0.65
        return self._fire_signal(
            "consensus_follower", trade, confidence, now_ts,
            extra={
                "leader_count": n_leaders,
                "consensus_window_h": self._CONSENSUS_WINDOW_HOURS,
            },
        )

    def _check_tier_entry(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """S/A tier wallet enters an uncertain-zone market.

        Backtested: N=20 OOS, WR=80%, EV=+0.195, p=0.009.
        Uses wallet_category_profiles S/A tiers (per-category, not global).
        """
        if trade.price is None or trade.price < 0.25 or trade.price > 0.75:
            return None
        # Check category-specific tier from wallet_category_profiles
        with self.db._lock:
            row = self.db._conn.execute(
                "SELECT tier, win_rate FROM wallet_category_profiles WHERE wallet = ? AND category = ?",
                (trade.wallet, trade.category),
            ).fetchone()
        if not row or row[0] not in ("S", "A"):
            return None
        tier_letter = row[0]
        tier_wr = row[1] or 0.5
        confidence = min(0.55 + (tier_wr - 0.50) * 1.5, 0.85)
        if tier_letter == "S":
            confidence = min(confidence + 0.05, 0.90)
        return self._fire_signal("tier_entry", trade, round(confidence, 4), now_ts,
                                 extra={"wallet_cat_tier": tier_letter, "wallet_cat_wr": tier_wr})

    def _check_cascade(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """3+ distinct tier-1 wallets sequentially entering same side."""
        cutoff = now_ts - int(CASCADE_WINDOW_HOURS * 3600)
        with self.db._lock:
            rows = self.db._conn.execute(
                """SELECT DISTINCT wallet FROM market_signals
                   WHERE condition_id = ? AND direction = ?
                     AND fired_at >= ?""",
                (trade.condition_id, trade.side, cutoff),
            ).fetchall()

        distinct = {r[0] for r in rows}
        if len(distinct) < CASCADE_MIN_WALLETS:
            return None

        extra_wallets = max(0, len(distinct) - CASCADE_MIN_WALLETS)
        confidence = min(0.75 + 0.10 * extra_wallets, 0.95)
        return self._fire_signal("cascade", trade, round(confidence, 4), now_ts,
                                 extra={"cascade_wallets": len(distinct)})

    def _check_whale_exit(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Tier1/1h wallet sells a meaningful chunk of an open position.

        Looks up the wallet's open position from wallet_market_positions and
        compares the sell size against total_shares. >= WHALE_EXIT_THRESHOLD
        of position size = full-conviction exit; >= POSITION_REDUCTION_THRESHOLD
        but less = partial reduction (lower-confidence signal).
        """
        if (trade.side or "").upper() != "SELL":
            return None
        if trade.wallet_tier not in ("tier1h", "tier1"):
            return None

        with self.db._lock:
            row = self.db._conn.execute(
                """SELECT total_shares, side, is_fully_exited
                   FROM wallet_market_positions
                   WHERE wallet = ? AND condition_id = ?
                   ORDER BY first_entry_ts DESC LIMIT 1""",
                (trade.wallet, trade.condition_id),
            ).fetchone()
        if not row or not row[0]:
            return None
        total_shares = float(row[0])
        if total_shares <= 0:
            return None

        sold_pct = (trade.size or 0) / total_shares
        if sold_pct < POSITION_REDUCTION_THRESHOLD:
            return None

        is_full_exit = sold_pct >= WHALE_EXIT_THRESHOLD
        signal_type = "whale_exit" if is_full_exit else "position_reduction"

        # Confidence: scale with sold_pct, boost for tier1h
        base = 0.40 + min(sold_pct, 1.0) * 0.40
        tier_mult = 1.15 if trade.wallet_tier == "tier1h" else 1.0
        confidence = round(min(base * tier_mult, 0.95), 4)
        return self._fire_signal(
            signal_type, trade, confidence, now_ts,
            extra={"sold_pct": round(sold_pct, 3), "prior_position_side": row[1] or ""},
        )

    def _check_no_position_entry(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Tier1/1h wallet buys NO tokens — bearish signal that was previously
        invisible (the engine treated all BUYs as long YES).
        """
        if (trade.side or "").upper() != "BUY":
            return None
        if trade.wallet_tier not in ("tier1h", "tier1"):
            return None
        # Outcome is the human-readable name of the token being bought.
        # 'No', 'Down', 'Under' all signal a bearish position on the YES side.
        outcome = (trade.outcome or "").strip().lower()
        if outcome not in ("no", "down", "under"):
            return None

        wr = trade.directional_win_rate or 0.5
        # Divisor 10→5 per calibration audit (2026-04-14).
        conv = min((trade.conviction_score or 0) / 5.0, 1.0)
        tier_mult = 1.10 if trade.wallet_tier == "tier1h" else 1.0
        confidence = round(min(wr * conv * tier_mult * 1.05, 0.95), 4)
        if confidence < 0.10:
            return None
        return self._fire_signal(
            "no_position_entry", trade, confidence, now_ts,
            extra={"no_outcome": trade.outcome or ""},
        )

    # ── Market-derived signals (no wallet, fed by background pollers) ───────

    # Per-market last-fired timestamps for debounce
    _ob_last_fired: dict[str, int] = {}
    _vel_last_fired: dict[str, int] = {}

    def on_order_book_signal(self, anomaly: dict[str, Any]) -> dict | None:
        """Map an OrderBookMonitor anomaly into a market_signals row.

        Anomaly types map to existing signal types so downstream consumers
        (paper executor, dashboards) don't need new code paths. Debounced
        per-market to avoid floods from continuous polling.
        """
        cid = anomaly.get("condition_id") or ""
        if not cid:
            return None
        now_ts = int(time.time())
        last = self._ob_last_fired.get(cid, 0)
        if now_ts - last < ORDER_BOOK_DEBOUNCE_SECONDS:
            return None

        signal_type = "order_flow_imbalance"

        severity = anomaly.get("severity", "medium")
        base_conf = {"critical": 0.70, "high": 0.55, "medium": 0.40}.get(severity, 0.40)
        imbalance = anomaly.get("imbalance") or 0.5
        if imbalance and imbalance > 0.85:
            base_conf = min(base_conf + 0.15, 0.90)
        confidence = round(base_conf, 4)

        # Synthesize a WhaleTrade-shaped object so we can reuse _fire_signal.
        synthetic = WhaleTrade(
            wallet="order_book_monitor",
            condition_id=cid,
            side="BUY",
            price=float(anomaly.get("mid_price") or 0.5),
            size=float(anomaly.get("bid_usd") or 0),
            outcome="Yes",
            question=anomaly.get("question") or "",
            category=anomaly.get("category") or "other",
            timestamp=now_ts,
            wallet_tier="market",
            directional_win_rate=0.0,
            conviction_score=0.0,
            total_volume_usdc=0.0,
        )
        try:
            sig = self._fire_signal(
                signal_type, synthetic, confidence, now_ts,
                extra={
                    "ob_anomaly_type": anomaly.get("type"),
                    "ob_severity": severity,
                    "ob_imbalance": imbalance,
                    "ob_detail": anomaly.get("detail", "")[:200],
                },
            )
            self._ob_last_fired[cid] = now_ts
            return sig
        except Exception as exc:
            logger.debug("on_order_book_signal failed: %s", exc)
            return None

    def on_price_velocity(
        self,
        condition_id: str,
        token_id: str,
        price_change: float,
        velocity: float,
        current_price: float,
        minutes_elapsed: float,
        question: str = "",
        category: str = "other",
    ) -> dict | None:
        """Fire a price_velocity signal when a market moves >10¢ rapidly."""
        if abs(price_change) < 0.10:
            return None
        now_ts = int(time.time())
        last = self._vel_last_fired.get(condition_id, 0)
        if now_ts - last < PRICE_VELOCITY_DEBOUNCE_SECONDS:
            return None

        direction = "BUY" if price_change > 0 else "SELL"
        # 10¢ move => 0.30, 30¢ => 0.85
        confidence = round(min(abs(price_change) * 3, 0.85), 4)

        synthetic = WhaleTrade(
            wallet="velocity_detector",
            condition_id=condition_id,
            side=direction,
            price=float(current_price),
            size=0.0,
            outcome="Yes",
            question=question or "",
            category=category or "other",
            timestamp=now_ts,
            wallet_tier="market",
            directional_win_rate=0.0,
            conviction_score=0.0,
            total_volume_usdc=0.0,
        )
        try:
            sig = self._fire_signal(
                "price_velocity", synthetic, confidence, now_ts,
                extra={
                    "price_change": round(price_change, 4),
                    "velocity_per_min": round(velocity, 5),
                    "minutes_elapsed": round(minutes_elapsed, 1),
                    "token_id": token_id,
                },
            )
            self._vel_last_fired[condition_id] = now_ts
            logger.warning(
                "[VELOCITY] %s: %+.0f%% in %.0fmin at %.2f",
                question[:40], price_change * 100, minutes_elapsed, current_price,
            )
            # Velocity-spike Telegram alerts were the noisiest source in the
            # historical 5,197-alert flood. Removed per the alert-system
            # rebuild — velocity is a technical scanner, not a real wallet
            # signal, and the thesis is that it should not page anyone.
            # The skip is recorded in AlertManager so the daily digest
            # surfaces the count instead of spamming individual messages.
            try:
                from trading_platform.polymarket.alert_manager import get_alert_manager
                get_alert_manager().record_signal_skipped("velocity_spike_no_wallet")
            except Exception:
                pass
            return sig
        except Exception as exc:
            logger.debug("on_price_velocity failed: %s", exc)
            return None

    def _fire_whale_entry(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Baseline signal for any watched wallet trade."""
        wr = trade.directional_win_rate
        conv = trade.conviction_score
        # Divisor 10→5 per calibration audit (2026-04-14). Previous 10.0
        # assumed 0-10 conv_score scale; actual range is 0-7 clustered at 1-4.
        # Audit showed stored confidence at [0,0.3) had 68% actual WR — +45pp
        # under-prediction — causing Kelly to undersize winners ~30%.
        confidence = wr * min(conv / 5.0, 1.0)
        tier_mult = 1.1 if trade.wallet_tier == "tier1h" else 1.0 if trade.wallet_tier == "tier1" else 0.75
        confidence = round(min(confidence * tier_mult, 0.95), 4)

        # 2026-04-30: empirical floor for promotable signals. The Bayesian
        # gate validates whale_entry at WR=80% on n=15, yet this formula
        # scores typical fires at 0.15-0.20 (formula is wr × conv/5 ×
        # tier_mult — for wr=0.5, conv=2, tier1: 0.20). 91 of yesterday's
        # 190 whale_entry fires got dropped at CONF_FLOOR=0.25 because of
        # this. Floor at 0.30 (between empirical and formula) lifts the
        # under-scored fires above the gate without overstating confidence.
        # Limited to PROMOTABLE_SIGNALS only — Bayesian-validated set.
        if confidence < 0.30:
            confidence = 0.30

        if confidence < 0.01:
            return None

        return self._fire_signal("whale_entry", trade, confidence, now_ts)

    # ── Shared fire logic ────────────────────────────────────────────────────

    def _fire_signal(
        self, signal_type: str, trade: WhaleTrade, confidence: float,
        now_ts: int, extra: dict | None = None,
    ) -> dict[str, Any]:
        """Write signal to DB and print to stdout."""
        # Resolve YES/NO token IDs from the cached markets table so
        # downstream ExecutionGates and live-executor code have real
        # clob_token_ids to hit. Cached hits are free; misses yield None
        # and the live executor falls back to live Gamma resolution.
        yes_tid, no_tid = None, None
        try:
            from trading_platform.polymarket.markets_table import get_token_ids
            yes_tid, no_tid = get_token_ids(trade.condition_id)
        except Exception:
            pass
        want_yes = (trade.side or "").upper() == "BUY"
        trade_token_id = yes_tid if want_yes else no_tid

        # 2026-04-27: enrich with subdomain from markets table so the
        # paper executor's z-subdomain lookup + alert badges have it.
        # Cheap single-row lookup; falls back to None on miss.
        # 2026-05-27: also pull end_date_iso for a horizon pre-filter.
        # Volume forensics showed 6,010 blocks/month from
        # already_resolved + long_horizon (>30d). Pre-filtering at
        # signal source eliminates that wasted compute + log noise.
        _subcat = None
        _end_iso = None
        try:
            with self.db._lock:
                _r = self.db._conn.execute(
                    "SELECT subcategory, end_date_iso FROM markets WHERE condition_id = ?",
                    (trade.condition_id,),
                ).fetchone()
            if _r:
                _subcat = _r[0]
                _end_iso = _r[1]
        except Exception:
            pass

        # Horizon pre-filter: skip signals on markets that have already
        # resolved OR resolve more than 30d out. These ALWAYS get blocked
        # downstream (already_resolved / long_horizon gate). Saves the
        # paper + live executors from being called on dead-on-arrival
        # signals.
        # 2026-05-28: extended with Gamma fallback when local markets
        # table doesn't have end_date_iso. Yesterday's deploy cut
        # already_resolved blocks 35% but long_horizon only -0% because
        # the markets table is sparse for non-curated cids that the
        # wallet engine fires on. Gamma covers those.
        if not _end_iso:
            try:
                import requests as _req
                r = _req.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"condition_ids": trade.condition_id}, timeout=4,
                )
                data = r.json()
                m = data[0] if isinstance(data, list) and data else None
                if m and (m.get("conditionId") or "").lower() == trade.condition_id.lower():
                    _end_iso = m.get("endDate") or m.get("end_date_iso")
            except Exception:
                pass

        if _end_iso:
            try:
                from datetime import datetime, timezone as _tz
                _clean = _end_iso.replace("Z", "+00:00") if _end_iso.endswith("Z") else _end_iso
                _end = datetime.fromisoformat(_clean)
                if _end.tzinfo is None:
                    _end = _end.replace(tzinfo=_tz.utc)
                _hours_out = (_end - datetime.now(tz=_tz.utc)).total_seconds() / 3600
                if _hours_out <= 0:
                    logger.debug(
                        "[PRE_FILTER] %s skipped — market already resolved (%.1fh ago)",
                        signal_type, -_hours_out,
                    )
                    return None
                if _hours_out > 30 * 24:
                    logger.debug(
                        "[PRE_FILTER] %s skipped — market resolves in %.0fd (>30d)",
                        signal_type, _hours_out / 24,
                    )
                    return None
            except Exception:
                pass

        signal = {
            "signal_type": signal_type,
            "condition_id": trade.condition_id,
            "direction": trade.side,
            "confidence": confidence,
            "wallet": trade.wallet,
            "price": trade.price,
            "size": trade.size,
            "category": trade.category,
            "subcategory": _subcat,
            "question": trade.question,
            "wallet_tier": trade.wallet_tier,
            "directional_win_rate": trade.directional_win_rate,
            "fired_at": now_ts,
            # The whale's own fill timestamp — lets executors record
            # per-trade detection latency (whale fill → our order), the
            # Phase-2 measurement in SCALING_PLAN_2026-07-02.md.
            "whale_trade_ts": trade.timestamp,
            # P3: real delivery-lane tag (chain_direct vs poller).
            "source_lane": getattr(trade, "source_lane", "poller"),
            # Real clob token_ids (may be None if markets table not yet
            # populated for this cid). token_id is the side we'd trade;
            # yes/no ids are provided so both executors can make their
            # own choice without re-resolving.
            "token_id": trade_token_id,
            "yes_token_id": yes_tid,
            "no_token_id": no_tid,
            # end_date_iso already resolved in the pre-filter (_end_iso above)
            # — pass it so the live executor's horizon gate is a dict read and
            # skips a synchronous Gamma round-trip on the hot path (N10).
            "end_date_iso": _end_iso,
        }
        if extra:
            signal.update(extra)

        # Alpha gate (Phase 1 of the signal_analysis_clean.md rollout):
        # only let real-wallet signals through if the firing wallet has
        # proven category-specific edge. Synthetic-wallet signals
        # (velocity_detector / order_book_monitor) bypass — they're
        # gated by the fillability floor in the executor instead.
        if trade.wallet and trade.wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                from trading_platform.polymarket.alpha_scores import get_wallet_alpha
                alpha = get_wallet_alpha(
                    str(self.db._path), trade.wallet, trade.category or "other",
                )
                # Alpha as confidence multiplier for tier1/tier1h (not a hard gate).
                # Proven wallets in the category get a boost; unproven get a small
                # dampen. tier2 still requires a positive score to fire at all.
                # Rationale: tier1h are vetted by polymarket leaderboard; blocking
                # them on no-data is worse than dampening. But we should still
                # use their category track record to calibrate confidence.
                tier = trade.wallet_tier
                if alpha <= 0:
                    if tier in ("tier1h", "tier1"):
                        # No data yet — apply mild confidence dampen (0.90×)
                        signal["alpha_score"] = 0.0
                        signal["confidence"] = round(signal["confidence"] * 0.90, 4)
                        logger.info(
                            "[ALPHA_GATE] wallet %s %s no-data dampen 0.90×, signal=%s cat=%s",
                            trade.wallet[:14], tier, signal_type, trade.category,
                        )
                    else:
                        logger.info(
                            "[ALPHA_GATE] wallet %s NOT copyable in %s, SKIP %s",
                            trade.wallet[:14], trade.category, signal_type,
                        )
                        return None
                else:
                    signal["alpha_score"] = alpha
                    # Boost confidence proportional to alpha score (up to 1.15×).
                    boost = 1.0 + min(alpha, 0.5) * 0.30
                    signal["confidence"] = round(min(signal["confidence"] * boost, 0.95), 4)
                    logger.info(
                        "[ALPHA_GATE] wallet %s copyable in %s, score=%.3f boost=%.2f× signal=%s",
                        trade.wallet[:14], trade.category, alpha, boost, signal_type,
                    )
            except Exception as exc:
                logger.debug("alpha gate (engine) lookup failed: %s", exc)

        # Resolve the actual clob token_id for the side we'd trade.
        # Historical bug: this column was set to condition_id, causing
        # downstream ExecutionGates depth/spread checks to fail-open on
        # every signal (CLOB /book returns 404 for a condition_id). Fix
        # pulls from the cached markets table first; falls back to None
        # (downstream will resolve via Gamma on-demand or skip the gate).
        resolved_tid: str | None = None
        try:
            from trading_platform.polymarket.markets_table import get_token_ids
            yes_tid, no_tid = get_token_ids(trade.condition_id)
            want_yes = (trade.side or "").upper() == "BUY"
            resolved_tid = yes_tid if want_yes else no_tid
        except Exception as exc:
            logger.debug("token_id lookup failed in _fire_signal: %s", exc)

        # Write to market_signals (with question for joins)
        self.db.insert_signal(
            token_id=resolved_tid,
            market_title=trade.question[:200],
            question=trade.question[:200],
            category=trade.category,
            direction=trade.side,
            confidence=confidence,
            net_smart_volume=trade.size if trade.side == "BUY" else -trade.size,
            weighted_net_volume=trade.size * confidence,
            smart_wallet_count=1,
            top_wallet_edge=trade.directional_win_rate,
            top_wallet_address=trade.wallet,
            signal_type=signal_type,
            wallet=trade.wallet,
            price=trade.price,
            size=trade.size,
            fired_at=now_ts,
            condition_id=trade.condition_id,
            status="open",
            executed=0,
            convergence_count=extra.get("converging_wallets", 0) if extra else 0,
        )

        # Write to wallet_alerts (tier1h treated as tier 1 for alert tier number)
        tier_num = 1 if trade.wallet_tier in ("tier1", "tier1h") else 2
        self.db.insert_alert(
            wallet=trade.wallet,
            token_id=trade.condition_id,
            market_title=trade.question[:200],
            side=trade.side,
            size=trade.size,
            price=trade.price,
            wallet_edge=trade.directional_win_rate,
            wallet_type=trade.wallet_tier,
            directional_win_rate=trade.directional_win_rate,
            tier=tier_num,
            trade_ts=trade.timestamp,
            signal_fired=1,
            question=trade.question[:200],
            category=trade.category,
        )

        # Track category
        self.db.increment_category_signal(trade.category)

        # Record in signal_outcomes for EV / live-readiness tracking.
        # Non-fatal — this must never block signal processing.
        try:
            self._record_signal_outcome(signal, trade, signal_type, confidence, now_ts)
        except Exception as exc:
            logger.debug("signal_outcomes record failed: %s", exc)

        print(
            f"[SIGNAL] {signal_type} | {trade.wallet_tier.upper()} | "
            f"{trade.wallet[:10]}... | {trade.side} | "
            f"{trade.category} | {trade.question[:40]}... | "
            f"conf={confidence:.2f} | ${trade.size:.0f}"
        )

        # Paper executor — place trade for $100K bankroll system.
        # Block signals that are proven to have zero/negative edge.
        _NON_TRADEABLE = ("velocity_detector", "order_book_monitor")
        if trade.wallet in _NON_TRADEABLE:
            return signal  # log but don't trade
        # 2026-04-25: whale_entry now trades directly AND forks to the
        # filtered + insider variants (was DISABLED-only). Both compete
        # for resolved-hypothesis volume; signal_health IC measures
        # which lane is actually carrying alpha after a 30-day window.
        if signal_type == "whale_entry":
            self._maybe_fire_filtered_whale_entry(signal, trade, now_ts)
            self._maybe_fire_insider_entry(signal, trade, now_ts)
        if signal_type in DISABLED_SIGNAL_TYPES:
            print(f"[DISABLED] {signal_type} signal blocked from trading", flush=True)
            return signal  # log but don't trade
        if signal_type in INFORMATIONAL_SIGNALS:
            print(f"[INFO_ONLY] {signal_type} logged as informational, not trading", flush=True)
            return signal  # log but don't trade
        # 2026-04-25: hard sports block removed. The block was killing
        # ~80% of whale signals (cascade/network_leader_entry/market_
        # maker_flip on NBA/MLB spreads, all tagged `wallet_derived`
        # with "Spread:" in the question matched the regex). Sports is
        # actually +$7.82 over 8d at 35.8% WR with a long-shot bias —
        # net positive EV. The downstream `LIVE_TRADE_CATEGORIES` gate
        # in the paper executor already handles category filtering for
        # capital deployment, and `_execute_discovery` runs $1 stakes
        # for unproven slices. Logging the sport-classified signals so
        # we can audit, but not blocking.
        if _is_sports_market(trade.category, trade.question):
            print(
                f"[SPORTS_TAG] {signal_type} wallet={trade.wallet[:14]} "
                f"cat={trade.category} — flowing to paper executor",
                flush=True,
            )
            # fall through

        # 2026-04-24: paper-trade freeze diagnostic — log every dispatch
        # so we can see whether execute_signal reaches the paper executor.
        # Use print() not logger.info — live-collect doesn't run
        # setup_logging() so logger.info gets dropped at INFO level.
        print(
            f"[DISPATCH] {signal_type} wallet={trade.wallet[:14]} "
            f"cat={trade.category} side={trade.side} conf={confidence:.2f} "
            f"→ paper_executor",
            flush=True,
        )

        try:
            from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
            if not hasattr(self, "_paper"):
                self._paper = PolymarketPaperExecutor()
            try:
                paper_trade = self._paper.execute_signal(signal)
            except Exception as _exec_exc:
                # The wallet-stream holds this paper executor for the whole
                # process lifetime (~18h+), so its persistent psycopg3
                # connection idles out ("the connection is closed") — the
                # scheduler path never hits this because it builds a fresh
                # executor per run. This silently killed the chain-direct
                # fast-lane paper bookkeeping (48 fails / 0 placed / 20h,
                # swallowed at the outer except). Rebuild once on a
                # closed/stale connection and retry. (3rd instance of the
                # stale-persistent-connection class this week — see also
                # wallet_db.stats() and the watchdog balance check.)
                if "closed" in str(_exec_exc).lower() or "ssl" in str(_exec_exc).lower():
                    logger.warning("[SIGNAL→TRADE] paper conn stale (%s) — "
                                   "rebuilding executor + retrying", _exec_exc)
                    try:
                        self._paper = PolymarketPaperExecutor()
                        paper_trade = self._paper.execute_signal(signal)
                    except Exception as _retry_exc:
                        logger.warning("[SIGNAL→TRADE] retry after rebuild "
                                       "failed: %s", _retry_exc)
                        raise
                else:
                    raise
            if paper_trade:
                signal["paper_stake"] = paper_trade["size_usd"]
                signal["paper_trade_id"] = paper_trade["id"]
                logger.info(
                    "[SIGNAL\u2192TRADE] PLACED: %s %s %s stake=$%s wallet=%s tier=%s",
                    signal_type, trade.side, (trade.question or "")[:30],
                    paper_trade.get("size_usd"), trade.wallet[:14],
                    trade.wallet_tier,
                )
                # Mark the most recent matching market_signals row as executed
                # AND set wallet_alerts.paper_trade_fired = 1 so the
                # signal->trade link can be queried downstream.
                try:
                    with self.db._lock:
                        self.db._conn.execute(
                            """UPDATE market_signals
                               SET executed=1, stake_usd=?, paper_trade_id=?
                               WHERE condition_id=? AND signal_type=?
                                 AND fired_at=?""",
                            (paper_trade["size_usd"], paper_trade["id"],
                             trade.condition_id, signal_type, now_ts),
                        )
                        self.db._conn.execute(
                            """UPDATE signal_outcomes
                               SET paper_trade_id=?, paper_stake_usd=?
                               WHERE condition_id=? AND signal_type=?
                                 AND fired_at=?""",
                            (paper_trade["id"], paper_trade["size_usd"],
                             trade.condition_id, signal_type, now_ts),
                        )
                        self.db._conn.execute(
                            """UPDATE wallet_alerts
                               SET paper_trade_fired = 1
                               WHERE wallet = ? AND token_id = ? AND side = ?
                                 AND detected_at >= ?""",
                            (trade.wallet, trade.condition_id, trade.side,
                             now_ts - 5),
                        )
                        self.db._conn.commit()
                except Exception:
                    pass
            else:
                logger.info(
                    "[SIGNAL\u2192TRADE] SKIP: %s %s wallet=%s — executor returned None (gate/dup/price-floor)",
                    signal_type, trade.side, trade.wallet[:14],
                )
        except Exception as exc:
            logger.warning("[SIGNAL→TRADE] EXECUTOR_FAILED: %s", exc)

        # Live executor — DRY_RUN by default. Will be blocked by KillSwitch
        # unless POLYMARKET_LIVE_ENABLED=1 in .env. Purely additive:
        # failures here never affect paper trading.
        # LIVE_SIGNAL_TYPES is a code-level whitelist on top of KillSwitch.
        # 2026-04-24: expanded from {whale_entry_filtered} to include
        # the post-Apr-18 proven winners — wallet_reversal (72.7% WR YES)
        # and cascade (55.6% WR YES). Both are NO-side-gated already via
        # EXCLUDE_SIGNAL_SIDE. The previous singleton whitelist combined
        # with a too-tight YES_FAV_GATE produced 0 live trades for 7
        # days despite 1192 whale_entry_filtered signals firing.
        # 2026-04-29: added `whale_entry`. The live executor's
        # LIVE_REAL_SIGNAL_TYPES allowlist only fires real money on
        # whale_entry, but the engine here was filtering it out before
        # it ever reached the executor — guaranteeing zero real trades.
        # whale_entry has the strongest Bayesian gate verdict
        # (n=15 / WR=80% / P(acc≥55%)=0.96 per /api/ladder/status).
        # specialist_entry 2026-05-03: 84% WR / +10.9% EV (n=74).
        # tier_entry 2026-05-03: 100% WR / +32% EV (n=15, probation).
        # whale_exit 2026-05-03: 100% WR / +3.7% EV (n=16, probation).
        LIVE_SIGNAL_TYPES = {
            "whale_entry", "whale_entry_filtered",
            "wallet_reversal", "cascade",
            "specialist_entry",
            "tier_entry", "whale_exit",
            # 2026-05-03: new signals
            "reversal_confluence",    # 3+ wallets flipping same market — high conviction
            "pre_resolution_entry",   # tier-1 buying within 72h of resolution
            # 2026-05-05: oversized_bet — IC30=+0.121, +$107 paper PnL n=34.
            # Was in LIVE_REAL_SIGNAL_TYPES but missing here → 1,044 signals/week
            # fired through paper only, 0 live_trades entries. Fixed.
            "oversized_bet",
        }
        if signal_type in LIVE_SIGNAL_TYPES:
            try:
                from trading_platform.polymarket.polymarket_live_executor import PolymarketLiveExecutor
                if not hasattr(self, "_live"):
                    self._live = PolymarketLiveExecutor()
                live_result = self._live.execute(signal)
                # Always log the live decision so we have an audit trail.
                # Previously success=False was silent — for 7 days we had
                # zero live activity AND zero diagnostic logs.
                if live_result.get("success"):
                    if live_result.get("mode") == "dry_run":
                        logger.info("[LIVE_DRY] would trade: %s", live_result)
                    elif live_result.get("mode") == "live":
                        logger.warning("[LIVE_TRADE] EXECUTED: %s", live_result)
                else:
                    logger.info(
                        "[LIVE_BLOCKED] %s side=%s ep=%.3f reason=%s",
                        signal_type, signal.get("side", "?"),
                        float(signal.get("entry_price") or signal.get("price") or 0),
                        live_result.get("reason", "unknown")[:120],
                    )
            except Exception as exc:
                # Promote from debug → warning so silent failures surface.
                logger.warning("[LIVE] Executor error: %s", exc)

        # Telegram alerts — ONLY on paper/live trade placement, not raw
        # signal fires. Previously every signal fire sent a notification
        # (1900+/day). Now: only when execute_signal actually placed a trade.
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            alerter = get_alerter()
            # ALL signal-derived alerts now require paper_trade to have
            # actually placed. Previously we'd fire phone notifications for
            # every politics/geopolitics signal + every insider_entry even
            # when the trade was rejected at downstream gates — generating
            # phone spam with no actionable trade behind it. Daily digest
            # still summarises everything; loud alerts gated on placement.
            if paper_trade and not paper_trade.get("discovery"):
                alerter.send_signal(signal, paper_result=paper_trade)

                if trade.category in ("politics", "geopolitics") and trade.wallet:
                    with self.db._lock:
                        tier_row = self.db._conn.execute(
                            """SELECT tier, win_rate, monthly_pnl
                               FROM wallet_category_profiles
                               WHERE wallet = ? AND category = ?""",
                            (trade.wallet, trade.category),
                        ).fetchone()
                    if tier_row and tier_row[0] in ("S", "A", "B"):
                        alerter.send_political_whale(
                            signal,
                            tier=tier_row[0],
                            win_rate=float(tier_row[1] or 0),
                            pnl_30d=float(tier_row[2] or 0),
                        )

                if signal_type == "insider_entry":
                    alerter.send_insider_entry(signal)
        except Exception:
            pass

        return signal

    # ── signal_outcomes recorder ───────────────────────────────────────────

    def _record_signal_outcome(
        self,
        signal: dict,
        trade: WhaleTrade,
        signal_type: str,
        confidence: float,
        now_ts: int,
    ) -> None:
        """Insert a row into signal_outcomes for EV tracking. Idempotent."""
        with self.db._lock:
            self.db._conn.execute(
                """INSERT OR IGNORE INTO signal_outcomes
                    (signal_type, fired_at, condition_id, token_id, question,
                     category, direction, confidence, entry_price, wallet,
                     wallet_tier, paper_trade_id, paper_stake_usd)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    signal_type,
                    now_ts,
                    trade.condition_id,
                    signal.get("token_id"),
                    (trade.question or "")[:200],
                    trade.category,
                    trade.side,
                    float(confidence),
                    float(trade.price) if trade.price is not None else None,
                    trade.wallet,
                    trade.wallet_tier,
                    signal.get("paper_trade_id"),
                    signal.get("paper_stake") or signal.get("stake_usd"),
                ),
            )
            self.db._conn.commit()

    # ── whale_entry_filtered fork ─────────────────────────────────────────────

    def _maybe_fire_filtered_whale_entry(
        self,
        signal: dict,
        trade: "WhaleTrade",
        now_ts: int,
    ) -> None:
        """If the whale_entry wallet is a copyable archetype, fire a
        whale_entry_filtered signal that IS eligible for paper/live execution."""
        try:
            from trading_platform.polymarket.wallet_archetype import WalletArchetypeClassifier
            if not hasattr(self, "_archetype_clf"):
                self._archetype_clf = WalletArchetypeClassifier(str(self.db._path))
            wallet = trade.wallet or ""
            if not wallet:
                return
            archetype = self._archetype_clf.get_archetype(wallet)
            if not self._archetype_clf.is_copyable(wallet):
                logger.debug(
                    "[FILTERED] skip whale_entry from %s wallet %s",
                    archetype or "unknown", wallet[:14],
                )
                return

            confidence = float(signal.get("confidence") or 0)
            # Boost for specialist/conviction in geopolitics only.
            # Politics has no demonstrated positive EV on any signal type
            # (see reports/category_grouping_analysis_2026-04-12.md).
            if archetype in ("specialist", "conviction") and trade.category == "geopolitics":
                confidence = min(1.0, confidence + 0.15)

            filtered = {**signal}
            filtered["signal_type"] = "whale_entry_filtered"
            filtered["original_type"] = "whale_entry"
            filtered["wallet_archetype"] = archetype
            filtered["confidence"] = confidence

            self._fire_signal(
                "whale_entry_filtered", trade, confidence, now_ts,
                extra={"wallet_archetype": archetype, "original_type": "whale_entry"},
            )
        except Exception as exc:
            logger.debug("filtered whale_entry fork failed: %s", exc)

    # ── insider_entry fork ─────────────────────────────────────────────────

    _ELECTION_KW = frozenset({"election", "vote", "primary", "nominee", "ballot"})

    # Cache for insider wallet profiles. Refreshed when a new wallet
    # hits the check path with no cache entry — not time-based, since
    # insider_wallets is rebuilt manually.
    _insider_profile_cache: dict = {}

    _MIN_HIGH_CONV_PRICE = 0.70
    _MIN_HIGH_CONV_SIZE_MULT = 5.0
    _MIN_HIGH_CONV_TIME_TO_CLOSE_S = 7 * 24 * 3600  # 7 days

    def _check_high_conviction_insider(self, trade: "WhaleTrade", now_ts: int) -> dict | None:
        """Pattern A/E high-conviction insider bet.

        Fires iff ALL of:
          1. Wallet is in insider_wallets (uncertain-zone WR >= 60%).
          2. Trade size USDC is >= 5x the wallet's avg_trade_size.
          3. Entry price >= 0.70 (late favorite territory).
          4. Market has at least 7 days to close.

        These four constraints define the "insider loading up on a
        likely winner" pattern. Observed profit-factors of 10–200 in
        the top-3-per-category taxonomy.
        """
        try:
            price = float(trade.price or 0)
            if price < self._MIN_HIGH_CONV_PRICE:
                return None
            trade_usdc = float(trade.size or 0) * price
            if trade_usdc <= 0:
                return None

            wallet_key = (trade.wallet or "").lower()
            prof = self._insider_profile_cache.get(wallet_key)
            if prof is None:
                from trading_platform.polymarket.db_connection import get_connection
                conn = get_connection()
                try:
                    row = conn.execute(
                        "SELECT insider_score, uncertain_accuracy, avg_trade_size "
                        "FROM insider_wallets WHERE LOWER(wallet) = ?",
                        (wallet_key,),
                    ).fetchone()
                finally:
                    try: conn.close()
                    except Exception: pass
                prof = dict(zip(
                    ("insider_score", "uncertain_accuracy", "avg_trade_size"),
                    row,
                )) if row else {}
                self._insider_profile_cache[wallet_key] = prof

            if not prof:
                return None
            avg_size = float(prof.get("avg_trade_size") or 0)
            if avg_size <= 0 or trade_usdc < self._MIN_HIGH_CONV_SIZE_MULT * avg_size:
                return None

            # Time-to-close gate. Pattern A entries too close to expiry
            # are brittle — if the wallet is right 1h before close the
            # price moves instantly and we miss. Require 7+ days runway.
            close_ts = int(trade.market_end_ts or 0) if hasattr(trade, "market_end_ts") else 0
            if close_ts and (close_ts - now_ts) < self._MIN_HIGH_CONV_TIME_TO_CLOSE_S:
                return None

            # Confidence ramps with accuracy + size multiple
            acc = float(prof.get("uncertain_accuracy") or 0.5)
            size_mult = min(trade_usdc / avg_size, 20.0)
            confidence = 0.60 + (acc - 0.60) * 0.8 + min(size_mult / 20.0, 0.2)
            confidence = max(0.40, min(0.95, confidence))

            return self._fire_signal(
                "high_conviction_insider", trade, round(confidence, 4), now_ts,
                extra={
                    "insider_score": prof.get("insider_score"),
                    "insider_accuracy": acc,
                    "size_mult_vs_avg": round(size_mult, 2),
                    "avg_wallet_size_usdc": round(avg_size, 0),
                },
            )
        except Exception as exc:
            logger.debug("high_conviction_insider check failed: %s", exc)
            return None

    def _maybe_fire_insider_entry(
        self,
        signal: dict,
        trade: "WhaleTrade",
        now_ts: int,
    ) -> None:
        """Fire insider_entry if the wallet is a detected insider."""
        try:
            from trading_platform.polymarket.insider_detector import InsiderDetector
            if not hasattr(self, "_insider_det"):
                self._insider_det = InsiderDetector(str(self.db._path))

            wallet = trade.wallet or ""
            if not self._insider_det.is_insider(wallet):
                return

            price = float(trade.price or 0)
            if price < 0.15 or price > 0.85:
                return

            # Election exclusion (insiders underperform on elections)
            q = (trade.question or "").lower()
            if any(k in q for k in self._ELECTION_KW):
                return

            profile = self._insider_det.get_insider_profile(wallet) or {}
            acc = profile.get("uncertain_accuracy", 0.5)

            # Confidence: base + accuracy boost + category boosts
            confidence = 0.50 + (acc - 0.50) * 1.0
            confidence += min(profile.get("uncertain_trades", 0) / 200, 0.10)
            if trade.category == profile.get("primary_category"):
                confidence += 0.05
            if trade.category in ("geopolitics", "politics"):
                confidence += 0.05
            confidence = max(0.30, min(0.95, confidence))

            self._fire_signal(
                "insider_entry", trade, round(confidence, 4), now_ts,
                extra={
                    "insider_score": profile.get("insider_score"),
                    "insider_accuracy": acc,
                    "insider_sample": profile.get("uncertain_trades"),
                    "original_type": "whale_entry",
                },
            )
        except Exception as exc:
            logger.debug("insider_entry fork failed: %s", exc)

    # ── Resolution + queries ─────────────────────────────────────────────────

    def resolve_signal(self, signal_id: int, won: bool, pnl: float) -> None:
        """Called when a paper trade resolves."""
        with self.db._lock:
            row = self.db._conn.execute(
                "SELECT category, confidence FROM market_signals WHERE id = ?",
                (signal_id,),
            ).fetchone()
            if not row:
                return
            category = row[0]

            self.db._conn.execute(
                "UPDATE market_signals SET status = 'resolved' WHERE id = ?",
                (signal_id,),
            )
            self.db._conn.execute(
                """INSERT INTO category_performance (category, signals_fired, signals_resolved, signals_won, win_rate, total_pnl, last_updated)
                   VALUES (?, 0, 1, ?, NULL, ?, ?)
                   ON CONFLICT(category) DO UPDATE SET
                   signals_resolved = signals_resolved + 1,
                   signals_won = signals_won + ?,
                   total_pnl = total_pnl + ?,
                   win_rate = CAST(signals_won + ? AS REAL) / MAX(signals_resolved + 1, 1),
                   last_updated = ?""",
                (category, 1 if won else 0, pnl, int(time.time()),
                 1 if won else 0, pnl, 1 if won else 0, int(time.time())),
            )
            self.db._conn.commit()

    def get_category_performance(self) -> list[dict[str, Any]]:
        return self.db.get_category_performance()

    def get_recent_signals(self, hours: float = 24.0) -> list[dict[str, Any]]:
        cutoff = int(time.time()) - int(hours * 3600)
        with self.db._lock:
            rows = self.db._conn.execute(
                """SELECT id, token_id, market_title, category, direction,
                          confidence, net_smart_volume, smart_wallet_count,
                          top_wallet_edge, top_wallet_address, signal_type,
                          wallet, price, size, fired_at, condition_id, computed_at
                   FROM market_signals
                   WHERE fired_at >= ? OR (fired_at IS NULL AND computed_at >= ?)
                   ORDER BY COALESCE(fired_at, computed_at) DESC LIMIT 50""",
                (cutoff, cutoff),
            ).fetchall()
        cols = [
            "id", "token_id", "market_title", "category", "direction",
            "confidence", "net_smart_volume", "smart_wallet_count",
            "top_wallet_edge", "top_wallet_address", "signal_type",
            "wallet", "price", "size", "fired_at", "condition_id", "computed_at",
        ]
        return [dict(zip(cols, r)) for r in rows]
