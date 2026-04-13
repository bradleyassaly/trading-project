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
]

# Signals proven to have zero or negative edge in the validation data.
# They still get LOGGED (for future analysis) but are blocked from
# reaching the paper executor. See reports/signal_analysis_clean.md.
DISABLED_SIGNAL_TYPES = {
    "price_velocity",      # not a wallet signal, fictional trades
    "oversized_bet",       # negative edge on corrected data (WR 10.6%, EV -0.39)
    "whale_entry",         # break-even raw; replaced by whale_entry_filtered
    "insider_entry",       # DISABLED: sampling bias invalidates accuracy metric
    "cascade",             # by the time 5 wallets are in, info is priced in
    "convergence",         # 87.7% WR = no edge over whale_entry alone (87.5%)
    "no_position_entry",   # redundant with whale_entry (same wallet, same gate)
    "market_maker_flip",   # negative EV on corrected data (-0.057)
    "wallet_reversal",     # negative EV on corrected data (-0.033)
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

# Sports markets dominate the feed and resolve unpredictably (game outcomes).
# Even with 86.6% copyable-wallet WR, they flood the funnel and slow time-to-50.
# Exclude all sports to focus on politics/crypto/economics where thesis is strongest.
EXCLUDED_CATEGORIES = {
    "sports", "soccer", "basketball", "football", "baseball",
    "hockey", "mma", "tennis", "esports", "motorsport",
}

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

        base_conf = trade.directional_win_rate * min(trade.conviction_score / 10.0, 1.0)
        boosted = round(min(base_conf * 1.20, 0.95), 4)

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
        confidence = min(cat_wr * min(conv / 10.0, 1.0), 0.80)
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
        # Check trade size is top-25% for this wallet (conviction)
        profile = self.db.get_profile(trade.wallet)
        if profile:
            avg_size = profile.get("avg_position_size_usdc") or profile.get("avg_win_size_usdc") or 0
            if avg_size > 0 and trade.size < avg_size * 0.75:
                return None
        # Confidence: higher for tier1h, boosted by life_pct closeness to 1.0
        base = 0.60 if trade.wallet_tier == "tier1h" else 0.55 if trade.wallet_tier == "tier1" else 0.50
        confidence = min(base + (life_pct - 0.90) * 3.0, 0.85)
        return self._fire_signal("late_conviction", trade, round(confidence, 4), now_ts,
                                 extra={"market_life_pct": round(life_pct, 3)})

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
        conv = min((trade.conviction_score or 0) / 10.0, 1.0)
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

        type_map = {
            "order_book_imbalance": "accumulation",
            "sustained_imbalance":  "convergence",
            "bid_wall":             "oversized_bet",
            "ask_removal":          "market_maker_flip",
            "spread_compression":   "cascade",
        }
        signal_type = type_map.get(anomaly.get("type", ""))
        if not signal_type:
            return None

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
        confidence = wr * min(conv / 10.0, 1.0)
        tier_mult = 1.1 if trade.wallet_tier == "tier1h" else 1.0 if trade.wallet_tier == "tier1" else 0.75
        confidence = round(min(confidence * tier_mult, 0.95), 4)

        if confidence < 0.01:
            return None

        return self._fire_signal("whale_entry", trade, confidence, now_ts)

    # ── Shared fire logic ────────────────────────────────────────────────────

    def _fire_signal(
        self, signal_type: str, trade: WhaleTrade, confidence: float,
        now_ts: int, extra: dict | None = None,
    ) -> dict[str, Any]:
        """Write signal to DB and print to stdout."""
        signal = {
            "signal_type": signal_type,
            "condition_id": trade.condition_id,
            "direction": trade.side,
            "confidence": confidence,
            "wallet": trade.wallet,
            "price": trade.price,
            "size": trade.size,
            "category": trade.category,
            "question": trade.question,
            "wallet_tier": trade.wallet_tier,
            "directional_win_rate": trade.directional_win_rate,
            "fired_at": now_ts,
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
                if alpha <= 0:
                    logger.info(
                        "[ALPHA_GATE] wallet %s NOT copyable in %s, SKIP %s",
                        trade.wallet[:14], trade.category, signal_type,
                    )
                    return None
                # Stash on the signal so the executor reuses it without
                # re-querying the DB.
                signal["alpha_score"] = alpha
                logger.info(
                    "[ALPHA_GATE] wallet %s copyable in %s, score=%.3f, signal=%s",
                    trade.wallet[:14], trade.category, alpha, signal_type,
                )
            except Exception as exc:
                logger.debug("alpha gate (engine) lookup failed: %s", exc)

        # Write to market_signals (with question for joins)
        self.db.insert_signal(
            token_id=trade.condition_id,
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
        if signal_type in DISABLED_SIGNAL_TYPES:
            # whale_entry is disabled from direct execution, but if the
            # firing wallet is a copyable archetype (not a bot), fork a
            # whale_entry_filtered signal that IS eligible for execution.
            if signal_type == "whale_entry":
                self._maybe_fire_filtered_whale_entry(signal, trade, now_ts)
                self._maybe_fire_insider_entry(signal, trade, now_ts)
            logger.info("[DISABLED] %s signal blocked from trading", signal_type)
            return signal  # log but don't trade
        if signal_type in INFORMATIONAL_SIGNALS:
            logger.info("[INFO_ONLY] %s logged as informational, not trading", signal_type)
            return signal  # log but don't trade
        if _is_sports_market(trade.category, trade.question):
            return signal  # log but don't trade

        try:
            from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
            if not hasattr(self, "_paper"):
                self._paper = PolymarketPaperExecutor()
            paper_trade = self._paper.execute_signal(signal)
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
            logger.warning("[SIGNAL\u2192TRADE] EXECUTOR_FAILED: %s", exc)

        # Live executor — DRY_RUN by default. Will be blocked by KillSwitch
        # unless POLYMARKET_LIVE_ENABLED=1 in .env. Purely additive:
        # failures here never affect paper trading.
        # LIVE_SIGNAL_TYPES is a code-level whitelist on top of KillSwitch.
        LIVE_SIGNAL_TYPES = {"accumulation", "whale_entry_filtered"}
        if signal_type in LIVE_SIGNAL_TYPES:
            try:
                from trading_platform.polymarket.polymarket_live_executor import PolymarketLiveExecutor
                if not hasattr(self, "_live"):
                    self._live = PolymarketLiveExecutor()
                live_result = self._live.execute(signal)
                if live_result.get("success"):
                    if live_result.get("mode") == "dry_run":
                        logger.info("[LIVE_DRY] would trade: %s", live_result)
                    elif live_result.get("mode") == "live":
                        logger.warning("[LIVE_TRADE] EXECUTED: %s", live_result)
            except Exception as exc:
                logger.debug("[LIVE] Executor error: %s", exc)

        # Telegram alerts (non-blocking, enriched with profile)
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            alerter = get_alerter()
            profile = self.db.get_profile(trade.wallet)
            if trade.wallet_tier in ("tier1", "tier1h"):
                alerter.send_whale_detection(trade, profile=profile)
            alerter.send_signal(signal)

            # Political/geopolitical whale alert: if the firing wallet is
            # S/A/B tier in politics or geopolitics, route it through the
            # dedicated loud alert path so operator doesn't miss it.
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

            # Insider entry alert
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
