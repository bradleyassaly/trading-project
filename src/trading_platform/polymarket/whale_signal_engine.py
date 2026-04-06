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
]

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
        tier_mult = 1.0 if trade.wallet_tier == "tier1" else 0.8
        confidence = round(confidence * tier_mult, 4)

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
                                 extra={"converging_wallets": len(distinct)})

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

    def _fire_whale_entry(self, trade: WhaleTrade, now_ts: int) -> dict | None:
        """Baseline signal for any watched wallet trade."""
        wr = trade.directional_win_rate
        conv = trade.conviction_score
        confidence = wr * min(conv / 10.0, 1.0)
        tier_mult = 1.0 if trade.wallet_tier == "tier1" else 0.75
        confidence = round(confidence * tier_mult, 4)

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

        # Write to market_signals
        self.db.insert_signal(
            token_id=trade.condition_id,
            market_title=trade.question[:200],
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
            convergence_count=extra.get("converging_wallets", 0) if extra else 0,
        )

        # Write to wallet_alerts
        tier_num = 1 if trade.wallet_tier == "tier1" else 2
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

        print(
            f"[SIGNAL] {signal_type} | {trade.wallet_tier.upper()} | "
            f"{trade.wallet[:10]}... | {trade.side} | "
            f"{trade.category} | {trade.question[:40]}... | "
            f"conf={confidence:.2f} | ${trade.size:.0f}"
        )

        return signal

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
