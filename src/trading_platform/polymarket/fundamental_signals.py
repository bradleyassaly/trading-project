"""Fundamental signal generators — uncorrelated with wallet activity.

These signals derive from price patterns and market lifecycle, not from
watching who's trading. They form the 'fundamental' cluster in the
ensemble scorer and provide true diversification vs wallet-based signals.
"""
from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


def scan_price_momentum(
    db_path: str | None = None,
    min_volume_24h: float = 50_000,
    momentum_threshold: float = 0.15,
) -> list[dict[str, Any]]:
    """Find markets with strong 24h price momentum in the fillable band.

    A market that moved >15pp in 24h AND is still in the 0.10-0.85 band
    has continuation potential — smart money drove the price and it hasn't
    fully resolved yet.

    Returns list of signal dicts ready for the paper executor.
    """
    from trading_platform.polymarket.db_connection import db as _db

    signals = []
    now_ts = int(time.time())
    cutoff_24h = now_ts - 86400

    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT mt.condition_id,
                       MIN(mt.price) AS low_24h,
                       MAX(mt.price) AS high_24h,
                       (SELECT mt2.price FROM market_ticks mt2
                        WHERE mt2.condition_id = mt.condition_id
                        ORDER BY mt2.timestamp DESC LIMIT 1) AS current_price,
                       m.question, m.end_date_iso
                FROM market_ticks mt
                JOIN markets m ON mt.condition_id = m.condition_id
                WHERE mt.timestamp >= ?
                GROUP BY mt.condition_id
                HAVING COUNT(*) >= 10
            """, (cutoff_24h,)).fetchall()

            for cid, low, high, current, question, end_date in rows:
                if current is None or current < 0.10 or current > 0.85:
                    continue
                price_range = (high or 0) - (low or 0)
                if price_range < momentum_threshold:
                    continue

                if current > (low + high) / 2:
                    direction = "BUY"
                    move = current - low
                else:
                    direction = "SELL"
                    move = high - current

                confidence = min(0.85, 0.40 + move * 1.5)

                signals.append({
                    "signal_type": "price_momentum",
                    "condition_id": cid,
                    "direction": direction,
                    "confidence": round(confidence, 4),
                    "price": current,
                    "question": question or "",
                    "category": "other",
                    "wallet": "price_scanner",
                    "wallet_tier": "market",
                    "size": 0,
                    "fired_at": now_ts,
                    "directional_win_rate": 0.0,
                    "momentum_24h": round(price_range, 4),
                })
    except Exception as exc:
        logger.debug("price_momentum scan failed: %s", exc)

    return signals


def scan_resolution_proximity(
    db_path: str | None = None,
    max_hours_to_close: float = 48,
    min_price_distance: float = 0.15,
) -> list[dict[str, Any]]:
    """Find markets near resolution where price implies a clear winner.

    Markets closing within 48h where price is >0.85 (likely YES) or <0.15
    (likely NO) are high-confidence short-duration opportunities. The
    market is about to resolve and the price already reflects the outcome
    — but hasn't fully converged to 0 or 1 yet, leaving a small edge.

    Returns list of signal dicts.
    """
    from trading_platform.polymarket.db_connection import db as _db
    from datetime import datetime, timezone

    signals = []
    now_ts = int(time.time())
    now_dt = datetime.now(tz=timezone.utc)

    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT m.condition_id, m.question, m.end_date_iso,
                       (SELECT mt.price FROM market_ticks mt
                        WHERE mt.condition_id = m.condition_id
                        ORDER BY mt.timestamp DESC LIMIT 1) AS current_price
                FROM markets m
                WHERE m.end_date_iso IS NOT NULL AND m.active = 1
            """).fetchall()

            for cid, question, end_date, current in rows:
                if current is None:
                    continue
                try:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    hours_left = (end_dt - now_dt).total_seconds() / 3600
                except Exception:
                    continue

                if hours_left <= 0 or hours_left > max_hours_to_close:
                    continue

                if current >= (1.0 - min_price_distance):
                    direction = "BUY"
                    confidence = round(min(0.90, 0.60 + (current - 0.85) * 3), 4)
                elif current <= min_price_distance:
                    direction = "SELL"
                    confidence = round(min(0.90, 0.60 + (0.15 - current) * 3), 4)
                else:
                    continue

                signals.append({
                    "signal_type": "resolution_proximity",
                    "condition_id": cid,
                    "direction": direction,
                    "confidence": confidence,
                    "price": current,
                    "question": question or "",
                    "category": "other",
                    "wallet": "resolution_scanner",
                    "wallet_tier": "market",
                    "size": 0,
                    "fired_at": now_ts,
                    "directional_win_rate": 0.0,
                    "hours_to_close": round(hours_left, 1),
                })
    except Exception as exc:
        logger.debug("resolution_proximity scan failed: %s", exc)

    return signals
