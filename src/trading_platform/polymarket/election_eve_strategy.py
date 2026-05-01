"""Election-eve momentum strategy.

Hypothesis: in the final 12-72 hours before a binary politics market
resolves, sustained directional flow (>10% price move with positive
volume) tends to continue rather than mean-revert. This is the
opposite intuition from sports CLV (where sharp money moves are
predictive of outcome). For politics, late-arriving news is dominant
and tends to compound.

Why it's complementary:
  - Different market dynamics from sports/crypto
  - Fires only during election cycles + politics resolution windows
  - Captures the "October surprise" effect formalized

How it fires:
  - Run every 30min
  - For each politics/geopolitics market with end_date_iso ∈ [now+12h, now+72h]:
    * Pull current YES, YES 12h ago
    * If |delta| > 0.08 AND volume_24h > $5K, fire FOLLOW signal
  - Direction-of-move signals = ride the late shift

Behind PHASE_F_ELECTION_EVE_ENABLED. Tier-C until validated.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "election_eve_momentum"
ENV_FLAG = "PHASE_F_ELECTION_EVE_ENABLED"

MIN_HOURS_TO_RESOLVE = 12.0
MAX_HOURS_TO_RESOLVE = 72.0
LOOKBACK_HOURS = 12.0

MIN_LINE_MOVE = 0.08
MAX_LINE_MOVE = 0.30  # bigger than this = market re-rated, not momentum

ENTRY_PRICE_LOW = 0.15
ENTRY_PRICE_HIGH = 0.85

MIN_VOLUME_24H = 5000.0


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _candidate_markets(conn) -> list[dict[str, Any]]:
    now = time.time()
    rows = conn.execute(
        """SELECT condition_id, slug, question, end_date_iso,
                  yes_token_id, no_token_id, outcome_prices, volume_24h,
                  subcategory
             FROM markets
            WHERE end_date_iso IS NOT NULL
              AND yes_token_id IS NOT NULL
              AND no_token_id IS NOT NULL
              AND (LOWER(subcategory) LIKE 'politics%'
                   OR LOWER(subcategory) LIKE 'geopolitics%')"""
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        cid, slug, q, end_iso, yes_tid, no_tid, prices_raw, vol, subcat = r
        cat = (subcat or "").split("/")[0] if subcat else "politics"
        try:
            from datetime import datetime, timezone
            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
            end_dt = datetime.fromisoformat(clean)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            hours = (end_dt.timestamp() - now) / 3600
        except Exception:
            continue
        if not (MIN_HOURS_TO_RESOLVE <= hours <= MAX_HOURS_TO_RESOLVE):
            continue
        if vol is None or float(vol) < MIN_VOLUME_24H:
            continue
        yes_price = None
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            if isinstance(prices, list) and prices:
                yes_price = float(prices[0])
        except Exception:
            yes_price = None
        if yes_price is None or not (ENTRY_PRICE_LOW <= yes_price <= ENTRY_PRICE_HIGH):
            continue
        out.append({
            "condition_id": cid, "slug": slug or "", "question": q or "",
            "end_iso": end_iso, "yes_token_id": yes_tid, "no_token_id": no_tid,
            "yes_price": yes_price, "hours_to_resolve": hours,
            "volume_24h": float(vol or 0), "category": (cat or "politics").lower(),
        })
    return out


def _line_move(conn, condition_id: str) -> float | None:
    cutoff = int(time.time()) - int(LOOKBACK_HOURS * 3600)
    try:
        rows = conn.execute(
            """SELECT timestamp, price FROM market_ticks
                WHERE condition_id = ? AND timestamp > ?
                ORDER BY timestamp ASC""",
            (condition_id, cutoff),
        ).fetchall()
    except Exception:
        return None
    if len(rows) < 2:
        return None
    return float(rows[-1][1] or 0.0) - float(rows[0][1] or 0.0)


def _confidence(line_move: float, hours_to_resolve: float, volume_24h: float) -> float:
    move_lift = 0.25 * min(abs(line_move) / MAX_LINE_MOVE, 1.0)
    time_lift = 0.15 * (1 - hours_to_resolve / MAX_HOURS_TO_RESOLVE)
    vol_lift = 0.10 * min(volume_24h / 50_000.0, 1.0)
    return round(min(0.85, max(0.30, 0.45 + move_lift + time_lift + vol_lift)), 4)


def _emit_signal(market: dict, line_move: float) -> None:
    side = "YES" if line_move > 0 else "NO"
    direction = "BUY"
    entry_price = market["yes_price"] if line_move > 0 else (1.0 - market["yes_price"])
    conf = _confidence(line_move, market["hours_to_resolve"], market["volume_24h"])
    now_ts = int(time.time())
    payload = {
        "signal_type": SIGNAL_TYPE,
        "condition_id": market["condition_id"],
        "side": side, "direction": direction,
        "entry_price": entry_price, "price": entry_price,
        "confidence": conf,
        "wallet": "election_eve_strategy", "wallet_tier": "synthetic",
        "slug": market["slug"], "question": market["question"][:200],
        "yes_token_id": market["yes_token_id"], "no_token_id": market["no_token_id"],
        "category": market["category"],
        "fired_at": now_ts,
        "line_move_12h": round(line_move, 4),
        "hours_to_resolve": round(market["hours_to_resolve"], 2),
    }
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            PolymarketPaperExecutor,
        )
        executor = PolymarketPaperExecutor()
        paper_trade = executor.execute_signal(payload)
        if paper_trade:
            logger.info(
                "[ELECTION_EVE] PLACED %s @ %.3f conf=%.2f move=%+.3f hrs=%.1f",
                side, entry_price, conf, line_move, market["hours_to_resolve"],
            )
    except Exception as exc:
        logger.debug("[ELECTION_EVE] paper executor failed: %s", exc)


def run_pipeline() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "candidates": 0, "fired": 0}
    t0 = time.time()
    conn = get_connection()
    fired = 0
    candidates: list = []
    try:
        candidates = _candidate_markets(conn)
        for m in candidates:
            move = _line_move(conn, m["condition_id"])
            if move is None:
                continue
            if not (MIN_LINE_MOVE <= abs(move) <= MAX_LINE_MOVE):
                continue
            _emit_signal(m, move)
            fired += 1
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "candidates": len(candidates), "fired": fired,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
