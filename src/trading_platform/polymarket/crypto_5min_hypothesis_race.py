"""Crypto 5-min hypothesis race — fire 6 competing strategies in parallel.

Hypothesis test: instead of believing one strategy works on BTC 5-min,
fire 6 distinct hypotheses each as their own signal_type. After N=20+
resolutions per hypothesis, the calibration loop + Bayesian gate ranks
them. Highest-WR hypothesis graduates to live.

Hypotheses (all run on the same market simultaneously when conditions
align — multiple may fire on one market with different sides):

  H1_OB_VELOCITY     — OB imbalance + 30s velocity, both directions align
  H2_PURE_VELOCITY   — 30s velocity ≥ 5%, follow direction (no OB)
  H3_MEAN_REVERSION  — yes_price > 0.7 in [60s,240s] window → bet NO
                       (hypothesis: late overshoot reverts)
  H4_LATE_MOMENTUM   — yes_price moved >5% in last 60s, near close → follow
                       (hypothesis: late info dominates)
  H5_CLOSE_POSITION  — at [-30s, -90s] before close, just bet whichever
                       side is winning (>0.55 → YES, <0.45 → NO)
                       (hypothesis: close-position bias)
  H6_RANDOM_BASELINE — random direction. Null hypothesis. Brier should
                       be ~0.25, WR ~50%. Any hypothesis that doesn't
                       beat this is no better than chance.

Each hypothesis records its own signal_type so the existing calibration
+ ladder pipeline ranks them naturally. After ~24-72h of accumulation:
  /api/calibration/by_signal — Brier per hypothesis
  /api/ladder/status promotable_signals — Bayesian-validated hypotheses

Behind PHASE_C_BTC_5MIN_RACE_ENABLED. Default off; turn on after
crypto_5min_tick_collector has been writing for at least an hour
(otherwise the velocity/imbalance signals are noise).
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import urllib.parse
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


ENV_FLAG = "PHASE_C_BTC_5MIN_RACE_ENABLED"
GAMMA_BASE = "https://gamma-api.polymarket.com"

CRYPTO_TICKERS = ("Bitcoin", "Ethereum", "Solana", "Dogecoin", "BNB", "XRP", "Hyperliquid")
UPDOWN_PATTERN = re.compile(
    r"^(" + "|".join(CRYPTO_TICKERS) + r")\s+Up\s+or\s+Down",
    re.IGNORECASE,
)

# Window — fire each hypothesis when secs-to-resolve in this band
H1_WINDOW = (30, 240)
H2_WINDOW = (30, 240)
H3_WINDOW = (60, 240)
H4_WINDOW = (15, 90)
H5_WINDOW = (30, 90)
H6_WINDOW = (30, 240)

# Thresholds
H1_MIN_IMB = 0.30
H1_MIN_VEL = 0.03
H2_MIN_VEL = 0.05
H3_PRICE_THRESH_HIGH = 0.70  # yes > 0.70 → bet NO
H3_PRICE_THRESH_LOW = 0.30   # yes < 0.30 → bet YES
H4_MIN_VEL_60S = 0.05
H5_PRICE_HIGH = 0.55
H5_PRICE_LOW = 0.45

MIN_CONFIDENCE = 0.30


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _fetch_active_markets() -> list[dict[str, Any]]:
    params = {"limit": "100", "closed": "false", "active": "true",
              "order": "startDate", "ascending": "false"}
    url = f"{GAMMA_BASE}/markets?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("[RACE] gamma fetch failed: %s", exc)
        return []
    now = time.time()
    out = []
    for m in data if isinstance(data, list) else []:
        if not UPDOWN_PATTERN.match(m.get("question", "")):
            continue
        end_iso = m.get("endDate", "")
        try:
            from datetime import datetime, timezone
            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
            end_dt = datetime.fromisoformat(clean)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            secs = end_dt.timestamp() - now
        except Exception:
            continue
        if not (10 <= secs <= 300):
            continue
        try:
            prices = json.loads(m.get("outcomePrices", "[]"))
            yes_price = float(prices[0]) if prices else None
        except Exception:
            yes_price = None
        if yes_price is None or not (0.05 <= yes_price <= 0.95):
            continue
        try:
            tids = json.loads(m.get("clobTokenIds", "[]"))
            yes_tid = str(tids[0]) if len(tids) > 0 else None
            no_tid = str(tids[1]) if len(tids) > 1 else None
        except Exception:
            yes_tid, no_tid = None, None
        out.append({
            "condition_id": m.get("conditionId"),
            "slug": m.get("slug", ""),
            "question": m.get("question", ""),
            "yes_token_id": yes_tid, "no_token_id": no_tid,
            "yes_price": yes_price, "secs_to_resolve": secs,
            "ticker": m.get("question", "").split()[0].lower(),
        })
    return out


def _price_velocity(conn, condition_id: str, lookback_s: int) -> float:
    cutoff = int(time.time()) - lookback_s
    try:
        rows = conn.execute(
            """SELECT timestamp, price FROM market_ticks
                WHERE condition_id = ? AND timestamp > ?
                ORDER BY timestamp ASC""",
            (condition_id, cutoff),
        ).fetchall()
    except Exception:
        return 0.0
    if len(rows) < 2:
        return 0.0
    return float(rows[-1][1] or 0.0) - float(rows[0][1] or 0.0)


def _ob_imbalance(conn, condition_id: str) -> float:
    try:
        row = conn.execute(
            """SELECT yes_bid_depth_usd, yes_ask_depth_usd
                 FROM order_book_snapshots
                WHERE condition_id = ? AND ts > ?
                ORDER BY ts DESC LIMIT 1""",
            (condition_id, int(time.time()) - 30),
        ).fetchone()
    except Exception:
        return 0.0
    if not row:
        return 0.0
    bid, ask = float(row[0] or 0), float(row[1] or 0)
    total = bid + ask
    return (bid - ask) / total if total > 0 else 0.0


def _emit(market: dict, signal_type: str, side: str, conf: float, **extras) -> bool:
    if conf < MIN_CONFIDENCE:
        return False
    entry_price = market["yes_price"] if side == "YES" else (1.0 - market["yes_price"])
    payload = {
        "signal_type": signal_type,
        "condition_id": market["condition_id"],
        "side": side, "direction": "BUY",
        "entry_price": entry_price, "price": entry_price,
        "confidence": conf,
        "wallet": signal_type, "wallet_tier": "synthetic",
        "slug": market["slug"], "question": market["question"][:200],
        "yes_token_id": market["yes_token_id"], "no_token_id": market["no_token_id"],
        "category": "crypto", "subcategory": f"crypto/{market['ticker']}",
        "fired_at": int(time.time()),
        **extras,
    }
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            PolymarketPaperExecutor,
        )
        executor = PolymarketPaperExecutor()
        trade = executor.execute_signal(payload)
        if trade:
            logger.info(
                "[RACE] %s PLACED %s @ %.3f conf=%.2f t-%.0fs",
                signal_type, side, entry_price, conf, market["secs_to_resolve"],
            )
            return True
    except Exception as exc:
        logger.debug("[RACE] %s emit failed: %s", signal_type, exc)
    return False


def _h1_ob_velocity(conn, m: dict) -> bool:
    if not (H1_WINDOW[0] <= m["secs_to_resolve"] <= H1_WINDOW[1]):
        return False
    imb = _ob_imbalance(conn, m["condition_id"])
    vel = _price_velocity(conn, m["condition_id"], 30)
    if abs(imb) < H1_MIN_IMB or abs(vel) < H1_MIN_VEL:
        return False
    if imb * vel < 0:
        return False  # disagreement
    side = "YES" if imb + vel > 0 else "NO"
    conf = min(0.85, 0.50 + 0.20 * min(abs(imb), 1.0) + 0.15 * min(abs(vel) / 0.10, 1.0))
    return _emit(m, "h1_ob_velocity", side, round(conf, 4),
                 imbalance=round(imb, 3), velocity=round(vel, 3))


def _h2_pure_velocity(conn, m: dict) -> bool:
    if not (H2_WINDOW[0] <= m["secs_to_resolve"] <= H2_WINDOW[1]):
        return False
    vel = _price_velocity(conn, m["condition_id"], 30)
    if abs(vel) < H2_MIN_VEL:
        return False
    side = "YES" if vel > 0 else "NO"
    conf = min(0.75, 0.45 + 0.30 * min(abs(vel) / 0.10, 1.0))
    return _emit(m, "h2_pure_velocity", side, round(conf, 4), velocity=round(vel, 3))


def _h3_mean_reversion(conn, m: dict) -> bool:
    if not (H3_WINDOW[0] <= m["secs_to_resolve"] <= H3_WINDOW[1]):
        return False
    yp = m["yes_price"]
    if H3_PRICE_THRESH_LOW <= yp <= H3_PRICE_THRESH_HIGH:
        return False  # mid-range, no edge
    side = "NO" if yp > H3_PRICE_THRESH_HIGH else "YES"
    # Confidence proportional to distance from 0.5
    conf = min(0.75, 0.40 + 0.50 * abs(yp - 0.5))
    return _emit(m, "h3_mean_reversion", side, round(conf, 4), yes_price=yp)


def _h4_late_momentum(conn, m: dict) -> bool:
    if not (H4_WINDOW[0] <= m["secs_to_resolve"] <= H4_WINDOW[1]):
        return False
    vel = _price_velocity(conn, m["condition_id"], 60)
    if abs(vel) < H4_MIN_VEL_60S:
        return False
    side = "YES" if vel > 0 else "NO"
    conf = min(0.80, 0.50 + 0.35 * min(abs(vel) / 0.15, 1.0))
    return _emit(m, "h4_late_momentum", side, round(conf, 4), velocity_60s=round(vel, 3))


def _h5_close_position(conn, m: dict) -> bool:
    if not (H5_WINDOW[0] <= m["secs_to_resolve"] <= H5_WINDOW[1]):
        return False
    yp = m["yes_price"]
    if H5_PRICE_LOW <= yp <= H5_PRICE_HIGH:
        return False
    side = "YES" if yp > H5_PRICE_HIGH else "NO"
    # Confidence is the leading-side probability
    conf = round(yp if side == "YES" else 1.0 - yp, 4)
    conf = min(0.80, max(0.45, conf))
    return _emit(m, "h5_close_position", side, conf, yes_price=yp)


def _h6_random_baseline(conn, m: dict) -> bool:
    """Null hypothesis — random side. Any genuine alpha must beat this."""
    if not (H6_WINDOW[0] <= m["secs_to_resolve"] <= H6_WINDOW[1]):
        return False
    side = random.choice(("YES", "NO"))
    return _emit(m, "h6_random_baseline", side, 0.50)


HYPOTHESES = [_h1_ob_velocity, _h2_pure_velocity, _h3_mean_reversion,
              _h4_late_momentum, _h5_close_position, _h6_random_baseline]


def run_pipeline() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "candidates": 0, "fired": 0}
    t0 = time.time()
    markets = _fetch_active_markets()
    if not markets:
        return {"elapsed_seconds": round(time.time() - t0, 2),
                "candidates": 0, "fired": 0}
    fired_by_hypothesis: dict[str, int] = {}
    conn = get_connection()
    try:
        for m in markets:
            for h in HYPOTHESES:
                try:
                    if h(conn, m):
                        name = h.__name__.lstrip("_")
                        fired_by_hypothesis[name] = fired_by_hypothesis.get(name, 0) + 1
                except Exception as exc:
                    logger.debug("hypothesis %s failed: %s", h.__name__, exc)
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "candidates": len(markets),
        "fired_total": sum(fired_by_hypothesis.values()),
        "fired_by_hypothesis": fired_by_hypothesis,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
