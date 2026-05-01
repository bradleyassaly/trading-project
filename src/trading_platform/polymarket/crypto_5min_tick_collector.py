"""Tick collector for crypto 5-min "Up or Down" markets.

The standard live-collect WebSocket only subscribes to markets in the
internal universe (min_volume=$10K), which excludes 5-min markets due
to their low individual volume. This module fills that gap by polling
the public CLOB prices for the 7 active crypto 5-min markets every
N seconds and writing rows to market_ticks — the same table the
strategy backtest framework reads.

Run via scheduler at high cadence (15s) during market hours. Each tick
is one INSERT — cheap. Polymarket's CLOB price endpoint is public + free.

Output: market_ticks rows with (condition_id, yes_token_id, ts, yes_price)
for the [-5min, +0min] window of each active market. After 24-72h of
collection, the BTC 5-min comparison harness can replay this data
against multiple hypothesis signals.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "crypto_5min_tick_collect"
ENV_FLAG = "PHASE_C_BTC_5MIN_ENABLED"  # share the strategy's flag

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_PRICE_URL = "https://clob.polymarket.com/price"

CRYPTO_TICKERS = ("Bitcoin", "Ethereum", "Solana", "Dogecoin", "BNB", "XRP", "Hyperliquid")
UPDOWN_PATTERN = re.compile(
    r"^(" + "|".join(CRYPTO_TICKERS) + r")\s+Up\s+or\s+Down",
    re.IGNORECASE,
)

# Collect ticks for markets resolving in next N seconds.
# Wider than the strategy fire-window so we capture the full evolution.
TICK_WINDOW_SECONDS = 600  # 10 min — covers full pre-resolution arc


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _fetch_active_markets() -> list[dict[str, Any]]:
    """Fetch all active 5-min crypto markets within TICK_WINDOW."""
    params = {"limit": "100", "closed": "false", "active": "true",
              "order": "startDate", "ascending": "false"}
    url = f"{GAMMA_BASE}/markets?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("[5MIN_TICK] gamma fetch failed: %s", exc)
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
        if not (-30 <= secs <= TICK_WINDOW_SECONDS):
            continue
        try:
            tids = json.loads(m.get("clobTokenIds", "[]"))
            yes_tid = tids[0] if len(tids) > 0 else None
            no_tid = tids[1] if len(tids) > 1 else None
        except Exception:
            continue
        if not yes_tid:
            continue
        out.append({
            "condition_id": m.get("conditionId"),
            "yes_token_id": str(yes_tid),
            "no_token_id": str(no_tid) if no_tid else None,
            "secs_to_resolve": secs,
            "question": m.get("question", ""),
        })
    return out


def _fetch_price(token_id: str, side: str = "buy") -> float | None:
    """Get current best price for a token. Falls back gracefully."""
    try:
        url = f"{CLOB_PRICE_URL}?token_id={token_id}&side={side}"
        req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return float(data.get("price") or 0)
    except Exception:
        return None


def _ensure_market_ticks_schema(conn) -> None:
    """market_ticks canonical schema is (id, condition_id, token_id,
    timestamp, price). Don't recreate — just ensure index exists."""
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_ticks_cid_ts "
            "ON market_ticks(condition_id, timestamp DESC)"
        )
        conn.commit()
    except Exception:
        pass


def run_collector() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set"}
    t0 = time.time()
    markets = _fetch_active_markets()
    if not markets:
        return {"elapsed_seconds": round(time.time() - t0, 2),
                "markets": 0, "ticks_written": 0}
    ticks_written = 0
    failed = 0
    conn = get_connection()
    try:
        _ensure_market_ticks_schema(conn)
        now_ts = int(time.time())
        for m in markets:
            yes_buy = _fetch_price(m["yes_token_id"], side="buy")
            if yes_buy is None or yes_buy <= 0 or yes_buy >= 1:
                failed += 1
                continue
            try:
                conn.execute(
                    """INSERT INTO market_ticks
                         (condition_id, token_id, timestamp, price)
                       VALUES (?,?,?,?)""",
                    (m["condition_id"], m["yes_token_id"], now_ts, yes_buy),
                )
                ticks_written += 1
            except Exception as exc:
                logger.debug("[5MIN_TICK] insert failed: %s", exc)
                failed += 1
        conn.commit()
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "markets": len(markets),
        "ticks_written": ticks_written,
        "failed": failed,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_collector())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
