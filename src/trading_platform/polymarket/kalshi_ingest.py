"""Kalshi market ingestion for cross-platform arb.

Pulls active Kalshi markets from the public trade-api and persists
them to `kalshi_markets`. Companion to the cross_platform_arb_strategy
which reads PM↔Kalshi pairs from cross_platform_market_map and looks
for >50bps gaps to arb.

Schema:
    kalshi_markets(ticker, title, event_ticker, yes_bid, yes_ask,
                   close_time, last_updated)

Run via scheduler every 15 min. Idempotent upserts.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
PAGE_LIMIT = 200  # Kalshi max per page
# 2026-04-30: increased 20 → 50 (4K → 10K markets/run). Top 4K are
# dominated by sports parlay multi-event tickers (KXMVE*). Real
# political/news markets live deeper in pagination. 10K total is still
# tractable in ~45s and covers the categories we actually want to arb.
MAX_PAGES = 50


_SCHEMA = """
CREATE TABLE IF NOT EXISTS kalshi_markets (
    ticker         TEXT PRIMARY KEY,
    title          TEXT,
    event_ticker   TEXT,
    yes_bid        DOUBLE PRECISION,
    yes_ask        DOUBLE PRECISION,
    last_price     DOUBLE PRECISION,
    liquidity_usd  DOUBLE PRECISION,
    close_time     BIGINT,
    last_updated   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_close_time ON kalshi_markets(close_time);
CREATE INDEX IF NOT EXISTS idx_kalshi_updated ON kalshi_markets(last_updated DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception: pass


def _fetch_page(cursor: str | None) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": PAGE_LIMIT, "status": "open"}
    if cursor:
        params["cursor"] = cursor
    url = f"{KALSHI_BASE}/markets?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _close_to_ts(close_time_str: str | None) -> int | None:
    if not close_time_str:
        return None
    try:
        from datetime import datetime, timezone
        s = close_time_str.replace("Z", "+00:00") if close_time_str.endswith("Z") else close_time_str
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _fetch_events_page(cursor: str | None) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": 200, "status": "open"}
    if cursor:
        params["cursor"] = cursor
    url = f"{KALSHI_BASE}/events?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_markets_for_event(event_ticker: str) -> list[dict[str, Any]]:
    params = {"limit": 50, "status": "open", "event_ticker": event_ticker}
    url = f"{KALSHI_BASE}/markets?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data.get("markets") or []
    except Exception:
        return []


# Categories worth ingesting beyond the default flat sweep. PM ↔ Kalshi
# overlap is highest in these.
TARGETED_CATEGORIES = {
    "Elections", "Politics", "World", "Economics", "Climate and Weather",
    "Crypto", "Companies", "Science and Technology",
}


def _ingest_events(conn, now: int) -> tuple[int, int]:
    """Pull events in TARGETED_CATEGORIES, then markets per event.

    This captures politics/economics/world markets that sit deep in
    pagination behind sports parlays in the flat /markets sweep.
    """
    n_event_pages = 0
    n_event_markets = 0
    cursor: str | None = None
    while n_event_pages < 30:  # ≤6000 events scanned
        try:
            page = _fetch_events_page(cursor)
        except Exception as exc:
            logger.warning("kalshi events page failed: %s", exc)
            break
        events = page.get("events") or []
        if not events:
            break
        for ev in events:
            cat = ev.get("category", "")
            if cat not in TARGETED_CATEGORIES:
                continue
            event_ticker = ev.get("event_ticker")
            if not event_ticker:
                continue
            for m in _fetch_markets_for_event(event_ticker):
                ticker = m.get("ticker")
                if not ticker:
                    continue
                title = (m.get("title") or m.get("yes_sub_title") or "")[:300]
                try:
                    yes_bid = float(m.get("yes_bid_dollars") or
                                    (float(m.get("yes_bid") or 0) / 100.0))
                    yes_ask = float(m.get("yes_ask_dollars") or
                                    (float(m.get("yes_ask") or 0) / 100.0))
                    last_price = float(m.get("last_price_dollars") or
                                       (float(m.get("last_price") or 0) / 100.0))
                    liquidity = float(m.get("liquidity_dollars") or 0)
                except Exception:
                    continue
                close_ts = _close_to_ts(m.get("close_time"))
                try:
                    conn.execute(
                        """INSERT INTO kalshi_markets
                             (ticker, title, event_ticker, yes_bid, yes_ask,
                              last_price, liquidity_usd, close_time, last_updated)
                           VALUES (?,?,?,?,?,?,?,?,?)
                           ON CONFLICT (ticker) DO UPDATE SET
                             title = EXCLUDED.title, event_ticker = EXCLUDED.event_ticker,
                             yes_bid = EXCLUDED.yes_bid, yes_ask = EXCLUDED.yes_ask,
                             last_price = EXCLUDED.last_price,
                             liquidity_usd = EXCLUDED.liquidity_usd,
                             close_time = EXCLUDED.close_time,
                             last_updated = EXCLUDED.last_updated""",
                        (ticker, title, event_ticker, yes_bid, yes_ask,
                         last_price, liquidity, close_ts, now),
                    )
                    n_event_markets += 1
                except Exception:
                    pass
        cursor = page.get("cursor")
        if not cursor:
            break
        n_event_pages += 1
    return n_event_pages, n_event_markets


def run_ingest() -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection()
    try:
        _ensure_schema(conn)
        now = int(time.time())
        cursor: str | None = None
        n_pages = 0
        n_upserts = 0
        n_errors = 0
        while n_pages < MAX_PAGES:
            try:
                page = _fetch_page(cursor)
            except Exception as exc:
                logger.warning("kalshi page fetch failed: %s", exc)
                n_errors += 1
                break
            markets = page.get("markets") or []
            if not markets:
                break
            for m in markets:
                ticker = m.get("ticker")
                if not ticker:
                    continue
                title = (m.get("title") or m.get("yes_sub_title") or "")[:300]
                event_ticker = m.get("event_ticker")
                # Kalshi prices are in cents (0-99) but `_dollars`
                # variants are 0.0-1.0 already. Prefer the dollar form.
                try:
                    yes_bid = float(m.get("yes_bid_dollars") or
                                    (float(m.get("yes_bid") or 0) / 100.0))
                except Exception:
                    yes_bid = 0.0
                try:
                    yes_ask = float(m.get("yes_ask_dollars") or
                                    (float(m.get("yes_ask") or 0) / 100.0))
                except Exception:
                    yes_ask = 0.0
                try:
                    last_price = float(m.get("last_price_dollars") or
                                       (float(m.get("last_price") or 0) / 100.0))
                except Exception:
                    last_price = 0.0
                try:
                    liquidity = float(m.get("liquidity_dollars") or 0)
                except Exception:
                    liquidity = 0.0
                close_ts = _close_to_ts(m.get("close_time"))
                try:
                    conn.execute(
                        """INSERT INTO kalshi_markets
                             (ticker, title, event_ticker, yes_bid, yes_ask,
                              last_price, liquidity_usd, close_time, last_updated)
                           VALUES (?,?,?,?,?,?,?,?,?)
                           ON CONFLICT (ticker) DO UPDATE SET
                             title = EXCLUDED.title,
                             event_ticker = EXCLUDED.event_ticker,
                             yes_bid = EXCLUDED.yes_bid,
                             yes_ask = EXCLUDED.yes_ask,
                             last_price = EXCLUDED.last_price,
                             liquidity_usd = EXCLUDED.liquidity_usd,
                             close_time = EXCLUDED.close_time,
                             last_updated = EXCLUDED.last_updated""",
                        (ticker, title, event_ticker, yes_bid, yes_ask,
                         last_price, liquidity, close_ts, now),
                    )
                    n_upserts += 1
                except Exception as exc:
                    logger.debug("kalshi upsert failed %s: %s", ticker[:24], exc)
                    n_errors += 1
            cursor = page.get("cursor")
            if not cursor:
                break
            n_pages += 1
        # Then targeted-categories pass — captures politics/world/etc.
        # markets that sit deep behind sports parlays in flat sweep.
        n_event_pages, n_event_markets = _ingest_events(conn, now)
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "flat_pages": n_pages, "flat_upserts": n_upserts,
            "event_pages": n_event_pages, "event_market_upserts": n_event_markets,
            "errors": n_errors,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_ingest())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
