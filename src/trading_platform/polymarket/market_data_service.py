"""
Market microstructure service.

Provides market-level OHLCV, order book, and pre-entry baseline data
that the wallet-level Polymarket Data API and Gamma API don't expose.
The fusion score uses this to make smarter paper-trading decisions:
did the whale move the market, or did the market already price in
the information before they entered?

Implementation
--------------

The previous version of this module was backed by the third-party
``pmxt`` library, which required a Node.js sidecar that was never
deployed in our Docker stack. Result: ``is_available()`` always
returned False and the fusion score's market_signal component was
permanently dark (see reports/data_validation.md DV-8).

This rewrite uses direct HTTP calls to the public CLOB and Gamma
APIs, which give us everything we need:

* CLOB ``/book?token_id=`` → bids/asks for order_book_imbalance
* CLOB ``/prices-history?market=&startTs=&endTs=&fidelity=``
  → minute-level price candles for velocity + baseline
* Gamma ``/markets?condition_ids=`` → token_id resolution from
  condition_id (replaces pmxt fetch_market)

The pmxt code path is left in place as a commented fallback in case
the sidecar is ever deployed.

Critical contracts
------------------

* **Always available.** ``is_available()`` returns True now — direct
  HTTP works in every container without sidecars or extra deps.
* **Graceful degradation.** Every public method returns a sensible
  empty/null dict on HTTP failure and logs the error. The signal
  engine, paper trading, calibration, and live executor never crash.
* **Outcome ID caching.** Translation from condition_id → CLOB
  token_id is cached in ``outcome_id_cache`` for 7 days.
* **Health tracking.** Every call updates the singleton
  ``pmxt_health`` row (kept under that legacy name so the GUI keeps
  rendering) so we can see error rate / freshness in the dashboard.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Cache TTL for outcome_id mappings — these almost never change
_OUTCOME_CACHE_TTL_SECONDS = 7 * 86400  # 7 days

# Direct API endpoints
_CLOB_BASE = "https://clob.polymarket.com"
_GAMMA_BASE = "https://gamma-api.polymarket.com"
_HTTP_TIMEOUT = 6.0

# Default fallback structure returned when pmxt is unavailable
_EMPTY_VELOCITY = {
    "current_price": None,
    "price_1h_ago": None,
    "velocity_1h": None,
    "velocity_6h": None,
    "velocity_24h": None,
    "volume_1h": None,
    "volume_24h": None,
    "volume_ratio": None,
    "candles": [],
    "available": False,
}

_EMPTY_BOOK = {
    "best_bid": None,
    "best_ask": None,
    "spread": None,
    "spread_pct": None,
    "bid_depth_10pct": None,
    "ask_depth_10pct": None,
    "imbalance": None,
    "available": False,
}

_EMPTY_BASELINE = {
    "price_before": None,
    "price_at_entry": None,
    "price_now": None,
    "whale_impact": None,
    "post_entry_drift": None,
    "info_already_priced": False,
    "available": False,
}


@dataclass
class _Health:
    last_call_at: int | None
    last_success_at: int | None
    total_calls: int
    total_errors: int
    cache_hits: int
    cache_misses: int
    last_error: str | None


class MarketDataService:
    """Wraps pmxt to provide market microstructure data.

    Designed as a *supplement* to existing integrations, never a
    replacement. The Polymarket Data API and Gamma API still handle
    wallet-level data; this handles market-level OHLCV / order book /
    trades. Every method degrades gracefully when pmxt is missing.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._memo_cache: dict[tuple[str, str], str | None] = {}
        if db_path is None:
            from trading_platform.polymarket.wallet_db import WalletDB
            db_path = WalletDB.default_path()
        self._db_path = str(db_path)
        # Lazy-imported requests session, shared across calls.
        self._session = None

    # ── HTTP session ────────────────────────────────────────────────────────

    def _http(self):
        """Lazy-init a requests Session for connection reuse."""
        if self._session is None:
            import requests
            self._session = requests.Session()
        return self._session

    def is_available(self) -> bool:
        """Always True — direct HTTP needs no sidecar."""
        return True

    # ── pmxt lifecycle (legacy fallback, kept for reference) ────────────────
    # The previous implementation depended on a pmxt Node.js sidecar that
    # was never deployed. Direct CLOB / Gamma HTTP calls cover the same
    # surface in 3 endpoints — see methods below. Restore this block if
    # the sidecar ever ships.
    #
    # def _get_exchange(self):
    #     if self._exchange_failed:
    #         return None
    #     if self._exchange is not None:
    #         return self._exchange
    #     try:
    #         import pmxt
    #         self._exchange = pmxt.Polymarket()
    #         return self._exchange
    #     except Exception as exc:
    #         logger.warning("pmxt unavailable: %s", exc)
    #         self._exchange_failed = True
    #         self._record_error(f"init failed: {exc}")
    #         return None

    # ── Health tracking ─────────────────────────────────────────────────────

    def _record_call(self, success: bool, error: str | None = None) -> None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                now = int(time.time())
                if success:
                    conn.execute(
                        """UPDATE pmxt_health
                           SET last_call_at = ?, last_success_at = ?,
                               total_calls = total_calls + 1
                           WHERE id = 1""",
                        (now, now),
                    )
                else:
                    conn.execute(
                        """UPDATE pmxt_health
                           SET last_call_at = ?, total_calls = total_calls + 1,
                               total_errors = total_errors + 1, last_error = ?
                           WHERE id = 1""",
                        (now, error or ""),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    def _record_error(self, msg: str) -> None:
        self._record_call(success=False, error=msg)

    def _record_cache(self, hit: bool) -> None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                if hit:
                    conn.execute(
                        "UPDATE pmxt_health SET cache_hits = cache_hits + 1 WHERE id = 1"
                    )
                else:
                    conn.execute(
                        "UPDATE pmxt_health SET cache_misses = cache_misses + 1 WHERE id = 1"
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    def get_health(self) -> dict[str, Any]:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                row = conn.execute(
                    """SELECT last_call_at, last_success_at, total_calls,
                              total_errors, cache_hits, cache_misses, last_error
                       FROM pmxt_health WHERE id = 1"""
                ).fetchone()
            finally:
                conn.close()
        except Exception as exc:
            return {
                "available": False, "error": str(exc),
                "pmxt_importable": self.is_available(),
            }
        last_call, last_success, total, errors, hits, misses, last_err = row or (
            None, None, 0, 0, 0, 0, None
        )
        total_lookups = (hits or 0) + (misses or 0)
        return {
            "available": True,
            "pmxt_importable": self.is_available(),
            "last_call_at": last_call,
            "last_success_at": last_success,
            "total_calls": total or 0,
            "total_errors": errors or 0,
            "error_rate": round((errors or 0) / max(1, total or 0), 3),
            "cache_hits": hits or 0,
            "cache_misses": misses or 0,
            "cache_hit_rate": round((hits or 0) / max(1, total_lookups), 3),
            "last_error": last_err,
        }

    # ── Outcome ID resolution + cache ───────────────────────────────────────

    def resolve_outcome_id(
        self,
        condition_id: str | None = None,
        market_slug: str | None = None,
        direction: str = "YES",
    ) -> str | None:
        """Map an existing-system identifier to a pmxt outcome_id.

        Strategy: in-memory memo → SQLite cache → pmxt fetch_market →
        match the requested direction. Returns ``None`` on any failure.
        """
        direction = (direction or "YES").upper()
        memo_key = (condition_id or market_slug or "", direction)
        if memo_key in self._memo_cache:
            self._record_cache(hit=True)
            return self._memo_cache[memo_key]

        # SQLite cache lookup
        if condition_id:
            try:
                conn = sqlite3.connect(self._db_path)
                try:
                    row = conn.execute(
                        """SELECT outcome_id, cached_at FROM outcome_id_cache
                           WHERE condition_id = ? AND direction = ?""",
                        (condition_id, direction),
                    ).fetchone()
                finally:
                    conn.close()
                if row and row[0]:
                    age = int(time.time()) - (row[1] or 0)
                    if age < _OUTCOME_CACHE_TTL_SECONDS:
                        self._memo_cache[memo_key] = row[0]
                        self._record_cache(hit=True)
                        return row[0]
            except Exception:
                pass

        self._record_cache(hit=False)

        # Live Gamma lookup — replaces pmxt fetch_market.
        # Gamma returns clobTokenIds: [yes_token_id, no_token_id].
        outcome_id: str | None = None
        try:
            params: dict[str, Any] = {}
            if condition_id:
                params["condition_ids"] = condition_id
            elif market_slug:
                params["slug"] = market_slug
            r = self._http().get(
                f"{_GAMMA_BASE}/markets", params=params, timeout=_HTTP_TIMEOUT,
            )
            if r.ok:
                data = r.json()
                if isinstance(data, list) and data:
                    m = data[0]
                    tids_raw = m.get("clobTokenIds") or "[]"
                    tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                    if tids:
                        # Convention: clobTokenIds[0] = YES, [1] = NO
                        if direction == "YES":
                            outcome_id = str(tids[0])
                        elif len(tids) >= 2:
                            outcome_id = str(tids[1])
                        else:
                            outcome_id = str(tids[0])
                self._record_call(success=True)
            else:
                self._record_call(success=False, error=f"gamma {r.status_code}")
        except Exception as exc:
            logger.debug("resolve_outcome_id failed: %s", exc)
            self._record_call(success=False, error=str(exc))
            outcome_id = None

        self._memo_cache[memo_key] = outcome_id
        if outcome_id and condition_id:
            try:
                conn = sqlite3.connect(self._db_path)
                try:
                    conn.execute(
                        """INSERT INTO outcome_id_cache
                            (condition_id, market_slug, direction, outcome_id, cached_at)
                           VALUES (?, ?, ?, ?, ?)
                           ON CONFLICT(condition_id, direction) DO UPDATE SET
                               market_slug = excluded.market_slug,
                               outcome_id = excluded.outcome_id,
                               cached_at = excluded.cached_at""",
                        (condition_id, market_slug or "", direction, outcome_id, int(time.time())),
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                pass
        return outcome_id

    # ── Public market-data methods ──────────────────────────────────────────

    def get_price_velocity(self, outcome_id: str, window_hours: int = 24) -> dict[str, Any]:
        """24h of 1h candles + computed velocities at 1h / 6h / 24h windows.

        Direct CLOB call: ``/prices-history?market=&startTs=&endTs=&fidelity=60``
        returns minute-granularity ticks within the time range. We
        downsample into hourly candles in :func:`_velocity_from_history`.
        Note: the older ``interval=1h&fidelity=60`` parameter form
        returns only 1 candle (a CLOB API quirk discovered during the
        signal-flow audit) — always pass startTs/endTs.
        """
        if not outcome_id:
            return dict(_EMPTY_VELOCITY)
        try:
            now = int(time.time())
            r = self._http().get(
                f"{_CLOB_BASE}/prices-history",
                params={
                    "market": outcome_id,
                    "startTs": now - int(window_hours) * 3600,
                    "endTs": now,
                    "fidelity": 60,  # 60-minute downsampling
                },
                timeout=_HTTP_TIMEOUT,
            )
            if not r.ok:
                self._record_call(success=False, error=f"clob {r.status_code}")
                return dict(_EMPTY_VELOCITY)
            history = r.json().get("history", [])
            self._record_call(success=True)
        except Exception as exc:
            logger.debug("get_price_velocity http failed: %s", exc)
            self._record_call(success=False, error=str(exc))
            return dict(_EMPTY_VELOCITY)

        return self._velocity_from_history(history)

    @staticmethod
    def _velocity_from_history(history: list[dict[str, Any]]) -> dict[str, Any]:
        """Convert CLOB /prices-history rows into the velocity dict.

        Each history row is ``{"t": unix_ts, "p": price}``. We compute
        1h, 6h, 24h velocities by indexing back from the latest tick.
        """
        if not history:
            return dict(_EMPTY_VELOCITY)
        rows = sorted(
            ((int(h.get("t", 0)), float(h.get("p", 0))) for h in history if h.get("t")),
            key=lambda x: x[0],
        )
        if not rows:
            return dict(_EMPTY_VELOCITY)
        latest_t, current = rows[-1]

        def _price_at(seconds_ago: int) -> float | None:
            target = latest_t - seconds_ago
            best: float | None = None
            for t, p in rows:
                if t <= target:
                    best = p
                else:
                    break
            return best

        p1 = _price_at(3600)
        p6 = _price_at(6 * 3600)
        p24 = _price_at(24 * 3600)

        def _vel(prev: float | None) -> float | None:
            if prev is None or prev == 0:
                return None
            return round((current - prev) / prev, 4)

        # CLOB /prices-history returns ticks not OHLCV — volume is not
        # available from this endpoint. Leave volume_* as None.
        return {
            "current_price": round(current, 4),
            "price_1h_ago": round(p1, 4) if p1 is not None else None,
            "velocity_1h": _vel(p1),
            "velocity_6h": _vel(p6),
            "velocity_24h": _vel(p24),
            "volume_1h": None,
            "volume_24h": None,
            "volume_ratio": None,
            "candles": [{"t": t, "c": p, "o": p, "h": p, "l": p, "v": 0} for t, p in rows],
            "available": True,
        }

    @staticmethod
    def _velocity_from_candles(candles: list[Any]) -> dict[str, Any]:
        """Pure-function: convert a candle list into the velocity dict.

        Accepts either ``pmxt.PriceCandle`` instances (with .open/.high/
        .low/.close/.volume/.timestamp attributes) or plain dicts.
        """
        if not candles:
            return dict(_EMPTY_VELOCITY)

        def _get(c: Any, key: str, alt: str | None = None) -> float | None:
            v = getattr(c, key, None)
            if v is None and alt:
                v = getattr(c, alt, None)
            if v is None and isinstance(c, dict):
                v = c.get(key) or (c.get(alt) if alt else None)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        normalized: list[dict[str, float | None]] = []
        for c in candles:
            normalized.append({
                "t": _get(c, "timestamp", "t"),
                "o": _get(c, "open", "o"),
                "h": _get(c, "high", "h"),
                "l": _get(c, "low", "l"),
                "c": _get(c, "close", "c"),
                "v": _get(c, "volume", "v") or 0.0,
            })

        # Sort ascending by ts so [-1] is latest
        normalized.sort(key=lambda x: x["t"] or 0)
        latest = normalized[-1]
        current_price = latest["c"]

        def _price_n_back(n: int) -> float | None:
            if len(normalized) <= n:
                return normalized[0]["c"] if normalized else None
            return normalized[-(n + 1)]["c"]

        def _vel(prev: float | None) -> float | None:
            if current_price is None or prev is None or prev == 0:
                return None
            return round((current_price - prev) / prev, 4)

        price_1h_ago = _price_n_back(1)
        price_6h_ago = _price_n_back(6)
        price_24h_ago = _price_n_back(24)

        # Volume aggregation
        vol_24h = sum((c["v"] or 0) for c in normalized[-24:])
        vol_1h = normalized[-1]["v"] or 0.0
        avg_hourly = (vol_24h / 24.0) if vol_24h > 0 else 0.0
        volume_ratio = round(vol_1h / avg_hourly, 3) if avg_hourly > 0 else None

        return {
            "current_price": current_price,
            "price_1h_ago": price_1h_ago,
            "velocity_1h": _vel(price_1h_ago),
            "velocity_6h": _vel(price_6h_ago),
            "velocity_24h": _vel(price_24h_ago),
            "volume_1h": round(vol_1h, 2),
            "volume_24h": round(vol_24h, 2),
            "volume_ratio": volume_ratio,
            "candles": normalized,
            "available": True,
        }

    def get_order_book_imbalance(self, outcome_id: str) -> dict[str, Any]:
        """Bid/ask imbalance over the top of book + 10% depth window.

        Direct CLOB call: ``/book?token_id=``.
        """
        if not outcome_id:
            return dict(_EMPTY_BOOK)
        try:
            r = self._http().get(
                f"{_CLOB_BASE}/book",
                params={"token_id": outcome_id},
                timeout=_HTTP_TIMEOUT,
            )
            if not r.ok:
                self._record_call(success=False, error=f"clob {r.status_code}")
                return dict(_EMPTY_BOOK)
            book = r.json()
            self._record_call(success=True)
        except Exception as exc:
            logger.debug("get_order_book_imbalance http failed: %s", exc)
            self._record_call(success=False, error=str(exc))
            return dict(_EMPTY_BOOK)

        return self._imbalance_from_book(book)

    @staticmethod
    def _imbalance_from_book(book: Any) -> dict[str, Any]:
        bids = getattr(book, "bids", None) or (book.get("bids") if isinstance(book, dict) else None) or []
        asks = getattr(book, "asks", None) or (book.get("asks") if isinstance(book, dict) else None) or []

        def _level(level: Any) -> tuple[float, float] | None:
            """Normalize a book level into (price, size). Handles
            pmxt OrderBookLevel, dicts, and 2-tuples."""
            if level is None:
                return None
            price = getattr(level, "price", None)
            size = getattr(level, "size", None)
            if price is None and isinstance(level, dict):
                price = level.get("price")
                size = level.get("size") if size is None else size
            if price is None and isinstance(level, (list, tuple)) and len(level) >= 2:
                price, size = level[0], level[1]
            try:
                return float(price), float(size or 0)
            except (TypeError, ValueError):
                return None

        norm_bids = [lv for lv in (_level(b) for b in bids) if lv]
        norm_asks = [lv for lv in (_level(a) for a in asks) if lv]
        if not norm_bids or not norm_asks:
            return dict(_EMPTY_BOOK)

        norm_bids.sort(key=lambda x: -x[0])  # high to low
        norm_asks.sort(key=lambda x: x[0])   # low to high

        best_bid = norm_bids[0][0]
        best_ask = norm_asks[0][0]
        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else None
        spread_pct = round(spread / mid, 4) if mid and mid > 0 else None

        bid_floor = best_bid * 0.90
        ask_ceiling = best_ask * 1.10
        bid_depth = round(sum(p * s for p, s in norm_bids if p >= bid_floor), 2)
        ask_depth = round(sum(p * s for p, s in norm_asks if p <= ask_ceiling), 2)
        denom = bid_depth + ask_depth
        imbalance = round((bid_depth - ask_depth) / denom, 4) if denom > 0 else 0.0

        return {
            "best_bid": round(best_bid, 4),
            "best_ask": round(best_ask, 4),
            "spread": round(spread, 4),
            "spread_pct": spread_pct,
            "bid_depth_10pct": bid_depth,
            "ask_depth_10pct": ask_depth,
            "imbalance": imbalance,
            "available": True,
        }

    def get_pre_entry_baseline(
        self,
        outcome_id: str,
        whale_entry_ts: int,
    ) -> dict[str, Any]:
        """Compare price 30 min before whale entry vs at-entry vs current.

        Used by the fusion score to detect "whale is late, info already
        leaked" patterns. ``whale_entry_ts`` is a Unix epoch second.
        """
        if not outcome_id or not whale_entry_ts:
            return dict(_EMPTY_BASELINE)
        try:
            r = self._http().get(
                f"{_CLOB_BASE}/prices-history",
                params={
                    "market": outcome_id,
                    "startTs": int(whale_entry_ts) - 7200,
                    "endTs": int(time.time()),
                    "fidelity": 5,  # 5-minute granularity for baseline
                },
                timeout=_HTTP_TIMEOUT,
            )
            if not r.ok:
                self._record_call(success=False, error=f"clob {r.status_code}")
                return dict(_EMPTY_BASELINE)
            history = r.json().get("history", [])
            self._record_call(success=True)
        except Exception as exc:
            logger.debug("get_pre_entry_baseline http failed: %s", exc)
            self._record_call(success=False, error=str(exc))
            return dict(_EMPTY_BASELINE)

        # Adapt the dict-style history to what _baseline_from_candles wants.
        candles = [{"t": h.get("t"), "c": h.get("p")} for h in history]
        return self._baseline_from_candles(candles, whale_entry_ts)

    @staticmethod
    def _baseline_from_candles(candles: list[Any], whale_entry_ts: int) -> dict[str, Any]:
        if not candles:
            return dict(_EMPTY_BASELINE)

        def _ts(c: Any) -> int:
            v = getattr(c, "timestamp", None)
            if v is None and isinstance(c, dict):
                v = c.get("t") or c.get("timestamp")
            if v is None:
                return 0
            try:
                # pmxt PriceCandle .timestamp is a datetime
                if hasattr(v, "timestamp"):
                    return int(v.timestamp())
                return int(v)
            except (TypeError, ValueError):
                return 0

        def _close(c: Any) -> float | None:
            v = getattr(c, "close", None)
            if v is None and isinstance(c, dict):
                v = c.get("c") or c.get("close")
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        rows = sorted(
            ((_ts(c), _close(c)) for c in candles),
            key=lambda x: x[0],
        )
        rows = [(t, p) for t, p in rows if t and p is not None]
        if not rows:
            return dict(_EMPTY_BASELINE)

        # Price 30 min before entry — pick the closest candle <= entry-1800
        before_target = whale_entry_ts - 1800
        price_before = None
        for t, p in rows:
            if t <= before_target:
                price_before = p
            else:
                break
        if price_before is None:
            price_before = rows[0][1]

        # Price at entry — closest candle with t <= entry
        price_at_entry = None
        for t, p in rows:
            if t <= whale_entry_ts:
                price_at_entry = p
            else:
                break
        if price_at_entry is None:
            price_at_entry = rows[0][1]

        price_now = rows[-1][1]
        whale_impact = round(price_at_entry - price_before, 4)
        post_drift = round(price_now - price_at_entry, 4)
        info_already_priced = abs(whale_impact) < 0.01 and abs(price_at_entry - rows[0][1]) > 0.05

        return {
            "price_before": round(price_before, 4),
            "price_at_entry": round(price_at_entry, 4),
            "price_now": round(price_now, 4),
            "whale_impact": whale_impact,
            "post_entry_drift": post_drift,
            "info_already_priced": bool(info_already_priced),
            "available": True,
        }

    # ── Aggregator + market quality score ───────────────────────────────────

    def get_market_microstructure(
        self,
        outcome_id: str | None = None,
        *,
        condition_id: str | None = None,
        direction: str = "YES",
        whale_entry_ts: int | None = None,
    ) -> dict[str, Any]:
        """All-in-one combined dict + market_quality_score (0..1).

        Either ``outcome_id`` or ``condition_id`` must be provided.
        """
        if not outcome_id and condition_id:
            outcome_id = self.resolve_outcome_id(
                condition_id=condition_id, direction=direction,
            )
        if not outcome_id:
            return self._empty_microstructure()

        velocity = self.get_price_velocity(outcome_id)
        book = self.get_order_book_imbalance(outcome_id)
        baseline = (
            self.get_pre_entry_baseline(outcome_id, whale_entry_ts)
            if whale_entry_ts else dict(_EMPTY_BASELINE)
        )
        quality = self._market_quality_score(velocity, book, baseline)
        return {
            "outcome_id": outcome_id,
            "velocity": velocity,
            "order_book": book,
            "baseline": baseline,
            "market_quality_score": quality,
            "available": velocity.get("available") or book.get("available"),
        }

    @staticmethod
    def _empty_microstructure() -> dict[str, Any]:
        return {
            "outcome_id": None,
            "velocity": dict(_EMPTY_VELOCITY),
            "order_book": dict(_EMPTY_BOOK),
            "baseline": dict(_EMPTY_BASELINE),
            "market_quality_score": None,
            "available": False,
        }

    @staticmethod
    def _market_quality_score(
        velocity: dict[str, Any],
        book: dict[str, Any],
        baseline: dict[str, Any],
    ) -> float | None:
        """Combine the three components into a single 0..1 score."""
        components: list[float] = []

        # Liquidity (spread + book depth)
        spread_pct = book.get("spread_pct")
        if spread_pct is not None:
            if spread_pct < 0.02:
                liq_score = 1.0
            elif spread_pct < 0.05:
                liq_score = 0.8
            elif spread_pct < 0.10:
                liq_score = 0.5
            else:
                liq_score = 0.2
            components.append(liq_score)

        # Recent volume / staleness
        volume_ratio = velocity.get("volume_ratio")
        if volume_ratio is not None:
            if volume_ratio > 3.0:
                vol_score = 0.9
            elif volume_ratio > 1.5:
                vol_score = 0.7
            elif volume_ratio > 0.5:
                vol_score = 0.5
            else:
                vol_score = 0.3
            components.append(vol_score)

        # Not already fully priced
        current = velocity.get("current_price")
        if current is not None:
            try:
                cf = float(current)
                if 0.05 < cf < 0.95:
                    components.append(1.0)
                elif 0.02 < cf < 0.98:
                    components.append(0.6)
                else:
                    components.append(0.2)
            except (TypeError, ValueError):
                pass

        # Whale hasn't already moved it
        if baseline.get("available"):
            if baseline.get("info_already_priced"):
                components.append(0.2)
            elif (baseline.get("whale_impact") or 0) > 0.05:
                components.append(0.6)
            else:
                components.append(1.0)

        if not components:
            return None
        return round(sum(components) / len(components), 4)
