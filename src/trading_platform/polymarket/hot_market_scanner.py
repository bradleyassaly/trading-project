"""
Hot market scanner — auto-discovers high-volume Polymarket markets.

Polls Gamma API for the top markets by 24h volume + liquidity, optionally
checks order book imbalance via CLOB, and adds qualifying markets to the
live monitoring universe.

Designed to run as a daemon thread alongside the live collector. Uses
``MarketUniverse.add_wallet_derived_markets()`` to surface new markets
without modifying the persisted whale-derived universe semantics.

Usage::

    from trading_platform.polymarket.hot_market_scanner import HotMarketScanner
    scanner = HotMarketScanner(universe, db_path)
    new = scanner.scan_once()
    # or:
    scanner.run_background(interval=300)
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"


class HotMarketScanner:
    """Discover high-volume markets and add them to the universe."""

    # Auto-add thresholds
    VOLUME_24H_THRESHOLD = 500_000   # $500K/24h = notable
    LIQUIDITY_THRESHOLD = 100_000    # $100K open interest
    ORDER_BOOK_IMBALANCE = 0.72      # 72% one-sided
    TOP_N_BY_VOLUME = 100
    TOP_N_BY_LIQUIDITY = 50
    OB_CHECK_LIMIT = 50              # max order book lookups per cycle

    def __init__(self, universe: Any, db_path: str | None = None) -> None:
        self._universe = universe
        self._db_path = db_path
        self._ob_imbalance_history: dict[str, list[float]] = {}
        self._stop_event = threading.Event()

    # ── Gamma API helpers ────────────────────────────────────────────────────

    def fetch_top_markets(self, order: str = "volume24hr", limit: int = 100) -> list[dict]:
        """Fetch top active markets from Gamma."""
        try:
            r = requests.get(
                GAMMA_URL,
                params={
                    "active": "true", "closed": "false",
                    "limit": limit, "order": order, "ascending": "false",
                },
                timeout=10,
            )
            data = r.json()
            return data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
        except Exception as exc:
            logger.debug("Gamma fetch failed (order=%s): %s", order, exc)
            return []

    # ── Order book sanity check ──────────────────────────────────────────────

    def check_order_book_imbalance(self, token_id: str) -> float:
        """Single-shot CLOB book fetch → bid/ask imbalance ratio (0..1)."""
        try:
            r = requests.get(CLOB_BOOK_URL, params={"token_id": token_id}, timeout=5)
            if r.status_code != 200:
                return 0.5
            book = r.json()
        except Exception:
            return 0.5

        bids = book.get("bids") or []
        asks = book.get("asks") or []

        def _usd(levels: list[dict], n: int = 15) -> float:
            total = 0.0
            for lvl in levels[:n]:
                try:
                    total += float(lvl.get("price") or 0) * float(lvl.get("size") or 0)
                except (TypeError, ValueError):
                    continue
            return total

        bid_usd = _usd(bids)
        ask_usd = _usd(asks)
        total = bid_usd + ask_usd
        return bid_usd / total if total > 1000 else 0.5

    # ── Main scan ────────────────────────────────────────────────────────────

    def scan_once(self) -> list[dict[str, Any]]:
        """One scan cycle. Returns list of newly added / flagged markets."""
        by_volume = self.fetch_top_markets("volume24hr", self.TOP_N_BY_VOLUME)
        by_liquidity = self.fetch_top_markets("liquidity", self.TOP_N_BY_LIQUIDITY)

        seen: set[str] = set()
        candidates: list[dict] = []
        for m in by_volume + by_liquidity:
            cid = m.get("conditionId") or ""
            if cid and cid not in seen:
                seen.add(cid)
                candidates.append(m)

        logger.info("[HOT_SCAN] Checking %d candidate markets", len(candidates))

        new_markets: list[dict[str, Any]] = []
        ob_checks_done = 0

        for m in candidates:
            cid = m.get("conditionId") or ""
            if not cid:
                continue

            try:
                vol_24h = float(m.get("volume24hr") or m.get("volume") or 0)
            except (TypeError, ValueError):
                vol_24h = 0.0
            try:
                liquidity = float(m.get("liquidity") or 0)
            except (TypeError, ValueError):
                liquidity = 0.0
            question = m.get("question", "") or ""
            category = m.get("category", "") or "other"

            tokens: list[str] = []
            try:
                raw = m.get("clobTokenIds") or "[]"
                tokens = json.loads(raw) if isinstance(raw, str) else list(raw or [])
            except Exception:
                tokens = []

            already_watching = False
            try:
                q = self._universe.get_question(cid)
                already_watching = bool(q)
            except Exception:
                already_watching = False

            reasons: list[str] = []
            if vol_24h >= self.VOLUME_24H_THRESHOLD:
                reasons.append(f"vol24h=${vol_24h:,.0f}")
            if liquidity >= self.LIQUIDITY_THRESHOLD:
                reasons.append(f"liquidity=${liquidity:,.0f}")

            # Order book check (rate-limited per cycle)
            imbalance = None
            if (
                tokens
                and reasons
                and ob_checks_done < self.OB_CHECK_LIMIT
            ):
                imbalance = self.check_order_book_imbalance(tokens[0])
                ob_checks_done += 1
                hist = self._ob_imbalance_history.setdefault(cid, [])
                hist.append(imbalance)
                if len(hist) > 10:
                    self._ob_imbalance_history[cid] = hist[-10:]
                if imbalance >= self.ORDER_BOOK_IMBALANCE:
                    reasons.append(f"ob_imbalance={imbalance:.0%}")
                time.sleep(0.2)

            if not reasons:
                continue

            if not already_watching and vol_24h >= self.VOLUME_24H_THRESHOLD:
                try:
                    self._universe.add_wallet_derived_markets({cid})
                    logger.info(
                        "[HOT_SCAN] Added market: %r (%s)",
                        question[:50], ", ".join(reasons),
                    )
                    new_markets.append({
                        "condition_id": cid,
                        "question": question,
                        "category": category,
                        "volume_24h": vol_24h,
                        "liquidity": liquidity,
                        "imbalance": imbalance,
                        "reasons": reasons,
                        "added": True,
                    })
                except Exception as exc:
                    logger.warning("[HOT_SCAN] add failed for %s: %s", cid[:20], exc)
                    continue

            # Even if already in universe, surface escalating imbalances
            ob_hist = self._ob_imbalance_history.get(cid, [])
            if (
                len(ob_hist) >= 2
                and ob_hist[-1] > ob_hist[-2]
                and ob_hist[-1] >= self.ORDER_BOOK_IMBALANCE
            ):
                new_markets.append({
                    "condition_id": cid,
                    "question": question,
                    "category": category,
                    "alert_type": "order_book_accumulation",
                    "imbalance": ob_hist[-1],
                    "imbalance_delta": round(ob_hist[-1] - ob_hist[-2], 4),
                    "added": False,
                })

        # Telegram batch alert for newly added markets only.
        added = [m for m in new_markets if m.get("added")]
        if added:
            try:
                from trading_platform.polymarket.telegram_alerts import get_alerter
                alerter = get_alerter()
                if alerter.enabled:
                    alerter.send_hot_market_discovered(added)
            except Exception as exc:
                logger.debug("hot market telegram failed: %s", exc)

        return new_markets

    # ── Background loop ──────────────────────────────────────────────────────

    def run_background(self, interval: int = 300) -> threading.Thread:
        """Run ``scan_once`` every ``interval`` seconds in a daemon thread."""
        def _loop() -> None:
            while not self._stop_event.is_set():
                try:
                    new = self.scan_once()
                    if new:
                        logger.info("[HOT_SCAN] Found %d hot markets this cycle", len(new))
                except Exception as exc:
                    logger.error("[HOT_SCAN] cycle failed: %s", exc)
                # Interruptible sleep
                self._stop_event.wait(timeout=interval)

        t = threading.Thread(target=_loop, daemon=True, name="hot_market_scanner")
        t.start()
        logger.info("[HOT_SCAN] Scanner started (interval=%ds)", interval)
        return t

    def stop(self) -> None:
        self._stop_event.set()
