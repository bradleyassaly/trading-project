"""
Pre-trade execution gates — must ALL pass before any trade (paper or live).

Each gate returns ``(passed, reason, data)`` where ``data`` is a dict of
metrics for post-trade analysis.

Failure policy (changed 2026-07-02):
- **paper**: all gates fail-open on API errors, so a flaky CLOB API never
  silently disables paper data collection.
- **live**: the two hard safety gates — **depth** and **exposure** — fail
  CLOSED on API errors. A degraded venue API is exactly when books are
  thinnest and exposure state is least knowable; real money does not
  trade blind. Spread and staleness remain fail-open in live (soft
  quality gates; blocking on them would halt trading on every book-API
  blip without a safety payoff).

Gates:
1. Spread/Edge — Is expected EV > transaction costs?
2. Depth — Is there enough liquidity to fill the order?
3. Price Staleness — Has the price already moved since the whale traded?
4. Exposure — Are we within category/position limits?
5. Drawdown — Are we in drawdown? Adjust size accordingly.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db import connect_wallet_db

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db")

# Thresholds — paper uses relaxed values; live overrides in constructor.
# 2026-05-02: 60-cap filled overnight (97 trades/24h after gate
# unblocking). Bumping to 150. Real fix is auto-closing stale
# positions, but cap raise is the immediate unblock. Live cap = 5.
# 2026-07-05: 150 saturated again with FRESH positions (oldest 10 days,
# archiver healthy) — the cap now throttles evidence collection that the
# scaling gates depend on. Paper positions cost nothing; 300 keeps the
# category-concentration math meaningful while doubling throughput.
_PAPER_THRESHOLDS = {
    "max_price_move_pct": 0.05,
    "max_category_pct": 0.30,
    "max_positions": 300,
    "depth_multiple": 2.0,
    "spread_safety_factor": 1.5,
}

_LIVE_THRESHOLDS = {
    "max_price_move_pct": 0.03,
    "max_category_pct": 0.25,
    "max_positions": 5,
    "depth_multiple": 3.0,
    "spread_safety_factor": 2.0,
}


class ExecutionGates:
    """Pre-trade gates that must ALL pass before execution."""

    def __init__(
        self,
        db_path: str | None = None,
        mode: str = "paper",
    ) -> None:
        self.db_path = db_path or _DEFAULT_DB
        self.mode = mode
        self.thresholds = _LIVE_THRESHOLDS if mode == "live" else _PAPER_THRESHOLDS
        # Hard safety gates (depth, exposure) fail closed in live mode.
        self._pass_on_error = mode != "live"

    # ── Gate 1: Spread vs Edge ──────────────────────────────────────────────

    def check_spread_edge(
        self, token_id: str | None, expected_ev: float,
    ) -> tuple[bool, str, dict]:
        """Is the expected EV greater than the spread cost?"""
        if not token_id or expected_ev <= 0:
            return True, "no_ev_data_pass", {"spread": None}

        try:
            import requests
            r = requests.get(
                f"https://clob.polymarket.com/book?token_id={token_id}",
                timeout=5,
            )
            if not r.ok:
                return True, "spread_unknown_pass", {"spread": None}

            book = r.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])

            if not bids or not asks:
                return False, "no_liquidity", {"spread": None, "bids": 0, "asks": 0}

            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
            spread = best_ask - best_bid
            mid = (best_ask + best_bid) / 2
            spread_pct = spread / mid if mid > 0 else 1.0

            estimated_cost = spread_pct * self.thresholds["spread_safety_factor"]

            if expected_ev <= estimated_cost:
                return False, (
                    f"edge_too_thin: EV={expected_ev:.3f} < cost={estimated_cost:.3f}"
                ), {
                    "spread": round(spread, 4),
                    "spread_pct": round(spread_pct, 4),
                    "estimated_cost": round(estimated_cost, 4),
                    "expected_ev": expected_ev,
                }

            return True, "spread_ok", {
                "spread": round(spread, 4),
                "spread_pct": round(spread_pct, 4),
                "estimated_cost": round(estimated_cost, 4),
                "expected_ev": expected_ev,
                "edge_after_costs": round(expected_ev - estimated_cost, 4),
            }
        except Exception as exc:
            return True, f"spread_check_failed: {exc}", {"spread": None}

    # ── Gate 2: Depth ───────────────────────────────────────────────────────

    def check_depth(
        self, token_id: str | None, stake: float,
    ) -> tuple[bool, str, dict]:
        """Is there enough liquidity to fill our order?"""
        if not token_id:
            return True, "no_token_pass", {}

        try:
            import requests
            r = requests.get(
                f"https://clob.polymarket.com/book?token_id={token_id}",
                timeout=5,
            )
            if not r.ok:
                if self._pass_on_error:
                    return True, "depth_unknown_pass", {}
                return False, f"depth_unknown_block_live: book API {r.status_code}", {}

            book = r.json()
            asks = book.get("asks", [])

            available = sum(
                float(a.get("size", 0)) * float(a.get("price", 0))
                for a in asks[:10]
            )

            needed = stake * self.thresholds["depth_multiple"]
            if available < needed:
                return False, (
                    f"insufficient_depth: ${available:.0f} < ${needed:.0f} needed"
                ), {"available_depth": round(available, 2), "stake": stake}

            return True, "depth_ok", {
                "available_depth": round(available, 2),
                "stake": stake,
            }
        except Exception as exc:
            if self._pass_on_error:
                return True, f"depth_check_failed: {exc}", {}
            return False, f"depth_check_failed_block_live: {exc}", {}

    # ── Gate 3: Price Staleness ─────────────────────────────────────────────

    def check_price_staleness(
        self,
        token_id: str | None,
        whale_trade_price: float | None,
        whale_trade_ts: int | None,
    ) -> tuple[bool, str, dict]:
        """Has the price already moved since the whale traded?"""
        if not token_id or not whale_trade_price or not whale_trade_ts:
            return True, "no_whale_data_skip", {}

        try:
            import requests
            r = requests.get(
                f"https://clob.polymarket.com/book?token_id={token_id}",
                timeout=5,
            )
            if not r.ok:
                return True, "price_check_failed", {}

            book = r.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if not bids or not asks:
                return False, "no_liquidity", {}

            current_mid = (float(bids[0]["price"]) + float(asks[0]["price"])) / 2
            price_move = (
                abs(current_mid - whale_trade_price) / whale_trade_price
                if whale_trade_price > 0 else 0
            )
            seconds_since = int(time.time()) - whale_trade_ts

            max_move = self.thresholds["max_price_move_pct"]
            if price_move > max_move:
                return False, (
                    f"price_moved: {price_move*100:.1f}% since whale entry "
                    f"(max {max_move*100:.0f}%)"
                ), {
                    "whale_price": whale_trade_price,
                    "current_price": round(current_mid, 4),
                    "move_pct": round(price_move, 4),
                    "seconds_since_whale": seconds_since,
                }

            return True, "price_fresh", {
                "whale_price": whale_trade_price,
                "current_price": round(current_mid, 4),
                "move_pct": round(price_move, 4),
                "seconds_since_whale": seconds_since,
            }
        except Exception as exc:
            return True, f"price_check_failed: {exc}", {}

    # ── Gate 4: Exposure ────────────────────────────────────────────────────

    def check_exposure(
        self,
        category: str,
        stake: float,
        bankroll: float,
    ) -> tuple[bool, str, dict]:
        """Does this trade exceed our category or position limits?"""
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                cat_exposure = conn.execute(
                    """SELECT COALESCE(SUM(size_usd), 0)
                       FROM polymarket_paper_trades
                       WHERE exit_ts IS NULL AND archived = 0
                         AND category = ?""",
                    (category,),
                ).fetchone()[0]

                open_count = conn.execute(
                    """SELECT COUNT(*)
                       FROM polymarket_paper_trades
                       WHERE exit_ts IS NULL AND archived = 0"""
                ).fetchone()[0]
            finally:
                conn.close()

            new_exposure = cat_exposure + stake
            cat_pct = new_exposure / bankroll if bankroll > 0 else 1.0
            max_cat = self.thresholds["max_category_pct"]
            max_pos = self.thresholds["max_positions"]

            if cat_pct > max_cat:
                return False, (
                    f"category_limit: {category} at {cat_pct*100:.0f}% "
                    f"(max {max_cat*100:.0f}%)"
                ), {
                    "category": category,
                    "current_exposure": round(cat_exposure, 2),
                    "new_exposure": round(new_exposure, 2),
                    "pct_of_bankroll": round(cat_pct, 4),
                }

            if open_count >= max_pos:
                return False, (
                    f"max_positions: {open_count}/{max_pos} open"
                ), {
                    "open_positions": open_count,
                    "max_positions": max_pos,
                }

            return True, "exposure_ok", {
                "category": category,
                "category_exposure_pct": round(cat_pct, 4),
                "open_positions": open_count + 1,
            }
        except Exception as exc:
            if self._pass_on_error:
                return True, f"exposure_check_failed: {exc}", {}
            return False, f"exposure_check_failed_block_live: {exc}", {}

    # ── Gate 5: Drawdown ────────────────────────────────────────────────────

    @staticmethod
    def check_drawdown(
        bankroll: float,
        starting_bankroll: float,
    ) -> tuple[bool, str, dict]:
        """Are we in drawdown? Returns a size_multiplier."""
        if starting_bankroll <= 0:
            return True, "no_baseline", {"size_multiplier": 1.0, "drawdown": 0}

        drawdown = max(0, (starting_bankroll - bankroll) / starting_bankroll)

        if drawdown >= 0.20:
            return False, (
                f"circuit_breaker_level: {drawdown*100:.0f}% drawdown"
            ), {"drawdown": round(drawdown, 4), "size_multiplier": 0.0}
        elif drawdown >= 0.15:
            return True, (
                f"severe_drawdown: {drawdown*100:.0f}%, quarter size"
            ), {"drawdown": round(drawdown, 4), "size_multiplier": 0.25}
        elif drawdown >= 0.10:
            return True, (
                f"moderate_drawdown: {drawdown*100:.0f}%, half size"
            ), {"drawdown": round(drawdown, 4), "size_multiplier": 0.5}

        return True, "no_drawdown", {
            "drawdown": round(drawdown, 4),
            "size_multiplier": 1.0,
        }

    # ── Run All Gates ───────────────────────────────────────────────────────

    def run_all_gates(
        self,
        *,
        token_id: str | None = None,
        expected_ev: float = 0,
        stake: float = 0,
        category: str = "other",
        bankroll: float = 100_000,
        starting_bankroll: float = 100_000,
        whale_trade_price: float | None = None,
        whale_trade_ts: int | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """Run ALL gates. Returns (should_trade, gate_results)."""
        results: dict[str, Any] = {}
        should_trade = True

        p, r, d = self.check_spread_edge(token_id, expected_ev)
        results["spread"] = {"passed": p, "reason": r, **d}
        if not p:
            should_trade = False

        p, r, d = self.check_depth(token_id, stake)
        results["depth"] = {"passed": p, "reason": r, **d}
        if not p:
            should_trade = False

        if whale_trade_price and whale_trade_ts:
            p, r, d = self.check_price_staleness(
                token_id, whale_trade_price, whale_trade_ts,
            )
            results["staleness"] = {"passed": p, "reason": r, **d}
            if not p:
                should_trade = False

        p, r, d = self.check_exposure(category, stake, bankroll)
        results["exposure"] = {"passed": p, "reason": r, **d}
        if not p:
            should_trade = False

        p, r, d = self.check_drawdown(bankroll, starting_bankroll)
        results["drawdown"] = {"passed": p, "reason": r, **d}
        if not p:
            should_trade = False

        dd_mult = results.get("drawdown", {}).get("size_multiplier", 1.0)
        results["adjusted_stake"] = round(stake * dd_mult, 2)
        results["size_multiplier"] = dd_mult

        return should_trade, results
