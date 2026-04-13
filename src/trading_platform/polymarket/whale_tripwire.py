"""
Whale detection tripwire for Polymarket WebSocket events.

Watches for trades from wallets with proven directional edge,
returning structured WhaleTrade objects for signal generation.

Usage::

    from trading_platform.polymarket.whale_tripwire import WhaleTripwire
    tripwire = WhaleTripwire()
    whale = tripwire.check_event(event_dict)
"""
from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db")


@dataclass
class WhaleTrade:
    wallet: str
    condition_id: str
    side: str                   # BUY or SELL
    price: float
    size: float
    outcome: str                # YES or NO
    question: str
    category: str
    timestamp: int
    wallet_tier: str            # tier1 or tier2
    directional_win_rate: float
    conviction_score: float
    total_volume_usdc: float


class WhaleTripwire:
    """Detects watched wallet activity in WebSocket events."""

    TIER1_MIN_WR = 0.58
    TIER1_MIN_RESOLVED = 10
    TIER1_MIN_VOLUME = 5_000
    TIER2_MIN_WR = 0.53
    TIER2_MIN_RESOLVED = 5
    TIER2_MIN_VOLUME = 1_000

    def __init__(
        self,
        db_path: str | Path | None = None,
        universe: Any | None = None,
    ) -> None:
        # Accept either a path or a WalletDB instance
        if db_path is None:
            self.db_path = _DEFAULT_DB
        elif hasattr(db_path, "_path"):
            # WalletDB instance
            self.db_path = str(db_path._path)
        else:
            self.db_path = str(db_path)
        self.universe = universe
        # watched_tier1 = combined tier1 + tier1h (used by check_event lookup)
        self.watched_tier1: set[str] = set()
        self.watched_tier2: set[str] = set()
        # Distinct sets for tier1h vs tier1 (used by signal engine + telegram)
        self._tier1h: set[str] = set()
        self._tier1: set[str] = set()
        self._tier2: set[str] = set()
        self.leaderboard: dict[str, dict] = {}
        self._last_reload: float = 0
        self.reload()

    @property
    def all_watched(self) -> set[str]:
        """Combined set of all watched wallet addresses."""
        return self._tier1h | self._tier1 | self._tier2

    def reload(self) -> None:
        """Load watched wallets from leaderboard table (atomic, version-checked).

        Falls back to wallet_profiles if leaderboard is not yet populated.

        Resilience: retries up to 5× with exponential backoff on
        ``database is locked``. If all retries fail, keeps the last
        successfully-loaded wallet sets in memory rather than going
        blind. This is the **single most critical resilience point**
        in the system: a WhaleTripwire with 0 watched wallets means
        whale detection is silently disabled. Recorded as a CRITICAL
        log line so operators can act.
        """
        # Snapshot the previous state so we can restore on failure.
        prev_t1 = set(self.watched_tier1)
        prev_t2 = set(self.watched_tier2)
        prev_t1h = set(self._tier1h)
        prev_t1_only = set(self._tier1)
        prev_t2_only = set(self._tier2)
        prev_lb = dict(self.leaderboard)

        for attempt in range(5):
            try:
                self._reload_inner()
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                delay = 1.0 * (2 ** attempt)
                print(
                    f"[WhaleTripwire] DB locked on reload attempt {attempt+1}/5, "
                    f"retrying in {delay:.0f}s..."
                )
                time.sleep(delay)
            except Exception as exc:
                print(f"[WhaleTripwire] reload failed: {exc}")
                break

        # All retries exhausted — restore previous state if we had one,
        # otherwise we're going blind and that needs to be loud.
        if prev_t1 or prev_t2:
            self.watched_tier1 = prev_t1
            self.watched_tier2 = prev_t2
            self._tier1h = prev_t1h
            self._tier1 = prev_t1_only
            self._tier2 = prev_t2_only
            self.leaderboard = prev_lb
            print(
                f"[WhaleTripwire] DB locked after 5 retries — using cached "
                f"wallet set ({len(prev_t1)} tier1+1h, {len(prev_t2)} tier2)"
            )
        else:
            print(
                "[WhaleTripwire] CRITICAL: DB locked, no cached wallets — "
                "whale detection BLIND. Run: trading-cli data polymarket build-leaderboard"
            )

    def _reload_inner(self) -> None:
        """Actual reload logic — separated so reload() can wrap it in retry."""
        self.watched_tier1.clear()
        self.watched_tier2.clear()
        self._tier1h.clear()
        self._tier1.clear()
        self._tier2.clear()
        self.leaderboard.clear()

        try:
            conn = sqlite3.connect(self.db_path, timeout=60)
            conn.execute("PRAGMA busy_timeout=60000")
            conn.row_factory = sqlite3.Row

            # Try leaderboard first (atomic, with rolling WR demotion applied)
            meta = conn.execute(
                "SELECT is_valid, current_version FROM leaderboard_meta WHERE id=1"
            ).fetchone()

            if meta and meta["is_valid"]:
                rows = conn.execute(
                    """SELECT wallet, tier, directional_win_rate, rolling_20_wr,
                              conviction_score, resolved_trades, total_volume_usdc,
                              wallet_type, version, net_pnl_usdc, leaderboard_source, pseudonym
                       FROM leaderboard"""
                ).fetchall()
                # Also pull alpha-copyable wallets that are NOT in the
                # leaderboard. These are per-category specialists whose
                # lifetime WR is below the leaderboard cutoff but who
                # have proven edge in a specific category. Without this
                # join, the alpha gate has them but live-collect doesn't
                # watch them — they're invisible. See
                # ground_truth_validation.md for the diagnosis.
                alpha_extras = conn.execute(
                    """SELECT DISTINCT wa.wallet
                       FROM wallet_alpha_scores wa
                       WHERE wa.is_copyable = 1
                         AND wa.wallet NOT IN (SELECT wallet FROM leaderboard)"""
                ).fetchall()
                conn.close()

                for row in rows:
                    w = row["wallet"]
                    profile = dict(row)
                    self.leaderboard[w] = profile
                    # tier1h wallets join watched_tier1 set (treated as tier1 for confidence)
                    if row["tier"] == "tier1h":
                        self.watched_tier1.add(w)
                        self._tier1h.add(w)
                    elif row["tier"] == "tier1":
                        self.watched_tier1.add(w)
                        self._tier1.add(w)
                    elif row["tier"] == "tier2":
                        self.watched_tier2.add(w)
                        self._tier2.add(w)

                # Add the alpha-only wallets as tier2 watchers (they
                # don't pass the leaderboard's lifetime gate but they
                # have category-specific edge worth watching).
                alpha_added = 0
                for r in alpha_extras:
                    w = r["wallet"]
                    if w not in self.leaderboard:
                        self.leaderboard[w] = {"wallet": w, "tier": "tier2_alpha",
                                               "leaderboard_source": "alpha_only"}
                        self.watched_tier2.add(w)
                        self._tier2.add(w)
                        alpha_added += 1

                self._last_reload = time.time()
                t1 = len(self.watched_tier1)
                t2 = len(self.watched_tier2)
                t1h = sum(1 for p in self.leaderboard.values() if p.get("tier") == "tier1h")
                pm_sourced = sum(
                    1 for p in self.leaderboard.values()
                    if (p.get("leaderboard_source") or "").startswith("polymarket_")
                )
                local_sourced = t1h - pm_sourced
                ver = meta["current_version"]
                print(
                    f"[WhaleTripwire] Loaded {t1 + t2} wallets from leaderboard v{ver} — "
                    f"tier1+1h: {t1} (incl {t1h} high-conviction: {local_sourced} local, {pm_sourced} polymarket), "
                    f"tier2: {t2} (incl {alpha_added} alpha-copyable specialists)"
                )

                if t1 + t2 == 0:
                    print("[WhaleTripwire] WARNING: Leaderboard valid but 0 wallets.")
                    print("  Run: trading-cli data polymarket build-leaderboard")
                return

            # Fallback: read directly from wallet_profiles
            rows = conn.execute(
                """SELECT wallet, directional_win_rate, resolved_trades,
                          total_volume_usdc, conviction_score, wallet_type
                   FROM wallet_profiles
                   WHERE directional_win_rate IS NOT NULL
                     AND resolved_trades IS NOT NULL
                     AND total_volume_usdc IS NOT NULL"""
            ).fetchall()
            conn.close()
        except Exception as exc:
            logger.warning("WhaleTripwire: Failed to load: %s", exc)
            print(f"[WhaleTripwire] WARNING: Could not load wallets: {exc}")
            self._last_reload = time.time()
            return

        for row in rows:
            w = row["wallet"]
            wr = row["directional_win_rate"] or 0
            resolved = row["resolved_trades"] or 0
            volume = row["total_volume_usdc"] or 0
            profile = dict(row)
            self.leaderboard[w] = profile

            if (wr >= self.TIER1_MIN_WR
                    and resolved >= self.TIER1_MIN_RESOLVED
                    and volume >= self.TIER1_MIN_VOLUME):
                self.watched_tier1.add(w)
                self._tier1.add(w)
            elif (wr >= self.TIER2_MIN_WR
                  and resolved >= self.TIER2_MIN_RESOLVED
                  and volume >= self.TIER2_MIN_VOLUME):
                self.watched_tier2.add(w)
                self._tier2.add(w)

        self._last_reload = time.time()
        t1 = len(self.watched_tier1)
        t2 = len(self.watched_tier2)
        print(f"[WhaleTripwire] Loaded {t1 + t2} wallets from profiles (fallback) — tier1: {t1}, tier2: {t2}")

        if t1 + t2 == 0:
            print("[WhaleTripwire] WARNING: 0 watched wallets loaded.")
            print("  Run: trading-cli data polymarket wallet-profiles --from-db")
            print("  Then: trading-cli data polymarket build-leaderboard")

    def maybe_reload(self, interval_seconds: float = 21600) -> None:
        if time.time() - self._last_reload > interval_seconds:
            self.reload()

    def check_event(self, event: dict) -> WhaleTrade | None:
        """Check a single WebSocket event for watched wallet activity."""
        if not isinstance(event, dict):
            return None

        # Find wallet address in event
        wallet = None
        tier = None
        for field in ("maker", "taker", "maker_address", "taker_address",
                      "funder", "trader", "owner", "user"):
            addr = event.get(field)
            if addr and isinstance(addr, str):
                addr = addr.strip().lower()
                t = self.tier(addr)
                if t:
                    wallet = addr
                    tier = t
                    break

        if not wallet or not tier:
            return None

        # Resolve condition_id
        condition_id = (
            event.get("condition_id")
            or event.get("market")
            or event.get("asset_id", "")
        )

        # If we have a universe, try token→condition mapping
        if self.universe and not condition_id:
            token_map = self.universe.get_token_to_condition_map()
            for field in ("asset_id", "token_id", "maker_asset_id", "taker_asset_id"):
                tid = event.get(field)
                if tid and tid in token_map:
                    condition_id = token_map[tid]
                    break

        if not condition_id:
            return None

        # Check if in universe
        if self.universe:
            if condition_id not in self.universe.get_all_condition_ids():
                return None
            question = self.universe.get_question(condition_id)
            category = self.universe.get_category(condition_id)
        else:
            question = event.get("question", event.get("title", ""))
            category = event.get("category", "other")

        # Determine side and outcome
        side_raw = (event.get("side") or "").upper()
        outcome = (event.get("outcome") or event.get("outcome_label") or "YES").upper()
        if side_raw in ("BUY", "SELL"):
            side = side_raw
        elif event.get("maker_asset_id") == "0":
            side = "BUY"
            outcome = "YES"
        else:
            side = "BUY"

        # Price and size
        try:
            price = float(event.get("price", 0) or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            size = float(
                event.get("size", 0)
                or event.get("amount", 0)
                or event.get("maker_amount", 0)
                or 0
            )
        except (TypeError, ValueError):
            size = 0.0

        # Timestamp
        ts = event.get("timestamp")
        if isinstance(ts, (int, float)):
            timestamp = int(ts)
        else:
            timestamp = int(time.time())

        profile = self.leaderboard.get(wallet, {})

        return WhaleTrade(
            wallet=wallet,
            condition_id=condition_id,
            side=side,
            price=price,
            size=size,
            outcome=outcome,
            question=question,
            category=category,
            timestamp=timestamp,
            wallet_tier=tier,
            directional_win_rate=profile.get("directional_win_rate", 0) or 0,
            conviction_score=profile.get("conviction_score", 0) or 0,
            total_volume_usdc=profile.get("total_volume_usdc", 0) or 0,
        )

    def tier(self, wallet: str) -> str | None:
        w = wallet.strip().lower()
        if w in self.watched_tier1:
            # Distinguish tier1h from tier1 by looking at leaderboard profile
            profile = self.leaderboard.get(w) or self.leaderboard.get(wallet)
            if profile and profile.get("tier") == "tier1h":
                return "tier1h"
            return "tier1"
        if w in self.watched_tier2:
            return "tier2"
        return None
