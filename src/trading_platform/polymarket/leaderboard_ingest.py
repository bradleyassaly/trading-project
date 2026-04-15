"""
Polymarket leaderboard ingestion.

Discovers top wallets from Polymarket's official leaderboard API and
backfills their trade history. This complements our bottom-up trade
ingestion by ensuring known top traders are in our dataset.

Usage::

    from trading_platform.polymarket.leaderboard_ingest import PolymarketLeaderboardIngestor
    ing = PolymarketLeaderboardIngestor()
    summary = ing.ingest_top_wallets(limit=200)
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.request
from typing import Any

import requests

from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)

LEADERBOARD_PROFIT_URL = "https://lb-api.polymarket.com/profit"
LEADERBOARD_VOLUME_URL = "https://lb-api.polymarket.com/volume"
DATA_API_TRADES = "https://data-api.polymarket.com/trades"
PAGE_SIZE = 50


class PolymarketLeaderboardIngestor:
    """Fetch Polymarket's official leaderboard and backfill missing wallets."""

    def __init__(self, db: WalletDB | None = None) -> None:
        self.db = db or WalletDB()

    def _fetch_paginated(self, base_url: str, limit: int, window: str) -> list[dict[str, Any]]:
        """Fetch leaderboard with pagination.

        Note: Polymarket only paginates the 'all' window. The 30d/7d/1d windows
        ignore offset and always return the same top 50, so we cap and warn.
        """
        # Time-bound windows are capped at 50 by the API
        if window in ("30d", "7d", "1d", "1h") and limit > PAGE_SIZE:
            logger.warning("window=%s is capped at %d wallets by Polymarket API", window, PAGE_SIZE)
            print(f"  [INFO] window={window} is capped at {PAGE_SIZE} wallets by Polymarket API")
            limit = PAGE_SIZE

        all_wallets: list[dict[str, Any]] = []
        offset = 0
        rank_offset = 0
        while len(all_wallets) < limit:
            url = f"{base_url}?window={window}&limit={PAGE_SIZE}&offset={offset}"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read())
            except Exception as exc:
                logger.warning("Leaderboard fetch failed at offset %d: %s", offset, exc)
                break

            items = data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
            if not items:
                break

            for i, entry in enumerate(items):
                wallet = (entry.get("proxyWallet") or "").lower()
                if not wallet:
                    continue
                all_wallets.append({
                    "wallet": wallet,
                    "profit": entry.get("amount", 0),
                    "pseudonym": entry.get("pseudonym", ""),
                    "name": entry.get("name", ""),
                    "rank": rank_offset + i + 1,
                })
                if len(all_wallets) >= limit:
                    break

            # Single-fetch windows: stop after one page
            if window in ("30d", "7d", "1d", "1h"):
                break
            if len(items) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            rank_offset += PAGE_SIZE
            time.sleep(0.3)

        return all_wallets[:limit]

    def fetch_top_wallets(self, limit: int = 200, window: str = "all") -> list[dict[str, Any]]:
        """Fetch top wallets by profit (paginated for 'all', single-fetch otherwise)."""
        return self._fetch_paginated(LEADERBOARD_PROFIT_URL, limit, window)

    def fetch_top_by_volume(self, limit: int = 200, window: str = "all") -> list[dict[str, Any]]:
        """Fetch top wallets by trading volume (market makers, active traders)."""
        return self._fetch_paginated(LEADERBOARD_VOLUME_URL, limit, window)

    # ── Market-based discovery ──────────────────────────────────────────────

    def discover_wallets_from_markets(
        self,
        markets_limit: int = 50,
        traders_per_market: int = 100,
        days_back: int = 30,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Reconstruct top traders by scanning recent high-volume markets.

        Step 1: Top N markets by volume (closed in last days_back days)
        Step 2: For each market, fetch top traders
        Step 3: Deduplicate, find missing wallets
        Step 4: Backfill missing wallet histories
        """
        t0 = time.time()
        print(f"Discovering wallets from top {markets_limit} markets (last {days_back}d)...")

        # Step 1: Get top markets by volume
        markets = self._fetch_top_recent_markets(markets_limit, days_back)
        if not markets:
            print("[WARNING] No markets fetched — skipping discovery")
            return {
                "markets_scanned": 0, "unique_wallets_found": 0,
                "wallets_already_in_db": 0, "wallets_new": 0,
                "trades_fetched": 0,
                "elapsed_seconds": round(time.time() - t0, 1),
            }
        print(f"  Found {len(markets)} markets")

        # Step 2: Fetch traders per market
        unique_wallets: set[str] = set()
        for i, m in enumerate(markets, 1):
            cid = m.get("conditionId") or m.get("condition_id")
            if not cid:
                continue
            try:
                traders = self._fetch_market_traders(cid, limit=traders_per_market)
                unique_wallets.update(traders)
                if i % 5 == 0 or i == len(markets):
                    print(f"  [{i}/{len(markets)}] {len(unique_wallets)} unique wallets so far")
            except Exception as exc:
                logger.debug("Market %s fetch failed: %s", cid[:16], exc)
            time.sleep(0.3)

        print(f"  Total unique wallets: {len(unique_wallets)}")

        # Step 3: Find missing
        with self.db._lock:
            existing = {
                r[0].lower() for r in self.db._conn.execute(
                    "SELECT DISTINCT wallet FROM wallet_trades"
                ).fetchall()
            }
        missing = [w for w in unique_wallets if w.lower() not in existing]
        already = len(unique_wallets) - len(missing)
        print(f"  Already in DB: {already}")
        print(f"  Missing: {len(missing)}")

        if dry_run:
            return {
                "markets_scanned": len(markets),
                "unique_wallets_found": len(unique_wallets),
                "wallets_already_in_db": already,
                "wallets_new": len(missing),
                "trades_fetched": 0,
                "elapsed_seconds": round(time.time() - t0, 1),
                "dry_run": True,
            }

        # Step 4: Backfill missing wallets
        total = 0
        for i, wallet in enumerate(missing, 1):
            print(f"  [{i}/{len(missing)}] Backfilling {wallet[:16]}...", end=" ", flush=True)
            n = self.fetch_wallet_history(wallet)
            total += n
            print(f"{n} trades")
            time.sleep(0.5)

        return {
            "markets_scanned": len(markets),
            "unique_wallets_found": len(unique_wallets),
            "wallets_already_in_db": already,
            "wallets_new": len(missing),
            "trades_fetched": total,
            "elapsed_seconds": round(time.time() - t0, 1),
        }

    def _fetch_top_recent_markets(self, limit: int, days_back: int) -> list[dict]:
        """Fetch top markets by volume that closed in the last N days."""
        cutoff = int(time.time()) - days_back * 86400
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={
                    "order": "volumeNum",
                    "ascending": "false",
                    "limit": limit * 2,  # over-fetch for date filtering
                    "closed": "true",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Top markets fetch failed: %s", exc)
            return []

        if not isinstance(data, list):
            return []

        # Filter by close date
        filtered: list[dict] = []
        for m in data:
            end_iso = m.get("endDate") or m.get("endDateIso") or ""
            if not end_iso:
                continue
            try:
                from datetime import datetime as _dt
                end_ts = int(_dt.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp())
                if end_ts >= cutoff:
                    filtered.append(m)
            except Exception:
                continue
            if len(filtered) >= limit:
                break

        return filtered[:limit]

    def _fetch_market_traders(self, condition_id: str, limit: int = 100) -> set[str]:
        """Fetch unique trader addresses from a market's recent trades."""
        wallets: set[str] = set()
        url = f"{DATA_API_TRADES}?market={condition_id}&limit={limit}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                trades = json.loads(resp.read())
        except Exception as exc:
            logger.debug("Market traders fetch failed: %s", exc)
            return wallets

        if not isinstance(trades, list):
            return wallets

        for t in trades:
            for field in ("proxyWallet", "maker", "taker", "user"):
                addr = (t.get(field) or "").lower()
                if addr and addr.startswith("0x"):
                    wallets.add(addr)
        return wallets

    def get_missing_wallets(self, top_wallets: list[dict]) -> list[str]:
        """Return wallet addresses from leaderboard not yet in our DB."""
        if not top_wallets:
            return []
        with self.db._lock:
            existing = {
                r[0].lower() for r in self.db._conn.execute(
                    "SELECT DISTINCT wallet FROM wallet_trades"
                ).fetchall()
            }
        return [w["wallet"] for w in top_wallets if w["wallet"].lower() not in existing]

    def fetch_wallet_history(self, wallet_address: str, limit_per_page: int = 500, max_pages: int = 20) -> int:
        """Fetch all trades for a wallet from data-api. Returns trades inserted."""
        inserted = 0
        offset = 0
        for _page in range(max_pages):
            url = f"{DATA_API_TRADES}?user={wallet_address}&limit={limit_per_page}&offset={offset}"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    trades = json.loads(resp.read())
            except Exception as exc:
                logger.debug("Trade fetch failed for %s offset %d: %s", wallet_address[:10], offset, exc)
                break

            if not trades or not isinstance(trades, list):
                break

            for t in trades:
                if self._insert_trade(wallet_address, t):
                    inserted += 1

            if len(trades) < limit_per_page:
                break
            offset += limit_per_page
            time.sleep(0.5)

        return inserted

    def _insert_trade(self, wallet: str, trade: dict[str, Any]) -> bool:
        """Insert a single trade into wallet_trades. Returns True if new."""
        tx = trade.get("transactionHash") or trade.get("transaction_hash")
        if not tx:
            return False

        ts = trade.get("timestamp") or trade.get("createdAt") or 0
        if isinstance(ts, str):
            try:
                from datetime import datetime as _dt
                ts = int(_dt.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
            except Exception:
                ts = 0

        side = (trade.get("side") or "").upper()
        if not side:
            # Polymarket data-api uses 'BUY'/'SELL' or maker/taker semantics
            side = "BUY" if trade.get("type") == "buy" else "SELL"

        try:
            return self.db.upsert_trade(
                wallet=wallet.lower(),
                proxy_wallet=wallet.lower(),
                asset=trade.get("asset") or trade.get("token_id", ""),
                condition_id=trade.get("conditionId") or trade.get("condition_id", ""),
                side=side,
                size=float(trade.get("size") or trade.get("amount") or 0),
                price=float(trade.get("price") or 0),
                timestamp=int(ts),
                title=trade.get("title", ""),
                slug=trade.get("slug", ""),
                outcome=trade.get("outcome", ""),
                event_slug=trade.get("eventSlug") or trade.get("event_slug", ""),
                transaction_hash=tx,
                synced_at=int(time.time()),
            )
        except Exception:
            return False

    def upsert_to_leaderboard(self, top_wallets: list[dict], window: str) -> int:
        """Upsert Polymarket-sourced wallets to the leaderboard table as tier1h.

        Skips wallets already classified locally as tier1h to avoid downgrades.
        Updates pseudonym field on wallet_profiles for known names.
        """
        if not top_wallets:
            return 0

        source_label = f"polymarket_{window}"
        upserted = 0
        try:
            import sqlite3 as _sq
            conn = _sq.connect(str(self.db._path))

            # Ensure schema
            cols = {r[1] for r in conn.execute("PRAGMA table_info(leaderboard)").fetchall()}
            for col, typedef in [
                ("leaderboard_source", "TEXT"),
                ("pseudonym", "TEXT"),
                ("net_pnl_usdc", "REAL"),
                ("pm_pnl", "REAL"),
                ("pm_rank", "INTEGER"),
            ]:
                if col not in cols:
                    conn.execute(f"ALTER TABLE leaderboard ADD COLUMN {col} {typedef}")

            wp_cols = {r[1] for r in conn.execute("PRAGMA table_info(wallet_profiles)").fetchall()}
            if "pseudonym" not in wp_cols:
                conn.execute("ALTER TABLE wallet_profiles ADD COLUMN pseudonym TEXT")

            for w in top_wallets:
                wallet = (w.get("wallet") or "").lower()
                if not wallet:
                    continue
                pnl = w.get("profit", 0) or 0
                pseudonym = w.get("pseudonym") or w.get("name") or ""
                rank = w.get("rank", 0)

                # Check if already locally classified as tier1h
                existing = conn.execute(
                    "SELECT tier, leaderboard_source FROM leaderboard WHERE wallet = ?",
                    (wallet,),
                ).fetchone()
                if existing and existing[0] == "tier1h" and (existing[1] or "local") == "local":
                    # Don't downgrade — but still update pseudonym
                    if pseudonym:
                        conn.execute(
                            "UPDATE leaderboard SET pseudonym = ? WHERE wallet = ?",
                            (pseudonym, wallet),
                        )
                    continue

                # Upsert as polymarket-sourced tier1h
                # pm_pnl/pm_rank = authoritative Polymarket-reported values
                # net_pnl_usdc kept in sync for back-compat with existing queries
                conn.execute("""
                    INSERT INTO leaderboard
                        (wallet, category, tier, leaderboard_source, net_pnl_usdc, pm_pnl, pseudonym, rank, pm_rank, updated_at)
                    VALUES (?, 'overall', 'tier1h', ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(wallet) DO UPDATE SET
                        category = COALESCE(leaderboard.category, 'overall'),
                        tier = 'tier1h',
                        leaderboard_source = excluded.leaderboard_source,
                        net_pnl_usdc = excluded.net_pnl_usdc,
                        pm_pnl = excluded.pm_pnl,
                        pseudonym = COALESCE(excluded.pseudonym, leaderboard.pseudonym),
                        rank = excluded.rank,
                        pm_rank = excluded.pm_rank,
                        updated_at = excluded.updated_at
                """, (wallet, source_label, pnl, pnl, pseudonym, rank, rank, int(time.time())))
                upserted += 1

                # Mirror pseudonym onto wallet_profiles
                if pseudonym:
                    conn.execute(
                        "UPDATE wallet_profiles SET pseudonym = ? WHERE wallet = ?",
                        (pseudonym, wallet),
                    )

            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning("upsert_to_leaderboard failed: %s", exc)
            print(f"[ERROR] Could not upsert to leaderboard: {exc}")

        return upserted

    def ingest_top_wallets(self, limit: int = 200, window: str = "all", dry_run: bool = False, by_volume: bool = False) -> dict[str, Any]:
        """Main entry point. Fetch leaderboard, find gaps, fill history."""
        t0 = time.time()
        window_label = {"all": "all-time", "30d": "current month", "7d": "current week", "1d": "today"}.get(window, window)
        sort_label = "volume" if by_volume else "profit"
        print(f"Fetching top {limit} wallets by {sort_label} (window={window} = {window_label})...")

        if by_volume:
            top = self.fetch_top_by_volume(limit=limit, window=window)
        else:
            top = self.fetch_top_wallets(limit=limit, window=window)

        # Upsert as polymarket-sourced tier1h (preserves local tier1h)
        if top and not dry_run and not by_volume:
            upserted = self.upsert_to_leaderboard(top, window=window)
            if upserted:
                print(f"  Upserted {upserted} wallets to leaderboard as polymarket_{window} tier1h")
        if not top:
            return {
                "leaderboard_wallets_found": 0,
                "wallets_already_in_db": 0,
                "wallets_missing": 0,
                "trades_fetched": 0,
                "elapsed_seconds": round(time.time() - t0, 1),
            }

        missing = self.get_missing_wallets(top)
        already_in_db = len(top) - len(missing)
        print(f"  Found {len(top)} leaderboard wallets")
        print(f"  Already in DB: {already_in_db}")
        print(f"  Missing: {len(missing)}")

        if dry_run:
            print()
            print("DRY RUN — would fetch history for these wallets:")
            for w in missing[:10]:
                print(f"  {w}")
            if len(missing) > 10:
                print(f"  ... and {len(missing) - 10} more")
            return {
                "leaderboard_wallets_found": len(top),
                "wallets_already_in_db": already_in_db,
                "wallets_missing": len(missing),
                "trades_fetched": 0,
                "elapsed_seconds": round(time.time() - t0, 1),
                "dry_run": True,
            }

        total_trades = 0
        for i, wallet in enumerate(missing, 1):
            print(f"  [{i}/{len(missing)}] Fetching {wallet[:16]}...", end=" ", flush=True)
            n = self.fetch_wallet_history(wallet)
            total_trades += n
            print(f"{n} trades")
            time.sleep(0.5)

        return {
            "leaderboard_wallets_found": len(top),
            "wallets_already_in_db": already_in_db,
            "wallets_missing": len(missing),
            "trades_fetched": total_trades,
            "elapsed_seconds": round(time.time() - t0, 1),
        }

    def backfill_wallet(self, wallet_address: str) -> dict[str, Any]:
        """Backfill complete history for a single wallet."""
        t0 = time.time()
        wallet_address = wallet_address.lower()
        print(f"Backfilling {wallet_address[:20]}...")
        n = self.fetch_wallet_history(wallet_address)
        return {
            "wallet": wallet_address,
            "trades_fetched": n,
            "elapsed_seconds": round(time.time() - t0, 1),
        }

    def fetch_resolved_trades(self, wallet: str, db_path: str) -> int:
        """Mark wallet_trades as resolved by checking gamma-api per condition_id.

        Step 1: Get all unique condition_ids the wallet traded.
        Step 2: For each (max 200), check if market resolved via gamma-api.
        Step 3: UPDATE wallet_trades.market_resolved = 1 for resolved markets.
        Returns count of trades marked resolved.
        """
        wallet = wallet.lower()
        conn = sqlite3.connect(db_path)

        # Step 1: get all unresolved condition_ids for this wallet
        rows = conn.execute(
            """SELECT DISTINCT condition_id FROM wallet_trades
               WHERE wallet = ? AND market_resolved = 0
                 AND condition_id IS NOT NULL AND condition_id != ''
               LIMIT 200""",
            (wallet,),
        ).fetchall()
        cids = [r[0] for r in rows]
        if not cids:
            conn.close()
            return 0

        print(f"  Checking {len(cids)} condition_ids for resolution...")
        resolved_cids: set[str] = set()
        for i, cid in enumerate(cids):
            try:
                r = requests.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"condition_ids": cid, "limit": 1},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=5,
                )
                if r.status_code == 200:
                    data = r.json()
                    m = data[0] if isinstance(data, list) and data else None
                    if m and (m.get("resolved") or m.get("resolutionTime") or m.get("closed")):
                        resolved_cids.add(cid)
            except Exception:
                pass
            time.sleep(0.1)

        # Step 3: UPDATE
        updated = 0
        for cid in resolved_cids:
            cur = conn.execute(
                "UPDATE wallet_trades SET market_resolved = 1 WHERE condition_id = ? AND wallet = ?",
                (cid, wallet),
            )
            updated += cur.rowcount
        conn.commit()
        conn.close()
        return updated
