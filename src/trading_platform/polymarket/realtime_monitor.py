# DEPRECATED: This module used EWR-based smart money detection.
# Replaced by whale_tripwire.py + whale_signal_engine.py (9-signal system).
# Do not run or import this module in new code.
# Preserved for reference only.
"""
Real-time smart money monitor.

Watches Tier 1 and Tier 2 wallets via Goldsky stream, triggers
alerts and optional auto paper trading on detected fills.

Usage::

    import asyncio
    from trading_platform.polymarket.realtime_monitor import RealtimeMonitor

    monitor = RealtimeMonitor()
    asyncio.run(monitor.run())
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.polymarket.alert_log import AlertLog
from trading_platform.polymarket.alert_system import AlertSystem
from trading_platform.polymarket.goldsky_stream import GoldskyStream
from trading_platform.polymarket.price_lookup import PriceLookup, is_sports_market

logger = logging.getLogger(__name__)

_DEFAULT_PROFILES = "data/polymarket/wallet_profiles.parquet"
_DEFAULT_DB = "data/polymarket/live/prices.db"


class RealtimeMonitor:
    """Watch smart money wallets and alert on fills in real time."""

    def __init__(
        self,
        wallet_profiles_path: str | Path = _DEFAULT_PROFILES,
        prices_db_path: str | Path = _DEFAULT_DB,
        paper_executor: Any = None,
        alert_system: AlertSystem | None = None,
        *,
        min_fill_usdc: float = 100.0,
        min_ewr: float = 0.70,
        auto_trade: bool = False,
        tier1_count: int = 50,
        tier2_count: int = 100,
        tier1_only: bool = False,
    ) -> None:
        self._prices_db = Path(prices_db_path)
        self._price_lookup = PriceLookup(prices_db_path)
        self._paper_executor = paper_executor
        self._alert_system = alert_system or AlertSystem()
        self._min_fill = min_fill_usdc
        self._min_ewr = min_ewr
        self._auto_trade = auto_trade
        self._tier1_only = tier1_only

        # Load wallet profiles and build watch tiers
        self._tier1: set[str] = set()
        self._tier2: set[str] = set()
        self._wallet_profiles: dict[str, dict[str, Any]] = {}
        self._fills_processed = 0
        self._alerts_sent = 0
        self._trades_placed = 0
        self._sports_filtered = 0
        self._alerted_ids: set[str] = set()  # dedup alerts
        self._alert_log = AlertLog()

        # Structured file logging
        self._file_logger = logging.getLogger("monitor_file")
        self._file_logger.setLevel(logging.INFO)
        if not self._file_logger.handlers:
            log_path = Path("logs/monitor.log")
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                str(log_path), maxBytes=10 * 1024 * 1024, backupCount=3,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._file_logger.addHandler(handler)

        self._load_profiles(wallet_profiles_path, tier1_count, tier2_count)

    def _load_profiles(
        self,
        path: str | Path,
        tier1_count: int,
        tier2_count: int,
    ) -> None:
        path = Path(path)
        if not path.exists():
            logger.warning("Wallet profiles not found: %s", path)
            return

        df = pd.read_parquet(path)

        # Filter and sort
        conf_col = "confidence_score" if "confidence_score" in df.columns else "edge"
        informed = df[df.get("is_early_informed", df.get("smart_money", pd.Series(dtype=bool))) == True] if "is_early_informed" in df.columns else df
        if informed.empty:
            informed = df

        # Exclude sports domain and low EWR
        if "best_domain" in informed.columns:
            informed = informed[informed["best_domain"] != "sports"]
        if "early_win_rate" in informed.columns:
            informed = informed[informed["early_win_rate"] >= 0.65]

        informed = informed.sort_values(conf_col, ascending=False)

        # Tier 1: top N by confidence
        tier1_df = informed.head(tier1_count)
        tier2_df = informed.iloc[tier1_count:tier1_count + tier2_count]

        for _, row in tier1_df.iterrows():
            w = str(row["wallet"]).lower()
            self._tier1.add(w)
            self._wallet_profiles[w] = self._row_to_profile(row)

        if not self._tier1_only:
            for _, row in tier2_df.iterrows():
                w = str(row["wallet"]).lower()
                self._tier2.add(w)
                self._wallet_profiles[w] = self._row_to_profile(row)

    @staticmethod
    def _row_to_profile(row: Any) -> dict[str, Any]:
        return {
            "edge": float(row.get("edge", 0)),
            "early_win_rate": float(row.get("early_win_rate", 0)),
            "confidence_score": float(row.get("confidence_score", 0)),
            "uncertain_early_trades": int(row.get("uncertain_early_trades", 0)),
            "best_domain": str(row.get("best_domain", "other")),
            "total_volume_usdc": float(row.get("total_volume_usdc", 0)),
            "is_degraded": bool(row.get("is_degraded", False)),
        }

    @property
    def all_watched(self) -> set[str]:
        return self._tier1 | self._tier2

    def print_watch_summary(self) -> None:
        t1 = len(self._tier1)
        t2 = len(self._tier2)
        print(f"Watching {t1} Tier 1 wallets, {t2} Tier 2 wallets")
        if self._wallet_profiles:
            # Show top wallet
            top = max(self._wallet_profiles.items(), key=lambda x: x[1]["confidence_score"])
            p = top[1]
            print(f"Top wallet: {top[0][:16]}... EWR={p['early_win_rate']:.1%} "
                  f"domain={p['best_domain']} conf={p['confidence_score']:.2f}")
        if self._auto_trade:
            print("Auto paper trading: ENABLED (Tier 1 only)")
        print()

    async def on_fill(self, fill: dict[str, Any]) -> None:
        """Process a single fill from the stream."""
        # Dedup: same fill ID should only trigger one alert
        fill_id = str(fill.get("id", ""))
        if fill_id and fill_id in self._alerted_ids:
            return
        if fill_id:
            self._alerted_ids.add(fill_id)
            if len(self._alerted_ids) > 50_000:
                self._alerted_ids = set(list(self._alerted_ids)[-25_000:])

        maker = fill.get("maker", "").lower()
        if maker not in self.all_watched:
            return

        # Parse amount
        amount = int(fill.get("makerAmountFilled", 0)) / 1e6
        if amount < self._min_fill:
            return

        self._fills_processed += 1

        # Determine direction
        maker_asset = fill.get("makerAssetId", "")
        if maker_asset == "0":
            direction = "YES"
            token_id = fill.get("takerAssetId", "")
        else:
            direction = "NO"
            token_id = fill.get("makerAssetId", "")

        # Get price/spread/question via shared PriceLookup
        pi = self._price_lookup.get_price(token_id)
        if pi is None:
            return

        current_price = pi["price"]
        best_bid = pi["best_bid"]
        best_ask = pi["best_ask"]
        spread = pi["spread"]
        question = pi["question"]

        # Skip sports markets
        if is_sports_market(question):
            self._sports_filtered += 1
            return

        # Skip expired/settled markets
        if current_price is not None and (current_price < 0.05 or current_price > 0.95):
            return

        profile = self._wallet_profiles.get(maker, {})
        tier = 1 if maker in self._tier1 else 2

        # Send alert
        q_safe = (question or token_id[:20]).encode("ascii", "replace").decode()[:50]
        self._alert_system.send_mirror_alert(
            fill, profile, question or token_id[:20],
            current_price, spread,
            tier=tier, direction=direction, fill_amount=amount,
        )
        self._alerts_sent += 1

        # Derive alert type from wallet bucket
        wt = profile.get("wallet_type", profile.get("best_domain", "unknown"))
        if wt in ("concentrated_whale", "contrarian_whale"):
            alert_type = "whale_entry"
        elif wt == "domain_specialist":
            alert_type = "domain_specialist"
        elif wt == "directional":
            alert_type = "whale_entry" if amount >= 5000 else "volume_trader"
        else:
            alert_type = "volume_trader"

        # Persist to JSONL alert log
        self._alert_log.append(
            wallet=maker, token_id=token_id,
            market_question=q_safe, side=direction,
            amount_usdc=amount, wallet_edge=profile.get("edge", 0),
            early_win_rate=profile.get("early_win_rate", 0),
            tier=tier, alert_type=alert_type,
        )

        # Structured file log
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        tier_label = f"TIER{tier}"
        self._file_logger.info(
            "%s | %s | %s | %s | %s | $%.0f",
            tier_label, ts, maker[:14], q_safe, direction, amount,
        )

        # Auto paper trade (Tier 1 only)
        tradeable = pi.get("tradeable", False)
        if (self._auto_trade and tier == 1 and tradeable
                and profile.get("early_win_rate", 0) >= self._min_ewr
                and self._paper_executor is not None):
            self._execute_mirror_trade(
                token_id=token_id,
                direction=direction,
                market_price=current_price,
                trigger_wallet=maker,
                wallet_edge=profile.get("edge", 0),
                confidence=profile.get("confidence_score", 0),
            )
            self._trades_placed += 1

    def _execute_mirror_trade(
        self,
        token_id: str,
        direction: str,
        market_price: float,
        trigger_wallet: str,
        wallet_edge: float,
        confidence: float,
    ) -> None:
        """Place a paper trade via the Polymarket executor."""
        from dataclasses import dataclass

        @dataclass
        class _MirrorSignal:
            token_id: str
            direction: str
            confidence: float
            top_wallet_edge: float
            weighted_net_volume: float

        signal = _MirrorSignal(
            token_id=token_id,
            direction=direction,
            confidence=confidence,
            top_wallet_edge=wallet_edge,
            weighted_net_volume=0.0,
        )
        try:
            placed = self._paper_executor.execute_trade(signal, market_price)
            if placed:
                logger.info("Auto paper trade: %s %s @ %.2f (wallet=%s)",
                             direction, token_id[:16], market_price, trigger_wallet[:10])
        except Exception as exc:
            logger.warning("Auto trade failed: %s", exc)

    def _get_price(self, token_id: str) -> tuple[float, float | None, float | None, str] | None:
        """Look up current price from prices.db."""
        if not self._prices_db.exists():
            return None
        try:
            conn = sqlite3.connect(str(self._prices_db), check_same_thread=False)
            row = conn.execute(
                "SELECT market_id, question FROM markets WHERE yes_token_id = ?",
                (token_id,),
            ).fetchone()
            if not row:
                conn.close()
                return None
            mid, question = row

            price_row = conn.execute(
                "SELECT price FROM ticks WHERE market_id = ? AND msg_type IN ('last_trade_price', 'book') "
                "ORDER BY id DESC LIMIT 1",
                (mid,),
            ).fetchone()
            price = float(price_row[0]) if price_row else None

            book_row = conn.execute(
                "SELECT best_bid, best_ask FROM ticks WHERE market_id = ? AND msg_type = 'book' "
                "ORDER BY id DESC LIMIT 1",
                (mid,),
            ).fetchone()
            bid = float(book_row[0]) if book_row and book_row[0] is not None else None
            ask = float(book_row[1]) if book_row and book_row[1] is not None else None

            conn.close()
            if price is None:
                return None
            return (price, bid, ask, question or "")
        except Exception:
            return None

    @staticmethod
    def _fetch_gamma_price(token_id: str) -> dict[str, Any] | None:
        """Fetch current price from Gamma API for untracked markets."""
        try:
            import requests
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"clob_token_ids": token_id},
                timeout=5,
            )
            if resp.status_code != 200 or not resp.json():
                return None
            mkt = resp.json()[0] if isinstance(resp.json(), list) else resp.json()
            price = None
            outcome_prices = mkt.get("outcomePrices")
            if outcome_prices:
                import json as _json
                prices = outcome_prices if isinstance(outcome_prices, list) else _json.loads(outcome_prices)
                price = float(prices[0])
            return {"price": price, "question": mkt.get("question", "")}
        except Exception:
            return None

    async def run(self) -> None:
        """Start the real-time monitor."""
        if not self.all_watched:
            print("No wallets to watch. Run goldsky-wallet-profiles first.")
            return

        self.print_watch_summary()

        stream = GoldskyStream(
            watch_wallets=list(self.all_watched),
            callback=self.on_fill,
        )
        print("Real-time monitor started. Watching for smart money fills...")
        print(f"Min fill: ${self._min_fill:,.0f} | Min EWR: {self._min_ewr:.0%}")
        print()

        # Run stream with periodic stats
        stats_task = asyncio.create_task(self._print_stats_loop())
        try:
            await stream.run()
        finally:
            stats_task.cancel()

    async def _print_stats_loop(self) -> None:
        """Print monitor stats every 5 minutes."""
        while True:
            await asyncio.sleep(300)
            now = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
            ts_full = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}] Fills: {self._fills_processed} | "
                  f"Alerts: {self._alerts_sent} | Sports filtered: {self._sports_filtered} | "
                  f"Trades: {self._trades_placed}")
            self._file_logger.info(
                "STATS | %s | fills=%d alerts=%d sports=%d trades=%d",
                ts_full, self._fills_processed, self._alerts_sent,
                self._sports_filtered, self._trades_placed,
            )
