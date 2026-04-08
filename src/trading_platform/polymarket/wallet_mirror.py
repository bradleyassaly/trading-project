# DEPRECATED: This module used EWR-based smart money detection.
# Replaced by whale_tripwire.py + whale_signal_engine.py (9-signal system).
# Do not run or import this module in new code.
# Preserved for reference only.
"""
Wallet mirroring — track and mirror top smart money wallets in real time.

Watches the top N wallets by confidence score and detects when they
trade on active markets. Produces actionable mirror signals.

Usage::

    from trading_platform.polymarket.wallet_mirror import WalletMirror
    mirror = WalletMirror("data/polymarket/wallet_profiles.parquet")
    signals = mirror.get_mirror_signals(since_minutes=90, min_fill_usdc=100)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.polymarket.price_lookup import PriceLookup, is_sports_market

logger = logging.getLogger(__name__)

_DEFAULT_MIN_FILL = 100.0


@dataclass(frozen=True)
class MirrorSignal:
    token_id: str
    direction: str
    trigger_wallet: str
    wallet_edge: float
    wallet_uncertain_trades: int
    fill_amount_usdc: float
    fill_count: int
    fill_timestamp: datetime
    current_price: float | None
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    question: str
    price_since_fill: float | None
    minutes_since_fill: float
    tradeable: bool


class WalletMirror:
    """Mirror top smart money wallets."""

    def __init__(
        self,
        wallet_profiles_path: str | Path,
        *,
        top_n: int = 30,
        min_confidence: float = 0.70,
        min_uncertain_trades: int = 20,
    ) -> None:
        self.watch_list: list[str] = []
        self._wallet_info: dict[str, dict[str, Any]] = {}
        self._tier1: set[str] = set()
        self._tier2: set[str] = set()
        self._tier3: set[str] = set()
        self.excluded_degraded = 0
        self.excluded_sports = 0
        self.excluded_low_ewr = 0

        path = Path(wallet_profiles_path)
        if not path.exists():
            return

        df = pd.read_parquet(path)

        conf_col = "confidence_score" if "confidence_score" in df.columns else "edge"
        uet_col = "uncertain_early_trades" if "uncertain_early_trades" in df.columns else "early_trades"

        if "is_degraded" in df.columns:
            before = len(df)
            df = df[df["is_degraded"] != True]
            self.excluded_degraded = before - len(df)

        if "best_domain" in df.columns:
            before = len(df)
            df = df[df["best_domain"] != "sports"]
            self.excluded_sports = before - len(df)

        if "early_win_rate" in df.columns:
            before = len(df)
            df = df[df["early_win_rate"] >= 0.65]
            self.excluded_low_ewr = before - len(df)

        informed = df[df.get("is_early_informed", df.get("smart_money", pd.Series(dtype=bool))) == True] if "is_early_informed" in df.columns else df
        informed = informed.sort_values(conf_col, ascending=False)

        afpt_col = "avg_fills_per_token" if "avg_fills_per_token" in informed.columns else None

        for _, row in informed.iterrows():
            w = row["wallet"]
            ewr = float(row.get("early_win_rate", 0))
            uet = int(row.get(uet_col, 0))
            vol = float(row.get("total_volume_usdc", 0))
            afpt = float(row.get("avg_fills_per_token", 0)) if afpt_col else 0

            info = {
                "edge": float(row.get("edge", 0)),
                "confidence_score": float(row.get("confidence_score", 0)),
                "uncertain_trades": uet,
                "best_domain": str(row.get("best_domain", "")),
                "early_win_rate": ewr,
                "total_volume": vol,
                "is_degraded": bool(row.get("is_degraded", False)),
            }

            if ewr >= 0.75 and uet >= 15 and vol >= 10000 and (afpt < 25 or afpt == 0):
                self._tier1.add(w)
            elif ewr >= 0.65 and uet >= 10:
                self._tier2.add(w)
            else:
                self._tier3.add(w)

            self.watch_list.append(w)
            self._wallet_info[w] = info

            if len(self.watch_list) >= top_n:
                break

    def fetch_recent_trades(self, *, since_minutes: int = 90, goldsky_client: Any = None,
                            max_fills: int = 500) -> pd.DataFrame:
        if not self.watch_list:
            return pd.DataFrame()
        if goldsky_client is None:
            from trading_platform.polymarket.goldsky_client import GoldskyClient
            goldsky_client = GoldskyClient()

        # Fetch in batches of 10 wallets with per-batch error handling
        batch_size = 10
        all_dfs: list[pd.DataFrame] = []
        total_fills = 0
        for i in range(0, len(self.watch_list), batch_size):
            if total_fills >= max_fills:
                break
            batch = self.watch_list[i:i + batch_size]
            try:
                df = goldsky_client.fetch_fills_by_makers(
                    batch,
                    since_hours=since_minutes / 60.0,
                    batch_size=batch_size,
                )
                if not df.empty:
                    all_dfs.append(df)
                    total_fills += len(df)
            except Exception as exc:
                logger.debug("Batch %d failed: %s, skipping", i // batch_size + 1, exc)
                continue

        if not all_dfs:
            return pd.DataFrame()
        combined = pd.concat(all_dfs, ignore_index=True)
        if "id" in combined.columns:
            combined = combined.drop_duplicates(subset=["id"], keep="first")
        return combined.head(max_fills)

    def get_mirror_signals(
        self,
        *,
        since_minutes: int = 90,
        min_fill_usdc: float = _DEFAULT_MIN_FILL,
        goldsky_client: Any = None,
        live_db_path: str | Path | None = None,
    ) -> list[MirrorSignal]:
        fills = self.fetch_recent_trades(since_minutes=since_minutes, goldsky_client=goldsky_client)
        if fills.empty:
            return []

        price_lookup = PriceLookup(live_db_path or "data/polymarket/live/prices.db")

        now = datetime.now(tz=timezone.utc)
        today_str = now.strftime("%Y-%m-%d")
        sports_count = 0
        expired_count = 0

        # Collect raw signals keyed by (wallet, token) for dedup
        raw: dict[tuple[str, str], dict[str, Any]] = {}

        for _, row in fills.iterrows():
            wallet = str(row.get("maker_wallet", ""))
            info = self._wallet_info.get(wallet)
            if not info:
                continue

            amount = float(row.get("maker_amount", 0))
            if amount < min_fill_usdc:
                continue

            maker_asset = str(row.get("maker_asset_id", ""))
            taker_asset = str(row.get("taker_asset_id", ""))

            if maker_asset == "0":
                token_id = taker_asset
                direction = "YES"
            else:
                token_id = maker_asset
                direction = "NO"

            # Lookup price/spread/question
            pi = price_lookup.get_price(token_id)
            if pi is None:
                continue

            question = pi["question"]
            current_price = pi["price"]
            best_bid = pi["best_bid"]
            best_ask = pi["best_ask"]
            spread = pi["spread"]
            end_date = pi.get("end_date_iso")
            tradeable = pi["tradeable"]

            # Filter expired
            if end_date and end_date[:10] < today_str:
                expired_count += 1
                continue

            # Filter sports by question content
            if question and is_sports_market(question):
                sports_count += 1
                continue

            if current_price is not None and (current_price < 0.05 or current_price > 0.95):
                continue

            taker_amt = float(row.get("taker_amount", 0))
            fill_price = amount / taker_amt if taker_amt > 0 else None
            price_move = None
            if current_price is not None and fill_price is not None and fill_price > 0:
                price_move = current_price - fill_price
                if abs(price_move) > 0.20:
                    continue

            ts = row.get("timestamp")
            if hasattr(ts, "to_pydatetime"):
                fill_dt = ts.to_pydatetime()
            elif isinstance(ts, datetime):
                fill_dt = ts
            else:
                fill_dt = now
            minutes_ago = (now - fill_dt).total_seconds() / 60

            # Dedup by (wallet, token) — keep largest fill, count all
            key = (wallet, token_id)
            existing = raw.get(key)
            if existing is None:
                raw[key] = {
                    "token_id": token_id, "direction": direction,
                    "wallet": wallet, "info": info,
                    "fill_amount": amount, "fill_count": 1,
                    "fill_dt": fill_dt, "minutes_ago": minutes_ago,
                    "current_price": current_price,
                    "best_bid": best_bid, "best_ask": best_ask,
                    "spread": spread, "question": question,
                    "price_move": price_move, "tradeable": tradeable,
                }
            elif amount > existing["fill_amount"]:
                prev_count = existing["fill_count"]
                existing.update({
                    "fill_amount": amount, "fill_count": prev_count + 1,
                    "fill_dt": fill_dt, "minutes_ago": minutes_ago,
                    "price_move": price_move,
                })
            else:
                existing["fill_count"] += 1

        self._last_expired_count = expired_count
        self._last_sports_count = sports_count

        # Build signals from deduped raw
        signals: list[MirrorSignal] = []
        for entry in raw.values():
            info = entry["info"]
            signals.append(MirrorSignal(
                token_id=entry["token_id"],
                direction=entry["direction"],
                trigger_wallet=entry["wallet"],
                wallet_edge=info["edge"],
                wallet_uncertain_trades=info["uncertain_trades"],
                fill_amount_usdc=round(entry["fill_amount"], 2),
                fill_count=entry["fill_count"],
                fill_timestamp=entry["fill_dt"],
                current_price=entry["current_price"],
                best_bid=entry["best_bid"],
                best_ask=entry["best_ask"],
                spread=round(entry["spread"], 4) if entry["spread"] is not None else None,
                question=entry["question"] or "untracked",
                price_since_fill=round(entry["price_move"], 4) if entry["price_move"] is not None else None,
                minutes_since_fill=round(entry["minutes_ago"], 1),
                tradeable=entry["tradeable"],
            ))

        if expired_count:
            logger.info("Expired markets filtered: %d", expired_count)
        if sports_count:
            logger.info("Sports markets filtered: %d", sports_count)

        signals.sort(key=lambda s: (s.tradeable, s.wallet_edge), reverse=True)
        return signals
