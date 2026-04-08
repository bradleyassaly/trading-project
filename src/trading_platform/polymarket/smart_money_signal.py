# DEPRECATED: This module used EWR-based smart money detection.
# Replaced by whale_tripwire.py + whale_signal_engine.py (9-signal system).
# Do not run or import this module in new code.
# Preserved for reference only.
"""
Smart money directional signal for Polymarket markets.

Computes buy/sell imbalance from wallets flagged as ``is_early_informed``
in the wallet profiles parquet, using Goldsky on-chain fill data.

Usage::

    from trading_platform.polymarket.smart_money_signal import SmartMoneySignalGenerator
    gen = SmartMoneySignalGenerator("data/polymarket/wallet_profiles.parquet")
    signals = gen.compute(recent_fills_df)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.polymarket.resolution_resolver import normalize_token_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmartMoneySignal:
    token_id: str
    net_smart_volume: float
    weighted_net_volume: float
    smart_buy_volume: float
    smart_sell_volume: float
    smart_trade_count: int
    direction: str  # "YES", "NO", "NEUTRAL"
    confidence: float  # 0.0-1.0
    top_wallet: str
    top_wallet_edge: float
    top_wallet_uncertain_early_trades: int
    most_recent_smart_trade: str  # ISO timestamp
    hours_since_last_smart_trade: float


class SmartMoneySignalGenerator:
    """Generate smart money signals from Goldsky fills + wallet profiles."""

    def __init__(self, wallet_profiles_path: str | Path) -> None:
        self._profiles_path = Path(wallet_profiles_path)
        self._smart_wallets: dict[str, dict[str, Any]] = {}
        if self._profiles_path.exists():
            df = pd.read_parquet(self._profiles_path)
            smart = df[df["is_early_informed"] == True] if "is_early_informed" in df.columns else df[df["smart_money"] == True]
            for _, row in smart.iterrows():
                self._smart_wallets[row["wallet"]] = {
                    "edge": float(row.get("edge", 0)),
                    "early_win_rate": float(row.get("early_win_rate", 0)),
                    "uncertain_early_trades": int(row.get("uncertain_early_trades", row.get("early_trades", 0))),
                    "total_volume": float(row.get("total_volume_usdc", 0)),
                }
            logger.info("Loaded %d smart wallets from %s", len(self._smart_wallets), self._profiles_path)

    @property
    def smart_wallet_count(self) -> int:
        return len(self._smart_wallets)

    def compute(self, recent_fills: pd.DataFrame) -> list[SmartMoneySignal]:
        """Compute signals from recent Goldsky fills.

        Accepts both Goldsky format (maker_wallet, maker_asset_id, taker_asset_id, maker_amount)
        and Data API format (wallet, token_id, side, amount/total_usdc).
        """
        if recent_fills.empty or not self._smart_wallets:
            return []

        results_by_token: dict[str, dict[str, Any]] = {}
        is_goldsky = "maker_wallet" in recent_fills.columns

        for _, row in recent_fills.iterrows():
            if is_goldsky:
                wallet = str(row.get("maker_wallet", "")).strip()
                if wallet not in self._smart_wallets:
                    # Also check taker_wallet (maker could be on either side)
                    wallet = str(row.get("taker_wallet", "")).strip()
                    if wallet not in self._smart_wallets:
                        continue

                maker_asset = str(row.get("maker_asset_id", ""))
                taker_asset = str(row.get("taker_asset_id", ""))
                amt = float(row.get("maker_amount", 0))

                # maker_asset_id == "0" means maker paid USDC = bought tokens (bullish)
                if maker_asset == "0":
                    token_id = normalize_token_id(taker_asset)
                    is_buy = True
                else:
                    token_id = normalize_token_id(maker_asset)
                    is_buy = False
            else:
                # Data API format fallback
                wallet = str(row.get("wallet", "")).strip()
                if wallet not in self._smart_wallets:
                    continue
                token_id = normalize_token_id(str(row.get("token_id", "")))
                amt_col = "amount" if "amount" in recent_fills.columns else "total_usdc"
                amt = float(row.get(amt_col, 0))
                is_buy = str(row.get("side", "")).upper() == "BUY"

            if not token_id:
                continue

            if token_id not in results_by_token:
                results_by_token[token_id] = {
                    "buy": 0.0, "sell": 0.0,
                    "weighted_buy": 0.0, "weighted_sell": 0.0,
                    "count": 0, "wallets": {}, "latest_ts": None,
                }
            entry = results_by_token[token_id]

            # Recency weight: 0-6h=1.0, 6-24h=0.6, 24-48h=0.3
            now = datetime.now(tz=timezone.utc)
            ts = row.get("timestamp")
            weight = 0.3  # default for older fills
            if ts is not None:
                try:
                    if hasattr(ts, "to_pydatetime"):
                        fill_dt = ts.to_pydatetime()
                    elif isinstance(ts, datetime):
                        fill_dt = ts
                    else:
                        fill_dt = None
                    if fill_dt:
                        hours_ago = (now - fill_dt).total_seconds() / 3600
                        if hours_ago <= 6:
                            weight = 1.0
                        elif hours_ago <= 24:
                            weight = 0.6
                        # Track latest timestamp
                        if entry["latest_ts"] is None or fill_dt > entry["latest_ts"]:
                            entry["latest_ts"] = fill_dt
                except Exception:
                    pass

            if is_buy:
                entry["buy"] += amt
                entry["weighted_buy"] += amt * weight
            else:
                entry["sell"] += amt
                entry["weighted_sell"] += amt * weight
            entry["count"] += 1
            entry["wallets"][wallet] = entry["wallets"].get(wallet, 0) + amt

        # Build signals
        now = datetime.now(tz=timezone.utc)
        signals: list[SmartMoneySignal] = []
        for token_id, data in results_by_token.items():
            total = data["buy"] + data["sell"]
            if total < 50.0:
                continue

            net = data["buy"] - data["sell"]
            weighted_net = data["weighted_buy"] - data["weighted_sell"]
            ratio = abs(net) / total

            if ratio < 0.20:
                direction = "NEUTRAL"
            elif net > 0:
                direction = "YES"
            else:
                direction = "NO"

            # Recency
            latest = data["latest_ts"]
            if latest:
                hours_since = (now - latest).total_seconds() / 3600
                latest_iso = latest.isoformat()
            else:
                hours_since = 999.0
                latest_iso = ""

            # Find top wallet by edge
            top_wallet = ""
            top_edge = 0.0
            top_uet = 0
            for w in data["wallets"]:
                profile = self._smart_wallets.get(w, {})
                w_edge = profile.get("edge", 0)
                if w_edge > top_edge:
                    top_wallet = w
                    top_edge = w_edge
                    top_uet = profile.get("uncertain_early_trades", 0)

            signals.append(SmartMoneySignal(
                token_id=token_id,
                net_smart_volume=round(net, 2),
                weighted_net_volume=round(weighted_net, 2),
                smart_buy_volume=round(data["buy"], 2),
                smart_sell_volume=round(data["sell"], 2),
                smart_trade_count=data["count"],
                direction=direction,
                confidence=round(ratio, 4),
                top_wallet=top_wallet,
                top_wallet_edge=round(top_edge, 4),
                top_wallet_uncertain_early_trades=top_uet,
                most_recent_smart_trade=latest_iso,
                hours_since_last_smart_trade=round(hours_since, 1),
            ))

        signals.sort(key=lambda s: abs(s.weighted_net_volume), reverse=True)
        return signals
