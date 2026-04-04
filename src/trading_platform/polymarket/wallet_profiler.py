"""
Polymarket wallet profiler and smart money detection.

Analyzes on-chain trade history to identify wallets that consistently
trade on the winning side of resolved markets. These "smart money"
wallets produce an alpha signal when they trade on active markets.

Usage::

    from trading_platform.polymarket.wallet_profiler import WalletProfiler
    profiler = WalletProfiler()
    result = profiler.build_profiles("trades.csv", "resolution.csv", "profiles.parquet")
    signal = profiler.get_smart_money_signal(trades, profiles, "market-123")
"""
from __future__ import annotations

import csv
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WalletProfile:
    wallet: str
    total_trades: int
    wins: int
    win_rate: float
    total_volume_usdc: float
    avg_trade_size: float
    markets_traded: int
    edge: float  # win_rate - 0.5
    smart_money: bool


@dataclass
class ProfileBuildResult:
    wallets_analyzed: int = 0
    wallets_with_resolved_trades: int = 0
    smart_money_count: int = 0
    output_path: str = ""


class WalletProfiler:
    """Profile wallet trading performance across resolved Polymarket markets."""

    def build_profiles(
        self,
        trades_csv: str | Path,
        resolution_csv: str | Path,
        output_path: str | Path,
        *,
        min_resolved_trades: int = 5,
        early_hours_threshold: float = 24.0,
        early_win_rate_threshold: float = 0.65,
        min_early_trades: int = 5,
    ) -> ProfileBuildResult:
        trades_csv = Path(trades_csv)
        resolution_csv = Path(resolution_csv)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        result = ProfileBuildResult(output_path=str(output_path))

        # Load resolution outcomes: ticker → {resolution_price, close_time}
        resolution_map: dict[str, float] = {}
        close_time_map: dict[str, datetime] = {}
        if resolution_csv.exists():
            with resolution_csv.open(newline="", encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    ticker = row.get("ticker", "")
                    rp = row.get("resolution_price", "")
                    if ticker and rp:
                        try:
                            resolution_map[ticker] = float(rp)
                        except ValueError:
                            pass
                    ct = row.get("close_time") or row.get("end_date_iso") or ""
                    if ticker and ct:
                        try:
                            close_time_map[ticker] = datetime.fromisoformat(
                                ct.replace("Z", "+00:00")
                            )
                        except (ValueError, AttributeError):
                            pass

        if not resolution_map:
            logger.warning("No resolved markets found in %s", resolution_csv)
            return result

        # Load trades and group by wallet
        wallet_trades: dict[str, list[dict[str, Any]]] = defaultdict(list)
        with trades_csv.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                wallet = row.get("wallet", "").strip()
                if wallet:
                    wallet_trades[wallet].append(row)

        result.wallets_analyzed = len(wallet_trades)

        # Analyze each wallet
        profiles: list[dict[str, Any]] = []
        for wallet, trades in wallet_trades.items():
            wins = 0
            resolved_trades = 0
            early_wins = 0
            early_trades = 0
            total_usdc = 0.0
            markets_seen: set[str] = set()
            hours_before_close_list: list[float] = []

            for trade in trades:
                token_id = trade.get("token_id", "")
                side = trade.get("side", "").upper()
                usdc = float(trade.get("total_usdc") or 0)
                total_usdc += usdc

                market_id = token_id[:16]
                markets_seen.add(market_id)

                rp = resolution_map.get(market_id)
                if rp is None:
                    continue
                resolved_trades += 1

                resolved_yes = rp >= 99.0
                is_win = (side == "BUY" and resolved_yes) or (side == "SELL" and not resolved_yes)
                if is_win:
                    wins += 1

                # Compute hours before close
                close_dt = close_time_map.get(market_id)
                trade_ts = trade.get("timestamp", "")
                hours_before = None
                if close_dt and trade_ts:
                    try:
                        trade_dt = datetime.fromisoformat(trade_ts.replace("Z", "+00:00"))
                        hours_before = (close_dt - trade_dt).total_seconds() / 3600.0
                        if hours_before > 0:
                            hours_before_close_list.append(hours_before)
                    except (ValueError, AttributeError):
                        pass

                # Early trade detection
                if hours_before is not None and hours_before > early_hours_threshold:
                    early_trades += 1
                    if is_win:
                        early_wins += 1

            if resolved_trades < min_resolved_trades:
                continue

            result.wallets_with_resolved_trades += 1
            win_rate = wins / resolved_trades
            early_win_rate = early_wins / early_trades if early_trades > 0 else 0.0
            avg_hours = sum(hours_before_close_list) / len(hours_before_close_list) if hours_before_close_list else 0.0

            # Smart money = genuinely early informed, not late arbitrageur
            is_early_informed = (
                early_win_rate >= early_win_rate_threshold
                and early_trades >= min_early_trades
            )

            profiles.append({
                "wallet": wallet,
                "total_trades": len(trades),
                "resolved_trades": resolved_trades,
                "wins": wins,
                "win_rate": win_rate,
                "early_trades": early_trades,
                "early_wins": early_wins,
                "early_win_rate": early_win_rate,
                "avg_hours_before_close": round(avg_hours, 1),
                "total_volume_usdc": total_usdc,
                "avg_trade_size": total_usdc / len(trades) if trades else 0,
                "markets_traded": len(markets_seen),
                "edge": win_rate - 0.5,
                "is_early_informed": is_early_informed,
                "smart_money": is_early_informed,
            })

        if not profiles:
            return result

        result.smart_money_count = sum(1 for p in profiles if p["smart_money"])

        # Write to parquet
        df = pd.DataFrame(profiles)
        df.to_parquet(output_path, index=False)

        logger.info(
            "Built %d wallet profiles (%d smart money) from %d wallets",
            result.wallets_with_resolved_trades,
            result.smart_money_count,
            result.wallets_analyzed,
        )
        return result

    @staticmethod
    def get_smart_money_signal(
        trades_csv: str | Path,
        profiles_path: str | Path,
        market_id: str,
        *,
        lookback_hours: float = 48,
    ) -> float:
        """Compute smart money imbalance signal for a market.

        Returns float in [-1, 1]:
          +1 = heavy smart money buying YES
          -1 = heavy smart money buying NO
           0 = no smart money activity
        """
        profiles_path = Path(profiles_path)
        trades_csv = Path(trades_csv)

        if not profiles_path.exists() or not trades_csv.exists():
            return 0.0

        # Load smart money wallets
        profiles_df = pd.read_parquet(profiles_path)
        smart_wallets = set(
            profiles_df[profiles_df["smart_money"] == True]["wallet"].tolist()
        )
        if not smart_wallets:
            return 0.0

        # Load recent trades for this market
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=lookback_hours)
        smart_buy_volume = 0.0
        smart_sell_volume = 0.0

        with trades_csv.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                token_id = row.get("token_id", "")
                if token_id[:16] != market_id and token_id != market_id:
                    continue
                wallet = row.get("wallet", "").strip()
                if wallet not in smart_wallets:
                    continue
                usdc = float(row.get("total_usdc") or 0)
                side = row.get("side", "").upper()
                if side == "BUY":
                    smart_buy_volume += usdc
                elif side == "SELL":
                    smart_sell_volume += usdc

        total = smart_buy_volume + smart_sell_volume
        if total < 1.0:
            return 0.0
        return (smart_buy_volume - smart_sell_volume) / total
