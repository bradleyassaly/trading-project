# DEPRECATED: The CSV-based build_profiles() method in this module is replaced by
# wallet_profile_rebuild.rebuild_profiles() which reads directly from wallet_intelligence.db.
# Use: trading-cli data polymarket wallet-profiles --from-db
# This module is preserved for reference but the CSV path is not part of the active pipeline.
# See ARCHITECTURE.md for what is currently active.
"""
Polymarket wallet profiler and smart money detection.

Analyzes trade history to identify wallets that consistently trade on
the winning side of resolved markets *before* the outcome is obvious.
Uses ``ResolutionResolver`` for outcome lookups and ``MarketCloseTimeMap``
for early-trade detection.

Usage::

    from trading_platform.polymarket.wallet_profiler import WalletProfiler
    profiler = WalletProfiler()
    result = profiler.build_profiles("trades.csv", "resolution.csv", "profiles.parquet")
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

from trading_platform.polymarket.resolution_resolver import ResolutionResolver, normalize_token_id

logger = logging.getLogger(__name__)


@dataclass
class WalletProfile:
    wallet: str
    total_trades: int
    resolved_trades: int
    wins: int
    win_rate: float
    early_trades: int
    early_wins: int
    early_win_rate: float
    avg_hours_before_close: float
    total_volume_usdc: float
    avg_trade_size: float
    markets_traded: int
    edge: float
    is_early_informed: bool
    smart_money: bool


@dataclass
class ProfileBuildResult:
    wallets_analyzed: int = 0
    wallets_with_resolved_trades: int = 0
    wallets_with_early_trades: int = 0
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
        min_resolved_trades: int = 3,
        early_hours_threshold: float = 24.0,
        early_win_rate_threshold: float = 0.65,
        min_early_trades: int = 5,
        close_time_db_path: str | Path | None = None,
    ) -> ProfileBuildResult:
        trades_csv = Path(trades_csv)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result = ProfileBuildResult(output_path=str(output_path))

        # Load resolution resolver (normalizes token IDs automatically)
        resolver = ResolutionResolver(resolution_csv)
        if resolver.count == 0:
            logger.warning("No resolutions loaded from %s", resolution_csv)

        # Load close times for early-trade detection
        from trading_platform.polymarket.market_close_times import MarketCloseTimeMap
        db_path = close_time_db_path or Path("data/polymarket/live/prices.db")
        close_times = MarketCloseTimeMap.from_live_db(db_path)

        # Load trades
        wallet_trades: dict[str, list[dict[str, Any]]] = defaultdict(list)
        with trades_csv.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                wallet = (row.get("wallet") or row.get("proxyWallet") or "").strip()
                if not wallet:
                    continue
                if "token_id" not in row and "asset" in row:
                    row["token_id"] = row["asset"]
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
            hours_list: list[float] = []

            for trade in trades:
                token_id = (trade.get("token_id") or "").strip()
                side = trade.get("side", "").upper()
                usdc = float(trade.get("total_usdc") or trade.get("amount") or 0)
                total_usdc += usdc
                markets_seen.add(normalize_token_id(token_id))

                # Resolve market outcome via resolver (token_id + condition_id)
                cond_id = trade.get("condition_id", "").strip()
                rp = resolver.resolve(token_id, condition_id=cond_id)
                if rp is None:
                    continue
                resolved_trades += 1
                resolved_yes = rp >= 99.0

                # Determine which side the trader is on
                outcome_str = (trade.get("outcome") or "").strip().upper()
                if outcome_str in ("YES", "NO"):
                    # outcome tells us trader's side (YES/NO contract)
                    trader_on_yes = outcome_str == "YES"
                elif side in ("BUY", "YES"):
                    trader_on_yes = True
                else:
                    trader_on_yes = False

                is_win = (trader_on_yes and resolved_yes) or (not trader_on_yes and not resolved_yes)

                if is_win:
                    wins += 1

                # Early trade detection
                close_dt = close_times.get(token_id)
                ts_str = trade.get("timestamp", "")
                hours_before = None
                if close_dt and ts_str:
                    try:
                        if ts_str.replace(".", "").isdigit():
                            trade_dt = datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
                        else:
                            trade_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        hours_before = (close_dt - trade_dt).total_seconds() / 3600.0
                        if hours_before > 0:
                            hours_list.append(hours_before)
                    except (ValueError, AttributeError, OSError):
                        pass

                if hours_before is not None and hours_before > early_hours_threshold:
                    early_trades += 1
                    if is_win:
                        early_wins += 1

            if resolved_trades < min_resolved_trades:
                continue

            result.wallets_with_resolved_trades += 1
            win_rate = wins / resolved_trades
            early_win_rate = early_wins / early_trades if early_trades > 0 else 0.0
            avg_hours = sum(hours_list) / len(hours_list) if hours_list else 0.0

            is_early_informed = (
                early_win_rate >= early_win_rate_threshold
                and early_trades >= min_early_trades
            )
            if early_trades >= min_early_trades:
                result.wallets_with_early_trades += 1

            profiles.append({
                "wallet": wallet,
                "total_trades": len(trades),
                "resolved_trades": resolved_trades,
                "wins": wins,
                "win_rate": round(win_rate, 4),
                "early_trades": early_trades,
                "early_wins": early_wins,
                "early_win_rate": round(early_win_rate, 4),
                "avg_hours_before_close": round(avg_hours, 1),
                "total_volume_usdc": round(total_usdc, 2),
                "avg_trade_size": round(total_usdc / len(trades), 2) if trades else 0,
                "markets_traded": len(markets_seen),
                "edge": round(win_rate - 0.5, 4),
                "is_early_informed": is_early_informed,
                "smart_money": is_early_informed,
            })

        if not profiles:
            return result

        result.smart_money_count = sum(1 for p in profiles if p["smart_money"])
        df = pd.DataFrame(profiles)
        df.to_parquet(output_path, index=False)
        logger.info(
            "Built %d profiles (%d smart money) from %d wallets",
            result.wallets_with_resolved_trades,
            result.smart_money_count,
            result.wallets_analyzed,
        )
        return result

    def build_profiles_from_data_api(
        self,
        trades_dir: str | Path,
        output_path: str | Path,
        *,
        min_resolved_trades: int = 5,
    ) -> ProfileBuildResult:
        """Build profiles from Data API trade CSVs + Graph resolutions."""
        from trading_platform.polymarket.graph_resolver import GraphResolver

        trades_dir = Path(trades_dir)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result = ProfileBuildResult(output_path=str(output_path))

        all_trades: list[dict[str, Any]] = []
        for csv_file in sorted(trades_dir.glob("*.csv")):
            try:
                with csv_file.open(newline="", encoding="utf-8-sig") as fh:
                    for row in csv.DictReader(fh):
                        all_trades.append(row)
            except Exception:
                continue

        if not all_trades:
            return result

        condition_ids = list({
            t.get("condition_id", "").strip()
            for t in all_trades if t.get("condition_id", "").strip()
        })
        resolver = GraphResolver()
        resolutions = resolver.get_resolutions(condition_ids)

        wallet_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"trades": 0, "resolved": 0, "wins": 0, "volume": 0.0, "markets": set()}
        )

        for trade in all_trades:
            wallet = (trade.get("wallet") or trade.get("proxyWallet") or "").strip()
            if not wallet:
                continue
            side = trade.get("side", "").upper()
            if side != "BUY":
                continue

            cid = trade.get("condition_id", "").strip()
            outcome = trade.get("outcome", "").strip()
            usdc = float(trade.get("total_usdc") or 0)

            ws = wallet_stats[wallet]
            ws["trades"] += 1
            ws["volume"] += usdc
            ws["markets"].add(cid)

            resolves_yes = resolutions.get(cid)
            if resolves_yes is None:
                continue
            ws["resolved"] += 1
            if (outcome == "Yes" and resolves_yes) or (outcome == "No" and not resolves_yes):
                ws["wins"] += 1

        result.wallets_analyzed = len(wallet_stats)
        profiles: list[dict[str, Any]] = []
        for wallet, ws in wallet_stats.items():
            if ws["resolved"] < min_resolved_trades:
                continue
            result.wallets_with_resolved_trades += 1
            win_rate = ws["wins"] / ws["resolved"]
            profiles.append({
                "wallet": wallet,
                "total_trades": ws["trades"],
                "resolved_trades": ws["resolved"],
                "wins": ws["wins"],
                "win_rate": round(win_rate, 4),
                "early_trades": 0, "early_wins": 0, "early_win_rate": 0.0,
                "avg_hours_before_close": 0.0,
                "total_volume_usdc": round(ws["volume"], 2),
                "avg_trade_size": round(ws["volume"] / ws["trades"], 2) if ws["trades"] > 0 else 0,
                "markets_traded": len(ws["markets"]),
                "edge": round(win_rate - 0.5, 4),
                "is_early_informed": False,
                "smart_money": win_rate >= 0.65 and ws["resolved"] >= min_resolved_trades,
            })

        if profiles:
            result.smart_money_count = sum(1 for p in profiles if p["smart_money"])
            pd.DataFrame(profiles).to_parquet(output_path, index=False)

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

        Returns float in [-1, 1].
        """
        profiles_path = Path(profiles_path)
        trades_csv = Path(trades_csv)
        if not profiles_path.exists() or not trades_csv.exists():
            return 0.0

        profiles_df = pd.read_parquet(profiles_path)
        smart_wallets = set(
            profiles_df[profiles_df["smart_money"] == True]["wallet"].tolist()
        )
        if not smart_wallets:
            return 0.0

        smart_buy = 0.0
        smart_sell = 0.0
        normalized_market = normalize_token_id(market_id)

        with trades_csv.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                token_id = row.get("token_id") or row.get("asset") or ""
                if normalize_token_id(token_id) != normalized_market:
                    continue
                wallet = (row.get("wallet") or row.get("proxyWallet") or "").strip()
                if wallet not in smart_wallets:
                    continue
                usdc = float(row.get("total_usdc") or 0)
                side = row.get("side", "").upper()
                if side == "BUY":
                    smart_buy += usdc
                elif side == "SELL":
                    smart_sell += usdc

        total = smart_buy + smart_sell
        if total < 1.0:
            return 0.0
        return (smart_buy - smart_sell) / total
