"""
Smart money trading loop — converts live smart money signals into
paper trades via the existing KalshiPaperExecutor.

Usage::

    from trading_platform.polymarket.smart_money_loop import run_smart_money_cycle
    results = run_smart_money_cycle(executor)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def run_smart_money_cycle(
    executor: Any,
    *,
    profiles_path: str | Path | None = None,
    fills_dir: str | Path | None = None,
    min_confidence: float = 0.60,
    min_net_volume: float = 5000.0,
    min_edge: float = 0.50,
    hours_back: int = 48,
) -> dict[str, Any]:
    """Run one cycle: load fills → compute signals → place paper trades.

    Returns summary dict.
    """
    from trading_platform.polymarket.smart_money_signal import SmartMoneySignalGenerator
    from trading_platform.kalshi.market_scanner import ScanResult

    profiles_path = Path(profiles_path or PROJECT_ROOT / "data" / "polymarket" / "wallet_profiles.parquet")
    fills_dir = Path(fills_dir or PROJECT_ROOT / "data" / "polymarket" / "orderflow")

    summary: dict[str, Any] = {
        "smart_wallets": 0, "fills_loaded": 0, "signals_found": 0,
        "signals_qualifying": 0, "trades_placed": 0, "errors": [],
    }

    if not profiles_path.exists():
        return summary

    gen = SmartMoneySignalGenerator(profiles_path)
    summary["smart_wallets"] = gen.smart_wallet_count

    if not fills_dir.exists():
        return summary

    # Load recent fills
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours_back)
    all_dfs: list[pd.DataFrame] = []
    for path in fills_dir.glob("*.parquet"):
        try:
            df = pd.read_parquet(path)
            if not df.empty and "timestamp" in df.columns:
                try:
                    df = df[df["timestamp"] >= cutoff]
                except TypeError:
                    pass
                if not df.empty:
                    all_dfs.append(df)
        except Exception:
            continue

    if not all_dfs:
        return summary

    fills = pd.concat(all_dfs, ignore_index=True)
    summary["fills_loaded"] = len(fills)

    signals = gen.compute(fills)
    summary["signals_found"] = len(signals)

    # Filter to qualifying signals
    qualifying = [
        s for s in signals
        if s.confidence >= min_confidence
        and abs(s.net_smart_volume) >= min_net_volume
        and s.top_wallet_edge >= min_edge
        and s.direction != "NEUTRAL"
    ]
    summary["signals_qualifying"] = len(qualifying)

    # Convert to ScanResult and place trades
    for sig in qualifying:
        result = ScanResult(
            ticker=sig.token_id[:32],
            yes_price=50.0,  # unknown for Polymarket — use midpoint
            days_to_close=0,
            signal_scores={"smart_money": sig.net_smart_volume},
            strongest_signal="smart_money",
            recommended_side=sig.direction,
            confidence=sig.confidence,
            news_context="unscheduled",
            kelly_fraction=min(0.02, sig.confidence * 0.03),
            smart_money_direction=sig.direction,
            smart_money_confidence=sig.confidence,
        )
        try:
            placed = executor.execute_trade(result)
            if placed:
                summary["trades_placed"] += 1
                logger.info(
                    "Smart money trade: %s %s conf=%.2f net=$%.0f edge=%.1f%%",
                    sig.direction, sig.token_id[:20], sig.confidence,
                    sig.net_smart_volume, sig.top_wallet_edge * 100,
                )
        except Exception as exc:
            summary["errors"].append(str(exc))

    return summary
