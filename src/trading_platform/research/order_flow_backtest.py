"""
Order flow signal backtester.

Validates whether taker_imbalance and large_order signals have
real predictive power on resolved Polymarket markets using
historical fill data from Goldsky.

Usage::

    from trading_platform.research.order_flow_backtest import OrderFlowBacktester
    bt = OrderFlowBacktester()
    result = bt.run("taker_imbalance", hours_before_close=12)
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.signals.order_flow import OrderFlowFeatures
from trading_platform.polymarket.resolution_resolver import ResolutionResolver

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    signal: str
    hours_before_close: int
    window_hours: int
    win_rate: float
    n_trades: int
    mean_signal_score: float
    sharpe: float


class OrderFlowBacktester:
    """Backtest order flow signals on resolved markets."""

    def __init__(
        self,
        fills_dir: str = "data/polymarket/orderflow/",
        resolution_csv: str = "data/polymarket/gamma_resolution.csv",
        min_fills_per_market: int = 20,
    ) -> None:
        self._fills_dir = Path(fills_dir)
        self._resolver = ResolutionResolver(resolution_csv)
        self._min_fills = min_fills_per_market

    def run(
        self,
        signal: str,
        hours_before_close: int,
        *,
        window_hours: int = 4,
    ) -> BacktestResult:
        """Run backtest for a single signal/horizon/window combination."""
        if not self._fills_dir.exists():
            return BacktestResult(signal=signal, hours_before_close=hours_before_close,
                                  window_hours=window_hours, win_rate=0.0, n_trades=0,
                                  mean_signal_score=0.0, sharpe=0.0)

        outcomes: list[tuple[float, bool]] = []  # (signal_score, correctly_predicted)

        for path in self._fills_dir.glob("*.parquet"):
            token_id = path.stem
            resolution = self._resolver.resolve(token_id)
            if resolution is None:
                continue

            try:
                df = pd.read_parquet(path)
            except Exception:
                continue

            if len(df) < self._min_fills:
                continue

            resolved_yes = resolution >= 99.0

            # Compute signal score
            if signal == "taker_imbalance":
                score = OrderFlowFeatures.taker_imbalance(df, window_hours=window_hours)
            elif signal == "large_order":
                score = OrderFlowFeatures.large_order(df)
            else:
                continue

            if math.isnan(score):
                continue

            # Did the signal predict correctly?
            predicted_yes = score > 0
            correct = predicted_yes == resolved_yes
            outcomes.append((abs(score), correct))

        n = len(outcomes)
        if n == 0:
            return BacktestResult(signal=signal, hours_before_close=hours_before_close,
                                  window_hours=window_hours, win_rate=0.0, n_trades=0,
                                  mean_signal_score=0.0, sharpe=0.0)

        wins = sum(1 for _, c in outcomes if c)
        win_rate = wins / n
        mean_score = sum(s for s, _ in outcomes) / n

        # Rough Sharpe
        import numpy as np
        binary = np.array([1.0 if c else 0.0 for _, c in outcomes])
        std = float(binary.std())
        sharpe = (win_rate - 0.5) / std if std > 0 else 0.0

        return BacktestResult(
            signal=signal,
            hours_before_close=hours_before_close,
            window_hours=window_hours,
            win_rate=round(win_rate, 4),
            n_trades=n,
            mean_signal_score=round(mean_score, 4),
            sharpe=round(sharpe, 4),
        )

    def run_sweep(self) -> pd.DataFrame:
        """Run full parameter sweep. Returns DataFrame sorted by sharpe."""
        rows: list[dict[str, Any]] = []
        signals = ["taker_imbalance", "large_order"]
        horizons = [1, 4, 12, 24, 48]
        windows = [1, 4, 12]

        for sig in signals:
            for hbc in horizons:
                for wh in windows:
                    result = self.run(sig, hbc, window_hours=wh)
                    rows.append({
                        "signal": result.signal,
                        "hours_before_close": result.hours_before_close,
                        "window_hours": result.window_hours,
                        "win_rate": result.win_rate,
                        "n_trades": result.n_trades,
                        "sharpe": result.sharpe,
                    })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("sharpe", ascending=False)
        return df
