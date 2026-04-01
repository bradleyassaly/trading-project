"""
Binary prediction market backtester for Kalshi signals.

Binary market semantics differ from continuous equity returns:
- Entry price is the yes-price (0–100 scale) at signal trigger time.
- Exit price is the resolution value: 100 (YES resolves) or 0 (NO resolves).
- Edge per trade = (resolution_price - entry_price) for a YES position
  when signal is positive, or (entry_price - resolution_price) for a
  NO position when signal is negative.
- No partial fills, no slippage modelled at this research stage.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pandas as pd

from trading_platform.kalshi.signals import KalshiSignalFamily, compute_kalshi_signal


@dataclass(frozen=True)
class KalshiBacktestResult:
    signal_family: str
    n_trades: int
    win_rate: float
    mean_edge: float
    sharpe: float
    max_drawdown: float
    ic: float


def _compute_max_drawdown(equity_curve: pd.Series) -> float:
    if equity_curve.empty:
        return 0.0
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max.replace(0.0, float("nan"))
    return float(drawdown.min()) if not drawdown.isna().all() else 0.0


def _compute_sharpe(edges: pd.Series) -> float:
    if len(edges) < 2:
        return 0.0
    std = float(edges.std())
    if std == 0.0 or math.isnan(std):
        return 0.0
    return float(edges.mean()) / std * math.sqrt(min(len(edges), 252))


def _compute_ic(signal: pd.Series, forward_edge: pd.Series) -> float:
    valid = pd.concat([signal, forward_edge], axis=1).dropna()
    if len(valid) < 5:
        return float("nan")
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))


class KalshiBacktester:
    """
    Evaluate Kalshi signal families against historical resolution data.

    :param entry_threshold:  Minimum |signal| to enter a trade (default 0.5).
    :param long_only:        Only take YES positions (True) or also NO (False).
    """

    def __init__(
        self,
        *,
        entry_threshold: float = 0.5,
        long_only: bool = False,
    ) -> None:
        self.entry_threshold = entry_threshold
        self.long_only = long_only

    def run(
        self,
        feature_dir: Path,
        resolution_data: pd.DataFrame,
        signal_families: Sequence[KalshiSignalFamily],
        output_dir: Path,
    ) -> list[KalshiBacktestResult]:
        """
        Run the backtester for each signal family.

        :param feature_dir:      Directory containing ``<ticker>.parquet`` feature files.
        :param resolution_data:  DataFrame with columns ``ticker`` and ``resolution_price``
                                 (0 or 100) and optionally ``resolved_at``.
        :param signal_families:  Sequence of :class:`KalshiSignalFamily` to evaluate.
        :param output_dir:       Directory where ``backtest_results.csv`` is written.
        :returns:                List of :class:`KalshiBacktestResult`, one per family.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        resolution_map: dict[str, float] = {}
        if not resolution_data.empty and "ticker" in resolution_data.columns and "resolution_price" in resolution_data.columns:
            for _, row in resolution_data.iterrows():
                resolution_map[str(row["ticker"])] = float(row["resolution_price"])

        feature_files = sorted(feature_dir.glob("*.parquet"))
        if not feature_files:
            rows: list[dict] = []
            results = []
            for family in signal_families:
                result = KalshiBacktestResult(
                    signal_family=family.name,
                    n_trades=0,
                    win_rate=float("nan"),
                    mean_edge=float("nan"),
                    sharpe=float("nan"),
                    max_drawdown=float("nan"),
                    ic=float("nan"),
                )
                results.append(result)
                rows.append(_result_to_row(result))
            pd.DataFrame(rows).to_csv(output_dir / "backtest_results.csv", index=False)
            return results

        results = []
        summary_rows = []

        for family in signal_families:
            trade_edges: list[float] = []
            signals_all: list[float] = []
            edges_all: list[float] = []

            for fpath in feature_files:
                ticker = fpath.stem
                resolution_price = resolution_map.get(ticker)
                if resolution_price is None:
                    continue

                try:
                    df = pd.read_parquet(fpath)
                except Exception:
                    continue

                if df.empty:
                    continue

                signal = compute_kalshi_signal(df, family)
                if signal.isna().all():
                    continue

                last_valid_idx = signal.last_valid_index()
                if last_valid_idx is None:
                    continue

                entry_signal = float(signal.loc[last_valid_idx])
                if math.isnan(entry_signal):
                    continue

                entry_close_col = "close"
                if entry_close_col not in df.columns:
                    continue
                entry_price = float(pd.to_numeric(df[entry_close_col], errors="coerce").dropna().iloc[-1])

                if abs(entry_signal) < self.entry_threshold:
                    continue

                if entry_signal > 0:
                    edge = resolution_price - entry_price
                elif not self.long_only:
                    edge = entry_price - resolution_price
                else:
                    continue

                trade_edges.append(edge)

                forward_edge_for_ic = resolution_price - entry_price
                signals_all.append(entry_signal)
                edges_all.append(forward_edge_for_ic)

            if not trade_edges:
                result = KalshiBacktestResult(
                    signal_family=family.name,
                    n_trades=0,
                    win_rate=float("nan"),
                    mean_edge=float("nan"),
                    sharpe=float("nan"),
                    max_drawdown=float("nan"),
                    ic=float("nan"),
                )
            else:
                edges_series = pd.Series(trade_edges)
                equity = edges_series.cumsum()
                win_rate = float((edges_series > 0).mean())
                mean_edge = float(edges_series.mean())
                sharpe = _compute_sharpe(edges_series)
                max_dd = _compute_max_drawdown(equity)
                ic = _compute_ic(pd.Series(signals_all), pd.Series(edges_all))

                result = KalshiBacktestResult(
                    signal_family=family.name,
                    n_trades=len(trade_edges),
                    win_rate=win_rate,
                    mean_edge=mean_edge,
                    sharpe=sharpe,
                    max_drawdown=max_dd,
                    ic=ic,
                )

            results.append(result)
            summary_rows.append(_result_to_row(result))

        pd.DataFrame(summary_rows).to_csv(output_dir / "backtest_results.csv", index=False)
        return results


def _result_to_row(result: KalshiBacktestResult) -> dict:
    return {
        "signal_family": result.signal_family,
        "n_trades": result.n_trades,
        "win_rate": result.win_rate,
        "mean_edge": result.mean_edge,
        "sharpe": result.sharpe,
        "max_drawdown": result.max_drawdown,
        "ic": result.ic,
    }
