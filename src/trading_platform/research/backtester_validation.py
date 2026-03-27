from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_platform.integrations.vectorbt_validation_adapter import run_vectorbt_target_weight_scenario
from trading_platform.simulation.metrics import summarize_equity_curve
from trading_platform.simulation.portfolio import simulate_target_weight_portfolio
from trading_platform.simulation.single_asset import simulate_single_asset


@dataclass(frozen=True)
class ValidationScenario:
    scenario_name: str
    close_prices: pd.DataFrame
    target_weights: pd.DataFrame
    platform_returns: pd.Series
    platform_equity: pd.Series
    fees: float


def _single_asset_buy_and_hold() -> ValidationScenario:
    index = pd.date_range("2025-01-01", periods=6, freq="B")
    close = pd.Series([100.0, 101.0, 103.0, 102.0, 105.0, 107.0], index=index)
    returns = close.pct_change().fillna(0.0)
    signal_frame = pd.DataFrame({"position": 1.0, "asset_return": returns}, index=index)
    platform = simulate_single_asset(signal_frame, cost_per_turnover=0.0, initial_equity=1.0)
    weights = pd.DataFrame({"AAPL": 1.0}, index=index)
    return ValidationScenario(
        scenario_name="single_asset_buy_and_hold",
        close_prices=pd.DataFrame({"AAPL": close}),
        target_weights=weights,
        platform_returns=platform.timeseries["strategy_return_net"],
        platform_equity=platform.timeseries["equity"],
        fees=0.0,
    )


def _monthly_equal_weight_rebalance() -> ValidationScenario:
    index = pd.date_range("2025-01-01", periods=8, freq="B")
    close = pd.DataFrame(
        {
            "AAPL": [100, 101, 102, 101, 103, 104, 103, 105],
            "MSFT": [50, 51, 50, 52, 53, 52, 54, 55],
        },
        index=index,
    ).astype(float)
    returns = close.pct_change().fillna(0.0)
    weights = pd.DataFrame(0.5, index=index, columns=close.columns)
    platform = simulate_target_weight_portfolio(returns, weights, cost_per_turnover=0.0, initial_equity=1.0)
    return ValidationScenario(
        scenario_name="monthly_equal_weight_rebalance",
        close_prices=close,
        target_weights=weights,
        platform_returns=platform.timeseries["portfolio_return_net"],
        platform_equity=platform.timeseries["portfolio_equity"],
        fees=0.0,
    )


def _top_n_cross_sectional_momentum() -> ValidationScenario:
    index = pd.date_range("2025-01-01", periods=6, freq="B")
    close = pd.DataFrame(
        {
            "AAPL": [100, 102, 104, 106, 108, 110],
            "MSFT": [100, 99, 98, 97, 96, 95],
            "NVDA": [50, 51, 53, 54, 56, 57],
        },
        index=index,
    ).astype(float)
    returns = close.pct_change().fillna(0.0)
    momentum_rank = returns.rolling(2).sum().fillna(0.0)
    weights = pd.DataFrame(0.0, index=index, columns=close.columns)
    for ts, row in momentum_rank.iterrows():
        top_symbol = str(row.idxmax())
        weights.loc[ts, top_symbol] = 1.0
    platform = simulate_target_weight_portfolio(returns, weights, cost_per_turnover=0.0, initial_equity=1.0)
    return ValidationScenario(
        scenario_name="top_n_cross_sectional_momentum",
        close_prices=close,
        target_weights=weights,
        platform_returns=platform.timeseries["portfolio_return_net"],
        platform_equity=platform.timeseries["portfolio_equity"],
        fees=0.0,
    )


def _vol_scaled_single_asset() -> ValidationScenario:
    index = pd.date_range("2025-01-01", periods=7, freq="B")
    close = pd.Series([100, 103, 101, 104, 102, 105, 106], index=index, dtype=float)
    returns = close.pct_change().fillna(0.0)
    rolling_vol = returns.rolling(2).std().replace(0.0, np.nan).bfill().fillna(0.01)
    target_weight = (0.02 / rolling_vol).clip(upper=1.0).fillna(1.0)
    weights = pd.DataFrame({"AAPL": target_weight}, index=index)
    platform = simulate_target_weight_portfolio(
        pd.DataFrame({"AAPL": returns}), weights, cost_per_turnover=0.0, initial_equity=1.0
    )
    return ValidationScenario(
        scenario_name="vol_scaled_single_asset",
        close_prices=pd.DataFrame({"AAPL": close}),
        target_weights=weights,
        platform_returns=platform.timeseries["portfolio_return_net"],
        platform_equity=platform.timeseries["portfolio_equity"],
        fees=0.0,
    )


def build_validation_scenarios() -> list[ValidationScenario]:
    return [
        _single_asset_buy_and_hold(),
        _monthly_equal_weight_rebalance(),
        _top_n_cross_sectional_momentum(),
        _vol_scaled_single_asset(),
    ]


def run_vectorbt_validation_harness(*, package_override=None) -> dict[str, Any]:
    summary_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for scenario in build_validation_scenarios():
        vectorbt_result = run_vectorbt_target_weight_scenario(
            close_prices=scenario.close_prices,
            target_weights=scenario.target_weights,
            fees=scenario.fees,
            package_override=package_override,
        )
        platform_metrics = summarize_equity_curve(
            returns=scenario.platform_returns,
            equity=scenario.platform_equity,
        )
        vectorbt_metrics = summarize_equity_curve(
            returns=vectorbt_result.returns,
            equity=vectorbt_result.equity,
        )
        aligned = pd.concat(
            [scenario.platform_returns.rename("platform"), vectorbt_result.returns.rename("vectorbt")],
            axis=1,
        ).dropna()
        correlation = float(aligned["platform"].corr(aligned["vectorbt"])) if len(aligned) > 1 else 1.0
        summary_rows.append(
            {
                "scenario_name": scenario.scenario_name,
                "total_return_difference": float(platform_metrics["total_return"] - vectorbt_metrics["total_return"]),
                "annual_return_difference": float(
                    platform_metrics["annual_return"] - vectorbt_metrics["annual_return"]
                ),
                "max_drawdown_difference": float(platform_metrics["max_drawdown"] - vectorbt_metrics["max_drawdown"]),
                "return_series_correlation": correlation,
                "trade_count_difference": int(
                    abs(
                        len(vectorbt_result.trades.index)
                        - int((scenario.target_weights.diff().abs().sum(axis=1) > 0).sum())
                    )
                ),
                "holdings_overlap": float((scenario.target_weights > 0).mean().mean()),
            }
        )
        metric_rows.append(
            {
                "scenario_name": scenario.scenario_name,
                "platform_total_return": float(platform_metrics["total_return"]),
                "vectorbt_total_return": float(vectorbt_metrics["total_return"]),
                "platform_max_drawdown": float(platform_metrics["max_drawdown"]),
                "vectorbt_max_drawdown": float(vectorbt_metrics["max_drawdown"]),
            }
        )
        if not vectorbt_result.trades.empty:
            trade_frame = vectorbt_result.trades.copy()
            trade_frame["scenario_name"] = scenario.scenario_name
            trade_rows.extend(trade_frame.to_dict(orient="records"))
    return {
        "summary": pd.DataFrame(summary_rows),
        "metrics": metric_rows,
        "trade_comparison": pd.DataFrame(trade_rows),
    }


def write_vectorbt_validation_artifacts(
    *,
    result: dict[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary_path = output_path / "vectorbt_validation_summary.csv"
    metrics_path = output_path / "vectorbt_validation_metrics.json"
    trade_comparison_path = output_path / "vectorbt_validation_trade_comparison.csv"
    pd.DataFrame(result["summary"]).to_csv(summary_path, index=False)
    pd.DataFrame(result.get("trade_comparison", pd.DataFrame())).to_csv(trade_comparison_path, index=False)
    metrics_path.write_text(json.dumps({"rows": result.get("metrics", [])}, indent=2), encoding="utf-8")
    return {
        "vectorbt_validation_summary_path": summary_path,
        "vectorbt_validation_metrics_path": metrics_path,
        "vectorbt_validation_trade_comparison_path": trade_comparison_path,
    }
