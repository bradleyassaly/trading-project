from __future__ import annotations

import math
from dataclasses import asdict, dataclass

import pandas as pd

from trading_platform.construction.weighting import build_top_n_portfolio_weights
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.simulation.portfolio import simulate_target_weight_portfolio


@dataclass(frozen=True)
class CompositePortfolioConfig:
    enabled: bool = True
    top_n: int = 10
    long_quantile: float = 0.2
    short_quantile: float = 0.2
    max_weight: float | None = None
    gross_target: float = 1.0
    net_target: float = 0.0
    commission: float = 0.0
    rebalance_frequency: str = "daily"
    timing: str = "next_bar"
    modes: tuple[str, ...] = ("long_only_top_n", "long_short_quantile")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


DEFAULT_COMPOSITE_PORTFOLIO_CONFIG = CompositePortfolioConfig()


def build_asset_return_matrix(symbol_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not symbol_data:
        return pd.DataFrame()

    return_frames: list[pd.DataFrame] = []
    for symbol, df in symbol_data.items():
        if "timestamp" not in df.columns or "close" not in df.columns:
            continue
        frame = df[["timestamp", "close"]].copy()
        frame["asset_return"] = pd.to_numeric(frame["close"], errors="coerce").pct_change()
        frame["symbol"] = symbol
        return_frames.append(frame[["timestamp", "symbol", "asset_return"]])

    if not return_frames:
        return pd.DataFrame()

    returns_df = pd.concat(return_frames, ignore_index=True)
    return (
        returns_df.pivot(index="timestamp", columns="symbol", values="asset_return")
        .sort_index()
        .sort_index(axis=1)
        .fillna(0.0)
    )


def _quantile_count(count: int, quantile: float) -> int:
    if count <= 0 or quantile <= 0.0:
        return 0
    return min(count, max(1, math.ceil(count * quantile)))


def build_long_short_quantile_weights(
    scores_df: pd.DataFrame,
    *,
    long_quantile: float,
    short_quantile: float,
    gross_target: float,
    net_target: float,
) -> pd.DataFrame:
    if scores_df.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "weight"])

    required_cols = {"timestamp", "symbol", "score"}
    missing = required_cols - set(scores_df.columns)
    if missing:
        raise ValueError(f"scores_df missing required columns: {sorted(missing)}")

    long_target = (gross_target + net_target) / 2.0
    short_target = (gross_target - net_target) / 2.0
    output_frames: list[pd.DataFrame] = []

    for timestamp, group in scores_df.groupby("timestamp", sort=True):
        ranked = group.sort_values(["score", "symbol"], ascending=[False, True]).copy()
        n_obs = len(ranked)
        long_n = _quantile_count(n_obs, long_quantile)
        short_n = _quantile_count(n_obs, short_quantile)

        legs: list[pd.DataFrame] = []
        if long_n > 0 and long_target > 0:
            long_leg = ranked.head(long_n).copy()
            long_leg["weight"] = long_target / len(long_leg)
            legs.append(long_leg[["timestamp", "symbol", "weight"]])

        if short_n > 0 and short_target > 0:
            short_leg = ranked.tail(short_n).copy()
            short_leg["weight"] = -short_target / len(short_leg)
            legs.append(short_leg[["timestamp", "symbol", "weight"]])

        if not legs:
            continue

        combined = pd.concat(legs, ignore_index=True)
        combined = (
            combined.groupby(["timestamp", "symbol"], as_index=False)["weight"].sum()
        )
        output_frames.append(combined)

    if not output_frames:
        return pd.DataFrame(columns=["timestamp", "symbol", "weight"])

    return pd.concat(output_frames, ignore_index=True)


def build_composite_portfolio_weights(
    composite_scores_df: pd.DataFrame,
    *,
    config: CompositePortfolioConfig,
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "symbol",
        "weight",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
    ]
    if composite_scores_df.empty or not config.enabled:
        return pd.DataFrame(columns=columns)

    frames: list[pd.DataFrame] = []
    for (horizon, weighting_scheme), group in composite_scores_df.groupby(
        ["horizon", "weighting_scheme"]
    ):
        score_frame = group.rename(columns={"composite_score": "score"})[
            ["timestamp", "symbol", "score"]
        ].copy()

        if "long_only_top_n" in config.modes:
            long_only = build_top_n_portfolio_weights(
                scores_df=score_frame,
                top_n=config.top_n,
                max_weight=config.max_weight,
            )
            if not long_only.empty:
                long_only["horizon"] = int(horizon)
                long_only["weighting_scheme"] = str(weighting_scheme)
                long_only["portfolio_mode"] = "long_only_top_n"
                frames.append(long_only[columns])

        if "long_short_quantile" in config.modes:
            long_short = build_long_short_quantile_weights(
                score_frame,
                long_quantile=config.long_quantile,
                short_quantile=config.short_quantile,
                gross_target=config.gross_target,
                net_target=config.net_target,
            )
            if not long_short.empty:
                long_short["horizon"] = int(horizon)
                long_short["weighting_scheme"] = str(weighting_scheme)
                long_short["portfolio_mode"] = "long_short_quantile"
                frames.append(long_short[columns])

    if not frames:
        return pd.DataFrame(columns=columns)

    return pd.concat(frames, ignore_index=True).sort_values(
        ["portfolio_mode", "weighting_scheme", "horizon", "timestamp", "symbol"]
    ).reset_index(drop=True)


def run_composite_portfolio_backtest(
    composite_scores_df: pd.DataFrame,
    *,
    symbol_data: dict[str, pd.DataFrame],
    config: CompositePortfolioConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    returns_columns = [
        "timestamp",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "portfolio_return",
        "portfolio_return_gross",
        "portfolio_return_net",
        "portfolio_equity",
        "benchmark_return",
        "benchmark_equity",
        "turnover",
        "transaction_cost",
        "active_positions",
    ]
    metrics_columns = [
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "portfolio_total_return",
        "portfolio_annual_return",
        "portfolio_annual_vol",
        "portfolio_sharpe",
        "portfolio_max_drawdown",
        "benchmark_total_return",
        "benchmark_annual_return",
        "benchmark_annual_vol",
        "benchmark_sharpe",
        "benchmark_max_drawdown",
        "excess_total_return",
        "mean_turnover",
        "mean_active_positions",
    ]
    if composite_scores_df.empty or not config.enabled:
        diagnostics = {
            "config": config.to_dict(),
            "portfolios_tested": 0,
        }
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            pd.DataFrame(columns=["timestamp", "symbol", "weight", "horizon", "weighting_scheme", "portfolio_mode"]),
            diagnostics,
        )

    asset_returns = build_asset_return_matrix(symbol_data)
    if asset_returns.empty:
        diagnostics = {
            "config": config.to_dict(),
            "portfolios_tested": 0,
            "reason": "no_asset_returns",
        }
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            pd.DataFrame(columns=["timestamp", "symbol", "weight", "horizon", "weighting_scheme", "portfolio_mode"]),
            diagnostics,
        )

    weights_df = build_composite_portfolio_weights(composite_scores_df, config=config)
    if weights_df.empty:
        diagnostics = {
            "config": config.to_dict(),
            "portfolios_tested": 0,
            "reason": "no_portfolio_weights",
        }
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            weights_df,
            diagnostics,
        )

    timeseries_frames: list[pd.DataFrame] = []
    metrics_rows: list[dict[str, object]] = []
    diagnostics_rows: list[dict[str, object]] = []
    policy = ExecutionPolicy(
        timing=config.timing,
        rebalance_frequency=config.rebalance_frequency,
    )

    for (horizon, weighting_scheme, portfolio_mode), group in weights_df.groupby(
        ["horizon", "weighting_scheme", "portfolio_mode"]
    ):
        weight_matrix = (
            group.pivot(index="timestamp", columns="symbol", values="weight")
            .sort_index()
            .sort_index(axis=1)
            .fillna(0.0)
        )
        if weight_matrix.empty:
            continue

        simulation = simulate_target_weight_portfolio(
            asset_returns=asset_returns,
            target_weights=weight_matrix,
            cost_per_turnover=config.commission,
            execution_policy=policy,
        )
        active_positions = (simulation.weights.abs() > 0).sum(axis=1)
        timeseries = simulation.timeseries.reset_index().rename(columns={"index": "timestamp"})
        timeseries["active_positions"] = (
            active_positions.reindex(timeseries["timestamp"]).fillna(0.0).to_numpy()
        )
        timeseries["horizon"] = int(horizon)
        timeseries["weighting_scheme"] = str(weighting_scheme)
        timeseries["portfolio_mode"] = str(portfolio_mode)
        timeseries_frames.append(timeseries[returns_columns])

        metrics_rows.append(
            {
                "horizon": int(horizon),
                "weighting_scheme": str(weighting_scheme),
                "portfolio_mode": str(portfolio_mode),
                **simulation.summary,
                "mean_turnover": float(simulation.timeseries["turnover"].mean()),
                "mean_active_positions": float(active_positions.mean()),
            }
        )
        diagnostics_rows.append(
            {
                "horizon": int(horizon),
                "weighting_scheme": str(weighting_scheme),
                "portfolio_mode": str(portfolio_mode),
                "turnover_mean": float(simulation.timeseries["turnover"].mean()),
                "turnover_max": float(simulation.timeseries["turnover"].max()),
                "position_count_mean": float(active_positions.mean()),
                "position_count_max": float(active_positions.max()),
                "n_weight_rows": int(len(group)),
            }
        )

    portfolio_returns_df = (
        pd.concat(timeseries_frames, ignore_index=True)
        if timeseries_frames
        else pd.DataFrame(columns=returns_columns)
    )
    portfolio_metrics_df = pd.DataFrame(metrics_rows, columns=metrics_columns)
    diagnostics = {
        "config": config.to_dict(),
        "portfolios_tested": int(len(metrics_rows)),
        "portfolio_diagnostics": diagnostics_rows,
    }
    return portfolio_returns_df, portfolio_metrics_df, weights_df, diagnostics
