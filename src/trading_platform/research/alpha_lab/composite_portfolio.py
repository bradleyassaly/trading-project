from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Iterable

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


def _portfolio_returns_columns() -> list[str]:
    return [
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


def _portfolio_metrics_columns() -> list[str]:
    return [
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


def _empty_portfolio_weights_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["timestamp", "symbol", "weight", "horizon", "weighting_scheme", "portfolio_mode"]
    )


def compute_drawdown_duration(equity: pd.Series) -> int:
    if equity.empty:
        return 0
    drawdown = equity / equity.cummax() - 1.0
    max_duration = 0
    current_duration = 0
    for value in drawdown.fillna(0.0):
        if value < 0:
            current_duration += 1
            max_duration = max(max_duration, current_duration)
        else:
            current_duration = 0
    return int(max_duration)


def _summarize_timeseries_slice(timeseries: pd.DataFrame) -> dict[str, float]:
    if timeseries.empty:
        return {
            "n_periods": 0.0,
            "total_return": 0.0,
            "annual_return": float("nan"),
            "annual_vol": float("nan"),
            "sharpe": float("nan"),
            "max_drawdown": 0.0,
            "max_drawdown_duration": 0.0,
            "mean_turnover": 0.0,
            "mean_active_positions": 0.0,
        }

    net_returns = pd.to_numeric(timeseries["portfolio_return_net"], errors="coerce").fillna(0.0)
    equity = pd.to_numeric(timeseries["portfolio_equity"], errors="coerce").ffill().fillna(1.0)
    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1.0) if len(equity) > 0 else 0.0
    annual_vol = float(net_returns.std(ddof=0) * math.sqrt(252)) if len(net_returns) > 0 else float("nan")
    annual_return = (
        float((1.0 + net_returns).prod() ** (252 / len(net_returns)) - 1.0)
        if len(net_returns) > 0 and (1.0 + net_returns).prod() > 0
        else float("nan")
    )
    sharpe = (
        float(annual_return / annual_vol)
        if pd.notna(annual_return) and pd.notna(annual_vol) and annual_vol > 0
        else float("nan")
    )
    drawdown = equity / equity.cummax() - 1.0
    return {
        "n_periods": float(len(timeseries)),
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "max_drawdown": float(drawdown.min()) if len(drawdown) > 0 else 0.0,
        "max_drawdown_duration": float(compute_drawdown_duration(equity)),
        "mean_turnover": float(timeseries["turnover"].mean()),
        "mean_active_positions": float(timeseries["active_positions"].mean()),
    }


def build_period_performance_report(
    portfolio_returns_df: pd.DataFrame,
    *,
    period: str = "year",
) -> pd.DataFrame:
    columns = [
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "period",
        "n_periods",
        "total_return",
        "annual_return",
        "annual_vol",
        "sharpe",
        "max_drawdown",
        "max_drawdown_duration",
        "mean_turnover",
        "mean_active_positions",
    ]
    if portfolio_returns_df.empty:
        return pd.DataFrame(columns=columns)

    frame = portfolio_returns_df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    if period == "year":
        frame["period"] = frame["timestamp"].dt.year.astype(str)
    else:
        raise ValueError(f"Unsupported period: {period}")

    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(["horizon", "weighting_scheme", "portfolio_mode", "period"]):
        rows.append(
            {
                "horizon": int(keys[0]),
                "weighting_scheme": str(keys[1]),
                "portfolio_mode": str(keys[2]),
                "period": str(keys[3]),
                **_summarize_timeseries_slice(group),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def build_market_regime_labels(asset_returns: pd.DataFrame) -> pd.DataFrame:
    if asset_returns.empty:
        return pd.DataFrame(columns=["timestamp", "regime", "cross_sectional_vol"])

    cross_sectional_vol = asset_returns.std(axis=1, ddof=0).fillna(0.0)
    threshold = float(cross_sectional_vol.median()) if len(cross_sectional_vol) > 0 else 0.0
    return pd.DataFrame(
        {
            "timestamp": cross_sectional_vol.index,
            "cross_sectional_vol": cross_sectional_vol.to_numpy(),
            "regime": [
                "high_vol" if value >= threshold else "low_vol"
                for value in cross_sectional_vol.to_numpy()
            ],
        }
    )


def build_regime_performance_report(
    portfolio_returns_df: pd.DataFrame,
    *,
    asset_returns: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "regime",
        "n_periods",
        "total_return",
        "annual_return",
        "annual_vol",
        "sharpe",
        "max_drawdown",
        "max_drawdown_duration",
        "mean_turnover",
        "mean_active_positions",
    ]
    if portfolio_returns_df.empty or asset_returns.empty:
        return pd.DataFrame(columns=columns)

    regime_labels = build_market_regime_labels(asset_returns)
    if regime_labels.empty:
        return pd.DataFrame(columns=columns)

    frame = portfolio_returns_df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    frame = frame.merge(regime_labels[["timestamp", "regime"]], on="timestamp", how="left")

    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(["horizon", "weighting_scheme", "portfolio_mode", "regime"]):
        rows.append(
            {
                "horizon": int(keys[0]),
                "weighting_scheme": str(keys[1]),
                "portfolio_mode": str(keys[2]),
                "regime": str(keys[3]),
                **_summarize_timeseries_slice(group),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def build_fold_stability_report(
    portfolio_returns_df: pd.DataFrame,
    *,
    folds: Iterable,
) -> pd.DataFrame:
    columns = [
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "folds_evaluated",
        "mean_fold_return",
        "std_fold_return",
        "worst_fold_return",
        "mean_fold_sharpe",
        "worst_fold_sharpe",
        "mean_fold_turnover",
        "mean_fold_max_drawdown",
    ]
    if portfolio_returns_df.empty:
        return pd.DataFrame(columns=columns)

    frame = portfolio_returns_df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    fold_rows: list[dict[str, object]] = []
    for fold in folds:
        fold_slice = frame.loc[
            (frame["timestamp"] >= fold.test_start) & (frame["timestamp"] <= fold.test_end)
        ].copy()
        if fold_slice.empty:
            continue
        for keys, group in fold_slice.groupby(["horizon", "weighting_scheme", "portfolio_mode"]):
            summary = _summarize_timeseries_slice(group)
            fold_rows.append(
                {
                    "horizon": int(keys[0]),
                    "weighting_scheme": str(keys[1]),
                    "portfolio_mode": str(keys[2]),
                    "fold_id": int(fold.fold_id),
                    "fold_return": summary["total_return"],
                    "fold_sharpe": summary["sharpe"],
                    "fold_turnover": summary["mean_turnover"],
                    "fold_max_drawdown": summary["max_drawdown"],
                }
            )

    if not fold_rows:
        return pd.DataFrame(columns=columns)

    fold_df = pd.DataFrame(fold_rows)
    return (
        fold_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"], as_index=False)
        .agg(
            folds_evaluated=("fold_id", "nunique"),
            mean_fold_return=("fold_return", "mean"),
            std_fold_return=("fold_return", "std"),
            worst_fold_return=("fold_return", "min"),
            mean_fold_sharpe=("fold_sharpe", "mean"),
            worst_fold_sharpe=("fold_sharpe", "min"),
            mean_fold_turnover=("fold_turnover", "mean"),
            mean_fold_max_drawdown=("fold_max_drawdown", "mean"),
        )
        .fillna({"std_fold_return": 0.0})
        .sort_values(["mean_fold_return", "worst_fold_return"], ascending=[False, False])
        .reset_index(drop=True)
    )


def shuffle_composite_scores_by_date(
    composite_scores_df: pd.DataFrame,
    *,
    random_state: int = 0,
) -> pd.DataFrame:
    if composite_scores_df.empty:
        return composite_scores_df.copy()

    shuffled_frames: list[pd.DataFrame] = []
    for _, group in composite_scores_df.groupby(["timestamp", "horizon", "weighting_scheme"], sort=False):
        shuffled = group.copy()
        shuffled["composite_score"] = shuffled["composite_score"].sample(
            frac=1.0,
            random_state=random_state,
        ).to_numpy()
        shuffled_frames.append(shuffled)

    return pd.concat(shuffled_frames, ignore_index=True) if shuffled_frames else composite_scores_df.iloc[0:0].copy()


def lag_composite_scores(
    composite_scores_df: pd.DataFrame,
    *,
    periods: int = 1,
) -> pd.DataFrame:
    if composite_scores_df.empty:
        return composite_scores_df.copy()

    lagged = composite_scores_df.copy().sort_values(
        ["horizon", "weighting_scheme", "symbol", "timestamp"]
    )
    lagged["composite_score"] = lagged.groupby(
        ["horizon", "weighting_scheme", "symbol"]
    )["composite_score"].shift(periods)
    return lagged.dropna(subset=["composite_score"]).reset_index(drop=True)


def build_robustness_report(
    portfolio_returns_df: pd.DataFrame,
    portfolio_weights_df: pd.DataFrame,
    *,
    folds: Iterable,
) -> pd.DataFrame:
    columns = [
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "mean_top_position_weight",
        "max_top_position_weight",
        "mean_net_exposure",
        "mean_gross_exposure",
        "mean_long_positions",
        "mean_short_positions",
        "mean_turnover",
        "turnover_p95",
        "portfolio_max_drawdown",
        "portfolio_max_drawdown_duration",
        "folds_evaluated",
        "mean_fold_return",
        "std_fold_return",
        "worst_fold_return",
        "mean_fold_sharpe",
        "worst_fold_sharpe",
    ]
    if portfolio_returns_df.empty or portfolio_weights_df.empty:
        return pd.DataFrame(columns=columns)

    fold_report = build_fold_stability_report(portfolio_returns_df, folds=folds)
    weights = portfolio_weights_df.copy()
    rows: list[dict[str, object]] = []
    for keys, weight_group in weights.groupby(["horizon", "weighting_scheme", "portfolio_mode"]):
        top_weights = weight_group.groupby("timestamp")["weight"].apply(lambda s: s.abs().max())
        net_exposure = weight_group.groupby("timestamp")["weight"].sum()
        gross_exposure = weight_group.groupby("timestamp")["weight"].apply(lambda s: s.abs().sum())
        long_counts = weight_group.groupby("timestamp")["weight"].apply(lambda s: (s > 0).sum())
        short_counts = weight_group.groupby("timestamp")["weight"].apply(lambda s: (s < 0).sum())
        returns_group = portfolio_returns_df.loc[
            (portfolio_returns_df["horizon"] == keys[0])
            & (portfolio_returns_df["weighting_scheme"] == keys[1])
            & (portfolio_returns_df["portfolio_mode"] == keys[2])
        ].copy()
        summary = _summarize_timeseries_slice(returns_group)
        fold_slice = fold_report.loc[
            (fold_report["horizon"] == keys[0])
            & (fold_report["weighting_scheme"] == keys[1])
            & (fold_report["portfolio_mode"] == keys[2])
        ]
        rows.append(
            {
                "horizon": int(keys[0]),
                "weighting_scheme": str(keys[1]),
                "portfolio_mode": str(keys[2]),
                "mean_top_position_weight": float(top_weights.mean()),
                "max_top_position_weight": float(top_weights.max()),
                "mean_net_exposure": float(net_exposure.mean()),
                "mean_gross_exposure": float(gross_exposure.mean()),
                "mean_long_positions": float(long_counts.mean()),
                "mean_short_positions": float(short_counts.mean()),
                "mean_turnover": float(returns_group["turnover"].mean()),
                "turnover_p95": float(returns_group["turnover"].quantile(0.95)),
                "portfolio_max_drawdown": summary["max_drawdown"],
                "portfolio_max_drawdown_duration": summary["max_drawdown_duration"],
                "folds_evaluated": float(fold_slice["folds_evaluated"].iloc[0]) if not fold_slice.empty else 0.0,
                "mean_fold_return": float(fold_slice["mean_fold_return"].iloc[0]) if not fold_slice.empty else float("nan"),
                "std_fold_return": float(fold_slice["std_fold_return"].iloc[0]) if not fold_slice.empty else float("nan"),
                "worst_fold_return": float(fold_slice["worst_fold_return"].iloc[0]) if not fold_slice.empty else float("nan"),
                "mean_fold_sharpe": float(fold_slice["mean_fold_sharpe"].iloc[0]) if not fold_slice.empty else float("nan"),
                "worst_fold_sharpe": float(fold_slice["worst_fold_sharpe"].iloc[0]) if not fold_slice.empty else float("nan"),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def run_stress_tests(
    composite_scores_df: pd.DataFrame,
    *,
    symbol_data: dict[str, pd.DataFrame],
    config: CompositePortfolioConfig,
) -> pd.DataFrame:
    columns = [
        "stress_test",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "portfolio_total_return",
        "portfolio_sharpe",
        "mean_turnover",
    ]
    if composite_scores_df.empty:
        return pd.DataFrame(columns=columns)

    scenarios = {
        "baseline": composite_scores_df,
        "shuffle_by_date": shuffle_composite_scores_by_date(composite_scores_df),
        "lag_plus_one": lag_composite_scores(composite_scores_df, periods=1),
    }
    rows: list[dict[str, object]] = []
    for scenario_name, scenario_scores in scenarios.items():
        _, metrics_df, _, _ = run_composite_portfolio_backtest(
            scenario_scores,
            symbol_data=symbol_data,
            config=config,
        )
        if metrics_df.empty:
            continue
        for _, row in metrics_df.iterrows():
            rows.append(
                {
                    "stress_test": scenario_name,
                    "horizon": int(row["horizon"]),
                    "weighting_scheme": str(row["weighting_scheme"]),
                    "portfolio_mode": str(row["portfolio_mode"]),
                    "portfolio_total_return": float(row["portfolio_total_return"]),
                    "portfolio_sharpe": float(row["portfolio_sharpe"]),
                    "mean_turnover": float(row["mean_turnover"]),
                }
            )
    return pd.DataFrame(rows, columns=columns)


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
            "asset_returns_matrix": pd.DataFrame(),
        }
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            _empty_portfolio_weights_df(),
            diagnostics,
        )

    asset_returns = build_asset_return_matrix(symbol_data)
    if asset_returns.empty:
        diagnostics = {
            "config": config.to_dict(),
            "portfolios_tested": 0,
            "reason": "no_asset_returns",
            "asset_returns_matrix": asset_returns,
        }
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            _empty_portfolio_weights_df(),
            diagnostics,
        )

    weights_df = build_composite_portfolio_weights(composite_scores_df, config=config)
    if weights_df.empty:
        diagnostics = {
            "config": config.to_dict(),
            "portfolios_tested": 0,
            "reason": "no_portfolio_weights",
            "asset_returns_matrix": asset_returns,
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
        top_position_weight = simulation.weights.abs().max(axis=1).fillna(0.0)
        long_positions = (simulation.weights > 0).sum(axis=1)
        short_positions = (simulation.weights < 0).sum(axis=1)
        gross_exposure = simulation.weights.abs().sum(axis=1).fillna(0.0)
        net_exposure = simulation.weights.sum(axis=1).fillna(0.0)
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
                "turnover_p95": float(simulation.timeseries["turnover"].quantile(0.95)),
                "position_count_mean": float(active_positions.mean()),
                "position_count_max": float(active_positions.max()),
                "long_positions_mean": float(long_positions.mean()),
                "short_positions_mean": float(short_positions.mean()),
                "top_position_weight_mean": float(top_position_weight.mean()),
                "top_position_weight_max": float(top_position_weight.max()),
                "gross_exposure_mean": float(gross_exposure.mean()),
                "net_exposure_mean": float(net_exposure.mean()),
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
        "asset_returns_matrix": asset_returns,
    }
    return portfolio_returns_df, portfolio_metrics_df, weights_df, diagnostics
