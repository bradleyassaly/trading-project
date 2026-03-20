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
    min_price: float | None = None
    min_volume: float | None = None
    min_avg_dollar_volume: float | None = None
    max_adv_participation: float = 0.05
    max_position_pct_of_adv: float = 0.1
    max_notional_per_name: float | None = None
    slippage_bps_per_turnover: float = 0.0
    slippage_bps_per_adv: float = 10.0
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


def _empty_implementability_report_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "horizon",
            "weighting_scheme",
            "portfolio_mode",
            "excluded_names",
            "excluded_weight_fraction",
            "mean_estimated_slippage_cost",
            "total_estimated_slippage_cost",
            "mean_capacity_multiple",
            "min_capacity_multiple",
            "baseline_total_return",
            "implementable_total_return",
            "return_drag",
        ]
    )


def _empty_capacity_scenarios_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "horizon",
            "weighting_scheme",
            "portfolio_mode",
            "scenario",
            "max_adv_participation",
            "mean_capacity_multiple",
            "min_capacity_multiple",
        ]
    )


def _empty_liquidity_filtered_metrics_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=_portfolio_metrics_columns()
        + [
            "mean_estimated_slippage_cost",
            "total_estimated_slippage_cost",
            "excluded_names",
            "excluded_weight_fraction",
        ]
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


def build_liquidity_panel(
    symbol_data: dict[str, pd.DataFrame],
    *,
    adv_window: int = 20,
) -> pd.DataFrame:
    columns = ["timestamp", "symbol", "close", "volume", "dollar_volume", "avg_dollar_volume"]
    if not symbol_data:
        return pd.DataFrame(columns=columns)

    frames: list[pd.DataFrame] = []
    for symbol, df in symbol_data.items():
        if "timestamp" not in df.columns or "close" not in df.columns:
            continue
        frame = df[["timestamp", "close"]].copy()
        frame["symbol"] = symbol
        if "volume" in df.columns:
            frame["volume"] = pd.to_numeric(df["volume"], errors="coerce")
            frame["dollar_volume"] = (
                pd.to_numeric(frame["close"], errors="coerce") * frame["volume"]
            )
            frame["avg_dollar_volume"] = frame["dollar_volume"].rolling(adv_window, min_periods=1).mean()
        else:
            frame["volume"] = float("nan")
            frame["dollar_volume"] = float("nan")
            frame["avg_dollar_volume"] = float("nan")
        frames.append(frame[columns])

    return pd.concat(frames, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True) if frames else pd.DataFrame(columns=columns)


def _rescale_filtered_weights(group: pd.DataFrame) -> pd.DataFrame:
    result = group.copy()
    original_positive = result.loc[result["original_weight"] > 0, "original_weight"].sum()
    original_negative = result.loc[result["original_weight"] < 0, "original_weight"].sum()
    current_positive = result.loc[result["weight"] > 0, "weight"].sum()
    current_negative = result.loc[result["weight"] < 0, "weight"].sum()

    if current_positive > 0 and original_positive > 0:
        result.loc[result["weight"] > 0, "weight"] *= original_positive / current_positive
    if current_negative < 0 and original_negative < 0:
        result.loc[result["weight"] < 0, "weight"] *= original_negative / current_negative
    return result


def apply_liquidity_filters(
    portfolio_weights_df: pd.DataFrame,
    *,
    liquidity_panel: pd.DataFrame,
    config: CompositePortfolioConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    exclusions_columns = [
        "timestamp",
        "symbol",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "reason",
        "original_weight",
    ]
    low_liquidity_columns = [
        "timestamp",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "excluded_weight_fraction",
    ]
    if portfolio_weights_df.empty:
        return (
            _empty_portfolio_weights_df(),
            pd.DataFrame(columns=exclusions_columns),
            pd.DataFrame(columns=low_liquidity_columns),
        )

    merged = portfolio_weights_df.merge(
        liquidity_panel,
        on=["timestamp", "symbol"],
        how="left",
    ).copy()
    merged["original_weight"] = merged["weight"]

    reasons: list[pd.Series] = []
    if config.min_price is not None:
        reasons.append(
            pd.Series(
                ["min_price" if pd.notna(v) and v < config.min_price else "" for v in merged["close"]],
                index=merged.index,
            )
        )
    if config.min_volume is not None:
        reasons.append(
            pd.Series(
                ["min_volume" if pd.notna(v) and v < config.min_volume else "" for v in merged["volume"]],
                index=merged.index,
            )
        )
    if config.min_avg_dollar_volume is not None:
        reasons.append(
            pd.Series(
                [
                    "min_avg_dollar_volume"
                    if pd.notna(v) and v < config.min_avg_dollar_volume
                    else ""
                    for v in merged["avg_dollar_volume"]
                ],
                index=merged.index,
            )
        )

    if reasons:
        reason_frame = pd.concat(reasons, axis=1).fillna("").astype(str)
        merged["reason"] = reason_frame.apply(
            lambda row: ";".join(value for value in row.tolist() if value),
            axis=1,
        )
    else:
        merged["reason"] = ""

    low_liquidity_fraction = (
        merged.assign(excluded_abs_weight=merged["original_weight"].abs().where(merged["reason"] != "", 0.0))
        .groupby(["timestamp", "horizon", "weighting_scheme", "portfolio_mode"], as_index=False)
        .agg(
            excluded_abs_weight=("excluded_abs_weight", "sum"),
            gross_weight=("original_weight", lambda s: s.abs().sum()),
        )
    )
    low_liquidity_fraction["excluded_weight_fraction"] = low_liquidity_fraction["excluded_abs_weight"] / low_liquidity_fraction["gross_weight"].replace(0.0, pd.NA)
    low_liquidity_fraction["excluded_weight_fraction"] = low_liquidity_fraction["excluded_weight_fraction"].fillna(0.0)
    low_liquidity_fraction = low_liquidity_fraction[low_liquidity_columns]

    exclusions_df = merged.loc[merged["reason"] != "", exclusions_columns].reset_index(drop=True)
    filtered = merged.loc[merged["reason"] == ""].copy()
    if filtered.empty:
        return _empty_portfolio_weights_df(), exclusions_df, low_liquidity_fraction

    filtered_groups = [
        _rescale_filtered_weights(group)
        for _, group in filtered.groupby(
            ["timestamp", "horizon", "weighting_scheme", "portfolio_mode"],
            sort=False,
        )
    ]
    filtered = (
        pd.concat(filtered_groups, ignore_index=True)
        if filtered_groups
        else filtered.iloc[0:0].copy()
    )
    return filtered[["timestamp", "symbol", "weight", "horizon", "weighting_scheme", "portfolio_mode"]], exclusions_df, low_liquidity_fraction


def estimate_capacity(
    portfolio_weights_df: pd.DataFrame,
    *,
    liquidity_panel: pd.DataFrame,
    config: CompositePortfolioConfig,
    participation_rate: float | None = None,
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "capacity_multiple",
    ]
    if portfolio_weights_df.empty:
        return pd.DataFrame(columns=columns)

    participation = participation_rate if participation_rate is not None else config.max_adv_participation
    merged = portfolio_weights_df.merge(
        liquidity_panel[["timestamp", "symbol", "avg_dollar_volume"]],
        on=["timestamp", "symbol"],
        how="left",
    ).copy()
    merged["avg_dollar_volume"] = pd.to_numeric(merged["avg_dollar_volume"], errors="coerce")
    merged["abs_weight"] = merged["weight"].abs()

    capacity_rows: list[dict[str, object]] = []
    for keys, group in merged.groupby(["timestamp", "horizon", "weighting_scheme", "portfolio_mode"]):
        group = group.loc[group["abs_weight"] > 0].copy()
        if group.empty:
            continue

        capacity_limits: list[float] = []
        if group["avg_dollar_volume"].notna().any():
            valid = group.loc[group["avg_dollar_volume"] > 0].copy()
            if not valid.empty and participation > 0:
                participation_capacity = (
                    valid["avg_dollar_volume"] * participation / valid["abs_weight"]
                ).min()
                capacity_limits.append(float(participation_capacity))
            if not valid.empty and config.max_position_pct_of_adv > 0:
                position_capacity = (
                    valid["avg_dollar_volume"] * config.max_position_pct_of_adv / valid["abs_weight"]
                ).min()
                capacity_limits.append(float(position_capacity))
        if config.max_notional_per_name is not None and config.max_notional_per_name > 0:
            notional_capacity = (config.max_notional_per_name / group["abs_weight"]).min()
            capacity_limits.append(float(notional_capacity))

        capacity_multiple = min(capacity_limits) if capacity_limits else float("nan")
        capacity_rows.append(
            {
                "timestamp": keys[0],
                "horizon": int(keys[1]),
                "weighting_scheme": str(keys[2]),
                "portfolio_mode": str(keys[3]),
                "capacity_multiple": capacity_multiple,
            }
        )

    return pd.DataFrame(capacity_rows, columns=columns)


def estimate_slippage_costs(
    portfolio_weights_df: pd.DataFrame,
    *,
    liquidity_panel: pd.DataFrame,
    config: CompositePortfolioConfig,
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "estimated_slippage_cost",
    ]
    if portfolio_weights_df.empty:
        return pd.DataFrame(columns=columns)

    frames: list[pd.DataFrame] = []
    for keys, group in portfolio_weights_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"]):
        pivot = (
            group.pivot(index="timestamp", columns="symbol", values="weight")
            .sort_index()
            .sort_index(axis=1)
            .fillna(0.0)
        )
        delta = pivot.diff().abs().fillna(pivot.abs())
        trade_df = delta.stack().reset_index(name="abs_trade_weight")
        trade_df["horizon"] = int(keys[0])
        trade_df["weighting_scheme"] = str(keys[1])
        trade_df["portfolio_mode"] = str(keys[2])
        frames.append(trade_df.rename(columns={"level_1": "symbol"}))

    trades = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["timestamp", "symbol", "abs_trade_weight", "horizon", "weighting_scheme", "portfolio_mode"]
    )
    if trades.empty:
        return pd.DataFrame(columns=columns)

    trades = trades.merge(
        liquidity_panel[["timestamp", "symbol", "avg_dollar_volume"]],
        on=["timestamp", "symbol"],
        how="left",
    )
    trades["avg_dollar_volume"] = pd.to_numeric(trades["avg_dollar_volume"], errors="coerce")
    trades["adv_fraction"] = trades["abs_trade_weight"] / trades["avg_dollar_volume"].replace(0.0, pd.NA)
    trades["estimated_slippage_cost"] = trades["abs_trade_weight"] * (
        config.slippage_bps_per_turnover + trades["adv_fraction"].fillna(0.0) * config.slippage_bps_per_adv
    ) / 10000.0

    return (
        trades.groupby(["timestamp", "horizon", "weighting_scheme", "portfolio_mode"], as_index=False)[
            "estimated_slippage_cost"
        ]
        .sum()
        .sort_values(["portfolio_mode", "weighting_scheme", "horizon", "timestamp"])
        .reset_index(drop=True)
    )


def build_capacity_scenarios(
    portfolio_weights_df: pd.DataFrame,
    *,
    liquidity_panel: pd.DataFrame,
    config: CompositePortfolioConfig,
) -> pd.DataFrame:
    columns = [
        "horizon",
        "weighting_scheme",
        "portfolio_mode",
        "scenario",
        "max_adv_participation",
        "mean_capacity_multiple",
        "min_capacity_multiple",
    ]
    if portfolio_weights_df.empty:
        return pd.DataFrame(columns=columns)

    scenario_rates = [
        ("tight", max(config.max_adv_participation / 2.0, 1e-6)),
        ("base", config.max_adv_participation),
        ("loose", config.max_adv_participation * 2.0),
    ]
    rows: list[dict[str, object]] = []
    for scenario_name, participation_rate in scenario_rates:
        scenario_df = estimate_capacity(
            portfolio_weights_df,
            liquidity_panel=liquidity_panel,
            config=config,
            participation_rate=participation_rate,
        )
        if scenario_df.empty:
            continue
        for keys, group in scenario_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"]):
            rows.append(
                {
                    "horizon": int(keys[0]),
                    "weighting_scheme": str(keys[1]),
                    "portfolio_mode": str(keys[2]),
                    "scenario": scenario_name,
                    "max_adv_participation": participation_rate,
                    "mean_capacity_multiple": float(group["capacity_multiple"].mean()),
                    "min_capacity_multiple": float(group["capacity_multiple"].min()),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _summarize_simulation_outputs(
    simulation,
    *,
    horizon: int,
    weighting_scheme: str,
    portfolio_mode: str,
    returns_columns: list[str],
) -> tuple[pd.DataFrame, dict[str, object], dict[str, object]]:
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

    metrics_row = {
        "horizon": int(horizon),
        "weighting_scheme": str(weighting_scheme),
        "portfolio_mode": str(portfolio_mode),
        **simulation.summary,
        "mean_turnover": float(simulation.timeseries["turnover"].mean()),
        "mean_active_positions": float(active_positions.mean()),
    }
    diagnostics_row = {
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
    }
    return timeseries[returns_columns], metrics_row, diagnostics_row


def _run_portfolio_simulation(
    weights_df: pd.DataFrame,
    *,
    asset_returns: pd.DataFrame,
    config: CompositePortfolioConfig,
    returns_columns: list[str],
    metrics_columns: list[str],
    slippage_costs_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, object]]]:
    if weights_df.empty:
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            [],
        )

    timeseries_frames: list[pd.DataFrame] = []
    metrics_rows: list[dict[str, object]] = []
    diagnostics_rows: list[dict[str, object]] = []
    policy = ExecutionPolicy(
        timing=config.timing,
        rebalance_frequency=config.rebalance_frequency,
    )
    slippage_df = (
        slippage_costs_df.copy()
        if slippage_costs_df is not None
        else pd.DataFrame(
            columns=[
                "timestamp",
                "horizon",
                "weighting_scheme",
                "portfolio_mode",
                "estimated_slippage_cost",
            ]
        )
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
        timeseries, metrics_row, diagnostics_row = _summarize_simulation_outputs(
            simulation,
            horizon=int(horizon),
            weighting_scheme=str(weighting_scheme),
            portfolio_mode=str(portfolio_mode),
            returns_columns=returns_columns,
        )
        slippage_slice = slippage_df.loc[
            (slippage_df["horizon"] == horizon)
            & (slippage_df["weighting_scheme"] == weighting_scheme)
            & (slippage_df["portfolio_mode"] == portfolio_mode)
        ][["timestamp", "estimated_slippage_cost"]]
        if not slippage_slice.empty:
            timeseries = timeseries.merge(slippage_slice, on="timestamp", how="left")
            timeseries["estimated_slippage_cost"] = (
                pd.to_numeric(timeseries["estimated_slippage_cost"], errors="coerce").fillna(0.0)
            )
            timeseries["portfolio_return"] = (
                pd.to_numeric(timeseries["portfolio_return"], errors="coerce")
                - timeseries["estimated_slippage_cost"]
            )
            timeseries["portfolio_return_net"] = (
                pd.to_numeric(timeseries["portfolio_return_net"], errors="coerce")
                - timeseries["estimated_slippage_cost"]
            )
            timeseries["transaction_cost"] = (
                pd.to_numeric(timeseries["transaction_cost"], errors="coerce")
                + timeseries["estimated_slippage_cost"]
            )
            timeseries["portfolio_equity"] = (1.0 + timeseries["portfolio_return_net"]).cumprod()
            adjusted_summary = _summarize_timeseries_slice(timeseries)
            metrics_row["portfolio_annual_return"] = adjusted_summary["annual_return"]
            metrics_row["portfolio_annual_vol"] = adjusted_summary["annual_vol"]
            metrics_row["portfolio_sharpe"] = adjusted_summary["sharpe"]
            metrics_row["portfolio_max_drawdown"] = adjusted_summary["max_drawdown"]
            metrics_row["mean_turnover"] = adjusted_summary["mean_turnover"]
            metrics_row["mean_active_positions"] = adjusted_summary["mean_active_positions"]
            metrics_row["portfolio_total_return"] = (
                float(timeseries["portfolio_equity"].iloc[-1] - 1.0)
                if not timeseries.empty
                else 0.0
            )
            metrics_row["excess_total_return"] = (
                float(metrics_row["portfolio_total_return"])
                - float(metrics_row["benchmark_total_return"])
            )
            diagnostics_row["estimated_slippage_cost_mean"] = float(
                timeseries["estimated_slippage_cost"].mean()
            )
            diagnostics_row["estimated_slippage_cost_total"] = float(
                timeseries["estimated_slippage_cost"].sum()
            )
            timeseries = timeseries.drop(columns=["estimated_slippage_cost"])
        else:
            diagnostics_row["estimated_slippage_cost_mean"] = 0.0
            diagnostics_row["estimated_slippage_cost_total"] = 0.0

        metrics_rows.append(metrics_row)
        diagnostics_row["n_weight_rows"] = int(len(group))
        diagnostics_rows.append(diagnostics_row)
        timeseries_frames.append(timeseries[returns_columns])

    return (
        pd.concat(timeseries_frames, ignore_index=True)
        if timeseries_frames
        else pd.DataFrame(columns=returns_columns),
        pd.DataFrame(metrics_rows, columns=metrics_columns),
        diagnostics_rows,
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
    returns_columns = _portfolio_returns_columns()
    metrics_columns = _portfolio_metrics_columns()
    if composite_scores_df.empty or not config.enabled:
        diagnostics = {
            "config": config.to_dict(),
            "portfolios_tested": 0,
            "asset_returns_matrix": pd.DataFrame(),
            "implementability_report": _empty_implementability_report_df(),
            "liquidity_filtered_portfolio_metrics": _empty_liquidity_filtered_metrics_df(),
            "capacity_scenarios": _empty_capacity_scenarios_df(),
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
            "implementability_report": _empty_implementability_report_df(),
            "liquidity_filtered_portfolio_metrics": _empty_liquidity_filtered_metrics_df(),
            "capacity_scenarios": _empty_capacity_scenarios_df(),
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
            "implementability_report": _empty_implementability_report_df(),
            "liquidity_filtered_portfolio_metrics": _empty_liquidity_filtered_metrics_df(),
            "capacity_scenarios": _empty_capacity_scenarios_df(),
        }
        return (
            pd.DataFrame(columns=returns_columns),
            pd.DataFrame(columns=metrics_columns),
            weights_df,
            diagnostics,
        )

    portfolio_returns_df, portfolio_metrics_df, diagnostics_rows = _run_portfolio_simulation(
        weights_df,
        asset_returns=asset_returns,
        config=config,
        returns_columns=returns_columns,
        metrics_columns=metrics_columns,
    )
    liquidity_panel = build_liquidity_panel(symbol_data)
    filtered_weights_df, exclusions_df, low_liquidity_fraction_df = apply_liquidity_filters(
        weights_df,
        liquidity_panel=liquidity_panel,
        config=config,
    )
    slippage_costs_df = estimate_slippage_costs(
        filtered_weights_df,
        liquidity_panel=liquidity_panel,
        config=config,
    )
    capacity_df = estimate_capacity(
        filtered_weights_df,
        liquidity_panel=liquidity_panel,
        config=config,
    )
    capacity_scenarios_df = build_capacity_scenarios(
        filtered_weights_df,
        liquidity_panel=liquidity_panel,
        config=config,
    )
    (
        liquidity_filtered_returns_df,
        liquidity_filtered_metrics_df,
        liquidity_filtered_diagnostics,
    ) = _run_portfolio_simulation(
        filtered_weights_df,
        asset_returns=asset_returns,
        config=config,
        returns_columns=returns_columns,
        metrics_columns=metrics_columns,
        slippage_costs_df=slippage_costs_df,
    )

    baseline_metrics = portfolio_metrics_df.rename(
        columns={
            "portfolio_total_return": "baseline_total_return",
            "mean_turnover": "baseline_mean_turnover",
        }
    )
    implementable_metrics = liquidity_filtered_metrics_df.rename(
        columns={
            "portfolio_total_return": "implementable_total_return",
            "mean_turnover": "implementable_mean_turnover",
        }
    )
    exclusions_summary = (
        exclusions_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"], as_index=False)
        .agg(excluded_names=("symbol", "nunique"))
        if not exclusions_df.empty
        else pd.DataFrame(columns=["horizon", "weighting_scheme", "portfolio_mode", "excluded_names"])
    )
    low_liquidity_summary = (
        low_liquidity_fraction_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"], as_index=False)
        .agg(excluded_weight_fraction=("excluded_weight_fraction", "mean"))
        if not low_liquidity_fraction_df.empty
        else pd.DataFrame(columns=["horizon", "weighting_scheme", "portfolio_mode", "excluded_weight_fraction"])
    )
    slippage_summary = (
        slippage_costs_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"], as_index=False)
        .agg(
            mean_estimated_slippage_cost=("estimated_slippage_cost", "mean"),
            total_estimated_slippage_cost=("estimated_slippage_cost", "sum"),
        )
        if not slippage_costs_df.empty
        else pd.DataFrame(
            columns=[
                "horizon",
                "weighting_scheme",
                "portfolio_mode",
                "mean_estimated_slippage_cost",
                "total_estimated_slippage_cost",
            ]
        )
    )
    capacity_summary = (
        capacity_df.groupby(["horizon", "weighting_scheme", "portfolio_mode"], as_index=False)
        .agg(
            mean_capacity_multiple=("capacity_multiple", "mean"),
            min_capacity_multiple=("capacity_multiple", "min"),
        )
        if not capacity_df.empty
        else pd.DataFrame(
            columns=[
                "horizon",
                "weighting_scheme",
                "portfolio_mode",
                "mean_capacity_multiple",
                "min_capacity_multiple",
            ]
        )
    )
    implementability_report_df = baseline_metrics[
        ["horizon", "weighting_scheme", "portfolio_mode", "baseline_total_return"]
    ].merge(
        implementable_metrics[
            ["horizon", "weighting_scheme", "portfolio_mode", "implementable_total_return"]
        ],
        on=["horizon", "weighting_scheme", "portfolio_mode"],
        how="outer",
    )
    for frame in (
        exclusions_summary,
        low_liquidity_summary,
        slippage_summary,
        capacity_summary,
    ):
        implementability_report_df = implementability_report_df.merge(
            frame,
            on=["horizon", "weighting_scheme", "portfolio_mode"],
            how="left",
        )
    if implementability_report_df.empty:
        implementability_report_df = _empty_implementability_report_df()
    else:
        implementability_report_df["excluded_names"] = (
            pd.to_numeric(implementability_report_df["excluded_names"], errors="coerce").fillna(0.0)
        )
        implementability_report_df["excluded_weight_fraction"] = (
            pd.to_numeric(implementability_report_df["excluded_weight_fraction"], errors="coerce").fillna(0.0)
        )
        implementability_report_df["mean_estimated_slippage_cost"] = (
            pd.to_numeric(implementability_report_df["mean_estimated_slippage_cost"], errors="coerce").fillna(0.0)
        )
        implementability_report_df["total_estimated_slippage_cost"] = (
            pd.to_numeric(implementability_report_df["total_estimated_slippage_cost"], errors="coerce").fillna(0.0)
        )
        implementability_report_df["return_drag"] = (
            pd.to_numeric(implementability_report_df["baseline_total_return"], errors="coerce").fillna(0.0)
            - pd.to_numeric(implementability_report_df["implementable_total_return"], errors="coerce").fillna(0.0)
        )
        implementability_report_df = implementability_report_df[
            _empty_implementability_report_df().columns.tolist()
        ]
    if liquidity_filtered_metrics_df.empty:
        liquidity_filtered_metrics_df = _empty_liquidity_filtered_metrics_df()
    else:
        liquidity_filtered_metrics_df = liquidity_filtered_metrics_df.merge(
            slippage_summary,
            on=["horizon", "weighting_scheme", "portfolio_mode"],
            how="left",
        ).merge(
            exclusions_summary,
            on=["horizon", "weighting_scheme", "portfolio_mode"],
            how="left",
        ).merge(
            low_liquidity_summary,
            on=["horizon", "weighting_scheme", "portfolio_mode"],
            how="left",
        )
        liquidity_filtered_metrics_df["mean_estimated_slippage_cost"] = (
            pd.to_numeric(
                liquidity_filtered_metrics_df["mean_estimated_slippage_cost"],
                errors="coerce",
            ).fillna(0.0)
        )
        liquidity_filtered_metrics_df["total_estimated_slippage_cost"] = (
            pd.to_numeric(
                liquidity_filtered_metrics_df["total_estimated_slippage_cost"],
                errors="coerce",
            ).fillna(0.0)
        )
        liquidity_filtered_metrics_df["excluded_names"] = (
            pd.to_numeric(liquidity_filtered_metrics_df["excluded_names"], errors="coerce").fillna(0.0)
        )
        liquidity_filtered_metrics_df["excluded_weight_fraction"] = (
            pd.to_numeric(
                liquidity_filtered_metrics_df["excluded_weight_fraction"],
                errors="coerce",
            ).fillna(0.0)
        )
        liquidity_filtered_metrics_df = liquidity_filtered_metrics_df[
            _empty_liquidity_filtered_metrics_df().columns.tolist()
        ]
    diagnostics = {
        "config": config.to_dict(),
        "portfolios_tested": int(len(portfolio_metrics_df)),
        "portfolio_diagnostics": diagnostics_rows,
        "asset_returns_matrix": asset_returns,
        "liquidity_filtered_portfolio_diagnostics": liquidity_filtered_diagnostics,
        "liquidity_filtered_weights": filtered_weights_df,
        "liquidity_exclusions": exclusions_df,
        "low_liquidity_weight_fraction": low_liquidity_fraction_df,
        "estimated_slippage_costs": slippage_costs_df,
        "implementability_report": implementability_report_df,
        "liquidity_filtered_portfolio_metrics": liquidity_filtered_metrics_df,
        "capacity_scenarios": capacity_scenarios_df,
    }
    return portfolio_returns_df, portfolio_metrics_df, weights_df, diagnostics
