from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_platform.research.diagnostics import (
    POSITION_EPSILON,
    activity_note,
    classify_activity_profile,
)
from trading_platform.signals.common import normalize_price_frame
from trading_platform.simulation.portfolio import simulate_target_weight_portfolio


@dataclass
class XsecMomentumResult:
    scores: pd.DataFrame
    asset_returns: pd.DataFrame
    target_weights: pd.DataFrame
    positions: pd.DataFrame
    timeseries: pd.DataFrame
    summary: dict[str, float | int | bool | None]


def build_close_panel(prepared_frames: dict[str, dict[str, object]]) -> tuple[pd.DataFrame, dict[str, Path]]:
    close_frames: list[pd.Series] = []
    feature_paths: dict[str, Path] = {}

    for symbol, prepared in prepared_frames.items():
        normalized = normalize_price_frame(prepared["df"])
        close_frames.append(normalized["close"].rename(symbol))
        feature_paths[symbol] = Path(prepared["path"])

    if not close_frames:
        raise ValueError("No symbol frames available for cross-sectional research")

    close_panel = pd.concat(close_frames, axis=1, join="inner").sort_index()
    if close_panel.empty:
        raise ValueError("No overlapping timestamps across the requested symbol set")
    return close_panel, feature_paths


def compute_xsec_momentum_scores(
    close_panel: pd.DataFrame,
    *,
    lookback_bars: int,
    skip_bars: int = 0,
) -> pd.DataFrame:
    if lookback_bars <= 0:
        raise ValueError("lookback_bars must be positive")
    if skip_bars < 0:
        raise ValueError("skip_bars must be >= 0")

    reference_close = close_panel.shift(skip_bars) if skip_bars > 0 else close_panel
    return reference_close / reference_close.shift(lookback_bars) - 1.0


def build_xsec_topn_weights(
    scores: pd.DataFrame,
    *,
    top_n: int,
    rebalance_bars: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if rebalance_bars <= 0:
        raise ValueError("rebalance_bars must be positive")

    selection_rows: list[dict[str, float]] = []
    weight_rows: list[pd.Series] = []

    for row_number, timestamp in enumerate(scores.index):
        if row_number % rebalance_bars != 0:
            continue

        row_scores = scores.loc[timestamp].dropna().sort_values(ascending=False)
        selected = row_scores.head(top_n).index.tolist()

        selection_row = {symbol: 1.0 if symbol in selected else 0.0 for symbol in scores.columns}
        selection_rows.append({"timestamp": timestamp, **selection_row})

        weight_row = pd.Series(0.0, index=scores.columns, name=timestamp)
        if selected:
            weight_row.loc[selected] = 1.0 / len(selected)
        weight_rows.append(weight_row)

    if not weight_rows:
        empty = pd.DataFrame(0.0, index=scores.index, columns=scores.columns)
        return empty.copy(), empty.copy()

    rebalance_weights = pd.DataFrame(weight_rows).sort_index()
    rebalance_selection = (
        pd.DataFrame(selection_rows)
        .set_index("timestamp")
        .reindex(rebalance_weights.index)
        .fillna(0.0)
    )
    target_weights = rebalance_weights.reindex(scores.index).ffill().fillna(0.0)
    selection = rebalance_selection.reindex(scores.index).ffill().fillna(0.0)
    return selection, target_weights


def weight_sum_profile(target_weights: pd.DataFrame) -> pd.Series:
    return target_weights.fillna(0.0).sum(axis=1)


def _average_holding_period(positions: pd.DataFrame) -> float | None:
    holding_lengths: list[int] = []
    for symbol in positions.columns:
        current = 0
        for active in positions[symbol].fillna(0.0).astype(float).tolist():
            if active > POSITION_EPSILON:
                current += 1
                continue
            if current > 0:
                holding_lengths.append(current)
                current = 0
        if current > 0:
            holding_lengths.append(current)
    if not holding_lengths:
        return None
    return float(sum(holding_lengths) / len(holding_lengths))


def summarize_xsec_result(
    *,
    result,
    positions: pd.DataFrame,
    target_weights: pd.DataFrame,
    strategy: str,
    lookback_bars: int,
    skip_bars: int,
    top_n: int,
    rebalance_bars: int,
) -> dict[str, object]:
    summary = dict(result.summary)

    previous_positions = positions.shift(1, fill_value=0.0)
    entry_count = int(((positions > POSITION_EPSILON) & (previous_positions <= POSITION_EPSILON)).sum().sum())
    exit_count = int(((positions <= POSITION_EPSILON) & (previous_positions > POSITION_EPSILON)).sum().sum())
    trade_count = entry_count + exit_count
    percent_time_in_market = float((target_weights.abs().sum(axis=1) > POSITION_EPSILON).mean() * 100.0)
    final_position_size = float(target_weights.abs().sum(axis=1).iloc[-1])
    rebalance_count = int((result.timeseries["turnover"].fillna(0.0) > POSITION_EPSILON).sum())
    weight_sums = weight_sum_profile(target_weights)

    diagnostics: dict[str, object] = {
        "strategy": strategy,
        "lookback_bars": lookback_bars,
        "skip_bars": skip_bars,
        "top_n": top_n,
        "rebalance_bars": rebalance_bars,
        "Return [%]": summary.get("portfolio_total_return", float("nan")) * 100.0,
        "Sharpe Ratio": summary.get("portfolio_sharpe", float("nan")),
        "Max. Drawdown [%]": summary.get("portfolio_max_drawdown", float("nan")) * 100.0,
        "benchmark_return_pct": summary.get("benchmark_total_return", float("nan")) * 100.0,
        "excess_return_pct": summary.get("excess_total_return", float("nan")) * 100.0,
        "trade_count": trade_count,
        "entry_count": entry_count,
        "exit_count": exit_count,
        "percent_time_in_market": percent_time_in_market,
        "average_holding_period_bars": _average_holding_period(positions),
        "final_position_size": final_position_size,
        "ended_in_cash": bool(final_position_size <= POSITION_EPSILON),
        "average_number_of_holdings": float(positions.sum(axis=1).mean()),
        "rebalance_count": rebalance_count,
        "mean_turnover": float(result.timeseries["turnover"].fillna(0.0).mean()),
        "percent_invested": float(target_weights.abs().sum(axis=1).mean() * 100.0),
        "average_gross_exposure": float(target_weights.abs().sum(axis=1).mean()),
        "initial_equity": summary.get("portfolio_initial_equity"),
        "final_equity": summary.get("portfolio_final_equity"),
        "rebalance_weight_sum_min": float(weight_sums.min()) if not weight_sums.empty else float("nan"),
        "rebalance_weight_sum_max": float(weight_sums.max()) if not weight_sums.empty else float("nan"),
    }
    diagnostics["activity_profile"] = classify_activity_profile(diagnostics)
    diagnostics["activity_note"] = activity_note(diagnostics)
    return diagnostics


def run_xsec_momentum_topn(
    *,
    prepared_frames: dict[str, dict[str, object]],
    lookback_bars: int,
    skip_bars: int,
    top_n: int,
    rebalance_bars: int,
    commission: float,
    cash: float,
) -> XsecMomentumResult:
    close_panel, _ = build_close_panel(prepared_frames)
    asset_returns = close_panel.pct_change().fillna(0.0)
    scores = compute_xsec_momentum_scores(
        close_panel,
        lookback_bars=lookback_bars,
        skip_bars=skip_bars,
    )
    positions, target_weights = build_xsec_topn_weights(
        scores,
        top_n=top_n,
        rebalance_bars=rebalance_bars,
    )
    simulation = simulate_target_weight_portfolio(
        asset_returns=asset_returns,
        target_weights=target_weights,
        cost_per_turnover=commission,
        initial_equity=cash,
    )
    summary = summarize_xsec_result(
        result=simulation,
        positions=positions,
        target_weights=target_weights,
        strategy="xsec_momentum_topn",
        lookback_bars=lookback_bars,
        skip_bars=skip_bars,
        top_n=top_n,
        rebalance_bars=rebalance_bars,
    )
    return XsecMomentumResult(
        scores=scores,
        asset_returns=asset_returns,
        target_weights=target_weights,
        positions=positions,
        timeseries=simulation.timeseries,
        summary=summary,
    )
