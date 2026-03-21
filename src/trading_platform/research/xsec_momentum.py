from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_platform.metadata.groups import load_symbol_groups
from trading_platform.research.diagnostics import (
    POSITION_EPSILON,
    activity_note,
    classify_activity_profile,
)
from trading_platform.signals.common import normalize_price_frame
from trading_platform.simulation.portfolio import equal_weight_buy_and_hold_returns
from trading_platform.simulation.metrics import summarize_equity_curve
from trading_platform.simulation.portfolio import simulate_target_weight_portfolio


@dataclass
class XsecMomentumResult:
    scores: pd.DataFrame
    asset_returns: pd.DataFrame
    target_weights: pd.DataFrame
    positions: pd.DataFrame
    timeseries: pd.DataFrame
    rebalance_diagnostics: pd.DataFrame
    summary: dict[str, object]


def build_close_panel(prepared_frames: dict[str, dict[str, object]]) -> tuple[pd.DataFrame, dict[str, Path]]:
    close_frames: list[pd.Series] = []
    feature_paths: dict[str, Path] = {}

    for symbol, prepared in prepared_frames.items():
        normalized = normalize_price_frame(prepared["df"])
        feature_paths[symbol] = Path(prepared["path"])
        if normalized.empty:
            continue
        close_frames.append(normalized["close"].rename(symbol))

    if not close_frames:
        raise ValueError("No symbol frames available for cross-sectional research")

    close_panel = pd.concat(close_frames, axis=1).sort_index().copy()
    if close_panel.empty:
        raise ValueError("No timestamps available across the requested symbol set")
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


def build_volume_panel(prepared_frames: dict[str, dict[str, object]]) -> pd.DataFrame:
    volume_frames: list[pd.Series] = []
    for symbol, prepared in prepared_frames.items():
        normalized = normalize_price_frame(prepared["df"])
        if normalized.empty or "volume" not in normalized.columns:
            continue
        volume_frames.append(pd.to_numeric(normalized["volume"], errors="coerce").rename(symbol))

    if not volume_frames:
        return pd.DataFrame()

    return pd.concat(volume_frames, axis=1).sort_index().copy()


def compute_avg_dollar_volume_panel(
    close_panel: pd.DataFrame,
    volume_panel: pd.DataFrame,
    *,
    window: int = 20,
) -> pd.DataFrame:
    if volume_panel.empty:
        return pd.DataFrame(index=close_panel.index, columns=close_panel.columns, dtype=float)

    aligned_volume = volume_panel.reindex(index=close_panel.index, columns=close_panel.columns)
    dollar_volume = close_panel * aligned_volume
    return dollar_volume.rolling(window=window, min_periods=window).mean()


def build_symbol_start_dates(prepared_frames: dict[str, dict[str, object]]) -> dict[str, str]:
    start_dates: dict[str, str] = {}
    for symbol, prepared in prepared_frames.items():
        normalized = normalize_price_frame(prepared["df"])
        if normalized.empty:
            continue
        start_dates[symbol] = pd.Timestamp(normalized.index.min()).date().isoformat()
    return start_dates


def load_sector_map(symbols: list[str]) -> tuple[dict[str, str] | None, str | None]:
    try:
        raw_map = load_symbol_groups()
    except FileNotFoundError:
        return None, "sector_metadata_unavailable"

    sector_map = {symbol: raw_map.get(symbol.upper(), symbol.upper()) for symbol in symbols}
    return sector_map, None


def _score_gap_from_bps(turnover_buffer_bps: float) -> float:
    return float(turnover_buffer_bps) / 10_000.0


def _selected_group_counts(selected: list[str], sector_map: dict[str, str] | None) -> dict[str, int]:
    counts: dict[str, int] = {}
    if sector_map is None:
        return counts
    for symbol in selected:
        group = sector_map.get(symbol, symbol)
        counts[group] = counts.get(group, 0) + 1
    return counts


def _select_symbols_for_rebalance(
    *,
    row_scores: pd.Series,
    previous_weights: pd.Series,
    top_n: int,
    turnover_buffer_bps: float,
    sector_map: dict[str, str] | None,
    max_names_per_sector: int | None,
) -> tuple[list[str], dict[str, object]]:
    selected = row_scores.head(top_n).index.tolist()
    diagnostics = {
        "buffer_blocked_replacements": 0,
        "sector_cap_excluded_count": 0,
        "excluded_by_sector": set(),
    }

    if max_names_per_sector is not None and sector_map is not None:
        constrained: list[str] = []
        group_counts: dict[str, int] = {}
        for symbol in row_scores.index:
            group = sector_map.get(symbol, symbol)
            if group_counts.get(group, 0) >= max_names_per_sector:
                diagnostics["sector_cap_excluded_count"] += 1
                diagnostics["excluded_by_sector"].add(symbol)
                continue
            constrained.append(symbol)
            group_counts[group] = group_counts.get(group, 0) + 1
            if len(constrained) >= top_n:
                break
        selected = constrained

    threshold = _score_gap_from_bps(turnover_buffer_bps)
    if threshold <= 0.0:
        diagnostics["excluded_by_sector"] = sorted(diagnostics["excluded_by_sector"])
        return selected, diagnostics

    current_holdings = [symbol for symbol in previous_weights[previous_weights > POSITION_EPSILON].index if symbol in row_scores.index]
    if not current_holdings:
        diagnostics["excluded_by_sector"] = sorted(diagnostics["excluded_by_sector"])
        return selected, diagnostics

    retained = current_holdings[:]
    outsiders = [symbol for symbol in row_scores.index if symbol not in retained]

    while len(retained) < top_n and outsiders:
        candidate = outsiders.pop(0)
        if candidate not in retained:
            retained.append(candidate)

    if len(retained) >= top_n and outsiders:
        retained = sorted(retained, key=lambda symbol: (row_scores.get(symbol, float("-inf")), symbol), reverse=True)[:top_n]
        best_outsider = outsiders[0]
        worst_incumbent = min(retained, key=lambda symbol: (row_scores.get(symbol, float("-inf")), symbol))
        gap = float(row_scores.get(best_outsider, float("-inf")) - row_scores.get(worst_incumbent, float("-inf")))
        if gap > threshold:
            retained.remove(worst_incumbent)
            retained.append(best_outsider)
            retained = sorted(retained, key=lambda symbol: (row_scores.get(symbol, float("-inf")), symbol), reverse=True)[:top_n]
        else:
            diagnostics["buffer_blocked_replacements"] = 1

    diagnostics["excluded_by_sector"] = sorted(diagnostics["excluded_by_sector"])
    return retained, diagnostics


def _compute_weight_row(
    *,
    timestamp: pd.Timestamp,
    selected: list[str],
    asset_returns: pd.DataFrame,
    weighting_scheme: str,
    vol_lookback_bars: int,
) -> pd.Series:
    weight_row = pd.Series(0.0, index=asset_returns.columns, name=timestamp)
    if not selected:
        return weight_row

    if weighting_scheme == "equal":
        weight_row.loc[selected] = 1.0 / len(selected)
        return weight_row

    if weighting_scheme != "inv_vol":
        raise ValueError(f"Unsupported weighting_scheme: {weighting_scheme}")

    if vol_lookback_bars <= 1:
        raise ValueError("vol_lookback_bars must be > 1 for inv_vol weighting")

    returns_history = asset_returns.loc[:timestamp, selected]
    rolling_vol = returns_history.rolling(window=vol_lookback_bars, min_periods=vol_lookback_bars).std(ddof=0).iloc[-1]
    inverse_vol = 1.0 / rolling_vol.replace(0.0, pd.NA)
    inverse_vol = inverse_vol.replace([float("inf"), float("-inf")], pd.NA).dropna()

    if inverse_vol.empty:
        weight_row.loc[selected] = 1.0 / len(selected)
        return weight_row

    normalized = inverse_vol / inverse_vol.sum()
    weight_row.loc[normalized.index] = normalized.astype(float)
    remaining = [symbol for symbol in selected if symbol not in normalized.index]
    if remaining:
        fallback_weight = float(max(0.0, 1.0 - weight_row.sum())) / len(remaining)
        weight_row.loc[remaining] = fallback_weight
    return weight_row


def _apply_turnover_cap(
    previous_weights: pd.Series,
    ideal_weights: pd.Series,
    *,
    max_turnover_per_rebalance: float | None,
) -> tuple[pd.Series, bool]:
    if max_turnover_per_rebalance is None:
        return ideal_weights, False
    if max_turnover_per_rebalance < 0:
        raise ValueError("max_turnover_per_rebalance must be >= 0")

    turnover = float((ideal_weights - previous_weights).abs().sum())
    if turnover <= max_turnover_per_rebalance + 1e-12:
        return ideal_weights, False
    if turnover <= 0:
        return ideal_weights, False

    scale = float(max_turnover_per_rebalance) / turnover
    capped = previous_weights + (ideal_weights - previous_weights) * scale
    return capped.astype(float), True


def _apply_max_position_weight(
    weight_row: pd.Series,
    *,
    max_position_weight: float | None,
) -> pd.Series:
    if max_position_weight is None:
        return weight_row.astype(float)
    if max_position_weight <= 0 or max_position_weight > 1:
        raise ValueError("max_position_weight must be in (0, 1]")

    positive_mask = weight_row > POSITION_EPSILON
    capped = weight_row.clip(lower=0.0, upper=max_position_weight).astype(float)
    remaining_capacity = pd.Series(0.0, index=weight_row.index, dtype=float)
    remaining_capacity.loc[positive_mask] = max_position_weight - capped.loc[positive_mask]
    residual = float(max(0.0, 1.0 - capped.sum()))

    while residual > 1e-12:
        eligible = remaining_capacity[remaining_capacity > 1e-12]
        if eligible.empty:
            break
        allocation = residual * (eligible / eligible.sum())
        capped.loc[eligible.index] = capped.loc[eligible.index] + allocation
        capped = capped.clip(lower=0.0, upper=max_position_weight)
        remaining_capacity = max_position_weight - capped
        residual = float(max(0.0, 1.0 - capped.sum()))

    return capped


def build_xsec_topn_weights(
    scores: pd.DataFrame,
    *,
    close_panel: pd.DataFrame,
    asset_returns: pd.DataFrame,
    avg_dollar_volume_panel: pd.DataFrame | None = None,
    top_n: int,
    rebalance_bars: int,
    min_eligible_symbols: int = 1,
    max_position_weight: float | None = None,
    min_avg_dollar_volume: float | None = None,
    max_names_per_sector: int | None = None,
    turnover_buffer_bps: float = 0.0,
    max_turnover_per_rebalance: float | None = None,
    weighting_scheme: str = "equal",
    vol_lookback_bars: int = 20,
    sector_map: dict[str, str] | None = None,
    sector_warning: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if rebalance_bars <= 0:
        raise ValueError("rebalance_bars must be positive")
    if min_eligible_symbols <= 0:
        raise ValueError("min_eligible_symbols must be positive")

    selection_rows: list[dict[str, float]] = []
    weight_rows: list[pd.Series] = []
    diagnostics_rows: list[dict[str, object]] = []
    previous_weights = pd.Series(0.0, index=scores.columns, dtype=float)
    liquidity_filter_active = min_avg_dollar_volume is not None
    sector_cap_active = max_names_per_sector is not None and sector_map is not None

    for row_number, timestamp in enumerate(scores.index):
        if row_number % rebalance_bars != 0:
            continue

        score_row = scores.loc[timestamp]
        close_row = close_panel.loc[timestamp].reindex(scores.columns)
        available_mask = close_row.notna()
        adv_row = (
            avg_dollar_volume_panel.loc[timestamp].reindex(scores.columns)
            if avg_dollar_volume_panel is not None and not avg_dollar_volume_panel.empty and timestamp in avg_dollar_volume_panel.index
            else pd.Series(float("nan"), index=scores.columns)
        )
        liquidity_excluded = pd.Series(False, index=scores.columns, dtype=bool)
        if min_avg_dollar_volume is not None:
            liquidity_excluded = available_mask & (adv_row.isna() | (adv_row < min_avg_dollar_volume))

        eligible_mask = available_mask & score_row.notna() & ~liquidity_excluded
        row_scores = score_row[eligible_mask].sort_values(ascending=False)
        eligible_count = int(len(row_scores))
        selected: list[str] = []
        selection_diagnostics = {
            "buffer_blocked_replacements": 0,
            "sector_cap_excluded_count": 0,
            "excluded_by_sector": [],
        }
        if eligible_count >= min_eligible_symbols:
            selected, selection_diagnostics = _select_symbols_for_rebalance(
                row_scores=row_scores,
                previous_weights=previous_weights,
                top_n=top_n,
                turnover_buffer_bps=turnover_buffer_bps,
                sector_map=sector_map if sector_cap_active else None,
                max_names_per_sector=max_names_per_sector if sector_cap_active else None,
            )

        excluded_reasons: dict[str, str] = {}
        for symbol in scores.columns:
            if symbol in selected:
                excluded_reasons[symbol] = "selected"
            elif not bool(available_mask.get(symbol, False)):
                excluded_reasons[symbol] = "no_price"
            elif bool(liquidity_excluded.get(symbol, False)):
                excluded_reasons[symbol] = "liquidity_filter"
            elif pd.isna(score_row.get(symbol)):
                excluded_reasons[symbol] = "insufficient_history"
            elif eligible_count < min_eligible_symbols:
                excluded_reasons[symbol] = "below_min_eligible"
            elif symbol in selection_diagnostics["excluded_by_sector"]:
                excluded_reasons[symbol] = "sector_cap"
            else:
                excluded_reasons[symbol] = "not_top_n"

        ideal_weights = _compute_weight_row(
            timestamp=timestamp,
            selected=selected,
            asset_returns=asset_returns,
            weighting_scheme=weighting_scheme,
            vol_lookback_bars=vol_lookback_bars,
        )
        ideal_weights = _apply_max_position_weight(
            ideal_weights,
            max_position_weight=max_position_weight,
        )
        weight_row, turnover_cap_bound = _apply_turnover_cap(
            previous_weights,
            ideal_weights,
            max_turnover_per_rebalance=max_turnover_per_rebalance,
        )
        previous_weights = weight_row.copy()

        selection_row = {symbol: 1.0 if weight_row.loc[symbol] > POSITION_EPSILON else 0.0 for symbol in scores.columns}
        selection_rows.append({"timestamp": timestamp, **selection_row})
        weight_row.name = timestamp
        weight_rows.append(weight_row)
        diagnostics_rows.append(
            {
                "timestamp": timestamp,
                "valid_score_count": int(row_scores.notna().sum()),
                "available_symbol_count": int(available_mask.sum()),
                "eligible_symbol_count": eligible_count,
                "selected_symbol_count": int((weight_row > POSITION_EPSILON).sum()),
                "selected_symbols": ",".join(weight_row[weight_row > POSITION_EPSILON].index.tolist()),
                "selected_weights": ",".join(
                    f"{symbol}:{weight_row.loc[symbol]:.6f}"
                    for symbol in weight_row[weight_row > POSITION_EPSILON].index.tolist()
                ),
                "excluded_reasons": ";".join(f"{symbol}:{reason}" for symbol, reason in excluded_reasons.items()),
                "weight_sum": float(weight_row.sum()),
                "empty_selection": bool(len(selected) == 0),
                "liquidity_filter_active": liquidity_filter_active,
                "sector_cap_active": sector_cap_active,
                "sector_warning": sector_warning or "",
                "liquidity_excluded_count": int(liquidity_excluded.sum()) if liquidity_filter_active else 0,
                "sector_cap_excluded_count": int(selection_diagnostics["sector_cap_excluded_count"]),
                "buffer_blocked_replacements": int(selection_diagnostics["buffer_blocked_replacements"]),
                "turnover_cap_bound": bool(turnover_cap_bound),
                "weighting_scheme": weighting_scheme,
                "max_position_weight": max_position_weight,
                "min_avg_dollar_volume": min_avg_dollar_volume,
                "max_names_per_sector": max_names_per_sector,
                "turnover_buffer_bps": turnover_buffer_bps,
                "max_turnover_per_rebalance": max_turnover_per_rebalance,
            }
        )

    if not weight_rows:
        empty = pd.DataFrame(0.0, index=scores.index, columns=scores.columns)
        diagnostics = pd.DataFrame(
            [
                {
                    "timestamp": timestamp,
                    "valid_score_count": 0,
                    "available_symbol_count": 0,
                    "eligible_symbol_count": 0,
                    "selected_symbol_count": 0,
                    "selected_symbols": "",
                    "selected_weights": "",
                    "excluded_reasons": "",
                    "weight_sum": 0.0,
                    "empty_selection": True,
                    "liquidity_filter_active": liquidity_filter_active,
                    "sector_cap_active": sector_cap_active,
                    "sector_warning": sector_warning or "",
                    "liquidity_excluded_count": 0,
                    "sector_cap_excluded_count": 0,
                    "buffer_blocked_replacements": 0,
                    "turnover_cap_bound": False,
                    "weighting_scheme": weighting_scheme,
                    "max_position_weight": max_position_weight,
                    "min_avg_dollar_volume": min_avg_dollar_volume,
                    "max_names_per_sector": max_names_per_sector,
                    "turnover_buffer_bps": turnover_buffer_bps,
                    "max_turnover_per_rebalance": max_turnover_per_rebalance,
                }
                for timestamp in scores.index
            ]
        ).set_index("timestamp")
        return empty.copy(), empty.copy(), diagnostics

    rebalance_weights = pd.DataFrame(weight_rows).sort_index()
    rebalance_selection = (
        pd.DataFrame(selection_rows)
        .set_index("timestamp")
        .reindex(rebalance_weights.index)
        .fillna(0.0)
    )
    target_weights = rebalance_weights.reindex(scores.index).ffill().fillna(0.0)
    selection = rebalance_selection.reindex(scores.index).ffill().fillna(0.0)
    diagnostics = pd.DataFrame(diagnostics_rows).set_index("timestamp").sort_index()
    return selection, target_weights, diagnostics


def weight_sum_profile(target_weights: pd.DataFrame) -> pd.Series:
    return target_weights.fillna(0.0).sum(axis=1)


def compute_xsec_benchmark_returns(
    asset_returns: pd.DataFrame,
    *,
    benchmark_type: str,
) -> pd.Series:
    if benchmark_type == "equal_weight":
        return equal_weight_buy_and_hold_returns(asset_returns)
    raise ValueError(f"Unsupported xsec benchmark type: {benchmark_type}")


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
    timeseries: pd.DataFrame,
    positions: pd.DataFrame,
    target_weights: pd.DataFrame,
    rebalance_diagnostics: pd.DataFrame,
    summary: dict[str, object],
    strategy: str,
    lookback_bars: int,
    skip_bars: int,
    top_n: int,
    rebalance_bars: int,
    cost_per_turnover: float,
    vol_lookback_bars: int,
    benchmark_type: str,
) -> dict[str, object]:
    previous_positions = positions.shift(1, fill_value=0.0)
    entry_count = int(((positions > POSITION_EPSILON) & (previous_positions <= POSITION_EPSILON)).sum().sum())
    exit_count = int(((positions <= POSITION_EPSILON) & (previous_positions > POSITION_EPSILON)).sum().sum())
    trade_count = entry_count + exit_count
    percent_time_in_market = float((target_weights.abs().sum(axis=1) > POSITION_EPSILON).mean() * 100.0)
    final_position_size = float(target_weights.abs().sum(axis=1).iloc[-1])
    rebalance_count = int((timeseries["turnover"].fillna(0.0) > POSITION_EPSILON).sum())
    weight_sums = weight_sum_profile(target_weights)
    gross_summary = summarize_equity_curve(
        returns=timeseries["portfolio_return_gross"],
        equity=(summary.get("portfolio_initial_equity", 1.0) or 1.0) * (1.0 + timeseries["portfolio_return_gross"].fillna(0.0)).cumprod(),
        prefix="gross_",
    )
    mean_turnover = float(timeseries["turnover"].fillna(0.0).mean())
    mean_transaction_cost = float(timeseries["transaction_cost"].fillna(0.0).mean())
    total_transaction_cost = float(timeseries["transaction_cost"].fillna(0.0).sum())
    available_symbols = rebalance_diagnostics["available_symbol_count"] if "available_symbol_count" in rebalance_diagnostics.columns else pd.Series(dtype=float)
    eligible_symbols = rebalance_diagnostics["eligible_symbol_count"] if "eligible_symbol_count" in rebalance_diagnostics.columns else pd.Series(dtype=float)
    liquidity_excluded = rebalance_diagnostics["liquidity_excluded_count"] if "liquidity_excluded_count" in rebalance_diagnostics.columns else pd.Series(dtype=float)
    sector_excluded = rebalance_diagnostics["sector_cap_excluded_count"] if "sector_cap_excluded_count" in rebalance_diagnostics.columns else pd.Series(dtype=float)
    buffer_blocked = rebalance_diagnostics["buffer_blocked_replacements"] if "buffer_blocked_replacements" in rebalance_diagnostics.columns else pd.Series(dtype=float)
    turnover_cap_bound = rebalance_diagnostics["turnover_cap_bound"] if "turnover_cap_bound" in rebalance_diagnostics.columns else pd.Series(dtype=bool)
    current_weighting_scheme = (
        rebalance_diagnostics["weighting_scheme"].iloc[0]
        if "weighting_scheme" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty
        else "equal"
    )

    diagnostics: dict[str, object] = {
        "strategy": strategy,
        "lookback_bars": lookback_bars,
        "skip_bars": skip_bars,
        "top_n": top_n,
        "rebalance_bars": rebalance_bars,
        "cost_per_turnover": cost_per_turnover,
        "benchmark_type": benchmark_type,
        "weighting_scheme": current_weighting_scheme,
        "max_position_weight": rebalance_diagnostics["max_position_weight"].iloc[0] if "max_position_weight" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else None,
        "min_avg_dollar_volume": rebalance_diagnostics["min_avg_dollar_volume"].iloc[0] if "min_avg_dollar_volume" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else None,
        "max_names_per_sector": rebalance_diagnostics["max_names_per_sector"].iloc[0] if "max_names_per_sector" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else None,
        "turnover_buffer_bps": rebalance_diagnostics["turnover_buffer_bps"].iloc[0] if "turnover_buffer_bps" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else 0.0,
        "turnover_buffer_score_gap": _score_gap_from_bps(float(rebalance_diagnostics["turnover_buffer_bps"].iloc[0])) if "turnover_buffer_bps" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else 0.0,
        "max_turnover_per_rebalance": rebalance_diagnostics["max_turnover_per_rebalance"].iloc[0] if "max_turnover_per_rebalance" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else None,
        "vol_lookback_bars": vol_lookback_bars if current_weighting_scheme == "inv_vol" else None,
        "sector_cap_active": bool(rebalance_diagnostics["sector_cap_active"].iloc[0]) if "sector_cap_active" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else False,
        "liquidity_filter_active": bool(rebalance_diagnostics["liquidity_filter_active"].iloc[0]) if "liquidity_filter_active" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else False,
        "sector_warning": rebalance_diagnostics["sector_warning"].iloc[0] if "sector_warning" in rebalance_diagnostics.columns and not rebalance_diagnostics.empty else "",
        "Return [%]": summary.get("portfolio_total_return", float("nan")) * 100.0,
        "gross_return_pct": gross_summary.get("gross_total_return", float("nan")) * 100.0,
        "net_return_pct": summary.get("portfolio_total_return", float("nan")) * 100.0,
        "cost_drag_return_pct": (gross_summary.get("gross_total_return", float("nan")) - summary.get("portfolio_total_return", float("nan"))) * 100.0,
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
        "mean_turnover": mean_turnover,
        "annualized_turnover": mean_turnover * 252.0,
        "mean_transaction_cost": mean_transaction_cost,
        "total_transaction_cost": total_transaction_cost,
        "estimated_cost_drag_bps": (gross_summary.get("gross_total_return", float("nan")) - summary.get("portfolio_total_return", float("nan"))) * 10_000.0,
        "percent_invested": float(target_weights.abs().sum(axis=1).mean() * 100.0),
        "average_gross_exposure": float(target_weights.abs().sum(axis=1).mean()),
        "initial_equity": summary.get("portfolio_initial_equity"),
        "final_equity": summary.get("portfolio_final_equity"),
        "rebalance_weight_sum_min": float(weight_sums.min()) if not weight_sums.empty else float("nan"),
        "rebalance_weight_sum_max": float(weight_sums.max()) if not weight_sums.empty else float("nan"),
        "min_available_symbols": float(available_symbols.min()) if not available_symbols.empty else float("nan"),
        "average_available_symbols": float(available_symbols.mean()) if not available_symbols.empty else float("nan"),
        "max_available_symbols": float(available_symbols.max()) if not available_symbols.empty else float("nan"),
        "average_valid_scores": float(rebalance_diagnostics["valid_score_count"].mean()) if not rebalance_diagnostics.empty else float("nan"),
        "min_eligible_symbols": float(eligible_symbols.min()) if not eligible_symbols.empty else float("nan"),
        "average_eligible_symbols": float(rebalance_diagnostics["eligible_symbol_count"].mean()) if not rebalance_diagnostics.empty else float("nan"),
        "max_eligible_symbols": float(eligible_symbols.max()) if not eligible_symbols.empty else float("nan"),
        "average_selected_symbols": float(rebalance_diagnostics["selected_symbol_count"].mean()) if not rebalance_diagnostics.empty else float("nan"),
        "percent_empty_rebalances": float(rebalance_diagnostics["empty_selection"].astype(float).mean() * 100.0) if not rebalance_diagnostics.empty else float("nan"),
        "average_liquidity_excluded_symbols": float(liquidity_excluded.mean()) if not liquidity_excluded.empty else 0.0,
        "total_liquidity_excluded_symbols": int(liquidity_excluded.sum()) if not liquidity_excluded.empty else 0,
        "average_sector_cap_excluded_symbols": float(sector_excluded.mean()) if not sector_excluded.empty else 0.0,
        "total_sector_cap_excluded_symbols": int(sector_excluded.sum()) if not sector_excluded.empty else 0,
        "turnover_cap_binding_count": int(turnover_cap_bound.fillna(False).astype(bool).sum()) if not turnover_cap_bound.empty else 0,
        "turnover_buffer_blocked_replacements": int(buffer_blocked.sum()) if not buffer_blocked.empty else 0,
    }
    diagnostics["activity_profile"] = classify_activity_profile(diagnostics)
    diagnostics["activity_note"] = activity_note(diagnostics)
    return diagnostics


def summarize_xsec_timeseries(timeseries: pd.DataFrame) -> dict[str, float]:
    summary: dict[str, float] = {}
    summary.update(
        summarize_equity_curve(
            returns=timeseries["portfolio_return_net"],
            equity=timeseries["portfolio_equity"],
            prefix="portfolio_",
        )
    )
    summary.update(
        summarize_equity_curve(
            returns=timeseries["benchmark_return"],
            equity=timeseries["benchmark_equity"],
            prefix="benchmark_",
        )
    )
    summary["excess_total_return"] = summary["portfolio_total_return"] - summary["benchmark_total_return"]
    return summary


def run_xsec_momentum_topn(
    *,
    prepared_frames: dict[str, dict[str, object]],
    lookback_bars: int,
    skip_bars: int,
    top_n: int,
    rebalance_bars: int,
    commission: float,
    cash: float,
    max_position_weight: float | None = None,
    min_avg_dollar_volume: float | None = None,
    max_names_per_sector: int | None = None,
    turnover_buffer_bps: float = 0.0,
    max_turnover_per_rebalance: float | None = None,
    weighting_scheme: str = "equal",
    vol_lookback_bars: int = 20,
    benchmark_type: str = "equal_weight",
    active_start: str | pd.Timestamp | None = None,
    active_end: str | pd.Timestamp | None = None,
) -> XsecMomentumResult:
    close_panel, _ = build_close_panel(prepared_frames)
    asset_returns = close_panel.pct_change(fill_method=None)
    volume_panel = build_volume_panel(prepared_frames)
    avg_dollar_volume_panel = compute_avg_dollar_volume_panel(
        close_panel,
        volume_panel,
    )
    sector_map, sector_warning = load_sector_map(list(close_panel.columns)) if max_names_per_sector is not None else (None, None)
    scores = compute_xsec_momentum_scores(
        close_panel,
        lookback_bars=lookback_bars,
        skip_bars=skip_bars,
    )
    positions, target_weights, rebalance_diagnostics = build_xsec_topn_weights(
        scores,
        close_panel=close_panel,
        asset_returns=asset_returns,
        avg_dollar_volume_panel=avg_dollar_volume_panel,
        top_n=top_n,
        rebalance_bars=rebalance_bars,
        max_position_weight=max_position_weight,
        min_avg_dollar_volume=min_avg_dollar_volume,
        max_names_per_sector=max_names_per_sector,
        turnover_buffer_bps=turnover_buffer_bps,
        max_turnover_per_rebalance=max_turnover_per_rebalance,
        weighting_scheme=weighting_scheme,
        vol_lookback_bars=vol_lookback_bars,
        sector_map=sector_map,
        sector_warning=sector_warning,
    )
    simulation = simulate_target_weight_portfolio(
        asset_returns=asset_returns.fillna(0.0),
        target_weights=target_weights,
        cost_per_turnover=commission,
        initial_equity=cash,
    )
    benchmark_return = compute_xsec_benchmark_returns(
        asset_returns,
        benchmark_type=benchmark_type,
    )
    benchmark_equity = cash * (1.0 + benchmark_return.fillna(0.0)).cumprod()
    simulation.timeseries["benchmark_return"] = benchmark_return
    simulation.timeseries["benchmark_equity"] = benchmark_equity
    rebalance_diagnostics = rebalance_diagnostics.join(
        simulation.timeseries[["turnover", "transaction_cost", "portfolio_return_gross", "portfolio_return_net"]],
        how="left",
    )
    active_timeseries = simulation.timeseries.copy()
    active_positions = positions.copy()
    active_weights = target_weights.copy()
    active_scores = scores.copy()
    active_asset_returns = asset_returns.copy()
    active_rebalance_diagnostics = rebalance_diagnostics.copy()

    if active_start is not None:
        active_start_ts = pd.Timestamp(active_start)
        active_mask = active_timeseries.index >= active_start_ts
        active_timeseries = active_timeseries.loc[active_mask].copy()
        active_positions = active_positions.loc[active_mask].copy()
        active_weights = active_weights.loc[active_mask].copy()
        active_scores = active_scores.loc[active_mask].copy()
        active_asset_returns = active_asset_returns.loc[active_mask].copy()
        active_rebalance_diagnostics = active_rebalance_diagnostics.loc[active_rebalance_diagnostics.index >= active_start_ts].copy()
    if active_end is not None:
        active_end_ts = pd.Timestamp(active_end)
        active_mask = active_timeseries.index <= active_end_ts
        active_timeseries = active_timeseries.loc[active_mask].copy()
        active_positions = active_positions.loc[active_mask].copy()
        active_weights = active_weights.loc[active_mask].copy()
        active_scores = active_scores.loc[active_mask].copy()
        active_asset_returns = active_asset_returns.loc[active_mask].copy()
        active_rebalance_diagnostics = active_rebalance_diagnostics.loc[active_rebalance_diagnostics.index <= active_end_ts].copy()

    active_summary = summarize_xsec_timeseries(active_timeseries)
    symbol_start_dates = build_symbol_start_dates(prepared_frames)
    summary = summarize_xsec_result(
        timeseries=active_timeseries,
        positions=active_positions,
        target_weights=active_weights,
        rebalance_diagnostics=active_rebalance_diagnostics,
        summary=active_summary,
        strategy="xsec_momentum_topn",
        lookback_bars=lookback_bars,
        skip_bars=skip_bars,
        top_n=top_n,
        rebalance_bars=rebalance_bars,
        cost_per_turnover=commission,
        vol_lookback_bars=vol_lookback_bars,
        benchmark_type=benchmark_type,
    )
    summary["earliest_data_date_by_symbol"] = json.dumps(symbol_start_dates, sort_keys=True)
    return XsecMomentumResult(
        scores=active_scores,
        asset_returns=active_asset_returns.fillna(0.0),
        target_weights=active_weights,
        positions=active_positions,
        timeseries=active_timeseries,
        rebalance_diagnostics=active_rebalance_diagnostics,
        summary=summary,
    )
