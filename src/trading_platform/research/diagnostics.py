from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd


POSITION_EPSILON = 1e-12
LOW_EXPOSURE_THRESHOLD_PCT = 20.0


def classify_activity_profile(diagnostics: dict[str, object]) -> str:
    trade_count_raw = diagnostics.get("trade_count")
    percent_time_in_market_raw = diagnostics.get("percent_time_in_market")

    trade_count = None
    if trade_count_raw is not None and not pd.isna(trade_count_raw):
        trade_count = float(trade_count_raw)

    percent_time_in_market = None
    if percent_time_in_market_raw is not None and not pd.isna(percent_time_in_market_raw):
        percent_time_in_market = float(percent_time_in_market_raw)

    if trade_count is not None and abs(trade_count) <= POSITION_EPSILON:
        return "no_trades"
    if percent_time_in_market is not None and float(percent_time_in_market) < LOW_EXPOSURE_THRESHOLD_PCT:
        return "low_exposure"
    if trade_count is None:
        return "unknown"
    return "active"


def _average_holding_period(lengths: Iterable[int]) -> float | None:
    cleaned = [int(length) for length in lengths if int(length) > 0]
    if not cleaned:
        return None
    return float(sum(cleaned) / len(cleaned))


def diagnostics_from_timeseries(timeseries: pd.DataFrame) -> dict[str, object]:
    if timeseries.empty or "effective_position" not in timeseries.columns:
        diagnostics = {
            "trade_count": None,
            "entry_count": None,
            "exit_count": None,
            "percent_time_in_market": None,
            "average_holding_period_bars": None,
            "final_position_size": None,
            "ended_in_cash": None,
        }
        diagnostics["activity_profile"] = "unknown"
        return diagnostics

    position = timeseries["effective_position"].fillna(0.0).astype(float)
    in_market = position.abs() > POSITION_EPSILON
    prev_in_market = in_market.shift(1, fill_value=False)

    entry_count = int((in_market & ~prev_in_market).sum())
    exit_count = int((~in_market & prev_in_market).sum())
    trade_count = int(entry_count + exit_count)
    percent_time_in_market = float(in_market.mean() * 100.0)

    holding_lengths: list[int] = []
    current_length = 0
    for invested in in_market.tolist():
        if invested:
            current_length += 1
            continue
        if current_length > 0:
            holding_lengths.append(current_length)
            current_length = 0
    if current_length > 0:
        holding_lengths.append(current_length)

    final_position_size = float(position.iloc[-1])
    diagnostics = {
        "trade_count": trade_count,
        "entry_count": entry_count,
        "exit_count": exit_count,
        "percent_time_in_market": percent_time_in_market,
        "average_holding_period_bars": _average_holding_period(holding_lengths),
        "final_position_size": final_position_size,
        "ended_in_cash": bool(abs(final_position_size) <= POSITION_EPSILON),
    }
    diagnostics["activity_profile"] = classify_activity_profile(diagnostics)
    return diagnostics


def diagnostics_from_legacy_stats(stats: dict[str, Any]) -> dict[str, object]:
    trades = stats.get("_trades")
    equity_curve = stats.get("_equity_curve")
    total_bars = None
    if isinstance(equity_curve, pd.DataFrame):
        total_bars = int(len(equity_curve))

    if not isinstance(trades, pd.DataFrame) or trades.empty:
        diagnostics = {
            "trade_count": 0,
            "entry_count": 0,
            "exit_count": 0,
            "percent_time_in_market": 0.0 if total_bars else None,
            "average_holding_period_bars": None,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }
        diagnostics["activity_profile"] = classify_activity_profile(diagnostics)
        return diagnostics

    holding_lengths: list[int] = []
    invested_bars = 0
    for _, trade in trades.iterrows():
        entry_bar = int(trade.get("EntryBar", 0))
        exit_bar = int(trade.get("ExitBar", entry_bar))
        holding_bars = max(exit_bar - entry_bar, 1)
        holding_lengths.append(holding_bars)
        invested_bars += holding_bars

    percent_time_in_market = None
    if total_bars and total_bars > 0:
        percent_time_in_market = float(min(invested_bars / total_bars, 1.0) * 100.0)

    diagnostics = {
        "trade_count": int(len(trades) * 2),
        "entry_count": int(len(trades)),
        "exit_count": int(len(trades)),
        "percent_time_in_market": percent_time_in_market,
        "average_holding_period_bars": _average_holding_period(holding_lengths),
        "final_position_size": 0.0,
        "ended_in_cash": True,
    }
    diagnostics["activity_profile"] = classify_activity_profile(diagnostics)
    return diagnostics


def activity_note(diagnostics: dict[str, object]) -> str:
    profile = diagnostics.get("activity_profile") or classify_activity_profile(diagnostics)
    if profile == "no_trades":
        return "no_trades"
    if profile == "low_exposure":
        return "low_exposure"
    if profile == "unknown":
        return "unknown"
    return "active"
