from __future__ import annotations

import pandas as pd

from trading_platform.signals.common import make_base_signal_frame, normalize_price_frame


def _build_breakout_position(
    *,
    close: pd.Series,
    breakout_high: pd.Series,
    breakout_low: pd.Series,
    momentum_ok: pd.Series,
) -> pd.Series:
    in_position = False
    positions: list[float] = []

    for timestamp in close.index:
        price = close.loc[timestamp]
        entry_level = breakout_high.loc[timestamp]
        exit_level = breakout_low.loc[timestamp]
        allow_entry = bool(momentum_ok.loc[timestamp])

        if in_position and pd.notna(exit_level) and price < exit_level:
            in_position = False
        elif not in_position and allow_entry and pd.notna(entry_level) and price > entry_level:
            in_position = True

        positions.append(1.0 if in_position else 0.0)

    return pd.Series(positions, index=close.index, dtype=float)


def generate_signal_frame(
    df: pd.DataFrame,
    *,
    entry_lookback: int = 55,
    exit_lookback: int = 20,
    momentum_lookback: int | None = None,
    **_: object,
) -> pd.DataFrame:
    if entry_lookback <= 0:
        raise ValueError(f"entry_lookback must be positive, got {entry_lookback}")
    if exit_lookback <= 0:
        raise ValueError(f"exit_lookback must be positive, got {exit_lookback}")
    if momentum_lookback is not None and momentum_lookback <= 0:
        raise ValueError(f"momentum_lookback must be positive when provided, got {momentum_lookback}")

    working = normalize_price_frame(df)
    out = make_base_signal_frame(working)

    close = working["close"]
    breakout_high = close.shift(1).rolling(entry_lookback).max()
    breakout_low = close.shift(1).rolling(exit_lookback).min()
    trailing_return = (
        close / close.shift(momentum_lookback) - 1.0
        if momentum_lookback is not None
        else pd.Series(1.0, index=close.index)
    )
    momentum_ok = trailing_return > 0.0 if momentum_lookback is not None else pd.Series(True, index=close.index)

    out["breakout_high"] = breakout_high
    out["breakout_low"] = breakout_low
    out["trailing_return"] = trailing_return
    out["score"] = close / breakout_high - 1.0
    out["position"] = _build_breakout_position(
        close=close,
        breakout_high=breakout_high,
        breakout_low=breakout_low,
        momentum_ok=momentum_ok.fillna(False),
    ).fillna(0.0)

    return out[["close", "asset_return", "score", "position"]]
