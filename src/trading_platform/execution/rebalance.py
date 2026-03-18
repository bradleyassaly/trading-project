from __future__ import annotations

import pandas as pd


def daily_rebalance_mask(index: pd.Index) -> pd.Series:
    idx = pd.DatetimeIndex(index)
    return pd.Series(True, index=idx)


def weekly_rebalance_mask(index: pd.Index) -> pd.Series:
    idx = pd.DatetimeIndex(index)
    iso = idx.isocalendar()
    combined = pd.Series(
        iso.year.astype(str) + "-" + iso.week.astype(str),
        index=idx,
    )
    return ~combined.duplicated()


def monthly_rebalance_mask(index: pd.Index) -> pd.Series:
    idx = pd.DatetimeIndex(index)
    periods = pd.Series(idx.to_period("M").astype(str), index=idx)
    return ~periods.duplicated()


def build_rebalance_mask(index: pd.Index, frequency: str) -> pd.Series:
    if frequency == "daily":
        return daily_rebalance_mask(index)
    if frequency == "weekly":
        return weekly_rebalance_mask(index)
    if frequency == "monthly":
        return monthly_rebalance_mask(index)
    raise ValueError(f"Unsupported rebalance frequency: {frequency}")