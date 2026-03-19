from __future__ import annotations

import math

import pandas as pd


def _safe_corr(a: pd.Series, b: pd.Series, method: str = "pearson") -> float:
    joined = pd.concat([a, b], axis=1).dropna()
    if len(joined) < 3:
        return math.nan
    return float(joined.iloc[:, 0].corr(joined.iloc[:, 1], method=method))


def compute_hit_rate(signal: pd.Series, forward_return: pd.Series) -> float:
    joined = pd.concat([signal, forward_return], axis=1).dropna()
    if joined.empty:
        return math.nan

    signal_col = joined.iloc[:, 0]
    return_col = joined.iloc[:, 1]

    directional_correct = ((signal_col > 0) & (return_col > 0)) | (
        (signal_col < 0) & (return_col < 0)
    )
    nonzero_signal = signal_col != 0
    usable = directional_correct[nonzero_signal]

    if usable.empty:
        return math.nan

    return float(usable.mean())


def compute_turnover(signal: pd.Series) -> float:
    ranked = signal.dropna()
    if len(ranked) < 2:
        return math.nan
    return float(ranked.diff().abs().mean())


def compute_quantile_spread(
    signal: pd.Series,
    forward_return: pd.Series,
    *,
    top_quantile: float = 0.2,
    bottom_quantile: float = 0.2,
) -> float:
    joined = pd.concat([signal, forward_return], axis=1).dropna()
    if len(joined) < 3:
        return math.nan

    signal_col = joined.iloc[:, 0]
    return_col = joined.iloc[:, 1]

    top_cut = signal_col.quantile(1.0 - top_quantile)
    bottom_cut = signal_col.quantile(bottom_quantile)

    top_mean = return_col[signal_col >= top_cut].mean()
    bottom_mean = return_col[signal_col <= bottom_cut].mean()

    return float(top_mean - bottom_mean)


def evaluate_signal(
    signal: pd.Series,
    forward_return: pd.Series,
    *,
    top_quantile: float = 0.2,
    bottom_quantile: float = 0.2,
) -> dict[str, float]:
    joined = pd.concat([signal, forward_return], axis=1).dropna()
    n = len(joined)

    if n < 3:
        return {
            "n_obs": n,
            "pearson_ic": math.nan,
            "spearman_ic": math.nan,
            "hit_rate": math.nan,
            "quantile_spread": math.nan,
            "turnover": math.nan,
        }

    signal_col = joined.iloc[:, 0]
    return_col = joined.iloc[:, 1]

    return {
        "n_obs": float(n),
        "pearson_ic": _safe_corr(signal_col, return_col, method="pearson"),
        "spearman_ic": _safe_corr(signal_col, return_col, method="spearman"),
        "hit_rate": compute_hit_rate(signal_col, return_col),
        "quantile_spread": compute_quantile_spread(
            signal_col,
            return_col,
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        ),
        "turnover": compute_turnover(signal_col),
    }