from __future__ import annotations

import math

import pandas as pd


def _cross_section_quantile_count(count: int, quantile: float) -> int:
    if count <= 0 or quantile <= 0.0:
        return 0
    return min(count, max(1, math.ceil(count * quantile)))


def _compute_rank_long_short_spread(
    ranked_signal: pd.Series,
    forward_return: pd.Series,
    *,
    top_quantile: float = 0.2,
    bottom_quantile: float = 0.2,
) -> float:
    joined = pd.concat([ranked_signal, forward_return], axis=1).dropna()
    if len(joined) < 2:
        return math.nan

    signal_col = joined.iloc[:, 0]
    return_col = joined.iloc[:, 1]
    n_obs = len(joined)

    top_n = _cross_section_quantile_count(n_obs, top_quantile)
    bottom_n = _cross_section_quantile_count(n_obs, bottom_quantile)
    if top_n == 0 or bottom_n == 0:
        return math.nan

    bottom_mask = signal_col <= bottom_n
    top_mask = signal_col > (n_obs - top_n)

    if not top_mask.any() or not bottom_mask.any():
        return math.nan

    top_mean = return_col[top_mask].mean()
    bottom_mean = return_col[bottom_mask].mean()
    return float(top_mean - bottom_mean)


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


def compute_cross_sectional_turnover(
    panel: pd.DataFrame,
    *,
    date_col: str = "timestamp",
    symbol_col: str = "symbol",
    signal_col: str = "signal",
) -> float:
    usable = panel[[date_col, symbol_col, signal_col]].dropna()
    if usable.empty:
        return math.nan

    ranked = usable.copy()
    ranked["rank"] = ranked.groupby(date_col)[signal_col].rank(method="average", pct=True)

    rank_matrix = ranked.pivot(index=symbol_col, columns=date_col, values="rank").sort_index(axis=1)
    if rank_matrix.shape[1] < 2:
        return math.nan

    turnover = rank_matrix.diff(axis=1).abs().stack().mean()
    return float(turnover) if pd.notna(turnover) else math.nan


def compute_cross_sectional_daily_metrics(
    panel: pd.DataFrame,
    *,
    signal_col: str = "signal",
    forward_return_col: str = "forward_return",
    date_col: str = "timestamp",
    top_quantile: float = 0.2,
    bottom_quantile: float = 0.2,
) -> pd.DataFrame:
    required_cols = [date_col, signal_col, forward_return_col]
    joined = panel[required_cols].dropna().copy()
    if joined.empty:
        return pd.DataFrame(
            columns=[
                date_col,
                "n_obs",
                "pearson_ic",
                "spearman_ic",
                "hit_rate",
                "long_short_spread",
                "quantile_spread",
            ]
        )

    daily_rows: list[dict[str, float]] = []
    for timestamp, date_df in joined.groupby(date_col):
        if len(date_df) < 2:
            continue

        ranked_signal = date_df[signal_col].rank(method="first", ascending=True)
        forward_return = date_df[forward_return_col]
        long_short_spread = _compute_rank_long_short_spread(
            ranked_signal,
            forward_return,
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        )

        daily_rows.append(
            {
                date_col: timestamp,
                "n_obs": float(len(date_df)),
                "pearson_ic": _safe_corr(ranked_signal, forward_return, method="pearson"),
                "spearman_ic": _safe_corr(ranked_signal, forward_return, method="spearman"),
                "hit_rate": compute_hit_rate(date_df[signal_col], forward_return),
                "long_short_spread": long_short_spread,
                "quantile_spread": long_short_spread,
            }
        )

    return pd.DataFrame(daily_rows)


def evaluate_cross_sectional_signal(
    panel: pd.DataFrame,
    *,
    signal_col: str = "signal",
    forward_return_col: str = "forward_return",
    date_col: str = "timestamp",
    symbol_col: str = "symbol",
    top_quantile: float = 0.2,
    bottom_quantile: float = 0.2,
) -> dict[str, float]:
    required_cols = [date_col, symbol_col, signal_col, forward_return_col]
    joined = panel[required_cols].dropna().copy()
    if joined.empty:
        return {
            "n_obs": 0.0,
            "dates_evaluated": 0.0,
            "symbols_evaluated": 0.0,
            "pearson_ic": math.nan,
            "spearman_ic": math.nan,
            "hit_rate": math.nan,
            "long_short_spread": math.nan,
            "quantile_spread": math.nan,
            "turnover": math.nan,
        }

    daily_metrics = compute_cross_sectional_daily_metrics(
        joined,
        signal_col=signal_col,
        forward_return_col=forward_return_col,
        date_col=date_col,
        top_quantile=top_quantile,
        bottom_quantile=bottom_quantile,
    )

    if daily_metrics.empty:
        n_obs = float(len(joined))
        return {
            "n_obs": n_obs,
            "dates_evaluated": 0.0,
            "symbols_evaluated": float(joined[symbol_col].nunique()),
            "pearson_ic": math.nan,
            "spearman_ic": math.nan,
            "hit_rate": math.nan,
            "long_short_spread": math.nan,
            "quantile_spread": math.nan,
            "turnover": compute_cross_sectional_turnover(
                joined,
                date_col=date_col,
                symbol_col=symbol_col,
                signal_col=signal_col,
            ),
        }

    long_short_spread = float(daily_metrics["long_short_spread"].mean())

    return {
        "n_obs": float(daily_metrics["n_obs"].sum()),
        "dates_evaluated": float(len(daily_metrics)),
        "symbols_evaluated": float(joined[symbol_col].nunique()),
        "pearson_ic": float(daily_metrics["pearson_ic"].mean()),
        "spearman_ic": float(daily_metrics["spearman_ic"].mean()),
        "hit_rate": float(daily_metrics["hit_rate"].mean()),
        "long_short_spread": long_short_spread,
        "quantile_spread": long_short_spread,
        "turnover": compute_cross_sectional_turnover(
            joined,
            date_col=date_col,
            symbol_col=symbol_col,
            signal_col=signal_col,
        ),
    }


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
