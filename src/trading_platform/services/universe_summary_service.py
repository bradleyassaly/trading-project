from __future__ import annotations

from statistics import mean, median
from typing import Any

import pandas as pd


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_universe_leaderboard(outputs: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for symbol, result in outputs.get("results", {}).items():
        stats = result.get("stats", {})
        rows.append(
            {
                "symbol": symbol,
                "experiment_id": result.get("experiment_id"),
                "return_pct": _safe_float(stats.get("Return [%]")),
                "sharpe_ratio": _safe_float(stats.get("Sharpe Ratio")),
                "max_drawdown_pct": _safe_float(stats.get("Max. Drawdown [%]")),
                "normalized_path": str(result.get("normalized_path", "")),
                "features_path": str(result.get("features_path", "")),
            }
        )

    df = pd.DataFrame(rows)

    if not df.empty and "return_pct" in df.columns:
        df = df.sort_values(
            by="return_pct",
            ascending=False,
            na_position="last",
        ).reset_index(drop=True)

    return df


def build_universe_aggregate_summary(
    leaderboard: pd.DataFrame,
    error_count: int,
) -> dict[str, object]:
    if leaderboard.empty:
        return {
            "success_count": 0,
            "failure_count": int(error_count),
            "mean_return_pct": None,
            "median_return_pct": None,
            "mean_sharpe_ratio": None,
            "best_symbol_by_return": None,
            "best_symbol_by_sharpe": None,
            "worst_symbol_by_drawdown": None,
        }

    returns = [x for x in leaderboard["return_pct"].tolist() if pd.notna(x)]
    sharpes = [x for x in leaderboard["sharpe_ratio"].tolist() if pd.notna(x)]

    best_return_symbol = None
    if leaderboard["return_pct"].notna().any():
        best_return_symbol = leaderboard.loc[
            leaderboard["return_pct"].idxmax(),
            "symbol",
        ]

    best_sharpe_symbol = None
    if leaderboard["sharpe_ratio"].notna().any():
        best_sharpe_symbol = leaderboard.loc[
            leaderboard["sharpe_ratio"].idxmax(),
            "symbol",
        ]

    worst_drawdown_symbol = None
    if leaderboard["max_drawdown_pct"].notna().any():
        worst_drawdown_symbol = leaderboard.loc[
            leaderboard["max_drawdown_pct"].idxmin(),
            "symbol",
        ]

    return {
        "success_count": int(len(leaderboard)),
        "failure_count": int(error_count),
        "mean_return_pct": mean(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "mean_sharpe_ratio": mean(sharpes) if sharpes else None,
        "best_symbol_by_return": best_return_symbol,
        "best_symbol_by_sharpe": best_sharpe_symbol,
        "worst_symbol_by_drawdown": worst_drawdown_symbol,
    }