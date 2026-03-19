from __future__ import annotations

import math

import pandas as pd


def compute_portfolio_summary_metrics(
    portfolio_returns_df: pd.DataFrame,
) -> dict:
    if portfolio_returns_df.empty:
        return {
            "n_periods": 0,
            "total_return": 0.0,
            "mean_net_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe_like": 0.0,
        }

    required_cols = {"timestamp", "net_return"}
    missing = required_cols - set(portfolio_returns_df.columns)
    if missing:
        raise ValueError(
            f"portfolio_returns_df missing required columns: {sorted(missing)}"
        )

    returns = pd.to_numeric(portfolio_returns_df["net_return"], errors="coerce").fillna(0.0)
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0

    sharpe_like = 0.0
    if len(returns) > 1 and returns.std(ddof=0) > 0:
        sharpe_like = float((returns.mean() / returns.std(ddof=0)) * math.sqrt(12))

    return {
        "n_periods": int(len(portfolio_returns_df)),
        "total_return": float(equity.iloc[-1] - 1.0),
        "mean_net_return": float(returns.mean()),
        "max_drawdown": float(drawdown.min()),
        "sharpe_like": sharpe_like,
    }