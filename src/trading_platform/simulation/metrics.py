from __future__ import annotations

import math

import numpy as np
import pandas as pd


def max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    return float(drawdown.min())


def annualized_return(returns: pd.Series, periods_per_year: int = 252) -> float:
    clean = returns.dropna()
    if clean.empty:
        return float("nan")

    growth = float((1.0 + clean).prod())
    n = len(clean)

    if n == 0 or growth <= 0:
        return float("nan")

    return growth ** (periods_per_year / n) - 1.0


def annualized_volatility(returns: pd.Series, periods_per_year: int = 252) -> float:
    clean = returns.dropna()
    if clean.empty:
        return float("nan")

    return float(clean.std(ddof=0) * math.sqrt(periods_per_year))


def summarize_equity_curve(
    returns: pd.Series,
    equity: pd.Series,
    *,
    prefix: str = "",
) -> dict[str, float]:
    clean_returns = returns.dropna()
    ann_return = annualized_return(returns)
    ann_vol = annualized_volatility(returns)

    sharpe = (
        float(ann_return / ann_vol)
        if ann_vol and not np.isnan(ann_vol) and ann_vol > 0
        else float("nan")
    )

    name = (lambda s: f"{prefix}{s}" if prefix else s)
    total_return = float((1.0 + clean_returns).prod() - 1.0) if not clean_returns.empty else float("nan")
    initial_equity = float(equity.iloc[0] / (1.0 + returns.iloc[0])) if not equity.empty and not returns.empty and pd.notna(returns.iloc[0]) else float("nan")
    final_equity = float(equity.iloc[-1]) if not equity.empty else float("nan")

    return {
        name("total_return"): total_return,
        name("annual_return"): float(ann_return),
        name("annual_vol"): float(ann_vol),
        name("sharpe"): sharpe,
        name("max_drawdown"): max_drawdown(equity) if not equity.empty else float("nan"),
        name("initial_equity"): initial_equity,
        name("final_equity"): final_equity,
    }
