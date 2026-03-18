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
    ann_return = annualized_return(returns)
    ann_vol = annualized_volatility(returns)

    sharpe = (
        float(ann_return / ann_vol)
        if ann_vol and not np.isnan(ann_vol) and ann_vol > 0
        else float("nan")
    )

    name = (lambda s: f"{prefix}{s}" if prefix else s)

    return {
        name("total_return"): float(equity.iloc[-1] - 1.0) if not equity.empty else float("nan"),
        name("annual_return"): float(ann_return),
        name("annual_vol"): float(ann_vol),
        name("sharpe"): sharpe,
        name("max_drawdown"): max_drawdown(equity) if not equity.empty else float("nan"),
    }