from __future__ import annotations

import pandas as pd

from trading_platform.config.models import (
    PortfolioConstructionConfig,
    PortfolioConstructionResult,
)
from trading_platform.construction.weighting import build_top_n_portfolio_weights
from trading_platform.portfolio.backtest import backtest_portfolio_from_weights
from trading_platform.portfolio.metrics import compute_portfolio_summary_metrics


def run_portfolio_construction(
    scores_df: pd.DataFrame,
    forward_returns_df: pd.DataFrame,
    portfolio_config: PortfolioConstructionConfig,
) -> PortfolioConstructionResult:
    """
    Build weights from cross-sectional scores and backtest the resulting portfolio.
    """
    if portfolio_config.method != "top_n":
        raise ValueError(
            f"Unsupported portfolio construction method: {portfolio_config.method}"
        )

    weights_df = build_top_n_portfolio_weights(
        scores_df=scores_df,
        top_n=portfolio_config.top_n,
        max_weight=portfolio_config.max_weight,
    )

    portfolio_returns_df = backtest_portfolio_from_weights(
        weights_df=weights_df,
        forward_returns_df=forward_returns_df,
        transaction_cost_bps=portfolio_config.transaction_cost_bps,
    )

    summary = compute_portfolio_summary_metrics(portfolio_returns_df)

    summary.update(
        {
            "method": portfolio_config.method,
            "top_n": portfolio_config.top_n,
            "max_weight": portfolio_config.max_weight,
            "transaction_cost_bps": portfolio_config.transaction_cost_bps,
            "n_weight_rows": int(len(weights_df)),
        }
    )

    return PortfolioConstructionResult(
        weights_df=weights_df,
        portfolio_returns_df=portfolio_returns_df,
        summary=summary,
    )