from __future__ import annotations

import pandas as pd

from trading_platform.config.models import PortfolioConstructionConfig
from trading_platform.services.portfolio_service import run_portfolio_construction


def test_run_portfolio_construction_builds_weights_and_returns() -> None:
    scores_df = pd.DataFrame(
        {
            "timestamp": [
                "2024-01-31", "2024-01-31", "2024-01-31",
                "2024-02-29", "2024-02-29", "2024-02-29",
            ],
            "symbol": ["AAPL", "MSFT", "NVDA", "AAPL", "MSFT", "NVDA"],
            "score": [0.10, 0.30, 0.20, 0.40, 0.10, 0.50],
        }
    )
    forward_returns_df = pd.DataFrame(
        {
            "timestamp": [
                "2024-01-31", "2024-01-31",
                "2024-02-29", "2024-02-29",
            ],
            "symbol": ["MSFT", "NVDA", "NVDA", "AAPL"],
            "forward_return": [0.02, 0.01, 0.03, 0.01],
        }
    )

    config = PortfolioConstructionConfig(
        method="top_n",
        top_n=2,
        max_weight=0.60,
        transaction_cost_bps=0.0,
    )

    result = run_portfolio_construction(
        scores_df=scores_df,
        forward_returns_df=forward_returns_df,
        portfolio_config=config,
    )

    assert not result.weights_df.empty
    assert not result.portfolio_returns_df.empty
    assert result.summary["method"] == "top_n"
    assert result.summary["top_n"] == 2