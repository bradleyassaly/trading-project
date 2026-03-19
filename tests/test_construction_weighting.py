from __future__ import annotations

import pandas as pd

from trading_platform.construction.weighting import build_top_n_portfolio_weights


def test_build_top_n_portfolio_weights_selects_top_names() -> None:
    scores_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31", "2024-01-31"],
            "symbol": ["AAPL", "MSFT", "NVDA"],
            "score": [0.10, 0.30, 0.20],
        }
    )

    result = build_top_n_portfolio_weights(scores_df=scores_df, top_n=2)

    assert list(result["symbol"]) == ["MSFT", "NVDA"]
    assert result["weight"].sum() == 1.0


def test_build_top_n_portfolio_weights_normalizes_after_cap() -> None:
    scores_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31", "2024-01-31"],
            "symbol": ["AAPL", "MSFT", "NVDA"],
            "score": [3.0, 2.0, 1.0],
        }
    )

    result = build_top_n_portfolio_weights(
        scores_df=scores_df,
        top_n=3,
        max_weight=0.20,
    )

    assert len(result) == 3
    assert round(float(result["weight"].sum()), 10) == 1.0
    assert result["weight"].max() <= (1.0 / 3.0) + 1e-12


def test_build_top_n_portfolio_weights_multiple_timestamps() -> None:
    scores_df = pd.DataFrame(
        {
            "timestamp": [
                "2024-01-31", "2024-01-31",
                "2024-02-29", "2024-02-29",
            ],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "score": [0.2, 0.1, 0.3, 0.4],
        }
    )

    result = build_top_n_portfolio_weights(scores_df=scores_df, top_n=1)

    assert len(result) == 2
    assert list(result["symbol"]) == ["AAPL", "MSFT"]