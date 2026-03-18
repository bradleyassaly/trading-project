from __future__ import annotations

from trading_platform.services.universe_summary_service import (
    build_universe_aggregate_summary,
    build_universe_leaderboard,
)


def test_build_universe_leaderboard_sorts_by_return_desc() -> None:
    outputs = {
        "results": {
            "MSFT": {
                "experiment_id": "exp-msft",
                "normalized_path": "/tmp/msft_norm.parquet",
                "features_path": "/tmp/msft_feat.parquet",
                "stats": {
                    "Return [%]": 5.0,
                    "Sharpe Ratio": 0.9,
                    "Max. Drawdown [%]": -7.0,
                },
            },
            "AAPL": {
                "experiment_id": "exp-aapl",
                "normalized_path": "/tmp/aapl_norm.parquet",
                "features_path": "/tmp/aapl_feat.parquet",
                "stats": {
                    "Return [%]": 12.0,
                    "Sharpe Ratio": 1.2,
                    "Max. Drawdown [%]": -10.0,
                },
            },
        },
        "errors": {},
    }

    df = build_universe_leaderboard(outputs)

    assert list(df["symbol"]) == ["AAPL", "MSFT"]
    assert list(df.columns) == [
        "symbol",
        "experiment_id",
        "return_pct",
        "sharpe_ratio",
        "max_drawdown_pct",
        "normalized_path",
        "features_path",
    ]


def test_build_universe_aggregate_summary_returns_metrics() -> None:
    outputs = {
        "results": {
            "AAPL": {
                "experiment_id": "exp-aapl",
                "normalized_path": "/tmp/aapl_norm.parquet",
                "features_path": "/tmp/aapl_feat.parquet",
                "stats": {
                    "Return [%]": 12.0,
                    "Sharpe Ratio": 1.2,
                    "Max. Drawdown [%]": -10.0,
                },
            },
            "MSFT": {
                "experiment_id": "exp-msft",
                "normalized_path": "/tmp/msft_norm.parquet",
                "features_path": "/tmp/msft_feat.parquet",
                "stats": {
                    "Return [%]": 6.0,
                    "Sharpe Ratio": 0.8,
                    "Max. Drawdown [%]": -5.0,
                },
            },
        },
        "errors": {
            "NVDA": "ValueError: boom",
        },
    }

    df = build_universe_leaderboard(outputs)
    summary = build_universe_aggregate_summary(df, error_count=1)

    assert summary["success_count"] == 2
    assert summary["failure_count"] == 1
    assert summary["mean_return_pct"] == 9.0
    assert summary["median_return_pct"] == 9.0
    assert summary["mean_sharpe_ratio"] == 1.0
    assert summary["best_symbol_by_return"] == "AAPL"
    assert summary["best_symbol_by_sharpe"] == "AAPL"
    assert summary["worst_symbol_by_drawdown"] == "AAPL"


def test_build_universe_aggregate_summary_handles_empty_leaderboard() -> None:
    df = build_universe_leaderboard({"results": {}, "errors": {}})
    summary = build_universe_aggregate_summary(df, error_count=2)

    assert summary["success_count"] == 0
    assert summary["failure_count"] == 2
    assert summary["mean_return_pct"] is None
    assert summary["best_symbol_by_return"] is None