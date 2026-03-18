from __future__ import annotations

from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.services.job_artifact_service import build_job_summary


def test_build_job_summary_includes_results_errors_and_aggregate_summary() -> None:
    config = ResearchWorkflowConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast=20,
        slow=50,
    )

    outputs = {
        "results": {
            "AAPL": {
                "normalized_path": "/tmp/normalized/AAPL.parquet",
                "features_path": "/tmp/features/AAPL.parquet",
                "experiment_id": "exp-aapl",
                "stats": {
                    "Return [%]": 12.5,
                    "Sharpe Ratio": 1.3,
                    "Max. Drawdown [%]": -8.2,
                },
            }
        },
        "errors": {
            "MSFT": "ValueError: boom",
        },
    }

    summary = build_job_summary(
        config=config,
        symbols=["AAPL", "MSFT"],
        outputs=outputs,
        leaderboard_csv_path="artifacts/jobs/job_test.leaderboard.csv",
    )

    assert summary["summary"]["requested_count"] == 2
    assert summary["summary"]["success_count"] == 1
    assert summary["summary"]["failure_count"] == 1
    assert summary["results"]["AAPL"]["experiment_id"] == "exp-aapl"
    assert summary["errors"]["MSFT"] == "ValueError: boom"
    assert summary["leaderboard_csv_path"] == "artifacts/jobs/job_test.leaderboard.csv"
    assert summary["aggregate_summary"]["success_count"] == 1
    assert summary["aggregate_summary"]["failure_count"] == 1
    assert summary["aggregate_summary"]["best_symbol_by_return"] == "AAPL"