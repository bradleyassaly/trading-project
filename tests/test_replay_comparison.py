from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.dataset_registry import ResearchDatasetRegistryEntry, upsert_dataset_registry_entry
from trading_platform.research.replay_comparison import (
    ReplayComparisonRequest,
    run_replay_comparison,
    write_replay_comparison_artifacts,
)
from trading_platform.research.replay_evaluation import (
    build_replay_evaluation_request,
    run_replay_evaluation,
    write_replay_evaluation_artifacts,
)


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _seed_registry(tmp_path: Path) -> Path:
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"
    binance_path = tmp_path / "data" / "research" / "binance_features.parquet"
    kalshi_path = tmp_path / "data" / "research" / "kalshi_features.parquet"
    _write_parquet(
        binance_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m", "1m", "1m"],
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01T00:00:00Z",
                        "2024-01-01T00:01:00Z",
                        "2024-01-01T00:02:00Z",
                        "2024-01-01T00:03:00Z",
                    ],
                    utc=True,
                ),
                "feature_time": pd.to_datetime(
                    [
                        "2024-01-01T00:00:30Z",
                        "2024-01-01T00:01:30Z",
                        "2024-01-01T00:02:30Z",
                        "2024-01-01T00:03:30Z",
                    ],
                    utc=True,
                ),
                "feature_a": [1.0, 2.0, 3.0, 4.0],
                "target_return_1": [0.1, 0.2, 0.3, 0.4],
            }
        ),
    )
    _write_parquet(
        kalshi_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m", "1m", "1m"],
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01T00:00:00Z",
                        "2024-01-01T00:01:00Z",
                        "2024-01-01T00:02:00Z",
                        "2024-01-01T00:03:00Z",
                    ],
                    utc=True,
                ),
                "feature_b": [1.0, 1.0, -1.0, -1.0],
                "target_return_1": [0.2, -0.2, 0.2, -0.2],
            }
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="binance.crypto_market_features",
            provider="binance",
            asset_class="crypto",
            dataset_name="crypto_market_features",
            dataset_path=str(binance_path),
            symbols=["BTCUSDT"],
            intervals=["1m"],
            latest_event_time="2024-01-01T00:03:30Z",
            latest_materialized_at="2024-01-01T00:04:00Z",
            metadata={"time_semantics": {"feature_time": "feature time"}},
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(kalshi_path),
            symbols=["BTCUSDT"],
            intervals=["1m"],
            latest_event_time="2024-01-01T00:03:00Z",
            latest_materialized_at="2024-01-01T00:04:30Z",
            metadata={"time_semantics": {"timestamp": "market time"}},
        ),
    )
    return registry_path


def test_replay_comparison_ranks_provider_slices_from_on_demand_evaluations(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)

    result = run_replay_comparison(
        ReplayComparisonRequest(
            registry_path=registry_path,
            providers=["binance", "kalshi"],
            comparison_mode="provider",
            min_row_count=2,
            max_candidates=5,
        )
    )

    assert result.rankings
    assert result.candidate_slices
    assert result.candidate_slices[0].providers == ["binance"]
    assert result.candidate_slices[0].target_column == "binance__crypto_market_features__target_return_1"


def test_replay_comparison_reads_existing_evaluation_summaries_and_writes_artifacts(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    binance_result = run_replay_evaluation(
        build_replay_evaluation_request(
            registry_path=registry_path,
            providers=["binance"],
            evaluation_name="binance_eval",
        )
    )
    kalshi_result = run_replay_evaluation(
        build_replay_evaluation_request(
            registry_path=registry_path,
            providers=["kalshi"],
            evaluation_name="kalshi_eval",
        )
    )
    binance_paths = write_replay_evaluation_artifacts(
        result=binance_result,
        output_dir=tmp_path / "artifacts" / "eval" / "binance",
    )
    kalshi_paths = write_replay_evaluation_artifacts(
        result=kalshi_result,
        output_dir=tmp_path / "artifacts" / "eval" / "kalshi",
    )

    comparison = run_replay_comparison(
        ReplayComparisonRequest(
            evaluation_summary_paths=[
                binance_paths["summary_path"],
                kalshi_paths["summary_path"],
            ],
            min_row_count=2,
            max_candidates=3,
        )
    )
    paths = write_replay_comparison_artifacts(
        result=comparison,
        output_dir=tmp_path / "artifacts" / "comparison",
    )

    summary = json.loads(Path(paths["summary_path"]).read_text(encoding="utf-8"))
    rankings = pd.read_csv(paths["rankings_path"])
    candidates = json.loads(Path(paths["candidates_path"]).read_text(encoding="utf-8"))

    assert summary["candidate_count"] >= 1
    assert "composite_score" in rankings.columns
    assert candidates[0]["providers"] == ["binance"]
