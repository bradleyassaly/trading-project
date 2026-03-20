from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.automation import (
    AutomatedAlphaResearchConfig,
    SignalSearchSpace,
    generate_candidate_configs,
    run_automated_alpha_research_loop,
    select_untested_candidates,
)


def test_generate_candidate_configs_builds_unique_family_range_grid() -> None:
    candidates = generate_candidate_configs(
        [
            SignalSearchSpace(
                signal_family="momentum",
                lookbacks=(5, 10),
                horizons=(1, 5),
            ),
            SignalSearchSpace(
                signal_family="momentum",
                lookbacks=(5,),
                horizons=(1,),
            ),
        ]
    )

    assert len(candidates) == 4
    assert candidates["candidate_id"].nunique() == 4


def test_select_untested_candidates_skips_existing_completed_registry_rows() -> None:
    candidates = pd.DataFrame(
        [
            {"candidate_id": "momentum|5|1", "signal_family": "momentum", "lookback": 5, "horizon": 1},
            {"candidate_id": "momentum|10|1", "signal_family": "momentum", "lookback": 10, "horizon": 1},
        ]
    )
    registry = pd.DataFrame(
        [
            {
                "candidate_id": "momentum|5|1",
                "evaluation_status": "completed",
            }
        ]
    )

    pending = select_untested_candidates(candidates, registry)

    assert pending["candidate_id"].tolist() == ["momentum|10|1"]


def test_run_automated_alpha_research_loop_updates_registry_without_retesting(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    daily_returns = {
        "AAPL": 0.010,
        "MSFT": 0.015,
        "NVDA": 0.020,
    }
    for symbol, daily_return in daily_returns.items():
        closes = [100.0]
        for _ in range(79):
            closes.append(closes[-1] * (1.0 + daily_return))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        search_spaces=(
            SignalSearchSpace(
                signal_family="momentum",
                lookbacks=(1, 2),
                horizons=(1,),
            ),
        ),
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=20,
        test_size=10,
        step_size=10,
        schedule_frequency="manual",
    )

    first_result = run_automated_alpha_research_loop(config=config)
    second_result = run_automated_alpha_research_loop(config=config)

    registry_df = pd.read_csv(first_result["registry_path"])
    history_df = pd.read_csv(first_result["history_path"])
    leaderboard_df = pd.read_csv(first_result["leaderboard_path"])
    promoted_df = pd.read_csv(first_result["promoted_signals_path"])
    composite_inputs = (output_dir / "composite_inputs.json").read_text(encoding="utf-8")
    second_schedule = pd.read_json(second_result["schedule_path"], typ="series")

    assert len(registry_df) == 2
    assert len(history_df) == 2
    assert set(registry_df["promotion_status"]) == {"promote"}
    assert not leaderboard_df.empty
    assert len(promoted_df) == 2
    assert "selected_signals" in composite_inputs
    assert int(second_schedule["candidates_evaluated"]) == 0
