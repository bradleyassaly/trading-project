from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.automation import (
    AutomatedAlphaResearchConfig,
    load_research_registry,
    run_automated_alpha_research_loop,
    select_untested_candidates,
)
from trading_platform.research.alpha_lab.generation import (
    SignalGenerationConfig,
    generate_candidate_signals,
)


def _write_feature_file(
    feature_dir: Path,
    symbol: str,
    *,
    base_return: float,
) -> None:
    timestamps = pd.date_range("2024-01-01", periods=160, freq="D")
    closes = [100.0]
    for day_index in range(1, len(timestamps)):
        drift = base_return + 0.00005 * day_index
        closes.append(closes[-1] * (1.0 + drift))
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": symbol,
            "close": closes,
        }
    )
    frame["mom_20"] = frame["close"].pct_change(20)
    frame["vol_20"] = frame["close"].pct_change().rolling(20).std()
    frame["dist_sma_200"] = 0.0
    frame["vol_ratio_20"] = 1.0 + base_return
    frame.to_parquet(feature_dir / f"{symbol}.parquet", index=False)


def test_generate_candidate_signals_are_unique_across_families_and_parameters() -> None:
    candidates = generate_candidate_signals(
        SignalGenerationConfig(
            signal_families=("momentum", "mean_reversion", "volatility", "feature_combo"),
            lookbacks=(5, 10),
            vol_windows=(10,),
            combo_thresholds=(0.5, 1.0),
            combo_pairs=(("mom_20", "vol_20"),),
            horizons=(1, 5),
        ),
        feature_columns=["mom_20", "vol_20"],
    )

    assert not candidates.empty
    assert candidates["candidate_id"].nunique() == len(candidates)
    assert set(candidates["signal_name"]) == {"momentum", "mean_reversion", "volatility", "feature_combo"}


def test_select_untested_candidates_skips_completed_registry_entries() -> None:
    candidates = generate_candidate_signals(
        SignalGenerationConfig(
            signal_families=("momentum",),
            lookbacks=(5, 10),
            horizons=(1,),
        )
    )
    registry = pd.DataFrame(
        [
            {
                "candidate_id": candidates.iloc[0]["candidate_id"],
                "evaluation_status": "completed",
            }
        ]
    )

    pending = select_untested_candidates(candidates, registry)

    assert pending["candidate_id"].tolist() == [candidates.iloc[1]["candidate_id"]]


def test_run_automated_alpha_research_loop_updates_registry_and_artifacts(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum", "mean_reversion", "feature_combo"),
            lookbacks=(5, 20),
            combo_thresholds=(0.5,),
            combo_pairs=(("mom_20", "vol_20"),),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
    )

    first_result = run_automated_alpha_research_loop(config=config)
    first_schedule_payload = json.loads((output_dir / "research_schedule.json").read_text(encoding="utf-8"))
    second_result = run_automated_alpha_research_loop(config=config)

    registry_df = pd.read_csv(first_result["registry_path"])
    signal_registry_df = pd.read_csv(first_result["signal_registry_path"])
    history_df = pd.read_csv(first_result["history_path"])
    promoted_df = pd.read_csv(first_result["promoted_signals_path"])
    rejected_df = pd.read_csv(first_result["rejected_signals_path"])
    composite_inputs = json.loads((output_dir / "composite_inputs.json").read_text(encoding="utf-8"))

    assert not registry_df.empty
    assert registry_df.equals(signal_registry_df)
    assert {"signal_name", "parameters_json", "promotion_status", "rejection_reason"} <= set(registry_df.columns)
    assert len(history_df) == len(registry_df)
    assert len(promoted_df) + len(rejected_df) == len(registry_df)
    assert "horizons" in composite_inputs
    assert first_schedule_payload["candidates_evaluated"] == len(registry_df)

    second_schedule = json.loads(Path(second_result["schedule_path"]).read_text(encoding="utf-8"))
    assert second_schedule["candidates_evaluated"] == 0


def test_registry_updates_incrementally_without_retesting_existing_candidates(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    first_config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum",),
            lookbacks=(5,),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
    )
    second_config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum", "volatility"),
            lookbacks=(5,),
            vol_windows=(10,),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
    )

    first_result = run_automated_alpha_research_loop(config=first_config)
    first_registry_df = pd.read_csv(first_result["registry_path"])
    second_result = run_automated_alpha_research_loop(config=second_config)

    registry_df = load_research_registry(Path(second_result["registry_path"]))
    history_df = pd.read_csv(second_result["history_path"])

    assert len(registry_df) == 2
    assert history_df["candidate_id"].nunique() == 2
    assert history_df["candidate_id"].value_counts().max() == 1
    assert len(first_registry_df) == 1
