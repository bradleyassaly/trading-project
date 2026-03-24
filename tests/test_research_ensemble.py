from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.alpha_lab.runner import run_alpha_research
from trading_platform.research.ensemble import (
    EnsembleConfig,
    assign_member_weights,
    build_ensemble_scores,
    compute_ensemble_weights,
    normalize_signal_scores,
    select_ensemble_members,
)


def test_normalize_signal_scores_handles_rank_pct_zscore_and_raw() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"]),
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "signal": [1.0, 2.0, 3.0, 3.0],
        }
    )
    raw = normalize_signal_scores(df, "raw")
    zscore = normalize_signal_scores(df, "zscore")
    rank_pct = normalize_signal_scores(df, "rank_pct")

    assert raw["normalized_score"].tolist() == [1.0, 2.0, 3.0, 3.0]
    assert zscore.loc[zscore["timestamp"] == pd.Timestamp("2025-01-02"), "normalized_score"].tolist() == [0.0, 0.0]
    assert rank_pct["normalized_score"].between(0.0, 1.0).all()


def test_compute_ensemble_weights_and_selection_behave_deterministically() -> None:
    members = pd.DataFrame(
        [
            {"candidate_id": "momentum|5|1", "signal_family": "momentum", "lookback": 5, "horizon": 1, "mean_spearman_ic": 0.05, "promotion_status": "promote", "total_obs": 100},
            {"candidate_id": "value|5|1", "signal_family": "value", "lookback": 5, "horizon": 1, "mean_spearman_ic": 0.03, "promotion_status": "promote", "total_obs": 100},
            {"candidate_id": "value|10|1", "signal_family": "value", "lookback": 10, "horizon": 1, "mean_spearman_ic": 0.01, "promotion_status": "reject", "total_obs": 100},
        ]
    )
    config = EnsembleConfig(
        enabled=True,
        mode="family_weighted",
        weight_method="rank_weighted",
        normalize_scores="rank_pct",
        max_members=2,
        require_promoted_only=True,
        max_members_per_family=1,
    )
    selected = select_ensemble_members(members, config)
    weighted = assign_member_weights(selected, config)

    assert weighted["included_in_ensemble"].sum() == 2
    assert "family_cap" in set(weighted["exclusion_reason"]) or "not_promoted" in set(weighted["exclusion_reason"])
    assert weighted.loc[weighted["included_in_ensemble"], "normalized_weight"].sum() == pytest.approx(1.0)

    performance_weighted = compute_ensemble_weights(
        weighted.loc[weighted["included_in_ensemble"]],
        EnsembleConfig(enabled=True, mode="candidate_weighted", weight_method="performance_weighted"),
    )
    assert performance_weighted["normalized_weight"].sum() == pytest.approx(1.0)
    with pytest.raises(ValueError, match="Unsupported ensemble mode"):
        EnsembleConfig(enabled=True, mode="bad_mode")


def test_build_ensemble_scores_supports_candidate_and_family_modes() -> None:
    member_summary = pd.DataFrame(
        [
            {
                "member_id": "momentum|5|1",
                "member_type": "candidate",
                "family": "momentum",
                "selection_rank": 1,
                "raw_metric": 0.05,
                "normalized_weight": 0.5,
                "included_in_ensemble": True,
                "exclusion_reason": "",
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "promotion_status": "promote",
                "total_obs": 50,
            },
            {
                "member_id": "value|5|1",
                "member_type": "candidate",
                "family": "value",
                "selection_rank": 2,
                "raw_metric": 0.04,
                "normalized_weight": 0.5,
                "included_in_ensemble": True,
                "exclusion_reason": "",
                "signal_family": "value",
                "lookback": 5,
                "horizon": 1,
                "promotion_status": "promote",
                "total_obs": 50,
            },
        ]
    )
    signal_frames = {
        "momentum|5|1": pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-01"]),
                "symbol": ["AAPL", "MSFT"],
                "signal": [1.0, 0.0],
            }
        ),
        "value|5|1": pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-01"]),
                "symbol": ["AAPL", "MSFT"],
                "signal": [0.5, 2.0],
            }
        ),
    }

    candidate_scores = build_ensemble_scores(
        signal_frames,
        EnsembleConfig(enabled=True, mode="candidate_weighted", weight_method="equal", normalize_scores="raw"),
        member_summary,
    )
    family_scores = build_ensemble_scores(
        signal_frames,
        EnsembleConfig(enabled=True, mode="family_weighted", weight_method="equal", normalize_scores="raw"),
        member_summary,
    )

    assert list(candidate_scores["symbol"]) == ["AAPL", "MSFT"]
    assert list(family_scores["symbol"]) == ["AAPL", "MSFT"]
    assert candidate_scores["ensemble_score"].notna().all()
    assert family_scores["ensemble_score"].notna().all()


def test_run_alpha_research_writes_ensemble_artifacts_when_enabled(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    for symbol, daily_return in {"AAPL": 0.010, "MSFT": 0.015, "NVDA": 0.020}.items():
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

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1, 2],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        ensemble_enabled=True,
        ensemble_mode="candidate_weighted",
        ensemble_weight_method="equal",
        ensemble_normalize_scores="rank_pct",
        ensemble_max_members=2,
    )

    member_summary = pd.read_csv(result["ensemble_member_summary_path"])
    signal_snapshot = pd.read_csv(result["ensemble_signal_snapshot_path"])

    assert Path(result["ensemble_research_summary_path"]).exists()
    assert {"member_id", "normalized_weight", "included_in_ensemble"} <= set(member_summary.columns)
    assert {"ensemble_score", "member_count", "top_contributing_member"} <= set(signal_snapshot.columns)
