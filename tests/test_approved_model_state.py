from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.composite import build_composite_paper_snapshot
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.research.approved_model_state import (
    load_approved_model_state,
    write_approved_model_state,
)


def test_write_and_load_approved_model_state(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "momentum_lb5_h1",
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
            }
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(
        [{"left_candidate_id": "a", "right_candidate_id": "b", "score_corr": 0.1}]
    ).to_csv(artifact_dir / "redundancy_report.csv", index=False)
    (artifact_dir / "composite_inputs.json").write_text(
        json.dumps(
            {
                "horizons": {
                    "1": {
                        "selected_signals": [
                            {
                                "candidate_id": "momentum_lb5_h1",
                                "signal_family": "momentum",
                                "lookback": 5,
                                "horizon": 1,
                            }
                        ],
                        "excluded_signals": [],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "signal_diagnostics.json").write_text(
        json.dumps(
            {
                "composite_portfolio": {"top_n": 10},
                "signal_lifecycle": {},
                "regime": {},
                "signal_composition_preset": "composite_v1",
                "signal_composition": {"preset": "composite_v1"},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps({"config": {"weighting_scheme": "equal"}}),
        encoding="utf-8",
    )

    paths = write_approved_model_state(artifact_dir=artifact_dir)
    payload = load_approved_model_state(paths["approved_model_state_path"])

    assert payload["artifact_type"] == "approved_model_state"
    assert payload["approval_status"] == "approved"
    assert payload["promoted_signals"][0]["candidate_id"] == "momentum_lb5_h1"
    assert payload["composite_inputs"]["horizons"]["1"]["selected_signals"]
    assert payload["signal_composition_preset"] == "composite_v1"


def test_build_composite_paper_snapshot_uses_approved_model_state(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "momentum_lb1_h1", "signal_family": "momentum", "lookback": 1, "horizon": 1}
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(columns=["left_candidate_id", "right_candidate_id", "score_corr"]).to_csv(
        artifact_dir / "redundancy_report.csv", index=False
    )
    (artifact_dir / "composite_inputs.json").write_text(
        json.dumps(
            {
                "horizons": {
                    "1": {
                        "selected_signals": [
                            {
                                "candidate_id": "momentum_lb1_h1",
                                "signal_family": "momentum",
                                "lookback": 1,
                                "horizon": 1,
                            }
                        ],
                        "excluded_signals": [],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "signal_diagnostics.json").write_text(
        json.dumps({"composite_portfolio": {"top_n": 10}, "signal_lifecycle": {}, "regime": {}}),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps({"config": {"weighting_scheme": "equal"}}),
        encoding="utf-8",
    )
    approved_paths = write_approved_model_state(artifact_dir=artifact_dir)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=4, freq="D"),
                "close": [100.0, 101.0, 102.0, 103.0] if symbol == "AAPL" else [50.0, 49.0, 51.0, 52.0],
                "volume": [1000, 1100, 1200, 1300],
            }
        )

    monkeypatch.setattr(
        "trading_platform.paper.composite.load_feature_frame",
        fake_load_feature_frame,
    )

    snapshot, diagnostics = build_composite_paper_snapshot(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT"],
            signal_source="composite",
            approved_model_state_path=approved_paths["approved_model_state_path"],
            composite_horizon=1,
        )
    )

    assert not snapshot.scores.empty
    assert diagnostics["approved_model_state_path"] == approved_paths["approved_model_state_path"]


def test_build_composite_paper_snapshot_honors_signal_variant_metadata(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "momentum|quality_tilt|1|1",
                "signal_family": "momentum",
                "signal_variant": "quality_tilt",
                "variant_parameters_json": json.dumps({"quality_bias": 1.0}),
                "lookback": 1,
                "horizon": 1,
            }
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(columns=["left_candidate_id", "right_candidate_id", "score_corr"]).to_csv(
        artifact_dir / "redundancy_report.csv", index=False
    )
    (artifact_dir / "composite_inputs.json").write_text(
        json.dumps(
            {
                "horizons": {
                    "1": {
                        "selected_signals": [
                            {
                                "candidate_id": "momentum|quality_tilt|1|1",
                                "signal_family": "momentum",
                                "signal_variant": "quality_tilt",
                                "variant_parameters_json": json.dumps({"quality_bias": 1.0}),
                                "lookback": 1,
                                "horizon": 1,
                            }
                        ],
                        "excluded_signals": [],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "signal_diagnostics.json").write_text(
        json.dumps({"composite_portfolio": {"top_n": 10}, "signal_lifecycle": {}, "regime": {}}),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps({"config": {"weighting_scheme": "equal"}}),
        encoding="utf-8",
    )
    approved_paths = write_approved_model_state(artifact_dir=artifact_dir)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=4, freq="D"),
                "close": [100.0, 101.0, 102.0, 103.0] if symbol == "AAPL" else [50.0, 49.0, 51.0, 52.0],
                "volume": [1000, 1100, 1200, 1300],
                "quality_bias": [1.0, 1.0, 1.0, 1.0],
            }
        )

    calls: list[dict[str, object]] = []

    def fake_build_signal(df: pd.DataFrame, **kwargs) -> pd.Series:
        calls.append(kwargs)
        signal_variant = str(kwargs.get("signal_variant") or "base")
        if signal_variant != "quality_tilt":
            return pd.Series([float("nan")] * len(df), index=df.index)
        return pd.Series([0.1, 0.2, 0.3, 0.4], index=df.index)

    monkeypatch.setattr(
        "trading_platform.paper.composite.load_feature_frame",
        fake_load_feature_frame,
    )
    monkeypatch.setattr(
        "trading_platform.paper.composite.build_signal",
        fake_build_signal,
    )

    snapshot, diagnostics = build_composite_paper_snapshot(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT"],
            signal_source="composite",
            approved_model_state_path=approved_paths["approved_model_state_path"],
            composite_horizon=1,
        )
    )

    assert not snapshot.scores.empty
    assert diagnostics["selected_signals"][0]["signal_family"] == "momentum"
    assert calls and calls[0]["signal_variant"] == "quality_tilt"
    assert calls[0]["variant_params"] == {"quality_bias": 1.0}


def test_build_composite_paper_snapshot_merges_daily_fundamental_features(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    daily_features_path = tmp_path / "daily_fundamental_features.parquet"
    pd.DataFrame(
        [
            {"timestamp": ts, "symbol": symbol, "quality_bias": 1.0}
            for symbol in ["AAPL", "MSFT"]
            for ts in pd.date_range("2025-01-01", periods=4, freq="D")
        ]
    ).to_parquet(daily_features_path, index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "momentum|quality_tilt|1|1",
                "signal_family": "momentum",
                "signal_variant": "quality_tilt",
                "variant_parameters_json": json.dumps({"quality_bias": 1.0}),
                "lookback": 1,
                "horizon": 1,
            }
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(columns=["left_candidate_id", "right_candidate_id", "score_corr"]).to_csv(
        artifact_dir / "redundancy_report.csv", index=False
    )
    (artifact_dir / "composite_inputs.json").write_text(
        json.dumps(
            {
                "horizons": {
                    "1": {
                        "selected_signals": [
                            {
                                "candidate_id": "momentum|quality_tilt|1|1",
                                "signal_family": "momentum",
                                "signal_variant": "quality_tilt",
                                "variant_parameters_json": json.dumps({"quality_bias": 1.0}),
                                "lookback": 1,
                                "horizon": 1,
                            }
                        ],
                        "excluded_signals": [],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "signal_diagnostics.json").write_text(
        json.dumps(
            {
                "composite_portfolio": {"top_n": 10},
                "signal_lifecycle": {},
                "regime": {},
                "signal_composition_preset": "composite_v1",
                "signal_composition": {"preset": "composite_v1"},
                "fundamentals": {"enabled": True, "daily_features_path": str(daily_features_path)},
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps({"config": {"weighting_scheme": "equal"}}),
        encoding="utf-8",
    )
    approved_paths = write_approved_model_state(artifact_dir=artifact_dir)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=4, freq="D"),
                "close": [100.0, 101.0, 102.0, 103.0] if symbol == "AAPL" else [50.0, 49.0, 51.0, 52.0],
                "volume": [1000, 1100, 1200, 1300],
            }
        )

    def fake_build_signal(df: pd.DataFrame, **kwargs) -> pd.Series:
        if "quality_bias" not in df.columns:
            return pd.Series([float("nan")] * len(df), index=df.index)
        return pd.Series([0.1, 0.2, 0.3, 0.4], index=df.index)

    monkeypatch.setattr(
        "trading_platform.paper.composite.load_feature_frame",
        fake_load_feature_frame,
    )
    monkeypatch.setattr(
        "trading_platform.paper.composite.build_signal",
        fake_build_signal,
    )

    snapshot, diagnostics = build_composite_paper_snapshot(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT"],
            signal_source="composite",
            approved_model_state_path=approved_paths["approved_model_state_path"],
            composite_horizon=1,
        )
    )

    assert not snapshot.scores.empty
    assert diagnostics["latest_component_scores"]
