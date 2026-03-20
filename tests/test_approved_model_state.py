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
        json.dumps({"composite_portfolio": {"top_n": 10}, "signal_lifecycle": {}, "regime": {}}),
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
