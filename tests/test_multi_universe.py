from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.multi_universe import (
    MultiUniverseResearchConfig,
    build_multi_universe_comparison_report,
    run_multi_universe_alpha_research,
)


def _write_universe_artifacts(
    artifact_dir: Path,
    *,
    promoted_signals: list[dict[str, object]],
    portfolio_total_return: float,
    portfolio_sharpe: float,
    return_drag: float,
    regime_enabled: bool = False,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(promoted_signals).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "weighting_scheme": "equal",
                "portfolio_mode": "long_only_top_n",
                "portfolio_total_return": portfolio_total_return,
                "portfolio_sharpe": portfolio_sharpe,
            }
        ]
    ).to_csv(artifact_dir / "portfolio_metrics.csv", index=False)
    pd.DataFrame(
        [
            {
                "weighting_scheme": "equal",
                "portfolio_mode": "long_only_top_n",
                "mean_top_position_weight": 0.45,
                "worst_fold_return": -0.02,
            }
        ]
    ).to_csv(artifact_dir / "robustness_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "horizon": 1,
                "weighting_scheme": "equal",
                "portfolio_mode": "long_only_top_n",
                "excluded_names": 1,
                "return_drag": return_drag,
            }
        ]
    ).to_csv(artifact_dir / "implementability_report.csv", index=False)
    (artifact_dir / "signal_diagnostics.json").write_text(
        json.dumps({"regime": {"enabled": regime_enabled}}, indent=2),
        encoding="utf-8",
    )


def test_build_multi_universe_comparison_report_generates_artifacts_and_overlap(tmp_path: Path) -> None:
    output_dir = tmp_path / "multi"
    universe_a_dir = output_dir / "universe_a"
    universe_b_dir = output_dir / "universe_b"
    _write_universe_artifacts(
        universe_a_dir,
        promoted_signals=[
            {"candidate_id": "signal_a", "signal_family": "momentum", "lookback": 5, "horizon": 1},
            {"candidate_id": "signal_b", "signal_family": "momentum", "lookback": 10, "horizon": 1},
        ],
        portfolio_total_return=0.30,
        portfolio_sharpe=1.5,
        return_drag=0.01,
        regime_enabled=True,
    )
    _write_universe_artifacts(
        universe_b_dir,
        promoted_signals=[
            {"candidate_id": "signal_b", "signal_family": "momentum", "lookback": 10, "horizon": 1},
            {"candidate_id": "signal_c", "signal_family": "momentum", "lookback": 20, "horizon": 1},
        ],
        portfolio_total_return=0.05,
        portfolio_sharpe=0.4,
        return_drag=0.08,
    )

    result = build_multi_universe_comparison_report(output_dir=output_dir)

    overlap_df = pd.read_csv(result["approved_signal_overlap_path"])
    summary_df = pd.read_csv(result["universe_summary_path"])
    summary_payload = json.loads(Path(result["cross_universe_comparison_summary_path"]).read_text(encoding="utf-8"))

    assert overlap_df.iloc[0]["overlap_count"] == 1
    assert abs(float(overlap_df.iloc[0]["jaccard_overlap"]) - (1 / 3)) < 1e-9
    assert set(summary_df["universe"]) == {"universe_a", "universe_b"}
    assert summary_payload["performance_concentrated_in_one_universe"] is True
    assert "signal_a" in summary_payload["universe_specific_signals"]
    assert "signal_c" in summary_payload["universe_specific_signals"]


def test_run_multi_universe_alpha_research_executes_all_universes(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "multi"
    tracker_dir = tmp_path / "tracker"
    seen_universes: list[str] = []
    registered_dirs: list[str] = []

    universe_map = {
        "u1": ["AAPL", "MSFT"],
        "u2": ["NVDA", "AMZN"],
    }

    def fake_get_universe_symbols(name: str) -> list[str]:
        return universe_map[name]

    def fake_run_alpha_research(*, symbols, output_dir, signal_family, **kwargs):
        seen_universes.append(Path(output_dir).name)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [{"candidate_id": f"{Path(output_dir).name}_signal", "signal_family": signal_family, "lookback": 5, "horizon": 1}]
        ).to_csv(Path(output_dir) / "promoted_signals.csv", index=False)
        pd.DataFrame(
            [{"weighting_scheme": "equal", "portfolio_mode": "long_only_top_n", "portfolio_total_return": 0.1, "portfolio_sharpe": 1.0}]
        ).to_csv(Path(output_dir) / "portfolio_metrics.csv", index=False)
        pd.DataFrame(
            [{"weighting_scheme": "equal", "portfolio_mode": "long_only_top_n", "mean_top_position_weight": 0.4}]
        ).to_csv(Path(output_dir) / "robustness_report.csv", index=False)
        pd.DataFrame(
            [{"horizon": 1, "weighting_scheme": "equal", "portfolio_mode": "long_only_top_n", "excluded_names": 0, "return_drag": 0.01}]
        ).to_csv(Path(output_dir) / "implementability_report.csv", index=False)
        (Path(output_dir) / "signal_diagnostics.json").write_text("{}", encoding="utf-8")
        return {"leaderboard_path": str(Path(output_dir) / "leaderboard.csv")}

    def fake_build_alpha_experiment_record(artifact_dir: Path) -> dict[str, object]:
        return {
            "experiment_id": artifact_dir.name,
            "experiment_type": "alpha_research",
            "run_id": artifact_dir.name,
            "timestamp": "2026-03-19T12:00:00+00:00",
            "artifact_dir": str(artifact_dir),
            "config_fingerprint": artifact_dir.name,
            "duplicate_of": "",
            "signal_family": "momentum",
            "parameters_json": "{}",
            "promotion_status": "approved",
            "promoted_signal_count": 1,
            "rejected_signal_count": 0,
            "composite_config_json": "{}",
            "regime_config_json": "{}",
            "portfolio_weighting_scheme": "equal",
            "portfolio_mode": "long_only_top_n",
            "portfolio_total_return": 0.1,
            "portfolio_sharpe": 1.0,
            "portfolio_max_drawdown": -0.1,
            "robustness_worst_fold_return": -0.02,
            "robustness_worst_fold_sharpe": 0.1,
            "implementability_return_drag": 0.01,
            "implementability_mean_capacity_multiple": 1.0,
            "paper_signal_source": "",
            "paper_equity": float("nan"),
            "paper_order_count": float("nan"),
            "paper_fill_count": float("nan"),
            "paper_vs_backtest_return_gap": float("nan"),
            "artifacts_json": "{}",
        }

    def fake_register_experiment(record: dict[str, object], *, tracker_dir: Path) -> dict[str, str]:
        registered_dirs.append(record["artifact_dir"])
        tracker_dir.mkdir(parents=True, exist_ok=True)
        return {"experiment_registry_path": str(tracker_dir / "experiment_registry.csv")}

    monkeypatch.setattr("trading_platform.research.multi_universe.get_universe_symbols", fake_get_universe_symbols)
    monkeypatch.setattr("trading_platform.research.multi_universe.run_alpha_research", fake_run_alpha_research)
    monkeypatch.setattr("trading_platform.research.multi_universe.build_alpha_experiment_record", fake_build_alpha_experiment_record)
    monkeypatch.setattr("trading_platform.research.multi_universe.register_experiment", fake_register_experiment)

    result = run_multi_universe_alpha_research(
        config=MultiUniverseResearchConfig(
            universes=("u1", "u2"),
            feature_dir=tmp_path / "features",
            output_dir=output_dir,
            experiment_tracker_dir=tracker_dir,
        )
    )

    assert seen_universes == ["u1", "u2"]
    assert len(registered_dirs) == 2
    assert Path(result["promoted_signals_by_universe_path"]).exists()
    assert Path(result["config_path"]).exists()


def test_build_multi_universe_comparison_report_handles_empty_case(tmp_path: Path) -> None:
    result = build_multi_universe_comparison_report(output_dir=tmp_path / "empty")

    overlap_df = pd.read_csv(result["approved_signal_overlap_path"])
    promoted_df = pd.read_csv(result["promoted_signals_by_universe_path"])
    summary_payload = json.loads(Path(result["cross_universe_comparison_summary_path"]).read_text(encoding="utf-8"))

    assert overlap_df.empty
    assert promoted_df.empty
    assert summary_payload["universes_evaluated"] == []
