from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.experiment_tracking import (
    build_alpha_experiment_record,
    build_experiment_summary_report,
    build_latest_model_state,
    build_paper_experiment_record,
    load_experiment_registry,
    register_experiment,
)


def _write_alpha_artifacts(
    artifact_dir: Path,
    *,
    run_id: str,
    promoted_signals: list[dict[str, object]],
    portfolio_sharpe: float,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "signal_diagnostics.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "run_timestamp": "2026-03-19T12:00:00+00:00",
                "signal_family": "momentum",
                "lookbacks": [5, 10],
                "horizons": [1],
                "signal_lifecycle": {"recent_quality_window": 20},
                "regime": {"enabled": True},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps(
            {
                "config": {"weighting_schemes": ["equal", "regime_aware"]},
                "horizons": {"1": {"selected_signals": promoted_signals}},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "promotion_status": "promote",
            },
            {
                "signal_family": "momentum",
                "lookback": 10,
                "horizon": 1,
                "promotion_status": "reject",
            },
        ]
    ).to_csv(artifact_dir / "leaderboard.csv", index=False)
    pd.DataFrame(promoted_signals).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "portfolio_total_return": 0.25,
                "portfolio_sharpe": portfolio_sharpe,
                "portfolio_max_drawdown": -0.10,
            }
        ]
    ).to_csv(artifact_dir / "portfolio_metrics.csv", index=False)
    pd.DataFrame(
        [
            {
                "worst_fold_return": -0.03,
                "worst_fold_sharpe": 0.20,
            }
        ]
    ).to_csv(artifact_dir / "robustness_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "return_drag": 0.02,
                "mean_capacity_multiple": 1.5,
            }
        ]
    ).to_csv(artifact_dir / "implementability_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "weighting_scheme": "regime_aware",
                "regime": "high_vol",
                "total_return": 0.10,
            }
        ]
    ).to_csv(artifact_dir / "regime_performance.csv", index=False)


def _write_paper_artifacts(artifact_dir: Path) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "paper_summary.json").write_text(
        json.dumps(
            {
                "as_of": "2026-03-19",
                "equity": 105000.0,
                "orders": [{"symbol": "AAPL"}],
                "fills": [{"symbol": "AAPL"}],
                "diagnostics": {
                    "signal_source": "composite",
                    "target_construction": {"selection_count": 2},
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps(
            {
                "selected_signals": [{"signal_family": "momentum", "lookback": 5, "horizon": 1}],
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "horizon": 1,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_experiment_registry_updates_and_summary_report_generation(tmp_path: Path) -> None:
    tracker_dir = tmp_path / "tracker"
    alpha_dir = tmp_path / "alpha_run"
    paper_dir = tmp_path / "paper_run"
    _write_alpha_artifacts(
        alpha_dir,
        run_id="alpha_001",
        promoted_signals=[{"signal_family": "momentum", "lookback": 5, "horizon": 1}],
        portfolio_sharpe=1.4,
    )
    _write_paper_artifacts(paper_dir)

    register_experiment(build_alpha_experiment_record(alpha_dir), tracker_dir=tracker_dir)
    register_experiment(build_paper_experiment_record(paper_dir), tracker_dir=tracker_dir)
    report_paths = build_experiment_summary_report(tracker_dir=tracker_dir)

    registry_df = load_experiment_registry(tracker_dir / "experiment_registry.csv")
    report_payload = json.loads(Path(report_paths["experiment_summary_report_path"]).read_text(encoding="utf-8"))
    latest_model_state = json.loads(Path(report_paths["latest_model_state_path"]).read_text(encoding="utf-8"))

    assert len(registry_df) == 2
    assert {"alpha_research", "paper_trading"} == set(registry_df["experiment_type"])
    assert report_payload["top_experiments"]
    assert report_payload["paper_vs_backtest_comparison"]
    assert latest_model_state["latest_approved_experiment"]["promoted_signals"]


def test_duplicate_experiment_detection_marks_duplicate_of_existing_run(tmp_path: Path) -> None:
    tracker_dir = tmp_path / "tracker"
    alpha_dir = tmp_path / "alpha_run"
    _write_alpha_artifacts(
        alpha_dir,
        run_id="alpha_001",
        promoted_signals=[{"signal_family": "momentum", "lookback": 5, "horizon": 1}],
        portfolio_sharpe=1.4,
    )

    record = build_alpha_experiment_record(alpha_dir)
    register_experiment(record, tracker_dir=tracker_dir)
    register_experiment(record, tracker_dir=tracker_dir)

    registry_df = load_experiment_registry(tracker_dir / "experiment_registry.csv")

    assert len(registry_df) == 2
    assert registry_df["duplicate_of"].iloc[1] == registry_df["experiment_id"].iloc[0]


def test_latest_model_state_reports_differences_between_approved_configurations(tmp_path: Path) -> None:
    tracker_dir = tmp_path / "tracker"
    first_dir = tmp_path / "alpha_run_1"
    second_dir = tmp_path / "alpha_run_2"
    _write_alpha_artifacts(
        first_dir,
        run_id="alpha_001",
        promoted_signals=[{"signal_family": "momentum", "lookback": 5, "horizon": 1}],
        portfolio_sharpe=1.0,
    )
    _write_alpha_artifacts(
        second_dir,
        run_id="alpha_002",
        promoted_signals=[
            {"signal_family": "momentum", "lookback": 5, "horizon": 1},
            {"signal_family": "momentum", "lookback": 10, "horizon": 1},
        ],
        portfolio_sharpe=1.5,
    )
    register_experiment(build_alpha_experiment_record(first_dir), tracker_dir=tracker_dir)
    register_experiment(build_alpha_experiment_record(second_dir), tracker_dir=tracker_dir)

    latest_model_state = build_latest_model_state(tracker_dir)

    assert latest_model_state["latest_approved_experiment"]["experiment_id"]
    assert latest_model_state["differences_vs_prior"]


def test_summary_report_handles_empty_and_missing_artifacts(tmp_path: Path) -> None:
    tracker_dir = tmp_path / "tracker"
    empty_paths = build_experiment_summary_report(tracker_dir=tracker_dir)
    empty_report = json.loads(Path(empty_paths["experiment_summary_report_path"]).read_text(encoding="utf-8"))

    assert empty_report["top_experiments"] == []

    missing_record = {
        "experiment_id": "missing-alpha",
        "experiment_type": "alpha_research",
        "run_id": "missing",
        "timestamp": "2026-03-19T12:00:00+00:00",
        "artifact_dir": str(tmp_path / "missing_artifacts"),
        "config_fingerprint": "missing-config",
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
    register_experiment(missing_record, tracker_dir=tracker_dir)
    missing_paths = build_experiment_summary_report(tracker_dir=tracker_dir)
    missing_report = json.loads(Path(missing_paths["experiment_summary_report_path"]).read_text(encoding="utf-8"))

    assert missing_report["diagnostics"]["missing_artifacts"]
