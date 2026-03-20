from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.automation import AutomatedAlphaResearchConfig
from trading_platform.research.alpha_lab.generation import SignalGenerationConfig
from trading_platform.research.experiment_tracking import (
    build_alpha_experiment_record,
    build_paper_experiment_record,
    register_experiment,
)
from trading_platform.research.refresh_monitoring import (
    MonitoringConfig,
    ScheduledResearchRefreshConfig,
    build_monitoring_report,
    run_scheduled_research_refresh,
    show_current_vs_previous_configuration,
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


def _write_alpha_artifacts(
    artifact_dir: Path,
    *,
    run_id: str,
    promoted_signals: list[dict[str, object]],
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
        promoted_signals
        + [
            {
                "candidate_id": "momentum|horizon=1|lookback=10",
                "signal_family": "momentum",
                "lookback": 10,
                "horizon": 1,
                "promotion_status": "reject",
                "rejection_reason": "low_ic",
            }
        ]
    ).to_csv(artifact_dir / "leaderboard.csv", index=False)
    pd.DataFrame(promoted_signals).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "portfolio_total_return": 0.25,
                "portfolio_sharpe": 1.4,
                "portfolio_max_drawdown": -0.10,
                "mean_turnover": 0.20,
                "mean_active_positions": 2.0,
            }
        ]
    ).to_csv(artifact_dir / "portfolio_metrics.csv", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-10",
                "horizon": 1,
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "portfolio_return_net": 0.010,
            },
            {
                "timestamp": "2026-03-11",
                "horizon": 1,
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "portfolio_return_net": 0.012,
            },
        ]
    ).to_csv(artifact_dir / "portfolio_returns.csv", index=False)
    pd.DataFrame(
        [
            {
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "mean_top_position_weight": 0.50,
                "mean_net_exposure": 1.0,
                "mean_gross_exposure": 1.0,
            }
        ]
    ).to_csv(artifact_dir / "robustness_report.csv", index=False)
    pd.DataFrame([{"return_drag": 0.02, "mean_capacity_multiple": 1.5}]).to_csv(
        artifact_dir / "implementability_report.csv",
        index=False,
    )
    pd.DataFrame([{"regime": "high_vol", "total_return": 0.10}]).to_csv(
        artifact_dir / "regime_performance.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {"timestamp": "2026-03-17", "regime": "low_vol"},
            {"timestamp": "2026-03-18", "regime": "high_vol"},
        ]
    ).to_csv(artifact_dir / "regime_labels_by_date.csv", index=False)


def _write_paper_artifacts(
    artifact_dir: Path,
    *,
    as_of: str,
    equity: float,
    weights: list[dict[str, object]],
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "paper_summary.json").write_text(
        json.dumps(
            {
                "as_of": as_of,
                "equity": equity,
                "orders": [{"symbol": "AAPL"}],
                "fills": [{"symbol": "AAPL"}],
                "diagnostics": {
                    "signal_source": "composite",
                    "target_construction": {"selection_count": len(weights)},
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "composite_diagnostics.json").write_text(
        json.dumps(
            {
                "selected_signals": [{"candidate_id": "momentum|horizon=1|lookback=5"}],
                "weighting_scheme": "regime_aware",
                "portfolio_mode": "long_only_top_n",
                "horizon": 1,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(weights).to_csv(artifact_dir / "paper_target_weights.csv", index=False)


def test_scheduled_refresh_updates_history_and_snapshots(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "refresh"
    feature_dir.mkdir(parents=True, exist_ok=True)
    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    alpha_config = AutomatedAlphaResearchConfig(
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

    result = run_scheduled_research_refresh(
        config=ScheduledResearchRefreshConfig(alpha_config=alpha_config)
    )

    refresh_history_df = pd.read_csv(result["refresh_history_path"])
    latest_snapshot = json.loads(
        Path(result["latest_configuration_snapshot_path"]).read_text(encoding="utf-8")
    )

    assert result["status"] == "completed"
    assert len(refresh_history_df) == 1
    assert latest_snapshot["signal_registry_summary"]["total_signals"] >= 1
    assert Path(result["approved_configuration_snapshots_path"]).exists()


def test_approved_configuration_diff_reports_changes_between_snapshots(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    first = {
        "promoted_signals": [{"candidate_id": "signal_a"}],
        "composite_inputs": {"horizons": {"1": {"selected_signals": [{"candidate_id": "signal_a"}]}}},
    }
    second = {
        "promoted_signals": [{"candidate_id": "signal_a"}, {"candidate_id": "signal_b"}],
        "composite_inputs": {"horizons": {"1": {"selected_signals": [{"candidate_id": "signal_a"}, {"candidate_id": "signal_b"}]}}},
    }
    (snapshot_dir / "approved_configuration_20260319T120000Z.json").write_text(
        json.dumps(first, indent=2),
        encoding="utf-8",
    )
    (snapshot_dir / "approved_configuration_20260320T120000Z.json").write_text(
        json.dumps(second, indent=2),
        encoding="utf-8",
    )

    diff = show_current_vs_previous_configuration(snapshot_dir=snapshot_dir)

    assert diff["differences_vs_previous"]["changed"] is True
    assert diff["differences_vs_previous"]["promoted_signals_added"]


def test_monitoring_report_generates_drift_alerts(tmp_path: Path) -> None:
    tracker_dir = tmp_path / "tracker"
    snapshot_dir = tmp_path / "snapshots"
    alpha_dir = tmp_path / "alpha_run"
    paper_dir_1 = tmp_path / "paper_run_1"
    paper_dir_2 = tmp_path / "paper_run_2"
    paper_dir_3 = tmp_path / "paper_run_3"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    _write_alpha_artifacts(
        alpha_dir,
        run_id="alpha_001",
        promoted_signals=[
            {
                "candidate_id": "momentum|horizon=1|lookback=5",
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "promotion_status": "promote",
            }
        ],
    )
    _write_paper_artifacts(
        paper_dir_1,
        as_of="2026-03-17",
        equity=100_000.0,
        weights=[
            {"symbol": "AAPL", "effective_target_weight": 0.50},
            {"symbol": "MSFT", "effective_target_weight": 0.50},
        ],
    )
    _write_paper_artifacts(
        paper_dir_2,
        as_of="2026-03-18",
        equity=95_000.0,
        weights=[
            {"symbol": "AAPL", "effective_target_weight": 1.00},
        ],
    )
    _write_paper_artifacts(
        paper_dir_3,
        as_of="2026-03-19",
        equity=90_000.0,
        weights=[
            {"symbol": "MSFT", "effective_target_weight": 1.00},
        ],
    )

    register_experiment(build_alpha_experiment_record(alpha_dir), tracker_dir=tracker_dir)
    register_experiment(build_paper_experiment_record(paper_dir_1), tracker_dir=tracker_dir)
    register_experiment(build_paper_experiment_record(paper_dir_2), tracker_dir=tracker_dir)
    register_experiment(build_paper_experiment_record(paper_dir_3), tracker_dir=tracker_dir)

    (snapshot_dir / "approved_configuration_20260318T120000Z.json").write_text(
        json.dumps({"promoted_signals": [{"candidate_id": "signal_a"}], "composite_inputs": {}}, indent=2),
        encoding="utf-8",
    )
    (snapshot_dir / "approved_configuration_20260319T120000Z.json").write_text(
        json.dumps(
            {
                "promoted_signals": [
                    {"candidate_id": "signal_a"},
                    {"candidate_id": "signal_b"},
                    {"candidate_id": "signal_c"},
                    {"candidate_id": "signal_d"},
                ],
                "composite_inputs": {"changed": True},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = build_monitoring_report(
        config=MonitoringConfig(
            tracker_dir=tracker_dir,
            output_dir=tmp_path / "monitoring",
            snapshot_dir=snapshot_dir,
            recent_paper_runs=3,
            performance_degradation_buffer=0.001,
            turnover_spike_multiple=1.2,
            concentration_spike_multiple=1.2,
            signal_churn_threshold=2,
        )
    )

    alerts_df = pd.read_csv(result["drift_alerts_path"])
    alert_types = set(alerts_df["alert_type"])

    assert {
        "performance_degradation",
        "turnover_spike",
        "concentration_increase",
        "regime_shift",
        "signal_churn",
    } <= alert_types


def test_monitoring_report_handles_empty_inputs(tmp_path: Path) -> None:
    tracker_dir = tmp_path / "tracker"
    result = build_monitoring_report(
        config=MonitoringConfig(
            tracker_dir=tracker_dir,
            output_dir=tmp_path / "monitoring",
            snapshot_dir=tmp_path / "snapshots",
        )
    )

    report_payload = json.loads(Path(result["monitoring_report_path"]).read_text(encoding="utf-8"))
    alerts_df = pd.read_csv(result["drift_alerts_path"])

    assert report_payload["alerts"] == []
    assert alerts_df.empty
