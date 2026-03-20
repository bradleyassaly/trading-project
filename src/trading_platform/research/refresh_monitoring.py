from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.automation import (
    AutomatedAlphaResearchConfig,
    run_automated_alpha_research_loop,
)
from trading_platform.research.experiment_tracking import (
    build_latest_model_state,
    load_experiment_registry,
)


REFRESH_HISTORY_COLUMNS = [
    "refresh_run_id",
    "timestamp",
    "status",
    "output_dir",
    "snapshot_path",
    "stale_after_days",
    "candidates_generated",
    "candidates_evaluated",
    "promoted_signal_count",
    "rejected_signal_count",
    "config_changed",
]


DRIFT_ALERT_COLUMNS = [
    "timestamp",
    "alert_type",
    "severity",
    "metric",
    "current_value",
    "expected_value",
    "details",
]


@dataclass(frozen=True)
class ScheduledResearchRefreshConfig:
    alpha_config: AutomatedAlphaResearchConfig
    tracker_dir: Path | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["alpha_config"]["feature_dir"] = str(self.alpha_config.feature_dir)
        payload["alpha_config"]["output_dir"] = str(self.alpha_config.output_dir)
        payload["tracker_dir"] = str(self.tracker_dir) if self.tracker_dir else None
        return payload


@dataclass(frozen=True)
class MonitoringConfig:
    tracker_dir: Path
    output_dir: Path
    snapshot_dir: Path | None = None
    alpha_artifact_dir: Path | None = None
    paper_artifact_dir: Path | None = None
    recent_paper_runs: int = 10
    performance_degradation_buffer: float = 0.002
    turnover_spike_multiple: float = 1.5
    concentration_spike_multiple: float = 1.5
    signal_churn_threshold: int = 3


def _safe_read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _json_dump(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, default=str)


def _load_schedule_payload(output_dir: Path) -> dict[str, object]:
    return _safe_read_json(output_dir / "research_schedule.json")


def _load_promoted_signals(output_dir: Path) -> pd.DataFrame:
    return _safe_read_csv(output_dir / "promoted_signals.csv")


def _load_rejected_signals(output_dir: Path) -> pd.DataFrame:
    return _safe_read_csv(output_dir / "rejected_signals.csv")


def build_configuration_snapshot(
    *,
    output_dir: Path,
    tracker_dir: Path | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    promoted_df = _load_promoted_signals(output_dir)
    rejected_df = _load_rejected_signals(output_dir)
    signal_registry_df = _safe_read_csv(output_dir / "signal_registry.csv")
    composite_inputs = _safe_read_json(output_dir / "composite_inputs.json")
    schedule_payload = _load_schedule_payload(output_dir)
    tracker_state = build_latest_model_state(tracker_dir) if tracker_dir else {}
    snapshot_time = now or datetime.now(UTC)

    return {
        "snapshot_timestamp": snapshot_time.isoformat(),
        "run_id": schedule_payload.get("run_id") or snapshot_time.strftime("%Y%m%dT%H%M%SZ"),
        "output_dir": str(output_dir),
        "candidates_generated": int(schedule_payload.get("candidates_generated", 0) or 0),
        "candidates_evaluated": int(schedule_payload.get("candidates_evaluated", 0) or 0),
        "promoted_signals": promoted_df.to_dict(orient="records"),
        "rejected_signals": rejected_df.to_dict(orient="records"),
        "composite_inputs": composite_inputs,
        "signal_registry_summary": {
            "total_signals": int(len(signal_registry_df)),
            "promoted_signal_count": int(len(promoted_df)),
            "rejected_signal_count": int(len(rejected_df)),
        },
        "latest_model_state": tracker_state,
    }


def compare_configuration_snapshots(
    current_snapshot: dict[str, object],
    previous_snapshot: dict[str, object] | None,
) -> dict[str, object]:
    if not previous_snapshot:
        return {
            "promoted_signals_added": [],
            "promoted_signals_removed": [],
            "composite_inputs_changed": False,
            "changed": False,
        }

    current_promoted = {
        _json_dump(item)
        for item in current_snapshot.get("promoted_signals", [])
    }
    previous_promoted = {
        _json_dump(item)
        for item in previous_snapshot.get("promoted_signals", [])
    }
    added = sorted(current_promoted - previous_promoted)
    removed = sorted(previous_promoted - current_promoted)
    composite_changed = (
        current_snapshot.get("composite_inputs") != previous_snapshot.get("composite_inputs")
    )
    return {
        "promoted_signals_added": [json.loads(item) for item in added],
        "promoted_signals_removed": [json.loads(item) for item in removed],
        "composite_inputs_changed": composite_changed,
        "changed": bool(added or removed or composite_changed),
    }


def _load_latest_snapshot(snapshot_dir: Path) -> tuple[dict[str, object] | None, Path | None]:
    if not snapshot_dir.exists():
        return None, None
    candidates = sorted(snapshot_dir.glob("approved_configuration_*.json"))
    if not candidates:
        return None, None
    latest_path = candidates[-1]
    return _safe_read_json(latest_path), latest_path


def _write_refresh_history(history_df: pd.DataFrame, output_dir: Path) -> dict[str, str]:
    csv_path = output_dir / "refresh_history.csv"
    parquet_path = output_dir / "refresh_history.parquet"
    json_path = output_dir / "refresh_history.json"
    history_df.to_csv(csv_path, index=False)
    history_df.to_parquet(parquet_path, index=False)
    json_path.write_text(
        json.dumps(history_df.to_dict(orient="records"), indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "refresh_history_path": str(csv_path),
        "refresh_history_parquet_path": str(parquet_path),
        "refresh_history_json_path": str(json_path),
    }


def run_scheduled_research_refresh(
    *,
    config: ScheduledResearchRefreshConfig,
) -> dict[str, str]:
    output_dir = config.alpha_config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir = output_dir / "approved_configuration_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    previous_snapshot, _ = _load_latest_snapshot(snapshot_dir)
    loop_result = run_automated_alpha_research_loop(config=config.alpha_config)
    timestamp = datetime.now(UTC)
    refresh_run_id = timestamp.strftime("%Y%m%dT%H%M%SZ")
    snapshot_payload: dict[str, object] | None = None
    snapshot_path: Path | None = None
    comparison = {"changed": False, "promoted_signals_added": [], "promoted_signals_removed": []}

    if loop_result.get("status") != "skipped":
        snapshot_payload = build_configuration_snapshot(
            output_dir=output_dir,
            tracker_dir=config.tracker_dir,
            now=timestamp,
        )
        comparison = compare_configuration_snapshots(snapshot_payload, previous_snapshot)
        snapshot_payload["differences_vs_previous"] = comparison
        snapshot_path = snapshot_dir / f"approved_configuration_{refresh_run_id}.json"
        snapshot_path.write_text(
            json.dumps(snapshot_payload, indent=2, default=str),
            encoding="utf-8",
        )
        latest_path = snapshot_dir / "latest_approved_configuration.json"
        latest_path.write_text(
            json.dumps(snapshot_payload, indent=2, default=str),
            encoding="utf-8",
        )

    schedule_payload = _load_schedule_payload(output_dir)
    promoted_df = _load_promoted_signals(output_dir)
    rejected_df = _load_rejected_signals(output_dir)
    history_path = output_dir / "refresh_history.csv"
    history_df = _safe_read_csv(history_path).reindex(columns=REFRESH_HISTORY_COLUMNS)
    history_row = {
        "refresh_run_id": refresh_run_id,
        "timestamp": timestamp.isoformat(),
        "status": loop_result.get("status", ""),
        "output_dir": str(output_dir),
        "snapshot_path": str(snapshot_path) if snapshot_path else "",
        "stale_after_days": (
            int(config.alpha_config.stale_after_days)
            if config.alpha_config.stale_after_days is not None
            else pd.NA
        ),
        "candidates_generated": int(schedule_payload.get("candidates_generated", 0) or 0),
        "candidates_evaluated": int(schedule_payload.get("candidates_evaluated", 0) or 0),
        "promoted_signal_count": int(len(promoted_df)),
        "rejected_signal_count": int(len(rejected_df)),
        "config_changed": bool(comparison.get("changed", False)),
    }
    history_df = pd.concat(
        [history_df, pd.DataFrame([history_row], columns=REFRESH_HISTORY_COLUMNS)],
        ignore_index=True,
    )
    history_paths = _write_refresh_history(history_df, output_dir)

    refresh_config_path = output_dir / "research_refresh_config.json"
    refresh_config_path.write_text(
        json.dumps(config.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )

    result = {
        "status": str(loop_result.get("status", "")),
        "approved_configuration_snapshots_path": str(snapshot_dir),
        "latest_configuration_snapshot_path": str(snapshot_dir / "latest_approved_configuration.json"),
        "refresh_config_path": str(refresh_config_path),
    }
    result.update(history_paths)
    result.update(loop_result)
    return result


def show_current_vs_previous_configuration(
    *,
    snapshot_dir: Path,
) -> dict[str, object]:
    latest_snapshot, latest_path = _load_latest_snapshot(snapshot_dir)
    if latest_snapshot is None:
        return {"latest_snapshot_path": None, "differences_vs_previous": {}}

    candidates = sorted(snapshot_dir.glob("approved_configuration_*.json"))
    previous_snapshot = None
    if len(candidates) > 1:
        previous_snapshot = _safe_read_json(candidates[-2])
    return {
        "latest_snapshot_path": str(latest_path) if latest_path else None,
        "differences_vs_previous": compare_configuration_snapshots(
            latest_snapshot,
            previous_snapshot,
        ),
    }


def _select_latest_artifact_dir(
    registry_df: pd.DataFrame,
    *,
    experiment_type: str,
) -> Path | None:
    subset = registry_df.loc[registry_df["experiment_type"] == experiment_type].copy()
    if subset.empty:
        return None
    subset["timestamp"] = pd.to_datetime(subset["timestamp"], errors="coerce")
    latest = subset.sort_values("timestamp").iloc[-1]
    return Path(str(latest["artifact_dir"]))


def _best_alpha_configuration(alpha_dir: Path) -> tuple[str, str]:
    metrics_df = _safe_read_csv(alpha_dir / "portfolio_metrics.csv")
    if metrics_df.empty:
        return "", ""
    ordered = metrics_df.sort_values(
        ["portfolio_sharpe", "portfolio_total_return"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)
    row = ordered.iloc[0]
    return str(row.get("weighting_scheme", "")), str(row.get("portfolio_mode", ""))


def _expected_alpha_metrics(alpha_dir: Path) -> dict[str, float | str]:
    metrics_df = _safe_read_csv(alpha_dir / "portfolio_metrics.csv")
    robustness_df = _safe_read_csv(alpha_dir / "robustness_report.csv")
    if metrics_df.empty:
        return {}
    weighting_scheme, portfolio_mode = _best_alpha_configuration(alpha_dir)
    metric_row = metrics_df.loc[
        (metrics_df["weighting_scheme"] == weighting_scheme)
        & (metrics_df["portfolio_mode"] == portfolio_mode)
    ]
    robustness_row = robustness_df.loc[
        (robustness_df["weighting_scheme"] == weighting_scheme)
        & (robustness_df["portfolio_mode"] == portfolio_mode)
    ] if not robustness_df.empty else pd.DataFrame()
    portfolio_returns_df = _safe_read_csv(alpha_dir / "portfolio_returns.csv")
    if not portfolio_returns_df.empty:
        recent_backtest_mean_return = float(
            pd.to_numeric(
                portfolio_returns_df.loc[
                    (portfolio_returns_df["weighting_scheme"] == weighting_scheme)
                    & (portfolio_returns_df["portfolio_mode"] == portfolio_mode),
                    "portfolio_return_net",
                ],
                errors="coerce",
            ).tail(10).mean()
        )
    else:
        recent_backtest_mean_return = float("nan")
    return {
        "weighting_scheme": weighting_scheme,
        "portfolio_mode": portfolio_mode,
        "expected_mean_turnover": float(metric_row["mean_turnover"].iloc[0]) if not metric_row.empty else float("nan"),
        "expected_recent_mean_return": recent_backtest_mean_return,
        "expected_mean_active_positions": float(metric_row["mean_active_positions"].iloc[0]) if not metric_row.empty else float("nan"),
        "expected_top_position_weight": float(robustness_row["mean_top_position_weight"].iloc[0]) if not robustness_row.empty else float("nan"),
        "expected_gross_exposure": float(robustness_row["mean_gross_exposure"].iloc[0]) if not robustness_row.empty else float("nan"),
        "expected_net_exposure": float(robustness_row["mean_net_exposure"].iloc[0]) if not robustness_row.empty else float("nan"),
    }


def _load_recent_paper_target_weights(
    registry_df: pd.DataFrame,
    *,
    recent_paper_runs: int,
) -> pd.DataFrame:
    paper_runs = registry_df.loc[registry_df["experiment_type"] == "paper_trading"].copy()
    if paper_runs.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "effective_target_weight"])
    paper_runs["timestamp"] = pd.to_datetime(paper_runs["timestamp"], errors="coerce")
    paper_runs = paper_runs.sort_values("timestamp").tail(recent_paper_runs)

    frames: list[pd.DataFrame] = []
    for _, row in paper_runs.iterrows():
        artifact_dir = Path(str(row["artifact_dir"]))
        weights_df = _safe_read_csv(artifact_dir / "paper_target_weights.csv")
        if weights_df.empty:
            continue
        weights_df = weights_df.copy()
        weights_df["timestamp"] = pd.Timestamp(row["timestamp"])
        if "effective_target_weight" not in weights_df.columns:
            continue
        frames.append(weights_df[["timestamp", "symbol", "effective_target_weight"]])
    if not frames:
        return pd.DataFrame(columns=["timestamp", "symbol", "effective_target_weight"])
    return pd.concat(frames, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def _summarize_realized_portfolio_behavior(weights_df: pd.DataFrame) -> dict[str, float]:
    if weights_df.empty:
        return {
            "realized_mean_turnover": float("nan"),
            "realized_mean_position_count": float("nan"),
            "realized_mean_gross_exposure": float("nan"),
            "realized_mean_net_exposure": float("nan"),
            "realized_mean_top_position_weight": float("nan"),
        }

    pivoted = (
        weights_df.pivot(index="timestamp", columns="symbol", values="effective_target_weight")
        .sort_index()
        .sort_index(axis=1)
        .fillna(0.0)
    )
    turnover = pivoted.diff().abs().sum(axis=1).fillna(0.0)
    position_count = (pivoted.abs() > 0).sum(axis=1)
    gross_exposure = pivoted.abs().sum(axis=1)
    net_exposure = pivoted.sum(axis=1)
    top_weight = pivoted.abs().max(axis=1)
    return {
        "realized_mean_turnover": float(turnover.mean()),
        "realized_mean_position_count": float(position_count.mean()),
        "realized_mean_gross_exposure": float(gross_exposure.mean()),
        "realized_mean_net_exposure": float(net_exposure.mean()),
        "realized_mean_top_position_weight": float(top_weight.mean()),
    }


def _recent_paper_performance(registry_df: pd.DataFrame, *, recent_paper_runs: int) -> float:
    paper_runs = registry_df.loc[
        (registry_df["experiment_type"] == "paper_trading")
        & pd.notna(registry_df["paper_equity"])
    ].copy()
    if len(paper_runs) < 2:
        return float("nan")
    paper_runs["timestamp"] = pd.to_datetime(paper_runs["timestamp"], errors="coerce")
    paper_runs = paper_runs.sort_values("timestamp").tail(recent_paper_runs)
    paper_runs["paper_return"] = pd.to_numeric(paper_runs["paper_equity"], errors="coerce").pct_change()
    return float(paper_runs["paper_return"].dropna().mean())


def _latest_regime_shift(alpha_dir: Path) -> dict[str, object]:
    regime_labels_df = _safe_read_csv(alpha_dir / "regime_labels_by_date.csv")
    if regime_labels_df.empty or "timestamp" not in regime_labels_df.columns:
        return {"latest_regime": "", "prior_regime": "", "regime_shift": False}
    regime_labels_df["timestamp"] = pd.to_datetime(regime_labels_df["timestamp"], errors="coerce")
    regime_labels_df = regime_labels_df.dropna(subset=["timestamp"]).sort_values("timestamp")
    if regime_labels_df.empty:
        return {"latest_regime": "", "prior_regime": "", "regime_shift": False}
    latest_regime = str(regime_labels_df.iloc[-1].get("regime", ""))
    prior_regime = str(regime_labels_df.iloc[-2].get("regime", "")) if len(regime_labels_df) > 1 else ""
    return {
        "latest_regime": latest_regime,
        "prior_regime": prior_regime,
        "regime_shift": bool(prior_regime and latest_regime != prior_regime),
    }


def _build_alert_rows(
    *,
    timestamp: datetime,
    expected: dict[str, float | str],
    realized: dict[str, float],
    recent_paper_mean_return: float,
    regime_shift: dict[str, object],
    snapshot_diff: dict[str, object],
    config: MonitoringConfig,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    def add_alert(
        alert_type: str,
        severity: str,
        metric: str,
        current_value: object,
        expected_value: object,
        details: object,
    ) -> None:
        rows.append(
            {
                "timestamp": timestamp.isoformat(),
                "alert_type": alert_type,
                "severity": severity,
                "metric": metric,
                "current_value": json.dumps(current_value, default=str),
                "expected_value": json.dumps(expected_value, default=str),
                "details": json.dumps(details, default=str),
            }
        )

    expected_return = expected.get("expected_recent_mean_return")
    if pd.notna(expected_return) and pd.notna(recent_paper_mean_return):
        if float(recent_paper_mean_return) < float(expected_return) - config.performance_degradation_buffer:
            add_alert(
                "performance_degradation",
                "high",
                "recent_mean_return",
                recent_paper_mean_return,
                expected_return,
                {"buffer": config.performance_degradation_buffer},
            )

    expected_turnover = expected.get("expected_mean_turnover")
    realized_turnover = realized.get("realized_mean_turnover")
    if pd.notna(expected_turnover) and pd.notna(realized_turnover) and float(expected_turnover) > 0:
        if float(realized_turnover) > float(expected_turnover) * config.turnover_spike_multiple:
            add_alert(
                "turnover_spike",
                "medium",
                "mean_turnover",
                realized_turnover,
                expected_turnover,
                {"multiple": config.turnover_spike_multiple},
            )

    expected_top_weight = expected.get("expected_top_position_weight")
    realized_top_weight = realized.get("realized_mean_top_position_weight")
    if pd.notna(expected_top_weight) and pd.notna(realized_top_weight) and float(expected_top_weight) > 0:
        if float(realized_top_weight) > float(expected_top_weight) * config.concentration_spike_multiple:
            add_alert(
                "concentration_increase",
                "medium",
                "top_position_weight",
                realized_top_weight,
                expected_top_weight,
                {"multiple": config.concentration_spike_multiple},
            )

    if regime_shift.get("regime_shift"):
        add_alert(
            "regime_shift",
            "low",
            "regime",
            regime_shift.get("latest_regime", ""),
            regime_shift.get("prior_regime", ""),
            {},
        )

    signal_churn = len(snapshot_diff.get("promoted_signals_added", [])) + len(snapshot_diff.get("promoted_signals_removed", []))
    if signal_churn >= config.signal_churn_threshold:
        add_alert(
            "signal_churn",
            "medium",
            "signal_changes",
            signal_churn,
            config.signal_churn_threshold,
            snapshot_diff,
        )

    return pd.DataFrame(rows, columns=DRIFT_ALERT_COLUMNS)


def build_monitoring_report(
    *,
    config: MonitoringConfig,
) -> dict[str, str]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    registry_df = load_experiment_registry(config.tracker_dir / "experiment_registry.csv")
    alpha_dir = config.alpha_artifact_dir or _select_latest_artifact_dir(registry_df, experiment_type="alpha_research")
    paper_dir = config.paper_artifact_dir or _select_latest_artifact_dir(registry_df, experiment_type="paper_trading")
    snapshot_diff = (
        show_current_vs_previous_configuration(snapshot_dir=config.snapshot_dir)
        if config.snapshot_dir is not None
        else {"differences_vs_previous": {}}
    )

    timestamp = datetime.now(UTC)
    report_path = config.output_dir / "monitoring_report.json"
    alerts_csv_path = config.output_dir / "drift_alerts.csv"
    alerts_parquet_path = config.output_dir / "drift_alerts.parquet"

    if alpha_dir is None or paper_dir is None:
        empty_alerts = pd.DataFrame(columns=DRIFT_ALERT_COLUMNS)
        empty_alerts.to_csv(alerts_csv_path, index=False)
        empty_alerts.to_parquet(alerts_parquet_path, index=False)
        payload = {
            "timestamp": timestamp.isoformat(),
            "alpha_artifact_dir": str(alpha_dir) if alpha_dir else "",
            "paper_artifact_dir": str(paper_dir) if paper_dir else "",
            "paper_vs_backtest": {},
            "realized_vs_expected": {},
            "signal_set_comparison": snapshot_diff.get("differences_vs_previous", {}),
            "diagnostics": {"missing_artifacts": [str(path) for path in [alpha_dir, paper_dir] if path is None]},
            "alerts": [],
        }
        report_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return {
            "monitoring_report_path": str(report_path),
            "drift_alerts_path": str(alerts_csv_path),
            "drift_alerts_parquet_path": str(alerts_parquet_path),
        }

    expected = _expected_alpha_metrics(alpha_dir)
    weights_df = _load_recent_paper_target_weights(
        registry_df,
        recent_paper_runs=config.recent_paper_runs,
    )
    realized = _summarize_realized_portfolio_behavior(weights_df)
    recent_paper_mean_return = _recent_paper_performance(
        registry_df,
        recent_paper_runs=config.recent_paper_runs,
    )
    regime_shift = _latest_regime_shift(alpha_dir)
    alerts_df = _build_alert_rows(
        timestamp=timestamp,
        expected=expected,
        realized=realized,
        recent_paper_mean_return=recent_paper_mean_return,
        regime_shift=regime_shift,
        snapshot_diff=snapshot_diff.get("differences_vs_previous", {}),
        config=config,
    )
    alerts_df.to_csv(alerts_csv_path, index=False)
    alerts_df.to_parquet(alerts_parquet_path, index=False)

    active_signal_set = _safe_read_json(paper_dir / "composite_diagnostics.json").get("selected_signals", [])
    report_payload = {
        "timestamp": timestamp.isoformat(),
        "alpha_artifact_dir": str(alpha_dir),
        "paper_artifact_dir": str(paper_dir),
        "paper_vs_backtest": {
            "recent_paper_mean_return": recent_paper_mean_return,
            "expected_recent_backtest_mean_return": expected.get("expected_recent_mean_return"),
        },
        "realized_vs_expected": {
            **realized,
            **expected,
        },
        "signal_set_comparison": {
            "active_paper_signals": active_signal_set,
            "current_vs_previous_approved": snapshot_diff.get("differences_vs_previous", {}),
        },
        "regime_monitoring": regime_shift,
        "diagnostics": {
            "recent_paper_runs": config.recent_paper_runs,
            "weights_rows_loaded": int(len(weights_df)),
        },
        "alerts": alerts_df.to_dict(orient="records"),
    }
    report_path.write_text(json.dumps(report_payload, indent=2, default=str), encoding="utf-8")
    return {
        "monitoring_report_path": str(report_path),
        "drift_alerts_path": str(alerts_csv_path),
        "drift_alerts_parquet_path": str(alerts_parquet_path),
    }
