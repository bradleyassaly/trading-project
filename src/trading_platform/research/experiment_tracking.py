from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


EXPERIMENT_REGISTRY_COLUMNS = [
    "experiment_id",
    "experiment_type",
    "run_id",
    "timestamp",
    "artifact_dir",
    "config_fingerprint",
    "duplicate_of",
    "signal_family",
    "parameters_json",
    "promotion_status",
    "promoted_signal_count",
    "rejected_signal_count",
    "composite_config_json",
    "regime_config_json",
    "portfolio_weighting_scheme",
    "portfolio_mode",
    "portfolio_total_return",
    "portfolio_sharpe",
    "portfolio_max_drawdown",
    "robustness_worst_fold_return",
    "robustness_worst_fold_sharpe",
    "implementability_return_drag",
    "implementability_mean_capacity_multiple",
    "paper_signal_source",
    "paper_equity",
    "paper_order_count",
    "paper_fill_count",
    "paper_vs_backtest_return_gap",
    "artifacts_json",
]


def load_experiment_registry(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=EXPERIMENT_REGISTRY_COLUMNS)
    return pd.read_csv(path)


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


def _json_dumps(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, default=str)


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _stable_hash(payload: object) -> str:
    return hashlib.sha1(_json_dumps(payload).encode("utf-8")).hexdigest()


def _best_portfolio_row(portfolio_metrics_df: pd.DataFrame) -> pd.Series | None:
    if portfolio_metrics_df.empty:
        return None
    ordered = portfolio_metrics_df.sort_values(
        ["portfolio_sharpe", "portfolio_total_return"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)
    return ordered.iloc[0]


def _first_row(frame: pd.DataFrame) -> pd.Series | None:
    if frame.empty:
        return None
    return frame.iloc[0]


def _compare_model_states(current_payload: dict[str, object], prior_payload: dict[str, object]) -> list[dict[str, object]]:
    current_promoted = {
        json.dumps(item, sort_keys=True)
        for item in current_payload.get("promoted_signals", [])
    }
    prior_promoted = {
        json.dumps(item, sort_keys=True)
        for item in prior_payload.get("promoted_signals", [])
    }
    diffs = []
    added = sorted(current_promoted - prior_promoted)
    removed = sorted(prior_promoted - current_promoted)
    if added:
        diffs.append({"field": "promoted_signals_added", "value": len(added)})
    if removed:
        diffs.append({"field": "promoted_signals_removed", "value": len(removed)})
    if current_payload.get("composite_config") != prior_payload.get("composite_config"):
        diffs.append({"field": "composite_config_changed", "value": True})
    if current_payload.get("regime_config") != prior_payload.get("regime_config"):
        diffs.append({"field": "regime_config_changed", "value": True})
    return diffs


def build_alpha_experiment_record(artifact_dir: Path) -> dict[str, object]:
    diagnostics = _safe_read_json(artifact_dir / "signal_diagnostics.json")
    composite_diagnostics = _safe_read_json(artifact_dir / "composite_diagnostics.json")
    leaderboard_df = _safe_read_csv(artifact_dir / "leaderboard.csv")
    promoted_signals_df = _safe_read_csv(artifact_dir / "promoted_signals.csv")
    portfolio_metrics_df = _safe_read_csv(artifact_dir / "portfolio_metrics.csv")
    robustness_report_df = _safe_read_csv(artifact_dir / "robustness_report.csv")
    implementability_report_df = _safe_read_csv(artifact_dir / "implementability_report.csv")
    regime_performance_df = _safe_read_csv(artifact_dir / "regime_performance.csv")

    run_timestamp = str(
        diagnostics.get("run_timestamp")
        or datetime.fromtimestamp(artifact_dir.stat().st_mtime, tz=UTC).isoformat()
    )
    composite_config = composite_diagnostics.get("config", {})
    regime_config = diagnostics.get("regime", {})
    promoted_signals = promoted_signals_df[
        [column for column in ["signal_family", "lookback", "horizon", "candidate_id"] if column in promoted_signals_df.columns]
    ].to_dict(orient="records") if not promoted_signals_df.empty else []
    best_portfolio_row = _best_portfolio_row(portfolio_metrics_df)
    robustness_sort_columns = [
        column
        for column in ["mean_fold_return", "worst_fold_return"]
        if column in robustness_report_df.columns
    ]
    robustness_row = _first_row(
        robustness_report_df.sort_values(
            robustness_sort_columns,
            ascending=[False] * len(robustness_sort_columns),
            na_position="last",
        )
        if robustness_sort_columns
        else robustness_report_df
    )
    implementability_row = _first_row(implementability_report_df)

    config_payload = {
        "signal_family": diagnostics.get("signal_family"),
        "lookbacks": diagnostics.get("lookbacks"),
        "horizons": diagnostics.get("horizons"),
        "promotion_rules": diagnostics.get("promotion_rules"),
        "signal_lifecycle": diagnostics.get("signal_lifecycle"),
        "composite_config": composite_config,
        "regime_config": regime_config,
        "promoted_signals": promoted_signals,
    }
    return {
        "experiment_id": _stable_hash({"artifact_dir": str(artifact_dir), "timestamp": run_timestamp, "type": "alpha_research"}),
        "experiment_type": "alpha_research",
        "run_id": str(diagnostics.get("run_id") or _stable_hash({"artifact_dir": str(artifact_dir), "timestamp": run_timestamp})[:12]),
        "timestamp": run_timestamp,
        "artifact_dir": str(artifact_dir),
        "config_fingerprint": _stable_hash(config_payload),
        "duplicate_of": "",
        "signal_family": str(diagnostics.get("signal_family") or ""),
        "parameters_json": _json_dumps(
            {
                "lookbacks": diagnostics.get("lookbacks", []),
                "horizons": diagnostics.get("horizons", []),
            }
        ),
        "promotion_status": "approved" if not promoted_signals_df.empty else "none_promoted",
        "promoted_signal_count": int(len(promoted_signals_df)),
        "rejected_signal_count": int(
            len(leaderboard_df.loc[leaderboard_df.get("promotion_status", pd.Series(dtype="object")) != "promote"])
        ) if not leaderboard_df.empty else 0,
        "composite_config_json": _json_dumps(composite_config),
        "regime_config_json": _json_dumps(regime_config),
        "portfolio_weighting_scheme": str(best_portfolio_row["weighting_scheme"]) if best_portfolio_row is not None else "",
        "portfolio_mode": str(best_portfolio_row["portfolio_mode"]) if best_portfolio_row is not None else "",
        "portfolio_total_return": float(best_portfolio_row["portfolio_total_return"]) if best_portfolio_row is not None else float("nan"),
        "portfolio_sharpe": float(best_portfolio_row["portfolio_sharpe"]) if best_portfolio_row is not None else float("nan"),
        "portfolio_max_drawdown": float(best_portfolio_row["portfolio_max_drawdown"]) if best_portfolio_row is not None else float("nan"),
        "robustness_worst_fold_return": float(robustness_row["worst_fold_return"]) if robustness_row is not None and "worst_fold_return" in robustness_row else float("nan"),
        "robustness_worst_fold_sharpe": float(robustness_row["worst_fold_sharpe"]) if robustness_row is not None and "worst_fold_sharpe" in robustness_row else float("nan"),
        "implementability_return_drag": float(implementability_row["return_drag"]) if implementability_row is not None and "return_drag" in implementability_row else float("nan"),
        "implementability_mean_capacity_multiple": float(implementability_row["mean_capacity_multiple"]) if implementability_row is not None and "mean_capacity_multiple" in implementability_row else float("nan"),
        "paper_signal_source": "",
        "paper_equity": float("nan"),
        "paper_order_count": float("nan"),
        "paper_fill_count": float("nan"),
        "paper_vs_backtest_return_gap": float("nan"),
        "artifacts_json": _json_dumps(
            {
                "leaderboard": str(artifact_dir / "leaderboard.csv"),
                "promoted_signals": str(artifact_dir / "promoted_signals.csv"),
                "regime_performance": str(artifact_dir / "regime_performance.csv") if not regime_performance_df.empty else "",
                "portfolio_metrics": str(artifact_dir / "portfolio_metrics.csv"),
                "robustness_report": str(artifact_dir / "robustness_report.csv"),
                "implementability_report": str(artifact_dir / "implementability_report.csv"),
            }
        ),
    }


def build_automated_alpha_loop_experiment_record(artifact_dir: Path) -> dict[str, object]:
    run_summary = _safe_read_json(artifact_dir / "research_loop_run_summary.json")
    config = _safe_read_json(artifact_dir / "research_loop_config.json")
    candidate_grid_manifest = _safe_read_json(artifact_dir / "candidate_grid_manifest.json")
    allocation_summary = _safe_read_json(artifact_dir / "candidate_allocation_summary.json")
    promotion_diagnostics = _safe_read_json(artifact_dir / "promotion_threshold_diagnostics.json")
    promoted_df = _safe_read_csv(artifact_dir / "promoted_signals.csv")
    rejected_df = _safe_read_csv(artifact_dir / "rejected_signals.csv")

    run_id = str(run_summary.get("run_id") or _stable_hash({"artifact_dir": str(artifact_dir), "type": "automated_alpha_research_loop"})[:12])
    timestamp = str(run_summary.get("artifact_paths", {}).get("last_run_at") or run_summary.get("last_run_at") or datetime.fromtimestamp(artifact_dir.stat().st_mtime, tz=UTC).isoformat())
    signal_families = candidate_grid_manifest.get("candidate_families", [])
    signal_family = (
        str(signal_families[0])
        if len(signal_families) == 1
        else "multi_family"
    )
    config_payload = {
        "generation_config": config.get("generation_config"),
        "resource_allocation": config.get("resource_allocation"),
        "schedule_frequency": config.get("schedule_frequency"),
        "universe": config.get("universe"),
        "search_spaces": config.get("search_spaces"),
    }
    promoted_signal_count = int(run_summary.get("promoted_candidates") or len(promoted_df))
    rejected_signal_count = int(run_summary.get("rejected_candidates") or len(rejected_df))
    mean_rank_ic = _safe_float(
        promotion_diagnostics.get("mean_rank_ic_distribution", {}).get("mean")
    )

    return {
        "experiment_id": _stable_hash({"artifact_dir": str(artifact_dir), "run_id": run_id, "type": "alpha_research_loop"}),
        "experiment_type": "alpha_research_loop",
        "run_id": run_id,
        "timestamp": timestamp,
        "artifact_dir": str(artifact_dir),
        "config_fingerprint": _stable_hash(config_payload),
        "duplicate_of": "",
        "signal_family": signal_family,
        "parameters_json": _json_dumps(
            {
                "signal_families": signal_families,
                "candidate_count": candidate_grid_manifest.get("candidate_count"),
                "resource_allocation": config.get("resource_allocation", {}),
            }
        ),
        "promotion_status": "approved" if promoted_signal_count > 0 else "none_promoted",
        "promoted_signal_count": promoted_signal_count,
        "rejected_signal_count": rejected_signal_count,
        "composite_config_json": _json_dumps(
            {
                "candidate_families": signal_families,
                "allocation_summary": allocation_summary,
            }
        ),
        "regime_config_json": _json_dumps({}),
        "portfolio_weighting_scheme": "",
        "portfolio_mode": "",
        "portfolio_total_return": float("nan"),
        "portfolio_sharpe": float("nan"),
        "portfolio_max_drawdown": float("nan"),
        "robustness_worst_fold_return": float("nan"),
        "robustness_worst_fold_sharpe": float("nan"),
        "implementability_return_drag": float("nan"),
        "implementability_mean_capacity_multiple": float("nan"),
        "paper_signal_source": "",
        "paper_equity": float("nan"),
        "paper_order_count": float("nan"),
        "paper_fill_count": float("nan"),
        "paper_vs_backtest_return_gap": float("nan"),
        "artifacts_json": _json_dumps(
            {
                "run_summary": str(artifact_dir / "research_loop_run_summary.json"),
                "config": str(artifact_dir / "research_loop_config.json"),
                "candidate_grid_manifest": str(artifact_dir / "candidate_grid_manifest.json"),
                "candidate_allocation_summary": str(artifact_dir / "candidate_allocation_summary.json"),
                "promoted_signals": str(artifact_dir / "promoted_signals.csv"),
                "rejected_signals": str(artifact_dir / "rejected_signals.csv"),
                "mean_spearman_ic": mean_rank_ic,
            }
        ),
    }


def build_paper_experiment_record(artifact_dir: Path) -> dict[str, object]:
    summary = _safe_read_json(artifact_dir / "paper_summary.json")
    diagnostics = summary.get("diagnostics", {}) if isinstance(summary, dict) else {}
    composite_diagnostics = _safe_read_json(artifact_dir / "composite_diagnostics.json")
    config_payload = {
        "signal_source": diagnostics.get("signal_source"),
        "target_construction": diagnostics.get("target_construction"),
        "composite": {
            "selected_signals": composite_diagnostics.get("selected_signals", []),
            "weighting_scheme": composite_diagnostics.get("weighting_scheme"),
            "portfolio_mode": composite_diagnostics.get("portfolio_mode"),
            "horizon": composite_diagnostics.get("horizon"),
        },
    }
    fallback_timestamp = (
        datetime.fromtimestamp(artifact_dir.stat().st_mtime, tz=UTC).isoformat()
        if artifact_dir.exists()
        else datetime.now(UTC).isoformat()
    )
    timestamp = str(summary.get("as_of") or fallback_timestamp)
    return {
        "experiment_id": _stable_hash({"artifact_dir": str(artifact_dir), "timestamp": timestamp, "type": "paper_trading"}),
        "experiment_type": "paper_trading",
        "run_id": _stable_hash({"artifact_dir": str(artifact_dir), "timestamp": timestamp})[:12],
        "timestamp": timestamp,
        "artifact_dir": str(artifact_dir),
        "config_fingerprint": _stable_hash(config_payload),
        "duplicate_of": "",
        "signal_family": "",
        "parameters_json": _json_dumps({}),
        "promotion_status": "",
        "promoted_signal_count": len(composite_diagnostics.get("selected_signals", [])) if isinstance(composite_diagnostics, dict) else 0,
        "rejected_signal_count": 0,
        "composite_config_json": _json_dumps(composite_diagnostics),
        "regime_config_json": _json_dumps({}),
        "portfolio_weighting_scheme": str(composite_diagnostics.get("weighting_scheme") or ""),
        "portfolio_mode": str(composite_diagnostics.get("portfolio_mode") or ""),
        "portfolio_total_return": float("nan"),
        "portfolio_sharpe": float("nan"),
        "portfolio_max_drawdown": float("nan"),
        "robustness_worst_fold_return": float("nan"),
        "robustness_worst_fold_sharpe": float("nan"),
        "implementability_return_drag": float("nan"),
        "implementability_mean_capacity_multiple": float("nan"),
        "paper_signal_source": str(diagnostics.get("signal_source") or ""),
        "paper_equity": float(summary.get("equity")) if summary.get("equity") is not None else float("nan"),
        "paper_order_count": float(len(summary.get("orders", []))) if isinstance(summary, dict) else float("nan"),
        "paper_fill_count": float(len(summary.get("fills", []))) if isinstance(summary, dict) else float("nan"),
        "paper_vs_backtest_return_gap": float("nan"),
        "artifacts_json": _json_dumps(
            {
                "paper_summary": str(artifact_dir / "paper_summary.json"),
                "paper_orders": str(artifact_dir / "paper_orders.csv"),
                "paper_targets": str(artifact_dir / "paper_target_weights.csv"),
                "daily_composite_scores": str(artifact_dir / "daily_composite_scores.csv"),
            }
        ),
    }


def _update_duplicate_fields(record: dict[str, object], registry_df: pd.DataFrame) -> dict[str, object]:
    updated = dict(record)
    if registry_df.empty:
        return updated
    matches = registry_df.loc[
        (registry_df["experiment_type"] == updated["experiment_type"])
        & (registry_df["config_fingerprint"] == updated["config_fingerprint"])
    ]
    if matches.empty:
        return updated
    updated["duplicate_of"] = str(matches.iloc[0]["experiment_id"])
    return updated


def register_experiment(
    record: dict[str, object],
    *,
    tracker_dir: Path,
) -> dict[str, str]:
    tracker_dir.mkdir(parents=True, exist_ok=True)
    registry_csv_path = tracker_dir / "experiment_registry.csv"
    registry_parquet_path = tracker_dir / "experiment_registry.parquet"
    registry_json_path = tracker_dir / "experiment_registry.json"

    registry_df = load_experiment_registry(registry_csv_path)
    record = _update_duplicate_fields(record, registry_df)
    updated_registry = pd.concat(
        [registry_df, pd.DataFrame([record], columns=EXPERIMENT_REGISTRY_COLUMNS)],
        ignore_index=True,
    ).reindex(columns=EXPERIMENT_REGISTRY_COLUMNS)
    updated_registry.to_csv(registry_csv_path, index=False)
    updated_registry.to_parquet(registry_parquet_path, index=False)
    registry_json_path.write_text(
        json.dumps(updated_registry.to_dict(orient="records"), indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "experiment_registry_path": str(registry_csv_path),
        "experiment_registry_parquet_path": str(registry_parquet_path),
        "experiment_registry_json_path": str(registry_json_path),
    }


def build_latest_model_state(tracker_dir: Path) -> dict[str, object]:
    registry_df = load_experiment_registry(tracker_dir / "experiment_registry.csv")
    alpha_runs = registry_df.loc[registry_df["experiment_type"] == "alpha_research"].copy()
    if alpha_runs.empty:
        return {"latest_approved_experiment": None, "differences_vs_prior": []}

    alpha_runs["timestamp"] = pd.to_datetime(alpha_runs["timestamp"], errors="coerce")
    approved = alpha_runs.loc[alpha_runs["promoted_signal_count"] > 0].sort_values("timestamp")
    if approved.empty:
        latest = alpha_runs.sort_values("timestamp").iloc[-1]
        return {"latest_approved_experiment": latest.to_dict(), "differences_vs_prior": []}

    latest = approved.iloc[-1]
    latest_artifact_dir = Path(str(latest["artifact_dir"]))
    promoted_signals_df = _safe_read_csv(latest_artifact_dir / "promoted_signals.csv")
    composite_diagnostics = _safe_read_json(latest_artifact_dir / "composite_diagnostics.json")
    signal_diagnostics = _safe_read_json(latest_artifact_dir / "signal_diagnostics.json")
    latest_payload = {
        "experiment_id": latest["experiment_id"],
        "timestamp": str(latest["timestamp"]),
        "artifact_dir": str(latest_artifact_dir),
        "promoted_signals": promoted_signals_df.to_dict(orient="records"),
        "composite_config": composite_diagnostics.get("config", {}),
        "regime_config": signal_diagnostics.get("regime", {}),
    }
    prior_payload: dict[str, object] = {}
    if len(approved) > 1:
        prior = approved.iloc[-2]
        prior_artifact_dir = Path(str(prior["artifact_dir"]))
        prior_payload = {
            "experiment_id": prior["experiment_id"],
            "promoted_signals": _safe_read_csv(prior_artifact_dir / "promoted_signals.csv").to_dict(orient="records"),
            "composite_config": _safe_read_json(prior_artifact_dir / "composite_diagnostics.json").get("config", {}),
            "regime_config": _safe_read_json(prior_artifact_dir / "signal_diagnostics.json").get("regime", {}),
        }
    return {
        "latest_approved_experiment": latest_payload,
        "differences_vs_prior": _compare_model_states(latest_payload, prior_payload) if prior_payload else [],
    }


def build_experiment_summary_report(
    *,
    tracker_dir: Path,
    output_dir: Path | None = None,
    top_metric: str = "portfolio_sharpe",
    limit: int = 10,
) -> dict[str, str]:
    tracker_dir.mkdir(parents=True, exist_ok=True)
    output_dir = output_dir or tracker_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    registry_df = load_experiment_registry(tracker_dir / "experiment_registry.csv")
    report_path = output_dir / "experiment_summary_report.json"
    latest_model_state_path = output_dir / "latest_model_state.json"

    if registry_df.empty:
        empty_payload = {
            "top_experiments": [],
            "latest_promoted_signals": [],
            "active_composite_configuration": {},
            "performance_by_regime": [],
            "robustness_comparison": [],
            "paper_vs_backtest_comparison": [],
            "diagnostics": {"duplicates_detected": 0, "missing_artifacts": [], "differences_vs_prior": []},
        }
        report_path.write_text(json.dumps(empty_payload, indent=2, default=str), encoding="utf-8")
        latest_model_state_path.write_text(
            json.dumps({"latest_approved_experiment": None, "differences_vs_prior": []}, indent=2, default=str),
            encoding="utf-8",
        )
        return {
            "experiment_summary_report_path": str(report_path),
            "latest_model_state_path": str(latest_model_state_path),
        }

    registry_df["timestamp"] = pd.to_datetime(registry_df["timestamp"], errors="coerce")
    alpha_runs = registry_df.loc[registry_df["experiment_type"] == "alpha_research"].copy()
    paper_runs = registry_df.loc[registry_df["experiment_type"] == "paper_trading"].copy()
    latest_alpha = alpha_runs.sort_values("timestamp").iloc[-1] if not alpha_runs.empty else None
    latest_alpha_dir = Path(str(latest_alpha["artifact_dir"])) if latest_alpha is not None else None

    latest_promoted_signals = (
        _safe_read_csv(latest_alpha_dir / "promoted_signals.csv").to_dict(orient="records")
        if latest_alpha_dir is not None
        else []
    )
    active_composite_configuration = (
        _safe_read_json(latest_alpha_dir / "composite_diagnostics.json")
        if latest_alpha_dir is not None
        else {}
    )
    performance_by_regime = (
        _safe_read_csv(latest_alpha_dir / "regime_performance.csv").to_dict(orient="records")
        if latest_alpha_dir is not None
        else []
    )
    top_experiments = (
        alpha_runs.sort_values([top_metric, "timestamp"], ascending=[False, False], na_position="last")
        .head(limit)
        .fillna("")
        .to_dict(orient="records")
        if not alpha_runs.empty and top_metric in alpha_runs.columns
        else []
    )
    robustness_comparison = (
        alpha_runs[
            [
                "experiment_id",
                "timestamp",
                "portfolio_total_return",
                "portfolio_sharpe",
                "robustness_worst_fold_return",
                "robustness_worst_fold_sharpe",
                "implementability_return_drag",
            ]
        ]
        .sort_values("timestamp", ascending=False)
        .fillna("")
        .to_dict(orient="records")
        if not alpha_runs.empty
        else []
    )
    paper_vs_backtest = []
    if latest_alpha is not None and not paper_runs.empty:
        latest_paper = paper_runs.sort_values("timestamp").iloc[-1]
        paper_vs_backtest.append(
            {
                "paper_experiment_id": latest_paper["experiment_id"],
                "backtest_experiment_id": latest_alpha["experiment_id"],
                "paper_equity": latest_paper["paper_equity"],
                "backtest_total_return": latest_alpha["portfolio_total_return"],
                "paper_signal_source": latest_paper["paper_signal_source"],
            }
        )

    missing_artifacts: list[str] = []
    for _, row in registry_df.iterrows():
        artifact_dir = Path(str(row["artifact_dir"]))
        if not artifact_dir.exists():
            missing_artifacts.append(str(artifact_dir))

    latest_model_state = build_latest_model_state(tracker_dir)
    report_payload = {
        "top_experiments": top_experiments,
        "latest_promoted_signals": latest_promoted_signals,
        "active_composite_configuration": active_composite_configuration,
        "performance_by_regime": performance_by_regime,
        "robustness_comparison": robustness_comparison,
        "paper_vs_backtest_comparison": paper_vs_backtest,
        "diagnostics": {
            "duplicates_detected": int((registry_df["duplicate_of"].fillna("") != "").sum()),
            "missing_artifacts": missing_artifacts,
            "differences_vs_prior": latest_model_state.get("differences_vs_prior", []),
        },
    }
    report_path.write_text(json.dumps(report_payload, indent=2, default=str), encoding="utf-8")
    latest_model_state_path.write_text(json.dumps(latest_model_state, indent=2, default=str), encoding="utf-8")
    return {
        "experiment_summary_report_path": str(report_path),
        "latest_model_state_path": str(latest_model_state_path),
    }


def list_recent_experiments(*, tracker_dir: Path, limit: int = 10) -> pd.DataFrame:
    registry_df = load_experiment_registry(tracker_dir / "experiment_registry.csv")
    if registry_df.empty:
        return registry_df
    registry_df["timestamp"] = pd.to_datetime(registry_df["timestamp"], errors="coerce")
    return registry_df.sort_values("timestamp", ascending=False).head(limit).reset_index(drop=True)
