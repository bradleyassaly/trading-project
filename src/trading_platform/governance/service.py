from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.models import (
    MultiStrategyPortfolioConfig,
    MultiStrategySleeveConfig,
)
from trading_platform.governance.models import (
    CriteriaComparison,
    DegradationCriteria,
    GovernanceDecisionReport,
    PromotionCriteria,
    RegistrySelectionOptions,
    StrategyMetricsSnapshot,
    StrategyRegistry,
    StrategyRegistryAuditEvent,
    StrategyRegistryEntry,
)
from trading_platform.governance.persistence import (
    append_audit_event,
    get_registry_entry,
    save_strategy_registry,
    upsert_registry_entry,
    validate_status_transition,
)

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: Path) -> dict[str, Any]:
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


def _resolve_artifact_dir(path_or_dir: str | Path) -> Path:
    path = Path(path_or_dir)
    return path.parent if path.is_file() else path


def _best_metric_row(frame: pd.DataFrame) -> pd.Series | None:
    if frame.empty:
        return None
    sort_columns = [
        column
        for column in ["portfolio_sharpe", "portfolio_total_return", "mean_test_return", "sharpe"]
        if column in frame.columns
    ]
    if not sort_columns:
        return frame.iloc[0]
    ordered = frame.sort_values(
        sort_columns,
        ascending=[False] * len(sort_columns),
        na_position="last",
    ).reset_index(drop=True)
    return ordered.iloc[0]


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    return numeric


def extract_strategy_metrics(entry: StrategyRegistryEntry) -> StrategyMetricsSnapshot:
    artifact_dir = _resolve_artifact_dir(entry.research_artifact_paths[0])
    portfolio_metrics = _safe_read_csv(artifact_dir / "portfolio_metrics.csv")
    robustness_report = _safe_read_csv(artifact_dir / "robustness_report.csv")
    implementability_report = _safe_read_csv(artifact_dir / "implementability_report.csv")
    redundancy_report = _safe_read_csv(artifact_dir / "redundancy_report.csv")
    if redundancy_report.empty:
        redundancy_report = _safe_read_csv(artifact_dir / "redundancy_diagnostics.csv")
    leaderboard = _safe_read_csv(artifact_dir / "leaderboard.csv")
    signal_diagnostics = _safe_read_json(artifact_dir / "signal_diagnostics.json")

    best_row = _best_metric_row(portfolio_metrics)
    robustness_row = robustness_report.iloc[0] if not robustness_report.empty else None
    implementability_row = implementability_report.iloc[0] if not implementability_report.empty else None
    leaderboard_row = _best_metric_row(leaderboard)

    paper_dir = _resolve_artifact_dir(entry.paper_artifact_path) if entry.paper_artifact_path else None
    paper_latest = (
        _safe_read_json(paper_dir / "paper_run_summary_latest.json")
        if paper_dir is not None
        else {}
    )
    paper_summary = paper_latest.get("summary", {}) if isinstance(paper_latest, dict) else {}
    paper_health_checks = paper_latest.get("health_checks", []) if isinstance(paper_latest, dict) else []
    paper_orders_history = (
        _safe_read_csv(paper_dir / "paper_orders_history.csv")
        if paper_dir is not None
        else pd.DataFrame()
    )

    live_dir = _resolve_artifact_dir(entry.live_artifact_path) if entry.live_artifact_path else None
    live_latest = (
        _safe_read_json(live_dir / "live_run_summary_latest.json")
        if live_dir is not None and (live_dir / "live_run_summary_latest.json").exists()
        else _safe_read_json(live_dir / "live_dry_run_summary.json") if live_dir is not None else {}
    )
    live_summary = live_latest.get("summary", live_latest) if isinstance(live_latest, dict) else {}
    live_health_checks = live_latest.get("health_checks", []) if isinstance(live_latest, dict) else []

    metrics: dict[str, Any] = {
        "walk_forward_folds": _coerce_int(
            (robustness_row.get("folds_tested") if robustness_row is not None else None)
            or signal_diagnostics.get("folds_tested")
        ),
        "mean_test_return": _coerce_float(
            (best_row.get("portfolio_total_return") if best_row is not None else None)
            or (robustness_row.get("mean_fold_return") if robustness_row is not None else None)
            or signal_diagnostics.get("mean_test_return")
        ),
        "sharpe": _coerce_float(
            (best_row.get("portfolio_sharpe") if best_row is not None else None)
            or (robustness_row.get("mean_fold_sharpe") if robustness_row is not None else None)
            or signal_diagnostics.get("sharpe")
        ),
        "max_drawdown": abs(
            _coerce_float(
                (best_row.get("portfolio_max_drawdown") if best_row is not None else None)
                or signal_diagnostics.get("max_drawdown")
                or 0.0
            )
        ),
        "hit_rate": _coerce_float(
            (best_row.get("hit_rate") if best_row is not None else None)
            or (best_row.get("win_rate") if best_row is not None else None)
            or signal_diagnostics.get("hit_rate")
        ),
        "ic_rank_ic": _coerce_float(
            (leaderboard_row.get("mean_spearman_ic") if leaderboard_row is not None else None)
            or signal_diagnostics.get("mean_spearman_ic")
        ),
        "turnover": _coerce_float(
            (robustness_row.get("mean_turnover") if robustness_row is not None else None)
            or (implementability_row.get("mean_turnover") if implementability_row is not None else None)
            or signal_diagnostics.get("mean_turnover")
        ),
        "redundancy_correlation": _coerce_float(
            redundancy_report[
                [column for column in ["score_corr", "performance_corr", "rank_ic_corr"] if column in redundancy_report.columns]
            ].max().max()
            if not redundancy_report.empty
            else None
        ),
        "paper_observation_days": _coerce_int(
            paper_orders_history["timestamp"].nunique()
            if not paper_orders_history.empty and "timestamp" in paper_orders_history.columns
            else 0
        ),
        "trade_count": _coerce_int(len(paper_orders_history)),
        "paper_turnover_estimate": _coerce_float(paper_summary.get("turnover_estimate")),
        "paper_current_equity": _coerce_float(paper_summary.get("current_equity")),
        "paper_health_failures": sum(1 for item in paper_health_checks if item.get("status") == "fail"),
        "paper_missing_data_failures": sum(
            1
            for item in paper_health_checks
            if item.get("status") == "fail" and "data" in str(item.get("check_name", "")).lower()
        ),
        "rolling_underperformance_vs_benchmark": _coerce_float(
            signal_diagnostics.get("rolling_underperformance_vs_benchmark")
            or paper_summary.get("rolling_underperformance_vs_benchmark")
        ),
        "signal_stability": _coerce_float(
            signal_diagnostics.get("signal_stability")
            or signal_diagnostics.get("stability_score")
        ),
        "live_fail_checks": sum(1 for item in live_health_checks if item.get("status") == "fail"),
        "live_warn_checks": sum(1 for item in live_health_checks if item.get("status") == "warn"),
        "live_readiness": live_summary.get("readiness"),
    }
    return StrategyMetricsSnapshot(
        strategy_id=entry.strategy_id,
        strategy_name=entry.strategy_name,
        family=entry.family,
        version=entry.version,
        preset_name=entry.preset_name,
        timestamp=_now_utc(),
        metrics=metrics,
    )


def _compare_min(
    *,
    criterion: str,
    actual: Any,
    threshold: Any,
) -> CriteriaComparison:
    passed = actual is not None and threshold is not None and actual >= threshold
    return CriteriaComparison(
        criterion=criterion,
        actual=actual,
        threshold=threshold,
        passed=passed,
        message=f"{criterion}: actual={actual} threshold>={threshold}",
    )


def _compare_max(
    *,
    criterion: str,
    actual: Any,
    threshold: Any,
) -> CriteriaComparison:
    passed = actual is not None and threshold is not None and actual <= threshold
    return CriteriaComparison(
        criterion=criterion,
        actual=actual,
        threshold=threshold,
        passed=passed,
        message=f"{criterion}: actual={actual} threshold<={threshold}",
    )


def evaluate_promotion(
    *,
    entry: StrategyRegistryEntry,
    criteria: PromotionCriteria,
) -> tuple[GovernanceDecisionReport, StrategyMetricsSnapshot]:
    snapshot = extract_strategy_metrics(entry)
    metrics = snapshot.metrics
    comparisons: list[CriteriaComparison] = [
        _compare_min(
            criterion="minimum_walk_forward_folds",
            actual=metrics.get("walk_forward_folds"),
            threshold=criteria.minimum_walk_forward_folds,
        ),
    ]
    if criteria.minimum_mean_test_return is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_mean_test_return",
                actual=metrics.get("mean_test_return"),
                threshold=criteria.minimum_mean_test_return,
            )
        )
    if criteria.minimum_sharpe is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_sharpe",
                actual=metrics.get("sharpe"),
                threshold=criteria.minimum_sharpe,
            )
        )
    if criteria.maximum_drawdown is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_drawdown",
                actual=metrics.get("max_drawdown"),
                threshold=criteria.maximum_drawdown,
            )
        )
    if criteria.minimum_hit_rate is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_hit_rate",
                actual=metrics.get("hit_rate"),
                threshold=criteria.minimum_hit_rate,
            )
        )
    if criteria.minimum_ic_rank_ic is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_ic_rank_ic",
                actual=metrics.get("ic_rank_ic"),
                threshold=criteria.minimum_ic_rank_ic,
            )
        )
    if criteria.maximum_turnover is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_turnover",
                actual=metrics.get("turnover"),
                threshold=criteria.maximum_turnover,
            )
        )
    if criteria.maximum_redundancy_correlation is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_redundancy_correlation",
                actual=metrics.get("redundancy_correlation"),
                threshold=criteria.maximum_redundancy_correlation,
            )
        )
    if criteria.minimum_paper_trading_observation_window is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_paper_trading_observation_window",
                actual=metrics.get("paper_observation_days"),
                threshold=criteria.minimum_paper_trading_observation_window,
            )
        )
    if criteria.minimum_trade_count is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_trade_count",
                actual=metrics.get("trade_count"),
                threshold=criteria.minimum_trade_count,
            )
        )

    failed = [comparison.criterion for comparison in comparisons if not comparison.passed]
    passed = not failed
    recommendation = "promote" if passed else "paper-test-longer"
    report = GovernanceDecisionReport(
        strategy_id=entry.strategy_id,
        strategy_name=entry.strategy_name,
        family=entry.family,
        version=entry.version,
        timestamp=_now_utc(),
        decision_type="promotion",
        passed=passed,
        failed_criteria=failed,
        summary_metrics=metrics,
        comparisons=comparisons,
        recommendation=recommendation,
    )
    return report, snapshot


def evaluate_degradation(
    *,
    entry: StrategyRegistryEntry,
    criteria: DegradationCriteria,
) -> tuple[GovernanceDecisionReport, StrategyMetricsSnapshot]:
    snapshot = extract_strategy_metrics(entry)
    metrics = snapshot.metrics
    comparisons: list[CriteriaComparison] = []

    if criteria.maximum_rolling_underperformance_vs_benchmark is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_rolling_underperformance_vs_benchmark",
                actual=metrics.get("rolling_underperformance_vs_benchmark"),
                threshold=criteria.maximum_rolling_underperformance_vs_benchmark,
            )
        )
    if criteria.maximum_drawdown is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_drawdown",
                actual=metrics.get("max_drawdown"),
                threshold=criteria.maximum_drawdown,
            )
        )
    if criteria.maximum_turnover is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_turnover",
                actual=metrics.get("paper_turnover_estimate") or metrics.get("turnover"),
                threshold=criteria.maximum_turnover,
            )
        )
    if criteria.minimum_signal_stability is not None:
        comparisons.append(
            _compare_min(
                criterion="minimum_signal_stability",
                actual=metrics.get("signal_stability"),
                threshold=criteria.minimum_signal_stability,
            )
        )
    if criteria.maximum_missing_data_failures is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_missing_data_failures",
                actual=metrics.get("paper_missing_data_failures"),
                threshold=criteria.maximum_missing_data_failures,
            )
        )
    if criteria.maximum_live_fail_checks is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_live_fail_checks",
                actual=metrics.get("live_fail_checks"),
                threshold=criteria.maximum_live_fail_checks,
            )
        )
    if criteria.maximum_live_warn_checks is not None:
        comparisons.append(
            _compare_max(
                criterion="maximum_live_warn_checks",
                actual=metrics.get("live_warn_checks"),
                threshold=criteria.maximum_live_warn_checks,
            )
        )

    failed = [comparison.criterion for comparison in comparisons if not comparison.passed]
    passed = not failed
    recommendation = "keep" if passed else "demote"
    report = GovernanceDecisionReport(
        strategy_id=entry.strategy_id,
        strategy_name=entry.strategy_name,
        family=entry.family,
        version=entry.version,
        timestamp=_now_utc(),
        decision_type="degradation",
        passed=passed,
        failed_criteria=failed,
        summary_metrics=metrics,
        comparisons=comparisons,
        recommendation=recommendation,
    )
    return report, snapshot


STATUS_PRIORITY = {
    "approved": 5,
    "paper": 4,
    "candidate": 3,
    "research": 2,
    "live_disabled": 1,
    "retired": 0,
}


def build_family_comparison(
    registry: StrategyRegistry,
) -> tuple[list[dict[str, Any]], dict[str, StrategyRegistryEntry]]:
    grouped: dict[str, list[StrategyRegistryEntry]] = {}
    for entry in registry.entries:
        grouped.setdefault(entry.family, []).append(entry)

    rows: list[dict[str, Any]] = []
    champions: dict[str, StrategyRegistryEntry] = {}
    for family, entries in sorted(grouped.items()):
        enriched: list[tuple[StrategyRegistryEntry, StrategyMetricsSnapshot]] = [
            (entry, extract_strategy_metrics(entry))
            for entry in entries
        ]
        champion_entry, champion_snapshot = max(
            enriched,
            key=lambda item: (
                STATUS_PRIORITY[item[0].status],
                _coerce_float(item[1].metrics.get("sharpe")) or float("-inf"),
                _coerce_float(item[1].metrics.get("mean_test_return")) or float("-inf"),
                item[0].created_at,
            ),
        )
        champions[family] = champion_entry
        champion_sharpe = _coerce_float(champion_snapshot.metrics.get("sharpe")) or 0.0
        champion_return = _coerce_float(champion_snapshot.metrics.get("mean_test_return")) or 0.0
        champion_drawdown = _coerce_float(champion_snapshot.metrics.get("max_drawdown")) or float("inf")

        for entry, snapshot in sorted(enriched, key=lambda item: item[0].strategy_id):
            sharpe = _coerce_float(snapshot.metrics.get("sharpe")) or 0.0
            mean_return = _coerce_float(snapshot.metrics.get("mean_test_return")) or 0.0
            drawdown = _coerce_float(snapshot.metrics.get("max_drawdown")) or float("inf")
            if entry.strategy_id == champion_entry.strategy_id:
                recommendation = "champion"
            elif entry.status in {"candidate", "paper"} and sharpe >= champion_sharpe and mean_return >= champion_return and drawdown <= champion_drawdown:
                recommendation = "replace"
            elif entry.status == "paper":
                recommendation = "paper-test-longer"
            else:
                recommendation = "keep"
            rows.append(
                {
                    "family": family,
                    "strategy_id": entry.strategy_id,
                    "version": entry.version,
                    "status": entry.status,
                    "champion_strategy_id": champion_entry.strategy_id,
                    "is_champion": entry.strategy_id == champion_entry.strategy_id,
                    "sharpe": sharpe,
                    "mean_test_return": mean_return,
                    "max_drawdown": drawdown,
                    "recommendation": recommendation,
                }
            )
    return rows, champions


def _family_comparison_markdown(rows: list[dict[str, Any]]) -> str:
    lines = ["# Champion Challenger Report", ""]
    for family in sorted({row["family"] for row in rows}):
        lines.append(f"## {family}")
        family_rows = [row for row in rows if row["family"] == family]
        champion = next(row for row in family_rows if row["is_champion"])
        lines.append(
            f"- Champion: `{champion['strategy_id']}` (`{champion['version']}`) sharpe={champion['sharpe']:.6f} return={champion['mean_test_return']:.6f}"
        )
        for row in family_rows:
            if row["is_champion"]:
                continue
            lines.append(
                f"- Challenger `{row['strategy_id']}` (`{row['version']}`) status={row['status']} recommendation={row['recommendation']} sharpe={row['sharpe']:.6f} return={row['mean_test_return']:.6f}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_decision_artifacts(
    *,
    report: GovernanceDecisionReport,
    snapshot: StrategyMetricsSnapshot,
    output_dir: str | Path,
    prefix: str,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    decision_json = output_path / f"{prefix}.json"
    decision_md = output_path / f"{prefix}.md"
    metrics_json = output_path / "strategy_metrics_snapshot.json"

    decision_json.write_text(
        json.dumps(report.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    decision_md.write_text(_render_decision_markdown(report), encoding="utf-8")
    metrics_json.write_text(
        json.dumps(snapshot.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    return {
        f"{prefix}_json_path": decision_json,
        f"{prefix}_md_path": decision_md,
        "strategy_metrics_snapshot_path": metrics_json,
    }


def _render_decision_markdown(report: GovernanceDecisionReport) -> str:
    lines = [
        f"# {report.decision_type.title()} Decision: {report.strategy_id}",
        "",
        f"- Strategy: `{report.strategy_name}`",
        f"- Family: `{report.family}`",
        f"- Version: `{report.version}`",
        f"- Timestamp: `{report.timestamp}`",
        f"- Passed: `{report.passed}`",
        f"- Recommendation: `{report.recommendation}`",
        "",
        "## Criteria",
    ]
    for comparison in report.comparisons:
        lines.append(
            f"- `{comparison.criterion}`: actual=`{comparison.actual}` threshold=`{comparison.threshold}` passed=`{comparison.passed}`"
        )
    return "\n".join(lines) + "\n"


def promote_registry_entry(
    *,
    registry: StrategyRegistry,
    strategy_id: str,
    note: str | None = None,
) -> StrategyRegistry:
    entry = get_registry_entry(registry, strategy_id)
    transition_map = {
        "research": "candidate",
        "candidate": "paper",
        "paper": "approved",
        "live_disabled": "paper",
    }
    if entry.status not in transition_map:
        raise ValueError(f"Strategy {strategy_id} cannot be promoted from status {entry.status}")
    new_status = transition_map[entry.status]
    validate_status_transition(from_status=entry.status, to_status=new_status)
    updated_entry = StrategyRegistryEntry(
        **{
            **entry.to_dict(),
            "status": new_status,
            "current_deployment_stage": new_status,
        }
    )
    updated = upsert_registry_entry(registry, updated_entry)
    return append_audit_event(
        updated,
        StrategyRegistryAuditEvent(
            timestamp=_now_utc(),
            strategy_id=strategy_id,
            action="promote",
            from_status=entry.status,
            to_status=new_status,
            note=note,
        ),
    )


def demote_registry_entry(
    *,
    registry: StrategyRegistry,
    strategy_id: str,
    note: str | None = None,
) -> StrategyRegistry:
    entry = get_registry_entry(registry, strategy_id)
    transition_map = {
        "approved": "paper",
        "paper": "candidate",
        "candidate": "research",
        "live_disabled": "retired",
    }
    if entry.status not in transition_map:
        raise ValueError(f"Strategy {strategy_id} cannot be demoted from status {entry.status}")
    new_status = transition_map[entry.status]
    validate_status_transition(from_status=entry.status, to_status=new_status)
    updated_entry = StrategyRegistryEntry(
        **{
            **entry.to_dict(),
            "status": new_status,
            "current_deployment_stage": new_status,
        }
    )
    updated = upsert_registry_entry(registry, updated_entry)
    return append_audit_event(
        updated,
        StrategyRegistryAuditEvent(
            timestamp=_now_utc(),
            strategy_id=strategy_id,
            action="demote",
            from_status=entry.status,
            to_status=new_status,
            note=note,
        ),
    )


def filter_registry_entries(
    registry: StrategyRegistry,
    options: RegistrySelectionOptions,
) -> list[tuple[StrategyRegistryEntry, StrategyMetricsSnapshot]]:
    selected: list[tuple[StrategyRegistryEntry, StrategyMetricsSnapshot]] = []
    for entry in registry.entries:
        if entry.status not in options.include_statuses:
            continue
        if options.universe and entry.universe != options.universe:
            continue
        if options.family and entry.family != options.family:
            continue
        if options.tag and options.tag not in entry.tags:
            continue
        if options.deployment_stage and entry.current_deployment_stage != options.deployment_stage:
            continue
        selected.append((entry, extract_strategy_metrics(entry)))

    selected.sort(
        key=lambda item: (
            STATUS_PRIORITY[item[0].status],
            _coerce_float(item[1].metrics.get("sharpe")) or float("-inf"),
            _coerce_float(item[1].metrics.get("mean_test_return")) or float("-inf"),
            item[0].strategy_id,
        ),
        reverse=True,
    )
    if options.max_strategies is not None:
        selected = selected[: options.max_strategies]
    return selected


def build_multi_strategy_config_from_registry(
    *,
    registry: StrategyRegistry,
    options: RegistrySelectionOptions,
) -> tuple[MultiStrategyPortfolioConfig, list[dict[str, Any]]]:
    selected = filter_registry_entries(registry, options)
    if not selected:
        raise ValueError("Registry selection did not produce any eligible strategies")

    if options.weighting_scheme == "equal":
        raw_weights = {entry.strategy_id: 1.0 for entry, _ in selected}
    else:
        raw_weights = {}
        for entry, snapshot in selected:
            sharpe = _coerce_float(snapshot.metrics.get("sharpe")) or 0.0
            raw_weights[entry.strategy_id] = max(sharpe, 0.0)
        if sum(raw_weights.values()) <= 0:
            raw_weights = {entry.strategy_id: 1.0 for entry, _ in selected}

    total = float(sum(raw_weights.values()))
    sleeves = [
        MultiStrategySleeveConfig(
            sleeve_name=entry.strategy_id,
            preset_name=entry.preset_name,
            target_capital_weight=raw_weights[entry.strategy_id] / total,
            enabled=True,
            notes=entry.notes,
            tags=entry.tags,
        )
        for entry, _snapshot in selected
    ]
    config = MultiStrategyPortfolioConfig(sleeves=sleeves)
    comparison_rows, _champions = build_family_comparison(registry)
    return config, comparison_rows


def write_registry_backed_multi_strategy_artifacts(
    *,
    config: MultiStrategyPortfolioConfig,
    family_rows: list[dict[str, Any]],
    output_path: str | Path,
) -> dict[str, Path]:
    file_path = Path(output_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "gross_leverage_cap": config.gross_leverage_cap,
        "net_exposure_cap": config.net_exposure_cap,
        "max_position_weight": config.max_position_weight,
        "max_symbol_concentration": config.max_symbol_concentration,
        "sector_caps": [asdict(item) for item in config.sector_caps],
        "turnover_cap": config.turnover_cap,
        "cash_reserve_pct": config.cash_reserve_pct,
        "group_map_path": config.group_map_path,
        "rebalance_timestamp": config.rebalance_timestamp,
        "notes": config.notes,
        "tags": config.tags,
        "sleeves": [asdict(item) for item in config.sleeves],
    }
    if file_path.suffix.lower() == ".json":
        file_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    elif file_path.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError(
                "PyYAML is required for YAML output. Install with `pip install pyyaml`."
            )
        file_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    else:
        raise ValueError("output_path must end with .json, .yaml, or .yml")

    family_csv = file_path.parent / "family_comparison.csv"
    family_md = file_path.parent / "champion_challenger_report.md"
    pd.DataFrame(family_rows).to_csv(family_csv, index=False)
    family_md.write_text(_family_comparison_markdown(family_rows), encoding="utf-8")
    return {
        "multi_strategy_config_path": file_path,
        "family_comparison_path": family_csv,
        "champion_challenger_report_path": family_md,
    }


def save_mutated_registry(registry: StrategyRegistry, path: str | Path) -> Path:
    return save_strategy_registry(registry, path)
