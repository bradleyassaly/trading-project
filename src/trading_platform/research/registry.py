from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.conditional import evaluate_conditional_research


MANIFEST_NAME = "research_run.json"
REGISTRY_JSON_NAME = "research_registry.json"
REGISTRY_CSV_NAME = "research_registry.csv"
LEADERBOARD_JSON_NAME = "research_leaderboard.json"
LEADERBOARD_CSV_NAME = "research_leaderboard.csv"
PROMOTION_CANDIDATES_JSON_NAME = "promotion_candidates.json"
PROMOTION_CANDIDATES_CSV_NAME = "promotion_candidates.csv"
CONDITIONAL_PROMOTION_CANDIDATES_CSV_NAME = "conditional_promotion_candidates.csv"
COMPARE_RUNS_JSON_NAME = "research_run_comparison.json"
COMPARE_RUNS_MD_NAME = "research_run_comparison.md"

REGISTRY_COLUMNS = [
    "run_id",
    "timestamp",
    "workflow_type",
    "artifact_dir",
    "git_commit",
    "universe",
    "signal_family",
    "candidate_count",
    "promoted_signal_count",
    "folds_tested",
    "symbols_requested_count",
    "mean_spearman_ic",
    "portfolio_sharpe",
    "portfolio_total_return",
    "portfolio_max_drawdown",
    "implementability_return_drag",
    "promotion_status",
    "promotion_recommendation",
]

LEADERBOARD_COLUMNS = [
    "rank",
    "run_id",
    "timestamp",
    "signal_family",
    "universe",
    "workflow_type",
    "metric_name",
    "metric_value",
    "promoted_signal_count",
    "folds_tested",
    "candidate_count",
    "promotion_recommendation",
]

PROMOTION_CANDIDATE_COLUMNS = [
    "run_id",
    "timestamp",
    "signal_family",
    "universe",
    "eligible",
    "promotion_recommendation",
    "reason_count",
    "reasons",
    "mean_spearman_ic",
    "portfolio_sharpe",
    "promoted_signal_count",
    "folds_tested",
    "candidate_count",
]

CONDITIONAL_PROMOTION_CANDIDATE_COLUMNS = [
    "run_id",
    "timestamp",
    "signal_family",
    "universe",
    "condition_id",
    "condition_type",
    "eligible",
    "recommendation",
    "sample_size",
    "metric_name",
    "metric_value",
    "baseline_metric_value",
    "improvement_vs_baseline",
    "reason",
]


@dataclass(slots=True)
class ResearchPromotionRules:
    min_folds_tested: int = 3
    min_mean_spearman_ic: float = 0.01
    min_portfolio_sharpe: float = 0.5
    max_implementability_return_drag: float = 0.25
    min_promoted_signals: int = 1
    require_nonempty_portfolio_metrics: bool = True


@dataclass(slots=True)
class ResearchPromotionDecision:
    eligible: bool
    recommendation: str
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "recommendation": self.recommendation,
            "reasons": list(self.reasons),
        }


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(value: Any) -> float | None:
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


def _safe_int(value: Any) -> int | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _clean_optional_reason(value: Any) -> str | None:
    text = _clean_optional_text(value)
    if text is None:
        return None
    if text.casefold() in {"none", "null", "n/a", "na"}:
        return None
    return text


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _git_commit(cwd: str | Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    commit = result.stdout.strip()
    return commit or None


def _top_row_by_metric(frame: pd.DataFrame, metric: str) -> dict[str, Any]:
    if frame.empty or metric not in frame.columns:
        return {}
    candidate = frame.copy()
    candidate[metric] = pd.to_numeric(candidate[metric], errors="coerce")
    candidate = candidate.dropna(subset=[metric])
    if candidate.empty:
        return {}
    return candidate.sort_values(metric, ascending=False).iloc[0].to_dict()


def _first_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return frame.iloc[0].to_dict()


def _metric_from_row(row: dict[str, Any], *names: str) -> float | None:
    for name in names:
        value = _safe_float(row.get(name))
        if value is not None:
            return value
    return None


def _repair_manifest_portfolio_metrics(manifest: dict[str, Any]) -> dict[str, Any]:
    top_metrics = manifest.setdefault("top_metrics", {})
    top_metrics["rejection_reason"] = _clean_optional_reason(top_metrics.get("rejection_reason"))
    top_candidate = manifest.setdefault("top_candidate", {})
    top_candidate["rejection_reason"] = _clean_optional_reason(top_candidate.get("rejection_reason"))
    if _safe_float(top_metrics.get("portfolio_sharpe")) is not None:
        manifest["promotion_recommendation"] = evaluate_manifest_promotion_readiness(manifest).to_dict()
        return manifest
    artifact_paths = manifest.get("artifact_paths", {})
    portfolio_metrics_df = _safe_read_csv(artifact_paths.get("portfolio_metrics_path"))
    top_portfolio = (
        _top_row_by_metric(portfolio_metrics_df, "portfolio_sharpe")
        or _top_row_by_metric(portfolio_metrics_df, "sharpe")
    )
    portfolio_sharpe = _metric_from_row(top_portfolio, "portfolio_sharpe", "sharpe")
    if portfolio_sharpe is None:
        return manifest
    top_metrics["portfolio_sharpe"] = portfolio_sharpe
    if _safe_float(top_metrics.get("portfolio_total_return")) is None:
        top_metrics["portfolio_total_return"] = _metric_from_row(top_portfolio, "portfolio_total_return", "total_return")
    if _safe_float(top_metrics.get("portfolio_max_drawdown")) is None:
        top_metrics["portfolio_max_drawdown"] = _metric_from_row(top_portfolio, "portfolio_max_drawdown", "max_drawdown")
    manifest["promotion_recommendation"] = evaluate_manifest_promotion_readiness(manifest).to_dict()
    return manifest


def _sorted_unique_strings(values: list[Any]) -> list[str]:
    cleaned = {str(value) for value in values if value is not None and str(value).strip()}
    return sorted(cleaned)


def _manifest_signal_families(manifest: dict[str, Any]) -> list[str]:
    families = manifest.get("signal_families")
    if isinstance(families, list):
        return _sorted_unique_strings(families)
    return _sorted_unique_strings([manifest.get("signal_family")])


def _normalize_paths(paths: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in sorted(paths.items()):
        if value is None:
            continue
        text = str(value)
        if text:
            normalized[key] = text
    return normalized


def _manifest_summary_row(manifest: dict[str, Any]) -> dict[str, Any]:
    top_metrics = manifest.get("top_metrics", {})
    promotion = manifest.get("promotion_recommendation", {})
    return {
        "run_id": manifest.get("run_id"),
        "timestamp": manifest.get("timestamp"),
        "workflow_type": manifest.get("workflow_type"),
        "artifact_dir": manifest.get("artifact_dir"),
        "git_commit": manifest.get("git_commit"),
        "universe": manifest.get("universe"),
        "signal_family": manifest.get("signal_family"),
        "candidate_count": manifest.get("candidate_count"),
        "promoted_signal_count": manifest.get("promoted_signal_count"),
        "folds_tested": manifest.get("folds_tested"),
        "symbols_requested_count": manifest.get("symbols_requested_count"),
        "mean_spearman_ic": top_metrics.get("mean_spearman_ic"),
        "portfolio_sharpe": top_metrics.get("portfolio_sharpe"),
        "portfolio_total_return": top_metrics.get("portfolio_total_return"),
        "portfolio_max_drawdown": top_metrics.get("portfolio_max_drawdown"),
        "implementability_return_drag": top_metrics.get("implementability_return_drag"),
        "promotion_status": top_metrics.get("promotion_status"),
        "promotion_recommendation": promotion.get("recommendation"),
    }


def evaluate_manifest_promotion_readiness(
    manifest: dict[str, Any],
    *,
    rules: ResearchPromotionRules | None = None,
) -> ResearchPromotionDecision:
    active_rules = rules or ResearchPromotionRules()
    top_metrics = manifest.get("top_metrics", {})
    reasons: list[str] = []

    folds_tested = _safe_int(manifest.get("folds_tested"))
    if folds_tested is None or folds_tested < active_rules.min_folds_tested:
        reasons.append(
            f"folds_tested {folds_tested if folds_tested is not None else 'missing'} < {active_rules.min_folds_tested}"
        )

    mean_spearman_ic = _safe_float(top_metrics.get("mean_spearman_ic"))
    if mean_spearman_ic is None or mean_spearman_ic < active_rules.min_mean_spearman_ic:
        reasons.append(
            f"mean_spearman_ic {mean_spearman_ic if mean_spearman_ic is not None else 'missing'} < {active_rules.min_mean_spearman_ic}"
        )

    portfolio_sharpe = _safe_float(top_metrics.get("portfolio_sharpe"))
    if active_rules.require_nonempty_portfolio_metrics and portfolio_sharpe is None:
        reasons.append("portfolio_sharpe missing")
    elif portfolio_sharpe is not None and portfolio_sharpe < active_rules.min_portfolio_sharpe:
        reasons.append(f"portfolio_sharpe {portfolio_sharpe} < {active_rules.min_portfolio_sharpe}")

    promoted_signal_count = _safe_int(manifest.get("promoted_signal_count"))
    if promoted_signal_count is None or promoted_signal_count < active_rules.min_promoted_signals:
        reasons.append(
            f"promoted_signal_count {promoted_signal_count if promoted_signal_count is not None else 'missing'} < {active_rules.min_promoted_signals}"
        )

    return_drag = _safe_float(top_metrics.get("implementability_return_drag"))
    if return_drag is not None and return_drag > active_rules.max_implementability_return_drag:
        reasons.append(
            f"implementability_return_drag {return_drag} > {active_rules.max_implementability_return_drag}"
        )

    rejection_reason = _clean_optional_reason(top_metrics.get("rejection_reason"))
    if rejection_reason:
        reasons.append(f"top_candidate_rejection_reason: {rejection_reason}")

    if reasons:
        return ResearchPromotionDecision(
            eligible=False,
            recommendation="needs_more_research",
            reasons=reasons,
        )

    return ResearchPromotionDecision(
        eligible=True,
        recommendation="promotion_candidate",
        reasons=[
            f"folds_tested >= {active_rules.min_folds_tested}",
            f"mean_spearman_ic >= {active_rules.min_mean_spearman_ic}",
            f"portfolio_sharpe >= {active_rules.min_portfolio_sharpe}",
            f"promoted_signal_count >= {active_rules.min_promoted_signals}",
        ],
    )


def build_research_run_manifest(
    *,
    output_dir: str | Path,
    workflow_type: str,
    command: str | None,
    feature_dir: str | Path | None,
    signal_family: str | None,
    signal_families: list[str] | None = None,
    universe: str | None,
    symbols_requested: list[str] | None,
    lookbacks: list[int] | None,
    horizons: list[int] | None,
    min_rows: int | None,
    train_size: int | None,
    test_size: int | None,
    step_size: int | None,
    min_train_size: int | None,
    config_paths: list[str] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    promotion_rules: ResearchPromotionRules | None = None,
    git_commit: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    artifact_dir = Path(output_dir)
    normalized_artifacts = _normalize_paths(artifact_paths or {})

    leaderboard_df = _safe_read_csv(normalized_artifacts.get("leaderboard_path"))
    fold_results_df = _safe_read_csv(normalized_artifacts.get("fold_results_path"))
    promoted_signals_df = _safe_read_csv(normalized_artifacts.get("promoted_signals_path"))
    portfolio_metrics_df = _safe_read_csv(normalized_artifacts.get("portfolio_metrics_path"))
    implementability_report_df = _safe_read_csv(normalized_artifacts.get("implementability_report_path"))
    diagnostics = _safe_read_json(normalized_artifacts.get("signal_diagnostics_path"))

    top_candidate = _top_row_by_metric(leaderboard_df, "mean_spearman_ic")
    top_portfolio = (
        _top_row_by_metric(portfolio_metrics_df, "portfolio_sharpe")
        or _top_row_by_metric(portfolio_metrics_df, "sharpe")
    )
    top_implementability = _first_row(implementability_report_df)

    manifest_timestamp = timestamp or _now_utc()
    manifest = {
        "manifest_version": 1,
        "run_id": f"{artifact_dir.name}",
        "timestamp": manifest_timestamp,
        "workflow_type": workflow_type,
        "command": command,
        "git_commit": git_commit if git_commit is not None else _git_commit(artifact_dir),
        "artifact_dir": str(artifact_dir),
        "config_paths": sorted(str(path) for path in (config_paths or []) if path),
        "feature_dir": str(feature_dir) if feature_dir is not None else None,
        "universe": universe,
        "symbol_scope": "universe" if universe else "symbols",
        "symbols_requested": sorted(symbols_requested or []),
        "symbols_requested_count": len(symbols_requested or []),
        "signal_family": signal_family,
        "signal_families": sorted(signal_families or ([signal_family] if signal_family else [])),
        "candidate_count": int(len(leaderboard_df.index)),
        "promoted_signal_count": int(len(promoted_signals_df.index)),
        "folds_tested": int(fold_results_df["fold_id"].nunique()) if not fold_results_df.empty and "fold_id" in fold_results_df.columns else 0,
        "evaluation_periods": {
            "train_size": train_size,
            "test_size": test_size,
            "step_size": step_size,
            "min_train_size": min_train_size,
            "lookbacks": list(lookbacks or []),
            "horizons": list(horizons or []),
            "min_rows": min_rows,
            "earliest_test_start": str(fold_results_df["test_start"].min()) if not fold_results_df.empty and "test_start" in fold_results_df.columns else None,
            "latest_test_end": str(fold_results_df["test_end"].max()) if not fold_results_df.empty and "test_end" in fold_results_df.columns else None,
        },
        "top_metrics": {
            "mean_spearman_ic": _safe_float(top_candidate.get("mean_spearman_ic")),
            "mean_hit_rate": _safe_float(top_candidate.get("mean_hit_rate")),
            "mean_turnover": _safe_float(top_candidate.get("mean_turnover")),
            "portfolio_sharpe": _metric_from_row(top_portfolio, "portfolio_sharpe", "sharpe"),
            "portfolio_total_return": _metric_from_row(top_portfolio, "portfolio_total_return", "total_return"),
            "portfolio_max_drawdown": _metric_from_row(top_portfolio, "portfolio_max_drawdown", "max_drawdown"),
            "implementability_return_drag": _safe_float(top_implementability.get("return_drag")),
            "promotion_status": _clean_optional_text(top_candidate.get("promotion_status")),
            "rejection_reason": _clean_optional_reason(top_candidate.get("rejection_reason")),
        },
        "top_candidate": {
            "signal_family": _clean_optional_text(top_candidate.get("signal_family")),
            "lookback": _safe_int(top_candidate.get("lookback")),
            "horizon": _safe_int(top_candidate.get("horizon")),
            "promotion_status": _clean_optional_text(top_candidate.get("promotion_status")),
            "rejection_reason": _clean_optional_reason(top_candidate.get("rejection_reason")),
        },
        "diagnostics_snapshot": {
            "evaluation_mode": diagnostics.get("evaluation_mode"),
            "promotion_rules": diagnostics.get("promotion_rules", {}),
            "signal_lifecycle": diagnostics.get("signal_lifecycle", {}),
            "regime": diagnostics.get("regime", {}),
            "composite_portfolio": diagnostics.get("composite_portfolio", {}),
        },
        "artifact_paths": normalized_artifacts,
    }

    manifest["promotion_recommendation"] = evaluate_manifest_promotion_readiness(
        manifest,
        rules=promotion_rules,
    ).to_dict()
    conditional_result = evaluate_conditional_research(
        output_dir=artifact_dir,
        artifact_paths=normalized_artifacts,
        signal_family=signal_family,
        top_candidate=manifest["top_candidate"],
        top_metrics=manifest["top_metrics"],
        universe=universe,
    )
    manifest["conditional_research"] = {
        "enabled": conditional_result.get("enabled", False),
        "summary": conditional_result.get("summary", {}),
        "promotion_candidates": conditional_result.get("promotion_candidates", []),
        "artifacts": conditional_result.get("artifacts", {}),
    }
    normalized_artifacts.update(conditional_result.get("artifacts", {}))
    manifest["artifact_paths"] = normalized_artifacts
    return manifest


def write_research_run_manifest(
    *,
    output_dir: str | Path,
    workflow_type: str,
    command: str | None,
    feature_dir: str | Path | None,
    signal_family: str | None,
    signal_families: list[str] | None = None,
    universe: str | None,
    symbols_requested: list[str] | None,
    lookbacks: list[int] | None,
    horizons: list[int] | None,
    min_rows: int | None,
    train_size: int | None,
    test_size: int | None,
    step_size: int | None,
    min_train_size: int | None,
    config_paths: list[str] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    promotion_rules: ResearchPromotionRules | None = None,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    manifest = build_research_run_manifest(
        output_dir=output_path,
        workflow_type=workflow_type,
        command=command,
        feature_dir=feature_dir,
        signal_family=signal_family,
        signal_families=signal_families,
        universe=universe,
        symbols_requested=symbols_requested,
        lookbacks=lookbacks,
        horizons=horizons,
        min_rows=min_rows,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
        config_paths=config_paths,
        artifact_paths=artifact_paths,
        promotion_rules=promotion_rules,
    )
    path = output_path / MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return path


def discover_research_run_manifests(artifacts_root: str | Path) -> list[Path]:
    root = Path(artifacts_root)
    manifests = sorted(root.rglob(MANIFEST_NAME))
    manifests.sort(
        key=lambda path: (
            _safe_read_json(path).get("timestamp") or "",
            str(path.parent),
        ),
        reverse=True,
    )
    return manifests


def resolve_latest_research_run_dir(artifacts_root: str | Path) -> Path | None:
    manifests = discover_research_run_manifests(artifacts_root)
    if not manifests:
        return None
    return manifests[0].parent


def load_research_manifests(artifacts_root: str | Path) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for path in discover_research_run_manifests(artifacts_root):
        payload = _safe_read_json(path)
        if not payload:
            continue
        payload["manifest_path"] = str(path)
        manifests.append(_repair_manifest_portfolio_metrics(payload))
    return manifests


def _write_json(path: Path, payload: Any) -> Path:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def build_research_registry(
    *,
    artifacts_root: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    manifests = load_research_manifests(artifacts_root)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    rows = [_manifest_summary_row(manifest) for manifest in manifests]
    registry_df = pd.DataFrame(rows, columns=REGISTRY_COLUMNS)
    if not registry_df.empty:
        registry_df = registry_df.sort_values(
            by=["timestamp", "mean_spearman_ic", "portfolio_sharpe", "run_id"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)

    summary = {
        "generated_at": _now_utc(),
        "artifacts_root": str(Path(artifacts_root)),
        "run_count": len(manifests),
        "signal_families": _sorted_unique_strings(
            [
                family
                for manifest in manifests
                for family in _manifest_signal_families(manifest)
            ]
        ),
        "universes": _sorted_unique_strings([manifest.get("universe") for manifest in manifests]),
        "workflow_types": _sorted_unique_strings([manifest.get("workflow_type") for manifest in manifests]),
    }
    registry_json = {
        "summary": summary,
        "runs": manifests,
    }
    json_path = _write_json(output_path / REGISTRY_JSON_NAME, registry_json)
    csv_path = output_path / REGISTRY_CSV_NAME
    registry_df.to_csv(csv_path, index=False)
    return {
        "registry_json_path": json_path,
        "registry_csv_path": csv_path,
        "run_count": len(manifests),
    }


def build_research_leaderboard(
    *,
    artifacts_root: str | Path,
    output_dir: str | Path,
    metric: str = "portfolio_sharpe",
    group_by: str = "none",
    limit: int = 20,
) -> dict[str, Any]:
    manifests = load_research_manifests(artifacts_root)
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        metrics = manifest.get("top_metrics", {})
        row = {
            "run_id": manifest.get("run_id"),
            "timestamp": manifest.get("timestamp"),
            "signal_family": manifest.get("signal_family"),
            "universe": manifest.get("universe"),
            "workflow_type": manifest.get("workflow_type"),
            "metric_name": metric,
            "metric_value": _safe_float(metrics.get(metric)),
            "promoted_signal_count": manifest.get("promoted_signal_count"),
            "folds_tested": manifest.get("folds_tested"),
            "candidate_count": manifest.get("candidate_count"),
            "promotion_recommendation": manifest.get("promotion_recommendation", {}).get("recommendation"),
        }
        if row["metric_value"] is None:
            continue
        rows.append(row)

    leaderboard_df = pd.DataFrame(rows, columns=LEADERBOARD_COLUMNS[1:])
    if not leaderboard_df.empty:
        sort_columns = ["metric_value", "timestamp", "run_id"]
        leaderboard_df = leaderboard_df.sort_values(sort_columns, ascending=[False, False, True]).reset_index(drop=True)
        if group_by and group_by != "none" and group_by in leaderboard_df.columns:
            leaderboard_df = leaderboard_df.groupby(group_by, as_index=False).head(1).reset_index(drop=True)
            leaderboard_df = leaderboard_df.sort_values(sort_columns, ascending=[False, False, True]).reset_index(drop=True)
        leaderboard_df.insert(0, "rank", range(1, len(leaderboard_df.index) + 1))
    leaderboard_df = leaderboard_df.head(limit)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _now_utc(),
        "artifacts_root": str(Path(artifacts_root)),
        "metric": metric,
        "group_by": group_by,
        "rows": leaderboard_df.to_dict(orient="records"),
    }
    json_path = _write_json(output_path / LEADERBOARD_JSON_NAME, payload)
    csv_path = output_path / LEADERBOARD_CSV_NAME
    leaderboard_df.to_csv(csv_path, index=False)
    return {
        "leaderboard_json_path": json_path,
        "leaderboard_csv_path": csv_path,
        "row_count": int(len(leaderboard_df.index)),
    }


def build_promotion_candidates(
    *,
    artifacts_root: str | Path,
    output_dir: str | Path,
    rules: ResearchPromotionRules | None = None,
) -> dict[str, Any]:
    manifests = load_research_manifests(artifacts_root)
    rows: list[dict[str, Any]] = []
    conditional_rows: list[dict[str, Any]] = []
    for manifest in manifests:
        decision = evaluate_manifest_promotion_readiness(manifest, rules=rules)
        reasons = "; ".join(decision.reasons)
        top_metrics = manifest.get("top_metrics", {})
        rows.append(
            {
                "run_id": manifest.get("run_id"),
                "timestamp": manifest.get("timestamp"),
                "signal_family": manifest.get("signal_family"),
                "universe": manifest.get("universe"),
                "eligible": decision.eligible,
                "promotion_recommendation": decision.recommendation,
                "reason_count": len(decision.reasons),
                "reasons": reasons,
                "mean_spearman_ic": top_metrics.get("mean_spearman_ic"),
                "portfolio_sharpe": top_metrics.get("portfolio_sharpe"),
                "promoted_signal_count": manifest.get("promoted_signal_count"),
                "folds_tested": manifest.get("folds_tested"),
                "candidate_count": manifest.get("candidate_count"),
            }
        )
        for candidate in manifest.get("conditional_research", {}).get("promotion_candidates", []):
            conditional_rows.append(
                {
                    "run_id": manifest.get("run_id"),
                    "timestamp": manifest.get("timestamp"),
                    "signal_family": manifest.get("signal_family"),
                    "universe": manifest.get("universe"),
                    "condition_id": candidate.get("condition_id"),
                    "condition_type": candidate.get("condition_type"),
                    "eligible": candidate.get("eligible"),
                    "recommendation": candidate.get("recommendation"),
                    "sample_size": candidate.get("sample_size"),
                    "metric_name": candidate.get("metric_name"),
                    "metric_value": candidate.get("metric_value"),
                    "baseline_metric_value": candidate.get("baseline_metric_value"),
                    "improvement_vs_baseline": candidate.get("improvement_vs_baseline"),
                    "reason": candidate.get("reason"),
                }
            )
    candidates_df = pd.DataFrame(rows, columns=PROMOTION_CANDIDATE_COLUMNS)
    conditional_df = pd.DataFrame(conditional_rows, columns=CONDITIONAL_PROMOTION_CANDIDATE_COLUMNS)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values(
            by=["eligible", "portfolio_sharpe", "mean_spearman_ic", "timestamp", "run_id"],
            ascending=[False, False, False, False, True],
        ).reset_index(drop=True)
    if not conditional_df.empty:
        conditional_df = conditional_df.sort_values(
            by=["eligible", "improvement_vs_baseline", "sample_size", "timestamp", "run_id"],
            ascending=[False, False, False, False, True],
        ).reset_index(drop=True)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _now_utc(),
        "rules": asdict(rules or ResearchPromotionRules()),
        "rows": candidates_df.to_dict(orient="records"),
        "conditional_rows": conditional_df.to_dict(orient="records"),
    }
    json_path = _write_json(output_path / PROMOTION_CANDIDATES_JSON_NAME, payload)
    csv_path = output_path / PROMOTION_CANDIDATES_CSV_NAME
    candidates_df.to_csv(csv_path, index=False)
    conditional_csv_path = output_path / CONDITIONAL_PROMOTION_CANDIDATES_CSV_NAME
    conditional_df.to_csv(conditional_csv_path, index=False)
    return {
        "promotion_candidates_json_path": json_path,
        "promotion_candidates_csv_path": csv_path,
        "conditional_promotion_candidates_csv_path": conditional_csv_path,
        "eligible_count": int((candidates_df.get("eligible", pd.Series(dtype=bool)) == True).sum()) if not candidates_df.empty else 0,
    }


def refresh_research_registry_bundle(
    *,
    artifacts_root: str | Path,
    output_dir: str | Path,
    rules: ResearchPromotionRules | None = None,
) -> dict[str, Any]:
    registry_result = build_research_registry(
        artifacts_root=artifacts_root,
        output_dir=output_dir,
    )
    candidate_result = build_promotion_candidates(
        artifacts_root=artifacts_root,
        output_dir=output_dir,
        rules=rules,
    )
    return {
        **registry_result,
        **candidate_result,
        "output_dir": str(Path(output_dir)),
    }


def refresh_run_local_registry_bundle(
    *,
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    rules: ResearchPromotionRules | None = None,
) -> dict[str, Any]:
    run_path = Path(run_dir)
    resolved_output_dir = Path(output_dir) if output_dir is not None else (run_path / "research_registry")
    return refresh_research_registry_bundle(
        artifacts_root=run_path,
        output_dir=resolved_output_dir,
        rules=rules,
    )


def compare_research_runs(
    *,
    artifacts_root: str | Path,
    run_id_a: str,
    run_id_b: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    manifests = {manifest.get("run_id"): manifest for manifest in load_research_manifests(artifacts_root)}
    manifest_a = manifests.get(run_id_a)
    manifest_b = manifests.get(run_id_b)
    if manifest_a is None:
        raise ValueError(f"Unknown research run_id: {run_id_a}")
    if manifest_b is None:
        raise ValueError(f"Unknown research run_id: {run_id_b}")

    metrics_a = manifest_a.get("top_metrics", {})
    metrics_b = manifest_b.get("top_metrics", {})
    comparison_rows: list[dict[str, Any]] = []
    for metric in [
        "mean_spearman_ic",
        "portfolio_sharpe",
        "portfolio_total_return",
        "portfolio_max_drawdown",
        "implementability_return_drag",
    ]:
        value_a = _safe_float(metrics_a.get(metric))
        value_b = _safe_float(metrics_b.get(metric))
        delta = None if value_a is None or value_b is None else value_b - value_a
        comparison_rows.append(
            {
                "metric": metric,
                "run_a": value_a,
                "run_b": value_b,
                "delta_b_minus_a": delta,
            }
        )

    payload = {
        "generated_at": _now_utc(),
        "run_a": _manifest_summary_row(manifest_a),
        "run_b": _manifest_summary_row(manifest_b),
        "metric_comparison": comparison_rows,
    }
    lines = [
        "# Research Run Comparison",
        "",
        f"- Run A: `{run_id_a}`",
        f"- Run B: `{run_id_b}`",
        "",
        "| metric | run_a | run_b | delta_b_minus_a |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in comparison_rows:
        lines.append(
            f"| {row['metric']} | {row['run_a']} | {row['run_b']} | {row['delta_b_minus_a']} |"
        )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = _write_json(output_path / COMPARE_RUNS_JSON_NAME, payload)
    md_path = output_path / COMPARE_RUNS_MD_NAME
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "comparison_json_path": json_path,
        "comparison_md_path": md_path,
    }
