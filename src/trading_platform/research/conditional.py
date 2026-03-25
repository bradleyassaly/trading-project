from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


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
    return int(numeric) if numeric is not None else None


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists() or file_path.is_dir():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _candidate_id(top_candidate: dict[str, Any], signal_family: str | None) -> str | None:
    family = _safe_text(top_candidate.get("signal_family")) or _safe_text(signal_family)
    lookback = _safe_int(top_candidate.get("lookback"))
    horizon = _safe_int(top_candidate.get("horizon"))
    if family is None or lookback is None or horizon is None:
        return None
    return f"{family}|{lookback}|{horizon}"


def _sample_size(row: dict[str, Any]) -> int | None:
    for key in ("sample_size", "dates_evaluated", "observation_count", "observations", "coverage_count"):
        value = _safe_int(row.get(key))
        if value is not None:
            return value
    return None


def _metric_name_and_value(row: dict[str, Any]) -> tuple[str | None, float | None]:
    for metric_name in ("portfolio_sharpe", "mean_spearman_ic", "excess_return", "mean_long_short_spread"):
        value = _safe_float(row.get(metric_name))
        if value is not None:
            return metric_name, value
    return None, None


@dataclass(frozen=True)
class ConditionDefinition:
    condition_id: str
    condition_type: str
    condition_name: str
    condition_source: str
    base_universe_id: str | None = None
    sub_universe_id: str | None = None
    regime_label: str | None = None
    benchmark_context_label: str | None = None
    taxonomy_constraint: str | None = None
    volatility_state_label: str | None = None
    liquidity_state_label: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConditionalResearchResult:
    condition: ConditionDefinition
    signal_family: str | None
    candidate_id: str | None
    metric_name: str | None
    metric_value: float | None
    baseline_metric_name: str | None
    baseline_metric_value: float | None
    improvement_vs_baseline: float | None
    sample_size: int | None
    condition_status: str
    recommendation: str
    rejection_reason: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update(payload.pop("condition"))
        return payload


@dataclass(frozen=True)
class ConditionalPromotionDecision:
    condition_id: str
    condition_type: str
    recommendation: str
    eligible: bool
    signal_family: str | None
    sample_size: int | None
    metric_name: str | None
    metric_value: float | None
    baseline_metric_value: float | None
    improvement_vs_baseline: float | None
    reason: str
    activation_condition: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyActivationCondition:
    condition_id: str
    condition_type: str
    condition_name: str
    activation_status: str = "active_when_matched"
    notes: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConditionalEvaluationConfig:
    enabled: bool = True
    condition_types: list[str] = field(default_factory=lambda: ["regime", "sub_universe", "benchmark_context"])
    min_sample_size: int = 20
    min_improvement: float = 0.0
    compare_to_unconditional: bool = True

    def __post_init__(self) -> None:
        if self.min_sample_size < 0:
            raise ValueError("min_sample_size must be >= 0")


def summarize_conditional_edge(row: dict[str, Any]) -> str:
    metric_name = _safe_text(row.get("metric_name")) or "metric"
    metric_value = _safe_float(row.get("metric_value"))
    improvement = _safe_float(row.get("improvement_vs_baseline"))
    metric_text = f"{metric_name}={metric_value:.4f}" if metric_value is not None else f"{metric_name}=n/a"
    if improvement is None:
        return metric_text
    sign = "+" if improvement >= 0 else ""
    return f"{metric_text} vs baseline {sign}{improvement:.4f}"


def summarize_condition_coverage(row: dict[str, Any]) -> str:
    sample_size = _safe_int(row.get("sample_size"))
    if sample_size is None:
        return "sample_size unavailable"
    return f"sample_size={sample_size}"


def summarize_conditional_promotion(row: dict[str, Any]) -> str:
    recommendation = _safe_text(row.get("recommendation")) or "unavailable"
    reason = _safe_text(row.get("reason")) or _safe_text(row.get("rejection_reason")) or "no explicit rationale"
    return f"{recommendation}: {reason}"


def summarize_strategy_activation_condition(row: dict[str, Any]) -> str:
    condition_type = _safe_text(row.get("condition_type")) or "condition"
    condition_name = _safe_text(row.get("condition_name")) or _safe_text(row.get("condition_id")) or "unknown"
    return f"{condition_type}={condition_name}"


def _normalize_condition_rows(
    frame: pd.DataFrame,
    *,
    condition_type: str,
    condition_source: str,
    signal_family: str | None,
    top_candidate: dict[str, Any],
    top_metrics: dict[str, Any],
    universe: str | None,
    config: ConditionalEvaluationConfig,
) -> list[ConditionalResearchResult]:
    if frame.empty:
        return []
    candidate_id = _candidate_id(top_candidate, signal_family)
    working = frame.copy()
    if candidate_id and "candidate_id" in working.columns:
        subset = working.loc[working["candidate_id"].astype(str) == candidate_id].copy()
        if not subset.empty:
            working = subset
    elif candidate_id and {"signal_family", "lookback", "horizon"}.issubset(working.columns):
        family, lookback, horizon = candidate_id.split("|", 2)
        subset = working.loc[
            (working["signal_family"].astype(str) == family)
            & (pd.to_numeric(working["lookback"], errors="coerce") == float(lookback))
            & (pd.to_numeric(working["horizon"], errors="coerce") == float(horizon))
        ].copy()
        if not subset.empty:
            working = subset

    rows: list[ConditionalResearchResult] = []
    baseline_metric_name = "mean_spearman_ic" if _safe_float(top_metrics.get("mean_spearman_ic")) is not None else None
    baseline_metric_value = _safe_float(top_metrics.get("mean_spearman_ic"))
    for record in working.to_dict(orient="records"):
        if condition_type == "regime":
            condition_name = _safe_text(record.get("regime_key")) or "unlabeled_regime"
            definition = ConditionDefinition(
                condition_id=f"regime::{condition_name}",
                condition_type=condition_type,
                condition_name=condition_name,
                condition_source=condition_source,
                base_universe_id=universe,
                regime_label=condition_name,
                volatility_state_label=_safe_text(record.get("volatility_regime")),
                metadata_json={
                    "trend_regime": _safe_text(record.get("trend_regime")),
                    "dispersion_regime": _safe_text(record.get("dispersion_regime")),
                },
            )
        elif condition_type == "sub_universe":
            sub_universe_id = _safe_text(record.get("sub_universe_id")) or _safe_text(record.get("condition_name")) or "sub_universe"
            definition = ConditionDefinition(
                condition_id=f"sub_universe::{sub_universe_id}",
                condition_type=condition_type,
                condition_name=sub_universe_id,
                condition_source=condition_source,
                base_universe_id=universe,
                sub_universe_id=sub_universe_id,
            )
        elif condition_type == "benchmark_context":
            benchmark_label = _safe_text(record.get("benchmark_context_label")) or _safe_text(record.get("condition_name")) or "benchmark_context"
            definition = ConditionDefinition(
                condition_id=f"benchmark_context::{benchmark_label}",
                condition_type=condition_type,
                condition_name=benchmark_label,
                condition_source=condition_source,
                base_universe_id=universe,
                benchmark_context_label=benchmark_label,
            )
        else:
            condition_name = _safe_text(record.get("condition_name")) or _safe_text(record.get("condition_id")) or "condition"
            definition = ConditionDefinition(
                condition_id=f"{condition_type}::{condition_name}",
                condition_type=condition_type,
                condition_name=condition_name,
                condition_source=condition_source,
                base_universe_id=universe,
            )
        metric_name, metric_value = _metric_name_and_value(record)
        sample_size = _sample_size(record)
        improvement = (
            None
            if metric_value is None or baseline_metric_value is None or not config.compare_to_unconditional
            else metric_value - baseline_metric_value
        )
        rejection_reason = None
        recommendation = "unavailable"
        if metric_value is None:
            rejection_reason = "missing_metric"
            recommendation = "skip_missing_metric"
        elif sample_size is None or sample_size < config.min_sample_size:
            rejection_reason = f"sample_size_below_min:{sample_size if sample_size is not None else 'missing'}"
            recommendation = "reject_small_sample"
        elif improvement is not None and improvement < config.min_improvement:
            rejection_reason = f"improvement_below_min:{improvement:.4f}"
            recommendation = "reject_no_material_improvement"
        else:
            recommendation = "promote_conditional"
        rows.append(
            ConditionalResearchResult(
                condition=definition,
                signal_family=signal_family,
                candidate_id=candidate_id,
                metric_name=metric_name,
                metric_value=metric_value,
                baseline_metric_name=baseline_metric_name,
                baseline_metric_value=baseline_metric_value,
                improvement_vs_baseline=improvement,
                sample_size=sample_size,
                condition_status="available",
                recommendation=recommendation,
                rejection_reason=rejection_reason,
                metadata_json=record,
            )
        )
    return rows


def evaluate_conditional_research(
    *,
    output_dir: str | Path,
    artifact_paths: dict[str, Any],
    signal_family: str | None,
    top_candidate: dict[str, Any],
    top_metrics: dict[str, Any],
    universe: str | None,
    config: ConditionalEvaluationConfig | None = None,
) -> dict[str, Any]:
    active_config = config or ConditionalEvaluationConfig()
    result: dict[str, Any] = {
        "enabled": active_config.enabled,
        "config": asdict(active_config),
        "evaluated_condition_types": [],
        "available_condition_types": [],
        "unavailable_condition_types": [],
        "rows": [],
        "promotion_candidates": [],
        "summary": {},
        "artifacts": {},
    }
    if not active_config.enabled:
        return result

    sources = [
        ("regime", "signal_performance_by_regime_path", "signal_performance_by_regime"),
        ("sub_universe", "signal_performance_by_sub_universe_path", "signal_performance_by_sub_universe"),
        ("benchmark_context", "signal_performance_by_benchmark_context_path", "signal_performance_by_benchmark_context"),
    ]
    normalized_rows: list[dict[str, Any]] = []
    promotion_rows: list[dict[str, Any]] = []
    for condition_type, artifact_key, source_name in sources:
        if condition_type not in active_config.condition_types:
            continue
        result["evaluated_condition_types"].append(condition_type)
        frame = _safe_read_csv(artifact_paths.get(artifact_key))
        if frame.empty:
            result["unavailable_condition_types"].append(condition_type)
            continue
        result["available_condition_types"].append(condition_type)
        rows = _normalize_condition_rows(
            frame,
            condition_type=condition_type,
            condition_source=source_name,
            signal_family=signal_family,
            top_candidate=top_candidate,
            top_metrics=top_metrics,
            universe=universe,
            config=active_config,
        )
        for row in rows:
            normalized = row.to_dict()
            normalized["edge_summary"] = summarize_conditional_edge(normalized)
            normalized["coverage_summary"] = summarize_condition_coverage(normalized)
            normalized_rows.append(normalized)
            activation_condition = StrategyActivationCondition(
                condition_id=normalized["condition_id"],
                condition_type=normalized["condition_type"],
                condition_name=normalized["condition_name"],
                notes=summarize_strategy_activation_condition(normalized),
                metadata_json={
                    "condition_source": normalized["condition_source"],
                    "base_universe_id": normalized.get("base_universe_id"),
                    "sub_universe_id": normalized.get("sub_universe_id"),
                    "regime_label": normalized.get("regime_label"),
                    "benchmark_context_label": normalized.get("benchmark_context_label"),
                },
            ).to_dict()
            eligible = normalized["recommendation"] == "promote_conditional"
            decision = ConditionalPromotionDecision(
                condition_id=normalized["condition_id"],
                condition_type=normalized["condition_type"],
                recommendation=normalized["recommendation"],
                eligible=eligible,
                signal_family=normalized.get("signal_family"),
                sample_size=normalized.get("sample_size"),
                metric_name=normalized.get("metric_name"),
                metric_value=normalized.get("metric_value"),
                baseline_metric_value=normalized.get("baseline_metric_value"),
                improvement_vs_baseline=normalized.get("improvement_vs_baseline"),
                reason=normalized.get("rejection_reason") or normalized["edge_summary"],
                activation_condition=activation_condition,
            ).to_dict()
            decision["promotion_summary"] = summarize_conditional_promotion(decision)
            promotion_rows.append(decision)

    promotion_rows.sort(
        key=lambda row: (
            bool(row.get("eligible")),
            _safe_float(row.get("improvement_vs_baseline")) or float("-inf"),
            _safe_int(row.get("sample_size")) or -1,
            str(row.get("condition_id") or ""),
        ),
        reverse=True,
    )
    result["rows"] = normalized_rows
    result["promotion_candidates"] = promotion_rows
    result["summary"] = {
        "row_count": len(normalized_rows),
        "eligible_condition_count": sum(1 for row in promotion_rows if row.get("eligible")),
        "best_condition_id": promotion_rows[0]["condition_id"] if promotion_rows else None,
        "best_condition_summary": promotion_rows[0]["promotion_summary"] if promotion_rows else None,
        "available_condition_types": list(result["available_condition_types"]),
        "unavailable_condition_types": list(result["unavailable_condition_types"]),
    }

    if not normalized_rows:
        return result

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    normalized_df = pd.DataFrame(normalized_rows)
    promotion_df = pd.DataFrame(promotion_rows)
    csv_path = output_path / "conditional_signal_performance.csv"
    json_path = output_path / "conditional_signal_performance.json"
    summary_path = output_path / "conditional_research_summary.json"
    promotion_path = output_path / "conditional_promotion_candidates.csv"
    normalized_df.to_csv(csv_path, index=False)
    _write_json(
        json_path,
        {
            "summary": result["summary"],
            "rows": normalized_rows,
        },
    )
    _write_json(
        summary_path,
        {
            "summary": result["summary"],
            "config": result["config"],
            "promotion_candidates": promotion_rows,
        },
    )
    promotion_df.to_csv(promotion_path, index=False)
    result["artifacts"] = {
        "conditional_signal_performance_path": str(csv_path),
        "conditional_signal_performance_json_path": str(json_path),
        "conditional_research_summary_path": str(summary_path),
        "conditional_promotion_candidates_path": str(promotion_path),
    }
    return result
