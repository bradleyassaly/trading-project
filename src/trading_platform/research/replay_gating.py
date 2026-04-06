from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from trading_platform.governance.models import (
    PromotionDecision,
    PromotionGateEvaluation,
    PromotionGateResult,
)
from trading_platform.research.replay_history import ReplayHistoryRecord


REPLAY_GATING_SCHEMA_VERSION = "shared_replay_gating_v1"


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(mean(clean))


def _safe_std(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if len(clean) < 2:
        return 0.0 if clean else None
    return float(pstdev(clean))


def _gate_result(
    *,
    gate_name: str,
    passed: bool,
    reason_code: str,
    actual: float | int | None,
    threshold: float | int | None,
    comparator: str | None,
    message: str,
    severity: str,
    metadata: dict[str, Any] | None = None,
) -> PromotionGateResult:
    payload = {"severity": severity}
    if metadata:
        payload.update(metadata)
    return PromotionGateResult(
        gate_name=gate_name,
        passed=passed,
        reason_code=reason_code,
        actual=actual,
        threshold=threshold,
        comparator=comparator,
        message=message,
        metadata=payload,
    )


@dataclass(frozen=True)
class ReplayResearchGateThresholds:
    min_row_count: int = 25
    min_replay_runs: int = 2
    min_mean_abs_spearman: float = 0.02
    min_mean_directional_accuracy: float = 0.51
    min_mean_composite_score: float = 0.10
    max_score_stddev: float = 0.20
    max_best_rank: int = 5
    max_best_rank_percentile: float = 0.50
    min_provider_count: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplayCandidateHistorySummary:
    candidate_id: str
    providers: list[str]
    dataset_keys: list[str]
    alignment_modes: list[str]
    feature_column: str | None
    target_column: str | None
    replay_run_count: int
    comparison_run_count: int
    source_run_count: int
    provider_count: int
    mean_row_count: float | None
    mean_abs_spearman: float | None
    mean_directional_accuracy: float | None
    mean_composite_score: float | None
    score_stddev: float | None
    best_rank: int | None
    best_rank_percentile: float | None
    provenance_paths: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplayResearchGateDecision:
    candidate_id: str
    overall_status: str
    promotion_decision: PromotionDecision
    rejection_reasons: list[str]
    watchlist_reasons: list[str]
    supporting_metrics: dict[str, Any]
    providers: list[str] = field(default_factory=list)
    dataset_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "overall_status": self.overall_status,
            "promotion_decision": self.promotion_decision.to_dict(),
            "rejection_reasons": list(self.rejection_reasons),
            "watchlist_reasons": list(self.watchlist_reasons),
            "supporting_metrics": dict(self.supporting_metrics),
            "providers": list(self.providers),
            "dataset_keys": list(self.dataset_keys),
        }


@dataclass(frozen=True)
class ReplayResearchGatingResult:
    generated_at: str
    thresholds: ReplayResearchGateThresholds
    history_summary: dict[str, Any]
    candidate_summaries: list[ReplayCandidateHistorySummary]
    decisions: list[ReplayResearchGateDecision]
    warnings: list[str] = field(default_factory=list)

    def to_summary(self) -> dict[str, Any]:
        promotable = [decision.to_dict() for decision in self.decisions if decision.overall_status == "promotable"]
        watchlist = [decision.to_dict() for decision in self.decisions if decision.overall_status == "watchlist"]
        rejected = [decision.to_dict() for decision in self.decisions if decision.overall_status == "rejected"]
        return {
            "schema_version": REPLAY_GATING_SCHEMA_VERSION,
            "generated_at": self.generated_at,
            "thresholds": self.thresholds.to_dict(),
            "history_summary": dict(self.history_summary),
            "summary_counts": {
                "candidate_count": len(self.decisions),
                "promotable_count": len(promotable),
                "watchlist_count": len(watchlist),
                "rejected_count": len(rejected),
            },
            "candidate_summaries": [summary.to_dict() for summary in self.candidate_summaries],
            "decisions": [decision.to_dict() for decision in self.decisions],
            "promotable_candidates": promotable,
            "watchlist_candidates": watchlist,
            "rejected_candidates": rejected,
            "warnings": list(self.warnings),
        }


def summarize_replay_history(records: list[ReplayHistoryRecord]) -> list[ReplayCandidateHistorySummary]:
    grouped: dict[str, list[ReplayHistoryRecord]] = {}
    for record in records:
        grouped.setdefault(record.candidate_id, []).append(record)

    summaries: list[ReplayCandidateHistorySummary] = []
    for candidate_id, candidate_records in grouped.items():
        first = candidate_records[0]
        providers = sorted({provider for record in candidate_records for provider in record.providers})
        dataset_keys = sorted({key for record in candidate_records for key in record.dataset_keys})
        alignment_modes = sorted({record.alignment_mode for record in candidate_records if record.alignment_mode})
        source_keys = {
            (record.source_type, record.source_name, record.source_generated_at, record.source_summary_path)
            for record in candidate_records
        }
        provenance_paths = sorted(
            {
                str(record.source_summary_path)
                for record in candidate_records
                if record.source_summary_path
            }
        )
        summaries.append(
            ReplayCandidateHistorySummary(
                candidate_id=candidate_id,
                providers=providers,
                dataset_keys=dataset_keys,
                alignment_modes=alignment_modes,
                feature_column=first.feature_column,
                target_column=first.target_column,
                replay_run_count=sum(1 for record in candidate_records if record.source_type == "evaluation"),
                comparison_run_count=sum(1 for record in candidate_records if record.source_type == "comparison"),
                source_run_count=len(source_keys),
                provider_count=len(providers),
                mean_row_count=_safe_mean([record.row_count for record in candidate_records]),
                mean_abs_spearman=_safe_mean(
                    [abs(record.spearman_correlation) if record.spearman_correlation is not None else None for record in candidate_records]
                ),
                mean_directional_accuracy=_safe_mean([record.directional_accuracy for record in candidate_records]),
                mean_composite_score=_safe_mean([record.composite_score for record in candidate_records]),
                score_stddev=_safe_std([record.composite_score for record in candidate_records]),
                best_rank=min(
                    [record.comparison_rank for record in candidate_records if record.comparison_rank is not None],
                    default=None,
                ),
                best_rank_percentile=min(
                    [
                        record.comparison_rank_percentile
                        for record in candidate_records
                        if record.comparison_rank_percentile is not None
                    ],
                    default=None,
                ),
                provenance_paths=provenance_paths,
            )
        )
    summaries.sort(
        key=lambda item: (
            -(item.mean_composite_score or -999.0),
            -(item.mean_abs_spearman or -999.0),
            -(item.mean_directional_accuracy or -999.0),
            item.candidate_id,
        )
    )
    return summaries


def _evaluate_candidate_summary(
    summary: ReplayCandidateHistorySummary,
    *,
    thresholds: ReplayResearchGateThresholds,
) -> ReplayResearchGateDecision:
    gate_results: list[PromotionGateResult] = []
    gate_results.append(
        _gate_result(
            gate_name="minimum_sample_size",
            passed=(summary.mean_row_count or 0.0) >= thresholds.min_row_count,
            reason_code="insufficient_replay_rows",
            actual=summary.mean_row_count,
            threshold=thresholds.min_row_count,
            comparator=">=",
            message="Mean replay row count must clear the minimum sample threshold.",
            severity="hard",
        )
    )
    gate_results.append(
        _gate_result(
            gate_name="minimum_replay_runs",
            passed=summary.source_run_count >= thresholds.min_replay_runs,
            reason_code="insufficient_replay_runs",
            actual=summary.source_run_count,
            threshold=thresholds.min_replay_runs,
            comparator=">=",
            message="Candidate must appear in enough replay evaluation/comparison runs.",
            severity="hard",
        )
    )
    gate_results.append(
        _gate_result(
            gate_name="predictive_signal_strength",
            passed=(summary.mean_abs_spearman or 0.0) >= thresholds.min_mean_abs_spearman,
            reason_code="weak_rank_signal",
            actual=summary.mean_abs_spearman,
            threshold=thresholds.min_mean_abs_spearman,
            comparator=">=",
            message="Mean absolute Spearman correlation must clear the minimum signal threshold.",
            severity="hard",
        )
    )
    gate_results.append(
        _gate_result(
            gate_name="directional_accuracy",
            passed=(summary.mean_directional_accuracy or 0.0) >= thresholds.min_mean_directional_accuracy,
            reason_code="weak_directional_accuracy",
            actual=summary.mean_directional_accuracy,
            threshold=thresholds.min_mean_directional_accuracy,
            comparator=">=",
            message="Directional accuracy must remain above the configured baseline.",
            severity="soft",
        )
    )
    gate_results.append(
        _gate_result(
            gate_name="composite_score",
            passed=(summary.mean_composite_score or 0.0) >= thresholds.min_mean_composite_score,
            reason_code="weak_composite_score",
            actual=summary.mean_composite_score,
            threshold=thresholds.min_mean_composite_score,
            comparator=">=",
            message="Mean comparison score must clear the configured research-selection threshold.",
            severity="soft",
        )
    )
    gate_results.append(
        _gate_result(
            gate_name="score_stability",
            passed=(summary.score_stddev or 0.0) <= thresholds.max_score_stddev,
            reason_code="unstable_replay_scores",
            actual=summary.score_stddev,
            threshold=thresholds.max_score_stddev,
            comparator="<=",
            message="Replay comparison scores should remain stable across runs.",
            severity="soft",
        )
    )
    rank_available = summary.best_rank is not None and summary.best_rank_percentile is not None
    rank_passed = True
    rank_reason = "comparison_rank_not_available"
    rank_message = "Comparison rank is not yet available for this candidate."
    if rank_available:
        rank_passed = (
            summary.best_rank <= thresholds.max_best_rank
            and summary.best_rank_percentile <= thresholds.max_best_rank_percentile
        )
        rank_reason = "weak_comparison_rank"
        rank_message = "Best comparison rank must remain within the configured promotion window."
    gate_results.append(
        _gate_result(
            gate_name="comparison_rank",
            passed=rank_passed,
            reason_code=rank_reason,
            actual=summary.best_rank_percentile if summary.best_rank_percentile is not None else summary.best_rank,
            threshold=thresholds.max_best_rank_percentile if rank_available else None,
            comparator="<=" if rank_available else None,
            message=rank_message,
            severity="soft",
            metadata={"applicable": rank_available},
        )
    )
    gate_results.append(
        _gate_result(
            gate_name="cross_provider_support",
            passed=summary.provider_count >= thresholds.min_provider_count,
            reason_code="insufficient_provider_support",
            actual=summary.provider_count,
            threshold=thresholds.min_provider_count,
            comparator=">=",
            message="Candidate should be supported by the configured number of providers.",
            severity="soft",
        )
    )

    failed_gates = [gate for gate in gate_results if not gate.passed]
    failed_gate_names = [gate.gate_name for gate in failed_gates]
    passed_gate_names = [gate.gate_name for gate in gate_results if gate.passed]
    hard_failures = [gate for gate in failed_gates if gate.metadata.get("severity") == "hard"]
    rejection_reasons = [gate.reason_code for gate in hard_failures]
    watchlist_reasons = [gate.reason_code for gate in failed_gates if gate.metadata.get("severity") != "hard"]

    if hard_failures:
        overall_status = "rejected"
    elif failed_gates:
        overall_status = "watchlist"
    else:
        overall_status = "promotable"

    evaluation = PromotionGateEvaluation(
        candidate_id=summary.candidate_id,
        passed=overall_status == "promotable",
        gate_results=gate_results,
        rejection_reasons=rejection_reasons + watchlist_reasons,
        passed_gate_names=passed_gate_names,
        failed_gate_names=failed_gate_names,
        metadata={
            "providers": list(summary.providers),
            "dataset_keys": list(summary.dataset_keys),
            "alignment_modes": list(summary.alignment_modes),
            "replay_run_count": summary.source_run_count,
        },
    )
    decision = PromotionDecision.from_gate_evaluation(
        evaluation,
        final_status=overall_status,
        summary_metadata={
            "overall_status": overall_status,
            "watchlist_reason_count": len(watchlist_reasons),
            "rejection_reason_count": len(rejection_reasons),
        },
    )
    return ReplayResearchGateDecision(
        candidate_id=summary.candidate_id,
        overall_status=overall_status,
        promotion_decision=decision,
        rejection_reasons=rejection_reasons,
        watchlist_reasons=watchlist_reasons,
        supporting_metrics=summary.to_dict(),
        providers=list(summary.providers),
        dataset_keys=list(summary.dataset_keys),
    )


def evaluate_research_gates(
    records: list[ReplayHistoryRecord],
    *,
    thresholds: ReplayResearchGateThresholds | None = None,
) -> ReplayResearchGatingResult:
    thresholds = thresholds or ReplayResearchGateThresholds()
    candidate_summaries = summarize_replay_history(records)
    decisions = [
        _evaluate_candidate_summary(summary, thresholds=thresholds)
        for summary in candidate_summaries
    ]
    decisions.sort(
        key=lambda item: (
            {"promotable": 0, "watchlist": 1, "rejected": 2}.get(item.overall_status, 3),
            -(item.supporting_metrics.get("mean_composite_score") or -999.0),
            item.candidate_id,
        )
    )
    history_paths = sorted(
        {
            str(record.source_summary_path)
            for record in records
            if record.source_summary_path
        }
    )
    return ReplayResearchGatingResult(
        generated_at=_now_utc(),
        thresholds=thresholds,
        history_summary={
            "record_count": len(records),
            "candidate_count": len(candidate_summaries),
            "source_paths": history_paths,
        },
        candidate_summaries=candidate_summaries,
        decisions=decisions,
        warnings=[] if records else ["no_replay_history_records"],
    )


def write_research_gating_artifacts(
    *,
    result: ReplayResearchGatingResult,
    output_dir: str | Path,
) -> dict[str, str]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / "latest_research_gating_summary.json"
    promotable_path = root / "latest_promotable_candidates.json"
    watchlist_path = root / "latest_watchlist_candidates.json"
    rejected_path = root / "latest_rejected_candidates.json"
    summary = result.to_summary()
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    promotable_path.write_text(json.dumps(summary["promotable_candidates"], indent=2), encoding="utf-8")
    watchlist_path.write_text(json.dumps(summary["watchlist_candidates"], indent=2), encoding="utf-8")
    rejected_path.write_text(json.dumps(summary["rejected_candidates"], indent=2), encoding="utf-8")
    return {
        "summary_path": str(summary_path),
        "promotable_path": str(promotable_path),
        "watchlist_path": str(watchlist_path),
        "rejected_path": str(rejected_path),
    }
