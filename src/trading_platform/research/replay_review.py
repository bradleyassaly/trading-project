from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from trading_platform.governance.models import PromotionGateResult
from trading_platform.research.replay_history import ReplayHistoryRecord, filter_replay_history


REPLAY_REVIEW_SCHEMA_VERSION = "shared_replay_review_v1"


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
class ReplayDriftThresholds:
    recent_window: int = 4
    min_recent_runs: int = 2
    max_abs_spearman_drop: float = 0.03
    max_directional_accuracy_drop: float = 0.03
    max_rank_percentile_worsening: float = 0.25
    max_recent_score_stddev: float = 0.20
    min_recent_provider_count: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplayCandidateDriftDecision:
    candidate_id: str
    provider_scope: str
    drift_status: str
    recommendation: str
    check_results: list[PromotionGateResult]
    reasons: list[str] = field(default_factory=list)
    supporting_metrics: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "provider_scope": self.provider_scope,
            "drift_status": self.drift_status,
            "recommendation": self.recommendation,
            "check_results": [row.to_dict() for row in self.check_results],
            "reasons": list(self.reasons),
            "supporting_metrics": dict(self.supporting_metrics),
            "provenance": dict(self.provenance),
        }


@dataclass(frozen=True)
class ReplayDriftResult:
    generated_at: str
    thresholds: ReplayDriftThresholds
    decisions: list[ReplayCandidateDriftDecision]
    warnings: list[str] = field(default_factory=list)

    def to_summary(self) -> dict[str, Any]:
        stable = [item.to_dict() for item in self.decisions if item.drift_status == "stable"]
        warning = [item.to_dict() for item in self.decisions if item.drift_status == "warning"]
        drifted = [item.to_dict() for item in self.decisions if item.drift_status == "drifted"]
        return {
            "schema_version": REPLAY_REVIEW_SCHEMA_VERSION,
            "generated_at": self.generated_at,
            "thresholds": self.thresholds.to_dict(),
            "summary_counts": {
                "candidate_count": len(self.decisions),
                "stable_count": len(stable),
                "warning_count": len(warning),
                "drifted_count": len(drifted),
            },
            "decisions": [item.to_dict() for item in self.decisions],
            "stable_candidates": stable,
            "warning_candidates": warning,
            "drifted_candidates": drifted,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class ReplayReviewQueueEntry:
    queue_state: str
    candidate_id: str
    provider_scope: str
    providers: list[str]
    latest_gating_status: str
    latest_drift_status: str
    queue_reasons: list[str]
    supporting_metrics: dict[str, Any]
    updated_at: str
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplayReviewQueueResult:
    generated_at: str
    entries: list[ReplayReviewQueueEntry]
    warnings: list[str] = field(default_factory=list)

    def to_summary(self) -> dict[str, Any]:
        grouped: dict[str, list[dict[str, Any]]] = {
            "promotable_review": [],
            "watchlist_review": [],
            "rejected_archive": [],
            "needs_rerun": [],
        }
        for entry in self.entries:
            grouped.setdefault(entry.queue_state, []).append(entry.to_dict())
        return {
            "schema_version": REPLAY_REVIEW_SCHEMA_VERSION,
            "generated_at": self.generated_at,
            "summary_counts": {f"{key}_count": len(value) for key, value in grouped.items()},
            "entries": [entry.to_dict() for entry in self.entries],
            **grouped,
            "warnings": list(self.warnings),
        }


def _recent_and_baseline(
    records: list[ReplayHistoryRecord],
    *,
    recent_window: int,
) -> tuple[list[ReplayHistoryRecord], list[ReplayHistoryRecord]]:
    ordered = sorted(records, key=lambda item: (item.source_generated_at or item.recorded_at, item.record_id))
    if not ordered:
        return [], []
    if len(ordered) <= recent_window:
        return ordered, []
    return ordered[-recent_window:], ordered[:-recent_window]


def evaluate_replay_drift(
    *,
    history_records: list[ReplayHistoryRecord],
    gating_summary: dict[str, Any],
    thresholds: ReplayDriftThresholds | None = None,
) -> ReplayDriftResult:
    thresholds = thresholds or ReplayDriftThresholds()
    warnings: list[str] = []
    decisions: list[ReplayCandidateDriftDecision] = []
    for decision in list(gating_summary.get("decisions") or []):
        candidate_id = str(decision.get("candidate_id") or "")
        candidate_records = filter_replay_history(history_records, candidate_id=candidate_id)
        recent, baseline = _recent_and_baseline(candidate_records, recent_window=thresholds.recent_window)
        latest_record = recent[-1] if recent else None
        providers = sorted({provider for record in recent for provider in record.providers})
        provider_scope = "+".join(providers) or "unknown_provider"

        recent_mean_abs_spearman = _safe_mean(
            [abs(record.spearman_correlation) if record.spearman_correlation is not None else None for record in recent]
        )
        baseline_mean_abs_spearman = _safe_mean(
            [abs(record.spearman_correlation) if record.spearman_correlation is not None else None for record in baseline]
        )
        recent_mean_directional = _safe_mean([record.directional_accuracy for record in recent])
        baseline_mean_directional = _safe_mean([record.directional_accuracy for record in baseline])
        recent_best_percentile = min(
            [record.comparison_rank_percentile for record in recent if record.comparison_rank_percentile is not None],
            default=None,
        )
        baseline_best_percentile = min(
            [record.comparison_rank_percentile for record in baseline if record.comparison_rank_percentile is not None],
            default=None,
        )
        recent_score_stddev = _safe_std([record.composite_score for record in recent])

        recent_run_count = len(
            {
                (record.source_type, record.source_name, record.source_generated_at, record.source_summary_path)
                for record in recent
            }
        )
        recent_provider_count = len(providers)
        spearman_drop = (
            baseline_mean_abs_spearman - recent_mean_abs_spearman
            if baseline_mean_abs_spearman is not None and recent_mean_abs_spearman is not None
            else None
        )
        directional_drop = (
            baseline_mean_directional - recent_mean_directional
            if baseline_mean_directional is not None and recent_mean_directional is not None
            else None
        )
        rank_worsening = (
            recent_best_percentile - baseline_best_percentile
            if baseline_best_percentile is not None and recent_best_percentile is not None
            else None
        )
        latest_abs_spearman = abs(latest_record.spearman_correlation) if latest_record and latest_record.spearman_correlation is not None else None
        latest_directional_accuracy = latest_record.directional_accuracy if latest_record else None
        latest_spearman_drop = (
            baseline_mean_abs_spearman - latest_abs_spearman
            if baseline_mean_abs_spearman is not None and latest_abs_spearman is not None
            else None
        )
        latest_directional_drop = (
            baseline_mean_directional - latest_directional_accuracy
            if baseline_mean_directional is not None and latest_directional_accuracy is not None
            else None
        )

        check_results = [
            _gate_result(
                gate_name="recent_support",
                passed=recent_run_count >= thresholds.min_recent_runs,
                reason_code="insufficient_recent_support",
                actual=recent_run_count,
                threshold=thresholds.min_recent_runs,
                comparator=">=",
                message="Candidate should have enough recent replay support.",
                severity="hard",
            ),
            _gate_result(
                gate_name="spearman_drift",
                passed=(
                    baseline_mean_abs_spearman is None
                    or recent_mean_abs_spearman is None
                    or recent_mean_abs_spearman >= baseline_mean_abs_spearman - thresholds.max_abs_spearman_drop
                ),
                reason_code="spearman_drifted",
                actual=recent_mean_abs_spearman,
                threshold=(baseline_mean_abs_spearman - thresholds.max_abs_spearman_drop)
                if baseline_mean_abs_spearman is not None
                else None,
                comparator=">=" if baseline_mean_abs_spearman is not None else None,
                message="Recent rank-signal strength should not degrade too far versus trailing history.",
                severity="hard"
                if (
                    (spearman_drop is not None and spearman_drop > thresholds.max_abs_spearman_drop * 2.0)
                    or (latest_spearman_drop is not None and latest_spearman_drop > thresholds.max_abs_spearman_drop * 2.0)
                )
                else "soft",
                metadata={"baseline_mean_abs_spearman": baseline_mean_abs_spearman, "drop": spearman_drop, "latest_drop": latest_spearman_drop},
            ),
            _gate_result(
                gate_name="directional_accuracy_drift",
                passed=(
                    baseline_mean_directional is None
                    or recent_mean_directional is None
                    or recent_mean_directional >= baseline_mean_directional - thresholds.max_directional_accuracy_drop
                ),
                reason_code="directional_accuracy_drifted",
                actual=recent_mean_directional,
                threshold=(baseline_mean_directional - thresholds.max_directional_accuracy_drop)
                if baseline_mean_directional is not None
                else None,
                comparator=">=" if baseline_mean_directional is not None else None,
                message="Recent directional accuracy should remain close to trailing history.",
                severity="hard"
                if (
                    (directional_drop is not None and directional_drop > thresholds.max_directional_accuracy_drop * 2.0)
                    or (
                        latest_directional_drop is not None
                        and latest_directional_drop > thresholds.max_directional_accuracy_drop * 2.0
                    )
                )
                else "soft",
                metadata={"baseline_mean_directional_accuracy": baseline_mean_directional, "drop": directional_drop, "latest_drop": latest_directional_drop},
            ),
            _gate_result(
                gate_name="rank_percentile_drift",
                passed=(
                    baseline_best_percentile is None
                    or recent_best_percentile is None
                    or recent_best_percentile <= baseline_best_percentile + thresholds.max_rank_percentile_worsening
                ),
                reason_code="rank_percentile_drifted",
                actual=recent_best_percentile,
                threshold=(baseline_best_percentile + thresholds.max_rank_percentile_worsening)
                if baseline_best_percentile is not None
                else None,
                comparator="<=" if baseline_best_percentile is not None else None,
                message="Recent ranking percentile should not worsen too far versus trailing history.",
                severity="hard"
                if rank_worsening is not None and rank_worsening > thresholds.max_rank_percentile_worsening * 2.0
                else "soft",
                metadata={"baseline_best_rank_percentile": baseline_best_percentile, "worsening": rank_worsening},
            ),
            _gate_result(
                gate_name="recent_score_stability",
                passed=(recent_score_stddev or 0.0) <= thresholds.max_recent_score_stddev,
                reason_code="recent_score_instability",
                actual=recent_score_stddev,
                threshold=thresholds.max_recent_score_stddev,
                comparator="<=",
                message="Recent replay comparison scores should stay stable.",
                severity="soft",
            ),
            _gate_result(
                gate_name="recent_provider_support",
                passed=recent_provider_count >= thresholds.min_recent_provider_count,
                reason_code="insufficient_recent_provider_support",
                actual=recent_provider_count,
                threshold=thresholds.min_recent_provider_count,
                comparator=">=",
                message="Recent history should retain enough provider support.",
                severity="soft",
            ),
        ]

        failed = [item for item in check_results if not item.passed]
        hard_failed = [item for item in failed if item.metadata.get("severity") == "hard"]
        reasons = [item.reason_code for item in failed]
        if hard_failed:
            drift_status = "drifted"
            recommendation = "rerun"
        elif failed:
            drift_status = "warning"
            recommendation = "keep_in_queue"
        else:
            drift_status = "stable"
            recommendation = "keep_in_queue"

        if drift_status == "drifted" and decision.get("overall_status") != "rejected":
            recommendation = "rerun"
        elif decision.get("overall_status") == "rejected":
            recommendation = "deprioritize"

        decisions.append(
            ReplayCandidateDriftDecision(
                candidate_id=candidate_id,
                provider_scope=provider_scope,
                drift_status=drift_status,
                recommendation=recommendation,
                check_results=check_results,
                reasons=reasons,
                supporting_metrics={
                    "recent_run_count": recent_run_count,
                    "recent_provider_count": recent_provider_count,
                    "recent_mean_abs_spearman": recent_mean_abs_spearman,
                    "baseline_mean_abs_spearman": baseline_mean_abs_spearman,
                    "recent_mean_directional_accuracy": recent_mean_directional,
                    "baseline_mean_directional_accuracy": baseline_mean_directional,
                    "recent_best_rank_percentile": recent_best_percentile,
                    "baseline_best_rank_percentile": baseline_best_percentile,
                    "recent_score_stddev": recent_score_stddev,
                    "latest_abs_spearman": latest_abs_spearman,
                    "latest_directional_accuracy": latest_directional_accuracy,
                },
                provenance={
                    "history_record_count": len(candidate_records),
                    "gating_status": decision.get("overall_status"),
                },
            )
        )
    decisions.sort(key=lambda item: ({"drifted": 0, "warning": 1, "stable": 2}.get(item.drift_status, 3), item.candidate_id))
    if not decisions:
        warnings.append("no_candidates_available_for_drift")
    return ReplayDriftResult(
        generated_at=_now_utc(),
        thresholds=thresholds,
        decisions=decisions,
        warnings=warnings,
    )


def build_research_review_queue(
    *,
    gating_summary: dict[str, Any],
    drift_result: ReplayDriftResult,
    history_path: str | None = None,
    gating_summary_path: str | None = None,
    drift_summary_path: str | None = None,
) -> ReplayReviewQueueResult:
    drift_map = {item.candidate_id: item for item in drift_result.decisions}
    entries: list[ReplayReviewQueueEntry] = []
    for decision in list(gating_summary.get("decisions") or []):
        candidate_id = str(decision.get("candidate_id") or "")
        gating_status = str(decision.get("overall_status") or "unknown")
        drift = drift_map.get(candidate_id)
        providers = list(decision.get("providers") or [])
        provider_scope = "+".join(providers) or "unknown_provider"
        queue_state = "rejected_archive"
        if gating_status == "promotable":
            queue_state = "promotable_review"
        elif gating_status == "watchlist":
            queue_state = "watchlist_review"
        if drift and drift.recommendation == "rerun":
            queue_state = "needs_rerun"
        reasons = [*list(decision.get("watchlist_reasons") or []), *list(decision.get("rejection_reasons") or [])]
        if drift:
            reasons.extend(drift.reasons)
        entries.append(
            ReplayReviewQueueEntry(
                queue_state=queue_state,
                candidate_id=candidate_id,
                provider_scope=provider_scope,
                providers=providers,
                latest_gating_status=gating_status,
                latest_drift_status=drift.drift_status if drift else "unknown",
                queue_reasons=sorted(set(reason for reason in reasons if reason)),
                supporting_metrics={
                    "gating_metrics": dict(decision.get("supporting_metrics") or {}),
                    "drift_metrics": dict(drift.supporting_metrics) if drift else {},
                },
                updated_at=_now_utc(),
                provenance={
                    "history_path": history_path,
                    "gating_summary_path": gating_summary_path,
                    "drift_summary_path": drift_summary_path,
                },
            )
        )
    entries.sort(key=lambda item: ({"needs_rerun": 0, "promotable_review": 1, "watchlist_review": 2, "rejected_archive": 3}.get(item.queue_state, 4), item.candidate_id))
    return ReplayReviewQueueResult(generated_at=_now_utc(), entries=entries, warnings=[])


def write_replay_review_artifacts(
    *,
    drift_result: ReplayDriftResult,
    queue_result: ReplayReviewQueueResult,
    output_dir: str | Path,
) -> dict[str, str]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    drift_path = root / "latest_replay_drift_summary.json"
    queue_path = root / "latest_review_queue_summary.json"
    promotable_path = root / "latest_promotable_review.json"
    watchlist_path = root / "latest_watchlist_review.json"
    rerun_path = root / "latest_needs_rerun.json"
    rejected_path = root / "latest_rejected_archive.json"
    drift_summary = drift_result.to_summary()
    queue_summary = queue_result.to_summary()
    drift_path.write_text(json.dumps(drift_summary, indent=2), encoding="utf-8")
    queue_path.write_text(json.dumps(queue_summary, indent=2), encoding="utf-8")
    promotable_path.write_text(json.dumps(queue_summary["promotable_review"], indent=2), encoding="utf-8")
    watchlist_path.write_text(json.dumps(queue_summary["watchlist_review"], indent=2), encoding="utf-8")
    rerun_path.write_text(json.dumps(queue_summary["needs_rerun"], indent=2), encoding="utf-8")
    rejected_path.write_text(json.dumps(queue_summary["rejected_archive"], indent=2), encoding="utf-8")
    return {
        "drift_summary_path": str(drift_path),
        "queue_summary_path": str(queue_path),
        "promotable_review_path": str(promotable_path),
        "watchlist_review_path": str(watchlist_path),
        "needs_rerun_path": str(rerun_path),
        "rejected_archive_path": str(rejected_path),
    }
