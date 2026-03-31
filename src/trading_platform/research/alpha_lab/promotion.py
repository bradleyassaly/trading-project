from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from trading_platform.governance.models import (
    PromotionDecision,
    PromotionGateEvaluation,
    PromotionGateResult,
)


@dataclass(frozen=True)
class PromotionThresholds:
    min_mean_spearman_ic: float = 0.02
    min_symbols_tested: float = 2.0
    min_folds_tested: int = 2
    min_mean_dates_evaluated: float = 3.0
    min_total_obs: float = 100.0
    max_mean_turnover: float = 0.75
    min_worst_fold_spearman_ic: float = -0.10

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


DEFAULT_PROMOTION_THRESHOLDS = PromotionThresholds()


def _candidate_id(row: pd.Series) -> str:
    explicit_candidate_id = str(row.get("candidate_id") or "").strip()
    if explicit_candidate_id:
        return explicit_candidate_id
    parts = [
        str(row.get("signal_family") or "").strip(),
        str(row.get("lookback") or "").strip(),
        str(row.get("horizon") or "").strip(),
    ]
    candidate_id = "|".join(part for part in parts if part)
    return candidate_id or "unknown_candidate"


def _promotion_gate_result(
    *,
    gate_name: str,
    passed: bool,
    reason_code: str,
    actual: float | int | None,
    threshold: float | int,
    comparator: str,
) -> PromotionGateResult:
    status = "passed" if passed else "failed"
    message = f"{gate_name} {status}: actual={actual} comparator={comparator} threshold={threshold}"
    return PromotionGateResult(
        gate_name=gate_name,
        passed=passed,
        reason_code=reason_code,
        actual=actual,
        threshold=threshold,
        comparator=comparator,
        message=message,
    )


def evaluate_promotion_gate(row: pd.Series, *, thresholds: PromotionThresholds = DEFAULT_PROMOTION_THRESHOLDS) -> PromotionGateEvaluation:
    gate_results = [
        _promotion_gate_result(
            gate_name="mean_rank_ic",
            passed=(not pd.isna(row["mean_spearman_ic"]) and row["mean_spearman_ic"] > thresholds.min_mean_spearman_ic),
            reason_code="low_mean_rank_ic",
            actual=None if pd.isna(row["mean_spearman_ic"]) else float(row["mean_spearman_ic"]),
            threshold=float(thresholds.min_mean_spearman_ic),
            comparator=">",
        ),
        _promotion_gate_result(
            gate_name="symbols_tested",
            passed=float(row["symbols_tested"]) >= thresholds.min_symbols_tested,
            reason_code="insufficient_symbols",
            actual=float(row["symbols_tested"]),
            threshold=float(thresholds.min_symbols_tested),
            comparator=">=",
        ),
        _promotion_gate_result(
            gate_name="folds_tested",
            passed=int(row["folds_tested"]) >= thresholds.min_folds_tested,
            reason_code="insufficient_folds",
            actual=int(row["folds_tested"]),
            threshold=int(thresholds.min_folds_tested),
            comparator=">=",
        ),
        _promotion_gate_result(
            gate_name="dates_evaluated",
            passed=float(row["mean_dates_evaluated"]) >= thresholds.min_mean_dates_evaluated,
            reason_code="insufficient_dates",
            actual=float(row["mean_dates_evaluated"]),
            threshold=float(thresholds.min_mean_dates_evaluated),
            comparator=">=",
        ),
        _promotion_gate_result(
            gate_name="total_observations",
            passed=float(row["total_obs"]) >= thresholds.min_total_obs,
            reason_code="insufficient_observations",
            actual=float(row["total_obs"]),
            threshold=float(thresholds.min_total_obs),
            comparator=">=",
        ),
        _promotion_gate_result(
            gate_name="turnover",
            passed=(not pd.isna(row["mean_turnover"]) and row["mean_turnover"] <= thresholds.max_mean_turnover),
            reason_code="high_turnover",
            actual=None if pd.isna(row["mean_turnover"]) else float(row["mean_turnover"]),
            threshold=float(thresholds.max_mean_turnover),
            comparator="<=",
        ),
        _promotion_gate_result(
            gate_name="worst_fold_rank_ic",
            passed=(
                not pd.isna(row["worst_fold_spearman_ic"])
                and row["worst_fold_spearman_ic"] >= thresholds.min_worst_fold_spearman_ic
            ),
            reason_code="weak_worst_fold_rank_ic",
            actual=None if pd.isna(row["worst_fold_spearman_ic"]) else float(row["worst_fold_spearman_ic"]),
            threshold=float(thresholds.min_worst_fold_spearman_ic),
            comparator=">=",
        ),
    ]
    failed_gate_names = [gate.gate_name for gate in gate_results if not gate.passed]
    rejection_reasons = [gate.reason_code for gate in gate_results if not gate.passed]
    passed_gate_names = [gate.gate_name for gate in gate_results if gate.passed]
    return PromotionGateEvaluation(
        candidate_id=_candidate_id(row),
        passed=not failed_gate_names,
        gate_results=gate_results,
        rejection_reasons=rejection_reasons,
        passed_gate_names=passed_gate_names,
        failed_gate_names=failed_gate_names,
        metadata={
            "signal_family": row.get("signal_family"),
            "lookback": row.get("lookback"),
            "horizon": row.get("horizon"),
        },
    )


def evaluate_promotion_gates(
    leaderboard_df: pd.DataFrame,
    *,
    thresholds: PromotionThresholds = DEFAULT_PROMOTION_THRESHOLDS,
) -> list[PromotionGateEvaluation]:
    if leaderboard_df.empty:
        return []
    return [evaluate_promotion_gate(row, thresholds=thresholds) for _, row in leaderboard_df.iterrows()]


def build_promotion_decision(
    evaluation: PromotionGateEvaluation,
    *,
    final_status: str,
) -> PromotionDecision:
    return PromotionDecision.from_gate_evaluation(evaluation, final_status=final_status)


def apply_promotion_rules(
    leaderboard_df: pd.DataFrame,
    *,
    thresholds: PromotionThresholds = DEFAULT_PROMOTION_THRESHOLDS,
) -> pd.DataFrame:
    if leaderboard_df.empty:
        result = leaderboard_df.copy()
        result["rejection_reason"] = pd.Series(dtype="object")
        result["promotion_status"] = pd.Series(dtype="object")
        result["promotion_gate_results"] = pd.Series(dtype="object")
        result["promotion_gate_summary"] = pd.Series(dtype="object")
        result["failed_promotion_gates"] = pd.Series(dtype="object")
        result["passed_promotion_gates"] = pd.Series(dtype="object")
        result["promotion_decision"] = pd.Series(dtype="object")
        return result

    result = leaderboard_df.copy()
    gate_evaluations = evaluate_promotion_gates(result, thresholds=thresholds)
    result["promotion_gate_results"] = [
        [gate.to_dict() for gate in evaluation.gate_results]
        for evaluation in gate_evaluations
    ]
    result["promotion_gate_summary"] = [evaluation.to_dict() for evaluation in gate_evaluations]
    result["failed_promotion_gates"] = [list(evaluation.failed_gate_names) for evaluation in gate_evaluations]
    result["passed_promotion_gates"] = [list(evaluation.passed_gate_names) for evaluation in gate_evaluations]
    result["rejection_reason"] = [";".join(evaluation.rejection_reasons) for evaluation in gate_evaluations]
    result["promotion_status"] = result["rejection_reason"].map(
        lambda value: "promote" if not value else "reject"
    )
    result["promotion_decision"] = [
        build_promotion_decision(evaluation, final_status=status).to_dict()
        for evaluation, status in zip(gate_evaluations, result["promotion_status"].tolist(), strict=True)
    ]
    result.loc[result["promotion_status"] == "promote", "rejection_reason"] = "none"
    return result
