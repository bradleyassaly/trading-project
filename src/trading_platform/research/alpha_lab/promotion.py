from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd


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


def apply_promotion_rules(
    leaderboard_df: pd.DataFrame,
    *,
    thresholds: PromotionThresholds = DEFAULT_PROMOTION_THRESHOLDS,
) -> pd.DataFrame:
    if leaderboard_df.empty:
        result = leaderboard_df.copy()
        result["rejection_reason"] = pd.Series(dtype="object")
        result["promotion_status"] = pd.Series(dtype="object")
        return result

    def rejection_reasons(row: pd.Series) -> str:
        reasons: list[str] = []
        if pd.isna(row["mean_spearman_ic"]) or row["mean_spearman_ic"] <= thresholds.min_mean_spearman_ic:
            reasons.append("low_mean_rank_ic")
        if row["symbols_tested"] < thresholds.min_symbols_tested:
            reasons.append("insufficient_symbols")
        if row["folds_tested"] < thresholds.min_folds_tested:
            reasons.append("insufficient_folds")
        if row["mean_dates_evaluated"] < thresholds.min_mean_dates_evaluated:
            reasons.append("insufficient_dates")
        if row["total_obs"] < thresholds.min_total_obs:
            reasons.append("insufficient_observations")
        if pd.isna(row["mean_turnover"]) or row["mean_turnover"] > thresholds.max_mean_turnover:
            reasons.append("high_turnover")
        if (
            pd.isna(row["worst_fold_spearman_ic"])
            or row["worst_fold_spearman_ic"] < thresholds.min_worst_fold_spearman_ic
        ):
            reasons.append("weak_worst_fold_rank_ic")
        return ";".join(reasons)

    result = leaderboard_df.copy()
    result["rejection_reason"] = result.apply(rejection_reasons, axis=1)
    result["promotion_status"] = result["rejection_reason"].map(
        lambda value: "promote" if not value else "reject"
    )
    result.loc[result["promotion_status"] == "promote", "rejection_reason"] = "none"
    return result
