from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class EnsembleConfig:
    enabled: bool = False
    mode: str = "disabled"
    weight_method: str = "equal"
    normalize_scores: str = "rank_pct"
    max_members: int = 5
    min_member_score: float = 0.0
    require_promoted_only: bool = True
    max_members_per_family: int | None = None
    minimum_member_observations: int = 0
    minimum_member_metric: float | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"disabled", "candidate_weighted", "family_weighted"}:
            raise ValueError(f"Unsupported ensemble mode: {self.mode}")
        if self.weight_method not in {"equal", "performance_weighted", "rank_weighted"}:
            raise ValueError(f"Unsupported ensemble weight_method: {self.weight_method}")
        if self.normalize_scores not in {"raw", "zscore", "rank_pct"}:
            raise ValueError(f"Unsupported ensemble normalize_scores: {self.normalize_scores}")
        if self.max_members <= 0:
            raise ValueError("ensemble max_members must be > 0")
        if self.max_members_per_family is not None and self.max_members_per_family <= 0:
            raise ValueError("ensemble max_members_per_family must be > 0 when provided")
        if self.minimum_member_observations < 0:
            raise ValueError("ensemble minimum_member_observations must be >= 0")


def _member_id(row: pd.Series) -> str:
    if row.get("candidate_id"):
        return str(row["candidate_id"])
    return f"{row.get('signal_family')}|{row.get('lookback')}|{row.get('horizon')}"


def normalize_signal_scores(df: pd.DataFrame, method: str) -> pd.DataFrame:
    if df.empty:
        return df.assign(normalized_score=pd.Series(dtype="float64"))
    normalized = df.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce")
    normalized["signal"] = pd.to_numeric(normalized["signal"], errors="coerce")
    if method == "raw":
        normalized["normalized_score"] = normalized["signal"]
        return normalized
    if method == "zscore":
        grouped = normalized.groupby("timestamp")["signal"]
        means = grouped.transform("mean")
        stds = grouped.transform("std").replace(0.0, pd.NA)
        normalized["normalized_score"] = ((normalized["signal"] - means) / stds).fillna(0.0)
        return normalized
    if method == "rank_pct":
        normalized["normalized_score"] = normalized.groupby("timestamp")["signal"].rank(
            method="average",
            pct=True,
        ).fillna(0.5)
        return normalized
    raise ValueError(f"Unsupported normalization method: {method}")


def select_ensemble_members(
    members_df: pd.DataFrame,
    config: EnsembleConfig,
) -> pd.DataFrame:
    columns = [
        "member_id",
        "member_type",
        "family",
        "selection_rank",
        "raw_metric",
        "normalized_weight",
        "included_in_ensemble",
        "exclusion_reason",
        "signal_family",
        "lookback",
        "horizon",
        "promotion_status",
        "total_obs",
    ]
    if members_df.empty:
        return pd.DataFrame(columns=columns)

    minimum_metric = max(
        float(config.min_member_score),
        float(config.minimum_member_metric if config.minimum_member_metric is not None else config.min_member_score),
    )
    working = members_df.copy()
    working["member_id"] = working.apply(_member_id, axis=1)
    working["member_type"] = "candidate"
    working["family"] = working["signal_family"].astype(str)
    working["raw_metric"] = pd.to_numeric(working.get("mean_spearman_ic"), errors="coerce").fillna(0.0)
    working["total_obs"] = pd.to_numeric(working.get("total_obs"), errors="coerce").fillna(0.0)
    working = working.sort_values(
        ["raw_metric", "family", "member_id"],
        ascending=[False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    included_rows: list[int] = []
    family_counts: dict[str, int] = {}
    reasons: list[str] = []
    ranks: list[int | None] = []
    rank = 0
    for idx, row in working.iterrows():
        reason = ""
        if config.require_promoted_only and str(row.get("promotion_status") or "") != "promote":
            reason = "not_promoted"
        elif float(row["total_obs"]) < float(config.minimum_member_observations):
            reason = "insufficient_observations"
        elif float(row["raw_metric"]) < minimum_metric:
            reason = "below_minimum_metric"
        elif config.max_members_per_family is not None and family_counts.get(str(row["family"]), 0) >= config.max_members_per_family:
            reason = "family_cap"
        elif len(included_rows) >= int(config.max_members):
            reason = "max_members"
        else:
            included_rows.append(idx)
            family_counts[str(row["family"])] = family_counts.get(str(row["family"]), 0) + 1
            rank += 1
            ranks.append(rank)
            reasons.append("")
            continue
        ranks.append(None)
        reasons.append(reason)

    working["selection_rank"] = ranks
    working["included_in_ensemble"] = [idx in included_rows for idx in working.index]
    working["exclusion_reason"] = reasons
    working["normalized_weight"] = 0.0
    return working[columns]


def compute_ensemble_weights(df: pd.DataFrame, config: EnsembleConfig) -> pd.DataFrame:
    if df.empty:
        return df.assign(normalized_weight=pd.Series(dtype="float64"))
    working = df.copy()
    if config.weight_method == "equal":
        raw_weights = pd.Series(1.0, index=working.index)
    elif config.weight_method == "performance_weighted":
        raw_weights = pd.to_numeric(working["raw_metric"], errors="coerce").clip(lower=0.0)
        if float(raw_weights.sum()) <= 0.0:
            raw_weights = pd.Series(1.0, index=working.index)
    elif config.weight_method == "rank_weighted":
        ranks = pd.to_numeric(working["selection_rank"], errors="coerce")
        max_rank = int(ranks.max()) if not ranks.empty else 0
        raw_weights = (max_rank - ranks + 1).clip(lower=1.0)
    else:
        raise ValueError(f"Unsupported ensemble weight_method: {config.weight_method}")
    total = float(raw_weights.sum())
    if total <= 0.0:
        working["normalized_weight"] = 0.0
    else:
        working["normalized_weight"] = raw_weights / total
    return working


def assign_member_weights(member_summary: pd.DataFrame, config: EnsembleConfig) -> pd.DataFrame:
    included = member_summary.loc[member_summary["included_in_ensemble"]].copy()
    if included.empty:
        return member_summary.copy()
    if config.mode == "candidate_weighted":
        weighted = compute_ensemble_weights(included, config)
    elif config.mode == "family_weighted":
        family_metrics = (
            included.groupby("family", as_index=False)
            .agg(raw_metric=("raw_metric", "mean"), selection_rank=("selection_rank", "min"))
        )
        family_weights = compute_ensemble_weights(family_metrics.assign(member_id=family_metrics["family"]), config)
        family_weight_lookup = {
            str(row["family"]): float(row["normalized_weight"])
            for _, row in family_weights.iterrows()
        }
        weighted_frames: list[pd.DataFrame] = []
        for family, family_df in included.groupby("family", sort=True):
            family_weighted = compute_ensemble_weights(family_df, config)
            family_weighted["normalized_weight"] = (
                family_weighted["normalized_weight"] * family_weight_lookup.get(str(family), 0.0)
            )
            weighted_frames.append(family_weighted)
        weighted = pd.concat(weighted_frames, ignore_index=True) if weighted_frames else included.assign(normalized_weight=0.0)
    else:
        weighted = included.assign(normalized_weight=0.0)

    merged = member_summary.copy()
    weight_lookup = {
        str(row["member_id"]): float(row["normalized_weight"])
        for _, row in weighted.iterrows()
    }
    merged["normalized_weight"] = merged["member_id"].map(weight_lookup).fillna(0.0)
    return merged


def build_ensemble_scores(
    signal_frames: dict[str, pd.DataFrame],
    config: EnsembleConfig,
    member_summary: pd.DataFrame,
) -> pd.DataFrame:
    included = member_summary.loc[member_summary["included_in_ensemble"]].copy()
    if not config.enabled or config.mode == "disabled" or included.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "ensemble_score",
                "member_count",
                "contributing_families",
                "contributing_candidates",
                "top_contributing_member",
                "top_contributing_family",
            ]
        )

    weighted_members = assign_member_weights(included, config).loc[lambda df: df["included_in_ensemble"]].copy()
    contributions: list[pd.DataFrame] = []
    if config.mode == "candidate_weighted":
        for _, member in weighted_members.iterrows():
            panel = signal_frames.get(str(member["member_id"]), pd.DataFrame())
            if panel.empty:
                continue
            normalized_panel = normalize_signal_scores(panel, config.normalize_scores)
            normalized_panel["member_id"] = str(member["member_id"])
            normalized_panel["family"] = str(member["family"])
            normalized_panel["normalized_weight"] = float(member["normalized_weight"])
            normalized_panel["weighted_contribution"] = (
                pd.to_numeric(normalized_panel["normalized_score"], errors="coerce").fillna(0.0)
                * float(member["normalized_weight"])
            )
            contributions.append(normalized_panel)
    elif config.mode == "family_weighted":
        for family, family_df in weighted_members.groupby("family", sort=True):
            for _, member in family_df.iterrows():
                panel = signal_frames.get(str(member["member_id"]), pd.DataFrame())
                if panel.empty:
                    continue
                normalized_panel = normalize_signal_scores(panel, config.normalize_scores)
                normalized_panel["member_id"] = str(member["member_id"])
                normalized_panel["family"] = str(member["family"])
                normalized_panel["normalized_weight"] = float(member["normalized_weight"])
                normalized_panel["weighted_contribution"] = (
                    pd.to_numeric(normalized_panel["normalized_score"], errors="coerce").fillna(0.0)
                    * float(normalized_panel["normalized_weight"].iloc[0])
                )
                contributions.append(normalized_panel)
    else:
        raise ValueError(f"Unsupported ensemble mode: {config.mode}")

    if not contributions:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "ensemble_score",
                "member_count",
                "contributing_families",
                "contributing_candidates",
                "top_contributing_member",
                "top_contributing_family",
            ]
        )

    contribution_df = pd.concat(contributions, ignore_index=True)
    contribution_df["timestamp"] = pd.to_datetime(contribution_df["timestamp"], errors="coerce")
    contribution_df = contribution_df.dropna(subset=["timestamp", "symbol"]).sort_values(
        ["timestamp", "symbol", "family", "member_id"],
        kind="stable",
    )
    grouped_rows: list[dict[str, Any]] = []
    for (timestamp, symbol), group in contribution_df.groupby(["timestamp", "symbol"], sort=True):
        ordered = group.sort_values(["weighted_contribution", "member_id"], ascending=[False, True], kind="stable")
        grouped_rows.append(
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "ensemble_score": float(group["weighted_contribution"].sum()),
                "member_count": int(group["member_id"].nunique()),
                "contributing_families": ";".join(sorted(group["family"].astype(str).unique())),
                "contributing_candidates": ";".join(sorted(group["member_id"].astype(str).unique())),
                "top_contributing_member": str(ordered.iloc[0]["member_id"]),
                "top_contributing_family": str(ordered.iloc[0]["family"]),
            }
        )
    return pd.DataFrame(grouped_rows).sort_values(["timestamp", "symbol"], kind="stable").reset_index(drop=True)
