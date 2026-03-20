from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from trading_platform.research.alpha_lab.metrics import evaluate_cross_sectional_signal


@dataclass(frozen=True)
class CompositeConfig:
    redundancy_corr_threshold: float = 0.8
    weighting_schemes: tuple[str, ...] = ("equal", "quality")
    quality_metric: str = "mean_spearman_ic"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


DEFAULT_COMPOSITE_CONFIG = CompositeConfig()


def candidate_id(signal_family: str, lookback: int, horizon: int) -> str:
    return f"{signal_family}|{lookback}|{horizon}"


def normalize_signal_by_date(
    panel: pd.DataFrame,
    *,
    date_col: str = "timestamp",
    score_col: str = "signal",
    output_col: str = "normalized_signal",
) -> pd.DataFrame:
    required_cols = [date_col, score_col]
    normalized = panel.copy()
    usable = normalized[required_cols].dropna()
    if usable.empty:
        normalized[output_col] = pd.Series(dtype="float64")
        return normalized

    normalized[output_col] = normalized.groupby(date_col)[score_col].transform(
        lambda scores: ((scores.rank(method="average", pct=True) - 0.5) * 2.0)
        if scores.notna().sum() >= 2
        else pd.Series(index=scores.index, dtype="float64")
    )
    return normalized


def select_low_redundancy_signals(
    promoted_signals_df: pd.DataFrame,
    redundancy_df: pd.DataFrame,
    *,
    horizon: int,
    redundancy_corr_threshold: float,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    promoted = promoted_signals_df.loc[promoted_signals_df["horizon"] == horizon].copy()
    if promoted.empty:
        return promoted, []

    promoted = promoted.sort_values(
        ["mean_spearman_ic", "mean_long_short_spread"],
        ascending=[False, False],
    ).reset_index(drop=True)
    if "candidate_id" not in promoted.columns:
        promoted["candidate_id"] = promoted.apply(
            lambda row: candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
            ),
            axis=1,
        )

    pair_corr: dict[frozenset[str], float] = {}
    for _, row in redundancy_df.iterrows():
        left_id = row.get("candidate_id_a")
        if pd.isna(left_id):
            left_id = candidate_id(
                str(row["signal_family_a"]),
                int(row["lookback_a"]),
                int(row["horizon_a"]),
            )
        right_id = row.get("candidate_id_b")
        if pd.isna(right_id):
            right_id = candidate_id(
                str(row["signal_family_b"]),
                int(row["lookback_b"]),
                int(row["horizon_b"]),
            )
        corr_value = row["score_corr"]
        if pd.isna(corr_value):
            corr_value = row["performance_corr"]
        if pd.isna(corr_value):
            corr_value = row["rank_ic_corr"]
        pair_corr[frozenset((left_id, right_id))] = float(corr_value) if pd.notna(corr_value) else float("nan")

    selected_rows: list[pd.Series] = []
    excluded_rows: list[dict[str, object]] = []
    selected_ids: list[str] = []
    for _, row in promoted.iterrows():
        current_id = str(row["candidate_id"])
        redundant_with: str | None = None
        redundant_corr = float("nan")
        for selected_id in selected_ids:
            corr_value = pair_corr.get(frozenset((current_id, selected_id)), float("nan"))
            if pd.notna(corr_value) and abs(corr_value) >= redundancy_corr_threshold:
                redundant_with = selected_id
                redundant_corr = corr_value
                break

        if redundant_with is None:
            selected_rows.append(row)
            selected_ids.append(current_id)
            continue

        excluded_rows.append(
            {
                "candidate_id": current_id,
                "excluded_with_candidate_id": redundant_with,
                "redundancy_corr": redundant_corr,
            }
        )

    if not selected_rows:
        return promoted.iloc[0:0].copy(), excluded_rows

    selected = pd.DataFrame(selected_rows).reset_index(drop=True)
    return selected, excluded_rows


def build_component_weights(
    selected_signals_df: pd.DataFrame,
    *,
    weighting_scheme: str,
    quality_metric: str,
) -> pd.DataFrame:
    if selected_signals_df.empty:
        result = selected_signals_df.copy()
        result["component_weight"] = pd.Series(dtype="float64")
        return result

    result = selected_signals_df.copy()
    if weighting_scheme == "quality" and quality_metric in result.columns:
        quality = result[quality_metric].clip(lower=0.0)
        if quality.sum() > 0:
            result["component_weight"] = quality / quality.sum()
            return result

    result["component_weight"] = 1.0 / len(result)
    return result


def build_composite_scores(
    selected_signals_df: pd.DataFrame,
    *,
    score_panel_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
    weighting_scheme: str,
    quality_metric: str,
    dynamic_signal_weights_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if selected_signals_df.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "horizon",
                "weighting_scheme",
                "composite_score",
                "component_count",
                "selected_signal_count",
            ]
        )

    weighted_components = build_component_weights(
        selected_signals_df,
        weighting_scheme=weighting_scheme,
        quality_metric=quality_metric,
    )

    component_frames: list[pd.DataFrame] = []
    for _, row in weighted_components.iterrows():
        candidate_key = (
            str(row["signal_family"]),
            int(row["lookback"]),
            int(row["horizon"]),
        )
        signal_candidate_id = row.get("candidate_id", candidate_id(*candidate_key))
        score_panel = score_panel_by_candidate.get(candidate_key)
        if score_panel is None or score_panel.empty:
            continue

        normalized = normalize_signal_by_date(score_panel)
        normalized = normalized[["timestamp", "symbol", "normalized_signal"]].dropna().copy()
        if normalized.empty:
            continue

        if dynamic_signal_weights_df is not None:
            signal_weights = dynamic_signal_weights_df.loc[
                dynamic_signal_weights_df["candidate_id"] == signal_candidate_id,
                ["timestamp", "signal_weight"],
            ].rename(columns={"signal_weight": "component_weight"})
            normalized = normalized.merge(signal_weights, on="timestamp", how="inner")
            normalized = normalized.loc[normalized["component_weight"] > 0.0].copy()
            if normalized.empty:
                continue
        else:
            normalized["component_weight"] = float(row["component_weight"])
        normalized["weighted_score"] = (
            normalized["normalized_signal"] * normalized["component_weight"]
        )
        normalized["candidate_id"] = signal_candidate_id
        component_frames.append(normalized)

    if not component_frames:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "horizon",
                "weighting_scheme",
                "composite_score",
                "component_count",
                "selected_signal_count",
            ]
        )

    components_df = pd.concat(component_frames, ignore_index=True)
    grouped = (
        components_df.groupby(["timestamp", "symbol"], as_index=False)
        .agg(
            weighted_score_sum=("weighted_score", "sum"),
            weight_sum=("component_weight", "sum"),
            component_count=("candidate_id", "nunique"),
        )
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
    )
    grouped["composite_score"] = grouped["weighted_score_sum"] / grouped["weight_sum"]
    grouped["horizon"] = int(weighted_components["horizon"].iloc[0])
    grouped["weighting_scheme"] = weighting_scheme
    grouped["selected_signal_count"] = int(len(weighted_components))
    return grouped[
        [
            "timestamp",
            "symbol",
            "horizon",
            "weighting_scheme",
            "composite_score",
            "component_count",
            "selected_signal_count",
        ]
    ]


def evaluate_composite_scores(
    composite_scores_df: pd.DataFrame,
    *,
    label_panel_by_horizon: dict[int, pd.DataFrame],
    folds: list,
    top_quantile: float,
    bottom_quantile: float,
) -> pd.DataFrame:
    columns = [
        "horizon",
        "weighting_scheme",
        "selected_signal_count",
        "mean_component_count",
        "folds_tested",
        "mean_dates_evaluated",
        "mean_pearson_ic",
        "mean_spearman_ic",
        "mean_hit_rate",
        "mean_long_short_spread",
        "mean_quantile_spread",
        "mean_turnover",
        "worst_fold_spearman_ic",
        "total_obs",
    ]
    if composite_scores_df.empty:
        return pd.DataFrame(columns=columns)

    fold_rows: list[dict[str, float | int | str]] = []
    for (horizon, weighting_scheme), composite_group in composite_scores_df.groupby(
        ["horizon", "weighting_scheme"]
    ):
        labels = label_panel_by_horizon.get(int(horizon), pd.DataFrame())
        if labels.empty:
            continue

        for fold in folds:
            fold_scores = composite_group.loc[
                (composite_group["timestamp"] >= fold.test_start)
                & (composite_group["timestamp"] <= fold.test_end)
            ].copy()
            if fold_scores.empty:
                continue

            fold_panel = fold_scores.merge(labels, on=["timestamp", "symbol"], how="inner")
            if fold_panel.empty:
                continue

            metrics = evaluate_cross_sectional_signal(
                fold_panel.rename(columns={"composite_score": "signal"}),
                signal_col="signal",
                forward_return_col="forward_return",
                top_quantile=top_quantile,
                bottom_quantile=bottom_quantile,
            )
            fold_rows.append(
                {
                    "horizon": int(horizon),
                    "weighting_scheme": str(weighting_scheme),
                    "selected_signal_count": int(fold_scores["selected_signal_count"].max()),
                    "mean_component_count": float(fold_scores["component_count"].mean()),
                    "fold_id": int(fold.fold_id),
                    **metrics,
                }
            )

    if not fold_rows:
        return pd.DataFrame(columns=columns)

    fold_df = pd.DataFrame(fold_rows)
    return (
        fold_df.groupby(["horizon", "weighting_scheme"], as_index=False)
        .agg(
            selected_signal_count=("selected_signal_count", "max"),
            mean_component_count=("mean_component_count", "mean"),
            folds_tested=("fold_id", "nunique"),
            mean_dates_evaluated=("dates_evaluated", "mean"),
            mean_pearson_ic=("pearson_ic", "mean"),
            mean_spearman_ic=("spearman_ic", "mean"),
            mean_hit_rate=("hit_rate", "mean"),
            mean_long_short_spread=("long_short_spread", "mean"),
            mean_quantile_spread=("quantile_spread", "mean"),
            mean_turnover=("turnover", "mean"),
            worst_fold_spearman_ic=("spearman_ic", "min"),
            total_obs=("n_obs", "sum"),
        )
        .sort_values(["mean_spearman_ic", "mean_long_short_spread"], ascending=[False, False])
        .reset_index(drop=True)
    )
