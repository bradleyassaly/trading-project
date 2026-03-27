from __future__ import annotations

import json
from dataclasses import asdict, dataclass

import pandas as pd

from trading_platform.research.alpha_lab.metrics import evaluate_cross_sectional_signal
from trading_platform.research.alpha_lab.signals import build_signal


@dataclass(frozen=True)
class CompositeConfig:
    redundancy_corr_threshold: float = 0.8
    weighting_schemes: tuple[str, ...] = ("equal", "quality")
    quality_metric: str = "mean_spearman_ic"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


DEFAULT_COMPOSITE_CONFIG = CompositeConfig()


@dataclass(frozen=True)
class CompositeRuntimeComputabilityConfig:
    require_composite_runtime_computability_for_approval: bool = False
    min_composite_runtime_computable_symbols_for_approval: int = 5
    allow_research_only_noncomputable_composites: bool = True
    composite_runtime_computability_check_mode: str = "strict"
    composite_runtime_computability_penalty_on_ranking: float = 0.02

    def __post_init__(self) -> None:
        if self.min_composite_runtime_computable_symbols_for_approval < 0:
            raise ValueError("min_composite_runtime_computable_symbols_for_approval must be >= 0")
        if self.composite_runtime_computability_penalty_on_ranking < 0:
            raise ValueError("composite_runtime_computability_penalty_on_ranking must be >= 0")
        if self.composite_runtime_computability_check_mode not in {"strict", "penalize", "diagnostic_only"}:
            raise ValueError("composite_runtime_computability_check_mode must be one of: strict, penalize, diagnostic_only")

    def strict_mode_enabled(self) -> bool:
        return (
            self.require_composite_runtime_computability_for_approval
            and self.composite_runtime_computability_check_mode == "strict"
        )


def _parse_variant_parameters(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", "{}"):
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def candidate_id(
    signal_family: str,
    lookback: int,
    horizon: int,
    signal_variant: str | None = None,
) -> str:
    normalized_variant = str(signal_variant or "base").strip()
    if normalized_variant and normalized_variant != "base":
        return f"{signal_family}|{normalized_variant}|{lookback}|{horizon}"
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

    ranking_metric = "runtime_adjusted_mean_spearman_ic" if "runtime_adjusted_mean_spearman_ic" in promoted.columns else "mean_spearman_ic"
    promoted = promoted.sort_values(
        [ranking_metric, "mean_long_short_spread"],
        ascending=[False, False],
    ).reset_index(drop=True)
    if "candidate_id" not in promoted.columns:
        promoted["candidate_id"] = promoted.apply(
            lambda row: candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
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
                str(row.get("signal_variant_a") or "base"),
            )
        right_id = row.get("candidate_id_b")
        if pd.isna(right_id):
            right_id = candidate_id(
                str(row["signal_family_b"]),
                int(row["lookback_b"]),
                int(row["horizon_b"]),
                str(row.get("signal_variant_b") or "base"),
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


def build_runtime_composite_validation(
    selected_signals_df: pd.DataFrame,
    *,
    feature_data_by_symbol: dict[str, pd.DataFrame],
    weighting_scheme: str = "equal",
    quality_metric: str = DEFAULT_COMPOSITE_CONFIG.quality_metric,
    signal_composition_preset: str = "standard",
    enable_context_confirmations: bool | None = None,
    enable_relative_features: bool | None = None,
    enable_flow_confirmations: bool | None = None,
) -> dict[str, object]:
    latest_timestamp = max(
        (
            pd.to_datetime(frame["timestamp"], errors="coerce").max()
            for frame in feature_data_by_symbol.values()
            if not frame.empty and "timestamp" in frame.columns
        ),
        default=pd.NaT,
    )
    base_result: dict[str, object] = {
        "selected_member_count": int(len(selected_signals_df.index)),
        "loaded_member_score_symbol_count": 0,
        "latest_component_score_count": 0,
        "latest_composite_score_count": 0,
        "composite_runtime_computable_symbol_count": 0,
        "composite_runtime_computability_pass": False,
        "composite_runtime_computability_reason": "no_selected_signals",
        "selected_members": selected_signals_df.get("candidate_id", pd.Series(dtype="object")).astype(str).tolist()
        if not selected_signals_df.empty and "candidate_id" in selected_signals_df.columns
        else [],
        "latest_timestamp": str(pd.Timestamp(latest_timestamp).isoformat()) if pd.notna(latest_timestamp) else None,
    }
    if selected_signals_df.empty or pd.isna(latest_timestamp):
        return base_result

    score_panel_by_candidate: dict[str, pd.DataFrame] = {}
    latest_component_rows: list[pd.DataFrame] = []
    for _, row in selected_signals_df.iterrows():
        signal_candidate_id = str(
            row.get("candidate_id")
            or candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
            )
        )
        panel_frames: list[pd.DataFrame] = []
        for symbol, feature_df in feature_data_by_symbol.items():
            signal = build_signal(
                feature_df,
                signal_family=str(row["signal_family"]),
                lookback=int(row["lookback"]),
                signal_variant=str(row.get("signal_variant") or "base"),
                variant_params=_parse_variant_parameters(row.get("variant_parameters_json")),
                signal_composition_preset=signal_composition_preset,
                enable_context_confirmations=enable_context_confirmations,
                enable_relative_features=enable_relative_features,
                enable_flow_confirmations=enable_flow_confirmations,
            )
            signal_frame = feature_df[["timestamp", "symbol"]].copy()
            signal_frame["signal"] = signal
            signal_frame = signal_frame.dropna(subset=["signal"])
            if signal_frame.empty:
                continue
            panel_frames.append(signal_frame)
        if not panel_frames:
            continue
        score_panel = pd.concat(panel_frames, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
        score_panel_by_candidate[signal_candidate_id] = score_panel
        normalized = normalize_signal_by_date(score_panel)
        latest_slice = normalized.loc[normalized["timestamp"] == latest_timestamp].copy()
        if latest_slice.empty:
            continue
        latest_slice["candidate_id"] = signal_candidate_id
        latest_component_rows.append(latest_slice)

    if not latest_component_rows:
        return {
            **base_result,
            "composite_runtime_computability_reason": "empty_component_scores",
        }

    latest_components_df = pd.concat(latest_component_rows, ignore_index=True)
    latest_usable_components_df = latest_components_df.dropna(subset=["normalized_signal"]).copy()
    latest_component_symbols = {
        str(symbol)
        for symbol in latest_usable_components_df.get("symbol", pd.Series(dtype="object")).tolist()
        if str(symbol).strip()
    }
    latest_component_candidate_count = int(
        latest_usable_components_df.get("candidate_id", pd.Series(dtype="object")).astype(str).nunique()
    ) if not latest_usable_components_df.empty else 0
    if latest_usable_components_df.empty:
        return {
            **base_result,
            "loaded_member_score_symbol_count": 0,
            "latest_component_score_count": 0,
            "latest_composite_score_count": 0,
            "composite_runtime_computable_symbol_count": 0,
            "composite_runtime_computability_reason": "empty_component_scores",
        }
    composite_scores_df = build_composite_scores(
        selected_signals_df,
        score_panel_by_candidate=score_panel_by_candidate,
        weighting_scheme=weighting_scheme,
        quality_metric=quality_metric,
    )
    latest_composite_df = composite_scores_df.loc[composite_scores_df["timestamp"] == latest_timestamp].copy()
    latest_composite_symbols = {
        str(symbol)
        for symbol in latest_composite_df.get("symbol", pd.Series(dtype="object")).tolist()
        if str(symbol).strip()
    }
    if latest_composite_df.empty:
        failure_reason = (
            "empty_component_scores"
            if not latest_component_symbols or latest_component_candidate_count < int(len(selected_signals_df.index))
            else "empty_signal_scores"
        )
        return {
            **base_result,
            "loaded_member_score_symbol_count": int(len(latest_component_symbols)),
            "latest_component_score_count": int(len(latest_usable_components_df.index)),
            "latest_composite_score_count": 0,
            "composite_runtime_computable_symbol_count": 0,
            "composite_runtime_computability_reason": failure_reason,
        }

    return {
        **base_result,
        "loaded_member_score_symbol_count": int(len(latest_component_symbols)),
        "latest_component_score_count": int(len(latest_usable_components_df.index)),
        "latest_composite_score_count": int(len(latest_composite_df.index)),
        "composite_runtime_computable_symbol_count": int(len(latest_composite_symbols)),
        "composite_runtime_computability_pass": True,
        "composite_runtime_computability_reason": "runtime_scores_available",
    }


def build_composite_scores(
    selected_signals_df: pd.DataFrame,
    *,
    score_panel_by_candidate: dict[str, pd.DataFrame],
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
        signal_candidate_id = str(
            row.get("candidate_id")
            or candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
            )
        )
        score_panel = score_panel_by_candidate.get(signal_candidate_id)
        if score_panel is None:
            score_panel = score_panel_by_candidate.get(
                (
                    str(row["signal_family"]),
                    int(row["lookback"]),
                    int(row["horizon"]),
                )
            )
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
