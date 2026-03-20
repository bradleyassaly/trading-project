from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SignalLifecycleConfig:
    weighting_schemes: tuple[str, ...] = ("equal", "recent_quality", "stability_decay")
    recent_quality_window: int = 20
    min_history: int = 5
    downweight_mean_rank_ic: float = 0.01
    deactivate_mean_rank_ic: float = -0.02
    deactivate_worst_rank_ic: float = -0.10
    deteriorating_trend_threshold: float = -0.03
    downweight_multiplier: float = 0.5
    stability_penalty_scale: float = 4.0
    decay_penalty_scale: float = 6.0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


DEFAULT_SIGNAL_LIFECYCLE_CONFIG = SignalLifecycleConfig()


def _candidate_id_from_row(row: pd.Series) -> str:
    return f"{row['signal_family']}|{int(row['lookback'])}|{int(row['horizon'])}"


def _compute_recent_quality_frame(
    daily_metrics_df: pd.DataFrame,
    *,
    window: int,
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "recent_obs",
        "recent_mean_rank_ic",
        "recent_rank_ic_std",
        "recent_worst_rank_ic",
        "recent_rank_ic_trend",
    ]
    if daily_metrics_df.empty:
        return pd.DataFrame(columns=columns)

    quality = daily_metrics_df[["timestamp", "spearman_ic"]].dropna().copy()
    if quality.empty:
        return pd.DataFrame(columns=columns)

    quality = (
        quality.groupby("timestamp", as_index=False)["spearman_ic"]
        .mean()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    past_ic = quality["spearman_ic"].shift(1)
    quality["recent_obs"] = past_ic.rolling(window, min_periods=1).count()
    quality["recent_mean_rank_ic"] = past_ic.rolling(window, min_periods=1).mean()
    quality["recent_rank_ic_std"] = past_ic.rolling(window, min_periods=2).std().fillna(0.0)
    quality["recent_worst_rank_ic"] = past_ic.rolling(window, min_periods=1).min()
    quality["recent_rank_ic_trend"] = past_ic.rolling(window, min_periods=2).apply(
        lambda values: float(values.iloc[-1] - values.iloc[0]),
        raw=False,
    )
    quality["recent_rank_ic_trend"] = quality["recent_rank_ic_trend"].fillna(0.0)
    return quality[columns]


def _classify_signal_state(
    row: pd.Series,
    *,
    config: SignalLifecycleConfig,
) -> tuple[str, str]:
    recent_obs = float(row.get("recent_obs", 0.0) or 0.0)
    recent_mean = row.get("recent_mean_rank_ic", float("nan"))
    recent_worst = row.get("recent_worst_rank_ic", float("nan"))
    recent_trend = row.get("recent_rank_ic_trend", float("nan"))

    if recent_obs < config.min_history:
        return "promote", "insufficient_recent_history"

    if (
        pd.notna(recent_mean)
        and recent_mean <= config.deactivate_mean_rank_ic
    ) or (
        pd.notna(recent_worst)
        and recent_worst <= config.deactivate_worst_rank_ic
    ):
        return "deactivate", "failed_recent_rank_ic"

    if (
        pd.notna(recent_mean)
        and recent_mean <= config.downweight_mean_rank_ic
    ) or (
        pd.notna(recent_trend)
        and recent_trend <= config.deteriorating_trend_threshold
    ):
        return "downweight", "deteriorating_recent_rank_ic"

    return "active", "none"


def _raw_weight_for_scheme(
    row: pd.Series,
    *,
    weighting_scheme: str,
    config: SignalLifecycleConfig,
) -> float:
    lifecycle_status = str(row["lifecycle_status"])
    if lifecycle_status == "deactivate":
        return 0.0

    lifecycle_multiplier = 1.0
    if lifecycle_status == "downweight":
        lifecycle_multiplier = config.downweight_multiplier

    recent_obs = float(row.get("recent_obs", 0.0) or 0.0)
    recent_mean = float(row["recent_mean_rank_ic"]) if pd.notna(row.get("recent_mean_rank_ic")) else 0.0
    recent_std = float(row["recent_rank_ic_std"]) if pd.notna(row.get("recent_rank_ic_std")) else 0.0
    recent_trend = float(row["recent_rank_ic_trend"]) if pd.notna(row.get("recent_rank_ic_trend")) else 0.0

    if weighting_scheme == "equal":
        return lifecycle_multiplier

    base_quality = max(recent_mean, 0.0)
    if recent_obs < config.min_history and base_quality <= 0.0:
        base_quality = 1.0
    elif base_quality <= 0.0:
        base_quality = 0.0

    if weighting_scheme == "recent_quality":
        return base_quality * lifecycle_multiplier

    if weighting_scheme == "stability_decay":
        stability_penalty = 1.0 / (1.0 + (recent_std * config.stability_penalty_scale))
        trend_penalty = 1.0
        if recent_trend < 0.0:
            trend_penalty = 1.0 / (1.0 + (abs(recent_trend) * config.decay_penalty_scale))
        return base_quality * stability_penalty * trend_penalty * lifecycle_multiplier

    raise ValueError(f"Unsupported dynamic weighting scheme: {weighting_scheme}")


def _normalize_group_weights(group: pd.DataFrame) -> pd.DataFrame:
    result = group.copy()
    eligible = result["lifecycle_status"] != "deactivate"
    positive = eligible & (result["raw_weight"] > 0.0)
    if positive.any():
        result.loc[positive, "signal_weight"] = (
            result.loc[positive, "raw_weight"] / result.loc[positive, "raw_weight"].sum()
        )
        result.loc[~positive, "signal_weight"] = 0.0
        return result

    if eligible.any():
        fallback = result.loc[eligible, "fallback_weight"].astype(float)
        result.loc[eligible, "signal_weight"] = fallback / fallback.sum()
        result.loc[~eligible, "signal_weight"] = 0.0
        return result

    result["signal_weight"] = 0.0
    return result


def build_dynamic_signal_weights(
    selected_signals_df: pd.DataFrame,
    *,
    daily_metrics_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
    horizon: int,
    config: SignalLifecycleConfig = DEFAULT_SIGNAL_LIFECYCLE_CONFIG,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    weight_columns = [
        "timestamp",
        "candidate_id",
        "signal_family",
        "lookback",
        "horizon",
        "weighting_scheme",
        "lifecycle_status",
        "lifecycle_reason",
        "recent_obs",
        "recent_mean_rank_ic",
        "recent_rank_ic_std",
        "recent_worst_rank_ic",
        "recent_rank_ic_trend",
        "raw_weight",
        "signal_weight",
    ]
    report_columns = [
        "report_type",
        "horizon",
        "weighting_scheme",
        "candidate_id",
        "signal_family",
        "lookback",
        "entry_date",
        "exit_date",
        "deactivation_date",
        "active_dates",
        "mean_weight",
        "max_weight",
        "weight_drift",
        "mean_top_weight",
        "max_top_weight",
        "mean_effective_component_count",
    ]
    selected = selected_signals_df.loc[selected_signals_df["horizon"] == horizon].copy()
    if selected.empty:
        empty_weights = pd.DataFrame(columns=weight_columns)
        empty_report = pd.DataFrame(columns=report_columns)
        return empty_weights, empty_weights.copy(), empty_weights.copy(), empty_report

    signal_frames: list[pd.DataFrame] = []
    for _, row in selected.iterrows():
        candidate_key = (
            str(row["signal_family"]),
            int(row["lookback"]),
            int(row["horizon"]),
        )
        daily_metrics_df = daily_metrics_by_candidate.get(candidate_key, pd.DataFrame())
        recent_quality = _compute_recent_quality_frame(
            daily_metrics_df,
            window=config.recent_quality_window,
        )
        if recent_quality.empty:
            continue

        recent_quality["candidate_id"] = _candidate_id_from_row(row)
        recent_quality["signal_family"] = str(row["signal_family"])
        recent_quality["lookback"] = int(row["lookback"])
        recent_quality["horizon"] = int(row["horizon"])
        signal_frames.append(recent_quality)

    if not signal_frames:
        empty_weights = pd.DataFrame(columns=weight_columns)
        empty_report = pd.DataFrame(columns=report_columns)
        return empty_weights, empty_weights.copy(), empty_weights.copy(), empty_report

    quality_frame = pd.concat(signal_frames, ignore_index=True)
    lifecycle = quality_frame.apply(
        lambda row: _classify_signal_state(row, config=config),
        axis=1,
        result_type="expand",
    )
    lifecycle.columns = ["lifecycle_status", "lifecycle_reason"]
    quality_frame = pd.concat([quality_frame, lifecycle], axis=1)
    quality_frame["fallback_weight"] = np.where(
        quality_frame["lifecycle_status"] == "downweight",
        config.downweight_multiplier,
        np.where(quality_frame["lifecycle_status"] == "deactivate", 0.0, 1.0),
    )

    weighted_frames: list[pd.DataFrame] = []
    for weighting_scheme in config.weighting_schemes:
        scheme_frame = quality_frame.copy()
        scheme_frame["weighting_scheme"] = str(weighting_scheme)
        scheme_frame["raw_weight"] = scheme_frame.apply(
            lambda row: _raw_weight_for_scheme(
                row,
                weighting_scheme=str(weighting_scheme),
                config=config,
            ),
            axis=1,
        )
        normalized_groups: list[pd.DataFrame] = []
        for _, group in scheme_frame.groupby(["timestamp", "horizon", "weighting_scheme"], sort=False):
            normalized_groups.append(_normalize_group_weights(group))
        normalized = pd.concat(normalized_groups, ignore_index=True) if normalized_groups else scheme_frame.iloc[0:0].copy()
        weighted_frames.append(normalized[weight_columns + ["fallback_weight"]])

    dynamic_weights = pd.concat(weighted_frames, ignore_index=True) if weighted_frames else pd.DataFrame(columns=weight_columns)
    if dynamic_weights.empty:
        empty_report = pd.DataFrame(columns=report_columns)
        return dynamic_weights, dynamic_weights.copy(), dynamic_weights.copy(), empty_report

    active_signals = dynamic_weights.loc[
        (dynamic_weights["lifecycle_status"] != "deactivate") & (dynamic_weights["signal_weight"] > 0.0)
    ].copy()
    deactivated_signals = dynamic_weights.loc[
        dynamic_weights["lifecycle_status"] == "deactivate"
    ].copy()

    signal_summary_rows: list[dict[str, object]] = []
    for keys, group in active_signals.groupby(["horizon", "weighting_scheme", "candidate_id"], sort=False):
        deactivated_group = deactivated_signals.loc[
            (deactivated_signals["horizon"] == keys[0])
            & (deactivated_signals["weighting_scheme"] == keys[1])
            & (deactivated_signals["candidate_id"] == keys[2])
        ]
        ordered = group.sort_values("timestamp").reset_index(drop=True)
        signal_summary_rows.append(
            {
                "report_type": "signal_summary",
                "horizon": int(keys[0]),
                "weighting_scheme": str(keys[1]),
                "candidate_id": str(keys[2]),
                "signal_family": str(ordered["signal_family"].iloc[0]),
                "lookback": int(ordered["lookback"].iloc[0]),
                "entry_date": ordered["timestamp"].min(),
                "exit_date": ordered["timestamp"].max(),
                "deactivation_date": deactivated_group["timestamp"].min() if not deactivated_group.empty else pd.NaT,
                "active_dates": int(ordered["timestamp"].nunique()),
                "mean_weight": float(ordered["signal_weight"].mean()),
                "max_weight": float(ordered["signal_weight"].max()),
                "weight_drift": float(ordered["signal_weight"].iloc[-1] - ordered["signal_weight"].iloc[0]),
                "mean_top_weight": float("nan"),
                "max_top_weight": float("nan"),
                "mean_effective_component_count": float("nan"),
            }
        )

    concentration = (
        active_signals.groupby(["timestamp", "horizon", "weighting_scheme"], as_index=False)
        .agg(
            top_weight=("signal_weight", "max"),
            effective_component_count=("signal_weight", lambda values: float(1.0 / np.square(values).sum()) if np.square(values).sum() > 0.0 else 0.0),
        )
        if not active_signals.empty
        else pd.DataFrame(columns=["timestamp", "horizon", "weighting_scheme", "top_weight", "effective_component_count"])
    )
    concentration_rows = [
        {
            "report_type": "weight_concentration_summary",
            "horizon": int(keys[0]),
            "weighting_scheme": str(keys[1]),
            "candidate_id": pd.NA,
            "signal_family": pd.NA,
            "lookback": pd.NA,
            "entry_date": pd.NaT,
            "exit_date": pd.NaT,
            "deactivation_date": pd.NaT,
            "active_dates": int(len(group)),
            "mean_weight": float("nan"),
            "max_weight": float("nan"),
            "weight_drift": float("nan"),
            "mean_top_weight": float(group["top_weight"].mean()),
            "max_top_weight": float(group["top_weight"].max()),
            "mean_effective_component_count": float(group["effective_component_count"].mean()),
        }
        for keys, group in concentration.groupby(["horizon", "weighting_scheme"], sort=False)
    ]

    lifecycle_report = pd.DataFrame(
        signal_summary_rows + concentration_rows,
        columns=report_columns,
    )
    return (
        dynamic_weights[weight_columns],
        active_signals[weight_columns],
        deactivated_signals[weight_columns],
        lifecycle_report,
    )
