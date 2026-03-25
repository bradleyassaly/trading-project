from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from trading_platform.research.alpha_lab.composite import candidate_id


@dataclass(frozen=True)
class RegimeConfig:
    enabled: bool = False
    volatility_window: int = 20
    trend_window: int = 60
    dispersion_window: int = 20
    min_history: int = 5
    underweight_mean_rank_ic: float = 0.01
    exclude_mean_rank_ic: float = -0.01
    underweight_multiplier: float = 0.5
    overweight_scale: float = 5.0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


DEFAULT_REGIME_CONFIG = RegimeConfig()


def build_regime_labels_by_date(
    asset_returns: pd.DataFrame,
    *,
    config: RegimeConfig = DEFAULT_REGIME_CONFIG,
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "market_return",
        "volatility_value",
        "volatility_threshold",
        "volatility_regime",
        "trend_value",
        "trend_regime",
        "dispersion_value",
        "dispersion_threshold",
        "dispersion_regime",
        "regime_key",
    ]
    if asset_returns.empty:
        return pd.DataFrame(columns=columns)

    ordered = asset_returns.sort_index()
    market_return = ordered.mean(axis=1).fillna(0.0)
    cross_sectional_dispersion = ordered.std(axis=1, ddof=0).fillna(0.0)

    volatility_value = market_return.rolling(config.volatility_window, min_periods=2).std().shift(1)
    volatility_threshold = volatility_value.expanding(min_periods=config.min_history).median()
    dispersion_value = cross_sectional_dispersion.rolling(config.dispersion_window, min_periods=2).mean().shift(1)
    dispersion_threshold = dispersion_value.expanding(min_periods=config.min_history).median()
    trend_value = market_return.rolling(config.trend_window, min_periods=2).mean().shift(1)

    labels = pd.DataFrame(
        {
            "timestamp": ordered.index,
            "market_return": market_return.to_numpy(),
            "volatility_value": volatility_value.to_numpy(),
            "volatility_threshold": volatility_threshold.to_numpy(),
            "trend_value": trend_value.to_numpy(),
            "dispersion_value": dispersion_value.to_numpy(),
            "dispersion_threshold": dispersion_threshold.to_numpy(),
        }
    )
    labels["volatility_regime"] = labels.apply(
        lambda row: (
            "high_vol"
            if pd.notna(row["volatility_value"]) and pd.notna(row["volatility_threshold"]) and row["volatility_value"] >= row["volatility_threshold"]
            else "low_vol"
        ),
        axis=1,
    )
    labels["trend_regime"] = labels["trend_value"].map(
        lambda value: "uptrend" if pd.notna(value) and value >= 0.0 else "downtrend"
    )
    labels["dispersion_regime"] = labels.apply(
        lambda row: (
            "high_dispersion"
            if pd.notna(row["dispersion_value"]) and pd.notna(row["dispersion_threshold"]) and row["dispersion_value"] >= row["dispersion_threshold"]
            else "low_dispersion"
        ),
        axis=1,
    )
    labels["regime_key"] = (
        labels["volatility_regime"].astype(str)
        + "|"
        + labels["trend_regime"].astype(str)
        + "|"
        + labels["dispersion_regime"].astype(str)
    )
    return labels[columns]


def compute_signal_performance_by_regime(
    selected_signals_df: pd.DataFrame,
    *,
    daily_metrics_by_candidate: dict[str, pd.DataFrame],
    regime_labels_df: pd.DataFrame,
    horizon: int,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "signal_family",
        "signal_variant",
        "lookback",
        "horizon",
        "regime_key",
        "volatility_regime",
        "trend_regime",
        "dispersion_regime",
        "dates_evaluated",
        "mean_spearman_ic",
        "mean_long_short_spread",
    ]
    selected = selected_signals_df.loc[selected_signals_df["horizon"] == horizon].copy()
    if selected.empty or regime_labels_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for _, row in selected.iterrows():
        signal_candidate_id = str(
            row.get("candidate_id")
            or candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
            )
        )
        daily_metrics_df = daily_metrics_by_candidate.get(signal_candidate_id)
        if daily_metrics_df is None:
            daily_metrics_df = daily_metrics_by_candidate.get(
                (
                    str(row["signal_family"]),
                    int(row["lookback"]),
                    int(row["horizon"]),
                )
            )
        if daily_metrics_df is None:
            daily_metrics_df = pd.DataFrame()
        if daily_metrics_df.empty:
            continue
        merged = daily_metrics_df.merge(
            regime_labels_df[
                ["timestamp", "regime_key", "volatility_regime", "trend_regime", "dispersion_regime"]
            ],
            on="timestamp",
            how="inner",
        )
        if merged.empty:
            continue
        summary = (
            merged.groupby(
                ["regime_key", "volatility_regime", "trend_regime", "dispersion_regime"],
                as_index=False,
            )
            .agg(
                dates_evaluated=("timestamp", "nunique"),
                mean_spearman_ic=("spearman_ic", "mean"),
                mean_long_short_spread=("long_short_spread", "mean"),
            )
        )
        summary["candidate_id"] = signal_candidate_id
        summary["signal_family"] = str(row["signal_family"])
        summary["signal_variant"] = str(row.get("signal_variant") or "base")
        summary["lookback"] = int(row["lookback"])
        summary["horizon"] = int(row["horizon"])
        rows.extend(summary[columns].to_dict(orient="records"))
    return pd.DataFrame(rows, columns=columns)


def build_regime_aware_signal_weights(
    base_dynamic_weights_df: pd.DataFrame,
    *,
    daily_metrics_by_candidate: dict[str, pd.DataFrame],
    regime_labels_df: pd.DataFrame,
    horizon: int,
    config: RegimeConfig = DEFAULT_REGIME_CONFIG,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    weight_columns = [
        "timestamp",
        "candidate_id",
        "signal_family",
        "signal_variant",
        "lookback",
        "horizon",
        "weighting_scheme",
        "lifecycle_status",
        "lifecycle_reason",
        "regime_key",
        "volatility_regime",
        "trend_regime",
        "dispersion_regime",
        "regime_obs",
        "regime_mean_rank_ic",
        "regime_multiplier",
        "signal_weight",
    ]
    report_columns = [
        "report_type",
        "horizon",
        "weighting_scheme",
        "regime_key",
        "candidate_id",
        "mean_weight",
        "max_weight",
        "mean_regime_multiplier",
        "regime_dates",
    ]
    if not config.enabled or base_dynamic_weights_df.empty or regime_labels_df.empty:
        return pd.DataFrame(columns=weight_columns), pd.DataFrame(columns=report_columns)

    base = base_dynamic_weights_df.loc[
        (base_dynamic_weights_df["horizon"] == horizon)
        & (base_dynamic_weights_df["weighting_scheme"] == "stability_decay")
    ].copy()
    if base.empty:
        return pd.DataFrame(columns=weight_columns), pd.DataFrame(columns=report_columns)
    if "signal_variant" not in base.columns:
        base["signal_variant"] = "base"

    history_frames: list[pd.DataFrame] = []
    for candidate_id, group in base.groupby("candidate_id", sort=False):
        metrics_df = daily_metrics_by_candidate.get(str(candidate_id))
        if metrics_df is None:
            metrics_df = daily_metrics_by_candidate.get(
                (
                    str(group["signal_family"].iloc[0]),
                    int(group["lookback"].iloc[0]),
                    int(horizon),
                )
            )
        if metrics_df is None:
            metrics_df = pd.DataFrame()
        if metrics_df.empty:
            continue
        merged = metrics_df.merge(
            regime_labels_df[
                ["timestamp", "regime_key", "volatility_regime", "trend_regime", "dispersion_regime"]
            ],
            on="timestamp",
            how="inner",
        ).sort_values("timestamp")
        if merged.empty:
            continue
        merged["candidate_id"] = candidate_id
        merged["regime_obs"] = merged.groupby("regime_key")["spearman_ic"].transform(
            lambda series: series.shift(1).expanding(min_periods=1).count()
        )
        merged["regime_mean_rank_ic"] = merged.groupby("regime_key")["spearman_ic"].transform(
            lambda series: series.shift(1).expanding(min_periods=1).mean()
        )
        history_frames.append(
            merged[
                [
                    "timestamp",
                    "candidate_id",
                    "regime_key",
                    "volatility_regime",
                    "trend_regime",
                    "dispersion_regime",
                    "regime_obs",
                    "regime_mean_rank_ic",
                ]
            ]
        )

    history_df = pd.concat(history_frames, ignore_index=True) if history_frames else pd.DataFrame()
    merged_weights = base.merge(
        regime_labels_df[
            ["timestamp", "regime_key", "volatility_regime", "trend_regime", "dispersion_regime"]
        ],
        on="timestamp",
        how="left",
    )
    if history_df.empty:
        merged_weights["regime_obs"] = 0.0
        merged_weights["regime_mean_rank_ic"] = float("nan")
    else:
        merged_weights = merged_weights.merge(
            history_df,
            on=[
                "timestamp",
                "candidate_id",
                "regime_key",
                "volatility_regime",
                "trend_regime",
                "dispersion_regime",
            ],
            how="left",
        )
    merged_weights["weighting_scheme"] = "regime_aware"

    def regime_multiplier(row: pd.Series) -> float:
        regime_obs = float(row.get("regime_obs", 0.0) or 0.0)
        mean_rank_ic = row.get("regime_mean_rank_ic", float("nan"))
        if row.get("lifecycle_status") == "deactivate":
            return 0.0
        if regime_obs < config.min_history or pd.isna(mean_rank_ic):
            return 1.0
        if mean_rank_ic <= config.exclude_mean_rank_ic:
            return 0.0
        if mean_rank_ic <= config.underweight_mean_rank_ic:
            return config.underweight_multiplier
        return 1.0 + (float(mean_rank_ic) * config.overweight_scale)

    merged_weights["regime_multiplier"] = merged_weights.apply(regime_multiplier, axis=1)
    merged_weights["raw_regime_weight"] = (
        pd.to_numeric(merged_weights["signal_weight"], errors="coerce").fillna(0.0)
        * pd.to_numeric(merged_weights["regime_multiplier"], errors="coerce").fillna(0.0)
    )

    normalized_frames: list[pd.DataFrame] = []
    for _, group in merged_weights.groupby(["timestamp", "horizon"], sort=False):
        normalized = group.copy()
        positive = normalized["raw_regime_weight"] > 0.0
        if positive.any():
            normalized.loc[positive, "signal_weight"] = (
                normalized.loc[positive, "raw_regime_weight"]
                / normalized.loc[positive, "raw_regime_weight"].sum()
            )
            normalized.loc[~positive, "signal_weight"] = 0.0
        else:
            base_sum = pd.to_numeric(normalized["signal_weight"], errors="coerce").fillna(0.0).sum()
            if base_sum > 0.0:
                normalized["signal_weight"] = (
                    pd.to_numeric(normalized["signal_weight"], errors="coerce").fillna(0.0) / base_sum
                )
            else:
                normalized["signal_weight"] = 0.0
        normalized_frames.append(normalized)

    regime_weights_df = (
        pd.concat(normalized_frames, ignore_index=True)[weight_columns]
        if normalized_frames
        else pd.DataFrame(columns=weight_columns)
    )
    if regime_weights_df.empty:
        return regime_weights_df, pd.DataFrame(columns=report_columns)

    report_rows: list[dict[str, object]] = []
    for keys, group in regime_weights_df.groupby(["horizon", "weighting_scheme", "candidate_id", "regime_key"], sort=False):
        report_rows.append(
            {
                "report_type": "signal_regime_weight_summary",
                "horizon": int(keys[0]),
                "weighting_scheme": str(keys[1]),
                "regime_key": str(keys[3]),
                "candidate_id": str(keys[2]),
                "mean_weight": float(group["signal_weight"].mean()),
                "max_weight": float(group["signal_weight"].max()),
                "mean_regime_multiplier": float(group["regime_multiplier"].mean()),
                "regime_dates": int(group["timestamp"].nunique()),
            }
        )

    for keys, group in regime_labels_df.groupby("regime_key", sort=False):
        report_rows.append(
            {
                "report_type": "regime_frequency_summary",
                "horizon": int(horizon),
                "weighting_scheme": "regime_aware",
                "regime_key": str(keys),
                "candidate_id": pd.NA,
                "mean_weight": float("nan"),
                "max_weight": float("nan"),
                "mean_regime_multiplier": float("nan"),
                "regime_dates": int(group["timestamp"].nunique()),
            }
        )

    return regime_weights_df, pd.DataFrame(report_rows, columns=report_columns)
