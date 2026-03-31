from __future__ import annotations

import json
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SignalGenerationConfig:
    signal_families: tuple[str, ...] = (
        "momentum",
        "mean_reversion",
        "volatility",
        "market_residual_momentum",
        "residual_momentum",
        "sector_relative_momentum",
        "cross_sectional_rank_momentum",
        "cross_sectional_zscore_momentum",
        "vol_adjusted_momentum",
        "vol_adjusted_reversal",
        "breakout_distance",
        "extreme_move",
        "volume_surprise",
        "interaction_momentum_volatility",
        "interaction_reversal_volume_spike",
        "feature_combo",
    )
    lookbacks: tuple[int, ...] = (5, 10, 20, 60)
    vol_windows: tuple[int, ...] = (10, 20, 60)
    combo_thresholds: tuple[float, ...] = (0.5, 1.0)
    combo_pairs: tuple[tuple[str, str], ...] = (
        ("mom_20", "vol_20"),
        ("dist_sma_200", "vol_ratio_20"),
    )
    horizons: tuple[int, ...] = (1, 5, 20)
    candidate_grid_preset: str = "standard"
    max_variants_per_family: int | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


CROSS_SECTIONAL_TRANSFORMS = {
    "cross_sectional_rank_momentum": "rank",
    "cross_sectional_zscore_momentum": "zscore",
}

GENERATED_GRID_PRESETS = ("standard", "broad_v1")


def _safe_numeric_series(df: pd.DataFrame, column: str) -> pd.Series | None:
    if column not in df.columns:
        return None
    return pd.to_numeric(df[column], errors="coerce")


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window).mean()
    rolling_std = series.rolling(window).std()
    return (series - rolling_mean) / rolling_std.replace(0.0, np.nan)


def apply_cross_sectional_transform(panel: pd.DataFrame, *, method: str) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()

    transformed = panel.copy()
    signal = pd.to_numeric(transformed["signal"], errors="coerce")
    transformed["signal"] = signal

    if method == "rank":
        transformed["signal"] = transformed.groupby("timestamp")["signal"].transform(
            lambda values: values.rank(method="average", pct=True)
        )
        return transformed

    if method == "zscore":
        grouped = transformed.groupby("timestamp")["signal"]
        mean = grouped.transform("mean")
        std = grouped.transform("std").replace(0.0, np.nan)
        transformed["signal"] = (signal - mean) / std
        return transformed

    raise ValueError(f"Unsupported cross-sectional transform: {method}")


def build_candidate_signal_id(
    signal_name: str,
    *,
    horizon: int,
    params: dict[str, object],
    variant_id: str = "base",
) -> str:
    param_key = "|".join(
        f"{key}={params[key]}"
        for key in sorted(params)
        if params[key] is not None
    )
    base_key = f"{signal_name}|horizon={int(horizon)}|{param_key}" if param_key else f"{signal_name}|horizon={int(horizon)}"
    normalized_variant = str(variant_id or "base").strip() or "base"
    if normalized_variant == "base":
        return base_key
    return f"{base_key}|variant={normalized_variant}"


def build_generated_candidate_name(
    signal_name: str,
    *,
    horizon: int,
    params: dict[str, object],
    variant_id: str = "base",
) -> str:
    param_key = "_".join(
        f"{key}{params[key]}"
        for key in sorted(params)
        if params[key] is not None
    )
    variant_key = str(variant_id or "base").strip() or "base"
    base_name = f"{signal_name}_hz{int(horizon)}"
    if variant_key != "base":
        base_name = f"{base_name}_{variant_key}"
    return f"{base_name}_{param_key}" if param_key else base_name


def _variant_templates_for_generated_family(
    signal_family: str,
    *,
    preset: str,
) -> list[tuple[str, dict[str, object]]]:
    base_templates: list[tuple[str, dict[str, object]]] = [("base", {})]
    if preset == "standard":
        return base_templates

    if signal_family in {
        "momentum",
        "mean_reversion",
        "market_residual_momentum",
        "residual_momentum",
        "sector_relative_momentum",
        "vol_adjusted_momentum",
        "vol_adjusted_reversal",
    }:
        return [
            ("base", {}),
            ("smoothed", {"smoothing_window_source": "lookback"}),
            ("risk_scaled", {"vol_scale_window_source": "lookback"}),
        ]

    if signal_family in {
        "volatility",
        "breakout_distance",
        "extreme_move",
        "volume_surprise",
        "interaction_momentum_volatility",
        "interaction_reversal_volume_spike",
    }:
        return [
            ("base", {}),
            ("zscored", {"zscore_window_source": "window"}),
            ("clipped", {"clip_abs": 3.0}),
        ]

    return base_templates


def _resolve_variant_params(
    *,
    variant_params: dict[str, object],
    params: dict[str, object],
) -> dict[str, object]:
    resolved = dict(variant_params)

    smoothing_source = resolved.pop("smoothing_window_source", None)
    if smoothing_source is not None and smoothing_source in params:
        resolved["smoothing_window"] = int(params[str(smoothing_source)])

    vol_scale_source = resolved.pop("vol_scale_window_source", None)
    if vol_scale_source is not None and vol_scale_source in params:
        resolved["vol_scale_window"] = int(params[str(vol_scale_source)])

    zscore_source = resolved.pop("zscore_window_source", None)
    if zscore_source is not None and zscore_source in params:
        resolved["zscore_window"] = int(params[str(zscore_source)])

    return resolved


def _build_candidate_row(
    *,
    signal_name: str,
    signal_family: str,
    params: dict[str, object],
    horizon: int,
    lookback: object = pd.NA,
    window: object = pd.NA,
    threshold: object = pd.NA,
    feature_a: object = pd.NA,
    feature_b: object = pd.NA,
    variant_id: str = "base",
    variant_params: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_variant_id = str(variant_id or "base").strip() or "base"
    normalized_variant_params = dict(variant_params or {})
    candidate_config = {
        "signal_family": signal_family,
        "signal_name": signal_name,
        "horizon": int(horizon),
        "parameters": params,
        "variant_id": normalized_variant_id,
        "variant_params": normalized_variant_params,
    }
    return {
        "candidate_id": build_candidate_signal_id(
            signal_name,
            horizon=int(horizon),
            params=params,
            variant_id=normalized_variant_id,
        ),
        "candidate_name": build_generated_candidate_name(
            signal_name,
            horizon=int(horizon),
            params=params,
            variant_id=normalized_variant_id,
        ),
        "variant_id": normalized_variant_id,
        "signal_variant": normalized_variant_id,
        "signal_name": signal_name,
        "signal_family": signal_family,
        "lookback": lookback,
        "window": window,
        "threshold": threshold,
        "feature_a": feature_a,
        "feature_b": feature_b,
        "horizon": int(horizon),
        "parameters_json": json.dumps(params, sort_keys=True),
        "variant_parameters_json": json.dumps(normalized_variant_params, sort_keys=True),
        "config_json": json.dumps(candidate_config, sort_keys=True),
    }


def generate_candidate_signals(
    config: SignalGenerationConfig,
    *,
    feature_columns: list[str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    feature_column_set = set(feature_columns or [])
    normalized_preset = str(config.candidate_grid_preset or "standard").strip().lower()
    if normalized_preset not in GENERATED_GRID_PRESETS:
        raise ValueError(
            f"Unsupported candidate_grid_preset: {config.candidate_grid_preset}. "
            f"Expected one of {', '.join(GENERATED_GRID_PRESETS)}."
        )

    for signal_family in config.signal_families:
        variant_templates = _variant_templates_for_generated_family(
            signal_family,
            preset=normalized_preset,
        )
        if config.max_variants_per_family is not None:
            variant_templates = variant_templates[: max(1, int(config.max_variants_per_family))]
        if signal_family == "momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="momentum",
                                signal_family="momentum",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family in {"mean_reversion", "short_term_reversal"}:
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="mean_reversion",
                                signal_family="mean_reversion",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family in {"volatility", "volatility_based"}:
            for window in config.vol_windows:
                for horizon in config.horizons:
                    params = {"window": int(window)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="volatility",
                                signal_family="volatility",
                                params=params,
                                horizon=int(horizon),
                                window=int(window),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "market_residual_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="market_residual_momentum",
                                signal_family="market_residual_momentum",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "residual_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="residual_momentum",
                                signal_family="residual_momentum",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "sector_relative_momentum":
            sector_feature_prefixes = (
                "sector_momentum_",
                "group_momentum_",
                "industry_momentum_",
                "benchmark_momentum_",
                "sector_return_",
                "group_return_",
                "industry_return_",
            )
            mappings_exist = any(
                any(column.startswith(prefix) for prefix in sector_feature_prefixes)
                for column in feature_column_set
            ) or {"sector", "group", "industry"} & feature_column_set
            if not feature_column_set or mappings_exist:
                for lookback in config.lookbacks:
                    for horizon in config.horizons:
                        params = {"lookback": int(lookback)}
                        for variant_id, variant_params in variant_templates:
                            rows.append(
                                _build_candidate_row(
                                    signal_name="sector_relative_momentum",
                                    signal_family="sector_relative_momentum",
                                    params=params,
                                    horizon=int(horizon),
                                    lookback=int(lookback),
                                    variant_id=variant_id,
                                    variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                                )
                            )
        elif signal_family == "cross_sectional_rank_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="cross_sectional_rank_momentum",
                                signal_family="cross_sectional_rank_momentum",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "cross_sectional_zscore_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="cross_sectional_zscore_momentum",
                                signal_family="cross_sectional_zscore_momentum",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "vol_adjusted_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="vol_adjusted_momentum",
                                signal_family="vol_adjusted_momentum",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "vol_adjusted_reversal":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="vol_adjusted_reversal",
                                signal_family="vol_adjusted_reversal",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "breakout_distance":
            for window in config.lookbacks:
                for horizon in config.horizons:
                    params = {"window": int(window)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="breakout_distance",
                                signal_family="breakout_distance",
                                params=params,
                                horizon=int(horizon),
                                window=int(window),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "extreme_move":
            for window in config.vol_windows:
                for horizon in config.horizons:
                    params = {"window": int(window)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="extreme_move",
                                signal_family="extreme_move",
                                params=params,
                                horizon=int(horizon),
                                window=int(window),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "volume_surprise":
            if not feature_column_set or {"volume", "dollar_volume", "avg_dollar_volume_20"} & feature_column_set:
                for window in config.vol_windows:
                    for horizon in config.horizons:
                        params = {"window": int(window)}
                        for variant_id, variant_params in variant_templates:
                            rows.append(
                                _build_candidate_row(
                                    signal_name="volume_surprise",
                                    signal_family="volume_surprise",
                                    params=params,
                                    horizon=int(horizon),
                                    window=int(window),
                                    variant_id=variant_id,
                                    variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                                )
                            )
        elif signal_family == "interaction_momentum_volatility":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    for variant_id, variant_params in variant_templates:
                        rows.append(
                            _build_candidate_row(
                                signal_name="interaction_momentum_volatility",
                                signal_family="interaction_momentum_volatility",
                                params=params,
                                horizon=int(horizon),
                                lookback=int(lookback),
                                variant_id=variant_id,
                                variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                            )
                        )
        elif signal_family == "interaction_reversal_volume_spike":
            if not feature_column_set or {"volume", "dollar_volume", "avg_dollar_volume_20"} & feature_column_set:
                for lookback in config.lookbacks:
                    for horizon in config.horizons:
                        params = {"lookback": int(lookback)}
                        for variant_id, variant_params in variant_templates:
                            rows.append(
                                _build_candidate_row(
                                    signal_name="interaction_reversal_volume_spike",
                                    signal_family="interaction_reversal_volume_spike",
                                    params=params,
                                    horizon=int(horizon),
                                    lookback=int(lookback),
                                    variant_id=variant_id,
                                    variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                                )
                            )
        elif signal_family == "feature_combo":
            for feature_a, feature_b in config.combo_pairs:
                if feature_column_set and ({feature_a, feature_b} - feature_column_set):
                    continue
                for threshold in config.combo_thresholds:
                    for horizon in config.horizons:
                        params = {
                            "feature_a": feature_a,
                            "feature_b": feature_b,
                            "threshold": float(threshold),
                        }
                        for variant_id, variant_params in variant_templates:
                            rows.append(
                                _build_candidate_row(
                                    signal_name="feature_combo",
                                    signal_family="feature_combo",
                                    params=params,
                                    horizon=int(horizon),
                                    threshold=float(threshold),
                                    feature_a=feature_a,
                                    feature_b=feature_b,
                                    variant_id=variant_id,
                                    variant_params=_resolve_variant_params(variant_params=variant_params, params=params),
                                )
                            )
        else:
            raise ValueError(f"Unsupported signal family in generation config: {signal_family}")

    columns = [
        "candidate_id",
        "candidate_name",
        "variant_id",
        "signal_variant",
        "signal_name",
        "signal_family",
        "lookback",
        "window",
        "threshold",
        "feature_a",
        "feature_b",
        "horizon",
        "parameters_json",
        "variant_parameters_json",
        "config_json",
    ]
    return pd.DataFrame(rows, columns=columns).drop_duplicates(subset=["candidate_id"]).reset_index(drop=True)


def build_generated_signal(
    df: pd.DataFrame,
    candidate_row: pd.Series,
) -> pd.Series:
    signal_name = str(candidate_row["signal_name"])
    close = pd.to_numeric(df["close"], errors="coerce")
    returns = close.pct_change()
    volume = _safe_numeric_series(df, "volume")
    if volume is None:
        volume = _safe_numeric_series(df, "dollar_volume")

    if signal_name == "momentum":
        lookback = int(candidate_row["lookback"])
        signal = close.pct_change(lookback)
    elif signal_name == "mean_reversion":
        lookback = int(candidate_row["lookback"])
        signal = -close.pct_change(lookback)
    elif signal_name == "short_term_reversal":
        lookback = int(candidate_row["lookback"])
        signal = -close.pct_change(lookback)
    elif signal_name == "volatility":
        window = int(candidate_row["window"])
        signal = -returns.rolling(window).std()
    elif signal_name == "market_residual_momentum":
        lookback = int(candidate_row["lookback"])
        raw_momentum = close.pct_change(lookback)
        market_series = None
        for column in (f"market_return_{lookback}", "market_return", f"benchmark_return_{lookback}", "benchmark_return"):
            market_series = _safe_numeric_series(df, column)
            if market_series is not None:
                break
        if market_series is None:
            market_series = returns.rolling(lookback).mean()
        signal = raw_momentum - market_series
    elif signal_name == "vol_adjusted_momentum":
        lookback = int(candidate_row["lookback"])
        vol = returns.rolling(lookback).std()
        raw_momentum = close.pct_change(lookback)
        signal = raw_momentum / vol.replace(0.0, np.nan)
    elif signal_name == "residual_momentum":
        lookback = int(candidate_row["lookback"])
        raw_momentum = close.pct_change(lookback)
        trailing_baseline = returns.rolling(lookback).mean() * float(lookback)
        signal = raw_momentum - trailing_baseline
    elif signal_name == "sector_relative_momentum":
        lookback = int(candidate_row["lookback"])
        raw_momentum = close.pct_change(lookback)
        baseline_columns = [
            f"sector_mean_return_{lookback}",
            "sector_mean_return",
            f"sector_momentum_{lookback}",
            f"group_momentum_{lookback}",
            f"industry_momentum_{lookback}",
            f"benchmark_momentum_{lookback}",
            f"sector_return_{lookback}",
            f"group_return_{lookback}",
            f"industry_return_{lookback}",
        ]
        baseline_series = None
        for column in baseline_columns:
            if column in df.columns:
                baseline_series = pd.to_numeric(df[column], errors="coerce")
                break
        if baseline_series is None:
            signal = pd.Series(index=df.index, dtype="float64")
        else:
            signal = raw_momentum - baseline_series
    elif signal_name == "cross_sectional_rank_momentum":
        lookback = int(candidate_row["lookback"])
        signal = close.pct_change(lookback)
    elif signal_name == "cross_sectional_zscore_momentum":
        lookback = int(candidate_row["lookback"])
        signal = close.pct_change(lookback)
    elif signal_name == "vol_adjusted_reversal":
        lookback = int(candidate_row["lookback"])
        raw_reversal = -close.pct_change(lookback)
        vol = returns.rolling(lookback).std()
        signal = raw_reversal / vol.replace(0.0, np.nan)
    elif signal_name == "breakout_distance":
        window = int(candidate_row["window"])
        rolling_high = close.rolling(window).max()
        rolling_low = close.rolling(window).min()
        high_distance = close / rolling_high - 1.0
        low_distance = close / rolling_low - 1.0
        signal = high_distance + low_distance
    elif signal_name == "extreme_move":
        window = int(candidate_row["window"])
        move = returns
        rolling_rank = move.rolling(window).rank(pct=True)
        extreme_high = (rolling_rank >= 0.9).astype(float)
        extreme_low = (rolling_rank <= 0.1).astype(float)
        signal = extreme_high - extreme_low
    elif signal_name == "volume_surprise":
        window = int(candidate_row["window"])
        if volume is None:
            signal = pd.Series(index=df.index, dtype="float64")
        else:
            trailing_volume = volume.rolling(window).mean()
            signal = volume / trailing_volume.replace(0.0, np.nan) - 1.0
    elif signal_name == "interaction_momentum_volatility":
        lookback = int(candidate_row["lookback"])
        momentum = close.pct_change(lookback)
        volatility = returns.rolling(lookback).std()
        signal = momentum * _rolling_zscore(volatility, lookback)
    elif signal_name == "interaction_reversal_volume_spike":
        lookback = int(candidate_row["lookback"])
        reversal = -close.pct_change(lookback)
        if volume is None:
            signal = pd.Series(index=df.index, dtype="float64")
        else:
            volume_spike = volume / volume.rolling(lookback).mean().replace(0.0, np.nan) - 1.0
            signal = reversal * volume_spike
    elif signal_name == "feature_combo":
        feature_a = str(candidate_row["feature_a"])
        feature_b = str(candidate_row["feature_b"])
        threshold = float(candidate_row["threshold"])
        if feature_a not in df.columns or feature_b not in df.columns:
            signal = pd.Series(index=df.index, dtype="float64")
        else:
            left = pd.to_numeric(df[feature_a], errors="coerce")
            right = pd.to_numeric(df[feature_b], errors="coerce")
            signal = left - threshold * right
    else:
        raise ValueError(f"Unsupported generated signal name: {signal_name}")

    raw_variant_params = candidate_row.get("variant_parameters_json")
    if isinstance(raw_variant_params, str) and raw_variant_params.strip():
        try:
            variant_params = json.loads(raw_variant_params)
        except json.JSONDecodeError:
            variant_params = {}
    elif isinstance(raw_variant_params, dict):
        variant_params = dict(raw_variant_params)
    else:
        variant_params = {}

    smoothing_window = variant_params.get("smoothing_window")
    if smoothing_window is not None:
        signal = signal.rolling(int(smoothing_window)).mean()

    zscore_window = variant_params.get("zscore_window")
    if zscore_window is not None:
        signal = _rolling_zscore(signal, int(zscore_window))

    vol_scale_window = variant_params.get("vol_scale_window")
    if vol_scale_window is not None:
        scale = returns.rolling(int(vol_scale_window)).std()
        signal = signal / scale.replace(0.0, np.nan)

    clip_abs = variant_params.get("clip_abs")
    if clip_abs is not None:
        signal = signal.clip(lower=-float(clip_abs), upper=float(clip_abs))

    return signal
