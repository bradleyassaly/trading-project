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

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


CROSS_SECTIONAL_TRANSFORMS = {
    "cross_sectional_rank_momentum": "rank",
    "cross_sectional_zscore_momentum": "zscore",
}


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
) -> str:
    param_key = "|".join(
        f"{key}={params[key]}"
        for key in sorted(params)
        if params[key] is not None
    )
    return f"{signal_name}|horizon={int(horizon)}|{param_key}" if param_key else f"{signal_name}|horizon={int(horizon)}"


def generate_candidate_signals(
    config: SignalGenerationConfig,
    *,
    feature_columns: list[str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    feature_column_set = set(feature_columns or [])

    for signal_family in config.signal_families:
        if signal_family == "momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("momentum", horizon=int(horizon), params=params),
                            "signal_name": "momentum",
                            "signal_family": "momentum",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family in {"mean_reversion", "short_term_reversal"}:
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("mean_reversion", horizon=int(horizon), params=params),
                            "signal_name": "mean_reversion",
                            "signal_family": "mean_reversion",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family in {"volatility", "volatility_based"}:
            for window in config.vol_windows:
                for horizon in config.horizons:
                    params = {"window": int(window)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("volatility", horizon=int(horizon), params=params),
                            "signal_name": "volatility",
                            "signal_family": "volatility",
                            "lookback": pd.NA,
                            "window": int(window),
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "market_residual_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("market_residual_momentum", horizon=int(horizon), params=params),
                            "signal_name": "market_residual_momentum",
                            "signal_family": "market_residual_momentum",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "residual_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("residual_momentum", horizon=int(horizon), params=params),
                            "signal_name": "residual_momentum",
                            "signal_family": "residual_momentum",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
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
                        rows.append(
                            {
                                "candidate_id": build_candidate_signal_id("sector_relative_momentum", horizon=int(horizon), params=params),
                                "signal_name": "sector_relative_momentum",
                                "signal_family": "sector_relative_momentum",
                                "lookback": int(lookback),
                                "window": pd.NA,
                                "threshold": pd.NA,
                                "feature_a": pd.NA,
                                "feature_b": pd.NA,
                                "horizon": int(horizon),
                                "parameters_json": json.dumps(params, sort_keys=True),
                            }
                        )
        elif signal_family == "cross_sectional_rank_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("cross_sectional_rank_momentum", horizon=int(horizon), params=params),
                            "signal_name": "cross_sectional_rank_momentum",
                            "signal_family": "cross_sectional_rank_momentum",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "cross_sectional_zscore_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("cross_sectional_zscore_momentum", horizon=int(horizon), params=params),
                            "signal_name": "cross_sectional_zscore_momentum",
                            "signal_family": "cross_sectional_zscore_momentum",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "vol_adjusted_momentum":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("vol_adjusted_momentum", horizon=int(horizon), params=params),
                            "signal_name": "vol_adjusted_momentum",
                            "signal_family": "vol_adjusted_momentum",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "vol_adjusted_reversal":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("vol_adjusted_reversal", horizon=int(horizon), params=params),
                            "signal_name": "vol_adjusted_reversal",
                            "signal_family": "vol_adjusted_reversal",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "breakout_distance":
            for window in config.lookbacks:
                for horizon in config.horizons:
                    params = {"window": int(window)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("breakout_distance", horizon=int(horizon), params=params),
                            "signal_name": "breakout_distance",
                            "signal_family": "breakout_distance",
                            "lookback": pd.NA,
                            "window": int(window),
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "extreme_move":
            for window in config.vol_windows:
                for horizon in config.horizons:
                    params = {"window": int(window)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("extreme_move", horizon=int(horizon), params=params),
                            "signal_name": "extreme_move",
                            "signal_family": "extreme_move",
                            "lookback": pd.NA,
                            "window": int(window),
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "volume_surprise":
            if not feature_column_set or {"volume", "dollar_volume", "avg_dollar_volume_20"} & feature_column_set:
                for window in config.vol_windows:
                    for horizon in config.horizons:
                        params = {"window": int(window)}
                        rows.append(
                            {
                                "candidate_id": build_candidate_signal_id("volume_surprise", horizon=int(horizon), params=params),
                                "signal_name": "volume_surprise",
                                "signal_family": "volume_surprise",
                                "lookback": pd.NA,
                                "window": int(window),
                                "threshold": pd.NA,
                                "feature_a": pd.NA,
                                "feature_b": pd.NA,
                                "horizon": int(horizon),
                                "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "interaction_momentum_volatility":
            for lookback in config.lookbacks:
                for horizon in config.horizons:
                    params = {"lookback": int(lookback)}
                    rows.append(
                        {
                            "candidate_id": build_candidate_signal_id("interaction_momentum_volatility", horizon=int(horizon), params=params),
                            "signal_name": "interaction_momentum_volatility",
                            "signal_family": "interaction_momentum_volatility",
                            "lookback": int(lookback),
                            "window": pd.NA,
                            "threshold": pd.NA,
                            "feature_a": pd.NA,
                            "feature_b": pd.NA,
                            "horizon": int(horizon),
                            "parameters_json": json.dumps(params, sort_keys=True),
                        }
                    )
        elif signal_family == "interaction_reversal_volume_spike":
            if not feature_column_set or {"volume", "dollar_volume", "avg_dollar_volume_20"} & feature_column_set:
                for lookback in config.lookbacks:
                    for horizon in config.horizons:
                        params = {"lookback": int(lookback)}
                        rows.append(
                            {
                                "candidate_id": build_candidate_signal_id("interaction_reversal_volume_spike", horizon=int(horizon), params=params),
                                "signal_name": "interaction_reversal_volume_spike",
                                "signal_family": "interaction_reversal_volume_spike",
                                "lookback": int(lookback),
                                "window": pd.NA,
                                "threshold": pd.NA,
                                "feature_a": pd.NA,
                                "feature_b": pd.NA,
                                "horizon": int(horizon),
                                "parameters_json": json.dumps(params, sort_keys=True),
                            }
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
                        rows.append(
                            {
                                "candidate_id": build_candidate_signal_id("feature_combo", horizon=int(horizon), params=params),
                                "signal_name": "feature_combo",
                                "signal_family": "feature_combo",
                                "lookback": pd.NA,
                                "window": pd.NA,
                                "threshold": float(threshold),
                                "feature_a": feature_a,
                                "feature_b": feature_b,
                                "horizon": int(horizon),
                                "parameters_json": json.dumps(params, sort_keys=True),
                            }
                        )
        else:
            raise ValueError(f"Unsupported signal family in generation config: {signal_family}")

    columns = [
        "candidate_id",
        "signal_name",
        "signal_family",
        "lookback",
        "window",
        "threshold",
        "feature_a",
        "feature_b",
        "horizon",
        "parameters_json",
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
        return close.pct_change(lookback)

    if signal_name == "mean_reversion":
        lookback = int(candidate_row["lookback"])
        return -close.pct_change(lookback)

    if signal_name == "short_term_reversal":
        lookback = int(candidate_row["lookback"])
        return -close.pct_change(lookback)

    if signal_name == "volatility":
        window = int(candidate_row["window"])
        return -returns.rolling(window).std()

    if signal_name == "market_residual_momentum":
        lookback = int(candidate_row["lookback"])
        raw_momentum = close.pct_change(lookback)
        market_series = None
        for column in (f"market_return_{lookback}", "market_return", f"benchmark_return_{lookback}", "benchmark_return"):
            market_series = _safe_numeric_series(df, column)
            if market_series is not None:
                break
        if market_series is None:
            market_series = returns.rolling(lookback).mean()
        return raw_momentum - market_series

    if signal_name == "vol_adjusted_momentum":
        lookback = int(candidate_row["lookback"])
        vol = returns.rolling(lookback).std()
        raw_momentum = close.pct_change(lookback)
        return raw_momentum / vol.replace(0.0, np.nan)

    if signal_name == "residual_momentum":
        lookback = int(candidate_row["lookback"])
        raw_momentum = close.pct_change(lookback)
        trailing_baseline = returns.rolling(lookback).mean() * float(lookback)
        return raw_momentum - trailing_baseline

    if signal_name == "sector_relative_momentum":
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
            return pd.Series(index=df.index, dtype="float64")
        return raw_momentum - baseline_series

    if signal_name == "cross_sectional_rank_momentum":
        lookback = int(candidate_row["lookback"])
        return close.pct_change(lookback)

    if signal_name == "cross_sectional_zscore_momentum":
        lookback = int(candidate_row["lookback"])
        return close.pct_change(lookback)

    if signal_name == "vol_adjusted_reversal":
        lookback = int(candidate_row["lookback"])
        raw_reversal = -close.pct_change(lookback)
        vol = returns.rolling(lookback).std()
        return raw_reversal / vol.replace(0.0, np.nan)

    if signal_name == "breakout_distance":
        window = int(candidate_row["window"])
        rolling_high = close.rolling(window).max()
        rolling_low = close.rolling(window).min()
        high_distance = close / rolling_high - 1.0
        low_distance = close / rolling_low - 1.0
        return high_distance + low_distance

    if signal_name == "extreme_move":
        window = int(candidate_row["window"])
        move = returns
        rolling_rank = move.rolling(window).rank(pct=True)
        extreme_high = (rolling_rank >= 0.9).astype(float)
        extreme_low = (rolling_rank <= 0.1).astype(float)
        return extreme_high - extreme_low

    if signal_name == "volume_surprise":
        window = int(candidate_row["window"])
        if volume is None:
            return pd.Series(index=df.index, dtype="float64")
        trailing_volume = volume.rolling(window).mean()
        return volume / trailing_volume.replace(0.0, np.nan) - 1.0

    if signal_name == "interaction_momentum_volatility":
        lookback = int(candidate_row["lookback"])
        momentum = close.pct_change(lookback)
        volatility = returns.rolling(lookback).std()
        return momentum * _rolling_zscore(volatility, lookback)

    if signal_name == "interaction_reversal_volume_spike":
        lookback = int(candidate_row["lookback"])
        reversal = -close.pct_change(lookback)
        if volume is None:
            return pd.Series(index=df.index, dtype="float64")
        volume_spike = volume / volume.rolling(lookback).mean().replace(0.0, np.nan) - 1.0
        return reversal * volume_spike

    if signal_name == "feature_combo":
        feature_a = str(candidate_row["feature_a"])
        feature_b = str(candidate_row["feature_b"])
        threshold = float(candidate_row["threshold"])
        if feature_a not in df.columns or feature_b not in df.columns:
            return pd.Series(index=df.index, dtype="float64")
        left = pd.to_numeric(df[feature_a], errors="coerce")
        right = pd.to_numeric(df[feature_b], errors="coerce")
        return left - threshold * right

    raise ValueError(f"Unsupported generated signal name: {signal_name}")
