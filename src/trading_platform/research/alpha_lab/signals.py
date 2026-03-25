from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


SUPPORTED_SIGNAL_FAMILIES = (
    "momentum",
    "short_term_reversal",
    "vol_adjusted_momentum",
    "volatility_adjusted_momentum",
    "volatility_adjusted_reversal",
    "equity_context_momentum",
    "short_horizon_mean_reversion",
    "momentum_acceleration",
    "cross_sectional_momentum",
    "cross_sectional_relative_strength",
    "breakout_continuation",
    "benchmark_relative_rotation",
    "regime_conditioned_momentum",
    "volatility_dispersion_selection",
    "sector_relative_momentum",
    "liquidity_flow_tilt",
    "volume_shock_momentum",
    "fundamental_value",
    "fundamental_quality",
    "fundamental_growth",
    "fundamental_quality_value",
)

CANDIDATE_GRID_PRESETS = ("standard", "broad_v1")
SIGNAL_COMPOSITION_PRESETS = ("standard", "composite_v1", "research_rich_v1")


@dataclass(frozen=True)
class SignalCandidateSpec:
    signal_family: str
    signal_variant: str
    lookback: int
    horizon: int
    variant_params: dict[str, float | str | bool]


def build_candidate_name(
    signal_family: str,
    *,
    signal_variant: str,
    lookback: int,
    horizon: int,
) -> str:
    if str(signal_variant or "base") == "base":
        return f"{signal_family}_lb{int(lookback)}_hz{int(horizon)}"
    return f"{signal_family}_{signal_variant}_lb{int(lookback)}_hz{int(horizon)}"


def _variant_templates_for_family(
    signal_family: str,
    *,
    preset: str,
) -> list[tuple[str, dict[str, float | str | bool]]]:
    base_templates: list[tuple[str, dict[str, float | str | bool]]] = [("base", {})]
    if preset == "standard":
        return base_templates

    if signal_family in {"momentum", "cross_sectional_momentum", "cross_sectional_relative_strength"}:
        return [
            ("base", {}),
            ("fast_trend", {"raw_momentum_weight": 0.35, "breadth_weight": 0.15, "trend_weight": 1.2}),
            ("relative_strength_focus", {"market_relative_weight": 1.3, "raw_momentum_weight": 0.15, "relative_rank_weight": 0.35}),
            ("breadth_confirmed", {"market_relative_weight": 1.1, "breadth_weight": 0.45, "context_confirmation_weight": 0.25}),
            ("volatility_filtered", {"market_relative_weight": 1.0, "volatility_penalty": 0.6, "low_vol_preference_weight": 0.3}),
            ("flow_confirmed", {"market_relative_weight": 0.9, "volume_confirmation_weight": 0.35, "flow_confirmation_weight": 0.3}),
        ]

    if signal_family == "breakout_continuation":
        return [
            ("base", {}),
            ("tight_breakout", {"breakout_weight": 1.35, "range_penalty_weight": 0.8, "breakout_percentile_weight": 0.35}),
            ("range_expansion", {"breakout_weight": 1.15, "range_penalty_weight": 0.45, "trend_weight": 0.9}),
            ("momentum_heavy", {"momentum_weight": 1.3, "breakout_weight": 0.95, "trend_weight": 1.1}),
            ("market_confirmed", {"market_bias_weight": 0.35, "momentum_weight": 1.0, "context_confirmation_weight": 0.35}),
            ("pure_breakout", {"momentum_weight": 0.55, "breakout_weight": 1.65, "breakout_percentile_weight": 0.5}),
        ]

    if signal_family == "benchmark_relative_rotation":
        return [
            ("base", {}),
            ("breadth_confirmed", {"breadth_weight": 1.5, "volatility_penalty_power": 1.0, "relative_rank_weight": 0.2}),
            ("low_vol_leaders", {"breadth_weight": 1.0, "volatility_penalty_power": 1.35, "low_vol_preference_weight": 0.4}),
            ("offensive_rotation", {"breadth_weight": 1.2, "market_bias_weight": 0.35, "context_confirmation_weight": 0.2}),
            ("defensive_rotation", {"breadth_weight": 0.75, "volatility_penalty_power": 1.6, "context_confirmation_weight": 0.25}),
            ("relative_strength_max", {"breadth_weight": 0.9, "volatility_penalty_power": 0.8, "relative_rank_weight": 0.45}),
        ]

    if signal_family == "regime_conditioned_momentum":
        return [
            ("base", {}),
            ("defensive", {"risk_off_multiplier": 0.10, "risk_on_multiplier": 0.95, "breadth_weight": 0.5, "low_vol_preference_weight": 0.3}),
            ("balanced", {"risk_off_multiplier": 0.35, "risk_on_multiplier": 1.0, "breadth_weight": 1.0, "relative_strength_weight": 0.2}),
            ("breadth_amplified", {"risk_off_multiplier": 0.25, "risk_on_multiplier": 1.0, "breadth_weight": 1.8, "context_confirmation_weight": 0.25}),
            ("pro_cyclical", {"risk_off_multiplier": 0.55, "risk_on_multiplier": 1.1, "breadth_weight": 1.2, "trend_weight": 1.1}),
            ("relative_regime", {"risk_off_multiplier": 0.3, "relative_strength_weight": 0.45, "breadth_weight": 0.9, "relative_rank_weight": 0.25}),
        ]

    if signal_family == "volatility_adjusted_reversal":
        return [
            ("base", {}),
            ("fast_reversal", {"volatility_power": 0.85, "return_zscore_window": 5, "reversal_intensity_weight": 0.4}),
            ("balanced_reversal", {"volatility_power": 1.0, "return_zscore_window": 10, "reversal_intensity_weight": 0.3}),
            ("shock_reversal", {"volatility_power": 1.15, "relative_strength_weight": 0.25, "flow_confirmation_weight": 0.15}),
            ("smoothed_reversal", {"volatility_power": 1.25, "return_zscore_window": 20, "reversal_intensity_weight": 0.2}),
        ]

    if signal_family == "volatility_dispersion_selection":
        return [
            ("base", {}),
            ("high_dispersion", {"dispersion_multiplier": 1.4, "vol_penalty_weight": 0.8, "dispersion_weight": 0.45}),
            ("low_vol_quality", {"dispersion_multiplier": 0.9, "vol_penalty_weight": 1.4, "low_vol_preference_weight": 0.45}),
            ("breadth_supported", {"dispersion_multiplier": 1.1, "breadth_weight": 0.35, "context_confirmation_weight": 0.2}),
            ("balanced_dispersion", {"dispersion_multiplier": 1.0, "vol_penalty_weight": 1.0, "dispersion_weight": 0.3}),
        ]

    if signal_family == "sector_relative_momentum":
        return [
            ("base", {}),
            ("sector_leader", {"sector_weight": 1.25, "market_context_weight": 0.0, "trend_weight": 0.25}),
            ("sector_plus_market", {"sector_weight": 0.9, "market_context_weight": 0.3, "context_confirmation_weight": 0.2}),
            ("sector_plus_breadth", {"sector_weight": 1.0, "breadth_weight": 0.35, "relative_rank_weight": 0.2}),
            ("market_neutral_sector", {"sector_weight": 1.3, "market_context_weight": -0.15, "low_vol_preference_weight": 0.2}),
            ("raw_sector_gap", {"sector_weight": 1.5, "market_context_weight": 0.0, "relative_rank_weight": 0.15}),
        ]

    if signal_family == "liquidity_flow_tilt":
        return [
            ("base", {}),
            ("volume_emphasis", {"volume_weight": 0.8, "dollar_flow_weight": 0.2, "trend_weight": 1.0, "flow_confirmation_weight": 0.3}),
            ("dollar_flow_emphasis", {"volume_weight": 0.2, "dollar_flow_weight": 0.8, "trend_weight": 1.0, "flow_confirmation_weight": 0.35}),
            ("flow_with_trend", {"volume_weight": 0.5, "dollar_flow_weight": 0.5, "trend_weight": 1.3, "breakout_weight": 0.2}),
            ("conservative_flow", {"volume_weight": 0.5, "dollar_flow_weight": 0.5, "flow_bias_weight": 0.15, "flow_clip_max": 1.75}),
            ("aggressive_flow", {"volume_weight": 0.6, "dollar_flow_weight": 0.4, "trend_weight": 1.15, "flow_bias_weight": 0.35, "flow_confirmation_weight": 0.4}),
        ]

    if signal_family == "fundamental_value":
        return [
            ("base", {}),
            ("earnings_yield_focus", {"earnings_yield_weight": 1.4, "book_to_market_weight": 0.7}),
            ("book_to_market_focus", {"earnings_yield_weight": 0.7, "book_to_market_weight": 1.4}),
            ("cash_flow_value", {"free_cash_flow_yield_weight": 1.3, "sales_to_price_weight": 0.7}),
            ("sector_neutral_value", {"sector_neutral_weight": 1.2}),
        ]

    if signal_family == "fundamental_quality":
        return [
            ("base", {}),
            ("profitability_focus", {"profitability_weight": 1.3, "balance_sheet_weight": 0.7}),
            ("balance_sheet_focus", {"profitability_weight": 0.7, "balance_sheet_weight": 1.3}),
            ("cash_flow_quality", {"cash_flow_quality_weight": 1.25}),
            ("sector_neutral_quality", {"sector_neutral_weight": 1.2}),
        ]

    if signal_family == "fundamental_growth":
        return [
            ("base", {}),
            ("revenue_growth_focus", {"revenue_growth_weight": 1.4, "net_income_growth_weight": 0.7}),
            ("earnings_growth_focus", {"revenue_growth_weight": 0.7, "net_income_growth_weight": 1.4}),
            ("balanced_growth", {"revenue_growth_weight": 1.0, "net_income_growth_weight": 1.0}),
            ("sector_neutral_growth", {"sector_neutral_weight": 1.2}),
        ]

    if signal_family == "fundamental_quality_value":
        return [
            ("base", {}),
            ("value_tilt", {"value_weight": 1.3, "quality_weight": 0.8}),
            ("quality_tilt", {"value_weight": 0.8, "quality_weight": 1.3}),
            ("balanced_compounder", {"value_weight": 1.0, "quality_weight": 1.0}),
            ("sector_neutral_blend", {"sector_neutral_weight": 1.2}),
        ]

    return base_templates


def build_candidate_grid(
    *,
    signal_family: str,
    lookbacks: list[int],
    horizons: list[int],
    candidate_grid_preset: str = "standard",
    max_variants_per_family: int | None = None,
) -> list[SignalCandidateSpec]:
    normalized_preset = str(candidate_grid_preset or "standard").strip().lower()
    if normalized_preset not in CANDIDATE_GRID_PRESETS:
        raise ValueError(
            f"Unsupported candidate_grid_preset: {candidate_grid_preset}. "
            f"Expected one of {', '.join(CANDIDATE_GRID_PRESETS)}."
        )

    templates = _variant_templates_for_family(signal_family, preset=normalized_preset)
    if max_variants_per_family is not None:
        templates = templates[: max(1, int(max_variants_per_family))]

    candidates: list[SignalCandidateSpec] = []
    for variant_name, params in templates:
        for lookback in lookbacks:
            for horizon in horizons:
                candidates.append(
                    SignalCandidateSpec(
                        signal_family=signal_family,
                        signal_variant=str(variant_name),
                        lookback=int(lookback),
                        horizon=int(horizon),
                        variant_params=dict(params),
                    )
                )
    return candidates


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window).mean()
    rolling_std = series.rolling(window).std()
    return (series - rolling_mean) / rolling_std.replace(0.0, np.nan)


def _feature(
    df: pd.DataFrame,
    *candidates: str,
    default: float | pd.Series = 0.0,
) -> pd.Series:
    for column in candidates:
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce")
    if isinstance(default, pd.Series):
        return pd.to_numeric(default, errors="coerce")
    return pd.Series(float(default), index=df.index, dtype=float)


def _relative_strength_signal(
    df: pd.DataFrame,
    *,
    lookback: int,
    close: pd.Series,
) -> pd.Series:
    relative_column = f"relative_return_{lookback}"
    if relative_column in df.columns:
        return pd.to_numeric(df[relative_column], errors="coerce")
    market_column = f"market_return_{lookback}"
    raw_momentum = close.pct_change(lookback)
    if market_column in df.columns:
        market_return = pd.to_numeric(df[market_column], errors="coerce")
        return raw_momentum - market_return
    return raw_momentum


def _baseline_series(
    df: pd.DataFrame,
    candidates: tuple[str, ...],
) -> pd.Series | None:
    for column in candidates:
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce")
    return None


def _resolve_signal_composition(
    *,
    signal_composition_preset: str,
    enable_context_confirmations: bool | None,
    enable_relative_features: bool | None,
    enable_flow_confirmations: bool | None,
) -> dict[str, bool | float | str]:
    normalized = str(signal_composition_preset or "standard").strip().lower()
    if normalized not in SIGNAL_COMPOSITION_PRESETS:
        raise ValueError(
            f"Unsupported signal_composition_preset: {signal_composition_preset}. "
            f"Expected one of {', '.join(SIGNAL_COMPOSITION_PRESETS)}."
        )

    preset_defaults = {
        "standard": {"context": False, "relative": False, "flow": False, "richness": 1.0},
        "composite_v1": {"context": True, "relative": True, "flow": True, "richness": 1.0},
        "research_rich_v1": {"context": True, "relative": True, "flow": True, "richness": 1.15},
    }[normalized]
    return {
        "preset": normalized,
        "use_composite": normalized != "standard",
        "context": preset_defaults["context"] if enable_context_confirmations is None else bool(enable_context_confirmations),
        "relative": preset_defaults["relative"] if enable_relative_features is None else bool(enable_relative_features),
        "flow": preset_defaults["flow"] if enable_flow_confirmations is None else bool(enable_flow_confirmations),
        "richness": float(preset_defaults["richness"]),
    }


def _trend_features(df: pd.DataFrame, *, lookback: int, close: pd.Series) -> tuple[pd.Series, pd.Series]:
    raw_momentum = close.pct_change(lookback)
    trend_slope = _feature(df, f"trend_slope_{lookback}", default=raw_momentum)
    trend_persistence = _feature(df, f"trend_persistence_{lookback}", default=0.0)
    return trend_slope.fillna(0.0), trend_persistence.fillna(0.0)


def _volatility_preference(df: pd.DataFrame, *, lookback: int, close: pd.Series) -> tuple[pd.Series, pd.Series]:
    realized_vol = _feature(
        df,
        f"realized_vol_{lookback}",
        "realized_vol_20",
        default=close.pct_change().rolling(max(lookback, 2)).std(),
    )
    vol_rank = _feature(df, f"cross_sectional_vol_rank_{lookback}", default=_rolling_zscore(realized_vol, max(lookback, 5)))
    return realized_vol, (-vol_rank).fillna(0.0)


def _context_confirmation(df: pd.DataFrame, *, lookback: int) -> pd.Series:
    breadth = _feature(df, f"breadth_impulse_{lookback}", default=0.0)
    market_trend = _feature(df, f"market_trend_strength_{lookback}", f"market_return_{lookback}", default=0.0)
    return (0.6 * breadth.fillna(0.0) + 0.4 * market_trend.fillna(0.0)).clip(lower=-2.0, upper=2.0)


def _flow_confirmation(df: pd.DataFrame, *, lookback: int) -> pd.Series:
    return _feature(
        df,
        f"flow_confirmation_{lookback}",
        f"dollar_volume_ratio_{lookback}",
        f"volume_ratio_{lookback}",
        "volume_ratio_20",
        default=0.0,
    ).fillna(0.0)


def _relative_rank(df: pd.DataFrame, *, lookback: int) -> pd.Series:
    return _feature(
        df,
        f"cross_sectional_relative_rank_{lookback}",
        f"cross_sectional_return_rank_{lookback}",
        default=0.0,
    ).fillna(0.0)


def _fundamental_recency_weight(df: pd.DataFrame, *, lookback: int) -> pd.Series:
    filing_age_days = _feature(df, "days_since_available", default=np.nan)
    return np.exp(-pd.to_numeric(filing_age_days, errors="coerce").clip(lower=0.0) / max(float(lookback), 1.0))


def _fundamental_signal(df: pd.DataFrame, *, signal_family: str, lookback: int, params: dict[str, float | str | bool]) -> pd.Series:
    value_score = _feature(df, "fundamental_value_score", default=0.0).fillna(0.0)
    quality_score = _feature(df, "fundamental_quality_score", default=0.0).fillna(0.0)
    growth_score = _feature(df, "fundamental_growth_score", default=0.0).fillna(0.0)
    quality_value_score = _feature(df, "fundamental_quality_value_score", default=0.0).fillna(0.0)
    recency_weight = _fundamental_recency_weight(df, lookback=lookback)

    if signal_family == "fundamental_value":
        sector_neutral_weight = float(params.get("sector_neutral_weight", 0.0))
        sector_neutral_score = _feature(df, "sector_neutral_value_score", default=value_score).fillna(0.0)
        signal = (
            float(params.get("earnings_yield_weight", 1.0)) * _feature(df, "earnings_yield", default=0.0).fillna(0.0)
            + float(params.get("book_to_market_weight", 1.0)) * _feature(df, "book_to_market", default=0.0).fillna(0.0)
            + float(params.get("sales_to_price_weight", 1.0)) * _feature(df, "sales_to_price", default=0.0).fillna(0.0)
            + float(params.get("free_cash_flow_yield_weight", 1.0)) * _feature(df, "free_cash_flow_yield", default=0.0).fillna(0.0)
            + sector_neutral_weight * sector_neutral_score
        ) / max(4.0 + sector_neutral_weight, 1.0)
        return signal.fillna(value_score) * recency_weight

    if signal_family == "fundamental_quality":
        sector_neutral_weight = float(params.get("sector_neutral_weight", 0.0))
        sector_neutral_score = _feature(df, "sector_neutral_quality_score", default=quality_score).fillna(0.0)
        profitability = _feature(df, "roe", default=0.0).fillna(0.0) + _feature(df, "roa", default=0.0).fillna(0.0) + _feature(df, "gross_margin", default=0.0).fillna(0.0) + _feature(df, "operating_margin", default=0.0).fillna(0.0)
        balance_sheet = _feature(df, "current_ratio", default=0.0).fillna(0.0) - _feature(df, "debt_to_equity", default=0.0).fillna(0.0)
        cash_flow_quality = _feature(df, "free_cash_flow_yield", default=0.0).fillna(0.0) - _feature(df, "accruals_proxy", default=0.0).fillna(0.0)
        signal = (
            float(params.get("profitability_weight", 1.0)) * profitability / 4.0
            + float(params.get("balance_sheet_weight", 1.0)) * balance_sheet / 2.0
            + float(params.get("cash_flow_quality_weight", 1.0)) * cash_flow_quality / 2.0
            + sector_neutral_weight * sector_neutral_score
        ) / max(3.0 + sector_neutral_weight, 1.0)
        return signal.fillna(quality_score) * recency_weight

    if signal_family == "fundamental_growth":
        sector_neutral_weight = float(params.get("sector_neutral_weight", 0.0))
        sector_neutral_score = _feature(df, "sector_neutral_growth_score", default=growth_score).fillna(0.0)
        signal = (
            float(params.get("revenue_growth_weight", 1.0)) * _feature(df, "revenue_growth_yoy", default=0.0).fillna(0.0)
            + float(params.get("net_income_growth_weight", 1.0)) * _feature(df, "net_income_growth_yoy", default=0.0).fillna(0.0)
            + sector_neutral_weight * sector_neutral_score
        ) / max(2.0 + sector_neutral_weight, 1.0)
        return signal.fillna(growth_score) * recency_weight

    if signal_family == "fundamental_quality_value":
        sector_neutral_weight = float(params.get("sector_neutral_weight", 0.0))
        sector_neutral_score = _feature(df, "sector_neutral_quality_value_score", default=quality_value_score).fillna(0.0)
        signal = (
            float(params.get("value_weight", 1.0)) * value_score
            + float(params.get("quality_weight", 1.0)) * quality_score
            + sector_neutral_weight * sector_neutral_score
        ) / max(float(params.get("value_weight", 1.0)) + float(params.get("quality_weight", 1.0)) + sector_neutral_weight, 1.0)
        return signal.fillna(quality_value_score) * recency_weight

    raise ValueError(f"Unsupported signal family: {signal_family}")


def _legacy_signal(
    df: pd.DataFrame,
    *,
    signal_family: str,
    lookback: int,
    params: dict[str, float | str | bool],
    close: pd.Series,
) -> pd.Series:
    if signal_family == "momentum":
        raw_momentum = close.pct_change(lookback)
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight and f"breadth_impulse_{lookback}" in df.columns:
            breadth = pd.to_numeric(df[f"breadth_impulse_{lookback}"], errors="coerce").fillna(0.0)
            raw_momentum = raw_momentum + breadth_weight * breadth
        return raw_momentum

    if signal_family == "short_term_reversal":
        return -close.pct_change(lookback)

    if signal_family in {"vol_adjusted_momentum", "volatility_adjusted_momentum"}:
        returns = close.pct_change()
        vol = returns.rolling(lookback).std()
        raw_momentum = close.pct_change(lookback)
        return raw_momentum / vol.replace(0.0, np.nan)

    if signal_family == "volatility_adjusted_reversal":
        returns = close.pct_change()
        vol = returns.rolling(lookback).std()
        raw_reversal = -close.pct_change(lookback)
        volatility_power = float(params.get("volatility_power", 1.0))
        signal = raw_reversal / vol.replace(0.0, np.nan).pow(volatility_power)
        return_zscore_window = int(params.get("return_zscore_window", 0) or 0)
        if return_zscore_window > 1:
            signal = signal + _rolling_zscore(raw_reversal, return_zscore_window).fillna(0.0)
        relative_strength_weight = float(params.get("relative_strength_weight", 0.0))
        if relative_strength_weight:
            signal = signal - relative_strength_weight * _relative_strength_signal(
                df,
                lookback=lookback,
                close=close,
            ).fillna(0.0)
        return signal

    if signal_family == "short_horizon_mean_reversion":
        returns = close.pct_change()
        return -_rolling_zscore(returns, lookback)

    if signal_family == "momentum_acceleration":
        fast_lookback = max(1, lookback // 2)
        return close.pct_change(fast_lookback) - close.pct_change(lookback)

    if signal_family in {"cross_sectional_relative_strength", "cross_sectional_momentum"}:
        relative_signal = _relative_strength_signal(df, lookback=lookback, close=close)
        signal = relative_signal * float(params.get("market_relative_weight", 1.0))
        raw_momentum_weight = float(params.get("raw_momentum_weight", 0.0))
        if raw_momentum_weight:
            signal = signal.fillna(0.0) + raw_momentum_weight * close.pct_change(lookback).fillna(0.0)
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight and f"breadth_impulse_{lookback}" in df.columns:
            breadth = pd.to_numeric(df[f"breadth_impulse_{lookback}"], errors="coerce").fillna(0.0)
            signal = signal.fillna(0.0) + breadth_weight * breadth
        volatility_penalty = float(params.get("volatility_penalty", 0.0))
        if volatility_penalty:
            realized_vol = pd.to_numeric(
                df.get("realized_vol_20", df.get(f"realized_vol_{lookback}", close.pct_change().rolling(lookback).std())),
                errors="coerce",
            ).fillna(0.0)
            signal = signal.fillna(0.0) - volatility_penalty * _rolling_zscore(realized_vol, max(lookback, 5)).fillna(0.0)
        volume_confirmation_weight = float(params.get("volume_confirmation_weight", 0.0))
        if volume_confirmation_weight:
            volume_ratio = pd.to_numeric(
                df.get("volume_ratio_20", df.get(f"volume_ratio_{lookback}", pd.Series(1.0, index=df.index))),
                errors="coerce",
            ).fillna(1.0)
            signal = signal.fillna(0.0) + volume_confirmation_weight * (volume_ratio.clip(lower=0.5, upper=2.0) - 1.0)
        return signal.where(relative_signal.notna(), np.nan)

    if signal_family == "breakout_continuation":
        rolling_high = close.shift(1).rolling(lookback).max()
        rolling_low = close.shift(1).rolling(lookback).min()
        breakout_distance = (close - rolling_high) / rolling_high.replace(0.0, np.nan)
        continuation_momentum = close.pct_change(lookback)
        trading_range = (rolling_high - rolling_low) / rolling_low.replace(0.0, np.nan)
        momentum_weight = float(params.get("momentum_weight", 1.0))
        breakout_weight = float(params.get("breakout_weight", 1.0))
        range_penalty_weight = float(params.get("range_penalty_weight", 1.0))
        signal = (
            momentum_weight * continuation_momentum
            + breakout_weight * breakout_distance / trading_range.replace(0.0, np.nan).pow(range_penalty_weight)
        )
        if f"market_return_{lookback}" in df.columns:
            market_bias_weight = float(params.get("market_bias_weight", 0.25))
            signal = signal + market_bias_weight * pd.to_numeric(df[f"market_return_{lookback}"], errors="coerce")
        return signal

    if signal_family == "benchmark_relative_rotation":
        relative_signal = _relative_strength_signal(df, lookback=lookback, close=close)
        breadth = pd.to_numeric(
            df.get(f"breadth_impulse_{lookback}", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)
        realized_vol = pd.to_numeric(
            df.get("realized_vol_20", df.get(f"realized_vol_{lookback}", pd.Series(1.0, index=df.index))),
            errors="coerce",
        ).where(lambda values: values.abs() > 1e-6, 1.0).fillna(1.0)
        breadth_weight = float(params.get("breadth_weight", 1.0))
        volatility_penalty_power = float(params.get("volatility_penalty_power", 1.0))
        signal = relative_signal * (
            1.0 + breadth_weight * breadth.clip(lower=-0.5, upper=0.5)
        ) / realized_vol.pow(volatility_penalty_power)
        market_bias_weight = float(params.get("market_bias_weight", 0.0))
        if market_bias_weight and f"market_return_{lookback}" in df.columns:
            signal = signal + market_bias_weight * pd.to_numeric(df[f"market_return_{lookback}"], errors="coerce").fillna(0.0)
        return signal

    if signal_family == "regime_conditioned_momentum":
        raw_momentum = close.pct_change(lookback)
        market_return = pd.to_numeric(
            df.get(f"market_return_{lookback}", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)
        breadth = pd.to_numeric(
            df.get(f"breadth_impulse_{lookback}", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)
        risk_on_multiplier = float(params.get("risk_on_multiplier", 1.0))
        risk_off_multiplier = float(params.get("risk_off_multiplier", 0.25))
        risk_multiplier = np.where(market_return >= 0.0, risk_on_multiplier, risk_off_multiplier)
        breadth_weight = float(params.get("breadth_weight", 1.0))
        signal = raw_momentum * risk_multiplier * (
            1.0 + breadth_weight * breadth.clip(lower=-0.5, upper=0.5)
        )
        relative_strength_weight = float(params.get("relative_strength_weight", 0.0))
        if relative_strength_weight:
            signal = signal + relative_strength_weight * _relative_strength_signal(
                df,
                lookback=lookback,
                close=close,
            ).fillna(0.0)
        return signal

    if signal_family == "volatility_dispersion_selection":
        relative_signal = _relative_strength_signal(df, lookback=lookback, close=close).fillna(0.0)
        realized_vol = pd.to_numeric(
            df.get("realized_vol_20", df.get(f"realized_vol_{lookback}", close.pct_change().rolling(lookback).std())),
            errors="coerce",
        )
        vol_penalty_weight = float(params.get("vol_penalty_weight", 1.0))
        vol_penalty = vol_penalty_weight * _rolling_zscore(realized_vol, max(lookback, 5)).fillna(0.0)
        regime_multiplier = pd.Series(1.0, index=df.index, dtype=float)
        if "dispersion_regime" in df.columns:
            dispersion_regime = df["dispersion_regime"].astype(str).str.lower()
            dispersion_multiplier = float(params.get("dispersion_multiplier", 1.25))
            regime_multiplier = pd.Series(
                np.where(dispersion_regime.eq("high_dispersion"), dispersion_multiplier, 0.85),
                index=df.index,
                dtype=float,
            )
        signal = (relative_signal - vol_penalty) * regime_multiplier
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight and f"breadth_impulse_{lookback}" in df.columns:
            signal = signal + breadth_weight * pd.to_numeric(df[f"breadth_impulse_{lookback}"], errors="coerce").fillna(0.0)
        return signal

    if signal_family == "sector_relative_momentum":
        raw_momentum = close.pct_change(lookback)
        sector_baseline = _baseline_series(
            df,
            (
                f"sector_mean_return_{lookback}",
                "sector_mean_return",
                f"sector_momentum_{lookback}",
                f"group_momentum_{lookback}",
                f"industry_momentum_{lookback}",
                f"sector_return_{lookback}",
                f"group_return_{lookback}",
                f"industry_return_{lookback}",
                f"benchmark_return_{lookback}",
            ),
        )
        if sector_baseline is None:
            return _relative_strength_signal(df, lookback=lookback, close=close)
        sector_weight = float(params.get("sector_weight", 1.0))
        signal = sector_weight * (raw_momentum - sector_baseline)
        market_context_weight = float(params.get("market_context_weight", 0.0))
        if market_context_weight and f"relative_return_{lookback}" in df.columns:
            signal = signal + market_context_weight * pd.to_numeric(df[f"relative_return_{lookback}"], errors="coerce").fillna(0.0)
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight and f"breadth_impulse_{lookback}" in df.columns:
            signal = signal + breadth_weight * pd.to_numeric(df[f"breadth_impulse_{lookback}"], errors="coerce").fillna(0.0)
        return signal

    if signal_family == "liquidity_flow_tilt":
        relative_signal = _relative_strength_signal(df, lookback=lookback, close=close).fillna(0.0)
        volume_ratio = pd.to_numeric(
            df.get("volume_ratio_20", df.get(f"volume_ratio_{lookback}", pd.Series(1.0, index=df.index))),
            errors="coerce",
        ).fillna(1.0)
        if "avg_dollar_volume_20" in df.columns and "dollar_volume" in df.columns:
            avg_dollar_volume = pd.to_numeric(df["avg_dollar_volume_20"], errors="coerce")
            dollar_volume = pd.to_numeric(df["dollar_volume"], errors="coerce")
            dollar_flow = (dollar_volume / avg_dollar_volume.replace(0.0, np.nan)).fillna(1.0)
        else:
            dollar_flow = volume_ratio
        volume_weight = float(params.get("volume_weight", 0.5))
        dollar_flow_weight = float(params.get("dollar_flow_weight", 0.5))
        flow_clip_max = float(params.get("flow_clip_max", 2.5))
        flow_tilt = (
            volume_weight * volume_ratio.clip(lower=0.5, upper=flow_clip_max)
            + dollar_flow_weight * dollar_flow.clip(lower=0.5, upper=flow_clip_max)
        )
        trend_weight = float(params.get("trend_weight", 1.0))
        flow_bias_weight = float(params.get("flow_bias_weight", 0.25))
        return trend_weight * relative_signal * flow_tilt + flow_bias_weight * (flow_tilt - 1.0)

    if signal_family == "volume_shock_momentum":
        raw_momentum = close.pct_change(lookback)
        if "volume_ratio_20" in df.columns:
            volume_ratio = pd.to_numeric(df["volume_ratio_20"], errors="coerce")
        elif "volume" in df.columns:
            volume = pd.to_numeric(df["volume"], errors="coerce")
            volume_ratio = volume / volume.rolling(max(lookback, 5)).mean()
        else:
            volume_ratio = pd.Series(1.0, index=df.index, dtype=float)
        return raw_momentum * volume_ratio.clip(lower=0.5, upper=2.0)

    if signal_family == "equity_context_momentum":
        raw_signal = pd.to_numeric(
            df.get(f"relative_return_{lookback}", close.pct_change(lookback)),
            errors="coerce",
        )
        if f"breadth_impulse_{lookback}" in df.columns:
            breadth = pd.to_numeric(df[f"breadth_impulse_{lookback}"], errors="coerce").fillna(0.0)
            raw_signal = raw_signal * (1.0 + breadth)
        for column in ("realized_vol_20", f"realized_vol_{lookback}"):
            if column in df.columns:
                realized_vol = pd.to_numeric(df[column], errors="coerce")
                volatility_scale = realized_vol.where(realized_vol > 1e-6, 1.0).fillna(1.0)
                raw_signal = raw_signal / volatility_scale
                break
        if "volume_ratio_20" in df.columns:
            volume_ratio = pd.to_numeric(df["volume_ratio_20"], errors="coerce").clip(lower=0.5, upper=1.5)
            raw_signal = raw_signal * volume_ratio.fillna(1.0)
        return raw_signal

    raise ValueError(f"Unsupported signal family: {signal_family}")


def _composite_signal(
    df: pd.DataFrame,
    *,
    signal_family: str,
    lookback: int,
    params: dict[str, float | str | bool],
    close: pd.Series,
    composition: dict[str, bool | float | str],
) -> pd.Series:
    richness = float(composition["richness"])
    raw_momentum = close.pct_change(lookback).fillna(0.0)
    relative_signal = _relative_strength_signal(df, lookback=lookback, close=close).fillna(0.0)
    relative_rank = _relative_rank(df, lookback=lookback)
    trend_slope, trend_persistence = _trend_features(df, lookback=lookback, close=close)
    realized_vol, low_vol_preference = _volatility_preference(df, lookback=lookback, close=close)
    context_confirmation = _context_confirmation(df, lookback=lookback) if bool(composition["context"]) else pd.Series(0.0, index=df.index, dtype=float)
    flow_confirmation = _flow_confirmation(df, lookback=lookback) if bool(composition["flow"]) else pd.Series(0.0, index=df.index, dtype=float)
    relative_component = relative_signal if bool(composition["relative"]) else raw_momentum

    if signal_family in {"momentum", "cross_sectional_momentum", "cross_sectional_relative_strength"}:
        signal = (
            float(params.get("market_relative_weight", 1.0)) * relative_component
            + float(params.get("raw_momentum_weight", 0.2)) * raw_momentum
            + float(params.get("trend_weight", 0.35)) * (0.6 * trend_slope + 0.4 * trend_persistence)
            + float(params.get("relative_rank_weight", 0.25)) * relative_rank
            + float(params.get("low_vol_preference_weight", 0.2)) * low_vol_preference
            + float(params.get("context_confirmation_weight", 0.15)) * context_confirmation
            + float(params.get("flow_confirmation_weight", 0.15)) * flow_confirmation
        )
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight:
            signal = signal + breadth_weight * _feature(df, f"breadth_impulse_{lookback}", default=0.0).fillna(0.0)
        volatility_penalty = float(params.get("volatility_penalty", 0.0))
        if volatility_penalty:
            signal = signal - volatility_penalty * _rolling_zscore(realized_vol, max(lookback, 5)).fillna(0.0)
        volume_confirmation_weight = float(params.get("volume_confirmation_weight", 0.0))
        if volume_confirmation_weight:
            signal = signal + volume_confirmation_weight * flow_confirmation
        return signal.where(_relative_strength_signal(df, lookback=lookback, close=close).notna(), np.nan)

    if signal_family == "breakout_continuation":
        breakout_distance = _feature(df, f"breakout_distance_{lookback}", default=0.0).fillna(0.0)
        breakout_percentile = _feature(df, f"breakout_percentile_{lookback}", default=0.0).fillna(0.0)
        volatility_scale = realized_vol.where(realized_vol.abs() > 1e-6, 1.0).fillna(1.0)
        signal = (
            float(params.get("momentum_weight", 1.0)) * raw_momentum
            + float(params.get("breakout_weight", 1.0)) * breakout_distance / volatility_scale.pow(float(params.get("range_penalty_weight", 1.0)))
            + float(params.get("breakout_percentile_weight", 0.3)) * breakout_percentile
            + 0.25 * trend_slope
            + float(params.get("context_confirmation_weight", 0.2)) * context_confirmation
            + 0.2 * flow_confirmation
        )
        market_bias_weight = float(params.get("market_bias_weight", 0.0))
        if market_bias_weight:
            signal = signal + market_bias_weight * _feature(df, f"market_return_{lookback}", default=0.0).fillna(0.0)
        return signal * richness

    if signal_family == "benchmark_relative_rotation":
        signal = (
            relative_component
            + float(params.get("relative_rank_weight", 0.25)) * relative_rank
            + float(params.get("breadth_weight", 1.0)) * _feature(df, f"breadth_impulse_{lookback}", default=0.0).fillna(0.0)
            + 0.25 * context_confirmation
            + float(params.get("low_vol_preference_weight", 0.2)) * low_vol_preference
        ) / realized_vol.where(realized_vol.abs() > 1e-6, 1.0).fillna(1.0).pow(float(params.get("volatility_penalty_power", 1.0)))
        market_bias_weight = float(params.get("market_bias_weight", 0.0))
        if market_bias_weight:
            signal = signal + market_bias_weight * _feature(df, f"market_return_{lookback}", default=0.0).fillna(0.0)
        return signal * richness

    if signal_family == "regime_conditioned_momentum":
        market_return = _feature(df, f"market_return_{lookback}", default=0.0).fillna(0.0)
        risk_multiplier = pd.Series(
            np.where(
                market_return >= 0.0,
                float(params.get("risk_on_multiplier", 1.0)),
                float(params.get("risk_off_multiplier", 0.25)),
            ),
            index=df.index,
            dtype=float,
        )
        signal = (
            raw_momentum
            + float(params.get("relative_strength_weight", 0.2 if composition["relative"] else 0.0)) * relative_signal
            + float(params.get("trend_weight", 0.35)) * (0.6 * trend_slope + 0.4 * trend_persistence)
            + float(params.get("breadth_weight", 1.0)) * _feature(df, f"breadth_impulse_{lookback}", default=0.0).fillna(0.0)
            + 0.25 * context_confirmation
            + float(params.get("low_vol_preference_weight", 0.0)) * low_vol_preference
        ) * risk_multiplier
        return signal * richness

    if signal_family == "volatility_adjusted_reversal":
        raw_reversal = -close.pct_change(lookback).fillna(0.0)
        reversal_intensity = _feature(
            df,
            f"reversal_intensity_{lookback}",
            default=raw_reversal / realized_vol.where(realized_vol.abs() > 1e-6, 1.0).fillna(1.0),
        ).fillna(0.0)
        signal = raw_reversal / realized_vol.where(realized_vol.abs() > 1e-6, 1.0).fillna(1.0).pow(float(params.get("volatility_power", 1.0)))
        return_zscore_window = int(params.get("return_zscore_window", 0) or 0)
        if return_zscore_window > 1:
            signal = signal + _rolling_zscore(raw_reversal, return_zscore_window).fillna(0.0)
        signal = signal + float(params.get("reversal_intensity_weight", 0.3)) * reversal_intensity
        relative_strength_weight = float(params.get("relative_strength_weight", 0.0))
        if relative_strength_weight:
            signal = signal - relative_strength_weight * relative_signal
        signal = signal + 0.15 * low_vol_preference + 0.1 * flow_confirmation
        return signal * richness

    if signal_family == "volatility_dispersion_selection":
        signal = (
            relative_component
            + 0.25 * relative_rank
            + float(params.get("dispersion_weight", 0.3))
            * float(params.get("dispersion_multiplier", 1.0))
            * _feature(df, f"market_dispersion_{lookback}", "dispersion_state_score", default=0.0).fillna(0.0)
            + float(params.get("low_vol_preference_weight", 0.25)) * low_vol_preference
            + 0.15 * context_confirmation
            - float(params.get("vol_penalty_weight", 1.0)) * _rolling_zscore(realized_vol, max(lookback, 5)).fillna(0.0)
        )
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight:
            signal = signal + breadth_weight * _feature(df, f"breadth_impulse_{lookback}", default=0.0).fillna(0.0)
        return signal * richness

    if signal_family == "sector_relative_momentum":
        sector_baseline = _baseline_series(
            df,
            (
                f"sector_mean_return_{lookback}",
                "sector_mean_return",
                f"sector_momentum_{lookback}",
                f"group_momentum_{lookback}",
                f"industry_momentum_{lookback}",
                f"sector_return_{lookback}",
                f"group_return_{lookback}",
                f"industry_return_{lookback}",
                f"benchmark_return_{lookback}",
            ),
        )
        sector_gap = relative_signal if sector_baseline is None else raw_momentum - sector_baseline.fillna(0.0)
        signal = (
            float(params.get("sector_weight", 1.0)) * sector_gap
            + float(params.get("market_context_weight", 0.0)) * relative_signal
            + float(params.get("relative_rank_weight", 0.2)) * relative_rank
            + 0.25 * trend_slope
            + 0.15 * context_confirmation
            + float(params.get("low_vol_preference_weight", 0.0)) * low_vol_preference
        )
        breadth_weight = float(params.get("breadth_weight", 0.0))
        if breadth_weight:
            signal = signal + breadth_weight * _feature(df, f"breadth_impulse_{lookback}", default=0.0).fillna(0.0)
        return signal * richness

    if signal_family == "liquidity_flow_tilt":
        volume_ratio = _feature(df, f"volume_ratio_{lookback}", "volume_ratio_20", default=1.0).fillna(1.0)
        dollar_ratio = _feature(df, f"dollar_volume_ratio_{lookback}", "avg_dollar_volume_20", default=1.0).fillna(1.0)
        breakout_distance = _feature(df, f"breakout_distance_{lookback}", default=0.0).fillna(0.0)
        flow_tilt = (
            float(params.get("volume_weight", 0.5)) * volume_ratio.clip(lower=0.5, upper=float(params.get("flow_clip_max", 2.5)))
            + float(params.get("dollar_flow_weight", 0.5)) * dollar_ratio.clip(lower=0.5, upper=float(params.get("flow_clip_max", 2.5)))
        )
        signal = (
            float(params.get("trend_weight", 1.0)) * (0.7 * relative_component + 0.3 * trend_slope) * flow_tilt
            + float(params.get("flow_confirmation_weight", 0.25)) * flow_confirmation
            + float(params.get("flow_bias_weight", 0.25)) * (flow_tilt - 1.0)
            + float(params.get("breakout_weight", 0.1)) * breakout_distance
        )
        return signal * richness

    if signal_family == "equity_context_momentum":
        signal = (
            relative_component
            + 0.25 * (trend_slope + trend_persistence)
            + 0.2 * context_confirmation
            + 0.15 * flow_confirmation
            + 0.15 * low_vol_preference
        )
        return signal * richness

    if signal_family in {"vol_adjusted_momentum", "volatility_adjusted_momentum"}:
        signal = (
            _feature(
                df,
                f"vol_adjusted_return_{lookback}",
                default=raw_momentum / realized_vol.where(realized_vol.abs() > 1e-6, 1.0).fillna(1.0),
            ).fillna(0.0)
            + 0.2 * trend_persistence
            + 0.15 * low_vol_preference
            + 0.1 * context_confirmation
        )
        return signal * richness

    if signal_family == "volume_shock_momentum":
        return raw_momentum * (1.0 + 0.5 * flow_confirmation.clip(lower=-1.0, upper=2.0)) * richness

    return _legacy_signal(df, signal_family=signal_family, lookback=lookback, params=params, close=close)


def build_signal(
    df: pd.DataFrame,
    *,
    signal_family: str,
    lookback: int,
    signal_variant: str | None = None,
    variant_params: dict[str, float | str | bool] | None = None,
    close_column: str = "close",
    signal_composition_preset: str = "standard",
    enable_context_confirmations: bool | None = None,
    enable_relative_features: bool | None = None,
    enable_flow_confirmations: bool | None = None,
) -> pd.Series:
    close = df[close_column]
    params = variant_params or {}
    if signal_family in {"fundamental_value", "fundamental_quality", "fundamental_growth", "fundamental_quality_value"}:
        return _fundamental_signal(df, signal_family=signal_family, lookback=lookback, params=params)
    composition = _resolve_signal_composition(
        signal_composition_preset=signal_composition_preset,
        enable_context_confirmations=enable_context_confirmations,
        enable_relative_features=enable_relative_features,
        enable_flow_confirmations=enable_flow_confirmations,
    )

    if not bool(composition["use_composite"]):
        return _legacy_signal(
            df,
            signal_family=signal_family,
            lookback=lookback,
            params=params,
            close=close,
        )

    return _composite_signal(
        df,
        signal_family=signal_family,
        lookback=lookback,
        params=params,
        close=close,
        composition=composition,
    )
