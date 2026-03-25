from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from trading_platform.research.alpha_lab.data_loading import load_symbol_feature_data

from trading_platform.research.registry import write_research_run_manifest
from trading_platform.research.alpha_lab.composite import (
    DEFAULT_COMPOSITE_CONFIG,
    build_composite_scores,
    candidate_id,
    evaluate_composite_scores,
    select_low_redundancy_signals,
)
from trading_platform.research.alpha_lab.lifecycle import (
    DEFAULT_SIGNAL_LIFECYCLE_CONFIG,
    SignalLifecycleConfig,
    build_dynamic_signal_weights,
)
from trading_platform.research.alpha_lab.regime import (
    DEFAULT_REGIME_CONFIG,
    RegimeConfig,
    build_regime_aware_signal_weights,
    build_regime_labels_by_date,
    compute_signal_performance_by_regime,
)
from trading_platform.research.alpha_lab.context import (
    build_benchmark_context_by_symbol_date,
    build_sub_universe_membership_by_symbol_date,
    compute_signal_performance_by_benchmark_context,
    compute_signal_performance_by_sub_universe,
    enrich_symbol_data_with_explicit_context,
    write_context_feature_panels,
)
from trading_platform.research.alpha_lab.composite_portfolio import (
    DEFAULT_COMPOSITE_PORTFOLIO_CONFIG,
    CompositePortfolioConfig,
    build_asset_return_matrix,
    build_regime_performance_report,
    build_robustness_report,
    run_composite_portfolio_backtest,
    run_stress_tests,
)
from trading_platform.research.approved_model_state import write_approved_model_state
from trading_platform.research.alpha_lab.folds import build_walk_forward_folds
from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.metrics import (
    compute_cross_sectional_daily_metrics,
    evaluate_cross_sectional_signal,
)
from trading_platform.research.ensemble import (
    EnsembleConfig,
    assign_member_weights,
    build_ensemble_scores,
    select_ensemble_members,
)
from trading_platform.research.alpha_lab.promotion import (
    DEFAULT_PROMOTION_THRESHOLDS,
    apply_promotion_rules,
)
from trading_platform.research.alpha_lab.signals import (
    build_candidate_grid,
    build_candidate_name,
    build_signal,
)


def _centered_cross_sectional_rank(panel: pd.DataFrame) -> pd.DataFrame:
    return panel.rank(axis=1, pct=True).sub(0.5).mul(2.0)


def _build_signal_family_summary(leaderboard_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "signal_family",
        "candidate_count",
        "variant_count",
        "composite_candidate_count",
        "promotion_count",
        "mean_spearman_ic",
        "top_spearman_ic",
    ]
    if leaderboard_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for signal_family, family_df in leaderboard_df.groupby("signal_family", dropna=False):
        variant_series = family_df.get("signal_variant", pd.Series(index=family_df.index, dtype=object)).fillna("base").astype(str)
        rows.append(
            {
                "signal_family": signal_family,
                "candidate_count": int(len(family_df)),
                "variant_count": int(variant_series.nunique()),
                "composite_candidate_count": int((variant_series != "base").sum()),
                "promotion_count": int((family_df["promotion_status"] == "promote").sum()),
                "mean_spearman_ic": float(pd.to_numeric(family_df["mean_spearman_ic"], errors="coerce").mean()),
                "top_spearman_ic": float(pd.to_numeric(family_df["mean_spearman_ic"], errors="coerce").max()),
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("signal_family").reset_index(drop=True)


def _load_daily_fundamental_features(
    daily_features_path: Path | None,
    *,
    symbols: list[str],
) -> pd.DataFrame:
    if daily_features_path is None or not daily_features_path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(daily_features_path)
    if frame.empty:
        return frame
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    return frame.loc[frame["symbol"].isin([symbol.upper() for symbol in symbols])].copy()


def _merge_daily_fundamental_features(
    symbol_data: dict[str, pd.DataFrame],
    *,
    daily_features_df: pd.DataFrame,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    if daily_features_df.empty:
        return symbol_data, {"enabled": False, "rows": 0, "symbols_with_features": 0}

    enriched: dict[str, pd.DataFrame] = {}
    symbols_with_features = 0
    for symbol, df in symbol_data.items():
        symbol_features = daily_features_df.loc[daily_features_df["symbol"] == symbol.upper()].copy()
        if symbol_features.empty:
            enriched[symbol] = df
            continue
        symbols_with_features += 1
        merged = df.merge(
            symbol_features.drop(columns=["symbol"], errors="ignore"),
            on="timestamp",
            how="left",
        )
        enriched[symbol] = merged
    return enriched, {
        "enabled": True,
        "rows": int(len(daily_features_df)),
        "symbols_with_features": symbols_with_features,
        "feature_columns": sorted(
            column for column in daily_features_df.columns if column not in {"timestamp", "symbol"}
        ),
    }


def _build_fundamental_feature_ic_summary(
    symbol_data: dict[str, pd.DataFrame],
    *,
    horizons: list[int],
    top_quantile: float,
    bottom_quantile: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    fundamental_columns = set()
    for df in symbol_data.values():
        fundamental_columns.update(
            column
            for column in df.columns
            if column.startswith("fundamental_")
            or column.startswith("sector_neutral_")
            or column in {
                "earnings_yield",
                "book_to_market",
                "sales_to_price",
                "roe",
                "roa",
                "gross_margin",
                "operating_margin",
                "revenue_growth_yoy",
                "net_income_growth_yoy",
                "debt_to_equity",
                "current_ratio",
                "free_cash_flow_yield",
                "accruals_proxy",
            }
        )
    for horizon in horizons:
        label_col = f"fwd_return_{horizon}d"
        for feature_name in sorted(fundamental_columns):
            frames: list[pd.DataFrame] = []
            for symbol, df in symbol_data.items():
                if feature_name not in df.columns or label_col not in df.columns:
                    continue
                frames.append(
                    df[["timestamp", "symbol", feature_name, label_col]]
                    .rename(columns={feature_name: "signal", label_col: "forward_return"})
                    .dropna(subset=["signal", "forward_return"])
                )
            if not frames:
                continue
            panel = pd.concat(frames, ignore_index=True)
            if panel.empty:
                continue
            metrics = evaluate_cross_sectional_signal(
                panel,
                top_quantile=top_quantile,
                bottom_quantile=bottom_quantile,
            )
            rows.append({"feature_name": feature_name, "horizon": int(horizon), **metrics})
    return pd.DataFrame(rows)


def _load_symbol_feature_data(feature_dir: Path, symbol: str) -> pd.DataFrame:
    return load_symbol_feature_data(feature_dir, symbol)


def _resolve_symbols(symbols: list[str] | None, universe: str | None) -> list[str]:
    if symbols:
        return sorted(set(symbols))

    if universe:
        universe_path = Path("config/universes") / f"{universe}.txt"
        if universe_path.exists():
            return [line.strip() for line in universe_path.read_text().splitlines() if line.strip()]
        raise ValueError(
            f"Universe '{universe}' was provided, but no resolver is wired yet."
        )

    raise ValueError("Provide either --symbols or --universe.")


def _slice_fold(
    df: pd.DataFrame,
    *,
    test_start: pd.Timestamp,
    test_end: pd.Timestamp,
) -> pd.DataFrame:
    mask = (df["timestamp"] >= test_start) & (df["timestamp"] <= test_end)
    return df.loc[mask].copy()


def _build_shared_folds(
    symbol_data: dict[str, pd.DataFrame],
    *,
    train_size: int,
    test_size: int,
    step_size: int | None,
    min_train_size: int | None,
) -> list:
    if not symbol_data:
        return []

    timestamps = pd.Series(
        sorted(
            {
                timestamp
                for df in symbol_data.values()
                for timestamp in pd.to_datetime(df["timestamp"]).tolist()
            }
        )
    )

    return build_walk_forward_folds(
        timestamps,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
    )


def _add_equity_context_features(
    symbol_data: dict[str, pd.DataFrame],
    *,
    lookbacks: list[int],
    include_volume: bool,
    vol_window: int = 20,
    volume_window: int = 20,
) -> dict[str, pd.DataFrame]:
    if not symbol_data:
        return symbol_data

    unique_lookbacks = sorted(set(int(lookback) for lookback in lookbacks))
    close_panel = pd.concat(
        [
            pd.Series(pd.to_numeric(df["close"], errors="coerce").to_numpy(), index=pd.to_datetime(df["timestamp"]), name=symbol)
            for symbol, df in sorted(symbol_data.items())
        ],
        axis=1,
    ).sort_index()
    return_panel = close_panel.pct_change()
    own_return_panels = {
        lookback: close_panel.pct_change(lookback)
        for lookback in unique_lookbacks
    }
    market_return_panels = {
        lookback: close_panel.pct_change(lookback).mean(axis=1, skipna=True)
        for lookback in unique_lookbacks
    }
    breadth_panels = {
        lookback: close_panel.pct_change(lookback).gt(0.0).mean(axis=1, skipna=True)
        for lookback in unique_lookbacks
    }
    realized_vol_panels = {
        lookback: return_panel.rolling(max(lookback, 2)).std()
        for lookback in unique_lookbacks
    }
    market_dispersion_panels = {
        lookback: own_return_panels[lookback].std(axis=1, skipna=True)
        for lookback in unique_lookbacks
    }
    relative_return_panels = {
        lookback: own_return_panels[lookback].sub(market_return_panels[lookback], axis=0)
        for lookback in unique_lookbacks
    }
    cross_sectional_return_rank_panels = {
        lookback: _centered_cross_sectional_rank(own_return_panels[lookback])
        for lookback in unique_lookbacks
    }
    cross_sectional_relative_rank_panels = {
        lookback: _centered_cross_sectional_rank(relative_return_panels[lookback])
        for lookback in unique_lookbacks
    }
    cross_sectional_vol_rank_panels = {
        lookback: _centered_cross_sectional_rank(realized_vol_panels[lookback])
        for lookback in unique_lookbacks
    }
    market_vol_panels = {
        lookback: market_return_panels[lookback].rolling(max(5, min(lookback * 2, 60))).std()
        for lookback in unique_lookbacks
    }

    volume_panel: pd.DataFrame | None = None
    if include_volume and all("volume" in df.columns for df in symbol_data.values()):
        volume_panel = pd.concat(
            [
                pd.Series(pd.to_numeric(df["volume"], errors="coerce").to_numpy(), index=pd.to_datetime(df["timestamp"]), name=symbol)
                for symbol, df in sorted(symbol_data.items())
            ],
            axis=1,
        ).sort_index()

    enriched: dict[str, pd.DataFrame] = {}
    for symbol, df in symbol_data.items():
        working = df.sort_values("timestamp").copy()
        timestamp_index = pd.to_datetime(working["timestamp"])
        symbol_close = pd.Series(pd.to_numeric(working["close"], errors="coerce").to_numpy(), index=timestamp_index)
        symbol_returns = return_panel[symbol].reindex(timestamp_index)
        working[f"realized_vol_{vol_window}"] = symbol_returns.rolling(vol_window).std().to_numpy()
        for lookback in unique_lookbacks:
            own_return = own_return_panels[lookback][symbol].reindex(timestamp_index)
            market_return = market_return_panels[lookback].reindex(timestamp_index)
            breadth = breadth_panels[lookback].reindex(timestamp_index)
            realized_vol = realized_vol_panels[lookback][symbol].reindex(timestamp_index)
            rolling_high = symbol_close.shift(1).rolling(lookback).max()
            rolling_low = symbol_close.shift(1).rolling(lookback).min()
            rolling_range = (rolling_high - rolling_low).replace(0.0, np.nan)
            short_reversal_window = max(2, min(lookback, 5))
            working[f"market_return_{lookback}"] = market_return.to_numpy()
            working[f"relative_return_{lookback}"] = (own_return - market_return).to_numpy()
            working[f"breadth_positive_{lookback}"] = breadth.to_numpy()
            working[f"breadth_impulse_{lookback}"] = (breadth - 0.5).to_numpy()
            working[f"return_{lookback}"] = own_return.to_numpy()
            working[f"realized_vol_{lookback}"] = realized_vol.to_numpy()
            working[f"vol_adjusted_return_{lookback}"] = (
                own_return / realized_vol.replace(0.0, np.nan)
            ).to_numpy()
            working[f"trend_slope_{lookback}"] = (
                own_return / realized_vol.replace(0.0, np.nan)
            ).to_numpy()
            working[f"trend_persistence_{lookback}"] = (
                symbol_returns.gt(0.0).rolling(lookback).mean().sub(0.5).mul(2.0)
            ).reindex(timestamp_index).to_numpy()
            working[f"breakout_distance_{lookback}"] = (
                (symbol_close - rolling_high) / rolling_high.replace(0.0, np.nan)
            ).to_numpy()
            working[f"breakout_percentile_{lookback}"] = (
                ((symbol_close - rolling_low) / rolling_range) - 0.5
            ).mul(2.0).to_numpy()
            working[f"reversal_intensity_{lookback}"] = (
                -symbol_close.pct_change(short_reversal_window) / realized_vol.replace(0.0, np.nan)
            ).to_numpy()
            working[f"market_trend_strength_{lookback}"] = (
                market_return / market_vol_panels[lookback].reindex(timestamp_index).replace(0.0, np.nan)
            ).to_numpy()
            working[f"market_dispersion_{lookback}"] = market_dispersion_panels[lookback].reindex(timestamp_index).to_numpy()
            working[f"cross_sectional_return_rank_{lookback}"] = (
                cross_sectional_return_rank_panels[lookback][symbol].reindex(timestamp_index)
            ).to_numpy()
            working[f"cross_sectional_relative_rank_{lookback}"] = (
                cross_sectional_relative_rank_panels[lookback][symbol].reindex(timestamp_index)
            ).to_numpy()
            working[f"cross_sectional_vol_rank_{lookback}"] = (
                cross_sectional_vol_rank_panels[lookback][symbol].reindex(timestamp_index)
            ).to_numpy()
        if volume_panel is not None:
            symbol_volume = volume_panel[symbol].reindex(timestamp_index)
            dollar_volume = symbol_close * symbol_volume
            working["dollar_volume"] = dollar_volume.to_numpy()
            working[f"volume_ratio_{volume_window}"] = (
                symbol_volume / symbol_volume.rolling(volume_window).mean()
            ).to_numpy()
            working[f"avg_dollar_volume_{volume_window}"] = dollar_volume.rolling(volume_window).mean().to_numpy()
            for lookback in unique_lookbacks:
                volume_ratio = symbol_volume / symbol_volume.rolling(lookback).mean()
                dollar_volume_ratio = dollar_volume / dollar_volume.rolling(lookback).mean()
                own_return = own_return_panels[lookback][symbol].reindex(timestamp_index)
                working[f"volume_ratio_{lookback}"] = volume_ratio.to_numpy()
                working[f"avg_dollar_volume_{lookback}"] = dollar_volume.rolling(lookback).mean().to_numpy()
                working[f"dollar_volume_ratio_{lookback}"] = dollar_volume_ratio.to_numpy()
                flow_confirmation = (
                    0.5 * volume_ratio.clip(lower=0.5, upper=2.5).sub(1.0)
                    + 0.5 * dollar_volume_ratio.clip(lower=0.5, upper=2.5).sub(1.0)
                ) * np.sign(own_return.fillna(0.0))
                working[f"flow_confirmation_{lookback}"] = flow_confirmation.to_numpy()
        enriched[symbol] = working
    return enriched


def _candidate_key(
    signal_family: str,
    lookback: int,
    horizon: int,
    signal_variant: str | None = None,
) -> str:
    return candidate_id(
        signal_family,
        int(lookback),
        int(horizon),
        signal_variant,
    )


def _signal_family_requires_equity_context(signal_family: str) -> bool:
    return signal_family in {
        "cross_sectional_relative_strength",
        "cross_sectional_momentum",
        "benchmark_relative_rotation",
        "regime_conditioned_momentum",
        "sector_relative_momentum",
        "volatility_dispersion_selection",
        "liquidity_flow_tilt",
    }


def _safe_series_corr(left: pd.Series, right: pd.Series) -> float:
    joined = pd.concat([left, right], axis=1).dropna()
    if len(joined) < 2:
        return float("nan")

    left_series = joined.iloc[:, 0]
    right_series = joined.iloc[:, 1]
    left_unique = left_series.nunique()
    right_unique = right_series.nunique()
    if left_unique == 1 and right_unique == 1:
        return 1.0 if left_series.iloc[0] == right_series.iloc[0] else float("nan")
    if left_unique == 1 or right_unique == 1:
        return float("nan")

    corr = left_series.corr(right_series)
    if pd.notna(corr):
        return float(corr)

    return float("nan")


def _compute_redundancy_diagnostics(
    leaderboard_df: pd.DataFrame,
    *,
    daily_metrics_by_candidate: dict[str, pd.DataFrame],
    score_panel_by_candidate: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    columns = [
        "candidate_id_a",
        "signal_family_a",
        "signal_variant_a",
        "lookback_a",
        "horizon_a",
        "candidate_id_b",
        "signal_family_b",
        "signal_variant_b",
        "lookback_b",
        "horizon_b",
        "overlap_dates",
        "overlap_scores",
        "performance_corr",
        "rank_ic_corr",
        "score_corr",
    ]
    promoted = leaderboard_df.loc[leaderboard_df["promotion_status"] == "promote"].copy()
    if len(promoted) < 2:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, float | int | str]] = []
    for _, left_row in promoted.iterrows():
        for _, right_row in promoted.iterrows():
            left_key = _candidate_key(
                str(left_row["signal_family"]),
                int(left_row["lookback"]),
                int(left_row["horizon"]),
                str(left_row.get("signal_variant") or "base"),
            )
            right_key = _candidate_key(
                str(right_row["signal_family"]),
                int(right_row["lookback"]),
                int(right_row["horizon"]),
                str(right_row.get("signal_variant") or "base"),
            )
            if left_key >= right_key:
                continue

            left_daily = daily_metrics_by_candidate.get(
                left_key,
                pd.DataFrame(columns=["timestamp", "long_short_spread", "spearman_ic"]),
            ).rename(
                columns={
                    "long_short_spread": "long_short_spread_a",
                    "spearman_ic": "spearman_ic_a",
                }
            )
            right_daily = daily_metrics_by_candidate.get(
                right_key,
                pd.DataFrame(columns=["timestamp", "long_short_spread", "spearman_ic"]),
            ).rename(
                columns={
                    "long_short_spread": "long_short_spread_b",
                    "spearman_ic": "spearman_ic_b",
                }
            )
            daily_overlap = left_daily.merge(right_daily, on="timestamp", how="inner")

            left_scores = score_panel_by_candidate.get(
                left_key,
                pd.DataFrame(columns=["timestamp", "symbol", "signal"]),
            ).rename(columns={"signal": "signal_a"})
            right_scores = score_panel_by_candidate.get(
                right_key,
                pd.DataFrame(columns=["timestamp", "symbol", "signal"]),
            ).rename(columns={"signal": "signal_b"})
            score_overlap = left_scores.merge(
                right_scores,
                on=["timestamp", "symbol"],
                how="inner",
            )

            performance_corr = _safe_series_corr(
                daily_overlap["long_short_spread_a"],
                daily_overlap["long_short_spread_b"],
            )
            rank_ic_corr = _safe_series_corr(
                daily_overlap["spearman_ic_a"],
                daily_overlap["spearman_ic_b"],
            )
            score_corr = _safe_series_corr(score_overlap["signal_a"], score_overlap["signal_b"])

            rows.append(
                {
                    "candidate_id_a": left_key,
                    "signal_family_a": str(left_row["signal_family"]),
                    "signal_variant_a": str(left_row.get("signal_variant") or "base"),
                    "lookback_a": int(left_row["lookback"]),
                    "horizon_a": int(left_row["horizon"]),
                    "candidate_id_b": right_key,
                    "signal_family_b": str(right_row["signal_family"]),
                    "signal_variant_b": str(right_row.get("signal_variant") or "base"),
                    "lookback_b": int(right_row["lookback"]),
                    "horizon_b": int(right_row["horizon"]),
                    "overlap_dates": int(len(daily_overlap)),
                    "overlap_scores": int(len(score_overlap)),
                    "performance_corr": performance_corr,
                    "rank_ic_corr": rank_ic_corr,
                    "score_corr": score_corr,
                }
            )

    return pd.DataFrame(rows, columns=columns)


def run_alpha_research(
    *,
    symbols: list[str] | None,
    universe: str | None,
    feature_dir: Path,
    signal_family: str,
    lookbacks: list[int],
    horizons: list[int],
    min_rows: int,
    top_quantile: float,
    bottom_quantile: float,
    candidate_grid_preset: str = "standard",
    signal_composition_preset: str = "standard",
    max_variants_per_family: int | None = None,
    output_dir: Path,
    train_size: int = 252 * 3,
    test_size: int = 63,
    step_size: int | None = None,
    min_train_size: int | None = None,
    portfolio_top_n: int = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.top_n,
    portfolio_long_quantile: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.long_quantile,
    portfolio_short_quantile: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.short_quantile,
    commission: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.commission,
    min_price: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.min_price,
    min_volume: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.min_volume,
    min_avg_dollar_volume: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.min_avg_dollar_volume,
    max_adv_participation: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.max_adv_participation,
    max_position_pct_of_adv: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.max_position_pct_of_adv,
    max_notional_per_name: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.max_notional_per_name,
    slippage_bps_per_turnover: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.slippage_bps_per_turnover,
    slippage_bps_per_adv: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.slippage_bps_per_adv,
    dynamic_recent_quality_window: int = DEFAULT_SIGNAL_LIFECYCLE_CONFIG.recent_quality_window,
    dynamic_min_history: int = DEFAULT_SIGNAL_LIFECYCLE_CONFIG.min_history,
    dynamic_downweight_mean_rank_ic: float = DEFAULT_SIGNAL_LIFECYCLE_CONFIG.downweight_mean_rank_ic,
    dynamic_deactivate_mean_rank_ic: float = DEFAULT_SIGNAL_LIFECYCLE_CONFIG.deactivate_mean_rank_ic,
    regime_aware_enabled: bool = DEFAULT_REGIME_CONFIG.enabled,
    regime_min_history: int = DEFAULT_REGIME_CONFIG.min_history,
    regime_underweight_mean_rank_ic: float = DEFAULT_REGIME_CONFIG.underweight_mean_rank_ic,
    regime_exclude_mean_rank_ic: float = DEFAULT_REGIME_CONFIG.exclude_mean_rank_ic,
    equity_context_enabled: bool = False,
    equity_context_include_volume: bool = False,
    fundamentals_enabled: bool = False,
    fundamentals_daily_features_path: Path | None = None,
    enable_context_confirmations: bool | None = None,
    enable_relative_features: bool | None = None,
    enable_flow_confirmations: bool | None = None,
    ensemble_enabled: bool = False,
    ensemble_mode: str = "disabled",
    ensemble_weight_method: str = "equal",
    ensemble_normalize_scores: str = "rank_pct",
    ensemble_max_members: int = 5,
    ensemble_require_promoted_only: bool = True,
    ensemble_max_members_per_family: int | None = None,
    ensemble_minimum_member_observations: int = 0,
    ensemble_minimum_member_metric: float | None = None,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    resolved_symbols = _resolve_symbols(symbols, universe)
    symbol_data: dict[str, pd.DataFrame] = {}

    for symbol in resolved_symbols:
        try:
            df = _load_symbol_feature_data(feature_dir, symbol)
        except FileNotFoundError:
            continue

        if len(df) < min_rows:
            continue

        if "timestamp" not in df.columns:
            raise ValueError(f"{symbol} feature data must include a 'timestamp' column.")

        symbol_data[symbol] = add_forward_return_labels(df, horizons=horizons)

    daily_fundamental_features_df = _load_daily_fundamental_features(
        fundamentals_daily_features_path,
        symbols=resolved_symbols,
    )
    symbol_data, fundamentals_summary = _merge_daily_fundamental_features(
        symbol_data,
        daily_features_df=daily_fundamental_features_df if fundamentals_enabled else pd.DataFrame(),
    )

    requires_rich_signal_context = (
        str(signal_composition_preset or "standard").strip().lower() != "standard"
        or bool(enable_context_confirmations)
        or bool(enable_relative_features)
        or bool(enable_flow_confirmations)
    )
    if equity_context_enabled or _signal_family_requires_equity_context(signal_family) or requires_rich_signal_context:
        symbol_data = _add_equity_context_features(
            symbol_data,
            lookbacks=lookbacks,
            include_volume=equity_context_include_volume,
        )
    context_artifact_dir = feature_dir.parent / "metadata"
    symbol_data, context_coverage_summary = enrich_symbol_data_with_explicit_context(
        symbol_data,
        lookbacks=lookbacks,
        context_artifact_dir=context_artifact_dir if context_artifact_dir.exists() else None,
    )
    context_artifact_paths = write_context_feature_panels(
        symbol_data,
        output_dir=output_dir,
        coverage_summary=context_coverage_summary,
    )

    folds = _build_shared_folds(
        symbol_data,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
    )

    candidate_specs = build_candidate_grid(
        signal_family=signal_family,
        lookbacks=lookbacks,
        horizons=horizons,
        candidate_grid_preset=candidate_grid_preset,
        max_variants_per_family=max_variants_per_family,
    )

    signal_cache: dict[tuple[str, str], pd.Series] = {}
    detailed_rows: list[dict] = []
    daily_metrics_by_candidate: dict[str, list[pd.DataFrame]] = {}
    score_panel_by_candidate: dict[str, list[pd.DataFrame]] = {}
    label_panel_by_horizon: dict[int, list[pd.DataFrame]] = {}
    candidate_panel_by_candidate: dict[str, list[pd.DataFrame]] = {}

    for candidate_spec in candidate_specs:
        current_candidate_id = _candidate_key(
            candidate_spec.signal_family,
            candidate_spec.lookback,
            candidate_spec.horizon,
            candidate_spec.signal_variant,
        )
        current_candidate_name = build_candidate_name(
            candidate_spec.signal_family,
            signal_variant=candidate_spec.signal_variant,
            lookback=candidate_spec.lookback,
            horizon=candidate_spec.horizon,
        )
        for symbol, df in symbol_data.items():
            signal_cache[(symbol, current_candidate_id)] = build_signal(
                df,
                signal_family=candidate_spec.signal_family,
                lookback=candidate_spec.lookback,
                signal_variant=candidate_spec.signal_variant,
                variant_params=candidate_spec.variant_params,
                signal_composition_preset=signal_composition_preset,
                enable_context_confirmations=enable_context_confirmations,
                enable_relative_features=enable_relative_features,
                enable_flow_confirmations=enable_flow_confirmations,
            )
        label_col = f"fwd_return_{candidate_spec.horizon}d"

        for fold in folds:
            fold_frames: list[pd.DataFrame] = []

            for symbol, df in symbol_data.items():
                test_df = _slice_fold(
                    df.assign(_signal=signal_cache[(symbol, current_candidate_id)]),
                    test_start=fold.test_start,
                    test_end=fold.test_end,
                )

                if test_df.empty:
                    continue

                fold_frames.append(
                    test_df[["timestamp", "symbol", "_signal", label_col]].rename(
                        columns={"_signal": "signal", label_col: "forward_return"}
                    )
                )

            if not fold_frames:
                continue

            fold_panel = pd.concat(fold_frames, ignore_index=True)
            metrics = evaluate_cross_sectional_signal(
                fold_panel,
                top_quantile=top_quantile,
                bottom_quantile=bottom_quantile,
            )
            daily_metrics = compute_cross_sectional_daily_metrics(
                fold_panel,
                top_quantile=top_quantile,
                bottom_quantile=bottom_quantile,
            )

            if not daily_metrics.empty:
                daily_metrics_by_candidate.setdefault(current_candidate_id, []).append(daily_metrics)
            candidate_panel_by_candidate.setdefault(current_candidate_id, []).append(fold_panel.copy())

            score_panel = fold_panel[["timestamp", "symbol", "signal"]].dropna().copy()
            if not score_panel.empty:
                score_panel_by_candidate.setdefault(current_candidate_id, []).append(score_panel)
            label_panel = fold_panel[["timestamp", "symbol", "forward_return"]].dropna().copy()
            if not label_panel.empty:
                label_panel_by_horizon.setdefault(candidate_spec.horizon, []).append(label_panel)

            detailed_rows.append(
                {
                    "candidate_id": current_candidate_id,
                    "candidate_name": current_candidate_name,
                    "signal_family": candidate_spec.signal_family,
                    "signal_variant": candidate_spec.signal_variant,
                    "variant_parameters_json": json.dumps(candidate_spec.variant_params, sort_keys=True),
                    "lookback": candidate_spec.lookback,
                    "horizon": candidate_spec.horizon,
                    "fold_id": fold.fold_id,
                    "train_start": fold.train_start,
                    "train_end": fold.train_end,
                    "test_start": fold.test_start,
                    "test_end": fold.test_end,
                    **metrics,
                }
            )

    detailed_columns = [
        "candidate_id",
        "candidate_name",
        "signal_family",
        "signal_variant",
        "variant_parameters_json",
        "lookback",
        "horizon",
        "fold_id",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "dates_evaluated",
        "symbols_evaluated",
        "n_obs",
        "pearson_ic",
        "spearman_ic",
        "hit_rate",
        "long_short_spread",
        "quantile_spread",
        "turnover",
    ]
    detailed_df = pd.DataFrame(detailed_rows, columns=detailed_columns)

    if detailed_df.empty:
        leaderboard_df = pd.DataFrame(
            columns=[
                "candidate_id",
                "candidate_name",
                "signal_family",
                "signal_variant",
                "variant_parameters_json",
                "lookback",
                "horizon",
                "symbols_tested",
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
                "rejection_reason",
                "promotion_status",
            ]
        )
    else:
        leaderboard_df = (
            detailed_df.groupby(
                ["candidate_id", "candidate_name", "signal_family", "signal_variant", "variant_parameters_json", "lookback", "horizon"],
                as_index=False,
            )
            .agg(
                symbols_tested=("symbols_evaluated", "max"),
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
            .sort_values(
                ["mean_spearman_ic", "mean_quantile_spread"],
                ascending=[False, False],
            )
            .reset_index(drop=True)
        )
        leaderboard_df = apply_promotion_rules(leaderboard_df)

    combined_daily_metrics = {
        key: pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
        for key, frames in daily_metrics_by_candidate.items()
        if frames
    }
    combined_score_panels = {
        key: pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp", "symbol"])
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
        for key, frames in score_panel_by_candidate.items()
        if frames
    }
    combined_label_panels = {
        horizon: pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp", "symbol"])
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
        for horizon, frames in label_panel_by_horizon.items()
        if frames
    }
    combined_candidate_panels = {
        key: pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp", "symbol"])
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
        for key, frames in candidate_panel_by_candidate.items()
        if frames
    }
    asset_returns_matrix = build_asset_return_matrix(symbol_data)
    regime_config = RegimeConfig(
        enabled=regime_aware_enabled,
        min_history=regime_min_history,
        underweight_mean_rank_ic=regime_underweight_mean_rank_ic,
        exclude_mean_rank_ic=regime_exclude_mean_rank_ic,
    )
    regime_labels_df = build_regime_labels_by_date(
        asset_returns_matrix,
        config=regime_config,
    )
    sub_universe_membership_df = build_sub_universe_membership_by_symbol_date(symbol_data)
    benchmark_context_by_lookback = build_benchmark_context_by_symbol_date(
        symbol_data,
        lookbacks=lookbacks,
    )
    redundancy_df = _compute_redundancy_diagnostics(
        leaderboard_df,
        daily_metrics_by_candidate=combined_daily_metrics,
        score_panel_by_candidate=combined_score_panels,
    )
    promoted_signals_df = leaderboard_df.loc[
        leaderboard_df["promotion_status"] == "promote"
    ].reset_index(drop=True)
    signal_family_summary_df = _build_signal_family_summary(leaderboard_df)
    fundamental_feature_ic_summary_df = _build_fundamental_feature_ic_summary(
        symbol_data,
        horizons=horizons,
        top_quantile=top_quantile,
        bottom_quantile=bottom_quantile,
    ) if fundamentals_enabled else pd.DataFrame()
    ensemble_config = EnsembleConfig(
        enabled=ensemble_enabled,
        mode=ensemble_mode if ensemble_enabled else "disabled",
        weight_method=ensemble_weight_method,
        normalize_scores=ensemble_normalize_scores,
        max_members=ensemble_max_members,
        require_promoted_only=ensemble_require_promoted_only,
        max_members_per_family=ensemble_max_members_per_family,
        minimum_member_observations=ensemble_minimum_member_observations,
        minimum_member_metric=ensemble_minimum_member_metric,
    )
    ensemble_member_summary_df = pd.DataFrame(
        columns=[
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
    )
    ensemble_signal_snapshot_df = pd.DataFrame(
        columns=[
            "timestamp",
            "symbol",
            "ensemble_score",
            "member_count",
            "contributing_families",
            "contributing_candidates",
            "top_contributing_member",
            "top_contributing_family",
            "horizon",
            "ensemble_mode",
            "weight_method",
            "normalize_scores",
        ]
    )
    ensemble_research_summary: dict[str, object] = {
        "enabled": ensemble_config.enabled,
        "mode": ensemble_config.mode,
        "weight_method": ensemble_config.weight_method,
        "normalize_scores": ensemble_config.normalize_scores,
        "eligible_member_count": 0,
        "included_member_count": 0,
        "ensemble_horizon_count": 0,
    }
    if ensemble_config.enabled and not leaderboard_df.empty:
        ensemble_members = leaderboard_df.copy()
        ensemble_members["candidate_id"] = ensemble_members.apply(
            lambda row: candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
            ),
            axis=1,
        )
        ensemble_member_summary_df = select_ensemble_members(ensemble_members, ensemble_config)
        ensemble_member_summary_df = assign_member_weights(ensemble_member_summary_df, ensemble_config)
        ensemble_frames: list[pd.DataFrame] = []
        for horizon in sorted(ensemble_member_summary_df["horizon"].dropna().unique().tolist()):
            horizon_members = ensemble_member_summary_df.loc[
                ensemble_member_summary_df["horizon"] == horizon
            ].copy()
            signal_frames = {
                str(row["member_id"]): combined_score_panels.get(
                    _candidate_key(
                        str(row["signal_family"]),
                        int(row["lookback"]),
                        int(row["horizon"]),
                        str(row.get("signal_variant") or "base"),
                    ),
                    pd.DataFrame(columns=["timestamp", "symbol", "signal"]),
                )
                for _, row in horizon_members.iterrows()
            }
            ensemble_frame = build_ensemble_scores(signal_frames, ensemble_config, horizon_members)
            if ensemble_frame.empty:
                continue
            ensemble_frame["horizon"] = int(horizon)
            ensemble_frame["ensemble_mode"] = ensemble_config.mode
            ensemble_frame["weight_method"] = ensemble_config.weight_method
            ensemble_frame["normalize_scores"] = ensemble_config.normalize_scores
            ensemble_frames.append(ensemble_frame)
        if ensemble_frames:
            ensemble_signal_snapshot_df = pd.concat(ensemble_frames, ignore_index=True)
        ensemble_research_summary.update(
            {
                "eligible_member_count": int(len(ensemble_member_summary_df)),
                "included_member_count": int(ensemble_member_summary_df["included_in_ensemble"].sum()),
                "ensemble_horizon_count": int(ensemble_signal_snapshot_df["horizon"].nunique()) if not ensemble_signal_snapshot_df.empty else 0,
            }
        )
    lifecycle_config = SignalLifecycleConfig(
        recent_quality_window=dynamic_recent_quality_window,
        min_history=dynamic_min_history,
        downweight_mean_rank_ic=dynamic_downweight_mean_rank_ic,
        deactivate_mean_rank_ic=dynamic_deactivate_mean_rank_ic,
    )
    composite_score_frames: list[pd.DataFrame] = []
    dynamic_weight_frames: list[pd.DataFrame] = []
    active_signal_frames: list[pd.DataFrame] = []
    deactivated_signal_frames: list[pd.DataFrame] = []
    lifecycle_report_frames: list[pd.DataFrame] = []
    signal_performance_by_regime_frames: list[pd.DataFrame] = []
    signal_performance_by_sub_universe_frames: list[pd.DataFrame] = []
    signal_performance_by_benchmark_context_frames: list[pd.DataFrame] = []
    regime_aware_weight_frames: list[pd.DataFrame] = []
    regime_selection_report_frames: list[pd.DataFrame] = []
    composite_diagnostics: dict[str, object] = {
        "config": DEFAULT_COMPOSITE_CONFIG.to_dict(),
        "signal_lifecycle": lifecycle_config.to_dict(),
        "regime": regime_config.to_dict(),
        "horizons": {},
    }
    for horizon in sorted(promoted_signals_df["horizon"].unique().tolist()) if not promoted_signals_df.empty else []:
        selected_signals_df, excluded_rows = select_low_redundancy_signals(
            promoted_signals_df,
            redundancy_df,
            horizon=int(horizon),
            redundancy_corr_threshold=DEFAULT_COMPOSITE_CONFIG.redundancy_corr_threshold,
        )
        composite_diagnostics["horizons"][str(int(horizon))] = {
            "selected_signals": selected_signals_df[
                ["candidate_id", "signal_family", "signal_variant", "lookback", "horizon"]
            ].to_dict(orient="records"),
            "excluded_signals": excluded_rows,
        }
        dynamic_weights_df, active_signals_df, deactivated_signals_df, lifecycle_report_df = build_dynamic_signal_weights(
            selected_signals_df,
            daily_metrics_by_candidate=combined_daily_metrics,
            horizon=int(horizon),
            config=lifecycle_config,
        )
        if not dynamic_weights_df.empty:
            dynamic_weight_frames.append(dynamic_weights_df)
        if not active_signals_df.empty:
            active_signal_frames.append(active_signals_df)
        if not deactivated_signals_df.empty:
            deactivated_signal_frames.append(deactivated_signals_df)
        if not lifecycle_report_df.empty:
            lifecycle_report_frames.append(lifecycle_report_df)
        signal_performance_by_regime_df = compute_signal_performance_by_regime(
            selected_signals_df,
            daily_metrics_by_candidate=combined_daily_metrics,
            regime_labels_df=regime_labels_df,
            horizon=int(horizon),
        )
        if not signal_performance_by_regime_df.empty:
            signal_performance_by_regime_frames.append(signal_performance_by_regime_df)
        signal_performance_by_sub_universe_df = compute_signal_performance_by_sub_universe(
            selected_signals_df,
            candidate_panels_by_candidate=combined_candidate_panels,
            sub_universe_membership_df=sub_universe_membership_df,
            horizon=int(horizon),
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        )
        if not signal_performance_by_sub_universe_df.empty:
            signal_performance_by_sub_universe_frames.append(signal_performance_by_sub_universe_df)
        signal_performance_by_benchmark_context_df = compute_signal_performance_by_benchmark_context(
            selected_signals_df,
            candidate_panels_by_candidate=combined_candidate_panels,
            benchmark_context_by_lookback=benchmark_context_by_lookback,
            horizon=int(horizon),
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        )
        if not signal_performance_by_benchmark_context_df.empty:
            signal_performance_by_benchmark_context_frames.append(signal_performance_by_benchmark_context_df)
        regime_aware_weights_df, regime_selection_report_df = build_regime_aware_signal_weights(
            dynamic_weights_df,
            daily_metrics_by_candidate=combined_daily_metrics,
            regime_labels_df=regime_labels_df,
            horizon=int(horizon),
            config=regime_config,
        )
        if not regime_aware_weights_df.empty:
            regime_aware_weight_frames.append(regime_aware_weights_df)
        if not regime_selection_report_df.empty:
            regime_selection_report_frames.append(regime_selection_report_df)
        composite_diagnostics["horizons"][str(int(horizon))]["dynamic_weighting"] = {
            "active_dates": int(active_signals_df["timestamp"].nunique()) if not active_signals_df.empty else 0,
            "deactivated_dates": int(deactivated_signals_df["timestamp"].nunique()) if not deactivated_signals_df.empty else 0,
            "weighting_schemes": lifecycle_config.weighting_schemes,
        }
        for weighting_scheme in lifecycle_config.weighting_schemes:
            composite_scores = build_composite_scores(
                selected_signals_df,
                score_panel_by_candidate=combined_score_panels,
                weighting_scheme=weighting_scheme,
                quality_metric=DEFAULT_COMPOSITE_CONFIG.quality_metric,
                dynamic_signal_weights_df=dynamic_weights_df.loc[
                    dynamic_weights_df["weighting_scheme"] == weighting_scheme
                ].copy(),
            )
            if composite_scores.empty:
                continue
            composite_score_frames.append(composite_scores)
        if regime_config.enabled:
            composite_scores = build_composite_scores(
                selected_signals_df,
                score_panel_by_candidate=combined_score_panels,
                weighting_scheme="regime_aware",
                quality_metric=DEFAULT_COMPOSITE_CONFIG.quality_metric,
                dynamic_signal_weights_df=regime_aware_weights_df,
            )
            if not composite_scores.empty:
                composite_score_frames.append(composite_scores)

    dynamic_signal_weights_df = (
        pd.concat(dynamic_weight_frames, ignore_index=True)
        if dynamic_weight_frames
        else pd.DataFrame(
            columns=[
                "timestamp",
                "candidate_id",
                "signal_family",
                "signal_variant",
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
        )
    )
    active_signals_by_date_df = (
        pd.concat(active_signal_frames, ignore_index=True)
        if active_signal_frames
        else dynamic_signal_weights_df.iloc[0:0].copy()
    )
    deactivated_signals_df = (
        pd.concat(deactivated_signal_frames, ignore_index=True)
        if deactivated_signal_frames
        else dynamic_signal_weights_df.iloc[0:0].copy()
    )
    signal_lifecycle_report_df = (
        pd.concat(lifecycle_report_frames, ignore_index=True)
        if lifecycle_report_frames
        else pd.DataFrame(
            columns=[
                "report_type",
                "horizon",
                "weighting_scheme",
                "candidate_id",
                "signal_family",
                "signal_variant",
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
        )
    )
    signal_performance_by_regime_df = (
        pd.concat(signal_performance_by_regime_frames, ignore_index=True)
        if signal_performance_by_regime_frames
        else pd.DataFrame(
            columns=[
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
        )
    )
    signal_performance_by_sub_universe_df = (
        pd.concat(signal_performance_by_sub_universe_frames, ignore_index=True)
        if signal_performance_by_sub_universe_frames
        else pd.DataFrame(
            columns=[
                "candidate_id",
                "signal_family",
                "signal_variant",
                "lookback",
                "horizon",
                "sub_universe_id",
                "dates_evaluated",
                "sample_size",
                "coverage_ratio",
                "mean_spearman_ic",
                "mean_long_short_spread",
                "context_source",
                "context_status",
            ]
        )
    )
    signal_performance_by_benchmark_context_df = (
        pd.concat(signal_performance_by_benchmark_context_frames, ignore_index=True)
        if signal_performance_by_benchmark_context_frames
        else pd.DataFrame(
            columns=[
                "candidate_id",
                "signal_family",
                "signal_variant",
                "lookback",
                "horizon",
                "benchmark_context_label",
                "dates_evaluated",
                "sample_size",
                "coverage_ratio",
                "mean_spearman_ic",
                "mean_long_short_spread",
                "mean_relative_return",
                "mean_market_return",
                "context_source",
                "context_status",
            ]
        )
    )
    regime_aware_signal_weights_df = (
        pd.concat(regime_aware_weight_frames, ignore_index=True)
        if regime_aware_weight_frames
        else pd.DataFrame(
            columns=[
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
        )
    )
    regime_selection_report_df = (
        pd.concat(regime_selection_report_frames, ignore_index=True)
        if regime_selection_report_frames
        else pd.DataFrame(
            columns=[
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
        )
    )
    composite_scores_df = (
        pd.concat(composite_score_frames, ignore_index=True)
        if composite_score_frames
        else pd.DataFrame(
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
    )
    composite_leaderboard_df = evaluate_composite_scores(
        composite_scores_df,
        label_panel_by_horizon=combined_label_panels,
        folds=folds,
        top_quantile=top_quantile,
        bottom_quantile=bottom_quantile,
    )
    ensemble_leaderboard_df = pd.DataFrame()
    if not ensemble_signal_snapshot_df.empty:
        ensemble_eval_input = ensemble_signal_snapshot_df.rename(
            columns={
                "ensemble_score": "composite_score",
                "member_count": "component_count",
            }
        ).copy()
        ensemble_eval_input["weighting_scheme"] = (
            "ensemble_" + ensemble_config.mode + "_" + ensemble_config.weight_method
        )
        ensemble_eval_input["selected_signal_count"] = ensemble_eval_input["component_count"]
        ensemble_leaderboard_df = evaluate_composite_scores(
            ensemble_eval_input[
                [
                    "timestamp",
                    "symbol",
                    "horizon",
                    "weighting_scheme",
                    "composite_score",
                    "component_count",
                    "selected_signal_count",
                ]
            ],
            label_panel_by_horizon=combined_label_panels,
            folds=folds,
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        )
        if not ensemble_leaderboard_df.empty:
            top_row = ensemble_leaderboard_df.iloc[0].to_dict()
            ensemble_research_summary["top_ensemble_metric"] = top_row
    portfolio_config = CompositePortfolioConfig(
        top_n=portfolio_top_n,
        long_quantile=portfolio_long_quantile,
        short_quantile=portfolio_short_quantile,
        commission=commission,
        min_price=min_price,
        min_volume=min_volume,
        min_avg_dollar_volume=min_avg_dollar_volume,
        max_adv_participation=max_adv_participation,
        max_position_pct_of_adv=max_position_pct_of_adv,
        max_notional_per_name=max_notional_per_name,
        slippage_bps_per_turnover=slippage_bps_per_turnover,
        slippage_bps_per_adv=slippage_bps_per_adv,
    )
    (
        composite_portfolio_returns_df,
        composite_portfolio_metrics_df,
        composite_portfolio_weights_df,
        composite_portfolio_diagnostics,
    ) = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=portfolio_config,
    )
    asset_returns_matrix = (
        composite_portfolio_diagnostics.get("asset_returns_matrix")
        if isinstance(composite_portfolio_diagnostics, dict)
        else None
    )
    robustness_report_df = build_robustness_report(
        composite_portfolio_returns_df,
        composite_portfolio_weights_df,
        folds=folds,
    )
    regime_performance_df = build_regime_performance_report(
        composite_portfolio_returns_df,
        asset_returns=asset_returns_matrix if isinstance(asset_returns_matrix, pd.DataFrame) else pd.DataFrame(),
    )
    stress_test_results_df = run_stress_tests(
        composite_scores_df,
        symbol_data=symbol_data,
        config=portfolio_config,
    )
    implementability_report_df = composite_portfolio_diagnostics.get(
        "implementability_report",
        pd.DataFrame(),
    )
    liquidity_filtered_portfolio_metrics_df = composite_portfolio_diagnostics.get(
        "liquidity_filtered_portfolio_metrics",
        pd.DataFrame(),
    )
    capacity_scenarios_df = composite_portfolio_diagnostics.get(
        "capacity_scenarios",
        pd.DataFrame(),
    )

    detailed_path_csv = output_dir / "fold_results.csv"
    leaderboard_path_csv = output_dir / "leaderboard.csv"
    detailed_path_parquet = output_dir / "fold_results.parquet"
    leaderboard_path_parquet = output_dir / "leaderboard.parquet"
    promoted_signals_path_csv = output_dir / "promoted_signals.csv"
    promoted_signals_path_parquet = output_dir / "promoted_signals.parquet"
    signal_family_summary_path_csv = output_dir / "signal_family_summary.csv"
    signal_family_summary_path_parquet = output_dir / "signal_family_summary.parquet"
    fundamental_feature_ic_summary_path_csv = output_dir / "fundamental_feature_ic_summary.csv"
    fundamental_feature_ic_summary_path_parquet = output_dir / "fundamental_feature_ic_summary.parquet"
    redundancy_report_path_csv = output_dir / "redundancy_report.csv"
    redundancy_report_path_parquet = output_dir / "redundancy_report.parquet"
    redundancy_path_csv = output_dir / "redundancy_diagnostics.csv"
    redundancy_path_parquet = output_dir / "redundancy_diagnostics.parquet"
    composite_scores_path_csv = output_dir / "composite_scores.csv"
    composite_scores_path_parquet = output_dir / "composite_scores.parquet"
    composite_leaderboard_path_csv = output_dir / "composite_leaderboard.csv"
    composite_leaderboard_path_parquet = output_dir / "composite_leaderboard.parquet"
    ensemble_member_summary_path_csv = output_dir / "ensemble_member_summary.csv"
    ensemble_member_summary_path_parquet = output_dir / "ensemble_member_summary.parquet"
    ensemble_signal_snapshot_path_csv = output_dir / "ensemble_signal_snapshot.csv"
    ensemble_signal_snapshot_path_parquet = output_dir / "ensemble_signal_snapshot.parquet"
    ensemble_research_summary_path = output_dir / "ensemble_research_summary.json"
    dynamic_signal_weights_path_csv = output_dir / "dynamic_signal_weights.csv"
    dynamic_signal_weights_path_parquet = output_dir / "dynamic_signal_weights.parquet"
    active_signals_by_date_path_csv = output_dir / "active_signals_by_date.csv"
    active_signals_by_date_path_parquet = output_dir / "active_signals_by_date.parquet"
    deactivated_signals_path_csv = output_dir / "deactivated_signals.csv"
    deactivated_signals_path_parquet = output_dir / "deactivated_signals.parquet"
    signal_lifecycle_report_path_csv = output_dir / "signal_lifecycle_report.csv"
    signal_lifecycle_report_path_parquet = output_dir / "signal_lifecycle_report.parquet"
    regime_labels_by_date_path_csv = output_dir / "regime_labels_by_date.csv"
    regime_labels_by_date_path_parquet = output_dir / "regime_labels_by_date.parquet"
    signal_performance_by_regime_path_csv = output_dir / "signal_performance_by_regime.csv"
    signal_performance_by_regime_path_parquet = output_dir / "signal_performance_by_regime.parquet"
    signal_performance_by_sub_universe_path_csv = output_dir / "signal_performance_by_sub_universe.csv"
    signal_performance_by_sub_universe_path_parquet = output_dir / "signal_performance_by_sub_universe.parquet"
    signal_performance_by_benchmark_context_path_csv = output_dir / "signal_performance_by_benchmark_context.csv"
    signal_performance_by_benchmark_context_path_parquet = output_dir / "signal_performance_by_benchmark_context.parquet"
    regime_aware_signal_weights_path_csv = output_dir / "regime_aware_signal_weights.csv"
    regime_aware_signal_weights_path_parquet = output_dir / "regime_aware_signal_weights.parquet"
    regime_selection_report_path_csv = output_dir / "regime_selection_report.csv"
    regime_selection_report_path_parquet = output_dir / "regime_selection_report.parquet"
    composite_diagnostics_path = output_dir / "composite_diagnostics.json"
    portfolio_returns_path_csv = output_dir / "portfolio_returns.csv"
    portfolio_returns_path_parquet = output_dir / "portfolio_returns.parquet"
    portfolio_metrics_path_csv = output_dir / "portfolio_metrics.csv"
    portfolio_metrics_path_parquet = output_dir / "portfolio_metrics.parquet"
    portfolio_weights_path_csv = output_dir / "portfolio_weights.csv"
    portfolio_weights_path_parquet = output_dir / "portfolio_weights.parquet"
    portfolio_diagnostics_path = output_dir / "portfolio_diagnostics.json"
    robustness_report_path_csv = output_dir / "robustness_report.csv"
    robustness_report_path_parquet = output_dir / "robustness_report.parquet"
    regime_performance_path_csv = output_dir / "regime_performance.csv"
    regime_performance_path_parquet = output_dir / "regime_performance.parquet"
    stress_test_results_path_csv = output_dir / "stress_test_results.csv"
    stress_test_results_path_parquet = output_dir / "stress_test_results.parquet"
    implementability_report_path_csv = output_dir / "implementability_report.csv"
    implementability_report_path_parquet = output_dir / "implementability_report.parquet"
    liquidity_filtered_metrics_path_csv = output_dir / "liquidity_filtered_portfolio_metrics.csv"
    liquidity_filtered_metrics_path_parquet = output_dir / "liquidity_filtered_portfolio_metrics.parquet"
    capacity_scenarios_path_csv = output_dir / "capacity_scenarios.csv"
    capacity_scenarios_path_parquet = output_dir / "capacity_scenarios.parquet"
    diagnostics_path = output_dir / "signal_diagnostics.json"

    detailed_df.to_csv(detailed_path_csv, index=False)
    leaderboard_df.to_csv(leaderboard_path_csv, index=False)
    promoted_signals_df.to_csv(promoted_signals_path_csv, index=False)
    signal_family_summary_df.to_csv(signal_family_summary_path_csv, index=False)
    fundamental_feature_ic_summary_df.to_csv(fundamental_feature_ic_summary_path_csv, index=False)
    redundancy_df.to_csv(redundancy_report_path_csv, index=False)
    redundancy_df.to_csv(redundancy_path_csv, index=False)
    composite_scores_df.to_csv(composite_scores_path_csv, index=False)
    composite_leaderboard_df.to_csv(composite_leaderboard_path_csv, index=False)
    ensemble_member_summary_df.to_csv(ensemble_member_summary_path_csv, index=False)
    ensemble_signal_snapshot_df.to_csv(ensemble_signal_snapshot_path_csv, index=False)
    dynamic_signal_weights_df.to_csv(dynamic_signal_weights_path_csv, index=False)
    active_signals_by_date_df.to_csv(active_signals_by_date_path_csv, index=False)
    deactivated_signals_df.to_csv(deactivated_signals_path_csv, index=False)
    signal_lifecycle_report_df.to_csv(signal_lifecycle_report_path_csv, index=False)
    regime_labels_df.to_csv(regime_labels_by_date_path_csv, index=False)
    signal_performance_by_regime_df.to_csv(signal_performance_by_regime_path_csv, index=False)
    signal_performance_by_sub_universe_df.to_csv(signal_performance_by_sub_universe_path_csv, index=False)
    signal_performance_by_benchmark_context_df.to_csv(signal_performance_by_benchmark_context_path_csv, index=False)
    regime_aware_signal_weights_df.to_csv(regime_aware_signal_weights_path_csv, index=False)
    regime_selection_report_df.to_csv(regime_selection_report_path_csv, index=False)
    composite_portfolio_returns_df.to_csv(portfolio_returns_path_csv, index=False)
    composite_portfolio_metrics_df.to_csv(portfolio_metrics_path_csv, index=False)
    composite_portfolio_weights_df.to_csv(portfolio_weights_path_csv, index=False)
    robustness_report_df.to_csv(robustness_report_path_csv, index=False)
    regime_performance_df.to_csv(regime_performance_path_csv, index=False)
    stress_test_results_df.to_csv(stress_test_results_path_csv, index=False)
    implementability_report_df.to_csv(implementability_report_path_csv, index=False)
    liquidity_filtered_portfolio_metrics_df.to_csv(liquidity_filtered_metrics_path_csv, index=False)
    capacity_scenarios_df.to_csv(capacity_scenarios_path_csv, index=False)
    detailed_df.to_parquet(detailed_path_parquet, index=False)
    leaderboard_df.to_parquet(leaderboard_path_parquet, index=False)
    promoted_signals_df.to_parquet(promoted_signals_path_parquet, index=False)
    signal_family_summary_df.to_parquet(signal_family_summary_path_parquet, index=False)
    fundamental_feature_ic_summary_df.to_parquet(fundamental_feature_ic_summary_path_parquet, index=False)
    redundancy_df.to_parquet(redundancy_report_path_parquet, index=False)
    redundancy_df.to_parquet(redundancy_path_parquet, index=False)
    composite_scores_df.to_parquet(composite_scores_path_parquet, index=False)
    composite_leaderboard_df.to_parquet(composite_leaderboard_path_parquet, index=False)
    ensemble_member_summary_df.to_parquet(ensemble_member_summary_path_parquet, index=False)
    ensemble_signal_snapshot_df.to_parquet(ensemble_signal_snapshot_path_parquet, index=False)
    dynamic_signal_weights_df.to_parquet(dynamic_signal_weights_path_parquet, index=False)
    active_signals_by_date_df.to_parquet(active_signals_by_date_path_parquet, index=False)
    deactivated_signals_df.to_parquet(deactivated_signals_path_parquet, index=False)
    signal_lifecycle_report_df.to_parquet(signal_lifecycle_report_path_parquet, index=False)
    regime_labels_df.to_parquet(regime_labels_by_date_path_parquet, index=False)
    signal_performance_by_regime_df.to_parquet(signal_performance_by_regime_path_parquet, index=False)
    signal_performance_by_sub_universe_df.to_parquet(signal_performance_by_sub_universe_path_parquet, index=False)
    signal_performance_by_benchmark_context_df.to_parquet(signal_performance_by_benchmark_context_path_parquet, index=False)
    regime_aware_signal_weights_df.to_parquet(regime_aware_signal_weights_path_parquet, index=False)
    regime_selection_report_df.to_parquet(regime_selection_report_path_parquet, index=False)
    composite_portfolio_returns_df.to_parquet(portfolio_returns_path_parquet, index=False)
    composite_portfolio_metrics_df.to_parquet(portfolio_metrics_path_parquet, index=False)
    composite_portfolio_weights_df.to_parquet(portfolio_weights_path_parquet, index=False)
    robustness_report_df.to_parquet(robustness_report_path_parquet, index=False)
    regime_performance_df.to_parquet(regime_performance_path_parquet, index=False)
    stress_test_results_df.to_parquet(stress_test_results_path_parquet, index=False)
    implementability_report_df.to_parquet(implementability_report_path_parquet, index=False)
    liquidity_filtered_portfolio_metrics_df.to_parquet(liquidity_filtered_metrics_path_parquet, index=False)
    capacity_scenarios_df.to_parquet(capacity_scenarios_path_parquet, index=False)
    composite_diagnostics_path.write_text(json.dumps(composite_diagnostics, indent=2, default=str))
    ensemble_research_summary_path.write_text(
        json.dumps(ensemble_research_summary, indent=2, default=str),
        encoding="utf-8",
    )
    portfolio_diagnostics_payload: dict[str, object] = {}
    for key, value in composite_portfolio_diagnostics.items():
        if key == "asset_returns_matrix":
            continue
        if isinstance(value, pd.DataFrame):
            portfolio_diagnostics_payload[key] = value.to_dict(orient="records")
        else:
            portfolio_diagnostics_payload[key] = value
    portfolio_diagnostics_path.write_text(
        json.dumps(
            portfolio_diagnostics_payload,
            indent=2,
            default=str,
        )
    )

    diagnostics = {
        "symbols_requested": resolved_symbols,
        "signal_family": signal_family,
        "signal_family_requires_equity_context": _signal_family_requires_equity_context(signal_family),
        "lookbacks": lookbacks,
        "horizons": horizons,
        "candidate_grid_preset": candidate_grid_preset,
        "signal_composition_preset": signal_composition_preset,
        "max_variants_per_family": max_variants_per_family,
        "generated_candidate_count": len(candidate_specs),
        "generated_signal_variants": sorted({spec.signal_variant for spec in candidate_specs}),
        "signal_composition": {
            "preset": signal_composition_preset,
            "enable_context_confirmations": enable_context_confirmations,
            "enable_relative_features": enable_relative_features,
            "enable_flow_confirmations": enable_flow_confirmations,
        },
        "min_rows": min_rows,
        "feature_dir": str(feature_dir),
        "evaluation_mode": "cross_sectional_long_short",
        "train_size": train_size,
        "test_size": test_size,
        "step_size": step_size,
        "min_train_size": min_train_size,
        "promotion_rules": DEFAULT_PROMOTION_THRESHOLDS.to_dict(),
        "signal_lifecycle": lifecycle_config.to_dict(),
        "regime": regime_config.to_dict(),
        "composite_portfolio": portfolio_config.to_dict(),
        "equity_context": {
            "enabled": equity_context_enabled,
            "include_volume": equity_context_include_volume,
        },
        "fundamentals": {
            "enabled": fundamentals_enabled,
            "daily_features_path": str(fundamentals_daily_features_path) if fundamentals_daily_features_path is not None else None,
            **fundamentals_summary,
        },
        "research_context": context_coverage_summary,
        "signal_family_summary": signal_family_summary_df.to_dict(orient="records"),
        "ensemble": {
            "enabled": ensemble_config.enabled,
            "mode": ensemble_config.mode,
            "weight_method": ensemble_config.weight_method,
            "normalize_scores": ensemble_config.normalize_scores,
            "max_members": ensemble_config.max_members,
            "require_promoted_only": ensemble_config.require_promoted_only,
        },
    }
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, default=str))
    approved_model_state_paths = write_approved_model_state(artifact_dir=output_dir)
    result_paths = {
        "leaderboard_path": str(leaderboard_path_csv),
        "fold_results_path": str(detailed_path_csv),
        "promoted_signals_path": str(promoted_signals_path_csv),
        "signal_family_summary_path": str(signal_family_summary_path_csv),
        "fundamental_feature_ic_summary_path": str(fundamental_feature_ic_summary_path_csv),
        "redundancy_report_path": str(redundancy_report_path_csv),
        "redundancy_path": str(redundancy_path_csv),
        "composite_scores_path": str(composite_scores_path_csv),
        "composite_leaderboard_path": str(composite_leaderboard_path_csv),
        "ensemble_member_summary_path": str(ensemble_member_summary_path_csv),
        "ensemble_signal_snapshot_path": str(ensemble_signal_snapshot_path_csv),
        "ensemble_research_summary_path": str(ensemble_research_summary_path),
        "dynamic_signal_weights_path": str(dynamic_signal_weights_path_csv),
        "active_signals_by_date_path": str(active_signals_by_date_path_csv),
        "deactivated_signals_path": str(deactivated_signals_path_csv),
        "signal_lifecycle_report_path": str(signal_lifecycle_report_path_csv),
        "regime_labels_by_date_path": str(regime_labels_by_date_path_csv),
        "signal_performance_by_regime_path": str(signal_performance_by_regime_path_csv),
        "signal_performance_by_sub_universe_path": str(signal_performance_by_sub_universe_path_csv),
        "signal_performance_by_benchmark_context_path": str(signal_performance_by_benchmark_context_path_csv),
        "regime_aware_signal_weights_path": str(regime_aware_signal_weights_path_csv),
        "regime_selection_report_path": str(regime_selection_report_path_csv),
        "composite_diagnostics_path": str(composite_diagnostics_path),
        "portfolio_returns_path": str(portfolio_returns_path_csv),
        "portfolio_metrics_path": str(portfolio_metrics_path_csv),
        "portfolio_weights_path": str(portfolio_weights_path_csv),
        "portfolio_diagnostics_path": str(portfolio_diagnostics_path),
        "robustness_report_path": str(robustness_report_path_csv),
        "regime_performance_path": str(regime_performance_path_csv),
        "stress_test_results_path": str(stress_test_results_path_csv),
        "implementability_report_path": str(implementability_report_path_csv),
        "liquidity_filtered_portfolio_metrics_path": str(liquidity_filtered_metrics_path_csv),
        "capacity_scenarios_path": str(capacity_scenarios_path_csv),
        "signal_diagnostics_path": str(diagnostics_path),
        **context_artifact_paths,
        **approved_model_state_paths,
    }
    manifest_path = write_research_run_manifest(
        output_dir=output_dir,
        workflow_type="alpha_research",
        command="service:run_alpha_research",
        feature_dir=feature_dir,
        signal_family=signal_family,
        universe=universe,
        symbols_requested=resolved_symbols,
        lookbacks=lookbacks,
        horizons=horizons,
        min_rows=min_rows,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
        artifact_paths=result_paths,
    )
    result_paths["research_manifest_path"] = str(manifest_path)

    return result_paths
