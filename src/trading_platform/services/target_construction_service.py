from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.cli.common import normalize_paper_weighting_scheme
from trading_platform.construction.service import build_top_n_portfolio_weights
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.metadata.groups import build_group_series
from trading_platform.paper.composite import (
    build_composite_paper_snapshot,
    compute_latest_composite_target_weights,
)
from trading_platform.paper.models import PaperSignalSnapshot, PaperTradingConfig
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn
from trading_platform.signals.common import normalize_price_frame
from trading_platform.signals.loaders import load_feature_frame, resolve_feature_frame_path
from trading_platform.signals.registry import SIGNAL_REGISTRY


@dataclass(frozen=True)
class TargetConstructionResult:
    as_of: str
    scheduled_target_weights: dict[str, float]
    effective_target_weights: dict[str, float]
    latest_prices: dict[str, float]
    latest_scores: dict[str, float]
    target_diagnostics: dict[str, Any]
    skipped_symbols: list[str] = field(default_factory=list)
    signal_snapshot: PaperSignalSnapshot | None = None
    extra_diagnostics: dict[str, Any] = field(default_factory=dict)


def _load_xsec_prepared_frames(
    symbols: list[str],
) -> tuple[dict[str, dict[str, object]], list[str], dict[str, str]]:
    prepared_frames: dict[str, dict[str, object]] = {}
    skipped_symbols: list[str] = []
    skip_reasons: dict[str, str] = {}

    for symbol in symbols:
        try:
            prepared_frames[symbol] = {
                "df": load_feature_frame(symbol),
                "path": Path(resolve_feature_frame_path(symbol)),
            }
        except Exception as exc:
            skipped_symbols.append(symbol)
            skip_reasons[symbol] = repr(exc)

    if not prepared_frames:
        raise ValueError(
            f"No valid symbol frames available for xsec paper trading. Reasons: {skip_reasons}"
        )

    return prepared_frames, skipped_symbols, skip_reasons


def load_signal_snapshot(
    *,
    symbols: list[str],
    strategy: str,
    fast: int | None = None,
    slow: int | None = None,
    lookback: int | None = None,
) -> PaperSignalSnapshot:
    signal_fn = SIGNAL_REGISTRY[strategy]
    asset_return_frames: list[pd.Series] = []
    score_frames: list[pd.Series] = []
    close_frames: list[pd.Series] = []
    skipped_symbols: list[str] = []
    skip_reasons: dict[str, str] = {}

    for symbol in symbols:
        try:
            feature_df = load_feature_frame(symbol)
            signal_kwargs = {}
            if fast is not None:
                signal_kwargs["fast"] = fast
            if slow is not None:
                signal_kwargs["slow"] = slow
            if lookback is not None:
                signal_kwargs["lookback"] = lookback

            signal_df = signal_fn(feature_df, **signal_kwargs).copy()

            if "score" not in signal_df.columns:
                raise ValueError("Signal frame missing required column: score")
            if "asset_return" not in signal_df.columns:
                raise ValueError("Signal frame missing required column: asset_return")
            if "close" not in signal_df.columns:
                raise ValueError(
                    "Signal frame missing required column: close. "
                    "Paper trading requires a reference execution price."
                )

            if "timestamp" in signal_df.columns:
                signal_df["timestamp"] = pd.to_datetime(signal_df["timestamp"], errors="coerce")
                signal_df = signal_df.dropna(subset=["timestamp"]).sort_values("timestamp")
                signal_df = signal_df.set_index("timestamp")
            else:
                if not isinstance(signal_df.index, pd.DatetimeIndex):
                    raise ValueError(
                        "Signal frame must have a 'timestamp' column or DatetimeIndex"
                    )
                signal_df = signal_df.sort_index()

            asset_return_frames.append(signal_df["asset_return"].rename(symbol))
            score_frames.append(signal_df["score"].rename(symbol))
            close_frames.append(signal_df["close"].rename(symbol))
        except Exception as exc:
            skipped_symbols.append(symbol)
            skip_reasons[symbol] = repr(exc)

    if not asset_return_frames or not score_frames or not close_frames:
        raise ValueError(
            f"No valid symbol frames available for paper trading. Reasons: {skip_reasons}"
        )

    asset_returns = pd.concat(asset_return_frames, axis=1).sort_index().fillna(0.0)
    scores = pd.concat(score_frames, axis=1).sort_index()
    closes = pd.concat(close_frames, axis=1).sort_index().ffill()

    return PaperSignalSnapshot(
        asset_returns=asset_returns,
        scores=scores,
        closes=closes,
        skipped_symbols=skipped_symbols,
    )


def compute_latest_target_weights(
    *,
    config: PaperTradingConfig,
    snapshot: PaperSignalSnapshot,
) -> tuple[str, dict[str, float], dict[str, float], dict[str, Any]]:
    symbol_groups = build_group_series(
        config.symbols,
        path=config.group_map_path,
    )
    selection, raw_target_weights = build_top_n_portfolio_weights(
        scores=snapshot.scores,
        asset_returns=snapshot.asset_returns,
        top_n=config.top_n,
        weighting_scheme=normalize_paper_weighting_scheme(config.weighting_scheme),
        vol_window=config.vol_window,
        min_score=config.min_score,
        max_weight=config.max_weight,
        symbol_groups=symbol_groups,
        max_names_per_group=config.max_names_per_group,
        max_group_weight=config.max_group_weight,
    )
    policy = ExecutionPolicy(
        timing=config.timing,
        rebalance_frequency=config.rebalance_frequency,
    )
    scheduled_weights_df, effective_weights_df = build_executed_weights(
        raw_target_weights,
        policy=policy,
    )
    as_of = str(pd.Timestamp(scheduled_weights_df.index.max()).date())
    latest_scheduled = {
        symbol: float(weight)
        for symbol, weight in scheduled_weights_df.loc[scheduled_weights_df.index.max()].items()
        if pd.notna(weight) and abs(float(weight)) > 0.0
    }
    latest_effective = {
        symbol: float(weight)
        for symbol, weight in effective_weights_df.loc[effective_weights_df.index.max()].items()
        if pd.notna(weight) and abs(float(weight)) > 0.0
    }
    latest_selection = {
        symbol: int(flag)
        for symbol, flag in selection.loc[selection.index.max()].items()
        if pd.notna(flag) and int(flag) != 0
    }
    diagnostics = {
        "selected_symbols": sorted(latest_selection.keys()),
        "selection_count": int(sum(latest_selection.values())),
        "raw_total_weight": float(raw_target_weights.iloc[-1].fillna(0.0).sum()),
        "scheduled_total_weight": float(scheduled_weights_df.iloc[-1].fillna(0.0).sum()),
        "effective_total_weight": float(effective_weights_df.iloc[-1].fillna(0.0).sum()),
    }
    return as_of, latest_scheduled, latest_effective, diagnostics


def _compute_latest_xsec_target_weights(
    *,
    config: PaperTradingConfig,
) -> tuple[str, dict[str, float], dict[str, float], dict[str, float], dict[str, float], dict[str, Any], list[str]]:
    prepared_frames, skipped_symbols, skip_reasons = _load_xsec_prepared_frames(config.symbols)
    result = run_xsec_momentum_topn(
        prepared_frames=prepared_frames,
        lookback_bars=int(config.lookback_bars or 84),
        skip_bars=int(config.skip_bars or 0),
        top_n=int(config.top_n),
        rebalance_bars=int(config.rebalance_bars or 21),
        commission=0.0,
        cash=float(config.initial_cash),
        max_position_weight=config.max_position_weight,
        min_avg_dollar_volume=config.min_avg_dollar_volume,
        max_names_per_sector=config.max_names_per_sector,
        turnover_buffer_bps=float(config.turnover_buffer_bps),
        max_turnover_per_rebalance=config.max_turnover_per_rebalance,
        weighting_scheme="inv_vol" if config.weighting_scheme == "inverse_vol" else config.weighting_scheme,
        vol_lookback_bars=int(config.vol_window),
        portfolio_construction_mode=config.portfolio_construction_mode,
        benchmark_type="equal_weight",
    )
    as_of_ts = pd.Timestamp(result.target_weights.index.max())
    as_of = as_of_ts.date().isoformat()
    latest_target_row = result.target_weights.loc[as_of_ts].fillna(0.0)
    latest_target_weights = {
        symbol: float(weight)
        for symbol, weight in latest_target_row.items()
        if abs(float(weight)) > 0.0
    }
    latest_scores_row = result.scores.loc[as_of_ts].dropna()
    latest_scores = {
        symbol: float(score)
        for symbol, score in latest_scores_row.items()
    }
    latest_prices: dict[str, float] = {}
    for symbol, prepared in prepared_frames.items():
        normalized = normalize_price_frame(prepared["df"])
        latest_price = pd.to_numeric(normalized["close"], errors="coerce").dropna()
        if not latest_price.empty:
            latest_prices[symbol] = float(latest_price.iloc[-1])

    if not result.rebalance_diagnostics.empty:
        latest_diag_row = result.rebalance_diagnostics.loc[:as_of_ts].iloc[-1].to_dict()
        latest_diag_timestamp = str(pd.Timestamp(result.rebalance_diagnostics.loc[:as_of_ts].index[-1]).date())
    else:
        latest_diag_row = {}
        latest_diag_timestamp = as_of

    target_diagnostics = {
        "preset_name": config.preset_name,
        "strategy": config.strategy,
        "portfolio_construction_mode": config.portfolio_construction_mode,
        "selected_symbols": latest_diag_row.get("selected_symbols", ""),
        "target_selected_symbols": latest_diag_row.get("target_selected_symbols", ""),
        "realized_holdings_count": latest_diag_row.get("realized_holdings_count"),
        "realized_holdings_minus_top_n": latest_diag_row.get("realized_holdings_minus_top_n"),
        "average_gross_exposure": result.summary.get("average_gross_exposure"),
        "liquidity_excluded_count": latest_diag_row.get("liquidity_excluded_count"),
        "sector_cap_excluded_count": latest_diag_row.get("sector_cap_excluded_count"),
        "turnover_cap_bound": latest_diag_row.get("turnover_cap_bound"),
        "turnover_cap_binding_count": result.summary.get("turnover_cap_binding_count"),
        "turnover_buffer_blocked_replacements": result.summary.get("turnover_buffer_blocked_replacements"),
        "semantic_warning": latest_diag_row.get("semantic_warning", ""),
        "rebalance_timestamp": latest_diag_timestamp,
        "weight_sum": latest_diag_row.get("weight_sum"),
        "weighting_scheme": result.summary.get("weighting_scheme"),
        "target_selected_count": latest_diag_row.get("target_selected_count"),
        "summary": result.summary,
        "skip_reasons": skip_reasons,
    }
    return as_of, latest_target_weights.copy(), latest_target_weights, latest_prices, latest_scores, target_diagnostics, skipped_symbols


def build_target_construction_result(
    *,
    config: PaperTradingConfig,
) -> TargetConstructionResult:
    if config.signal_source == "composite":
        snapshot, snapshot_diagnostics = build_composite_paper_snapshot(config=config)
        composite_targets = compute_latest_composite_target_weights(
            config=config,
            snapshot=snapshot,
            snapshot_diagnostics=snapshot_diagnostics,
        )
        return TargetConstructionResult(
            as_of=composite_targets.as_of,
            scheduled_target_weights=composite_targets.scheduled_target_weights,
            effective_target_weights=composite_targets.effective_target_weights,
            latest_prices=composite_targets.latest_prices,
            latest_scores=composite_targets.latest_scores,
            target_diagnostics=composite_targets.diagnostics.get("target_construction", {}),
            skipped_symbols=snapshot.skipped_symbols,
            signal_snapshot=snapshot,
            extra_diagnostics={
                key: value
                for key, value in composite_targets.diagnostics.items()
                if key != "target_construction"
            },
        )

    if config.strategy == "xsec_momentum_topn":
        (
            as_of,
            latest_scheduled_weights,
            latest_effective_weights,
            latest_prices,
            latest_scores,
            target_diagnostics,
            skipped_symbols,
        ) = _compute_latest_xsec_target_weights(config=config)
        snapshot = PaperSignalSnapshot(
            asset_returns=pd.DataFrame(),
            scores=pd.DataFrame(),
            closes=pd.DataFrame(),
            skipped_symbols=skipped_symbols,
            metadata={"mode": "xsec"},
        )
        return TargetConstructionResult(
            as_of=as_of,
            scheduled_target_weights=latest_scheduled_weights,
            effective_target_weights=latest_effective_weights,
            latest_prices=latest_prices,
            latest_scores=latest_scores,
            target_diagnostics=target_diagnostics,
            skipped_symbols=skipped_symbols,
            signal_snapshot=snapshot,
        )

    snapshot = load_signal_snapshot(
        symbols=config.symbols,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
    )
    latest_prices = {
        symbol: float(price)
        for symbol, price in snapshot.closes.iloc[-1].fillna(0.0).items()
        if float(price) > 0.0
    }
    latest_scores = {
        symbol: float(score)
        for symbol, score in snapshot.scores.iloc[-1].fillna(0.0).items()
    }
    as_of, latest_scheduled_weights, latest_effective_weights, target_diagnostics = (
        compute_latest_target_weights(
            config=config,
            snapshot=snapshot,
        )
    )
    return TargetConstructionResult(
        as_of=as_of,
        scheduled_target_weights=latest_scheduled_weights,
        effective_target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        latest_scores=latest_scores,
        target_diagnostics=target_diagnostics,
        skipped_symbols=snapshot.skipped_symbols,
        signal_snapshot=snapshot,
    )
