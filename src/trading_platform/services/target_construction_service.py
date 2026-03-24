from __future__ import annotations

from dataclasses import dataclass, field
from dataclasses import replace
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.cli.common import normalize_paper_weighting_scheme
from trading_platform.construction.service import build_top_n_portfolio_weights
from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.decision_journal.service import build_candidate_journal_for_snapshot
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.metadata.groups import build_group_series
from trading_platform.paper.composite import (
    build_composite_paper_snapshot,
    compute_latest_composite_target_weights,
)
from trading_platform.paper.ensemble import build_ensemble_paper_snapshot
from trading_platform.paper.models import PaperExecutionPriceSnapshot, PaperSignalSnapshot, PaperTradingConfig
from trading_platform.paper.price_diagnostics import (
    build_execution_price_snapshots,
    summarize_execution_price_snapshots,
)
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn
from trading_platform.signals.common import normalize_price_frame
from trading_platform.signals.loaders import load_feature_frame, resolve_feature_frame_path
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.ingestion.alpaca_data import (
    fetch_alpaca_bars,
    merge_historical_with_latest,
)
from trading_platform.universe_provenance.models import UniverseBuildBundle
from trading_platform.universe_provenance.service import build_universe_provenance_bundle


logger = logging.getLogger(__name__)


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
    price_snapshots: list[PaperExecutionPriceSnapshot] = field(default_factory=list)
    decision_bundle: DecisionJournalBundle | None = None
    universe_bundle: UniverseBuildBundle | None = None


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


def _historical_price_source(config: PaperTradingConfig) -> str:
    prices = (config.data_sources or {}).get("prices", {})
    historical = str(prices.get("historical", "yfinance")).lower()
    return historical or "yfinance"


def _latest_price_source(config: PaperTradingConfig) -> str:
    if config.use_alpaca_latest_data:
        return "alpaca"
    prices = (config.data_sources or {}).get("prices", {})
    latest = str(prices.get("latest", "yfinance")).lower()
    return latest or "yfinance"


def _latest_fetch_window(frames: dict[str, pd.DataFrame]) -> tuple[str, str] | None:
    timestamps: list[pd.Timestamp] = []
    for frame in frames.values():
        normalized = pd.to_datetime(frame.get("timestamp"), errors="coerce") if "timestamp" in frame.columns else pd.Series(dtype="datetime64[ns]")
        normalized = normalized.dropna()
        if not normalized.empty:
            timestamps.append(pd.Timestamp(normalized.max()))
    if not timestamps:
        return None
    latest_timestamp = max(timestamps)
    start = (latest_timestamp - pd.Timedelta(days=7)).date().isoformat()
    end = (pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1)).date().isoformat()
    return start, end


def _build_decision_bundle(
    *,
    config: PaperTradingConfig,
    as_of: str,
    latest_scores: dict[str, float],
    latest_prices: dict[str, float],
    scheduled_target_weights: dict[str, float],
    effective_target_weights: dict[str, float],
    skipped_symbols: list[str],
    skip_reasons: dict[str, str] | None = None,
    asset_return_map: dict[str, float | None] | None = None,
    selected_rejection_reasons: dict[str, str] | None = None,
    universe_bundle: UniverseBuildBundle | None = None,
    metadata: dict[str, Any] | None = None,
) -> DecisionJournalBundle:
    run_id = (
        f"{config.preset_name or 'manual'}|{config.strategy}|{config.universe_name or 'symbols'}|{as_of}"
    )
    return build_candidate_journal_for_snapshot(
        timestamp=as_of,
        run_id=run_id,
        cycle_id=as_of,
        strategy_id=config.strategy,
        universe_id=config.universe_name,
        base_universe_id=(universe_bundle.summary.base_universe_id if universe_bundle and universe_bundle.summary else config.universe_name),
        sub_universe_id=(universe_bundle.summary.sub_universe_id if universe_bundle and universe_bundle.summary else config.sub_universe_id),
        score_map=latest_scores,
        latest_prices=latest_prices,
        selected_weights=effective_target_weights,
        scheduled_weights=scheduled_target_weights,
        skipped_symbols=skipped_symbols,
        skip_reasons=skip_reasons,
        asset_return_map=asset_return_map,
        selected_rejection_reasons=selected_rejection_reasons,
        filtered_out_symbols=[
            row.symbol
            for row in (universe_bundle.membership_records if universe_bundle is not None else [])
            if row.inclusion_status == "excluded"
        ],
        filtered_reasons={
            row.symbol: str(row.exclusion_reason or "filtered_out_before_ranking")
            for row in (universe_bundle.membership_records if universe_bundle is not None else [])
            if row.inclusion_status == "excluded"
        },
        universe_metadata_by_symbol={
            row.symbol: {
                **dict(row.metadata_snapshot.to_dict()),
                **dict(row.taxonomy.to_dict()),
                **dict(row.benchmark_context.to_dict()),
                "membership_resolution_status": row.membership.membership_resolution_status,
                "membership_source": row.membership.membership_source,
                "metadata_coverage_status": row.metadata_coverage_status,
            }
            for row in (universe_bundle.enrichment_records if universe_bundle is not None else [])
        },
        metadata={
            "preset_name": config.preset_name,
            "signal_source": config.signal_source,
            **dict(metadata or {}),
        },
    )


def _build_universe_bundle(config: PaperTradingConfig) -> UniverseBuildBundle:
    return build_universe_provenance_bundle(
        symbols=config.symbols,
        base_universe_id=config.universe_name,
        sub_universe_id=config.sub_universe_id,
        filter_definitions=config.universe_filters,
        feature_loader=load_feature_frame,
        group_map_path=config.group_map_path,
        membership_history_path=config.universe_membership_path,
        benchmark_id=config.benchmark,
        market_regime_path=config.market_regime_path,
    )


def _merge_latest_bar_into_frame(historical_frame: pd.DataFrame, latest_bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if latest_bars.empty:
        return historical_frame
    if "symbol" not in historical_frame.columns:
        historical_frame = historical_frame.copy()
        historical_frame["symbol"] = symbol
    latest_symbol = latest_bars[latest_bars["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if latest_symbol.empty:
        return historical_frame
    latest_symbol = latest_symbol.rename(columns={"date": "timestamp"})
    merged = merge_historical_with_latest(historical_frame, latest_symbol)
    if "date" in merged.columns and "timestamp" not in merged.columns:
        merged = merged.rename(columns={"date": "timestamp"})
    return merged


def _apply_latest_market_data(
    *,
    config: PaperTradingConfig,
    historical_frames: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], str, bool]:
    latest_source = _latest_price_source(config)
    historical_source = _historical_price_source(config)
    logger.info(
        "paper market data sources: historical=%s latest=%s symbols=%s",
        historical_source,
        latest_source,
        len(historical_frames),
    )
    if latest_source != "alpaca":
        return historical_frames, latest_source, False

    window = _latest_fetch_window(historical_frames)
    if window is None:
        logger.warning("Alpaca latest-data override requested but no historical timestamps were available; using historical data only")
        return historical_frames, "yfinance", True
    start, end = window
    try:
        latest_bars = fetch_alpaca_bars(sorted(historical_frames), start=start, end=end, timeframe="1Day")
    except Exception as exc:
        logger.warning("Alpaca latest-data fetch failed for %s symbol(s); falling back to historical data: %s", len(historical_frames), exc)
        return historical_frames, "yfinance", True
    if latest_bars.empty:
        logger.warning("Alpaca latest-data fetch returned no rows for %s symbol(s); falling back to historical data", len(historical_frames))
        return historical_frames, "yfinance", True
    merged_frames: dict[str, pd.DataFrame] = {}
    for symbol, frame in historical_frames.items():
        merged_frames[symbol] = _merge_latest_bar_into_frame(frame, latest_bars, symbol)
    logger.info("using Alpaca latest data for %s symbol(s)", len(merged_frames))
    return merged_frames, "alpaca", False


def load_signal_snapshot(
    *,
    symbols: list[str],
    strategy: str,
    fast: int | None = None,
    slow: int | None = None,
    lookback: int | None = None,
    config: PaperTradingConfig | None = None,
) -> PaperSignalSnapshot:
    feature_frames: dict[str, pd.DataFrame] = {}
    historical_frames: dict[str, pd.DataFrame] = {}
    signal_fn = SIGNAL_REGISTRY[strategy]
    asset_return_frames: list[pd.Series] = []
    score_frames: list[pd.Series] = []
    close_frames: list[pd.Series] = []
    skipped_symbols: list[str] = []
    skip_reasons: dict[str, str] = {}

    for symbol in symbols:
        try:
            loaded_frame = load_feature_frame(symbol)
            feature_frames[symbol] = loaded_frame
            historical_frames[symbol] = loaded_frame.copy()
        except Exception as exc:
            skipped_symbols.append(symbol)
            skip_reasons[symbol] = repr(exc)

    latest_source_effective = _latest_price_source(config) if config is not None else "yfinance"
    latest_fallback_used = False
    if config is not None and feature_frames:
        historical_source = _historical_price_source(config)
        latest_source = _latest_price_source(config)
        logger.info("loading signal snapshot with historical=%s latest=%s symbols=%s", historical_source, latest_source, len(feature_frames))
        feature_frames, latest_source_effective, latest_fallback_used = _apply_latest_market_data(
            config=config,
            historical_frames=feature_frames,
        )
        if latest_fallback_used:
            logger.warning("signal snapshot fell back to yfinance-derived history for latest bars")

    for symbol in symbols:
        try:
            feature_df = feature_frames[symbol]
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
    historical_source = _historical_price_source(config) if config is not None else "yfinance"
    price_snapshots = build_execution_price_snapshots(
        historical_frames=historical_frames,
        final_frames=feature_frames,
        historical_source=historical_source,
        latest_data_source=latest_source_effective,
        fallback_used=latest_fallback_used,
        latest_data_max_age_seconds=int(getattr(config, "latest_data_max_age_seconds", 86_400) or 86_400),
    )
    freshness_summary = summarize_execution_price_snapshots(
        price_snapshots,
        latest_data_source=latest_source_effective,
        fallback_used=latest_fallback_used,
    )

    return PaperSignalSnapshot(
        asset_returns=asset_returns,
        scores=scores,
        closes=closes,
        skipped_symbols=skipped_symbols,
        metadata={
            "skip_reasons": skip_reasons,
            "historical_source": historical_source,
            "latest_source": latest_source_effective,
            "latest_fallback_used": latest_fallback_used,
            "price_snapshots": price_snapshots,
            "freshness_summary": freshness_summary,
        },
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
) -> tuple[str, dict[str, float], dict[str, float], dict[str, float], dict[str, float], dict[str, Any], list[str], list[PaperExecutionPriceSnapshot]]:
    prepared_frames, skipped_symbols, skip_reasons = _load_xsec_prepared_frames(config.symbols)
    historical_frames = {
        symbol: prepared["df"]
        for symbol, prepared in prepared_frames.items()
    }
    merged_frames, effective_latest_source, fallback_used = _apply_latest_market_data(
        config=config,
        historical_frames=historical_frames,
    )
    if fallback_used:
        logger.warning("xsec target construction fell back to yfinance-derived history for latest bars")
    prepared_frames = {
        symbol: {
            **prepared_frames[symbol],
            "df": merged_frames[symbol],
        }
        for symbol in prepared_frames
    }
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
    price_snapshots = build_execution_price_snapshots(
        historical_frames=historical_frames,
        final_frames=merged_frames,
        historical_source=_historical_price_source(config),
        latest_data_source=effective_latest_source,
        fallback_used=fallback_used,
        latest_data_max_age_seconds=int(config.latest_data_max_age_seconds),
    )
    freshness_summary = summarize_execution_price_snapshots(
        price_snapshots,
        latest_data_source=effective_latest_source,
        fallback_used=fallback_used,
    )

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
        "excluded_reasons": latest_diag_row.get("excluded_reasons", {}),
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
        "historical_price_source": _historical_price_source(config),
        "latest_price_source": effective_latest_source,
        "latest_price_fallback_used": fallback_used,
        "universe_summary": {},
        **freshness_summary,
    }
    return as_of, latest_target_weights.copy(), latest_target_weights, latest_prices, latest_scores, target_diagnostics, skipped_symbols, price_snapshots


def build_target_construction_result(
    *,
    config: PaperTradingConfig,
) -> TargetConstructionResult:
    universe_bundle = _build_universe_bundle(config)
    effective_symbols = universe_bundle.eligible_symbols or []
    working_config = replace(config, symbols=effective_symbols)
    if not effective_symbols:
        as_of = universe_bundle.summary.as_of if universe_bundle.summary is not None else pd.Timestamp.utcnow().date().isoformat()
        empty_snapshot = PaperSignalSnapshot(
            asset_returns=pd.DataFrame(),
            scores=pd.DataFrame(),
            closes=pd.DataFrame(),
            skipped_symbols=[],
            metadata={"mode": "empty_universe"},
        )
        decision_bundle = _build_decision_bundle(
            config=config,
            as_of=as_of,
            latest_scores={},
            latest_prices={},
            scheduled_target_weights={},
            effective_target_weights={},
            skipped_symbols=[],
            universe_bundle=universe_bundle,
            metadata={"target_construction_mode": "empty_universe"},
        )
        return TargetConstructionResult(
            as_of=as_of,
            scheduled_target_weights={},
            effective_target_weights={},
            latest_prices={},
            latest_scores={},
            target_diagnostics={
                "universe_summary": universe_bundle.summary.to_dict() if universe_bundle.summary is not None else {},
                "universe_filters_active": [row.filter_name for row in universe_bundle.filter_definitions if row.enabled],
            },
            skipped_symbols=[],
            signal_snapshot=empty_snapshot,
            decision_bundle=decision_bundle,
            universe_bundle=universe_bundle,
        )
    if config.signal_source == "composite":
        snapshot, snapshot_diagnostics = build_composite_paper_snapshot(config=working_config)
        composite_targets = compute_latest_composite_target_weights(
            config=working_config,
            snapshot=snapshot,
            snapshot_diagnostics=snapshot_diagnostics,
        )
        asset_return_map = {}
        if not snapshot.asset_returns.empty:
            asset_return_map = {
                symbol: float(value)
                for symbol, value in snapshot.asset_returns.iloc[-1].dropna().items()
            }
        decision_bundle = _build_decision_bundle(
            config=config,
            as_of=composite_targets.as_of,
            latest_scores=composite_targets.latest_scores,
            latest_prices=composite_targets.latest_prices,
            scheduled_target_weights=composite_targets.scheduled_target_weights,
            effective_target_weights=composite_targets.effective_target_weights,
            skipped_symbols=snapshot.skipped_symbols,
            skip_reasons=dict(snapshot.metadata.get("skip_reasons", {})),
            asset_return_map=asset_return_map,
            universe_bundle=universe_bundle,
            metadata={"target_construction_mode": "composite"},
        )
        return TargetConstructionResult(
            as_of=composite_targets.as_of,
            scheduled_target_weights=composite_targets.scheduled_target_weights,
            effective_target_weights=composite_targets.effective_target_weights,
            latest_prices=composite_targets.latest_prices,
            latest_scores=composite_targets.latest_scores,
            target_diagnostics={
                **composite_targets.diagnostics.get("target_construction", {}),
                "universe_summary": universe_bundle.summary.to_dict() if universe_bundle.summary is not None else {},
            },
            skipped_symbols=snapshot.skipped_symbols,
            signal_snapshot=snapshot,
            extra_diagnostics={
                key: value
                for key, value in composite_targets.diagnostics.items()
                if key != "target_construction"
            },
            price_snapshots=composite_targets.price_snapshots,
            decision_bundle=decision_bundle,
            universe_bundle=universe_bundle,
        )
    if config.signal_source == "ensemble":
        snapshot, snapshot_diagnostics = build_ensemble_paper_snapshot(config=working_config)
        latest_prices = {
            symbol: float(price)
            for symbol, price in snapshot.closes.iloc[-1].fillna(0.0).items()
            if float(price) > 0.0
        }
        latest_scores = {
            symbol: float(score)
            for symbol, score in snapshot.scores.iloc[-1].fillna(0.0).items()
        }
        as_of, latest_scheduled_weights, latest_effective_weights, target_diagnostics = compute_latest_target_weights(
            config=working_config,
            snapshot=snapshot,
        )
        target_diagnostics = {
            **target_diagnostics,
            **snapshot_diagnostics,
            "universe_summary": universe_bundle.summary.to_dict() if universe_bundle.summary is not None else {},
        }
        ensemble_snapshot = snapshot.metadata.get("ensemble_snapshot", pd.DataFrame())
        if isinstance(ensemble_snapshot, pd.DataFrame) and not ensemble_snapshot.empty:
            serializable_snapshot = ensemble_snapshot.copy()
            if "timestamp" in serializable_snapshot.columns:
                serializable_snapshot["timestamp"] = pd.to_datetime(
                    serializable_snapshot["timestamp"],
                    errors="coerce",
                ).astype(str)
        else:
            serializable_snapshot = pd.DataFrame()
        asset_return_map = {}
        if not snapshot.asset_returns.empty:
            asset_return_map = {
                symbol: float(value)
                for symbol, value in snapshot.asset_returns.iloc[-1].dropna().items()
            }
        decision_bundle = _build_decision_bundle(
            config=config,
            as_of=as_of,
            latest_scores=latest_scores,
            latest_prices=latest_prices,
            scheduled_target_weights=latest_scheduled_weights,
            effective_target_weights=latest_effective_weights,
            skipped_symbols=snapshot.skipped_symbols,
            skip_reasons=dict(snapshot.metadata.get("skip_reasons", {})),
            asset_return_map=asset_return_map,
            universe_bundle=universe_bundle,
            metadata={"target_construction_mode": "ensemble"},
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
            extra_diagnostics={
                "ensemble_snapshot": serializable_snapshot.to_dict(orient="records")
                if not serializable_snapshot.empty
                else [],
                "ensemble_member_summary": snapshot.metadata.get("ensemble_member_summary", pd.DataFrame()).to_dict(orient="records")
                if isinstance(snapshot.metadata.get("ensemble_member_summary"), pd.DataFrame)
                else [],
            },
            price_snapshots=list(snapshot.metadata.get("price_snapshots", [])),
            decision_bundle=decision_bundle,
            universe_bundle=universe_bundle,
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
            price_snapshots,
        ) = _compute_latest_xsec_target_weights(config=working_config)
        target_diagnostics = {
            **target_diagnostics,
            "universe_summary": universe_bundle.summary.to_dict() if universe_bundle.summary is not None else {},
        }
        snapshot = PaperSignalSnapshot(
            asset_returns=pd.DataFrame(),
            scores=pd.DataFrame(),
            closes=pd.DataFrame(),
            skipped_symbols=skipped_symbols,
            metadata={"mode": "xsec"},
        )
        decision_bundle = _build_decision_bundle(
            config=config,
            as_of=as_of,
            latest_scores=latest_scores,
            latest_prices=latest_prices,
            scheduled_target_weights=latest_scheduled_weights,
            effective_target_weights=latest_effective_weights,
            skipped_symbols=skipped_symbols,
            skip_reasons=dict(target_diagnostics.get("skip_reasons", {})),
            selected_rejection_reasons={
                str(symbol): str(reason)
                for symbol, reason in dict(target_diagnostics.get("excluded_reasons", {})).items()
            }
            if isinstance(target_diagnostics.get("excluded_reasons"), dict)
            else None,
            universe_bundle=universe_bundle,
            metadata={
                "target_construction_mode": "xsec",
                "excluded_reasons": target_diagnostics.get("excluded_reasons", {}),
            },
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
            price_snapshots=price_snapshots,
            decision_bundle=decision_bundle,
            universe_bundle=universe_bundle,
        )
    snapshot = load_signal_snapshot(
        symbols=working_config.symbols,
        strategy=working_config.strategy,
        fast=working_config.fast,
        slow=working_config.slow,
        lookback=working_config.lookback,
        config=working_config,
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
            config=working_config,
            snapshot=snapshot,
        )
    )
    target_diagnostics = {
        **target_diagnostics,
        "historical_price_source": snapshot.metadata.get("historical_source", "yfinance"),
        "latest_price_source": snapshot.metadata.get("latest_source", "yfinance"),
        "latest_price_fallback_used": snapshot.metadata.get("latest_fallback_used", False),
        "universe_summary": universe_bundle.summary.to_dict() if universe_bundle.summary is not None else {},
        **dict(snapshot.metadata.get("freshness_summary", {})),
    }
    asset_return_map = {}
    if not snapshot.asset_returns.empty:
        asset_return_map = {
            symbol: float(value)
            for symbol, value in snapshot.asset_returns.iloc[-1].dropna().items()
        }
    decision_bundle = _build_decision_bundle(
        config=config,
        as_of=as_of,
        latest_scores=latest_scores,
        latest_prices=latest_prices,
        scheduled_target_weights=latest_scheduled_weights,
        effective_target_weights=latest_effective_weights,
        skipped_symbols=snapshot.skipped_symbols,
        skip_reasons=dict(snapshot.metadata.get("skip_reasons", {})),
        asset_return_map=asset_return_map,
        universe_bundle=universe_bundle,
        metadata={"target_construction_mode": "signal_snapshot"},
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
        price_snapshots=list(snapshot.metadata.get("price_snapshots", [])),
        decision_bundle=decision_bundle,
        universe_bundle=universe_bundle,
    )
