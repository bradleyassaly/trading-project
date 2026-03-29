from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd

import trading_platform.services.target_construction_service as target_construction_service
from trading_platform.broker.base import BrokerFill, BrokerOrder
from trading_platform.broker.paper_broker import PaperBroker, PaperBrokerConfig
from trading_platform.cli.common import normalize_paper_weighting_scheme
from trading_platform.construction.service import build_top_n_portfolio_weights
from trading_platform.decision_journal.service import (
    enrich_bundle_with_orders,
    write_decision_journal_artifacts,
)
from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.execution.realism import (
    ExecutableOrder,
    ExecutionSimulationResult,
    ExecutionConfig,
    ExecutionOrderRequest,
    ExecutionSummary,
    LiquidityDiagnostic,
    RejectedOrder,
    build_execution_requests_from_target_weights,
    simulate_execution,
    write_execution_artifacts,
)
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.metadata.groups import build_group_series
from trading_platform.paper.models import (
    OrderGenerationResult,
    PaperOrder,
    PaperExecutionPriceSnapshot,
    PaperPortfolioState,
    PaperPosition,
    PaperSignalSnapshot,
    PaperTradeLot,
    PaperTradingConfig,
    PaperTradingRunResult,
)
from trading_platform.reporting.pnl_attribution import (
    allocate_integer_quantities,
    build_daily_attribution,
    build_attribution_summary,
    build_reconciliation_summary,
    write_pnl_attribution_artifacts,
)
from trading_platform.reporting.ev_lifecycle import (
    build_trade_ev_lifecycle_rows,
    write_trade_ev_lifecycle_artifacts,
)
from trading_platform.settings import METADATA_DIR
from trading_platform.paper.slippage import apply_order_slippage, validate_slippage_config
from trading_platform.risk.pre_trade_checks import validate_orders
from trading_platform.research.trade_ev import (
    build_trade_ev_calibration,
    build_trade_ev_candidate_market_features,
    build_trade_ev_training_dataset,
    score_trade_ev_candidates,
    train_trade_ev_model,
    write_trade_ev_artifacts,
)
from trading_platform.research.trade_ev_regression import (
    build_trade_ev_regression_history_dataset,
    score_trade_ev_regression_candidates,
    train_trade_ev_regression_model,
)
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn
from trading_platform.signals.common import normalize_price_frame
from trading_platform.signals.loaders import load_feature_frame, resolve_feature_frame_path
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.services.target_construction_service import (
    _compute_latest_xsec_target_weights as shared_compute_latest_xsec_target_weights,
    build_target_construction_result,
    compute_latest_target_weights as shared_compute_latest_target_weights,
    load_signal_snapshot as shared_load_signal_snapshot,
)
from trading_platform.universe_provenance.models import UniverseBuildBundle
from trading_platform.universe_provenance.service import write_universe_provenance_artifacts


class JsonPaperStateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> PaperPortfolioState:
        if not self.path.exists():
            return PaperPortfolioState(cash=0.0)

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        positions = {
            symbol: PaperPosition(**position_payload)
            for symbol, position_payload in payload.get("positions", {}).items()
        }
        state = PaperPortfolioState(
            as_of=payload.get("as_of"),
            cash=float(payload.get("cash", 0.0)),
            positions=positions,
            last_targets={symbol: float(weight) for symbol, weight in payload.get("last_targets", {}).items()},
            initial_cash_basis=float(payload.get("initial_cash_basis", 0.0) or 0.0),
            cumulative_realized_pnl=float(payload.get("cumulative_realized_pnl", 0.0) or 0.0),
            cumulative_gross_realized_pnl=float(payload.get("cumulative_gross_realized_pnl", 0.0) or 0.0),
            cumulative_fees=float(payload.get("cumulative_fees", 0.0) or 0.0),
            cumulative_slippage_cost=float(payload.get("cumulative_slippage_cost", 0.0) or 0.0),
            cumulative_spread_cost=float(payload.get("cumulative_spread_cost", 0.0) or 0.0),
            cumulative_execution_cost=float(payload.get("cumulative_execution_cost", 0.0) or 0.0),
            open_lots={
                symbol: [PaperTradeLot(**row) for row in rows]
                for symbol, rows in dict(payload.get("open_lots", {})).items()
            },
            next_trade_id=int(payload.get("next_trade_id", 1) or 1),
        )
        if state.initial_cash_basis <= 0.0:
            state.initial_cash_basis = float(state.cash + sum(position.cost_basis for position in positions.values()))
        return state

    def save(self, state: PaperPortfolioState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "as_of": state.as_of,
            "cash": state.cash,
            "positions": {symbol: asdict(position) for symbol, position in sorted(state.positions.items())},
            "last_targets": state.last_targets,
            "initial_cash_basis": state.initial_cash_basis,
            "cumulative_realized_pnl": state.cumulative_realized_pnl,
            "cumulative_gross_realized_pnl": state.cumulative_gross_realized_pnl,
            "cumulative_fees": state.cumulative_fees,
            "cumulative_slippage_cost": state.cumulative_slippage_cost,
            "cumulative_spread_cost": state.cumulative_spread_cost,
            "cumulative_execution_cost": state.cumulative_execution_cost,
            "open_lots": {symbol: [asdict(lot) for lot in lots] for symbol, lots in sorted(state.open_lots.items())},
            "next_trade_id": int(state.next_trade_id),
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_xsec_prepared_frames(
    symbols: list[str],
) -> tuple[dict[str, dict[str, object]], list[str], dict[str, str]]:
    raise NotImplementedError(
        "_load_xsec_prepared_frames moved to trading_platform.services.target_construction_service"
    )


def _compute_latest_xsec_target_weights(
    *,
    config: PaperTradingConfig,
) -> tuple[
    str,
    dict[str, float],
    dict[str, float],
    dict[str, float],
    dict[str, float],
    dict[str, Any],
    list[str],
    list[PaperExecutionPriceSnapshot],
]:
    target_construction_service.load_feature_frame = load_feature_frame
    target_construction_service.resolve_feature_frame_path = resolve_feature_frame_path
    target_construction_service.run_xsec_momentum_topn = run_xsec_momentum_topn
    target_construction_service.normalize_price_frame = normalize_price_frame
    return shared_compute_latest_xsec_target_weights(config=config)


def bootstrap_paper_portfolio_state(
    *,
    initial_cash: float,
) -> PaperPortfolioState:
    return PaperPortfolioState(
        cash=float(initial_cash),
        initial_cash_basis=float(initial_cash),
    )


def _clone_state(state: PaperPortfolioState) -> PaperPortfolioState:
    return PaperPortfolioState(
        as_of=state.as_of,
        cash=float(state.cash),
        positions={
            symbol: PaperPosition(
                symbol=position.symbol,
                quantity=int(position.quantity),
                avg_price=float(position.avg_price),
                last_price=float(position.last_price),
            )
            for symbol, position in state.positions.items()
        },
        last_targets={symbol: float(weight) for symbol, weight in state.last_targets.items()},
        initial_cash_basis=float(state.initial_cash_basis),
        cumulative_realized_pnl=float(state.cumulative_realized_pnl),
        cumulative_gross_realized_pnl=float(state.cumulative_gross_realized_pnl),
        cumulative_fees=float(state.cumulative_fees),
        cumulative_slippage_cost=float(state.cumulative_slippage_cost),
        cumulative_spread_cost=float(state.cumulative_spread_cost),
        cumulative_execution_cost=float(state.cumulative_execution_cost),
        open_lots={
            symbol: [
                PaperTradeLot(
                    trade_id=str(lot.trade_id),
                    symbol=lot.symbol,
                    strategy_id=lot.strategy_id,
                    signal_source=lot.signal_source,
                    signal_family=lot.signal_family,
                    side=lot.side,
                    entry_as_of=lot.entry_as_of,
                    entry_reference_price=float(lot.entry_reference_price),
                    entry_price=float(lot.entry_price),
                    quantity=int(lot.quantity),
                    remaining_quantity=int(lot.remaining_quantity),
                    entry_slippage_cost=float(lot.entry_slippage_cost),
                    entry_spread_cost=float(lot.entry_spread_cost),
                    entry_commission_cost=float(lot.entry_commission_cost),
                    entry_total_execution_cost=float(lot.entry_total_execution_cost),
                    cost_model=str(lot.cost_model),
                    attribution_method=lot.attribution_method,
                    metadata=dict(lot.metadata),
                )
                for lot in lots
            ]
            for symbol, lots in state.open_lots.items()
        },
        next_trade_id=int(state.next_trade_id),
    )


def load_signal_snapshot(
    *,
    symbols: list[str],
    strategy: str,
    fast: int | None = None,
    slow: int | None = None,
    lookback: int | None = None,
    config: PaperTradingConfig | None = None,
) -> PaperSignalSnapshot:
    target_construction_service.load_feature_frame = load_feature_frame
    target_construction_service.SIGNAL_REGISTRY = SIGNAL_REGISTRY
    return shared_load_signal_snapshot(
        symbols=symbols,
        strategy=strategy,
        fast=fast,
        slow=slow,
        lookback=lookback,
        config=config,
    )


def compute_latest_target_weights(
    *,
    config: PaperTradingConfig,
    snapshot: PaperSignalSnapshot,
) -> tuple[str, dict[str, float], dict[str, float], dict[str, Any]]:
    target_construction_service.build_group_series = build_group_series
    target_construction_service.build_top_n_portfolio_weights = build_top_n_portfolio_weights
    target_construction_service.normalize_paper_weighting_scheme = normalize_paper_weighting_scheme
    target_construction_service.ExecutionPolicy = ExecutionPolicy
    target_construction_service.build_executed_weights = build_executed_weights
    return shared_compute_latest_target_weights(config=config, snapshot=snapshot)


def sync_state_prices(
    state: PaperPortfolioState,
    latest_prices: dict[str, float],
) -> PaperPortfolioState:
    for symbol, position in state.positions.items():
        if symbol in latest_prices:
            position.last_price = float(latest_prices[symbol])
    return state


def _score_band_enabled(config: PaperTradingConfig) -> bool:
    return any(
        value is not None
        for value in (
            config.entry_score_threshold,
            config.exit_score_threshold,
            config.entry_score_percentile,
            config.exit_score_percentile,
        )
    )


def _score_rank_lookup(latest_scores: dict[str, float]) -> dict[str, dict[str, float]]:
    rows: list[tuple[str, float]] = []
    for symbol, raw_score in sorted(latest_scores.items()):
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            continue
        if pd.isna(score):
            continue
        rows.append((str(symbol), score))
    ordered = sorted(rows, key=lambda item: (-item[1], item[0]))
    total = len(ordered)
    lookup: dict[str, dict[str, float]] = {}
    for index, (symbol, score) in enumerate(ordered, start=1):
        percentile = float((total - index + 1) / total) if total > 0 else 0.0
        lookup[symbol] = {
            "score_value": score,
            "score_rank": float(index),
            "score_percentile": percentile,
        }
    return lookup


def _resolve_score_band_thresholds(
    *,
    config: PaperTradingConfig,
) -> tuple[float | None, float | None]:
    if config.use_percentile_thresholds:
        entry_threshold = (
            float(config.entry_score_percentile)
            if config.entry_score_percentile is not None
            else (
                float(config.entry_score_threshold)
                if config.entry_score_threshold is not None
                else None
            )
        )
        exit_threshold = (
            float(config.exit_score_percentile)
            if config.exit_score_percentile is not None
            else (
                float(config.exit_score_threshold)
                if config.exit_score_threshold is not None
                else entry_threshold
            )
        )
        return entry_threshold, exit_threshold
    entry_threshold = float(config.entry_score_threshold) if config.entry_score_threshold is not None else None
    exit_threshold = (
        float(config.exit_score_threshold)
        if config.exit_score_threshold is not None
        else entry_threshold
    )
    return entry_threshold, exit_threshold


def _metric_for_band_decision(
    *,
    symbol: str,
    latest_scores: dict[str, float],
    rank_lookup: dict[str, dict[str, float]],
    use_percentile_thresholds: bool,
) -> tuple[float | None, float | None, float | None]:
    rank_row = rank_lookup.get(symbol, {})
    score_value = rank_row.get("score_value")
    score_rank = rank_row.get("score_rank")
    score_percentile = rank_row.get("score_percentile")
    if score_value is None:
        raw_score = latest_scores.get(symbol)
        try:
            score_value = None if raw_score is None else float(raw_score)
        except (TypeError, ValueError):
            score_value = None
    metric_value = score_percentile if use_percentile_thresholds else score_value
    return metric_value, score_value, score_rank


def _apply_score_band_to_target(
    *,
    symbol: str,
    current_weight: float,
    target_weight: float,
    latest_scores: dict[str, float],
    rank_lookup: dict[str, dict[str, float]],
    config: PaperTradingConfig,
    entry_threshold: float | None,
    exit_threshold: float | None,
) -> tuple[float, dict[str, Any]]:
    metric_value, score_value, score_rank = _metric_for_band_decision(
        symbol=symbol,
        latest_scores=latest_scores,
        rank_lookup=rank_lookup,
        use_percentile_thresholds=bool(config.use_percentile_thresholds),
    )
    score_percentile = rank_lookup.get(symbol, {}).get("score_percentile")
    score_band_enabled = _score_band_enabled(config)
    band_row = {
        "symbol": symbol,
        "score_value": score_value,
        "score_rank": score_rank,
        "score_percentile": score_percentile,
        "entry_threshold": entry_threshold,
        "exit_threshold": exit_threshold,
        "score_band_enabled": score_band_enabled,
        "band_decision": "bands_disabled",
        "action_reason": "",
        "band_metric_value": metric_value,
        "band_metric_name": "score_percentile" if config.use_percentile_thresholds else "score_value",
    }
    if not score_band_enabled:
        return target_weight, band_row
    if metric_value is None:
        band_row["band_decision"] = "missing_score"
        band_row["action_reason"] = "missing_score_for_band_decision"
        return target_weight, band_row

    currently_held = abs(current_weight) > 1e-12
    if not currently_held:
        if (
            config.apply_bands_to_new_entries
            and abs(target_weight) > 1e-12
            and entry_threshold is not None
            and metric_value < entry_threshold
        ):
            band_row["band_decision"] = "blocked_entry"
            band_row["action_reason"] = "blocked_below_entry_threshold"
            return 0.0, band_row
        if abs(target_weight) > 1e-12 and entry_threshold is not None:
            band_row["band_decision"] = "passed_entry"
            band_row["action_reason"] = "passed_entry_threshold"
        return target_weight, band_row

    if exit_threshold is not None and metric_value < exit_threshold:
        if config.apply_bands_to_full_exits:
            band_row["band_decision"] = "forced_exit"
            band_row["action_reason"] = "exit_below_exit_threshold"
            return 0.0, band_row
        band_row["band_decision"] = "exit_threshold_observed"
        band_row["action_reason"] = "exit_below_exit_threshold"
        return target_weight, band_row

    if (
        bool(config.hold_score_band)
        and entry_threshold is not None
        and exit_threshold is not None
        and exit_threshold <= metric_value < entry_threshold
    ):
        reducing = target_weight < current_weight - 1e-12
        increasing = target_weight > current_weight + 1e-12
        if (reducing and (config.apply_bands_to_reductions or (target_weight <= 0 and config.apply_bands_to_full_exits))) or (
            increasing and config.apply_bands_to_new_entries
        ):
            band_row["band_decision"] = "hold_zone"
            band_row["action_reason"] = "held_within_hold_zone"
            return current_weight, band_row
        band_row["band_decision"] = "hold_zone_observed"
        band_row["action_reason"] = "held_within_hold_zone"
        return target_weight, band_row

    if entry_threshold is not None and metric_value >= entry_threshold:
        band_row["band_decision"] = "passed_entry"
        band_row["action_reason"] = "passed_entry_threshold"
    return target_weight, band_row


def _dominant_strategy_id(provenance: dict[str, Any] | None) -> str | None:
    strategy_rows = list((provenance or {}).get("strategy_rows") or [])
    if not strategy_rows:
        return None
    ordered = sorted(
        strategy_rows,
        key=lambda row: abs(float((row or {}).get("ownership_share", 0.0) or 0.0)),
        reverse=True,
    )
    strategy_id = ordered[0].get("strategy_id") if ordered else None
    return str(strategy_id) if strategy_id else None


def _dominant_signal_family(provenance: dict[str, Any] | None) -> str | None:
    strategy_rows = list((provenance or {}).get("strategy_rows") or [])
    ordered = sorted(
        strategy_rows,
        key=lambda row: abs(float((row or {}).get("ownership_share", 0.0) or 0.0)),
        reverse=True,
    )
    for row in ordered:
        family = row.get("signal_family")
        if family:
            return str(family)
    families = list((provenance or {}).get("signal_families") or [])
    return str(families[0]) if families else None


def _ev_distribution(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0,
            "mean": 0.0,
            "min": 0.0,
            "p25": 0.0,
            "median": 0.0,
            "p75": 0.0,
            "max": 0.0,
        }
    series = pd.Series([float(value) for value in values], dtype=float)
    return {
        "count": int(series.count()),
        "mean": float(series.mean()),
        "min": float(series.min()),
        "p25": float(series.quantile(0.25)),
        "median": float(series.quantile(0.50)),
        "p75": float(series.quantile(0.75)),
        "max": float(series.max()),
    }


def generate_rebalance_orders(
    *,
    as_of: str | None = None,
    state: PaperPortfolioState,
    latest_target_weights: dict[str, float],
    latest_prices: dict[str, float],
    latest_scores: dict[str, float] | None = None,
    config: PaperTradingConfig | None = None,
    min_trade_dollars: float = 25.0,
    min_weight_change_to_trade: float = 0.0,
    lot_size: int = 1,
    reserve_cash_pct: float = 0.0,
    provenance_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> OrderGenerationResult:
    equity = state.equity
    investable_equity = equity * (1.0 - reserve_cash_pct)
    if investable_equity < 0:
        raise ValueError("Investable equity cannot be negative")

    all_symbols = sorted(set(state.positions.keys()) | set(latest_target_weights.keys()))
    active_config = config or PaperTradingConfig(symbols=sorted(latest_target_weights))
    effective_latest_prices = {str(symbol): float(price) for symbol, price in latest_prices.items()}
    effective_latest_scores = {str(symbol): value for symbol, value in dict(latest_scores or {}).items()}
    exit_price_fallback_symbols: list[str] = []
    missing_price_symbols: list[str] = []
    for symbol in all_symbols:
        if symbol in effective_latest_prices and float(effective_latest_prices[symbol]) > 0.0:
            continue
        position = state.positions.get(symbol)
        fallback_price = float(position.last_price) if position is not None else 0.0
        if fallback_price > 0.0:
            effective_latest_prices[symbol] = fallback_price
            exit_price_fallback_symbols.append(symbol)
        else:
            missing_price_symbols.append(symbol)
    diagnostics: dict[str, Any] = {
        "equity": equity,
        "investable_equity": investable_equity,
        "reserve_cash_pct": reserve_cash_pct,
        "current_cash": state.cash,
        "min_weight_change_to_trade": float(min_weight_change_to_trade),
        "score_band_enabled": _score_band_enabled(active_config),
        "exit_price_fallback_symbols": exit_price_fallback_symbols,
        "missing_price_symbols": missing_price_symbols,
    }
    entry_threshold, exit_threshold = _resolve_score_band_thresholds(config=active_config)
    diagnostics["entry_threshold_used"] = entry_threshold
    diagnostics["exit_threshold_used"] = exit_threshold
    diagnostics["score_band_mode"] = "percentile" if active_config.use_percentile_thresholds else "raw_score"
    orders: list[PaperOrder] = []
    skipped_trade_rows: list[dict[str, Any]] = []
    band_decision_rows: list[dict[str, Any]] = []
    ev_prediction_rows: list[dict[str, Any]] = []
    candidate_trade_rows: list[dict[str, Any]] = []
    skipped_turnover = 0.0
    total_requested_turnover = 0.0
    blocked_entries_count = 0
    held_in_hold_zone_count = 0
    forced_exit_count = 0
    ev_gate_blocked_count = 0
    rank_lookup = _score_rank_lookup(effective_latest_scores)
    adjusted_target_weights: dict[str, float] = {}
    current_weight_lookup: dict[str, float] = {}
    requested_target_weight_lookup: dict[str, float] = {}
    for symbol in all_symbols:
        current_price = float(effective_latest_prices.get(symbol, 0.0) or 0.0)
        current_position = state.positions.get(symbol)
        current_market_value = float(current_position.quantity * current_price) if current_position is not None else 0.0
        current_weight = float(current_market_value / equity) if equity > 0.0 else 0.0
        requested_target_weight = float(latest_target_weights.get(symbol, 0.0) or 0.0)
        adjusted_target_weight, band_row = _apply_score_band_to_target(
            symbol=symbol,
            current_weight=current_weight,
            target_weight=requested_target_weight,
            latest_scores=effective_latest_scores,
            rank_lookup=rank_lookup,
            config=active_config,
            entry_threshold=entry_threshold,
            exit_threshold=exit_threshold,
        )
        band_decision_rows.append(
            {
                **band_row,
                "current_weight": current_weight,
                "requested_target_weight": requested_target_weight,
                "adjusted_target_weight": adjusted_target_weight,
            }
        )
        if band_row["band_decision"] == "blocked_entry":
            blocked_entries_count += 1
        elif band_row["band_decision"] in {"hold_zone", "hold_zone_observed"}:
            held_in_hold_zone_count += 1
        elif band_row["band_decision"] == "forced_exit":
            forced_exit_count += 1
        adjusted_target_weights[symbol] = float(adjusted_target_weight)
        current_weight_lookup[symbol] = current_weight
        requested_target_weight_lookup[symbol] = requested_target_weight

    execution_requests = build_execution_requests_from_target_weights(
        target_weights=adjusted_target_weights,
        current_positions=state.positions,
        latest_prices=effective_latest_prices,
        portfolio_equity=equity,
        reserve_cash_pct=reserve_cash_pct,
        provenance_by_symbol=provenance_by_symbol,
    )
    band_lookup = {str(row["symbol"]): row for row in band_decision_rows}
    ev_training_rows: list[dict[str, Any]] = []
    ev_model: dict[str, Any] = {}
    ev_training_summary: dict[str, Any] = {
        "training_sample_count": 0,
        "warnings": [],
        "training_available": False,
    }
    ev_gate_mode = str(getattr(active_config, "ev_gate_mode", "hard") or "hard").lower()
    ev_weight_multiplier_enabled = bool(getattr(active_config, "ev_gate_weight_multiplier", False))
    ev_weight_scale = float(getattr(active_config, "ev_gate_weight_scale", 1.0) or 0.0)
    ev_requested_training_source = str(
        getattr(active_config, "ev_gate_training_source", "executed_trades") or "executed_trades"
    ).lower()
    ev_target_type = str(getattr(active_config, "ev_gate_target_type", "market_proxy") or "market_proxy").lower()
    ev_hybrid_alpha = float(getattr(active_config, "ev_gate_hybrid_alpha", 0.8) or 0.8)
    ev_extreme_negative_threshold = getattr(active_config, "ev_gate_extreme_negative_threshold", None)
    ev_score_clip_min = getattr(active_config, "ev_gate_score_clip_min", None)
    ev_score_clip_max = getattr(active_config, "ev_gate_score_clip_max", None)
    ev_normalize_scores = bool(getattr(active_config, "ev_gate_normalize_scores", False))
    ev_normalization_method = str(getattr(active_config, "ev_gate_normalization_method", "zscore") or "zscore")
    ev_normalize_within = str(getattr(active_config, "ev_gate_normalize_within", "all_candidates") or "all_candidates")
    ev_use_normalized_score_for_weighting = bool(
        getattr(active_config, "ev_gate_use_normalized_score_for_weighting", True)
    )
    ev_weight_multiplier_min = getattr(active_config, "ev_gate_weight_multiplier_min", None)
    ev_weight_multiplier_max = getattr(active_config, "ev_gate_weight_multiplier_max", None)
    ev_use_confidence_weighting = bool(getattr(active_config, "ev_gate_use_confidence_weighting", False))
    ev_confidence_method = str(getattr(active_config, "ev_gate_confidence_method", "residual_std") or "residual_std")
    ev_confidence_scale = float(getattr(active_config, "ev_gate_confidence_scale", 1.0))
    ev_confidence_clip_min = float(getattr(active_config, "ev_gate_confidence_clip_min", 0.5))
    ev_confidence_clip_max = float(getattr(active_config, "ev_gate_confidence_clip_max", 1.5))
    ev_confidence_min_samples_per_bucket = int(
        getattr(active_config, "ev_gate_confidence_min_samples_per_bucket", 20) or 20
    )
    ev_confidence_shrinkage_enabled = bool(
        getattr(active_config, "ev_gate_confidence_shrinkage_enabled", True)
    )
    ev_confidence_component_residual_std_weight = float(
        getattr(active_config, "ev_gate_confidence_component_residual_std_weight", 1.0) or 0.0
    )
    ev_confidence_component_magnitude_weight = float(
        getattr(active_config, "ev_gate_confidence_component_magnitude_weight", 0.0) or 0.0
    )
    ev_confidence_component_model_performance_weight = float(
        getattr(active_config, "ev_gate_confidence_component_model_performance_weight", 0.0) or 0.0
    )
    ev_use_confidence_filter = bool(getattr(active_config, "ev_gate_use_confidence_filter", False))
    ev_confidence_threshold = float(getattr(active_config, "ev_gate_confidence_threshold", 0.0) or 0.0)
    requested_ev_model_type = str(getattr(active_config, "ev_gate_model_type", "bucketed_mean") or "bucketed_mean")
    execution_ev_model_type = requested_ev_model_type
    ev_model_fallback_reason: str | None = None
    if requested_ev_model_type.lower() == "regression":
        execution_ev_model_type = "bucketed_mean"
        ev_model_fallback_reason = "regression_model_is_soft_weight_only_for_now"
    if ev_extreme_negative_threshold is not None:
        ev_extreme_negative_threshold = float(ev_extreme_negative_threshold)
    if ev_weight_multiplier_min is not None:
        ev_weight_multiplier_min = float(ev_weight_multiplier_min)
    if ev_weight_multiplier_max is not None:
        ev_weight_multiplier_max = float(ev_weight_multiplier_max)
    if bool(active_config.ev_gate_enabled) and as_of:
        ev_training_rows, ev_training_summary = build_trade_ev_training_dataset(
            history_root=active_config.ev_gate_training_root,
            as_of_date=as_of,
            horizon_days=int(active_config.ev_gate_horizon_days),
            training_source=ev_requested_training_source,
            target_type=ev_target_type,
            hybrid_alpha=ev_hybrid_alpha,
        )
        if (
            ev_requested_training_source == "candidate_decisions"
            and len(ev_training_rows) < int(active_config.ev_gate_min_training_samples)
        ):
            fallback_rows, fallback_summary = build_trade_ev_training_dataset(
                history_root=active_config.ev_gate_training_root,
                as_of_date=as_of,
                horizon_days=int(active_config.ev_gate_horizon_days),
                training_source="executed_trades",
                target_type=ev_target_type,
                hybrid_alpha=ev_hybrid_alpha,
            )
            if len(fallback_rows) >= int(active_config.ev_gate_min_training_samples):
                ev_training_rows = fallback_rows
                ev_training_summary = {
                    **dict(fallback_summary or {}),
                    "requested_training_source": ev_requested_training_source,
                    "training_source": "executed_trades",
                    "fallback_reason": "insufficient_candidate_history_for_ev_gate",
                }
        ev_model = train_trade_ev_model(
            training_rows=ev_training_rows,
            model_type=execution_ev_model_type,
            min_training_samples=int(active_config.ev_gate_min_training_samples),
        )
        ev_training_summary = {
            **dict(ev_training_summary or {}),
            "requested_training_source": ev_requested_training_source,
            "training_source": str((ev_training_summary or {}).get("training_source") or ev_requested_training_source),
            "requested_model_type": requested_ev_model_type,
            "model_type": execution_ev_model_type,
            "training_available": bool(ev_model.get("training_available", False)),
            "training_sample_count": int(ev_model.get("training_sample_count", len(ev_training_rows))),
        }
        if requested_ev_model_type.lower() == "regression":
            ev_training_summary["fallback_reason"] = "regression_model_is_diagnostics_only_for_now"
    regression_training_rows: list[dict[str, Any]] = []
    regression_training_summary: dict[str, Any] = {}
    regression_model: dict[str, Any] = {}
    regression_prediction_lookup: dict[str, dict[str, Any]] = {}
    diagnostics["ev_gate_enabled"] = bool(active_config.ev_gate_enabled)
    diagnostics["ev_gate_training_summary"] = ev_training_summary
    diagnostics["ev_gate_model_type"] = str(execution_ev_model_type)
    diagnostics["ev_gate_requested_model_type"] = str(requested_ev_model_type)
    diagnostics["ev_model_type_requested"] = str(requested_ev_model_type)
    diagnostics["ev_model_type_used"] = str(execution_ev_model_type)
    diagnostics["ev_model_fallback_reason"] = ev_model_fallback_reason
    diagnostics["ev_gate_target_type"] = ev_target_type
    diagnostics["ev_gate_hybrid_alpha"] = ev_hybrid_alpha
    diagnostics["ev_gate_mode"] = ev_gate_mode
    diagnostics["ev_gate_weight_multiplier"] = ev_weight_multiplier_enabled
    diagnostics["ev_gate_weight_scale"] = ev_weight_scale
    diagnostics["ev_gate_extreme_negative_threshold"] = ev_extreme_negative_threshold
    diagnostics["ev_gate_score_clip_min"] = ev_score_clip_min
    diagnostics["ev_gate_score_clip_max"] = ev_score_clip_max
    diagnostics["ev_gate_normalize_scores"] = ev_normalize_scores
    diagnostics["ev_gate_normalization_method"] = ev_normalization_method
    diagnostics["ev_gate_normalize_within"] = ev_normalize_within
    diagnostics["ev_gate_use_normalized_score_for_weighting"] = ev_use_normalized_score_for_weighting
    diagnostics["ev_gate_weight_multiplier_min"] = ev_weight_multiplier_min
    diagnostics["ev_gate_weight_multiplier_max"] = ev_weight_multiplier_max
    diagnostics["ev_gate_use_confidence_weighting"] = ev_use_confidence_weighting
    diagnostics["ev_gate_confidence_method"] = ev_confidence_method
    diagnostics["ev_gate_confidence_scale"] = ev_confidence_scale
    diagnostics["ev_gate_confidence_clip_min"] = ev_confidence_clip_min
    diagnostics["ev_gate_confidence_clip_max"] = ev_confidence_clip_max
    diagnostics["ev_gate_confidence_min_samples_per_bucket"] = ev_confidence_min_samples_per_bucket
    diagnostics["ev_gate_confidence_shrinkage_enabled"] = ev_confidence_shrinkage_enabled
    diagnostics["ev_gate_confidence_component_residual_std_weight"] = (
        ev_confidence_component_residual_std_weight
    )
    diagnostics["ev_gate_confidence_component_magnitude_weight"] = ev_confidence_component_magnitude_weight
    diagnostics["ev_gate_confidence_component_model_performance_weight"] = (
        ev_confidence_component_model_performance_weight
    )
    diagnostics["ev_gate_use_confidence_filter"] = ev_use_confidence_filter
    diagnostics["ev_gate_confidence_threshold"] = ev_confidence_threshold
    diagnostics["ev_gate_training_source"] = str(
        (ev_training_summary or {}).get("training_source") or ev_requested_training_source
    )
    ev_executed_expected_net_returns: list[float] = []
    ev_blocked_expected_net_returns: list[float] = []
    ev_adjusted_exposures: list[float] = []
    ev_executed_multipliers: list[float] = []
    ev_executed_raw_scores: list[float] = []
    ev_executed_normalized_scores: list[float] = []
    ev_executed_weighting_scores: list[float] = []
    regression_prediction_available_count = 0
    regression_prediction_missing_count = 0
    regression_executed_scores: list[float] = []
    regression_executed_weighting_scores: list[float] = []
    regression_adjusted_exposures: list[float] = []
    ev_executed_confidences: list[float] = []
    ev_executed_confidence_multipliers: list[float] = []
    ev_executed_scores_before_confidence: list[float] = []
    ev_executed_scores_after_confidence: list[float] = []
    ev_calibration_rows: list[dict[str, Any]] = []
    ev_calibration_summary: dict[str, Any] = {}
    confidence_filtered_count = 0
    market_feature_cache: dict[str, pd.DataFrame] = {}
    request_contexts: list[dict[str, Any]] = []
    candidate_row_by_symbol: dict[str, dict[str, Any]] = {}
    request_symbols: set[str] = set()
    for symbol in all_symbols:
        current_price = float(effective_latest_prices.get(symbol, 0.0) or 0.0)
        current_weight = float(current_weight_lookup.get(symbol, 0.0) or 0.0)
        requested_target_weight = float(requested_target_weight_lookup.get(symbol, 0.0) or 0.0)
        adjusted_target_weight = float(adjusted_target_weights.get(symbol, requested_target_weight) or 0.0)
        requested_weight_delta = float(requested_target_weight - current_weight)
        adjusted_weight_delta = float(adjusted_target_weight - current_weight)
        band_row = dict(band_lookup.get(symbol) or {})
        if (
            abs(requested_weight_delta) <= 1e-12
            and str(band_row.get("band_decision") or "") not in {"blocked_entry", "hold_zone", "hold_zone_observed"}
        ):
            continue
        market_features = (
            build_trade_ev_candidate_market_features(
                symbol=symbol,
                as_of_date=as_of,
                frame_cache=market_feature_cache,
            )
            if as_of
            else {}
        )
        estimated_execution_cost_pct = 0.0
        current_quantity = int(state.positions.get(symbol).quantity if state.positions.get(symbol) is not None else 0)
        if current_price > 0.0 and abs(requested_weight_delta) > 1e-12:
            requested_target_shares = int((equity * (1.0 - reserve_cash_pct) * requested_target_weight) / current_price)
            requested_delta_shares = int(requested_target_shares - current_quantity)
            if requested_delta_shares != 0:
                provisional_order = apply_order_slippage(
                    PaperOrder(
                        symbol=symbol,
                        side="BUY" if requested_delta_shares > 0 else "SELL",
                        quantity=int(abs(requested_delta_shares)),
                        reference_price=float(current_price),
                        target_weight=requested_target_weight,
                        current_quantity=current_quantity,
                        target_quantity=int(requested_target_shares),
                        notional=float(abs(requested_delta_shares)) * float(current_price),
                        reason="candidate_pre_execution_filter",
                        provenance=dict((provenance_by_symbol or {}).get(symbol) or {}),
                    ),
                    active_config,
                )
                gross_notional = abs(float(provisional_order.expected_gross_notional or 0.0))
                if gross_notional > 0.0:
                    estimated_execution_cost_pct = float(provisional_order.expected_total_execution_cost / gross_notional)
        candidate_row = {
            "date": as_of,
            "symbol": symbol,
            "strategy_id": _dominant_strategy_id((provenance_by_symbol or {}).get(symbol)),
            "signal_family": _dominant_signal_family((provenance_by_symbol or {}).get(symbol)),
            "signal_score": band_row.get("score_value"),
            "score_rank": band_row.get("score_rank"),
            "score_percentile": band_row.get("score_percentile"),
            "expected_horizon_days": int(active_config.ev_gate_horizon_days or 5),
            "current_weight": current_weight,
            "target_weight": adjusted_target_weight,
            "weight_delta": adjusted_weight_delta,
            "requested_target_weight": requested_target_weight,
            "requested_weight_delta": requested_weight_delta,
            "adjusted_target_weight": adjusted_target_weight,
            "adjusted_weight_delta": adjusted_weight_delta,
            "action": "buy" if requested_weight_delta > 0.0 else ("sell" if requested_weight_delta < 0.0 else "hold"),
            "action_type": (
                "entry"
                if abs(current_weight) <= 1e-12 and abs(requested_target_weight) > 1e-12
                else (
                    "exit"
                    if abs(current_weight) > 1e-12 and abs(requested_target_weight) <= 1e-12
                    else ("increase" if requested_weight_delta > 0.0 else ("reduction" if requested_weight_delta < 0.0 else "hold"))
                )
            ),
            "current_position_held": int(abs(current_weight) > 1e-12),
            "estimated_execution_cost_pct": estimated_execution_cost_pct,
            "recent_return_3d": float(market_features.get("recent_return_3d", 0.0) or 0.0),
            "recent_return_5d": float(market_features.get("recent_return_5d", 0.0) or 0.0),
            "recent_return_10d": float(market_features.get("recent_return_10d", 0.0) or 0.0),
            "recent_vol_20d": float(market_features.get("recent_vol_20d", 0.0) or 0.0),
            "dollar_volume": float(market_features.get("dollar_volume", 0.0) or 0.0),
            "candidate_status": "considered",
            "candidate_outcome": "pending",
            "candidate_stage": "pre_execution_filter",
            "skip_reason": None,
            "action_reason": band_row.get("action_reason"),
            "band_decision": band_row.get("band_decision"),
            "entry_threshold": band_row.get("entry_threshold"),
            "exit_threshold": band_row.get("exit_threshold"),
            "score_band_enabled": bool(diagnostics["score_band_enabled"]),
            "ev_gate_enabled": bool(active_config.ev_gate_enabled),
            "ev_gate_mode": ev_gate_mode,
            "ev_gate_decision": None,
            "probability_positive": None,
        }
        candidate_trade_rows.append(candidate_row)
        candidate_row_by_symbol[symbol] = candidate_row
    for request in execution_requests:
        request_symbols.add(str(request.symbol))
        current_price = float(effective_latest_prices.get(request.symbol, request.price) or request.price or 0.0)
        current_weight = float(current_weight_lookup.get(request.symbol, 0.0) or 0.0)
        requested_target_weight = float(requested_target_weight_lookup.get(request.symbol, request.target_weight) or 0.0)
        band_row = dict(band_lookup.get(request.symbol) or {})
        target_weight = float(request.target_weight)
        weight_delta = float(target_weight - current_weight)
        temp_order = apply_order_slippage(
            PaperOrder(
                symbol=request.symbol,
                side=request.side,
                quantity=int(abs(request.requested_shares)),
                reference_price=float(current_price),
                target_weight=target_weight,
                current_quantity=int(request.current_shares),
                target_quantity=int(request.target_shares),
                notional=float(abs(request.requested_shares)) * float(current_price),
                reason="rebalance_to_target",
                provenance=dict(request.provenance or {}),
            ),
            active_config,
        )
        estimated_cost_pct = (
            float(temp_order.expected_total_execution_cost / abs(temp_order.expected_gross_notional))
            if abs(float(temp_order.expected_gross_notional or 0.0)) > 0.0
            else 0.0
        )
        market_features = (
            build_trade_ev_candidate_market_features(
                symbol=request.symbol,
                as_of_date=as_of,
                frame_cache=market_feature_cache,
            )
            if as_of
            else {}
        )
        candidate_row = {
            "date": as_of,
            "symbol": request.symbol,
            "strategy_id": _dominant_strategy_id(request.provenance),
            "signal_family": _dominant_signal_family(request.provenance),
            "signal_score": band_row.get("score_value"),
            "score_rank": band_row.get("score_rank"),
            "score_percentile": band_row.get("score_percentile"),
            "expected_horizon_days": int(active_config.ev_gate_horizon_days or 5),
            "current_weight": current_weight,
            "target_weight": target_weight,
            "weight_delta": weight_delta,
            "action": request.side.lower(),
            "action_type": (
                "entry"
                if abs(current_weight) <= 1e-12 and abs(target_weight) > 1e-12
                else (
                    "exit"
                    if abs(current_weight) > 1e-12 and abs(target_weight) <= 1e-12
                    else ("increase" if weight_delta > 0.0 else ("reduction" if weight_delta < 0.0 else "hold"))
                )
            ),
            "current_position_held": int(abs(request.current_shares) > 0),
            "estimated_execution_cost_pct": estimated_cost_pct,
            "recent_return_3d": float(market_features.get("recent_return_3d", 0.0) or 0.0),
            "recent_return_5d": float(market_features.get("recent_return_5d", 0.0) or 0.0),
            "recent_return_10d": float(market_features.get("recent_return_10d", 0.0) or 0.0),
            "recent_vol_20d": float(market_features.get("recent_vol_20d", 0.0) or 0.0),
            "dollar_volume": float(market_features.get("dollar_volume", 0.0) or 0.0),
        }
        request_contexts.append(
            {
                "request": request,
                "current_price": current_price,
                "current_weight": current_weight,
                "requested_target_weight": requested_target_weight,
                "band_row": band_row,
                "target_weight": target_weight,
                "weight_delta": weight_delta,
                "candidate_row": candidate_row,
            }
        )
    regression_requested_for_soft_weight = (
        bool(active_config.ev_gate_enabled)
        and requested_ev_model_type.lower() == "regression"
        and ev_gate_mode == "soft"
        and bool(active_config.ev_gate_weight_multiplier)
    )
    ev_prediction_lookup: dict[str, dict[str, Any]] = {}
    if (
        bool(active_config.ev_gate_enabled)
        and bool(ev_model.get("training_available", False))
        and request_contexts
    ):
        scored_predictions = score_trade_ev_candidates(
            model=ev_model,
            candidate_rows=[dict(row["candidate_row"]) for row in request_contexts],
            min_expected_net_return=float(active_config.ev_gate_min_expected_net_return),
            min_probability_positive=active_config.ev_gate_min_probability_positive,
            risk_penalty_lambda=float(active_config.ev_gate_risk_penalty_lambda),
            score_clip_min=ev_score_clip_min,
            score_clip_max=ev_score_clip_max,
            normalize_scores=ev_normalize_scores,
            normalization_method=ev_normalization_method,
            normalize_within=ev_normalize_within,
            use_normalized_score_for_weighting=ev_use_normalized_score_for_weighting,
        )
        ev_prediction_lookup = {str(row.get("symbol")): dict(row) for row in scored_predictions if row.get("symbol")}
    if regression_requested_for_soft_weight and as_of and request_contexts:
        regression_training_rows, regression_training_summary = build_trade_ev_regression_history_dataset(
            history_root=active_config.ev_gate_training_root,
            as_of_date=as_of,
            expected_horizon_days=int(active_config.ev_gate_horizon_days or 5),
        )
        regression_model = train_trade_ev_regression_model(
            training_rows=regression_training_rows,
            min_training_samples=int(active_config.ev_gate_min_training_samples),
            confidence_min_samples_per_bucket=ev_confidence_min_samples_per_bucket,
            confidence_shrinkage_enabled=ev_confidence_shrinkage_enabled,
        )
        if bool(regression_model.get("training_available", False)):
            regression_predictions = score_trade_ev_regression_candidates(
                model=regression_model,
                candidate_rows=[dict(row["candidate_row"]) for row in request_contexts],
                use_confidence_weighting=ev_use_confidence_weighting and ev_confidence_method == "residual_std",
                confidence_scale=ev_confidence_scale,
                confidence_clip_min=ev_confidence_clip_min,
                confidence_clip_max=ev_confidence_clip_max,
                confidence_min_samples_per_bucket=ev_confidence_min_samples_per_bucket,
                confidence_shrinkage_enabled=ev_confidence_shrinkage_enabled,
                confidence_component_residual_std_weight=ev_confidence_component_residual_std_weight,
                confidence_component_magnitude_weight=ev_confidence_component_magnitude_weight,
                confidence_component_model_performance_weight=ev_confidence_component_model_performance_weight,
                score_clip_min=ev_score_clip_min,
                score_clip_max=ev_score_clip_max,
                normalize_scores=ev_normalize_scores,
                normalization_method=ev_normalization_method,
                normalize_within=ev_normalize_within,
                use_normalized_score_for_weighting=ev_use_normalized_score_for_weighting,
            )
            regression_prediction_lookup = {
                str(row.get("symbol")): dict(row) for row in regression_predictions if row.get("symbol")
            }
        else:
            ev_model_fallback_reason = "regression_predictions_unavailable"
    if bool(active_config.ev_gate_enabled) and bool(ev_model.get("training_available", False)) and ev_training_rows:
        training_predictions = score_trade_ev_candidates(
            model=ev_model,
            candidate_rows=[dict(row) for row in ev_training_rows],
            min_expected_net_return=float(active_config.ev_gate_min_expected_net_return),
            min_probability_positive=active_config.ev_gate_min_probability_positive,
            risk_penalty_lambda=float(active_config.ev_gate_risk_penalty_lambda),
            score_clip_min=ev_score_clip_min,
            score_clip_max=ev_score_clip_max,
            normalize_scores=False,
            normalization_method=ev_normalization_method,
            normalize_within=ev_normalize_within,
            use_normalized_score_for_weighting=ev_use_normalized_score_for_weighting,
        )
        ev_calibration_rows, ev_calibration_summary = build_trade_ev_calibration(
            prediction_rows=[
                {
                    **row,
                    "realized_gross_return": float(row.get("forward_gross_return", 0.0) or 0.0),
                    "realized_net_return": float(row.get("forward_net_return", 0.0) or 0.0),
                    "execution_cost": float(row.get("estimated_execution_cost_pct", 0.0) or 0.0),
                    "ev_weight_multiplier": 1.0,
                }
                for row in training_predictions
            ]
        )
    for context in request_contexts:
        request = context["request"]
        current_price = float(context["current_price"])
        current_weight = float(context["current_weight"])
        requested_target_weight = float(context["requested_target_weight"])
        band_row = dict(context["band_row"])
        target_weight = float(context["target_weight"])
        weight_delta = float(context["weight_delta"])
        ev_prediction: dict[str, Any] | None = None
        regression_prediction: dict[str, Any] | None = None
        effective_request = request
        ev_adjusted_target_weight = target_weight
        ev_adjusted_weight_delta = weight_delta
        ev_weight_multiplier = 1.0
        ev_model_type_used = execution_ev_model_type
        local_ev_fallback_reason = ev_model_fallback_reason
        if bool(active_config.ev_gate_enabled) and bool(ev_model.get("training_available", False)):
            ev_prediction = dict(ev_prediction_lookup.get(request.symbol) or {})
            ev_prediction["ev_gate_mode"] = ev_gate_mode
            ev_prediction["ev_weight_multiplier"] = 1.0
            ev_prediction["ev_adjusted_target_weight"] = target_weight
            ev_prediction["ev_adjusted_weight_delta"] = weight_delta
            ev_prediction["ev_confidence"] = 1.0
            ev_prediction["ev_confidence_multiplier"] = 1.0
            ev_prediction["ev_score_before_confidence"] = ev_prediction.get("ev_weighting_score", ev_prediction.get("ev_decision_score", 0.0))
            ev_prediction["ev_score_after_confidence"] = ev_prediction.get("ev_weighting_score", ev_prediction.get("ev_decision_score", 0.0))
            ev_score = float(ev_prediction.get("ev_decision_score", 0.0) or 0.0)
            ev_weighting_score = float(ev_prediction.get("ev_weighting_score", ev_score) or ev_score)
            if regression_requested_for_soft_weight:
                regression_prediction = dict(regression_prediction_lookup.get(request.symbol) or {})
                if bool(regression_prediction.get("prediction_available", False)):
                    regression_prediction_available_count += 1
                    ev_weighting_score = float(
                        regression_prediction.get("ev_weighting_score", regression_prediction.get("predicted_ev", 0.0))
                        or 0.0
                    )
                    ev_model_type_used = "regression"
                    local_ev_fallback_reason = None
                else:
                    regression_prediction_missing_count += 1
                    if local_ev_fallback_reason is None:
                        local_ev_fallback_reason = "regression_prediction_missing_for_candidate"
            ev_prediction["ev_weighting_score"] = ev_weighting_score
            if regression_prediction is not None:
                ev_prediction["ev_confidence"] = float(regression_prediction.get("ev_confidence", 1.0))
                ev_prediction["ev_confidence_multiplier"] = float(
                    regression_prediction.get("ev_confidence_multiplier", 1.0)
                )
                ev_prediction["ev_score_before_confidence"] = float(
                    regression_prediction.get("ev_score_before_confidence", ev_weighting_score) or ev_weighting_score
                )
                ev_prediction["ev_score_after_confidence"] = float(
                    regression_prediction.get("ev_score_after_confidence", ev_weighting_score) or ev_weighting_score
                )
                ev_prediction["residual_std_used"] = regression_prediction.get("residual_std_used")
                ev_prediction["confidence_source"] = regression_prediction.get("confidence_source")
            if ev_gate_mode == "soft":
                if ev_weight_multiplier_enabled:
                    ev_weight_multiplier = max(0.0, 1.0 + (ev_weight_scale * ev_weighting_score))
                    if ev_weight_multiplier_min is not None:
                        ev_weight_multiplier = max(ev_weight_multiplier, ev_weight_multiplier_min)
                    if ev_weight_multiplier_max is not None:
                        ev_weight_multiplier = min(ev_weight_multiplier, ev_weight_multiplier_max)
                ev_adjusted_target_weight = float(target_weight * ev_weight_multiplier)
                ev_adjusted_weight_delta = float(ev_adjusted_target_weight - current_weight)
                target_shares = int((equity * (1.0 - reserve_cash_pct) * ev_adjusted_target_weight) / current_price) if current_price > 0.0 else 0
                requested_delta = int(target_shares - request.current_shares)
                effective_request = replace(
                    request,
                    side="BUY" if requested_delta > 0 else "SELL",
                    requested_shares=abs(requested_delta),
                    requested_notional=float(abs(requested_delta) * current_price),
                    target_shares=target_shares,
                    target_weight=ev_adjusted_target_weight,
                    provenance={
                        **dict(request.provenance or {}),
                        "ev_weight_multiplier": ev_weight_multiplier,
                        "ev_adjusted_target_weight": ev_adjusted_target_weight,
                        "ev_adjusted_weight_delta": ev_adjusted_weight_delta,
                        "ev_weighting_score": ev_weighting_score,
                        "ev_confidence": ev_prediction.get("ev_confidence"),
                        "ev_confidence_multiplier": ev_prediction.get("ev_confidence_multiplier"),
                        "ev_score_before_confidence": ev_prediction.get("ev_score_before_confidence"),
                        "ev_score_after_confidence": ev_prediction.get("ev_score_after_confidence"),
                    },
                )
                ev_prediction["ev_weight_multiplier"] = ev_weight_multiplier
                ev_prediction["ev_adjusted_target_weight"] = ev_adjusted_target_weight
                ev_prediction["ev_adjusted_weight_delta"] = ev_adjusted_weight_delta
                ev_prediction["ev_model_type_requested"] = requested_ev_model_type
                ev_prediction["ev_model_type_used"] = ev_model_type_used
                ev_prediction["ev_model_fallback_reason"] = local_ev_fallback_reason
                if regression_prediction is not None:
                    ev_prediction["regression_raw_ev_score"] = regression_prediction.get("regression_raw_ev_score")
                    ev_prediction["regression_normalized_ev_score"] = regression_prediction.get(
                        "regression_normalized_ev_score"
                    )
                    ev_prediction["regression_ev_score_post_clip"] = regression_prediction.get(
                        "regression_ev_score_post_clip"
                    )
                if abs(ev_adjusted_target_weight - target_weight) <= 1e-12:
                    ev_prediction["ev_gate_decision"] = "allow"
                    ev_prediction["action_reason"] = "passed_ev_soft_gate"
                elif ev_adjusted_target_weight == 0.0:
                    ev_prediction["ev_gate_decision"] = "scale_to_zero"
                    ev_prediction["action_reason"] = "scaled_to_zero_by_ev_gate"
                elif abs(ev_weight_multiplier - 1.0) <= 1e-12:
                    ev_prediction["ev_gate_decision"] = "allow"
                    ev_prediction["action_reason"] = "passed_ev_soft_gate"
                elif ev_weight_multiplier > 1.0:
                    ev_prediction["ev_gate_decision"] = "scale_up"
                    ev_prediction["action_reason"] = "scaled_up_by_ev_gate"
                else:
                    ev_prediction["ev_gate_decision"] = "scale_down"
                    ev_prediction["action_reason"] = "scaled_down_by_ev_gate"
            if ev_extreme_negative_threshold is not None and ev_score < ev_extreme_negative_threshold:
                ev_prediction["ev_gate_decision"] = "block"
                ev_prediction["action_reason"] = "blocked_by_ev_gate"
            ev_prediction_rows.append(ev_prediction)
            if request.symbol in candidate_row_by_symbol:
                candidate_row_by_symbol[request.symbol]["ev_gate_decision"] = ev_prediction.get("ev_gate_decision")
                candidate_row_by_symbol[request.symbol]["probability_positive"] = ev_prediction.get(
                    "probability_positive"
                )
                candidate_row_by_symbol[request.symbol]["raw_ev_score"] = ev_prediction.get("raw_ev_score")
                candidate_row_by_symbol[request.symbol]["normalized_ev_score"] = ev_prediction.get(
                    "normalized_ev_score"
                )
                candidate_row_by_symbol[request.symbol]["ev_score_pre_clip"] = ev_prediction.get("ev_score_pre_clip")
                candidate_row_by_symbol[request.symbol]["ev_score_post_clip"] = ev_prediction.get("ev_score_post_clip")
                candidate_row_by_symbol[request.symbol]["ev_score_clipped"] = ev_prediction.get("ev_score_clipped")
                candidate_row_by_symbol[request.symbol]["ev_weighting_score"] = ev_prediction.get(
                    "ev_weighting_score"
                )
                candidate_row_by_symbol[request.symbol]["ev_confidence"] = ev_prediction.get("ev_confidence")
                candidate_row_by_symbol[request.symbol]["ev_confidence_multiplier"] = ev_prediction.get(
                    "ev_confidence_multiplier"
                )
                candidate_row_by_symbol[request.symbol]["ev_score_before_confidence"] = ev_prediction.get(
                    "ev_score_before_confidence"
                )
                candidate_row_by_symbol[request.symbol]["ev_score_after_confidence"] = ev_prediction.get(
                    "ev_score_after_confidence"
                )
                candidate_row_by_symbol[request.symbol]["residual_std_bucket"] = ev_prediction.get(
                    "residual_std_bucket"
                )
                candidate_row_by_symbol[request.symbol]["residual_std_global"] = ev_prediction.get(
                    "residual_std_global"
                )
                candidate_row_by_symbol[request.symbol]["residual_std_final"] = ev_prediction.get(
                    "residual_std_final"
                )
                candidate_row_by_symbol[request.symbol]["sample_size_used"] = ev_prediction.get("sample_size_used")
                candidate_row_by_symbol[request.symbol]["residual_std_confidence"] = ev_prediction.get(
                    "residual_std_confidence"
                )
                candidate_row_by_symbol[request.symbol]["magnitude_confidence"] = ev_prediction.get(
                    "magnitude_confidence"
                )
                candidate_row_by_symbol[request.symbol]["model_performance_confidence"] = ev_prediction.get(
                    "model_performance_confidence"
                )
                candidate_row_by_symbol[request.symbol]["combined_confidence"] = ev_prediction.get(
                    "combined_confidence"
                )
                candidate_row_by_symbol[request.symbol]["normalization_method"] = ev_prediction.get(
                    "normalization_method"
                )
                candidate_row_by_symbol[request.symbol]["normalize_within"] = ev_prediction.get("normalize_within")
                candidate_row_by_symbol[request.symbol]["candidate_count_for_normalization"] = ev_prediction.get(
                    "candidate_count_for_normalization"
                )
                candidate_row_by_symbol[request.symbol]["ev_model_type_requested"] = requested_ev_model_type
                candidate_row_by_symbol[request.symbol]["ev_model_type_used"] = ev_model_type_used
                candidate_row_by_symbol[request.symbol]["ev_model_fallback_reason"] = local_ev_fallback_reason
                candidate_row_by_symbol[request.symbol]["regression_raw_ev_score"] = (
                    regression_prediction.get("regression_raw_ev_score") if regression_prediction else None
                )
                candidate_row_by_symbol[request.symbol]["regression_normalized_ev_score"] = (
                    regression_prediction.get("regression_normalized_ev_score") if regression_prediction else None
                )
                candidate_row_by_symbol[request.symbol]["regression_ev_score_post_clip"] = (
                    regression_prediction.get("regression_ev_score_post_clip") if regression_prediction else None
                )
            if (
                regression_requested_for_soft_weight
                and ev_use_confidence_filter
                and float(ev_prediction.get("ev_confidence", 1.0)) < ev_confidence_threshold
            ):
                confidence_filtered_count += 1
                ev_prediction["was_filtered_by_confidence"] = True
                ev_prediction["ev_gate_decision"] = "confidence_filter_block"
                ev_prediction["action_reason"] = "filtered_by_confidence"
                if request.symbol in candidate_row_by_symbol:
                    candidate_row_by_symbol[request.symbol]["candidate_status"] = "skipped"
                    candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "confidence_filtered"
                    candidate_row_by_symbol[request.symbol]["candidate_stage"] = "confidence_filter"
                    candidate_row_by_symbol[request.symbol]["skip_reason"] = "below_confidence_threshold"
                    candidate_row_by_symbol[request.symbol]["action_reason"] = "filtered_by_confidence"
                    candidate_row_by_symbol[request.symbol]["was_filtered_by_confidence"] = True
                skipped_trade_rows.append(
                    {
                        "symbol": request.symbol,
                        "score_value": band_row.get("score_value"),
                        "score_rank": band_row.get("score_rank"),
                        "entry_threshold": band_row.get("entry_threshold"),
                        "exit_threshold": band_row.get("exit_threshold"),
                        "band_decision": band_row.get("band_decision"),
                        "current_weight": current_weight,
                        "target_weight": target_weight,
                        "weight_delta": weight_delta,
                        "requested_notional": float(request.requested_notional),
                        "skip_reason": "below_confidence_threshold",
                        "action_reason": "filtered_by_confidence",
                        "ev_confidence": ev_prediction.get("ev_confidence"),
                    }
                )
                continue
            if ev_prediction["ev_gate_decision"] == "block":
                ev_gate_blocked_count += 1
                ev_blocked_expected_net_returns.append(float(ev_prediction.get("expected_net_return", 0.0) or 0.0))
                if request.symbol in candidate_row_by_symbol:
                    candidate_row_by_symbol[request.symbol]["candidate_status"] = "skipped"
                    candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "ev_gate_blocked"
                    candidate_row_by_symbol[request.symbol]["candidate_stage"] = "ev_gate"
                    candidate_row_by_symbol[request.symbol]["skip_reason"] = "ev_gate_blocked"
                    candidate_row_by_symbol[request.symbol]["action_reason"] = "blocked_by_ev_gate"
                skipped_trade_rows.append(
                    {
                        "symbol": request.symbol,
                        "score_value": band_row.get("score_value"),
                        "score_rank": band_row.get("score_rank"),
                        "entry_threshold": band_row.get("entry_threshold"),
                        "exit_threshold": band_row.get("exit_threshold"),
                        "band_decision": band_row.get("band_decision"),
                        "current_weight": current_weight,
                        "target_weight": target_weight,
                        "weight_delta": weight_delta,
                        "requested_notional": float(request.requested_notional),
                        "skip_reason": "ev_gate_blocked",
                        "action_reason": "blocked_by_ev_gate",
                        "expected_net_return": ev_prediction.get("expected_net_return"),
                        "probability_positive": ev_prediction.get("probability_positive"),
                    }
                )
                continue
        adjusted_target_weights[request.symbol] = float(ev_adjusted_target_weight)
        effective_target_weight = float(effective_request.target_weight)
        effective_weight_delta = float(effective_target_weight - current_weight)
        if abs(effective_request.requested_shares) == 0 and abs(effective_weight_delta) <= 1e-12:
            if request.symbol in candidate_row_by_symbol:
                candidate_row_by_symbol[request.symbol]["candidate_status"] = "skipped"
                candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "scaled_to_zero"
                candidate_row_by_symbol[request.symbol]["candidate_stage"] = "ev_gate"
                candidate_row_by_symbol[request.symbol]["skip_reason"] = "ev_scaled_to_zero"
                candidate_row_by_symbol[request.symbol]["action_reason"] = str(
                    (ev_prediction or {}).get("action_reason") or "scaled_to_zero_by_ev_gate"
                )
            continue
        total_requested_turnover += abs(effective_weight_delta)
        if abs(effective_weight_delta) < float(min_weight_change_to_trade or 0.0):
            skipped_turnover += abs(effective_weight_delta)
            if request.symbol in candidate_row_by_symbol:
                candidate_row_by_symbol[request.symbol]["candidate_status"] = "skipped"
                candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "hysteresis_blocked"
                candidate_row_by_symbol[request.symbol]["candidate_stage"] = "hysteresis"
                candidate_row_by_symbol[request.symbol]["skip_reason"] = "below_min_weight_change_to_trade"
                candidate_row_by_symbol[request.symbol]["action_reason"] = "skipped_small_weight_delta"
            skipped_trade_rows.append(
                {
                    "symbol": request.symbol,
                    "score_value": band_row.get("score_value"),
                    "score_rank": band_row.get("score_rank"),
                    "entry_threshold": band_row.get("entry_threshold"),
                    "exit_threshold": band_row.get("exit_threshold"),
                    "band_decision": band_row.get("band_decision"),
                    "current_weight": current_weight,
                    "target_weight": effective_target_weight,
                    "weight_delta": effective_weight_delta,
                    "requested_notional": float(effective_request.requested_notional),
                    "skip_reason": "below_min_weight_change_to_trade",
                    "action_reason": "skipped_small_weight_delta",
                }
            )
            continue
        if abs(effective_target_weight) <= 1e-12 and abs(current_weight) <= 1e-12:
            if request.symbol in candidate_row_by_symbol:
                candidate_row_by_symbol[request.symbol]["candidate_status"] = "skipped"
                candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "not_actionable"
                candidate_row_by_symbol[request.symbol]["candidate_stage"] = "post_adjustment"
                candidate_row_by_symbol[request.symbol]["skip_reason"] = "zero_effective_target"
            continue
        notional = float(effective_request.requested_notional)
        if notional < min_trade_dollars:
            if request.symbol in candidate_row_by_symbol:
                candidate_row_by_symbol[request.symbol]["candidate_status"] = "skipped"
                candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "below_min_trade_dollars"
                candidate_row_by_symbol[request.symbol]["candidate_stage"] = "min_trade"
                candidate_row_by_symbol[request.symbol]["skip_reason"] = "below_min_trade_dollars"
                candidate_row_by_symbol[request.symbol]["action_reason"] = "below_min_trade_dollars"
            continue
        target_quantity = (
            (int(effective_request.target_shares) // lot_size) * lot_size
            if lot_size > 0
            else int(effective_request.target_shares)
        )
        if ev_prediction is not None:
            ev_executed_expected_net_returns.append(float(ev_prediction.get("expected_net_return", 0.0) or 0.0))
            ev_adjusted_exposures.append(abs(effective_target_weight))
            ev_executed_multipliers.append(float(ev_prediction.get("ev_weight_multiplier", 1.0) or 1.0))
            ev_executed_raw_scores.append(float(ev_prediction.get("raw_ev_score", 0.0) or 0.0))
            ev_executed_normalized_scores.append(float(ev_prediction.get("normalized_ev_score", 0.0) or 0.0))
            ev_executed_weighting_scores.append(float(ev_prediction.get("ev_weighting_score", 0.0) or 0.0))
            ev_executed_confidences.append(float(ev_prediction.get("ev_confidence", 1.0)))
            ev_executed_confidence_multipliers.append(float(ev_prediction.get("ev_confidence_multiplier", 1.0)))
            ev_executed_scores_before_confidence.append(
                float(ev_prediction.get("ev_score_before_confidence", ev_prediction.get("ev_weighting_score", 0.0)) or 0.0)
            )
            ev_executed_scores_after_confidence.append(
                float(ev_prediction.get("ev_score_after_confidence", ev_prediction.get("ev_weighting_score", 0.0)) or 0.0)
            )
            if ev_model_type_used == "regression":
                regression_executed_scores.append(
                    float((regression_prediction or {}).get("regression_raw_ev_score", 0.0) or 0.0)
                )
                regression_executed_weighting_scores.append(float(ev_prediction.get("ev_weighting_score", 0.0) or 0.0))
                regression_adjusted_exposures.append(abs(effective_target_weight))
        orders.append(
            PaperOrder(
                symbol=effective_request.symbol,
                side=effective_request.side,
                quantity=int(effective_request.requested_shares),
                reference_price=float(effective_request.price),
                target_weight=float(effective_request.target_weight),
                current_quantity=int(effective_request.current_shares),
                target_quantity=int(target_quantity),
                notional=notional,
                reason="rebalance_to_target",
                provenance={
                    **dict(effective_request.provenance or {}),
                    "current_weight": current_weight,
                    "target_weight": effective_target_weight,
                    "weight_delta": effective_weight_delta,
                    "requested_target_weight": requested_target_weight,
                    "ev_requested_target_weight": target_weight,
                    "ev_weight_multiplier": ev_weight_multiplier,
                    "ev_adjusted_target_weight": ev_adjusted_target_weight,
                    "ev_adjusted_weight_delta": ev_adjusted_weight_delta,
                    "score_value": band_row.get("score_value"),
                    "score_rank": band_row.get("score_rank"),
                    "score_percentile": band_row.get("score_percentile"),
                    "entry_threshold": band_row.get("entry_threshold"),
                    "exit_threshold": band_row.get("exit_threshold"),
                    "band_decision": band_row.get("band_decision"),
                    "action_reason": band_row.get("action_reason"),
                    "expected_gross_return": (ev_prediction or {}).get("expected_gross_return"),
                    "expected_net_return": (ev_prediction or {}).get("expected_net_return"),
                    "probability_positive": (ev_prediction or {}).get("probability_positive"),
                    "raw_ev_score": (ev_prediction or {}).get("raw_ev_score"),
                    "normalized_ev_score": (ev_prediction or {}).get("normalized_ev_score"),
                    "ev_score_post_clip": (ev_prediction or {}).get("ev_score_post_clip"),
                    "ev_confidence": (ev_prediction or {}).get("ev_confidence"),
                    "ev_confidence_multiplier": (ev_prediction or {}).get("ev_confidence_multiplier"),
                    "ev_score_before_confidence": (ev_prediction or {}).get("ev_score_before_confidence"),
                    "ev_score_after_confidence": (ev_prediction or {}).get("ev_score_after_confidence"),
                    "residual_std_bucket": (ev_prediction or {}).get("residual_std_bucket"),
                    "residual_std_global": (ev_prediction or {}).get("residual_std_global"),
                    "residual_std_final": (ev_prediction or {}).get("residual_std_final"),
                    "sample_size_used": (ev_prediction or {}).get("sample_size_used"),
                    "residual_std_confidence": (ev_prediction or {}).get("residual_std_confidence"),
                    "magnitude_confidence": (ev_prediction or {}).get("magnitude_confidence"),
                    "model_performance_confidence": (ev_prediction or {}).get("model_performance_confidence"),
                    "combined_confidence": (ev_prediction or {}).get("combined_confidence"),
                    "was_filtered_by_confidence": (ev_prediction or {}).get("was_filtered_by_confidence", False),
                    "ev_model_type_requested": requested_ev_model_type,
                    "ev_model_type_used": ev_model_type_used,
                    "ev_model_fallback_reason": local_ev_fallback_reason,
                    "regression_raw_ev_score": (regression_prediction or {}).get("regression_raw_ev_score"),
                    "regression_normalized_ev_score": (regression_prediction or {}).get(
                        "regression_normalized_ev_score"
                    ),
                    "regression_ev_score_post_clip": (regression_prediction or {}).get(
                        "regression_ev_score_post_clip"
                    ),
                },
            )
        )
        if request.symbol in candidate_row_by_symbol:
            candidate_row_by_symbol[request.symbol]["candidate_status"] = "executed"
            candidate_row_by_symbol[request.symbol]["candidate_outcome"] = "executed"
            candidate_row_by_symbol[request.symbol]["candidate_stage"] = "execution"
            candidate_row_by_symbol[request.symbol]["skip_reason"] = None
            candidate_row_by_symbol[request.symbol]["action_reason"] = "executed_candidate"
            candidate_row_by_symbol[request.symbol]["target_weight"] = effective_target_weight
            candidate_row_by_symbol[request.symbol]["weight_delta"] = effective_weight_delta
            candidate_row_by_symbol[request.symbol]["adjusted_target_weight"] = effective_target_weight
            candidate_row_by_symbol[request.symbol]["adjusted_weight_delta"] = effective_weight_delta

    diagnostics["order_count"] = len(orders)
    diagnostics["skipped_trades_count"] = len(skipped_trade_rows)
    diagnostics["skipped_turnover"] = float(skipped_turnover)
    diagnostics["effective_turnover_reduction"] = (
        float(skipped_turnover / total_requested_turnover) if total_requested_turnover > 0.0 else 0.0
    )
    diagnostics["blocked_entries_count"] = int(blocked_entries_count)
    diagnostics["held_in_hold_zone_count"] = int(held_in_hold_zone_count)
    diagnostics["forced_exit_count"] = int(forced_exit_count)
    diagnostics["ev_gate_blocked_count"] = int(ev_gate_blocked_count)
    diagnostics["confidence_filtered_count"] = int(confidence_filtered_count)
    diagnostics["skipped_due_to_entry_band_count"] = int(blocked_entries_count)
    diagnostics["skipped_due_to_hold_zone_count"] = int(held_in_hold_zone_count)
    diagnostics["skipped_trade_rows"] = skipped_trade_rows
    diagnostics["band_decision_rows"] = band_decision_rows
    diagnostics["ev_prediction_rows"] = ev_prediction_rows
    for symbol, row in candidate_row_by_symbol.items():
        if str(row.get("candidate_outcome") or "") != "pending":
            continue
        band_decision = str(row.get("band_decision") or "")
        if symbol not in request_symbols and band_decision == "blocked_entry":
            row["candidate_status"] = "skipped"
            row["candidate_outcome"] = "score_band_blocked"
            row["candidate_stage"] = "score_band"
            row["skip_reason"] = "blocked_below_entry_threshold"
            row["action_reason"] = "blocked_below_entry_threshold"
        elif symbol not in request_symbols and band_decision in {"hold_zone", "hold_zone_observed"}:
            row["candidate_status"] = "skipped"
            row["candidate_outcome"] = "hold_zone_skipped"
            row["candidate_stage"] = "score_band"
            row["skip_reason"] = "held_within_hold_zone"
            row["action_reason"] = "held_within_hold_zone"
        else:
            row["candidate_status"] = "skipped"
            row["candidate_outcome"] = "not_executed"
            row["candidate_stage"] = "pre_execution_filter"
            row["skip_reason"] = str(row.get("skip_reason") or "not_executed")
    diagnostics["candidate_trade_rows"] = candidate_trade_rows
    diagnostics["candidate_dataset_row_count"] = len(candidate_trade_rows)
    diagnostics["candidate_executed_count"] = sum(
        1 for row in candidate_trade_rows if str(row.get("candidate_outcome") or "") == "executed"
    )
    diagnostics["candidate_skipped_count"] = sum(
        1 for row in candidate_trade_rows if str(row.get("candidate_status") or "") == "skipped"
    )
    diagnostics["avg_expected_net_return_traded"] = (
        float(sum(ev_executed_expected_net_returns) / len(ev_executed_expected_net_returns))
        if ev_executed_expected_net_returns
        else 0.0
    )
    diagnostics["avg_expected_net_return_blocked"] = (
        float(sum(ev_blocked_expected_net_returns) / len(ev_blocked_expected_net_returns))
        if ev_blocked_expected_net_returns
        else 0.0
    )
    diagnostics["avg_ev_executed_trades"] = diagnostics["avg_expected_net_return_traded"]
    diagnostics["avg_raw_ev_executed_trades"] = (
        float(sum(ev_executed_raw_scores) / len(ev_executed_raw_scores)) if ev_executed_raw_scores else 0.0
    )
    diagnostics["avg_normalized_ev_executed_trades"] = (
        float(sum(ev_executed_normalized_scores) / len(ev_executed_normalized_scores))
        if ev_executed_normalized_scores
        else 0.0
    )
    diagnostics["avg_ev_weighting_score"] = (
        float(sum(ev_executed_weighting_scores) / len(ev_executed_weighting_scores))
        if ev_executed_weighting_scores
        else 0.0
    )
    diagnostics["avg_ev_confidence"] = (
        float(sum(ev_executed_confidences) / len(ev_executed_confidences)) if ev_executed_confidences else 1.0
    )
    diagnostics["avg_ev_confidence_multiplier"] = (
        float(sum(ev_executed_confidence_multipliers) / len(ev_executed_confidence_multipliers))
        if ev_executed_confidence_multipliers
        else 1.0
    )
    diagnostics["avg_ev_score_before_confidence"] = (
        float(sum(ev_executed_scores_before_confidence) / len(ev_executed_scores_before_confidence))
        if ev_executed_scores_before_confidence
        else 0.0
    )
    diagnostics["avg_ev_score_after_confidence"] = (
        float(sum(ev_executed_scores_after_confidence) / len(ev_executed_scores_after_confidence))
        if ev_executed_scores_after_confidence
        else 0.0
    )
    diagnostics["ev_weighted_exposure"] = float(sum(ev_adjusted_exposures))
    diagnostics["avg_ev_weight_multiplier"] = (
        float(sum(ev_executed_multipliers) / len(ev_executed_multipliers)) if ev_executed_multipliers else 1.0
    )
    diagnostics["regression_prediction_available_count"] = int(regression_prediction_available_count)
    diagnostics["regression_prediction_missing_count"] = int(regression_prediction_missing_count)
    diagnostics["avg_regression_ev_executed_trades"] = (
        float(sum(regression_executed_scores) / len(regression_executed_scores)) if regression_executed_scores else 0.0
    )
    diagnostics["avg_regression_ev_weighting_score"] = (
        float(sum(regression_executed_weighting_scores) / len(regression_executed_weighting_scores))
        if regression_executed_weighting_scores
        else 0.0
    )
    diagnostics["regression_ev_weighted_exposure"] = float(sum(regression_adjusted_exposures))
    diagnostics["ev_model_type_used"] = (
        "regression"
        if regression_requested_for_soft_weight and regression_prediction_available_count > 0
        else str(execution_ev_model_type)
    )
    diagnostics["ev_model_fallback_reason"] = (
        ev_model_fallback_reason
        if regression_requested_for_soft_weight and regression_prediction_available_count == 0
        else None
    )
    diagnostics["regression_training_summary"] = regression_training_summary
    diagnostics["ev_distribution"] = _ev_distribution(
        [float(row.get("expected_net_return", 0.0) or 0.0) for row in ev_prediction_rows]
    )
    diagnostics["ev_calibration_rows"] = ev_calibration_rows
    diagnostics["ev_calibration_summary"] = ev_calibration_summary
    diagnostics["symbols_considered"] = len(all_symbols)
    diagnostics["target_weight_sum"] = float(sum(latest_target_weights.values()))
    diagnostics["adjusted_target_weight_sum"] = float(sum(adjusted_target_weights.values()))
    diagnostics["estimated_buy_notional"] = float(sum(order.notional for order in orders if order.side == "BUY"))
    diagnostics["estimated_sell_notional"] = float(sum(order.notional for order in orders if order.side == "SELL"))
    return OrderGenerationResult(
        orders=orders,
        target_weights=adjusted_target_weights,
        diagnostics=diagnostics,
    )


def _simulate_execution_for_paper_orders(
    *,
    orders: list[PaperOrder],
    execution_config: ExecutionConfig,
    latest_target_weights: dict[str, float],
    current_cash: float,
    current_equity: float,
) -> tuple[list[PaperOrder], dict[str, Any]]:
    requests = [
        ExecutionOrderRequest(
            symbol=order.symbol,
            side=order.side,
            requested_shares=order.quantity,
            requested_notional=float(order.quantity) * float(order.reference_price),
            price=order.reference_price,
            target_weight=latest_target_weights.get(order.symbol, order.target_weight),
            current_shares=order.current_quantity,
            target_shares=order.target_quantity,
        )
        for order in orders
    ]
    simulation = simulate_execution(
        requests=requests,
        config=execution_config,
        current_cash=current_cash,
        current_equity=current_equity,
    )
    order_map = {(order.symbol, order.side, order.quantity): order for order in orders}
    executable_orders: list[PaperOrder] = []
    for executable in simulation.executable_orders:
        original = order_map[(executable.symbol, executable.side, executable.requested_shares)]
        executable_orders.append(
            PaperOrder(
                symbol=original.symbol,
                side=original.side,
                quantity=executable.adjusted_shares,
                reference_price=original.reference_price,
                target_weight=original.target_weight,
                current_quantity=original.current_quantity,
                target_quantity=original.current_quantity
                + (executable.adjusted_shares if original.side == "BUY" else -executable.adjusted_shares),
                notional=executable.adjusted_notional,
                reason=executable.clipping_reason or original.reason,
                expected_fill_price=executable.estimated_fill_price,
                expected_fees=executable.commission,
                expected_slippage_bps=executable.slippage_bps,
                provenance=dict(executable.provenance or original.provenance or {}),
            )
        )
    diagnostics = {
        "execution_summary": simulation.summary.to_dict(),
        "requested_orders": [order.to_dict() for order in simulation.requested_orders],
        "rejected_orders": [order.to_dict() for order in simulation.rejected_orders],
        "executable_orders": [order.to_dict() for order in simulation.executable_orders],
        "liquidity_constraints_report": [row.to_dict() for row in simulation.liquidity_diagnostics],
        "turnover_summary": simulation.turnover_rows,
        "symbol_tradeability_report": simulation.symbol_tradeability_rows,
    }
    return executable_orders, diagnostics


def apply_filled_orders(
    *,
    state: PaperPortfolioState,
    orders: list[PaperOrder],
    fill_prices: dict[str, float] | None = None,
) -> PaperPortfolioState:
    price_map = fill_prices or {}
    for order in orders:
        fill_price = float(price_map.get(order.symbol, order.reference_price))
        signed_qty = order.quantity if order.side == "BUY" else -order.quantity
        cash_change = -signed_qty * fill_price
        state.cash += cash_change

        current = state.positions.get(order.symbol)
        prior_quantity = current.quantity if current else 0
        new_quantity = prior_quantity + signed_qty

        if new_quantity == 0:
            state.positions.pop(order.symbol, None)
            continue

        if current is None:
            avg_price = fill_price
        elif signed_qty > 0 and prior_quantity >= 0:
            avg_price = ((current.avg_price * prior_quantity) + (fill_price * signed_qty)) / new_quantity
        else:
            avg_price = current.avg_price

        state.positions[order.symbol] = PaperPosition(
            symbol=order.symbol,
            quantity=new_quantity,
            avg_price=float(avg_price),
            last_price=fill_price,
        )

    return state


def _normalized_order_ownership(order: PaperOrder) -> dict[str, float]:
    ownership = dict((order.provenance or {}).get("strategy_ownership") or {})
    if ownership:
        total = sum(abs(float(weight)) for weight in ownership.values())
        if total > 0.0:
            return {
                str(strategy_id): float(abs(float(weight)) / total)
                for strategy_id, weight in ownership.items()
                if abs(float(weight)) > 0.0
            }
    strategy_id = str((order.provenance or {}).get("strategy_id") or "").strip()
    if strategy_id:
        return {strategy_id: 1.0}
    return {}


def _lot_signal_source(order: PaperOrder, strategy_id: str) -> str | None:
    strategy_rows = list((order.provenance or {}).get("strategy_rows") or [])
    for row in strategy_rows:
        if str(row.get("strategy_id") or "") == strategy_id:
            return str(row.get("signal_source") or "multi_strategy")
    value = (order.provenance or {}).get("signal_source")
    return str(value) if value else None


def _lot_signal_family(order: PaperOrder, strategy_id: str) -> str | None:
    strategy_rows = list((order.provenance or {}).get("strategy_rows") or [])
    for row in strategy_rows:
        if str(row.get("strategy_id") or "") == strategy_id:
            family = row.get("signal_family")
            return str(family) if family else None
    families = list((order.provenance or {}).get("signal_families") or [])
    return str(families[0]) if len(families) == 1 else None


def _next_trade_id(state: PaperPortfolioState) -> str:
    trade_id = f"paper-trade-{int(state.next_trade_id)}"
    state.next_trade_id += 1
    return trade_id


def _allocated_order_metrics(order: PaperOrder, quantity: int) -> dict[str, dict[str, float]]:
    ownership = _normalized_order_ownership(order)
    allocated_qty = allocate_integer_quantities(quantity, ownership)
    subset_qty = max(int(quantity), 0)
    total_order_qty = abs(int(order.quantity))
    if subset_qty <= 0 or total_order_qty <= 0:
        return {}
    metrics: dict[str, dict[str, float]] = {}
    for strategy_id, strategy_qty in allocated_qty.items():
        share = float(strategy_qty) / float(total_order_qty)
        metrics[strategy_id] = {
            "quantity": int(strategy_qty),
            "gross_notional": float(order.reference_price) * float(strategy_qty),
            "effective_notional": float(order.expected_fill_price or order.reference_price) * float(strategy_qty),
            "slippage_cost": float(order.expected_slippage_cost) * share,
            "spread_cost": float(order.expected_spread_cost) * share,
            "commission_cost": float(order.expected_commission_cost) * share,
            "total_execution_cost": float(order.expected_total_execution_cost) * share,
        }
    return metrics


def _build_attributed_fill_rows(
    *,
    order: PaperOrder,
    quantity: int,
    as_of: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fill_date = pd.Timestamp(as_of).date().isoformat() if as_of else None
    for strategy_id, metrics in sorted(_allocated_order_metrics(order, quantity).items()):
        rows.append(
            {
                "date": fill_date,
                "symbol": order.symbol,
                "strategy_id": strategy_id,
                "signal_source": _lot_signal_source(order, strategy_id),
                "signal_family": _lot_signal_family(order, strategy_id),
                "side": str(order.side).upper(),
                "quantity": int(metrics["quantity"]),
                "reference_price": float(order.reference_price),
                "fill_price": float(order.expected_fill_price or order.reference_price),
                "gross_notional": float(metrics["gross_notional"]),
                "notional": float(metrics["effective_notional"]),
                "slippage_cost": float(metrics["slippage_cost"]),
                "spread_cost": float(metrics["spread_cost"]),
                "commission_cost": float(metrics["commission_cost"]),
                "total_execution_cost": float(metrics["total_execution_cost"]),
                "fill_count": 1,
                "cost_model": str(order.cost_model),
            }
        )
    return rows


def _append_open_lots(
    *,
    state: PaperPortfolioState,
    order: PaperOrder,
    fill_price: float,
    quantity: int,
    as_of: str | None,
) -> None:
    if quantity <= 0:
        return
    allocated = _allocated_order_metrics(order, quantity)
    if not allocated:
        return
    lots = state.open_lots.setdefault(order.symbol, [])
    side = "long" if str(order.side).upper() == "BUY" else "short"
    signed_qty = 1 if side == "long" else -1
    for strategy_id, metrics in sorted(allocated.items()):
        lot_qty = int(metrics["quantity"])
        lots.append(
            PaperTradeLot(
                trade_id=_next_trade_id(state),
                symbol=order.symbol,
                strategy_id=strategy_id,
                signal_source=_lot_signal_source(order, strategy_id),
                signal_family=_lot_signal_family(order, strategy_id),
                side=side,
                entry_as_of=str(as_of or state.as_of or ""),
                entry_reference_price=float(order.reference_price),
                entry_price=float(fill_price),
                quantity=int(lot_qty * signed_qty),
                remaining_quantity=int(lot_qty * signed_qty),
                entry_slippage_cost=float(metrics["slippage_cost"]),
                entry_spread_cost=float(metrics["spread_cost"]),
                entry_commission_cost=float(metrics["commission_cost"]),
                entry_total_execution_cost=float(metrics["total_execution_cost"]),
                cost_model=str(order.cost_model),
                metadata={
                    "order_reason": order.reason,
                    "target_weight": (order.provenance or {}).get("target_weight"),
                    "ev_entry": (order.provenance or {}).get("raw_ev_score"),
                    "score_entry": (order.provenance or {}).get("score_value"),
                    "score_percentile_entry": (order.provenance or {}).get("score_percentile"),
                    "entry_reason": (order.provenance or {}).get("action_reason"),
                },
            )
        )


def _close_open_lots(
    *,
    state: PaperPortfolioState,
    order: PaperOrder,
    fill_price: float,
    quantity: int,
    as_of: str | None,
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]]]:
    lots = list(state.open_lots.get(order.symbol, []))
    if quantity <= 0 or not lots:
        return 0.0, [], []
    closing_side = str(order.side).upper()
    closes_long = closing_side == "SELL"
    remaining_to_close = int(quantity)
    total_order_qty = abs(int(order.quantity))
    realized_total = 0.0
    trade_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    updated_lots: list[PaperTradeLot] = []
    for lot in lots:
        remaining_qty = int(lot.remaining_quantity)
        if remaining_qty == 0:
            continue
        lot_is_long = remaining_qty > 0
        if remaining_to_close > 0 and ((closes_long and lot_is_long) or ((not closes_long) and (not lot_is_long))):
            closed_qty = min(abs(remaining_qty), remaining_to_close)
            remaining_to_close -= closed_qty
            sign = 1.0 if lot_is_long else -1.0
            gross_realized_pnl = (float(order.reference_price) - float(lot.entry_reference_price)) * closed_qty * sign
            net_realized_pnl = (float(fill_price) - float(lot.entry_price)) * closed_qty * sign
            realized_total += net_realized_pnl
            exit_ts = pd.Timestamp(as_of).date().isoformat() if as_of else None
            holding_days = None
            if lot.entry_as_of and exit_ts:
                holding_days = int((pd.Timestamp(exit_ts) - pd.Timestamp(lot.entry_as_of)).days)
            lot_share = float(closed_qty) / float(abs(remaining_qty))
            entry_slippage_cost = float(lot.entry_slippage_cost) * lot_share
            entry_spread_cost = float(lot.entry_spread_cost) * lot_share
            entry_commission_cost = float(lot.entry_commission_cost) * lot_share
            entry_total_execution_cost = float(lot.entry_total_execution_cost) * lot_share
            exit_share = (float(closed_qty) / float(total_order_qty)) if total_order_qty > 0 else 0.0
            exit_slippage_cost = float(order.expected_slippage_cost) * exit_share
            exit_spread_cost = float(order.expected_spread_cost) * exit_share
            exit_commission_cost = float(order.expected_commission_cost) * exit_share
            exit_total_execution_cost = float(order.expected_total_execution_cost) * exit_share
            total_execution_cost = float(entry_total_execution_cost + exit_total_execution_cost)
            trade_rows.append(
                {
                    "trade_id": lot.trade_id,
                    "date": exit_ts,
                    "symbol": lot.symbol,
                    "strategy_id": lot.strategy_id,
                    "signal_source": lot.signal_source,
                    "signal_family": lot.signal_family,
                    "side": lot.side,
                    "quantity": int(closed_qty),
                    "entry_reference_price": float(lot.entry_reference_price),
                    "entry_price": float(lot.entry_price),
                    "exit_reference_price": float(order.reference_price),
                    "exit_price": float(fill_price),
                    "gross_realized_pnl": float(gross_realized_pnl),
                    "net_realized_pnl": float(net_realized_pnl),
                    "realized_pnl": float(net_realized_pnl),
                    "slippage_cost": float(entry_slippage_cost + exit_slippage_cost),
                    "spread_cost": float(entry_spread_cost + exit_spread_cost),
                    "commission_cost": float(entry_commission_cost + exit_commission_cost),
                    "total_execution_cost": total_execution_cost,
                    "holding_period_days": holding_days,
                    "attribution_method": lot.attribution_method,
                    "cost_model": str(lot.cost_model or order.cost_model),
                    "status": "closed",
                    "entry_date": lot.entry_as_of,
                    "exit_date": exit_ts,
                    "ev_entry": lot.metadata.get("ev_entry"),
                    "score_entry": lot.metadata.get("score_entry"),
                    "score_percentile_entry": lot.metadata.get("score_percentile_entry"),
                    "entry_reason": lot.metadata.get("entry_reason"),
                    "ev_exit": (order.provenance or {}).get("raw_ev_score"),
                    "score_exit": (order.provenance or {}).get("score_value"),
                    "score_percentile_exit": (order.provenance or {}).get("score_percentile"),
                    "exit_reason": (order.provenance or {}).get("action_reason") or order.reason,
                }
            )
            fill_rows.append(
                {
                    "date": exit_ts,
                    "symbol": lot.symbol,
                    "strategy_id": lot.strategy_id,
                    "signal_source": lot.signal_source,
                    "signal_family": lot.signal_family,
                    "side": closing_side,
                    "quantity": int(closed_qty),
                    "reference_price": float(order.reference_price),
                    "fill_price": float(fill_price),
                    "gross_notional": float(closed_qty * float(order.reference_price)),
                    "notional": float(closed_qty * fill_price),
                    "slippage_cost": float(exit_slippage_cost),
                    "spread_cost": float(exit_spread_cost),
                    "commission_cost": float(exit_commission_cost),
                    "total_execution_cost": float(exit_total_execution_cost),
                    "fill_count": 1,
                    "cost_model": str(order.cost_model),
                }
            )
            new_remaining = abs(remaining_qty) - closed_qty
            if new_remaining > 0:
                updated_lots.append(
                    replace(
                        lot,
                        remaining_quantity=int(new_remaining if lot_is_long else -new_remaining),
                        entry_slippage_cost=float(lot.entry_slippage_cost - entry_slippage_cost),
                        entry_spread_cost=float(lot.entry_spread_cost - entry_spread_cost),
                        entry_commission_cost=float(lot.entry_commission_cost - entry_commission_cost),
                        entry_total_execution_cost=float(lot.entry_total_execution_cost - entry_total_execution_cost),
                    )
                )
            continue
        updated_lots.append(lot)
    state.open_lots[order.symbol] = updated_lots
    return float(realized_total), trade_rows, fill_rows


def _apply_fill_to_state_with_attribution(
    *,
    state: PaperPortfolioState,
    order: PaperOrder,
    fill_price: float,
    as_of: str | None,
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]]]:
    current = state.positions.get(order.symbol)
    prior_quantity = int(current.quantity) if current is not None else 0
    signed_qty = int(order.quantity) if order.side == "BUY" else -int(order.quantity)
    closing_quantity = (
        min(abs(prior_quantity), abs(signed_qty)) if prior_quantity and (prior_quantity * signed_qty) < 0 else 0
    )
    realized_pnl, trade_rows, fill_rows = _close_open_lots(
        state=state,
        order=order,
        fill_price=fill_price,
        quantity=closing_quantity,
        as_of=as_of,
    )
    state.cash += -signed_qty * float(fill_price)
    new_quantity = prior_quantity + signed_qty
    if new_quantity == 0:
        state.positions.pop(order.symbol, None)
    else:
        if current is None:
            avg_price = float(fill_price)
        elif signed_qty > 0 and prior_quantity >= 0:
            avg_price = ((current.avg_price * prior_quantity) + (float(fill_price) * signed_qty)) / new_quantity
        elif signed_qty < 0 and prior_quantity <= 0:
            avg_price = ((current.avg_price * abs(prior_quantity)) + (float(fill_price) * abs(signed_qty))) / abs(
                new_quantity
            )
        elif abs(signed_qty) > abs(prior_quantity):
            avg_price = float(fill_price)
        else:
            avg_price = float(current.avg_price)
        state.positions[order.symbol] = PaperPosition(
            symbol=order.symbol,
            quantity=int(new_quantity),
            avg_price=float(avg_price),
            last_price=float(fill_price),
        )
    opening_quantity = max(abs(signed_qty) - closing_quantity, 0)
    if opening_quantity > 0:
        _append_open_lots(
            state=state,
            order=order,
            fill_price=fill_price,
            quantity=opening_quantity,
            as_of=as_of,
        )
        fill_rows.extend(
            _build_attributed_fill_rows(
                order=order,
                quantity=opening_quantity,
                as_of=as_of,
            )
        )
    state.cumulative_realized_pnl += float(realized_pnl)
    state.cumulative_gross_realized_pnl += float(
        sum(float(row.get("gross_realized_pnl", 0.0) or 0.0) for row in trade_rows)
    )
    state.cumulative_fees += float(order.expected_commission_cost)
    state.cumulative_slippage_cost += float(order.expected_slippage_cost)
    state.cumulative_spread_cost += float(order.expected_spread_cost)
    state.cumulative_execution_cost += float(order.expected_total_execution_cost)
    return float(realized_pnl), trade_rows, fill_rows


def _estimate_order_realized_pnl(
    *,
    state: PaperPortfolioState,
    order: PaperOrder,
    fill_price: float,
) -> float:
    current = state.positions.get(order.symbol)
    if current is None:
        return 0.0
    prior_quantity = int(current.quantity)
    realized_pnl = 0.0
    if order.side == "SELL" and prior_quantity > 0:
        closed_quantity = min(prior_quantity, int(order.quantity))
        realized_pnl += (float(fill_price) - float(current.avg_price)) * float(closed_quantity)
    elif order.side == "BUY" and prior_quantity < 0:
        closed_quantity = min(abs(prior_quantity), int(order.quantity))
        realized_pnl += (float(current.avg_price) - float(fill_price)) * float(closed_quantity)
    return float(realized_pnl)


def _apply_fill_to_state(
    *,
    state: PaperPortfolioState,
    symbol: str,
    side: str,
    quantity: int,
    fill_price: float,
) -> float:
    signed_qty = int(quantity) if side == "BUY" else -int(quantity)
    realized_pnl = _estimate_order_realized_pnl(
        state=state,
        order=PaperOrder(
            symbol=symbol,
            side=side,
            quantity=int(quantity),
            reference_price=float(fill_price),
            target_weight=0.0,
            current_quantity=int(state.positions.get(symbol).quantity) if symbol in state.positions else 0,
            target_quantity=0,
            notional=float(quantity) * float(fill_price),
            reason="fill_application",
        ),
        fill_price=float(fill_price),
    )
    state.cash += -signed_qty * float(fill_price)
    current = state.positions.get(symbol)
    prior_quantity = current.quantity if current else 0
    new_quantity = prior_quantity + signed_qty
    if new_quantity == 0:
        state.positions.pop(symbol, None)
    else:
        if current is None:
            avg_price = float(fill_price)
        elif signed_qty > 0 and prior_quantity >= 0:
            avg_price = ((current.avg_price * prior_quantity) + (float(fill_price) * signed_qty)) / new_quantity
        elif signed_qty < 0 and prior_quantity <= 0:
            avg_price = ((current.avg_price * abs(prior_quantity)) + (float(fill_price) * abs(signed_qty))) / abs(
                new_quantity
            )
        else:
            avg_price = float(current.avg_price)
        state.positions[symbol] = PaperPosition(
            symbol=symbol,
            quantity=int(new_quantity),
            avg_price=float(avg_price),
            last_price=float(fill_price),
        )
    state.cumulative_realized_pnl += float(realized_pnl)
    return float(realized_pnl)


def _apply_execution_orders_to_state(
    *,
    state: PaperPortfolioState,
    orders: list[PaperOrder],
    as_of: str | None,
) -> tuple[PaperPortfolioState, list[BrokerFill], list[dict[str, Any]], list[dict[str, Any]]]:
    fills = []
    trade_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    for order in orders:
        fill_price = float(order.expected_fill_price or order.reference_price)
        realized_pnl, closed_trade_rows, attributed_fill_rows = _apply_fill_to_state_with_attribution(
            state=state,
            order=order,
            fill_price=fill_price,
            as_of=as_of,
        )
        trade_rows.extend(closed_trade_rows)
        fill_rows.extend(attributed_fill_rows)
        primary_strategy = None
        ownership = _normalized_order_ownership(order)
        if ownership:
            primary_strategy = max(ownership.items(), key=lambda item: (item[1], item[0]))[0]
        fills.append(
            BrokerFill(
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                fill_price=fill_price,
                notional=float(order.quantity) * fill_price,
                reference_price=float(order.reference_price),
                gross_notional=float(order.expected_gross_notional or (float(order.reference_price) * order.quantity)),
                commission=float(order.expected_commission_cost),
                slippage_bps=float(order.expected_slippage_bps),
                spread_bps=float(order.expected_spread_bps),
                slippage_cost=float(order.expected_slippage_cost),
                spread_cost=float(order.expected_spread_cost),
                total_execution_cost=float(order.expected_total_execution_cost),
                cost_model=str(order.cost_model),
                realized_pnl=float(realized_pnl),
                strategy_id=primary_strategy,
                signal_source=str((order.provenance or {}).get("signal_source") or "multi_strategy"),
                provenance=dict(order.provenance or {}),
            )
        )
    return state, fills, trade_rows, fill_rows


def _apply_execution_orders_with_paper_broker(
    *,
    state: PaperPortfolioState,
    orders: list[PaperOrder],
    as_of: str | None,
) -> tuple[PaperPortfolioState, list[BrokerFill], list[dict[str, Any]], list[dict[str, Any]]]:
    accounting_state = _clone_state(state)
    broker = PaperBroker(
        state=state,
        config=PaperBrokerConfig(
            commission_per_order=0.0,
            slippage_bps=0.0,
        ),
    )
    broker_orders = [
        BrokerOrder(
            symbol=order.symbol,
            side=order.side,
            quantity=int(order.quantity),
            reference_price=float(order.expected_fill_price or order.reference_price),
            reason=order.reason,
        )
        for order in orders
    ]
    broker_fills = broker.submit_orders(broker_orders)
    fills: list[BrokerFill] = []
    trade_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    for order, fill in zip(orders, broker_fills, strict=False):
        realized_pnl, closed_trade_rows, attributed_fill_rows = _apply_fill_to_state_with_attribution(
            state=accounting_state,
            order=order,
            fill_price=float(fill.fill_price),
            as_of=as_of,
        )
        trade_rows.extend(closed_trade_rows)
        fill_rows.extend(attributed_fill_rows)
        position = state.positions.get(order.symbol)
        if position is not None:
            position.last_price = float(fill.fill_price)
        primary_strategy = None
        ownership = _normalized_order_ownership(order)
        if ownership:
            primary_strategy = max(ownership.items(), key=lambda item: (item[1], item[0]))[0]
        fills.append(
            BrokerFill(
                symbol=fill.symbol,
                side=fill.side,
                quantity=int(fill.quantity),
                fill_price=float(fill.fill_price),
                notional=float(fill.notional),
                reference_price=float(order.reference_price),
                gross_notional=float(order.expected_gross_notional or (float(order.reference_price) * order.quantity)),
                commission=float(order.expected_commission_cost),
                slippage_bps=float(order.expected_slippage_bps),
                spread_bps=float(order.expected_spread_bps),
                slippage_cost=float(order.expected_slippage_cost),
                spread_cost=float(order.expected_spread_cost),
                total_execution_cost=float(order.expected_total_execution_cost),
                cost_model=str(order.cost_model),
                realized_pnl=float(realized_pnl),
                strategy_id=primary_strategy,
                signal_source=str((order.provenance or {}).get("signal_source") or "multi_strategy"),
                provenance=dict(order.provenance or {}),
            )
        )
    state.cumulative_realized_pnl = float(accounting_state.cumulative_realized_pnl)
    state.cumulative_gross_realized_pnl = float(accounting_state.cumulative_gross_realized_pnl)
    state.cumulative_fees = float(accounting_state.cumulative_fees)
    state.cumulative_slippage_cost = float(accounting_state.cumulative_slippage_cost)
    state.cumulative_spread_cost = float(accounting_state.cumulative_spread_cost)
    state.cumulative_execution_cost = float(accounting_state.cumulative_execution_cost)
    state.open_lots = accounting_state.open_lots
    state.next_trade_id = accounting_state.next_trade_id
    return state, fills, trade_rows, fill_rows


def _apply_paper_slippage(
    *,
    orders: list[PaperOrder],
    config: PaperTradingConfig,
) -> tuple[list[PaperOrder], dict[str, Any]]:
    validate_slippage_config(config)
    adjusted = [apply_order_slippage(order, config) for order in orders]
    model = str(config.slippage_model or "none").lower()
    return adjusted, {
        "slippage_enabled": model != "none",
        "slippage_model": model,
        "slippage_buy_bps": float(config.slippage_buy_bps),
        "slippage_sell_bps": float(config.slippage_sell_bps),
        "cost_model_enabled": bool(config.enable_cost_model),
        "commission_bps": float(config.commission_bps),
        "minimum_commission": float(config.minimum_commission),
        "spread_bps": float(config.spread_bps),
        "cost_model": "paper_v2_cost_model" if bool(config.enable_cost_model) or model != "none" else "disabled",
        "expected_slippage_cost": float(sum(order.expected_slippage_cost for order in adjusted)),
        "expected_spread_cost": float(sum(order.expected_spread_cost for order in adjusted)),
        "expected_commission_cost": float(sum(order.expected_commission_cost for order in adjusted)),
        "expected_total_execution_cost": float(sum(order.expected_total_execution_cost for order in adjusted)),
        "slippage_order_count": len(adjusted),
    }


def _build_accounting_summary(
    *,
    starting_state: PaperPortfolioState,
    ending_state: PaperPortfolioState,
    fills: list[BrokerFill],
    auto_apply_fills: bool,
    latest_effective_weights: dict[str, float],
) -> dict[str, Any]:
    buy_fill_count = sum(1 for fill in fills if fill.side == "BUY")
    sell_fill_count = sum(1 for fill in fills if fill.side == "SELL")
    fill_notional = sum(float(fill.notional) for fill in fills)
    fill_application_status = (
        "fills_applied"
        if auto_apply_fills and fills
        else "no_executable_orders"
        if auto_apply_fills and not latest_effective_weights
        else "auto_apply_disabled"
        if not auto_apply_fills
        else "orders_generated_but_not_filled"
    )
    starting_equity = float(starting_state.equity)
    ending_equity = float(ending_state.equity)
    realized_delta = float(ending_state.cumulative_realized_pnl - starting_state.cumulative_realized_pnl)
    gross_realized_delta = float(
        ending_state.cumulative_gross_realized_pnl - starting_state.cumulative_gross_realized_pnl
    )
    fee_delta = float(ending_state.cumulative_fees - starting_state.cumulative_fees)
    slippage_cost_delta = float(ending_state.cumulative_slippage_cost - starting_state.cumulative_slippage_cost)
    spread_cost_delta = float(ending_state.cumulative_spread_cost - starting_state.cumulative_spread_cost)
    execution_cost_delta = float(ending_state.cumulative_execution_cost - starting_state.cumulative_execution_cost)
    total_pnl_delta = float(ending_equity - starting_equity)
    gross_unrealized_pnl = 0.0
    for lots in ending_state.open_lots.values():
        for lot in lots:
            remaining_qty = int(lot.remaining_quantity)
            if remaining_qty == 0:
                continue
            position = ending_state.positions.get(lot.symbol)
            if position is None:
                continue
            sign = 1.0 if remaining_qty > 0 else -1.0
            gross_unrealized_pnl += (
                float(position.last_price) - float(lot.entry_reference_price)
            ) * abs(remaining_qty) * sign
    gross_total_pnl = float(ending_state.cumulative_gross_realized_pnl + gross_unrealized_pnl)
    net_total_pnl = float(ending_state.total_pnl)
    net_unrealized_pnl = float(net_total_pnl - ending_state.cumulative_realized_pnl)
    return {
        "auto_apply_fills": bool(auto_apply_fills),
        "fill_application_status": fill_application_status,
        "starting_cash": float(starting_state.cash),
        "ending_cash": float(ending_state.cash),
        "starting_gross_market_value": float(starting_state.gross_market_value),
        "ending_gross_market_value": float(ending_state.gross_market_value),
        "starting_equity": starting_equity,
        "ending_equity": ending_equity,
        "fill_count": int(len(fills)),
        "buy_fill_count": int(buy_fill_count),
        "sell_fill_count": int(sell_fill_count),
        "fill_notional": float(fill_notional),
        "gross_realized_pnl_delta": gross_realized_delta,
        "realized_pnl_delta": realized_delta,
        "net_realized_pnl_delta": realized_delta,
        "gross_realized_pnl": float(ending_state.cumulative_gross_realized_pnl),
        "cumulative_realized_pnl": float(ending_state.cumulative_realized_pnl),
        "net_realized_pnl": float(ending_state.cumulative_realized_pnl),
        "gross_unrealized_pnl": float(gross_unrealized_pnl),
        "unrealized_pnl": net_unrealized_pnl,
        "net_unrealized_pnl": net_unrealized_pnl,
        "gross_total_pnl": gross_total_pnl,
        "net_total_pnl": net_total_pnl,
        "total_pnl": net_total_pnl,
        "total_pnl_delta": total_pnl_delta,
        "fees_paid_delta": fee_delta,
        "cumulative_fees": float(ending_state.cumulative_fees),
        "total_commission_cost": float(ending_state.cumulative_fees),
        "commission_cost_delta": fee_delta,
        "total_slippage_cost": float(ending_state.cumulative_slippage_cost),
        "slippage_cost_delta": slippage_cost_delta,
        "total_spread_cost": float(ending_state.cumulative_spread_cost),
        "spread_cost_delta": spread_cost_delta,
        "total_execution_cost": float(ending_state.cumulative_execution_cost),
        "execution_cost_delta": execution_cost_delta,
        "cost_drag_pct": (execution_cost_delta / starting_equity) if starting_equity > 0.0 else 0.0,
        "position_count": int(len(ending_state.positions)),
        "target_weight_sum": float(sum(latest_effective_weights.values())),
    }


def run_paper_trading_cycle_for_targets(
    *,
    config: PaperTradingConfig,
    state_store: JsonPaperStateStore,
    as_of: str,
    latest_prices: dict[str, float],
    latest_scores: dict[str, float],
    latest_scheduled_weights: dict[str, float],
    latest_effective_weights: dict[str, float],
    target_diagnostics: dict[str, Any],
    skipped_symbols: list[str],
    extra_diagnostics: dict[str, Any] | None = None,
    price_snapshots: list[PaperExecutionPriceSnapshot] | None = None,
    decision_bundle: DecisionJournalBundle | None = None,
    universe_bundle: UniverseBuildBundle | None = None,
    execution_config: ExecutionConfig | None = None,
    auto_apply_fills: bool = False,
) -> PaperTradingRunResult:
    state = state_store.load()
    if state.cash <= 0 and not state.positions:
        state = bootstrap_paper_portfolio_state(initial_cash=config.initial_cash)

    state = sync_state_prices(state, latest_prices)
    starting_state = _clone_state(state)

    order_result = generate_rebalance_orders(
        as_of=as_of,
        state=state,
        latest_target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        latest_scores=latest_scores,
        config=config,
        min_trade_dollars=config.min_trade_dollars,
        min_weight_change_to_trade=config.min_weight_change_to_trade,
        lot_size=config.lot_size,
        reserve_cash_pct=config.reserve_cash_pct,
        provenance_by_symbol=getattr(decision_bundle, "provenance_by_symbol", None),
    )

    execution_diagnostics: dict[str, Any] = {}
    executable_orders = order_result.orders
    if execution_config is not None:
        executable_orders, execution_diagnostics = _simulate_execution_for_paper_orders(
            orders=order_result.orders,
            execution_config=execution_config,
            latest_target_weights=latest_effective_weights,
            current_cash=state.cash,
            current_equity=state.equity,
        )
    executable_orders, slippage_diagnostics = _apply_paper_slippage(
        orders=executable_orders,
        config=config,
    )

    broker_orders = [
        BrokerOrder(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            reference_price=order.expected_fill_price or order.reference_price,
            reason=order.reason,
        )
        for order in executable_orders
    ]

    risk_result = validate_orders(
        orders=broker_orders,
        equity=state.equity,
        max_single_order_notional=None,
        max_gross_order_notional_pct=None,
    )

    if not risk_result.passed:
        raise ValueError(f"Pre-trade checks failed: {risk_result.violations}")

    fills = []
    realized_trade_rows: list[dict[str, Any]] = []
    attributed_fill_rows: list[dict[str, Any]] = []
    if auto_apply_fills and executable_orders:
        state, fills, realized_trade_rows, attributed_fill_rows = _apply_execution_orders_with_paper_broker(
            state=state,
            orders=executable_orders,
            as_of=as_of,
        )
        state = sync_state_prices(state, latest_prices)

    state.as_of = as_of
    if state.initial_cash_basis <= 0.0:
        state.initial_cash_basis = float(
            starting_state.initial_cash_basis or starting_state.equity or config.initial_cash
        )
    state.last_targets = latest_effective_weights.copy()
    state_store.save(state)
    accounting_summary = _build_accounting_summary(
        starting_state=starting_state,
        ending_state=state,
        fills=fills,
        auto_apply_fills=auto_apply_fills,
        latest_effective_weights=latest_effective_weights,
    )
    attribution_payload = build_daily_attribution(
        as_of=as_of,
        state=state,
        equity=state.equity,
        realized_trade_rows=realized_trade_rows,
        fill_rows=attributed_fill_rows,
    )
    attribution_reconciliation = build_reconciliation_summary(
        strategy_rows=attribution_payload.get("strategy_rows", []),
        symbol_rows=attribution_payload.get("symbol_rows", []),
        portfolio_realized_pnl=float(accounting_summary.get("realized_pnl_delta", 0.0)),
        portfolio_unrealized_pnl=float(accounting_summary.get("unrealized_pnl", 0.0)),
        portfolio_gross_realized_pnl=float(accounting_summary.get("gross_realized_pnl_delta", 0.0)),
        portfolio_gross_unrealized_pnl=float(accounting_summary.get("gross_unrealized_pnl", 0.0)),
        portfolio_total_execution_cost=float(
            (
                float(accounting_summary.get("gross_realized_pnl_delta", 0.0) or 0.0)
                + float(accounting_summary.get("gross_unrealized_pnl", 0.0) or 0.0)
            )
            - (
                float(accounting_summary.get("realized_pnl_delta", 0.0) or 0.0)
                + float(accounting_summary.get("unrealized_pnl", 0.0) or 0.0)
            )
        ),
    )
    attribution_summary = build_attribution_summary(
        strategy_rows=attribution_payload.get("strategy_rows", []),
        symbol_rows=attribution_payload.get("symbol_rows", []),
        trade_rows=attribution_payload.get("trade_rows", []),
        reconciliation=attribution_reconciliation,
    )
    attribution_payload["summary"] = attribution_summary

    diagnostics = {
        "signal_source": config.signal_source,
        "preset_name": config.preset_name,
        "target_construction": target_diagnostics,
        "order_generation": order_result.diagnostics,
        "risk_checks": {
            "passed": risk_result.passed,
            "violations": risk_result.violations,
        },
        "fill_count": len(fills),
        "execution": execution_diagnostics,
        "paper_execution": {
            "slippage_enabled": slippage_diagnostics["slippage_enabled"],
            "slippage_model": slippage_diagnostics["slippage_model"],
            "slippage_buy_bps": slippage_diagnostics["slippage_buy_bps"],
            "slippage_sell_bps": slippage_diagnostics["slippage_sell_bps"],
            "cost_model_enabled": slippage_diagnostics["cost_model_enabled"],
            "cost_model": slippage_diagnostics["cost_model"],
            "commission_bps": slippage_diagnostics["commission_bps"],
            "minimum_commission": slippage_diagnostics["minimum_commission"],
            "spread_bps": slippage_diagnostics["spread_bps"],
            "expected_slippage_cost": slippage_diagnostics["expected_slippage_cost"],
            "expected_spread_cost": slippage_diagnostics["expected_spread_cost"],
            "expected_commission_cost": slippage_diagnostics["expected_commission_cost"],
            "expected_total_execution_cost": slippage_diagnostics["expected_total_execution_cost"],
            "min_weight_change_to_trade": float(config.min_weight_change_to_trade),
            "score_band_enabled": bool(order_result.diagnostics.get("score_band_enabled", False)),
            "entry_threshold_used": order_result.diagnostics.get("entry_threshold_used"),
            "exit_threshold_used": order_result.diagnostics.get("exit_threshold_used"),
            "score_band_mode": str(order_result.diagnostics.get("score_band_mode", "raw_score")),
            "blocked_entries_count": int(order_result.diagnostics.get("blocked_entries_count", 0) or 0),
            "held_in_hold_zone_count": int(order_result.diagnostics.get("held_in_hold_zone_count", 0) or 0),
            "forced_exit_count": int(order_result.diagnostics.get("forced_exit_count", 0) or 0),
            "ev_gate_enabled": bool(order_result.diagnostics.get("ev_gate_enabled", False)),
            "ev_gate_model_type": str(order_result.diagnostics.get("ev_gate_model_type", "bucketed_mean")),
            "ev_model_type_requested": str(
                order_result.diagnostics.get("ev_model_type_requested", "bucketed_mean") or "bucketed_mean"
            ),
            "ev_model_type_used": str(
                order_result.diagnostics.get("ev_model_type_used", order_result.diagnostics.get("ev_gate_model_type", "bucketed_mean"))
                or "bucketed_mean"
            ),
            "ev_model_fallback_reason": str(order_result.diagnostics.get("ev_model_fallback_reason", "") or ""),
            "ev_gate_target_type": str(order_result.diagnostics.get("ev_gate_target_type", "market_proxy") or "market_proxy"),
            "ev_gate_mode": str(order_result.diagnostics.get("ev_gate_mode", "hard") or "hard"),
            "ev_gate_training_source": str(
                order_result.diagnostics.get("ev_gate_training_source", "executed_trades") or "executed_trades"
            ),
            "ev_gate_weight_multiplier": bool(order_result.diagnostics.get("ev_gate_weight_multiplier", False)),
            "ev_gate_weight_scale": float(order_result.diagnostics.get("ev_gate_weight_scale", 1.0) or 0.0),
            "ev_gate_extreme_negative_threshold": order_result.diagnostics.get("ev_gate_extreme_negative_threshold"),
            "ev_gate_score_clip_min": order_result.diagnostics.get("ev_gate_score_clip_min"),
            "ev_gate_score_clip_max": order_result.diagnostics.get("ev_gate_score_clip_max"),
            "ev_gate_normalize_scores": bool(order_result.diagnostics.get("ev_gate_normalize_scores", False)),
            "ev_gate_normalization_method": str(
                order_result.diagnostics.get("ev_gate_normalization_method", "zscore") or "zscore"
            ),
            "ev_gate_normalize_within": str(
                order_result.diagnostics.get("ev_gate_normalize_within", "all_candidates") or "all_candidates"
            ),
            "ev_gate_use_normalized_score_for_weighting": bool(
                order_result.diagnostics.get("ev_gate_use_normalized_score_for_weighting", True)
            ),
            "ev_gate_weight_multiplier_min": order_result.diagnostics.get("ev_gate_weight_multiplier_min"),
            "ev_gate_weight_multiplier_max": order_result.diagnostics.get("ev_gate_weight_multiplier_max"),
            "ev_gate_use_confidence_weighting": bool(
                order_result.diagnostics.get("ev_gate_use_confidence_weighting", False)
            ),
            "ev_gate_confidence_method": str(
                order_result.diagnostics.get("ev_gate_confidence_method", "residual_std") or "residual_std"
            ),
            "ev_gate_confidence_scale": float(order_result.diagnostics.get("ev_gate_confidence_scale", 1.0)),
            "ev_gate_confidence_clip_min": float(
                order_result.diagnostics.get("ev_gate_confidence_clip_min", 0.5)
            ),
            "ev_gate_confidence_clip_max": float(
                order_result.diagnostics.get("ev_gate_confidence_clip_max", 1.5)
            ),
            "ev_gate_confidence_min_samples_per_bucket": int(
                order_result.diagnostics.get("ev_gate_confidence_min_samples_per_bucket", 20) or 20
            ),
            "ev_gate_confidence_shrinkage_enabled": bool(
                order_result.diagnostics.get("ev_gate_confidence_shrinkage_enabled", True)
            ),
            "ev_gate_confidence_component_residual_std_weight": float(
                order_result.diagnostics.get("ev_gate_confidence_component_residual_std_weight", 1.0) or 0.0
            ),
            "ev_gate_confidence_component_magnitude_weight": float(
                order_result.diagnostics.get("ev_gate_confidence_component_magnitude_weight", 0.0) or 0.0
            ),
            "ev_gate_confidence_component_model_performance_weight": float(
                order_result.diagnostics.get("ev_gate_confidence_component_model_performance_weight", 0.0) or 0.0
            ),
            "ev_gate_use_confidence_filter": bool(
                order_result.diagnostics.get("ev_gate_use_confidence_filter", False)
            ),
            "ev_gate_confidence_threshold": float(
                order_result.diagnostics.get("ev_gate_confidence_threshold", 0.0) or 0.0
            ),
            "ev_gate_blocked_count": int(order_result.diagnostics.get("ev_gate_blocked_count", 0) or 0),
            "confidence_filtered_count": int(order_result.diagnostics.get("confidence_filtered_count", 0) or 0),
            "avg_expected_net_return_traded": float(
                order_result.diagnostics.get("avg_expected_net_return_traded", 0.0) or 0.0
            ),
            "avg_expected_net_return_blocked": float(
                order_result.diagnostics.get("avg_expected_net_return_blocked", 0.0) or 0.0
            ),
            "avg_ev_executed_trades": float(order_result.diagnostics.get("avg_ev_executed_trades", 0.0) or 0.0),
            "avg_raw_ev_executed_trades": float(
                order_result.diagnostics.get("avg_raw_ev_executed_trades", 0.0) or 0.0
            ),
            "avg_normalized_ev_executed_trades": float(
                order_result.diagnostics.get("avg_normalized_ev_executed_trades", 0.0) or 0.0
            ),
            "avg_ev_weighting_score": float(order_result.diagnostics.get("avg_ev_weighting_score", 0.0) or 0.0),
            "avg_ev_confidence": float(order_result.diagnostics.get("avg_ev_confidence", 1.0)),
            "avg_ev_confidence_multiplier": float(
                order_result.diagnostics.get("avg_ev_confidence_multiplier", 1.0)
            ),
            "avg_ev_score_before_confidence": float(
                order_result.diagnostics.get("avg_ev_score_before_confidence", 0.0) or 0.0
            ),
            "avg_ev_score_after_confidence": float(
                order_result.diagnostics.get("avg_ev_score_after_confidence", 0.0) or 0.0
            ),
            "ev_weighted_exposure": float(order_result.diagnostics.get("ev_weighted_exposure", 0.0) or 0.0),
            "avg_ev_weight_multiplier": float(order_result.diagnostics.get("avg_ev_weight_multiplier", 1.0) or 1.0),
            "regression_prediction_available_count": int(
                order_result.diagnostics.get("regression_prediction_available_count", 0) or 0
            ),
            "regression_prediction_missing_count": int(
                order_result.diagnostics.get("regression_prediction_missing_count", 0) or 0
            ),
            "avg_regression_ev_executed_trades": float(
                order_result.diagnostics.get("avg_regression_ev_executed_trades", 0.0) or 0.0
            ),
            "avg_regression_ev_weighting_score": float(
                order_result.diagnostics.get("avg_regression_ev_weighting_score", 0.0) or 0.0
            ),
            "regression_ev_weighted_exposure": float(
                order_result.diagnostics.get("regression_ev_weighted_exposure", 0.0) or 0.0
            ),
            "ev_distribution": dict(order_result.diagnostics.get("ev_distribution", {}) or {}),
            "ev_calibration_summary": dict(order_result.diagnostics.get("ev_calibration_summary", {}) or {}),
            "ev_model_training_window": {
                "start": (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("training_window_start"),
                "end": (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("training_window_end"),
            },
            "ev_model_sample_count": int(
                (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("training_sample_count", 0) or 0
            ),
            "ev_labeled_row_count": int(
                (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("labeled_row_count", 0) or 0
            ),
            "ev_excluded_unlabeled_row_count": int(
                (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("excluded_unlabeled_row_count", 0) or 0
            ),
            "ev_average_target_value": float(
                (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("average_target_value", 0.0) or 0.0
            ),
            "ev_positive_label_rate": float(
                (order_result.diagnostics.get("ev_gate_training_summary") or {}).get("positive_label_rate", 0.0) or 0.0
            ),
            "candidate_dataset_row_count": int(order_result.diagnostics.get("candidate_dataset_row_count", 0) or 0),
            "candidate_executed_count": int(order_result.diagnostics.get("candidate_executed_count", 0) or 0),
            "candidate_skipped_count": int(order_result.diagnostics.get("candidate_skipped_count", 0) or 0),
            "skipped_due_to_entry_band_count": int(
                order_result.diagnostics.get("skipped_due_to_entry_band_count", 0) or 0
            ),
            "skipped_due_to_hold_zone_count": int(
                order_result.diagnostics.get("skipped_due_to_hold_zone_count", 0) or 0
            ),
            "skipped_trades_count": int(order_result.diagnostics.get("skipped_trades_count", 0) or 0),
            "skipped_turnover": float(order_result.diagnostics.get("skipped_turnover", 0.0) or 0.0),
            "effective_turnover_reduction": float(
                order_result.diagnostics.get("effective_turnover_reduction", 0.0) or 0.0
            ),
            "auto_apply_fills": bool(auto_apply_fills),
            "fill_application_status": accounting_summary["fill_application_status"],
            "ensemble_enabled": bool(config.ensemble_enabled and config.signal_source == "ensemble"),
            "ensemble_mode": config.ensemble_mode,
            "ensemble_weight_method": config.ensemble_weight_method,
            "latest_data_source": target_diagnostics.get(
                "latest_data_source", target_diagnostics.get("latest_price_source")
            ),
            "latest_data_fallback_used": bool(
                target_diagnostics.get(
                    "latest_data_fallback_used", target_diagnostics.get("latest_price_fallback_used", False)
                )
            ),
            "latest_bar_timestamp": target_diagnostics.get("latest_bar_timestamp"),
            "latest_bar_age_seconds": target_diagnostics.get("latest_bar_age_seconds"),
            "latest_data_stale": target_diagnostics.get("latest_data_stale"),
        },
        "accounting": accounting_summary,
        "pnl_attribution": attribution_payload,
    }
    diagnostics.update(extra_diagnostics or {})

    run_id = f"{config.preset_name or 'manual'}|{config.strategy}|{config.universe_name or 'symbols'}|{as_of}"
    decision_bundle = enrich_bundle_with_orders(
        decision_bundle,
        timestamp=as_of,
        run_id=run_id,
        cycle_id=as_of,
        strategy_id=config.strategy,
        universe_id=config.universe_name,
        current_positions=state.positions,
        latest_target_weights=latest_effective_weights,
        scheduled_target_weights=latest_scheduled_weights,
        latest_prices=latest_prices,
        orders=executable_orders,
        execution_payload=execution_diagnostics,
        reserve_cash_pct=config.reserve_cash_pct,
        portfolio_equity=state.equity,
    )

    return PaperTradingRunResult(
        as_of=as_of,
        state=state,
        latest_prices=latest_prices,
        latest_scores=latest_scores,
        latest_target_weights=latest_effective_weights,
        scheduled_target_weights=latest_scheduled_weights,
        orders=executable_orders,
        fills=fills,
        skipped_symbols=skipped_symbols,
        diagnostics=diagnostics,
        price_snapshots=list(price_snapshots or []),
        decision_bundle=decision_bundle,
        universe_bundle=universe_bundle,
        attribution=attribution_payload,
    )


def run_paper_trading_cycle(
    *,
    config: PaperTradingConfig,
    state_store: JsonPaperStateStore,
    execution_config: ExecutionConfig | None = None,
    auto_apply_fills: bool = False,
) -> PaperTradingRunResult:
    target_construction_service.load_feature_frame = load_feature_frame
    target_construction_service.resolve_feature_frame_path = resolve_feature_frame_path
    target_construction_service.run_xsec_momentum_topn = run_xsec_momentum_topn
    target_construction_service.normalize_price_frame = normalize_price_frame
    target_construction_service.SIGNAL_REGISTRY = SIGNAL_REGISTRY
    target_construction_service.build_group_series = build_group_series
    target_construction_service.build_top_n_portfolio_weights = build_top_n_portfolio_weights
    target_construction_service.normalize_paper_weighting_scheme = normalize_paper_weighting_scheme
    target_construction_service.ExecutionPolicy = ExecutionPolicy
    target_construction_service.build_executed_weights = build_executed_weights
    target_result = build_target_construction_result(config=config)
    return run_paper_trading_cycle_for_targets(
        config=config,
        state_store=state_store,
        as_of=target_result.as_of,
        latest_prices=target_result.latest_prices,
        latest_scores=target_result.latest_scores,
        latest_scheduled_weights=target_result.scheduled_target_weights,
        latest_effective_weights=target_result.effective_target_weights,
        target_diagnostics=target_result.target_diagnostics,
        skipped_symbols=target_result.skipped_symbols,
        extra_diagnostics=target_result.extra_diagnostics,
        price_snapshots=target_result.price_snapshots,
        decision_bundle=target_result.decision_bundle,
        universe_bundle=target_result.universe_bundle,
        execution_config=execution_config,
        auto_apply_fills=auto_apply_fills,
    )


def write_paper_trading_artifacts(
    *,
    result: PaperTradingRunResult,
    output_dir: str | Path,
    metadata_dir: str | Path | None = METADATA_DIR,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    orders_path = output_path / "paper_orders.csv"
    fills_path = output_path / "paper_fills.csv"
    equity_snapshot_path = output_path / "paper_equity_snapshot.csv"
    positions_path = output_path / "paper_positions.csv"
    targets_path = output_path / "paper_target_weights.csv"
    execution_price_snapshot_path = output_path / "execution_price_snapshot.csv"
    summary_path = output_path / "paper_summary.json"
    portfolio_performance_summary_path = output_path / "portfolio_performance_summary.json"
    execution_summary_path = output_path / "execution_summary.json"
    strategy_contribution_summary_path = output_path / "strategy_contribution_summary.json"
    trades_path = output_path / "paper_trades.csv"

    pd.DataFrame([asdict(order) for order in result.orders]).to_csv(orders_path, index=False)
    pd.DataFrame(
        sorted(
            [
                {
                    "symbol": position.symbol,
                    "quantity": int(position.quantity),
                    "avg_price": float(position.avg_price),
                    "last_price": float(position.last_price),
                    "cost_basis": float(position.cost_basis),
                    "market_value": float(position.market_value),
                    "unrealized_pnl": float(position.unrealized_pnl),
                    "portfolio_weight": float(position.market_value / result.state.equity)
                    if result.state.equity > 0
                    else 0.0,
                }
                for position in result.state.positions.values()
            ],
            key=lambda row: row["symbol"],
        )
    ).to_csv(
        positions_path,
        index=False,
    )
    pd.DataFrame(
        [
            {
                "symbol": symbol,
                "scheduled_target_weight": result.scheduled_target_weights.get(symbol, 0.0),
                "effective_target_weight": weight,
                "latest_price": result.latest_prices.get(symbol),
                "latest_score": result.latest_scores.get(symbol),
            }
            for symbol, weight in sorted(result.latest_target_weights.items())
        ]
    ).to_csv(targets_path, index=False)
    pd.DataFrame([asdict(snapshot) for snapshot in result.price_snapshots]).to_csv(
        execution_price_snapshot_path,
        index=False,
    )

    extra_paths: dict[str, Path] = {}
    if result.diagnostics.get("signal_source") == "composite":
        composite_scores_path = output_path / "daily_composite_scores.csv"
        approved_targets_path = output_path / "approved_target_weights.csv"
        composite_diagnostics_path = output_path / "composite_diagnostics.json"
        pd.DataFrame(
            result.diagnostics.get("latest_composite_scores", []),
        ).to_csv(composite_scores_path, index=False)
        pd.DataFrame(
            result.diagnostics.get("approved_target_weights", []),
        ).to_csv(approved_targets_path, index=False)
        composite_diagnostics_path.write_text(
            json.dumps(
                {
                    "selected_signals": result.diagnostics.get("selected_signals", []),
                    "excluded_signals": result.diagnostics.get("excluded_signals", []),
                    "latest_component_scores": result.diagnostics.get("latest_component_scores", []),
                    "liquidity_exclusions": result.diagnostics.get("liquidity_exclusions", []),
                    "artifact_dir": result.diagnostics.get("artifact_dir"),
                    "weighting_scheme": result.diagnostics.get("weighting_scheme"),
                    "portfolio_mode": result.diagnostics.get("portfolio_mode"),
                    "horizon": result.diagnostics.get("horizon"),
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        extra_paths = {
            "daily_composite_scores_path": composite_scores_path,
            "approved_target_weights_path": approved_targets_path,
            "composite_diagnostics_path": composite_diagnostics_path,
        }
    elif result.diagnostics.get("signal_source") == "ensemble":
        ensemble_snapshot_path = output_path / "paper_ensemble_decision_snapshot.csv"
        pd.DataFrame(result.diagnostics.get("ensemble_snapshot", [])).to_csv(ensemble_snapshot_path, index=False)
        extra_paths = {
            "paper_ensemble_decision_snapshot_path": ensemble_snapshot_path,
        }

    pd.DataFrame(
        [
            {
                "as_of": result.as_of,
                **asdict(fill),
            }
            for fill in result.fills
        ]
    ).to_csv(fills_path, index=False)
    open_trade_rows = [
        {
            "trade_id": lot.trade_id,
            "symbol": lot.symbol,
            "side": lot.side,
            "qty": abs(int(lot.remaining_quantity)),
            "entry_ts": lot.entry_as_of,
            "entry_reference_price": float(lot.entry_reference_price),
            "entry_price": float(lot.entry_price),
            "exit_ts": None,
            "exit_reference_price": None,
            "exit_price": None,
            "gross_realized_pnl": 0.0,
            "net_realized_pnl": 0.0,
            "realized_pnl": 0.0,
            "slippage_cost": float(lot.entry_slippage_cost),
            "spread_cost": float(lot.entry_spread_cost),
            "commission_cost": float(lot.entry_commission_cost),
            "total_execution_cost": float(lot.entry_total_execution_cost),
            "status": "open",
            "strategy_id": lot.strategy_id,
            "signal_source": lot.signal_source,
            "signal_family": lot.signal_family,
            "attribution_method": lot.attribution_method,
            "cost_model": lot.cost_model,
        }
        for lots in result.state.open_lots.values()
        for lot in lots
        if int(lot.remaining_quantity) != 0
    ]
    trade_rows = list(result.attribution.get("trade_rows", [])) + open_trade_rows
    trade_columns = (
        sorted({key for row in trade_rows for key in row}) if trade_rows else ["trade_id", "symbol", "status"]
    )
    if trades_path.exists():
        try:
            existing_trades = pd.read_csv(trades_path)
        except pd.errors.EmptyDataError:
            existing_trades = pd.DataFrame(columns=trade_columns)
    else:
        existing_trades = pd.DataFrame(columns=trade_columns)
    new_trades = pd.DataFrame(trade_rows, columns=trade_columns)
    existing_trades = existing_trades.reindex(columns=trade_columns)
    new_trades = new_trades.reindex(columns=trade_columns)
    combined_trades = (
        new_trades.copy() if existing_trades.empty else pd.concat([existing_trades, new_trades], ignore_index=True)
    )
    if not combined_trades.empty:
        combined_trades = combined_trades.drop_duplicates(subset=["trade_id"], keep="last")
        combined_trades = combined_trades.sort_values(["trade_id"], kind="stable")
    combined_trades.to_csv(trades_path, index=False)

    pd.DataFrame(
        [
            {
                "as_of": result.as_of,
                "cash": result.state.cash,
                "gross_market_value": result.state.gross_market_value,
                "equity": result.state.equity,
                "cost_basis": result.state.cost_basis,
                "gross_unrealized_pnl": float(result.diagnostics.get("accounting", {}).get("gross_unrealized_pnl", 0.0)),
                "unrealized_pnl": result.state.unrealized_pnl,
                "net_unrealized_pnl": result.state.unrealized_pnl,
                "gross_realized_pnl": float(
                    result.diagnostics.get("accounting", {}).get("gross_realized_pnl", result.state.cumulative_gross_realized_pnl)
                ),
                "cumulative_realized_pnl": result.state.cumulative_realized_pnl,
                "net_realized_pnl": result.state.cumulative_realized_pnl,
                "gross_total_pnl": float(result.diagnostics.get("accounting", {}).get("gross_total_pnl", 0.0)),
                "total_pnl": result.state.total_pnl,
                "net_total_pnl": result.state.total_pnl,
                "total_execution_cost": float(result.state.cumulative_execution_cost),
                "position_count": len(result.state.positions),
            }
        ]
    ).to_csv(equity_snapshot_path, index=False)

    summary_payload = {
        "as_of": result.as_of,
        "cash": result.state.cash,
        "equity": result.state.equity,
        "gross_market_value": result.state.gross_market_value,
        "orders": [asdict(order) for order in result.orders],
        "fills": [asdict(fill) for fill in result.fills],
        "skipped_symbols": result.skipped_symbols,
        "diagnostics": result.diagnostics,
        "price_snapshots": [asdict(snapshot) for snapshot in result.price_snapshots],
    }
    accounting_diag = dict(result.diagnostics.get("accounting", {}))
    execution_diag = dict(result.diagnostics.get("execution", {}))
    paper_execution_diag = dict(result.diagnostics.get("paper_execution", {}))
    if accounting_diag:
        summary_payload["accounting"] = accounting_diag
    target_diag = dict(result.diagnostics.get("target_construction", {}))
    handoff = dict(result.diagnostics.get("strategy_execution_handoff", {}))
    if handoff:
        summary_payload["strategy_execution_handoff"] = handoff
        summary_payload["active_strategy_count"] = int(handoff.get("active_strategy_count", 0) or 0)
        summary_payload["active_unconditional_count"] = int(handoff.get("active_unconditional_count", 0) or 0)
        summary_payload["active_conditional_count"] = int(handoff.get("active_conditional_count", 0) or 0)
        summary_payload["inactive_conditional_count"] = int(handoff.get("inactive_conditional_count", 0) or 0)
        summary_payload["source_portfolio_path"] = handoff.get("source_portfolio_path")
        summary_payload["activation_applied"] = bool(handoff.get("activation_applied", False))
    for key in (
        "requested_active_strategy_count",
        "requested_symbol_count",
        "pre_validation_target_symbol_count",
        "post_validation_target_symbol_count",
        "usable_symbol_count",
        "skipped_symbol_count",
        "target_drop_stage",
        "zero_target_reason",
        "target_drop_reason",
        "generated_preset_path",
        "signal_artifact_path",
        "latest_price_source_summary",
    ):
        if key in target_diag:
            summary_payload[key] = target_diag[key]
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    portfolio_performance_summary_path.write_text(
        json.dumps(
            {
                "as_of": result.as_of,
                "preset_name": result.diagnostics.get("preset_name"),
                "signal_source": result.diagnostics.get("signal_source"),
                "accounting": accounting_diag,
                "price_snapshot_count": len(result.price_snapshots),
                "position_count": len(result.state.positions),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    execution_summary_path.write_text(
        json.dumps(
            {
                "as_of": result.as_of,
                "requested_order_count": len(result.orders),
                "fill_count": len(result.fills),
                "order_generation": result.diagnostics.get("order_generation", {}),
                "execution": execution_diag,
                "paper_execution": paper_execution_diag,
                "accounting": accounting_diag,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    strategy_contribution_summary_path.write_text(
        json.dumps(
            {
                "as_of": result.as_of,
                "sleeve_contribution": (
                    (target_diag.get("multi_strategy_allocation") or {}).get("sleeve_contribution", {})
                    if isinstance(target_diag.get("multi_strategy_allocation"), dict)
                    else {}
                ),
                "normalized_capital_weights": (
                    (target_diag.get("multi_strategy_allocation") or {}).get("normalized_capital_weights", {})
                    if isinstance(target_diag.get("multi_strategy_allocation"), dict)
                    else {}
                ),
                "activation_summary": handoff,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    paths = {
        "orders_path": orders_path,
        "fills_path": fills_path,
        "equity_snapshot_path": equity_snapshot_path,
        "positions_path": positions_path,
        "targets_path": targets_path,
        "execution_price_snapshot_path": execution_price_snapshot_path,
        "summary_path": summary_path,
        "portfolio_performance_summary_path": portfolio_performance_summary_path,
        "execution_summary_json_path": execution_summary_path,
        "strategy_contribution_summary_path": strategy_contribution_summary_path,
        "paper_trades_path": trades_path,
    }
    execution_payload = result.diagnostics.get("execution", {})
    if execution_payload.get("execution_summary"):
        simulation_result = ExecutionSimulationResult(
            requested_orders=[ExecutionOrderRequest(**row) for row in execution_payload.get("requested_orders", [])],
            executable_orders=[ExecutableOrder(**row) for row in execution_payload.get("executable_orders", [])],
            rejected_orders=[RejectedOrder(**row) for row in execution_payload.get("rejected_orders", [])],
            summary=ExecutionSummary(**execution_payload.get("execution_summary", {})),
            liquidity_diagnostics=[
                LiquidityDiagnostic(**row) for row in execution_payload.get("liquidity_constraints_report", [])
            ],
            turnover_rows=execution_payload.get("turnover_summary", []),
            symbol_tradeability_rows=execution_payload.get("symbol_tradeability_report", []),
        )
        execution_paths = write_execution_artifacts(simulation_result, output_path)
        paths.update(execution_paths)
    paths.update(write_decision_journal_artifacts(bundle=result.decision_bundle, output_dir=output_path))
    paths.update(write_pnl_attribution_artifacts(output_dir=output_path, attribution_payload=result.attribution))
    paths.update(
        write_trade_ev_lifecycle_artifacts(
            output_dir=output_path,
            lifecycle_rows=build_trade_ev_lifecycle_rows(
                trade_rows=list(result.attribution.get("trade_rows", [])),
            ),
        )
    )
    paths.update(
        write_trade_ev_artifacts(
            output_dir=output_path,
            training_summary=dict((result.diagnostics.get("order_generation") or {}).get("ev_gate_training_summary") or {}),
            prediction_rows=list((result.diagnostics.get("order_generation") or {}).get("ev_prediction_rows") or []),
            candidate_rows=list((result.diagnostics.get("order_generation") or {}).get("candidate_trade_rows") or []),
            calibration_rows=list((result.diagnostics.get("order_generation") or {}).get("ev_calibration_rows") or []),
            calibration_summary=dict((result.diagnostics.get("order_generation") or {}).get("ev_calibration_summary") or {}),
        )
    )
    paths.update(
        write_universe_provenance_artifacts(
            bundle=result.universe_bundle,
            output_dir=output_path,
            metadata_dir=metadata_dir,
        )
    )
    paths.update(extra_paths)
    return paths
