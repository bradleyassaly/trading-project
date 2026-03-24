from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
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
    PaperTradingConfig,
    PaperTradingRunResult,
)
from trading_platform.paper.ledger import append_equity_snapshot, append_fills
from trading_platform.paper.slippage import apply_order_slippage, validate_slippage_config
from trading_platform.risk.pre_trade_checks import validate_orders
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
        return PaperPortfolioState(
            as_of=payload.get("as_of"),
            cash=float(payload.get("cash", 0.0)),
            positions=positions,
            last_targets={
                symbol: float(weight)
                for symbol, weight in payload.get("last_targets", {}).items()
            },
        )

    def save(self, state: PaperPortfolioState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "as_of": state.as_of,
            "cash": state.cash,
            "positions": {
                symbol: asdict(position)
                for symbol, position in sorted(state.positions.items())
            },
            "last_targets": state.last_targets,
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
) -> tuple[str, dict[str, float], dict[str, float], dict[str, float], dict[str, float], dict[str, Any], list[str], list[PaperExecutionPriceSnapshot]]:
    target_construction_service.load_feature_frame = load_feature_frame
    target_construction_service.resolve_feature_frame_path = resolve_feature_frame_path
    target_construction_service.run_xsec_momentum_topn = run_xsec_momentum_topn
    target_construction_service.normalize_price_frame = normalize_price_frame
    return shared_compute_latest_xsec_target_weights(config=config)


def bootstrap_paper_portfolio_state(
    *,
    initial_cash: float,
) -> PaperPortfolioState:
    return PaperPortfolioState(cash=float(initial_cash))


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


def generate_rebalance_orders(
    *,
    state: PaperPortfolioState,
    latest_target_weights: dict[str, float],
    latest_prices: dict[str, float],
    min_trade_dollars: float = 25.0,
    lot_size: int = 1,
    reserve_cash_pct: float = 0.0,
    provenance_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> OrderGenerationResult:
    equity = state.equity
    investable_equity = equity * (1.0 - reserve_cash_pct)
    if investable_equity < 0:
        raise ValueError("Investable equity cannot be negative")

    all_symbols = sorted(set(state.positions.keys()) | set(latest_target_weights.keys()))
    diagnostics: dict[str, Any] = {
        "equity": equity,
        "investable_equity": investable_equity,
        "reserve_cash_pct": reserve_cash_pct,
        "current_cash": state.cash,
    }
    orders: list[PaperOrder] = []
    execution_requests = build_execution_requests_from_target_weights(
        target_weights=latest_target_weights,
        current_positions=state.positions,
        latest_prices=latest_prices,
        portfolio_equity=equity,
        reserve_cash_pct=reserve_cash_pct,
        provenance_by_symbol=provenance_by_symbol,
    )
    for request in execution_requests:
        notional = float(request.requested_notional)
        if notional < min_trade_dollars:
            continue
        target_quantity = (int(request.target_shares) // lot_size) * lot_size if lot_size > 0 else int(request.target_shares)
        orders.append(
            PaperOrder(
                symbol=request.symbol,
                side=request.side,
                quantity=int(request.requested_shares),
                reference_price=float(request.price),
                target_weight=float(request.target_weight),
                current_quantity=int(request.current_shares),
                target_quantity=int(target_quantity),
                notional=notional,
                reason="rebalance_to_target",
            )
        )

    diagnostics["order_count"] = len(orders)
    diagnostics["symbols_considered"] = len(all_symbols)
    diagnostics["target_weight_sum"] = float(sum(latest_target_weights.values()))
    diagnostics["estimated_buy_notional"] = float(
        sum(order.notional for order in orders if order.side == "BUY")
    )
    diagnostics["estimated_sell_notional"] = float(
        sum(order.notional for order in orders if order.side == "SELL")
    )
    return OrderGenerationResult(
        orders=orders,
        target_weights=latest_target_weights,
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
                target_quantity=original.current_quantity + (executable.adjusted_shares if original.side == "BUY" else -executable.adjusted_shares),
                notional=executable.adjusted_notional,
                reason=executable.clipping_reason or original.reason,
                expected_fill_price=executable.estimated_fill_price,
                expected_fees=executable.commission,
                expected_slippage_bps=executable.slippage_bps,
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
            avg_price = (
                (current.avg_price * prior_quantity) + (fill_price * signed_qty)
            ) / new_quantity
        else:
            avg_price = current.avg_price

        state.positions[order.symbol] = PaperPosition(
            symbol=order.symbol,
            quantity=new_quantity,
            avg_price=float(avg_price),
            last_price=fill_price,
        )

    return state


def _apply_execution_orders_to_state(
    *,
    state: PaperPortfolioState,
    orders: list[PaperOrder],
) -> tuple[PaperPortfolioState, list]:
    fills = []
    for order in orders:
        fill_price = float(order.expected_fill_price or order.reference_price)
        signed_qty = order.quantity if order.side == "BUY" else -order.quantity
        state.cash += (-signed_qty * fill_price) - float(order.expected_fees)

        current = state.positions.get(order.symbol)
        prior_quantity = current.quantity if current else 0
        new_quantity = prior_quantity + signed_qty
        if new_quantity == 0:
            state.positions.pop(order.symbol, None)
        else:
            if current is None:
                avg_price = fill_price
            elif signed_qty > 0 and prior_quantity >= 0:
                avg_price = (((current.avg_price * prior_quantity) + (fill_price * signed_qty)) / new_quantity)
            else:
                avg_price = current.avg_price
            state.positions[order.symbol] = PaperPosition(
                symbol=order.symbol,
                quantity=new_quantity,
                avg_price=float(avg_price),
                last_price=fill_price,
            )
        fills.append(
            BrokerFill(
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                fill_price=fill_price,
                notional=float(order.quantity) * fill_price,
                commission=float(order.expected_fees),
                slippage_bps=float(order.expected_slippage_bps),
            )
        )
    return state, fills


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
        "slippage_order_count": len(adjusted),
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

    order_result = generate_rebalance_orders(
        state=state,
        latest_target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        min_trade_dollars=config.min_trade_dollars,
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
    if auto_apply_fills and executable_orders:
        state, fills = _apply_execution_orders_to_state(
            state=state,
            orders=executable_orders,
        )
        state = sync_state_prices(state, latest_prices)

    state.as_of = as_of
    state.last_targets = latest_effective_weights.copy()
    state_store.save(state)

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
            "ensemble_enabled": bool(config.ensemble_enabled and config.signal_source == "ensemble"),
            "ensemble_mode": config.ensemble_mode,
            "ensemble_weight_method": config.ensemble_weight_method,
            "latest_data_source": target_diagnostics.get("latest_data_source", target_diagnostics.get("latest_price_source")),
            "latest_data_fallback_used": bool(target_diagnostics.get("latest_data_fallback_used", target_diagnostics.get("latest_price_fallback_used", False))),
            "latest_bar_timestamp": target_diagnostics.get("latest_bar_timestamp"),
            "latest_bar_age_seconds": target_diagnostics.get("latest_bar_age_seconds"),
            "latest_data_stale": target_diagnostics.get("latest_data_stale"),
        },
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

    pd.DataFrame([asdict(order) for order in result.orders]).to_csv(orders_path, index=False)
    pd.DataFrame([asdict(position) for position in result.state.positions.values()]).to_csv(
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

    pd.DataFrame(
        [
            {
                "as_of": result.as_of,
                "cash": result.state.cash,
                "gross_market_value": result.state.gross_market_value,
                "equity": result.state.equity,
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
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    paths = {
        "orders_path": orders_path,
        "fills_path": fills_path,
        "equity_snapshot_path": equity_snapshot_path,
        "positions_path": positions_path,
        "targets_path": targets_path,
        "execution_price_snapshot_path": execution_price_snapshot_path,
        "summary_path": summary_path,
    }
    execution_payload = result.diagnostics.get("execution", {})
    if execution_payload.get("execution_summary"):
        simulation_result = ExecutionSimulationResult(
            requested_orders=[ExecutionOrderRequest(**row) for row in execution_payload.get("requested_orders", [])],
            executable_orders=[ExecutableOrder(**row) for row in execution_payload.get("executable_orders", [])],
            rejected_orders=[RejectedOrder(**row) for row in execution_payload.get("rejected_orders", [])],
            summary=ExecutionSummary(**execution_payload.get("execution_summary", {})),
            liquidity_diagnostics=[LiquidityDiagnostic(**row) for row in execution_payload.get("liquidity_constraints_report", [])],
            turnover_rows=execution_payload.get("turnover_summary", []),
            symbol_tradeability_rows=execution_payload.get("symbol_tradeability_report", []),
        )
        execution_paths = write_execution_artifacts(simulation_result, output_path)
        paths.update(execution_paths)
    paths.update(write_decision_journal_artifacts(bundle=result.decision_bundle, output_dir=output_path))
    paths.update(write_universe_provenance_artifacts(bundle=result.universe_bundle, output_dir=output_path))
    paths.update(extra_paths)
    return paths

