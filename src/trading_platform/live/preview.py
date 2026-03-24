from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.artifacts.summary_utils import (
    add_standard_summary_fields,
    warnings_and_errors_from_checks,
    workflow_status_from_checks,
)
from trading_platform.broker.alpaca_broker import AlpacaBroker, AlpacaBrokerConfig
from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
    LiveBrokerPosition,
)
from trading_platform.execution.realism import (
    ExecutableOrder,
    ExecutionConfig,
    ExecutionOrderRequest,
    ExecutionSimulationResult,
    ExecutionSummary,
    LiquidityDiagnostic,
    RejectedOrder,
    simulate_execution,
    write_execution_artifacts,
)
from trading_platform.execution.open_order_adjustment import adjust_orders_for_open_orders
from trading_platform.execution.reconciliation import (
    ReconciliationResult,
    build_rebalance_orders_from_broker_state,
)
from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.decision_journal.service import (
    build_candidate_journal_for_snapshot,
    enrich_bundle_with_orders,
    write_decision_journal_artifacts,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import (
    _compute_latest_xsec_target_weights,
    compute_latest_target_weights,
    load_signal_snapshot,
)
from trading_platform.services.target_construction_service import build_target_construction_result
from trading_platform.universe_provenance.models import UniverseBuildBundle
from trading_platform.universe_provenance.service import (
    build_universe_provenance_bundle,
    write_universe_provenance_artifacts,
)


@dataclass(frozen=True)
class LivePreviewConfig:
    symbols: list[str]
    preset_name: str | None = None
    universe_name: str | None = None
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    lookback_bars: int | None = None
    skip_bars: int = 0
    top_n: int = 1
    weighting_scheme: str = "equal"
    vol_lookback_bars: int = 20
    rebalance_bars: int | None = None
    portfolio_construction_mode: str = "pure_topn"
    max_position_weight: float | None = None
    min_avg_dollar_volume: float | None = None
    max_names_per_sector: int | None = None
    turnover_buffer_bps: float = 0.0
    max_turnover_per_rebalance: float | None = None
    benchmark: str | None = None
    initial_cash: float = 100_000.0
    min_trade_dollars: float = 25.0
    lot_size: int = 1
    reserve_cash_pct: float = 0.0
    order_type: str = "market"
    time_in_force: str = "day"
    broker: str = "mock"
    mock_equity: float = 100_000.0
    mock_cash: float = 100_000.0
    mock_positions_path: str | None = None
    sub_universe_id: str | None = None
    universe_filters: list[dict[str, Any]] = field(default_factory=list)
    universe_membership_path: str | None = None
    market_regime_path: str | None = None
    output_dir: Path = Path("artifacts/live_dry_run")


@dataclass(frozen=True)
class LivePreviewHealthCheck:
    check_name: str
    status: str
    message: str
    timestamp: str
    preset: str | None
    strategy: str
    universe: str | None


@dataclass
class LivePreviewResult:
    run_id: str
    as_of: str
    config: LivePreviewConfig
    account: BrokerAccount
    positions: dict[str, LiveBrokerPosition]
    open_orders: list[LiveBrokerOrderStatus]
    latest_prices: dict[str, float]
    target_weights: dict[str, float]
    target_diagnostics: dict[str, Any]
    reconciliation: ReconciliationResult
    adjusted_orders: list[LiveBrokerOrderRequest]
    order_adjustment_diagnostics: dict[str, Any]
    execution_result: ExecutionSimulationResult | None
    reconciliation_rows: list[dict[str, Any]]
    health_checks: list[LivePreviewHealthCheck]
    decision_bundle: DecisionJournalBundle | None = None
    universe_bundle: UniverseBuildBundle | None = None
    artifacts: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class MockBrokerConfig:
    equity: float = 100_000.0
    cash: float = 100_000.0
    positions: dict[str, LiveBrokerPosition] | None = None
    open_orders: list[LiveBrokerOrderStatus] | None = None


class MockBroker:
    def __init__(self, config: MockBrokerConfig) -> None:
        self.config = config

    def get_account(self) -> BrokerAccount:
        return BrokerAccount(
            account_id="mock-account",
            cash=float(self.config.cash),
            equity=float(self.config.equity),
            buying_power=float(self.config.cash),
            currency="USD",
        )

    def get_positions(self) -> dict[str, LiveBrokerPosition]:
        return dict(self.config.positions or {})

    def list_open_orders(self) -> list[LiveBrokerOrderStatus]:
        return list(self.config.open_orders or [])


def load_mock_positions(path: str | None) -> dict[str, LiveBrokerPosition]:
    if not path:
        return {}

    df = pd.read_csv(path)
    required = {"symbol", "quantity", "avg_price", "market_price"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mock positions file missing required columns: {sorted(missing)}")

    positions: dict[str, LiveBrokerPosition] = {}
    for row in df.to_dict(orient="records"):
        symbol = str(row["symbol"])
        quantity = int(row["quantity"])
        avg_price = float(row["avg_price"])
        market_price = float(row["market_price"])
        positions[symbol] = LiveBrokerPosition(
            symbol=symbol,
            quantity=quantity,
            avg_price=avg_price,
            market_price=market_price,
            market_value=quantity * market_price,
        )
    return positions


def _resolve_broker(config: LivePreviewConfig):
    if config.broker == "mock":
        return MockBroker(
            MockBrokerConfig(
                equity=config.mock_equity,
                cash=config.mock_cash,
                positions=load_mock_positions(config.mock_positions_path),
            )
        )
    return AlpacaBroker(AlpacaBrokerConfig.from_env())


def _build_paper_config(config: LivePreviewConfig) -> PaperTradingConfig:
    return PaperTradingConfig(
        symbols=config.symbols,
        preset_name=config.preset_name,
        universe_name=config.universe_name,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
        lookback_bars=config.lookback_bars,
        skip_bars=config.skip_bars,
        top_n=config.top_n,
        weighting_scheme=config.weighting_scheme,
        vol_window=config.vol_lookback_bars,
        rebalance_bars=config.rebalance_bars,
        portfolio_construction_mode=config.portfolio_construction_mode,
        max_position_weight=config.max_position_weight,
        min_avg_dollar_volume=config.min_avg_dollar_volume,
        max_names_per_sector=config.max_names_per_sector,
        turnover_buffer_bps=config.turnover_buffer_bps,
        max_turnover_per_rebalance=config.max_turnover_per_rebalance,
        benchmark=config.benchmark,
        initial_cash=config.initial_cash,
        min_trade_dollars=config.min_trade_dollars,
        lot_size=config.lot_size,
        reserve_cash_pct=config.reserve_cash_pct,
        sub_universe_id=config.sub_universe_id,
        universe_filters=list(config.universe_filters),
        universe_membership_path=config.universe_membership_path,
        market_regime_path=config.market_regime_path,
    )


def _build_target_preview(
    config: LivePreviewConfig,
) -> tuple[str, dict[str, float], dict[str, float], dict[str, Any], DecisionJournalBundle | None, UniverseBuildBundle | None]:
    if (
        not config.universe_filters
        and not config.sub_universe_id
        and not config.universe_membership_path
        and not config.market_regime_path
    ):
        paper_config = _build_paper_config(config)
        universe_bundle = build_universe_provenance_bundle(
            symbols=config.symbols,
            base_universe_id=config.universe_name,
            sub_universe_id=config.sub_universe_id,
            filter_definitions=[],
        )
        if config.strategy == "xsec_momentum_topn":
            xsec_result = _compute_latest_xsec_target_weights(config=paper_config)
            if len(xsec_result) == 8:
                as_of, scheduled_target_weights, target_weights, latest_prices, latest_scores, target_diagnostics, skipped_symbols, _price_snapshots = xsec_result
            else:
                as_of, scheduled_target_weights, target_weights, latest_prices, latest_scores, target_diagnostics, skipped_symbols = xsec_result
            decision_bundle = build_candidate_journal_for_snapshot(
                timestamp=as_of,
                run_id=f"{config.preset_name or 'manual'}|{config.strategy}|{config.universe_name or 'symbols'}|{as_of}",
                cycle_id=as_of,
                strategy_id=config.strategy,
                universe_id=config.universe_name,
                base_universe_id=universe_bundle.summary.base_universe_id if universe_bundle.summary is not None else config.universe_name,
                sub_universe_id=universe_bundle.summary.sub_universe_id if universe_bundle.summary is not None else config.sub_universe_id,
                score_map=latest_scores,
                latest_prices=latest_prices,
                selected_weights=target_weights,
                scheduled_weights=scheduled_target_weights,
                skipped_symbols=skipped_symbols,
                selected_rejection_reasons=dict(target_diagnostics.get("excluded_reasons", {}))
                if isinstance(target_diagnostics.get("excluded_reasons"), dict)
                else None,
                universe_metadata_by_symbol={row.symbol: dict(row.metadata) for row in universe_bundle.membership_records},
            )
            return as_of, scheduled_target_weights, target_weights, latest_prices, target_diagnostics, decision_bundle, universe_bundle

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
        as_of, scheduled_target_weights, target_weights, target_diagnostics = compute_latest_target_weights(
            config=paper_config,
            snapshot=snapshot,
        )
        asset_return_map = {
            symbol: float(value)
            for symbol, value in snapshot.asset_returns.iloc[-1].dropna().items()
        } if not snapshot.asset_returns.empty else {}
        decision_bundle = build_candidate_journal_for_snapshot(
            timestamp=as_of,
            run_id=f"{config.preset_name or 'manual'}|{config.strategy}|{config.universe_name or 'symbols'}|{as_of}",
            cycle_id=as_of,
            strategy_id=config.strategy,
            universe_id=config.universe_name,
            base_universe_id=universe_bundle.summary.base_universe_id if universe_bundle.summary is not None else config.universe_name,
            sub_universe_id=universe_bundle.summary.sub_universe_id if universe_bundle.summary is not None else config.sub_universe_id,
            score_map=latest_scores,
            latest_prices=latest_prices,
            selected_weights=target_weights,
            scheduled_weights=scheduled_target_weights,
            skipped_symbols=snapshot.skipped_symbols,
            asset_return_map=asset_return_map,
            universe_metadata_by_symbol={row.symbol: dict(row.metadata) for row in universe_bundle.membership_records},
        )
        return as_of, scheduled_target_weights, target_weights, latest_prices, target_diagnostics, decision_bundle, universe_bundle

    target_result = build_target_construction_result(config=_build_paper_config(config))
    return (
        target_result.as_of,
        target_result.scheduled_target_weights,
        target_result.effective_target_weights,
        target_result.latest_prices,
        target_result.target_diagnostics,
        target_result.decision_bundle,
        target_result.universe_bundle,
    )


def _health_check(
    *,
    name: str,
    status: str,
    message: str,
    config: LivePreviewConfig,
    timestamp: str,
) -> LivePreviewHealthCheck:
    return LivePreviewHealthCheck(
        check_name=name,
        status=status,
        message=message,
        timestamp=timestamp,
        preset=config.preset_name,
        strategy=config.strategy,
        universe=config.universe_name,
    )


def _build_health_checks(
    *,
    config: LivePreviewConfig,
    as_of: str,
    account: BrokerAccount,
    positions: dict[str, LiveBrokerPosition],
    latest_prices: dict[str, float],
    target_weights: dict[str, float],
    target_diagnostics: dict[str, Any],
    reconciliation: ReconciliationResult,
    adjusted_orders: list[LiveBrokerOrderRequest],
    open_orders: list[LiveBrokerOrderStatus],
) -> list[LivePreviewHealthCheck]:
    checks: list[LivePreviewHealthCheck] = []
    checks.append(_health_check(name="broker_connectivity", status="pass", message=f"loaded account {account.account_id or 'unknown'}", config=config, timestamp=as_of))
    checks.append(_health_check(name="market_data", status="pass" if latest_prices else "fail", message=f"latest prices available for {len(latest_prices)} symbols", config=config, timestamp=as_of))

    target_weight_sum = float(sum(target_weights.values()))
    weight_status = "pass" if 0.0 <= target_weight_sum <= 1.05 else "fail"
    checks.append(_health_check(name="target_weight_sum", status=weight_status, message=f"target_weight_sum={target_weight_sum:.6f}", config=config, timestamp=as_of))

    duplicate_symbols = len(config.symbols) - len(set(config.symbols))
    checks.append(_health_check(name="duplicate_symbols", status="pass" if duplicate_symbols == 0 else "fail", message=f"duplicate_symbol_count={duplicate_symbols}", config=config, timestamp=as_of))

    selected_count = int(target_diagnostics.get("target_selected_count") or len(target_weights))
    selection_status = "pass" if selected_count > 0 else "warn"
    checks.append(_health_check(name="selected_set", status=selection_status, message=f"target_selected_count={selected_count}", config=config, timestamp=as_of))

    realized_holdings = target_diagnostics.get("realized_holdings_count")
    top_n = int(config.top_n or 0)
    if config.portfolio_construction_mode == "pure_topn" and isinstance(realized_holdings, (int, float)) and top_n > 0 and realized_holdings > top_n * 2:
        holdings_status = "warn"
        holdings_message = f"pure_topn realized_holdings_count={realized_holdings} materially exceeds top_n={top_n}"
    else:
        holdings_status = "pass"
        holdings_message = f"realized_holdings_count={realized_holdings}"
    checks.append(_health_check(name="holdings_reasonableness", status=holdings_status, message=holdings_message, config=config, timestamp=as_of))

    liquidity_excluded = int(target_diagnostics.get("liquidity_excluded_count") or 0)
    liquidity_status = "warn" if liquidity_excluded > max(5, int(len(config.symbols) * 0.1)) else "pass"
    checks.append(_health_check(name="liquidity_exclusions", status=liquidity_status, message=f"liquidity_excluded_count={liquidity_excluded}", config=config, timestamp=as_of))

    turnover_cap_bindings = int(target_diagnostics.get("turnover_cap_binding_count") or 0)
    cap_status = "warn" if turnover_cap_bindings > 0 else "pass"
    checks.append(_health_check(name="turnover_cap_bindings", status=cap_status, message=f"turnover_cap_binding_count={turnover_cap_bindings}", config=config, timestamp=as_of))

    open_order_count = len(open_orders)
    open_status = "warn" if open_order_count > 0 else "pass"
    checks.append(_health_check(name="open_orders", status=open_status, message=f"open_order_count={open_order_count}", config=config, timestamp=as_of))

    large_turnover = float(reconciliation.diagnostics.get("order_count", 0)) > max(25, len(config.symbols) // 2)
    checks.append(_health_check(name="order_count_sanity", status="warn" if large_turnover else "pass", message=f"order_count={reconciliation.diagnostics.get('order_count', 0)}", config=config, timestamp=as_of))

    target_notional = float(reconciliation.diagnostics.get("investable_equity", account.equity)) * float(sum(target_weights.values()))
    cash_residual = float(account.equity) - float(target_notional)
    residual_status = "warn" if float(account.equity) > 0 and abs(cash_residual / float(account.equity)) > 0.25 else "pass"
    checks.append(_health_check(name="cash_residual", status=residual_status, message=f"cash_residual={cash_residual:.2f}", config=config, timestamp=as_of))

    max_position_change = 0.0
    if float(account.equity) > 0:
        for symbol, target_weight in target_weights.items():
            current_value = float(positions[symbol].market_value) if symbol in positions else 0.0
            current_weight = current_value / float(account.equity)
            max_position_change = max(max_position_change, abs(float(target_weight) - current_weight))
    change_status = "warn" if max_position_change > 0.5 else "pass"
    checks.append(_health_check(name="single_position_change", status=change_status, message=f"max_single_position_change={max_position_change:.4f}", config=config, timestamp=as_of))

    blocked_symbols = [row.symbol for row in adjusted_orders if row.reason == "blocked"]
    checks.append(_health_check(name="output_ready", status="pass", message=f"adjusted_order_count={len(adjusted_orders)} blocked_symbol_count={len(blocked_symbols)}", config=config, timestamp=as_of))
    return checks


def _build_reconciliation_rows(
    *,
    config: LivePreviewConfig,
    account: BrokerAccount,
    positions: dict[str, LiveBrokerPosition],
    target_weights: dict[str, float],
    latest_prices: dict[str, float],
    reconciliation: ReconciliationResult,
    raw_orders: list[LiveBrokerOrderRequest],
    adjusted_orders: list[LiveBrokerOrderRequest],
    pending_deltas: dict[str, int],
) -> list[dict[str, Any]]:
    investable_equity = float(reconciliation.diagnostics.get("investable_equity", account.equity))
    current_weight_denominator = float(account.equity) if float(account.equity) else 1.0
    raw_order_map = {order.symbol: order for order in raw_orders}
    adjusted_order_map = {order.symbol: order for order in adjusted_orders}
    rows: list[dict[str, Any]] = []

    for symbol in sorted(set(positions) | set(target_weights) | set(reconciliation.target_quantities) | set(latest_prices)):
        price = float(latest_prices.get(symbol, positions.get(symbol).market_price if symbol in positions else 0.0))
        current_qty = int(positions[symbol].quantity) if symbol in positions else 0
        current_notional = current_qty * price
        current_weight = current_notional / current_weight_denominator if current_weight_denominator else 0.0
        target_weight = float(target_weights.get(symbol, 0.0))
        target_notional = investable_equity * target_weight
        target_qty = int(reconciliation.target_quantities.get(symbol, 0))
        delta_qty = target_qty - current_qty
        delta_notional = target_notional - current_notional
        raw_order = raw_order_map.get(symbol)
        adjusted_order = adjusted_order_map.get(symbol)
        pending_delta = int(pending_deltas.get(symbol, 0))

        reason = "already_at_target"
        blocked_flag = False
        warning_flag = False
        if price <= 0:
            reason = "missing_market_price"
            warning_flag = True
        elif raw_order is None and delta_qty != 0 and abs(delta_qty * price) < float(config.min_trade_dollars):
            reason = "below_min_trade_dollars"
        elif raw_order is not None and adjusted_order is None and pending_delta != 0:
            reason = "offset_by_open_orders"
            blocked_flag = True
        elif adjusted_order is not None:
            reason = adjusted_order.reason or "rebalance_to_target"
        elif raw_order is not None:
            reason = raw_order.reason or "rebalance_to_target"

        rows.append(
            {
                "symbol": symbol,
                "current_qty": current_qty,
                "current_weight": current_weight,
                "target_weight": target_weight,
                "target_notional": target_notional,
                "delta_notional": delta_notional,
                "current_price": price,
                "target_qty": target_qty,
                "delta_qty": delta_qty,
                "proposed_side": adjusted_order.side if adjusted_order else (raw_order.side if raw_order else ""),
                "proposed_qty": int(adjusted_order.quantity if adjusted_order else (raw_order.quantity if raw_order else 0)),
                "pending_open_order_qty": pending_delta,
                "reason": reason,
                "blocked_flag": blocked_flag,
                "warning_flag": warning_flag,
            }
        )
    return rows


def _simulate_execution_for_live_orders(
    *,
    config: LivePreviewConfig,
    account: BrokerAccount,
    positions: dict[str, LiveBrokerPosition],
    latest_prices: dict[str, float],
    target_weights: dict[str, float],
    reconciliation: ReconciliationResult,
    execution_config: ExecutionConfig,
) -> tuple[list[LiveBrokerOrderRequest], ExecutionSimulationResult]:
    requests = []
    for order in reconciliation.orders:
        current_quantity = int(positions[order.symbol].quantity) if order.symbol in positions else 0
        target_quantity = int(reconciliation.target_quantities.get(order.symbol, current_quantity))
        requests.append(
            ExecutionOrderRequest(
                symbol=order.symbol,
                side=order.side,
                requested_shares=order.quantity,
                requested_notional=float(order.quantity) * float(latest_prices.get(order.symbol, 0.0)),
                price=float(latest_prices.get(order.symbol, 0.0)),
                target_weight=float(target_weights.get(order.symbol, 0.0)),
                current_shares=current_quantity,
                target_shares=target_quantity,
            )
        )
    execution_result = simulate_execution(
        requests=requests,
        config=execution_config,
        current_cash=float(account.cash),
        current_equity=float(account.equity),
    )
    executable_orders = [
        LiveBrokerOrderRequest(
            symbol=order.symbol,
            side=order.side,
            quantity=order.adjusted_shares,
            order_type="market",
            time_in_force="day",
            reason=order.clipping_reason or "rebalance_to_target",
        )
        for order in execution_result.executable_orders
    ]
    return executable_orders, execution_result


def run_live_dry_run_preview(
    config: LivePreviewConfig,
    execution_config: ExecutionConfig | None = None,
) -> LivePreviewResult:
    as_of, scheduled_target_weights, target_weights, latest_prices, target_diagnostics, decision_bundle, universe_bundle = _build_target_preview(config)
    return run_live_dry_run_preview_for_targets(
        config=config,
        as_of=as_of,
        scheduled_target_weights=scheduled_target_weights,
        target_weights=target_weights,
        latest_prices=latest_prices,
        target_diagnostics=target_diagnostics,
        decision_bundle=decision_bundle,
        universe_bundle=universe_bundle,
        execution_config=execution_config,
    )


def run_live_dry_run_preview_for_targets(
    *,
    config: LivePreviewConfig,
    as_of: str,
    target_weights: dict[str, float],
    latest_prices: dict[str, float],
    target_diagnostics: dict[str, Any],
    scheduled_target_weights: dict[str, float] | None = None,
    decision_bundle: DecisionJournalBundle | None = None,
    universe_bundle: UniverseBuildBundle | None = None,
    execution_config: ExecutionConfig | None = None,
) -> LivePreviewResult:
    broker = _resolve_broker(config)
    account = broker.get_account()
    positions = broker.get_positions()
    open_orders = broker.list_open_orders() if hasattr(broker, "list_open_orders") else []
    reconciliation = build_rebalance_orders_from_broker_state(
        account=account,
        positions=positions,
        latest_target_weights=target_weights,
        latest_prices=latest_prices,
        reserve_cash_pct=config.reserve_cash_pct,
        min_trade_dollars=config.min_trade_dollars,
        lot_size=config.lot_size,
        order_type=config.order_type,
        time_in_force=config.time_in_force,
    )
    execution_result = None
    proposed_orders = reconciliation.orders
    if execution_config is not None:
        proposed_orders, execution_result = _simulate_execution_for_live_orders(
            config=config,
            account=account,
            positions=positions,
            latest_prices=latest_prices,
            target_weights=target_weights,
            reconciliation=reconciliation,
            execution_config=execution_config,
        )
    adjustment = adjust_orders_for_open_orders(
        proposed_orders=proposed_orders,
        open_orders=open_orders,
    )
    reconciliation_rows = _build_reconciliation_rows(
        config=config,
        account=account,
        positions=positions,
        target_weights=target_weights,
        latest_prices=latest_prices,
        reconciliation=reconciliation,
        raw_orders=reconciliation.orders,
        adjusted_orders=adjustment.adjusted_orders,
        pending_deltas=adjustment.pending_deltas,
    )
    health_checks = _build_health_checks(
        config=config,
        as_of=as_of,
        account=account,
        positions=positions,
        latest_prices=latest_prices,
        target_weights=target_weights,
        target_diagnostics=target_diagnostics,
        reconciliation=reconciliation,
        adjusted_orders=adjustment.adjusted_orders,
        open_orders=open_orders,
    )
    run_id = f"{config.preset_name or 'manual'}|{config.strategy}|{config.universe_name or 'symbols'}|{as_of}"
    journal_bundle = enrich_bundle_with_orders(
        decision_bundle,
        timestamp=as_of,
        run_id=run_id,
        cycle_id=as_of,
        strategy_id=config.strategy,
        universe_id=config.universe_name,
        current_positions=positions,
        latest_target_weights=target_weights,
        scheduled_target_weights=scheduled_target_weights or target_weights,
        latest_prices=latest_prices,
        orders=adjustment.adjusted_orders,
        execution_payload=execution_result.to_dict() if execution_result is not None else None,
        reserve_cash_pct=config.reserve_cash_pct,
        portfolio_equity=float(account.equity),
    )
    return LivePreviewResult(
        run_id=run_id,
        as_of=as_of,
        config=config,
        account=account,
        positions=positions,
        open_orders=open_orders,
        latest_prices=latest_prices,
        target_weights=target_weights,
        target_diagnostics=target_diagnostics,
        reconciliation=reconciliation,
        adjusted_orders=adjustment.adjusted_orders,
        order_adjustment_diagnostics=adjustment.diagnostics,
        execution_result=execution_result,
        reconciliation_rows=reconciliation_rows,
        health_checks=health_checks,
        decision_bundle=journal_bundle,
        universe_bundle=universe_bundle,
    )


def _write_markdown_summary(payload: dict[str, Any], health_checks: list[dict[str, Any]]) -> str:
    lines = [
        f"# Live Dry-Run Summary: {payload.get('preset_name') or payload.get('strategy')}",
        "",
        f"- Timestamp: `{payload['timestamp']}`",
        f"- Preset: `{payload.get('preset_name')}`",
        f"- Strategy: `{payload['strategy']}`",
        f"- Universe: `{payload.get('universe')}`",
        f"- Portfolio construction mode: `{payload.get('portfolio_construction_mode')}`",
        f"- Broker: `{payload['broker']}`",
        f"- Equity: `{payload['equity']:.2f}`",
        f"- Cash: `{payload['cash']:.2f}`",
        f"- Adjusted proposed orders: `{payload['adjusted_order_count']}`",
        f"- Selected names: `{','.join(payload.get('selected_names', []))}`",
        f"- Target names: `{','.join(payload.get('target_names', []))}`",
        "",
        "## Health Checks",
    ]
    for check in health_checks:
        lines.append(f"- `{check['status']}` {check['check_name']}: {check['message']}")
    return "\n".join(lines) + "\n"


def write_live_dry_run_artifacts(result: LivePreviewResult) -> dict[str, Path]:
    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    current_positions_path = output_dir / "live_dry_run_current_positions.csv"
    target_positions_path = output_dir / "live_dry_run_target_positions.csv"
    proposed_orders_path = output_dir / "live_dry_run_proposed_orders.csv"
    reconciliation_path = output_dir / "live_dry_run_reconciliation.csv"
    health_checks_path = output_dir / "live_dry_run_health_checks.csv"
    summary_json_path = output_dir / "live_dry_run_summary.json"
    summary_md_path = output_dir / "live_dry_run_summary.md"
    preview_summary_json_path = output_dir / "live_execution_preview_summary.json"
    preview_summary_md_path = output_dir / "live_execution_preview_summary.md"

    current_positions_rows = [
        {
            "symbol": position.symbol,
            "current_qty": position.quantity,
            "avg_price": position.avg_price,
            "market_price": position.market_price,
            "market_value": position.market_value,
        }
        for position in sorted(result.positions.values(), key=lambda item: item.symbol)
    ]
    pd.DataFrame(
        current_positions_rows,
        columns=["symbol", "current_qty", "avg_price", "market_price", "market_value"],
    ).to_csv(current_positions_path, index=False)

    target_positions_rows = [
        {
            "symbol": symbol,
            "target_weight": float(weight),
            "target_notional": float(result.reconciliation.diagnostics.get("investable_equity", result.account.equity)) * float(weight),
            "latest_price": result.latest_prices.get(symbol),
        }
        for symbol, weight in sorted(result.target_weights.items())
    ]
    pd.DataFrame(
        target_positions_rows,
        columns=["symbol", "target_weight", "target_notional", "latest_price"],
    ).to_csv(target_positions_path, index=False)

    pd.DataFrame(
        [asdict(order) for order in result.adjusted_orders],
        columns=["symbol", "side", "quantity", "order_type", "time_in_force", "limit_price", "client_order_id", "reason"],
    ).to_csv(proposed_orders_path, index=False)
    pd.DataFrame(
        result.reconciliation_rows,
        columns=[
            "symbol",
            "current_qty",
            "current_weight",
            "target_weight",
            "target_notional",
            "delta_notional",
            "current_price",
            "target_qty",
            "delta_qty",
            "proposed_side",
            "proposed_qty",
            "pending_open_order_qty",
            "reason",
            "blocked_flag",
            "warning_flag",
        ],
    ).to_csv(reconciliation_path, index=False)

    target_diagnostics = result.target_diagnostics
    summary_payload = {
        "run_id": result.run_id,
        "timestamp": result.as_of,
        "preset_name": result.config.preset_name,
        "strategy": result.config.strategy,
        "universe": result.config.universe_name,
        "portfolio_construction_mode": result.config.portfolio_construction_mode,
        "benchmark": result.config.benchmark,
        "broker": result.config.broker,
        "cash": float(result.account.cash),
        "equity": float(result.account.equity),
        "gross_exposure": float(target_diagnostics.get("average_gross_exposure") or 0.0),
        "selected_names": sorted([symbol for symbol, weight in result.target_weights.items() if abs(float(weight)) > 0.0]),
        "target_names": [name for name in str(target_diagnostics.get("target_selected_symbols") or "").split(",") if name],
        "realized_holdings_count": target_diagnostics.get("realized_holdings_count"),
        "target_selected_count": target_diagnostics.get("target_selected_count"),
        "realized_holdings_minus_top_n": target_diagnostics.get("realized_holdings_minus_top_n"),
        "liquidity_excluded_count": target_diagnostics.get("liquidity_excluded_count"),
        "sector_cap_excluded_count": target_diagnostics.get("sector_cap_excluded_count"),
        "turnover_cap_binding_count": target_diagnostics.get("turnover_cap_binding_count"),
        "turnover_buffer_blocked_replacements": target_diagnostics.get("turnover_buffer_blocked_replacements"),
        "semantic_warning": target_diagnostics.get("semantic_warning"),
        "adjusted_order_count": len(result.adjusted_orders),
        "raw_order_count": len(result.reconciliation.orders),
        "open_order_count": len(result.open_orders),
        "target_weight_sum": result.reconciliation.diagnostics.get("target_weight_sum"),
        "reconciliation_diagnostics": result.reconciliation.diagnostics,
        "order_adjustment_diagnostics": result.order_adjustment_diagnostics,
    }
    if result.execution_result is not None:
        summary_payload["execution_summary"] = result.execution_result.summary.to_dict()
    output_check = _health_check(
        name="output_files",
        status="pass",
        message="live dry-run artifacts written successfully",
        config=result.config,
        timestamp=result.as_of,
    )
    result.health_checks.append(output_check)
    health_check_rows = [asdict(check) for check in result.health_checks]
    pd.DataFrame(health_check_rows).to_csv(health_checks_path, index=False)
    summary_payload["health_checks"] = health_check_rows
    warnings, errors = warnings_and_errors_from_checks(health_check_rows)
    summary_payload = add_standard_summary_fields(
        summary_payload,
        summary_type="live_dry_run",
        timestamp=result.as_of,
        status=workflow_status_from_checks(health_check_rows),
        key_counts={
            "adjusted_order_count": len(result.adjusted_orders),
            "raw_order_count": len(result.reconciliation.orders),
            "open_order_count": len(result.open_orders),
        },
        key_metrics={
            "equity": float(result.account.equity),
            "cash": float(result.account.cash),
            "gross_exposure": float(target_diagnostics.get("average_gross_exposure") or 0.0),
            "target_weight_sum": result.reconciliation.diagnostics.get("target_weight_sum"),
        },
        warnings=warnings,
        errors=errors,
    )
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    summary_md_path.write_text(_write_markdown_summary(summary_payload, health_check_rows), encoding="utf-8")
    preview_summary_json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    preview_summary_md_path.write_text(_write_markdown_summary(summary_payload, health_check_rows), encoding="utf-8")

    paths = {
        "summary_json_path": summary_json_path,
        "summary_md_path": summary_md_path,
        "live_execution_preview_summary_json_path": preview_summary_json_path,
        "live_execution_preview_summary_md_path": preview_summary_md_path,
        "target_positions_path": target_positions_path,
        "current_positions_path": current_positions_path,
        "proposed_orders_path": proposed_orders_path,
        "reconciliation_path": reconciliation_path,
        "health_checks_path": health_checks_path,
    }
    if result.execution_result is not None:
        execution_paths = write_execution_artifacts(result.execution_result, output_dir)
        paths.update(execution_paths)
    paths.update(write_decision_journal_artifacts(bundle=result.decision_bundle, output_dir=output_dir))
    paths.update(write_universe_provenance_artifacts(bundle=result.universe_bundle, output_dir=output_dir))
    summary_payload["artifact_paths"] = {name: str(path) for name, path in paths.items()}
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    preview_summary_json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    result.artifacts = paths
    return paths
