from __future__ import annotations

from pathlib import Path

from trading_platform.broker.alpaca_broker import AlpacaBroker, AlpacaBrokerConfig
from trading_platform.execution.reconciliation import (
    build_rebalance_orders_from_broker_state,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import (
    compute_latest_target_weights,
    load_signal_snapshot,
)
from trading_platform.universes.registry import get_universe_symbols


def _resolve_symbols(args) -> list[str]:
    has_symbols = bool(getattr(args, "symbols", None))
    has_universe = bool(getattr(args, "universe", None))

    if has_symbols == has_universe:
        raise ValueError("Provide exactly one of --symbols or --universe")

    if has_universe:
        return get_universe_symbols(args.universe)

    return list(args.symbols)


def cmd_live_dry_run(args) -> None:
    symbols = _resolve_symbols(args)

    config = PaperTradingConfig(
        symbols=symbols,
        strategy=args.strategy,
        fast=args.fast,
        slow=args.slow,
        lookback=args.lookback,
        top_n=args.top_n,
        weighting_scheme=args.weighting_scheme,
        vol_window=args.vol_window,
        min_score=args.min_score,
        max_weight=args.max_weight,
        max_names_per_group=args.max_names_per_group,
        max_group_weight=args.max_group_weight,
        group_map_path=args.group_map_path,
        rebalance_frequency=args.rebalance_frequency,
        timing=args.timing,
        initial_cash=args.initial_cash,
        min_trade_dollars=args.min_trade_dollars,
        lot_size=args.lot_size,
        reserve_cash_pct=args.reserve_cash_pct,
    )

    print(
        "Running live dry-run for "
        f"{len(symbols)} symbol(s): {', '.join(symbols)}"
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

    as_of, _, latest_effective_weights, target_diagnostics = compute_latest_target_weights(
        config=config,
        snapshot=snapshot,
    )

    broker = AlpacaBroker(AlpacaBrokerConfig.from_env())
    account = broker.get_account()
    positions = broker.get_positions()

    reconciliation = build_rebalance_orders_from_broker_state(
        account=account,
        positions=positions,
        latest_target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        reserve_cash_pct=config.reserve_cash_pct,
        min_trade_dollars=config.min_trade_dollars,
        lot_size=config.lot_size,
        order_type=args.order_type,
        time_in_force=args.time_in_force,
    )

    print(f"As of: {as_of}")
    print(f"Broker equity: {account.equity:,.2f}")
    print(f"Broker cash: {account.cash:,.2f}")
    print(f"Computed orders: {len(reconciliation.orders)}")
    print("Orders:")
    for order in reconciliation.orders:
        print(
            f"  {order.side} {order.quantity} {order.symbol} "
            f"type={order.order_type} tif={order.time_in_force}"
        )

    print("Diagnostics:")
    print(f"  target_construction: {target_diagnostics}")
    print(f"  reconciliation: {reconciliation.diagnostics}")