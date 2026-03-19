from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from trading_platform.broker.alpaca_broker import AlpacaBroker, AlpacaBrokerConfig
from trading_platform.broker.live_models import BrokerAccount, LiveBrokerPosition
from trading_platform.execution.reconciliation import (
    build_rebalance_orders_from_broker_state,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import (
    compute_latest_target_weights,
    load_signal_snapshot,
)
from trading_platform.universes.registry import get_universe_symbols
from trading_platform.execution.open_order_adjustment import adjust_orders_for_open_orders
from trading_platform.broker.live_models import BrokerAccount, LiveBrokerPosition, LiveBrokerOrderStatus

def _load_mock_positions(path: str | None) -> dict[str, LiveBrokerPosition]:
    if not path:
        return {}

    df = pd.read_csv(path)

    required = {"symbol", "quantity", "avg_price", "market_price"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Mock positions file missing required columns: {sorted(missing)}"
        )

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

def _resolve_symbols(args) -> list[str]:
    has_symbols = bool(getattr(args, "symbols", None))
    has_universe = bool(getattr(args, "universe", None))

    if has_symbols == has_universe:
        raise ValueError("Provide exactly one of --symbols or --universe")

    if has_universe:
        return get_universe_symbols(args.universe)

    return list(args.symbols)


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

    broker_name = getattr(args, "broker", "mock")

    if broker_name == "mock":
        equity = getattr(args, "mock_equity", 100_000.0)
        cash = getattr(args, "mock_cash", 100_000.0)
        mock_positions = _load_mock_positions(
            getattr(args, "mock_positions_path", None)
        )
        broker = MockBroker(
            MockBrokerConfig(
                equity=equity,
                cash=cash,
                positions=mock_positions,
            )
        )
    else:
        broker = AlpacaBroker(AlpacaBrokerConfig.from_env())

    account = broker.get_account()
    positions = broker.get_positions()
    print(f"Current broker positions: {len(positions)}")
    for symbol, position in sorted(positions.items()):
        print(
            f"  {symbol}: qty={position.quantity} "
            f"price={position.market_price:.2f} "
            f"value={position.market_value:,.2f}"
        )

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

    open_orders = []
    if hasattr(broker, "list_open_orders"):
        try:
            open_orders = broker.list_open_orders()
        except NotImplementedError:
            open_orders = []

    adjustment = adjust_orders_for_open_orders(
        proposed_orders=reconciliation.orders,
        open_orders=open_orders,
    )
    print(f"As of: {as_of}")
    print(f"Broker: {broker_name}")
    print(f"Broker equity: {account.equity:,.2f}")
    print(f"Broker cash: {account.cash:,.2f}")
    print(f"Current broker positions: {len(positions)}")
    for symbol, position in sorted(positions.items()):
        print(
            f"  {symbol}: qty={position.quantity} "
            f"price={position.market_price:.2f} "
            f"value={position.market_value:,.2f}"
        )

    print(f"Open orders: {len(open_orders)}")
    for order in open_orders:
        print(
            f"  {order.side} {order.remaining_quantity} {order.symbol} "
            f"status={order.status}"
        )

    print(f"Raw computed orders: {len(reconciliation.orders)}")
    for order in reconciliation.orders:
        print(
            f"  {order.side} {order.quantity} {order.symbol} "
            f"type={order.order_type} tif={order.time_in_force}"
        )

    print(f"Adjusted orders after open-order awareness: {len(adjustment.adjusted_orders)}")
    for order in adjustment.adjusted_orders:
        print(
            f"  {order.side} {order.quantity} {order.symbol} "
            f"type={order.order_type} tif={order.time_in_force}"
        )

    print("Diagnostics:")
    print(f"  target_construction: {target_diagnostics}")
    print(f"  reconciliation: {reconciliation.diagnostics}")
    print(f"  open_order_adjustment: {adjustment.diagnostics}")