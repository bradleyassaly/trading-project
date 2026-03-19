from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.construction.service import build_top_n_portfolio_weights
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.metadata.groups import build_group_series
from trading_platform.signals.loaders import load_feature_frame
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.broker.base import BrokerOrder
from trading_platform.broker.paper_broker import PaperBroker, PaperBrokerConfig
from trading_platform.risk.pre_trade_checks import validate_orders
from trading_platform.paper.models import (
    OrderGenerationResult,
    PaperOrder,
    PaperPortfolioState,
    PaperPosition,
    PaperSignalSnapshot,
    PaperTradingConfig,
    PaperTradingRunResult,
)
from trading_platform.paper.ledger import append_equity_snapshot, append_fills


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
            signal_df = signal_fn(
                feature_df,
                fast=fast,
                slow=slow,
                lookback=lookback,
            ).copy()

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
        weighting_scheme=config.weighting_scheme,
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

    for symbol in all_symbols:
        price = float(latest_prices.get(symbol, 0.0))
        if price <= 0:
            continue

        target_weight = float(latest_target_weights.get(symbol, 0.0))
        target_notional = investable_equity * target_weight
        raw_target_quantity = int(target_notional / price)
        target_quantity = (raw_target_quantity // lot_size) * lot_size

        current_position = state.positions.get(symbol)
        current_quantity = int(current_position.quantity) if current_position else 0
        delta_quantity = target_quantity - current_quantity
        if delta_quantity == 0:
            continue

        notional = abs(delta_quantity) * price
        if notional < min_trade_dollars:
            continue

        side = "BUY" if delta_quantity > 0 else "SELL"
        reason = "rebalance_to_target"
        orders.append(
            PaperOrder(
                symbol=symbol,
                side=side,
                quantity=abs(delta_quantity),
                reference_price=price,
                target_weight=target_weight,
                current_quantity=current_quantity,
                target_quantity=target_quantity,
                notional=notional,
                reason=reason,
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


def run_paper_trading_cycle(
    *,
    config: PaperTradingConfig,
    state_store: JsonPaperStateStore,
    auto_apply_fills: bool = False,
) -> PaperTradingRunResult:
    state = state_store.load()
    if state.cash <= 0 and not state.positions:
        state = bootstrap_paper_portfolio_state(initial_cash=config.initial_cash)

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

    state = sync_state_prices(state, latest_prices)

    as_of, latest_scheduled_weights, latest_effective_weights, target_diagnostics = (
        compute_latest_target_weights(
            config=config,
            snapshot=snapshot,
        )
    )

    order_result = generate_rebalance_orders(
        state=state,
        latest_target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        min_trade_dollars=config.min_trade_dollars,
        lot_size=config.lot_size,
        reserve_cash_pct=config.reserve_cash_pct,
    )

    broker_orders = [
        BrokerOrder(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            reference_price=order.reference_price,
            reason=order.reason,
        )
        for order in order_result.orders
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
    if auto_apply_fills and broker_orders:
        broker = PaperBroker(
            state=state,
            config=PaperBrokerConfig(
                commission_per_order=0.0,
                slippage_bps=0.0,
            ),
        )
        fills = broker.submit_orders(broker_orders)
        state = sync_state_prices(state, latest_prices)

    state.as_of = as_of
    state.last_targets = latest_effective_weights.copy()

    state_store.save(state)

    diagnostics = {
        "target_construction": target_diagnostics,
        "order_generation": order_result.diagnostics,
        "risk_checks": {
            "passed": risk_result.passed,
            "violations": risk_result.violations,
        },
        "fill_count": len(fills),
    }

    return PaperTradingRunResult(
        as_of=as_of,
        state=state,
        latest_prices=latest_prices,
        latest_scores=latest_scores,
        latest_target_weights=latest_effective_weights,
        scheduled_target_weights=latest_scheduled_weights,
        orders=order_result.orders,
        fills=fills,
        skipped_symbols=snapshot.skipped_symbols,
        diagnostics=diagnostics,
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
    equity_curve_path = output_path / "paper_equity_curve.csv"
    positions_path = output_path / "paper_positions.csv"
    targets_path = output_path / "paper_target_weights.csv"
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
    ).to_csv(equity_curve_path, index=False)

    summary_payload = {
        "as_of": result.as_of,
        "cash": result.state.cash,
        "equity": result.state.equity,
        "gross_market_value": result.state.gross_market_value,
        "orders": [asdict(order) for order in result.orders],
        "fills": [asdict(fill) for fill in result.fills],
        "skipped_symbols": result.skipped_symbols,
        "diagnostics": result.diagnostics,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    return {
        "orders_path": orders_path,
        "fills_path": fills_path,
        "equity_curve_path": equity_curve_path,
        "positions_path": positions_path,
        "targets_path": targets_path,
        "summary_path": summary_path,
    }

