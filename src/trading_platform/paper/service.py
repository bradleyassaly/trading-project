from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.construction.service import build_top_n_portfolio_weights
from trading_platform.cli.common import normalize_paper_weighting_scheme
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.metadata.groups import build_group_series
from trading_platform.signals.loaders import load_feature_frame, resolve_feature_frame_path
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
from trading_platform.paper.composite import (
    build_composite_paper_snapshot,
    compute_latest_composite_target_weights,
)
from trading_platform.paper.ledger import append_equity_snapshot, append_fills
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn
from trading_platform.signals.common import normalize_price_frame


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


def _compute_latest_xsec_target_weights(
    *,
    config: PaperTradingConfig,
) -> tuple[str, dict[str, float], dict[str, float], dict[str, float], dict[str, Any], list[str]]:
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

    if config.signal_source == "composite":
        snapshot, snapshot_diagnostics = build_composite_paper_snapshot(config=config)
        composite_targets = compute_latest_composite_target_weights(
            config=config,
            snapshot=snapshot,
            snapshot_diagnostics=snapshot_diagnostics,
        )
        as_of = composite_targets.as_of
        latest_prices = composite_targets.latest_prices
        latest_scores = composite_targets.latest_scores
        latest_scheduled_weights = composite_targets.scheduled_target_weights
        latest_effective_weights = composite_targets.effective_target_weights
        target_diagnostics = composite_targets.diagnostics.get("target_construction", {})
        extra_diagnostics = {
            key: value
            for key, value in composite_targets.diagnostics.items()
            if key != "target_construction"
        }
    elif config.strategy == "xsec_momentum_topn":
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
        extra_diagnostics = {}
    else:
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
        extra_diagnostics = {}

    state = sync_state_prices(state, latest_prices)

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
        "signal_source": config.signal_source,
        "preset_name": config.preset_name,
        "target_construction": target_diagnostics,
        "order_generation": order_result.diagnostics,
        "risk_checks": {
            "passed": risk_result.passed,
            "violations": risk_result.violations,
        },
        "fill_count": len(fills),
    }
    diagnostics.update(extra_diagnostics)

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
    equity_snapshot_path = output_path / "paper_equity_snapshot.csv"
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
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    paths = {
        "orders_path": orders_path,
        "fills_path": fills_path,
        "equity_snapshot_path": equity_snapshot_path,
        "positions_path": positions_path,
        "targets_path": targets_path,
        "summary_path": summary_path,
    }
    paths.update(extra_paths)
    return paths

