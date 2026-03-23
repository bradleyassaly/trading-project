from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.broker.alpaca_broker import AlpacaBroker, AlpacaBrokerConfig
from trading_platform.broker.base import BrokerOrder
from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerFill,
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
    LiveBrokerPosition,
)
from trading_platform.execution.open_order_adjustment import adjust_orders_for_open_orders
from trading_platform.execution.reconciliation import build_rebalance_orders_from_broker_state
from trading_platform.metadata.groups import build_group_series
from trading_platform.paper.composite import (
    build_composite_paper_snapshot,
    compute_latest_composite_target_weights,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import compute_latest_target_weights, load_signal_snapshot
from trading_platform.risk.pre_trade_checks import validate_orders
from trading_platform.universes.registry import get_universe_symbols


@dataclass(frozen=True)
class LiveExecutionControlConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    signal_source: str = "legacy"
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    top_n: int = 10
    weighting_scheme: str = "equal"
    vol_window: int = 20
    min_score: float | None = None
    max_weight: float | None = None
    max_names_per_group: int | None = None
    max_group_weight: float | None = None
    group_map_path: str | None = None
    rebalance_frequency: str = "daily"
    timing: str = "next_bar"
    initial_cash: float = 100_000.0
    min_trade_dollars: float = 25.0
    lot_size: int = 1
    reserve_cash_pct: float = 0.0
    approved_model_state_path: str | None = None
    composite_artifact_dir: str | None = None
    composite_horizon: int = 1
    composite_weighting_scheme: str = "equal"
    composite_portfolio_mode: str = "long_only_top_n"
    composite_long_quantile: float = 0.2
    composite_short_quantile: float = 0.2
    min_price: float | None = None
    min_volume: float | None = None
    min_avg_dollar_volume: float | None = None
    max_adv_participation: float = 0.05
    max_position_pct_of_adv: float = 0.1
    max_notional_per_name: float | None = None
    order_type: str = "market"
    time_in_force: str = "day"
    broker: str = "mock"
    mock_equity: float = 100_000.0
    mock_cash: float = 100_000.0
    mock_positions_path: str | None = None
    kill_switch: bool = False
    kill_switch_path: str | None = None
    blocked_symbols: tuple[str, ...] = ()
    max_gross_exposure: float | None = 1.0
    max_net_exposure: float | None = 1.0
    max_position_weight_limit: float | None = None
    max_group_exposure: float | None = None
    max_order_notional: float | None = None
    max_daily_turnover: float | None = None
    min_cash_reserve: float | None = 0.0
    max_data_staleness_days: int | None = 3
    max_config_staleness_days: int | None = 30
    approval_artifact_path: str | None = None
    approved: bool = False
    drift_alerts_path: str | None = None
    output_dir: str | Path = "artifacts/live_execution"


@dataclass(frozen=True)
class LiveExecutionRunResult:
    decision: str
    reason_codes: list[str]
    adjusted_orders: list[LiveBrokerOrderRequest]
    blocked_orders: list[dict[str, object]]
    diagnostics: dict[str, Any]
    artifacts: dict[str, str]


@dataclass(frozen=True)
class MockBrokerConfig:
    equity: float = 100_000.0
    cash: float = 100_000.0
    positions: dict[str, LiveBrokerPosition] | None = None
    open_orders: list[LiveBrokerOrderStatus] | None = None


class MockLiveBroker:
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

    def submit_orders(
        self,
        orders: list[LiveBrokerOrderRequest],
    ) -> list[LiveBrokerOrderStatus]:
        return [
            LiveBrokerOrderStatus(
                broker_order_id=f"mock-{index}",
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                filled_quantity=0,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                status="accepted",
                submitted_at=datetime.now(UTC).isoformat(),
            )
            for index, order in enumerate(orders, start=1)
        ]


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _safe_read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _resolve_symbols(config: LiveExecutionControlConfig) -> list[str]:
    has_symbols = bool(config.symbols)
    has_universe = bool(config.universe)
    if has_symbols == has_universe:
        raise ValueError("Provide exactly one of symbols or universe")
    if has_universe:
        return get_universe_symbols(str(config.universe))
    return list(config.symbols or [])


def _load_mock_positions(path: str | None) -> dict[str, LiveBrokerPosition]:
    if not path:
        return {}
    df = pd.read_csv(path)
    if df.empty:
        return {}
    positions: dict[str, LiveBrokerPosition] = {}
    for row in df.to_dict(orient="records"):
        quantity = int(row["quantity"])
        market_price = float(row["market_price"])
        positions[str(row["symbol"])] = LiveBrokerPosition(
            symbol=str(row["symbol"]),
            quantity=quantity,
            avg_price=float(row["avg_price"]),
            market_price=market_price,
            market_value=quantity * market_price,
        )
    return positions


def _build_paper_config(config: LiveExecutionControlConfig, symbols: list[str]) -> PaperTradingConfig:
    return PaperTradingConfig(
        symbols=symbols,
        signal_source=config.signal_source,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
        top_n=config.top_n,
        weighting_scheme=config.weighting_scheme,
        vol_window=config.vol_window,
        min_score=config.min_score,
        max_weight=config.max_weight,
        max_names_per_group=config.max_names_per_group,
        max_group_weight=config.max_group_weight,
        group_map_path=config.group_map_path,
        rebalance_frequency=config.rebalance_frequency,
        timing=config.timing,
        initial_cash=config.initial_cash,
        min_trade_dollars=config.min_trade_dollars,
        lot_size=config.lot_size,
        reserve_cash_pct=config.reserve_cash_pct,
        approved_model_state_path=config.approved_model_state_path,
        composite_artifact_dir=config.composite_artifact_dir,
        composite_horizon=config.composite_horizon,
        composite_weighting_scheme=config.composite_weighting_scheme,
        composite_portfolio_mode=config.composite_portfolio_mode,
        composite_long_quantile=config.composite_long_quantile,
        composite_short_quantile=config.composite_short_quantile,
        min_price=config.min_price,
        min_volume=config.min_volume,
        min_avg_dollar_volume=config.min_avg_dollar_volume,
        max_adv_participation=config.max_adv_participation,
        max_position_pct_of_adv=config.max_position_pct_of_adv,
        max_notional_per_name=config.max_notional_per_name,
    )


def _build_targets(
    config: LiveExecutionControlConfig,
    *,
    symbols: list[str],
) -> tuple[str, pd.Timestamp | None, dict[str, float], dict[str, float], dict[str, float], dict[str, Any]]:
    paper_config = _build_paper_config(config, symbols)
    if config.signal_source == "composite":
        snapshot, snapshot_diagnostics = build_composite_paper_snapshot(config=paper_config)
        target_result = compute_latest_composite_target_weights(
            config=paper_config,
            snapshot=snapshot,
            snapshot_diagnostics=snapshot_diagnostics,
        )
        latest_timestamp = snapshot.closes.index.max() if not snapshot.closes.empty else None
        diagnostics = dict(target_result.diagnostics)
        diagnostics["skipped_symbols"] = snapshot.skipped_symbols
        return (
            target_result.as_of,
            latest_timestamp,
            target_result.scheduled_target_weights,
            target_result.effective_target_weights,
            target_result.latest_prices,
            diagnostics,
        )

    snapshot = load_signal_snapshot(
        symbols=symbols,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
    )
    as_of, latest_scheduled_weights, latest_effective_weights, target_diagnostics = compute_latest_target_weights(
        config=paper_config,
        snapshot=snapshot,
    )
    latest_prices = {
        symbol: float(price)
        for symbol, price in snapshot.closes.iloc[-1].fillna(0.0).items()
        if float(price) > 0.0
    }
    latest_timestamp = snapshot.closes.index.max() if not snapshot.closes.empty else None
    return (
        as_of,
        latest_timestamp,
        latest_scheduled_weights,
        latest_effective_weights,
        latest_prices,
        {
            "signal_source": "legacy",
            "target_construction": target_diagnostics,
            "skipped_symbols": snapshot.skipped_symbols,
        },
    )


def _resolve_broker(config: LiveExecutionControlConfig):
    if config.broker == "mock":
        return MockLiveBroker(
            MockBrokerConfig(
                equity=config.mock_equity,
                cash=config.mock_cash,
                positions=_load_mock_positions(config.mock_positions_path),
            )
        )
    return AlpacaBroker(AlpacaBrokerConfig.from_env())


def _weights_from_positions(
    positions: dict[str, LiveBrokerPosition],
    *,
    equity: float,
) -> dict[str, float]:
    if equity <= 0:
        return {}
    return {
        symbol: float(position.market_value) / float(equity)
        for symbol, position in positions.items()
        if float(position.market_value) != 0.0
    }


def _estimate_turnover(current_weights: dict[str, float], target_weights: dict[str, float]) -> float:
    symbols = sorted(set(current_weights) | set(target_weights))
    return float(
        sum(abs(float(target_weights.get(symbol, 0.0)) - float(current_weights.get(symbol, 0.0))) for symbol in symbols)
    )


def _group_exposures(weights: dict[str, float], *, symbols: list[str], group_map_path: str | None) -> dict[str, float]:
    if not weights:
        return {}
    groups = build_group_series(symbols, path=group_map_path)
    exposures: dict[str, float] = {}
    for symbol, weight in weights.items():
        group = str(groups.get(symbol, "UNGROUPED"))
        exposures[group] = exposures.get(group, 0.0) + float(weight)
    return exposures


def _approval_status(config: LiveExecutionControlConfig) -> dict[str, object]:
    approval_source_path = config.approval_artifact_path or config.approved_model_state_path
    artifact_payload = {}
    artifact_approved = False
    if approval_source_path:
        artifact_payload = _safe_read_json(Path(approval_source_path))
        artifact_approved = bool(
            artifact_payload.get("approved", False)
            or artifact_payload.get("approval_status") == "approved"
        )
    return {
        "approved_flag": bool(config.approved),
        "approval_artifact_path": approval_source_path or "",
        "approval_artifact": artifact_payload,
        "approved": bool(config.approved or artifact_approved),
    }


def _config_age_violation(
    *,
    config: LiveExecutionControlConfig,
) -> str | None:
    approval_source_path = config.approval_artifact_path or config.approved_model_state_path
    if config.max_config_staleness_days is None or not approval_source_path:
        return None
    approval_payload = _safe_read_json(Path(approval_source_path))
    approved_at = (
        approval_payload.get("approved_at")
        or approval_payload.get("snapshot_timestamp")
        or approval_payload.get("approval_metadata", {}).get("approved_at")
    )
    if not approved_at:
        return "stale_config_missing_timestamp"
    approved_timestamp = pd.to_datetime(approved_at, errors="coerce", utc=True)
    if pd.isna(approved_timestamp):
        return "stale_config_invalid_timestamp"
    age_days = (pd.Timestamp(datetime.now(UTC)) - approved_timestamp).days
    if age_days > int(config.max_config_staleness_days):
        return "stale_config"
    return None


def _data_staleness_violation(
    *,
    latest_timestamp: pd.Timestamp | None,
    max_data_staleness_days: int | None,
) -> str | None:
    if latest_timestamp is None or max_data_staleness_days is None:
        return None
    latest = pd.Timestamp(latest_timestamp)
    if latest.tzinfo is None:
        current_date = datetime.now().date()
    else:
        current_date = datetime.now(latest.tzinfo).date()
    latest_date = latest.date()
    if current_date <= latest_date:
        return None
    # Daily trading inputs are typically evaluated on market business days rather
    # than raw calendar-day gaps, so weekend spacing alone should not trigger a
    # stale-data abort.
    age_days = max(0, len(pd.bdate_range(latest_date, current_date)) - 1)
    if age_days > int(max_data_staleness_days):
        return "stale_data"
    return None


def _load_drift_alert_violations(path: str | None) -> list[str]:
    if not path:
        return []
    alerts_df = _safe_read_csv(Path(path))
    if alerts_df.empty or "severity" not in alerts_df.columns:
        return []
    severe = alerts_df.loc[alerts_df["severity"].astype(str).str.lower().isin(["high", "critical"])]
    return ["drift_alert_block"] if not severe.empty else []


def _account_sanity_violations(
    account: BrokerAccount,
    positions: dict[str, LiveBrokerPosition],
) -> list[str]:
    violations: list[str] = []
    if float(account.equity) <= 0:
        violations.append("invalid_equity")
    if float(account.buying_power) < 0:
        violations.append("negative_buying_power")
    gross_market_value = float(sum(float(position.market_value) for position in positions.values()))
    implied_equity = float(account.cash) + gross_market_value
    tolerance = max(1.0, abs(float(account.equity)) * 0.1)
    if abs(implied_equity - float(account.equity)) > tolerance:
        violations.append("account_equity_inconsistent")
    return violations


def _apply_order_blocks(
    orders: list[LiveBrokerOrderRequest],
    *,
    blocked_symbols: tuple[str, ...],
    latest_prices: dict[str, float],
    max_order_notional: float | None,
) -> tuple[list[LiveBrokerOrderRequest], list[dict[str, object]]]:
    remaining_orders: list[LiveBrokerOrderRequest] = []
    blocked_rows: list[dict[str, object]] = []
    blocked_symbol_set = {symbol.upper() for symbol in blocked_symbols}
    for order in orders:
        symbol = order.symbol.upper()
        if symbol in blocked_symbol_set:
            blocked_rows.append(
                {
                    "symbol": order.symbol,
                    "side": order.side,
                    "quantity": order.quantity,
                    "rule": "symbol_block_list",
                    "reason_code": "symbol_blocked",
                }
            )
            continue
        notional = float(order.quantity) * float(latest_prices.get(order.symbol, 0.0))
        if max_order_notional is not None and notional > float(max_order_notional):
            blocked_rows.append(
                {
                    "symbol": order.symbol,
                    "side": order.side,
                    "quantity": order.quantity,
                    "rule": "max_order_notional",
                    "reason_code": "order_notional_exceeded",
                }
            )
            continue
        remaining_orders.append(order)
    return remaining_orders, blocked_rows


def _risk_violations(
    *,
    account: BrokerAccount,
    positions: dict[str, LiveBrokerPosition],
    target_weights: dict[str, float],
    latest_prices: dict[str, float],
    orders: list[LiveBrokerOrderRequest],
    symbols: list[str],
    config: LiveExecutionControlConfig,
) -> tuple[list[str], dict[str, object]]:
    violations: list[str] = []
    current_weights = _weights_from_positions(positions, equity=float(account.equity))
    gross_exposure = float(sum(abs(weight) for weight in target_weights.values()))
    net_exposure = float(sum(target_weights.values()))
    max_position_weight = float(max((abs(weight) for weight in target_weights.values()), default=0.0))
    group_exposures = _group_exposures(
        target_weights,
        symbols=symbols,
        group_map_path=config.group_map_path,
    )
    turnover_estimate = _estimate_turnover(current_weights, target_weights)
    long_weight = float(sum(weight for weight in target_weights.values() if weight > 0))
    estimated_cash_reserve = max(0.0, 1.0 - long_weight)

    if config.max_gross_exposure is not None and gross_exposure > float(config.max_gross_exposure):
        violations.append("max_gross_exposure_exceeded")
    if config.max_net_exposure is not None and abs(net_exposure) > float(config.max_net_exposure):
        violations.append("max_net_exposure_exceeded")
    if config.max_position_weight_limit is not None and max_position_weight > float(config.max_position_weight_limit):
        violations.append("max_position_weight_exceeded")
    if config.max_group_exposure is not None and any(
        abs(exposure) > float(config.max_group_exposure) for exposure in group_exposures.values()
    ):
        violations.append("max_group_exposure_exceeded")
    if config.max_daily_turnover is not None and turnover_estimate > float(config.max_daily_turnover):
        violations.append("max_daily_turnover_exceeded")
    if config.min_cash_reserve is not None and estimated_cash_reserve < float(config.min_cash_reserve):
        violations.append("min_cash_reserve_breached")

    broker_orders = [
        BrokerOrder(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            reference_price=float(latest_prices.get(order.symbol, 0.0)),
            reason=order.reason or "rebalance_to_target",
        )
        for order in orders
    ]
    pretrade = validate_orders(
        orders=broker_orders,
        equity=float(account.equity),
        max_single_order_notional=config.max_order_notional,
    )
    if not pretrade.passed:
        violations.extend(["existing_pretrade_check_failed"])

    diagnostics = {
        "gross_exposure_before": float(sum(abs(weight) for weight in current_weights.values())),
        "net_exposure_before": float(sum(current_weights.values())),
        "gross_exposure_after": gross_exposure,
        "net_exposure_after": net_exposure,
        "max_position_weight_after": max_position_weight,
        "group_exposures_after": group_exposures,
        "turnover_estimate": turnover_estimate,
        "estimated_cash_reserve_after": estimated_cash_reserve,
        "existing_pretrade_violations": pretrade.violations,
    }
    return violations, diagnostics


def _write_artifacts(
    *,
    output_dir: Path,
    pretrade_report: dict[str, object],
    blocked_orders: list[dict[str, object]],
    decision_payload: dict[str, object],
    approval_status: dict[str, object],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    pretrade_path = output_dir / "pretrade_risk_report.json"
    blocked_path = output_dir / "blocked_orders_report.csv"
    decision_path = output_dir / "live_execution_decision.json"
    approval_path = output_dir / "approval_status_snapshot.json"
    pretrade_path.write_text(json.dumps(pretrade_report, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(blocked_orders).to_csv(blocked_path, index=False)
    decision_path.write_text(json.dumps(decision_payload, indent=2, default=str), encoding="utf-8")
    approval_path.write_text(json.dumps(approval_status, indent=2, default=str), encoding="utf-8")
    return {
        "pretrade_risk_report_path": str(pretrade_path),
        "blocked_orders_report_path": str(blocked_path),
        "live_execution_decision_path": str(decision_path),
        "approval_status_snapshot_path": str(approval_path),
    }


def run_live_execution_control(
    *,
    config: LiveExecutionControlConfig,
    execute: bool,
) -> LiveExecutionRunResult:
    symbols = _resolve_symbols(config)
    as_of, latest_timestamp, _, latest_effective_weights, latest_prices, target_diagnostics = _build_targets(
        config,
        symbols=symbols,
    )
    broker = _resolve_broker(config)
    account = broker.get_account()
    positions = broker.get_positions()
    open_orders = broker.list_open_orders() if hasattr(broker, "list_open_orders") else []
    reconciliation = build_rebalance_orders_from_broker_state(
        account=account,
        positions=positions,
        latest_target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        reserve_cash_pct=config.reserve_cash_pct,
        min_trade_dollars=config.min_trade_dollars,
        lot_size=config.lot_size,
        order_type=config.order_type,
        time_in_force=config.time_in_force,
    )
    adjustment = adjust_orders_for_open_orders(
        proposed_orders=reconciliation.orders,
        open_orders=open_orders,
    )
    adjusted_orders, blocked_rows = _apply_order_blocks(
        adjustment.adjusted_orders,
        blocked_symbols=config.blocked_symbols,
        latest_prices=latest_prices,
        max_order_notional=config.max_order_notional,
    )

    approval_status = _approval_status(config)
    reason_codes: list[str] = []
    critical_violations: list[str] = []
    if config.kill_switch or (config.kill_switch_path and Path(config.kill_switch_path).exists()):
        critical_violations.append("kill_switch_active")
    critical_violations.extend(_account_sanity_violations(account, positions))
    data_violation = _data_staleness_violation(
        latest_timestamp=latest_timestamp,
        max_data_staleness_days=config.max_data_staleness_days,
    )
    if data_violation:
        critical_violations.append(data_violation)
    config_violation = _config_age_violation(config=config)
    if config_violation:
        critical_violations.append(config_violation)
    critical_violations.extend(_load_drift_alert_violations(config.drift_alerts_path))
    risk_violations, risk_diagnostics = _risk_violations(
        account=account,
        positions=positions,
        target_weights=latest_effective_weights,
        latest_prices=latest_prices,
        orders=adjusted_orders,
        symbols=symbols,
        config=config,
    )
    critical_violations.extend(risk_violations)
    if not adjusted_orders:
        critical_violations.append("no_orders")

    submitted_orders: list[dict[str, object]] = []
    if critical_violations:
        decision = "abort"
        reason_codes.extend(critical_violations)
    elif not execute:
        decision = "dry-run"
        reason_codes.append("validation_only")
    elif not approval_status["approved"]:
        decision = "dry-run"
        reason_codes.append("missing_approval")
    elif not hasattr(broker, "submit_orders"):
        decision = "abort"
        reason_codes.append("broker_submit_unavailable")
    else:
        try:
            submitted = broker.submit_orders(adjusted_orders)
        except NotImplementedError:
            decision = "abort"
            reason_codes.append("broker_submit_not_implemented")
        else:
            decision = "execute"
            submitted_orders = [asdict(order) for order in submitted]
            reason_codes.append("approved_for_execution")

    pretrade_report = {
        "as_of": as_of,
        "latest_timestamp": str(latest_timestamp) if latest_timestamp is not None else "",
        "account": asdict(account),
        "position_count": len(positions),
        "target_construction": target_diagnostics,
        "reconciliation": reconciliation.diagnostics,
        "open_order_adjustment": adjustment.diagnostics,
        "risk_diagnostics": risk_diagnostics,
        "critical_violations": critical_violations,
    }
    decision_payload = {
        "decision": decision,
        "reason_codes": reason_codes,
        "submitted_orders": submitted_orders,
        "adjusted_order_count": len(adjusted_orders),
        "blocked_order_count": len(blocked_rows),
        "signal_source": config.signal_source,
        "broker": config.broker,
        "execute_requested": execute,
    }
    artifacts = _write_artifacts(
        output_dir=Path(config.output_dir),
        pretrade_report=pretrade_report,
        blocked_orders=blocked_rows,
        decision_payload=decision_payload,
        approval_status=approval_status,
    )
    return LiveExecutionRunResult(
        decision=decision,
        reason_codes=reason_codes,
        adjusted_orders=adjusted_orders,
        blocked_orders=blocked_rows,
        diagnostics={
            "pretrade_report": pretrade_report,
            "approval_status": approval_status,
            "submitted_orders": submitted_orders,
        },
        artifacts=artifacts,
    )
