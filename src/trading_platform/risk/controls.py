import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from trading_platform.reporting.system_health import build_system_health_payload

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperOrder, PaperPortfolioState, PaperTradingConfig, PaperTradingRunResult


PAPER_RISK_CONTROL_SCHEMA_VERSION = "paper_risk_controls_v1"
OPERATING_STATES = {"healthy", "restricted", "halted"}
RISK_SCOPES = {"portfolio", "strategy", "instrument"}
RISK_ACTIONS = {"observe", "throttle_orders", "block_symbol", "halt_trading", "recommend_restrict", "recommend_halt"}
STATE_RANK = {"healthy": 0, "restricted": 1, "halted": 2}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _bounded_scale(value: float) -> float:
    return float(min(max(value, 0.0), 1.0))


def _drawdown(*, equity: float, baseline: float) -> float | None:
    if baseline <= 0.0:
        return None
    return float(max(0.0, 1.0 - (float(equity) / float(baseline))))


def _transition_state(current: str, candidate: str) -> str:
    if STATE_RANK[candidate] > STATE_RANK[current]:
        return candidate
    return current


def _strategy_id_from_order(order: "PaperOrder") -> str:
    provenance = dict(order.provenance or {})
    strategy_id = provenance.get("strategy_id")
    if strategy_id:
        return str(strategy_id)
    ownership = dict(provenance.get("strategy_ownership") or {})
    if ownership:
        winner = max(ownership.items(), key=lambda item: (abs(float(item[1] or 0.0)), str(item[0])))
        return str(winner[0])
    return "unknown_strategy"


@dataclass(frozen=True)
class RiskControlTrigger:
    as_of: str
    scope: str
    scope_id: str
    trigger_type: str
    severity: str
    threshold: float | None
    observed_value: float | None
    operating_state: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.scope not in RISK_SCOPES:
            raise ValueError(f"Unsupported risk scope: {self.scope}")
        if self.operating_state not in OPERATING_STATES:
            raise ValueError(f"Unsupported operating state: {self.operating_state}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "trigger_type": self.trigger_type,
            "severity": self.severity,
            "threshold": self.threshold,
            "observed_value": self.observed_value,
            "operating_state": self.operating_state,
            "message": self.message,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = json.dumps(self.metadata, sort_keys=True)
        return payload


@dataclass(frozen=True)
class RiskControlAction:
    as_of: str
    scope: str
    scope_id: str
    action: str
    operating_state: str
    quantity_scale: float | None = None
    affected_symbols: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.scope not in RISK_SCOPES:
            raise ValueError(f"Unsupported risk scope: {self.scope}")
        if self.action not in RISK_ACTIONS:
            raise ValueError(f"Unsupported risk action: {self.action}")
        if self.operating_state not in OPERATING_STATES:
            raise ValueError(f"Unsupported operating state: {self.operating_state}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "action": self.action,
            "operating_state": self.operating_state,
            "quantity_scale": self.quantity_scale,
            "affected_symbols": list(self.affected_symbols),
            "reason_codes": list(self.reason_codes),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["affected_symbols"] = "|".join(self.affected_symbols)
        payload["reason_codes"] = "|".join(self.reason_codes)
        payload["metadata"] = json.dumps(self.metadata, sort_keys=True)
        return payload


@dataclass(frozen=True)
class RiskControlEvent:
    as_of: str
    scope: str
    scope_id: str
    previous_state: str
    new_state: str
    action: str
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.scope not in RISK_SCOPES:
            raise ValueError(f"Unsupported risk scope: {self.scope}")
        if self.previous_state not in OPERATING_STATES or self.new_state not in OPERATING_STATES:
            raise ValueError("Unsupported operating state transition")

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "action": self.action,
            "reason_codes": list(self.reason_codes),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["reason_codes"] = "|".join(self.reason_codes)
        payload["metadata"] = json.dumps(self.metadata, sort_keys=True)
        return payload


@dataclass(frozen=True)
class PaperRiskControlReport:
    as_of: str
    enabled: bool
    operating_state: str
    schema_version: str = PAPER_RISK_CONTROL_SCHEMA_VERSION
    triggers: list[RiskControlTrigger] = field(default_factory=list)
    actions: list[RiskControlAction] = field(default_factory=list)
    events: list[RiskControlEvent] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.operating_state not in OPERATING_STATES:
            raise ValueError(f"Unsupported operating state: {self.operating_state}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "enabled": bool(self.enabled),
            "operating_state": self.operating_state,
            "schema_version": self.schema_version,
            "triggers": [row.to_dict() for row in self.triggers],
            "actions": [row.to_dict() for row in self.actions],
            "events": [row.to_dict() for row in self.events],
            "summary": dict(self.summary),
        }


def _scaled_order(order: "PaperOrder", scale: float) -> "PaperOrder | None":
    prior_quantity = abs(int(order.quantity))
    if prior_quantity <= 0:
        return None
    new_quantity = int(prior_quantity * scale)
    if new_quantity <= 0:
        return None
    ratio = float(new_quantity / prior_quantity)
    signed_target = order.current_quantity + (new_quantity if str(order.side).upper() == "BUY" else -new_quantity)
    return replace(
        order,
        quantity=int(new_quantity),
        target_quantity=int(signed_target),
        notional=float(abs(new_quantity) * float(order.reference_price)),
        expected_fees=float(order.expected_fees) * ratio,
        expected_slippage_cost=float(order.expected_slippage_cost) * ratio,
        expected_spread_cost=float(order.expected_spread_cost) * ratio,
        expected_commission_cost=float(order.expected_commission_cost) * ratio,
        expected_total_execution_cost=float(order.expected_total_execution_cost) * ratio,
    )


def apply_pretrade_risk_controls(
    *,
    as_of: str,
    orders: list["PaperOrder"],
    state: "PaperPortfolioState",
    config: "PaperTradingConfig",
) -> tuple[list["PaperOrder"], list[RiskControlTrigger], list[RiskControlAction], list[RiskControlEvent], str]:
    if not bool(getattr(config, "risk_controls_enabled", False)) or not orders:
        return list(orders), [], [], [], "healthy"

    baseline = float(state.initial_cash_basis or state.equity or 0.0)
    drawdown = _drawdown(equity=float(state.equity), baseline=baseline)
    triggers: list[RiskControlTrigger] = []
    actions: list[RiskControlAction] = []
    events: list[RiskControlEvent] = []
    operating_state = "healthy"

    halt_threshold = _safe_float(getattr(config, "risk_halt_drawdown", None))
    restrict_threshold = _safe_float(getattr(config, "risk_restrict_drawdown", None))
    if drawdown is not None and halt_threshold is not None and drawdown >= halt_threshold:
        operating_state = "halted"
        triggers.append(
            RiskControlTrigger(
                as_of=as_of,
                scope="portfolio",
                scope_id="portfolio",
                trigger_type="drawdown_breach",
                severity="critical",
                threshold=halt_threshold,
                observed_value=drawdown,
                operating_state="halted",
                message="portfolio drawdown breached halt threshold",
                metadata={"baseline_equity": baseline, "current_equity": float(state.equity)},
            )
        )
        actions.append(
            RiskControlAction(
                as_of=as_of,
                scope="portfolio",
                scope_id="portfolio",
                action="halt_trading",
                operating_state="halted",
                quantity_scale=0.0,
                affected_symbols=sorted({order.symbol for order in orders}),
                reason_codes=["drawdown_breach"],
            )
        )
        events.append(
            RiskControlEvent(
                as_of=as_of,
                scope="portfolio",
                scope_id="portfolio",
                previous_state="healthy",
                new_state="halted",
                action="halt_trading",
                reason_codes=["drawdown_breach"],
            )
        )
        return [], triggers, actions, events, operating_state

    if drawdown is not None and restrict_threshold is not None and drawdown >= restrict_threshold:
        operating_state = "restricted"
        scale = _bounded_scale(float(getattr(config, "risk_restricted_order_quantity_scale", 0.5) or 0.0))
        scaled_orders = [scaled for scaled in (_scaled_order(order, scale) for order in orders) if scaled is not None]
        blocked_symbols = sorted({order.symbol for order in orders if order.symbol not in {row.symbol for row in scaled_orders}})
        triggers.append(
            RiskControlTrigger(
                as_of=as_of,
                scope="portfolio",
                scope_id="portfolio",
                trigger_type="drawdown_warning",
                severity="warning",
                threshold=restrict_threshold,
                observed_value=drawdown,
                operating_state="restricted",
                message="portfolio drawdown breached restriction threshold",
                metadata={"baseline_equity": baseline, "current_equity": float(state.equity)},
            )
        )
        actions.append(
            RiskControlAction(
                as_of=as_of,
                scope="portfolio",
                scope_id="portfolio",
                action="throttle_orders",
                operating_state="restricted",
                quantity_scale=scale,
                affected_symbols=sorted({order.symbol for order in orders}),
                reason_codes=["drawdown_warning"],
                metadata={"blocked_symbols": blocked_symbols},
            )
        )
        events.append(
            RiskControlEvent(
                as_of=as_of,
                scope="portfolio",
                scope_id="portfolio",
                previous_state="healthy",
                new_state="restricted",
                action="throttle_orders",
                reason_codes=["drawdown_warning"],
                metadata={"quantity_scale": scale},
            )
        )
        return scaled_orders, triggers, actions, events, operating_state

    return list(orders), triggers, actions, events, operating_state


def build_paper_risk_control_report(
    *,
    result: "PaperTradingRunResult",
    config: "PaperTradingConfig",
    starting_state: "PaperPortfolioState | None" = None,
    pretrade_triggers: list[RiskControlTrigger] | None = None,
    pretrade_actions: list[RiskControlAction] | None = None,
    pretrade_events: list[RiskControlEvent] | None = None,
    pretrade_state: str = "healthy",
) -> PaperRiskControlReport:
    enabled = bool(getattr(config, "risk_controls_enabled", False))
    operating_state = pretrade_state if pretrade_state in OPERATING_STATES else "healthy"
    triggers = list(pretrade_triggers or [])
    actions = list(pretrade_actions or [])
    events = list(pretrade_events or [])

    baseline = float(
        (starting_state.initial_cash_basis if starting_state is not None else 0.0)
        or result.state.initial_cash_basis
        or result.state.equity
        or 0.0
    )
    portfolio_drawdown = _drawdown(equity=float(result.state.equity), baseline=baseline)
    halt_drawdown = _safe_float(getattr(config, "risk_halt_drawdown", None))
    restrict_drawdown = _safe_float(getattr(config, "risk_restrict_drawdown", None))

    def add_trigger(trigger: RiskControlTrigger) -> None:
        nonlocal operating_state
        triggers.append(trigger)
        operating_state = _transition_state(operating_state, trigger.operating_state)

    if enabled and portfolio_drawdown is not None:
        if halt_drawdown is not None and portfolio_drawdown >= halt_drawdown and pretrade_state != "halted":
            add_trigger(
                RiskControlTrigger(
                    as_of=result.as_of,
                    scope="portfolio",
                    scope_id="portfolio",
                    trigger_type="drawdown_breach",
                    severity="critical",
                    threshold=halt_drawdown,
                    observed_value=portfolio_drawdown,
                    operating_state="halted",
                    message="portfolio drawdown breached halt threshold",
                    metadata={"baseline_equity": baseline, "ending_equity": float(result.state.equity)},
                )
            )
        elif restrict_drawdown is not None and portfolio_drawdown >= restrict_drawdown and pretrade_state == "healthy":
            add_trigger(
                RiskControlTrigger(
                    as_of=result.as_of,
                    scope="portfolio",
                    scope_id="portfolio",
                    trigger_type="drawdown_warning",
                    severity="warning",
                    threshold=restrict_drawdown,
                    observed_value=portfolio_drawdown,
                    operating_state="restricted",
                    message="portfolio drawdown breached restriction threshold",
                    metadata={"baseline_equity": baseline, "ending_equity": float(result.state.equity)},
                )
            )

    restrict_gap = _safe_float(getattr(config, "risk_restrict_forecast_gap", None))
    halt_gap = _safe_float(getattr(config, "risk_halt_forecast_gap", None))
    outcome_report = result.outcome_attribution_report
    if enabled and outcome_report is not None:
        for aggregate in outcome_report.aggregates:
            if aggregate.group_type not in {"strategy", "instrument"}:
                continue
            forecast_gap = abs(float(aggregate.mean_forecast_gap)) if aggregate.mean_forecast_gap is not None else None
            if forecast_gap is None:
                continue
            if halt_gap is not None and forecast_gap >= halt_gap:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope=aggregate.group_type,
                        scope_id=aggregate.group_key,
                        trigger_type="expected_realized_divergence",
                        severity="critical",
                        threshold=halt_gap,
                        observed_value=forecast_gap,
                        operating_state="halted",
                        message=f"{aggregate.group_type} forecast gap breached halt threshold",
                    )
                )
            elif restrict_gap is not None and forecast_gap >= restrict_gap:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope=aggregate.group_type,
                        scope_id=aggregate.group_key,
                        trigger_type="expected_realized_divergence",
                        severity="warning",
                        threshold=restrict_gap,
                        observed_value=forecast_gap,
                        operating_state="restricted",
                        message=f"{aggregate.group_type} forecast gap breached restriction threshold",
                    )
                )

    execution_report = result.execution_simulation_report
    restrict_reject = _safe_float(getattr(config, "risk_restrict_rejected_order_ratio", None))
    halt_reject = _safe_float(getattr(config, "risk_halt_rejected_order_ratio", None))
    restrict_shortfall = _safe_float(getattr(config, "risk_restrict_execution_shortfall", None))
    halt_shortfall = _safe_float(getattr(config, "risk_halt_execution_shortfall", None))
    if enabled and execution_report is not None:
        summary = dict(execution_report.summary or {})
        rejected_ratio = _safe_float(summary.get("rejected_order_ratio"))
        partial_fill_ratio = None
        requested_count = int(summary.get("requested_order_count", 0) or 0)
        partial_fill_count = int(summary.get("partial_fill_order_count", 0) or 0)
        if requested_count > 0:
            partial_fill_ratio = float(partial_fill_count / requested_count)
        if rejected_ratio is not None:
            if halt_reject is not None and rejected_ratio >= halt_reject:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope="portfolio",
                        scope_id="portfolio",
                        trigger_type="execution_rejections",
                        severity="critical",
                        threshold=halt_reject,
                        observed_value=rejected_ratio,
                        operating_state="halted",
                        message="portfolio rejected-order ratio breached halt threshold",
                    )
                )
            elif restrict_reject is not None and rejected_ratio >= restrict_reject:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope="portfolio",
                        scope_id="portfolio",
                        trigger_type="execution_rejections",
                        severity="warning",
                        threshold=restrict_reject,
                        observed_value=rejected_ratio,
                        operating_state="restricted",
                        message="portfolio rejected-order ratio breached restriction threshold",
                    )
                )
        if partial_fill_ratio is not None:
            if halt_shortfall is not None and partial_fill_ratio >= halt_shortfall:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope="portfolio",
                        scope_id="portfolio",
                        trigger_type="execution_partial_fill_ratio",
                        severity="critical",
                        threshold=halt_shortfall,
                        observed_value=partial_fill_ratio,
                        operating_state="halted",
                        message="portfolio partial-fill ratio breached halt threshold",
                    )
                )
            elif restrict_shortfall is not None and partial_fill_ratio >= restrict_shortfall:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope="portfolio",
                        scope_id="portfolio",
                        trigger_type="execution_partial_fill_ratio",
                        severity="warning",
                        threshold=restrict_shortfall,
                        observed_value=partial_fill_ratio,
                        operating_state="restricted",
                        message="portfolio partial-fill ratio breached restriction threshold",
                    )
                )
        for order in execution_report.orders:
            shortfall = float(max(0.0, 1.0 - float(order.filled_fraction)))
            state_for_order = None
            threshold = None
            severity = None
            if halt_shortfall is not None and shortfall >= halt_shortfall:
                state_for_order = "halted"
                threshold = halt_shortfall
                severity = "critical"
            elif restrict_shortfall is not None and shortfall >= restrict_shortfall:
                state_for_order = "restricted"
                threshold = restrict_shortfall
                severity = "warning"
            if state_for_order is not None:
                add_trigger(
                    RiskControlTrigger(
                        as_of=result.as_of,
                        scope="instrument",
                        scope_id=order.symbol,
                        trigger_type="execution_shortfall",
                        severity=str(severity),
                        threshold=threshold,
                        observed_value=shortfall,
                        operating_state=state_for_order,
                        message="instrument execution shortfall breached risk threshold",
                        metadata={"status": order.status, "filled_fraction": float(order.filled_fraction)},
                    )
                )

    if enabled and bool(getattr(config, "risk_halt_on_system_health_failure", False)):
        system_health = build_system_health_payload(result=result)
        failing_checks = [row for row in system_health.checks if row.status != "pass"]
        if failing_checks:
            add_trigger(
                RiskControlTrigger(
                    as_of=result.as_of,
                    scope="portfolio",
                    scope_id="portfolio",
                    trigger_type="system_health_degradation",
                    severity="critical",
                    threshold=0.0,
                    observed_value=float(len(failing_checks)),
                    operating_state="halted",
                    message="system health failures triggered halt state",
                    metadata={"failed_checks": [row.check_name for row in failing_checks]},
                )
            )

    observed_states: dict[tuple[str, str], str] = {}
    observed_reasons: dict[tuple[str, str], list[str]] = {}
    for trigger in triggers:
        key = (trigger.scope, trigger.scope_id)
        observed_states[key] = _transition_state(observed_states.get(key, "healthy"), trigger.operating_state)
        observed_reasons.setdefault(key, []).append(trigger.trigger_type)

    existing_keys = {(event.scope, event.scope_id, event.new_state, event.action) for event in events}
    existing_action_keys = {(action.scope, action.scope_id, action.action) for action in actions}
    for (scope, scope_id), state in sorted(observed_states.items()):
        if state == "healthy":
            continue
        action_name = "recommend_halt" if state == "halted" else "recommend_restrict"
        if (scope, scope_id, state, action_name) not in existing_keys:
            events.append(
                RiskControlEvent(
                    as_of=result.as_of,
                    scope=scope,
                    scope_id=scope_id,
                    previous_state="healthy",
                    new_state=state,
                    action=action_name,
                    reason_codes=sorted(set(observed_reasons.get((scope, scope_id), []))),
                )
            )
        effective_action = "halt_trading" if state == "halted" and scope == "portfolio" else action_name
        if (scope, scope_id, effective_action) in existing_action_keys:
            continue
        if scope == "portfolio" and state == "halted":
            actions.append(
                RiskControlAction(
                    as_of=result.as_of,
                    scope=scope,
                    scope_id=scope_id,
                    action="halt_trading",
                    operating_state=state,
                    quantity_scale=0.0,
                    affected_symbols=sorted({order.symbol for order in result.orders}),
                    reason_codes=sorted(set(observed_reasons.get((scope, scope_id), []))),
                )
            )
        else:
            actions.append(
                RiskControlAction(
                    as_of=result.as_of,
                    scope=scope,
                    scope_id=scope_id,
                    action="recommend_halt" if state == "halted" else "recommend_restrict",
                    operating_state=state,
                    quantity_scale=float(getattr(config, "risk_restricted_order_quantity_scale", 0.5))
                    if state == "restricted"
                    else 0.0,
                    affected_symbols=sorted(
                        {
                            order.symbol
                            for order in result.orders
                            if scope == "portfolio"
                            or order.symbol == scope_id
                            or _strategy_id_from_order(order) == scope_id
                        }
                    ),
                    reason_codes=sorted(set(observed_reasons.get((scope, scope_id), []))),
                )
            )

    summary = {
        "trigger_count": len(triggers),
        "event_count": len(events),
        "action_count": len(actions),
        "operating_state": operating_state,
        "portfolio_drawdown": portfolio_drawdown,
        "restricted_scope_count": sum(1 for state in observed_states.values() if state == "restricted"),
        "halted_scope_count": sum(1 for state in observed_states.values() if state == "halted"),
    }
    return PaperRiskControlReport(
        as_of=result.as_of,
        enabled=enabled,
        operating_state=operating_state,
        triggers=triggers,
        actions=actions,
        events=events,
        summary=summary,
    )


def write_paper_risk_control_artifacts(*, output_dir: str | Path, report: PaperRiskControlReport) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "paper_risk_controls.json"
    triggers_path = output_path / "paper_risk_control_triggers.csv"
    actions_path = output_path / "paper_risk_control_actions.csv"
    events_path = output_path / "paper_risk_control_events.csv"
    summary_path = output_path / "paper_risk_control_summary.json"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    summary_path.write_text(json.dumps(report.summary, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in report.triggers]).to_csv(triggers_path, index=False)
    pd.DataFrame([row.flat_dict() for row in report.actions]).to_csv(actions_path, index=False)
    pd.DataFrame([row.flat_dict() for row in report.events]).to_csv(events_path, index=False)
    return {
        "paper_risk_controls_json_path": json_path,
        "paper_risk_control_triggers_csv_path": triggers_path,
        "paper_risk_control_actions_csv_path": actions_path,
        "paper_risk_control_events_csv_path": events_path,
        "paper_risk_control_summary_json_path": summary_path,
    }
