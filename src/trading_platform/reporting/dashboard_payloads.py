from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.decision_journal.models import TradeDecision
    from trading_platform.execution.order_lifecycle import OrderLifecycleRecord
    from trading_platform.paper.models import PaperTradingRunResult


KPI_SCHEMA_VERSION = "kpi_payload_v1"
TRADE_EXPLORER_SCHEMA_VERSION = "trade_explorer_payload_v1"
STRATEGY_HEALTH_SCHEMA_VERSION = "strategy_health_payload_v1"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_tags(tags: list[str] | None) -> list[str]:
    values = [str(tag).strip() for tag in tags or [] if str(tag).strip()]
    return sorted(dict.fromkeys(values))


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


def _flat_dict(value: dict[str, Any]) -> str:
    return "|".join(f"{key}={value[key]}" for key in sorted(value) if value[key] not in (None, "", [], {}))


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


@dataclass(frozen=True)
class KpiRecord:
    as_of: str
    scope: str
    entity_id: str
    metric_name: str
    metric_value: float
    unit: str = "scalar"
    tags: list[str] = field(default_factory=list)
    dimensions: dict[str, Any] = field(default_factory=dict)
    source_artifact: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", _normalize_tags(self.tags))
        object.__setattr__(self, "dimensions", _normalize_metadata(self.dimensions))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scope": self.scope,
            "entity_id": self.entity_id,
            "metric_name": self.metric_name,
            "metric_value": float(self.metric_value),
            "unit": self.unit,
            "tags": list(self.tags),
            "dimensions": dict(self.dimensions),
            "source_artifact": self.source_artifact,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scope": self.scope,
            "entity_id": self.entity_id,
            "metric_name": self.metric_name,
            "metric_value": float(self.metric_value),
            "unit": self.unit,
            "tags": "|".join(self.tags),
            "dimensions": _flat_dict(self.dimensions),
            "source_artifact": self.source_artifact,
            "metadata": _flat_dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "KpiRecord":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            scope=str(data["scope"]),
            entity_id=str(data["entity_id"]),
            metric_name=str(data["metric_name"]),
            metric_value=float(data["metric_value"]),
            unit=str(data.get("unit", "scalar")),
            tags=[str(tag) for tag in data.get("tags", [])],
            dimensions=dict(data.get("dimensions") or {}),
            source_artifact=str(data["source_artifact"]) if data.get("source_artifact") is not None else None,
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class KpiPayload:
    as_of: str
    schema_version: str = KPI_SCHEMA_VERSION
    records: list[KpiRecord] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "records": [row.to_dict() for row in self.records],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "KpiPayload":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", KPI_SCHEMA_VERSION)),
            records=[KpiRecord.from_dict(row) for row in data.get("records", [])],
            summary=dict(data.get("summary") or {}),
        )


@dataclass(frozen=True)
class TradeExplorerRow:
    trade_id: str
    symbol: str
    strategy_id: str
    as_of: str
    status: str
    side: str | None = None
    quantity: int = 0
    decision_id: str | None = None
    entry_date: str | None = None
    exit_date: str | None = None
    predicted_return: float | None = None
    expected_value_net: float | None = None
    confidence_score: float | None = None
    reliability_score: float | None = None
    realized_pnl: float | None = None
    total_execution_cost: float | None = None
    vetoed: bool = False
    veto_reasons: list[str] = field(default_factory=list)
    rationale_summary: str | None = None
    lifecycle_status: str | None = None
    reconciliation_status: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "veto_reasons", _normalize_tags(self.veto_reasons))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "strategy_id": self.strategy_id,
            "as_of": self.as_of,
            "status": self.status,
            "side": self.side,
            "quantity": int(self.quantity),
            "decision_id": self.decision_id,
            "entry_date": self.entry_date,
            "exit_date": self.exit_date,
            "predicted_return": self.predicted_return,
            "expected_value_net": self.expected_value_net,
            "confidence_score": self.confidence_score,
            "reliability_score": self.reliability_score,
            "realized_pnl": self.realized_pnl,
            "total_execution_cost": self.total_execution_cost,
            "vetoed": bool(self.vetoed),
            "veto_reasons": list(self.veto_reasons),
            "rationale_summary": self.rationale_summary,
            "lifecycle_status": self.lifecycle_status,
            "reconciliation_status": self.reconciliation_status,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["veto_reasons"] = "|".join(self.veto_reasons)
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeExplorerRow":
        data = dict(payload or {})
        return cls(
            trade_id=str(data["trade_id"]),
            symbol=str(data["symbol"]),
            strategy_id=str(data["strategy_id"]),
            as_of=str(data["as_of"]),
            status=str(data["status"]),
            side=str(data["side"]) if data.get("side") is not None else None,
            quantity=int(data.get("quantity", 0) or 0),
            decision_id=str(data["decision_id"]) if data.get("decision_id") is not None else None,
            entry_date=str(data["entry_date"]) if data.get("entry_date") is not None else None,
            exit_date=str(data["exit_date"]) if data.get("exit_date") is not None else None,
            predicted_return=_safe_float(data.get("predicted_return")),
            expected_value_net=_safe_float(data.get("expected_value_net")),
            confidence_score=_safe_float(data.get("confidence_score")),
            reliability_score=_safe_float(data.get("reliability_score")),
            realized_pnl=_safe_float(data.get("realized_pnl")),
            total_execution_cost=_safe_float(data.get("total_execution_cost")),
            vetoed=bool(data.get("vetoed", False)),
            veto_reasons=[str(reason) for reason in data.get("veto_reasons", [])],
            rationale_summary=str(data["rationale_summary"]) if data.get("rationale_summary") is not None else None,
            lifecycle_status=str(data["lifecycle_status"]) if data.get("lifecycle_status") is not None else None,
            reconciliation_status=(
                str(data["reconciliation_status"]) if data.get("reconciliation_status") is not None else None
            ),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class TradeExplorerPayload:
    as_of: str
    schema_version: str = TRADE_EXPLORER_SCHEMA_VERSION
    rows: list[TradeExplorerRow] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeExplorerPayload":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", TRADE_EXPLORER_SCHEMA_VERSION)),
            rows=[TradeExplorerRow.from_dict(row) for row in data.get("rows", [])],
            summary=dict(data.get("summary") or {}),
        )


@dataclass(frozen=True)
class StrategyHealthRow:
    strategy_id: str
    as_of: str
    status: str
    decision_count: int = 0
    veto_count: int = 0
    mismatch_count: int = 0
    total_pnl: float | None = None
    realized_pnl: float | None = None
    unrealized_pnl: float | None = None
    total_execution_cost: float | None = None
    turnover: float | None = None
    trade_count: int | None = None
    win_rate: float | None = None
    expected_value_net_mean: float | None = None
    reliability_score_mean: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "as_of": self.as_of,
            "status": self.status,
            "decision_count": int(self.decision_count),
            "veto_count": int(self.veto_count),
            "mismatch_count": int(self.mismatch_count),
            "total_pnl": self.total_pnl,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_execution_cost": self.total_execution_cost,
            "turnover": self.turnover,
            "trade_count": self.trade_count,
            "win_rate": self.win_rate,
            "expected_value_net_mean": self.expected_value_net_mean,
            "reliability_score_mean": self.reliability_score_mean,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyHealthRow":
        data = dict(payload or {})
        return cls(
            strategy_id=str(data["strategy_id"]),
            as_of=str(data["as_of"]),
            status=str(data["status"]),
            decision_count=int(data.get("decision_count", 0) or 0),
            veto_count=int(data.get("veto_count", 0) or 0),
            mismatch_count=int(data.get("mismatch_count", 0) or 0),
            total_pnl=_safe_float(data.get("total_pnl")),
            realized_pnl=_safe_float(data.get("realized_pnl")),
            unrealized_pnl=_safe_float(data.get("unrealized_pnl")),
            total_execution_cost=_safe_float(data.get("total_execution_cost")),
            turnover=_safe_float(data.get("turnover")),
            trade_count=int(data["trade_count"]) if data.get("trade_count") is not None else None,
            win_rate=_safe_float(data.get("win_rate")),
            expected_value_net_mean=_safe_float(data.get("expected_value_net_mean")),
            reliability_score_mean=_safe_float(data.get("reliability_score_mean")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class StrategyHealthPayload:
    as_of: str
    schema_version: str = STRATEGY_HEALTH_SCHEMA_VERSION
    rows: list[StrategyHealthRow] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyHealthPayload":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", STRATEGY_HEALTH_SCHEMA_VERSION)),
            rows=[StrategyHealthRow.from_dict(row) for row in data.get("rows", [])],
            summary=dict(data.get("summary") or {}),
        )


def build_kpi_payload(*, result: "PaperTradingRunResult") -> KpiPayload:
    records: list[KpiRecord] = [
        KpiRecord(result.as_of, "portfolio", "paper_portfolio", "equity", float(result.state.equity), unit="usd"),
        KpiRecord(result.as_of, "portfolio", "paper_portfolio", "cash", float(result.state.cash), unit="usd"),
        KpiRecord(
            result.as_of,
            "portfolio",
            "paper_portfolio",
            "gross_market_value",
            float(result.state.gross_market_value),
            unit="usd",
        ),
        KpiRecord(result.as_of, "portfolio", "paper_portfolio", "total_pnl", float(result.state.total_pnl), unit="usd"),
        KpiRecord(
            result.as_of,
            "portfolio",
            "paper_portfolio",
            "total_execution_cost",
            float(result.state.cumulative_execution_cost),
            unit="usd",
        ),
        KpiRecord(result.as_of, "portfolio", "paper_portfolio", "order_count", float(len(result.orders)), unit="count"),
        KpiRecord(result.as_of, "portfolio", "paper_portfolio", "fill_count", float(len(result.fills)), unit="count"),
        KpiRecord(
            result.as_of,
            "portfolio",
            "paper_portfolio",
            "target_symbol_count",
            float(len(result.latest_target_weights)),
            unit="count",
        ),
        KpiRecord(
            result.as_of,
            "portfolio",
            "paper_portfolio",
            "position_count",
            float(len(result.state.positions)),
            unit="count",
        ),
    ]
    if result.reconciliation_result is not None:
        records.append(
            KpiRecord(
                result.as_of,
                "portfolio",
                "paper_portfolio",
                "reconciliation_mismatch_count",
                float(len(result.reconciliation_result.mismatches)),
                unit="count",
            )
        )
    if result.trade_decision_contracts:
        veto_count = sum(1 for row in result.trade_decision_contracts if row.vetoed)
        mean_ev = _mean([row.expected_value_net for row in result.trade_decision_contracts])
        mean_reliability = _mean([row.reliability_score for row in result.trade_decision_contracts])
        records.append(
            KpiRecord(
                result.as_of,
                "trade",
                "trade_decisions",
                "decision_count",
                float(len(result.trade_decision_contracts)),
                unit="count",
            )
        )
        records.append(
            KpiRecord(result.as_of, "trade", "trade_decisions", "veto_count", float(veto_count), unit="count")
        )
        if mean_ev is not None:
            records.append(KpiRecord(result.as_of, "trade", "trade_decisions", "mean_expected_value_net", mean_ev))
        if mean_reliability is not None:
            records.append(
                KpiRecord(result.as_of, "trade", "trade_decisions", "mean_reliability_score", mean_reliability)
            )
    if result.outcome_attribution_report is not None:
        summary = dict(result.outcome_attribution_report.summary)
        records.append(
            KpiRecord(
                result.as_of,
                "trade_outcome",
                "trade_outcomes",
                "closed_trade_count",
                float(summary.get("closed_trade_count", 0) or 0.0),
                unit="count",
            )
        )
        for metric_name in (
            "mean_predicted_net_return",
            "mean_realized_net_return",
            "mean_forecast_gap",
            "mean_alpha_error",
            "mean_cost_error",
        ):
            metric_value = _safe_float(summary.get(metric_name))
            if metric_value is None:
                continue
            records.append(
                KpiRecord(
                    result.as_of,
                    "trade_outcome",
                    "trade_outcomes",
                    metric_name,
                    metric_value,
                    unit="ratio",
                )
            )
    if result.risk_control_report is not None:
        risk_summary = dict(result.risk_control_report.summary)
        state_value = {"healthy": 0.0, "restricted": 1.0, "halted": 2.0}.get(
            str(result.risk_control_report.operating_state), 0.0
        )
        records.append(
            KpiRecord(
                result.as_of,
                "risk_control",
                "paper_risk_controls",
                "operating_state_code",
                state_value,
                unit="state_code",
            )
        )
        for metric_name in ("trigger_count", "action_count", "event_count", "portfolio_drawdown"):
            metric_value = _safe_float(risk_summary.get(metric_name))
            if metric_value is None:
                continue
            records.append(
                KpiRecord(
                    result.as_of,
                    "risk_control",
                    "paper_risk_controls",
                    metric_name,
                    metric_value,
                    unit="count" if metric_name.endswith("count") else "ratio",
                )
            )
    if result.drift_report is not None:
        drift_summary = dict(result.drift_report.summary)
        records.append(
            KpiRecord(
                result.as_of,
                "drift",
                "drift_detection",
                "signal_count",
                float(drift_summary.get("signal_count", 0) or 0.0),
                unit="count",
            )
        )
        severity_rank = {"info": 1.0, "watch": 2.0, "warning": 3.0, "critical": 4.0}
        highest_severity = str(drift_summary.get("highest_severity") or "")
        if highest_severity in severity_rank:
            records.append(
                KpiRecord(
                    result.as_of,
                    "drift",
                    "drift_detection",
                    "highest_severity_code",
                    severity_rank[highest_severity],
                    unit="state_code",
                )
            )
        for metric_name in ("snapshot_count",):
            metric_value = _safe_float(drift_summary.get(metric_name))
            if metric_value is None:
                continue
            records.append(
                KpiRecord(
                    result.as_of,
                    "drift",
                    "drift_detection",
                    metric_name,
                    metric_value,
                    unit="count",
                )
            )
    if result.calibration_report is not None:
        calibration_summary = dict(result.calibration_report.summary)
        records.append(
            KpiRecord(
                result.as_of,
                "calibration",
                "calibration_pipeline",
                "record_count",
                float(calibration_summary.get("record_count", 0) or 0.0),
                unit="count",
            )
        )
        for metric_name in (
            "bucket_count",
            "sufficient_scope_count",
            "mean_raw_confidence_error",
            "mean_calibrated_confidence_error",
            "mean_raw_expected_value_error",
            "mean_calibrated_expected_value_error",
        ):
            metric_value = _safe_float(calibration_summary.get(metric_name))
            if metric_value is None:
                continue
            records.append(
                KpiRecord(
                    result.as_of,
                    "calibration",
                    "calibration_pipeline",
                    metric_name,
                    metric_value,
                    unit="count" if metric_name.endswith("count") else "ratio",
                )
            )
    if result.strategy_decay_report is not None:
        decay_summary = dict(result.strategy_decay_report.summary)
        records.append(
            KpiRecord(
                result.as_of,
                "strategy_decay",
                "strategy_decay",
                "strategy_count",
                float(decay_summary.get("strategy_count", 0) or 0.0),
                unit="count",
            )
        )
        for metric_name in ("signal_count", "critical_count", "warning_count", "watch_count", "portfolio_decay_score"):
            metric_value = _safe_float(decay_summary.get(metric_name))
            if metric_value is None:
                continue
            records.append(
                KpiRecord(
                    result.as_of,
                    "strategy_decay",
                    "strategy_decay",
                    metric_name,
                    metric_value,
                    unit="count" if metric_name.endswith("count") else "ratio",
                )
            )
    if result.strategy_lifecycle_report is not None:
        lifecycle_summary = dict(result.strategy_lifecycle_report.summary)
        records.append(
            KpiRecord(
                result.as_of,
                "strategy_lifecycle",
                "strategy_lifecycle",
                "strategy_count",
                float(lifecycle_summary.get("strategy_count", 0) or 0.0),
                unit="count",
            )
        )
        for metric_name in (
            "action_count",
            "transition_count",
            "demotion_count",
            "retraining_trigger_count",
            "watch_count",
            "constrained_count",
            "demoted_count",
            "suppressed_action_count",
        ):
            metric_value = _safe_float(lifecycle_summary.get(metric_name))
            if metric_value is None:
                continue
            records.append(
                KpiRecord(
                    result.as_of,
                    "strategy_lifecycle",
                    "strategy_lifecycle",
                    metric_name,
                    metric_value,
                    unit="count",
                )
            )
    for row in list(result.attribution.get("strategy_rows", [])):
        strategy_id = str(row.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        for metric_name in ("total_pnl", "realized_pnl", "unrealized_pnl", "total_execution_cost", "turnover", "win_rate"):
            metric_value = _safe_float(row.get(metric_name))
            if metric_value is None:
                continue
            unit = "usd" if "pnl" in metric_name or "cost" in metric_name else ("ratio" if "rate" in metric_name else "scalar")
            records.append(KpiRecord(result.as_of, "strategy", strategy_id, metric_name, metric_value, unit=unit))
    return KpiPayload(
        as_of=result.as_of,
        records=records,
        summary={
            "record_count": len(records),
            "scope_counts": {
                scope: sum(1 for row in records if row.scope == scope)
                for scope in sorted({row.scope for row in records})
            },
        },
    )


def build_trade_explorer_payload(*, result: "PaperTradingRunResult") -> TradeExplorerPayload:
    decisions_by_symbol: dict[str, TradeDecision] = {
        row.instrument: row for row in sorted(result.trade_decision_contracts, key=lambda item: (item.instrument, item.decision_id))
    }
    lifecycle_by_symbol: dict[str, OrderLifecycleRecord] = {
        row.intent.symbol: row for row in sorted(result.order_lifecycle_records, key=lambda item: (item.intent.symbol, item.intent.order_id))
    }
    mismatch_by_symbol: dict[str, list[str]] = {}
    if result.reconciliation_result is not None:
        for mismatch in result.reconciliation_result.mismatches:
            mismatch_by_symbol.setdefault(mismatch.symbol, []).append(mismatch.reason_code)
    trade_rows = list(result.attribution.get("trade_rows", []))
    trade_by_symbol: dict[str, dict[str, Any]] = {
        str(row.get("symbol") or ""): dict(row)
        for row in sorted(trade_rows, key=lambda item: (str(item.get("symbol") or ""), str(item.get("trade_id") or "")))
        if str(row.get("symbol") or "")
    }
    rows: list[TradeExplorerRow] = []
    symbols = sorted(set(decisions_by_symbol) | set(lifecycle_by_symbol) | set(trade_by_symbol))
    for symbol in symbols:
        decision = decisions_by_symbol.get(symbol)
        lifecycle = lifecycle_by_symbol.get(symbol)
        trade = trade_by_symbol.get(symbol, {})
        mismatch_codes = sorted(mismatch_by_symbol.get(symbol, []))
        rows.append(
            TradeExplorerRow(
                trade_id=str(trade.get("trade_id") or getattr(getattr(lifecycle, "intent", None), "order_id", None) or getattr(decision, "decision_id", symbol)),
                symbol=symbol,
                strategy_id=str(trade.get("strategy_id") or getattr(decision, "strategy_id", "unknown_strategy")),
                as_of=result.as_of,
                status=str(trade.get("status") or getattr(lifecycle, "final_status", None) or ("vetoed" if getattr(decision, "vetoed", False) else "planned")),
                side=str(trade.get("side") or getattr(decision, "side", None)) if (trade.get("side") or getattr(decision, "side", None)) is not None else None,
                quantity=int(trade.get("quantity", getattr(getattr(lifecycle, "intent", None), "quantity", 0)) or 0),
                decision_id=getattr(decision, "decision_id", None),
                entry_date=str(trade.get("entry_date")) if trade.get("entry_date") is not None else None,
                exit_date=str(trade.get("exit_date")) if trade.get("exit_date") is not None else None,
                predicted_return=getattr(decision, "predicted_return", None),
                expected_value_net=getattr(decision, "expected_value_net", None),
                confidence_score=getattr(decision, "confidence_score", None),
                reliability_score=getattr(decision, "reliability_score", None),
                realized_pnl=_safe_float(trade.get("realized_pnl")),
                total_execution_cost=_safe_float(trade.get("total_execution_cost")),
                vetoed=bool(getattr(decision, "vetoed", False)),
                veto_reasons=list(getattr(decision, "veto_reasons", [])),
                rationale_summary=getattr(decision, "rationale_summary", None),
                lifecycle_status=getattr(lifecycle, "final_status", None),
                reconciliation_status="mismatch" if mismatch_codes else "reconciled",
                metadata={
                    "signal_family": getattr(decision, "strategy_family", None),
                    "mismatch_reason_codes": mismatch_codes,
                },
            )
        )
    return TradeExplorerPayload(
        as_of=result.as_of,
        rows=rows,
        summary={
            "row_count": len(rows),
            "vetoed_count": sum(1 for row in rows if row.vetoed),
            "mismatch_count": sum(1 for row in rows if row.reconciliation_status == "mismatch"),
            "closed_trade_count": sum(1 for row in rows if row.exit_date is not None),
        },
    )


def build_strategy_health_payload(*, result: "PaperTradingRunResult") -> StrategyHealthPayload:
    attribution_by_strategy = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in list(result.attribution.get("strategy_rows", []))
        if str(row.get("strategy_id") or "").strip()
    }
    decisions_by_strategy: dict[str, list[TradeDecision]] = {}
    for row in result.trade_decision_contracts:
        decisions_by_strategy.setdefault(row.strategy_id, []).append(row)
    mismatch_by_strategy: dict[str, int] = {}
    if result.reconciliation_result is not None:
        decision_symbol_to_strategy = {row.instrument: row.strategy_id for row in result.trade_decision_contracts}
        for mismatch in result.reconciliation_result.mismatches:
            strategy_id = decision_symbol_to_strategy.get(mismatch.symbol, "unknown_strategy")
            mismatch_by_strategy[strategy_id] = mismatch_by_strategy.get(strategy_id, 0) + 1
    strategy_ids = sorted(set(attribution_by_strategy) | set(decisions_by_strategy))
    rows: list[StrategyHealthRow] = []
    for strategy_id in strategy_ids:
        attribution_row = attribution_by_strategy.get(strategy_id, {})
        decisions = decisions_by_strategy.get(strategy_id, [])
        veto_count = sum(1 for row in decisions if row.vetoed)
        mismatch_count = int(mismatch_by_strategy.get(strategy_id, 0))
        status = "mismatch_detected" if mismatch_count > 0 else "observed"
        rows.append(
            StrategyHealthRow(
                strategy_id=strategy_id,
                as_of=result.as_of,
                status=status,
                decision_count=len(decisions),
                veto_count=veto_count,
                mismatch_count=mismatch_count,
                total_pnl=_safe_float(attribution_row.get("total_pnl")),
                realized_pnl=_safe_float(attribution_row.get("realized_pnl")),
                unrealized_pnl=_safe_float(attribution_row.get("unrealized_pnl")),
                total_execution_cost=_safe_float(attribution_row.get("total_execution_cost")),
                turnover=_safe_float(attribution_row.get("turnover")),
                trade_count=int(attribution_row["trade_count"]) if attribution_row.get("trade_count") is not None else None,
                win_rate=_safe_float(attribution_row.get("win_rate")),
                expected_value_net_mean=_mean([row.expected_value_net for row in decisions]),
                reliability_score_mean=_mean([row.reliability_score for row in decisions]),
                metadata={
                    "veto_rate": float(veto_count / len(decisions)) if decisions else None,
                },
            )
        )
    ordered_by_pnl = sorted(rows, key=lambda row: (row.total_pnl if row.total_pnl is not None else float("-inf")), reverse=True)
    return StrategyHealthPayload(
        as_of=result.as_of,
        rows=rows,
        summary={
            "strategy_count": len(rows),
            "mismatch_strategy_count": sum(1 for row in rows if row.mismatch_count > 0),
            "top_strategy_by_total_pnl": ordered_by_pnl[0].strategy_id if ordered_by_pnl else None,
        },
    )


def write_kpi_payload_artifacts(*, output_dir: str | Path, payload: KpiPayload) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "kpi_payload.json"
    csv_path = output_path / "kpi_records.csv"
    json_path.write_text(json.dumps(payload.to_dict(), indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in payload.records]).to_csv(csv_path, index=False)
    return {"kpi_payload_json_path": json_path, "kpi_records_csv_path": csv_path}


def write_trade_explorer_payload_artifacts(
    *,
    output_dir: str | Path,
    payload: TradeExplorerPayload,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "trade_explorer_payload.json"
    csv_path = output_path / "trade_explorer_rows.csv"
    json_path.write_text(json.dumps(payload.to_dict(), indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in payload.rows]).to_csv(csv_path, index=False)
    return {"trade_explorer_payload_json_path": json_path, "trade_explorer_rows_csv_path": csv_path}


def write_strategy_health_payload_artifacts(
    *,
    output_dir: str | Path,
    payload: StrategyHealthPayload,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "strategy_health_payload.json"
    csv_path = output_path / "strategy_health_payload.csv"
    json_path.write_text(json.dumps(payload.to_dict(), indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in payload.rows]).to_csv(csv_path, index=False)
    return {"strategy_health_payload_json_path": json_path, "strategy_health_payload_csv_path": csv_path}
