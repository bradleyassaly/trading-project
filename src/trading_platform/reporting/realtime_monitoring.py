from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult


REALTIME_MONITORING_SCHEMA_VERSION = "realtime_kpi_monitoring_v1"


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


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


def _flat_metadata(metadata: dict[str, Any]) -> str:
    return "|".join(f"{key}={metadata[key]}" for key in sorted(metadata) if metadata[key] not in (None, "", [], {}))


@dataclass(frozen=True)
class RealtimeMonitoringMetric:
    as_of: str
    metric_name: str
    metric_value: float
    unit: str = "scalar"
    scope: str = "paper_portfolio"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "metric_name": self.metric_name,
            "metric_value": float(self.metric_value),
            "unit": self.unit,
            "scope": self.scope,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_metadata(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RealtimeMonitoringMetric":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            metric_name=str(data["metric_name"]),
            metric_value=float(data["metric_value"]),
            unit=str(data.get("unit", "scalar")),
            scope=str(data.get("scope", "paper_portfolio")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class RealtimeMonitoringPayload:
    as_of: str
    schema_version: str = REALTIME_MONITORING_SCHEMA_VERSION
    metrics: list[RealtimeMonitoringMetric] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "metrics": [row.to_dict() for row in self.metrics],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RealtimeMonitoringPayload":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", REALTIME_MONITORING_SCHEMA_VERSION)),
            metrics=[RealtimeMonitoringMetric.from_dict(row) for row in data.get("metrics", [])],
            summary=dict(data.get("summary") or {}),
        )


def build_realtime_monitoring_payload(*, result: "PaperTradingRunResult") -> RealtimeMonitoringPayload:
    equity = float(result.state.equity)
    gross_exposure = float(sum(abs(position.market_value) for position in result.state.positions.values()) / equity) if equity > 0 else 0.0
    net_exposure = float(sum(position.market_value for position in result.state.positions.values()) / equity) if equity > 0 else 0.0
    baseline_equity = float(result.state.initial_cash_basis or equity or 0.0)
    current_drawdown = float(max((baseline_equity - equity) / baseline_equity, 0.0)) if baseline_equity > 0 else 0.0
    accounting = dict(result.diagnostics.get("accounting", {}))
    realized_pnl = float(accounting.get("realized_pnl_delta", result.state.cumulative_realized_pnl) or 0.0)
    expected_net_total = float(
        sum(
            float(decision.expected_value_net or 0.0)
            for decision in result.trade_decision_contracts
            if not getattr(decision, "vetoed", False)
        )
    )
    requested_order_count = len(result.requested_orders or result.orders)
    executable_order_count = len(result.orders)
    fill_count = len(result.fills)
    fill_rate = float(fill_count / requested_order_count) if requested_order_count > 0 else 0.0
    partial_fill_ratio = 0.0
    if result.execution_simulation_report is not None and requested_order_count > 0:
        partial_fill_ratio = float(
            (result.execution_simulation_report.summary.get("partial_fill_order_count", 0) or 0) / requested_order_count
        )
    avg_fill_slippage_bps = (
        float(sum(fill.slippage_bps for fill in result.fills) / fill_count)
        if fill_count > 0
        else _safe_float(
            result.execution_simulation_report.summary.get("estimated_cost_bps_on_executed_notional")
            if result.execution_simulation_report is not None
            else None
        )
        or 0.0
    )
    total_execution_cost = float(result.state.cumulative_execution_cost or 0.0)
    metrics = [
        RealtimeMonitoringMetric(result.as_of, "equity", equity, unit="usd"),
        RealtimeMonitoringMetric(result.as_of, "total_pnl", float(result.state.total_pnl), unit="usd"),
        RealtimeMonitoringMetric(result.as_of, "realized_pnl", realized_pnl, unit="usd"),
        RealtimeMonitoringMetric(result.as_of, "unrealized_pnl", float(result.state.unrealized_pnl), unit="usd"),
        RealtimeMonitoringMetric(result.as_of, "gross_exposure", gross_exposure, unit="ratio"),
        RealtimeMonitoringMetric(result.as_of, "net_exposure", net_exposure, unit="ratio"),
        RealtimeMonitoringMetric(
            result.as_of,
            "drawdown",
            current_drawdown,
            unit="ratio",
            metadata={"basis": "initial_cash_basis"},
        ),
        RealtimeMonitoringMetric(result.as_of, "expected_value_net_total", expected_net_total, unit="ratio"),
        RealtimeMonitoringMetric(
            result.as_of,
            "realized_vs_expected_gap",
            float(realized_pnl - expected_net_total),
            unit="scalar",
            metadata={"realized_basis": "usd", "expected_basis": "net_expected_return_sum"},
        ),
        RealtimeMonitoringMetric(result.as_of, "fill_rate", fill_rate, unit="ratio"),
        RealtimeMonitoringMetric(result.as_of, "partial_fill_ratio", partial_fill_ratio, unit="ratio"),
        RealtimeMonitoringMetric(result.as_of, "average_fill_slippage_bps", avg_fill_slippage_bps, unit="bps"),
        RealtimeMonitoringMetric(result.as_of, "total_execution_cost", total_execution_cost, unit="usd"),
        RealtimeMonitoringMetric(result.as_of, "requested_order_count", float(requested_order_count), unit="count"),
        RealtimeMonitoringMetric(result.as_of, "executable_order_count", float(executable_order_count), unit="count"),
    ]
    return RealtimeMonitoringPayload(
        as_of=result.as_of,
        metrics=metrics,
        summary={
            "metric_count": len(metrics),
            "fill_count": fill_count,
            "requested_order_count": requested_order_count,
            "executable_order_count": executable_order_count,
            "fill_quality_available": bool(fill_count > 0 or result.execution_simulation_report is not None),
            "execution_simulation_enabled": result.execution_simulation_report is not None,
        },
    )


def write_realtime_monitoring_artifacts(
    *,
    output_dir: str | Path,
    payload: RealtimeMonitoringPayload,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "realtime_kpi_monitoring.json"
    csv_path = output_path / "realtime_kpi_monitoring.csv"
    json_path.write_text(json.dumps(payload.to_dict(), indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in payload.metrics]).to_csv(csv_path, index=False)
    return {"realtime_kpi_monitoring_json_path": json_path, "realtime_kpi_monitoring_csv_path": csv_path}
