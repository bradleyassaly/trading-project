from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import PaperExecutionSimulationReport, PaperOrder


def _normalize_metadata(value: dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    return {str(key): value[key] for key in sorted(value)}


ORDER_STATUSES = {
    "intent",
    "submitted",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected",
}


@dataclass(frozen=True)
class OrderIntent:
    order_id: str
    symbol: str
    side: str
    quantity: int
    reference_price: float
    target_weight: float
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.order_id or "").strip():
            raise ValueError("order_id must be a non-empty string")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderIntent":
        data = dict(payload or {})
        data.setdefault("metadata", {})
        return cls(
            order_id=str(data["order_id"]),
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            quantity=int(data["quantity"]),
            reference_price=float(data["reference_price"]),
            target_weight=float(data["target_weight"]),
            reason=str(data["reason"]),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": int(self.quantity),
            "reference_price": float(self.reference_price),
            "target_weight": float(self.target_weight),
            "reason": self.reason,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class SubmittedOrder:
    order_id: str
    symbol: str
    side: str
    quantity: int
    order_type: str
    time_in_force: str
    status: str
    submitted_at: str | None = None
    broker_order_id: str | None = None
    client_order_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.order_id or "").strip():
            raise ValueError("order_id must be a non-empty string")
        if self.status not in ORDER_STATUSES:
            raise ValueError(f"Unsupported order lifecycle status: {self.status}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SubmittedOrder":
        data = dict(payload or {})
        data.setdefault("submitted_at", None)
        data.setdefault("broker_order_id", None)
        data.setdefault("client_order_id", None)
        data.setdefault("metadata", {})
        return cls(
            order_id=str(data["order_id"]),
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            quantity=int(data["quantity"]),
            order_type=str(data["order_type"]),
            time_in_force=str(data["time_in_force"]),
            status=str(data["status"]),
            submitted_at=str(data["submitted_at"]) if data.get("submitted_at") is not None else None,
            broker_order_id=str(data["broker_order_id"]) if data.get("broker_order_id") is not None else None,
            client_order_id=str(data["client_order_id"]) if data.get("client_order_id") is not None else None,
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": int(self.quantity),
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "status": self.status,
            "submitted_at": self.submitted_at,
            "broker_order_id": self.broker_order_id,
            "client_order_id": self.client_order_id,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class OrderFillRecord:
    order_id: str
    fill_id: str
    symbol: str
    side: str
    quantity: int
    fill_price: float
    notional: float
    fill_status: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.fill_id or "").strip():
            raise ValueError("fill_id must be a non-empty string")
        if self.fill_status not in {"partial_fill", "filled"}:
            raise ValueError("fill_status must be one of: partial_fill, filled")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderFillRecord":
        data = dict(payload or {})
        data.setdefault("metadata", {})
        return cls(
            order_id=str(data["order_id"]),
            fill_id=str(data["fill_id"]),
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            quantity=int(data["quantity"]),
            fill_price=float(data["fill_price"]),
            notional=float(data["notional"]),
            fill_status=str(data["fill_status"]),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "fill_id": self.fill_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": int(self.quantity),
            "fill_price": float(self.fill_price),
            "notional": float(self.notional),
            "fill_status": self.fill_status,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class OrderCancellation:
    order_id: str
    cancellation_id: str
    symbol: str
    cancelled_quantity: int
    reason_code: str
    cancelled_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.cancellation_id or "").strip():
            raise ValueError("cancellation_id must be a non-empty string")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderCancellation":
        data = dict(payload or {})
        data.setdefault("cancelled_at", None)
        data.setdefault("metadata", {})
        return cls(
            order_id=str(data["order_id"]),
            cancellation_id=str(data["cancellation_id"]),
            symbol=str(data["symbol"]),
            cancelled_quantity=int(data["cancelled_quantity"]),
            reason_code=str(data["reason_code"]),
            cancelled_at=str(data["cancelled_at"]) if data.get("cancelled_at") is not None else None,
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "cancellation_id": self.cancellation_id,
            "symbol": self.symbol,
            "cancelled_quantity": int(self.cancelled_quantity),
            "reason_code": self.reason_code,
            "cancelled_at": self.cancelled_at,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class OrderStatusEvent:
    order_id: str
    event_id: str
    status: str
    event_type: str
    timestamp: str | None = None
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.event_id or "").strip():
            raise ValueError("event_id must be a non-empty string")
        if self.status not in ORDER_STATUSES:
            raise ValueError(f"Unsupported order lifecycle status: {self.status}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderStatusEvent":
        data = dict(payload or {})
        data.setdefault("timestamp", None)
        data.setdefault("message", None)
        data.setdefault("metadata", {})
        return cls(
            order_id=str(data["order_id"]),
            event_id=str(data["event_id"]),
            status=str(data["status"]),
            event_type=str(data["event_type"]),
            timestamp=str(data["timestamp"]) if data.get("timestamp") is not None else None,
            message=str(data["message"]) if data.get("message") is not None else None,
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "event_id": self.event_id,
            "status": self.status,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "message": self.message,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class OrderLifecycleRecord:
    intent: OrderIntent
    submitted_order: SubmittedOrder | None = None
    fills: list[OrderFillRecord] = field(default_factory=list)
    cancellations: list[OrderCancellation] = field(default_factory=list)
    status_events: list[OrderStatusEvent] = field(default_factory=list)
    final_status: str = "intent"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.final_status not in ORDER_STATUSES:
            raise ValueError(f"Unsupported order lifecycle status: {self.final_status}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderLifecycleRecord":
        data = dict(payload or {})
        data.setdefault("submitted_order", None)
        data.setdefault("fills", [])
        data.setdefault("cancellations", [])
        data.setdefault("status_events", [])
        data.setdefault("metadata", {})
        return cls(
            intent=OrderIntent.from_dict(data["intent"]),
            submitted_order=SubmittedOrder.from_dict(data["submitted_order"]) if data.get("submitted_order") else None,
            fills=[OrderFillRecord.from_dict(row) for row in data.get("fills", [])],
            cancellations=[OrderCancellation.from_dict(row) for row in data.get("cancellations", [])],
            status_events=[OrderStatusEvent.from_dict(row) for row in data.get("status_events", [])],
            final_status=str(data["final_status"]),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent.to_dict(),
            "submitted_order": self.submitted_order.to_dict() if self.submitted_order is not None else None,
            "fills": [row.to_dict() for row in self.fills],
            "cancellations": [row.to_dict() for row in self.cancellations],
            "status_events": [row.to_dict() for row in self.status_events],
            "final_status": self.final_status,
            "metadata": dict(self.metadata),
        }


def build_paper_order_lifecycle_records(
    *,
    as_of: str,
    orders: list[PaperOrder],
    fills: list[BrokerFill],
    simulation_report: PaperExecutionSimulationReport | None = None,
) -> list[OrderLifecycleRecord]:
    fill_lookup: dict[tuple[str, str], list[BrokerFill]] = {}
    for fill in fills:
        fill_lookup.setdefault((str(fill.symbol), str(fill.side)), []).append(fill)
    simulation_lookup: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for row in list(getattr(simulation_report, "orders", [])):
        key = (str(row.symbol), str(row.side), int(row.requested_quantity))
        simulation_lookup.setdefault(key, []).append(row.to_dict())

    records: list[OrderLifecycleRecord] = []
    for index, order in enumerate(orders, start=1):
        order_id = f"{as_of}|{order.symbol}|{index}"
        simulation_rows = simulation_lookup.get((str(order.symbol), str(order.side), int(order.quantity)), [])
        simulation_row = simulation_rows.pop(0) if simulation_rows else None
        intent = OrderIntent(
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=int(order.quantity),
            reference_price=float(order.reference_price),
            target_weight=float(order.target_weight),
            reason=str(order.reason),
            metadata=dict(order.provenance),
        )
        submitted = SubmittedOrder(
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=int(order.quantity),
            order_type="market",
            time_in_force="day",
            status="submitted" if fill_lookup.get((order.symbol, order.side)) or simulation_row else "intent",
            submitted_at=as_of,
            metadata={
                "current_quantity": int(order.current_quantity),
                "target_quantity": int(order.target_quantity),
                "submission_delay_seconds": (
                    float(simulation_row.get("submission_delay_seconds", 0.0)) if simulation_row is not None else 0.0
                ),
                "fill_delay_seconds": (
                    float(simulation_row.get("fill_delay_seconds", 0.0)) if simulation_row is not None else 0.0
                ),
            },
        )
        matched_fill_rows = fill_lookup.get((order.symbol, order.side), [])
        matched_fill = matched_fill_rows.pop(0) if matched_fill_rows else None
        fill_records: list[OrderFillRecord] = []
        if matched_fill is not None:
            fill_records.append(
                OrderFillRecord(
                    order_id=order_id,
                    fill_id=f"{order_id}|fill|1",
                    symbol=matched_fill.symbol,
                    side=matched_fill.side,
                    quantity=int(matched_fill.quantity),
                    fill_price=float(matched_fill.fill_price),
                    notional=float(matched_fill.notional),
                    fill_status="filled" if int(matched_fill.quantity) >= int(order.quantity) else "partial_fill",
                    metadata={
                        "trade_id": matched_fill.trade_id,
                        "strategy_id": matched_fill.strategy_id,
                        "slippage_bps": float(matched_fill.slippage_bps),
                        "spread_bps": float(matched_fill.spread_bps),
                        "slippage_cost": float(matched_fill.slippage_cost),
                        "spread_cost": float(matched_fill.spread_cost),
                        "total_execution_cost": float(matched_fill.total_execution_cost),
                    },
                )
            )
        elif simulation_row is not None and int(simulation_row.get("executable_quantity", 0) or 0) > 0:
            simulated_quantity = int(simulation_row.get("executable_quantity", 0) or 0)
            fill_records.append(
                OrderFillRecord(
                    order_id=order_id,
                    fill_id=f"{order_id}|fill|1",
                    symbol=str(simulation_row["symbol"]),
                    side=str(simulation_row["side"]),
                    quantity=simulated_quantity,
                    fill_price=float(simulation_row.get("estimated_fill_price", order.reference_price) or order.reference_price),
                    notional=float(simulation_row.get("executable_notional", 0.0) or 0.0),
                    fill_status="filled" if simulated_quantity >= int(order.quantity) else "partial_fill",
                    metadata={
                        "simulated": True,
                        "slippage_bps": float(simulation_row.get("slippage_bps", 0.0) or 0.0),
                        "spread_bps": float(simulation_row.get("spread_bps", 0.0) or 0.0),
                        "commission": float(simulation_row.get("commission", 0.0) or 0.0),
                        "submission_delay_seconds": float(simulation_row.get("submission_delay_seconds", 0.0) or 0.0),
                        "fill_delay_seconds": float(simulation_row.get("fill_delay_seconds", 0.0) or 0.0),
                    },
                )
            )
        filled_quantity = sum(fill.quantity for fill in fill_records)
        final_status = (
            "filled"
            if filled_quantity >= int(order.quantity) and filled_quantity > 0
            else (
                "partially_filled"
                if filled_quantity > 0
                else ("rejected" if simulation_row is not None and simulation_row.get("status") == "rejected" else "intent")
            )
        )
        status_events = [
            OrderStatusEvent(
                order_id=order_id,
                event_id=f"{order_id}|event|1",
                status="intent",
                event_type="intent_created",
                timestamp=as_of,
            )
        ]
        if simulation_row is not None:
            status_events.append(
                OrderStatusEvent(
                    order_id=order_id,
                    event_id=f"{order_id}|event|2",
                    status=final_status if final_status in {"partially_filled", "filled", "rejected"} else "submitted",
                    event_type="execution_simulated",
                    timestamp=as_of,
                    metadata={
                        "requested_quantity": int(simulation_row.get("requested_quantity", 0) or 0),
                        "executable_quantity": int(simulation_row.get("executable_quantity", 0) or 0),
                        "filled_fraction": float(simulation_row.get("filled_fraction", 0.0) or 0.0),
                        "clipping_reason": simulation_row.get("clipping_reason"),
                        "rejection_reason": simulation_row.get("rejection_reason"),
                    },
                )
            )
        if matched_fill is not None:
            status_events.append(
                OrderStatusEvent(
                    order_id=order_id,
                    event_id=f"{order_id}|event|3",
                    status=final_status,
                    event_type="fill_received",
                    timestamp=as_of,
                    metadata={"fill_count": 1},
                )
            )
        cancellations: list[OrderCancellation] = []
        unfilled_quantity = max(int(order.quantity) - int(filled_quantity), 0)
        if simulation_row is not None and unfilled_quantity > 0:
            cancellations.append(
                OrderCancellation(
                    order_id=order_id,
                    cancellation_id=f"{order_id}|cancel|1",
                    symbol=order.symbol,
                    cancelled_quantity=unfilled_quantity,
                    reason_code=str(
                        simulation_row.get("clipping_reason")
                        or simulation_row.get("rejection_reason")
                        or "simulated_unfilled_quantity"
                    ),
                    cancelled_at=as_of,
                    metadata={
                        "submission_delay_seconds": float(simulation_row.get("submission_delay_seconds", 0.0) or 0.0),
                        "fill_delay_seconds": float(simulation_row.get("fill_delay_seconds", 0.0) or 0.0),
                    },
                )
            )
        records.append(
            OrderLifecycleRecord(
                intent=intent,
                submitted_order=submitted,
                fills=fill_records,
                cancellations=cancellations,
                status_events=status_events,
                final_status=final_status,
                metadata={
                    "filled_quantity": filled_quantity,
                    "requested_quantity": int(order.quantity),
                    "simulated_execution_status": simulation_row.get("status") if simulation_row is not None else None,
                },
            )
        )
    return records
