from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import PaperOrder, PaperTradingConfig


TRANSACTION_COST_SCHEMA_VERSION = "transaction_cost_report_v1"


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


@dataclass(frozen=True)
class TransactionCostModelConfig:
    cost_model: str
    slippage_model: str
    slippage_buy_bps: float
    slippage_sell_bps: float
    commission_bps: float
    minimum_commission: float
    spread_bps: float
    cost_model_enabled: bool

    @classmethod
    def from_paper_trading_config(cls, config: PaperTradingConfig) -> "TransactionCostModelConfig":
        return cls(
            cost_model="paper_v2_cost_model" if bool(config.enable_cost_model) else "disabled",
            slippage_model=str(config.slippage_model or "none"),
            slippage_buy_bps=float(config.slippage_buy_bps),
            slippage_sell_bps=float(config.slippage_sell_bps),
            commission_bps=float(config.commission_bps),
            minimum_commission=float(config.minimum_commission),
            spread_bps=float(config.spread_bps),
            cost_model_enabled=bool(config.enable_cost_model),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "cost_model": self.cost_model,
            "slippage_model": self.slippage_model,
            "slippage_buy_bps": float(self.slippage_buy_bps),
            "slippage_sell_bps": float(self.slippage_sell_bps),
            "commission_bps": float(self.commission_bps),
            "minimum_commission": float(self.minimum_commission),
            "spread_bps": float(self.spread_bps),
            "cost_model_enabled": bool(self.cost_model_enabled),
        }


@dataclass(frozen=True)
class TransactionCostRecord:
    as_of: str
    symbol: str
    side: str
    quantity: int
    stage: str
    gross_notional: float
    net_notional: float
    slippage_bps: float = 0.0
    spread_bps: float = 0.0
    slippage_cost: float = 0.0
    spread_cost: float = 0.0
    commission_cost: float = 0.0
    total_cost: float = 0.0
    cost_model: str = "disabled"
    trade_id: str | None = None
    strategy_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": int(self.quantity),
            "stage": self.stage,
            "gross_notional": float(self.gross_notional),
            "net_notional": float(self.net_notional),
            "slippage_bps": float(self.slippage_bps),
            "spread_bps": float(self.spread_bps),
            "slippage_cost": float(self.slippage_cost),
            "spread_cost": float(self.spread_cost),
            "commission_cost": float(self.commission_cost),
            "total_cost": float(self.total_cost),
            "cost_model": self.cost_model,
            "trade_id": self.trade_id,
            "strategy_id": self.strategy_id,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = "|".join(
            f"{key}={payload['metadata'][key]}"
            for key in sorted(payload["metadata"])
            if payload["metadata"][key] not in (None, "", [], {})
        )
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TransactionCostRecord":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            quantity=int(data.get("quantity", 0) or 0),
            stage=str(data.get("stage", "estimate") or "estimate"),
            gross_notional=float(data.get("gross_notional", 0.0) or 0.0),
            net_notional=float(data.get("net_notional", 0.0) or 0.0),
            slippage_bps=float(data.get("slippage_bps", 0.0) or 0.0),
            spread_bps=float(data.get("spread_bps", 0.0) or 0.0),
            slippage_cost=float(data.get("slippage_cost", 0.0) or 0.0),
            spread_cost=float(data.get("spread_cost", 0.0) or 0.0),
            commission_cost=float(data.get("commission_cost", 0.0) or 0.0),
            total_cost=float(data.get("total_cost", 0.0) or 0.0),
            cost_model=str(data.get("cost_model", "disabled") or "disabled"),
            trade_id=str(data["trade_id"]) if data.get("trade_id") is not None else None,
            strategy_id=str(data["strategy_id"]) if data.get("strategy_id") is not None else None,
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class TransactionCostReport:
    as_of: str
    model_config: TransactionCostModelConfig
    schema_version: str = TRANSACTION_COST_SCHEMA_VERSION
    records: list[TransactionCostRecord] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "model_config": self.model_config.to_dict(),
            "records": [row.to_dict() for row in self.records],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TransactionCostReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", TRANSACTION_COST_SCHEMA_VERSION)),
            model_config=TransactionCostModelConfig(**dict(data.get("model_config") or {})),
            records=[TransactionCostRecord.from_dict(row) for row in data.get("records", [])],
            summary=dict(data.get("summary") or {}),
        )


def build_transaction_cost_report(
    *,
    as_of: str,
    config: PaperTradingConfig,
    orders: list[PaperOrder],
    fills: list[BrokerFill],
) -> TransactionCostReport:
    records: list[TransactionCostRecord] = []
    for order in sorted(orders, key=lambda row: (row.symbol, row.side, row.quantity)):
        records.append(
            TransactionCostRecord(
                as_of=as_of,
                symbol=order.symbol,
                side=order.side,
                quantity=int(order.quantity),
                stage="estimate",
                gross_notional=float(order.expected_gross_notional or (float(order.reference_price) * float(order.quantity))),
                net_notional=float(order.notional),
                slippage_bps=float(order.expected_slippage_bps),
                spread_bps=float(order.expected_spread_bps),
                slippage_cost=float(order.expected_slippage_cost),
                spread_cost=float(order.expected_spread_cost),
                commission_cost=float(order.expected_commission_cost),
                total_cost=float(order.expected_total_execution_cost),
                cost_model=str(order.cost_model),
                strategy_id=str((order.provenance or {}).get("strategy_id")) if (order.provenance or {}).get("strategy_id") is not None else None,
                metadata={"source": "paper_order"},
            )
        )
    for fill in sorted(fills, key=lambda row: (row.symbol, row.side, row.quantity, row.trade_id or "")):
        records.append(
            TransactionCostRecord(
                as_of=as_of,
                symbol=fill.symbol,
                side=fill.side,
                quantity=int(fill.quantity),
                stage="realized",
                gross_notional=float(fill.gross_notional or float(fill.reference_price) * float(fill.quantity)),
                net_notional=float(fill.notional),
                slippage_bps=float(fill.slippage_bps),
                spread_bps=float(fill.spread_bps),
                slippage_cost=float(fill.slippage_cost),
                spread_cost=float(fill.spread_cost),
                commission_cost=float(fill.commission),
                total_cost=float(fill.total_execution_cost),
                cost_model=str(fill.cost_model),
                trade_id=fill.trade_id,
                strategy_id=fill.strategy_id,
                metadata={"source": "broker_fill"},
            )
        )
    estimate_rows = [row for row in records if row.stage == "estimate"]
    realized_rows = [row for row in records if row.stage == "realized"]
    return TransactionCostReport(
        as_of=as_of,
        model_config=TransactionCostModelConfig.from_paper_trading_config(config),
        records=records,
        summary={
            "record_count": len(records),
            "estimate_count": len(estimate_rows),
            "realized_count": len(realized_rows),
            "estimated_total_cost": float(sum(row.total_cost for row in estimate_rows)),
            "realized_total_cost": float(sum(row.total_cost for row in realized_rows)),
            "estimated_slippage_cost": float(sum(row.slippage_cost for row in estimate_rows)),
            "realized_slippage_cost": float(sum(row.slippage_cost for row in realized_rows)),
            "estimated_commission_cost": float(sum(row.commission_cost for row in estimate_rows)),
            "realized_commission_cost": float(sum(row.commission_cost for row in realized_rows)),
        },
    )


def write_transaction_cost_artifacts(
    *,
    output_dir: str | Path,
    report: TransactionCostReport,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "transaction_cost_report.json"
    csv_path = output_path / "transaction_cost_records.csv"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in report.records]).to_csv(csv_path, index=False)
    return {"transaction_cost_report_json_path": json_path, "transaction_cost_records_csv_path": csv_path}
