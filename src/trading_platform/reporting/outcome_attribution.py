from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult


TRADE_OUTCOME_ATTRIBUTION_SCHEMA_VERSION = "trade_outcome_attribution_v1"
AGGREGATION_DIMENSIONS = {"strategy", "instrument", "regime", "confidence_bucket", "horizon"}


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


def _flat_dict(value: dict[str, Any]) -> str:
    return "|".join(f"{key}={value[key]}" for key in sorted(value) if value[key] not in (None, "", [], {}))


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


def _confidence_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.4:
        return "low"
    if value < 0.7:
        return "medium"
    return "high"


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _trade_return(pnl: float | None, entry_notional: float | None) -> float | None:
    if pnl is None or entry_notional is None or abs(entry_notional) <= 1e-12:
        return None
    return float(pnl / entry_notional)


def _entry_notional(row: dict[str, Any]) -> float | None:
    qty = _safe_float(row.get("quantity"))
    entry_reference_price = _safe_float(row.get("entry_reference_price"))
    if qty is None or entry_reference_price is None:
        return None
    return float(abs(qty * entry_reference_price))


@dataclass(frozen=True)
class TradeOutcome:
    trade_id: str
    decision_id: str | None
    strategy_id: str
    instrument: str
    entry_date: str | None
    exit_date: str | None
    side: str | None
    quantity: int
    horizon_days: int | None
    holding_period_days: int | None
    regime_label: str | None
    confidence_bucket: str
    probability_positive: float | None = None
    confidence_score: float | None = None
    reliability_score: float | None = None
    calibration_score: float | None = None
    predicted_return: float | None = None
    predicted_gross_return: float | None = None
    predicted_cost: float | None = None
    predicted_net_return: float | None = None
    realized_gross_return: float | None = None
    realized_cost: float | None = None
    realized_net_return: float | None = None
    realized_gross_pnl: float | None = None
    realized_cost_total: float | None = None
    realized_net_pnl: float | None = None
    entry_reference_price: float | None = None
    entry_price: float | None = None
    exit_reference_price: float | None = None
    exit_price: float | None = None
    status: str = "closed"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "decision_id": self.decision_id,
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "entry_date": self.entry_date,
            "exit_date": self.exit_date,
            "side": self.side,
            "quantity": int(self.quantity),
            "horizon_days": self.horizon_days,
            "holding_period_days": self.holding_period_days,
            "regime_label": self.regime_label,
            "confidence_bucket": self.confidence_bucket,
            "probability_positive": self.probability_positive,
            "confidence_score": self.confidence_score,
            "reliability_score": self.reliability_score,
            "calibration_score": self.calibration_score,
            "predicted_return": self.predicted_return,
            "predicted_gross_return": self.predicted_gross_return,
            "predicted_cost": self.predicted_cost,
            "predicted_net_return": self.predicted_net_return,
            "realized_gross_return": self.realized_gross_return,
            "realized_cost": self.realized_cost,
            "realized_net_return": self.realized_net_return,
            "realized_gross_pnl": self.realized_gross_pnl,
            "realized_cost_total": self.realized_cost_total,
            "realized_net_pnl": self.realized_net_pnl,
            "entry_reference_price": self.entry_reference_price,
            "entry_price": self.entry_price,
            "exit_reference_price": self.exit_reference_price,
            "exit_price": self.exit_price,
            "status": self.status,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeOutcome":
        data = dict(payload or {})
        return cls(
            trade_id=str(data["trade_id"]),
            decision_id=str(data["decision_id"]) if data.get("decision_id") is not None else None,
            strategy_id=str(data["strategy_id"]),
            instrument=str(data["instrument"]),
            entry_date=str(data["entry_date"]) if data.get("entry_date") is not None else None,
            exit_date=str(data["exit_date"]) if data.get("exit_date") is not None else None,
            side=str(data["side"]) if data.get("side") is not None else None,
            quantity=int(data.get("quantity", 0) or 0),
            horizon_days=int(data["horizon_days"]) if data.get("horizon_days") is not None else None,
            holding_period_days=int(data["holding_period_days"]) if data.get("holding_period_days") is not None else None,
            regime_label=str(data["regime_label"]) if data.get("regime_label") is not None else None,
            confidence_bucket=str(data.get("confidence_bucket", "unknown")),
            probability_positive=_safe_float(data.get("probability_positive")),
            confidence_score=_safe_float(data.get("confidence_score")),
            reliability_score=_safe_float(data.get("reliability_score")),
            calibration_score=_safe_float(data.get("calibration_score")),
            predicted_return=_safe_float(data.get("predicted_return")),
            predicted_gross_return=_safe_float(data.get("predicted_gross_return")),
            predicted_cost=_safe_float(data.get("predicted_cost")),
            predicted_net_return=_safe_float(data.get("predicted_net_return")),
            realized_gross_return=_safe_float(data.get("realized_gross_return")),
            realized_cost=_safe_float(data.get("realized_cost")),
            realized_net_return=_safe_float(data.get("realized_net_return")),
            realized_gross_pnl=_safe_float(data.get("realized_gross_pnl")),
            realized_cost_total=_safe_float(data.get("realized_cost_total")),
            realized_net_pnl=_safe_float(data.get("realized_net_pnl")),
            entry_reference_price=_safe_float(data.get("entry_reference_price")),
            entry_price=_safe_float(data.get("entry_price")),
            exit_reference_price=_safe_float(data.get("exit_reference_price")),
            exit_price=_safe_float(data.get("exit_price")),
            status=str(data.get("status", "closed")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class TradeAttribution:
    trade_id: str
    decision_id: str | None
    strategy_id: str
    instrument: str
    as_of: str
    forecast_gap: float | None
    alpha_error: float | None
    cost_error: float | None
    timing_error: float | None
    execution_error: float | None
    sizing_error: float | None
    regime_mismatch: bool | None
    regime_mismatch_score: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "decision_id": self.decision_id,
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "as_of": self.as_of,
            "forecast_gap": self.forecast_gap,
            "alpha_error": self.alpha_error,
            "cost_error": self.cost_error,
            "timing_error": self.timing_error,
            "execution_error": self.execution_error,
            "sizing_error": self.sizing_error,
            "regime_mismatch": self.regime_mismatch,
            "regime_mismatch_score": self.regime_mismatch_score,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeAttribution":
        data = dict(payload or {})
        return cls(
            trade_id=str(data["trade_id"]),
            decision_id=str(data["decision_id"]) if data.get("decision_id") is not None else None,
            strategy_id=str(data["strategy_id"]),
            instrument=str(data["instrument"]),
            as_of=str(data["as_of"]),
            forecast_gap=_safe_float(data.get("forecast_gap")),
            alpha_error=_safe_float(data.get("alpha_error")),
            cost_error=_safe_float(data.get("cost_error")),
            timing_error=_safe_float(data.get("timing_error")),
            execution_error=_safe_float(data.get("execution_error")),
            sizing_error=_safe_float(data.get("sizing_error")),
            regime_mismatch=bool(data["regime_mismatch"]) if data.get("regime_mismatch") is not None else None,
            regime_mismatch_score=_safe_float(data.get("regime_mismatch_score")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class TradeAttributionAggregate:
    group_type: str
    group_key: str
    trade_count: int
    win_rate: float | None
    mean_predicted_net_return: float | None
    mean_realized_net_return: float | None
    mean_forecast_gap: float | None
    mean_alpha_error: float | None
    mean_cost_error: float | None
    mean_execution_error: float | None
    total_realized_net_pnl: float
    total_realized_cost: float
    regime_mismatch_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.group_type not in AGGREGATION_DIMENSIONS:
            raise ValueError(f"Unsupported aggregation group_type: {self.group_type}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_type": self.group_type,
            "group_key": self.group_key,
            "trade_count": int(self.trade_count),
            "win_rate": self.win_rate,
            "mean_predicted_net_return": self.mean_predicted_net_return,
            "mean_realized_net_return": self.mean_realized_net_return,
            "mean_forecast_gap": self.mean_forecast_gap,
            "mean_alpha_error": self.mean_alpha_error,
            "mean_cost_error": self.mean_cost_error,
            "mean_execution_error": self.mean_execution_error,
            "total_realized_net_pnl": float(self.total_realized_net_pnl),
            "total_realized_cost": float(self.total_realized_cost),
            "regime_mismatch_count": int(self.regime_mismatch_count),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeAttributionAggregate":
        data = dict(payload or {})
        return cls(
            group_type=str(data["group_type"]),
            group_key=str(data["group_key"]),
            trade_count=int(data.get("trade_count", 0) or 0),
            win_rate=_safe_float(data.get("win_rate")),
            mean_predicted_net_return=_safe_float(data.get("mean_predicted_net_return")),
            mean_realized_net_return=_safe_float(data.get("mean_realized_net_return")),
            mean_forecast_gap=_safe_float(data.get("mean_forecast_gap")),
            mean_alpha_error=_safe_float(data.get("mean_alpha_error")),
            mean_cost_error=_safe_float(data.get("mean_cost_error")),
            mean_execution_error=_safe_float(data.get("mean_execution_error")),
            total_realized_net_pnl=float(data.get("total_realized_net_pnl", 0.0) or 0.0),
            total_realized_cost=float(data.get("total_realized_cost", 0.0) or 0.0),
            regime_mismatch_count=int(data.get("regime_mismatch_count", 0) or 0),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class TradeOutcomeAttributionReport:
    as_of: str
    schema_version: str = TRADE_OUTCOME_ATTRIBUTION_SCHEMA_VERSION
    outcomes: list[TradeOutcome] = field(default_factory=list)
    attributions: list[TradeAttribution] = field(default_factory=list)
    aggregates: list[TradeAttributionAggregate] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "outcomes": [row.to_dict() for row in self.outcomes],
            "attributions": [row.to_dict() for row in self.attributions],
            "aggregates": [row.to_dict() for row in self.aggregates],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeOutcomeAttributionReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", TRADE_OUTCOME_ATTRIBUTION_SCHEMA_VERSION)),
            outcomes=[TradeOutcome.from_dict(row) for row in data.get("outcomes", [])],
            attributions=[TradeAttribution.from_dict(row) for row in data.get("attributions", [])],
            aggregates=[TradeAttributionAggregate.from_dict(row) for row in data.get("aggregates", [])],
            summary=dict(data.get("summary") or {}),
        )


def _decision_lookup(decisions: list[Any]) -> dict[tuple[str, str, str], Any]:
    lookup: dict[tuple[str, str, str], Any] = {}
    for row in decisions:
        key = (str(row.timestamp or ""), str(row.strategy_id or ""), str(row.instrument or ""))
        lookup[key] = row
    return lookup


def _build_outcome_and_attribution(
    *,
    as_of: str,
    trade_row: dict[str, Any],
    decision_lookup: dict[tuple[str, str, str], Any],
) -> tuple[TradeOutcome, TradeAttribution]:
    strategy_id = str(trade_row.get("strategy_id") or "unknown_strategy")
    instrument = str(trade_row.get("symbol") or "")
    decision_key = (str(trade_row.get("entry_decision_timestamp") or trade_row.get("entry_date") or ""), strategy_id, instrument)
    decision = decision_lookup.get(decision_key)

    predicted_return = _safe_float(trade_row.get("predicted_return"))
    predicted_gross_return = _safe_float(trade_row.get("predicted_gross_return", trade_row.get("expected_gross_return")))
    predicted_cost = _safe_float(trade_row.get("predicted_cost", trade_row.get("expected_cost")))
    predicted_net_return = _safe_float(trade_row.get("predicted_net_return", trade_row.get("expected_net_return")))
    probability_positive = _safe_float(trade_row.get("probability_positive"))
    confidence_score = _safe_float(trade_row.get("confidence_score"))
    reliability_score = _safe_float(trade_row.get("reliability_score"))
    calibration_score = _safe_float(trade_row.get("calibration_score"))
    regime_label = str(trade_row["regime_label"]) if trade_row.get("regime_label") is not None else None
    horizon_days = int(trade_row["expected_horizon_days"]) if trade_row.get("expected_horizon_days") is not None else None
    decision_id = str(trade_row["entry_decision_id"]) if trade_row.get("entry_decision_id") is not None else None

    if decision is not None:
        predicted_return = predicted_return if predicted_return is not None else _safe_float(getattr(decision, "predicted_return", None))
        predicted_gross_return = predicted_gross_return if predicted_gross_return is not None else _safe_float(getattr(decision, "expected_value_gross", None))
        predicted_cost = predicted_cost if predicted_cost is not None else _safe_float(getattr(decision, "expected_cost", None))
        predicted_net_return = predicted_net_return if predicted_net_return is not None else _safe_float(getattr(decision, "expected_value_net", None))
        probability_positive = probability_positive if probability_positive is not None else _safe_float(getattr(decision, "probability_positive", None))
        confidence_score = confidence_score if confidence_score is not None else _safe_float(getattr(decision, "confidence_score", None))
        reliability_score = reliability_score if reliability_score is not None else _safe_float(getattr(decision, "reliability_score", None))
        calibration_score = calibration_score if calibration_score is not None else _safe_float(getattr(decision, "calibration_score", None))
        regime_label = regime_label or getattr(decision, "regime_label", None)
        horizon_days = horizon_days if horizon_days is not None else int(getattr(decision, "horizon_days", 0) or 0) or None
        decision_id = decision_id or getattr(decision, "decision_id", None)

    entry_notional = _entry_notional(trade_row)
    realized_gross_pnl = _safe_float(trade_row.get("gross_realized_pnl"))
    realized_net_pnl = _safe_float(trade_row.get("net_realized_pnl", trade_row.get("realized_pnl")))
    realized_cost_total = _safe_float(trade_row.get("total_execution_cost"))
    realized_gross_return = _trade_return(realized_gross_pnl, entry_notional)
    realized_cost = _trade_return(realized_cost_total, entry_notional)
    realized_net_return = _trade_return(realized_net_pnl, entry_notional)

    reference_entry = _safe_float(trade_row.get("entry_reference_price"))
    reference_exit = _safe_float(trade_row.get("exit_reference_price"))
    actual_entry = _safe_float(trade_row.get("entry_price"))
    actual_exit = _safe_float(trade_row.get("exit_price"))
    execution_error = None
    if None not in (reference_entry, reference_exit, actual_entry, actual_exit) and abs(reference_entry or 0.0) > 1e-12:
        side_sign = 1.0 if str(trade_row.get("side") or "long").lower() == "long" else -1.0
        execution_error = float((((actual_exit - reference_exit) - (actual_entry - reference_entry)) * side_sign) / reference_entry)

    timing_error = None
    holding_period_days = int(trade_row["holding_period_days"]) if trade_row.get("holding_period_days") is not None else None
    if horizon_days is not None and holding_period_days is not None and horizon_days > 0 and predicted_gross_return is not None:
        timing_error = float(((holding_period_days - horizon_days) / horizon_days) * predicted_gross_return)

    sizing_error = _safe_float(trade_row.get("sizing_error"))
    if sizing_error is None and trade_row.get("target_weight_entry") is not None and trade_row.get("target_weight_exit") is not None:
        sizing_error = float(_safe_float(trade_row.get("target_weight_exit")) or 0.0) - float(_safe_float(trade_row.get("target_weight_entry")) or 0.0)

    exit_regime_label = str(trade_row["exit_regime_label"]) if trade_row.get("exit_regime_label") is not None else None
    regime_mismatch = None
    regime_mismatch_score = None
    if regime_label is not None and exit_regime_label is not None:
        regime_mismatch = regime_label != exit_regime_label
        regime_mismatch_score = 1.0 if regime_mismatch else 0.0

    forecast_gap = (
        float(realized_net_return - predicted_net_return)
        if realized_net_return is not None and predicted_net_return is not None
        else None
    )
    alpha_error = (
        float(realized_gross_return - predicted_gross_return)
        if realized_gross_return is not None and predicted_gross_return is not None
        else None
    )
    cost_error = (
        float(predicted_cost - realized_cost)
        if predicted_cost is not None and realized_cost is not None
        else None
    )

    outcome = TradeOutcome(
        trade_id=str(trade_row.get("trade_id") or ""),
        decision_id=str(decision_id) if decision_id is not None else None,
        strategy_id=strategy_id,
        instrument=instrument,
        entry_date=str(trade_row["entry_date"]) if trade_row.get("entry_date") is not None else None,
        exit_date=str(trade_row["exit_date"]) if trade_row.get("exit_date") is not None else None,
        side=str(trade_row["side"]) if trade_row.get("side") is not None else None,
        quantity=int(trade_row.get("quantity", 0) or 0),
        horizon_days=horizon_days,
        holding_period_days=holding_period_days,
        regime_label=regime_label,
        confidence_bucket=_confidence_bucket(confidence_score),
        probability_positive=probability_positive,
        confidence_score=confidence_score,
        reliability_score=reliability_score,
        calibration_score=calibration_score,
        predicted_return=predicted_return,
        predicted_gross_return=predicted_gross_return,
        predicted_cost=predicted_cost,
        predicted_net_return=predicted_net_return,
        realized_gross_return=realized_gross_return,
        realized_cost=realized_cost,
        realized_net_return=realized_net_return,
        realized_gross_pnl=realized_gross_pnl,
        realized_cost_total=realized_cost_total,
        realized_net_pnl=realized_net_pnl,
        entry_reference_price=reference_entry,
        entry_price=actual_entry,
        exit_reference_price=reference_exit,
        exit_price=actual_exit,
        status=str(trade_row.get("status") or "closed"),
        metadata={
            "entry_reason": trade_row.get("entry_reason"),
            "exit_reason": trade_row.get("exit_reason"),
            "score_entry": trade_row.get("score_entry"),
            "score_exit": trade_row.get("score_exit"),
            "score_percentile_entry": trade_row.get("score_percentile_entry"),
            "score_percentile_exit": trade_row.get("score_percentile_exit"),
        },
    )
    attribution = TradeAttribution(
        trade_id=outcome.trade_id,
        decision_id=outcome.decision_id,
        strategy_id=outcome.strategy_id,
        instrument=outcome.instrument,
        as_of=as_of,
        forecast_gap=forecast_gap,
        alpha_error=alpha_error,
        cost_error=cost_error,
        timing_error=timing_error,
        execution_error=execution_error,
        sizing_error=sizing_error,
        regime_mismatch=regime_mismatch,
        regime_mismatch_score=regime_mismatch_score,
        metadata={
            "holding_period_days": holding_period_days,
            "horizon_days": horizon_days,
            "entry_notional": entry_notional,
        },
    )
    return outcome, attribution


def _aggregate_rows(
    *,
    group_type: str,
    outcomes: list[TradeOutcome],
    attributions: list[TradeAttribution],
) -> list[TradeAttributionAggregate]:
    keyed_outcomes: dict[str, TradeOutcome] = {row.trade_id: row for row in outcomes}
    keyed_attributions: dict[str, TradeAttribution] = {row.trade_id: row for row in attributions}
    buckets: dict[str, list[str]] = {}
    for outcome in outcomes:
        if group_type == "strategy":
            key = outcome.strategy_id
        elif group_type == "instrument":
            key = outcome.instrument
        elif group_type == "regime":
            key = outcome.regime_label or "unknown"
        elif group_type == "confidence_bucket":
            key = outcome.confidence_bucket
        else:
            key = str(outcome.horizon_days) if outcome.horizon_days is not None else "unknown"
        buckets.setdefault(str(key), []).append(outcome.trade_id)

    aggregates: list[TradeAttributionAggregate] = []
    for group_key, trade_ids in sorted(buckets.items()):
        bucket_outcomes = [keyed_outcomes[trade_id] for trade_id in trade_ids]
        bucket_attributions = [keyed_attributions[trade_id] for trade_id in trade_ids]
        realized_net_returns = [row.realized_net_return for row in bucket_outcomes]
        predicted_net_returns = [row.predicted_net_return for row in bucket_outcomes]
        forecast_gaps = [row.forecast_gap for row in bucket_attributions]
        alpha_errors = [row.alpha_error for row in bucket_attributions]
        cost_errors = [row.cost_error for row in bucket_attributions]
        execution_errors = [row.execution_error for row in bucket_attributions]
        winning = [row for row in bucket_outcomes if (row.realized_net_pnl or 0.0) > 0.0]
        aggregates.append(
            TradeAttributionAggregate(
                group_type=group_type,
                group_key=group_key,
                trade_count=len(trade_ids),
                win_rate=(len(winning) / len(trade_ids)) if trade_ids else None,
                mean_predicted_net_return=_mean(predicted_net_returns),
                mean_realized_net_return=_mean(realized_net_returns),
                mean_forecast_gap=_mean(forecast_gaps),
                mean_alpha_error=_mean(alpha_errors),
                mean_cost_error=_mean(cost_errors),
                mean_execution_error=_mean(execution_errors),
                total_realized_net_pnl=float(sum(float(row.realized_net_pnl or 0.0) for row in bucket_outcomes)),
                total_realized_cost=float(sum(float(row.realized_cost_total or 0.0) for row in bucket_outcomes)),
                regime_mismatch_count=sum(1 for row in bucket_attributions if row.regime_mismatch is True),
                metadata={"trade_ids": trade_ids},
            )
        )
    return aggregates


def build_trade_outcome_attribution_report(*, result: "PaperTradingRunResult") -> TradeOutcomeAttributionReport:
    trade_rows = [dict(row) for row in list(result.attribution.get("trade_rows", [])) if str(row.get("status") or "") == "closed"]
    decision_lookup = _decision_lookup(list(result.trade_decision_contracts))
    outcomes: list[TradeOutcome] = []
    attributions: list[TradeAttribution] = []
    for row in sorted(trade_rows, key=lambda item: (str(item.get("trade_id") or ""), str(item.get("strategy_id") or ""), str(item.get("symbol") or ""))):
        outcome, attribution = _build_outcome_and_attribution(as_of=result.as_of, trade_row=row, decision_lookup=decision_lookup)
        outcomes.append(outcome)
        attributions.append(attribution)

    aggregates: list[TradeAttributionAggregate] = []
    for group_type in sorted(AGGREGATION_DIMENSIONS):
        aggregates.extend(_aggregate_rows(group_type=group_type, outcomes=outcomes, attributions=attributions))

    summary = {
        "outcome_count": len(outcomes),
        "closed_trade_count": len(outcomes),
        "mean_predicted_net_return": _mean([row.predicted_net_return for row in outcomes]),
        "mean_realized_net_return": _mean([row.realized_net_return for row in outcomes]),
        "mean_forecast_gap": _mean([row.forecast_gap for row in attributions]),
        "mean_alpha_error": _mean([row.alpha_error for row in attributions]),
        "mean_cost_error": _mean([row.cost_error for row in attributions]),
        "mean_execution_error": _mean([row.execution_error for row in attributions]),
        "total_realized_net_pnl": float(sum(float(row.realized_net_pnl or 0.0) for row in outcomes)),
        "total_realized_cost": float(sum(float(row.realized_cost_total or 0.0) for row in outcomes)),
        "regime_mismatch_count": sum(1 for row in attributions if row.regime_mismatch is True),
        "aggregation_group_count": len(aggregates),
    }
    return TradeOutcomeAttributionReport(
        as_of=result.as_of,
        outcomes=outcomes,
        attributions=attributions,
        aggregates=aggregates,
        summary=summary,
    )


def write_trade_outcome_attribution_artifacts(
    *,
    output_dir: str | Path,
    report: TradeOutcomeAttributionReport,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_json_path = output_path / "trade_outcome_attribution_report.json"
    outcomes_csv_path = output_path / "trade_outcomes.csv"
    attributions_csv_path = output_path / "trade_outcome_attribution.csv"
    aggregates_csv_path = output_path / "trade_outcome_aggregates.csv"
    summary_json_path = output_path / "trade_outcome_attribution_summary.json"
    report_json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    summary_json_path.write_text(json.dumps(report.summary, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in report.outcomes]).to_csv(outcomes_csv_path, index=False)
    pd.DataFrame([row.flat_dict() for row in report.attributions]).to_csv(attributions_csv_path, index=False)
    pd.DataFrame([row.flat_dict() for row in report.aggregates]).to_csv(aggregates_csv_path, index=False)
    return {
        "trade_outcome_attribution_report_json_path": report_json_path,
        "trade_outcomes_csv_path": outcomes_csv_path,
        "trade_outcome_attribution_csv_path": attributions_csv_path,
        "trade_outcome_aggregates_csv_path": aggregates_csv_path,
        "trade_outcome_attribution_summary_json_path": summary_json_path,
    }
