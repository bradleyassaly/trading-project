from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult
    from trading_platform.reporting.outcome_attribution import TradeOutcome


CALIBRATION_SCHEMA_VERSION = "calibration_pipeline_v1"
CALIBRATION_TYPES = {"confidence", "expected_value"}
CALIBRATION_SCOPES = {"portfolio", "strategy", "regime"}
MIN_SCOPE_SAMPLE_COUNT = 4
MIN_BUCKET_SAMPLE_COUNT = 2
SHRINKAGE_SAMPLE_TARGET = 8.0
CONFIDENCE_BUCKETS: list[tuple[float, float, str]] = [
    (0.0, 0.2, "0.00_to_0.20"),
    (0.2, 0.4, "0.20_to_0.40"),
    (0.4, 0.6, "0.40_to_0.60"),
    (0.6, 0.8, "0.60_to_0.80"),
    (0.8, 1.0000001, "0.80_to_1.00"),
]
EV_BUCKETS: list[tuple[float, float, str]] = [
    (-10.0, -0.02, "lt_-0.02"),
    (-0.02, -0.005, "-0.02_to_-0.005"),
    (-0.005, 0.005, "-0.005_to_0.005"),
    (0.005, 0.02, "0.005_to_0.02"),
    (0.02, 10.0, "gt_0.02"),
]


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


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


def _flat_dict(value: dict[str, Any]) -> str:
    return "|".join(f"{key}={value[key]}" for key in sorted(value) if value[key] not in (None, "", [], {}, ()))


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _bounded_probability(value: float | None) -> float | None:
    if value is None:
        return None
    return float(min(max(value, 0.0), 1.0))


def _bucket_label(value: float | None, buckets: list[tuple[float, float, str]]) -> str | None:
    if value is None:
        return None
    numeric = float(value)
    for lower, upper, label in buckets:
        if lower <= numeric < upper:
            return label
    return buckets[-1][2] if buckets else None


def _shrinkage_weight(sample_count: int) -> float:
    if sample_count <= 0:
        return 0.0
    return float(min(1.0, float(sample_count) / SHRINKAGE_SAMPLE_TARGET))


@dataclass(frozen=True)
class CalibrationBucket:
    as_of: str
    calibration_type: str
    scope: str
    scope_id: str
    bucket_label: str
    lower_bound: float | None
    upper_bound: float | None
    sample_count: int
    raw_mean: float | None
    realized_mean: float | None
    correction_delta: float | None
    shrinkage_weight: float
    calibrated_mean: float | None
    sufficient_samples: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.calibration_type not in CALIBRATION_TYPES:
            raise ValueError(f"Unsupported calibration_type: {self.calibration_type}")
        if self.scope not in CALIBRATION_SCOPES:
            raise ValueError(f"Unsupported calibration scope: {self.scope}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "calibration_type": self.calibration_type,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "bucket_label": self.bucket_label,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "sample_count": int(self.sample_count),
            "raw_mean": self.raw_mean,
            "realized_mean": self.realized_mean,
            "correction_delta": self.correction_delta,
            "shrinkage_weight": self.shrinkage_weight,
            "calibrated_mean": self.calibrated_mean,
            "sufficient_samples": bool(self.sufficient_samples),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class CalibratedPredictionAdjustment:
    as_of: str
    calibration_type: str
    scope: str
    scope_id: str
    bucket_label: str
    sample_count: int
    raw_reference_value: float | None
    realized_reference_value: float | None
    adjustment_delta: float | None
    shrinkage_weight: float
    calibrated_reference_value: float | None
    sufficient_samples: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.calibration_type not in CALIBRATION_TYPES:
            raise ValueError(f"Unsupported calibration_type: {self.calibration_type}")
        if self.scope not in CALIBRATION_SCOPES:
            raise ValueError(f"Unsupported calibration scope: {self.scope}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "calibration_type": self.calibration_type,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "bucket_label": self.bucket_label,
            "sample_count": int(self.sample_count),
            "raw_reference_value": self.raw_reference_value,
            "realized_reference_value": self.realized_reference_value,
            "adjustment_delta": self.adjustment_delta,
            "shrinkage_weight": self.shrinkage_weight,
            "calibrated_reference_value": self.calibrated_reference_value,
            "sufficient_samples": bool(self.sufficient_samples),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class CalibrationRecord:
    as_of: str
    trade_id: str
    decision_id: str | None
    strategy_id: str
    instrument: str
    regime_label: str | None
    calibration_version: str
    confidence_bucket: str
    expected_value_bucket: str | None
    raw_confidence_value: float | None
    calibrated_confidence_value: float | None
    raw_expected_value_net: float | None
    calibrated_expected_value_net: float | None
    realized_hit: float | None
    realized_net_return: float | None
    confidence_adjustment_scope: str
    confidence_adjustment_scope_id: str
    expected_value_adjustment_scope: str
    expected_value_adjustment_scope_id: str
    confidence_noop: bool
    expected_value_noop: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "trade_id": self.trade_id,
            "decision_id": self.decision_id,
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "regime_label": self.regime_label,
            "calibration_version": self.calibration_version,
            "confidence_bucket": self.confidence_bucket,
            "expected_value_bucket": self.expected_value_bucket,
            "raw_confidence_value": self.raw_confidence_value,
            "calibrated_confidence_value": self.calibrated_confidence_value,
            "raw_expected_value_net": self.raw_expected_value_net,
            "calibrated_expected_value_net": self.calibrated_expected_value_net,
            "realized_hit": self.realized_hit,
            "realized_net_return": self.realized_net_return,
            "confidence_adjustment_scope": self.confidence_adjustment_scope,
            "confidence_adjustment_scope_id": self.confidence_adjustment_scope_id,
            "expected_value_adjustment_scope": self.expected_value_adjustment_scope,
            "expected_value_adjustment_scope_id": self.expected_value_adjustment_scope_id,
            "confidence_noop": bool(self.confidence_noop),
            "expected_value_noop": bool(self.expected_value_noop),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class CalibrationScopeSummary:
    as_of: str
    scope: str
    scope_id: str
    sample_count: int
    confidence_sample_count: int
    expected_value_sample_count: int
    mean_raw_confidence_error: float | None
    mean_calibrated_confidence_error: float | None
    mean_raw_expected_value_error: float | None
    mean_calibrated_expected_value_error: float | None
    sufficient_samples: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.scope not in CALIBRATION_SCOPES:
            raise ValueError(f"Unsupported calibration scope: {self.scope}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "sample_count": int(self.sample_count),
            "confidence_sample_count": int(self.confidence_sample_count),
            "expected_value_sample_count": int(self.expected_value_sample_count),
            "mean_raw_confidence_error": self.mean_raw_confidence_error,
            "mean_calibrated_confidence_error": self.mean_calibrated_confidence_error,
            "mean_raw_expected_value_error": self.mean_raw_expected_value_error,
            "mean_calibrated_expected_value_error": self.mean_calibrated_expected_value_error,
            "sufficient_samples": bool(self.sufficient_samples),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class CalibrationSummaryReport:
    as_of: str
    schema_version: str = CALIBRATION_SCHEMA_VERSION
    records: list[CalibrationRecord] = field(default_factory=list)
    buckets: list[CalibrationBucket] = field(default_factory=list)
    adjustments: list[CalibratedPredictionAdjustment] = field(default_factory=list)
    scope_summaries: list[CalibrationScopeSummary] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "records": [row.to_dict() for row in self.records],
            "buckets": [row.to_dict() for row in self.buckets],
            "adjustments": [row.to_dict() for row in self.adjustments],
            "scope_summaries": [row.to_dict() for row in self.scope_summaries],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CalibrationSummaryReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", CALIBRATION_SCHEMA_VERSION)),
            records=[CalibrationRecord(**row) for row in data.get("records", [])],
            buckets=[CalibrationBucket(**row) for row in data.get("buckets", [])],
            adjustments=[CalibratedPredictionAdjustment(**row) for row in data.get("adjustments", [])],
            scope_summaries=[CalibrationScopeSummary(**row) for row in data.get("scope_summaries", [])],
            summary=dict(data.get("summary") or {}),
        )


def _scope_groups(rows: list["TradeOutcome"]) -> dict[tuple[str, str], list["TradeOutcome"]]:
    groups: dict[tuple[str, str], list["TradeOutcome"]] = {("portfolio", "portfolio"): list(rows)}
    for row in rows:
        groups.setdefault(("strategy", str(row.strategy_id)), []).append(row)
        groups.setdefault(("regime", str(row.regime_label or "unlabeled")), []).append(row)
    return groups


def _bucket_spec(calibration_type: str) -> list[tuple[float, float, str]]:
    return CONFIDENCE_BUCKETS if calibration_type == "confidence" else EV_BUCKETS


def _raw_confidence_value(row: "TradeOutcome") -> float | None:
    return _bounded_probability(row.probability_positive if row.probability_positive is not None else row.confidence_score)


def _raw_expected_value(row: "TradeOutcome") -> float | None:
    return _safe_float(row.predicted_net_return)


def _realized_hit(row: "TradeOutcome") -> float | None:
    if row.realized_net_return is None:
        return None
    return 1.0 if float(row.realized_net_return) > 0.0 else 0.0


def _row_value(row: "TradeOutcome", calibration_type: str) -> float | None:
    return _raw_confidence_value(row) if calibration_type == "confidence" else _raw_expected_value(row)


def _target_value(row: "TradeOutcome", calibration_type: str) -> float | None:
    return _realized_hit(row) if calibration_type == "confidence" else _safe_float(row.realized_net_return)


def _bucket_rows(rows: list["TradeOutcome"], calibration_type: str) -> list[tuple[tuple[float, float, str], list["TradeOutcome"]]]:
    bucket_groups: dict[str, list["TradeOutcome"]] = {}
    spec = _bucket_spec(calibration_type)
    for row in rows:
        label = _bucket_label(_row_value(row, calibration_type), spec)
        if label is None:
            continue
        bucket_groups.setdefault(label, []).append(row)
    ordered: list[tuple[tuple[float, float, str], list["TradeOutcome"]]] = []
    for lower, upper, label in spec:
        ordered.append(((lower, upper, label), list(bucket_groups.get(label, []))))
    return ordered


def _build_scope_bucket_artifacts(
    *,
    as_of: str,
    scope: str,
    scope_id: str,
    rows: list["TradeOutcome"],
) -> tuple[list[CalibrationBucket], list[CalibratedPredictionAdjustment], dict[tuple[str, str], CalibratedPredictionAdjustment]]:
    buckets: list[CalibrationBucket] = []
    adjustments: list[CalibratedPredictionAdjustment] = []
    lookup: dict[tuple[str, str], CalibratedPredictionAdjustment] = {}
    for calibration_type in sorted(CALIBRATION_TYPES):
        for (lower, upper, label), bucket_rows in _bucket_rows(rows, calibration_type):
            raw_values = [_row_value(row, calibration_type) for row in bucket_rows]
            target_values = [_target_value(row, calibration_type) for row in bucket_rows]
            sample_count = len([value for value in raw_values if value is not None and target_values[raw_values.index(value)] is not None]) if bucket_rows else 0
            # Recompute without relying on index lookup to keep duplicate values safe.
            paired = [
                (_row_value(row, calibration_type), _target_value(row, calibration_type))
                for row in bucket_rows
                if _row_value(row, calibration_type) is not None and _target_value(row, calibration_type) is not None
            ]
            sample_count = len(paired)
            raw_mean = _mean([value for value, _ in paired])
            realized_mean = _mean([value for _, value in paired])
            sufficient = sample_count >= MIN_BUCKET_SAMPLE_COUNT
            weight = _shrinkage_weight(sample_count) if sufficient else 0.0
            correction_delta = (
                float(weight * (float(realized_mean) - float(raw_mean)))
                if sufficient and raw_mean is not None and realized_mean is not None
                else 0.0
            )
            calibrated_mean = None
            if raw_mean is not None:
                calibrated_mean = raw_mean + float(correction_delta or 0.0)
                if calibration_type == "confidence":
                    calibrated_mean = _bounded_probability(calibrated_mean)
            bucket = CalibrationBucket(
                as_of=as_of,
                calibration_type=calibration_type,
                scope=scope,
                scope_id=scope_id,
                bucket_label=label,
                lower_bound=lower,
                upper_bound=upper if upper < 10.0 else None,
                sample_count=sample_count,
                raw_mean=raw_mean,
                realized_mean=realized_mean,
                correction_delta=_bounded_probability(correction_delta) if False else correction_delta,
                shrinkage_weight=weight,
                calibrated_mean=calibrated_mean,
                sufficient_samples=sufficient,
            )
            adjustment = CalibratedPredictionAdjustment(
                as_of=as_of,
                calibration_type=calibration_type,
                scope=scope,
                scope_id=scope_id,
                bucket_label=label,
                sample_count=sample_count,
                raw_reference_value=raw_mean,
                realized_reference_value=realized_mean,
                adjustment_delta=correction_delta,
                shrinkage_weight=weight,
                calibrated_reference_value=calibrated_mean,
                sufficient_samples=sufficient,
            )
            buckets.append(bucket)
            adjustments.append(adjustment)
            lookup[(calibration_type, label)] = adjustment
    return buckets, adjustments, lookup


def _adjustment_priority(row: "TradeOutcome") -> list[tuple[str, str]]:
    return [
        ("strategy", str(row.strategy_id)),
        ("regime", str(row.regime_label or "unlabeled")),
        ("portfolio", "portfolio"),
    ]


def _calibrate_row_value(
    *,
    row: "TradeOutcome",
    calibration_type: str,
    adjustment_maps: dict[tuple[str, str], dict[tuple[str, str], CalibratedPredictionAdjustment]],
) -> tuple[float | None, str, str, bool]:
    raw_value = _row_value(row, calibration_type)
    bucket_label = _bucket_label(raw_value, _bucket_spec(calibration_type))
    if raw_value is None or bucket_label is None:
        return raw_value, "portfolio", "portfolio", True
    for scope, scope_id in _adjustment_priority(row):
        adjustment = adjustment_maps.get((scope, scope_id), {}).get((calibration_type, bucket_label))
        if adjustment is None:
            continue
        if not adjustment.sufficient_samples or adjustment.adjustment_delta is None:
            continue
        calibrated = float(raw_value + adjustment.adjustment_delta)
        if calibration_type == "confidence":
            calibrated = _bounded_probability(calibrated)
        return calibrated, scope, scope_id, False
    return raw_value, "portfolio", "portfolio", True


def _scope_summary(*, as_of: str, scope: str, scope_id: str, records: list[CalibrationRecord]) -> CalibrationScopeSummary:
    confidence_pairs = [
        (row.raw_confidence_value, row.calibrated_confidence_value, row.realized_hit)
        for row in records
        if row.raw_confidence_value is not None and row.realized_hit is not None
    ]
    ev_pairs = [
        (row.raw_expected_value_net, row.calibrated_expected_value_net, row.realized_net_return)
        for row in records
        if row.raw_expected_value_net is not None and row.realized_net_return is not None
    ]
    return CalibrationScopeSummary(
        as_of=as_of,
        scope=scope,
        scope_id=scope_id,
        sample_count=len(records),
        confidence_sample_count=len(confidence_pairs),
        expected_value_sample_count=len(ev_pairs),
        mean_raw_confidence_error=_mean([abs(raw - realized) for raw, _, realized in confidence_pairs]),
        mean_calibrated_confidence_error=_mean([abs(calibrated - realized) for _, calibrated, realized in confidence_pairs]),
        mean_raw_expected_value_error=_mean([abs(raw - realized) for raw, _, realized in ev_pairs]),
        mean_calibrated_expected_value_error=_mean([abs(calibrated - realized) for _, calibrated, realized in ev_pairs]),
        sufficient_samples=len(records) >= MIN_SCOPE_SAMPLE_COUNT,
    )


def build_calibration_summary_report(*, result: "PaperTradingRunResult") -> CalibrationSummaryReport:
    outcome_report = result.outcome_attribution_report
    if outcome_report is None or not outcome_report.outcomes:
        return CalibrationSummaryReport(
            as_of=result.as_of,
            summary={
                "record_count": 0,
                "bucket_count": 0,
                "adjustment_count": 0,
                "sufficient_scope_count": 0,
            },
        )
    outcomes = list(outcome_report.outcomes)
    buckets: list[CalibrationBucket] = []
    adjustments: list[CalibratedPredictionAdjustment] = []
    adjustment_maps: dict[tuple[str, str], dict[tuple[str, str], CalibratedPredictionAdjustment]] = {}
    group_lookup = _scope_groups(outcomes)
    for (scope, scope_id), rows in sorted(group_lookup.items()):
        if len(rows) < MIN_SCOPE_SAMPLE_COUNT:
            continue
        scope_buckets, scope_adjustments, scope_lookup = _build_scope_bucket_artifacts(
            as_of=result.as_of,
            scope=scope,
            scope_id=scope_id,
            rows=rows,
        )
        buckets.extend(scope_buckets)
        adjustments.extend(scope_adjustments)
        adjustment_maps[(scope, scope_id)] = scope_lookup

    records: list[CalibrationRecord] = []
    for row in outcomes:
        raw_confidence = _raw_confidence_value(row)
        raw_ev = _raw_expected_value(row)
        calibrated_confidence, confidence_scope, confidence_scope_id, confidence_noop = _calibrate_row_value(
            row=row,
            calibration_type="confidence",
            adjustment_maps=adjustment_maps,
        )
        calibrated_ev, ev_scope, ev_scope_id, ev_noop = _calibrate_row_value(
            row=row,
            calibration_type="expected_value",
            adjustment_maps=adjustment_maps,
        )
        records.append(
            CalibrationRecord(
                as_of=result.as_of,
                trade_id=row.trade_id,
                decision_id=row.decision_id,
                strategy_id=row.strategy_id,
                instrument=row.instrument,
                regime_label=row.regime_label,
                calibration_version=CALIBRATION_SCHEMA_VERSION,
                confidence_bucket=str(row.confidence_bucket or "unknown"),
                expected_value_bucket=_bucket_label(raw_ev, EV_BUCKETS),
                raw_confidence_value=raw_confidence,
                calibrated_confidence_value=calibrated_confidence,
                raw_expected_value_net=raw_ev,
                calibrated_expected_value_net=calibrated_ev,
                realized_hit=_realized_hit(row),
                realized_net_return=row.realized_net_return,
                confidence_adjustment_scope=confidence_scope,
                confidence_adjustment_scope_id=confidence_scope_id,
                expected_value_adjustment_scope=ev_scope,
                expected_value_adjustment_scope_id=ev_scope_id,
                confidence_noop=confidence_noop,
                expected_value_noop=ev_noop,
                metadata={
                    "horizon_days": row.horizon_days,
                    "holding_period_days": row.holding_period_days,
                },
            )
        )

    scope_records: dict[tuple[str, str], list[CalibrationRecord]] = {}
    for record in records:
        scope_records.setdefault(("portfolio", "portfolio"), []).append(record)
        scope_records.setdefault(("strategy", record.strategy_id), []).append(record)
        scope_records.setdefault(("regime", str(record.regime_label or "unlabeled")), []).append(record)
    scope_summaries = [
        _scope_summary(as_of=result.as_of, scope=scope, scope_id=scope_id, records=rows)
        for (scope, scope_id), rows in sorted(scope_records.items())
        if scope == "portfolio" or len(rows) >= MIN_SCOPE_SAMPLE_COUNT
    ]

    portfolio_summary = next(
        (row for row in scope_summaries if row.scope == "portfolio" and row.scope_id == "portfolio"),
        None,
    )
    summary = {
        "record_count": len(records),
        "bucket_count": len(buckets),
        "adjustment_count": len(adjustments),
        "sufficient_scope_count": sum(1 for row in scope_summaries if row.sufficient_samples),
        "mean_raw_confidence_error": portfolio_summary.mean_raw_confidence_error if portfolio_summary else None,
        "mean_calibrated_confidence_error": portfolio_summary.mean_calibrated_confidence_error if portfolio_summary else None,
        "mean_raw_expected_value_error": portfolio_summary.mean_raw_expected_value_error if portfolio_summary else None,
        "mean_calibrated_expected_value_error": portfolio_summary.mean_calibrated_expected_value_error if portfolio_summary else None,
        "strategy_summary_count": sum(1 for row in scope_summaries if row.scope == "strategy"),
        "regime_summary_count": sum(1 for row in scope_summaries if row.scope == "regime"),
        "confidence_noop_count": sum(1 for row in records if row.confidence_noop),
        "expected_value_noop_count": sum(1 for row in records if row.expected_value_noop),
    }
    return CalibrationSummaryReport(
        as_of=result.as_of,
        records=records,
        buckets=buckets,
        adjustments=adjustments,
        scope_summaries=scope_summaries,
        summary=summary,
    )


def write_calibration_artifacts(*, output_dir: str | Path, report: CalibrationSummaryReport) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "calibration_summary_report.json"
    records_path = output_path / "calibration_records.csv"
    buckets_path = output_path / "calibration_buckets.csv"
    adjustments_path = output_path / "calibrated_prediction_adjustments.csv"
    scope_summaries_path = output_path / "calibration_scope_summaries.csv"
    summary_path = output_path / "calibration_summary.json"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    summary_path.write_text(json.dumps(report.summary, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in report.records], columns=[row.name for row in fields(CalibrationRecord)]).to_csv(
        records_path,
        index=False,
    )
    pd.DataFrame([row.flat_dict() for row in report.buckets], columns=[row.name for row in fields(CalibrationBucket)]).to_csv(
        buckets_path,
        index=False,
    )
    pd.DataFrame(
        [row.flat_dict() for row in report.adjustments],
        columns=[row.name for row in fields(CalibratedPredictionAdjustment)],
    ).to_csv(adjustments_path, index=False)
    pd.DataFrame(
        [row.flat_dict() for row in report.scope_summaries],
        columns=[row.name for row in fields(CalibrationScopeSummary)],
    ).to_csv(scope_summaries_path, index=False)
    return {
        "calibration_summary_report_json_path": json_path,
        "calibration_records_csv_path": records_path,
        "calibration_buckets_csv_path": buckets_path,
        "calibrated_prediction_adjustments_csv_path": adjustments_path,
        "calibration_scope_summaries_csv_path": scope_summaries_path,
        "calibration_summary_json_path": summary_path,
    }
