from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult
    from trading_platform.reporting.outcome_attribution import TradeAttribution, TradeOutcome


DRIFT_DETECTION_SCHEMA_VERSION = "drift_detection_v1"
DRIFT_CATEGORIES = {"performance", "decision", "execution"}
DRIFT_SEVERITIES = {"info", "watch", "warning", "critical"}
DRIFT_RECOMMENDED_ACTIONS = {"monitor", "review", "constrain", "escalate_to_risk_controls"}
DRIFT_SCOPES = {"portfolio", "strategy", "instrument", "regime"}
SEVERITY_ORDER = {"info": 0, "watch": 1, "warning": 2, "critical": 3}
METRIC_THRESHOLDS: dict[str, tuple[float, float, float, float]] = {
    "forecast_gap": (0.015, 0.03, 0.06, 0.10),
    "win_rate_gap": (0.05, 0.10, 0.20, 0.30),
    "predicted_net_return_shift": (0.01, 0.02, 0.04, 0.06),
    "confidence_score_shift": (0.05, 0.10, 0.20, 0.30),
    "confidence_bucket_distribution_shift": (0.15, 0.30, 0.60, 1.00),
    "regime_mix_shift": (0.15, 0.30, 0.60, 1.00),
    "trade_count_shift": (0.10, 0.25, 0.40, 0.60),
    "cost_gap": (0.005, 0.01, 0.02, 0.04),
    "execution_error_shift": (0.005, 0.01, 0.02, 0.04),
    "fill_rate_gap": (0.02, 0.05, 0.10, 0.20),
}


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


def _window_bounds(rows: list["TradeOutcome"]) -> tuple[str | None, str | None]:
    if not rows:
        return None, None
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.exit_date or row.entry_date or ""),
            str(row.entry_date or ""),
            str(row.trade_id),
        ),
    )
    start = ordered[0].exit_date or ordered[0].entry_date
    end = ordered[-1].exit_date or ordered[-1].entry_date
    return start, end


def _window_labels(rows: list["TradeOutcome"], *, fallback_label: str) -> dict[str, Any]:
    start, end = _window_bounds(rows)
    return {"label": fallback_label, "start": start, "end": end}


def _split_recent_and_baseline(rows: list["TradeOutcome"]) -> tuple[list["TradeOutcome"], list["TradeOutcome"], str]:
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.exit_date or row.entry_date or ""),
            str(row.entry_date or ""),
            str(row.trade_id),
        ),
    )
    if len(ordered) >= 4:
        midpoint = len(ordered) // 2
        return ordered[:midpoint], ordered[midpoint:], "rolling_half_split"
    return [], ordered, "expected_reference"


def _distribution_distance(values: list[str], baseline_values: list[str]) -> float | None:
    if not values or not baseline_values:
        return None
    categories = sorted(set(values) | set(baseline_values))
    if not categories:
        return None
    current_count = float(len(values))
    baseline_count = float(len(baseline_values))
    distance = 0.0
    for category in categories:
        current_weight = sum(1 for value in values if value == category) / current_count
        baseline_weight = sum(1 for value in baseline_values if value == category) / baseline_count
        distance += abs(float(current_weight - baseline_weight))
    return float(distance)


def _relative_delta(*, recent_value: float | None, baseline_value: float | None) -> float | None:
    if recent_value is None or baseline_value is None or abs(baseline_value) <= 1e-12:
        return None
    return float((recent_value - baseline_value) / abs(baseline_value))


def _severity_for_metric(metric_name: str, magnitude: float | None) -> tuple[str | None, float | None]:
    if magnitude is None:
        return None, None
    info_threshold, watch_threshold, warning_threshold, critical_threshold = METRIC_THRESHOLDS[metric_name]
    if magnitude >= critical_threshold:
        return "critical", critical_threshold
    if magnitude >= warning_threshold:
        return "warning", warning_threshold
    if magnitude >= watch_threshold:
        return "watch", watch_threshold
    if magnitude >= info_threshold:
        return "info", info_threshold
    return None, None


def _recommended_action(*, severity: str, category: str) -> str:
    if severity == "critical":
        return "escalate_to_risk_controls"
    if severity == "warning":
        return "constrain" if category in {"performance", "execution"} else "review"
    if severity == "watch":
        return "review"
    return "monitor"


@dataclass(frozen=True)
class DriftMetricSnapshot:
    as_of: str
    category: str
    metric_name: str
    scope: str
    scope_id: str
    comparator_mode: str
    recent_window_label: str
    baseline_window_label: str
    recent_window_start: str | None
    recent_window_end: str | None
    baseline_window_start: str | None
    baseline_window_end: str | None
    recent_value: float | None
    baseline_value: float | None
    delta: float | None
    relative_delta: float | None
    observation_count_recent: int
    observation_count_baseline: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.category not in DRIFT_CATEGORIES:
            raise ValueError(f"Unsupported drift category: {self.category}")
        if self.scope not in DRIFT_SCOPES:
            raise ValueError(f"Unsupported drift scope: {self.scope}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "category": self.category,
            "metric_name": self.metric_name,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "comparator_mode": self.comparator_mode,
            "recent_window_label": self.recent_window_label,
            "baseline_window_label": self.baseline_window_label,
            "recent_window_start": self.recent_window_start,
            "recent_window_end": self.recent_window_end,
            "baseline_window_start": self.baseline_window_start,
            "baseline_window_end": self.baseline_window_end,
            "recent_value": self.recent_value,
            "baseline_value": self.baseline_value,
            "delta": self.delta,
            "relative_delta": self.relative_delta,
            "observation_count_recent": int(self.observation_count_recent),
            "observation_count_baseline": int(self.observation_count_baseline),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DriftMetricSnapshot":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            category=str(data["category"]),
            metric_name=str(data["metric_name"]),
            scope=str(data["scope"]),
            scope_id=str(data["scope_id"]),
            comparator_mode=str(data.get("comparator_mode", "expected_reference")),
            recent_window_label=str(data.get("recent_window_label", "recent")),
            baseline_window_label=str(data.get("baseline_window_label", "baseline")),
            recent_window_start=str(data["recent_window_start"]) if data.get("recent_window_start") is not None else None,
            recent_window_end=str(data["recent_window_end"]) if data.get("recent_window_end") is not None else None,
            baseline_window_start=str(data["baseline_window_start"]) if data.get("baseline_window_start") is not None else None,
            baseline_window_end=str(data["baseline_window_end"]) if data.get("baseline_window_end") is not None else None,
            recent_value=_safe_float(data.get("recent_value")),
            baseline_value=_safe_float(data.get("baseline_value")),
            delta=_safe_float(data.get("delta")),
            relative_delta=_safe_float(data.get("relative_delta")),
            observation_count_recent=int(data.get("observation_count_recent", 0) or 0),
            observation_count_baseline=int(data.get("observation_count_baseline", 0) or 0),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class DriftSignal:
    as_of: str
    category: str
    metric_name: str
    scope: str
    scope_id: str
    severity: str
    recommended_action: str
    comparator_mode: str
    recent_value: float | None
    baseline_value: float | None
    delta: float | None
    relative_delta: float | None
    threshold: float | None
    recent_window_label: str
    baseline_window_label: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.category not in DRIFT_CATEGORIES:
            raise ValueError(f"Unsupported drift category: {self.category}")
        if self.scope not in DRIFT_SCOPES:
            raise ValueError(f"Unsupported drift scope: {self.scope}")
        if self.severity not in DRIFT_SEVERITIES:
            raise ValueError(f"Unsupported drift severity: {self.severity}")
        if self.recommended_action not in DRIFT_RECOMMENDED_ACTIONS:
            raise ValueError(f"Unsupported recommended action: {self.recommended_action}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "category": self.category,
            "metric_name": self.metric_name,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "severity": self.severity,
            "recommended_action": self.recommended_action,
            "comparator_mode": self.comparator_mode,
            "recent_value": self.recent_value,
            "baseline_value": self.baseline_value,
            "delta": self.delta,
            "relative_delta": self.relative_delta,
            "threshold": self.threshold,
            "recent_window_label": self.recent_window_label,
            "baseline_window_label": self.baseline_window_label,
            "message": self.message,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DriftSignal":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            category=str(data["category"]),
            metric_name=str(data["metric_name"]),
            scope=str(data["scope"]),
            scope_id=str(data["scope_id"]),
            severity=str(data["severity"]),
            recommended_action=str(data["recommended_action"]),
            comparator_mode=str(data.get("comparator_mode", "expected_reference")),
            recent_value=_safe_float(data.get("recent_value")),
            baseline_value=_safe_float(data.get("baseline_value")),
            delta=_safe_float(data.get("delta")),
            relative_delta=_safe_float(data.get("relative_delta")),
            threshold=_safe_float(data.get("threshold")),
            recent_window_label=str(data.get("recent_window_label", "recent")),
            baseline_window_label=str(data.get("baseline_window_label", "baseline")),
            message=str(data.get("message", "")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class DriftSummaryReport:
    as_of: str
    schema_version: str = DRIFT_DETECTION_SCHEMA_VERSION
    metric_snapshots: list[DriftMetricSnapshot] = field(default_factory=list)
    signals: list[DriftSignal] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "metric_snapshots": [row.to_dict() for row in self.metric_snapshots],
            "signals": [row.to_dict() for row in self.signals],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DriftSummaryReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", DRIFT_DETECTION_SCHEMA_VERSION)),
            metric_snapshots=[DriftMetricSnapshot.from_dict(row) for row in data.get("metric_snapshots", [])],
            signals=[DriftSignal.from_dict(row) for row in data.get("signals", [])],
            summary=dict(data.get("summary") or {}),
        )


def _attribution_lookup(rows: list["TradeAttribution"]) -> dict[str, "TradeAttribution"]:
    return {str(row.trade_id): row for row in rows}


def _snapshot_and_signal(
    *,
    as_of: str,
    category: str,
    metric_name: str,
    scope: str,
    scope_id: str,
    comparator_mode: str,
    recent_rows: list["TradeOutcome"],
    baseline_rows: list["TradeOutcome"],
    recent_value: float | None,
    baseline_value: float | None,
    metadata: dict[str, Any] | None = None,
) -> tuple[DriftMetricSnapshot, DriftSignal | None]:
    delta = None
    if recent_value is not None and baseline_value is not None:
        delta = float(recent_value - baseline_value)
    relative_delta = _relative_delta(recent_value=recent_value, baseline_value=baseline_value)
    recent_window = _window_labels(recent_rows, fallback_label="recent_window")
    baseline_window = _window_labels(baseline_rows, fallback_label="baseline_window" if baseline_rows else "expected_reference")
    snapshot = DriftMetricSnapshot(
        as_of=as_of,
        category=category,
        metric_name=metric_name,
        scope=scope,
        scope_id=scope_id,
        comparator_mode=comparator_mode,
        recent_window_label=str(recent_window["label"]),
        baseline_window_label=str(baseline_window["label"]),
        recent_window_start=recent_window["start"],
        recent_window_end=recent_window["end"],
        baseline_window_start=baseline_window["start"],
        baseline_window_end=baseline_window["end"],
        recent_value=recent_value,
        baseline_value=baseline_value,
        delta=delta,
        relative_delta=relative_delta,
        observation_count_recent=len(recent_rows),
        observation_count_baseline=len(baseline_rows),
        metadata=dict(metadata or {}),
    )
    magnitude = abs(delta) if delta is not None else None
    severity, threshold = _severity_for_metric(metric_name, magnitude)
    if severity is None:
        return snapshot, None
    signal = DriftSignal(
        as_of=as_of,
        category=category,
        metric_name=metric_name,
        scope=scope,
        scope_id=scope_id,
        severity=severity,
        recommended_action=_recommended_action(severity=severity, category=category),
        comparator_mode=comparator_mode,
        recent_value=recent_value,
        baseline_value=baseline_value,
        delta=delta,
        relative_delta=relative_delta,
        threshold=threshold,
        recent_window_label=snapshot.recent_window_label,
        baseline_window_label=snapshot.baseline_window_label,
        message=f"{category} drift detected for {scope}:{scope_id} on {metric_name}",
        metadata=dict(metadata or {}),
    )
    return snapshot, signal


def _confidence_bucket_distribution(rows: list["TradeOutcome"]) -> list[str]:
    return [str(row.confidence_bucket or "unknown") for row in rows]


def _regime_distribution(rows: list["TradeOutcome"]) -> list[str]:
    return [str(row.regime_label or "unlabeled") for row in rows]


def _build_scope_groups(rows: list["TradeOutcome"]) -> dict[tuple[str, str], list["TradeOutcome"]]:
    groups: dict[tuple[str, str], list["TradeOutcome"]] = {("portfolio", "portfolio"): list(rows)}
    for row in rows:
        groups.setdefault(("strategy", str(row.strategy_id)), []).append(row)
        groups.setdefault(("instrument", str(row.instrument)), []).append(row)
        groups.setdefault(("regime", str(row.regime_label or "unlabeled")), []).append(row)
    return groups


def _mean_win_rate(rows: list["TradeOutcome"]) -> float | None:
    if not rows:
        return None
    wins = [1.0 if (row.realized_net_return or 0.0) > 0.0 else 0.0 for row in rows if row.realized_net_return is not None]
    return _mean(wins)


def _trade_decision_count_shift(*, recent_count: int, baseline_count: int) -> float | None:
    if baseline_count <= 0:
        return None
    return float((recent_count - baseline_count) / baseline_count)


def build_drift_summary_report(*, result: "PaperTradingRunResult") -> DriftSummaryReport:
    outcome_report = result.outcome_attribution_report
    if outcome_report is None:
        return DriftSummaryReport(
            as_of=result.as_of,
            summary={
                "snapshot_count": 0,
                "signal_count": 0,
                "severity_counts": {},
                "category_counts": {},
            },
        )
    outcomes = list(outcome_report.outcomes)
    attribution_by_trade = _attribution_lookup(list(outcome_report.attributions))
    metric_snapshots: list[DriftMetricSnapshot] = []
    signals: list[DriftSignal] = []

    for (scope, scope_id), grouped_outcomes in sorted(_build_scope_groups(outcomes).items()):
        baseline_rows, recent_rows, comparator_mode = _split_recent_and_baseline(grouped_outcomes)
        if not recent_rows:
            continue
        recent_predicted_net = _mean([row.predicted_net_return for row in recent_rows])
        recent_realized_net = _mean([row.realized_net_return for row in recent_rows])
        baseline_realized_net = _mean([row.realized_net_return for row in baseline_rows]) if baseline_rows else recent_predicted_net
        snapshot, signal = _snapshot_and_signal(
            as_of=result.as_of,
            category="performance",
            metric_name="forecast_gap",
            scope=scope,
            scope_id=scope_id,
            comparator_mode=comparator_mode,
            recent_rows=recent_rows,
            baseline_rows=baseline_rows,
            recent_value=recent_realized_net,
            baseline_value=baseline_realized_net,
            metadata={"reference_expected_value": recent_predicted_net},
        )
        metric_snapshots.append(snapshot)
        if signal is not None:
            signals.append(signal)

        recent_win_rate = _mean_win_rate(recent_rows)
        baseline_win_rate = _mean_win_rate(baseline_rows) if baseline_rows else _mean(
            [row.probability_positive for row in recent_rows]
        )
        snapshot, signal = _snapshot_and_signal(
            as_of=result.as_of,
            category="performance",
            metric_name="win_rate_gap",
            scope=scope,
            scope_id=scope_id,
            comparator_mode=comparator_mode,
            recent_rows=recent_rows,
            baseline_rows=baseline_rows,
            recent_value=recent_win_rate,
            baseline_value=baseline_win_rate,
        )
        metric_snapshots.append(snapshot)
        if signal is not None:
            signals.append(signal)

        if baseline_rows:
            snapshot, signal = _snapshot_and_signal(
                as_of=result.as_of,
                category="decision",
                metric_name="predicted_net_return_shift",
                scope=scope,
                scope_id=scope_id,
                comparator_mode=comparator_mode,
                recent_rows=recent_rows,
                baseline_rows=baseline_rows,
                recent_value=_mean([row.predicted_net_return for row in recent_rows]),
                baseline_value=_mean([row.predicted_net_return for row in baseline_rows]),
            )
            metric_snapshots.append(snapshot)
            if signal is not None:
                signals.append(signal)

            snapshot, signal = _snapshot_and_signal(
                as_of=result.as_of,
                category="decision",
                metric_name="confidence_score_shift",
                scope=scope,
                scope_id=scope_id,
                comparator_mode=comparator_mode,
                recent_rows=recent_rows,
                baseline_rows=baseline_rows,
                recent_value=_mean([row.confidence_score for row in recent_rows]),
                baseline_value=_mean([row.confidence_score for row in baseline_rows]),
            )
            metric_snapshots.append(snapshot)
            if signal is not None:
                signals.append(signal)

            snapshot, signal = _snapshot_and_signal(
                as_of=result.as_of,
                category="decision",
                metric_name="trade_count_shift",
                scope=scope,
                scope_id=scope_id,
                comparator_mode=comparator_mode,
                recent_rows=recent_rows,
                baseline_rows=baseline_rows,
                recent_value=float(len(recent_rows)),
                baseline_value=float(len(baseline_rows)),
                metadata={
                    "normalized_trade_count_shift": _trade_decision_count_shift(
                        recent_count=len(recent_rows),
                        baseline_count=len(baseline_rows),
                    )
                },
            )
            metric_snapshots.append(snapshot)
            if signal is not None:
                signals.append(signal)

            confidence_distance = _distribution_distance(
                _confidence_bucket_distribution(recent_rows),
                _confidence_bucket_distribution(baseline_rows),
            )
            snapshot, signal = _snapshot_and_signal(
                as_of=result.as_of,
                category="decision",
                metric_name="confidence_bucket_distribution_shift",
                scope=scope,
                scope_id=scope_id,
                comparator_mode=comparator_mode,
                recent_rows=recent_rows,
                baseline_rows=baseline_rows,
                recent_value=confidence_distance,
                baseline_value=0.0 if confidence_distance is not None else None,
            )
            metric_snapshots.append(snapshot)
            if signal is not None:
                signals.append(signal)

            regime_distance = _distribution_distance(
                _regime_distribution(recent_rows),
                _regime_distribution(baseline_rows),
            )
            snapshot, signal = _snapshot_and_signal(
                as_of=result.as_of,
                category="decision",
                metric_name="regime_mix_shift",
                scope=scope,
                scope_id=scope_id,
                comparator_mode=comparator_mode,
                recent_rows=recent_rows,
                baseline_rows=baseline_rows,
                recent_value=regime_distance,
                baseline_value=0.0 if regime_distance is not None else None,
            )
            metric_snapshots.append(snapshot)
            if signal is not None:
                signals.append(signal)

        recent_cost = _mean([row.realized_cost for row in recent_rows])
        baseline_cost = _mean([row.realized_cost for row in baseline_rows]) if baseline_rows else _mean(
            [row.predicted_cost for row in recent_rows]
        )
        snapshot, signal = _snapshot_and_signal(
            as_of=result.as_of,
            category="execution",
            metric_name="cost_gap",
            scope=scope,
            scope_id=scope_id,
            comparator_mode=comparator_mode,
            recent_rows=recent_rows,
            baseline_rows=baseline_rows,
            recent_value=recent_cost,
            baseline_value=baseline_cost,
        )
        metric_snapshots.append(snapshot)
        if signal is not None:
            signals.append(signal)

        recent_execution_error = _mean(
            [_safe_float(getattr(attribution_by_trade.get(row.trade_id), "execution_error", None)) for row in recent_rows]
        )
        baseline_execution_error = _mean(
            [_safe_float(getattr(attribution_by_trade.get(row.trade_id), "execution_error", None)) for row in baseline_rows]
        ) if baseline_rows else 0.0
        snapshot, signal = _snapshot_and_signal(
            as_of=result.as_of,
            category="execution",
            metric_name="execution_error_shift",
            scope=scope,
            scope_id=scope_id,
            comparator_mode=comparator_mode,
            recent_rows=recent_rows,
            baseline_rows=baseline_rows,
            recent_value=recent_execution_error,
            baseline_value=baseline_execution_error,
        )
        metric_snapshots.append(snapshot)
        if signal is not None:
            signals.append(signal)

    execution_report = result.execution_simulation_report
    if execution_report is not None and execution_report.orders:
        fills = [
            float(row.executable_quantity) / float(row.requested_quantity)
            for row in execution_report.orders
            if int(row.requested_quantity) > 0
        ]
        fill_rate = _mean(fills)
        snapshot, signal = _snapshot_and_signal(
            as_of=result.as_of,
            category="execution",
            metric_name="fill_rate_gap",
            scope="portfolio",
            scope_id="portfolio",
            comparator_mode="expected_reference",
            recent_rows=outcomes,
            baseline_rows=[],
            recent_value=fill_rate,
            baseline_value=1.0 if fill_rate is not None else None,
            metadata={"execution_order_count": len(execution_report.orders)},
        )
        metric_snapshots.append(snapshot)
        if signal is not None:
            signals.append(signal)

    highest_severity = None
    if signals:
        highest_severity = max(signals, key=lambda row: SEVERITY_ORDER[row.severity]).severity
    summary = {
        "snapshot_count": len(metric_snapshots),
        "signal_count": len(signals),
        "highest_severity": highest_severity,
        "severity_counts": {
            severity: sum(1 for row in signals if row.severity == severity)
            for severity in sorted({row.severity for row in signals}, key=lambda value: SEVERITY_ORDER[value])
        },
        "category_counts": {
            category: sum(1 for row in signals if row.category == category)
            for category in sorted({row.category for row in signals})
        },
        "scope_counts": {
            scope: sum(1 for row in signals if row.scope == scope)
            for scope in sorted({row.scope for row in signals})
        },
    }
    return DriftSummaryReport(
        as_of=result.as_of,
        metric_snapshots=metric_snapshots,
        signals=signals,
        summary=summary,
    )


def write_drift_detection_artifacts(*, output_dir: str | Path, report: DriftSummaryReport) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "drift_detection_report.json"
    snapshots_path = output_path / "drift_metric_snapshots.csv"
    signals_path = output_path / "drift_signals.csv"
    summary_path = output_path / "drift_detection_summary.json"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    summary_path.write_text(json.dumps(report.summary, indent=2, default=str), encoding="utf-8")
    snapshot_columns = [row.name for row in fields(DriftMetricSnapshot)]
    signal_columns = [row.name for row in fields(DriftSignal)]
    pd.DataFrame([row.flat_dict() for row in report.metric_snapshots], columns=snapshot_columns).to_csv(
        snapshots_path,
        index=False,
    )
    pd.DataFrame([row.flat_dict() for row in report.signals], columns=signal_columns).to_csv(signals_path, index=False)
    return {
        "drift_detection_report_json_path": json_path,
        "drift_metric_snapshots_csv_path": snapshots_path,
        "drift_signals_csv_path": signals_path,
        "drift_detection_summary_json_path": summary_path,
    }
