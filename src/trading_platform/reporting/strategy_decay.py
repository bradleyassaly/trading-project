from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult
    from trading_platform.reporting.calibration import CalibrationScopeSummary
    from trading_platform.reporting.drift_detection import DriftSignal
    from trading_platform.reporting.outcome_attribution import TradeAttribution, TradeOutcome
    from trading_platform.risk.controls import RiskControlAction, RiskControlTrigger


STRATEGY_DECAY_SCHEMA_VERSION = "strategy_decay_v1"
DECAY_SEVERITIES = {"healthy", "watch", "warning", "critical"}
DECAY_ACTIONS = {"monitor", "review", "constrain", "demote_candidate"}
MIN_STRATEGY_SAMPLE_COUNT = 4


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


def _sum(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean))


def _window_bounds(rows: list["TradeOutcome"]) -> tuple[str | None, str | None]:
    if not rows:
        return None, None
    ordered = sorted(rows, key=lambda row: (str(row.exit_date or row.entry_date or ""), str(row.trade_id)))
    return ordered[0].exit_date or ordered[0].entry_date, ordered[-1].exit_date or ordered[-1].entry_date


def _severity_from_score(score: float | None) -> str:
    if score is None:
        return "healthy"
    if score >= 0.80:
        return "critical"
    if score >= 0.55:
        return "warning"
    if score >= 0.30:
        return "watch"
    return "healthy"


def _recommended_action(severity: str) -> str:
    if severity == "critical":
        return "demote_candidate"
    if severity == "warning":
        return "constrain"
    if severity == "watch":
        return "review"
    return "monitor"


def _strategy_groups(outcomes: list["TradeOutcome"]) -> dict[str, list["TradeOutcome"]]:
    groups: dict[str, list["TradeOutcome"]] = {}
    for row in outcomes:
        groups.setdefault(str(row.strategy_id), []).append(row)
    return groups


def _regime_breakdown(rows: list["TradeOutcome"]) -> dict[str, int]:
    breakdown: dict[str, int] = {}
    for row in rows:
        key = str(row.regime_label or "unlabeled")
        breakdown[key] = breakdown.get(key, 0) + 1
    return breakdown


@dataclass(frozen=True)
class StrategyDecaySignal:
    as_of: str
    strategy_id: str
    signal_type: str
    severity: str
    observed_value: float | None
    threshold: float | None
    recommended_action: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.severity not in DECAY_SEVERITIES:
            raise ValueError(f"Unsupported strategy decay severity: {self.severity}")
        if self.recommended_action not in DECAY_ACTIONS:
            raise ValueError(f"Unsupported strategy decay recommended_action: {self.recommended_action}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "signal_type": self.signal_type,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
            "recommended_action": self.recommended_action,
            "message": self.message,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class StrategyLifecycleRecommendation:
    as_of: str
    strategy_id: str
    severity: str
    recommended_action: str
    decay_score: float | None
    sufficient_samples: bool
    rationale: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.severity not in DECAY_SEVERITIES:
            raise ValueError(f"Unsupported strategy lifecycle severity: {self.severity}")
        if self.recommended_action not in DECAY_ACTIONS:
            raise ValueError(f"Unsupported strategy lifecycle recommended_action: {self.recommended_action}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "severity": self.severity,
            "recommended_action": self.recommended_action,
            "decay_score": self.decay_score,
            "sufficient_samples": bool(self.sufficient_samples),
            "rationale": list(self.rationale),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["rationale"] = "|".join(self.rationale)
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class StrategyDecayRecord:
    as_of: str
    strategy_id: str
    evaluation_window_start: str | None
    evaluation_window_end: str | None
    trade_count: int
    sufficient_samples: bool
    mean_predicted_net_return: float | None
    mean_realized_net_return: float | None
    mean_forecast_gap: float | None
    mean_cost_error: float | None
    mean_execution_error: float | None
    drift_signal_count: int
    drift_warning_or_worse_count: int
    calibration_confidence_error: float | None
    calibration_expected_value_error: float | None
    risk_trigger_count: int
    risk_halted_or_restricted_count: int
    realized_drawdown_proxy: float | None
    decay_score: float | None
    severity: str
    recommended_action: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.severity not in DECAY_SEVERITIES:
            raise ValueError(f"Unsupported strategy decay severity: {self.severity}")
        if self.recommended_action not in DECAY_ACTIONS:
            raise ValueError(f"Unsupported strategy decay recommended_action: {self.recommended_action}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "evaluation_window_start": self.evaluation_window_start,
            "evaluation_window_end": self.evaluation_window_end,
            "trade_count": int(self.trade_count),
            "sufficient_samples": bool(self.sufficient_samples),
            "mean_predicted_net_return": self.mean_predicted_net_return,
            "mean_realized_net_return": self.mean_realized_net_return,
            "mean_forecast_gap": self.mean_forecast_gap,
            "mean_cost_error": self.mean_cost_error,
            "mean_execution_error": self.mean_execution_error,
            "drift_signal_count": int(self.drift_signal_count),
            "drift_warning_or_worse_count": int(self.drift_warning_or_worse_count),
            "calibration_confidence_error": self.calibration_confidence_error,
            "calibration_expected_value_error": self.calibration_expected_value_error,
            "risk_trigger_count": int(self.risk_trigger_count),
            "risk_halted_or_restricted_count": int(self.risk_halted_or_restricted_count),
            "realized_drawdown_proxy": self.realized_drawdown_proxy,
            "decay_score": self.decay_score,
            "severity": self.severity,
            "recommended_action": self.recommended_action,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class StrategyDecaySummaryReport:
    as_of: str
    schema_version: str = STRATEGY_DECAY_SCHEMA_VERSION
    records: list[StrategyDecayRecord] = field(default_factory=list)
    signals: list[StrategyDecaySignal] = field(default_factory=list)
    recommendations: list[StrategyLifecycleRecommendation] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "records": [row.to_dict() for row in self.records],
            "signals": [row.to_dict() for row in self.signals],
            "recommendations": [row.to_dict() for row in self.recommendations],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyDecaySummaryReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", STRATEGY_DECAY_SCHEMA_VERSION)),
            records=[StrategyDecayRecord(**row) for row in data.get("records", [])],
            signals=[StrategyDecaySignal(**row) for row in data.get("signals", [])],
            recommendations=[StrategyLifecycleRecommendation(**row) for row in data.get("recommendations", [])],
            summary=dict(data.get("summary") or {}),
        )


def _attribution_by_trade(result: "PaperTradingRunResult") -> dict[str, "TradeAttribution"]:
    outcome_report = result.outcome_attribution_report
    if outcome_report is None:
        return {}
    return {str(row.trade_id): row for row in outcome_report.attributions}


def _drift_signals_by_strategy(result: "PaperTradingRunResult") -> dict[str, list["DriftSignal"]]:
    grouped: dict[str, list["DriftSignal"]] = {}
    drift_report = result.drift_report
    if drift_report is None:
        return grouped
    for row in drift_report.signals:
        if row.scope == "strategy":
            grouped.setdefault(str(row.scope_id), []).append(row)
    return grouped


def _calibration_summary_by_strategy(result: "PaperTradingRunResult") -> dict[str, "CalibrationScopeSummary"]:
    report = result.calibration_report
    if report is None:
        return {}
    return {
        str(row.scope_id): row
        for row in report.scope_summaries
        if row.scope == "strategy"
    }


def _risk_triggers_by_strategy(result: "PaperTradingRunResult") -> dict[str, list["RiskControlTrigger"]]:
    grouped: dict[str, list["RiskControlTrigger"]] = {}
    report = result.risk_control_report
    if report is None:
        return grouped
    for row in report.triggers:
        if row.scope == "strategy":
            grouped.setdefault(str(row.scope_id), []).append(row)
    return grouped


def _risk_actions_by_strategy(result: "PaperTradingRunResult") -> dict[str, list["RiskControlAction"]]:
    grouped: dict[str, list["RiskControlAction"]] = {}
    report = result.risk_control_report
    if report is None:
        return grouped
    for row in report.actions:
        if row.scope == "strategy":
            grouped.setdefault(str(row.scope_id), []).append(row)
    return grouped


def _drawdown_proxy(rows: list["TradeOutcome"]) -> float | None:
    returns = [float(row.realized_net_return) for row in rows if row.realized_net_return is not None]
    if not returns:
        return None
    cumulative = pd.Series(1.0 + pd.Series(returns, dtype=float)).cumprod()
    peak = cumulative.cummax()
    drawdown = 1.0 - (cumulative / peak)
    return float(drawdown.max()) if not drawdown.empty else None


def _build_signals_for_strategy(
    *,
    as_of: str,
    strategy_id: str,
    sufficient_samples: bool,
    trade_count: int,
    mean_forecast_gap: float | None,
    drift_rows: list["DriftSignal"],
    calibration_summary: "CalibrationScopeSummary | None",
    risk_trigger_rows: list["RiskControlTrigger"],
    risk_action_rows: list["RiskControlAction"],
    drawdown_proxy: float | None,
) -> list[StrategyDecaySignal]:
    signals: list[StrategyDecaySignal] = []
    if not sufficient_samples:
        signals.append(
            StrategyDecaySignal(
                as_of=as_of,
                strategy_id=strategy_id,
                signal_type="insufficient_data",
                severity="healthy",
                observed_value=float(trade_count),
                threshold=float(MIN_STRATEGY_SAMPLE_COUNT),
                recommended_action="monitor",
                message="strategy decay evaluation has insufficient trade history",
            )
        )
        return signals
    if mean_forecast_gap is not None and abs(mean_forecast_gap) >= 0.06:
        signals.append(
            StrategyDecaySignal(
                as_of=as_of,
                strategy_id=strategy_id,
                signal_type="forecast_gap_decay",
                severity="warning" if abs(mean_forecast_gap) < 0.10 else "critical",
                observed_value=abs(mean_forecast_gap),
                threshold=0.06,
                recommended_action="constrain" if abs(mean_forecast_gap) < 0.10 else "demote_candidate",
                message="strategy expected-vs-realized gap indicates decay pressure",
            )
        )
    if len(drift_rows) >= 2:
        critical_or_warning = sum(1 for row in drift_rows if row.severity in {"warning", "critical"})
        signals.append(
            StrategyDecaySignal(
                as_of=as_of,
                strategy_id=strategy_id,
                signal_type="drift_signal_pressure",
                severity="watch" if critical_or_warning < 2 else "warning",
                observed_value=float(critical_or_warning),
                threshold=2.0,
                recommended_action="review" if critical_or_warning < 2 else "constrain",
                message="strategy drift signals indicate deteriorating behavior",
            )
        )
    if calibration_summary is not None:
        calibration_error = _safe_float(calibration_summary.mean_calibrated_expected_value_error)
        if calibration_error is not None and calibration_error >= 0.04:
            signals.append(
                StrategyDecaySignal(
                    as_of=as_of,
                    strategy_id=strategy_id,
                    signal_type="calibration_decay",
                    severity="watch" if calibration_error < 0.06 else "warning",
                    observed_value=calibration_error,
                    threshold=0.04,
                    recommended_action="review" if calibration_error < 0.06 else "constrain",
                    message="strategy calibration quality has degraded",
                )
            )
    if drawdown_proxy is not None and drawdown_proxy >= 0.08:
        signals.append(
            StrategyDecaySignal(
                as_of=as_of,
                strategy_id=strategy_id,
                signal_type="drawdown_instability",
                severity="watch" if drawdown_proxy < 0.15 else "warning",
                observed_value=drawdown_proxy,
                threshold=0.08,
                recommended_action="review" if drawdown_proxy < 0.15 else "constrain",
                message="strategy realized-return path shows instability",
            )
        )
    if risk_trigger_rows or risk_action_rows:
        signals.append(
            StrategyDecaySignal(
                as_of=as_of,
                strategy_id=strategy_id,
                signal_type="risk_context_pressure",
                severity="watch",
                observed_value=float(len(risk_trigger_rows) + len(risk_action_rows)),
                threshold=1.0,
                recommended_action="review",
                message="strategy has related risk-control context",
            )
        )
    return signals


def build_strategy_decay_summary_report(*, result: "PaperTradingRunResult") -> StrategyDecaySummaryReport:
    outcome_report = result.outcome_attribution_report
    if outcome_report is None or not outcome_report.outcomes:
        return StrategyDecaySummaryReport(
            as_of=result.as_of,
            summary={
                "strategy_count": 0,
                "signal_count": 0,
                "critical_count": 0,
                "warning_count": 0,
                "watch_count": 0,
            },
        )

    attribution_lookup = _attribution_by_trade(result)
    drift_by_strategy = _drift_signals_by_strategy(result)
    calibration_by_strategy = _calibration_summary_by_strategy(result)
    risk_triggers_by_strategy = _risk_triggers_by_strategy(result)
    risk_actions_by_strategy = _risk_actions_by_strategy(result)

    records: list[StrategyDecayRecord] = []
    signals: list[StrategyDecaySignal] = []
    recommendations: list[StrategyLifecycleRecommendation] = []

    for strategy_id, rows in sorted(_strategy_groups(list(outcome_report.outcomes)).items()):
        start, end = _window_bounds(rows)
        trade_count = len(rows)
        sufficient_samples = trade_count >= MIN_STRATEGY_SAMPLE_COUNT
        mean_predicted_net = _mean([row.predicted_net_return for row in rows])
        mean_realized_net = _mean([row.realized_net_return for row in rows])
        trade_attributions = [attribution_lookup.get(str(row.trade_id)) for row in rows]
        mean_forecast_gap = _mean([_safe_float(getattr(row, "forecast_gap", None)) for row in trade_attributions])
        mean_cost_error = _mean([_safe_float(getattr(row, "cost_error", None)) for row in trade_attributions])
        mean_execution_error = _mean([_safe_float(getattr(row, "execution_error", None)) for row in trade_attributions])
        drift_rows = list(drift_by_strategy.get(strategy_id, []))
        drift_warning_or_worse = sum(1 for row in drift_rows if row.severity in {"warning", "critical"})
        calibration_summary = calibration_by_strategy.get(strategy_id)
        risk_trigger_rows = list(risk_triggers_by_strategy.get(strategy_id, []))
        risk_action_rows = list(risk_actions_by_strategy.get(strategy_id, []))
        drawdown_proxy = _drawdown_proxy(rows)

        gap_component = min(abs(mean_forecast_gap or 0.0) / 0.10, 1.0) if sufficient_samples else 0.0
        drift_component = min(float(drift_warning_or_worse) / 3.0, 1.0) if sufficient_samples else 0.0
        calibration_component = min(
            float(_safe_float(getattr(calibration_summary, "mean_calibrated_expected_value_error", None)) or 0.0) / 0.08,
            1.0,
        ) if sufficient_samples else 0.0
        drawdown_component = min(float(drawdown_proxy or 0.0) / 0.20, 1.0) if sufficient_samples else 0.0
        risk_component = min(float(len(risk_trigger_rows) + len(risk_action_rows)) / 4.0, 1.0) if sufficient_samples else 0.0
        decay_score = None
        if sufficient_samples:
            decay_score = float(
                (gap_component * 0.35)
                + (drift_component * 0.20)
                + (calibration_component * 0.20)
                + (drawdown_component * 0.15)
                + (risk_component * 0.10)
            )
        severity = _severity_from_score(decay_score)
        recommended_action = _recommended_action(severity)
        strategy_signals = _build_signals_for_strategy(
            as_of=result.as_of,
            strategy_id=strategy_id,
            sufficient_samples=sufficient_samples,
            trade_count=trade_count,
            mean_forecast_gap=mean_forecast_gap,
            drift_rows=drift_rows,
            calibration_summary=calibration_summary,
            risk_trigger_rows=risk_trigger_rows,
            risk_action_rows=risk_action_rows,
            drawdown_proxy=drawdown_proxy,
        )
        signals.extend(strategy_signals)
        rationale = [row.signal_type for row in strategy_signals if row.signal_type != "insufficient_data"]
        recommendations.append(
            StrategyLifecycleRecommendation(
                as_of=result.as_of,
                strategy_id=strategy_id,
                severity=severity,
                recommended_action=recommended_action,
                decay_score=decay_score,
                sufficient_samples=sufficient_samples,
                rationale=sorted(rationale),
                metadata={"trade_count": trade_count},
            )
        )
        records.append(
            StrategyDecayRecord(
                as_of=result.as_of,
                strategy_id=strategy_id,
                evaluation_window_start=start,
                evaluation_window_end=end,
                trade_count=trade_count,
                sufficient_samples=sufficient_samples,
                mean_predicted_net_return=mean_predicted_net,
                mean_realized_net_return=mean_realized_net,
                mean_forecast_gap=mean_forecast_gap,
                mean_cost_error=mean_cost_error,
                mean_execution_error=mean_execution_error,
                drift_signal_count=len(drift_rows),
                drift_warning_or_worse_count=drift_warning_or_worse,
                calibration_confidence_error=_safe_float(
                    getattr(calibration_summary, "mean_calibrated_confidence_error", None)
                ),
                calibration_expected_value_error=_safe_float(
                    getattr(calibration_summary, "mean_calibrated_expected_value_error", None)
                ),
                risk_trigger_count=len(risk_trigger_rows),
                risk_halted_or_restricted_count=sum(
                    1 for row in risk_trigger_rows if str(row.operating_state) in {"restricted", "halted"}
                ),
                realized_drawdown_proxy=drawdown_proxy,
                decay_score=decay_score,
                severity=severity,
                recommended_action=recommended_action,
                metadata={
                    "regime_breakdown": _regime_breakdown(rows),
                    "signal_types": sorted({row.signal_type for row in strategy_signals}),
                },
            )
        )

    portfolio_decay_score = _mean([row.decay_score for row in records if row.decay_score is not None])
    summary = {
        "strategy_count": len(records),
        "signal_count": len(signals),
        "critical_count": sum(1 for row in records if row.severity == "critical"),
        "warning_count": sum(1 for row in records if row.severity == "warning"),
        "watch_count": sum(1 for row in records if row.severity == "watch"),
        "healthy_count": sum(1 for row in records if row.severity == "healthy"),
        "portfolio_decay_score": portfolio_decay_score,
        "recommended_action_counts": {
            action: sum(1 for row in records if row.recommended_action == action)
            for action in sorted({row.recommended_action for row in records})
        },
    }
    return StrategyDecaySummaryReport(
        as_of=result.as_of,
        records=records,
        signals=signals,
        recommendations=recommendations,
        summary=summary,
    )


def write_strategy_decay_artifacts(*, output_dir: str | Path, report: StrategyDecaySummaryReport) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "strategy_decay_report.json"
    records_path = output_path / "strategy_decay_records.csv"
    signals_path = output_path / "strategy_decay_signals.csv"
    recommendations_path = output_path / "strategy_decay_recommendations.csv"
    summary_path = output_path / "strategy_decay_summary.json"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    summary_path.write_text(json.dumps(report.summary, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in report.records], columns=[row.name for row in fields(StrategyDecayRecord)]).to_csv(
        records_path,
        index=False,
    )
    pd.DataFrame([row.flat_dict() for row in report.signals], columns=[row.name for row in fields(StrategyDecaySignal)]).to_csv(
        signals_path,
        index=False,
    )
    pd.DataFrame(
        [row.flat_dict() for row in report.recommendations],
        columns=[row.name for row in fields(StrategyLifecycleRecommendation)],
    ).to_csv(recommendations_path, index=False)
    return {
        "strategy_decay_report_json_path": json_path,
        "strategy_decay_records_csv_path": records_path,
        "strategy_decay_signals_csv_path": signals_path,
        "strategy_decay_recommendations_csv_path": recommendations_path,
        "strategy_decay_summary_json_path": summary_path,
    }
