from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from trading_platform.governance.models import StrategyRegistry, StrategyRegistryAuditEvent, StrategyRegistryEntry
from trading_platform.governance.persistence import append_audit_event, get_registry_entry, upsert_registry_entry
from trading_platform.governance.service import demote_registry_entry

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult
    from trading_platform.reporting.strategy_decay import StrategyDecayRecord
    from trading_platform.risk.controls import RiskControlAction, RiskControlTrigger


STRATEGY_LIFECYCLE_SCHEMA_VERSION = "strategy_lifecycle_v1"
LIFECYCLE_STATES = {"active", "watch", "constrained", "demoted"}
LIFECYCLE_ACTIONS = {"none", "watch", "constrain", "demote", "retrain"}
ACTION_STATUSES = {"proposed", "suppressed_duplicate", "suppressed_cooldown", "no_action"}
MIN_LIFECYCLE_SAMPLE_COUNT = 4
DEFAULT_LIFECYCLE_COOLDOWN_DAYS = 7


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


def _parse_timestamp(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(timestamp) else timestamp


def _days_between(current: str, prior: str | None) -> float | None:
    current_ts = _parse_timestamp(current)
    prior_ts = _parse_timestamp(prior)
    if current_ts is None or prior_ts is None:
        return None
    return float((current_ts - prior_ts).total_seconds() / 86_400.0)


@dataclass(frozen=True)
class StrategyLifecycleState:
    as_of: str
    strategy_id: str
    state: str
    active_for_selection: bool
    constrained: bool = False
    monitoring_level: str = "standard"
    retraining_requested: bool = False
    last_action: str | None = None
    last_action_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.state not in LIFECYCLE_STATES:
            raise ValueError(f"Unsupported strategy lifecycle state: {self.state}")
        if self.last_action is not None and self.last_action not in LIFECYCLE_ACTIONS:
            raise ValueError(f"Unsupported lifecycle last_action: {self.last_action}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "state": self.state,
            "active_for_selection": bool(self.active_for_selection),
            "constrained": bool(self.constrained),
            "monitoring_level": self.monitoring_level,
            "retraining_requested": bool(self.retraining_requested),
            "last_action": self.last_action,
            "last_action_at": self.last_action_at,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class StrategyLifecycleAction:
    as_of: str
    strategy_id: str
    action_type: str
    status: str
    severity: str
    previous_state: str
    proposed_state: str
    final_state: str
    evidence_sources: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    cooldown_applied: bool = False
    deduplicated: bool = False
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action_type not in LIFECYCLE_ACTIONS:
            raise ValueError(f"Unsupported lifecycle action_type: {self.action_type}")
        if self.status not in ACTION_STATUSES:
            raise ValueError(f"Unsupported lifecycle action status: {self.status}")
        if self.previous_state not in LIFECYCLE_STATES or self.proposed_state not in LIFECYCLE_STATES:
            raise ValueError("Unsupported lifecycle state in action")
        if self.final_state not in LIFECYCLE_STATES:
            raise ValueError("Unsupported final lifecycle state")
        object.__setattr__(self, "evidence_sources", [str(item) for item in self.evidence_sources if str(item)])
        object.__setattr__(self, "reason_codes", [str(item) for item in self.reason_codes if str(item)])
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "action_type": self.action_type,
            "status": self.status,
            "severity": self.severity,
            "previous_state": self.previous_state,
            "proposed_state": self.proposed_state,
            "final_state": self.final_state,
            "evidence_sources": list(self.evidence_sources),
            "reason_codes": list(self.reason_codes),
            "cooldown_applied": bool(self.cooldown_applied),
            "deduplicated": bool(self.deduplicated),
            "message": self.message,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["evidence_sources"] = "|".join(self.evidence_sources)
        payload["reason_codes"] = "|".join(self.reason_codes)
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class LifecycleTransitionRecord:
    as_of: str
    strategy_id: str
    previous_state: str
    new_state: str
    triggering_action: str
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.previous_state not in LIFECYCLE_STATES or self.new_state not in LIFECYCLE_STATES:
            raise ValueError("Unsupported lifecycle transition state")
        if self.triggering_action not in LIFECYCLE_ACTIONS:
            raise ValueError(f"Unsupported lifecycle triggering_action: {self.triggering_action}")
        object.__setattr__(self, "reason_codes", [str(item) for item in self.reason_codes if str(item)])
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "triggering_action": self.triggering_action,
            "reason_codes": list(self.reason_codes),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["reason_codes"] = "|".join(self.reason_codes)
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class DemotionDecision:
    as_of: str
    strategy_id: str
    approved_for_demotion: bool
    previous_state: str
    new_state: str
    required_governance_review: bool
    rationale: list[str] = field(default_factory=list)
    evidence_summary: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.previous_state not in LIFECYCLE_STATES or self.new_state not in LIFECYCLE_STATES:
            raise ValueError("Unsupported demotion decision state")
        object.__setattr__(self, "rationale", [str(item) for item in self.rationale if str(item)])
        object.__setattr__(self, "evidence_summary", _normalize_metadata(self.evidence_summary))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "approved_for_demotion": bool(self.approved_for_demotion),
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "required_governance_review": bool(self.required_governance_review),
            "rationale": list(self.rationale),
            "evidence_summary": dict(self.evidence_summary),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["rationale"] = "|".join(self.rationale)
        payload["evidence_summary"] = _flat_dict(self.evidence_summary)
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class RetrainingTrigger:
    trigger_id: str
    as_of: str
    strategy_id: str
    source_action: str
    target_candidate_status: str
    governance_required_before_reactivation: bool
    reason_codes: list[str] = field(default_factory=list)
    handoff_payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.source_action not in LIFECYCLE_ACTIONS:
            raise ValueError(f"Unsupported retraining source_action: {self.source_action}")
        object.__setattr__(self, "reason_codes", [str(item) for item in self.reason_codes if str(item)])
        object.__setattr__(self, "handoff_payload", _normalize_metadata(self.handoff_payload))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "trigger_id": self.trigger_id,
            "as_of": self.as_of,
            "strategy_id": self.strategy_id,
            "source_action": self.source_action,
            "target_candidate_status": self.target_candidate_status,
            "governance_required_before_reactivation": bool(self.governance_required_before_reactivation),
            "reason_codes": list(self.reason_codes),
            "handoff_payload": dict(self.handoff_payload),
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["reason_codes"] = "|".join(self.reason_codes)
        payload["handoff_payload"] = _flat_dict(self.handoff_payload)
        payload["metadata"] = _flat_dict(self.metadata)
        return payload


@dataclass(frozen=True)
class StrategyLifecycleSummaryReport:
    as_of: str
    schema_version: str = STRATEGY_LIFECYCLE_SCHEMA_VERSION
    states: list[StrategyLifecycleState] = field(default_factory=list)
    actions: list[StrategyLifecycleAction] = field(default_factory=list)
    transitions: list[LifecycleTransitionRecord] = field(default_factory=list)
    demotion_decisions: list[DemotionDecision] = field(default_factory=list)
    retraining_triggers: list[RetrainingTrigger] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "states": [row.to_dict() for row in self.states],
            "actions": [row.to_dict() for row in self.actions],
            "transitions": [row.to_dict() for row in self.transitions],
            "demotion_decisions": [row.to_dict() for row in self.demotion_decisions],
            "retraining_triggers": [row.to_dict() for row in self.retraining_triggers],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyLifecycleSummaryReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", STRATEGY_LIFECYCLE_SCHEMA_VERSION)),
            states=[StrategyLifecycleState(**row) for row in data.get("states", [])],
            actions=[StrategyLifecycleAction(**row) for row in data.get("actions", [])],
            transitions=[LifecycleTransitionRecord(**row) for row in data.get("transitions", [])],
            demotion_decisions=[DemotionDecision(**row) for row in data.get("demotion_decisions", [])],
            retraining_triggers=[RetrainingTrigger(**row) for row in data.get("retraining_triggers", [])],
            summary=dict(data.get("summary") or {}),
        )


def _prior_state_lookup(
    prior_states: list[StrategyLifecycleState] | None,
) -> dict[str, StrategyLifecycleState]:
    return {str(row.strategy_id): row for row in prior_states or []}


def _risk_rows_by_strategy(
    result: "PaperTradingRunResult",
) -> tuple[dict[str, list["RiskControlTrigger"]], dict[str, list["RiskControlAction"]]]:
    trigger_groups: dict[str, list["RiskControlTrigger"]] = {}
    action_groups: dict[str, list["RiskControlAction"]] = {}
    report = result.risk_control_report
    if report is None:
        return trigger_groups, action_groups
    for row in report.triggers:
        if row.scope == "strategy":
            trigger_groups.setdefault(str(row.scope_id), []).append(row)
    for row in report.actions:
        if row.scope == "strategy":
            action_groups.setdefault(str(row.scope_id), []).append(row)
    return trigger_groups, action_groups


def _retraining_reasons(
    *,
    record: "StrategyDecayRecord",
    strategy_triggers: list["RiskControlTrigger"],
) -> list[str]:
    reasons: list[str] = []
    forecast_gap = abs(float(record.mean_forecast_gap or 0.0))
    calibration_error = float(record.calibration_expected_value_error or 0.0)
    if forecast_gap >= 0.10:
        reasons.append("persistent_forecast_gap")
    if calibration_error >= 0.06:
        reasons.append("calibration_breakdown")
    if int(record.drift_warning_or_worse_count) >= 2:
        reasons.append("multi_signal_drift")
    if int(record.risk_halted_or_restricted_count) > 0:
        reasons.append("risk_escalation")
    if any(str(row.operating_state) == "halted" for row in strategy_triggers):
        reasons.append("halt_state_strategy_risk")
    return sorted(dict.fromkeys(reasons))


def _state_from_action(action_type: str, previous_state: str) -> str:
    if action_type == "watch":
        return "watch"
    if action_type == "constrain":
        return "constrained"
    if action_type == "demote":
        return "demoted"
    return previous_state


def build_strategy_lifecycle_summary_report(
    *,
    result: "PaperTradingRunResult",
    prior_states: list[StrategyLifecycleState] | None = None,
    cooldown_days: int = DEFAULT_LIFECYCLE_COOLDOWN_DAYS,
) -> StrategyLifecycleSummaryReport:
    decay_report = result.strategy_decay_report
    if decay_report is None or not decay_report.records:
        return StrategyLifecycleSummaryReport(
            as_of=result.as_of,
            summary={
                "strategy_count": 0,
                "action_count": 0,
                "transition_count": 0,
                "demotion_count": 0,
                "retraining_trigger_count": 0,
            },
        )

    prior_lookup = _prior_state_lookup(prior_states)
    trigger_groups, action_groups = _risk_rows_by_strategy(result)
    recommendations = {str(row.strategy_id): row for row in decay_report.recommendations}

    states: list[StrategyLifecycleState] = []
    actions: list[StrategyLifecycleAction] = []
    transitions: list[LifecycleTransitionRecord] = []
    demotion_decisions: list[DemotionDecision] = []
    retraining_triggers: list[RetrainingTrigger] = []

    for record in sorted(decay_report.records, key=lambda row: str(row.strategy_id)):
        recommendation = recommendations.get(str(record.strategy_id))
        previous_state_row = prior_lookup.get(str(record.strategy_id))
        previous_state = previous_state_row.state if previous_state_row is not None else "active"
        last_action = previous_state_row.last_action if previous_state_row is not None else None
        last_action_at = previous_state_row.last_action_at if previous_state_row is not None else None
        days_since_last = _days_between(result.as_of, last_action_at)

        proposed_action = "none"
        reason_codes: list[str] = []
        evidence_sources = ["strategy_decay"]
        if not bool(record.sufficient_samples) or int(record.trade_count) < MIN_LIFECYCLE_SAMPLE_COUNT:
            proposed_action = "none"
            reason_codes.append("insufficient_data")
        elif recommendation is not None:
            if recommendation.recommended_action == "demote_candidate":
                proposed_action = "demote"
                reason_codes.extend(["decay_demote_candidate", f"severity_{record.severity}"])
            elif recommendation.recommended_action == "constrain":
                proposed_action = "constrain"
                reason_codes.extend(["decay_constrain", f"severity_{record.severity}"])
            elif recommendation.recommended_action == "review":
                proposed_action = "watch"
                reason_codes.extend(["decay_review", f"severity_{record.severity}"])
            else:
                reason_codes.extend([f"severity_{record.severity}"])

        strategy_triggers = list(trigger_groups.get(str(record.strategy_id), []))
        strategy_actions = list(action_groups.get(str(record.strategy_id), []))
        if strategy_triggers or strategy_actions:
            evidence_sources.append("risk_controls")
        if proposed_action == "watch" and any(str(row.operating_state) in {"restricted", "halted"} for row in strategy_triggers):
            proposed_action = "constrain"
            reason_codes.append("risk_restriction_context")
        if proposed_action == "none" and any(str(row.operating_state) in {"restricted", "halted"} for row in strategy_triggers):
            proposed_action = "constrain"
            reason_codes.append("risk_restriction_context")

        proposed_state = _state_from_action(proposed_action, previous_state)
        status = "proposed"
        cooldown_applied = False
        deduplicated = False
        final_state = proposed_state
        if proposed_action == "none":
            status = "no_action"
            final_state = previous_state
        elif proposed_action == last_action and days_since_last is not None and days_since_last < float(cooldown_days):
            status = "suppressed_cooldown"
            cooldown_applied = True
            final_state = previous_state
        elif previous_state == proposed_state:
            status = "suppressed_duplicate"
            deduplicated = True
            final_state = previous_state

        message = (
            "strategy lifecycle action suppressed"
            if status.startswith("suppressed")
            else "strategy lifecycle action proposed"
        )
        action = StrategyLifecycleAction(
            as_of=result.as_of,
            strategy_id=str(record.strategy_id),
            action_type=proposed_action,
            status=status,
            severity=str(record.severity),
            previous_state=previous_state,
            proposed_state=proposed_state,
            final_state=final_state,
            evidence_sources=sorted(dict.fromkeys(evidence_sources)),
            reason_codes=sorted(dict.fromkeys(reason_codes)),
            cooldown_applied=cooldown_applied,
            deduplicated=deduplicated,
            message=message,
            metadata={
                "trade_count": int(record.trade_count),
                "decay_score": _safe_float(record.decay_score),
            },
        )
        actions.append(action)

        retraining_reason_codes: list[str] = []
        if proposed_action == "demote":
            retraining_reason_codes = _retraining_reasons(record=record, strategy_triggers=strategy_triggers)
            demotion_decisions.append(
                DemotionDecision(
                    as_of=result.as_of,
                    strategy_id=str(record.strategy_id),
                    approved_for_demotion=status == "proposed",
                    previous_state=previous_state,
                    new_state="demoted" if status == "proposed" else previous_state,
                    required_governance_review=True,
                    rationale=sorted(dict.fromkeys(reason_codes)),
                    evidence_summary={
                        "trade_count": int(record.trade_count),
                        "decay_score": _safe_float(record.decay_score),
                        "forecast_gap": _safe_float(record.mean_forecast_gap),
                        "risk_trigger_count": int(record.risk_trigger_count),
                    },
                    metadata={"retraining_candidate": bool(retraining_reason_codes)},
                )
            )

        if status == "proposed" and proposed_action != "none" and previous_state != final_state:
            transitions.append(
                LifecycleTransitionRecord(
                    as_of=result.as_of,
                    strategy_id=str(record.strategy_id),
                    previous_state=previous_state,
                    new_state=final_state,
                    triggering_action=proposed_action,
                    reason_codes=sorted(dict.fromkeys(reason_codes)),
                    metadata={"severity": record.severity},
                )
            )

        retraining_requested = False
        if retraining_reason_codes and not cooldown_applied:
            retraining_requested = True
            retraining_triggers.append(
                RetrainingTrigger(
                    trigger_id=f"{record.strategy_id}|{result.as_of}|retrain",
                    as_of=result.as_of,
                    strategy_id=str(record.strategy_id),
                    source_action="retrain",
                    target_candidate_status="candidate",
                    governance_required_before_reactivation=True,
                    reason_codes=retraining_reason_codes,
                    handoff_payload={
                        "source_report": "strategy_lifecycle_report",
                        "required_next_step": "normal_promotion_flow",
                        "governance_rule": "retraining_does_not_reactivate_without_promotion",
                    },
                    metadata={
                        "linked_decay_score": _safe_float(record.decay_score),
                        "linked_demotion_action_status": status,
                    },
                )
            )

        states.append(
            StrategyLifecycleState(
                as_of=result.as_of,
                strategy_id=str(record.strategy_id),
                state=final_state,
                active_for_selection=final_state != "demoted",
                constrained=final_state == "constrained",
                monitoring_level="elevated" if final_state in {"watch", "constrained"} else "standard",
                retraining_requested=retraining_requested,
                last_action=proposed_action if proposed_action != "none" else last_action,
                last_action_at=result.as_of if proposed_action != "none" and status == "proposed" else last_action_at,
                metadata={
                    "severity": record.severity,
                    "trade_count": int(record.trade_count),
                    "prior_state": previous_state,
                },
            )
        )

    summary = {
        "strategy_count": len(states),
        "action_count": len(actions),
        "transition_count": len(transitions),
        "demotion_count": sum(1 for row in demotion_decisions if row.approved_for_demotion),
        "retraining_trigger_count": len(retraining_triggers),
        "watch_count": sum(1 for row in states if row.state == "watch"),
        "constrained_count": sum(1 for row in states if row.state == "constrained"),
        "demoted_count": sum(1 for row in states if row.state == "demoted"),
        "suppressed_action_count": sum(1 for row in actions if row.status.startswith("suppressed")),
    }
    return StrategyLifecycleSummaryReport(
        as_of=result.as_of,
        states=states,
        actions=actions,
        transitions=transitions,
        demotion_decisions=demotion_decisions,
        retraining_triggers=retraining_triggers,
        summary=summary,
    )


def _update_entry_metadata(entry: StrategyRegistryEntry, metadata_updates: dict[str, Any]) -> StrategyRegistryEntry:
    new_metadata = dict(entry.metadata or {})
    new_metadata.update(metadata_updates)
    return StrategyRegistryEntry(**{**entry.to_dict(), "metadata": new_metadata})


def apply_strategy_lifecycle_report_to_registry(
    *,
    registry: StrategyRegistry,
    report: StrategyLifecycleSummaryReport,
    note_prefix: str = "strategy lifecycle policy",
) -> StrategyRegistry:
    updated = registry
    trigger_by_strategy = {str(row.strategy_id): row for row in report.retraining_triggers}
    demotion_by_strategy = {str(row.strategy_id): row for row in report.demotion_decisions}
    state_by_strategy = {str(row.strategy_id): row for row in report.states}

    for strategy_id in sorted(set(state_by_strategy) | set(trigger_by_strategy) | set(demotion_by_strategy)):
        entry = get_registry_entry(updated, strategy_id)
        state = state_by_strategy.get(strategy_id)
        demotion = demotion_by_strategy.get(strategy_id)
        retraining = trigger_by_strategy.get(strategy_id)

        if demotion is not None and demotion.approved_for_demotion and entry.status in {"approved", "paper", "candidate", "live_disabled"}:
            updated = demote_registry_entry(
                registry=updated,
                strategy_id=strategy_id,
                note=f"{note_prefix}: auto-demotion from lifecycle report",
            )
            entry = get_registry_entry(updated, strategy_id)

        metadata_updates: dict[str, Any] = {}
        if state is not None:
            metadata_updates["lifecycle_state"] = state.state
            metadata_updates["lifecycle_active_for_selection"] = bool(state.active_for_selection)
            metadata_updates["lifecycle_last_action"] = state.last_action
            metadata_updates["lifecycle_last_action_at"] = state.last_action_at
        if retraining is not None:
            metadata_updates["retraining_requested"] = True
            metadata_updates["retraining_trigger_id"] = retraining.trigger_id
            metadata_updates["retraining_target_candidate_status"] = retraining.target_candidate_status
            metadata_updates["retraining_governance_required_before_reactivation"] = True
        if metadata_updates:
            updated_entry = _update_entry_metadata(entry, metadata_updates)
            updated = upsert_registry_entry(updated, updated_entry)
        if retraining is not None:
            updated = append_audit_event(
                updated,
                StrategyRegistryAuditEvent(
                    timestamp=report.as_of,
                    strategy_id=strategy_id,
                    action="retrain_requested",
                    from_status=get_registry_entry(updated, strategy_id).status,
                    to_status=get_registry_entry(updated, strategy_id).status,
                    note=(
                        f"{note_prefix}: retraining requested; resulting candidate must pass normal promotion "
                        f"before reactivation"
                    ),
                ),
            )
    return updated


def write_strategy_lifecycle_artifacts(
    *,
    output_dir: str | Path,
    report: StrategyLifecycleSummaryReport,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_path = output_path / "strategy_lifecycle_report.json"
    states_path = output_path / "strategy_lifecycle_states.csv"
    actions_path = output_path / "strategy_lifecycle_actions.csv"
    transitions_path = output_path / "strategy_lifecycle_transitions.csv"
    demotions_path = output_path / "strategy_lifecycle_demotions.csv"
    retraining_path = output_path / "strategy_lifecycle_retraining_triggers.csv"
    summary_path = output_path / "strategy_lifecycle_summary.json"
    report_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    summary_path.write_text(json.dumps(report.summary, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in report.states], columns=[row.name for row in fields(StrategyLifecycleState)]).to_csv(
        states_path,
        index=False,
    )
    pd.DataFrame([row.flat_dict() for row in report.actions], columns=[row.name for row in fields(StrategyLifecycleAction)]).to_csv(
        actions_path,
        index=False,
    )
    pd.DataFrame(
        [row.flat_dict() for row in report.transitions],
        columns=[row.name for row in fields(LifecycleTransitionRecord)],
    ).to_csv(transitions_path, index=False)
    pd.DataFrame(
        [row.flat_dict() for row in report.demotion_decisions],
        columns=[row.name for row in fields(DemotionDecision)],
    ).to_csv(demotions_path, index=False)
    pd.DataFrame(
        [row.flat_dict() for row in report.retraining_triggers],
        columns=[row.name for row in fields(RetrainingTrigger)],
    ).to_csv(retraining_path, index=False)
    return {
        "strategy_lifecycle_report_json_path": report_path,
        "strategy_lifecycle_states_csv_path": states_path,
        "strategy_lifecycle_actions_csv_path": actions_path,
        "strategy_lifecycle_transitions_csv_path": transitions_path,
        "strategy_lifecycle_demotions_csv_path": demotions_path,
        "strategy_lifecycle_retraining_triggers_csv_path": retraining_path,
        "strategy_lifecycle_summary_json_path": summary_path,
    }
