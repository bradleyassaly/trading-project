from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.portfolio.adaptive_allocation import load_adaptive_allocation
from trading_platform.portfolio.strategy_monitoring import load_strategy_monitoring
from trading_platform.research.strategy_validation import load_strategy_validation


STRATEGY_LIFECYCLE_SCHEMA_VERSION = 1
LIFECYCLE_STATES = {
    "candidate",
    "validated",
    "promoted",
    "active",
    "under_review",
    "degraded",
    "demoted",
}
TERMINAL_STATES = {"demoted"}


@dataclass(frozen=True)
class StrategyGovernancePolicyConfig:
    schema_version: int = STRATEGY_LIFECYCLE_SCHEMA_VERSION
    demote_after_deactivate_events: int = 2
    demote_after_degraded_cycles: int = 2
    under_review_on_weak_validation: bool = True
    degrade_on_reduce_recommendation: bool = True
    under_review_on_review_recommendation: bool = True
    under_review_on_low_confidence: bool = True
    low_confidence_values: list[str] = field(default_factory=lambda: ["low"])
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != STRATEGY_LIFECYCLE_SCHEMA_VERSION:
            raise ValueError(f"Unsupported strategy governance schema_version: {self.schema_version}")
        if self.demote_after_deactivate_events <= 0:
            raise ValueError("demote_after_deactivate_events must be > 0")
        if self.demote_after_degraded_cycles <= 0:
            raise ValueError("demote_after_degraded_cycles must be > 0")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists() or file_path.is_dir():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _normalize_strategy_id(preset_name: str | None, source_run_id: str | None) -> str:
    return str(preset_name or source_run_id or "")


def load_strategy_lifecycle(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "strategy_lifecycle.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Strategy lifecycle artifact not found or invalid: {path}")
    return payload


def _transition_count(entry: dict[str, Any], state: str) -> int:
    history = entry.get("transition_history", [])
    return sum(1 for item in history if item.get("to_state") == state)


def _append_transition(entry: dict[str, Any], *, new_state: str, reason: str) -> None:
    previous = entry.get("current_state")
    if previous == new_state:
        entry.setdefault("latest_reasons", [])
        if reason not in entry["latest_reasons"]:
            entry["latest_reasons"].append(reason)
        return
    entry.setdefault("transition_history", []).append(
        {
            "timestamp": _now_utc(),
            "from_state": previous,
            "to_state": new_state,
            "reason": reason,
        }
    )
    entry["current_state"] = new_state
    entry["last_transition_at"] = _now_utc()
    entry["latest_reasons"] = [reason]


def update_strategy_lifecycle_state(
    *,
    lifecycle_path: str | Path,
    strategy_id: str,
    new_state: str,
    reason: str,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    if new_state not in LIFECYCLE_STATES:
        raise ValueError(f"Unknown lifecycle state: {new_state}")
    payload = _safe_read_json(lifecycle_path) if Path(lifecycle_path).exists() else {
        "schema_version": STRATEGY_LIFECYCLE_SCHEMA_VERSION,
        "generated_at": None,
        "strategies": [],
    }
    strategies = payload.setdefault("strategies", [])
    entry = next((row for row in strategies if row.get("strategy_id") == strategy_id), None)
    if entry is None:
        entry = {
            "strategy_id": strategy_id,
            "preset_name": strategy_id,
            "source_run_id": None,
            "signal_family": None,
            "universe": None,
            "current_state": "candidate",
            "last_transition_at": None,
            "transition_history": [],
            "latest_reasons": [],
        }
        strategies.append(entry)
    _append_transition(entry, new_state=new_state, reason=reason)
    payload["generated_at"] = _now_utc()
    payload["summary"] = {
        "strategy_count": len(strategies),
        "state_counts": {
            state: sum(1 for row in strategies if row.get("current_state") == state)
            for state in sorted(LIFECYCLE_STATES)
        },
    }
    destination = Path(output_path) if output_path is not None else Path(lifecycle_path)
    json_path = _write_json(destination if destination.suffix else destination / "strategy_lifecycle.json", payload)
    return {"strategy_lifecycle_json_path": str(json_path)}


def apply_strategy_governance(
    *,
    promoted_dir: str | Path,
    strategy_validation_path: str | Path | None,
    strategy_monitoring_path: str | Path | None,
    adaptive_allocation_path: str | Path | None,
    lifecycle_path: str | Path | None,
    output_dir: str | Path,
    policy: StrategyGovernancePolicyConfig,
    dry_run: bool = False,
) -> dict[str, Any]:
    promoted_payload = _safe_read_json(Path(promoted_dir) / "promoted_strategies.json")
    validation_payload = load_strategy_validation(strategy_validation_path) if strategy_validation_path else {"rows": []}
    monitoring_payload = load_strategy_monitoring(strategy_monitoring_path) if strategy_monitoring_path else {"strategies": []}
    adaptive_payload = load_adaptive_allocation(adaptive_allocation_path) if adaptive_allocation_path else {"strategies": []}
    existing_payload = _safe_read_json(lifecycle_path) if lifecycle_path else {}
    strategies = {
        row.get("strategy_id"): row
        for row in existing_payload.get("strategies", [])
        if row.get("strategy_id")
    }
    validation_lookup = {str(row.get("run_id")): row for row in validation_payload.get("rows", []) if row.get("run_id")}
    monitoring_lookup = {
        str(row.get("preset_name")): row
        for row in monitoring_payload.get("strategies", [])
        if row.get("preset_name")
    }
    adaptive_lookup = {
        str(row.get("preset_name")): row
        for row in adaptive_payload.get("strategies", [])
        if row.get("preset_name")
    }
    promoted_rows = list(promoted_payload.get("strategies", []))

    for promoted in promoted_rows:
        strategy_id = _normalize_strategy_id(promoted.get("preset_name"), promoted.get("source_run_id"))
        entry = strategies.setdefault(
            strategy_id,
            {
                "strategy_id": strategy_id,
                "preset_name": promoted.get("preset_name"),
                "source_run_id": promoted.get("source_run_id"),
                "signal_family": promoted.get("signal_family"),
                "universe": promoted.get("universe"),
                "current_state": "candidate",
                "last_transition_at": None,
                "transition_history": [],
                "latest_reasons": [],
            },
        )
        validation_row = validation_lookup.get(str(promoted.get("source_run_id") or ""))
        monitoring_row = monitoring_lookup.get(str(promoted.get("preset_name") or ""))
        adaptive_row = adaptive_lookup.get(str(promoted.get("preset_name") or ""))

        if entry.get("current_state") in TERMINAL_STATES:
            continue

        validation_status = str(validation_row.get("validation_status") or "") if validation_row else ""
        desired_state = "active" if str(promoted.get("status")) == "active" else "promoted"
        reason = "promotion_status"

        if validation_row and validation_status == "pass":
            desired_state = "validated" if desired_state == "promoted" else desired_state
            reason = "validation_pass"
        if validation_row and validation_status == "weak" and policy.under_review_on_weak_validation:
            desired_state = "under_review"
            reason = "weak_validation"
        if monitoring_row:
            recommendation = str(monitoring_row.get("recommendation") or "keep")
            confidence = str(monitoring_row.get("attribution_confidence") or "")
            if recommendation == "deactivate":
                if _transition_count(entry, "degraded") + 1 >= policy.demote_after_deactivate_events:
                    desired_state = "demoted"
                    reason = "repeated_deactivate_recommendation"
                else:
                    desired_state = "degraded"
                    reason = "deactivate_recommendation"
            elif recommendation == "reduce" and policy.degrade_on_reduce_recommendation:
                if _transition_count(entry, "degraded") + 1 >= policy.demote_after_degraded_cycles:
                    desired_state = "demoted"
                    reason = "repeated_degraded_cycles"
                else:
                    desired_state = "degraded"
                    reason = "reduce_recommendation"
            elif recommendation == "review" and policy.under_review_on_review_recommendation:
                desired_state = "under_review"
                reason = "review_recommendation"
            elif policy.under_review_on_low_confidence and confidence.lower() in {
                value.lower() for value in policy.low_confidence_values
            }:
                desired_state = "under_review"
                reason = "low_attribution_confidence"
        if adaptive_row and float(adaptive_row.get("adjusted_weight", 0.0) or 0.0) <= 1e-9 and desired_state not in TERMINAL_STATES:
            desired_state = "degraded"
            reason = "adaptive_zero_weight"

        _append_transition(entry, new_state=desired_state, reason=reason)
        entry["validation_status"] = validation_status or None
        entry["monitoring_recommendation"] = monitoring_row.get("recommendation") if monitoring_row else None
        entry["adaptive_adjusted_weight"] = adaptive_row.get("adjusted_weight") if adaptive_row else None

    for validation_row in validation_payload.get("rows", []):
        strategy_id = _normalize_strategy_id(None, validation_row.get("run_id"))
        entry = strategies.setdefault(
            strategy_id,
            {
                "strategy_id": strategy_id,
                "preset_name": None,
                "source_run_id": validation_row.get("run_id"),
                "signal_family": validation_row.get("signal_family"),
                "universe": validation_row.get("universe"),
                "current_state": "candidate",
                "last_transition_at": None,
                "transition_history": [],
                "latest_reasons": [],
            },
        )
        if entry.get("current_state") in TERMINAL_STATES:
            continue
        validation_status = str(validation_row.get("validation_status") or "")
        if validation_status == "pass":
            _append_transition(entry, new_state="validated", reason="validation_pass")
        elif validation_status == "weak":
            _append_transition(entry, new_state="candidate", reason="weak_validation_candidate")
        else:
            _append_transition(entry, new_state="candidate", reason="validation_fail_candidate")
        entry["validation_status"] = validation_status or None

    strategy_rows = sorted(strategies.values(), key=lambda row: str(row.get("strategy_id") or ""))
    payload = {
        "schema_version": STRATEGY_LIFECYCLE_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "policy": asdict(policy),
        "summary": {
            "strategy_count": len(strategy_rows),
            "demoted_count": sum(1 for row in strategy_rows if row.get("current_state") == "demoted"),
            "degraded_count": sum(1 for row in strategy_rows if row.get("current_state") == "degraded"),
            "under_review_count": sum(1 for row in strategy_rows if row.get("current_state") == "under_review"),
            "active_count": sum(1 for row in strategy_rows if row.get("current_state") == "active"),
        },
        "strategies": strategy_rows,
    }
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    lifecycle_json_path = output_path / "strategy_lifecycle.json"
    lifecycle_csv_path = output_path / "strategy_lifecycle.csv"
    governance_json_path = output_path / "strategy_governance_summary.json"
    if not dry_run:
        _write_json(lifecycle_json_path, payload)
        _write_json(
            governance_json_path,
            {
                "generated_at": _now_utc(),
                "summary": payload["summary"],
                "strategies": [
                    {
                        "strategy_id": row.get("strategy_id"),
                        "current_state": row.get("current_state"),
                        "latest_reasons": row.get("latest_reasons", []),
                    }
                    for row in strategy_rows
                ],
            },
        )
        pd.DataFrame(
            [
                {
                    "strategy_id": row.get("strategy_id"),
                    "preset_name": row.get("preset_name"),
                    "source_run_id": row.get("source_run_id"),
                    "signal_family": row.get("signal_family"),
                    "universe": row.get("universe"),
                    "current_state": row.get("current_state"),
                    "validation_status": row.get("validation_status"),
                    "monitoring_recommendation": row.get("monitoring_recommendation"),
                    "adaptive_adjusted_weight": row.get("adaptive_adjusted_weight"),
                    "last_transition_at": row.get("last_transition_at"),
                    "latest_reasons": "|".join(row.get("latest_reasons", [])),
                }
                for row in strategy_rows
            ]
        ).to_csv(lifecycle_csv_path, index=False)
        if lifecycle_path:
            persistent_path = Path(lifecycle_path)
            if persistent_path.suffix:
                _write_json(persistent_path, payload)
            else:
                _write_json(persistent_path / "strategy_lifecycle.json", payload)
    return {
        "strategy_lifecycle_json_path": str(lifecycle_json_path if not lifecycle_path else (Path(lifecycle_path) if Path(lifecycle_path).suffix else Path(lifecycle_path) / "strategy_lifecycle.json")),
        "strategy_lifecycle_csv_path": str(lifecycle_csv_path),
        "strategy_governance_summary_json_path": str(governance_json_path),
        "demoted_count": payload["summary"]["demoted_count"],
        "degraded_count": payload["summary"]["degraded_count"],
        "under_review_count": payload["summary"]["under_review_count"],
    }
