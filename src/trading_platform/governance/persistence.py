from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.governance.models import (
    GovernanceCriteriaConfig,
    PromotionCriteria,
    DegradationCriteria,
    STATUS_TRANSITIONS,
    StrategyRegistry,
    StrategyRegistryAuditEvent,
    StrategyRegistryEntry,
)

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _read_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError(
                "PyYAML is required for YAML files. Install with `pip install pyyaml`."
            )
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}
    raise ValueError(f"Unsupported file type: {suffix}")


def _write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".json":
        path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        return
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError(
                "PyYAML is required for YAML files. Install with `pip install pyyaml`."
            )
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return
    raise ValueError(f"Unsupported file type: {suffix}")


def load_strategy_registry(path: str | Path) -> StrategyRegistry:
    file_path = Path(path)
    payload = _read_payload(file_path)
    entries = [
        StrategyRegistryEntry(**entry_payload)
        for entry_payload in payload.get("entries", [])
    ]
    audit_log = [
        StrategyRegistryAuditEvent(**event_payload)
        for event_payload in payload.get("audit_log", [])
    ]
    updated_at = str(payload.get("updated_at") or "")
    return StrategyRegistry(
        schema_version=int(payload.get("schema_version", 1)),
        updated_at=updated_at,
        entries=entries,
        audit_log=audit_log,
    )


def save_strategy_registry(registry: StrategyRegistry, path: str | Path) -> Path:
    file_path = Path(path)
    payload = registry.to_dict()
    payload["updated_at"] = registry.updated_at or _now_utc()
    _write_payload(file_path, payload)
    return file_path


def load_governance_criteria_config(path: str | Path) -> GovernanceCriteriaConfig:
    payload = _read_payload(Path(path))
    return GovernanceCriteriaConfig(
        promotion=PromotionCriteria(**payload.get("promotion", {})),
        degradation=DegradationCriteria(**payload.get("degradation", {})),
    )


def get_registry_entry(
    registry: StrategyRegistry,
    strategy_id: str,
) -> StrategyRegistryEntry:
    for entry in registry.entries:
        if entry.strategy_id == strategy_id:
            return entry
    raise ValueError(f"Unknown strategy_id: {strategy_id}")


def upsert_registry_entry(
    registry: StrategyRegistry,
    entry: StrategyRegistryEntry,
) -> StrategyRegistry:
    updated_entries = [item for item in registry.entries if item.strategy_id != entry.strategy_id]
    updated_entries.append(entry)
    updated_entries = sorted(updated_entries, key=lambda item: item.strategy_id)
    return StrategyRegistry(
        schema_version=registry.schema_version,
        updated_at=_now_utc(),
        entries=updated_entries,
        audit_log=registry.audit_log,
    )


def append_audit_event(
    registry: StrategyRegistry,
    event: StrategyRegistryAuditEvent,
) -> StrategyRegistry:
    return StrategyRegistry(
        schema_version=registry.schema_version,
        updated_at=_now_utc(),
        entries=registry.entries,
        audit_log=[*registry.audit_log, event],
    )


def validate_status_transition(
    *,
    from_status: str,
    to_status: str,
) -> None:
    allowed = STATUS_TRANSITIONS.get(from_status, set())
    if to_status not in allowed:
        raise ValueError(f"Invalid status transition: {from_status} -> {to_status}")
