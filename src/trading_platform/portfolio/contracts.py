from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _normalize_string_list(value: list[Any] | tuple[Any, ...] | str | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    return [str(value)] if str(value) else []


def _normalize_metadata(value: dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    return {str(key): value[key] for key in sorted(value)}


def _normalize_float_map(value: dict[str, Any] | None) -> dict[str, float]:
    if value is None:
        return {}
    normalized: dict[str, float] = {}
    for key in sorted(value):
        normalized[str(key)] = float(value[key] or 0.0)
    return normalized


@dataclass(frozen=True)
class StrategyPortfolioInput:
    sleeve_name: str
    preset_name: str
    as_of: str
    capital_weight_raw: float
    capital_weight_normalized: float
    scheduled_target_weights: dict[str, float] = field(default_factory=dict)
    effective_target_weights: dict[str, float] = field(default_factory=dict)
    latest_prices: dict[str, float] = field(default_factory=dict)
    latest_scores: dict[str, float] = field(default_factory=dict)
    skipped_symbols: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.sleeve_name or "").strip():
            raise ValueError("sleeve_name must be a non-empty string")
        if not str(self.preset_name or "").strip():
            raise ValueError("preset_name must be a non-empty string")
        if not str(self.as_of or "").strip():
            raise ValueError("as_of must be a non-empty string")
        object.__setattr__(self, "scheduled_target_weights", _normalize_float_map(self.scheduled_target_weights))
        object.__setattr__(self, "effective_target_weights", _normalize_float_map(self.effective_target_weights))
        object.__setattr__(self, "latest_prices", _normalize_float_map(self.latest_prices))
        object.__setattr__(self, "latest_scores", _normalize_float_map(self.latest_scores))
        object.__setattr__(self, "skipped_symbols", _normalize_string_list(self.skipped_symbols))
        object.__setattr__(self, "diagnostics", _normalize_metadata(self.diagnostics))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyPortfolioInput":
        data = dict(payload or {})
        data.setdefault("scheduled_target_weights", {})
        data.setdefault("effective_target_weights", {})
        data.setdefault("latest_prices", {})
        data.setdefault("latest_scores", {})
        data.setdefault("skipped_symbols", [])
        data.setdefault("diagnostics", {})
        data.setdefault("metadata", {})
        return cls(
            sleeve_name=str(data["sleeve_name"]),
            preset_name=str(data["preset_name"]),
            as_of=str(data["as_of"]),
            capital_weight_raw=float(data["capital_weight_raw"]),
            capital_weight_normalized=float(data["capital_weight_normalized"]),
            scheduled_target_weights=_normalize_float_map(data.get("scheduled_target_weights")),
            effective_target_weights=_normalize_float_map(data.get("effective_target_weights")),
            latest_prices=_normalize_float_map(data.get("latest_prices")),
            latest_scores=_normalize_float_map(data.get("latest_scores")),
            skipped_symbols=_normalize_string_list(data.get("skipped_symbols")),
            diagnostics=_normalize_metadata(data.get("diagnostics")),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sleeve_name": self.sleeve_name,
            "preset_name": self.preset_name,
            "as_of": self.as_of,
            "capital_weight_raw": float(self.capital_weight_raw),
            "capital_weight_normalized": float(self.capital_weight_normalized),
            "scheduled_target_weights": dict(self.scheduled_target_weights),
            "effective_target_weights": dict(self.effective_target_weights),
            "latest_prices": dict(self.latest_prices),
            "latest_scores": dict(self.latest_scores),
            "skipped_symbols": list(self.skipped_symbols),
            "diagnostics": dict(self.diagnostics),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ConflictResolutionRecord:
    symbol: str
    resolution_rule: str
    overlap_type: str
    sleeve_count: int
    gross_weight_before: float
    net_weight_after: float
    conflicting_sleeves: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.symbol or "").strip():
            raise ValueError("symbol must be a non-empty string")
        if not str(self.resolution_rule or "").strip():
            raise ValueError("resolution_rule must be a non-empty string")
        if self.overlap_type not in {"single", "overlap", "conflict"}:
            raise ValueError("overlap_type must be one of: single, overlap, conflict")
        object.__setattr__(self, "conflicting_sleeves", _normalize_string_list(self.conflicting_sleeves))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ConflictResolutionRecord":
        data = dict(payload or {})
        data.setdefault("conflicting_sleeves", [])
        data.setdefault("metadata", {})
        return cls(
            symbol=str(data["symbol"]),
            resolution_rule=str(data["resolution_rule"]),
            overlap_type=str(data["overlap_type"]),
            sleeve_count=int(data["sleeve_count"]),
            gross_weight_before=float(data["gross_weight_before"]),
            net_weight_after=float(data["net_weight_after"]),
            conflicting_sleeves=_normalize_string_list(data.get("conflicting_sleeves")),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "resolution_rule": self.resolution_rule,
            "overlap_type": self.overlap_type,
            "sleeve_count": int(self.sleeve_count),
            "gross_weight_before": float(self.gross_weight_before),
            "net_weight_after": float(self.net_weight_after),
            "conflicting_sleeves": list(self.conflicting_sleeves),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ExposureConstraintDecision:
    constraint_name: str
    scope: str
    binding: bool
    before_weight: float
    after_weight: float
    affected_symbol: str | None = None
    action: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.constraint_name or "").strip():
            raise ValueError("constraint_name must be a non-empty string")
        if not str(self.scope or "").strip():
            raise ValueError("scope must be a non-empty string")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExposureConstraintDecision":
        data = dict(payload or {})
        data.setdefault("affected_symbol", None)
        data.setdefault("action", None)
        data.setdefault("metadata", {})
        return cls(
            constraint_name=str(data["constraint_name"]),
            scope=str(data["scope"]),
            binding=bool(data["binding"]),
            before_weight=float(data["before_weight"]),
            after_weight=float(data["after_weight"]),
            affected_symbol=str(data["affected_symbol"]) if data.get("affected_symbol") is not None else None,
            action=str(data["action"]) if data.get("action") is not None else None,
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "constraint_name": self.constraint_name,
            "scope": self.scope,
            "binding": self.binding,
            "before_weight": float(self.before_weight),
            "after_weight": float(self.after_weight),
            "affected_symbol": self.affected_symbol,
            "action": self.action,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class AllocationRationaleRecord:
    symbol: str
    final_target_weight: float
    rationale_codes: list[str] = field(default_factory=list)
    source_sleeves: list[str] = field(default_factory=list)
    constraint_actions: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.symbol or "").strip():
            raise ValueError("symbol must be a non-empty string")
        object.__setattr__(self, "rationale_codes", _normalize_string_list(self.rationale_codes))
        object.__setattr__(self, "source_sleeves", _normalize_string_list(self.source_sleeves))
        object.__setattr__(self, "constraint_actions", _normalize_string_list(self.constraint_actions))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AllocationRationaleRecord":
        data = dict(payload or {})
        data.setdefault("rationale_codes", [])
        data.setdefault("source_sleeves", [])
        data.setdefault("constraint_actions", [])
        data.setdefault("metadata", {})
        return cls(
            symbol=str(data["symbol"]),
            final_target_weight=float(data["final_target_weight"]),
            rationale_codes=_normalize_string_list(data.get("rationale_codes")),
            source_sleeves=_normalize_string_list(data.get("source_sleeves")),
            constraint_actions=_normalize_string_list(data.get("constraint_actions")),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "final_target_weight": float(self.final_target_weight),
            "rationale_codes": list(self.rationale_codes),
            "source_sleeves": list(self.source_sleeves),
            "constraint_actions": list(self.constraint_actions),
            "metadata": dict(self.metadata),
        }
