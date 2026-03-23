from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.portfolio.strategy_monitoring import load_strategy_monitoring
from trading_platform.portfolio.strategy_portfolio import (
    export_multi_strategy_run_config_bundle,
    load_strategy_portfolio,
)


ADAPTIVE_ALLOCATION_SCHEMA_VERSION = 1
_WEIGHTING_MODES = {"equal_weight", "score_scaled", "performance_tilted", "drawdown_penalized"}
_FALLBACK_MODES = {"prior_weight", "equal_weight"}


@dataclass(frozen=True)
class AdaptiveAllocationPolicyConfig:
    schema_version: int = ADAPTIVE_ALLOCATION_SCHEMA_VERSION
    lookback_window_days: int | None = 30
    weighting_mode: str = "performance_tilted"
    max_upweight_per_cycle: float = 0.10
    max_downweight_per_cycle: float = 0.10
    max_weight_per_strategy: float = 0.50
    min_weight_per_strategy: float = 0.0
    neutral_weight_fallback: str = "prior_weight"
    review_penalty: float = 0.90
    reduce_penalty: float = 0.60
    deactivate_penalty: float = 0.10
    family_diversification_penalty: float = 1.0
    universe_diversification_penalty: float = 1.0
    rebalance_smoothing: float = 0.50
    require_min_observations: int = 5
    max_monitoring_age_days: int | None = 7
    freeze_on_stale_monitoring: bool = True
    freeze_on_low_confidence: bool = False
    dry_run: bool = False
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != ADAPTIVE_ALLOCATION_SCHEMA_VERSION:
            raise ValueError(f"Unsupported adaptive allocation schema_version: {self.schema_version}")
        if self.lookback_window_days is not None and self.lookback_window_days <= 0:
            raise ValueError("lookback_window_days must be > 0 when provided")
        if self.weighting_mode not in _WEIGHTING_MODES:
            raise ValueError(f"weighting_mode must be one of: {sorted(_WEIGHTING_MODES)}")
        if self.max_upweight_per_cycle < 0 or self.max_downweight_per_cycle < 0:
            raise ValueError("max_upweight_per_cycle and max_downweight_per_cycle must be >= 0")
        if self.max_weight_per_strategy <= 0:
            raise ValueError("max_weight_per_strategy must be > 0")
        if self.min_weight_per_strategy < 0:
            raise ValueError("min_weight_per_strategy must be >= 0")
        if self.min_weight_per_strategy > self.max_weight_per_strategy:
            raise ValueError("min_weight_per_strategy must be <= max_weight_per_strategy")
        if self.neutral_weight_fallback not in _FALLBACK_MODES:
            raise ValueError(f"neutral_weight_fallback must be one of: {sorted(_FALLBACK_MODES)}")
        for name, value in {
            "review_penalty": self.review_penalty,
            "reduce_penalty": self.reduce_penalty,
            "deactivate_penalty": self.deactivate_penalty,
            "family_diversification_penalty": self.family_diversification_penalty,
            "universe_diversification_penalty": self.universe_diversification_penalty,
            "rebalance_smoothing": self.rebalance_smoothing,
        }.items():
            if value < 0:
                raise ValueError(f"{name} must be >= 0")
        if self.rebalance_smoothing > 1:
            raise ValueError("rebalance_smoothing must be <= 1")
        if self.require_min_observations < 0:
            raise ValueError("require_min_observations must be >= 0")
        if self.max_monitoring_age_days is not None and self.max_monitoring_age_days < 0:
            raise ValueError("max_monitoring_age_days must be >= 0 when provided")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


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


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _fallback_weight(
    *,
    prior_weight: float,
    strategy_count: int,
    policy: AdaptiveAllocationPolicyConfig,
) -> float:
    if policy.neutral_weight_fallback == "equal_weight" and strategy_count > 0:
        return 1.0 / strategy_count
    return prior_weight


def _recommendation_penalty(recommendation: str, policy: AdaptiveAllocationPolicyConfig) -> float:
    if recommendation == "review":
        return policy.review_penalty
    if recommendation == "reduce":
        return policy.reduce_penalty
    if recommendation == "deactivate":
        return policy.deactivate_penalty
    return 1.0


def _confidence_multiplier(confidence: str) -> float:
    normalized = str(confidence or "").lower()
    if normalized == "high":
        return 1.05
    if normalized == "low":
        return 0.95
    return 1.0


def _performance_multiplier(
    *,
    weighting_mode: str,
    realized_return: float | None,
    realized_sharpe: float | None,
    drawdown: float | None,
) -> tuple[float, str]:
    sharpe = max(min(realized_sharpe or 0.0, 2.0), -2.0)
    pnl = max(min(realized_return or 0.0, 0.25), -0.25)
    realized_drawdown = max(min(drawdown or 0.0, 0.50), 0.0)
    if weighting_mode == "equal_weight":
        return 1.0, "equal_weight_mode"
    if weighting_mode == "score_scaled":
        return max(0.5, 1.0 + (sharpe * 0.10)), "score_scaled_from_realized_sharpe"
    if weighting_mode == "drawdown_penalized":
        return max(0.25, 1.0 - realized_drawdown), "drawdown_penalized_from_realized_drawdown"
    multiplier = 1.0 + (pnl * 0.5) + (sharpe * 0.08) - (realized_drawdown * 0.5)
    return max(0.35, multiplier), "performance_tilted_from_return_sharpe_drawdown"


def _project_weights_with_bounds(
    *,
    names: list[str],
    raw_targets: dict[str, float],
    lower_bounds: dict[str, float],
    upper_bounds: dict[str, float],
) -> tuple[dict[str, float], list[str]]:
    warnings: list[str] = []
    if not names:
        return {}, warnings
    lower_total = sum(lower_bounds[name] for name in names)
    upper_total = sum(upper_bounds[name] for name in names)
    if lower_total > 1.0 + 1e-12 or upper_total < 1.0 - 1e-12:
        raise ValueError("Adaptive allocation bounds are infeasible")

    fixed: dict[str, float] = {}
    remaining = set(names)
    while remaining:
        remaining_weight = max(1.0 - sum(fixed.values()), 0.0)
        raw_total = sum(max(raw_targets[name], 0.0) for name in remaining)
        if raw_total <= 0:
            provisional = {name: remaining_weight / len(remaining) for name in remaining}
            warnings.append("fallback_equal_weight_projection")
        else:
            provisional = {
                name: remaining_weight * (max(raw_targets[name], 0.0) / raw_total)
                for name in remaining
            }

        changed = False
        for name, value in list(provisional.items()):
            if value < lower_bounds[name] - 1e-12:
                fixed[name] = lower_bounds[name]
                remaining.remove(name)
                changed = True
            elif value > upper_bounds[name] + 1e-12:
                fixed[name] = upper_bounds[name]
                remaining.remove(name)
                changed = True
        if not changed:
            fixed.update(provisional)
            break
    return fixed, warnings


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    serialized = dict(row)
    if isinstance(serialized.get("reason_for_adjustment"), list):
        serialized["reason_for_adjustment"] = "|".join(serialized["reason_for_adjustment"])
    return serialized


def build_adaptive_allocation(
    *,
    strategy_portfolio_path: str | Path,
    strategy_monitoring_path: str | Path,
    output_dir: str | Path,
    policy: AdaptiveAllocationPolicyConfig,
) -> dict[str, Any]:
    portfolio = load_strategy_portfolio(strategy_portfolio_path)
    monitoring = load_strategy_monitoring(strategy_monitoring_path)
    selected_rows = list(portfolio.get("selected_strategies", []))
    if not selected_rows:
        raise ValueError("Strategy portfolio contains no selected strategies")

    monitor_lookup = {
        str(row.get("preset_name")): row
        for row in monitoring.get("strategies", [])
        if row.get("preset_name")
    }
    strategy_count = len(selected_rows)
    family_counts = {
        str(row.get("signal_family") or ""): sum(
            1 for item in selected_rows if str(item.get("signal_family") or "") == str(row.get("signal_family") or "")
        )
        for row in selected_rows
        if row.get("signal_family")
    }
    universe_counts = {
        str(row.get("universe") or ""): sum(
            1 for item in selected_rows if str(item.get("universe") or "") == str(row.get("universe") or "")
        )
        for row in selected_rows
        if row.get("universe")
    }
    monitoring_generated_at = _parse_timestamp(monitoring.get("generated_at"))
    monitoring_age_days = (
        (datetime.now(UTC) - monitoring_generated_at).days if monitoring_generated_at is not None else None
    )

    raw_targets: dict[str, float] = {}
    lower_bounds: dict[str, float] = {}
    upper_bounds: dict[str, float] = {}
    row_payloads: list[dict[str, Any]] = []
    warnings: list[str] = []

    for row in selected_rows:
        preset_name = str(row.get("preset_name") or "")
        prior_weight = float(row.get("target_capital_fraction", row.get("allocation_weight", 0.0)) or 0.0)
        monitor_row = monitor_lookup.get(preset_name, {})
        recommendation = str(monitor_row.get("recommendation") or "keep")
        realized_return = _safe_float(monitor_row.get("realized_return", monitor_row.get("proxy_return_contribution")))
        realized_drawdown = _safe_float(monitor_row.get("drawdown", monitor_row.get("proxy_drawdown_contribution")))
        realized_sharpe = _safe_float(monitor_row.get("realized_sharpe"))
        observation_count = int(monitor_row.get("paper_observation_count", monitoring.get("summary", {}).get("observation_count", 0)) or 0)
        attribution_confidence = str(monitor_row.get("attribution_confidence") or "unknown")
        missing_data_days = _safe_float(monitor_row.get("missing_data_days"))

        stale_monitoring = False
        if policy.max_monitoring_age_days is not None and monitoring_age_days is not None:
            stale_monitoring = monitoring_age_days > policy.max_monitoring_age_days
        if policy.max_monitoring_age_days is not None and missing_data_days is not None:
            stale_monitoring = stale_monitoring or missing_data_days > policy.max_monitoring_age_days

        insufficient_data = observation_count < policy.require_min_observations
        low_confidence = attribution_confidence.lower() == "low"
        fallback_weight = _fallback_weight(
            prior_weight=prior_weight,
            strategy_count=strategy_count,
            policy=policy,
        )
        reasons: list[str] = []
        capped_by_policy = False

        if not monitor_row:
            raw_target = fallback_weight
            reasons.append("fallback_missing_monitoring")
        elif stale_monitoring and policy.freeze_on_stale_monitoring:
            raw_target = prior_weight
            reasons.append("frozen_stale_monitoring")
            warnings.append(f"{preset_name}:stale_monitoring")
        elif insufficient_data:
            raw_target = fallback_weight
            reasons.append("fallback_insufficient_observations")
        elif low_confidence and policy.freeze_on_low_confidence:
            raw_target = prior_weight
            reasons.append("frozen_low_attribution_confidence")
        else:
            performance_multiplier, performance_reason = _performance_multiplier(
                weighting_mode=policy.weighting_mode,
                realized_return=realized_return,
                realized_sharpe=realized_sharpe,
                drawdown=realized_drawdown,
            )
            family_penalty = (
                policy.family_diversification_penalty ** max(family_counts.get(str(row.get("signal_family") or ""), 1) - 1, 0)
                if policy.family_diversification_penalty > 0
                else 1.0
            )
            universe_penalty = (
                policy.universe_diversification_penalty ** max(universe_counts.get(str(row.get("universe") or ""), 1) - 1, 0)
                if policy.universe_diversification_penalty > 0
                else 1.0
            )
            recommendation_penalty = _recommendation_penalty(recommendation, policy)
            confidence_multiplier = _confidence_multiplier(attribution_confidence)
            full_multiplier = performance_multiplier * recommendation_penalty * confidence_multiplier * family_penalty * universe_penalty
            smoothed_multiplier = 1.0 + ((full_multiplier - 1.0) * policy.rebalance_smoothing)
            raw_target = prior_weight * max(smoothed_multiplier, 0.0)
            reasons.extend(
                [
                    performance_reason,
                    f"recommendation_penalty:{recommendation}",
                    f"confidence:{attribution_confidence.lower() or 'unknown'}",
                ]
            )
            if family_penalty != 1.0:
                reasons.append("family_diversification_penalty")
            if universe_penalty != 1.0:
                reasons.append("universe_diversification_penalty")

        lower_bound = max(policy.min_weight_per_strategy, prior_weight - policy.max_downweight_per_cycle)
        upper_bound = min(policy.max_weight_per_strategy, prior_weight + policy.max_upweight_per_cycle)
        if stale_monitoring and policy.freeze_on_stale_monitoring:
            lower_bound = prior_weight
            upper_bound = prior_weight
            capped_by_policy = True
        if insufficient_data and policy.neutral_weight_fallback == "prior_weight":
            lower_bound = max(lower_bound, prior_weight - min(policy.max_downweight_per_cycle, 1e-6))
            upper_bound = min(upper_bound, prior_weight + min(policy.max_upweight_per_cycle, 1e-6))
            capped_by_policy = True
        lower_bounds[preset_name] = lower_bound
        upper_bounds[preset_name] = upper_bound
        raw_targets[preset_name] = raw_target
        row_payloads.append(
            {
                "preset_name": preset_name,
                "source_run_id": row.get("source_run_id"),
                "signal_family": row.get("signal_family"),
                "universe": row.get("universe"),
                "prior_weight": prior_weight,
                "adjusted_weight": 0.0,
                "adjustment_factor": None,
                "realized_return": realized_return,
                "realized_drawdown": realized_drawdown,
                "realized_sharpe": realized_sharpe,
                "monitoring_recommendation": recommendation,
                "attribution_confidence": attribution_confidence,
                "reason_for_adjustment": reasons,
                "capped_by_policy": capped_by_policy,
                "observation_count": observation_count,
                "generated_preset_path": row.get("generated_preset_path"),
            }
        )

    projected_weights, projection_warnings = _project_weights_with_bounds(
        names=[row["preset_name"] for row in row_payloads],
        raw_targets=raw_targets,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
    )
    warnings.extend(projection_warnings)

    for row in row_payloads:
        preset_name = row["preset_name"]
        adjusted_weight = float(projected_weights.get(preset_name, 0.0))
        row["adjusted_weight"] = adjusted_weight
        row["target_capital_fraction"] = adjusted_weight
        row["allocation_weight"] = adjusted_weight
        prior_weight = float(row["prior_weight"])
        row["adjustment_factor"] = (adjusted_weight / prior_weight) if prior_weight > 0 else None
        if adjusted_weight <= lower_bounds[preset_name] + 1e-9 or adjusted_weight >= upper_bounds[preset_name] - 1e-9:
            row["capped_by_policy"] = True

    gross_before = float(sum(max(value, 0.0) for value in raw_targets.values()))
    gross_after = float(sum(row["adjusted_weight"] for row in row_payloads))
    absolute_weight_change = float(sum(abs(row["adjusted_weight"] - row["prior_weight"]) for row in row_payloads))
    top_changes = sorted(
        row_payloads,
        key=lambda row: abs(float(row["adjusted_weight"]) - float(row["prior_weight"])),
        reverse=True,
    )

    payload = {
        "schema_version": ADAPTIVE_ALLOCATION_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "dry_run": policy.dry_run,
        "strategy_portfolio_path": str(Path(strategy_portfolio_path)),
        "strategy_monitoring_path": str(Path(strategy_monitoring_path)),
        "policy": asdict(policy),
        "summary": {
            "total_selected_strategies": len(row_payloads),
            "gross_weight_before_normalization": gross_before,
            "gross_weight_after_normalization": gross_after,
            "absolute_weight_change": absolute_weight_change,
            "top_upweight_preset_name": next(
                (
                    row["preset_name"]
                    for row in sorted(
                        row_payloads,
                        key=lambda item: float(item["adjusted_weight"]) - float(item["prior_weight"]),
                        reverse=True,
                    )
                    if float(row["adjusted_weight"]) > float(row["prior_weight"])
                ),
                None,
            ),
            "top_downweight_preset_name": next(
                (
                    row["preset_name"]
                    for row in sorted(
                        row_payloads,
                        key=lambda item: float(item["adjusted_weight"]) - float(item["prior_weight"]),
                    )
                    if float(row["adjusted_weight"]) < float(row["prior_weight"])
                ),
                None,
            ),
            "warning_count": len(sorted(set(warnings))),
            "monitoring_age_days": monitoring_age_days,
            "status": "warning" if warnings else "healthy",
        },
        "strategies": row_payloads,
        "warnings": sorted(set(warnings)),
        "top_changes": [
            {
                "preset_name": row["preset_name"],
                "prior_weight": row["prior_weight"],
                "adjusted_weight": row["adjusted_weight"],
                "delta_weight": float(row["adjusted_weight"]) - float(row["prior_weight"]),
                "monitoring_recommendation": row["monitoring_recommendation"],
            }
            for row in top_changes[:10]
        ],
    }

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "adaptive_allocation.json"
    csv_path = output_path / "adaptive_allocation.csv"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([_serialize_row(row) for row in row_payloads]).to_csv(csv_path, index=False)
    return {
        "adaptive_allocation_json_path": str(json_path),
        "adaptive_allocation_csv_path": str(csv_path),
        "selected_count": len(row_payloads),
        "warning_count": len(payload["warnings"]),
        "absolute_weight_change": absolute_weight_change,
    }


def load_adaptive_allocation(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "adaptive_allocation.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Adaptive allocation artifact not found or invalid: {path}")
    return payload


def export_adaptive_allocation_run_config(
    *,
    adaptive_allocation_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    payload = load_adaptive_allocation(adaptive_allocation_path)
    strategies = list(payload.get("strategies", []))
    if not strategies:
        raise ValueError("Adaptive allocation contains no strategies")
    export_rows = [
        {
            **row,
            "enabled": float(row.get("adjusted_weight", 0.0) or 0.0) > 0,
            "reason_for_adjustment": "|".join(row.get("reason_for_adjustment", []))
            if isinstance(row.get("reason_for_adjustment"), list)
            else row.get("reason_for_adjustment"),
        }
        for row in strategies
    ]
    return export_multi_strategy_run_config_bundle(
        selected_rows=export_rows,
        output_dir=output_dir,
        bundle_name="adaptive_allocation",
        source_artifact_path=adaptive_allocation_path,
        notes="Generated from adaptive_allocation.json",
    )
