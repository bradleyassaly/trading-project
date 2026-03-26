from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.models import MultiStrategyPortfolioConfig, MultiStrategySleeveConfig
from trading_platform.orchestration.models import OrchestrationStageToggles, PipelineRunConfig

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


STRATEGY_PORTFOLIO_SCHEMA_VERSION = 1
_WEIGHTING_MODE_ALIASES = {
    "equal": "equal_weight",
    "metric_proportional": "metric_weighted",
}
_VALID_WEIGHTING_MODES = {
    "equal_weight",
    "metric_weighted",
    "capped_metric_weighted",
    "inverse_count_by_signal_family",
    "score_then_cap",
}


@dataclass(frozen=True)
class StrategyPortfolioPolicyConfig:
    schema_version: int = STRATEGY_PORTFOLIO_SCHEMA_VERSION
    max_strategies: int | None = 5
    max_strategies_per_signal_family: int | None = 1
    max_strategies_per_universe: int | None = None
    min_families_if_available: int = 0
    max_weight_per_strategy: float = 0.5
    min_weight_per_strategy: float = 0.0
    selection_metric: str = "ranking_value"
    weighting_mode: str = "equal"
    metric_weight_cap_multiple: float = 1.5
    weighting_smoothing_power: float = 1.0
    require_active_only: bool = False
    require_promotion_eligible_only: bool = True
    deduplicate_source_runs: bool = True
    include_conditional_strategies: bool = True
    max_conditional_strategies_total: int | None = None
    max_conditional_strategies_per_family: int | None = None
    require_baseline_for_conditional: bool = False
    conditional_weight_multiplier: float = 1.0
    conditional_selection_mode: str = "include"
    evaluate_conditional_activation: bool = False
    activation_context_sources: list[str] = field(default_factory=lambda: ["regime", "benchmark_context"])
    include_inactive_conditionals_in_output: bool = True
    allow_conditional_variant_siblings: bool = False
    conditional_variant_score_bonus: float = 0.0
    diversification_dimension: str = "signal_family"
    fallback_equal_weight_mode: bool = True
    warn_on_same_family_overlap: bool = True
    output_inactive_status: str = "inactive"
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != STRATEGY_PORTFOLIO_SCHEMA_VERSION:
            raise ValueError(f"Unsupported strategy portfolio schema_version: {self.schema_version}")
        if self.max_strategies is not None and self.max_strategies <= 0:
            raise ValueError("max_strategies must be > 0 when provided")
        if self.max_strategies_per_signal_family is not None and self.max_strategies_per_signal_family <= 0:
            raise ValueError("max_strategies_per_signal_family must be > 0 when provided")
        if self.max_strategies_per_universe is not None and self.max_strategies_per_universe <= 0:
            raise ValueError("max_strategies_per_universe must be > 0 when provided")
        if self.max_conditional_strategies_total is not None and self.max_conditional_strategies_total <= 0:
            raise ValueError("max_conditional_strategies_total must be > 0 when provided")
        if self.max_conditional_strategies_per_family is not None and self.max_conditional_strategies_per_family <= 0:
            raise ValueError("max_conditional_strategies_per_family must be > 0 when provided")
        if self.min_families_if_available < 0:
            raise ValueError("min_families_if_available must be >= 0")
        if self.max_weight_per_strategy <= 0:
            raise ValueError("max_weight_per_strategy must be > 0")
        if self.min_weight_per_strategy < 0:
            raise ValueError("min_weight_per_strategy must be >= 0")
        if self.min_weight_per_strategy > self.max_weight_per_strategy:
            raise ValueError("min_weight_per_strategy must be <= max_weight_per_strategy")
        resolved_weighting_mode = _resolve_weighting_mode(self.weighting_mode)
        if resolved_weighting_mode not in _VALID_WEIGHTING_MODES:
            raise ValueError(
                "weighting_mode must be one of: equal, metric_proportional, "
                "equal_weight, metric_weighted, capped_metric_weighted, "
                "inverse_count_by_signal_family, score_then_cap"
            )
        if self.metric_weight_cap_multiple <= 0:
            raise ValueError("metric_weight_cap_multiple must be > 0")
        if self.conditional_weight_multiplier < 0:
            raise ValueError("conditional_weight_multiplier must be >= 0")
        if self.weighting_smoothing_power <= 0:
            raise ValueError("weighting_smoothing_power must be > 0")
        if self.diversification_dimension not in {"none", "signal_family", "universe"}:
            raise ValueError("diversification_dimension must be one of: none, signal_family, universe")
        if self.conditional_selection_mode not in {"include", "separate_bucket", "shadow_only"}:
            raise ValueError("conditional_selection_mode must be one of: include, separate_bucket, shadow_only")
        allowed_sources = {"regime", "benchmark_context", "sub_universe"}
        if any(str(source) not in allowed_sources for source in self.activation_context_sources):
            raise ValueError("activation_context_sources must be drawn from: regime, benchmark_context, sub_universe")


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


def _write_payload(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".json":
        path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        return path
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML output. Install with `pip install pyyaml`.")
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return path
    raise ValueError(f"Unsupported output file type: {suffix}")


def _resolve_weighting_mode(weighting_mode: str) -> str:
    normalized = str(weighting_mode or "").strip()
    return _WEIGHTING_MODE_ALIASES.get(normalized, normalized)


def _load_promoted_strategies(promoted_dir: str | Path) -> list[dict[str, Any]]:
    payload = _safe_read_json(Path(promoted_dir) / "promoted_strategies.json")
    return list(payload.get("strategies", []))


def _row_metric_value(row: dict[str, Any], metric_name: str) -> float | None:
    if metric_name in row:
        return _safe_float(row.get(metric_name))
    if row.get("ranking_metric") == metric_name:
        return _safe_float(row.get("ranking_value"))
    if metric_name == "ranking_value":
        return _safe_float(row.get("ranking_value"))
    return None


def _is_conditional_variant(row: dict[str, Any]) -> bool:
    return bool(
        row.get("condition_id")
        or row.get("condition_type")
        or row.get("promotion_variant") == "conditional"
    )


def _activation_state_for_row(
    row: dict[str, Any],
    *,
    conditional_selection_mode: str,
) -> str:
    if not _is_conditional_variant(row):
        return "always_on"
    if conditional_selection_mode == "shadow_only":
        return "shadow_only"
    return "inactive_until_condition_match"


def _rank_candidates(
    rows: list[dict[str, Any]],
    metric_name: str,
    *,
    conditional_variant_score_bonus: float = 0.0,
) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            (_row_metric_value(row, metric_name) or float("-inf"))
            + (conditional_variant_score_bonus if _is_conditional_variant(row) else 0.0),
            str(row.get("promotion_timestamp") or ""),
            str(row.get("preset_name") or ""),
        ),
        reverse=True,
    )


def _passes_filters(row: dict[str, Any], policy: StrategyPortfolioPolicyConfig) -> tuple[bool, str | None]:
    if policy.require_active_only and row.get("status") != "active":
        return False, "status_not_active"
    if policy.require_promotion_eligible_only and not row.get("source_run_id"):
        return False, "missing_source_run_id"
    return True, None


def _build_base_weights(rows: list[dict[str, Any]], policy: StrategyPortfolioPolicyConfig) -> dict[str, float]:
    if not rows:
        return {}
    weighting_mode = _resolve_weighting_mode(policy.weighting_mode)
    if weighting_mode == "equal_weight":
        return {str(row["preset_name"]): 1.0 for row in rows}
    if weighting_mode == "inverse_count_by_signal_family":
        family_counts: dict[str, int] = {}
        for row in rows:
            family = str(row.get("signal_family") or "unknown")
            family_counts[family] = family_counts.get(family, 0) + 1
        return {
            str(row["preset_name"]): 1.0 / max(family_counts.get(str(row.get("signal_family") or "unknown"), 1), 1)
            for row in rows
        }
    if weighting_mode == "score_then_cap":
        ranked = _rank_candidates(
            rows,
            policy.selection_metric,
            conditional_variant_score_bonus=policy.conditional_variant_score_bonus,
        )
        count = len(ranked)
        return {
            str(row["preset_name"]): float(count - index)
            for index, row in enumerate(ranked)
        }
    raw = {}
    for row in rows:
        name = str(row["preset_name"])
        value = max(_row_metric_value(row, policy.selection_metric) or 0.0, 0.0)
        if _is_conditional_variant(row):
            value *= policy.conditional_weight_multiplier
        raw[name] = value
    if policy.weighting_smoothing_power != 1.0:
        raw = {
            name: value ** policy.weighting_smoothing_power if value > 0 else value
            for name, value in raw.items()
        }
    if weighting_mode == "capped_metric_weighted":
        positive_metrics = sorted(value for value in raw.values() if value > 0)
        if positive_metrics:
            median_metric = float(pd.Series(positive_metrics).median())
            metric_cap = median_metric * policy.metric_weight_cap_multiple
            raw = {
                name: min(value, metric_cap) if value > 0 else value
                for name, value in raw.items()
            }
    total = sum(raw.values())
    if total <= 0 and policy.fallback_equal_weight_mode:
        return {str(row["preset_name"]): 1.0 for row in rows}
    return raw


def _summarize_family_weights(selected_rows: list[dict[str, Any]]) -> dict[str, float]:
    family_weights: dict[str, float] = {}
    for row in selected_rows:
        family = str(row.get("signal_family") or "unknown")
        family_weights[family] = family_weights.get(family, 0.0) + float(row.get("allocation_weight") or 0.0)
    return dict(sorted(family_weights.items()))


def _effective_count(weights: list[float]) -> float:
    positive = [float(weight) for weight in weights if float(weight) > 0]
    if not positive:
        return 0.0
    total = sum(positive)
    normalized = [weight / total for weight in positive]
    return float(1.0 / sum(weight * weight for weight in normalized))


def _normalize_with_caps(
    rows: list[dict[str, Any]],
    policy: StrategyPortfolioPolicyConfig,
) -> tuple[dict[str, float], list[str]]:
    warnings: list[str] = []
    if not rows:
        return {}, warnings
    if len(rows) * policy.min_weight_per_strategy > 1.0 + 1e-12:
        raise ValueError("min_weight_per_strategy is too large for the number of selected strategies")

    raw = _build_base_weights(rows, policy)
    total = sum(raw.values())
    if total <= 0:
        raw = {str(row["preset_name"]): 1.0 for row in rows}
        total = float(len(rows))
        warnings.append("fallback_equal_weight_applied")
    weights = {name: value / total for name, value in raw.items()}
    selected_names = [str(row["preset_name"]) for row in rows]
    fixed: dict[str, float] = {}
    remaining = set(selected_names)

    while remaining:
        remaining_weight = max(1.0 - sum(fixed.values()), 0.0)
        raw_total = sum(raw[name] for name in remaining)
        if raw_total <= 0:
            provisional = {name: remaining_weight / len(remaining) for name in remaining}
        else:
            provisional = {name: remaining_weight * (raw[name] / raw_total) for name in remaining}

        changed = False
        for name, value in list(provisional.items()):
            if value < policy.min_weight_per_strategy - 1e-12:
                fixed[name] = policy.min_weight_per_strategy
                remaining.remove(name)
                changed = True
            elif value > policy.max_weight_per_strategy + 1e-12:
                fixed[name] = policy.max_weight_per_strategy
                remaining.remove(name)
                changed = True
        if not changed:
            fixed.update(provisional)
            break

        if sum(fixed.values()) > 1.0 + 1e-12:
            raise ValueError("Strategy portfolio weight constraints are infeasible")

    assigned_total = sum(fixed.values())
    if assigned_total < 0.999999:
        warnings.append("underfilled_allocation_due_to_caps")
    return fixed, warnings


def build_strategy_portfolio(
    *,
    promoted_dir: str | Path,
    output_dir: str | Path,
    policy: StrategyPortfolioPolicyConfig,
    lifecycle_path: str | Path | None = None,
) -> dict[str, Any]:
    promoted_rows = _load_promoted_strategies(promoted_dir)
    if not promoted_rows:
        raise FileNotFoundError(
            f"No promoted strategies found under {Path(promoted_dir) / 'promoted_strategies.json'}"
        )
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    lifecycle_lookup: dict[str, dict[str, Any]] = {}
    if lifecycle_path is not None and Path(lifecycle_path).exists():
        from trading_platform.governance.strategy_lifecycle import load_strategy_lifecycle

        lifecycle_payload = load_strategy_lifecycle(lifecycle_path)
        lifecycle_lookup = {
            str(row.get("preset_name") or row.get("source_run_id") or row.get("strategy_id")): row
            for row in lifecycle_payload.get("strategies", [])
            if row.get("preset_name") or row.get("source_run_id") or row.get("strategy_id")
        }

    selected_input: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    shadow_rows: list[dict[str, Any]] = []
    seen_run_keys: set[tuple[str, str]] = set()
    family_counts: dict[str, int] = {}
    universe_counts: dict[str, int] = {}
    conditional_family_counts: dict[str, int] = {}
    selected_conditional_count = 0
    baseline_runs_available = {
        str(row.get("source_run_id") or "")
        for row in promoted_rows
        if not _is_conditional_variant(row) and str(row.get("source_run_id") or "").strip()
    }

    ranked_rows = _rank_candidates(
        promoted_rows,
        policy.selection_metric,
        conditional_variant_score_bonus=policy.conditional_variant_score_bonus,
    )

    def row_family(row: dict[str, Any]) -> str:
        return str(row.get("signal_family") or "")

    def row_universe(row: dict[str, Any]) -> str:
        return str(row.get("universe") or "")

    def row_run_key(row: dict[str, Any]) -> tuple[str, str]:
        source_run_id = str(row.get("source_run_id") or "")
        if not source_run_id:
            return ("", str(row.get("preset_name") or ""))
        if policy.allow_conditional_variant_siblings:
            variant_key = "conditional" if _is_conditional_variant(row) else "unconditional"
            return (source_run_id, variant_key)
        return (source_run_id, "any")

    def can_select_row(row: dict[str, Any]) -> tuple[bool, str | None]:
        preset_name = str(row.get("preset_name") or "")
        if not preset_name:
            return False, None
        if _is_conditional_variant(row):
            if not policy.include_conditional_strategies:
                return False, "conditional_disabled"
            if policy.conditional_selection_mode == "shadow_only":
                return False, "shadow_only_conditional"
            if (
                policy.require_baseline_for_conditional
                and str(row.get("source_run_id") or "") not in baseline_runs_available
            ):
                return False, "missing_baseline_for_conditional"
        passes, reason = _passes_filters(row, policy)
        if not passes:
            return False, reason
        source_run_id = str(row.get("source_run_id") or "")
        lifecycle_row = lifecycle_lookup.get(preset_name) or lifecycle_lookup.get(source_run_id)
        lifecycle_state = str(lifecycle_row.get("current_state") or "") if lifecycle_row else ""
        if lifecycle_state == "demoted":
            return False, "lifecycle_demoted"
        run_key = row_run_key(row)
        if policy.deduplicate_source_runs and run_key[0] and run_key in seen_run_keys:
            return False, "duplicate_source_run"
        signal_family = row_family(row)
        if (
            policy.max_strategies_per_signal_family is not None
            and signal_family
            and family_counts.get(signal_family, 0) >= policy.max_strategies_per_signal_family
        ):
            return False, "signal_family_cap"
        universe = row_universe(row)
        if (
            policy.max_strategies_per_universe is not None
            and universe
            and universe_counts.get(universe, 0) >= policy.max_strategies_per_universe
        ):
            return False, "universe_cap"
        if _is_conditional_variant(row):
            if (
                policy.max_conditional_strategies_total is not None
                and selected_conditional_count >= policy.max_conditional_strategies_total
            ):
                return False, "conditional_strategy_cap"
            if (
                policy.max_conditional_strategies_per_family is not None
                and signal_family
                and conditional_family_counts.get(signal_family, 0) >= policy.max_conditional_strategies_per_family
            ):
                return False, "conditional_family_cap"
        if policy.max_strategies is not None and len(selected_input) >= policy.max_strategies:
            return False, "max_strategies_reached"
        return True, None

    def add_selected_row(row: dict[str, Any]) -> None:
        nonlocal selected_conditional_count
        preset_name = str(row.get("preset_name") or "")
        source_run_id = str(row.get("source_run_id") or "")
        lifecycle_row = lifecycle_lookup.get(preset_name) or lifecycle_lookup.get(source_run_id)
        lifecycle_state = str(lifecycle_row.get("current_state") or "") if lifecycle_row else ""

        selected_input.append(row)
        row["lifecycle_state"] = lifecycle_state or None
        run_key = row_run_key(row)
        if run_key[0]:
            seen_run_keys.add(run_key)
        signal_family = row_family(row)
        if signal_family:
            family_counts[signal_family] = family_counts.get(signal_family, 0) + 1
            if _is_conditional_variant(row):
                conditional_family_counts[signal_family] = conditional_family_counts.get(signal_family, 0) + 1
        universe = row_universe(row)
        if universe:
            universe_counts[universe] = universe_counts.get(universe, 0) + 1
        if _is_conditional_variant(row):
            selected_conditional_count += 1

    def build_shadow_row(row: dict[str, Any], reason: str) -> dict[str, Any]:
        return {
            "preset_name": str(row.get("preset_name") or ""),
            "source_run_id": row.get("source_run_id"),
            "signal_family": row.get("signal_family"),
            "universe": row.get("universe"),
            "promotion_variant": row.get("promotion_variant"),
            "condition_id": row.get("condition_id"),
            "condition_type": row.get("condition_type"),
            "activation_conditions": row.get("activation_conditions", []),
            "activation_state": "shadow_only",
            "portfolio_bucket": "shadow",
            "rationale": row.get("rationale"),
            "ranking_metric": row.get("ranking_metric"),
            "ranking_value": row.get("ranking_value"),
            "shadow_reason": reason,
        }

    distinct_families_available = {row_family(row) for row in ranked_rows if row_family(row)}
    target_min_families = min(
        policy.min_families_if_available,
        len(distinct_families_available),
        policy.max_strategies if policy.max_strategies is not None else len(distinct_families_available),
    )
    if target_min_families > 0:
        chosen_families: set[str] = set()
        for row in ranked_rows:
            if policy.max_strategies is not None and len(selected_input) >= policy.max_strategies:
                break
            family = row_family(row)
            if not family or family in chosen_families:
                continue
            passes, reason = can_select_row(row)
            if not passes:
                if reason:
                    if reason == "shadow_only_conditional":
                        shadow_rows.append(build_shadow_row(row, reason))
                    else:
                        excluded_rows.append({"preset_name": str(row.get("preset_name") or ""), "reason": reason})
                continue
            add_selected_row(row)
            chosen_families.add(family)
            if len(chosen_families) >= target_min_families:
                break

    selected_names = {str(row.get("preset_name") or "") for row in selected_input}
    for row in ranked_rows:
        preset_name = str(row.get("preset_name") or "")
        if preset_name in selected_names:
            continue
        passes, reason = can_select_row(row)
        if not passes:
            if reason:
                if reason == "shadow_only_conditional":
                    shadow_rows.append(build_shadow_row(row, reason))
                else:
                    excluded_rows.append({"preset_name": preset_name, "reason": reason})
            continue
        add_selected_row(row)
        selected_names.add(preset_name)

    weights, allocation_warnings = _normalize_with_caps(selected_input, policy)
    warnings = list(allocation_warnings)
    if policy.warn_on_same_family_overlap:
        combos: dict[tuple[str, str], int] = {}
        for row in selected_input:
            key = (str(row.get("signal_family") or ""), str(row.get("universe") or ""))
            combos[key] = combos.get(key, 0) + 1
        for (family, universe), count in sorted(combos.items()):
            if count > 1 and family:
                warnings.append(f"similar_strategy_proxy_overlap:{family}:{universe or 'unknown'}:{count}")

    selected_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(selected_input, start=1):
        preset_name = str(row["preset_name"])
        is_conditional = _is_conditional_variant(row)
        selected_rows.append(
            {
                "preset_name": preset_name,
                "source_run_id": row.get("source_run_id"),
                "signal_family": row.get("signal_family"),
                "universe": row.get("universe"),
                "regime_compatibility": row.get("regime_compatibility", []),
                "promotion_status": row.get("status"),
                "lifecycle_state": row.get("lifecycle_state"),
                "allocation_weight": weights.get(preset_name, 0.0),
                "target_capital_fraction": weights.get(preset_name, 0.0),
                "selection_rank": rank,
                "selection_metric": policy.selection_metric,
                "selection_metric_value": _row_metric_value(row, policy.selection_metric),
                "ranking_metric": row.get("ranking_metric"),
                "ranking_value": row.get("ranking_value"),
                "reason_selected": "selected_by_policy",
                "warnings": "|".join(row.get("warnings", [])),
                "promotion_variant": row.get("promotion_variant"),
                "condition_id": row.get("condition_id"),
                "condition_type": row.get("condition_type"),
                "activation_conditions": row.get("activation_conditions", []),
                "activation_state": _activation_state_for_row(
                    row,
                    conditional_selection_mode=policy.conditional_selection_mode,
                ),
                "portfolio_bucket": (
                    "conditional"
                    if is_conditional and policy.conditional_selection_mode == "separate_bucket"
                    else "primary"
                ),
                "rationale": row.get("rationale"),
                "generated_preset_path": row.get("generated_preset_path"),
                "generated_registry_path": row.get("generated_registry_path"),
                "generated_pipeline_config_path": row.get("generated_pipeline_config_path"),
            }
        )

    total_active_weight = float(sum(row["allocation_weight"] for row in selected_rows))
    family_weight_summary = _summarize_family_weights(selected_rows)
    family_effective_count = _effective_count(list(family_weight_summary.values()))
    effective_strategy_count = _effective_count([row["allocation_weight"] for row in selected_rows])
    conditional_selected_count = sum(1 for row in selected_rows if _is_conditional_variant(row))
    shadow_conditional_count = len(shadow_rows)
    preset_path_ready_count = sum(
        1 for row in selected_rows if row.get("generated_preset_path") and Path(str(row["generated_preset_path"])).suffix
    )
    pipeline_path_ready_count = sum(
        1 for row in selected_rows if row.get("generated_pipeline_config_path") and Path(str(row["generated_pipeline_config_path"])).suffix
    )
    payload = {
        "schema_version": STRATEGY_PORTFOLIO_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "strategy_lifecycle_path": str(Path(lifecycle_path)) if lifecycle_path is not None else None,
        "policy": asdict(policy),
        "summary": {
            "total_selected_strategies": len(selected_rows),
            "total_active_weight": total_active_weight,
            "weighting_mode_resolved": _resolve_weighting_mode(policy.weighting_mode),
            "signal_family_counts": family_counts,
            "universe_counts": universe_counts,
            "signal_family_weights": family_weight_summary,
            "max_strategy_weight": max((row["allocation_weight"] for row in selected_rows), default=0.0),
            "max_family_weight": max(family_weight_summary.values(), default=0.0),
            "effective_strategy_count": effective_strategy_count,
            "effective_family_count": family_effective_count,
            "selected_conditional_variant_count": conditional_selected_count,
            "shadow_conditional_variant_count": shadow_conditional_count,
            "preset_path_ready_count": preset_path_ready_count,
            "pipeline_path_ready_count": pipeline_path_ready_count,
            "warning_count": len(warnings),
        },
        "selected_strategies": selected_rows,
        "shadow_strategies": shadow_rows,
        "excluded_candidates": excluded_rows,
        "warnings": warnings,
    }

    json_path = _write_payload(output_path / "strategy_portfolio.json", payload)
    csv_path = output_path / "strategy_portfolio.csv"
    pd.DataFrame(selected_rows).to_csv(csv_path, index=False)
    condition_summary_rows = [
        {
            "preset_name": row.get("preset_name"),
            "source_run_id": row.get("source_run_id"),
            "signal_family": row.get("signal_family"),
            "promotion_variant": row.get("promotion_variant"),
            "condition_id": row.get("condition_id"),
            "condition_type": row.get("condition_type"),
            "activation_state": row.get("activation_state"),
            "portfolio_bucket": row.get("portfolio_bucket"),
            "selection_metric_value": row.get("selection_metric_value"),
            "ranking_metric": row.get("ranking_metric"),
            "ranking_value": row.get("ranking_value"),
            "rationale": row.get("rationale"),
        }
        for row in selected_rows
        if _is_conditional_variant(row)
    ] + [
        {
            "preset_name": row.get("preset_name"),
            "source_run_id": row.get("source_run_id"),
            "signal_family": row.get("signal_family"),
            "promotion_variant": row.get("promotion_variant"),
            "condition_id": row.get("condition_id"),
            "condition_type": row.get("condition_type"),
            "activation_state": row.get("activation_state"),
            "portfolio_bucket": row.get("portfolio_bucket"),
            "selection_metric_value": None,
            "ranking_metric": row.get("ranking_metric"),
            "ranking_value": row.get("ranking_value"),
            "rationale": row.get("rationale"),
        }
        for row in shadow_rows
    ]
    condition_summary_path = output_path / "strategy_portfolio_condition_summary.csv"
    if condition_summary_rows:
        pd.DataFrame(condition_summary_rows).to_csv(condition_summary_path, index=False)
    elif condition_summary_path.exists():
        condition_summary_path.unlink()
    return {
        "strategy_portfolio_json_path": str(json_path),
        "strategy_portfolio_csv_path": str(csv_path),
        "strategy_portfolio_condition_summary_path": str(condition_summary_path),
        "selected_count": len(selected_rows),
        "warning_count": len(warnings),
    }


def load_strategy_portfolio(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "strategy_portfolio.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Strategy portfolio artifact not found or invalid: {path}")
    return payload


def export_multi_strategy_run_config_bundle(
    *,
    selected_rows: list[dict[str, Any]],
    output_dir: str | Path,
    bundle_name: str,
    source_artifact_path: str | Path,
    notes: str,
    source_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    if not selected_rows:
        raise ValueError("Selected strategy rows are required to export a multi-strategy run config bundle")

    sleeves = [
        MultiStrategySleeveConfig(
            sleeve_name=str(row["preset_name"]),
            preset_name=str(row["preset_name"]),
            target_capital_weight=float(
                row.get("target_capital_fraction", row.get("allocation_weight", row.get("adjusted_weight", 0.0))) or 0.0
            ),
            preset_path=str(
                row.get("generated_preset_path", row.get("preset_path", ""))
            )
            or None,
            enabled=bool(row.get("enabled", True)),
            promotion_variant=str(row.get("promotion_variant") or "") or None,
            condition_id=str(row.get("condition_id") or "") or None,
            condition_type=str(row.get("condition_type") or "") or None,
            activation_state=str(row.get("activation_state") or "") or None,
            is_active=bool(row.get("is_active")) if row.get("is_active") is not None else None,
            activation_reason=str(row.get("activation_reason") or "") or None,
            portfolio_bucket=str(row.get("portfolio_bucket") or "") or None,
            notes=" | ".join(
                part
                for part in [
                    str(row.get("reason_selected", row.get("reason_for_adjustment", "")) or ""),
                    str(row.get("promotion_variant") or ""),
                    str(row.get("condition_id") or ""),
                    str(row.get("activation_state") or ""),
                ]
                if part
            ),
            tags=[
                tag
                for tag in [
                    str(row.get("signal_family") or ""),
                    str(row.get("universe") or ""),
                    str(row.get("promotion_variant") or ""),
                    str(row.get("condition_type") or ""),
                    *(str(tag) for tag in row.get("regime_compatibility", [])),
                ]
                if tag
            ],
        )
        for row in selected_rows
    ]
    source_summary = dict((source_payload or {}).get("summary") or {})
    active_strategy_count = int(source_summary.get("active_row_count", len(selected_rows)) or len(selected_rows))
    active_unconditional_count = int(
        source_summary.get(
            "activated_unconditional_count",
            sum(1 for row in selected_rows if str(row.get("promotion_variant") or "unconditional") != "conditional"),
        )
        or 0
    )
    active_conditional_count = int(
        source_summary.get(
            "activated_conditional_count",
            sum(1 for row in selected_rows if str(row.get("promotion_variant") or "") == "conditional"),
        )
        or 0
    )
    inactive_conditional_count = int(source_summary.get("inactive_conditional_count", 0) or 0)
    activation_applied = bool((source_payload or {}).get("active_strategies") is not None)
    multi_strategy_config = MultiStrategyPortfolioConfig(
        sleeves=sleeves,
        source_portfolio_path=str((source_payload or {}).get("source_portfolio_path") or Path(source_artifact_path)),
        source_activated_portfolio_path=(
            str(Path(source_artifact_path))
            if activation_applied
            else None
        ),
        activation_applied=activation_applied,
        active_strategy_count=active_strategy_count,
        active_unconditional_count=active_unconditional_count,
        active_conditional_count=active_conditional_count,
        inactive_conditional_count=inactive_conditional_count,
        notes=notes,
        tags=[bundle_name],
    )
    multi_strategy_payload = {
        "gross_leverage_cap": multi_strategy_config.gross_leverage_cap,
        "net_exposure_cap": multi_strategy_config.net_exposure_cap,
        "max_position_weight": multi_strategy_config.max_position_weight,
        "max_symbol_concentration": multi_strategy_config.max_symbol_concentration,
        "sector_caps": [],
        "turnover_cap": multi_strategy_config.turnover_cap,
        "cash_reserve_pct": multi_strategy_config.cash_reserve_pct,
        "group_map_path": multi_strategy_config.group_map_path,
        "rebalance_timestamp": multi_strategy_config.rebalance_timestamp,
        "notes": multi_strategy_config.notes,
        "tags": multi_strategy_config.tags,
        "sleeves": [asdict(item) for item in multi_strategy_config.sleeves],
    }
    multi_strategy_path = _write_payload(output_path / f"{bundle_name}_multi_strategy.json", multi_strategy_payload)

    primary_universe = str(selected_rows[0].get("universe") or "nasdaq100")
    pipeline_config = PipelineRunConfig(
        run_name=f"{bundle_name}_paper",
        schedule_type="ad_hoc",
        universes=[primary_universe],
        multi_strategy_input_path=str(multi_strategy_path),
        paper_state_path=f"artifacts/paper/{bundle_name}_state.json",
        output_root_dir="artifacts/orchestration",
        continue_on_stage_error=True,
        stages=OrchestrationStageToggles(
            portfolio_allocation=True,
            paper_trading=True,
            reporting=True,
            monitoring=False,
        ),
    )
    pipeline_path = _write_payload(output_path / f"{bundle_name}_pipeline.yaml", pipeline_config.to_dict())
    bundle_payload = {
        "generated_at": _now_utc(),
        "source_artifact_path": str(Path(source_artifact_path)),
        "activation_applied": activation_applied,
        "active_strategy_count": active_strategy_count,
        "active_unconditional_count": active_unconditional_count,
        "active_conditional_count": active_conditional_count,
        "inactive_conditional_count": inactive_conditional_count,
        "multi_strategy_config_path": str(multi_strategy_path),
        "pipeline_config_path": str(pipeline_path),
        "selected_preset_names": [row["preset_name"] for row in selected_rows],
        "selected_strategy_variants": [
            {
                "preset_name": row.get("preset_name"),
                "promotion_variant": row.get("promotion_variant"),
                "condition_id": row.get("condition_id"),
                "condition_type": row.get("condition_type"),
                "activation_state": row.get("activation_state"),
                "portfolio_bucket": row.get("portfolio_bucket"),
            }
            for row in selected_rows
        ],
    }
    bundle_path = _write_payload(output_path / f"{bundle_name}_run_bundle.json", bundle_payload)
    return {
        "multi_strategy_config_path": str(multi_strategy_path),
        "pipeline_config_path": str(pipeline_path),
        "run_bundle_path": str(bundle_path),
    }


def export_strategy_portfolio_run_config(
    *,
    strategy_portfolio_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    input_path = Path(strategy_portfolio_path)
    if input_path.is_dir():
        activated_candidates = [
            input_path / "activated_strategy_portfolio.json",
            input_path / "activated" / "activated_strategy_portfolio.json",
        ]
        payload_path = next(
            (candidate for candidate in activated_candidates if candidate.exists()),
            input_path / "strategy_portfolio.json",
        )
    else:
        payload_path = input_path
    portfolio_payload = _safe_read_json(payload_path)
    selected: list[dict[str, Any]]
    if portfolio_payload.get("active_strategies") is not None:
        selected = list(portfolio_payload.get("active_strategies", []))
    else:
        portfolio_payload = load_strategy_portfolio(strategy_portfolio_path)
        selected = list(portfolio_payload.get("selected_strategies", []))
    if not selected:
        raise ValueError("Strategy portfolio contains no selected strategies")
    return export_multi_strategy_run_config_bundle(
        selected_rows=selected,
        output_dir=output_dir,
        bundle_name="strategy_portfolio",
        source_artifact_path=str(payload_path),
        source_payload=portfolio_payload,
        notes="Generated from strategy_portfolio.json",
    )
