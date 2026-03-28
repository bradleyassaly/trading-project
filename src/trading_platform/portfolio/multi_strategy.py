from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.cli.presets import CliPreset, resolve_cli_preset
from trading_platform.config.models import MultiStrategyPortfolioConfig, MultiStrategySleeveConfig
from trading_platform.metadata.groups import build_group_series
from trading_platform.paper.models import PaperExecutionPriceSnapshot, PaperTradingConfig
from trading_platform.services.target_construction_service import build_target_construction_result
from trading_platform.universes.registry import get_universe_symbols


@dataclass(frozen=True)
class SleeveTarget:
    sleeve_name: str
    preset_name: str
    symbol: str
    target_weight: float
    side: str
    scaled_target_weight: float
    as_of: str
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SleeveTargetBundle:
    sleeve: MultiStrategySleeveConfig
    paper_config: PaperTradingConfig
    as_of: str
    latest_prices: dict[str, float]
    latest_scores: dict[str, float]
    scheduled_target_weights: dict[str, float]
    effective_target_weights: dict[str, float]
    diagnostics: dict[str, Any]
    skipped_symbols: list[str]
    price_snapshots: list[PaperExecutionPriceSnapshot]
    signal_snapshot: Any | None


@dataclass(frozen=True)
class MultiStrategyAllocationResult:
    as_of: str
    portfolio_config: MultiStrategyPortfolioConfig
    sleeve_bundles: list[SleeveTargetBundle]
    combined_target_weights: dict[str, float]
    latest_prices: dict[str, float]
    sleeve_rows: list[dict[str, Any]]
    combined_rows: list[dict[str, Any]]
    symbol_overlap_rows: list[dict[str, Any]]
    sleeve_attribution_rows: list[dict[str, Any]]
    portfolio_diagnostics_rows: list[dict[str, Any]]
    overlap_matrix_rows: list[dict[str, Any]]
    execution_symbol_coverage_rows: list[dict[str, Any]]
    target_generation_stage_rows: list[dict[str, Any]]
    sleeve_target_diagnostics_rows: list[dict[str, Any]]
    summary: dict[str, Any]
    latest_scores: dict[str, float] = field(default_factory=dict)


def _load_cli_preset(preset_name: str, preset_path: str | None = None) -> CliPreset:
    if preset_path:
        path = Path(preset_path)
        if not path.exists():
            raise ValueError(f"Preset path does not exist: {preset_path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        return CliPreset(
            name=str(payload.get("name") or preset_name),
            description=str(payload.get("description") or f"Generated preset from {path.name}"),
            params=dict(payload.get("params", {})),
            decision_context=dict(payload.get("decision_context", {})),
        )
    try:
        return resolve_cli_preset(preset_name)
    except SystemExit as exc:
        raise ValueError(f"Unknown preset: {preset_name}") from exc


def _paper_config_from_preset(preset_name: str, preset_path: str | None = None) -> PaperTradingConfig:
    preset = _load_cli_preset(preset_name, preset_path)

    params = dict(preset.params)
    symbols = params.pop("symbols", None)
    universe = params.pop("universe", None)
    if symbols is None and universe is not None:
        symbols = get_universe_symbols(str(universe))
    if not symbols:
        raise ValueError(f"Preset {preset_name} did not resolve any symbols")

    config_kwargs = {
        "symbols": symbols,
        "preset_name": preset.name,
        "universe_name": universe,
        "signal_source": params.pop("signal_source", "legacy"),
        "strategy": params.pop("strategy", "sma_cross"),
        "fast": params.pop("fast", None),
        "slow": params.pop("slow", None),
        "lookback": params.pop("lookback", None),
        "lookback_bars": params.pop("lookback_bars", None),
        "skip_bars": params.pop("skip_bars", 0),
        "top_n": params.pop("top_n", 10),
        "weighting_scheme": params.pop("weighting_scheme", "equal"),
        "vol_window": params.pop("vol_lookback_bars", params.pop("vol_window", 20)),
        "rebalance_bars": params.pop("rebalance_bars", None),
        "portfolio_construction_mode": params.pop("portfolio_construction_mode", "pure_topn"),
        "max_position_weight": params.pop("max_position_weight", None),
        "min_score": params.pop("min_score", None),
        "max_weight": params.pop("max_weight", None),
        "max_names_per_group": params.pop("max_names_per_group", None),
        "max_group_weight": params.pop("max_group_weight", None),
        "group_map_path": params.pop("group_map_path", None),
        "max_names_per_sector": params.pop("max_names_per_sector", None),
        "turnover_buffer_bps": params.pop("turnover_buffer_bps", 0.0),
        "max_turnover_per_rebalance": params.pop("max_turnover_per_rebalance", None),
        "benchmark": params.pop("benchmark", None),
        "rebalance_frequency": params.pop("rebalance_frequency", "daily"),
        "timing": params.pop("timing", "next_bar"),
        "initial_cash": params.pop("initial_cash", 100_000.0),
        "min_trade_dollars": params.pop("min_trade_dollars", 25.0),
        "lot_size": params.pop("lot_size", 1),
        "reserve_cash_pct": params.pop("reserve_cash_pct", 0.0),
        "approved_model_state_path": params.pop("approved_model_state_path", None),
        "composite_artifact_dir": params.pop("composite_artifact_dir", None),
        "composite_horizon": params.pop("composite_horizon", 1),
        "composite_weighting_scheme": params.pop("composite_weighting_scheme", "equal"),
        "composite_portfolio_mode": params.pop("composite_portfolio_mode", "long_only_top_n"),
        "composite_long_quantile": params.pop("composite_long_quantile", 0.2),
        "composite_short_quantile": params.pop("composite_short_quantile", 0.2),
        "min_price": params.pop("min_price", None),
        "min_volume": params.pop("min_volume", None),
        "min_avg_dollar_volume": params.pop("min_avg_dollar_volume", None),
        "max_adv_participation": params.pop("max_adv_participation", 0.05),
        "max_position_pct_of_adv": params.pop("max_position_pct_of_adv", 0.1),
        "max_notional_per_name": params.pop("max_notional_per_name", None),
    }
    return PaperTradingConfig(**config_kwargs)


def load_strategy_sleeves(
    portfolio_config: MultiStrategyPortfolioConfig,
    *,
    as_of_date: str | None = None,
) -> list[SleeveTargetBundle]:
    bundles: list[SleeveTargetBundle] = []
    for sleeve in portfolio_config.sleeves:
        if not sleeve.enabled:
            continue
        if sleeve.preset_path:
            paper_config = _paper_config_from_preset(sleeve.preset_name, sleeve.preset_path)
        else:
            paper_config = _paper_config_from_preset(sleeve.preset_name)
        if as_of_date:
            paper_config = replace(paper_config, replay_as_of_date=as_of_date)
        target_result = build_target_construction_result(config=paper_config)
        bundles.append(
            SleeveTargetBundle(
                sleeve=sleeve,
                paper_config=paper_config,
                as_of=target_result.as_of,
                latest_prices=target_result.latest_prices,
                latest_scores=target_result.latest_scores,
                scheduled_target_weights=target_result.scheduled_target_weights,
                effective_target_weights=target_result.effective_target_weights,
                diagnostics=target_result.target_diagnostics | target_result.extra_diagnostics,
                skipped_symbols=target_result.skipped_symbols,
                price_snapshots=target_result.price_snapshots,
                signal_snapshot=target_result.signal_snapshot,
            )
        )
    if not bundles:
        raise ValueError("No enabled sleeves were available to allocate")
    return bundles


def _resolve_capital_weights(
    sleeves: list[SleeveTargetBundle],
) -> tuple[dict[str, float], dict[str, float], float]:
    raw = {bundle.sleeve.sleeve_name: float(bundle.sleeve.target_capital_weight) for bundle in sleeves}
    total = float(sum(raw.values()))
    if total <= 0:
        raise ValueError("Enabled sleeve capital weights must sum to a positive number")
    normalized = {name: weight / total for name, weight in raw.items()}
    return raw, normalized, total


def _combine_latest_scores(
    sleeves: list[SleeveTargetBundle],
    normalized_weights: dict[str, float],
) -> dict[str, float]:
    combined_scores: dict[str, float] = {}
    combined_weight: dict[str, float] = {}
    for bundle in sleeves:
        sleeve_weight = abs(float(normalized_weights.get(bundle.sleeve.sleeve_name, 0.0) or 0.0))
        if sleeve_weight <= 0.0:
            continue
        for symbol, raw_score in sorted(bundle.latest_scores.items()):
            try:
                score = float(raw_score)
            except (TypeError, ValueError):
                continue
            if pd.isna(score):
                continue
            combined_scores[symbol] = combined_scores.get(symbol, 0.0) + (score * sleeve_weight)
            combined_weight[symbol] = combined_weight.get(symbol, 0.0) + sleeve_weight
    return {
        symbol: float(total_score / combined_weight[symbol])
        for symbol, total_score in sorted(combined_scores.items())
        if combined_weight.get(symbol, 0.0) > 0.0
    }


def _apply_symbol_concentration_cap(
    contributions: dict[str, dict[str, float]],
    cap: float,
    clipped: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    if cap <= 0:
        return {symbol: {} for symbol in contributions}

    adjusted: dict[str, dict[str, float]] = {}
    for symbol, sleeve_weights in contributions.items():
        gross = float(sum(abs(weight) for weight in sleeve_weights.values()))
        if gross > cap and gross > 0:
            scale = cap / gross
            adjusted[symbol] = {
                sleeve_name: weight * scale
                for sleeve_name, weight in sleeve_weights.items()
            }
            clipped.append(
                {
                    "constraint_name": "max_symbol_concentration",
                    "symbol": symbol,
                    "before_weight": gross,
                    "after_weight": cap,
                    "action": "scaled_symbol_gross",
                }
            )
        else:
            adjusted[symbol] = dict(sleeve_weights)
    return adjusted


def _apply_position_cap(
    combined_weights: dict[str, float],
    cap: float,
    clipped: list[dict[str, Any]],
) -> dict[str, float]:
    if cap <= 0:
        return {symbol: 0.0 for symbol in combined_weights}

    adjusted: dict[str, float] = {}
    for symbol, weight in combined_weights.items():
        clipped_weight = min(max(weight, -cap), cap)
        adjusted[symbol] = clipped_weight
        if clipped_weight != weight:
            clipped.append(
                {
                    "constraint_name": "max_position_weight",
                    "symbol": symbol,
                    "before_weight": weight,
                    "after_weight": clipped_weight,
                    "action": "clipped_position",
                }
            )
    return adjusted


def _apply_group_caps(
    combined_weights: dict[str, float],
    portfolio_config: MultiStrategyPortfolioConfig,
    clipped: list[dict[str, Any]],
) -> dict[str, float]:
    if not portfolio_config.sector_caps:
        return combined_weights

    group_series = build_group_series(
        list(combined_weights.keys()),
        path=portfolio_config.group_map_path,
    )
    adjusted = dict(combined_weights)
    for group_cap in portfolio_config.sector_caps:
        symbols = [symbol for symbol, group in group_series.items() if group == group_cap.group]
        gross = float(sum(abs(adjusted.get(symbol, 0.0)) for symbol in symbols))
        if gross <= group_cap.max_weight or gross <= 0:
            continue
        scale = group_cap.max_weight / gross
        for symbol in symbols:
            adjusted[symbol] = adjusted.get(symbol, 0.0) * scale
        clipped.append(
            {
                "constraint_name": "sector_cap",
                "symbol": group_cap.group,
                "before_weight": gross,
                "after_weight": group_cap.max_weight,
                "action": "scaled_group_gross",
            }
        )
    return adjusted


def _scale_portfolio(
    combined_weights: dict[str, float],
    *,
    scale: float,
    constraint_name: str,
    clipped: list[dict[str, Any]],
) -> dict[str, float]:
    if scale >= 1.0:
        return combined_weights
    adjusted = {symbol: weight * scale for symbol, weight in combined_weights.items()}
    clipped.append(
        {
            "constraint_name": constraint_name,
            "symbol": "PORTFOLIO",
            "before_weight": float(sum(abs(weight) for weight in combined_weights.values())),
            "after_weight": float(sum(abs(weight) for weight in adjusted.values())),
            "action": "scaled_portfolio",
        }
    )
    return adjusted


def _constraint_summary(weights: dict[str, float]) -> dict[str, float]:
    gross = float(sum(abs(weight) for weight in weights.values()))
    net = float(sum(weights.values()))
    return {
        "gross_exposure": gross,
        "net_exposure": net,
        "position_count": float(sum(1 for weight in weights.values() if abs(weight) > 0)),
        "max_abs_weight": max((abs(weight) for weight in weights.values()), default=0.0),
    }


def _positive_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return numeric


def _resolve_execution_price(
    *,
    symbol: str,
    bundle: SleeveTargetBundle,
    allow_latest_close_fallback: bool,
) -> tuple[float | None, str | None, bool, str | None]:
    preferred_price = _positive_float(bundle.latest_prices.get(symbol))
    if preferred_price is not None:
        return (
            preferred_price,
            str(bundle.diagnostics.get("latest_price_source") or bundle.diagnostics.get("latest_data_source") or "latest_prices"),
            False,
            None,
        )

    if not allow_latest_close_fallback:
        return None, None, False, "missing_latest_price"

    snapshot = next((item for item in bundle.price_snapshots if item.symbol == symbol), None)
    if snapshot is None:
        return None, None, False, "missing_market_data"

    fallback_price = _positive_float(snapshot.final_price_used)
    if fallback_price is None:
        fallback_price = _positive_float(snapshot.latest_price)
    if fallback_price is None:
        fallback_price = _positive_float(snapshot.historical_price)
    if fallback_price is None:
        return None, None, False, "invalid_or_zero_price"

    return (
        fallback_price,
        str(snapshot.price_source_used or snapshot.latest_data_source or "latest_close_fallback"),
        True,
        None,
    )


def _zero_target_reason(
    *,
    active_strategy_count: int,
    requested_symbol_count: int,
    pre_validation_target_symbol_count: int,
    usable_symbol_count: int,
    combined_target_count: int,
    missing_market_data_count: int,
    missing_latest_price_count: int,
    invalid_price_count: int,
) -> str | None:
    if active_strategy_count <= 0:
        return "no_active_strategies"
    if requested_symbol_count <= 0:
        return "no_active_symbols"
    if pre_validation_target_symbol_count <= 0:
        return "no_targets_generated"
    if usable_symbol_count <= 0:
        if missing_market_data_count > 0:
            return "missing_market_data"
        if missing_latest_price_count > 0:
            return "missing_latest_price"
        if invalid_price_count > 0:
            return "invalid_or_zero_price"
        return "no_usable_symbols"
    if combined_target_count <= 0:
        return "all_target_weights_resolved_below_threshold"
    return None


def _derive_target_drop(bundle: SleeveTargetBundle) -> tuple[str | None, str | None]:
    target_diag = dict(bundle.diagnostics.get("target_construction", {}))
    raw_reason = str(target_diag.get("reason") or bundle.diagnostics.get("reason") or "").strip()
    score_symbol_count = int(bundle.signal_snapshot.scores.shape[1]) if getattr(bundle.signal_snapshot, "scores", None) is not None and not bundle.signal_snapshot.scores.empty else 0
    selected_signal_count = len(bundle.diagnostics.get("selected_signals", []) or [])
    latest_component_score_count = len(bundle.diagnostics.get("latest_component_scores", []) or [])
    latest_composite_score_count = len(bundle.diagnostics.get("latest_composite_scores", []) or [])
    liquidity_exclusion_count = len(bundle.diagnostics.get("liquidity_exclusions", []) or [])
    pre_validation_target_count = len(set(bundle.scheduled_target_weights) | set(bundle.effective_target_weights))

    if raw_reason == "no_approved_signals":
        return "signal_snapshot", "missing_signal_snapshot"
    if raw_reason == "no_composite_scores":
        if selected_signal_count > 0 and latest_component_score_count == 0:
            return "signal_scoring", "empty_signal_scores"
        if score_symbol_count == 0 or latest_composite_score_count == 0:
            return "signal_scoring", "empty_signal_scores"
        return "signal_scoring", "no_targets_generated"
    if raw_reason == "no_eligible_names":
        if liquidity_exclusion_count > 0:
            return "liquidity_filter", "all_symbols_failed_liquidity_filter"
        return "rank_selection", "all_symbols_failed_rank_selection"
    if pre_validation_target_count <= 0:
        if score_symbol_count <= 0:
            return "signal_scoring", "empty_signal_scores"
        return "rank_selection", "all_symbols_failed_rank_selection"
    return None, None


def _build_sleeve_target_diagnostics(
    *,
    bundle: SleeveTargetBundle,
    post_validation_target_symbol_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    score_symbol_count = int(bundle.signal_snapshot.scores.shape[1]) if getattr(bundle.signal_snapshot, "scores", None) is not None and not bundle.signal_snapshot.scores.empty else 0
    requested_symbol_count = len(getattr(bundle.paper_config, "symbols", []) or [])
    loaded_signal_symbol_count = max(score_symbol_count, len(bundle.latest_scores))
    selected_signal_count = len(bundle.diagnostics.get("selected_signals", []) or [])
    latest_component_score_count = len(bundle.diagnostics.get("latest_component_scores", []) or [])
    latest_composite_score_count = len(bundle.diagnostics.get("latest_composite_scores", []) or [])
    pre_validation_target_symbol_count = len(set(bundle.scheduled_target_weights) | set(bundle.effective_target_weights))
    rank_selection_count = int(
        bundle.diagnostics.get("target_construction", {}).get("selection_count")
        or bundle.diagnostics.get("target_construction", {}).get("target_selected_count")
        or pre_validation_target_symbol_count
    )
    liquidity_rows = list(bundle.diagnostics.get("liquidity_exclusions", []) or [])
    liquidity_exclusion_count = len({str(row.get("symbol")) for row in liquidity_rows if row.get("symbol")})
    liquidity_survivor_count = pre_validation_target_symbol_count
    target_drop_stage, target_drop_reason = _derive_target_drop(bundle)
    generated_preset_path = str(bundle.sleeve.preset_path or "")
    signal_artifact_path = str(
        getattr(bundle.paper_config, "approved_model_state_path", None)
        or getattr(bundle.paper_config, "composite_artifact_dir", None)
        or ""
    )

    diagnostics_row = {
        "sleeve_name": bundle.sleeve.sleeve_name,
        "preset_name": bundle.sleeve.preset_name,
        "generated_preset_path": generated_preset_path,
        "signal_source": getattr(bundle.paper_config, "signal_source", ""),
        "strategy": getattr(bundle.paper_config, "strategy", ""),
        "signal_artifact_path": signal_artifact_path,
        "requested_symbol_count": requested_symbol_count,
        "loaded_signal_symbol_count": loaded_signal_symbol_count,
        "selected_signal_count": selected_signal_count,
        "latest_component_score_count": latest_component_score_count,
        "latest_composite_score_count": latest_composite_score_count,
        "liquidity_exclusion_count": liquidity_exclusion_count,
        "rank_selection_count": rank_selection_count,
        "pre_validation_target_symbol_count": pre_validation_target_symbol_count,
        "post_validation_target_symbol_count": post_validation_target_symbol_count,
        "target_drop_stage": target_drop_stage or "",
        "target_drop_reason": target_drop_reason or "",
        "raw_target_reason": str(bundle.diagnostics.get("target_construction", {}).get("reason") or bundle.diagnostics.get("reason") or ""),
        "as_of": bundle.as_of,
    }
    stage_rows = [
        {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "stage_name": "symbols_requested",
            "count": requested_symbol_count,
            "reason": "",
        },
        {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "stage_name": "signals_loaded",
            "count": loaded_signal_symbol_count,
            "reason": "",
        },
        {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "stage_name": "rank_selection",
            "count": rank_selection_count,
            "reason": "",
        },
        {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "stage_name": "liquidity_survivors",
            "count": liquidity_survivor_count,
            "reason": "",
        },
        {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "stage_name": "pre_validation_targets",
            "count": pre_validation_target_symbol_count,
            "reason": "",
        },
        {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "stage_name": "post_validation_targets",
            "count": post_validation_target_symbol_count,
            "reason": target_drop_reason or "",
        },
    ]
    return diagnostics_row, stage_rows


def allocate_multi_strategy_portfolio(
    portfolio_config: MultiStrategyPortfolioConfig,
    *,
    previous_weights: dict[str, float] | None = None,
    as_of_date: str | None = None,
) -> MultiStrategyAllocationResult:
    sleeve_bundles = load_strategy_sleeves(portfolio_config, as_of_date=as_of_date)
    raw_weights, normalized_weights, raw_weight_sum = _resolve_capital_weights(sleeve_bundles)
    latest_scores = _combine_latest_scores(sleeve_bundles, normalized_weights)

    sleeve_rows: list[dict[str, Any]] = []
    latest_prices: dict[str, float] = {}
    symbol_contributions: dict[str, dict[str, float]] = {}
    execution_symbol_coverage_rows: list[dict[str, Any]] = []
    requested_symbols: set[str] = set()
    pre_validation_target_symbols: set[str] = set()
    usable_symbols: set[str] = set()
    skipped_execution_symbols: set[str] = set()
    latest_price_source_counts: dict[str, int] = {}

    for bundle in sleeve_bundles:
        sleeve_name = bundle.sleeve.sleeve_name
        capital_weight = normalized_weights[sleeve_name]
        bundle_requested_symbols = {
            str(symbol)
            for symbol in (
                getattr(bundle.paper_config, "symbols", None)
                or list(bundle.effective_target_weights)
                or list(bundle.scheduled_target_weights)
            )
        }
        requested_symbols.update(bundle_requested_symbols)
        bundle_target_symbols = set(bundle.effective_target_weights) | set(bundle.scheduled_target_weights)
        pre_validation_target_symbols.update(bundle_target_symbols)

        for symbol in sorted(bundle_target_symbols):
            raw_target_weight = float(bundle.effective_target_weights.get(symbol, 0.0) or 0.0)
            scheduled_target_weight = float(bundle.scheduled_target_weights.get(symbol, 0.0) or 0.0)
            execution_price, price_source, fallback_used, skip_reason = _resolve_execution_price(
                symbol=symbol,
                bundle=bundle,
                allow_latest_close_fallback=portfolio_config.allow_latest_close_fallback,
            )
            has_usable_market_data = execution_price is not None
            if has_usable_market_data:
                usable_symbols.add(symbol)
                latest_prices.setdefault(symbol, float(execution_price))
                latest_price_source_counts[price_source or "unknown"] = latest_price_source_counts.get(price_source or "unknown", 0) + 1
            else:
                skipped_execution_symbols.add(symbol)
            execution_symbol_coverage_rows.append(
                {
                    "sleeve_name": sleeve_name,
                    "preset_name": bundle.sleeve.preset_name,
                    "symbol": symbol,
                    "requested_by_strategy": symbol in bundle_requested_symbols,
                    "has_target_weight": abs(raw_target_weight) > 0.0 or abs(scheduled_target_weight) > 0.0,
                    "scheduled_target_weight": scheduled_target_weight,
                    "effective_target_weight": raw_target_weight,
                    "latest_price": execution_price,
                    "latest_price_source": price_source or "",
                    "latest_price_fallback_used": fallback_used,
                    "has_usable_market_data": has_usable_market_data,
                    "skip_reason": skip_reason or "",
                    "survived_target_generation": has_usable_market_data and abs(raw_target_weight) > 0.0,
                    "as_of": bundle.as_of,
                }
            )

        for symbol, target_weight in sorted(bundle.effective_target_weights.items()):
            execution_price, _price_source, _fallback_used, skip_reason = _resolve_execution_price(
                symbol=symbol,
                bundle=bundle,
                allow_latest_close_fallback=portfolio_config.allow_latest_close_fallback,
            )
            if execution_price is None:
                skipped_execution_symbols.add(symbol)
                continue
            scaled_weight = float(target_weight) * capital_weight
            symbol_contributions.setdefault(symbol, {})[sleeve_name] = scaled_weight
            latest_prices.setdefault(symbol, execution_price)
            sleeve_rows.append(
                {
                    "sleeve_name": sleeve_name,
                    "preset_name": bundle.sleeve.preset_name,
                    "symbol": symbol,
                    "target_weight": float(target_weight),
                    "scaled_target_weight": scaled_weight,
                    "side": "long" if scaled_weight >= 0 else "short",
                    "preset_path": bundle.sleeve.preset_path or "",
                    "capital_weight_raw": raw_weights[sleeve_name],
                    "capital_weight_normalized": capital_weight,
                    "rebalance_priority": bundle.sleeve.rebalance_priority,
                    "promotion_variant": bundle.sleeve.promotion_variant or "",
                    "condition_id": bundle.sleeve.condition_id or "",
                    "condition_type": bundle.sleeve.condition_type or "",
                    "activation_state": bundle.sleeve.activation_state or "",
                    "is_active": bundle.sleeve.is_active,
                    "activation_reason": bundle.sleeve.activation_reason or "",
                    "portfolio_bucket": bundle.sleeve.portfolio_bucket or "",
                    "tags": "|".join(bundle.sleeve.tags),
                    "notes": bundle.sleeve.notes or "",
                    "as_of": bundle.as_of,
                    "execution_skip_reason": skip_reason or "",
                }
            )

    symbol_overlap_rows: list[dict[str, Any]] = []
    for symbol, sleeve_weights in sorted(symbol_contributions.items()):
        gross = float(sum(abs(weight) for weight in sleeve_weights.values()))
        net = float(sum(sleeve_weights.values()))
        long_sleeves = sum(1 for weight in sleeve_weights.values() if weight > 0)
        short_sleeves = sum(1 for weight in sleeve_weights.values() if weight < 0)
        overlap_type = "conflict" if long_sleeves and short_sleeves else ("overlap" if len(sleeve_weights) > 1 else "single")
        symbol_overlap_rows.append(
            {
                "symbol": symbol,
                "sleeve_count": len(sleeve_weights),
                "long_sleeve_count": long_sleeves,
                "short_sleeve_count": short_sleeves,
                "gross_weight_before_constraints": gross,
                "net_weight_before_constraints": net,
                "overlap_type": overlap_type,
                "sleeves": "|".join(sorted(sleeve_weights)),
            }
        )

    clipped_rows: list[dict[str, Any]] = []
    constrained_contributions = _apply_symbol_concentration_cap(
        symbol_contributions,
        portfolio_config.max_symbol_concentration,
        clipped_rows,
    )
    combined_before_constraints = {
        symbol: float(sum(sleeve_weights.values()))
        for symbol, sleeve_weights in constrained_contributions.items()
    }
    before_summary = _constraint_summary(combined_before_constraints)

    constrained_weights = _apply_position_cap(
        combined_before_constraints,
        portfolio_config.max_position_weight,
        clipped_rows,
    )
    constrained_weights = _apply_group_caps(
        constrained_weights,
        portfolio_config,
        clipped_rows,
    )

    after_position_summary = _constraint_summary(constrained_weights)
    if after_position_summary["gross_exposure"] > portfolio_config.gross_leverage_cap and after_position_summary["gross_exposure"] > 0:
        scale = portfolio_config.gross_leverage_cap / after_position_summary["gross_exposure"]
        constrained_weights = _scale_portfolio(
            constrained_weights,
            scale=scale,
            constraint_name="gross_leverage_cap",
            clipped=clipped_rows,
        )

    net_after_gross = abs(float(sum(constrained_weights.values())))
    if net_after_gross > portfolio_config.net_exposure_cap and net_after_gross > 0:
        scale = portfolio_config.net_exposure_cap / net_after_gross
        constrained_weights = _scale_portfolio(
            constrained_weights,
            scale=scale,
            constraint_name="net_exposure_cap",
            clipped=clipped_rows,
        )

    constrained_weights = {
        symbol: weight
        for symbol, weight in sorted(constrained_weights.items())
        if abs(weight) > 1e-12
    }
    after_summary = _constraint_summary(constrained_weights)
    missing_market_data_count = sum(
        1 for row in execution_symbol_coverage_rows if row["skip_reason"] == "missing_market_data"
    )
    missing_latest_price_count = sum(
        1 for row in execution_symbol_coverage_rows if row["skip_reason"] == "missing_latest_price"
    )
    invalid_price_count = sum(
        1 for row in execution_symbol_coverage_rows if row["skip_reason"] == "invalid_or_zero_price"
    )
    usable_symbol_count = len(usable_symbols)
    requested_symbol_count = len(requested_symbols)
    skipped_symbol_count = len(skipped_execution_symbols)
    target_symbol_count = len(pre_validation_target_symbols)
    usable_symbol_fraction = (
        float(usable_symbol_count) / float(target_symbol_count)
        if target_symbol_count > 0
        else 0.0
    )
    zero_target_reason = _zero_target_reason(
        active_strategy_count=len(sleeve_bundles),
        requested_symbol_count=requested_symbol_count,
        pre_validation_target_symbol_count=target_symbol_count,
        usable_symbol_count=usable_symbol_count,
        combined_target_count=len(constrained_weights),
        missing_market_data_count=missing_market_data_count,
        missing_latest_price_count=missing_latest_price_count,
        invalid_price_count=invalid_price_count,
    )

    if portfolio_config.fail_if_no_usable_symbols and usable_symbol_count <= 0:
        raise ValueError("No usable symbols were available for multi-strategy execution")
    if (
        portfolio_config.min_usable_symbol_fraction is not None
        and target_symbol_count > 0
        and usable_symbol_fraction < float(portfolio_config.min_usable_symbol_fraction)
    ):
        raise ValueError(
            f"Usable symbol fraction {usable_symbol_fraction:.4f} is below configured minimum {portfolio_config.min_usable_symbol_fraction:.4f}"
        )
    if portfolio_config.fail_if_zero_targets_after_validation and len(constrained_weights) <= 0:
        raise ValueError(f"Zero targets remained after execution validation: {zero_target_reason or 'unspecified'}")

    previous = previous_weights or {}
    turnover_estimate = float(
        0.5 * sum(
            abs(constrained_weights.get(symbol, 0.0) - previous.get(symbol, 0.0))
            for symbol in set(previous) | set(constrained_weights)
        )
    )
    turnover_binding = (
        portfolio_config.turnover_cap is not None
        and turnover_estimate > portfolio_config.turnover_cap
    )

    combined_rows = [
        {
            "symbol": symbol,
            "target_weight": weight,
            "side": "long" if weight >= 0 else "short",
            "latest_price": latest_prices.get(symbol),
        }
        for symbol, weight in constrained_weights.items()
    ]

    sleeve_attribution_rows: list[dict[str, Any]] = []
    gross_contributions: dict[str, float] = {}
    net_contributions: dict[str, float] = {}
    for bundle in sleeve_bundles:
        sleeve_name = bundle.sleeve.sleeve_name
        sleeve_symbol_rows = [row for row in sleeve_rows if row["sleeve_name"] == sleeve_name]
        gross = float(sum(abs(row["scaled_target_weight"]) for row in sleeve_symbol_rows))
        net = float(sum(row["scaled_target_weight"] for row in sleeve_symbol_rows))
        matched_final = 0.0
        for row in sleeve_symbol_rows:
            symbol = str(row["symbol"])
            contribution = float(row["scaled_target_weight"])
            final_weight = float(constrained_weights.get(symbol, 0.0))
            if final_weight == 0:
                continue
            same_direction = (contribution >= 0 and final_weight >= 0) or (contribution <= 0 and final_weight <= 0)
            if same_direction:
                matched_final += min(abs(contribution), abs(final_weight))
        gross_contributions[sleeve_name] = gross
        net_contributions[sleeve_name] = net
        sleeve_attribution_rows.append(
            {
                "sleeve_name": sleeve_name,
                "preset_name": bundle.sleeve.preset_name,
                "capital_weight_raw": raw_weights[sleeve_name],
                "capital_weight_normalized": normalized_weights[sleeve_name],
                "gross_contribution": gross,
                "net_contribution": net,
                "final_portfolio_weight_contribution": matched_final,
                "symbol_count": len(sleeve_symbol_rows),
                "as_of": bundle.as_of,
            }
        )

    sleeve_names = [bundle.sleeve.sleeve_name for bundle in sleeve_bundles]
    overlap_matrix_rows: list[dict[str, Any]] = []
    for left in sleeve_names:
        left_map = {row["symbol"]: row["scaled_target_weight"] for row in sleeve_rows if row["sleeve_name"] == left}
        for right in sleeve_names:
            right_map = {row["symbol"]: row["scaled_target_weight"] for row in sleeve_rows if row["sleeve_name"] == right}
            shared_symbols = sorted(set(left_map) & set(right_map))
            gross_shared = float(sum(min(abs(left_map[s]), abs(right_map[s])) for s in shared_symbols))
            conflicting_symbols = sum(1 for s in shared_symbols if left_map[s] * right_map[s] < 0)
            overlap_matrix_rows.append(
                {
                    "sleeve_name": left,
                    "other_sleeve_name": right,
                    "shared_symbol_count": len(shared_symbols),
                    "gross_overlap_weight": gross_shared,
                    "conflicting_symbol_count": conflicting_symbols,
                }
            )

    active_weights = [abs(weight) for weight in constrained_weights.values() if abs(weight) > 0]
    normalized_abs = [weight / sum(active_weights) for weight in active_weights] if active_weights else []
    effective_positions = (
        float(1.0 / sum(weight * weight for weight in normalized_abs))
        if normalized_abs
        else 0.0
    )
    sleeve_abs = [gross for gross in gross_contributions.values() if gross > 0]
    sleeve_abs_norm = [gross / sum(sleeve_abs) for gross in sleeve_abs] if sleeve_abs else []
    effective_sleeves = (
        float(1.0 / sum(weight * weight for weight in sleeve_abs_norm))
        if sleeve_abs_norm
        else 0.0
    )
    overlap_concentration = float(
        sum(
            row["gross_weight_before_constraints"]
            for row in symbol_overlap_rows
            if row["sleeve_count"] > 1
        )
    )
    sleeve_target_diagnostics_rows: list[dict[str, Any]] = []
    target_generation_stage_rows: list[dict[str, Any]] = []
    post_validation_counts_by_sleeve: dict[str, int] = {}
    for row in sleeve_rows:
        post_validation_counts_by_sleeve[row["sleeve_name"]] = post_validation_counts_by_sleeve.get(row["sleeve_name"], 0) + 1
    for bundle in sleeve_bundles:
        sleeve_diag, stage_rows = _build_sleeve_target_diagnostics(
            bundle=bundle,
            post_validation_target_symbol_count=post_validation_counts_by_sleeve.get(bundle.sleeve.sleeve_name, 0),
        )
        sleeve_target_diagnostics_rows.append(sleeve_diag)
        target_generation_stage_rows.extend(stage_rows)

    primary_drop = next(
        (
            row
            for row in sleeve_target_diagnostics_rows
            if row.get("target_drop_reason")
        ),
        {},
    )
    primary_metadata = next(
        (
            row
            for row in sleeve_target_diagnostics_rows
            if row.get("generated_preset_path") or row.get("signal_artifact_path")
        ),
        primary_drop,
    )

    portfolio_diagnostics_rows = [
        {"metric": "gross_exposure_before_constraints", "value": before_summary["gross_exposure"]},
        {"metric": "net_exposure_before_constraints", "value": before_summary["net_exposure"]},
        {"metric": "gross_exposure_after_constraints", "value": after_summary["gross_exposure"]},
        {"metric": "net_exposure_after_constraints", "value": after_summary["net_exposure"]},
        {"metric": "max_abs_weight_after_constraints", "value": after_summary["max_abs_weight"]},
        {"metric": "turnover_estimate", "value": turnover_estimate},
        {"metric": "overlap_concentration", "value": overlap_concentration},
        {"metric": "effective_number_of_sleeves", "value": effective_sleeves},
        {"metric": "effective_number_of_positions", "value": effective_positions},
        {"metric": "raw_enabled_capital_weight_sum", "value": raw_weight_sum},
        {"metric": "cash_reserve_pct", "value": portfolio_config.cash_reserve_pct},
        {"metric": "requested_active_strategy_count", "value": float(len(sleeve_bundles))},
        {"metric": "requested_symbol_count", "value": float(requested_symbol_count)},
        {"metric": "pre_validation_target_symbol_count", "value": float(target_symbol_count)},
        {"metric": "usable_symbol_count", "value": float(usable_symbol_count)},
        {"metric": "skipped_symbol_count", "value": float(skipped_symbol_count)},
        {"metric": "usable_symbol_fraction", "value": usable_symbol_fraction},
        {"metric": "missing_market_data_symbol_count", "value": float(missing_market_data_count)},
        {"metric": "missing_latest_price_symbol_count", "value": float(missing_latest_price_count)},
        {"metric": "invalid_zero_price_symbol_count", "value": float(invalid_price_count)},
    ]

    as_of = max(bundle.as_of for bundle in sleeve_bundles)
    summary = {
        "as_of": as_of,
        "enabled_sleeve_count": len(sleeve_bundles),
        "raw_enabled_capital_weight_sum": raw_weight_sum,
        "normalized_capital_weights": normalized_weights,
        "gross_exposure_before_constraints": before_summary["gross_exposure"],
        "net_exposure_before_constraints": before_summary["net_exposure"],
        "gross_exposure_after_constraints": after_summary["gross_exposure"],
        "net_exposure_after_constraints": after_summary["net_exposure"],
        "max_abs_weight_after_constraints": after_summary["max_abs_weight"],
        "turnover_estimate": turnover_estimate,
        "turnover_cap": portfolio_config.turnover_cap,
        "turnover_cap_binding": turnover_binding,
        "cash_reserve_pct": portfolio_config.cash_reserve_pct,
        "activation_applied": bool(portfolio_config.activation_applied),
        "active_strategy_count": int(portfolio_config.active_strategy_count or len(sleeve_bundles)),
        "active_unconditional_count": int(portfolio_config.active_unconditional_count or len(sleeve_bundles)),
        "active_conditional_count": int(portfolio_config.active_conditional_count or 0),
        "inactive_conditional_count": int(portfolio_config.inactive_conditional_count or 0),
        "source_portfolio_path": portfolio_config.source_portfolio_path,
        "source_activated_portfolio_path": portfolio_config.source_activated_portfolio_path,
        "requested_active_strategy_count": len(sleeve_bundles),
        "requested_symbol_count": requested_symbol_count,
        "pre_validation_target_symbol_count": target_symbol_count,
        "usable_symbol_count": usable_symbol_count,
        "skipped_symbol_count": skipped_symbol_count,
        "usable_symbol_fraction": usable_symbol_fraction,
        "missing_market_data_symbol_count": missing_market_data_count,
        "missing_latest_price_symbol_count": missing_latest_price_count,
        "invalid_zero_price_symbol_count": invalid_price_count,
        "zero_target_reason": zero_target_reason,
        "latest_price_source_summary": latest_price_source_counts,
        "target_drop_stage": primary_drop.get("target_drop_stage"),
        "target_drop_reason": primary_drop.get("target_drop_reason"),
        "generated_preset_path": primary_metadata.get("generated_preset_path"),
        "signal_artifact_path": primary_metadata.get("signal_artifact_path"),
        "overlap_concentration": overlap_concentration,
        "effective_number_of_sleeves": effective_sleeves,
        "effective_number_of_positions": effective_positions,
        "symbols_removed_or_clipped": clipped_rows,
        "sleeve_contribution": gross_contributions,
    }

    return MultiStrategyAllocationResult(
        as_of=as_of,
        portfolio_config=portfolio_config,
        sleeve_bundles=sleeve_bundles,
        combined_target_weights=constrained_weights,
        latest_prices=latest_prices,
        sleeve_rows=sleeve_rows,
        combined_rows=combined_rows,
        symbol_overlap_rows=symbol_overlap_rows,
        sleeve_attribution_rows=sleeve_attribution_rows,
        portfolio_diagnostics_rows=portfolio_diagnostics_rows,
        overlap_matrix_rows=overlap_matrix_rows,
        execution_symbol_coverage_rows=execution_symbol_coverage_rows,
        target_generation_stage_rows=target_generation_stage_rows,
        sleeve_target_diagnostics_rows=sleeve_target_diagnostics_rows,
        summary=summary,
        latest_scores=latest_scores,
    )


def _render_summary_markdown(result: MultiStrategyAllocationResult) -> str:
    lines = [
        "# Multi-Strategy Allocation Summary",
        "",
        f"- As of: `{result.as_of}`",
        f"- Enabled sleeves: `{result.summary['enabled_sleeve_count']}`",
        f"- Raw enabled capital weight sum: `{result.summary['raw_enabled_capital_weight_sum']:.6f}`",
        f"- Gross exposure before constraints: `{result.summary['gross_exposure_before_constraints']:.6f}`",
        f"- Gross exposure after constraints: `{result.summary['gross_exposure_after_constraints']:.6f}`",
        f"- Net exposure after constraints: `{result.summary['net_exposure_after_constraints']:.6f}`",
        f"- Turnover estimate: `{result.summary['turnover_estimate']:.6f}`",
        f"- Effective sleeves: `{result.summary['effective_number_of_sleeves']:.6f}`",
        f"- Effective positions: `{result.summary['effective_number_of_positions']:.6f}`",
        f"- Requested symbols: `{result.summary.get('requested_symbol_count', 0)}`",
        f"- Usable symbols: `{result.summary.get('usable_symbol_count', 0)}`",
        f"- Skipped symbols: `{result.summary.get('skipped_symbol_count', 0)}`",
        f"- Target drop stage: `{result.summary.get('target_drop_stage')}`",
        f"- Zero target reason: `{result.summary.get('zero_target_reason')}`",
        "",
        "## Sleeve Contributions",
    ]
    for row in result.sleeve_attribution_rows:
        lines.append(
            f"- `{row['sleeve_name']}` ({row['preset_name']}): gross={row['gross_contribution']:.6f}, net={row['net_contribution']:.6f}, final={row['final_portfolio_weight_contribution']:.6f}"
        )
    if result.summary["symbols_removed_or_clipped"]:
        lines.extend(["", "## Constraint Actions"])
        for row in result.summary["symbols_removed_or_clipped"]:
            lines.append(
                f"- `{row['constraint_name']}` {row['symbol']}: {row['before_weight']:.6f} -> {row['after_weight']:.6f}"
            )
    return "\n".join(lines) + "\n"


def write_multi_strategy_artifacts(
    result: MultiStrategyAllocationResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    combined_path = output_path / "combined_target_weights.csv"
    sleeve_path = output_path / "sleeve_target_weights.csv"
    overlap_path = output_path / "symbol_overlap_report.csv"
    summary_json_path = output_path / "allocation_summary.json"
    summary_md_path = output_path / "allocation_summary.md"
    sleeve_attr_path = output_path / "sleeve_attribution.csv"
    diagnostics_path = output_path / "portfolio_diagnostics.csv"
    overlap_matrix_path = output_path / "overlap_matrix.csv"
    execution_coverage_path = output_path / "execution_symbol_coverage.csv"
    execution_summary_path = output_path / "execution_data_availability_summary.json"
    target_generation_summary_path = output_path / "target_generation_summary.json"
    target_generation_stage_counts_path = output_path / "target_generation_stage_counts.csv"
    sleeve_target_diagnostics_path = output_path / "sleeve_target_diagnostics.csv"

    pd.DataFrame(
        result.combined_rows,
        columns=["symbol", "target_weight", "side", "latest_price"],
    ).to_csv(combined_path, index=False)
    pd.DataFrame(
        result.sleeve_rows,
        columns=[
            "sleeve_name",
            "preset_name",
            "symbol",
            "target_weight",
            "scaled_target_weight",
            "side",
            "preset_path",
            "capital_weight_raw",
            "capital_weight_normalized",
            "rebalance_priority",
            "promotion_variant",
            "condition_id",
            "condition_type",
            "activation_state",
            "is_active",
            "activation_reason",
            "portfolio_bucket",
            "tags",
            "notes",
            "execution_skip_reason",
            "as_of",
        ],
    ).to_csv(sleeve_path, index=False)
    pd.DataFrame(result.symbol_overlap_rows).to_csv(overlap_path, index=False)
    pd.DataFrame(result.sleeve_attribution_rows).to_csv(sleeve_attr_path, index=False)
    pd.DataFrame(result.portfolio_diagnostics_rows).to_csv(diagnostics_path, index=False)
    pd.DataFrame(result.overlap_matrix_rows).to_csv(overlap_matrix_path, index=False)
    pd.DataFrame(
        result.execution_symbol_coverage_rows,
        columns=[
            "sleeve_name",
            "preset_name",
            "symbol",
            "requested_by_strategy",
            "has_target_weight",
            "scheduled_target_weight",
            "effective_target_weight",
            "latest_price",
            "latest_price_source",
            "latest_price_fallback_used",
            "has_usable_market_data",
            "skip_reason",
            "survived_target_generation",
            "as_of",
        ],
    ).to_csv(execution_coverage_path, index=False)
    pd.DataFrame(
        result.target_generation_stage_rows,
        columns=["sleeve_name", "preset_name", "stage_name", "count", "reason"],
    ).to_csv(target_generation_stage_counts_path, index=False)
    pd.DataFrame(
        result.sleeve_target_diagnostics_rows,
        columns=[
            "sleeve_name",
            "preset_name",
            "generated_preset_path",
            "signal_source",
            "strategy",
            "signal_artifact_path",
            "requested_symbol_count",
            "loaded_signal_symbol_count",
            "selected_signal_count",
            "latest_component_score_count",
            "latest_composite_score_count",
            "liquidity_exclusion_count",
            "rank_selection_count",
            "pre_validation_target_symbol_count",
            "post_validation_target_symbol_count",
            "target_drop_stage",
            "target_drop_reason",
            "raw_target_reason",
            "as_of",
        ],
    ).to_csv(sleeve_target_diagnostics_path, index=False)

    summary_json_path.write_text(
        json.dumps(
            {
                "summary": result.summary,
                "as_of": result.as_of,
                "normalized_capital_weights": result.summary["normalized_capital_weights"],
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    execution_summary_path.write_text(
        json.dumps(
            {
                "as_of": result.as_of,
                "requested_active_strategy_count": result.summary.get("requested_active_strategy_count"),
                "requested_symbol_count": result.summary.get("requested_symbol_count"),
                "pre_validation_target_symbol_count": result.summary.get("pre_validation_target_symbol_count"),
                "usable_symbol_count": result.summary.get("usable_symbol_count"),
                "skipped_symbol_count": result.summary.get("skipped_symbol_count"),
                "usable_symbol_fraction": result.summary.get("usable_symbol_fraction"),
                "missing_market_data_symbol_count": result.summary.get("missing_market_data_symbol_count"),
                "missing_latest_price_symbol_count": result.summary.get("missing_latest_price_symbol_count"),
                "invalid_zero_price_symbol_count": result.summary.get("invalid_zero_price_symbol_count"),
                "zero_target_reason": result.summary.get("zero_target_reason"),
                "latest_price_source_summary": result.summary.get("latest_price_source_summary", {}),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    target_generation_summary_path.write_text(
        json.dumps(
            {
                "as_of": result.as_of,
                "requested_active_strategy_count": result.summary.get("requested_active_strategy_count"),
                "enabled_sleeve_count": result.summary.get("enabled_sleeve_count"),
                "pre_validation_target_symbol_count": result.summary.get("pre_validation_target_symbol_count"),
                "post_validation_target_symbol_count": len(result.combined_target_weights),
                "target_drop_stage": result.summary.get("target_drop_stage"),
                "target_drop_reason": result.summary.get("target_drop_reason"),
                "generated_preset_path": result.summary.get("generated_preset_path"),
                "signal_artifact_path": result.summary.get("signal_artifact_path"),
                "sleeves": result.sleeve_target_diagnostics_rows,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    summary_md_path.write_text(_render_summary_markdown(result), encoding="utf-8")

    return {
        "combined_target_weights_path": combined_path,
        "sleeve_target_weights_path": sleeve_path,
        "symbol_overlap_report_path": overlap_path,
        "allocation_summary_json_path": summary_json_path,
        "allocation_summary_md_path": summary_md_path,
        "sleeve_attribution_path": sleeve_attr_path,
        "portfolio_diagnostics_path": diagnostics_path,
        "overlap_matrix_path": overlap_matrix_path,
        "execution_symbol_coverage_path": execution_coverage_path,
        "execution_data_availability_summary_path": execution_summary_path,
        "target_generation_summary_path": target_generation_summary_path,
        "target_generation_stage_counts_path": target_generation_stage_counts_path,
        "sleeve_target_diagnostics_path": sleeve_target_diagnostics_path,
    }
