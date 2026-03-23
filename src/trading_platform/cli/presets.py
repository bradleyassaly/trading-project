from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


@dataclass(frozen=True)
class CliPreset:
    name: str
    description: str
    params: dict[str, Any]
    decision_context: dict[str, Any] = field(default_factory=dict)


XSEC_FAMILY_VERSION = "xsec_nasdaq100_momentum_v1"


CLI_PRESETS: dict[str, CliPreset] = {
    "xsec_nasdaq100_momentum_v1_research": CliPreset(
        name="xsec_nasdaq100_momentum_v1_research",
        description="Validated Nasdaq-100 xsec momentum research baseline using pure_topn with the 84/21/2/21 family.",
        params={
            "universe": "nasdaq100",
            "strategy": "xsec_momentum_topn",
            "portfolio_construction_mode": "pure_topn",
            "lookback_bars": 84,
            "skip_bars": 21,
            "top_n": 2,
            "rebalance_bars": 21,
            "benchmark": "equal_weight",
            "cost_bps": 10.0,
            "lookback_bars_values": [84],
            "skip_bars_values": [21],
            "top_n_values": [2],
            "rebalance_bars_values": [21],
        },
        decision_context={
            "family_version": XSEC_FAMILY_VERSION,
            "role": "research_baseline",
            "universe": "nasdaq100",
            "summary": "Research-clean baseline that preserves pure top-N semantics for xsec momentum validation.",
            "why_selected": [
                "Validated as the most robust pure_topn candidate across 0/10/25/50 bps cost assumptions.",
                "Remained strong across both 756/126/126 and 504/126/126 walk-forward schedules.",
                "Delivered higher excess return than the alternative pure_topn candidate without collapsing under costs.",
            ],
        },
    ),
    "xsec_nasdaq100_momentum_v1_deploy": CliPreset(
        name="xsec_nasdaq100_momentum_v1_deploy",
        description="Validated Nasdaq-100 xsec momentum deployable overlay using transition construction and implementability controls.",
        params={
            "universe": "nasdaq100",
            "strategy": "xsec_momentum_topn",
            "portfolio_construction_mode": "transition",
            "lookback_bars": 84,
            "skip_bars": 21,
            "top_n": 2,
            "rebalance_bars": 21,
            "benchmark": "equal_weight",
            "cost_bps": 10.0,
            "max_position_weight": 0.5,
            "min_avg_dollar_volume": 50_000_000.0,
            "weighting_scheme": "inv_vol",
            "vol_lookback_bars": 20,
            "max_turnover_per_rebalance": 0.5,
            "turnover_buffer_bps": 0.0,
            "lookback_bars_values": [84],
            "skip_bars_values": [21],
            "top_n_values": [2],
            "rebalance_bars_values": [21],
            "preset_name": "xsec_nasdaq100_momentum_v1_deploy",
        },
        decision_context={
            "family_version": XSEC_FAMILY_VERSION,
            "role": "deploy_overlay",
            "universe": "nasdaq100",
            "summary": "Deployable overlay that keeps the validated 84/21/2/21 family but adds transition and implementability controls.",
            "why_selected": [
                "Validated as the most stable deployable overlay across costs and train-window choices.",
                "Delivered the cleanest turnover and drawdown trade-off among the tested transition candidates.",
                "Preserved the validated Nasdaq-100 xsec momentum family while making turnover controls explicit.",
            ],
        },
    ),
}

GENERATED_PRESET_DIRECTORIES = [Path("configs/generated_strategies")]


PRESET_OVERRIDE_OPTION_ALIASES: dict[str, list[str]] = {
    "symbols": ["--symbols"],
    "universe": ["--universe"],
    "strategy": ["--strategy"],
    "lookback_bars": ["--lookback-bars"],
    "skip_bars": ["--skip-bars"],
    "top_n": ["--top-n"],
    "rebalance_bars": ["--rebalance-bars"],
    "portfolio_construction_mode": ["--portfolio-construction-mode"],
    "max_position_weight": ["--max-position-weight"],
    "min_avg_dollar_volume": ["--min-avg-dollar-volume"],
    "max_names_per_sector": ["--max-names-per-sector"],
    "turnover_buffer_bps": ["--turnover-buffer-bps"],
    "max_turnover_per_rebalance": ["--max-turnover-per-rebalance"],
    "weighting_scheme": ["--weighting-scheme"],
    "vol_lookback_bars": ["--vol-lookback-bars"],
    "benchmark": ["--benchmark"],
    "cost_bps": ["--cost-bps"],
    "lookback_bars_values": ["--lookback-bars-values"],
    "skip_bars_values": ["--skip-bars-values"],
    "top_n_values": ["--top-n-values"],
    "rebalance_bars_values": ["--rebalance-bars-values"],
    "preset_name": ["--preset-name"],
}


def get_preset_choices() -> list[str]:
    return sorted({*CLI_PRESETS.keys(), *_load_generated_presets().keys()})


def resolve_cli_preset(name: str) -> CliPreset:
    if name in CLI_PRESETS:
        return CLI_PRESETS[name]
    generated = _load_generated_presets()
    try:
        return generated[name]
    except KeyError as exc:
        raise SystemExit(f"Unknown preset: {name}") from exc


def _explicit_options(args) -> set[str]:
    argv = list(getattr(args, "_cli_argv", []) or [])
    return {token.split("=", 1)[0] for token in argv if token.startswith("--")}


def _is_explicit(args, attr_name: str) -> bool:
    explicit = _explicit_options(args)
    aliases = PRESET_OVERRIDE_OPTION_ALIASES.get(attr_name, [f"--{attr_name.replace('_', '-')}"])
    return any(alias in explicit for alias in aliases)


def apply_cli_preset(args) -> CliPreset | None:
    preset_name = getattr(args, "preset", None)
    if not preset_name:
        return None

    preset = resolve_cli_preset(str(preset_name))
    explicit = _explicit_options(args)
    symbol_related = {"--symbols", "--universe"}

    for attr_name, value in preset.params.items():
        if not hasattr(args, attr_name):
            continue
        if attr_name in {"symbols", "universe"} and explicit.intersection(symbol_related):
            continue
        if _is_explicit(args, attr_name):
            continue
        setattr(args, attr_name, deepcopy(value))

    setattr(args, "_resolved_preset", preset.name)
    return preset


def build_decision_memo_payload(
    *,
    research_preset_name: str,
    deploy_preset_name: str,
) -> dict[str, Any]:
    research_preset = resolve_cli_preset(research_preset_name)
    deploy_preset = resolve_cli_preset(deploy_preset_name)

    return {
        "family_version": XSEC_FAMILY_VERSION,
        "strategy": "xsec_momentum_topn",
        "universe": research_preset.params.get("universe", "nasdaq100"),
        "research_preset": {
            "name": research_preset.name,
            "description": research_preset.description,
            "params": deepcopy(research_preset.params),
            "role": research_preset.decision_context.get("role"),
            "why_selected": deepcopy(research_preset.decision_context.get("why_selected", [])),
        },
        "deploy_preset": {
            "name": deploy_preset.name,
            "description": deploy_preset.description,
            "params": deepcopy(deploy_preset.params),
            "role": deploy_preset.decision_context.get("role"),
            "why_selected": deepcopy(deploy_preset.decision_context.get("why_selected", [])),
        },
        "baseline_vs_overlay": {
            "pure_topn": "Research baseline used for clean signal validation, parameter comparison, and robustness checks without transition-induced holdings drift.",
            "transition": "Deployable overlay used to study turnover controls, liquidity filters, and partial transitions after the pure_topn baseline is understood.",
        },
        "robustness_findings": [
            "The validated Nasdaq-100 family centers on 84 lookback bars, 21 skip bars, top_n=2, and 21-bar rebalances.",
            "The pure_topn preset remained strong across both 756/126/126 and 504/126/126 walk-forward schedules and under 0/10/25/50 bps cost assumptions.",
            "The transition preset traded some return for cleaner turnover, shallower drawdowns, and better cost resistance in deployable settings.",
        ],
        "main_caveats": [
            "Results remain sensitive to regime changes; future validation should continue across expanding windows and out-of-family universes.",
            "Cost assumptions are linear turnover approximations and do not include spread, impact, or queue-position effects.",
            "The current nasdaq100 universe is a present-day survivor approximation, so historical tests still carry survivorship bias until point-in-time membership is implemented.",
        ],
        "next_steps": [
            "Run regular compare-xsec-construction checks when the family is revisited.",
            "Paper trade the deploy preset and monitor turnover-cap bindings, liquidity exclusions, and holdings drift against the research baseline.",
            "Extend validation to additional liquid universes before promoting a new preset version.",
        ],
    }


def _read_preset_payload(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML preset files. Install with `pip install pyyaml`.")
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload or {}
    raise ValueError(f"Unsupported preset file type: {suffix}")


def _load_generated_presets() -> dict[str, CliPreset]:
    presets: dict[str, CliPreset] = {}
    for directory in GENERATED_PRESET_DIRECTORIES:
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.*")):
            if path.suffix.lower() not in {".json", ".yaml", ".yml"}:
                continue
            try:
                payload = _read_preset_payload(path)
            except (json.JSONDecodeError, ValueError):
                continue
            if payload.get("preset_type") != "generated_strategy_preset":
                continue
            name = str(payload.get("name") or "").strip()
            if not name:
                continue
            presets[name] = CliPreset(
                name=name,
                description=str(payload.get("description") or f"Generated preset from {path.name}"),
                params=dict(payload.get("params", {})),
                decision_context=dict(payload.get("decision_context", {})),
            )
    return presets
