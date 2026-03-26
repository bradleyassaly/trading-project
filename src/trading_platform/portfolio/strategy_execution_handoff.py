from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from trading_platform.config.models import MultiStrategyPortfolioConfig, MultiStrategySleeveConfig


@dataclass(frozen=True)
class StrategyExecutionHandoffConfig:
    use_activated_portfolio_for_paper: bool = True
    fail_if_no_active_strategies: bool = False
    include_inactive_conditionals_in_reports: bool = True


@dataclass(frozen=True)
class StrategyExecutionHandoff:
    source_kind: str
    source_path: str
    portfolio_config: MultiStrategyPortfolioConfig | None
    summary: dict[str, Any]
    warnings: list[str]


def _safe_read_json(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists() or file_path.is_dir():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _resolve_run_bundle_path(path_or_dir: str | Path) -> Path | None:
    path = Path(path_or_dir)
    if path.is_file() and path.name.endswith("_run_bundle.json"):
        return path
    if path.is_dir():
        direct = path / "strategy_portfolio_run_bundle.json"
        if direct.exists():
            return direct
        candidates = sorted(path.glob("*_run_bundle.json"))
        if candidates:
            return candidates[0]
    return None


def _resolve_relative_path(base_path: Path, raw_path: str | None) -> str | None:
    if not raw_path:
        return None
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return str(candidate)
    return str((base_path.parent / candidate).resolve())


def _derive_counts_from_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    active_rows = [row for row in rows if bool(row.get("is_active", True))]
    return {
        "active_strategy_count": len(active_rows),
        "active_unconditional_count": sum(
            1 for row in active_rows if str(row.get("promotion_variant") or "unconditional") != "conditional"
        ),
        "active_conditional_count": sum(
            1 for row in active_rows if str(row.get("promotion_variant") or "") == "conditional"
        ),
        "inactive_conditional_count": sum(
            1
            for row in rows
            if str(row.get("promotion_variant") or "") == "conditional" and not bool(row.get("is_active", False))
        ),
    }


def _build_portfolio_config_from_rows(
    *,
    rows: list[dict[str, Any]],
    source_portfolio_path: str,
    source_activated_portfolio_path: str | None,
    activation_applied: bool,
    handoff_config: StrategyExecutionHandoffConfig,
    counts: dict[str, int],
) -> MultiStrategyPortfolioConfig:
    sleeves = [
        MultiStrategySleeveConfig(
            sleeve_name=str(row.get("preset_name") or ""),
            preset_name=str(row.get("preset_name") or ""),
            target_capital_weight=float(
                row.get("target_capital_fraction", row.get("allocation_weight", row.get("adjusted_weight", 0.0))) or 0.0
            ),
            preset_path=str(row.get("generated_preset_path") or row.get("preset_path") or "") or None,
            enabled=bool(row.get("is_active", True)),
            promotion_variant=str(row.get("promotion_variant") or "") or None,
            condition_id=str(row.get("condition_id") or "") or None,
            condition_type=str(row.get("condition_type") or "") or None,
            activation_state=str(row.get("activation_state") or "") or None,
            is_active=bool(row.get("is_active", True)),
            activation_reason=str(row.get("activation_reason") or "") or None,
            portfolio_bucket=str(row.get("portfolio_bucket") or "") or None,
            notes=str(row.get("rationale") or "") or None,
            tags=[
                tag
                for tag in [
                    str(row.get("signal_family") or ""),
                    str(row.get("universe") or ""),
                    str(row.get("promotion_variant") or ""),
                    str(row.get("condition_type") or ""),
                ]
                if tag
            ],
        )
        for row in rows
    ]
    return MultiStrategyPortfolioConfig(
        sleeves=sleeves,
        source_portfolio_path=source_portfolio_path,
        source_activated_portfolio_path=source_activated_portfolio_path,
        activation_applied=activation_applied,
        use_activated_portfolio_for_paper=handoff_config.use_activated_portfolio_for_paper,
        fail_if_no_active_strategies=handoff_config.fail_if_no_active_strategies,
        include_inactive_conditionals_in_reports=handoff_config.include_inactive_conditionals_in_reports,
        active_strategy_count=counts["active_strategy_count"],
        active_unconditional_count=counts["active_unconditional_count"],
        active_conditional_count=counts["active_conditional_count"],
        inactive_conditional_count=counts["inactive_conditional_count"],
        notes="Generated from strategy execution handoff",
        tags=["strategy_execution_handoff"],
    )


def resolve_strategy_execution_handoff(
    path_or_dir: str | Path,
    *,
    config: StrategyExecutionHandoffConfig | None = None,
) -> StrategyExecutionHandoff:
    handoff_config = config or StrategyExecutionHandoffConfig()
    path = Path(path_or_dir)
    warnings: list[str] = []

    bundle_path = _resolve_run_bundle_path(path)
    if bundle_path is not None:
        from trading_platform.config.loader import load_multi_strategy_portfolio_config

        bundle_payload = _safe_read_json(bundle_path)
        config_path = _resolve_relative_path(bundle_path, bundle_payload.get("multi_strategy_config_path"))
        if not config_path:
            raise FileNotFoundError(f"Run bundle missing multi_strategy_config_path: {bundle_path}")
        portfolio_config = load_multi_strategy_portfolio_config(config_path)
        summary = {
            "source_kind": "run_bundle",
            "source_path": str(bundle_path),
            "activation_applied": bool(bundle_payload.get("activation_applied", portfolio_config.activation_applied)),
            "source_portfolio_path": bundle_payload.get("source_artifact_path") or portfolio_config.source_portfolio_path,
            "fail_if_no_active_strategies": bool(portfolio_config.fail_if_no_active_strategies),
            "selected_strategy_variants": list(bundle_payload.get("selected_strategy_variants", [])),
            "active_strategy_count": int(bundle_payload.get("active_strategy_count", portfolio_config.active_strategy_count)),
            "active_unconditional_count": int(bundle_payload.get("active_unconditional_count", portfolio_config.active_unconditional_count)),
            "active_conditional_count": int(bundle_payload.get("active_conditional_count", portfolio_config.active_conditional_count)),
            "inactive_conditional_count": int(bundle_payload.get("inactive_conditional_count", portfolio_config.inactive_conditional_count)),
        }
        return StrategyExecutionHandoff(
            source_kind="run_bundle",
            source_path=str(bundle_path),
            portfolio_config=portfolio_config,
            summary=summary,
            warnings=warnings,
        )

    payload_path = path / "activated_strategy_portfolio.json" if path.is_dir() and (path / "activated_strategy_portfolio.json").exists() else path
    payload = _safe_read_json(payload_path)
    if payload.get("active_strategies") is not None or payload.get("strategies") is not None:
        active_rows = (
            list(payload.get("active_strategies") or [])
            if handoff_config.use_activated_portfolio_for_paper
            else list(payload.get("strategies") or [])
        )
        if not active_rows and payload.get("strategies") and handoff_config.use_activated_portfolio_for_paper:
            active_rows = [row for row in payload.get("strategies", []) if bool(row.get("is_active", False))]
        counts = {
            "active_strategy_count": int(payload.get("summary", {}).get("active_row_count", len(active_rows)) or len(active_rows)),
            "active_unconditional_count": int(payload.get("summary", {}).get("activated_unconditional_count", 0) or 0),
            "active_conditional_count": int(payload.get("summary", {}).get("activated_conditional_count", 0) or 0),
            "inactive_conditional_count": int(payload.get("summary", {}).get("inactive_conditional_count", 0) or 0),
        }
        if not any(counts.values()):
            counts = _derive_counts_from_rows(list(payload.get("strategies") or active_rows))
        summary = {
            "source_kind": "activated_portfolio",
            "source_path": str(payload_path),
            "activation_applied": True,
            "source_portfolio_path": str(payload.get("source_portfolio_path") or payload_path),
            "use_activated_portfolio_for_paper": bool(handoff_config.use_activated_portfolio_for_paper),
            "fail_if_no_active_strategies": bool(handoff_config.fail_if_no_active_strategies),
            **counts,
        }
        if not active_rows:
            warnings.append("no_active_strategies")
            return StrategyExecutionHandoff(
                source_kind="activated_portfolio",
                source_path=str(payload_path),
                portfolio_config=None,
                summary=summary,
                warnings=warnings,
            )
        portfolio_config = _build_portfolio_config_from_rows(
            rows=active_rows,
            source_portfolio_path=str(payload.get("source_portfolio_path") or payload_path),
            source_activated_portfolio_path=str(payload_path),
            activation_applied=True,
            handoff_config=handoff_config,
            counts=counts,
        )
        return StrategyExecutionHandoff(
            source_kind="activated_portfolio",
            source_path=str(payload_path),
            portfolio_config=portfolio_config,
            summary=summary,
            warnings=warnings,
        )

    if payload.get("selected_strategies") is not None:
        selected_rows = list(payload.get("selected_strategies") or [])
        counts = _derive_counts_from_rows(selected_rows)
        portfolio_config = _build_portfolio_config_from_rows(
            rows=selected_rows,
            source_portfolio_path=str(payload_path),
            source_activated_portfolio_path=None,
            activation_applied=False,
            handoff_config=handoff_config,
            counts=counts,
        )
        return StrategyExecutionHandoff(
            source_kind="strategy_portfolio",
            source_path=str(payload_path),
            portfolio_config=portfolio_config,
            summary={
                "source_kind": "strategy_portfolio",
                "source_path": str(payload_path),
                "activation_applied": False,
                "source_portfolio_path": str(payload_path),
                "fail_if_no_active_strategies": bool(handoff_config.fail_if_no_active_strategies),
                **counts,
            },
            warnings=warnings,
        )

    from trading_platform.config.loader import load_multi_strategy_portfolio_config

    portfolio_config = load_multi_strategy_portfolio_config(path)
    return StrategyExecutionHandoff(
        source_kind="multi_strategy_config",
        source_path=str(path),
        portfolio_config=portfolio_config,
        summary={
            "source_kind": "multi_strategy_config",
            "source_path": str(path),
            "activation_applied": bool(portfolio_config.activation_applied),
            "source_portfolio_path": portfolio_config.source_portfolio_path,
            "fail_if_no_active_strategies": bool(portfolio_config.fail_if_no_active_strategies),
            "active_strategy_count": int(portfolio_config.active_strategy_count or len(portfolio_config.enabled_sleeves)),
            "active_unconditional_count": int(portfolio_config.active_unconditional_count or len(portfolio_config.enabled_sleeves)),
            "active_conditional_count": int(portfolio_config.active_conditional_count or 0),
            "inactive_conditional_count": int(portfolio_config.inactive_conditional_count or 0),
        },
        warnings=warnings,
    )


def write_strategy_execution_handoff_summary(
    *,
    handoff: StrategyExecutionHandoff,
    output_dir: str | Path,
    artifact_name: str,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    path = output_path / artifact_name
    payload = {
        "source_kind": handoff.source_kind,
        "source_path": handoff.source_path,
        "warnings": list(handoff.warnings),
        "summary": handoff.summary,
        "portfolio_config": asdict(handoff.portfolio_config) if handoff.portfolio_config is not None else None,
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path
