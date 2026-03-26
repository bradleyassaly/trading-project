from __future__ import annotations

import json
from pathlib import Path

from trading_platform.config.loader import load_multi_strategy_portfolio_config
from trading_platform.portfolio.strategy_execution_handoff import (
    StrategyExecutionHandoffConfig,
    resolve_strategy_execution_handoff,
)


def test_resolve_activated_portfolio_with_only_unconditional_active_rows(tmp_path: Path) -> None:
    activated_path = tmp_path / "activated_strategy_portfolio.json"
    activated_path.write_text(
        json.dumps(
            {
                "source_portfolio_path": "artifacts/strategy_portfolio/run_current/strategy_portfolio.json",
                "summary": {
                    "active_row_count": 1,
                    "activated_unconditional_count": 1,
                    "activated_conditional_count": 0,
                    "inactive_conditional_count": 1,
                },
                "active_strategies": [
                    {
                        "preset_name": "generated_base",
                        "target_capital_fraction": 0.5,
                        "generated_preset_path": str(tmp_path / "generated_base.json"),
                        "promotion_variant": "unconditional",
                        "activation_state": "active",
                        "is_active": True,
                    }
                ],
                "strategies": [
                    {
                        "preset_name": "generated_base",
                        "promotion_variant": "unconditional",
                        "is_active": True,
                    },
                    {
                        "preset_name": "generated_conditional",
                        "promotion_variant": "conditional",
                        "condition_id": "regime::risk_on",
                        "is_active": False,
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    handoff = resolve_strategy_execution_handoff(activated_path)

    assert handoff.portfolio_config is not None
    assert len(handoff.portfolio_config.sleeves) == 1
    assert handoff.summary["active_strategy_count"] == 1
    assert handoff.summary["inactive_conditional_count"] == 1


def test_resolve_activated_portfolio_with_active_conditional_rows(tmp_path: Path) -> None:
    activated_path = tmp_path / "activated_strategy_portfolio.json"
    activated_path.write_text(
        json.dumps(
            {
                "source_portfolio_path": "artifacts/strategy_portfolio/run_current/strategy_portfolio.json",
                "summary": {
                    "active_row_count": 2,
                    "activated_unconditional_count": 1,
                    "activated_conditional_count": 1,
                    "inactive_conditional_count": 0,
                },
                "active_strategies": [
                    {
                        "preset_name": "generated_base",
                        "target_capital_fraction": 0.5,
                        "generated_preset_path": str(tmp_path / "generated_base.json"),
                        "promotion_variant": "unconditional",
                        "activation_state": "active",
                        "is_active": True,
                    },
                    {
                        "preset_name": "generated_conditional",
                        "target_capital_fraction": 0.5,
                        "generated_preset_path": str(tmp_path / "generated_conditional.json"),
                        "promotion_variant": "conditional",
                        "condition_id": "regime::risk_on",
                        "condition_type": "regime",
                        "activation_state": "active",
                        "is_active": True,
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    handoff = resolve_strategy_execution_handoff(activated_path)

    assert handoff.portfolio_config is not None
    assert handoff.portfolio_config.active_conditional_count == 1
    assert handoff.portfolio_config.sleeves[1].condition_id == "regime::risk_on"


def test_resolve_activated_portfolio_with_zero_active_strategies(tmp_path: Path) -> None:
    activated_path = tmp_path / "activated_strategy_portfolio.json"
    activated_path.write_text(
        json.dumps(
            {
                "summary": {
                    "active_row_count": 0,
                    "activated_unconditional_count": 0,
                    "activated_conditional_count": 0,
                    "inactive_conditional_count": 2,
                },
                "active_strategies": [],
                "strategies": [
                    {"preset_name": "generated_conditional_a", "promotion_variant": "conditional", "is_active": False},
                    {"preset_name": "generated_conditional_b", "promotion_variant": "conditional", "is_active": False},
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    handoff = resolve_strategy_execution_handoff(
        activated_path,
        config=StrategyExecutionHandoffConfig(fail_if_no_active_strategies=False),
    )

    assert handoff.portfolio_config is None
    assert "no_active_strategies" in handoff.warnings
    assert handoff.summary["inactive_conditional_count"] == 2


def test_resolve_run_bundle_preserves_activation_metadata(tmp_path: Path) -> None:
    config_path = tmp_path / "multi_strategy.json"
    config_path.write_text(
        json.dumps(
            {
                "sleeves": [
                    {
                        "sleeve_name": "generated_base",
                        "preset_name": "generated_base",
                        "target_capital_weight": 1.0,
                        "promotion_variant": "unconditional",
                        "activation_state": "active",
                        "is_active": True,
                    }
                ],
                "activation_applied": True,
                "active_strategy_count": 1,
                "active_unconditional_count": 1,
                "active_conditional_count": 0,
                "inactive_conditional_count": 1,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    bundle_path = tmp_path / "strategy_portfolio_run_bundle.json"
    bundle_path.write_text(
        json.dumps(
            {
                "multi_strategy_config_path": str(config_path),
                "activation_applied": True,
                "active_strategy_count": 1,
                "active_unconditional_count": 1,
                "active_conditional_count": 0,
                "inactive_conditional_count": 1,
                "selected_strategy_variants": [
                    {
                        "preset_name": "generated_base",
                        "promotion_variant": "unconditional",
                        "activation_state": "active",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    handoff = resolve_strategy_execution_handoff(bundle_path)

    assert handoff.source_kind == "run_bundle"
    assert handoff.summary["activation_applied"] is True
    assert handoff.summary["active_strategy_count"] == 1


def test_multi_strategy_loader_accepts_activation_aware_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "multi_strategy.json"
    config_path.write_text(
        json.dumps(
            {
                "sleeves": [
                    {
                        "sleeve_name": "generated_conditional",
                        "preset_name": "generated_conditional",
                        "target_capital_weight": 1.0,
                        "promotion_variant": "conditional",
                        "condition_id": "regime::risk_on",
                        "condition_type": "regime",
                        "activation_state": "active",
                        "is_active": True,
                        "activation_reason": "regime=risk_on",
                        "portfolio_bucket": "conditional",
                    }
                ],
                "activation_applied": True,
                "use_activated_portfolio_for_paper": True,
                "fail_if_no_active_strategies": False,
                "include_inactive_conditionals_in_reports": True,
                "fail_if_no_usable_symbols": False,
                "fail_if_zero_targets_after_validation": False,
                "allow_latest_close_fallback": True,
                "min_usable_symbol_fraction": 0.5,
                "active_strategy_count": 1,
                "active_unconditional_count": 0,
                "active_conditional_count": 1,
                "inactive_conditional_count": 0,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    config = load_multi_strategy_portfolio_config(config_path)

    assert config.activation_applied is True
    assert config.active_conditional_count == 1
    assert config.allow_latest_close_fallback is True
    assert config.min_usable_symbol_fraction == 0.5
    assert config.sleeves[0].condition_id == "regime::risk_on"
