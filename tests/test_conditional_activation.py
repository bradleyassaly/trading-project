from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.portfolio.conditional_activation import (
    ConditionalActivationConfig,
    activate_strategy_portfolio,
    load_activated_strategy_portfolio,
)


def _write_portfolio(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "policy": {
            "evaluate_conditional_activation": True,
            "activation_context_sources": ["regime", "benchmark_context", "sub_universe"],
            "include_inactive_conditionals_in_output": True,
        },
        "selected_strategies": [
            {
                "preset_name": "generated_base",
                "promotion_variant": "unconditional",
                "activation_conditions": [],
                "activation_state": "always_on",
                "portfolio_bucket": "primary",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
            },
            {
                "preset_name": "generated_regime",
                "promotion_variant": "conditional",
                "condition_id": "regime::high_vol|uptrend|low_dispersion",
                "condition_type": "regime",
                "activation_conditions": [
                    {"condition_id": "regime::high_vol|uptrend|low_dispersion", "condition_type": "regime"}
                ],
                "activation_state": "inactive_until_condition_match",
                "portfolio_bucket": "conditional",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.12,
                "rationale": "regime edge",
            },
            {
                "preset_name": "generated_benchmark",
                "promotion_variant": "conditional",
                "condition_id": "benchmark_context::risk_on_outperform_broad",
                "condition_type": "benchmark_context",
                "activation_conditions": [
                    {"condition_id": "benchmark_context::risk_on_outperform_broad", "condition_type": "benchmark_context"}
                ],
                "activation_state": "inactive_until_condition_match",
                "portfolio_bucket": "conditional",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.1,
                "rationale": "benchmark edge",
            },
        ],
        "shadow_strategies": [],
    }
    (root / "strategy_portfolio.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return root


def _write_regime(root: Path, regime_key: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    volatility_regime, trend_regime, dispersion_regime = regime_key.split("|", 2)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-26",
                "regime_key": regime_key,
                "volatility_regime": volatility_regime,
                "trend_regime": trend_regime,
                "dispersion_regime": dispersion_regime,
            }
        ]
    ).to_csv(root / "regime_labels_by_date.csv", index=False)
    return root


def _write_metadata(root: Path, benchmark_label: str = "risk_on_outperform_broad") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "sub_universe_id": "liquid_trend_candidates",
                "benchmark_context_label": benchmark_label,
                "relative_strength_20": 0.03,
            }
        ]
    ).to_csv(root / "universe_enrichment.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "sub_universe_id": "liquid_trend_candidates",
                "inclusion_status": "included",
            }
        ]
    ).to_csv(root / "sub_universe_snapshot.csv", index=False)
    return root


def test_unconditional_strategies_always_active(tmp_path: Path) -> None:
    portfolio_dir = _write_portfolio(tmp_path / "portfolio")
    _write_regime(tmp_path / "regime", "high_vol|uptrend|low_dispersion")
    _write_metadata(tmp_path / "metadata")

    result = activate_strategy_portfolio(
        portfolio_path=portfolio_dir,
        output_dir=tmp_path / "activated",
        config=ConditionalActivationConfig(evaluate_conditional_activation=True),
        regime_labels_path=tmp_path / "regime",
        metadata_dir=tmp_path / "metadata",
    )
    payload = load_activated_strategy_portfolio(tmp_path / "activated")

    assert result["active_count"] >= 1
    base_row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_base")
    assert base_row["is_active"] is True
    assert base_row["activation_state"] == "active"


def test_matching_regime_condition_activates_strategy(tmp_path: Path) -> None:
    portfolio_dir = _write_portfolio(tmp_path / "portfolio")
    _write_regime(tmp_path / "regime", "high_vol|uptrend|low_dispersion")
    _write_metadata(tmp_path / "metadata")

    payload = load_activated_strategy_portfolio(
        activate_strategy_portfolio(
            portfolio_path=portfolio_dir,
            output_dir=tmp_path / "activated",
            config=ConditionalActivationConfig(evaluate_conditional_activation=True),
            regime_labels_path=tmp_path / "regime",
            metadata_dir=tmp_path / "metadata",
        )["activated_strategy_portfolio_json_path"]
    )

    row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_regime")
    assert row["is_active"] is True
    assert row["matched_conditions"]


def test_non_matching_regime_condition_leaves_strategy_inactive(tmp_path: Path) -> None:
    portfolio_dir = _write_portfolio(tmp_path / "portfolio")
    _write_regime(tmp_path / "regime", "low_vol|downtrend|high_dispersion")
    _write_metadata(tmp_path / "metadata")

    payload = load_activated_strategy_portfolio(
        activate_strategy_portfolio(
            portfolio_path=portfolio_dir,
            output_dir=tmp_path / "activated",
            config=ConditionalActivationConfig(evaluate_conditional_activation=True),
            regime_labels_path=tmp_path / "regime",
            metadata_dir=tmp_path / "metadata",
        )["activated_strategy_portfolio_json_path"]
    )

    row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_regime")
    assert row["is_active"] is False
    assert row["unmatched_conditions"]


def test_matching_benchmark_context_activates_strategy(tmp_path: Path) -> None:
    portfolio_dir = _write_portfolio(tmp_path / "portfolio")
    _write_regime(tmp_path / "regime", "high_vol|uptrend|low_dispersion")
    _write_metadata(tmp_path / "metadata", benchmark_label="risk_on_outperform_broad")

    payload = load_activated_strategy_portfolio(
        activate_strategy_portfolio(
            portfolio_path=portfolio_dir,
            output_dir=tmp_path / "activated",
            config=ConditionalActivationConfig(evaluate_conditional_activation=True),
            regime_labels_path=tmp_path / "regime",
            metadata_dir=tmp_path / "metadata",
        )["activated_strategy_portfolio_json_path"]
    )

    row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_benchmark")
    assert row["is_active"] is True


def test_mixed_portfolio_activation_preserves_metadata(tmp_path: Path) -> None:
    portfolio_dir = _write_portfolio(tmp_path / "portfolio")
    _write_regime(tmp_path / "regime", "high_vol|uptrend|low_dispersion")
    _write_metadata(tmp_path / "metadata")

    result = activate_strategy_portfolio(
        portfolio_path=portfolio_dir,
        output_dir=tmp_path / "activated",
        config=ConditionalActivationConfig(evaluate_conditional_activation=True),
        regime_labels_path=tmp_path / "regime",
        metadata_dir=tmp_path / "metadata",
    )
    payload = load_activated_strategy_portfolio(tmp_path / "activated")

    assert Path(result["activated_strategy_portfolio_csv_path"]).exists()
    assert payload["summary"]["activated_unconditional_count"] == 1
    assert payload["summary"]["activated_conditional_count"] == 2
    row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_benchmark")
    assert row["portfolio_bucket"] == "conditional"
    assert row["ranking_metric"] == "mean_spearman_ic"
    assert row["ranking_value"] == 0.1


def test_activation_respects_enabled_context_sources(tmp_path: Path) -> None:
    portfolio_dir = _write_portfolio(tmp_path / "portfolio")
    _write_regime(tmp_path / "regime", "high_vol|uptrend|low_dispersion")
    _write_metadata(tmp_path / "metadata", benchmark_label="risk_on_outperform_broad")

    payload = load_activated_strategy_portfolio(
        activate_strategy_portfolio(
            portfolio_path=portfolio_dir,
            output_dir=tmp_path / "activated",
            config=ConditionalActivationConfig(
                evaluate_conditional_activation=True,
                activation_context_sources=["regime"],
            ),
            regime_labels_path=tmp_path / "regime",
            metadata_dir=tmp_path / "metadata",
        )["activated_strategy_portfolio_json_path"]
    )

    regime_row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_regime")
    benchmark_row = next(row for row in payload["strategies"] if row["preset_name"] == "generated_benchmark")
    assert regime_row["is_active"] is True
    assert benchmark_row["is_active"] is False
    assert "context_source_disabled:benchmark_context" in benchmark_row["activation_reason"]
