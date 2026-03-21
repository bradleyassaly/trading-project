from __future__ import annotations

from types import SimpleNamespace

from trading_platform.cli.presets import apply_cli_preset, build_decision_memo_payload


def test_apply_cli_preset_sets_defaults_when_not_explicit() -> None:
    args = SimpleNamespace(
        preset="xsec_nasdaq100_momentum_v1_research",
        universe=None,
        strategy="sma_cross",
        lookback_bars=126,
        skip_bars=0,
        top_n=3,
        rebalance_bars=21,
        portfolio_construction_mode="transition",
        benchmark=None,
        cost_bps=None,
        lookback_bars_values=None,
        skip_bars_values=None,
        top_n_values=None,
        rebalance_bars_values=None,
        _cli_argv=["--preset", "xsec_nasdaq100_momentum_v1_research"],
    )

    preset = apply_cli_preset(args)

    assert preset is not None
    assert args.universe == "nasdaq100"
    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars == 84
    assert args.top_n == 2
    assert args.portfolio_construction_mode == "pure_topn"


def test_apply_cli_preset_respects_explicit_overrides() -> None:
    args = SimpleNamespace(
        preset="xsec_nasdaq100_momentum_v1_deploy",
        universe=None,
        strategy="sma_cross",
        lookback_bars=126,
        skip_bars=0,
        top_n=5,
        rebalance_bars=21,
        portfolio_construction_mode="pure_topn",
        benchmark=None,
        cost_bps=None,
        min_avg_dollar_volume=None,
        weighting_scheme="equal",
        lookback_bars_values=None,
        skip_bars_values=None,
        top_n_values=None,
        rebalance_bars_values=None,
        _cli_argv=[
            "--preset",
            "xsec_nasdaq100_momentum_v1_deploy",
            "--top-n",
            "5",
            "--weighting-scheme",
            "equal",
        ],
    )

    apply_cli_preset(args)

    assert args.strategy == "xsec_momentum_topn"
    assert args.top_n == 5
    assert args.weighting_scheme == "equal"
    assert args.min_avg_dollar_volume == 50_000_000.0


def test_build_decision_memo_payload_contains_both_presets() -> None:
    payload = build_decision_memo_payload(
        research_preset_name="xsec_nasdaq100_momentum_v1_research",
        deploy_preset_name="xsec_nasdaq100_momentum_v1_deploy",
    )

    assert payload["family_version"] == "xsec_nasdaq100_momentum_v1"
    assert payload["research_preset"]["params"]["portfolio_construction_mode"] == "pure_topn"
    assert payload["deploy_preset"]["params"]["portfolio_construction_mode"] == "transition"
