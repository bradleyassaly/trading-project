from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.xsec_momentum import (
    build_close_panel,
    compute_xsec_benchmark_returns,
    build_xsec_topn_weights,
    compute_xsec_momentum_scores,
    run_xsec_momentum_topn,
)


def _prepared(symbol: str, closes: list[float]) -> dict[str, object]:
    dates = pd.date_range("2024-01-01", periods=len(closes), freq="D")
    return {
        "df": pd.DataFrame({"timestamp": dates, "Close": closes}),
        "path": Path(f"{symbol}.parquet"),
        "effective_start": str(dates.min().date()),
        "effective_end": str(dates.max().date()),
    }


def _prepared_with_offset(symbol: str, closes: list[float], offset: int) -> dict[str, object]:
    dates = pd.date_range("2024-01-01", periods=len(closes), freq="B") + pd.offsets.BDay(offset)
    return {
        "df": pd.DataFrame({"timestamp": dates, "Close": closes}),
        "path": Path(f"{symbol}.parquet"),
        "effective_start": str(dates.min().date()),
        "effective_end": str(dates.max().date()),
        "date_col": "timestamp",
        "rows": len(closes),
    }


def test_xsec_momentum_scores_rank_stronger_symbol_higher() -> None:
    close_panel, _ = build_close_panel(
        {
            "AAPL": _prepared("AAPL", [10, 11, 12, 13]),
            "MSFT": _prepared("MSFT", [10, 10.2, 10.3, 10.4]),
        }
    )

    scores = compute_xsec_momentum_scores(close_panel, lookback_bars=2, skip_bars=0)

    assert scores.loc[scores.index[-1], "AAPL"] > scores.loc[scores.index[-1], "MSFT"]


def test_xsec_topn_builds_equal_weight_selection() -> None:
    scores = pd.DataFrame(
        {
            "AAPL": [0.5, 0.1],
            "MSFT": [0.4, 0.3],
            "NVDA": [0.2, 0.6],
        },
        index=pd.date_range("2024-01-01", periods=2, freq="D"),
    )

    close_panel = pd.DataFrame(
        {
            "AAPL": [100.0, 101.0],
            "MSFT": [100.0, 101.0],
            "NVDA": [100.0, 101.0],
        },
        index=scores.index,
    )

    selection, weights, diagnostics = build_xsec_topn_weights(
        scores,
        close_panel=close_panel,
        asset_returns=close_panel.pct_change(fill_method=None),
        top_n=2,
        rebalance_bars=1,
    )

    assert selection.iloc[0].tolist() == [1.0, 1.0, 0.0]
    assert weights.iloc[0].tolist() == [0.5, 0.5, 0.0]
    assert weights.iloc[1].tolist() == [0.0, 0.5, 0.5]
    assert diagnostics.iloc[0]["selected_symbol_count"] == 2
    assert diagnostics.iloc[0]["target_selected_count"] == 2
    assert diagnostics.iloc[0]["realized_holdings_count"] == 2
    assert diagnostics.iloc[0]["weight_sum"] == pytest.approx(1.0)
    assert diagnostics.iloc[0]["available_symbol_count"] == 3


def test_run_xsec_momentum_topn_reports_portfolio_diagnostics() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [10, 11, 12, 13, 14, 15]),
            "MSFT": _prepared("MSFT", [10, 10.1, 10.2, 10.3, 10.4, 10.5]),
            "NVDA": _prepared("NVDA", [10, 10.2, 10.4, 10.6, 10.8, 11.0]),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
    )

    assert "average_number_of_holdings" in result.summary
    assert "rebalance_count" in result.summary
    assert "percent_invested" in result.summary
    assert "initial_equity" in result.summary
    assert "final_equity" in result.summary
    assert "portfolio_construction_mode" in result.summary
    assert "average_realized_holdings_count" in result.summary
    assert result.summary["final_equity"] < 20_000.0


def test_xsec_topn_two_symbol_top1_return_matches_hand_calculation() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 105, 110, 121]),
            "MSFT": _prepared("MSFT", [100, 101, 102, 103]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
    )

    # next-bar execution means the portfolio captures AAPL on bars 3 and 4
    expected_portfolio_return = (1.0 + (110 / 105 - 1.0)) * (1.0 + (121 / 110 - 1.0)) - 1.0
    expected_benchmark_return = (
        (1.0 + (((105 / 100 - 1.0) + (101 / 100 - 1.0)) / 2.0))
        * (1.0 + (((110 / 105 - 1.0) + (102 / 101 - 1.0)) / 2.0))
        * (1.0 + (((121 / 110 - 1.0) + (103 / 102 - 1.0)) / 2.0))
        - 1.0
    )

    assert result.summary["Return [%]"] == pytest.approx(expected_portfolio_return * 100.0)
    assert result.summary["benchmark_return_pct"] == pytest.approx(expected_benchmark_return * 100.0)
    assert result.summary["benchmark_type"] == "equal_weight"


def test_xsec_equal_weight_benchmark_matches_toy_hand_calculation() -> None:
    asset_returns = pd.DataFrame(
        {
            "AAPL": [0.10, -0.05, 0.20],
            "MSFT": [0.00, 0.10, -0.10],
        },
        index=pd.date_range("2024-01-01", periods=3, freq="D"),
    )

    benchmark = compute_xsec_benchmark_returns(asset_returns, benchmark_type="equal_weight")

    expected = pd.Series([0.05, 0.025, 0.05], index=asset_returns.index)
    pd.testing.assert_series_equal(benchmark, expected)


def test_xsec_topn_no_trade_window_has_zero_return_and_zero_invested() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 100, 100, 100]),
            "MSFT": _prepared("MSFT", [100, 100, 100, 100]),
        },
        lookback_bars=10,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
    )

    assert result.summary["Return [%]"] == pytest.approx(0.0)
    assert result.summary["percent_invested"] == pytest.approx(0.0)
    assert result.summary["average_gross_exposure"] == pytest.approx(0.0)
    assert result.summary["trade_count"] == 0


def test_xsec_topn_stays_invested_between_rebalances_when_scores_are_available() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 103, 106, 109, 112, 115, 118, 121]),
            "MSFT": _prepared("MSFT", [100, 102, 104, 106, 108, 110, 112, 114]),
            "NVDA": _prepared("NVDA", [100, 101, 102, 103, 104, 105, 106, 107]),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=2,
        commission=0.0,
        cash=10_000.0,
    )

    active_weights = result.target_weights.iloc[2:]
    assert not active_weights.empty
    assert all(abs(weight_sum - 1.0) < 1e-9 for weight_sum in active_weights.sum(axis=1))
    assert result.summary["average_eligible_symbols"] >= 2.0
    assert result.summary["average_selected_symbols"] >= 1.0
    assert result.summary["percent_empty_rebalances"] < 50.0
    assert (result.rebalance_diagnostics["selected_symbol_count"].iloc[1:] == 2).all()
    assert "AAPL" in result.rebalance_diagnostics.iloc[-1]["selected_symbols"]
    assert "MSFT" in result.rebalance_diagnostics.iloc[-1]["selected_symbols"]


def test_xsec_topn_active_window_uses_prior_history_for_selection() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 101, 102, 103, 104, 105, 106, 107]),
            "MSFT": _prepared("MSFT", [100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5]),
            "NVDA": _prepared("NVDA", [100, 99, 98, 97, 96, 95, 94, 93]),
        },
        lookback_bars=3,
        skip_bars=0,
        top_n=2,
        rebalance_bars=2,
        commission=0.0,
        cash=10_000.0,
        active_start="2024-01-05",
        active_end="2024-01-08",
    )

    assert len(result.timeseries) == 4
    assert result.summary["percent_invested"] > 0.0
    assert result.summary["average_selected_symbols"] == pytest.approx(2.0)
    assert result.summary["percent_empty_rebalances"] == pytest.approx(0.0)


def test_xsec_topn_returns_do_not_explode_on_realistic_prices() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 101, 102, 103, 104, 105]),
            "MSFT": _prepared("MSFT", [100, 100.5, 101, 101.5, 102, 102.5]),
            "NVDA": _prepared("NVDA", [100, 102, 101, 103, 102, 104]),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=2,
        commission=0.0,
        cash=10_000.0,
    )

    assert abs(result.summary["Return [%]"]) < 100.0
    assert abs(result.summary["benchmark_return_pct"]) < 100.0


def test_xsec_topn_nonzero_cost_reduces_net_return_and_reports_turnover() -> None:
    no_cost = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 90, 120, 85, 130]),
            "MSFT": _prepared("MSFT", [100, 90, 115, 85, 125, 80]),
            "NVDA": _prepared("NVDA", [100, 95, 96, 97, 98, 99]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
    )
    with_cost = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 90, 120, 85, 130]),
            "MSFT": _prepared("MSFT", [100, 90, 115, 85, 125, 80]),
            "NVDA": _prepared("NVDA", [100, 95, 96, 97, 98, 99]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.01,
        cash=10_000.0,
    )

    assert with_cost.summary["net_return_pct"] < no_cost.summary["net_return_pct"]
    assert with_cost.summary["cost_drag_return_pct"] > 0.0
    assert with_cost.summary["mean_turnover"] > 0.0
    assert with_cost.summary["annualized_turnover"] > 0.0
    assert with_cost.summary["total_transaction_cost"] > 0.0


def test_xsec_topn_rebalance_diagnostics_include_turnover_and_cost() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 90, 120, 85, 130]),
            "MSFT": _prepared("MSFT", [100, 90, 115, 85, 125, 80]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.005,
        cash=10_000.0,
    )

    assert {"turnover", "transaction_cost", "portfolio_return_gross", "portfolio_return_net"}.issubset(result.rebalance_diagnostics.columns)
    assert result.rebalance_diagnostics["turnover"].fillna(0.0).ge(0.0).all()
    assert result.rebalance_diagnostics["transaction_cost"].fillna(0.0).ge(0.0).all()


def test_xsec_topn_backward_compatible_without_constraints() -> None:
    baseline = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 102, 104, 106, 108, 110]),
            "MSFT": _prepared("MSFT", [100, 101, 102, 103, 104, 105]),
            "NVDA": _prepared("NVDA", [100, 101, 103, 104, 106, 108]),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
    )
    explicit_defaults = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 102, 104, 106, 108, 110]),
            "MSFT": _prepared("MSFT", [100, 101, 102, 103, 104, 105]),
            "NVDA": _prepared("NVDA", [100, 101, 103, 104, 106, 108]),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        max_position_weight=None,
        min_avg_dollar_volume=None,
        max_names_per_sector=None,
        turnover_buffer_bps=0.0,
        max_turnover_per_rebalance=None,
        weighting_scheme="equal",
        vol_lookback_bars=20,
    )

    pd.testing.assert_frame_equal(baseline.target_weights, explicit_defaults.target_weights)
    assert baseline.summary["Return [%]"] == pytest.approx(explicit_defaults.summary["Return [%]"])


def test_xsec_topn_liquidity_filter_excludes_ineligible_names() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": {
                "df": pd.DataFrame(
                    {
                        "timestamp": pd.date_range("2024-01-01", periods=6, freq="D"),
                        "Close": [100, 102, 104, 106, 108, 110],
                        "Volume": [2_000_000] * 6,
                    }
                ),
                "path": Path("AAPL.parquet"),
            },
            "MSFT": {
                "df": pd.DataFrame(
                    {
                        "timestamp": pd.date_range("2024-01-01", periods=6, freq="D"),
                        "Close": [100, 103, 106, 109, 112, 115],
                        "Volume": [10_000] * 6,
                    }
                ),
                "path": Path("MSFT.parquet"),
            },
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        min_avg_dollar_volume=50_000_000,
    )

    assert result.summary["liquidity_filter_active"] is True
    assert result.summary["total_liquidity_excluded_symbols"] > 0
    assert "MSFT:liquidity_filter" in result.rebalance_diagnostics.iloc[-1]["excluded_reasons"]


def test_xsec_topn_sector_cap_limits_holdings_per_sector(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.research.xsec_momentum.load_sector_map",
        lambda symbols: (
            {"AAPL": "TECH", "MSFT": "TECH", "NVDA": "TECH", "PEP": "STAPLES"},
            None,
        ),
    )

    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 103, 106, 109, 112, 115]),
            "MSFT": _prepared("MSFT", [100, 102, 104, 106, 108, 110]),
            "NVDA": _prepared("NVDA", [100, 104, 108, 112, 116, 120]),
            "PEP": _prepared("PEP", [100, 101, 102, 103, 104, 105]),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        max_names_per_sector=1,
    )

    assert result.summary["sector_cap_active"] is True
    assert result.summary["total_sector_cap_excluded_symbols"] > 0
    assert result.rebalance_diagnostics["selected_symbols"].iloc[-1].count("NVDA") + result.rebalance_diagnostics["selected_symbols"].iloc[-1].count("AAPL") + result.rebalance_diagnostics["selected_symbols"].iloc[-1].count("MSFT") <= 1


def test_xsec_topn_turnover_buffer_blocks_marginal_replacement() -> None:
    buffered = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 121, 122, 123, 124]),
            "MSFT": _prepared("MSFT", [100, 109, 118, 130, 132, 134]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        turnover_buffer_bps=2_000.0,
    )

    assert buffered.summary["turnover_buffer_blocked_replacements"] > 0


def test_xsec_topn_max_turnover_per_rebalance_partially_moves_toward_target() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 90, 120, 85, 130]),
            "MSFT": _prepared("MSFT", [100, 90, 115, 85, 125, 80]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        max_turnover_per_rebalance=0.5,
    )

    assert result.summary["turnover_cap_binding_count"] > 0
    assert result.rebalance_diagnostics["weight_sum"].max() <= 1.0 + 1e-9
    assert result.summary["average_realized_holdings_count"] <= 1.0
    assert result.summary["mean_turnover"] > 0.0


def test_xsec_pure_topn_keeps_realized_holdings_at_or_below_top_n_with_constraints() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 90, 120, 85, 130]),
            "MSFT": _prepared("MSFT", [100, 90, 115, 85, 125, 80]),
            "NVDA": _prepared("NVDA", [100, 91, 116, 84, 126, 79]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        max_position_weight=0.5,
        max_turnover_per_rebalance=0.5,
        portfolio_construction_mode="pure_topn",
    )

    assert result.summary["portfolio_construction_mode"] == "pure_topn"
    assert result.summary["average_realized_holdings_count"] <= 1.0
    assert not result.summary["realized_holdings_exceeded_top_n"]
    assert result.rebalance_diagnostics["realized_holdings_count"].max() <= 1


def test_xsec_transition_mode_can_exceed_top_n_when_turnover_cap_is_active() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared("AAPL", [100, 110, 90, 120, 85, 130]),
            "MSFT": _prepared("MSFT", [100, 90, 115, 85, 125, 80]),
            "NVDA": _prepared("NVDA", [100, 91, 116, 84, 126, 79]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        max_turnover_per_rebalance=0.5,
        portfolio_construction_mode="transition",
    )

    assert result.summary["portfolio_construction_mode"] == "transition"
    assert result.summary["turnover_cap_binding_count"] > 0
    assert result.summary["average_realized_holdings_count"] > 1.0
    assert result.rebalance_diagnostics["realized_holdings_count"].max() > 1


def test_xsec_pure_topn_warning_triggers_if_semantic_drift_occurs(monkeypatch) -> None:
    scores = pd.DataFrame(
        {"AAPL": [0.3], "MSFT": [0.2], "NVDA": [0.1]},
        index=pd.date_range("2024-01-01", periods=1, freq="D"),
    )
    close_panel = pd.DataFrame(
        {"AAPL": [100.0], "MSFT": [100.0], "NVDA": [100.0]},
        index=scores.index,
    )

    monkeypatch.setattr(
        "trading_platform.research.xsec_momentum._build_realized_weights",
        lambda **kwargs: (
            pd.Series({"AAPL": 0.34, "MSFT": 0.33, "NVDA": 0.33}),
            True,
        ),
    )

    _, _, diagnostics = build_xsec_topn_weights(
        scores,
        close_panel=close_panel,
        asset_returns=close_panel.pct_change(fill_method=None).fillna(0.0),
        top_n=1,
        rebalance_bars=1,
        portfolio_construction_mode="pure_topn",
    )

    assert bool(diagnostics.iloc[0]["realized_holdings_exceeded_top_n"]) is True
    assert "pure_topn_realized_holdings_exceeded_threshold" in diagnostics.iloc[0]["semantic_warning"]


def test_xsec_topn_inverse_vol_weighting_overweights_lower_vol_name() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "LOWVOL": _prepared("LOWVOL", [100, 101, 102, 103, 104, 105]),
            "HIGHVOL": _prepared("HIGHVOL", [100, 105, 95, 110, 90, 115]),
        },
        lookback_bars=1,
        skip_bars=0,
        top_n=2,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
        weighting_scheme="inv_vol",
        vol_lookback_bars=3,
    )

    last_weights = result.target_weights.iloc[-1]
    assert last_weights["LOWVOL"] > last_weights["HIGHVOL"]


def test_build_close_panel_uses_union_of_symbol_timestamps() -> None:
    close_panel, _ = build_close_panel(
        {
            "AAPL": _prepared_with_offset("AAPL", [100, 101, 102, 103, 104], 0),
            "MSFT": _prepared_with_offset("MSFT", [100, 101, 102, 103, 104], 0),
            "ARM": _prepared_with_offset("ARM", [50, 51, 52], 3),
        }
    )

    assert close_panel.index.min() == pd.Timestamp("2024-01-01")
    assert close_panel.index.max() == pd.Timestamp("2024-01-08")
    assert pd.isna(close_panel.loc[pd.Timestamp("2024-01-01"), "ARM"])
    assert close_panel.loc[pd.Timestamp("2024-01-04"), "ARM"] == pytest.approx(50.0)


def test_xsec_topn_handles_mixed_listing_dates_with_dynamic_eligibility() -> None:
    result = run_xsec_momentum_topn(
        prepared_frames={
            "AAPL": _prepared_with_offset("AAPL", [100, 102, 104, 106, 108, 110, 112, 114], 0),
            "MSFT": _prepared_with_offset("MSFT", [100, 101, 102, 103, 104, 105, 106, 107], 0),
            "ARM": _prepared_with_offset("ARM", [50, 55, 60, 66, 73], 3),
        },
        lookback_bars=2,
        skip_bars=0,
        top_n=2,
        rebalance_bars=1,
        commission=0.0,
        cash=10_000.0,
    )

    assert result.timeseries.index.min() == pd.Timestamp("2024-01-01")
    assert result.summary["percent_invested"] > 0.0
    assert result.summary["min_available_symbols"] == pytest.approx(2.0)
    assert result.summary["max_available_symbols"] == pytest.approx(3.0)
    assert result.summary["min_eligible_symbols"] == pytest.approx(0.0)
    assert result.summary["average_eligible_symbols"] >= 1.0
    assert result.summary["max_eligible_symbols"] >= 2.0
    assert result.rebalance_diagnostics["available_symbol_count"].max() == 3
    assert result.rebalance_diagnostics["eligible_symbol_count"].iloc[-1] >= 3
    assert "ARM" in result.rebalance_diagnostics.iloc[-1]["selected_symbols"]
