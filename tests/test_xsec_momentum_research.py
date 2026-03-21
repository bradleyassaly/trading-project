from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.xsec_momentum import (
    build_close_panel,
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

    selection, weights = build_xsec_topn_weights(scores, top_n=2, rebalance_bars=1)

    assert selection.iloc[0].tolist() == [1.0, 1.0, 0.0]
    assert weights.iloc[0].tolist() == [0.5, 0.5, 0.0]
    assert weights.iloc[1].tolist() == [0.0, 0.5, 0.5]


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
