from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult


def test_cmd_paper_run_writes_artifacts(monkeypatch, tmp_path: Path, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_run_paper_trading_cycle(*, config, state_store, auto_apply_fills):
        captured["config"] = config
        captured["state_store"] = state_store
        captured["auto_apply_fills"] = auto_apply_fills
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=10_000.0),
            latest_prices={"AAPL": 103.0, "MSFT": 203.0},
            latest_scores={"AAPL": 1.0, "MSFT": 2.0},
            latest_target_weights={"AAPL": 0.5, "MSFT": 0.5},
            scheduled_target_weights={"AAPL": 0.5, "MSFT": 0.5},
            orders=[],
            fills=[],
            skipped_symbols=[],
            diagnostics={"ok": True},
        )

    def fake_write_paper_trading_artifacts(*, result, output_dir):
        captured["output_dir"] = Path(output_dir)
        return {
            "orders_path": Path(output_dir) / "paper_orders.csv",
            "positions_path": Path(output_dir) / "paper_positions.csv",
            "targets_path": Path(output_dir) / "paper_target_weights.csv",
            "summary_path": Path(output_dir) / "paper_summary.json",
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run.run_paper_trading_cycle",
        fake_run_paper_trading_cycle,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run.write_paper_trading_artifacts",
        fake_write_paper_trading_artifacts,
    )

    args = SimpleNamespace(
        symbols=["AAPL", "MSFT"],
        universe=None,
        signal_source="legacy",
        strategy="sma_cross",
        fast=20,
        slow=100,
        lookback=20,
        top_n=2,
        weighting_scheme="equal",
        vol_window=20,
        min_score=None,
        max_weight=None,
        max_names_per_group=None,
        max_group_weight=None,
        group_map_path=None,
        rebalance_frequency="daily",
        timing="next_bar",
        initial_cash=100_000.0,
        min_trade_dollars=25.0,
        lot_size=1,
        reserve_cash_pct=0.0,
        composite_artifact_dir=None,
        composite_horizon=1,
        composite_weighting_scheme="equal",
        composite_portfolio_mode="long_only_top_n",
        composite_long_quantile=0.2,
        composite_short_quantile=0.2,
        min_price=None,
        min_volume=None,
        min_avg_dollar_volume=None,
        max_adv_participation=0.05,
        max_position_pct_of_adv=0.1,
        max_notional_per_name=None,
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
        auto_apply_fills=False,
    )

    cmd_paper_run(args)

    assert captured["config"].symbols == ["AAPL", "MSFT"]
    assert captured["auto_apply_fills"] is False
    assert captured["output_dir"] == tmp_path / "paper"

    stdout = capsys.readouterr().out
    assert "Running paper trading cycle for 2 symbol(s): AAPL, MSFT" in stdout
    assert "As of: 2025-01-04" in stdout
    assert "Orders: 0" in stdout
    assert "Fills: 0" in stdout
    assert "Cash: 10,000.00" in stdout
    assert "Equity: 10,000.00" in stdout
    assert "Artifacts:" in stdout
