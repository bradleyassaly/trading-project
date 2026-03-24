from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult


def test_cmd_paper_run_writes_artifacts(monkeypatch, tmp_path: Path, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_run_paper_trading_cycle(*, config, state_store, execution_config, auto_apply_fills):
        captured["config"] = config
        captured["state_store"] = state_store
        captured["execution_config"] = execution_config
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
        use_alpaca_latest_data=True,
        latest_data_max_age_seconds=900,
        slippage_model="fixed_bps",
        slippage_buy_bps=5.0,
        slippage_sell_bps=7.0,
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
        auto_apply_fills=False,
        preset=None,
        lookback_bars=126,
        skip_bars=0,
        rebalance_bars=21,
        portfolio_construction_mode="pure_topn",
        max_position_weight=None,
        max_names_per_sector=None,
        turnover_buffer_bps=0.0,
        max_turnover_per_rebalance=None,
        vol_lookback_bars=20,
    )

    cmd_paper_run(args)

    assert captured["config"].symbols == ["AAPL", "MSFT"]
    assert captured["config"].use_alpaca_latest_data is True
    assert captured["config"].latest_data_max_age_seconds == 900
    assert captured["config"].slippage_model == "fixed_bps"
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


def test_cmd_paper_run_prints_xsec_preset_diagnostics(monkeypatch, tmp_path: Path, capsys) -> None:
    def fake_run_paper_trading_cycle(*, config, state_store, execution_config, auto_apply_fills):
        return PaperTradingRunResult(
            as_of="2025-01-21",
            state=PaperPortfolioState(cash=95_000.0),
            latest_prices={"AAPL": 103.0, "MSFT": 203.0},
            latest_scores={"AAPL": 0.1, "MSFT": 0.2},
            latest_target_weights={"AAPL": 0.5, "MSFT": 0.5},
            scheduled_target_weights={"AAPL": 0.5, "MSFT": 0.5},
            orders=[],
            fills=[],
            skipped_symbols=[],
            diagnostics={
                "preset_name": "xsec_nasdaq100_momentum_v1_deploy",
                "target_construction": {
                    "portfolio_construction_mode": "transition",
                    "rebalance_timestamp": "2025-01-21",
                    "selected_symbols": "AAPL,MSFT",
                    "target_selected_symbols": "AAPL,MSFT",
                    "realized_holdings_count": 2,
                    "realized_holdings_minus_top_n": 0,
                    "average_gross_exposure": 1.0,
                    "liquidity_excluded_count": 1,
                    "sector_cap_excluded_count": 0,
                    "turnover_cap_binding_count": 3,
                    "turnover_buffer_blocked_replacements": 0,
                    "semantic_warning": "",
                },
            },
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run.run_paper_trading_cycle",
        fake_run_paper_trading_cycle,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run.write_paper_trading_artifacts",
        lambda *, result, output_dir: {"summary_path": Path(output_dir) / "paper_summary.json"},
    )

    args = SimpleNamespace(
        symbols=None,
        universe="nasdaq100",
        signal_source="legacy",
        strategy="xsec_momentum_topn",
        fast=20,
        slow=100,
        lookback=20,
        lookback_bars=84,
        skip_bars=21,
        top_n=2,
        weighting_scheme="inv_vol",
        vol_window=20,
        vol_lookback_bars=20,
        min_score=None,
        max_weight=None,
        max_position_weight=0.5,
        max_names_per_group=None,
        max_group_weight=None,
        group_map_path=None,
        max_names_per_sector=None,
        turnover_buffer_bps=0.0,
        max_turnover_per_rebalance=0.5,
        portfolio_construction_mode="transition",
        rebalance_bars=21,
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
        min_avg_dollar_volume=50_000_000.0,
        max_adv_participation=0.05,
        max_position_pct_of_adv=0.1,
        max_notional_per_name=None,
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
        auto_apply_fills=False,
        preset="xsec_nasdaq100_momentum_v1_deploy",
        experiment_tracker_dir=None,
        _cli_argv=["--preset", "xsec_nasdaq100_momentum_v1_deploy"],
    )

    cmd_paper_run(args)

    stdout = capsys.readouterr().out
    assert "Preset: xsec_nasdaq100_momentum_v1_deploy" in stdout
    assert "portfolio_construction_mode: transition" in stdout
    assert "selected_names: AAPL,MSFT" in stdout
    assert "realized_holdings_count: 2" in stdout
    assert "turnover_cap_binding_count: 3" in stdout


def test_cmd_paper_run_loads_execution_config(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    execution_config = object()
    monkeypatch.setattr("trading_platform.cli.commands.paper_run.load_execution_config", lambda path: execution_config)

    def fake_run_paper_trading_cycle(*, config, state_store, execution_config, auto_apply_fills):
        captured["execution_config"] = execution_config
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=10_000.0),
            latest_prices={"AAPL": 103.0},
            latest_scores={"AAPL": 1.0},
            latest_target_weights={"AAPL": 1.0},
            scheduled_target_weights={"AAPL": 1.0},
            orders=[],
            fills=[],
            skipped_symbols=[],
            diagnostics={},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run.run_paper_trading_cycle",
        fake_run_paper_trading_cycle,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run.write_paper_trading_artifacts",
        lambda *, result, output_dir: {"summary_path": Path(output_dir) / "paper_summary.json"},
    )

    args = SimpleNamespace(
        symbols=["AAPL"],
        universe=None,
        signal_source="legacy",
        strategy="sma_cross",
        fast=20,
        slow=100,
        lookback=20,
        top_n=1,
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
        execution_config=str(tmp_path / "execution.json"),
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
        preset=None,
        lookback_bars=126,
        skip_bars=0,
        rebalance_bars=21,
        portfolio_construction_mode="pure_topn",
        max_position_weight=None,
        max_names_per_sector=None,
        turnover_buffer_bps=0.0,
        max_turnover_per_rebalance=None,
        vol_lookback_bars=20,
        experiment_tracker_dir=None,
    )

    cmd_paper_run(args)

    assert captured["execution_config"] is execution_config
