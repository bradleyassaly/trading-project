from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.daily_paper_job import cmd_daily_paper_job
from trading_platform.jobs.daily_paper_trading import DailyPaperTradingJobResult


def test_cmd_daily_paper_job_runs_job_and_prints_summary(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_run_daily_paper_trading_job(*, config, state_path, output_dir, auto_apply_fills):
        captured["config"] = config
        captured["state_path"] = state_path
        captured["output_dir"] = output_dir
        captured["auto_apply_fills"] = auto_apply_fills
        return DailyPaperTradingJobResult(
            as_of="2025-01-04",
            symbols=["AAPL", "MSFT"],
            order_count=2,
            fill_count=1,
            cash=9_000.0,
            equity=10_100.0,
            artifact_paths={
                "summary_path": Path(output_dir) / "runs" / "2025-01-04" / "paper_summary.json",
            },
            ledger_paths={
                "equity_ledger_path": Path(output_dir) / "ledgers" / "equity_curve.csv",
            },
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.daily_paper_job.run_daily_paper_trading_job",
        fake_run_daily_paper_trading_job,
    )

    args = SimpleNamespace(
        symbols=["AAPL", "MSFT"],
        universe=None,
        signal_source="legacy",
        strategy="sma_cross",
        fast=None,
        slow=None,
        lookback=None,
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
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
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
        output_dir=str(tmp_path / "artifacts"),
        auto_apply_fills=True,
    )

    cmd_daily_paper_job(args)

    stdout = capsys.readouterr().out
    assert "Running daily paper trading job for 2 symbol(s): AAPL, MSFT" in stdout
    assert "As of: 2025-01-04" in stdout
    assert "Orders: 2" in stdout
    assert "Fills: 1" in stdout
    assert "Ledgers:" in stdout

    config = captured["config"]
    assert config.symbols == ["AAPL", "MSFT"]
    assert captured["auto_apply_fills"] is True
