from __future__ import annotations

from pathlib import Path

from trading_platform.jobs.daily_paper_trading import run_daily_paper_trading_job
from trading_platform.paper.models import (
    PaperPortfolioState,
    PaperTradingConfig,
    PaperTradingRunResult,
)


def test_run_daily_paper_trading_job_calls_refresh_build_and_writes_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: dict[str, object] = {}

    def fake_refresh_data(symbols: list[str]) -> None:
        calls["refresh_data"] = list(symbols)

    def fake_build_features(symbols: list[str]) -> None:
        calls["build_features"] = list(symbols)

    def fake_run_paper_trading_cycle(*, config, state_store, auto_apply_fills):
        calls["config"] = config
        calls["state_store"] = state_store
        calls["auto_apply_fills"] = auto_apply_fills
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=9_000.0),
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
        calls["artifact_result"] = result
        calls["output_dir"] = Path(output_dir)
        return {
            "summary_path": Path(output_dir) / "paper_summary.json",
            "orders_path": Path(output_dir) / "paper_orders.csv",
        }

    def fake_append_fill_ledger(*, path, as_of, fills):
        calls["fills_ledger_path"] = Path(path)
        return Path(path)

    def fake_append_equity_ledger(*, path, as_of, state):
        calls["equity_ledger_path"] = Path(path)
        return Path(path)

    def fake_append_positions_history(*, path, as_of, state):
        calls["positions_history_path"] = Path(path)
        return Path(path)

    def fake_append_orders_history(*, path, as_of, orders):
        calls["orders_history_path"] = Path(path)
        return Path(path)

    monkeypatch.setattr(
        "trading_platform.jobs.daily_paper_trading.run_paper_trading_cycle",
        fake_run_paper_trading_cycle,
    )
    monkeypatch.setattr(
        "trading_platform.jobs.daily_paper_trading.write_paper_trading_artifacts",
        fake_write_paper_trading_artifacts,
    )
    monkeypatch.setattr(
        "trading_platform.jobs.daily_paper_trading.append_fill_ledger",
        fake_append_fill_ledger,
    )
    monkeypatch.setattr(
        "trading_platform.jobs.daily_paper_trading.append_equity_ledger",
        fake_append_equity_ledger,
    )
    monkeypatch.setattr(
        "trading_platform.jobs.daily_paper_trading.append_positions_history",
        fake_append_positions_history,
    )
    monkeypatch.setattr(
        "trading_platform.jobs.daily_paper_trading.append_orders_history",
        fake_append_orders_history,
    )

    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT"],
        strategy="sma_cross",
        top_n=2,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_daily_paper_trading_job(
        config=config,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "artifacts",
        auto_apply_fills=True,
        refresh_data_fn=fake_refresh_data,
        build_features_fn=fake_build_features,
    )

    assert calls["refresh_data"] == ["AAPL", "MSFT"]
    assert calls["build_features"] == ["AAPL", "MSFT"]
    assert calls["auto_apply_fills"] is True
    assert calls["output_dir"] == tmp_path / "artifacts" / "runs" / "2025-01-04"
    assert calls["fills_ledger_path"] == tmp_path / "artifacts" / "ledgers" / "fills.csv"
    assert result.as_of == "2025-01-04"
    assert result.order_count == 0
    assert result.fill_count == 0
    assert result.cash == 9_000.0
    assert result.equity == 9_000.0