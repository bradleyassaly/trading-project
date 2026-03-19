from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.paper.models import (
    PaperPortfolioState,
    PaperPosition,
    PaperTradingConfig,
    PaperOrder,
    PaperSignalSnapshot,
    OrderGenerationResult,
)
from trading_platform.paper.service import (
    JsonPaperStateStore,
    apply_filled_orders,
    generate_rebalance_orders,
    run_paper_trading_cycle,
)
from trading_platform.paper.models import (
    PaperPortfolioState,
    PaperTradingRunResult,
)

class DummyStateStore:
    def __init__(self) -> None:
        self.loaded = False

    def load(self) -> PaperPortfolioState:  # pragma: no cover - not used directly
        self.loaded = True
        return PaperPortfolioState(cash=100_000.0)



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
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
        auto_apply_fills=False,
    )

    cmd_paper_run(args)

    assert captured["config"].symbols == ["AAPL", "MSFT"]
    assert captured["auto_apply_fills"] is False
    assert captured["output_dir"] == tmp_path / "paper" / "run_2025-01-04"

    stdout = capsys.readouterr().out
    assert "Paper trading summary:" in stdout
    assert "as_of: 2025-01-04" in stdout
