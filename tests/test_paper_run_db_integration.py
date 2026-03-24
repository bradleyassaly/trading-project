from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from alembic import command
from alembic.config import Config

from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.decision_journal.models import CandidateEvaluation, DecisionJournalBundle, TradeDecisionRecord
from trading_platform.paper.models import PaperOrder, PaperPortfolioState, PaperTradingRunResult


def test_cmd_paper_run_writes_optional_db_metadata(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "paper.db"
    monkeypatch.setenv("TRADING_PLATFORM_ENABLE_DATABASE_METADATA", "1")
    monkeypatch.setenv("TRADING_PLATFORM_DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    command.upgrade(Config(str(Path(__file__).resolve().parents[1] / "alembic.ini")), "head")

    def fake_run_paper_trading_cycle(*, config, state_store, execution_config, auto_apply_fills):
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=10_000.0),
            latest_prices={"AAPL": 103.0},
            latest_scores={"AAPL": 1.0},
            latest_target_weights={"AAPL": 1.0},
            scheduled_target_weights={"AAPL": 1.0},
            orders=[PaperOrder(symbol="AAPL", side="BUY", quantity=10, reference_price=103.0, target_weight=1.0, current_quantity=0, target_quantity=10, notional=1030.0, reason="rebalance")],
            fills=[],
            skipped_symbols=[],
            diagnostics={},
            decision_bundle=DecisionJournalBundle(
                candidate_evaluations=[
                    CandidateEvaluation(
                        decision_id="cand-1",
                        timestamp="2025-01-04T00:00:00Z",
                        run_id="paper-demo",
                        cycle_id="2025-01-04",
                        symbol="AAPL",
                        side="BUY",
                        strategy_id="sma_cross",
                        universe_id="demo",
                        candidate_status="selected",
                        final_signal_score=1.0,
                        rank=1,
                    )
                ],
                trade_decisions=[
                    TradeDecisionRecord(
                        decision_id="trade-1",
                        timestamp="2025-01-04T00:00:00Z",
                        run_id="paper-demo",
                        cycle_id="2025-01-04",
                        symbol="AAPL",
                        side="BUY",
                        strategy_id="sma_cross",
                        universe_id="demo",
                        candidate_status="selected",
                        final_signal_score=1.0,
                        target_weight_post_constraint=1.0,
                        target_quantity=10,
                    )
                ],
            ),
        )

    def fake_write_paper_trading_artifacts(*, result, output_dir):
        summary_path = Path(output_dir) / "paper_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        return {"summary_path": summary_path}

    def fake_persist_paper_run_outputs(*, result, config, output_dir, state_file_preexisting):
        summary_csv = Path(output_dir) / "paper_run_summary.csv"
        summary_csv.write_text("run_key\npaper\n", encoding="utf-8")
        return {"paper_run_summary_csv": summary_csv}, [], {}

    monkeypatch.setattr("trading_platform.cli.commands.paper_run.run_paper_trading_cycle", fake_run_paper_trading_cycle)
    monkeypatch.setattr("trading_platform.cli.commands.paper_run.write_paper_trading_artifacts", fake_write_paper_trading_artifacts)
    monkeypatch.setattr("trading_platform.cli.commands.paper_run.persist_paper_run_outputs", fake_persist_paper_run_outputs)
    monkeypatch.setattr("trading_platform.cli.commands.paper_run.build_paper_experiment_record", lambda output_dir: {"output_dir": str(output_dir)})
    monkeypatch.setattr("trading_platform.cli.commands.paper_run.register_experiment", lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.json"})

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
        execution_config=None,
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
        use_alpaca_latest_data=False,
        latest_data_max_age_seconds=86400,
        slippage_model="none",
        slippage_buy_bps=0.0,
        slippage_sell_bps=0.0,
    )

    cmd_paper_run(args)

    from sqlalchemy import create_engine, text

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as conn:
        assert conn.execute(text("select count(*) from portfolio_runs")).scalar() == 1
        assert conn.execute(text("select count(*) from portfolio_decisions")).scalar() == 1
        assert conn.execute(text("select count(*) from candidate_evaluations")).scalar() == 1
        assert conn.execute(text("select count(*) from artifacts")).scalar() >= 1
