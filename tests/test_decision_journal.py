from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.decision_journal.service import (
    build_candidate_journal_for_snapshot,
    summarize_entry_reason,
    summarize_exit_reason,
    summarize_selection_context,
    summarize_sizing_context,
    write_decision_journal_artifacts,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import JsonPaperStateStore, run_paper_trading_cycle_for_targets


def test_build_candidate_journal_for_snapshot_serializes_rank_and_rejections() -> None:
    bundle = build_candidate_journal_for_snapshot(
        timestamp="2025-01-04",
        run_id="manual|xsec|demo|2025-01-04",
        cycle_id="2025-01-04",
        strategy_id="xsec_momentum_topn",
        universe_id="demo",
        score_map={"AAPL": 2.0, "MSFT": 1.0},
        latest_prices={"AAPL": 100.0, "MSFT": 200.0},
        selected_weights={"AAPL": 1.0},
        scheduled_weights={"AAPL": 1.0, "MSFT": 0.0},
        skipped_symbols=["NVDA"],
        skip_reasons={"NVDA": "missing_feature_frame"},
        asset_return_map={"AAPL": 0.04, "MSFT": 0.01},
        selected_rejection_reasons={"MSFT": "outranked_by_other_candidate"},
    )

    assert len(bundle.candidate_evaluations) == 3
    rows = {row.symbol: row for row in bundle.candidate_evaluations}
    assert rows["AAPL"].candidate_status == "selected"
    assert rows["AAPL"].rank == 1
    assert rows["MSFT"].rejection_reason == "outranked_by_other_candidate"
    assert rows["NVDA"].rejection_reason == "missing_feature_frame"
    assert rows["AAPL"].flat_dict()["feature_snapshot"] == "asset_return=0.04|latest_price=100.0"


def test_write_decision_journal_artifacts_writes_flattened_outputs(tmp_path: Path) -> None:
    bundle = build_candidate_journal_for_snapshot(
        timestamp="2025-01-04",
        run_id="manual|sma_cross|demo|2025-01-04",
        cycle_id="2025-01-04",
        strategy_id="sma_cross",
        universe_id="demo",
        score_map={"AAPL": 1.5},
        latest_prices={"AAPL": 101.0},
        selected_weights={"AAPL": 1.0},
        scheduled_weights={"AAPL": 1.0},
    )

    paths = write_decision_journal_artifacts(bundle=bundle, output_dir=tmp_path)

    assert paths["candidate_snapshot_json"].exists()
    assert paths["candidate_snapshot_csv"].exists()
    candidate_df = pd.read_csv(paths["candidate_snapshot_csv"])
    assert candidate_df.iloc[0]["symbol"] == "AAPL"
    assert candidate_df.iloc[0]["candidate_status"] == "selected"


def test_run_paper_trading_cycle_for_targets_enriches_and_persists_decision_bundle(tmp_path: Path) -> None:
    config = PaperTradingConfig(symbols=["AAPL"], strategy="sma_cross", initial_cash=10_000.0, top_n=1)
    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    base_bundle = build_candidate_journal_for_snapshot(
        timestamp="2025-01-04",
        run_id="manual|sma_cross|symbols|2025-01-04",
        cycle_id="2025-01-04",
        strategy_id="sma_cross",
        universe_id=None,
        score_map={"AAPL": 2.0},
        latest_prices={"AAPL": 100.0},
        selected_weights={"AAPL": 1.0},
        scheduled_weights={"AAPL": 1.0},
    )

    result = run_paper_trading_cycle_for_targets(
        config=config,
        state_store=state_store,
        as_of="2025-01-04",
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 2.0},
        latest_scheduled_weights={"AAPL": 1.0},
        latest_effective_weights={"AAPL": 1.0},
        target_diagnostics={},
        skipped_symbols=[],
        decision_bundle=base_bundle,
        auto_apply_fills=False,
    )

    assert result.decision_bundle is not None
    assert any(row.symbol == "AAPL" and row.candidate_status == "selected" for row in result.decision_bundle.trade_decisions)

    paths = write_decision_journal_artifacts(bundle=result.decision_bundle, output_dir=tmp_path / "artifacts")
    trade_df = pd.read_csv(paths["trade_decisions_csv"])
    assert trade_df.iloc[0]["symbol"] == "AAPL"
    assert "selected" in summarize_entry_reason(result.decision_bundle.trade_decisions[0])
    assert "selected" in summarize_selection_context(result.decision_bundle.selection_decisions[0])
    assert isinstance(summarize_sizing_context(result.decision_bundle.sizing_decisions[0]), str)


def test_decision_journal_summary_helpers_handle_missing_fields() -> None:
    bundle = DecisionJournalBundle()
    assert summarize_entry_reason({"candidate_status": "rejected", "rejection_reason": None}) == "no explicit entry rationale"
    assert summarize_exit_reason({"exit_trigger_type": "rebalance", "exit_reason_summary": None}) == "rebalance"
    assert summarize_selection_context({"selection_status": "rejected", "candidate_count": None}) == "rejected"
    assert summarize_sizing_context({"target_quantity": None}) == "no sizing context"
