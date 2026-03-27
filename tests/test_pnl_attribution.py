from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.dashboard.service import DashboardDataService
from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.persistence import persist_paper_run_outputs
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle_for_targets,
    write_paper_trading_artifacts,
)
from trading_platform.reporting.pnl_attribution import aggregate_replay_attribution
from trading_platform.reporting.pnl_attribution import write_pnl_attribution_artifacts


def _run_cycle(
    *,
    state_path: Path,
    as_of: str,
    latest_price: float,
    target_weight: float,
    provenance: dict[str, dict],
) -> object:
    config = PaperTradingConfig(
        symbols=["AAPL"],
        preset_name="multi_strategy",
        universe_name="test",
        strategy="multi_strategy",
        signal_source="multi_strategy",
        initial_cash=1_000.0,
        min_trade_dollars=1.0,
    )
    return run_paper_trading_cycle_for_targets(
        config=config,
        state_store=JsonPaperStateStore(state_path),
        as_of=as_of,
        latest_prices={"AAPL": latest_price},
        latest_scores={},
        latest_scheduled_weights={"AAPL": target_weight},
        latest_effective_weights={"AAPL": target_weight},
        target_diagnostics={"rebalance_timestamp": as_of},
        skipped_symbols=[],
        decision_bundle=DecisionJournalBundle(provenance_by_symbol=provenance),
        auto_apply_fills=True,
    )


def test_single_strategy_realized_attribution_reconciles(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    provenance = {
        "AAPL": {
            "strategy_id": "alpha",
            "strategy_ownership": {"alpha": 1.0},
            "signal_source": "legacy",
        }
    }
    _run_cycle(
        state_path=state_path,
        as_of="2025-01-02",
        latest_price=100.0,
        target_weight=1.0,
        provenance=provenance,
    )
    result = _run_cycle(
        state_path=state_path,
        as_of="2025-01-03",
        latest_price=110.0,
        target_weight=0.0,
        provenance=provenance,
    )

    strategy_rows = result.attribution["strategy_rows"]
    trade_rows = result.attribution["trade_rows"]
    summary = result.attribution["summary"]

    assert len(strategy_rows) == 1
    assert strategy_rows[0]["strategy_id"] == "alpha"
    assert strategy_rows[0]["realized_pnl"] == 100.0
    assert strategy_rows[0]["unrealized_pnl"] == 0.0
    assert len(trade_rows) == 1
    assert trade_rows[0]["realized_pnl"] == 100.0
    assert summary["reconciliation"]["strategy_reconciled"] is True
    assert summary["reconciliation"]["symbol_reconciled"] is True


def test_two_strategy_blended_attribution_splits_realized_and_unrealized(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    provenance = {
        "AAPL": {
            "strategy_id": "multi_strategy",
            "strategy_ownership": {"alpha": 0.6, "beta": 0.4},
            "strategy_rows": [
                {"strategy_id": "alpha", "signal_source": "multi_strategy", "signal_family": "momentum"},
                {"strategy_id": "beta", "signal_source": "multi_strategy", "signal_family": "value"},
            ],
            "signal_source": "multi_strategy",
            "signal_families": ["momentum", "value"],
        }
    }
    _run_cycle(
        state_path=state_path,
        as_of="2025-01-02",
        latest_price=100.0,
        target_weight=1.0,
        provenance=provenance,
    )
    no_trade_result = _run_cycle(
        state_path=state_path,
        as_of="2025-01-03",
        latest_price=110.0,
        target_weight=1.0,
        provenance=provenance,
    )
    no_trade_rows = {row["strategy_id"]: row for row in no_trade_result.attribution["strategy_rows"]}
    assert no_trade_rows["alpha"]["unrealized_pnl"] == 60.0
    assert no_trade_rows["beta"]["unrealized_pnl"] == 40.0

    close_result = _run_cycle(
        state_path=state_path,
        as_of="2025-01-04",
        latest_price=120.0,
        target_weight=0.0,
        provenance=provenance,
    )
    close_rows = {row["strategy_id"]: row for row in close_result.attribution["strategy_rows"]}
    trade_rows = sorted(close_result.attribution["trade_rows"], key=lambda row: row["strategy_id"])
    assert close_rows["alpha"]["realized_pnl"] == 120.0
    assert close_rows["beta"]["realized_pnl"] == 80.0
    assert [row["strategy_id"] for row in trade_rows] == ["alpha", "beta"]
    assert [row["realized_pnl"] for row in trade_rows] == [120.0, 80.0]


def test_partial_close_preserves_remaining_unrealized_ownership(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    provenance = {
        "AAPL": {
            "strategy_id": "multi_strategy",
            "strategy_ownership": {"alpha": 0.6, "beta": 0.4},
            "strategy_rows": [
                {"strategy_id": "alpha", "signal_source": "multi_strategy", "signal_family": "momentum"},
                {"strategy_id": "beta", "signal_source": "multi_strategy", "signal_family": "value"},
            ],
            "signal_source": "multi_strategy",
            "signal_families": ["momentum", "value"],
        }
    }
    _run_cycle(
        state_path=state_path,
        as_of="2025-01-02",
        latest_price=100.0,
        target_weight=1.0,
        provenance=provenance,
    )
    result = _run_cycle(
        state_path=state_path,
        as_of="2025-01-03",
        latest_price=110.0,
        target_weight=0.5,
        provenance=provenance,
    )

    strategy_rows = {row["strategy_id"]: row for row in result.attribution["strategy_rows"]}
    trade_rows = result.attribution["trade_rows"]
    summary = result.attribution["summary"]

    assert trade_rows == [
        {
            "trade_id": "paper-trade-1",
            "date": "2025-01-03",
            "symbol": "AAPL",
            "strategy_id": "alpha",
            "signal_source": "multi_strategy",
            "signal_family": "momentum",
            "side": "long",
            "quantity": 5,
            "entry_price": 100.0,
            "exit_price": 110.0,
            "realized_pnl": 50.0,
            "holding_period_days": 1,
            "attribution_method": "target_weight_proportional",
            "status": "closed",
            "entry_date": "2025-01-02",
            "exit_date": "2025-01-03",
        }
    ]
    assert strategy_rows["alpha"]["realized_pnl"] == 50.0
    assert strategy_rows["beta"]["realized_pnl"] == 0.0
    assert strategy_rows["alpha"]["unrealized_pnl"] == 10.0
    assert strategy_rows["beta"]["unrealized_pnl"] == 40.0
    assert summary["total_realized_pnl"] == 50.0
    assert summary["total_unrealized_pnl"] == 50.0
    assert summary["reconciliation"]["strategy_residual"] == 0.0
    assert summary["reconciliation"]["symbol_residual"] == 0.0
    assert summary["reconciliation"]["strategy_reconciled"] is True
    assert summary["reconciliation"]["symbol_reconciled"] is True


def test_attribution_artifacts_and_dashboard_payloads(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    provenance = {
        "AAPL": {
            "strategy_id": "alpha",
            "strategy_ownership": {"alpha": 1.0},
            "signal_source": "legacy",
        }
    }
    result = _run_cycle(
        state_path=state_path,
        as_of="2025-01-02",
        latest_price=100.0,
        target_weight=1.0,
        provenance=provenance,
    )
    paper_dir = tmp_path / "paper"
    paths = write_paper_trading_artifacts(result=result, output_dir=paper_dir)
    persist_paper_run_outputs(
        result=result,
        config=PaperTradingConfig(
            symbols=["AAPL"], preset_name="multi_strategy", universe_name="test", strategy="multi_strategy"
        ),
        output_dir=paper_dir,
        state_file_preexisting=False,
    )

    assert paths["paper_trades_path"].exists()
    assert (paper_dir / "strategy_pnl_attribution.csv").exists()
    assert (paper_dir / "symbol_pnl_attribution.csv").exists()
    assert (paper_dir / "trade_pnl_attribution.csv").exists()
    assert (paper_dir / "pnl_attribution_summary.json").exists()

    service = DashboardDataService(tmp_path)
    strategy_payload = service.strategy_pnl_latest_payload()
    symbol_payload = service.symbol_pnl_latest_payload()
    attribution_payload = service.attribution_latest_payload()

    assert strategy_payload["rows"][0]["strategy_id"] == "alpha"
    assert symbol_payload["rows"][0]["symbol"] == "AAPL"
    assert attribution_payload["summary"]["attribution_method"] == "target_weight_proportional"


def test_replay_attribution_aggregation(tmp_path: Path) -> None:
    day_one = tmp_path / "2025-01-02" / "paper"
    day_two = tmp_path / "2025-01-03" / "paper"
    day_one.mkdir(parents=True, exist_ok=True)
    day_two.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": "2025-01-02",
                "strategy_id": "alpha",
                "realized_pnl": 0.0,
                "unrealized_pnl": 50.0,
                "total_pnl": 50.0,
                "turnover": 1_000.0,
                "trade_count": 1,
                "closed_trade_count": 0,
                "winning_trade_count": 0,
                "strategy_weight": 1.0,
                "gross_exposure": 1_050.0,
                "net_exposure": 1_050.0,
                "position_count": 1,
            }
        ]
    ).to_csv(day_one / "strategy_pnl_attribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "strategy_id": "alpha",
                "realized_pnl": 100.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 100.0,
                "turnover": 1_100.0,
                "trade_count": 1,
                "closed_trade_count": 1,
                "winning_trade_count": 1,
                "strategy_weight": 0.0,
                "gross_exposure": 0.0,
                "net_exposure": 0.0,
                "position_count": 0,
            }
        ]
    ).to_csv(day_two / "strategy_pnl_attribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "realized_pnl": 100.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 100.0,
                "traded_notional": 2_100.0,
                "fill_count": 2,
            }
        ]
    ).to_csv(day_two / "symbol_pnl_attribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_id": "paper-trade-1",
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "realized_pnl": 100.0,
                "entry_price": 100.0,
                "exit_price": 110.0,
            }
        ]
    ).to_csv(day_two / "trade_pnl_attribution.csv", index=False)

    replay = aggregate_replay_attribution(replay_root=tmp_path)
    assert replay["strategy_rows"][0]["strategy_id"] == "alpha"
    assert replay["strategy_rows"][0]["realized_pnl"] == 100.0
    assert replay["strategy_rows"][0]["unrealized_pnl"] == 0.0
    assert replay["summary"]["top_strategies_by_total_pnl"][0]["strategy_id"] == "alpha"


def test_replay_attribution_reconciles_mixed_realized_and_unrealized(tmp_path: Path) -> None:
    day_one = tmp_path / "2025-01-02" / "paper"
    day_two = tmp_path / "2025-01-03" / "paper"
    day_one.mkdir(parents=True, exist_ok=True)
    day_two.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": "2025-01-02",
                "strategy_id": "alpha",
                "realized_pnl": 0.0,
                "unrealized_pnl": 60.0,
                "total_pnl": 60.0,
                "turnover": 1_000.0,
                "trade_count": 1,
                "closed_trade_count": 0,
                "winning_trade_count": 0,
                "strategy_weight": 0.6,
                "gross_exposure": 660.0,
                "net_exposure": 660.0,
                "position_count": 1,
            },
            {
                "date": "2025-01-02",
                "strategy_id": "beta",
                "realized_pnl": 0.0,
                "unrealized_pnl": 40.0,
                "total_pnl": 40.0,
                "turnover": 1_000.0,
                "trade_count": 1,
                "closed_trade_count": 0,
                "winning_trade_count": 0,
                "strategy_weight": 0.4,
                "gross_exposure": 440.0,
                "net_exposure": 440.0,
                "position_count": 1,
            },
        ]
    ).to_csv(day_one / "strategy_pnl_attribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-02",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "realized_pnl": 0.0,
                "unrealized_pnl": 60.0,
                "total_pnl": 60.0,
                "traded_notional": 0.0,
                "fill_count": 0,
                "end_position": 6,
            },
            {
                "date": "2025-01-02",
                "symbol": "AAPL",
                "strategy_id": "beta",
                "realized_pnl": 0.0,
                "unrealized_pnl": 40.0,
                "total_pnl": 40.0,
                "traded_notional": 0.0,
                "fill_count": 0,
                "end_position": 4,
            },
        ]
    ).to_csv(day_one / "symbol_pnl_attribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "strategy_id": "alpha",
                "realized_pnl": 30.0,
                "unrealized_pnl": 30.0,
                "total_pnl": 60.0,
                "turnover": 550.0,
                "trade_count": 1,
                "closed_trade_count": 1,
                "winning_trade_count": 1,
                "strategy_weight": 0.6,
                "gross_exposure": 330.0,
                "net_exposure": 330.0,
                "position_count": 1,
            },
            {
                "date": "2025-01-03",
                "strategy_id": "beta",
                "realized_pnl": 20.0,
                "unrealized_pnl": 20.0,
                "total_pnl": 40.0,
                "turnover": 550.0,
                "trade_count": 1,
                "closed_trade_count": 1,
                "winning_trade_count": 1,
                "strategy_weight": 0.4,
                "gross_exposure": 220.0,
                "net_exposure": 220.0,
                "position_count": 1,
            },
        ]
    ).to_csv(day_two / "strategy_pnl_attribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "realized_pnl": 30.0,
                "unrealized_pnl": 30.0,
                "total_pnl": 60.0,
                "traded_notional": 330.0,
                "fill_count": 1,
                "end_position": 3,
            },
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "beta",
                "realized_pnl": 20.0,
                "unrealized_pnl": 20.0,
                "total_pnl": 40.0,
                "traded_notional": 220.0,
                "fill_count": 1,
                "end_position": 2,
            },
        ]
    ).to_csv(day_two / "symbol_pnl_attribution.csv", index=False)

    replay = aggregate_replay_attribution(replay_root=tmp_path)
    reconciliation = replay["summary"]["reconciliation"]

    assert reconciliation["portfolio_realized_pnl"] == 50.0
    assert reconciliation["portfolio_unrealized_pnl"] == 50.0
    assert reconciliation["strategy_total_pnl"] == 100.0
    assert reconciliation["symbol_total_pnl"] == 100.0
    assert reconciliation["strategy_residual"] == 0.0
    assert reconciliation["symbol_residual"] == 0.0
    assert reconciliation["strategy_reconciled"] is True
    assert reconciliation["symbol_reconciled"] is True


def test_paper_summary_includes_attribution_summary(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    provenance = {
        "AAPL": {
            "strategy_id": "alpha",
            "strategy_ownership": {"alpha": 1.0},
            "signal_source": "legacy",
        }
    }
    result = _run_cycle(
        state_path=state_path,
        as_of="2025-01-02",
        latest_price=100.0,
        target_weight=1.0,
        provenance=provenance,
    )
    paper_dir = tmp_path / "paper"
    write_paper_trading_artifacts(result=result, output_dir=paper_dir)
    persist_paper_run_outputs(
        result=result,
        config=PaperTradingConfig(
            symbols=["AAPL"], preset_name="multi_strategy", universe_name="test", strategy="multi_strategy"
        ),
        output_dir=paper_dir,
        state_file_preexisting=False,
    )
    payload = json.loads((paper_dir / "paper_run_summary_latest.json").read_text(encoding="utf-8"))
    assert payload["pnl_attribution_summary"]["attribution_method"] == "target_weight_proportional"


def test_writer_emits_header_only_csvs_for_empty_attribution(tmp_path: Path) -> None:
    paths = write_pnl_attribution_artifacts(
        output_dir=tmp_path,
        attribution_payload={"strategy_rows": [], "symbol_rows": [], "trade_rows": [], "summary": {}},
    )

    strategy_text = paths["strategy_pnl_attribution_path"].read_text(encoding="utf-8").strip()
    symbol_text = paths["symbol_pnl_attribution_path"].read_text(encoding="utf-8").strip()
    trade_text = paths["trade_pnl_attribution_path"].read_text(encoding="utf-8").strip()

    assert "strategy_id" in strategy_text
    assert "symbol" in symbol_text
    assert "trade_id" in trade_text


def test_aggregate_replay_attribution_tolerates_zero_byte_trade_csv(tmp_path: Path) -> None:
    day_dir = tmp_path / "2025-01-02" / "paper"
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "trade_pnl_attribution.csv").write_text("", encoding="utf-8")

    replay = aggregate_replay_attribution(replay_root=tmp_path)

    assert replay["trade_rows"] == []
    assert replay["summary"] == {}


def test_aggregate_replay_attribution_tolerates_header_only_empty_csv(tmp_path: Path) -> None:
    day_dir = tmp_path / "2025-01-02" / "paper"
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "trade_pnl_attribution.csv").write_text("trade_id,date\n", encoding="utf-8")

    replay = aggregate_replay_attribution(replay_root=tmp_path)

    assert replay["trade_rows"] == []
    assert replay["summary"] == {}


def test_aggregate_replay_attribution_tolerates_missing_per_day_files(tmp_path: Path) -> None:
    (tmp_path / "2025-01-02" / "paper").mkdir(parents=True, exist_ok=True)

    replay = aggregate_replay_attribution(replay_root=tmp_path)

    assert replay["strategy_rows"] == []
    assert replay["symbol_rows"] == []
    assert replay["trade_rows"] == []
