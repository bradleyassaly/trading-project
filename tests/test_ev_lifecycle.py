from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.reporting.ev_lifecycle import (
    aggregate_replay_ev_lifecycle,
    build_trade_ev_lifecycle_rows,
    write_replay_ev_lifecycle_artifacts,
)


def test_build_trade_ev_lifecycle_rows_computes_decay_efficiency_and_excursions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-02", periods=3, freq="D"),
                "close": [100.0, 105.0, 103.0],
            }
        )

    monkeypatch.setattr("trading_platform.reporting.ev_lifecycle.load_feature_frame", fake_load_feature_frame)
    rows = build_trade_ev_lifecycle_rows(
        trade_rows=[
            {
                "trade_id": "paper-trade-1",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_source": "legacy",
                "signal_family": "momentum",
                "side": "long",
                "quantity": 10,
                "entry_reference_price": 100.0,
                "exit_reference_price": 103.0,
                "gross_realized_pnl": 30.0,
                "realized_pnl": 20.0,
                "holding_period_days": 2,
                "status": "closed",
                "entry_date": "2025-01-02",
                "exit_date": "2025-01-04",
                "ev_entry": 0.03,
                "score_entry": 0.9,
                "score_percentile_entry": 0.95,
                "ev_exit": -0.01,
                "score_exit": 0.4,
                "score_percentile_exit": 0.30,
                "exit_reason": "exit_below_exit_threshold",
            }
        ]
    )

    assert len(rows) == 1
    assert rows[0]["mfe_pnl"] == pytest.approx(50.0)
    assert rows[0]["mae_pnl"] == pytest.approx(0.0)
    assert rows[0]["exit_efficiency"] == pytest.approx(0.4)
    assert rows[0]["ev_decay"] == pytest.approx(0.04)
    assert rows[0]["ev_alignment"] == 1


def test_aggregate_replay_ev_lifecycle_summarizes_trade_quality(tmp_path: Path) -> None:
    day_one = tmp_path / "2025-01-03" / "paper"
    day_two = tmp_path / "2025-01-06" / "paper"
    day_one.mkdir(parents=True, exist_ok=True)
    day_two.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "trade_id": "paper-trade-1",
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "side": "long",
                "ev_entry": 0.03,
                "ev_exit": -0.01,
                "realized_pnl": 20.0,
                "realized_return": 0.02,
                "mfe_pnl": 50.0,
                "mae_pnl": -10.0,
                "holding_period_days": 2,
                "exit_efficiency": 0.4,
                "ev_decay": 0.04,
                "ev_alignment": 1,
            }
        ]
    ).to_csv(day_one / "trade_ev_lifecycle.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_id": "paper-trade-2",
                "date": "2025-01-06",
                "symbol": "MSFT",
                "strategy_id": "beta",
                "side": "long",
                "ev_entry": -0.02,
                "ev_exit": 0.01,
                "realized_pnl": -10.0,
                "realized_return": -0.01,
                "mfe_pnl": 10.0,
                "mae_pnl": -20.0,
                "holding_period_days": 1,
                "exit_efficiency": -1.0,
                "ev_decay": -0.03,
                "ev_alignment": 1,
            }
        ]
    ).to_csv(day_two / "trade_ev_lifecycle.csv", index=False)

    rows, summary = aggregate_replay_ev_lifecycle(replay_root=tmp_path)

    assert len(rows) == 2
    assert summary["trade_count"] == 2
    assert summary["avg_EV_entry"] == pytest.approx(0.005)
    assert summary["avg_EV_exit"] == pytest.approx(0.0)
    assert summary["EV_alignment_rate"] == pytest.approx(1.0)
    assert summary["pct_trades_EV_entry_positive"] == pytest.approx(0.5)
    assert summary["pct_exits_EV_exit_negative"] == pytest.approx(0.5)
    assert "bucket_rows" in summary


def test_write_replay_ev_lifecycle_artifacts_writes_expected_files(tmp_path: Path) -> None:
    artifact_paths = write_replay_ev_lifecycle_artifacts(
        replay_root=tmp_path,
        lifecycle_rows=[
            {
                "trade_id": "paper-trade-1",
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "ev_entry": 0.01,
            }
        ],
        summary={"trade_count": 1, "avg_EV_entry": 0.01},
    )

    assert Path(artifact_paths["replay_trade_ev_lifecycle_csv_path"]).exists()
    payload = json.loads(Path(artifact_paths["replay_ev_lifecycle_summary_json_path"]).read_text(encoding="utf-8"))
    assert payload["trade_count"] == 1
