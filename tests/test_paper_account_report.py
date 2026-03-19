from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.reporting.paper_account_report import (
    build_paper_account_report,
    write_paper_account_report,
)


def test_build_paper_account_report_from_ledgers(tmp_path: Path) -> None:
    ledger_dir = tmp_path / "ledgers"
    ledger_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"as_of": "2025-01-03", "cash": 10000.0, "gross_market_value": 0.0, "equity": 10000.0, "position_count": 0},
            {"as_of": "2025-01-04", "cash": 9000.0, "gross_market_value": 1100.0, "equity": 10100.0, "position_count": 1},
            {"as_of": "2025-01-05", "cash": 9200.0, "gross_market_value": 1200.0, "equity": 10400.0, "position_count": 1},
        ]
    ).to_csv(ledger_dir / "equity_curve.csv", index=False)

    pd.DataFrame(
        [
            {"as_of": "2025-01-04", "symbol": "AAPL", "side": "BUY", "quantity": 10, "fill_price": 101.0, "notional": 1010.0, "commission": 1.0, "slippage_bps": 5.0},
            {"as_of": "2025-01-05", "symbol": "MSFT", "side": "SELL", "quantity": 5, "fill_price": 202.0, "notional": 1010.0, "commission": 0.0, "slippage_bps": 0.0},
        ]
    ).to_csv(ledger_dir / "fills.csv", index=False)

    pd.DataFrame(
        [
            {"as_of": "2025-01-05", "symbol": "NVDA", "quantity": 4, "avg_price": 250.0, "last_price": 300.0, "market_value": 1200.0},
        ]
    ).to_csv(ledger_dir / "positions_history.csv", index=False)

    pd.DataFrame(
        [
            {"as_of": "2025-01-05", "symbol": "NVDA", "side": "BUY", "quantity": 4, "reference_price": 300.0, "target_weight": 0.12, "current_quantity": 0, "target_quantity": 4, "notional": 1200.0, "reason": "rebalance_to_target"},
        ]
    ).to_csv(ledger_dir / "orders_history.csv", index=False)

    report = build_paper_account_report(tmp_path)

    assert report.as_of == "2025-01-05"
    assert report.latest_equity == 10400.0
    assert round(report.cumulative_return, 4) == 0.04
    assert report.total_fills == 2
    assert report.buy_fill_count == 1
    assert report.sell_fill_count == 1
    assert report.open_position_count == 1
    assert report.top_position_symbol == "NVDA"
    assert round(report.top_position_weight, 6) == round(1200.0 / 10400.0, 6)


def test_build_paper_account_report_handles_empty_ledgers(tmp_path: Path) -> None:
    (tmp_path / "ledgers").mkdir(parents=True, exist_ok=True)

    report = build_paper_account_report(tmp_path)

    assert report.as_of is None
    assert report.latest_equity == 0.0
    assert report.total_fills == 0
    assert report.open_position_count == 0


def test_write_paper_account_report_writes_files(tmp_path: Path) -> None:
    ledger_dir = tmp_path / "ledgers"
    ledger_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"as_of": "2025-01-04", "cash": 10000.0, "gross_market_value": 0.0, "equity": 10000.0, "position_count": 0},
        ]
    ).to_csv(ledger_dir / "equity_curve.csv", index=False)

    report = build_paper_account_report(tmp_path)
    paths = write_paper_account_report(report=report, output_dir=tmp_path / "reports")

    assert paths["json_path"].exists()
    assert paths["csv_path"].exists()