from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.paper_report import cmd_paper_report
from trading_platform.reporting.paper_account_report import PaperAccountReport


def test_cmd_paper_report_prints_summary_and_writes_files(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_build_paper_account_report(account_dir):
        captured["account_dir"] = Path(account_dir)
        return PaperAccountReport(
            as_of="2025-01-05",
            latest_equity=10400.0,
            cumulative_return=0.04,
            max_drawdown=-0.01,
            avg_daily_return=0.02,
            daily_volatility=0.015,
            sharpe_ratio=2.0,
            total_fills=2,
            buy_fill_count=1,
            sell_fill_count=1,
            gross_traded_notional=2020.0,
            total_commissions=1.0,
            open_position_count=1,
            gross_market_value=1200.0,
            top_position_symbol="NVDA",
            top_position_weight=1200.0 / 10400.0,
            metrics={},
        )

    def fake_write_paper_account_report(*, report, output_dir):
        captured["output_dir"] = Path(output_dir)
        return {
            "json_path": Path(output_dir) / "paper_account_report.json",
            "csv_path": Path(output_dir) / "paper_account_summary.csv",
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_report.build_paper_account_report",
        fake_build_paper_account_report,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_report.write_paper_account_report",
        fake_write_paper_account_report,
    )

    args = SimpleNamespace(
        account_dir=str(tmp_path / "paper"),
        output_dir=str(tmp_path / "reports"),
    )

    cmd_paper_report(args)

    stdout = capsys.readouterr().out
    assert "Paper account report" in stdout
    assert "As of: 2025-01-05" in stdout
    assert "Latest equity: 10,400.00" in stdout
    assert "Cumulative return: 4.00%" in stdout
    assert "Max drawdown: -1.00%" in stdout
    assert "Total fills: 2" in stdout
    assert "Open positions: 1" in stdout
    assert "Top position: NVDA" in stdout
    assert "Report files:" in stdout