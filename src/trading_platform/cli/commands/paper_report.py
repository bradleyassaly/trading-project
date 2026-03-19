from __future__ import annotations

from pathlib import Path

from trading_platform.reporting.paper_account_report import (
    build_paper_account_report,
    write_paper_account_report,
)


def cmd_paper_report(args) -> None:
    account_dir = Path(args.account_dir)
    report = build_paper_account_report(account_dir)

    print("Paper account report")
    print(f"As of: {report.as_of}")
    print(f"Latest equity: {report.latest_equity:,.2f}")
    print(f"Cumulative return: {report.cumulative_return * 100:.2f}%")
    print(f"Max drawdown: {report.max_drawdown * 100:.2f}%")
    print(f"Average daily return: {report.avg_daily_return * 100:.4f}%")
    print(f"Daily volatility: {report.daily_volatility * 100:.4f}%")
    print(f"Sharpe ratio: {report.sharpe_ratio:.4f}")
    print(f"Total fills: {report.total_fills}")
    print(f"Buy fills: {report.buy_fill_count}")
    print(f"Sell fills: {report.sell_fill_count}")
    print(f"Gross traded notional: {report.gross_traded_notional:,.2f}")
    print(f"Total commissions: {report.total_commissions:,.2f}")
    print(f"Open positions: {report.open_position_count}")
    print(f"Gross market value: {report.gross_market_value:,.2f}")
    print(
        "Top position: "
        f"{report.top_position_symbol} ({report.top_position_weight * 100:.2f}%)"
    )

    if args.output_dir:
        paths = write_paper_account_report(
            report=report,
            output_dir=Path(args.output_dir),
        )
        print("Report files:")
        for name, path in sorted(paths.items()):
            print(f"  {name}: {path}")