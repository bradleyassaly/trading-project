from __future__ import annotations

from trading_platform.portfolio.strategy_portfolio import load_strategy_portfolio


def cmd_strategy_portfolio_show(args) -> None:
    payload = load_strategy_portfolio(args.portfolio)
    summary = payload.get("summary", {})
    print(f"Selected strategies: {summary.get('total_selected_strategies', 0)}")
    print(f"Total active weight: {summary.get('total_active_weight', 0.0)}")
    print(f"Warnings: {summary.get('warning_count', 0)}")
    for row in payload.get("selected_strategies", []):
        print(
            f"- {row['preset_name']}: weight={row['allocation_weight']:.6f} "
            f"family={row.get('signal_family')} universe={row.get('universe')}"
        )
