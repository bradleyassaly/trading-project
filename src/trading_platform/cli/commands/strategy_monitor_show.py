from __future__ import annotations

from trading_platform.portfolio.strategy_monitoring import load_strategy_monitoring


def cmd_strategy_monitor_show(args) -> None:
    payload = load_strategy_monitoring(args.monitoring)
    summary = payload.get("summary", {})
    print(f"Selected strategies: {summary.get('selected_strategy_count', 0)}")
    print(f"Warning strategies: {summary.get('warning_strategy_count', 0)}")
    print(f"Deactivation candidates: {summary.get('deactivation_candidate_count', 0)}")
    print(f"Aggregate return: {summary.get('aggregate_return')}")
    print(f"Aggregate drawdown: {summary.get('aggregate_drawdown')}")
