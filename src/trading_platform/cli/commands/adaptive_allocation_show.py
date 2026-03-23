from __future__ import annotations

from trading_platform.portfolio.adaptive_allocation import load_adaptive_allocation


def cmd_adaptive_allocation_show(args) -> None:
    payload = load_adaptive_allocation(args.allocation)
    summary = payload.get("summary", {})
    print(f"Selected strategies: {summary.get('total_selected_strategies', 0)}")
    print(f"Warnings: {summary.get('warning_count', 0)}")
    print(f"Absolute weight change: {summary.get('absolute_weight_change', 0.0)}")
    for row in payload.get("strategies", []):
        print(
            f"- {row['preset_name']}: prior={float(row['prior_weight']):.6f} "
            f"adjusted={float(row['adjusted_weight']):.6f} "
            f"recommendation={row.get('monitoring_recommendation')}"
        )
