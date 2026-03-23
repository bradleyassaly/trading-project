from __future__ import annotations

from pathlib import Path

from trading_platform.portfolio.strategy_monitoring import recommend_kill_switch_actions


def cmd_strategy_monitor_recommend_kill_switch(args) -> None:
    result = recommend_kill_switch_actions(
        strategy_monitoring_path=Path(args.monitoring),
        output_dir=Path(args.output_dir) if getattr(args, "output_dir", None) else None,
        include_review=bool(getattr(args, "include_review", False)),
    )
    print(f"Recommendation count: {result['recommendation_count']}")
    print(f"Kill-switch recommendations JSON: {result['kill_switch_recommendations_json_path']}")
    print(f"Kill-switch recommendations CSV: {result['kill_switch_recommendations_csv_path']}")
