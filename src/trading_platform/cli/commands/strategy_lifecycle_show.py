from __future__ import annotations

from pathlib import Path

from trading_platform.governance.strategy_lifecycle import load_strategy_lifecycle


def cmd_strategy_lifecycle_show(args) -> None:
    payload = load_strategy_lifecycle(Path(args.lifecycle))
    summary = payload.get("summary", {})
    print(f"Strategy count: {summary.get('strategy_count', 0)}")
    print(f"Active: {summary.get('active_count', 0)}")
    print(f"Under review: {summary.get('under_review_count', 0)}")
    print(f"Degraded: {summary.get('degraded_count', 0)}")
    print(f"Demoted: {summary.get('demoted_count', 0)}")
    for row in payload.get("strategies", [])[:20]:
        print(
            f"- {row.get('strategy_id')}: state={row.get('current_state')} "
            f"reasons={','.join(row.get('latest_reasons', []))}"
        )
