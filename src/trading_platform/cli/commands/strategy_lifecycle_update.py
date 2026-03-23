from __future__ import annotations

from pathlib import Path

from trading_platform.governance.strategy_lifecycle import update_strategy_lifecycle_state


def cmd_strategy_lifecycle_update(args) -> None:
    result = update_strategy_lifecycle_state(
        lifecycle_path=Path(args.lifecycle),
        strategy_id=args.strategy_id,
        new_state=args.state,
        reason=args.reason,
        output_path=Path(args.output_path) if getattr(args, "output_path", None) else None,
    )
    print(f"Updated lifecycle: {result['strategy_lifecycle_json_path']}")
