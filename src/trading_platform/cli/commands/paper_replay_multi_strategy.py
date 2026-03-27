from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_execution_config
from trading_platform.paper.multi_strategy_replay import build_requested_replay_dates, run_multi_strategy_paper_replay
from trading_platform.portfolio.strategy_execution_handoff import (
    StrategyExecutionHandoffConfig,
    resolve_strategy_execution_handoff,
    write_strategy_execution_handoff_summary,
)


def cmd_paper_replay_multi_strategy(args) -> None:
    requested_dates = build_requested_replay_dates(
        start_date=getattr(args, "start_date", None),
        end_date=getattr(args, "end_date", None),
        explicit_dates=list(getattr(args, "dates", []) or []),
        max_steps=getattr(args, "max_steps", None),
    )
    handoff = resolve_strategy_execution_handoff(
        args.config,
        config=StrategyExecutionHandoffConfig(),
    )
    handoff_summary_path = write_strategy_execution_handoff_summary(
        handoff=handoff,
        output_dir=Path(args.output_dir),
        artifact_name="paper_active_strategy_summary.json",
    )
    if handoff.portfolio_config is None:
        if handoff.summary.get("fail_if_no_active_strategies"):
            raise ValueError(f"No active strategies available for paper trading replay: {args.config}")
        print("No active strategies available for paper trading replay.")
        print(f"Handoff summary: {handoff_summary_path}")
        return

    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    replay = run_multi_strategy_paper_replay(
        portfolio_config=handoff.portfolio_config,
        handoff_summary=handoff.summary,
        requested_dates=requested_dates,
        state_path=args.state_path,
        output_dir=args.output_dir,
        execution_config=execution_config,
        auto_apply_fills=bool(getattr(args, "auto_apply_fills", True)),
        reset_state=bool(getattr(args, "reset_state", False)),
    )

    print(f"Requested dates: {len(replay.requested_dates)}")
    print(f"Processed dates: {len(replay.steps)}")
    print(f"Skipped dates: {len(replay.skipped_dates)}")
    if replay.steps:
        print(f"Final as of: {replay.summary.get('final_as_of')}")
        print(f"Final equity: {float(replay.summary.get('final_equity', 0.0)):,.2f}")
        print(f"Cumulative realized PnL: {float(replay.summary.get('cumulative_realized_pnl', 0.0)):,.2f}")
        print(f"Cumulative fees: {float(replay.summary.get('cumulative_fees', 0.0)):,.2f}")
    if replay.skipped_dates:
        print("Skipped detail:")
        for row in replay.skipped_dates:
            print(f"  {row['requested_date']}: {row['reason']}")
    print("Artifacts:")
    for name, path in sorted(replay.artifact_paths.items()):
        print(f"  {name}: {path}")
    print(f"  paper_active_strategy_summary_path: {handoff_summary_path}")
