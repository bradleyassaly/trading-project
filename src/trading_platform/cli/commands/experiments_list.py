from __future__ import annotations

from pathlib import Path

from trading_platform.research.experiment_tracking import list_recent_experiments


def cmd_experiments_list(args) -> None:
    frame = list_recent_experiments(
        tracker_dir=Path(args.tracker_dir),
        limit=args.limit,
    )
    if frame.empty:
        print("No experiments found.")
        return

    columns = [
        "timestamp",
        "experiment_type",
        "experiment_id",
        "signal_family",
        "portfolio_sharpe",
        "duplicate_of",
    ]
    available = [column for column in columns if column in frame.columns]
    print(frame[available].fillna("").to_string(index=False))
