from __future__ import annotations

from pathlib import Path

from trading_platform.research.registry import build_research_leaderboard


def cmd_research_leaderboard(args) -> None:
    result = build_research_leaderboard(
        artifacts_root=Path(args.artifacts_root),
        output_dir=Path(args.output_dir),
        metric=args.metric,
        group_by=args.group_by,
        limit=args.limit,
    )
    print(f"Leaderboard rows: {result['row_count']}")
    print(f"Leaderboard JSON: {result['leaderboard_json_path']}")
    print(f"Leaderboard CSV: {result['leaderboard_csv_path']}")
