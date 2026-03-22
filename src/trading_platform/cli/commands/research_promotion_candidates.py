from __future__ import annotations

from pathlib import Path

from trading_platform.research.registry import build_promotion_candidates


def cmd_research_promotion_candidates(args) -> None:
    result = build_promotion_candidates(
        artifacts_root=Path(args.artifacts_root),
        output_dir=Path(args.output_dir),
    )
    print(f"Eligible promotion candidates: {result['eligible_count']}")
    print(f"Promotion candidates JSON: {result['promotion_candidates_json_path']}")
    print(f"Promotion candidates CSV: {result['promotion_candidates_csv_path']}")
