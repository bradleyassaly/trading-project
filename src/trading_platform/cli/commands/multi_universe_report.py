from __future__ import annotations

from pathlib import Path

from trading_platform.research.multi_universe import (
    build_multi_universe_comparison_report,
)


def cmd_multi_universe_report(args) -> None:
    result = build_multi_universe_comparison_report(
        output_dir=Path(args.output_dir),
    )
    print("Multi-universe comparison report complete.")
    for key, value in sorted(result.items()):
        print(f"{key}: {value}")
