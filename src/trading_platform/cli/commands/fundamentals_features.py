from __future__ import annotations

from pathlib import Path

from trading_platform.cli.common import resolve_symbols
from trading_platform.data.fundamentals.service import (
    FundamentalFeatureBuildRequest,
    build_daily_fundamental_features,
)


def cmd_fundamentals_features(args) -> None:
    symbols = resolve_symbols(args)
    request = FundamentalFeatureBuildRequest(
        artifact_root=Path(getattr(args, "artifact_root", "data/fundamentals")),
        daily_features_path=Path(getattr(args, "daily_features_path")) if getattr(args, "daily_features_path", None) else None,
        calendar_dir=Path(getattr(args, "calendar_dir", "data/features")),
        symbols=symbols,
    )
    result = build_daily_fundamental_features(request)
    for key, value in sorted(result.items()):
        print(f"{key}: {value}")
