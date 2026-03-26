from __future__ import annotations

from pathlib import Path

from trading_platform.cli.config_support import load_and_apply_workflow_config
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.loader import load_fundamentals_snapshot_workflow_config
from trading_platform.data.fundamentals.service import (
    FundamentalsSnapshotBuildRequest,
    build_sec_fundamentals_snapshot,
)


def cmd_fundamentals_snapshot_build(args) -> None:
    load_and_apply_workflow_config(
        args,
        loader=load_fundamentals_snapshot_workflow_config,
    )
    symbols = resolve_symbols(args)
    request = FundamentalsSnapshotBuildRequest(
        symbols=symbols,
        artifact_root=Path(getattr(args, "artifact_root", "data/fundamentals")),
        raw_sec_cache_root=Path(getattr(args, "raw_sec_cache_root")) if getattr(args, "raw_sec_cache_root", None) else None,
        symbol_cik_map_path=Path(getattr(args, "symbol_cik_map_path")) if getattr(args, "symbol_cik_map_path", None) else None,
        sec_user_agent=getattr(args, "sec_user_agent", None),
        sec_request_delay_seconds=getattr(args, "sec_request_delay_seconds", 0.2),
        sec_max_retries=getattr(args, "sec_max_retries", 4),
        cache_enabled=bool(getattr(args, "cache_enabled", True)),
        cache_ttl_days=getattr(args, "cache_ttl_days", 30.0),
        force_refresh=bool(getattr(args, "force_refresh", False)),
        max_symbols_per_run=getattr(args, "max_symbols_per_run", None),
        max_requests_per_run=getattr(args, "max_requests_per_run", None),
        build_daily_features=bool(getattr(args, "build_daily_features", True)),
        calendar_dir=Path(getattr(args, "calendar_dir")) if getattr(args, "calendar_dir", None) else None,
        offline=bool(getattr(args, "offline", False)),
    )
    result = build_sec_fundamentals_snapshot(request)
    print(f"Built SEC fundamentals snapshot for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")
    for key, value in sorted(result.items()):
        print(f"{key}: {value}")
