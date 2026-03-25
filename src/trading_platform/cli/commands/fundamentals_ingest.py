from __future__ import annotations

from pathlib import Path

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.data.fundamentals.service import (
    FundamentalsIngestionRequest,
    ingest_fundamentals,
)


def cmd_fundamentals_ingest(args) -> None:
    symbols = resolve_symbols(args)
    request = FundamentalsIngestionRequest(
        symbols=symbols,
        artifact_root=Path(getattr(args, "artifact_root", "data/fundamentals")),
        providers=tuple(getattr(args, "providers", ["sec", "vendor"])),
        sec_companyfacts_root=getattr(args, "sec_companyfacts_root", None),
        sec_submissions_root=getattr(args, "sec_submissions_root", None),
        vendor_file_path=getattr(args, "vendor_file_path", None),
        vendor_api_key=getattr(args, "vendor_api_key", None) or getattr(args, "fundamentals_vendor_api_key", None),
        vendor_cache_enabled=bool(getattr(args, "vendor_cache_enabled", True)),
        vendor_cache_root=Path(getattr(args, "vendor_cache_root")) if getattr(args, "vendor_cache_root", None) else None,
        vendor_cache_ttl_hours=getattr(args, "vendor_cache_ttl_hours", 24.0),
        vendor_force_refresh=bool(getattr(args, "vendor_force_refresh", False)),
        vendor_request_delay_seconds=getattr(args, "vendor_request_delay_seconds", 0.5),
        vendor_max_retries=getattr(args, "vendor_max_retries", 4),
        vendor_max_symbols_per_run=getattr(args, "vendor_max_symbols_per_run", None),
        vendor_max_requests_per_run=getattr(args, "vendor_max_requests_per_run", None),
    )
    result = ingest_fundamentals(request)
    print(f"Ingested fundamentals for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")
    for key, value in sorted(result.items()):
        print(f"{key}: {value}")
