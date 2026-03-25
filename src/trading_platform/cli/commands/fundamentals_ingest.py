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
        vendor_api_key=getattr(args, "vendor_api_key", None),
    )
    result = ingest_fundamentals(request)
    print(f"Ingested fundamentals for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")
    for key, value in sorted(result.items()):
        print(f"{key}: {value}")
