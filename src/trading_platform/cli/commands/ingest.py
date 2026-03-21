from __future__ import annotations

import argparse
import csv
from pathlib import Path

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.models import IngestConfig
from trading_platform.services.ingest_service import run_ingest


def cmd_ingest(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Ingesting {len(symbols)} symbol(s): {print_symbol_list(symbols)}")
    successes: list[tuple[str, object]] = []
    failures: list[dict[str, str]] = []

    for symbol in symbols:
        config = IngestConfig(
            symbol=symbol,
            start=args.start,
            end=getattr(args, "end", None),
            interval=getattr(args, "interval", "1d"),
        )
        try:
            path = run_ingest(config=config)
        except Exception as exc:
            failures.append(
                {
                    "symbol": symbol,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            print(f"[ERROR] {symbol}: {type(exc).__name__}: {exc}")
            if getattr(args, "fail_fast", False):
                break
            continue

        successes.append((symbol, path))
        print(f"[OK] {symbol}: saved raw data to {path}")

    print(
        f"Ingest summary: successes={len(successes)}, failures={len(failures)}"
    )
    if failures:
        failed_symbols = [item["symbol"] for item in failures]
        print(f"Failed symbols: {', '.join(failed_symbols)}")

    failure_report = getattr(args, "failure_report", None)
    if failure_report and failures:
        report_path = Path(failure_report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["symbol", "error_type", "error"])
            writer.writeheader()
            writer.writerows(failures)
        print(f"Saved ingest failure report to {report_path}")
