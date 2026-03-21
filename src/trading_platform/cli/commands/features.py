from __future__ import annotations

import argparse
import csv
from pathlib import Path

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.models import FeatureConfig
from trading_platform.services.feature_service import run_feature_build


def cmd_features(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Building features for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    successes: list[str] = []
    failures: list[dict[str, str]] = []

    for symbol in symbols:
        config = FeatureConfig(
            symbol=symbol,
            feature_groups=args.feature_groups,
        )

        try:
            path = run_feature_build(config=config)
        except Exception as exc:
            failures.append(
                {
                    "symbol": symbol,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
            print(f"[FAIL] {symbol}: {type(exc).__name__}: {exc}")
            continue

        successes.append(symbol)
        print(f"[OK] {symbol}: saved features to {path}")

    print(
        f"[SUMMARY] feature build completed: "
        f"{len(successes)} succeeded, {len(failures)} failed"
    )

    if failures:
        failed_symbols = [failure["symbol"] for failure in failures]
        print(f"[SUMMARY] failed symbols: {', '.join(failed_symbols)}")

        report_path = getattr(args, "failure_report", None)
        if report_path:
            _write_failure_report(Path(report_path), failures)
            print(f"[SUMMARY] wrote failure report to {report_path}")


def _write_failure_report(path: Path, failures: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "error_type", "error_message"],
        )
        writer.writeheader()
        writer.writerows(failures)
