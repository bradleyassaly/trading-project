"""
CLI command: trading-cli data kalshi features

Reads tracked tickers from the Kalshi config YAML, loads trade history
from ``data/kalshi/trades/<TICKER>.parquet``, computes features, and
writes output to ``data/kalshi/features/<TICKER>.parquet``.

Usage
-----
    trading-cli data kalshi features --config configs/kalshi.yaml
    trading-cli data kalshi features --config configs/kalshi.yaml \\
        --tickers SOME-TICKER-24 OTHER-TICKER-24 \\
        --period 1h \\
        --output-dir data/kalshi/features

If no tickers are provided on the command line, the command reads
``ingestion.tracked_tickers`` from the YAML config.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from trading_platform.kalshi.features import (
    KALSHI_FEATURE_GROUPS,
    build_kalshi_features,
    load_trades_parquet,
    write_feature_parquet,
)

logger = logging.getLogger(__name__)


def _load_config(config_path: str) -> dict[str, Any]:
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def _parse_close_time(raw: str | None) -> datetime | None:
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    logger.warning("Could not parse close_time %r — time-decay features will be null.", raw)
    return None


def cmd_kalshi_features(args: argparse.Namespace) -> None:
    config: dict[str, Any] = {}
    if args.config:
        config = _load_config(args.config)

    ingestion_cfg = config.get("ingestion", {})

    # Determine tickers: CLI flag overrides config
    tickers: list[str] = list(args.tickers) if args.tickers else []
    if not tickers:
        tickers = ingestion_cfg.get("tracked_tickers", [])
    if not tickers:
        print("[INFO] No tickers specified. Use --tickers or set ingestion.tracked_tickers in the config.")
        return

    trades_dir = Path(args.trades_dir)
    output_dir = Path(args.output_dir)
    period: str = args.period
    feature_groups: list[str] | None = args.feature_groups or None

    print(f"Building Kalshi features for {len(tickers)} ticker(s): {', '.join(tickers)}")
    print(f"  trades dir : {trades_dir}")
    print(f"  output dir : {output_dir}")
    print(f"  period     : {period}")

    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    for ticker in tickers:
        try:
            trades = load_trades_parquet(trades_dir, ticker)
            logger.debug("Loaded %d trades for %s", len(trades), ticker)

            # Optional: resolve close_time from a per-ticker override in config
            close_time_raw = ingestion_cfg.get("close_times", {}).get(ticker)
            close_time = _parse_close_time(close_time_raw)

            df = build_kalshi_features(
                trades,
                ticker=ticker,
                period=period,
                close_time=close_time,
                feature_groups=feature_groups,
            )
            path = write_feature_parquet(df, output_dir, ticker)
            successes.append(ticker)
            print(f"[OK] {ticker}: {len(df)} bars → {path}")

        except FileNotFoundError as exc:
            failures.append((ticker, str(exc)))
            print(f"[SKIP] {ticker}: no trade data found ({exc})")
        except Exception as exc:
            failures.append((ticker, f"{type(exc).__name__}: {exc}"))
            print(f"[FAIL] {ticker}: {type(exc).__name__}: {exc}")
            logger.exception("Feature build failed for %s", ticker)

    print(
        f"[SUMMARY] {len(successes)} succeeded, "
        f"{len(failures)} failed/skipped out of {len(tickers)} tickers"
    )
    if failures:
        print("[SUMMARY] Issues:")
        for ticker, msg in failures:
            print(f"  {ticker}: {msg}")
