"""CLI: trading-cli data polymarket daily-refresh

Runs the complete daily pipeline:
  1. Fetch new resolutions (last 7 days)
  2. Backfill fills for newly resolved markets
  3. Rebuild wallet profiles
  4. Print summary

All output is tee'd to logs/daily_refresh.log so the log captures
everything even when run headless via Task Scheduler.
"""
from __future__ import annotations

import argparse
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
LOG_PATH = PROJECT_ROOT / "logs" / "daily_refresh.log"


class _TeeWriter:
    """Write to both stdout and a log file simultaneously."""

    def __init__(self, log_file: io.TextIOWrapper, original_stdout: io.TextIOBase) -> None:
        self._log = log_file
        self._stdout = original_stdout

    def write(self, text: str) -> int:
        self._stdout.write(text)
        self._log.write(text)
        return len(text)

    def flush(self) -> None:
        self._stdout.flush()
        self._log.flush()


def cmd_polymarket_daily_refresh(args: argparse.Namespace) -> None:
    initial = getattr(args, "initial", False)
    ts_start = datetime.now(tz=timezone.utc)
    ts = ts_start.strftime("%Y-%m-%d %H:%M UTC")

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Tee all print output to the log file
    log_file = LOG_PATH.open("a", encoding="utf-8")
    original_stdout = sys.stdout
    tee = _TeeWriter(log_file, original_stdout)
    sys.stdout = tee  # type: ignore[assignment]

    try:
        _run_refresh(args, initial, ts)
    except Exception as exc:
        print(f"\n[ERROR] Daily refresh failed: {exc}")
        raise
    finally:
        elapsed = time.time() - ts_start.timestamp()
        ts_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"--- END daily-refresh | {ts_end} | elapsed={elapsed:.0f}s ---")
        print()
        sys.stdout = original_stdout
        log_file.close()


def _run_refresh(args: argparse.Namespace, initial: bool, ts: str) -> None:
    import pandas as pd

    t0 = time.time()
    print(f"--- START daily-refresh | {ts} | initial={initial} ---")
    print()

    # ── Step 1: Fetch new resolutions ────────────────────────────────────────
    print("[Step 1/4] Fetching new resolutions...")
    from trading_platform.polymarket.gamma_resolution_fetcher import GammaResolutionFetcher

    res_csv = PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"
    fetcher = GammaResolutionFetcher()
    res_count = fetcher.fetch_all_resolved(res_csv, since_days=7)
    print(f"  Resolutions: {res_count}")
    print()

    # ── Step 2: Backfill fills for new markets ───────────────────────────────
    print("[Step 2/4] Backfilling fills for newly resolved markets...")
    from trading_platform.polymarket.goldsky_backfill import backfill_goldsky_fills

    fills_dir = PROJECT_ROOT / "data" / "polymarket" / "goldsky_resolved_fills"
    existing_count = len(list(fills_dir.glob("*.parquet"))) if fills_dir.exists() else 0

    limit = 5000 if initial else 500
    result = backfill_goldsky_fills(
        resolution_csv=str(res_csv),
        output_dir=str(fills_dir),
        limit=limit,
        min_volume=5000.0,
        strategy="balanced",
        skip_existing=True,
    )
    new_count = len(list(fills_dir.glob("*.parquet"))) if fills_dir.exists() else 0
    markets_added = new_count - existing_count
    print(f"  New market files: {markets_added}")
    print()

    # ── Step 3: Rebuild wallet profiles ──────────────────────────────────────
    print("[Step 3/4] Rebuilding wallet profiles...")
    from trading_platform.polymarket.goldsky_wallet_profiler import GoldskyWalletProfiler

    profiles_path = PROJECT_ROOT / "data" / "polymarket" / "wallet_profiles.parquet"
    prev_smart: set[str] = set()
    if profiles_path.exists():
        try:
            prev = pd.read_parquet(profiles_path)
            prev_smart = set(prev[prev["smart_money"] == True]["wallet"].tolist())
        except Exception:
            pass

    profiler = GoldskyWalletProfiler(str(fills_dir), str(res_csv))
    df = profiler.build_profiles()

    if not df.empty:
        profiles_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(profiles_path, index=False)

    wallets_profiled = len(df) if not df.empty else 0
    current_smart = set(df[df["smart_money"] == True]["wallet"].tolist()) if not df.empty else set()
    smart_count = len(current_smart)
    print(f"  Wallets profiled: {wallets_profiled}")
    print(f"  Smart money: {smart_count}")
    print()

    # ── Step 4: Summary ──────────────────────────────────────────────────────
    elapsed = time.time() - t0
    promoted = current_smart - prev_smart
    demoted = prev_smart - current_smart

    print(f"[Step 4/4] Summary")
    print(f"  New resolutions fetched: {res_count}")
    print(f"  New market fills added: {markets_added}")
    print(f"  Wallets profiled: {wallets_profiled}")
    print(f"  Smart money: {smart_count} (was {len(prev_smart)})")

    if promoted:
        print(f"  Promoted to smart money ({len(promoted)}):")
        for w in sorted(promoted)[:10]:
            print(f"    + {w[:20]}")
        if len(promoted) > 10:
            print(f"    ... and {len(promoted) - 10} more")

    if demoted:
        print(f"  Demoted from smart money ({len(demoted)}):")
        for w in sorted(demoted)[:10]:
            print(f"    - {w[:20]}")
        if len(demoted) > 10:
            print(f"    ... and {len(demoted) - 10} more")

    print(f"  Elapsed: {elapsed:.0f}s")
    print()

    # Structured summary line (easy to grep)
    print(
        f"REFRESH | {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} | "
        f"markets_added={markets_added} | wallets_profiled={wallets_profiled} | "
        f"smart_money={smart_count} | promoted={len(promoted)} | "
        f"demoted={len(demoted)} | elapsed={elapsed:.0f}s"
    )
