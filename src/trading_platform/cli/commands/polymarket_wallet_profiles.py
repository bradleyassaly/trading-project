"""
CLI command: trading-cli data polymarket wallet-profiles

Builds wallet performance profiles from trade history.

Usage
-----
    trading-cli data polymarket wallet-profiles \
      --trades-csv data/polymarket/raw/blockchain_trades.csv \
      --resolution-csv data/polymarket/blockchain/resolution.csv

    trading-cli data polymarket wallet-profiles \
      --trades-dir data/polymarket/data_api_trades \
      --resolution-csv data/polymarket/blockchain/resolution.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _project_relative(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else PROJECT_ROOT / p


def cmd_polymarket_wallet_profiles(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_profiler import WalletProfiler

    trades_csv = getattr(args, "trades_csv", None)
    trades_dir = getattr(args, "trades_dir", None)
    use_graph = getattr(args, "use_graph_resolution", False)
    resolution_csv_str = getattr(args, "resolution_csv", None)
    output = _project_relative(getattr(args, "output", None) or "data/polymarket/wallet_profiles.parquet")

    # Graph-based resolution path (no resolution CSV needed)
    if use_graph and trades_dir:
        trades_dir_path = Path(trades_dir).expanduser()
        if not trades_dir_path.exists():
            print(f"[ERROR] Trades directory not found: {trades_dir_path}")
            return
        csv_count = len(list(trades_dir_path.glob("*.csv")))
        print("Polymarket Wallet Profiler (Graph Resolution)")
        print(f"  trades dir   : {trades_dir_path} ({csv_count} files)")
        print(f"  resolution   : The Graph positions subgraph")
        print(f"  output       : {output}")
        print()
        profiler = WalletProfiler()
        result = profiler.build_profiles_from_data_api(trades_dir_path, output)
        print("[DONE] Wallet profiles built.")
        print(f"  Wallets analyzed            : {result.wallets_analyzed}")
        print(f"  With resolved trades        : {result.wallets_with_resolved_trades}")
        print(f"  Smart money flagged         : {result.smart_money_count}")
        print(f"  Output                      : {result.output_path}")
        return

    # Standard resolution CSV path
    if not resolution_csv_str:
        print("[ERROR] Provide --resolution-csv or use --use-graph-resolution with --trades-dir")
        return
    resolution_csv = Path(resolution_csv_str).expanduser()
    if not resolution_csv.exists():
        print(f"[ERROR] Resolution CSV not found: {resolution_csv}")
        return

    # Handle --trades-dir: concat all CSVs into one temp file
    if trades_dir:
        trades_dir_path = Path(trades_dir).expanduser()
        if not trades_dir_path.exists():
            print(f"[ERROR] Trades directory not found: {trades_dir_path}")
            return
        csv_files = sorted(trades_dir_path.glob("*.csv"))
        if not csv_files:
            print(f"[ERROR] No CSV files in {trades_dir_path}")
            return
        print(f"Polymarket Wallet Profiler")
        print(f"  trades dir   : {trades_dir_path} ({len(csv_files)} files)")
        print(f"  resolution   : {resolution_csv}")
        print(f"  output       : {output}")
        print()

        # Concat all CSVs into a single temp file, deduplicating
        seen: set[str] = set()
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8")
        fieldnames: list[str] | None = None
        writer = None
        total_rows = 0

        for csv_file in csv_files:
            try:
                with csv_file.open(newline="", encoding="utf-8-sig") as fh:
                    reader = csv.DictReader(fh)
                    if fieldnames is None and reader.fieldnames:
                        fieldnames = list(reader.fieldnames)
                        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
                        writer.writeheader()
                    for row in reader:
                        # Deduplicate by (wallet, token_id, timestamp)
                        wallet = row.get("wallet") or row.get("proxyWallet") or ""
                        token = row.get("token_id") or row.get("asset") or ""
                        ts = row.get("timestamp", "")
                        key = f"{wallet}|{token}|{ts}"
                        if key in seen:
                            continue
                        seen.add(key)
                        if writer:
                            writer.writerow(row)
                            total_rows += 1
            except Exception as exc:
                logger.debug("Skipping %s: %s", csv_file.name, exc)

        tmp.close()
        print(f"  combined     : {total_rows} unique trades from {len(csv_files)} files")
        trades_csv_path = Path(tmp.name)
    elif trades_csv:
        trades_csv_path = Path(trades_csv).expanduser()
        if not trades_csv_path.exists():
            print(f"[ERROR] Trades CSV not found: {trades_csv_path}")
            return
        print(f"Polymarket Wallet Profiler")
        print(f"  trades csv   : {trades_csv_path}")
        print(f"  resolution   : {resolution_csv}")
        print(f"  output       : {output}")
        print()
    else:
        print("[ERROR] Provide --trades-csv or --trades-dir")
        return

    profiler = WalletProfiler()
    result = profiler.build_profiles(trades_csv_path, resolution_csv, output)

    print("[DONE] Wallet profiles built.")
    print(f"  Wallets analyzed            : {result.wallets_analyzed}")
    print(f"  With resolved trades        : {result.wallets_with_resolved_trades}")
    print(f"  Smart money flagged         : {result.smart_money_count}")
    print(f"  Output                      : {result.output_path}")

    # Cleanup temp file
    if trades_dir:
        try:
            trades_csv_path.unlink()
        except OSError:
            pass
