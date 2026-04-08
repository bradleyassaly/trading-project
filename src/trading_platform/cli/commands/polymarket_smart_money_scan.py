# DEPRECATED: This module used EWR-based smart money detection.
# Replaced by whale_tripwire.py + whale_signal_engine.py (9-signal system).
# Do not run or import this module in new code.
# Preserved for reference only.
"""
CLI: trading-cli data polymarket smart-money-scan
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cmd_polymarket_smart_money_scan(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.smart_money_signal import SmartMoneySignalGenerator

    profiles_path = PROJECT_ROOT / "data" / "polymarket" / "wallet_profiles.parquet"
    fills_dir = PROJECT_ROOT / "data" / "polymarket" / "orderflow"
    live_db = PROJECT_ROOT / "data" / "polymarket" / "live" / "prices.db"

    if not profiles_path.exists():
        print("[ERROR] Wallet profiles not found")
        return

    # Load live markets — filter expired and illiquid
    today_str = date.today().isoformat()
    token_to_question: dict[str, str] = {}
    active_tokens: set[str] = set()
    expired_count = 0
    total_db = 0

    if live_db.exists():
        try:
            conn = sqlite3.connect(str(live_db), check_same_thread=False)
            rows = conn.execute(
                "SELECT yes_token_id, question, end_date_iso FROM markets WHERE yes_token_id IS NOT NULL"
            ).fetchall()
            conn.close()
            for tid, q, end_date in rows:
                total_db += 1
                if end_date and end_date < today_str:
                    expired_count += 1
                    continue
                token_to_question[tid] = q
                active_tokens.add(tid)
        except Exception:
            pass

    gen = SmartMoneySignalGenerator(profiles_path)
    print("Smart Money Scan")
    print(f"  Smart wallets: {gen.smart_wallet_count}")
    print(f"  Markets in DB: {total_db}")
    print(f"  Expired filtered: {expired_count}")
    print(f"  Active markets: {len(active_tokens)}")

    if not fills_dir.exists():
        print("  [WARN] No orderflow data")
        return

    # Load fills only for active markets
    all_dfs: list[pd.DataFrame] = []
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=48)
    for path in fills_dir.glob("*.parquet"):
        try:
            df = pd.read_parquet(path)
            if df.empty or "timestamp" not in df.columns:
                continue
            try:
                df = df[df["timestamp"] >= cutoff]
            except TypeError:
                pass
            if df.empty:
                continue
            # Filter to active markets by checking token_id prefix
            if "taker_asset_id" in df.columns:
                taker_ids = df["taker_asset_id"].astype(str).unique()
                has_active = any(
                    any(at.startswith(tid[:30]) for at in taker_ids)
                    for tid in active_tokens
                )
                if not has_active:
                    continue
            all_dfs.append(df)
        except Exception:
            continue

    if not all_dfs:
        print("  No recent fills for active markets")
        return

    fills = pd.concat(all_dfs, ignore_index=True)
    print(f"  Recent fills: {len(fills):,} across {len(all_dfs)} active markets")

    signals = gen.compute(fills)

    # Match to active markets and filter
    matched = []
    for sig in signals:
        question = ""
        for full_tid, q in token_to_question.items():
            if full_tid.startswith(sig.token_id[:30]):
                question = q
                break
        if question:
            matched.append((sig, question))

    stale = sum(1 for s, _ in matched if s.hours_since_last_smart_trade > 24)
    fresh = [m for m in matched if m[0].hours_since_last_smart_trade <= 24]

    print(f"  Signals on active markets: {len(matched)} ({stale} stale >24h)")
    print()

    if not matched:
        print("  No actionable smart money signals today.")
        return

    print(f"  {'Question':<42} {'Dir':>4} {'Wt$':>9} {'Hrs':>5} {'Edge':>6} {'Status'}")
    print(f"  {'-'*42} {'-'*4} {'-'*9} {'-'*5} {'-'*6} {'-'*6}")
    for sig, question in matched[:15]:
        q_trunc = question[:42] if question else sig.token_id[:42]
        status = "STALE" if sig.hours_since_last_smart_trade > 24 else "FRESH"
        print(
            f"  {q_trunc:<42} {sig.direction:>4} {sig.weighted_net_volume:>9,.0f} "
            f"{sig.hours_since_last_smart_trade:>5.1f} {sig.top_wallet_edge:>6.1%} {status}"
        )
