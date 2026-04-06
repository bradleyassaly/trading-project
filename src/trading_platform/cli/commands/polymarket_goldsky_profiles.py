"""CLI: trading-cli data polymarket goldsky-wallet-profiles"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cmd_polymarket_goldsky_profiles(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.goldsky_wallet_profiler import GoldskyWalletProfiler

    fills_dir = getattr(args, "fills_dir", None) or str(PROJECT_ROOT / "data" / "polymarket" / "goldsky_resolved_fills")
    resolution = getattr(args, "resolution", None) or str(PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv")
    output = Path(getattr(args, "output", None) or str(PROJECT_ROOT / "data" / "polymarket" / "wallet_profiles.parquet"))

    print("Goldsky Wallet Profiler")
    print(f"  fills dir    : {fills_dir}")
    print(f"  resolution   : {resolution}")
    print(f"  output       : {output}")
    print()

    profiler = GoldskyWalletProfiler(fills_dir, resolution)

    # Report resolution join diagnostics
    import csv as _csv
    _rp_map = {}
    with open(resolution, newline="", encoding="utf-8-sig") as _f:
        for _row in _csv.DictReader(_f):
            _tid = _row.get("ticker", "").strip()
            _rp = _row.get("resolution_price", "")
            if _tid and _rp:
                _rp_map[_tid] = float(_rp)
    _all_tokens = set()
    for _p in Path(fills_dir).glob("*.parquet"):
        try:
            _df = pd.read_parquet(_p, columns=["taker_asset_id"])
            _all_tokens.update(_df["taker_asset_id"].astype(str).str.strip().unique())
        except Exception:
            pass
    _matched = {t: _rp_map[t] for t in _all_tokens if t in _rp_map}
    _yes = sum(1 for rp in _matched.values() if rp >= 99)
    _no = sum(1 for rp in _matched.values() if rp <= 1)
    _total = _yes + _no
    _br = _yes / _total * 100 if _total > 0 else 0
    print(f"  Resolution join: {len(_matched)} tokens matched ({len(_matched)}/{len(_all_tokens)} = {len(_matched)/len(_all_tokens)*100:.0f}% match rate)")
    print(f"  Base rate: {_yes} YES, {_no} NO ({_br:.1f}% YES tokens)")
    if _br < 40 or _br > 60:
        print(f"  WARNING: Base rate {_br:.1f}% outside 40-60% target")
        print(f"  Run: backfill-goldsky-fills --strategy balanced --skip-existing")
    print()

    df = profiler.build_profiles()

    if df.empty:
        print("[DONE] No profiles generated (insufficient data).")
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output, index=False)

    smart = df[df["smart_money"] == True]
    early_informed = df[df["is_early_informed"] == True]

    print(f"[DONE] Wallet profiles built.")
    print(f"  Total wallets profiled     : {len(df)}")
    print(f"  Early informed             : {len(early_informed)}")
    print(f"  Smart money flagged        : {len(smart)}")
    if "avg_fills_per_token" in df.columns:
        print(f"  Avg fills per token        : {df['avg_fills_per_token'].mean():.1f}")
    print(f"  Output                     : {output}")

    # Uncertain early trades bucket distribution
    if "uncertain_early_trades" in df.columns:
        print(f"\n  UET distribution:")
        print(f"  {'Bucket':<20} {'Count':>6} {'Median EWR':>12}")
        for label, lo, hi in [("1-2 trades", 1, 2), ("3-5 trades", 3, 5),
                               ("6-10 trades", 6, 10), ("11-20 trades", 11, 20), ("21+ trades", 21, 99999)]:
            bucket = df[(df["uncertain_early_trades"] >= lo) & (df["uncertain_early_trades"] <= hi)]
            if len(bucket) > 0:
                med = bucket["early_win_rate"].median()
                print(f"  {label:<20} {len(bucket):>6} {med:>12.1%}")

    # Confidence score stats
    if "confidence_score" in df.columns:
        cs = df["confidence_score"]
        print(f"\n  Confidence score: mean={cs.mean():.3f} median={cs.median():.3f} max={cs.max():.3f}")

    # Domain breakdown for top 20
    if "best_domain" in df.columns and len(early_informed) > 0:
        sort_col = "confidence_score" if "confidence_score" in early_informed.columns else "edge"
        print(f"\n  Top 20 by {sort_col}:")
        top = early_informed.nlargest(20, sort_col)
        print(f"  {'Wallet':<18} {'Conf':>6} {'Domain':<12} {'DomWR':>6} {'EWR':>6} {'UET':>5} {'Vol':>10}")
        for _, row in top.iterrows():
            print(f"  {row['wallet'][:18]:<18} {row.get('confidence_score', 0):>6.2f} "
                  f"{row.get('best_domain', '?'):<12} {row.get('best_domain_win_rate', 0):>6.1%} "
                  f"{row['early_win_rate']:>6.1%} {int(row.get('uncertain_early_trades', 0)):>5} "
                  f"${row['total_volume_usdc']:>9,.0f}")
