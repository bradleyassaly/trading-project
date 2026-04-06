"""CLI commands for the wallet intelligence system."""
from __future__ import annotations

import argparse


def cmd_sync_wallet_trades(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_trade_sync import sync_wallet_trades
    from trading_platform.polymarket.wallet_db import WalletDB

    top_n = int(getattr(args, "top_n", None) or 200)
    db = WalletDB()

    # Seed from existing parquet profiles if DB is empty
    _seed_from_parquet(db)

    print(f"Syncing trades for up to {top_n} wallets...")
    result = sync_wallet_trades(db=db, top_n=top_n)
    print(f"[DONE] Wallets synced: {result['wallets_synced']}, "
          f"new trades: {result['new_trades']}, "
          f"skipped: {result['skipped']}, errors: {result['errors']}")
    print(f"  DB stats: {db.stats()}")


def cmd_sync_wallet_positions(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_position_sync import sync_wallet_positions
    from trading_platform.polymarket.wallet_db import WalletDB

    top_n = getattr(args, "top_n", None)
    top_n = int(top_n) if top_n else None
    db = WalletDB()

    _seed_from_parquet(db)

    print(f"Syncing positions...")
    result = sync_wallet_positions(db=db, top_n=top_n)
    print(f"[DONE] Wallets synced: {result['wallets_synced']}, "
          f"positions: {result['positions']}, errors: {result['errors']}")


def cmd_rebuild_wallet_profiles(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_profile_rebuild import rebuild_profiles
    from trading_platform.polymarket.wallet_db import WalletDB

    db = WalletDB()
    print("Rebuilding wallet profiles from trade data...")
    result = rebuild_profiles(db=db)
    print(f"[DONE] Profiles rebuilt: {result['wallets_rebuilt']} "
          f"in {result['elapsed_seconds']}s")

    # Show leaderboard
    top = db.get_leaderboard(limit=10)
    if top:
        print(f"\n  Top 10 by equity score:")
        print(f"  {'Wallet':<18} {'Type':<14} {'EqScore':>8} {'DirWR':>6} {'Trades':>6}")
        for w in top:
            dwr = f"{w['directional_win_rate']:.0%}" if w.get("directional_win_rate") else "n/a"
            print(f"  {w['wallet'][:18]:<18} {w.get('wallet_type','?'):<14} "
                  f"{w.get('equity_score', 0):>8.3f} {dwr:>6} "
                  f"{w.get('resolved_trades', 0):>6}")


def cmd_classify_wallet_buckets(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_buckets import classify_wallets
    from trading_platform.polymarket.wallet_db import WalletDB

    db = WalletDB()
    print("Classifying wallet buckets...")
    result = classify_wallets(db=db)
    print(f"[DONE] Classified {result['classified']} wallets in {result['elapsed']}s")
    print(f"  Buckets: {result['buckets']}")


def cmd_seed_from_leaderboard(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.seed_leaderboard import seed_from_leaderboard
    from trading_platform.polymarket.wallet_db import WalletDB

    db = WalletDB()
    _seed_from_parquet(db)

    print("Seeding from Polymarket leaderboard...")
    result = seed_from_leaderboard(db=db)
    print(f"[DONE] Wallets: {result['wallets_collected']}, synced: {result['wallets_synced']}")
    print(f"  New trades: {result['new_trades']}, positions: {result['new_positions']}, errors: {result['errors']}")
    print(f"  DB stats: {db.stats()}")

    # Auto-enrich and rebuild
    print("\nEnriching trade resolutions...")
    from trading_platform.polymarket.enrich_resolution import enrich_trades
    enrich_result = enrich_trades(db=db)
    print(f"  Enriched: {enrich_result['enriched']} trades")

    print("Rebuilding profiles...")
    from trading_platform.polymarket.wallet_profile_rebuild import rebuild_profiles
    rebuild_result = rebuild_profiles(db=db)
    print(f"  Rebuilt: {rebuild_result['wallets_rebuilt']} profiles")

    top = db.get_leaderboard(limit=5)
    if top:
        print("\n  Top 5 by equity score:")
        for w in top:
            pnl = w.get("net_pnl_usdc")
            pnl_s = f"${pnl:+,.0f}" if pnl else "--"
            print(f"    {w['wallet'][:18]} eq={w.get('equity_score', 0):.3f} pnl={pnl_s} type={w.get('wallet_type', '?')}")


def cmd_build_address_map(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.address_map import AddressMap
    import pandas as pd
    from pathlib import Path

    profiles_path = Path("data/polymarket/wallet_profiles.parquet")
    if not profiles_path.exists():
        print("No wallet_profiles.parquet found")
        return

    df = pd.read_parquet(profiles_path)
    wallets = df["wallet"].tolist()
    top_n = int(getattr(args, "top_n", None) or len(wallets))
    wallets = wallets[:top_n]

    print(f"Building address map for {len(wallets)} wallets...")
    am = AddressMap()
    new = am.build_map(wallets)
    print(f"[DONE] {new} new mappings, {am.size} total in map")


def cmd_enrich_trade_resolution(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.enrich_resolution import enrich_trades
    from trading_platform.polymarket.wallet_db import WalletDB

    db = WalletDB()
    print("Enriching trades with resolution outcomes...")
    result = enrich_trades(db=db)
    print(f"[DONE] Enriched {result['enriched']} / {result['total_unresolved']} trades")
    print(f"  Gamma pass: {result.get('gamma_enriched', result['enriched'])}")
    print(f"  Positions pass: {result.get('positions_enriched', 0)}")
    print(f"  Resolved tokens available: {result['resolved_tokens_available']}")
    print(f"  Elapsed: {result['elapsed']}s")

    # Show resolved counts
    stats = db.stats()
    with db._lock:
        resolved_count = db._conn.execute("SELECT COUNT(*) FROM wallet_trades WHERE market_resolved = 1").fetchone()[0]
    print(f"  DB trades resolved: {resolved_count} / {stats['trades']}")


def cmd_compute_signals(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.signal_engine import SignalEngine

    print("Computing market signals from wallet positions...")
    engine = SignalEngine()
    signals = engine.compute_market_signals()

    if not signals:
        print("[DONE] No multi-wallet signals found.")
        return

    print(f"[DONE] {len(signals)} signals computed.")
    actionable = engine.get_actionable_signals()
    print(f"  Actionable (2+ wallets, 60%+ conf): {len(actionable)}")
    for s in actionable[:10]:
        print(f"    {s.get('direction', '?'):>3} {s.get('market_title', '')[:40]:<40} "
              f"wallets={s.get('directional_wallet_count', 0)} "
              f"conf={s.get('confidence', 0):.0%} "
              f"${s.get('weighted_net_volume', 0):,.0f}")


def _seed_from_parquet(db: WalletDB) -> None:
    """Seed DB wallet_profiles from existing parquet if DB is empty."""
    if db.stats()["profiles"] > 0:
        return
    from pathlib import Path
    parquet_path = Path("data/polymarket/wallet_profiles.parquet")
    if not parquet_path.exists():
        return
    import pandas as pd
    df = pd.read_parquet(parquet_path)
    count = 0
    for _, row in df.iterrows():
        db.upsert_profile(
            row["wallet"],
            edge=float(row.get("edge", 0)),
            early_win_rate=float(row.get("early_win_rate", 0)),
            win_rate=float(row.get("win_rate", 0)),
            resolved_trades=int(row.get("resolved_trades", 0)),
            total_volume_usdc=float(row.get("total_volume_usdc", 0)),
            is_early_informed=int(row.get("is_early_informed", False)),
            uncertain_early_trades=int(row.get("uncertain_early_trades", 0)),
        )
        count += 1
    print(f"  Seeded {count} wallet profiles from parquet")
