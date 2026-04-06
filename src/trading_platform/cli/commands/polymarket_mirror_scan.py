"""CLI: trading-cli data polymarket mirror-scan"""
from __future__ import annotations

import argparse
import logging

logger = logging.getLogger(__name__)


def cmd_polymarket_mirror_scan(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_mirror import WalletMirror

    top_n = int(getattr(args, "top_n", None) or 30)
    minutes = int(getattr(args, "minutes", None) or 90)
    min_fill = float(getattr(args, "min_fill", None) or 100)
    show_all = getattr(args, "all", False)
    min_conf = float(getattr(args, "min_confidence", None) or 0.70)
    profiles = "data/polymarket/wallet_profiles.parquet"

    mirror = WalletMirror(profiles, top_n=top_n, min_confidence=min_conf)
    print(f"Wallet Mirror Scan")
    print(f"  Watch list: {len(mirror.watch_list)} wallets (min conf={min_conf})")
    print(f"    Tier 1 (auto-trade): {len(mirror._tier1)}")
    print(f"    Tier 2 (alert only): {len(mirror._tier2)}")
    print(f"    Tier 3 (watch only): {len(mirror._tier3)}")
    if mirror.excluded_degraded:
        print(f"  Excluded {mirror.excluded_degraded} degraded wallets")
    if mirror.excluded_sports:
        print(f"  Excluded {mirror.excluded_sports} sports domain wallets")
    if mirror.excluded_low_ewr:
        print(f"  Excluded {mirror.excluded_low_ewr} low EWR (<65%) wallets")
    print(f"  Lookback: {minutes} min | Min fill: ${min_fill:,.0f}")
    print()

    if not mirror.watch_list:
        print("  No qualifying wallets found")
        return

    print(f"  {'Wallet':<14} {'Conf':>5} {'Domain':<12} {'EWR':>5} {'UET':>5} {'Volume':>10}")
    print(f"  {'-'*14} {'-'*5} {'-'*12} {'-'*5} {'-'*5} {'-'*10}")
    for w in mirror.watch_list[:10]:
        info = mirror._wallet_info[w]
        print(f"  {w[:14]:<14} {info['confidence_score']:>5.2f} {info.get('best_domain',''):<12} "
              f"{info['early_win_rate']:>5.0%} {info['uncertain_trades']:>5} "
              f"${info['total_volume']:>9,.0f}")
    if len(mirror.watch_list) > 10:
        print(f"  ... and {len(mirror.watch_list) - 10} more")
    print()

    batch_count = (len(mirror.watch_list) + 9) // 10
    print(f"  Fetching recent trades from Goldsky ({batch_count} batches)...")
    signals = mirror.get_mirror_signals(since_minutes=minutes, min_fill_usdc=min_fill)

    tradeable = [s for s in signals if s.tradeable]
    no_quotes = [s for s in signals if s.current_price is not None and s.spread is None and not s.tradeable]
    illiquid = [s for s in signals if s.spread is not None and not s.tradeable]
    untracked = [s for s in signals if s.current_price is None]
    expired = getattr(mirror, "_last_expired_count", 0)
    sports = getattr(mirror, "_last_sports_count", 0)

    print(f"  Signals: {len(signals)} total")
    print(f"    Tradeable: {len(tradeable)}")
    print(f"    No quotes (price only): {len(no_quotes)}")
    print(f"    Illiquid (spread>15c): {len(illiquid)}")
    print(f"    Not tracked: {len(untracked)}")
    if expired:
        print(f"    Expired markets filtered: {expired}")
    if sports:
        print(f"    Sports markets filtered: {sports}")
    print()

    display = signals if show_all else tradeable
    if not display:
        if signals:
            print(f"  No tradeable signals ({len(signals)} total filtered)")
        else:
            print(f"  No mirror signals in last {minutes} min above ${min_fill:,.0f}")
        return

    print(f"  {'Tier':<4} {'Wallet':<12} {'Question':<32} {'Dir':>4} {'$Fill':>7} {'#':>3} {'Min':>5} {'Price':>6} {'Spread':>7} {'Edge':>6}")
    print(f"  {'-'*4} {'-'*12} {'-'*32} {'-'*4} {'-'*7} {'-'*3} {'-'*5} {'-'*6} {'-'*7} {'-'*6}")
    for s in display[:30]:
        q = s.question[:32].encode("ascii", "replace").decode() if s.question else "?"
        p = f"{s.current_price:.2f}" if s.current_price is not None else "n/a"
        sp = f"{s.spread:.3f}" if s.spread is not None else "n/a"
        tier = "T1" if s.trigger_wallet in mirror._tier1 else ("T2" if s.trigger_wallet in mirror._tier2 else "T3")
        fc = f"{s.fill_count}" if s.fill_count > 1 else ""
        print(
            f"  [{tier}] {s.trigger_wallet[:12]:<12} {q:<32} {s.direction:>4} "
            f"{s.fill_amount_usdc:>7,.0f} {fc:>3} {s.minutes_since_fill:>5.0f} "
            f"{p:>6} {sp:>7} {s.wallet_edge:>6.1%}"
        )
