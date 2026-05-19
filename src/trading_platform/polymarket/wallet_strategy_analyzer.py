"""
Wallet strategy analyzer.

Identifies what strategies top wallets use, clusters them by behavior,
and surfaces which strategy × category combinations have real edge.
Run standalone or import for scheduled analysis.

Usage::
    python -m trading_platform.polymarket.wallet_strategy_analyzer
    python -m trading_platform.polymarket.wallet_strategy_analyzer --top 100
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Minimum resolved trades to include a wallet in strategy analysis
MIN_RESOLVED = 10
# Minimum per-category resolved trades to score wallet in that category
MIN_CAT_RESOLVED = 5


def _wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    """One-sided Wilson 90% lower bound on win rate."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    spread = (z / denom) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, centre - spread)


def _classify_strategy(stats: dict) -> str:
    """Classify a wallet's trading strategy from behavioral statistics."""
    avg_hold_h = stats.get("avg_hold_hours", 0) or 0
    avg_entry = stats.get("avg_entry_price", 0.5) or 0.5
    win_rate = stats.get("win_rate", 0) or 0
    n_cats = stats.get("n_categories", 1) or 1
    avg_size = stats.get("avg_trade_size", 0) or 0
    trades_per_market = stats.get("trades_per_market", 1) or 1

    # Information trader: buys high-probability outcomes, short hold, high WR
    if avg_entry > 0.65 and win_rate > 0.65 and avg_hold_h < 48:
        return "information"

    # Value player: buys at low prices (longshots), needs big win to profit
    if avg_entry < 0.30 and win_rate > 0.30:
        return "value"

    # Specialist: concentrated in few categories, high WR in those
    if n_cats <= 2 and win_rate > 0.55:
        return "specialist"

    # Accumulator: multiple fills on same market, building position
    if trades_per_market > 2.5:
        return "accumulator"

    # Conviction: large single trades, moderate entry price
    if avg_size > 500 and avg_entry > 0.35:
        return "conviction"

    # Diversified: broad category coverage, moderate performance
    if n_cats >= 5:
        return "diversified"

    return "generalist"


def analyze(top_n: int = 200, min_pnl: float = 0) -> dict:
    """Run full strategy analysis on top wallets.

    Returns a summary dict with top wallets, strategy clusters, and
    category specialists suitable for seeding the leaderboard or
    adjusting signal routing.
    """
    from trading_platform.polymarket.db_connection import get_connection

    conn = get_connection()
    try:
        # Top wallets by total PnL with sufficient resolved trades
        wallet_rows = conn.execute("""
            SELECT
                wallet,
                COUNT(DISTINCT condition_id)           AS markets,
                SUM(CASE WHEN market_resolved=1 AND pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN market_resolved=1 THEN 1 ELSE 0 END)              AS resolved,
                ROUND(SUM(COALESCE(pnl, 0))::numeric, 2)                        AS total_pnl,
                ROUND(AVG(price)::numeric, 3)                                    AS avg_entry,
                ROUND(AVG(size)::numeric, 2)                                     AS avg_size,
                COUNT(DISTINCT LOWER(category))                                  AS n_categories
            FROM wallet_trades
            GROUP BY wallet
            HAVING SUM(CASE WHEN market_resolved=1 THEN 1 ELSE 0 END) >= %s
               AND SUM(COALESCE(pnl, 0)) >= %s
            ORDER BY total_pnl DESC
            LIMIT %s
        """, (MIN_RESOLVED, min_pnl, top_n)).fetchall()

        # Per-wallet, per-category performance
        cat_rows = conn.execute("""
            SELECT
                wallet,
                LOWER(category)                                                  AS category,
                SUM(CASE WHEN market_resolved=1 AND pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN market_resolved=1 THEN 1 ELSE 0 END)              AS resolved,
                ROUND(AVG(CASE WHEN market_resolved=1 THEN pnl END)::numeric, 4) AS avg_pnl
            FROM wallet_trades
            WHERE category IS NOT NULL
            GROUP BY wallet, LOWER(category)
            HAVING SUM(CASE WHEN market_resolved=1 THEN 1 ELSE 0 END) >= %s
        """, (MIN_CAT_RESOLVED,)).fetchall()

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Build per-wallet category map
    wallet_cats: dict[str, list[dict]] = {}
    for wallet, cat, wins, resolved, avg_pnl in cat_rows:
        if wallet not in wallet_cats:
            wallet_cats[wallet] = []
        wr = wins / resolved if resolved else 0
        wilson = _wilson_lower(int(wins), int(resolved))
        wallet_cats[wallet].append({
            "category": cat,
            "n": int(resolved),
            "wins": int(wins),
            "wr": round(wr, 3),
            "wilson_lower": round(wilson, 3),
            "avg_pnl": float(avg_pnl or 0),
        })

    # Build wallet profiles
    wallets = []
    strategy_counts: dict[str, int] = {}
    category_specialists: dict[str, list[dict]] = {}

    for row in wallet_rows:
        wallet, markets, wins, resolved, total_pnl, avg_entry, avg_size, n_cats = row
        wr = wins / resolved if resolved else 0
        cats = sorted(
            wallet_cats.get(wallet, []),
            key=lambda x: (x["wilson_lower"], x["avg_pnl"]),
            reverse=True,
        )
        best_cats = [c for c in cats if c["wilson_lower"] > 0.45 and c["n"] >= MIN_CAT_RESOLVED]

        stats = {
            "avg_entry_price": float(avg_entry or 0.5),
            "avg_trade_size": float(avg_size or 0),
            "win_rate": wr,
            "n_categories": int(n_cats),
            "trades_per_market": int(resolved) / max(int(markets), 1),
        }
        strategy = _classify_strategy(stats)
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        profile = {
            "wallet": wallet,
            "markets": int(markets),
            "resolved": int(resolved),
            "wins": int(wins),
            "win_rate": round(wr, 3),
            "wilson_lower": round(_wilson_lower(int(wins), int(resolved)), 3),
            "total_pnl": float(total_pnl),
            "avg_entry": float(avg_entry or 0),
            "avg_size": float(avg_size or 0),
            "n_categories": int(n_cats),
            "strategy": strategy,
            "strong_categories": best_cats[:4],
        }
        wallets.append(profile)

        # Index by strong category for specialist discovery
        for cat_data in best_cats[:2]:
            cat = cat_data["category"]
            if cat not in category_specialists:
                category_specialists[cat] = []
            category_specialists[cat].append({
                "wallet": wallet,
                "wr": cat_data["wr"],
                "wilson_lower": cat_data["wilson_lower"],
                "n": cat_data["n"],
                "avg_pnl": cat_data["avg_pnl"],
                "total_pnl": float(total_pnl),
            })

    # Sort each category's specialists by wilson_lower × total_pnl
    for cat in category_specialists:
        category_specialists[cat].sort(
            key=lambda x: x["wilson_lower"] * math.log1p(abs(x["total_pnl"])),
            reverse=True,
        )
        category_specialists[cat] = category_specialists[cat][:20]

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "top_n": top_n,
        "wallets_analyzed": len(wallets),
        "strategy_distribution": strategy_counts,
        "top_wallets": wallets[:50],
        "category_specialists": category_specialists,
    }


def print_report(result: dict) -> None:
    """Print human-readable strategy analysis report."""
    ts = result["generated_at"]
    n = result["wallets_analyzed"]
    print(f"\n{'='*70}")
    print(f"WALLET STRATEGY ANALYSIS  —  {ts[:19]}  ({n} wallets)")
    print(f"{'='*70}")

    print("\nStrategy distribution:")
    for strat, count in sorted(result["strategy_distribution"].items(),
                                key=lambda x: -x[1]):
        print(f"  {strat:<20} {count:>4} wallets")

    print("\nTop 20 wallets by PnL:")
    print(f"  {'Wallet':<16} {'WR':>5} {'n':>5} {'PnL':>12}  {'Strategy':<14} {'Top categories'}")
    print(f"  {'-'*80}")
    for w in result["top_wallets"][:20]:
        cats = ", ".join(c["category"] for c in w["strong_categories"][:3]) or "—"
        print(f"  {w['wallet'][:14]}  {w['win_rate']:>4.0%}  {w['resolved']:>5}  "
              f"${w['total_pnl']:>10,.0f}  {w['strategy']:<14}  {cats}")

    print("\nCategory specialists (top 5 per category):")
    for cat, specialists in sorted(result["category_specialists"].items()):
        print(f"\n  {cat.upper()}:")
        for s in specialists[:5]:
            print(f"    {s['wallet'][:14]}  WR={s['wr']:.0%} (Wilson={s['wilson_lower']:.0%})  "
                  f"n={s['n']}  avg_pnl={s['avg_pnl']:+.2f}  total=${s['total_pnl']:,.0f}")


def save_report(result: dict) -> Path:
    """Save JSON report to data/polymarket/wallet_strategy_report.json."""
    out = _PROJECT_ROOT / "data" / "polymarket" / "wallet_strategy_report.json"
    out.write_text(json.dumps(result, indent=2))
    return out


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    p = argparse.ArgumentParser(description="Wallet strategy analysis")
    p.add_argument("--top", type=int, default=500, help="Top N wallets by PnL")
    p.add_argument("--min-pnl", type=float, default=0, help="Minimum total PnL filter")
    p.add_argument("--json", action="store_true", help="Output raw JSON instead of report")
    args = p.parse_args()

    result = analyze(top_n=args.top, min_pnl=args.min_pnl)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_report(result)
        out = save_report(result)
        print(f"\nReport saved to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
